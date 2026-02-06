#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
利润报表-MSKU维度按天数据采集任务
从领星API获取利润报表数据并存入数据库
API: /bd/profit/report/open/report/msku/list
唯一键：dataDate + sid + msku + asin
"""
import asyncio
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import re

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase

# 获取日志记录器
logger = get_logger('profit_report_msku_daily')

# 重试配置（令牌桶容量为10，比listing接口更宽松）
MAX_RETRIES = 5  # 最大重试次数
RETRY_DELAY = 10  # 重试延迟（秒）
REQUEST_DELAY = 2  # 请求间隔（秒）- 令牌桶容量为10，可以适当减少间隔
TOKEN_BUCKET_CAPACITY = 10  # 令牌桶容量


async def fetch_page_with_retry(op_api: OpenApiBase, token: str, 
                                 req_body: dict, offset: int) -> Tuple[List[Dict[str, Any]], int, str, bool]:
    """
    带重试机制的单页数据获取（指数退避策略，令牌桶感知）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        req_body: 请求体
        offset: 分页偏移量
        
    Returns:
        Tuple[List, int, str, bool]: (数据列表, 总数, 错误信息, 是否遇到限流)
    """
    was_rate_limited = False
    
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"offset={offset}，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token, 
                "/bd/profit/report/open/report/msku/list", 
                "POST", 
                req_body=req_body
            )
            
            # 兼容Pydantic v1和v2
            try:
                result = resp.model_dump()  # Pydantic v2
            except AttributeError:
                result = resp.dict()  # Pydantic v1
            
            code = result.get('code', 0)
            message = result.get('msg', '') or result.get('message', '')
            
            # 检查是否请求过于频繁（使用指数退避）
            if code == 3001008:  # 请求过于频繁（令牌桶无令牌）
                was_rate_limited = True
                wait_time = RETRY_DELAY * (2 ** retry)  # 指数退避：10, 20, 40, 80, 160秒
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒让令牌回收...")
                await asyncio.sleep(wait_time)
                continue
            
            # 检查是否token过期
            if code in [401, 403, 2001003, 2001005, 3001001, 3001002]:  # token相关错误
                logger.error(f"🔑 Token错误 (code={code}): {message}，需要刷新token")
                return [], 0, "TOKEN_EXPIRED", False
            
            # 检查其他错误
            if code != 0:
                logger.warning(f"⚠️  API返回错误: code={code}, message={message}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数，放弃该页")
                    return [], 0, f"API_ERROR: {message}", was_rate_limited
            
            # 获取数据
            data = result.get('data', {})
            if data is None:
                data = {}
            
            records = data.get('records', [])
            total = data.get('total', 0)
            
            # 确保records不为None
            if records is None:
                records = []
            
            # 成功获取数据
            if retry > 0:
                logger.info(f"✅ 重试成功！")
            
            return records, total, "", was_rate_limited
            
        except Exception as e:
            # 记录详细的异常信息（包括异常类型和堆栈）
            error_msg = str(e) if str(e) else repr(e)
            error_type = type(e).__name__
            logger.error(f"❌ 请求异常（第 {retry + 1}/{MAX_RETRIES} 次尝试）: {error_type}: {error_msg}", exc_info=True)
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"❌ 达到最大重试次数，请求失败")
                return [], 0, f"EXCEPTION: {error_type}: {error_msg}", was_rate_limited
    
    return [], 0, "MAX_RETRIES_EXCEEDED", was_rate_limited


async def fetch_all_profit_reports(op_api: OpenApiBase, token_resp,
                                    start_date: str,
                                    end_date: str,
                                    sid_list: List[str] = None,
                                    max_records: int = None,
                                    monthly_query: bool = False,
                                    test_msku: str = None) -> List[Dict[str, Any]]:
    """
    获取所有利润报表数据（分页处理，支持重试和token刷新）
    
    Args:
        op_api: OpenAPI客户端
        token_resp: 访问令牌响应对象（包含access_token和refresh_token）
        start_date: 开始时间（格式：Y-m-d 或 Y-m）
        end_date: 结束时间（格式：Y-m-d 或 Y-m）
        sid_list: 店铺ID列表（可选，如果为None则查询所有店铺）
        max_records: 最大抓取记录数（可选，用于测试，如果为None则抓取全部）
        monthly_query: 是否按月查询（默认False，按天查询）
        test_msku: 测试用MSKU（可选，只查询指定的MSKU）
        
    Returns:
        List[Dict[str, Any]]: 利润报表列表
    """
    all_records = []
    offset = 0
    length = 1000  # 每页1000条
    token = token_resp.access_token
    total_records = None
    total_filtered = 0  # 统计被过滤掉的店铺数据总数
    # 需要过滤的店铺列表
    excluded_shops = ['CY-US', 'WSH-US', 'ZX-US']
    
    # 令牌桶感知：动态调整请求间隔
    current_delay = REQUEST_DELAY  # 当前请求间隔
    consecutive_success = 0  # 连续成功次数
    consecutive_rate_limited = 0  # 连续限流次数
    
    # 构建请求参数（只保留必要的字段）
    req_body = {
        "offset": offset,
        "length": length,
        "startDate": start_date,
        "endDate": end_date,
        "currencyCode": "CNY",  # 币种为CNY
        "orderStatus": "Disbursed"  # 交易状态为Disbursed（已发放）
    }
    
    # 如果是测试模式，只查询指定的MSKU
    if test_msku:
        req_body["searchField"] = "seller_sku"
        req_body["searchValue"] = [test_msku]
        logger.info(f"🧪 测试模式：只查询MSKU={test_msku}")
    
    while True:
        req_body["offset"] = offset
        
        # 带重试机制获取数据（令牌桶感知）
        records, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
        
        # 处理token过期
        if error == "TOKEN_EXPIRED":
            logger.info("Token已过期，正在刷新...")
            try:
                token_resp = await op_api.generate_access_token()
                token = token_resp.access_token
                logger.info(f"Token刷新成功，有效期: {token_resp.expires_in}秒")
                # 重试当前页
                records, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
            except Exception as e:
                logger.error(f"Token刷新失败: {e}")
                break
        
        # 处理其他错误
        if error and error != "TOKEN_EXPIRED":
            logger.error(f"获取offset={offset}数据失败: {error}")
            break
        
        # 记录总数
        if total_records is None and total > 0:
            total_records = total
            logger.info(f"📊 总共需要获取 {total_records} 条数据")
        
        logger.info(f"✓ offset={offset}: 获取 {len(records)} 条数据")
        
        if not records:
            if offset == 0:
                logger.warning("第一页就没有数据")
            break
        
        # 过滤掉指定店铺的数据（CY-US, WSH-US, ZX-US）
        filtered_records = [r for r in records if r.get('storeName', '') not in excluded_shops]
        filtered_count = len(records) - len(filtered_records)
        if filtered_count > 0:
            total_filtered += filtered_count
            # 统计每个被过滤店铺的数量
            shop_filtered = {}
            for r in records:
                shop_name = r.get('storeName', '')
                if shop_name in excluded_shops:
                    shop_filtered[shop_name] = shop_filtered.get(shop_name, 0) + 1
            filtered_shops_str = ', '.join([f"{shop}: {count}条" for shop, count in shop_filtered.items()])
            logger.info(f"  过滤掉 {filtered_count} 条店铺数据 ({filtered_shops_str})")
        
        all_records.extend(filtered_records)
        
        # 测试模式：如果设置了最大记录数，达到后退出
        if max_records and len(all_records) >= max_records:
            logger.info(f"🧪 测试模式：已达到最大记录数 {max_records} 条，停止抓取")
            all_records = all_records[:max_records]  # 确保不超过限制
            break
        
        # 显示进度
        if total_records:
            progress = (len(all_records) / total_records) * 100
            logger.info(f"📈 进度: {len(all_records)}/{total_records} ({progress:.1f}%)")
        elif max_records:
            logger.info(f"📈 进度: {len(all_records)}/{max_records} (测试模式)")
        
        # 如果返回的数据少于length，说明已经是最后一页
        if len(records) < length:
            break
        
        offset += length
        
        # 令牌桶感知：动态调整请求间隔
        if was_rate_limited:
            # 遇到限流，增加间隔（给令牌更多时间回收）
            consecutive_rate_limited += 1
            consecutive_success = 0
            # 间隔递增：2秒 → 3秒 → 4秒 → 5秒（最大5秒）
            current_delay = min(REQUEST_DELAY + consecutive_rate_limited, 5)
            logger.info(f"🪣 检测到限流，调整请求间隔为 {current_delay} 秒")
        else:
            # 成功获取，重置限流计数
            consecutive_rate_limited = 0
            consecutive_success += 1
            # 连续成功3次后，可以稍微减少间隔（但不少于基础间隔）
            if consecutive_success >= 3:
                current_delay = max(REQUEST_DELAY - 0.5, 1.0)
                consecutive_success = 0  # 重置计数
            else:
                current_delay = REQUEST_DELAY
        
        # 请求间隔延时（动态调整，令牌桶感知）
        await asyncio.sleep(current_delay)
    
    # 数据完整性检查（考虑被过滤的店铺数据）
    if total_records:
        expected_after_filter = total_records - total_filtered
        excluded_shops_str = ', '.join(excluded_shops)
        if len(all_records) < expected_after_filter:
            logger.warning(f"⚠️  数据可能不完整: 预期 {expected_after_filter} 条（已排除 {total_filtered} 条店铺数据: {excluded_shops_str}），实际获取 {len(all_records)} 条")
            logger.warning(f"   缺失: {expected_after_filter - len(all_records)} 条数据")
        else:
            logger.info(f"✅ 数据完整性验证通过: {len(all_records)}/{expected_after_filter} 条（已排除 {total_filtered} 条店铺数据: {excluded_shops_str}）")
    
    return all_records


def add_performance_indexes(table_name: str) -> None:
    """
    添加性能优化索引（如果不存在）
    
    这些索引用于优化后续的查询性能，特别是：
    - update_profit_report_calculated_fields.py 中的复杂查询
    - 按日期、店铺、SKU等维度的数据分析
    
    Args:
        table_name: 表名
    """
    indexes_to_add = [
        {
            'name': 'idx_stat_date',
            'columns': ['统计日期'],
            'comment': '统计日期索引，用于日期范围查询'
        },
        {
            'name': 'idx_shop',
            'columns': ['店铺'],
            'comment': '店铺索引，用于按店铺查询'
        },
        {
            'name': 'idx_sku',
            'columns': ['SKU'],
            'comment': 'SKU索引，用于产品管理表JOIN'
        },
        {
            'name': 'idx_shop_person_date',
            'columns': ['店铺', '负责人', '统计日期'],
            'comment': '店铺+负责人+统计日期复合索引，用于头程单价匹配'
        },
        {
            'name': 'idx_date_shop',
            'columns': ['统计日期', '店铺'],
            'comment': '统计日期+店铺复合索引，用于日期范围内按店铺查询'
        },
    ]
    
    with db_cursor(dictionary=False) as cursor:
        # 获取已存在的索引
        cursor.execute(f"""
            SELECT DISTINCT INDEX_NAME 
            FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = DATABASE() 
              AND TABLE_NAME = '{table_name}'
        """)
        existing_indexes = {row[0] for row in cursor.fetchall()}
        
        added_count = 0
        for idx_info in indexes_to_add:
            idx_name = idx_info['name']
            
            # 检查索引是否已存在
            if idx_name in existing_indexes:
                logger.debug(f"  索引 {idx_name} 已存在，跳过")
                continue
            
            try:
                # 构建索引SQL
                columns_str = ', '.join([f"`{col}`" for col in idx_info['columns']])
                sql = f"CREATE INDEX `{idx_name}` ON `{table_name}` ({columns_str})"
                
                cursor.execute(sql)
                logger.info(f"  ✅ 已添加索引: {idx_name} ({columns_str}) - {idx_info['comment']}")
                added_count += 1
            except Exception as e:
                logger.error(f"  ❌ 添加索引 {idx_name} 失败: {e}")
        
        if added_count > 0:
            logger.info(f"成功添加 {added_count} 个性能优化索引")
        else:
            logger.info("所有性能优化索引都已存在")


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any]) -> None:
    """
    创建数据表（如果不存在），如果表存在但结构不匹配则报错
    
    Args:
        table_name: 表名
        sample_row: 样本数据行
        
    Raises:
        ValueError: 如果表存在但结构不匹配
    """
    with db_cursor(dictionary=False) as cursor:
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()
        
        if exists:
            # 字段重命名映射（旧字段名 -> 新字段名）
            field_rename_map = {
                '平台佣金': '平台费',
                'FBM销售退款': 'FBM销售退款额',
                'FBA销售退款': 'FBA销售退款额',
                '买家运费退款': '买家运费退款额',
                '促销折扣退款': '促销折扣退款额',
                '库存调整费': '库存调整费用',
                'FBA国际入库费': 'FBA国际物流货运费',
            }
            
            # 检查表结构，自动添加缺失的字段
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            existing_columns = {row[0] for row in cursor.fetchall()}
            expected_columns = set(sample_row.keys())
            
            # 处理字段重命名
            for old_name, new_name in field_rename_map.items():
                if old_name in existing_columns and new_name in expected_columns:
                    try:
                        cursor.execute(f"ALTER TABLE `{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` DOUBLE")
                        logger.info(f"  ✅ 已重命名字段: {old_name} -> {new_name}")
                        existing_columns.remove(old_name)
                        existing_columns.add(new_name)
                    except Exception as e:
                        logger.error(f"  ❌ 重命名字段 {old_name} -> {new_name} 失败: {e}")
            
            # 找出缺失的字段
            missing_columns = expected_columns - existing_columns
            
            if missing_columns:
                logger.info(f"表 {table_name} 缺少字段: {missing_columns}，正在添加...")
                for col_name in missing_columns:
                    col_value = sample_row[col_name]
                    if isinstance(col_value, int):
                        col_type = "INT"
                    elif isinstance(col_value, float):
                        col_type = "DOUBLE"
                    elif col_name == '统计日期':
                        col_type = "DATE"
                    elif col_name in ['店铺', 'MSKU', 'ASIN']:
                        col_type = "VARCHAR(100)"
                    elif '缩略图' in col_name or 'URL' in col_name.upper() or '地址' in col_name or col_name == '商品标题':
                        col_type = "TEXT"
                    else:
                        col_type = "VARCHAR(500)"
                    
                    try:
                        cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}")
                        logger.info(f"  ✅ 已添加字段: {col_name} ({col_type})")
                    except Exception as e:
                        logger.error(f"  ❌ 添加字段 {col_name} 失败: {e}")
            
            # 检查是否有多余的字段（只警告，不报错）
            extra_columns = existing_columns - expected_columns - {'id'}
            if extra_columns:
                logger.warning(f"表 {table_name} 存在额外字段: {extra_columns}")
            
            logger.info(f"表 {table_name} 结构检查完成")
            
            # 检查并添加性能优化索引
            logger.info("正在检查性能优化索引...")
            add_performance_indexes(table_name)
            return
        
        # 表不存在，创建表
        fields = []
        for k, v in sample_row.items():
            if isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                # 统计日期使用DATE类型
                if k == '统计日期':
                    fields.append(f"`{k}` DATE")
                # 对于唯一索引字段，使用较小的长度以避免索引键过长
                elif k in ['店铺', 'MSKU', 'ASIN']:
                    # 唯一键字段使用VARCHAR(100)
                    fields.append(f"`{k}` VARCHAR(100)")
                # 对于可能包含URL或长文本的字段，使用TEXT类型
                elif '缩略图' in k or 'URL' in k.upper() or '地址' in k or k == '商品标题':
                    fields.append(f"`{k}` TEXT")
                else:
                    fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        # 添加唯一索引：统计日期 + 店铺 + MSKU + ASIN
        # 使用VARCHAR(100)后，索引键长度 = 100*4*4 = 1600字节，远小于3072字节限制
        sql = f"""CREATE TABLE `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {fields_sql},
            UNIQUE KEY `uk_date_shop_msku_asin` (`统计日期`, `店铺`, `MSKU`, `ASIN`)
        )"""
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功（唯一键：统计日期 + 店铺 + MSKU + ASIN）")
        
        # 创建性能优化索引
        logger.info("正在添加性能优化索引...")
        add_performance_indexes(table_name)


def insert_data_batch(table_name: str, data_list: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    批量插入数据（使用 ON DUPLICATE KEY UPDATE）
    
    Args:
        table_name: 表名
        data_list: 数据列表
        
    Returns:
        Tuple[int, int]: (插入的记录数, 更新的记录数)
    """
    if not data_list:
        return 0, 0
    
    with db_cursor(dictionary=False) as cursor:
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        
        # 使用 ON DUPLICATE KEY UPDATE 处理重复键
        update_fields = ','.join([f"`{k}`=VALUES(`{k}`)" for k in keys if k not in ['统计日期', '店铺', 'MSKU', 'ASIN']])
        sql = f"INSERT INTO `{table_name}` ({fields}) VALUES ({values_placeholder}) ON DUPLICATE KEY UPDATE {update_fields}"
        
        batch_size = 200
        total_inserted = 0
        total_updated = 0
        
        for i in range(0, len(data_list), batch_size):
            batch = [tuple(row.values()) for row in data_list[i:i+batch_size]]
            cursor.executemany(sql, batch)
            
            # MySQL的affected_rows规则：
            # - 插入新行：返回1
            # - 更新已存在的行：返回2（如果数据有变化）
            # - 更新已存在的行但数据无变化：返回0
            affected = cursor.rowcount
            # 估算插入和更新的数量（简化处理）
            batch_count = len(batch)
            if affected >= batch_count:
                # 有更新发生
                total_updated += (affected - batch_count)
                total_inserted += batch_count
            else:
                # 全部是插入
                total_inserted += affected
            
            logger.info(f"已处理 {min(i+batch_size, len(data_list))}/{len(data_list)} 条...")
        
        logger.info(f"成功写入 {len(data_list)} 条数据到表 {table_name}")
        return total_inserted, total_updated


def convert_profit_report_data(records: List[Dict[str, Any]], 
                                sid_to_name_map: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """
    转换利润报表数据（精简版，只保留必要字段）
    
    保留字段：
    - 基础信息：SKU, SPU, MSKU, ASIN, 父ASIN, 店铺, 品名, 商品标题, 商品缩略图, 统计日期
    - 分类信息：分类, 负责人, 产品开发负责人
    - 财务信息：汇率, 毛利润
    - 销量：FBA销量, FBM销量, FBA补换货量, FBM补换货量
    - 退货：退货量(可售), 退货量(不可售)
    - 销售额：总退款额, FBA销售额, FBM销售额
    - 收入：包装收入, 买家交易保障索赔, 积分抵减收入, FBA库存赔偿, 促销折扣, 买家运费, 其他收入, 
            清算收入, 亚马逊运费赔偿, Safe-T索赔, Netco交易, 赔偿收入, 追索收入, 清算调整, 混合VAT收入, 平台收入
    - 费用：平台费, FBM销售退款额, FBA销售退款额, 买家运费退款额, 买家包装退款额, 促销折扣退款额, 
            买家拒付, 积分抵减退回, 平台费退款额, 发货费退款额, 其他订单费退款额, 运输标签费退款, 
            交易费用退款额, 积分费用, 费用退款, FBA发货费, FBA发货费(多渠道), FBA配送费, 其他订单费用,
            FBA国际物流货运费, 调整费用, 订阅费, 秒杀费, 优惠券, 早期评论人计划, vine
    - 广告费：广告费总计, SP广告费, SB广告费, SBV广告费, SD广告费
    - 仓储费：仓储费总计, 其他仓储费, 月仓储费-本月计提, 月仓储费-上月冲销, 长期仓储费-本月计提, 
              长期仓储费-上月冲销, FBA销毁费, FBA移除费, 入仓手续费, 标签费, 塑料包装费, 
              FBA卖家退回费, FBA仓储费入库缺陷费, 库存调整费用, 合作承运费, 入库配置费, 超量仓储费, 清算费, 其他服务费
    - 成本：采购成本, 头程成本, 其他成本, 销毁费用, 商品成本附加费, 头程成本附加费, 广告费用减免
    - 税费：销售税
    - 其他：平台其他费总计, 交易状态
    
    Args:
        records: 利润报表列表
        sid_to_name_map: 店铺ID到店铺名称的映射（可选）
        
    Returns:
        List[Dict[str, Any]]: 转换后的数据列表
    """
    report_list = []
    
    for record in records:
        # 店铺ID和店铺名（保留原始店铺名称，不使用映射）
        sid = record.get('sid', '')
        shop_name = record.get('storeName', '') or '无'
        
        # 基础字段
        local_sku = record.get('localSku', '') or ''
        # SPU：取SKU第一个"-"之前的字符
        spu = local_sku.split('-')[0] if local_sku and '-' in local_sku else local_sku
        msku = record.get('msku', '') or ''
        asin = record.get('asin', '') or ''
        parent_asin = record.get('parentAsin', '') or ''
        local_name = record.get('localName', '') or ''
        item_name = record.get('itemName', '') or ''
        small_image_url = record.get('smallImageUrl', '') or ''
        
        # 去掉图片URL中的._SL75_等尺寸参数
        if small_image_url:
            small_image_url = re.sub(r'\._SL\d+_', '', small_image_url)
        
        # 统计日期
        data_date = record.get('postedDateLocale', '') or record.get('reportDateMonth', '') or ''
        
        # 分类信息
        category_name = record.get('categoryName', '') or ''
        principal_realname = record.get('principalRealname', '') or ''
        product_developer_realname = record.get('productDeveloperRealname', '') or ''
        
        # 汇率（从API获取，如果没有则使用默认值1）- 强制转换为float
        exchange_rate = float(record.get('exchangeRate', 1) or 1)
        
        # 毛利润 - 强制转换为float
        gross_profit = float(record.get('grossProfit', 0) or 0)
        
        # 销量数据 - 强制转换为float
        fba_sales_quantity = float(record.get('fbaSalesQuantity', 0) or 0)
        fbm_sales_quantity = float(record.get('fbmSalesQuantity', 0) or 0)
        reship_fba_product_sales_quantity = float(record.get('reshipFbaProductSalesQuantity', 0) or 0)
        reship_fbm_product_sales_quantity = float(record.get('reshipFbmProductSalesQuantity', 0) or 0)
        
        # 退货数据 - 强制转换为float
        fba_returns_saleable_quantity = float(record.get('fbaReturnsSaleableQuantity', 0) or 0)
        fba_returns_unsaleable_quantity = float(record.get('fbaReturnsUnsaleableQuantity', 0) or 0)
        
        # 销售额数据 - 强制转换为float
        total_sales_refunds = float(record.get('totalSalesRefunds', 0) or 0)
        fba_sale_amount = float(record.get('fbaSaleAmount', 0) or 0)
        fbm_sale_amount = float(record.get('fbmSaleAmount', 0) or 0)
        
        # 收入数据 - 强制转换为float
        gift_wrap_credits = float(record.get('giftWrapCredits', 0) or 0)  # 包装收入
        guarantee_claims = float(record.get('guaranteeClaims', 0) or 0)  # 买家交易保障索赔
        cost_of_po_integers_granted = float(record.get('costOfPoIntegersGranted', 0) or 0)  # 积分抵减收入
        fba_inventory_credit = float(record.get('fbaInventoryCredit', 0) or 0)  # FBA库存赔偿
        promotional_rebates = float(record.get('promotionalRebates', 0) or 0)  # 促销折扣
        shipping_credits = float(record.get('shippingCredits', 0) or 0)  # 买家运费
        other_in_amount = float(record.get('otherInAmount', 0) or 0)  # 其他收入
        fba_liquidation_proceeds = float(record.get('fbaLiquidationProceeds', 0) or 0)  # 清算收入
        amazon_shipping_reimbursement = float(record.get('amazonShippingReimbursement', 0) or 0)  # 亚马逊运费赔偿
        safe_t_reimbursement = float(record.get('safeTReimbursement', 0) or 0)  # Safe-T索赔
        netco_transaction = float(record.get('netcoTransaction', 0) or 0)  # Netco交易
        reimbursements = float(record.get('reimbursements', 0) or 0)  # 赔偿收入
        clawbacks = float(record.get('clawbacks', 0) or 0)  # 追索收入
        fba_liquidation_proceeds_adjustments = float(record.get('fbaLiquidationProceedsAdjustments', 0) or 0)  # 清算调整
        shared_commingling_vat_income = float(record.get('sharedComminglingVatIncome', 0) or 0)  # 混合VAT收入
        
        # 平台收入（总销售额）- 强制转换为float
        total_sales_amount = float(record.get('totalSalesAmount', 0) or 0)
        
        # 费用数据 - 强制转换为float
        platform_fee = float(record.get('platformFee', 0) or 0)  # 平台费
        fbm_sales_refunds = float(record.get('fbmSalesRefunds', 0) or 0)  # FBM销售退款额
        fba_sales_refunds = float(record.get('fbaSalesRefunds', 0) or 0)  # FBA销售退款额
        shipping_credit_refunds = float(record.get('shippingCreditRefunds', 0) or 0)  # 买家运费退款额
        gift_wrap_credit_refunds = float(record.get('giftWrapCreditRefunds', 0) or 0)  # 买家包装退款额
        promotional_rebate_refunds = float(record.get('promotionalRebateRefunds', 0) or 0)  # 促销折扣退款额
        chargebacks = float(record.get('chargebacks', 0) or 0)  # 买家拒付
        cost_of_po_integers_returned = float(record.get('costOfPoIntegersReturned', 0) or 0)  # 积分抵减退回
        selling_fee_refunds = float(record.get('sellingFeeRefunds', 0) or 0)  # 平台费退款额
        fba_transaction_fee_refunds = float(record.get('fbaTransactionFeeRefunds', 0) or 0)  # 发货费退款额
        other_transaction_fee_refunds = float(record.get('otherTransactionFeeRefunds', 0) or 0)  # 其他订单费退款额
        shipping_label_refunds = float(record.get('shippingLabelRefunds', 0) or 0)  # 运输标签费退款
        refund_administration_fees = float(record.get('refundAdministrationFees', 0) or 0)  # 交易费用退款额
        points_adjusted = float(record.get('pointsAdjusted', 0) or 0)  # 积分费用
        total_fee_refunds = float(record.get('totalFeeRefunds', 0) or 0)  # 费用退款
        fba_delivery_fee = float(record.get('fbaDeliveryFee', 0) or 0)  # FBA发货费
        mc_fba_delivery_fee = float(record.get('mcFbaDeliveryFee', 0) or 0)  # FBA发货费(多渠道)
        total_fba_delivery_fee = float(record.get('totalFbaDeliveryFee', 0) or 0)  # FBA配送费
        other_transaction_fees = float(record.get('otherTransactionFees', 0) or 0)  # 其他订单费用
        shared_fba_integerernational_inbound_fee = float(record.get('sharedFbaIntegerernationalInboundFee', 0) or 0)  # FBA国际物流货运费
        adjustments = float(record.get('adjustments', 0) or 0)  # 调整费用
        shared_subscription_fee = float(record.get('sharedSubscriptionFee', 0) or 0)  # 订阅费
        shared_ld_fee = float(record.get('sharedLdFee', 0) or 0)  # 秒杀费
        shared_coupon_fee = float(record.get('sharedCouponFee', 0) or 0)  # 优惠券
        shared_early_reviewer_program_fee = float(record.get('sharedEarlyReviewerProgramFee', 0) or 0)  # 早期评论人计划
        shared_vine_fee = float(record.get('sharedVineFee', 0) or 0)  # vine
        
        # 广告费 - 强制转换为float
        total_ads_cost = float(record.get('totalAdsCost', 0) or 0)  # 广告费总计
        ads_sp_cost = float(record.get('adsSpCost', 0) or 0)  # SP广告费
        ads_sb_cost = float(record.get('adsSbCost', 0) or 0)  # SB广告费
        ads_sbv_cost = float(record.get('adsSbvCost', 0) or 0)  # SBV广告费
        ads_sd_cost = float(record.get('adsSdCost', 0) or 0)  # SD广告费
        
        # 仓储费 - 强制转换为float
        total_storage_fee = float(record.get('totalStorageFee', 0) or 0)  # 仓储费总计
        shared_other_fba_inventory_fees = float(record.get('sharedOtherFbaInventoryFees', 0) or 0)  # 其他仓储费
        fba_storage_fee_accrual = float(record.get('fbaStorageFeeAccrual', 0) or 0)  # 月仓储费-本月计提
        fba_storage_fee_accrual_difference = float(record.get('fbaStorageFeeAccrualDifference', 0) or 0)  # 月仓储费-上月冲销
        long_term_storage_fee_accrual = float(record.get('longTermStorageFeeAccrual', 0) or 0)  # 长期仓储费-本月计提
        long_term_storage_fee_accrual_difference = float(record.get('longTermStorageFeeAccrualDifference', 0) or 0)  # 长期仓储费-上月冲销
        # 新增仓储费用字段
        fba_storage_fee = float(record.get('fbaStorageFee', 0) or 0)  # 月度仓储费
        shared_fba_storage_fee = float(record.get('sharedFbaStorageFee', 0) or 0)  # 月度仓储费差异
        long_term_storage_fee = float(record.get('longTermStorageFee', 0) or 0)  # 长期仓储费
        shared_long_term_storage_fee = float(record.get('sharedLongTermStorageFee', 0) or 0)  # 长期仓储费差异
        shared_fba_disposal_fee = float(record.get('sharedFbaDisposalFee', 0) or 0)  # FBA销毁费
        shared_fba_removal_fee = float(record.get('sharedFbaRemovalFee', 0) or 0)  # FBA移除费
        shared_fba_inbound_transportation_program_fee = float(record.get('sharedFbaInboundTransportationProgramFee', 0) or 0)  # 入仓手续费
        shared_labeling_fee = float(record.get('sharedLabelingFee', 0) or 0)  # 标签费
        shared_polybagging_fee = float(record.get('sharedPolybaggingFee', 0) or 0)  # 塑料包装费
        shared_fba_customer_return_fee = float(record.get('sharedFbaCustomerReturnFee', 0) or 0)  # FBA卖家退回费
        shared_fba_inbound_defect_fee = float(record.get('sharedFbaInboundDefectFee', 0) or 0)  # FBA仓储费入库缺陷费
        shared_item_fee_adjustment = float(record.get('sharedItemFeeAdjustment', 0) or 0)  # 库存调整费用
        shared_amazon_partnered_carrier_shipment_fee = float(record.get('sharedAmazonPartneredCarrierShipmentFee', 0) or 0)  # 合作承运费
        shared_fba_inbound_convenience_fee = float(record.get('sharedFbaInboundConvenienceFee', 0) or 0)  # 入库配置费
        shared_fba_overage_fee = float(record.get('sharedFbaOverageFee', 0) or 0)  # 超量仓储费
        shared_liquidations_fees = float(record.get('sharedLiquidationsFees', 0) or 0)  # 清算费
        shared_other_service_fees = float(record.get('sharedOtherServiceFees', 0) or 0)  # 其他服务费
        
        # 成本数据 - 强制转换为float
        cg_price_total = float(record.get('cgPriceTotal', 0) or 0)  # 采购成本
        cg_transport_costs_total = float(record.get('cgTransportCostsTotal', 0) or 0)  # 头程成本
        cg_other_costs_total = float(record.get('cgOtherCostsTotal', 0) or 0)  # 其他成本
        
        # 从 otherFeeStr 数组中解析自定义费用
        other_fee_str = record.get('otherFeeStr', []) or []
        disposal_fee = 0.0  # 销毁费用
        cg_price_additional_fee = 0.0  # 商品成本附加费
        cg_transport_additional_fee = 0.0  # 头程成本附加费
        ads_fee_reduction = 0.0  # 广告费用减免
        
        if other_fee_str and isinstance(other_fee_str, list):
            for fee_item in other_fee_str:
                fee_name = fee_item.get('otherFeeName', '')
                fee_amount = float(fee_item.get('feeAllocation', 0) or 0)
                
                if fee_name == '销毁费用':
                    disposal_fee = fee_amount
                elif fee_name == '商品成本附加费':
                    cg_price_additional_fee = fee_amount
                elif fee_name == '头程成本附加费':
                    cg_transport_additional_fee = fee_amount
                elif fee_name == '广告费用减免':
                    ads_fee_reduction = fee_amount
        
        # 税费数据 - 强制转换为float
        total_sales_tax = float(record.get('totalSalesTax', 0) or 0)
        
        # 其他平台费用 - 强制转换为float
        total_platform_other_fee = float(record.get('totalPlatformOtherFee', 0) or 0)
        
        # 交易状态
        transaction_status = record.get('transactionStatus', '') or ''
        
        # 计算实际销量 = FBA销量 + FBM销量 + FBM补换货量 + FBA补换货量 - 退货量（可售） - 退货量（不可售）
        actual_quantity = fba_sales_quantity + fbm_sales_quantity + reship_fba_product_sales_quantity + reship_fbm_product_sales_quantity - fba_returns_saleable_quantity - fba_returns_unsaleable_quantity
        
        # 构建报表记录（只保留必要字段）
        report_record = {
            'SKU': local_sku,
            'SPU': spu,
            'MSKU': msku,
            'ASIN': asin,
            '父ASIN': parent_asin,
            '店铺id': str(sid) if sid else '',
            '店铺': shop_name,
            '品名': local_name,
            '商品标题': item_name,
            '商品缩略图': small_image_url,
            '统计日期': data_date,
            '分类': category_name,
            '负责人': principal_realname,
            '产品开发负责人': product_developer_realname,
            '汇率': exchange_rate,
            '毛利润': gross_profit,
            'FBA销量': fba_sales_quantity,
            'FBM销量': fbm_sales_quantity,
            'FBA补换货量': reship_fba_product_sales_quantity,
            'FBM补换货量': reship_fbm_product_sales_quantity,
            '退货量(可售)': fba_returns_saleable_quantity,
            '退货量(不可售)': fba_returns_unsaleable_quantity,
            '总退款额': total_sales_refunds,
            'FBA销售额': fba_sale_amount,
            'FBM销售额': fbm_sale_amount,
            '包装收入': gift_wrap_credits,
            '买家交易保障索赔': guarantee_claims,
            '积分抵减收入': cost_of_po_integers_granted,
            'FBA库存赔偿': fba_inventory_credit,
            '促销折扣': promotional_rebates,
            '买家运费': shipping_credits,
            '其他收入': other_in_amount,
            '清算收入': fba_liquidation_proceeds,
            '亚马逊运费赔偿': amazon_shipping_reimbursement,
            'Safe-T索赔': safe_t_reimbursement,
            'Netco交易': netco_transaction,
            '赔偿收入': reimbursements,
            '追索收入': clawbacks,
            '清算调整': fba_liquidation_proceeds_adjustments,
            '混合VAT收入': shared_commingling_vat_income,
            '平台收入': total_sales_amount,
            '平台费': platform_fee,
            'FBM销售退款额': fbm_sales_refunds,
            'FBA销售退款额': fba_sales_refunds,
            '买家运费退款额': shipping_credit_refunds,
            '买家包装退款额': gift_wrap_credit_refunds,
            '促销折扣退款额': promotional_rebate_refunds,
            '买家拒付': chargebacks,
            '积分抵减退回': cost_of_po_integers_returned,
            '平台费退款额': selling_fee_refunds,
            '发货费退款额': fba_transaction_fee_refunds,
            '其他订单费退款额': other_transaction_fee_refunds,
            '运输标签费退款': shipping_label_refunds,
            '交易费用退款额': refund_administration_fees,
            '积分费用': points_adjusted,
            '费用退款': total_fee_refunds,
            'FBA发货费': fba_delivery_fee,
            'FBA发货费(多渠道)': mc_fba_delivery_fee,
            'FBA配送费': total_fba_delivery_fee,
            '其他订单费用': other_transaction_fees,
            'FBA国际物流货运费': shared_fba_integerernational_inbound_fee,
            '调整费用': adjustments,
            '订阅费': shared_subscription_fee,
            '秒杀费': shared_ld_fee,
            '优惠券': shared_coupon_fee,
            '早期评论人计划': shared_early_reviewer_program_fee,
            'vine': shared_vine_fee,
            '广告费总计': total_ads_cost,
            'SP广告费': ads_sp_cost,
            'SB广告费': ads_sb_cost,
            'SBV广告费': ads_sbv_cost,
            'SD广告费': ads_sd_cost,
            '仓储费总计': total_storage_fee,
            '其他仓储费': shared_other_fba_inventory_fees,
            '月仓储费-本月计提': fba_storage_fee_accrual,
            '月仓储费-上月冲销': fba_storage_fee_accrual_difference,
            '长期仓储费-本月计提': long_term_storage_fee_accrual,
            '长期仓储费-上月冲销': long_term_storage_fee_accrual_difference,
            '月度仓储费': fba_storage_fee,
            '月度仓储费差异': shared_fba_storage_fee,
            '长期仓储费': long_term_storage_fee,
            '长期仓储费差异': shared_long_term_storage_fee,
            'FBA销毁费': shared_fba_disposal_fee,
            'FBA移除费': shared_fba_removal_fee,
            '入仓手续费': shared_fba_inbound_transportation_program_fee,
            '标签费': shared_labeling_fee,
            '塑料包装费': shared_polybagging_fee,
            'FBA卖家退回费': shared_fba_customer_return_fee,
            'FBA仓储费入库缺陷费': shared_fba_inbound_defect_fee,
            '库存调整费用': shared_item_fee_adjustment,
            '合作承运费': shared_amazon_partnered_carrier_shipment_fee,
            '入库配置费': shared_fba_inbound_convenience_fee,
            '超量仓储费': shared_fba_overage_fee,
            '清算费': shared_liquidations_fees,
            '其他服务费': shared_other_service_fees,
            '采购成本': cg_price_total,
            '头程成本': cg_transport_costs_total,
            '其他成本': cg_other_costs_total,
            '销毁费用': disposal_fee,
            '商品成本附加费': cg_price_additional_fee,
            '头程成本附加费': cg_transport_additional_fee,
            '广告费用减免': ads_fee_reduction,
            '销售税': total_sales_tax,
            '平台其他费总计': total_platform_other_fee,
            '交易状态': transaction_status,
            '实际销量': actual_quantity,  # 计算的实际销量
            '实际头程费用': 0.0,  # 新增字段，保留为空（浮点数0）
            '录入费用单头程': 0.0,  # 新增字段，保留为空（浮点数0）
        }
        
        report_list.append(report_record)
    
    return report_list


async def main(start_date: str = None, end_date: str = None):
    """
    主函数
    
    Args:
        start_date: 开始日期，格式：Y-m-d，默认：1月1号
        end_date: 结束日期，格式：Y-m-d，默认：今天
    """
    logger.info("="*80)
    logger.info("利润报表-MSKU维度按天数据采集")
    logger.info("="*80)
    
    # 验证配置
    if not settings.validate():
        logger.error("配置验证失败，请检查.env文件")
        return
    
    # 创建OpenAPI客户端
    config = settings.lingxing_config
    op_api = OpenApiBase(
        host=config['host'],
        app_id=config['app_id'],
        app_secret=config['app_secret'],
        proxy_url=config['proxy_url']
    )
    
    # 获取访问令牌
    try:
        token_resp = await op_api.generate_access_token()
        logger.info(f"Token有效期: {token_resp.expires_in}秒")
    except Exception as e:
        logger.error(f"获取访问令牌失败: {e}")
        return
    
    # 不使用店铺映射，直接使用API返回的原始店铺名称
    sid_to_name_map = None
    
    # 确定日期范围（默认1月1号到今天）
    if not end_date:
        month_end = datetime.now()
    else:
        month_end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if not start_date:
        # 默认从1月1号开始
        month_start = month_end.replace(month=1, day=1)
    else:
        month_start = datetime.strptime(start_date, '%Y-%m-%d')
    
    logger.info(f"📅 拉取数据（1月1号到今天）")
    logger.info(f"日期范围: {month_start.strftime('%Y-%m-%d')} ~ {month_end.strftime('%Y-%m-%d')}")
    logger.info(f"⏱️  配置参数（令牌桶容量为10）:")
    logger.info(f"   - 请求间隔: {REQUEST_DELAY}秒")
    logger.info(f"   - 最大重试: {MAX_RETRIES}次")
    logger.info(f"   - 重试延迟: {RETRY_DELAY}秒（指数退避）")
    logger.info(f"   - 币种: CNY")
    logger.info(f"   - 交易状态: Disbursed（已发放）")
    logger.info(f"   - 查询维度: 按天")
    logger.info("="*80)
    
    # 处理数据库表（先创建表）
    table_name = '利润报表'
    
    # 按天循环拉取数据
    current_date = month_start
    total_days = (month_end - month_start).days + 1
    day_count = 0
    total_records_all = 0
    
    while current_date <= month_end:
        day_count += 1
        date_str = current_date.strftime('%Y-%m-%d')
        
        logger.info("")
        logger.info("="*80)
        logger.info(f"📅 [{day_count}/{total_days}] 正在拉取 {date_str} 的数据...")
        logger.info("="*80)
        
        # 获取当天的利润报表数据
        records = await fetch_all_profit_reports(
            op_api, token_resp,
            start_date=date_str,
            end_date=date_str,
            sid_list=None,  # 不限制店铺，查询全部
            max_records=None,  # 获取所有数据
            monthly_query=False,  # 按天查询
            test_msku=None
        )
        
        logger.info(f"✅ {date_str} 共获取 {len(records)} 条利润报表数据")
        
        if not records:
            logger.warning(f"⚠️  {date_str} 没有数据返回，跳过")
            current_date += timedelta(days=1)
            continue
        
        # 显示数据概览
        logger.info(f"📊 {date_str} 数据概览：")
        logger.info(f"   - 总记录数: {len(records)} 条")
        
        # 统计各店铺数据量
        shop_counts = {}
        for record in records:
            shop = record.get('storeName', '未知')
            shop_counts[shop] = shop_counts.get(shop, 0) + 1
        
        logger.info(f"   - 店铺数量: {len(shop_counts)} 个")
        top_shops = sorted(shop_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for shop, count in top_shops:
            logger.info(f"     {shop}: {count} 条")
        
        # 检查原始数据中的重复记录
        logger.info(f"正在检查 {date_str} 数据中的重复记录...")
        unique_keys = {}
        duplicates = []
        
        for idx, record in enumerate(records):
            # 构建唯一键
            data_date = record.get('postedDateLocale', '') or record.get('reportDateMonth', '') or ''
            sid = str(record.get('sid', ''))
            msku = record.get('msku', '') or ''
            asin = record.get('asin', '') or ''
            
            # 如果为空，填"无"
            if not sid:
                sid = '无'
            if not msku:
                msku = '无'
            if not asin:
                asin = '无'
            
            key = (data_date, sid, msku, asin)
            
            if key in unique_keys:
                # 发现重复
                duplicates.append({
                    'key': key,
                    'first_index': unique_keys[key],
                    'duplicate_index': idx,
                    'first_record': records[unique_keys[key]],
                    'duplicate_record': record
                })
            else:
                unique_keys[key] = idx
        
        if duplicates:
            logger.warning(f"⚠️  发现 {len(duplicates)} 个重复的唯一键组合！")
            logger.info("="*80)
            logger.info("重复记录详情（前10个）：")
            logger.info("="*80)
            
            for i, dup in enumerate(duplicates[:10], 1):
                logger.info(f"\n重复 #{i}:")
                logger.info(f"  唯一键: 日期={dup['key'][0]}, 店铺ID={dup['key'][1]}, MSKU={dup['key'][2]}, ASIN={dup['key'][3]}")
                logger.info(f"  第一条记录索引: {dup['first_index']}")
                logger.info(f"    - 记录ID: {dup['first_record'].get('id')}")
                logger.info(f"    - 店铺名: {dup['first_record'].get('storeName')}")
                logger.info(f"    - 交易状态: {dup['first_record'].get('transactionStatus')} ({dup['first_record'].get('transactionStatusCode')})")
                logger.info(f"    - 销量: {dup['first_record'].get('totalSalesQuantity')}")
                logger.info(f"    - 销售额: {dup['first_record'].get('totalSalesAmount')}")
                logger.info(f"    - 毛利润: {dup['first_record'].get('grossProfit')}")
                
                logger.info(f"  重复记录索引: {dup['duplicate_index']}")
                logger.info(f"    - 记录ID: {dup['duplicate_record'].get('id')}")
                logger.info(f"    - 店铺名: {dup['duplicate_record'].get('storeName')}")
                logger.info(f"    - 交易状态: {dup['duplicate_record'].get('transactionStatus')} ({dup['duplicate_record'].get('transactionStatusCode')})")
                logger.info(f"    - 销量: {dup['duplicate_record'].get('totalSalesQuantity')}")
                logger.info(f"    - 销售额: {dup['duplicate_record'].get('totalSalesAmount')}")
                logger.info(f"    - 毛利润: {dup['duplicate_record'].get('grossProfit')}")
            
            if len(duplicates) > 10:
                logger.info(f"\n... 还有 {len(duplicates) - 10} 个重复记录未显示")
            
            logger.info("="*80)
        else:
            logger.info(f"✅ 未发现重复记录，所有 {len(records)} 条记录的唯一键都是唯一的")
        
        # 转换数据格式
        logger.info(f"正在转换 {date_str} 数据格式...")
        report_data_list = convert_profit_report_data(records, sid_to_name_map)
        logger.info(f"转换完成: {len(report_data_list)} 条")
        
        if not report_data_list:
            logger.warning(f"⚠️  {date_str} 没有数据需要保存，跳过")
            current_date += timedelta(days=1)
            continue
        
        try:
            # 第一天创建表
            if day_count == 1:
                logger.info(f"正在创建数据库表 {table_name}...")
                create_table_if_needed(table_name, report_data_list[0])
            
            # 插入数据（使用ON DUPLICATE KEY UPDATE）
            logger.info(f"正在写入 {date_str} 数据到数据库...")
            inserted, updated = insert_data_batch(table_name, report_data_list)
            
            total_records_all += len(report_data_list)
            
            logger.info(f"✅ {date_str} 数据写入完成: 新增~{inserted}条, 更新~{updated}条")
            
        except Exception as e:
            logger.error(f"❌ {date_str} 数据库操作失败: {e}", exc_info=True)
            # 继续处理下一天
        
        # 移动到下一天
        current_date += timedelta(days=1)
        
        # 每天之间稍作延迟，避免请求过快
        if current_date <= month_end:
            await asyncio.sleep(1)
    
    # 所有数据处理完成后，输出总体统计
    logger.info("")
    logger.info("="*80)
    logger.info("📊 总体统计信息：")
    logger.info(f"  处理天数: {day_count} 天")
    logger.info(f"  总记录数: {total_records_all} 条")
    
    # 查询数据库获取最终统计
    try:
        with db_cursor() as cursor:
            # 统计总记录数
            cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
            total_in_db = cursor.fetchone()['total']
            
            # 统计各店铺记录数
            cursor.execute(f"""
                SELECT 
                    `店铺`, 
                    COUNT(*) as count
                FROM `{table_name}`
                GROUP BY `店铺`
                ORDER BY count DESC
            """)
            shop_stats = cursor.fetchall()
            
            # 统计各日期记录数
            cursor.execute(f"""
                SELECT 
                    `统计日期`, 
                    COUNT(*) as count
                FROM `{table_name}`
                WHERE `统计日期` >= '{month_start.strftime('%Y-%m-%d')}'
                GROUP BY `统计日期`
                ORDER BY `统计日期` DESC
            """)
            date_stats = cursor.fetchall()
            
            logger.info(f"  数据库总记录: {total_in_db} 条")
            logger.info("  各店铺统计：")
            for shop in shop_stats[:10]:  # 只显示前10个店铺
                logger.info(f"    {shop['店铺']}: {shop['count']} 条记录")
            if len(shop_stats) > 10:
                logger.info(f"    ... 还有 {len(shop_stats) - 10} 个店铺")
            logger.info(f"  各日期统计：")
            for date_stat in date_stats:
                logger.info(f"    {date_stat['统计日期']}: {date_stat['count']} 条记录")
    except Exception as e:
        logger.warning(f"查询数据库统计失败: {e}")
    
    logger.info("="*80)
    logger.info("✅ 数据采集全部完成！")
    logger.info("="*80)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='利润报表数据采集')
    parser.add_argument('--start-date', type=str, default=None,
                       help='开始日期，格式：Y-m-d，默认：1月1号')
    parser.add_argument('--end-date', type=str, default=None,
                       help='结束日期，格式：Y-m-d，默认：今天')
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main(start_date=args.start_date, end_date=args.end_date))
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断执行")
    except Exception as e:
        logger.error(f"\n❌ 执行失败: {e}", exc_info=True)

