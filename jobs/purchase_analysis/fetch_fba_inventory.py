#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
FBA仓库库存数据采集任务
从领星API获取FBA仓库库存数据并存入数据库
API: /basicOpen/openapi/storage/fbaWarehouseDetail
"""
import asyncio
import json
from typing import List, Dict, Any, Tuple

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase
from utils import normalize_shop_name
from .shop_mapping import get_shop_mapping

# 获取日志记录器
logger = get_logger('fba_inventory')

# 重试配置（优先保证数据完整性）
# 根据领星令牌桶算法优化：
# - 令牌桶维度：appId + 接口url
# - 每个请求消耗1个令牌
# - 令牌桶容量：1（非常严格，需要更长的请求间隔）
# - 令牌在请求完成/异常/超时(2min)后自动回收
# - 无令牌时返回错误码3001008
MAX_RETRIES = 5  # 最大重试次数
RETRY_DELAY = 10  # 重试延迟（秒）
REQUEST_DELAY = 3  # 请求间隔（秒）- 令牌桶容量只有1，需要更长的间隔
TOKEN_BUCKET_CAPACITY = 1  # 令牌桶容量（官方文档说明）


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
                "/basicOpen/openapi/storage/fbaWarehouseDetail", 
                "POST", 
                req_body=req_body
            )
            
            # 兼容Pydantic v1和v2
            try:
                result = resp.model_dump()  # Pydantic v2
            except AttributeError:
                result = resp.dict()  # Pydantic v1
            
            code = result.get('code', 0)
            message = result.get('message', '')
            
            # 检查是否请求过于频繁（使用指数退避）
            if code == 3001008:  # 请求过于频繁（令牌桶无令牌）
                was_rate_limited = True
                wait_time = RETRY_DELAY * (2 ** retry)  # 指数退避：10, 20, 40, 80, 160秒
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒让令牌回收...")
                await asyncio.sleep(wait_time)
                continue
            
            # 检查是否token过期
            # 错误码：401, 403, 2001003, 3001001, 3001002 都表示Token过期或无效
            if code in [401, 403, 2001003, 3001001, 3001002]:  # token相关错误
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
            
            items = result.get('data', [])
            total = result.get('total', 0)
            
            # 确保items不为None
            if items is None:
                items = []
            
            # 成功获取数据
            if retry > 0:
                logger.info(f"✅ 重试成功！")
            
            return items, total, "", was_rate_limited
            
        except Exception as e:
            logger.error(f"❌ 请求异常（第 {retry + 1}/{MAX_RETRIES} 次尝试）: {e}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"❌ 达到最大重试次数，请求失败")
                return [], 0, f"EXCEPTION: {str(e)}", was_rate_limited
    
    return [], 0, "MAX_RETRIES_EXCEEDED", was_rate_limited


async def fetch_all_fba_inventory(op_api: OpenApiBase, token_resp, 
                                   limit: int = None) -> List[Dict[str, Any]]:
    """
    获取所有FBA库存数据（分页处理，支持重试和token刷新）
    
    Args:
        op_api: OpenAPI客户端
        token_resp: 访问令牌响应对象（包含access_token和refresh_token）
        limit: 限制获取的数据条数（用于测试，None表示获取全部）
        
    Returns:
        List[Dict[str, Any]]: FBA库存列表
    """
    all_items = []
    offset = 0
    length = 200  # 每页200条
    token = token_resp.access_token
    total_records = None
    
    # 令牌桶感知：动态调整请求间隔
    current_delay = REQUEST_DELAY  # 当前请求间隔
    consecutive_success = 0  # 连续成功次数
    consecutive_rate_limited = 0  # 连续限流次数
    
    while True:
        req_body = {
            "offset": offset,
            "length": length,
            "search_field": "seller_sku"  # 以seller_sku维度获取数据
        }
        
        # 带重试机制获取数据（令牌桶感知）
        items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
        
        # 处理token过期
        if error == "TOKEN_EXPIRED":
            logger.info("Token已过期，正在刷新...")
            try:
                token_resp = await op_api.generate_access_token()
                token = token_resp.access_token
                logger.info(f"Token刷新成功，有效期: {token_resp.expires_in}秒")
                # 重试当前页
                items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
            except Exception as e:
                logger.error(f"Token刷新失败: {e}")
                break
        
        # 处理其他错误
        if error and error != "TOKEN_EXPIRED":
            logger.error(f"获取 offset={offset} 数据失败: {error}")
            break
        
        # 令牌桶感知：动态调整请求间隔
        if was_rate_limited:
            # 遇到限流，增加间隔（给令牌更多时间回收）
            consecutive_rate_limited += 1
            consecutive_success = 0
            # 间隔递增：3秒 → 5秒 → 7秒 → 10秒（最大10秒）
            current_delay = min(REQUEST_DELAY + consecutive_rate_limited * 2, 10)
            logger.info(f"🪣 检测到限流，调整请求间隔为 {current_delay} 秒")
        else:
            # 成功获取，重置限流计数
            consecutive_rate_limited = 0
            consecutive_success += 1
            # 连续成功3次后，可以稍微减少间隔（但不少于基础间隔）
            if consecutive_success >= 3:
                current_delay = max(REQUEST_DELAY - 0.5, 2.5)
                consecutive_success = 0  # 重置计数
            else:
                current_delay = REQUEST_DELAY
        
        # 记录总数
        if total_records is None and total > 0:
            total_records = total
            logger.info(f"📊 总共需要获取 {total_records} 条数据")
        
        logger.info(f"✓ offset={offset}: 获取 {len(items)} 条数据")
        
        if not items:
            if offset == 0:
                logger.warning("第一页就没有数据")
            break
        
        # 如果指定了限制，只添加需要的数据
        if limit:
            remaining = limit - len(all_items)
            if remaining > 0:
                all_items.extend(items[:remaining])
                if len(all_items) >= limit:
                    logger.info(f"✅ 已达到限制数量 {limit} 条，停止获取")
                    break
        else:
            all_items.extend(items)
        
        # 显示进度
        if total_records:
            progress = (len(all_items) / total_records) * 100
            logger.info(f"📈 进度: {len(all_items)}/{total_records} ({progress:.1f}%)")
        
        # 如果已经获取所有数据，退出循环
        if total_records and len(all_items) >= total_records:
            logger.info("✅ 所有数据获取完成")
            break
        
        offset += length
        
        # 请求间隔延时（动态调整，令牌桶感知）
        await asyncio.sleep(current_delay)
    
    # 数据完整性检查
    if limit:
        logger.info(f"✅ 获取了 {len(all_items)} 条数据（限制: {limit} 条）")
    elif total_records and len(all_items) < total_records:
        logger.warning(f"⚠️  数据可能不完整: 预期 {total_records} 条，实际获取 {len(all_items)} 条")
        logger.warning(f"   缺失: {total_records - len(all_items)} 条数据")
    elif total_records:
        logger.info(f"✅ 数据完整性验证通过: {len(all_items)}/{total_records} 条")
    
    return all_items


def convert_fba_data(items: List[Dict[str, Any]], 
                     sid_to_name_map: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """
    转换FBA库存数据格式
    
    Args:
        items: 原始FBA库存数据列表
        sid_to_name_map: 店铺ID到店铺名称的映射
        
    Returns:
        List[Dict[str, Any]]: 转换后的数据列表
    """
    fba_list = []
    
    for item in items:
        # SKU
        sku = item.get('sku', '') or '无'
        
        # MSKU (seller_sku)
        msku = item.get('seller_sku', '') or '无'
        
        # 店铺名称（从sid转换）
        sid = item.get('sid', 0)
        if sid and sid_to_name_map:
            shop_name = sid_to_name_map.get(str(sid), f'店铺{sid}')
        else:
            shop_name = item.get('seller_group_name', '') or ''
        
        # 规范化店铺名称
        if shop_name:
            shop_name = normalize_shop_name(shop_name)
        
        # 如果店铺为空或None，填充为"无"
        if not shop_name or shop_name.strip() == '':
            shop_name = '无'
        
        # 仓库名称
        warehouse_name = item.get('name', '') or '无'
        
        # ASIN
        asin = item.get('asin', '') or '无'
        
        # 品名
        product_name = item.get('product_name', '') or '无'
        
        # FNSKU
        fnsku = item.get('fnsku', '') or '无'
        
        # 总数
        total = item.get('total', 0)
        try:
            total = int(total)
        except:
            total = 0
        
        # FBA可售
        afn_fulfillable_quantity = item.get('afn_fulfillable_quantity', 0)
        try:
            afn_fulfillable_quantity = int(afn_fulfillable_quantity)
        except:
            afn_fulfillable_quantity = 0
        
        # 可用总数
        available_total = item.get('available_total', 0)
        try:
            available_total = int(available_total)
        except:
            available_total = 0
        
        # 待调仓
        reserved_fc_transfers = item.get('reserved_fc_transfers', 0)
        try:
            reserved_fc_transfers = int(reserved_fc_transfers)
        except:
            reserved_fc_transfers = 0
        
        # 调仓中
        reserved_fc_processing = item.get('reserved_fc_processing', 0)
        try:
            reserved_fc_processing = int(reserved_fc_processing)
        except:
            reserved_fc_processing = 0
        
        # 待发货
        reserved_customerorders = item.get('reserved_customerorders', 0)
        try:
            reserved_customerorders = int(reserved_customerorders)
        except:
            reserved_customerorders = 0
        
        # 不可售
        afn_unsellable_quantity = item.get('afn_unsellable_quantity', 0)
        try:
            afn_unsellable_quantity = int(afn_unsellable_quantity)
        except:
            afn_unsellable_quantity = 0
        
        # 计划入库
        afn_inbound_working_quantity = item.get('afn_inbound_working_quantity', 0)
        try:
            afn_inbound_working_quantity = int(afn_inbound_working_quantity)
        except:
            afn_inbound_working_quantity = 0
        
        # 在途
        afn_inbound_shipped_quantity = item.get('afn_inbound_shipped_quantity', 0)
        try:
            afn_inbound_shipped_quantity = int(afn_inbound_shipped_quantity)
        except:
            afn_inbound_shipped_quantity = 0
        
        # 入库中
        afn_inbound_receiving_quantity = item.get('afn_inbound_receiving_quantity', 0)
        try:
            afn_inbound_receiving_quantity = int(afn_inbound_receiving_quantity)
        except:
            afn_inbound_receiving_quantity = 0
        
        # 实际在途
        stock_up_num = item.get('stock_up_num', 0)
        try:
            stock_up_num = int(stock_up_num)
        except:
            stock_up_num = 0
        
        # 总可用库存
        total_fulfillable_quantity = item.get('total_fulfillable_quantity', 0)
        try:
            total_fulfillable_quantity = int(total_fulfillable_quantity)
        except:
            total_fulfillable_quantity = 0
        
        # 库龄字段（各个时间段的库龄）
        inv_age_0_to_30 = item.get('inv_age_0_to_30_days', 0) or 0
        inv_age_31_to_60 = item.get('inv_age_31_to_60_days', 0) or 0
        inv_age_61_to_90 = item.get('inv_age_61_to_90_days', 0) or 0
        inv_age_0_to_90 = item.get('inv_age_0_to_90_days', 0) or 0
        inv_age_91_to_180 = item.get('inv_age_91_to_180_days', 0) or 0
        inv_age_181_to_270 = item.get('inv_age_181_to_270_days', 0) or 0
        inv_age_271_to_330 = item.get('inv_age_271_to_330_days', 0) or 0
        inv_age_271_to_365 = item.get('inv_age_271_to_365_days', 0) or 0
        inv_age_331_to_365 = item.get('inv_age_331_to_365_days', 0) or 0
        inv_age_365_plus = item.get('inv_age_365_plus_days', 0) or 0
        
        try:
            inv_age_0_to_30 = int(inv_age_0_to_30)
        except:
            inv_age_0_to_30 = 0
        try:
            inv_age_31_to_60 = int(inv_age_31_to_60)
        except:
            inv_age_31_to_60 = 0
        try:
            inv_age_61_to_90 = int(inv_age_61_to_90)
        except:
            inv_age_61_to_90 = 0
        try:
            inv_age_0_to_90 = int(inv_age_0_to_90)
        except:
            inv_age_0_to_90 = 0
        try:
            inv_age_91_to_180 = int(inv_age_91_to_180)
        except:
            inv_age_91_to_180 = 0
        try:
            inv_age_181_to_270 = int(inv_age_181_to_270)
        except:
            inv_age_181_to_270 = 0
        try:
            inv_age_271_to_330 = int(inv_age_271_to_330)
        except:
            inv_age_271_to_330 = 0
        try:
            inv_age_271_to_365 = int(inv_age_271_to_365)
        except:
            inv_age_271_to_365 = 0
        try:
            inv_age_331_to_365 = int(inv_age_331_to_365)
        except:
            inv_age_331_to_365 = 0
        try:
            inv_age_365_plus = int(inv_age_365_plus)
        except:
            inv_age_365_plus = 0
        
        fba_record = {
            'SKU': sku,
            'MSKU': msku,
            '店铺': shop_name,
            '仓库': warehouse_name,
            'ASIN': asin,
            '品名': product_name,
            'FNSKU': fnsku,
            '总数': total,
            'FBA可售': afn_fulfillable_quantity,
            '可用总数': available_total,
            '待调仓': reserved_fc_transfers,
            '调仓中': reserved_fc_processing,
            '待发货': reserved_customerorders,
            '不可售': afn_unsellable_quantity,
            '计划入库': afn_inbound_working_quantity,
            '在途': afn_inbound_shipped_quantity,
            '入库中': afn_inbound_receiving_quantity,
            '实际在途': stock_up_num,
            '总可用库存': total_fulfillable_quantity,
            '库龄0-1个月': inv_age_0_to_30,
            '库龄1-2个月': inv_age_31_to_60,
            '库龄2-3个月': inv_age_61_to_90,
            '库龄0-3个月': inv_age_0_to_90,
            '库龄3-6个月': inv_age_91_to_180,
            '库龄6-9个月': inv_age_181_to_270,
            '库龄9-11个月': inv_age_271_to_330,
            '库龄9-12个月': inv_age_271_to_365,
            '库龄11-12个月': inv_age_331_to_365,
            '库龄12个月以上': inv_age_365_plus,
        }
        
        fba_list.append(fba_record)
    
    return fba_list


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any]) -> None:
    """
    创建或重建数据表
    
    Args:
        table_name: 表名
        sample_row: 样本数据行
    """
    with db_cursor(dictionary=False) as cursor:
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()
        
        if exists:
            # 检查表结构是否匹配
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
            expected = ['id'] + list(sample_row.keys())
            
            if columns == expected:
                logger.info(f"表 {table_name} 结构正确")
                return
            else:
                logger.warning(f"表 {table_name} 结构不符，正在重建...")
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # 创建表
        fields = []
        for k, v in sample_row.items():
            if isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql})"
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def delete_all_data(table_name: str) -> int:
    """
    删除表中的所有数据（全量更新）
    
    Args:
        table_name: 表名
        
    Returns:
        int: 删除的记录数
    """
    with db_cursor(dictionary=False) as cursor:
        cursor.execute(f"DELETE FROM `{table_name}`")
        deleted_count = cursor.rowcount
        logger.info(f"已删除 {deleted_count} 条旧数据")
        return deleted_count


def insert_data_batch(table_name: str, data_list: List[Dict[str, Any]]) -> None:
    """
    批量插入数据
    
    Args:
        table_name: 表名
        data_list: 数据列表
    """
    if not data_list:
        return
    
    with db_cursor(dictionary=False) as cursor:
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `{table_name}` ({fields}) VALUES ({values_placeholder})"
        
        batch_size = 200
        for i in range(0, len(data_list), batch_size):
            batch = [tuple(row.values()) for row in data_list[i:i+batch_size]]
            cursor.executemany(sql, batch)
            logger.info(f"已录入 {min(i+batch_size, len(data_list))} 条...")
    
    logger.info(f"成功写入 {len(data_list)} 条数据到表 {table_name}")


async def main():
    """
    主函数（全量更新FBA库存数据到数据库）
    """
    logger.info("="*80)
    logger.info("FBA仓库库存数据采集（全量更新）")
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
    
    # 获取店铺映射
    logger.info("正在获取店铺映射...")
    try:
        shop_mapping = await get_shop_mapping()
        sid_to_name_map = {str(k): v for k, v in shop_mapping.items()}
        logger.info(f"✅ 共获取 {len(sid_to_name_map)} 个店铺映射")
    except Exception as e:
        logger.warning(f"获取店铺映射失败: {e}")
        sid_to_name_map = {}
    
    # 获取FBA库存数据（全量）
    logger.info("正在获取FBA库存数据（全量）...")
    logger.info(f"⏱️  配置参数（优先保证数据完整性）:")
    logger.info(f"   - 请求间隔: {REQUEST_DELAY}秒（令牌桶容量只有1，需要更长间隔）")
    logger.info(f"   - 最大重试: {MAX_RETRIES}次")
    logger.info(f"   - 重试延迟: {RETRY_DELAY}秒（指数退避）")
    logger.info("="*80)
    
    try:
        items = await fetch_all_fba_inventory(op_api, token_resp, limit=None)
        
        if not items:
            logger.warning("⚠️  没有获取到FBA库存数据")
            return
        
        logger.info(f"✅ 共获取 {len(items)} 条FBA库存数据")
        
        # 转换数据格式
        logger.info("正在转换数据格式...")
        fba_data_list = convert_fba_data(items, sid_to_name_map)
        logger.info(f"共生成 {len(fba_data_list)} 条FBA库存记录")
        
        if not fba_data_list:
            logger.warning("没有数据需要保存")
            return
        
        # 处理数据库
        table_name = 'FBA库存明细'
        logger.info(f"正在处理数据库表 {table_name}...")
        
        try:
            # 创建或检查表结构
            create_table_if_needed(table_name, fba_data_list[0])
            
            # 删除所有旧数据（全量更新）
            logger.info("正在删除所有旧数据（全量更新）...")
            deleted_count = delete_all_data(table_name)
            
            # 插入新数据
            logger.info("正在写入新数据...")
            insert_data_batch(table_name, fba_data_list)
            
            # 输出统计信息
            logger.info("="*80)
            logger.info("📊 统计信息：")
            logger.info(f"  更新策略: 全量更新")
            logger.info(f"  删除旧记录: {deleted_count} 条")
            logger.info(f"  原始数据: {len(items)} 条")
            logger.info(f"  新增记录: {len(fba_data_list)} 条")
            
            # 查询数据库获取最终统计
            try:
                with db_cursor() as cursor:
                    # 统计总记录数
                    cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
                    total_in_db = cursor.fetchone()['total']
                    
                    # 统计总库存和各个时间段的库龄
                    cursor.execute(f"""
                        SELECT 
                            SUM(`总数`) as total_count,
                            SUM(`FBA可售`) as total_fulfillable,
                            SUM(`可用总数`) as total_available,
                            SUM(`库龄0-1个月`) as inv_age_0_30,
                            SUM(`库龄1-2个月`) as inv_age_31_60,
                            SUM(`库龄2-3个月`) as inv_age_61_90,
                            SUM(`库龄0-3个月`) as inv_age_0_90,
                            SUM(`库龄3-6个月`) as inv_age_91_180,
                            SUM(`库龄6-9个月`) as inv_age_181_270,
                            SUM(`库龄9-11个月`) as inv_age_271_330,
                            SUM(`库龄9-12个月`) as inv_age_271_365,
                            SUM(`库龄11-12个月`) as inv_age_331_365,
                            SUM(`库龄12个月以上`) as inv_age_365_plus
                        FROM `{table_name}`
                    """)
                    stats = cursor.fetchone()
                    total_count = stats['total_count'] or 0
                    total_fulfillable = stats['total_fulfillable'] or 0
                    total_available = stats['total_available'] or 0
                    inv_age_0_30 = stats['inv_age_0_30'] or 0
                    inv_age_31_60 = stats['inv_age_31_60'] or 0
                    inv_age_61_90 = stats['inv_age_61_90'] or 0
                    inv_age_0_90 = stats['inv_age_0_90'] or 0
                    inv_age_91_180 = stats['inv_age_91_180'] or 0
                    inv_age_181_270 = stats['inv_age_181_270'] or 0
                    inv_age_271_330 = stats['inv_age_271_330'] or 0
                    inv_age_271_365 = stats['inv_age_271_365'] or 0
                    inv_age_331_365 = stats['inv_age_331_365'] or 0
                    inv_age_365_plus = stats['inv_age_365_plus'] or 0
                    
                    # 统计各店铺记录数
                    cursor.execute(f"""
                        SELECT 
                            `店铺`, 
                            COUNT(*) as count, 
                            SUM(`总数`) as total_count,
                            SUM(`FBA可售`) as total_fulfillable
                        FROM `{table_name}`
                        GROUP BY `店铺`
                        ORDER BY count DESC
                    """)
                    shop_stats = cursor.fetchall()
                    
                    logger.info(f"  数据库总记录: {total_in_db} 条")
                    logger.info(f"  总库存数: {total_count}")
                    logger.info(f"  总FBA可售: {total_fulfillable}")
                    logger.info(f"  总可用数: {total_available}")
                    logger.info("  各时间段库龄统计：")
                    logger.info(f"    0-1个月: {inv_age_0_30}")
                    logger.info(f"    1-2个月: {inv_age_31_60}")
                    logger.info(f"    2-3个月: {inv_age_61_90}")
                    logger.info(f"    0-3个月: {inv_age_0_90}")
                    logger.info(f"    3-6个月: {inv_age_91_180}")
                    logger.info(f"    6-9个月: {inv_age_181_270}")
                    logger.info(f"    9-11个月: {inv_age_271_330}")
                    logger.info(f"    9-12个月: {inv_age_271_365}")
                    logger.info(f"    11-12个月: {inv_age_331_365}")
                    logger.info(f"    12个月以上: {inv_age_365_plus}")
                    logger.info("  各店铺统计：")
                    for shop in shop_stats:
                        logger.info(f"    {shop['店铺']}: {shop['count']} 条记录, "
                                  f"总数={shop['total_count']}, "
                                  f"FBA可售={shop['total_fulfillable']}")
            except Exception as e:
                logger.warning(f"查询数据库统计失败: {e}")
            
            logger.info("="*80)
            logger.info("✅ 数据采集完成！")
            logger.info("="*80)
            
        except Exception as e:
            logger.error(f"数据库操作失败: {e}", exc_info=True)
            raise
        
    except Exception as e:
        logger.error(f"❌ 获取FBA库存数据失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    # 全量更新FBA库存数据到数据库
    asyncio.run(main())

