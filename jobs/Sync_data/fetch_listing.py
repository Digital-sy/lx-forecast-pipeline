#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
亚马逊Listing数据采集任务
从领星API获取亚马逊Listing数据并存入数据库
API: /erp/sc/data/mws/listing
唯一键：sid + seller_sku
"""
import asyncio
import json
from typing import List, Dict, Any, Tuple
from datetime import datetime
import re

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase
from utils import normalize_shop_name
from ..purchase_analysis.shop_mapping import get_shop_mapping

# 获取日志记录器
logger = get_logger('listing')

# 重试配置（令牌桶容量为1，需要更谨慎）
MAX_RETRIES = 5  # 最大重试次数
RETRY_DELAY = 10  # 重试延迟（秒）
REQUEST_DELAY = 3  # 请求间隔（秒）- 令牌桶容量为1，需要更长的间隔
TOKEN_BUCKET_CAPACITY = 1  # 令牌桶容量


def convert_to_beijing_time(time_str: str, is_utc: bool = False) -> str:
    """
    将时间字符串转换为北京时间（UTC+8）
    
    Args:
        time_str: 时间字符串
        is_utc: 是否为UTC时间（零时区）
        
    Returns:
        str: 北京时间字符串，格式：YYYY-MM-DD HH:MM:SS
    """
    if not time_str:
        return ''
    
    try:
        # 处理各种时间格式
        # 格式1: 2021-02-04 01:15:58 PST
        # 格式2: 2021-02-04 01:15:58 -08:00
        # 格式3: 2021-03-14 06:53:24 (UTC时间)
        # 格式4: 2022-02-23 18:10:45 (已经是北京时间)
        # 格式5: 2023-01-11 (只有日期)
        
        # 如果是UTC时间，直接加8小时
        if is_utc:
            # 尝试解析UTC时间格式
            try:
                # 格式：YYYY-MM-DD HH:MM:SS
                dt = datetime.strptime(time_str.strip(), '%Y-%m-%d %H:%M:%S')
                # UTC转北京时间（+8小时）
                from datetime import timedelta
                beijing_dt = dt + timedelta(hours=8)
                return beijing_dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                # 如果解析失败，尝试其他格式
                pass
        
        # 处理带时区信息的格式
        # 格式：2021-02-04 01:15:58 -08:00
        timezone_pattern = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([+-]\d{2}):(\d{2})'
        match = re.match(timezone_pattern, time_str.strip())
        if match:
            dt_str, tz_hour, tz_min = match.groups()
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
            # 计算时区偏移（小时）
            tz_offset = int(tz_hour) + (int(tz_min) / 60.0)
            # 转换为北京时间（UTC+8）
            from datetime import timedelta
            beijing_dt = dt - timedelta(hours=tz_offset) + timedelta(hours=8)
            return beijing_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 处理PST等时区标识
        if 'PST' in time_str or 'PDT' in time_str:
            # PST是UTC-8，PDT是UTC-7
            # 提取时间部分
            dt_str = re.sub(r'\s+(PST|PDT)', '', time_str.strip())
            try:
                dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                # PST转北京时间：+16小时，PDT转北京时间：+15小时
                from datetime import timedelta
                offset = 16 if 'PST' in time_str else 15
                beijing_dt = dt + timedelta(hours=offset)
                return beijing_dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        # 尝试标准格式（假设已经是北京时间或需要转换）
        try:
            # 格式：YYYY-MM-DD HH:MM:SS
            dt = datetime.strptime(time_str.strip(), '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
        
        # 格式：YYYY-MM-DD（只有日期）
        try:
            dt = datetime.strptime(time_str.strip(), '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
        
        # 如果都解析失败，返回原字符串
        return time_str
        
    except Exception as e:
        logger.warning(f"时间转换失败: {time_str}, 错误: {e}")
        return time_str


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
                "/erp/sc/data/mws/listing", 
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


async def fetch_all_listings(op_api: OpenApiBase, token_resp, 
                             sid_list: List[str] = None,
                             max_records: int = None) -> List[Dict[str, Any]]:
    """
    获取所有Listing数据（分页处理，支持重试和token刷新）
    
    Args:
        op_api: OpenAPI客户端
        token_resp: 访问令牌响应对象（包含access_token和refresh_token）
        sid_list: 店铺ID列表（可选，如果为None则查询所有店铺）
        max_records: 最大抓取记录数（可选，用于测试，如果为None则抓取全部）
        
    Returns:
        List[Dict[str, Any]]: Listing列表
    """
    all_items = []
    offset = 0
    length = 1000  # 每页最多1000条（API上限）
    token = token_resp.access_token
    total_records = None
    
    # 令牌桶感知：动态调整请求间隔
    current_delay = REQUEST_DELAY  # 当前请求间隔
    consecutive_success = 0  # 连续成功次数
    consecutive_rate_limited = 0  # 连续限流次数
    
    # 构建请求参数
    req_body = {
        "offset": offset,
        "length": length,
        "is_delete": 0  # 只查询未删除的
    }
    
    # 如果指定了店铺ID列表，添加到请求参数
    if sid_list:
        req_body["sid"] = ",".join(sid_list)
    
    while True:
        req_body["offset"] = offset
        
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
            logger.error(f"获取offset={offset}数据失败: {error}")
            break
        
        # 记录总数
        if total_records is None and total > 0:
            total_records = total
            logger.info(f"📊 总共需要获取 {total_records} 条数据")
        
        logger.info(f"✓ offset={offset}: 获取 {len(items)} 条数据")
        
        if not items:
            if offset == 0:
                logger.warning("第一页就没有数据")
            break
        
        all_items.extend(items)
        
        # 测试模式：如果设置了最大记录数，达到后退出
        if max_records and len(all_items) >= max_records:
            logger.info(f"🧪 测试模式：已达到最大记录数 {max_records} 条，停止抓取")
            all_items = all_items[:max_records]  # 确保不超过限制
            break
        
        # 显示进度
        if total_records:
            progress = (len(all_items) / total_records) * 100
            logger.info(f"📈 进度: {len(all_items)}/{total_records} ({progress:.1f}%)")
        elif max_records:
            logger.info(f"📈 进度: {len(all_items)}/{max_records} (测试模式)")
        
        # 如果已经获取所有数据，退出循环
        if total_records and len(all_items) >= total_records:
            logger.info("✅ 所有数据获取完成")
            break
        
        # 如果返回的数据少于length，说明已经是最后一页
        if len(items) < length:
            break
        
        offset += length
        
        # 令牌桶感知：动态调整请求间隔
        if was_rate_limited:
            # 遇到限流，增加间隔（给令牌更多时间回收）
            consecutive_rate_limited += 1
            consecutive_success = 0
            # 间隔递增：3秒 → 4秒 → 5秒 → 6秒（最大6秒）
            current_delay = min(REQUEST_DELAY + consecutive_rate_limited, 6)
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
        
        # 请求间隔延时（动态调整，令牌桶感知）
        await asyncio.sleep(current_delay)
    
    # 数据完整性检查
    if total_records and len(all_items) < total_records:
        logger.warning(f"⚠️  数据可能不完整: 预期 {total_records} 条，实际获取 {len(all_items)} 条")
        logger.warning(f"   缺失: {total_records - len(all_items)} 条数据")
    elif total_records:
        logger.info(f"✅ 数据完整性验证通过: {len(all_items)}/{total_records} 条")
    
    return all_items


def convert_listing_data(items: List[Dict[str, Any]], 
                         sid_to_name_map: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """
    转换Listing数据
    
    Args:
        items: Listing列表
        sid_to_name_map: 店铺ID到店铺名称的映射（可选）
        
    Returns:
        List[Dict[str, Any]]: 转换后的数据列表
    """
    listing_list = []
    debug_count = 0  # 调试计数器，只打印前3条数据的详细信息
    
    for item in items:
        # 调试：打印前3条数据的原始字段值
        if debug_count < 3:
            logger.debug(f"=== 调试数据 #{debug_count + 1} ===")
            logger.debug(f"principal_info 原始值: {item.get('principal_info', 'N/A')}")
            logger.debug(f"dimension_info 原始值: {item.get('dimension_info', 'N/A')}")
            logger.debug(f"first_order_time 原始值: {item.get('first_order_time', 'N/A')}")
            logger.debug(f"on_sale_time 原始值: {item.get('on_sale_time', 'N/A')}")
            debug_count += 1
        # 店铺ID和店铺名
        sid = item.get('sid', '')
        if sid and sid_to_name_map:
            shop_name = sid_to_name_map.get(str(sid), '无')
            shop_name = normalize_shop_name(shop_name) if shop_name != '无' else '无'
        else:
            shop_name = '无'
        
        # 基础字段
        seller_sku = item.get('seller_sku', '') or ''  # MSKU
        fnsku = item.get('fnsku', '') or ''  # FNSKU
        asin = item.get('asin', '') or ''  # ASIN
        parent_asin = item.get('parent_asin', '') or ''  # 父ASIN
        small_image_url = item.get('small_image_url', '') or ''  # 商品缩略图地址
        # 去掉图片URL中的._SL75_等尺寸参数
        if small_image_url:
            # 使用正则表达式去掉所有 ._SL数字_ 格式的参数（替换为空字符串）
            small_image_url = re.sub(r'\._SL\d+_', '', small_image_url)
        status = item.get('status', 0)  # 状态：0 停售，1 在售
        item_name = item.get('item_name', '') or ''  # 标题
        local_sku = item.get('local_sku', '') or ''  # 本地产品SKU
        local_name = item.get('local_name', '') or ''  # 品名
        afn_fulfillable_quantity = item.get('afn_fulfillable_quantity', 0) or 0  # FBA可售
        seller_brand = item.get('seller_brand', '') or ''  # 亚马逊品牌
        
        # 时间字段（转换为北京时间）
        open_date_display = item.get('open_date_display', '') or ''  # 商品创建时间
        if open_date_display:
            open_date_display = convert_to_beijing_time(open_date_display)
        
        listing_update_date = item.get('listing_update_date', '') or ''  # All Listing报表更新时间（UTC）
        if listing_update_date:
            listing_update_date = convert_to_beijing_time(listing_update_date, is_utc=True)
        
        first_order_time = item.get('first_order_time', '') or ''  # 首单时间
        if first_order_time:
            # 首单时间只有日期，格式：YYYY-MM-DD
            try:
                first_order_time_str = str(first_order_time).strip()
                if first_order_time_str:
                    dt = datetime.strptime(first_order_time_str, '%Y-%m-%d')
                    first_order_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    first_order_time = ''
            except Exception as e:
                logger.debug(f"首单时间解析失败: {first_order_time}, 错误: {e}")
                first_order_time = str(first_order_time) if first_order_time else ''
        
        on_sale_time = item.get('on_sale_time', '') or ''  # 开售时间
        if on_sale_time:
            # 开售时间只有日期，格式：YYYY-MM-DD
            try:
                on_sale_time_str = str(on_sale_time).strip()
                if on_sale_time_str:
                    dt = datetime.strptime(on_sale_time_str, '%Y-%m-%d')
                    on_sale_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    on_sale_time = ''
            except Exception as e:
                logger.debug(f"开售时间解析失败: {on_sale_time}, 错误: {e}")
                on_sale_time = str(on_sale_time) if on_sale_time else ''
        
        # 负责人信息
        principal_info = item.get('principal_info', None)
        principal_name = ''
        if principal_info:
            if isinstance(principal_info, dict):
                principal_name = principal_info.get('principal_name', '') or ''
            elif isinstance(principal_info, list) and len(principal_info) > 0:
                # 如果是列表，取第一个元素的principal_name
                first_principal = principal_info[0]
                if isinstance(first_principal, dict):
                    principal_name = first_principal.get('principal_name', '') or ''
        
        # 销量字段
        yesterday_volume = item.get('yesterday_volume', '0') or '0'  # 销量-昨天
        try:
            yesterday_volume = int(float(yesterday_volume))
        except:
            yesterday_volume = 0
        
        fourteen_volume = item.get('fourteen_volume', '0') or '0'  # 销量-14天
        try:
            fourteen_volume = int(float(fourteen_volume))
        except:
            fourteen_volume = 0
        
        thirty_volume = item.get('thirty_volume', '0') or '0'  # 销量-30天
        try:
            thirty_volume = int(float(thirty_volume))
        except:
            thirty_volume = 0
        
        total_volume = item.get('total_volume', '0') or '0'  # 销量-7天
        try:
            total_volume = int(float(total_volume))
        except:
            total_volume = 0
        
        # 商品重量
        dimension_info = item.get('dimension_info', None)
        item_weight = ''
        if dimension_info:
            if isinstance(dimension_info, dict):
                item_weight = dimension_info.get('item_weight', '') or ''
                # 如果item_weight是数字，转换为字符串
                if isinstance(item_weight, (int, float)):
                    item_weight = str(item_weight)
            elif isinstance(dimension_info, list) and len(dimension_info) > 0:
                # 如果是列表，取第一个元素的item_weight
                first_dimension = dimension_info[0]
                if isinstance(first_dimension, dict):
                    item_weight = first_dimension.get('item_weight', '') or ''
                    if isinstance(item_weight, (int, float)):
                        item_weight = str(item_weight)
        
        # 小类排名信息
        small_rank = item.get('small_rank', []) or []
        small_category = ''  # 小类名称
        small_rank_value = None  # 小类排名（整数类型，空值用None）
        if isinstance(small_rank, list) and len(small_rank) > 0:
            first_rank = small_rank[0]
            if isinstance(first_rank, dict):
                small_category = first_rank.get('category', '') or ''
                rank_str = first_rank.get('rank', '') or ''
                # 尝试转换为整数，如果失败则设为None
                if rank_str:
                    try:
                        small_rank_value = int(float(rank_str))
                    except:
                        small_rank_value = None
                else:
                    small_rank_value = None
        
        # 全局标签
        global_tags = item.get('global_tags', []) or []
        tag_color = ''  # 颜色
        tag_name = ''  # 标签名称
        if isinstance(global_tags, list) and len(global_tags) > 0:
            first_tag = global_tags[0]
            if isinstance(first_tag, dict):
                tag_color = first_tag.get('color', '') or ''
                tag_name = first_tag.get('tagName', '') or ''
        
        # 构建Listing记录
        listing_record = {
            '店铺id': str(sid) if sid else '',
            '店铺': shop_name,
            'MSKU': seller_sku,
            'FNSKU': fnsku,
            'ASIN': asin,
            '父ASIN': parent_asin,
            '商品缩略图': small_image_url,
            '状态': status,
            '标题': item_name,
            'SKU': local_sku,
            '品名': local_name,
            'FBA可售': afn_fulfillable_quantity,
            '创建时间': open_date_display,
            '报表更新时间': listing_update_date,
            '品牌': seller_brand,
            '负责人': principal_name,
            '销量昨天': yesterday_volume,
            '销量14天': fourteen_volume,
            '销量30天': thirty_volume,
            '销量7天': total_volume,
            '首单时间': first_order_time,
            '开售时间': on_sale_time,
            '商品重量': item_weight,
            '小类名称': small_category,
            '小类排名': small_rank_value,
            '标签颜色': tag_color,
            '标签名称': tag_name,
        }
        
        listing_list.append(listing_record)
    
    return listing_list


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
            # 特殊处理：小类排名字段，使用可空的INT类型
            if k == '小类排名':
                fields.append(f"`{k}` INT DEFAULT NULL")
            elif isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                # 对于唯一索引字段，使用较小的长度以避免索引键过长
                if k in ['店铺id', 'MSKU']:
                    # 店铺ID和MSKU使用VARCHAR(100)，足够存储且不会超过索引键长度限制
                    fields.append(f"`{k}` VARCHAR(100)")
                # 对于可能包含URL或长文本的字段，使用TEXT类型
                elif '缩略图' in k or 'URL' in k.upper() or '地址' in k:
                    fields.append(f"`{k}` TEXT")
                else:
                    fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        # 添加唯一索引：店铺id + MSKU
        # 使用VARCHAR(100)后，索引键长度 = 100*4*2 = 800字节，远小于3072字节限制
        sql = f"""CREATE TABLE `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {fields_sql},
            UNIQUE KEY `uk_sid_msku` (`店铺id`, `MSKU`)
        )"""
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功（唯一键：店铺id + MSKU）")


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
    批量插入数据（全量更新模式）
    
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
            # 处理数据：将空字符串转换为None，确保INT字段能正确插入
            batch = []
            for row in data_list[i:i+batch_size]:
                row_values = []
                for key, value in zip(keys, row.values()):
                    # 对于小类排名字段，空字符串转换为None
                    if key == '小类排名' and value == '':
                        row_values.append(None)
                    else:
                        row_values.append(value)
                batch.append(tuple(row_values))
            cursor.executemany(sql, batch)
            logger.info(f"已录入 {min(i+batch_size, len(data_list))} 条...")
        
        logger.info(f"成功写入 {len(data_list)} 条数据到表 {table_name}")


async def main():
    """主函数"""
    logger.info("="*80)
    logger.info("亚马逊Listing数据采集")
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
    sid_to_name_map = {}
    try:
        shop_mapping = await get_shop_mapping()
        # shop_mapping 已经是 {seller_id: shop_name} 的格式，直接使用
        sid_to_name_map = {str(k): v for k, v in shop_mapping.items()}
        logger.info(f"✅ 共获取 {len(sid_to_name_map)} 个店铺映射")
    except Exception as e:
        logger.warning(f"获取店铺映射失败: {e}，将使用店铺ID")
    
    # 获取所有店铺ID（如果映射中有）
    sid_list = list(sid_to_name_map.keys()) if sid_to_name_map else None
    
    # 获取Listing数据
    # 测试模式：只抓取1000条数据
    TEST_MODE = False  # 设置为False可抓取全部数据
    MAX_TEST_RECORDS = 1000 if TEST_MODE else None
    
    logger.info("正在获取Listing数据...")
    if TEST_MODE:
        logger.info("🧪 测试模式：只抓取前1000条数据")
    else:
        logger.info("📊 正式模式：抓取全部数据")
    logger.info(f"⏱️  配置参数（令牌桶容量为1，请求更谨慎）:")
    logger.info(f"   - 请求间隔: {REQUEST_DELAY}秒")
    logger.info(f"   - 最大重试: {MAX_RETRIES}次")
    logger.info(f"   - 重试延迟: {RETRY_DELAY}秒（指数退避）")
    if sid_list:
        logger.info(f"   - 查询店铺: {len(sid_list)} 个")
    else:
        logger.info(f"   - 查询店铺: 全部")
    logger.info("="*80)
    
    items = await fetch_all_listings(op_api, token_resp, sid_list, max_records=MAX_TEST_RECORDS)
    logger.info(f"✅ 共获取 {len(items)} 条Listing数据")
    
    if not items:
        logger.warning("没有数据需要保存")
        return
    
    # 转换为标准格式
    logger.info("正在转换数据格式...")
    listing_data_list = convert_listing_data(items, sid_to_name_map)
    logger.info(f"共生成 {len(listing_data_list)} 条Listing记录")
    
    if not listing_data_list:
        logger.warning("没有数据需要保存")
        return
    
    # 处理数据库
    table_name = 'listing'
    logger.info(f"正在处理数据库表 {table_name}...")
    
    try:
        # 创建或检查表结构
        create_table_if_needed(table_name, listing_data_list[0])
        
        # 删除所有旧数据（全量更新）
        logger.info("正在删除所有旧数据（全量更新）...")
        deleted_count = delete_all_data(table_name)
        
        # 插入新数据
        logger.info("正在写入新数据...")
        insert_data_batch(table_name, listing_data_list)
        
        # 输出统计信息
        logger.info("="*80)
        logger.info("📊 统计信息：")
        logger.info(f"  更新策略: 全量更新")
        logger.info(f"  删除旧记录: {deleted_count} 条")
        logger.info(f"  原始数据: {len(items)} 条")
        logger.info(f"  转换后数据: {len(listing_data_list)} 条")
        logger.info(f"  新增记录: {len(listing_data_list)} 条")
        
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
                
                logger.info(f"  数据库总记录: {total_in_db} 条")
                logger.info("  各店铺统计：")
                for shop in shop_stats:
                    logger.info(f"    {shop['店铺']}: {shop['count']} 条记录")
        except Exception as e:
            logger.warning(f"查询数据库统计失败: {e}")
        
        logger.info("="*80)
        logger.info("✅ 数据采集完成！")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"数据库操作失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    asyncio.run(main())

