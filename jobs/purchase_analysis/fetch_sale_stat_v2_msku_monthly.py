#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
销量统计数据采集任务（MSKU维度-月度）
从领星API获取销量统计数据并存入数据库
API: /basicOpen/platformStatisticsV2/saleStat/pageList
"""
import asyncio
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase
from utils import normalize_shop_name
from .shop_mapping import get_shop_mapping

# 获取日志记录器
logger = get_logger('sale_stat_v2_msku_monthly')

# 重试配置（优先保证数据完整性）
# 根据领星令牌桶算法优化：
# - 令牌桶维度：appId + 接口url
# - 每个请求消耗1个令牌
# - 令牌在请求完成/异常/超时(2min)后自动回收
# - 无令牌时返回错误码3001008
MAX_RETRIES = 5  # 最大重试次数（增加到5次）
RETRY_DELAY = 10  # 重试延迟（秒，增加到10秒）
REQUEST_DELAY = 2  # 请求间隔（秒）- 给令牌回收时间，API响应快时令牌回收也快
MONTH_DELAY = 5  # 月份间延迟（秒，处理完一个月后等待）
TOKEN_BUCKET_CAPACITY = 5  # 令牌桶容量（推测值，用于计算安全间隔）


async def fetch_page_with_retry(op_api: OpenApiBase, token: str, 
                                 req_body: dict, page: int) -> Tuple[List[Dict[str, Any]], int, str, bool]:
    """
    带重试机制的单页数据获取（指数退避策略，令牌桶感知）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        req_body: 请求体
        page: 页码
        
    Returns:
        Tuple[List, int, str, bool]: (数据列表, 总数, 错误信息, 是否遇到限流)
    """
    was_rate_limited = False
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"第 {page} 页，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token, 
                "/basicOpen/platformStatisticsV2/saleStat/pageList", 
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
            # 错误码：401, 403, 2001003, 2001005, 3001001, 3001002 都表示Token过期或无效
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
            
            stats = result.get('data', [])
            total = result.get('total', 0)
            
            # 确保stats不为None
            if stats is None:
                stats = []
            
            # 成功获取数据
            if retry > 0:
                logger.info(f"✅ 重试成功！")
            
            return stats, total, "", was_rate_limited
            
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


async def fetch_all_sale_stats(op_api: OpenApiBase, token_resp, 
                                start_date_str: str, end_date_str: str,
                                sids: List[str] = None) -> List[Dict[str, Any]]:
    """
    获取所有销量统计数据（分页处理，支持重试和token刷新）
    
    Args:
        op_api: OpenAPI客户端
        token_resp: 访问令牌响应对象（包含access_token和refresh_token）
        start_date_str: 开始日期（格式：Y-m-d）
        end_date_str: 结束日期（格式：Y-m-d）
        sids: 店铺ID列表（可选）
        
    Returns:
        List[Dict[str, Any]]: 销量统计列表
    """
    all_stats = []
    page = 1
    length = 500  # 每页500条
    token = token_resp.access_token
    total_records = None
    
    # 令牌桶感知：动态调整请求间隔
    current_delay = REQUEST_DELAY  # 当前请求间隔
    consecutive_success = 0  # 连续成功次数
    consecutive_rate_limited = 0  # 连续限流次数
    
    while True:
        req_body = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "result_type": "1",  # 1=销量, 2=订单量, 3=销售额
            "date_unit": "2",    # 1=年, 2=月, 3=周, 4=日
            "data_type": "3",    # 1=ASIN, 2=父体, 3=MSKU, 4=SKU, 5=SPU, 6=店铺
            "page": page,
            "length": length
        }
        
        # 如果提供了店铺ID列表，添加到请求中
        if sids:
            req_body["sids"] = sids
        
        # 带重试机制获取数据（令牌桶感知）
        stats, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, page)
        
        # 处理token过期
        if error == "TOKEN_EXPIRED":
            logger.info("Token已过期，正在刷新...")
            try:
                token_resp = await op_api.generate_access_token()
                token = token_resp.access_token
                logger.info(f"Token刷新成功，有效期: {token_resp.expires_in}秒")
                # 重试当前页
                stats, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, page)
            except Exception as e:
                logger.error(f"Token刷新失败: {e}")
                break
        
        # 处理其他错误
        if error and error != "TOKEN_EXPIRED":
            logger.error(f"获取第 {page} 页数据失败: {error}")
            break
        
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
                current_delay = max(REQUEST_DELAY - 0.5, 1.5)
                consecutive_success = 0  # 重置计数
            else:
                current_delay = REQUEST_DELAY
        
        # 记录总数
        if total_records is None and total > 0:
            total_records = total
            logger.info(f"📊 总共需要获取 {total_records} 条数据")
        
        logger.info(f"✓ 第 {page} 页: 获取 {len(stats)} 条数据")
        
        if not stats:
            if page == 1:
                logger.warning("第一页就没有数据")
            break
        
        all_stats.extend(stats)
        
        # 显示进度
        if total_records:
            progress = (len(all_stats) / total_records) * 100
            logger.info(f"📈 进度: {len(all_stats)}/{total_records} ({progress:.1f}%)")
        
        # 如果已经获取所有数据，退出循环
        if total_records and len(all_stats) >= total_records:
            logger.info("✅ 所有数据获取完成")
            break
        
        page += 1
        
        # 请求间隔延时（动态调整，令牌桶感知）
        await asyncio.sleep(current_delay)
    
    # 数据完整性检查
    if total_records and len(all_stats) < total_records:
        logger.warning(f"⚠️  数据可能不完整: 预期 {total_records} 条，实际获取 {len(all_stats)} 条")
        logger.warning(f"   缺失: {total_records - len(all_stats)} 条数据")
    elif total_records:
        logger.info(f"✅ 数据完整性验证通过: {len(all_stats)}/{total_records} 条")
    
    return all_stats


def extract_spu_and_color(sku: str) -> Tuple[str, str]:
    """
    从SKU中提取SPU和SPU颜色
    
    Args:
        sku: SKU字符串，格式如 "SPU-颜色-其他"
    
    Returns:
        Tuple[str, str]: (spu, spu颜色)
        - spu: 第一个"-"之前的字符
        - spu颜色: 第二个"-"之前的字符（即 SPU-颜色）
    """
    if not sku or sku == '无':
        return ('无', '无')
    
    # 找到第一个"-"的位置
    first_dash = sku.find('-')
    if first_dash == -1:
        # 没有"-"，整个SKU就是spu
        return (sku, '无')
    
    # spu是第一个"-"之前的字符
    spu = sku[:first_dash]
    
    # 找到第二个"-"的位置
    second_dash = sku.find('-', first_dash + 1)
    if second_dash == -1:
        # 没有第二个"-"，spu颜色就是第一个"-"之后的所有字符
        spu_color = sku[first_dash + 1:]
    else:
        # spu颜色是第二个"-"之前的字符（包含第一个"-"）
        spu_color = sku[:second_dash]
    
    return (spu, spu_color)


def convert_to_msku_dimension(stats: List[Dict[str, Any]], month_start: str) -> List[Dict[str, Any]]:
    """
    将销量统计数据转换为MSKU维度的数据
    
    Args:
        stats: 销量统计列表
        month_start: 查询的开始日期（格式：YYYY-MM-DD），用于提取统计月份
        
    Returns:
        List[Dict[str, Any]]: MSKU维度数据列表
    """
    msku_data_list = []
    
    for stat in stats:
        # 处理MSKU字段（可能是JSON字符串或列表）
        msku_raw = stat.get('msku', [])
        if isinstance(msku_raw, str):
            try:
                msku_list = json.loads(msku_raw)
            except:
                msku_list = [msku_raw] if msku_raw else []
        elif isinstance(msku_raw, list):
            msku_list = msku_raw
        else:
            msku_list = []
        # 如果为空，默认为空字符串
        msku = ','.join(msku_list) if msku_list else ''
        
        # 处理SKU字段
        sku_raw = stat.get('sku', [])
        if isinstance(sku_raw, str):
            try:
                sku_list = json.loads(sku_raw)
            except:
                sku_list = [sku_raw] if sku_raw else []
        elif isinstance(sku_raw, list):
            sku_list = sku_raw
        else:
            sku_list = []
        # 如果为空，默认为空字符串
        sku = ','.join(sku_list) if sku_list else ''
        
        # 从SKU中提取spu和spu颜色（取第一个SKU进行提取）
        first_sku = sku_list[0] if sku_list else sku if sku else '无'
        spu, spu_color = extract_spu_and_color(first_sku)
        
        # 处理店铺名称字段
        store_name_raw = stat.get('store_name', [])
        if isinstance(store_name_raw, list):
            store_name = ','.join(store_name_raw) if store_name_raw else ''
        else:
            store_name = str(store_name_raw) if store_name_raw else ''
        # 规范化店铺名称，如果为空则保持为空字符串
        store_name = normalize_shop_name(store_name) if store_name else ''
        
        # 处理品名字段
        product_name_raw = stat.get('product_name', [])
        if isinstance(product_name_raw, list):
            product_name = ','.join(product_name_raw) if product_name_raw else ''
        else:
            product_name = str(product_name_raw) if product_name_raw else ''
        # 如果为空，默认为空字符串
        product_name = product_name if product_name else ''
        
        # 处理销量字段（volumeTotal）
        volume_total = stat.get('volumeTotal', 0)
        try:
            volume_total = int(volume_total) if volume_total else 0
        except (ValueError, TypeError):
            volume_total = 0
        # 如果为空或无效，默认为0
        volume_total = volume_total if volume_total else 0
        
        # 统计日期：使用每个月1号（month_start 已经是每月1号，如 "2025-01-01"）
        # 确保格式为 YYYY-MM-DD，MySQL可以自动转换为DATE类型
        if month_start and len(month_start) >= 10:
            stat_date = month_start[:10]  # 取前10位，确保格式为 YYYY-MM-DD
        else:
            stat_date = None  # 如果格式不对，设为None（MySQL会处理为NULL）
        
        # 构建MSKU维度数据（每条API数据对应一条记录）
        # 字段：SKU, MSKU, SPU, spu颜色, 店铺, 品名, 销量, 统计日期
        # 所有字段如果为空，都默认为"无"（销量默认为0，统计日期为None）
        msku_record = {
            'SKU': sku if sku else '无',              # 如果为空，默认为"无"
            'MSKU': msku if msku else '无',            # 如果为空，默认为"无"
            'SPU': spu,                               # SPU（第一个"-"之前的字符）
            'spu颜色': spu_color,                      # SPU颜色（第二个"-"之前的字符）
            '店铺': store_name if store_name else '无',      # 如果为空，默认为"无"
            '品名': product_name if product_name else '无',    # 如果为空，默认为"无"
            '销量': volume_total,         # 如果为空，默认为0
            '统计日期': stat_date,         # 日期类型，如果为空则为None
        }
        
        msku_data_list.append(msku_record)
    
    return msku_data_list


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
            elif k == '统计日期':
                # 统计日期使用DATE类型
                fields.append(f"`{k}` DATE")
            else:
                # 图片地址和标题可能很长，使用TEXT类型
                if k in ['图片地址', '标题']:
                    fields.append(f"`{k}` TEXT")
                else:
                    fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql})"
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def delete_by_date_range(table_name: str, start_date_str: str, end_date_str: str) -> int:
    """
    删除指定日期范围内的数据（按统计日期）
    
    Args:
        table_name: 表名
        start_date_str: 开始日期（格式：YYYY-MM-DD，每月1号）
        end_date_str: 结束日期（格式：YYYY-MM-DD，每月最后一天，但统计日期是每月1号）
        
    Returns:
        int: 删除的记录数
    """
    with db_cursor(dictionary=False) as cursor:
        # 统计日期就是每月1号，所以只需要删除统计日期等于该月1号的记录
        # start_date_str 已经是每月1号（如 "2025-12-01"）
        sql = f"DELETE FROM `{table_name}` WHERE `统计日期` = %s"
        cursor.execute(sql, (start_date_str,))
        deleted_count = cursor.rowcount
        logger.info(f"已删除 {deleted_count} 条旧数据（统计日期={start_date_str}）")
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


def calculate_months_ago(months: int) -> datetime:
    """
    计算N个月前的日期
    
    Args:
        months: 月份数
        
    Returns:
        datetime: N个月前的日期（该月1号）
    """
    now = datetime.now()
    year = now.year
    month = now.month - months
    
    # 处理跨年情况
    while month < 1:
        month += 12
        year -= 1
    
    return datetime(year, month, 1)


def get_current_month_range() -> Tuple[str, str]:
    """
    获取当前月份的范围（每月1号到月末或今天）
    
    Returns:
        Tuple[str, str]: (开始日期, 结束日期)
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    # 当前月的第一天
    first_day = datetime(current_year, current_month, 1)
    
    # 当前月的最后一天（如果是当前月，则为今天）
    if current_month == 12:
        last_day = datetime(current_year, 12, 31)
    else:
        # 下个月的第一天减去一天
        next_month_first = datetime(current_year, current_month + 1, 1)
        last_day = next_month_first - timedelta(days=1)
    
    # 如果是当前月，结束日期为今天
    if now.day < last_day.day:
        last_day = now
    
    return (
        first_day.strftime("%Y-%m-%d"),
        last_day.strftime("%Y-%m-%d")
    )


def get_2024_month_ranges() -> List[Tuple[str, str]]:
    """
    获取2024年1月到12月的月份范围列表（临时添加2024年数据）
    
    Returns:
        List[Tuple[str, str]]: 月份范围列表，每个元素为(开始日期, 结束日期)
    """
    month_ranges = []
    
    for month in range(1, 13):
        # 该月的第一天
        first_day = datetime(2024, month, 1)
        
        # 该月的最后一天
        if month == 12:
            last_day = datetime(2024, 12, 31)
        else:
            next_month_first = datetime(2024, month + 1, 1)
            last_day = next_month_first - timedelta(days=1)
        
        month_ranges.append((
            first_day.strftime("%Y-%m-%d"),
            last_day.strftime("%Y-%m-%d")
        ))
    
    return month_ranges


def get_current_and_last_month_ranges() -> List[Tuple[str, str]]:
    """
    获取当前月份和上个月的月份范围列表（增量更新当月和上个月）
    
    Returns:
        List[Tuple[str, str]]: 月份范围列表，每个元素为(开始日期, 结束日期)
        第一个是上个月，第二个是当前月
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    month_ranges = []
    
    # 1. 上个月
    if current_month == 1:
        last_month_year = current_year - 1
        last_month = 12
    else:
        last_month_year = current_year
        last_month = current_month - 1
    
    # 上个月的第一天
    last_month_first = datetime(last_month_year, last_month, 1)
    # 上个月的最后一天
    if last_month == 12:
        last_month_last = datetime(last_month_year, 12, 31)
    else:
        next_month_first = datetime(last_month_year, last_month + 1, 1)
        last_month_last = next_month_first - timedelta(days=1)
    
    month_ranges.append((
        last_month_first.strftime("%Y-%m-%d"),
        last_month_last.strftime("%Y-%m-%d")
    ))
    
    # 2. 当前月
    current_month_first = datetime(current_year, current_month, 1)
    if current_month == 12:
        current_month_last = datetime(current_year, 12, 31)
    else:
        next_month_first = datetime(current_year, current_month + 1, 1)
        current_month_last = next_month_first - timedelta(days=1)
    
    # 如果是当前月，结束日期为今天
    if now.day < current_month_last.day:
        current_month_last = now
    
    month_ranges.append((
        current_month_first.strftime("%Y-%m-%d"),
        current_month_last.strftime("%Y-%m-%d")
    ))
    
    return month_ranges


def get_month_ranges(start_year: int = 2025, start_month: int = 1) -> List[Tuple[str, str]]:
    """
    生成从指定年月到当前月的所有月份范围
    
    Args:
        start_year: 起始年份（默认2025）
        start_month: 起始月份（默认1）
        
    Returns:
        List[Tuple[str, str]]: 月份范围列表，每个元素为(开始日期, 结束日期)
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    month_ranges = []
    year = start_year
    month = start_month
    
    while (year < current_year) or (year == current_year and month <= current_month):
        # 计算该月的第一天
        first_day = datetime(year, month, 1)
        
        # 计算该月的最后一天
        if month == 12:
            last_day = datetime(year, 12, 31)
        else:
            # 下个月的第一天减去一天
            next_month_first = datetime(year if month < 12 else year + 1, 
                                       month + 1 if month < 12 else 1, 1)
            last_day = next_month_first - timedelta(days=1)
        
        # 如果是当前月，结束日期为今天
        if year == current_year and month == current_month:
            last_day = now
        
        month_ranges.append((
            first_day.strftime("%Y-%m-%d"),
            last_day.strftime("%Y-%m-%d")
        ))
        
        # 移动到下一个月
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    
    return month_ranges


async def main(start_date: str = None, end_date: str = None):
    """
    主函数（默认增量更新当月和上个月）
    
    Args:
        start_date: 开始日期（格式：YYYY-MM-DD），如果指定则查询指定日期范围
        end_date: 结束日期（格式：YYYY-MM-DD），如果指定则查询指定日期范围
        如果不指定，默认增量更新当月和上个月
    """
    logger.info("="*80)
    logger.info("销量统计数据采集（MSKU维度-月度统计，默认增量更新当月和上个月）")
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
    
    # 生成月份范围（默认增量更新当月和上个月）
    if start_date and end_date:
        # 使用指定的日期范围，按月份拆分
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 生成从开始日期到结束日期的所有月份范围
            month_ranges = []
            current_dt = datetime(start_dt.year, start_dt.month, 1)
            
            while current_dt <= end_dt:
                # 该月的第一天
                month_start = current_dt
                
                # 该月的最后一天
                if current_dt.month == 12:
                    month_end = datetime(current_dt.year, 12, 31)
                else:
                    next_month = datetime(current_dt.year, current_dt.month + 1, 1)
                    month_end = next_month - timedelta(days=1)
                
                # 如果是开始月份，使用实际的开始日期
                if current_dt.year == start_dt.year and current_dt.month == start_dt.month:
                    month_start = start_dt
                
                # 如果是结束月份，使用实际的结束日期或今天（取较小值）
                if current_dt.year == end_dt.year and current_dt.month == end_dt.month:
                    month_end = min(end_dt, datetime.now())
                
                month_ranges.append((
                    month_start.strftime("%Y-%m-%d"),
                    month_end.strftime("%Y-%m-%d")
                ))
                
                # 移动到下一个月
                if current_dt.month == 12:
                    current_dt = datetime(current_dt.year + 1, 1, 1)
                else:
                    current_dt = datetime(current_dt.year, current_dt.month + 1, 1)
            
            logger.info(f"📅 使用指定日期范围: {start_date} 至 {end_date}")
            logger.info(f"   已拆分为 {len(month_ranges)} 个月份进行查询")
        except Exception as e:
            logger.error(f"解析日期范围失败: {e}")
            logger.info("将使用默认的增量更新策略")
            month_ranges = get_current_and_last_month_ranges()
    else:
        # 默认增量更新当月和上个月
        month_ranges = get_current_and_last_month_ranges()
        logger.info(f"📅 增量更新当月和上个月:")
        for i, (m_start, m_end) in enumerate(month_ranges, 1):
            month_name = "上个月" if i == 1 else "当前月"
            logger.info(f"   {month_name}: {m_start} 至 {m_end}")
    
    logger.info(f"📊 共需要处理 {len(month_ranges)} 个月份")
    for i, (m_start, m_end) in enumerate(month_ranges, 1):
        logger.info(f"   {i}. {m_start} 至 {m_end}")
    
    # 获取店铺映射
    logger.info("\n正在加载店铺映射...")
    try:
        sid_to_name_map = await get_shop_mapping()
        logger.info(f"已加载 {len(sid_to_name_map)} 个店铺映射")
        sids = None  # 设置为None表示查询所有店铺
    except Exception as e:
        logger.warning(f"获取店铺映射失败: {e}")
        sids = None
    
    # 准备数据库表
    table_name = '销量统计_MSKU月度'
    logger.info(f"\n正在准备数据库表 {table_name}...")
    
    # 汇总统计
    total_stats = 0
    total_msku_records = 0
    total_deleted = 0
    
    # 按月份循环获取数据
    logger.info(f"\n⏱️  配置参数（优先保证数据完整性）:")
    logger.info(f"   - 请求间隔: {REQUEST_DELAY}秒")
    logger.info(f"   - 月份间隔: {MONTH_DELAY}秒")
    logger.info(f"   - 最大重试: {MAX_RETRIES}次")
    logger.info(f"   - 重试延迟: {RETRY_DELAY}秒（指数退避）")
    logger.info("="*80)
    
    for month_idx, (month_start, month_end) in enumerate(month_ranges, 1):
        logger.info(f"\n📆 [{month_idx}/{len(month_ranges)}] 正在处理: {month_start} 至 {month_end}")
        logger.info("-"*80)
        
        try:
            # 获取该月的销量统计数据
            stats = await fetch_all_sale_stats(op_api, token_resp, 
                                               month_start, month_end, sids)
            
            if not stats:
                logger.warning(f"⚠️  {month_start} 至 {month_end} 没有数据")
                continue
            
            logger.info(f"✅ 本月获取 {len(stats)} 条原始数据")
            
            # 转换为MSKU维度数据（传入月份开始日期）
            msku_data_list = convert_to_msku_dimension(stats, month_start)
            logger.info(f"✅ 本月生成 {len(msku_data_list)} 条MSKU记录")
            
            if not msku_data_list:
                logger.warning(f"⚠️  {month_start} 至 {month_end} 没有MSKU数据")
                continue
            
            # 第一次处理时创建表
            if month_idx == 1:
                create_table_if_needed(table_name, msku_data_list[0])
            
            # 删除该月的旧数据
            deleted_count = delete_by_date_range(table_name, month_start, month_end)
            logger.info(f"🗑️  删除旧数据: {deleted_count} 条")
            
            # 插入新数据
            insert_data_batch(table_name, msku_data_list)
            logger.info(f"💾 写入新数据: {len(msku_data_list)} 条")
            
            # 累计统计
            total_stats += len(stats)
            total_msku_records += len(msku_data_list)
            total_deleted += deleted_count
            
            logger.info(f"✅ {month_start} 至 {month_end} 处理完成")
            
            # 月份间延时（避免API限流，确保数据完整性）
            if month_idx < len(month_ranges):
                logger.info(f"⏳ 等待 {MONTH_DELAY} 秒后处理下一个月...")
                await asyncio.sleep(MONTH_DELAY)
            
        except Exception as e:
            logger.error(f"❌ 处理 {month_start} 至 {month_end} 失败: {e}", exc_info=True)
            continue
        
    # 输出总体统计信息
    logger.info("\n" + "="*80)
    logger.info("📊 总体统计信息")
    logger.info("="*80)
    if start_date and end_date:
        logger.info(f"  更新策略: 指定日期范围更新")
    else:
        logger.info(f"  更新策略: 增量更新（当月和上个月）")
    logger.info(f"  处理月份数: {len(month_ranges)} 个月")
    logger.info(f"  时间范围: {month_ranges[0][0]} 至 {month_ranges[-1][1]}")
    logger.info(f"  删除旧记录: {total_deleted} 条")
    logger.info(f"  原始统计数据: {total_stats} 条")
    logger.info(f"  新增MSKU记录: {total_msku_records} 条")
    
    # 查询数据库获取最终统计
    try:
        with db_cursor() as cursor:
            # 统计总记录数
            cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
            total_in_db = cursor.fetchone()['total']
            
            # 统计总销量
            cursor.execute(f"SELECT SUM(`销量`) as total_volume FROM `{table_name}`")
            total_volume = cursor.fetchone()['total_volume'] or 0
            
            # 统计各店铺记录数
            cursor.execute(f"""
                SELECT `店铺`, COUNT(*) as count, SUM(`销量`) as volume
                FROM `{table_name}`
                GROUP BY `店铺`
                ORDER BY count DESC
            """)
            shop_stats = cursor.fetchall()
            
            logger.info(f"  数据库总记录: {total_in_db} 条")
            logger.info(f"  数据库总销量: {total_volume}")
            logger.info("  各店铺统计：")
            for shop in shop_stats:
                logger.info(f"    {shop['店铺']}: {shop['count']} 条记录, {shop['volume']} 销量")
    except Exception as e:
        logger.warning(f"查询数据库统计失败: {e}")
    
    logger.info("="*80)
    logger.info("✅ 数据采集完成！")
    logger.info("="*80)


if __name__ == '__main__':
    import sys
    
    # 支持命令行参数
    # 用法: python -m jobs.purchase_analysis.fetch_sale_stat_v2_msku_monthly
    #       默认增量更新当月和上个月
    # 或: python -m jobs.purchase_analysis.fetch_sale_stat_v2_msku_monthly [start_date] [end_date]
    #       指定日期范围进行更新
    if len(sys.argv) == 3:
        # 指定日期范围
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        asyncio.run(main(start_date=start_date, end_date=end_date))
    else:
        # 默认增量更新当月和上个月
        asyncio.run(main())

