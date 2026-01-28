#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
销量数据写入飞书多维表任务
从数据库读取销量统计_MSKU月度数据，按店铺分组，为每个店铺创建飞书多维表
多维表包含SKU字段和从本月往前推13个月的销量字段
"""
import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from collections import defaultdict

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import settings, get_logger
from common.database import db_cursor
from common.feishu import FeishuClient

logger = get_logger('feishu_write_sales')

# 飞书多维表格配置（需要手动创建多维表格，然后填写app_token）
# 注意：需要在飞书中先创建一个多维表格，然后获取app_token
FEISHU_APP_TOKEN = "A1oCb6elda8Q76s0vNKcHYEznCg"  # 需要手动填写多维表格的app_token

# 需要排除的店铺列表
EXCLUDED_SHOPS = {
    'TEMU半托管-A店',
    'TEMU半托管-C店',
    'TEMU半托管-M店',
    'TEMU半托管-P店',
    'TEMU半托管-V店',
    'TEMU半托管-本土店-R店',
    'TK本土店-1店',
    'TK跨境店-2店',
    'CY-US',
    'DX-US',
    'MT-CA'
}


def get_month_labels(current_date: datetime = None) -> List[str]:
    """
    生成销量字段标签：从上个月的去年同期开始往后5个月，以及上个月的销量
    
    例如：如果现在是26年1月，上个月是25年12月，上个月的去年同期是24年12月
    则返回：24年12月、25年1月、25年2月、25年3月、25年4月、25年5月、25年12月
    
    Args:
        current_date: 当前日期，如果为None则使用今天
        
    Returns:
        List[str]: 月份标签列表，格式如 ['24年12月销量', '25年1月销量', ..., '25年12月销量']
    """
    if current_date is None:
        current_date = datetime.now()
    
    month_labels = []
    current_year = current_date.year
    current_month = current_date.month
    
    # 计算上个月
    last_month = current_month - 1
    last_month_year = current_year
    if last_month < 1:
        last_month += 12
        last_month_year -= 1
    
    # 计算上个月的去年同期（往前推1年）
    last_month_yoy_year = last_month_year - 1
    last_month_yoy_month = last_month
    
    # 从上个月的去年同期开始，往后5个月
    for i in range(6):  # 0到5，共6个月（包括起始月）
        target_month = last_month_yoy_month + i
        target_year = last_month_yoy_year
        
        # 处理跨年情况
        while target_month > 12:
            target_month -= 12
            target_year += 1
        
        # 格式化：取年份后两位 + 月份
        year_short = str(target_year)[-2:]
        month_label = f"{year_short}年{target_month}月销量"
        month_labels.append(month_label)
    
    # 添加上个月的销量（如果不在前面的列表中）
    last_month_label = f"{str(last_month_year)[-2:]}年{last_month}月销量"
    if last_month_label not in month_labels:
        month_labels.append(last_month_label)
    
    return month_labels


def get_month_key(year: int, month: int) -> str:
    """
    生成月份键，用于匹配数据
    
    Args:
        year: 年份
        month: 月份
        
    Returns:
        str: 月份键，格式如 '25年12月销量'
    """
    year_short = str(year)[-2:]
    return f"{year_short}年{month}月销量"


def get_forecast_sales_labels(current_date: datetime = None) -> List[str]:
    """
    生成包含本月在内的未来4个月的预计销量字段标签
    
    Args:
        current_date: 当前日期，如果为None则使用今天
        
    Returns:
        List[str]: 月份标签列表，格式如 ['26年1月预计销量', '26年2月预计销量', ...]
                   （包含本月，共4个月）
    """
    if current_date is None:
        current_date = datetime.now()
    
    labels = []
    current_year = current_date.year
    current_month = current_date.month
    
    for i in range(4):  # 包含本月在内的未来4个月
        # 计算目标月份（往后推i个月）
        target_month = current_month + i
        target_year = current_year
        
        # 处理跨年情况
        while target_month > 12:
            target_month -= 12
            target_year += 1
        
        # 格式化：取年份后两位 + 月份
        year_short = str(target_year)[-2:]
        month_label = f"{year_short}年{target_month}月预计销量"
        labels.append(month_label)
    
    return labels


def get_forecast_order_labels(current_date: datetime = None) -> List[str]:
    """
    生成包含本月在内的未来3个月的预计下单量字段标签
    
    Args:
        current_date: 当前日期，如果为None则使用今天
        
    Returns:
        List[str]: 月份标签列表，格式如 ['26年1月预计下单量', '26年2月预计下单量', ...]
                   （包含本月，共3个月）
    """
    if current_date is None:
        current_date = datetime.now()
    
    labels = []
    current_year = current_date.year
    current_month = current_date.month
    
    for i in range(3):  # 包含本月在内的未来3个月
        # 计算目标月份（往后推i个月）
        target_month = current_month + i
        target_year = current_year
        
        # 处理跨年情况
        while target_month > 12:
            target_month -= 12
            target_year += 1
        
        # 格式化：取年份后两位 + 月份
        year_short = str(target_year)[-2:]
        month_label = f"{year_short}年{target_month}月预计下单量"
        labels.append(month_label)
    
    return labels


def get_recent_3_months_labels(current_date: datetime = None) -> List[str]:
    """
    生成包含本月在内的最近3个月的销量字段标签（用于过滤）
    
    Args:
        current_date: 当前日期，如果为None则使用今天
        
    Returns:
        List[str]: 月份标签列表，格式如 ['25年11月销量', '25年12月销量', '26年1月销量']
                   （包含本月，共3个月，从最早到最晚）
    """
    if current_date is None:
        current_date = datetime.now()
    
    labels = []
    current_year = current_date.year
    current_month = current_date.month
    
    # 往前推2个月，加上本月，共3个月
    for i in range(2, -1, -1):  # i=2,1,0 表示往前推2个月、1个月、本月
        target_month = current_month - i
        target_year = current_year
        
        # 处理跨年情况
        while target_month < 1:
            target_month += 12
            target_year -= 1
        
        # 格式化：取年份后两位 + 月份
        year_short = str(target_year)[-2:]
        month_label = f"{year_short}年{target_month}月销量"
        labels.append(month_label)
    
    return labels


def filter_skus_by_spu_sales(aggregated_data: Dict[str, Dict[str, Dict[str, Any]]], 
                              month_labels: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    按SPU过滤：如果SPU在所有查询月份的总销量为0，则过滤掉该SPU下的所有SKU
    否则，保留该SPU下的所有SKU（包括销量为0的SKU）
    
    Args:
        aggregated_data: 聚合后的数据
        month_labels: 所有查询的月份标签列表
        
    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: 过滤后的数据
    """
    logger.info(f"过滤条件：如果SPU在所有查询月份（共{len(month_labels)}个月）的总销量为0，则过滤掉该SPU")
    logger.info(f"查询月份：{', '.join(month_labels)}")
    
    filtered_data = {}
    total_skus = 0
    filtered_skus = 0
    total_spus = 0
    filtered_spus = 0
    
    for shop_name, shop_data in aggregated_data.items():
        # 第一步：按SPU聚合，计算每个SPU在所有月份的总销量
        spu_total_sales = {}  # {spu: 总销量}
        spu_skus = defaultdict(list)  # {spu: [sku1, sku2, ...]}
        
        for sku, sku_data in shop_data.items():
            total_skus += 1
            spu = sku_data.get('SPU', '').strip() if sku_data.get('SPU') else extract_spu_from_sku(sku)
            if not spu:
                # 如果没有SPU，使用SKU本身作为SPU
                spu = sku
            
            # 初始化SPU的总销量
            if spu not in spu_total_sales:
                spu_total_sales[spu] = 0
                total_spus += 1
            
            # 累加该SKU在所有月份的销量
            for month_label in month_labels:
                sales = sku_data.get(month_label, 0) or 0
                spu_total_sales[spu] += sales
            
            # 记录该SPU下的SKU列表
            spu_skus[spu].append(sku)
        
        # 第二步：根据SPU总销量决定是否保留
        filtered_shop_data = {}
        for spu, total_sales in spu_total_sales.items():
            if total_sales > 0:
                # SPU有销量，保留该SPU下的所有SKU
                for sku in spu_skus[spu]:
                    filtered_shop_data[sku] = shop_data[sku]
            else:
                # SPU总销量为0，过滤掉该SPU下的所有SKU
                filtered_spus += 1
                filtered_skus += len(spu_skus[spu])
        
        if filtered_shop_data:
            filtered_data[shop_name] = filtered_shop_data
    
    logger.info(f"过滤完成：共 {total_spus} 个SPU，{total_skus} 个SKU")
    logger.info(f"过滤掉 {filtered_spus} 个总销量为0的SPU（包含 {filtered_skus} 个SKU）")
    logger.info(f"保留 {total_spus - filtered_spus} 个有销量的SPU（包含 {total_skus - filtered_skus} 个SKU）")
    
    return filtered_data


def remove_psc_pattern(sku: str) -> str:
    """
    去除SKU中的"数字+PSC/PCS"模式（例如：4PSC, 1PCS, 10PSC等）
    去除后会清理多余的分隔符（将连续的分隔符合并为一个）
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: 去除"数字+PSC/PCS"后的SKU，并清理多余分隔符
    """
    if not sku:
        return sku
    # 匹配任意数字+PSC或PCS的模式，例如：4PSC, 1PCS, 10PSC等
    # 使用正则表达式 \d+(?:PSC|PCS) 匹配，并去除
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    # 清理多余的分隔符：将连续的分隔符合并为一个
    sku = re.sub(r'-+', '-', sku)
    # 去除首尾的分隔符
    sku = sku.strip('-')
    return sku


def extract_spu_from_sku(sku: str) -> str:
    """
    从SKU中提取SPU（第一个"-"之前的部分）
    会先去除"数字+PSC"模式（例如：4PSC）
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: SPU（如果SKU中没有"-"，则返回整个SKU）
    """
    if not sku:
        return ''
    # 先去除"数字+PSC"模式
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    if idx > 0:
        return sku[:idx]
    return sku


def get_responsible_from_sales_data() -> Dict[Tuple[str, str], str]:
    """
    从listing表获取负责人信息（使用SPU+店铺匹配）
    
    Returns:
        Dict[Tuple[str, str], str]: {(SPU, 店铺): 负责人} 的映射字典
    """
    responsible_map = {}
    
    try:
        with db_cursor() as cursor:
            # 先检查表是否存在以及是否有负责人字段
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'listing'
                AND COLUMN_NAME = '负责人'
            """)
            result = cursor.fetchone()
            has_responsible_field = (result.get('cnt', 0) if result else 0) > 0
            
            if not has_responsible_field:
                logger.info("listing表中没有负责人字段")
                return responsible_map
            
            # 查询listing表，获取SKU、店铺和负责人的映射
            # 注意：listing表中的店铺字段叫做"店铺"
            sql = """
            SELECT 
                SKU,
                店铺,
                负责人
            FROM `listing`
            WHERE SKU IS NOT NULL 
              AND SKU != '' 
              AND SKU != '无'
              AND 店铺 IS NOT NULL
              AND 店铺 != ''
              AND 店铺 != '无'
              AND 负责人 IS NOT NULL
              AND 负责人 != ''
              AND 负责人 != '无'
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            for row in results:
                sku = row.get('SKU', '').strip()
                shop = row.get('店铺', '').strip()  # listing表中是"店铺"
                responsible = row.get('负责人', '').strip()
                
                if sku and shop and responsible:
                    # 提取SPU（第一个"-"之前的部分）
                    spu = extract_spu_from_sku(sku)
                    if spu:
                        key = (spu, shop)
                        # 如果同一个SPU+店铺有多个负责人，保留第一个（或可以合并）
                        if key not in responsible_map:
                            responsible_map[key] = responsible
            
            logger.info(f"从listing表读取到 {len(responsible_map)} 条负责人映射（SPU+店铺）")
    except Exception as e:
        logger.warning(f"从listing表读取负责人失败: {e}")
    
    return responsible_map


def get_operation_from_product_info() -> Dict[Tuple[str, str], str]:
    """
    从产品信息表获取运营信息（使用SPU+店铺匹配）
    
    Returns:
        Dict[Tuple[str, str], str]: {(SPU, 店铺): 运营} 的映射字典
    """
    operation_map = {}
    
    try:
        with db_cursor() as cursor:
            # 查询产品信息表，获取SKU、店铺名和运营的映射
            # 注意：产品信息表中的店铺字段叫做"店铺名"
            # 匹配逻辑：先提取SKU的SPU（第一个"-"之前），然后用SPU+店铺名匹配
            sql = """
            SELECT 
                SKU,
                店铺名,
                运营
            FROM `产品信息`
            WHERE SKU IS NOT NULL 
              AND SKU != '' 
              AND SKU != '无'
              AND 店铺名 IS NOT NULL
              AND 店铺名 != ''
              AND 店铺名 != '无'
              AND 运营 IS NOT NULL
              AND 运营 != ''
              AND 运营 != '无'
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            for row in results:
                sku = row.get('SKU', '').strip()
                shop = row.get('店铺名', '').strip()  # 注意：产品信息表中是"店铺名"
                operation = row.get('运营', '').strip()
                
                if sku and shop and operation:
                    # 提取SPU（第一个"-"之前的部分）
                    spu = extract_spu_from_sku(sku)
                    if spu:
                        key = (spu, shop)
                        # 如果同一个SPU+店铺有多个运营，保留第一个（或可以合并）
                        if key not in operation_map:
                            operation_map[key] = operation
            
            logger.info(f"从产品信息表读取到 {len(operation_map)} 条运营映射（SPU+店铺）")
    except Exception as e:
        logger.warning(f"从产品信息表读取运营失败: {e}")
    
    return operation_map


def get_order_forecast_from_db(shop_name: str) -> Dict[Tuple[str, str], int]:
    """
    从运营预计下单表获取预计下单量数据（按SKU+统计日期匹配）
    
    Args:
        shop_name: 店铺名称
        
    Returns:
        Dict[Tuple[str, str], int]: {(SKU, 统计日期): 预计下单量} 的字典
    """
    order_forecast_data = {}
    
    try:
        with db_cursor() as cursor:
            # 先检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '运营预计下单表'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.debug(f"表 '运营预计下单表' 不存在，返回空数据")
                return order_forecast_data
            
            sql = """
            SELECT 
                SKU,
                统计日期,
                预计下单量
            FROM `运营预计下单表`
            WHERE 店铺 = %s
              AND SKU IS NOT NULL 
              AND SKU != '' 
              AND 统计日期 IS NOT NULL
            """
            cursor.execute(sql, (shop_name,))
            results = cursor.fetchall()
            
            for row in results:
                sku = row.get('SKU', '').strip()
                stat_date = row.get('统计日期')
                quantity = int(row.get('预计下单量', 0) or 0)
                
                if sku and stat_date:
                    # 处理日期格式，转换为 YYYY-MM-DD 格式
                    if isinstance(stat_date, str):
                        stat_date_str = stat_date[:10] if len(stat_date) >= 10 else stat_date
                    elif hasattr(stat_date, 'strftime'):
                        stat_date_str = stat_date.strftime('%Y-%m-%d')
                    else:
                        stat_date_str = str(stat_date)[:10]
                    
                    key = (sku, stat_date_str)
                    order_forecast_data[key] = quantity
            
            logger.debug(f"从运营预计下单表读取到 {len(order_forecast_data)} 条预计下单量数据（店铺: {shop_name}）")
    except Exception as e:
        logger.warning(f"从运营预计下单表读取预计下单量失败: {e}")
    
    return order_forecast_data


def get_inventory_from_estimate_table() -> Tuple[Dict[Tuple[str, str], int], Dict[str, int]]:
    """
    从库存预估表获取库存数据
    
    匹配逻辑：
    - 对于每个 (SKU, 店铺)，匹配：
      1. 库存表中 (SKU, 店铺) 完全匹配的记录
      2. 库存表中 (SKU, "无") 或 (SKU, "") 的记录（这些记录对所有店铺都适用）
    - 对所有匹配上的数量求和，得到总库存
    
    Returns:
        Tuple[Dict[Tuple[str, str], int], Dict[str, int]]: 
            ({(SKU, 店铺): 库存}, {SKU: 通用库存（无店铺）}) 的元组
    """
    inventory_by_shop = {}
    inventory_no_shop = {}
    
    try:
        with db_cursor() as cursor:
            # 查询库存预估表，获取SKU、店铺和数量
            sql = """
            SELECT 
                sku,
                店铺,
                SUM(数量) as 总数量
            FROM `库存预估表`
            WHERE sku IS NOT NULL 
              AND sku != '' 
              AND sku != '无'
              AND 数量 IS NOT NULL
            GROUP BY sku, 店铺
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            for row in results:
                sku = row.get('sku', '').strip()
                shop = row.get('店铺', '').strip() if row.get('店铺') else ''
                quantity = int(row.get('总数量', 0) or 0)
                
                if not sku:
                    continue
                
                if not shop or shop == '无':
                    # 店铺为"无"或空，这个SKU的所有店铺都可以使用
                    if sku not in inventory_no_shop:
                        inventory_no_shop[sku] = 0
                    inventory_no_shop[sku] += quantity
                else:
                    # 店铺有值，按 (SKU, 店铺) 存储
                    key = (sku, shop)
                    if key not in inventory_by_shop:
                        inventory_by_shop[key] = 0
                    inventory_by_shop[key] += quantity
            
            logger.info(f"从库存预估表读取到 {len(inventory_by_shop)} 条具体店铺库存，{len(inventory_no_shop)} 个SKU的通用库存")
            
    except Exception as e:
        logger.warning(f"从库存预估表读取库存失败: {e}")
    
    return inventory_by_shop, inventory_no_shop


def read_sales_data_from_db() -> List[Dict[str, Any]]:
    """
    从数据库读取销量统计数据
    
    Returns:
        List[Dict[str, Any]]: 销量统计数据列表，包含SKU、店铺、统计日期、销量、spu字段
    """
    table_name = '销量统计_msku月度'  # 注意：表名使用小写msku
    
    with db_cursor() as cursor:
        # 先检查表是否有SPU字段
        # 注意：销量统计_msku月度表没有负责人和运营字段，需要从其他表匹配
        cursor.execute(f"""
            SELECT COUNT(*) as cnt FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = '{table_name}' 
            AND COLUMN_NAME = 'SPU'
        """)
        result = cursor.fetchone()
        has_spu_field = (result.get('cnt', 0) if result else 0) > 0
        
        # 先查询几条样本数据，查看日期格式
        sample_sql = f"""
        SELECT 
            SKU,
            店铺,
            统计日期,
            销量
            {', SPU' if has_spu_field else ''}
        FROM `{table_name}`
        WHERE 店铺 IS NOT NULL 
          AND 店铺 != '' 
          AND 店铺 != '无'
          AND SKU IS NOT NULL
          AND SKU != ''
          AND SKU != '无'
          AND 统计日期 IS NOT NULL
        ORDER BY 店铺, SKU, 统计日期
        LIMIT 5
        """
        cursor.execute(sample_sql)
        sample_results = cursor.fetchall()
        if sample_results:
            logger.info(f"样本数据（前5条）:")
            for i, row in enumerate(sample_results[:3], 1):
                stat_date = row.get('统计日期')
                logger.info(f"  样本{i}: 统计日期={stat_date} (类型: {type(stat_date).__name__}, repr: {repr(stat_date)})")
        
        # 查询所有数据
        # 注意：销量统计_msku月度表没有负责人和运营字段，不查询这些字段
        sql = f"""
        SELECT 
            SKU,
            店铺,
            统计日期,
            销量
            {', SPU' if has_spu_field else ''}
        FROM `{table_name}`
        WHERE 店铺 IS NOT NULL 
          AND 店铺 != '' 
          AND 店铺 != '无'
          AND SKU IS NOT NULL
          AND SKU != ''
          AND SKU != '无'
          AND 统计日期 IS NOT NULL
        ORDER BY 店铺, SKU, 统计日期
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.info(f"从数据库读取到 {len(results)} 条销量统计数据")
        
        # SPU从销量统计表获取（如果表中有SPU字段）
        # 如果表中没有SPU字段，则从SKU提取SPU（第一个"-"之前）
        if not has_spu_field:
            logger.info("销量统计表中没有SPU字段，从SKU提取SPU")
            for row in results:
                sku = row.get('SKU', '').strip()
                row['SPU'] = extract_spu_from_sku(sku) if sku else None
        else:
            # 如果表中有SPU字段，但某些记录的SPU为空，则从SKU提取
            for row in results:
                if not row.get('SPU'):
                    sku = row.get('SKU', '').strip()
                    row['SPU'] = extract_spu_from_sku(sku) if sku else None
        
        # 从listing表获取负责人（使用SPU+店铺匹配）
        # 注意：销量统计_msku月度表没有负责人和运营字段，需要从其他表匹配
        logger.info("正在从listing表获取负责人字段（SPU+店铺匹配）...")
        responsible_map_from_listing = get_responsible_from_sales_data()
        
        # 从产品信息表获取运营字段（作为补充，使用SPU+店铺匹配）
        logger.info("正在从产品信息表获取运营字段（SPU+店铺匹配）...")
        operation_map_from_product = get_operation_from_product_info()
        
        # 为每条记录添加运营字段
        matched_from_listing = 0
        matched_from_product = 0
        for row in results:
            sku = row.get('SKU', '').strip()
            shop = row.get('店铺', '').strip()
            operation = None
            
            # 提取SPU（第一个"-"之前的部分）
            spu = row.get('SPU', '').strip() if row.get('SPU') else extract_spu_from_sku(sku)
            
            # 第一优先级：从listing表获取负责人（使用SPU+店铺匹配）
            if spu and shop:
                key = (spu, shop)
                operation = responsible_map_from_listing.get(key, '')
                if operation:
                    matched_from_listing += 1
            
            # 第二优先级：从产品信息表获取（使用SPU+店铺）
            if not operation and spu and shop:
                key = (spu, shop)
                operation = operation_map_from_product.get(key, '')
                if operation:
                    matched_from_product += 1
            
            row['运营'] = operation if operation else None
        
        logger.info(f"运营字段匹配完成：从listing表匹配 {matched_from_listing} 条，从产品信息表匹配 {matched_from_product} 条")
        
        return results


def aggregate_sales_by_shop_and_sku(data: List[Dict[str, Any]], 
                                     month_labels: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    按店铺和SKU聚合销量数据
    
    Args:
        data: 原始数据列表
        month_labels: 月份标签列表
        
    Returns:
        Dict[str, Dict[str, Dict[str, Any]]]: 
            第一层key是店铺名，第二层key是SKU，第三层key是月份标签或'spu'、'总库存'，值是销量或SPU或库存
    """
    result = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: None)))
    
    # 从库存预估表获取库存数据
    logger.info("正在从库存预估表获取库存数据...")
    inventory_by_shop, inventory_no_shop = get_inventory_from_estimate_table()
    
    # 调试统计
    skipped_no_shop = 0
    skipped_no_sku = 0
    skipped_no_date = 0
    skipped_date_parse_error = 0
    skipped_date_not_in_range = 0
    processed_count = 0
    
    # 显示月份标签范围用于调试
    logger.debug(f"月份标签范围: {month_labels[0]} 到 {month_labels[-1]}")
    
    for row in data:
        shop = row.get('店铺', '').strip()
        sku = row.get('SKU', '').strip()
        stat_date = row.get('统计日期')
        sales = row.get('销量', 0) or 0
        
        if not shop or shop == '无':
            skipped_no_shop += 1
            continue
        if not sku or sku == '无':
            skipped_no_sku += 1
            continue
        if not stat_date:
            skipped_no_date += 1
            continue
        
        # 解析统计日期
        date_obj = None
        try:
            # 处理 datetime 对象
            if isinstance(stat_date, datetime):
                date_obj = stat_date
            # 处理 date 对象（MySQL返回的DATE类型）
            elif hasattr(stat_date, 'year') and hasattr(stat_date, 'month') and hasattr(stat_date, 'day'):
                # 这是 date 对象
                from datetime import date
                if isinstance(stat_date, date):
                    date_obj = datetime.combine(stat_date, datetime.min.time())
            # 处理字符串
            elif isinstance(stat_date, str):
                try:
                    # 尝试多种日期格式
                    if len(stat_date) >= 10:
                        date_obj = datetime.strptime(stat_date[:10], '%Y-%m-%d')
                    else:
                        date_obj = datetime.strptime(stat_date, '%Y-%m-%d')
                except:
                    try:
                        # 尝试其他格式
                        date_obj = datetime.strptime(stat_date, '%Y/%m/%d')
                    except:
                        raise ValueError(f"无法解析日期字符串: {stat_date}")
            # 其他类型，尝试转换为字符串再解析
            elif stat_date is not None:
                date_str = str(stat_date)
                if len(date_str) >= 10:
                    date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                else:
                    raise ValueError(f"日期字符串太短: {date_str}")
            else:
                raise ValueError("日期为空")
        except Exception as e:
            skipped_date_parse_error += 1
            if skipped_date_parse_error <= 3:  # 只打印前3个错误示例
                logger.warning(f"日期解析失败: {stat_date} (类型: {type(stat_date).__name__}), 错误: {e}")
            continue
        
        if date_obj is None:
            skipped_date_parse_error += 1
            continue
        
        year = date_obj.year
        month = date_obj.month
        
        # 生成月份键
        month_key = get_month_key(year, month)
        
        # 保存SPU信息（从销量统计表获取，如果存在）
        spu = row.get('SPU', '').strip() if row.get('SPU') else ''
        if spu and not result[shop][sku].get('SPU'):
            result[shop][sku]['SPU'] = spu
        
        # 保存运营信息（从产品信息表获取，如果存在）
        operation = row.get('运营', '').strip() if row.get('运营') else ''
        if operation and not result[shop][sku].get('运营'):
            result[shop][sku]['运营'] = operation
        
        # 如果这个月份在我们要处理的范围内，则累加销量
        if month_key in month_labels:
            current_sales = result[shop][sku].get(month_key, 0) or 0
            result[shop][sku][month_key] = current_sales + int(sales)
            processed_count += 1
        else:
            skipped_date_not_in_range += 1
            if skipped_date_not_in_range <= 5:  # 只打印前5个示例
                logger.debug(f"日期不在范围内: {stat_date} -> {month_key}, 范围: {month_labels[0]} 到 {month_labels[-1]}")
    
    # 添加库存数据
    logger.info("正在添加库存数据...")
    inventory_count = 0
    for shop, sku_data in result.items():
        for sku in sku_data.keys():
            # 匹配库存：先尝试精确匹配 (SKU, 店铺)，再尝试 (SKU, "无")
            key = (sku, shop)
            total_inventory = 0
            
            # 精确匹配 (SKU, 店铺)
            if key in inventory_by_shop:
                total_inventory += inventory_by_shop[key]
            
            # 匹配 (SKU, "无") 或 (SKU, "") 的记录（这些记录对所有店铺都适用）
            if sku in inventory_no_shop:
                total_inventory += inventory_no_shop[sku]
            
            # 如果总库存大于0，则保存
            if total_inventory > 0:
                result[shop][sku]['总库存'] = total_inventory
                inventory_count += 1
    
    # 输出调试信息
    logger.info(f"数据聚合完成，共 {len(result)} 个店铺，{inventory_count} 个SKU有库存数据")
    logger.info(f"处理统计: 成功处理 {processed_count} 条, 跳过无店铺 {skipped_no_shop} 条, 跳过无SKU {skipped_no_sku} 条, "
                f"跳过无日期 {skipped_no_date} 条, 日期解析失败 {skipped_date_parse_error} 条, "
                f"日期不在范围内 {skipped_date_not_in_range} 条")
    for shop, sku_data in result.items():
        logger.info(f"  店铺 {shop}: {len(sku_data)} 个SKU")
    
    return result


def prepare_feishu_records(shop_data: Dict[str, Dict[str, Any]], 
                           month_labels: List[str],
                           forecast_sales_labels: List[str] = None,
                           forecast_order_labels: List[str] = None,
                           current_date: datetime = None,
                           shop_name: str = None,
                           order_forecast_data: Dict[Tuple[str, str], int] = None) -> List[Dict[str, Any]]:
    """
    准备飞书多维表的记录数据
    
    Args:
        shop_data: 店铺的SKU数据，格式为 {SKU: {月份标签: 销量, 'SPU': SPU, '运营': 运营}}
        month_labels: 月份标签列表
        forecast_sales_labels: 预计销量字段标签列表（可选）
        forecast_order_labels: 预计下单量字段标签列表（可选）
        current_date: 当前日期，用于计算趋势因子和预计销量
        
    Returns:
        List[Dict[str, Any]]: 飞书记录列表
    """
    if current_date is None:
        current_date = datetime.now()
    
    # 计算上个月和上个月的去年同期
    current_year = current_date.year
    current_month = current_date.month
    
    last_month = current_month - 1
    last_month_year = current_year
    if last_month < 1:
        last_month += 12
        last_month_year -= 1
    
    last_month_yoy_year = last_month_year - 1
    last_month_yoy_month = last_month
    
    # 生成月份标签
    last_month_label = f"{str(last_month_year)[-2:]}年{last_month}月销量"
    last_month_yoy_label = f"{str(last_month_yoy_year)[-2:]}年{last_month_yoy_month}月销量"
    
    records = []
    
    for sku, sku_data in shop_data.items():
        record = {
            'SKU': sku
        }
        
        # 添加SPU字段（从销量统计表获取，如果没有则默认为空字符串）
        spu = sku_data.get('SPU', '')
        record['SPU'] = spu if spu else ''
        
        # 添加运营字段（从产品信息表获取，如果没有则默认为空字符串）
        operation = sku_data.get('运营', '')
        record['运营'] = operation if operation else ''
        
        # 添加总库存字段（从库存预估表获取，如果没有则默认为0）
        total_inventory = sku_data.get('总库存', 0)
        record['总库存'] = total_inventory if total_inventory else 0
        
        # 为每个月份字段填充销量，如果没有数据则为0
        for month_label in month_labels:
            record[month_label] = sku_data.get(month_label, 0) or 0
        
        # 计算趋势因子：上个月销量 / 上个月去年同期销量
        last_month_sales = sku_data.get(last_month_label, 0) or 0
        last_month_yoy_sales = sku_data.get(last_month_yoy_label, 0) or 0
        
        if last_month_yoy_sales > 0:
            trend_factor = last_month_sales / last_month_yoy_sales
        else:
            trend_factor = 0.0
        
        record['趋势因子'] = round(trend_factor, 2) if trend_factor > 0 else 0.0
        
        # 添加预计销量字段（默认值 = 去年同期销量 × 趋势因子）
        if forecast_sales_labels:
            for i, label in enumerate(forecast_sales_labels):
                # 计算对应的去年同期月份
                forecast_month = current_month + i
                forecast_year = current_year
                while forecast_month > 12:
                    forecast_month -= 12
                    forecast_year += 1
                
                yoy_month = forecast_month
                yoy_year = forecast_year - 1
                yoy_label = f"{str(yoy_year)[-2:]}年{yoy_month}月销量"
                
                # 获取去年同期销量
                yoy_sales = sku_data.get(yoy_label, 0) or 0
                
                # 计算预计销量 = 去年同期销量 × 趋势因子
                if yoy_sales > 0 and trend_factor > 0:
                    forecast_sales = int(yoy_sales * trend_factor)
                else:
                    forecast_sales = 0
                
                record[label] = forecast_sales
        
        # 添加预计下单量字段（从运营预计下单表获取默认值）
        if forecast_order_labels:
            for label in forecast_order_labels:
                # 解析月份标签，生成统计日期
                # 格式：XX年X月预计下单量 -> 2026-01-01
                default_value = 0
                if order_forecast_data:
                    # 解析月份标签
                    pattern = r'(\d{2})年(\d{1,2})月预计下单量'
                    match = re.match(pattern, label)
                    if match:
                        year_short = int(match.group(1))
                        month = int(match.group(2))
                        
                        # 将两位年份转换为四位年份
                        if year_short < 50:
                            year = 2000 + year_short
                        else:
                            year = 1900 + year_short
                        
                        # 生成统计日期（月份的第一天）
                        stat_date_str = f"{year}-{month:02d}-01"
                        
                        # 从运营预计下单表获取预计下单量
                        key = (sku, stat_date_str)
                        default_value = order_forecast_data.get(key, 0)
                
                record[label] = default_value
        
        records.append(record)
    
    return records


async def process_shop_data(shop_name: str, 
                            shop_data: Dict[str, Dict[str, Any]], 
                            month_labels: List[str],
                            app_token: str) -> bool:
    """
    处理单个店铺的数据，创建或更新飞书多维表
    
    Args:
        shop_name: 店铺名称
        shop_data: 店铺的SKU数据
        month_labels: 月份标签列表
        app_token: 飞书多维表格的app_token
        
    Returns:
        bool: 是否成功
    """
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"正在处理店铺: {shop_name}")
        logger.info(f"{'='*80}")
        
        # 创建飞书客户端（先使用临时table_id，后面会创建或获取实际的table_id）
        feishu_client = FeishuClient(
            app_token=app_token,
            table_id=""  # 临时值，会在ensure_table_and_fields中设置
        )
        
        # 生成预计销量和预计下单量字段标签
        current_date = datetime.now()
        forecast_sales_labels = get_forecast_sales_labels(current_date)
        forecast_order_labels = get_forecast_order_labels(current_date)
        
        # 准备字段列表
        field_list = [
            {'name': 'SKU', 'type': 'text'},
            {'name': 'SPU', 'type': 'text'},
            {'name': '运营', 'type': 'text'},
            {'name': '总库存', 'type': 'number'}
        ]
        # 添加历史销量字段
        for month_label in month_labels:
            field_list.append({'name': month_label, 'type': 'number'})
        # 添加趋势因子字段（保留2位小数）
        field_list.append({'name': '趋势因子', 'type': 'number', 'precision': 2})
        # 添加预计销量字段（未来4个月）
        for label in forecast_sales_labels:
            field_list.append({'name': label, 'type': 'number'})
        # 添加预计下单量字段（未来3个月）
        for label in forecast_order_labels:
            field_list.append({'name': label, 'type': 'number'})
        
        # 确保表和字段存在，并删除多余的旧月份字段
        logger.info(f"正在确保数据表和字段存在（自动更新月份字段）...")
        table_id = await feishu_client.ensure_table_and_fields(
            shop_name, 
            field_list, 
            remove_extra_fields=True  # 删除不在当前月份列表中的旧字段
        )
        
        # 更新client的table_id
        feishu_client.table_id = table_id
        
        # 从运营预计下单表获取预计下单量默认值
        logger.info(f"正在从运营预计下单表获取预计下单量默认值...")
        order_forecast_data = get_order_forecast_from_db(shop_name)
        if order_forecast_data:
            logger.info(f"获取到 {len(order_forecast_data)} 条预计下单量默认值")
        
        # 准备记录数据
        logger.info(f"正在准备记录数据...")
        records = prepare_feishu_records(
            shop_data, 
            month_labels,
            forecast_sales_labels=forecast_sales_labels,
            forecast_order_labels=forecast_order_labels,
            current_date=current_date,
            shop_name=shop_name,
            order_forecast_data=order_forecast_data
        )
        logger.info(f"共准备 {len(records)} 条记录")
        
        if not records:
            logger.warning(f"店铺 {shop_name} 没有数据需要写入")
            return True
        
        # 先清空现有数据，确保表是空的再写入新数据
        logger.info(f"正在清空现有数据...")
        max_retries = 3
        deleted_count = 0
        
        for retry in range(max_retries):
            try:
                deleted_count = await feishu_client.delete_all_records()
                logger.info(f"成功清空 {deleted_count} 条旧记录")
                break  # 成功清空，退出重试循环
            except asyncio.TimeoutError:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 10  # 等待10秒、20秒、30秒
                    logger.warning(f"清空数据超时（尝试 {retry + 1}/{max_retries}），{wait_time}秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"清空数据超时，已重试 {max_retries} 次。表可能包含大量数据，请手动清空表后再运行脚本。")
                    raise Exception(f"清空数据失败：超时。表可能包含大量数据，请手动清空表后再运行脚本。")
            except Exception as e:
                error_str = str(e)
                if "超时" in error_str or "timeout" in error_str.lower():
                    if retry < max_retries - 1:
                        wait_time = (retry + 1) * 10
                        logger.warning(f"清空数据超时（尝试 {retry + 1}/{max_retries}），{wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"清空数据超时，已重试 {max_retries} 次。表可能包含大量数据，请手动清空表后再运行脚本。")
                        raise Exception(f"清空数据失败：超时。表可能包含大量数据，请手动清空表后再运行脚本。")
                else:
                    logger.error(f"清空数据失败: {e}")
                    raise Exception(f"清空数据失败: {e}。请检查表状态后重试。")
        
        # 检查记录数限制（飞书多维表最大支持20000条记录）
        max_records = 20000
        if len(records) > max_records:
            logger.error(f"要写入的记录数 {len(records)} 超过飞书多维表的最大限制 {max_records} 条")
            raise Exception(f"要写入的记录数 {len(records)} 超过飞书多维表的最大限制 {max_records} 条。请减少数据量或分批处理。")
        
        # 写入数据
        logger.info(f"正在写入数据到飞书多维表...")
        written_count = await feishu_client.write_records(records, batch_size=500)
        logger.info(f"✓ 成功写入 {written_count} 条记录到店铺 {shop_name} 的多维表")
        
        # 按负责人创建视图
        logger.info(f"正在按负责人创建视图...")
        try:
            # 获取"运营"字段的ID
            operation_field_info = await feishu_client.get_field_info("运营")
            if not operation_field_info:
                logger.warning("无法获取'运营'字段信息，跳过创建视图")
            else:
                operation_field_id = operation_field_info.get("field_id")
                if not operation_field_id:
                    logger.warning("'运营'字段ID为空，跳过创建视图")
                else:
                    # 收集所有负责人（去重）
                    responsible_set = set()
                    for record in records:
                        responsible = record.get('运营', '').strip()
                        if responsible:
                            responsible_set.add(responsible)
                    
                    # 为每个负责人创建视图
                    created_views = 0
                    for responsible in sorted(responsible_set):
                        view_name = responsible
                        # 构建过滤条件：运营字段等于该负责人
                        # 对于文本字段，使用 "is" 操作符
                        # value 需要是字符串化的JSON数组，使用 ensure_ascii=False 保持中文字符
                        filter_condition = {
                            "conjunction": "and",
                            "conditions": [
                                {
                                    "field_id": operation_field_id,
                                    "operator": "is",  # 文本字段使用 is
                                    "value": json.dumps([responsible], ensure_ascii=False)  # 字符串化的JSON数组，保持中文
                                }
                            ]
                        }
                        logger.debug(f"为负责人 {responsible} 创建视图，过滤条件: {filter_condition}")
                        try:
                            view_id = await feishu_client.ensure_view(view_name, filter_condition)
                            logger.info(f"✓ 成功创建/更新视图: {view_name} (ID: {view_id})")
                            created_views += 1
                        except Exception as e:
                            logger.warning(f"创建视图 {view_name} 失败: {e}")
                    
                    # 检查是否有无负责人的记录
                    has_no_responsible = any(
                        not record.get('运营', '').strip() 
                        for record in records
                    )
                    
                    if has_no_responsible:
                        # 创建"无匹配负责人"视图：运营字段为空
                        # 根据飞书API文档，当operator为isEmpty时，value字段必须留空（不包含在条件中）
                        # 格式参考：https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-view/update-view
                        view_name = "无匹配负责人"
                        filter_condition = {
                            "conjunction": "and",
                            "conditions": [
                                {
                                    "field_id": operation_field_id,
                                    "operator": "isEmpty"
                                    # 注意：isEmpty操作符时，不包含value字段（留空）
                                }
                            ]
                        }
                        logger.debug(f"创建'无匹配负责人'视图，过滤条件: {filter_condition}")
                        try:
                            view_id = await feishu_client.ensure_view(view_name, filter_condition)
                            logger.info(f"✓ 成功创建/更新视图: {view_name} (ID: {view_id})")
                            created_views += 1
                        except Exception as e:
                            logger.warning(f"创建视图 {view_name} 失败: {e}")
                    
                    logger.info(f"✓ 共创建/更新 {created_views} 个负责人视图")
        except Exception as e:
            logger.warning(f"创建负责人视图时出错: {e}，但不影响主流程")
        
        return True
        
    except Exception as e:
        logger.error(f"处理店铺 {shop_name} 失败: {e}", exc_info=True)
        return False


async def main():
    """主函数"""
    logger.info("="*80)
    logger.info("销量数据写入飞书多维表任务")
    logger.info("="*80)
    
    # 检查配置
    if not FEISHU_APP_TOKEN:
        logger.error("请先配置 FEISHU_APP_TOKEN（飞书多维表格的app_token）")
        logger.error("使用方法：")
        logger.error("1. 在飞书中创建一个多维表格")
        logger.error("2. 获取多维表格的app_token（可以从URL中获取，格式如：https://xxx.feishu.cn/base/XXXXXXXXXX）")
        logger.error("3. 在脚本中设置 FEISHU_APP_TOKEN 变量")
        return
    
    # 生成月份标签
    current_date = datetime.now()
    month_labels = get_month_labels(current_date)
    logger.info(f"\n月份字段列表（共 {len(month_labels)} 个）:")
    for i, label in enumerate(month_labels, 1):
        logger.info(f"  {i}. {label}")
    
    # 从数据库读取数据
    logger.info(f"\n正在从数据库读取销量统计数据...")
    try:
        raw_data = read_sales_data_from_db()
        if not raw_data:
            logger.warning("数据库中没有销量统计数据")
            return
    except Exception as e:
        logger.error(f"读取数据库失败: {e}", exc_info=True)
        return
    
    # 聚合数据
    logger.info(f"\n正在聚合数据...")
    aggregated_data = aggregate_sales_by_shop_and_sku(raw_data, month_labels)
    
    if not aggregated_data:
        logger.warning("没有需要处理的数据")
        return
    
    # 按SPU过滤：如果SPU在所有查询月份的总销量为0，则过滤掉该SPU
    logger.info(f"\n正在过滤数据（按SPU过滤：如果SPU在所有查询月份总销量为0则过滤）...")
    filtered_by_sales = filter_skus_by_spu_sales(aggregated_data, month_labels)
    
    if not filtered_by_sales:
        logger.warning("过滤后没有需要处理的数据（所有SPU在所有查询月份的总销量都为0）")
        return
    
    # 过滤掉需要排除的店铺
    filtered_data = {
        shop_name: shop_data 
        for shop_name, shop_data in filtered_by_sales.items() 
        if shop_name not in EXCLUDED_SHOPS
    }
    
    excluded_count = len(aggregated_data) - len(filtered_data)
    if excluded_count > 0:
        excluded_shops = [s for s in EXCLUDED_SHOPS if s in aggregated_data]
        logger.info(f"\n已排除 {excluded_count} 个店铺: {', '.join(excluded_shops)}")
    
    # 处理每个店铺的数据
    logger.info(f"\n开始处理各店铺数据（共 {len(filtered_data)} 个店铺）...")
    success_count = 0
    fail_count = 0
    
    for shop_name, shop_data in filtered_data.items():
        try:
            success = await process_shop_data(shop_name, shop_data, month_labels, FEISHU_APP_TOKEN)
            if success:
                success_count += 1
            else:
                fail_count += 1
        except asyncio.CancelledError:
            logger.error(f"处理店铺 {shop_name} 时被中断")
            fail_count += 1
            raise  # 重新抛出，让调用者知道被中断了
        except KeyboardInterrupt:
            logger.error(f"处理店铺 {shop_name} 时被用户中断")
            fail_count += 1
            raise  # 重新抛出，让调用者知道被中断了
        except Exception as e:
            logger.error(f"处理店铺 {shop_name} 时发生错误: {e}", exc_info=True)
            fail_count += 1
            # 继续处理下一个店铺，不中断整个流程
        
        # 店铺间延迟，避免API限流
        try:
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            logger.warning("延迟被中断，继续处理下一个店铺")
            break
    
    # 输出统计信息
    logger.info("\n" + "="*80)
    logger.info("处理完成！")
    logger.info("="*80)
    logger.info(f"成功处理: {success_count} 个店铺")
    if fail_count > 0:
        logger.warning(f"处理失败: {fail_count} 个店铺")
    logger.info("="*80)


if __name__ == '__main__':
    asyncio.run(main())

