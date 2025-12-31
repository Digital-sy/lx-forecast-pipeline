#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成销量预估表
基于历史销量、库存和采购计划，生成本月及未来3个月的销量预估

数据维度：按 SKU + 店铺 + 月份 汇总
字段：sku, 店铺, 月份, spu, spu颜色, 面料, 实际销量, 预估销量, 趋势因子, 预估方式, 预计下单, 月环比, 月同比, FBA库存, 本地库存
更新策略：每次更新本月+未来3个月的数据（增量更新，保留历史数据）
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

from common import settings, get_logger
from common.database import db_cursor
from utils import normalize_shop_name

logger = get_logger('sales_forecast')


def extract_spu_fields(sku: str) -> Tuple[str, str]:
    """
    从SKU中提取spu和spu颜色
    
    Args:
        sku: SKU字符串，例如 'ZQZ373-BO-M'
    
    Returns:
        Tuple[str, str]: (spu, spu颜色)
    """
    if not sku or not isinstance(sku, str):
        return '', ''
    
    sku = sku.strip()
    if not sku:
        return '', ''
    
    parts = sku.split('-')
    if len(parts) >= 1 and parts[0]:
        spu = parts[0].strip()
    else:
        spu = ''
    
    if len(parts) >= 2 and parts[0] and parts[1]:
        spu_color = '-'.join(parts[:2]).strip()
    elif len(parts) >= 1 and parts[0]:
        spu_color = spu
    else:
        spu_color = ''
    
    return spu, spu_color


def get_target_months() -> List[str]:
    """
    获取目标月份列表（本月+未来3个月）
    
    Returns:
        List[str]: 月份列表，格式：['2025-12', '2026-01', '2026-02', '2026-03']
    """
    now = datetime.now()
    months = []
    
    for i in range(4):  # 本月 + 未来3个月
        year = now.year
        month = now.month + i
        
        # 处理跨年情况
        while month > 12:
            month -= 12
            year += 1
        
        month_str = f"{year}-{month:02d}"
        months.append(month_str)
    
    return months


def load_historical_sales(months_back: int = 15) -> Dict[Tuple[str, str, str], int]:
    """
    从销量统计表加载历史销量数据（用于计算月环比、月同比和预估）
    
    说明：需要至少15个月的数据，因为趋势因子法需要：
    - 今年前三个月的数据
    - 去年前三个月的数据
    - 去年同月的数据
    例如：预估2026年1月需要2024年10月到2025年12月的数据（约15个月）
    
    Args:
        months_back: 查询历史数据的月数（默认15个月，确保趋势因子法有足够数据）
    
    Returns:
        Dict[Tuple[str, str, str], int]: {(SKU, 店铺, 月份): 销量} 的字典
    """
    logger.info(f"正在加载历史销量数据（最近{months_back}个月）...")
    
    now = datetime.now()
    start_date = (now - timedelta(days=30 * months_back)).replace(day=1)
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    sales_data = {}
    
    with db_cursor(dictionary=True) as cursor:
        sql = """
        SELECT 
            SKU,
            店铺,
            DATE_FORMAT(统计日期, '%%Y-%%m') as 月份,
            SUM(销量) as 总销量
        FROM `销量统计_MSKU月度`
        WHERE 统计日期 >= %s
          AND SKU IS NOT NULL AND SKU != '' AND SKU != '无'
          AND 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
        GROUP BY SKU, 店铺, 月份
        """
        
        cursor.execute(sql, (start_date_str,))
        results = cursor.fetchall()
    
    for row in results:
        sku = row['SKU']
        shop = normalize_shop_name(row['店铺'] or '')
        month = row['月份']
        sales = int(row['总销量'] or 0)
        
        key = (sku, shop, month)
        sales_data[key] = sales
    
    logger.info(f"   加载了 {len(sales_data)} 条历史销量记录")
    return sales_data


def load_current_inventory() -> Dict[Tuple[str, str], Dict[str, int]]:
    """
    从库存预估表加载当前库存数据
    
    Returns:
        Dict[Tuple[str, str], Dict[str, int]]: {(SKU, 店铺): {库存状态: 数量}} 的字典
    """
    logger.info("正在加载当前库存数据...")
    
    inventory_data = {}
    
    with db_cursor(dictionary=True) as cursor:
        sql = """
        SELECT 
            sku,
            店铺,
            库存状态,
            SUM(数量) as 总数量
        FROM `库存预估表`
        WHERE sku IS NOT NULL AND sku != '' AND sku != '无'
          AND 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
        GROUP BY sku, 店铺, 库存状态
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
    
    for row in results:
        sku = row['sku']
        shop = normalize_shop_name(row['店铺'] or '')
        status = row['库存状态']
        quantity = int(row['总数量'] or 0)
        
        key = (sku, shop)
        if key not in inventory_data:
            inventory_data[key] = {}
        inventory_data[key][status] = quantity
    
    logger.info(f"   加载了 {len(inventory_data)} 个SKU+店铺的库存数据")
    return inventory_data


def load_purchase_plans(target_months: List[str]) -> Dict[Tuple[str, str, str], int]:
    """
    从下单分析表加载采购计划数据
    
    Args:
        target_months: 目标月份列表
    
    Returns:
        Dict[Tuple[str, str, str], int]: {(SKU, 店铺, 月份): 预计下单数量} 的字典
    """
    logger.info("正在加载采购计划数据...")
    
    # 计算日期范围
    now = datetime.now()
    start_date = now.replace(day=1).strftime('%Y-%m-%d')
    
    # 计算未来3个月后的月末
    future_date = now + timedelta(days=30 * 3)
    if future_date.month == 12:
        end_date = future_date.replace(year=future_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        end_date = future_date.replace(month=future_date.month + 1, day=1) - timedelta(days=1)
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    purchase_data = {}
    
    with db_cursor(dictionary=True) as cursor:
        sql = """
        SELECT 
            SKU,
            店铺,
            DATE_FORMAT(日期, '%%Y-%%m') as 月份,
            SUM(预计下单数量) as 总预计下单数量
        FROM `下单分析表`
        WHERE 日期 >= %s AND 日期 <= %s
          AND SKU IS NOT NULL AND SKU != '' AND SKU != '无'
          AND 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
        GROUP BY SKU, 店铺, DATE_FORMAT(日期, '%%Y-%%m')
        """
        
        cursor.execute(sql, (start_date, end_date_str))
        results = cursor.fetchall()
    
    for row in results:
        sku = row['SKU']
        shop = normalize_shop_name(row['店铺'] or '')
        month = row['月份']
        quantity = int(row['总预计下单数量'] or 0)
        
        key = (sku, shop, month)
        purchase_data[key] = quantity
    
    logger.info(f"   加载了 {len(purchase_data)} 条采购计划记录")
    return purchase_data


def load_fabric_data() -> Dict[str, str]:
    """从产品信息表获取面料信息，按SKU映射"""
    logger.info("正在加载面料数据...")
    
    try:
        with db_cursor(dictionary=True) as cursor:
            sql = """
            SELECT 
                SKU,
                面料
            FROM 产品信息
            WHERE SKU IS NOT NULL AND SKU != ''
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
        
        fabric_dict = {}
        for row in results:
            sku = row['SKU']
            fabric = row['面料'] or ''
            if sku:
                fabric_dict[sku] = fabric
        
        logger.info(f"   加载了 {len(fabric_dict)} 个SKU的面料数据")
        return fabric_dict
        
    except Exception as e:
        logger.warning(f"   获取面料数据失败: {e}")
        return {}


def determine_unified_forecast_method(sku_sales_dict: Dict[str, int],
                                     sku: str, shop: str,
                                     target_months: List[str]) -> Dict[str, Any]:
    """
    为同一SKU+店铺的所有月份确定统一的预估方法和参数
    确保所有月份使用相同的计算方法，保证趋势一致
    
    Args:
        sku_sales_dict: 该SKU+店铺的销量字典 {月份: 销量}
        sku: SKU
        shop: 店铺
        target_months: 目标月份列表
    
    Returns:
        Dict包含：method_type, method_name, trend_factor, base_data等
    """
    if not sku_sales_dict:
        return {'method_type': 'no_data', 'method_name': '无数据', 'trend_factor': 0.0}
    
    now = datetime.now()
    current_month_str = now.strftime('%Y-%m')
    current_year_actual, current_month_actual = map(int, current_month_str.split('-'))
    
    # 策略1：优先使用趋势因子法（同期数据）- 这是最准确的方法
    # 尝试使用3个月、2个月、1个月的同期数据
    for months_count in range(3, 0, -1):
        # 检查第一个目标月份（通常是当前月）
        if not target_months:
            break
        
        first_target_month = target_months[0]
        try:
            target_year, target_month_num = map(int, first_target_month.split('-'))
        except:
            continue
        
        last_year = target_year - 1
        last_year_same_month = f"{last_year}-{target_month_num:02d}"
        last_year_same_month_sales = sku_sales_dict.get(last_year_same_month, 0)
        
        # 检查今年同期月份是否已过去
        current_year_same_month = f"{current_year_actual}-{target_month_num:02d}"
        if current_year_same_month > current_month_str:
            continue  # 今年同期月份还没到，无法使用同期数据
        
        # 获取今年已过去的同期月份
        same_period_months = []
        for i in range(months_count):
            month_num = target_month_num + i
            year = current_year_actual
            if month_num > 12:
                month_num -= 12
                year += 1
            month_str = f"{year}-{month_num:02d}"
            if month_str <= current_month_str:
                same_period_months.append((year, month_num))
        
        if len(same_period_months) < months_count:
            continue  # 数据不够，尝试更少的月份
        
        # 获取今年同期月份的销量
        current_year_sales = []
        for year, month_num in same_period_months:
            month_str = f"{year}-{month_num:02d}"
            if month_str in sku_sales_dict:
                current_year_sales.append(sku_sales_dict[month_str])
        
        # 获取去年前一年同期月份的销量
        last_year_sales = []
        for year, month_num in same_period_months:
            year -= 1
            month_str = f"{year}-{month_num:02d}"
            if month_str in sku_sales_dict:
                last_year_sales.append(sku_sales_dict[month_str])
        
        # 如果去年同月销量>0，且今年和去年前一年同期都有足够数据
        if last_year_same_month_sales > 0:
            if len(current_year_sales) == months_count and len(last_year_sales) == months_count:
                current_avg = sum(current_year_sales) / len(current_year_sales)
                last_avg = sum(last_year_sales) / len(last_year_sales)
                
                if last_avg > 0:
                    trend_factor = current_avg / last_avg
                    return {
                        'method_type': 'trend_factor_same_period',
                        'method_name': f'趋势因子法(同期{months_count}个月)',
                        'trend_factor': round(trend_factor, 4),
                        'months_count': months_count,
                        'same_period_months': same_period_months
                    }
        
        # 如果去年同月销量=0，但今年同期月份有销量
        elif len(current_year_sales) >= months_count and sum(current_year_sales) > 0:
            avg_sales = sum(current_year_sales) / len(current_year_sales)
            return {
                'method_type': 'current_year_avg',
                'method_name': f'今年同期平均(同期{len(current_year_sales)}个月)',
                'trend_factor': 0.0,
                'avg_sales': int(avg_sales),
                'months_count': len(current_year_sales)
            }
    
    # 策略2：如果同期数据不够，使用前几个月的数据
    for months_count in range(3, 0, -1):
        if not target_months:
            break
        
        first_target_month = target_months[0]
        try:
            target_year, target_month_num = map(int, first_target_month.split('-'))
        except:
            continue
        
        last_year = target_year - 1
        last_year_same_month = f"{last_year}-{target_month_num:02d}"
        last_year_same_month_sales = sku_sales_dict.get(last_year_same_month, 0)
        
        if last_year_same_month_sales <= 0:
            continue
        
        # 获取前N个月的月份
        prev_months = []
        for i in range(months_count, 0, -1):
            month_num = target_month_num - i
            year = target_year
            if month_num <= 0:
                month_num += 12
                year -= 1
            prev_months.append((year, month_num))
        
        # 获取今年前N个月的销量
        current_year_sales = []
        for year, month_num in prev_months:
            month_str = f"{year}-{month_num:02d}"
            if month_str in sku_sales_dict:
                current_year_sales.append(sku_sales_dict[month_str])
        
        # 获取去年前N个月的销量
        last_year_sales = []
        for year, month_num in prev_months:
            year -= 1
            month_str = f"{year}-{month_num:02d}"
            if month_str in sku_sales_dict:
                last_year_sales.append(sku_sales_dict[month_str])
        
        if len(current_year_sales) == months_count and len(last_year_sales) == months_count:
            current_avg = sum(current_year_sales) / len(current_year_sales)
            last_avg = sum(last_year_sales) / len(last_year_sales)
            
            if last_avg > 0:
                trend_factor = current_avg / last_avg
                return {
                    'method_type': 'trend_factor_prev_months',
                    'method_name': f'趋势因子法(前{months_count}个月)',
                    'trend_factor': round(trend_factor, 4),
                    'months_count': months_count
                }
    
    # 策略3：趋势走向法（作为备选）
    if not target_months:
        return {'method_type': 'no_data', 'method_name': '无数据', 'trend_factor': 0.0}
    
    first_target_month = target_months[0]
    try:
        target_year, target_month_num = map(int, first_target_month.split('-'))
    except:
        return {'method_type': 'no_data', 'method_name': '无数据', 'trend_factor': 0.0}
    
    recent_sales_data = []
    for i in range(1, 4):
        month_num = target_month_num - i
        year = target_year
        if month_num <= 0:
            month_num += 12
            year -= 1
        month_str = f"{year}-{month_num:02d}"
        if month_str in sku_sales_dict:
            recent_sales_data.append((i, sku_sales_dict[month_str]))
    
    if len(recent_sales_data) >= 2:
        weights = {1: 0.5, 2: 0.3, 3: 0.2}
        total_weight = 0
        weighted_sum = 0
        
        for months_ago, sales in recent_sales_data:
            weight = weights.get(months_ago, 0.2)
            weighted_sum += sales * weight
            total_weight += weight
        
        if total_weight > 0:
            weighted_avg = weighted_sum / total_weight
            return {
                'method_type': 'trend_oriented',
                'method_name': f'趋势走向法({len(recent_sales_data)}个月)',
                'trend_factor': 0.0,
                'weighted_avg': weighted_avg,
                'recent_sales_data': recent_sales_data
            }
    elif len(recent_sales_data) == 1:
        return {
            'method_type': 'trend_oriented',
            'method_name': '趋势走向法(1个月)',
            'trend_factor': 0.0,
            'last_month_sales': recent_sales_data[0][1]
        }
    
    # 策略4：上个月数据
    last_month_num = target_month_num - 1
    last_month_year = target_year
    if last_month_num <= 0:
        last_month_num += 12
        last_month_year -= 1
    
    last_month_str = f"{last_month_year}-{last_month_num:02d}"
    if last_month_str in sku_sales_dict:
        return {
            'method_type': 'last_month',
            'method_name': '上个月数据',
            'trend_factor': 0.0,
            'last_month_sales': sku_sales_dict[last_month_str]
        }
    
    # 策略5：无数据
    return {'method_type': 'no_data', 'method_name': '无数据', 'trend_factor': 0.0}


def calculate_forecast_sales_with_unified_method(sku_sales_dict: Dict[str, int],
                                                 sku: str, shop: str,
                                                 target_month: str,
                                                 unified_method: Dict[str, Any]) -> Tuple[int, str, float]:
    """
    使用统一的预估方法计算指定月份的预估销量
    
    Args:
        sku_sales_dict: 该SKU+店铺的销量字典 {月份: 销量}
        sku: SKU
        shop: 店铺
        target_month: 目标月份（格式：YYYY-MM）
        unified_method: 统一的预估方法配置
    
    Returns:
        Tuple[int, str, float]: (预估销量, 预估方式, 趋势因子)
    """
    try:
        target_year, target_month_num = map(int, target_month.split('-'))
    except:
        return 0, unified_method.get('method_name', '无数据'), unified_method.get('trend_factor', 0.0)
    
    method_type = unified_method.get('method_type', 'no_data')
    
    if method_type == 'no_data':
        return 0, '无数据', 0.0
    
    last_year = target_year - 1
    last_year_same_month = f"{last_year}-{target_month_num:02d}"
    last_year_same_month_sales = sku_sales_dict.get(last_year_same_month, 0)
    
    if method_type == 'trend_factor_same_period':
        # 趋势因子法（同期数据）
        trend_factor = unified_method.get('trend_factor', 1.0)
        if last_year_same_month_sales > 0:
            forecast = int(last_year_same_month_sales * trend_factor)
            return forecast, unified_method.get('method_name', '趋势因子法'), trend_factor
        else:
            # 如果去年同月销量为0，尝试使用今年同期月份的数据
            now = datetime.now()
            current_month_str = now.strftime('%Y-%m')
            current_year_actual, _ = map(int, current_month_str.split('-'))
            current_year_same_month = f"{current_year_actual}-{target_month_num:02d}"
            if current_year_same_month <= current_month_str:
                current_sales = sku_sales_dict.get(current_year_same_month, 0)
                if current_sales > 0:
                    return current_sales, unified_method.get('method_name', '趋势因子法'), trend_factor
    
    elif method_type == 'trend_factor_prev_months':
        # 趋势因子法（前几个月数据）
        trend_factor = unified_method.get('trend_factor', 1.0)
        if last_year_same_month_sales > 0:
            forecast = int(last_year_same_month_sales * trend_factor)
            return forecast, unified_method.get('method_name', '趋势因子法'), trend_factor
    
    elif method_type == 'current_year_avg':
        # 今年同期平均
        avg_sales = unified_method.get('avg_sales', 0)
        return avg_sales, unified_method.get('method_name', '今年同期平均'), 0.0
    
    elif method_type == 'trend_oriented':
        # 趋势走向法
        if 'last_month_sales' in unified_method:
            return unified_method['last_month_sales'], unified_method.get('method_name', '趋势走向法'), 0.0
        elif 'weighted_avg' in unified_method:
            weighted_avg = unified_method['weighted_avg']
            recent_sales_data = unified_method.get('recent_sales_data', [])
            if len(recent_sales_data) >= 2:
                recent_sales_data.sort(key=lambda x: x[0], reverse=True)
                oldest_sales = recent_sales_data[-1][1]
                newest_sales = recent_sales_data[0][1]
                if oldest_sales > 0:
                    trend_ratio = newest_sales / oldest_sales
                    if trend_ratio > 1.1:
                        forecast = int(newest_sales * min(trend_ratio * 0.9, 1.25))
                    elif trend_ratio < 0.9:
                        forecast = int(weighted_avg * max(trend_ratio, 0.8))
                    else:
                        forecast = int(weighted_avg)
                else:
                    forecast = int(weighted_avg)
            else:
                forecast = int(weighted_avg)
            return forecast, unified_method.get('method_name', '趋势走向法'), 0.0
    
    elif method_type == 'last_month':
        # 上个月数据
        last_month_sales = unified_method.get('last_month_sales', 0)
        return last_month_sales, unified_method.get('method_name', '上个月数据'), 0.0
    
    # 默认返回0
    return 0, unified_method.get('method_name', '无数据'), unified_method.get('trend_factor', 0.0)


def calculate_forecast_sales(sku_sales_dict: Dict[str, int],
                            sku: str, shop: str, target_month: str) -> Tuple[int, str, float]:
    """
    计算预估销量、预估方式和趋势因子
    
    优先级：
    1. 趋势因子法：计算今年和去年前三个月的趋势参数，用去年同月销量×趋势因子
    2. 3个月均值：最近3个月的平均销量
    3. 上个月数据：上个月的销量
    4. 无数据：填0
    
    Args:
        sku_sales_dict: 该SKU+店铺的销量字典 {月份: 销量}
        sku: SKU
        shop: 店铺
        target_month: 目标月份（格式：YYYY-MM）
    
    Returns:
        Tuple[int, str, float]: (预估销量, 预估方式, 趋势因子)
    """
    try:
        target_year, target_month_num = map(int, target_month.split('-'))
    except:
        return 0, '无数据', 0.0
    
    if not sku_sales_dict:
        return 0, '无数据', 0.0
    
    # 方法1：趋势因子法（支持1-3个月的数据）
    # 优先使用去年同期同期数据，如果不够再用前几个月的数据
    current_year = target_year
    last_year = target_year - 1
    
    # 获取去年同月的销量
    last_year_same_month = f"{last_year}-{target_month_num:02d}"
    last_year_same_month_sales = sku_sales_dict.get(last_year_same_month, 0)
    
    now = datetime.now()
    current_month_str = now.strftime('%Y-%m')
    current_year_actual, current_month_actual = map(int, current_month_str.split('-'))
    
    # 检查今年同期月份是否已过去
    current_year_same_month = f"{current_year_actual}-{target_month_num:02d}"
    current_year_same_month_sales = sku_sales_dict.get(current_year_same_month, 0) if current_year_same_month <= current_month_str else 0
    
    # 策略1：优先使用今年已过去的同期月份数据（考虑季节性）
    # 例如：预估2026年1月，使用2025年1-3月（今年已过去）vs 2024年1-3月（去年同期）的数据
    # 这样可以捕捉到季节性增长趋势（如1-3月的爆发式增长）
    # 关键：对于同一SPU的所有未来月份，使用相同的同期月份数据计算趋势因子，确保趋势一致
    if current_year_same_month <= current_month_str:
        # 今年同期月份已过去，可以使用同期数据
        # 优先使用3个月的数据，确保所有月份使用相同的趋势因子
        for months_count in range(3, 0, -1):  # 优先尝试3个月，然后2个月，最后1个月
            # 获取今年已过去的同期月份（从目标月份开始，往后推N-1个月）
            # 例如：预估1月，用今年1月、2月、3月（如果这些月份已经过去）
            same_period_months = []
            for i in range(months_count):
                month_num = target_month_num + i
                year = current_year_actual
                if month_num > 12:
                    month_num -= 12
                    year += 1
                month_str = f"{year}-{month_num:02d}"
                # 只使用已过去的月份
                if month_str <= current_month_str:
                    same_period_months.append((year, month_num))
            
            if len(same_period_months) < months_count:
                continue  # 数据不够，尝试更少的月份
            
            # 获取今年同期月份的销量
            current_year_sales = []
            for year, month_num in same_period_months:
                month_str = f"{year}-{month_num:02d}"
                if month_str in sku_sales_dict:
                    current_year_sales.append(sku_sales_dict[month_str])
            
            # 获取去年前一年同期月份的销量
            last_year_sales = []
            for year, month_num in same_period_months:
                year -= 1  # 前一年
                month_str = f"{year}-{month_num:02d}"
                if month_str in sku_sales_dict:
                    last_year_sales.append(sku_sales_dict[month_str])
            
            # 情况1：如果去年同月销量>0，且今年和去年前一年同期都有足够数据，使用趋势因子法
            if last_year_same_month_sales > 0:
                if len(current_year_sales) == months_count and len(last_year_sales) == months_count:
                    current_avg = sum(current_year_sales) / len(current_year_sales)
                    last_avg = sum(last_year_sales) / len(last_year_sales)
                    
                    if last_avg > 0:
                        # 计算趋势因子（这个因子对于同一SPU的所有月份都是相同的）
                        trend_factor = current_avg / last_avg
                        
                        # 使用趋势因子法计算：去年同月销量 × 趋势因子
                        # 这样不同月份的预估会跟随去年同月的趋势，但乘以相同的趋势因子
                        forecast = int(last_year_same_month_sales * trend_factor)
                        method_name = f'趋势因子法(同期{months_count}个月)'
                        return forecast, method_name, round(trend_factor, 4)
            
            # 情况2：如果去年同月销量=0，但今年同期月份有销量，使用今年同期月份的数据
            elif len(current_year_sales) >= months_count and current_year_same_month_sales > 0:
                # 如果今年同期月份有销量，直接使用今年同期月份的数据
                forecast = current_year_same_month_sales
                method_name = f'今年同期数据(同期{len(current_year_sales)}个月)'
                return forecast, method_name, 0.0
            
            # 情况3：如果今年同期月份有销量，但去年同月销量=0，使用今年同期月份的平均值
            elif len(current_year_sales) >= months_count and sum(current_year_sales) > 0:
                # 使用今年同期月份的平均值（对于所有月份都使用相同的平均值，确保趋势一致）
                forecast = int(sum(current_year_sales) / len(current_year_sales))
                method_name = f'今年同期平均(同期{len(current_year_sales)}个月)'
                return forecast, method_name, 0.0
    
    # 策略2：如果去年同月销量>0，但同期数据不够，使用前几个月的数据（相对于目标月份）
    if last_year_same_month_sales > 0:
        for months_count in range(3, 0, -1):  # 优先尝试3个月，然后2个月，最后1个月
            # 获取前N个月的月份（相对于目标月份）
            prev_months = []
            for i in range(months_count, 0, -1):  # N个月前、...、1个月前
                month_num = target_month_num - i
                year = current_year
                if month_num <= 0:
                    month_num += 12
                    year -= 1
                prev_months.append((year, month_num))
            
            # 获取今年前N个月的销量
            current_year_sales = []
            for year, month_num in prev_months:
                month_str = f"{year}-{month_num:02d}"
                if month_str in sku_sales_dict:
                    current_year_sales.append(sku_sales_dict[month_str])
            
            # 获取去年前N个月的销量
            last_year_sales = []
            for year, month_num in prev_months:
                year -= 1  # 去年
                month_str = f"{year}-{month_num:02d}"
                if month_str in sku_sales_dict:
                    last_year_sales.append(sku_sales_dict[month_str])
            
            # 如果今年和去年前N个月都有数据，且去年前N个月总和不为0
            if len(current_year_sales) == months_count and len(last_year_sales) == months_count:
                current_avg = sum(current_year_sales) / len(current_year_sales)
                last_avg = sum(last_year_sales) / len(last_year_sales)
                
                if last_avg > 0:
                    # 计算趋势因子
                    trend_factor = current_avg / last_avg
                    
                    # 使用趋势因子法计算
                    forecast = int(last_year_same_month_sales * trend_factor)
                    method_name = f'趋势因子法(前{months_count}个月)'
                    return forecast, method_name, round(trend_factor, 4)
    
    # 方法2：趋势走向法（考虑换季问题，使用趋势而不是平均值）
    # 获取最近3个月的数据（不包括目标月份本身）
    recent_sales_data = []  # [(月份序号, 销量)]，月份序号用于计算趋势
    for i in range(1, 4):  # 1个月前、2个月前、3个月前
        month_num = target_month_num - i
        year = current_year
        if month_num <= 0:
            month_num += 12
            year -= 1
        
        month_str = f"{year}-{month_num:02d}"
        if month_str in sku_sales_dict:
            recent_sales_data.append((i, sku_sales_dict[month_str]))  # i表示几个月前
    
    if len(recent_sales_data) >= 2:
        # 使用加权平均和趋势走向（考虑换季，更近的月份影响更大）
        # 权重：1个月前=0.5, 2个月前=0.3, 3个月前=0.2
        weights = {1: 0.5, 2: 0.3, 3: 0.2}
        total_weight = 0
        weighted_sum = 0
        
        for months_ago, sales in recent_sales_data:
            weight = weights.get(months_ago, 0.2)
            weighted_sum += sales * weight
            total_weight += weight
        
        if total_weight > 0:
            # 计算加权平均作为基础值
            weighted_avg = weighted_sum / total_weight
            
            # 计算趋势走向：按时间顺序排序，分析趋势
            recent_sales_data.sort(key=lambda x: x[0], reverse=True)  # 从远到近排序
            
            # 如果有至少2个月的数据，计算趋势
            if len(recent_sales_data) >= 2:
                # 获取最近2个月的数据（最远的和最近的）
                oldest_sales = recent_sales_data[-1][1]  # 最远的月份
                newest_sales = recent_sales_data[0][1]    # 最近的月份
                
                # 计算趋势比率
                if oldest_sales > 0:
                    trend_ratio = newest_sales / oldest_sales
                    
                    # 根据趋势调整预测值
                    if trend_ratio > 1.1:  # 上升趋势（增长超过10%）
                        # 预测值基于最近一个月，但考虑趋势，限制增长不超过25%
                        forecast = int(newest_sales * min(trend_ratio * 0.9, 1.25))
                    elif trend_ratio < 0.9:  # 下降趋势（下降超过10%）
                        # 预测值基于加权平均，但考虑下降趋势
                        forecast = int(weighted_avg * max(trend_ratio, 0.8))
                    else:  # 平稳趋势
                        forecast = int(weighted_avg)
                else:
                    forecast = int(weighted_avg)
            else:
                forecast = int(weighted_avg)
            
            method_name = f'趋势走向法({len(recent_sales_data)}个月)'
            return forecast, method_name, 0.0
    elif len(recent_sales_data) == 1:
        # 只有1个月的数据，直接使用
        forecast = recent_sales_data[0][1]
        return forecast, '趋势走向法(1个月)', 0.0
    
    # 方法3：上个月数据
    # 获取上个月的销量
    last_month_num = target_month_num - 1
    last_month_year = current_year
    if last_month_num <= 0:
        last_month_num += 12
        last_month_year -= 1
    
    last_month_str = f"{last_month_year}-{last_month_num:02d}"
    if last_month_str in sku_sales_dict:
        return sku_sales_dict[last_month_str], '上个月数据', 0.0
    
    # 方法4：无数据
    return 0, '无数据', 0.0


def calculate_month_over_month(sku_sales_dict: Dict[str, int],
                              current_month: str) -> Optional[float]:
    """
    计算月环比（当前月 vs 上个月）
    
    Args:
        sku_sales_dict: 该SKU+店铺的销量字典 {月份: 销量}
        current_month: 当前月份（格式：YYYY-MM）
    
    Returns:
        Optional[float]: 月环比（百分比），如果无法计算则返回None
    """
    # 计算上个月
    try:
        year, month = map(int, current_month.split('-'))
        if month == 1:
            last_month = f"{year-1}-12"
        else:
            last_month = f"{year}-{month-1:02d}"
    except:
        return None
    
    current_sales = sku_sales_dict.get(current_month, 0)
    last_sales = sku_sales_dict.get(last_month, 0)
    
    if last_sales == 0:
        return None
    
    mom = ((current_sales - last_sales) / last_sales) * 100
    return round(mom, 2)


def calculate_year_over_year(sku_sales_dict: Dict[str, int],
                             current_month: str) -> Optional[float]:
    """
    计算月同比（当前月 vs 去年同期）
    
    Args:
        sku_sales_dict: 该SKU+店铺的销量字典 {月份: 销量}
        current_month: 当前月份（格式：YYYY-MM）
    
    Returns:
        Optional[float]: 月同比（百分比），如果无法计算则返回None
    """
    # 计算去年同期
    try:
        year, month = map(int, current_month.split('-'))
        last_year_month = f"{year-1}-{month:02d}"
    except:
        return None
    
    current_sales = sku_sales_dict.get(current_month, 0)
    last_year_sales = sku_sales_dict.get(last_year_month, 0)
    
    if last_year_sales == 0:
        return None
    
    yoy = ((current_sales - last_year_sales) / last_year_sales) * 100
    return round(yoy, 2)


def generate_sales_forecast_table() -> List[Dict[str, Any]]:
    """
    生成销量预估表数据
    
    Returns:
        List[Dict[str, Any]]: 销量预估表数据列表
    """
    logger.info("="*80)
    logger.info("生成销量预估表")
    logger.info("="*80)
    
    # 1. 获取目标月份（本月+未来3个月）
    target_months = get_target_months()
    logger.info(f"目标月份: {', '.join(target_months)}")
    
    # 2. 加载历史销量数据（至少15个月，确保趋势因子法有足够数据）
    historical_sales = load_historical_sales(months_back=15)
    
    # 3. 加载当前库存数据
    current_inventory = load_current_inventory()
    
    # 4. 加载采购计划数据
    purchase_plans = load_purchase_plans(target_months)
    
    # 5. 加载面料数据
    fabric_dict = load_fabric_data()
    
    # 6. 获取所有SKU+店铺组合
    all_sku_shops = set()
    
    # 从历史销量中获取
    for (sku, shop, month) in historical_sales.keys():
        if month in target_months:
            all_sku_shops.add((sku, shop))
    
    # 从库存中获取
    for (sku, shop) in current_inventory.keys():
        all_sku_shops.add((sku, shop))
    
    # 从采购计划中获取
    for (sku, shop, month) in purchase_plans.keys():
        if month in target_months:
            all_sku_shops.add((sku, shop))
    
    logger.info(f"共找到 {len(all_sku_shops)} 个SKU+店铺组合")
    
    # 7. 预先构建按SKU+店铺索引的销量字典（优化性能）
    logger.info("正在构建销量索引字典（优化性能）...")
    sku_shop_sales_dict = defaultdict(dict)  # {(sku, shop): {月份: 销量}}
    for (s, sh, month), sales in historical_sales.items():
        sku_shop_sales_dict[(s, sh)][month] = sales
    logger.info(f"   构建了 {len(sku_shop_sales_dict)} 个SKU+店铺的销量索引")
    
    # 7.5. 构建按SPU+店铺聚合的销量字典（用于预估）
    logger.info("正在构建SPU+店铺聚合销量字典...")
    spu_shop_sales_dict = defaultdict(dict)  # {(spu, shop): {月份: 销量}}
    spu_shop_skus = defaultdict(set)  # {(spu, shop): {sku1, sku2, ...}}
    
    for (sku, shop), sales_by_month in sku_shop_sales_dict.items():
        spu, _ = extract_spu_fields(sku)
        if spu:
            spu_shop_key = (spu, shop)
            spu_shop_skus[spu_shop_key].add(sku)
            for month, sales in sales_by_month.items():
                if month not in spu_shop_sales_dict[spu_shop_key]:
                    spu_shop_sales_dict[spu_shop_key][month] = 0
                spu_shop_sales_dict[spu_shop_key][month] += sales
    
    logger.info(f"   构建了 {len(spu_shop_sales_dict)} 个SPU+店铺的聚合销量索引")
    
    # 8. 按SPU+店铺计算预估，然后分配到各个SKU
    result_list = []
    now = datetime.now()
    current_month = now.strftime('%Y-%m')
    
    # 先按SPU+店铺计算预估
    logger.info("正在按SPU+店铺计算预估...")
    spu_shop_forecasts = {}  # {(spu, shop, month): (预估销量, 预估方式, 趋势因子)}
    
    for (spu, shop), spu_sales_dict in spu_shop_sales_dict.items():
        # 为同一SPU+店铺的所有月份确定统一的预估方法（确保趋势一致）
        unified_method = determine_unified_forecast_method(
            spu_sales_dict, spu, shop, target_months
        )
        
        # 为每个目标月份计算预估
        for month in target_months:
            forecast_sales, forecast_method, trend_factor = calculate_forecast_sales_with_unified_method(
                spu_sales_dict, spu, shop, month, unified_method
            )
            spu_shop_forecasts[(spu, shop, month)] = (forecast_sales, forecast_method, trend_factor)
    
    logger.info(f"   完成了 {len(spu_shop_forecasts)} 个SPU+店铺+月份的预估计算")
    
    # 9. 将预估分配到各个SKU（按历史比例）
    logger.info("正在将预估分配到各个SKU...")
    total_combinations = len(all_sku_shops)
    processed = 0
    
    for sku, shop in all_sku_shops:
        processed += 1
        if processed % 1000 == 0:
            logger.info(f"   正在处理: {processed}/{total_combinations} ({processed*100//total_combinations}%)")
        
        # 获取该SKU+店铺的销量字典
        sku_sales_dict = sku_shop_sales_dict.get((sku, shop), {})
        # 从SKU中提取spu和spu颜色
        spu, spu_color = extract_spu_fields(sku)
        
        if not spu:
            # 如果无法提取SPU，跳过
            continue
        
        # 获取面料
        fabric = fabric_dict.get(sku, '无')
        
        # 获取当前库存（只对当前月有效，未来月份为0）
        inventory_info = current_inventory.get((sku, shop), {})
        fba_inventory = inventory_info.get('FBA可售', 0) + inventory_info.get('FBA在途', 0)
        local_inventory = inventory_info.get('本地可用量', 0) + inventory_info.get('本地待到货', 0)
        
        # 获取该SPU+店铺下所有SKU的历史销量（用于计算分配比例）
        spu_shop_key = (spu, shop)
        all_skus_in_spu = spu_shop_skus.get(spu_shop_key, {sku})
        spu_total_sales_by_month = {}  # {月份: SPU总销量}
        
        for other_sku in all_skus_in_spu:
            other_sku_sales = sku_shop_sales_dict.get((other_sku, shop), {})
            for month, sales in other_sku_sales.items():
                if month not in spu_total_sales_by_month:
                    spu_total_sales_by_month[month] = 0
                spu_total_sales_by_month[month] += sales
        
        # 为每个目标月份生成记录
        for month in target_months:
            # 实际销量（只有历史月份才有，未来月份为0）
            actual_sales = sku_sales_dict.get(month, 0) if month <= current_month else 0
            
            # 获取SPU+店铺的预估
            spu_forecast, forecast_method, trend_factor = spu_shop_forecasts.get(
                (spu, shop, month), (0, '无数据', 0.0)
            )
            
            # 计算该SKU在SPU中的历史占比（用于分配预估）
            # 使用最近3个月的平均占比，如果没有数据则平均分配
            sku_ratio = 0.0
            recent_months = []
            for i in range(1, 4):  # 最近3个月
                try:
                    year, month_num = map(int, month.split('-'))
                    check_month_num = month_num - i
                    check_year = year
                    if check_month_num <= 0:
                        check_month_num += 12
                        check_year -= 1
                    check_month = f"{check_year}-{check_month_num:02d}"
                    if check_month <= current_month and check_month in spu_total_sales_by_month:
                        recent_months.append(check_month)
                except:
                    pass
            
            if recent_months:
                # 计算最近几个月的平均占比
                total_ratio = 0.0
                for recent_month in recent_months:
                    sku_sales = sku_sales_dict.get(recent_month, 0)
                    spu_total = spu_total_sales_by_month.get(recent_month, 0)
                    if spu_total > 0:
                        total_ratio += sku_sales / spu_total
                if len(recent_months) > 0:
                    sku_ratio = total_ratio / len(recent_months)
            
            # 如果没有历史占比数据，则平均分配
            if sku_ratio == 0.0 or len(all_skus_in_spu) == 0:
                sku_ratio = 1.0 / max(len(all_skus_in_spu), 1)
            
            # 分配预估销量
            forecast_sales = int(spu_forecast * sku_ratio)
            
            # 预计下单
            planned_purchase = purchase_plans.get((sku, shop, month), 0)
            
            # 月环比（只有当前月及之前有数据才能计算）
            mom = None
            if month <= current_month:
                mom = calculate_month_over_month(sku_sales_dict, month)
            
            # 月同比（只有当前月及之前有数据才能计算）
            yoy = None
            if month <= current_month:
                yoy = calculate_year_over_year(sku_sales_dict, month)
            
            # 库存（只有当前月使用实际库存，未来月份为0）
            if month == current_month:
                month_fba_inventory = fba_inventory
                month_local_inventory = local_inventory
            else:
                month_fba_inventory = 0
                month_local_inventory = 0
            
            # 将月份转换为日期（每月1号）
            try:
                year, month_num = map(int, month.split('-'))
                date_value = datetime(year, month_num, 1).strftime('%Y-%m-%d')
            except:
                date_value = None
            
            record = {
                'SKU': sku,
                '店铺': shop if shop and shop.strip() else '无',
                '月份': month,
                '日期': date_value,
                'spu': spu if spu and spu.strip() else '无',
                'spu颜色': spu_color if spu_color and spu_color.strip() else '无',
                '面料': fabric if fabric and fabric.strip() else '无',
                '实际销量': actual_sales,
                '预估销量': forecast_sales,
                '趋势因子': trend_factor,
                '预估方式': forecast_method,
                '预计下单': planned_purchase,
                '月环比': mom,
                '月同比': yoy,
                'FBA库存': month_fba_inventory,
                '本地库存': month_local_inventory,
                '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            result_list.append(record)
    
    logger.info(f"共生成 {len(result_list)} 条销量预估记录")
    return result_list


def create_forecast_table_if_not_exists() -> None:
    """创建销量预估表（如果不存在）"""
    with db_cursor(dictionary=False) as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS `销量预估表` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `SKU` VARCHAR(255),
            `店铺` VARCHAR(255),
            `月份` VARCHAR(50) COMMENT '月份，格式：YYYY-MM',
            `日期` DATE COMMENT '日期，每月1号',
            `spu` VARCHAR(255) COMMENT '从SKU第一个-之前提取',
            `spu颜色` VARCHAR(255) COMMENT '从SKU第二个-之前提取',
            `面料` VARCHAR(255),
            `实际销量` INT DEFAULT 0 COMMENT '实际销量（历史月份有值，未来月份为0）',
            `预估销量` INT DEFAULT 0 COMMENT '预估销量',
            `趋势因子` DECIMAL(10,4) DEFAULT 0 COMMENT '趋势因子（仅趋势因子法有值，其他为0）',
            `预估方式` VARCHAR(50) COMMENT '预估方式：趋势因子法/趋势走向法/上个月数据/无数据',
            `预计下单` INT DEFAULT 0 COMMENT '预计下单数量',
            `月环比` DECIMAL(10,2) COMMENT '月环比（百分比），当前月vs上个月',
            `月同比` DECIMAL(10,2) COMMENT '月同比（百分比），当前月vs去年同期',
            `FBA库存` INT DEFAULT 0 COMMENT 'FBA库存（当前月有值，未来月份为0）',
            `本地库存` INT DEFAULT 0 COMMENT '本地库存（当前月有值，未来月份为0）',
            `更新时间` DATETIME,
            INDEX idx_sku_shop_month (SKU, 店铺, 月份),
            INDEX idx_date (日期),
            INDEX idx_month (月份),
            INDEX idx_forecast_method (预估方式)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销量预估表-按SKU+店铺+月份汇总'
        """
        
        cursor.execute(sql)
        logger.info("   销量预估表结构检查完成")
        
        # 为已存在的表添加趋势因子字段（如果不存在）
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '销量预估表' 
                AND COLUMN_NAME = '趋势因子'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    ALTER TABLE `销量预估表` 
                    ADD COLUMN `趋势因子` DECIMAL(10,4) DEFAULT 0 COMMENT '趋势因子（仅趋势因子法有值，其他为0）' 
                    AFTER `预估销量`
                """)
                logger.info("   已添加字段: 趋势因子")
        except Exception as e:
            logger.warning(f"   检查/添加趋势因子字段时出错: {e}")
        
        # 为已存在的表添加日期字段（如果不存在）
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '销量预估表' 
                AND COLUMN_NAME = '日期'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    ALTER TABLE `销量预估表` 
                    ADD COLUMN `日期` DATE COMMENT '日期，每月1号' 
                    AFTER `月份`
                """)
                logger.info("   已添加字段: 日期")
                
                # 更新现有数据的日期字段
                cursor.execute("""
                    UPDATE `销量预估表` 
                    SET `日期` = STR_TO_DATE(CONCAT(`月份`, '-01'), '%%Y-%%m-%%d')
                    WHERE `日期` IS NULL
                """)
                logger.info("   已更新现有数据的日期字段")
        except Exception as e:
            logger.warning(f"   检查/添加日期字段时出错: {e}")


def save_to_database(data_list: List[Dict[str, Any]], target_months: List[str]) -> None:
    """保存数据到销量预估表"""
    logger.info("\n正在保存到数据库...")
    
    if not data_list:
        logger.warning("   没有数据需要保存")
        return
    
    # 创建表
    create_forecast_table_if_not_exists()
    
    with db_cursor(dictionary=False) as cursor:
        # 删除目标月份的旧数据（增量更新，只删除本月+未来3个月，保留历史数据）
        logger.info(f"   正在删除目标月份的旧数据: {', '.join(target_months)}...")
        placeholders = ','.join(['%s'] * len(target_months))
        delete_sql = f"DELETE FROM `销量预估表` WHERE `月份` IN ({placeholders})"
        cursor.execute(delete_sql, target_months)
        deleted_count = cursor.rowcount
        logger.info(f"   已删除 {deleted_count} 条旧数据（仅删除目标月份，保留历史数据）")
        
        # 插入新数据
        logger.info("   正在插入新数据...")
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `销量预估表` ({fields}) VALUES ({values_placeholder})"
        
        batch_size = 200
        for i in range(0, len(data_list), batch_size):
            batch = [tuple(row.values()) for row in data_list[i:i+batch_size]]
            cursor.executemany(sql, batch)
            logger.info(f"   已录入 {min(i+batch_size, len(data_list))} 条...")
    
    logger.info(f"   成功写入 {len(data_list)} 条新数据到销量预估表")
    logger.info(f"   ✅ 保留了目标月份之外的历史数据")


def print_statistics(data_list: List[Dict[str, Any]], target_months: List[str]) -> None:
    """打印统计信息"""
    logger.info("\n" + "="*80)
    logger.info("统计信息：")
    
    logger.info(f"  更新策略: 增量更新（本月+未来3个月）")
    logger.info(f"  目标月份: {', '.join(target_months)}")
    logger.info(f"  本次记录数: {len(data_list)} 条")
    
    if not data_list:
        return
    
    # 统计各月份记录数
    month_counts = defaultdict(int)
    for record in data_list:
        month_counts[record['月份']] += 1
    
    logger.info(f"\n  各月份记录数：")
    for month in sorted(month_counts.keys()):
        logger.info(f"    {month}: {month_counts[month]} 条")
    
    # 统计预估方式分布
    method_counts = defaultdict(int)
    for record in data_list:
        method_counts[record['预估方式']] += 1
    
    logger.info(f"\n  预估方式分布：")
    for method, count in sorted(method_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"    {method}: {count} 条")
    
    # 统计有实际销量的记录
    actual_sales_count = sum(1 for r in data_list if r['实际销量'] > 0)
    forecast_sales_count = sum(1 for r in data_list if r['预估销量'] > 0)
    trend_factor_count = sum(1 for r in data_list if r['趋势因子'] > 0)
    
    logger.info(f"\n  销量统计：")
    logger.info(f"    有实际销量: {actual_sales_count} 条")
    logger.info(f"    有预估销量: {forecast_sales_count} 条")
    logger.info(f"    使用趋势因子: {trend_factor_count} 条")
    
    logger.info("="*80)


def main():
    """主函数"""
    logger.info("="*80)
    logger.info("生成销量预估表")
    logger.info("="*80)
    
    # 验证配置
    if not settings.validate():
        logger.error("配置验证失败，请检查.env文件")
        return
    
    try:
        # 获取目标月份
        target_months = get_target_months()
        
        # 生成销量预估表数据
        logger.info("正在生成销量预估表数据...")
        forecast_data = generate_sales_forecast_table()
        
        if not forecast_data:
            logger.warning("没有数据需要保存")
            return
        
        # 保存到数据库
        save_to_database(forecast_data, target_months)
        
        # 打印统计信息
        print_statistics(forecast_data, target_months)
        
        logger.info("\n✅ 销量预估表生成完成！")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ 生成销量预估表失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()

