#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
更新利润报表计算字段
包括：
1. 商品成本附加费 = 采购成本 * 0.15
2. 实际销量 = FBA销量 + FBM销量 + FBM补换货量 + FBA补换货量 - 退货量（可售） - 退货量（不可售）
3. 实际头程费用 = -(单品毛重/1000) * 实际销量 * 头程单价（直接计算为负数）
4. 头程成本附加费 = 实际头程费用 * 0.15（使用负数计算）
5. 录入费用单头程 = 实际头程费用 - 头程成本（使用负数计算）
6. 平台费用 = 塑料包装费 + FBA卖家退回费 + FBA仓储费入库缺陷费 + 库存调整费用 + 合作承运费 + 
             入库配置费 + 超量仓储费 + FBA销毁费 + FBA移除费 + 入仓手续费 + 标签费 + 订阅费 + 
             秒杀费 + 优惠券 + 早期评论人计划 + vine + 其他仓储费 + 月度仓储费 + 月度仓储费差异 + 
             长期仓储费 + 长期仓储费差异 + 平台费 + FBA发货费 + FBA发货费(多渠道) + 
             其他订单费用 + FBA国际物流货运费 + 调整费用 + 平台费退款额 + 发货费退款额 + 
             其他订单费退款额 + 运输标签费退款 + 交易费用退款额 + 积分费用
7. 营收 = 包装收入 + 买家交易保障索赔 + 积分抵减收入 + 清算收入 + 亚马逊运费赔偿 + Safe-T索赔 + 
         Netco交易 + 赔偿收入 + 追索收入 + 其他收入 + 清算调整 + 混合VAT收入 + 
         FBM销售退款额 + FBA销售退款额 + 买家运费退款额 + 买家包装退款额 + 促销折扣退款额 + 
         买家拒付 + 积分抵减退回 + FBA销售额 + FBM销售额 + 买家运费 + 促销折扣 + FBA库存赔偿
8. 广告费用 = SP广告费 + SD广告费 + SB广告费 + SBV广告费 + 广告费用减免
9. 汇损 = -((营收 + 平台费用 + 广告费用) × 1%)

头程单价匹配逻辑（按优先级）：
1. 店铺 + 负责人 + 统计日期（上个月 -> 上上个月）
2. 店铺 + 统计日期（取该店铺所有负责人的平均值，上个月 -> 上上个月）
3. 品牌前缀 + 统计日期（取该品牌前缀下所有店铺的平均值，例如"RR-"开头的所有店铺，上个月 -> 上上个月）
注意：不匹配当月，因为不会出现当月的头程单价

单品毛重匹配逻辑（按优先级）：
1. SKU匹配：直接从产品管理表通过SKU匹配单品毛重
2. SPU平均匹配：如果SKU匹配不到，则使用SPU匹配，取相同SPU的所有记录的单品毛重平均值
"""

import sys
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import wraps

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor

# 获取日志记录器
logger = get_logger('update_profit_report_calc')


def timing_decorator(func):
    """性能监控装饰器 - 记录函数执行时间"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        
        # 根据耗时选择日志级别
        if elapsed > 60:
            logger.warning(f"⏱️  {func.__name__} 执行耗时: {elapsed:.2f}秒 ({elapsed/60:.1f}分钟)")
        elif elapsed > 10:
            logger.info(f"⏱️  {func.__name__} 执行耗时: {elapsed:.2f}秒")
        else:
            logger.debug(f"⏱️  {func.__name__} 执行耗时: {elapsed:.2f}秒")
        
        return result
    return wrapper


def safe_decimal(value, default=0.0):
    """
    安全地将值转换为高精度Decimal类型
    
    Args:
        value: 要转换的值
        default: 默认值
        
    Returns:
        Decimal类型的值
    """
    if value is None:
        return Decimal(str(default))
    
    try:
        # 如果已经是Decimal类型，直接返回
        if isinstance(value, Decimal):
            return value
        
        # 转换为字符串再转为Decimal，避免浮点数精度问题
        return Decimal(str(float(value) if value else default))
    except (ValueError, TypeError, AttributeError):
        return Decimal(str(default))


def get_optimal_batch_size(total_records: int, operation_type: str = 'update') -> int:
    """
    根据记录数和操作类型动态计算最佳批次大小
    
    Args:
        total_records: 总记录数
        operation_type: 操作类型 ('update', 'query', 'insert')
        
    Returns:
        最佳批次大小
    """
    if operation_type == 'query':
        # 查询操作：根据数据量调整
        if total_records < 10000:
            return total_records
        elif total_records < 100000:
            return 10000
        else:
            return 50000
    elif operation_type == 'update':
        # 更新操作：固定较大批次以提升性能
        return 10000
    elif operation_type == 'insert':
        # 插入操作：相对保守
        return 1000
    else:
        return 5000


def precise_round(value, precision=10):
    """
    高精度四舍五入
    
    Args:
        value: 要四舍五入的值
        precision: 精度（小数位数，默认10位）
        
    Returns:
        四舍五入后的Decimal值
    """
    if isinstance(value, (int, float)):
        value = Decimal(str(value))
    elif not isinstance(value, Decimal):
        value = safe_decimal(value)
    
    # 使用ROUND_HALF_UP进行四舍五入
    return value.quantize(Decimal('0.' + '0' * precision), rounding=ROUND_HALF_UP)


def extract_brand_prefix(shop: str) -> Optional[str]:
    """
    从店铺名称中提取品牌前缀
    
    店铺格式：品牌缩写-站点，例如 "RR-US" -> "RR-", "JQ-UK" -> "JQ-"
    
    Args:
        shop: 店铺名称
        
    Returns:
        品牌前缀（包含"-"），如果无法提取则返回None
    """
    if not shop or not isinstance(shop, str):
        return None
    
    # 查找第一个"-"的位置
    dash_index = shop.find('-')
    if dash_index > 0:
        # 提取前缀（包含"-"）
        return shop[:dash_index + 1]
    
    return None


def extract_sku_from_msku(msku: str) -> Optional[str]:
    """
    从MSKU中提取SKU（当SKU为空时使用）
    
    提取规则：
    1. 取MSKU中"-FBA"或"-FBM"之前的字符
    2. 去掉前两个字符
    
    例如：EUZQZ369-SC-M-FBA-TD-SY022 -> ZQZ369-SC-M
    
    Args:
        msku: MSKU
        
    Returns:
        SKU，如果无法提取则返回None
    """
    if not msku or not isinstance(msku, str):
        return None
    
    # 查找"-FBA"或"-FBM"的位置
    fba_index = msku.find('-FBA')
    fbm_index = msku.find('-FBM')
    
    # 找到第一个匹配的位置
    cut_index = -1
    if fba_index > 0:
        cut_index = fba_index
    if fbm_index > 0 and (cut_index == -1 or fbm_index < cut_index):
        cut_index = fbm_index
    
    if cut_index > 0:
        # 提取"-FBA"或"-FBM"之前的部分
        prefix = msku[:cut_index]
        # 去掉前两个字符
        if len(prefix) > 2:
            return prefix[2:]
    
    return None


def extract_spu_from_sku(sku: str) -> Optional[str]:
    """
    从SKU中提取SPU
    
    SPU格式：SKU第一个"-"之前的部分，例如 "ABC-001-RED" -> "ABC"
    
    Args:
        sku: SKU
        
    Returns:
        SPU，如果无法提取则返回None
    """
    if not sku or not isinstance(sku, str):
        return None
    
    # 查找第一个"-"的位置
    dash_index = sku.find('-')
    if dash_index > 0:
        # 提取SPU（第一个"-"之前的部分）
        return sku[:dash_index]
    
    return None


def get_freight_unit_price(shop: str, person: str, stat_date: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """
    获取头程单价
    
    匹配逻辑：
    1. 优先匹配：店铺 + 负责人 + 统计日期（本月 -> 上个月 -> 上上个月）
    2. 如果负责人为空或匹配不到，则匹配：店铺 + 统计日期（取该店铺所有负责人的平均值）
    3. 如果店铺匹配不到，则匹配：品牌前缀 + 统计日期（取该品牌前缀下所有店铺的平均值）
    
    Args:
        shop: 店铺名称
        person: 负责人
        stat_date: 统计日期（格式：YYYY-MM-DD 或 YYYY-MM-01）
        
    Returns:
        Tuple[头程单价, 匹配逻辑, 匹配日期]
    """
    if not shop or not stat_date:
        return None, None, None
    
    # 解析统计日期
    try:
        if isinstance(stat_date, str):
            date_obj = datetime.strptime(stat_date, '%Y-%m-%d')
        elif isinstance(stat_date, datetime):
            date_obj = stat_date
        else:
            # 如果是date类型，转换为datetime
            date_obj = datetime.combine(stat_date, datetime.min.time())
    except:
        return None, None, None
    
    # 本月第一天
    from dateutil.relativedelta import relativedelta
    current_month = date_obj.replace(day=1).strftime('%Y-%m-%d')
    # 上月第一天（使用relativedelta确保正确计算）
    last_month = (date_obj.replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
    # 上上个月第一天
    last_last_month = (date_obj.replace(day=1) - relativedelta(months=2)).strftime('%Y-%m-%d')
    
    # 日期匹配顺序：本月 -> 上个月 -> 上上个月
    months_to_try = [current_month, last_month, last_last_month]
    
    with db_cursor() as cursor:
        # 策略1: 如果有负责人，尝试匹配 店铺 + 负责人 + 统计日期
        if person and person.strip():
            sql = """
            SELECT `头程单价`, `统计日期`
            FROM `头程单价`
            WHERE `店铺` = %s 
              AND `负责人` = %s 
              AND `统计日期` = %s
            LIMIT 1
            """
            # 按优先级尝试：上个月 -> 上上个月
            for month_date in months_to_try:
                cursor.execute(sql, (shop, person, month_date))
                result = cursor.fetchone()
                
                if result and result.get('头程单价'):
                    return (
                        float(result['头程单价']),
                        f"负责人匹配({person})",
                        result['统计日期']
                    )
        
        # 策略2: 匹配 店铺 + 统计日期（取平均值）
        sql = """
        SELECT AVG(`头程单价`) as avg_price, `统计日期`
        FROM `头程单价`
        WHERE `店铺` = %s 
          AND `统计日期` = %s
          AND `头程单价` IS NOT NULL
          AND `头程单价` > 0
        GROUP BY `统计日期`
        LIMIT 1
        """
        # 按优先级尝试：上个月 -> 上上个月
        for month_date in months_to_try:
            cursor.execute(sql, (shop, month_date))
            result = cursor.fetchone()
            
            if result and result.get('avg_price'):
                return (
                    float(result['avg_price']),
                    "店铺平均",
                    result['统计日期']
                )
        
        # 策略3: 匹配 品牌前缀 + 统计日期（取该品牌前缀下所有店铺的平均值）
        brand_prefix = extract_brand_prefix(shop)
        if brand_prefix:
            sql = """
            SELECT AVG(`头程单价`) as avg_price, `统计日期`
            FROM `头程单价`
            WHERE `店铺` LIKE %s 
              AND `统计日期` = %s
              AND `头程单价` IS NOT NULL
              AND `头程单价` > 0
            GROUP BY `统计日期`
            LIMIT 1
            """
            # 按优先级尝试：上个月 -> 上上个月
            for month_date in months_to_try:
                cursor.execute(sql, (f"{brand_prefix}%", month_date))
                result = cursor.fetchone()
                
                if result and result.get('avg_price'):
                    return (
                        float(result['avg_price']),
                        f"品牌前缀平均({brand_prefix})",
                        result['统计日期']
                    )
    
    return None, "未匹配", None


def get_product_weight(sku: str) -> Optional[float]:
    """
    从产品管理表获取单品毛重
    
    匹配逻辑：
    1. 优先使用SKU匹配
    2. 如果SKU匹配不到，则使用SPU匹配，取相同SPU的所有记录的单品毛重平均值
    
    Args:
        sku: SKU
        
    Returns:
        单品毛重（克）
    """
    if not sku:
        return None
    
    with db_cursor() as cursor:
        # 策略1: 使用SKU匹配（使用TRIM处理空格）
        sql = """
        SELECT `单品毛重`
        FROM `产品管理`
        WHERE TRIM(`SKU`) = TRIM(%s)
          AND `SKU` IS NOT NULL
          AND `SKU` != ''
          AND `单品毛重` IS NOT NULL
          AND `单品毛重` > 0
        LIMIT 1
        """
        cursor.execute(sql, (sku,))
        result = cursor.fetchone()
        
        if result and result.get('单品毛重'):
            try:
                return float(result['单品毛重'])
            except:
                pass
        
        # 策略2: 使用SPU匹配，取平均值
        spu = extract_spu_from_sku(sku)
        if spu:
            sql = """
            SELECT AVG(`单品毛重`) as avg_weight
            FROM `产品管理`
            WHERE TRIM(`SPU`) = TRIM(%s)
              AND `SPU` IS NOT NULL
              AND `SPU` != ''
              AND `单品毛重` IS NOT NULL
              AND `单品毛重` > 0
            LIMIT 1
            """
            cursor.execute(sql, (spu,))
            result = cursor.fetchone()
            
            if result and result.get('avg_weight'):
                try:
                    return float(result['avg_weight'])
                except:
                    pass
    
    return None


def calculate_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    计算各个字段
    
    Args:
        record: 利润报表记录
        
    Returns:
        包含计算结果的字典
    """
    # 提取必要字段
    sku = record.get('SKU', '')
    shop = record.get('店铺', '')
    person = record.get('负责人', '')
    stat_date = record.get('统计日期', '')
    
    # 采购成本
    cg_price = float(record.get('采购成本', 0) or 0)
    
    # 头程成本
    cg_transport = float(record.get('头程成本', 0) or 0)
    
    # 销量数据
    fba_sales = float(record.get('FBA销量', 0) or 0)
    fbm_sales = float(record.get('FBM销量', 0) or 0)
    fba_reship = float(record.get('FBA补换货量', 0) or 0)
    fbm_reship = float(record.get('FBM补换货量', 0) or 0)
    return_saleable = float(record.get('退货量(可售)', 0) or 0)
    return_unsaleable = float(record.get('退货量(不可售)', 0) or 0)
    
    # 计算1: 商品成本附加费 = 采购成本 * 0.15
    cg_price_additional = round(cg_price * 0.15, 10)
    
    # 计算2: 实际销量 = FBA销量 + FBM销量 + FBM补换货量 + FBA补换货量 - 退货量（可售） - 退货量（不可售）
    actual_quantity = fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable
    
    # 计算3: 获取头程单价
    freight_price, match_logic, match_date = get_freight_unit_price(shop, person, stat_date)
    
    # 计算4: 获取单品毛重
    product_weight = get_product_weight(sku)
    
    # 计算5: 实际头程费用 = -(单品毛重/1000) * 实际销量 * 头程单价（直接计算为负数）
    actual_freight_fee = 0.0
    if freight_price and product_weight and actual_quantity:
        actual_freight_fee = -round((product_weight / 1000) * actual_quantity * freight_price, 10)
    
    # 计算6: 头程成本附加费 = 实际头程费用 * 0.15（使用负数）
    cg_transport_additional = round(actual_freight_fee * 0.15, 10)
    
    # 计算7: 录入费用单头程 = 实际头程费用 - 头程成本（使用负数）
    recorded_freight = round(actual_freight_fee - cg_transport, 10)
    
    return {
        '商品成本附加费': cg_price_additional,
        '实际销量': actual_quantity,
        '头程单价': freight_price,
        '匹配逻辑': match_logic,
        '匹配日期': match_date,
        '单品毛重': product_weight,
        '实际头程费用': actual_freight_fee,
        '头程成本附加费': cg_transport_additional,
        '录入费用单头程': recorded_freight,
    }


@timing_decorator
def _process_batch(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    处理一批记录，计算字段并收集未匹配数据
    
    Args:
        records: 需要处理的记录列表
        
    Returns:
        Tuple[更新记录列表, 未匹配记录列表, 统计信息]
    """
    update_records = []
    unmatched_records = []
    stats = {
        '总记录数': len(records),
        '跳过实际销量为0': 0,
        '有头程单价': 0,
        '无头程单价': 0,
        '有单品毛重': 0,
        '无单品毛重': 0,
        'SKU匹配单品毛重': 0,
        'SPU平均匹配单品毛重': 0,
        '负责人匹配': 0,
        '店铺平均': 0,
        '品牌前缀平均': 0,
        '未匹配': 0,
    }
    
    # 将Decimal转换为float时，确保保留10位小数精度
    def decimal_to_float(value, precision=10):
        """将Decimal转换为float，保留指定精度"""
        if value is None:
            return None
        if isinstance(value, Decimal):
            # 先四舍五入到指定精度，再转换为float
            return float(value.quantize(Decimal('0.' + '0' * precision), rounding=ROUND_HALF_UP))
        return float(value)
    
    for i, record in enumerate(records, 1):
        if i % 5000 == 0:
            logger.info(f"  已处理 {i}/{len(records)} 条...")
        
        # 从JOIN结果中提取数据（使用高精度Decimal）
        cg_price = safe_decimal(record.get('采购成本', 0))
        cg_transport = safe_decimal(record.get('头程成本', 0))
        
        # 销量数据（使用高精度Decimal）
        fba_sales = safe_decimal(record.get('FBA销量', 0))
        fbm_sales = safe_decimal(record.get('FBM销量', 0))
        fba_reship = safe_decimal(record.get('FBA补换货量', 0))
        fbm_reship = safe_decimal(record.get('FBM补换货量', 0))
        return_saleable = safe_decimal(record.get('退货量(可售)', 0))
        return_unsaleable = safe_decimal(record.get('退货量(不可售)', 0))
        
        # 计算2: 实际销量（保留10位小数精度）
        actual_quantity = precise_round(
            fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable,
            10
        )
        
        # 注意：不再跳过实际销量为0的记录，确保所有记录的单品毛重都能被更新
        if actual_quantity == 0:
            stats['跳过实际销量为0'] += 1
            # 即使实际销量为0，仍然需要更新单品毛重等字段，所以不continue
        
        # 计算1: 商品成本附加费
        cg_price_additional = precise_round(cg_price * Decimal('0.15'), 10)
        
        # 计算1.5: 平台费用（所有平台相关费用的总和）
        # 根据新的需求，包含所有列出的费用项目（使用高精度Decimal）
        plastic_packaging_fee = safe_decimal(record.get('塑料包装费', 0))
        fba_seller_return_fee = safe_decimal(record.get('FBA卖家退回费', 0))
        fba_storage_defect_fee = safe_decimal(record.get('FBA仓储费入库缺陷费', 0))
        inventory_adjustment_fee = safe_decimal(record.get('库存调整费用', 0))
        cooperation_carrier_fee = safe_decimal(record.get('合作承运费', 0))
        inbound_config_fee = safe_decimal(record.get('入库配置费', 0))
        excess_storage_fee = safe_decimal(record.get('超量仓储费', 0))
        fba_disposal_fee = safe_decimal(record.get('FBA销毁费', 0))
        fba_removal_fee = safe_decimal(record.get('FBA移除费', 0))
        inbound_handling_fee = safe_decimal(record.get('入仓手续费', 0))
        label_fee = safe_decimal(record.get('标签费', 0))
        subscription_fee = safe_decimal(record.get('订阅费', 0))
        flash_sale_fee = safe_decimal(record.get('秒杀费', 0))
        coupon_fee = safe_decimal(record.get('优惠券', 0))
        early_reviewer_program = safe_decimal(record.get('早期评论人计划', 0))
        vine_fee = safe_decimal(record.get('vine', 0))
        other_storage_fee = safe_decimal(record.get('其他仓储费', 0))
        monthly_storage_fee = safe_decimal(record.get('月度仓储费', 0))
        monthly_storage_fee_diff = safe_decimal(record.get('月度仓储费差异', 0))
        long_term_storage_fee = safe_decimal(record.get('长期仓储费', 0))
        long_term_storage_fee_diff = safe_decimal(record.get('长期仓储费差异', 0))
        platform_fee = safe_decimal(record.get('平台费', 0))
        fba_shipping_fee_fba = safe_decimal(record.get('FBA发货费', 0))
        fba_shipping_fee_multi = safe_decimal(record.get('FBA发货费(多渠道)', 0))
        other_order_fee = safe_decimal(record.get('其他订单费用', 0))
        fba_intl_logistics_fee = safe_decimal(record.get('FBA国际物流货运费', 0))
        adjustment_fee = safe_decimal(record.get('调整费用', 0))
        platform_fee_refund = safe_decimal(record.get('平台费退款额', 0))
        shipping_fee_refund = safe_decimal(record.get('发货费退款额', 0))
        other_order_fee_refund = safe_decimal(record.get('其他订单费退款额', 0))
        shipping_label_refund = safe_decimal(record.get('运输标签费退款', 0))
        transaction_fee_refund = safe_decimal(record.get('交易费用退款额', 0))
        points_fee = safe_decimal(record.get('积分费用', 0))
        
        total_platform_fee = precise_round(
            plastic_packaging_fee + fba_seller_return_fee + fba_storage_defect_fee + 
            inventory_adjustment_fee + cooperation_carrier_fee + inbound_config_fee + 
            excess_storage_fee + fba_disposal_fee + fba_removal_fee + inbound_handling_fee + 
            label_fee + subscription_fee + flash_sale_fee + coupon_fee + 
            early_reviewer_program + vine_fee + other_storage_fee + monthly_storage_fee + 
            monthly_storage_fee_diff + long_term_storage_fee + long_term_storage_fee_diff + 
            platform_fee + fba_shipping_fee_fba + fba_shipping_fee_multi + other_order_fee + 
            fba_intl_logistics_fee + adjustment_fee + platform_fee_refund + 
            shipping_fee_refund + other_order_fee_refund + shipping_label_refund + 
            transaction_fee_refund + points_fee,
            10
        )
        
        # 计算1.6: 营收（所有营收相关费用的总和）（使用高精度Decimal）
        packaging_income = safe_decimal(record.get('包装收入', 0))
        buyer_guarantee_claim = safe_decimal(record.get('买家交易保障索赔', 0))
        points_deduction_income = safe_decimal(record.get('积分抵减收入', 0))
        settlement_income = safe_decimal(record.get('清算收入', 0))
        amazon_shipping_compensation = safe_decimal(record.get('亚马逊运费赔偿', 0))
        safe_t_claim = safe_decimal(record.get('Safe-T索赔', 0))
        netco_transaction = safe_decimal(record.get('Netco交易', 0))
        compensation_income = safe_decimal(record.get('赔偿收入', 0))
        recovery_income = safe_decimal(record.get('追索收入', 0))
        other_income = safe_decimal(record.get('其他收入', 0))
        settlement_adjustment = safe_decimal(record.get('清算调整', 0))
        mixed_vat_income = safe_decimal(record.get('混合VAT收入', 0))
        fbm_sales_refund = safe_decimal(record.get('FBM销售退款额', 0))
        fba_sales_refund = safe_decimal(record.get('FBA销售退款额', 0))
        buyer_shipping_refund = safe_decimal(record.get('买家运费退款额', 0))
        buyer_packaging_refund = safe_decimal(record.get('买家包装退款额', 0))
        promotion_discount_refund = safe_decimal(record.get('促销折扣退款额', 0))
        buyer_chargeback = safe_decimal(record.get('买家拒付', 0))
        points_deduction_return = safe_decimal(record.get('积分抵减退回', 0))
        fba_sales_amount = safe_decimal(record.get('FBA销售额', 0))
        fbm_sales_amount = safe_decimal(record.get('FBM销售额', 0))
        buyer_shipping = safe_decimal(record.get('买家运费', 0))
        promotion_discount = safe_decimal(record.get('促销折扣', 0))
        fba_inventory_compensation = safe_decimal(record.get('FBA库存赔偿', 0))
        
        total_revenue = precise_round(
            packaging_income + buyer_guarantee_claim + points_deduction_income + 
            settlement_income + amazon_shipping_compensation + safe_t_claim + 
            netco_transaction + compensation_income + recovery_income + other_income + 
            settlement_adjustment + mixed_vat_income + fbm_sales_refund + fba_sales_refund + 
            buyer_shipping_refund + buyer_packaging_refund + promotion_discount_refund + 
            buyer_chargeback + points_deduction_return + fba_sales_amount + fbm_sales_amount + 
            buyer_shipping + promotion_discount + fba_inventory_compensation,
            10
        )
        
        # 计算1.7: 广告费用（所有广告相关费用的总和）（使用高精度Decimal）
        sp_advertising = safe_decimal(record.get('SP广告费', 0))
        sd_advertising = safe_decimal(record.get('SD广告费', 0))
        sb_advertising = safe_decimal(record.get('SB广告费', 0))
        sbv_advertising = safe_decimal(record.get('SBV广告费', 0))
        advertising_fee_reduction = safe_decimal(record.get('广告费用减免', 0))
        
        total_advertising_fee = precise_round(
            sp_advertising + sd_advertising + sb_advertising + sbv_advertising + advertising_fee_reduction,
            10
        )
        
        # 计算1.8: 汇损 = -((营收 + 平台费用 + 广告费用) × 1%)
        exchange_loss = precise_round(
            -(total_revenue + total_platform_fee + total_advertising_fee) * Decimal('0.01'),
            10
        )
        
        # 计算3: 获取头程单价（优先使用负责人匹配，其次店铺平均，最后品牌前缀平均）
        # 日期优先级：本月 -> 上个月 -> 上上个月
        freight_price = None
        match_logic = "未匹配"
        match_date = None
        
        # 负责人匹配（优先级：本月 -> 上个月 -> 上上个月）（使用高精度Decimal，保留10位小数）
        if record.get('头程单价_负责人_本月'):
            freight_price = precise_round(safe_decimal(record['头程单价_负责人_本月']), 10)
            match_logic = f"负责人匹配({record.get('负责人', '')})"
            match_date = record.get('匹配日期_负责人_本月')
            stats['负责人匹配'] += 1
        elif record.get('头程单价_负责人_上月'):
            freight_price = precise_round(safe_decimal(record['头程单价_负责人_上月']), 10)
            match_logic = f"负责人匹配({record.get('负责人', '')})"
            match_date = record.get('匹配日期_负责人_上月')
            stats['负责人匹配'] += 1
        elif record.get('头程单价_负责人_上上月'):
            freight_price = precise_round(safe_decimal(record['头程单价_负责人_上上月']), 10)
            match_logic = f"负责人匹配({record.get('负责人', '')})"
            match_date = record.get('匹配日期_负责人_上上月')
            stats['负责人匹配'] += 1
        # 店铺平均（优先级：本月 -> 上个月 -> 上上个月）
        elif record.get('头程单价_店铺平均_本月'):
            freight_price = precise_round(safe_decimal(record['头程单价_店铺平均_本月']), 10)
            match_logic = "店铺平均"
            match_date = record.get('匹配日期_店铺平均_本月')
            stats['店铺平均'] += 1
        elif record.get('头程单价_店铺平均_上月'):
            freight_price = precise_round(safe_decimal(record['头程单价_店铺平均_上月']), 10)
            match_logic = "店铺平均"
            match_date = record.get('匹配日期_店铺平均_上月')
            stats['店铺平均'] += 1
        elif record.get('头程单价_店铺平均_上上月'):
            freight_price = precise_round(safe_decimal(record['头程单价_店铺平均_上上月']), 10)
            match_logic = "店铺平均"
            match_date = record.get('匹配日期_店铺平均_上上月')
            stats['店铺平均'] += 1
        # 品牌前缀平均（优先级：本月 -> 上个月 -> 上上个月）
        elif record.get('头程单价_品牌前缀平均_本月'):
            freight_price = precise_round(safe_decimal(record['头程单价_品牌前缀平均_本月']), 10)
            brand_prefix = record.get('品牌前缀_本月', '')
            match_logic = f"品牌前缀平均({brand_prefix})"
            match_date = record.get('匹配日期_品牌前缀平均_本月')
            stats['品牌前缀平均'] += 1
        elif record.get('头程单价_品牌前缀平均_上月'):
            freight_price = precise_round(safe_decimal(record['头程单价_品牌前缀平均_上月']), 10)
            brand_prefix = record.get('品牌前缀_上月', '')
            match_logic = f"品牌前缀平均({brand_prefix})"
            match_date = record.get('匹配日期_品牌前缀平均_上月')
            stats['品牌前缀平均'] += 1
        elif record.get('头程单价_品牌前缀平均_上上月'):
            freight_price = precise_round(safe_decimal(record['头程单价_品牌前缀平均_上上月']), 10)
            brand_prefix = record.get('品牌前缀_上上月', '')
            match_logic = f"品牌前缀平均({brand_prefix})"
            match_date = record.get('匹配日期_品牌前缀平均_上上月')
            stats['品牌前缀平均'] += 1
        else:
            freight_price = None
            stats['未匹配'] += 1
        
        # 计算4: 获取单品毛重
        # 匹配逻辑：
        # 1. 优先使用SKU匹配：如果产品管理表中有该SKU且单品毛重有效（不为NULL且>0），则使用SKU匹配
        # 2. 如果SKU匹配不到（产品管理表中没有该SKU或单品毛重无效），则使用SPU平均值
        product_weight = None
        
        # 策略1: 使用SKU匹配（产品管理表中必须有该SKU的有效数据）（使用高精度Decimal，保留10位小数）
        sku_value = record.get('SKU', '')
        # 如果SKU为空，尝试从MSKU中提取SKU
        if not sku_value or not sku_value.strip():
            msku_value = record.get('MSKU', '')
            if msku_value:
                extracted_sku = extract_sku_from_msku(msku_value)
                if extracted_sku:
                    sku_value = extracted_sku
        
        if record.get('单品毛重_SKU'):
            try:
                product_weight = precise_round(safe_decimal(record['单品毛重_SKU']), 10)
                stats['SKU匹配单品毛重'] += 1
            except:
                product_weight = None
        
        # 策略2: 如果SKU匹配不到（产品管理表中没有该SKU或单品毛重无效），使用SPU平均值
        if not product_weight and record.get('单品毛重_SPU平均'):
            try:
                product_weight = precise_round(safe_decimal(record['单品毛重_SPU平均']), 10)
                stats['SPU平均匹配单品毛重'] += 1
            except:
                product_weight = None
        
        # 调试：检查特定SKU的匹配情况
        if sku_value and 'EUBQ054-NB-XL' in sku_value and not product_weight:
            logger.warning(f"⚠️  SKU {sku_value} 未匹配到单品毛重 - 单品毛重_SKU: {record.get('单品毛重_SKU')}, 单品毛重_SPU平均: {record.get('单品毛重_SPU平均')}")
        
        # 统计
        if freight_price:
            stats['有头程单价'] += 1
        else:
            stats['无头程单价'] += 1
        
        if product_weight:
            stats['有单品毛重'] += 1
        else:
            stats['无单品毛重'] += 1
        
        # 计算5: 实际头程费用 = -(单品毛重/1000) * 实际销量 * 头程单价（直接计算为负数）
        actual_freight_fee = Decimal('0.0')
        if freight_price and product_weight and actual_quantity:
            actual_freight_fee = -precise_round((product_weight / Decimal('1000')) * actual_quantity * freight_price, 10)
        
        # 计算6: 头程成本附加费 = 实际头程费用 * 0.15（使用负数）
        cg_transport_additional = precise_round(actual_freight_fee * Decimal('0.15'), 10)
        
        # 计算7: 录入费用单头程 = 实际头程费用 - 头程成本（使用负数）
        recorded_freight = precise_round(actual_freight_fee - cg_transport, 10)
        
        update_records.append({
            'id': record['id'],
            '商品成本附加费': decimal_to_float(cg_price_additional, 10),
            '实际头程费用': decimal_to_float(actual_freight_fee, 10),
            '头程成本附加费': decimal_to_float(cg_transport_additional, 10),
            '录入费用单头程': decimal_to_float(recorded_freight, 10),
            '实际销量': decimal_to_float(actual_quantity, 10),
            '头程单价': decimal_to_float(freight_price, 10),
            '匹配逻辑': match_logic,
            '匹配日期': match_date,
            '单品毛重': decimal_to_float(product_weight, 10),
            '平台费用': decimal_to_float(total_platform_fee, 10),
            '营收': decimal_to_float(total_revenue, 10),
            '广告费用': decimal_to_float(total_advertising_fee, 10),
            '汇损': decimal_to_float(exchange_loss, 10),
        })
        
        # 收集未匹配数据（只要不是第一优先级匹配的，就记录）
        # 头程单价：如果不是"负责人匹配"，就记录
        # 单品毛重：如果不是"SKU匹配"，就记录
        freight_match_type = None
        weight_match_type = None
        
        # 判断头程单价匹配类型（如果不是第一优先级"负责人匹配"，则记录）
        if not freight_price:
            freight_match_type = "未匹配"
        elif "负责人匹配" not in match_logic:
            # 店铺平均或品牌前缀平均
            if "店铺平均" in match_logic:
                freight_match_type = "店铺平均"
            elif "品牌前缀平均" in match_logic:
                freight_match_type = "品牌前缀平均"
            else:
                freight_match_type = "未匹配"
        
        # 判断单品毛重匹配类型（如果不是第一优先级"SKU匹配"，则记录）
        if not product_weight:
            weight_match_type = "未匹配"
        elif not record.get('单品毛重_SKU'):
            # 使用了SPU平均匹配
            weight_match_type = "SPU平均匹配"
        
        # 如果头程或毛重不是第一优先级匹配，则记录
        if freight_match_type or weight_match_type:
            msku = record.get('MSKU', '')
            sku = record.get('SKU', '')
            person = record.get('负责人', '')
            stat_date = record.get('统计日期', '')
            create_date = datetime.now()
            
            # 如果头程是第一优先级（负责人匹配），则显示"负责人匹配"
            # 如果毛重是第一优先级（SKU匹配），则显示"SKU匹配"
            unmatched_records.append({
                'MSKU': msku or '',  # 确保不为None
                'SKU': sku or '',
                '负责人': person or '',
                '头程': freight_match_type if freight_match_type else "负责人匹配",
                '毛重': weight_match_type if weight_match_type else "SKU匹配",
                '统计日期': stat_date,
                '创建日期': create_date,
            })
    
    return update_records, unmatched_records, stats


@timing_decorator
def update_product_weight_only(records: List[Dict[str, Any]], batch_size: int = 10000) -> int:
    """
    批量更新单品毛重（高性能版：直接更新，不检查值是否变化）
    
    Args:
        records: 需要更新的记录列表（包含id和单品毛重）
        batch_size: 批次大小（增大到10000以大幅提升性能）
        
    Returns:
        更新的记录数（MySQL会自动处理值相同的情况，rowcount会正确反映）
    """
    if not records:
        return 0
    
    updated_count = 0
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    with db_cursor() as cursor:
        # 直接更新所有记录，不检查值是否变化
        # MySQL会自动处理值相同的情况（rowcount会正确反映实际更新的记录数）
        # 这种方式比WHERE条件检查快得多
        sql = """
        UPDATE `利润报表`
        SET `单品毛重` = %s
        WHERE `id` = %s
        """
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_data = [
                (r.get('单品毛重'), r['id'])
                for r in batch
            ]
            
            try:
                # 使用executemany批量执行（大幅提升性能）
                cursor.executemany(sql, batch_data)
                # rowcount表示实际更新的记录数（MySQL会自动处理值相同的情况）
                updated_count += cursor.rowcount
            except Exception as e:
                logger.error(f"批量更新单品毛重失败（批次 {i//batch_size + 1}）: {e}")
                # 如果批量更新失败，尝试逐条更新这一批
                for r in batch:
                    try:
                        product_weight = r.get('单品毛重')
                        record_id = r['id']
                        cursor.execute(sql, (product_weight, record_id))
                        updated_count += cursor.rowcount
                    except Exception as e2:
                        logger.error(f"更新记录 ID={r['id']} 单品毛重失败: {e2}")
            
            # 减少日志输出频率（每5批输出一次）
            if (i // batch_size) % 5 == 0 or i + batch_size >= len(records):
                logger.info(f"已处理单品毛重 {min(i + batch_size, len(records))}/{len(records)} 条记录...")
    
    return updated_count


@timing_decorator
def update_profit_report_batch(records: List[Dict[str, Any]], batch_size: int = 10000) -> int:
    """
    批量更新利润报表（优化版：增大批次大小，简化WHERE条件）
    
    Args:
        records: 需要更新的记录列表（包含id和计算字段）
        batch_size: 批次大小（增大到5000以提升性能）
        
    Returns:
        更新的记录数
    """
    if not records:
        return 0
    
    updated_count = 0
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    with db_cursor() as cursor:
        # 直接更新所有记录，不检查值是否变化
        # MySQL会自动处理值相同的情况（rowcount会正确反映实际更新的记录数）
        # 这种方式比复杂的WHERE条件检查快得多
        sql = """
        UPDATE `利润报表`
        SET 
            `商品成本附加费` = %s,
            `实际头程费用` = %s,
            `头程成本附加费` = %s,
            `录入费用单头程` = %s,
            `实际销量` = %s,
            `头程单价` = %s,
            `匹配逻辑` = %s,
            `匹配日期` = %s,
            `平台费用` = %s,
            `营收` = %s,
            `广告费用` = %s,
            `汇损` = %s
        WHERE `id` = %s
        """
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_data = [
                (
                    r['商品成本附加费'],
                    r['实际头程费用'],
                    r['头程成本附加费'],
                    r['录入费用单头程'],
                    r['实际销量'],
                    r['头程单价'],
                    r['匹配逻辑'],
                    r['匹配日期'],
                    r['平台费用'],
                    r['营收'],
                    r['广告费用'],
                    r['汇损'],
                    r['id']
                )
                for r in batch
            ]
            
            try:
                # 执行批量更新（只更新值有变化的记录）
                cursor.executemany(sql, batch_data)
                batch_updated = cursor.rowcount
                updated_count += batch_updated
            except Exception as e:
                logger.error(f"批量更新失败（批次 {i//batch_size + 1}）: {e}")
                # 如果批量更新失败，跳过这一批
                continue
            
            # 显示进度
            current_batch = (i // batch_size) + 1
            progress = (min(i + batch_size, len(records)) / len(records)) * 100
            if (i // batch_size) % 5 == 0 or i + batch_size >= len(records):
                logger.info(f"📊 利润报表更新进度: {min(i + batch_size, len(records))}/{len(records)} ({progress:.1f}%) "
                           f"[批次 {current_batch}/{total_batches}] 本批更新 {batch_updated} 条")
    
    return updated_count


def add_indexes_if_not_exist():
    """检查并添加数据库索引（如果不存在）"""
    indexes_to_add = [
        {
            'table': '利润报表',
            'index_name': 'idx_sku',
            'columns': ['SKU'],
            'comment': 'SKU索引，用于JOIN产品管理表'
        },
        {
            'table': '利润报表',
            'index_name': 'idx_stat_date',
            'columns': ['统计日期'],
            'comment': '统计日期索引，用于日期范围查询'
        },
        {
            'table': '利润报表',
            'index_name': 'idx_shop',
            'columns': ['店铺'],
            'comment': '店铺索引，用于JOIN头程单价表'
        },
        {
            'table': '利润报表',
            'index_name': 'idx_shop_person_date',
            'columns': ['店铺', '负责人', '统计日期'],
            'comment': '店铺+负责人+统计日期复合索引，用于头程单价匹配'
        },
        {
            'table': '产品管理',
            'index_name': 'idx_sku_trim',
            'columns': ['SKU'],
            'comment': 'SKU索引，用于JOIN利润报表'
        },
        {
            'table': '产品管理',
            'index_name': 'idx_spu',
            'columns': ['SPU'],
            'comment': 'SPU索引，用于SPU平均匹配'
        },
        {
            'table': '头程单价',
            'index_name': 'idx_shop_person_date',
            'columns': ['店铺', '负责人', '统计日期'],
            'comment': '店铺+负责人+统计日期复合索引，用于头程单价匹配'
        },
        {
            'table': '头程单价',
            'index_name': 'idx_shop_date',
            'columns': ['店铺', '统计日期'],
            'comment': '店铺+统计日期复合索引，用于店铺平均匹配'
        },
    ]
    
    with db_cursor() as cursor:
        for idx_info in indexes_to_add:
            try:
                table_name = idx_info['table']
                index_name = idx_info['index_name']
                columns = idx_info['columns']
                comment = idx_info.get('comment', '')
                
                # 检查索引是否存在
                cursor.execute("""
                    SELECT COUNT(*) as cnt FROM information_schema.STATISTICS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                      AND TABLE_NAME = %s
                      AND INDEX_NAME = %s
                """, (table_name, index_name))
                
                result = cursor.fetchone()
                exists = result.get('cnt', 0) > 0 if result else False
                
                if not exists:
                    # 检查表是否存在
                    cursor.execute("""
                        SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                        WHERE TABLE_SCHEMA = DATABASE() 
                          AND TABLE_NAME = %s
                    """, (table_name,))
                    table_result = cursor.fetchone()
                    table_exists = table_result.get('cnt', 0) > 0 if table_result else False
                    
                    if table_exists:
                        # 添加索引
                        columns_str = ', '.join([f"`{col}`" for col in columns])
                        sql = f"""
                        CREATE INDEX `{index_name}` ON `{table_name}` ({columns_str})
                        """
                        cursor.execute(sql)
                        logger.info(f"✅ 已添加索引: {table_name}.{index_name} ({columns_str})")
                    else:
                        logger.warning(f"⚠️  表不存在，跳过索引: {table_name}.{index_name}")
                else:
                    logger.debug(f"索引已存在: {table_name}.{index_name}")
                    
            except Exception as e:
                logger.error(f"❌ 添加索引 {idx_info.get('index_name', 'unknown')} 失败: {e}")


def add_calculated_fields_if_not_exist():
    """检查并添加计算字段（如果不存在）"""
    fields_to_add = [
        ('实际销量', 'DECIMAL(30,10)', '实际销量 = FBA销量 + FBM销量 + 补换货量 - 退货量'),
        ('头程单价', 'DECIMAL(30,10)', '从头程单价表匹配的单价'),
        ('匹配逻辑', 'VARCHAR(100)', '头程单价匹配逻辑（负责人匹配/店铺平均/品牌前缀平均/未匹配）'),
        ('匹配日期', 'DATE', '头程单价匹配到的统计日期'),
        ('单品毛重', 'DECIMAL(30,10)', '从产品管理表匹配的单品毛重（克）'),
        ('平台费用', 'DECIMAL(30,10)', '平台费用合计（平台费+平台费退款额+FBA发货费等）'),
        ('营收', 'DECIMAL(30,10)', '营收合计（包装收入+买家交易保障索赔+FBA销售额+FBM销售额等）'),
        ('广告费用', 'DECIMAL(30,10)', '广告费用合计（SP广告费+SD广告费+SB广告费+SBV广告费+广告费用减免）'),
        ('汇损', 'DECIMAL(30,10)', '汇损 = -((营收+平台费用+广告费用)×1%)'),
    ]
    
    with db_cursor() as cursor:
        for field_name, field_type, comment in fields_to_add:
            try:
                # 检查字段是否存在
                cursor.execute("""
                    SELECT COUNT(*) as cnt FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                      AND TABLE_NAME = '利润报表'
                      AND COLUMN_NAME = %s
                """, (field_name,))
                
                result = cursor.fetchone()
                exists = result.get('cnt', 0) > 0 if result else False
                
                if not exists:
                    # 添加字段
                    sql = f"""
                    ALTER TABLE `利润报表`
                    ADD COLUMN `{field_name}` {field_type} COMMENT '{comment}'
                    """
                    cursor.execute(sql)
                    logger.info(f"✅ 已添加字段: {field_name} ({field_type})")
                else:
                    logger.info(f"字段已存在: {field_name}")
                    
            except Exception as e:
                logger.error(f"❌ 添加字段 {field_name} 失败: {e}")


def create_unmatched_data_table_if_not_exist():
    """创建毛重头程未匹配数据表（如果不存在）"""
    with db_cursor() as cursor:
        try:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                  AND TABLE_NAME = '毛重头程未匹配数据'
            """)
            
            result = cursor.fetchone()
            exists = result.get('cnt', 0) > 0 if result else False
            
            if not exists:
                # 创建表
                sql = """
                CREATE TABLE `毛重头程未匹配数据` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `MSKU` VARCHAR(100) COMMENT 'MSKU',
                    `SKU` VARCHAR(500) COMMENT 'SKU',
                    `负责人` VARCHAR(100) COMMENT '负责人',
                    `头程` VARCHAR(100) COMMENT '头程匹配情况（未匹配/店铺平均/品牌前缀平均）',
                    `毛重` VARCHAR(100) COMMENT '毛重匹配情况（未匹配/SPU平均匹配）',
                    `统计日期` DATE COMMENT '统计日期',
                    `创建日期` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
                    INDEX `idx_msku` (`MSKU`),
                    INDEX `idx_stat_date` (`统计日期`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='毛重头程未匹配数据表'
                """
                cursor.execute(sql)
                logger.info("✅ 已创建表: 毛重头程未匹配数据")
            else:
                logger.info("表已存在: 毛重头程未匹配数据")
                
        except Exception as e:
            logger.error(f"❌ 创建表 毛重头程未匹配数据 失败: {e}")


def insert_unmatched_data_batch(records: List[Dict[str, Any]], batch_size: int = 1000) -> int:
    """
    批量插入未匹配数据
    
    Args:
        records: 未匹配数据记录列表
        batch_size: 批次大小
        
    Returns:
        插入的记录数
    """
    if not records:
        return 0
    
    inserted_count = 0
    
    with db_cursor() as cursor:
        sql = """
        INSERT INTO `毛重头程未匹配数据` 
        (`MSKU`, `SKU`, `负责人`, `头程`, `毛重`, `统计日期`, `创建日期`)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batch_data = [
                (
                    r['MSKU'],
                    r['SKU'],
                    r['负责人'],
                    r['头程'],
                    r['毛重'],
                    r['统计日期'],
                    r['创建日期']
                )
                for r in batch
            ]
            
            try:
                cursor.executemany(sql, batch_data)
                inserted_count += len(batch)
                # 减少日志输出频率
                if (i // batch_size) % 5 == 0 or i + batch_size >= len(records):
                    logger.info(f"已插入未匹配数据 {min(i + batch_size, len(records))}/{len(records)} 条...")
            except Exception as e:
                logger.error(f"❌ 插入未匹配数据失败: {e}")
    
    return inserted_count


def main(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None):
    """
    主函数（优化版：使用SQL JOIN预加载数据）
    
    Args:
        start_date: 开始日期（格式：YYYY-MM-DD），如果为None则默认前5天
        end_date: 结束日期（格式：YYYY-MM-DD），如果为None则默认今天
        limit: 限制处理的记录数（用于测试）
    """
    logger.info("="*80)
    logger.info("更新利润报表计算字段（优化版）")
    logger.info("="*80)
    
    # 1. 检查并添加必要的字段
    logger.info("\n📋 步骤1: 检查并添加必要的计算字段...")
    add_calculated_fields_if_not_exist()
    
    # 1.1 检查并添加数据库索引
    logger.info("\n📋 步骤1.1: 检查并添加数据库索引...")
    add_indexes_if_not_exist()
    
    # 1.2 创建未匹配数据表（如果不存在）
    logger.info("\n📋 步骤1.2: 检查并创建未匹配数据表...")
    create_unmatched_data_table_if_not_exist()
    
    # 2. 确定日期范围（默认前5天到今天）
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        # 默认更新前5天的数据
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    
    logger.info(f"\n📅 步骤2: 确定更新范围")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    
    if limit:
        logger.info(f"  记录限制: {limit} 条（测试模式）")
    
    # 2.1 确定分批处理参数
    query_batch_size = 50000  # 每次查询的记录数（分批处理大数据量）
    if limit and limit < query_batch_size:
        query_batch_size = limit
    
    # 3. 使用SQL JOIN分批查询所有需要的数据（优化：分批处理大数据量）
    logger.info(f"\n📊 步骤3: 分批查询并JOIN所有需要的数据...")
    logger.info(f"  批次大小: {query_batch_size} 条/批")
    
    # 3.1 先检查数据量
    with db_cursor() as cursor:
        count_sql = f"""
        SELECT COUNT(*) as total
        FROM `利润报表` p
        WHERE p.`统计日期` >= %s AND p.`统计日期` <= %s
        """
        cursor.execute(count_sql, (start_date, end_date))
        result = cursor.fetchone()
        total_count = result['total'] if result else 0
        logger.info(f"  预计处理记录数: {total_count:,} 条")
        
        if total_count == 0:
            logger.warning("  没有需要处理的记录")
            return
        
        if total_count > 100000:
            logger.warning(f"  ⚠️  记录数较多（{total_count:,}条），查询可能需要较长时间（预计{total_count//10000}分钟）")
    
    # 获取所有涉及月份（包括本月、上个月和上上个月，用于匹配头程单价）
    from dateutil.relativedelta import relativedelta
    
    # 计算涉及的月份
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    months = set()
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.add(current.strftime('%Y-%m-%d'))
        # 同时添加本月、上个月和上上个月（用于匹配）
        current_month = current.strftime('%Y-%m-%d')
        last_month = (current - relativedelta(months=1)).strftime('%Y-%m-%d')
        last_last_month = (current - relativedelta(months=2)).strftime('%Y-%m-%d')
        months.add(current_month)
        months.add(last_month)
        months.add(last_last_month)
        current += relativedelta(months=1)
    months_list = ','.join([f"'{m}'" for m in months]) if months else "''"
    
    with db_cursor() as cursor:
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # 构建日期过滤条件：指定日期范围 OR 头程单价为空 OR 单品毛重为空
        # 注意：使用 %s 作为SQL参数占位符（不在f-string中，所以不需要转义）
        date_filter_clause = """AND (
            (p.`统计日期` >= %s AND p.`统计日期` <= %s) OR
            (p.`头程单价` IS NULL OR p.`头程单价` = 0) OR
            (p.`单品毛重` IS NULL OR p.`单品毛重` = 0)
        )"""
        
        # 优化的SQL：使用LEFT JOIN预加载所有需要的数据
        # 优化点：
        # 1. 在WHERE子句中提前过滤实际销量为0的记录，减少数据传输
        # 2. 使用预计算的月份列表，减少JOIN时的计算
        # 3. 日期匹配优先级：上个月 -> 上上个月
        # 注意：使用字符串拼接而不是f-string，避免 %s 占位符被错误解释
        sql = """
        SELECT 
            p.`id`,
            p.`SKU`,
            p.`MSKU`,
            p.`店铺`,
            p.`负责人`,
            p.`统计日期`,
            p.`采购成本`,
            p.`头程成本`,
            p.`FBA销量`,
            p.`FBM销量`,
            p.`FBA补换货量`,
            p.`FBM补换货量`,
            p.`退货量(可售)`,
            p.`退货量(不可售)`,
            -- 平台费用相关字段
            p.`塑料包装费`,
            p.`FBA卖家退回费`,
            p.`FBA仓储费入库缺陷费`,
            p.`库存调整费用`,
            p.`合作承运费`,
            p.`入库配置费`,
            p.`超量仓储费`,
            p.`FBA销毁费`,
            p.`FBA移除费`,
            p.`入仓手续费`,
            p.`标签费`,
            p.`订阅费`,
            p.`秒杀费`,
            p.`优惠券`,
            p.`早期评论人计划`,
            p.`vine`,
            p.`其他仓储费`,
            p.`月度仓储费`,
            p.`月度仓储费差异`,
            p.`长期仓储费`,
            p.`长期仓储费差异`,
            p.`平台费`,
            p.`FBA发货费`,
            p.`FBA发货费(多渠道)`,
            p.`其他订单费用`,
            p.`FBA国际物流货运费`,
            p.`调整费用`,
            p.`平台费退款额`,
            p.`发货费退款额`,
            p.`其他订单费退款额`,
            p.`运输标签费退款`,
            p.`交易费用退款额`,
            p.`积分费用`,
            -- 营收相关字段
            p.`包装收入`,
            p.`买家交易保障索赔`,
            p.`积分抵减收入`,
            p.`清算收入`,
            p.`亚马逊运费赔偿`,
            p.`Safe-T索赔`,
            p.`Netco交易`,
            p.`赔偿收入`,
            p.`追索收入`,
            p.`其他收入`,
            p.`清算调整`,
            p.`混合VAT收入`,
            p.`FBM销售退款额`,
            p.`FBA销售退款额`,
            p.`买家运费退款额`,
            p.`买家包装退款额`,
            p.`促销折扣退款额`,
            p.`买家拒付`,
            p.`积分抵减退回`,
            p.`FBA销售额`,
            p.`FBM销售额`,
            p.`买家运费`,
            p.`促销折扣`,
            p.`FBA库存赔偿`,
            -- 广告费用相关字段
            p.`SP广告费`,
            p.`SD广告费`,
            p.`SB广告费`,
            p.`SBV广告费`,
            p.`广告费用减免`,
            pm.`单品毛重` AS 单品毛重_SKU,
            pm_spu.`单品毛重_平均` AS 单品毛重_SPU平均,
            -- 负责人匹配（本月、上个月、上上个月）
            fp_person_current.`头程单价` AS 头程单价_负责人_本月,
            fp_person_current.`统计日期` AS 匹配日期_负责人_本月,
            fp_person_last.`头程单价` AS 头程单价_负责人_上月,
            fp_person_last.`统计日期` AS 匹配日期_负责人_上月,
            fp_person_last2.`头程单价` AS 头程单价_负责人_上上月,
            fp_person_last2.`统计日期` AS 匹配日期_负责人_上上月,
            -- 店铺平均（本月、上个月、上上个月）
            fp_shop_current.`头程单价_平均` AS 头程单价_店铺平均_本月,
            fp_shop_current.`统计日期` AS 匹配日期_店铺平均_本月,
            fp_shop_last.`头程单价_平均` AS 头程单价_店铺平均_上月,
            fp_shop_last.`统计日期` AS 匹配日期_店铺平均_上月,
            fp_shop_last2.`头程单价_平均` AS 头程单价_店铺平均_上上月,
            fp_shop_last2.`统计日期` AS 匹配日期_店铺平均_上上月,
            -- 品牌前缀平均（本月、上个月、上上个月）
            fp_brand_current.`头程单价_平均` AS 头程单价_品牌前缀平均_本月,
            fp_brand_current.`统计日期` AS 匹配日期_品牌前缀平均_本月,
            fp_brand_current.`品牌前缀` AS 品牌前缀_本月,
            fp_brand_last.`头程单价_平均` AS 头程单价_品牌前缀平均_上月,
            fp_brand_last.`统计日期` AS 匹配日期_品牌前缀平均_上月,
            fp_brand_last.`品牌前缀` AS 品牌前缀_上月,
            fp_brand_last2.`头程单价_平均` AS 头程单价_品牌前缀平均_上上月,
            fp_brand_last2.`统计日期` AS 匹配日期_品牌前缀平均_上上月,
            fp_brand_last2.`品牌前缀` AS 品牌前缀_上上月
        FROM `利润报表` p
        
        -- LEFT JOIN 产品管理表获取单品毛重（SKU匹配 - 策略1）
        -- 只匹配产品管理表中有该SKU且单品毛重有效（不为NULL且>0）的记录
        -- 使用子查询去重，确保每个SKU只匹配一条有效记录
        -- 如果产品管理表中没有该SKU或单品毛重无效，则pm.`单品毛重`为NULL，后续会使用SPU平均值
        LEFT JOIN (
            SELECT 
                TRIM(`SKU`) AS `SKU`,
                MAX(`单品毛重`) AS `单品毛重`
            FROM `产品管理`
            WHERE `SKU` IS NOT NULL
              AND `SKU` != ''
              AND TRIM(`SKU`) != ''
              AND `单品毛重` IS NOT NULL
              AND `单品毛重` > 0
            GROUP BY TRIM(`SKU`)
        ) pm ON (
            (
                -- 如果SKU不为空，直接使用SKU匹配
                (p.`SKU` IS NOT NULL AND p.`SKU` != '' AND TRIM(p.`SKU`) != '' AND TRIM(p.`SKU`) = pm.`SKU`)
                OR
                -- 如果SKU为空，从MSKU中提取SKU进行匹配
                (
                    (p.`SKU` IS NULL OR p.`SKU` = '' OR TRIM(p.`SKU`) = '')
                    AND p.`MSKU` IS NOT NULL
                    AND p.`MSKU` != ''
                    AND (
                        -- 提取MSKU中"-FBA"之前的字符，去掉前两个字符
                        (
                            p.`MSKU` LIKE '%%-FBA%%'
                            AND SUBSTRING(SUBSTRING_INDEX(p.`MSKU`, '-FBA', 1), 3) = pm.`SKU`
                        )
                        OR
                        -- 提取MSKU中"-FBM"之前的字符，去掉前两个字符
                        (
                            p.`MSKU` LIKE '%%-FBM%%'
                            AND SUBSTRING(SUBSTRING_INDEX(p.`MSKU`, '-FBM', 1), 3) = pm.`SKU`
                        )
                    )
                )
            )
        )
        
        -- LEFT JOIN 产品管理表获取单品毛重（SPU平均 - 策略2）
        -- 当SKU匹配不到时，使用SPU维度计算平均值
        -- 从SKU中提取SPU（第一个"-"之前的部分），然后计算相同SPU的所有记录的单品毛重平均值
        LEFT JOIN (
            SELECT 
                `SPU`,
                AVG(`单品毛重`) AS `单品毛重_平均`
            FROM `产品管理`
            WHERE `SPU` IS NOT NULL
              AND `SPU` != ''
              AND `单品毛重` IS NOT NULL
              AND `单品毛重` > 0
            GROUP BY `SPU`
        ) pm_spu ON (
            (
                -- 如果SKU不为空，从SKU中提取SPU
                (
                    p.`SKU` IS NOT NULL
                    AND p.`SKU` != ''
                    AND TRIM(p.`SKU`) != ''
                    AND SUBSTRING_INDEX(TRIM(p.`SKU`), '-', 1) = TRIM(pm_spu.`SPU`)
                )
                OR
                -- 如果SKU为空，从MSKU中提取SKU，然后提取SPU
                (
                    (p.`SKU` IS NULL OR p.`SKU` = '' OR TRIM(p.`SKU`) = '')
                    AND p.`MSKU` IS NOT NULL
                    AND p.`MSKU` != ''
                    AND (
                        -- 从MSKU中提取SKU（"-FBA"或"-FBM"之前的部分，去掉前两个字符），然后提取SPU
                        (
                            p.`MSKU` LIKE '%%-FBA%%'
                            AND SUBSTRING_INDEX(SUBSTRING(SUBSTRING_INDEX(p.`MSKU`, '-FBA', 1), 3), '-', 1) = TRIM(pm_spu.`SPU`)
                        )
                        OR
                        (
                            p.`MSKU` LIKE '%%-FBM%%'
                            AND SUBSTRING_INDEX(SUBSTRING(SUBSTRING_INDEX(p.`MSKU`, '-FBM', 1), 3), '-', 1) = TRIM(pm_spu.`SPU`)
                        )
                    )
                )
            )
        )
        
        -- LEFT JOIN 头程单价表（负责人匹配 - 本月）
        LEFT JOIN (
            SELECT 
                `店铺`,
                `负责人`,
                `头程单价`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
        ) fp_person_current ON (
            p.`店铺` = fp_person_current.`店铺` 
            AND p.`负责人` = fp_person_current.`负责人`
            AND DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = DATE_FORMAT(fp_person_current.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（负责人匹配 - 上个月）
        LEFT JOIN (
            SELECT 
                `店铺`,
                `负责人`,
                `头程单价`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
        ) fp_person_last ON (
            p.`店铺` = fp_person_last.`店铺` 
            AND p.`负责人` = fp_person_last.`负责人`
            AND DATE_FORMAT(DATE_SUB(p.`统计日期`, INTERVAL 1 MONTH), '%%Y-%%m-01') = DATE_FORMAT(fp_person_last.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（负责人匹配 - 上上个月）
        LEFT JOIN (
            SELECT 
                `店铺`,
                `负责人`,
                `头程单价`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
        ) fp_person_last2 ON (
            p.`店铺` = fp_person_last2.`店铺` 
            AND p.`负责人` = fp_person_last2.`负责人`
            AND DATE_FORMAT(DATE_SUB(p.`统计日期`, INTERVAL 2 MONTH), '%%Y-%%m-01') = DATE_FORMAT(fp_person_last2.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（店铺平均 - 本月）
        LEFT JOIN (
            SELECT 
                `店铺`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
            GROUP BY `店铺`, `统计日期`
        ) fp_shop_current ON (
            p.`店铺` = fp_shop_current.`店铺`
            AND DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = DATE_FORMAT(fp_shop_current.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（店铺平均 - 上个月）
        LEFT JOIN (
            SELECT 
                `店铺`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
            GROUP BY `店铺`, `统计日期`
        ) fp_shop_last ON (
            p.`店铺` = fp_shop_last.`店铺`
            AND DATE_FORMAT(DATE_SUB(p.`统计日期`, INTERVAL 1 MONTH), '%%Y-%%m-01') = DATE_FORMAT(fp_shop_last.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（店铺平均 - 上上个月）
        LEFT JOIN (
            SELECT 
                `店铺`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
            GROUP BY `店铺`, `统计日期`
        ) fp_shop_last2 ON (
            p.`店铺` = fp_shop_last2.`店铺`
            AND DATE_FORMAT(DATE_SUB(p.`统计日期`, INTERVAL 2 MONTH), '%%Y-%%m-01') = DATE_FORMAT(fp_shop_last2.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（品牌前缀平均 - 本月）
        LEFT JOIN (
            SELECT 
                CONCAT(SUBSTRING_INDEX(`店铺`, '-', 1), '-') AS `品牌前缀`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
              AND `店铺` LIKE '%%-%%'
              AND `头程单价` IS NOT NULL
              AND `头程单价` > 0
            GROUP BY `品牌前缀`, `统计日期`
        ) fp_brand_current ON (
            CONCAT(SUBSTRING_INDEX(p.`店铺`, '-', 1), '-') = fp_brand_current.`品牌前缀`
            AND DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = DATE_FORMAT(fp_brand_current.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（品牌前缀平均 - 上个月）
        LEFT JOIN (
            SELECT 
                CONCAT(SUBSTRING_INDEX(`店铺`, '-', 1), '-') AS `品牌前缀`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
              AND `店铺` LIKE '%%-%%'
              AND `头程单价` IS NOT NULL
              AND `头程单价` > 0
            GROUP BY `品牌前缀`, `统计日期`
        ) fp_brand_last ON (
            CONCAT(SUBSTRING_INDEX(p.`店铺`, '-', 1), '-') = fp_brand_last.`品牌前缀`
            AND DATE_FORMAT(DATE_SUB(p.`统计日期`, INTERVAL 1 MONTH), '%%Y-%%m-01') = DATE_FORMAT(fp_brand_last.`统计日期`, '%%Y-%%m-01')
        )
        
        -- LEFT JOIN 头程单价表（品牌前缀平均 - 上上个月）
        LEFT JOIN (
            SELECT 
                CONCAT(SUBSTRING_INDEX(`店铺`, '-', 1), '-') AS `品牌前缀`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
              AND `店铺` LIKE '%%-%%'
              AND `头程单价` IS NOT NULL
              AND `头程单价` > 0
            GROUP BY `品牌前缀`, `统计日期`
        ) fp_brand_last2 ON (
            CONCAT(SUBSTRING_INDEX(p.`店铺`, '-', 1), '-') = fp_brand_last2.`品牌前缀`
            AND DATE_FORMAT(DATE_SUB(p.`统计日期`, INTERVAL 2 MONTH), '%%Y-%%m-01') = DATE_FORMAT(fp_brand_last2.`统计日期`, '%%Y-%%m-01')
        )
        
        WHERE 1=1
          {date_filter_clause}
          -- 优先处理单品毛重为0或NULL的记录，确保同一个SKU的单品毛重一致
          -- 注意：不再过滤实际销量为0的记录，确保所有记录的单品毛重都能被更新
        ORDER BY 
          CASE WHEN p.`单品毛重` IS NULL OR p.`单品毛重` = 0 THEN 0 ELSE 1 END,
          p.`统计日期`, 
          p.`店铺`, 
          p.`SKU`
        {limit_clause}
        """
        
        # 使用 replace() 方法替换占位符，避免 .format() 与 % 符号冲突
        sql = sql.replace('{months_list}', months_list)
        sql = sql.replace('{date_filter_clause}', date_filter_clause)
        sql = sql.replace('{limit_clause}', limit_clause)
        
        # 执行SQL（总是使用日期参数）
        import time
        logger.info("  正在执行查询...")
        start_time = time.time()
        
        try:
            cursor.execute(sql, (start_date, end_date))
            query_time = time.time() - start_time
            logger.info(f"  查询执行完成，耗时: {query_time:.2f}秒")
            
            logger.info("  正在获取数据...")
            fetch_start = time.time()
            records = cursor.fetchall()
            fetch_time = time.time() - fetch_start
            logger.info(f"  数据获取完成，耗时: {fetch_time:.2f}秒")
        except Exception as e:
            logger.error(f"  查询执行失败: {e}")
            logger.error(f"  SQL前200字符: {sql[:200]}")
            raise
        
        logger.info(f"  查询到 {len(records)} 条记录（已JOIN单品毛重和头程单价）")
    
    if not records:
        logger.warning("⚠️  没有找到需要更新的记录")
        return
    
    # 4. 批量计算字段（在内存中计算，无需额外查询）
    # 如果数据量很大，分批处理和更新（优化：分批处理大数据量）
    logger.info(f"\n🔢 步骤4: 批量计算字段...")
    
    # 如果记录数超过批次大小，分批处理
    if len(records) > query_batch_size:
        logger.info(f"  数据量较大（{len(records)} 条），将分批处理（每批 {query_batch_size} 条）...")
        all_update_records = []
        all_unmatched_records = []
        total_stats = {
            '总记录数': 0,
            '跳过实际销量为0': 0,
            '有头程单价': 0,
            '无头程单价': 0,
            '有单品毛重': 0,
            '无单品毛重': 0,
            'SKU匹配单品毛重': 0,
            'SPU平均匹配单品毛重': 0,
            '负责人匹配': 0,
            '店铺平均': 0,
            '品牌前缀平均': 0,
            '未匹配': 0,
        }
        
        # 分批处理
        for batch_start in range(0, len(records), query_batch_size):
            batch_end = min(batch_start + query_batch_size, len(records))
            batch_records = records[batch_start:batch_end]
            batch_num = (batch_start // query_batch_size) + 1
            total_batches = (len(records) + query_batch_size - 1) // query_batch_size
            
            logger.info(f"  处理批次 {batch_num}/{total_batches}: {batch_start+1} ~ {batch_end} 条...")
            
            # 处理当前批次
            batch_update_records, batch_unmatched_records, batch_stats = _process_batch(batch_records)
            
            all_update_records.extend(batch_update_records)
            all_unmatched_records.extend(batch_unmatched_records)
            for key in total_stats:
                total_stats[key] += batch_stats[key]
            
            # 每批处理完后立即更新（减少内存占用）
            if batch_update_records:
                # 4.1 先更新单品毛重
                weight_update_records = [
                    {'id': r['id'], '单品毛重': r['单品毛重']}
                    for r in batch_update_records
                ]
                update_product_weight_only(weight_update_records)
                
                # 6. 更新其他字段
                update_profit_report_batch(batch_update_records)
        
        update_records = all_update_records
        unmatched_records = all_unmatched_records
        stats = total_stats
    else:
        # 数据量不大，直接处理
        update_records, unmatched_records, stats = _process_batch(records)
    
    logger.info(f"  计算完成! 共计算 {len(update_records)} 条记录")
    
    # 4.1 先更新单品毛重（如果还没有分批更新）
    if len(records) <= query_batch_size:
        logger.info(f"\n📦 步骤4.1: 优先更新单品毛重...")
        weight_update_records = [
            {
                'id': r['id'],
                '单品毛重': r['单品毛重']
            }
            for r in update_records
        ]
        weight_updated_count = update_product_weight_only(weight_update_records)
        logger.info(f"  ✅ 成功更新 {weight_updated_count} 条记录的单品毛重")
    
    # 5. 输出统计信息
    logger.info(f"\n📈 步骤5: 统计信息")
    logger.info(f"  总记录数: {stats['总记录数']}")
    logger.info(f"  跳过实际销量为0: {stats['跳过实际销量为0']} 条 ({stats['跳过实际销量为0']/stats['总记录数']*100:.1f}%)")
    logger.info(f"  实际处理记录数: {stats['总记录数'] - stats['跳过实际销量为0']} 条")
    logger.info(f"  头程单价:")
    processed_count = stats['总记录数'] - stats['跳过实际销量为0']
    if processed_count > 0:
        logger.info(f"    - 有头程单价: {stats['有头程单价']} 条 ({stats['有头程单价']/processed_count*100:.1f}%)")
        logger.info(f"    - 无头程单价: {stats['无头程单价']} 条 ({stats['无头程单价']/processed_count*100:.1f}%)")
    else:
        logger.info(f"    - 有头程单价: {stats['有头程单价']} 条")
        logger.info(f"    - 无头程单价: {stats['无头程单价']} 条")
    logger.info(f"  单品毛重:")
    if processed_count > 0:
        logger.info(f"    - 有单品毛重: {stats['有单品毛重']} 条 ({stats['有单品毛重']/processed_count*100:.1f}%)")
        logger.info(f"    - 无单品毛重: {stats['无单品毛重']} 条 ({stats['无单品毛重']/processed_count*100:.1f}%)")
    else:
        logger.info(f"    - 有单品毛重: {stats['有单品毛重']} 条")
        logger.info(f"    - 无单品毛重: {stats['无单品毛重']} 条")
    logger.info(f"    - SKU匹配: {stats['SKU匹配单品毛重']} 条")
    logger.info(f"    - SPU平均匹配: {stats['SPU平均匹配单品毛重']} 条")
    logger.info(f"  匹配逻辑:")
    logger.info(f"    - 负责人匹配: {stats['负责人匹配']} 条")
    logger.info(f"    - 店铺平均: {stats['店铺平均']} 条")
    logger.info(f"    - 品牌前缀平均: {stats['品牌前缀平均']} 条")
    logger.info(f"    - 未匹配: {stats['未匹配']} 条")
    
    # 6. 更新数据库（如果还没有分批更新）
    if len(records) <= query_batch_size:
        logger.info(f"\n💾 步骤6: 更新数据库...")
        logger.info(f"  准备更新 {len(update_records)} 条记录（只更新值有变化的记录）")
        updated_count = update_profit_report_batch(update_records)
        logger.info(f"  ✅ 成功更新 {updated_count} 条记录")
        if updated_count != len(update_records):
            logger.warning(f"  ⚠️  注意：准备更新 {len(update_records)} 条，实际更新 {updated_count} 条（可能部分记录的值没有变化）")
    else:
        logger.info(f"\n💾 步骤6: 更新数据库...")
        logger.info(f"  已在分批处理时完成更新，跳过此步骤")
    
    # 7. 插入未匹配数据
    if unmatched_records:
        logger.info(f"\n📝 步骤7: 插入未匹配数据...")
        logger.info(f"  准备插入 {len(unmatched_records)} 条未匹配数据记录")
        inserted_count = insert_unmatched_data_batch(unmatched_records)
        logger.info(f"  ✅ 成功插入 {inserted_count} 条未匹配数据记录")
    else:
        logger.info(f"\n📝 步骤7: 未匹配数据")
        logger.info(f"  所有记录都使用了第一优先级匹配，无需插入未匹配数据")
    
    logger.info("\n" + "="*80)
    logger.info("✅ 更新完成!")
    logger.info("="*80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='更新利润报表计算字段')
    parser.add_argument('--start-date', type=str, help='开始日期（格式：YYYY-MM-DD），默认为前5天')
    parser.add_argument('--end-date', type=str, help='结束日期（格式：YYYY-MM-DD），默认为今天')
    parser.add_argument('--limit', type=int, help='限制处理的记录数（用于测试）')
    
    args = parser.parse_args()
    
    try:
        main(
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit
        )
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断执行")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 执行失败: {e}", exc_info=True)
        sys.exit(1)

