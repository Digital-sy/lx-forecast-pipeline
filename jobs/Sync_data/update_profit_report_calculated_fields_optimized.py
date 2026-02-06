#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
更新利润报表计算字段（优化版 - 分阶段查询）

优化策略：
1. 避免复杂的多表JOIN
2. 分阶段查询和处理数据
3. 使用字典缓存减少数据库查询
4. 添加详细的进度日志
5. 设置数据库超时防止卡住

性能提升：
- 避免9个LEFT JOIN到头程单价表
- 避免JOIN条件中的字符串函数
- 减少临时表创建
- 提升查询可控性和可维护性
"""

import sys
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import wraps
from collections import defaultdict

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor

# 获取日志记录器
logger = get_logger('update_profit_report_calc_optimized')


def timing_decorator(func):
    """性能监控装饰器 - 记录函数执行时间"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        
        if elapsed > 60:
            logger.warning(f"⏱️  {func.__name__} 执行耗时: {elapsed:.2f}秒 ({elapsed/60:.1f}分钟)")
        elif elapsed > 10:
            logger.info(f"⏱️  {func.__name__} 执行耗时: {elapsed:.2f}秒")
        else:
            logger.debug(f"⏱️  {func.__name__} 执行耗时: {elapsed:.2f}秒")
        
        return result
    return wrapper


def safe_decimal(value, default=0.0):
    """安全地将值转换为高精度Decimal类型"""
    if value is None:
        return Decimal(str(default))
    
    try:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(float(value) if value else default))
    except (ValueError, TypeError, AttributeError):
        return Decimal(str(default))


def precise_round(value, precision=10):
    """高精度四舍五入"""
    if isinstance(value, (int, float)):
        value = Decimal(str(value))
    elif not isinstance(value, Decimal):
        value = safe_decimal(value)
    
    return value.quantize(Decimal('0.' + '0' * precision), rounding=ROUND_HALF_UP)


def extract_brand_prefix(shop: str) -> Optional[str]:
    """从店铺名称中提取品牌前缀"""
    if not shop or not isinstance(shop, str):
        return None
    dash_index = shop.find('-')
    if dash_index > 0:
        return shop[:dash_index + 1]
    return None


def extract_sku_from_msku(msku: str) -> Optional[str]:
    """从MSKU中提取SKU"""
    if not msku or not isinstance(msku, str):
        return None
    
    fba_index = msku.find('-FBA')
    fbm_index = msku.find('-FBM')
    
    cut_index = -1
    if fba_index > 0:
        cut_index = fba_index
    if fbm_index > 0 and (cut_index == -1 or fbm_index < cut_index):
        cut_index = fbm_index
    
    if cut_index > 0:
        prefix = msku[:cut_index]
        if len(prefix) > 2:
            return prefix[2:]
    
    return None


def extract_spu_from_sku(sku: str) -> Optional[str]:
    """从SKU中提取SPU"""
    if not sku or not isinstance(sku, str):
        return None
    dash_index = sku.find('-')
    if dash_index > 0:
        return sku[:dash_index]
    return None


@timing_decorator
def load_product_weights() -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    加载产品管理表的单品毛重数据（一次性加载到内存）
    
    Returns:
        Tuple[SKU字典, SPU平均字典]
    """
    logger.info("📦 加载产品管理表数据...")
    
    sku_weights = {}
    spu_weights_raw = defaultdict(list)
    
    with db_cursor() as cursor:
        # 查询所有有效的单品毛重数据
        sql = """
        SELECT 
            TRIM(`SKU`) AS sku,
            TRIM(`SPU`) AS spu,
            `单品毛重`
        FROM `产品管理`
        WHERE (`SKU` IS NOT NULL AND `SKU` != '' OR `SPU` IS NOT NULL AND `SPU` != '')
          AND `单品毛重` IS NOT NULL
          AND `单品毛重` > 0
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        for row in results:
            sku = row.get('sku', '').strip()
            spu = row.get('spu', '').strip()
            weight = float(row.get('单品毛重', 0))
            
            # SKU字典
            if sku and weight > 0:
                if sku not in sku_weights:
                    sku_weights[sku] = weight
            
            # SPU字典（用于计算平均值）
            if spu and weight > 0:
                spu_weights_raw[spu].append(weight)
    
    # 计算SPU平均值
    spu_weights = {}
    for spu, weights in spu_weights_raw.items():
        if weights:
            spu_weights[spu] = sum(weights) / len(weights)
    
    logger.info(f"  ✅ 加载完成: SKU={len(sku_weights):,} 个, SPU={len(spu_weights):,} 个")
    return sku_weights, spu_weights


@timing_decorator
def load_freight_prices(months: List[str]) -> Dict[str, List[Dict]]:
    """
    加载头程单价表数据（一次性加载到内存）
    
    Args:
        months: 需要查询的月份列表（格式：YYYY-MM-DD）
        
    Returns:
        Dict[店铺, List[记录]]
    """
    logger.info(f"🚚 加载头程单价表数据（{len(months)}个月份）...")
    
    freight_data = defaultdict(list)
    
    with db_cursor() as cursor:
        if not months:
            logger.warning("  ⚠️  没有指定月份，跳过加载")
            return freight_data
        
        months_list = ','.join([f"'{m}'" for m in months])
        
        sql = f"""
        SELECT 
            `店铺`,
            `负责人`,
            `头程单价`,
            DATE_FORMAT(`统计日期`, '%Y-%m-01') AS month_key
        FROM `头程单价`
        WHERE `统计日期` IN ({months_list})
          AND `头程单价` IS NOT NULL
          AND `头程单价` > 0
        """
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        for row in results:
            shop = row.get('店铺', '').strip()
            if shop:
                freight_data[shop].append({
                    '负责人': row.get('负责人', '').strip(),
                    '头程单价': float(row.get('头程单价', 0)),
                    'month_key': row.get('month_key', '')
                })
        
        logger.info(f"  ✅ 加载完成: {len(freight_data):,} 个店铺, {len(results):,} 条记录")
    
    return freight_data


def get_product_weight_from_cache(
    sku: str,
    msku: str,
    sku_weights: Dict[str, float],
    spu_weights: Dict[str, float]
) -> Tuple[Optional[float], str]:
    """
    从缓存中获取单品毛重
    
    Returns:
        Tuple[单品毛重, 匹配类型]
    """
    # 如果SKU为空，尝试从MSKU提取
    if not sku or not sku.strip():
        if msku:
            sku = extract_sku_from_msku(msku)
    
    if not sku:
        return None, "未匹配"
    
    sku = sku.strip()
    
    # 策略1: SKU匹配
    if sku in sku_weights:
        return sku_weights[sku], "SKU匹配"
    
    # 策略2: SPU平均匹配
    spu = extract_spu_from_sku(sku)
    if spu and spu in spu_weights:
        return spu_weights[spu], "SPU平均匹配"
    
    return None, "未匹配"


def get_freight_price_from_cache(
    shop: str,
    person: str,
    stat_date: str,
    freight_data: Dict[str, List[Dict]]
) -> Tuple[Optional[float], str, Optional[str]]:
    """
    从缓存中获取头程单价
    
    Returns:
        Tuple[头程单价, 匹配逻辑, 匹配日期]
    """
    if not shop or not stat_date:
        return None, "未匹配", None
    
    shop = shop.strip()
    person = person.strip() if person else ""
    
    # 解析统计日期
    try:
        if isinstance(stat_date, str):
            date_obj = datetime.strptime(stat_date, '%Y-%m-%d')
        else:
            date_obj = stat_date
    except:
        return None, "未匹配", None
    
    # 计算本月、上月、上上月
    current_month = date_obj.replace(day=1).strftime('%Y-%m-01')
    last_month = (date_obj.replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-01')
    last_last_month = (date_obj.replace(day=1) - relativedelta(months=2)).strftime('%Y-%m-01')
    months_to_try = [current_month, last_month, last_last_month]
    
    # 如果店铺不在缓存中
    if shop not in freight_data:
        # 尝试品牌前缀匹配
        brand_prefix = extract_brand_prefix(shop)
        if brand_prefix:
            return get_freight_price_by_brand(brand_prefix, months_to_try, freight_data)
        return None, "未匹配", None
    
    shop_records = freight_data[shop]
    
    # 策略1: 负责人匹配
    if person:
        for month in months_to_try:
            for record in shop_records:
                if record['负责人'] == person and record['month_key'] == month:
                    return record['头程单价'], f"负责人匹配({person})", month
    
    # 策略2: 店铺平均
    for month in months_to_try:
        month_records = [r for r in shop_records if r['month_key'] == month]
        if month_records:
            avg_price = sum(r['头程单价'] for r in month_records) / len(month_records)
            return avg_price, "店铺平均", month
    
    # 策略3: 品牌前缀平均
    brand_prefix = extract_brand_prefix(shop)
    if brand_prefix:
        return get_freight_price_by_brand(brand_prefix, months_to_try, freight_data)
    
    return None, "未匹配", None


def get_freight_price_by_brand(
    brand_prefix: str,
    months_to_try: List[str],
    freight_data: Dict[str, List[Dict]]
) -> Tuple[Optional[float], str, Optional[str]]:
    """按品牌前缀匹配头程单价"""
    for month in months_to_try:
        brand_records = []
        for shop, records in freight_data.items():
            if shop.startswith(brand_prefix):
                brand_records.extend([r for r in records if r['month_key'] == month])
        
        if brand_records:
            avg_price = sum(r['头程单价'] for r in brand_records) / len(brand_records)
            return avg_price, f"品牌前缀平均({brand_prefix})", month
    
    return None, "未匹配", None


@timing_decorator
def query_profit_report_records(start_date: str, end_date: str, limit: Optional[int] = None) -> List[Dict]:
    """
    查询利润报表记录（简单查询，不JOIN）
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        limit: 限制记录数
        
    Returns:
        记录列表
    """
    logger.info("📊 查询利润报表记录...")
    
    with db_cursor() as cursor:
        # 先统计数量
        count_sql = """
        SELECT COUNT(*) as total
        FROM `利润报表`
        WHERE `统计日期` >= %s AND `统计日期` <= %s
        """
        cursor.execute(count_sql, (start_date, end_date))
        result = cursor.fetchone()
        total_count = result['total'] if result else 0
        
        logger.info(f"  日期范围: {start_date} ~ {end_date}")
        logger.info(f"  预计记录数: {total_count:,} 条")
        
        if limit:
            logger.info(f"  限制记录数: {limit:,} 条")
        
        # 查询数据（简化版，只查询必要字段）
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        sql = f"""
        SELECT 
            `id`,
            `SKU`,
            `MSKU`,
            `店铺`,
            `负责人`,
            `统计日期`,
            `采购成本`,
            `头程成本`,
            `FBA销量`,
            `FBM销量`,
            `FBA补换货量`,
            `FBM补换货量`,
            `退货量(可售)`,
            `退货量(不可售)`,
            `塑料包装费`, `FBA卖家退回费`, `FBA仓储费入库缺陷费`, `库存调整费用`,
            `合作承运费`, `入库配置费`, `超量仓储费`, `FBA销毁费`, `FBA移除费`,
            `入仓手续费`, `标签费`, `订阅费`, `秒杀费`, `优惠券`, `早期评论人计划`,
            `vine`, `其他仓储费`, `月度仓储费`, `月度仓储费差异`, `长期仓储费`,
            `长期仓储费差异`, `平台费`, `FBA发货费`, `FBA发货费(多渠道)`,
            `其他订单费用`, `FBA国际物流货运费`, `调整费用`, `平台费退款额`,
            `发货费退款额`, `其他订单费退款额`, `运输标签费退款`, `交易费用退款额`,
            `积分费用`, `包装收入`, `买家交易保障索赔`, `积分抵减收入`, `清算收入`,
            `亚马逊运费赔偿`, `Safe-T索赔`, `Netco交易`, `赔偿收入`, `追索收入`,
            `其他收入`, `清算调整`, `混合VAT收入`, `FBM销售退款额`, `FBA销售退款额`,
            `买家运费退款额`, `买家包装退款额`, `促销折扣退款额`, `买家拒付`,
            `积分抵减退回`, `FBA销售额`, `FBM销售额`, `买家运费`, `促销折扣`,
            `FBA库存赔偿`, `SP广告费`, `SD广告费`, `SB广告费`, `SBV广告费`,
            `广告费用减免`
        FROM `利润报表`
        WHERE `统计日期` >= %s AND `统计日期` <= %s
        ORDER BY `统计日期`, `店铺`, `SKU`
        {limit_clause}
        """
        
        logger.info("  执行查询...")
        start_time = time.time()
        cursor.execute(sql, (start_date, end_date))
        
        logger.info("  获取数据...")
        records = cursor.fetchall()
        elapsed = time.time() - start_time
        
        logger.info(f"  ✅ 查询完成: {len(records):,} 条记录，耗时 {elapsed:.2f}秒")
        
        return records


@timing_decorator
def process_records(
    records: List[Dict],
    sku_weights: Dict[str, float],
    spu_weights: Dict[str, float],
    freight_data: Dict[str, List[Dict]]
) -> Tuple[List[Dict], Dict[str, int]]:
    """
    处理记录，计算各个字段
    
    Returns:
        Tuple[更新记录列表, 统计信息]
    """
    logger.info(f"🔢 处理记录并计算字段...")
    
    update_records = []
    stats = {
        '总记录数': len(records),
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
    
    for i, record in enumerate(records, 1):
        if i % 10000 == 0:
            logger.info(f"  已处理 {i:,}/{len(records):,} 条 ({i/len(records)*100:.1f}%)")
        
        # 提取字段
        sku = record.get('SKU', '')
        msku = record.get('MSKU', '')
        shop = record.get('店铺', '')
        person = record.get('负责人', '')
        stat_date = record.get('统计日期', '')
        
        # 采购成本和头程成本
        cg_price = safe_decimal(record.get('采购成本', 0))
        cg_transport = safe_decimal(record.get('头程成本', 0))
        
        # 销量数据
        fba_sales = safe_decimal(record.get('FBA销量', 0))
        fbm_sales = safe_decimal(record.get('FBM销量', 0))
        fba_reship = safe_decimal(record.get('FBA补换货量', 0))
        fbm_reship = safe_decimal(record.get('FBM补换货量', 0))
        return_saleable = safe_decimal(record.get('退货量(可售)', 0))
        return_unsaleable = safe_decimal(record.get('退货量(不可售)', 0))
        
        # 计算1: 商品成本附加费
        cg_price_additional = precise_round(cg_price * Decimal('0.15'), 10)
        
        # 计算2: 实际销量
        actual_quantity = precise_round(
            fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable,
            10
        )
        
        # 计算3: 获取单品毛重（从缓存）
        product_weight, weight_match_type = get_product_weight_from_cache(
            sku, msku, sku_weights, spu_weights
        )
        
        if product_weight:
            stats['有单品毛重'] += 1
            if weight_match_type == "SKU匹配":
                stats['SKU匹配单品毛重'] += 1
            elif weight_match_type == "SPU平均匹配":
                stats['SPU平均匹配单品毛重'] += 1
        else:
            stats['无单品毛重'] += 1
        
        # 计算4: 获取头程单价（从缓存）
        freight_price, match_logic, match_date = get_freight_price_from_cache(
            shop, person, stat_date, freight_data
        )
        
        if freight_price:
            stats['有头程单价'] += 1
            if "负责人匹配" in match_logic:
                stats['负责人匹配'] += 1
            elif "店铺平均" in match_logic:
                stats['店铺平均'] += 1
            elif "品牌前缀平均" in match_logic:
                stats['品牌前缀平均'] += 1
        else:
            stats['无头程单价'] += 1
            stats['未匹配'] += 1
        
        # 计算5: 实际头程费用
        actual_freight_fee = Decimal('0.0')
        if freight_price and product_weight and actual_quantity != 0:
            actual_freight_fee = -precise_round(
                (Decimal(str(product_weight)) / Decimal('1000')) * actual_quantity * Decimal(str(freight_price)),
                10
            )
        
        # 计算6: 头程成本附加费
        cg_transport_additional = precise_round(actual_freight_fee * Decimal('0.15'), 10)
        
        # 计算7: 录入费用单头程
        recorded_freight = precise_round(actual_freight_fee - cg_transport, 10)
        
        # 计算8: 平台费用
        platform_fee_fields = [
            '塑料包装费', 'FBA卖家退回费', 'FBA仓储费入库缺陷费', '库存调整费用',
            '合作承运费', '入库配置费', '超量仓储费', 'FBA销毁费', 'FBA移除费',
            '入仓手续费', '标签费', '订阅费', '秒杀费', '优惠券', '早期评论人计划',
            'vine', '其他仓储费', '月度仓储费', '月度仓储费差异', '长期仓储费',
            '长期仓储费差异', '平台费', 'FBA发货费', 'FBA发货费(多渠道)',
            '其他订单费用', 'FBA国际物流货运费', '调整费用', '平台费退款额',
            '发货费退款额', '其他订单费退款额', '运输标签费退款', '交易费用退款额', '积分费用'
        ]
        total_platform_fee = precise_round(
            sum(safe_decimal(record.get(field, 0)) for field in platform_fee_fields),
            10
        )
        
        # 计算9: 营收
        revenue_fields = [
            '包装收入', '买家交易保障索赔', '积分抵减收入', '清算收入',
            '亚马逊运费赔偿', 'Safe-T索赔', 'Netco交易', '赔偿收入', '追索收入',
            '其他收入', '清算调整', '混合VAT收入', 'FBM销售退款额', 'FBA销售退款额',
            '买家运费退款额', '买家包装退款额', '促销折扣退款额', '买家拒付',
            '积分抵减退回', 'FBA销售额', 'FBM销售额', '买家运费', '促销折扣', 'FBA库存赔偿'
        ]
        total_revenue = precise_round(
            sum(safe_decimal(record.get(field, 0)) for field in revenue_fields),
            10
        )
        
        # 计算10: 广告费用
        advertising_fields = ['SP广告费', 'SD广告费', 'SB广告费', 'SBV广告费', '广告费用减免']
        total_advertising_fee = precise_round(
            sum(safe_decimal(record.get(field, 0)) for field in advertising_fields),
            10
        )
        
        # 计算11: 汇损
        exchange_loss = precise_round(
            -(total_revenue + total_platform_fee + total_advertising_fee) * Decimal('0.01'),
            10
        )
        
        # 添加到更新列表
        update_records.append({
            'id': record['id'],
            '商品成本附加费': float(cg_price_additional),
            '实际头程费用': float(actual_freight_fee),
            '头程成本附加费': float(cg_transport_additional),
            '录入费用单头程': float(recorded_freight),
            '实际销量': float(actual_quantity),
            '头程单价': float(freight_price) if freight_price else None,
            '匹配逻辑': match_logic,
            '匹配日期': match_date,
            '单品毛重': product_weight,
            '平台费用': float(total_platform_fee),
            '营收': float(total_revenue),
            '广告费用': float(total_advertising_fee),
            '汇损': float(exchange_loss),
        })
    
    logger.info(f"  ✅ 处理完成: {len(update_records):,} 条记录")
    
    return update_records, stats


@timing_decorator
def update_profit_report_batch(records: List[Dict[str, Any]], batch_size: int = 5000) -> int:
    """批量更新利润报表"""
    if not records:
        return 0
    
    logger.info(f"💾 批量更新数据库（批次大小: {batch_size}）...")
    
    updated_count = 0
    total_batches = (len(records) + batch_size - 1) // batch_size
    
    with db_cursor() as cursor:
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
            `单品毛重` = %s,
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
                    r['商品成本附加费'], r['实际头程费用'], r['头程成本附加费'],
                    r['录入费用单头程'], r['实际销量'], r['头程单价'],
                    r['匹配逻辑'], r['匹配日期'], r['单品毛重'],
                    r['平台费用'], r['营收'], r['广告费用'], r['汇损'], r['id']
                )
                for r in batch
            ]
            
            try:
                cursor.executemany(sql, batch_data)
                batch_updated = cursor.rowcount
                updated_count += batch_updated
                
                current_batch = (i // batch_size) + 1
                progress = (min(i + batch_size, len(records)) / len(records)) * 100
                logger.info(f"  批次 {current_batch}/{total_batches}: "
                           f"{min(i + batch_size, len(records)):,}/{len(records):,} "
                           f"({progress:.1f}%) - 更新 {batch_updated} 条")
            except Exception as e:
                logger.error(f"  ❌ 批次 {(i // batch_size) + 1} 更新失败: {e}")
                continue
    
    logger.info(f"  ✅ 更新完成: {updated_count:,} 条记录")
    return updated_count


def main(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None):
    """主函数（优化版）"""
    logger.info("="*80)
    logger.info("更新利润报表计算字段（优化版 - 分阶段查询）")
    logger.info("="*80)
    
    # 确定日期范围
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    
    logger.info(f"\n📅 更新范围: {start_date} ~ {end_date}")
    if limit:
        logger.info(f"📊 限制记录数: {limit:,} 条")
    
    # 计算需要查询的月份
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    months = set()
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.add(current.strftime('%Y-%m-%d'))
        months.add((current - relativedelta(months=1)).strftime('%Y-%m-%d'))
        months.add((current - relativedelta(months=2)).strftime('%Y-%m-%d'))
        current += relativedelta(months=1)
    
    try:
        # 阶段1: 加载产品管理表数据
        sku_weights, spu_weights = load_product_weights()
        
        # 阶段2: 加载头程单价表数据
        freight_data = load_freight_prices(list(months))
        
        # 阶段3: 查询利润报表记录（不JOIN）
        records = query_profit_report_records(start_date, end_date, limit)
        
        if not records:
            logger.warning("⚠️  没有需要处理的记录")
            return
        
        # 阶段4: 处理记录（使用缓存数据）
        update_records, stats = process_records(records, sku_weights, spu_weights, freight_data)
        
        # 阶段5: 输出统计信息
        logger.info(f"\n📈 统计信息:")
        logger.info(f"  总记录数: {stats['总记录数']:,}")
        logger.info(f"  头程单价: 有={stats['有头程单价']:,}, 无={stats['无头程单价']:,}")
        logger.info(f"    - 负责人匹配: {stats['负责人匹配']:,}")
        logger.info(f"    - 店铺平均: {stats['店铺平均']:,}")
        logger.info(f"    - 品牌前缀平均: {stats['品牌前缀平均']:,}")
        logger.info(f"    - 未匹配: {stats['未匹配']:,}")
        logger.info(f"  单品毛重: 有={stats['有单品毛重']:,}, 无={stats['无单品毛重']:,}")
        logger.info(f"    - SKU匹配: {stats['SKU匹配单品毛重']:,}")
        logger.info(f"    - SPU平均匹配: {stats['SPU平均匹配单品毛重']:,}")
        
        # 阶段6: 更新数据库
        updated_count = update_profit_report_batch(update_records)
        
        logger.info("\n" + "="*80)
        logger.info(f"✅ 更新完成! 成功更新 {updated_count:,} 条记录")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"\n❌ 执行失败: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='更新利润报表计算字段（优化版）')
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

