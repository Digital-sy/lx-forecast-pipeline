#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
更新利润报表计算字段
包括：
1. 商品成本附加费 = 采购成本 * 0.15
2. 实际销量 = FBA销量 + FBM销量 + FBM补换货量 + FBA补换货量 - 退货量（可售） - 退货量（不可售）
3. 实际头程费用 = (单品毛重/1000) * 实际销量 * 头程单价
4. 头程成本附加费 = 实际头程费用 * 0.15
5. 录入费用单头程 = 实际头程费用 - 头程成本
"""

import sys
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor

# 获取日志记录器
logger = get_logger('update_profit_report_calc')


def get_freight_unit_price(shop: str, person: str, stat_date: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """
    获取头程单价
    
    匹配逻辑：
    1. 优先匹配：店铺 + 负责人 + 统计日期（当月或上月）
    2. 如果负责人为空或匹配不到，则匹配：店铺 + 统计日期（取该店铺所有负责人的平均值）
    
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
    
    # 当月第一天
    current_month = date_obj.replace(day=1).strftime('%Y-%m-%d')
    # 上月第一天（使用relativedelta确保正确计算）
    from dateutil.relativedelta import relativedelta
    last_month = (date_obj.replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
    
    with db_cursor() as cursor:
        # 策略1: 如果有负责人，尝试匹配 店铺 + 负责人 + 统计日期
        if person and person.strip():
            # 先尝试匹配当月
            sql = """
            SELECT `头程单价`, `统计日期`
            FROM `头程单价`
            WHERE `店铺` = %s 
              AND `负责人` = %s 
              AND `统计日期` = %s
            LIMIT 1
            """
            cursor.execute(sql, (shop, person, current_month))
            result = cursor.fetchone()
            
            if result and result.get('头程单价'):
                return (
                    float(result['头程单价']),
                    f"负责人匹配({person})",
                    result['统计日期']
                )
            
            # 再尝试匹配上月
            cursor.execute(sql, (shop, person, last_month))
            result = cursor.fetchone()
            
            if result and result.get('头程单价'):
                return (
                    float(result['头程单价']),
                    f"负责人匹配({person})",
                    result['统计日期']
                )
        
        # 策略2: 匹配 店铺 + 统计日期（取平均值）
        # 先尝试当月
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
        cursor.execute(sql, (shop, current_month))
        result = cursor.fetchone()
        
        if result and result.get('avg_price'):
            return (
                float(result['avg_price']),
                "店铺平均",
                result['统计日期']
            )
        
        # 再尝试上月
        cursor.execute(sql, (shop, last_month))
        result = cursor.fetchone()
        
        if result and result.get('avg_price'):
            return (
                float(result['avg_price']),
                "店铺平均",
                result['统计日期']
            )
    
    return None, "未匹配", None


def get_product_weight(sku: str) -> Optional[float]:
    """
    从产品管理表获取单品毛重
    
    Args:
        sku: SKU
        
    Returns:
        单品毛重（克）
    """
    if not sku:
        return None
    
    with db_cursor() as cursor:
        sql = """
        SELECT `单品毛重`
        FROM `产品管理`
        WHERE `SKU` = %s
        LIMIT 1
        """
        cursor.execute(sql, (sku,))
        result = cursor.fetchone()
        
        if result and result.get('单品毛重'):
            try:
                return float(result['单品毛重'])
            except:
                return None
    
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
    cg_price_additional = round(cg_price * 0.15, 2)
    
    # 计算2: 实际销量 = FBA销量 + FBM销量 + FBM补换货量 + FBA补换货量 - 退货量（可售） - 退货量（不可售）
    actual_quantity = fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable
    
    # 计算3: 获取头程单价
    freight_price, match_logic, match_date = get_freight_unit_price(shop, person, stat_date)
    
    # 计算4: 获取单品毛重
    product_weight = get_product_weight(sku)
    
    # 计算5: 实际头程费用 = (单品毛重/1000) * 实际销量 * 头程单价
    actual_freight_fee = 0.0
    if freight_price and product_weight and actual_quantity:
        actual_freight_fee = round((product_weight / 1000) * actual_quantity * freight_price, 2)
    
    # 计算6: 头程成本附加费 = 实际头程费用 * 0.15
    cg_transport_additional = round(actual_freight_fee * 0.15, 2)
    
    # 计算7: 录入费用单头程 = 实际头程费用 - 头程成本
    recorded_freight = round(actual_freight_fee - cg_transport, 2)
    
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


def update_profit_report_batch(records: List[Dict[str, Any]], batch_size: int = 500) -> int:
    """
    批量更新利润报表
    
    Args:
        records: 需要更新的记录列表（包含id和计算字段）
        batch_size: 批次大小
        
    Returns:
        更新的记录数
    """
    if not records:
        return 0
    
    updated_count = 0
    
    with db_cursor() as cursor:
        # 批量更新（包含所有计算字段和辅助字段）
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
            `单品毛重` = %s
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
                    r['单品毛重'],
                    r['id']
                )
                for r in batch
            ]
            
            cursor.executemany(sql, batch_data)
            updated_count += cursor.rowcount
            
            logger.info(f"已更新 {min(i + batch_size, len(records))}/{len(records)} 条记录...")
    
    return updated_count


def add_calculated_fields_if_not_exist():
    """检查并添加计算字段（如果不存在）"""
    fields_to_add = [
        ('实际销量', 'DOUBLE', '实际销量 = FBA销量 + FBM销量 + 补换货量 - 退货量'),
        ('头程单价', 'DOUBLE', '从头程单价表匹配的单价'),
        ('匹配逻辑', 'VARCHAR(100)', '头程单价匹配逻辑（负责人匹配/店铺平均/未匹配）'),
        ('匹配日期', 'DATE', '头程单价匹配到的统计日期'),
        ('单品毛重', 'DOUBLE', '从产品管理表匹配的单品毛重（克）'),
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


def main(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: Optional[int] = None):
    """
    主函数（优化版：使用SQL JOIN预加载数据）
    
    Args:
        start_date: 开始日期（格式：YYYY-MM-DD），默认为当月第一天
        end_date: 结束日期（格式：YYYY-MM-DD），默认为今天
        limit: 限制处理的记录数（用于测试）
    """
    logger.info("="*80)
    logger.info("更新利润报表计算字段（优化版）")
    logger.info("="*80)
    
    # 1. 检查并添加必要的字段
    logger.info("\n📋 步骤1: 检查并添加必要的计算字段...")
    add_calculated_fields_if_not_exist()
    
    # 2. 确定日期范围
    if not start_date:
        start_date = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"\n📅 步骤2: 确定更新范围")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    if limit:
        logger.info(f"  记录限制: {limit} 条（测试模式）")
    
    # 3. 使用SQL JOIN一次性查询所有需要的数据
    logger.info(f"\n📊 步骤3: 查询并JOIN所有需要的数据...")
    
    # 计算当前查询范围涉及的月份
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 获取所有涉及月份（包括上个月，用于匹配头程单价）
    from dateutil.relativedelta import relativedelta
    months = set()
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.add(current.strftime('%Y-%m-%d'))
        # 同时添加上个月（用于匹配）
        last_month = (current - relativedelta(months=1)).strftime('%Y-%m-%d')
        months.add(last_month)
        current += relativedelta(months=1)
    
    months_list = ','.join([f"'{m}'" for m in months])
    
    with db_cursor() as cursor:
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # 优化的SQL：使用LEFT JOIN预加载所有需要的数据
        sql = f"""
        SELECT 
            p.`id`,
            p.`SKU`,
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
            pm.`单品毛重`,
            fp_person.`头程单价` AS 头程单价_负责人,
            fp_person.`统计日期` AS 匹配日期_负责人,
            fp_shop.`头程单价_平均` AS 头程单价_店铺平均,
            fp_shop.`统计日期` AS 匹配日期_店铺平均
        FROM `利润报表` p
        
        -- LEFT JOIN 产品管理表获取单品毛重
        LEFT JOIN `产品管理` pm ON p.`SKU` = pm.`SKU`
        
        -- LEFT JOIN 头程单价表（负责人匹配）
        -- 使用子查询优先匹配当月，如果没有则匹配上月
        LEFT JOIN (
            SELECT 
                `店铺`,
                `负责人`,
                `头程单价`,
                `统计日期`,
                DATE_FORMAT(DATE_ADD(`统计日期`, INTERVAL 1 MONTH), '%%Y-%%m-01') AS next_month
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
        ) fp_person ON (
            p.`店铺` = fp_person.`店铺` 
            AND p.`负责人` = fp_person.`负责人`
            AND (
                DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = fp_person.`统计日期`
                OR DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = fp_person.next_month
            )
        )
        
        -- LEFT JOIN 头程单价表（店铺平均）
        LEFT JOIN (
            SELECT 
                `店铺`,
                AVG(`头程单价`) AS `头程单价_平均`,
                `统计日期`,
                DATE_FORMAT(DATE_ADD(`统计日期`, INTERVAL 1 MONTH), '%%Y-%%m-01') AS next_month
            FROM `头程单价`
            WHERE `统计日期` IN ({months_list})
            GROUP BY `店铺`, `统计日期`
        ) fp_shop ON (
            p.`店铺` = fp_shop.`店铺`
            AND (
                DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = fp_shop.`统计日期`
                OR DATE_FORMAT(p.`统计日期`, '%%Y-%%m-01') = fp_shop.next_month
            )
        )
        
        WHERE p.`统计日期` >= %s
          AND p.`统计日期` <= %s
        ORDER BY p.`统计日期`, p.`店铺`, p.`SKU`
        {limit_clause}
        """
        
        cursor.execute(sql, (start_date, end_date))
        records = cursor.fetchall()
        
        logger.info(f"  查询到 {len(records)} 条记录（已JOIN单品毛重和头程单价）")
    
    if not records:
        logger.warning("⚠️  没有找到需要更新的记录")
        return
    
    # 4. 批量计算字段（在内存中计算，无需额外查询）
    logger.info(f"\n🔢 步骤4: 批量计算字段...")
    
    update_records = []
    stats = {
        '总记录数': len(records),
        '有头程单价': 0,
        '无头程单价': 0,
        '有单品毛重': 0,
        '无单品毛重': 0,
        '负责人匹配': 0,
        '店铺平均': 0,
        '未匹配': 0,
    }
    
    for i, record in enumerate(records, 1):
        if i % 1000 == 0:
            logger.info(f"  已处理 {i}/{len(records)} 条...")
        
        # 从JOIN结果中提取数据
        cg_price = float(record.get('采购成本', 0) or 0)
        cg_transport = float(record.get('头程成本', 0) or 0)
        
        # 销量数据
        fba_sales = float(record.get('FBA销量', 0) or 0)
        fbm_sales = float(record.get('FBM销量', 0) or 0)
        fba_reship = float(record.get('FBA补换货量', 0) or 0)
        fbm_reship = float(record.get('FBM补换货量', 0) or 0)
        return_saleable = float(record.get('退货量(可售)', 0) or 0)
        return_unsaleable = float(record.get('退货量(不可售)', 0) or 0)
        
        # 计算1: 商品成本附加费
        cg_price_additional = round(cg_price * 0.15, 2)
        
        # 计算2: 实际销量
        actual_quantity = fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable
        
        # 计算3: 获取头程单价（优先使用负责人匹配，否则使用店铺平均）
        freight_price = None
        match_logic = "未匹配"
        match_date = None
        
        if record.get('头程单价_负责人'):
            freight_price = float(record['头程单价_负责人'])
            match_logic = f"负责人匹配({record.get('负责人', '')})"
            match_date = record.get('匹配日期_负责人')
            stats['负责人匹配'] += 1
        elif record.get('头程单价_店铺平均'):
            freight_price = float(record['头程单价_店铺平均'])
            match_logic = "店铺平均"
            match_date = record.get('匹配日期_店铺平均')
            stats['店铺平均'] += 1
        else:
            stats['未匹配'] += 1
        
        # 计算4: 获取单品毛重
        product_weight = None
        if record.get('单品毛重'):
            try:
                product_weight = float(record['单品毛重'])
            except:
                pass
        
        # 统计
        if freight_price:
            stats['有头程单价'] += 1
        else:
            stats['无头程单价'] += 1
        
        if product_weight:
            stats['有单品毛重'] += 1
        else:
            stats['无单品毛重'] += 1
        
        # 计算5: 实际头程费用
        actual_freight_fee = 0.0
        if freight_price and product_weight and actual_quantity:
            actual_freight_fee = round((product_weight / 1000) * actual_quantity * freight_price, 2)
        
        # 计算6: 头程成本附加费
        cg_transport_additional = round(actual_freight_fee * 0.15, 2)
        
        # 计算7: 录入费用单头程
        recorded_freight = round(actual_freight_fee - cg_transport, 2)
        
        update_records.append({
            'id': record['id'],
            '商品成本附加费': cg_price_additional,
            '实际头程费用': actual_freight_fee,
            '头程成本附加费': cg_transport_additional,
            '录入费用单头程': recorded_freight,
            '实际销量': actual_quantity,
            '头程单价': freight_price,
            '匹配逻辑': match_logic,
            '匹配日期': match_date,
            '单品毛重': product_weight,
        })
    
    logger.info(f"  计算完成!")
    
    # 5. 输出统计信息
    logger.info(f"\n📈 步骤5: 统计信息")
    logger.info(f"  总记录数: {stats['总记录数']}")
    logger.info(f"  头程单价:")
    logger.info(f"    - 有头程单价: {stats['有头程单价']} 条 ({stats['有头程单价']/stats['总记录数']*100:.1f}%)")
    logger.info(f"    - 无头程单价: {stats['无头程单价']} 条 ({stats['无头程单价']/stats['总记录数']*100:.1f}%)")
    logger.info(f"  单品毛重:")
    logger.info(f"    - 有单品毛重: {stats['有单品毛重']} 条 ({stats['有单品毛重']/stats['总记录数']*100:.1f}%)")
    logger.info(f"    - 无单品毛重: {stats['无单品毛重']} 条 ({stats['无单品毛重']/stats['总记录数']*100:.1f}%)")
    logger.info(f"  匹配逻辑:")
    logger.info(f"    - 负责人匹配: {stats['负责人匹配']} 条")
    logger.info(f"    - 店铺平均: {stats['店铺平均']} 条")
    logger.info(f"    - 未匹配: {stats['未匹配']} 条")
    
    # 6. 更新数据库
    logger.info(f"\n💾 步骤6: 更新数据库...")
    updated_count = update_profit_report_batch(update_records)
    logger.info(f"  ✅ 成功更新 {updated_count} 条记录")
    
    logger.info("\n" + "="*80)
    logger.info("✅ 更新完成!")
    logger.info("="*80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='更新利润报表计算字段')
    parser.add_argument('--start-date', type=str, help='开始日期（格式：YYYY-MM-DD），默认为当月第一天')
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

