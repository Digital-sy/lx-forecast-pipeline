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
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor

# 获取日志记录器
logger = get_logger('update_profit_report_calc')


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
    1. 优先匹配：店铺 + 负责人 + 统计日期（上个月 -> 上上个月）
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
    
    # 上月第一天（使用relativedelta确保正确计算）
    from dateutil.relativedelta import relativedelta
    last_month = (date_obj.replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
    # 上上个月第一天
    last_last_month = (date_obj.replace(day=1) - relativedelta(months=2)).strftime('%Y-%m-%d')
    
    # 日期匹配顺序：上个月 -> 上上个月
    months_to_try = [last_month, last_last_month]
    
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
        # 策略1: 使用SKU匹配
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
                pass
        
        # 策略2: 使用SPU匹配，取平均值
        spu = extract_spu_from_sku(sku)
        if spu:
            sql = """
            SELECT AVG(`单品毛重`) as avg_weight
            FROM `产品管理`
            WHERE `SPU` = %s
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
    cg_price_additional = round(cg_price * 0.15, 2)
    
    # 计算2: 实际销量 = FBA销量 + FBM销量 + FBM补换货量 + FBA补换货量 - 退货量（可售） - 退货量（不可售）
    actual_quantity = fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable
    
    # 计算3: 获取头程单价
    freight_price, match_logic, match_date = get_freight_unit_price(shop, person, stat_date)
    
    # 计算4: 获取单品毛重
    product_weight = get_product_weight(sku)
    
    # 计算5: 实际头程费用 = -(单品毛重/1000) * 实际销量 * 头程单价（直接计算为负数）
    actual_freight_fee = 0.0
    if freight_price and product_weight and actual_quantity:
        actual_freight_fee = -round((product_weight / 1000) * actual_quantity * freight_price, 2)
    
    # 计算6: 头程成本附加费 = 实际头程费用 * 0.15（使用负数）
    cg_transport_additional = round(actual_freight_fee * 0.15, 2)
    
    # 计算7: 录入费用单头程 = 实际头程费用 - 头程成本（使用负数）
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


def update_profit_report_batch(records: List[Dict[str, Any]], batch_size: int = 1000) -> int:
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
            batch_updated = cursor.rowcount
            updated_count += batch_updated
            
            # 记录每批的更新情况（减少日志输出频率）
            if (i // batch_size) % 5 == 0 or i + batch_size >= len(records):
                logger.info(f"已处理 {min(i + batch_size, len(records))}/{len(records)} 条记录（本批更新 {batch_updated} 条）...")
    
    return updated_count


def add_calculated_fields_if_not_exist():
    """检查并添加计算字段（如果不存在）"""
    fields_to_add = [
        ('实际销量', 'DOUBLE', '实际销量 = FBA销量 + FBM销量 + 补换货量 - 退货量'),
        ('头程单价', 'DOUBLE', '从头程单价表匹配的单价'),
        ('匹配逻辑', 'VARCHAR(100)', '头程单价匹配逻辑（负责人匹配/店铺平均/品牌前缀平均/未匹配）'),
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
        start_date: 开始日期（格式：YYYY-MM-DD），默认为前16天
        end_date: 结束日期（格式：YYYY-MM-DD），默认为今天
        limit: 限制处理的记录数（用于测试）
    """
    logger.info("="*80)
    logger.info("更新利润报表计算字段（优化版）")
    logger.info("="*80)
    
    # 1. 检查并添加必要的字段
    logger.info("\n📋 步骤1: 检查并添加必要的计算字段...")
    add_calculated_fields_if_not_exist()
    
    # 1.1 创建未匹配数据表（如果不存在）
    logger.info("\n📋 步骤1.1: 检查并创建未匹配数据表...")
    create_unmatched_data_table_if_not_exist()
    
    # 2. 确定日期范围（默认更新前15天的数据）
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        # 默认更新前15天的数据
        start_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
    
    logger.info(f"\n📅 步骤2: 确定更新范围")
    logger.info(f"  日期范围: {start_date} ~ {end_date}")
    if limit:
        logger.info(f"  记录限制: {limit} 条（测试模式）")
    
    # 3. 使用SQL JOIN一次性查询所有需要的数据
    logger.info(f"\n📊 步骤3: 查询并JOIN所有需要的数据...")
    
    # 计算当前查询范围涉及的月份
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 获取所有涉及月份（包括上个月和上上个月，用于匹配头程单价）
    from dateutil.relativedelta import relativedelta
    months = set()
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.add(current.strftime('%Y-%m-%d'))
        # 同时添加上个月和上上个月（用于匹配）
        last_month = (current - relativedelta(months=1)).strftime('%Y-%m-%d')
        last_last_month = (current - relativedelta(months=2)).strftime('%Y-%m-%d')
        months.add(last_month)
        months.add(last_last_month)
        current += relativedelta(months=1)
    
    months_list = ','.join([f"'{m}'" for m in months])
    
    with db_cursor() as cursor:
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # 优化的SQL：使用LEFT JOIN预加载所有需要的数据
        # 优化点：
        # 1. 在WHERE子句中提前过滤实际销量为0的记录，减少数据传输
        # 2. 使用预计算的月份列表，减少JOIN时的计算
        # 3. 日期匹配优先级：上个月 -> 上上个月
        sql = f"""
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
            pm.`单品毛重` AS 单品毛重_SKU,
            pm_spu.`单品毛重_平均` AS 单品毛重_SPU平均,
            -- 负责人匹配（上个月、上上个月）
            fp_person_last.`头程单价` AS 头程单价_负责人_上月,
            fp_person_last.`统计日期` AS 匹配日期_负责人_上月,
            fp_person_last2.`头程单价` AS 头程单价_负责人_上上月,
            fp_person_last2.`统计日期` AS 匹配日期_负责人_上上月,
            -- 店铺平均（上个月、上上个月）
            fp_shop_last.`头程单价_平均` AS 头程单价_店铺平均_上月,
            fp_shop_last.`统计日期` AS 匹配日期_店铺平均_上月,
            fp_shop_last2.`头程单价_平均` AS 头程单价_店铺平均_上上月,
            fp_shop_last2.`统计日期` AS 匹配日期_店铺平均_上上月,
            -- 品牌前缀平均（上个月、上上个月）
            fp_brand_last.`头程单价_平均` AS 头程单价_品牌前缀平均_上月,
            fp_brand_last.`统计日期` AS 匹配日期_品牌前缀平均_上月,
            fp_brand_last.`品牌前缀` AS 品牌前缀_上月,
            fp_brand_last2.`头程单价_平均` AS 头程单价_品牌前缀平均_上上月,
            fp_brand_last2.`统计日期` AS 匹配日期_品牌前缀平均_上上月,
            fp_brand_last2.`品牌前缀` AS 品牌前缀_上上月
        FROM `利润报表` p
        
        -- LEFT JOIN 产品管理表获取单品毛重（SKU匹配）
        LEFT JOIN `产品管理` pm ON p.`SKU` = pm.`SKU`
        
        -- LEFT JOIN 产品管理表获取单品毛重（SPU平均）
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
            SUBSTRING_INDEX(p.`SKU`, '-', 1) = pm_spu.`SPU`
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
        
        WHERE p.`统计日期` >= %s
          AND p.`统计日期` <= %s
          -- 提前过滤实际销量为0的记录，减少数据传输和处理
          AND (
            (COALESCE(p.`FBA销量`, 0) + COALESCE(p.`FBM销量`, 0) + 
             COALESCE(p.`FBA补换货量`, 0) + COALESCE(p.`FBM补换货量`, 0) - 
             COALESCE(p.`退货量(可售)`, 0) - COALESCE(p.`退货量(不可售)`, 0)) != 0
          )
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
    unmatched_records = []  # 存储未匹配数据
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
    
    for i, record in enumerate(records, 1):
        if i % 5000 == 0:
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
        
        # 计算2: 实际销量（先计算，如果为0则跳过后续计算）
        actual_quantity = fba_sales + fbm_sales + fba_reship + fbm_reship - return_saleable - return_unsaleable
        
        # 如果实际销量为0，跳过后续所有计算
        if actual_quantity == 0:
            stats['跳过实际销量为0'] += 1
            continue
        
        # 计算1: 商品成本附加费
        cg_price_additional = round(cg_price * 0.15, 2)
        
        # 计算3: 获取头程单价（优先使用负责人匹配，其次店铺平均，最后品牌前缀平均）
        # 日期优先级：上个月 -> 上上个月
        freight_price = None
        match_logic = "未匹配"
        match_date = None
        
        # 负责人匹配（优先级：上个月 -> 上上个月）
        if record.get('头程单价_负责人_上月'):
            freight_price = float(record['头程单价_负责人_上月'])
            match_logic = f"负责人匹配({record.get('负责人', '')})"
            match_date = record.get('匹配日期_负责人_上月')
            stats['负责人匹配'] += 1
        elif record.get('头程单价_负责人_上上月'):
            freight_price = float(record['头程单价_负责人_上上月'])
            match_logic = f"负责人匹配({record.get('负责人', '')})"
            match_date = record.get('匹配日期_负责人_上上月')
            stats['负责人匹配'] += 1
        # 店铺平均（优先级：上个月 -> 上上个月）
        elif record.get('头程单价_店铺平均_上月'):
            freight_price = float(record['头程单价_店铺平均_上月'])
            match_logic = "店铺平均"
            match_date = record.get('匹配日期_店铺平均_上月')
            stats['店铺平均'] += 1
        elif record.get('头程单价_店铺平均_上上月'):
            freight_price = float(record['头程单价_店铺平均_上上月'])
            match_logic = "店铺平均"
            match_date = record.get('匹配日期_店铺平均_上上月')
            stats['店铺平均'] += 1
        # 品牌前缀平均（优先级：上个月 -> 上上个月）
        elif record.get('头程单价_品牌前缀平均_上月'):
            freight_price = float(record['头程单价_品牌前缀平均_上月'])
            brand_prefix = record.get('品牌前缀_上月', '')
            match_logic = f"品牌前缀平均({brand_prefix})"
            match_date = record.get('匹配日期_品牌前缀平均_上月')
            stats['品牌前缀平均'] += 1
        elif record.get('头程单价_品牌前缀平均_上上月'):
            freight_price = float(record['头程单价_品牌前缀平均_上上月'])
            brand_prefix = record.get('品牌前缀_上上月', '')
            match_logic = f"品牌前缀平均({brand_prefix})"
            match_date = record.get('匹配日期_品牌前缀平均_上上月')
            stats['品牌前缀平均'] += 1
        else:
            stats['未匹配'] += 1
        
        # 计算4: 获取单品毛重（优先使用SKU匹配，如果匹配不到则使用SPU平均）
        product_weight = None
        if record.get('单品毛重_SKU'):
            try:
                product_weight = float(record['单品毛重_SKU'])
                stats['SKU匹配单品毛重'] += 1
            except:
                pass
        
        # 如果SKU匹配不到，使用SPU平均
        if not product_weight and record.get('单品毛重_SPU平均'):
            try:
                product_weight = float(record['单品毛重_SPU平均'])
                stats['SPU平均匹配单品毛重'] += 1
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
        
        # 计算5: 实际头程费用 = -(单品毛重/1000) * 实际销量 * 头程单价（直接计算为负数）
        actual_freight_fee = 0.0
        if freight_price and product_weight and actual_quantity:
            actual_freight_fee = -round((product_weight / 1000) * actual_quantity * freight_price, 2)
        
        # 计算6: 头程成本附加费 = 实际头程费用 * 0.15（使用负数）
        cg_transport_additional = round(actual_freight_fee * 0.15, 2)
        
        # 计算7: 录入费用单头程 = 实际头程费用 - 头程成本（使用负数）
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
    
    logger.info(f"  计算完成! 共计算 {len(update_records)} 条记录")
    
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
    
    # 6. 更新数据库
    logger.info(f"\n💾 步骤6: 更新数据库...")
    logger.info(f"  准备更新 {len(update_records)} 条记录（所有查询到的记录都会重新计算并更新）")
    updated_count = update_profit_report_batch(update_records)
    logger.info(f"  ✅ 成功更新 {updated_count} 条记录")
    if updated_count != len(update_records):
        logger.warning(f"  ⚠️  注意：准备更新 {len(update_records)} 条，实际更新 {updated_count} 条（可能部分记录的值没有变化）")
    
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

