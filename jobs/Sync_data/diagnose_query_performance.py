#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
SQL查询性能诊断工具
用于分析 update_profit_report_calculated_fields.py 中的查询性能问题
"""

import sys
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor

# 获取日志记录器
logger = get_logger('query_diagnosis')


def check_table_sizes():
    """检查相关表的数据量"""
    logger.info("="*80)
    logger.info("步骤1: 检查表数据量")
    logger.info("="*80)
    
    tables = ['利润报表', '产品管理', '头程单价']
    
    with db_cursor() as cursor:
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table}`")
                result = cursor.fetchone()
                count = result['cnt'] if result else 0
                logger.info(f"  {table}: {count:,} 条记录")
            except Exception as e:
                logger.error(f"  {table}: 查询失败 - {e}")


def check_indexes():
    """检查索引情况"""
    logger.info("\n" + "="*80)
    logger.info("步骤2: 检查索引")
    logger.info("="*80)
    
    tables = ['利润报表', '产品管理', '头程单价']
    
    with db_cursor() as cursor:
        for table in tables:
            try:
                cursor.execute("""
                    SELECT 
                        INDEX_NAME,
                        COLUMN_NAME,
                        SEQ_IN_INDEX,
                        CARDINALITY
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = %s
                    ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """, (table,))
                
                indexes = cursor.fetchall()
                
                if indexes:
                    logger.info(f"\n  {table} 的索引:")
                    current_index = None
                    for idx in indexes:
                        if idx['INDEX_NAME'] != current_index:
                            current_index = idx['INDEX_NAME']
                            logger.info(f"    - {idx['INDEX_NAME']}: ", end="")
                        logger.info(f"{idx['COLUMN_NAME']}", end="")
                        if idx['SEQ_IN_INDEX'] < len([i for i in indexes if i['INDEX_NAME'] == current_index]):
                            logger.info(", ", end="")
                        else:
                            logger.info(f" (基数: {idx['CARDINALITY']:,})")
                else:
                    logger.warning(f"  {table}: 没有索引!")
            except Exception as e:
                logger.error(f"  {table}: 检查索引失败 - {e}")


def test_simple_query(start_date, end_date):
    """测试简单查询（不JOIN）"""
    logger.info("\n" + "="*80)
    logger.info("步骤3: 测试简单查询（不JOIN）")
    logger.info("="*80)
    
    sql = """
    SELECT COUNT(*) as total
    FROM `利润报表` p
    WHERE p.`统计日期` >= %s AND p.`统计日期` <= %s
    """
    
    with db_cursor() as cursor:
        logger.info("  执行中...")
        start_time = time.time()
        try:
            cursor.execute(sql, (start_date, end_date))
            result = cursor.fetchone()
            elapsed = time.time() - start_time
            
            count = result['total'] if result else 0
            logger.info(f"  ✅ 查询成功: {count:,} 条记录")
            logger.info(f"  ⏱️  耗时: {elapsed:.2f}秒")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"  ❌ 查询失败（{elapsed:.2f}秒）: {e}")


def test_product_join(start_date, end_date):
    """测试产品管理表JOIN"""
    logger.info("\n" + "="*80)
    logger.info("步骤4: 测试产品管理表JOIN")
    logger.info("="*80)
    
    sql = """
    SELECT COUNT(*) as total
    FROM `利润报表` p
    LEFT JOIN (
        SELECT 
            TRIM(`SKU`) AS `SKU`,
            MAX(`单品毛重`) AS `单品毛重`
        FROM `产品管理`
        WHERE `SKU` IS NOT NULL
          AND `SKU` != ''
          AND `单品毛重` IS NOT NULL
          AND `单品毛重` > 0
        GROUP BY TRIM(`SKU`)
    ) pm ON TRIM(p.`SKU`) = pm.`SKU`
    WHERE p.`统计日期` >= %s AND p.`统计日期` <= %s
    LIMIT 1000
    """
    
    with db_cursor() as cursor:
        logger.info("  执行中（限制1000条）...")
        start_time = time.time()
        try:
            cursor.execute(sql, (start_date, end_date))
            result = cursor.fetchone()
            elapsed = time.time() - start_time
            
            count = result['total'] if result else 0
            logger.info(f"  ✅ 查询成功: {count:,} 条记录")
            logger.info(f"  ⏱️  耗时: {elapsed:.2f}秒")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"  ❌ 查询失败（{elapsed:.2f}秒）: {e}")


def test_freight_join(start_date, end_date):
    """测试头程单价表JOIN（简化版）"""
    logger.info("\n" + "="*80)
    logger.info("步骤5: 测试头程单价表JOIN（简化版）")
    logger.info("="*80)
    
    # 计算涉及的月份
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    months = set()
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.add(current.strftime('%Y-%m-%d'))
        last_month = (current - relativedelta(months=1)).strftime('%Y-%m-%d')
        last_last_month = (current - relativedelta(months=2)).strftime('%Y-%m-%d')
        months.add(last_month)
        months.add(last_last_month)
        current += relativedelta(months=1)
    months_list = ','.join([f"'{m}'" for m in months]) if months else "''"
    
    sql = f"""
    SELECT COUNT(*) as total
    FROM `利润报表` p
    LEFT JOIN (
        SELECT 
            `店铺`,
            `负责人`,
            `头程单价`,
            `统计日期`
        FROM `头程单价`
        WHERE `统计日期` IN ({months_list})
    ) fp ON (
        p.`店铺` = fp.`店铺` 
        AND p.`负责人` = fp.`负责人`
        AND DATE_FORMAT(p.`统计日期`, '%Y-%m-01') = DATE_FORMAT(fp.`统计日期`, '%Y-%m-01')
    )
    WHERE p.`统计日期` >= %s AND p.`统计日期` <= %s
    LIMIT 1000
    """
    
    with db_cursor() as cursor:
        logger.info(f"  月份列表: {months_list}")
        logger.info("  执行中（限制1000条）...")
        start_time = time.time()
        try:
            cursor.execute(sql, (start_date, end_date))
            result = cursor.fetchone()
            elapsed = time.time() - start_time
            
            count = result['total'] if result else 0
            logger.info(f"  ✅ 查询成功: {count:,} 条记录")
            logger.info(f"  ⏱️  耗时: {elapsed:.2f}秒")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"  ❌ 查询失败（{elapsed:.2f}秒）: {e}")


def explain_complex_query(start_date, end_date):
    """分析复杂查询的执行计划"""
    logger.info("\n" + "="*80)
    logger.info("步骤6: 分析完整查询的执行计划（EXPLAIN）")
    logger.info("="*80)
    
    # 计算涉及的月份
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    months = set()
    current = start_dt.replace(day=1)
    while current <= end_dt:
        months.add(current.strftime('%Y-%m-%d'))
        last_month = (current - relativedelta(months=1)).strftime('%Y-%m-%d')
        last_last_month = (current - relativedelta(months=2)).strftime('%Y-%m-%d')
        months.add(last_month)
        months.add(last_last_month)
        current += relativedelta(months=1)
    months_list = ','.join([f"'{m}'" for m in months]) if months else "''"
    
    # 简化的查询（只包含主要JOIN）
    sql = f"""
    SELECT 
        p.`id`,
        p.`SKU`,
        pm.`单品毛重` AS 单品毛重_SKU,
        fp_person.`头程单价` AS 头程单价_负责人
    FROM `利润报表` p
    LEFT JOIN (
        SELECT 
            TRIM(`SKU`) AS `SKU`,
            MAX(`单品毛重`) AS `单品毛重`
        FROM `产品管理`
        WHERE `SKU` IS NOT NULL
          AND `SKU` != ''
          AND `单品毛重` IS NOT NULL
          AND `单品毛重` > 0
        GROUP BY TRIM(`SKU`)
    ) pm ON TRIM(p.`SKU`) = pm.`SKU`
    LEFT JOIN (
        SELECT 
            `店铺`,
            `负责人`,
            `头程单价`,
            `统计日期`
        FROM `头程单价`
        WHERE `统计日期` IN ({months_list})
    ) fp_person ON (
        p.`店铺` = fp_person.`店铺` 
        AND p.`负责人` = fp_person.`负责人`
        AND DATE_FORMAT(p.`统计日期`, '%Y-%m-01') = DATE_FORMAT(fp_person.`统计日期`, '%Y-%m-01')
    )
    WHERE p.`统计日期` >= %s AND p.`统计日期` <= %s
    LIMIT 100
    """
    
    with db_cursor() as cursor:
        logger.info("  执行 EXPLAIN...")
        try:
            cursor.execute(f"EXPLAIN {sql}", (start_date, end_date))
            results = cursor.fetchall()
            
            if results:
                logger.info("\n  执行计划:")
                logger.info("  " + "-"*70)
                for row in results:
                    logger.info(f"  表: {row.get('table', 'N/A')}")
                    logger.info(f"    类型: {row.get('type', 'N/A')}")
                    logger.info(f"    可能的键: {row.get('possible_keys', 'N/A')}")
                    logger.info(f"    使用的键: {row.get('key', 'N/A')}")
                    logger.info(f"    行数: {row.get('rows', 'N/A')}")
                    logger.info(f"    Extra: {row.get('Extra', 'N/A')}")
                    logger.info("  " + "-"*70)
            else:
                logger.warning("  没有执行计划结果")
        except Exception as e:
            logger.error(f"  ❌ EXPLAIN失败: {e}")


def check_slow_query_log():
    """检查慢查询日志配置"""
    logger.info("\n" + "="*80)
    logger.info("步骤7: 检查MySQL慢查询日志配置")
    logger.info("="*80)
    
    with db_cursor() as cursor:
        try:
            cursor.execute("SHOW VARIABLES LIKE 'slow_query_log%'")
            results = cursor.fetchall()
            for row in results:
                logger.info(f"  {row['Variable_name']}: {row['Value']}")
            
            cursor.execute("SHOW VARIABLES LIKE 'long_query_time'")
            result = cursor.fetchone()
            if result:
                logger.info(f"  {result['Variable_name']}: {result['Value']}秒")
        except Exception as e:
            logger.error(f"  检查失败: {e}")


def main():
    """主函数"""
    logger.info("SQL查询性能诊断工具")
    logger.info("="*80)
    
    # 默认诊断最近5天的数据
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
    
    logger.info(f"诊断日期范围: {start_date} ~ {end_date}\n")
    
    try:
        # 1. 检查表数据量
        check_table_sizes()
        
        # 2. 检查索引
        check_indexes()
        
        # 3. 测试简单查询
        test_simple_query(start_date, end_date)
        
        # 4. 测试产品管理表JOIN
        test_product_join(start_date, end_date)
        
        # 5. 测试头程单价表JOIN
        test_freight_join(start_date, end_date)
        
        # 6. 分析执行计划
        explain_complex_query(start_date, end_date)
        
        # 7. 检查慢查询日志
        check_slow_query_log()
        
        logger.info("\n" + "="*80)
        logger.info("✅ 诊断完成!")
        logger.info("="*80)
        
        logger.info("\n🔍 分析建议:")
        logger.info("  1. 如果简单查询很快，但JOIN很慢，说明JOIN是瓶颈")
        logger.info("  2. 检查执行计划中的'type'列，如果是'ALL'说明全表扫描")
        logger.info("  3. 检查执行计划中的'rows'列，如果很大说明扫描行数过多")
        logger.info("  4. 如果'key'列为NULL，说明没有使用索引")
        logger.info("  5. 如果'Extra'列包含'Using temporary'或'Using filesort'，性能会受影响")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  用户中断诊断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ 诊断失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

