#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
检查BX401的销量预估数据，分析算法问题
"""
from datetime import datetime, timedelta
from collections import defaultdict
from common import settings, get_logger
from common.database import db_cursor
from utils import normalize_shop_name

logger = get_logger('check_bx401')


def extract_spu_fields(sku: str):
    """从SKU中提取spu和spu颜色"""
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


def check_bx401_data():
    """检查BX401的数据"""
    logger.info("="*80)
    logger.info("检查BX401的销量预估数据")
    logger.info("="*80)
    
    # 1. 查询所有BX401相关的SKU
    logger.info("\n1. 查询BX401相关的SKU...")
    with db_cursor(dictionary=True) as cursor:
        sql = """
        SELECT DISTINCT SKU, spu, spu颜色
        FROM `销量预估表`
        WHERE spu = 'BX401'
        ORDER BY SKU
        """
        cursor.execute(sql)
        skus = cursor.fetchall()
    
    logger.info(f"   找到 {len(skus)} 个SKU:")
    for sku_info in skus:
        logger.info(f"     - {sku_info['SKU']} (spu: {sku_info['spu']}, spu颜色: {sku_info['spu颜色']})")
    
    if not skus:
        logger.warning("   未找到BX401相关的SKU，尝试从销量统计表查询...")
        with db_cursor(dictionary=True) as cursor:
            sql = """
            SELECT DISTINCT SKU
            FROM `销量统计_MSKU月度`
            WHERE SKU LIKE 'BX401%'
            ORDER BY SKU
            """
            cursor.execute(sql)
            skus_from_sales = cursor.fetchall()
        
        if skus_from_sales:
            logger.info(f"   从销量统计表找到 {len(skus_from_sales)} 个SKU:")
            for sku_info in skus_from_sales:
                spu, spu_color = extract_spu_fields(sku_info['SKU'])
                logger.info(f"     - {sku_info['SKU']} (提取spu: {spu}, spu颜色: {spu_color})")
        else:
            logger.warning("   未找到任何BX401相关的数据")
            return
    
    # 2. 查询历史销量数据（最近15个月）
    logger.info("\n2. 查询历史销量数据（最近15个月）...")
    now = datetime.now()
    start_date = (now - timedelta(days=30 * 15)).replace(day=1)
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    with db_cursor(dictionary=True) as cursor:
        sql = """
        SELECT 
            SKU,
            店铺,
            DATE_FORMAT(统计日期, '%%Y-%%m') as 月份,
            SUM(销量) as 总销量
        FROM `销量统计_MSKU月度`
        WHERE 统计日期 >= %s
          AND SKU LIKE %s
          AND SKU IS NOT NULL AND SKU != '' AND SKU != '无'
          AND 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
        GROUP BY SKU, 店铺, 月份
        ORDER BY SKU, 店铺, 月份
        """
        cursor.execute(sql, (start_date_str, 'BX401%'))
        historical_sales = cursor.fetchall()
    
    logger.info(f"   找到 {len(historical_sales)} 条历史销量记录")
    
    # 按SKU+店铺组织数据
    sku_shop_sales = defaultdict(dict)  # {(sku, shop): {月份: 销量}}
    for row in historical_sales:
        sku = row['SKU']
        shop = normalize_shop_name(row['店铺'] or '')
        month = row['月份']
        sales = int(row['总销量'] or 0)
        sku_shop_sales[(sku, shop)][month] = sales
    
    # 按SPU+店铺聚合
    spu_shop_sales = defaultdict(dict)  # {(spu, shop): {月份: 销量}}
    for (sku, shop), sales_by_month in sku_shop_sales.items():
        spu, _ = extract_spu_fields(sku)
        if spu == 'BX401':
            spu_shop_key = (spu, shop)
            for month, sales in sales_by_month.items():
                if month not in spu_shop_sales[spu_shop_key]:
                    spu_shop_sales[spu_shop_key][month] = 0
                spu_shop_sales[spu_shop_key][month] += sales
    
    # 打印SPU+店铺的销量数据
    logger.info("\n   SPU+店铺聚合销量数据:")
    for (spu, shop), sales_by_month in sorted(spu_shop_sales.items()):
        logger.info(f"\n   SPU: {spu}, 店铺: {shop}")
        for month in sorted(sales_by_month.keys()):
            logger.info(f"     {month}: {sales_by_month[month]}")
    
    # 3. 查询预估数据
    logger.info("\n3. 查询销量预估数据...")
    with db_cursor(dictionary=True) as cursor:
        sql = """
        SELECT 
            SKU,
            店铺,
            月份,
            实际销量,
            预估销量,
            趋势因子,
            预估方式,
            预计下单,
            月环比,
            月同比,
            FBA库存,
            本地库存,
            更新时间
        FROM `销量预估表`
        WHERE spu = 'BX401'
        ORDER BY SKU, 店铺, 月份
        """
        cursor.execute(sql)
        forecast_data = cursor.fetchall()
    
    logger.info(f"   找到 {len(forecast_data)} 条预估记录")
    
    # 按SKU+店铺组织预估数据
    sku_shop_forecast = defaultdict(list)
    for row in forecast_data:
        sku = row['SKU']
        shop = row['店铺']
        sku_shop_forecast[(sku, shop)].append(row)
    
    # 打印预估数据
    logger.info("\n   销量预估数据详情:")
    for (sku, shop), records in sorted(sku_shop_forecast.items()):
        logger.info(f"\n   SKU: {sku}, 店铺: {shop}")
        for record in sorted(records, key=lambda x: x['月份']):
            logger.info(f"     {record['月份']}: "
                       f"实际={record['实际销量']}, "
                       f"预估={record['预估销量']}, "
                       f"趋势因子={record['趋势因子']}, "
                       f"预估方式={record['预估方式']}")
    
    # 4. 分析算法逻辑
    logger.info("\n4. 分析算法逻辑...")
    now = datetime.now()
    current_month_str = now.strftime('%Y-%m')
    current_year_actual, current_month_actual = map(int, current_month_str.split('-'))
    
    # 获取目标月份（本月+未来3个月）
    target_months = []
    for i in range(4):
        year = now.year
        month = now.month + i
        while month > 12:
            month -= 12
            year += 1
        month_str = f"{year}-{month:02d}"
        target_months.append(month_str)
    
    logger.info(f"   当前月份: {current_month_str}")
    logger.info(f"   目标月份: {', '.join(target_months)}")
    
    # 分析每个SPU+店铺的预估逻辑
    for (spu, shop), spu_sales_dict in sorted(spu_shop_sales.items()):
        logger.info(f"\n   分析 SPU: {spu}, 店铺: {shop}")
        logger.info(f"   历史销量数据:")
        for month in sorted(spu_sales_dict.keys()):
            logger.info(f"     {month}: {spu_sales_dict[month]}")
        
        # 模拟算法：计算前N个月（不包含本月）
        logger.info(f"\n   算法分析（趋势因子法）:")
        
        # 尝试3个月
        for months_count in range(3, 0, -1):
            prev_months = []
            for i in range(months_count, 0, -1):
                month_num = current_month_actual - i
                year = current_year_actual
                if month_num <= 0:
                    month_num += 12
                    year -= 1
                month_str = f"{year}-{month_num:02d}"
                if month_str < current_month_str:
                    prev_months.append((year, month_num))
            
            if len(prev_months) < months_count:
                continue
            
            # 获取今年前N个月的销量
            current_year_sales = []
            for year, month_num in prev_months:
                month_str = f"{year}-{month_num:02d}"
                if month_str in spu_sales_dict:
                    current_year_sales.append(spu_sales_dict[month_str])
            
            # 获取去年同期的N个月销量
            last_year_sales = []
            for year, month_num in prev_months:
                year -= 1
                month_str = f"{year}-{month_num:02d}"
                if month_str in spu_sales_dict:
                    last_year_sales.append(spu_sales_dict[month_str])
            
            if len(current_year_sales) == months_count and len(last_year_sales) == months_count:
                current_avg = sum(current_year_sales) / len(current_year_sales)
                last_avg = sum(last_year_sales) / len(last_year_sales)
                
                if last_avg > 0:
                    trend_factor = current_avg / last_avg
                    logger.info(f"     前{months_count}个月数据:")
                    logger.info(f"       今年前{months_count}个月: {prev_months}")
                    logger.info(f"       今年销量: {current_year_sales}, 平均: {current_avg:.2f}")
                    logger.info(f"       去年销量: {last_year_sales}, 平均: {last_avg:.2f}")
                    logger.info(f"       趋势因子: {trend_factor:.4f}")
                    
                    # 计算每个目标月份的预估
                    for month in target_months:
                        try:
                            target_year, target_month_num = map(int, month.split('-'))
                            last_year = target_year - 1
                            last_year_same_month = f"{last_year}-{target_month_num:02d}"
                            last_year_same_month_sales = spu_sales_dict.get(last_year_same_month, 0)
                            
                            if last_year_same_month_sales > 0:
                                forecast = int(last_year_same_month_sales * trend_factor)
                                logger.info(f"       {month}: 去年同月={last_year_same_month_sales}, "
                                          f"预估={forecast} (去年同月 × {trend_factor:.4f})")
                            else:
                                logger.info(f"       {month}: 去年同月无数据，无法预估")
                        except:
                            pass
                    
                    break  # 找到足够的数据，退出循环
    
    logger.info("\n" + "="*80)
    logger.info("检查完成")
    logger.info("="*80)


if __name__ == '__main__':
    check_bx401_data()

