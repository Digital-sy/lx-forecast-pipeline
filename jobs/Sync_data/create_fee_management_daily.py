#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
费用单管理任务 - 按日维度处理
每天作废并重新创建费用单
默认处理本月每一天
"""
import asyncio
from datetime import date, timedelta
from calendar import monthrange
from typing import List, Dict, Any

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from jobs.Sync_data.create_fee_management import (
    FeeManagement,
    discard_existing_fee_orders,
    REQUEST_DELAY,
    MAX_FEE_ITEMS_PER_ORDER,
    REST_BATCH_INTERVAL,
    REST_DURATION
)

# 获取日志记录器
logger = get_logger('fee_management_daily')


def fetch_profit_report_data_daily(target_date: str) -> List[Dict[str, Any]]:
    """
    从数据库读取利润报表数据（按日，不汇总）
    
    Args:
        target_date: 目标日期，格式：Y-m-d
    
    Returns:
        List[Dict]: 利润报表数据列表，按日返回
    """
    try:
        with db_cursor() as cursor:
            sql = """
                SELECT 
                    `MSKU`,
                    `店铺id`,
                    `统计日期`,
                    `商品成本附加费`,
                    `头程成本附加费`,
                    `录入费用单头程`,
                    `汇损`
                FROM `利润报表`
                WHERE `统计日期` = %s
                  AND (
                      (`商品成本附加费` IS NOT NULL AND `商品成本附加费` != 0) OR
                      (`头程成本附加费` IS NOT NULL AND `头程成本附加费` != 0) OR
                      (`录入费用单头程` IS NOT NULL AND `录入费用单头程` != 0) OR
                      (`汇损` IS NOT NULL AND `汇损` != 0)
                  )
                ORDER BY `店铺id`, `MSKU`
            """
            cursor.execute(sql, (target_date,))
            records = cursor.fetchall()
            
            if records:
                unique_msku_shop = set()
                for record in records:
                    msku = record.get('MSKU', '').strip()
                    shop_id = record.get('店铺id')
                    if msku and shop_id:
                        unique_msku_shop.add((msku, str(shop_id)))
                
                logger.info(f"✅ 从数据库读取到 {len(records)} 条日期为 {target_date} 的利润报表数据")
                logger.info(f"   包含 {len(unique_msku_shop)} 个不同的(MSKU, 店铺ID)组合")
            else:
                logger.info(f"ℹ️  日期 {target_date} 没有需要创建费用单的数据")
            
            return records
    except Exception as e:
        logger.error(f"❌ 读取日期 {target_date} 的利润报表数据失败: {str(e)}")
        return []


async def create_fee_orders_for_single_day(
    fee_mgmt: FeeManagement,
    target_date: str,
    profit_data: List[Dict[str, Any]],
    fee_type_ids: Dict[str, int]
) -> int:
    """
    为单个日期创建费用单
    
    Args:
        fee_mgmt: 费用管理实例
        target_date: 目标日期，格式：Y-m-d
        profit_data: 利润报表数据列表（单日数据）
        fee_type_ids: 费用类型ID字典
    
    Returns:
        int: 成功创建的费用单数量
    """
    if not profit_data:
        logger.info(f"  ℹ️  日期 {target_date} 没有数据，跳过")
        return 0
    
    logger.info(f"\n{'=' * 80}")
    logger.info(f"📝 创建日期 {target_date} 的费用单")
    logger.info(f"{'=' * 80}")
    
    # 按店铺ID分组
    from collections import defaultdict
    
    grouped_data = defaultdict(list)
    skipped_records = []
    
    for record in profit_data:
        shop_id = record.get('店铺id')
        msku = record.get('MSKU', '').strip()
        
        # 检查数据完整性
        if not shop_id:
            skipped_records.append({
                'reason': '店铺ID为空',
                'record': record
            })
            continue
        
        if not msku:
            skipped_records.append({
                'reason': 'MSKU为空',
                'record': record
            })
            continue
        
        grouped_data[str(shop_id)].append(record)
    
    # 报告被跳过的记录
    if skipped_records:
        logger.warning(f"  ⚠️  发现 {len(skipped_records)} 条记录被跳过（数据不完整）")
    
    logger.info(f"  共 {len(profit_data)} 条数据，按店铺分组后共 {len(grouped_data)} 组")
    
    success_count = 0
    total_count = 0
    
    # 按店铺分组创建费用单
    for shop_id, records in grouped_data.items():
        total_count += 1
        
        # 统计该组包含的MSKU
        group_mskus = set()
        for record in records:
            msku = record.get('MSKU', '').strip()
            if msku:
                group_mskus.add(msku)
        
        logger.info(f"\n  处理第 {total_count}/{len(grouped_data)} 组：店铺ID={shop_id}, 包含 {len(records)} 条记录, {len(group_mskus)} 个MSKU")
        
        # 构建费用明细项
        fee_items = []
        
        for record in records:
            msku = record.get('MSKU', '').strip()
            if not msku:
                continue
            
            # 获取金额，并控制精度（保留4位小数，避免浮点数精度问题导致签名不一致）
            cg_price_additional_fee = round(float(record.get('商品成本附加费', 0) or 0), 4)
            cg_transport_additional_fee = round(float(record.get('头程成本附加费', 0) or 0), 4)
            recorded_freight = round(float(record.get('录入费用单头程', 0) or 0), 4)
            exchange_loss = round(float(record.get('汇损', 0) or 0), 4)
            
            # 商品成本附加费
            if cg_price_additional_fee != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": target_date,  # 使用日期格式：Y-m-d
                    "other_fee_type_id": fee_type_ids['商品成本附加费_id'],
                    "fee": cg_price_additional_fee,
                    "currency_code": "CNY",
                    "remark": f"{msku}-ProductCost"
                })
            
            # 头程成本附加费
            if cg_transport_additional_fee != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": target_date,
                    "other_fee_type_id": fee_type_ids['头程成本附加费_id'],
                    "fee": cg_transport_additional_fee,
                    "currency_code": "CNY",
                    "remark": f"{msku}-InboundCost"
                })
            
            # 录入费用单头程（对应头程费用）
            if recorded_freight != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": target_date,
                    "other_fee_type_id": fee_type_ids['头程费用_id'],
                    "fee": recorded_freight,
                    "currency_code": "CNY",
                    "remark": f"{msku}-InboundFee"
                })
            
            # 汇损
            if exchange_loss != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": target_date,
                    "other_fee_type_id": fee_type_ids['汇损_id'],
                    "fee": exchange_loss,
                    "currency_code": "CNY",
                    "remark": f"{msku}-ExchangeLoss"
                })
        
        if not fee_items:
            logger.info(f"    ⚠️  该组没有需要创建的费用项（所有费用都为0），跳过")
            continue
        
        logger.info(f"    准备创建费用单，包含 {len(fee_items)} 个费用明细项")
        
        # 如果费用明细项数量超过限制，分批创建
        if len(fee_items) > MAX_FEE_ITEMS_PER_ORDER:
            logger.info(f"    ⚠️  费用明细项数量({len(fee_items)})超过限制({MAX_FEE_ITEMS_PER_ORDER})，将分批创建")
            
            batch_count = (len(fee_items) + MAX_FEE_ITEMS_PER_ORDER - 1) // MAX_FEE_ITEMS_PER_ORDER
            logger.info(f"    将分成 {batch_count} 批创建费用单")
            
            for batch_idx in range(batch_count):
                start_idx = batch_idx * MAX_FEE_ITEMS_PER_ORDER
                end_idx = min(start_idx + MAX_FEE_ITEMS_PER_ORDER, len(fee_items))
                batch_fee_items = fee_items[start_idx:end_idx]
                
                logger.info(f"    创建第 {batch_idx + 1}/{batch_count} 批，包含 {len(batch_fee_items)} 个费用明细项")
                
                result = await fee_mgmt.create_fee_order(
                    submit_type=2,  # 2=提交
                    dimension=1,  # 1=msku
                    apportion_rule=2,  # 2=按销量
                    is_request_pool=0,  # 0=否
                    remark=f"Auto-{target_date}-{batch_idx + 1}/{batch_count}",
                    fee_items=batch_fee_items
                )
                
                if result:
                    success_count += 1
                    logger.info(f"    ✅ 第 {batch_idx + 1}/{batch_count} 批费用单创建成功")
                    
                    # 每N批后休息一次，避免累积速率限制
                    if (batch_idx + 1) % REST_BATCH_INTERVAL == 0:
                        logger.info(f"    ⏸️  已创建 {batch_idx + 1} 批，休息 {REST_DURATION} 秒以避免累积速率限制...")
                        await asyncio.sleep(REST_DURATION)
                else:
                    error_msg = f"第 {batch_idx + 1}/{batch_count} 批费用单创建失败 (日期={target_date}, 店铺ID={shop_id})"
                    logger.error(f"    ❌ {error_msg}")
                    logger.error(f"    包含 {len(batch_fee_items)} 个费用明细项")
                    logger.error(f"    已成功创建 {success_count} 个费用单，现在停止执行")
                    raise RuntimeError(f"费用单创建失败: {error_msg}，请检查日志并重试")
                
                await asyncio.sleep(REQUEST_DELAY)
        else:
            # 费用明细项数量在限制内，直接创建
            result = await fee_mgmt.create_fee_order(
                submit_type=2,  # 2=提交
                dimension=1,  # 1=msku
                apportion_rule=2,  # 2=按销量
                is_request_pool=0,  # 0=否
                remark=f"Auto-{target_date}",
                fee_items=fee_items
            )
            
            if result:
                success_count += 1
                logger.info(f"    ✅ 费用单创建成功")
            else:
                error_msg = f"费用单创建失败 (日期={target_date}, 店铺ID={shop_id})"
                logger.error(f"    ❌ {error_msg}")
                logger.error(f"    包含 {len(fee_items)} 个费用明细项")
                logger.error(f"    已成功创建 {success_count} 个费用单，现在停止执行")
                raise RuntimeError(f"费用单创建失败: {error_msg}，请检查日志并重试")
            
            await asyncio.sleep(REQUEST_DELAY)
    
    logger.info(f"\n  ✅ 日期 {target_date} 费用单创建完成：成功创建 {success_count} 个费用单")
    
    return success_count


async def process_daily_fee_orders(start_date: str = None, end_date: str = None):
    """
    按日维度处理费用单（遍历每一天，先作废再创建）
    
    Args:
        start_date: 开始日期，格式：Y-m-d，默认：本月1号
        end_date: 结束日期，格式：Y-m-d，默认：今天
    """
    from datetime import datetime
    
    # 确定日期范围（默认为本月1号到今天）
    if start_date is None or end_date is None:
        today = date.today()
        this_month_first_day = date(today.year, today.month, 1)
        
        if start_date is None:
            start_date = this_month_first_day.strftime('%Y-%m-%d')
        if end_date is None:
            end_date = today.strftime('%Y-%m-%d')
    
    logger.info("=" * 80)
    logger.info("🚀 费用单管理 - 按日维度处理（逐日作废+创建）")
    logger.info("=" * 80)
    logger.info(f"日期范围: {start_date} 至 {end_date}")
    logger.info(f"处理模式: 按日处理，每天先作废再创建（默认：本月1号至今天）")
    logger.info("=" * 80)
    
    # 初始化费用管理
    fee_mgmt = FeeManagement()
    
    # 获取访问令牌
    logger.info("\n获取访问令牌...")
    if not await fee_mgmt.init_token():
        logger.error("❌ 无法获取访问令牌")
        return
    
    # 步骤0: 查询费用类型列表
    logger.info("\n查询费用类型列表...")
    fee_types = await fee_mgmt.get_fee_types()
    
    if not fee_types:
        logger.error("❌ 无法获取费用类型列表")
        return
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 从费用类型列表中找到需要的四个费用类型
    fee_type_map = {ft.get('name'): ft.get('id') for ft in fee_types}
    
    商品成本附加费_id = fee_type_map.get('商品成本附加费')
    头程成本附加费_id = fee_type_map.get('头程成本附加费')
    头程费用_id = fee_type_map.get('头程费用')
    汇损_id = fee_type_map.get('汇损')
    
    if not 商品成本附加费_id or not 头程成本附加费_id or not 头程费用_id or not 汇损_id:
        logger.error("❌ 无法找到所需的费用类型ID")
        logger.error(f"  商品成本附加费_id: {商品成本附加费_id}")
        logger.error(f"  头程成本附加费_id: {头程成本附加费_id}")
        logger.error(f"  头程费用_id: {头程费用_id}")
        logger.error(f"  汇损_id: {汇损_id}")
        return
    
    fee_type_ids = {
        '商品成本附加费_id': 商品成本附加费_id,
        '头程成本附加费_id': 头程成本附加费_id,
        '头程费用_id': 头程费用_id,
        '汇损_id': 汇损_id
    }
    
    logger.info(f"找到费用类型ID:")
    logger.info(f"  商品成本附加费_id: {商品成本附加费_id}")
    logger.info(f"  头程成本附加费_id: {头程成本附加费_id}")
    logger.info(f"  头程费用_id: {头程费用_id}")
    logger.info(f"  汇损_id: {汇损_id}")
    
    # 生成日期列表
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    date_list = []
    current_date = start_date_obj
    while current_date <= end_date_obj:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    logger.info(f"\n需要处理 {len(date_list)} 天: {date_list[0]} 至 {date_list[-1]}")
    
    # 统计信息
    total_success_count = 0
    processed_days = 0
    skipped_days = 0
    
    # 逐日处理
    for day_index, target_date in enumerate(date_list, 1):
        logger.info("\n" + "=" * 80)
        logger.info(f"处理第 {day_index}/{len(date_list)} 天: {target_date}")
        logger.info("=" * 80)
        
        # 步骤1: 作废该天的费用单
        logger.info(f"\n步骤1: 作废日期 {target_date} 的已有费用单")
        await discard_existing_fee_orders(
            fee_mgmt,
            target_date,  # 开始日期
            target_date,  # 结束日期（同一天）
            [商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id]
        )
        
        await asyncio.sleep(REQUEST_DELAY)
        
        # 步骤2: 读取该天的数据
        logger.info(f"\n步骤2: 从数据库读取日期 {target_date} 的利润报表数据")
        profit_data = fetch_profit_report_data_daily(target_date)
        
        if not profit_data:
            logger.info(f"  ℹ️  日期 {target_date} 没有需要创建费用单的数据，跳过")
            skipped_days += 1
            continue
        
        # 步骤3: 创建该天的费用单
        logger.info(f"\n步骤3: 创建日期 {target_date} 的费用单")
        day_success_count = await create_fee_orders_for_single_day(
            fee_mgmt,
            target_date,
            profit_data,
            fee_type_ids
        )
        
        total_success_count += day_success_count
        processed_days += 1
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"✅ 日期 {target_date} 处理完成，创建了 {day_success_count} 个费用单")
        logger.info(f"{'=' * 80}")
        
        # 避免请求过快
        await asyncio.sleep(REQUEST_DELAY)
    
    # 最终统计
    logger.info("\n" + "=" * 80)
    logger.info("🎉 所有日期处理完成")
    logger.info("=" * 80)
    logger.info(f"总计处理天数: {len(date_list)} 天")
    logger.info(f"成功处理: {processed_days} 天")
    logger.info(f"跳过（无数据）: {skipped_days} 天")
    logger.info(f"总计创建费用单: {total_success_count} 个")
    logger.info("=" * 80)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='按日维度处理费用单（逐日作废+创建）')
    parser.add_argument('--start-date', type=str, default=None, 
                       help='开始日期，格式：Y-m-d，默认：本月1号')
    parser.add_argument('--end-date', type=str, default=None,
                       help='结束日期，格式：Y-m-d，默认：今天')
    
    args = parser.parse_args()
    
    try:
        asyncio.run(process_daily_fee_orders(
            start_date=args.start_date,
            end_date=args.end_date
        ))
    except KeyboardInterrupt:
        logger.info("⚠️  任务被用户中断")
    except Exception as e:
        logger.error(f"❌ 任务执行失败: {str(e)}", exc_info=True)
        raise

