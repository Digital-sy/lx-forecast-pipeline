#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购下单分析项目 - 主入口

功能：
1. 采集FBA库存数据（从领星API，全量更新）
2. 采集仓库库存明细（从领星API，全仓数据）
3. 采集销量统计数据（从领星API，MSKU维度-月度）
4. 采集采购单数据（从领星API）
5. 采集运营下单计划（从飞书）
6. 生成采购vs计划分析表
7. 生成库存预估表
8. 生成销量预估表

运行方式：
    python -m jobs.purchase_analysis.main
"""
import asyncio
from datetime import datetime
from common import get_logger

logger = get_logger('purchase_analysis.main')

# 任务间隔时间（秒）
TASK_INTERVAL = 60  # 1分钟


async def main():
    """主函数：按顺序执行所有任务，每个任务间隔1分钟"""
    logger.info("="*80)
    logger.info("采购下单分析项目 - 开始执行")
    logger.info("="*80)
    
    start_time = datetime.now()
    
    try:
        # 0. 加载店铺映射（可选，如果需要在日志中显示）
        logger.info("\n[0/8] 加载店铺映射...")
        from .shop_mapping import get_shop_mapping
        shop_mapping = await get_shop_mapping()
        logger.info(f"✅ 店铺映射加载完成，共 {len(shop_mapping)} 个店铺\n")
        
        # 1. 采集FBA库存数据（全量更新）
        logger.info("[1/8] 开始采集FBA库存数据（全量更新）...")
        from .fetch_fba_inventory import main as fetch_fba_main
        await fetch_fba_main()
        logger.info("✅ FBA库存数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 2. 采集仓库库存明细（全仓数据）
        logger.info("\n[2/8] 开始采集仓库库存明细（全仓数据）...")
        from .fetch_inventory_details import main as fetch_inventory_main
        await fetch_inventory_main()
        logger.info("✅ 仓库库存明细采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 3. 采集销量统计数据（MSKU维度-月度）
        logger.info("\n[3/8] 开始采集销量统计数据（MSKU维度-月度）...")
        from .fetch_sale_stat_v2_msku_monthly import main as fetch_sale_stat_main
        await fetch_sale_stat_main()
        logger.info("✅ 销量统计数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 4. 采集采购单数据
        logger.info("\n[4/8] 开始采集采购单数据...")
        from .fetch_purchase import main as fetch_purchase_main
        await fetch_purchase_main()
        logger.info("✅ 采购单数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 5. 采集运营下单计划
        logger.info("\n[5/8] 开始采集运营下单计划...")
        from .fetch_operation import main as fetch_operation_main
        await fetch_operation_main()
        logger.info("✅ 运营下单计划采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 6. 生成分析表
        logger.info("\n[6/8] 开始生成分析表...")
        from .generate_analysis import main as generate_analysis_main
        generate_analysis_main()
        logger.info("✅ 分析表生成完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 7. 生成库存预估表
        logger.info("\n[7/8] 开始生成库存预估表...")
        from .generate_inventory_estimate import main as generate_inventory_main
        generate_inventory_main()
        logger.info("✅ 库存预估表生成完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 8. 生成销量预估表
        logger.info("\n[8/8] 开始生成销量预估表...")
        from .generate_sales_forecast import main as generate_sales_forecast_main
        generate_sales_forecast_main()
        logger.info("✅ 销量预估表生成完成")
        
        # 统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*80)
        logger.info("✅ 采购下单分析项目 - 全部完成")
        logger.info(f"   总耗时: {duration:.2f} 秒")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ 执行失败: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())

