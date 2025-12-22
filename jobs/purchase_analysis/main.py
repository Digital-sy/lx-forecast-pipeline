#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购下单分析项目 - 主入口

功能：
1. 采集采购单数据（从领星API）
2. 采集运营下单计划（从飞书）
3. 生成采购vs计划分析表

运行方式：
    python -m jobs.purchase_analysis.main
"""
import asyncio
from datetime import datetime
from common import get_logger

logger = get_logger('purchase_analysis.main')


async def main():
    """主函数：按顺序执行所有任务"""
    logger.info("="*80)
    logger.info("采购下单分析项目 - 开始执行")
    logger.info("="*80)
    
    start_time = datetime.now()
    
    try:
        # 0. 加载店铺映射（可选，如果需要在日志中显示）
        logger.info("\n[0/3] 加载店铺映射...")
        from .shop_mapping import get_shop_mapping
        shop_mapping = await get_shop_mapping()
        logger.info(f"✅ 店铺映射加载完成，共 {len(shop_mapping)} 个店铺\n")
        
        # 1. 采集采购单数据
        logger.info("[1/3] 开始采集采购单数据...")
        from .fetch_purchase import main as fetch_purchase_main
        await fetch_purchase_main()
        logger.info("✅ 采购单数据采集完成")
        
        # 2. 采集运营下单计划
        logger.info("\n[2/3] 开始采集运营下单计划...")
        from .fetch_operation import main as fetch_operation_main
        await fetch_operation_main()
        logger.info("✅ 运营下单计划采集完成")
        
        # 3. 生成分析表
        logger.info("\n[3/3] 开始生成分析表...")
        from .generate_analysis import main as generate_analysis_main
        generate_analysis_main()
        logger.info("✅ 分析表生成完成")
        
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

