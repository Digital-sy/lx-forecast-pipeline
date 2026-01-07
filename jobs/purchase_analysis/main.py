#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购下单分析项目 - 主入口

功能：
1. 采集产品管理数据（从领星API，增量更新最近7天）
2. 采集FBA库存数据（从领星API，全量更新）
3. 采集仓库库存明细（从领星API，全仓数据）
4. 采集销量统计数据（从领星API，MSKU维度-月度）
5. 采集采购单数据（从领星API）
6. 生成库存预估表

注意：以下任务已暂时禁用：
- 采集运营下单计划（从飞书）
- 生成采购vs计划分析表
- 生成销量预估表

运行方式：
    python -m jobs.purchase_analysis.main
"""
import asyncio
import httpx
import traceback
from datetime import datetime
from common import get_logger

logger = get_logger('purchase_analysis.main')

# 任务间隔时间（秒）
TASK_INTERVAL = 60  # 1分钟

# 飞书webhook地址
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/00640680-6577-4a95-b25a-35c34864ff45"


async def send_feishu_message(message: str) -> bool:
    """
    发送消息到飞书群
    
    Args:
        message: 要发送的消息内容
        
    Returns:
        bool: 是否发送成功
    """
    try:
        data = {
            "msg_type": "text",
            "content": {
                "text": message
            }
        }
        
        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(FEISHU_WEBHOOK_URL, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                logger.info("飞书消息发送成功")
                return True
            else:
                logger.error(f"飞书消息发送失败: {result.get('msg')}")
                return False
    except Exception as e:
        logger.error(f"发送飞书消息异常: {e}")
        return False


def parse_error_info(error: Exception, traceback_str: str) -> dict:
    """
    解析错误信息，提取关键信息
    
    Args:
        error: 异常对象
        traceback_str: 完整的堆栈跟踪字符串
        
    Returns:
        dict: 包含错误详细信息的字典
    """
    import sys
    
    error_type = type(error).__name__
    error_message = str(error)
    
    # 解析堆栈跟踪，提取文件名和行号
    file_name = "未知"
    line_number = 0
    function_name = "未知"
    
    # 从堆栈跟踪中提取信息
    lines = traceback_str.split('\n')
    for i, line in enumerate(lines):
        # 查找包含文件路径和行号的行（格式：File "path", line X, in function）
        if 'File "' in line or "File '" in line:
            try:
                # 提取文件路径和行号
                import re
                # 匹配 File "path", line X, in function
                match = re.search(r'File ["\'](.+?)["\'], line (\d+), in (.+)', line)
                if match:
                    file_path = match.group(1)
                    line_number = int(match.group(2))
                    function_name = match.group(3)
                    
                    # 提取文件名（只取最后一部分）
                    import os
                    file_name = os.path.basename(file_path)
                    
                    # 如果找到了，就使用这个（通常是第一个，即最接近错误的位置）
                    if file_name != "main.py":  # 优先显示非main.py的文件
                        break
            except:
                pass
    
    # 如果没有找到，尝试从最后一个堆栈帧获取
    if file_name == "未知" and hasattr(error, '__traceback__'):
        tb = error.__traceback__
        while tb:
            frame = tb.tb_frame
            file_path = frame.f_code.co_filename
            line_number = tb.tb_lineno
            function_name = frame.f_code.co_name
            import os
            file_name = os.path.basename(file_path)
            if file_name != "main.py":
                break
            tb = tb.tb_next
    
    return {
        "error_type": error_type,
        "error_message": error_message,
        "file_name": file_name,
        "line_number": line_number,
        "function_name": function_name,
        "full_traceback": traceback_str
    }


async def main():
    """主函数：按顺序执行所有任务，每个任务间隔1分钟"""
    logger.info("="*80)
    logger.info("采购下单分析项目 - 开始执行")
    logger.info("="*80)
    
    start_time = datetime.now()
    current_task = "未知"  # 当前执行的任务名称
    
    try:
        # 0. 加载店铺映射（可选，如果需要在日志中显示）
        logger.info("\n[0/6] 加载店铺映射...")
        from .shop_mapping import get_shop_mapping
        shop_mapping = await get_shop_mapping()
        logger.info(f"✅ 店铺映射加载完成，共 {len(shop_mapping)} 个店铺\n")
        
        # 1. 采集产品管理数据（增量更新最近7天）
        current_task = "[1/6] 采集产品管理数据"
        logger.info(f"{current_task}（增量更新最近7天）...")
        from .fetch_product import main as fetch_product_main
        await fetch_product_main()
        logger.info("✅ 产品管理数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 2. 采集FBA库存数据（全量更新）
        current_task = "[2/6] 采集FBA库存数据"
        logger.info(f"\n{current_task}（全量更新）...")
        from .fetch_fba_inventory import main as fetch_fba_main
        await fetch_fba_main()
        logger.info("✅ FBA库存数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 3. 采集仓库库存明细（全仓数据）
        current_task = "[3/6] 采集仓库库存明细"
        logger.info(f"\n{current_task}（全仓数据）...")
        from .fetch_inventory_details import main as fetch_inventory_main
        await fetch_inventory_main()
        logger.info("✅ 仓库库存明细采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 4. 采集销量统计数据（MSKU维度-月度）
        current_task = "[4/6] 采集销量统计数据"
        logger.info(f"\n{current_task}（MSKU维度-月度）...")
        from .fetch_sale_stat_v2_msku_monthly import main as fetch_sale_stat_main
        await fetch_sale_stat_main()
        logger.info("✅ 销量统计数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 5. 采集采购单数据
        current_task = "[5/6] 采集采购单数据"
        logger.info(f"\n{current_task}...")
        from .fetch_purchase import main as fetch_purchase_main
        await fetch_purchase_main()
        logger.info("✅ 采购单数据采集完成")
        
        # 任务间隔：等待1分钟
        logger.info(f"\n⏳ 等待 {TASK_INTERVAL} 秒后执行下一个任务...")
        await asyncio.sleep(TASK_INTERVAL)
        
        # 6. 生成库存预估表（原任务8）
        current_task = "[6/6] 生成库存预估表"
        logger.info(f"\n{current_task}...")
        from .generate_inventory_estimate import main as generate_inventory_main
        generate_inventory_main()
        logger.info("✅ 库存预估表生成完成")
        
        # 统计信息
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*80)
        logger.info("✅ 采购下单分析项目 - 全部完成")
        logger.info(f"   总耗时: {duration:.2f} 秒")
        logger.info("="*80)
        
    except Exception as e:
        error_traceback = traceback.format_exc()
        
        # 记录错误日志
        logger.error(f"❌ 执行失败: {e}", exc_info=True)
        
        # 解析错误信息
        error_info = parse_error_info(e, error_traceback)
        
        # 发送错误消息到飞书
        feishu_message = f"""❌ 采购下单分析项目执行失败

📋 错误类型: {error_info['error_type']}
📝 错误原因: {error_info['error_message']}
📁 出错文件: {error_info['file_name']}
📍 出错行号: {error_info['line_number']}
🔧 出错函数: {error_info['function_name']}
📌 当前任务: {current_task}
⏰ 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📄 完整错误堆栈:
```
{error_info['full_traceback']}
```

请及时检查并处理！"""
        
        try:
            await send_feishu_message(feishu_message)
        except Exception as feishu_error:
            logger.error(f"发送飞书消息失败: {feishu_error}")
        
        raise


if __name__ == "__main__":
    asyncio.run(main())

