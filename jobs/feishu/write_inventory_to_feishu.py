#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
仓库库存明细写入飞书多维表任务
从数据库读取仓库库存明细数据，用SKU匹配销量统计_msku月度表的品名字段，然后写入飞书多维表
"""
import asyncio
import traceback
import httpx
from typing import List, Dict, Any
from datetime import datetime

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import settings, get_logger
from common.database import db_cursor
from common.feishu import FeishuClient

logger = get_logger('feishu_write_inventory')

# 飞书webhook地址（与 main.py 保持一致）
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

# 飞书多维表格配置
FEISHU_APP_TOKEN = "Ir66bUSjdamVttsGiSRcnK1ln2e"  # 飞书多维表格的app_token
FEISHU_TABLE_ID = "tblO2ascMB5pvR0R"  # 飞书多维表格的table_id


def read_inventory_data() -> List[Dict[str, Any]]:
    """
    从数据库读取仓库库存明细数据
    
    Returns:
        List[Dict[str, Any]]: 库存明细数据列表
    """
    with db_cursor() as cursor:
        sql = """
        SELECT 
            SKU,
            店铺,
            仓库,
            FNSKU,
            实际库存总量,
            可用量,
            待到货量,
            在途数量,
            待检待上架量,
            调拨在途头程成本,
            每件库存成本,
            总库存成本,
            平均库龄,
            `0-15天库龄`,
            `16-30天库龄`,
            `31-60天库龄`,
            `61-90天库龄`,
            `91-120天库龄`,
            `121-180天库龄`,
            `181-360天库龄`,
            `361天以上库龄`
        FROM `仓库库存明细`
        WHERE SKU IS NOT NULL 
          AND SKU != '' 
          AND SKU != '无'
        ORDER BY SKU, 店铺, 仓库
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        logger.info(f"从数据库读取到 {len(results)} 条仓库库存明细数据")
        return results


def get_product_name_mapping() -> Dict[str, str]:
    """
    从产品管理表获取SKU到品名的映射
    
    Returns:
        Dict[str, str]: {SKU: 品名} 的映射字典
    """
    product_name_map = {}
    
    try:
        with db_cursor() as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '产品管理'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("产品管理表不存在，无法匹配品名")
                return product_name_map
            
            # 检查是否有品名字段
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '产品管理'
                AND COLUMN_NAME = '品名'
            """)
            result = cursor.fetchone()
            has_product_name = (result.get('cnt', 0) if result else 0) > 0
            
            if not has_product_name:
                logger.warning("产品管理表中没有品名字段，无法匹配品名")
                return product_name_map
            
            # 查询SKU和品名的映射（去重，如果同一个SKU有多个品名，取第一个非空的）
            sql = """
            SELECT 
                SKU,
                品名
            FROM `产品管理`
            WHERE SKU IS NOT NULL 
              AND SKU != '' 
              AND SKU != '无'
              AND 品名 IS NOT NULL
              AND 品名 != ''
              AND 品名 != '无'
            GROUP BY SKU, 品名
            ORDER BY SKU, 品名
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            for row in results:
                sku = row.get('SKU', '').strip()
                product_name = row.get('品名', '').strip()
                
                if sku and product_name:
                    # 如果同一个SKU有多个品名，保留第一个
                    if sku not in product_name_map:
                        product_name_map[sku] = product_name
            
            logger.info(f"从产品管理表读取到 {len(product_name_map)} 条SKU到品名的映射")
    except Exception as e:
        logger.warning(f"从产品管理表读取品名映射失败: {e}")
    
    return product_name_map


def prepare_feishu_records(inventory_data: List[Dict[str, Any]], 
                           product_name_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    准备飞书多维表的记录数据
    过滤掉可用量和待到货量都为0的记录
    
    Args:
        inventory_data: 库存明细数据列表
        product_name_map: SKU到品名的映射字典
        
    Returns:
        List[Dict[str, Any]]: 飞书记录列表
    """
    records = []
    filtered_count = 0
    
    for row in inventory_data:
        sku = row.get('SKU', '').strip()
        if not sku or sku == '无':
            continue
        
        # 获取可用量和待到货量
        available = int(row.get('可用量', 0) or 0)
        pending = int(row.get('待到货量', 0) or 0)
        
        # 如果可用量和待到货量都为0，则跳过
        if available == 0 and pending == 0:
            filtered_count += 1
            continue
        
        # 获取品名（从映射中查找）
        product_name = product_name_map.get(sku, '')
        
        record = {
            'SKU': sku,
            '品名': product_name if product_name else '',
            '店铺': row.get('店铺', '') or '',
            '仓库': row.get('仓库', '') or '',
            'FNSKU': row.get('FNSKU', '') or '',
            '实际库存总量': int(row.get('实际库存总量', 0) or 0),
            '可用量': available,
            '待到货量': pending,
            '在途数量': int(row.get('在途数量', 0) or 0),
            '待检待上架量': int(row.get('待检待上架量', 0) or 0),
            '调拨在途头程成本': float(row.get('调拨在途头程成本', 0) or 0),
            '每件库存成本': float(row.get('每件库存成本', 0) or 0),
            '总库存成本': float(row.get('总库存成本', 0) or 0),
            '平均库龄': int(row.get('平均库龄', 0) or 0),
            '0-15天库龄': int(row.get('0-15天库龄', 0) or 0),
            '16-30天库龄': int(row.get('16-30天库龄', 0) or 0),
            '31-60天库龄': int(row.get('31-60天库龄', 0) or 0),
            '61-90天库龄': int(row.get('61-90天库龄', 0) or 0),
            '91-120天库龄': int(row.get('91-120天库龄', 0) or 0),
            '121-180天库龄': int(row.get('121-180天库龄', 0) or 0),
            '181-360天库龄': int(row.get('181-360天库龄', 0) or 0),
            '361天以上库龄': int(row.get('361天以上库龄', 0) or 0),
        }
        
        records.append(record)
    
    if filtered_count > 0:
        logger.info(f"已过滤 {filtered_count} 条可用量和待到货量都为0的记录")
    logger.info(f"共准备 {len(records)} 条飞书记录")
    return records


async def write_to_feishu(records: List[Dict[str, Any]]) -> bool:
    """
    将数据写入飞书多维表
    
    Args:
        records: 记录列表
        
    Returns:
        bool: 是否成功
    """
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"正在写入数据到飞书多维表...")
        logger.info(f"{'='*80}")
        
        # 创建飞书客户端
        feishu_client = FeishuClient(
            app_token=FEISHU_APP_TOKEN,
            table_id=FEISHU_TABLE_ID
        )
        
        # 准备字段列表
        field_list = [
            {'name': 'SKU', 'type': 'text'},
            {'name': '品名', 'type': 'text'},
            {'name': '店铺', 'type': 'text'},
            {'name': '仓库', 'type': 'text'},
            {'name': 'FNSKU', 'type': 'text'},
            {'name': '实际库存总量', 'type': 'number'},
            {'name': '可用量', 'type': 'number'},
            {'name': '待到货量', 'type': 'number'},
            {'name': '在途数量', 'type': 'number'},
            {'name': '待检待上架量', 'type': 'number'},
            {'name': '调拨在途头程成本', 'type': 'number'},
            {'name': '每件库存成本', 'type': 'number'},
            {'name': '总库存成本', 'type': 'number'},
            {'name': '平均库龄', 'type': 'number'},
            {'name': '0-15天库龄', 'type': 'number'},
            {'name': '16-30天库龄', 'type': 'number'},
            {'name': '31-60天库龄', 'type': 'number'},
            {'name': '61-90天库龄', 'type': 'number'},
            {'name': '91-120天库龄', 'type': 'number'},
            {'name': '121-180天库龄', 'type': 'number'},
            {'name': '181-360天库龄', 'type': 'number'},
            {'name': '361天以上库龄', 'type': 'number'},
        ]
        
        # 确保字段存在
        logger.info(f"正在确保字段存在...")
        existing_fields = await feishu_client.get_table_fields()  # {field_id: field_name}
        existing_field_names = set(existing_fields.values())
        
        # 创建缺失的字段
        for field_info in field_list:
            field_name = field_info.get('name', '')
            field_type_str = field_info.get('type', 'text')
            
            if field_name in existing_field_names:
                logger.debug(f"字段 {field_name} 已存在，跳过创建")
                continue
            
            # 转换字段类型
            if field_type_str == 'number':
                field_type = "2"  # 数字类型
            else:
                field_type = "1"  # 多行文本类型
            
            try:
                await feishu_client.create_field(field_name, field_type)
                logger.info(f"创建字段: {field_name} (类型: {field_type_str})")
            except Exception as e:
                logger.warning(f"创建字段 {field_name} 失败: {e}")
                # 继续处理其他字段
        
        if not records:
            logger.warning("没有数据需要写入")
            return True
        
        # 先清空现有数据
        logger.info(f"正在清空现有数据...")
        max_retries = 3
        deleted_count = 0
        
        for retry in range(max_retries):
            try:
                deleted_count = await feishu_client.delete_all_records()
                logger.info(f"成功清空 {deleted_count} 条旧记录")
                break
            except asyncio.TimeoutError:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 10
                    logger.warning(f"清空数据超时（尝试 {retry + 1}/{max_retries}），{wait_time}秒后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"清空数据超时，已重试 {max_retries} 次")
                    raise Exception(f"清空数据失败：超时")
            except Exception as e:
                error_str = str(e)
                if "超时" in error_str or "timeout" in error_str.lower():
                    if retry < max_retries - 1:
                        wait_time = (retry + 1) * 10
                        logger.warning(f"清空数据超时（尝试 {retry + 1}/{max_retries}），{wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"清空数据超时，已重试 {max_retries} 次")
                        raise Exception(f"清空数据失败：超时")
                else:
                    logger.error(f"清空数据失败: {e}")
                    raise Exception(f"清空数据失败: {e}")
        
        # 检查记录数限制（飞书多维表最大支持20000条记录）
        max_records = 20000
        if len(records) > max_records:
            logger.error(f"要写入的记录数 {len(records)} 超过飞书多维表的最大限制 {max_records} 条")
            raise Exception(f"要写入的记录数 {len(records)} 超过飞书多维表的最大限制 {max_records} 条")
        
        # 写入数据
        logger.info(f"正在写入数据到飞书多维表...")
        written_count = await feishu_client.write_records(records, batch_size=500)
        logger.info(f"✓ 成功写入 {written_count} 条记录到飞书多维表")
        
        return True
        
    except Exception as e:
        logger.error(f"写入飞书多维表失败: {e}", exc_info=True)
        return False


async def main():
    """主函数"""
    logger.info("="*80)
    logger.info("仓库库存明细写入飞书多维表任务")
    logger.info("="*80)
    
    # 检查配置
    if not FEISHU_APP_TOKEN or not FEISHU_TABLE_ID:
        logger.error("请先配置 FEISHU_APP_TOKEN 和 FEISHU_TABLE_ID")
        return
    
    # 验证配置
    if not settings.validate():
        logger.error("配置验证失败，请检查.env文件")
        return
    
    try:
        # 1. 从数据库读取仓库库存明细数据
        logger.info("\n正在从数据库读取仓库库存明细数据...")
        inventory_data = read_inventory_data()
        
        if not inventory_data:
            logger.warning("数据库中没有仓库库存明细数据")
            return
        
        # 2. 从产品管理表获取SKU到品名的映射
        logger.info("\n正在从产品管理表获取品名映射...")
        product_name_map = get_product_name_mapping()
        
        matched_count = 0
        unmatched_count = 0
        for row in inventory_data:
            sku = row.get('SKU', '').strip()
            if sku and sku in product_name_map:
                matched_count += 1
            else:
                unmatched_count += 1
        
        logger.info(f"品名匹配统计: 匹配成功 {matched_count} 条，未匹配 {unmatched_count} 条")
        
        # 3. 准备飞书记录
        logger.info("\n正在准备飞书记录...")
        records = prepare_feishu_records(inventory_data, product_name_map)
        
        if not records:
            logger.warning("没有数据需要写入")
            return
        
        # 4. 写入飞书多维表
        logger.info("\n正在写入飞书多维表...")
        success = await write_to_feishu(records)
        
        if success:
            logger.info("\n" + "="*80)
            logger.info("✅ 数据写入完成！")
            logger.info("="*80)
        else:
            logger.error("\n" + "="*80)
            logger.error("❌ 数据写入失败！")
            logger.error("="*80)
        
    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"❌ 处理失败: {e}", exc_info=True)
        
        # 发送错误消息到飞书
        feishu_message = f"""❌ 库存明细写入飞书任务执行失败

📋 错误类型: {type(e).__name__}
📝 错误原因: {str(e)}
⏰ 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📄 完整错误堆栈:
```
{error_traceback}
```

请及时检查并处理！"""
        
        try:
            await send_feishu_message(feishu_message)
        except Exception as feishu_error:
            logger.error(f"发送飞书消息失败: {feishu_error}")
        
        raise


if __name__ == '__main__':
    asyncio.run(main())

