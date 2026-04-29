#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
从飞书多维表格读取面料数据
读取指定飞书表格的款号、季节、面料等信息，按款号+面料展开成多条记录
对于同一款号，选择创建时间最新的记录
"""
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import settings, get_logger
from common.feishu import FeishuClient
from common.database import db_cursor

logger = get_logger('feishu_fetch_data')

# 飞书多维表格配置
FEISHU_APP_TOKEN = "PItubmXkWarkqosFanGcxKJGnvT"  # 多维表格ID
FEISHU_TABLE_ID = "tblWgIHRbvi9uWvR"              # 数据表ID
FEISHU_VIEW_ID = "vew7QHqIW2"                      # 视图ID（可选）


def extract_text_value(value: Any) -> str:
    """
    从飞书字段值中提取文本
    
    Args:
        value: 飞书字段值
        
    Returns:
        str: 提取的文本值
    """
    if value is None:
        return ''
    
    # 如果是列表，取第一个元素
    if isinstance(value, list) and len(value) > 0:
        first_item = value[0]
        # 如果是字典，尝试提取text字段
        if isinstance(first_item, dict):
            return first_item.get('text', '')
        return str(first_item)
    
    # 如果是字典，尝试提取text字段
    if isinstance(value, dict):
        return value.get('text', '')
    
    # 其他情况直接转字符串
    return str(value)


def extract_numeric_value(value: Any) -> float:
    """
    从飞书字段值中提取数值
    
    Args:
        value: 飞书字段值
        
    Returns:
        float: 提取的数值
    """
    if value is None:
        return 0.0
    
    # 如果是列表，取第一个元素
    if isinstance(value, list) and len(value) > 0:
        first_item = value[0]
        # 如果是字典，尝试提取text字段
        if isinstance(first_item, dict):
            text = first_item.get('text', '0')
            try:
                return float(text)
            except (ValueError, TypeError):
                return 0.0
        try:
            return float(first_item)
        except (ValueError, TypeError):
            return 0.0
    
    # 如果是字符串或数字，直接转换
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def extract_fabric_records(record, field_map):
    """
    从一条飞书记录中提取面料信息，展开成多条记录。
    新增：核价类型（记录级，所有面料共享）、适用部位（面料级）。
    """
    fields = record.get('fields', {})

    # 记录级基础字段
    spu = extract_text_value(fields.get('款号', ''))
    season = extract_text_value(fields.get('季节', ''))

    # 核价类型：记录级，所有面料共用同一个值
    pricing_type = extract_text_value(fields.get('核价类型', ''))

    create_time = fields.get('创建时间', 0)
    if create_time and isinstance(create_time, (int, float)):
        from datetime import datetime
        create_time_str = datetime.fromtimestamp(create_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
    else:
        create_time_str = str(create_time)

    fabric_records = []

    for i in range(1, 4):  # 面料1 ~ 面料3
        fabric_name = extract_text_value(fields.get(f'面料{i}', ''))
        if not fabric_name:
            continue

        usage     = extract_numeric_value(fields.get(f'单件用量/M-面料{i}', 0))
        loss      = extract_numeric_value(fields.get(f'单件损耗/M-面料{i}', 0))
        price     = extract_numeric_value(fields.get(f'单件价格-面料{i}', 0))
        unit_price = extract_numeric_value(fields.get(f'面料{i}单价', 0))

        # 适用部位：面料级，每个面料独立
        applicable_part = extract_text_value(fields.get(f'适用部位-面料{i}', ''))

        fabric_records.append({
            'SPU':       spu,
            '季节':      season,
            '面料':      fabric_name,
            '单件用量':  usage,
            '单件损耗':  loss,
            '单件价格':  price,
            '面料单价':  unit_price,
            '核价类型':  pricing_type,      # 新增
            '适用部位':  applicable_part,   # 新增
            '创建时间':  create_time_str,
            '创建时间戳': create_time,      # 供 filter_latest_by_spu 排序用，不入库
        })

    return fabric_records


# ────────────────────────────────────────────────────────────────────────────
# 2. create_fabric_table_if_not_exists
# ────────────────────────────────────────────────────────────────────────────

def filter_latest_by_spu(fabric_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    对于同一个SPU，只保留创建时间最新的记录
    
    Args:
        fabric_records: 面料记录列表
        
    Returns:
        List[Dict[str, Any]]: 过滤后的记录列表
    """
    # 按SPU分组
    spu_groups = defaultdict(list)
    for record in fabric_records:
        spu = record.get('SPU', '')
        if spu:
            spu_groups[spu].append(record)
    
    # 对每个SPU，选择创建时间最新的记录
    latest_records = []
    for spu, records in spu_groups.items():
        # 按创建时间戳排序，取最新的
        records_sorted = sorted(records, key=lambda x: x.get('创建时间戳', 0), reverse=True)
        # 同一个SPU的所有面料记录都来自同一条原始记录，所以一起保留
        if records_sorted:
            # 获取最新记录的时间戳
            latest_timestamp = records_sorted[0].get('创建时间戳', 0)
            # 保留所有时间戳等于最新时间戳的记录（同一个SPU的多个面料）
            for record in records_sorted:
                if record.get('创建时间戳', 0) == latest_timestamp:
                    latest_records.append(record)
    
    return latest_records


def create_fabric_table_if_not_exists():
    """
    创建面料核价表（如果不存在）；
    若表已存在则用 ALTER TABLE 兼容式追加 核价类型、适用部位 两列。
    """
    from common.database import db_cursor
    import logging
    logger = logging.getLogger('feishu_fetch_data')
    logger.info("正在检查/创建面料核价表...")

    try:
        with db_cursor(dictionary=False) as cursor:
            # 建表（含新字段）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `面料核价表` (
                    `id`           INT AUTO_INCREMENT PRIMARY KEY,
                    `SPU`          VARCHAR(255)  COMMENT '款号',
                    `季节`         VARCHAR(100),
                    `面料`         VARCHAR(500),
                    `单件用量`     DOUBLE        COMMENT '单位：米',
                    `单件损耗`     DOUBLE        COMMENT '损耗系数',
                    `单件价格`     DOUBLE        COMMENT '单位：元',
                    `面料单价`     DOUBLE        COMMENT '每米单价，单位：元/米',
                    `核价类型`     VARCHAR(100)  COMMENT '飞书核价类型字段，如终版/初版等',
                    `适用部位`     VARCHAR(200)  COMMENT '该面料适用的部位，如主体/里布等',
                    `创建时间`     DATETIME      COMMENT '飞书记录创建时间',
                    `数据更新时间` DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_spu    (`SPU`),
                    INDEX idx_season (`季节`),
                    INDEX idx_fabric (`面料`(100)),
                    UNIQUE KEY uk_spu_fabric (`SPU`, `面料`(100))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='飞书面料核价数据'
            """)
            logger.info("  表检查/创建完成（含 核价类型、适用部位 字段）")

    except Exception as e:
        logger.error(f"创建面料核价表失败: {e}", exc_info=True)
        raise


# ────────────────────────────────────────────────────────────────────────────
# 3. insert_fabric_data_batch
# ────────────────────────────────────────────────────────────────────────────

def insert_fabric_data_batch(data_list):
    """
    批量插入面料数据到数据库（INSERT ... ON DUPLICATE KEY UPDATE）。
    新增 核价类型、适用部位 两字段。
    """
    from common.database import db_cursor
    import logging
    logger = logging.getLogger('feishu_fetch_data')

    if not data_list:
        logger.warning("没有数据需要插入")
        return 0

    try:
        with db_cursor(dictionary=False) as cursor:
            # 先删除这批 SPU 的所有旧记录，确保与飞书最新版一致
            spus = list({r.get("SPU", "") for r in data_list if r.get("SPU")})
            if spus:
                placeholders = ",".join(["%s"] * len(spus))
                cursor.execute(f"DELETE FROM `面料核价表` WHERE SPU IN ({placeholders})", spus)
                logger.info(f"  已删除旧记录 {cursor.rowcount} 条（涉及 {len(spus)} 个SPU）")
            sql = """
            INSERT IGNORE INTO `面料核价表`
                (`SPU`, `季节`, `面料`, `单件用量`, `单件损耗`, `单件价格`, `面料单价`,
                 `核价类型`, `适用部位`, `创建时间`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            batch_data = [
                (
                    r.get('SPU', ''),
                    r.get('季节', ''),
                    r.get('面料', ''),
                    r.get('单件用量', 0),
                    r.get('单件损耗', 0),
                    r.get('单件价格', 0),
                    r.get('面料单价', 0),
                    r.get('核价类型', ''),   # 新增
                    r.get('适用部位', ''),   # 新增
                    r.get('创建时间', '1970-01-01 00:00:00'),
                )
                for r in data_list
            ]

            cursor.executemany(sql, batch_data)
            affected_rows = cursor.rowcount
            logger.info(f"成功插入/更新 {affected_rows} 条面料记录")
            return affected_rows

    except Exception as e:
        logger.error(f"插入面料数据失败: {e}", exc_info=True)
        raise

async def fetch_and_print_data():
    """
    从飞书多维表格获取数据，提取面料信息并打印
    """
    try:
        # 创建飞书客户端
        logger.info("正在初始化飞书客户端...")
        
        client = FeishuClient(
            app_token=FEISHU_APP_TOKEN,
            table_id=FEISHU_TABLE_ID,
            view_id=FEISHU_VIEW_ID
        )
        
        # 获取访问令牌
        logger.info("正在获取访问令牌...")
        await client.get_access_token()
        
        # 获取表格字段信息
        logger.info("正在获取表格字段信息...")
        field_map = await client.get_table_fields()
        logger.info(f"表格字段: {field_map}")
        
        # 读取数据
        logger.info("正在读取表格数据...")
        records = await client.read_records(page_size=500)
        
        if not records:
            logger.warning("表格中没有数据")
            return
        
        logger.info(f"共读取到 {len(records)} 条原始记录")
        
        # 打印第一条原始记录用于调试
        logger.info("=" * 80)
        logger.info("第一条原始记录:")
        first_raw = records[0]
        logger.info(f"记录ID: {first_raw.get('record_id', '')}")
        logger.info(f"字段数据:")
        for field_id, field_value in first_raw.get('fields', {}).items():
            field_name = field_map.get(field_id, f"未知({field_id})")
            logger.info(f"  {field_name}: {field_value} (类型: {type(field_value).__name__})")
        logger.info("=" * 80)
        
        # 提取并展开面料记录
        logger.info("正在提取面料信息...")
        all_fabric_records = []
        
        for record in records:
            fabric_records = extract_fabric_records(record, field_map)
            all_fabric_records.extend(fabric_records)
        
        logger.info(f"展开后共 {len(all_fabric_records)} 条面料记录")
        
        # 过滤：同一款号只保留创建时间最新的
        logger.info("正在过滤，同一款号只保留创建时间最新的记录...")
        filtered_records = filter_latest_by_spu(all_fabric_records)
        
        logger.info(f"过滤后共 {len(filtered_records)} 条记录")
        
        # 打印第一条记录
        if filtered_records:
            logger.info("=" * 80)
            logger.info("第一条面料记录:")
            logger.info("=" * 80)
            
            first_record = filtered_records[0]
            for key, value in first_record.items():
                if key != '创建时间戳':  # 跳过时间戳字段
                    logger.info(f"  {key}: {value}")
            
            logger.info("=" * 80)
            
            # 打印所有记录的摘要
            logger.info("\n所有面料记录摘要:")
            logger.info("-" * 80)
            logger.info(f"{'SPU':<20} {'季节':<10} {'面料':<30} {'用量':<10} {'损耗':<10} {'单价':<10}")
            logger.info("-" * 80)
            
            for record in filtered_records[:10]:  # 只打印前10条
                spu = str(record.get('SPU', ''))[:20]
                season = str(record.get('季节', ''))[:10]
                fabric = str(record.get('面料', ''))[:30]
                usage = record.get('单件用量', 0)
                loss = record.get('单件损耗', 0)
                price = record.get('单件价格', 0)
                
                logger.info(f"{spu:<20} {season:<10} {fabric:<30} {usage:<10} {loss:<10} {price:<10}")
            
            if len(filtered_records) > 10:
                logger.info(f"... 还有 {len(filtered_records) - 10} 条记录未显示")
            
            logger.info("-" * 80)
        
        return filtered_records
        
    except Exception as e:
        logger.error(f"获取数据失败: {e}", exc_info=True)
        raise


def main():
    """主函数"""
    try:
        logger.info("开始获取飞书面料数据...")
        
        # 创建表（如果不存在）
        create_fabric_table_if_not_exists()
        
        # 获取数据
        fabric_records = asyncio.run(fetch_and_print_data())
        
        # 存入数据库
        if fabric_records:
            logger.info(f"正在将 {len(fabric_records)} 条记录存入数据库...")
            inserted_count = insert_fabric_data_batch(fabric_records)
            logger.info(f"数据库操作完成，影响 {inserted_count} 条记录")
        else:
            logger.warning("没有获取到任何面料数据")
        
        logger.info("数据获取和存储完成")
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        return 1
    return 0


if __name__ == '__main__':
    exit(main())

