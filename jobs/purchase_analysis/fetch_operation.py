#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
运营下单数据采集任务
从飞书多维表格读取数据并存入数据库
上传后清空飞书表格
"""
import asyncio
from typing import List, Dict, Any

from common import settings, get_logger
from common.database import db_cursor
from common.feishu import FeishuClient
from utils.data_transform import convert_feishu_record_to_dict

logger = get_logger('operation_order')

# 飞书表格配置（运营下单表）
FEISHU_APP_TOKEN = "UYcGbtgw7a6gTfsEGiEcV1Gxnke"
FEISHU_TABLE_ID = "tbl8nvA0EMOVYbqR"
FEISHU_VIEW_ID = "vewhLYH65h"


def table_structure_matches(table_name: str, sample_row: Dict[str, Any], time_fields: List[str] = None) -> bool:
    """检查表结构是否匹配"""
    if time_fields is None:
        time_fields = []
    
    with db_cursor(dictionary=False) as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()
        if not exists:
            return False
        
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        columns = [row[0] for row in cursor.fetchall()]
        expected = ['id'] + list(sample_row.keys())
        return columns == expected


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any], time_fields: List[str] = None, numeric_fields: List[str] = None) -> None:
    """创建或重建数据表"""
    if time_fields is None:
        time_fields = []
    if numeric_fields is None:
        numeric_fields = []
    
    with db_cursor(dictionary=False) as cursor:
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()
        
        if exists:
            # 检查表结构是否匹配
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
            expected = ['id'] + list(sample_row.keys())
            
            if columns == expected:
                logger.info(f"表 {table_name} 结构正确")
                return
            else:
                logger.warning(f"表 {table_name} 结构不符，正在重建...")
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # 创建表
        fields = []
        for k, v in sample_row.items():
            # 时间字段使用 DATETIME 类型
            if k in time_fields:
                fields.append(f"`{k}` DATETIME")
            # 数字字段使用 INT 类型（优先检查字段名，再检查值类型）
            elif k in numeric_fields:
                fields.append(f"`{k}` INT")
            elif isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql})"
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def insert_data_batch(table_name: str, data_list: List[Dict[str, Any]], time_fields: List[str] = None, numeric_fields: List[str] = None) -> None:
    """批量插入数据"""
    if time_fields is None:
        time_fields = []
    if numeric_fields is None:
        numeric_fields = []
    
    if not data_list:
        logger.warning("没有数据需要插入")
        return
    
    # 预处理数据：处理时间字段和数字字段
    processed_data = []
    for row in data_list:
        processed_row = {}
        for k, v in row.items():
            # 时间字段：空字符串转换为 None（MySQL DATETIME 不接受空字符串）
            if k in time_fields:
                if v == '' or v is None:
                    processed_row[k] = None
                else:
                    processed_row[k] = v
            # 数字字段：转换为数字类型
            elif k in numeric_fields:
                if v == '' or v is None:
                    processed_row[k] = 0
                elif isinstance(v, (int, float)):
                    # 如果是浮点数，转换为整数（向下取整）
                    processed_row[k] = int(v)
                elif isinstance(v, str):
                    try:
                        # 尝试转换为整数（先转float再转int，以处理小数）
                        processed_row[k] = int(float(v))
                    except (ValueError, TypeError):
                        processed_row[k] = 0
                else:
                    processed_row[k] = 0
            # 其他数字字段：确保类型正确
            elif isinstance(v, (int, float)):
                processed_row[k] = v
            elif isinstance(v, str) and v.strip() == '':
                # 空字符串保持为空字符串（对于非时间、非数字字段）
                processed_row[k] = v
            else:
                processed_row[k] = v
        processed_data.append(processed_row)
    
    with db_cursor(dictionary=False) as cursor:
        keys = processed_data[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `{table_name}` ({fields}) VALUES ({values_placeholder})"
        
        batch_size = 200
        for i in range(0, len(processed_data), batch_size):
            batch = [tuple(row.values()) for row in processed_data[i:i+batch_size]]
            cursor.executemany(sql, batch)
            logger.info(f"已录入 {min(i+batch_size, len(processed_data))} 条...")
    
    logger.info(f"成功写入 {len(processed_data)} 条数据到表 {table_name}")


async def main():
    """主函数"""
    logger.info("="*80)
    logger.info("从飞书收集表读取数据并上传到数据库")
    logger.info("="*80)
    
    # 创建飞书客户端
    try:
        logger.info("\n1. 正在连接飞书...")
        feishu_client = FeishuClient(
            app_token=FEISHU_APP_TOKEN,
            table_id=FEISHU_TABLE_ID,
            view_id=FEISHU_VIEW_ID
        )
        logger.info("   飞书客户端创建成功")
    except Exception as e:
        logger.error(f"   创建飞书客户端失败: {e}")
        return
    
    # 获取表格字段信息
    try:
        logger.info("\n2. 正在获取表格字段信息...")
        field_map = await feishu_client.get_table_fields()
        logger.info(f"   获取到 {len(field_map)} 个字段")
    except Exception as e:
        logger.error(f"   获取字段信息失败: {e}")
        return
    
    # 读取飞书多维表格数据
    try:
        logger.info("\n3. 正在从飞书多维表格读取数据...")
        records = await feishu_client.read_records()
        logger.info(f"   读取到 {len(records)} 条记录")
        
        if len(records) == 0:
            logger.warning("   ⚠️ 飞书表格中没有数据")
            return
    except Exception as e:
        logger.error(f"   读取表格数据失败: {e}", exc_info=True)
        return
    
    # 转换为字典列表
    logger.info("\n4. 正在转换数据格式...")
    
    # 定义需要转换时间戳的字段
    time_fields = ['下单时间', '创建时间', '更新时间', '订单时间', '修改时间']
    # 定义需要转换为数字的字段
    numeric_fields = ['下单数量']
    
    # 转换数据
    data_list = convert_feishu_record_to_dict(records, field_map, time_fields)
    logger.info(f"   转换后共 {len(data_list)} 条数据")
    
    if not data_list:
        logger.warning("没有数据需要保存")
        return
    
    # 显示第一条数据示例
    if data_list:
        logger.info(f"\n   【数据示例】第一条数据的字段：")
        first_data = data_list[0]
        for key, value in list(first_data.items())[:5]:  # 只显示前5个字段
            logger.info(f"     {key}: {type(value).__name__} = {repr(value)[:50]}")
    
    # 处理数据库表
    table_name = '运营下单表'
    logger.info(f"\n5. 正在处理数据库表 {table_name}...")
    
    try:
        # 使用第一条数据作为样本来判断表结构
        sample_row = data_list[0]
        
        # 创建或检查表结构，传入时间字段列表和数字字段列表
        create_table_if_needed(table_name, sample_row, time_fields, numeric_fields)
        
        # 插入所有数据到数据库
        logger.info(f"\n6. 正在插入所有数据到数据库...")
        insert_data_batch(table_name, data_list, time_fields, numeric_fields)
        
        # 清空飞书表格
        logger.info(f"\n7. 正在清空飞书表格...")
        try:
            deleted_count = await feishu_client.delete_all_records()
            logger.info(f"   飞书表格已清空（删除 {deleted_count} 条记录）")
        except Exception as e:
            logger.error(f"   清空飞书表格失败: {e}")
            logger.warning("   注意：数据已写入数据库，但飞书表格未清空，请手动处理")
        
        # 显示数据示例
        logger.info(f"\n8. 数据示例（前3条）：")
        for idx, row in enumerate(data_list[:3], 1):
            logger.info(f"\n   记录 {idx}:")
            for key, value in list(row.items())[:5]:  # 只显示前5个字段
                logger.info(f"     {key}: {value}")
        
        logger.info("\n" + "="*80)
        logger.info("处理完成！")
        logger.info(f"  共处理 {len(data_list)} 条数据")
        logger.info(f"  已写入数据库表: {table_name}")
        logger.info(f"  飞书表格已清空")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"数据库操作失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    asyncio.run(main())

