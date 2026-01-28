#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
从飞书多维表格读取面料参数数据并存入数据库
表名：定制面料参数
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import settings, get_logger
from common.feishu import FeishuClient
from common.database import db_cursor

logger = get_logger('feishu_fabric_params')

# 飞书多维表格配置
# 从 URL 中提取: .../XT6pbXxmdas4rdsme0XctyefnGu/table=tbldT91ivWPJYTob&view=vewpBJmIfm
FEISHU_APP_TOKEN = "XT6pbXxmdas4rdsme0XctyefnGu"  # 多维表格ID (bitable)
FEISHU_TABLE_ID = "tbldT91ivWPJYTob"              # 数据表ID
FEISHU_VIEW_ID = None  # 视图ID（暂不使用，避免权限问题）


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


def extract_fabric_params(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    从飞书记录中提取面料参数信息
    
    Args:
        records: 飞书记录列表
        
    Returns:
        List[Dict[str, Any]]: 提取后的面料参数列表
    """
    logger.info("正在提取面料参数信息...")
    
    fabric_params_list = []
    
    for record in records:
        fields = record.get('fields', {})
        
        # 提取实际字段
        # 布种、面料编号、公斤数/条、米数/条、出米数/公斤
        fabric_param = {
            '面料': extract_text_value(fields.get('布种', '')),  # 布种即面料
            '面料编号': extract_text_value(fields.get('面料编号', '')),
            '公斤数每条': extract_numeric_value(fields.get(' 公斤数/条', 0)),  # 注意字段名前有空格
            '米数每条': extract_numeric_value(fields.get(' 米数/条', 0)),
            '出米数每公斤': extract_numeric_value(fields.get('出米数/公斤', 0)),
            '创建时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 只添加有面料的记录
        if fabric_param['面料']:
            fabric_params_list.append(fabric_param)
    
    logger.info(f"提取了 {len(fabric_params_list)} 条面料参数记录")
    return fabric_params_list


def create_fabric_params_table_if_not_exists() -> None:
    """创建定制面料参数表（如果不存在）"""
    logger.info("正在检查/创建定制面料参数表...")
    
    try:
        with db_cursor(dictionary=False) as cursor:
            # 先删除旧表（如果存在且结构不对）
            cursor.execute("DROP TABLE IF EXISTS `定制面料参数`")
            
            # 创建表
            sql = """
            CREATE TABLE IF NOT EXISTS `定制面料参数` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `面料` VARCHAR(500) COMMENT '面料名称（布种）',
                `面料编号` VARCHAR(255) COMMENT '面料编号',
                `公斤数每条` DOUBLE COMMENT '每条布的公斤数',
                `米数每条` DOUBLE COMMENT '每条布的米数',
                `出米数每公斤` DOUBLE COMMENT '每公斤出多少米',
                `创建时间` DATETIME,
                `数据更新时间` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_fabric (`面料`(100)),
                INDEX idx_fabric_code (`面料编号`),
                UNIQUE KEY uk_fabric (`面料`(200))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='飞书定制面料参数数据'
            """
            
            cursor.execute(sql)
            logger.info("  表创建完成")
            
    except Exception as e:
        logger.error(f"创建定制面料参数表失败: {e}", exc_info=True)
        raise


def insert_fabric_params_batch(data_list: List[Dict[str, Any]]) -> int:
    """
    批量插入面料参数数据到数据库
    使用 INSERT ... ON DUPLICATE KEY UPDATE 实现更新插入
    
    Args:
        data_list: 面料参数数据列表
        
    Returns:
        int: 插入/更新的记录数
    """
    if not data_list:
        logger.warning("没有数据需要插入")
        return 0
    
    try:
        with db_cursor(dictionary=False) as cursor:
            sql = """
            INSERT INTO `定制面料参数` 
            (`面料`, `面料编号`, `公斤数每条`, `米数每条`, `出米数每公斤`, `创建时间`)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `面料编号` = VALUES(`面料编号`),
                `公斤数每条` = VALUES(`公斤数每条`),
                `米数每条` = VALUES(`米数每条`),
                `出米数每公斤` = VALUES(`出米数每公斤`),
                `创建时间` = VALUES(`创建时间`)
            """
            
            # 准备批量插入的数据
            batch_data = []
            for record in data_list:
                batch_data.append((
                    record.get('面料', ''),
                    record.get('面料编号', ''),
                    record.get('公斤数每条', 0),
                    record.get('米数每条', 0),
                    record.get('出米数每公斤', 0),
                    record.get('创建时间', '1970-01-01 00:00:00')
                ))
            
            # 执行批量插入
            cursor.executemany(sql, batch_data)
            affected_rows = cursor.rowcount
            
            logger.info(f"成功插入/更新 {affected_rows} 条面料参数记录")
            return affected_rows
            
    except Exception as e:
        logger.error(f"插入面料参数数据失败: {e}", exc_info=True)
        raise


async def fetch_and_process_data():
    """
    从飞书多维表格获取数据并处理
    
    Returns:
        List[Dict[str, Any]]: 处理后的数据列表
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
        logger.info(f"获取到 {len(field_map)} 个字段")
        logger.info(f"字段列表: {list(field_map.values())}")
        
        # 读取数据
        logger.info("正在读取表格数据...")
        records = await client.read_records(page_size=500)
        
        if not records:
            logger.warning("表格中没有数据")
            return []
        
        logger.info(f"共读取到 {len(records)} 条原始记录")
        
        # 打印第一条记录的字段信息（用于调试）
        if records:
            logger.info("=" * 80)
            logger.info("第一条记录的字段:")
            first_record = records[0]
            for field_name, field_value in first_record.get('fields', {}).items():
                value_preview = str(field_value)[:100] if field_value else 'None'
                logger.info(f"  {field_name}: {value_preview}")
            logger.info("=" * 80)
        
        # 提取面料参数
        fabric_params = extract_fabric_params(records)
        
        return fabric_params
        
    except Exception as e:
        logger.error(f"获取数据失败: {e}", exc_info=True)
        raise


def main():
    """主函数"""
    try:
        logger.info("开始获取飞书面料参数数据...")
        
        # 创建表（如果不存在）
        create_fabric_params_table_if_not_exists()
        
        # 获取数据
        fabric_params = asyncio.run(fetch_and_process_data())
        
        # 存入数据库
        if fabric_params:
            logger.info(f"正在将 {len(fabric_params)} 条记录存入数据库...")
            inserted_count = insert_fabric_params_batch(fabric_params)
            logger.info(f"数据库操作完成，影响 {inserted_count} 条记录")
            
            # 打印前5条数据样例
            logger.info("\n前5条数据样例:")
            logger.info("-" * 80)
            for i, param in enumerate(fabric_params[:5], 1):
                logger.info(f"\n记录 {i}:")
                for key, value in param.items():
                    if key != '创建时间':  # 跳过创建时间字段
                        logger.info(f"  {key}: {value}")
            logger.info("-" * 80)
        else:
            logger.warning("没有获取到任何面料参数数据")
        
        logger.info("数据获取和存储完成")
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        return 1
    return 0


if __name__ == '__main__':
    exit(main())

