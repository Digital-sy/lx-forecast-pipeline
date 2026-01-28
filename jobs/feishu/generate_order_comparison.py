#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成下单对比表
按SKU+店铺+月维度汇总采购单和运营预计下单表数据，生成对比分析表

表结构说明：
- SKU：两表的并集
- 店铺：店铺名称
- 统计日期：每月第一天（DATE类型，如 2026-01-01）
- 面料：从产品信息表匹配（使用SPU，优先匹配更新时间最新的）
- 负责人：优先使用运营预计下单表的运营字段，否则用SKU+店铺去listing表匹配
- 实际下单量：从采购单表按SKU+店铺+月份汇总（需要将创建时间转换为月份）
- 预计下单量：从运营预计下单表获取

更新策略（数据库）：
- 每次运行只更新本月及未来月份的数据
- 删除本月及未来月份的旧数据，然后插入新数据
- 历史月份（本月之前）的数据保持不变

更新策略（飞书）：
- 维度：SPU + 颜色缩写 + 日期 + 店铺 + 运营（负责人）【聚合维度】
- 字段映射：SPU→SPU, 店铺→店铺, 统计日期→统计日期(日期类型), 
           负责人→运营, 颜色缩写→颜色缩写, 颜色→颜色,
           实际下单量→实际下单量, 预计下单量→预计下单量
- 计算字段：下单差额 = 预计 - 实际, 下单完成率 = 实际 / 预计（小数形式）
- 聚合逻辑：按SPU+颜色缩写+日期+店铺+负责人维度聚合，累加实际和预计下单量
- 过滤条件：只上传预计下单量 > 0 的记录
- 每次清空飞书多维表，然后写入所有聚合后的数据
"""
import sys
import asyncio
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import defaultdict

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor
from common.feishu import FeishuClient

logger = get_logger('order_comparison')

# 飞书多维表配置
FEISHU_APP_TOKEN = "B4WPbxb4UaNQ1qssnHFcxDNLn3o"  # 从URL中提取
FEISHU_TABLE_ID = "tblNMbtADEoy6Fbx"  # 从URL中提取


def get_current_month_first_day() -> str:
    """
    获取当前月份的第一天
    
    Returns:
        str: 当前月份第一天，格式：YYYY-MM-01
    """
    now = datetime.now()
    return f"{now.year}-{now.month:02d}-01"


def remove_psc_pattern(sku: str) -> str:
    """
    去除SKU中的"数字+PSC/PCS"模式（例如：4PSC, 1PCS, 10PSC等）
    去除后会清理多余的分隔符（将连续的分隔符合并为一个）
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: 去除"数字+PSC/PCS"后的SKU，并清理多余分隔符
    """
    if not sku:
        return sku
    # 匹配任意数字+PSC或PCS的模式，例如：4PSC, 1PCS, 10PSC等
    # 使用正则表达式 \d+(?:PSC|PCS) 匹配，并去除
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    # 清理多余的分隔符：将连续的分隔符合并为一个
    sku = re.sub(r'-+', '-', sku)
    # 去除首尾的分隔符
    sku = sku.strip('-')
    return sku


def extract_spu_from_sku(sku: str) -> str:
    """
    从SKU中提取SPU（第一个"-"之前的部分）
    会先去除"数字+PSC"模式（例如：4PSC）
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: SPU（如果SKU中没有"-"，则返回整个SKU）
    """
    if not sku:
        return ''
    # 先去除"数字+PSC"模式
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    if idx > 0:
        return sku[:idx]
    return sku


def extract_spu_and_color_from_sku(sku: str) -> tuple:
    """
    从SKU中提取SPU、颜色和SPU-颜色
    SKU格式：
    - 标准格式：SPU-颜色-尺码 (如 ABC-RED-XL)
    - 带长度标识：SPU-LONG-颜色-尺码 或 SPU-SHORT-颜色-尺码
    会先去除"数字+PSC"模式（例如：4PSC）
    
    Args:
        sku: SKU字符串
        
    Returns:
        tuple: (SPU, 颜色, SPU-颜色)
        例如: 
        - "ABC-RED-XL" → ("ABC", "RED", "ABC-RED")
        - "ABC-LONG-RED-XL" → ("ABC", "RED", "ABC-RED")
        - "ABC-SHORT-BLUE-M" → ("ABC", "BLUE", "ABC-BLUE")
        - "ABC-4PSC-RED-XL" → ("ABC", "RED", "ABC-RED") (去除4PSC)
    """
    if not sku:
        return ('', '', '')
    
    # 先去除"数字+PSC"模式
    sku = remove_psc_pattern(sku)
    
    parts = sku.split('-')
    
    # 如果只有1部分，整个作为SPU
    if len(parts) == 1:
        return (sku, '', sku)
    
    # 如果只有2部分：SPU-颜色
    if len(parts) == 2:
        spu = parts[0]
        color = parts[1]
        spu_color = f"{spu}-{color}"
        return (spu, color, spu_color)
    
    # 3部分或更多，需要判断是否有LONG/SHORT
    spu = parts[0]
    
    # 检查第二部分是否是LONG或SHORT
    if len(parts) >= 4 and parts[1].upper() in ['LONG', 'SHORT']:
        # 格式：SPU-LONG-颜色-尺码 或 SPU-SHORT-颜色-尺码
        color = parts[2]
        spu_color = f"{spu}-{color}"
        return (spu, color, spu_color)
    else:
        # 标准格式：SPU-颜色-尺码
        color = parts[1]
        spu_color = f"{spu}-{color}"
        return (spu, color, spu_color)


def get_actual_order_quantity() -> Dict[Tuple[str, str, str], int]:
    """
    从采购单表获取实际下单量，按SKU+店铺+月份汇总
    只获取本月及以后的数据
    
    Returns:
        Dict[Tuple[str, str, str], int]: {(SKU, 店铺, 统计日期): 实际下单量}
    """
    # 获取当前月份第一天
    current_month = get_current_month_first_day()
    logger.info(f"正在从采购单表获取实际下单量（{current_month}及以后）...")
    
    actual_orders = defaultdict(int)
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 查询采购单表
            # 创建时间格式：2025-07-31 18:23:17
            # 需要转换为月份的第一天
            # 只获取本月及以后的数据
            sql = """
            SELECT 
                SKU,
                店铺,
                实际数量,
                创建时间,
                DATE_FORMAT(创建时间, '%%Y-%%m-01') as 统计日期
            FROM 采购单
            WHERE SKU IS NOT NULL 
              AND SKU != ''
              AND 店铺 IS NOT NULL
              AND 店铺 != ''
              AND 创建时间 IS NOT NULL
              AND DATE_FORMAT(创建时间, '%%Y-%%m-01') >= %s
              AND 实际数量 IS NOT NULL
              AND 实际数量 > 0
            """
            
            cursor.execute(sql, (current_month,))
            results = cursor.fetchall()
            
            logger.info(f"  从采购单表读取到 {len(results)} 条记录")
            
            # 按 SKU+店铺+月份 汇总
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                shop = row['店铺'].strip() if row['店铺'] else ''
                stat_date = row['统计日期']
                quantity = int(row['实际数量']) if row['实际数量'] else 0
                
                if sku and shop and stat_date:
                    # 确保统计日期是字符串格式
                    if isinstance(stat_date, str):
                        stat_date_str = stat_date[:10] if len(stat_date) >= 10 else stat_date
                    elif hasattr(stat_date, 'strftime'):
                        stat_date_str = stat_date.strftime('%Y-%m-%d')
                    else:
                        stat_date_str = str(stat_date)[:10]
                    
                    key = (sku, shop, stat_date_str)
                    actual_orders[key] += quantity
            
            logger.info(f"  汇总后共 {len(actual_orders)} 个SKU+店铺+月份组合")
    except Exception as e:
        logger.error(f"从采购单表获取数据失败: {e}", exc_info=True)
    
    return dict(actual_orders)


def get_forecast_order_quantity() -> Dict[Tuple[str, str, str], Tuple[int, str]]:
    """
    从运营预计下单表获取预计下单量和运营信息
    只获取本月及以后的数据
    
    Returns:
        Dict[Tuple[str, str, str], Tuple[int, str]]: {(SKU, 店铺, 统计日期): (预计下单量, 运营)}
    """
    # 获取当前月份第一天
    current_month = get_current_month_first_day()
    logger.info(f"正在从运营预计下单表获取预计下单量（{current_month}及以后）...")
    
    forecast_orders = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '运营预计下单表'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("运营预计下单表不存在，预计下单量将为空")
                return forecast_orders
            
            # 查询运营预计下单表
            # 只获取本月及以后的数据
            sql = """
            SELECT 
                SKU,
                店铺,
                统计日期,
                预计下单量,
                运营
            FROM 运营预计下单表
            WHERE SKU IS NOT NULL 
              AND SKU != ''
              AND 店铺 IS NOT NULL
              AND 店铺 != ''
              AND 统计日期 IS NOT NULL
              AND 统计日期 >= %s
            """
            
            cursor.execute(sql, (current_month,))
            results = cursor.fetchall()
            
            logger.info(f"  从运营预计下单表读取到 {len(results)} 条记录")
            
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                shop = row['店铺'].strip() if row['店铺'] else ''
                stat_date = row['统计日期']
                quantity = int(row['预计下单量']) if row['预计下单量'] else 0
                operation = row['运营'].strip() if row.get('运营') else ''
                
                if sku and shop and stat_date:
                    # 确保统计日期是字符串格式
                    if isinstance(stat_date, str):
                        stat_date_str = stat_date[:10] if len(stat_date) >= 10 else stat_date
                    elif hasattr(stat_date, 'strftime'):
                        stat_date_str = stat_date.strftime('%Y-%m-%d')
                    else:
                        stat_date_str = str(stat_date)[:10]
                    
                    key = (sku, shop, stat_date_str)
                    # 如果已存在，累加数量，保留运营信息
                    if key in forecast_orders:
                        old_quantity, old_operation = forecast_orders[key]
                        forecast_orders[key] = (old_quantity + quantity, operation or old_operation)
                    else:
                        forecast_orders[key] = (quantity, operation)
            
            logger.info(f"  汇总后共 {len(forecast_orders)} 个SKU+店铺+月份组合")
    except Exception as e:
        logger.error(f"从运营预计下单表获取数据失败: {e}", exc_info=True)
    
    return forecast_orders


def get_fabric_mapping() -> Dict[str, str]:
    """
    从面料核价表获取面料信息（按SPU映射，一个SKU有多个面料用逗号隔开）
    
    Returns:
        Dict[str, str]: {SPU: 面料列表（逗号隔开）}
    """
    logger.info("正在从面料核价表获取面料信息...")
    
    fabric_map = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查面料核价表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '面料核价表'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("面料核价表不存在，面料信息将为空")
                return fabric_map
            
            # 查询面料核价表，按SPU分组获取所有面料
            sql = """
            SELECT 
                SPU,
                GROUP_CONCAT(DISTINCT 面料 ORDER BY 面料 SEPARATOR ',') as 面料列表
            FROM `面料核价表`
            WHERE SPU IS NOT NULL 
              AND SPU != ''
              AND 面料 IS NOT NULL
              AND 面料 != ''
            GROUP BY SPU
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从面料核价表读取到 {len(results)} 个SPU的面料信息")
            
            for row in results:
                spu = row['SPU'].strip() if row['SPU'] else ''
                fabric_list = row['面料列表'].strip() if row['面料列表'] else ''
                
                if spu and fabric_list:
                    fabric_map[spu] = fabric_list
            
            logger.info(f"  构建了 {len(fabric_map)} 个SPU的面料映射")
    except Exception as e:
        logger.error(f"从面料核价表获取面料信息失败: {e}", exc_info=True)
    
    return fabric_map


def get_color_mapping() -> Dict[str, str]:
    """
    从颜色对照获取颜色缩写到颜色中文名的映射
    优先匹配新旧='新'的记录,匹配不到再匹配'旧'的记录
    
    Returns:
        Dict[str, str]: {颜色缩写: 颜色中文名}
    """
    logger.info("正在从颜色对照获取颜色映射...")
    
    color_map = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '颜色对照'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("颜色对照不存在")
                return color_map
            
            # 先获取所有'新'的记录
            sql_new = """
            SELECT 
                颜色缩写,
                颜色中文
            FROM `颜色对照`
            WHERE 颜色缩写 IS NOT NULL 
              AND 颜色缩写 != ''
              AND 颜色中文 IS NOT NULL
              AND 颜色中文 != ''
              AND 新旧 = '新'
            """
            
            cursor.execute(sql_new)
            results_new = cursor.fetchall()
            
            logger.info(f"  从颜色对照读取到 {len(results_new)} 条'新'记录")
            
            for row in results_new:
                abbr = row['颜色缩写'].strip() if row['颜色缩写'] else ''
                color = row['颜色中文'].strip() if row['颜色中文'] else ''
                if abbr and color:
                    color_map[abbr] = color
            
            # 再获取所有'旧'的记录(不覆盖已有的)
            sql_old = """
            SELECT 
                颜色缩写,
                颜色中文
            FROM `颜色对照`
            WHERE 颜色缩写 IS NOT NULL 
              AND 颜色缩写 != ''
              AND 颜色中文 IS NOT NULL
              AND 颜色中文 != ''
              AND 新旧 = '旧'
            """
            
            cursor.execute(sql_old)
            results_old = cursor.fetchall()
            
            logger.info(f"  从颜色对照读取到 {len(results_old)} 条'旧'记录")
            
            for row in results_old:
                abbr = row['颜色缩写'].strip() if row['颜色缩写'] else ''
                color = row['颜色中文'].strip() if row['颜色中文'] else ''
                if abbr and color and abbr not in color_map:
                    # 只有当颜色缩写不存在时才添加(优先使用'新'的)
                    color_map[abbr] = color
            
            logger.info(f"  构建了 {len(color_map)} 个颜色缩写的映射")
    except Exception as e:
        logger.error(f"从颜色对照获取数据失败: {e}", exc_info=True)
    
    return color_map


def get_responsible_from_listing() -> Dict[Tuple[str, str], str]:
    """
    从listing表获取负责人信息（使用SKU+店铺匹配）
    
    Returns:
        Dict[Tuple[str, str], str]: {(SKU, 店铺): 负责人}
    """
    logger.info("正在从listing表获取负责人信息...")
    
    responsible_map = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在以及是否有负责人字段
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'listing'
                AND COLUMN_NAME = '负责人'
            """)
            result = cursor.fetchone()
            has_responsible_field = (result.get('cnt', 0) if result else 0) > 0
            
            if not has_responsible_field:
                logger.warning("listing表中没有负责人字段")
                return responsible_map
            
            # 查询listing表
            sql = """
            SELECT 
                SKU,
                店铺,
                负责人
            FROM listing
            WHERE SKU IS NOT NULL 
              AND SKU != '' 
              AND SKU != '无'
              AND 店铺 IS NOT NULL
              AND 店铺 != ''
              AND 店铺 != '无'
              AND 负责人 IS NOT NULL
              AND 负责人 != ''
              AND 负责人 != '无'
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从listing表读取到 {len(results)} 条记录")
            
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                shop = row['店铺'].strip() if row['店铺'] else ''
                responsible = row['负责人'].strip() if row['负责人'] else ''
                
                if sku and shop and responsible:
                    key = (sku, shop)
                    # 如果同一个SKU+店铺有多个负责人，保留第一个
                    if key not in responsible_map:
                        responsible_map[key] = responsible
            
            logger.info(f"  构建了 {len(responsible_map)} 个SKU+店铺的负责人映射")
    except Exception as e:
        logger.error(f"从listing表获取负责人信息失败: {e}", exc_info=True)
    
    return responsible_map


def merge_order_data(
    actual_orders: Dict[Tuple[str, str, str], int],
    forecast_orders: Dict[Tuple[str, str, str], Tuple[int, str]],
    fabric_map: Dict[str, str],
    responsible_map: Dict[Tuple[str, str], str],
    color_map: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    合并实际下单和预计下单数据
    
    Args:
        actual_orders: 实际下单量字典
        forecast_orders: 预计下单量和运营字典
        fabric_map: 面料映射字典 {SPU: 面料列表（逗号隔开）}
        responsible_map: 负责人映射字典
        color_map: 颜色缩写到颜色中文名的映射
        
    Returns:
        List[Dict[str, Any]]: 合并后的数据列表
    """
    logger.info("正在合并数据...")
    
    # 获取所有的SKU+店铺+统计日期组合（两表的并集）
    all_keys = set(actual_orders.keys()) | set(forecast_orders.keys())
    
    logger.info(f"  共有 {len(all_keys)} 个唯一的SKU+店铺+月份组合")
    logger.info(f"    仅在采购单表: {len(set(actual_orders.keys()) - set(forecast_orders.keys()))} 个")
    logger.info(f"    仅在预计下单表: {len(set(forecast_orders.keys()) - set(actual_orders.keys()))} 个")
    logger.info(f"    两表都有: {len(set(actual_orders.keys()) & set(forecast_orders.keys()))} 个")
    
    merged_data = []
    
    for key in all_keys:
        sku, shop, stat_date = key
        
        # 从SKU中提取SPU、颜色、SPU-颜色
        spu, color_abbr, spu_color = extract_spu_and_color_from_sku(sku)
        
        # 从颜色对照获取颜色中文名
        color_name = color_map.get(color_abbr, '')
        
        # 获取实际下单量
        actual_quantity = actual_orders.get(key, 0)
        
        # 获取预计下单量和运营
        forecast_data = forecast_orders.get(key, (0, ''))
        forecast_quantity = forecast_data[0]
        operation = forecast_data[1]
        
        # 获取面料（使用SPU匹配，多个面料用逗号隔开）
        fabric = ''
        if spu in fabric_map:
            fabric = fabric_map[spu]
        
        # 获取负责人
        # 优先使用运营预计下单表的运营字段
        # 如果为空，则用SKU+店铺去listing表匹配
        responsible = operation if operation else responsible_map.get((sku, shop), '')
        
        # 计算是否有预估：预计下单量>0为"有预估"，否则为"无预估"
        has_forecast = "有预估" if forecast_quantity > 0 else "无预估"
        
        # 构建数据记录
        record = {
            'SKU': sku,
            'SPU': spu,
            'SPU-颜色': spu_color,
            '颜色缩写': color_abbr,
            '颜色': color_name,
            '店铺': shop,
            '统计日期': stat_date,
            '面料': fabric,
            '负责人': responsible,
            '实际下单量': actual_quantity,
            '预计下单量': forecast_quantity,
            '是否有预估': has_forecast
        }
        
        merged_data.append(record)
    
    logger.info(f"  合并完成，共生成 {len(merged_data)} 条记录")
    
    return merged_data


def create_comparison_table_if_not_exists() -> None:
    """创建下单对比表（如果不存在）"""
    logger.info("正在创建或检查下单对比表...")
    
    try:
        with db_cursor(dictionary=False) as cursor:
            # 创建表
            sql = """
            CREATE TABLE IF NOT EXISTS `下单对比表` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `SKU` VARCHAR(500),
                `SPU` VARCHAR(500) COMMENT 'SKU第一个-之前的部分',
                `SPU-颜色` VARCHAR(500) COMMENT 'SKU前两个-之前的部分',
                `颜色缩写` VARCHAR(100) COMMENT 'SKU中的颜色缩写',
                `颜色` VARCHAR(100) COMMENT '颜色中文名,从颜色对照匹配',
                `店铺` VARCHAR(500),
                `统计日期` DATE COMMENT '月份的第一天',
                `面料` VARCHAR(500),
                `负责人` VARCHAR(500),
                `实际下单量` INT DEFAULT 0,
                `预计下单量` INT DEFAULT 0,
                `是否有预估` VARCHAR(20) COMMENT '预计下单量>0为有预估，否则为无预估',
                `更新时间` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_sku (SKU(100)),
                INDEX idx_spu (SPU(100)),
                INDEX idx_spu_color (`SPU-颜色`(100)),
                INDEX idx_color_abbr (颜色缩写),
                INDEX idx_shop (店铺(100)),
                INDEX idx_date (统计日期),
                INDEX idx_sku_shop_date (SKU(100), 店铺(100), 统计日期),
                UNIQUE KEY uk_sku_shop_date (SKU(100), 店铺(100), 统计日期)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='下单对比表-按月汇总'
            """
            
            cursor.execute(sql)
            logger.info("  表检查/创建完成")
            
            # 检查并添加新字段（如果表已存在但缺少新字段）
            try:
                # 检查SPU字段
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单对比表' 
                    AND COLUMN_NAME = 'SPU'
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE `下单对比表` ADD COLUMN `SPU` VARCHAR(500) COMMENT 'SKU第一个-之前的部分' AFTER `SKU`")
                    logger.info("  已添加字段: SPU")
                
                # 检查SPU-颜色字段
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单对比表' 
                    AND COLUMN_NAME = 'SPU-颜色'
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE `下单对比表` ADD COLUMN `SPU-颜色` VARCHAR(500) COMMENT 'SKU前两个-之前的部分' AFTER `SPU`")
                    logger.info("  已添加字段: SPU-颜色")
                
                # 检查颜色缩写字段
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单对比表' 
                    AND COLUMN_NAME = '颜色缩写'
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE `下单对比表` ADD COLUMN `颜色缩写` VARCHAR(100) COMMENT 'SKU中的颜色缩写' AFTER `SPU-颜色`")
                    logger.info("  已添加字段: 颜色缩写")
                
                # 检查颜色字段(修改为新的定义)
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单对比表' 
                    AND COLUMN_NAME = '颜色'
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE `下单对比表` ADD COLUMN `颜色` VARCHAR(100) COMMENT '颜色中文名,从颜色对照匹配' AFTER `颜色缩写`")
                    logger.info("  已添加字段: 颜色")
                
                # 检查是否有预估字段
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单对比表' 
                    AND COLUMN_NAME = '是否有预估'
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute("ALTER TABLE `下单对比表` ADD COLUMN `是否有预估` VARCHAR(20) COMMENT '预计下单量>0为有预估，否则为无预估' AFTER `预计下单量`")
                    logger.info("  已添加字段: 是否有预估")
                
                # 删除面料-颜色字段(如果存在)
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单对比表' 
                    AND COLUMN_NAME = '面料-颜色'
                """)
                if cursor.fetchone()[0] > 0:
                    cursor.execute("ALTER TABLE `下单对比表` DROP COLUMN `面料-颜色`")
                    logger.info("  已删除字段: 面料-颜色")
            except Exception as e:
                logger.warning(f"  检查/添加新字段时出错: {e}")
                
    except Exception as e:
        logger.error(f"创建下单对比表失败: {e}", exc_info=True)
        raise


def delete_current_and_future_data() -> int:
    """
    删除本月及未来月份的数据（即将要更新的数据）
    保留历史月份的数据不变
    
    Returns:
        int: 删除的记录数
    """
    current_month = get_current_month_first_day()
    logger.info(f"正在删除 {current_month} 及以后的数据（准备更新）...")
    
    try:
        with db_cursor(dictionary=False) as cursor:
            sql = """
            DELETE FROM `下单对比表`
            WHERE 统计日期 >= %s
            """
            cursor.execute(sql, (current_month,))
            deleted_count = cursor.rowcount
            logger.info(f"  已删除 {deleted_count} 条数据（本月及未来月份）")
            return deleted_count
    except Exception as e:
        logger.error(f"删除数据失败: {e}", exc_info=True)
        return 0


def insert_data_batch(data_list: List[Dict[str, Any]]) -> None:
    """
    批量插入数据到数据库
    
    Args:
        data_list: 数据列表
    """
    if not data_list:
        logger.warning("没有数据需要插入")
        return
    
    logger.info(f"正在写入 {len(data_list)} 条数据到数据库...")
    
    try:
        with db_cursor(dictionary=False) as cursor:
            sql = """
            INSERT INTO `下单对比表` (SKU, SPU, `SPU-颜色`, 颜色缩写, 颜色, 店铺, 统计日期, 面料, 负责人, 实际下单量, 预计下单量, 是否有预估)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                SPU = VALUES(SPU),
                `SPU-颜色` = VALUES(`SPU-颜色`),
                颜色缩写 = VALUES(颜色缩写),
                颜色 = VALUES(颜色),
                面料 = VALUES(面料),
                负责人 = VALUES(负责人),
                实际下单量 = VALUES(实际下单量),
                预计下单量 = VALUES(预计下单量),
                是否有预估 = VALUES(是否有预估),
                更新时间 = CURRENT_TIMESTAMP
            """
            
            batch_size = 200
            total_inserted = 0
            
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                values = [
                    (
                        row.get('SKU', ''),
                        row.get('SPU', ''),
                        row.get('SPU-颜色', ''),
                        row.get('颜色缩写', ''),
                        row.get('颜色', ''),
                        row.get('店铺', ''),
                        row.get('统计日期', ''),
                        row.get('面料', ''),
                        row.get('负责人', ''),
                        row.get('实际下单量', 0),
                        row.get('预计下单量', 0),
                        row.get('是否有预估', '无预估')
                    )
                    for row in batch
                ]
                cursor.executemany(sql, values)
                total_inserted += len(batch)
                logger.info(f"  已插入 {total_inserted}/{len(data_list)} 条数据...")
        
        logger.info(f"✓ 成功写入 {len(data_list)} 条数据到下单对比表")
    except Exception as e:
        logger.error(f"写入数据库失败: {e}", exc_info=True)
        raise


def prepare_feishu_data(merged_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    准备写入飞书的数据（按SPU+颜色缩写+日期+店铺+负责人维度聚合）
    添加计算字段：下单差额、下单完成率（小数形式）
    只保留预计下单量 > 0 的记录
    
    Args:
        merged_data: 按SKU+店铺+日期维度的数据列表
        
    Returns:
        List[Dict[str, Any]]: 聚合后准备写入飞书的数据列表（只包含预计下单量 > 0 的记录）
    """
    logger.info("正在按SPU+颜色缩写+日期+店铺+负责人维度聚合数据...")
    
    # 使用字典进行聚合：{(SPU, 颜色缩写, 统计日期, 店铺, 负责人): 聚合数据}
    aggregated = defaultdict(lambda: {
        'SPU': '',
        '颜色缩写': '',
        '统计日期': '',
        '店铺': '',
        '负责人': '',
        '颜色': '',
        '实际下单量': 0,
        '预计下单量': 0
    })
    
    for record in merged_data:
        spu = record['SPU']
        color_abbr = record['颜色缩写']
        stat_date = record['统计日期']
        shop = record['店铺']
        responsible = record['负责人']
        
        # 聚合键：SPU + 颜色缩写 + 日期 + 店铺 + 负责人
        key = (spu, color_abbr, stat_date, shop, responsible)
        
        agg_data = aggregated[key]
        
        # 设置基本字段（第一次遇到时）
        if not agg_data['SPU']:
            agg_data['SPU'] = spu
            agg_data['颜色缩写'] = color_abbr
            agg_data['统计日期'] = stat_date
            agg_data['店铺'] = shop
            agg_data['负责人'] = responsible
            agg_data['颜色'] = record['颜色']
        
        # 累加数量
        agg_data['实际下单量'] += record['实际下单量']
        agg_data['预计下单量'] += record['预计下单量']
    
    # 转换为列表格式，并计算下单差额和下单完成率
    result = []
    filtered_count = 0  # 记录过滤掉的记录数
    for key, agg_data in aggregated.items():
        actual = agg_data['实际下单量']
        forecast = agg_data['预计下单量']
        
        # 只保留预计下单量 > 0 的记录
        if forecast <= 0:
            filtered_count += 1
            continue
        
        # 计算下单差额 = 预计 - 实际
        order_diff = forecast - actual
        
        # 计算下单完成率 = 实际 / 预计（小数形式）
        completion_rate = round(actual / forecast, 4)  # 小数形式，保留4位小数
        
        feishu_record = {
            'SPU': agg_data['SPU'],
            '颜色缩写': agg_data['颜色缩写'],
            '统计日期': agg_data['统计日期'],
            '店铺': agg_data['店铺'],
            '负责人': agg_data['负责人'],
            '颜色': agg_data['颜色'],
            '实际下单量': actual,
            '预计下单量': forecast,
            '下单差额': order_diff,
            '下单完成率': completion_rate
        }
        result.append(feishu_record)
    
    logger.info(f"  聚合完成，从 {len(merged_data)} 条记录聚合为 {len(aggregated)} 条记录")
    logger.info(f"  过滤掉预计下单量为0的记录: {filtered_count} 条")
    logger.info(f"  最终上传到飞书: {len(result)} 条记录（预计下单量 > 0）")
    logger.info(f"  维度：SPU + 颜色缩写 + 日期 + 店铺 + 负责人")
    
    return result


async def write_to_feishu(feishu_data: List[Dict[str, Any]]) -> bool:
    """
    将数据写入飞书多维表
    
    Args:
        feishu_data: 准备写入飞书的数据列表
        
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
            {'name': 'SPU', 'type': 'text'},
            {'name': '颜色缩写', 'type': 'text'},
            {'name': '统计日期', 'type': 'date'},  # 日期类型字段
            {'name': '店铺', 'type': 'text'},
            {'name': '运营', 'type': 'text'},  # 负责人映射到运营字段
            {'name': '颜色', 'type': 'text'},
            {'name': '实际下单量', 'type': 'number'},
            {'name': '预计下单量', 'type': 'number'},
            {'name': '下单差额', 'type': 'number'},
            {'name': '下单完成率', 'type': 'number', 'precision': 4},  # 小数形式，保留4位小数
        ]
        
        # 确保字段存在
        logger.info(f"正在确保字段存在...")
        existing_fields = await feishu_client.get_table_fields()
        existing_field_names = set(existing_fields.values())
        
        for field_info in field_list:
            field_name = field_info['name']
            if field_name not in existing_field_names:
                logger.info(f"  字段 '{field_name}' 不存在，正在创建...")
                try:
                    if field_info['type'] == 'date':
                        # 日期类型：5=日期，1004=日期（新版本）
                        field_type = "5"
                    elif field_info['type'] == 'number':
                        field_type = "2"
                    else:
                        field_type = "1"  # 文本类型
                    
                    precision = field_info.get('precision', 0)
                    await feishu_client.create_field(field_name, field_type, precision)
                except Exception as e:
                    logger.warning(f"  创建字段 '{field_name}' 失败: {e}")
            else:
                logger.debug(f"  字段 '{field_name}' 已存在")
        
        # 准备写入的记录（转换日期格式）
        logger.info(f"正在准备飞书记录...")
        records = []
        for row in feishu_data:
            # 将日期字符串转换为飞书日期格式（毫秒时间戳）
            stat_date_str = row.get('统计日期', '')
            stat_date_timestamp = None
            if stat_date_str:
                try:
                    from datetime import datetime
                    # 解析日期字符串 "YYYY-MM-DD"
                    dt = datetime.strptime(stat_date_str, '%Y-%m-%d')
                    # 转换为毫秒时间戳（飞书日期字段需要）
                    stat_date_timestamp = int(dt.timestamp() * 1000)
                except Exception as e:
                    logger.warning(f"日期转换失败: {stat_date_str}, 错误: {e}")
                    stat_date_timestamp = None
            
            record = {
                'SPU': row.get('SPU', ''),
                '颜色缩写': row.get('颜色缩写', ''),
                '统计日期': stat_date_timestamp,  # 日期类型，使用毫秒时间戳
                '店铺': row.get('店铺', ''),
                '运营': row.get('负责人', ''),  # 负责人映射到运营字段
                '颜色': row.get('颜色', ''),
                '实际下单量': row.get('实际下单量', 0),
                '预计下单量': row.get('预计下单量', 0),
                '下单差额': row.get('下单差额', 0),
                '下单完成率': row.get('下单完成率', 0),
            }
            records.append(record)
        
        logger.info(f"共准备 {len(records)} 条飞书记录")
        logger.info(f"维度：SPU + 颜色缩写 + 日期 + 店铺 + 运营（负责人）")
        
        # 清空旧数据
        logger.info(f"正在清空飞书多维表的旧数据...")
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
            return False
        
        # 写入数据
        logger.info(f"正在写入数据到飞书多维表...")
        written_count = await feishu_client.write_records(records, batch_size=500)
        logger.info(f"✓ 成功写入 {written_count} 条记录到飞书多维表")
        
        return True
        
    except Exception as e:
        logger.error(f"写入飞书多维表失败: {e}", exc_info=True)
        return False


async def main_async():
    """异步主函数"""
    logger.info("="*80)
    logger.info("生成下单对比表任务")
    logger.info("="*80)
    
    try:
        # 1. 获取实际下单量（从采购单表）
        actual_orders = get_actual_order_quantity()
        
        # 2. 获取预计下单量（从运营预计下单表）
        forecast_orders = get_forecast_order_quantity()
        
        # 3. 获取面料信息（从产品信息表）
        fabric_map = get_fabric_mapping()
        
        # 4. 获取负责人信息（从listing表）
        responsible_map = get_responsible_from_listing()
        
        # 5. 获取颜色映射（从颜色对照）
        color_map = get_color_mapping()
        
        # 6. 合并数据
        merged_data = merge_order_data(
            actual_orders,
            forecast_orders,
            fabric_map,
            responsible_map,
            color_map
        )
        
        if not merged_data:
            logger.warning("没有数据需要写入")
            return
        
        # 7. 创建表（如果不存在）
        create_comparison_table_if_not_exists()
        
        # 8. 删除本月及未来月份的数据（准备更新）
        deleted_count = delete_current_and_future_data()
        
        # 9. 写入数据库
        insert_data_batch(merged_data)
        
        # 10. 准备飞书数据（添加计算字段）
        feishu_data = prepare_feishu_data(merged_data)
        
        # 11. 写入飞书多维表
        logger.info("\n正在写入飞书多维表...")
        feishu_success = await write_to_feishu(feishu_data)
        
        # 12. 输出统计信息
        current_month = get_current_month_first_day()
        logger.info("\n" + "="*80)
        logger.info("处理完成！")
        logger.info("="*80)
        logger.info(f"数据范围: {current_month} 及以后")
        logger.info(f"删除数据: {deleted_count} 条（本月及未来月份）")
        logger.info(f"历史数据: 保持不变（{current_month} 之前的月份）")
        logger.info(f"数据库记录数: {len(merged_data)}")
        logger.info(f"  有实际下单: {sum(1 for d in merged_data if d['实际下单量'] > 0)} 条")
        logger.info(f"  有预计下单: {sum(1 for d in merged_data if d['预计下单量'] > 0)} 条")
        logger.info(f"  有面料信息: {sum(1 for d in merged_data if d['面料'])} 条")
        logger.info(f"  有负责人信息: {sum(1 for d in merged_data if d['负责人'])} 条")
        logger.info(f"飞书多维表记录数: {len(feishu_data)}")
        logger.info(f"  写入状态: {'成功' if feishu_success else '失败'}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        raise


def main():
    """主函数"""
    asyncio.run(main_async())


if __name__ == '__main__':
    main()

