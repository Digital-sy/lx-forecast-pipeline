#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成面料预估表
根据运营预计下单表和面料核价表,计算面料用量预估

表结构说明:
- 面料: 从定制面料参数表获取
- 面料编号: SKU去掉最后一个-之后的字符
- 颜色缩写: SKU的第一个-和第二个-之间的字符(去掉LONG/SHORT)
- 面料颜色编号: 面料编号-颜色缩写
- 预计下单件数: 从运营预计下单表获取
- 预计用量: 预计下单量 * (单件用量 * 单件损耗之和)
- 米数每条: 从定制面料参数表获取
- 库存量/条: 从仓库库存明细匹配面料颜色编号获取可用量
- 库存量/米: 库存量/条 * 米数每条
- 用量信息缺失SPU: 有SPU但面料单件用量为空的记录
- 更新时间、创建时间
"""
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Set, Optional
from collections import defaultdict

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger('fabric_forecast')


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


def extract_fabric_code_from_sku(sku: str) -> str:
    """
    从SKU中提取面料编号(去掉最后一个-之后的字符)
    会先去除"数字+PSC"模式（例如：4PSC）
    例如: ABC-RED-XL -> ABC-RED
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: 面料编号
    """
    if not sku:
        return ''
    
    # 先去除"数字+PSC"模式
    sku = remove_psc_pattern(sku)
    
    idx = sku.rfind('-')  # 找最后一个-
    if idx > 0:
        return sku[:idx]
    return sku


def extract_color_abbr_from_sku(sku: str) -> str:
    """
    从SKU中提取颜色缩写(第一个-和第二个-之间的字符,去掉LONG/SHORT)
    会先去除"数字+PSC"模式（例如：4PSC）
    例如: 
    - ABC-RED-XL -> RED
    - ABC-LONG-RED-XL -> RED
    - ABC-SHORT-BLUE-M -> BLUE
    - ABC-4PSC-RED-XL -> RED (去除4PSC)
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: 颜色缩写
    """
    if not sku:
        return ''
    
    # 先去除"数字+PSC"模式
    sku = remove_psc_pattern(sku)
    
    parts = sku.split('-')
    
    if len(parts) < 2:
        return ''
    
    # 检查第二部分是否是LONG或SHORT
    if len(parts) >= 3 and parts[1].upper() in ['LONG', 'SHORT']:
        # 格式: SPU-LONG-颜色-尺码 或 SPU-SHORT-颜色-尺码
        return parts[2] if len(parts) >= 3 else ''
    else:
        # 标准格式: SPU-颜色-尺码
        return parts[1]


def extract_spu_from_sku(sku: str) -> str:
    """
    从SKU中提取SPU(第一个-之前的部分)
    会先去除"数字+PSC"模式（例如：4PSC）
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: SPU
    """
    if not sku:
        return ''
    # 先去除"数字+PSC"模式
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    if idx > 0:
        return sku[:idx]
    return sku


def remove_last_dash_part(product_name: str) -> str:
    """
    去掉品名倒数第一个-之后的字符
    例如: ABC-RED-001 -> ABC-RED
    
    Args:
        product_name: 品名
        
    Returns:
        str: 处理后的品名
    """
    if not product_name:
        return ''
    
    idx = product_name.rfind('-')
    if idx > 0:
        return product_name[:idx]
    return product_name


def get_fabric_params() -> Dict[str, Dict[str, Any]]:
    """
    从定制面料参数表获取面料参数
    
    Returns:
        Dict[str, Dict[str, Any]]: {面料: {米数每条, 公斤数每条, 面料编号, ...}}
    """
    logger.info("正在从定制面料参数表获取面料参数...")
    
    fabric_params = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '定制面料参数'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("定制面料参数表不存在")
                return fabric_params
            
            sql = """
            SELECT 
                面料,
                面料编号,
                米数每条,
                公斤数每条,
                出米数每公斤
            FROM `定制面料参数`
            WHERE 面料 IS NOT NULL 
              AND 面料 != ''
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从定制面料参数表读取到 {len(results)} 条记录")
            
            for row in results:
                fabric = row['面料'].strip() if row['面料'] else ''
                if fabric:
                    fabric_params[fabric] = {
                        '面料编号': row['面料编号'].strip() if row['面料编号'] else '',
                        '米数每条': float(row['米数每条']) if row['米数每条'] else 0.0,
                        '公斤数每条': float(row['公斤数每条']) if row['公斤数每条'] else 0.0,
                        '出米数每公斤': float(row['出米数每公斤']) if row['出米数每公斤'] else 0.0
                    }
            
            logger.info(f"  构建了 {len(fabric_params)} 个面料的参数映射")
    except Exception as e:
        logger.error(f"从定制面料参数表获取数据失败: {e}", exc_info=True)
    
    return fabric_params


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


def get_fabric_product_name_mapping() -> Dict[str, str]:
    """
    从产品管理表获取面料颜色编号到品名的映射
    用面料颜色编号匹配SKU获取品名
    
    Returns:
        Dict[str, str]: {面料颜色编号(SKU): 品名}
    """
    logger.info("正在从产品管理表获取面料品名映射...")
    
    product_name_map = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '产品管理'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("产品管理表不存在")
                return product_name_map
            
            # 查询所有有品名的SKU
            sql = """
            SELECT 
                SKU,
                品名
            FROM `产品管理`
            WHERE SKU IS NOT NULL 
              AND SKU != ''
              AND 品名 IS NOT NULL
              AND 品名 != ''
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从产品管理表读取到 {len(results)} 条记录")
            
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                product_name = row['品名'].strip() if row['品名'] else ''
                
                if sku and product_name:
                    # 直接使用完整的SKU作为key
                    # 例如: FAB-KNIT-JER-0017-BK -> 013仿棉拉架-30#深黑
                    if sku not in product_name_map:
                        product_name_map[sku] = product_name
            
            logger.info(f"  构建了 {len(product_name_map)} 个面料颜色编号的品名映射")
    except Exception as e:
        logger.error(f"从产品管理表获取品名映射失败: {e}", exc_info=True)
    
    return product_name_map


def get_fabric_to_sku_mapping(fabric_params: Dict[str, Dict[str, Any]]) -> Dict[str, Set[str]]:
    """
    从产品管理表获取面料到SKU的映射
    匹配逻辑: 用面料名称与品名(去掉最后一个-之后的字符)完全匹配
    例如: 面料"381锦单面" 匹配 品名"381锦单面-10#银牡丹" -> SKU "FAB-KNIT-JER-0007-SP"
    
    Args:
        fabric_params: 面料参数字典
        
    Returns:
        Dict[str, Set[str]]: {面料: {SKU集合}}
    """
    logger.info("正在从产品管理表获取面料到SKU的映射...")
    
    fabric_sku_map = defaultdict(set)
    
    # 只处理在定制面料参数表中存在的面料
    valid_fabrics = set(fabric_params.keys())
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '产品管理'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("产品管理表不存在")
                return fabric_sku_map
            
            # 查询所有有品名的SKU
            sql = """
            SELECT 
                SKU,
                品名
            FROM `产品管理`
            WHERE SKU IS NOT NULL 
              AND SKU != ''
              AND 品名 IS NOT NULL
              AND 品名 != ''
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从产品管理表读取到 {len(results)} 条记录")
            
            matched_count = 0
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                product_name = row['品名'].strip() if row['品名'] else ''
                
                if not sku or not product_name:
                    continue
                
                # 将品名去掉倒数第一个-之后的字符
                product_name_trimmed = remove_last_dash_part(product_name)
                
                # 遍历所有面料,看是否与处理后的品名完全匹配
                for fabric in valid_fabrics:
                    if fabric == product_name_trimmed:
                        # 匹配成功,记录这个面料对应的SKU
                        fabric_sku_map[fabric].add(sku)
                        matched_count += 1
                        break
            
            logger.info(f"  匹配到 {matched_count} 个SKU")
            logger.info(f"  匹配到 {len(fabric_sku_map)} 种面料有对应的SKU")
    except Exception as e:
        logger.error(f"从产品管理表获取数据失败: {e}", exc_info=True)
    
    return dict(fabric_sku_map)


def get_fabric_price_data() -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    从面料核价表获取SPU对应的面料及用量信息
    
    Returns:
        Dict[Tuple[str, str], Dict[str, Any]]: {(SPU, 面料): {单件用量, 单件损耗, ...}}
    """
    logger.info("正在从面料核价表获取面料用量信息...")
    
    fabric_usage = {}
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '面料核价表'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("面料核价表不存在")
                return fabric_usage
            
            sql = """
            SELECT 
                SPU,
                面料,
                单件用量,
                单件损耗
            FROM `面料核价表`
            WHERE SPU IS NOT NULL 
              AND SPU != ''
              AND 面料 IS NOT NULL
              AND 面料 != ''
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从面料核价表读取到 {len(results)} 条记录")
            
            for row in results:
                spu = row['SPU'].strip() if row['SPU'] else ''
                fabric = row['面料'].strip() if row['面料'] else ''
                
                if spu and fabric:
                    key = (spu, fabric)
                    fabric_usage[key] = {
                        '单件用量': float(row['单件用量']) if row['单件用量'] else None,
                        '单件损耗': float(row['单件损耗']) if row['单件损耗'] else None
                    }
            
            logger.info(f"  构建了 {len(fabric_usage)} 个SPU-面料组合的用量映射")
    except Exception as e:
        logger.error(f"从面料核价表获取数据失败: {e}", exc_info=True)
    
    return fabric_usage


def get_forecast_order_data() -> Dict[Tuple[str, str], int]:
    """
    从运营预计下单表获取预计下单件数(按SKU+统计日期汇总)
    
    Returns:
        Dict[Tuple[str, str], int]: {(SKU, 统计日期): 预计下单件数}
    """
    logger.info("正在从运营预计下单表获取预计下单件数...")
    
    forecast_data = defaultdict(int)
    
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
                logger.warning("运营预计下单表不存在")
                return dict(forecast_data)
            
            sql = """
            SELECT 
                SKU,
                统计日期,
                SUM(预计下单量) as 总预计下单量
            FROM `运营预计下单表`
            WHERE SKU IS NOT NULL 
              AND SKU != ''
              AND 统计日期 IS NOT NULL
              AND 预计下单量 IS NOT NULL
              AND 预计下单量 > 0
            GROUP BY SKU, 统计日期
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从运营预计下单表读取到 {len(results)} 个SKU+统计日期组合")
            
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                stat_date = row['统计日期']
                quantity = int(row['总预计下单量']) if row['总预计下单量'] else 0
                
                if not sku or not stat_date:
                    continue
                
                # 确保统计日期是字符串格式
                if isinstance(stat_date, str):
                    stat_date_str = stat_date[:10] if len(stat_date) >= 10 else stat_date
                elif hasattr(stat_date, 'strftime'):
                    stat_date_str = stat_date.strftime('%Y-%m-%d')
                else:
                    stat_date_str = str(stat_date)[:10]
                
                if sku and quantity > 0:
                    key = (sku, stat_date_str)
                    forecast_data[key] += quantity
            
            logger.info(f"  构建了 {len(forecast_data)} 个SKU+统计日期的预计下单量映射")
    except Exception as e:
        logger.error(f"从运营预计下单表获取数据失败: {e}", exc_info=True)
    
    return dict(forecast_data)


def get_inventory_data() -> Dict[str, int]:
    """
    从仓库库存明细表获取可用量
    直接使用SKU作为面料颜色编号进行匹配
    
    Returns:
        Dict[str, int]: {面料颜色编号(完整SKU): 可用量}
    """
    logger.info("正在从仓库库存明细表获取库存数据...")
    
    inventory_data = defaultdict(int)
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 检查表是否存在
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '仓库库存明细'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.warning("仓库库存明细表不存在")
                return dict(inventory_data)
            
            sql = """
            SELECT 
                SKU,
                SUM(可用量) as 总可用量
            FROM `仓库库存明细`
            WHERE SKU IS NOT NULL 
              AND SKU != ''
            GROUP BY SKU
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            logger.info(f"  从仓库库存明细表读取到 {len(results)} 个SKU")
            
            for row in results:
                sku = row['SKU'].strip() if row['SKU'] else ''
                quantity = int(row['总可用量']) if row['总可用量'] else 0
                
                if sku and quantity > 0:
                    # 直接使用完整的SKU作为面料颜色编号
                    # 例如: FAB-KNIT-JER-0017-BK
                    inventory_data[sku] += quantity
            
            logger.info(f"  构建了 {len(inventory_data)} 个面料颜色编号的库存映射")
    except Exception as e:
        logger.error(f"从仓库库存明细表获取数据失败: {e}", exc_info=True)
    
    return dict(inventory_data)


def calculate_average_usage_for_fabric(
    fabric_name: str,
    current_spu: str,
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]]
) -> Tuple[Optional[float], Optional[float]]:
    """
    计算使用相同面料的其他SPU的平均用量
    
    Args:
        fabric_name: 面料名称
        current_spu: 当前SPU(排除自己)
        fabric_usage: 面料用量信息 {(SPU, 面料): {单件用量, 单件损耗}}
        
    Returns:
        Tuple[Optional[float], Optional[float]]: (平均单件用量, 平均单件损耗), 如果找不到则返回 (None, None)
    """
    usage_list = []
    loss_list = []
    
    # 查找使用相同面料的其他SPU
    for (spu, fabric), usage_data in fabric_usage.items():
        if fabric == fabric_name and spu != current_spu:
            unit_usage = usage_data.get('单件用量')
            unit_loss = usage_data.get('单件损耗')
            
            # 只统计有有效用量值的记录
            if unit_usage is not None and unit_usage > 0:
                usage_list.append(unit_usage)
                # 损耗值如果没有则默认为1.0
                if unit_loss is not None:
                    loss_list.append(unit_loss)
                else:
                    loss_list.append(1.0)
    
    if not usage_list:
        return (None, None)
    
    # 计算平均值
    avg_usage = sum(usage_list) / len(usage_list)
    avg_loss = sum(loss_list) / len(loss_list) if loss_list else 1.0
    
    return (avg_usage, avg_loss)


def generate_fabric_forecast(
    fabric_params: Dict[str, Dict[str, Any]],
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]],
    forecast_data: Dict[Tuple[str, str], int],
    inventory_data: Dict[str, int],
    product_name_map: Dict[str, str],
    color_map: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    生成面料预估数据
    
    Args:
        fabric_params: 面料参数
        fabric_usage: 面料用量信息 {(SPU, 面料): {单件用量, 单件损耗}}
        forecast_data: 预计下单数据 {(成品SKU, 统计日期): 预计下单量}
        inventory_data: 库存数据
        product_name_map: 面料颜色编号到品名的映射
        color_map: 颜色缩写到颜色中文名的映射
        inventory_data: 库存数据
        product_name_map: 面料颜色编号到品名的映射
        
    Returns:
        List[Dict[str, Any]]: 面料预估数据列表
    """
    logger.info("正在生成面料预估数据...")
    
    # 用于存储每个SKU+面料+统计日期的数据
    sku_fabric_data = []
    
    # 遍历运营预计下单表中的所有成品SKU+统计日期
    processed_count = 0
    skipped_count = 0
    skipped_no_fabric = 0  # 统计没有面料信息的SKU
    filled_with_avg_count = 0  # 统计使用平均值填充的数量
    
    logger.info(f"开始处理运营预计下单表中的 {len(forecast_data)} 个成品SKU+统计日期组合...")
    
    for (sku, stat_date), forecast_quantity in forecast_data.items():
        if forecast_quantity == 0:
            continue
        
        # 从成品SKU提取SPU
        spu = extract_spu_from_sku(sku)
        if not spu:
            skipped_count += 1
            continue
        
        # 查找这个SPU在面料核价表中的所有面料
        spu_fabrics = [(s, f, data) for (s, f), data in fabric_usage.items() if s == spu]
        
        if not spu_fabrics:
            # 没有找到面料信息,跳过
            skipped_no_fabric += 1
            continue
        
        # 遍历这个SPU的所有面料
        for spu_item, fabric_name, usage_data in spu_fabrics:
            # 检查面料是否在定制面料参数表中
            if fabric_name not in fabric_params:
                continue
            
            # 从成品SKU提取颜色缩写
            color_abbr = extract_color_abbr_from_sku(sku)
            
            if not color_abbr:
                skipped_count += 1
                continue
            
            # 从定制面料参数表获取面料编号
            fabric_code = fabric_params[fabric_name].get('面料编号', '')
            
            if not fabric_code:
                skipped_count += 1
                continue
            
            # 面料颜色编号 = 定制面料参数表的面料编号-颜色缩写
            fabric_color_code = f"{fabric_code}-{color_abbr}"
            
            # 获取单件用量和单件损耗
            unit_usage = usage_data.get('单件用量')
            unit_loss = usage_data.get('单件损耗')
            
            # 计算用量系数
            usage_missing = False
            filled_with_avg = False  # 标记是否使用了平均值填充
            
            if unit_usage is None or unit_usage == 0:
                # 用量信息缺失,标记为缺失
                usage_missing = True
                
                # 尝试从使用相同面料的其他SPU获取平均值
                avg_usage, avg_loss = calculate_average_usage_for_fabric(
                    fabric_name, spu, fabric_usage
                )
                
                if avg_usage is not None and avg_usage > 0:
                    # 找到了平均值,使用平均值填充
                    unit_usage = avg_usage
                    unit_loss = avg_loss if avg_loss is not None else 1.0
                    filled_with_avg = True
                    filled_with_avg_count += 1
                    logger.debug(f"SPU {spu} 的面料 {fabric_name} 用量缺失,使用平均值填充: 单件用量={avg_usage:.2f}, 单件损耗={avg_loss:.2f}")
            
            # 计算用量系数
            if usage_missing and not filled_with_avg:
                # 用量缺失且没有找到平均值,用量系数为0
                usage_factor = 0.0
            else:
                # 有原始用量或使用了平均值填充,计算用量系数
                # 确保 unit_usage 有值
                if unit_usage is None or unit_usage <= 0:
                    # 如果仍然没有有效值,用量系数为0
                    usage_factor = 0.0
                else:
                    # 单件损耗默认为1(如果没有损耗值)
                    loss_factor = unit_loss if unit_loss is not None else 1.0
                    usage_factor = unit_usage * loss_factor
            
            # 预计用量 = 预计下单量 * 用量系数
            total_usage = forecast_quantity * usage_factor
            
            # 为每个SKU+面料+统计日期创建一条记录
            sku_fabric_data.append({
                'SKU': sku,
                'SPU': spu,
                '面料': fabric_name,
                '面料编号': fabric_code,
                '颜色缩写': color_abbr,
                '面料颜色编号': fabric_color_code,
                '统计日期': stat_date,
                '预计下单件数': forecast_quantity,
                '预计用量/米': total_usage,
                '用量信息缺失': usage_missing
            })
            
            processed_count += 1
    
    logger.info(f"  处理了 {processed_count} 个成品SKU+面料的记录")
    logger.info(f"  跳过 {skipped_no_fabric} 个SKU (没有面料信息)")
    logger.info(f"  跳过 {skipped_count} 个SKU (无面料编号/颜色缩写)")
    if filled_with_avg_count > 0:
        logger.info(f"  使用平均值填充了 {filled_with_avg_count} 个缺失用量信息的SPU+面料组合")
    logger.info(f"  生成了 {len(sku_fabric_data)} 条SKU+面料记录")
    
    # 构建最终结果
    result_list = []
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for idx, data in enumerate(sku_fabric_data):
        sku = data['SKU']
        spu = data['SPU']
        fabric_name = data['面料']
        fabric_code = data['面料编号']
        color_abbr = data['颜色缩写']
        fabric_color_code = data['面料颜色编号']
        stat_date_str = data['统计日期']
        forecast_quantity = data['预计下单件数']
        total_usage = data['预计用量/米']
        usage_missing = data['用量信息缺失']
        
        # 面料品名 = 用面料颜色编号匹配产品管理表的SKU得到的品名
        fabric_product_name = product_name_map.get(fabric_color_code, '')
        
        # 颜色 = 用颜色缩写匹配颜色对照得到的颜色中文名
        color_name = color_map.get(color_abbr, '')
        
        # 生成月份字段(格式: 25-06)
        try:
            if isinstance(stat_date_str, str):
                date_obj = datetime.strptime(stat_date_str[:10], '%Y-%m-%d')
            else:
                date_obj = stat_date_str
            
            # 格式化为 YY-MM 格式
            month_str = date_obj.strftime('%y-%m')
        except:
            month_str = ''
        
        # 获取米数每条
        meters_per_roll = 0.0
        if fabric_name and fabric_name in fabric_params:
            meters_per_roll = fabric_params[fabric_name].get('米数每条', 0.0)
        
        # 获取库存量/条(用面料颜色编号从仓库库存明细匹配SKU)
        # 直接使用总库存量,不计算平均值
        inventory_rolls = inventory_data.get(fabric_color_code, 0)
        
        # 库存量/米 = 库存量/条 * 米数每条
        inventory_meters = inventory_rolls * meters_per_roll
        
        # 预计用量/条 = 预计用量 / 米数每条
        usage_per_roll = 0.0
        if meters_per_roll > 0:
            usage_per_roll = total_usage / meters_per_roll
        
        # 用量信息缺失SPU
        missing_spu_str = spu if usage_missing else ''
        
        record = {
            'SKU': sku,
            'SPU': spu,
            '面料': fabric_name,
            '面料品名': fabric_product_name,
            '面料编号': fabric_code,
            '颜色缩写': color_abbr,
            '颜色': color_name,
            '面料颜色编号': fabric_color_code,
            '统计日期': stat_date_str,
            '月份': month_str,
            '预计下单件数': forecast_quantity,
            '预计用量/米': round(total_usage, 2),
            '米数每条': meters_per_roll,
            '预计用量/条': round(usage_per_roll, 2),
            '库存量/条': inventory_rolls,
            '库存量/米': round(inventory_meters, 2),
            '用量信息缺失SPU': missing_spu_str,
            '创建时间': current_time,
            '更新时间': current_time
        }
        
        result_list.append(record)
    
    logger.info(f"  生成了 {len(result_list)} 条面料预估记录")
    
    return result_list


def create_fabric_forecast_table_if_not_exists() -> None:
    """创建面料预估表(如果不存在)"""
    logger.info("正在创建或检查面料预估表...")
    
    try:
        with db_cursor(dictionary=False) as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS `面料预估表` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `SKU` VARCHAR(200) COMMENT '成品SKU',
                `SPU` VARCHAR(100) COMMENT '成品SPU',
                `面料` VARCHAR(500) COMMENT '面料名称',
                `面料品名` VARCHAR(500) COMMENT '面料-颜色缩写',
                `面料编号` VARCHAR(500) COMMENT 'SKU去掉最后一个-之后的字符',
                `颜色缩写` VARCHAR(100) COMMENT 'SKU第一个-和第二个-之间的字符(去掉LONG/SHORT)',
                `颜色` VARCHAR(100) COMMENT '颜色中文名,从颜色对照匹配',
                `面料颜色编号` VARCHAR(500) COMMENT '面料编号-颜色缩写',
                `统计日期` DATE COMMENT '统计日期,为当月1号',
                `月份` VARCHAR(20) COMMENT '月份,格式:25-06',
                `预计下单件数` INT DEFAULT 0 COMMENT '从运营预计下单表获取',
                `预计用量/米` DOUBLE DEFAULT 0 COMMENT '预计下单量*(单件用量*单件损耗之和)',
                `米数每条` DOUBLE DEFAULT 0 COMMENT '从定制面料参数表获取',
                `预计用量/条` DOUBLE DEFAULT 0 COMMENT '预计用量/米数每条',
                `库存量/条` DOUBLE DEFAULT 0 COMMENT '从仓库库存明细匹配面料颜色编号获取(多个SKU使用同一面料时均分)',
                `库存量/米` DOUBLE DEFAULT 0 COMMENT '库存量/条*米数每条',
                `用量信息缺失SPU` TEXT COMMENT '有SPU但面料单件用量为空的SPU列表,逗号隔开',
                `创建时间` DATETIME,
                `更新时间` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_sku (`SKU`),
                INDEX idx_spu (`SPU`),
                INDEX idx_fabric (`面料`(100)),
                INDEX idx_fabric_product_name (`面料品名`(100)),
                INDEX idx_fabric_code (`面料编号`(100)),
                INDEX idx_fabric_color_code (`面料颜色编号`(100)),
                INDEX idx_stat_date (`统计日期`),
                INDEX idx_month (`月份`),
                UNIQUE KEY uk_sku_fabric_date (`SKU`, `面料`(100), `统计日期`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='面料预估表'
            """
            
            cursor.execute(sql)
            logger.info("  表检查/创建完成")
            
    except Exception as e:
        logger.error(f"创建面料预估表失败: {e}", exc_info=True)
        raise


def insert_fabric_forecast_batch(data_list: List[Dict[str, Any]]) -> None:
    """
    批量插入面料预估数据到数据库
    
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
            INSERT INTO `面料预估表` (
                SKU, SPU, 面料, 面料品名, 面料编号, 颜色缩写, 颜色, 面料颜色编号, 统计日期, 月份,
                预计下单件数, `预计用量/米`, 米数每条, `预计用量/条`, `库存量/条`, `库存量/米`, 
                用量信息缺失SPU, 创建时间, 更新时间
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                SPU = VALUES(SPU),
                面料品名 = VALUES(面料品名),
                面料编号 = VALUES(面料编号),
                颜色缩写 = VALUES(颜色缩写),
                颜色 = VALUES(颜色),
                面料颜色编号 = VALUES(面料颜色编号),
                月份 = VALUES(月份),
                预计下单件数 = VALUES(预计下单件数),
                `预计用量/米` = VALUES(`预计用量/米`),
                米数每条 = VALUES(米数每条),
                `预计用量/条` = VALUES(`预计用量/条`),
                `库存量/条` = VALUES(`库存量/条`),
                `库存量/米` = VALUES(`库存量/米`),
                用量信息缺失SPU = VALUES(用量信息缺失SPU),
                更新时间 = VALUES(更新时间)
            """
            
            batch_size = 200
            total_inserted = 0
            
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                values = [
                    (
                        row.get('SKU', ''),
                        row.get('SPU', ''),
                        row.get('面料', ''),
                        row.get('面料品名', ''),
                        row.get('面料编号', ''),
                        row.get('颜色缩写', ''),
                        row.get('颜色', ''),
                        row.get('面料颜色编号', ''),
                        row.get('统计日期', ''),
                        row.get('月份', ''),
                        row.get('预计下单件数', 0),
                        row.get('预计用量/米', 0.0),
                        row.get('米数每条', 0.0),
                        row.get('预计用量/条', 0.0),
                        row.get('库存量/条', 0),
                        row.get('库存量/米', 0.0),
                        row.get('用量信息缺失SPU', ''),
                        row.get('创建时间', ''),
                        row.get('更新时间', '')
                    )
                    for row in batch
                ]
                cursor.executemany(sql, values)
                total_inserted += len(batch)
                logger.info(f"  已插入 {total_inserted}/{len(data_list)} 条数据...")
        
        logger.info(f"✓ 成功写入 {len(data_list)} 条数据到面料预估表")
    except Exception as e:
        logger.error(f"写入数据库失败: {e}", exc_info=True)
        raise


def main():
    """主函数"""
    logger.info("="*80)
    logger.info("生成面料预估表任务")
    logger.info("="*80)
    
    try:
        # 1. 获取定制面料参数
        fabric_params = get_fabric_params()
        
        if not fabric_params:
            logger.warning("没有获取到面料参数,无法继续")
            return
        
        # 2. 获取面料核价表数据
        fabric_usage = get_fabric_price_data()
        
        if not fabric_usage:
            logger.warning("没有获取到面料核价表数据")
            return
        
        # 3. 获取运营预计下单数据
        forecast_data = get_forecast_order_data()
        
        if not forecast_data:
            logger.warning("没有获取到预计下单数据")
            return
        
        # 4. 获取库存数据
        inventory_data = get_inventory_data()
        
        # 5. 获取面料品名映射(从产品管理表)
        product_name_map = get_fabric_product_name_mapping()
        
        # 6. 获取颜色映射(从颜色对照表)
        color_map = get_color_mapping()
        
        # 7. 生成面料预估数据
        fabric_forecast_list = generate_fabric_forecast(
            fabric_params,
            fabric_usage,
            forecast_data,
            inventory_data,
            product_name_map,
            color_map
        )
        
        if not fabric_forecast_list:
            logger.warning("没有生成面料预估数据")
            return
        
        # 8. 创建表(如果不存在)
        create_fabric_forecast_table_if_not_exists()
        
        # 9. 写入数据库
        insert_fabric_forecast_batch(fabric_forecast_list)
        
        # 10. 输出统计信息
        logger.info("\n" + "="*80)
        logger.info("处理完成!")
        logger.info("="*80)
        logger.info(f"面料参数: {len(fabric_params)} 种")
        logger.info(f"面料用量信息: {len(fabric_usage)} 个SPU-面料组合")
        logger.info(f"预计下单SKU: {len(forecast_data)} 个")
        logger.info(f"库存SKU: {len(inventory_data)} 个")
        logger.info(f"面料品名映射: {len(product_name_map)} 个")
        logger.info(f"生成面料预估记录: {len(fabric_forecast_list)} 条")
        
        # 统计有用量信息缺失的记录
        missing_usage_count = sum(1 for d in fabric_forecast_list if d.get('用量信息缺失SPU'))
        if missing_usage_count > 0:
            logger.info(f"  其中有 {missing_usage_count} 条记录存在用量信息缺失")
        
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"执行失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()


