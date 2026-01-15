#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成库存预估表
结合FBA库存和本地库存明细，生成库存预估表

数据维度：按 SKU + 店铺 + 库存状态 汇总
字段：sku, spu, spu颜色, 店铺, 库存状态, 数量
库存状态：FBA可售, FBA在途, 本地可用量, 本地待到货
"""
from typing import List, Dict, Any, Tuple
from collections import defaultdict

from common import settings, get_logger
from common.database import db_cursor

logger = get_logger('inventory_estimate')


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any]) -> None:
    """
    创建数据表（如果不存在），如果表存在但结构不匹配则报错
    
    Args:
        table_name: 表名
        sample_row: 样本数据行
        
    Raises:
        ValueError: 如果表存在但结构不匹配
    """
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
                # 检查数量字段的类型，确保是数值型
                need_alter = False
                
                if '数量' in expected:
                    cursor.execute(f"""
                        SELECT DATA_TYPE FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table_name}' 
                        AND COLUMN_NAME = '数量'
                    """)
                    result = cursor.fetchone()
                    if result and result[0] not in ['int', 'bigint', 'tinyint', 'smallint', 'mediumint']:
                        # 字段类型不是整数类型，需要修改
                        logger.info(f"   正在修改字段 数量 的类型为 INT...")
                        cursor.execute(f"ALTER TABLE `{table_name}` MODIFY COLUMN `数量` INT DEFAULT 0")
                        need_alter = True
                
                if not need_alter:
                    logger.info(f"表 {table_name} 结构正确")
                return
            else:
                # 表存在但结构不匹配，报错而不是重建
                error_msg = (
                    f"表 {table_name} 已存在但结构不匹配！\n"
                    f"  期望字段: {expected}\n"
                    f"  实际字段: {columns}\n"
                    f"  请手动处理表结构问题，不要自动重建表"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # 表不存在，创建表，使用库存状态+数量的结构
        fields = []
        
        for k, v in sample_row.items():
            if k == '库存状态':
                fields.append(f"`{k}` VARCHAR(50)")
            elif k == '数量':
                fields.append(f"`{k}` INT DEFAULT 0")
            elif isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def delete_all_data(table_name: str) -> int:
    """
    删除表中的所有数据（全量更新）
    
    Args:
        table_name: 表名
        
    Returns:
        int: 删除的记录数
    """
    with db_cursor(dictionary=False) as cursor:
        cursor.execute(f"DELETE FROM `{table_name}`")
        deleted_count = cursor.rowcount
        logger.info(f"已删除 {deleted_count} 条旧数据")
        return deleted_count


def insert_data_batch(table_name: str, data_list: List[Dict[str, Any]]) -> None:
    """
    批量插入数据
    
    Args:
        table_name: 表名
        data_list: 数据列表
    """
    if not data_list:
        return
    
    with db_cursor(dictionary=False) as cursor:
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `{table_name}` ({fields}) VALUES ({values_placeholder})"
        
        batch_size = 200
        for i in range(0, len(data_list), batch_size):
            batch = [tuple(row.values()) for row in data_list[i:i+batch_size]]
            cursor.executemany(sql, batch)
            logger.info(f"已录入 {min(i+batch_size, len(data_list))} 条...")
    
    logger.info(f"成功写入 {len(data_list)} 条数据到表 {table_name}")


def load_fba_inventory() -> Dict[tuple, Tuple[int, int]]:
    """
    从FBA库存明细表加载数据，按SKU+店铺汇总
    
    Returns:
        Dict[tuple, Tuple[int, int]]: {(SKU, 店铺): (fba可售, FBA在途)} 的字典
    """
    fba_data = {}
    
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT `SKU`, `店铺`, 
                   SUM(`FBA可售`) as fba_sellable,
                   SUM(`实际在途`) as fba_in_transit
            FROM `FBA库存明细`
            GROUP BY `SKU`, `店铺`
        """)
        rows = cursor.fetchall()
        
        for row in rows:
            sku = row['SKU'] or '无'
            shop = row['店铺'] or '无'
            # 确保转换为整数类型
            fba_sellable = int(row['fba_sellable'] or 0)
            fba_in_transit = int(row['fba_in_transit'] or 0)
            key = (sku, shop)
            fba_data[key] = (fba_sellable, fba_in_transit)
    
    logger.info(f"从FBA库存明细表加载了 {len(fba_data)} 条SKU+店铺组合")
    return fba_data


def load_local_inventory() -> Dict[tuple, Tuple[int, int]]:
    """
    从仓库库存明细表加载数据，按SKU+店铺汇总
    
    Returns:
        Dict[tuple, Tuple[int, int]]: {(SKU, 店铺): (可用量, 待到货量)} 的字典
    """
    local_data = {}
    
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT `SKU`, `店铺`, 
                   SUM(`可用量`) as available,
                   SUM(`待到货量`) as pending
            FROM `仓库库存明细`
            GROUP BY `SKU`, `店铺`
        """)
        rows = cursor.fetchall()
        
        for row in rows:
            sku = row['SKU'] or '无'
            shop = row['店铺'] or '无'
            # 确保转换为整数类型
            available = int(row['available'] or 0)
            pending = int(row['pending'] or 0)
            key = (sku, shop)
            local_data[key] = (available, pending)
    
    logger.info(f"从仓库库存明细表加载了 {len(local_data)} 条SKU+店铺组合")
    return local_data


def extract_spu_and_color(sku: str) -> Tuple[str, str]:
    """
    从SKU中提取SPU和SPU颜色
    
    Args:
        sku: SKU字符串，格式如 "SPU-颜色-其他"
    
    Returns:
        Tuple[str, str]: (spu, spu颜色)
        - spu: 第一个"-"之前的字符
        - spu颜色: 第二个"-"之前的字符（即 SPU-颜色）
    """
    if not sku or sku == '无':
        return ('无', '无')
    
    # 找到第一个"-"的位置
    first_dash = sku.find('-')
    if first_dash == -1:
        # 没有"-"，整个SKU就是spu
        return (sku, '无')
    
    # spu是第一个"-"之前的字符
    spu = sku[:first_dash]
    
    # 找到第二个"-"的位置
    second_dash = sku.find('-', first_dash + 1)
    if second_dash == -1:
        # 没有第二个"-"，spu颜色就是第一个"-"之后的所有字符
        spu_color = sku[first_dash + 1:]
    else:
        # spu颜色是第二个"-"之前的字符（包含第一个"-"）
        spu_color = sku[:second_dash]
    
    return (spu, spu_color)


def generate_inventory_estimate() -> List[Dict[str, Any]]:
    """
    生成库存预估表数据
    
    结合逻辑：
    1. 相交：SKU和店铺都匹配（且店铺都不是"无"）
    2. 如果本地库存里店铺是"无"，则匹配到对应SKU的所有店铺的FBA库存
    3. 如果FBA库存里店铺是"无"，则匹配到对应SKU的所有店铺的本地库存
    4. 如果两个地方都没有店铺（都是"无"），则合并
    5. 处理只有FBA库存或只有本地库存的情况
    
    Returns:
        List[Dict[str, Any]]: 库存预估表数据列表
    """
    logger.info("正在加载FBA库存数据...")
    fba_data = load_fba_inventory()
    
    logger.info("正在加载本地库存数据...")
    local_data = load_local_inventory()
    
    # 用于存储结果 {(SKU, 店铺): {sku, spu, spu颜色, 店铺, fba可售, FBA在途, 本地可用量, 本地待到货}}
    # 注意：最终会转换为 {sku, spu, spu颜色, 店铺, 库存状态, 数量} 的结构
    result_dict = {}
    
    # 1. 处理相交部分（SKU和店铺都匹配，且店铺都不是"无"）
    logger.info("正在处理相交部分（SKU和店铺都匹配，且店铺都不是'无'）...")
    for (sku, shop) in set(fba_data.keys()) & set(local_data.keys()):
        if shop != '无':
            fba_sellable, fba_in_transit = fba_data[(sku, shop)]
            local_available, local_pending = local_data[(sku, shop)]
            spu, spu_color = extract_spu_and_color(sku)
            
            result_dict[(sku, shop)] = {
                'SKU': sku,
                'SPU': spu,
                'spu颜色': spu_color,
                '店铺': shop,
                'fba可售': fba_sellable,
                'FBA在途': fba_in_transit,
                '本地可用量': local_available,
                '本地待到货': local_pending
            }
    
    # 2. 处理本地库存店铺为"无"的情况
    logger.info("正在处理本地库存店铺为'无'的情况...")
    local_no_shop = {(sku, shop): val for (sku, shop), val in local_data.items() if shop == '无'}
    for (sku, _) in local_no_shop.keys():
        local_available, local_pending = local_data[(sku, '无')]
        
        # 检查FBA是否有该SKU
        fba_has_sku = any(fba_sku == sku for (fba_sku, _) in fba_data.keys())
        
        if fba_has_sku:
            # 如果FBA有该SKU，优先匹配US结尾的店铺
            fba_shops = [(fba_sku, fba_shop) for (fba_sku, fba_shop) in fba_data.keys() 
                        if fba_sku == sku and fba_shop != '无']
            
            if fba_shops:
                # 优先选择US结尾的店铺
                us_shops = [(s, sh) for (s, sh) in fba_shops if sh.endswith('-US') or sh.endswith('US')]
                if us_shops:
                    # 如果有US结尾的店铺，优先加到第一个US店铺
                    target_shop = us_shops[0][1]
                else:
                    # 如果没有US结尾的，选择第一个FBA店铺
                    target_shop = fba_shops[0][1]
                
                key = (sku, target_shop)
                fba_sellable, fba_in_transit = fba_data[key]
                spu, spu_color = extract_spu_and_color(sku)
                
                if key not in result_dict:
                    result_dict[key] = {
                        'SKU': sku,
                        'SPU': spu,
                        'spu颜色': spu_color,
                        '店铺': target_shop,
                        'fba可售': fba_sellable,
                        'FBA在途': fba_in_transit,
                        '本地可用量': local_available,
                        '本地待到货': local_pending
                    }
                else:
                    # 如果已存在，累加本地库存
                    result_dict[key]['本地可用量'] += local_available
                    result_dict[key]['本地待到货'] += local_pending
        else:
            # 如果FBA没有该SKU，则和本地库存其他有店铺的数据相加
            # 优先加到US结尾的店铺
            local_shops = [(local_sku, local_shop) for (local_sku, local_shop) in local_data.keys() 
                          if local_sku == sku and local_shop != '无']
            
            if local_shops:
                # 优先选择US结尾的店铺
                us_shops = [(s, sh) for (s, sh) in local_shops if sh.endswith('-US') or sh.endswith('US')]
                if us_shops:
                    # 如果有US结尾的店铺，优先加到第一个US店铺
                    target_shop = us_shops[0][1]
                else:
                    # 如果没有US结尾的，选择第一个有店铺的
                    target_shop = local_shops[0][1]
                
                key = (sku, target_shop)
                existing_available, existing_pending = local_data[key]
                spu, spu_color = extract_spu_and_color(sku)
                
                if key not in result_dict:
                    # 如果该店铺还没有在结果中，先添加它（包含原有的本地库存）
                    result_dict[key] = {
                        'SKU': sku,
                        'SPU': spu,
                        'spu颜色': spu_color,
                        '店铺': target_shop,
                        'fba可售': 0,
                        'FBA在途': 0,
                        '本地可用量': existing_available + local_available,
                        '本地待到货': existing_pending + local_pending
                    }
                else:
                    # 如果已存在，累加本地库存（店铺为"无"的部分）
                    result_dict[key]['本地可用量'] += local_available
                    result_dict[key]['本地待到货'] += local_pending
            else:
                # 如果本地库存也没有其他店铺，则保留店铺为"无"的记录（在步骤4中处理）
                pass
    
    # 3. 处理FBA库存店铺为"无"的情况，匹配对应SKU的所有店铺的本地库存
    logger.info("正在处理FBA库存店铺为'无'的情况...")
    fba_no_shop = {(sku, shop): val for (sku, shop), val in fba_data.items() if shop == '无'}
    for (sku, _) in fba_no_shop.keys():
        fba_sellable, fba_in_transit = fba_data[(sku, '无')]
        # 查找该SKU的所有本地库存（所有店铺）
        for (local_sku, local_shop) in local_data.keys():
            if local_sku == sku and local_shop != '无':
                key = (sku, local_shop)
                local_available, local_pending = local_data[key]
                spu, spu_color = extract_spu_and_color(sku)
                
                if key not in result_dict:
                    result_dict[key] = {
                        'SKU': sku,
                        'SPU': spu,
                        'spu颜色': spu_color,
                        '店铺': local_shop,
                        'fba可售': fba_sellable,
                        'FBA在途': fba_in_transit,
                        '本地可用量': local_available,
                        '本地待到货': local_pending
                    }
                else:
                    # 如果已存在，累加FBA库存
                    result_dict[key]['fba可售'] += fba_sellable
                    result_dict[key]['FBA在途'] += fba_in_transit
    
    # 4. 处理两个地方都没有店铺的情况（店铺都是"无"）
    logger.info("正在处理两个地方都没有店铺的情况（店铺都是'无'）...")
    for (sku, _) in set(local_no_shop.keys()) & set(fba_no_shop.keys()):
        key = (sku, '无')
        # 检查是否已经在步骤2中处理过（FBA没有该SKU，本地也没有其他店铺）
        if key not in result_dict:
            fba_sellable, fba_in_transit = fba_data[key]
            local_available, local_pending = local_data[key]
            spu, spu_color = extract_spu_and_color(sku)
            
            result_dict[key] = {
                'SKU': sku,
                'SPU': spu,
                'spu颜色': spu_color,
                '店铺': '无',
                'fba可售': fba_sellable,
                'FBA在途': fba_in_transit,
                '本地可用量': local_available,
                '本地待到货': local_pending
            }
    
    # 4.1 处理只有本地库存店铺为"无"且FBA没有该SKU，且本地也没有其他店铺的情况
    logger.info("正在处理只有本地库存店铺为'无'且FBA没有该SKU的情况...")
    for (sku, _) in local_no_shop.keys():
        # 检查FBA是否有该SKU
        fba_has_sku = any(fba_sku == sku for (fba_sku, _) in fba_data.keys())
        # 检查本地是否有其他店铺
        local_has_other_shops = any(local_sku == sku and local_shop != '无' 
                                   for (local_sku, local_shop) in local_data.keys())
        
        if not fba_has_sku and not local_has_other_shops:
            # FBA没有该SKU，本地也没有其他店铺，保留店铺为"无"的记录
            key = (sku, '无')
            if key not in result_dict:
                local_available, local_pending = local_data[key]
                spu, spu_color = extract_spu_and_color(sku)
                
                result_dict[key] = {
                    'SKU': sku,
                    'SPU': spu,
                    'spu颜色': spu_color,
                    '店铺': '无',
                    'fba可售': 0,
                    'FBA在途': 0,
                    '本地可用量': local_available,
                    '本地待到货': local_pending
                }
    
    # 5. 处理只有FBA库存或只有本地库存的情况（店铺不是"无"）
    logger.info("正在处理只有FBA库存或只有本地库存的情况...")
    
    # 只有FBA库存的情况（店铺不是"无"）
    for (sku, shop) in fba_data.keys():
        if shop != '无' and (sku, shop) not in result_dict:
            # 检查是否有对应的本地库存（店铺为"无"）
            if (sku, '无') not in local_data:
                fba_sellable, fba_in_transit = fba_data[(sku, shop)]
                spu, spu_color = extract_spu_and_color(sku)
                
                result_dict[(sku, shop)] = {
                    'SKU': sku,
                    'SPU': spu,
                    'spu颜色': spu_color,
                    '店铺': shop,
                    'fba可售': fba_sellable,
                    'FBA在途': fba_in_transit,
                    '本地可用量': 0,
                    '本地待到货': 0
                }
    
    # 只有本地库存的情况（店铺不是"无"）
    for (sku, shop) in local_data.keys():
        if shop != '无' and (sku, shop) not in result_dict:
            # 检查是否有对应的FBA库存（店铺为"无"）
            if (sku, '无') not in fba_data:
                local_available, local_pending = local_data[(sku, shop)]
                spu, spu_color = extract_spu_and_color(sku)
                
                result_dict[(sku, shop)] = {
                    'SKU': sku,
                    'SPU': spu,
                    'spu颜色': spu_color,
                    '店铺': shop,
                    'fba可售': 0,
                    'FBA在途': 0,
                    '本地可用量': local_available,
                    '本地待到货': local_pending
                }
    
    # 转换为列表，将每个记录拆分成4行（每个状态一行）
    result_list = []
    filtered_count = 0
    
    # 定义4个库存状态
    inventory_statuses = [
        ('FBA可售', 'fba可售'),
        ('FBA在途', 'FBA在途'),
        ('本地可用量', '本地可用量'),
        ('本地待到货', '本地待到货')
    ]
    
    for record in result_dict.values():
        # 确保所有数字字段都是 int 类型
        fba_sellable = int(record.get('fba可售', 0) or 0)
        fba_in_transit = int(record.get('FBA在途', 0) or 0)
        local_available = int(record.get('本地可用量', 0) or 0)
        local_pending = int(record.get('本地待到货', 0) or 0)
        
        # 检查是否全部为0
        if fba_sellable == 0 and fba_in_transit == 0 and local_available == 0 and local_pending == 0:
            filtered_count += 1
            continue
        
        # 为每个状态创建一行记录
        for status_name, field_name in inventory_statuses:
            quantity = int(record.get(field_name, 0) or 0)
            
            # 只添加数量不为0的记录
            if quantity > 0:
                new_record = {
                    'SKU': record['SKU'],
                    'SPU': record['SPU'],
                    'spu颜色': record['spu颜色'],
                    '店铺': record['店铺'],
                    '库存状态': status_name,
                    '数量': quantity
                }
                result_list.append(new_record)
    
    if filtered_count > 0:
        logger.info(f"已过滤 {filtered_count} 条全部为0的记录")
    
    logger.info(f"共生成 {len(result_list)} 条库存预估记录")
    
    return result_list


def main():
    """主函数"""
    logger.info("="*80)
    logger.info("生成库存预估表")
    logger.info("="*80)
    
    # 验证配置
    if not settings.validate():
        logger.error("配置验证失败，请检查.env文件")
        return
    
    try:
        # 生成库存预估表数据
        logger.info("正在生成库存预估表数据...")
        estimate_data = generate_inventory_estimate()
        
        if not estimate_data:
            logger.warning("没有数据需要保存")
            return
        
        # 处理数据库
        table_name = '库存预估表'
        logger.info(f"正在处理数据库表 {table_name}...")
        
        # 创建或检查表结构
        create_table_if_needed(table_name, estimate_data[0])
        
        # 删除所有旧数据（全量更新）
        logger.info("正在删除所有旧数据（全量更新）...")
        deleted_count = delete_all_data(table_name)
        
        # 插入新数据
        logger.info("正在写入新数据...")
        insert_data_batch(table_name, estimate_data)
        
        # 输出统计信息
        logger.info("="*80)
        logger.info("📊 统计信息：")
        logger.info(f"  更新策略: 全量更新")
        logger.info(f"  删除旧记录: {deleted_count} 条")
        logger.info(f"  新增记录: {len(estimate_data)} 条")
        
        # 查询数据库获取最终统计
        try:
            with db_cursor() as cursor:
                # 统计总记录数
                cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
                total_in_db = cursor.fetchone()['total']
                
                # 按库存状态统计总数量
                cursor.execute(f"""
                    SELECT 
                        `库存状态`,
                        SUM(`数量`) as total_quantity,
                        COUNT(*) as count
                    FROM `{table_name}`
                    GROUP BY `库存状态`
                    ORDER BY `库存状态`
                """)
                status_stats = cursor.fetchall()
                
                # 统计各店铺记录数
                cursor.execute(f"""
                    SELECT 
                        `店铺`, 
                        COUNT(*) as count,
                        SUM(CASE WHEN `库存状态` = 'FBA可售' THEN `数量` ELSE 0 END) as total_fba_sellable,
                        SUM(CASE WHEN `库存状态` = 'FBA在途' THEN `数量` ELSE 0 END) as total_fba_in_transit,
                        SUM(CASE WHEN `库存状态` = '本地可用量' THEN `数量` ELSE 0 END) as total_local_available,
                        SUM(CASE WHEN `库存状态` = '本地待到货' THEN `数量` ELSE 0 END) as total_local_pending
                    FROM `{table_name}`
                    GROUP BY `店铺`
                    ORDER BY count DESC
                """)
                shop_stats = cursor.fetchall()
                
                logger.info(f"  数据库总记录: {total_in_db} 条")
                logger.info("  各库存状态统计：")
                for status in status_stats:
                    logger.info(f"    {status['库存状态']}: {status['total_quantity']} (共 {status['count']} 条记录)")
                logger.info("  各店铺统计：")
                for shop in shop_stats:
                    logger.info(f"    {shop['店铺']}: {shop['count']} 条记录, "
                              f"FBA可售={shop['total_fba_sellable']}, "
                              f"FBA在途={shop['total_fba_in_transit']}, "
                              f"本地可用量={shop['total_local_available']}, "
                              f"本地待到货={shop['total_local_pending']}")
        except Exception as e:
            logger.warning(f"查询数据库统计失败: {e}")
        
        logger.info("="*80)
        logger.info("✅ 库存预估表生成完成！")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ 生成库存预估表失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()

