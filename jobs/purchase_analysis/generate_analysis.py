#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成下单分析表
从MySQL的采购单表和运营下单表中读取数据，生成下单分析表

数据维度：按 SKU + 店铺 + 日期 汇总
时间范围：最近4个月 + 未来所有时间（增量更新，保留4个月之前的历史数据）
保留字段：日期（主维度）、月份（辅助查询）
"""
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Tuple

from common import settings, get_logger
from common.database import db_cursor
from utils import parse_month, normalize_shop_name

logger = get_logger('analysis_table')


def extract_spu_fields(sku: str) -> Tuple[str, str]:
    """
    从SKU中提取spu和spu颜色
    
    Args:
        sku: SKU字符串，例如 'ZQZ373-BO-M'
    
    Returns:
        Tuple[str, str]: (spu, spu颜色)
            spu: 第一个'-'之前的部分，例如 'ZQZ373'
            spu颜色: 第二个'-'之前的部分，例如 'ZQZ373-BO'
    """
    if not sku or not isinstance(sku, str):
        return '', ''
    
    # 去除首尾空格
    sku = sku.strip()
    if not sku:
        return '', ''
    
    # 提取spu（第一个'-'之前）
    parts = sku.split('-')
    if len(parts) >= 1 and parts[0]:
        spu = parts[0].strip()
    else:
        spu = ''
    
    # 提取spu颜色（第二个'-'之前）
    if len(parts) >= 2 and parts[0] and parts[1]:
        spu_color = '-'.join(parts[:2]).strip()
    elif len(parts) >= 1 and parts[0]:
        spu_color = spu  # 如果只有一个部分，spu颜色等于spu
    else:
        spu_color = ''
    
    return spu, spu_color


def calculate_date_range() -> Tuple[str, str]:
    """
    计算更新的日期范围
    
    Returns:
        Tuple[str, str]: (起始日期, 结束日期)
            起始日期：4个月前的月初
            结束日期：未来（使用一个很大的日期，例如10年后）
    """
    now = datetime.now()
    
    # 计算4个月前的月初
    year = now.year
    month = now.month - 4
    
    # 处理跨年情况
    while month < 1:
        month += 12
        year -= 1
    
    start_date = datetime(year, month, 1)
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    # 未来日期（10年后，足够大）
    end_date = datetime(now.year + 10, 12, 31)
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    return start_date_str, end_date_str


def get_purchase_data(start_date: str) -> Dict:
    """从采购单表获取实际下单数据，按SKU+店铺+日期分组"""
    logger.info("\n1. 正在从采购单表读取数据...")
    logger.info(f"   日期范围: {start_date} 及之后")
    
    with db_cursor(dictionary=True) as cursor:
        # 查询采购单数据（从指定日期开始）
        sql = """
        SELECT 
            SKU,
            店铺名 as 店铺,
            实际数量,
            创建时间,
            DATE(创建时间) as 日期
        FROM 采购单
        WHERE SKU IS NOT NULL AND SKU != ''
          AND DATE(创建时间) >= %s
        """
        
        cursor.execute(sql, (start_date,))
        results = cursor.fetchall()
    
    logger.info(f"   从采购单表读取 {len(results)} 条记录")
    
    # 按 SKU+店铺+日期 分组汇总
    purchase_dict = defaultdict(lambda: {'实际已下单数量': 0, '月份': ''})
    skipped_count = 0
    
    for row in results:
        sku = row['SKU']
        # 确保SKU不为空
        if not sku or not isinstance(sku, str) or not sku.strip():
            skipped_count += 1
            continue
        
        shop = normalize_shop_name(row['店铺'] or '')
        quantity = row['实际数量'] or 0
        create_time = row['创建时间']
        date = row['日期']
        
        # 提取月份（仅用于存储）
        month = parse_month(str(create_time))
        if not month:
            skipped_count += 1
            continue
        
        # 转换日期为字符串格式
        date_str = str(date) if date else str(create_time)[:10]
        
        key = (sku, shop, date_str)
        purchase_dict[key]['实际已下单数量'] += int(quantity)
        purchase_dict[key]['月份'] = month  # 保存月份信息
    
    if skipped_count > 0:
        logger.info(f"   跳过 {skipped_count} 条无效日期的记录")
    logger.info(f"   汇总后共 {len(purchase_dict)} 个SKU+店铺+日期组合")
    return purchase_dict


def get_fabric_data() -> Dict[str, str]:
    """从产品信息表获取面料信息，按SKU映射"""
    logger.info("\n2. 正在从产品信息表读取面料数据...")
    
    try:
        with db_cursor(dictionary=True) as cursor:
            # 查询产品信息表的面料数据
            sql = """
            SELECT 
                SKU,
                面料
            FROM 产品信息
            WHERE SKU IS NOT NULL AND SKU != ''
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
        
        logger.info(f"   从产品信息表读取 {len(results)} 条面料记录")
        
        # 构建SKU到面料的映射
        fabric_dict = {}
        for row in results:
            sku = row['SKU']
            fabric = row['面料'] or ''
            if sku:
                fabric_dict[sku] = fabric
        
        logger.info(f"   构建了 {len(fabric_dict)} 个SKU的面料映射")
        return fabric_dict
        
    except Exception as e:
        logger.warning(f"   获取面料数据失败: {e}")
        logger.warning("   将使用空面料数据继续")
        return {}


def get_operation_order_data(start_date: str) -> Dict:
    """从运营下单表获取预计下单数据，按SKU+店铺+日期分组"""
    logger.info("\n3. 正在从运营下单表读取数据...")
    logger.info(f"   日期范围: {start_date} 及之后")
    
    with db_cursor(dictionary=True) as cursor:
        # 查询运营下单表数据（从指定日期开始）
        sql = """
        SELECT 
            sku as SKU,
            店铺,
            下单数量,
            下单时间,
            DATE(下单时间) as 日期,
            下单人,
            所属部门
        FROM 运营下单表
        WHERE sku IS NOT NULL AND sku != ''
          AND DATE(下单时间) >= %s
        """
        
        cursor.execute(sql, (start_date,))
        results = cursor.fetchall()
    
    logger.info(f"   从运营下单表读取 {len(results)} 条记录")
    
    # 按 SKU+店铺+日期 分组汇总
    operation_dict = defaultdict(lambda: {'预计下单数量': 0, '下单人': set(), '所属部门': set(), '月份': ''})
    skipped_count = 0
    
    for row in results:
        sku = row['SKU']
        # 确保SKU不为空
        if not sku or not isinstance(sku, str) or not sku.strip():
            skipped_count += 1
            continue
        
        shop = normalize_shop_name(row['店铺'] or '')
        quantity = row['下单数量'] or 0
        order_time = row['下单时间']
        date = row['日期']
        orderer = row['下单人'] or ''
        department = row['所属部门'] or ''
        
        # 处理时间字段：如果 order_time 为 None，跳过这条记录
        if order_time is None:
            skipped_count += 1
            continue
        
        # 提取月份（仅用于存储）
        month = parse_month(str(order_time))
        if not month:
            skipped_count += 1
            continue
        
        # 转换日期为字符串格式
        # 优先使用 DATE() 函数提取的日期，如果没有则从 order_time 中提取
        if date:
            date_str = str(date)
        elif order_time:
            # 如果 order_time 是 datetime 对象，转换为字符串后取前10位
            # 如果已经是字符串，直接取前10位
            date_str = str(order_time)[:10]
        else:
            skipped_count += 1
            continue
        
        key = (sku, shop, date_str)
        operation_dict[key]['预计下单数量'] += int(quantity)
        operation_dict[key]['月份'] = month  # 保存月份信息
        if orderer:
            operation_dict[key]['下单人'].add(orderer)
        if department:
            operation_dict[key]['所属部门'].add(department)
    
    # 将set转换为字符串
    for key in operation_dict:
        operation_dict[key]['下单人'] = ', '.join(operation_dict[key]['下单人'])
        operation_dict[key]['所属部门'] = ', '.join(operation_dict[key]['所属部门'])
    
    if skipped_count > 0:
        logger.info(f"   跳过 {skipped_count} 条无效日期的记录")
    logger.info(f"   汇总后共 {len(operation_dict)} 个SKU+店铺+日期组合")
    return operation_dict


def merge_data(purchase_dict: Dict, operation_dict: Dict, fabric_dict: Dict) -> List[Dict[str, Any]]:
    """合并采购单和运营下单表的数据"""
    logger.info("\n4. 正在合并数据...")
    
    # 获取所有的key（SKU+店铺+日期组合）
    all_keys = set(purchase_dict.keys()) | set(operation_dict.keys())
    
    merged_data = []
    
    for key in all_keys:
        sku, shop, date_str = key
        
        # 确保SKU不为空
        if not sku or not isinstance(sku, str) or not sku.strip():
            logger.warning(f"   跳过无效SKU的记录: {key}")
            continue
        
        # 规范化店铺名称
        shop = normalize_shop_name(shop)
        
        # 获取采购数据
        purchase_data = purchase_dict.get(key, {})
        actual_quantity = purchase_data.get('实际已下单数量', 0)
        
        # 获取运营数据
        operation_data = operation_dict.get(key, {})
        expected_quantity = operation_data.get('预计下单数量', 0)
        orderer = operation_data.get('下单人', '')
        department = operation_data.get('所属部门', '')
        
        # 获取月份（优先从采购数据，其次从运营数据）
        month = purchase_data.get('月份') or operation_data.get('月份', '')
        
        # 计算差值
        difference = expected_quantity - actual_quantity
        
        # 更新时间
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 从产品信息表获取面料
        fabric = fabric_dict.get(sku, '')
        
        # 从SKU中提取spu和spu颜色
        spu, spu_color = extract_spu_fields(sku)
        
        # 处理空值：如果字段为空则填充为"无"
        shop = shop if shop and shop.strip() else '无'
        fabric = fabric if fabric and fabric.strip() else '无'
        orderer = orderer if orderer and orderer.strip() else '无'
        department = department if department and department.strip() else '无'
        spu = spu if spu and spu.strip() else '无'
        spu_color = spu_color if spu_color and spu_color.strip() else '无'
        
        # 组装数据
        record = {
            'SKU': sku,
            'spu': spu,
            'spu颜色': spu_color,
            '店铺': shop,
            '面料': fabric,
            '下单人': orderer,
            '所属部门': department,
            '日期': date_str,
            '月份': month,
            '实际已下单数量': actual_quantity,
            '预计下单数量': expected_quantity,
            '下单差值': difference,
            '更新时间': update_time
        }
        
        merged_data.append(record)
    
    # 统计spu和spu颜色的提取情况
    spu_empty_count = sum(1 for r in merged_data if r.get('spu') == '无' or not r.get('spu'))
    spu_color_empty_count = sum(1 for r in merged_data if r.get('spu颜色') == '无' or not r.get('spu颜色'))
    spu_valid_count = len(merged_data) - spu_empty_count
    spu_color_valid_count = len(merged_data) - spu_color_empty_count
    
    logger.info(f"   合并后共 {len(merged_data)} 条记录")
    logger.info(f"   spu提取情况: 有效 {spu_valid_count} 条, 为空 {spu_empty_count} 条")
    logger.info(f"   spu颜色提取情况: 有效 {spu_color_valid_count} 条, 为空 {spu_color_empty_count} 条")
    
    # 如果有很多空的spu，输出一些示例SKU用于调试
    if spu_empty_count > 0 and spu_empty_count <= 10:
        empty_spu_examples = [r['SKU'] for r in merged_data if r.get('spu') == '无' or not r.get('spu')]
        logger.info(f"   spu为空的SKU示例: {empty_spu_examples[:5]}")
    
    return merged_data


def create_analysis_table_if_not_exists() -> None:
    """创建下单分析表（如果不存在）"""
    with db_cursor(dictionary=False) as cursor:
        # 创建表（如果不存在）
        sql = """
        CREATE TABLE IF NOT EXISTS `下单分析表` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `SKU` VARCHAR(255),
            `spu` VARCHAR(255) COMMENT '从SKU第一个-之前提取',
            `spu颜色` VARCHAR(255) COMMENT '从SKU第二个-之前提取',
            `店铺` VARCHAR(255),
            `面料` VARCHAR(255),
            `下单人` VARCHAR(255),
            `所属部门` VARCHAR(255),
            `日期` DATE COMMENT '下单日期',
            `月份` VARCHAR(50) COMMENT '所属月份，格式：YYYY-MM',
            `实际已下单数量` INT,
            `预计下单数量` INT,
            `下单差值` INT,
            `更新时间` DATETIME,
            INDEX idx_sku_shop_date (SKU, 店铺, 日期),
            INDEX idx_month (月份),
            INDEX idx_date (日期)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='下单分析表-按日汇总'
        """
        
        cursor.execute(sql)
        
        # 为已存在的表添加新字段（如果字段不存在）
        # 先检查spu字段
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '下单分析表' 
                AND COLUMN_NAME = 'spu'
            """)
            spu_exists = cursor.fetchone()[0] > 0
            
            if not spu_exists:
                cursor.execute("""
                    ALTER TABLE `下单分析表` 
                    ADD COLUMN `spu` VARCHAR(255) COMMENT '从SKU第一个-之前提取' 
                    AFTER `SKU`
                """)
                logger.info("   已添加字段: spu")
        except Exception as e:
            logger.warning(f"   检查/添加spu字段时出错: {e}")
        
        # 再检查spu颜色字段（如果spu不存在，则放在SKU之后；如果spu存在，则放在spu之后）
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '下单分析表' 
                AND COLUMN_NAME = 'spu颜色'
            """)
            spu_color_exists = cursor.fetchone()[0] > 0
            
            if not spu_color_exists:
                # 检查spu字段是否存在，决定放在哪里
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '下单分析表' 
                    AND COLUMN_NAME = 'spu'
                """)
                spu_exists = cursor.fetchone()[0] > 0
                
                if spu_exists:
                    # spu存在，放在spu之后
                    cursor.execute("""
                        ALTER TABLE `下单分析表` 
                        ADD COLUMN `spu颜色` VARCHAR(255) COMMENT '从SKU第二个-之前提取' 
                        AFTER `spu`
                    """)
                else:
                    # spu不存在，放在SKU之后
                    cursor.execute("""
                        ALTER TABLE `下单分析表` 
                        ADD COLUMN `spu颜色` VARCHAR(255) COMMENT '从SKU第二个-之前提取' 
                        AFTER `SKU`
                    """)
                logger.info("   已添加字段: spu颜色")
        except Exception as e:
            logger.warning(f"   检查/添加spu颜色字段时出错: {e}")


def save_to_database(data_list: List[Dict[str, Any]], start_date: str) -> None:
    """保存数据到下单分析表"""
    logger.info("\n5. 正在保存到数据库...")
    
    if not data_list:
        logger.warning("   没有数据需要保存")
        return
    
    # 创建表
    create_analysis_table_if_not_exists()
    
    with db_cursor(dictionary=False) as cursor:
        # 删除指定日期范围的旧数据（保留4个月之前的历史数据）
        logger.info(f"   正在删除 {start_date} 及之后的旧数据...")
        delete_sql = "DELETE FROM `下单分析表` WHERE `日期` >= %s"
        cursor.execute(delete_sql, (start_date,))
        deleted_count = cursor.rowcount
        logger.info(f"   已删除 {deleted_count} 条旧数据")
        
        # 插入新数据
        logger.info("   正在插入新数据...")
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `下单分析表` ({fields}) VALUES ({values_placeholder})"
        
        batch_size = 200
        for i in range(0, len(data_list), batch_size):
            batch = [tuple(row.values()) for row in data_list[i:i+batch_size]]
            cursor.executemany(sql, batch)
            logger.info(f"   已录入 {min(i+batch_size, len(data_list))} 条...")
    
    logger.info(f"   成功写入 {len(data_list)} 条新数据到下单分析表")
    logger.info(f"   ✅ 保留了 {start_date} 之前的历史数据")
    
    # 在表生成后，通过SQL更新spu和spu颜色字段
    update_spu_fields_from_sku(start_date)


def update_spu_fields_from_sku(start_date: str) -> None:
    """
    在表生成后，通过SQL从SKU字段提取并更新spu和spu颜色字段
    
    Args:
        start_date: 更新日期范围的起始日期
    """
    logger.info("\n6. 正在通过SQL更新spu和spu颜色字段...")
    
    try:
        with db_cursor(dictionary=False) as cursor:
            # 更新spu字段：从SKU的第一个'-'之前提取
            # 使用SUBSTRING_INDEX函数：SUBSTRING_INDEX(SKU, '-', 1) 获取第一个'-'之前的部分
            update_spu_sql = """
                UPDATE `下单分析表`
                SET `spu` = SUBSTRING_INDEX(`SKU`, '-', 1)
                WHERE `日期` >= %s
                  AND (`SKU` IS NOT NULL AND `SKU` != '')
                  AND (`spu` IS NULL OR `spu` = '' OR `spu` = '无')
            """
            cursor.execute(update_spu_sql, (start_date,))
            spu_updated = cursor.rowcount
            logger.info(f"   已更新 {spu_updated} 条记录的spu字段")
            
            # 更新spu颜色字段：从SKU的第二个'-'之前提取
            # 使用SUBSTRING_INDEX函数：SUBSTRING_INDEX(SKU, '-', 2) 获取第二个'-'之前的部分
            # 如果只有一个'-'，则spu颜色等于整个SKU
            # 如果没有'-'，则spu颜色等于spu
            update_spu_color_sql = """
                UPDATE `下单分析表`
                SET `spu颜色` = CASE
                    WHEN LOCATE('-', `SKU`) > 0 AND LOCATE('-', `SKU`, LOCATE('-', `SKU`) + 1) > 0 
                    THEN SUBSTRING_INDEX(`SKU`, '-', 2)
                    WHEN LOCATE('-', `SKU`) > 0 
                    THEN `SKU`
                    ELSE `spu`
                END
                WHERE `日期` >= %s
                  AND (`SKU` IS NOT NULL AND `SKU` != '')
                  AND (`spu颜色` IS NULL OR `spu颜色` = '' OR `spu颜色` = '无')
            """
            cursor.execute(update_spu_color_sql, (start_date,))
            spu_color_updated = cursor.rowcount
            logger.info(f"   已更新 {spu_color_updated} 条记录的spu颜色字段")
            
            # 统计更新后的情况
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN `spu` IS NULL OR `spu` = '' OR `spu` = '无' THEN 1 ELSE 0 END) as empty_spu,
                    SUM(CASE WHEN `spu颜色` IS NULL OR `spu颜色` = '' OR `spu颜色` = '无' THEN 1 ELSE 0 END) as empty_spu_color
                FROM `下单分析表`
                WHERE `日期` >= %s
            """, (start_date,))
            stats = cursor.fetchone()
            total = stats[0]
            empty_spu = stats[1]
            empty_spu_color = stats[2]
            
            logger.info(f"   更新后统计: 总记录 {total} 条, spu为空 {empty_spu} 条, spu颜色为空 {empty_spu_color} 条")
            
    except Exception as e:
        logger.error(f"   更新spu和spu颜色字段时出错: {e}", exc_info=True)


def print_statistics(data_list: List[Dict[str, Any]], start_date: str) -> None:
    """打印统计信息"""
    logger.info("\n" + "="*80)
    logger.info("统计信息：")
    
    logger.info(f"  更新策略: 增量更新（保留4个月之前的历史数据）")
    logger.info(f"  更新范围: {start_date} 及之后")
    logger.info(f"  本次记录数: {len(data_list)} 条（按天汇总）")
    
    if not data_list:
        return
    
    # 统计日期范围
    dates = [record['日期'] for record in data_list if record.get('日期')]
    if dates:
        min_date = min(dates)
        max_date = max(dates)
        logger.info(f"  实际日期范围: {min_date} 至 {max_date}")
    
    # 统计月份分布
    month_counts = defaultdict(int)
    for record in data_list:
        month_counts[record['月份']] += 1
    
    logger.info(f"\n  各月份记录数：")
    for month in sorted(month_counts.keys()):
        logger.info(f"    {month}: {month_counts[month]} 条")
    
    # 统计店铺分布
    shop_counts = defaultdict(int)
    for record in data_list:
        shop_counts[record['店铺']] += 1
    
    logger.info(f"\n  各店铺记录数：")
    for shop, count in sorted(shop_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"    {shop}: {count} 条")
    
    # 统计差值情况
    positive_diff = sum(1 for r in data_list if r['下单差值'] > 0)
    negative_diff = sum(1 for r in data_list if r['下单差值'] < 0)
    zero_diff = sum(1 for r in data_list if r['下单差值'] == 0)
    
    logger.info(f"\n  下单差值统计：")
    logger.info(f"    欠额下单（差值>0，实际少于预计）: {positive_diff} 条")
    logger.info(f"    超额下单（差值<0，实际多于预计）: {negative_diff} 条")
    logger.info(f"    准确下单（差值=0）: {zero_diff} 条")
    
    logger.info("="*80)


def main():
    """主函数"""
    logger.info("="*80)
    logger.info("生成下单分析表")
    logger.info("更新策略：增量更新（最近4个月 + 未来所有时间）")
    logger.info("="*80)
    
    # 计算日期范围
    start_date, end_date = calculate_date_range()
    logger.info(f"\n更新日期范围：")
    logger.info(f"  起始日期: {start_date}（4个月前的月初）")
    logger.info(f"  结束日期: 未来所有时间")
    logger.info(f"  保留数据: {start_date} 之前的历史记录")
    
    try:
        # 1. 从采购单表获取实际下单数据
        purchase_dict = get_purchase_data(start_date)
        
        # 2. 从产品信息表获取面料数据
        fabric_dict = get_fabric_data()
        
        # 3. 从运营下单表获取预计下单数据
        operation_dict = get_operation_order_data(start_date)
        
        # 4. 合并数据
        merged_data = merge_data(purchase_dict, operation_dict, fabric_dict)
        
        # 5. 保存到数据库
        save_to_database(merged_data, start_date)
        
        # 6. 打印统计信息
        print_statistics(merged_data, start_date)
        
        logger.info("\n✅ 处理完成！")
        
    except Exception as e:
        logger.error(f"\n❌ 发生错误: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()

