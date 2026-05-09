#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
运营预计下单数据从飞书收集到数据库任务
从飞书多维表读取运营填写的预计下单量数据，写入数据库的运营预计下单表
数据源：write_sales_to_feishu.py 生成的飞书多维表（每个店铺一个表）
"""
import asyncio
import re
from typing import List, Dict, Any

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor
from common.feishu import FeishuClient
from utils.data_transform import convert_feishu_record_to_dict

logger = get_logger('feishu_write_order_forecast')

FEISHU_APP_TOKEN = "A1oCb6elda8Q76s0vNKcHYEznCg"

EXCLUDED_SHOPS = {
    'TEMU半托管-A店',
    'TEMU半托管-C店',
    'TEMU半托管-M店',
    'TEMU半托管-P店',
    'TEMU半托管-V店',
    'TEMU半托管-本土店-R店',
    'TK本土店-1店',
    'TK跨境店-2店',
    'CY-US',
    'DX-US',
    'MT-CA'
}


def parse_month_label(month_label: str) -> tuple:
    pattern = r'(\d{2})年(\d{1,2})月预计下单量(?:\(运营填写\))?'
    match = re.match(pattern, month_label)

    if match:
        year_short = int(match.group(1))
        month = int(match.group(2))
        year = 2000 + year_short if year_short < 50 else 1900 + year_short
        return (year, month)

    return None


def get_stat_date_from_month_label(month_label: str) -> str:
    parsed = parse_month_label(month_label)
    if parsed:
        year, month = parsed
        return f"{year}-{month:02d}-01"
    return None


def remove_psc_pattern(sku: str) -> str:
    if not sku:
        return sku
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    sku = re.sub(r'-+', '-', sku)
    sku = sku.strip('-')
    return sku


def extract_spu_from_sku(sku: str) -> str:
    if not sku:
        return ''
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    if idx > 0:
        return sku[:idx]
    return sku


def get_previous_forecast_data() -> Dict[tuple, int]:
    previous_data = {}

    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = '运营预计下单表'
            """)
            result = cursor.fetchone()
            if not result or result.get('cnt', 0) == 0:
                logger.info("表 '运营预计下单表' 不存在，返回空数据（首次运行）")
                return previous_data

            sql = """
            SELECT
                SKU,
                店铺,
                统计日期,
                预计下单量
            FROM `运营预计下单表`
            WHERE SKU IS NOT NULL
              AND SKU != ''
              AND 店铺 IS NOT NULL
              AND 店铺 != ''
              AND 统计日期 IS NOT NULL
            """
            cursor.execute(sql)
            results = cursor.fetchall()

            for row in results:
                sku = row.get('SKU', '').strip()
                shop = row.get('店铺', '').strip()
                stat_date = row.get('统计日期')
                quantity = int(row.get('预计下单量', 0) or 0)

                if sku and shop and stat_date:
                    if isinstance(stat_date, str):
                        stat_date_str = stat_date[:10] if len(stat_date) >= 10 else stat_date
                    elif hasattr(stat_date, 'strftime'):
                        stat_date_str = stat_date.strftime('%Y-%m-%d')
                    else:
                        stat_date_str = str(stat_date)[:10]

                    key = (sku, shop, stat_date_str)
                    previous_data[key] = quantity

            logger.info(f"从数据库读取到 {len(previous_data)} 条上次预估数据")
    except Exception as e:
        error_str = str(e)
        if "doesn't exist" in error_str or "不存在" in error_str:
            logger.info("表 '运营预计下单表' 不存在，返回空数据（首次运行）")
        else:
            logger.warning(f"读取上次预估数据失败: {e}")

    return previous_data


def extract_order_forecast_data(records: List[Dict[str, Any]],
                                field_map: Dict[str, str],
                                shop_name: str,
                                previous_data: Dict[tuple, int]) -> List[Dict[str, Any]]:
    data_list = []
    converted_records = convert_feishu_record_to_dict(records, field_map)

    for record in converted_records:
        sku = record.get('SKU', '').strip()
        if not sku:
            continue

        spu = extract_spu_from_sku(sku)
        operation = record.get('运营', '').strip()

        for field_name, field_value in record.items():
            if '预计下单量' in field_name and '年' in field_name and '月' in field_name:
                stat_date = get_stat_date_from_month_label(field_name)
                if not stat_date:
                    logger.warning(f"无法解析月份标签: {field_name}")
                    continue

                try:
                    if field_value is None or field_value == '':
                        quantity = 0
                    elif isinstance(field_value, (int, float)):
                        quantity = int(field_value)
                    elif isinstance(field_value, str):
                        quantity = int(float(field_value)) if field_value.strip() else 0
                    else:
                        quantity = 0
                except (ValueError, TypeError):
                    quantity = 0

                key = (sku, shop_name, stat_date)
                previous_quantity = previous_data.get(key, 0)
                forecast_change = quantity - previous_quantity

                if forecast_change != 0:
                    data_list.append({
                        'SKU': sku,
                        'SPU': spu,
                        '运营': operation,
                        '店铺': shop_name,
                        '统计日期': stat_date,
                        '预计下单量': quantity,
                        '上次预估下单量': previous_quantity,
                        '预估变化': forecast_change
                    })

    return data_list


def create_table_if_needed(table_name: str) -> None:
    with db_cursor(dictionary=False) as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()

        if exists:
            logger.info(f"表 {table_name} 已存在，检查字段...")
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE '上次预估下单量'")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `上次预估下单量` INT DEFAULT 0")
                logger.info("已添加字段：上次预估下单量")

            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE '预估变化'")
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `预估变化` INT DEFAULT 0")
                logger.info("已添加字段：预估变化")

            cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE '飞书同步时间'")
            if not cursor.fetchone():
                cursor.execute(
                    f"ALTER TABLE `{table_name}` "
                    f"ADD COLUMN `飞书同步时间` DATETIME DEFAULT NULL "
                    f"COMMENT '本条记录最近一次从飞书同步入库时间'"
                )
                logger.info("已添加字段：飞书同步时间")
            return

        sql = f"""
        CREATE TABLE `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            SKU VARCHAR(500),
            SPU VARCHAR(500),
            运营 VARCHAR(500),
            店铺 VARCHAR(500),
            统计日期 DATE,
            预计下单量 INT,
            上次预估下单量 INT DEFAULT 0,
            预估变化 INT DEFAULT 0,
            飞书同步时间 DATETIME DEFAULT NULL COMMENT '本条记录最近一次从飞书同步入库时间',
            INDEX idx_sku (SKU(100)),
            INDEX idx_shop (店铺(100)),
            INDEX idx_date (统计日期),
            UNIQUE KEY uk_sku_shop_date (SKU(100), 店铺(100), 统计日期)
        )
        """
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def insert_data_batch(table_name: str, data_list: List[Dict[str, Any]]) -> None:
    if not data_list:
        logger.warning("没有数据需要插入")
        return

    with db_cursor(dictionary=False) as cursor:
        sql = """
        INSERT INTO `运营预计下单表`
            (SKU, SPU, 运营, 店铺, 统计日期, 预计下单量, 上次预估下单量, 预估变化, 飞书同步时间)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            SPU = VALUES(SPU),
            运营 = VALUES(运营),
            预计下单量 = VALUES(预计下单量),
            上次预估下单量 = VALUES(上次预估下单量),
            预估变化 = VALUES(预估变化),
            飞书同步时间 = NOW()
        """

        batch_size = 200
        total_inserted = 0

        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i+batch_size]
            values = [
                (
                    row.get('SKU', ''),
                    row.get('SPU', ''),
                    row.get('运营', ''),
                    row.get('店铺', ''),
                    row.get('统计日期', ''),
                    row.get('预计下单量', 0),
                    row.get('上次预估下单量', 0),
                    row.get('预估变化', 0)
                )
                for row in batch
            ]
            cursor.executemany(sql, values)
            total_inserted += len(batch)
            logger.info(f"已插入 {total_inserted}/{len(data_list)} 条数据...")

    logger.info(f"成功写入 {len(data_list)} 条数据到表 {table_name}")


async def process_shop_table(shop_name: str, app_token: str, previous_data: Dict[tuple, int] = None) -> List[Dict[str, Any]]:
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"正在处理店铺: {shop_name}")
        logger.info(f"{'='*80}")

        feishu_client = FeishuClient(
            app_token=app_token,
            table_id=""
        )

        tables = await feishu_client.get_tables()

        if shop_name not in tables:
            logger.warning(f"店铺 {shop_name} 的飞书多维表不存在")
            return []

        table_id = tables[shop_name]
        feishu_client.table_id = table_id

        field_map = await feishu_client.get_table_fields()
        logger.info(f"获取到 {len(field_map)} 个字段")

        logger.info(f"正在从飞书多维表读取数据...")
        records = await feishu_client.read_records()
        logger.info(f"读取到 {len(records)} 条记录")

        if not records:
            logger.info(f"店铺 {shop_name} 没有数据")
            return []

        logger.info(f"正在提取预计下单量数据...")
        if previous_data is None:
            previous_data = {}
        order_forecast_data = extract_order_forecast_data(records, field_map, shop_name, previous_data)
        logger.info(f"提取到 {len(order_forecast_data)} 条预计下单量数据")

        return order_forecast_data

    except Exception as e:
        logger.error(f"处理店铺 {shop_name} 失败: {e}", exc_info=True)
        return []


async def main():
    logger.info("="*80)
    logger.info("运营预计下单数据从飞书收集到数据库任务")
    logger.info("="*80)

    if not FEISHU_APP_TOKEN:
        logger.error("请先配置 FEISHU_APP_TOKEN（飞书多维表格的app_token）")
        logger.error("使用方法：")
        logger.error("1. 在飞书中找到 write_sales_to_feishu.py 生成的多维表格")
        logger.error("2. 获取多维表格的app_token（可以从URL中获取，格式如：https://xxx.feishu.cn/base/XXXXXXXXXX）")
        logger.error("3. 在脚本中设置 FEISHU_APP_TOKEN 变量")
        return

    try:
        logger.info("\n正在连接飞书...")
        feishu_client = FeishuClient(
            app_token=FEISHU_APP_TOKEN,
            table_id=""
        )
        logger.info("飞书客户端创建成功")
    except Exception as e:
        logger.error(f"创建飞书客户端失败: {e}")
        return

    try:
        logger.info("\n正在获取所有店铺表...")
        tables = await feishu_client.get_tables()
        logger.info(f"找到 {len(tables)} 个表")

        shop_tables = {
            shop_name: table_id
            for shop_name, table_id in tables.items()
            if shop_name not in EXCLUDED_SHOPS
        }

        excluded_count = len(tables) - len(shop_tables)
        if excluded_count > 0:
            excluded_shops = [s for s in EXCLUDED_SHOPS if s in tables]
            logger.info(f"已排除 {excluded_count} 个店铺: {', '.join(excluded_shops)}")

        logger.info(f"将处理 {len(shop_tables)} 个店铺表")
        for shop_name in sorted(shop_tables.keys()):
            logger.info(f"  - {shop_name}")
    except Exception as e:
        logger.error(f"获取表列表失败: {e}", exc_info=True)
        return

    if not shop_tables:
        logger.warning("没有需要处理的店铺表")
        return

    table_name = '运营预计下单表'
    try:
        create_table_if_needed(table_name)
    except Exception as e:
        logger.error(f"创建表失败: {e}", exc_info=True)
        return

    logger.info(f"\n正在获取上次预估数据...")
    previous_data = get_previous_forecast_data()

    logger.info(f"\n正在从飞书提取数据（包含上次预估和变化）...")
    all_data = []
    success_count = 0
    fail_count = 0

    for shop_name in sorted(shop_tables.keys()):
        try:
            shop_data = await process_shop_table(shop_name, FEISHU_APP_TOKEN, previous_data)
            if shop_data:
                all_data.extend(shop_data)
                success_count += 1
            else:
                logger.info(f"店铺 {shop_name} 没有数据")
        except Exception as e:
            logger.error(f"处理店铺 {shop_name} 失败: {e}", exc_info=True)
            fail_count += 1

        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.warning("延迟被中断，继续处理下一个店铺")
            break

    if all_data:
        logger.info(f"\n{'='*80}")
        logger.info(f"正在写入数据库...")
        logger.info(f"{'='*80}")

        try:
            insert_data_batch(table_name, all_data)
            logger.info(f"\n✓ 成功写入 {len(all_data)} 条数据到数据库表 {table_name}")
        except Exception as e:
            logger.error(f"写入数据库失败: {e}", exc_info=True)
            return
    else:
        logger.warning("没有数据需要写入数据库")

    logger.info("\n" + "="*80)
    logger.info("处理完成！")
    logger.info("="*80)
    logger.info(f"成功处理: {success_count} 个店铺")
    if fail_count > 0:
        logger.warning(f"处理失败: {fail_count} 个店铺")
    logger.info(f"共收集到: {len(all_data)} 条预计下单量数据")
    logger.info("="*80)


if __name__ == '__main__':
    asyncio.run(main())
