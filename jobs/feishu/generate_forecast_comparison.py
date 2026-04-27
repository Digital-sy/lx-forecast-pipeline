#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
系统预测销量 vs 运营预计下单量 对比报告
维度：SPU + 店铺 + 月份（本月起未来4个月）

数据来源：
  系统预测销量  ← 销量统计_msku月度(DB) → forecast_sales_improved 算法重算 → 按SPU+店铺聚合
  运营预计下单量 ← 运营预计下单表(DB) → 按SPU+店铺+月聚合

输出：
  预测对比表     — SPU + 店铺 + 月份（原有，用于飞书展示）
  预测对比表_SKU — SKU + SPU + 店铺 + 月份（新增，用于面料预估）

更新策略：删除本月及未来月份旧数据，重新插入；历史月份保留不动。
"""

import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor
from jobs.feishu.forecast_sales_improved import compute_forecast_for_shop

logger = get_logger('forecast_comparison')

# 需要排除的店铺（与 write_sales_to_feishu 保持一致）
EXCLUDED_SHOPS = {
    'TEMU半托管-A店', 'TEMU半托管-C店', 'TEMU半托管-M店',
    'TEMU半托管-P店', 'TEMU半托管-V店', 'TEMU半托管-本土店-R店',
    'TK本土店-1店', 'TK跨境店-2店', 'CY-US', 'DX-US', 'MT-CA'
}


# ────────────────────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────────────────────

def remove_psc_pattern(sku: str) -> str:
    if not sku:
        return sku
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    sku = re.sub(r'-+', '-', sku)
    return sku.strip('-')


def extract_spu_from_sku(sku: str) -> str:
    if not sku:
        return ''
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    return sku[:idx] if idx > 0 else sku


def get_forecast_month_labels(current_date: datetime) -> List[Tuple[int, int, str]]:
    """生成本月起未来4个月的列表。返回 [(year, month, label), ...]"""
    months = []
    y, m = current_date.year, current_date.month
    for i in range(4):
        tm, ty = m + i, y
        while tm > 12:
            tm -= 12
            ty += 1
        label = f"{str(ty)[-2:]}年{tm}月"
        months.append((ty, tm, label))
    return months


def get_month_label_sales(year: int, month: int) -> str:
    return f"{str(year)[-2:]}年{month}月销量"


def get_forecast_sales_label(year: int, month: int) -> str:
    return f"{str(year)[-2:]}年{month}月预计销量"


# ────────────────────────────────────────────────────────────────────────────
# Step1：从销量统计_msku月度读取历史销量
# ────────────────────────────────────────────────────────────────────────────

def read_sales_history(
    month_labels_needed: List[str],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    从 销量统计_msku月度 读取所有需要的月份历史销量。
    返回：{shop: {SKU: {月份标签: 销量, 'SPU': spu}}}
    """
    logger.info("正在从数据库读取销量历史...")

    result: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(lambda: defaultdict(dict))

    try:
        with db_cursor() as cursor:
            # 检查是否有SPU字段
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = '销量统计_msku月度'
                AND COLUMN_NAME = 'SPU'
            """)
            has_spu = cursor.fetchone().get('cnt', 0) > 0

            sql = f"""
            SELECT SKU, 店铺, 统计日期, 销量 {', SPU' if has_spu else ''}
            FROM `销量统计_msku月度`
            WHERE 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
              AND SKU IS NOT NULL AND SKU != '' AND SKU != '无'
              AND 统计日期 IS NOT NULL
            """
            cursor.execute(sql)
            rows = cursor.fetchall()
            logger.info(f"读取到 {len(rows)} 条原始销量记录")

        for row in rows:
            shop = (row.get('店铺') or '').strip()
            sku = (row.get('SKU') or '').strip()
            stat_date = row.get('统计日期')
            sales = int(row.get('销量') or 0)

            if not shop or not sku or shop in EXCLUDED_SHOPS:
                continue

            try:
                if isinstance(stat_date, datetime):
                    y, m = stat_date.year, stat_date.month
                elif hasattr(stat_date, 'year'):
                    y, m = stat_date.year, stat_date.month
                elif isinstance(stat_date, str):
                    dt = datetime.strptime(stat_date[:10], '%Y-%m-%d')
                    y, m = dt.year, dt.month
                else:
                    continue
            except Exception:
                continue

            month_key = get_month_label_sales(y, m)
            if month_key not in month_labels_needed:
                continue

            sku_dict = result[shop][sku]

            if 'SPU' not in sku_dict:
                spu = (row.get('SPU') or '').strip() if has_spu else ''
                sku_dict['SPU'] = spu or extract_spu_from_sku(sku)

            sku_dict[month_key] = sku_dict.get(month_key, 0) + sales

    except Exception as e:
        logger.error(f"读取销量历史失败: {e}", exc_info=True)

    logger.info(f"销量历史整理完成，共 {len(result)} 个店铺")
    return result


# ────────────────────────────────────────────────────────────────────────────
# Step2：调用算法计算系统预测
# ────────────────────────────────────────────────────────────────────────────

def compute_system_forecast(
    shop_sales: Dict[str, Dict[str, Dict[str, Any]]],
    forecast_months: List[Tuple[int, int, str]],
    current_date: datetime,
) -> Tuple[Dict[Tuple[str, str, str], int], Dict[Tuple[str, str, str, str], int]]:
    """
    对每个店铺调用 compute_forecast_for_shop，聚合到两个维度：
      - SPU+店铺+月份  （用于写 预测对比表）
      - SKU+SPU+店铺+月份  （用于写 预测对比表_SKU）

    返回：
      system_forecast_spu: {(SPU, 店铺, 月份label): 系统预测销量}
      system_forecast_sku: {(SKU, SPU, 店铺, 月份label): 系统预测销量}
    """
    logger.info("正在计算系统预测销量...")

    forecast_sales_labels = [get_forecast_sales_label(y, m) for y, m, _ in forecast_months]
    label_to_month: Dict[str, str] = {
        get_forecast_sales_label(y, m): lbl
        for y, m, lbl in forecast_months
    }

    system_forecast_spu: Dict[Tuple[str, str, str], int] = defaultdict(int)
    system_forecast_sku: Dict[Tuple[str, str, str, str], int] = {}

    for shop, shop_data in shop_sales.items():
        sku_forecasts = compute_forecast_for_shop(shop_data, forecast_sales_labels, current_date)

        for sku, forecast_dict in sku_forecasts.items():
            spu = (shop_data[sku].get('SPU') or extract_spu_from_sku(sku)).strip()
            if not spu:
                continue

            for flabel, month_lbl in label_to_month.items():
                qty = forecast_dict.get(flabel, 0) or 0

                # SPU 维度聚合（原有）
                system_forecast_spu[(spu, shop, month_lbl)] += qty

                # SKU 维度（新增）
                system_forecast_sku[(sku, spu, shop, month_lbl)] = qty

    logger.info(f"系统预测聚合完成：SPU维度 {len(system_forecast_spu)} 条，SKU维度 {len(system_forecast_sku)} 条")
    return dict(system_forecast_spu), dict(system_forecast_sku)


# ────────────────────────────────────────────────────────────────────────────
# Step3：从运营预计下单表读取，聚合到 SPU+店铺+月 维度
# ────────────────────────────────────────────────────────────────────────────

def read_operation_forecast(
    forecast_months: List[Tuple[int, int, str]],
) -> Dict[Tuple[str, str, str], int]:
    """
    从 运营预计下单表 按 SPU+店铺+月 聚合预计下单量。
    返回：{(SPU, 店铺, 月份label): 运营预计下单量}
    """
    logger.info("正在从运营预计下单表读取数据...")

    date_to_label: Dict[str, str] = {}
    for y, m, lbl in forecast_months:
        date_str = f"{y}-{m:02d}-01"
        date_to_label[date_str] = lbl

    op_forecast: Dict[Tuple[str, str, str], int] = defaultdict(int)

    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '运营预计下单表'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("运营预计下单表不存在，运营预计下单量全部为0")
                return op_forecast

            placeholders = ', '.join(['%s'] * len(date_to_label))
            sql = f"""
            SELECT SKU, 店铺, 统计日期, 预计下单量
            FROM `运营预计下单表`
            WHERE SKU IS NOT NULL AND SKU != ''
              AND 店铺 IS NOT NULL AND 店铺 != ''
              AND 统计日期 IN ({placeholders})
            """
            cursor.execute(sql, list(date_to_label.keys()))
            rows = cursor.fetchall()
            logger.info(f"运营预计下单表读取到 {len(rows)} 条记录")

        for row in rows:
            sku = (row.get('SKU') or '').strip()
            shop = (row.get('店铺') or '').strip()
            stat_date = row.get('统计日期')
            qty = int(row.get('预计下单量') or 0)

            if not sku or not shop or shop in EXCLUDED_SHOPS:
                continue

            if isinstance(stat_date, str):
                date_str = stat_date[:10]
            elif hasattr(stat_date, 'strftime'):
                date_str = stat_date.strftime('%Y-%m-%d')
            else:
                date_str = str(stat_date)[:10]

            month_lbl = date_to_label.get(date_str)
            if not month_lbl:
                continue

            spu = extract_spu_from_sku(sku)
            if spu:
                op_forecast[(spu, shop, month_lbl)] += qty

    except Exception as e:
        logger.error(f"读取运营预计下单表失败: {e}", exc_info=True)

    logger.info(f"运营预计聚合完成，共 {len(op_forecast)} 个 SPU+店铺+月 组合")
    return dict(op_forecast)


# ────────────────────────────────────────────────────────────────────────────
# Step4：合并，生成对比记录
# ────────────────────────────────────────────────────────────────────────────

def build_comparison_records(
    system_forecast: Dict[Tuple[str, str, str], int],
    op_forecast: Dict[Tuple[str, str, str], int],
    forecast_months: List[Tuple[int, int, str]],
) -> List[Dict[str, Any]]:
    """
    合并系统预测和运营预计，生成最终对比表记录。
    只保留系统预测 > 0 或运营预计 > 0 的行。
    """
    all_keys = set(system_forecast.keys()) | set(op_forecast.keys())
    month_order = {lbl: i for i, (_, _, lbl) in enumerate(forecast_months)}

    records = []
    for key in all_keys:
        spu, shop, month_lbl = key
        sys_qty = system_forecast.get(key, 0)
        op_qty = op_forecast.get(key, 0)

        if sys_qty == 0 and op_qty == 0:
            continue

        diff = op_qty - sys_qty
        diff_rate = round(diff / sys_qty, 4) if sys_qty > 0 else None

        records.append({
            'SPU': spu,
            '店铺': shop,
            '月份': month_lbl,
            '系统预测销量': sys_qty,
            '运营预计下单量': op_qty,
            '差异(运营-系统)': diff,
            '差异率': diff_rate,
            '_month_order': month_order.get(month_lbl, 99),
        })

    records.sort(key=lambda r: (r['SPU'], r['店铺'], r['_month_order']))
    for r in records:
        r.pop('_month_order')

    logger.info(f"对比记录生成完成，共 {len(records)} 条")
    return records


# ────────────────────────────────────────────────────────────────────────────
# Step5：写入 预测对比表（SPU 维度，原有）
# ────────────────────────────────────────────────────────────────────────────

TABLE_NAME = '预测对比表'


def ensure_table() -> None:
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
                `id`            INT AUTO_INCREMENT PRIMARY KEY,
                `SPU`           VARCHAR(200) NOT NULL COMMENT 'SPU',
                `店铺`          VARCHAR(200) NOT NULL COMMENT '店铺名称',
                `月份`          VARCHAR(20)  NOT NULL COMMENT '如 26年4月',
                `统计日期`      DATE         NOT NULL COMMENT '月份第一天，如 2026-04-01',
                `系统预测销量`  INT          NOT NULL DEFAULT 0,
                `运营预计下单量` INT         NOT NULL DEFAULT 0,
                `差异`          INT          NOT NULL DEFAULT 0 COMMENT '运营-系统',
                `差异率`        DECIMAL(8,4)          DEFAULT NULL COMMENT '系统为0时为NULL',
                `更新时间`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                            ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_spu_shop_date (`SPU`, `店铺`, `统计日期`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统预测 vs 运营预计下单对比';
        """)
    logger.info(f"表 `{TABLE_NAME}` 已就绪")


def delete_current_and_future(current_month_date: str) -> int:
    with db_cursor() as cursor:
        cursor.execute(
            f"DELETE FROM `{TABLE_NAME}` WHERE `统计日期` >= %s",
            (current_month_date,)
        )
        cnt = cursor.rowcount
    logger.info(f"已删除 {cnt} 条旧数据（{current_month_date} 及以后）")
    return cnt


def insert_records(records: List[Dict[str, Any]], forecast_months: List[Tuple[int, int, str]]) -> int:
    label_to_date: Dict[str, str] = {
        lbl: f"{y}-{m:02d}-01"
        for y, m, lbl in forecast_months
    }

    sql = f"""
        INSERT INTO `{TABLE_NAME}`
            (`SPU`, `店铺`, `月份`, `统计日期`,
             `系统预测销量`, `运营预计下单量`, `差异`, `差异率`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            r['SPU'], r['店铺'], r['月份'],
            label_to_date.get(r['月份'], '2000-01-01'),
            r['系统预测销量'], r['运营预计下单量'],
            r['差异(运营-系统)'], r['差异率'],
        )
        for r in records
    ]

    BATCH = 500
    total = 0
    with db_cursor() as cursor:
        for i in range(0, len(rows), BATCH):
            cursor.executemany(sql, rows[i:i + BATCH])
            total += cursor.rowcount

    logger.info(f"成功写入 {total} 条记录到 `{TABLE_NAME}`")
    return total


# ────────────────────────────────────────────────────────────────────────────
# Step6：写入 预测对比表_SKU（SKU 维度，新增）
# ────────────────────────────────────────────────────────────────────────────

SKU_TABLE_NAME = '预测对比表_SKU'


def ensure_sku_table() -> None:
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{SKU_TABLE_NAME}` (
                `id`           INT AUTO_INCREMENT PRIMARY KEY,
                `SKU`          VARCHAR(200) NOT NULL COMMENT 'SKU',
                `SPU`          VARCHAR(200) NOT NULL COMMENT 'SPU',
                `店铺`         VARCHAR(200) NOT NULL COMMENT '店铺名称',
                `月份`         VARCHAR(20)  NOT NULL COMMENT '如 26年4月',
                `统计日期`     DATE         NOT NULL COMMENT '月份第一天，如 2026-04-01',
                `系统预测销量` INT          NOT NULL DEFAULT 0,
                `更新时间`     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_sku_shop_date (`SKU`, `店铺`, `统计日期`),
                INDEX idx_spu           (`SPU`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统预测销量（SKU维度）'
        """)
    logger.info(f"表 `{SKU_TABLE_NAME}` 已就绪")


def delete_sku_current_and_future(current_month_date: str) -> int:
    with db_cursor() as cursor:
        cursor.execute(
            f"DELETE FROM `{SKU_TABLE_NAME}` WHERE `统计日期` >= %s",
            (current_month_date,)
        )
        cnt = cursor.rowcount
    logger.info(f"[SKU表] 已删除 {cnt} 条旧数据（{current_month_date} 及以后）")
    return cnt


def insert_sku_records(
    sku_forecast: Dict[Tuple[str, str, str, str], int],
    forecast_months: List[Tuple[int, int, str]],
) -> int:
    label_to_date: Dict[str, str] = {
        lbl: f"{y}-{m:02d}-01"
        for y, m, lbl in forecast_months
    }

    rows = [
        (sku, spu, shop, month_lbl, label_to_date.get(month_lbl, '2000-01-01'), qty)
        for (sku, spu, shop, month_lbl), qty in sku_forecast.items()
        if qty > 0
    ]

    if not rows:
        logger.warning("[SKU表] 没有可写入的记录")
        return 0

    sql = f"""
        INSERT INTO `{SKU_TABLE_NAME}`
            (`SKU`, `SPU`, `店铺`, `月份`, `统计日期`, `系统预测销量`)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    BATCH = 500
    total = 0
    with db_cursor() as cursor:
        for i in range(0, len(rows), BATCH):
            cursor.executemany(sql, rows[i:i + BATCH])
            total += cursor.rowcount

    logger.info(f"[SKU表] 成功写入 {total} 条记录到 `{SKU_TABLE_NAME}`")
    return total


# ────────────────────────────────────────────────────────────────────────────
# 主函数
# ────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("系统预测 vs 运营预计下单 对比报告 → 写入数据库")
    logger.info("=" * 70)

    current_date = datetime.now()

    # 未来4个月
    forecast_months = get_forecast_month_labels(current_date)
    logger.info(f"对比月份：{[lbl for _, _, lbl in forecast_months]}")

    # 算法所需历史月份（近3个月 + 去年同期含缓冲）
    needed_labels: set = set()
    y, m = current_date.year, current_date.month
    for delta in [-1, -2, -3]:
        tm, ty = m + delta, y
        while tm < 1:
            tm += 12
            ty -= 1
        needed_labels.add(get_month_label_sales(ty, tm))
    for fy, fm, _ in forecast_months:
        for delta in [-1, 0, 1]:
            tm, ty = fm + delta, fy - 1
            while tm < 1:
                tm += 12
                ty -= 1
            while tm > 12:
                tm -= 12
                ty += 1
            needed_labels.add(get_month_label_sales(ty, tm))

    # Step1：读销量历史
    shop_sales = read_sales_history(list(needed_labels))

    # Step2：系统预测（同时返回 SPU 和 SKU 两个维度）
    system_forecast, system_forecast_sku = compute_system_forecast(
        shop_sales, forecast_months, current_date
    )

    # Step3：运营预计
    op_forecast = read_operation_forecast(forecast_months)

    # Step4：合并对比
    records = build_comparison_records(system_forecast, op_forecast, forecast_months)

    if not records:
        logger.warning("没有生成任何对比记录，退出")
        return

    current_month_date = f"{forecast_months[0][0]}-{forecast_months[0][1]:02d}-01"

    # Step5：写 预测对比表（SPU维度，原有）
    ensure_table()
    delete_current_and_future(current_month_date)
    insert_records(records, forecast_months)

    # Step6：写 预测对比表_SKU（SKU维度，新增）
    ensure_sku_table()
    delete_sku_current_and_future(current_month_date)
    insert_sku_records(system_forecast_sku, forecast_months)

    # 摘要
    total    = len(records)
    both     = sum(1 for r in records if r['系统预测销量'] > 0 and r['运营预计下单量'] > 0)
    only_sys = sum(1 for r in records if r['系统预测销量'] > 0 and r['运营预计下单量'] == 0)
    only_op  = sum(1 for r in records if r['系统预测销量'] == 0 and r['运营预计下单量'] > 0)

    logger.info("\n" + "=" * 70)
    logger.info(f"完成！SPU对比表 {total} 条，SKU预测表 {len(system_forecast_sku)} 条")
    logger.info(f"  两方都有数据：{both} 条")
    logger.info(f"  仅系统有预测：{only_sys} 条（运营未填）")
    logger.info(f"  仅运营有预计：{only_op} 条（新品/系统无数据）")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
