#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""生成未来4个月系统预测与运营预计对比，并事务性更新 SPU/SKU 结果表。"""
from __future__ import annotations

import calendar
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor
from jobs.feishu.forecast_sales_improved import compute_forecast_for_shop, load_spu_season_map

logger = get_logger("forecast_comparison")
TABLE_NAME = "预测对比表"
SKU_TABLE_NAME = "预测对比表_SKU"
BATCH_SIZE = 500
EXCLUDED_SHOPS = {
    "TEMU半托管-A店", "TEMU半托管-C店", "TEMU半托管-M店",
    "TEMU半托管-P店", "TEMU半托管-V店", "TEMU半托管-本土店-R店",
    "TK本土店-1店", "TK跨境店-2店", "CY-US", "DX-US", "MT-CA",
}


def remove_psc_pattern(sku: str) -> str:
    if not sku:
        return sku
    sku = re.sub(r"\d+(?:PSC|PCS)", "", sku, flags=re.IGNORECASE)
    return re.sub(r"-+", "-", sku).strip("-")


def extract_spu_from_sku(sku: str) -> str:
    sku = remove_psc_pattern(sku or "")
    idx = sku.find("-")
    return sku[:idx] if idx > 0 else sku


def get_forecast_month_labels(current_date: datetime) -> List[Tuple[int, int, str]]:
    months = []
    for offset in range(4):
        year, month = current_date.year, current_date.month + offset
        while month > 12:
            month -= 12
            year += 1
        months.append((year, month, f"{str(year)[-2:]}年{month}月"))
    return months


def get_month_label_sales(year: int, month: int) -> str:
    return f"{str(year)[-2:]}年{month}月销量"


def get_forecast_sales_label(year: int, month: int) -> str:
    return f"{str(year)[-2:]}年{month}月预计销量"


def _needed_sales_labels(current_date: datetime, forecast_months: List[Tuple[int, int, str]]) -> List[str]:
    """当月、近3个月，以及去年各预测月前后1个月。"""
    needed: set[str] = set()
    for delta in (0, -1, -2, -3):
        year, month = current_date.year, current_date.month + delta
        while month < 1:
            month += 12
            year -= 1
        needed.add(get_month_label_sales(year, month))

    for forecast_year, forecast_month, _ in forecast_months:
        for delta in (-1, 0, 1):
            year, month = forecast_year - 1, forecast_month + delta
            while month < 1:
                month += 12
                year -= 1
            while month > 12:
                month -= 12
                year += 1
            needed.add(get_month_label_sales(year, month))
    return sorted(needed)


def read_sales_history(month_labels_needed: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """读取历史销量；当月累计值按已完成天数补全整月，仅作为预测输入。"""
    logger.info("正在从数据库读取销量历史...")
    result: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(lambda: defaultdict(dict))
    needed, today = set(month_labels_needed), datetime.now()
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME='销量统计_msku月度' AND COLUMN_NAME='SPU'
            """)
            has_spu = cursor.fetchone().get("cnt", 0) > 0
            cursor.execute(f"""
                SELECT SKU, 店铺, 统计日期, 销量 {', SPU' if has_spu else ''}
                FROM `销量统计_msku月度`
                WHERE 店铺 IS NOT NULL AND 店铺 NOT IN ('', '无')
                  AND SKU IS NOT NULL AND SKU NOT IN ('', '无')
                  AND 统计日期 IS NOT NULL
            """)
            rows = cursor.fetchall()
        logger.info(f"读取到 {len(rows)} 条原始销量记录")

        bad_dates = 0
        for row in rows:
            shop = str(row.get("店铺") or "").strip()
            sku = str(row.get("SKU") or "").strip()
            if not shop or not sku or shop in EXCLUDED_SHOPS:
                continue
            stat_date = row.get("统计日期")
            try:
                if isinstance(stat_date, datetime) or (hasattr(stat_date, "year") and hasattr(stat_date, "month")):
                    year, month = stat_date.year, stat_date.month
                elif isinstance(stat_date, str):
                    parsed = datetime.strptime(stat_date[:10], "%Y-%m-%d")
                    year, month = parsed.year, parsed.month
                else:
                    bad_dates += 1
                    continue
            except (TypeError, ValueError):
                bad_dates += 1
                continue

            month_key = get_month_label_sales(year, month)
            if month_key not in needed:
                continue
            sku_data = result[shop][sku]
            if "SPU" not in sku_data:
                source_spu = str(row.get("SPU") or "").strip() if has_spu else ""
                sku_data["SPU"] = source_spu or extract_spu_from_sku(sku)

            sales = int(row.get("销量") or 0)
            if year == today.year and month == today.month and today.day > 1:
                sales = int(sales * calendar.monthrange(year, month)[1] / (today.day - 1))
            sku_data[month_key] = int(sku_data.get(month_key, 0) or 0) + sales

        if bad_dates:
            logger.warning(f"跳过 {bad_dates} 条无法解析统计日期的销量记录")
    except Exception as exc:
        logger.error(f"读取销量历史失败: {exc}", exc_info=True)
        raise

    sku_count = sum(len(skus) for skus in result.values())
    logger.info(f"销量历史整理完成：{len(result)} 个店铺，{sku_count} 个 SKU")
    return {shop: dict(skus) for shop, skus in result.items()}


def compute_system_forecast(
    shop_sales: Dict[str, Dict[str, Dict[str, Any]]],
    forecast_months: List[Tuple[int, int, str]],
    current_date: datetime,
) -> Tuple[Dict[Tuple[str, str, str], int], Dict[Tuple[str, str, str, str], int]]:
    labels = [get_forecast_sales_label(y, m) for y, m, _ in forecast_months]
    label_to_month = {get_forecast_sales_label(y, m): label for y, m, label in forecast_months}
    season_map = load_spu_season_map()
    by_spu: Dict[Tuple[str, str, str], int] = defaultdict(int)
    by_sku: Dict[Tuple[str, str, str, str], int] = {}

    for shop, shop_data in shop_sales.items():
        forecasts = compute_forecast_for_shop(shop_data, labels, current_date, spu_season_map=season_map)
        for sku, values in forecasts.items():
            spu = str(shop_data[sku].get("SPU") or extract_spu_from_sku(sku)).strip()
            if not spu:
                continue
            for forecast_label, month_label in label_to_month.items():
                qty = int(values.get(forecast_label, 0) or 0)
                by_spu[(spu, shop, month_label)] += qty
                by_sku[(sku, spu, shop, month_label)] = qty

    logger.info(f"系统预测聚合完成：SPU维度 {len(by_spu)} 条，SKU维度 {len(by_sku)} 条")
    return dict(by_spu), by_sku


def read_operation_forecast(forecast_months: List[Tuple[int, int, str]]) -> Dict[Tuple[str, str, str], int]:
    date_to_label = {f"{y}-{m:02d}-01": label for y, m, label in forecast_months}
    result: Dict[Tuple[str, str, str], int] = defaultdict(int)
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='运营预计下单表'
            """)
            if not cursor.fetchone().get("cnt", 0):
                logger.warning("运营预计下单表不存在，运营预计下单量按0处理")
                return {}
            placeholders = ", ".join(["%s"] * len(date_to_label))
            cursor.execute(f"""
                SELECT SKU, 店铺, 统计日期, 预计下单量 FROM `运营预计下单表`
                WHERE SKU IS NOT NULL AND SKU != ''
                  AND 店铺 IS NOT NULL AND 店铺 != ''
                  AND 统计日期 IN ({placeholders})
            """, list(date_to_label))
            rows = cursor.fetchall()

        for row in rows:
            sku = str(row.get("SKU") or "").strip()
            shop = str(row.get("店铺") or "").strip()
            if not sku or not shop or shop in EXCLUDED_SHOPS:
                continue
            stat_date = row.get("统计日期")
            date_text = stat_date[:10] if isinstance(stat_date, str) else (
                stat_date.strftime("%Y-%m-%d") if hasattr(stat_date, "strftime") else str(stat_date)[:10]
            )
            month_label = date_to_label.get(date_text)
            spu = extract_spu_from_sku(sku)
            if month_label and spu:
                result[(spu, shop, month_label)] += int(row.get("预计下单量") or 0)
    except Exception as exc:
        logger.error(f"读取运营预计下单表失败: {exc}", exc_info=True)
        raise

    logger.info(f"运营预计聚合完成，共 {len(result)} 个 SPU+店铺+月组合")
    return dict(result)


def build_comparison_records(
    system_forecast: Dict[Tuple[str, str, str], int],
    operation_forecast: Dict[Tuple[str, str, str], int],
    forecast_months: List[Tuple[int, int, str]],
) -> List[Dict[str, Any]]:
    month_order = {label: idx for idx, (_, _, label) in enumerate(forecast_months)}
    records = []
    for spu, shop, month_label in set(system_forecast) | set(operation_forecast):
        system_qty = int(system_forecast.get((spu, shop, month_label), 0) or 0)
        operation_qty = int(operation_forecast.get((spu, shop, month_label), 0) or 0)
        if system_qty == 0 and operation_qty == 0:
            continue
        diff = operation_qty - system_qty
        records.append({
            "SPU": spu, "店铺": shop, "月份": month_label,
            "系统预测销量": system_qty, "运营预计下单量": operation_qty,
            "差异(运营-系统)": diff,
            "差异率": round(diff / system_qty, 4) if system_qty > 0 else None,
            "_month_order": month_order.get(month_label, 99),
        })
    records.sort(key=lambda x: (x["SPU"], x["店铺"], x["_month_order"]))
    for row in records:
        row.pop("_month_order", None)
    logger.info(f"对比记录生成完成，共 {len(records)} 条")
    return records


def ensure_tables() -> None:
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
              `id` INT AUTO_INCREMENT PRIMARY KEY,
              `SPU` VARCHAR(200) NOT NULL, `店铺` VARCHAR(200) NOT NULL,
              `月份` VARCHAR(20) NOT NULL, `统计日期` DATE NOT NULL,
              `系统预测销量` INT NOT NULL DEFAULT 0,
              `运营预计下单量` INT NOT NULL DEFAULT 0,
              `差异` INT NOT NULL DEFAULT 0,
              `差异率` DECIMAL(8,4) DEFAULT NULL,
              `更新时间` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              INDEX idx_spu_shop_date (`SPU`, `店铺`, `统计日期`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统预测 vs 运营预计下单对比'
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{SKU_TABLE_NAME}` (
              `id` INT AUTO_INCREMENT PRIMARY KEY,
              `SKU` VARCHAR(200) NOT NULL, `SPU` VARCHAR(200) NOT NULL,
              `店铺` VARCHAR(200) NOT NULL, `月份` VARCHAR(20) NOT NULL,
              `统计日期` DATE NOT NULL, `系统预测销量` INT NOT NULL DEFAULT 0,
              `更新时间` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              INDEX idx_sku_shop_date (`SKU`, `店铺`, `统计日期`), INDEX idx_spu (`SPU`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统预测销量（SKU维度）'
        """)


def replace_forecast_tables(
    records: List[Dict[str, Any]],
    sku_forecast: Dict[Tuple[str, str, str, str], int],
    forecast_months: List[Tuple[int, int, str]],
    current_month_date: str,
) -> Tuple[int, int, int, int]:
    """同一事务内删除并重写两张表；异常由 db_cursor 回滚。"""
    dates = {label: f"{y}-{m:02d}-01" for y, m, label in forecast_months}
    spu_rows = [(
        r["SPU"], r["店铺"], r["月份"], dates[r["月份"]], r["系统预测销量"],
        r["运营预计下单量"], r["差异(运营-系统)"], r["差异率"],
    ) for r in records]
    sku_rows = [
        (sku, spu, shop, month, dates[month], int(qty))
        for (sku, spu, shop, month), qty in sku_forecast.items()
        if int(qty or 0) > 0 and month in dates
    ]
    with db_cursor() as cursor:
        cursor.execute(f"DELETE FROM `{TABLE_NAME}` WHERE `统计日期` >= %s", (current_month_date,))
        deleted_spu = cursor.rowcount
        cursor.execute(f"DELETE FROM `{SKU_TABLE_NAME}` WHERE `统计日期` >= %s", (current_month_date,))
        deleted_sku = cursor.rowcount

        inserted_spu = 0
        sql_spu = f"""INSERT INTO `{TABLE_NAME}`
            (`SPU`,`店铺`,`月份`,`统计日期`,`系统预测销量`,`运营预计下单量`,`差异`,`差异率`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        for start in range(0, len(spu_rows), BATCH_SIZE):
            cursor.executemany(sql_spu, spu_rows[start:start + BATCH_SIZE])
            inserted_spu += cursor.rowcount

        inserted_sku = 0
        sql_sku = f"""INSERT INTO `{SKU_TABLE_NAME}`
            (`SKU`,`SPU`,`店铺`,`月份`,`统计日期`,`系统预测销量`)
            VALUES (%s,%s,%s,%s,%s,%s)"""
        for start in range(0, len(sku_rows), BATCH_SIZE):
            cursor.executemany(sql_sku, sku_rows[start:start + BATCH_SIZE])
            inserted_sku += cursor.rowcount

    logger.info(
        f"预测表事务替换完成：SPU删除{deleted_spu}/写入{inserted_spu}，"
        f"SKU删除{deleted_sku}/写入{inserted_sku}"
    )
    return deleted_spu, inserted_spu, deleted_sku, inserted_sku


def main() -> None:
    logger.info("=" * 70)
    logger.info("系统预测 vs 运营预计下单对比报告 → 写入数据库")
    logger.info("=" * 70)
    current_date = datetime.now()
    forecast_months = get_forecast_month_labels(current_date)
    logger.info(f"对比月份：{[label for _, _, label in forecast_months]}")

    shop_sales = read_sales_history(_needed_sales_labels(current_date, forecast_months))
    if not shop_sales:
        raise RuntimeError("销量历史为空，停止覆盖预测表；请检查销量统计_msku月度")
    system_forecast, system_forecast_sku = compute_system_forecast(shop_sales, forecast_months, current_date)
    if not any(int(qty or 0) > 0 for qty in system_forecast_sku.values()):
        raise RuntimeError("未生成任何大于0的 SKU 系统预测，停止覆盖预测表")

    operation_forecast = read_operation_forecast(forecast_months)
    records = build_comparison_records(system_forecast, operation_forecast, forecast_months)
    if not records:
        raise RuntimeError("未生成任何预测对比记录，停止覆盖预测表")

    ensure_tables()
    current_month_date = f"{forecast_months[0][0]}-{forecast_months[0][1]:02d}-01"
    _, inserted_spu, _, inserted_sku = replace_forecast_tables(
        records, system_forecast_sku, forecast_months, current_month_date
    )
    both = sum(1 for r in records if r["系统预测销量"] > 0 and r["运营预计下单量"] > 0)
    only_system = sum(1 for r in records if r["系统预测销量"] > 0 and r["运营预计下单量"] == 0)
    only_operation = sum(1 for r in records if r["系统预测销量"] == 0 and r["运营预计下单量"] > 0)
    logger.info("\n" + "=" * 70)
    logger.info(f"完成：SPU对比表写入 {inserted_spu} 条，SKU预测表写入 {inserted_sku} 条")
    logger.info(f"  两方都有数据：{both} 条")
    logger.info(f"  仅系统有预测：{only_system} 条（运营未填）")
    logger.info(f"  仅运营有预计：{only_operation} 条（新品/系统无数据）")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
