#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""统计无法唯一识别 A2023/B2024 的 SKU 数量、销量及占比（只读）。"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common import get_logger
from common.database import db_cursor
from jobs.feishu.color_system_resolver import (
    ColorSystemResolver,
    UNKNOWN_SYSTEM,
    normalize_sku,
)

logger = get_logger("unresolved_color_system_sales")
OUTPUT_DIR = ROOT / "exports"
PERIODS = ("全历史", "当年累计", "近12个月")


def month_start_offset(base: date, delta: int) -> date:
    year, month = base.year, base.month + delta
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return date(year, month, 1)


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def pct(part: float, total: float) -> float:
    return round(part / total * 100, 4) if total else 0.0


def load_snapshot() -> dict[str, dict[str, Any]]:
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT sku, spu, product_name, custom_fields_json
            FROM `lxpm_product_category_snapshot`
            WHERE sku IS NOT NULL
              AND CHAR_LENGTH(TRIM(CAST(sku AS CHAR))) > 0
        """)
        rows = cursor.fetchall()
    result = {}
    for row in rows:
        sku = normalize_sku(row.get("sku"))
        if sku and sku not in result:
            result[sku] = row
    logger.info(f"产品快照唯一 SKU：{len(result):,}")
    return result


def load_sales() -> list[dict[str, Any]]:
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT SKU, 统计日期, SUM(COALESCE(销量, 0)) AS 销量
            FROM `销量统计_msku月度`
            WHERE SKU IS NOT NULL
              AND CHAR_LENGTH(TRIM(CAST(SKU AS CHAR))) > 0
              AND 统计日期 IS NOT NULL
            GROUP BY SKU, 统计日期
        """)
        rows = list(cursor.fetchall())
    logger.info(f"销量明细：{len(rows):,} 个 SKU+月份")
    return rows


def included_periods(stat_date: date, today: date) -> tuple[str, ...]:
    labels = ["全历史"]
    if date(today.year, 1, 1) <= stat_date < date(today.year + 1, 1, 1):
        labels.append("当年累计")
    if month_start_offset(today, -11) <= stat_date < month_start_offset(today, 1):
        labels.append("近12个月")
    return tuple(labels)


def main() -> None:
    today = datetime.now().date()
    snapshot = load_snapshot()
    resolver = ColorSystemResolver(snapshot.values())
    identities = {
        sku: resolver.resolve(sku, str(row.get("spu") or ""))
        for sku, row in snapshot.items()
    }
    unresolved = {
        sku for sku, identity in identities.items()
        if identity.color_system == UNKNOWN_SYSTEM
    }

    sales_by_sku = defaultdict(lambda: {p: 0.0 for p in PERIODS})
    table_total = {p: 0.0 for p in PERIODS}
    table_not_in_snapshot = {p: 0.0 for p in PERIODS}

    for row in load_sales():
        sku = normalize_sku(row.get("SKU"))
        stat_date = parse_date(row.get("统计日期"))
        qty = float(row.get("销量") or 0)
        if not sku or stat_date is None or qty == 0:
            continue
        for period in included_periods(stat_date, today):
            table_total[period] += qty
            if sku in snapshot:
                sales_by_sku[sku][period] += qty
            else:
                table_not_in_snapshot[period] += qty

    snapshot_total = {
        p: sum(values[p] for values in sales_by_sku.values())
        for p in PERIODS
    }
    unresolved_total = {
        p: sum(sales_by_sku[sku][p] for sku in unresolved)
        for p in PERIODS
    }
    unresolved_with_sales = sum(
        1 for sku in unresolved if sales_by_sku[sku]["全历史"] > 0
    )

    print("\n" + "=" * 76)
    print("A2023 / B2024 无法唯一识别 SKU 分析")
    print("=" * 76)
    print(f"统计日期：{today}")
    print(
        f"当前产品SKU总数：{len(snapshot):,}\n"
        f"待定SKU数量：{len(unresolved):,}\n"
        f"待定SKU占全盘：{pct(len(unresolved), len(snapshot)):.4f}%\n"
        f"待定且有历史销量SKU：{unresolved_with_sales:,}"
    )
    print("-" * 76)
    for period in PERIODS:
        print(
            f"{period}：待定销量 {unresolved_total[period]:,.0f}；"
            f"当前快照全盘销量 {snapshot_total[period]:,.0f}；"
            f"销量占比 {pct(unresolved_total[period], snapshot_total[period]):.4f}%"
        )
    print("-" * 76)
    print("辅助核对：销量表中未匹配当前产品快照的销量")
    for period in PERIODS:
        print(
            f"{period}：{table_not_in_snapshot[period]:,.0f} / "
            f"销量表全部 {table_total[period]:,.0f}"
        )
    print("=" * 76)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output = OUTPUT_DIR / (
        "颜色体系待定SKU销量明细_"
        + datetime.now().strftime("%Y%m%d_%H%M%S")
        + ".csv"
    )
    headers = [
        "SKU", "SPU", "颜色代码", "颜色汇总代码", "识别来源", "产品名称",
        "全历史销量", "当年累计销量", "近12个月销量",
    ]
    with output.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for sku in sorted(
            unresolved,
            key=lambda x: (
                -sales_by_sku[x]["当年累计"],
                -sales_by_sku[x]["近12个月"],
                x,
            ),
        ):
            identity = identities[sku]
            row = snapshot[sku]
            writer.writerow({
                "SKU": sku,
                "SPU": identity.spu,
                "颜色代码": identity.color_code,
                "颜色汇总代码": identity.aggregate_code,
                "识别来源": identity.source,
                "产品名称": row.get("product_name") or "",
                "全历史销量": round(sales_by_sku[sku]["全历史"], 2),
                "当年累计销量": round(sales_by_sku[sku]["当年累计"], 2),
                "近12个月销量": round(sales_by_sku[sku]["近12个月"], 2),
            })
    print(f"\n待定SKU明细：{output}")


if __name__ == "__main__":
    main()
