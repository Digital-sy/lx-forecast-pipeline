#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""导出“当前颜色体系未收录，但另一颜色体系有候选”的精确 SKU 清单（只读）。

判定示例：
- SKU 当前识别为 A2023；A2023 中查不到颜色代码 CD；B2024 中能查到“科尔多瓦红”。
- SKU 当前识别为 B2024；B2024 中查不到颜色代码；A2023 中存在候选。

输出包含产品快照信息、颜色体系识别来源和全历史/当年累计/近12个月销量，
用于区分“SKU 颜色体系可能打错”与“颜色编制表需要补录”。
"""
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
from jobs.feishu.color_mapping_catalog import ColorMappingCatalog, SUPPORTED_SYSTEMS
from jobs.feishu.color_system_resolver import ColorSystemResolver, normalize_sku

logger = get_logger("color_system_mapping_mismatch_skus")
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


def included_periods(stat_date: date, today: date) -> tuple[str, ...]:
    labels = ["全历史"]
    if date(today.year, 1, 1) <= stat_date < date(today.year + 1, 1, 1):
        labels.append("当年累计")
    if month_start_offset(today, -11) <= stat_date < month_start_offset(today, 1):
        labels.append("近12个月")
    return tuple(labels)


def load_snapshot() -> dict[str, dict[str, Any]]:
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT sku, spu, product_name, custom_fields_json
            FROM `lxpm_product_category_snapshot`
            WHERE sku IS NOT NULL
              AND CHAR_LENGTH(TRIM(CAST(sku AS CHAR))) > 0
        """)
        rows = cursor.fetchall()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        sku = normalize_sku(row.get("sku"))
        if sku and sku not in result:
            result[sku] = row
    logger.info(f"产品快照唯一 SKU：{len(result):,}")
    return result


def load_sales(today: date) -> dict[str, dict[str, float]]:
    sales = defaultdict(lambda: {period: 0.0 for period in PERIODS})
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT SKU, 统计日期, SUM(COALESCE(销量, 0)) AS 销量
            FROM `销量统计_msku月度`
            WHERE SKU IS NOT NULL
              AND CHAR_LENGTH(TRIM(CAST(SKU AS CHAR))) > 0
              AND 统计日期 IS NOT NULL
            GROUP BY SKU, 统计日期
        """)
        rows = cursor.fetchall()

    for row in rows:
        sku = normalize_sku(row.get("SKU"))
        stat_date = parse_date(row.get("统计日期"))
        qty = float(row.get("销量") or 0)
        if not sku or stat_date is None or qty == 0:
            continue
        for period in included_periods(stat_date, today):
            sales[sku][period] += qty
    logger.info(f"销量明细：{len(rows):,} 个 SKU+月份")
    return sales


def find_mismatches(
    snapshot: dict[str, dict[str, Any]],
    sales: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    resolver = ColorSystemResolver(snapshot.values())
    catalog = ColorMappingCatalog.from_runtime()
    result: list[dict[str, Any]] = []

    for sku, row in snapshot.items():
        identity = resolver.resolve(sku, str(row.get("spu") or ""))
        system = identity.color_system
        if system not in SUPPORTED_SYSTEMS:
            continue

        other_system = "B2024" if system == "A2023" else "A2023"
        current_entry = catalog.lookup(system, identity.color_code)
        other_entry = catalog.lookup(other_system, identity.color_code)
        if current_entry is not None or other_entry is None:
            continue

        sku_sales = sales.get(sku, {period: 0.0 for period in PERIODS})
        result.append({
            "SKU": sku,
            "SPU": identity.spu,
            "产品名称": row.get("product_name") or "",
            "当前颜色体系": system,
            "颜色代码": identity.color_code,
            "当前体系中文候选": "",
            "另一颜色体系": other_system,
            "另一体系中文候选": other_entry.chinese,
            "另一体系英文候选": other_entry.english,
            "另一体系潘通色号": other_entry.pantone,
            "颜色体系识别来源": identity.source,
            "全历史销量": round(float(sku_sales.get("全历史", 0)), 2),
            "当年累计销量": round(float(sku_sales.get("当年累计", 0)), 2),
            "近12个月销量": round(float(sku_sales.get("近12个月", 0)), 2),
            "建议核对方向": (
                f"核对该 SKU 是否应改为 {other_system}；"
                f"若当前 {system} 标签无误，则需在 {system} 颜色编制表补录代码 {identity.color_code}"
            ),
        })

    result.sort(key=lambda item: (
        -float(item["当年累计销量"]),
        -float(item["近12个月销量"]),
        -float(item["全历史销量"]),
        str(item["当前颜色体系"]),
        str(item["颜色代码"]),
        str(item["SKU"]),
    ))
    return result


def main() -> None:
    today = datetime.now().date()
    snapshot = load_snapshot()
    sales = load_sales(today)
    rows = find_mismatches(snapshot, sales)

    a_to_b = sum(1 for row in rows if row["当前颜色体系"] == "A2023")
    b_to_a = sum(1 for row in rows if row["当前颜色体系"] == "B2024")
    with_sales = sum(1 for row in rows if float(row["全历史销量"]) > 0)
    ytd_sales = sum(float(row["当年累计销量"]) for row in rows)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output = OUTPUT_DIR / (
        "当前体系未收录但另一体系有候选_SKU清单_"
        + datetime.now().strftime("%Y%m%d_%H%M%S")
        + ".csv"
    )
    headers = [
        "SKU", "SPU", "产品名称", "当前颜色体系", "颜色代码",
        "当前体系中文候选", "另一颜色体系", "另一体系中文候选",
        "另一体系英文候选", "另一体系潘通色号", "颜色体系识别来源",
        "全历史销量", "当年累计销量", "近12个月销量", "建议核对方向",
    ]
    with output.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    print("\n" + "=" * 76)
    print("当前体系未收录、另一体系有候选的 SKU 清单")
    print("=" * 76)
    print(f"SKU总数：{len(rows):,}")
    print(f"A2023未收录、B2024有候选：{a_to_b:,}")
    print(f"B2024未收录、A2023有候选：{b_to_a:,}")
    print(f"有历史销量SKU：{with_sales:,}")
    print(f"这些SKU当年累计销量：{ytd_sales:,.0f}")
    print(f"输出文件：{output}")
    print("=" * 76)


if __name__ == "__main__":
    main()
