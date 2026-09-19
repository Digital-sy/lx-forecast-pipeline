#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only trace for final business fabric-color usage.

Reuses the same inputs, Scheme B fabric-splitting policy, and color-resolution
logic as jobs.feishu.export_fabric_color_order_forecast_final, but records the
SKU/SPU contributions behind selected final Feishu colors.

Example:
  python scripts/trace_final_fabric_color_usage.py \
    --fabric '037超绒面料-优化' \
    --color '27#杏色' --color '28#雾霾蓝'
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu import generate_fabric_forecast as fabric_base
from jobs.feishu import generate_fabric_forecast_color_system as color_system
from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu.fabric_color_stocking_spu import load_spu_manual_mapping_catalog


def _round(v: Any) -> float:
    return round(float(v or 0), 2)


def _month_delta(stat_date: Any, current_date: datetime) -> int | None:
    return final_export._month_delta(stat_date, current_date)


async def run(
    fabric_name: str,
    colors: list[str],
    output: Path,
    manual_mapping: Path,
    spu_manual_mapping: Path,
) -> None:
    now = datetime.now()
    target_colors = set(colors)

    governance_catalog = stocking.ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, _ = await stocking.load_catalog_from_feishu(
        base_token=os.getenv("FABRIC_COLOR_CATALOG_BASE_TOKEN", stocking.DEFAULT_BASE_TOKEN),
        table_id=os.getenv("FABRIC_COLOR_CATALOG_TABLE_ID", stocking.DEFAULT_CATALOG_TABLE_ID),
        view_id=os.getenv("FABRIC_COLOR_CATALOG_VIEW_ID", stocking.DEFAULT_CATALOG_VIEW_ID),
    )
    target_catalog = [r for r in catalog_rows if r.fabric_name == fabric_name]
    index = stocking.CatalogIndex(target_catalog)

    manual_catalog = stocking.load_manual_mapping_catalog(manual_mapping)
    spu_catalog = load_spu_manual_mapping_catalog(spu_manual_mapping)

    forecasts, _ = stocking.load_forecast_skus(governance_catalog)
    forecast_by_sku = {r.sku: r for r in forecasts}
    snapshot_rows = stocking._load_snapshot_rows()
    snapshot_by_sku, _ = stocking._snapshot_index(snapshot_rows)
    resolver = ColorSystemResolver(snapshot_rows)

    fabric_usage = fabric_base.get_fabric_price_data()
    purchase_order_data = fabric_base.get_purchase_order_data()
    system_forecast_data = fabric_base.get_system_forecast_data()
    suggest_data = color_system.get_suggest_order_data_color(resolver, current_date=now)
    operation_forecast_data = fabric_base.get_forecast_order_data()
    effective_qty, _ = color_system._effective_sku_quantities(
        resolver, system_forecast_data, suggest_data, now
    )

    month_labels = [label for _, label in color_system.future_months(now, 4)]
    details: list[dict[str, Any]] = []

    def add(sku: str, qty: int, source: str, delta: int | None = None) -> None:
        if qty <= 0:
            return
        forecast = final_export._fallback_forecast(
            sku, forecast_by_sku, resolver, snapshot_by_sku, governance_catalog
        )
        spu = str(forecast.spu or "").strip()
        if not spu:
            return
        usage = fabric_usage.get((spu, fabric_name))
        if not usage:
            return
        meters, missing = fabric_base._calc_usage_meters(
            qty,
            usage.get("单件用量"),
            usage.get("单件损耗"),
            fabric_name,
            spu,
            fabric_usage,
        )
        if meters <= 0:
            return
        decision = final_export._resolve_final_color(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog,
            spu_catalog,
        )
        if not decision.row:
            return
        final_color = decision.row.identity[1]
        if final_color not in target_colors:
            return

        if source == "purchase":
            period = month_labels[0]
            source_label = "当月已下单消耗"
        elif source == "system":
            period = month_labels[int(delta or 0)]
            source_label = "系统预估"
        else:
            period = month_labels[int(delta or 0)]
            source_label = "运营预估"

        details.append({
            "面料": fabric_name,
            "最终飞书颜色": final_color,
            "领星新颜色缩写": decision.row.identity[2],
            "来源口径": source_label,
            "月份": period,
            "SPU": spu,
            "SKU": forecast.sku,
            "品名": forecast.product_name,
            "颜色体系": forecast.color_system,
            "原颜色编码": forecast.color_code,
            "匹配方式": decision.method,
            "数量": int(qty),
            "单件用量": float(usage.get("单件用量") or 0),
            "单件损耗": float(usage.get("单件损耗") or 0),
            "用量米数": _round(meters),
            "用量参数状态": "平均单耗兜底" if missing else "正常",
        })

    for sku, qty in purchase_order_data.items():
        add(sku, int(qty), "purchase")

    for (sku, delta), qty in effective_qty.items():
        add(sku, int(qty), "system", int(delta))

    for (sku, stat_date), qty in operation_forecast_data.items():
        delta = _month_delta(stat_date, now)
        if delta is not None:
            add(sku, int(qty), "operation", delta)

    summary = defaultdict(float)
    by_spu = defaultdict(float)
    for row in details:
        summary[(row["最终飞书颜色"], row["来源口径"], row["月份"])] += row["用量米数"]
        by_spu[(row["最终飞书颜色"], row["来源口径"], row["月份"], row["SPU"])] += row["用量米数"]

    print("\n===== 颜色目录确认 =====")
    for color in colors:
        matches = [r for r in target_catalog if r.identity[1] == color]
        if not matches:
            print(f"{fabric_name} | {color}: 未在当前飞书清单找到")
            continue
        r = matches[0]
        print(f"{fabric_name} | {color} | 领星缩写={r.identity[2]} | record_id={'、'.join(r.record_ids)}")

    print("\n===== 用量汇总（与最终方案B算法同口径） =====")
    for color in colors:
        print(f"\n[{color}]")
        found = False
        for key in sorted(summary.keys(), key=lambda x: (x[1], x[2])):
            if key[0] != color:
                continue
            found = True
            print(f"  {key[1]} | {key[2]} | {summary[key]:.2f} 米")
        if not found:
            print("  当前无用量")

    print("\n===== 来源SPU（按米数降序） =====")
    rows = sorted(by_spu.items(), key=lambda kv: kv[1], reverse=True)
    for (color, source, month, spu), meters in rows:
        print(f"{color} | {source} | {month} | {spu} | {meters:.2f} 米")

    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "面料", "最终飞书颜色", "领星新颜色缩写", "来源口径", "月份", "SPU", "SKU", "品名",
        "颜色体系", "原颜色编码", "匹配方式", "数量", "单件用量", "单件损耗", "用量米数", "用量参数状态",
    ]
    with output.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(details)
    print(f"\n明细CSV: {output}")
    print(f"明细行数: {len(details)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="追溯最终方案B面料颜色用量来源（只读）")
    parser.add_argument("--fabric", required=True)
    parser.add_argument("--color", action="append", required=True)
    parser.add_argument(
        "--manual-mapping",
        type=Path,
        default=Path(os.getenv(
            "FABRIC_COLOR_MANUAL_MAPPING_PATH",
            "/opt/apps/pythondata/shared_config/fabric_color_manual_mapping.csv",
        )),
    )
    parser.add_argument(
        "--spu-manual-mapping",
        type=Path,
        default=Path(os.getenv(
            "FABRIC_COLOR_SPU_MANUAL_MAPPING_PATH",
            "/opt/apps/pythondata/shared_config/fabric_color_manual_mapping_spu.csv",
        )),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/opt/apps/pythondata/exports/fabric_color_usage_trace.csv"),
    )
    args = parser.parse_args()
    asyncio.run(run(
        args.fabric,
        args.color,
        args.output,
        args.manual_mapping,
        args.spu_manual_mapping,
    ))


if __name__ == "__main__":
    main()
