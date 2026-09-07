#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Diagnose color demand hidden by the "primary fabric only" split rule.

Read-only. The final business forecast always counts every configured fabric into
the fabric TOTAL bucket, but only the fabric with the largest per-piece usage for
an SPU is allowed into concrete color buckets. This script evaluates the target
fabric for every configured SPU regardless of that gate, resolves the final
Feishu color with the production resolver, and quantifies meters that are
currently present only in the fabric total but not allocated to a color row.

It does not change any MySQL/Feishu data and does not change production logic.

Example:
  python scripts/diagnose_non_primary_fabric_color_usage.py \
    --fabric '037超绒面料-优化' \
    --color '28#雾霾蓝' \
    --code HZB --code NTB
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


def _q(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _m(value: Any) -> float:
    try:
        return round(float(value or 0), 2)
    except Exception:
        return 0.0


async def run(
    fabric_name: str,
    colors: list[str],
    codes: list[str],
    output: Path,
    manual_mapping: Path,
    spu_manual_mapping: Path,
) -> None:
    now = datetime.now()
    target_colors = {str(x).strip() for x in colors if str(x).strip()}
    target_codes = {str(x).strip().upper() for x in codes if str(x).strip()}

    governance_catalog = stocking.ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, _ = await stocking.load_catalog_from_feishu(
        base_token=os.getenv(
            "FABRIC_COLOR_CATALOG_BASE_TOKEN", stocking.DEFAULT_BASE_TOKEN
        ),
        table_id=os.getenv(
            "FABRIC_COLOR_CATALOG_TABLE_ID", stocking.DEFAULT_CATALOG_TABLE_ID
        ),
        view_id=os.getenv(
            "FABRIC_COLOR_CATALOG_VIEW_ID", stocking.DEFAULT_CATALOG_VIEW_ID
        ),
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
    primary_fabric_by_spu = fabric_base.get_primary_fabric_by_spu(fabric_usage)
    target_usage_by_spu = {
        str(spu).strip(): usage
        for (spu, fabric), usage in fabric_usage.items()
        if fabric == fabric_name and str(spu).strip()
    }

    purchase_order_data = fabric_base.get_purchase_order_data()
    system_forecast_data = fabric_base.get_system_forecast_data()
    suggest_data = color_system.get_suggest_order_data_color(
        resolver, current_date=now
    )
    operation_forecast_data = fabric_base.get_forecast_order_data()
    effective_qty, _ = color_system._effective_sku_quantities(
        resolver,
        system_forecast_data,
        suggest_data,
        now,
    )
    month_labels = [label for _, label in color_system.future_months(now, 4)]

    demand_rows: list[tuple[str, int, str, int]] = []
    for sku, qty in purchase_order_data.items():
        if _q(qty) > 0:
            demand_rows.append((sku, _q(qty), "当月已下单消耗", 0))
    for (sku, delta), qty in effective_qty.items():
        if 0 <= int(delta) < 4 and _q(qty) > 0:
            demand_rows.append((sku, _q(qty), "系统预估", int(delta)))
    for (sku, stat_date), qty in operation_forecast_data.items():
        delta = final_export._month_delta(stat_date, now)
        if delta is not None and _q(qty) > 0:
            demand_rows.append((sku, _q(qty), "运营预估", int(delta)))

    details: list[dict[str, Any]] = []
    all_target_spus = set(target_usage_by_spu)
    active_spus: set[str] = set()
    main_spus = {
        spu for spu in all_target_spus
        if primary_fabric_by_spu.get(spu) == fabric_name
    }
    non_primary_spus = all_target_spus - main_spus

    for sku, qty, source, delta in demand_rows:
        forecast = final_export._fallback_forecast(
            sku,
            forecast_by_sku,
            resolver,
            snapshot_by_sku,
            governance_catalog,
        )
        spu = str(forecast.spu or "").strip()
        usage = target_usage_by_spu.get(spu)
        if not usage:
            continue
        active_spus.add(spu)

        meters, missing = fabric_base._calc_usage_meters(
            qty,
            usage.get("单件用量"),
            usage.get("单件损耗"),
            fabric_name,
            spu,
            fabric_usage,
        )
        if meters <= 0:
            continue

        primary = str(primary_fabric_by_spu.get(spu) or "").strip()
        is_primary = primary == fabric_name

        decision = final_export._resolve_final_color(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog,
            spu_catalog,
        )
        if decision.row:
            final_color = str(decision.row.identity[1] or "").strip()
            final_lx = str(decision.row.identity[2] or "").strip()
            match_method = str(decision.method or "")
            pending_reason = ""
        else:
            final_color = ""
            final_lx = ""
            match_method = ""
            pending_reason = str(
                decision.reason or decision.reason_code or "待确认"
            )

        code = str(forecast.color_code or "").strip()
        if target_colors and final_color not in target_colors:
            continue
        if target_codes:
            code_u = code.upper()
            sku_u = str(forecast.sku or sku).upper()
            if (
                code_u not in target_codes
                and final_lx.upper() not in target_codes
                and not any(f"-{c}-" in f"-{sku_u}-" for c in target_codes)
            ):
                continue

        period = month_labels[0] if source == "当月已下单消耗" else month_labels[delta]
        color_allocated_now = bool(is_primary and decision.row)
        hidden_by_primary_gate = bool((not is_primary) and decision.row)

        details.append({
            "面料": fabric_name,
            "SPU": spu,
            "SKU": forecast.sku,
            "品名": forecast.product_name,
            "主面料": primary,
            "当前角色": "主面料" if is_primary else "非主面料",
            "颜色体系": forecast.color_system,
            "原颜色编码": code,
            "最终飞书颜色": final_color,
            "最终领星缩写": final_lx,
            "匹配方式": match_method,
            "待确认原因": pending_reason,
            "来源口径": source,
            "月份": period,
            "数量": qty,
            "单件用量": _m(usage.get("单件用量")),
            "单件损耗": _m(usage.get("单件损耗")),
            "用量米数": _m(meters),
            "用量参数状态": "平均单耗兜底" if missing else "正常",
            "当前是否进入颜色行": "是" if color_allocated_now else "否",
            "是否被主面料规则挡住": "是" if hidden_by_primary_gate else "否",
            "颜色维度当前漏拆/米": _m(meters) if hidden_by_primary_gate else 0.0,
        })

    summary: dict[tuple[str, str, str, str], float] = defaultdict(float)
    by_spu: dict[tuple[str, str, str, str, str], float] = defaultdict(float)
    hidden_total = 0.0
    pending_non_primary_total = 0.0
    for row in details:
        color_key = row["最终飞书颜色"] or "待确认"
        summary[
            (row["当前角色"], color_key, row["来源口径"], row["月份"])
        ] += float(row["用量米数"])
        by_spu[
            (
                row["当前角色"],
                color_key,
                row["来源口径"],
                row["月份"],
                row["SPU"],
            )
        ] += float(row["用量米数"])
        hidden_total += float(row["颜色维度当前漏拆/米"])
        if row["当前角色"] == "非主面料" and not row["最终飞书颜色"]:
            pending_non_primary_total += float(row["用量米数"])

    print("\n===== SPU 配置范围 =====")
    print(f"目标面料: {fabric_name}")
    print(f"配置该面料SPU: {len(all_target_spus)}")
    print(f"其中当前判定为主面料: {len(main_spus)}")
    print(f"其中当前判定为非主面料: {len(non_primary_spus)}")
    print(f"未来/当月存在需求的SPU: {len(active_spus)}")

    print("\n===== 颜色维度影响 =====")
    print(
        "已确认颜色、但仅因‘非主面料’规则未进入颜色行: "
        f"{hidden_total:.2f} 米"
    )
    print(
        "非主面料且颜色仍待确认（不能直接分配）: "
        f"{pending_non_primary_total:.2f} 米"
    )
    print("说明: 上述‘漏拆’不是面料总量漏算；总量桶在主面料判断前已累加。")

    print("\n===== 按角色 / 颜色 / 月份汇总 =====")
    for key, meters in sorted(
        summary.items(),
        key=lambda item: (item[0][0], item[0][1], item[0][2], item[0][3]),
    ):
        role, color, source, month = key
        print(f"{role} | {color} | {source} | {month} | {meters:.2f} 米")

    print("\n===== 非主面料来源SPU（按米数降序） =====")
    non_primary_rows = [
        (key, meters)
        for key, meters in by_spu.items()
        if key[0] == "非主面料"
    ]
    for (role, color, source, month, spu), meters in sorted(
        non_primary_rows, key=lambda item: item[1], reverse=True
    )[:300]:
        print(f"{color} | {source} | {month} | {spu} | {meters:.2f} 米")

    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "面料", "SPU", "SKU", "品名", "主面料", "当前角色",
        "颜色体系", "原颜色编码", "最终飞书颜色", "最终领星缩写",
        "匹配方式", "待确认原因", "来源口径", "月份", "数量",
        "单件用量", "单件损耗", "用量米数", "用量参数状态",
        "当前是否进入颜色行", "是否被主面料规则挡住",
        "颜色维度当前漏拆/米",
    ]
    with output.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(details)
    print(f"\n诊断CSV: {output}")
    print(f"明细行数: {len(details)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="量化非主面料因主面料闸门而未拆到具体颜色的需求（只读）"
    )
    parser.add_argument("--fabric", required=True)
    parser.add_argument("--color", action="append", default=[])
    parser.add_argument("--code", action="append", default=[])
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
        default=Path(
            "/opt/apps/pythondata/exports/"
            "non_primary_fabric_color_usage_diagnosis.csv"
        ),
    )
    args = parser.parse_args()
    asyncio.run(run(
        args.fabric,
        args.color,
        args.code,
        args.output,
        args.manual_mapping,
        args.spu_manual_mapping,
    ))


if __name__ == "__main__":
    main()
