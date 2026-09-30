#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Diagnose whether an old/new color abbreviation transition causes missing fabric demand.

Read-only. It inspects all SKU demand sources used by the final Scheme B fabric
forecast and explains why candidate SKUs do or do not land on a selected final
Feishu fabric color. Primary fabric is retained only as context; being a
non-primary fabric is no longer a blocking reason under Scheme B.
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


def _q(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


async def run(
    fabric_name: str,
    final_color: str,
    codes: list[str],
    name_token: str,
    output: Path,
    manual_mapping: Path,
    spu_manual_mapping: Path,
) -> None:
    now = datetime.now()
    code_set = {str(x).strip().upper() for x in codes if str(x).strip()}

    governance_catalog = stocking.ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, _ = await stocking.load_catalog_from_feishu(
        base_token=os.getenv("FABRIC_COLOR_CATALOG_BASE_TOKEN", stocking.DEFAULT_BASE_TOKEN),
        table_id=os.getenv("FABRIC_COLOR_CATALOG_TABLE_ID", stocking.DEFAULT_CATALOG_TABLE_ID),
        view_id=os.getenv("FABRIC_COLOR_CATALOG_VIEW_ID", stocking.DEFAULT_CATALOG_VIEW_ID),
    )
    target_catalog = [r for r in catalog_rows if r.fabric_name == fabric_name]
    index = stocking.CatalogIndex(target_catalog)
    target_rows = [r for r in target_catalog if r.identity[1] == final_color]

    manual_catalog = stocking.load_manual_mapping_catalog(manual_mapping)
    spu_catalog = load_spu_manual_mapping_catalog(spu_manual_mapping)

    forecasts, _ = stocking.load_forecast_skus(governance_catalog)
    forecast_by_sku = {r.sku: r for r in forecasts}
    snapshot_rows = stocking._load_snapshot_rows()
    snapshot_by_sku, _ = stocking._snapshot_index(snapshot_rows)
    resolver = ColorSystemResolver(snapshot_rows)

    fabric_usage = fabric_base.get_fabric_price_data()
    primary_fabric_by_spu = fabric_base.get_primary_fabric_by_spu(fabric_usage)
    purchase_order_data = fabric_base.get_purchase_order_data()
    system_forecast_data = fabric_base.get_system_forecast_data()
    suggest_data = color_system.get_suggest_order_data_color(resolver, current_date=now)
    operation_forecast_data = fabric_base.get_forecast_order_data()
    effective_qty, _ = color_system._effective_sku_quantities(
        resolver, system_forecast_data, suggest_data, now
    )
    month_labels = [label for _, label in color_system.future_months(now, 4)]

    all_skus = set(purchase_order_data)
    all_skus.update(sku for sku, _ in effective_qty)
    all_skus.update(sku for sku, _ in operation_forecast_data)
    all_skus.update(forecast_by_sku)

    op_qty: dict[tuple[str, int], int] = defaultdict(int)
    for (sku, stat_date), qty in operation_forecast_data.items():
        delta = final_export._month_delta(stat_date, now)
        if delta is not None:
            op_qty[(sku, delta)] += _q(qty)

    rows: list[dict[str, Any]] = []
    for sku in sorted(all_skus):
        forecast = final_export._fallback_forecast(
            sku, forecast_by_sku, resolver, snapshot_by_sku, governance_catalog
        )
        code = str(forecast.color_code or "").strip().upper()
        pname = str(forecast.product_name or "")
        sku_u = str(forecast.sku or sku).upper()
        candidate = (
            code in code_set
            or any(f"-{c}-" in f"-{sku_u}-" for c in code_set)
            or (name_token and name_token in pname)
        )
        if not candidate:
            continue

        spu = str(forecast.spu or "").strip()
        primary = str(primary_fabric_by_spu.get(spu) or "")
        usage = fabric_usage.get((spu, fabric_name))
        decision = final_export._resolve_final_color(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog,
            spu_catalog,
        ) if usage else None

        if decision and decision.row:
            resolved_color = decision.row.identity[1]
            resolved_lx = decision.row.identity[2]
            decision_text = decision.method
        elif decision:
            resolved_color = ""
            resolved_lx = ""
            decision_text = str(decision.reason or decision.reason_code or "待确认")
        else:
            resolved_color = ""
            resolved_lx = ""
            decision_text = "无目标面料用量关系，未进入颜色匹配"

        sys_qtys = [_q(effective_qty.get((sku, d), 0)) for d in range(4)]
        op_qtys = [_q(op_qty.get((sku, d), 0)) for d in range(4)]
        purchase_qty = _q(purchase_order_data.get(sku, 0))

        reasons: list[str] = []
        if not spu:
            reasons.append("无SPU")
        if not usage:
            reasons.append("SPU未配置该面料")
        if usage and resolved_color != final_color:
            reasons.append(f"最终颜色未落到{final_color}(实际={resolved_color or decision_text})")
        if purchase_qty == 0 and sum(sys_qtys) == 0 and sum(op_qtys) == 0:
            reasons.append("当前采购/系统/运营均无数量")
        if not reasons:
            reasons.append("应进入目标颜色用量（方案B）")

        row = {
            "SKU": sku,
            "SPU": spu,
            "品名": pname,
            "颜色体系": forecast.color_system,
            "原颜色编码": forecast.color_code,
            "目标面料": fabric_name,
            "主面料": primary,
            "是否配置目标面料": "是" if usage else "否",
            "是否目标面料为主面料": "是" if usage and primary == fabric_name else "否",
            "最终飞书颜色": resolved_color,
            "最终领星缩写": resolved_lx,
            "匹配方式/原因": decision_text,
            "当月采购数量": purchase_qty,
            **{f"系统{month_labels[d]}数量": sys_qtys[d] for d in range(4)},
            **{f"运营{month_labels[d]}数量": op_qtys[d] for d in range(4)},
            "诊断": "；".join(reasons),
        }
        rows.append(row)

    print("\n===== 目标飞书颜色 =====")
    if target_rows:
        for r in target_rows:
            print(
                f"{fabric_name} | {final_color} | 当前领星缩写={r.identity[2]} | "
                f"record_id={'、'.join(r.record_ids)}"
            )
    else:
        print(f"{fabric_name} | {final_color}: 当前飞书清单不存在")

    print("\n===== 旧/新缩写候选诊断（方案B） =====")
    print(f"候选条件: 颜色编码/sku包含={sorted(code_set)}；品名包含={name_token or '-'}")
    print(f"候选SKU数: {len(rows)}")

    reason_count: dict[str, int] = defaultdict(int)
    for r in rows:
        for reason in str(r["诊断"]).split("；"):
            reason_count[reason] += 1
    for reason, count in sorted(reason_count.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {reason}: {count} SKU")

    print("\n===== 有需求的候选SKU =====")
    active = [
        r for r in rows
        if r["当月采购数量"]
        or any(_q(r.get(f"系统{m}数量")) for m in month_labels)
        or any(_q(r.get(f"运营{m}数量")) for m in month_labels)
    ]
    active.sort(key=lambda r: (
        "应进入目标颜色用量（方案B）" not in r["诊断"],
        r["SPU"], r["SKU"]
    ))
    for r in active[:200]:
        sys_total = sum(_q(r.get(f"系统{m}数量")) for m in month_labels)
        op_total = sum(_q(r.get(f"运营{m}数量")) for m in month_labels)
        print(
            f"{r['SKU']} | SPU={r['SPU']} | code={r['原颜色编码']} | system={r['颜色体系']} | "
            f"主面料={r['主面料']} | 最终={r['最终飞书颜色'] or '-'}({r['最终领星缩写'] or '-'}) | "
            f"采购={r['当月采购数量']} 系统4月={sys_total} 运营4月={op_total} | {r['诊断']}"
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with output.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    else:
        output.write_text("无候选SKU\n", encoding="utf-8-sig")
    print(f"\n诊断CSV: {output}")


def main() -> None:
    p = argparse.ArgumentParser(description="诊断旧/新颜色缩写是否导致最终方案B面料颜色无预测")
    p.add_argument("--fabric", required=True)
    p.add_argument("--final-color", required=True)
    p.add_argument("--code", action="append", default=[])
    p.add_argument("--name-token", default="")
    p.add_argument("--output", type=Path, default=Path("/opt/apps/pythondata/exports/fabric_color_code_transition_diagnosis.csv"))
    p.add_argument("--manual-mapping", type=Path, default=Path(os.getenv(
        "FABRIC_COLOR_MANUAL_MAPPING_PATH", "/opt/apps/pythondata/shared_config/fabric_color_manual_mapping.csv"
    )))
    p.add_argument("--spu-manual-mapping", type=Path, default=Path(os.getenv(
        "FABRIC_COLOR_SPU_MANUAL_MAPPING_PATH", "/opt/apps/pythondata/shared_config/fabric_color_manual_mapping_spu.csv"
    )))
    args = p.parse_args()
    asyncio.run(run(
        args.fabric, args.final_color, args.code, args.name_token, args.output,
        args.manual_mapping, args.spu_manual_mapping,
    ))


if __name__ == "__main__":
    main()
