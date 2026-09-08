#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only diagnosis for pending fabric-color allocations.

Given one target fabric and one or more raw color codes, print/export the full
context needed to decide whether each pending code is caused by:
- stale/missing SPU manual target color;
- missing/inactive Feishu fabric color;
- governance (A2023/B2024) mapping mismatch;
- unknown color system;
- product-name / Chinese-color mismatch.

The script reuses the same Scheme B read-only sources and final color resolver
as the production fabric forecast. It never writes MySQL or Feishu.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu import generate_fabric_forecast as fabric_base
from jobs.feishu import generate_fabric_forecast_color_system as color_system
from jobs.feishu.color_mapping_catalog import SUPPORTED_SYSTEMS
from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu.fabric_color_stocking_spu import load_spu_manual_mapping_catalog


def _q(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _m(value: Any) -> float:
    return round(float(value or 0), 2)


def _join(values: Iterable[Any]) -> str:
    return "、".join(sorted({str(v).strip() for v in values if str(v).strip()}))


def _governance_names(catalog: Any, system: str, code: str) -> str:
    return _join(
        entry.chinese
        for entry in catalog.entries_for_code(system, code)
        if getattr(entry, "chinese", "")
    )


def _manual_targets(entries: Iterable[Any]) -> str:
    return _join(getattr(entry, "catalog_color_name", "") for entry in entries)


async def run(
    fabric_name: str,
    codes: list[str],
    output: Path,
    manual_mapping: Path,
    spu_manual_mapping: Path,
) -> None:
    now = datetime.now()
    code_set = {str(code).strip().upper() for code in codes if str(code).strip()}
    if not code_set:
        raise ValueError("至少提供一个 --code")

    governance_catalog = stocking.ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, _ = await stocking.load_catalog_from_feishu(
        base_token=os.getenv("FABRIC_COLOR_CATALOG_BASE_TOKEN", stocking.DEFAULT_BASE_TOKEN),
        table_id=os.getenv("FABRIC_COLOR_CATALOG_TABLE_ID", stocking.DEFAULT_CATALOG_TABLE_ID),
        view_id=os.getenv("FABRIC_COLOR_CATALOG_VIEW_ID", stocking.DEFAULT_CATALOG_VIEW_ID),
    )
    target_catalog = [row for row in catalog_rows if row.fabric_name == fabric_name]
    index = stocking.CatalogIndex(target_catalog)

    manual_catalog = stocking.load_manual_mapping_catalog(manual_mapping)
    spu_catalog = load_spu_manual_mapping_catalog(spu_manual_mapping)

    forecasts, _ = stocking.load_forecast_skus(governance_catalog)
    forecast_by_sku = {row.sku: row for row in forecasts}
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

    op_qty: dict[tuple[str, int], int] = defaultdict(int)
    for (sku, stat_date), qty in operation_forecast_data.items():
        delta = final_export._month_delta(stat_date, now)
        if delta is not None:
            op_qty[(sku, delta)] += _q(qty)

    all_skus = set(purchase_order_data)
    all_skus.update(sku for sku, _ in effective_qty)
    all_skus.update(sku for sku, _ in operation_forecast_data)
    all_skus.update(forecast_by_sku)

    rows: list[dict[str, Any]] = []
    for sku in sorted(all_skus):
        forecast = final_export._fallback_forecast(
            sku,
            forecast_by_sku,
            resolver,
            snapshot_by_sku,
            governance_catalog,
        )
        code = str(forecast.color_code or "").strip().upper()
        if code not in code_set:
            continue

        spu = str(forecast.spu or "").strip()
        usage = fabric_usage.get((spu, fabric_name))
        if not usage:
            continue

        purchase_qty = _q(purchase_order_data.get(sku, 0))
        sys_qtys = [_q(effective_qty.get((sku, delta), 0)) for delta in range(4)]
        op_qtys = [_q(op_qty.get((sku, delta), 0)) for delta in range(4)]
        if purchase_qty == 0 and sum(sys_qtys) == 0 and sum(op_qtys) == 0:
            continue

        decision = final_export._resolve_final_color(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog,
            spu_catalog,
        )
        if decision.row:
            final_color = decision.row.identity[1]
            final_lx_code = decision.row.identity[2]
            decision_text = decision.method
        else:
            final_color = ""
            final_lx_code = ""
            decision_text = str(decision.reason or decision.reason_code or "待确认")

        spu_manual_entries = spu_catalog.by_key.get(
            (fabric_name, str(forecast.color_code or "").strip(), spu),
            (),
        )
        old_manual_entries = manual_catalog.active_by_key.get(
            (
                fabric_name,
                str(forecast.color_code or ""),
                str(forecast.color_name or ""),
                str(forecast.color_system or ""),
            ),
            (),
        )

        unit_usage = float(usage.get("单件用量") or 0)
        unit_loss = float(usage.get("单件损耗") or 0)
        sys_meters: list[float] = []
        for qty in sys_qtys:
            meters, _ = fabric_base._calc_usage_meters(
                qty,
                usage.get("单件用量"),
                usage.get("单件损耗"),
                fabric_name,
                spu,
                fabric_usage,
            )
            sys_meters.append(_m(meters))

        purchase_meters, _ = fabric_base._calc_usage_meters(
            purchase_qty,
            usage.get("单件用量"),
            usage.get("单件损耗"),
            fabric_name,
            spu,
            fabric_usage,
        )

        row = {
            "SKU": sku,
            "SPU": spu,
            "品名": forecast.product_name,
            "预测颜色中文名": forecast.color_name,
            "颜色体系": forecast.color_system,
            "原始颜色编码": forecast.color_code,
            "A2023治理中文名": _governance_names(governance_catalog, "A2023", code),
            "B2024治理中文名": _governance_names(governance_catalog, "B2024", code),
            "SPU人工目标色": _manual_targets(spu_manual_entries),
            "SPU人工确认来源": _join(getattr(e, "source", "") for e in spu_manual_entries),
            "历史四字段人工目标色": _manual_targets(old_manual_entries),
            "最终匹配颜色": final_color,
            "最终领星缩写": final_lx_code,
            "匹配方式/未确认原因": decision_text,
            "单件用量": unit_usage,
            "单件损耗": unit_loss,
            "当月已下单数量": purchase_qty,
            "当月已下单消耗/米": _m(purchase_meters),
            **{f"系统{month_labels[d]}数量": sys_qtys[d] for d in range(4)},
            **{f"系统{month_labels[d]}用量/米": sys_meters[d] for d in range(4)},
            **{f"运营{month_labels[d]}数量": op_qtys[d] for d in range(4)},
            "系统未来4月用量/米": _m(sum(sys_meters)),
        }
        rows.append(row)

    print("\n===== 当前飞书面料颜色清单 =====")
    print(f"面料: {fabric_name} | 有效颜色数: {len(index.by_fabric.get(fabric_name, ())) }生")
    for row in index.by_fabric.get(fabric_name, ()):
        print(
            f"  {row.color_name} | 领星新颜色缩写={row.lingxing_code or '-'} | "
            f"record_id={'、'.join(row.record_ids) or '-'}"
        )

    print("\n===== 待查编码汇总 =====")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["原始颜色编码"] or "").strip().upper()].append(row)

    for code in codes:
        code_u = str(code).strip().upper()
        code_rows = grouped.get(code_u, [])
        print(f"\n[{code_u}] 有需求SKU={len(code_rows)}")
        if not code_rows:
            print("  当前目标面料没有活跃需求SKU")
            continue
        print(f"  SPU={_join(row['SPU'] for row in code_rows)}")
        print(f"  颜色体系={_join(row['颜色体系'] for row in code_rows)}")
        print(f"  品名颜色={_join(row['预测颜色中文名'] for row in code_rows)}")
        print(f"  A2023治理中文名={_join(row['A2023治理中文名'] for row in code_rows)}")
        print(f"  B2024治理中文名={_join(row['B2024治理中文名'] for row in code_rows)}")
        print(f"  SPU人工目标色={_join(row['SPU人工目标色'] for row in code_rows) or '-'}")
        print(f"  历史人工目标色={_join(row['历史四字段人工目标色'] for row in code_rows) or '-'}")
        print(f"  当前最终匹配={_join(row['最终匹配颜色'] for row in code_rows) or '-'}")
        print(f"  原因={_join(row['匹配方式/未确认原因'] for row in code_rows)}")
        print(f"  系统未来4月={sum(float(row['系统未来4月用量/米'] or 0) for row in code_rows):.2f} 米")

    print("\n===== SPU人工目标色在当前飞书316清单中的状态 =====")
    target_names = {row.color_name for row in index.by_fabric.get(fabric_name, ())}
    seen_manual: set[tuple[str, str, str, str]] = set()
    for row in rows:
        manual_targets = str(row["SPU人工目标色"] or "")
        if not manual_targets:
            continue
        for target in manual_targets.split("、"):
            key = (str(row["原始颜色编码"]), str(row["SPU"]), target, str(row["颜色体系"]))
            if key in seen_manual:
                continue
            seen_manual.add(key)
            status = "在当前清单" if target in target_names else "不在当前清单"
            print(
                f"{key[0]} | SPU={key[1]} | system={key[3]} | 人工目标={target} | {status}"
            )

    output.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with output.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    else:
        output.write_text("无活跃需求SKU\n", encoding="utf-8-sig")
    print(f"\n诊断CSV: {output}")
    print(f"明细行数: {len(rows)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="只读诊断面料待确认颜色的完整上下文")
    parser.add_argument("--fabric", required=True)
    parser.add_argument("--code", action="append", required=True)
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
        default=Path("/opt/apps/pythondata/exports/pending_fabric_color_context.csv"),
    )
    args = parser.parse_args()
    asyncio.run(run(
        fabric_name=args.fabric,
        codes=args.code,
        output=args.output,
        manual_mapping=args.manual_mapping,
        spu_manual_mapping=args.spu_manual_mapping,
    ))


if __name__ == "__main__":
    main()
