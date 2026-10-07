#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only preview of NEW_VISIBLE production procurement overlay.

Runs the same data-loading/build/overlay path as the production color procurement
entrypoint, but performs NO database writes and NO Feishu writes.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime

from jobs.feishu import generate_procurement_report as base
from jobs.feishu import procurement_color_logic as logic
from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu import new_visible_procurement_bridge as nv_bridge
from jobs.feishu import new_visible_procurement_overlay as nv_overlay


def main() -> int:
    current_date = datetime.now()
    resolver = ColorSystemResolver.from_database()

    forecast_map, month_order = logic.read_system_forecast(resolver, current_date)
    if not forecast_map:
        raise RuntimeError("当前月起未来4个月没有系统预测数据")

    inventory_map = logic.read_inventory(resolver)
    factory_map = base.read_last_factory()
    op_forecast_map = base.read_op_forecast_by_month()
    fabric_info = logic.read_fabric_info()
    recs = nv_bridge.load_recommendations(current_date)

    legacy_orders, _legacy_fabric = logic.build_reports(
        forecast_map=forecast_map,
        month_order=month_order,
        inventory_map=inventory_map,
        fabric_info=fabric_info,
        factory_map=factory_map,
        op_forecast_map=op_forecast_map,
    )

    before = defaultdict(int)
    for row in legacy_orders:
        before[(str(row.get("SPU") or ""), str(row.get("店铺") or ""))] += int(
            row.get("建议下单量") or 0
        )

    orders, _fabric, summary = nv_overlay.apply_new_visible_overlay(
        order_records=legacy_orders,
        forecast_map=forecast_map,
        month_order=month_order,
        fabric_info=fabric_info,
        recommendations=recs,
    )

    after = defaultdict(int)
    for row in orders:
        after[(str(row.get("SPU") or ""), str(row.get("店铺") or ""))] += int(
            row.get("建议下单量") or 0
        )

    rows = []
    for key, rec in recs.items():
        expected = max(0, int(rec.get("recommended_qty_q50") or 0))
        got = int(after.get(key, 0))
        rows.append({
            "store_name": key[1],
            "spu": key[0],
            "fabric_type": str(rec.get("fabric_type") or ""),
            "recommendation_status": str(rec.get("recommendation_status") or ""),
            "legacy_qty": int(before.get(key, 0)),
            "champion_q50_qty": expected,
            "preview_color_sum": got,
            "delta_vs_legacy": got - int(before.get(key, 0)),
            "parity_ok": got == expected,
        })

    mismatch = [r for r in rows if not r["parity_ok"]]
    print("NV_PROD_PREVIEW_SCOPE=" + json.dumps({
        **summary,
        "rows": len(rows),
        "parity_mismatch_groups": len(mismatch),
        "read_only": True,
        "db_written": False,
        "feishu_written": False,
        "automatic_po_written": False,
    }, ensure_ascii=False))

    for r in sorted(
        rows,
        key=lambda x: (
            0 if x["recommendation_status"] == "ORDER_NOW_LT48" else
            1 if x["recommendation_status"] == "ORDER_WITHIN_30D" else 2,
            -x["champion_q50_qty"],
        ),
    )[:100]:
        print("NV_PROD_PREVIEW_ROW=" + json.dumps(r, ensure_ascii=False))

    if mismatch:
        raise RuntimeError(
            f"NEW_VISIBLE preview parity failed: {len(mismatch)} groups"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
