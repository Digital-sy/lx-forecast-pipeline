#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Overlay approved NEW_VISIBLE H48/H60 recommendations onto legacy color procurement.

The legacy procurement engine still builds the color universe, inventory and monthly
shape. This overlay replaces ONLY validated NEW_VISIBLE SPU+shop procurement quantity.

Rules:
- stock NEW_VISIBLE: authoritative SPU lot = recommended H60 Q50 (or zero when not due);
- custom NEW_VISIBLE: zero quantity (H90 HOLD);
- blocked NEW_VISIBLE: zero quantity;
- ages outside validated 7..120 are absent from the bridge and therefore keep legacy;
- color allocation is exact and sums back to the SPU recommendation.

No PO is created here.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from jobs.feishu import procurement_color_logic as logic

ProcurementKey = logic.ProcurementKey
RecommendationKey = Tuple[str, str]


def _rebuild_custom_fabric_records(
    order_records: Sequence[Mapping[str, Any]],
    fabric_info: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    agg: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "spu_set": set(),
            "建议下单量合计": 0,
            "原始单耗加权和": 0.0,
            "预计用量(米)": 0.0,
        }
    )

    for row in order_records:
        if str(row.get("面料类型") or "") != "定制面料":
            continue
        qty = max(0, int(row.get("建议下单量") or 0))
        if qty <= 0:
            continue
        spu = str(row.get("SPU") or "").strip()
        info = fabric_info.get(spu, {})
        for fabric, unit_usage, unit_loss in info.get("fabrics", []):
            if float(unit_usage or 0) <= 0:
                continue
            usage = float(unit_usage)
            loss = float(unit_loss or 1.0)
            if loss <= 0:
                loss = 1.0
            meters = qty * usage * loss
            bucket = agg[str(fabric)]
            bucket["spu_set"].add(spu)
            bucket["建议下单量合计"] += qty
            bucket["原始单耗加权和"] += qty * usage
            bucket["预计用量(米)"] += meters

    out: List[Dict[str, Any]] = []
    for fabric, bucket in sorted(
        agg.items(), key=lambda item: item[1]["预计用量(米)"], reverse=True
    ):
        qty = int(bucket["建议下单量合计"] or 0)
        meters = float(bucket["预计用量(米)"] or 0)
        out.append({
            "面料": fabric,
            "SPU数量": len(bucket["spu_set"]),
            "建议下单量合计": qty,
            "单件用量(米)": round(meters / qty if qty else 0.0, 3),
            "原始单耗加权均值": round(
                float(bucket["原始单耗加权和"] or 0) / qty if qty else 0.0,
                3,
            ),
            "预计用量(米)": round(meters, 2),
            "计算口径": "Σ(建议下单量×单件用量×单件损耗)",
        })
    return out


def apply_new_visible_overlay(
    order_records: List[Dict[str, Any]],
    forecast_map: Mapping[ProcurementKey, Mapping[str, int]],
    month_order: Sequence[str],
    fabric_info: Mapping[str, Mapping[str, Any]],
    recommendations: Mapping[RecommendationKey, Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    if not recommendations:
        return (
            order_records,
            _rebuild_custom_fabric_records(order_records, fabric_info),
            {
                "mode": "legacy",
                "override_groups": 0,
                "expected_q50_sum": 0,
                "allocated_q50_sum": 0,
                "missing_groups": [],
            },
        )

    groups: Dict[RecommendationKey, List[int]] = defaultdict(list)
    for idx, row in enumerate(order_records):
        key = (
            str(row.get("SPU") or "").strip(),
            str(row.get("店铺") or "").strip(),
        )
        groups[key].append(idx)

    expected_sum = 0
    allocated_sum = 0
    missing_groups: List[Dict[str, Any]] = []
    overridden = 0

    for group_key, rec in recommendations.items():
        indices = groups.get(group_key, [])
        expected = max(0, int(rec.get("recommended_qty_q50") or 0))
        expected_sum += expected
        if not indices:
            if expected > 0:
                missing_groups.append({
                    "SPU": group_key[0],
                    "店铺": group_key[1],
                    "expected_qty": expected,
                })
            continue

        rec_fabric = str(rec.get("fabric_type") or "UNKNOWN")
        row_fabrics = {
            str(order_records[idx].get("面料类型") or "")
            for idx in indices
        }
        status = str(rec.get("recommendation_status") or "")
        if rec_fabric != "UNKNOWN" and row_fabrics != {rec_fabric}:
            expected = 0
            status = "BLOCK_FABRIC_TYPE_MISMATCH"

        # Old color-level net recommendation is the best first allocation weight
        # because it already reflects color demand minus color inventory/pending.
        net_weights = {
            str(pos): max(
                0, int(order_records[idx].get("建议下单量") or 0)
            )
            for pos, idx in enumerate(indices)
        }

        # If legacy net needs are all zero, fall back to first-2-month color demand.
        demand_weights: Dict[str, float] = {}
        for pos, idx in enumerate(indices):
            row = order_records[idx]
            pkey: ProcurementKey = (
                str(row.get("SPU") or ""),
                str(row.get("颜色体系") or ""),
                str(row.get("颜色缩写") or ""),
                str(row.get("店铺") or ""),
            )
            monthly = forecast_map.get(pkey, {})
            demand_weights[str(pos)] = sum(
                int(monthly.get(month, 0) or 0)
                for month in month_order[:logic.COVERAGE_MONTHS_STOCK]
            )

        weights = (
            net_weights
            if sum(net_weights.values()) > 0
            else demand_weights
        )
        color_alloc = logic.largest_remainder_allocate(expected, weights)

        group_allocated = 0
        for pos, idx in enumerate(indices):
            row = order_records[idx]
            qty = int(color_alloc.get(str(pos), 0))
            group_allocated += qty

            pkey: ProcurementKey = (
                str(row.get("SPU") or ""),
                str(row.get("颜色体系") or ""),
                str(row.get("颜色缩写") or ""),
                str(row.get("店铺") or ""),
            )
            monthly = forecast_map.get(pkey, {})
            coverage = (
                logic.COVERAGE_MONTHS_CUSTOM
                if str(row.get("面料类型") or "") == "定制面料"
                else logic.COVERAGE_MONTHS_STOCK
            )
            selected_months = list(month_order[:coverage])
            month_weights = {
                month: int(monthly.get(month, 0) or 0)
                for month in selected_months
            }
            month_alloc = logic.largest_remainder_allocate(
                qty, month_weights
            )
            for month in month_order:
                row[f"{month}建议下单"] = int(month_alloc.get(month, 0))

            row["建议下单合计"] = qty
            row["建议下单量"] = qty

            # In-memory trace fields. Current production DB schema remains unchanged;
            # the authoritative audit trail is the recommendation bridge table.
            row["预测模型"] = str(
                rec.get("model_version")
                or "NV_PROCUREMENT_CHAMPION_V1_H48_H60_Q50"
            )
            row["新品动作状态"] = status
            row["新品H48风险"] = str(rec.get("h48_risk_level") or "")
            row["新品H60_Q50"] = float(rec.get("h60_q50") or 0)
            row["新品H60_Q75"] = float(rec.get("h60_q75") or 0)
            row["新品最晚下单日"] = rec.get("latest_order_date")
            row["新品推荐快照"] = rec.get("snapshot_date")

        if group_allocated != expected:
            raise RuntimeError(
                f"NEW_VISIBLE color allocation mismatch: {group_key}, "
                f"expected={expected}, allocated={group_allocated}"
            )

        allocated_sum += group_allocated
        overridden += 1

    if missing_groups:
        raise RuntimeError(
            "NEW_VISIBLE positive recommendation missing from legacy color universe: "
            + str(missing_groups[:20])
        )

    if allocated_sum != expected_sum:
        raise RuntimeError(
            f"NEW_VISIBLE allocation total mismatch: "
            f"expected={expected_sum}, allocated={allocated_sum}"
        )

    fabric_records = _rebuild_custom_fabric_records(order_records, fabric_info)
    return order_records, fabric_records, {
        "mode": "primary",
        "override_groups": overridden,
        "expected_q50_sum": expected_sum,
        "allocated_q50_sum": allocated_sum,
        "missing_groups": [],
    }
