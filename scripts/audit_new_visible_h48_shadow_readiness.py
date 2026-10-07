#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Audit live-input readiness for NEW_VISIBLE H48 procurement shadow.

Research-only. Read-only. No production or shadow table writes.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Set

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as mon
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research import build_new_visible_snapshots as hist
from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as stage1

SPECIAL_EXCLUSION_TABLE = "forecast_special_spu_exclusion"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def cols(table: str) -> Set[str]:
    return set(mon.get_columns(table)) if mon.table_exists(table) else set()


def main() -> int:
    required = [
        hist.DAILY_TABLE,
        mon.FEATURE_SNAPSHOT_TABLE,
        mon.INVENTORY_SNAPSHOT_TABLE,
        SPECIAL_EXCLUSION_TABLE,
    ]
    status = [
        {
            "table": t,
            "exists": mon.table_exists(t),
            "columns_n": len(cols(t)) if mon.table_exists(t) else 0,
        }
        for t in required
    ]
    print("H48_SHADOW_READINESS_SCOPE=" + json.dumps({
        "target_shops": list(TARGET_SHOPS),
        "h48_model": "NV-ML-V1-STAGE2-DIRECT48-CORE",
        "required_tables": status,
        "no_db_write": True,
    }, ensure_ascii=False))

    missing_tables = [x["table"] for x in status if not x["exists"]]
    if missing_tables:
        print("H48_SHADOW_READINESS_BLOCKER=" + json.dumps({
            "reason": "missing_required_tables",
            "tables": missing_tables,
        }, ensure_ascii=False))
        return 2

    daily = one(
        "SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt, COUNT(*) AS rows_n, "
        "COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS shop_spu_n "
        "FROM " + hist.DAILY_TABLE + " "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    )
    print("H48_SHADOW_DAILY_HISTORY=" + json.dumps({
        "min_dt": str(daily.get("min_dt") or ""),
        "max_dt": str(daily.get("max_dt") or ""),
        "rows_n": int(daily.get("rows_n", 0) or 0),
        "shop_spu_n": int(daily.get("shop_spu_n", 0) or 0),
    }, ensure_ascii=False))

    feature_latest = one(
        "SELECT MAX(snapshot_date) AS d FROM " + mon.FEATURE_SNAPSHOT_TABLE + " "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    ).get("d")
    inventory_latest = one(
        "SELECT MAX(snapshot_date) AS d FROM " + mon.INVENTORY_SNAPSHOT_TABLE + " "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    ).get("d")

    fcols = cols(mon.FEATURE_SNAPSHOT_TABLE)
    derivable = {"age_days", "launch_month", "snapshot_month"}
    required_core = set(abl.CORE_NUMERIC) | set(stage1.CATEGORICAL_FEATURES)
    missing_core = sorted(required_core - fcols - derivable)

    print("H48_SHADOW_LIVE_FEATURE_SCHEMA=" + json.dumps({
        "feature_snapshot_date": str(feature_latest or ""),
        "inventory_snapshot_date": str(inventory_latest or ""),
        "required_core_n": len(required_core),
        "directly_present_n": len(required_core & fcols),
        "missing_but_derivable": sorted((required_core - fcols) & derivable),
        "missing_core": missing_core,
        "compatible_as_is": len(missing_core) == 0,
    }, ensure_ascii=False))

    if not feature_latest or not inventory_latest:
        print("H48_SHADOW_READINESS_BLOCKER=" + json.dumps({
            "reason": "missing_latest_snapshot",
            "feature_latest": str(feature_latest or ""),
            "inventory_latest": str(inventory_latest or ""),
        }, ensure_ascii=False))
        return 3

    nv = q(
        "SELECT store_name, spu, first_sale_date "
        "FROM " + mon.FEATURE_SNAPSHOT_TABLE + " "
        "WHERE snapshot_date=%s AND store_name IN (%s,%s,%s,%s) "
        "AND forecastability=%s",
        (feature_latest, *TARGET_SHOPS, "NEW_VISIBLE"),
    )
    nv_keys = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip())
        for r in nv
        if r.get("store_name") and r.get("spu")
    }

    ex = q(
        "SELECT DISTINCT UPPER(TRIM(spu)) AS spu "
        "FROM " + SPECIAL_EXCLUSION_TABLE + " "
        "WHERE exclusion_code IN (%s,%s)",
        ("LCS_SPECIAL_LOW_PRICE", "XH_PREFIX_EXCLUSION"),
    )
    excluded = {str(r.get("spu") or "").strip().upper() for r in ex}
    eligible = {
        (shop, spu)
        for shop, spu in nv_keys
        if spu.upper() not in excluded and not spu.upper().startswith("XH")
    }

    inv = q(
        "SELECT store_name, spu, "
        "SUM(afn_fulfillable_quantity) AS fulfillable, "
        "SUM(afn_reserved_quantity) AS reserved, "
        "SUM(reserved_fc_transfers) AS transfers, "
        "SUM(afn_inbound_shipped_quantity) AS inbound_shipped, "
        "SUM(afn_inbound_receiving_quantity) AS inbound_receiving, "
        "SUM(fba_total_inventory) AS total_inventory, "
        "SUM(fba_available_inventory) AS available_inventory "
        "FROM " + mon.INVENTORY_SNAPSHOT_TABLE + " "
        "WHERE snapshot_date=%s AND store_name IN (%s,%s,%s,%s) "
        "AND spu IS NOT NULL AND TRIM(spu)<>'' "
        "GROUP BY store_name, spu",
        (inventory_latest, *TARGET_SHOPS),
    )
    inv_map = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in inv
    }
    covered = eligible & set(inv_map)
    missing_inv = sorted(eligible - set(inv_map))

    sums = {
        "fulfillable": 0.0,
        "reserved": 0.0,
        "transfers": 0.0,
        "inbound_shipped": 0.0,
        "inbound_receiving": 0.0,
    }
    for key in covered:
        row = inv_map[key]
        for field in sums:
            sums[field] += float(row.get(field, 0) or 0)

    coverage = len(covered) / len(eligible) if eligible else None
    print("H48_SHADOW_CURRENT_NV_SCOPE=" + json.dumps({
        "current_new_visible_shop_spu": len(nv_keys),
        "business_excluded_shop_spu": len(nv_keys - eligible),
        "eligible_new_visible_shop_spu": len(eligible),
        "inventory_covered_shop_spu": len(covered),
        "inventory_coverage": round(coverage, 6) if coverage is not None else None,
        "missing_inventory_shop_spu_n": len(missing_inv),
        "missing_inventory_sample": [
            {"store_name": x[0], "spu": x[1]} for x in missing_inv[:20]
        ],
        "inventory_component_sums": {k: round(v, 2) for k, v in sums.items()},
    }, ensure_ascii=False))

    blockers = []
    if missing_core:
        blockers.append("live_feature_snapshot_missing_frozen_CORE_fields")
    if not eligible:
        blockers.append("no_current_NEW_VISIBLE_after_business_exclusions")
    if coverage is not None and coverage < 0.95:
        blockers.append("inventory_coverage_below_95pct")

    print("H48_SHADOW_READINESS_DECISION=" + json.dumps({
        "ready_for_direct_scoring_from_existing_feature_snapshot": not missing_core,
        "ready_for_procurement_shadow": len(blockers) == 0,
        "blockers": blockers,
        "next_if_feature_blocked": (
            "build live CORE point-in-time features from fresh daily history; "
            "do not silently substitute missing training features"
        ),
        "next_if_ready": (
            "score H48 Q50/Q75 and join current inventory/inbound in shadow only"
        ),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
