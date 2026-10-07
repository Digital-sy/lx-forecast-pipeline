#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Classify NEW_VISIBLE H60 inventory gaps by production fabric type.

Shadow-only. Reuses jobs.feishu.procurement_color_logic.read_fabric_info so fabric
classification matches current production procurement logic.

Outputs:
- stock fabric rows -> H60 coverage-gap shadow
- custom fabric rows -> held for H90 research
- unknown fabric rows -> blocked
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from common.database import db_cursor
from jobs.feishu.procurement_color_logic import read_fabric_info

H60_TABLE = "forecast_new_visible_h60_inventory_coverage_daily"
H48_RISK_TABLE = "forecast_new_visible_h48_leadtime_risk_daily"
DEST_TABLE = "forecast_new_visible_h60_fabric_shadow_daily"
BUILD_VERSION = "NV_H60_FABRIC_SHADOW_V1"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def table_exists(name: str) -> bool:
    r = one(
        "SELECT COUNT(*) AS n FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
        (name,),
    )
    return int(r.get("n", 0) or 0) > 0


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
              snapshot_date DATE NOT NULL,
              store_name VARCHAR(200) NOT NULL,
              spu VARCHAR(200) NOT NULL,
              age_days INT NOT NULL,
              fabric_type VARCHAR(30) NOT NULL,
              primary_fabric VARCHAR(255) NOT NULL DEFAULT '',
              h60_coverage_status VARCHAR(50) NOT NULL,
              h48_risk_level VARCHAR(50) NOT NULL DEFAULT '',
              q50_gap_low DECIMAL(18,2) NOT NULL DEFAULT 0,
              q50_gap_high DECIMAL(18,2) NOT NULL DEFAULT 0,
              q75_gap_low DECIMAL(18,2) NOT NULL DEFAULT 0,
              q75_gap_high DECIMAL(18,2) NOT NULL DEFAULT 0,
              shadow_status VARCHAR(60) NOT NULL,
              priority_tier VARCHAR(20) NOT NULL,
              build_version VARCHAR(100) NOT NULL,
              materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (snapshot_date,store_name,spu),
              INDEX idx_nv_h60_fabric (snapshot_date,fabric_type),
              INDEX idx_nv_h60_priority (snapshot_date,priority_tier)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def priority(h48: str, h60: str) -> str:
    if h48 == "CRITICAL_LT_SHORTAGE" or h60 == "BELOW_Q50_EVEN_WITH_PENDING":
        return "P0"
    if h48 == "HIGH_LT_RISK" or h60 == "Q50_DEPENDS_ON_PENDING":
        return "P1"
    if h60 in ("Q50_COVERED_Q75_SHORT", "Q75_DEPENDS_ON_PENDING"):
        return "P2"
    if h60 == "Q75_COVERED_ON_HAND":
        return "P3"
    return "WATCH"


def main() -> int:
    if not table_exists(H60_TABLE):
        raise RuntimeError(f"{H60_TABLE} missing")
    if not table_exists(H48_RISK_TABLE):
        raise RuntimeError(f"{H48_RISK_TABLE} missing")

    d60 = one(f"SELECT MAX(snapshot_date) AS d FROM {H60_TABLE}").get("d")
    d48 = one(f"SELECT MAX(snapshot_date) AS d FROM {H48_RISK_TABLE}").get("d")
    if not d60 or str(d60) != str(d48):
        raise RuntimeError(f"snapshot mismatch: H60={d60}, H48={d48}")

    rows = q(
        f"""
        SELECT
          h.snapshot_date,h.store_name,h.spu,h.age_days,
          h.coverage_status,
          h.q50_gap_if_all_pending_arrives,
          h.q50_gap_if_no_pending_arrives,
          h.q75_gap_if_all_pending_arrives,
          h.q75_gap_if_no_pending_arrives,
          r.risk_level AS h48_risk_level
        FROM {H60_TABLE} h
        LEFT JOIN {H48_RISK_TABLE} r
          ON r.snapshot_date=h.snapshot_date
         AND r.store_name=h.store_name
         AND r.spu=h.spu
        WHERE h.snapshot_date=%s
        """,
        (d60,),
    )

    fabric_info = read_fabric_info()
    out = []
    counts = {}
    fabric_counts = {}

    for r in rows:
        spu = str(r.get("spu") or "").strip().upper()
        info = fabric_info.get(spu)
        if info:
            fabric_type = str(info.get("fabric_type") or "UNKNOWN")
            fabrics = info.get("fabrics") or []
            primary_fabric = str(fabrics[0][0]) if fabrics else ""
        else:
            fabric_type = "UNKNOWN"
            primary_fabric = ""

        h60_status = str(r.get("coverage_status") or "")
        h48 = str(r.get("h48_risk_level") or "")
        tier = priority(h48, h60_status)

        if fabric_type == "现货面料":
            shadow_status = "STOCK_H60_READY"
        elif fabric_type == "定制面料":
            shadow_status = "CUSTOM_HOLD_H90_RESEARCH"
        else:
            shadow_status = "BLOCK_FABRIC_TYPE_UNKNOWN"

        counts[shadow_status] = counts.get(shadow_status, 0) + 1
        fabric_counts[fabric_type] = fabric_counts.get(fabric_type, 0) + 1

        out.append({
            "snapshot_date": r["snapshot_date"],
            "store_name": r["store_name"],
            "spu": r["spu"],
            "age_days": int(r.get("age_days", 0) or 0),
            "fabric_type": fabric_type,
            "primary_fabric": primary_fabric,
            "h60_coverage_status": h60_status,
            "h48_risk_level": h48,
            "q50_gap_low": float(r.get("q50_gap_if_all_pending_arrives") or 0),
            "q50_gap_high": float(r.get("q50_gap_if_no_pending_arrives") or 0),
            "q75_gap_low": float(r.get("q75_gap_if_all_pending_arrives") or 0),
            "q75_gap_high": float(r.get("q75_gap_if_no_pending_arrives") or 0),
            "shadow_status": shadow_status,
            "priority_tier": tier,
            "build_version": BUILD_VERSION,
        })

    print("H60_FABRIC_SHADOW_SCOPE=" + json.dumps({
        "snapshot_date": str(d60),
        "rows": len(out),
        "fabric_counts": fabric_counts,
        "shadow_status_counts": counts,
        "production_tables_written": False,
    }, ensure_ascii=False))

    stock = [
        x for x in out
        if x["shadow_status"] == "STOCK_H60_READY"
        and (x["q50_gap_high"] > 0 or x["q75_gap_high"] > 0)
    ]
    rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "WATCH": 4}
    stock.sort(
        key=lambda x: (
            rank.get(x["priority_tier"], 9),
            -x["q50_gap_low"],
            -x["q75_gap_low"],
            -x["q50_gap_high"],
        )
    )
    for x in stock[:50]:
        print("H60_FABRIC_SHADOW_TOP=" + json.dumps(
            x, ensure_ascii=False, default=str
        ))

    custom = [x for x in out if x["shadow_status"] == "CUSTOM_HOLD_H90_RESEARCH"]
    for x in custom[:20]:
        print("H60_FABRIC_SHADOW_CUSTOM_HOLD=" + json.dumps(
            {
                "store_name": x["store_name"],
                "spu": x["spu"],
                "primary_fabric": x["primary_fabric"],
                "priority_tier": x["priority_tier"],
                "h60_coverage_status": x["h60_coverage_status"],
                "h48_risk_level": x["h48_risk_level"],
            },
            ensure_ascii=False,
            default=str,
        ))

    ensure_table()
    if out:
        cols = list(out[0].keys())
        sql = (
            f"INSERT INTO {DEST_TABLE} ({','.join(cols)}) VALUES "
            f"({','.join(['%s']*len(cols))}) ON DUPLICATE KEY UPDATE "
            + ",".join(
                f"{c}=VALUES({c})"
                for c in cols
                if c not in ("snapshot_date","store_name","spu")
            )
        )
        payload = [tuple(x.get(c) for c in cols) for x in out]
        with db_cursor() as c:
            c.executemany(sql, payload)

    print("H60_FABRIC_SHADOW_PERSISTED=" + json.dumps({
        "table": DEST_TABLE,
        "snapshot_date": str(d60),
        "rows": len(out),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
