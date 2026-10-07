#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Build NEW_VISIBLE H48 lead-time inventory risk shadow.

This is NOT a purchase-order quantity model.

H48 demand covers the normal 20d production + 28d sea-freight lead time. A shortage
inside that window cannot be solved by a normal order placed today, so the correct
output is lead-time coverage risk / expedite-transfer risk.

Purchase quantity after arrival requires a separate post-arrival review horizon.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from common.database import db_cursor

PRED_TABLE = "forecast_new_visible_h48_prediction_daily"
INV_TABLE = "forecast_new_visible_inventory_position_daily"
DEST_TABLE = "forecast_new_visible_h48_leadtime_risk_daily"
BUILD_VERSION = "NV_H48_LEADTIME_RISK_V1"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
              snapshot_date DATE NOT NULL,
              store_name VARCHAR(200) NOT NULL,
              spu VARCHAR(200) NOT NULL,
              age_days INT NOT NULL,
              checkpoint_validated TINYINT(1) NOT NULL DEFAULT 0,
              shadow_quantity_eligible TINYINT(1) NOT NULL DEFAULT 0,

              selected_calibration VARCHAR(40) NOT NULL,
              h48_q50 DECIMAL(18,2) NOT NULL DEFAULT 0,
              h48_q75 DECIMAL(18,2) NOT NULL DEFAULT 0,

              on_hand_position DECIMAL(18,2) NOT NULL DEFAULT 0,
              total_inventory_position DECIMAL(18,2) NOT NULL DEFAULT 0,

              q50_gap_on_hand DECIMAL(18,2) NOT NULL DEFAULT 0,
              q75_gap_on_hand DECIMAL(18,2) NOT NULL DEFAULT 0,
              q50_gap_total_position DECIMAL(18,2) NOT NULL DEFAULT 0,
              q75_gap_total_position DECIMAL(18,2) NOT NULL DEFAULT 0,

              on_hand_days_cover_q50 DECIMAL(16,4) DEFAULT NULL,
              total_position_days_cover_q50 DECIMAL(16,4) DEFAULT NULL,

              risk_level VARCHAR(40) NOT NULL,
              risk_reason VARCHAR(200) NOT NULL,
              inventory_source_status VARCHAR(100) NOT NULL,
              inventory_usable TINYINT(1) NOT NULL DEFAULT 0,
              normal_po_quantity_generated TINYINT(1) NOT NULL DEFAULT 0,
              build_version VARCHAR(100) NOT NULL,
              materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (snapshot_date,store_name,spu),
              INDEX idx_h48_risk (snapshot_date,risk_level),
              INDEX idx_h48_risk_shop (snapshot_date,store_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def safe_days(position: float, demand48: float):
    if demand48 <= 0:
        return None
    return 48.0 * position / demand48


def main() -> int:
    pd = one(f"SELECT MAX(snapshot_date) AS d FROM {PRED_TABLE}").get("d")
    invd = one(f"SELECT MAX(snapshot_date) AS d FROM {INV_TABLE}").get("d")
    if not pd or not invd:
        raise RuntimeError("prediction/inventory shadow missing")
    if str(pd) != str(invd):
        raise RuntimeError(f"snapshot date mismatch: prediction={pd}, inventory={invd}")

    rows = q(
        f"""
        SELECT
          p.snapshot_date,p.store_name,p.spu,p.age_days,
          p.checkpoint_validated,p.shadow_quantity_eligible,p.action_eligible,
          p.global_q50,p.global_q75,p.age_q50,p.age_q75,
          i.on_hand_position,i.total_inventory_position,
          i.inventory_usable,i.source_status
        FROM {PRED_TABLE} p
        LEFT JOIN {INV_TABLE} i
          ON i.snapshot_date=p.snapshot_date
         AND i.store_name=p.store_name
         AND i.spu=p.spu
        WHERE p.snapshot_date=%s
        """,
        (pd,),
    )

    out = []
    counts = {}
    for r in rows:
        validated = int(r.get("checkpoint_validated", 0) or 0)
        shadow_eligible = int(r.get("shadow_quantity_eligible", 0) or 0)
        inventory_usable = int(r.get("inventory_usable", 0) or 0)

        # Exact validated checkpoint -> AGE calibration has the closest lifecycle
        # coverage semantics. Interpolated ages -> use GLOBAL as the more stable
        # general calibration and keep AGE only as diagnostic in prediction table.
        if validated and r.get("age_q50") is not None and r.get("age_q75") is not None:
            cal = "AGE_EXACT_CHECKPOINT"
            q50 = float(r.get("age_q50") or 0)
            q75 = float(r.get("age_q75") or 0)
        else:
            cal = "GLOBAL_INTERPOLATED"
            q50 = float(r.get("global_q50") or 0)
            q75 = float(r.get("global_q75") or 0)

        q75 = max(q75, q50)
        on_hand = float(r.get("on_hand_position") or 0)
        total = float(r.get("total_inventory_position") or 0)

        g50_on = max(q50 - on_hand, 0.0)
        g75_on = max(q75 - on_hand, 0.0)
        g50_total = max(q50 - total, 0.0)
        g75_total = max(q75 - total, 0.0)

        if not shadow_eligible:
            level = "WATCH_ONLY"
            reason = "age outside validated 7-120 shadow range"
        elif not inventory_usable:
            level = "BLOCKED_DATA"
            reason = "inventory evidence incomplete; no quantity risk decision"
        elif total < q50:
            level = "CRITICAL_LT_SHORTAGE"
            reason = "total inventory position below H48 Q50; normal sea PO cannot prevent lead-time shortage"
        elif total < q75:
            level = "HIGH_LT_RISK"
            reason = "covers H48 Q50 but not Q75 safety demand"
        elif on_hand < q50:
            level = "INBOUND_DEPENDENT"
            reason = "on-hand below Q50 but total position covers Q75; timing of inbound is critical"
        else:
            level = "COVERED_Q75"
            reason = "current inventory position covers H48 Q75"

        counts[level] = counts.get(level, 0) + 1
        out.append({
            "snapshot_date": r["snapshot_date"],
            "store_name": r["store_name"],
            "spu": r["spu"],
            "age_days": int(r.get("age_days", 0) or 0),
            "checkpoint_validated": validated,
            "shadow_quantity_eligible": shadow_eligible,
            "selected_calibration": cal,
            "h48_q50": q50,
            "h48_q75": q75,
            "on_hand_position": on_hand,
            "total_inventory_position": total,
            "q50_gap_on_hand": g50_on,
            "q75_gap_on_hand": g75_on,
            "q50_gap_total_position": g50_total,
            "q75_gap_total_position": g75_total,
            "on_hand_days_cover_q50": safe_days(on_hand, q50),
            "total_position_days_cover_q50": safe_days(total, q50),
            "risk_level": level,
            "risk_reason": reason,
            "inventory_source_status": str(r.get("source_status") or "MISSING"),
            "inventory_usable": inventory_usable,
            "normal_po_quantity_generated": 0,
            "build_version": BUILD_VERSION,
        })

    print("H48_LEADTIME_RISK_SCOPE=" + json.dumps({
        "snapshot_date": str(pd),
        "rows": len(out),
        "risk_counts": counts,
        "normal_po_quantity_generated": False,
        "reason": "H48 is lead-time demand; standard PO sizing needs post-arrival review horizon",
    }, ensure_ascii=False))

    ranked = sorted(
        [x for x in out if x["risk_level"] in ("CRITICAL_LT_SHORTAGE","HIGH_LT_RISK","INBOUND_DEPENDENT")],
        key=lambda x: (
            0 if x["risk_level"] == "CRITICAL_LT_SHORTAGE" else
            1 if x["risk_level"] == "HIGH_LT_RISK" else 2,
            -x["q75_gap_total_position"],
        ),
    )
    for x in ranked[:30]:
        print("H48_LEADTIME_RISK_TOP=" + json.dumps({
            k: x[k] for k in (
                "store_name","spu","age_days","selected_calibration",
                "h48_q50","h48_q75","on_hand_position","total_inventory_position",
                "q50_gap_total_position","q75_gap_total_position",
                "total_position_days_cover_q50","risk_level"
            )
        }, ensure_ascii=False))

    ensure_table()
    cols = [
        "snapshot_date","store_name","spu","age_days",
        "checkpoint_validated","shadow_quantity_eligible",
        "selected_calibration","h48_q50","h48_q75",
        "on_hand_position","total_inventory_position",
        "q50_gap_on_hand","q75_gap_on_hand",
        "q50_gap_total_position","q75_gap_total_position",
        "on_hand_days_cover_q50","total_position_days_cover_q50",
        "risk_level","risk_reason","inventory_source_status","inventory_usable",
        "normal_po_quantity_generated","build_version",
    ]
    sql = (
        f"INSERT INTO {DEST_TABLE} ({','.join(cols)}) VALUES "
        f"({','.join(['%s']*len(cols))}) ON DUPLICATE KEY UPDATE "
        + ",".join(
            f"{c}=VALUES({c})"
            for c in cols if c not in ("snapshot_date","store_name","spu")
        )
    )
    payload = [tuple(x.get(c) for c in cols) for x in out]
    with db_cursor() as c:
        c.executemany(sql, payload)

    print("H48_LEADTIME_RISK_PERSISTED=" + json.dumps({
        "table": DEST_TABLE,
        "snapshot_date": str(pd),
        "rows": len(out),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
