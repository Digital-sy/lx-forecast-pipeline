#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Materialize production-facing NEW_VISIBLE procurement recommendations.

This is the approved bridge from the validated H48/H60 NEW_VISIBLE model into the
existing procurement recommendation flow.

Important:
- This table is a recommendation interface, NOT a purchase-order table.
- Automatic PO creation remains disabled.
- Stock fabrics release only the NET H60 Q50 shortage after current inventory position.
- Rows that are covered today but approach the 48-day lead-time point within 30 days
  are review reminders only; they release zero current procurement quantity.
- Custom fabrics remain H90 HOLD: no quantity is released.
- UNKNOWN fabric / unusable inventory is blocked.
- Ages outside 7..120 do not override the legacy flow.

No production PO table is written.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Sequence

from common.database import db_cursor

H60_COVERAGE = "forecast_new_visible_h60_inventory_coverage_daily"
H60_PRED = "forecast_new_visible_h60_prediction_daily"
ACTION = "forecast_new_visible_procurement_action_shadow_daily"
DEST = "forecast_new_visible_procurement_recommendation_daily"

LEAD_TIME_DAYS = 48
ORDER_WINDOW_DAYS = 30
MODEL_VERSION = "NV_PROCUREMENT_CHAMPION_V2_H48_H60_NET_GAP"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST} (
          snapshot_date DATE NOT NULL,
          as_of_date DATE NOT NULL,
          store_name VARCHAR(200) NOT NULL,
          spu VARCHAR(200) NOT NULL,
          age_days INT NOT NULL,
          fabric_type VARCHAR(30) NOT NULL,
          primary_fabric VARCHAR(255) NOT NULL DEFAULT '',
          h48_risk_level VARCHAR(50) NOT NULL DEFAULT '',
          h60_coverage_status VARCHAR(50) NOT NULL DEFAULT '',
          h60_q50 DECIMAL(18,2) NOT NULL DEFAULT 0,
          h60_q75 DECIMAL(18,2) NOT NULL DEFAULT 0,
          on_hand_position DECIMAL(18,2) NOT NULL DEFAULT 0,
          total_inventory_position DECIMAL(18,2) NOT NULL DEFAULT 0,
          q50_gap_low DECIMAL(18,2) NOT NULL DEFAULT 0,
          q50_gap_high DECIMAL(18,2) NOT NULL DEFAULT 0,
          q75_gap_low DECIMAL(18,2) NOT NULL DEFAULT 0,
          q75_gap_high DECIMAL(18,2) NOT NULL DEFAULT 0,
          qty_basis VARCHAR(120) NOT NULL DEFAULT '',
          days_cover_q50 DECIMAL(18,2) DEFAULT NULL,
          latest_order_date DATE DEFAULT NULL,
          recommendation_status VARCHAR(60) NOT NULL,
          recommended_qty_q50 INT NOT NULL DEFAULT 0,
          safety_qty_q75 INT NOT NULL DEFAULT 0,
          override_active TINYINT(1) NOT NULL DEFAULT 0,
          source_action_type VARCHAR(100) NOT NULL DEFAULT '',
          model_version VARCHAR(120) NOT NULL,
          automatic_po_enabled TINYINT(1) NOT NULL DEFAULT 0,
          production_po_written TINYINT(1) NOT NULL DEFAULT 0,
          materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (snapshot_date,store_name,spu),
          INDEX idx_nv_prod_rec_status (snapshot_date,recommendation_status),
          INDEX idx_nv_prod_rec_shop (snapshot_date,store_name,spu)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        for name, definition in (
            ("q50_gap_low", "DECIMAL(18,2) NOT NULL DEFAULT 0"),
            ("q50_gap_high", "DECIMAL(18,2) NOT NULL DEFAULT 0"),
            ("q75_gap_low", "DECIMAL(18,2) NOT NULL DEFAULT 0"),
            ("q75_gap_high", "DECIMAL(18,2) NOT NULL DEFAULT 0"),
            ("qty_basis", "VARCHAR(120) NOT NULL DEFAULT ''"),
        ):
            c.execute(
                """
                SELECT COUNT(*) AS n
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME=%s
                  AND COLUMN_NAME=%s
                """,
                (DEST, name),
            )
            if not int(c.fetchone().get("n", 0) or 0):
                c.execute(f"ALTER TABLE {DEST} ADD COLUMN {name} {definition}")


def main() -> int:
    d_cov = one(f"SELECT MAX(snapshot_date) AS d FROM {H60_COVERAGE}").get("d")
    d_pred = one(f"SELECT MAX(snapshot_date) AS d FROM {H60_PRED}").get("d")
    d_act = one(f"SELECT MAX(snapshot_date) AS d FROM {ACTION}").get("d")
    if not d_cov or str(d_cov) != str(d_pred) or str(d_cov) != str(d_act):
        raise RuntimeError(
            f"NEW_VISIBLE snapshot mismatch: coverage={d_cov}, h60={d_pred}, action={d_act}"
        )

    rows = q(f"""
        SELECT
          c.snapshot_date,
          p.as_of_date,
          c.store_name,
          c.spu,
          c.age_days,
          a.fabric_type,
          a.primary_fabric,
          a.h48_risk_level,
          c.coverage_status AS h60_coverage_status,
          c.h60_q50,
          c.h60_q75,
          c.on_hand_position,
          c.total_inventory_position,
          c.q50_gap_if_all_pending_arrives AS q50_gap_low,
          c.q50_gap_if_no_pending_arrives AS q50_gap_high,
          c.q75_gap_if_all_pending_arrives AS q75_gap_low,
          c.q75_gap_if_no_pending_arrives AS q75_gap_high,
          a.action_type
        FROM {H60_COVERAGE} c
        INNER JOIN {H60_PRED} p
          ON p.snapshot_date=c.snapshot_date
         AND p.store_name=c.store_name
         AND p.spu=c.spu
        INNER JOIN {ACTION} a
          ON a.snapshot_date=c.snapshot_date
         AND a.store_name=c.store_name
         AND a.spu=c.spu
        WHERE c.snapshot_date=%s
        ORDER BY c.store_name,c.spu
    """, (d_cov,))

    if not rows:
        raise RuntimeError("no NEW_VISIBLE rows to materialize")

    out: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}
    issue_date = to_date(d_cov)

    for r in rows:
        age = int(r.get("age_days") or 0)
        fabric = str(r.get("fabric_type") or "UNKNOWN")
        coverage = str(r.get("h60_coverage_status") or "")
        q50 = float(r.get("h60_q50") or 0)
        q75 = max(float(r.get("h60_q75") or 0), q50)
        on_hand = float(r.get("on_hand_position") or 0)
        total = float(r.get("total_inventory_position") or 0)
        q50_gap_low = max(float(r.get("q50_gap_low") or 0), 0.0)
        q50_gap_high = max(float(r.get("q50_gap_high") or 0), q50_gap_low)
        q75_gap_low = max(float(r.get("q75_gap_low") or 0), 0.0)
        q75_gap_high = max(float(r.get("q75_gap_high") or 0), q75_gap_low)

        days_cover = None
        latest_order = None
        status = "LEGACY_FALLBACK_AGE"
        qty50 = 0
        qty75 = 0
        qty_basis = "NO_RELEASE"
        override = 0

        if not (7 <= age <= 120):
            status = "LEGACY_FALLBACK_AGE"
            override = 0
        elif fabric == "UNKNOWN":
            status = "BLOCK_FABRIC_MAPPING"
            override = 1
        elif coverage == "BLOCK_INVENTORY":
            status = "BLOCK_INVENTORY"
            override = 1
        elif fabric == "定制面料":
            if str(r.get("h48_risk_level") or "") in (
                "CRITICAL_LT_SHORTAGE", "HIGH_LT_RISK"
            ):
                status = "CUSTOM_URGENT_H90_HOLD"
            else:
                status = "CUSTOM_H90_HOLD"
            override = 1
        elif fabric == "现货面料":
            override = 1
            if q50 <= 0:
                status = "NO_Q50_DEMAND"
            else:
                daily_q50 = q50 / 60.0
                days_cover = total / daily_q50 if daily_q50 > 0 else None

                # Quantity release and timing are deliberately separated:
                # 1) current production quantity = NET H60 Q50 shortage after
                #    all currently-known inventory/pending;
                # 2) if Q50 is covered today but the 48-day reorder point is
                #    within 30 days, only schedule a review. Do not pre-release
                #    the full H60 demand as a PO quantity.
                if q50_gap_low > 0 and days_cover is not None and days_cover <= LEAD_TIME_DAYS:
                    status = "ORDER_NOW_NET_Q50_GAP"
                    latest_order = issue_date
                    qty50 = int(math.ceil(q50_gap_low))
                    qty75 = int(math.ceil(q75_gap_low))
                    qty_basis = "CEIL(MAX(H60_Q50-TOTAL_INVENTORY_POSITION,0))"
                elif (
                    days_cover is not None
                    and days_cover <= LEAD_TIME_DAYS + ORDER_WINDOW_DAYS
                ):
                    status = "REVIEW_WITHIN_30D_NO_RELEASE"
                    delay = max(0.0, days_cover - LEAD_TIME_DAYS)
                    latest_order = issue_date + timedelta(days=int(math.floor(delay)))
                    qty50 = 0
                    qty75 = int(math.ceil(q75_gap_low))
                    qty_basis = "REVIEW_ONLY_Q50_COVERED_TODAY"
                else:
                    status = "NO_ORDER_WITHIN_30D"
                    qty_basis = "Q50_COVERED_OR_REORDER_POINT_OUTSIDE_30D"
        else:
            status = "BLOCK_FABRIC_TYPE_OTHER"
            override = 1

        counts[status] = counts.get(status, 0) + 1
        out.append({
            "snapshot_date": r["snapshot_date"],
            "as_of_date": r["as_of_date"],
            "store_name": str(r.get("store_name") or ""),
            "spu": str(r.get("spu") or ""),
            "age_days": age,
            "fabric_type": fabric,
            "primary_fabric": str(r.get("primary_fabric") or ""),
            "h48_risk_level": str(r.get("h48_risk_level") or ""),
            "h60_coverage_status": coverage,
            "h60_q50": q50,
            "h60_q75": q75,
            "on_hand_position": on_hand,
            "total_inventory_position": total,
            "q50_gap_low": q50_gap_low,
            "q50_gap_high": q50_gap_high,
            "q75_gap_low": q75_gap_low,
            "q75_gap_high": q75_gap_high,
            "qty_basis": qty_basis,
            "days_cover_q50": None if days_cover is None else round(days_cover, 2),
            "latest_order_date": latest_order,
            "recommendation_status": status,
            "recommended_qty_q50": qty50,
            "safety_qty_q75": qty75,
            "override_active": override,
            "source_action_type": str(r.get("action_type") or ""),
            "model_version": MODEL_VERSION,
            "automatic_po_enabled": 0,
            "production_po_written": 0,
        })

    ensure_table()
    cols = list(out[0].keys())
    sql = (
        f"INSERT INTO {DEST} ({','.join(cols)}) VALUES "
        f"({','.join(['%s'] * len(cols))}) ON DUPLICATE KEY UPDATE "
        + ",".join(
            f"{c}=VALUES({c})"
            for c in cols
            if c not in ("snapshot_date", "store_name", "spu")
        )
    )
    with db_cursor() as c:
        c.executemany(sql, [tuple(x.get(k) for k in cols) for x in out])

    print("NV_PROD_RECOMMENDATION_SCOPE=" + json.dumps({
        "snapshot_date": str(d_cov),
        "rows": len(out),
        "status_counts": counts,
        "override_active_rows": sum(int(x["override_active"]) for x in out),
        "recommended_stock_rows": sum(x["recommended_qty_q50"] > 0 for x in out),
        "recommended_q50_sum": sum(int(x["recommended_qty_q50"]) for x in out),
        "q50_gap_low_sum_stock": round(sum(float(x["q50_gap_low"]) for x in out if x["fabric_type"] == "现货面料"), 2),
        "quantity_semantics": "CURRENT_RELEASE_NET_Q50_GAP_ONLY",
        "automatic_po_enabled": False,
        "production_po_written": False,
    }, ensure_ascii=False))

    ordered = sorted(
        [x for x in out if x["override_active"]],
        key=lambda x: (
            0 if x["recommendation_status"] == "ORDER_NOW_NET_Q50_GAP" else
            1 if x["recommendation_status"] == "REVIEW_WITHIN_30D_NO_RELEASE" else 2,
            -int(x["recommended_qty_q50"]),
        ),
    )
    for x in ordered[:50]:
        print("NV_PROD_RECOMMENDATION_TOP=" + json.dumps({
            "store_name": x["store_name"],
            "spu": x["spu"],
            "fabric_type": x["fabric_type"],
            "h48_risk_level": x["h48_risk_level"],
            "h60_q50": x["h60_q50"],
            "h60_q75": x["h60_q75"],
            "total_inventory_position": x["total_inventory_position"],
            "q50_gap_low": x["q50_gap_low"],
            "q50_gap_high": x["q50_gap_high"],
            "q75_gap_low": x["q75_gap_low"],
            "q75_gap_high": x["q75_gap_high"],
            "qty_basis": x["qty_basis"],
            "days_cover_q50": x["days_cover_q50"],
            "latest_order_date": x["latest_order_date"],
            "recommendation_status": x["recommendation_status"],
            "recommended_qty_q50": x["recommended_qty_q50"],
        }, ensure_ascii=False, default=str))

    print("NV_PROD_RECOMMENDATION_PERSISTED=" + json.dumps({
        "table": DEST,
        "snapshot_date": str(d_cov),
        "rows": len(out),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
