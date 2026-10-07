#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Build H60 inventory coverage range for NEW_VISIBLE shadow."""
from __future__ import annotations
import json
from common.database import db_cursor

PRED = "forecast_new_visible_h60_prediction_daily"
INV = "forecast_new_visible_inventory_position_daily"
DEST = "forecast_new_visible_h60_inventory_coverage_daily"

def q(sql, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())

def one(sql, params=()):
    rows = q(sql, params)
    return rows[0] if rows else {}

def ensure_table():
    with db_cursor() as c:
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST} (
          snapshot_date DATE NOT NULL,
          store_name VARCHAR(200) NOT NULL,
          spu VARCHAR(200) NOT NULL,
          age_days INT NOT NULL,
          h60_q50 DECIMAL(18,2) NOT NULL DEFAULT 0,
          h60_q75 DECIMAL(18,2) NOT NULL DEFAULT 0,
          on_hand_position DECIMAL(18,2) NOT NULL DEFAULT 0,
          total_inventory_position DECIMAL(18,2) NOT NULL DEFAULT 0,
          q50_gap_if_all_pending_arrives DECIMAL(18,2) NOT NULL DEFAULT 0,
          q50_gap_if_no_pending_arrives DECIMAL(18,2) NOT NULL DEFAULT 0,
          q75_gap_if_all_pending_arrives DECIMAL(18,2) NOT NULL DEFAULT 0,
          q75_gap_if_no_pending_arrives DECIMAL(18,2) NOT NULL DEFAULT 0,
          coverage_status VARCHAR(50) NOT NULL,
          materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (snapshot_date,store_name,spu)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

def main():
    d1 = one(f"SELECT MAX(snapshot_date) AS d FROM {PRED}").get("d")
    d2 = one(f"SELECT MAX(snapshot_date) AS d FROM {INV}").get("d")
    if not d1 or str(d1) != str(d2):
        raise RuntimeError(f"snapshot mismatch: H60={d1}, inventory={d2}")

    rows = q(f"""
      SELECT
        p.snapshot_date,p.store_name,p.spu,p.age_days,
        p.shadow_quantity_eligible,p.selected_q50,p.selected_q75,
        i.on_hand_position,i.total_inventory_position,i.inventory_usable
      FROM {PRED} p
      LEFT JOIN {INV} i
        ON i.snapshot_date=p.snapshot_date
       AND i.store_name=p.store_name
       AND i.spu=p.spu
      WHERE p.snapshot_date=%s
    """, (d1,))

    out = []
    counts = {}
    for r in rows:
        q50 = float(r.get("selected_q50") or 0)
        q75 = max(float(r.get("selected_q75") or 0), q50)
        onh = float(r.get("on_hand_position") or 0)
        total = float(r.get("total_inventory_position") or 0)

        if not int(r.get("shadow_quantity_eligible", 0) or 0):
            status = "WATCH_ONLY_AGE"
        elif not int(r.get("inventory_usable", 0) or 0):
            status = "BLOCK_INVENTORY"
        elif total < q50:
            status = "BELOW_Q50_EVEN_WITH_PENDING"
        elif onh < q50:
            status = "Q50_DEPENDS_ON_PENDING"
        elif total < q75:
            status = "Q50_COVERED_Q75_SHORT"
        elif onh < q75:
            status = "Q75_DEPENDS_ON_PENDING"
        else:
            status = "Q75_COVERED_ON_HAND"

        counts[status] = counts.get(status, 0) + 1
        out.append({
            "snapshot_date": r["snapshot_date"],
            "store_name": r["store_name"],
            "spu": r["spu"],
            "age_days": int(r.get("age_days", 0) or 0),
            "h60_q50": q50,
            "h60_q75": q75,
            "on_hand_position": onh,
            "total_inventory_position": total,
            "q50_gap_if_all_pending_arrives": max(q50-total, 0),
            "q50_gap_if_no_pending_arrives": max(q50-onh, 0),
            "q75_gap_if_all_pending_arrives": max(q75-total, 0),
            "q75_gap_if_no_pending_arrives": max(q75-onh, 0),
            "coverage_status": status,
        })

    print("H60_INVENTORY_COVERAGE_SCOPE=" + json.dumps({
        "snapshot_date": str(d1),
        "rows": len(out),
        "status_counts": counts,
        "eta_known": False,
    }, ensure_ascii=False))

    top = [x for x in out if x["coverage_status"] not in ("Q75_COVERED_ON_HAND","WATCH_ONLY_AGE")]
    top.sort(key=lambda x: (-x["q75_gap_if_all_pending_arrives"], -x["q75_gap_if_no_pending_arrives"]))
    for x in top[:30]:
        print("H60_INVENTORY_COVERAGE_TOP=" + json.dumps(x, ensure_ascii=False))

    ensure_table()
    if out:
        cols = list(out[0].keys())
        sql = (
            f"INSERT INTO {DEST} ({','.join(cols)}) VALUES "
            f"({','.join(['%s']*len(cols))}) ON DUPLICATE KEY UPDATE "
            + ",".join(f"{c}=VALUES({c})" for c in cols if c not in ("snapshot_date","store_name","spu"))
        )
        with db_cursor() as c:
            c.executemany(sql, [tuple(x[k] for k in cols) for x in out])

    print("H60_INVENTORY_COVERAGE_PERSISTED=" + json.dumps({
        "table": DEST,
        "snapshot_date": str(d1),
        "rows": len(out),
    }, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
