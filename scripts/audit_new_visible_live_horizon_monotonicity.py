#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Audit live H60 >= H48 cumulative-demand monotonicity."""
from __future__ import annotations
import json
from common.database import db_cursor

H48 = "forecast_new_visible_h48_prediction_daily"
H60 = "forecast_new_visible_h60_prediction_daily"

def q(sql, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())

def one(sql, params=()):
    rows = q(sql, params)
    return rows[0] if rows else {}

def main():
    d48 = one(f"SELECT MAX(snapshot_date) AS d FROM {H48}").get("d")
    d60 = one(f"SELECT MAX(snapshot_date) AS d FROM {H60}").get("d")
    if not d48 or str(d48) != str(d60):
        raise RuntimeError(f"snapshot mismatch: H48={d48}, H60={d60}")
    rows = q(f"""
        SELECT
          h60.store_name,h60.spu,h60.age_days,
          CASE WHEN h48.checkpoint_validated=1
               THEN COALESCE(h48.age_q50,h48.global_q50)
               ELSE h48.global_q50 END AS h48_q50,
          CASE WHEN h48.checkpoint_validated=1
               THEN COALESCE(h48.age_q75,h48.global_q75)
               ELSE h48.global_q75 END AS h48_q75,
          h60.selected_q50 AS h60_q50,
          h60.selected_q75 AS h60_q75
        FROM {H60} h60
        JOIN {H48} h48
          ON h48.snapshot_date=h60.snapshot_date
         AND h48.store_name=h60.store_name
         AND h48.spu=h60.spu
        WHERE h60.snapshot_date=%s
    """, (d60,))
    bad50 = [r for r in rows if float(r["h60_q50"] or 0) + 1e-9 < float(r["h48_q50"] or 0)]
    bad75 = [r for r in rows if float(r["h60_q75"] or 0) + 1e-9 < float(r["h48_q75"] or 0)]
    print("HORIZON_MONOTONICITY_SCOPE=" + json.dumps({
        "snapshot_date": str(d60),
        "rows": len(rows),
        "q50_violations": len(bad50),
        "q75_violations": len(bad75),
    }, ensure_ascii=False))
    for r in (bad50 + bad75)[:30]:
        print("HORIZON_MONOTONICITY_VIOLATION=" + json.dumps(r, ensure_ascii=False, default=str))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
