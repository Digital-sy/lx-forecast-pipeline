#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Measure point-in-time H48/H60 response to an activity spike for one NEW_VISIBLE SPU.

Example:
  BQ106 before/after 2026-09-23 BD, with inventory fixed at 5400.

Daily semantics:
- as_of=2026-09-22: model only sees data through Sep-22.
- as_of=2026-09-23: model sees the completed Sep-23 daily observation.
Thus the second row isolates how the Sep-23 sales/sessions spike changes the next
daily model score.

Research/read-only. No DB writes.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as hist
from scripts.materialize_new_visible_live_core import (
    feature_row,
    load_first_sales,
    load_series,
    schema_guard,
)
from scripts.replay_new_visible_procurement_asof_inventory import (
    score_h48,
    score_h60,
)


def q(sql: str, params=()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def daily_rows(store: str, spu: str, end_date, days: int = 10):
    start = end_date - timedelta(days=days - 1)
    return q(
        f"""
        SELECT dt, sales_units, sessions
        FROM {hist.DAILY_TABLE}
        WHERE store_name=%s AND spu=%s
          AND dt BETWEEN %s AND %s
        ORDER BY dt
        """,
        (store, spu, start, end_date),
    )


def risk(inv: float, q50: float, q75: float) -> str:
    if inv < q50:
        return "CRITICAL_LT_SHORTAGE"
    if inv < q75:
        return "HIGH_LT_RISK"
    return "COVERED_Q75_ON_HAND"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", default="JQ-US")
    ap.add_argument("--spu", default="BQ106")
    ap.add_argument("--as-of", default="2026-09-22,2026-09-23")
    ap.add_argument("--inventory", type=float, default=5400.0)
    ap.add_argument("--daily-days", type=int, default=10)
    args = ap.parse_args()

    dates = [
        datetime.strptime(x.strip(), "%Y-%m-%d").date()
        for x in args.as_of.split(",")
        if x.strip()
    ]
    if not dates:
        raise RuntimeError("no as-of dates")

    key = (args.store, args.spu)
    fs_map = load_first_sales([key])
    if key not in fs_map:
        raise RuntimeError(f"no exact first sale for {key}")
    first_sale = fs_map[key]

    all_rows = []
    for as_of in dates:
        series = load_series([key], as_of)
        row = feature_row(
            snapshot_date=as_of + timedelta(days=1),
            as_of=as_of,
            shop=args.store,
            spu=args.spu,
            fs=first_sale,
            series=series.get(key, {}),
        )
        schema_guard([row])
        row["launch_key"] = f"{args.store}|{args.spu}"
        live = pd.DataFrame([row])

        h48 = score_h48(live, as_of)
        h60 = score_h60(live, as_of, h48)

        h48q50 = float(h48.iloc[0]["h48_q50"])
        h48q75 = float(h48.iloc[0]["h48_q75"])
        h60q50 = float(h60.iloc[0]["h60_q50"])
        h60q75 = float(h60.iloc[0]["h60_q75"])

        daily_rate_q50 = h60q50 / 60.0 if h60q50 > 0 else 0.0
        cover_days = args.inventory / daily_rate_q50 if daily_rate_q50 > 0 else None
        latest_delay = None if cover_days is None else max(0.0, cover_days - 48.0)

        result = {
            "as_of_date": str(as_of),
            "snapshot_date": str(as_of + timedelta(days=1)),
            "store_name": args.store,
            "spu": args.spu,
            "first_sale_day": str(first_sale),
            "age_days": int(row["age_days"]),
            "inventory_fixed": args.inventory,
            "sales_3d": float(row["sales_3d"]),
            "sales_prev_3d": float(row["sales_prev_3d"]),
            "sales_growth_3d": row["sales_growth_3d"],
            "sales_7d": float(row["sales_7d"]),
            "sales_prev_7d": float(row["sales_prev_7d"]),
            "sales_growth_7d": row["sales_growth_7d"],
            "sales_14d": float(row["sales_14d"]),
            "sales_30d": float(row["sales_30d"]),
            "sessions_3d": float(row["sessions_3d"]),
            "sessions_prev_3d": float(row["sessions_prev_3d"]),
            "sessions_growth_3d": row["sessions_growth_3d"],
            "sessions_7d": float(row["sessions_7d"]),
            "sessions_prev_7d": float(row["sessions_prev_7d"]),
            "sessions_growth_7d": row["sessions_growth_7d"],
            "cvr_3d": row["cvr_3d"],
            "cvr_7d": row["cvr_7d"],
            "cvr_prev_7d": row["cvr_prev_7d"],
            "sales_slope_7": row["sales_slope_7"],
            "sales_cv_7": row["sales_cv_7"],
            "sales_max_day_share_7": row["sales_max_day_share_7"],
            "h48_q50": round(h48q50, 2),
            "h48_q75": round(h48q75, 2),
            "h60_q50": round(h60q50, 2),
            "h60_q75": round(h60q75, 2),
            "h48_q50_gap_vs_inventory": round(max(h48q50 - args.inventory, 0.0), 2),
            "h48_q75_gap_vs_inventory": round(max(h48q75 - args.inventory, 0.0), 2),
            "h60_q50_gap_vs_inventory": round(max(h60q50 - args.inventory, 0.0), 2),
            "h60_q75_gap_vs_inventory": round(max(h60q75 - args.inventory, 0.0), 2),
            "h48_risk_with_fixed_inventory": risk(args.inventory, h48q50, h48q75),
            "h60_q50_days_cover": None if cover_days is None else round(cover_days, 2),
            "days_until_q50_reorder_point_48d_lt": None if latest_delay is None else round(latest_delay, 2),
        }
        all_rows.append(result)
        print("ACTIVITY_RESPONSE_POINT=" + json.dumps(
            result, ensure_ascii=False, default=str
        ))

        for d in daily_rows(args.store, args.spu, as_of, args.daily_days):
            print("ACTIVITY_RESPONSE_DAILY=" + json.dumps({
                "as_of_context": str(as_of),
                "dt": str(d.get("dt")),
                "sales_units": float(d.get("sales_units") or 0),
                "sessions": float(d.get("sessions") or 0),
            }, ensure_ascii=False, default=str))

    if len(all_rows) >= 2:
        a, b = all_rows[0], all_rows[-1]
        def pct(new, old):
            if old in (None, 0):
                return None
            return round((float(new) / float(old) - 1.0) * 100.0, 2)

        delta = {
            "from_as_of": a["as_of_date"],
            "to_as_of": b["as_of_date"],
            "sales_3d_change_pct": pct(b["sales_3d"], a["sales_3d"]),
            "sales_7d_change_pct": pct(b["sales_7d"], a["sales_7d"]),
            "sessions_3d_change_pct": pct(b["sessions_3d"], a["sessions_3d"]),
            "sessions_7d_change_pct": pct(b["sessions_7d"], a["sessions_7d"]),
            "h48_q50_change": round(b["h48_q50"] - a["h48_q50"], 2),
            "h48_q50_change_pct": pct(b["h48_q50"], a["h48_q50"]),
            "h48_q75_change": round(b["h48_q75"] - a["h48_q75"], 2),
            "h48_q75_change_pct": pct(b["h48_q75"], a["h48_q75"]),
            "h60_q50_change": round(b["h60_q50"] - a["h60_q50"], 2),
            "h60_q50_change_pct": pct(b["h60_q50"], a["h60_q50"]),
            "h60_q75_change": round(b["h60_q75"] - a["h60_q75"], 2),
            "h60_q75_change_pct": pct(b["h60_q75"], a["h60_q75"]),
            "q50_days_cover_change": (
                None
                if a["h60_q50_days_cover"] is None or b["h60_q50_days_cover"] is None
                else round(b["h60_q50_days_cover"] - a["h60_q50_days_cover"], 2)
            ),
            "risk_before": a["h48_risk_with_fixed_inventory"],
            "risk_after": b["h48_risk_with_fixed_inventory"],
        }
        print("ACTIVITY_RESPONSE_DELTA=" + json.dumps(
            delta, ensure_ascii=False, default=str
        ))

    print("ACTIVITY_RESPONSE_SCOPE=" + json.dumps({
        "store_name": args.store,
        "spu": args.spu,
        "as_of_dates": [str(x) for x in dates],
        "inventory_fixed": args.inventory,
        "daily_semantics": "as_of includes completed daily data through that date",
        "read_only": True,
        "production_po_written": False,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
