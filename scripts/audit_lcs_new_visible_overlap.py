#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only overlap audit: LCS-mapped SPUs vs strict NEW_VISIBLE launch candidates.

No tables are modified.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research import build_new_visible_snapshots as v1

LCS_START = date(2025, 3, 1)
LCS_END = date(2025, 9, 30)


def month_windows(start: date, end: date):
    cur = start.replace(day=1)
    while cur <= end:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        yield cur, min(end, nxt - timedelta(days=1))
        cur = nxt


def load_lcs_spus():
    source = v2.resolve_performance_source(date.today())
    table = source["table"]
    cols = set(base.get_columns(table))
    required = {"dt", "store_name", "msku", "spu"}
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(f"{table} missing fields: {missing}")
    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""

    spus = set()
    month_counts = []
    for s, e in month_windows(LCS_START, LCS_END):
        with db_cursor() as c:
            c.execute(
                f"""
                SELECT DISTINCT UPPER(TRIM(p.`spu`)) AS spu
                FROM {table} p
                WHERE p.`dt` BETWEEN %s AND %s
                  AND p.`store_name` IN (%s,%s,%s,%s)
                  AND p.`msku` LIKE 'LCS-%%'
                  AND p.`spu` IS NOT NULL
                  AND p.`spu` <> ''
                  {delete_filter}
                """,
                (s, e, *TARGET_SHOPS),
            )
            rows = c.fetchall()
        month_spus = {str(r.get("spu") or "").strip().upper() for r in rows}
        month_spus.discard("")
        spus.update(month_spus)
        month_counts.append({"month": s.strftime("%Y-%m"), "spu_n": len(month_spus)})
    return spus, month_counts, table


def main() -> int:
    lcs_spus, month_counts, source_table = load_lcs_spus()

    bounds = v1.one(
        f"SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt FROM `{v1.DAILY_TABLE}`"
    )
    min_dt = v1.to_date(bounds["min_dt"])
    max_dt = v1.to_date(bounds["max_dt"])
    burn_cutoff = min_dt + timedelta(days=v1.BURN_IN_DAYS)
    label_cutoff = max_dt - timedelta(days=v1.FUTURE_LABEL_DAYS)

    launches = v1.q(
        f"""
        SELECT store_name, spu, MIN(dt) AS first_sale_day
        FROM `{v1.DAILY_TABLE}`
        WHERE sales_units > 0
        GROUP BY store_name, spu
        """
    )
    monthly = v1.load_monthly_first_sale()

    total = eligible_before_lcs = eligible_lcs = 0
    lcs_launch_distinct_spu = set()
    by_shop = {}
    by_month = {}
    examples = []

    for r in launches:
        shop = str(r["store_name"]).strip()
        spu = str(r["spu"]).strip()
        spu_u = spu.upper()
        fs = v1.to_date(r["first_sale_day"])
        mf = monthly.get((shop, spu))
        total += 1

        eligible = True
        if fs < burn_cutoff or fs > label_cutoff:
            eligible = False
        elif mf is not None and v1.month_floor(mf) < v1.month_floor(fs):
            eligible = False

        if not eligible:
            continue

        eligible_before_lcs += 1
        if spu_u not in lcs_spus:
            continue

        eligible_lcs += 1
        lcs_launch_distinct_spu.add(spu_u)
        by_shop[shop] = by_shop.get(shop, 0) + 1
        ym = fs.strftime("%Y-%m")
        by_month[ym] = by_month.get(ym, 0) + 1
        if len(examples) < 100:
            examples.append({
                "shop": shop,
                "spu": spu,
                "first_sale_day": str(fs),
                "monthly_first_sale": str(mf or ""),
            })

    spring_summer_2025 = sum(
        n for ym, n in by_month.items()
        if "2025-03" <= ym <= "2025-09"
    )

    print("LCS_COHORT_OVERLAP_SUMMARY=" + json.dumps({
        "source_table": source_table,
        "lcs_mapping_range": [str(LCS_START), str(LCS_END)],
        "lcs_master_spu_n": len(lcs_spus),
        "month_spu_counts": month_counts,
        "launch_total": total,
        "eligible_before_lcs": eligible_before_lcs,
        "eligible_lcs_shop_spu": eligible_lcs,
        "eligible_lcs_distinct_spu": len(lcs_launch_distinct_spu),
        "eligible_after_lcs": eligible_before_lcs - eligible_lcs,
        "lcs_share_of_eligible_shop_spu": round(
            eligible_lcs / eligible_before_lcs, 6
        ) if eligible_before_lcs else None,
        "lcs_eligible_2025_03_to_09": spring_summer_2025,
    }, ensure_ascii=False))

    print("BY_SHOP=" + json.dumps(dict(sorted(by_shop.items())), ensure_ascii=False))
    print("BY_FIRST_SALE_MONTH=" + json.dumps(dict(sorted(by_month.items())), ensure_ascii=False))
    print("EXAMPLES=")
    for r in examples:
        print(json.dumps(r, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
