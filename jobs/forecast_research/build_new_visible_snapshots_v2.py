#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""NEW_VISIBLE historical snapshot builder V2.

Key change vs V1
----------------
Daily SPU history is the authoritative launch clock because it has exact positive-sale
observations. Monthly history is used only as a veto when it proves the item sold in an
earlier calendar month than the daily history can see.

Business exclusions
-------------------
SPUs whose style code starts with ``LCS-`` are special low-price disposal/handling
products rather than normal-selling launches. They remain in the raw SPU-day research
history for factual reconciliation, but are explicitly ineligible for NEW_VISIBLE
cohorts, snapshots, Breakout backtests, and future V1 model training.

Observed audit on 2024-01-01..2026-10-05:
- 194 monthly/daily month disagreements;
- all 194 are DAILY_EARLIER (daily history sees sales earlier than the monthly table);
- zero DAILY_LATER cases.
Therefore DAILY_EARLIER is retained rather than incorrectly excluded.

Research-only. Writes only forecast_research_* tables.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict

from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_research import build_new_visible_snapshots as v1

DATASET_VERSION = "new_visible_snapshot_v2_daily_first_priority_exclude_lcs"
SPECIAL_LOW_PRICE_PREFIXES = ("LCS-",)


def is_special_low_price_spu(spu: str) -> bool:
    s = str(spu or "").strip().upper()
    return any(s.startswith(prefix) for prefix in SPECIAL_LOW_PRICE_PREFIXES)


def rebuild_cohorts(dry_run: bool = False) -> Dict[str, Any]:
    bounds = v1.one(
        f"SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt FROM `{v1.DAILY_TABLE}`"
    )
    if not bounds.get("min_dt"):
        raise RuntimeError(f"{v1.DAILY_TABLE} 为空")

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

    rows = []
    stats = defaultdict(int)
    for r in launches:
        shop = str(r["store_name"]).strip()
        spu = str(r["spu"]).strip()
        fs = v1.to_date(r["first_sale_day"])
        mf = monthly.get((shop, spu))

        confidence = "DAILY_ONLY" if mf is None else "DAILY+MONTHLY_SAME_MONTH"
        eligible = True
        reason = None
        special_low_price = is_special_low_price_spu(spu)

        # Business rule has highest priority: LCS-* is not a normal-selling launch.
        if special_low_price:
            eligible = False
            reason = "SPECIAL_LOW_PRICE_SPU"
            confidence = "BUSINESS_EXCLUDED_LCS"
        elif fs < burn_cutoff:
            eligible = False
            reason = "LEFT_EDGE_BURN_IN"
        elif fs > label_cutoff:
            eligible = False
            reason = "NO_30D_FUTURE_WINDOW"
        elif mf is not None:
            daily_month = v1.month_floor(fs)
            monthly_month = v1.month_floor(mf)
            if monthly_month < daily_month:
                # Monthly table proves sales existed before daily first positive day.
                # This is a genuine left-censoring/data-gap risk and must be excluded.
                eligible = False
                reason = "MONTHLY_PROVES_EARLIER_SALE"
                confidence = "MONTHLY_EARLIER_THAN_DAILY"
            elif monthly_month > daily_month:
                # Daily history contains real positive sales earlier than monthly history.
                # Keep exact daily launch clock; monthly source is lagging/incomplete here.
                confidence = "DAILY_LEADS_MONTHLY"

        full_120 = int(
            fs <= max_dt - timedelta(days=v1.MAX_AGE_DAYS + v1.FUTURE_LABEL_DAYS)
        )

        stats["total"] += 1
        stats["eligible"] += int(eligible)
        stats["special_low_price_spu"] += int(special_low_price)
        stats["daily_only"] += int(mf is None)
        stats["daily_monthly_same"] += int(
            mf is not None and v1.month_floor(mf) == v1.month_floor(fs)
        )
        stats["daily_leads_monthly"] += int(
            mf is not None and v1.month_floor(fs) < v1.month_floor(mf)
        )
        stats["monthly_leads_daily"] += int(
            mf is not None and v1.month_floor(mf) < v1.month_floor(fs)
        )
        stats["full_120d"] += full_120

        rows.append(
            (
                shop,
                spu,
                fs,
                v1.month_floor(mf) if mf is not None else None,
                confidence,
                int(eligible),
                reason,
                full_120,
                min_dt,
                max_dt,
                DATASET_VERSION,
            )
        )

    summary = {
        "base_min": str(min_dt),
        "base_max": str(max_dt),
        "burn_cutoff": str(burn_cutoff),
        "label_cutoff": str(label_cutoff),
        "rule": (
            "daily first positive day authoritative; exclude LCS-* special low-price SPUs; "
            "exclude if monthly proves earlier sale"
        ),
        **{k: int(v) for k, v in stats.items()},
    }
    if dry_run:
        return summary

    sql = f"""
        INSERT INTO `{v1.COHORT_TABLE}`
        (`store_name`,`spu`,`first_sale_day`,`monthly_first_sale_month`,
         `cohort_confidence`,`eligible_strict`,`exclusion_reason`,
         `full_120d_window`,`base_min_dt`,`base_max_dt`,`dataset_version`)
        VALUES ({','.join(['%s'] * 11)})
        ON DUPLICATE KEY UPDATE
          `first_sale_day`=VALUES(`first_sale_day`),
          `monthly_first_sale_month`=VALUES(`monthly_first_sale_month`),
          `cohort_confidence`=VALUES(`cohort_confidence`),
          `eligible_strict`=VALUES(`eligible_strict`),
          `exclusion_reason`=VALUES(`exclusion_reason`),
          `full_120d_window`=VALUES(`full_120d_window`),
          `base_min_dt`=VALUES(`base_min_dt`),
          `base_max_dt`=VALUES(`base_max_dt`),
          `dataset_version`=VALUES(`dataset_version`),
          `materialized_at`=CURRENT_TIMESTAMP
    """
    with v1.db_cursor() as c:
        for i in range(0, len(rows), v1.BATCH_SIZE):
            c.executemany(sql, rows[i:i + v1.BATCH_SIZE])
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cohort-start", default=None)
    ap.add_argument("--cohort-end", default=None)
    ap.add_argument("--max-cohorts", type=int, default=None)
    ap.add_argument("--cohorts-only", action="store_true")
    args = ap.parse_args()

    if not base.table_exists(v1.DAILY_TABLE):
        raise RuntimeError(
            f"{v1.DAILY_TABLE} 不存在，先运行 jobs.forecast_research.build_spu_daily_history"
        )

    # Reuse the audited V1 schema and feature/future-outcome construction functions.
    if not args.dry_run:
        v1.ensure_tables()

    cohort_summary = rebuild_cohorts(dry_run=args.dry_run)
    print("LAUNCH_COHORT_V2_SUMMARY=" + json.dumps(cohort_summary, ensure_ascii=False))

    if args.cohorts_only:
        return 0

    if args.dry_run and not base.table_exists(v1.COHORT_TABLE):
        print(
            "NEW_VISIBLE_SNAPSHOT_V2_DRY_RUN="
            + json.dumps(
                {
                    "status": "COHORT_TABLE_NOT_YET_PERSISTED",
                    "next": "run V2 once with --cohorts-only, then rerun --dry-run",
                },
                ensure_ascii=False,
            )
        )
        return 0

    cohort_start = (
        datetime.strptime(args.cohort_start, "%Y-%m-%d").date()
        if args.cohort_start else None
    )
    cohort_end = (
        datetime.strptime(args.cohort_end, "%Y-%m-%d").date()
        if args.cohort_end else None
    )

    # Ensure persisted snapshot rows carry the V2 dataset version as well.
    v1.DATASET_VERSION = DATASET_VERSION
    snap_summary = v1.build_snapshots(
        dry_run=args.dry_run,
        cohort_start=cohort_start,
        cohort_end=cohort_end,
        max_cohorts=args.max_cohorts,
    )
    print(
        "NEW_VISIBLE_SNAPSHOT_V2_SUMMARY="
        + json.dumps(snap_summary, ensure_ascii=False, default=str)
    )

    if not args.dry_run:
        final = v1.one(
            f"""
            SELECT MIN(snapshot_date) AS min_dt, MAX(snapshot_date) AS max_dt,
                   COUNT(*) AS rows_n,
                   COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS launch_n
            FROM `{v1.SNAPSHOT_TABLE}`
            WHERE dataset_version=%s
            """,
            (DATASET_VERSION,),
        )
        print(
            "NEW_VISIBLE_SNAPSHOT_V2_FINAL="
            + json.dumps(
                {
                    "min_dt": str(final.get("min_dt") or ""),
                    "max_dt": str(final.get("max_dt") or ""),
                    "rows_n": int(final.get("rows_n", 0) or 0),
                    "launch_n": int(final.get("launch_n", 0) or 0),
                    "dataset_version": DATASET_VERSION,
                },
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
