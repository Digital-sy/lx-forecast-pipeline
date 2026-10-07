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
``LCS-`` is a special low-price handling MSKU prefix. Per business rule, any SPU mapped
from an audited LCS-* MSKU is excluded as a whole. The mapping is materialized once in
``forecast_special_spu_exclusion`` so model runs do not rescan the large ODS table.
The raw SPU-day research history remains untouched for factual reconciliation.

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
from typing import Any, Dict, Set

from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_research import build_new_visible_snapshots as v1

DATASET_VERSION = "new_visible_snapshot_v2_daily_first_priority_business_exclusions_v2"
SPECIAL_EXCLUSION_TABLE = "forecast_special_spu_exclusion"
LCS_EXCLUSION_CODE = "LCS_SPECIAL_LOW_PRICE"
XH_EXCLUSION_CODE = "XH_PREFIX_EXCLUSION"
XH_PREFIX = "XH"


def load_business_exclusions() -> Dict[str, Set[str]]:
    """Load materialized business exclusion sets by code."""
    if not base.table_exists(SPECIAL_EXCLUSION_TABLE):
        raise RuntimeError(
            f"{SPECIAL_EXCLUSION_TABLE} 不存在；先运行 scripts/materialize_lcs_special_spu_exclusion.py"
        )
    rows = v1.q(
        f"""
        SELECT spu, exclusion_code
        FROM `{SPECIAL_EXCLUSION_TABLE}`
        WHERE exclusion_code IN (%s,%s)
        """,
        (LCS_EXCLUSION_CODE, XH_EXCLUSION_CODE),
    )
    out = {"LCS": set(), "XH": set()}
    for r in rows:
        spu = str(r.get("spu") or "").strip().upper()
        code = str(r.get("exclusion_code") or "")
        if not spu:
            continue
        if code == LCS_EXCLUSION_CODE:
            out["LCS"].add(spu)
        elif code == XH_EXCLUSION_CODE:
            out["XH"].add(spu)
    if not out["LCS"]:
        raise RuntimeError(
            f"{SPECIAL_EXCLUSION_TABLE} 中没有 {LCS_EXCLUSION_CODE}；拒绝未排除LCS SPU时继续"
        )
    return out


def is_xh_spu(spu: str, xh_spus: Set[str]) -> bool:
    s = str(spu or "").strip().upper()
    return s.startswith(XH_PREFIX) or s in xh_spus


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
    business_exclusions = load_business_exclusions()
    lcs_spus = business_exclusions["LCS"]
    xh_spus = business_exclusions["XH"]

    rows = []
    stats = defaultdict(int)
    excluded_launch_spus = set()
    for r in launches:
        shop = str(r["store_name"]).strip()
        spu = str(r["spu"]).strip()
        fs = v1.to_date(r["first_sale_day"])
        mf = monthly.get((shop, spu))

        confidence = "DAILY_ONLY" if mf is None else "DAILY+MONTHLY_SAME_MONTH"
        eligible = True
        reason = None
        spu_u = spu.upper()
        lcs_excluded = spu_u in lcs_spus
        xh_excluded = is_xh_spu(spu, xh_spus)
        special_business_excluded = lcs_excluded or xh_excluded

        if special_business_excluded:
            eligible = False
            if lcs_excluded and xh_excluded:
                reason = "BUSINESS_EXCLUDED_LCS_AND_XH"
                confidence = "BUSINESS_EXCLUDED_LCS_AND_XH"
            elif lcs_excluded:
                reason = "SPECIAL_LOW_PRICE_SPU"
                confidence = "BUSINESS_EXCLUDED_LCS_MAPPED_SPU"
            else:
                reason = "XH_PREFIX_SPU"
                confidence = "BUSINESS_EXCLUDED_XH_PREFIX"
            excluded_launch_spus.add(spu_u)
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
                eligible = False
                reason = "MONTHLY_PROVES_EARLIER_SALE"
                confidence = "MONTHLY_EARLIER_THAN_DAILY"
            elif monthly_month > daily_month:
                confidence = "DAILY_LEADS_MONTHLY"

        full_120 = int(
            fs <= max_dt - timedelta(days=v1.MAX_AGE_DAYS + v1.FUTURE_LABEL_DAYS)
        )

        stats["total"] += 1
        stats["eligible"] += int(eligible)
        stats["lcs_excluded_shop_spu"] += int(lcs_excluded)
        stats["xh_excluded_shop_spu"] += int(xh_excluded)
        stats["xh_incremental_shop_spu"] += int(xh_excluded and not lcs_excluded)
        stats["business_excluded_shop_spu"] += int(special_business_excluded)
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
            "daily first positive day authoritative; exclude whole SPU for LCS-mapped and XH-prefix business rules; "
            "exclude if monthly proves earlier sale"
        ),
        "lcs_control_spu_n": len(lcs_spus),
        "xh_control_spu_n": len(xh_spus),
        "business_control_union_spu_n": len(lcs_spus | xh_spus),
        "business_excluded_distinct_spu": len(excluded_launch_spus),
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


def reset_snapshot_table_for_full_rebuild() -> int:
    """Clear the research snapshot table before a full V2 rebuild.

    The snapshot PK does not include dataset_version, so excluded launches from older
    research versions can otherwise remain as stale rows. A full rebuild is cheap enough
    to replace the research snapshot table contents atomically at the workflow level.
    """
    if not base.table_exists(v1.SNAPSHOT_TABLE):
        return 0
    before = v1.one(f"SELECT COUNT(*) AS n FROM `{v1.SNAPSHOT_TABLE}`")
    with v1.db_cursor() as c:
        c.execute(f"DELETE FROM `{v1.SNAPSHOT_TABLE}`")
    return int(before.get("n", 0) or 0)


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

    v1.DATASET_VERSION = DATASET_VERSION

    is_full_rebuild = (
        not args.dry_run
        and cohort_start is None
        and cohort_end is None
        and args.max_cohorts is None
    )
    if is_full_rebuild:
        deleted = reset_snapshot_table_for_full_rebuild()
        print(
            "NEW_VISIBLE_SNAPSHOT_V2_RESET="
            + json.dumps(
                {
                    "table": v1.SNAPSHOT_TABLE,
                    "deleted_old_rows": deleted,
                    "reason": "full rebuild clears stale rows because PK excludes dataset_version",
                },
                ensure_ascii=False,
            )
        )

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
