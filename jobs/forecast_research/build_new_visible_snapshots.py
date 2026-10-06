#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Build strict historical NEW_VISIBLE point-in-time snapshots.

Research-only. Writes ONLY forecast_research_* tables.

Inputs
------
forecast_research_spu_daily_history

Outputs
-------
1) forecast_research_launch_cohort
   One row per store x SPU with exact daily first sale and strict eligibility flags.

2) forecast_research_new_visible_snapshot
   One row per historical snapshot_date x store x SPU during age 0..120 days.
   Past features use only <= snapshot_date.
   Future columns are raw outcomes used later for label design/evaluation.

No historical inventory is fabricated. Inventory features are intentionally absent.

Leakage rules
-------------
- Native historical SPU identity comes from the already-materialized daily source.
- A candidate launch is excluded if monthly history proves it sold in an earlier month.
- Features use only dates <= snapshot_date.
- Future outcomes begin at snapshot_date + 1 day.
- Every snapshot requires a full 30-day future observation window.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import get_logger
from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research.build_spu_daily_history import DEST_TABLE as DAILY_TABLE

logger = get_logger("forecast_research_new_visible")

COHORT_TABLE = "forecast_research_launch_cohort"
SNAPSHOT_TABLE = "forecast_research_new_visible_snapshot"
DATASET_VERSION = "new_visible_snapshot_v1"

BURN_IN_DAYS = 60
MAX_AGE_DAYS = 120
FUTURE_LABEL_DAYS = 30
BATCH_SIZE = 500


def to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def month_key(d: date) -> Tuple[int, int]:
    return d.year, d.month


def month_text(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def month_floor(d: date) -> date:
    return date(d.year, d.month, 1)


def add_month(d: date, n: int = 1) -> date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return date(y, m, 1)


def iter_months(start: date, end: date):
    cur = month_floor(start)
    while cur <= end:
        nxt = add_month(cur)
        yield cur, min(end, nxt - timedelta(days=1))
        cur = nxt


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params=()):
    rows = q(sql, params)
    return rows[0] if rows else {}


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return float(a) / float(b)


def finite(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    return float(v) if math.isfinite(float(v)) else None


def ensure_tables() -> None:
    with db_cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{COHORT_TABLE}` (
              `store_name` VARCHAR(200) NOT NULL,
              `spu` VARCHAR(200) NOT NULL,
              `first_sale_day` DATE NOT NULL,
              `monthly_first_sale_month` DATE DEFAULT NULL,
              `cohort_confidence` VARCHAR(40) NOT NULL,
              `eligible_strict` TINYINT(1) NOT NULL DEFAULT 0,
              `exclusion_reason` VARCHAR(100) DEFAULT NULL,
              `full_120d_window` TINYINT(1) NOT NULL DEFAULT 0,
              `base_min_dt` DATE NOT NULL,
              `base_max_dt` DATE NOT NULL,
              `dataset_version` VARCHAR(100) NOT NULL,
              `materialized_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`store_name`,`spu`),
              INDEX `idx_launch_first_sale` (`first_sale_day`,`eligible_strict`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='研究层：历史新品首销cohort及严格资格判定'
        """)

        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{SNAPSHOT_TABLE}` (
              `snapshot_date` DATE NOT NULL,
              `store_name` VARCHAR(200) NOT NULL,
              `spu` VARCHAR(200) NOT NULL,
              `first_sale_day` DATE NOT NULL,
              `age_days` INT NOT NULL,

              `sales_3d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_prev_3d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_7d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_prev_7d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_14d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_prev_14d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_30d` DECIMAL(18,2) NOT NULL DEFAULT 0,

              `sessions_3d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `sessions_prev_3d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `sessions_7d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `sessions_prev_7d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `sessions_14d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `sessions_prev_14d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `sessions_30d` DECIMAL(20,2) NOT NULL DEFAULT 0,

              `cvr_3d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_prev_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_14d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_30d` DECIMAL(12,6) DEFAULT NULL,

              `sales_growth_3d` DECIMAL(14,6) DEFAULT NULL,
              `sales_growth_7d` DECIMAL(14,6) DEFAULT NULL,
              `sessions_growth_3d` DECIMAL(14,6) DEFAULT NULL,
              `sessions_growth_7d` DECIMAL(14,6) DEFAULT NULL,
              `cvr_ratio_7d` DECIMAL(14,6) DEFAULT NULL,

              `sales_positive_days_7` INT NOT NULL DEFAULT 0,
              `sessions_positive_days_7` INT NOT NULL DEFAULT 0,
              `sales_up_days_7` INT NOT NULL DEFAULT 0,
              `sessions_up_days_7` INT NOT NULL DEFAULT 0,
              `sales_slope_7` DECIMAL(18,6) DEFAULT NULL,
              `sessions_slope_7` DECIMAL(20,6) DEFAULT NULL,
              `sales_cv_7` DECIMAL(14,6) DEFAULT NULL,
              `sessions_cv_7` DECIMAL(14,6) DEFAULT NULL,
              `sales_max_day_share_7` DECIMAL(12,6) DEFAULT NULL,
              `sessions_max_day_share_7` DECIMAL(12,6) DEFAULT NULL,

              `clicks_7d` DECIMAL(20,2) DEFAULT NULL,
              `impressions_7d` DECIMAL(20,2) DEFAULT NULL,
              `ad_spend_7d` DECIMAL(20,4) DEFAULT NULL,
              `ad_orders_7d` DECIMAL(18,2) DEFAULT NULL,
              `ad_sales_7d` DECIMAL(20,4) DEFAULT NULL,
              `promotion_units_7d` DECIMAL(18,2) DEFAULT NULL,
              `avg_price_7d` DECIMAL(18,4) DEFAULT NULL,
              `ad_spend_30d` DECIMAL(20,4) DEFAULT NULL,
              `promotion_units_30d` DECIMAL(18,2) DEFAULT NULL,
              `avg_price_30d` DECIMAL(18,4) DEFAULT NULL,

              `future_sales_7d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `future_sales_14d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `future_sales_30d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `future_sessions_7d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `future_sessions_14d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `future_sessions_30d` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `future_sales_days_positive_30` INT NOT NULL DEFAULT 0,
              `future_peak_daily_sales_30` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `future_sales_first14` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `future_sales_second14` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `future_sales_30_to_past30_ratio` DECIMAL(14,6) DEFAULT NULL,
              `future_sales_14_to_past14_ratio` DECIMAL(14,6) DEFAULT NULL,

              `dataset_version` VARCHAR(100) NOT NULL,
              `materialized_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`snapshot_date`,`store_name`,`spu`),
              INDEX `idx_nv_spu_snapshot` (`store_name`,`spu`,`snapshot_date`),
              INDEX `idx_nv_age_snapshot` (`age_days`,`snapshot_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='研究层：历史NEW_VISIBLE point-in-time特征+原始未来结果，无伪历史库存'
        """)


def load_monthly_first_sale() -> Dict[Tuple[str, str], date]:
    if not base.table_exists(base.MONTHLY_SALES_TABLE):
        return {}
    cols = set(base.get_columns(base.MONTHLY_SALES_TABLE))
    if not {"店铺", "SPU", "统计日期", "销量"}.issubset(cols):
        return {}
    rows = q(
        f"""
        SELECT `店铺` AS store_name, `SPU` AS spu,
               MIN(`统计日期`) AS first_sale_month
        FROM `{base.MONTHLY_SALES_TABLE}`
        WHERE `店铺` IN (%s,%s,%s,%s)
          AND `SPU` IS NOT NULL AND TRIM(`SPU`)<>''
          AND COALESCE(`销量`,0)>0
        GROUP BY `店铺`,`SPU`
        """,
        TARGET_SHOPS,
    )
    return {
        (str(r["store_name"]).strip(), str(r["spu"]).strip()): to_date(r["first_sale_month"])
        for r in rows
        if r.get("store_name") and r.get("spu") and r.get("first_sale_month")
    }


def rebuild_cohorts(dry_run: bool = False) -> Dict[str, Any]:
    bounds = one(
        f"SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt FROM `{DAILY_TABLE}`"
    )
    if not bounds.get("min_dt"):
        raise RuntimeError(f"{DAILY_TABLE} 为空")
    min_dt = to_date(bounds["min_dt"])
    max_dt = to_date(bounds["max_dt"])
    burn_cutoff = min_dt + timedelta(days=BURN_IN_DAYS)
    label_cutoff = max_dt - timedelta(days=FUTURE_LABEL_DAYS)

    launches = q(
        f"""
        SELECT store_name, spu, MIN(dt) AS first_sale_day
        FROM `{DAILY_TABLE}`
        WHERE sales_units > 0
        GROUP BY store_name, spu
        """
    )
    monthly = load_monthly_first_sale()

    rows = []
    stats = defaultdict(int)
    for r in launches:
        shop = str(r["store_name"]).strip()
        spu = str(r["spu"]).strip()
        fs = to_date(r["first_sale_day"])
        mf = monthly.get((shop, spu))

        confidence = "DAILY+MONTHLY" if mf is not None else "DAILY_ONLY"
        eligible = True
        reason = None
        if fs < burn_cutoff:
            eligible = False
            reason = "LEFT_EDGE_BURN_IN"
        elif fs > label_cutoff:
            eligible = False
            reason = "NO_30D_FUTURE_WINDOW"
        elif mf is not None and month_key(mf) != month_key(fs):
            eligible = False
            reason = "MONTHLY_DAILY_FIRST_SALE_MONTH_MISMATCH"

        full_120 = int(fs <= max_dt - timedelta(days=MAX_AGE_DAYS + FUTURE_LABEL_DAYS))
        stats["total"] += 1
        stats["eligible"] += int(eligible)
        stats["daily_only"] += int(mf is None)
        stats["mismatch"] += int(reason == "MONTHLY_DAILY_FIRST_SALE_MONTH_MISMATCH")
        stats["full_120d"] += full_120
        rows.append(
            (
                shop,
                spu,
                fs,
                month_floor(mf) if mf is not None else None,
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
        **{k: int(v) for k, v in stats.items()},
    }
    if dry_run:
        return summary

    sql = f"""
        INSERT INTO `{COHORT_TABLE}`
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
    with db_cursor() as c:
        for i in range(0, len(rows), BATCH_SIZE):
            c.executemany(sql, rows[i:i+BATCH_SIZE])
    return summary


def lin_slope(values: Sequence[float]) -> Optional[float]:
    n = len(values)
    if n < 2:
        return None
    xbar = (n - 1) / 2.0
    ybar = sum(values) / n
    den = sum((i - xbar) ** 2 for i in range(n))
    if den <= 0:
        return None
    return sum((i - xbar) * (v - ybar) for i, v in enumerate(values)) / den


def coeff_var(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    mean = sum(values) / len(values)
    if mean <= 0:
        return None
    if len(values) == 1:
        return 0.0
    return statistics.pstdev(values) / mean


def sum_range(series: Mapping[date, Mapping[str, Any]], start: date, end: date, field: str) -> float:
    total = 0.0
    d = start
    while d <= end:
        v = series.get(d, {}).get(field)
        if v is not None:
            total += float(v)
        d += timedelta(days=1)
    return total


def values_range(series: Mapping[date, Mapping[str, Any]], start: date, end: date, field: str) -> List[float]:
    out = []
    d = start
    while d <= end:
        v = series.get(d, {}).get(field)
        out.append(float(v) if v is not None else 0.0)
        d += timedelta(days=1)
    return out


def mean_non_null(series: Mapping[date, Mapping[str, Any]], start: date, end: date, field: str) -> Optional[float]:
    vals = []
    d = start
    while d <= end:
        v = series.get(d, {}).get(field)
        if v is not None:
            vals.append(float(v))
        d += timedelta(days=1)
    return sum(vals) / len(vals) if vals else None


def maybe_sum(
    series: Mapping[date, Mapping[str, Any]], start: date, end: date, field: str
) -> Optional[float]:
    has = False
    total = 0.0
    d = start
    while d <= end:
        v = series.get(d, {}).get(field)
        if v is not None:
            has = True
            total += float(v)
        d += timedelta(days=1)
    return total if has else None


def build_snapshot_row(
    shop: str,
    spu: str,
    fs: date,
    snap: date,
    series: Mapping[date, Mapping[str, Any]],
) -> Tuple[Any, ...]:
    def s(days: int, end_offset: int = 0) -> float:
        end = snap + timedelta(days=end_offset)
        start = end - timedelta(days=days - 1)
        return sum_range(series, start, end, "sales_units")

    def t(days: int, end_offset: int = 0) -> float:
        end = snap + timedelta(days=end_offset)
        start = end - timedelta(days=days - 1)
        return sum_range(series, start, end, "sessions")

    sales3 = s(3)
    sales_prev3 = sum_range(series, snap - timedelta(days=5), snap - timedelta(days=3), "sales_units")
    sales7 = s(7)
    sales_prev7 = sum_range(series, snap - timedelta(days=13), snap - timedelta(days=7), "sales_units")
    sales14 = s(14)
    sales_prev14 = sum_range(series, snap - timedelta(days=27), snap - timedelta(days=14), "sales_units")
    sales30 = s(30)

    sess3 = t(3)
    sess_prev3 = sum_range(series, snap - timedelta(days=5), snap - timedelta(days=3), "sessions")
    sess7 = t(7)
    sess_prev7 = sum_range(series, snap - timedelta(days=13), snap - timedelta(days=7), "sessions")
    sess14 = t(14)
    sess_prev14 = sum_range(series, snap - timedelta(days=27), snap - timedelta(days=14), "sessions")
    sess30 = t(30)

    cvr3 = safe_div(sales3, sess3)
    cvr7 = safe_div(sales7, sess7)
    cvrp7 = safe_div(sales_prev7, sess_prev7)
    cvr14 = safe_div(sales14, sess14)
    cvr30 = safe_div(sales30, sess30)

    sv7 = values_range(series, snap - timedelta(days=6), snap, "sales_units")
    tv7 = values_range(series, snap - timedelta(days=6), snap, "sessions")
    sales_pos7 = sum(v > 0 for v in sv7)
    sess_pos7 = sum(v > 0 for v in tv7)
    sales_up7 = sum(sv7[i] > sv7[i-1] for i in range(1, len(sv7)))
    sess_up7 = sum(tv7[i] > tv7[i-1] for i in range(1, len(tv7)))
    sales_max_share = safe_div(max(sv7) if sv7 else 0.0, sum(sv7))
    sess_max_share = safe_div(max(tv7) if tv7 else 0.0, sum(tv7))

    p7_start = snap - timedelta(days=6)
    p30_start = snap - timedelta(days=29)

    f7_start = snap + timedelta(days=1)
    f7_end = snap + timedelta(days=7)
    f14_end = snap + timedelta(days=14)
    f30_end = snap + timedelta(days=30)
    future_sales7 = sum_range(series, f7_start, f7_end, "sales_units")
    future_sales14 = sum_range(series, f7_start, f14_end, "sales_units")
    future_sales30 = sum_range(series, f7_start, f30_end, "sales_units")
    future_sess7 = sum_range(series, f7_start, f7_end, "sessions")
    future_sess14 = sum_range(series, f7_start, f14_end, "sessions")
    future_sess30 = sum_range(series, f7_start, f30_end, "sessions")
    fv30 = values_range(series, f7_start, f30_end, "sales_units")
    future_pos30 = sum(v > 0 for v in fv30)
    future_peak30 = max(fv30) if fv30 else 0.0
    future_first14 = future_sales14
    future_second14 = sum_range(
        series, snap + timedelta(days=15), snap + timedelta(days=28), "sales_units"
    )

    return (
        snap, shop, spu, fs, (snap - fs).days,
        sales3, sales_prev3, sales7, sales_prev7, sales14, sales_prev14, sales30,
        sess3, sess_prev3, sess7, sess_prev7, sess14, sess_prev14, sess30,
        finite(cvr3), finite(cvr7), finite(cvrp7), finite(cvr14), finite(cvr30),
        finite(safe_div(sales3, sales_prev3)),
        finite(safe_div(sales7, sales_prev7)),
        finite(safe_div(sess3, sess_prev3)),
        finite(safe_div(sess7, sess_prev7)),
        finite(safe_div(cvr7, cvrp7 if cvrp7 is not None else 0.0)),
        int(sales_pos7), int(sess_pos7), int(sales_up7), int(sess_up7),
        finite(lin_slope(sv7)), finite(lin_slope(tv7)),
        finite(coeff_var(sv7)), finite(coeff_var(tv7)),
        finite(sales_max_share), finite(sess_max_share),
        maybe_sum(series, p7_start, snap, "clicks"),
        maybe_sum(series, p7_start, snap, "impressions"),
        maybe_sum(series, p7_start, snap, "ad_spend"),
        maybe_sum(series, p7_start, snap, "ad_orders"),
        maybe_sum(series, p7_start, snap, "ad_sales"),
        maybe_sum(series, p7_start, snap, "promotion_units"),
        mean_non_null(series, p7_start, snap, "avg_price"),
        maybe_sum(series, p30_start, snap, "ad_spend"),
        maybe_sum(series, p30_start, snap, "promotion_units"),
        mean_non_null(series, p30_start, snap, "avg_price"),
        future_sales7, future_sales14, future_sales30,
        future_sess7, future_sess14, future_sess30,
        int(future_pos30), future_peak30, future_first14, future_second14,
        finite(safe_div(future_sales30, sales30)),
        finite(safe_div(future_sales14, sales14)),
        DATASET_VERSION,
    )


SNAPSHOT_COLS = [
    "snapshot_date","store_name","spu","first_sale_day","age_days",
    "sales_3d","sales_prev_3d","sales_7d","sales_prev_7d","sales_14d","sales_prev_14d","sales_30d",
    "sessions_3d","sessions_prev_3d","sessions_7d","sessions_prev_7d","sessions_14d","sessions_prev_14d","sessions_30d",
    "cvr_3d","cvr_7d","cvr_prev_7d","cvr_14d","cvr_30d",
    "sales_growth_3d","sales_growth_7d","sessions_growth_3d","sessions_growth_7d","cvr_ratio_7d",
    "sales_positive_days_7","sessions_positive_days_7","sales_up_days_7","sessions_up_days_7",
    "sales_slope_7","sessions_slope_7","sales_cv_7","sessions_cv_7","sales_max_day_share_7","sessions_max_day_share_7",
    "clicks_7d","impressions_7d","ad_spend_7d","ad_orders_7d","ad_sales_7d","promotion_units_7d","avg_price_7d",
    "ad_spend_30d","promotion_units_30d","avg_price_30d",
    "future_sales_7d","future_sales_14d","future_sales_30d","future_sessions_7d","future_sessions_14d","future_sessions_30d",
    "future_sales_days_positive_30","future_peak_daily_sales_30","future_sales_first14","future_sales_second14",
    "future_sales_30_to_past30_ratio","future_sales_14_to_past14_ratio",
    "dataset_version",
]


def save_snapshot_rows(rows: Sequence[Tuple[Any, ...]]) -> None:
    if not rows:
        return
    update_cols = [c for c in SNAPSHOT_COLS if c not in ("snapshot_date","store_name","spu")]
    sql = f"""
        INSERT INTO `{SNAPSHOT_TABLE}`
        ({','.join(f'`{c}`' for c in SNAPSHOT_COLS)})
        VALUES ({','.join(['%s'] * len(SNAPSHOT_COLS))})
        ON DUPLICATE KEY UPDATE
          {','.join(f'`{c}`=VALUES(`{c}`)' for c in update_cols)},
          `materialized_at`=CURRENT_TIMESTAMP
    """
    with db_cursor() as c:
        for i in range(0, len(rows), BATCH_SIZE):
            c.executemany(sql, rows[i:i+BATCH_SIZE])


def load_cohort_daily(shop: str, spu: str, fs: date, max_dt: date) -> Dict[date, Dict[str, Any]]:
    start = fs - timedelta(days=29)
    end = min(max_dt, fs + timedelta(days=MAX_AGE_DAYS + FUTURE_LABEL_DAYS))
    rows = q(
        f"""
        SELECT dt, sales_units, sessions, clicks, impressions, ad_spend, ad_orders,
               ad_sales, promotion_units, avg_price
        FROM `{DAILY_TABLE}`
        WHERE store_name=%s AND spu=%s AND dt BETWEEN %s AND %s
        ORDER BY dt
        """,
        (shop, spu, start, end),
    )
    return {to_date(r["dt"]): r for r in rows}


def build_snapshots(
    dry_run: bool = False,
    cohort_start: Optional[date] = None,
    cohort_end: Optional[date] = None,
    max_cohorts: Optional[int] = None,
) -> Dict[str, Any]:
    where = ["eligible_strict=1"]
    params: List[Any] = []
    if cohort_start is not None:
        where.append("first_sale_day >= %s")
        params.append(cohort_start)
    if cohort_end is not None:
        where.append("first_sale_day <= %s")
        params.append(cohort_end)

    cohorts = q(
        f"""
        SELECT store_name, spu, first_sale_day, base_max_dt, cohort_confidence
        FROM `{COHORT_TABLE}`
        WHERE {' AND '.join(where)}
        ORDER BY first_sale_day, store_name, spu
        """,
        params,
    )
    if max_cohorts is not None:
        cohorts = cohorts[:max_cohorts]

    summary = defaultdict(int)
    per_shop = defaultdict(int)
    sample_rows = []
    for idx, r in enumerate(cohorts, start=1):
        shop = str(r["store_name"])
        spu = str(r["spu"])
        fs = to_date(r["first_sale_day"])
        max_dt = to_date(r["base_max_dt"])
        series = load_cohort_daily(shop, spu, fs, max_dt)

        snap_end = min(fs + timedelta(days=MAX_AGE_DAYS), max_dt - timedelta(days=FUTURE_LABEL_DAYS))
        if snap_end < fs:
            continue
        payload = []
        snap = fs
        while snap <= snap_end:
            row = build_snapshot_row(shop, spu, fs, snap, series)
            payload.append(row)
            if len(sample_rows) < 5:
                sample_rows.append({
                    "snapshot_date": str(row[0]),
                    "store": shop,
                    "spu": spu,
                    "age_days": row[4],
                    "sales_7d": row[7],
                    "sessions_7d": row[14],
                    "future_sales_30d": row[51],
                })
            snap += timedelta(days=1)

        if not dry_run:
            save_snapshot_rows(payload)
        summary["cohorts"] += 1
        summary["snapshots"] += len(payload)
        per_shop[shop] += len(payload)
        if idx % 100 == 0:
            logger.info(
                f"NEW_VISIBLE历史snapshot进度: cohorts={idx}/{len(cohorts)}, snapshots={summary['snapshots']}"
            )

    return {
        "cohorts": int(summary["cohorts"]),
        "snapshots": int(summary["snapshots"]),
        "per_shop_snapshot_rows": dict(per_shop),
        "sample_rows": sample_rows,
        "dry_run": dry_run,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cohort-start", default=None)
    ap.add_argument("--cohort-end", default=None)
    ap.add_argument("--max-cohorts", type=int, default=None)
    ap.add_argument("--cohorts-only", action="store_true")
    args = ap.parse_args()

    if not base.table_exists(DAILY_TABLE):
        raise RuntimeError(
            f"{DAILY_TABLE} 不存在，先运行 jobs.forecast_research.build_spu_daily_history"
        )

    if not args.dry_run:
        ensure_tables()

    # In dry-run cohort rebuild does not require the cohort table to exist.
    cohort_summary = rebuild_cohorts(dry_run=args.dry_run)
    print("LAUNCH_COHORT_SUMMARY=" + json.dumps(cohort_summary, ensure_ascii=False))

    if args.cohorts_only:
        return 0

    if args.dry_run:
        # Dry-run cannot query an unpersisted cohort table. If a cohort table already
        # exists, it can still simulate snapshot generation; otherwise stop after cohort audit.
        if not base.table_exists(COHORT_TABLE):
            print(
                "NEW_VISIBLE_SNAPSHOT_DRY_RUN="
                + json.dumps(
                    {
                        "status": "COHORT_TABLE_NOT_YET_PERSISTED",
                        "next": "run once with --cohorts-only, then rerun --dry-run",
                    },
                    ensure_ascii=False,
                )
            )
            return 0

    cs = datetime.strptime(args.cohort_start, "%Y-%m-%d").date() if args.cohort_start else None
    ce = datetime.strptime(args.cohort_end, "%Y-%m-%d").date() if args.cohort_end else None
    snap_summary = build_snapshots(
        dry_run=args.dry_run,
        cohort_start=cs,
        cohort_end=ce,
        max_cohorts=args.max_cohorts,
    )
    print("NEW_VISIBLE_SNAPSHOT_SUMMARY=" + json.dumps(snap_summary, ensure_ascii=False, default=str))

    if not args.dry_run:
        final = one(
            f"""
            SELECT MIN(snapshot_date) AS min_dt, MAX(snapshot_date) AS max_dt,
                   COUNT(*) AS rows_n,
                   COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS launch_n
            FROM `{SNAPSHOT_TABLE}`
            """
        )
        print(
            "NEW_VISIBLE_SNAPSHOT_FINAL="
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
