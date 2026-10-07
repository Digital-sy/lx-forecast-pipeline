#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Materialize training-identical live CORE features for current NEW_VISIBLE rows.

Shadow-only. No production forecast/procurement tables are modified.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as mon
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research import build_new_visible_snapshots as hist
from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as stage1

DEST_TABLE = "forecast_new_visible_core_snapshot_daily"
SPECIAL_EXCLUSION_TABLE = "forecast_special_spu_exclusion"
BUILD_VERSION = "LIVE_CORE_V1_TRAINING_IDENTICAL"
BATCH_SIZE = 500


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


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return float(a) / float(b)


def finite(v: Optional[float]) -> Optional[float]:
    if v is None:
        return None
    f = float(v)
    return f if math.isfinite(f) else None


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


def sum_range(series, start: date, end: date, field: str) -> float:
    total = 0.0
    d = start
    while d <= end:
        v = series.get(d, {}).get(field)
        if v is not None:
            total += float(v)
        d += timedelta(days=1)
    return total


def values_range(series, start: date, end: date, field: str) -> List[float]:
    out: List[float] = []
    d = start
    while d <= end:
        v = series.get(d, {}).get(field)
        out.append(float(v) if v is not None else 0.0)
        d += timedelta(days=1)
    return out


def ensure_table() -> None:
    numeric_sql = """
      age_days INT NOT NULL,
      sales_3d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sales_prev_3d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sales_7d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sales_prev_7d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sales_14d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sales_prev_14d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sales_30d DECIMAL(18,2) NOT NULL DEFAULT 0,
      sessions_3d DECIMAL(20,2) NOT NULL DEFAULT 0,
      sessions_prev_3d DECIMAL(20,2) NOT NULL DEFAULT 0,
      sessions_7d DECIMAL(20,2) NOT NULL DEFAULT 0,
      sessions_prev_7d DECIMAL(20,2) NOT NULL DEFAULT 0,
      sessions_14d DECIMAL(20,2) NOT NULL DEFAULT 0,
      sessions_prev_14d DECIMAL(20,2) NOT NULL DEFAULT 0,
      sessions_30d DECIMAL(20,2) NOT NULL DEFAULT 0,
      cvr_3d DECIMAL(14,6) DEFAULT NULL,
      cvr_7d DECIMAL(14,6) DEFAULT NULL,
      cvr_prev_7d DECIMAL(14,6) DEFAULT NULL,
      cvr_14d DECIMAL(14,6) DEFAULT NULL,
      cvr_30d DECIMAL(14,6) DEFAULT NULL,
      sales_growth_3d DECIMAL(16,6) DEFAULT NULL,
      sales_growth_7d DECIMAL(16,6) DEFAULT NULL,
      sessions_growth_3d DECIMAL(16,6) DEFAULT NULL,
      sessions_growth_7d DECIMAL(16,6) DEFAULT NULL,
      cvr_ratio_7d DECIMAL(16,6) DEFAULT NULL,
      sales_positive_days_7 INT NOT NULL DEFAULT 0,
      sessions_positive_days_7 INT NOT NULL DEFAULT 0,
      sales_up_days_7 INT NOT NULL DEFAULT 0,
      sessions_up_days_7 INT NOT NULL DEFAULT 0,
      sales_slope_7 DECIMAL(20,6) DEFAULT NULL,
      sessions_slope_7 DECIMAL(20,6) DEFAULT NULL,
      sales_cv_7 DECIMAL(16,6) DEFAULT NULL,
      sessions_cv_7 DECIMAL(16,6) DEFAULT NULL,
      sales_max_day_share_7 DECIMAL(16,6) DEFAULT NULL,
      sessions_max_day_share_7 DECIMAL(16,6) DEFAULT NULL
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
      snapshot_date DATE NOT NULL,
      as_of_date DATE NOT NULL,
      store_name VARCHAR(200) NOT NULL,
      spu VARCHAR(200) NOT NULL,
      first_sale_day DATE NOT NULL,
      launch_month VARCHAR(7) NOT NULL,
      snapshot_month VARCHAR(7) NOT NULL,
      {numeric_sql},
      build_version VARCHAR(100) NOT NULL,
      materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (snapshot_date, store_name, spu),
      INDEX idx_core_asof_shop (as_of_date, store_name),
      INDEX idx_core_spu (store_name, spu, snapshot_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with db_cursor() as c:
        c.execute(sql)


def latest_scope() -> Tuple[date, date, List[Tuple[str, str]]]:
    feature_latest = one(
        f"SELECT MAX(snapshot_date) AS d FROM {mon.FEATURE_SNAPSHOT_TABLE} "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    ).get("d")
    daily_latest = one(
        f"SELECT MAX(dt) AS d FROM {hist.DAILY_TABLE} "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    ).get("d")
    if not feature_latest or not daily_latest:
        raise RuntimeError("missing latest feature snapshot or daily research date")

    feature_latest = to_date(feature_latest)
    daily_latest = to_date(daily_latest)

    rows = q(
        f"SELECT store_name, spu FROM {mon.FEATURE_SNAPSHOT_TABLE} "
        "WHERE snapshot_date=%s AND store_name IN (%s,%s,%s,%s) "
        "AND forecastability=%s",
        (feature_latest, *TARGET_SHOPS, "NEW_VISIBLE"),
    )
    keys = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip())
        for r in rows
        if r.get("store_name") and r.get("spu")
    }

    excluded = set()
    if mon.table_exists(SPECIAL_EXCLUSION_TABLE):
        ex = q(
            f"SELECT DISTINCT UPPER(TRIM(spu)) AS spu FROM {SPECIAL_EXCLUSION_TABLE} "
            "WHERE exclusion_code IN (%s,%s)",
            ("LCS_SPECIAL_LOW_PRICE", "XH_PREFIX_EXCLUSION"),
        )
        excluded = {str(r.get("spu") or "").strip().upper() for r in ex}

    normal = sorted(
        (shop, spu)
        for shop, spu in keys
        if spu.upper() not in excluded and not spu.upper().startswith("XH")
    )
    return feature_latest, daily_latest, normal


def load_first_sales(keys: Sequence[Tuple[str, str]]) -> Dict[Tuple[str, str], date]:
    if not keys:
        return {}
    shops = sorted(set(x[0] for x in keys))
    spus = sorted(set(x[1] for x in keys))
    shop_ph = ",".join(["%s"] * len(shops))
    spu_ph = ",".join(["%s"] * len(spus))
    rows = q(
        f"SELECT store_name, spu, MIN(dt) AS first_sale_day "
        f"FROM {hist.DAILY_TABLE} WHERE sales_units>0 "
        f"AND store_name IN ({shop_ph}) AND spu IN ({spu_ph}) "
        "GROUP BY store_name, spu",
        (*shops, *spus),
    )
    wanted = set(keys)
    out = {}
    for r in rows:
        key = (
            str(r.get("store_name") or "").strip(),
            str(r.get("spu") or "").strip(),
        )
        if key in wanted and r.get("first_sale_day"):
            out[key] = to_date(r["first_sale_day"])
    return out


def load_series(keys: Sequence[Tuple[str, str]], as_of: date):
    if not keys:
        return {}
    start = as_of - timedelta(days=29)
    shops = sorted(set(x[0] for x in keys))
    spus = sorted(set(x[1] for x in keys))
    shop_ph = ",".join(["%s"] * len(shops))
    spu_ph = ",".join(["%s"] * len(spus))
    rows = q(
        f"SELECT dt,store_name,spu,sales_units,sessions FROM {hist.DAILY_TABLE} "
        f"WHERE dt BETWEEN %s AND %s "
        f"AND store_name IN ({shop_ph}) AND spu IN ({spu_ph})",
        (start, as_of, *shops, *spus),
    )
    wanted = set(keys)
    out = {}
    for r in rows:
        key = (
            str(r.get("store_name") or "").strip(),
            str(r.get("spu") or "").strip(),
        )
        if key not in wanted:
            continue
        out.setdefault(key, {})[to_date(r["dt"])] = {
            "sales_units": float(r.get("sales_units", 0) or 0),
            "sessions": float(r.get("sessions", 0) or 0),
        }
    return out


def feature_row(snapshot_date, as_of, shop, spu, fs, series):
    def s(days: int) -> float:
        return sum_range(series, as_of - timedelta(days=days - 1), as_of, "sales_units")

    def t(days: int) -> float:
        return sum_range(series, as_of - timedelta(days=days - 1), as_of, "sessions")

    sales3 = s(3)
    sales_prev3 = sum_range(series, as_of - timedelta(days=5), as_of - timedelta(days=3), "sales_units")
    sales7 = s(7)
    sales_prev7 = sum_range(series, as_of - timedelta(days=13), as_of - timedelta(days=7), "sales_units")
    sales14 = s(14)
    sales_prev14 = sum_range(series, as_of - timedelta(days=27), as_of - timedelta(days=14), "sales_units")
    sales30 = s(30)

    sess3 = t(3)
    sess_prev3 = sum_range(series, as_of - timedelta(days=5), as_of - timedelta(days=3), "sessions")
    sess7 = t(7)
    sess_prev7 = sum_range(series, as_of - timedelta(days=13), as_of - timedelta(days=7), "sessions")
    sess14 = t(14)
    sess_prev14 = sum_range(series, as_of - timedelta(days=27), as_of - timedelta(days=14), "sessions")
    sess30 = t(30)

    cvr3 = safe_div(sales3, sess3)
    cvr7 = safe_div(sales7, sess7)
    cvrp7 = safe_div(sales_prev7, sess_prev7)
    cvr14 = safe_div(sales14, sess14)
    cvr30 = safe_div(sales30, sess30)

    sv7 = values_range(series, as_of - timedelta(days=6), as_of, "sales_units")
    tv7 = values_range(series, as_of - timedelta(days=6), as_of, "sessions")

    return {
        "snapshot_date": snapshot_date,
        "as_of_date": as_of,
        "store_name": shop,
        "spu": spu,
        "first_sale_day": fs,
        # Must match train_new_visible_v1_stage1.load_rows exactly:
        # categorical month features are month-number strings, not YYYY-MM.
        "launch_month": str(fs.month),
        "snapshot_month": str(snapshot_date.month),
        "age_days": (as_of - fs).days,
        "sales_3d": sales3,
        "sales_prev_3d": sales_prev3,
        "sales_7d": sales7,
        "sales_prev_7d": sales_prev7,
        "sales_14d": sales14,
        "sales_prev_14d": sales_prev14,
        "sales_30d": sales30,
        "sessions_3d": sess3,
        "sessions_prev_3d": sess_prev3,
        "sessions_7d": sess7,
        "sessions_prev_7d": sess_prev7,
        "sessions_14d": sess14,
        "sessions_prev_14d": sess_prev14,
        "sessions_30d": sess30,
        "cvr_3d": finite(cvr3),
        "cvr_7d": finite(cvr7),
        "cvr_prev_7d": finite(cvrp7),
        "cvr_14d": finite(cvr14),
        "cvr_30d": finite(cvr30),
        "sales_growth_3d": finite(safe_div(sales3, sales_prev3)),
        "sales_growth_7d": finite(safe_div(sales7, sales_prev7)),
        "sessions_growth_3d": finite(safe_div(sess3, sess_prev3)),
        "sessions_growth_7d": finite(safe_div(sess7, sess_prev7)),
        "cvr_ratio_7d": finite(safe_div(cvr7, cvrp7 if cvrp7 is not None else 0.0)),
        "sales_positive_days_7": int(sum(v > 0 for v in sv7)),
        "sessions_positive_days_7": int(sum(v > 0 for v in tv7)),
        "sales_up_days_7": int(sum(sv7[i] > sv7[i - 1] for i in range(1, len(sv7)))),
        "sessions_up_days_7": int(sum(tv7[i] > tv7[i - 1] for i in range(1, len(tv7)))),
        "sales_slope_7": finite(lin_slope(sv7)),
        "sessions_slope_7": finite(lin_slope(tv7)),
        "sales_cv_7": finite(coeff_var(sv7)),
        "sessions_cv_7": finite(coeff_var(tv7)),
        "sales_max_day_share_7": finite(safe_div(max(sv7) if sv7 else 0.0, sum(sv7))),
        "sessions_max_day_share_7": finite(safe_div(max(tv7) if tv7 else 0.0, sum(tv7))),
        "build_version": BUILD_VERSION,
    }


def schema_guard(rows) -> None:
    required = set(abl.CORE_NUMERIC) | set(stage1.CATEGORICAL_FEATURES)
    for r in rows:
        missing = required - set(r)
        if missing:
            raise RuntimeError("live CORE row missing fields: " + ",".join(sorted(missing)))


def persist(rows) -> None:
    if not rows:
        return
    columns = [
        "snapshot_date","as_of_date","store_name","spu","first_sale_day",
        "launch_month","snapshot_month", *abl.CORE_NUMERIC, "build_version",
    ]
    placeholders = ",".join(["%s"] * len(columns))
    update_sql = ",".join(
        f"{c}=VALUES({c})"
        for c in columns
        if c not in ("snapshot_date","store_name","spu")
    )
    sql = (
        f"INSERT INTO {DEST_TABLE} ({','.join(columns)}) "
        f"VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_sql}"
    )
    payload = [tuple(r.get(c) for c in columns) for r in rows]
    with db_cursor() as c:
        for i in range(0, len(payload), BATCH_SIZE):
            c.executemany(sql, payload[i:i + BATCH_SIZE])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    snapshot_date, as_of, keys = latest_scope()
    first_sales = load_first_sales(keys)
    missing_first_sale = sorted(set(keys) - set(first_sales))
    valid_keys = [k for k in keys if k in first_sales]
    series = load_series(valid_keys, as_of)

    rows = [
        feature_row(
            snapshot_date, as_of, shop, spu, first_sales[(shop, spu)],
            series.get((shop, spu), {}),
        )
        for shop, spu in valid_keys
    ]
    schema_guard(rows)

    age_buckets = {}
    for r in rows:
        age = int(r["age_days"])
        bucket = (
            "0_6" if age < 7 else
            "7_13" if age < 14 else
            "14_29" if age < 30 else
            "30_59" if age < 60 else
            "60_89" if age < 90 else
            "90_120" if age <= 120 else
            "GT120"
        )
        age_buckets[bucket] = age_buckets.get(bucket, 0) + 1

    print("LIVE_CORE_SCOPE=" + json.dumps({
        "snapshot_date": str(snapshot_date),
        "as_of_date": str(as_of),
        "eligible_new_visible": len(keys),
        "rows_with_exact_daily_first_sale": len(rows),
        "missing_first_sale_n": len(missing_first_sale),
        "missing_first_sale_sample": [
            {"store_name": x[0], "spu": x[1]} for x in missing_first_sale[:20]
        ],
        "age_buckets": age_buckets,
        "core_numeric_n": len(abl.CORE_NUMERIC),
        "categorical_features": list(stage1.CATEGORICAL_FEATURES),
        "dry_run": args.dry_run,
    }, ensure_ascii=False))

    if rows:
        s = rows[0]
        print("LIVE_CORE_SAMPLE=" + json.dumps({
            k: s.get(k)
            for k in (
                "store_name","spu","first_sale_day","age_days",
                "sales_3d","sales_7d","sales_14d","sales_30d",
                "sessions_3d","sessions_7d","sessions_14d","sessions_30d",
                "sales_slope_7","sales_cv_7","sales_max_day_share_7",
                "launch_month","snapshot_month",
            )
        }, ensure_ascii=False, default=str))

    if not args.dry_run:
        ensure_table()
        persist(rows)
        check = one(
            f"SELECT COUNT(*) AS n, COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS k "
            f"FROM {DEST_TABLE} WHERE snapshot_date=%s",
            (snapshot_date,),
        )
        print("LIVE_CORE_PERSISTED=" + json.dumps({
            "table": DEST_TABLE,
            "snapshot_date": str(snapshot_date),
            "rows": int(check.get("n", 0) or 0),
            "shop_spu": int(check.get("k", 0) or 0),
        }, ensure_ascii=False))

    if missing_first_sale:
        print("LIVE_CORE_WARNING=" + json.dumps({
            "reason": "missing exact daily first sale",
            "count": len(missing_first_sale),
            "action": "do not score these rows",
        }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
