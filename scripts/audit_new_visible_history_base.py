#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of the historical NEW_VISIBLE research base.

Run after forecast_research_spu_daily_history has been materialized.
No table is created or modified.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research.build_spu_daily_history import DEST_TABLE

MAX_NEW_VISIBLE_AGE_DAYS = 120
LABEL_FUTURE_DAYS = 30
BURN_IN_DAYS = 60


def to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params=()):
    rows = q(sql, params)
    return rows[0] if rows else {}


def main() -> int:
    if not base.table_exists(DEST_TABLE):
        raise RuntimeError(
            f"{DEST_TABLE} 不存在。先运行 jobs.forecast_research.build_spu_daily_history"
        )

    bounds = one(
        f"""
        SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt, COUNT(*) AS rows_n,
               COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS shop_spu_n,
               COUNT(DISTINCT spu) AS spu_n,
               COUNT(DISTINCT dt) AS days_n
        FROM `{DEST_TABLE}`
        """
    )
    min_dt = to_date(bounds["min_dt"])
    max_dt = to_date(bounds["max_dt"])
    print("=" * 96)
    print("NEW_VISIBLE 历史训练底座审计（只读）")
    print("=" * 96)
    print(
        "BASE="
        + json.dumps(
            {
                "min_dt": str(min_dt),
                "max_dt": str(max_dt),
                "rows_n": int(bounds.get("rows_n", 0) or 0),
                "shop_spu_n": int(bounds.get("shop_spu_n", 0) or 0),
                "spu_n": int(bounds.get("spu_n", 0) or 0),
                "days_n": int(bounds.get("days_n", 0) or 0),
            },
            ensure_ascii=False,
        )
    )

    print("\n=== 1. 日连续性 ===")
    day_rows = q(
        f"""
        SELECT store_name, dt, COUNT(*) AS rows_n,
               SUM(sales_units) AS sales_units,
               SUM(sessions) AS sessions
        FROM `{DEST_TABLE}`
        GROUP BY store_name, dt
        ORDER BY store_name, dt
        """
    )
    by_shop: Dict[str, Dict[date, Dict[str, Any]]] = defaultdict(dict)
    for r in day_rows:
        by_shop[str(r["store_name"])][to_date(r["dt"])] = r
    for shop in TARGET_SHOPS:
        dm = by_shop.get(shop, {})
        expected = (max_dt - min_dt).days + 1
        missing = [
            min_dt + timedelta(days=i)
            for i in range(expected)
            if min_dt + timedelta(days=i) not in dm
        ]
        print(
            json.dumps(
                {
                    "shop": shop,
                    "days_present": len(dm),
                    "expected_days": expected,
                    "missing_days_n": len(missing),
                    "first_missing": [str(x) for x in missing[:10]],
                },
                ensure_ascii=False,
            )
        )

    # Daily first sale at exact day grain.
    launches = q(
        f"""
        SELECT store_name, spu, MIN(dt) AS first_sale_day,
               SUM(sales_units) AS lifetime_sales_in_base
        FROM `{DEST_TABLE}`
        WHERE sales_units > 0
        GROUP BY store_name, spu
        """
    )
    daily_first = {
        (str(r["store_name"]), str(r["spu"])): to_date(r["first_sale_day"])
        for r in launches
    }

    # Monthly first-sale is used only as a coverage/censoring cross-check.
    monthly_first: Dict[Tuple[str, str], date] = {}
    if base.table_exists(base.MONTHLY_SALES_TABLE):
        cols = set(base.get_columns(base.MONTHLY_SALES_TABLE))
        if {"店铺", "SPU", "统计日期", "销量"}.issubset(cols):
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
            monthly_first = {
                (str(r["store_name"]), str(r["spu"])): to_date(r["first_sale_month"])
                for r in rows
            }

    print("\n=== 2. 首销覆盖与左截断 ===")
    left_censored = 0
    aligned_month = 0
    mismatch_gt31 = 0
    no_monthly = 0
    launch_by_shop = Counter()
    cohort_by_month = Counter()
    eligible_launches = set()

    burn_cutoff = min_dt + timedelta(days=BURN_IN_DAYS)
    label_cutoff = max_dt - timedelta(days=LABEL_FUTURE_DAYS)

    for key, d in daily_first.items():
        shop, _spu = key
        launch_by_shop[shop] += 1
        cohort_by_month[(d.year, d.month)] += 1
        m = monthly_first.get(key)
        if m is None:
            no_monthly += 1
        else:
            # If monthly data says the item sold before our daily base starts, the
            # observed first positive day is left-censored and cannot be treated as launch.
            if m < date(min_dt.year, min_dt.month, 1):
                left_censored += 1
            delta = abs((d - m).days)
            if d.year == m.year and d.month == m.month:
                aligned_month += 1
            elif delta > 31:
                mismatch_gt31 += 1

        if d >= burn_cutoff and d <= label_cutoff:
            # If monthly history proves an older launch, exclude it.
            if m is None or m >= date(min_dt.year, min_dt.month, 1):
                eligible_launches.add(key)

    print(
        json.dumps(
            {
                "daily_positive_shop_spu": len(daily_first),
                "monthly_first_sale_keys": len(monthly_first),
                "left_censored_by_monthly_history": left_censored,
                "same_calendar_month_daily_vs_monthly": aligned_month,
                "mismatch_gt31_days": mismatch_gt31,
                "no_monthly_first_sale_match": no_monthly,
                "burn_in_days": BURN_IN_DAYS,
                "future_label_days": LABEL_FUTURE_DAYS,
                "eligible_launch_shop_spu": len(eligible_launches),
                "eligible_first_sale_range": [str(burn_cutoff), str(label_cutoff)],
            },
            ensure_ascii=False,
        )
    )
    print("按店首销数:", json.dumps(dict(launch_by_shop), ensure_ascii=False))
    print(
        "最近24个月cohort:",
        json.dumps(
            [
                {"month": f"{y}-{m:02d}", "launches": n}
                for (y, m), n in sorted(cohort_by_month.items())[-24:]
            ],
            ensure_ascii=False,
        ),
    )

    print("\n=== 3. 可形成的严格NEW_VISIBLE历史snapshot ===")
    # Count actual rows during age 0..120 with at least 30 future days remaining.
    # This does not invent missing days; it measures the materialized history actually available.
    snapshot_rows = q(
        f"""
        WITH first_sale AS (
          SELECT store_name, spu, MIN(dt) AS first_sale_day
          FROM `{DEST_TABLE}`
          WHERE sales_units > 0
          GROUP BY store_name, spu
        )
        SELECT h.store_name,
               COUNT(*) AS snapshot_rows,
               COUNT(DISTINCT CONCAT(h.store_name,'|',h.spu)) AS launch_n,
               MIN(h.dt) AS min_snapshot,
               MAX(h.dt) AS max_snapshot
        FROM `{DEST_TABLE}` h
        JOIN first_sale f
          ON f.store_name=h.store_name AND f.spu=h.spu
        WHERE f.first_sale_day >= %s
          AND f.first_sale_day <= %s
          AND h.dt BETWEEN f.first_sale_day
                       AND DATE_ADD(f.first_sale_day, INTERVAL %s DAY)
          AND h.dt <= DATE_SUB(%s, INTERVAL %s DAY)
        GROUP BY h.store_name
        ORDER BY h.store_name
        """,
        (
            burn_cutoff,
            label_cutoff,
            MAX_NEW_VISIBLE_AGE_DAYS,
            max_dt,
            LABEL_FUTURE_DAYS,
        ),
    )
    total_snapshots = 0
    total_launches = 0
    for r in snapshot_rows:
        total_snapshots += int(r["snapshot_rows"] or 0)
        total_launches += int(r["launch_n"] or 0)
        print(
            json.dumps(
                {
                    "shop": r["store_name"],
                    "snapshot_rows": int(r["snapshot_rows"] or 0),
                    "launch_n": int(r["launch_n"] or 0),
                    "min_snapshot": str(r["min_snapshot"]),
                    "max_snapshot": str(r["max_snapshot"]),
                },
                ensure_ascii=False,
            )
        )
    print(
        "SNAPSHOT_CAPACITY="
        + json.dumps(
            {
                "total_snapshot_rows": total_snapshots,
                "shop_launch_sum": total_launches,
                "max_new_visible_age_days": MAX_NEW_VISIBLE_AGE_DAYS,
                "future_label_days": LABEL_FUTURE_DAYS,
            },
            ensure_ascii=False,
        )
    )

    print("\n=== 4. 扩展特征覆盖 ===")
    cov = one(
        f"""
        SELECT COUNT(*) AS n,
               SUM(clicks IS NOT NULL) AS clicks_n,
               SUM(impressions IS NOT NULL) AS impressions_n,
               SUM(ad_spend IS NOT NULL) AS ad_spend_n,
               SUM(ad_orders IS NOT NULL) AS ad_orders_n,
               SUM(ad_sales IS NOT NULL) AS ad_sales_n,
               SUM(promotion_units IS NOT NULL) AS promotion_n,
               SUM(avg_price IS NOT NULL) AS price_n
        FROM `{DEST_TABLE}`
        WHERE dt BETWEEN %s AND %s
        """,
        (burn_cutoff, label_cutoff),
    )
    n = int(cov.get("n", 0) or 0)
    coverage = {}
    for k in (
        "clicks",
        "impressions",
        "ad_spend",
        "ad_orders",
        "ad_sales",
        "promotion",
        "price",
    ):
        count = int(cov.get(k + "_n", 0) or 0)
        coverage[k] = {
            "non_null": count,
            "rate": round(count / n, 4) if n else None,
        }
    print(json.dumps({"rows": n, "coverage": coverage}, ensure_ascii=False))

    print("\n=== 5. 结论边界 ===")
    print("这一步只验证历史训练底座是否足够，不定义Breakout标签，也不训练模型。")
    print("历史库存不会被伪造；2026-09-30以前的历史训练暂不使用库存特征。")
    print("只有通过burn-in且拥有至少30天未来观察窗的launch才进入下一阶段。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
