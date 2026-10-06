#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Materialize strict historical SPU-day performance for NEW_VISIBLE research.

Research-only. This job never modifies production forecast/procurement tables.

Why this table exists
---------------------
The live monitoring job can query a 30-day window every day, but historical model
research needs thousands of point-in-time snapshots. Re-scanning the 100M-row ODS table
for every snapshot would be slow and risky. This job performs one bounded monthly scan
at a time and collapses source rows to:

    date x store x SPU

The resulting table is the reusable source for historical NEW_VISIBLE features, V0
rule backtests, and Breakout V1 training.

Leakage guard
-------------
Historical materialization requires the performance source's OWN `spu` column.
It deliberately refuses to join today's product-management mapping to historical rows.
That prevents a current mapping from being silently projected into the past.

Inventory is NOT backfilled here. We only have true daily inventory snapshots from
2026-09-30 onward, so historical model research before that date must treat inventory as
unavailable or use inventory-independent labels/features.
"""
from __future__ import annotations

import argparse
import calendar
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import get_logger
from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS

logger = get_logger("forecast_research_spu_daily")

DEST_TABLE = "forecast_research_spu_daily_history"
BUILD_VERSION = "spu_daily_v1"

OPTIONAL_FIELDS: Dict[str, Sequence[str]] = {
    "clicks": ("clicks", "click"),
    "impressions": ("impressions", "impression"),
    "spend": ("spend", "ad_spend", "advertising_spend"),
    "ad_orders": ("ad_order_quantity", "ad_orders", "advertising_orders"),
    "ad_sales": ("ad_sales_amount", "ad_sales", "advertising_sales"),
    "promotion_volume": ("promotion_volume", "promo_volume", "promotion_units"),
    "price": ("price", "selling_price", "sale_price"),
}


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def next_month(d: date) -> date:
    return date(d.year + (1 if d.month == 12 else 0), 1 if d.month == 12 else d.month + 1, 1)


def month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def iter_month_ranges(start: date, end: date):
    cur = month_start(start)
    while cur <= end:
        yield max(start, cur), min(end, month_end(cur))
        cur = next_month(cur)


def pick_optional(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    actual = set(columns)
    for c in candidates:
        if c in actual:
            return c
    return None


def source_profile(snapshot_date: date) -> Dict[str, Any]:
    source = v2.resolve_performance_source(snapshot_date)
    table = source["table"]
    columns = base.get_columns(table)
    required = {
        "date": source.get("date"),
        "store": source.get("store"),
        "sku": source.get("sku"),
        "sales": source.get("sales"),
        "sessions": source.get("sessions"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"产品表现源缺少历史研究必需映射: {missing}")

    # Critical anti-leakage rule.
    if "spu" not in set(columns):
        raise RuntimeError(
            f"{table} 没有原生spu字段。历史研究V1拒绝使用当前产品管理表回填历史SPU，"
            "请先建立point-in-time SKU→SPU映射后再继续。"
        )

    optional = {
        name: pick_optional(columns, candidates)
        for name, candidates in OPTIONAL_FIELDS.items()
    }
    return {**source, "columns": columns, "spu": "spu", "optional": optional}


def probe_source_bounds(profile: Dict[str, Any]) -> Tuple[date, date]:
    table = profile["table"]
    dcol = profile["date"]
    store = profile["store"]
    with db_cursor() as c:
        c.execute(
            f"""
            SELECT `{dcol}` AS dt
            FROM {table}
            WHERE `{store}` IN (%s,%s,%s,%s)
              AND `spu` IS NOT NULL AND TRIM(`spu`)<>''
            ORDER BY `{dcol}` ASC
            LIMIT 1
            """,
            TARGET_SHOPS,
        )
        first = c.fetchone()
        c.execute(
            f"""
            SELECT `{dcol}` AS dt
            FROM {table}
            WHERE `{store}` IN (%s,%s,%s,%s)
              AND `spu` IS NOT NULL AND TRIM(`spu`)<>''
            ORDER BY `{dcol}` DESC
            LIMIT 1
            """,
            TARGET_SHOPS,
        )
        last = c.fetchone()
    if not first or not last:
        raise RuntimeError("目标四店在产品表现源中找不到非空SPU历史")
    return _to_date(first["dt"]), _to_date(last["dt"])


def _to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE}` (
              `dt` DATE NOT NULL,
              `store_name` VARCHAR(200) NOT NULL,
              `spu` VARCHAR(200) NOT NULL,
              `sales_units` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sessions` DECIMAL(20,2) NOT NULL DEFAULT 0,
              `clicks` DECIMAL(20,2) DEFAULT NULL,
              `impressions` DECIMAL(20,2) DEFAULT NULL,
              `ad_spend` DECIMAL(20,4) DEFAULT NULL,
              `ad_orders` DECIMAL(18,2) DEFAULT NULL,
              `ad_sales` DECIMAL(20,4) DEFAULT NULL,
              `promotion_units` DECIMAL(18,2) DEFAULT NULL,
              `avg_price` DECIMAL(18,4) DEFAULT NULL,
              `sku_count` INT NOT NULL DEFAULT 0,
              `source_row_count` BIGINT NOT NULL DEFAULT 0,
              `source_table` VARCHAR(200) NOT NULL,
              `build_version` VARCHAR(100) NOT NULL,
              `materialized_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`dt`,`store_name`,`spu`),
              INDEX `idx_research_spu_date` (`store_name`,`spu`,`dt`),
              INDEX `idx_research_date_store` (`dt`,`store_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='研究层：四店SPU日表现历史；不含伪造历史库存'
        """)


def agg_expr(col: Optional[str], alias: str, kind: str = "sum") -> str:
    if not col:
        return f"NULL AS `{alias}`"
    if kind == "avg_positive":
        return f"AVG(NULLIF(p.`{col}`,0)) AS `{alias}`"
    return f"SUM(COALESCE(p.`{col}`,0)) AS `{alias}`"


def build_month(profile: Dict[str, Any], start: date, end: date, dry_run: bool) -> Dict[str, Any]:
    table = profile["table"]
    dcol = profile["date"]
    store = profile["store"]
    sku = profile["sku"]
    sales = profile["sales"]
    sessions = profile["sessions"]
    delete_col = profile.get("delete_flag")
    opt = profile["optional"]

    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""

    select_sql = f"""
        SELECT
          p.`{dcol}` AS dt,
          p.`{store}` AS store_name,
          p.`spu` AS spu,
          SUM(COALESCE(p.`{sales}`,0)) AS sales_units,
          SUM(COALESCE(p.`{sessions}`,0)) AS sessions,
          {agg_expr(opt.get('clicks'), 'clicks')},
          {agg_expr(opt.get('impressions'), 'impressions')},
          {agg_expr(opt.get('spend'), 'ad_spend')},
          {agg_expr(opt.get('ad_orders'), 'ad_orders')},
          {agg_expr(opt.get('ad_sales'), 'ad_sales')},
          {agg_expr(opt.get('promotion_volume'), 'promotion_units')},
          {agg_expr(opt.get('price'), 'avg_price', 'avg_positive')},
          COUNT(DISTINCT p.`{sku}`) AS sku_count,
          COUNT(*) AS source_row_count
        FROM {table} p
        WHERE p.`{dcol}` BETWEEN %s AND %s
          AND p.`{store}` IN (%s,%s,%s,%s)
          AND p.`spu` IS NOT NULL AND TRIM(p.`spu`)<>''
          {delete_filter}
        GROUP BY p.`{dcol}`, p.`{store}`, p.`spu`
    """
    params: List[Any] = [start, end, *TARGET_SHOPS]

    if dry_run:
        # Only estimate the grouped result on a small bounded sample, never write.
        sample_end = min(end, start + timedelta(days=2))
        sample_params: List[Any] = [start, sample_end, *TARGET_SHOPS]
        with db_cursor() as c:
            c.execute(f"SELECT COUNT(*) AS n FROM ({select_sql}) x", sample_params)
            n = int((c.fetchone() or {}).get("n", 0) or 0)
        return {
            "range": [str(start), str(end)],
            "sample_range": [str(start), str(sample_end)],
            "sample_grouped_rows": n,
            "written": 0,
        }

    insert_sql = f"""
        INSERT INTO `{DEST_TABLE}`
        (`dt`,`store_name`,`spu`,`sales_units`,`sessions`,`clicks`,`impressions`,
         `ad_spend`,`ad_orders`,`ad_sales`,`promotion_units`,`avg_price`,
         `sku_count`,`source_row_count`,`source_table`,`build_version`)
        SELECT
          x.dt, x.store_name, x.spu, x.sales_units, x.sessions, x.clicks, x.impressions,
          x.ad_spend, x.ad_orders, x.ad_sales, x.promotion_units, x.avg_price,
          x.sku_count, x.source_row_count, %s, %s
        FROM ({select_sql}) x
        ON DUPLICATE KEY UPDATE
          `sales_units`=VALUES(`sales_units`),
          `sessions`=VALUES(`sessions`),
          `clicks`=VALUES(`clicks`),
          `impressions`=VALUES(`impressions`),
          `ad_spend`=VALUES(`ad_spend`),
          `ad_orders`=VALUES(`ad_orders`),
          `ad_sales`=VALUES(`ad_sales`),
          `promotion_units`=VALUES(`promotion_units`),
          `avg_price`=VALUES(`avg_price`),
          `sku_count`=VALUES(`sku_count`),
          `source_row_count`=VALUES(`source_row_count`),
          `source_table`=VALUES(`source_table`),
          `build_version`=VALUES(`build_version`),
          `materialized_at`=CURRENT_TIMESTAMP
    """
    with db_cursor() as c:
        c.execute(insert_sql, [table, BUILD_VERSION, *params])
        affected = int(c.rowcount or 0)

    with db_cursor() as c:
        c.execute(
            f"""
            SELECT COUNT(*) AS n,
                   COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS shop_spu_n,
                   SUM(sales_units) AS sales_units,
                   SUM(sessions) AS sessions
            FROM `{DEST_TABLE}`
            WHERE dt BETWEEN %s AND %s
            """,
            (start, end),
        )
        row = c.fetchone() or {}
    return {
        "range": [str(start), str(end)],
        "affected": affected,
        "materialized_rows": int(row.get("n", 0) or 0),
        "shop_spu_n": int(row.get("shop_spu_n", 0) or 0),
        "sales_units": float(row.get("sales_units", 0) or 0),
        "sessions": float(row.get("sessions", 0) or 0),
    }


def recent_spu_coverage(profile: Dict[str, Any], as_of: date) -> Dict[str, Any]:
    """Small 7-day source check: how many source rows carry native SPU."""
    table = profile["table"]
    dcol = profile["date"]
    store = profile["store"]
    delete_col = profile.get("delete_flag")
    delete_filter = f"AND COALESCE(`{delete_col}`,0)=0" if delete_col else ""
    start = as_of - timedelta(days=6)
    with db_cursor() as c:
        c.execute(
            f"""
            SELECT
              COUNT(*) AS total_rows,
              SUM(CASE WHEN spu IS NOT NULL AND TRIM(spu)<>'' THEN 1 ELSE 0 END) AS spu_rows,
              COUNT(DISTINCT CASE WHEN spu IS NOT NULL AND TRIM(spu)<>'' THEN spu END) AS spu_n
            FROM {table}
            WHERE `{dcol}` BETWEEN %s AND %s
              AND `{store}` IN (%s,%s,%s,%s)
              {delete_filter}
            """,
            (start, as_of, *TARGET_SHOPS),
        )
        row = c.fetchone() or {}
    total = int(row.get("total_rows", 0) or 0)
    spu_rows = int(row.get("spu_rows", 0) or 0)
    return {
        "range": [str(start), str(as_of)],
        "total_rows": total,
        "rows_with_native_spu": spu_rows,
        "native_spu_rate": round(spu_rows / total, 6) if total else None,
        "distinct_spu": int(row.get("spu_n", 0) or 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--max-months",
        type=int,
        default=None,
        help="Safety/debug: materialize at most N monthly chunks from start.",
    )
    args = ap.parse_args()

    today = date.today()
    profile = source_profile(today)
    source_first, source_last = probe_source_bounds(profile)
    requested_start = parse_date(args.start)
    requested_end = parse_date(args.end) if args.end else profile["as_of_date"]
    start = max(requested_start, source_first)
    end = min(requested_end, profile["as_of_date"], source_last)
    if start > end:
        raise RuntimeError(
            f"无可构建日期: requested={requested_start}..{requested_end}, "
            f"source={source_first}..{source_last}"
        )

    summary = {
        "source_table": profile["table"],
        "source_first": str(source_first),
        "source_last": str(source_last),
        "as_of_date": str(profile["as_of_date"]),
        "build_range": [str(start), str(end)],
        "target_shops": list(TARGET_SHOPS),
        "native_spu_required": True,
        "mapped_fields": {
            "date": profile["date"],
            "store": profile["store"],
            "spu": profile["spu"],
            "sku": profile["sku"],
            "sales": profile["sales"],
            "sessions": profile["sessions"],
            **profile["optional"],
        },
        "recent_native_spu_coverage": recent_spu_coverage(profile, profile["as_of_date"]),
        "dry_run": bool(args.dry_run),
    }
    print("SPU_DAILY_HISTORY_PREFLIGHT=" + json.dumps(summary, ensure_ascii=False, default=str))

    if not args.dry_run:
        ensure_table()

    results = []
    for idx, (m_start, m_end) in enumerate(iter_month_ranges(start, end), start=1):
        if args.max_months is not None and idx > args.max_months:
            break
        logger.info(f"历史SPU日聚合: {m_start} ~ {m_end}; dry_run={args.dry_run}")
        result = build_month(profile, m_start, m_end, args.dry_run)
        results.append(result)
        print("CHUNK=" + json.dumps(result, ensure_ascii=False, default=str))

    if not args.dry_run:
        with db_cursor() as c:
            c.execute(
                f"""
                SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt, COUNT(*) AS rows_n,
                       COUNT(DISTINCT CONCAT(store_name,'|',spu)) AS shop_spu_n,
                       COUNT(DISTINCT dt) AS days_n
                FROM `{DEST_TABLE}`
                WHERE dt BETWEEN %s AND %s
                """,
                (start, end),
            )
            final = c.fetchone() or {}
        print(
            "SPU_DAILY_HISTORY_SUMMARY="
            + json.dumps(
                {
                    "min_dt": str(final.get("min_dt") or ""),
                    "max_dt": str(final.get("max_dt") or ""),
                    "rows_n": int(final.get("rows_n", 0) or 0),
                    "shop_spu_n": int(final.get("shop_spu_n", 0) or 0),
                    "days_n": int(final.get("days_n", 0) or 0),
                    "build_version": BUILD_VERSION,
                },
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
