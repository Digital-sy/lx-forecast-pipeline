#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of LCS-* special low-price MSKU -> SPU mappings.

Business context
----------------
LCS-* identifies special low-price MSKUs. These are not normal-selling launches and the
corresponding SPUs must be excluded as a whole from NEW_VISIBLE research/modeling.

This audit intentionally reads the raw daily product-performance source because it
contains historical `msku` and native `spu` on the same row. No table is modified.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS

START = date(2025, 1, 1)
END = date(2025, 12, 31)


def main() -> int:
    source = v2.resolve_performance_source(date.today())
    table = source["table"]
    cols = set(base.get_columns(table))

    required = {"dt", "store_name", "msku", "spu"}
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(
            f"{table} 缺少LCS审计必需字段: {missing}; available_columns={sorted(cols)}"
        )

    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""

    print("=" * 100)
    print("LCS特殊低价MSKU -> SPU映射审计（只读）")
    print("=" * 100)
    print("SOURCE=" + json.dumps({
        "table": table,
        "range": [str(START), str(END)],
        "shops": list(TARGET_SHOPS),
        "msku_field": "msku",
        "spu_field": "spu",
    }, ensure_ascii=False))

    with db_cursor() as c:
        c.execute(
            f"""
            SELECT
              COUNT(*) AS rows_n,
              COUNT(DISTINCT UPPER(TRIM(p.`msku`))) AS msku_n,
              COUNT(DISTINCT UPPER(TRIM(p.`spu`))) AS spu_n,
              SUM(COALESCE(p.`volume`,0)) AS sales_units,
              SUM(COALESCE(p.`sessions_total`,0)) AS sessions
            FROM {table} p
            WHERE p.`dt` BETWEEN %s AND %s
              AND p.`store_name` IN (%s,%s,%s,%s)
              AND UPPER(TRIM(COALESCE(p.`msku`,''))) LIKE 'LCS-%%'
              {delete_filter}
            """,
            (START, END, *TARGET_SHOPS),
        )
        summary = c.fetchone() or {}

        c.execute(
            f"""
            SELECT
              DATE_FORMAT(p.`dt`,'%%Y-%%m') AS ym,
              p.`store_name` AS shop,
              COUNT(*) AS rows_n,
              COUNT(DISTINCT UPPER(TRIM(p.`msku`))) AS msku_n,
              COUNT(DISTINCT UPPER(TRIM(p.`spu`))) AS spu_n,
              SUM(COALESCE(p.`volume`,0)) AS sales_units,
              SUM(COALESCE(p.`sessions_total`,0)) AS sessions
            FROM {table} p
            WHERE p.`dt` BETWEEN %s AND %s
              AND p.`store_name` IN (%s,%s,%s,%s)
              AND UPPER(TRIM(COALESCE(p.`msku`,''))) LIKE 'LCS-%%'
              {delete_filter}
            GROUP BY DATE_FORMAT(p.`dt`,'%%Y-%%m'), p.`store_name`
            ORDER BY ym, shop
            """,
            (START, END, *TARGET_SHOPS),
        )
        by_month_shop = list(c.fetchall())

        c.execute(
            f"""
            SELECT
              p.`store_name` AS shop,
              UPPER(TRIM(p.`msku`)) AS msku,
              UPPER(TRIM(p.`spu`)) AS spu,
              MIN(p.`dt`) AS first_dt,
              MAX(p.`dt`) AS last_dt,
              SUM(COALESCE(p.`volume`,0)) AS sales_units,
              SUM(COALESCE(p.`sessions_total`,0)) AS sessions,
              COUNT(*) AS rows_n
            FROM {table} p
            WHERE p.`dt` BETWEEN %s AND %s
              AND p.`store_name` IN (%s,%s,%s,%s)
              AND UPPER(TRIM(COALESCE(p.`msku`,''))) LIKE 'LCS-%%'
              {delete_filter}
            GROUP BY p.`store_name`, UPPER(TRIM(p.`msku`)), UPPER(TRIM(p.`spu`))
            ORDER BY sales_units DESC, sessions DESC
            LIMIT 200
            """,
            (START, END, *TARGET_SHOPS),
        )
        mappings = list(c.fetchall())

    print("SUMMARY=" + json.dumps({
        "rows_n": int(summary.get("rows_n") or 0),
        "distinct_lcs_msku": int(summary.get("msku_n") or 0),
        "distinct_mapped_spu": int(summary.get("spu_n") or 0),
        "sales_units": float(summary.get("sales_units") or 0),
        "sessions": float(summary.get("sessions") or 0),
    }, ensure_ascii=False))

    print("\n=== MONTH_SHOP ===")
    for r in by_month_shop:
        print(json.dumps({
            "month": str(r.get("ym")),
            "shop": str(r.get("shop")),
            "rows": int(r.get("rows_n") or 0),
            "lcs_msku": int(r.get("msku_n") or 0),
            "mapped_spu": int(r.get("spu_n") or 0),
            "sales_units": float(r.get("sales_units") or 0),
            "sessions": float(r.get("sessions") or 0),
        }, ensure_ascii=False))

    print("\n=== TOP_MAPPING ===")
    blank_spu_rows = 0
    distinct_spus = set()
    for r in mappings:
        spu = str(r.get("spu") or "").strip()
        if not spu:
            blank_spu_rows += 1
        else:
            distinct_spus.add(spu)
        print(json.dumps({
            "shop": str(r.get("shop")),
            "msku": str(r.get("msku") or ""),
            "spu": spu,
            "first_dt": str(r.get("first_dt") or ""),
            "last_dt": str(r.get("last_dt") or ""),
            "sales_units": float(r.get("sales_units") or 0),
            "sessions": float(r.get("sessions") or 0),
            "rows": int(r.get("rows_n") or 0),
        }, ensure_ascii=False))

    print("\nAUDIT_DECISION=" + json.dumps({
        "lcs_found": int(summary.get("msku_n") or 0) > 0,
        "mapped_spu_found": int(summary.get("spu_n") or 0) > 0,
        "top_mapping_blank_spu_rows": blank_spu_rows,
        "top_mapping_distinct_spu": len(distinct_spus),
        "next": (
            "use raw performance msku->native spu as whole-SPU exclusion source"
            if int(summary.get("spu_n") or 0) > 0
            else "do not change cohort yet; inspect actual LCS field/value format"
        ),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
