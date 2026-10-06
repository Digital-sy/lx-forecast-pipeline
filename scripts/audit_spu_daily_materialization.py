#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only reconciliation of source native-SPU signal vs research SPU-day table.

Use after materializing one or more date ranges. It proves that the research aggregation
preserves all attributable sales/sessions exactly at store level.

No table is created or modified.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research.build_spu_daily_history import DEST_TABLE


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()
    start, end = parse_date(args.start), parse_date(args.end)

    if not base.table_exists(DEST_TABLE):
        raise RuntimeError(f"{DEST_TABLE} 不存在")

    source = v2.resolve_performance_source(date.today())
    table = source["table"]
    cols = set(base.get_columns(table))
    if "spu" not in cols:
        raise RuntimeError("源表无原生SPU，当前reconciliation脚本只适用于NATIVE_SPU模式")

    dcol = source["date"]
    store = source["store"]
    sales = source["sales"]
    sessions = source["sessions"]
    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""

    with db_cursor() as c:
        c.execute(
            f"""
            SELECT p.`{store}` AS store_name,
                   COUNT(*) AS source_rows,
                   COUNT(DISTINCT CONCAT(p.`{dcol}`,'|',p.`spu`)) AS expected_group_rows,
                   SUM(COALESCE(p.`{sales}`,0)) AS sales_units,
                   SUM(COALESCE(p.`{sessions}`,0)) AS sessions
            FROM {table} p
            WHERE p.`{dcol}` BETWEEN %s AND %s
              AND p.`{store}` IN (%s,%s,%s,%s)
              AND p.`spu` IS NOT NULL AND TRIM(p.`spu`)<>''
              {delete_filter}
            GROUP BY p.`{store}`
            ORDER BY p.`{store}`
            """,
            (start, end, *TARGET_SHOPS),
        )
        src = {str(r["store_name"]): r for r in c.fetchall()}

        c.execute(
            f"""
            SELECT store_name,
                   COUNT(*) AS research_rows,
                   SUM(sales_units) AS sales_units,
                   SUM(sessions) AS sessions
            FROM `{DEST_TABLE}`
            WHERE dt BETWEEN %s AND %s
              AND store_name IN (%s,%s,%s,%s)
            GROUP BY store_name
            ORDER BY store_name
            """,
            (start, end, *TARGET_SHOPS),
        )
        dst = {str(r["store_name"]): r for r in c.fetchall()}

    all_ok = True
    print("=" * 96)
    print(f"历史SPU日聚合对账（只读） {start} ~ {end}")
    print("=" * 96)
    for shop in TARGET_SHOPS:
        s = src.get(shop, {})
        d = dst.get(shop, {})
        source_group_rows = int(s.get("expected_group_rows", 0) or 0)
        research_rows = int(d.get("research_rows", 0) or 0)
        source_sales = float(s.get("sales_units", 0) or 0)
        research_sales = float(d.get("sales_units", 0) or 0)
        source_sessions = float(s.get("sessions", 0) or 0)
        research_sessions = float(d.get("sessions", 0) or 0)

        row_diff = research_rows - source_group_rows
        sales_diff = research_sales - source_sales
        sessions_diff = research_sessions - source_sessions
        ok = row_diff == 0 and abs(sales_diff) < 1e-6 and abs(sessions_diff) < 1e-6
        all_ok = all_ok and ok
        print(json.dumps({
            "shop": shop,
            "source_rows": int(s.get("source_rows", 0) or 0),
            "expected_group_rows": source_group_rows,
            "research_rows": research_rows,
            "row_diff": row_diff,
            "source_sales": source_sales,
            "research_sales": research_sales,
            "sales_diff": sales_diff,
            "source_sessions": source_sessions,
            "research_sessions": research_sessions,
            "sessions_diff": sessions_diff,
            "exact": ok,
        }, ensure_ascii=False))

    print("RECONCILIATION_SUMMARY=" + json.dumps({
        "start": str(start),
        "end": str(end),
        "all_exact": all_ok,
        "source_table": table,
        "research_table": DEST_TABLE,
    }, ensure_ascii=False))

    if not all_ok:
        raise RuntimeError("研究层与源表native-SPU信号未完全对账，请停止后续历史构建")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
