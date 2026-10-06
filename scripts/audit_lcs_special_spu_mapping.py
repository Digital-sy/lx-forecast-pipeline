#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of LCS-* special low-price MSKU -> SPU mappings.

Scans the raw product-performance source one month at a time to avoid long-running
full-year queries on the very large ODS table. No table is modified.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS

START = date(2025, 1, 1)
END = date(2025, 12, 31)


def month_windows(start: date, end: date):
    cur = start.replace(day=1)
    while cur <= end:
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        chunk_end = min(end, date.fromordinal(nxt.toordinal() - 1))
        yield cur, chunk_end
        cur = nxt


def main() -> int:
    source = v2.resolve_performance_source(date.today())
    table = source["table"]
    cols = set(base.get_columns(table))

    required = {"dt", "store_name", "msku", "spu", "volume", "sessions_total"}
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(
            f"{table} 缺少LCS审计必需字段: {missing}; available_columns={sorted(cols)}"
        )

    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""

    print("=" * 100)
    print("LCS特殊低价MSKU -> SPU映射审计（只读；按月分块）")
    print("=" * 100)
    print("SOURCE=" + json.dumps({
        "table": table,
        "range": [str(START), str(END)],
        "shops": list(TARGET_SHOPS),
        "msku_field": "msku",
        "spu_field": "spu",
        "scan_mode": "MONTHLY_CHUNKS",
    }, ensure_ascii=False))

    # Aggregate in Python so MySQL never has to hold a full-year GROUP BY.
    mapping: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    month_shop = defaultdict(lambda: {
        "rows": 0, "mskus": set(), "spus": set(), "sales_units": 0.0, "sessions": 0.0
    })
    all_mskus = set()
    all_spus = set()
    total_rows = 0
    total_sales = 0.0
    total_sessions = 0.0

    for chunk_start, chunk_end in month_windows(START, END):
        print(f"SCAN_MONTH={chunk_start}~{chunk_end}")
        with db_cursor() as c:
            c.execute(
                f"""
                SELECT
                  p.`store_name` AS shop,
                  UPPER(TRIM(p.`msku`)) AS msku,
                  UPPER(TRIM(COALESCE(p.`spu`,''))) AS spu,
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
                GROUP BY
                  p.`store_name`,
                  UPPER(TRIM(p.`msku`)),
                  UPPER(TRIM(COALESCE(p.`spu`,''))
                """,
                (chunk_start, chunk_end, *TARGET_SHOPS),
            )
            rows = list(c.fetchall())

        ym = chunk_start.strftime("%Y-%m")
        chunk_summary = {
            "month": ym,
            "mapping_rows": len(rows),
            "distinct_msku": len({str(r.get("msku") or "") for r in rows}),
            "distinct_spu": len({str(r.get("spu") or "") for r in rows if str(r.get("spu") or "").strip()}),
            "sales_units": float(sum(float(r.get("sales_units") or 0) for r in rows)),
            "sessions": float(sum(float(r.get("sessions") or 0) for r in rows)),
        }
        print("MONTH_RESULT=" + json.dumps(chunk_summary, ensure_ascii=False))

        for r in rows:
            shop = str(r.get("shop") or "").strip()
            msku = str(r.get("msku") or "").strip()
            spu = str(r.get("spu") or "").strip()
            rn = int(r.get("rows_n") or 0)
            sales = float(r.get("sales_units") or 0)
            sessions = float(r.get("sessions") or 0)
            first_dt = r.get("first_dt")
            last_dt = r.get("last_dt")

            total_rows += rn
            total_sales += sales
            total_sessions += sessions
            if msku:
                all_mskus.add(msku)
            if spu:
                all_spus.add(spu)

            ms = month_shop[(ym, shop)]
            ms["rows"] += rn
            if msku:
                ms["mskus"].add(msku)
            if spu:
                ms["spus"].add(spu)
            ms["sales_units"] += sales
            ms["sessions"] += sessions

            key = (shop, msku, spu)
            if key not in mapping:
                mapping[key] = {
                    "shop": shop,
                    "msku": msku,
                    "spu": spu,
                    "first_dt": first_dt,
                    "last_dt": last_dt,
                    "sales_units": sales,
                    "sessions": sessions,
                    "rows": rn,
                }
            else:
                x = mapping[key]
                if first_dt is not None and (x["first_dt"] is None or first_dt < x["first_dt"]):
                    x["first_dt"] = first_dt
                if last_dt is not None and (x["last_dt"] is None or last_dt > x["last_dt"]):
                    x["last_dt"] = last_dt
                x["sales_units"] += sales
                x["sessions"] += sessions
                x["rows"] += rn

    summary = {
        "rows_n": total_rows,
        "distinct_lcs_msku": len(all_mskus),
        "distinct_mapped_spu": len(all_spus),
        "sales_units": total_sales,
        "sessions": total_sessions,
    }
    print("SUMMARY=" + json.dumps(summary, ensure_ascii=False))

    print("\n=== MONTH_SHOP ===")
    for (ym, shop) in sorted(month_shop):
        r = month_shop[(ym, shop)]
        print(json.dumps({
            "month": ym,
            "shop": shop,
            "rows": r["rows"],
            "lcs_msku": len(r["mskus"]),
            "mapped_spu": len(r["spus"]),
            "sales_units": r["sales_units"],
            "sessions": r["sessions"],
        }, ensure_ascii=False))

    print("\n=== TOP_MAPPING ===")
    top = sorted(
        mapping.values(),
        key=lambda r: (float(r["sales_units"]), float(r["sessions"])),
        reverse=True,
    )[:200]
    blank_spu_rows = 0
    top_distinct_spus = set()
    for r in top:
        spu = str(r.get("spu") or "").strip()
        if not spu:
            blank_spu_rows += 1
        else:
            top_distinct_spus.add(spu)
        print(json.dumps({
            "shop": r["shop"],
            "msku": r["msku"],
            "spu": spu,
            "first_dt": str(r.get("first_dt") or ""),
            "last_dt": str(r.get("last_dt") or ""),
            "sales_units": float(r.get("sales_units") or 0),
            "sessions": float(r.get("sessions") or 0),
            "rows": int(r.get("rows") or 0),
        }, ensure_ascii=False))

    print("\nAUDIT_DECISION=" + json.dumps({
        "lcs_found": len(all_mskus) > 0,
        "mapped_spu_found": len(all_spus) > 0,
        "top_mapping_blank_spu_rows": blank_spu_rows,
        "top_mapping_distinct_spu": len(top_distinct_spus),
        "next": (
            "use raw performance msku->native spu as whole-SPU exclusion source"
            if len(all_spus) > 0
            else "do not change cohort yet; inspect actual LCS field/value format"
        ),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
