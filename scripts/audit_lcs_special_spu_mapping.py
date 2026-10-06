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

START = date(2025, 3, 1)
END = date(2025, 9, 30)


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

    # We only need an SPU exclusion mapping. Avoid expensive SUM/MIN/MAX/GROUP BY
    # over the large ODS table; fetch distinct raw MSKU->SPU pairs month by month.
    mapping = {}
    month_shop = defaultdict(lambda: {"mskus": set(), "spus": set()})
    all_mskus = set()
    all_spus = set()

    for chunk_start, chunk_end in month_windows(START, END):
        print(f"SCAN_MONTH={chunk_start}~{chunk_end}", flush=True)
        with db_cursor() as c:
            c.execute(
                f"""
                SELECT DISTINCT
                  p.`store_name` AS shop,
                  p.`msku` AS msku,
                  p.`spu` AS spu
                FROM {table} p
                WHERE p.`dt` BETWEEN %s AND %s
                  AND p.`store_name` IN (%s,%s,%s,%s)
                  AND p.`msku` LIKE 'LCS-%%'
                  AND p.`spu` IS NOT NULL
                  AND p.`spu` <> ''
                  {delete_filter}
                """,
                (chunk_start, chunk_end, *TARGET_SHOPS),
            )
            rows = list(c.fetchall())

        ym = chunk_start.strftime("%Y-%m")
        month_mskus = set()
        month_spus = set()
        for r in rows:
            shop = str(r.get("shop") or "").strip()
            msku = str(r.get("msku") or "").strip()
            spu = str(r.get("spu") or "").strip()
            if not msku or not spu:
                continue
            key = (shop, msku, spu)
            mapping[key] = {"shop": shop, "msku": msku, "spu": spu}
            all_mskus.add(msku)
            all_spus.add(spu)
            month_mskus.add(msku)
            month_spus.add(spu)
            month_shop[(ym, shop)]["mskus"].add(msku)
            month_shop[(ym, shop)]["spus"].add(spu)

        print("MONTH_RESULT=" + json.dumps({
            "month": ym,
            "mapping_rows": len(rows),
            "distinct_msku": len(month_mskus),
            "distinct_spu": len(month_spus),
        }, ensure_ascii=False), flush=True)

    summary = {
        "distinct_lcs_msku": len(all_mskus),
        "distinct_mapped_spu": len(all_spus),
        "distinct_shop_msku_spu": len(mapping),
    }
    print("SUMMARY=" + json.dumps(summary, ensure_ascii=False))

    print("\n=== MONTH_SHOP ===")
    for (ym, shop) in sorted(month_shop):
        r = month_shop[(ym, shop)]
        print(json.dumps({
            "month": ym,
            "shop": shop,
            "lcs_msku": len(r["mskus"]),
            "mapped_spu": len(r["spus"]),
        }, ensure_ascii=False))

    print("\n=== TOP_MAPPING ===")
    for r in sorted(mapping.values(), key=lambda x: (x["shop"], x["msku"], x["spu"]))[:200]:
        print(json.dumps(r, ensure_ascii=False))

    print("\nAUDIT_DECISION=" + json.dumps({
        "lcs_found": len(all_mskus) > 0,
        "mapped_spu_found": len(all_spus) > 0,
        "mapped_spu_count": len(all_spus),
        "next": (
            "use raw performance msku->native spu as whole-SPU exclusion source"
            if len(all_spus) > 0
            else "do not change cohort yet; inspect actual LCS field/value format"
        ),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
