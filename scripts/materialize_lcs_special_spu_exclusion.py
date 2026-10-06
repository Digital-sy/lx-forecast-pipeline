#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Materialize the business exclusion list for LCS-* special low-price SPUs.

Business rule: if an SPU is mapped from any LCS-* MSKU in the audited 2025 spring/summer
special-sale window, exclude the whole SPU from normal NEW_VISIBLE/Breakout modeling.

This writes only a small control table; it does not alter raw ODS, production forecast,
or procurement tables.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS

TABLE = "forecast_special_spu_exclusion"
LCS_CODE = "LCS_SPECIAL_LOW_PRICE"
XH_CODE = "XH_PREFIX_EXCLUSION"
XH_PREFIX = "XH"
START = date(2025, 3, 1)
END = date(2025, 9, 30)


def month_windows(start: date, end: date):
    cur = start.replace(day=1)
    while cur <= end:
        nxt = date(cur.year + (1 if cur.month == 12 else 0), 1 if cur.month == 12 else cur.month + 1, 1)
        yield cur, min(end, nxt - timedelta(days=1))
        cur = nxt


def main() -> int:
    source = v2.resolve_performance_source(date.today())
    table = source["table"]
    cols = set(base.get_columns(table))
    required = {"dt", "store_name", "msku", "spu"}
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(f"{table} 缺少LCS映射字段: {missing}")

    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""

    spus = set()
    by_month = []
    for s, e in month_windows(START, END):
        with db_cursor() as c:
            c.execute(
                f"""
                SELECT DISTINCT UPPER(TRIM(p.`spu`)) AS spu
                FROM {table} p
                WHERE p.`dt` BETWEEN %s AND %s
                  AND p.`store_name` IN (%s,%s,%s,%s)
                  AND p.`msku` LIKE 'LCS-%%'
                  AND p.`spu` IS NOT NULL
                  AND p.`spu` <> ''
                  {delete_filter}
                """,
                (s, e, *TARGET_SHOPS),
            )
            rows = list(c.fetchall())
        month_spus = {str(r.get("spu") or "").strip().upper() for r in rows}
        month_spus.discard("")
        spus.update(month_spus)
        by_month.append({"month": s.strftime("%Y-%m"), "spu_n": len(month_spus)})
        print("MONTH=" + json.dumps(by_month[-1], ensure_ascii=False), flush=True)

    if not spus:
        raise RuntimeError("LCS特殊低价SPU映射为空；为避免误清单，拒绝写入控制表")

    # Direct business rule: every SPU whose code starts with XH is excluded.
    # Use the already-materialized research SPU-day base instead of rescanning ODS.
    xh_spus = set()
    daily_table = "forecast_research_spu_daily_history"
    if base.table_exists(daily_table):
        with db_cursor() as c:
            c.execute(
                f"""
                SELECT DISTINCT UPPER(TRIM(spu)) AS spu
                FROM `{daily_table}`
                WHERE UPPER(TRIM(spu)) LIKE 'XH%%'
                """
            )
            xh_rows = list(c.fetchall())
        xh_spus = {str(r.get("spu") or "").strip().upper() for r in xh_rows}
        xh_spus.discard("")
    else:
        raise RuntimeError(f"{daily_table} 不存在；无法物化XH前缀排除清单")

    with db_cursor() as c:
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{TABLE}` (
              `spu` VARCHAR(128) NOT NULL,
              `exclusion_code` VARCHAR(64) NOT NULL,
              `reason` VARCHAR(255) NOT NULL,
              `source_table` VARCHAR(255) NOT NULL,
              `source_start` DATE NOT NULL,
              `source_end` DATE NOT NULL,
              `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                  ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`spu`, `exclusion_code`),
              KEY `idx_exclusion_code` (`exclusion_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        c.execute(f"DELETE FROM `{TABLE}` WHERE exclusion_code=%s", (LCS_CODE,))
        rows = [
            (
                spu,
                LCS_CODE,
                "LCS-* special low-price MSKU mapped SPU; business-rule whole-SPU exclusion",
                table,
                START,
                END,
            )
            for spu in sorted(spus)
        ]
        c.executemany(
            f"""
            INSERT INTO `{TABLE}`
              (spu, exclusion_code, reason, source_table, source_start, source_end)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )

        c.execute(f"DELETE FROM `{TABLE}` WHERE exclusion_code=%s", (XH_CODE,))
        xh_control_rows = [
            (
                spu,
                XH_CODE,
                "SPU prefix XH; business-rule whole-SPU exclusion",
                daily_table,
                START,
                END,
            )
            for spu in sorted(xh_spus)
        ]
        if xh_control_rows:
            c.executemany(
                f"""
                INSERT INTO `{TABLE}`
                  (spu, exclusion_code, reason, source_table, source_start, source_end)
                VALUES (%s,%s,%s,%s,%s,%s)
                """,
                xh_control_rows,
            )

    print("MATERIALIZED=" + json.dumps({
        "table": TABLE,
        "lcs_exclusion_code": LCS_CODE,
        "lcs_spu_n": len(spus),
        "xh_spu_n": len(xh_spus),
        "union_spu_n": len(spus | xh_spus),
        "source_table": table,
        "source_range": [str(START), str(END)],
        "month_counts": by_month,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
