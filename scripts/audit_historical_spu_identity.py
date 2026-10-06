#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of historical SPU identity coverage.

Before materializing the 2024-2026 research history, quantify whether rows with blank
native SPU carry meaningful sales/traffic and whether the same historical row's SKU
prefix can safely recover their SPU identity.

No table is created or modified.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS


def to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def pct(n: float, d: float):
    return round(n / d, 6) if d else None


def source_profile() -> Dict[str, Any]:
    source = v2.resolve_performance_source(date.today())
    cols = set(base.get_columns(source["table"]))
    if "spu" not in cols:
        raise RuntimeError("当前产品表现源没有原生spu字段，本诊断无需运行")
    for name in ("date", "store", "sku", "sales", "sessions"):
        if not source.get(name):
            raise RuntimeError(f"产品表现源缺少必需映射: {name}")
    return source


def audit_window(source: Dict[str, Any], start: date, end: date) -> List[Dict[str, Any]]:
    table = source["table"]
    dcol = source["date"]
    store = source["store"]
    sku = source["sku"]
    sales = source["sales"]
    sessions = source["sessions"]
    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.{delete_col},0)=0" if delete_col else ""

    prefix = f"TRIM(SUBSTRING_INDEX(COALESCE(p.{sku},''),'-',1))"
    native = "TRIM(COALESCE(p.spu,''))"

    sql = f"""
        SELECT
          p.{store} AS store_name,
          COUNT(*) AS total_rows,
          SUM({native}<>'') AS native_spu_rows,
          SUM({native}='') AS missing_native_rows,
          SUM({native}='' AND {prefix}<>'') AS missing_native_prefix_recoverable_rows,
          SUM({native}='' AND {prefix}='') AS missing_native_unrecoverable_rows,

          SUM(COALESCE(p.{sales},0)) AS total_sales,
          SUM(CASE WHEN {native}='' THEN COALESCE(p.{sales},0) ELSE 0 END) AS missing_native_sales,
          SUM(CASE WHEN {native}='' AND {prefix}<>'' THEN COALESCE(p.{sales},0) ELSE 0 END)
            AS recoverable_missing_sales,

          SUM(COALESCE(p.{sessions},0)) AS total_sessions,
          SUM(CASE WHEN {native}='' THEN COALESCE(p.{sessions},0) ELSE 0 END) AS missing_native_sessions,
          SUM(CASE WHEN {native}='' AND {prefix}<>'' THEN COALESCE(p.{sessions},0) ELSE 0 END)
            AS recoverable_missing_sessions,

          SUM({native}<>'' AND {prefix}<>'') AS both_identity_rows,
          SUM({native}<>'' AND {prefix}<>'' AND UPPER({native})=UPPER({prefix}))
            AS native_prefix_exact_match_rows,
          SUM({native}<>'' AND {prefix}<>'' AND UPPER({native})<>UPPER({prefix}))
            AS native_prefix_mismatch_rows,

          COUNT(DISTINCT CASE WHEN {native}='' AND {prefix}<>'' THEN {prefix} END)
            AS recoverable_prefix_spu_n
        FROM {table} p
        WHERE p.{dcol} BETWEEN %s AND %s
          AND p.{store} IN (%s,%s,%s,%s)
          {delete_filter}
        GROUP BY p.{store}
        ORDER BY p.{store}
    """
    with db_cursor() as c:
        c.execute(sql, (start, end, *TARGET_SHOPS))
        rows = list(c.fetchall())

    out = []
    for r in rows:
        total_rows = int(r.get("total_rows") or 0)
        native_rows = int(r.get("native_spu_rows") or 0)
        missing_rows = int(r.get("missing_native_rows") or 0)
        recover_rows = int(r.get("missing_native_prefix_recoverable_rows") or 0)
        both = int(r.get("both_identity_rows") or 0)
        exact = int(r.get("native_prefix_exact_match_rows") or 0)

        total_sales = float(r.get("total_sales") or 0)
        missing_sales = float(r.get("missing_native_sales") or 0)
        recover_sales = float(r.get("recoverable_missing_sales") or 0)

        total_sessions = float(r.get("total_sessions") or 0)
        missing_sessions = float(r.get("missing_native_sessions") or 0)
        recover_sessions = float(r.get("recoverable_missing_sessions") or 0)

        out.append({
            "window": [str(start), str(end)],
            "shop": str(r.get("store_name") or ""),
            "total_rows": total_rows,
            "native_spu_rate": pct(native_rows, total_rows),
            "missing_native_rows": missing_rows,
            "missing_native_row_rate": pct(missing_rows, total_rows),
            "prefix_recovery_rate_on_missing_rows": pct(recover_rows, missing_rows),
            "missing_native_sales": round(missing_sales, 2),
            "missing_native_sales_share": pct(missing_sales, total_sales),
            "recoverable_missing_sales_share": pct(recover_sales, missing_sales),
            "missing_native_sessions": round(missing_sessions, 2),
            "missing_native_sessions_share": pct(missing_sessions, total_sessions),
            "recoverable_missing_sessions_share": pct(recover_sessions, missing_sessions),
            "native_prefix_agreement_rate": pct(exact, both),
            "native_prefix_mismatch_rows": int(r.get("native_prefix_mismatch_rows") or 0),
            "recoverable_prefix_spu_n": int(r.get("recoverable_prefix_spu_n") or 0),
            "unrecoverable_missing_rows": int(r.get("missing_native_unrecoverable_rows") or 0),
        })
    return out


def top_missing(source: Dict[str, Any], start: date, end: date) -> List[Dict[str, Any]]:
    table = source["table"]
    dcol = source["date"]
    store = source["store"]
    sku = source["sku"]
    sales = source["sales"]
    sessions = source["sessions"]
    delete_col = source.get("delete_flag")
    delete_filter = f"AND COALESCE(p.{delete_col},0)=0" if delete_col else ""
    prefix = f"TRIM(SUBSTRING_INDEX(COALESCE(p.{sku},''),'-',1))"

    sql = f"""
        SELECT
          p.{store} AS store_name,
          {prefix} AS sku_prefix_spu,
          COUNT(*) AS rows_n,
          SUM(COALESCE(p.{sales},0)) AS sales_units,
          SUM(COALESCE(p.{sessions},0)) AS sessions
        FROM {table} p
        WHERE p.{dcol} BETWEEN %s AND %s
          AND p.{store} IN (%s,%s,%s,%s)
          AND (p.spu IS NULL OR TRIM(p.spu)='')
          {delete_filter}
        GROUP BY p.{store}, {prefix}
        HAVING sku_prefix_spu <> ''
        ORDER BY sales_units DESC, sessions DESC
        LIMIT 40
    """
    with db_cursor() as c:
        c.execute(sql, (start, end, *TARGET_SHOPS))
        return list(c.fetchall())


def main() -> int:
    source = source_profile()
    as_of = to_date(source["as_of_date"])
    windows: List[Tuple[date, date]] = [
        (date(2024, 1, 1), date(2024, 1, 7)),
        (date(2025, 1, 1), date(2025, 1, 7)),
        (date(2026, 1, 1), date(2026, 1, 7)),
        (as_of - timedelta(days=6), as_of),
    ]

    print("=" * 100)
    print("历史SPU身份覆盖诊断（只读）")
    print("=" * 100)
    print("SOURCE=" + json.dumps({
        "table": source["table"],
        "as_of": str(as_of),
        "date": source["date"],
        "store": source["store"],
        "sku": source["sku"],
        "sales": source["sales"],
        "sessions": source["sessions"],
    }, ensure_ascii=False))

    all_rows = []
    for start, end in windows:
        if end > as_of:
            continue
        print(f"\n=== WINDOW {start} ~ {end} ===")
        rows = audit_window(source, start, end)
        all_rows.extend(rows)
        for r in rows:
            print(json.dumps(r, ensure_ascii=False))

    recent_start, recent_end = windows[-1]
    print("\n=== 最近7天无原生SPU但SKU前缀可恢复的Top40 ===")
    for r in top_missing(source, recent_start, recent_end):
        print(json.dumps({
            "shop": r.get("store_name"),
            "sku_prefix_spu": r.get("sku_prefix_spu"),
            "rows_n": int(r.get("rows_n") or 0),
            "sales_units": float(r.get("sales_units") or 0),
            "sessions": float(r.get("sessions") or 0),
        }, ensure_ascii=False))

    total_rows = sum(int(r["total_rows"]) for r in all_rows)
    missing_rows = sum(int(r["missing_native_rows"]) for r in all_rows)
    recoverable_weight_num = sum(
        float(r["prefix_recovery_rate_on_missing_rows"] or 0) * int(r["missing_native_rows"])
        for r in all_rows
    )
    weighted_recovery = recoverable_weight_num / missing_rows if missing_rows else None

    print("\n=== SPU_IDENTITY_AUDIT_SUMMARY ===")
    print(json.dumps({
        "audited_rows": total_rows,
        "missing_native_rows": missing_rows,
        "missing_native_row_rate": pct(missing_rows, total_rows),
        "weighted_prefix_recovery_rate_on_missing_rows": (
            round(weighted_recovery, 6) if weighted_recovery is not None else None
        ),
        "decision_rule": {
            "safe_for_hybrid_fallback_if": [
                "prefix_recovery_rate_on_missing_rows is near 1.0",
                "native_prefix_agreement_rate is consistently very high",
                "unrecoverable missing sales/session share is negligible"
            ],
            "otherwise": "keep native-only and isolate missing identity rows from research"
        }
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
