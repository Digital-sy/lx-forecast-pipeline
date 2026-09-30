#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only lightweight preflight for the dynamic forecast monitoring pipeline.

No tables are created and no data is modified.
Avoids COUNT(*) / MIN / MAX full-table scans on 20M+ row ODS tables.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from jobs.forecast_monitoring import daily_monitor as dm
from jobs.forecast_monitoring import daily_monitor_v2 as dmv2


def safe_print_candidate(table: str) -> None:
    exists = dm.table_exists(table)
    print(f"- {table}: exists={exists}")
    if not exists:
        return

    cols = dm.get_columns(table)
    print(f"  columns({len(cols)}): {cols}")
    mapping = {
        key: dm.pick_col(cols, candidates, required=False)
        for key, candidates in dm.COLUMN_CANDIDATES.items()
    }
    print("  candidate_mapping=" + json.dumps(mapping, ensure_ascii=False))
    print(f"  approx_rows={dmv2.approximate_table_rows(table)}")

    dcol = mapping.get("date")
    if not dcol:
        print("  latest_probe=SKIP(no date field)")
        return

    try:
        indexes = dmv2.date_index_info(table, dcol)
        print("  date_indexes=" + json.dumps(indexes, ensure_ascii=False, default=str))
    except Exception as exc:
        print(f"  date_indexes=ERROR {type(exc).__name__}: {exc}")

    try:
        cutoff = date.today() - timedelta(days=1)
        latest = dmv2.probe_latest_date(table, dcol, cutoff)
        lag = (cutoff - latest).days
        print(f"  latest_date={latest}, freshness_lag_days={lag}")
        if lag > dmv2.MAX_STALENESS_DAYS:
            print(f"  WARNING: stale>{dmv2.MAX_STALENESS_DAYS}d")
    except Exception as exc:
        print(f"  latest_probe=ERROR {type(exc).__name__}: {exc}")


def main() -> int:
    print("=" * 80)
    print("销量预测动态监控：数据源只读轻量预检 V2")
    print("=" * 80)

    print("\n[1] 产品表现候选表")
    for table in dm.PERFORMANCE_TABLE_CANDIDATES:
        safe_print_candidate(table)

    print("\n[2] 自动选择的产品表现源（按最新日期选，过旧则fail closed）")
    try:
        source = dmv2.resolve_performance_source(date.today())
        print(json.dumps(source, ensure_ascii=False, default=str, indent=2))
    except Exception as exc:
        print(f"  ERROR: {type(exc).__name__}: {exc}")

    print("\n[3] FBA库存源")
    exists = dm.table_exists(dm.FBA_TABLE)
    print(f"- {dm.FBA_TABLE}: exists={exists}")
    if exists:
        cols = dm.get_columns(dm.FBA_TABLE)
        print(f"  columns({len(cols)}): {cols}")
        print(f"  approx_rows={dmv2.approximate_table_rows(dm.FBA_TABLE)}")

    print("\n[4] 产品管理 / 店铺 / 月销量")
    for table in (dm.PRODUCT_TABLE, dm.STORE_TABLE, dm.MONTHLY_SALES_TABLE):
        exists = dm.table_exists(table)
        print(f"- {table}: exists={exists}")
        if exists:
            cols = dm.get_columns(table)
            print(f"  columns({len(cols)}): {cols}")

    print("\n[5] 当前生产预测表")
    exists = dm.table_exists(dm.PRODUCTION_FORECAST_TABLE)
    print(f"- {dm.PRODUCTION_FORECAST_TABLE}: exists={exists}")
    if exists:
        print("  columns=", dm.get_columns(dm.PRODUCTION_FORECAST_TABLE))

    print("\n判定说明：")
    print("1. 不再对2700万级ODS表执行COUNT(*)/MIN/MAX全表聚合。")
    print("2. 产品表现源按最新可见日期选择；同日优先 ods_lx_product_performance。")
    print(f"3. freshest source落后超过{dmv2.MAX_STALENESS_DAYS}天时，正式每日任务直接失败，防止静默使用旧数据。")
    print("4. sessions_total 已在候选映射中，可用于Sessions/CVR日特征。")
    print("5. 库存无历史日表，只能从启用日开始积累，不允许伪回填。")
    print("6. 本脚本只读，不创建forecast_*表。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
