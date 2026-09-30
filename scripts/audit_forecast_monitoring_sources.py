#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only preflight for the dynamic forecast monitoring pipeline.

No tables are created and no data is modified. Run this before enabling the daily job.
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
from jobs.forecast_monitoring import daily_monitor as dm


def row_count_and_range(table: str, date_col: str | None = None):
    with db_cursor() as cursor:
        if date_col:
            cursor.execute(
                f"SELECT COUNT(*) AS cnt, MIN(`{date_col}`) AS min_dt, MAX(`{date_col}`) AS max_dt FROM {table}"
            )
        else:
            cursor.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
        return cursor.fetchone() or {}


def main() -> int:
    print("=" * 80)
    print("销量预测动态监控：数据源只读预检")
    print("=" * 80)

    print("\n[1] 产品表现候选表")
    for table in dm.PERFORMANCE_TABLE_CANDIDATES:
        exists = dm.table_exists(table)
        print(f"- {table}: exists={exists}")
        if not exists:
            continue
        cols = dm.get_columns(table)
        print(f"  columns({len(cols)}): {cols}")
        mapping = {}
        for key, candidates in dm.COLUMN_CANDIDATES.items():
            mapping[key] = dm.pick_col(cols, candidates, required=False)
        print("  candidate_mapping=" + json.dumps(mapping, ensure_ascii=False))
        if mapping.get("date"):
            print("  stats=", row_count_and_range(table, mapping["date"]))

    print("\n[2] 自动选择的产品表现源")
    try:
        source = dm.resolve_performance_source(date.today())
        print(json.dumps(source, ensure_ascii=False, default=str, indent=2))
        max_dt = source.get("as_of_date")
        if max_dt and max_dt < date.today() - timedelta(days=3):
            print(f"  WARNING: 产品表现数据较旧，as_of_date={max_dt}")
    except Exception as exc:
        print(f"  ERROR: {exc}")

    print("\n[3] FBA库存源")
    print(f"- {dm.FBA_TABLE}: exists={dm.table_exists(dm.FBA_TABLE)}")
    if dm.table_exists(dm.FBA_TABLE):
        cols = dm.get_columns(dm.FBA_TABLE)
        print(f"  columns({len(cols)}): {cols}")
        print("  stats=", row_count_and_range(dm.FBA_TABLE))

    print("\n[4] 产品管理 / 店铺 / 月销量")
    for table in (dm.PRODUCT_TABLE, dm.STORE_TABLE, dm.MONTHLY_SALES_TABLE):
        exists = dm.table_exists(table)
        print(f"- {table}: exists={exists}")
        if exists:
            print(f"  columns({len(dm.get_columns(table))}): {dm.get_columns(table)}")

    print("\n[5] 当前生产预测表")
    exists = dm.table_exists(dm.PRODUCTION_FORECAST_TABLE)
    print(f"- {dm.PRODUCTION_FORECAST_TABLE}: exists={exists}")
    if exists:
        print("  columns=", dm.get_columns(dm.PRODUCTION_FORECAST_TABLE))
        print("  stats=", row_count_and_range(dm.PRODUCTION_FORECAST_TABLE))

    print("\n判定说明：")
    print("1. 重点确认产品表现表能识别 date / sid / sku / sales / sessions。")
    print("2. sessions 若未识别，先不要启用正式Breakout监控；把实际流量字段名补入候选映射。")
    print("3. 库存无历史日表，因此只能从启用动态监控当天开始积累，不允许伪回填。")
    print("4. 本脚本只读，不创建 forecast_* 表。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
