#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Safer entrypoint for forecast daily monitoring.

V2 keeps the shadow-monitoring behavior from daily_monitor.py, but replaces product
performance source resolution with a lightweight/fail-closed selector:
- never COUNT(*) a 20M+ row performance table just to discover freshness;
- probe latest visible date with ORDER BY date DESC LIMIT 1;
- inspect every candidate and choose the freshest one;
- prefer ods_lx_product_performance on a freshness tie;
- refuse to run if the freshest source is older than MAX_STALENESS_DAYS.

This module monkey-patches only the source resolver of the V1 shadow job. It does not
change production forecast or procurement tables.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pymysql

from common.database import get_db_connection
from jobs.forecast_monitoring import daily_monitor as base

MAX_STALENESS_DAYS = 3
QUERY_TIMEOUT_MS = 8000
PREFERRED_TABLE = "ods_db.ods_lx_product_performance"


def _normalize_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def _safe_close(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def probe_latest_date(table: str, date_col: str, cutoff: date) -> date:
    """Read-only lightweight freshness probe.

    A dedicated connection is used instead of db_cursor() so a connection timeout does
    not trigger a second rollback exception on an already-dead socket.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            # MySQL 8 SELECT execution guard. Ignore if server/session rejects it.
            try:
                cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={int(QUERY_TIMEOUT_MS)}")
            except Exception:
                pass
            cursor.execute(
                f"SELECT `{date_col}` AS max_dt "
                f"FROM {table} "
                f"WHERE `{date_col}` <= %s "
                f"ORDER BY `{date_col}` DESC LIMIT 1",
                (cutoff,),
            )
            row = cursor.fetchone() or {}
        finally:
            try:
                cursor.close()
            except Exception:
                pass
        d = _normalize_date(row.get("max_dt"))
        if d is None:
            raise RuntimeError(f"{table}: {cutoff} 之前无可用日期")
        return d
    finally:
        _safe_close(conn)


def date_index_info(table: str, date_col: str) -> List[Dict[str, Any]]:
    schema, table_name = base.split_table(table)
    if not schema:
        return []
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(
                """
                SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, CARDINALITY
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """,
                (schema, table_name, date_col),
            )
            return list(cursor.fetchall())
        finally:
            try:
                cursor.close()
            except Exception:
                pass
    finally:
        _safe_close(conn)


def approximate_table_rows(table: str) -> Optional[int]:
    schema, table_name = base.split_table(table)
    if not schema:
        return None
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(
                """
                SELECT TABLE_ROWS
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                """,
                (schema, table_name),
            )
            row = cursor.fetchone() or {}
            v = row.get("TABLE_ROWS")
            return None if v is None else int(v)
        finally:
            try:
                cursor.close()
            except Exception:
                pass
    finally:
        _safe_close(conn)


def resolve_performance_source(snapshot_date: date) -> Dict[str, Any]:
    cutoff = snapshot_date - timedelta(days=1)
    valid: List[Dict[str, Any]] = []
    errors: List[str] = []

    for table in base.PERFORMANCE_TABLE_CANDIDATES:
        if not base.table_exists(table):
            errors.append(f"{table}: 不存在")
            continue
        cols = base.get_columns(table)
        try:
            mapping: Dict[str, Any] = {
                "table": table,
                "date": base.pick_col(cols, base.COLUMN_CANDIDATES["date"], True, "日期字段"),
                "sid": base.pick_col(cols, base.COLUMN_CANDIDATES["sid"], True, "店铺ID字段"),
                "store": base.pick_col(cols, base.COLUMN_CANDIDATES["store"], False),
                "sku": base.pick_col(cols, base.COLUMN_CANDIDATES["sku"], True, "SKU字段"),
                "sales": base.pick_col(cols, base.COLUMN_CANDIDATES["sales"], True, "销量字段"),
                "sessions": base.pick_col(cols, base.COLUMN_CANDIDATES["sessions"], False),
                "delete_flag": base.pick_col(cols, base.COLUMN_CANDIDATES["delete_flag"], False),
            }
            as_of = probe_latest_date(table, str(mapping["date"]), cutoff)
            mapping["as_of_date"] = as_of
            mapping["staleness_days"] = (cutoff - as_of).days
            valid.append(mapping)
        except Exception as exc:
            errors.append(f"{table}: {type(exc).__name__}: {exc}")

    if not valid:
        raise RuntimeError("未找到可探测的日维度产品表现源；" + "; ".join(errors))

    # Freshness first. On an exact date tie prefer the newer non-ASIN table.
    valid.sort(
        key=lambda x: (
            x["as_of_date"],
            1 if x["table"] == PREFERRED_TABLE else 0,
        ),
        reverse=True,
    )
    chosen = valid[0]

    if int(chosen["staleness_days"]) > MAX_STALENESS_DAYS:
        detail = "; ".join(
            f"{x['table']}={x['as_of_date']} (lag {x['staleness_days']}d)" for x in valid
        )
        if errors:
            detail += "; probe_errors=" + " | ".join(errors)
        raise RuntimeError(
            f"产品表现数据过旧，freshest lag={chosen['staleness_days']}天，"
            f"超过允许的{MAX_STALENESS_DAYS}天；{detail}"
        )

    if not chosen.get("sessions"):
        base.logger.warning(
            f"{chosen['table']} 未识别到Sessions字段；流量/CVR特征将保持NULL"
        )
    base.logger.info(
        "产品表现源V2选择: "
        f"{chosen['table']}; as_of={chosen['as_of_date']}; "
        f"lag={chosen['staleness_days']}d; sales={chosen['sales']}; sessions={chosen.get('sessions')}"
    )
    return chosen


def install_patch() -> None:
    base.resolve_performance_source = resolve_performance_source


def main() -> int:
    install_patch()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
