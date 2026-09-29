#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only adapter for the `amazon_category_insights` cloud schema.

Goal
----
Normalize Amazon Category Insights time-series data into:
    (browse_node_id, month) -> canonical metrics

Canonical metrics currently used by forecast experiments:
- units_sold
- search_volume
- search_clicks
- page_views
- net_sales

The source schema is intentionally discovered at runtime because the Category Insights
loader is maintained separately from lx-forecast-pipeline. This module NEVER writes to
that schema.

Safety principles
-----------------
1. Cross-database reads only; no DDL/DML.
2. Prefer explicit CLI column overrides when auto-discovery is ambiguous.
3. Never sum duplicate snapshot rows silently. If an audit/update timestamp exists,
   latest row wins; otherwise conflicting duplicate month/metric rows raise an error.
4. Partial current-month data is not treated as a completed month by downstream code.
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from common.database import db_cursor

DEFAULT_SCHEMA = "amazon_category_insights"


def _fetch_all(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor(dictionary=True) as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def _ident(name: str) -> str:
    """Quote a trusted identifier after conservative validation."""
    if not re.fullmatch(r"[A-Za-z0-9_$\u4e00-\u9fff]+", str(name or "")):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return f"`{name}`"


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").lower())


def schema_exists(schema: str = DEFAULT_SCHEMA) -> bool:
    rows = _fetch_all(
        "SELECT COUNT(*) AS cnt FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s",
        (schema,),
    )
    return bool(rows and int(rows[0].get("cnt") or 0) > 0)


def list_tables(schema: str = DEFAULT_SCHEMA) -> List[str]:
    rows = _fetch_all(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_NAME
        """,
        (schema,),
    )
    return [str(r["TABLE_NAME"]) for r in rows]


def table_columns(schema: str, table: str) -> List[str]:
    rows = _fetch_all(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table),
    )
    return [str(r["COLUMN_NAME"]) for r in rows]


NODE_NAMES = {
    "browsenodeid", "nodeid", "browsenode", "browsenodecode", "categorynodeid"
}
NODE_LABEL_NAMES = {
    "browsenodename", "nodename", "categoryname", "displayname"
}
DATE_NAMES = {
    "month", "metricmonth", "periodmonth", "date", "metricdate", "recorddate",
    "periodstart", "periodstartdate", "startdate", "timeseriesdate", "datapointdate"
}
METRIC_NAMES = {
    "metric", "metricname", "metrickey", "indicator", "indicatorname", "measure", "measurename"
}
VALUE_NAMES = {
    "value", "metricvalue", "numericvalue", "numbervalue", "measurevalue", "amount"
}
AUDIT_NAMES = {
    "updatedat", "updatedtime", "audittime", "collectedat", "collectedtime",
    "fetchedat", "createdat", "ingestedat", "syncedat"
}

WIDE_METRIC_ALIASES: Dict[str, Tuple[str, ...]] = {
    "units_sold": (
        "unitssold", "soldunits", "unitsordered", "orderedunits", "unitssales", "salesunits"
    ),
    "search_volume": (
        "searchvolume", "searches", "searchcount", "searchimpressions"
    ),
    "search_clicks": (
        "searchclicks", "searchclickcount", "clicks", "clickcount"
    ),
    "page_views": (
        "pageviews", "views", "viewcount", "glanceviews"
    ),
    "net_sales": (
        "netsales", "netrevenue", "salesamount", "revenue"
    ),
}

LONG_METRIC_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "units_sold": ("unit sold", "units sold", "sold units", "units ordered", "ordered units"),
    "search_volume": ("search volume", "searches", "search count"),
    "search_clicks": ("search click", "search clicks", "click count"),
    "page_views": ("page view", "page views", "glance view", "views"),
    "net_sales": ("net sales", "net revenue", "sales amount", "revenue"),
}


def _pick_exact(columns: Sequence[str], accepted_norms: Iterable[str]) -> Optional[str]:
    accepted = set(accepted_norms)
    for c in columns:
        if _norm(c) in accepted:
            return c
    return None


def _pick_wide_metric(columns: Sequence[str], aliases: Sequence[str]) -> Optional[str]:
    alias_set = set(aliases)
    for c in columns:
        n = _norm(c)
        if n in alias_set:
            return c
    return None


def discover_candidates(schema: str = DEFAULT_SCHEMA) -> List[Dict[str, Any]]:
    """Return scored source-table candidates without reading large table contents."""
    if not schema_exists(schema):
        raise RuntimeError(
            f"数据库 schema `{schema}` 在当前 MySQL 连接不可见。"
            "如果 Category Insights 位于另一台云数据库，需要为该库增加独立只读连接配置。"
        )

    candidates: List[Dict[str, Any]] = []
    for table in list_tables(schema):
        cols = table_columns(schema, table)
        node = _pick_exact(cols, NODE_NAMES)
        dt = _pick_exact(cols, DATE_NAMES)
        metric = _pick_exact(cols, METRIC_NAMES)
        value = _pick_exact(cols, VALUE_NAMES)
        audit = _pick_exact(cols, AUDIT_NAMES)
        node_name = _pick_exact(cols, NODE_LABEL_NAMES)
        wide = {
            canon: _pick_wide_metric(cols, aliases)
            for canon, aliases in WIDE_METRIC_ALIASES.items()
        }
        wide = {k: v for k, v in wide.items() if v}

        score = 0
        score += 5 if node else 0
        score += 5 if dt else 0
        score += 4 if metric and value else 0
        score += 2 * len(wide)
        score += 1 if audit else 0
        if score:
            candidates.append({
                "table": table,
                "score": score,
                "node_col": node,
                "node_name_col": node_name,
                "date_col": dt,
                "metric_col": metric,
                "value_col": value,
                "audit_col": audit,
                "wide_metrics": wide,
                "columns": cols,
            })
    candidates.sort(key=lambda x: (-int(x["score"]), str(x["table"])))
    return candidates


def describe_schema(schema: str = DEFAULT_SCHEMA, topn: int = 10) -> List[Dict[str, Any]]:
    rows = discover_candidates(schema)
    print(f"=== `{schema}` 候选表（Top {topn}） ===")
    for r in rows[:topn]:
        print({
            "table": r["table"],
            "score": r["score"],
            "node_col": r["node_col"],
            "date_col": r["date_col"],
            "metric_col": r["metric_col"],
            "value_col": r["value_col"],
            "audit_col": r["audit_col"],
            "wide_metrics": r["wide_metrics"],
        })
    return rows


def _month(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    if isinstance(value, (int, float)):
        # Epoch milliseconds/seconds support for generic ingestion tables.
        v = float(value)
        try:
            if v > 10_000_000_000:
                v /= 1000.0
            d = datetime.fromtimestamp(v)
            return date(d.year, d.month, 1)
        except Exception:
            return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y/%m/%d", "%Y/%m", "%Y%m%d", "%Y%m"):
        try:
            d = datetime.strptime(s[:10] if fmt in ("%Y-%m-%d", "%Y/%m/%d") else s, fmt)
            return date(d.year, d.month, 1)
        except Exception:
            pass
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return date(d.year, d.month, 1)
    except Exception:
        return None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def canonical_metric_name(source_name: str) -> Optional[str]:
    s = re.sub(r"[_\-]+", " ", str(source_name or "").strip().lower())
    compact = _norm(source_name)
    for canon, aliases in WIDE_METRIC_ALIASES.items():
        if compact in aliases:
            return canon
    for canon, patterns in LONG_METRIC_PATTERNS.items():
        if any(p in s for p in patterns):
            return canon
    return None


def _resolve_candidate(
    schema: str,
    table: Optional[str] = None,
    node_col: Optional[str] = None,
    date_col: Optional[str] = None,
    metric_col: Optional[str] = None,
    value_col: Optional[str] = None,
    audit_col: Optional[str] = None,
) -> Dict[str, Any]:
    candidates = discover_candidates(schema)
    if table:
        found = [x for x in candidates if x["table"] == table]
        if not found:
            cols = table_columns(schema, table)
            if not cols:
                raise RuntimeError(f"找不到 `{schema}`.`{table}`")
            found = [{
                "table": table, "score": 0, "columns": cols,
                "node_col": None, "node_name_col": None, "date_col": None,
                "metric_col": None, "value_col": None, "audit_col": None,
                "wide_metrics": {},
            }]
        c = dict(found[0])
    else:
        viable = [x for x in candidates if x.get("node_col") and x.get("date_col") and (
            (x.get("metric_col") and x.get("value_col")) or x.get("wide_metrics")
        )]
        if not viable:
            describe_schema(schema)
            raise RuntimeError("未自动发现可读取的 Category Insights 时序表，请通过 CLI 明确指定表/字段。")
        c = dict(viable[0])

    cols = set(c["columns"])
    for key, override in (
        ("node_col", node_col), ("date_col", date_col), ("metric_col", metric_col),
        ("value_col", value_col), ("audit_col", audit_col),
    ):
        if override:
            if override not in cols:
                raise RuntimeError(f"字段 `{override}` 不存在于 `{schema}`.`{c['table']}`")
            c[key] = override
    return c


def _latest_key(audit_value: Any) -> str:
    if audit_value is None:
        return ""
    if isinstance(audit_value, (date, datetime)):
        return audit_value.isoformat()
    return str(audit_value)


def load_monthly_metrics(
    schema: str = DEFAULT_SCHEMA,
    table: Optional[str] = None,
    start_month: Optional[date] = None,
    end_month: Optional[date] = None,
    node_ids: Optional[Sequence[str]] = None,
    node_col: Optional[str] = None,
    date_col: Optional[str] = None,
    metric_col: Optional[str] = None,
    value_col: Optional[str] = None,
    audit_col: Optional[str] = None,
) -> Tuple[Dict[Tuple[str, date], Dict[str, float]], Dict[str, Any]]:
    """Load canonical monthly metrics from an auto-discovered long or wide source table."""
    c = _resolve_candidate(schema, table, node_col, date_col, metric_col, value_col, audit_col)
    table_q = f"{_ident(schema)}.{_ident(c['table'])}"
    node_q = _ident(c["node_col"])
    date_q = _ident(c["date_col"])
    audit = c.get("audit_col")
    node_filter_sql = ""
    params: List[Any] = []
    if node_ids:
        placeholders = ",".join(["%s"] * len(node_ids))
        node_filter_sql = f" AND {node_q} IN ({placeholders})"
        params.extend([str(x) for x in node_ids])

    # Date filtering is intentionally applied in Python because source date types can
    # vary across loader versions. Node filtering remains in SQL to keep reads small.
    records: List[Dict[str, Any]] = []
    mode = "long" if c.get("metric_col") and c.get("value_col") else "wide"

    if mode == "long":
        metric_q = _ident(c["metric_col"])
        value_q = _ident(c["value_col"])
        metric_names = _fetch_all(
            f"SELECT DISTINCT {metric_q} AS metric_name FROM {table_q} "
            f"WHERE {metric_q} IS NOT NULL LIMIT 1000"
        )
        mapped_source_metrics: Dict[str, str] = {}
        for r in metric_names:
            src = str(r.get("metric_name") or "")
            canon = canonical_metric_name(src)
            if canon and canon not in mapped_source_metrics:
                mapped_source_metrics[canon] = src
        if not mapped_source_metrics:
            print("Category Insights metric names sample:", [r.get("metric_name") for r in metric_names[:100]])
            raise RuntimeError("无法把 metric_name 自动映射到销量/搜索/点击/浏览指标。")

        wanted = list(mapped_source_metrics.values())
        metric_ph = ",".join(["%s"] * len(wanted))
        select_audit = f", {_ident(audit)} AS audit_value" if audit else ""
        sql = (
            f"SELECT {node_q} AS node_id, {date_q} AS metric_date, "
            f"{metric_q} AS metric_name, {value_q} AS metric_value{select_audit} "
            f"FROM {table_q} WHERE {metric_q} IN ({metric_ph}){node_filter_sql}"
        )
        records = _fetch_all(sql, tuple(wanted + params))
        source_metric_to_canon = {v: k for k, v in mapped_source_metrics.items()}
    else:
        wide = dict(c.get("wide_metrics") or {})
        if not wide:
            raise RuntimeError("自动识别为 wide table，但没有识别到任何核心指标列。")
        metric_selects = [f"{_ident(src)} AS {_ident(canon)}" for canon, src in wide.items()]
        select_audit = f", {_ident(audit)} AS audit_value" if audit else ""
        sql = (
            f"SELECT {node_q} AS node_id, {date_q} AS metric_date, "
            + ", ".join(metric_selects)
            + select_audit
            + f" FROM {table_q} WHERE 1=1{node_filter_sql}"
        )
        records = _fetch_all(sql, tuple(params))
        source_metric_to_canon = {}

    # Latest audited row wins. Without an audit column, conflicting duplicates are an
    # error instead of being silently summed/averaged.
    values: Dict[Tuple[str, date, str], Tuple[str, float]] = {}
    conflicts: List[Tuple[str, date, str, float, float]] = []
    for r in records:
        node = str(r.get("node_id") or "").strip()
        m = _month(r.get("metric_date"))
        if not node or m is None:
            continue
        if start_month and m < start_month:
            continue
        if end_month and m > end_month:
            continue
        audit_key = _latest_key(r.get("audit_value"))
        pairs: List[Tuple[str, Any]] = []
        if mode == "long":
            canon = source_metric_to_canon.get(str(r.get("metric_name") or ""))
            if canon:
                pairs.append((canon, r.get("metric_value")))
        else:
            for canon in c.get("wide_metrics", {}):
                pairs.append((canon, r.get(canon)))

        for canon, raw in pairs:
            v = _as_float(raw)
            if v is None:
                continue
            key = (node, m, canon)
            if key not in values:
                values[key] = (audit_key, v)
                continue
            old_audit, old_v = values[key]
            if audit:
                if audit_key >= old_audit:
                    values[key] = (audit_key, v)
            elif abs(old_v - v) > max(1e-9, 1e-9 * max(abs(old_v), abs(v), 1.0)):
                conflicts.append((node, m, canon, old_v, v))

    if conflicts:
        raise RuntimeError(
            "Category Insights 同一 node/month/metric 存在冲突重复值，且未识别到审计时间字段。"
            f"示例: {conflicts[:10]}。请通过 --market-audit-col 指定最新批次时间字段。"
        )

    out: Dict[Tuple[str, date], Dict[str, float]] = defaultdict(dict)
    for (node, m, canon), (_audit, v) in values.items():
        out[(node, m)][canon] = v

    meta = {
        "schema": schema,
        "table": c["table"],
        "mode": mode,
        "node_col": c["node_col"],
        "date_col": c["date_col"],
        "metric_col": c.get("metric_col"),
        "value_col": c.get("value_col"),
        "audit_col": audit,
        "wide_metrics": c.get("wide_metrics"),
        "records_read": len(records),
        "monthly_node_rows": len(out),
    }
    return dict(out), meta


if __name__ == "__main__":
    describe_schema(DEFAULT_SCHEMA, 20)
