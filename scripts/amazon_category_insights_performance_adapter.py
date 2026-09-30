#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only adapter for the actual Amazon Category Insights performance_series schema.

The cloud schema stores time-series points as:
    node_id (internal surrogate), batch_id, retrieved_at, metric_path, range_key,
    series_id, point_label, value_num.

Important:
- performance_series.node_id is NOT the Amazon Browse Node ID.
- The real Browse Node ID is embedded in series_id; this adapter matches only Browse
  Node IDs explicitly supplied by config, so it never guesses node identity.
- Monthly dates are reconstructed from point_label and retrieved_at within each series.
- The latest retrieved series wins when multiple snapshots/batches exist.
- This module never writes to amazon_category_insights.

Backtest caveat:
Historical months reconstructed from a later Category Insights snapshot are not true
point-in-time vintages. Downstream experiments must label this as reconstructed external
history and must not treat it as final production-validation evidence.
"""
from __future__ import annotations

import calendar
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from common.database import db_cursor

DEFAULT_SCHEMA = "amazon_category_insights"
DEFAULT_TABLE = "performance_series"


def _fetch_all(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor(dictionary=True) as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def _ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_$\u4e00-\u9fff]+", str(name or "")):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return f"`{name}`"


def add_months(d: date, delta: int) -> date:
    y = d.year + (d.month - 1 + delta) // 12
    m = (d.month - 1 + delta) % 12 + 1
    return date(y, m, 1)


MONTH_LOOKUP: Dict[str, int] = {}
for i in range(1, 13):
    MONTH_LOOKUP[calendar.month_name[i].lower()] = i
    MONTH_LOOKUP[calendar.month_abbr[i].lower()] = i


def _explicit_month(label: Any) -> Optional[date]:
    """Parse labels that explicitly contain year+month."""
    s = str(label or "").strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m", "%Y/%m", "%Y-%m-%d", "%Y/%m/%d",
        "%b %Y", "%B %Y", "%b-%Y", "%B-%Y",
        "%Y %b", "%Y %B",
    ):
        try:
            d = datetime.strptime(s, fmt)
            return date(d.year, d.month, 1)
        except Exception:
            pass
    # Common Amazon/chart labels such as "Sep '25" or "Sep 25".
    m = re.search(r"\b([A-Za-z]{3,9})\b[^0-9]*(?:'?(\d{2})|(20\d{2}))\b", s)
    if m:
        mon = MONTH_LOOKUP.get(m.group(1).lower())
        yy = m.group(2)
        yyyy = m.group(3)
        if mon:
            year = int(yyyy) if yyyy else 2000 + int(yy)
            return date(year, mon, 1)
    return None


def _month_number(label: Any) -> Optional[int]:
    """Extract a month number from a label without requiring a year."""
    s = str(label or "").strip().lower()
    if not s:
        return None
    explicit = _explicit_month(label)
    if explicit:
        return explicit.month
    for token, mon in sorted(MONTH_LOOKUP.items(), key=lambda kv: -len(kv[0])):
        if re.search(rf"\b{re.escape(token)}\b", s):
            return mon
    # Numeric month labels are accepted only when clearly month-like.
    if re.fullmatch(r"(?:0?[1-9]|1[0-2])", s):
        return int(s)
    return None


def reconstruct_months(rows: Sequence[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], date]]:
    """Assign YYYY-MM to a monthly series.

    Rows must belong to one batch/series and be ordered by source_row. If labels include
    years, use them. Otherwise walk backwards from retrieved_at month so duplicate month
    names across year boundaries are resolved correctly.
    """
    ordered = sorted(rows, key=lambda r: (int(r.get("source_row") or 0), int(r.get("id") or 0)))
    if not ordered:
        return []

    explicit = [_explicit_month(r.get("point_label")) for r in ordered]
    if all(d is not None for d in explicit):
        return [(r, d) for r, d in zip(ordered, explicit) if d is not None]

    retrieved_values = [r.get("retrieved_at") for r in ordered if r.get("retrieved_at") is not None]
    if not retrieved_values:
        return []
    latest_raw = max(retrieved_values)
    if isinstance(latest_raw, datetime):
        cursor = date(latest_raw.year, latest_raw.month, 1)
    elif isinstance(latest_raw, date):
        cursor = date(latest_raw.year, latest_raw.month, 1)
    else:
        dt = datetime.fromisoformat(str(latest_raw).replace("Z", "+00:00"))
        cursor = date(dt.year, dt.month, 1)

    assigned_rev: List[Tuple[Dict[str, Any], date]] = []
    for r in reversed(ordered):
        d_explicit = _explicit_month(r.get("point_label"))
        if d_explicit is not None:
            d = d_explicit
            if d > cursor:
                # Do not invent a future point when a later snapshot label is malformed.
                continue
        else:
            mon = _month_number(r.get("point_label"))
            if mon is None:
                continue
            d = date(cursor.year, mon, 1)
            if d > cursor:
                d = date(cursor.year - 1, mon, 1)
        assigned_rev.append((r, d))
        cursor = add_months(d, -1)

    assigned_rev.reverse()
    return assigned_rev


def _canonical_metric(metric_path: Any) -> Optional[str]:
    p = str(metric_path or "").lower()
    compact = re.sub(r"[^a-z0-9]+", "", p)
    if "unitsold" in compact:
        return "units_sold"
    if "glanceviews" in compact or "pageviews" in compact:
        return "page_views"
    # Avoid mostPopularKeywords: keyword scores are not total market search volume.
    if "mostpopularkeywords" not in compact:
        if "searchvolume" in compact or "searchvol" in compact:
            return "search_volume"
        if "searchclick" in compact:
            return "search_clicks"
    if "netshippedgms" in compact or "netsales" in compact:
        return "net_sales"
    return None


def _match_browse_node(series_id: Any, browse_node_ids: Sequence[str]) -> Optional[str]:
    s = str(series_id or "")
    for node in browse_node_ids:
        # Series IDs observed in this loader use underscores around the Browse Node ID.
        if f"_{node}_" in s:
            return str(node)
    return None


def load_monthly_market(
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
    browse_node_ids: Optional[Sequence[str]] = None,
    start_month: Optional[date] = None,
    end_month: Optional[date] = None,
) -> Tuple[Dict[Tuple[str, date], Dict[str, float]], Dict[str, Any]]:
    """Normalize latest l12m monthly series for explicitly mapped Browse Nodes."""
    nodes = [str(x).strip() for x in (browse_node_ids or []) if str(x).strip()]
    if not nodes:
        raise RuntimeError("browse_node_ids 为空；为了避免误映射，本适配器不会自动猜 Browse Node。")

    schema_q = _ident(schema)
    table_q = _ident(table)

    # Node filtering is performed via series_id because node_id is an internal surrogate.
    node_clauses = " OR ".join(["series_id LIKE %s"] * len(nodes))
    params: List[Any] = [f"%_{n}_%" for n in nodes]
    sql = f"""
        SELECT id, batch_id, node_id, source_row, retrieved_at,
               section_name, metric_path, range_key, series_id,
               point_label, value_num, value_text, unit
        FROM {schema_q}.{table_q}
        WHERE section_name='demand'
          AND range_key='l12m'
          AND ({node_clauses})
        ORDER BY retrieved_at, batch_id, series_id, source_row, id
    """
    rows = _fetch_all(sql, params)
    if not rows:
        raise RuntimeError(
            f"`{schema}`.`{table}` 没有找到已映射 Browse Node {nodes} 的 l12m 时序数据。"
        )

    # Group by actual Browse Node + metric + batch/snapshot/series.
    grouped: Dict[Tuple[str, str, int, str, str], List[Dict[str, Any]]] = defaultdict(list)
    skipped_metric_paths = set()
    for r in rows:
        node = _match_browse_node(r.get("series_id"), nodes)
        canon = _canonical_metric(r.get("metric_path"))
        if not node:
            continue
        if not canon:
            skipped_metric_paths.add(str(r.get("metric_path") or ""))
            continue
        batch_id = int(r.get("batch_id") or 0)
        retrieved = str(r.get("retrieved_at") or "")
        series_id = str(r.get("series_id") or "")
        grouped[(node, canon, batch_id, retrieved, series_id)].append(r)

    # Latest snapshot wins separately for each node/metric.
    candidates: Dict[Tuple[str, str], List[Tuple[Tuple[str, int], Tuple[str, str, int, str, str]]]] = defaultdict(list)
    for key in grouped:
        node, canon, batch_id, retrieved, _series_id = key
        candidates[(node, canon)].append(((retrieved, batch_id), key))

    selected_keys = set()
    for nk, versions in candidates.items():
        versions.sort(key=lambda x: x[0])
        selected_keys.add(versions[-1][1])

    out: Dict[Tuple[str, date], Dict[str, float]] = defaultdict(dict)
    conflicts: List[Tuple[str, date, str, float, float]] = []
    source_info: List[Dict[str, Any]] = []
    for key in sorted(selected_keys):
        node, canon, batch_id, retrieved, series_id = key
        series_rows = grouped[key]
        assigned = reconstruct_months(series_rows)
        source_info.append({
            "browse_node_id": node,
            "metric": canon,
            "batch_id": batch_id,
            "retrieved_at": retrieved,
            "series_id": series_id,
            "points": len(series_rows),
            "parsed_months": len(assigned),
            "min_month": min((d for _r, d in assigned), default=None),
            "max_month": max((d for _r, d in assigned), default=None),
        })
        for r, m in assigned:
            if start_month and m < start_month:
                continue
            if end_month and m > end_month:
                continue
            raw = r.get("value_num")
            if raw is None:
                continue
            v = float(raw)
            k = (node, m)
            if canon in out[k] and abs(out[k][canon] - v) > max(1e-9, 1e-9 * max(abs(v), abs(out[k][canon]), 1.0)):
                conflicts.append((node, m, canon, out[k][canon], v))
            out[k][canon] = v

    if conflicts:
        raise RuntimeError(f"最新 l12m 序列内部出现 node/month/metric 冲突，示例: {conflicts[:10]}")

    meta = {
        "schema": schema,
        "table": table,
        "mode": "performance_series_l12m",
        "browse_node_filter": nodes,
        "rows_read": len(rows),
        "monthly_node_rows": len(out),
        "selected_series": source_info,
        "skipped_metric_paths_sample": sorted(skipped_metric_paths)[:30],
        "backtest_vintage": "reconstructed_from_latest_snapshot",
    }
    return dict(out), meta


def probe_node(
    browse_node_id: str,
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
) -> None:
    """Print a compact node-level probe for adapter validation."""
    node = str(browse_node_id)
    market, meta = load_monthly_market(schema=schema, table=table, browse_node_ids=[node])
    print("=== Adapter meta ===")
    print(meta)
    print(f"\n=== Browse Node {node} monthly canonical metrics ===")
    for (n, m), vals in sorted(market.items()):
        if n == node:
            print(m.strftime("%Y-%m"), vals)

    schema_q = _ident(schema)
    table_q = _ident(table)
    rows = _fetch_all(
        f"""
        SELECT metric_path, range_key, COUNT(*) AS cnt,
               MIN(retrieved_at) AS min_retrieved_at, MAX(retrieved_at) AS max_retrieved_at
        FROM {schema_q}.{table_q}
        WHERE series_id LIKE %s
        GROUP BY metric_path, range_key
        ORDER BY metric_path, range_key
        """,
        (f"%_{node}_%",),
    )
    print(f"\n=== Browse Node {node} all metric_path/range_key ===")
    for r in rows:
        print(r)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="1044544")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    args = ap.parse_args()
    probe_node(args.node, args.schema, args.table)
