#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Amazon Category Insights adapter V3: reconstruct a conservative 24-month market history.

Read-only. No DDL/DML.

Why V3
------
The verified performance_series schema exposes:
- l12m monthly series for current/recent market metrics;
- yearOnYearUnitSold.pv_ye and yearOnYearGlanceViews.pv_ye for the previous-year
  comparison line.

For a snapshot retrieved in 2026-09, the latest *complete* month is 2026-08. Therefore:
- l12m complete window: 2025-09 .. 2026-08
- pv_ye previous comparison window: 2024-09 .. 2025-08

This yields 24 continuous complete months for units_sold and page_views.
search_volume/search_clicks/net_sales remain l12m-only because no verified previous-year
comparison series has been identified for them.

Important safety decision
-------------------------
`pr_ye` is NOT used to reconstruct history. In the verified 1044544 sample its September
value is semantically ambiguous relative to l12m/current partial-month data. V3 uses only:
1) verified l12m absolute series; and
2) verified pv_ye previous-year absolute comparison lines.

Backtest caveat
---------------
These are reconstructed from a later Amazon snapshot, not true point-in-time vintages.
Use for signal-value experiments, not as final leakage-free production validation.
"""
from __future__ import annotations

import calendar
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from scripts.amazon_category_insights_performance_adapter_v2 import (
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    _fetch_all,
    _ident,
    _match_browse_node,
    add_months,
    load_monthly_market as load_l12m_market,
)


PV_METRIC_PATHS = {
    "demand.yearOnYearUnitSold.pv_ye": "units_sold",
    "demand.yearOnYearGlanceViews.pv_ye": "page_views",
}

MONTH_NUM = {calendar.month_abbr[i].lower(): i for i in range(1, 13)}
MONTH_NUM.update({calendar.month_name[i].lower(): i for i in range(1, 13)})


def _month_from_label(label: Any) -> Optional[int]:
    s = str(label or "").strip().lower()
    if not s:
        return None
    token = re.sub(r"[^a-z]", "", s)
    if token in MONTH_NUM:
        return MONTH_NUM[token]
    for name, mon in sorted(MONTH_NUM.items(), key=lambda kv: -len(kv[0])):
        if re.search(rf"\b{re.escape(name)}\b", s):
            return mon
    return None


def _latest_complete_month(retrieved_at: Any) -> date:
    if isinstance(retrieved_at, datetime):
        current = date(retrieved_at.year, retrieved_at.month, 1)
    elif isinstance(retrieved_at, date):
        current = date(retrieved_at.year, retrieved_at.month, 1)
    else:
        dt = datetime.fromisoformat(str(retrieved_at).replace("Z", "+00:00"))
        current = date(dt.year, dt.month, 1)
    return add_months(current, -1)


def _previous_window_month(month_num: int, previous_end: date) -> date:
    """Map Jan..Dec label into the previous 12-month comparison window.

    Example with previous_end=2025-08:
      Jan-Aug -> 2025
      Sep-Dec -> 2024
    """
    year = previous_end.year if month_num <= previous_end.month else previous_end.year - 1
    return date(year, month_num, 1)


def _load_previous_year_comparison(
    schema: str,
    table: str,
    browse_node_ids: Sequence[str],
) -> Tuple[Dict[Tuple[str, date], Dict[str, float]], List[Dict[str, Any]]]:
    nodes = [str(x).strip() for x in browse_node_ids if str(x).strip()]
    if not nodes:
        return {}, []

    schema_q = _ident(schema)
    table_q = _ident(table)
    node_clauses = " OR ".join(["series_id LIKE %s"] * len(nodes))
    metric_ph = ",".join(["%s"] * len(PV_METRIC_PATHS))
    params: List[Any] = list(PV_METRIC_PATHS) + [f"%_{n}_%" for n in nodes]

    rows = _fetch_all(
        f"""
        SELECT id, batch_id, source_row, retrieved_at, metric_path, range_key,
               series_id, point_label, value_num, value_text, unit
        FROM {schema_q}.{table_q}
        WHERE metric_path IN ({metric_ph})
          AND range_key='pv_ye'
          AND ({node_clauses})
        ORDER BY retrieved_at, batch_id, series_id, source_row, id
        """,
        params,
    )

    grouped: Dict[Tuple[str, str, int, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        node = _match_browse_node(r.get("series_id"), nodes)
        canon = PV_METRIC_PATHS.get(str(r.get("metric_path") or ""))
        if not node or not canon:
            continue
        key = (
            node,
            canon,
            int(r.get("batch_id") or 0),
            str(r.get("retrieved_at") or ""),
            str(r.get("series_id") or ""),
        )
        grouped[key].append(r)

    latest: Dict[Tuple[str, str], Tuple[Tuple[str, int], Tuple[str, str, int, str, str]]] = {}
    for key in grouped:
        node, canon, batch_id, retrieved, _series = key
        nk = (node, canon)
        rank = (retrieved, batch_id)
        if nk not in latest or rank > latest[nk][0]:
            latest[nk] = (rank, key)

    out: Dict[Tuple[str, date], Dict[str, float]] = defaultdict(dict)
    source_info: List[Dict[str, Any]] = []
    for (_node_metric, (_rank, key)) in sorted(latest.items()):
        node, canon, batch_id, retrieved, series_id = key
        series_rows = grouped[key]
        retrieved_values = [r.get("retrieved_at") for r in series_rows if r.get("retrieved_at") is not None]
        if not retrieved_values:
            continue
        latest_retrieved = max(retrieved_values)
        complete_end = _latest_complete_month(latest_retrieved)
        previous_end = add_months(complete_end, -12)
        previous_start = add_months(previous_end, -11)

        parsed = 0
        for r in series_rows:
            mon = _month_from_label(r.get("point_label"))
            if mon is None or r.get("value_num") is None:
                continue
            m = _previous_window_month(mon, previous_end)
            if not (previous_start <= m <= previous_end):
                continue
            out[(node, m)][canon] = float(r["value_num"])
            parsed += 1

        source_info.append({
            "browse_node_id": node,
            "metric": canon,
            "batch_id": batch_id,
            "retrieved_at": retrieved,
            "series_id": series_id,
            "window_start": previous_start,
            "window_end": previous_end,
            "points": len(series_rows),
            "parsed_months": parsed,
        })

    return dict(out), source_info


def load_monthly_market_24m(
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
    browse_node_ids: Optional[Sequence[str]] = None,
    start_month: Optional[date] = None,
    end_month: Optional[date] = None,
) -> Tuple[Dict[Tuple[str, date], Dict[str, float]], Dict[str, Any]]:
    nodes = [str(x).strip() for x in (browse_node_ids or []) if str(x).strip()]
    current, current_meta = load_l12m_market(
        schema=schema,
        table=table,
        browse_node_ids=nodes,
        start_month=None,
        end_month=None,
    )
    previous, previous_sources = _load_previous_year_comparison(schema, table, nodes)

    merged: Dict[Tuple[str, date], Dict[str, float]] = defaultdict(dict)
    source_flag: Dict[Tuple[str, date, str], str] = {}

    # Previous-year comparison first; verified l12m wins on any overlap.
    for key, vals in previous.items():
        for metric, value in vals.items():
            merged[key][metric] = value
            source_flag[(key[0], key[1], metric)] = "pv_ye_previous_window"

    for key, vals in current.items():
        for metric, value in vals.items():
            merged[key][metric] = value
            source_flag[(key[0], key[1], metric)] = "l12m"

    filtered: Dict[Tuple[str, date], Dict[str, float]] = {}
    for (node, month), vals in sorted(merged.items()):
        if start_month and month < start_month:
            continue
        if end_month and month > end_month:
            continue
        filtered[(node, month)] = dict(vals)

    coverage: Dict[str, Dict[str, Any]] = {}
    for node in nodes:
        node_rows = [(m, vals) for (n, m), vals in filtered.items() if n == node]
        metric_months: Dict[str, List[date]] = defaultdict(list)
        for m, vals in node_rows:
            for metric in vals:
                metric_months[metric].append(m)
        coverage[node] = {
            metric: {
                "months": len(months),
                "min_month": min(months) if months else None,
                "max_month": max(months) if months else None,
            }
            for metric, months in metric_months.items()
        }

    meta = {
        "schema": schema,
        "table": table,
        "mode": "performance_series_24m_l12m_plus_pv_ye",
        "browse_node_filter": nodes,
        "current_l12m_meta": current_meta,
        "previous_year_sources": previous_sources,
        "monthly_node_rows": len(filtered),
        "coverage": coverage,
        "backtest_vintage": "reconstructed_from_latest_snapshot",
        "pr_ye_used": False,
    }
    return filtered, meta


def probe_node_24m(
    browse_node_id: str,
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
) -> None:
    node = str(browse_node_id)
    market, meta = load_monthly_market_24m(
        schema=schema,
        table=table,
        browse_node_ids=[node],
    )
    print("=== V3 Adapter meta ===")
    print(meta)
    print(f"\n=== Browse Node {node} reconstructed monthly market ===")
    for (n, m), vals in sorted(market.items()):
        if n == node:
            print(m.strftime("%Y-%m"), vals)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="1044544")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    args = ap.parse_args()
    probe_node_24m(args.node, args.schema, args.table)
