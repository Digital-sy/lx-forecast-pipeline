#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Compatibility wrapper over amazon_category_insights_performance_adapter.

Adds the actual Category Insights click path:
    demand.clickCount.l12m -> search_clicks

Read-only. All loading/date-reconstruction logic remains in the validated base adapter.
"""
from __future__ import annotations

from typing import Any, Optional

from scripts import amazon_category_insights_performance_adapter as _base

DEFAULT_SCHEMA = _base.DEFAULT_SCHEMA
DEFAULT_TABLE = _base.DEFAULT_TABLE

_BASE_CANONICAL_METRIC = _base._canonical_metric


def _canonical_metric_v2(metric_path: Any) -> Optional[str]:
    p = str(metric_path or "").lower()
    compact = __import__("re").sub(r"[^a-z0-9]+", "", p)
    if "clickcount" in compact:
        return "search_clicks"
    return _BASE_CANONICAL_METRIC(metric_path)


def _patched_call(fn, *args, **kwargs):
    old = _base._canonical_metric
    _base._canonical_metric = _canonical_metric_v2
    try:
        return fn(*args, **kwargs)
    finally:
        _base._canonical_metric = old


def load_monthly_market(*args, **kwargs):
    return _patched_call(_base.load_monthly_market, *args, **kwargs)


def probe_node(*args, **kwargs):
    return _patched_call(_base.probe_node, *args, **kwargs)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="1044544")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    args = ap.parse_args()
    probe_node(args.node, args.schema, args.table)
