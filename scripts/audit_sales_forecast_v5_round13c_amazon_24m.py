#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Round-13C: reuse A17/A18 logic with conservative reconstructed 24m Amazon history.

Only the market-data seam changes relative to Round-13:
- current/recent metrics come from verified l12m series;
- older units_sold/page_views come from verified pv_ye previous-year comparison lines;
- pr_ye is intentionally unused.

A17/A18 forecast rules are unchanged so attribution remains clean.
"""
from __future__ import annotations

from typing import Any

from scripts import audit_sales_forecast_v5_round13_amazon_market_signal as r13
from scripts.amazon_category_insights_performance_adapter_v3 import (
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    load_monthly_market_24m,
    probe_node_24m,
)


def _compat_load_monthly_metrics(
    schema: str = DEFAULT_SCHEMA,
    table: str | None = None,
    start_month=None,
    end_month=None,
    node_ids=None,
    **_ignored: Any,
):
    return load_monthly_market_24m(
        schema=schema,
        table=table or DEFAULT_TABLE,
        browse_node_ids=node_ids,
        start_month=start_month,
        end_month=end_month,
    )


def main() -> int:
    r13.load_monthly_metrics = _compat_load_monthly_metrics
    return r13.main()


if __name__ == "__main__":
    raise SystemExit(main())
