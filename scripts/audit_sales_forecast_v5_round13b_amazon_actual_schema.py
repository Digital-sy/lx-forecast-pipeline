#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Round-13 runner wired to the actual amazon_category_insights performance_series schema.

This intentionally reuses all A17/A18 forecast logic from round-13 unchanged and only
replaces the market-data adapter. That keeps the experiment attributable: any result
change comes from real Amazon market evidence, not simultaneous forecast-rule changes.
"""
from __future__ import annotations

from typing import Any

from scripts import audit_sales_forecast_v5_round13_amazon_market_signal as r13
from scripts.amazon_category_insights_performance_adapter import (
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    load_monthly_market,
    probe_node,
)


def _compat_load_monthly_metrics(
    schema: str = DEFAULT_SCHEMA,
    table: str | None = None,
    start_month=None,
    end_month=None,
    node_ids=None,
    **_ignored: Any,
):
    return load_monthly_market(
        schema=schema,
        table=table or DEFAULT_TABLE,
        browse_node_ids=node_ids,
        start_month=start_month,
        end_month=end_month,
    )


def main() -> int:
    # Monkey-patch only the data-loading seam. A17/A18 logic remains exactly Round-13.
    r13.load_monthly_metrics = _compat_load_monthly_metrics
    return r13.main()


if __name__ == "__main__":
    raise SystemExit(main())
