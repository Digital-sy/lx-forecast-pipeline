#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V5.

V5 finalizes point-in-time forecastability classification after the UNKNOWN root-cause
study on 2026-09-30.

Observed evidence
-----------------
For all 1,166 V4 UNKNOWN shop+SPU rows in JQ-US/RKZ-US/SY-US/MT-US:
- no exact positive history existed in `销量统计_msku月度` for the current shop+SPU;
- no positive sale was found in the recent 100-day daily product-performance diagnostic;
- many SPUs existed only in another shop, confirming lifecycle must remain shop+SPU.

Therefore V5 removes UNKNOWN from normal monitoring:
1. monthly first-sale known -> keep V3/V4 NEW_VISIBLE or ESTABLISHED;
2. monthly first-sale missing but current 30-day daily sales > 0 -> NEW_VISIBLE
   (fallback for a fresh launch before the monthly table catches up; first-sale date stays NULL);
3. monthly first-sale missing and current 30-day daily sales <= 0 -> COLD_NO_HISTORY.

This is still a shadow system. It writes only forecast_* monitoring tables and never
modifies production forecast/procurement tables.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Dict

from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v4 as v4


def build_feature_rows(snapshot_date: date):
    rows, source = v4.build_feature_rows(snapshot_date)

    transitions = Counter()
    by_store = Counter()
    for r in rows:
        state = base.text(r.get("forecastability")) or "UNKNOWN"
        if state != "UNKNOWN":
            continue

        shop = base.text(r.get("store_name"))
        sales30 = base.num(r.get("sales_30d"))
        if sales30 > 0:
            # The monthly table may lag a very recent first sale. Do not invent an exact
            # first-sale date; simply keep the item inside the visible-new-product monitor.
            r["forecastability"] = "NEW_VISIBLE"
            r["first_sale_date"] = None
            r["months_since_first_sale"] = None
            transitions["UNKNOWN_TO_NEW_VISIBLE_DAILY_ACTIVITY"] += 1
            by_store[(shop, "NEW_VISIBLE_DAILY_ACTIVITY")] += 1
        else:
            r["forecastability"] = "COLD_NO_HISTORY"
            r["first_sale_date"] = None
            r["months_since_first_sale"] = None
            transitions["UNKNOWN_TO_COLD_NO_HISTORY"] += 1
            by_store[(shop, "COLD_NO_HISTORY")] += 1

    states = Counter(base.text(r.get("forecastability")) or "UNKNOWN" for r in rows)
    base.logger.info(
        "V5可预测性最终分层: "
        f"rows={len(rows)}, states={dict(states)}, transitions={dict(transitions)}"
    )
    if by_store:
        detail: Dict[str, Dict[str, int]] = {}
        for (shop, state), cnt in sorted(by_store.items()):
            detail.setdefault(shop, {})[state] = int(cnt)
        base.logger.info(f"V5 UNKNOWN去向按店铺: {detail}")

    # Fail closed: after V5 no ordinary monitoring row should remain UNKNOWN.
    unknown = int(states.get("UNKNOWN", 0))
    if unknown:
        raise RuntimeError(f"V5分层后仍有 UNKNOWN={unknown}，停止写入快照，请先检查分类逻辑")
    return rows, source


def install_patch() -> None:
    v4.install_patch()
    base.build_feature_rows = build_feature_rows


def main() -> int:
    install_patch()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
