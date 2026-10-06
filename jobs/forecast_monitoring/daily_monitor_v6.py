#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V6.

V6 keeps all V5 behavior and adds one business exclusion:
- SPUs starting with ``LCS-`` are special low-price handling products, not normal-selling
  launches, so they must not enter NEW_VISIBLE breakout scoring/alerts.

Feature snapshots are still preserved as raw operational facts. Only breakout candidate
selection/notification scope is filtered. Production forecast/procurement tables remain untouched.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Mapping, Sequence

from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v5 as v5

SPECIAL_LOW_PRICE_PREFIXES = ("LCS-",)
_ORIGINAL_BREAKOUT_WATCH = base.breakout_watch
_ORIGINAL_MAYBE_NOTIFY = base.maybe_notify


def is_special_low_price_spu(value: Any) -> bool:
    spu = base.text(value).upper()
    return any(spu.startswith(prefix) for prefix in SPECIAL_LOW_PRICE_PREFIXES)


def breakout_watch(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normal_rows = [r for r in rows if not is_special_low_price_spu(r.get("spu"))]
    excluded = len(rows) - len(normal_rows)
    if excluded:
        base.logger.info(f"V6特殊低价SPU摘除: LCS-*={excluded} rows; 不进入Breakout评分")
    alerts = _ORIGINAL_BREAKOUT_WATCH(normal_rows)
    for r in alerts:
        r["monitor_version"] = "RULE_V0_MONITOR_ONLY_EXCLUDE_LCS"
    return alerts


def maybe_notify(
    snapshot_date: date,
    features: Sequence[Mapping[str, Any]],
    alerts: Sequence[Mapping[str, Any]],
) -> None:
    normal_features = [r for r in features if not is_special_low_price_spu(r.get("spu"))]
    _ORIGINAL_MAYBE_NOTIFY(snapshot_date, normal_features, alerts)


def install_patch() -> None:
    v5.install_patch()
    base.breakout_watch = breakout_watch
    base.maybe_notify = maybe_notify


def main() -> int:
    install_patch()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
