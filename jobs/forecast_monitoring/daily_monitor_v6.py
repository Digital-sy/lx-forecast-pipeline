#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V6.

V6 keeps all V5 behavior and adds one business exclusion:
- ``LCS-`` identifies special low-price handling SKU/MSKU rows, not normal-selling
  launches;
- all SPUs mapped from any LCS-* SKU in ``销量统计_msku月度`` are excluded as a whole
  from NEW_VISIBLE breakout scoring/alerts.

Feature snapshots are still preserved as raw operational facts. Only breakout candidate
selection/notification scope is filtered. Production forecast/procurement tables remain untouched.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Mapping, Sequence, Set

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v5 as v5

SPECIAL_LOW_PRICE_PREFIXES = ("LCS-",)
_ORIGINAL_BREAKOUT_WATCH = base.breakout_watch
_ORIGINAL_MAYBE_NOTIFY = base.maybe_notify
_SPECIAL_SPUS_CACHE: Set[str] | None = None


def load_special_low_price_spus() -> Set[str]:
    global _SPECIAL_SPUS_CACHE
    if _SPECIAL_SPUS_CACHE is not None:
        return _SPECIAL_SPUS_CACHE

    if not base.table_exists(base.MONTHLY_SALES_TABLE):
        raise RuntimeError(
            f"无法执行LCS业务排除：{base.MONTHLY_SALES_TABLE} 不存在"
        )
    cols = set(base.get_columns(base.MONTHLY_SALES_TABLE))
    required = {"SKU", "SPU"}
    if not required.issubset(cols):
        raise RuntimeError(
            f"无法执行LCS业务排除：{base.MONTHLY_SALES_TABLE} 缺字段 {sorted(required-cols)}"
        )

    with db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT DISTINCT TRIM(`SPU`) AS spu
            FROM `{base.MONTHLY_SALES_TABLE}`
            WHERE `SPU` IS NOT NULL AND TRIM(`SPU`)<>''
              AND UPPER(TRIM(COALESCE(`SKU`,''))) LIKE 'LCS-%%'
            """
        )
        rows = cursor.fetchall()

    _SPECIAL_SPUS_CACHE = {
        base.text(r.get("spu")).upper()
        for r in rows
        if base.text(r.get("spu"))
    }
    base.logger.info(
        f"V6加载LCS特殊低价SPU排除清单: {len(_SPECIAL_SPUS_CACHE)} 个SPU"
    )
    return _SPECIAL_SPUS_CACHE


def is_special_low_price_spu(value: Any, excluded_spus: Set[str]) -> bool:
    spu = base.text(value).upper()
    return spu in excluded_spus or any(
        spu.startswith(prefix) for prefix in SPECIAL_LOW_PRICE_PREFIXES
    )


def breakout_watch(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    excluded_spus = load_special_low_price_spus()
    normal_rows = [
        r for r in rows
        if not is_special_low_price_spu(r.get("spu"), excluded_spus)
    ]
    excluded = len(rows) - len(normal_rows)
    if excluded:
        base.logger.info(
            f"V6特殊低价SPU摘除: {excluded} feature rows; 不进入Breakout评分"
        )
    alerts = _ORIGINAL_BREAKOUT_WATCH(normal_rows)
    for r in alerts:
        r["monitor_version"] = "RULE_V0_MONITOR_ONLY_EXCLUDE_LCS_MAPPED_SPU"
    return alerts


def maybe_notify(
    snapshot_date: date,
    features: Sequence[Mapping[str, Any]],
    alerts: Sequence[Mapping[str, Any]],
) -> None:
    excluded_spus = load_special_low_price_spus()
    normal_features = [
        r for r in features
        if not is_special_low_price_spu(r.get("spu"), excluded_spus)
    ]
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
