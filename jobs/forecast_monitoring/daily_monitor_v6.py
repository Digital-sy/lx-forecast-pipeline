#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V6.

V6 keeps all V5 behavior and applies business exclusions:
- ``LCS-`` identifies special low-price handling SKU/MSKU rows, not normal-selling
  launches;
- all SPUs mapped from audited LCS-* MSKUs are excluded as a whole;
- all SPUs whose code starts with XH are also excluded as a whole;
- materialized controls live in ``forecast_special_spu_exclusion``.

Feature snapshots are still preserved as raw operational facts. Only breakout candidate
selection/notification scope is filtered. Production forecast/procurement tables remain untouched.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Mapping, Sequence, Set

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v5 as v5

SPECIAL_EXCLUSION_TABLE = "forecast_special_spu_exclusion"
LCS_EXCLUSION_CODE = "LCS_SPECIAL_LOW_PRICE"
XH_EXCLUSION_CODE = "XH_PREFIX_EXCLUSION"
XH_PREFIX = "XH"
_ORIGINAL_BREAKOUT_WATCH = base.breakout_watch
_ORIGINAL_MAYBE_NOTIFY = base.maybe_notify
_SPECIAL_SPUS_CACHE: Set[str] | None = None


def load_business_exclusion_spus() -> Set[str]:
    global _SPECIAL_SPUS_CACHE
    if _SPECIAL_SPUS_CACHE is not None:
        return _SPECIAL_SPUS_CACHE

    if not base.table_exists(SPECIAL_EXCLUSION_TABLE):
        raise RuntimeError(
            f"{SPECIAL_EXCLUSION_TABLE} 不存在；先运行 scripts/materialize_lcs_special_spu_exclusion.py"
        )

    with db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT spu
            FROM `{SPECIAL_EXCLUSION_TABLE}`
            WHERE exclusion_code IN (%s,%s)
            """,
            (LCS_EXCLUSION_CODE, XH_EXCLUSION_CODE),
        )
        rows = cursor.fetchall()

    _SPECIAL_SPUS_CACHE = {
        base.text(r.get("spu")).upper()
        for r in rows
        if base.text(r.get("spu"))
    }
    if not _SPECIAL_SPUS_CACHE:
        raise RuntimeError(
            f"{SPECIAL_EXCLUSION_TABLE} 业务排除清单为空；拒绝继续Breakout评分"
        )
    base.logger.info(
        f"V6加载业务SPU排除清单: {len(_SPECIAL_SPUS_CACHE)} 个SPU"
    )
    return _SPECIAL_SPUS_CACHE


def is_business_excluded_spu(value: Any, excluded_spus: Set[str]) -> bool:
    spu = base.text(value).upper()
    return spu in excluded_spus or spu.startswith(XH_PREFIX)

def breakout_watch(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    excluded_spus = load_business_exclusion_spus()
    normal_rows = [
        r for r in rows
        if not is_business_excluded_spu(r.get("spu"), excluded_spus)
    ]
    excluded = len(rows) - len(normal_rows)
    if excluded:
        base.logger.info(
            f"V6业务排除SPU摘除(LCS/XH): {excluded} feature rows; 不进入Breakout评分"
        )
    alerts = _ORIGINAL_BREAKOUT_WATCH(normal_rows)
    for r in alerts:
        r["monitor_version"] = "RULE_V0_MONITOR_ONLY_EXCLUDE_LCS_XH_BUSINESS_SPU"
    return alerts


def maybe_notify(
    snapshot_date: date,
    features: Sequence[Mapping[str, Any]],
    alerts: Sequence[Mapping[str, Any]],
) -> None:
    excluded_spus = load_business_exclusion_spus()
    normal_features = [
        r for r in features
        if not is_business_excluded_spu(r.get("spu"), excluded_spus)
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
