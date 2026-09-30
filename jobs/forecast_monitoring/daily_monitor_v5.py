#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V5.

V5 finalizes point-in-time forecastability classification after the UNKNOWN root-cause
study on 2026-09-30 and adds a latest-day source-completeness guard before feature
snapshots are persisted.

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

Daily source completeness
-------------------------
A source date merely existing does not prove the daily load is complete. Before feature
snapshotting, V5 compares the latest visible row count for each target shop with the
median of the preceding 7 available days. If any target shop is below 80% of its recent
median, the feature/prediction part of the run fails closed so partial data is not stored
as a valid training snapshot. The query is bounded to 8 days on the indexed date field.

This is still a shadow system. It writes only forecast_* monitoring tables and never
modifies production forecast/procurement tables.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import median
from typing import Any, Dict, List

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v4 as v4

LATEST_DAY_MIN_RATIO = 0.80
LATEST_DAY_LOOKBACK_DAYS = 7


def assert_latest_day_complete(source: Dict[str, Any]) -> None:
    """Fail closed when the freshest daily product-performance load looks partial."""
    table = source["table"]
    dcol = source["date"]
    store_col = source.get("store")
    delete_col = source.get("delete_flag")
    as_of: date = source["as_of_date"]

    if not store_col:
        base.logger.warning("产品表现源无店铺字段，跳过最新日完整性检查")
        return

    start = as_of - timedelta(days=LATEST_DAY_LOOKBACK_DAYS)
    delete_filter = f"AND COALESCE(`{delete_col}`,0)=0" if delete_col else ""
    sql = f"""
        SELECT `{dcol}` AS dt, `{store_col}` AS store_name, COUNT(*) AS row_n
        FROM {table}
        WHERE `{dcol}` BETWEEN %s AND %s
          AND `{store_col}` IN (%s,%s,%s,%s)
          {delete_filter}
        GROUP BY `{dcol}`, `{store_col}`
        ORDER BY `{dcol}`, `{store_col}`
    """
    params: List[Any] = [start, as_of, *v4.TARGET_SHOPS]
    with db_cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    by_shop: Dict[str, Dict[date, int]] = defaultdict(dict)
    for r in rows:
        shop = base.text(r.get("store_name"))
        d = r.get("dt")
        if hasattr(d, "date") and not isinstance(d, date):
            d = d.date()
        if not isinstance(d, date):
            continue
        by_shop[shop][d] = int(r.get("row_n") or 0)

    detail: Dict[str, Dict[str, Any]] = {}
    failures: List[str] = []
    for shop in v4.TARGET_SHOPS:
        day_map = by_shop.get(shop, {})
        latest_n = int(day_map.get(as_of, 0))
        prior = [
            int(n) for d, n in sorted(day_map.items())
            if d < as_of and int(n) > 0
        ]
        if not prior:
            failures.append(f"{shop}: 无历史日可用于完整性基线")
            detail[shop] = {"latest_rows": latest_n, "prior_median": None, "ratio": None}
            continue
        med = float(median(prior[-LATEST_DAY_LOOKBACK_DAYS:]))
        ratio = (latest_n / med) if med > 0 else None
        detail[shop] = {
            "latest_rows": latest_n,
            "prior_median": round(med, 1),
            "ratio": None if ratio is None else round(ratio, 4),
        }
        if latest_n <= 0:
            failures.append(f"{shop}: 最新日无数据")
        elif ratio is not None and ratio < LATEST_DAY_MIN_RATIO:
            failures.append(
                f"{shop}: 最新日行数{latest_n}仅为前期中位数{med:.1f}的{ratio:.1%}"
            )

    base.logger.info(
        f"V5最新日完整性检查: as_of={as_of}, threshold={LATEST_DAY_MIN_RATIO:.0%}, detail={detail}"
    )
    if failures:
        raise RuntimeError(
            "产品表现最新日疑似未完整同步，停止写入当天特征/预测快照；" + "；".join(failures)
        )


def build_feature_rows(snapshot_date: date):
    rows, source = v4.build_feature_rows(snapshot_date)
    assert_latest_day_complete(source)

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
