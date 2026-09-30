#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only diagnostic for UNKNOWN first-sale classification in forecast monitoring.

Purpose
-------
Explain why some target-shop SPUs are UNKNOWN after V4 dry-run.
No table is created or modified.

Checks, at shop+SPU grain:
1. Does monthly sales contain exact positive shop+SPU history?
2. Does monthly sales contain same SPU in another shop?
3. Does monthly sales SKU prefix match the SPU even when SPU column does not?
4. Does daily product-performance show positive sales in the last 100 days?

The last check is diagnostic only. It does not automatically rewrite lifecycle logic.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Sequence, Set, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v4 as v4

TARGET_SHOPS = v4.TARGET_SHOPS


def text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def chunks(seq: Sequence[str], n: int = 300):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def load_monthly_rows() -> List[Dict[str, Any]]:
    with db_cursor() as cursor:
        cursor.execute(
            """
            SELECT `SKU`,`SPU`,`店铺`,`销量`,`统计日期`
            FROM `销量统计_msku月度`
            WHERE `店铺` IN (%s,%s,%s,%s)
              AND COALESCE(`销量`,0) > 0
            """,
            TARGET_SHOPS,
        )
        return list(cursor.fetchall())


def recent_daily_positive(source: Dict[str, Any], unknown: Sequence[Tuple[str, str]]) -> Dict[Tuple[str, str], date]:
    """Earliest positive date within last 100 days for UNKNOWN shop+SPU.

    Uses the performance table's own `spu` column when present. Query is bounded by date
    and shop to keep it read-only and reasonably light on the 100M-row source.
    """
    table = source["table"]
    cols = set(base.get_columns(table))
    if "spu" not in cols:
        return {}
    dcol = source["date"]
    store_col = source["store"]
    sales_col = source["sales"]
    delete_col = source.get("delete_flag")
    as_of = source["as_of_date"]
    start = as_of - timedelta(days=99)

    by_shop: Dict[str, List[str]] = defaultdict(list)
    for shop, spu in unknown:
        by_shop[shop].append(spu)

    result: Dict[Tuple[str, str], date] = {}
    for shop, spus in by_shop.items():
        uniq = sorted(set(spus))
        for part in chunks(uniq, 250):
            placeholders = ",".join(["%s"] * len(part))
            delete_filter = f"AND COALESCE(`{delete_col}`,0)=0" if delete_col else ""
            sql = f"""
                SELECT `{store_col}` AS shop, `spu` AS spu, MIN(`{dcol}`) AS first_dt,
                       SUM(COALESCE(`{sales_col}`,0)) AS qty
                FROM {table}
                WHERE `{dcol}` BETWEEN %s AND %s
                  AND `{store_col}`=%s
                  AND `spu` IN ({placeholders})
                  AND COALESCE(`{sales_col}`,0) > 0
                  {delete_filter}
                GROUP BY `{store_col}`, `spu`
            """
            params: List[Any] = [start, as_of, shop, *part]
            with db_cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
            for r in rows:
                s = text(r.get("shop")); p = text(r.get("spu")); d = r.get("first_dt")
                if s and p and d:
                    if hasattr(d, "date") and not isinstance(d, date):
                        d = d.date()
                    result[(s, p)] = d
    return result


def main() -> int:
    print("=" * 88)
    print("动态监控 UNKNOWN 首销根因诊断（只读）")
    print("=" * 88)

    v4.install_patch()
    features, source = base.build_feature_rows(date.today())
    unknown_rows = [
        r for r in features
        if text(r.get("store_name")) in TARGET_SHOPS
        and text(r.get("forecastability")) == "UNKNOWN"
    ]
    unknown_keys = sorted({(text(r.get("store_name")), text(r.get("spu"))) for r in unknown_rows})
    print(f"UNKNOWN shop+SPU: {len(unknown_keys)}")

    monthly = load_monthly_rows()
    exact_positive: Set[Tuple[str, str]] = set()
    prefix_positive: Set[Tuple[str, str]] = set()
    spu_any_shop: Dict[str, Set[str]] = defaultdict(set)

    for r in monthly:
        shop = text(r.get("店铺")); spu = text(r.get("SPU")); sku = text(r.get("SKU"))
        if spu:
            exact_positive.add((shop, spu))
            spu_any_shop[spu].add(shop)
        prefix = sku.split("-", 1)[0] if sku else ""
        if prefix:
            prefix_positive.add((shop, prefix))

    daily_recent = recent_daily_positive(source, unknown_keys)

    summary: Dict[str, Counter] = {shop: Counter() for shop in TARGET_SHOPS}
    samples: Dict[str, List[Dict[str, Any]]] = {shop: [] for shop in TARGET_SHOPS}

    for shop, spu in unknown_keys:
        c = summary[shop]
        c["UNKNOWN_TOTAL"] += 1
        if (shop, spu) in exact_positive:
            c["MONTHLY_EXACT_POSITIVE"] += 1
        else:
            c["MONTHLY_NO_EXACT"] += 1

        if (shop, spu) in prefix_positive:
            c["MONTHLY_SKU_PREFIX_MATCH"] += 1

        shops_with_spu = spu_any_shop.get(spu, set())
        if not shops_with_spu:
            c["MONTHLY_SPU_ABSENT_ALL_TARGET_SHOPS"] += 1
        elif shops_with_spu - {shop}:
            c["SAME_SPU_OTHER_SHOP"] += 1

        if (shop, spu) in daily_recent:
            c["DAILY_RECENT_POSITIVE_100D"] += 1
        else:
            c["DAILY_NO_POSITIVE_100D"] += 1

        if len(samples[shop]) < 15:
            samples[shop].append({
                "SPU": spu,
                "monthly_exact": (shop, spu) in exact_positive,
                "monthly_sku_prefix": (shop, spu) in prefix_positive,
                "monthly_spu_present_shops": sorted(shops_with_spu),
                "other_shops": sorted(shops_with_spu - {shop}),
                "daily_recent_first": str(daily_recent.get((shop, spu)) or ""),
            })

    print("\n=== 按店铺根因计数 ===")
    for shop in TARGET_SHOPS:
        print(shop, json.dumps(dict(summary[shop]), ensure_ascii=False))

    print("\n=== UNKNOWN样本（每店最多15个） ===")
    for shop in TARGET_SHOPS:
        print(f"\n[{shop}]")
        for r in samples[shop]:
            print(json.dumps(r, ensure_ascii=False))

    print("\n=== 判读规则 ===")
    print("MONTHLY_NO_EXACT 高：销量统计_msku月度 对该店首销覆盖不足。")
    print("MONTHLY_SPU_ABSENT_ALL_TARGET_SHOPS 高：月销量表四个目标店中完全没有该SPU正销量记录。")
    print("MONTHLY_SKU_PREFIX_MATCH 高但 exact 低：月销量表 SPU 字段/解析口径有问题。")
    print("DAILY_RECENT_POSITIVE_100D 高：日产品表现表可补充 NEW_VISIBLE 最近首销识别。")
    print("SAME_SPU_OTHER_SHOP 高：必须坚持 店铺×SPU，不可用全局SPU首销。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
