#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of NEW_VISIBLE launch-date disagreements and cohort concentration.

Compares exact first positive day from forecast_research_spu_daily_history against the
monthly sales table's first positive month. Also checks whether unusual cohort spikes are
broad-based across shops or concentrated in a narrow source pattern.

No table is created or modified.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS
from jobs.forecast_research.build_spu_daily_history import DEST_TABLE as DAILY_TABLE


def to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def month_floor(d: date) -> date:
    return date(d.year, d.month, 1)


def month_diff(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def main() -> int:
    if not base.table_exists(DAILY_TABLE):
        raise RuntimeError(f"{DAILY_TABLE} 不存在")
    if not base.table_exists(base.MONTHLY_SALES_TABLE):
        raise RuntimeError(f"{base.MONTHLY_SALES_TABLE} 不存在")

    cols = set(base.get_columns(base.MONTHLY_SALES_TABLE))
    required = {"店铺", "SPU", "统计日期", "销量"}
    if not required.issubset(cols):
        raise RuntimeError(f"月销量表缺字段: {sorted(required-cols)}")

    daily = q(
        f"""
        SELECT store_name, spu, MIN(dt) AS first_sale_day
        FROM `{DAILY_TABLE}`
        WHERE sales_units > 0
        GROUP BY store_name, spu
        """
    )
    monthly = q(
        f"""
        SELECT `店铺` AS store_name, `SPU` AS spu,
               MIN(`统计日期`) AS first_sale_month
        FROM `{base.MONTHLY_SALES_TABLE}`
        WHERE `店铺` IN (%s,%s,%s,%s)
          AND `SPU` IS NOT NULL AND TRIM(`SPU`)<>''
          AND COALESCE(`销量`,0)>0
        GROUP BY `店铺`,`SPU`
        """,
        TARGET_SHOPS,
    )

    dmap = {(str(r['store_name']), str(r['spu'])): to_date(r['first_sale_day']) for r in daily}
    mmap = {(str(r['store_name']), str(r['spu'])): to_date(r['first_sale_month']) for r in monthly}

    aligned = []
    mismatch = []
    missing_monthly = []
    for key, dd in dmap.items():
        md = mmap.get(key)
        if md is None:
            missing_monthly.append((key, dd))
            continue
        mdelta = month_diff(month_floor(md), month_floor(dd))
        day_delta = (dd - md).days
        rec = {
            "shop": key[0],
            "spu": key[1],
            "daily_first_day": dd,
            "monthly_first_month": month_floor(md),
            "month_delta_daily_minus_monthly": mdelta,
            "day_delta_daily_minus_monthly_date": day_delta,
        }
        if mdelta == 0:
            aligned.append(rec)
        else:
            mismatch.append(rec)

    monthly_only = [(k, d) for k, d in mmap.items() if k not in dmap]

    print("=" * 100)
    print("NEW_VISIBLE 首销冲突与cohort集中度审计（只读）")
    print("=" * 100)

    by_dir = Counter()
    by_shop = Counter()
    by_shop_dir = Counter()
    by_abs_month_delta = Counter()
    for r in mismatch:
        delta = int(r["month_delta_daily_minus_monthly"])
        direction = "DAILY_LATER" if delta > 0 else "DAILY_EARLIER"
        by_dir[direction] += 1
        by_shop[r["shop"]] += 1
        by_shop_dir[(r["shop"], direction)] += 1
        by_abs_month_delta[abs(delta)] += 1

    print("\n=== 1. 首销来源一致性 ===")
    print(json.dumps({
        "daily_positive_keys": len(dmap),
        "monthly_positive_keys": len(mmap),
        "same_calendar_month": len(aligned),
        "different_calendar_month": len(mismatch),
        "daily_key_missing_monthly": len(missing_monthly),
        "monthly_key_missing_daily": len(monthly_only),
        "mismatch_rate_among_matched": round(len(mismatch) / (len(aligned)+len(mismatch)), 4)
            if (aligned or mismatch) else None,
        "direction": dict(by_dir),
        "by_shop": dict(by_shop),
        "abs_month_delta_distribution": {str(k): v for k, v in sorted(by_abs_month_delta.items())},
    }, ensure_ascii=False, default=str))

    print("\n=== 2. 首销冲突：按店铺×方向 ===")
    for (shop, direction), n in sorted(by_shop_dir.items()):
        print(json.dumps({"shop": shop, "direction": direction, "n": n}, ensure_ascii=False))

    # Pull candidate-month signal from both sources for mismatches.
    details: List[Dict[str, Any]] = []
    for r in mismatch:
        shop, spu = r["shop"], r["spu"]
        dd = r["daily_first_day"]
        md = r["monthly_first_month"]
        daily_month = month_floor(dd)
        monthly_month = month_floor(md)
        month_labels = (monthly_month.strftime('%Y-%m'), daily_month.strftime('%Y-%m'))

        daily_month_rows = q(
            f"""
            SELECT DATE_FORMAT(dt,'%%Y-%%m') AS ym,
                   SUM(sales_units) AS sales,
                   SUM(sessions) AS sessions,
                   MIN(CASE WHEN sales_units>0 THEN dt END) AS first_positive_day
            FROM `{DAILY_TABLE}`
            WHERE store_name=%s AND spu=%s
              AND DATE_FORMAT(dt,'%%Y-%%m') IN (%s,%s)
            GROUP BY DATE_FORMAT(dt,'%%Y-%%m')
            """,
            (shop, spu, *month_labels),
        )
        mvals = q(
            f"""
            SELECT DATE_FORMAT(`统计日期`,'%%Y-%%m') AS ym,
                   SUM(COALESCE(`销量`,0)) AS sales
            FROM `{base.MONTHLY_SALES_TABLE}`
            WHERE `店铺`=%s AND `SPU`=%s
              AND DATE_FORMAT(`统计日期`,'%%Y-%%m') IN (%s,%s)
            GROUP BY DATE_FORMAT(`统计日期`,'%%Y-%%m')
            """,
            (shop, spu, *month_labels),
        )
        daily_by_ym = {str(x['ym']): x for x in daily_month_rows}
        monthly_by_ym = {str(x['ym']): x for x in mvals}
        rec = dict(r)
        rec.update({
            "monthly_first_month_daily_sales": float(daily_by_ym.get(month_labels[0], {}).get('sales',0) or 0),
            "monthly_first_month_daily_sessions": float(daily_by_ym.get(month_labels[0], {}).get('sessions',0) or 0),
            "monthly_first_month_monthly_sales": float(monthly_by_ym.get(month_labels[0], {}).get('sales',0) or 0),
            "daily_first_month_daily_sales": float(daily_by_ym.get(month_labels[1], {}).get('sales',0) or 0),
            "daily_first_month_monthly_sales": float(monthly_by_ym.get(month_labels[1], {}).get('sales',0) or 0),
        })
        details.append(rec)

    print("\n=== 3. 冲突样本Top60（按绝对月差） ===")
    details_sorted = sorted(
        details,
        key=lambda r: (abs(int(r['month_delta_daily_minus_monthly'])), r['shop'], r['spu']),
        reverse=True,
    )
    for r in details_sorted[:60]:
        x = dict(r)
        x['daily_first_day'] = str(x['daily_first_day'])
        x['monthly_first_month'] = str(x['monthly_first_month'])
        print(json.dumps(x, ensure_ascii=False))

    print("\n=== 4. Daily首销cohort按月×店铺 ===")
    cohort = Counter()
    for (shop, _spu), dd in dmap.items():
        cohort[(dd.strftime('%Y-%m'), shop)] += 1
    months = sorted({m for m, _s in cohort})
    for m in months:
        vals = {shop: cohort.get((m, shop), 0) for shop in TARGET_SHOPS}
        total = sum(vals.values())
        if total >= 15 or m >= '2025-01':
            print(json.dumps({"month": m, "total": total, **vals}, ensure_ascii=False))

    print("\n=== 5. 2025-05~08高峰新品是否跨店重复 ===")
    spike_keys = []
    for key, dd in dmap.items():
        if date(2025,5,1) <= dd <= date(2025,8,31):
            spike_keys.append((key, dd))
    spu_shops: Dict[str, set] = defaultdict(set)
    for (shop, spu), _dd in spike_keys:
        spu_shops[spu].add(shop)
    shop_count_dist = Counter(len(v) for v in spu_shops.values())
    print(json.dumps({
        "launch_shop_spu_rows": len(spike_keys),
        "distinct_spu": len(spu_shops),
        "number_of_shops_per_spu": {str(k): v for k, v in sorted(shop_count_dist.items())},
        "multi_shop_spu_n": sum(1 for v in spu_shops.values() if len(v)>1),
    }, ensure_ascii=False))

    print("\n=== 6. 判读原则 ===")
    print("DAILY_LATER: 月表声称更早已有销量；若日表对应月销量=0，应优先隔离该样本，不能把日表首销当真实launch。")
    print("DAILY_EARLIER: 日表声称更早已有销量；需检查月表是否漏历史月份。")
    print("若2025-05~08新品高峰主要由同一批SPU跨店同步首次销售构成，它可能是渠道扩店/迁移，而非独立产品创新cohort。")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
