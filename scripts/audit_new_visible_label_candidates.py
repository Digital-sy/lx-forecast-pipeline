#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of candidate Stage-1 labels for NEW_VISIBLE breakout modeling.

Why fixed ages
--------------
The snapshot table contains up to 121 highly-correlated daily rows per launch.  This
script evaluates label balance at fixed ages (7/14/30/60/90) so each launch contributes
at most one row to each comparison.  It also exposes the early-age denominator problem:
`future/past30` is mechanically inflated when a product has been live for <30 days.

No table is modified and no model is trained.
"""
from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as v1
from jobs.forecast_research import build_new_visible_snapshots_v2 as v2
from scripts.backtest_breakout_v0_history import score_v0

AGES = (7, 14, 30, 60, 90)


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def num(v: Any) -> float:
    return 0.0 if v is None else float(v)


def rate(n: int, d: int):
    return round(n / d, 4) if d else None


def percentile(vals: Sequence[float], p: float):
    if not vals:
        return None
    x = sorted(vals)
    if len(x) == 1:
        return x[0]
    pos = (len(x) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(x) - 1)
    w = pos - lo
    return x[lo] * (1 - w) + x[hi] * w


def persistent(r: Mapping[str, Any]) -> bool:
    a = num(r.get("future_sales_first14"))
    b = num(r.get("future_sales_second14"))
    return a > 0 and b >= 0.8 * a


def ratio30(r: Mapping[str, Any]):
    past = num(r.get("sales_30d"))
    return (num(r.get("future_sales_30d")) / past) if past > 0 else None


def label_flags(r: Mapping[str, Any]) -> Dict[str, bool]:
    f30 = num(r.get("future_sales_30d"))
    rat = ratio30(r)
    per = persistent(r)
    return {
        "ABS_500": f30 >= 500,
        "ABS_750": f30 >= 750,
        "ABS_1000": f30 >= 1000,
        "ABS_1500": f30 >= 1500,
        "PERSIST_750": f30 >= 750 and per,
        "PERSIST_1000": f30 >= 1000 and per,
        "BIG_2X_750": rat is not None and rat >= 2.0 and f30 >= 750,
        "BIG_2X_1000": rat is not None and rat >= 2.0 and f30 >= 1000,
        # Candidate that keeps very large outcomes even if the 14+14 persistence
        # split happens to be noisy, while demanding persistence for 1000-1499.
        "BALANCED_1000": f30 >= 1500 or (f30 >= 1000 and per),
    }


def temporal_block(first_sale_day: Any) -> str:
    s = str(first_sale_day)[:10]
    y = int(s[:4])
    m = int(s[5:7])
    return f"{y}H{1 if m <= 6 else 2}"


def summarize(rows: Sequence[Mapping[str, Any]], label: str) -> Dict[str, Any]:
    n = len(rows)
    f30 = [num(r.get("future_sales_30d")) for r in rows]
    past30 = [num(r.get("sales_30d")) for r in rows]
    ratios = [ratio30(r) for r in rows]
    ratios = [x for x in ratios if x is not None]

    scored = []
    for r in rows:
        s = score_v0(r)
        scored.append(s["risk"] == "HIGH")
    v0_high_n = sum(scored)

    out: Dict[str, Any] = {
        "group": label,
        "rows": n,
        "launches": len({(str(r.get("store_name")), str(r.get("spu"))) for r in rows}),
        "future30_median": round(statistics.median(f30), 2) if f30 else None,
        "future30_p75": round(percentile(f30, 0.75), 2) if f30 else None,
        "past30_median": round(statistics.median(past30), 2) if past30 else None,
        "ratio30_median": round(statistics.median(ratios), 3) if ratios else None,
        "v0_high_rate": rate(v0_high_n, n),
    }

    flags = [label_flags(r) for r in rows]
    for name in label_flags({}).keys():
        pos = sum(int(f[name]) for f in flags)
        tp = sum(int(f[name] and hi) for f, hi in zip(flags, scored))
        out[name + "_n"] = pos
        out[name + "_rate"] = rate(pos, n)
        out[name + "_v0_precision"] = rate(tp, v0_high_n)
        out[name + "_v0_recall"] = rate(tp, pos)
    return out


def main() -> int:
    if not v1.base.table_exists(v1.SNAPSHOT_TABLE):
        raise RuntimeError(f"{v1.SNAPSHOT_TABLE} 不存在")

    rows = q(
        f"""
        SELECT snapshot_date, store_name, spu, first_sale_day, age_days,
               sales_7d, sales_prev_7d, sales_30d,
               sessions_7d, sessions_prev_7d,
               cvr_7d, cvr_prev_7d,
               sales_growth_7d, sessions_growth_7d, cvr_ratio_7d,
               future_sales_30d, future_sales_first14, future_sales_second14,
               dataset_version
        FROM `{v1.SNAPSHOT_TABLE}`
        WHERE dataset_version=%s
          AND age_days IN ({','.join(['%s'] * len(AGES))})
        ORDER BY first_sale_day, store_name, spu, age_days
        """,
        (v2.DATASET_VERSION, *AGES),
    )
    if not rows:
        raise RuntimeError("当前dataset_version没有固定年龄snapshot")

    launch_keys = {(str(r["store_name"]), str(r["spu"])) for r in rows}
    print("LABEL_AUDIT_SCOPE=" + json.dumps({
        "dataset_version": v2.DATASET_VERSION,
        "ages": list(AGES),
        "rows": len(rows),
        "launches": len(launch_keys),
        "warning": "ratio-based labels before age30 are diagnostic only because pre-launch zero days inflate future/past30",
    }, ensure_ascii=False))

    print("\n=== FIXED_AGE_LABEL_MATRIX ===")
    for age in AGES:
        group = [r for r in rows if int(r.get("age_days") or -1) == age]
        print(json.dumps(summarize(group, f"AGE_{age}"), ensure_ascii=False))

    print("\n=== STORE_AT_AGE14 ===")
    age14 = [r for r in rows if int(r.get("age_days") or -1) == 14]
    by_shop = defaultdict(list)
    for r in age14:
        by_shop[str(r.get("store_name"))].append(r)
    for shop in sorted(by_shop):
        print(json.dumps(summarize(by_shop[shop], shop), ensure_ascii=False))

    print("\n=== STORE_AT_AGE30 ===")
    age30 = [r for r in rows if int(r.get("age_days") or -1) == 30]
    by_shop = defaultdict(list)
    for r in age30:
        by_shop[str(r.get("store_name"))].append(r)
    for shop in sorted(by_shop):
        print(json.dumps(summarize(by_shop[shop], shop), ensure_ascii=False))

    print("\n=== TEMPORAL_BLOCK_AT_AGE14 ===")
    by_block = defaultdict(list)
    for r in age14:
        by_block[temporal_block(r.get("first_sale_day"))].append(r)
    for block in sorted(by_block):
        print(json.dumps(summarize(by_block[block], block), ensure_ascii=False))

    print("\n=== TEMPORAL_BLOCK_AT_AGE30 ===")
    by_block = defaultdict(list)
    for r in age30:
        by_block[temporal_block(r.get("first_sale_day"))].append(r)
    for block in sorted(by_block):
        print(json.dumps(summarize(by_block[block], block), ensure_ascii=False))

    print("\n=== LABEL_GUIDANCE ===")
    print("1. AGE_7/14 的 2X 标签只用于诊断，不建议作为最终正类，因为past30含大量上架前0天。")
    print("2. 先比较 ABS_750/1000 与 PERSIST_750/1000 的跨店铺、跨半年度正类数量。")
    print("3. 若某店/时间块正类过少，不做分店模型；使用pooled global model + store/age/month特征。")
    print("4. Stage-1标签确定后，再做严格按launch分组的temporal OOS；禁止随机拆snapshot行。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
