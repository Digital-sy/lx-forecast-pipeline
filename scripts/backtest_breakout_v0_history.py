#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Historical backtest of the live Breakout RULE V0 on strict NEW_VISIBLE snapshots.

Read-only. Does not define the final ML label.

Because true historical FBA daily inventory only starts on 2026-09-30, this backtest
replays the V0 rule WITHOUT inventory points. It evaluates whether sales/traffic/CVR
signals separate future outcomes, and reports several raw future definitions rather than
hard-coding one final breakout label prematurely.
"""
from __future__ import annotations

import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_research.build_new_visible_snapshots import SNAPSHOT_TABLE

MONITOR_VERSION = "RULE_V0_HISTORY_NO_INVENTORY"


def text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def num(v: Any) -> float:
    if v is None:
        return 0.0
    return float(v)


def maybe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    x = float(v)
    return x if math.isfinite(x) else None


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def score_v0(r: Mapping[str, Any]) -> Dict[str, Any]:
    sg = maybe_float(r.get("sales_growth_7d"))
    tg = maybe_float(r.get("sessions_growth_7d"))
    cg = maybe_float(r.get("cvr_ratio_7d"))
    sales7 = num(r.get("sales_7d"))
    score = 0.0
    reasons: List[str] = []

    if sg is not None and sg >= 2.0:
        score += 30; reasons.append("SALES_SURGE_2X")
    elif sg is not None and sg >= 1.2:
        score += 15; reasons.append("SALES_RISING")

    if tg is not None and tg >= 2.0:
        score += 30; reasons.append("TRAFFIC_SURGE_2X")
    elif tg is not None and tg >= 1.2:
        score += 15; reasons.append("TRAFFIC_RISING")

    if cg is not None and cg >= 1.10:
        score += 15; reasons.append("CVR_IMPROVING")
    elif cg is not None and cg >= 0.85:
        score += 8; reasons.append("CVR_HOLDING")
    elif cg is not None and cg < 0.70:
        score -= 10; reasons.append("CVR_WEAKENING")

    if sales7 >= 1000:
        score += 15; reasons.append("HIGH_7D_VOLUME")
    elif sales7 >= 300:
        score += 8; reasons.append("MEDIUM_7D_VOLUME")

    # Deliberately no inventory points in historical backtest.
    score = max(0.0, min(100.0, score))
    risk = "HIGH" if score >= 70 else ("MEDIUM" if score >= 45 else "LOW")
    return {
        "score": score,
        "risk": risk,
        "reason": ",".join(reasons) if reasons else "NO_STRONG_SIGNAL",
    }


def percentile(vals: Sequence[float], p: float) -> Optional[float]:
    if not vals:
        return None
    x = sorted(vals)
    if len(x) == 1:
        return x[0]
    pos = (len(x) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return x[lo]
    w = pos - lo
    return x[lo] * (1 - w) + x[hi] * w


def rate(n: int, d: int) -> Optional[float]:
    return round(n / d, 4) if d else None


def future_flags(r: Mapping[str, Any]) -> Dict[str, bool]:
    past30 = num(r.get("sales_30d"))
    future30 = num(r.get("future_sales_30d"))
    f14a = num(r.get("future_sales_first14"))
    f14b = num(r.get("future_sales_second14"))
    ratio30 = future30 / past30 if past30 > 0 else None

    return {
        "future_ge_500": future30 >= 500,
        "future_ge_1000": future30 >= 1000,
        "future_2x": ratio30 is not None and ratio30 >= 2.0,
        "future_3x": ratio30 is not None and ratio30 >= 3.0,
        "big_2x_1000": ratio30 is not None and ratio30 >= 2.0 and future30 >= 1000,
        "persistent_14x2": f14a > 0 and f14b >= 0.8 * f14a,
        "temporary_spike": f14a > 0 and f14b < 0.5 * f14a,
    }


def summarize(rows: Sequence[Mapping[str, Any]], label: str) -> Dict[str, Any]:
    ratios = [
        num(r.get("future_sales_30d")) / num(r.get("sales_30d"))
        for r in rows if num(r.get("sales_30d")) > 0
    ]
    future30 = [num(r.get("future_sales_30d")) for r in rows]
    age = [num(r.get("age_days")) for r in rows]
    flags = [future_flags(r) for r in rows]
    out: Dict[str, Any] = {
        "group": label,
        "rows": len(rows),
        "launches": len({(text(r.get("store_name")), text(r.get("spu"))) for r in rows}),
        "median_age_days": round(statistics.median(age), 1) if age else None,
        "future30_median": round(statistics.median(future30), 2) if future30 else None,
        "future30_p75": round(percentile(future30, 0.75), 2) if future30 else None,
        "future30_p90": round(percentile(future30, 0.90), 2) if future30 else None,
        "future_to_past30_ratio_median": round(statistics.median(ratios), 3) if ratios else None,
        "future_to_past30_ratio_p75": round(percentile(ratios, 0.75), 3) if ratios else None,
    }
    for k in (
        "future_ge_500","future_ge_1000","future_2x","future_3x",
        "big_2x_1000","persistent_14x2","temporary_spike",
    ):
        n = sum(1 for f in flags if f[k])
        out[k + "_rate"] = rate(n, len(flags))
    return out


def main() -> int:
    if not base.table_exists(SNAPSHOT_TABLE):
        raise RuntimeError(
            f"{SNAPSHOT_TABLE} 不存在；先构建历史NEW_VISIBLE snapshot"
        )

    rows = q(
        f"""
        SELECT *
        FROM `{SNAPSHOT_TABLE}`
        ORDER BY store_name, spu, snapshot_date
        """
    )
    if not rows:
        raise RuntimeError(f"{SNAPSHOT_TABLE} 为空")

    enriched: List[Dict[str, Any]] = []
    by_launch: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        r = dict(raw)
        s = score_v0(r)
        r["v0_score"] = s["score"]
        r["v0_risk"] = s["risk"]
        r["v0_reason"] = s["reason"]
        enriched.append(r)
        by_launch[(text(r.get("store_name")), text(r.get("spu")))].append(r)

    print("=" * 100)
    print("Breakout RULE V0 历史回测（严格历史snapshot；无伪历史库存）")
    print("=" * 100)
    print(
        "BACKTEST_SCOPE="
        + json.dumps(
            {
                "snapshot_rows": len(enriched),
                "launches": len(by_launch),
                "monitor_version": MONITOR_VERSION,
                "inventory_points_replayed": False,
                "boundary": "raw future-outcome diagnostics; final breakout label not fixed yet",
            },
            ensure_ascii=False,
        )
    )

    print("\n=== 1. 按V0风险层级看未来结果（snapshot级） ===")
    for risk in ("HIGH","MEDIUM","LOW"):
        group = [r for r in enriched if r["v0_risk"] == risk]
        print(json.dumps(summarize(group, risk), ensure_ascii=False))

    # One first-HIGH event per launch: closer to how operations would consume alerts.
    first_high = []
    for key, arr in by_launch.items():
        arr = sorted(arr, key=lambda x: x["snapshot_date"])
        hs = [r for r in arr if r["v0_risk"] == "HIGH"]
        if hs:
            first_high.append(hs[0])

    print("\n=== 2. 每个新品首次HIGH后的真实结果（event级） ===")
    print(json.dumps(summarize(first_high, "FIRST_HIGH_EVENT"), ensure_ascii=False))

    # For every launch, take its maximum-score historical snapshot; this lets us compare
    # launches that never reached HIGH without selecting many correlated daily rows.
    best_event = []
    for key, arr in by_launch.items():
        best = sorted(
            arr,
            key=lambda x: (
                -float(x["v0_score"]),
                x["snapshot_date"],
            ),
        )[0]
        best_event.append(best)

    bands = {
        "MAX_HIGH": [r for r in best_event if r["v0_score"] >= 70],
        "MAX_MEDIUM": [r for r in best_event if 45 <= r["v0_score"] < 70],
        "MAX_LOW": [r for r in best_event if r["v0_score"] < 45],
    }
    print("\n=== 3. 每个新品历史最高V0分层（launch级） ===")
    for name, group in bands.items():
        print(json.dumps(summarize(group, name), ensure_ascii=False))

    print("\n=== 4. 首次HIGH按店铺 ===")
    by_shop = defaultdict(list)
    for r in first_high:
        by_shop[text(r.get("store_name"))].append(r)
    for shop in sorted(by_shop):
        print(json.dumps(summarize(by_shop[shop], shop), ensure_ascii=False))

    print("\n=== 5. 首次HIGH按首销年龄段 ===")
    age_bands = [
        ("AGE_0_14", 0, 14),
        ("AGE_15_30", 15, 30),
        ("AGE_31_60", 31, 60),
        ("AGE_61_90", 61, 90),
        ("AGE_91_120", 91, 120),
    ]
    for name, lo, hi in age_bands:
        group = [r for r in first_high if lo <= int(r.get("age_days") or 0) <= hi]
        print(json.dumps(summarize(group, name), ensure_ascii=False))

    print("\n=== 6. 首次HIGH样本Top（按未来30天销量） ===")
    top = sorted(first_high, key=lambda r: num(r.get("future_sales_30d")), reverse=True)[:30]
    for r in top:
        flags = future_flags(r)
        print(
            json.dumps(
                {
                    "snapshot_date": str(r.get("snapshot_date")),
                    "store": r.get("store_name"),
                    "spu": r.get("spu"),
                    "age_days": int(r.get("age_days") or 0),
                    "score": float(r["v0_score"]),
                    "reason": r["v0_reason"],
                    "sales_7d": num(r.get("sales_7d")),
                    "sales_30d": num(r.get("sales_30d")),
                    "future_sales_30d": num(r.get("future_sales_30d")),
                    "future_ratio_30": (
                        round(num(r.get("future_sales_30d")) / num(r.get("sales_30d")), 3)
                        if num(r.get("sales_30d")) > 0 else None
                    ),
                    **flags,
                },
                ensure_ascii=False,
            )
        )

    print("\n=== 7. 判读原则 ===")
    print("1. 不用历史当前库存伪回放，因此这是 RULE_V0_NO_INVENTORY baseline。")
    print("2. 先看HIGH/MEDIUM/LOW未来分布是否单调分离，再决定是否值得训练V1。")
    print("3. final label 暂不写死；重点观察 big_2x_1000、persistent_14x2、temporary_spike 的真实比例。")
    print("4. 如果HIGH里temporary_spike仍高，V1优先学习持续性；如果LOW里big_2x_1000高，则优先提升召回。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
