#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-15: repair the A3 baseline level/trajectory, read-only.

Round-14 showed that most H2/H3 T-shirt over-forecast already exists in A3, while
A7 usually reduces it. This experiment therefore changes NO seasonal logic. It tests
simple snapshot-safe baseline challengers against A3:

A19 spike re-anchor
    If the latest complete month M1 is >= 2x both M2 and M3, treat M1 as a possible
    one-month spike and cap the baseline at median(M1,M2,M3).

A20 strict decline extrapolation
    If M3 > M2 > M1 and M1 <= 85% of M3, estimate the monthly decay ratio as
    sqrt(M1/M3), clip it to [0.45, 0.95], and extrapolate from M1 to target.

A21 strict combined
    Apply spike re-anchor first, otherwise strict decline extrapolation.

A22 broad weak-momentum combined
    A21 plus a conservative fallback when M1 / mean(M2,M3) <= 0.80 but the strict
    monotonic decline test did not fire. This is intentionally a separate challenger.

A23 half-correction
    Move only halfway from A3 toward A22, to test whether the full correction is too
    aggressive.

All rules use only history visible before snapshot. No production tables/code are
modified. No Amazon future information is used.
"""
from __future__ import annotations

import argparse
import math
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5
from scripts import audit_sales_forecast_v5_round6_segments as r6
from scripts import audit_sales_forecast_v5_round7_router as r7
from scripts import audit_sales_forecast_v5_round9_category_diagnostics as r9
from scripts import audit_sales_forecast_v5_round10_current_momentum_veto as r10

A0 = "A0_上月延续"
A3 = "A3_SPU生命周期收缩"
A19 = "A19_最新月尖峰重锚"
A20 = "A20_严格衰退外推"
A21 = "A21_尖峰加严格衰退"
A22 = "A22_再加弱动量"
A23 = "A23_弱动量半修正"

MODELS = [(A0, A0), (A3, A3), (A19, A19), (A20, A20), (A21, A21), (A22, A22), (A23, A23)]


def parse_month(v: Any) -> date:
    if isinstance(v, datetime):
        return date(v.year, v.month, 1)
    if isinstance(v, date):
        return date(v.year, v.month, 1)
    return datetime.strptime(str(v)[:7], "%Y-%m").date().replace(day=1)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def hist(actual: Dict[Tuple[str, date], int], spu: str, d: date) -> float:
    return float(actual.get((spu, d), 0) or 0)


def recent3(actual: Dict[Tuple[str, date], int], row: Dict[str, Any]) -> Tuple[float, float, float]:
    snapshot = parse_month(row.get("快照月"))
    spu = str(row.get("SPU") or "")
    return tuple(hist(actual, spu, base.add_months(snapshot, -i)) for i in (1, 2, 3))  # type: ignore


def horizon_num(row: Dict[str, Any]) -> int:
    s = str(row.get("Horizon") or "H0")
    try:
        return int(s[1:])
    except Exception:
        return 0


def spike_candidate(a3: float, m1: float, m2: float, m3: float) -> Tuple[float, bool]:
    # Require all three months to be observed so cold-start/relaunch is not mistaken for a spike.
    fire = (
        m1 >= 50
        and m2 > 0
        and m3 > 0
        and m1 >= 2.0 * max(m2, m3)
    )
    if not fire:
        return a3, False
    robust = float(statistics.median([m1, m2, m3]))
    return min(a3, robust), True


def strict_decline_candidate(a3: float, h: int, m1: float, m2: float, m3: float) -> Tuple[float, bool, float | None]:
    fire = m1 > 0 and m2 > 0 and m3 > 0 and m1 < m2 < m3 and m1 <= 0.85 * m3
    if not fire:
        return a3, False, None
    # Geometric average decay across M3->M2->M1. Project from latest visible complete month
    # M1 to target: H2 means three calendar steps from M1 to target, H3 means four.
    ratio = math.sqrt(m1 / m3)
    ratio = clamp(ratio, 0.45, 0.95)
    steps = max(1, h + 1)
    projected = m1 * (ratio ** steps)
    return min(a3, projected), True, ratio


def weak_momentum_candidate(a3: float, h: int, m1: float, m2: float, m3: float) -> Tuple[float, bool, float | None]:
    if m1 <= 0 or m2 <= 0 or m3 <= 0:
        return a3, False, None
    denom = (m2 + m3) / 2.0
    if denom <= 0:
        return a3, False, None
    momentum = m1 / denom
    if momentum > 0.80:
        return a3, False, momentum
    # Mild fallback: square-root dampening prevents a noisy single weak month from being
    # extrapolated as aggressively as the strict monotonic-decline rule.
    decay = clamp(math.sqrt(max(0.0, momentum)), 0.70, 0.95)
    steps = max(1, h)
    projected = m1 * (decay ** steps)
    return min(a3, projected), True, momentum


def add_candidates(actual: Dict[Tuple[str, date], int], rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r.get(A3, 0) or 0)
        m1, m2, m3 = recent3(actual, r)
        h = horizon_num(r)

        p_spike, spike_fire = spike_candidate(a3, m1, m2, m3)
        p_decline, decline_fire, decline_ratio = strict_decline_candidate(a3, h, m1, m2, m3)

        # A19 / A20 isolate each mechanism.
        x[A19] = int(round(max(0.0, p_spike)))
        x[A20] = int(round(max(0.0, p_decline)))

        # A21: spike takes priority because a one-month spike makes local trend extrapolation unsafe.
        if spike_fire:
            strict_combined = p_spike
            strict_rule = "SPIKE_REANCHOR"
        elif decline_fire:
            strict_combined = p_decline
            strict_rule = "STRICT_DECLINE"
        else:
            strict_combined = a3
            strict_rule = "KEEP_A3"
        x[A21] = int(round(max(0.0, strict_combined)))

        # A22: only if A21 did nothing, test a broader weak-momentum fallback.
        weak_pred, weak_fire, momentum = weak_momentum_candidate(a3, h, m1, m2, m3)
        if strict_rule == "KEEP_A3" and weak_fire:
            broad = weak_pred
            broad_rule = "WEAK_MOMENTUM"
        else:
            broad = strict_combined
            broad_rule = strict_rule
        x[A22] = int(round(max(0.0, broad)))
        x[A23] = int(round(max(0.0, a3 + 0.5 * (broad - a3))))

        x["基线修正规则"] = broad_rule
        x["快照M1"] = int(round(m1))
        x["快照M2"] = int(round(m2))
        x["快照M3"] = int(round(m3))
        x["M1_均值M2M3"] = None if (m2 + m3) <= 0 else round(m1 / ((m2 + m3) / 2.0), 6)
        x["严格衰退月比"] = None if decline_ratio is None else round(decline_ratio, 6)
        x["弱动量比"] = None if momentum is None else round(momentum, 6)
        out.append(x)
    return out


def build_oos(actual, targets, max_horizon: int) -> List[Dict[str, Any]]:
    detail = r2.build_detail(actual, targets, max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r10.walk_forward_oos(actual, detail, targets, max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    rows = [r for r in oos if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")]
    rows = r9.enrich_category(rows, r9.load_spu_category_map())
    return rows


def ranges(rows: Sequence[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    return [
        ("全部ESTABLISHED", list(rows)),
        ("ABC_A", [r for r in rows if str(r.get("ABC") or "") == "A"]),
        ("T恤", [r for r in rows if str(r.get("品类") or "") == "T恤"]),
        ("T恤_ABC_A", [r for r in rows if str(r.get("品类") or "") == "T恤" and str(r.get("ABC") or "") == "A"]),
    ]


def summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for range_name, rr in ranges(rows):
        for hs in ("H2", "H3"):
            seg = [r for r in rr if str(r.get("Horizon")) == hs]
            for name, col in MODELS:
                out.append({"范围": range_name, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for range_name, rr in [("全部ESTABLISHED", list(rows)), ("T恤", [r for r in rows if str(r.get("品类") or "") == "T恤"])]:
        for hs in ("H2", "H3"):
            hr = [r for r in rr if str(r.get("Horizon")) == hs]
            for month in sorted({str(r.get("目标月")) for r in hr}):
                mr = [r for r in hr if str(r.get("目标月")) == month]
                for name, col in [(A3, A3), (A21, A21), (A22, A22), (A23, A23)]:
                    out.append({"范围": range_name, "Horizon": hs, "目标月": month, "模型": name, **base.metric(mr, col)})
    return out


def rule_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for rule in sorted({str(r.get("基线修正规则") or "UNKNOWN") for r in hr}):
            seg = [r for r in hr if str(r.get("基线修正规则") or "UNKNOWN") == rule]
            for name, col in [(A3, A3), (A22, A22), (A23, A23)]:
                out.append({"Horizon": hs, "规则": rule, "模型": name, **base.metric(seg, col)})
    return out


def july_top(rows: Sequence[Dict[str, Any]], topn: int) -> List[Dict[str, Any]]:
    fields = [
        "目标月", "快照月", "Horizon", "SPU", "品类", "生命周期", "ABC", "实际销量",
        "快照M1", "快照M2", "快照M3", "M1_均值M2M3", "严格衰退月比", "弱动量比",
        A3, A19, A20, A21, A22, A23, "基线修正规则",
    ]
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [r for r in rows if str(r.get("目标月")) == "2026-07" and str(r.get("Horizon")) == hs and str(r.get("品类")) == "T恤"]
        seg.sort(key=lambda r: float(r.get(A3, 0) or 0) - float(r.get("实际销量", 0) or 0), reverse=True)
        for i, r in enumerate(seg[:topn], 1):
            x = {"Rank": i}
            for f in fields:
                x[f] = r.get(f)
            x["A3误差"] = int(round(float(r.get(A3, 0) or 0) - float(r.get("实际销量", 0) or 0)))
            x["A22误差"] = int(round(float(r.get(A22, 0) or 0) - float(r.get("实际销量", 0) or 0)))
            out.append(x)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    ap.add_argument("--topn", type=int, default=30)
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    sales_end = base.add_months(max(targets), 1)
    print("读取店内销量:", history_start, "~", sales_end)
    sales_rows = base.read_sales(history_start, sales_end)
    actual = base.actual_spu_month(sales_rows)

    print("构建 ESTABLISHED H2/H3 OOS，并测试 A3 基线轨迹 challenger...")
    rows = build_oos(actual, targets, args.max_horizon)
    rows = add_candidates(actual, rows)

    summary_rows = base.norm(summary(rows))
    monthly_rows = base.norm(monthly(rows))
    rule_rows = base.norm(rule_summary(rows))
    top_rows = base.norm(july_top(rows, args.topn))
    detail_rows = base.norm(rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十五轮A3基线轨迹_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), summary_rows)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), monthly_rows)
    output_fix.write_csv(root.with_name(root.name + "_规则拆分.csv"), rule_rows)
    output_fix.write_csv(root.with_name(root.name + "_7月T恤TOP.csv"), top_rows)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("总览", summary_rows),
        ("逐月", monthly_rows),
        ("规则拆分", rule_rows),
        ("7月T恤TOP", top_rows),
        ("OOS明细", detail_rows),
    ])

    print("\n=== 全部ESTABLISHED / ABC_A / T恤：A3基线challenger ===")
    for r in summary_rows:
        print(r)
    print("\n=== A22基线修正规则拆分 ===")
    for r in rule_rows:
        print(r)
    print("\n=== 逐月 A3/A21/A22/A23 ===")
    for r in monthly_rows:
        print(r)
    print("\n=== 2026-07 T恤 A3高估 TOP：修正前后 ===")
    for r in top_rows:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：优先看全部ESTABLISHED与ABC_A是否跨多月改善；若只修好7月T恤但伤害其他月份/品类，则拒绝。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
