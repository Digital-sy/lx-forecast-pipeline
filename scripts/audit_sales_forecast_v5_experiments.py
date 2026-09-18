#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 experiment backtest at SPU-month granularity.

Read-only experiment. Does not modify production forecast tables or logic.

Models:
- V4: current production v4 replay.
- A0: last visible complete month SPU sales.
- A1_V4_no_floor: exact v4 source with the seasonal +10% floor disabled in-memory.
- A2_SPU_direct: direct SPU forecast using robust recent level + capped YoY seasonal candidate.
- A3_SPU_lifecycle_shrink: lifecycle-aware, horizon-shrunk challenger built on A0/A2.

The target is not to declare A2/A3 production-ready. The goal is to measure
which complexity adds value beyond A0 before changing the production model.
"""
from __future__ import annotations

import argparse
import inspect
import sys
import types
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
import jobs.feishu.forecast_sales_improved as prod_v4

MODELS = ("V4", "A0_上月延续", "A1_V4去floor", "A2_SPU直接", "A3_SPU生命周期收缩")


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def month_diff(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def load_no_floor_module():
    """Load an in-memory copy of production v4 with only _apply_floor disabled.

    Fail closed: the exact source pattern must occur once, otherwise abort.
    """
    src = inspect.getsource(prod_v4)
    needle = "        if is_uptrend:\n            floor = int(prev_forecast * 1.1)\n"
    replacement = "        if False and is_uptrend:\n            floor = int(prev_forecast * 1.1)\n"
    count = src.count(needle)
    if count != 1:
        raise RuntimeError(
            f"无法安全构造 no-floor V4：预期 floor 源码片段出现1次，实际{count}次。"
            "请先检查 forecast_sales_improved.py。"
        )
    src = src.replace(needle, replacement, 1)
    mod = types.ModuleType("forecast_sales_improved_no_floor")
    mod.__file__ = str(Path(prod_v4.__file__).resolve())
    mod.__package__ = prod_v4.__package__
    exec(compile(src, mod.__file__, "exec"), mod.__dict__)
    return mod


def replay_with_compute(
    rows: Sequence[Dict[str, Any]],
    snapshot: date,
    target: date,
    season_map: Dict[str, str],
    compute_fn,
) -> Dict[Tuple[str, date], int]:
    h = month_diff(snapshot, target)
    labels = [base.forecast_label(base.add_months(snapshot, i)) for i in range(h + 1)]
    history = base.build_history(rows, snapshot)
    out: Dict[Tuple[str, date], int] = defaultdict(int)
    target_label = base.forecast_label(target)
    for _shop, shop_data in history.items():
        f = compute_fn(
            shop_data,
            labels,
            current_date=datetime(snapshot.year, snapshot.month, 1),
            spu_season_map=season_map,
        )
        for sku, vals in f.items():
            spu = str(shop_data[sku].get("SPU") or base.extract_spu_from_sku(sku)).strip()
            if not spu:
                continue
            out[(spu, target)] += int(vals.get(target_label, 0) or 0)
    return dict(out)


def history_qty(actual: Dict[Tuple[str, date], int], spu: str, d: date) -> int:
    return int(actual.get((spu, d), 0) or 0)


def visible_history_months(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
    max_lookback: int = 24,
) -> List[Tuple[date, int]]:
    out = []
    for i in range(max_lookback, 0, -1):
        d = base.add_months(snapshot, -i)
        out.append((d, history_qty(actual, spu, d)))
    return out


def classify_lifecycle(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
) -> str:
    hist = visible_history_months(actual, spu, snapshot, 24)
    positive = [(d, q) for d, q in hist if q > 0]
    if not positive:
        return "无历史"
    first = positive[0][0]
    age = month_diff(first, snapshot)
    m1 = history_qty(actual, spu, base.add_months(snapshot, -1))
    m2 = history_qty(actual, spu, base.add_months(snapshot, -2))
    m3 = history_qty(actual, spu, base.add_months(snapshot, -3))
    if age <= 3:
        return "新品"
    if m1 > 0 and m2 > 0 and m3 > 0:
        if m1 < m2 < m3 and m1 <= 0.85 * m3:
            return "衰退"
        if m1 > m2 > m3 and m1 >= 1.15 * m3:
            return "成长"
    return "稳定"


def a0_forecast(
    actual: Dict[Tuple[str, date], int], spu: str, snapshot: date
) -> int:
    return history_qty(actual, spu, base.add_months(snapshot, -1))


def a2_direct_spu(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
    target: date,
) -> int:
    """Robust direct-SPU challenger using only pre-snapshot history."""
    m1d, m2d, m3d = (base.add_months(snapshot, -i) for i in (1, 2, 3))
    m1, m2, m3 = (history_qty(actual, spu, d) for d in (m1d, m2d, m3d))
    if m1 <= 0 and m2 <= 0 and m3 <= 0:
        return 0

    recent_level = 0.6 * m1 + 0.3 * m2 + 0.1 * m3
    yoy_target = history_qty(actual, spu, base.add_months(target, -12))
    ly_recent = [
        history_qty(actual, spu, base.add_months(d, -12))
        for d in (m1d, m2d, m3d)
    ]
    this3 = m1 + m2 + m3
    ly3 = sum(ly_recent)

    if yoy_target >= 10 and ly3 >= 30 and this3 > 0:
        growth = clamp(this3 / ly3, 0.70, 1.30)
        seasonal_candidate = yoy_target * growth
        pred = 0.65 * recent_level + 0.35 * seasonal_candidate
    else:
        pred = recent_level

    anchor = m1 if m1 > 0 else recent_level
    if anchor > 0:
        pred = clamp(pred, 0.60 * anchor, 1.40 * anchor)
    return max(0, int(round(pred)))


def a3_lifecycle_shrink(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
    target: date,
) -> Tuple[int, str]:
    """Conservative lifecycle-aware challenger with horizon shrinkage to A0."""
    h = month_diff(snapshot, target)
    a0 = a0_forecast(actual, spu, snapshot)
    a2 = a2_direct_spu(actual, spu, snapshot, target)
    lifecycle = classify_lifecycle(actual, spu, snapshot)

    weights = {0: 0.55, 1: 0.40, 2: 0.28, 3: 0.18}
    w = weights.get(h, max(0.10, 0.18 - 0.04 * (h - 3)))

    if lifecycle == "新品":
        w *= 0.50
    elif lifecycle == "衰退":
        w *= 0.60
        a2 = min(a2, a0) if a0 > 0 else a2
    elif lifecycle == "成长":
        w *= 0.85

    pred = a0 + w * (a2 - a0)

    if a0 > 0:
        if lifecycle == "新品":
            lo, hi = 0.65, 1.18
        elif lifecycle == "衰退":
            lo, hi = 0.55, 1.00
        elif lifecycle == "成长":
            lo, hi = 0.70, 1.22
        else:
            lo, hi = 0.70, 1.20
        pred = clamp(pred, lo * a0, hi * a0)

    return max(0, int(round(pred))), lifecycle


def sales_bucket(actual_qty: int) -> str:
    if actual_qty <= 0:
        return "0"
    if actual_qty < 10:
        return "1-9"
    if actual_qty < 50:
        return "10-49"
    if actual_qty < 200:
        return "50-199"
    return "200+"


def metric_rows(
    rows: Sequence[Dict[str, Any]],
    model_cols: Sequence[Tuple[str, str]],
    prefix: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    prefix = prefix or {}
    out = []
    for model, col in model_cols:
        out.append({**prefix, "模型": model, **base.metric(rows, col)})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)

    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)
    season_map = prod_v4.load_spu_season_map()
    no_floor = load_no_floor_module()
    print("no-floor V4: 已通过源码唯一片段校验，仅关闭 seasonal +10% floor")

    detail: List[Dict[str, Any]] = []

    for target in targets:
        for h in range(args.max_horizon + 1):
            snapshot = base.add_months(target, -h)
            v4, _methods = base.replay(sales_rows, snapshot, target, season_map)
            a1 = replay_with_compute(
                sales_rows, snapshot, target, season_map, no_floor.compute_forecast_for_shop
            )

            target_spus = {spu for (spu, d), qty in actual.items() if d == target and qty != 0}
            target_spus |= {spu for (spu, d) in v4 if d == target}
            target_spus |= {spu for (spu, d) in a1 if d == target}

            for spu in sorted(target_spus):
                key = (spu, target)
                act = history_qty(actual, spu, target)
                a0 = a0_forecast(actual, spu, snapshot)
                a2 = a2_direct_spu(actual, spu, snapshot, target)
                a3, lifecycle = a3_lifecycle_shrink(actual, spu, snapshot, target)
                season = season_map.get(spu)
                row = {
                    "目标月": target.strftime("%Y-%m"),
                    "快照月": snapshot.strftime("%Y-%m"),
                    "Horizon": f"H{h}",
                    "SPU": spu,
                    "季节": season if season else "未映射",
                    "生命周期": lifecycle,
                    "实际销量": act,
                    "销量层级": sales_bucket(act),
                    "V4": int(v4.get(key, 0) or 0),
                    "A0_上月延续": a0,
                    "A1_V4去floor": int(a1.get(key, 0) or 0),
                    "A2_SPU直接": a2,
                    "A3_SPU生命周期收缩": a3,
                }
                detail.append(row)

    model_cols = [(m, m) for m in MODELS]

    horizon_summary: List[Dict[str, Any]] = []
    for h in range(args.max_horizon + 1):
        subset = [r for r in detail if r["Horizon"] == f"H{h}"]
        horizon_summary.extend(metric_rows(subset, model_cols, {"Horizon": f"H{h}"}))

    month_summary: List[Dict[str, Any]] = []
    for target in targets:
        t = target.strftime("%Y-%m")
        for h in range(args.max_horizon + 1):
            subset = [r for r in detail if r["目标月"] == t and r["Horizon"] == f"H{h}"]
            month_summary.extend(metric_rows(subset, model_cols, {"目标月": t, "Horizon": f"H{h}"}))

    season_summary: List[Dict[str, Any]] = []
    seasons = sorted({r["季节"] for r in detail})
    for h in range(args.max_horizon + 1):
        for season in seasons:
            subset = [r for r in detail if r["Horizon"] == f"H{h}" and r["季节"] == season]
            season_summary.extend(metric_rows(subset, model_cols, {"Horizon": f"H{h}", "季节": season}))

    bucket_summary: List[Dict[str, Any]] = []
    for h in range(args.max_horizon + 1):
        for bucket in ["0", "1-9", "10-49", "50-199", "200+"]:
            subset = [r for r in detail if r["Horizon"] == f"H{h}" and r["销量层级"] == bucket]
            if subset:
                bucket_summary.extend(metric_rows(subset, model_cols, {"Horizon": f"H{h}", "销量层级": bucket}))

    lifecycle_summary: List[Dict[str, Any]] = []
    lifecycles = sorted({r["生命周期"] for r in detail})
    for h in range(args.max_horizon + 1):
        for lc in lifecycles:
            subset = [r for r in detail if r["Horizon"] == f"H{h}" and r["生命周期"] == lc]
            if subset:
                lifecycle_summary.extend(metric_rows(subset, model_cols, {"Horizon": f"H{h}", "生命周期": lc}))

    horizon_summary = base.norm(horizon_summary)
    month_summary = base.norm(month_summary)
    season_summary = base.norm(season_summary)
    bucket_summary = base.norm(bucket_summary)
    lifecycle_summary = base.norm(lifecycle_summary)

    top = []
    for r in detail:
        x = dict(r)
        x["A0绝对误差"] = abs(int(r["A0_上月延续"]) - int(r["实际销量"]))
        x["A3绝对误差"] = abs(int(r["A3_SPU生命周期收缩"]) - int(r["实际销量"]))
        x["A3相对A0误差改善"] = x["A0绝对误差"] - x["A3绝对误差"]
        top.append(x)
    top = sorted(top, key=lambda r: abs(r["A3相对A0误差改善"]), reverse=True)[:1000]

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5实验回测_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_Horizon总览.csv"), horizon_summary)
    output_fix.write_csv(root.with_name(root.name + "_月份.csv"), month_summary)
    output_fix.write_csv(root.with_name(root.name + "_季节.csv"), season_summary)
    output_fix.write_csv(root.with_name(root.name + "_销量层级.csv"), bucket_summary)
    output_fix.write_csv(root.with_name(root.name + "_生命周期.csv"), lifecycle_summary)
    output_fix.write_csv(root.with_name(root.name + "_SPU明细.csv"), detail)
    output_fix.write_csv(root.with_name(root.name + "_A3vsA0_TOP.csv"), top)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(
        xlsx,
        [
            ("Horizon总览", horizon_summary),
            ("月份", month_summary),
            ("季节", season_summary),
            ("销量层级", bucket_summary),
            ("生命周期", lifecycle_summary),
            ("SPU明细", detail),
            ("A3vsA0_TOP", top),
        ],
    )

    print("\n=== V5实验 Horizon总览 ===")
    for r in horizon_summary:
        print(r)
    print("\n=== A1 no-floor 相对V4 ===")
    for h in range(args.max_horizon + 1):
        v4 = next(r for r in horizon_summary if r["Horizon"] == f"H{h}" and r["模型"] == "V4")
        a1r = next(r for r in horizon_summary if r["Horizon"] == f"H{h}" and r["模型"] == "A1_V4去floor")
        print({
            "Horizon": f"H{h}",
            "V4_WAPE": v4.get("WAPE"),
            "no_floor_WAPE": a1r.get("WAPE"),
            "WAPE改善": None if v4.get("WAPE") is None or a1r.get("WAPE") is None else round(v4["WAPE"] - a1r["WAPE"], 6),
            "V4_Bias%": v4.get("Bias%"),
            "no_floor_Bias%": a1r.get("Bias%"),
        })

    print("\nExcel:", xlsx.resolve())
    print("说明：A2/A3仅为实验 challenger，不修改生产模型；A3只有稳定击败A0才值得继续。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
