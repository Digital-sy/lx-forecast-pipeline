#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-3: seasonal candidate + rolling bias calibration.

Read-only experiment. Does not modify production tables or production forecast code.

Why this experiment exists
--------------------------
Round-2 walk-forward showed that A0/A2/A3 all become increasingly negative-bias
at H2/H3. That means the common recent-level anchor is too conservative when the
target month is seasonally above the last visible complete month.

Models
------
A0: last visible complete month SPU sales.
A3: lifecycle + horizon shrink from round-1.
A5: blend A3 with a direct seasonal candidate:
        seasonal = last-year target-month sales * recent SPU YoY growth
    where recent SPU YoY growth is based on aggregated recent-3-month SPU sales,
    not an average of SKU ratios. Growth is robustly capped.
    The blend weight gamma is selected separately by horizon using past target
    months only.
A6: A5 plus a rolling aggregate bias calibration factor learned from the last
    three *already completed* target months only. This tests whether a small,
    transparent calibration layer can correct regime-level under/over forecast.

Validation
----------
1) Fixed temporal holdout: first 2/3 target months tune, last 1/3 validate.
2) Expanding walk-forward: for every test month after >=6 training months,
   gamma is selected only from prior target months; bias scale uses only the last
   3 prior target months. This is the primary out-of-sample test.
"""
from __future__ import annotations

import argparse
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_experiments as exp
from scripts import audit_sales_forecast_v5_round2 as r2


def seasonal_candidate(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
    target: date,
) -> Optional[int]:
    """Leakage-free SPU seasonal candidate.

    Uses only data strictly before snapshot plus last-year target month.
    Returns None when history is too sparse for a credible seasonal signal.
    """
    recent_dates = [base.add_months(snapshot, -i) for i in (1, 2, 3)]
    recent_vals = [exp.history_qty(actual, spu, d) for d in recent_dates]
    ly_recent_vals = [
        exp.history_qty(actual, spu, base.add_months(d, -12))
        for d in recent_dates
    ]
    recent_sum = sum(recent_vals)
    ly_recent_sum = sum(ly_recent_vals)
    yoy_target = exp.history_qty(actual, spu, base.add_months(target, -12))

    # Avoid unstable ratios on tiny bases.
    if recent_sum <= 0 or ly_recent_sum < 30 or yoy_target < 10:
        return None

    growth = exp.clamp(recent_sum / ly_recent_sum, 0.60, 1.60)
    return max(0, int(round(yoy_target * growth)))


def enrich_detail(
    actual: Dict[Tuple[str, date], int],
    detail: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in detail:
        x = dict(r)
        snapshot = datetime.strptime(r["快照月"], "%Y-%m").date().replace(day=1)
        target = datetime.strptime(r["目标月"], "%Y-%m").date().replace(day=1)
        cand = seasonal_candidate(actual, r["SPU"], snapshot, target)
        x["季节候选"] = cand
        x["有季节候选"] = 1 if cand is not None else 0
        out.append(x)
    return out


def add_a5(rows: Sequence[Dict[str, Any]], gamma: float, col: str = "A5_季节融合") -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r["A3_SPU生命周期收缩"] or 0)
        cand = r.get("季节候选")
        if cand is None:
            pred = a3
        else:
            pred = (1.0 - gamma) * a3 + gamma * float(cand)
        x[col] = max(0, int(round(pred)))
        out.append(x)
    return out


def metric_for_gamma(rows: Sequence[Dict[str, Any]], gamma: float) -> Dict[str, Any]:
    work = add_a5(rows, gamma)
    return {"gamma_seasonal": round(gamma, 2), **base.metric(work, "A5_季节融合")}


def choose_gamma(rows: Sequence[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
    grid = [i / 20 for i in range(21)]  # 0.00..1.00, step 0.05
    scored = [metric_for_gamma(rows, g) for g in grid]
    feasible = [x for x in scored if x.get("Bias%") is not None and abs(x["Bias%"]) <= 0.05]
    if feasible:
        best = min(feasible, key=lambda x: (x.get("WAPE", math.inf), abs(x.get("Bias%", math.inf))))
    else:
        best = min(
            scored,
            key=lambda x: (
                (x.get("WAPE", math.inf) if x.get("WAPE") is not None else math.inf)
                + 0.5 * abs(x.get("Bias%", math.inf) if x.get("Bias%") is not None else math.inf)
            ),
        )
    return float(best["gamma_seasonal"]), scored


def calibration_scale(
    train_rows: Sequence[Dict[str, Any]],
    gamma: float,
    recent_months: int = 3,
) -> Tuple[float, str]:
    """Aggregate rolling bias scale from the latest completed training months."""
    months = sorted({r["目标月"] for r in train_rows})
    use_months = set(months[-recent_months:])
    recent = [r for r in train_rows if r["目标月"] in use_months]
    work = add_a5(recent, gamma)
    actual_sum = sum(float(r["实际销量"] or 0) for r in work)
    pred_sum = sum(float(r["A5_季节融合"] or 0) for r in work)
    if pred_sum <= 0:
        return 1.0, ",".join(sorted(use_months))
    raw = actual_sum / pred_sum
    scale = exp.clamp(raw, 0.85, 1.20)
    return round(scale, 6), ",".join(sorted(use_months))


def add_a6(
    rows: Sequence[Dict[str, Any]],
    gamma: float,
    scale: float,
    a5_col: str = "A5_季节融合",
    a6_col: str = "A6_季节融合_滚动校准",
) -> List[Dict[str, Any]]:
    work = add_a5(rows, gamma, a5_col)
    out: List[Dict[str, Any]] = []
    for r in work:
        x = dict(r)
        x[a6_col] = max(0, int(round(float(r[a5_col] or 0) * scale)))
        out.append(x)
    return out


def score_models(rows: Sequence[Dict[str, Any]], gamma: float, scale: float) -> List[Dict[str, Any]]:
    work = add_a6(rows, gamma, scale)
    cols = [
        ("A0_上月延续", "A0_上月延续"),
        ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
        ("A5_季节融合", "A5_季节融合"),
        ("A6_季节融合_滚动校准", "A6_季节融合_滚动校准"),
    ]
    return [{"模型": name, **base.metric(work, col)} for name, col in cols]


def fixed_holdout(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    n = len(targets)
    train_n = max(6, int(math.floor(n * 2 / 3)))
    train_n = min(train_n, n - 2)
    train_months = {d.strftime("%Y-%m") for d in targets[:train_n]}
    test_months = {d.strftime("%Y-%m") for d in targets[train_n:]}

    choices: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    grid_rows: List[Dict[str, Any]] = []

    for h in range(max_horizon + 1):
        hs = f"H{h}"
        train = [r for r in detail if r["Horizon"] == hs and r["目标月"] in train_months]
        test = [r for r in detail if r["Horizon"] == hs and r["目标月"] in test_months]
        gamma, grid = choose_gamma(train)
        scale, scale_months = calibration_scale(train, gamma, 3)
        choices.append({
            "Horizon": hs,
            "gamma_seasonal": gamma,
            "rolling_scale": scale,
            "scale训练月份": scale_months,
            "训练月份": ",".join(sorted(train_months)),
            "验证月份": ",".join(sorted(test_months)),
        })
        for x in grid:
            grid_rows.append({"Horizon": hs, **x})
        for split, rows in (("TRAIN", train), ("HOLDOUT", test)):
            for x in score_models(rows, gamma, scale):
                results.append({
                    "数据集": split,
                    "Horizon": hs,
                    "gamma_seasonal": gamma if x["模型"].startswith("A5") or x["模型"].startswith("A6") else None,
                    "rolling_scale": scale if x["模型"].startswith("A6") else None,
                    **x,
                })
    return choices, results, grid_rows


def walk_forward(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    target_text = [d.strftime("%Y-%m") for d in targets]
    selections: List[Dict[str, Any]] = []
    oos: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(targets)):
        test_month = target_text[test_idx]
        train_months = set(target_text[:test_idx])
        for h in range(max_horizon + 1):
            hs = f"H{h}"
            train = [r for r in detail if r["Horizon"] == hs and r["目标月"] in train_months]
            test = [r for r in detail if r["Horizon"] == hs and r["目标月"] == test_month]
            gamma, _grid = choose_gamma(train)
            scale, scale_months = calibration_scale(train, gamma, 3)
            selections.append({
                "测试月": test_month,
                "Horizon": hs,
                "训练月数": len(train_months),
                "gamma_seasonal": gamma,
                "rolling_scale": scale,
                "scale训练月份": scale_months,
            })
            work = add_a6(test, gamma, scale)
            for r in work:
                x = dict(r)
                x["walk_gamma_seasonal"] = gamma
                x["walk_rolling_scale"] = scale
                oos.append(x)

    summary: List[Dict[str, Any]] = []
    month_summary: List[Dict[str, Any]] = []
    models = [
        ("A0_上月延续", "A0_上月延续"),
        ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
        ("A5_季节融合", "A5_季节融合"),
        ("A6_季节融合_滚动校准", "A6_季节融合_滚动校准"),
    ]
    for h in range(max_horizon + 1):
        hs = f"H{h}"
        rows = [r for r in oos if r["Horizon"] == hs]
        for name, col in models:
            summary.append({"Horizon": hs, "模型": name, **base.metric(rows, col)})
        for month in sorted({r["目标月"] for r in rows}):
            mr = [r for r in rows if r["目标月"] == month]
            for name, col in models:
                month_summary.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(mr, col)})
    return selections, summary, month_summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 8:
        raise RuntimeError("本实验至少需要8个连续目标月")

    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(rows)

    print("构建A0/A3无泄漏SPU-month预测...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = enrich_detail(actual, detail)
    coverage = sum(int(r["有季节候选"]) for r in detail) / len(detail) if detail else 0
    print(f"季节候选覆盖率: {coverage:.2%}")

    choices, holdout_results, grid_rows = fixed_holdout(detail, targets, args.max_horizon)
    holdout_results = base.norm(holdout_results)
    grid_rows = base.norm(grid_rows)

    wf_choices, wf_summary, wf_months = walk_forward(detail, targets, args.max_horizon, 6)
    wf_summary = base.norm(wf_summary)
    wf_months = base.norm(wf_months)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第三轮季节校准_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_固定留出参数.csv"), choices)
    output_fix.write_csv(root.with_name(root.name + "_固定留出结果.csv"), holdout_results)
    output_fix.write_csv(root.with_name(root.name + "_gamma网格.csv"), grid_rows)
    output_fix.write_csv(root.with_name(root.name + "_walkforward参数.csv"), wf_choices)
    output_fix.write_csv(root.with_name(root.name + "_walkforward结果.csv"), wf_summary)
    output_fix.write_csv(root.with_name(root.name + "_walkforward月份.csv"), wf_months)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("固定留出参数", choices),
        ("固定留出结果", holdout_results),
        ("gamma网格", grid_rows),
        ("walkforward参数", wf_choices),
        ("walkforward结果", wf_summary),
        ("walkforward月份", wf_months),
    ])

    print("\n=== 固定时间留出：参数 ===")
    for r in choices:
        print(r)
    print("\n=== 固定时间留出：HOLDOUT ===")
    for r in holdout_results:
        if r["数据集"] == "HOLDOUT":
            print(r)
    print("\n=== Walk-forward OOS 总览 ===")
    for r in wf_summary:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定原则：优先Walk-forward；A6若降低WAPE且把H1-H3 Bias明显拉向±5%，才进入下一轮生产候选评估。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
