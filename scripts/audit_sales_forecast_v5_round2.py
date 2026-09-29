#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-2 temporal validation for SPU-month forecasting.

Read-only experiment. No production tables or production forecast code are changed.

Purpose
-------
Use the first V5 experiment results without overfitting the full 12-month sample.
We compare:
- A0: last visible complete month SPU sales (champion baseline)
- A2: direct-SPU challenger
- A3: lifecycle + horizon shrink challenger
- A4: horizon-specific blend of A2/A3 selected ONLY on past target months

Validation
----------
1) Fixed temporal holdout: first 2/3 target months tune A4, last 1/3 validate.
2) Expanding walk-forward: after >=6 target months, choose lambda using only prior
   target months, then score the next target month. This is the main OOS test.

A4 blend:
    pred = (1-lambda) * A3 + lambda * A2
lambda is searched on [0, 1] in 0.05 steps.
Selection prefers abs(Bias)<=5%, then lowest WAPE. If no candidate meets the
bias guardrail, minimize WAPE + 0.5*abs(Bias).
"""
from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_experiments as exp


MODEL_COLS = [
    ("A0_上月延续", "A0_上月延续"),
    ("A2_SPU直接", "A2_SPU直接"),
    ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
]


def all_spus_from_actual(actual: Dict[Tuple[str, date], int]) -> List[str]:
    return sorted({spu for spu, _d in actual})


def has_visible_signal(actual: Dict[Tuple[str, date], int], spu: str, snapshot: date) -> bool:
    """Include SPU if it has any sales in the 24 months visible before snapshot."""
    for i in range(1, 25):
        if exp.history_qty(actual, spu, base.add_months(snapshot, -i)) != 0:
            return True
    return False


def build_detail(
    actual: Dict[Tuple[str, date], int],
    targets: Sequence[date],
    max_horizon: int,
) -> List[Dict[str, Any]]:
    """Build leakage-free A0/A2/A3 predictions at SPU-month grain.

    Evaluation universe per target/snapshot is union of:
    - SPUs with non-zero target actuals, and
    - SPUs with any visible sales in the preceding 24 months.
    This counts false-positive forecasts for dormant products while not using
    future information to decide whether a historical SPU is forecastable.
    """
    universe = all_spus_from_actual(actual)
    detail: List[Dict[str, Any]] = []

    for target in targets:
        actual_target = {spu for (spu, d), q in actual.items() if d == target and q != 0}
        for h in range(max_horizon + 1):
            snapshot = base.add_months(target, -h)
            visible = {spu for spu in universe if has_visible_signal(actual, spu, snapshot)}
            spus = sorted(actual_target | visible)
            for spu in spus:
                a0 = exp.a0_forecast(actual, spu, snapshot)
                a2 = exp.a2_direct_spu(actual, spu, snapshot, target)
                a3, lifecycle = exp.a3_lifecycle_shrink(actual, spu, snapshot, target)
                act = exp.history_qty(actual, spu, target)
                detail.append({
                    "目标月": target.strftime("%Y-%m"),
                    "快照月": snapshot.strftime("%Y-%m"),
                    "Horizon": f"H{h}",
                    "SPU": spu,
                    "生命周期": lifecycle,
                    "实际销量": act,
                    "销量层级": exp.sales_bucket(act),
                    "A0_上月延续": a0,
                    "A2_SPU直接": a2,
                    "A3_SPU生命周期收缩": a3,
                })
    return detail


def add_blend(rows: Sequence[Dict[str, Any]], lam: float, col: str = "A4") -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a2 = float(r["A2_SPU直接"] or 0)
        a3 = float(r["A3_SPU生命周期收缩"] or 0)
        x[col] = int(round((1.0 - lam) * a3 + lam * a2))
        out.append(x)
    return out


def metric_for_lambda(rows: Sequence[Dict[str, Any]], lam: float) -> Dict[str, Any]:
    mixed = add_blend(rows, lam, "A4")
    m = base.metric(mixed, "A4")
    return {"lambda_A2": round(lam, 2), **m}


def choose_lambda(rows: Sequence[Dict[str, Any]]) -> Tuple[float, List[Dict[str, Any]]]:
    grid = [i / 20 for i in range(21)]  # 0.00 ... 1.00, step 0.05
    scored = [metric_for_lambda(rows, lam) for lam in grid]
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
    return float(best["lambda_A2"]), scored


def metrics_for_models(rows: Sequence[Dict[str, Any]], a4_lambda: float | None = None) -> List[Dict[str, Any]]:
    work = list(rows)
    cols = list(MODEL_COLS)
    if a4_lambda is not None:
        work = add_blend(work, a4_lambda, "A4_时序选择")
        cols.append(("A4_时序选择", "A4_时序选择"))
    out = []
    for model, col in cols:
        out.append({"模型": model, **base.metric(work, col)})
    return out


def fixed_holdout(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    n = len(targets)
    if n < 6:
        raise RuntimeError("固定留出至少需要6个目标月")
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
        lam, grid = choose_lambda(train)
        choices.append({
            "Horizon": hs,
            "lambda_A2": lam,
            "lambda_A3": round(1-lam, 2),
            "训练月份": ",".join(sorted(train_months)),
            "验证月份": ",".join(sorted(test_months)),
        })
        for x in grid:
            grid_rows.append({"Horizon": hs, **x})

        for split, rows in (("TRAIN", train), ("HOLDOUT", test)):
            for x in metrics_for_models(rows, lam):
                results.append({"数据集": split, "Horizon": hs, "lambda_A2": lam if x["模型"] == "A4_时序选择" else None, **x})

    return choices, results, grid_rows


def walk_forward(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if len(targets) <= min_train_months:
        raise RuntimeError("目标月份不足以进行walk-forward")

    selections: List[Dict[str, Any]] = []
    oos_rows: List[Dict[str, Any]] = []

    target_text = [d.strftime("%Y-%m") for d in targets]
    for test_idx in range(min_train_months, len(targets)):
        test_month = target_text[test_idx]
        train_months = set(target_text[:test_idx])
        for h in range(max_horizon + 1):
            hs = f"H{h}"
            train = [r for r in detail if r["Horizon"] == hs and r["目标月"] in train_months]
            test = [r for r in detail if r["Horizon"] == hs and r["目标月"] == test_month]
            lam, _grid = choose_lambda(train)
            selections.append({
                "测试月": test_month,
                "Horizon": hs,
                "训练月数": len(train_months),
                "lambda_A2": lam,
                "lambda_A3": round(1-lam, 2),
            })
            mixed = add_blend(test, lam, "A4_时序选择")
            for r in mixed:
                x = dict(r)
                x["walk_lambda_A2"] = lam
                oos_rows.append(x)

    summary: List[Dict[str, Any]] = []
    for h in range(max_horizon + 1):
        hs = f"H{h}"
        rows = [r for r in oos_rows if r["Horizon"] == hs]
        for model, col in MODEL_COLS + [("A4_时序选择", "A4_时序选择")]:
            summary.append({"Horizon": hs, "模型": model, **base.metric(rows, col)})

    return selections, summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True, help="YYYY-MM,YYYY-MM,... in chronological span")
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 8:
        raise RuntimeError("本实验建议至少8个连续目标月")

    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(rows)

    print("构建A0/A2/A3 SPU-month无泄漏预测...")
    detail = build_detail(actual, targets, args.max_horizon)

    full_summary: List[Dict[str, Any]] = []
    for h in range(args.max_horizon + 1):
        hs = f"H{h}"
        subset = [r for r in detail if r["Horizon"] == hs]
        for x in metrics_for_models(subset):
            full_summary.append({"Horizon": hs, **x})
    full_summary = base.norm(full_summary)

    choices, holdout_results, grid_rows = fixed_holdout(detail, targets, args.max_horizon)
    holdout_results = base.norm(holdout_results)
    grid_rows = base.norm(grid_rows)

    wf_selections, wf_summary = walk_forward(detail, targets, args.max_horizon, min_train_months=6)
    wf_summary = base.norm(wf_summary)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第二轮时序验证_{stamp}"

    output_fix.write_csv(root.with_name(root.name + "_全样本.csv"), full_summary)
    output_fix.write_csv(root.with_name(root.name + "_固定留出权重.csv"), choices)
    output_fix.write_csv(root.with_name(root.name + "_固定留出结果.csv"), holdout_results)
    output_fix.write_csv(root.with_name(root.name + "_权重网格.csv"), grid_rows)
    output_fix.write_csv(root.with_name(root.name + "_walkforward权重.csv"), wf_selections)
    output_fix.write_csv(root.with_name(root.name + "_walkforward结果.csv"), wf_summary)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("全样本", full_summary),
        ("固定留出权重", choices),
        ("固定留出结果", holdout_results),
        ("权重网格", grid_rows),
        ("walkforward权重", wf_selections),
        ("walkforward结果", wf_summary),
    ])

    print("\n=== 固定时间留出：A4权重 ===")
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
    print("判定原则：优先看Walk-forward；A4若不能稳定优于A0且Bias受控，则不进入生产候选。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
