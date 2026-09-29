#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-5: asymmetric seasonal gating for SPU-month forecasting.

Read-only experiment. Does not modify production tables or production forecast code.

Evidence from round-4 diagnostics:
- Seasonal candidates cover only ~13% of SPU-month rows but >50% of actual volume.
- Downward seasonal adjustments materially improve H2/H3 accuracy.
- Upward seasonal adjustments can help spring ramp months, but global/full-strength
  uplift often overshoots, especially Jul/Aug.
- Reactivating A3==0 rows from a positive seasonal candidate is catastrophic.

This experiment therefore tests a stricter seasonal architecture:
1) Never reactivate when A3 <= 0.
2) Neutral candidate ratio 0.90..1.10 => keep A3.
3) Downward candidate (<0.90*A3) => allow a learned gamma_down.
4) Upward candidate (>1.10*A3) => only allow when last-year target-month phase was
   rising vs last-year previous month; cap the effective candidate to 1.50*A3;
   apply a separately learned gamma_up.
5) gamma_down/gamma_up are selected by horizon using past target months only.

Primary evaluation is expanding walk-forward OOS after >=6 target months.
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
from scripts import audit_sales_forecast_v5_round3 as r3


def historical_phase(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    target: date,
) -> str:
    """Classify target-month phase using last-year target vs previous month only."""
    ly_target_d = base.add_months(target, -12)
    ly_prev_d = base.add_months(target, -13)
    cur = exp.history_qty(actual, spu, ly_target_d)
    prev = exp.history_qty(actual, spu, ly_prev_d)
    if cur < 10 or prev < 10:
        return "UNKNOWN"
    ratio = cur / prev if prev > 0 else 1.0
    if ratio >= 1.05:
        return "RISING"
    if ratio <= 0.95:
        return "FALLING"
    return "FLAT"


def enrich(
    actual: Dict[Tuple[str, date], int],
    detail: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows = r3.enrich_detail(actual, detail)
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        target = datetime.strptime(r["目标月"], "%Y-%m").date().replace(day=1)
        a3 = float(r["A3_SPU生命周期收缩"] or 0)
        cand = r.get("季节候选")
        x["历史季节阶段"] = historical_phase(actual, r["SPU"], target)
        if cand is None or a3 <= 0:
            x["季节候选比A3"] = None
        else:
            x["季节候选比A3"] = float(cand) / a3
        out.append(x)
    return out


def add_a7(
    rows: Sequence[Dict[str, Any]],
    gamma_down: float,
    gamma_up: float,
    col: str = "A7_非对称季节门控",
    up_cap_ratio: float = 1.50,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r["A3_SPU生命周期收缩"] or 0)
        cand = r.get("季节候选")
        phase = r.get("历史季节阶段") or "UNKNOWN"

        # Never reactivate dormant/zero A3 rows from last-year seasonality alone.
        if a3 <= 0 or cand is None:
            pred = a3
            rule = "A3_ONLY"
        else:
            cand_f = float(cand)
            ratio = cand_f / a3 if a3 > 0 else 1.0
            if ratio < 0.90:
                pred = (1.0 - gamma_down) * a3 + gamma_down * cand_f
                rule = "DOWN"
            elif ratio <= 1.10:
                pred = a3
                rule = "NEUTRAL"
            else:
                # Upward seasonal lift is only trusted when last year's target month
                # was itself on a rising slope. Cap the candidate to avoid overshoot.
                if phase == "RISING":
                    effective = min(cand_f, a3 * up_cap_ratio)
                    pred = (1.0 - gamma_up) * a3 + gamma_up * effective
                    rule = "UP_RISING"
                else:
                    pred = a3
                    rule = "UP_BLOCKED"

        x[col] = max(0, int(round(pred)))
        x["A7规则"] = rule
        out.append(x)
    return out


def score_params(
    rows: Sequence[Dict[str, Any]],
    gamma_down: float,
    gamma_up: float,
) -> Dict[str, Any]:
    work = add_a7(rows, gamma_down, gamma_up)
    return {
        "gamma_down": round(gamma_down, 2),
        "gamma_up": round(gamma_up, 2),
        **base.metric(work, "A7_非对称季节门控"),
    }


def choose_params(rows: Sequence[Dict[str, Any]]) -> Tuple[float, float, List[Dict[str, Any]]]:
    # 0.00..1.00 step 0.10 => 121 combinations per horizon, intentionally small.
    grid = [i / 10 for i in range(11)]
    scored = [score_params(rows, gd, gu) for gd in grid for gu in grid]
    feasible = [x for x in scored if x.get("Bias%") is not None and abs(x["Bias%"]) <= 0.05]
    if feasible:
        best = min(
            feasible,
            key=lambda x: (x.get("WAPE", math.inf), abs(x.get("Bias%", math.inf))),
        )
    else:
        best = min(
            scored,
            key=lambda x: (
                (x.get("WAPE", math.inf) if x.get("WAPE") is not None else math.inf)
                + 0.5 * abs(x.get("Bias%", math.inf) if x.get("Bias%") is not None else math.inf)
            ),
        )
    return float(best["gamma_down"]), float(best["gamma_up"]), scored


def score_models(
    rows: Sequence[Dict[str, Any]],
    gd: float,
    gu: float,
    a5_gamma: float,
) -> List[Dict[str, Any]]:
    a5 = r3.add_a5(rows, a5_gamma)
    a7 = add_a7(rows, gd, gu)
    # Merge A7 prediction back by row position; both preserve order/length.
    work: List[Dict[str, Any]] = []
    for x5, x7 in zip(a5, a7):
        x = dict(x5)
        x["A7_非对称季节门控"] = x7["A7_非对称季节门控"]
        x["A7规则"] = x7["A7规则"]
        work.append(x)
    cols = [
        ("A0_上月延续", "A0_上月延续"),
        ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
        ("A5_季节融合", "A5_季节融合"),
        ("A7_非对称季节门控", "A7_非对称季节门控"),
    ]
    return [{"模型": name, **base.metric(work, col)} for name, col in cols]


def walk_forward(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    target_text = [d.strftime("%Y-%m") for d in targets]
    choices: List[Dict[str, Any]] = []
    oos: List[Dict[str, Any]] = []
    param_grid: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(targets)):
        test_month = target_text[test_idx]
        train_months = set(target_text[:test_idx])
        for h in range(max_horizon + 1):
            hs = f"H{h}"
            train = [r for r in detail if r["Horizon"] == hs and r["目标月"] in train_months]
            test = [r for r in detail if r["Horizon"] == hs and r["目标月"] == test_month]

            gd, gu, scored = choose_params(train)
            a5_gamma, _ = r3.choose_gamma(train)
            choices.append({
                "测试月": test_month,
                "Horizon": hs,
                "训练月数": len(train_months),
                "gamma_down": gd,
                "gamma_up": gu,
                "A5_gamma": a5_gamma,
            })
            # Keep only the best-training grid summary per test/horizon plus top metadata.
            best_train = score_params(train, gd, gu)
            param_grid.append({"测试月": test_month, "Horizon": hs, **best_train})

            a5_test = r3.add_a5(test, a5_gamma)
            a7_test = add_a7(test, gd, gu)
            for x5, x7 in zip(a5_test, a7_test):
                x = dict(x5)
                x["A7_非对称季节门控"] = x7["A7_非对称季节门控"]
                x["A7规则"] = x7["A7规则"]
                x["walk_gamma_down"] = gd
                x["walk_gamma_up"] = gu
                x["walk_A5_gamma"] = a5_gamma
                oos.append(x)

    summary: List[Dict[str, Any]] = []
    monthly: List[Dict[str, Any]] = []
    models = [
        ("A0_上月延续", "A0_上月延续"),
        ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
        ("A5_季节融合", "A5_季节融合"),
        ("A7_非对称季节门控", "A7_非对称季节门控"),
    ]
    for h in range(max_horizon + 1):
        hs = f"H{h}"
        rows = [r for r in oos if r["Horizon"] == hs]
        for name, col in models:
            summary.append({"Horizon": hs, "模型": name, **base.metric(rows, col)})
        for month in sorted({r["目标月"] for r in rows}):
            mr = [r for r in rows if r["目标月"] == month]
            for name, col in models:
                monthly.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(mr, col)})

    return choices, summary, monthly, param_grid


def rule_summary(oos: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Helper retained for future extension; current walk_forward returns only aggregate outputs.
    return []


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

    print("构建A0/A3/季节候选无泄漏SPU-month预测...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = enrich(actual, detail)

    wf_choices, wf_summary, wf_months, param_grid = walk_forward(
        detail, targets, args.max_horizon, min_train_months=6
    )
    wf_summary = base.norm(wf_summary)
    wf_months = base.norm(wf_months)
    param_grid = base.norm(param_grid)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第五轮非对称季节门控_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_walkforward参数.csv"), wf_choices)
    output_fix.write_csv(root.with_name(root.name + "_walkforward结果.csv"), wf_summary)
    output_fix.write_csv(root.with_name(root.name + "_walkforward逐月.csv"), wf_months)
    output_fix.write_csv(root.with_name(root.name + "_训练参数表现.csv"), param_grid)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("walkforward参数", wf_choices),
        ("walkforward结果", wf_summary),
        ("walkforward逐月", wf_months),
        ("训练参数表现", param_grid),
    ])

    print("\n=== Walk-forward OOS 总览 ===")
    for r in wf_summary:
        print(r)
    print("\n=== H2/H3逐月 A3/A5/A7 ===")
    for r in wf_months:
        if r["Horizon"] in ("H2", "H3") and r["模型"] in (
            "A3_SPU生命周期收缩", "A5_季节融合", "A7_非对称季节门控"
        ):
            print(r)
    print("\n=== Walk-forward 参数 ===")
    for r in wf_choices:
        if r["Horizon"] in ("H2", "H3"):
            print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：A7需在H2/H3降低WAPE，同时Bias优于A3/A5且不能依赖单一月份。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
