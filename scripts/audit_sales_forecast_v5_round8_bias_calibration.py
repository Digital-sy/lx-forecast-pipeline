#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-8: strict OOS rolling bias calibration for ESTABLISHED products.

Read-only experiment. Does not modify production forecast tables or production logic.

Why this exists
---------------
Round-7 confirmed two separate problems:
1) NEW_VISIBLE / COLD_NO_HISTORY require a separate launch/cold-start model.
2) On ESTABLISHED products A7 improves H2/H3 WAPE, but its later Stage-2 OOS
   window shows positive aggregate bias. The demand-profile router (A8) made this
   worse because short router history repeatedly selected A5 for high-volume Smooth
   products.

This round therefore removes routing complexity and asks a simpler question:
Can a lagged, aggregate calibration factor learned ONLY from already-completed
Stage-1 OOS months reduce A7 bias without damaging WAPE?

Candidates are fixed before evaluation (no parameter selection on test months):
- A7: raw asymmetric seasonal-gating forecast.
- A9: A7 * scale from last 2 completed Stage-1 OOS months, clamp 0.80..1.20.
- A10: A7 * scale from last 3 completed Stage-1 OOS months, clamp 0.80..1.20.
- A11: same last-3-month scale but DOWN-ONLY, clamp 0.80..1.00.
- A12: expanding-history DOWN-ONLY scale, clamp 0.85..1.00.

All calibration and evaluation is ESTABLISHED-only. Stage-2 starts after at least
2 completed Stage-1 OOS months, matching Round-7's strict second-stage window.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
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

A7_COL = "A7_非对称季节门控"
A9_COL = "A9_A7滚动2月校准"
A10_COL = "A10_A7滚动3月校准"
A11_COL = "A11_A7滚动3月只下调"
A12_COL = "A12_A7扩展窗只下调"

MODEL_COLS: List[Tuple[str, str]] = [
    ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
    ("A7_非对称季节门控", A7_COL),
    (A9_COL, A9_COL),
    (A10_COL, A10_COL),
    (A11_COL, A11_COL),
    (A12_COL, A12_COL),
]


def calibration_scale(
    rows: Sequence[Dict[str, Any]],
    lo: float,
    hi: float,
    down_only: bool = False,
) -> Tuple[float, float, int, int]:
    """Return leakage-free aggregate actual/pred scale and diagnostics."""
    actual_sum = int(round(sum(float(r.get("实际销量", 0) or 0) for r in rows)))
    pred_sum = int(round(sum(float(r.get(A7_COL, 0) or 0) for r in rows)))
    if pred_sum <= 0:
        raw = 1.0
    else:
        raw = actual_sum / pred_sum
    scale = max(lo, min(hi, raw))
    if down_only:
        scale = min(1.0, scale)
    return round(scale, 6), round(raw, 6), actual_sum, pred_sum


def add_scaled(row: Dict[str, Any], col: str, scale: float) -> None:
    row[col] = max(0, int(round(float(row.get(A7_COL, 0) or 0) * scale)))


def stage2_calibration(
    oos: Sequence[Dict[str, Any]],
    min_prior_oos_months: int = 2,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Apply fixed rolling calibration candidates to future Stage-1 OOS months."""
    out: List[Dict[str, Any]] = []
    scales: List[Dict[str, Any]] = []

    for hs in sorted({str(r["Horizon"]) for r in oos}):
        hrows = [
            r for r in oos
            if str(r["Horizon"]) == hs and r.get("可预测性") == "ESTABLISHED"
        ]
        months = sorted({str(r["目标月"]) for r in hrows})

        for idx in range(min_prior_oos_months, len(months)):
            test_month = months[idx]
            prior_months = months[:idx]
            test = [r for r in hrows if str(r["目标月"]) == test_month]
            if not test:
                continue

            def prior_rows(n: int | None) -> List[Dict[str, Any]]:
                use = prior_months if n is None else prior_months[-n:]
                use_set = set(use)
                return [r for r in hrows if str(r["目标月"]) in use_set]

            configs = [
                (A9_COL, prior_rows(2), 0.80, 1.20, False, "最近2个OOS月"),
                (A10_COL, prior_rows(3), 0.80, 1.20, False, "最近3个OOS月"),
                (A11_COL, prior_rows(3), 0.80, 1.00, True, "最近3个OOS月_只下调"),
                (A12_COL, prior_rows(None), 0.85, 1.00, True, "全部历史OOS月_只下调"),
            ]

            selected_scales: Dict[str, float] = {}
            for col, train, lo, hi, down_only, window_name in configs:
                scale, raw, train_actual, train_pred = calibration_scale(
                    train, lo=lo, hi=hi, down_only=down_only
                )
                selected_scales[col] = scale
                train_month_list = sorted({str(r["目标月"]) for r in train})
                scales.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "模型": col,
                    "窗口": window_name,
                    "训练OOS月份数": len(train_month_list),
                    "训练月份": ",".join(train_month_list),
                    "训练实际销量": train_actual,
                    "训练A7预测销量": train_pred,
                    "raw_scale": raw,
                    "最终scale": scale,
                    "下限": lo,
                    "上限": hi,
                    "只下调": 1 if down_only else 0,
                })

            for r in test:
                x = dict(r)
                for col, scale in selected_scales.items():
                    add_scaled(x, col, scale)
                out.append(x)

    return out, scales


def aggregate_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in sorted({str(r["Horizon"]) for r in rows}):
        hr = [r for r in rows if str(r["Horizon"]) == hs]
        for name, col in MODEL_COLS:
            out.append({"Horizon": hs, "模型": name, **base.metric(hr, col)})
    return out


def monthly_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in sorted({str(r["Horizon"]) for r in rows}):
        hr = [r for r in rows if str(r["Horizon"]) == hs]
        for month in sorted({str(r["目标月"]) for r in hr}):
            mr = [r for r in hr if str(r["目标月"]) == month]
            for name, col in MODEL_COLS:
                out.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(mr, col)})
    return out


def abc_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r["Horizon"]) == hs]
        for abc in sorted({str(r.get("ABC") or "UNKNOWN") for r in hr}):
            seg = [r for r in hr if str(r.get("ABC") or "UNKNOWN") == abc]
            if not seg:
                continue
            for name, col in MODEL_COLS:
                out.append({"Horizon": hs, "ABC": abc, "模型": name, **base.metric(seg, col)})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    ap.add_argument("--min-prior-oos-months", type=int, default=2)
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 10:
        raise RuntimeError("Round-8建议至少10个连续目标月，以保留严格二阶段OOS验证窗口")

    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建Stage-1 A0/A3/A5/A7 expanding walk-forward OOS...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r6.walk_forward_rows(detail, targets, args.max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)

    print("构建Stage-2 ESTABLISHED-only lagged aggregate calibration...")
    calibrated, scale_rows = stage2_calibration(oos, args.min_prior_oos_months)
    summary = base.norm(aggregate_summary(calibrated))
    monthly = base.norm(monthly_summary(calibrated))
    abc = base.norm(abc_summary(calibrated))
    scale_rows = base.norm(scale_rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第八轮老品滚动Bias校准_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_OOS总览.csv"), summary)
    output_fix.write_csv(root.with_name(root.name + "_OOS逐月.csv"), monthly)
    output_fix.write_csv(root.with_name(root.name + "_ABC.csv"), abc)
    output_fix.write_csv(root.with_name(root.name + "_scale轨迹.csv"), scale_rows)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("OOS总览", summary),
        ("OOS逐月", monthly),
        ("ABC", abc),
        ("scale轨迹", scale_rows),
    ])

    print("\n=== ESTABLISHED-only Stage-2 OOS 总览 ===")
    for r in summary:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== H2/H3 scale轨迹 ===")
    for r in scale_rows:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== H2/H3逐月 A7/A9/A10/A11/A12 ===")
    for r in monthly:
        if r["Horizon"] in ("H2", "H3") and r["模型"] != "A3_SPU生命周期收缩":
            print(r)

    print("\nExcel:", xlsx.resolve())
    print("判定：只有滚动校准在严格Stage-2 OOS中同时降低WAPE并显著收敛Bias，才值得进入V5-Existing候选。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
