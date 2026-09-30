#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-16: temporal calibration of A3 baseline corrections, read-only.

Round-15 proved that raw trajectory corrections contain signal but fixed/full-strength
rules over-correct. This experiment learns only the BLEND STRENGTH from prior target
months, separately by horizon and correction rule.

No production tables/code are modified.
No future target-month data is used to select lambda.

Models
------
A3  : existing lifecycle baseline.
A24 : per-rule temporal calibration. For each test month/horizon/rule, choose lambda
      in [0,1] using only earlier target months, then blend:
          pred = A3 + lambda * (A22_raw_candidate - A3)
A25 : same calibration, but ONLY WEAK_MOMENTUM is allowed to change A3. This isolates
      whether STRICT_DECLINE adds stable value after temporal calibration.

Selection
---------
Grid lambda = 0.0..1.0 step 0.1.
If any lambda has abs(Bias)<=10%, choose lowest WAPE (tie: lower abs Bias).
Otherwise choose lowest WAPE + 0.5*abs(Bias).
If the rule has insufficient prior evidence, lambda=0.
"""
from __future__ import annotations

import argparse
import math
import sys
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
from scripts import audit_sales_forecast_v5_round15_baseline_trajectory as r15

A3 = "A3_SPU生命周期收缩"
RAW = "A22_再加弱动量"
A24 = "A24_分规则时序校准"
A25 = "A25_仅弱动量时序校准"
RULE = "基线修正规则"

RULES = ("SPIKE_REANCHOR", "STRICT_DECLINE", "WEAK_MOMENTUM", "KEEP_A3")


def build_all_rows(actual, targets, max_horizon: int) -> List[Dict[str, Any]]:
    detail = r2.build_detail(actual, targets, max_horizon)
    detail = r5.enrich(actual, detail)
    detail = r6.enrich_segments(actual, detail)
    detail = r7.enrich_forecastability(detail)
    rows = [
        r for r in detail
        if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")
    ]
    rows = r9.enrich_category(rows, r9.load_spu_category_map())
    return r15.add_candidates(actual, rows)


def blend_value(row: Dict[str, Any], lam: float) -> int:
    a3 = float(row.get(A3, 0) or 0)
    raw = float(row.get(RAW, a3) or 0)
    return max(0, int(round(a3 + lam * (raw - a3))))


def metric_lambda(rows: Sequence[Dict[str, Any]], lam: float) -> Dict[str, Any]:
    work: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        x["_blend"] = blend_value(r, lam)
        work.append(x)
    return {"lambda": round(lam, 2), **base.metric(work, "_blend")}


def choose_lambda(
    rows: Sequence[Dict[str, Any]],
    min_records: int = 50,
    min_actual: int = 5000,
) -> Tuple[float, str, List[Dict[str, Any]]]:
    actual_sum = sum(float(r.get("实际销量", 0) or 0) for r in rows)
    if len(rows) < min_records or actual_sum < min_actual:
        return 0.0, "样本不足", []

    scored = [metric_lambda(rows, i / 10.0) for i in range(11)]
    feasible = [
        x for x in scored
        if x.get("Bias%") is not None and abs(float(x.get("Bias%") or 0.0)) <= 0.10
    ]
    if feasible:
        best = min(
            feasible,
            key=lambda x: (
                float(x.get("WAPE") if x.get("WAPE") is not None else math.inf),
                abs(float(x.get("Bias%") or 0.0)),
                float(x["lambda"]),
            ),
        )
        return float(best["lambda"]), "Bias<=10%后最小WAPE", scored

    best = min(
        scored,
        key=lambda x: (
            float(x.get("WAPE") if x.get("WAPE") is not None else math.inf)
            + 0.5 * abs(float(x.get("Bias%") or 0.0)),
            float(x["lambda"]),
        ),
    )
    return float(best["lambda"]), "风险分最小", scored


def temporal_apply(
    rows: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    target_text = [d.strftime("%Y-%m") for d in targets]
    out: List[Dict[str, Any]] = []
    choices: List[Dict[str, Any]] = []
    grid_rows: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(target_text)):
        test_month = target_text[test_idx]
        prior = set(target_text[:test_idx])

        for hs in ("H2", "H3"):
            htrain = [r for r in rows if str(r.get("Horizon")) == hs and str(r.get("目标月")) in prior]
            htest = [r for r in rows if str(r.get("Horizon")) == hs and str(r.get("目标月")) == test_month]

            lambdas: Dict[str, float] = {"KEEP_A3": 0.0}
            for rule in ("SPIKE_REANCHOR", "STRICT_DECLINE", "WEAK_MOMENTUM"):
                train_seg = [r for r in htrain if str(r.get(RULE) or "KEEP_A3") == rule]
                lam, reason, grid = choose_lambda(train_seg)
                lambdas[rule] = lam
                choices.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "规则": rule,
                    "训练月份": ",".join(sorted(prior)),
                    "训练记录数": len(train_seg),
                    "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in train_seg)),
                    "lambda": lam,
                    "选择原因": reason,
                })
                for g in grid:
                    grid_rows.append({"测试月": test_month, "Horizon": hs, "规则": rule, **g})

            for r in htest:
                x = dict(r)
                rule = str(r.get(RULE) or "KEEP_A3")
                lam = float(lambdas.get(rule, 0.0))
                x[A24] = blend_value(r, lam)
                x["A24_lambda"] = lam
                x["A24规则"] = rule

                # Isolate the most promising generalizable rule from Round-15.
                weak_lam = float(lambdas.get("WEAK_MOMENTUM", 0.0)) if rule == "WEAK_MOMENTUM" else 0.0
                x[A25] = blend_value(r, weak_lam)
                x["A25_lambda"] = weak_lam
                out.append(x)

    return out, choices, grid_rows


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
            for name, col in [(A3, A3), ("A23_固定半修正", "A23_弱动量半修正"), (A24, A24), (A25, A25)]:
                out.append({"范围": range_name, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for range_name, rr in [("全部ESTABLISHED", list(rows)), ("T恤", [r for r in rows if str(r.get("品类") or "") == "T恤"])]:
        for hs in ("H2", "H3"):
            hr = [r for r in rr if str(r.get("Horizon")) == hs]
            for month in sorted({str(r.get("目标月")) for r in hr}):
                mr = [r for r in hr if str(r.get("目标月")) == month]
                for name, col in [(A3, A3), (A24, A24), (A25, A25)]:
                    out.append({"范围": range_name, "Horizon": hs, "目标月": month, "模型": name, **base.metric(mr, col)})
    return out


def rule_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for rule in RULES:
            seg = [r for r in hr if str(r.get(RULE) or "KEEP_A3") == rule]
            if not seg:
                continue
            for name, col in [(A3, A3), (A24, A24), (A25, A25)]:
                out.append({"Horizon": hs, "规则": rule, "模型": name, **base.metric(seg, col)})
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
    sales_end = base.add_months(max(targets), 1)

    print("读取店内销量:", history_start, "~", sales_end)
    sales_rows = base.read_sales(history_start, sales_end)
    actual = base.actual_spu_month(sales_rows)

    print("构建全部目标月 ESTABLISHED H2/H3 基线候选...")
    all_rows = build_all_rows(actual, targets, args.max_horizon)
    print("按测试月仅使用此前月份选择各规则 lambda...")
    oos, choices, grids = temporal_apply(all_rows, targets, 6)

    summary_rows = base.norm(summary(oos))
    monthly_rows = base.norm(monthly(oos))
    rule_rows = base.norm(rule_summary(oos))
    choice_rows = base.norm(choices)
    grid_rows = base.norm(grids)
    detail_rows = base.norm(oos)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十六轮A3时序校准_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), summary_rows)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), monthly_rows)
    output_fix.write_csv(root.with_name(root.name + "_规则分解.csv"), rule_rows)
    output_fix.write_csv(root.with_name(root.name + "_lambda选择.csv"), choice_rows)
    output_fix.write_csv(root.with_name(root.name + "_lambda网格.csv"), grid_rows)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("总览", summary_rows),
        ("逐月", monthly_rows),
        ("规则分解", rule_rows),
        ("lambda选择", choice_rows),
        ("lambda网格", grid_rows),
        ("OOS明细", detail_rows),
    ])

    print("\n=== A3 vs A23固定半修正 vs A24/A25时序校准 ===")
    for r in summary_rows:
        print(r)
    print("\n=== 每月/每Horizon/每规则 lambda ===")
    for r in choice_rows:
        print(r)
    print("\n=== 规则分解 ===")
    for r in rule_rows:
        print(r)
    print("\n=== 逐月 ===")
    for r in monthly_rows:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：若A24/A25在全部ESTABLISHED和ABC_A跨月改善且Bias受控，再把新基线接回A7/A16；否则继续保留A3。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
