#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-21: strict temporal router for NEW_VISIBLE P75 uplift.

Read-only experiment. No production forecast tables or production code are modified.

Round-20 showed two simultaneous facts:
1) NEW_VISIBLE under-forecast is concentrated in a relatively small breakout cohort;
2) historical P75 launch uplift helps SURGE/RISING cohorts more than FLAT/FALLING cohorts,
   while applying it indiscriminately can worsen accuracy.

This round therefore does NOT hard-code the Round-20 hindsight result. For each test target
month and horizon, it uses ONLY earlier NEW_VISIBLE OOS target months to choose how much
of the upward-only P75 candidate (A30=max(A3,A29)) should be blended into A3.

Two challengers are tested:
- A31: one temporal lambda per horizon, learned from prior OOS months.
- A32: temporal lambda by pre-snapshot momentum bucket; if a bucket has insufficient
       prior OOS support, fall back to A31's global lambda.

Lambda grid: 0, .25, .50, .75, 1.00.
Selection objective: lowest prior-OOS WAPE, tie-break lower absolute Bias. A non-zero
lambda must improve prior-OOS WAPE by at least --min-improve versus lambda=0, otherwise
it is rejected. This keeps P75 as an uplift candidate, never as a mandatory rule.
"""
from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_round20_new_visible_diagnostics as r20

A3 = r20.A3
A30 = r20.A30
A31 = "A31_全局时序P75强度"
A32 = "A32_动量时序P75强度"
GRID = (0.0, 0.25, 0.50, 0.75, 1.00)
MODELS = [(A3, A3), (A30, A30), (A31, A31), (A32, A32)]


def blended_value(row: Mapping[str, Any], lam: float) -> int:
    a3 = float(row.get(A3, 0) or 0)
    up = max(0.0, float(row.get(A30, a3) or a3) - a3)
    return max(0, int(round(a3 + lam * up)))


def rows_with_lambda(rows: Sequence[Dict[str, Any]], lam: float, col: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        x[col] = blended_value(r, lam)
        out.append(x)
    return out


def score_lambda(rows: Sequence[Dict[str, Any]], lam: float) -> Dict[str, Any]:
    col = "_tmp_lambda_pred"
    rr = rows_with_lambda(rows, lam, col)
    return {"lambda": lam, **base.metric(rr, col)}


def choose_lambda(
    rows: Sequence[Dict[str, Any]],
    min_improve: float,
) -> Tuple[float, str, List[Dict[str, Any]]]:
    if not rows:
        return 0.0, "无训练样本", []
    scored = [score_lambda(rows, lam) for lam in GRID]
    baseline = next(x for x in scored if float(x["lambda"]) == 0.0)
    best = min(
        scored,
        key=lambda x: (
            float(x.get("WAPE") if x.get("WAPE") is not None else math.inf),
            abs(float(x.get("Bias%") or 0.0)),
            float(x.get("lambda") or 0.0),
        ),
    )
    base_wape = float(baseline.get("WAPE") if baseline.get("WAPE") is not None else math.inf)
    best_wape = float(best.get("WAPE") if best.get("WAPE") is not None else math.inf)
    if float(best["lambda"]) > 0 and best_wape <= base_wape - min_improve:
        return float(best["lambda"]), "历史OOS_WAPE显著改善", scored
    return 0.0, "改善不足_保持A3", scored


def build_temporal_router(
    rows: Sequence[Dict[str, Any]],
    router_min_months: int,
    min_records: int,
    min_actual: int,
    min_improve: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    routed: List[Dict[str, Any]] = []
    choices: List[Dict[str, Any]] = []
    grids: List[Dict[str, Any]] = []

    for hs in ("H2", "H3"):
        hrows = [r for r in rows if str(r.get("Horizon")) == hs]
        months = sorted({str(r.get("目标月")) for r in hrows})
        for idx in range(router_min_months, len(months)):
            test_month = months[idx]
            prior_months = set(months[:idx])
            train = [r for r in hrows if str(r.get("目标月")) in prior_months]
            test = [r for r in hrows if str(r.get("目标月")) == test_month]
            if not train or not test:
                continue

            global_lam, global_reason, global_grid = choose_lambda(train, min_improve)
            global_base = base.metric(train, A3)
            global_selected = score_lambda(train, global_lam)
            choices.append({
                "测试月": test_month,
                "Horizon": hs,
                "层级": "GLOBAL",
                "动量分组": "ALL",
                "训练OOS月份数": len(prior_months),
                "训练记录数": len(train),
                "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in train)),
                "选择lambda": global_lam,
                "选择原因": global_reason,
                "A3_WAPE": global_base.get("WAPE"),
                "A3_Bias%": global_base.get("Bias%"),
                "选择后_WAPE": global_selected.get("WAPE"),
                "选择后_Bias%": global_selected.get("Bias%"),
            })
            for g in global_grid:
                grids.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "层级": "GLOBAL",
                    "动量分组": "ALL",
                    **g,
                })

            segment_lambda: Dict[str, Tuple[float, str]] = {}
            for momentum in sorted({str(r.get("动量分组") or "UNKNOWN") for r in test}):
                seg_train = [r for r in train if str(r.get("动量分组") or "UNKNOWN") == momentum]
                seg_actual = int(sum(float(r.get("实际销量", 0) or 0) for r in seg_train))
                if len(seg_train) >= min_records and seg_actual >= min_actual:
                    lam, reason, seg_grid = choose_lambda(seg_train, min_improve)
                    used_reason = reason
                    for g in seg_grid:
                        grids.append({
                            "测试月": test_month,
                            "Horizon": hs,
                            "层级": "MOMENTUM",
                            "动量分组": momentum,
                            **g,
                        })
                else:
                    lam = global_lam
                    used_reason = "样本不足_回退GLOBAL"
                    seg_grid = []

                segment_lambda[momentum] = (lam, used_reason)
                m0 = base.metric(seg_train, A3) if seg_train else {}
                ms = score_lambda(seg_train, lam) if seg_train else {}
                choices.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "层级": "MOMENTUM",
                    "动量分组": momentum,
                    "训练OOS月份数": len(prior_months),
                    "训练记录数": len(seg_train),
                    "训练实际销量": seg_actual,
                    "选择lambda": lam,
                    "选择原因": used_reason,
                    "A3_WAPE": m0.get("WAPE"),
                    "A3_Bias%": m0.get("Bias%"),
                    "选择后_WAPE": ms.get("WAPE"),
                    "选择后_Bias%": ms.get("Bias%"),
                })

            for r in test:
                x = dict(r)
                momentum = str(r.get("动量分组") or "UNKNOWN")
                seg_lam, seg_reason = segment_lambda.get(momentum, (global_lam, "未见分组_回退GLOBAL"))
                x[A31] = blended_value(r, global_lam)
                x[A32] = blended_value(r, seg_lam)
                x["A31_lambda"] = global_lam
                x["A32_lambda"] = seg_lam
                x["A32选择原因"] = seg_reason
                routed.append(x)

    return routed, choices, grids


def summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [r for r in rows if str(r.get("Horizon")) == hs]
        for name, col in MODELS:
            out.append({"Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hrows = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hrows}):
            seg = [r for r in hrows if str(r.get("目标月")) == month]
            for name, col in MODELS:
                out.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def by_momentum(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[(str(r.get("Horizon")), str(r.get("动量分组") or "UNKNOWN"))].append(r)
    for (hs, momentum), seg in sorted(groups.items()):
        row: Dict[str, Any] = {
            "Horizon": hs,
            "动量分组": momentum,
            "记录数": len(seg),
            "实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in seg)),
        }
        for name, col in MODELS:
            m = base.metric(seg, col)
            row[f"{name}_WAPE"] = m.get("WAPE")
            row[f"{name}_Bias%"] = m.get("Bias%")
        out.append(row)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--router-min-months", type=int, default=2)
    ap.add_argument("--min-records", type=int, default=20)
    ap.add_argument("--min-actual", type=int, default=5000)
    ap.add_argument("--min-improve", type=float, default=0.005)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -30)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建Round-20严格OOS NEW_VISIBLE特征...")
    new_rows, first_sale, category_map = r20.build_new_visible(actual, targets, args.max_horizon)
    enriched = r20.enrich_diagnostics(actual, new_rows, first_sale, category_map)

    print("构建Round-21 expanding temporal router，仅使用更早OOS目标月选择lambda...")
    routed, choices, grids = build_temporal_router(
        enriched,
        router_min_months=args.router_min_months,
        min_records=args.min_records,
        min_actual=args.min_actual,
        min_improve=args.min_improve,
    )
    if not routed:
        raise RuntimeError("没有可评估的Round-21 OOS行；请降低 --router-min-months 或增加目标月份")

    summary_rows = base.norm(summary(routed))
    monthly_rows = base.norm(monthly(routed))
    momentum_rows = base.norm(by_momentum(routed))
    choice_rows = base.norm(choices)
    grid_rows = base.norm(grids)
    detail_rows = base.norm(routed)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第二十一轮NEW_VISIBLE时序动量路由_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), summary_rows)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), monthly_rows)
    output_fix.write_csv(root.with_name(root.name + "_动量分组.csv"), momentum_rows)
    output_fix.write_csv(root.with_name(root.name + "_参数选择.csv"), choice_rows)
    output_fix.write_csv(root.with_name(root.name + "_lambda网格.csv"), grid_rows)
    output_fix.write_csv(root.with_name(root.name + "_明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("总览", summary_rows),
        ("逐月", monthly_rows),
        ("动量分组", momentum_rows),
        ("参数选择", choice_rows),
        ("lambda网格", grid_rows),
        ("明细", detail_rows),
    ])

    print("\n=== Round21 NEW_VISIBLE严格时序总览 ===")
    for r in summary_rows:
        print(r)
    print("\n=== Round21 逐月 ===")
    for r in monthly_rows:
        print(r)
    print("\n=== Round21 动量分组 ===")
    for r in momentum_rows:
        print(r)
    print("\n=== Round21 参数选择 ===")
    for r in choice_rows:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：A32只有在严格时序OOS下同时改善H2/H3 WAPE，且不是靠单月/单分组偶然收益，才进入NEW_VISIBLE候选。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
