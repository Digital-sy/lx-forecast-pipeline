#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-7: split forecastability and test a strict OOS demand-profile router.

Read-only experiment. Does not modify production tables or production forecast code.

Why this exists
---------------
Round-6 showed that H2/H3 aggregate negative bias is heavily driven by products
that had little or no sales history at the forecast snapshot. A sales-history
model cannot forecast a future launch that is not represented in its inputs.

This script therefore separates the business problem into:
1) ESTABLISHED: existing products with enough visible history for time-series logic.
2) NEW_VISIBLE: new products with <=3 months of visible sales history.
3) COLD_NO_HISTORY: products with no visible sales history at the snapshot.
4) DORMANT_RETURN: older products with no sales in the trailing 12 months but some
   older visible history.

It also tests a second-stage router on ESTABLISHED products only. The router is
strictly out-of-sample:
- Stage 1 creates the same expanding walk-forward A0/A3/A5/A7 predictions as round 6.
- Stage 2 waits for at least two completed Stage-1 OOS months.
- For each demand profile (Smooth/Erratic/Intermittent/Lumpy), it chooses among
  A0/A3/A5/A7 using ONLY prior Stage-1 OOS rows.
- A profile override must beat the horizon-wide fallback by a risk-adjusted score
  margin; otherwise the fallback is retained.

The purpose is diagnostic/model-selection evidence, not a production router.
"""
from __future__ import annotations

import argparse
import math
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

MODEL_COLS: List[Tuple[str, str]] = [
    ("A0_上月延续", "A0_上月延续"),
    ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
    ("A5_季节融合", "A5_季节融合"),
    ("A7_非对称季节门控", "A7_非对称季节门控"),
]
MODEL_BY_NAME = dict(MODEL_COLS)
ROUTER_COL = "A8_需求形态路由"


def classify_forecastability(r: Dict[str, Any]) -> str:
    lifecycle = str(r.get("生命周期") or "")
    profile = str(r.get("需求形态") or "")
    if lifecycle == "无历史":
        return "COLD_NO_HISTORY"
    if profile == "Dormant":
        return "DORMANT_RETURN"
    if lifecycle == "新品":
        return "NEW_VISIBLE"
    return "ESTABLISHED"


def enrich_forecastability(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        x["可预测性"] = classify_forecastability(x)
        out.append(x)
    return out


def score_rowset(rows: Sequence[Dict[str, Any]], model_name: str) -> Dict[str, Any]:
    col = MODEL_BY_NAME[model_name]
    return base.metric(rows, col)


def risk_score(m: Dict[str, Any], bias_weight: float = 0.25) -> float:
    w = m.get("WAPE")
    b = m.get("Bias%")
    if w is None:
        return math.inf
    return float(w) + bias_weight * abs(float(b or 0.0))


def choose_global_fallback(rows: Sequence[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
    scored: List[Dict[str, Any]] = []
    for name, _col in MODEL_COLS:
        m = score_rowset(rows, name)
        scored.append({"模型": name, "风险分": risk_score(m), **m})

    # Prefer catalog-wide calibration within +/-10%, then lowest WAPE.
    feasible = [x for x in scored if x.get("Bias%") is not None and abs(x["Bias%"]) <= 0.10]
    if feasible:
        best = min(feasible, key=lambda x: (x.get("WAPE", math.inf), abs(x.get("Bias%") or 0.0)))
    else:
        best = min(scored, key=lambda x: (x["风险分"], x.get("WAPE", math.inf)))
    return str(best["模型"]), scored


def choose_profile_model(
    rows: Sequence[Dict[str, Any]],
    fallback_model: str,
    min_records: int = 30,
    min_actual: int = 5000,
    override_margin: float = 0.02,
) -> Tuple[str, str, List[Dict[str, Any]]]:
    actual_sum = int(sum(float(r.get("实际销量", 0) or 0) for r in rows))
    if len(rows) < min_records or actual_sum < min_actual:
        return fallback_model, "样本不足_使用全局", []

    scored: List[Dict[str, Any]] = []
    for name, _col in MODEL_COLS:
        m = score_rowset(rows, name)
        scored.append({"模型": name, "风险分": risk_score(m), **m})

    fallback = next(x for x in scored if x["模型"] == fallback_model)
    best = min(scored, key=lambda x: (x["风险分"], x.get("WAPE", math.inf)))

    # Guardrail inspired by robust catalog-wide forecasting practice: do not switch
    # on tiny apparent gains. Challenger must improve risk-adjusted score materially.
    if best["模型"] != fallback_model and best["风险分"] <= fallback["风险分"] - override_margin:
        return str(best["模型"]), "分群覆盖", scored
    return fallback_model, "改善不足_使用全局", scored


def forecastability_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H0", "H1", "H2", "H3"):
        hr = [r for r in rows if r["Horizon"] == hs]
        if not hr:
            continue
        total_actual = sum(float(r.get("实际销量", 0) or 0) for r in hr)
        for group in ("ESTABLISHED", "NEW_VISIBLE", "DORMANT_RETURN", "COLD_NO_HISTORY"):
            seg = [r for r in hr if r["可预测性"] == group]
            if not seg:
                continue
            seg_actual = sum(float(r.get("实际销量", 0) or 0) for r in seg)
            for name, col in MODEL_COLS:
                out.append({
                    "Horizon": hs,
                    "可预测性": group,
                    "模型": name,
                    "记录数_分群": len(seg),
                    "实际销量占比_全目录": seg_actual / total_actual if total_actual > 0 else None,
                    **base.metric(seg, col),
                })
    return out


def second_stage_router(
    oos: Sequence[Dict[str, Any]],
    min_router_months: int = 2,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Train router only on prior Stage-1 OOS rows, then score next OOS month."""
    routed_rows: List[Dict[str, Any]] = []
    choices: List[Dict[str, Any]] = []
    fallback_scores: List[Dict[str, Any]] = []

    for hs in sorted({str(r["Horizon"]) for r in oos}):
        hrows = [r for r in oos if r["Horizon"] == hs]
        months = sorted({str(r["目标月"]) for r in hrows})
        for idx in range(min_router_months, len(months)):
            test_month = months[idx]
            train_months = set(months[:idx])
            train = [
                r for r in hrows
                if r["目标月"] in train_months and r["可预测性"] == "ESTABLISHED"
            ]
            test = [
                r for r in hrows
                if r["目标月"] == test_month and r["可预测性"] == "ESTABLISHED"
            ]
            if not train or not test:
                continue

            fallback_model, global_scores = choose_global_fallback(train)
            for s in global_scores:
                fallback_scores.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "训练OOS月份数": len(train_months),
                    "候选层级": "全局",
                    **s,
                })

            profile_choice: Dict[str, str] = {}
            for profile in sorted({str(r.get("需求形态") or "UNKNOWN") for r in test}):
                ptrain = [r for r in train if str(r.get("需求形态") or "UNKNOWN") == profile]
                selected, reason, scored = choose_profile_model(ptrain, fallback_model)
                profile_choice[profile] = selected
                choices.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "训练OOS月份数": len(train_months),
                    "需求形态": profile,
                    "全局回退模型": fallback_model,
                    "选择模型": selected,
                    "选择原因": reason,
                    "训练记录数": len(ptrain),
                    "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in ptrain)),
                })
                for s in scored:
                    fallback_scores.append({
                        "测试月": test_month,
                        "Horizon": hs,
                        "训练OOS月份数": len(train_months),
                        "候选层级": f"需求形态:{profile}",
                        **s,
                    })

            for r in test:
                x = dict(r)
                profile = str(r.get("需求形态") or "UNKNOWN")
                model = profile_choice.get(profile, fallback_model)
                col = MODEL_BY_NAME[model]
                x[ROUTER_COL] = int(r.get(col, 0) or 0)
                x["A8选择模型"] = model
                x["A8全局回退模型"] = fallback_model
                routed_rows.append(x)

    return routed_rows, choices, fallback_scores


def router_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in sorted({str(r["Horizon"]) for r in rows}):
        hr = [r for r in rows if r["Horizon"] == hs]
        if not hr:
            continue
        for name, col in MODEL_COLS + [(ROUTER_COL, ROUTER_COL)]:
            out.append({"Horizon": hs, "模型": name, **base.metric(hr, col)})
    return out


def router_monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in sorted({str(r["Horizon"]) for r in rows}):
        hr = [r for r in rows if r["Horizon"] == hs]
        for month in sorted({str(r["目标月"]) for r in hr}):
            mr = [r for r in hr if r["目标月"] == month]
            for name, col in MODEL_COLS + [(ROUTER_COL, ROUTER_COL)]:
                out.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(mr, col)})
    return out


def selection_mix(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[(str(r["Horizon"]), str(r.get("A8选择模型") or "UNKNOWN"))].append(r)
    for (hs, model), vals in sorted(groups.items()):
        out.append({
            "Horizon": hs,
            "选择模型": model,
            "记录数": len(vals),
            "实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in vals)),
            "预测销量": int(sum(float(r.get(ROUTER_COL, 0) or 0) for r in vals)),
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    ap.add_argument("--router-min-months", type=int, default=2)
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 10:
        raise RuntimeError("Round-7建议至少10个连续目标月，以保留二阶段OOS验证窗口")

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
    oos = enrich_forecastability(oos)

    decomp = base.norm(forecastability_summary(oos))

    print("构建Stage-2 需求形态路由，仅使用既有商品+历史Stage-1 OOS...")
    routed, route_choices, route_scores = second_stage_router(oos, args.router_min_months)
    rsummary = base.norm(router_summary(routed))
    rmonthly = base.norm(router_monthly(routed))
    mix = selection_mix(routed)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第七轮可预测性拆分与路由_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_可预测性拆分.csv"), decomp)
    output_fix.write_csv(root.with_name(root.name + "_路由OOS总览.csv"), rsummary)
    output_fix.write_csv(root.with_name(root.name + "_路由OOS逐月.csv"), rmonthly)
    output_fix.write_csv(root.with_name(root.name + "_路由选择.csv"), route_choices)
    output_fix.write_csv(root.with_name(root.name + "_路由训练候选.csv"), base.norm(route_scores))
    output_fix.write_csv(root.with_name(root.name + "_路由模型占比.csv"), mix)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("可预测性拆分", decomp),
        ("既有商品路由OOS", rsummary),
        ("路由逐月", rmonthly),
        ("路由选择", route_choices),
        ("路由训练候选", base.norm(route_scores)),
        ("路由模型占比", mix),
    ])

    print("\n=== Forecastability H2/H3 ===")
    for r in decomp:
        if r["Horizon"] in ("H2", "H3") and r["模型"] in ("A3_SPU生命周期收缩", "A7_非对称季节门控"):
            print(r)

    print("\n=== Existing-only Stage-2 Router OOS ===")
    for r in rsummary:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== Router choices H2/H3 ===")
    for r in route_choices:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== Router model mix ===")
    for r in mix:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\nExcel:", xlsx.resolve())
    print("判定：老品模型与新品/冷启动必须分开评估；A8只有在严格二阶段OOS中稳定优于全局模型才考虑生产路由。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
