#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-10: current-momentum veto for A7 upward seasonal lifts.

Read-only experiment. Does not modify production forecast tables or production logic.

Why this exists
---------------
Round-9 showed July overforecast is concentrated in a few high-volume categories.
For several of those categories, current-year recent demand had already weakened at
the forecast snapshot while last year's target-month phase was still RISING.

A7 currently allows an upward seasonal lift when last year's target month was RISING,
but it does not check whether current-year recent demand is already weakening.

This round isolates that one hypothesis with FIXED veto thresholds:
- A13_t080: block A7 UP_RISING when SPU m1 / mean(m2,m3) < 0.80
- A13_t090: same threshold 0.90
- A13_t100: same threshold 1.00

The A7 gamma_down/gamma_up parameters are still selected walk-forward from prior
target months only. The veto thresholds are fixed before evaluation and are NOT
selected on test months.

Primary evaluation is ESTABLISHED-only. Category labels are appended only for
diagnostics and are not used by A13.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_experiments as exp
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5
from scripts import audit_sales_forecast_v5_round6_segments as r6
from scripts import audit_sales_forecast_v5_round7_router as r7
from scripts import audit_sales_forecast_v5_round9_category_diagnostics as r9

A7 = "A7_非对称季节门控"
A13_080 = "A13_当前趋势否决_t080"
A13_090 = "A13_当前趋势否决_t090"
A13_100 = "A13_当前趋势否决_t100"

MODELS: List[Tuple[str, str]] = [
    ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
    (A7, A7),
    (A13_080, A13_080),
    (A13_090, A13_090),
    (A13_100, A13_100),
]


def current_momentum(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
) -> Tuple[float | None, int, int, int]:
    """Leakage-free SPU momentum using three fully completed months before snapshot."""
    m1 = max(0, exp.history_qty(actual, spu, base.add_months(snapshot, -1)))
    m2 = max(0, exp.history_qty(actual, spu, base.add_months(snapshot, -2)))
    m3 = max(0, exp.history_qty(actual, spu, base.add_months(snapshot, -3)))
    denom = (m2 + m3) / 2.0
    ratio = None if denom <= 0 else m1 / denom
    return ratio, m1, m2, m3


def enrich_current_momentum(
    actual: Dict[Tuple[str, date], int],
    rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        snapshot = datetime.strptime(str(r["快照月"]), "%Y-%m").date().replace(day=1)
        ratio, m1, m2, m3 = current_momentum(actual, str(r["SPU"]), snapshot)
        x["当前趋势比_M1_vs_M2M3"] = ratio
        x["当前M1"] = m1
        x["当前M2"] = m2
        x["当前M3"] = m3
        x["当前3月销量"] = m1 + m2 + m3
        out.append(x)
    return out


def add_a13(
    rows: Sequence[Dict[str, Any]],
    gamma_down: float,
    gamma_up: float,
    threshold: float,
    col: str,
    up_cap_ratio: float = 1.50,
    min_recent_total: int = 30,
) -> List[Dict[str, Any]]:
    """A7 plus a current-year momentum veto on UP_RISING only."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r.get("A3_SPU生命周期收缩", 0) or 0)
        cand = r.get("季节候选")
        phase = str(r.get("历史季节阶段") or "UNKNOWN")
        mom = r.get("当前趋势比_M1_vs_M2M3")
        recent_total = int(r.get("当前3月销量", 0) or 0)

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
                if phase != "RISING":
                    pred = a3
                    rule = "UP_BLOCKED_LY_PHASE"
                elif (
                    mom is not None
                    and recent_total >= min_recent_total
                    and float(mom) < threshold
                ):
                    pred = a3
                    rule = "UP_VETO_CURRENT_WEAK"
                else:
                    effective = min(cand_f, a3 * up_cap_ratio)
                    pred = (1.0 - gamma_up) * a3 + gamma_up * effective
                    rule = "UP_RISING_ALLOWED"

        x[col] = max(0, int(round(pred)))
        x[f"{col}_规则"] = rule
        out.append(x)
    return out


def walk_forward_oos(
    actual: Dict[Tuple[str, date], int],
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    target_text = [d.strftime("%Y-%m") for d in targets]
    choices: List[Dict[str, Any]] = []
    oos: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(targets)):
        test_month = target_text[test_idx]
        train_months = set(target_text[:test_idx])

        for h in range(max_horizon + 1):
            hs = f"H{h}"
            train = [
                r for r in detail
                if r["Horizon"] == hs and r["目标月"] in train_months
            ]
            test = [
                r for r in detail
                if r["Horizon"] == hs and r["目标月"] == test_month
            ]
            gd, gu, _ = r5.choose_params(train)
            choices.append({
                "测试月": test_month,
                "Horizon": hs,
                "训练月数": len(train_months),
                "gamma_down": gd,
                "gamma_up": gu,
            })

            a7 = r5.add_a7(test, gd, gu)
            e = enrich_current_momentum(actual, test)
            v080 = add_a13(e, gd, gu, 0.80, A13_080)
            v090 = add_a13(e, gd, gu, 0.90, A13_090)
            v100 = add_a13(e, gd, gu, 1.00, A13_100)

            for x7, x80, x90, x100 in zip(a7, v080, v090, v100):
                x = dict(x100)
                x[A7] = x7[A7]
                x["A7规则"] = x7["A7规则"]
                x[A13_080] = x80[A13_080]
                x[f"{A13_080}_规则"] = x80[f"{A13_080}_规则"]
                x[A13_090] = x90[A13_090]
                x[f"{A13_090}_规则"] = x90[f"{A13_090}_规则"]
                oos.append(x)

    return choices, oos


def summarize(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H0", "H1", "H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        if not hr:
            continue
        for name, col in MODELS:
            out.append({"Horizon": hs, "模型": name, **base.metric(hr, col)})
    return out


def monthly_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            mr = [r for r in hr if str(r.get("目标月")) == month]
            for name, col in MODELS:
                out.append({
                    "目标月": month,
                    "Horizon": hs,
                    "模型": name,
                    **base.metric(mr, col),
                })
    return out


def rule_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        total_actual = sum(float(r.get("实际销量", 0) or 0) for r in hr)
        for col in (A13_080, A13_090, A13_100):
            rule_col = f"{col}_规则"
            for rule in sorted({str(r.get(rule_col) or "UNKNOWN") for r in hr}):
                seg = [r for r in hr if str(r.get(rule_col) or "UNKNOWN") == rule]
                if not seg:
                    continue
                actual_sum = sum(float(r.get("实际销量", 0) or 0) for r in seg)
                out.append({
                    "Horizon": hs,
                    "模型": col,
                    "规则": rule,
                    "记录数": len(seg),
                    "实际销量占比": None if total_actual <= 0 else actual_sum / total_actual,
                    **base.metric(seg, col),
                })
    return out


def july_category_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Current category labels for diagnosis only; not a model feature."""
    cat_map = r9.load_spu_category_map()
    work = r9.enrich_category(rows, cat_map)
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [
            r for r in work
            if str(r.get("Horizon")) == hs and str(r.get("目标月")) == "2026-07"
        ]
        cats = sorted({str(r.get("品类") or "未映射") for r in hr})
        for cat in cats:
            seg = [r for r in hr if str(r.get("品类") or "未映射") == cat]
            a7_err = sum(float(r.get(A7, 0) or 0) - float(r.get("实际销量", 0) or 0) for r in seg)
            row: Dict[str, Any] = {
                "Horizon": hs,
                "品类": cat,
                "记录数": len(seg),
                "A7有符号误差": int(round(a7_err)),
            }
            for name, col in MODELS[1:]:
                m = base.metric(seg, col)
                row[f"{name}_预测"] = m.get("预测销量")
                row[f"{name}_Bias%"] = m.get("Bias%")
                row[f"{name}_WAPE"] = m.get("WAPE")
            out.append(row)
    out.sort(key=lambda x: (x["Horizon"], -x["A7有符号误差"]))
    return out


def july_top_spu(rows: Sequence[Dict[str, Any]], n: int = 30) -> List[Dict[str, Any]]:
    cat_map = r9.load_spu_category_map()
    work = r9.enrich_category(rows, cat_map)
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [
            r for r in work
            if str(r.get("Horizon")) == hs and str(r.get("目标月")) == "2026-07"
        ]
        ranked = sorted(
            hr,
            key=lambda r: float(r.get(A7, 0) or 0) - float(r.get("实际销量", 0) or 0),
            reverse=True,
        )
        for rank, r in enumerate(ranked[:n], 1):
            out.append({
                "Horizon": hs,
                "排名": rank,
                "SPU": r.get("SPU"),
                "品类": r.get("品类"),
                "ABC": r.get("ABC"),
                "生命周期": r.get("生命周期"),
                "实际销量": r.get("实际销量"),
                "A7": r.get(A7),
                A13_080: r.get(A13_080),
                A13_090: r.get(A13_090),
                A13_100: r.get(A13_100),
                "当前趋势比_M1_vs_M2M3": r.get("当前趋势比_M1_vs_M2M3"),
                "A7规则": r.get("A7规则"),
                "A13_t090规则": r.get(f"{A13_090}_规则"),
            })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 8:
        raise RuntimeError("Round-10至少需要8个连续目标月")

    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建A3/季节候选，并进行A7 expanding walk-forward...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    choices, oos = walk_forward_oos(actual, detail, targets, args.max_horizon, 6)

    print("只评估ESTABLISHED商品；品类仅用于7月诊断...")
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    est = [r for r in oos if r.get("可预测性") == "ESTABLISHED"]

    summary = base.norm(summarize(est))
    monthly = base.norm(monthly_summary(est))
    rules = base.norm(rule_summary(est))
    july_cat = base.norm(july_category_summary(est))
    july_spu = base.norm(july_top_spu(est))

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十轮当前趋势否决_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_OOS总览.csv"), summary)
    output_fix.write_csv(root.with_name(root.name + "_OOS逐月.csv"), monthly)
    output_fix.write_csv(root.with_name(root.name + "_规则拆分.csv"), rules)
    output_fix.write_csv(root.with_name(root.name + "_7月品类.csv"), july_cat)
    output_fix.write_csv(root.with_name(root.name + "_7月SPU.csv"), july_spu)
    output_fix.write_csv(root.with_name(root.name + "_walkforward参数.csv"), choices)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("OOS总览", summary),
        ("OOS逐月", monthly),
        ("规则拆分", rules),
        ("7月品类", july_cat),
        ("7月SPU", july_spu),
        ("walkforward参数", choices),
    ])

    print("\n=== ESTABLISHED OOS 总览 ===")
    for r in summary:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== H2/H3逐月 A7/A13 ===")
    for r in monthly:
        print(r)

    print("\n=== 2026-07 品类：A7 vs A13 ===")
    for r in july_cat[:30]:
        print(r)

    print("\n=== 2026-07 SPU TOP：当前趋势否决效果 ===")
    for r in july_spu[:40]:
        print(r)

    print("\nExcel:", xlsx.resolve())
    print("判定：只有固定当前趋势否决在多个OOS月份降低H2/H3 WAPE，且不是只修复2026-07，才考虑进入V5-Existing。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
