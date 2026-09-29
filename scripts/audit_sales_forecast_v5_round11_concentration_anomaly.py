#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-11: category concentration + historical anomaly diagnostics.

Read-only diagnostic. No production tables or forecast logic are modified.

Hypothesis
----------
A category aggregate can show a strong prior-year rise because a few dominant SPUs
had promotions, launch ramps, or isolated spikes. Treating that aggregate move as
repeatable category seasonality would contaminate the rest of the category.

This round does NOT add a forecast model. It measures, leakage-free at each snapshot:
- category concentration: CR1 / CR3 / CR5 / HHI on prior-year target-month sales;
- trend breadth: how many continuing SPUs were actually rising;
- robust category trend after excluding launch-window / isolated-spike SPUs;
- per-SPU prior-year anomaly flags using surrounding historical months;
- whether a raw category RISING signal is contradicted by robust/breadth evidence.

Current product_category is diagnostic metadata only, as in round-9/10.
"""
from __future__ import annotations

import argparse
import math
import statistics
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
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5
from scripts import audit_sales_forecast_v5_round6_segments as r6
from scripts import audit_sales_forecast_v5_round7_router as r7
from scripts import audit_sales_forecast_v5_round9_category_diagnostics as r9
from scripts import audit_sales_forecast_v5_round10_current_momentum_veto as r10

A7 = "A7_非对称季节门控"


def months_between(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def first_sale_map(actual: Dict[Tuple[str, date], int]) -> Dict[str, date]:
    out: Dict[str, date] = {}
    for (spu, d), qty in actual.items():
        if int(qty or 0) <= 0:
            continue
        if spu not in out or d < out[spu]:
            out[spu] = d
    return out


def qty(actual: Dict[Tuple[str, date], int], spu: str, d: date) -> int:
    return max(0, int(actual.get((spu, d), 0) or 0))


def phase(ratio: float | None) -> str:
    if ratio is None:
        return "UNKNOWN"
    if ratio >= 1.05:
        return "RISING"
    if ratio <= 0.95:
        return "FALLING"
    return "FLAT"


def spu_history_features(
    actual: Dict[Tuple[str, date], int],
    first_sale: Dict[str, date],
    spu: str,
    target: date,
) -> Dict[str, Any]:
    ly = base.add_months(target, -12)
    vals = {
        "m_2": qty(actual, spu, base.add_months(ly, -2)),
        "m_1": qty(actual, spu, base.add_months(ly, -1)),
        "m0": qty(actual, spu, ly),
        "p_1": qty(actual, spu, base.add_months(ly, 1)),
        "p_2": qty(actual, spu, base.add_months(ly, 2)),
    }
    neigh = [vals["m_2"], vals["m_1"], vals["p_1"], vals["p_2"]]
    med = float(statistics.median(neigh)) if neigh else 0.0
    spike_ratio = None if med <= 0 else vals["m0"] / med
    prev_ratio = None if vals["m_1"] <= 0 else vals["m0"] / vals["m_1"]
    fs = first_sale.get(spu)
    launch_age = None if fs is None or fs > ly else months_between(fs, ly)
    launch_window = launch_age is not None and 0 <= launch_age <= 2

    isolated = False
    if vals["m0"] >= 100:
        if med > 0:
            isolated = (
                vals["m0"] >= 1.80 * med
                and vals["m0"] >= 1.30 * max(vals["m_1"], vals["p_1"], 1)
            )
        elif max(neigh) == 0:
            isolated = True

    anomaly = "NORMAL"
    if launch_window:
        anomaly = "LY_LAUNCH_WINDOW"
    elif isolated:
        anomaly = "LY_ISOLATED_SPIKE"

    return {
        "LY目标销量": vals["m0"],
        "LY前2月": vals["m_2"],
        "LY前1月": vals["m_1"],
        "LY后1月": vals["p_1"],
        "LY后2月": vals["p_2"],
        "LY邻月中位数": med,
        "LY异常倍数": spike_ratio,
        "LY目标环比": prev_ratio,
        "LY阶段": phase(prev_ratio),
        "LY距首销月数": launch_age,
        "LY异常类型": anomaly,
    }


def concentration(values: Sequence[int]) -> Tuple[float | None, float | None, float | None, float | None]:
    vals = sorted((max(0, int(v)) for v in values), reverse=True)
    total = sum(vals)
    if total <= 0:
        return None, None, None, None
    shares = [v / total for v in vals]
    cr1 = sum(shares[:1])
    cr3 = sum(shares[:3])
    cr5 = sum(shares[:5])
    hhi = sum(s * s for s in shares)
    return cr1, cr3, cr5, hhi


def category_diagnostics(
    actual: Dict[Tuple[str, date], int],
    first_sale: Dict[str, date],
    rows: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    by_cat: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_cat[(str(r["目标月"]), str(r["Horizon"]), str(r.get("品类") or "未映射"))].append(r)

    cat_out: List[Dict[str, Any]] = []
    spu_out: List[Dict[str, Any]] = []

    for (month, hs, cat), seg in sorted(by_cat.items()):
        target = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        features: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
        for r in seg:
            f = spu_history_features(actual, first_sale, str(r["SPU"]), target)
            features.append((r, f))
            spu_out.append({
                "目标月": month,
                "Horizon": hs,
                "品类": cat,
                "SPU": r.get("SPU"),
                "ABC": r.get("ABC"),
                "生命周期": r.get("生命周期"),
                "实际销量": r.get("实际销量"),
                "A7预测": r.get(A7),
                "A7有符号误差": int(round(float(r.get(A7, 0) or 0) - float(r.get("实际销量", 0) or 0))),
                **f,
            })

        ly_target_vals = [int(f["LY目标销量"] or 0) for _r, f in features]
        ly_prev_vals = [int(f["LY前1月"] or 0) for _r, f in features]
        cr1, cr3, cr5, hhi = concentration(ly_target_vals)
        raw_target = sum(ly_target_vals)
        raw_prev = sum(ly_prev_vals)
        raw_ratio = None if raw_prev <= 0 else raw_target / raw_prev

        robust = [(r, f) for r, f in features if f["LY异常类型"] == "NORMAL"]
        robust_target = sum(int(f["LY目标销量"] or 0) for _r, f in robust)
        robust_prev = sum(int(f["LY前1月"] or 0) for _r, f in robust)
        robust_ratio = None if robust_prev <= 0 else robust_target / robust_prev

        ratios: List[float] = []
        rising = 0
        valid = 0
        for _r, f in features:
            prev = int(f["LY前1月"] or 0)
            cur = int(f["LY目标销量"] or 0)
            if prev < 10 or cur < 10:
                continue
            rr = max(0.30, min(3.00, cur / prev))
            ratios.append(rr)
            valid += 1
            if rr >= 1.05:
                rising += 1
        median_ratio = statistics.median(ratios) if ratios else None
        breadth = None if valid <= 0 else rising / valid

        anomaly_spus = [f for _r, f in features if f["LY异常类型"] != "NORMAL"]
        anomaly_sales = sum(int(f["LY目标销量"] or 0) for f in anomaly_spus)
        anomaly_share = None if raw_target <= 0 else anomaly_sales / raw_target

        raw_phase = phase(raw_ratio)
        robust_phase = phase(robust_ratio)
        false_rise = (
            raw_phase == "RISING"
            and (
                robust_phase != "RISING"
                or (breadth is not None and breadth < 0.50)
                or (median_ratio is not None and median_ratio < 1.05)
            )
        )
        concentration_risk = (cr3 is not None and cr3 >= 0.50) or (hhi is not None and hhi >= 0.15)

        metric = base.metric(seg, A7)
        cat_out.append({
            "目标月": month,
            "Horizon": hs,
            "品类": cat,
            "SPU数": len(seg),
            "LY类目总销量": raw_target,
            "LY类目前月总销量": raw_prev,
            "LY原始环比": raw_ratio,
            "LY原始阶段": raw_phase,
            "去异常后销量": robust_target,
            "去异常后前月销量": robust_prev,
            "去异常后环比": robust_ratio,
            "去异常后阶段": robust_phase,
            "SPU环比中位数": median_ratio,
            "上涨SPU广度": breadth,
            "有效SPU数": valid,
            "异常SPU数": len(anomaly_spus),
            "异常SPU_LY销量占比": anomaly_share,
            "CR1": cr1,
            "CR3": cr3,
            "CR5": cr5,
            "HHI": hhi,
            "集中度风险": 1 if concentration_risk else 0,
            "假上涨风险": 1 if false_rise else 0,
            "A7实际销量": metric.get("实际销量"),
            "A7预测销量": metric.get("预测销量"),
            "A7_Bias%": metric.get("Bias%"),
            "A7_WAPE": metric.get("WAPE"),
        })

    return cat_out, spu_out


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
    first_sale = first_sale_map(actual)

    print("构建A7 expanding walk-forward OOS，仅保留ESTABLISHED H2/H3...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r10.walk_forward_oos(actual, detail, targets, args.max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    rows = [r for r in oos if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")]

    print("叠加当前品类标签（仅诊断）并测量集中度/异常历史/趋势广度...")
    rows = r9.enrich_category(rows, r9.load_spu_category_map())
    cat_diag, spu_diag = category_diagnostics(actual, first_sale, rows)

    cat_diag = base.norm(cat_diag)
    spu_diag = base.norm(spu_diag)
    july_cat = [r for r in cat_diag if r["目标月"] == "2026-07"]
    july_cat.sort(key=lambda x: (x["Horizon"], -(float(x.get("A7_Bias%") or 0))))
    july_spu = [r for r in spu_diag if r["目标月"] == "2026-07" and r["LY异常类型"] != "NORMAL"]
    july_spu.sort(key=lambda x: (x["Horizon"], -abs(int(x.get("A7有符号误差") or 0))))

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十一轮集中度与异常历史诊断_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_品类诊断.csv"), cat_diag)
    output_fix.write_csv(root.with_name(root.name + "_SPU异常诊断.csv"), spu_diag)
    output_fix.write_csv(root.with_name(root.name + "_7月品类风险.csv"), july_cat)
    output_fix.write_csv(root.with_name(root.name + "_7月异常SPU.csv"), july_spu)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("品类诊断", cat_diag),
        ("SPU异常诊断", spu_diag),
        ("7月品类风险", july_cat),
        ("7月异常SPU", july_spu),
    ])

    print("\n=== 2026-07 H2/H3 集中度与假上涨风险 ===")
    for r in july_cat:
        if r.get("集中度风险") or r.get("假上涨风险"):
            print(r)

    print("\n=== 2026-07 异常历史SPU TOP ===")
    for r in july_spu[:40]:
        print(r)

    print("\nExcel:", xlsx.resolve())
    print("判定：若高估品类同时表现为高CR3/HHI、异常SPU占比高、或原始RISING但去异常/广度不支持，则后续季节信号必须改为robust cohort，而非类目总量。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
