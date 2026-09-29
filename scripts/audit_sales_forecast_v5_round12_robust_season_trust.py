#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-12: robust seasonal-trust gates for ESTABLISHED SPU forecasts.

Read-only experiment. No production tables or production forecast code are modified.

Motivation
----------
Round-11 showed several distinct failure modes behind prior-year seasonal lifts:
1) SPU-level launch windows / isolated spikes can make last year's target month look
   repeatable when it was not.
2) Category total can rise while only a minority of continuing SPUs rise (poor breadth).
3) Some categories are extremely concentrated, so a category trend is effectively the
   history of one or a few SPUs.

This round NEVER uses category trend to increase an SPU forecast. Category evidence
may only veto or attenuate an A7 UP_RISING lift.

Candidates (all fixed before evaluation):
- A7: existing asymmetric seasonal gate.
- A14: block A7 UP_RISING if that SPU's prior-year target month is a launch-window or
       isolated-spike anomaly.
- A15: A14 + block UP_RISING when a sufficiently-sized robust category cohort does not
       show broad-based rising evidence.
- A16: A15 + when category concentration is high but broad rising evidence exists,
       keep only 50% of the A7 uplift above A3 (attenuation, never extra uplift).

Important caveat
----------------
Current product_category is treated as static diagnostic/product metadata, not as a
historical transactional feature. Productionization should use a stable canonical
SPU->category mapping.
"""
from __future__ import annotations

import argparse
import statistics
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
from scripts import audit_sales_forecast_v5_round7_router as r7
from scripts import audit_sales_forecast_v5_round9_category_diagnostics as r9
from scripts import audit_sales_forecast_v5_round10_current_momentum_veto as r10
from scripts import audit_sales_forecast_v5_round11_concentration_anomaly as r11

A3 = "A3_SPU生命周期收缩"
A7 = "A7_非对称季节门控"
A14 = "A14_SPU异常历史否决"
A15 = "A15_稳健类目广度否决"
A16 = "A16_广度否决_高集中半衰减"

MODELS: List[Tuple[str, str]] = [
    (A3, A3),
    (A7, A7),
    (A14, A14),
    (A15, A15),
    (A16, A16),
]


def category_support(
    actual: Dict[Tuple[str, date], int],
    first_sale: Dict[str, date],
    rows: Sequence[Dict[str, Any]],
    min_valid_spus: int = 5,
) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    """Build leakage-free robust prior-year category evidence for each target/horizon.

    The category signal is deliberately unweighted at the decision layer: it uses the
    median SPU ratio and breadth after excluding SPU prior-year anomalies. Aggregate
    concentration is measured separately and can only reduce trust.
    """
    grouped: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        grouped[(str(r["目标月"]), str(r["Horizon"]), str(r.get("品类") or "未映射"))].append(r)

    out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for key, seg in grouped.items():
        month, hs, cat = key
        target = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        target_vals: List[int] = []
        ratios: List[float] = []
        anomaly_count = 0
        anomaly_sales = 0
        raw_target = 0

        for r in seg:
            spu = str(r.get("SPU") or "")
            f = r11.spu_history_features(actual, first_sale, spu, target)
            cur = int(f.get("LY目标销量") or 0)
            prev = int(f.get("LY前1月") or 0)
            raw_target += cur
            target_vals.append(cur)
            if f.get("LY异常类型") != "NORMAL":
                anomaly_count += 1
                anomaly_sales += cur
                continue
            if prev < 10 or cur < 10:
                continue
            ratios.append(max(0.50, min(2.00, cur / prev)))

        cr1, cr3, cr5, hhi = r11.concentration(target_vals)
        valid = len(ratios)
        median_ratio = statistics.median(ratios) if ratios else None
        breadth = None if valid <= 0 else sum(1 for x in ratios if x >= 1.05) / valid
        broad_rising = (
            valid >= min_valid_spus
            and median_ratio is not None and median_ratio >= 1.05
            and breadth is not None and breadth >= 0.55
        )
        sufficient = valid >= min_valid_spus
        concentration_risk = (
            (cr1 is not None and cr1 >= 0.50)
            or (hhi is not None and hhi >= 0.25)
        )
        out[key] = {
            "有效SPU数": valid,
            "SPU环比中位数": median_ratio,
            "上涨SPU广度": breadth,
            "类目广泛上涨": 1 if broad_rising else 0,
            "类目信号充分": 1 if sufficient else 0,
            "CR1": cr1,
            "CR3": cr3,
            "CR5": cr5,
            "HHI": hhi,
            "集中度风险": 1 if concentration_risk else 0,
            "异常SPU数": anomaly_count,
            "异常SPU_LY销量占比": None if raw_target <= 0 else anomaly_sales / raw_target,
        }
    return out


def apply_trust_gates(
    actual: Dict[Tuple[str, date], int],
    first_sale: Dict[str, date],
    rows: Sequence[Dict[str, Any]],
    cat_support: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r.get(A3, 0) or 0)
        a7 = float(r.get(A7, 0) or 0)
        rule = str(r.get("A7规则") or "")
        month = str(r.get("目标月"))
        hs = str(r.get("Horizon"))
        cat = str(r.get("品类") or "未映射")
        target = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        spu_f = r11.spu_history_features(actual, first_sale, str(r.get("SPU") or ""), target)
        c = cat_support.get((month, hs, cat), {})

        p14 = a7
        p15 = a7
        p16 = a7
        r14 = "KEEP_A7"
        r15 = "KEEP_A7"
        r16 = "KEEP_A7"

        if rule == "UP_RISING":
            anomaly = str(spu_f.get("LY异常类型") or "NORMAL")
            if anomaly != "NORMAL":
                p14 = p15 = p16 = a3
                r14 = f"BLOCK_{anomaly}"
                r15 = r14
                r16 = r14
            else:
                sufficient = bool(c.get("类目信号充分", 0))
                broad_rising = bool(c.get("类目广泛上涨", 0))
                concentrated = bool(c.get("集中度风险", 0))
                r14 = "ALLOW_NORMAL_SPU"

                if sufficient and not broad_rising:
                    p15 = p16 = a3
                    r15 = "BLOCK_CATEGORY_NOT_BROAD"
                    r16 = r15
                else:
                    r15 = "ALLOW_CATEGORY_BROAD" if sufficient else "KEEP_SMALL_COHORT"
                    if concentrated:
                        p16 = a3 + 0.50 * max(0.0, a7 - a3)
                        r16 = "HALF_UPLIFT_HIGH_CONCENTRATION"
                    else:
                        r16 = r15

        x[A14] = max(0, int(round(p14)))
        x[A15] = max(0, int(round(p15)))
        x[A16] = max(0, int(round(p16)))
        x["A14规则"] = r14
        x["A15规则"] = r15
        x["A16规则"] = r16
        x["LY异常类型"] = spu_f.get("LY异常类型")
        for k, v in c.items():
            x[f"类目_{k}"] = v
        out.append(x)
    return out


def summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for name, col in MODELS:
            out.append({"Horizon": hs, "模型": name, **base.metric(hr, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            mr = [r for r in hr if str(r.get("目标月")) == month]
            for name, col in MODELS:
                out.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(mr, col)})
    return out


def rule_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        total_actual = sum(float(r.get("实际销量", 0) or 0) for r in hr)
        for model, rule_col in ((A14, "A14规则"), (A15, "A15规则"), (A16, "A16规则")):
            for rule in sorted({str(r.get(rule_col) or "UNKNOWN") for r in hr}):
                seg = [r for r in hr if str(r.get(rule_col) or "UNKNOWN") == rule]
                actual_sum = sum(float(r.get("实际销量", 0) or 0) for r in seg)
                out.append({
                    "Horizon": hs,
                    "模型": model,
                    "规则": rule,
                    "记录数": len(seg),
                    "实际销量占比": None if total_actual <= 0 else actual_sum / total_actual,
                    **base.metric(seg, model),
                })
    return out


def july_category(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs and str(r.get("目标月")) == "2026-07"]
        cats = sorted({str(r.get("品类") or "未映射") for r in hr})
        for cat in cats:
            seg = [r for r in hr if str(r.get("品类") or "未映射") == cat]
            row: Dict[str, Any] = {"Horizon": hs, "品类": cat, "记录数": len(seg)}
            for name, col in MODELS[1:]:
                m = base.metric(seg, col)
                row[f"{name}_预测"] = m.get("预测销量")
                row[f"{name}_Bias%"] = m.get("Bias%")
                row[f"{name}_WAPE"] = m.get("WAPE")
            sample = seg[0] if seg else {}
            for k in (
                "类目_有效SPU数",
                "类目_SPU环比中位数",
                "类目_上涨SPU广度",
                "类目_类目广泛上涨",
                "类目_CR1",
                "类目_HHI",
                "类目_集中度风险",
                "类目_异常SPU_LY销量占比",
            ):
                row[k] = sample.get(k)
            out.append(row)
    out.sort(key=lambda x: (x["Horizon"], -(abs(float(x.get(f"{A7}_Bias%") or 0)))))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 8:
        raise RuntimeError("Round-12至少需要8个连续目标月")

    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)
    first_sale = r11.first_sale_map(actual)

    print("构建A7 expanding walk-forward OOS，并限制为ESTABLISHED H2/H3...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r10.walk_forward_oos(actual, detail, targets, args.max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    rows = [r for r in oos if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")]

    print("叠加品类标签，并构建稳健类目季节可信度（只做否决/降权）...")
    rows = r9.enrich_category(rows, r9.load_spu_category_map())
    support = category_support(actual, first_sale, rows)
    rows = apply_trust_gates(actual, first_sale, rows, support)

    s = base.norm(summary(rows))
    m = base.norm(monthly(rows))
    rs = base.norm(rule_summary(rows))
    jc = base.norm(july_category(rows))

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十二轮稳健季节可信度_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_OOS总览.csv"), s)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), m)
    output_fix.write_csv(root.with_name(root.name + "_规则分解.csv"), rs)
    output_fix.write_csv(root.with_name(root.name + "_7月品类.csv"), jc)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("OOS总览", s),
        ("逐月", m),
        ("规则分解", rs),
        ("7月品类", jc),
    ])

    print("\n=== ESTABLISHED H2/H3 OOS 总览 ===")
    for r in s:
        print(r)
    print("\n=== H2/H3逐月 A7/A14/A15/A16 ===")
    for r in m:
        if r["模型"] in (A7, A14, A15, A16):
            print(r)
    print("\n=== 规则分解 ===")
    for r in rs:
        print(r)
    print("\n=== 2026-07 品类 A7/A14/A15/A16 ===")
    for r in jc:
        if abs(float(r.get(f"{A7}_Bias%") or 0)) >= 0.20:
            print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：只有在多个OOS月份同时降低WAPE且Bias不恶化，才接受更复杂的季节可信度门控。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
