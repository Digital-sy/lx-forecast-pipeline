#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-4 diagnostics: locate where the seasonal candidate actually adds value.

Read-only. Does not modify production tables or production forecasting code.

Round-3 produced a mixed result: A5 improved walk-forward H2/H3 overall, but did
not improve the May-Aug fixed holdout. This script does not tune a new model.
Instead it decomposes the out-of-sample A5 effect by:
- target month / horizon
- seasonal-candidate coverage (row count and actual-sales share)
- lifecycle
- actual-sales bucket
- direction and magnitude of seasonal adjustment vs A3

Gamma selection is exactly the same leakage-free expanding walk-forward process
as round-3: each test month chooses gamma only from earlier target months.
"""
from __future__ import annotations

import argparse
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
from scripts import audit_sales_forecast_v5_experiments as exp
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round3 as r3

MODELS = [
    ("A0_上月延续", "A0_上月延续"),
    ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
    ("A5_季节融合", "A5_季节融合"),
]


def direction_bucket(a3: int, cand: int | None) -> str:
    if cand is None:
        return "无候选"
    if a3 <= 0:
        return "候选新增>0" if cand > 0 else "两者为0"
    ratio = cand / a3
    if ratio < 0.75:
        return "大幅下调<0.75"
    if ratio < 0.90:
        return "下调0.75-0.90"
    if ratio <= 1.10:
        return "基本持平0.90-1.10"
    if ratio <= 1.30:
        return "上调1.10-1.30"
    return "大幅上调>1.30"


def build_walkforward_oos(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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
            gamma, _ = r3.choose_gamma(train)
            selections.append({
                "测试月": test_month,
                "Horizon": hs,
                "训练月数": len(train_months),
                "gamma_seasonal": gamma,
            })
            work = r3.add_a5(test, gamma)
            for row in work:
                x = dict(row)
                cand = x.get("季节候选")
                a3 = int(x.get("A3_SPU生命周期收缩") or 0)
                x["walk_gamma_seasonal"] = gamma
                x["季节调整方向"] = direction_bucket(a3, cand)
                x["候选相对A3倍率"] = None if cand is None or a3 <= 0 else round(float(cand) / a3, 4)
                oos.append(x)
    return selections, oos


def model_metrics(rows: Sequence[Dict[str, Any]], prefix: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [{**prefix, "模型": name, **base.metric(rows, col)} for name, col in MODELS]


def coverage_row(rows: Sequence[Dict[str, Any]], prefix: Dict[str, Any]) -> Dict[str, Any]:
    eligible = [r for r in rows if int(r.get("有季节候选") or 0) == 1]
    actual_total = sum(int(r.get("实际销量") or 0) for r in rows)
    actual_eligible = sum(int(r.get("实际销量") or 0) for r in eligible)
    a3_total = sum(int(r.get("A3_SPU生命周期收缩") or 0) for r in rows)
    a3_eligible = sum(int(r.get("A3_SPU生命周期收缩") or 0) for r in eligible)
    return {
        **prefix,
        "记录数": len(rows),
        "候选记录数": len(eligible),
        "候选记录覆盖率": (len(eligible) / len(rows)) if rows else 0,
        "实际销量": actual_total,
        "候选覆盖实际销量": actual_eligible,
        "候选实际销量覆盖率": (actual_eligible / actual_total) if actual_total else 0,
        "A3预测销量": a3_total,
        "候选覆盖A3预测量": a3_eligible,
        "候选A3预测量覆盖率": (a3_eligible / a3_total) if a3_total else 0,
    }


def add_delta(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Add A5-vs-A3 WAPE/Bias deltas inside each grouping by pivoting model rows.
    groups: Dict[Tuple[Any, ...], Dict[str, Dict[str, Any]]] = defaultdict(dict)
    key_fields = [k for k in ("目标月", "Horizon", "生命周期", "销量层级", "季节调整方向") if any(k in r for r in rows)]
    for r in rows:
        key = tuple(r.get(k) for k in key_fields)
        groups[key][r["模型"]] = r
    out: List[Dict[str, Any]] = []
    for r in rows:
        if r["模型"] == "A5_季节融合":
            key = tuple(r.get(k) for k in key_fields)
            a3 = groups.get(key, {}).get("A3_SPU生命周期收缩")
            if a3:
                r = dict(r)
                if r.get("WAPE") is not None and a3.get("WAPE") is not None:
                    r["A5相对A3_WAPE改善"] = round(a3["WAPE"] - r["WAPE"], 6)
                if r.get("Bias%") is not None and a3.get("Bias%") is not None:
                    r["A5相对A3_Bias变化"] = round(r["Bias%"] - a3["Bias%"], 6)
        out.append(r)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    if len(targets) < 8:
        raise RuntimeError("本诊断至少需要8个连续目标月")
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)

    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r3.enrich_detail(actual, detail)

    selections, oos = build_walkforward_oos(detail, targets, args.max_horizon, 6)

    monthly: List[Dict[str, Any]] = []
    coverage: List[Dict[str, Any]] = []
    eligible_summary: List[Dict[str, Any]] = []
    lifecycle: List[Dict[str, Any]] = []
    sales_bucket_rows: List[Dict[str, Any]] = []
    direction: List[Dict[str, Any]] = []

    horizons = [f"H{i}" for i in range(args.max_horizon + 1)]
    months = sorted({r["目标月"] for r in oos})

    for h in horizons:
        hr = [r for r in oos if r["Horizon"] == h]
        coverage.append(coverage_row(hr, {"Horizon": h, "目标月": "ALL"}))
        elig = [r for r in hr if int(r.get("有季节候选") or 0) == 1]
        if elig:
            eligible_summary.extend(model_metrics(elig, {"Horizon": h, "范围": "仅季节候选覆盖SPU"}))

        for m in months:
            mr = [r for r in hr if r["目标月"] == m]
            if not mr:
                continue
            monthly.extend(model_metrics(mr, {"目标月": m, "Horizon": h}))
            coverage.append(coverage_row(mr, {"Horizon": h, "目标月": m}))

        for lc in sorted({r["生命周期"] for r in hr}):
            rr = [r for r in hr if r["生命周期"] == lc and int(r.get("有季节候选") or 0) == 1]
            if rr:
                lifecycle.extend(model_metrics(rr, {"Horizon": h, "生命周期": lc}))

        for bucket in ["0", "1-9", "10-49", "50-199", "200+"]:
            rr = [r for r in hr if r["销量层级"] == bucket and int(r.get("有季节候选") or 0) == 1]
            if rr:
                sales_bucket_rows.extend(model_metrics(rr, {"Horizon": h, "销量层级": bucket}))

        for d in ["大幅下调<0.75", "下调0.75-0.90", "基本持平0.90-1.10", "上调1.10-1.30", "大幅上调>1.30", "候选新增>0", "两者为0"]:
            rr = [r for r in hr if r["季节调整方向"] == d]
            if rr:
                direction.extend(model_metrics(rr, {"Horizon": h, "季节调整方向": d}))

    monthly = base.norm(add_delta(monthly))
    eligible_summary = base.norm(add_delta(eligible_summary))
    lifecycle = base.norm(add_delta(lifecycle))
    sales_bucket_rows = base.norm(add_delta(sales_bucket_rows))
    direction = base.norm(add_delta(direction))
    coverage = base.norm(coverage)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第四轮季节诊断_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_walk参数.csv"), selections)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), monthly)
    output_fix.write_csv(root.with_name(root.name + "_候选覆盖.csv"), coverage)
    output_fix.write_csv(root.with_name(root.name + "_候选SPU总体.csv"), eligible_summary)
    output_fix.write_csv(root.with_name(root.name + "_生命周期.csv"), lifecycle)
    output_fix.write_csv(root.with_name(root.name + "_销量层级.csv"), sales_bucket_rows)
    output_fix.write_csv(root.with_name(root.name + "_调整方向.csv"), direction)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), oos)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("walk参数", selections),
        ("逐月", monthly),
        ("候选覆盖", coverage),
        ("候选SPU总体", eligible_summary),
        ("生命周期", lifecycle),
        ("销量层级", sales_bucket_rows),
        ("调整方向", direction),
        ("OOS明细", oos),
    ])

    print("\n=== 候选覆盖率（OOS总体） ===")
    for r in coverage:
        if r["目标月"] == "ALL":
            print(r)

    print("\n=== H2/H3逐月 OOS：A0/A3/A5 ===")
    for r in monthly:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== H2/H3 仅季节候选覆盖SPU ===")
    for r in eligible_summary:
        if r["Horizon"] in ("H2", "H3"):
            print(r)

    print("\n=== H2/H3 季节调整方向 ===")
    for r in direction:
        if r["Horizon"] in ("H2", "H3") and r["模型"] in ("A3_SPU生命周期收缩", "A5_季节融合"):
            print(r)

    print("\nExcel:", xlsx.resolve())
    print("说明：本轮只诊断A5在哪些场景有效，不产生新的生产候选公式。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
