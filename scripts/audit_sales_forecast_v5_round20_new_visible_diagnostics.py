#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-20: diagnose why NEW_VISIBLE under-forecasts, read-only.

Round-19 showed that median historical launch curves worsen NEW_VISIBLE forecasts:
A0/A3 already under-forecast heavily, while A26/A27 push forecasts even lower.
This round does NOT change production logic. It asks whether the missing demand is:
1) broad-based launch ramp that can be inferred from sales history; or
2) concentrated in a small set of post-snapshot breakout products that likely need
   launch-plan / inventory / traffic / advertising inputs.

Diagnostics are strictly based on information visible at each forecast snapshot, except
for clearly labeled realized/post-hoc fields used only to explain forecast error.

It also tests two diagnostic challengers (not production candidates):
- A29: historical same-age P75 launch ratio, category first then global fallback.
- A30: max(A3, A29), so a launch curve is allowed only to raise an under-ramped baseline.
These are used to measure whether upper-tail historical launch behavior contains useful
signal; they are not selected/tuned on the test month.
"""
from __future__ import annotations

import argparse
import math
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

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
from scripts import audit_sales_forecast_v5_round11_concentration_anomaly as r11
from scripts import audit_sales_forecast_v5_round19_launch_curve as r19

A0 = "A0_上月延续"
A3 = "A3_SPU生命周期收缩"
A29 = "A29_历史新品P75"
A30 = "A30_A3与P75取高"
MODELS = [(A0, A0), (A3, A3), (A29, A29), (A30, A30)]


def parse_month(v: Any) -> date:
    if isinstance(v, datetime):
        return date(v.year, v.month, 1)
    if isinstance(v, date):
        return date(v.year, v.month, 1)
    return datetime.strptime(str(v)[:7], "%Y-%m").date().replace(day=1)


def q(actual: Mapping[Tuple[str, date], int], spu: str, d: date) -> int:
    return int(actual.get((spu, d), 0) or 0)


def percentile(vals: Sequence[float], p: float) -> float | None:
    if not vals:
        return None
    s = sorted(float(x) for x in vals)
    if len(s) == 1:
        return s[0]
    pos = (len(s) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return s[lo]
    w = pos - lo
    return s[lo] * (1.0 - w) + s[hi] * w


def momentum_bucket(last_qty: int, prev_qty: int) -> str:
    if prev_qty <= 0:
        return "NO_PREV"
    x = last_qty / prev_qty
    if x < 0.70:
        return "FALLING_<0.70"
    if x < 1.20:
        return "FLAT_0.70_1.20"
    if x < 2.00:
        return "RISING_1.20_2.00"
    return "SURGE_>=2.00"


def level_bucket(v: int) -> str:
    if v < 50:
        return "<50"
    if v < 200:
        return "50-199"
    if v < 500:
        return "200-499"
    if v < 1000:
        return "500-999"
    if v < 3000:
        return "1000-2999"
    return "3000+"


def realized_type(actual_qty: int, last_qty: int) -> str:
    if last_qty <= 0:
        return "LAST_ZERO"
    ratio = actual_qty / last_qty
    if actual_qty >= 1000 and ratio >= 2.0:
        return "BIG_BREAKOUT_2X_1000+"
    if actual_qty >= 500 and ratio >= 2.0:
        return "BREAKOUT_2X_500+"
    if ratio >= 2.0:
        return "RAMP_>=2X"
    if ratio >= 1.25:
        return "GROWTH_1.25_2X"
    if ratio >= 0.75:
        return "STABLE_0.75_1.25"
    return "DECLINE_<0.75"


def build_new_visible(actual, targets: Sequence[date], max_horizon: int) -> Tuple[List[Dict[str, Any]], Mapping[str, date], Mapping[str, str]]:
    detail = r2.build_detail(actual, targets, max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r6.walk_forward_rows(detail, targets, max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    category_map = r9.load_spu_category_map()
    oos = r9.enrich_category(oos, category_map)
    first_sale = r11.first_sale_map(actual)
    rows = [
        r for r in oos
        if str(r.get("可预测性")) == "NEW_VISIBLE" and str(r.get("Horizon")) in ("H2", "H3")
    ]
    return rows, first_sale, category_map


def enrich_diagnostics(
    actual: Mapping[Tuple[str, date], int],
    rows: Sequence[Dict[str, Any]],
    first_sale: Mapping[str, date],
    category_map: Mapping[str, str],
    min_cat: int = 15,
    min_global: int = 30,
) -> List[Dict[str, Any]]:
    cache: Dict[Tuple[date, int, int, str | None], List[float]] = {}

    def ratios(snapshot: date, last_age: int, target_age: int, cat: str | None) -> List[float]:
        key = (snapshot, last_age, target_age, cat)
        if key not in cache:
            cache[key] = r19.analog_ratios(
                actual, first_sale, category_map, snapshot, last_age, target_age, cat
            )
        return cache[key]

    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        spu = str(r.get("SPU") or "")
        snapshot = parse_month(r.get("快照月"))
        target = parse_month(r.get("目标月"))
        f = first_sale.get(spu)
        cat = str(r.get("品类") or category_map.get(spu) or "未映射")
        last_d = base.add_months(snapshot, -1)
        prev_d = base.add_months(snapshot, -2)
        prev2_d = base.add_months(snapshot, -3)
        last_qty = q(actual, spu, last_d)
        prev_qty = q(actual, spu, prev_d)
        prev2_qty = q(actual, spu, prev2_d)
        actual_qty = int(r.get("实际销量", 0) or 0)

        last_age = None if f is None else r19.month_diff(f, last_d)
        target_age = None if f is None else r19.month_diff(f, target)
        cat_vals: List[float] = []
        global_vals: List[float] = []
        cat_p50 = cat_p75 = global_p50 = global_p75 = None
        if f is not None and last_age is not None and target_age is not None and last_age >= 0:
            cat_vals = ratios(snapshot, last_age, target_age, cat)
            global_vals = ratios(snapshot, last_age, target_age, None)
            if len(cat_vals) >= min_cat:
                cat_p50 = percentile(cat_vals, 0.50)
                cat_p75 = percentile(cat_vals, 0.75)
            if len(global_vals) >= min_global:
                global_p50 = percentile(global_vals, 0.50)
                global_p75 = percentile(global_vals, 0.75)

        p75 = cat_p75 if cat_p75 is not None else global_p75
        a3 = int(r.get(A3, 0) or 0)
        if last_qty > 0 and p75 is not None:
            p29 = max(0, int(round(last_qty * max(0.20, min(5.00, float(p75))))))
        else:
            p29 = a3
        p30 = max(a3, p29)

        x[A29] = p29
        x[A30] = p30
        x["首销月"] = None if f is None else f.strftime("%Y-%m")
        x["最后可见月"] = last_d.strftime("%Y-%m")
        x["最后可见月龄"] = last_age
        x["目标月龄"] = target_age
        x["M1销量"] = last_qty
        x["M2销量"] = prev_qty
        x["M3销量"] = prev2_qty
        x["M1_M2动量"] = None if prev_qty <= 0 else last_qty / prev_qty
        x["动量分组"] = momentum_bucket(last_qty, prev_qty)
        x["M1销量层"] = level_bucket(last_qty)
        x["实际相对M1倍率"] = None if last_qty <= 0 else actual_qty / last_qty
        x["事后走势类型"] = realized_type(actual_qty, last_qty)
        x["A3低估缺口"] = max(0, actual_qty - a3)
        x["类目analog数"] = len(cat_vals)
        x["全局analog数"] = len(global_vals)
        x["类目P50"] = cat_p50
        x["类目P75"] = cat_p75
        x["全局P50"] = global_p50
        x["全局P75"] = global_p75
        out.append(x)
    return out


def group_summary(rows: Sequence[Dict[str, Any]], field: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        total_gap = sum(float(r.get("A3低估缺口", 0) or 0) for r in hr)
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in hr:
            groups[str(r.get(field) if r.get(field) is not None else "UNKNOWN")].append(r)
        for key, seg in sorted(groups.items()):
            actual_sum = sum(float(r.get("实际销量", 0) or 0) for r in seg)
            gap = sum(float(r.get("A3低估缺口", 0) or 0) for r in seg)
            realized_mults = [float(r["实际相对M1倍率"]) for r in seg if r.get("实际相对M1倍率") is not None]
            row: Dict[str, Any] = {
                "Horizon": hs,
                "分组字段": field,
                "分组": key,
                "记录数": len(seg),
                "实际销量": int(actual_sum),
                "A3低估缺口": int(gap),
                "低估缺口占比": None if total_gap <= 0 else gap / total_gap,
                "实际/M1倍率中位数": None if not realized_mults else statistics.median(realized_mults),
                "实际/M1倍率P75": None if not realized_mults else percentile(realized_mults, 0.75),
            }
            for name, col in MODELS:
                m = base.metric(seg, col)
                row[f"{name}_WAPE"] = m.get("WAPE")
                row[f"{name}_Bias%"] = m.get("Bias%")
            out.append(row)
    return out


def overall(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [r for r in rows if str(r.get("Horizon")) == hs]
        for name, col in MODELS:
            out.append({"Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def concentration(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [r for r in rows if str(r.get("Horizon")) == hs]
        ranked = sorted(seg, key=lambda r: float(r.get("A3低估缺口", 0) or 0), reverse=True)
        total_gap = sum(float(r.get("A3低估缺口", 0) or 0) for r in ranked)
        total_actual = sum(float(r.get("实际销量", 0) or 0) for r in ranked)
        for n in (5, 10, 20, 50):
            top = ranked[:n]
            gap = sum(float(r.get("A3低估缺口", 0) or 0) for r in top)
            act = sum(float(r.get("实际销量", 0) or 0) for r in top)
            out.append({
                "Horizon": hs,
                "TopN": n,
                "低估缺口": int(gap),
                "低估缺口占比": None if total_gap <= 0 else gap / total_gap,
                "实际销量": int(act),
                "实际销量占比": None if total_actual <= 0 else act / total_actual,
            })
    return out


def top_errors(rows: Sequence[Dict[str, Any]], topn: int) -> List[Dict[str, Any]]:
    fields = [
        "Horizon", "目标月", "快照月", "SPU", "品类", "首销月", "最后可见月龄", "目标月龄",
        "M3销量", "M2销量", "M1销量", "M1_M2动量", "动量分组", "M1销量层",
        "实际销量", A0, A3, A29, A30, "A3低估缺口", "实际相对M1倍率", "事后走势类型",
        "类目analog数", "类目P50", "类目P75", "全局analog数", "全局P50", "全局P75",
    ]
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [r for r in rows if str(r.get("Horizon")) == hs]
        seg = sorted(seg, key=lambda r: float(r.get("A3低估缺口", 0) or 0), reverse=True)[:topn]
        for r in seg:
            out.append({k: r.get(k) for k in fields})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--topn", type=int, default=30)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -30)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建严格OOS NEW_VISIBLE诊断...")
    rows0, first_sale, category_map = build_new_visible(actual, targets, args.max_horizon)
    rows = enrich_diagnostics(actual, rows0, first_sale, category_map)
    print("NEW_VISIBLE rows:", len(rows))

    overall_rows = base.norm(overall(rows))
    grouped: List[Dict[str, Any]] = []
    for field in ("最后可见月龄", "动量分组", "M1销量层", "事后走势类型", "品类"):
        grouped.extend(group_summary(rows, field))
    group_rows = base.norm(grouped)
    conc_rows = base.norm(concentration(rows))
    top_rows = base.norm(top_errors(rows, args.topn))
    detail_rows = base.norm(rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第二十轮NEW_VISIBLE误差诊断_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), overall_rows)
    output_fix.write_csv(root.with_name(root.name + "_分组.csv"), group_rows)
    output_fix.write_csv(root.with_name(root.name + "_误差集中度.csv"), conc_rows)
    output_fix.write_csv(root.with_name(root.name + "_TOP低估.csv"), top_rows)
    output_fix.write_csv(root.with_name(root.name + "_明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("总览", overall_rows),
        ("分组", group_rows),
        ("误差集中度", conc_rows),
        ("TOP低估", top_rows),
        ("明细", detail_rows),
    ])

    print("\n=== NEW_VISIBLE A3 vs P75诊断challenger ===")
    for r in overall_rows:
        print(r)
    print("\n=== 误差集中度 ===")
    for r in conc_rows:
        print(r)
    print("\n=== 按事后走势类型 ===")
    for r in group_rows:
        if r.get("分组字段") == "事后走势类型":
            print(r)
    print("\n=== 按最后可见月龄 ===")
    for r in group_rows:
        if r.get("分组字段") == "最后可见月龄":
            print(r)
    print("\n=== 按快照动量 ===")
    for r in group_rows:
        if r.get("分组字段") == "动量分组":
            print(r)
    print("\n=== TOP低估SPU ===")
    for r in top_rows:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：若误差主要集中在快照后BIG_BREAKOUT，则停止用纯销量历史强行拟合NEW_VISIBLE，转向上新计划/库存/广告/流量先验；若广泛爬坡且P75可跨组改善，再做时序校准。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
