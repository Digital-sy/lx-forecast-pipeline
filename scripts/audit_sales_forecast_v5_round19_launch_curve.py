#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-19: leakage-safe launch-curve experiment for NEW_VISIBLE products.

Read-only experiment. No production tables or forecast code are modified.

Why this round exists
---------------------
Round-18 froze A16 as the ESTABLISHED champion. Remaining catalog error is structurally
separate: products with only 1-3 visible selling months (NEW_VISIBLE), and products with
no visible sales at the forecast snapshot (COLD_NO_HISTORY).

This script tests whether NEW_VISIBLE demand can be forecast from historical launch
curves that were fully observable before each forecast snapshot.

For every NEW_VISIBLE OOS row:
- locate its first visible selling month F;
- last visible month is snapshot-1;
- compute current visible age a and target age b;
- collect historical analog SPUs whose F+a and F+b months are BOTH strictly before
  the current snapshot;
- estimate the median ratio qty(age=b) / qty(age=a), first within current category,
  then fall back to catalog-wide same-age analogs when category sample is too small.

Models:
A0  : last visible complete month.
A3  : existing lifecycle baseline.
A26 : category launch-curve ratio (fallback global), applied to last visible month.
A27 : 50% shrink of A26 toward A3.
A28 : global launch-curve ratio only.

COLD_NO_HISTORY is NOT forecast from future information. It is only diagnosed using the
known realized first-sale date after the fact, to quantify how much future demand was
unforecastable from sales history alone and therefore needs launch-plan inputs.
"""
from __future__ import annotations

import argparse
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

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
from scripts import audit_sales_forecast_v5_round11_concentration_anomaly as r11

A0 = "A0_上月延续"
A3 = "A3_SPU生命周期收缩"
A26 = "A26_类目新品曲线"
A27 = "A27_新品曲线半收缩"
A28 = "A28_全局新品曲线"
MODELS = [(A0, A0), (A3, A3), (A26, A26), (A27, A27), (A28, A28)]


def parse_month(v: Any) -> date:
    if isinstance(v, datetime):
        return date(v.year, v.month, 1)
    if isinstance(v, date):
        return date(v.year, v.month, 1)
    return datetime.strptime(str(v)[:7], "%Y-%m").date().replace(day=1)


def month_diff(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def qty(actual: Mapping[Tuple[str, date], int], spu: str, d: date) -> int:
    return int(actual.get((spu, d), 0) or 0)


def add_months(d: date, n: int) -> date:
    return base.add_months(d, n)


def analog_ratios(
    actual: Mapping[Tuple[str, date], int],
    first_sale: Mapping[str, date],
    category_map: Mapping[str, str],
    snapshot: date,
    last_age: int,
    target_age: int,
    category: str | None,
    min_den: int = 5,
) -> List[float]:
    """Historical ratios known strictly before snapshot, optionally category filtered."""
    vals: List[float] = []
    cutoff = add_months(snapshot, -1)
    for spu, f in first_sale.items():
        if category is not None and str(category_map.get(spu) or "未映射") != category:
            continue
        den_d = add_months(f, last_age)
        num_d = add_months(f, target_age)
        # Both points must have been completed before the current forecast snapshot.
        if den_d > cutoff or num_d > cutoff:
            continue
        den = qty(actual, spu, den_d)
        num = qty(actual, spu, num_d)
        if den < min_den:
            continue
        vals.append(max(0.0, min(5.0, num / den)))
    return vals


def robust_ratio(vals: Sequence[float]) -> float | None:
    if not vals:
        return None
    med = statistics.median(vals)
    return max(0.20, min(3.00, float(med)))


def add_launch_predictions(
    actual: Mapping[Tuple[str, date], int],
    first_sale: Mapping[str, date],
    category_map: Mapping[str, str],
    rows: Sequence[Dict[str, Any]],
    min_category_analogs: int = 15,
    min_global_analogs: int = 30,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        spu = str(r.get("SPU") or "")
        snapshot = parse_month(r.get("快照月"))
        target = parse_month(r.get("目标月"))
        cat = str(r.get("品类") or category_map.get(spu) or "未映射")
        f = first_sale.get(spu)
        last_visible_d = add_months(snapshot, -1)
        last_qty = qty(actual, spu, last_visible_d)

        if f is None or f >= snapshot:
            # NEW_VISIBLE should not normally land here; fail closed to A3.
            x[A26] = int(r.get(A3, 0) or 0)
            x[A27] = int(r.get(A3, 0) or 0)
            x[A28] = int(r.get(A3, 0) or 0)
            x["新品曲线规则"] = "NO_VISIBLE_FIRST_SALE"
            x["新品可见月龄"] = None
            x["新品目标月龄"] = None
            x["类目analog数"] = 0
            x["全局analog数"] = 0
            out.append(x)
            continue

        last_age = month_diff(f, last_visible_d)
        target_age = month_diff(f, target)
        cat_vals = analog_ratios(actual, first_sale, category_map, snapshot, last_age, target_age, cat)
        global_vals = analog_ratios(actual, first_sale, category_map, snapshot, last_age, target_age, None)
        cat_ratio = robust_ratio(cat_vals) if len(cat_vals) >= min_category_analogs else None
        global_ratio = robust_ratio(global_vals) if len(global_vals) >= min_global_analogs else None

        a3 = float(r.get(A3, 0) or 0)
        if last_qty <= 0:
            p26 = p28 = a3
            rule = "LAST_VISIBLE_ZERO_FALLBACK_A3"
        else:
            if cat_ratio is not None:
                p26 = last_qty * cat_ratio
                rule = "CATEGORY_ANALOG"
            elif global_ratio is not None:
                p26 = last_qty * global_ratio
                rule = "GLOBAL_FALLBACK"
            else:
                p26 = a3
                rule = "INSUFFICIENT_ANALOG_FALLBACK_A3"
            p28 = last_qty * global_ratio if global_ratio is not None else a3

        p26i = max(0, int(round(p26)))
        p28i = max(0, int(round(p28)))
        p27i = max(0, int(round(0.50 * a3 + 0.50 * p26i)))
        x[A26] = p26i
        x[A27] = p27i
        x[A28] = p28i
        x["新品曲线规则"] = rule
        x["新品首销月"] = f.strftime("%Y-%m")
        x["新品可见月龄"] = last_age
        x["新品目标月龄"] = target_age
        x["快照最后月销量"] = last_qty
        x["类目analog数"] = len(cat_vals)
        x["全局analog数"] = len(global_vals)
        x["类目ratio"] = cat_ratio
        x["全局ratio"] = global_ratio
        out.append(x)
    return out


def metrics_by_scope(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    scopes = [
        ("全部NEW_VISIBLE", list(rows)),
        ("ABC_A", [r for r in rows if str(r.get("ABC") or "") == "A"]),
    ]
    cats = sorted({str(r.get("品类") or "未映射") for r in rows})
    for cat in cats:
        seg = [r for r in rows if str(r.get("品类") or "未映射") == cat]
        if sum(float(r.get("实际销量", 0) or 0) for r in seg) >= 5000:
            scopes.append((f"品类:{cat}", seg))

    for scope, rr in scopes:
        for hs in ("H2", "H3"):
            seg = [r for r in rr if str(r.get("Horizon")) == hs]
            if not seg:
                continue
            for name, col in MODELS:
                out.append({"范围": scope, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            seg = [r for r in hr if str(r.get("目标月")) == month]
            for name, col in MODELS:
                out.append({"目标月": month, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def rule_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for rule in sorted({str(r.get("新品曲线规则") or "UNKNOWN") for r in hr}):
            seg = [r for r in hr if str(r.get("新品曲线规则") or "UNKNOWN") == rule]
            row = {"Horizon": hs, "规则": rule, "记录数": len(seg), "实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in seg))}
            for name, col in MODELS:
                m = base.metric(seg, col)
                row[f"{name}_WAPE"] = m.get("WAPE")
                row[f"{name}_Bias%"] = m.get("Bias%")
            out.append(row)
    return out


def cold_diagnostic(
    rows: Sequence[Dict[str, Any]],
    first_sale_full: Mapping[str, date],
) -> List[Dict[str, Any]]:
    """Post-hoc only: realized launch timing. Never used as a forecast feature."""
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        spu = str(r.get("SPU") or "")
        snapshot = parse_month(r.get("快照月"))
        target = parse_month(r.get("目标月"))
        f = first_sale_full.get(spu)
        if f is None:
            label = "NO_REALIZED_FIRST_SALE"
        elif f < snapshot:
            label = "HISTORICAL_BUT_INVISIBLE_WINDOW"
        elif f <= target:
            label = "LAUNCH_BETWEEN_SNAPSHOT_TARGET"
        else:
            label = "LAUNCH_AFTER_TARGET"
        groups[(str(r.get("Horizon")), label)].append(r)

    out: List[Dict[str, Any]] = []
    for (hs, label), seg in sorted(groups.items()):
        out.append({
            "Horizon": hs,
            "事后首销类型": label,
            "记录数": len(seg),
            "实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in seg)),
            "A3预测": int(sum(float(r.get(A3, 0) or 0) for r in seg)),
            "Bias%": base.metric(seg, A3).get("Bias%"),
            "WAPE": base.metric(seg, A3).get("WAPE"),
        })
    return out


def launch_curve_table(
    actual: Mapping[Tuple[str, date], int],
    first_sale: Mapping[str, date],
    category_map: Mapping[str, str],
    cutoff: date,
    max_age: int = 6,
) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, int], List[int]] = defaultdict(list)
    for spu, f in first_sale.items():
        cat = str(category_map.get(spu) or "未映射")
        for age in range(max_age + 1):
            d = add_months(f, age)
            if d >= cutoff:
                continue
            groups[(cat, age)].append(qty(actual, spu, d))
    out: List[Dict[str, Any]] = []
    for (cat, age), vals in sorted(groups.items()):
        if len(vals) < 10:
            continue
        out.append({
            "品类": cat,
            "首销后月龄": age,
            "样本SPU数": len(vals),
            "中位销量": statistics.median(vals),
            "均值销量": statistics.fmean(vals),
            "P75销量": sorted(vals)[int(0.75 * (len(vals) - 1))],
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = add_months(min(targets), -args.max_horizon)
    history_start = add_months(min_snapshot, -30)
    end = add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建Stage-1严格OOS，并拆分 NEW_VISIBLE / COLD_NO_HISTORY...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r6.walk_forward_rows(detail, targets, args.max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    category_map = r9.load_spu_category_map()
    oos = r9.enrich_category(oos, category_map)

    first_sale = r11.first_sale_map(actual)
    new_rows = [r for r in oos if str(r.get("可预测性")) == "NEW_VISIBLE" and str(r.get("Horizon")) in ("H2", "H3")]
    cold_rows = [r for r in oos if str(r.get("可预测性")) == "COLD_NO_HISTORY" and str(r.get("Horizon")) in ("H2", "H3")]

    print("NEW_VISIBLE rows:", len(new_rows), "COLD_NO_HISTORY rows:", len(cold_rows))
    launch_rows = add_launch_predictions(actual, first_sale, category_map, new_rows)

    summary_rows = base.norm(metrics_by_scope(launch_rows))
    monthly_rows = base.norm(monthly(launch_rows))
    rule_rows = base.norm(rule_summary(launch_rows))
    cold_rows_out = base.norm(cold_diagnostic(cold_rows, first_sale))
    curve_rows = base.norm(launch_curve_table(actual, first_sale, category_map, cutoff=max(targets)))
    detail_rows = base.norm(launch_rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十九轮新品启动曲线_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_NEW_VISIBLE总览.csv"), summary_rows)
    output_fix.write_csv(root.with_name(root.name + "_NEW_VISIBLE逐月.csv"), monthly_rows)
    output_fix.write_csv(root.with_name(root.name + "_规则分解.csv"), rule_rows)
    output_fix.write_csv(root.with_name(root.name + "_COLD事后诊断.csv"), cold_rows_out)
    output_fix.write_csv(root.with_name(root.name + "_历史新品曲线.csv"), curve_rows)
    output_fix.write_csv(root.with_name(root.name + "_NEW_VISIBLE明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("NEW_VISIBLE总览", summary_rows),
        ("NEW_VISIBLE逐月", monthly_rows),
        ("规则分解", rule_rows),
        ("COLD事后诊断", cold_rows_out),
        ("历史新品曲线", curve_rows),
        ("NEW_VISIBLE明细", detail_rows),
    ])

    print("\n=== NEW_VISIBLE H2/H3 总览 ===")
    for r in summary_rows:
        if str(r.get("范围")) in ("全部NEW_VISIBLE", "ABC_A"):
            print(r)
    print("\n=== NEW_VISIBLE 逐月 ===")
    for r in monthly_rows:
        print(r)
    print("\n=== 启动曲线规则分解 ===")
    for r in rule_rows:
        print(r)
    print("\n=== COLD_NO_HISTORY 事后首销诊断 ===")
    for r in cold_rows_out:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：A26/A27若在NEW_VISIBLE H2/H3跨月优于A3且Bias受控，再进入新品模型下一轮；COLD仅用于量化必须接入上新计划的数据缺口。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
