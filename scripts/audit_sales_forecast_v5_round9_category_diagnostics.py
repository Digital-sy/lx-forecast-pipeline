#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-9: category-level turning-point diagnostics for ESTABLISHED products.

Read-only diagnostic. No production forecast tables or production logic are modified.

Why this exists
---------------
Round-8 showed A11 (A7 + last-3 completed OOS months, down-only calibration)
reduces H2/H3 WAPE, but the correction reacts one month late around abrupt turning
points. In particular, July 2026 H3 A7 bias spikes sharply, while lagged calibration
only reacts strongly in August.

This round does NOT add a new forecast formula. It answers:
1) Which product categories contribute most to the July overforecast?
2) Which SPUs inside those categories are the main drivers?
3) At the historical snapshot, was there already a category-level momentum or
   prior-year phase signal that could support a future top-down reconciliation?

Category labels are read from the CURRENT latest ods_lx_product_management snapshot.
They are diagnostic grouping metadata only and are NOT treated as leakage-free
historical features in this round.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5
from scripts import audit_sales_forecast_v5_round6_segments as r6
from scripts import audit_sales_forecast_v5_round7_router as r7
from scripts import audit_sales_forecast_v5_round8_bias_calibration as r8

A7 = "A7_非对称季节门控"
A11 = "A11_A7滚动3月只下调"
PRODUCT_TABLE = "ods_db.ods_lx_product_management"


def load_spu_category_map() -> Dict[str, str]:
    """Latest non-empty product_category per SPU; diagnostic metadata only."""
    sql = f"""
        WITH ranked AS (
            SELECT
                TRIM(spu) AS spu,
                NULLIF(TRIM(product_category), '') AS product_category,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(spu)
                    ORDER BY
                        CASE WHEN product_category IS NOT NULL
                                  AND TRIM(product_category) <> '' THEN 0 ELSE 1 END,
                        update_time DESC,
                        etl_load_time DESC,
                        product_id DESC
                ) AS rn
            FROM {PRODUCT_TABLE}
            WHERE spu IS NOT NULL
              AND TRIM(spu) <> ''
        )
        SELECT spu, product_category
        FROM ranked
        WHERE rn = 1
    """
    with db_cursor() as cur:
        cur.execute(sql)
        rows = list(cur.fetchall() or [])
    return {
        str(r.get("spu") or "").strip(): str(r.get("product_category") or "未映射").strip() or "未映射"
        for r in rows
        if str(r.get("spu") or "").strip()
    }


def enrich_category(rows: Sequence[Dict[str, Any]], cat_map: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        x["品类"] = cat_map.get(str(r.get("SPU") or "").strip(), "未映射")
        out.append(x)
    return out


def signed_error(rows: Sequence[Dict[str, Any]], col: str) -> int:
    return int(round(sum(float(r.get(col, 0) or 0) - float(r.get("实际销量", 0) or 0) for r in rows)))


def abs_error(rows: Sequence[Dict[str, Any]], col: str) -> int:
    return int(round(sum(abs(float(r.get(col, 0) or 0) - float(r.get("实际销量", 0) or 0)) for r in rows)))


def category_month_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            mr = [r for r in hr if str(r.get("目标月")) == month]
            month_signed = signed_error(mr, A7)
            month_abs = abs_error(mr, A7)
            cats = sorted({str(r.get("品类") or "未映射") for r in mr})
            for cat in cats:
                seg = [r for r in mr if str(r.get("品类") or "未映射") == cat]
                row: Dict[str, Any] = {
                    "目标月": month,
                    "Horizon": hs,
                    "品类": cat,
                    "记录数_品类": len(seg),
                    "A7有符号误差": signed_error(seg, A7),
                    "A7绝对误差": abs_error(seg, A7),
                    "占当月A7有符号误差": None if month_signed == 0 else signed_error(seg, A7) / month_signed,
                    "占当月A7绝对误差": None if month_abs == 0 else abs_error(seg, A7) / month_abs,
                }
                for name, col in ((A7, A7), (A11, A11)):
                    m = base.metric(seg, col)
                    row[f"{name}_实际销量"] = m.get("实际销量")
                    row[f"{name}_预测销量"] = m.get("预测销量")
                    row[f"{name}_Bias%"] = m.get("Bias%")
                    row[f"{name}_WAPE"] = m.get("WAPE")
                out.append(row)
    return out


def hist_sum(actual: Dict[Tuple[str, date], int], spus: Iterable[str], d: date) -> int:
    return int(sum(int(actual.get((spu, d), 0) or 0) for spu in spus))


def category_signal_summary(
    actual: Dict[Tuple[str, date], int], rows: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Snapshot-available aggregate signals for each category cohort."""
    out: List[Dict[str, Any]] = []
    keys = sorted({(str(r["目标月"]), str(r["Horizon"]), str(r.get("品类") or "未映射")) for r in rows if str(r["Horizon"]) in ("H2", "H3")})
    for month, hs, cat in keys:
        seg = [r for r in rows if str(r["目标月"]) == month and str(r["Horizon"]) == hs and str(r.get("品类") or "未映射") == cat]
        if not seg:
            continue
        snapshot = datetime.strptime(str(seg[0]["快照月"]), "%Y-%m").date().replace(day=1)
        target = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        spus = sorted({str(r.get("SPU") or "") for r in seg if str(r.get("SPU") or "")})
        m1 = hist_sum(actual, spus, base.add_months(snapshot, -1))
        m2 = hist_sum(actual, spus, base.add_months(snapshot, -2))
        m3 = hist_sum(actual, spus, base.add_months(snapshot, -3))
        prev_avg = (m2 + m3) / 2 if (m2 + m3) > 0 else 0.0
        recent_ratio = None if prev_avg <= 0 else m1 / prev_avg
        py_target = hist_sum(actual, spus, base.add_months(target, -12))
        py_prev = hist_sum(actual, spus, base.add_months(target, -13))
        if py_prev <= 0:
            py_phase = "UNKNOWN"
            py_ratio = None
        else:
            py_ratio = py_target / py_prev
            py_phase = "RISING" if py_ratio > 1.05 else ("FALLING" if py_ratio < 0.95 else "FLAT")
        a7m = base.metric(seg, A7)
        a11m = base.metric(seg, A11)
        out.append({
            "目标月": month,
            "快照月": snapshot.strftime("%Y-%m"),
            "Horizon": hs,
            "品类": cat,
            "SPU数": len(spus),
            "快照前M1销量": m1,
            "快照前M2销量": m2,
            "快照前M3销量": m3,
            "M1_vs_M2M3均值": recent_ratio,
            "去年目标月销量": py_target,
            "去年目标前月销量": py_prev,
            "去年阶段": py_phase,
            "去年目标月环比": py_ratio,
            "A7实际销量": a7m.get("实际销量"),
            "A7预测销量": a7m.get("预测销量"),
            "A7_Bias%": a7m.get("Bias%"),
            "A7_WAPE": a7m.get("WAPE"),
            "A11预测销量": a11m.get("预测销量"),
            "A11_Bias%": a11m.get("Bias%"),
            "A11_WAPE": a11m.get("WAPE"),
        })
    return out


def july_spu_drivers(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [r for r in rows if str(r.get("Horizon")) == hs and str(r.get("目标月")) == "2026-07"]
        ranked = sorted(seg, key=lambda r: float(r.get(A7, 0) or 0) - float(r.get("实际销量", 0) or 0), reverse=True)
        total_signed = signed_error(seg, A7)
        for rank, r in enumerate(ranked[:50], 1):
            err = int(round(float(r.get(A7, 0) or 0) - float(r.get("实际销量", 0) or 0)))
            out.append({
                "Horizon": hs,
                "排名": rank,
                "SPU": r.get("SPU"),
                "品类": r.get("品类"),
                "ABC": r.get("ABC"),
                "需求形态": r.get("需求形态"),
                "生命周期": r.get("生命周期"),
                "实际销量": r.get("实际销量"),
                "A7预测": r.get(A7),
                "A11预测": r.get(A11),
                "A7有符号误差": err,
                "占7月总高估": None if total_signed == 0 else err / total_signed,
                "近12月销量": r.get("近12月销量"),
            })
    return out


def top_july_categories(rows: Sequence[Dict[str, Any]], n: int = 15) -> List[Dict[str, Any]]:
    july = [r for r in rows if r.get("目标月") == "2026-07" and r.get("Horizon") in ("H2", "H3")]
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in july:
        grouped[(str(r["Horizon"]), str(r.get("品类") or "未映射"))].append(r)
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        items = []
        for (h, cat), vals in grouped.items():
            if h != hs:
                continue
            items.append((signed_error(vals, A7), cat, vals))
        items.sort(reverse=True, key=lambda x: x[0])
        total_signed = sum(x[0] for x in items)
        for rank, (err, cat, vals) in enumerate(items[:n], 1):
            m = base.metric(vals, A7)
            out.append({
                "Horizon": hs,
                "排名": rank,
                "品类": cat,
                "实际销量": m.get("实际销量"),
                "A7预测销量": m.get("预测销量"),
                "A7有符号误差": err,
                "占7月总高估": None if total_signed == 0 else err / total_signed,
                "A7_Bias%": m.get("Bias%"),
                "A7_WAPE": m.get("WAPE"),
            })
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
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建Stage-1 A7 OOS并叠加Round-8 A11...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r6.walk_forward_rows(detail, targets, args.max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    calibrated, _scales = r8.stage2_calibration(oos, 2)
    rows = [r for r in calibrated if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")]

    print("读取当前SPU品类映射（仅诊断分组，不作为历史预测特征）...")
    cat_map = load_spu_category_map()
    rows = enrich_category(rows, cat_map)

    by_cat = base.norm(category_month_summary(rows))
    signals = base.norm(category_signal_summary(actual, rows))
    top_cats = base.norm(top_july_categories(rows))
    top_spus = base.norm(july_spu_drivers(rows))

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第九轮品类转折诊断_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_品类逐月.csv"), by_cat)
    output_fix.write_csv(root.with_name(root.name + "_品类历史信号.csv"), signals)
    output_fix.write_csv(root.with_name(root.name + "_7月高估品类TOP.csv"), top_cats)
    output_fix.write_csv(root.with_name(root.name + "_7月高估SPUTOP.csv"), top_spus)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("品类逐月", by_cat),
        ("品类历史信号", signals),
        ("7月高估品类TOP", top_cats),
        ("7月高估SPUTOP", top_spus),
    ])

    print("\n=== 2026-07 H2/H3 A7高估品类TOP ===")
    for r in top_cats:
        print(r)
    print("\n=== 2026-07 H2/H3 A7高估SPU TOP20 ===")
    for r in top_spus:
        if int(r.get("排名") or 0) <= 20:
            print(r)
    print("\n=== 2026-07 高估品类的快照可见历史信号 ===")
    top_keys = {(r["Horizon"], r["品类"]) for r in top_cats[:30]}
    for r in signals:
        if r.get("目标月") == "2026-07" and (r.get("Horizon"), r.get("品类")) in top_keys:
            print(r)
    print("\nExcel:", xlsx.resolve())
    print("说明：本轮只诊断7月转折来源；当前品类标签仅用于分组，不作为无泄漏历史特征。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
