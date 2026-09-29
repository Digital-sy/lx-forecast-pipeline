#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-6: leakage-free ABC + ADI/CV² diagnostics.

Read-only diagnostics. No production tables or forecast code are modified.

Why this exists
---------------
Round-5 A7 reduced H2/H3 WAPE, but H3 aggregate bias remained too negative.
Before adding more rules, diagnose *where* A7 helps/hurts using two planning
segmentations inspired by retail-demand practice:

1) ABC by trailing-12-month SPU unit volume at each forecast snapshot.
   A = items cumulatively contributing first 80% of trailing volume,
   B = next 15%, C = remaining 5% / zero-volume tail.

2) Demand profile from trailing-12-month monthly demand using ADI/CV²:
   Smooth       ADI < 1.32 and CV² < 0.49
   Erratic      ADI < 1.32 and CV² >= 0.49
   Intermittent ADI >= 1.32 and CV² < 0.49
   Lumpy        ADI >= 1.32 and CV² >= 0.49

Because our source grain is monthly (not weekly/daily), these ADI/CV² labels are
DIAGNOSTIC only. They are not yet production routing rules.

All segmentation features use only data strictly before the snapshot month.
Primary evaluation remains expanding walk-forward OOS after >=6 target months.
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
from scripts import audit_sales_forecast_v5_experiments as exp
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round3 as r3
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5

MODELS = [
    ("A0_上月延续", "A0_上月延续"),
    ("A3_SPU生命周期收缩", "A3_SPU生命周期收缩"),
    ("A5_季节融合", "A5_季节融合"),
    ("A7_非对称季节门控", "A7_非对称季节门控"),
]


def trailing_values(
    actual: Dict[Tuple[str, date], int],
    spu: str,
    snapshot: date,
    months: int = 12,
) -> List[int]:
    """Monthly actuals strictly before snapshot, oldest -> newest."""
    ds = [base.add_months(snapshot, -i) for i in range(months, 0, -1)]
    return [max(0, exp.history_qty(actual, spu, d)) for d in ds]


def demand_profile(vals: Sequence[int]) -> Tuple[str, float | None, float | None, int, int]:
    """ADI/CV² diagnostic profile on monthly buckets.

    ADI is approximated as number of observed periods / number of non-zero periods,
    which is the standard regular-period intermittency estimate. CV² is the squared
    coefficient of variation of non-zero demand sizes.
    """
    n = len(vals)
    nz = [float(v) for v in vals if v > 0]
    total = int(sum(vals))
    if not nz:
        return "Dormant", None, None, 0, total

    adi = n / len(nz)
    if len(nz) <= 1:
        cv2 = 0.0
    else:
        mean = statistics.fmean(nz)
        sd = statistics.pstdev(nz)
        cv2 = (sd / mean) ** 2 if mean > 0 else 0.0

    if adi < 1.32 and cv2 < 0.49:
        prof = "Smooth"
    elif adi < 1.32 and cv2 >= 0.49:
        prof = "Erratic"
    elif adi >= 1.32 and cv2 < 0.49:
        prof = "Intermittent"
    else:
        prof = "Lumpy"
    return prof, adi, cv2, len(nz), total


def build_abc_map(
    actual: Dict[Tuple[str, date], int],
    spus: Sequence[str],
    snapshot: date,
    months: int = 12,
) -> Dict[str, str]:
    """Trailing-volume ABC map, leakage-free at one snapshot."""
    qtys: List[Tuple[str, int]] = []
    for spu in spus:
        qty = sum(trailing_values(actual, spu, snapshot, months))
        qtys.append((spu, qty))

    qtys.sort(key=lambda x: (-x[1], x[0]))
    total = sum(q for _s, q in qtys)
    if total <= 0:
        return {s: "C" for s, _q in qtys}

    out: Dict[str, str] = {}
    cum = 0
    for spu, qty in qtys:
        before = cum / total
        if qty <= 0:
            cls = "C"
        elif before < 0.80:
            cls = "A"
        elif before < 0.95:
            cls = "B"
        else:
            cls = "C"
        out[spu] = cls
        cum += qty
    return out


def walk_forward_rows(
    detail: Sequence[Dict[str, Any]],
    targets: Sequence[date],
    max_horizon: int,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Reproduce round-5 walk-forward but retain row-level OOS predictions."""
    target_text = [d.strftime("%Y-%m") for d in targets]
    choices: List[Dict[str, Any]] = []
    oos: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(targets)):
        test_month = target_text[test_idx]
        train_months = set(target_text[:test_idx])
        for h in range(max_horizon + 1):
            hs = f"H{h}"
            train = [r for r in detail if r["Horizon"] == hs and r["目标月"] in train_months]
            test = [r for r in detail if r["Horizon"] == hs and r["目标月"] == test_month]

            gd, gu, _ = r5.choose_params(train)
            a5_gamma, _ = r3.choose_gamma(train)
            choices.append({
                "测试月": test_month,
                "Horizon": hs,
                "训练月数": len(train_months),
                "gamma_down": gd,
                "gamma_up": gu,
                "A5_gamma": a5_gamma,
            })

            a5_test = r3.add_a5(test, a5_gamma)
            a7_test = r5.add_a7(test, gd, gu)
            for x5, x7 in zip(a5_test, a7_test):
                x = dict(x5)
                x["A7_非对称季节门控"] = x7["A7_非对称季节门控"]
                x["A7规则"] = x7["A7规则"]
                x["walk_gamma_down"] = gd
                x["walk_gamma_up"] = gu
                x["walk_A5_gamma"] = a5_gamma
                oos.append(x)

    return choices, oos


def enrich_segments(
    actual: Dict[Tuple[str, date], int],
    oos: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Add snapshot-specific ABC + demand profile without future leakage."""
    by_snapshot: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in oos:
        by_snapshot[r["快照月"]].append(r)

    out: List[Dict[str, Any]] = []
    for snap_text, rows in by_snapshot.items():
        snapshot = datetime.strptime(snap_text, "%Y-%m").date().replace(day=1)
        spus = sorted({r["SPU"] for r in rows})
        abc = build_abc_map(actual, spus, snapshot, 12)

        profile_cache: Dict[str, Tuple[str, float | None, float | None, int, int]] = {}
        for spu in spus:
            profile_cache[spu] = demand_profile(trailing_values(actual, spu, snapshot, 12))

        for r in rows:
            x = dict(r)
            prof, adi, cv2, nzm, trailing = profile_cache[r["SPU"]]
            x["ABC"] = abc.get(r["SPU"], "C")
            x["需求形态"] = prof
            x["ADI_12M"] = None if adi is None else round(adi, 6)
            x["CV2_12M"] = None if cv2 is None else round(cv2, 6)
            x["近12月非零月数"] = nzm
            x["近12月销量"] = trailing
            out.append(x)
    return out


def error_contribution(rows: Sequence[Dict[str, Any]], col: str) -> Tuple[int, int]:
    abs_err = sum(abs(float(r.get(col, 0) or 0) - float(r.get("实际销量", 0) or 0)) for r in rows)
    signed = sum(float(r.get(col, 0) or 0) - float(r.get("实际销量", 0) or 0) for r in rows)
    return int(round(abs_err)), int(round(signed))


def segment_summary(
    rows: Sequence[Dict[str, Any]],
    field: str,
    horizons: Sequence[str] = ("H2", "H3"),
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in horizons:
        hr = [r for r in rows if r["Horizon"] == hs]
        total_actual = sum(float(r["实际销量"] or 0) for r in hr)
        total_a7_abs, _ = error_contribution(hr, "A7_非对称季节门控")
        values = sorted({str(r.get(field, "UNKNOWN")) for r in hr})
        for v in values:
            seg = [r for r in hr if str(r.get(field, "UNKNOWN")) == v]
            seg_actual = sum(float(r["实际销量"] or 0) for r in seg)
            for name, col in MODELS:
                m = base.metric(seg, col)
                abs_err, signed_err = error_contribution(seg, col)
                out.append({
                    "Horizon": hs,
                    "分群字段": field,
                    "分群": v,
                    "模型": name,
                    "记录数": len(seg),
                    "实际销量占比": (seg_actual / total_actual if total_actual > 0 else None),
                    "绝对误差件数": abs_err,
                    "有符号误差件数": signed_err,
                    "占A7总绝对误差": (abs_err / total_a7_abs if name == "A7_非对称季节门控" and total_a7_abs > 0 else None),
                    **m,
                })
    return out


def combined_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if r["Horizon"] == hs]
        combos = sorted({(r["ABC"], r["需求形态"]) for r in hr})
        for abc, prof in combos:
            seg = [r for r in hr if r["ABC"] == abc and r["需求形态"] == prof]
            if not seg:
                continue
            for name, col in MODELS:
                out.append({
                    "Horizon": hs,
                    "ABC": abc,
                    "需求形态": prof,
                    "模型": name,
                    **base.metric(seg, col),
                })
    return out


def winner_summary(seg_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Best OOS WAPE by H/field/segment; diagnostic only, not a routing rule."""
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in seg_rows:
        groups[(r["Horizon"], r["分群字段"], r["分群"])].append(r)
    out: List[Dict[str, Any]] = []
    for (hs, field, seg), vals in sorted(groups.items()):
        candidates = [v for v in vals if v.get("WAPE") is not None]
        if not candidates:
            continue
        best = min(candidates, key=lambda x: (x["WAPE"], abs(x.get("Bias%") or 0)))
        a3 = next((x for x in vals if x["模型"] == "A3_SPU生命周期收缩"), None)
        a7 = next((x for x in vals if x["模型"] == "A7_非对称季节门控"), None)
        out.append({
            "Horizon": hs,
            "分群字段": field,
            "分群": seg,
            "OOS最低WAPE模型": best["模型"],
            "最低WAPE": best["WAPE"],
            "该模型Bias%": best.get("Bias%"),
            "A7_WAPE": a7.get("WAPE") if a7 else None,
            "A7_Bias%": a7.get("Bias%") if a7 else None,
            "A3_WAPE": a3.get("WAPE") if a3 else None,
            "A7相对A3_WAPE改善": ((a3["WAPE"] - a7["WAPE"]) if a3 and a7 and a3.get("WAPE") is not None and a7.get("WAPE") is not None else None),
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
        raise RuntimeError("本实验至少需要8个连续目标月")

    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    end = base.add_months(max(targets), 1)
    print("读取销量:", history_start, "~", end)
    sales_rows = base.read_sales(history_start, end)
    actual = base.actual_spu_month(sales_rows)

    print("构建Round-5无泄漏预测并保留OOS明细...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    choices, oos = walk_forward_rows(detail, targets, args.max_horizon, 6)

    print("计算快照级ABC与ADI/CV²需求形态...")
    oos = enrich_segments(actual, oos)

    seg_abc = segment_summary(oos, "ABC")
    seg_profile = segment_summary(oos, "需求形态")
    seg_lifecycle = segment_summary(oos, "生命周期")
    seg_phase = segment_summary(oos, "历史季节阶段")
    seg_rule = segment_summary(oos, "A7规则")
    combined = combined_summary(oos)
    winners = winner_summary(seg_abc + seg_profile + seg_lifecycle + seg_phase + seg_rule)

    seg_abc = base.norm(seg_abc)
    seg_profile = base.norm(seg_profile)
    seg_lifecycle = base.norm(seg_lifecycle)
    seg_phase = base.norm(seg_phase)
    seg_rule = base.norm(seg_rule)
    combined = base.norm(combined)
    winners = base.norm(winners)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第六轮分群诊断_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_ABC.csv"), seg_abc)
    output_fix.write_csv(root.with_name(root.name + "_需求形态.csv"), seg_profile)
    output_fix.write_csv(root.with_name(root.name + "_生命周期.csv"), seg_lifecycle)
    output_fix.write_csv(root.with_name(root.name + "_季节阶段.csv"), seg_phase)
    output_fix.write_csv(root.with_name(root.name + "_A7规则.csv"), seg_rule)
    output_fix.write_csv(root.with_name(root.name + "_ABCx需求形态.csv"), combined)
    output_fix.write_csv(root.with_name(root.name + "_分群赢家.csv"), winners)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), oos)

    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("ABC", seg_abc),
        ("需求形态", seg_profile),
        ("生命周期", seg_lifecycle),
        ("季节阶段", seg_phase),
        ("A7规则", seg_rule),
        ("ABCx需求形态", combined),
        ("分群赢家", winners),
        ("OOS明细", oos),
        ("walkforward参数", choices),
    ])

    print("\n=== H2/H3 ABC ===")
    for r in seg_abc:
        if r["模型"] in ("A3_SPU生命周期收缩", "A7_非对称季节门控"):
            print(r)

    print("\n=== H2/H3 需求形态 ===")
    for r in seg_profile:
        if r["模型"] in ("A3_SPU生命周期收缩", "A7_非对称季节门控"):
            print(r)

    print("\n=== H2/H3 生命周期 ===")
    for r in seg_lifecycle:
        if r["模型"] in ("A3_SPU生命周期收缩", "A7_非对称季节门控"):
            print(r)

    print("\n=== H2/H3 历史季节阶段 ===")
    for r in seg_phase:
        if r["模型"] in ("A3_SPU生命周期收缩", "A7_非对称季节门控"):
            print(r)

    print("\n=== 分群赢家 ===")
    for r in winners:
        print(r)

    print("\nExcel:", xlsx.resolve())
    print("说明：ADI/CV²基于月度12期，仅用于诊断；本轮不产生新的生产公式。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
