#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-17: rebuild the full ESTABLISHED chain on calibrated baselines.

Read-only experiment. No production tables or production forecast code are modified.

Goal
----
Round-16 showed that baseline trajectory corrections contain useful signal, but their
strength must be learned from prior target months. This round finally reconnects those
baselines to the downstream seasonal/category/market layers instead of evaluating the
baseline in isolation.

For every test target month after six warm-up target months, and separately for H2/H3:
1) choose A24/A25 baseline lambdas using only earlier target months;
2) re-fit A7 gamma_down/gamma_up on those same earlier months, but relative to the
   corresponding calibrated baseline (not the old A3 baseline);
3) recompute the seasonal rule on the test month;
4) apply the Round-12 robust category trust gate with the NEW baseline as anchor;
5) apply the Amazon market brake only where a manually verified Browse Node exists.

A24 allows temporal calibration of SPIKE_REANCHOR / STRICT_DECLINE / WEAK_MOMENTUM.
A25 only calibrates WEAK_MOMENTUM and otherwise retains A3.

Amazon A18 restore logic is intentionally NOT carried forward because Round-13C showed
that restoring uplift from an internal block materially worsened accuracy. Amazon is
used only as a brake/attenuator here.

The same pipeline is also run from A3 inside this script, so old-vs-new chain comparison
is apples-to-apples within Round-17.
"""
from __future__ import annotations

import argparse
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5
from scripts import audit_sales_forecast_v5_round11_concentration_anomaly as r11
from scripts import audit_sales_forecast_v5_round12_robust_season_trust as r12
from scripts import audit_sales_forecast_v5_round13_amazon_market_signal as r13
from scripts import audit_sales_forecast_v5_round16_temporal_baseline_calibration as r16
from scripts.amazon_category_insights_performance_adapter_v3 import (
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    load_monthly_market_24m,
)

A3 = "A3_SPU生命周期收缩"
A24 = "A24_分规则时序校准"
A25 = "A25_仅弱动量时序校准"
RAW = "A22_再加弱动量"
RULE = "基线修正规则"

OLD_A7 = "R17_A3_A7"
OLD_A16 = "R17_A3_A16"
OLD_A17 = "R17_A3_A17"

A24_A7 = "R17_A24_A7"
A24_A16 = "R17_A24_A16"
A24_A17 = "R17_A24_A17"

A25_A7 = "R17_A25_A7"
A25_A16 = "R17_A25_A16"
A25_A17 = "R17_A25_A17"

CHAIN_MODELS = [
    ("A3基线", A3),
    ("旧链_A3→A7", OLD_A7),
    ("旧链_A3→A16", OLD_A16),
    ("旧链_A3→A16→Amazon", OLD_A17),
    ("A24基线", A24),
    ("A24→A7", A24_A7),
    ("A24→A16", A24_A16),
    ("A24→A16→Amazon", A24_A17),
    ("A25基线", A25),
    ("A25→A7", A25_A7),
    ("A25→A16", A25_A16),
    ("A25→A16→Amazon", A25_A17),
]


def parse_month(v: Any) -> date:
    if isinstance(v, datetime):
        return date(v.year, v.month, 1)
    if isinstance(v, date):
        return date(v.year, v.month, 1)
    return datetime.strptime(str(v)[:7], "%Y-%m").date().replace(day=1)


def apply_baseline(
    rows: Sequence[Dict[str, Any]],
    lambdas: Mapping[str, float],
    out_col: str,
    weak_only: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        rule = str(r.get(RULE) or "KEEP_A3")
        if weak_only and rule != "WEAK_MOMENTUM":
            lam = 0.0
        else:
            lam = float(lambdas.get(rule, 0.0))
        a3 = float(r.get(A3, 0) or 0)
        raw = float(r.get(RAW, a3) or a3)
        x[out_col] = max(0, int(round(a3 + lam * (raw - a3))))
        x[f"{out_col}_lambda"] = lam
        out.append(x)
    return out


def add_a7_generic(
    rows: Sequence[Dict[str, Any]],
    base_col: str,
    out_col: str,
    gamma_down: float,
    gamma_up: float,
    up_cap_ratio: float = 1.50,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        b = float(r.get(base_col, 0) or 0)
        cand = r.get("季节候选")
        phase = str(r.get("历史季节阶段") or "UNKNOWN")

        if b <= 0 or cand is None:
            pred = b
            rule = "BASE_ONLY"
        else:
            cand_f = float(cand)
            ratio = cand_f / b if b > 0 else 1.0
            if ratio < 0.90:
                pred = (1.0 - gamma_down) * b + gamma_down * cand_f
                rule = "DOWN"
            elif ratio <= 1.10:
                pred = b
                rule = "NEUTRAL"
            else:
                if phase == "RISING":
                    effective = min(cand_f, b * up_cap_ratio)
                    pred = (1.0 - gamma_up) * b + gamma_up * effective
                    rule = "UP_RISING"
                else:
                    pred = b
                    rule = "UP_BLOCKED"

        x[out_col] = max(0, int(round(pred)))
        x[f"{out_col}_规则"] = rule
        out.append(x)
    return out


def score_gamma(
    rows: Sequence[Dict[str, Any]],
    base_col: str,
    gd: float,
    gu: float,
) -> Dict[str, Any]:
    col = "_season"
    work = add_a7_generic(rows, base_col, col, gd, gu)
    return {
        "gamma_down": round(gd, 2),
        "gamma_up": round(gu, 2),
        **base.metric(work, col),
    }


def choose_gamma(
    rows: Sequence[Dict[str, Any]],
    base_col: str,
) -> Tuple[float, float, List[Dict[str, Any]]]:
    grid = [i / 10.0 for i in range(11)]
    scored = [score_gamma(rows, base_col, gd, gu) for gd in grid for gu in grid]
    feasible = [
        x for x in scored
        if x.get("Bias%") is not None and abs(float(x.get("Bias%") or 0.0)) <= 0.05
    ]
    if feasible:
        best = min(
            feasible,
            key=lambda x: (
                float(x.get("WAPE") if x.get("WAPE") is not None else math.inf),
                abs(float(x.get("Bias%") or 0.0)),
            ),
        )
    else:
        best = min(
            scored,
            key=lambda x: (
                float(x.get("WAPE") if x.get("WAPE") is not None else math.inf)
                + 0.5 * abs(float(x.get("Bias%") or 0.0))
            ),
        )
    return float(best["gamma_down"]), float(best["gamma_up"]), scored


def apply_trust_generic(
    actual: Dict[Tuple[str, date], int],
    first_sale: Dict[str, date],
    rows: Sequence[Dict[str, Any]],
    cat_support: Mapping[Tuple[str, str, str], Mapping[str, Any]],
    base_col: str,
    season_col: str,
    out_col: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    season_rule_col = f"{season_col}_规则"
    for r in rows:
        x = dict(r)
        b = float(r.get(base_col, 0) or 0)
        seasonal = float(r.get(season_col, 0) or 0)
        rule = str(r.get(season_rule_col) or "")
        month = str(r.get("目标月"))
        hs = str(r.get("Horizon"))
        cat = str(r.get("品类") or "未映射")
        target = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        spu_f = r11.spu_history_features(actual, first_sale, str(r.get("SPU") or ""), target)
        c = cat_support.get((month, hs, cat), {})

        pred = seasonal
        gate_rule = "KEEP_SEASON"
        if rule == "UP_RISING":
            anomaly = str(spu_f.get("LY异常类型") or "NORMAL")
            if anomaly != "NORMAL":
                pred = b
                gate_rule = f"BLOCK_{anomaly}"
            else:
                sufficient = bool(c.get("类目信号充分", 0))
                broad = bool(c.get("类目广泛上涨", 0))
                concentrated = bool(c.get("集中度风险", 0))
                if sufficient and not broad:
                    pred = b
                    gate_rule = "BLOCK_CATEGORY_NOT_BROAD"
                elif concentrated:
                    pred = b + 0.50 * max(0.0, seasonal - b)
                    gate_rule = "HALF_UPLIFT_HIGH_CONCENTRATION"
                else:
                    gate_rule = "ALLOW_CATEGORY_BROAD" if sufficient else "KEEP_SMALL_COHORT"

        x[out_col] = max(0, int(round(pred)))
        x[f"{out_col}_规则"] = gate_rule
        x[f"{out_col}_LY异常类型"] = spu_f.get("LY异常类型")
        for k, v in c.items():
            x[f"类目_{k}"] = v
        out.append(x)
    return out


def apply_amazon_brake_generic(
    rows: Sequence[Dict[str, Any]],
    base_col: str,
    season_col: str,
    trust_col: str,
    out_col: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    season_rule_col = f"{season_col}_规则"
    for r in rows:
        x = dict(r)
        b = float(r.get(base_col, 0) or 0)
        seasonal = float(r.get(season_col, 0) or 0)
        trust = float(r.get(trust_col, 0) or 0)
        uplift = max(0.0, seasonal - b)
        mapped = bool(r.get("Amazon映射"))
        season_rule = str(r.get(season_rule_col) or "")

        pred = trust
        rule = "KEEP_TRUST"
        if mapped and season_rule == "UP_RISING" and uplift > 0:
            hist_phase = str(r.get("Amazon_LY销量阶段") or "UNKNOWN")
            hist_against = bool(r.get("Amazon_历史反对上涨", 0))
            current_weak = bool(r.get("Amazon_当前市场偏弱", 0))
            hist_support = bool(r.get("Amazon_历史强支持上涨", 0))

            if hist_against or current_weak:
                pred = min(trust, b + 0.25 * uplift)
                rule = "MARKET_STRONG_BRAKE"
            elif hist_phase == "FLAT":
                pred = min(trust, b + 0.50 * uplift)
                rule = "MARKET_HALF_BRAKE"
            elif hist_support:
                rule = "MARKET_SUPPORT_KEEP"

        x[out_col] = max(0, int(round(pred)))
        x[f"{out_col}_规则"] = rule
        out.append(x)
    return out


def choose_lambdas(
    htrain: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    lambdas: Dict[str, float] = {"KEEP_A3": 0.0}
    choices: List[Dict[str, Any]] = []
    for rule in ("SPIKE_REANCHOR", "STRICT_DECLINE", "WEAK_MOMENTUM"):
        seg = [r for r in htrain if str(r.get(RULE) or "KEEP_A3") == rule]
        lam, reason, _grid = r16.choose_lambda(seg)
        lambdas[rule] = lam
        choices.append({
            "规则": rule,
            "训练记录数": len(seg),
            "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in seg)),
            "lambda": lam,
            "选择原因": reason,
        })
    return lambdas, choices


def build_round17_oos(
    actual: Dict[Tuple[str, date], int],
    targets: Sequence[date],
    max_horizon: int,
    category_map_path: Path,
    market_schema: str,
    market_table: str,
    min_train_months: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    all_rows = r16.build_all_rows(actual, targets, max_horizon)
    target_text = [d.strftime("%Y-%m") for d in targets]
    first_sale = r11.first_sale_map(actual)

    staged: List[Dict[str, Any]] = []
    choice_rows: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(target_text)):
        test_month = target_text[test_idx]
        prior = set(target_text[:test_idx])
        for hs in ("H2", "H3"):
            htrain0 = [
                r for r in all_rows
                if str(r.get("Horizon")) == hs and str(r.get("目标月")) in prior
            ]
            htest0 = [
                r for r in all_rows
                if str(r.get("Horizon")) == hs and str(r.get("目标月")) == test_month
            ]
            if not htest0:
                continue

            lambdas, lchoices = choose_lambdas(htrain0)
            for c in lchoices:
                choice_rows.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "层": "baseline_lambda",
                    **c,
                })

            # Build all three baselines on the SAME train/test rows.
            train_a3 = [dict(r) for r in htrain0]
            test_a3 = [dict(r) for r in htest0]
            train_a24 = apply_baseline(htrain0, lambdas, A24, weak_only=False)
            test_a24 = apply_baseline(htest0, lambdas, A24, weak_only=False)
            train_a25 = apply_baseline(htrain0, lambdas, A25, weak_only=True)
            test_a25 = apply_baseline(htest0, lambdas, A25, weak_only=True)

            # Re-select seasonal strength against each baseline using PRIOR months only.
            old_gd, old_gu, _ = choose_gamma(train_a3, A3)
            a24_gd, a24_gu, _ = choose_gamma(train_a24, A24)
            a25_gd, a25_gu, _ = choose_gamma(train_a25, A25)
            for label, gd, gu in (
                ("A3", old_gd, old_gu),
                ("A24", a24_gd, a24_gu),
                ("A25", a25_gd, a25_gu),
            ):
                choice_rows.append({
                    "测试月": test_month,
                    "Horizon": hs,
                    "层": "season_gamma",
                    "规则": label,
                    "训练记录数": len(htrain0),
                    "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in htrain0)),
                    "gamma_down": gd,
                    "gamma_up": gu,
                })

            old_season = add_a7_generic(test_a3, A3, OLD_A7, old_gd, old_gu)
            a24_season = add_a7_generic(test_a24, A24, A24_A7, a24_gd, a24_gu)
            a25_season = add_a7_generic(test_a25, A25, A25_A7, a25_gd, a25_gu)

            # Merge three parallel chains row-by-row. All preserve identical order.
            for xo, x24, x25 in zip(old_season, a24_season, a25_season):
                x = dict(xo)
                x[A24] = x24[A24]
                x[f"{A24}_lambda"] = x24.get(f"{A24}_lambda")
                x[A24_A7] = x24[A24_A7]
                x[f"{A24_A7}_规则"] = x24[f"{A24_A7}_规则"]
                x[A25] = x25[A25]
                x[f"{A25}_lambda"] = x25.get(f"{A25}_lambda")
                x[A25_A7] = x25[A25_A7]
                x[f"{A25_A7}_规则"] = x25[f"{A25_A7}_规则"]
                staged.append(x)

    # Category support is independent of baseline choice, but only uses prior-year history.
    cat_support = r12.category_support(actual, first_sale, staged)
    old_trust = apply_trust_generic(actual, first_sale, staged, cat_support, A3, OLD_A7, OLD_A16)
    a24_trust = apply_trust_generic(actual, first_sale, old_trust, cat_support, A24, A24_A7, A24_A16)
    a25_trust = apply_trust_generic(actual, first_sale, a24_trust, cat_support, A25, A25_A7, A25_A16)

    # Amazon market evidence; manual mapping only.
    category_map = r13.load_category_node_map(category_map_path)
    node_ids = sorted({str(v["browse_node_id"]) for v in category_map.values()})
    market: Dict[Tuple[str, date], Dict[str, float]] = {}
    market_meta: Dict[str, Any] = {"mode": "no_mapped_nodes"}
    if node_ids:
        market, market_meta = load_monthly_market_24m(
            schema=market_schema,
            table=market_table,
            browse_node_ids=node_ids,
            start_month=base.add_months(min(targets), -13),
            end_month=max(targets),
        )
    enriched = r13.enrich_market(a25_trust, market, category_map)

    old_market = apply_amazon_brake_generic(enriched, A3, OLD_A7, OLD_A16, OLD_A17)
    a24_market = apply_amazon_brake_generic(old_market, A24, A24_A7, A24_A16, A24_A17)
    final_rows = apply_amazon_brake_generic(a24_market, A25, A25_A7, A25_A16, A25_A17)
    return final_rows, choice_rows, market_meta


def ranges(rows: Sequence[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    return [
        ("全部ESTABLISHED", list(rows)),
        ("ABC_A", [r for r in rows if str(r.get("ABC") or "") == "A"]),
        ("T恤", [r for r in rows if str(r.get("品类") or "") == "T恤"]),
        ("T恤_ABC_A", [
            r for r in rows
            if str(r.get("品类") or "") == "T恤" and str(r.get("ABC") or "") == "A"
        ]),
        ("Amazon已映射", [r for r in rows if r.get("Amazon映射")]),
    ]


def summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for scope, rr in ranges(rows):
        for hs in ("H2", "H3"):
            seg = [r for r in rr if str(r.get("Horizon")) == hs]
            if not seg:
                continue
            for name, col in CHAIN_MODELS:
                out.append({"范围": scope, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    keep_models = [
        ("旧链_A3→A16", OLD_A16),
        ("旧链_A3→A16→Amazon", OLD_A17),
        ("A24→A16", A24_A16),
        ("A24→A16→Amazon", A24_A17),
        ("A25→A16", A25_A16),
        ("A25→A16→Amazon", A25_A17),
    ]
    for scope, rr in [
        ("全部ESTABLISHED", list(rows)),
        ("ABC_A", [r for r in rows if str(r.get("ABC") or "") == "A"]),
        ("T恤", [r for r in rows if str(r.get("品类") or "") == "T恤"]),
    ]:
        for hs in ("H2", "H3"):
            hr = [r for r in rr if str(r.get("Horizon")) == hs]
            for month in sorted({str(r.get("目标月")) for r in hr}):
                mr = [r for r in hr if str(r.get("目标月")) == month]
                for name, col in keep_models:
                    out.append({"范围": scope, "Horizon": hs, "目标月": month, "模型": name, **base.metric(mr, col)})
    return out


def improvements(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for scope, rr in ranges(rows)[:4]:
        for hs in ("H2", "H3"):
            seg = [r for r in rr if str(r.get("Horizon")) == hs]
            if not seg:
                continue
            old = base.metric(seg, OLD_A16)
            for label, col in (("A24完整链", A24_A16), ("A25完整链", A25_A16)):
                new = base.metric(seg, col)
                ow = old.get("WAPE")
                nw = new.get("WAPE")
                out.append({
                    "范围": scope,
                    "Horizon": hs,
                    "候选": label,
                    "旧A16_WAPE": ow,
                    "新WAPE": nw,
                    "WAPE改善百分点": None if ow is None or nw is None else float(ow) - float(nw),
                    "WAPE相对改善": None if ow in (None, 0) or nw is None else (float(ow) - float(nw)) / float(ow),
                    "旧A16_Bias%": old.get("Bias%"),
                    "新Bias%": new.get("Bias%"),
                })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    ap.add_argument("--node-map", default="config/amazon_category_node_map.json")
    ap.add_argument("--market-db", default=DEFAULT_SCHEMA)
    ap.add_argument("--market-table", default=DEFAULT_TABLE)
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    sales_end = base.add_months(max(targets), 1)
    print("读取店内销量:", history_start, "~", sales_end)
    sales_rows = base.read_sales(history_start, sales_end)
    actual = base.actual_spu_month(sales_rows)

    print("构建 Round-17：A3/A24/A25 → A7 → A16 → Amazon，仅 ESTABLISHED H2/H3...")
    rows, choices, market_meta = build_round17_oos(
        actual=actual,
        targets=targets,
        max_horizon=args.max_horizon,
        category_map_path=PROJECT_ROOT / args.node_map,
        market_schema=args.market_db,
        market_table=args.market_table,
        min_train_months=6,
    )
    print("Market adapter mode:", market_meta.get("mode"))

    summary_rows = base.norm(summary(rows))
    monthly_rows = base.norm(monthly(rows))
    improve_rows = base.norm(improvements(rows))
    choice_rows = base.norm(choices)
    detail_rows = base.norm(rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十七轮完整Existing链_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), summary_rows)
    output_fix.write_csv(root.with_name(root.name + "_改善.csv"), improve_rows)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), monthly_rows)
    output_fix.write_csv(root.with_name(root.name + "_参数选择.csv"), choice_rows)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("总览", summary_rows),
        ("改善", improve_rows),
        ("逐月", monthly_rows),
        ("参数选择", choice_rows),
        ("OOS明细", detail_rows),
    ])

    print("\n=== Round17 完整链总览 ===")
    for r in summary_rows:
        if r.get("范围") in ("全部ESTABLISHED", "ABC_A", "T恤"):
            print(r)
    print("\n=== 相对Round17同口径旧A16的改善 ===")
    for r in improve_rows:
        print(r)
    print("\n=== 逐月：旧A16 vs A24/A25完整链 ===")
    for r in monthly_rows:
        if r.get("范围") == "全部ESTABLISHED":
            print(r)
    print("\n=== 参数选择 ===")
    for r in choice_rows:
        print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：优先看全部ESTABLISHED与ABC_A的H2/H3 WAPE是否同时下降、Bias是否保持在约±5%；T恤改善只作为机制证据，不单独决定上线。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
