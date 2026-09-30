#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-18: parity-controlled baseline -> A7 -> A16 audit.

Read-only. No production tables/code are modified.

Why this round exists
---------------------
Round-17 accidentally changed TWO things at once for the so-called old chain:
1) the candidate baseline; and
2) the training universe used to select A7 gamma_down/gamma_up.

Round-12 selected A7 parameters on ALL forecastability rows for prior target months,
then evaluated A16 on ESTABLISHED rows. Round-17 selected those parameters using only
ESTABLISHED rows. This changed the control itself and moved bias materially negative.

Round-18 restores strict parity:
- OLD control: exact Round-12 training/evaluation path.
- A24/A25: baseline lambdas are learned on prior ESTABLISHED rows only (as Round-16),
  but when re-selecting seasonal gamma, the training universe remains ALL rows, exactly
  like the old control. Non-ESTABLISHED rows keep A3 as their baseline in the new-chain
  gamma training, so the only intended change is the ESTABLISHED baseline correction.
- Final scoring remains ESTABLISHED H2/H3 only.
- Amazon is intentionally excluded here; Round-17 showed it changes the catalog-wide
  result only marginally with the current single verified mapping. We first need a
  trustworthy parity-controlled internal chain.

Expected parity check (from Round-12 historical result):
  old A16 H2 WAPE ~0.548729, Bias ~-0.018157
  old A16 H3 WAPE ~0.677294, Bias ~-0.005732
If the control does not reproduce these approximately, STOP and diagnose before using
any challenger result.
"""
from __future__ import annotations

import argparse
import sys
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
from scripts import audit_sales_forecast_v5_round12_robust_season_trust as r12
from scripts import audit_sales_forecast_v5_round15_baseline_trajectory as r15
from scripts import audit_sales_forecast_v5_round16_temporal_baseline_calibration as r16
from scripts import audit_sales_forecast_v5_round17_full_existing_chain as r17

A3 = "A3_SPU生命周期收缩"
A7 = "A7_非对称季节门控"
A16 = "A16_广度否决_高集中半衰减"
A24 = "A24_分规则时序校准"
A25 = "A25_仅弱动量时序校准"
RAW = "A22_再加弱动量"
RULE = "基线修正规则"

N24_A7 = "R18_A24_A7"
N24_A16 = "R18_A24_A16"
N25_A7 = "R18_A25_A7"
N25_A16 = "R18_A25_A16"


def build_all_rows(actual, targets, max_horizon: int) -> List[Dict[str, Any]]:
    detail = r2.build_detail(actual, targets, max_horizon)
    detail = r5.enrich(actual, detail)
    detail = r6.enrich_segments(actual, detail)
    detail = r7.enrich_forecastability(detail)
    detail = r9.enrich_category(detail, r9.load_spu_category_map())
    return r15.add_candidates(actual, detail)


def is_established(r: Mapping[str, Any]) -> bool:
    return str(r.get("可预测性") or "") == "ESTABLISHED"


def choose_lambdas(train_est: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    lambdas: Dict[str, float] = {"KEEP_A3": 0.0}
    choices: List[Dict[str, Any]] = []
    for rule in ("SPIKE_REANCHOR", "STRICT_DECLINE", "WEAK_MOMENTUM"):
        seg = [r for r in train_est if str(r.get(RULE) or "KEEP_A3") == rule]
        lam, reason, _ = r16.choose_lambda(seg)
        lambdas[rule] = lam
        choices.append({
            "层": "baseline_lambda",
            "规则": rule,
            "训练记录数": len(seg),
            "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in seg)),
            "lambda": lam,
            "选择原因": reason,
        })
    return lambdas, choices


def apply_baseline_parity(
    rows: Sequence[Dict[str, Any]],
    lambdas: Mapping[str, float],
    out_col: str,
    weak_only: bool,
) -> List[Dict[str, Any]]:
    """Modify ESTABLISHED only; all other forecastability rows retain A3."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r.get(A3, 0) or 0)
        if not is_established(r):
            lam = 0.0
        else:
            rule = str(r.get(RULE) or "KEEP_A3")
            if weak_only and rule != "WEAK_MOMENTUM":
                lam = 0.0
            else:
                lam = float(lambdas.get(rule, 0.0))
        raw = float(r.get(RAW, a3) or a3)
        x[out_col] = max(0, int(round(a3 + lam * (raw - a3))))
        x[f"{out_col}_lambda"] = lam
        out.append(x)
    return out


def merge_three(old_rows, a24_rows, a25_rows) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for xo, x24, x25 in zip(old_rows, a24_rows, a25_rows):
        x = dict(xo)
        x[A24] = x24[A24]
        x[f"{A24}_lambda"] = x24.get(f"{A24}_lambda")
        x[N24_A7] = x24[N24_A7]
        x[f"{N24_A7}_规则"] = x24[f"{N24_A7}_规则"]
        x[A25] = x25[A25]
        x[f"{A25}_lambda"] = x25.get(f"{A25}_lambda")
        x[N25_A7] = x25[N25_A7]
        x[f"{N25_A7}_规则"] = x25[f"{N25_A7}_规则"]
        out.append(x)
    return out


def build_oos(actual, targets: Sequence[date], max_horizon: int, min_train_months: int = 6):
    all_rows = build_all_rows(actual, targets, max_horizon)
    target_text = [d.strftime("%Y-%m") for d in targets]
    staged: List[Dict[str, Any]] = []
    choices: List[Dict[str, Any]] = []

    for test_idx in range(min_train_months, len(target_text)):
        test_month = target_text[test_idx]
        prior = set(target_text[:test_idx])
        for hs in ("H2", "H3"):
            train_all = [r for r in all_rows if str(r.get("Horizon")) == hs and str(r.get("目标月")) in prior]
            test_all = [r for r in all_rows if str(r.get("Horizon")) == hs and str(r.get("目标月")) == test_month]
            if not test_all:
                continue
            train_est = [r for r in train_all if is_established(r)]

            lambdas, lchoices = choose_lambdas(train_est)
            for c in lchoices:
                choices.append({"测试月": test_month, "Horizon": hs, **c})

            # OLD control: exact Round-12 gamma selection universe = ALL prior rows.
            old_gd, old_gu, _ = r5.choose_params(train_all)
            old_test = r5.add_a7(test_all, old_gd, old_gu)
            choices.append({
                "测试月": test_month, "Horizon": hs, "层": "season_gamma", "规则": "OLD_A3_ALL_ROWS",
                "训练记录数": len(train_all),
                "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in train_all)),
                "gamma_down": old_gd, "gamma_up": old_gu,
            })

            # New baselines change ESTABLISHED rows only; non-established stay A3 so the
            # seasonal hyperparameter training universe is otherwise identical.
            train24 = apply_baseline_parity(train_all, lambdas, A24, weak_only=False)
            test24 = apply_baseline_parity(test_all, lambdas, A24, weak_only=False)
            train25 = apply_baseline_parity(train_all, lambdas, A25, weak_only=True)
            test25 = apply_baseline_parity(test_all, lambdas, A25, weak_only=True)

            gd24, gu24, _ = r17.choose_gamma(train24, A24)
            gd25, gu25, _ = r17.choose_gamma(train25, A25)
            choices.append({
                "测试月": test_month, "Horizon": hs, "层": "season_gamma", "规则": "A24_ALL_ROWS",
                "训练记录数": len(train24), "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in train24)),
                "gamma_down": gd24, "gamma_up": gu24,
            })
            choices.append({
                "测试月": test_month, "Horizon": hs, "层": "season_gamma", "规则": "A25_ALL_ROWS",
                "训练记录数": len(train25), "训练实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in train25)),
                "gamma_down": gd25, "gamma_up": gu25,
            })

            season24 = r17.add_a7_generic(test24, A24, N24_A7, gd24, gu24)
            season25 = r17.add_a7_generic(test25, A25, N25_A7, gd25, gu25)
            merged = merge_three(old_test, season24, season25)
            staged.extend([r for r in merged if is_established(r)])

    first_sale = r11.first_sale_map(actual)
    support = r12.category_support(actual, first_sale, staged)

    # Exact old Round-12 A16 control.
    old = r12.apply_trust_gates(actual, first_sale, staged, support)
    # New chains use the same category evidence, only a different baseline/season pair.
    n24 = r17.apply_trust_generic(actual, first_sale, old, support, A24, N24_A7, N24_A16)
    n25 = r17.apply_trust_generic(actual, first_sale, n24, support, A25, N25_A7, N25_A16)
    return n25, choices


def scopes(rows: Sequence[Dict[str, Any]]):
    return [
        ("全部ESTABLISHED", list(rows)),
        ("ABC_A", [r for r in rows if str(r.get("ABC") or "") == "A"]),
        ("T恤", [r for r in rows if str(r.get("品类") or "") == "T恤"]),
        ("T恤_ABC_A", [r for r in rows if str(r.get("品类") or "") == "T恤" and str(r.get("ABC") or "") == "A"]),
    ]


def summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    models = [("A3基线", A3), ("旧A7", A7), ("旧A16_Parity", A16), ("A24基线", A24), ("A24→A7", N24_A7), ("A24→A16", N24_A16), ("A25基线", A25), ("A25→A7", N25_A7), ("A25→A16", N25_A16)]
    out: List[Dict[str, Any]] = []
    for scope, rr in scopes(rows):
        for hs in ("H2", "H3"):
            seg = [r for r in rr if str(r.get("Horizon")) == hs]
            for name, col in models:
                out.append({"范围": scope, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def improvements(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for scope, rr in scopes(rows):
        for hs in ("H2", "H3"):
            seg = [r for r in rr if str(r.get("Horizon")) == hs]
            old = base.metric(seg, A16)
            ow = old.get("WAPE")
            for label, col in (("A24完整链", N24_A16), ("A25完整链", N25_A16)):
                m = base.metric(seg, col)
                nw = m.get("WAPE")
                out.append({
                    "范围": scope, "Horizon": hs, "候选": label,
                    "旧A16_WAPE": ow, "新WAPE": nw,
                    "WAPE改善百分点": None if ow is None or nw is None else float(ow) - float(nw),
                    "WAPE相对改善": None if ow in (None, 0) or nw is None else (float(ow) - float(nw)) / float(ow),
                    "旧A16_Bias%": old.get("Bias%"), "新Bias%": m.get("Bias%"),
                })
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            seg = [r for r in hr if str(r.get("目标月")) == month]
            for name, col in (("旧A16_Parity", A16), ("A24→A16", N24_A16), ("A25→A16", N25_A16)):
                out.append({"Horizon": hs, "目标月": month, "模型": name, **base.metric(seg, col)})
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
    sales_end = base.add_months(max(targets), 1)
    print("读取店内销量:", history_start, "~", sales_end)
    actual = base.actual_spu_month(base.read_sales(history_start, sales_end))

    print("构建 Round-18 parity control：旧A16必须复现Round-12，再比较A24/A25完整链...")
    rows, choices = build_oos(actual, targets, args.max_horizon, 6)
    s = base.norm(summary(rows))
    imp = base.norm(improvements(rows))
    mon = base.norm(monthly(rows))
    ch = base.norm(choices)
    detail = base.norm(rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十八轮Parity基线链_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), s)
    output_fix.write_csv(root.with_name(root.name + "_改善.csv"), imp)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), mon)
    output_fix.write_csv(root.with_name(root.name + "_参数.csv"), ch)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), detail)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [("总览", s), ("改善", imp), ("逐月", mon), ("参数", ch), ("OOS明细", detail)])

    print("\n=== Round18 Parity总览 ===")
    for r in s: print(r)
    print("\n=== 相对严格Parity旧A16的改善 ===")
    for r in imp: print(r)
    print("\n=== 逐月 ===")
    for r in mon: print(r)
    print("\n=== 参数选择 ===")
    for r in ch: print(r)
    print("\nExcel:", xlsx.resolve())
    print("Parity检查目标：旧A16 H2约 WAPE=0.548729/Bias=-0.018157；H3约 WAPE=0.677294/Bias=-0.005732。若明显不一致，先停止比较challenger。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
