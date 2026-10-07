#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forward-only calibration audit for NEW_VISIBLE Stage-2 conditional quantiles.

Research-only. No DB writes, no model persistence, no production promotion.

Frozen models
-------------
Q50: NV-ML-V1-STAGE2-COND30-CORE (conditional median / regression_l1)
Q75: NV-ML-V1-STAGE2-QUANTILE-COND30-CORE-Q75

Motivation
----------
Raw Q75 improves pinball loss materially but under-covers (pooled coverage ~65% rather
than ~75%). This audit tests a simple multiplicative OOS calibration:

    ratio = actual_future30 / raw_quantile_prediction
    calibration_factor_q = empirical q-th quantile(ratio)
    calibrated_prediction = raw_prediction * calibration_factor_q

For each future test fold, the calibration factor may use only PRIOR OOS rows whose
30-day outcome is fully mature before the test fold starts:

    label_end_date < test_start

Two calibration scopes are compared:
- GLOBAL: one factor across all checkpoint ages
- AGE: per checkpoint-age factor when enough mature OOS rows exist; otherwise fallback
       to GLOBAL

Finally enforce monotonicity:
    calibrated_Q75 = max(calibrated_Q75, calibrated_Q50)

This script is an audit, not a frozen production policy.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd

from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2
from scripts import train_new_visible_v1_stage2_quantiles as quant


QUANTILES: Sequence[float] = (0.50, 0.75)
MIN_AGE_CAL_ROWS = 20
TEST_FOLDS = [x[0] for x in base.TEST_FOLDS][1:]


def make_oos_predictions() -> pd.DataFrame:
    """Rebuild row-level strict temporal OOS predictions for true PERSIST_750 rows."""
    df = base.load_rows()
    feature_cols = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    out: List[pd.DataFrame] = []

    for fold_name, start, end in base.TEST_FOLDS:
        train, test = stage2.prepare_fold(df, fold_name, start, end)
        if train.empty or test.empty:
            continue

        train_pos = train[train["target"].astype(int) == 1].copy()
        test_pos = test[test["target"].astype(int) == 1].copy()
        if len(train_pos) < quant.MIN_POSITIVE_TRAIN_ROWS or test_pos.empty:
            continue

        row = test_pos[[
            "snapshot_date", "label_end_date", "store_name", "spu", "launch_key",
            "age_days", "future_sales_30d"
        ]].copy()
        row["fold"] = fold_name

        for q in QUANTILES:
            model = quant.make_quantile_regressor(q)
            y_train_log = np.log1p(
                train_pos["future_sales_30d"].to_numpy(dtype=float)
            )
            model.fit(
                train_pos[feature_cols],
                y_train_log,
                reg__sample_weight=stage2.launch_balanced_weights(train_pos),
            )
            pred = np.expm1(model.predict(test_pos[feature_cols]))
            row[f"q{int(q*100)}_raw"] = np.maximum(pred, 1e-6)

        out.append(row)

    if not out:
        raise RuntimeError("no OOS quantile rows generated")
    return pd.concat(out, ignore_index=True)


def ratio_factor(
    history: pd.DataFrame,
    pred_col: str,
    q: float,
) -> float:
    ratio = (
        history["future_sales_30d"].astype(float)
        / history[pred_col].astype(float).clip(lower=1e-6)
    )
    ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
    if ratio.empty:
        return 1.0
    return float(np.quantile(ratio.to_numpy(dtype=float), q))


def build_factors(
    history: pd.DataFrame,
    q: float,
) -> Dict[str, Any]:
    pred_col = f"q{int(q*100)}_raw"
    global_factor = ratio_factor(history, pred_col, q)
    age_factors: Dict[int, Dict[str, Any]] = {}

    for age in base.CHECKPOINT_AGES:
        g = history[history["age_days"].astype(int) == int(age)].copy()
        if len(g) >= MIN_AGE_CAL_ROWS:
            factor = ratio_factor(g, pred_col, q)
            source = "age"
        else:
            factor = global_factor
            source = "global_fallback"
        age_factors[int(age)] = {
            "rows": int(len(g)),
            "factor": round(float(factor), 6),
            "source": source,
        }

    return {
        "q": q,
        "history_rows": int(len(history)),
        "global_factor": round(float(global_factor), 6),
        "age_factors": age_factors,
    }


def apply_factor(
    test: pd.DataFrame,
    factor_info: Mapping[str, Any],
    scope: str,
) -> np.ndarray:
    q = float(factor_info["q"])
    raw = test[f"q{int(q*100)}_raw"].to_numpy(dtype=float)

    if scope == "GLOBAL":
        return raw * float(factor_info["global_factor"])

    if scope == "AGE":
        factors = []
        af = factor_info["age_factors"]
        for age in test["age_days"].astype(int):
            factors.append(float(af[int(age)]["factor"]))
        return raw * np.asarray(factors, dtype=float)

    raise ValueError(scope)


def metrics(
    y: np.ndarray,
    pred: np.ndarray,
    q: float,
    weights: np.ndarray | None = None,
) -> Dict[str, Any]:
    return quant.quantile_metrics(y, pred, q, weights)


def main() -> int:
    oos = make_oos_predictions()
    fold_dates = {
        name: (pd.Timestamp(start), pd.Timestamp(end))
        for name, start, end in base.TEST_FOLDS
    }

    print("STAGE2_QUANTILE_CAL_SCOPE=" + json.dumps({
        "quantiles": list(QUANTILES),
        "methods": ["RAW", "GLOBAL", "AGE"],
        "age_min_cal_rows": MIN_AGE_CAL_ROWS,
        "maturity_guard": "calibration label_end_date < test_fold_start",
        "monotonic_postprocess": "Q75=max(Q75,Q50)",
        "test_folds": TEST_FOLDS,
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled: Dict[Tuple[float, str], List[pd.DataFrame]] = {
        (q, method): []
        for q in QUANTILES
        for method in ("RAW", "GLOBAL", "AGE")
    }

    for test_fold in TEST_FOLDS:
        test_start, _ = fold_dates[test_fold]
        fold_idx = [x[0] for x in base.TEST_FOLDS].index(test_fold)
        prior_folds = [x[0] for x in base.TEST_FOLDS][:fold_idx]

        history_candidates = oos[oos["fold"].isin(prior_folds)].copy()
        history = history_candidates[
            history_candidates["label_end_date"] < test_start
        ].copy()
        test = oos[oos["fold"] == test_fold].copy()

        if history.empty or test.empty:
            continue

        factors = {q: build_factors(history, q) for q in QUANTILES}

        print("STAGE2_QUANTILE_CAL_FOLD_SCOPE=" + json.dumps({
            "test_fold": test_fold,
            "test_start": str(test_start.date()),
            "prior_folds": prior_folds,
            "history_candidate_rows": int(len(history_candidates)),
            "history_rows": int(len(history)),
            "history_max_label_end": str(history["label_end_date"].max().date()),
            "maturity_guard_pass": bool(
                history["label_end_date"].max() < test_start
            ),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "factors": factors,
        }, ensure_ascii=False))

        pred_map: Dict[Tuple[float, str], np.ndarray] = {}
        for q in QUANTILES:
            raw = test[f"q{int(q*100)}_raw"].to_numpy(dtype=float)
            pred_map[(q, "RAW")] = raw
            pred_map[(q, "GLOBAL")] = apply_factor(test, factors[q], "GLOBAL")
            pred_map[(q, "AGE")] = apply_factor(test, factors[q], "AGE")

        # Enforce Q75 >= Q50 within each method after calibration.
        for method in ("RAW", "GLOBAL", "AGE"):
            pred_map[(0.75, method)] = np.maximum(
                pred_map[(0.75, method)],
                pred_map[(0.50, method)],
            )

        y = test["future_sales_30d"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(test)

        for q in QUANTILES:
            for method in ("RAW", "GLOBAL", "AGE"):
                pred = pred_map[(q, method)]
                result = {
                    "test_fold": test_fold,
                    "quantile": q,
                    "method": method,
                    "row_metrics": metrics(y, pred, q),
                    "launch_balanced_metrics": metrics(y, pred, q, w),
                }
                print("STAGE2_QUANTILE_CAL_RESULT=" + json.dumps(
                    result, ensure_ascii=False
                ))

                tmp = test[[
                    "fold", "launch_key", "age_days", "future_sales_30d"
                ]].copy()
                tmp["pred"] = pred
                pooled[(q, method)].append(tmp)

    print("\n=== STAGE2 QUANTILE CALIBRATION POOLED ===")
    for q in QUANTILES:
        for method in ("RAW", "GLOBAL", "AGE"):
            frames = pooled[(q, method)]
            if not frames:
                continue
            d = pd.concat(frames, ignore_index=True)
            y = d["future_sales_30d"].to_numpy(dtype=float)
            p = d["pred"].to_numpy(dtype=float)
            w = stage2.launch_balanced_weights(d)

            print("STAGE2_QUANTILE_CAL_POOLED=" + json.dumps({
                "quantile": q,
                "method": method,
                "folds": sorted(d["fold"].unique().tolist()),
                "row_metrics": metrics(y, p, q),
                "launch_balanced_metrics": metrics(y, p, q, w),
            }, ensure_ascii=False))

            for age in base.CHECKPOINT_AGES:
                g = d[d["age_days"].astype(int) == int(age)].copy()
                if g.empty:
                    continue
                gy = g["future_sales_30d"].to_numpy(dtype=float)
                gp = g["pred"].to_numpy(dtype=float)
                gw = stage2.launch_balanced_weights(g)
                print("STAGE2_QUANTILE_CAL_AGE=" + json.dumps({
                    "quantile": q,
                    "method": method,
                    "age": int(age),
                    "rows": int(len(g)),
                    "launches": int(g["launch_key"].nunique()),
                    "launch_balanced_metrics": metrics(gy, gp, q, gw),
                }, ensure_ascii=False))

    print("\n=== STAGE2_QUANTILE_CAL_GUIDANCE ===")
    print("1. 优先看P75：coverage是否从raw约65-70%前向提升到接近75%，同时pinball loss不能明显恶化。")
    print("2. GLOBAL若已经稳定，不优先采用AGE；AGE只有在Day14/30显著改善且不增加fold漂移时才值得保留。")
    print("3. P50若raw launch-balanced coverage已接近50%，校准层应尽量少动它。")
    print("4. 所有校准倍率都必须来自prior OOS且label已成熟；maturity_guard_pass必须始终为true。")
    print("5. P75校准通过后，下一步补non-breakout/base-demand组件，形成完整future30总销量分布。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
