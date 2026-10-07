#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forward-only calibration audit for DIRECT full-volume NEW_VISIBLE quantiles.

Research-only. No DB writes, no model persistence, no production promotion.

Frozen quantity architecture
----------------------------
DIRECT full-volume:
    NV-ML-V1-STAGE2-DIRECT30-CORE-Q50
    NV-ML-V1-STAGE2-DIRECT30-CORE-Q75

Raw DIRECT quantiles materially improve pinball loss versus age-quantile baselines,
but under-cover:
- Q50 pooled launch-balanced coverage ~43%
- Q75 pooled launch-balanced coverage ~66%

This audit tests a simple multiplicative calibration using PRIOR strict OOS residuals:

    ratio = actual_future30 / raw_quantile_prediction
    factor_q = empirical q-th quantile(ratio)
    calibrated = raw * factor_q

For each test fold, calibration rows must be fully mature before the test starts:

    label_end_date < test_start

Methods
-------
RAW:
    no calibration
GLOBAL:
    one factor across all checkpoint ages
AGE:
    age-specific factor when enough prior mature OOS rows exist;
    otherwise GLOBAL fallback

After calibration:
    Q75 = max(Q75, Q50)

Selection priority
------------------
1) Day14 / Day30 coverage and pinball loss
2) temporal fold stability
3) pooled launch-balanced coverage
4) simplicity (prefer GLOBAL over AGE if performance is similar)
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
METHODS: Sequence[str] = ("RAW", "GLOBAL", "AGE")
MIN_AGE_CAL_ROWS = 40
TEST_FOLDS = [x[0] for x in base.TEST_FOLDS][1:]
MODEL_PREFIX = "NV-ML-V1-STAGE2-DIRECT30-CORE"


def make_oos_predictions() -> pd.DataFrame:
    """Rebuild strict temporal OOS DIRECT quantile predictions for all rows."""
    df = base.load_rows()
    feature_cols = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    out: List[pd.DataFrame] = []

    for fold_name, start, end in base.TEST_FOLDS:
        train, test = stage2.prepare_fold(df, fold_name, start, end)
        if train.empty or test.empty:
            continue

        row = test[[
            "snapshot_date", "label_end_date", "store_name", "spu", "launch_key",
            "age_days", "future_sales_30d"
        ]].copy()
        row["fold"] = fold_name

        y_train_log = np.log1p(
            train["future_sales_30d"].to_numpy(dtype=float)
        )

        for q in QUANTILES:
            model = quant.make_quantile_regressor(q)
            model.fit(
                train[feature_cols],
                y_train_log,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            pred = np.expm1(model.predict(test[feature_cols]))
            row[f"q{int(q*100)}_raw"] = np.maximum(pred, 1e-6)

        out.append(row)

    if not out:
        raise RuntimeError("no DIRECT OOS quantile rows generated")
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
        "quantile": q,
        "history_rows": int(len(history)),
        "global_factor": round(float(global_factor), 6),
        "age_factors": age_factors,
    }


def apply_factor(
    test: pd.DataFrame,
    factor_info: Mapping[str, Any],
    method: str,
) -> np.ndarray:
    q = float(factor_info["quantile"])
    raw = test[f"q{int(q*100)}_raw"].to_numpy(dtype=float)

    if method == "RAW":
        return raw

    if method == "GLOBAL":
        return raw * float(factor_info["global_factor"])

    if method == "AGE":
        factors = np.asarray([
            float(factor_info["age_factors"][int(age)]["factor"])
            for age in test["age_days"].astype(int)
        ], dtype=float)
        return raw * factors

    raise ValueError(method)


def main() -> int:
    oos = make_oos_predictions()
    fold_dates = {
        name: (pd.Timestamp(start), pd.Timestamp(end))
        for name, start, end in base.TEST_FOLDS
    }
    fold_order = [x[0] for x in base.TEST_FOLDS]

    print("DIRECT_QUANTILE_CAL_SCOPE=" + json.dumps({
        "model_prefix": MODEL_PREFIX,
        "target": "future_sales_30d (all NEW_VISIBLE)",
        "quantiles": list(QUANTILES),
        "methods": list(METHODS),
        "age_min_cal_rows": MIN_AGE_CAL_ROWS,
        "maturity_guard": "calibration label_end_date < test_fold_start",
        "monotonic_postprocess": "Q75=max(Q75,Q50)",
        "priority_ages": [14, 30],
        "test_folds": TEST_FOLDS,
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled: Dict[Tuple[float, str], List[pd.DataFrame]] = {
        (q, method): []
        for q in QUANTILES
        for method in METHODS
    }

    for test_fold in TEST_FOLDS:
        test_start, _ = fold_dates[test_fold]
        fold_idx = fold_order.index(test_fold)
        prior_folds = fold_order[:fold_idx]

        history_candidates = oos[oos["fold"].isin(prior_folds)].copy()
        history = history_candidates[
            history_candidates["label_end_date"] < test_start
        ].copy()
        test = oos[oos["fold"] == test_fold].copy()

        if history.empty or test.empty:
            continue

        factors = {
            q: build_factors(history, q)
            for q in QUANTILES
        }

        print("DIRECT_QUANTILE_CAL_FOLD_SCOPE=" + json.dumps({
            "test_fold": test_fold,
            "test_start": str(test_start.date()),
            "prior_folds": prior_folds,
            "history_candidate_rows": int(len(history_candidates)),
            "history_rows": int(len(history)),
            "history_launches": int(history["launch_key"].nunique()),
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
            for method in METHODS:
                pred_map[(q, method)] = apply_factor(
                    test, factors[q], method
                )

        for method in METHODS:
            pred_map[(0.75, method)] = np.maximum(
                pred_map[(0.75, method)],
                pred_map[(0.50, method)],
            )

        y = test["future_sales_30d"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(test)

        for q in QUANTILES:
            for method in METHODS:
                pred = pred_map[(q, method)]

                result: Dict[str, Any] = {
                    "test_fold": test_fold,
                    "quantile": q,
                    "method": method,
                    "row_metrics": quant.quantile_metrics(
                        y, pred, q
                    ),
                    "launch_balanced_metrics": quant.quantile_metrics(
                        y, pred, q, w
                    ),
                }
                if q == 0.50:
                    result["volume_metrics"] = stage2.volume_metrics(
                        y, pred, w
                    )

                print("DIRECT_QUANTILE_CAL_RESULT=" + json.dumps(
                    result, ensure_ascii=False
                ))

                tmp = test[[
                    "fold", "launch_key", "age_days", "future_sales_30d"
                ]].copy()
                tmp["pred"] = pred
                pooled[(q, method)].append(tmp)

    print("\n=== DIRECT QUANTILE CALIBRATION POOLED ===")
    for q in QUANTILES:
        for method in METHODS:
            frames = pooled[(q, method)]
            if not frames:
                continue

            d = pd.concat(frames, ignore_index=True)
            y = d["future_sales_30d"].to_numpy(dtype=float)
            pred = d["pred"].to_numpy(dtype=float)
            w = stage2.launch_balanced_weights(d)

            out: Dict[str, Any] = {
                "quantile": q,
                "method": method,
                "folds": sorted(d["fold"].unique().tolist()),
                "row_metrics": quant.quantile_metrics(y, pred, q),
                "launch_balanced_metrics": quant.quantile_metrics(
                    y, pred, q, w
                ),
            }
            if q == 0.50:
                out["volume_metrics"] = stage2.volume_metrics(
                    y, pred, w
                )

            print("DIRECT_QUANTILE_CAL_POOLED=" + json.dumps(
                out, ensure_ascii=False
            ))

            for age in base.CHECKPOINT_AGES:
                g = d[d["age_days"].astype(int) == int(age)].copy()
                if g.empty:
                    continue

                gy = g["future_sales_30d"].to_numpy(dtype=float)
                gp = g["pred"].to_numpy(dtype=float)
                gw = stage2.launch_balanced_weights(g)

                age_out: Dict[str, Any] = {
                    "quantile": q,
                    "method": method,
                    "age": int(age),
                    "rows": int(len(g)),
                    "launches": int(g["launch_key"].nunique()),
                    "launch_balanced_metrics": quant.quantile_metrics(
                        gy, gp, q, gw
                    ),
                }
                if q == 0.50:
                    age_out["volume_metrics"] = stage2.volume_metrics(
                        gy, gp, gw
                    )

                print("DIRECT_QUANTILE_CAL_AGE=" + json.dumps(
                    age_out, ensure_ascii=False
                ))

    print("\n=== DIRECT_QUANTILE_CAL_GUIDANCE ===")
    print("1. Q50目标是coverage接近50%，但不要把aggregate Bias=0当作median模型硬门槛。")
    print("2. Q75目标是coverage接近75%；优先看Day14/30和每个forward fold，而不是只看pooled。")
    print("3. 若GLOBAL与AGE都能达标，优先GLOBAL；只有AGE在Day14/30明显更好时才采用AGE。")
    print("4. maturity_guard_pass必须全部true；任何失败都作废对应fold。")
    print("5. 若校准后Day14/30的Q50/Q75均可用，下一步停止模型研究，进入库存/在途/48天补货采购回放。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
