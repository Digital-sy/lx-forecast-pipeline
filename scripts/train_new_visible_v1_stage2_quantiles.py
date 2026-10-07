#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Temporal OOS quantile benchmark for NEW_VISIBLE Stage-2 conditional volume.

Research-only. No DB writes, no model persistence, no production promotion.

Frozen upstream
---------------
Stage-1: NV-ML-V1-B-CORE
Label:   NV-PERSIST-750-v1
Stage-2 point model: NV-ML-V1-STAGE2-COND30-CORE

This script evaluates conditional future30 quantiles among true PERSIST_750 rows:

    P50 = conditional median
    P75 = upper planning quantile

Why quantiles
-------------
The Stage-2 point model improves WAPE but still under-forecasts large winners,
especially at later ages. Procurement needs a planning band rather than one number.

Strict temporal controls
------------------------
- fixed checkpoint ages 7/14/30/60/90
- train label_end < test_start
- remove every test store x SPU launch from training
- positive-class training rows only
- CORE features only
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2


QUANTILES: Sequence[float] = (0.50, 0.75)
MODEL_PREFIX = "NV-ML-V1-STAGE2-QUANTILE-COND30-CORE"
MIN_POSITIVE_TRAIN_ROWS = 50


def make_quantile_regressor(alpha: float) -> Pipeline:
    prep = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline([
                    ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
                ]),
                list(abl.CORE_NUMERIC),
            ),
            (
                "cat",
                Pipeline([
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("onehot", OneHotEncoder(handle_unknown="ignore")),
                ]),
                base.CATEGORICAL_FEATURES,
            ),
        ],
        remainder="drop",
        sparse_threshold=0.3,
    )
    reg = LGBMRegressor(
        objective="quantile",
        alpha=float(alpha),
        n_estimators=350,
        learning_rate=0.03,
        num_leaves=15,
        max_depth=4,
        min_child_samples=20,
        subsample=0.90,
        colsample_bytree=0.85,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbosity=-1,
    )
    return Pipeline([("prep", prep), ("reg", reg)])


def empirical_age_quantile(
    train_pos: pd.DataFrame,
    test: pd.DataFrame,
    q: float,
) -> np.ndarray:
    global_q = float(train_pos["future_sales_30d"].quantile(q))
    by_age = (
        train_pos.groupby("age_days")["future_sales_30d"]
        .quantile(q)
        .to_dict()
    )
    return np.asarray(
        [float(by_age.get(int(a), global_q)) for a in test["age_days"]],
        dtype=float,
    )


def pinball_loss(
    y: np.ndarray,
    pred: np.ndarray,
    q: float,
    weights: np.ndarray | None = None,
) -> float:
    y = np.asarray(y, dtype=float)
    pred = np.asarray(pred, dtype=float)
    err = y - pred
    loss = np.maximum(q * err, (q - 1.0) * err)
    if weights is None:
        return float(np.mean(loss))
    w = np.asarray(weights, dtype=float)
    return float(np.sum(w * loss) / np.sum(w))


def quantile_metrics(
    y: np.ndarray,
    pred: np.ndarray,
    q: float,
    weights: np.ndarray | None = None,
) -> Dict[str, Any]:
    y = np.asarray(y, dtype=float)
    pred = np.maximum(0.0, np.asarray(pred, dtype=float))
    if weights is None:
        w = np.ones(len(y), dtype=float)
    else:
        w = np.asarray(weights, dtype=float)

    total_w = float(np.sum(w))
    covered = (y <= pred).astype(float)
    under = (y > pred).astype(float)
    actual_sum = float(np.sum(w * y))
    pred_sum = float(np.sum(w * pred))

    return {
        "rows": int(len(y)),
        "quantile": q,
        "coverage": round(float(np.sum(w * covered) / total_w), 6),
        "underprediction_rate": round(float(np.sum(w * under) / total_w), 6),
        "pinball_loss": round(pinball_loss(y, pred, q, w), 3),
        "actual_sum": round(actual_sum, 3),
        "pred_sum": round(pred_sum, 3),
        "bias": round((pred_sum - actual_sum) / actual_sum, 6)
        if actual_sum > 0 else None,
    }


def main() -> int:
    df = base.load_rows()
    feature_cols = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES

    print("STAGE2_QUANTILE_SCOPE=" + json.dumps({
        "model_prefix": MODEL_PREFIX,
        "stage1_model": "NV-ML-V1-B-CORE",
        "stage1_label": base.LABEL_NAME,
        "target": "future_sales_30d | PERSIST_750",
        "quantiles": list(QUANTILES),
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "checkpoint_ages": list(base.CHECKPOINT_AGES),
        "feature_set": "CORE",
        "target_transform": "log1p(future_sales_30d)",
        "leakage_rule": "train label_end < test_start and remove all test launches from train",
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled: Dict[float, Dict[str, List[Any]]] = {
        q: {
            "y": [], "baseline": [], "model": [],
            "launch_key": [], "age": [], "fold": []
        }
        for q in QUANTILES
    }
    crossing_rows: List[Dict[str, Any]] = []

    for fold_name, start, end in base.TEST_FOLDS:
        train, test = stage2.prepare_fold(df, fold_name, start, end)
        if train.empty or test.empty:
            continue

        train_pos = train[train["target"].astype(int) == 1].copy()
        test_pos = test[test["target"].astype(int) == 1].copy()
        if len(train_pos) < MIN_POSITIVE_TRAIN_ROWS or test_pos.empty:
            print("STAGE2_QUANTILE_FOLD_SKIPPED=" + json.dumps({
                "fold": fold_name,
                "train_positive_rows": int(len(train_pos)),
                "test_positive_rows": int(len(test_pos)),
            }, ensure_ascii=False))
            continue

        print("STAGE2_QUANTILE_FOLD_SCOPE=" + json.dumps({
            "fold": fold_name,
            "test_start": str(pd.Timestamp(start).date()),
            "train_positive_rows": int(len(train_pos)),
            "train_positive_launches": int(train_pos["launch_key"].nunique()),
            "train_max_label_end": str(train["label_end_date"].max().date()),
            "test_positive_rows": int(len(test_pos)),
            "test_positive_launches": int(test_pos["launch_key"].nunique()),
            "launch_overlap": int(
                len(set(train["launch_key"]) & set(test["launch_key"]))
            ),
        }, ensure_ascii=False))

        preds_by_q: Dict[float, np.ndarray] = {}
        baselines_by_q: Dict[float, np.ndarray] = {}
        y = test_pos["future_sales_30d"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(test_pos)

        for q in QUANTILES:
            model = make_quantile_regressor(q)
            y_train_log = np.log1p(
                train_pos["future_sales_30d"].to_numpy(dtype=float)
            )
            model.fit(
                train_pos[feature_cols],
                y_train_log,
                reg__sample_weight=stage2.launch_balanced_weights(train_pos),
            )

            pred = np.expm1(model.predict(test_pos[feature_cols]))
            pred = np.maximum(pred, 0.0)
            baseline = empirical_age_quantile(train_pos, test_pos, q)

            preds_by_q[q] = pred
            baselines_by_q[q] = baseline

            print("STAGE2_QUANTILE_OOS=" + json.dumps({
                "fold": fold_name,
                "quantile": q,
                "baseline": {
                    "name": f"train_positive_age_q{int(q*100)}",
                    "row_metrics": quantile_metrics(y, baseline, q),
                    "launch_balanced_metrics": quantile_metrics(y, baseline, q, w),
                },
                "lightgbm": {
                    "model": f"{MODEL_PREFIX}-Q{int(q*100)}",
                    "row_metrics": quantile_metrics(y, pred, q),
                    "launch_balanced_metrics": quantile_metrics(y, pred, q, w),
                },
            }, ensure_ascii=False))

            pooled[q]["y"].extend(y.tolist())
            pooled[q]["baseline"].extend(baseline.tolist())
            pooled[q]["model"].extend(pred.tolist())
            pooled[q]["launch_key"].extend(test_pos["launch_key"].astype(str).tolist())
            pooled[q]["age"].extend(test_pos["age_days"].astype(int).tolist())
            pooled[q]["fold"].extend([fold_name] * len(test_pos))

        if 0.50 in preds_by_q and 0.75 in preds_by_q:
            crossing = preds_by_q[0.75] < preds_by_q[0.50]
            crossing_rows.append({
                "fold": fold_name,
                "rows": int(len(test_pos)),
                "crossing_rows": int(crossing.sum()),
                "crossing_rate": round(float(crossing.mean()), 6),
                "median_band_width": round(
                    float(np.median(preds_by_q[0.75] - preds_by_q[0.50])), 3
                ),
                "p75_band_width": round(
                    float(np.quantile(preds_by_q[0.75] - preds_by_q[0.50], 0.75)), 3
                ),
            })
            print("STAGE2_QUANTILE_CROSSING=" + json.dumps(
                crossing_rows[-1], ensure_ascii=False
            ))

    print("\n=== STAGE2 QUANTILE POOLED ===")
    for q in QUANTILES:
        d = pd.DataFrame(pooled[q])
        if d.empty:
            continue
        y = d["y"].to_numpy(dtype=float)
        b = d["baseline"].to_numpy(dtype=float)
        m = d["model"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(d)

        print("STAGE2_QUANTILE_POOLED=" + json.dumps({
            "quantile": q,
            "folds": sorted(d["fold"].unique().tolist()),
            "baseline": {
                "row_metrics": quantile_metrics(y, b, q),
                "launch_balanced_metrics": quantile_metrics(y, b, q, w),
            },
            "lightgbm": {
                "row_metrics": quantile_metrics(y, m, q),
                "launch_balanced_metrics": quantile_metrics(y, m, q, w),
            },
        }, ensure_ascii=False))

        for age in base.CHECKPOINT_AGES:
            g = d[d["age"].astype(int) == int(age)].copy()
            if g.empty:
                continue
            gy = g["y"].to_numpy(dtype=float)
            gb = g["baseline"].to_numpy(dtype=float)
            gm = g["model"].to_numpy(dtype=float)
            gw = stage2.launch_balanced_weights(g)
            print("STAGE2_QUANTILE_AGE=" + json.dumps({
                "quantile": q,
                "age": int(age),
                "rows": int(len(g)),
                "launches": int(g["launch_key"].nunique()),
                "baseline": quantile_metrics(gy, gb, q, gw),
                "lightgbm": quantile_metrics(gy, gm, q, gw),
            }, ensure_ascii=False))

    print("\n=== STAGE2 QUANTILE CROSSING SUMMARY ===")
    print("STAGE2_QUANTILE_CROSSING_POOLED=" + json.dumps({
        "folds": crossing_rows,
        "note": "P75 should normally be >= P50; crossing indicates quantile inconsistency and may require post-processing",
    }, ensure_ascii=False))

    print("\n=== STAGE2_QUANTILE_GUIDANCE ===")
    print("1. P50 coverage目标约50%，P75 coverage目标约75%；同时比较pinball loss是否优于age-quantile baseline。")
    print("2. Day14/30优先，因为这两个年龄最接近实际补货决策窗口。")
    print("3. 若P75覆盖明显低于75%，说明高销量尾部仍被低估；采购安全量不能直接采用。")
    print("4. 若P75/P50 crossing明显，后续可做单调后处理：P75=max(P75,P50)。")
    print("5. Quantile通过后，再补non-breakout/base-demand组件，形成完整future30总销量而非仅breakout条件销量。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
