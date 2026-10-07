#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Temporal OOS NEW_VISIBLE Stage-2 conditional-volume benchmark.

Research-only. No DB writes, no model persistence, no production promotion.

Stage-1 is frozen as:
    model: NV-ML-V1-B-CORE
    label: NV-PERSIST-750-v1

Stage-2 question
----------------
Among snapshots that truly satisfy PERSIST_750, how much will the next 30 days sell?

This benchmark trains a conditional volume model only on Stage-1-positive training
rows and predicts future_sales_30d. It also combines the frozen Stage-1 probability
with the conditional volume forecast to form an exploratory:

    expected_opportunity_units
      = P(PERSIST_750) * E(future_sales_30d | PERSIST_750)

Important: expected_opportunity_units is NOT total NEW_VISIBLE sales. Negative-class
launches can still sell units. It is an opportunity-weighted breakout-volume estimate
for later procurement decision research.

Leakage controls
----------------
- fixed checkpoint ages 7/14/30/60/90
- chronological temporal OOS folds
- train label_end = snapshot_date + 30d < test_start
- every test launch (store x SPU) removed from training
- CORE point-in-time features only; no future_* inputs
"""
from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as base


MODEL_NAME = "NV-ML-V1-STAGE2-COND30-CORE"
TARGET_NAME = "future_sales_30d|PERSIST_750"
MIN_POSITIVE_TRAIN_ROWS = 50


def make_regressor() -> Pipeline:
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
        objective="regression_l1",
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


def launch_balanced_weights(df: pd.DataFrame) -> np.ndarray:
    counts = df.groupby("launch_key")["launch_key"].transform("count").astype(float)
    w = 1.0 / counts
    return (w * (len(w) / w.sum())).to_numpy(dtype=float)


def safe_div(a: float, b: float) -> float | None:
    return float(a / b) if b else None


def volume_metrics(
    y: np.ndarray,
    pred: np.ndarray,
    weights: np.ndarray | None = None,
) -> Dict[str, Any]:
    y = np.asarray(y, dtype=float)
    pred = np.maximum(0.0, np.asarray(pred, dtype=float))
    if weights is None:
        w = np.ones(len(y), dtype=float)
    else:
        w = np.asarray(weights, dtype=float)

    abs_err = np.abs(pred - y)
    sq_err = (pred - y) ** 2
    actual_sum = float(np.sum(w * y))
    pred_sum = float(np.sum(w * pred))
    denom_w = float(np.sum(w))

    ape = np.full(len(y), np.nan, dtype=float)
    np.divide(abs_err, y, out=ape, where=y > 0)
    finite_ape = ape[np.isfinite(ape)]

    return {
        "rows": int(len(y)),
        "actual_sum": round(actual_sum, 3),
        "pred_sum": round(pred_sum, 3),
        "wape": round(float(np.sum(w * abs_err) / actual_sum), 6)
        if actual_sum > 0 else None,
        "bias": round(float(np.sum(w * (pred - y)) / actual_sum), 6)
        if actual_sum > 0 else None,
        "mae": round(float(np.sum(w * abs_err) / denom_w), 3)
        if denom_w > 0 else None,
        "rmse": round(float(math.sqrt(np.sum(w * sq_err) / denom_w)), 3)
        if denom_w > 0 else None,
        "median_ape": round(float(np.median(finite_ape)), 6)
        if len(finite_ape) else None,
        "p75_ape": round(float(np.quantile(finite_ape, 0.75)), 6)
        if len(finite_ape) else None,
    }


def age_median_prediction(train_pos: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    global_median = float(train_pos["future_sales_30d"].median())
    medians = (
        train_pos.groupby("age_days")["future_sales_30d"]
        .median()
        .to_dict()
    )
    return np.asarray(
        [float(medians.get(int(a), global_median)) for a in test["age_days"]],
        dtype=float,
    )


def prepare_fold(
    df: pd.DataFrame,
    fold_name: str,
    start: Any,
    end: Any,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    test = df[
        (df["snapshot_date"] >= start_ts)
        & (df["snapshot_date"] <= end_ts)
    ].copy()
    test_launches = set(test["launch_key"].unique())

    train = df[
        (df["label_end_date"] < start_ts)
        & (~df["launch_key"].isin(test_launches))
    ].copy()

    return train, test


def main() -> int:
    df = base.load_rows()
    feature_cols = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES

    print("STAGE2_SCOPE=" + json.dumps({
        "model": MODEL_NAME,
        "stage1_model": "NV-ML-V1-B-CORE",
        "stage1_label": base.LABEL_NAME,
        "target": TARGET_NAME,
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "checkpoint_ages": list(base.CHECKPOINT_AGES),
        "feature_set": "CORE",
        "training_population": "PERSIST_750 positive rows only",
        "target_transform": "log1p(future_sales_30d)",
        "objective": "LightGBM regression_l1 on log1p target",
        "combined_metric_target": "target * future_sales_30d (opportunity units, not total sales)",
        "leakage_rule": "train label_end < test_start and remove all test launches from train",
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled_conditional: Dict[str, List[Any]] = {
        "y": [], "baseline": [], "model": [], "launch_key": [], "age": [], "fold": []
    }
    pooled_combined: Dict[str, List[Any]] = {
        "y": [], "baseline": [], "model": [], "launch_key": [], "age": [], "fold": []
    }

    for fold_name, start, end in base.TEST_FOLDS:
        train, test = prepare_fold(df, fold_name, start, end)
        if train.empty or test.empty:
            print("STAGE2_FOLD_SKIPPED=" + json.dumps({
                "fold": fold_name, "reason": "empty_train_or_test"
            }))
            continue

        train_pos = train[train["target"].astype(int) == 1].copy()
        test_pos = test[test["target"].astype(int) == 1].copy()
        if len(train_pos) < MIN_POSITIVE_TRAIN_ROWS or test_pos.empty:
            print("STAGE2_FOLD_SKIPPED=" + json.dumps({
                "fold": fold_name,
                "reason": "insufficient_positive_support",
                "train_positive_rows": int(len(train_pos)),
                "test_positive_rows": int(len(test_pos)),
            }, ensure_ascii=False))
            continue

        train_max_label_end = train["label_end_date"].max()
        fold_scope = {
            "fold": fold_name,
            "test_start": str(pd.Timestamp(start).date()),
            "test_end": str(pd.Timestamp(end).date()),
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "train_positive_rows": int(len(train_pos)),
            "train_positive_launches": int(train_pos["launch_key"].nunique()),
            "train_positive_rate": round(float(train["target"].mean()), 6),
            "train_max_label_end": str(train_max_label_end.date()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "test_positive_rows": int(len(test_pos)),
            "test_positive_launches": int(test_pos["launch_key"].nunique()),
            "test_positive_rate": round(float(test["target"].mean()), 6),
            "launch_overlap": int(
                len(set(train["launch_key"]) & set(test["launch_key"]))
            ),
        }
        print("STAGE2_FOLD_SCOPE=" + json.dumps(fold_scope, ensure_ascii=False))

        # Stage-1 classifier on all eligible train rows.
        classifier = abl.make_model(abl.CORE_NUMERIC)
        classifier.fit(
            train[feature_cols],
            train["target"].to_numpy(dtype=int),
            clf__sample_weight=base.launch_balanced_weights(train),
        )
        stage1_prob = classifier.predict_proba(test[feature_cols])[:, 1]

        # Stage-2 conditional-volume regressor on positive rows only.
        reg = make_regressor()
        y_train_log = np.log1p(
            train_pos["future_sales_30d"].to_numpy(dtype=float)
        )
        reg.fit(
            train_pos[feature_cols],
            y_train_log,
            reg__sample_weight=launch_balanced_weights(train_pos),
        )

        pred_cond_all = np.expm1(reg.predict(test[feature_cols]))
        pred_cond_all = np.maximum(pred_cond_all, 0.0)

        baseline_cond_all = age_median_prediction(train_pos, test)

        # Conditional-volume metrics: evaluate only true PERSIST_750 rows.
        pos_mask = test["target"].to_numpy(dtype=int) == 1
        y_cond = test.loc[pos_mask, "future_sales_30d"].to_numpy(dtype=float)
        model_cond = pred_cond_all[pos_mask]
        baseline_cond = baseline_cond_all[pos_mask]
        test_pos_for_w = test.loc[pos_mask].copy()
        cond_w = launch_balanced_weights(test_pos_for_w)

        print("STAGE2_CONDITIONAL_OOS=" + json.dumps({
            "fold": fold_name,
            "model": MODEL_NAME,
            "baseline": {
                "name": "train_positive_age_median",
                "row_metrics": volume_metrics(y_cond, baseline_cond),
                "launch_balanced_metrics": volume_metrics(
                    y_cond, baseline_cond, cond_w
                ),
            },
            "lightgbm": {
                "row_metrics": volume_metrics(y_cond, model_cond),
                "launch_balanced_metrics": volume_metrics(
                    y_cond, model_cond, cond_w
                ),
            },
        }, ensure_ascii=False))

        # Two-stage opportunity units:
        # realized = future30 units only when Stage-1 label is positive.
        y_opp = (
            test["future_sales_30d"].to_numpy(dtype=float)
            * test["target"].to_numpy(dtype=float)
        )
        model_opp = stage1_prob * pred_cond_all
        baseline_opp = stage1_prob * baseline_cond_all
        all_w = launch_balanced_weights(test)

        print("STAGE2_COMBINED_OOS=" + json.dumps({
            "fold": fold_name,
            "target": "realized_opportunity_units = target * future_sales_30d",
            "warning": "uses raw Stage-1 probability; diagnostic only until calibration is frozen",
            "stage1_x_age_median": {
                "row_metrics": volume_metrics(y_opp, baseline_opp),
                "launch_balanced_metrics": volume_metrics(
                    y_opp, baseline_opp, all_w
                ),
            },
            "stage1_x_stage2_lgbm": {
                "row_metrics": volume_metrics(y_opp, model_opp),
                "launch_balanced_metrics": volume_metrics(
                    y_opp, model_opp, all_w
                ),
            },
        }, ensure_ascii=False))

        pooled_conditional["y"].extend(y_cond.tolist())
        pooled_conditional["baseline"].extend(baseline_cond.tolist())
        pooled_conditional["model"].extend(model_cond.tolist())
        pooled_conditional["launch_key"].extend(
            test.loc[pos_mask, "launch_key"].astype(str).tolist()
        )
        pooled_conditional["age"].extend(
            test.loc[pos_mask, "age_days"].astype(int).tolist()
        )
        pooled_conditional["fold"].extend([fold_name] * int(pos_mask.sum()))

        pooled_combined["y"].extend(y_opp.tolist())
        pooled_combined["baseline"].extend(baseline_opp.tolist())
        pooled_combined["model"].extend(model_opp.tolist())
        pooled_combined["launch_key"].extend(
            test["launch_key"].astype(str).tolist()
        )
        pooled_combined["age"].extend(test["age_days"].astype(int).tolist())
        pooled_combined["fold"].extend([fold_name] * len(test))

    if not pooled_conditional["y"]:
        raise RuntimeError("no Stage-2 OOS predictions generated")

    print("\n=== STAGE2 POOLED CONDITIONAL VOLUME ===")
    cond_df = pd.DataFrame(pooled_conditional)
    cond_w = launch_balanced_weights(cond_df)
    y = cond_df["y"].to_numpy(dtype=float)
    b = cond_df["baseline"].to_numpy(dtype=float)
    m = cond_df["model"].to_numpy(dtype=float)
    print("STAGE2_CONDITIONAL_POOLED=" + json.dumps({
        "folds": sorted(cond_df["fold"].unique().tolist()),
        "baseline": {
            "name": "train_positive_age_median",
            "row_metrics": volume_metrics(y, b),
            "launch_balanced_metrics": volume_metrics(y, b, cond_w),
        },
        "lightgbm": {
            "model": MODEL_NAME,
            "row_metrics": volume_metrics(y, m),
            "launch_balanced_metrics": volume_metrics(y, m, cond_w),
        },
    }, ensure_ascii=False))

    print("\n=== STAGE2 CONDITIONAL BY AGE ===")
    for age in base.CHECKPOINT_AGES:
        g = cond_df[cond_df["age"].astype(int) == int(age)].copy()
        if g.empty:
            continue
        gw = launch_balanced_weights(g)
        gy = g["y"].to_numpy(dtype=float)
        gb = g["baseline"].to_numpy(dtype=float)
        gm = g["model"].to_numpy(dtype=float)
        print("STAGE2_CONDITIONAL_AGE=" + json.dumps({
            "age": int(age),
            "rows": int(len(g)),
            "launches": int(g["launch_key"].nunique()),
            "baseline": volume_metrics(gy, gb, gw),
            "lightgbm": volume_metrics(gy, gm, gw),
        }, ensure_ascii=False))

    print("\n=== STAGE2 POOLED OPPORTUNITY UNITS ===")
    opp_df = pd.DataFrame(pooled_combined)
    opp_w = launch_balanced_weights(opp_df)
    oy = opp_df["y"].to_numpy(dtype=float)
    ob = opp_df["baseline"].to_numpy(dtype=float)
    om = opp_df["model"].to_numpy(dtype=float)
    print("STAGE2_COMBINED_POOLED=" + json.dumps({
        "folds": sorted(opp_df["fold"].unique().tolist()),
        "target": "target * future_sales_30d",
        "warning": "opportunity units only, not total future sales",
        "stage1_x_age_median": {
            "row_metrics": volume_metrics(oy, ob),
            "launch_balanced_metrics": volume_metrics(oy, ob, opp_w),
        },
        "stage1_x_stage2_lgbm": {
            "row_metrics": volume_metrics(oy, om),
            "launch_balanced_metrics": volume_metrics(oy, om, opp_w),
        },
    }, ensure_ascii=False))

    print("\n=== STAGE2_GUIDANCE ===")
    print("1. 先判断conditional LightGBM是否稳定优于age-median baseline，尤其看WAPE/Bias和Day14/30。")
    print("2. 如果条件销量模型不优于简单baseline，不继续复杂化；采购端可先用age median/quantile。")
    print("3. combined opportunity-units只是Stage1×Stage2诊断，不等于NEW_VISIBLE总销量预测。")
    print("4. Stage1 HIGH已不作为自动追单门槛；后续采购决策应组合score/条件销量/库存/在途/补货周期。")
    print("5. 通过本轮后再研究quantile(P50/P75)和negative-class/base-demand组件，最终形成完整future30量化预测。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
