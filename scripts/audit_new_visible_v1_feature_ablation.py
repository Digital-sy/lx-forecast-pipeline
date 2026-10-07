#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Research-only temporal OOS feature ablation for NEW_VISIBLE Stage-1.

Goal
----
Test whether NV-ML-V1-B's gain comes from stable behavioral features or from optional
price/ads/promo fields whose historical coverage changes sharply over time.

Feature sets
------------
CORE
    age + sales + sessions + CVR + growth + positive/up days + slope/volatility/share
    + store/launch_month/snapshot_month.
CORE_PROMO
    CORE + promotion_units_7d/30d.
ENRICHED_NO_PRICE
    CORE + clicks/impressions/ad spend/orders/sales + promo; excludes avg_price.
ENRICHED_ALL
    Current full LightGBM feature set, including avg_price.

Uses the exact same strict temporal OOS folds and no-launch-overlap rule as
train_new_visible_v1_stage1.py. No database writes, no model persistence.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from scripts import train_new_visible_v1_stage1 as base

CORE_NUMERIC: List[str] = [
    "age_days",
    "sales_3d", "sales_prev_3d", "sales_7d", "sales_prev_7d",
    "sales_14d", "sales_prev_14d", "sales_30d",
    "sessions_3d", "sessions_prev_3d", "sessions_7d", "sessions_prev_7d",
    "sessions_14d", "sessions_prev_14d", "sessions_30d",
    "cvr_3d", "cvr_7d", "cvr_prev_7d", "cvr_14d", "cvr_30d",
    "sales_growth_3d", "sales_growth_7d",
    "sessions_growth_3d", "sessions_growth_7d", "cvr_ratio_7d",
    "sales_positive_days_7", "sessions_positive_days_7",
    "sales_up_days_7", "sessions_up_days_7",
    "sales_slope_7", "sessions_slope_7",
    "sales_cv_7", "sessions_cv_7",
    "sales_max_day_share_7", "sessions_max_day_share_7",
]

PROMO_FEATURES = ["promotion_units_7d", "promotion_units_30d"]
ADS_FEATURES = [
    "clicks_7d", "impressions_7d", "ad_spend_7d", "ad_orders_7d", "ad_sales_7d",
    "ad_spend_30d",
]
PRICE_FEATURES = ["avg_price_7d", "avg_price_30d"]

FEATURE_SETS: Dict[str, List[str]] = {
    "CORE": CORE_NUMERIC,
    "CORE_PROMO": CORE_NUMERIC + PROMO_FEATURES,
    "ENRICHED_NO_PRICE": CORE_NUMERIC + ADS_FEATURES + PROMO_FEATURES,
    "ENRICHED_ALL": list(base.NUMERIC_FEATURES),
}

EARLY_AGES = (7, 14, 30)


def make_model(numeric_features: Sequence[str]) -> Pipeline:
    prep = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline([
                    ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
                ]),
                list(numeric_features),
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
    clf = LGBMClassifier(
        objective="binary",
        n_estimators=300,
        learning_rate=0.03,
        num_leaves=15,
        max_depth=4,
        min_child_samples=25,
        subsample=0.90,
        colsample_bytree=0.85,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbosity=-1,
    )
    return Pipeline([("prep", prep), ("clf", clf)])


def evaluate_segment(y: np.ndarray, p: np.ndarray) -> Dict[str, Any]:
    return base.classifier_metrics(y, p)


def main() -> int:
    df = base.load_rows()
    print("ABLATION_SCOPE=" + json.dumps({
        "dataset_version": str(df["dataset_version"].iloc[0]),
        "label": base.LABEL_NAME,
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "feature_sets": {k: len(v) for k, v in FEATURE_SETS.items()},
        "early_ages": list(EARLY_AGES),
        "leakage_rule": "train label_end < test_start; all test launches removed from train",
    }, ensure_ascii=False))

    pooled: Dict[str, Dict[str, List[Any]]] = {
        name: {"y": [], "p": [], "age": [], "store": [], "fold": []}
        for name in FEATURE_SETS
    }

    for fold_name, start, end in base.TEST_FOLDS:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        test = df[(df["snapshot_date"] >= start_ts) & (df["snapshot_date"] <= end_ts)].copy()
        if test.empty:
            continue
        test_launches = set(test["launch_key"].unique())
        train = df[
            (df["label_end_date"] < start_ts)
            & (~df["launch_key"].isin(test_launches))
        ].copy()
        if train.empty or train["target"].nunique() < 2 or test["target"].nunique() < 2:
            continue

        y_train = train["target"].to_numpy(dtype=int)
        y_test = test["target"].to_numpy(dtype=int)
        weights = base.launch_balanced_weights(train)

        print("ABLATION_FOLD_SCOPE=" + json.dumps({
            "fold": fold_name,
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "test_positive_rate": round(float(test["target"].mean()), 6),
            "launch_overlap": int(len(set(train["launch_key"]) & test_launches)),
        }, ensure_ascii=False))

        for set_name, numeric_features in FEATURE_SETS.items():
            model = make_model(numeric_features)
            cols = list(numeric_features) + base.CATEGORICAL_FEATURES
            model.fit(train[cols], y_train, clf__sample_weight=weights)
            prob = model.predict_proba(test[cols])[:, 1]
            print("ABLATION_FOLD_MODEL=" + json.dumps({
                "fold": fold_name,
                "feature_set": set_name,
                **evaluate_segment(y_test, prob),
            }, ensure_ascii=False))

            d = pooled[set_name]
            d["y"].extend(y_test.tolist())
            d["p"].extend(prob.tolist())
            d["age"].extend(test["age_days"].astype(int).tolist())
            d["store"].extend(test["store_name"].astype(str).tolist())
            d["fold"].extend([fold_name] * len(test))

    print("\n=== ABLATION_POOLED ===")
    for set_name, d in pooled.items():
        if not d["y"]:
            continue
        y = np.asarray(d["y"], dtype=int)
        p = np.asarray(d["p"], dtype=float)
        print("ABLATION_POOLED_MODEL=" + json.dumps({
            "feature_set": set_name,
            **evaluate_segment(y, p),
        }, ensure_ascii=False))

    print("\n=== ABLATION_EARLY_AGE ===")
    for age in EARLY_AGES:
        for set_name, d in pooled.items():
            mask = np.asarray(d["age"], dtype=int) == age
            if not mask.any():
                continue
            y = np.asarray(d["y"], dtype=int)[mask]
            p = np.asarray(d["p"], dtype=float)[mask]
            print("ABLATION_AGE_MODEL=" + json.dumps({
                "age": age,
                "feature_set": set_name,
                **evaluate_segment(y, p),
            }, ensure_ascii=False))

    print("\n=== ABLATION_GUIDANCE ===")
    print("1. 若CORE接近或优于ENRICHED_ALL，首版Stage-1冻结CORE，避免数据源漂移。")
    print("2. 若CORE_PROMO稳定优于CORE，可把促销作为第二层可选增强。")
    print("3. 若ENRICHED_NO_PRICE只在后期改善、早期恶化，则广告字段保留为实验特征，不进首版core。")
    print("4. ENRICHED_ALL必须在Day7/14/30和最近fold都稳定改善，price才有资格进入后续版本。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
