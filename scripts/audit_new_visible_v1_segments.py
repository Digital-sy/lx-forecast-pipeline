#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only segmented OOS audit for NEW_VISIBLE Stage-1 benchmark.

Retrains the same strict temporal folds as train_new_visible_v1_stage1.py, then reports
pooled OOS metrics by checkpoint age and store.  This answers whether model gains appear
early enough to matter operationally, instead of being driven mainly by age 60/90 rows.

No table is modified and no model is saved/promoted.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

from scripts import train_new_visible_v1_stage1 as base
from scripts.backtest_breakout_v0_history import score_v0

MODELS_TO_RUN = [base.MODEL_LOGISTIC, base.MODEL_LGBM]


def v0_binary(rows: pd.DataFrame) -> np.ndarray:
    return np.asarray([
        1 if score_v0(r)["risk"] == "HIGH" else 0
        for r in rows.to_dict(orient="records")
    ], dtype=int)


def binary_metrics(y: np.ndarray, pred: np.ndarray) -> Dict[str, Any]:
    return {
        "rows": int(len(y)),
        "positives": int(y.sum()),
        "alerts": int(pred.sum()),
        "alert_rate": round(float(pred.mean()), 6) if len(pred) else None,
        "precision": round(float(precision_score(y, pred, zero_division=0)), 6),
        "recall": round(float(recall_score(y, pred, zero_division=0)), 6),
        "f1": round(float(f1_score(y, pred, zero_division=0)), 6),
    }


def main() -> int:
    df = base.load_rows()
    models = base.make_models()

    pooled: Dict[str, List[Any]] = {
        "fold": [], "age": [], "store": [], "launch": [], "y": [], "v0": []
    }
    for model_name in MODELS_TO_RUN:
        pooled[model_name] = []

    for fold_name, start, end in base.TEST_FOLDS:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        test = df[(df["snapshot_date"] >= start_ts) & (df["snapshot_date"] <= end_ts)].copy()
        if test.empty or test["target"].nunique() < 2:
            continue
        test_launches = set(test["launch_key"].unique())
        train = df[
            (df["label_end_date"] < start_ts)
            & (~df["launch_key"].isin(test_launches))
        ].copy()
        if train.empty or train["target"].nunique() < 2:
            continue

        X_train = train[base.NUMERIC_FEATURES + base.CATEGORICAL_FEATURES]
        y_train = train["target"].to_numpy(dtype=int)
        X_test = test[base.NUMERIC_FEATURES + base.CATEGORICAL_FEATURES]
        y_test = test["target"].to_numpy(dtype=int)
        weights = base.launch_balanced_weights(train)

        fold_probs: Dict[str, np.ndarray] = {}
        for model_name in MODELS_TO_RUN:
            model = models[model_name]
            model.fit(X_train, y_train, clf__sample_weight=weights)
            fold_probs[model_name] = model.predict_proba(X_test)[:, 1]

        pooled["fold"].extend([fold_name] * len(test))
        pooled["age"].extend(test["age_days"].astype(int).tolist())
        pooled["store"].extend(test["store_name"].astype(str).tolist())
        pooled["launch"].extend(test["launch_key"].astype(str).tolist())
        pooled["y"].extend(y_test.tolist())
        pooled["v0"].extend(v0_binary(test).tolist())
        for model_name in MODELS_TO_RUN:
            pooled[model_name].extend(fold_probs[model_name].tolist())

    out = pd.DataFrame({
        "fold": pooled["fold"],
        "age": pooled["age"],
        "store": pooled["store"],
        "launch": pooled["launch"],
        "y": pooled["y"],
        "v0": pooled["v0"],
        **{m: pooled[m] for m in MODELS_TO_RUN},
    })
    if out.empty:
        raise RuntimeError("没有可用的strict temporal OOS预测")

    print("SEGMENT_OOS_SCOPE=" + json.dumps({
        "rows": int(len(out)),
        "launches": int(out["launch"].nunique()),
        "positive_rate": round(float(out["y"].mean()), 6),
        "folds": sorted(out["fold"].unique().tolist()),
        "ages": sorted(out["age"].unique().tolist()),
        "stores": sorted(out["store"].unique().tolist()),
    }, ensure_ascii=False))

    print("\n=== V0_POOLED ===")
    print(json.dumps(binary_metrics(
        out["y"].to_numpy(dtype=int), out["v0"].to_numpy(dtype=int)
    ), ensure_ascii=False))

    print("\n=== MODEL_BY_AGE ===")
    for age in sorted(out["age"].unique()):
        g = out[out["age"] == age]
        y = g["y"].to_numpy(dtype=int)
        print("V0_SEGMENT=" + json.dumps({
            "segment": f"AGE_{age}", **binary_metrics(y, g["v0"].to_numpy(dtype=int))
        }, ensure_ascii=False))
        for model_name in MODELS_TO_RUN:
            p = g[model_name].to_numpy(dtype=float)
            print("MODEL_SEGMENT=" + json.dumps({
                "segment": f"AGE_{age}",
                "model": model_name,
                **base.classifier_metrics(y, p),
            }, ensure_ascii=False))

    print("\n=== MODEL_BY_STORE ===")
    for store in sorted(out["store"].unique()):
        g = out[out["store"] == store]
        y = g["y"].to_numpy(dtype=int)
        print("V0_SEGMENT=" + json.dumps({
            "segment": store, **binary_metrics(y, g["v0"].to_numpy(dtype=int))
        }, ensure_ascii=False))
        for model_name in MODELS_TO_RUN:
            p = g[model_name].to_numpy(dtype=float)
            print("MODEL_SEGMENT=" + json.dumps({
                "segment": store,
                "model": model_name,
                **base.classifier_metrics(y, p),
            }, ensure_ascii=False))

    print("\n=== LIGHTGBM_OPERATIONAL_THRESHOLDS ===")
    p = out[base.MODEL_LGBM].to_numpy(dtype=float)
    y = out["y"].to_numpy(dtype=int)
    for threshold in (0.20, 0.30, 0.40, 0.50):
        pred = (p >= threshold).astype(int)
        print(json.dumps({
            "threshold": threshold,
            **binary_metrics(y, pred),
        }, ensure_ascii=False))

    print("\n=== GUIDANCE ===")
    print("1. Day7/14/30必须单独优于V0，不能只看pooled总体。")
    print("2. 店铺段落用于发现系统性偏差，不用于训练四个独立店铺模型。")
    print("3. 阈值先视为ranking cutoff；完成时间序列校准前不要解释为真实概率。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
