#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Strict temporal OOS benchmark for NEW_VISIBLE non-breakout/base-demand volume.

Research-only. No DB writes, no model persistence, no production promotion.

Purpose
-------
Stage-1 ranks P(PERSIST_750). Stage-2 positive conditional model estimates:
    E(future_sales_30d | PERSIST_750)

This script adds the missing negative/base-demand component:
    E(future_sales_30d | NOT PERSIST_750)

and compares a full two-component mixture against a simpler direct all-row volume model.

Models
------
1) BASE30-CORE:
      conditional future30 model trained only on target=0 rows.
2) POS30-CORE:
      frozen Stage-2 positive conditional model trained only on target=1 rows.
3) MIXTURE30-RAW-P:
      raw Stage-1 probability * POS30
      + (1 - raw Stage-1 probability) * BASE30
4) DIRECT30-CORE:
      one LightGBM volume model trained on all rows, ignoring Stage-1 class.

Why DIRECT is included
----------------------
The mixture is more interpretable but depends on Stage-1 probability calibration.
DIRECT is a necessary simplicity benchmark. If DIRECT beats the mixture consistently,
we should not force the classification architecture into the quantity forecast.

Leakage controls
----------------
- fixed checkpoint ages 7/14/30/60/90
- chronological temporal OOS folds
- train label_end < test_start
- every test launch (store x SPU) removed from train
- CORE point-in-time features only
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2


MODEL_NEG = "NV-ML-V1-STAGE2-BASE30-CORE"
MODEL_DIRECT = "NV-ML-V1-STAGE2-DIRECT30-CORE"
MODEL_MIXTURE = "NV-ML-V1-STAGE2-MIXTURE30-RAW-P"
MIN_CLASS_TRAIN_ROWS = 50


def age_median_prediction(train_df: pd.DataFrame, test_df: pd.DataFrame) -> np.ndarray:
    global_median = float(train_df["future_sales_30d"].median())
    by_age = (
        train_df.groupby("age_days")["future_sales_30d"]
        .median()
        .to_dict()
    )
    return np.asarray(
        [float(by_age.get(int(a), global_median)) for a in test_df["age_days"]],
        dtype=float,
    )


def fit_volume_model(train_df: pd.DataFrame, feature_cols: Sequence[str]):
    model = stage2.make_regressor()
    y_log = np.log1p(train_df["future_sales_30d"].to_numpy(dtype=float))
    model.fit(
        train_df[list(feature_cols)],
        y_log,
        reg__sample_weight=stage2.launch_balanced_weights(train_df),
    )
    return model


def predict_volume(model, test_df: pd.DataFrame, feature_cols: Sequence[str]) -> np.ndarray:
    pred = np.expm1(model.predict(test_df[list(feature_cols)]))
    return np.maximum(pred, 0.0)


def class_distribution(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
    }
    for cls, name in ((0, "negative"), (1, "positive")):
        g = df[df["target"].astype(int) == cls].copy()
        vals = g["future_sales_30d"].to_numpy(dtype=float)
        out[name] = {
            "rows": int(len(g)),
            "launches": int(g["launch_key"].nunique()),
            "sales_sum": round(float(vals.sum()), 3),
            "sales_median": round(float(np.median(vals)), 3) if len(vals) else None,
            "sales_p75": round(float(np.quantile(vals, 0.75)), 3) if len(vals) else None,
            "sales_p90": round(float(np.quantile(vals, 0.90)), 3) if len(vals) else None,
            "zero_rate": round(float((vals <= 0).mean()), 6) if len(vals) else None,
        }
    total_sales = float(df["future_sales_30d"].sum())
    neg_sales = float(
        df.loc[df["target"].astype(int) == 0, "future_sales_30d"].sum()
    )
    pos_sales = float(
        df.loc[df["target"].astype(int) == 1, "future_sales_30d"].sum()
    )
    out["sales_share_negative"] = round(neg_sales / total_sales, 6) if total_sales else None
    out["sales_share_positive"] = round(pos_sales / total_sales, 6) if total_sales else None
    return out


def main() -> int:
    df = base.load_rows()
    feature_cols = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES

    print("BASE_DEMAND_SCOPE=" + json.dumps({
        "stage1_model": "NV-ML-V1-B-CORE",
        "stage1_label": base.LABEL_NAME,
        "positive_model": stage2.MODEL_NAME,
        "negative_model": MODEL_NEG,
        "mixture_model": MODEL_MIXTURE,
        "direct_model": MODEL_DIRECT,
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "checkpoint_ages": list(base.CHECKPOINT_AGES),
        "feature_set": "CORE",
        "leakage_rule": "train label_end < test_start and remove all test launches from train",
        "no_db_write": True,
    }, ensure_ascii=False))

    print("BASE_DEMAND_DATA_PROFILE=" + json.dumps(
        class_distribution(df), ensure_ascii=False
    ))

    pooled: Dict[str, Dict[str, List[Any]]] = {
        name: {"y": [], "pred": [], "launch_key": [], "age": [], "fold": []}
        for name in (
            "ALL_AGE_MEDIAN",
            "DIRECT",
            "MIXTURE",
            "NEG_AGE_MEDIAN",
            "NEG_MODEL",
            "POS_AGE_MEDIAN",
            "POS_MODEL",
        )
    }

    for fold_name, start, end in base.TEST_FOLDS:
        train, test = stage2.prepare_fold(df, fold_name, start, end)
        if train.empty or test.empty:
            continue

        train_neg = train[train["target"].astype(int) == 0].copy()
        train_pos = train[train["target"].astype(int) == 1].copy()
        test_neg = test[test["target"].astype(int) == 0].copy()
        test_pos = test[test["target"].astype(int) == 1].copy()

        if (
            len(train_neg) < MIN_CLASS_TRAIN_ROWS
            or len(train_pos) < MIN_CLASS_TRAIN_ROWS
            or test_neg.empty
            or test_pos.empty
        ):
            print("BASE_DEMAND_FOLD_SKIPPED=" + json.dumps({
                "fold": fold_name,
                "train_neg_rows": int(len(train_neg)),
                "train_pos_rows": int(len(train_pos)),
                "test_neg_rows": int(len(test_neg)),
                "test_pos_rows": int(len(test_pos)),
            }, ensure_ascii=False))
            continue

        print("BASE_DEMAND_FOLD_SCOPE=" + json.dumps({
            "fold": fold_name,
            "test_start": str(pd.Timestamp(start).date()),
            "test_end": str(pd.Timestamp(end).date()),
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "train_neg_rows": int(len(train_neg)),
            "train_pos_rows": int(len(train_pos)),
            "train_max_label_end": str(train["label_end_date"].max().date()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "test_neg_rows": int(len(test_neg)),
            "test_pos_rows": int(len(test_pos)),
            "launch_overlap": int(
                len(set(train["launch_key"]) & set(test["launch_key"]))
            ),
            "test_profile": class_distribution(test),
        }, ensure_ascii=False))

        # Stage-1 probability.
        classifier = abl.make_model(abl.CORE_NUMERIC)
        classifier.fit(
            train[feature_cols],
            train["target"].to_numpy(dtype=int),
            clf__sample_weight=base.launch_balanced_weights(train),
        )
        p = classifier.predict_proba(test[feature_cols])[:, 1]

        # Conditional positive / negative volume models.
        pos_model = fit_volume_model(train_pos, feature_cols)
        neg_model = fit_volume_model(train_neg, feature_cols)
        direct_model = fit_volume_model(train, feature_cols)

        pos_pred_all = predict_volume(pos_model, test, feature_cols)
        neg_pred_all = predict_volume(neg_model, test, feature_cols)
        direct_pred = predict_volume(direct_model, test, feature_cols)

        mixture_pred = p * pos_pred_all + (1.0 - p) * neg_pred_all

        # Simple all-row baseline.
        all_age_median = age_median_prediction(train, test)

        y_all = test["future_sales_30d"].to_numpy(dtype=float)
        w_all = stage2.launch_balanced_weights(test)

        print("BASE_DEMAND_FULL_OOS=" + json.dumps({
            "fold": fold_name,
            "actual_sales_share": {
                "negative": class_distribution(test)["sales_share_negative"],
                "positive": class_distribution(test)["sales_share_positive"],
            },
            "all_age_median": {
                "row_metrics": stage2.volume_metrics(y_all, all_age_median),
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_all, all_age_median, w_all
                ),
            },
            "direct_lgbm": {
                "model": MODEL_DIRECT,
                "row_metrics": stage2.volume_metrics(y_all, direct_pred),
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_all, direct_pred, w_all
                ),
            },
            "mixture_raw_stage1_p": {
                "model": MODEL_MIXTURE,
                "mean_stage1_probability": round(float(np.mean(p)), 6),
                "actual_positive_rate": round(float(test["target"].mean()), 6),
                "row_metrics": stage2.volume_metrics(y_all, mixture_pred),
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_all, mixture_pred, w_all
                ),
            },
        }, ensure_ascii=False))

        # Negative/base-demand conditional evaluation.
        neg_mask = test["target"].to_numpy(dtype=int) == 0
        y_neg = test.loc[neg_mask, "future_sales_30d"].to_numpy(dtype=float)
        pred_neg = neg_pred_all[neg_mask]
        base_neg = age_median_prediction(train_neg, test.loc[neg_mask])
        w_neg = stage2.launch_balanced_weights(test.loc[neg_mask])

        print("BASE_DEMAND_NEGATIVE_OOS=" + json.dumps({
            "fold": fold_name,
            "baseline": {
                "name": "train_negative_age_median",
                "row_metrics": stage2.volume_metrics(y_neg, base_neg),
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_neg, base_neg, w_neg
                ),
            },
            "lightgbm": {
                "model": MODEL_NEG,
                "row_metrics": stage2.volume_metrics(y_neg, pred_neg),
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_neg, pred_neg, w_neg
                ),
            },
        }, ensure_ascii=False))

        # Positive conditional comparison, repeated here so full-volume and both
        # components are visible in one audit.
        pos_mask = test["target"].to_numpy(dtype=int) == 1
        y_pos = test.loc[pos_mask, "future_sales_30d"].to_numpy(dtype=float)
        pred_pos = pos_pred_all[pos_mask]
        base_pos = age_median_prediction(train_pos, test.loc[pos_mask])
        w_pos = stage2.launch_balanced_weights(test.loc[pos_mask])

        print("BASE_DEMAND_POSITIVE_OOS=" + json.dumps({
            "fold": fold_name,
            "baseline": {
                "name": "train_positive_age_median",
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_pos, base_pos, w_pos
                ),
            },
            "lightgbm": {
                "model": stage2.MODEL_NAME,
                "launch_balanced_metrics": stage2.volume_metrics(
                    y_pos, pred_pos, w_pos
                ),
            },
        }, ensure_ascii=False))

        def add(name: str, frame: pd.DataFrame, y, pred):
            pooled[name]["y"].extend(np.asarray(y, dtype=float).tolist())
            pooled[name]["pred"].extend(np.asarray(pred, dtype=float).tolist())
            pooled[name]["launch_key"].extend(frame["launch_key"].astype(str).tolist())
            pooled[name]["age"].extend(frame["age_days"].astype(int).tolist())
            pooled[name]["fold"].extend([fold_name] * len(frame))

        add("ALL_AGE_MEDIAN", test, y_all, all_age_median)
        add("DIRECT", test, y_all, direct_pred)
        add("MIXTURE", test, y_all, mixture_pred)
        add("NEG_AGE_MEDIAN", test.loc[neg_mask], y_neg, base_neg)
        add("NEG_MODEL", test.loc[neg_mask], y_neg, pred_neg)
        add("POS_AGE_MEDIAN", test.loc[pos_mask], y_pos, base_pos)
        add("POS_MODEL", test.loc[pos_mask], y_pos, pred_pos)

    print("\n=== BASE DEMAND POOLED FULL VOLUME ===")
    for name in ("ALL_AGE_MEDIAN", "DIRECT", "MIXTURE"):
        d = pd.DataFrame(pooled[name])
        if d.empty:
            continue
        y = d["y"].to_numpy(dtype=float)
        pred = d["pred"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(d)
        print("BASE_DEMAND_FULL_POOLED=" + json.dumps({
            "model": name,
            "folds": sorted(d["fold"].unique().tolist()),
            "row_metrics": stage2.volume_metrics(y, pred),
            "launch_balanced_metrics": stage2.volume_metrics(y, pred, w),
        }, ensure_ascii=False))

        for age in base.CHECKPOINT_AGES:
            g = d[d["age"].astype(int) == int(age)].copy()
            if g.empty:
                continue
            gy = g["y"].to_numpy(dtype=float)
            gp = g["pred"].to_numpy(dtype=float)
            gw = stage2.launch_balanced_weights(g)
            print("BASE_DEMAND_FULL_AGE=" + json.dumps({
                "model": name,
                "age": int(age),
                "rows": int(len(g)),
                "launches": int(g["launch_key"].nunique()),
                "launch_balanced_metrics": stage2.volume_metrics(gy, gp, gw),
            }, ensure_ascii=False))

    print("\n=== BASE DEMAND POOLED CONDITIONAL COMPONENTS ===")
    for pair_name, baseline_name, model_name in (
        ("NEGATIVE", "NEG_AGE_MEDIAN", "NEG_MODEL"),
        ("POSITIVE", "POS_AGE_MEDIAN", "POS_MODEL"),
    ):
        bd = pd.DataFrame(pooled[baseline_name])
        md = pd.DataFrame(pooled[model_name])
        if bd.empty or md.empty:
            continue
        y = md["y"].to_numpy(dtype=float)
        bp = bd["pred"].to_numpy(dtype=float)
        mp = md["pred"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(md)
        print("BASE_DEMAND_COMPONENT_POOLED=" + json.dumps({
            "component": pair_name,
            "baseline": stage2.volume_metrics(y, bp, w),
            "lightgbm": stage2.volume_metrics(y, mp, w),
        }, ensure_ascii=False))

    print("\n=== BASE_DEMAND_GUIDANCE ===")
    print("1. 先看NEGATIVE组件是否稳定优于negative age-median；若不优，不强行建复杂base-demand模型。")
    print("2. 再比较DIRECT vs MIXTURE的full future30 WAPE/Bias；更简单且更稳的方案优先。")
    print("3. MIXTURE使用raw Stage-1 probability，因此若DIRECT明显胜出，优先怀疑概率校准而非条件销量模型。")
    print("4. 重点看Day14/30，因为这是补货动作最有价值的窗口。")
    print("5. 本轮仍是完整销量点预测；P50/P75总销量分布要在选定full-volume架构后再做。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
