#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Temporal OOS research benchmark for NEW_VISIBLE Stage-1 classification.

Research-only. Reads the current strict snapshot dataset and trains two in-memory
challengers against the frozen PERSIST_750 label:

- NV-ML-V1-A: Logistic Regression baseline
- NV-ML-V1-B: LightGBM challenger

Label
-----
PERSIST_750 = future_sales_30d >= 750 AND
              future_sales_second14 >= 0.8 * future_sales_first14
              (with first14 > 0)

Leakage controls
----------------
1. Only fixed checkpoint ages 7/14/30/60/90 are used in this first benchmark, so a
   launch contributes at most five highly interpretable rows.
2. Test folds are chronological by snapshot_date.
3. Training labels must be fully mature BEFORE the test period starts:
      train_label_end = snapshot_date + 30 days < test_start
4. Any launch (store x SPU) appearing in the test fold is removed entirely from that
   fold's training data, preventing same-launch identity leakage across the boundary.
5. No SPU identifier is used as a model feature.
6. No future_* field is used as an input feature.

No database table is modified and no model is promoted/saved by this script.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd

try:
    import sklearn
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        average_precision_score,
        brier_score_loss,
        f1_score,
        log_loss,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
except Exception as exc:  # pragma: no cover - operator-facing dependency guard
    raise RuntimeError(
        "缺少 scikit-learn；先执行 ./venv/bin/pip install -r requirements-ml.txt"
    ) from exc

try:
    import lightgbm as lgb
    from lightgbm import LGBMClassifier
except Exception as exc:  # pragma: no cover
    raise RuntimeError(
        "缺少 lightgbm；先执行 ./venv/bin/pip install -r requirements-ml.txt"
    ) from exc

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as v1
from jobs.forecast_research import build_new_visible_snapshots_v2 as v2
from scripts.backtest_breakout_v0_history import score_v0

CHECKPOINT_AGES = (7, 14, 30, 60, 90)
LABEL_NAME = "NV-PERSIST-750-v1"
MODEL_LOGISTIC = "NV-ML-V1-A_LOGISTIC"
MODEL_LGBM = "NV-ML-V1-B_LIGHTGBM"
LABEL_HORIZON_DAYS = 30

NUMERIC_FEATURES = [
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
    "clicks_7d", "impressions_7d", "ad_spend_7d", "ad_orders_7d",
    "ad_sales_7d", "promotion_units_7d", "avg_price_7d",
    "ad_spend_30d", "promotion_units_30d", "avg_price_30d",
]

CATEGORICAL_FEATURES = [
    "store_name",
    "launch_month",
    "snapshot_month",
]

TEST_FOLDS = [
    ("2025H1", date(2025, 1, 1), date(2025, 6, 30)),
    ("2025H2", date(2025, 7, 1), date(2025, 12, 31)),
    ("2026H1", date(2026, 1, 1), date(2026, 6, 30)),
    ("2026H2", date(2026, 7, 1), date(2026, 9, 5)),
]


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def rate(n: int, d: int) -> float | None:
    return round(n / d, 6) if d else None


def load_rows() -> pd.DataFrame:
    fields = [
        "snapshot_date", "store_name", "spu", "first_sale_day", "age_days",
        *[x for x in NUMERIC_FEATURES if x != "age_days"],
        "future_sales_30d", "future_sales_first14", "future_sales_second14",
        "dataset_version",
    ]
    sql = f"""
        SELECT {','.join('`' + f + '`' for f in fields)}
        FROM `{v1.SNAPSHOT_TABLE}`
        WHERE dataset_version=%s
          AND age_days IN ({','.join(['%s'] * len(CHECKPOINT_AGES))})
        ORDER BY snapshot_date, store_name, spu, age_days
    """
    rows = q(sql, (v2.DATASET_VERSION, *CHECKPOINT_AGES))
    if not rows:
        raise RuntimeError("当前 dataset_version 没有固定年龄 NEW_VISIBLE snapshot")

    df = pd.DataFrame(rows)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["first_sale_day"] = pd.to_datetime(df["first_sale_day"])
    df["label_end_date"] = df["snapshot_date"] + pd.to_timedelta(LABEL_HORIZON_DAYS, unit="D")
    df["launch_key"] = df["store_name"].astype(str) + "|" + df["spu"].astype(str)
    df["launch_month"] = df["first_sale_day"].dt.month.astype(str)
    df["snapshot_month"] = df["snapshot_date"].dt.month.astype(str)

    for col in NUMERIC_FEATURES + [
        "future_sales_30d", "future_sales_first14", "future_sales_second14"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["target"] = (
        (df["future_sales_30d"] >= 750.0)
        & (df["future_sales_first14"] > 0.0)
        & (df["future_sales_second14"] >= 0.8 * df["future_sales_first14"])
    ).astype(int)
    return df


def launch_balanced_weights(df: pd.DataFrame) -> np.ndarray:
    counts = df.groupby("launch_key")["launch_key"].transform("count").astype(float)
    w = 1.0 / counts
    # normalize to mean 1 so estimator regularization scale stays intuitive
    return (w * (len(w) / w.sum())).to_numpy(dtype=float)


def make_preprocessor(scale_numeric: bool) -> ColumnTransformer:
    numeric_steps: List[Tuple[str, Any]] = [
        ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
    ]
    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler(with_mean=False)))

    return ColumnTransformer(
        transformers=[
            ("num", Pipeline(numeric_steps), NUMERIC_FEATURES),
            (
                "cat",
                Pipeline([
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("onehot", OneHotEncoder(handle_unknown="ignore")),
                ]),
                CATEGORICAL_FEATURES,
            ),
        ],
        remainder="drop",
        sparse_threshold=0.3,
    )


def make_models() -> Dict[str, Pipeline]:
    logistic = Pipeline([
        ("prep", make_preprocessor(scale_numeric=True)),
        (
            "clf",
            LogisticRegression(
                max_iter=3000,
                C=1.0,
                solver="lbfgs",
                random_state=42,
            ),
        ),
    ])

    lightgbm = Pipeline([
        ("prep", make_preprocessor(scale_numeric=False)),
        (
            "clf",
            LGBMClassifier(
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
            ),
        ),
    ])
    return {MODEL_LOGISTIC: logistic, MODEL_LGBM: lightgbm}


def safe_auc(y: np.ndarray, p: np.ndarray) -> float | None:
    if len(np.unique(y)) < 2:
        return None
    return float(roc_auc_score(y, p))


def classifier_metrics(y: np.ndarray, p: np.ndarray) -> Dict[str, Any]:
    eps = 1e-7
    p = np.clip(p.astype(float), eps, 1.0 - eps)
    out: Dict[str, Any] = {
        "rows": int(len(y)),
        "positives": int(y.sum()),
        "positive_rate": rate(int(y.sum()), int(len(y))),
        "pr_auc": round(float(average_precision_score(y, p)), 6),
        "roc_auc": round(safe_auc(y, p), 6) if safe_auc(y, p) is not None else None,
        "brier": round(float(brier_score_loss(y, p)), 6),
        "log_loss": round(float(log_loss(y, p, labels=[0, 1])), 6),
        "mean_probability": round(float(p.mean()), 6),
    }
    for threshold in (0.20, 0.30, 0.50, 0.70):
        pred = (p >= threshold).astype(int)
        suffix = str(threshold).replace(".", "p")
        out[f"precision_{suffix}"] = round(
            float(precision_score(y, pred, zero_division=0)), 6
        )
        out[f"recall_{suffix}"] = round(
            float(recall_score(y, pred, zero_division=0)), 6
        )
        out[f"f1_{suffix}"] = round(float(f1_score(y, pred, zero_division=0)), 6)
        out[f"alert_rate_{suffix}"] = round(float(pred.mean()), 6)
    return out


def calibration_bins(y: np.ndarray, p: np.ndarray) -> List[Dict[str, Any]]:
    tmp = pd.DataFrame({"y": y, "p": p})
    tmp["bin"] = pd.cut(
        tmp["p"], bins=np.linspace(0.0, 1.0, 11), include_lowest=True, right=True
    )
    out = []
    for b, g in tmp.groupby("bin", observed=True):
        out.append({
            "bin": str(b),
            "n": int(len(g)),
            "mean_probability": round(float(g["p"].mean()), 6),
            "actual_rate": round(float(g["y"].mean()), 6),
        })
    return out


def v0_metrics(test: pd.DataFrame) -> Dict[str, Any]:
    y = test["target"].to_numpy(dtype=int)
    pred = np.array([
        1 if score_v0(row)["risk"] == "HIGH" else 0
        for row in test.to_dict(orient="records")
    ], dtype=int)
    return {
        "rows": int(len(test)),
        "positives": int(y.sum()),
        "alerts": int(pred.sum()),
        "alert_rate": round(float(pred.mean()), 6),
        "precision": round(float(precision_score(y, pred, zero_division=0)), 6),
        "recall": round(float(recall_score(y, pred, zero_division=0)), 6),
        "f1": round(float(f1_score(y, pred, zero_division=0)), 6),
    }


def feature_coverage(df: pd.DataFrame) -> Dict[str, Any]:
    out = {}
    for col in NUMERIC_FEATURES:
        nonnull = int(df[col].notna().sum())
        positive = int((pd.to_numeric(df[col], errors="coerce").fillna(0) > 0).sum())
        out[col] = {
            "nonnull_rate": rate(nonnull, len(df)),
            "positive_rate": rate(positive, len(df)),
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--models",
        default="logistic,lightgbm",
        help="comma-separated: logistic,lightgbm",
    )
    parser.add_argument(
        "--show-calibration",
        action="store_true",
        help="print 10-bin calibration tables for each fold/model",
    )
    args = parser.parse_args()

    selected = {x.strip().lower() for x in args.models.split(",") if x.strip()}
    valid = {"logistic", "lightgbm"}
    if not selected or not selected.issubset(valid):
        raise RuntimeError(f"--models must be subset of {sorted(valid)}")

    df = load_rows()
    print("NV_V1_STAGE1_SCOPE=" + json.dumps({
        "dataset_version": v2.DATASET_VERSION,
        "label": LABEL_NAME,
        "checkpoint_ages": list(CHECKPOINT_AGES),
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "positives": int(df["target"].sum()),
        "positive_rate": round(float(df["target"].mean()), 6),
        "sklearn_version": sklearn.__version__,
        "lightgbm_version": lgb.__version__,
        "leakage_rule": "train label_end < test_start and remove all test launches from train",
    }, ensure_ascii=False))

    print("FEATURE_COVERAGE=" + json.dumps(feature_coverage(df), ensure_ascii=False))

    models = make_models()
    if "logistic" not in selected:
        models.pop(MODEL_LOGISTIC, None)
    if "lightgbm" not in selected:
        models.pop(MODEL_LGBM, None)

    pooled: Dict[str, Dict[str, List[Any]]] = {
        name: {"y": [], "p": [], "fold": []} for name in models
    }

    for fold_name, start, end in TEST_FOLDS:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        test = df[(df["snapshot_date"] >= start_ts) & (df["snapshot_date"] <= end_ts)].copy()
        if test.empty:
            print("FOLD_SKIPPED=" + json.dumps({"fold": fold_name, "reason": "no_test_rows"}))
            continue

        test_launches = set(test["launch_key"].unique())
        train = df[
            (df["label_end_date"] < start_ts)
            & (~df["launch_key"].isin(test_launches))
        ].copy()

        if train.empty or train["target"].nunique() < 2 or test["target"].nunique() < 2:
            print("FOLD_SKIPPED=" + json.dumps({
                "fold": fold_name,
                "reason": "insufficient_class_support",
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
            }, ensure_ascii=False))
            continue

        fold_scope = {
            "fold": fold_name,
            "test_start": str(start),
            "test_end": str(end),
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "train_positive_rate": round(float(train["target"].mean()), 6),
            "train_max_label_end": str(train["label_end_date"].max().date()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "test_positive_rate": round(float(test["target"].mean()), 6),
            "launch_overlap": int(len(set(train["launch_key"]) & test_launches)),
        }
        print("FOLD_SCOPE=" + json.dumps(fold_scope, ensure_ascii=False))
        print("V0_BASELINE=" + json.dumps({"fold": fold_name, **v0_metrics(test)}, ensure_ascii=False))

        X_train = train[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
        y_train = train["target"].to_numpy(dtype=int)
        X_test = test[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
        y_test = test["target"].to_numpy(dtype=int)
        weights = launch_balanced_weights(train)

        for model_name, model in models.items():
            model.fit(X_train, y_train, clf__sample_weight=weights)
            prob = model.predict_proba(X_test)[:, 1]
            result = {
                "fold": fold_name,
                "model": model_name,
                **classifier_metrics(y_test, prob),
            }
            print("MODEL_OOS=" + json.dumps(result, ensure_ascii=False))
            if args.show_calibration:
                print("CALIBRATION=" + json.dumps({
                    "fold": fold_name,
                    "model": model_name,
                    "bins": calibration_bins(y_test, prob),
                }, ensure_ascii=False))

            pooled[model_name]["y"].extend(y_test.tolist())
            pooled[model_name]["p"].extend(prob.tolist())
            pooled[model_name]["fold"].extend([fold_name] * len(y_test))

    print("\n=== POOLED STRICT TEMPORAL OOS ===")
    for model_name, d in pooled.items():
        if not d["y"]:
            continue
        y = np.asarray(d["y"], dtype=int)
        p = np.asarray(d["p"], dtype=float)
        print("MODEL_POOLED_OOS=" + json.dumps({
            "model": model_name,
            "folds": sorted(set(d["fold"])),
            **classifier_metrics(y, p),
        }, ensure_ascii=False))
        if args.show_calibration:
            print("POOLED_CALIBRATION=" + json.dumps({
                "model": model_name,
                "bins": calibration_bins(y, p),
            }, ensure_ascii=False))

    print("\n=== RESEARCH_DECISION_RULES ===")
    print("1. 这是 research challenger；不写生产表、不替换 PROD-V4、不改 A16。")
    print("2. 首先比较 PR-AUC，其次看 Recall/Precision，再看 Brier/LogLoss/校准。")
    print("3. 同一 launch 不允许跨 train/test；训练标签窗口必须完整结束于 test_start 之前。")
    print("4. 只有多折 temporal OOS 稳定优于 V0 且校准可接受，才进入下一轮特征/阈值研究。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
