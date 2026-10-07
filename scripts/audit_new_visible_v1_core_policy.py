#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Research-only threshold/calibration audit for frozen NEW_VISIBLE V1 CORE model.

Uses strict temporal OOS predictions from LightGBM CORE feature set and evaluates:
- overall + age-specific score calibration
- threshold grid precision / recall / F1 / alert rate
- empirical score bins
- exploratory candidate cutoffs

Important: cutoffs reported here are descriptive OOS policy candidates, NOT production
thresholds. No DB write, no model persistence, no production promotion.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as base

THRESHOLDS = [round(x, 2) for x in np.arange(0.10, 0.71, 0.05)]
AGES = (7, 14, 30, 60, 90)


def metrics_at_threshold(y: np.ndarray, p: np.ndarray, t: float) -> Dict[str, Any]:
    pred = (p >= t).astype(int)
    return {
        "threshold": round(float(t), 2),
        "alerts": int(pred.sum()),
        "alert_rate": round(float(pred.mean()), 6),
        "precision": round(float(precision_score(y, pred, zero_division=0)), 6),
        "recall": round(float(recall_score(y, pred, zero_division=0)), 6),
        "f1": round(float(f1_score(y, pred, zero_division=0)), 6),
    }


def candidate_policy(y: np.ndarray, p: np.ndarray) -> Dict[str, Any]:
    rows = [metrics_at_threshold(y, p, t) for t in THRESHOLDS]
    max_f1 = max(rows, key=lambda r: (r["f1"], r["threshold"]))

    p50 = [r for r in rows if r["precision"] >= 0.50 and r["alerts"] >= 10]
    precision50 = min(p50, key=lambda r: r["threshold"]) if p50 else None

    r50 = [r for r in rows if r["recall"] >= 0.50]
    recall50 = max(r50, key=lambda r: r["threshold"]) if r50 else None

    return {
        "max_f1": max_f1,
        "precision50_first_cutoff": precision50,
        "recall50_highest_cutoff": recall50,
        "note": "exploratory only; final threshold needs forward/nested temporal validation",
    }


def score_bins(y: np.ndarray, p: np.ndarray) -> List[Dict[str, Any]]:
    tmp = pd.DataFrame({"y": y, "p": p})
    tmp["bin"] = pd.cut(
        tmp["p"],
        bins=np.linspace(0.0, 1.0, 11),
        include_lowest=True,
        right=True,
    )
    out: List[Dict[str, Any]] = []
    for b, g in tmp.groupby("bin", observed=True):
        out.append({
            "bin": str(b),
            "n": int(len(g)),
            "mean_score": round(float(g["p"].mean()), 6),
            "actual_rate": round(float(g["y"].mean()), 6),
        })
    return out


def build_oos() -> pd.DataFrame:
    df = base.load_rows()
    records: List[pd.DataFrame] = []
    cols = abl.CORE_NUMERIC + base.CATEGORICAL_FEATURES

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

        model = abl.make_model(abl.CORE_NUMERIC)
        weights = base.launch_balanced_weights(train)
        model.fit(
            train[cols],
            train["target"].to_numpy(dtype=int),
            clf__sample_weight=weights,
        )
        prob = model.predict_proba(test[cols])[:, 1]

        out = test[[
            "snapshot_date", "store_name", "spu", "launch_key", "age_days", "target"
        ]].copy()
        out["score"] = prob
        out["fold"] = fold_name
        records.append(out)

    if not records:
        raise RuntimeError("no OOS predictions generated")
    return pd.concat(records, ignore_index=True)


def summarize_scope(name: str, g: pd.DataFrame) -> None:
    y = g["target"].to_numpy(dtype=int)
    p = g["score"].to_numpy(dtype=float)
    print("CORE_POLICY_SCOPE=" + json.dumps({
        "segment": name,
        "rows": int(len(g)),
        "launches": int(g["launch_key"].nunique()),
        "positives": int(y.sum()),
        "positive_rate": round(float(y.mean()), 6),
        "mean_score": round(float(p.mean()), 6),
        "score_minus_actual": round(float(p.mean() - y.mean()), 6),
    }, ensure_ascii=False))

    for t in THRESHOLDS:
        print("CORE_POLICY_THRESHOLD=" + json.dumps({
            "segment": name,
            **metrics_at_threshold(y, p, t),
        }, ensure_ascii=False))

    print("CORE_POLICY_CANDIDATE=" + json.dumps({
        "segment": name,
        **candidate_policy(y, p),
    }, ensure_ascii=False))

    print("CORE_POLICY_CALIBRATION=" + json.dumps({
        "segment": name,
        "bins": score_bins(y, p),
    }, ensure_ascii=False))


def main() -> int:
    oos = build_oos()
    print("CORE_POLICY_OOS=" + json.dumps({
        "model": "NV-ML-V1-B-CORE",
        "label": base.LABEL_NAME,
        "rows": int(len(oos)),
        "launches": int(oos["launch_key"].nunique()),
        "folds": sorted(oos["fold"].unique().tolist()),
        "ages": sorted(oos["age_days"].astype(int).unique().tolist()),
        "warning": "thresholds are ranking cutoffs until calibration is validated",
    }, ensure_ascii=False))

    summarize_scope("ALL", oos)
    for age in AGES:
        g = oos[oos["age_days"].astype(int) == age]
        if not g.empty:
            summarize_scope(f"AGE_{age}", g)

    print("\n=== CORE_POLICY_GUIDANCE ===")
    print("1. Day7/14/30 should not be forced to share one threshold.")
    print("2. Use score ordering first; do not call raw score a true probability yet.")
    print("3. Candidate cutoffs are selected on pooled OOS and are descriptive, not final unbiased gates.")
    print("4. Final policy requires forward/nested temporal validation and daily first-alert backtest.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
