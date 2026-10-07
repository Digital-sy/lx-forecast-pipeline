#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forward-only policy validation for frozen NEW_VISIBLE V1 CORE scores.

Research-only.  This script does NOT select thresholds using the same future period
that it evaluates.  It first rebuilds the strict temporal-OOS CORE predictions, then
walks folds forward in time:

- 2025H2 policy may use only 2025H1 OOS outcomes
- 2026H1 policy may use only 2025H1+2025H2 OOS outcomes
- 2026H2 policy may use only earlier OOS outcomes

For each checkpoint age (7/14/30/60/90):
- MEDIUM cutoff = historical threshold with maximum F1.
- HIGH cutoff = first threshold strictly above MEDIUM with historical precision >=50%
  and at least 10 historical alerts.  If no such cutoff exists, HIGH is disabled for
  that age in that forward fold.
- LOW = score below MEDIUM.

The purpose is to test whether lifecycle-specific LOW/MEDIUM/HIGH policy is stable
when thresholds themselves are chosen only from the past.  No DB writes and no model
persistence.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

from scripts import audit_new_visible_v1_core_policy as core

FOLD_ORDER = ["2025H1", "2025H2", "2026H1", "2026H2"]
AGES = (7, 14, 30, 60, 90)
MIN_HIGH_ALERTS = 10
HIGH_PRECISION_TARGET = 0.50


def metrics(y: np.ndarray, pred: np.ndarray) -> Dict[str, Any]:
    return {
        "alerts": int(pred.sum()),
        "alert_rate": round(float(pred.mean()), 6),
        "precision": round(float(precision_score(y, pred, zero_division=0)), 6),
        "recall": round(float(recall_score(y, pred, zero_division=0)), 6),
        "f1": round(float(f1_score(y, pred, zero_division=0)), 6),
    }


def threshold_rows(g: pd.DataFrame) -> List[Dict[str, Any]]:
    y = g["target"].to_numpy(dtype=int)
    p = g["score"].to_numpy(dtype=float)
    return [core.metrics_at_threshold(y, p, t) for t in core.THRESHOLDS]


def select_policy(history: pd.DataFrame) -> Dict[str, Any]:
    rows = threshold_rows(history)
    medium = max(rows, key=lambda r: (r["f1"], -r["threshold"]))
    high_candidates = [
        r for r in rows
        if r["threshold"] > medium["threshold"]
        and r["precision"] >= HIGH_PRECISION_TARGET
        and r["alerts"] >= MIN_HIGH_ALERTS
    ]
    high = min(high_candidates, key=lambda r: r["threshold"]) if high_candidates else None
    return {"medium": medium, "high": high}


def tier_metrics(g: pd.DataFrame, medium_t: float, high_t: Optional[float]) -> Dict[str, Any]:
    p = g["score"].to_numpy(dtype=float)
    y = g["target"].to_numpy(dtype=int)
    if high_t is None:
        tier = np.where(p >= medium_t, "MEDIUM", "LOW")
    else:
        tier = np.where(p >= high_t, "HIGH", np.where(p >= medium_t, "MEDIUM", "LOW"))

    base_rate = float(y.mean()) if len(y) else 0.0
    out: Dict[str, Any] = {
        "rows": int(len(g)),
        "positives": int(y.sum()),
        "positive_rate": round(base_rate, 6),
        "medium_threshold": round(float(medium_t), 2),
        "high_threshold": round(float(high_t), 2) if high_t is not None else None,
        "tiers": {},
    }
    for name in ("LOW", "MEDIUM", "HIGH"):
        mask = tier == name
        n = int(mask.sum())
        if n == 0:
            out["tiers"][name] = {"n": 0, "actual_rate": None, "lift": None}
            continue
        actual = float(y[mask].mean())
        out["tiers"][name] = {
            "n": n,
            "actual_rate": round(actual, 6),
            "lift": round(actual / base_rate, 4) if base_rate > 0 else None,
        }

    med_plus = (p >= medium_t).astype(int)
    out["medium_plus"] = metrics(y, med_plus)
    if high_t is not None:
        out["high_only"] = metrics(y, (p >= high_t).astype(int))
    else:
        out["high_only"] = None

    rates = [
        out["tiers"][x]["actual_rate"]
        for x in ("LOW", "MEDIUM", "HIGH")
        if out["tiers"][x]["actual_rate"] is not None
    ]
    out["tier_actual_rate_monotonic"] = all(
        rates[i] <= rates[i + 1] for i in range(len(rates) - 1)
    )
    return out


def main() -> int:
    oos = core.build_oos()
    print("FORWARD_POLICY_SCOPE=" + json.dumps({
        "model": "NV-ML-V1-B-CORE",
        "label": "NV-PERSIST-750-v1",
        "fold_order": FOLD_ORDER,
        "ages": list(AGES),
        "medium_rule": "historical max-F1 threshold",
        "high_rule": "first threshold > MEDIUM with precision>=0.50 and alerts>=10",
        "note": "each test fold threshold uses prior OOS outcomes only",
    }, ensure_ascii=False))

    validated: List[pd.DataFrame] = []
    fold_summaries: List[Dict[str, Any]] = []

    for outer_idx in range(1, len(FOLD_ORDER)):
        test_fold = FOLD_ORDER[outer_idx]
        prior_folds = FOLD_ORDER[:outer_idx]
        history_all = oos[oos["fold"].isin(prior_folds)].copy()
        test_all = oos[oos["fold"] == test_fold].copy()
        if history_all.empty or test_all.empty:
            continue

        print("FORWARD_FOLD_SCOPE=" + json.dumps({
            "test_fold": test_fold,
            "policy_source_folds": prior_folds,
            "history_rows": int(len(history_all)),
            "test_rows": int(len(test_all)),
            "history_max_date": str(history_all["snapshot_date"].max().date()),
            "test_min_date": str(test_all["snapshot_date"].min().date()),
        }, ensure_ascii=False))

        fold_records: List[pd.DataFrame] = []
        age_results: List[Dict[str, Any]] = []
        for age in AGES:
            hist = history_all[history_all["age_days"].astype(int) == age].copy()
            test = test_all[test_all["age_days"].astype(int) == age].copy()
            if hist.empty or test.empty:
                continue

            selected = select_policy(hist)
            medium_t = float(selected["medium"]["threshold"])
            high_t = (
                float(selected["high"]["threshold"])
                if selected["high"] is not None else None
            )
            result = {
                "test_fold": test_fold,
                "policy_source_folds": prior_folds,
                "age": age,
                "history_rows": int(len(hist)),
                "history_positive_rate": round(float(hist["target"].mean()), 6),
                "selected_medium": selected["medium"],
                "selected_high": selected["high"],
                "test": tier_metrics(test, medium_t, high_t),
            }
            age_results.append(result)
            print("FORWARD_AGE_POLICY=" + json.dumps(result, ensure_ascii=False))

            tmp = test[[
                "snapshot_date", "store_name", "spu", "launch_key", "age_days",
                "target", "score", "fold"
            ]].copy()
            tmp["medium_threshold"] = medium_t
            tmp["high_threshold"] = high_t if high_t is not None else np.nan
            tmp["is_medium_plus"] = (tmp["score"] >= medium_t).astype(int)
            tmp["is_high"] = (
                (tmp["score"] >= high_t).astype(int) if high_t is not None else 0
            )
            fold_records.append(tmp)

        if fold_records:
            fold_df = pd.concat(fold_records, ignore_index=True)
            validated.append(fold_df)
            y = fold_df["target"].to_numpy(dtype=int)
            med = fold_df["is_medium_plus"].to_numpy(dtype=int)
            hi = fold_df["is_high"].to_numpy(dtype=int)
            fs = {
                "test_fold": test_fold,
                "rows": int(len(fold_df)),
                "positives": int(y.sum()),
                "positive_rate": round(float(y.mean()), 6),
                "medium_plus": metrics(y, med),
                "high_only": metrics(y, hi),
                "ages_evaluated": [x["age"] for x in age_results],
            }
            fold_summaries.append(fs)
            print("FORWARD_FOLD_RESULT=" + json.dumps(fs, ensure_ascii=False))

    if not validated:
        raise RuntimeError("no forward-policy folds validated")

    allv = pd.concat(validated, ignore_index=True)
    y = allv["target"].to_numpy(dtype=int)
    med = allv["is_medium_plus"].to_numpy(dtype=int)
    hi = allv["is_high"].to_numpy(dtype=int)
    print("FORWARD_POLICY_POOLED=" + json.dumps({
        "folds": sorted(allv["fold"].unique().tolist()),
        "rows": int(len(allv)),
        "positives": int(y.sum()),
        "positive_rate": round(float(y.mean()), 6),
        "medium_plus": metrics(y, med),
        "high_only": metrics(y, hi),
    }, ensure_ascii=False))

    print("\n=== FORWARD_POLICY_GUIDANCE ===")
    print("1. 只把前向验证稳定的生命周期阈值带入daily first-alert回测。")
    print("2. Day7/14若历史上无法稳定达到HIGH precision>=50%，正式策略应允许HIGH为空。")
    print("3. MEDIUM/HIGH仍是ranking policy，不把raw score解释成真实概率。")
    print("4. 本脚本通过后，下一步才做全日龄snapshot的daily first-alert回测。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
