#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forward daily first-alert backtest for frozen NEW_VISIBLE V1 CORE model.

Research-only. No DB writes, no model persistence, no production promotion.

Purpose
-------
Move from fixed checkpoint evaluation to an operational replay closer to how the
monitor would actually run every day.

For each forward test cohort (launch first_sale_day inside 2025H2 / 2026H1 / 2026H2):
1) Freeze the CORE LightGBM using only labels fully mature before the fold start.
2) Remove every test shop x SPU launch from model training.
3) Select lifecycle MEDIUM/HIGH cutoffs only from earlier checkpoint OOS outcomes.
4) Score every available daily snapshot from age 7 through age 120.
5) Apply checkpoint-derived cutoffs as lifecycle bands:
      age 7-13   -> Day7 policy
      age 14-29  -> Day14 policy
      age 30-59  -> Day30 policy
      age 60-89  -> Day60 policy
      age 90-120 -> Day90 policy
6) Evaluate the FIRST MEDIUM+ and FIRST HIGH event per launch against the exact
   PERSIST_750 label at that alert date.
7) Compare with RULE V0 first-HIGH on the same age>=7 daily rows.

Important
---------
- Raw model score remains a ranking score, not a calibrated probability.
- Launches without an age>=7 mature snapshot are reported as censored/unexposed and
  are not used in first-alert denominators.
- 2026H2 is right-censored by the historical label cutoff; exposure by age is printed.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as v1
from jobs.forecast_research import build_new_visible_snapshots_v2 as v2
from scripts import audit_new_visible_v1_core_policy as core
from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import audit_new_visible_v1_forward_policy as fwd
from scripts import train_new_visible_v1_stage1 as base
from scripts.backtest_breakout_v0_history import score_v0

AGE_BANDS: Sequence[Tuple[str, int, int, int]] = (
    ("AGE_7_13", 7, 13, 7),
    ("AGE_14_29", 14, 29, 14),
    ("AGE_30_59", 30, 59, 30),
    ("AGE_60_89", 60, 89, 60),
    ("AGE_90_120", 90, 120, 90),
)
TEST_FOLDS = fwd.FOLD_ORDER[1:]


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def load_daily_rows() -> pd.DataFrame:
    numeric = list(abl.CORE_NUMERIC)
    fields = [
        "snapshot_date", "store_name", "spu", "first_sale_day", "age_days",
        *[x for x in numeric if x != "age_days"],
        "future_sales_30d", "future_sales_first14", "future_sales_second14",
        "dataset_version",
    ]
    rows = q(
        f"""
        SELECT {','.join('`' + x + '`' for x in fields)}
        FROM `{v1.SNAPSHOT_TABLE}`
        WHERE dataset_version=%s
          AND age_days BETWEEN 0 AND 120
        ORDER BY first_sale_day, store_name, spu, snapshot_date
        """,
        (v2.DATASET_VERSION,),
    )
    if not rows:
        raise RuntimeError("daily NEW_VISIBLE snapshot is empty")

    df = pd.DataFrame(rows)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["first_sale_day"] = pd.to_datetime(df["first_sale_day"])
    df["launch_key"] = df["store_name"].astype(str) + "|" + df["spu"].astype(str)
    df["launch_month"] = df["first_sale_day"].dt.month.astype(str)
    df["snapshot_month"] = df["snapshot_date"].dt.month.astype(str)

    for col in numeric + ["future_sales_30d", "future_sales_first14", "future_sales_second14"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["target"] = (
        (df["future_sales_30d"] >= 750.0)
        & (df["future_sales_first14"] > 0.0)
        & (df["future_sales_second14"] >= 0.8 * df["future_sales_first14"])
    ).astype(int)
    return df


def band_for_age(age: int) -> Optional[Tuple[str, int]]:
    for name, lo, hi, checkpoint in AGE_BANDS:
        if lo <= age <= hi:
            return name, checkpoint
    return None


def pct(vals: Sequence[float], p: float) -> Optional[float]:
    if not vals:
        return None
    return round(float(np.quantile(np.asarray(vals, dtype=float), p)), 3)


def binary_metrics(y: np.ndarray, pred: np.ndarray) -> Dict[str, Any]:
    return {
        "alerts": int(pred.sum()),
        "alert_rate": round(float(pred.mean()), 6) if len(pred) else None,
        "precision": round(float(precision_score(y, pred, zero_division=0)), 6) if len(y) else None,
        "recall": round(float(recall_score(y, pred, zero_division=0)), 6) if len(y) else None,
        "f1": round(float(f1_score(y, pred, zero_division=0)), 6) if len(y) else None,
    }


def select_thresholds(checkpoint_oos: pd.DataFrame, prior_folds: Sequence[str]) -> Dict[int, Dict[str, Any]]:
    history = checkpoint_oos[checkpoint_oos["fold"].isin(list(prior_folds))].copy()
    out: Dict[int, Dict[str, Any]] = {}
    for checkpoint in fwd.AGES:
        g = history[history["age_days"].astype(int) == checkpoint].copy()
        if g.empty:
            continue
        selected = fwd.select_policy(g)
        out[checkpoint] = {
            "history_rows": int(len(g)),
            "history_positive_rate": round(float(g["target"].mean()), 6),
            "medium": selected["medium"],
            "high": selected["high"],
        }
    return out


def score_fold_model(
    checkpoint_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    fold_name: str,
    fold_start: pd.Timestamp,
    fold_end: pd.Timestamp,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    # Cohort membership is defined by LAUNCH DATE, not snapshot date. This makes the
    # replay closer to deploying a frozen model at the beginning of a semester and
    # following that semester's new launches forward through their lifecycle.
    cohort_all = daily_df[
        (daily_df["first_sale_day"] >= fold_start)
        & (daily_df["first_sale_day"] <= fold_end)
    ].copy()
    test_launches = set(cohort_all["launch_key"].unique())
    if not test_launches:
        return pd.DataFrame(), {"test_fold": fold_name, "reason": "no_launches"}

    train = checkpoint_df[
        (checkpoint_df["label_end_date"] < fold_start)
        & (~checkpoint_df["launch_key"].isin(test_launches))
    ].copy()
    if train.empty or train["target"].nunique() < 2:
        return pd.DataFrame(), {"test_fold": fold_name, "reason": "insufficient_training"}

    score_rows = cohort_all[cohort_all["age_days"].astype(int) >= 7].copy()
    if score_rows.empty:
        return pd.DataFrame(), {"test_fold": fold_name, "reason": "no_age7_exposure"}

    cols = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    model = abl.make_model(abl.CORE_NUMERIC)
    weights = base.launch_balanced_weights(train)
    model.fit(
        train[cols],
        train["target"].to_numpy(dtype=int),
        clf__sample_weight=weights,
    )
    score_rows["score"] = model.predict_proba(score_rows[cols])[:, 1]
    score_rows["fold"] = fold_name

    exposure = {
        "test_fold": fold_name,
        "fold_start": str(fold_start.date()),
        "fold_end": str(fold_end.date()),
        "train_rows": int(len(train)),
        "train_launches": int(train["launch_key"].nunique()),
        "train_max_label_end": str(train["label_end_date"].max().date()),
        "cohort_launches_total": int(len(test_launches)),
        "cohort_score_rows": int(len(score_rows)),
        "launch_overlap": int(len(set(train["launch_key"]) & test_launches)),
    }
    max_age = cohort_all.groupby("launch_key")["age_days"].max()
    for a in (7, 14, 30, 60, 90, 120):
        exposure[f"launches_reaching_age_{a}"] = int((max_age >= a).sum())
    return score_rows, exposure


def apply_policy(rows: pd.DataFrame, policies: Mapping[int, Mapping[str, Any]]) -> pd.DataFrame:
    x = rows.copy()
    x["policy_band"] = None
    x["checkpoint_age"] = np.nan
    x["medium_threshold"] = np.nan
    x["high_threshold"] = np.nan
    x["is_medium_plus"] = 0
    x["is_high"] = 0

    for idx, r in x.iterrows():
        info = band_for_age(int(r["age_days"]))
        if info is None:
            continue
        band, checkpoint = info
        policy = policies.get(checkpoint)
        if not policy:
            continue
        mt = float(policy["medium"]["threshold"])
        ht = float(policy["high"]["threshold"]) if policy.get("high") is not None else None
        score = float(r["score"])
        x.at[idx, "policy_band"] = band
        x.at[idx, "checkpoint_age"] = checkpoint
        x.at[idx, "medium_threshold"] = mt
        x.at[idx, "is_medium_plus"] = int(score >= mt)
        if ht is not None:
            x.at[idx, "high_threshold"] = ht
            x.at[idx, "is_high"] = int(score >= ht)

    # Fair operational comparison: V0 is also evaluated only after age 7.
    v0 = [score_v0(r)["risk"] for r in x.to_dict(orient="records")]
    x["v0_high"] = np.asarray([1 if z == "HIGH" else 0 for z in v0], dtype=int)
    return x


def first_events(rows: pd.DataFrame, flag: str) -> pd.DataFrame:
    alerts = rows[rows[flag].astype(int) == 1].copy()
    if alerts.empty:
        return alerts
    return (
        alerts.sort_values(["launch_key", "snapshot_date"])
        .groupby("launch_key", as_index=False, sort=False)
        .first()
    )


def event_summary(
    rows: pd.DataFrame,
    events: pd.DataFrame,
    flag: str,
    name: str,
) -> Dict[str, Any]:
    launch_rows = rows.groupby("launch_key", as_index=False).agg(
        max_age=("age_days", "max"),
        ever_positive=("target", "max"),
    )
    exposed = int(len(launch_rows))
    opportunities = int(launch_rows["ever_positive"].sum())

    if events.empty:
        return {
            "event": name,
            "exposed_launches": exposed,
            "opportunity_launches": opportunities,
            "alert_launches": 0,
            "launch_alert_rate": 0.0,
            "precision_at_first_alert": None,
            "useful_opportunity_recall": 0.0 if opportunities else None,
        }

    y = events["target"].to_numpy(dtype=int)
    ages = events["age_days"].astype(float).tolist()
    future30 = events["future_sales_30d"].astype(float).tolist()
    useful = int(y.sum())

    out: Dict[str, Any] = {
        "event": name,
        "exposed_launches": exposed,
        "opportunity_launches": opportunities,
        "alert_launches": int(len(events)),
        "launch_alert_rate": round(float(len(events) / exposed), 6) if exposed else None,
        "precision_at_first_alert": round(float(y.mean()), 6),
        "useful_opportunity_recall": round(float(useful / opportunities), 6) if opportunities else None,
        "first_alert_age_median": pct(ages, 0.50),
        "first_alert_age_p25": pct(ages, 0.25),
        "first_alert_age_p75": pct(ages, 0.75),
        "future30_median_at_alert": pct(future30, 0.50),
        "future30_p75_at_alert": pct(future30, 0.75),
        "alerts_by_age_band": {},
        "alerts_by_store": {},
    }
    for band, lo, hi, _ in AGE_BANDS:
        g = events[(events["age_days"].astype(int) >= lo) & (events["age_days"].astype(int) <= hi)]
        out["alerts_by_age_band"][band] = {
            "n": int(len(g)),
            "precision": round(float(g["target"].mean()), 6) if len(g) else None,
        }
    for store, g in events.groupby("store_name"):
        out["alerts_by_store"][str(store)] = {
            "n": int(len(g)),
            "precision": round(float(g["target"].mean()), 6) if len(g) else None,
            "median_age": pct(g["age_days"].astype(float).tolist(), 0.50),
        }
    return out


def policy_table(policies: Mapping[int, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for band, lo, hi, checkpoint in AGE_BANDS:
        p = policies.get(checkpoint)
        if not p:
            continue
        out.append({
            "band": band,
            "age_range": [lo, hi],
            "checkpoint_age": checkpoint,
            "history_rows": int(p["history_rows"]),
            "history_positive_rate": p["history_positive_rate"],
            "medium_threshold": p["medium"]["threshold"],
            "high_threshold": p["high"]["threshold"] if p.get("high") is not None else None,
        })
    return out


def main() -> int:
    checkpoint_df = base.load_rows()
    checkpoint_oos = core.build_oos()
    daily_df = load_daily_rows()

    print("DAILY_FIRST_ALERT_SCOPE=" + json.dumps({
        "model": "NV-ML-V1-B-CORE",
        "label": base.LABEL_NAME,
        "dataset_version": v2.DATASET_VERSION,
        "test_folds": TEST_FOLDS,
        "age_bands": [
            {"band": n, "age_range": [lo, hi], "checkpoint": cp}
            for n, lo, hi, cp in AGE_BANDS
        ],
        "model_training": "fixed checkpoint rows; label_end < fold_start; remove test launch keys",
        "policy_training": "prior checkpoint OOS folds only",
        "score_window": "daily age 7..120",
        "no_db_write": True,
    }, ensure_ascii=False))

    fold_lookup = {name: (pd.Timestamp(start), pd.Timestamp(end)) for name, start, end in base.TEST_FOLDS}
    pooled_rows: List[pd.DataFrame] = []
    pooled_events: Dict[str, List[pd.DataFrame]] = defaultdict(list)

    for fold_name in TEST_FOLDS:
        fold_idx = fwd.FOLD_ORDER.index(fold_name)
        prior_folds = fwd.FOLD_ORDER[:fold_idx]
        start_ts, end_ts = fold_lookup[fold_name]
        policies = select_thresholds(checkpoint_oos, prior_folds)
        scored, exposure = score_fold_model(checkpoint_df, daily_df, fold_name, start_ts, end_ts)
        print("DAILY_FIRST_ALERT_EXPOSURE=" + json.dumps(exposure, ensure_ascii=False))
        if scored.empty:
            continue

        print("DAILY_FIRST_ALERT_POLICY=" + json.dumps({
            "test_fold": fold_name,
            "policy_source_folds": prior_folds,
            "bands": policy_table(policies),
        }, ensure_ascii=False))

        replay = apply_policy(scored, policies)
        pooled_rows.append(replay)

        first_med = first_events(replay, "is_medium_plus")
        first_high = first_events(replay, "is_high")
        first_v0 = first_events(replay, "v0_high")
        pooled_events["ML_FIRST_MEDIUM_PLUS"].append(first_med)
        pooled_events["ML_FIRST_HIGH"].append(first_high)
        pooled_events["V0_FIRST_HIGH_AGE7_PLUS"].append(first_v0)

        for name, ev, flag in (
            ("ML_FIRST_MEDIUM_PLUS", first_med, "is_medium_plus"),
            ("ML_FIRST_HIGH", first_high, "is_high"),
            ("V0_FIRST_HIGH_AGE7_PLUS", first_v0, "v0_high"),
        ):
            print("DAILY_FIRST_ALERT_RESULT=" + json.dumps({
                "test_fold": fold_name,
                **event_summary(replay, ev, flag, name),
            }, ensure_ascii=False))

    if not pooled_rows:
        raise RuntimeError("no daily first-alert folds evaluated")

    all_rows = pd.concat(pooled_rows, ignore_index=True)
    print("\n=== POOLED DAILY FIRST-ALERT ===")
    for name, flag in (
        ("ML_FIRST_MEDIUM_PLUS", "is_medium_plus"),
        ("ML_FIRST_HIGH", "is_high"),
        ("V0_FIRST_HIGH_AGE7_PLUS", "v0_high"),
    ):
        evs = [x for x in pooled_events[name] if not x.empty]
        events = pd.concat(evs, ignore_index=True) if evs else pd.DataFrame()
        print("DAILY_FIRST_ALERT_POOLED=" + json.dumps(
            event_summary(all_rows, events, flag, name), ensure_ascii=False
        ))

    # Useful launch-level view: how many opportunities existed and how much mature-age
    # exposure each forward cohort actually had.
    launch = all_rows.groupby(["fold", "launch_key"], as_index=False).agg(
        max_age=("age_days", "max"),
        ever_positive=("target", "max"),
    )
    print("DAILY_FIRST_ALERT_CENSORING=" + json.dumps({
        "launches": int(len(launch)),
        "ever_positive_launches": int(launch["ever_positive"].sum()),
        "max_age_lt_14": int((launch["max_age"] < 14).sum()),
        "max_age_lt_30": int((launch["max_age"] < 30).sum()),
        "max_age_lt_60": int((launch["max_age"] < 60).sum()),
        "max_age_lt_90": int((launch["max_age"] < 90).sum()),
        "by_fold": {
            str(f): {
                "launches": int(len(g)),
                "ever_positive": int(g["ever_positive"].sum()),
                "median_max_age": pct(g["max_age"].astype(float).tolist(), 0.50),
                "lt30": int((g["max_age"] < 30).sum()),
                "lt60": int((g["max_age"] < 60).sum()),
            }
            for f, g in launch.groupby("fold")
        },
    }, ensure_ascii=False))

    print("\n=== DAILY_FIRST_ALERT_GUIDANCE ===")
    print("1. 第一报警 precision 比固定checkpoint precision更接近真实运营价值。")
    print("2. HIGH若最近fold第一报警precision仍明显低于50%，不得直接上线为强追单信号。")
    print("3. MEDIUM主要看早期发现率、useful opportunity recall和首次报警年龄。")
    print("4. 2026H2需结合censoring判断，age60/90样本不足时不得过度解读。")
    print("5. 若ML第一报警稳定优于V0，下一步才进入shadow模型持久化/实时特征对接。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
