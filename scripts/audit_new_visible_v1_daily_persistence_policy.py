#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Research-only persistence/debounce audit for NEW_VISIBLE V1 daily alerts.

Purpose
-------
Fixed checkpoint thresholds were useful, but applying them every day inflated first-alert
frequency because each launch gets many chances to cross a cutoff. This script keeps the
same forward-only threshold policy and adds simple confirmation rules:

- 1of1: current day crosses threshold (raw baseline)
- 2consec: current and immediately previous age-day both cross
- 2of3: current day crosses and at least 2 of last 3 age-days cross
- 3of5: current day crosses and at least 3 of last 5 age-days cross

Thresholds are NOT re-tuned here. For each forward test fold they still come only from
prior checkpoint OOS outcomes, exactly as audit_new_visible_v1_forward_policy.py.

This isolates one question:
    Does persistence confirmation improve true first-alert precision enough to make
    daily HIGH operationally credible without sacrificing too much useful recall?

Research-only: no DB writes, no model persistence, no production promotion.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd

from scripts import audit_new_visible_v1_core_policy as core
from scripts import audit_new_visible_v1_forward_policy as fwd
from scripts import backtest_new_visible_v1_daily_first_alert as daily
from scripts import train_new_visible_v1_stage1 as base


RULES: Sequence[str] = ("1of1", "2consec", "2of3", "3of5")
TEST_FOLDS = fwd.FOLD_ORDER[1:]


def _rolling_confirmation(
    g: pd.DataFrame,
    raw_col: str,
    rule: str,
) -> pd.Series:
    """Return confirmed-alert flag aligned to g.index.

    Confirmation never looks forward. It also never carries evidence across lifecycle
    bands: callers group by launch_key + checkpoint_age before invoking this helper.
    """
    x = g.sort_values("snapshot_date").copy()
    raw = x[raw_col].astype(int)

    if rule == "1of1":
        out = raw.astype(int)

    elif rule == "2consec":
        prev = raw.shift(1, fill_value=0)
        age_prev = x["age_days"].shift(1)
        consecutive = (x["age_days"] - age_prev) == 1
        out = ((raw == 1) & (prev == 1) & consecutive.fillna(False)).astype(int)

    elif rule == "2of3":
        hits = raw.rolling(window=3, min_periods=3).sum()
        # Require the current day itself to cross; this avoids alerting on a falling day.
        out = ((raw == 1) & (hits >= 2)).astype(int)

    elif rule == "3of5":
        hits = raw.rolling(window=5, min_periods=5).sum()
        out = ((raw == 1) & (hits >= 3)).astype(int)

    else:
        raise ValueError(f"unknown rule: {rule}")

    out.index = x.index
    return out.reindex(g.index).fillna(0).astype(int)


def add_confirmations(rows: pd.DataFrame) -> pd.DataFrame:
    x = rows.copy()
    for tier, raw_col in (("MEDIUM", "is_medium_plus"), ("HIGH", "is_high")):
        for rule in RULES:
            col = f"{tier.lower()}_{rule}"
            x[col] = 0

    # Do not let a hit under one lifecycle threshold confirm a later lifecycle threshold.
    valid = x[x["checkpoint_age"].notna()].copy()
    for (_, _), idx in valid.groupby(["launch_key", "checkpoint_age"]).groups.items():
        g = x.loc[list(idx)].sort_values("snapshot_date")
        for tier, raw_col in (("MEDIUM", "is_medium_plus"), ("HIGH", "is_high")):
            for rule in RULES:
                col = f"{tier.lower()}_{rule}"
                x.loc[g.index, col] = _rolling_confirmation(g, raw_col, rule).to_numpy()

    return x


def first_event(rows: pd.DataFrame, flag_col: str) -> pd.DataFrame:
    alerts = rows[rows[flag_col].astype(int) == 1].copy()
    if alerts.empty:
        return alerts
    return (
        alerts.sort_values(["launch_key", "snapshot_date"])
        .groupby("launch_key", sort=False, as_index=False)
        .head(1)
        .copy()
    )


def event_metrics(rows: pd.DataFrame, events: pd.DataFrame, name: str) -> Dict[str, Any]:
    launch_rows = (
        rows.groupby("launch_key", as_index=False)
        .agg(max_age=("age_days", "max"), ever_positive=("target", "max"))
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
            "first_alert_age_median": None,
            "future30_median_at_alert": None,
        }

    useful = int(events["target"].astype(int).sum())
    ages = events["age_days"].astype(float).to_numpy()
    future30 = events["future_sales_30d"].astype(float).to_numpy()
    return {
        "event": name,
        "exposed_launches": exposed,
        "opportunity_launches": opportunities,
        "alert_launches": int(len(events)),
        "launch_alert_rate": round(float(len(events) / exposed), 6) if exposed else None,
        "precision_at_first_alert": round(float(events["target"].mean()), 6),
        "useful_opportunity_recall": round(float(useful / opportunities), 6)
        if opportunities else None,
        "first_alert_age_median": round(float(np.median(ages)), 3),
        "first_alert_age_p25": round(float(np.quantile(ages, 0.25)), 3),
        "first_alert_age_p75": round(float(np.quantile(ages, 0.75)), 3),
        "future30_median_at_alert": round(float(np.median(future30)), 3),
        "future30_p75_at_alert": round(float(np.quantile(future30, 0.75)), 3),
    }


def build_scored_folds() -> Tuple[pd.DataFrame, pd.DataFrame]:
    checkpoint_df = base.load_rows()
    checkpoint_oos = core.build_oos()
    daily_df = daily.load_daily_rows()

    scored: List[pd.DataFrame] = []
    for fold_name, start, end in base.TEST_FOLDS:
        rows, exposure = daily.score_fold_model(
            checkpoint_df,
            daily_df,
            fold_name,
            pd.Timestamp(start),
            pd.Timestamp(end),
        )
        if rows.empty:
            continue
        rows = rows.copy()
        rows["fold"] = fold_name
        scored.append(rows)

    if not scored:
        raise RuntimeError("no daily OOS scores generated")
    return pd.concat(scored, ignore_index=True), checkpoint_oos


def main() -> int:
    scored_all, checkpoint_oos = build_scored_folds()

    print("DAILY_PERSISTENCE_SCOPE=" + json.dumps({
        "model": "NV-ML-V1-B-CORE",
        "label": base.LABEL_NAME,
        "rules": list(RULES),
        "threshold_source": "prior checkpoint OOS folds only; no threshold retuning here",
        "test_folds": TEST_FOLDS,
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled: Dict[Tuple[str, str], List[pd.DataFrame]] = {
        (tier, rule): []
        for tier in ("MEDIUM", "HIGH")
        for rule in RULES
    }

    for test_fold in TEST_FOLDS:
        outer_idx = fwd.FOLD_ORDER.index(test_fold)
        prior_folds = fwd.FOLD_ORDER[:outer_idx]

        fold_rows = scored_all[scored_all["fold"] == test_fold].copy()
        if fold_rows.empty:
            continue

        policies = daily.select_thresholds(checkpoint_oos, prior_folds)
        applied = daily.apply_policy(fold_rows, policies)
        applied = add_confirmations(applied)

        print("DAILY_PERSISTENCE_FOLD_SCOPE=" + json.dumps({
            "test_fold": test_fold,
            "policy_source_folds": prior_folds,
            "rows": int(len(applied)),
            "launches": int(applied["launch_key"].nunique()),
            "policy": daily.policy_table(policies),
        }, ensure_ascii=False))

        for tier in ("MEDIUM", "HIGH"):
            for rule in RULES:
                col = f"{tier.lower()}_{rule}"
                ev = first_event(applied, col)
                summary = event_metrics(
                    applied,
                    ev,
                    f"ML_FIRST_{tier}_{rule}",
                )
                print("DAILY_PERSISTENCE_RESULT=" + json.dumps({
                    "test_fold": test_fold,
                    "tier": tier,
                    "rule": rule,
                    **summary,
                }, ensure_ascii=False))
                if not ev.empty:
                    pooled[(tier, rule)].append(ev)

    print("\n=== POOLED DAILY PERSISTENCE ===")
    # Exposed/opportunity denominators should be computed from the union of all test rows.
    pooled_rows = scored_all[scored_all["fold"].isin(TEST_FOLDS)].copy()
    for tier in ("MEDIUM", "HIGH"):
        for rule in RULES:
            evs = pooled[(tier, rule)]
            events = pd.concat(evs, ignore_index=True) if evs else pd.DataFrame()
            summary = event_metrics(
                pooled_rows,
                events,
                f"ML_FIRST_{tier}_{rule}",
            )
            print("DAILY_PERSISTENCE_POOLED=" + json.dumps({
                "tier": tier,
                "rule": rule,
                **summary,
            }, ensure_ascii=False))

    print("\n=== DAILY_PERSISTENCE_GUIDANCE ===")
    print("1. 重点看HIGH：若2consec/2of3能在各前向fold提高first-alert precision，并保持可接受recall，说明daily去抖有效。")
    print("2. 1of1是当前daily first-alert基线；本脚本不重新挑threshold，因此提升来自持续确认而非阈值过拟合。")
    print("3. MEDIUM若即使持久化后precision仍很低，应降级为WATCH，不作为追单动作。")
    print("4. 2026H2 age60/90严重右截断；最近fold主要解读Day30附近的HIGH，不据此否定后期生命周期策略。")
    print("5. 若没有任何确认规则把HIGH稳定推到约50% precision，下一步再做daily-event级阈值前向选择，而不是继续堆特征。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
