#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forward daily-event threshold policy audit for NEW_VISIBLE V1 CORE.

Research-only. No DB writes, no model persistence, no production promotion.

Why this exists
---------------
Checkpoint-derived HIGH cutoffs worked at fixed ages but weakened under daily
first-alert replay. Persistence confirmation improved precision but did not reach a
stable ~50% across forward folds.

This script changes only the policy-selection unit:
- model stays frozen NV-ML-V1-B-CORE
- label stays NV-PERSIST-750-v1
- confirmation is fixed at 2 consecutive days (2consec)
- HIGH score thresholds are selected from PRIOR DAILY OOS FIRST-ALERT EVENTS,
  not from fixed checkpoint rows

To avoid choosing one historical precision target by feel, three policy targets are
forward-tested:
    P50: historical first-alert precision >= 0.50
    P55: historical first-alert precision >= 0.55
    P60: historical first-alert precision >= 0.60

For every lifecycle band, the lowest score threshold satisfying the requested
historical event precision and minimum alert count is selected. If none qualifies,
HIGH is disabled for that band.

Then the selected band thresholds are carried untouched into the next time fold.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from scripts import audit_new_visible_v1_daily_persistence_policy as persist
from scripts import audit_new_visible_v1_forward_policy as fwd
from scripts import backtest_new_visible_v1_daily_first_alert as daily
from scripts import train_new_visible_v1_stage1 as base


CONFIRMATION_RULE = "2consec"
POLICY_PRECISION_TARGETS: Sequence[float] = (0.50, 0.55, 0.60)
THRESHOLDS: Sequence[float] = tuple(
    round(float(x), 2) for x in np.arange(0.10, 0.81, 0.05)
)
MIN_ALERTS = 10
TEST_FOLDS = fwd.FOLD_ORDER[1:]


def band_mask(df: pd.DataFrame, lo: int, hi: int) -> pd.Series:
    age = df["age_days"].astype(int)
    return (age >= lo) & (age <= hi)


def confirmed_first_events(
    rows: pd.DataFrame,
    threshold: float,
    lo: int,
    hi: int,
) -> pd.DataFrame:
    """First confirmed 2consec event for one lifecycle band and score threshold."""
    band = rows[band_mask(rows, lo, hi)].copy()
    if band.empty:
        return band

    band["raw_candidate"] = (band["score"].astype(float) >= float(threshold)).astype(int)
    band["confirmed_candidate"] = 0

    for launch_key, idx in band.groupby("launch_key").groups.items():
        g = band.loc[list(idx)].sort_values("snapshot_date")
        flags = persist._rolling_confirmation(
            g,
            "raw_candidate",
            CONFIRMATION_RULE,
        )
        band.loc[g.index, "confirmed_candidate"] = flags.to_numpy()

    return persist.first_event(band, "confirmed_candidate")


def event_candidate(
    rows: pd.DataFrame,
    threshold: float,
    band_name: str,
    lo: int,
    hi: int,
) -> Dict[str, Any]:
    band = rows[band_mask(rows, lo, hi)].copy()
    events = confirmed_first_events(rows, threshold, lo, hi)
    summary = persist.event_metrics(
        band,
        events,
        f"HIST_{band_name}_{CONFIRMATION_RULE}_{threshold:.2f}",
    )
    return {
        "band": band_name,
        "age_range": [lo, hi],
        "threshold": round(float(threshold), 2),
        "alerts": summary["alert_launches"],
        "alert_rate": summary["launch_alert_rate"],
        "precision": summary["precision_at_first_alert"],
        "useful_recall": summary["useful_opportunity_recall"],
        "first_alert_age_median": summary.get("first_alert_age_median"),
        "future30_median": summary.get("future30_median_at_alert"),
    }


def select_band_threshold(
    history_rows: pd.DataFrame,
    band_name: str,
    lo: int,
    hi: int,
    precision_target: float,
) -> Dict[str, Any]:
    candidates = [
        event_candidate(history_rows, t, band_name, lo, hi)
        for t in THRESHOLDS
    ]
    qualified = [
        c for c in candidates
        if c["alerts"] >= MIN_ALERTS
        and c["precision"] is not None
        and float(c["precision"]) >= float(precision_target)
    ]

    # Lowest qualifying threshold preserves recall/earliness and is easy to explain.
    selected = min(qualified, key=lambda c: c["threshold"]) if qualified else None
    return {
        "band": band_name,
        "age_range": [lo, hi],
        "precision_target": precision_target,
        "min_alerts": MIN_ALERTS,
        "selected": selected,
        "candidates": candidates,
    }


def select_event_policy(
    history_rows: pd.DataFrame,
    precision_target: float,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for band_name, lo, hi, checkpoint in daily.AGE_BANDS:
        result = select_band_threshold(
            history_rows,
            band_name,
            lo,
            hi,
            precision_target,
        )
        result["checkpoint_reference"] = checkpoint
        out[band_name] = result
    return out


def apply_event_policy(
    rows: pd.DataFrame,
    policy: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    """Apply selected band thresholds + 2consec confirmation to a future fold."""
    x = rows.copy()
    x["event_high_raw"] = 0
    x["event_high_confirmed"] = 0
    x["event_high_threshold"] = np.nan
    x["event_high_band"] = None

    for band_name, lo, hi, _ in daily.AGE_BANDS:
        info = policy.get(band_name)
        selected = info.get("selected") if info else None
        if not selected:
            continue

        threshold = float(selected["threshold"])
        mask = band_mask(x, lo, hi)
        x.loc[mask, "event_high_threshold"] = threshold
        x.loc[mask, "event_high_band"] = band_name
        x.loc[mask, "event_high_raw"] = (
            x.loc[mask, "score"].astype(float) >= threshold
        ).astype(int)

        band_rows = x[mask].copy()
        for launch_key, idx in band_rows.groupby("launch_key").groups.items():
            g = x.loc[list(idx)].sort_values("snapshot_date")
            flags = persist._rolling_confirmation(
                g,
                "event_high_raw",
                CONFIRMATION_RULE,
            )
            x.loc[g.index, "event_high_confirmed"] = flags.to_numpy()

    return x


def policy_summary(policy: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for band_name, lo, hi, _ in daily.AGE_BANDS:
        info = policy.get(band_name)
        selected = info.get("selected") if info else None
        out.append({
            "band": band_name,
            "age_range": [lo, hi],
            "enabled": selected is not None,
            "threshold": selected["threshold"] if selected else None,
            "history_alerts": selected["alerts"] if selected else 0,
            "history_precision": selected["precision"] if selected else None,
            "history_useful_recall": selected["useful_recall"] if selected else None,
            "history_first_alert_age_median": (
                selected["first_alert_age_median"] if selected else None
            ),
        })
    return out


def main() -> int:
    scored_all, _checkpoint_oos = persist.build_scored_folds()

    print("DAILY_EVENT_POLICY_SCOPE=" + json.dumps({
        "model": "NV-ML-V1-B-CORE",
        "label": base.LABEL_NAME,
        "confirmation_rule": CONFIRMATION_RULE,
        "precision_targets": list(POLICY_PRECISION_TARGETS),
        "threshold_grid": list(THRESHOLDS),
        "min_historical_alerts": MIN_ALERTS,
        "selection_unit": "prior daily OOS first-alert events by lifecycle band",
        "test_folds": TEST_FOLDS,
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled_events: Dict[float, List[pd.DataFrame]] = {
        t: [] for t in POLICY_PRECISION_TARGETS
    }
    pooled_rows: Dict[float, List[pd.DataFrame]] = {
        t: [] for t in POLICY_PRECISION_TARGETS
    }

    for test_fold in TEST_FOLDS:
        fold_idx = fwd.FOLD_ORDER.index(test_fold)
        prior_folds = fwd.FOLD_ORDER[:fold_idx]
        history_rows = scored_all[scored_all["fold"].isin(prior_folds)].copy()
        test_rows = scored_all[scored_all["fold"] == test_fold].copy()
        if history_rows.empty or test_rows.empty:
            continue

        print("DAILY_EVENT_FOLD_SCOPE=" + json.dumps({
            "test_fold": test_fold,
            "policy_source_folds": prior_folds,
            "history_rows": int(len(history_rows)),
            "history_launches": int(history_rows["launch_key"].nunique()),
            "test_rows": int(len(test_rows)),
            "test_launches": int(test_rows["launch_key"].nunique()),
            "history_max_snapshot": str(history_rows["snapshot_date"].max().date()),
            "test_min_snapshot": str(test_rows["snapshot_date"].min().date()),
        }, ensure_ascii=False))

        for precision_target in POLICY_PRECISION_TARGETS:
            policy = select_event_policy(history_rows, precision_target)
            applied = apply_event_policy(test_rows, policy)
            events = persist.first_event(applied, "event_high_confirmed")
            summary = persist.event_metrics(
                applied,
                events,
                f"DAILY_EVENT_HIGH_P{int(precision_target * 100)}",
            )

            print("DAILY_EVENT_POLICY_SELECTED=" + json.dumps({
                "test_fold": test_fold,
                "precision_target": precision_target,
                "confirmation_rule": CONFIRMATION_RULE,
                "policy_source_folds": prior_folds,
                "bands": policy_summary(policy),
            }, ensure_ascii=False))

            print("DAILY_EVENT_POLICY_RESULT=" + json.dumps({
                "test_fold": test_fold,
                "precision_target": precision_target,
                "confirmation_rule": CONFIRMATION_RULE,
                **summary,
            }, ensure_ascii=False))

            pooled_rows[precision_target].append(applied)
            if not events.empty:
                pooled_events[precision_target].append(events)

    print("\n=== POOLED FORWARD DAILY-EVENT POLICY ===")
    for precision_target in POLICY_PRECISION_TARGETS:
        rows_list = pooled_rows[precision_target]
        if not rows_list:
            continue
        all_rows = pd.concat(rows_list, ignore_index=True)
        ev_list = pooled_events[precision_target]
        all_events = (
            pd.concat(ev_list, ignore_index=True)
            if ev_list else pd.DataFrame()
        )
        summary = persist.event_metrics(
            all_rows,
            all_events,
            f"DAILY_EVENT_HIGH_P{int(precision_target * 100)}",
        )
        print("DAILY_EVENT_POLICY_POOLED=" + json.dumps({
            "precision_target": precision_target,
            "confirmation_rule": CONFIRMATION_RULE,
            **summary,
        }, ensure_ascii=False))

    print("\n=== DAILY_EVENT_POLICY_GUIDANCE ===")
    print("1. 这是daily-event级前向阈值选择：测试fold从未参与自己的阈值选择。")
    print("2. 优先比较P50/P55/P60哪一档能在每个未来fold维持接近目标precision，同时保留足够recall和提前量。")
    print("3. 若P55/P60仍无法把最近fold first-alert precision稳定推到约50%，HIGH不应作为自动追单信号。")
    print("4. WATCH不在本轮重新优化；现有MEDIUM已明确只适合观察池，不作为采购动作。")
    print("5. 若某年龄段历史样本不足或无合格threshold，该年龄段HIGH应保持关闭，而不是强制补齐。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
