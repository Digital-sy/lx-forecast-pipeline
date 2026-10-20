"""Independent monthly replay oracles; no trainer, SQL, or engine imports.

The functions below derive calendar boundaries, historical eligibility, frozen
cores, baseline arithmetic and metrics directly from supplied immutable records.
They deliberately do not call either the replay engine or its score helpers.
"""
from __future__ import annotations

import datetime as dt
import argparse
import hashlib
import inspect
import importlib.metadata
import json
import math
import os
from pathlib import Path
import traceback

import numpy as np
import pandas as pd

WINDOWS = {"d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
SEGMENTS = {"M1": (1, 30), "M2": (31, 60)}
SHOPS = ["JQ-US", "MT-US", "RKZ-US", "SY-US"]
VALID_STATES = {"OBSERVED", "EXPLICIT_ZERO"}
OBJECT_KEYS = ["cohort_id", "issue_date", "shop", "spu", "window"]


def date(value):
    return pd.Timestamp(value).date()


def finite(value):
    return value is not None and pd.notna(value) and math.isfinite(float(value))


def target_window(issue, window):
    issue = date(issue)
    first, last = WINDOWS[window]
    return issue + dt.timedelta(days=first), issue + dt.timedelta(days=last)


def issue_grid(first="2025-01", last="2026-10", truth_end="2026-10-01"):
    rows = []
    for month in pd.period_range(first, last, freq="M"):
        issue = month.start_time.date() + dt.timedelta(days=1)
        for window in WINDOWS:
            start, end = target_window(issue, window)
            rows.append({"issue_month": str(month), "issue_date": issue.isoformat(),
                         "input_cutoff": (issue - dt.timedelta(days=2)).isoformat(),
                         "window": window, "target_start": start.isoformat(), "target_end": end.isoformat(),
                         "window_days": (end - start).days + 1, "calendar_mature": end <= date(truth_end)})
    return pd.DataFrame(rows)


def direct_dates(actual_issue, simulated_issue, segment):
    actual_cutoff = date(actual_issue) - dt.timedelta(days=2)
    simulated = date(simulated_issue)
    cutoff = simulated - dt.timedelta(days=2)
    first, last = SEGMENTS[segment]
    label_start, label_end = simulated + dt.timedelta(days=first), simulated + dt.timedelta(days=last)
    return {"input_cutoff": cutoff, "history_start": cutoff - dt.timedelta(days=27), "history_end": cutoff,
            "label_start": label_start, "label_end": label_end, "label_mature": label_end <= actual_cutoff}


def positive_completed_months(monthly, shop, spu, issue):
    cutoff = date(issue) - dt.timedelta(days=2)
    sub = monthly[monthly.shop.eq(shop) & monthly.spu.eq(spu)].copy()
    sub["period"] = pd.PeriodIndex(sub.month, freq="M")
    if sub.duplicated("period").any():
        raise ValueError("monthly qualification source is not unique")
    return sum(row.qty > 0 and row.period.end_time.date() <= cutoff
               for row in sub.itertuples() if finite(row.qty))


def _daily(source):
    table = source.copy()
    table["ds"] = pd.to_datetime(table.ds).dt.normalize()
    if table.duplicated(["shop", "spu", "ds"]).any():
        raise ValueError("daily source grain is not unique")
    return table


def daily_total(source, shop, spu, start, end, truth_end=None):
    """Exact calendar sum; only explicit valid zero rows contribute zeros."""
    start, end = date(start), date(end)
    expected = (end - start).days + 1
    if expected <= 0:
        raise ValueError("empty or inverted target interval")
    if truth_end is not None and end > date(truth_end):
        return {"actual": None, "truth_status": "NOT_MATURED", "expected_days": expected, "known_days": 0}
    table = _daily(source)
    rows = table[table.shop.eq(shop) & table.spu.eq(spu)].set_index("ds")
    aligned = rows.reindex(pd.date_range(start, end))
    numeric = pd.to_numeric(aligned.y, errors="coerce")
    known = np.isfinite(numeric) & numeric.ge(0) & aligned.sales_state.isin(VALID_STATES) & aligned.mapped.eq(True)
    complete = bool(known.all())
    return {"actual": float(numeric.sum()) if complete else None,
            "truth_status": "OBSERVED" if complete else "UNKNOWN_TRUTH",
            "expected_days": expected, "known_days": int(known.sum())}


def baseline_predictions(source, shop, spu, issue):
    issue = date(issue)
    cutoff = issue - dt.timedelta(days=2)
    start = cutoff - dt.timedelta(days=27)
    table = _daily(source)
    check = daily_total(table, shop, spu, start, cutoff)
    result = {"history_start": str(start), "history_end": str(cutoff), "sum28": check["actual"],
              "mean28": None, "input_status": "MISSING_INPUT", "F28": {}, "WEEKDAY4": {}}
    if check["truth_status"] != "OBSERVED":
        for model in ["F28", "WEEKDAY4"]:
            result[model] = {window: None for window in WINDOWS}
        return result
    rows = table[table.shop.eq(shop) & table.spu.eq(spu)].set_index("ds").reindex(pd.date_range(start, cutoff))
    rate = float(rows.y.sum()) / 28
    weekday = {day: float(rows.loc[rows.index.dayofweek == day, "y"].sum()) / 4 for day in range(7)}
    result.update(mean28=rate, input_status="OK")
    for window in WINDOWS:
        a, b = target_window(issue, window)
        target = pd.date_range(a, b)
        result["F28"][window] = len(target) * rate
        result["WEEKDAY4"][window] = sum(weekday[day.weekday()] for day in target)
    return result


def rank_core(totals, full_shop_units, fraction=.9):
    """Original full-shop denominator and all boundary ties, including failure to reach it."""
    if not 0 < fraction <= 1 or not finite(full_shop_units) or full_shop_units < 0:
        raise ValueError("invalid core-selection denominator")
    ranked = totals.sort_values(["selection_qty", "spu"], ascending=[False, True]).copy()
    if ranked.selection_qty.isna().any() or (ranked.selection_qty < 0).any():
        raise ValueError("unknown or negative selection quantity")
    if full_shop_units == 0 or ranked.empty:
        ranked["is_core"] = False
        return ranked, None
    threshold = fraction * full_shop_units
    cumulative, boundary = 0., None
    for row in ranked.itertuples():
        cumulative += float(row.selection_qty)
        if cumulative >= threshold:
            boundary = float(row.selection_qty)
            break
    ranked["is_core"] = True if boundary is None else ranked.selection_qty.ge(boundary)
    return ranked, boundary


def selection_snapshot(source, shop, issue, fraction=.9):
    """Store-date completeness gates a core; object absence is not invented zero history."""
    cutoff = date(issue) - dt.timedelta(days=2)
    start = cutoff - dt.timedelta(days=89)
    table = _daily(source)
    table = table[table.shop.eq(shop) & table.ds.between(pd.Timestamp(start), pd.Timestamp(cutoff))]
    expected = set(pd.date_range(start, cutoff))
    present = set(table.ds)
    numeric = pd.to_numeric(table.y, errors="coerce")
    invalid = ~np.isfinite(numeric) | numeric.lt(0) | ~table.sales_state.isin(VALID_STATES)
    quality = {"issue_date": str(date(issue)), "shop": shop, "selection_start": str(start), "selection_end": str(cutoff),
               "expected_days": 90, "present_days": len(present), "invalid_days": int(table.loc[invalid, "ds"].nunique()),
               "core_n": None, "full_shop_units": None, "mapped_units": None, "unmapped_units": None,
               "selected_units": None, "selection_coverage": None, "boundary_qty": None}
    if expected != present or invalid.any():
        quality["selection_status"] = "INSUFFICIENT_HISTORY" if expected != present else "INVALID_SELECTION_INPUT"
        return pd.DataFrame(columns=["shop", "spu", "selection_qty", "is_core"]), quality
    full = float(numeric.sum())
    mapped = table[table.mapped.eq(True)]
    totals = mapped.groupby(["shop", "spu"], as_index=False).agg(selection_qty=("y", "sum"))
    ranked, boundary = rank_core(totals, full, fraction)
    selected = float(ranked.loc[ranked.is_core, "selection_qty"].sum())
    quality.update(full_shop_units=full, mapped_units=float(mapped.y.sum()), unmapped_units=float(table.loc[~table.mapped.eq(True), "y"].sum()),
                   core_n=int(ranked.is_core.sum()), selected_units=selected,
                   selection_coverage=selected / full if full > 0 else None, boundary_qty=boundary,
                   selection_status="NO_POSITIVE_SALES" if full == 0 else "MAPPED_BELOW_TARGET" if selected < fraction * full else "OK")
    return ranked, quality


def score_record(actual, prediction):
    valid_actual = finite(actual) and actual >= 0
    valid_prediction = finite(prediction) and prediction >= 0
    comparable = valid_actual and valid_prediction
    absolute = abs(float(prediction) - float(actual)) if comparable else None
    relative = (float(prediction) - float(actual)) / float(actual) if comparable and actual > 0 else None
    decision = "UNJUDGED" if relative is None else "HIT" if absolute <= .2 * actual else "MISS"
    return {"absolute_error": absolute, "relative_error": relative, "decision": decision}


def metrics(frame):
    actual = pd.to_numeric(frame.actual, errors="coerce")
    prediction = pd.to_numeric(frame.prediction, errors="coerce")
    known, output = np.isfinite(actual) & actual.ge(0), np.isfinite(prediction) & prediction.ge(0)
    paired = known & output
    positive, zero = known & actual.gt(0), known & actual.eq(0)
    error = prediction[paired] - actual[paired]
    hit = paired & positive & (prediction - actual).abs().le(.2 * actual)
    miss = paired & positive & ~hit
    denom, full_denom = float(actual[paired].sum()), float(actual[known].sum())
    immature = frame.truth_status.eq("NOT_MATURED") if "truth_status" in frame else pd.Series(False, index=frame.index)
    return {"fixed_core_n": len(frame), "output_n": int(output.sum()), "truth_n": int(known.sum()),
            "missing_output_n": int((~output).sum()), "missing_truth_n": int((~known).sum()),
            "unknown_truth_n": int((~known & ~immature).sum()), "not_matured_n": int(immature.sum()),
            "scored_n": int(paired.sum()), "positive_truth_n": int(positive.sum()), "zero_actual_n": int(zero.sum()),
            "scored_zero_actual_n": int((paired & zero).sum()),
            "hit_n": int(hit.sum()), "miss_n": int(miss.sum()), "unjudged_n": len(frame) - int(hit.sum()) - int(miss.sum()),
            "hit_rate": float(hit.sum() / positive.sum()) if positive.any() else None,
            "full_core_hit_share": float(hit.sum() / len(frame)) if len(frame) else None,
            "hit_sales_share": float(actual[hit].sum() / full_denom) if full_denom else None,
            "actual_units": denom, "known_fixed_core_actual_units": full_denom,
            "forecast_units": float(prediction[paired].sum()), "wape": float(error.abs().sum() / denom) if denom else None,
            "bias": float(error.sum() / denom) if denom else None,
            "over_units": float(error.clip(lower=0).sum()), "under_units": float((-error).clip(lower=0).sum()),
            "zero_actual_absolute_error": float((prediction[paired & zero] - actual[paired & zero]).abs().sum())}


def paired_with_f28(scores, candidate, keys=None):
    keys = OBJECT_KEYS if keys is None else keys
    left = scores[scores.model.eq("F28")]
    right = scores[scores.model.eq(candidate)]
    if left.duplicated(keys).any() or right.duplicated(keys).any():
        raise ValueError("paired score grain is not unique")
    if set(left[keys].itertuples(index=False, name=None)) != set(right[keys].itertuples(index=False, name=None)):
        raise ValueError("candidate did not preserve the frozen cohort")
    paired = left.merge(right, on=keys, suffixes=("_f28", "_candidate"), validate="one_to_one")
    both_unknown = paired.actual_f28.isna() & paired.actual_candidate.isna()
    if not ((paired.actual_f28 == paired.actual_candidate) | both_unknown).all():
        raise ValueError("candidate and F28 use different truth")
    paired["paired"] = np.isfinite(paired.actual_f28) & np.isfinite(paired.prediction_f28) & np.isfinite(paired.prediction_candidate)
    return paired


def paired_metrics(pairs):
    valid = pairs[pairs.paired]
    f28 = metrics(valid.rename(columns={"actual_f28": "actual", "prediction_f28": "prediction"}))
    candidate = metrics(valid.rename(columns={"actual_candidate": "actual", "prediction_candidate": "prediction"}))
    return {"fixed_core_n": len(pairs), "paired_n": len(valid), "unpaired_n": len(pairs) - len(valid),
            "f28": f28, "candidate": candidate}


def f28_shape(mean28, actual1, actual2):
    prediction = 30 * float(mean28) if finite(mean28) and mean28 >= 0 else None
    result = {"mean28": mean28, "prediction1": prediction, "prediction2": prediction,
              "prediction60": 2 * prediction if prediction is not None else None,
              "actual1": actual1, "actual2": actual2, "actual60": None, "shape_ratio": None,
              "shape_status": "UNKNOWN_TRUTH", "equal_prediction_cannot_hit_both": None,
              "actual_mean1_to_input_mean": None, "actual_mean2_to_input_mean": None,
              "level_status": "UNKNOWN_INPUT"}
    if not (finite(actual1) and finite(actual2) and actual1 >= 0 and actual2 >= 0):
        return result
    result["actual60"] = float(actual1) + float(actual2)
    if finite(mean28) and mean28 > 0:
        result["actual_mean1_to_input_mean"] = float(actual1) / (30 * mean28)
        result["actual_mean2_to_input_mean"] = float(actual2) / (30 * mean28)
    elif finite(mean28) and mean28 == 0:
        result["level_status"] = "ZERO_INPUT_MEAN"
    if actual1 == 0 or actual2 == 0:
        result["shape_status"] = "ZERO_ACTUAL"
        return result
    low, high = min(actual1, actual2), max(actual1, actual2)
    impossible = 2 * high > 3 * low
    result.update(shape_ratio=float(high / low), shape_status="VALID_POSITIVE_PAIR", equal_prediction_cannot_hit_both=bool(impossible))
    if prediction is not None:
        result["level_status"] = "SHAPE_PRECLUDES_DUAL_HIT" if impossible else "BELOW_FEASIBLE_LEVEL" if prediction < .8 * high else "ABOVE_FEASIBLE_LEVEL" if prediction > 1.2 * low else "WITHIN_FEASIBLE_LEVEL"
    return result


def joint_decision(decisions):
    decisions = list(decisions)
    if len(decisions) != 3 or any(value == "UNJUDGED" for value in decisions):
        return "UNJUDGED"
    return "HIT" if all(value == "HIT" for value in decisions) else "MISS"


def direct_features(history, shop, target_start):
    """Original native integer-history representation, independently accumulated."""
    history = np.asarray(history, dtype=np.float32)
    if len(history) != 28 or not np.isfinite(history).all() or (history < 0).any():
        raise ValueError("direct features require 28 valid consecutive values")
    fields = {"shop_code": SHOPS.index(shop), **{f"lag{lag}": float(history[-lag]) for lag in [1, 7, 14, 28]}}
    for width in [7, 28]:
        total = np.float32(0)
        for value in history[-width:]:
            total = np.float32(total + value)
        fields[f"rolling_mean_lag1_window_size{width}"] = float(np.float32(total / np.float32(width)))
    target = date(target_start)
    fields.update(dayofweek=target.weekday(), month=target.month, dayofyear=target.timetuple().tm_yday)
    return fields


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def equal_number(observed, expected, label, tolerance=1e-8):
    if not finite(observed) and not finite(expected):
        if (observed is not None and pd.notna(observed)) or (expected is not None and pd.notna(expected)):
            raise AssertionError(f"{label}: infinity is not missing")
        return 0.
    if not finite(observed) or not finite(expected):
        raise AssertionError(f"{label}: observed={observed}, expected={expected}")
    difference = abs(float(observed) - float(expected))
    if difference > tolerance:
        raise AssertionError(f"{label}: observed={observed}, expected={expected}, difference={difference}")
    return difference


class CalendarOracle:
    """Dense calendar masks permit independent full-population sums without imputation."""
    def __init__(self, source, truth_end="2026-10-01"):
        source = _daily(source)
        self.first = date(source.ds.min())
        self.last = date(source.ds.max())
        self.truth_end = date(truth_end)
        self.length = (self.last - self.first).days + 1
        self.data = {}
        for key, group in source.groupby(["shop", "spu"], dropna=False, sort=False):
            positions = (group.ds - pd.Timestamp(self.first)).dt.days.to_numpy()
            values = np.full(self.length, np.nan)
            known = np.zeros(self.length, dtype=bool)
            y = pd.to_numeric(group.y, errors="coerce").to_numpy(dtype=float)
            values[positions] = y
            known[positions] = np.isfinite(y) & (y >= 0) & group.sales_state.isin(VALID_STATES).to_numpy() & group.mapped.eq(True).to_numpy()
            self.data[key] = (values, known, np.r_[0., np.cumsum(np.where(known, values, 0.))], np.r_[0, np.cumsum(known)])

    def total(self, shop, spu, start, end, maturity=True):
        start, end = date(start), date(end)
        if maturity and end > self.truth_end:
            return None, "NOT_MATURED"
        left, right = (start - self.first).days, (end - self.first).days + 1
        if left < 0 or right > self.length or right <= left or (shop, spu) not in self.data:
            return None, "UNKNOWN_TRUTH"
        values, known, sums, counts = self.data[(shop, spu)]
        if counts[right] - counts[left] != right - left:
            return None, "UNKNOWN_TRUTH"
        return float(sums[right] - sums[left]), "OBSERVED"

    def history(self, shop, spu, issue, width=28):
        cutoff = date(issue) - dt.timedelta(days=2)
        value, status = self.total(shop, spu, cutoff - dt.timedelta(days=width - 1), cutoff, maturity=False)
        if status != "OBSERVED":
            return None
        right = (cutoff - self.first).days + 1
        return self.data[(shop, spu)][0][right - width:right]

    def baselines(self, shop, spu, issue):
        history = self.history(shop, spu, issue)
        if history is None:
            return None, None
        cutoff = date(issue) - dt.timedelta(days=2)
        history_dates = [cutoff - dt.timedelta(days=27 - i) for i in range(28)]
        weekday = {day: sum(float(history[i]) for i, d in enumerate(history_dates) if d.weekday() == day) / 4 for day in range(7)}
        return float(history.sum() / 28), weekday


def verified_manifest(folder, name):
    manifest = json.loads((Path(folder) / name).read_text(encoding="utf-8-sig"))
    for relative, digest in manifest.get("files", {}).items():
        if sha(Path(folder) / relative) != digest:
            raise AssertionError(f"manifest mismatch: {folder}/{relative}")
    for path, digest in manifest.get("sources", {}).items():
        if sha(path) != digest:
            raise AssertionError(f"input source changed: {path}")
    return manifest


def monthly_positive_ends(monthly):
    source = monthly.copy()
    source["period"] = pd.PeriodIndex(source.month, freq="M")
    if source.duplicated(["shop", "spu", "period"]).any():
        raise AssertionError("monthly source has duplicate qualification records")
    return {pair: np.array(sorted(period.end_time.date().toordinal() for period in group.period), dtype=np.int64)
            for pair, group in source[source.qty.gt(0)].groupby(["shop", "spu"], sort=False)}


def reconstruct_direct_training(oracle, positive_ends, issue, segment):
    """Rebuild every accepted historical key, target and feature from source masks."""
    cutoff = date(issue) - dt.timedelta(days=2)
    dates = pd.date_range(oracle.first + dt.timedelta(days=29), cutoff)
    historical_cutoffs = dates - pd.Timedelta(days=2)
    cutoff_ordinals = np.array([value.date().toordinal() for value in historical_cutoffs])
    positions = cutoff_ordinals - oracle.first.toordinal()
    first, last = SEGMENTS[segment]
    starts, ends = dates + pd.Timedelta(days=first), dates + pd.Timedelta(days=last)
    label_left = positions + 2 + first
    label_right = positions + 2 + last + 1
    mature = np.asarray(ends <= pd.Timestamp(cutoff))
    parts = []
    for (shop, spu), (raw, known, totals, counts) in sorted(oracle.data.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))):
        if shop not in SHOPS:
            continue
        eligible = np.searchsorted(positive_ends.get((shop, spu), np.array([], dtype=np.int64)), cutoff_ordinals, side="right") >= 4
        history_ok = counts[positions + 1] - counts[positions - 27] == 28
        bounded = (label_left >= 0) & (label_right <= oracle.length)
        label_ok = counts[np.clip(label_right, 0, oracle.length)] - counts[np.clip(label_left, 0, oracle.length)] == 30
        chosen = np.flatnonzero(eligible & mature & history_ok & bounded & label_ok)
        if not len(chosen):
            continue
        history_positions = positions[chosen, None] + np.arange(-27, 1)
        history = raw[history_positions].astype(np.float32)
        frame = pd.DataFrame({"shop": shop, "spu": spu, "simulated_issue_date": dates[chosen],
            "input_cutoff": historical_cutoffs[chosen], "label_start": starts[chosen], "label_end": ends[chosen],
            "y_label": totals[label_right[chosen]] - totals[label_left[chosen]], "shop_code": SHOPS.index(shop),
            **{f"lag{lag}": history[:, -lag] for lag in [1, 7, 14, 28]}})
        for width in [7, 28]:
            accumulator = np.zeros(len(chosen), dtype=np.float32)
            for column in range(28 - width, 28):
                accumulator += history[:, column]
            frame[f"rolling_mean_lag1_window_size{width}"] = accumulator / np.float32(width)
        frame["dayofweek"] = starts[chosen].dayofweek
        frame["month"] = starts[chosen].month
        frame["dayofyear"] = starts[chosen].dayofyear
        parts.append(frame)
    columns = ["shop", "spu", "simulated_issue_date", "input_cutoff", "label_start", "label_end", "y_label",
               "shop_code", "lag1", "lag7", "lag14", "lag28", "rolling_mean_lag1_window_size7",
               "rolling_mean_lag1_window_size28", "dayofweek", "month", "dayofyear"]
    return pd.concat(parts, ignore_index=True)[columns] if parts else pd.DataFrame(columns=columns)


def reconstruct_recursive_training(source, positive_ends, issue):
    """Independent original actual-issue eligibility and 56-day segment transform.

Exogenous NaNs are retained as in the frozen MLForecast input handling. Only
the generated lag feature / target nulls are dropped; this is not an exogenous
complete-case training policy.
"""
    cutoff = pd.Timestamp(issue) - pd.Timedelta(days=2)
    hist = source[source.ds.le(cutoff) & source.shop.isin(SHOPS)]
    hist = hist[hist.sales_state.isin(VALID_STATES) & hist.mapped.eq(True)].copy()
    covariates = ["listing_sessions_sum", "null_sessions", "order_qty", "ad_spend", "ad_clicks", "ad_orders"]
    for column in covariates:
        if column not in hist:
            hist[column] = np.nan
    parts = []
    for (shop, spu), group in hist.groupby(["shop", "spu"], sort=True):
        if np.searchsorted(positive_ends.get((shop, spu), []), cutoff.date().toordinal(), side="right") < 4:
            continue
        group = group.sort_values("ds")
        run_ids = group.ds.diff().ne(pd.Timedelta(days=1)).cumsum()
        for run_id, run in group.groupby(run_ids):
            if len(run) < 56:
                continue
            target = run.y.to_numpy(dtype=np.float32)
            samples = np.arange(28, len(run))
            history = target[samples[:, None] + np.arange(-28, 0)]
            times = pd.DatetimeIndex(run.ds.iloc[28:])
            frame = pd.DataFrame({"unique_id": f"{shop}|{spu}|run{run_id}", "ds": times,
                                  "y": target[28:], "shop_code": SHOPS.index(shop)})
            sessions = run.listing_sessions_sum.where(run.null_sessions.eq(0))
            count = sessions.rolling(28, min_periods=28).sum().shift(1)
            frame["session_mean28"] = sessions.rolling(28, min_periods=28).mean().shift(1).iloc[28:].to_numpy()
            frame["order_session_ratio28"] = (run.order_qty.rolling(28, min_periods=28).sum().shift(1) / count.where(count.gt(0))).iloc[28:].to_numpy()
            for column in ["ad_spend", "ad_clicks", "ad_orders"]:
                frame[column + "_mean28"] = run[column].rolling(28, min_periods=28).mean().shift(1).iloc[28:].to_numpy()
            for lag in [1, 7, 14, 28]:
                frame[f"lag{lag}"] = history[:, -lag]
            for width in [7, 28]:
                accumulator = np.zeros(len(samples), dtype=np.float32)
                for column in range(28 - width, 28):
                    accumulator += history[:, column]
                frame[f"rolling_mean_lag1_window_size{width}"] = accumulator / np.float32(width)
            frame["dayofweek"] = times.dayofweek.astype(np.uint8)
            frame["month"] = times.month.astype(np.uint8)
            frame["dayofyear"] = times.dayofyear.astype(np.uint16)
            parts.append(frame)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def compare_training_audit(expected, path):
    audit = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if len(expected) != audit["rows"]:
        raise AssertionError(f"full training row count differs: {path}")
    canonical = expected.sort_values(audit["sort_keys"], kind="stable")[audit["columns"]]
    verified = {}
    for column in audit["columns"]:
        if str(canonical[column].dtype) != audit["dtypes"][column]:
            raise AssertionError(f"training native dtype differs at {path}: {column} {canonical[column].dtype} vs {audit['dtypes'][column]}")
        digest = hashlib.sha256(pd.util.hash_pandas_object(canonical[column], index=False).to_numpy().tobytes()).hexdigest()
        if digest != audit["column_sha256"][column]:
            raise AssertionError(f"full training column mismatch at {path}: {column}")
        verified[column] = digest
    return {"rows": len(expected), "columns": len(verified), "audit_sha256": sha(path), "column_sha256": verified}


def verify_training(root, source, monthly, oracle, selected=None):
    positive_ends = monthly_positive_ends(monthly)
    results = []
    for month in pd.period_range("2025-01", "2026-08", freq="M"):
        issue = str((month.start_time + pd.Timedelta(days=1)).date())
        recursive = None
        for kind in ["B1", "B2", "B3", "M1", "M2"]:
            if selected is not None and (issue, kind) not in selected:
                continue
            slot = root / "effects/slots" / f"{issue}_{kind}"
            audit_path = slot / "training_audit.json"
            if not audit_path.exists():
                result_path = slot / "result.json"
                if result_path.exists() and json.loads(result_path.read_text(encoding="utf-8-sig")).get("status") in {"FITTED", "RECOVERED"}:
                    raise AssertionError("successful model has no full training column audit")
                continue
            if kind.startswith("B"):
                if recursive is None:
                    recursive = reconstruct_recursive_training(source, positive_ends, issue)
                expected = recursive
            else:
                expected = reconstruct_direct_training(oracle, positive_ends, issue, kind)
            verified = compare_training_audit(expected, audit_path)
            results.append({"issue_date": issue, "kind": kind, **verified})
            print(f"independent training {issue}/{kind}: {verified['rows']} rows, {verified['columns']} columns", flush=True)
        del recursive
    return results


def _check_booster(booster, frozen_parameters):
    if booster.num_trees() > 200:
        raise AssertionError("saved model exceeded tree budget")
    aliases = {"n_estimators": "num_iterations", "min_child_samples": "min_data_in_leaf",
               "reg_lambda": "lambda_l2", "random_state": "seed", "n_jobs": "num_threads"}
    for field, value in frozen_parameters.items():
        # Pickled sklearn boosters retain sklearn aliases; native text models use
        # LightGBM canonical names. Neither route changes the frozen parameters.
        observed = booster.params.get(field, booster.params.get(aliases.get(field, field)))
        if observed != value:
            raise AssertionError(f"saved model parameter changed: {field}={observed}, expected={value}")


def verify_saved_checkpoints(root, source, monthly, oracle, cohorts, predictions=None, fitted=None):
    """Read saved models and predict once. This verifier has no fit calls."""
    from lightgbm import Booster
    from mlforecast import MLForecast
    frozen = json.loads((root / "prepared/frozen_config.json").read_text(encoding="utf-8-sig"))["original"]
    if fitted is None:
        fitted = json.loads((root / "effects/fits.json").read_text(encoding="utf-8-sig"))
    positive_ends = monthly_positive_ends(monthly)
    source_by_pair = {key: group.set_index("ds") for key, group in source.groupby(["shop", "spu"], sort=False)}
    direct_columns = ["shop_code", "lag1", "lag7", "lag14", "lag28", "rolling_mean_lag1_window_size7", "rolling_mean_lag1_window_size28", "dayofweek", "month", "dayofyear"]
    exog = {"B1": [], "B2": ["session_mean28", "order_session_ratio28"],
            "B3": ["session_mean28", "order_session_ratio28", "ad_spend_mean28", "ad_clicks_mean28", "ad_orders_mean28"]}
    reports = []
    for result in fitted:
        issue, kind = result["issue_date"], result["kind"]
        if result["status"] not in {"FITTED", "RECOVERED"}:
            continue
        slot = root / "effects/slots" / f"{issue}_{kind}"
        checkpoint = json.loads((slot / "checkpoint.json").read_text(encoding="utf-8-sig"))
        if result.get("fit_seconds", 0) > 600:
            raise AssertionError("successful fit exceeded frozen time budget")
        cutoff = date(issue) - dt.timedelta(days=2)
        slot_predictions = pd.read_parquet(slot / "predictions.parquet")
        maximum = 0.
        if kind in SEGMENTS:
            path = slot / "model.txt"
            if sha(path) != result["model_sha256"] or result["model_sha256"] != checkpoint["model_sha256"]:
                raise AssertionError("DIRECT checkpoint hash changed")
            booster = Booster(model_file=os.path.relpath(path, Path.cwd()))
            _check_booster(booster, frozen["lightgbm"])
            if booster.feature_name() != direct_columns:
                raise AssertionError("DIRECT feature names differ from the frozen ten")
            inference = pd.read_parquet(slot / "segment_predictions.parquet")
            expected_objects = set(cohorts.loc[cohorts.issue_date.eq(issue), ["shop", "spu"]].itertuples(index=False, name=None))
            if len(inference) != len(expected_objects) or set(inference[["shop", "spu"]].itertuples(index=False, name=None)) != expected_objects:
                raise AssertionError("DIRECT inference changed cohort union")
            own_features, valid_indices = [], []
            for index, row in inference.iterrows():
                count = np.searchsorted(positive_ends.get((row.shop, row.spu), []), cutoff.toordinal(), side="right")
                history = oracle.history(row.shop, row.spu, issue)
                status = "INSUFFICIENT_QUALIFICATION" if count < 4 else "MISSING_HISTORY" if history is None else "OK"
                if row.input_status != status:
                    raise AssertionError("DIRECT inference qualification/history status differs")
                expected_dates = direct_dates(issue, issue, kind)
                for field in ["input_cutoff", "history_start", "history_end", "label_start", "label_end"]:
                    if date(row[field]) != expected_dates[field]:
                        raise AssertionError("DIRECT inference temporal feature anchor changed")
                if status == "OK":
                    expected_feature = direct_features(history, row.shop, expected_dates["label_start"])
                    for field, value in expected_feature.items():
                        equal_number(row[field], value, "DIRECT saved inference feature " + field, 0.)
                    own_features.append(expected_feature)
                    valid_indices.append(index)
            expected_values = booster.predict(pd.DataFrame(own_features, columns=direct_columns), validate_features=True, num_threads=2) if own_features else []
            raw = dict(zip(valid_indices, expected_values))
            for index, row in inference.iterrows():
                value = raw.get(index)
                maximum = max(maximum, equal_number(row.prediction_raw, value, "saved DIRECT raw prediction", 1e-8))
                maximum = max(maximum, equal_number(row.prediction, max(value, 0) if value is not None else None, "saved DIRECT clipped prediction", 1e-8))
            inference_lookup = inference.set_index(["shop", "spu"])
            for row in slot_predictions.itertuples(index=False):
                expected_window = "d01_30" if kind == "M1" else "d31_60"
                if row.window != expected_window:
                    raise AssertionError("DIRECT slot reports a different segment")
                value = inference_lookup.loc[(row.shop, row.spu), "prediction"]
                maximum = max(maximum, equal_number(row.prediction, value, "DIRECT window=checkpoint segment", 1e-8))
            if predictions is not None:
                segment = "d01_30" if kind == "M1" else "d31_60"
                target_rows = predictions[predictions.issue_date.eq(issue) & predictions.model.eq("DIRECT30x2") & predictions.window.eq(segment)]
                for row in target_rows.itertuples(index=False):
                    maximum = max(maximum, equal_number(row.prediction, inference_lookup.loc[(row.shop, row.spu), "prediction"], "DIRECT final window=checkpoint", 1e-8))
            checked_rows = len(inference)
            trees = booster.num_trees()
        else:
            for name, expected_hash in result["checkpoint_hashes"].items():
                if sha(slot / "model" / name) != expected_hash or checkpoint["checkpoint_hashes"][name] != expected_hash:
                    raise AssertionError("recursive checkpoint changed")
            model = MLForecast.load(slot / "model")
            booster = model.models_[kind].booster_
            _check_booster(booster, frozen["lightgbm"])
            saved = pd.read_parquet(slot / "daily_forecasts.parquet")
            objects = set(cohorts.loc[cohorts.issue_date.eq(issue), ["shop", "spu"]].itertuples(index=False, name=None))
            saved = saved[[tuple(pair) in objects for pair in saved[["shop", "spu"]].itertuples(index=False, name=None)]]
            objects_by_id = {row.unique_id: (row.shop, row.spu) for row in saved[["unique_id", "shop", "spu"]].drop_duplicates().itertuples(index=False)}
            ids = sorted(objects_by_id)
            if not ids:
                reports.append({"issue_date": issue, "kind": kind, "status": "NO_CORE_PREDICTION_IN_CHECKPOINT", "actual_trees": booster.num_trees()})
                continue
            states = np.array([oracle.history(*objects_by_id[uid], issue) for uid in ids], dtype=np.float32)
            expected_exog, future = {}, []
            dates = pd.date_range(cutoff + dt.timedelta(days=1), date(issue) + dt.timedelta(days=60))
            for uid in ids:
                shop, spu = objects_by_id[uid]
                if exog[kind]:
                    recent = source_by_pair[(shop, spu)].reindex(pd.date_range(cutoff - dt.timedelta(days=27), cutoff))
                    sessions = recent.listing_sessions_sum.where(recent.null_sessions.eq(0))
                    values = {"session_mean28": float(sessions.mean()) if sessions.notna().all() else np.nan,
                              "order_session_ratio28": float(recent.order_qty.sum() / sessions.sum()) if sessions.notna().all() and sessions.sum() > 0 else np.nan}
                    for field in ["ad_spend", "ad_clicks", "ad_orders"]:
                        values[field + "_mean28"] = float(recent[field].mean()) if field in recent and recent[field].notna().all() else np.nan
                    expected_exog[uid] = values
                    future.extend({"unique_id": uid, "ds": time, **{field: values[field] for field in exog[kind]}} for time in dates)
            state_holder = {"history": states, "step": 0, "max_feature_difference": 0.}

            def before(features):
                step = state_holder["step"]
                history = state_holder["history"]
                time = dates[step]
                expected_fields = {"shop_code": np.array([SHOPS.index(objects_by_id[uid][0]) for uid in ids]),
                                   **{f"lag{lag}": history[:, -lag] for lag in [1, 7, 14, 28]},
                                   "dayofweek": np.full(len(ids), time.dayofweek), "month": np.full(len(ids), time.month), "dayofyear": np.full(len(ids), time.dayofyear)}
                for width in [7, 28]:
                    accumulator = np.zeros(len(ids), dtype=np.float32)
                    for column in range(28 - width, 28):
                        accumulator += history[:, column]
                    expected_fields[f"rolling_mean_lag1_window_size{width}"] = accumulator / np.float32(width)
                for field in exog[kind]:
                    expected_fields[field] = np.array([expected_exog[uid][field] for uid in ids])
                if len(features) != len(ids):
                    raise AssertionError("recursive inference object order/size changed")
                for field, values in expected_fields.items():
                    actual = features[field].to_numpy(dtype=float)
                    if not np.allclose(actual, values, rtol=0., atol=1e-8, equal_nan=True):
                        raise AssertionError(f"recursive native feature mismatch: {issue}/{kind}/{step}/{field}")
                    finite_mask = np.isfinite(values)
                    if finite_mask.any():
                        state_holder["max_feature_difference"] = max(state_holder["max_feature_difference"], float(np.max(np.abs(actual[finite_mask] - values[finite_mask]))))
                return features

            def after(values):
                clipped = np.maximum(values, 0.)
                state_holder["history"] = np.column_stack([state_holder["history"][:, 1:], clipped.astype(np.float32)])
                state_holder["step"] += 1
                return clipped

            actual = model.predict(62, ids=ids, X_df=pd.DataFrame(future) if future else None, before_predict_callback=before, after_predict_callback=after)
            if state_holder["step"] != 62:
                raise AssertionError("recursive model skipped bridge/forecast steps")
            paired = saved.merge(actual, on=["unique_id", "ds"], suffixes=("_saved", "_replayed"), validate="one_to_one")
            if len(paired) != len(saved):
                raise AssertionError("recursive checkpoint replay lost daily records")
            difference = float(np.max(np.abs(paired[kind + "_saved"] - paired[kind + "_replayed"])))
            if difference > 1e-8:
                raise AssertionError("recursive checkpoint forecast differs from saved daily forecast")
            maximum = max(maximum, difference, state_holder["max_feature_difference"])
            # Confirm reported windows are sums of the saved/replayed 62-day paths.
            paths = {(s, p): group.set_index("ds")[kind] for (s, p), group in saved.groupby(["shop", "spu"])}
            target_rows = slot_predictions if predictions is None else predictions[predictions.issue_date.eq(issue) & predictions.model.eq(kind)]
            for row in target_rows.itertuples(index=False):
                path = paths.get((row.shop, row.spu))
                start, end = target_window(issue, row.window)
                value = float(path.reindex(pd.date_range(start, end)).sum()) if path is not None else None
                maximum = max(maximum, equal_number(row.prediction, value, "recursive window=sum daily path", 1e-8))
            checked_rows = len(saved)
            trees = booster.num_trees()
        reports.append({"issue_date": issue, "kind": kind, "status": "PASS_SAVED_MODEL_NO_FIT",
                        "checked_rows": checked_rows, "actual_trees": trees, "max_difference": maximum})
        print(f"saved model only {issue}/{kind}: {checked_rows} rows, max_difference={maximum}", flush=True)
    return reports


def slot_evidence_key(root, result):
    """A cache never survives changed source, validator, training, or model bytes."""
    if SHOPS != ["JQ-US", "MT-US", "RKZ-US", "SY-US"] or WINDOWS != {"d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)} or SEGMENTS != {"M1": (1, 30), "M2": (31, 60)} or VALID_STATES != {"OBSERVED", "EXPLICIT_ZERO"}:
        raise AssertionError("model verifier constants changed; prior receipts are not reusable")
    slot = root / "effects/slots" / f"{result['issue_date']}_{result['kind']}"
    paths = [root / "prepared/prepare_manifest.json", root / "prepared/frozen_config.json",
             slot / "result.json", slot / "training_audit.json", slot / "checkpoint.json", slot / "predictions.parquet"]
    paths += [slot / "model.txt", slot / "segment_predictions.parquet"] if result["kind"] in SEGMENTS else [slot / "model/models.pkl", slot / "model/ts.pkl", slot / "daily_forecasts.parquet"]
    evidence = {str(path.resolve()): sha(path) for path in paths}
    # Hash the complete model-verification implementation, not unrelated score
    # readers or CLI orchestration, so a reporting fix cannot force a model rerun.
    names = ["date", "finite", "target_window", "direct_dates", "direct_features", "sha", "equal_number", "CalendarOracle",
             "monthly_positive_ends", "reconstruct_direct_training", "reconstruct_recursive_training", "compare_training_audit",
             "verify_training", "_check_booster", "verify_saved_checkpoints"]
    evidence["validator_model_logic_sha256"] = hashlib.sha256("\n".join(inspect.getsource(globals()[name]) for name in names).encode()).hexdigest()
    return hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest(), evidence


def verify_completed_slots(root, source, monthly, oracle, cohorts, predictions=None):
    """Incremental zero-fit validation, immutable content-addressed PASS receipts."""
    verified_manifest(root / "prepared", "prepare_manifest.json")
    runtime = json.loads((root / "prepared/frozen_config.json").read_text(encoding="utf-8-sig"))["runtime"]
    if {name: importlib.metadata.version(name) for name in runtime} != runtime:
        raise AssertionError("frozen model runtime changed; prior cache receipts cannot establish replay equivalence")
    completed = []
    for result_path in sorted((root / "effects/slots").glob("*/result.json")):
        result = json.loads(result_path.read_text(encoding="utf-8-sig"))
        if result.get("status") in {"FITTED", "RECOVERED"}:
            completed.append(result)
    cache = root / "validation_cache"
    cache.mkdir(exist_ok=True)
    new_results, receipts = [], []
    for result in completed:
        key, evidence = slot_evidence_key(root, result)
        receipt_path = cache / f"{result['issue_date']}_{result['kind']}_{key}.json"
        # The first 35 receipts predated separated model/score fingerprints.
        # These two exact hashes attest to the unchanged model implementation
        # in that source version; every non-code input must still match.
        legacy = None
        for path in cache.glob(f"{result['issue_date']}_{result['kind']}_*.json"):
            candidate = json.loads(path.read_text(encoding="utf-8-sig"))
            old_evidence = dict(candidate.get("evidence", {}))
            old_code = old_evidence.pop(str(Path(__file__).resolve()), None)
            if old_code == "08cd722909698febadd6a8659ead98710d9a863cb0487174c48ab84a3a3a38f2" and evidence["validator_model_logic_sha256"] == "b90c34365090f63c15d3d4cd8aa061a6a3bff332cd4781ec7bc3b438ed6a4388":
                old_evidence["validator_model_logic_sha256"] = evidence["validator_model_logic_sha256"]
                if old_evidence == evidence and candidate.get("status") == "PASS_INDEPENDENT_TRAINING_AND_SAVED_MODEL":
                    legacy = candidate
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
            if receipt.get("status") != "PASS_INDEPENDENT_TRAINING_AND_SAVED_MODEL" or receipt.get("evidence") != evidence:
                raise AssertionError("invalid incremental validation cache receipt")
            receipts.append(receipt)
        elif legacy is not None:
            receipts.append(legacy)
        else:
            new_results.append((result, receipt_path, evidence))
    for issue in sorted({item[0]["issue_date"] for item in new_results}):
        batch = [item for item in new_results if item[0]["issue_date"] == issue]
        selected = {(result["issue_date"], result["kind"]) for result, _, _ in batch}
        training = {(row["issue_date"], row["kind"]): row for row in verify_training(root, source, monthly, oracle, selected)}
        checkpoints = {(row["issue_date"], row["kind"]): row for row in verify_saved_checkpoints(root, source, monthly, oracle, cohorts, fitted=[item[0] for item in batch])}
        for result, receipt_path, evidence in batch:
            checkpoint = checkpoints[(result["issue_date"], result["kind"])]
            if checkpoint["status"] not in {"PASS_SAVED_MODEL_NO_FIT", "NO_CORE_PREDICTION_IN_CHECKPOINT"}:
                raise AssertionError("saved model did not receive an independent check")
            key = (result["issue_date"], result["kind"])
            receipt = {"status": "PASS_INDEPENDENT_TRAINING_AND_SAVED_MODEL", "issue_date": key[0], "kind": key[1],
                       "evidence": evidence, "training": training[key], "checkpoint": checkpoint,
                       "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "new_fits": 0}
            with receipt_path.open("x", encoding="utf-8") as stream:
                json.dump(receipt, stream, ensure_ascii=False, indent=2, allow_nan=False)
            receipts.append(receipt)
    if predictions is not None:
        # Cached checks established checkpoint -> slot output. Reconcile slot ->
        # final panel now, without replaying or fitting any saved model again.
        for result in completed:
            slot = root / "effects/slots" / f"{result['issue_date']}_{result['kind']}"
            slot_values = pd.read_parquet(slot / "predictions.parquet").set_index(["shop", "spu", "window"])
            model = "DIRECT30x2" if result["kind"] in SEGMENTS else result["kind"]
            final = predictions[predictions.issue_date.eq(result["issue_date"]) & predictions.model.eq(model)]
            if result["kind"] in SEGMENTS:
                final = final[final.window.eq("d01_30" if result["kind"] == "M1" else "d31_60")]
            for row in final.itertuples(index=False):
                key = (row.shop, row.spu, row.window)
                value = slot_values.loc[key, "prediction"] if key in slot_values.index else None
                equal_number(row.prediction, value, "final prediction preserves validated checkpoint output", 1e-8)
                if key in slot_values.index and row.prediction_status != slot_values.loc[key, "prediction_status"]:
                    raise AssertionError("final panel lost the checkpoint's missing-output explanation")
    return {"completed_slots": len(completed), "newly_verified_slots": len(new_results), "reused_slots": len(completed) - len(new_results),
            "training_rows": sum(receipt["training"]["rows"] for receipt in receipts),
            "max_checkpoint_difference": max((receipt["checkpoint"].get("max_difference", 0.) for receipt in receipts), default=0.),
            "receipts": [{"issue_date": receipt["issue_date"], "kind": receipt["kind"], "status": receipt["status"],
                          "training_rows": receipt["training"]["rows"], "audit_sha256": receipt["training"]["audit_sha256"]} for receipt in receipts]}


def verify_prepared(root, source, monthly, archive):
    prepared = root / "prepared"
    manifest = verified_manifest(prepared, "prepare_manifest.json")
    grid = pd.read_csv(prepared / "issue_grid.csv")
    expected = issue_grid().query("window == 'd01_60'").drop(columns=["window", "window_days"])
    pd.testing.assert_frame_equal(grid[expected.columns].reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
    quality = pd.read_csv(prepared / "selection_quality.csv")
    if len(quality) != 88 or quality.duplicated(["issue_date", "shop"]).any():
        raise AssertionError("selection quality must retain all 22 x 4 store-months")
    qindex = quality.set_index(["issue_date", "shop"])
    universe = pd.read_parquet(prepared / "universe.parquet")
    core = pd.read_parquet(prepared / "core.parquet")
    expected_keys, expected_universe_keys = set(), set()
    months = monthly.copy()
    months["period"] = pd.PeriodIndex(months.month, freq="M")
    positive_periods = {key: sorted(group.period.tolist()) for key, group in months[months.qty.gt(0)].groupby(["shop", "spu"])}
    positive_ordinals = {key: np.array([period.ordinal for period in periods], dtype=np.int64) for key, periods in positive_periods.items()}
    for issue in grid.issue_date:
        cutoff = pd.Timestamp(issue) - pd.Timedelta(days=2)
        selection_source = source[source.ds.between(cutoff - pd.Timedelta(days=89), cutoff)]
        for shop in SHOPS:
            rank, oracle = selection_snapshot(selection_source, shop, issue)
            observed = qindex.loc[(issue, shop)]
            aliases = {"MAPPED_BELOW_TARGET": "MAPPING_COVERAGE_GAP", "NO_POSITIVE_SALES": "NO_POSITIVE_SELECTION_SALES"}
            if observed.selection_status != aliases.get(oracle["selection_status"], oracle["selection_status"]):
                raise AssertionError(f"selection status differs: {issue}/{shop}")
            for field in ["core_n", "expected_days", "present_days", "invalid_days", "full_shop_units", "mapped_units", "unmapped_units", "selected_units", "selection_coverage"]:
                equal_number(observed[field], oracle[field], "selection " + field, 1e-8)
            for field in ["selection_start", "selection_end"]:
                if date(observed[field]) != date(oracle[field]):
                    raise AssertionError("selection calendar shifted")
            actual = universe[universe.issue_date.eq(issue) & universe.shop.eq(shop)].set_index("spu")
            for row in rank.itertuples():
                expected_universe_keys.add((issue, shop, row.spu))
                candidate = actual.loc[row.spu]
                equal_number(candidate.selection_qty, row.selection_qty, "selection quantity", 0.)
                if bool(candidate.is_core) != bool(row.is_core):
                    raise AssertionError("core threshold boundary ties changed")
                if row.is_core:
                    expected_keys.add((issue, shop, row.spu))
                ordinals = positive_ordinals.get((shop, row.spu), np.array([], dtype=np.int64))
                count = int(np.searchsorted(ordinals, cutoff.to_period("M").ordinal, side="right"))
                lifecycle = "ESTABLISHED" if count >= 4 else "INSUFFICIENT_POSITIVE_HISTORY" if count else "UNKNOWN"
                if candidate.lifecycle != lifecycle:
                    raise AssertionError("actual-issue qualification differs")
                if count:
                    age = cutoff.to_period("M").ordinal - int(ordinals[0]) + 1
                    maturity = "0_3" if age <= 3 else "4_5" if age <= 5 else "6_11" if age <= 11 else "12_plus"
                    if candidate.maturity != maturity:
                        raise AssertionError("original observed age bins changed")
    keys = ["issue_date", "shop", "spu"]
    if len(universe) != len(expected_universe_keys) or set(universe[keys].itertuples(index=False, name=None)) != expected_universe_keys:
        raise AssertionError("monthly universe omits or adds candidates")
    if len(core) != len(expected_keys) or set(core[keys].itertuples(index=False, name=None)) != expected_keys:
        raise AssertionError("monthly core differs from independent cutoff-only selection")
    old = pd.read_csv(archive / "effects_v1/core_lists.csv")
    panel = pd.read_csv(prepared / "original_core_panel.csv")
    if len(old) != 385 or len(panel) != 385:
        raise AssertionError("original historical panel changed")
    pd.testing.assert_frame_equal(old.sort_values(keys).reset_index(drop=True), panel[old.columns].sort_values(keys).reset_index(drop=True), check_dtype=False)
    return pd.concat([core, panel], ignore_index=True), {"monthly_core_rows": len(core), "monthly_universe_rows": len(universe),
        "original_panel_rows": len(panel), "store_months": len(quality), "prepare_manifest_sha256": sha(prepared / "prepare_manifest.json")}


def verify_prior_archives(archive):
    manifests = [(archive / "effects_v1", "run_manifest.json"),
                 (archive.parent / "bc60_rolling_v2_20261019/b_rolling", "run_manifest.json"),
                 (archive.parent / "bc60_rolling_v2_20261019/a_diagnostic_final", "diagnostic_manifest.json"),
                 (archive.parent / "bc60_direct_v3_20261019/prepared", "prepare_manifest.json"),
                 (archive.parent / "bc60_direct_v3_20261019/effects", "run_manifest.json")]
    checked = {}
    for folder, filename in manifests:
        manifest = verified_manifest(folder, filename)
        checked[str(folder)] = {"manifest_sha256": sha(folder / filename), "file_count": len(manifest.get("files", {}))}
    original = json.loads((archive / "effects_v1/run_manifest.json").read_text(encoding="utf-8-sig"))
    for name, digest in original["inputs"].items():
        if sha(archive / "inputs_v1_final" / name) != digest:
            raise AssertionError("original V1 input archive was changed")
    if sha(archive / "inputs_v1_final/daily.parquet") != "ff69063d3c4e506b2b2b8c701db6afa95b4f8af7ce7af3b7c17a898e70bfc7da" or sha(archive / "effects_v1/core_lists.csv") != "f634f9e1ee857be71db2e4f31e7136a4d7f6ff08761fb227edea972cc71f32cf":
        raise AssertionError("anchored original input/core hash changed")
    return checked


def nonoverlap_count(label_starts):
    last_end, count = None, 0
    for start in sorted(set(pd.to_datetime(label_starts))):
        if last_end is None or start > last_end:
            count += 1
            last_end = start + pd.Timedelta(days=29)
    return count


def verify_training_metadata(root, source, monthly, oracle, partial=False):
    """Separate full metadata/funnel audit; no estimator operations occur here."""
    fits = json.loads((root / "effects/fits.json").read_text(encoding="utf-8-sig"))
    expected_grid = {(issue, kind) for issue in issue_grid().issue_date.unique() for kind in ["B1", "B2", "B3", "M1", "M2"]}
    actual_grid = {(r["issue_date"], r["kind"]) for r in fits}
    if len(fits) != len(actual_grid) or not actual_grid <= expected_grid or (not partial and actual_grid != expected_grid):
        raise AssertionError("fit status does not preserve 22 months x five frozen slots")
    ends = monthly_positive_ends(monthly)
    direct_rows, objects, exclusions = 0, 0, 0
    for result in fits:
        issue, kind = result["issue_date"], result["kind"]
        cutoff = date(issue) - dt.timedelta(days=2)
        if date(issue) > dt.date(2026, 8, 2):
            if result["status"] != "NOT_MATURED" or result["fit_attempted"]:
                raise AssertionError("immature slot was treated as a trainable final effect")
            continue
        if result["status"] not in {"FITTED", "RECOVERED"}:
            continue
        if date(result["input_cutoff"]) != cutoff:
            raise AssertionError("model training cutoff shifted")
        if kind.startswith("B"):
            if date(result["effective_label_last"]) > cutoff or date(result["latest_history_date"]) > cutoff:
                raise AssertionError("recursive training includes an unavailable label")
            continue
        expected = reconstruct_direct_training(oracle, ends, issue, kind)
        slot = root / "effects/slots" / f"{issue}_{kind}"
        per_object = pd.read_csv(slot / "per_object_windows.csv")
        groups = {key: group for key, group in expected.groupby(["shop", "spu"])}
        if len(per_object) != len(groups) or per_object.duplicated(["shop", "spu"]).any():
            raise AssertionError("DIRECT object support table changes the training population")
        for row in per_object.itertuples(index=False):
            group = groups[(row.shop, row.spu)]
            for field, value in {"train_rows": len(group), "simulated_dates": group.simulated_issue_date.nunique(), "nonoverlap_windows": nonoverlap_count(group.label_start)}.items():
                equal_number(getattr(row, field), value, "DIRECT object support " + field, 0.)
            if date(row.first_simulated_issue) != date(group.simulated_issue_date.min()) or date(row.last_simulated_issue) != date(group.simulated_issue_date.max()):
                raise AssertionError("DIRECT per-object support dates changed")
        wanted = {"train_rows": len(expected), "train_objects": len(groups), "train_dates": expected.simulated_issue_date.nunique(),
                  "positive_labels": int(expected.y_label.gt(0).sum()), "zero_labels": int(expected.y_label.eq(0).sum()),
                  "max_global_nonoverlap": nonoverlap_count(expected.label_start)}
        for field, value in wanted.items():
            equal_number(result[field], value, "DIRECT aggregate training support " + field, 0.)
        if date(result["first_label"]) != date(expected.label_start.min()) or date(result["last_label"]) != date(expected.label_end.max()) or date(expected.label_end.max()) > cutoff:
            raise AssertionError("DIRECT maturity or first/last label metadata differs")
        candidates = pd.date_range(oracle.first + dt.timedelta(days=29), cutoff)
        positions = np.asarray((candidates - pd.Timestamp(oracle.first)).days) - 2
        cutoff_ordinals = positions + oracle.first.toordinal()
        first, last = SEGMENTS[kind]
        label_left, label_right = positions + 2 + first, positions + 3 + last
        mature = np.asarray(candidates + pd.Timedelta(days=last) <= pd.Timestamp(cutoff))
        reason_counts = {reason: np.zeros(len(candidates), dtype=np.int64) for reason in ["INSUFFICIENT_QUALIFICATION", "LABEL_NOT_MATURE", "MISSING_HISTORY", "MISSING_LABEL_DAYS"]}
        historical = source[source.ds.le(pd.Timestamp(cutoff)) & source.mapped.eq(True) & source.shop.isin(SHOPS)]
        for pair in set(historical[["shop", "spu"]].itertuples(index=False, name=None)):
            known_counts = oracle.data[pair][3]
            eligible = np.searchsorted(ends.get(pair, []), cutoff_ordinals, side="right") >= 4
            history = known_counts[positions + 1] - known_counts[positions - 27] == 28
            labels = known_counts[np.clip(label_right, 0, oracle.length)] - known_counts[np.clip(label_left, 0, oracle.length)] == 30
            reason_counts["INSUFFICIENT_QUALIFICATION"] += ~eligible
            reason_counts["LABEL_NOT_MATURE"] += eligible & ~mature
            reason_counts["MISSING_HISTORY"] += eligible & mature & ~history
            reason_counts["MISSING_LABEL_DAYS"] += eligible & mature & history & ~labels
        expected_exclusions = {(str(candidates[i].date()), reason): int(value) for reason, values in reason_counts.items() for i, value in enumerate(values) if value}
        observed = pd.read_csv(slot / "training_exclusions.csv")
        got = {(row.simulated_issue_date, row.reason): int(row.rows) for row in observed.itertuples(index=False)}
        if len(observed) != len(got) or got != expected_exclusions:
            raise AssertionError("DIRECT ordered qualification/maturity/history/label exclusion funnel differs")
        direct_rows += len(expected)
        objects += len(per_object)
        exclusions += len(observed)
    return {"partial": partial, "slot_records": len(fits), "direct_training_rows": direct_rows, "direct_object_support_rows": objects, "exclusion_rows": exclusions}


def verify_inference_coverage(root, monthly, oracle, cohorts, predictions):
    """Independently establish who must receive an output, not just row retention."""
    ends = monthly_positive_ends(monthly)
    fits = json.loads((root / "effects/fits.json").read_text(encoding="utf-8-sig"))
    fit_lookup = {(row["issue_date"], row["kind"]): row for row in fits}
    checked = 0
    for row in predictions.itertuples(index=False):
        if row.model in {"F28", "WEEKDAY4"}:
            continue
        cutoff = date(row.issue_date) - dt.timedelta(days=2)
        count = np.searchsorted(ends.get((row.shop, row.spu), []), cutoff.toordinal(), side="right")
        kinds = ["M1"] if row.model == "DIRECT30x2" and row.window == "d01_30" else ["M2"] if row.model == "DIRECT30x2" and row.window == "d31_60" else ["M1", "M2"] if row.model == "DIRECT30x2" else [row.model]
        reasons = set()
        for kind in kinds:
            fit = fit_lookup[(row.issue_date, kind)]
            if fit["status"] not in {"FITTED", "RECOVERED"}:
                reasons.add(fit["status"])
            elif count < 4:
                reasons.add("INSUFFICIENT_QUALIFICATION")
            elif oracle.history(row.shop, row.spu, row.issue_date, 28 if kind in SEGMENTS else 56) is None:
                reasons.add("MISSING_HISTORY")
        expected = "|".join(sorted(reasons)) if reasons else "OK"
        if row.prediction_status != expected or finite(row.prediction) != (expected == "OK"):
            raise AssertionError(f"qualified complete-history object omitted or unexplained output: {row.issue_date}/{row.shop}/{row.spu}/{row.model}/{row.window}")
        checked += 1
    old = predictions[predictions.cohort_id.eq("original_fixed_385") & predictions.window.eq("d01_60")]
    counts = {name: {"core_n": len(group), "output_n": int(group.prediction.notna().sum())} for name, group in old.groupby("model")}
    return {"checked_learned_prediction_rows": checked, "original_fixed_panel_output_counts": counts}


def verify_predictions(root, oracle, cohorts):
    effects = root / "effects"
    manifest = verified_manifest(effects, "run_manifest.json")
    predictions = pd.read_parquet(effects / "window_predictions.parquet")
    keys = ["cohort_id", "issue_date", "shop", "spu"]
    models = ["F28", "WEEKDAY4", "B1", "B2", "B3", "DIRECT30x2"]
    expected_keys = {(*key, model, window) for key in cohorts[keys].itertuples(index=False, name=None) for model in models for window in WINDOWS}
    actual_keys = set(predictions[keys + ["model", "window"]].itertuples(index=False, name=None))
    if actual_keys != expected_keys or len(predictions) != len(actual_keys):
        raise AssertionError("prediction grid does not retain every frozen object/model/window")
    maximum = 0.
    baseline_cache = {}
    indexed = predictions.set_index(keys + ["model", "window"])
    for row in predictions.itertuples(index=False):
        start, end = target_window(row.issue_date, row.window)
        if date(row.target_start) != start or date(row.target_end) != end or date(row.input_cutoff) != date(row.issue_date) - dt.timedelta(days=2):
            raise AssertionError("prediction metadata has shifted issue/bridge/target dates")
        if row.issue_month != row.issue_date[:7]:
            raise AssertionError("issue month was replaced by a target month")
        if finite(row.prediction) and row.prediction < 0:
            raise AssertionError("negative postprocessed forecast")
        if pd.notna(row.prediction) and not finite(row.prediction):
            raise AssertionError("nonfinite prediction must not masquerade as output")
        if row.model in {"F28", "WEEKDAY4"}:
            key = (row.shop, row.spu, row.issue_date)
            if key not in baseline_cache:
                baseline_cache[key] = oracle.baselines(*key)
            mean, weekday = baseline_cache[key]
            expected = None if mean is None else (end - start).days * mean + mean if row.model == "F28" else sum(weekday[day.weekday()] for day in pd.date_range(start, end))
            maximum = max(maximum, equal_number(row.prediction, expected, "baseline from exact original history", 1e-8))
            if row.prediction_status != ("OK" if expected is not None else "MISSING_HISTORY"):
                raise AssertionError("baseline missing-history reason differs")
    for key in cohorts[keys].itertuples(index=False, name=None):
        for model in models:
            first, second, total = [indexed.loc[(*key, model, window), "prediction"] for window in ["d01_30", "d31_60", "d01_60"]]
            expected = float(first) + float(second) if finite(first) and finite(second) else None
            maximum = max(maximum, equal_number(total, expected, "segment sum and missingness", 1e-7))
            if model == "DIRECT30x2":
                reasons = {indexed.loc[(*key, model, window), "prediction_status"] for window, value in [("d01_30", first), ("d31_60", second)] if not finite(value)}
                if indexed.loc[(*key, model, "d01_60"), "prediction_status"] != ("OK" if expected is not None else "|".join(sorted(reasons))):
                    raise AssertionError("DIRECT total discarded a missing segment reason")
    ledger = json.loads((root / "formal_fit_ledger.json").read_text(encoding="utf-8-sig"))
    attempts = ledger.get("attempts", [])
    attempted = [item for item in attempts if item.get("fit_attempted")]
    if len(attempted) > 100 or len({(item["issue_date"], item["kind"]) for item in attempted}) != len(attempted):
        raise AssertionError("formal fit budget/unique slots violated")
    if any(date(item["issue_date"]) > dt.date(2026, 8, 2) for item in attempted):
        raise AssertionError("immature issue was fitted outside the 20-month budget")
    return predictions, {"prediction_rows": len(predictions), "max_baseline_or_sum_difference": maximum,
        "formal_fit_count": len(attempted), "effect_manifest_sha256": sha(effects / "run_manifest.json")}


def verify_scores(score_root, predictions, oracle, cohorts, source_manifest=True):
    if source_manifest:
        verified_manifest(score_root, "score_manifest.json")
    else:
        # Explicitly limited to a build-time arithmetic/schema probe. The
        # training process may have advanced the referenced predictions after
        # this partial snapshot; this route cannot establish source identity.
        manifest = json.loads((score_root / "score_manifest.json").read_text(encoding="utf-8-sig"))
        for name, digest in manifest["files"].items():
            if sha(score_root / name) != digest:
                raise AssertionError("partial score snapshot file changed")
    scores = pd.read_parquet(score_root / "scores.parquet")
    keys = ["cohort_id", "issue_date", "shop", "spu", "model", "window"]
    if len(scores) != len(predictions) or scores.duplicated(keys).any() or set(scores[keys].itertuples(index=False, name=None)) != set(predictions[keys].itertuples(index=False, name=None)):
        raise AssertionError("scoring dropped frozen predictions")
    index = predictions.set_index(keys)
    metadata = cohorts.set_index(["cohort_id", "issue_date", "shop", "spu"])
    truth_cache = {}
    maximum_truth = 0.
    for row in scores.itertuples(index=False):
        truth_key = (row.issue_date, row.shop, row.spu, row.window)
        if truth_key not in truth_cache:
            start, end = target_window(row.issue_date, row.window)
            truth_cache[truth_key] = oracle.total(row.shop, row.spu, start, end)
        actual, state = truth_cache[truth_key]
        if state == "NOT_MATURED" and row.truth_status != state:
            raise AssertionError("immature truth was shortened or scored")
        if state == "OBSERVED" and row.truth_status != state:
            raise AssertionError("complete original truth was lost")
        if state == "UNKNOWN_TRUTH" and row.truth_status in {"OBSERVED", "NOT_MATURED"}:
            raise AssertionError("missing truth was imputed or misclassified as immature")
        maximum_truth = max(maximum_truth, equal_number(row.actual, actual, "score true calendar sum", 0.))
        expected_prediction = index.loc[tuple(getattr(row, key) for key in keys), "prediction"]
        equal_number(row.prediction, expected_prediction, "score preserved forecast", 0.)
        expected_score = score_record(actual, expected_prediction)
        if row.decision != expected_score["decision"]:
            raise AssertionError("zero/missing/boundary judgment differs")
        for field in ["absolute_error", "relative_error"]:
            equal_number(getattr(row, field), expected_score[field], field, 1e-8)
        original_meta = metadata.loc[(row.cohort_id, row.issue_date, row.shop, row.spu)]
        for field in ["lifecycle", "maturity", "category"]:
            if getattr(row, field) != original_meta[field]:
                raise AssertionError("frozen cohort metadata replaced by other cohort or future state")
        equal_number(row.selection_qty, original_meta.selection_qty, "frozen selection quantity", 0.)
    score_metrics = pd.read_csv(score_root / "metrics.csv")
    names = {"core_n": "fixed_core_n", "hit_fixed_share": "full_core_hit_share", "known_core_actual_units": "known_fixed_core_actual_units",
             "zero_absolute_error": "zero_actual_absolute_error"}
    slices = {"overall_overlapping_issue_records": [], "overall_mature_overlapping_issue_records": [], "issue": ["issue_month"], "shop_issue": ["issue_month", "shop"],
              "maturity_issue": ["issue_month", "maturity"], "qualification_issue": ["issue_month", "lifecycle"], "category_issue": ["issue_month", "category"]}
    maximum_metric = 0.
    expected_metric_keys = set()
    for label, extra in slices.items():
        population = scores[pd.to_datetime(scores.target_end).le(pd.Timestamp(oracle.truth_end))] if label == "overall_mature_overlapping_issue_records" else scores
        for group_key, group in population.groupby(["cohort_id", "model", "window"] + extra, dropna=False):
            expected_metric_keys.add((label, *group_key))
    seen = set()
    for row in score_metrics.itertuples(index=False):
        extra = slices[row.slice]
        metric_key = (row.slice, row.cohort_id, row.model, row.window, *(getattr(row, key) for key in extra))
        if metric_key in seen:
            raise AssertionError("duplicate metric group")
        seen.add(metric_key)
        group = scores[scores.cohort_id.eq(row.cohort_id) & scores.model.eq(row.model) & scores.window.eq(row.window)]
        if row.slice == "overall_mature_overlapping_issue_records":
            group = group[pd.to_datetime(group.target_end).le(pd.Timestamp(oracle.truth_end))]
        for key in extra:
            group = group[group[key].eq(getattr(row, key))]
        expected = metrics(group)
        for field in score_metrics.columns:
            expected_field = names.get(field, field)
            if expected_field in expected:
                maximum_metric = max(maximum_metric, equal_number(getattr(row, field), expected[expected_field], "metric " + field, 1e-6))
    if seen != expected_metric_keys:
        raise AssertionError("metric grouping coverage changed")
    return scores, {"source_identity_checked": source_manifest, "score_rows": len(scores), "metric_rows": len(score_metrics), "max_truth_difference": maximum_truth,
                    "max_metric_difference": maximum_metric, "score_manifest_sha256": sha(score_root / "score_manifest.json")}


METRIC_NAMES = {"core_n": "fixed_core_n", "hit_fixed_share": "full_core_hit_share", "known_core_actual_units": "known_fixed_core_actual_units",
                "zero_absolute_error": "zero_actual_absolute_error"}


def check_metric_values(row, expected, fields, prefix=""):
    maximum = 0.
    for field in fields:
        if not field.startswith(prefix):
            continue
        name = field[len(prefix):]
        expected_name = METRIC_NAMES.get(name, name)
        if expected_name in expected:
            maximum = max(maximum, equal_number(getattr(row, field), expected[expected_name], "recomputed metric " + field, 1e-6))
    return maximum


def migration(old, new):
    if old == "UNJUDGED" or new == "UNJUDGED":
        return "UNJUDGED"
    return "RETAINED_HIT" if old == new == "HIT" else "NEW_HIT" if new == "HIT" else "EXITED_HIT" if old == "HIT" else "BOTH_MISS"


def verify_pair_joint_common(score_root, scores):
    object_keys = ["cohort_id", "issue_date", "shop", "spu"]
    score_keys = object_keys + ["model", "window"]
    indexed = scores.set_index(score_keys)
    pairs = pd.read_parquet(score_root / "paired_details.parquet")
    expected = scores[scores.model.ne("F28")]
    if len(pairs) != len(expected) or pairs.duplicated(score_keys).any() or set(pairs[score_keys].itertuples(index=False, name=None)) != set(expected[score_keys].itertuples(index=False, name=None)):
        raise AssertionError("candidate-F28 pair grid changed")
    for row in pairs.itertuples(index=False):
        key = (row.cohort_id, row.issue_date, row.shop, row.spu)
        a, b = indexed.loc[(*key, "F28", row.window)], indexed.loc[(*key, row.model, row.window)]
        equal_number(row.baseline_prediction, a.prediction, "pair baseline value", 0.)
        equal_number(row.prediction, b.prediction, "pair candidate value", 0.)
        equal_number(row.actual, a.actual, "pair truth", 0.)
        paired = finite(a.actual) and finite(a.prediction) and finite(b.prediction)
        delta = abs(b.prediction - a.actual) - abs(a.prediction - a.actual) if paired else None
        state = "UNJUDGED" if not paired else "IMPROVED" if delta < 0 else "DEGRADED" if delta > 0 else "UNCHANGED"
        if row.paired != paired or row.pair_state != state or row.baseline_decision != a.decision:
            raise AssertionError("pair state, missingness or comparison scope differs")
        equal_number(row.error_change, delta, "paired absolute-error change", 1e-8)
    ps = pd.read_csv(score_root / "paired_f28.csv")
    slices = {"overall": [], "issue": ["issue_month"], "shop_issue": ["issue_month", "shop"]}
    expected_groups = {(label, *key) for label, extra in slices.items()
                       for key, group in pairs.groupby(["cohort_id", "model", "window"] + extra, dropna=False)}
    seen = set()
    maximum = 0.
    for row in ps.itertuples(index=False):
        extra = slices[row.slice]
        group_key = (row.slice, row.cohort_id, row.model, row.window, *(getattr(row, field) for field in extra))
        if group_key in seen:
            raise AssertionError("duplicate paired metric group")
        seen.add(group_key)
        group = pairs[pairs.cohort_id.eq(row.cohort_id) & pairs.model.eq(row.model) & pairs.window.eq(row.window)]
        for field in extra:
            group = group[group[field].eq(getattr(row, field))]
        matched = group[group.paired]
        baseline = matched.copy()
        baseline["prediction"] = baseline.baseline_prediction
        candidate_metric, baseline_metric = metrics(matched), metrics(baseline)
        maximum = max(maximum, check_metric_values(row, candidate_metric, ps.columns, "candidate_"), check_metric_values(row, baseline_metric, ps.columns, "f28_"))
        equal_number(row.full_core_n, len(group), "paired fixed N", 0.)
        equal_number(row.paired_n, len(matched), "paired N", 0.)
        delta = candidate_metric["wape"] - baseline_metric["wape"] if candidate_metric["wape"] is not None else None
        equal_number(row.wape_change, delta, "fair WAPE difference", 1e-12)
        denominator = float(matched.actual.sum())
        for state in ["IMPROVED", "DEGRADED", "UNCHANGED", "UNJUDGED"]:
            mask = group.pair_state.eq(state)
            equal_number(getattr(row, state.lower() + "_n"), mask.sum(), "pair change count", 0.)
            weight = float(group.loc[mask & group.paired, "actual"].sum() / denominator) if denominator > 0 else None
            equal_number(getattr(row, state.lower() + "_sales_weight"), weight, "pair sales weight", 1e-12)
    if seen != expected_groups:
        raise AssertionError("paired summaries omit groups")
    joint = pd.read_csv(score_root / "joint_hits.csv")
    expected_joint = {}
    joint_counts = {}
    for key, group in scores.groupby(object_keys + ["model"], dropna=False):
        expected_joint[key] = joint_decision(group.decision)
        joint_counts[key] = (int(np.isfinite(group.prediction).sum()), int(np.isfinite(group.actual).sum()))
    if len(joint) != len(expected_joint) or joint.duplicated(object_keys + ["model"]).any():
        raise AssertionError("joint grid incomplete")
    for row in joint.itertuples(index=False):
        key = tuple(getattr(row, name) for name in object_keys + ["model"])
        if row.decision != expected_joint[key]:
            raise AssertionError("joint zero/missing/three-window judgment differs")
        equal_number(row.output_windows, joint_counts[key][0], "joint output window count", 0.)
        equal_number(row.truth_windows, joint_counts[key][1], "joint truth window count", 0.)
    joint_summary = pd.read_csv(score_root / "joint_summary.csv")
    summary_keys = ["cohort_id", "issue_date", "model", "decision"]
    expected_summary = joint.groupby(summary_keys).size().reset_index(name="records").sort_values(summary_keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(joint_summary[expected_summary.columns].sort_values(summary_keys).reset_index(drop=True), expected_summary, check_dtype=False)
    joint_migrations = pd.read_csv(score_root / "joint_migrations.csv")
    if len(joint_migrations) != len(joint[joint.model.ne("F28")]) or joint_migrations.duplicated(object_keys + ["model"]).any():
        raise AssertionError("joint migration grid incomplete")
    for row in joint_migrations.itertuples(index=False):
        key = tuple(getattr(row, name) for name in object_keys)
        a, b = expected_joint[(*key, "F28")], expected_joint[(*key, row.model)]
        if row.migration != migration(a, b) or row.baseline_decision != a or row.decision != b:
            raise AssertionError("joint migration or unknown classification differs")
    all_models = set(scores.model.unique())
    complete_keys = set()
    for key, group in scores.groupby(object_keys + ["window"], dropna=False):
        if set(group.model) == all_models and np.isfinite(group.prediction).all() and np.isfinite(group.actual).all():
            complete_keys.add(key)
    common = scores[[tuple(row) in complete_keys for row in scores[object_keys + ["window"]].itertuples(index=False, name=None)]]
    common_table = pd.read_csv(score_root / "all_model_common.csv")
    common_groups = {key: group for key, group in common.groupby(["cohort_id", "issue_month", "model", "window"])}
    if len(common_table) != len(common_groups):
        raise AssertionError("all-model common set size differs")
    for row in common_table.itertuples(index=False):
        key = (row.cohort_id, row.issue_month, row.model, row.window)
        check_metric_values(row, metrics(common_groups[key]), common_table.columns)
    return {"paired_rows": len(pairs), "paired_summary_rows": len(ps), "joint_rows": len(joint),
            "joint_migration_rows": len(joint_migrations), "all_model_common_rows": len(common), "max_metric_difference": maximum}


def verify_shapes_and_grid(root, score_root, scores, oracle, cohorts, source):
    keys = ["cohort_id", "issue_date", "shop", "spu"]
    shapes = pd.read_csv(score_root / "f28_shape_details.csv")
    if len(shapes) != len(cohorts) or shapes.duplicated(keys).any() or set(shapes[keys].itertuples(index=False, name=None)) != set(cohorts[keys].itertuples(index=False, name=None)):
        raise AssertionError("F28 shape audit changed its fixed cohort")
    expected_shapes = []
    for row in shapes.itertuples(index=False):
        mean, weekday = oracle.baselines(row.shop, row.spu, row.issue_date)
        a1 = oracle.total(row.shop, row.spu, *target_window(row.issue_date, "d01_30"))[0]
        a2 = oracle.total(row.shop, row.spu, *target_window(row.issue_date, "d31_60"))[0]
        expected = f28_shape(mean, a1, a2)
        columns = {"mean28": "mean28", "p1": "prediction1", "p2": "prediction2", "p60": "prediction60",
                   "a1": "actual1", "a2": "actual2", "shape_ratio": "shape_ratio",
                   "first_level_ratio": "actual_mean1_to_input_mean", "second_level_ratio": "actual_mean2_to_input_mean"}
        for observed, wanted in columns.items():
            equal_number(getattr(row, observed), expected[wanted], "F28 shape " + observed, 1e-8)
        equal_number(row.sum28, mean * 28 if mean is not None else None, "F28 input28 sum", 1e-8)
        expected_status = {"VALID_POSITIVE_PAIR": "POSITIVE_BOTH", "UNKNOWN_TRUTH": "UNKNOWN_ACTUAL", "ZERO_ACTUAL": "ZERO_ACTUAL"}[expected["shape_status"]]
        if row.shape_status != expected_status:
            raise AssertionError("zero/unknown shape status differs")
        flag = expected["equal_prediction_cannot_hit_both"]
        if flag is None:
            if pd.notna(row.equal_segments_cannot_both_hit):
                raise AssertionError("undefined shape was assigned a boolean claim")
        elif bool(row.equal_segments_cannot_both_hit) != flag:
            raise AssertionError("shape>1.5 boundary is wrong")
        if flag and mean is not None and score_record(a1, mean * 30)["decision"] == score_record(a2, mean * 30)["decision"] == "HIT":
            raise AssertionError("impossible equal-segment dual hit")
        expected_shapes.append({**dict(zip(keys, [getattr(row, key) for key in keys])), **expected})
    expected_shapes = pd.DataFrame(expected_shapes)
    summaries = pd.read_csv(score_root / "f28_shape_summary.csv")
    expected_groups = {key: group for key, group in expected_shapes.groupby(["cohort_id", "issue_date"])}
    if len(summaries) != len(expected_groups):
        raise AssertionError("shape summary drops issue records")
    for row in summaries.itertuples(index=False):
        group = expected_groups[(row.cohort_id, row.issue_date)]
        positive = group.shape_status.eq("VALID_POSITIVE_PAIR")
        flagged = group.equal_prediction_cannot_hit_both.eq(True)
        all_units, positive_units, shape_units = float(group.actual60.sum()), float(group.loc[positive, "actual60"].sum()), float(group.loc[flagged, "actual60"].sum())
        wanted = {"core_n": len(group), "positive_both_n": int(positive.sum()), "incompatible_shape_n": int(flagged.sum()),
                  "object_share": flagged.sum() / positive.sum() if positive.any() else None,
                  "sales_share_known_core": shape_units / all_units if all_units else None,
                  "sales_share_positive_both": shape_units / positive_units if positive_units else None,
                  "zero_or_unknown_n": int((~positive).sum())}
        for field, value in wanted.items():
            equal_number(getattr(row, field), value, "shape summary " + field, 1e-12)
    grid = pd.read_csv(score_root / "full_issue_status_grid.csv")
    expected_grid = {(issue, shop, model, window) for issue in issue_grid().issue_date.unique() for shop in SHOPS
                     for model in ["F28", "WEEKDAY4", "B1", "B2", "B3", "DIRECT30x2"] for window in WINDOWS}
    grid_keys = ["issue_date", "shop", "model", "window"]
    if len(grid) != len(expected_grid) or set(grid[grid_keys].itertuples(index=False, name=None)) != expected_grid:
        raise AssertionError("full 22-month model/shop/window status map incomplete")
    quality = pd.read_csv(root / "prepared/selection_quality.csv").set_index(["issue_date", "shop"])
    group_lookup = {key: group for key, group in scores[scores.cohort_id.eq("monthly_core")].groupby(grid_keys)}
    for row in grid.itertuples(index=False):
        key = tuple(getattr(row, field) for field in grid_keys)
        group = group_lookup.get(key)
        equal_number(row.selection_core_n, quality.loc[(row.issue_date, row.shop), "core_n"], "grid selection N", 0.)
        start, end = target_window(row.issue_date, row.window)
        if date(row.target_start) != start or date(row.target_end) != end:
            raise AssertionError("status grid wrong target dates")
        if end > oracle.truth_end and row.cell_status != "NOT_MATURED":
            raise AssertionError("immature status cell dropped its reason")
        if group is not None:
            check_metric_values(row, metrics(group), grid.columns)
        else:
            equal_number(row.core_n, quality.loc[(row.issue_date, row.shop), "core_n"], "unknown vs empty core N", 0.)
    coverage = pd.read_csv(score_root / "future_coverage.csv")
    core_groups = {key: group for key, group in cohorts.groupby(["cohort_id", "issue_date", "shop"])}
    if len(coverage) != len(core_groups) * 3 or coverage.duplicated(["cohort_id", "issue_date", "shop", "window"]).any():
        raise AssertionError("future coverage changed cohort/window grid")
    for row in coverage.itertuples(index=False):
        group = core_groups[(row.cohort_id, row.issue_date, row.shop)]
        start, end = target_window(row.issue_date, row.window)
        window = source[source.shop.eq(row.shop) & source.ds.between(pd.Timestamp(start), pd.Timestamp(end))]
        valid = window.sales_state.isin(VALID_STATES) & np.isfinite(window.y) & window.y.ge(0)
        denominator = float(window.loc[valid, "y"].sum())
        partial = float(window.loc[valid & window.mapped.eq(True) & window.spu.isin(group.spu), "y"].sum())
        complete = [oracle.total(row.shop, spu, start, end)[0] for spu in group.spu]
        actual_complete = sum(value for value in complete if value is not None)
        status = "NOT_MATURED" if end > oracle.truth_end else "PARTIAL_SOURCE" if window.ds.nunique() != (end - start).days + 1 or not valid.all() else "OBSERVED_SOURCE_COVERAGE"
        if row.status != status:
            raise AssertionError("future all-shop coverage status changed")
        wanted = {"core_n": len(group), "observed_shop_units": denominator, "observed_core_units": partial,
                  "complete_window_core_units": actual_complete, "complete_window_core_n": sum(value is not None for value in complete),
                  "coverage": actual_complete / denominator if denominator > 0 and status == "OBSERVED_SOURCE_COVERAGE" else None,
                  "observed_partial_coverage": partial / denominator if denominator > 0 and status != "NOT_MATURED" else None,
                  "unmapped_units": float(window.loc[valid & ~window.mapped.eq(True), "y"].sum()),
                  "present_days": window.ds.nunique(), "expected_days": (end - start).days + 1}
        for field, value in wanted.items():
            equal_number(getattr(row, field), value, "future coverage " + field, 1e-8)
    return {"shape_rows": len(shapes), "shape_summary_rows": len(summaries), "full_grid_rows": len(grid), "future_coverage_rows": len(coverage)}


def verify_operating_indicators(score_root, source, cohorts):
    table = pd.read_csv(score_root / "operating_indicators_diagnostic.csv")
    keys = ["cohort_id", "issue_date", "shop", "spu"]
    groups = {key: group.set_index("ds") for key, group in source.groupby(["shop", "spu"], sort=False)}
    expected_keys = {key for key in cohorts[keys].itertuples(index=False, name=None) if (key[2], key[3]) in groups}
    if len(table) != len(expected_keys) or set(table[keys].itertuples(index=False, name=None)) != expected_keys:
        raise AssertionError("posthoc indicator panel changes the frozen cohort")
    maximum = 0.
    mean_fields = ["listing_sessions_sum", "order_qty", "ad_spend", "ad_clicks", "ad_orders", "sales_amt", "sku_count", "promotion_units"]
    ratios = {"order_session_ratio": ("order_qty", "listing_sessions_sum"), "ad_cost_sales_ratio": ("ad_spend", "ad_sales_amt"),
              "revenue_per_sold_unit_proxy": ("sales_amt", "y"), "volume_weighted_price": ("volume_weighted_price_numerator", "priced_units")}
    for row in table.itertuples(index=False):
        if row.evidence_grade != "INDICATOR_ONLY_NOT_CAUSAL" or any(getattr(row, field) != "UNKNOWN" for field in ["historical_effective_operations_log", "actual_budget", "confirmed_stockout_interval", "historical_category"]):
            raise AssertionError("posthoc source indicators were promoted to confirmed causal operations")
        cutoff = pd.Timestamp(row.issue_date) - pd.Timedelta(days=2)
        frames = {"input": groups[(row.shop, row.spu)].reindex(pd.date_range(cutoff - pd.Timedelta(days=27), cutoff)),
                  "target": groups[(row.shop, row.spu)].reindex(pd.date_range(pd.Timestamp(row.issue_date) + pd.Timedelta(days=1), pd.Timestamp(row.issue_date) + pd.Timedelta(days=60)))}
        for field in mean_fields:
            means = {}
            for period, frame in frames.items():
                values = frame[field] if field in frame else pd.Series(dtype=float)
                value = float(values.mean()) if len(values) == (28 if period == "input" else 60) and np.isfinite(values).all() else None
                means[period] = value
                maximum = max(maximum, equal_number(getattr(row, field + "_" + period + "_daily_mean"), value, "posthoc full-calendar mean", 1e-7))
            ratio = means["target"] / means["input"] if means["input"] is not None and means["input"] > 0 and means["target"] is not None else None
            maximum = max(maximum, equal_number(getattr(row, field + "_ratio"), ratio, "posthoc change ratio", 1e-9))
        for period, frame in frames.items():
            def total(field):
                values = frame[field] if field in frame else pd.Series(dtype=float)
                return float(values.sum()) if len(values) == (28 if period == "input" else 60) and np.isfinite(values).all() and values.ge(0).all() else None
            for label, (numerator, denominator) in ratios.items():
                n, d = total(numerator), total(denominator)
                value = n / d if n is not None and d is not None and d > 0 else None
                maximum = max(maximum, equal_number(getattr(row, period + "_" + label), value, "posthoc ratio of source sums", 1e-8))
            maximum = max(maximum, equal_number(getattr(row, period + "_zero_inventory_row_observations"), total("zero_inventory_observation_rows"), "inventory observation count, not stockout duration", 0.))
    return {"rows": len(table), "max_difference": maximum, "causal_or_effective_operations_claims": 0}


def validate_native_v4(root, output=None):
    """Independent native-side truth/summary audit, never regenerate V4 output."""
    root = Path(root).resolve()
    folder = root / "native_v4_extension"
    output = Path(output).resolve() if output else root / "validation_native_v4"
    output.mkdir(exist_ok=False)
    report = {"status": "RUNNING", "validator_sha256": sha(__file__), "scope": "NATIVE_MONTHLY_ONLY_NOT_DAILY_60DAY",
              "native_predictions_regenerated": False, "fits": 0, "native_calls": 0, "database_queries": 0,
              "limitation": "Checks archived source truth, cutoff eligibility, output coverage, decisions, summaries and hashes; does not independently regenerate native V4 predictions."}
    try:
        manifest = json.loads((folder / "execution_manifest.json").read_text(encoding="utf-8-sig"))
        if manifest["status"] != "COMPLETE" or not manifest["source_unchanged"] or manifest["fits"] != 0 or manifest["native_calculation_count"] > 144:
            raise AssertionError("native extension execution boundary changed")
        for path, digest in manifest["source_hashes"].items():
            if sha(path) != digest:
                raise AssertionError("native extension original source changed")
        for name, digest in manifest["files"].items():
            if sha(folder / name) != digest:
                raise AssertionError("native extension output changed")
        archive = next(Path(path) for path in manifest["source_hashes"] if Path(path).name == "backtest_inputs.jsonl")
        records = []
        with archive.open(encoding="utf-8-sig") as stream:
            for line in stream:
                event = json.loads(line)
                if event.get("kind") != "sales":
                    continue
                value = event["value"]
                shop, sku, spu = (str(value.get(name) or "").strip() for name in ["店铺", "SKU", "SPU"])
                quantity = value.get("销量")
                if not shop in SHOPS or not sku or not spu or not finite(quantity) or quantity < 0 or quantity != int(quantity):
                    raise AssertionError("native raw source requires an unverified mapping or quantity assumption")
                records.append((shop, sku, spu, pd.Period(str(value["统计日期"])[:7], freq="M"), int(quantity)))
        sales = pd.DataFrame(records, columns=["shop", "sku", "spu", "month", "quantity"])
        if sales.groupby(["shop", "sku"]).spu.nunique().gt(1).any():
            raise AssertionError("native SKU mapping conflict")
        truth = sales.groupby(["shop", "spu", "month"]).quantity.sum().to_dict()
        first = sales[sales.quantity.gt(0)].groupby(["shop", "spu"]).month.min().to_dict()
        details = pd.read_parquet(folder / "native_v4_detail.parquet")
        calculations = pd.read_csv(folder / "native_calculations.csv")
        targets = [str(period) for period in pd.period_range("2025-01", "2025-09", freq="M")] + ["2026-04", "2026-05", "2026-06"]
        expected_calls = {(target, f"H{h}", shop) for target in targets for h in [1, 2, 3] for shop in SHOPS}
        call_keys = ["target_month", "horizon", "shop"]
        if len(calculations) != 144 or set(calculations[call_keys].itertuples(index=False, name=None)) != expected_calls:
            raise AssertionError("native extension omitted a call or reran an archived month")
        observed_groups = {key: group for key, group in details.groupby(call_keys)}
        checked = 0
        for call in calculations.itertuples(index=False):
            target = pd.Period(call.target_month, freq="M")
            cutoff = target - int(call.horizon[1:])
            end = cutoff.start_time.date() - dt.timedelta(days=1)
            if call.cutoff_month != str(cutoff) or date(call.complete_input_month_end) != end or call.kind != "DETERMINISTIC_NATIVE_CALL_NO_FIT":
                raise AssertionError("native monthly horizon/input cutoff shifted")
            history = sales[sales.shop.eq(call.shop) & sales.month.lt(cutoff)]
            objects = history.groupby("spu").sku.nunique().to_dict()
            group = observed_groups[(call.target_month, call.horizon, call.shop)]
            if len(group) != len(objects) or set(group.spu) != set(objects):
                raise AssertionError("native inference population is not the full cutoff-known SKU/SPU context")
            equal_number(call.input_skus, history.sku.nunique(), "native input SKU context", 0.)
            for row in group.itertuples(index=False):
                age = cutoff.ordinal - first[(row.shop, row.spu)].ordinal
                qualification = "ESTABLISHED_OBSERVED_4P" if age >= 4 else "RECENT_OBSERVED_0_TO_3"
                if row.qualification != qualification or date(row.input_cutoff) != end or row.cutoff_month != str(cutoff):
                    raise AssertionError("native qualification uses a future month or the daily-core definition")
                equal_number(row.observed_age_months, age, "native observable age", 0.)
                equal_number(row.input_sku_n, objects[row.spu], "native per-SPU SKU denominator", 0.)
                actual = truth.get((row.shop, row.spu, target))
                equal_number(row.actual, actual, "native truth from original monthly archive, never missing=0", 0.)
                expected_state = "UNKNOWN_NO_MONTH_ROW" if actual is None else "EXPLICIT_ZERO" if actual == 0 else "OBSERVED"
                if row.truth_status != expected_state:
                    raise AssertionError("native truth state changed")
                complete = row.output_sku_n == row.input_sku_n
                if finite(row.prediction) != complete or (finite(row.prediction) and (row.prediction < 0 or row.prediction != int(row.prediction))):
                    raise AssertionError("native incomplete/negative/noninteger output masquerades as complete")
                own = score_record(actual, row.prediction)
                decision = "ZERO_ACTUAL_ABSOLUTE_ERROR" if actual == 0 and finite(row.prediction) else own["decision"]
                if row.decision != decision:
                    raise AssertionError("native ±20% decision differs")
                equal_number(row.absolute_error, own["absolute_error"], "native absolute error", 0.)
                equal_number(row.error, row.prediction - actual if actual is not None and finite(row.prediction) else None, "native signed error", 0.)
                expected_hit = None if own["decision"] == "UNJUDGED" else own["decision"] == "HIT"
                if expected_hit is None:
                    if pd.notna(row.hit20):
                        raise AssertionError("native unknown/zero actual assigned a percentage hit")
                elif bool(row.hit20) != expected_hit:
                    raise AssertionError("native hit20 flag differs")
                checked += 1
        summary = pd.read_csv(folder / "native_v4_summary.csv")
        mature = details[details.qualification.eq("ESTABLISHED_OBSERVED_4P")]
        seen = set()
        maximum = 0.
        for row in summary.itertuples(index=False):
            dims = row.group_by.split("+")
            if dims not in [["target_month", "horizon"], ["target_month", "horizon", "shop"]]:
                raise AssertionError("native side summary silently changes grouping")
            group = mature
            key = tuple([row.group_by] + [getattr(row, name) for name in dims])
            if key in seen:
                raise AssertionError("duplicate native summary group")
            seen.add(key)
            for field in dims:
                group = group[group[field].eq(getattr(row, field))]
            own = metrics(group)
            renames = {"eligible_n": "fixed_core_n", "hit20_n": "hit_n", "hit20_rate": "hit_rate", "unknown_actual_n": "unknown_truth_n"}
            for field in summary:
                metric = renames.get(field, field)
                if metric in own:
                    maximum = max(maximum, equal_number(getattr(row, field), own[metric], "native separate-scope summary " + field, 1e-6))
            if row.scope != "ESTABLISHED_OBSERVED_4P" or row.model != "V4_NATIVE_RECONSTRUCTED_EXTENSION":
                raise AssertionError("native scope/model relabelled as a daily-core competitor")
        expected_groups = {(label, *key) for dims, label in [(["target_month", "horizon"], "target_month+horizon"), (["target_month", "horizon", "shop"], "target_month+horizon+shop")] for key, group in mature.groupby(dims)}
        if seen != expected_groups:
            raise AssertionError("native summary grouping coverage changed")
        report.update(status="PASS_INDEPENDENT_NATIVE_TRUTH_SUMMARY_HASHES_NOT_PREDICTION_REPLAY", detail_rows=checked,
                      mature_rows=len(mature), summary_rows=len(summary), calculations=len(calculations), max_difference=maximum,
                      execution_manifest_sha256=sha(folder / "execution_manifest.json"), source_count=len(manifest["source_hashes"]), output_count=len(manifest["files"]))
        return report
    except Exception as error:
        report.update(status="FAILED", error=str(error), traceback=traceback.format_exc())
        raise
    finally:
        report["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        (output / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def validate(root, archive, score_root=None, output=None, prepared_only=False, checkpoints_only=False):
    root, archive = Path(root).resolve(), Path(archive).resolve()
    score_root = Path(score_root).resolve() if score_root else root / "scoring"
    output = Path(output).resolve() if output else root / ("validation_prepared" if prepared_only else "validation_checkpoint_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S") if checkpoints_only else "validation")
    output.mkdir(parents=True, exist_ok=False)
    report = {"status": "RUNNING", "started_at": dt.datetime.now(dt.timezone.utc).isoformat(), "validator_sha256": sha(__file__),
              "method": "independent source/calendar/core/baseline/metric arithmetic; no replay or score helper imports",
              "fits": 0, "database_queries": 0, "network_sources": 0}
    try:
        source = pd.read_parquet(root / "inputs/daily.parquet")
        source["ds"] = pd.to_datetime(source.ds).dt.normalize()
        monthly = pd.read_parquet(root / "inputs/monthly_reference.parquet")
        prior_prepared = root / "validation_prepared/validation.json"
        if checkpoints_only and prior_prepared.exists():
            prior = json.loads(prior_prepared.read_text(encoding="utf-8-sig"))
            verified_manifest(root / "prepared", "prepare_manifest.json")
            if prior.get("status") != "PASS_INDEPENDENT_PREPARED_RECONCILIATION" or prior["prepared"]["prepare_manifest_sha256"] != sha(root / "prepared/prepare_manifest.json"):
                raise AssertionError("prepared-only receipt is not applicable to these immutable inputs")
            cohorts = pd.concat([pd.read_parquet(root / "prepared/core.parquet"), pd.read_csv(root / "prepared/original_core_panel.csv")], ignore_index=True)
            report["prepared"] = {**prior["prepared"], "reused_prepared_receipt_sha256": sha(prior_prepared)}
        else:
            cohorts, report["prepared"] = verify_prepared(root, source, monthly, archive)
        if checkpoints_only:
            report["completed_models"] = verify_completed_slots(root, source, monthly, CalendarOracle(source), cohorts)
            report["status"] = "PASS_COMPLETED_SLOTS_ONLY_NOT_FINAL"
        elif not prepared_only:
            report["prior_archives"] = verify_prior_archives(archive)
            oracle = CalendarOracle(source)
            predictions, report["predictions"] = verify_predictions(root, oracle, cohorts)
            scores, report["scores"] = verify_scores(score_root, predictions, oracle, cohorts)
            report["pairs_joints_common"] = verify_pair_joint_common(score_root, scores)
            report["shape_grid_coverage"] = verify_shapes_and_grid(root, score_root, scores, oracle, cohorts, source)
            report["completed_models"] = verify_completed_slots(root, source, monthly, oracle, cohorts, predictions)
            report["inference_coverage"] = verify_inference_coverage(root, monthly, oracle, cohorts, predictions)
            report["training_metadata"] = verify_training_metadata(root, source, monthly, oracle)
            report["operating_indicators"] = verify_operating_indicators(score_root, source, cohorts)
            fitted = json.loads((root / "effects/fits.json").read_text(encoding="utf-8-sig"))
            if report["completed_models"]["completed_slots"] != sum(row["status"] in {"FITTED", "RECOVERED"} for row in fitted):
                raise AssertionError("checkpoint validation omits a completed formal slot")
            verified_manifest(root / "prepared", "prepare_manifest.json")
            verified_manifest(root / "effects", "run_manifest.json")
            verified_manifest(score_root, "score_manifest.json")
            if verify_prior_archives(archive) != report["prior_archives"]:
                raise AssertionError("original archive changed during validation")
            report["status"] = "PASS_INDEPENDENT_FULL_RECONCILIATION"
        else:
            report["status"] = "PASS_INDEPENDENT_PREPARED_RECONCILIATION"
        return report
    except Exception as error:
        report.update(status="FAILED", error=str(error), traceback=traceback.format_exc())
        raise
    finally:
        report["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        (output / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--archive")
    parser.add_argument("--scores")
    parser.add_argument("--output")
    parser.add_argument("--prepared-only", action="store_true")
    parser.add_argument("--checkpoints-only", action="store_true", help="Read-only audit of completed slots; no new fits; content-addressed cache receipts")
    parser.add_argument("--native-v4-only", action="store_true", help="Audit native side-panel source truth and summaries without rerunning its predictions")
    args = parser.parse_args()
    if args.native_v4_only:
        validate_native_v4(args.root, args.output)
    elif args.archive is None:
        parser.error("--archive is required for daily-core validation")
    else:
        validate(args.root, args.archive, args.scores, args.output, args.prepared_only, args.checkpoints_only)
