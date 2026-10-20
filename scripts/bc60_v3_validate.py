"""Independent BC60 V3 verification from immutable local source calendars.

No training-engine imports, fits, database calls, or source mutations.  The
calendar and qualification oracles below reconstruct feature/label boundaries
directly, including all historical candidate dates and completed-month eligibility.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import traceback

import numpy as np
import pandas as pd

try:
    from scripts.bc60_v2_validate import independent_metric, independent_native_mean, verify_old_hashes, sha256, day, _same_number
except ModuleNotFoundError:
    from bc60_v2_validate import independent_metric, independent_native_mean, verify_old_hashes, sha256, day, _same_number


SHOPS = ["JQ-US", "MT-US", "RKZ-US", "SY-US"]
VALID = {"OBSERVED", "EXPLICIT_ZERO"}
FEATURES = ["shop_code", "lag1", "lag7", "lag14", "lag28", "rolling_mean_lag1_window_size7",
            "rolling_mean_lag1_window_size28", "dayofweek", "month", "dayofyear"]
SEGMENTS = {"M1": (1, 30), "M2": (31, 60)}
WINDOWS = {"d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
CORE_KEY = ["issue_date", "shop", "spu"]


def label_dates(simulated_issue, segment):
    first, last = SEGMENTS[segment]
    issue = day(simulated_issue)
    return issue + dt.timedelta(days=first), issue + dt.timedelta(days=last)


def label_mature(actual_issue, simulated_issue, segment):
    return label_dates(simulated_issue, segment)[1] <= day(actual_issue) - dt.timedelta(days=2)


def completed_month_end(cutoff):
    cutoff = day(cutoff)
    this_month = pd.Period(cutoff, freq="M")
    end = this_month.end_time.date()
    return end if end <= cutoff else (this_month - 1).end_time.date()


def independent_nonoverlap_count(simulated_issues, segment):
    """Maximum count for equal-length intervals, via earliest-ending greedy scan."""
    last_end, count = None, 0
    for issue in sorted(set(map(day, simulated_issues))):
        start, end = label_dates(issue, segment)
        if last_end is None or start > last_end:
            count += 1
            last_end = end
    return count


class Qualification:
    def __init__(self, monthly):
        table = monthly.copy()
        table["month"] = pd.PeriodIndex(table.month, freq="M")
        if table.duplicated(["shop", "spu", "month"]).any():
            raise ValueError("monthly qualification grain is not unique")
        self.positive_ends = {}
        for pair, group in table[table.qty.gt(0)].groupby(["shop", "spu"], sort=False):
            self.positive_ends[pair] = sorted(month.end_time.date() for month in group.month)

    def count(self, shop, spu, simulated_issue):
        cutoff = day(simulated_issue) - dt.timedelta(days=2)
        return bisect_right(self.positive_ends.get((shop, spu), []), cutoff)

    def eligible(self, shop, spu, simulated_issue):
        return self.count(shop, spu, simulated_issue) >= 4


class DirectCalendar:
    """A complete calendar with validity counts; missing/invalid is never zero."""
    def __init__(self, daily):
        data = daily.copy()
        data["ds"] = pd.to_datetime(data.ds).dt.normalize()
        if data.duplicated(["shop", "spu", "ds"]).any():
            raise ValueError("daily source grain is not unique")
        self.start, self.end = day(data.ds.min()), day(data.ds.max())
        self.length = (self.end - self.start).days + 1
        self.values, self.states, self.mapped, self.prefix, self.valid_prefix = {}, {}, {}, {}, {}
        self.context = set()
        self.first_mapped = {}
        for pair, group in data.groupby(["shop", "spu"], sort=False):
            position = (group.ds - pd.Timestamp(self.start)).dt.days.to_numpy()
            values = np.full(self.length, np.nan)
            states = np.full(self.length, "MISSING", dtype=object)
            mapped = np.zeros(self.length, dtype=bool)
            values[position] = group.y.to_numpy(dtype=float)
            states[position] = group.sales_state.to_numpy()
            mapped[position] = group.mapped.to_numpy(dtype=bool)
            valid = np.isfinite(values) & (values >= 0) & np.isin(states, list(VALID)) & mapped
            self.values[pair], self.states[pair], self.mapped[pair] = values, states, mapped
            self.prefix[pair] = np.r_[0., np.cumsum(np.where(valid, values, 0.))]
            self.valid_prefix[pair] = np.r_[0, np.cumsum(valid)]
            if pair[0] in SHOPS and mapped.any():
                self.context.add(pair)
                self.first_mapped[pair] = self.start + dt.timedelta(days=int(np.flatnonzero(mapped)[0]))

    def total(self, shop, spu, start, end):
        start, end, pair = day(start), day(end), (shop, spu)
        if end > self.end:
            return np.nan, "NOT_MATURED"
        if end < start or start < self.start or pair not in self.values:
            return np.nan, "UNKNOWN_TRUTH"
        left, right = (start - self.start).days, (end - self.start).days + 1
        if self.valid_prefix[pair][right] - self.valid_prefix[pair][left] != right - left:
            return np.nan, "UNKNOWN_TRUTH"
        return float(self.prefix[pair][right] - self.prefix[pair][left]), "OBSERVED"

    def features(self, shop, spu, simulated_issue, segment):
        cutoff = day(simulated_issue) - dt.timedelta(days=2)
        known, state = self.total(shop, spu, cutoff - dt.timedelta(days=27), cutoff)
        if state != "OBSERVED":
            return None
        position = (cutoff - self.start).days
        history = self.values[(shop, spu)][position - 27:position + 1].astype(np.float32)
        target_start, _ = label_dates(simulated_issue, segment)
        result = {"shop_code": SHOPS.index(shop)}
        result.update({f"lag{lag}": float(history[-lag]) for lag in [1, 7, 14, 28]})
        for width in [7, 28]:
            result[f"rolling_mean_lag1_window_size{width}"] = float(independent_native_mean(history[-width:], np.float32))
        result.update(dayofweek=target_start.weekday(), month=target_start.month, dayofyear=target_start.timetuple().tm_yday)
        return result

    def label(self, shop, spu, simulated_issue, segment):
        first, last = label_dates(simulated_issue, segment)
        return self.total(shop, spu, first, last)


def candidate_status(calendar, qualification, actual_issue, simulated_issue, shop, spu, segment):
    """Independent order-free exclusion reasons and the valid label if any."""
    reasons = []
    if (shop, spu) not in calendar.context:
        reasons.append("UNSAFE_OR_OUT_OF_SCOPE")
    if not label_mature(actual_issue, simulated_issue, segment):
        reasons.append("LABEL_NOT_MATURE")
    if not qualification.eligible(shop, spu, simulated_issue):
        reasons.append("INSUFFICIENT_POSITIVE_MONTHS")
    features = calendar.features(shop, spu, simulated_issue, segment)
    if features is None:
        reasons.append("MISSING_HISTORY")
    label, label_state = calendar.label(shop, spu, simulated_issue, segment)
    if label_state != "OBSERVED":
        reasons.append("MISSING_LABEL")
    return reasons, features, label


def support_status(simulated_issues, segment):
    issues = list(simulated_issues)
    if not issues:
        return "NOT_TRAINABLE"
    return "LIMITED_TEMPORAL_SUPPORT" if independent_nonoverlap_count(issues, segment) < 2 else "TEMPORAL_SUPPORT_AT_LEAST_TWO_NONOVERLAPPING_WINDOWS"


def add_segments(first, second):
    if pd.isna(first) or pd.isna(second):
        return np.nan
    return float(first) + float(second)


def reconstruct_training(calendar, qualification, actual_issue, segment):
    """Exhaustive context/date key reconstruction, independent of saved train rows."""
    actual_issue = day(actual_issue)
    cutoff = actual_issue - dt.timedelta(days=2)
    earliest = calendar.start + dt.timedelta(days=29)
    dates = [earliest + dt.timedelta(days=i) for i in range(max(0, (cutoff - earliest).days + 1))]
    cutoffs = np.array([(date - dt.timedelta(days=2)).toordinal() for date in dates])
    positions = cutoffs - calendar.start.toordinal()
    first, last = SEGMENTS[segment]
    label_left = positions + 2 + first
    label_right = positions + 2 + last + 1
    mature = np.array([date + dt.timedelta(days=last) <= cutoff for date in dates])
    expected, per_object = {}, []
    context = {pair for pair in calendar.context if calendar.first_mapped[pair] <= cutoff}
    reason_counts = {reason: np.zeros(len(dates), dtype=np.int64) for reason in ["INSUFFICIENT_QUALIFICATION", "LABEL_NOT_MATURE", "MISSING_HISTORY", "MISSING_LABEL_DAYS"]}
    funnel = {"candidate_n": len(context) * len(dates), "qualification_excluded_n": 0,
              "maturity_excluded_n": 0, "history_excluded_n": 0, "label_excluded_n": 0, "eligible_n": 0}
    for shop, spu in sorted(context):
        ends = np.array([date.toordinal() for date in qualification.positive_ends.get((shop, spu), [])], dtype=np.int64)
        counts = np.searchsorted(ends, cutoffs, side="right")
        valid = counts >= 4
        funnel["qualification_excluded_n"] += int((~valid).sum())
        reason_counts["INSUFFICIENT_QUALIFICATION"] += ~valid
        funnel["maturity_excluded_n"] += int((valid & ~mature).sum())
        reason_counts["LABEL_NOT_MATURE"] += valid & ~mature
        valid &= mature
        prefix = calendar.valid_prefix[(shop, spu)]
        history_ok = (positions >= 27) & (positions < calendar.length)
        history_ok &= prefix[np.clip(positions + 1, 0, calendar.length)] - prefix[np.clip(positions - 27, 0, calendar.length)] == 28
        funnel["history_excluded_n"] += int((valid & ~history_ok).sum())
        reason_counts["MISSING_HISTORY"] += valid & ~history_ok
        valid &= history_ok
        label_ok = (label_left >= 0) & (label_right <= calendar.length)
        label_ok &= prefix[np.clip(label_right, 0, calendar.length)] - prefix[np.clip(label_left, 0, calendar.length)] == 30
        funnel["label_excluded_n"] += int((valid & ~label_ok).sum())
        reason_counts["MISSING_LABEL_DAYS"] += valid & ~label_ok
        valid &= label_ok
        accepted_dates = []
        totals = calendar.prefix[(shop, spu)]
        for index in np.flatnonzero(valid):
            issue = dates[index]
            expected[(shop, spu, issue.isoformat())] = (float(totals[label_right[index]] - totals[label_left[index]]), int(counts[index]))
            accepted_dates.append(issue)
        per_object.append({"shop": shop, "spu": spu, "train_rows": len(accepted_dates),
                           "nonoverlapping_windows": independent_nonoverlap_count(accepted_dates, segment)})
    funnel["eligible_n"] = len(expected)
    excluded = pd.DataFrame([{"simulated_issue_date": str(date), "reason": reason, "rows": int(counts[index])}
                            for reason, counts in reason_counts.items() for index, date in enumerate(dates) if counts[index] > 0])
    return expected, funnel, pd.DataFrame(per_object), excluded


def verify_feature_record(row, calendar, segment):
    simulated = day(row.simulated_issue_date)
    cutoff = simulated - dt.timedelta(days=2)
    if day(row.input_cutoff) != cutoff or day(row.history_start) != cutoff - dt.timedelta(days=27) or day(row.history_end) != cutoff:
        raise AssertionError("feature history/cutoff does not follow simulated issue")
    start, end = label_dates(simulated, segment)
    if day(row.label_start) != start or day(row.label_end) != end:
        raise AssertionError("wrong label bounds or shifted bridge days")
    expected = calendar.features(row.shop, row.spu, simulated, segment)
    if expected is None:
        raise AssertionError("feature row has incomplete/unsafe historical input")
    maximum = 0.0
    for field, value in expected.items():
        maximum = max(maximum, _same_number(getattr(row, field), value, f"independent V3 feature {field}", 0.0))
    return maximum


def verify_prepared(root, calendar, qualification, core):
    prepared = root / "prepared"
    manifest = json.loads((prepared / "prepare_manifest.json").read_text(encoding="utf-8-sig"))
    for relative, expected_hash in manifest["files"].items():
        if sha256(prepared / relative) != expected_hash:
            raise AssertionError(f"prepared manifest mismatch: {relative}")
    coverage = pd.read_csv(prepared / "coverage.csv").set_index(["issue_date", "segment"])
    reported_exclusions = pd.read_csv(prepared / "exclusions.csv")
    reported_objects = pd.read_csv(prepared / "per_object_windows.csv")
    reports, funnel_rows, object_rows = [], [], []
    artifact_hashes = {}
    for issue in sorted(core.issue_date.unique()):
        issue_core = core[core.issue_date.eq(issue)]
        for segment in SEGMENTS:
            expected, funnel, object_support, excluded = reconstruct_training(calendar, qualification, issue, segment)
            path = prepared / "train" / f"{issue}_{segment}.parquet"
            table = pd.read_parquet(path)
            artifact_hashes[str(path.relative_to(root))] = sha256(path)
            observed = {(row.shop, row.spu, str(day(row.simulated_issue_date))) for row in table.itertuples(index=False)}
            if len(observed) != len(table) or observed != set(expected):
                missing, extra = len(set(expected) - observed), len(observed - set(expected))
                raise AssertionError(f"full-context train keys differ for {issue}/{segment}: missing={missing}, extra={extra}")
            max_label_difference = max_feature_difference = 0.0
            for row in table.itertuples(index=False):
                if day(row.issue_date) != day(issue) or row.segment != segment:
                    raise AssertionError("train row assigned to wrong issue/segment")
                label, positive_months = expected[(row.shop, row.spu, str(day(row.simulated_issue_date)))]
                max_label_difference = max(max_label_difference, _same_number(row.y_label, label, "training label from original daily", 0.0))
                if row.positive_completed_months != positive_months:
                    raise AssertionError("training qualification uses the wrong monthly visibility")
                max_feature_difference = max(max_feature_difference, verify_feature_record(row, calendar, segment))
            inference_path = prepared / "inference" / f"{issue}_{segment}.parquet"
            inference = pd.read_parquet(inference_path)
            artifact_hashes[str(inference_path.relative_to(root))] = sha256(inference_path)
            if len(inference) != len(issue_core) or inference.duplicated(["shop", "spu"]).any() or set(inference[["shop", "spu"]].itertuples(index=False, name=None)) != set(issue_core[["shop", "spu"]].itertuples(index=False, name=None)):
                raise AssertionError("inference changed frozen original cohort")
            for row in inference.itertuples(index=False):
                if day(row.issue_date) != day(issue) or day(row.simulated_issue_date) != day(issue) or row.segment != segment:
                    raise AssertionError("inference feature date/segment changed")
                eligible = qualification.eligible(row.shop, row.spu, issue)
                feature = calendar.features(row.shop, row.spu, issue, segment)
                state = "INSUFFICIENT_QUALIFICATION" if not eligible else "MISSING_HISTORY" if feature is None else "OK"
                if row.input_status != state:
                    raise AssertionError("inference qualification/missingness status mismatch")
                if state == "OK":
                    if row.positive_completed_months != qualification.count(row.shop, row.spu, issue):
                        raise AssertionError("inference positive-month count changed")
                    verify_feature_record(row, calendar, segment)
            dates = sorted({key[2] for key in expected})
            labels = [value[0] for value in expected.values()]
            report = {"issue_date": issue, "segment": segment, "train_rows": len(expected),
                      "objects": sum(object_support.train_rows.gt(0)), "simulated_dates": len(dates),
                      "first_simulated_issue": min(dates) if dates else None,
                      "last_simulated_issue": max(dates) if dates else None,
                      "positive_labels": sum(value > 0 for value in labels), "zero_labels": sum(value == 0 for value in labels),
                      "global_nonoverlap": independent_nonoverlap_count(dates, segment),
                      "support_status": support_status(dates, segment),
                      "max_label_difference": max_label_difference, "max_feature_difference": max_feature_difference}
            if labels and not any(value > 0 for value in labels):
                report["support_status"] = "NOT_TRAINABLE_ZERO_LABEL_SUPPORT"
            cov = coverage.loc[(issue, segment)]
            count_map = {"train_rows": "train_rows", "train_objects": "objects", "train_issue_dates": "simulated_dates",
                         "positive_labels": "positive_labels", "zero_labels": "zero_labels", "max_global_nonoverlap": "global_nonoverlap"}
            for field, expected_field in count_map.items():
                if cov[field] != report[expected_field]:
                    raise AssertionError(f"training coverage {field} differs at {issue}/{segment}")
            expected_support = "TRAINABLE_REPLAY_ONLY" if report["support_status"] == "TEMPORAL_SUPPORT_AT_LEAST_TWO_NONOVERLAPPING_WINDOWS" else report["support_status"]
            if cov.support_status != expected_support:
                raise AssertionError("limited/zero training support status mismatch")
            if cov.context_objects != len(object_support) or cov.inference_ok != int(inference.input_status.eq("OK").sum()):
                raise AssertionError("coverage context or inference count mismatch")
            if dates:
                for field, value in [("first_simulated_issue", min(dates)), ("last_simulated_issue", max(dates)),
                                     ("first_label", label_dates(min(dates), segment)[0]), ("last_label", label_dates(max(dates), segment)[1])]:
                    if day(cov[field]) != day(value):
                        raise AssertionError(f"coverage date {field} changed")
            actual_exc = reported_exclusions[reported_exclusions.issue_date.eq(issue) & reported_exclusions.segment.eq(segment)]
            actual_exclusions = {(str(day(r.simulated_issue_date)), r.reason): r.rows for r in actual_exc.itertuples()}
            expected_exclusions = {(r.simulated_issue_date, r.reason): r.rows for r in excluded.itertuples()}
            if len(actual_exc) != len(actual_exclusions) or actual_exclusions != expected_exclusions:
                raise AssertionError("ordered training exclusion funnel differs by simulated date")
            actual_objects = reported_objects[reported_objects.issue_date.eq(issue) & reported_objects.segment.eq(segment)]
            actual_support = {(r.shop, r.spu): (r.train_rows, r.nonoverlap_windows) for r in actual_objects.itertuples()}
            expected_object_support = {(r.shop, r.spu): (r.train_rows, r.nonoverlapping_windows) for r in object_support.itertuples() if r.train_rows > 0}
            if len(actual_objects) != len(actual_support) or actual_support != expected_object_support:
                raise AssertionError("per-object independent/nonoverlapping label support mismatch")
            reports.append(report)
            funnel_rows.append({"issue_date": issue, "segment": segment, **funnel})
            object_rows.append(object_support.assign(issue_date=issue, segment=segment))
            print(f"independent prepared {issue} {segment}: {len(expected)} rows, {len(dates)} dates", flush=True)
    return reports, pd.DataFrame(funnel_rows), pd.concat(object_rows, ignore_index=True), artifact_hashes


def verify_v2_hashes(v2_root):
    checked = {}
    for folder, filename in [("b_rolling", "run_manifest.json"), ("a_diagnostic_final", "diagnostic_manifest.json")]:
        manifest = json.loads((v2_root / folder / filename).read_text(encoding="utf-8-sig"))
        for name, expected in manifest["files"].items():
            path = v2_root / folder / name
            actual = sha256(path)
            if actual != expected:
                raise AssertionError(f"prior V2 output modified: {folder}/{name}")
            checked[f"{folder}/{name}"] = actual
    return checked


def independent_decision(actual, prediction):
    if not np.isfinite(actual) or not np.isfinite(prediction) or actual <= 0:
        return "UNJUDGED"
    return "HIT" if abs(prediction - actual) / actual <= .2 else "MISS"


def independent_joint(decisions):
    values = list(decisions)
    if len(values) != 3 or "UNJUDGED" in values:
        return "UNJUDGED"
    return "HIT" if all(value == "HIT" for value in values) else "MISS"


def independent_migration(old, new):
    if "UNJUDGED" in (old, new):
        return "UNJUDGED"
    return {("HIT", "HIT"): "RETAINED_HIT", ("MISS", "HIT"): "NEW_HIT",
            ("HIT", "MISS"): "EXITED_HIT", ("MISS", "MISS"): "BOTH_MISS"}[(old, new)]


def score_scopes(scores):
    return {"full_core": scores, "original_b1_369": scores[scores.b1_original_eligible],
            "common_complete": scores[scores.common_complete]}


def assert_key_grid(frame, keys, expected, label):
    actual = set(frame[keys].itertuples(index=False, name=None))
    if len(actual) != len(frame) or actual != set(expected):
        raise AssertionError(f"{label} missing, duplicated, or extra keys")


def verify_checkpoints(root, core, calendar, qualification):
    """Load each saved booster for prediction only; never instantiate a trainer."""
    from lightgbm import Booster
    prepared, effects = root / "prepared", root / "effects"
    manifest = json.loads((effects / "run_manifest.json").read_text(encoding="utf-8-sig"))
    if manifest["classification"] != "RECONSTRUCTED_HISTORICAL_REPLAY" or not manifest["originals_unchanged"]:
        raise AssertionError("effect classification or prior-data integrity failed")
    if manifest["prepared_manifest_sha256"] != sha256(prepared / "prepare_manifest.json"):
        raise AssertionError("effects used a different prepared manifest")
    effect_hashes = {}
    for relative, expected in manifest["files"].items():
        observed = sha256(effects / relative)
        if observed != expected:
            raise AssertionError(f"effect file hash mismatch: {relative}")
        effect_hashes[relative] = observed
    frozen = json.loads((prepared / "frozen_config.json").read_text(encoding="utf-8-sig"))
    base = Path(__file__).resolve().parents[1] / "docs/bc60_design_lock_v1.json"
    if sha256(base) != frozen["v3"]["base_config_sha256"] or frozen["lightgbm"] != json.loads(base.read_text(encoding="utf-8-sig"))["lightgbm"]:
        raise AssertionError("original estimator parameters changed")
    if frozen["v3"]["features"] != FEATURES:
        raise AssertionError("features differ from the preregistered ten")
    fits = json.loads((effects / "fits.json").read_text(encoding="utf-8-sig"))
    ledger = json.loads((root / "formal_fit_ledger.json").read_text(encoding="utf-8-sig"))
    expected_pairs = {(issue, segment) for issue in core.issue_date.unique() for segment in SEGMENTS}
    if len(fits) != 6 or {(fit["issue_date"], fit["segment"]) for fit in fits} != expected_pairs:
        raise AssertionError("fit targets changed")
    formal_count = sum(bool(fit["fit_attempted"]) for fit in ledger["fits"])
    if formal_count > 6 or manifest["formal_fit_count"] != sum(bool(fit["fit_attempted"]) for fit in fits):
        raise AssertionError("formal fit budget or ledger inconsistent")
    predictions = pd.read_csv(effects / "predictions.csv")
    expected_grid = {(*key, segment) for key in core[CORE_KEY].itertuples(index=False, name=None) for segment in SEGMENTS}
    assert_key_grid(predictions, CORE_KEY + ["segment"], expected_grid, "segment predictions")
    predictions_index = predictions.set_index(CORE_KEY + ["segment"])
    checked = []
    for fit in fits:
        issue, segment = fit["issue_date"], fit["segment"]
        training_path = prepared / "train" / f"{issue}_{segment}.parquet"
        if fit["training_sha256"] != sha256(training_path):
            raise AssertionError("model used different training records")
        if fit.get("fit_seconds", 0) > 600:
            raise AssertionError("fit exceeded frozen time budget")
        inference = pd.read_parquet(prepared / "inference" / f"{issue}_{segment}.parquet")
        if fit["status"] not in {"FITTED", "RECOVERED"}:
            raise AssertionError(f"cannot accept unsuccessful formal fit: {fit['status']}")
        model_path = effects / "models" / f"{issue}_{segment}.txt"
        if sha256(model_path) != fit["model_sha256"]:
            raise AssertionError("checkpoint hash differs from recorded fit")
        # Windows native LightGBM fopen cannot handle the Chinese absolute parent;
        # an ASCII relative path still addresses the identical hash-verified file.
        model = Booster(model_file=os.path.relpath(model_path, Path.cwd()))
        if model.feature_name() != FEATURES or model.num_trees() != fit["actual_trees"] or model.num_trees() > 200:
            raise AssertionError("saved model feature schema or tree budget changed")
        parameter_aliases = {"n_estimators": "num_iterations", "min_child_samples": "min_data_in_leaf",
                             "reg_lambda": "lambda_l2", "random_state": "seed", "n_jobs": "num_threads"}
        for parameter, value in frozen["lightgbm"].items():
            saved = model.params.get(parameter_aliases.get(parameter, parameter))
            if saved != value:
                raise AssertionError(f"saved checkpoint parameter differs: {parameter}={saved}, expected={value}")
        valid = inference.input_status.eq("OK")
        features = pd.DataFrame([calendar.features(r.shop, r.spu, issue, segment)
                                 for r in inference[valid].itertuples(index=False)], columns=FEATURES)
        expected_raw = model.predict(features, validate_features=True, num_threads=2)
        replay = dict(zip(inference.index[valid], expected_raw))
        maximum = 0.0
        clipped_n = 0
        for index, row in inference.iterrows():
            observed = predictions_index.loc[(issue, row.shop, row.spu, segment)]
            expected_status = "OK" if index in replay else row.input_status
            if observed.prediction_status != expected_status:
                raise AssertionError("segment prediction missing-state changed")
            if qualification.eligible(row.shop, row.spu, issue) != (row.positive_completed_months >= 4):
                raise AssertionError("segment qualification status changed")
            start, end = label_dates(issue, segment)
            if day(observed.input_cutoff) != day(issue) - dt.timedelta(days=2) or day(observed.target_start) != start or day(observed.target_end) != end:
                raise AssertionError("segment prediction temporal metadata changed")
            if observed.model_sha256 != fit["model_sha256"]:
                raise AssertionError("prediction checkpoint provenance changed")
            raw = float(replay[index]) if index in replay else np.nan
            expected = max(raw, 0.) if np.isfinite(raw) else np.nan
            maximum = max(maximum, _same_number(observed.prediction_raw, raw, "checkpoint raw replay", 1e-8))
            maximum = max(maximum, _same_number(observed.prediction, expected, "clipped checkpoint replay", 1e-8))
            clipped_n += int(np.isfinite(raw) and raw < 0)
        if fit["negative_clipped_n"] != clipped_n:
            raise AssertionError("negative-clipping count changed")
        checked.append({"issue_date": issue, "segment": segment, "checkpoint_sha256": fit["model_sha256"],
                        "output_n": int(valid.sum()), "max_checkpoint_prediction_difference": maximum,
                        "actual_trees": model.num_trees()})
    return predictions, checked, effect_hashes, formal_count


def verify_effects(archive, v2_root, root, calendar, qualification, core, output):
    effects = root / "effects"
    predictions, checkpoints, effect_hashes, formal_count = verify_checkpoints(root, core, calendar, qualification)
    originals = pd.read_csv(archive / "effects_v1/scores.csv")
    original = originals.set_index(CORE_KEY + ["window", "model"])
    diag = pd.read_csv(v2_root / "a_diagnostic_final/stage_scores.csv")
    diag = diag[diag.model.eq("D1")].set_index(CORE_KEY + ["stage"])
    b1 = originals[originals.model.eq("B1") & originals.window.eq("d01_60")]
    eligible = set(b1.loc[b1.prediction.notna(), CORE_KEY].itertuples(index=False, name=None))
    if len(eligible) != 369:
        raise AssertionError("original 369/16 availability changed")
    core_by_key = core.set_index(CORE_KEY)
    scores = pd.read_csv(effects / "scores.csv")
    parquet = pd.read_parquet(effects / "scores.parquet")
    pd.testing.assert_frame_equal(scores, parquet, check_dtype=False, check_exact=False, rtol=1e-13, atol=1e-9)
    models = ["F28", "B1", "D1", "DIRECT30x2"]
    expected_keys = {(*key, window, model) for key in core[CORE_KEY].itertuples(index=False, name=None)
                     for window in WINDOWS for model in models}
    assert_key_grid(scores, CORE_KEY + ["window", "model"], expected_keys, "fixed-cohort scores")
    pp = predictions.set_index(CORE_KEY + ["segment"])
    max_truth = max_prediction = 0.0
    independent = []
    for row in scores.itertuples(index=False):
        key = (row.issue_date, row.shop, row.spu)
        origin = core_by_key.loc[key]
        issue = day(row.issue_date)
        first, last = WINDOWS[row.window]
        start, end = issue + dt.timedelta(days=first), issue + dt.timedelta(days=last)
        actual, truth_state = calendar.total(row.shop, row.spu, start, end)
        if day(row.input_cutoff) != issue - dt.timedelta(days=2) or day(row.target_start) != start or day(row.target_end) != end:
            raise AssertionError("score target bounds or cutoff changed")
        if row.lifecycle != origin.lifecycle or row.maturity != origin.maturity or row.b1_original_eligible != (key in eligible):
            raise AssertionError("score changed frozen lifecycle, maturity, or B1 scope")
        if row.model in {"F28", "B1"}:
            prior = original.loc[(*key, row.window, "rolling28" if row.model == "F28" else "B1")]
            expected_prediction, prediction_status = prior.prediction, prior.prediction_state
        elif row.model == "D1":
            prior = diag.loc[(*key, row.window)]
            expected_prediction, prediction_status = prior.prediction, prior.prediction_state
        else:
            segments = ["M1"] if row.window == "d01_30" else ["M2"] if row.window == "d31_60" else ["M1", "M2"]
            parts = [pp.loc[(*key, segment)] for segment in segments]
            values = [part.prediction for part in parts]
            expected_prediction = values[0] if len(values) == 1 else add_segments(*values)
            prediction_status = "OK" if np.isfinite(expected_prediction) else "|".join(sorted({part.prediction_status for part in parts if pd.isna(part.prediction)}))
            if key not in eligible and np.isfinite(expected_prediction):
                raise AssertionError("DIRECT filled an original unqualified 16-record gap")
        max_truth = max(max_truth, _same_number(row.actual, actual, "independent window truth", 0.0))
        max_prediction = max(max_prediction, _same_number(row.prediction, expected_prediction, "frozen controls or segment sum", 1e-8))
        if row.truth_status != truth_state or row.prediction_status != prediction_status:
            raise AssertionError("score missing/truth status changed")
        decision = independent_decision(actual, expected_prediction)
        if row.decision != decision:
            raise AssertionError("zero/missing/hit-boundary decision mismatch")
        absolute = abs(expected_prediction - actual) if np.isfinite(expected_prediction) and np.isfinite(actual) else np.nan
        relative = (expected_prediction - actual) / actual if np.isfinite(absolute) and actual > 0 else np.nan
        _same_number(row.absolute_error, absolute, "score absolute error", 1e-8)
        _same_number(row.relative_error, relative, "score relative error", 1e-12)
        independent.append({**row._asdict(), "actual": actual, "prediction": expected_prediction,
                            "decision": decision, "absolute_error": absolute, "relative_error": relative})
    reconstructed = pd.DataFrame(independent)
    complete = reconstructed.groupby(CORE_KEY)[["actual", "prediction"]].apply(lambda group: np.isfinite(group).all().all())
    expected_common = np.array([bool(complete.loc[key]) for key in reconstructed[CORE_KEY].itertuples(index=False, name=None)])
    if not np.array_equal(reconstructed.common_complete.to_numpy(), expected_common):
        raise AssertionError("new common scope drops or adds records incorrectly")
    scopes = score_scopes(reconstructed)
    summaries = pd.read_csv(effects / "summary.csv")
    metric_names = {"fixed_n": "fixed_core_n", "known_fixed_actual_units": "known_fixed_core_actual_units"}
    slice_fields = {"overall": [], "issue": ["issue_date"], "shop": ["shop"], "maturity": ["maturity"], "lifecycle": ["lifecycle"]}
    metric_records, maximum_metric = [], 0.0
    expected_summary_keys = set()
    for scope, table in scopes.items():
        for slice_name, fields in slice_fields.items():
            for values, group in table.groupby(["model", "window"] + fields, dropna=False):
                expected_summary_keys.add((scope, slice_name, *values))
    observed_summary_keys = set()
    for row in summaries.itertuples(index=False):
        fields = slice_fields[row.slice]
        key = (row.scope, row.slice, row.model, row.window, *(getattr(row, field) for field in fields))
        if key in observed_summary_keys:
            raise AssertionError("duplicate summary")
        observed_summary_keys.add(key)
        group = scopes[row.scope]
        group = group[group.model.eq(row.model) & group.window.eq(row.window)]
        for field in fields:
            group = group[group[field].eq(getattr(row, field))]
        metric = independent_metric(group)
        for field in summaries.columns:
            expected_field = metric_names.get(field, field)
            if expected_field in metric:
                maximum_metric = max(maximum_metric, _same_number(getattr(row, field), metric[expected_field], f"summary {field}", 1e-7))
        metric_records.append({"scope": row.scope, "slice": row.slice, "model": row.model, "window": row.window,
                               **{field: getattr(row, field) for field in fields}, **metric})
    if observed_summary_keys != expected_summary_keys:
        raise AssertionError("summary scope/slice coverage incomplete")
    pd.DataFrame(metric_records).to_csv(output / "independent_summary.csv", index=False, encoding="utf-8-sig")
    joint = pd.read_csv(effects / "joint_hits.csv")
    joint_keys = {(*key, model) for key in core[CORE_KEY].itertuples(index=False, name=None) for model in models}
    assert_key_grid(joint, CORE_KEY + ["model"], joint_keys, "joint decisions")
    independent_joint_rows = []
    for key, group in reconstructed.groupby(CORE_KEY + ["model"]):
        independent_joint_rows.append({**dict(zip(CORE_KEY + ["model"], key)),
            "joint_decision": independent_joint(group.decision),
            "actual60": float(group.loc[group.window.eq("d01_60"), "actual"].iloc[0]),
            "b1_original_eligible": bool(group.b1_original_eligible.iloc[0]), "common_complete": bool(group.common_complete.iloc[0])})
    expected_joint = pd.DataFrame(independent_joint_rows).set_index(CORE_KEY + ["model"])
    for row in joint.itertuples(index=False):
        expected = expected_joint.loc[(row.issue_date, row.shop, row.spu, row.model)]
        if row.joint_decision != expected.joint_decision or row.b1_original_eligible != expected.b1_original_eligible or row.common_complete != expected.common_complete:
            raise AssertionError("joint decision or population differs")
        _same_number(row.actual60, expected.actual60, "joint actual60", 0.0)
    paired = verify_pairs(effects, reconstructed, expected_joint, slice_fields, output)
    reconstructed.to_csv(output / "independent_scores.csv", index=False, encoding="utf-8-sig")
    return {"score_rows": len(scores), "summary_rows": len(summaries), "joint_rows": len(joint),
            "original_b1_eligible": len(eligible), "original_gap": 385 - len(eligible),
            "common_complete_records": int(complete.sum()), "formal_fit_count": formal_count,
            "max_truth_difference": max_truth, "max_prediction_difference": max_prediction,
            "max_summary_difference": maximum_metric, "checkpoints": checkpoints, "paired": paired,
            "effect_hashes": effect_hashes}


def verify_pairs(effects, scores, joint, slice_fields, output):
    paired = pd.read_csv(effects / "paired_details.csv")
    migrations = pd.read_csv(effects / "joint_migrations.csv")
    summary = pd.read_csv(effects / "paired_summary.csv")
    score_index = scores.set_index(CORE_KEY + ["window", "model"])
    expected_pair_keys, expected_migration_keys = set(), set()
    for scope, table in score_scopes(scores).items():
        expected_pair_keys.update((scope, *key) for key in table[CORE_KEY + ["window"]].drop_duplicates().itertuples(index=False, name=None))
        expected_migration_keys.update((scope, *key) for key in table[CORE_KEY].drop_duplicates().itertuples(index=False, name=None))
    assert_key_grid(paired, ["scope"] + CORE_KEY + ["window"], expected_pair_keys, "paired score rows")
    assert_key_grid(migrations, ["scope"] + CORE_KEY, expected_migration_keys, "joint migration rows")
    window_migrations = []
    for row in paired.itertuples(index=False):
        key = (row.issue_date, row.shop, row.spu, row.window)
        a, b = score_index.loc[(*key, "F28")], score_index.loc[(*key, "DIRECT30x2")]
        change = b.absolute_error - a.absolute_error
        expected = "UNJUDGED" if not np.isfinite(change) else "IMPROVED" if change < 0 else "DEGRADED" if change > 0 else "UNCHANGED"
        if row.change != expected:
            raise AssertionError("paired improvement classification differs")
        window_migrations.append({"scope": row.scope, **dict(zip(CORE_KEY + ["window"], key)),
            "decision_f28": a.decision, "decision_direct": b.decision, "actual": a.actual,
            "migration": independent_migration(a.decision, b.decision)})
        _same_number(row.absolute_error_change, change, "paired error change", 1e-8)
        for suffix, source in [("f28", a), ("direct", b)]:
            for field in ["actual", "prediction", "absolute_error"]:
                _same_number(getattr(row, field + "_" + suffix), source[field], "paired inherited value", 1e-8)
    for row in migrations.itertuples(index=False):
        key = (row.issue_date, row.shop, row.spu)
        a, b = joint.loc[(*key, "F28")], joint.loc[(*key, "DIRECT30x2")]
        if row.joint_decision_f28 != a.joint_decision or row.joint_decision_direct != b.joint_decision or row.migration != independent_migration(a.joint_decision, b.joint_decision):
            raise AssertionError("joint-hit migration differs")
        _same_number(row.actual60_f28, a.actual60, "migration actual60 F28", 0.0)
        _same_number(row.actual60_direct, b.actual60, "migration actual60 direct", 0.0)
    maximum = 0.0
    expected_summary = set()
    for scope, group in paired.groupby("scope"):
        for slice_name, fields in slice_fields.items():
            if slice_name not in set(summary.slice):
                continue
            for values, part in group.groupby(["window"] + [field + "_f28" if field not in CORE_KEY else field for field in fields]):
                expected_summary.add((scope, slice_name, *values))
    actual_summary = set()
    for row in summary.itertuples(index=False):
        fields = slice_fields[row.slice]
        key = (row.scope, row.slice, row.window, *(getattr(row, field) for field in fields))
        if key in actual_summary:
            raise AssertionError("duplicate paired summary")
        actual_summary.add(key)
        group = paired[paired.scope.eq(row.scope) & paired.window.eq(row.window)]
        for field in fields:
            column = field if field in CORE_KEY else field + "_f28"
            group = group[group[column].eq(getattr(row, field))]
        both = group[group.absolute_error_f28.notna() & group.absolute_error_direct.notna()]
        denom = float(both.actual_f28.sum())
        change = both.absolute_error_direct - both.absolute_error_f28
        expected = {"fixed_n": len(group), "paired_n": len(both), "unjudged_n": len(group) - len(both),
                    "improved_n": int(change.lt(0).sum()), "degraded_n": int(change.gt(0).sum()), "unchanged_n": int(change.eq(0).sum()),
                    "improved_sales_share": float(both.loc[change.lt(0), "actual_f28"].sum() / denom) if denom else np.nan,
                    "degraded_sales_share": float(both.loc[change.gt(0), "actual_f28"].sum() / denom) if denom else np.nan,
                    "wape_change": float(change.sum() / denom) if denom else np.nan}
        for field, value in expected.items():
            maximum = max(maximum, _same_number(getattr(row, field), value, "paired summary " + field, 1e-8))
    if expected_summary != actual_summary:
        raise AssertionError("paired summary slices incomplete")
    pd.DataFrame(window_migrations).to_csv(output / "independent_window_hit_migrations.csv", index=False, encoding="utf-8-sig")
    return {"paired_rows": len(paired), "migration_rows": len(migrations), "summary_rows": len(summary), "max_difference": maximum}


def reuse_prepared_evidence(evidence_path, root, old_hashes, v2_hashes):
    """Reuse exhaustive prior arithmetic only for its exact source/table bytes."""
    evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8-sig"))
    if evidence["status"] != "PASS_INDEPENDENT_PREPARED_RECONCILIATION" or evidence["v1_hashes"] != old_hashes or evidence["v2_hashes"] != v2_hashes:
        raise AssertionError("prepared evidence is not an applicable independent PASS")
    for relative, expected in evidence["prepared_hashes"].items():
        if sha256(root / relative) != expected:
            raise AssertionError("previously verified training/inference table changed")
    manifest = json.loads((root / "prepared/prepare_manifest.json").read_text(encoding="utf-8-sig"))
    for relative, expected in manifest["files"].items():
        if sha256(root / "prepared" / relative) != expected:
            raise AssertionError("prepared artifact manifest no longer matches")
    if sha256(root / "prepare_source.py") != manifest["script_sha256"]:
        raise AssertionError("preserved prepare implementation changed")
    return evidence["training"], evidence["prepared_hashes"]


def validate(archive, v2_root, root, output=None, prepared_only=False, prepared_evidence=None):
    archive, v2_root, root = Path(archive).resolve(), Path(v2_root).resolve(), Path(root).resolve()
    output = Path(output).resolve() if output else root / ("validation_prepared" if prepared_only else "validation")
    output.mkdir(parents=True, exist_ok=False)
    report = {"status": "RUNNING", "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "method": "independent source calendars and closed-month qualification; no training-helper imports",
              "validator_sha256": sha256(Path(__file__))}
    try:
        old_hashes, v2_hashes = verify_old_hashes(archive), verify_v2_hashes(v2_root)
        daily = pd.read_parquet(archive / "inputs_v1_final/daily.parquet")
        monthly = pd.read_parquet(archive / "inputs_v1_final/monthly_reference.parquet")
        core = pd.read_csv(archive / "effects_v1/core_lists.csv")
        if len(core) != 385 or len(core[["shop", "spu"]].drop_duplicates()) != 174:
            raise AssertionError("original 385/174 cohort changed")
        calendar, qualification = DirectCalendar(daily), Qualification(monthly)
        if prepared_evidence:
            training, artifacts = reuse_prepared_evidence(prepared_evidence, root, old_hashes, v2_hashes)
            report["prepared_evidence"] = {"path": str(Path(prepared_evidence).resolve()), "sha256": sha256(Path(prepared_evidence))}
        else:
            training, funnels, objects, artifacts = verify_prepared(root, calendar, qualification, core)
            funnels.to_csv(output / "independent_training_exclusions.csv", index=False, encoding="utf-8-sig")
            objects.to_csv(output / "independent_per_object_windows.csv", index=False, encoding="utf-8-sig")
            pd.DataFrame(training).to_csv(output / "independent_training_coverage.csv", index=False, encoding="utf-8-sig")
        if not prepared_only:
            report["effects"] = verify_effects(archive, v2_root, root, calendar, qualification, core, output)
        if old_hashes != verify_old_hashes(archive) or v2_hashes != verify_v2_hashes(v2_root):
            raise AssertionError("prior source/output hashes changed while verifying")
        report.update(status="PASS_INDEPENDENT_PREPARED_RECONCILIATION" if prepared_only else "PASS_INDEPENDENT_V3_RECONCILIATION", fixed_core_records=385, distinct_shop_spu=174,
                      complete_safe_context_objects=len(calendar.context), training=training, prepared_hashes=artifacts,
                      prepared_manifest_sha256=sha256(root / "prepared/prepare_manifest.json"), v1_hashes=old_hashes, v2_hashes=v2_hashes)
        print(json.dumps(report, ensure_ascii=False, indent=2, default=int))
        return report
    except Exception as error:
        report.update(status="FAILED", error=str(error), traceback=traceback.format_exc())
        raise
    finally:
        report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        (output / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=int, allow_nan=False), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--v2-root", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--output")
    parser.add_argument("--prepared-only", action="store_true")
    parser.add_argument("--prepared-evidence", help="Reuse exact-byte prior independent training checks")
    arguments = parser.parse_args()
    validate(arguments.archive, arguments.v2_root, arguments.root, arguments.output, arguments.prepared_only, arguments.prepared_evidence)


if __name__ == "__main__":
    main()
