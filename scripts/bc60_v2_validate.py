"""Independent offline verifier for BC60 V2; never imports experiment arithmetic.

Reads the frozen V1 archive and a new V2 output directory.  Every target sum is
reconstructed from calendar dates, and every rolling estimate is reconstructed
from its own information cutoff.  Verification results are append-only by run:
the destination must not already exist, including after a failed verification.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from functools import lru_cache
from pathlib import Path
import traceback

import numpy as np
import pandas as pd


KNOWN_DAILY_SHA256 = "ff69063d3c4e506b2b2b8c701db6afa95b4f8af7ce7af3b7c17a898e70bfc7da"
KNOWN_CORE_SHA256 = "f634f9e1ee857be71db2e4f31e7136a4d7f6ff08761fb227edea972cc71f32cf"
WINDOWS = {"d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
VALID_SALES_STATES = {"OBSERVED", "EXPLICIT_ZERO"}
KEYS = ["original_issue_date", "shop", "spu"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@lru_cache(maxsize=4096)
def day(value) -> dt.date:
    return pd.Timestamp(value).date()


def information_dates(issue, update, model):
    """Calendar arithmetic independent of the implementation under test."""
    issue, update = day(issue), day(update)
    offset = (update - issue).days
    if offset < 0:
        raise ValueError("update precedes the original issue")
    if model == "F28":
        estimate = issue
    elif model == "R28-D":
        estimate = update
    elif model == "R28-W":
        estimate = issue + dt.timedelta(days=offset // 7 * 7)
    else:
        raise ValueError(model)
    return estimate, estimate - dt.timedelta(days=2)


def target_dates(issue, update, track, window):
    issue, update = day(issue), day(update)
    if track == "T1":
        start, end = update + dt.timedelta(days=1), issue + dt.timedelta(days=60)
    else:
        if track not in {"T0", "T2"}:
            raise ValueError(track)
        first, last = WINDOWS[window]
        origin = issue if track == "T0" else update
        start, end = origin + dt.timedelta(days=first), origin + dt.timedelta(days=last)
    if end < start:
        raise ValueError("empty target window")
    return start, end


class CalendarTruth:
    """Exact calendar-day sums with a separate count of valid observations.

    Zero-filled *temporary summation arrays* never authorize an absent day: a
    result is returned only when every requested calendar date is valid.
    """

    def __init__(self, daily: pd.DataFrame):
        d = daily.copy()
        d["ds"] = pd.to_datetime(d["ds"]).dt.normalize()
        if d.duplicated(["shop", "spu", "ds"]).any():
            raise ValueError("duplicate shop/SPU/calendar date in truth")
        self.start = day(d.ds.min())
        self.end = day(d.ds.max())
        self.length = (self.end - self.start).days + 1
        self.data = {}
        self.raw = {}
        for pair, group in d.groupby(["shop", "spu"], sort=False):
            values = np.zeros(self.length, dtype=float)
            valid = np.zeros(self.length, dtype=np.int64)
            positions = (group.ds - pd.Timestamp(self.start)).dt.days.to_numpy()
            okay = group.y.notna() & np.isfinite(group.y.astype(float)) & group.y.ge(0)
            if "sales_state" in group:
                okay &= group.sales_state.isin(VALID_SALES_STATES)
            values[positions[okay.to_numpy()]] = group.loc[okay, "y"].to_numpy(dtype=float)
            valid[positions[okay.to_numpy()]] = 1
            self.data[pair] = (np.r_[0.0, np.cumsum(values)], np.r_[0, np.cumsum(valid)])
            raw_values = np.full(self.length, np.nan)
            raw_states = np.full(self.length, "MISSING", dtype=object)
            raw_values[positions] = group.y.to_numpy(dtype=float)
            raw_states[positions] = group.sales_state.to_numpy() if "sales_state" in group else "OBSERVED"
            self.raw[pair] = (raw_values, raw_states)

    def total(self, shop, spu, start, end):
        start, end = day(start), day(end)
        if end < start:
            raise ValueError("empty calendar interval")
        if end > self.end:
            return np.nan, "NOT_MATURED"
        if start < self.start or (shop, spu) not in self.data:
            return np.nan, "MISSING_TRUTH"
        left, right = (start - self.start).days, (end - self.start).days + 1
        sums, counts = self.data[(shop, spu)]
        if counts[right] - counts[left] != right - left:
            return np.nan, "MISSING_TRUTH"
        return float(sums[right] - sums[left]), "OBSERVED"

    def mean28(self, shop, spu, cutoff):
        cutoff = day(cutoff)
        value, status = self.total(shop, spu, cutoff - dt.timedelta(days=27), cutoff)
        return value / 28.0 if np.isfinite(value) else np.nan, status

    def history_hash(self, shop, spu, cutoff, source_sha256):
        cutoff = day(cutoff)
        start = cutoff - dt.timedelta(days=27)
        records = []
        raw_values, raw_states = self.raw.get((shop, spu), (None, None))
        for offset in range(28):
            date = start + dt.timedelta(days=offset)
            position = (date - self.start).days
            value, state = None, "MISSING"
            if raw_values is not None and 0 <= position < self.length:
                value = float(raw_values[position]) if np.isfinite(raw_values[position]) else None
                state = str(raw_states[position])
            records.append([str(date), value, state])
        document = {"version": "bc60-v2-history-v1-canonical-json", "source_sha256": source_sha256,
                    "shop": shop, "spu": spu, "history_start": str(start), "history_end": str(cutoff), "days": records}
        serialised = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def independent_metric(frame: pd.DataFrame) -> dict:
    """Frozen-list counts use all rows; WAPE and bias use scored truth only."""
    actual = pd.to_numeric(frame.actual, errors="coerce")
    prediction = pd.to_numeric(frame.prediction, errors="coerce")
    observed = actual.notna() & np.isfinite(actual)
    scored = observed & prediction.notna() & np.isfinite(prediction)
    positive = observed & actual.gt(0)
    error = prediction - actual
    hit = scored & positive & error.abs().le(actual * 0.2)
    missed = scored & positive & ~hit
    scored_units = float(actual[scored].sum())
    observed_units = float(actual[observed].sum())
    zero = scored & actual.eq(0)
    return {
        "fixed_core_n": len(frame), "output_n": int(prediction.notna().sum()),
        "truth_n": int(observed.sum()), "scored_n": int(scored.sum()), "positive_truth_n": int(positive.sum()),
        "hit_n": int(hit.sum()), "miss_n": int(missed.sum()),
        "unjudged_n": int((~(hit | missed)).sum()),
        "hit_rate": float(hit.sum() / positive.sum()) if positive.any() else np.nan,
        "full_core_hit_share": float(hit.sum() / len(frame)) if len(frame) else np.nan,
        "hit_sales_share": float(actual[hit].sum() / observed_units) if observed_units > 0 else np.nan,
        "wape": float(error[scored].abs().sum() / scored_units) if scored_units > 0 else np.nan,
        "bias": float(error[scored].sum() / scored_units) if scored_units > 0 else np.nan,
        "over_units": float(error[scored].clip(lower=0).sum()),
        "under_units": float((-error[scored]).clip(lower=0).sum()),
        "actual_units": scored_units, "known_fixed_core_actual_units": observed_units,
        "forecast_units": float(prediction[scored].sum()),
        "zero_actual_n": int((observed & actual.eq(0)).sum()),
        "zero_actual_absolute_error": float(error[zero].abs().sum()),
        "not_matured_n": int(frame.truth_status.eq("NOT_MATURED").sum()) if "truth_status" in frame else 0,
        "unknown_truth_n": int((~observed & ~frame.truth_status.eq("NOT_MATURED")).sum()) if "truth_status" in frame else int((~observed).sum()),
        "missing_output_n": int(prediction.isna().sum()),
    }


def verify_old_hashes(archive: Path) -> dict:
    manifest = json.loads((archive / "effects_v1/run_manifest.json").read_text(encoding="utf-8-sig"))
    paths = {archive / "inputs_v1_final" / name: value for name, value in manifest["inputs"].items()}
    paths.update({archive / "effects_v1" / name: value for name, value in manifest["files"].items()})
    paths[archive / "inputs_v1_final/daily.parquet"] = KNOWN_DAILY_SHA256
    paths[archive / "effects_v1/core_lists.csv"] = KNOWN_CORE_SHA256
    checked = {}
    for path, expected in paths.items():
        actual = sha256(path)
        if actual != expected:
            raise AssertionError(f"original archive hash mismatch: {path.name}")
        checked[str(path.relative_to(archive))] = actual
    return checked


def _same_number(actual, expected, label, tolerance=1e-7):
    if pd.isna(expected):
        if not pd.isna(actual):
            raise AssertionError(f"{label}: expected missing, got {actual}")
        return 0.0
    if pd.isna(actual) or not np.isfinite(actual):
        raise AssertionError(f"{label}: expected finite {expected}, got {actual}")
    difference = abs(float(actual) - float(expected))
    if difference > tolerance:
        raise AssertionError(f"{label}: {actual} != {expected} (difference {difference})")
    return difference


def independent_native_mean(values, dtype):
    """Coreforecast's native rolling accumulator rounds after every addition."""
    dtype = np.dtype(dtype)
    accumulator = dtype.type(0)
    for value in values:
        accumulator = dtype.type(accumulator + dtype.type(value))
    return dtype.type(accumulator / dtype.type(len(values)))


def _normalise_dates(frame):
    for name in ["original_issue_date", "update_date", "input_cutoff", "estimate_issue_date", "history_start", "history_end", "target_start", "target_end"]:
        if name in frame:
            frame[name] = pd.to_datetime(frame[name]).dt.strftime("%Y-%m-%d")
    return frame


def _preserved_core(frame, core, grouping, label):
    frozen = set(core[KEYS].itertuples(index=False, name=None))
    for group, subset in frame.groupby(grouping, dropna=False, sort=False):
        observed = set(subset[KEYS].itertuples(index=False, name=None))
        if len(subset) != len(frozen) or observed != frozen:
            raise AssertionError(f"{label} changed frozen cohort at {group}: {len(subset)}")


def _independent_summaries(scores):
    rows = []
    dimensions = ["track", "update_offset", "window", "model"]
    scopes = [("full_core", scores), ("original_b1_369", scores[scores.b1_original_eligible]),
              ("t2_balanced_279", scores[scores.track.eq("T2") & scores.original_issue_date.isin(["2026-04-02", "2026-06-02"])])]
    for scope, subset in scopes:
        for label, extra in [("overall", []), ("shop", ["shop"]), ("issue", ["original_issue_date"]),
                             ("lifecycle", ["lifecycle"]), ("maturity", ["maturity"])]:
            keys = dimensions + extra
            for group, part in subset.groupby(keys, dropna=False, sort=False):
                rows.append(dict(zip(keys, group)) | {"scope": scope, "slice": label} | independent_metric(part))
    return pd.DataFrame(rows)


def verify_daily_vintages(folder, rates):
    """Verify every explicit daily forecast against its issued calendar window."""
    key = ["original_issue_date", "update_date", "shop", "spu", "model"]
    expected_rows = 385 * (sum(range(1, 61)) + 2 * 60 * 60)
    total_rows, files, maximum = 0, 0, 0.0
    for path in sorted((folder / "daily_vintages").glob("*.parquet")):
        frame = _normalise_dates(pd.read_parquet(path))
        if frame.duplicated(key + ["target_date"]).any():
            raise AssertionError("duplicate daily vintage date")
        merged = frame.merge(rates[key + ["rate", "input_hash", "input_cutoff"]], on=key,
                             how="left", validate="many_to_one", indicator=True, suffixes=("", "_issued"))
        if not merged._merge.eq("both").all():
            raise AssertionError("daily vintage has no matching issue")
        if not merged.input_hash.eq(merged.input_hash_issued).all() or not merged.input_cutoff.eq(merged.input_cutoff_issued).all():
            raise AssertionError("daily vintage lost its information boundary")
        both_missing = merged.forecast.isna() & merged.rate.isna()
        difference = (merged.forecast - merged.rate).abs()
        if not (both_missing | difference.le(1e-8)).all():
            raise AssertionError("daily vintage does not equal its issued prediction")
        maximum = max(maximum, float(difference.max()))
        merged["target_date"] = pd.to_datetime(merged.target_date)
        checks = merged.groupby(key).target_date.agg(["min", "max", "size"]).reset_index()
        starts = pd.to_datetime(checks.update_date) + pd.Timedelta(days=1)
        ends = pd.to_datetime(checks.update_date) + pd.Timedelta(days=60)
        ends = ends.where(checks.model.ne("F28"), pd.to_datetime(checks.original_issue_date) + pd.Timedelta(days=60))
        if not (checks["min"].eq(starts) & checks["max"].eq(ends) & checks["size"].eq((ends - starts).dt.days + 1)).all():
            raise AssertionError("daily vintage is truncated, extended, or contains realized dates")
        total_rows += len(frame)
        files += 1
    if files != 9 or total_rows != expected_rows:
        raise AssertionError("missing or duplicated explicit daily vintage file")
    return {"files": files, "rows": total_rows, "max_rate_difference": maximum}


def verify_pairs(folder, scores, core, output):
    detail = pd.read_parquet(folder / "paired_details.parquet")
    keys = ["track", "original_issue_date", "update_date", "shop", "spu", "window"]
    if len(detail) != 138600 or detail.duplicated(keys + ["comparison"]).any():
        raise AssertionError("paired detail cardinality mismatch")
    _preserved_core(detail, core, ["track", "update_offset", "window", "comparison"], "paired details")
    before_after = detail.comparison.str.split("->", expand=True)
    checked = detail.copy()
    for side, number in [("before", 0), ("after", 1)]:
        checked[f"model_{side}"] = before_after[number]
        expected = scores[keys + ["model", "actual", "prediction"]].rename(columns={
            "model": f"model_{side}", "actual": f"expected_actual_{side}", "prediction": f"expected_prediction_{side}"})
        checked = checked.merge(expected, on=keys + [f"model_{side}"], how="left", validate="many_to_one", indicator=True)
        if not checked._merge.eq("both").all():
            raise AssertionError("paired detail has no matching score")
        checked = checked.drop(columns="_merge")
        for name in ["actual", "prediction"]:
            original = checked[f"{name}_{side}"]
            expected_values = checked[f"expected_{name}_{side}"]
            if not ((original.isna() & expected_values.isna()) | (original - expected_values).abs().le(1e-8)).all():
                raise AssertionError("paired values changed the score/denominator")
    before_error = checked.expected_prediction_before - checked.expected_actual_before
    after_error = checked.expected_prediction_after - checked.expected_actual_after
    delta = after_error.abs() - before_error.abs()
    if not ((detail.delta_absolute_error.isna() & delta.isna()) | (detail.delta_absolute_error - delta).abs().le(1e-8)).all():
        raise AssertionError("paired absolute-error delta mismatch")
    expected_change = np.where(delta.isna(), "UNJUDGED", np.where(delta < -1e-8, "IMPROVED", np.where(delta > 1e-8, "DEGRADED", "UNCHANGED")))
    if not detail.change.eq(expected_change).all():
        raise AssertionError("paired improvement/degradation classification mismatch")
    checked["error_before"] = before_error
    checked["error_after"] = after_error
    checked["actual"] = checked.expected_actual_before
    checked["delta_absolute_error"] = delta
    checked["change"] = expected_change
    rows = []
    scopes = [("full_core", checked), ("original_b1_369", checked[checked.b1_original_eligible]),
              ("t2_balanced_279", checked[checked.track.eq("T2") & checked.original_issue_date.isin(["2026-04-02", "2026-06-02"])])]
    for scope, subset in scopes:
        for label, extra in [("overall", []), ("shop", ["shop"]), ("issue", ["original_issue_date"]),
                             ("lifecycle", ["lifecycle"]), ("maturity", ["maturity"])]:
            dimensions = ["track", "update_offset", "window", "comparison"] + extra
            for group, part in subset.groupby(dimensions, dropna=False, sort=False):
                z = part[part.change.ne("UNJUDGED")]
                total = float(z.actual.sum())
                improve, degrade = z.change.eq("IMPROVED"), z.change.eq("DEGRADED")
                hit_before = z.actual.gt(0) & z.error_before.abs().le(z.actual * .2)
                hit_after = z.actual.gt(0) & z.error_after.abs().le(z.actual * .2)
                rows.append(dict(zip(dimensions, group)) | {"scope": scope, "slice": label,
                    "fixed_core_n": len(part), "paired_n": len(z), "unjudged_n": len(part) - len(z),
                    "paired_actual_units": total, "delta_wape": float(z.delta_absolute_error.sum() / total) if total > 0 else np.nan,
                    "delta_over_units": float(z.error_after.clip(lower=0).sum() - z.error_before.clip(lower=0).sum()),
                    "delta_under_units": float((-z.error_after).clip(lower=0).sum() - (-z.error_before).clip(lower=0).sum()),
                    "improved_n": int(improve.sum()), "degraded_n": int(degrade.sum()),
                    "unchanged_n": int(z.change.eq("UNCHANGED").sum()),
                    "improved_sales_weight": float(z.loc[improve, "actual"].sum() / total) if total > 0 else np.nan,
                    "degraded_sales_weight": float(z.loc[degrade, "actual"].sum() / total) if total > 0 else np.nan,
                    "delta_hit_n": int(hit_after.sum() - hit_before.sum())})
    independent = pd.DataFrame(rows)
    independent.to_csv(output / "independent_paired_summary.csv", index=False, encoding="utf-8-sig")
    index = ["track", "update_offset", "window", "comparison", "scope", "slice", "shop", "original_issue_date", "lifecycle", "maturity"]
    ours = independent.copy()
    ours[index] = ours[index].fillna("__ALL__")
    ours = ours.set_index(index)
    reported = pd.read_csv(folder / "paired_summary.csv")
    reported[index] = reported[index].fillna("__ALL__")
    if len(reported) != len(independent) or reported.duplicated(index).any():
        raise AssertionError("paired summary omitted or duplicated scope/slice cells")
    maximum = 0.0
    for row in reported.itertuples(index=False):
        expected = ours.loc[tuple(getattr(row, key) for key in index)]
        for field in ours.columns:
            maximum = max(maximum, _same_number(getattr(row, field), expected[field], f"paired summary {field}"))
    return {"detail_rows": len(detail), "summary_rows": len(reported), "max_difference": maximum}


def verify_joint_and_revisions(folder, scores, rates):
    selected = scores[scores.track.isin(["T0", "T2"])].copy()
    selected["independent_hit"] = selected.actual.gt(0) & (selected.prediction - selected.actual).abs().le(selected.actual * .2)
    selected["independent_known"] = selected.actual.gt(0) & selected.prediction.notna()
    keys = ["track", "original_issue_date", "update_date", "update_offset", "shop", "spu", "model"]
    expected = selected.groupby(keys).agg(hit=("independent_hit", "all"), known=("independent_known", "all"), windows=("window", "nunique"))
    if not expected.windows.eq(3).all():
        raise AssertionError("joint hit lacks all three windows")
    reported = pd.read_csv(folder / "joint_hits.csv")
    if len(reported) != len(expected) or reported.duplicated(keys).any():
        raise AssertionError("joint result cardinality mismatch")
    for row in reported.itertuples(index=False):
        value = expected.loc[tuple(getattr(row, name) for name in keys)]
        if bool(row.all_three_hit) != bool(value.hit) or row.joint_state != ("KNOWN" if value.known else "UNJUDGED"):
            raise AssertionError("joint hit incorrectly handles missingness or a failed window")
    revision_keys = ["original_issue_date", "shop", "spu", "model"]
    revisions = pd.read_csv(folder / "revision_summary.csv")
    if len(revisions) != 385 * 3 or revisions.duplicated(revision_keys).any():
        raise AssertionError("revision summary cardinality mismatch")
    revisions = revisions.set_index(revision_keys)
    maximum = 0.0
    for group_key, group in rates.groupby(revision_keys, sort=False):
        values = group.sort_values("update_date").rate.to_numpy()
        differences = [float(current - previous) for previous, current in zip(values[:-1], values[1:]) if np.isfinite(previous) and np.isfinite(current)]
        active = [difference for difference in differences if abs(difference) > 1e-8]
        signs = [1 if value > 0 else -1 for value in active]
        expected = {"known_updates": int(np.isfinite(values).sum()), "nonzero_revisions": len(active),
                    "direction_reversals": sum(left != right for left, right in zip(signs[:-1], signs[1:])),
                    "rate_total_absolute_revision": sum(abs(value) for value in differences),
                    "standardized60_absolute_revision": 60 * sum(abs(value) for value in differences),
                    "max_standardized60_revision": 60 * max((abs(value) for value in differences), default=np.nan)}
        reported_values = revisions.loc[group_key]
        for field, value in expected.items():
            maximum = max(maximum, _same_number(reported_values[field], value, f"revision {field}"))
    return {"joint_rows": len(reported), "revision_rows": len(revisions), "max_revision_difference": maximum}


def verify_b1_diagnostic(folder, archive, truth, core, b1_keys):
    if not (folder / "diagnostic_manifest.json").exists():
        return {"status": "NOT_PRESENT"}
    manifest = json.loads((folder / "diagnostic_manifest.json").read_text(encoding="utf-8-sig"))
    if manifest["status"] != "COMPLETE":
        return {"status": "PARTIAL_NOT_REPORTED_AS_VALIDATED", "diagnostic_status": manifest["status"], "errors": manifest.get("errors", [])}
    daily = pd.read_parquet(folder / "daily_d0_d1.parquet")
    daily["ds"] = pd.to_datetime(daily.ds).dt.normalize()
    frozen = set(core[KEYS].itertuples(index=False, name=None))
    if len(daily) != 385 * 62 * 2 or daily.duplicated(["issue_date", "shop", "spu", "model", "ds"]).any():
        raise AssertionError("B1 diagnostic daily cardinality mismatch")
    predictions = {}
    max_actual_difference = max_original_difference = max_stage_difference = 0.0
    archived = pd.read_parquet(archive / "effects_v1/daily_forecasts.parquet")
    archived = archived[archived.model.eq("B1")]
    old = {(r.issue_date, r.shop, r.spu, day(r.ds)): r.forecast for r in archived.itertuples(index=False)}
    for (model, relative), group in daily.groupby(["model", "relative_day"]):
        if set(group[["issue_date", "shop", "spu"]].itertuples(index=False, name=None)) != frozen:
            raise AssertionError("B1 diagnostic changed original fixed core")
    for row in daily.itertuples(index=False):
        if row.model not in {"D0", "D1"} or row.relative_day not in range(-1, 61):
            raise AssertionError("B1 diagnostic model/relative day invalid")
        issue, target = day(row.issue_date), day(row.ds)
        if target != issue + dt.timedelta(days=row.relative_day) or day(row.information_cutoff) != issue - dt.timedelta(days=2):
            raise AssertionError("B1 diagnostic calendar boundary changed")
        key = (row.issue_date, row.shop, row.spu)
        if bool(row.original_b1_available) != (key in b1_keys):
            raise AssertionError("B1 diagnostic original eligibility changed")
        if row.source_daily_sha256 != KNOWN_DAILY_SHA256:
            raise AssertionError("B1 diagnostic source hash changed")
        actual, _ = truth.total(row.shop, row.spu, target, target)
        max_actual_difference = max(max_actual_difference, _same_number(row.actual, actual, "B1 diagnostic daily truth"))
        if row.model == "D0":
            max_original_difference = max(max_original_difference, _same_number(row.forecast, old.get((*key, target), np.nan), "D0 vs original daily", 1e-8))
        if key not in b1_keys and pd.notna(row.forecast):
            raise AssertionError("B1 diagnostic backfilled an original unavailable object")
        predictions[(*key, row.model, target)] = row.forecast
    first = daily[daily.relative_day.eq(-1)].pivot(index=["issue_date", "shop", "spu"], columns="model", values="forecast")
    first_difference = float((first.D0 - first.D1).abs().max())
    if first_difference > 1e-8 or not first.D0.isna().eq(first.D1.isna()).all():
        raise AssertionError("D1 first bridge step differs from D0")
    stages = {"bridge": (-1, 0), "d01_07": (1, 7), "d08_14": (8, 14), "d15_30": (15, 30),
              "d31_45": (31, 45), "d46_60": (46, 60), "d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
    scores = pd.read_csv(folder / "stage_scores.csv")
    if len(scores) != 385 * 2 * len(stages) or scores.duplicated(["issue_date", "shop", "spu", "model", "stage"]).any():
        raise AssertionError("B1 stage score cardinality mismatch")
    for row in scores.itertuples(index=False):
        first_day, last_day = stages[row.stage]
        start, end = day(row.issue_date) + dt.timedelta(days=first_day), day(row.issue_date) + dt.timedelta(days=last_day)
        if day(row.target_start) != start or day(row.target_end) != end or row.days != last_day - first_day + 1:
            raise AssertionError("B1 stage has wrong target dates")
        actual, _ = truth.total(row.shop, row.spu, start, end)
        values = [predictions[(row.issue_date, row.shop, row.spu, row.model, start + dt.timedelta(days=i))] for i in range(row.days)]
        prediction = float(np.sum(values)) if np.isfinite(values).all() else np.nan
        max_stage_difference = max(max_stage_difference, _same_number(row.actual, actual, "B1 stage truth"),
                                   _same_number(row.prediction, prediction, "B1 stage prediction"))
    summaries = pd.read_csv(folder / "stage_summary.csv")
    max_summary_difference = 0.0
    for row in summaries.itertuples(index=False):
        part = scores[scores.model.eq(row.model) & scores.stage.eq(row.stage)]
        if row.scope == "SECONDARY_ORIGINAL_369_B1_OUTPUTS":
            part = part[part.original_b1_available]
        elif row.scope != "FULL_FROZEN_CORE":
            raise AssertionError("B1 stage summary silently changed scope")
        if row.slice != "overall":
            field = "issue_date" if row.slice == "issue" else row.slice
            part = part[part[field].eq(getattr(row, field))]
        expected = independent_metric(part)
        shared = ["fixed_core_n", "output_n", "truth_n", "scored_n", "positive_truth_n", "zero_actual_n", "hit_n", "hit_rate", "hit_sales_share",
                  "known_fixed_core_actual_units", "actual_units", "forecast_units", "wape", "bias", "over_units", "under_units", "zero_actual_absolute_error"]
        for field in shared:
            max_summary_difference = max(max_summary_difference, _same_number(getattr(row, field), expected[field], f"B1 stage summary {field}"))
        for field, target in [("hit_count", "hit_n"), ("miss_count", "miss_n"), ("unjudged_count", "unjudged_n"),
                              ("missing_output_count", "missing_output_n"), ("fixed_core_hit_share", "full_core_hit_share")]:
            max_summary_difference = max(max_summary_difference, _same_number(getattr(row, field), expected[target], f"B1 stage summary {field}"))
    archived_stages = pd.read_csv(folder / "archived_stage_scores.csv")
    stage_keys = ["issue_date", "shop", "spu", "stage"]
    if len(archived_stages) != 385 * len(stages) or set(archived_stages.model) != {"B1_ARCHIVED"}:
        raise AssertionError("original archived stage diagnostic is incomplete")
    reproduced_stages = scores[scores.model.eq("D0")][stage_keys + ["actual", "prediction"]]
    stage_comparison = archived_stages.merge(reproduced_stages, on=stage_keys, how="outer", validate="one_to_one", indicator=True, suffixes=("_archive", "_d0"))
    if not stage_comparison._merge.eq("both").all():
        raise AssertionError("archived stage diagnostic changed fixed cohort")
    for field in ["actual", "prediction"]:
        a, b = stage_comparison[f"{field}_archive"], stage_comparison[f"{field}_d0"]
        if not ((a.isna() & b.isna()) | (a - b).abs().le(1e-8)).all():
            raise AssertionError("archived stage diagnostic differs from independently validated D0")
    feature_rows, max_feature_difference = 0, 0.0
    feature_paths = sorted((folder / "features").glob("*.parquet"))
    if len(feature_paths) != 6:
        raise AssertionError("B1 diagnostic must preserve both feature paths for all three issues")
    if manifest["actual_model_fits"] > 3:
        raise AssertionError("B1 diagnostic exceeded the preregistered fit budget")
    for path in feature_paths:
        features = pd.read_parquet(path)
        for (issue, shop, spu, model), group in features.groupby(["issue_date", "shop", "spu", "model"], sort=False):
            group = group.sort_values("recursive_step")
            if group.recursive_step.tolist() != list(range(1, 63)):
                raise AssertionError("B1 feedback audit missing a recursive step")
            cutoff = day(issue) - dt.timedelta(days=2)
            history = [truth.total(shop, spu, cutoff - dt.timedelta(days=i), cutoff - dt.timedelta(days=i))[0] for i in range(27, -1, -1)]
            if not np.isfinite(history).all():
                raise AssertionError("B1 initial audit history contains unknown input")
            proxy = float(np.mean(history))
            native_dtype = group.lag1.dtype
            history = list(np.asarray(history, dtype=native_dtype))
            for row in group.itertuples(index=False):
                target = cutoff + dt.timedelta(days=row.recursive_step)
                if day(row.ds) != target or row.dayofweek != target.weekday() or row.month != target.month or row.dayofyear != target.timetuple().tm_yday:
                    raise AssertionError("B1 feature calendar changed")
                expected = {f"lag{lag}": history[-lag] for lag in [1, 7, 14, 28]}
                for width in [7, 28]:
                    name = f"rolling_mean_lag1_window_size{width}"
                    expected[name] = np.asarray(independent_native_mean(history[-width:], native_dtype), dtype=group[name].dtype).item()
                expected["cutoff_known28_mean"] = proxy
                expected["model_output_nonnegative"] = max(row.model_output_raw, 0.0)
                expected["recursive_feedback"] = proxy if model == "D1" else row.model_output_nonnegative
                if hasattr(row, "recursive_feedback_native"):
                    expected["recursive_feedback_native"] = np.asarray(expected["recursive_feedback"], dtype=native_dtype).item()
                for field, value in expected.items():
                    max_feature_difference = max(max_feature_difference, _same_number(getattr(row, field), value, f"B1 independent feature {field}", 1e-8))
                core_key = (issue, shop, spu)
                if core_key in b1_keys:
                    _same_number(predictions[(*core_key, model, target)], row.model_output_nonnegative, "D1 captured output used for scoring", 1e-8)
                history.append(np.asarray(expected["recursive_feedback"], dtype=native_dtype).item())
            feature_rows += len(group)
    return {"status": "PASS_INDEPENDENT_B1_RECONCILIATION", "daily_rows": len(daily), "stage_rows": len(scores),
            "feature_rows": feature_rows, "max_truth_difference": max_actual_difference,
            "max_d0_original_difference": max_original_difference, "max_first_step_difference": first_difference,
            "max_stage_difference": max_stage_difference, "max_feature_difference": max_feature_difference,
            "stage_summary_rows": len(summaries), "max_summary_difference": max_summary_difference,
            "archived_stage_rows": len(archived_stages), "recovery_model_fits": manifest["actual_model_fits"]}


def validate(archive, root, output=None, diagnostic=None, skip_diagnostic=False, only_diagnostic=False):
    archive, root = Path(archive).resolve(), Path(root).resolve()
    output = Path(output).resolve() if output else root / "validation"
    output.mkdir(parents=True, exist_ok=False)
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    report = {"started_at": started, "status": "RUNNING", "method": "independent calendar arithmetic; no rolling experiment helpers imported"}
    try:
        before_hashes = verify_old_hashes(archive)
        core = pd.read_csv(archive / "effects_v1/core_lists.csv").rename(columns={"issue_date": "original_issue_date"})
        original_scores = pd.read_parquet(archive / "effects_v1/scores.parquet")
        original_score_values = {(r.issue_date, r.shop, r.spu, r.model, r.window): (r.actual, r.prediction) for r in original_scores.itertuples(index=False)}
        forecasts = pd.read_parquet(archive / "effects_v1/daily_forecasts.parquet")
        truth = CalendarTruth(pd.read_parquet(archive / "inputs_v1_final/daily.parquet"))
        if len(core) != 385 or len(core[["shop", "spu"]].drop_duplicates()) != 174:
            raise AssertionError("original cohort is not 385 issue records / 174 shop-SPUs")
        frozen_classification = {tuple(getattr(row, name) for name in KEYS): (row.lifecycle, row.maturity) for row in core.itertuples(index=False)}
        b1 = original_scores[original_scores.model.eq("B1") & original_scores.window.eq("d01_60")]
        b1_keys = set(b1.loc[b1.prediction.notna(), ["issue_date", "shop", "spu"]].itertuples(index=False, name=None))
        if len(b1_keys) != 369:
            raise AssertionError("original B1 eligible set is not 369")
        diagnostic_folder = Path(diagnostic).resolve() if diagnostic else root / "a_diagnostic"
        if only_diagnostic:
            diagnostic_check = verify_b1_diagnostic(diagnostic_folder, archive, truth, core, b1_keys)
            if diagnostic_check["status"] != "PASS_INDEPENDENT_B1_RECONCILIATION":
                raise AssertionError(f"diagnostic is unavailable for verification: {diagnostic_check}")
            after_hashes = verify_old_hashes(archive)
            if before_hashes != after_hashes:
                raise AssertionError("old archive changed during B1 verification")
            report.update({"status": "PASS_INDEPENDENT_B1_RECONCILIATION", "b1_diagnostic": diagnostic_check,
                           "original_hashes": after_hashes, "diagnostic_directory": str(diagnostic_folder)})
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return report
        original_lookup = {}
        for key, group in forecasts.groupby(["issue_date", "shop", "spu", "model"], sort=False):
            original_lookup[key] = {day(record.ds): float(record.forecast) for record in group.itertuples()}

        def original_sum(issue, shop, spu, model, start, end):
            source_model = {"F28": "rolling28", "WEEKDAY4": "weekday4", "B1": "B1"}[model]
            daily = original_lookup.get((issue, shop, spu, source_model), {})
            values = [daily.get(day(start) + dt.timedelta(days=i), np.nan) for i in range((day(end) - day(start)).days + 1)]
            return float(np.sum(values)) if np.isfinite(values).all() else np.nan

        folder = root / "b_rolling"
        rate_path = folder / "predictions_daily_rates.parquet"
        rates = _normalise_dates(pd.read_parquet(rate_path))
        rate_keys = ["original_issue_date", "update_date", "shop", "spu", "model"]
        if rates.duplicated(rate_keys).any() or len(rates) != 385 * 60 * 3:
            raise AssertionError("daily-rate cardinality/uniqueness mismatch")
        rate_offsets = (pd.to_datetime(rates.update_date) - pd.to_datetime(rates.original_issue_date)).dt.days
        if set(rate_offsets) != set(range(60)) or set(rates.model) != {"F28", "R28-D", "R28-W"}:
            raise AssertionError("daily-rate schedule/models changed")
        _preserved_core(rates.assign(update_offset=rate_offsets), core, ["update_offset", "model"], "rates")
        max_rate_difference = 0.0
        rates_by_key = {}
        input_hashes = set()
        reconstructed_hashes = {}
        for row in rates.itertuples(index=False):
            estimate, cutoff = information_dates(row.original_issue_date, row.update_date, row.model)
            if day(row.input_cutoff) != cutoff or day(row.estimate_issue_date) != estimate:
                raise AssertionError(f"incorrect information cutoff: {row}")
            if day(row.history_start) != cutoff - dt.timedelta(days=27) or day(row.history_end) != cutoff:
                raise AssertionError("history is not a 28-calendar-day interval")
            expected_rate, state = truth.mean28(row.shop, row.spu, cutoff)
            max_rate_difference = max(max_rate_difference, _same_number(row.rate, expected_rate, "28-day rate"))
            if row.model == "F28":
                archived = original_sum(row.original_issue_date, row.shop, row.spu, "F28", day(row.original_issue_date) + dt.timedelta(days=1), day(row.original_issue_date) + dt.timedelta(days=60)) / 60.0
                _same_number(row.rate, archived, "original F28 preservation")
            key = tuple(getattr(row, field) for field in rate_keys)
            rates_by_key[key] = row
            if not isinstance(row.input_hash, str) or len(row.input_hash) != 64:
                raise AssertionError("missing/invalid input hash")
            if row.source_daily_sha256 != KNOWN_DAILY_SHA256:
                raise AssertionError("daily-rate source hash is not the frozen input")
            history_key = (row.shop, row.spu, cutoff)
            if history_key not in reconstructed_hashes:
                reconstructed_hashes[history_key] = truth.history_hash(row.shop, row.spu, cutoff, KNOWN_DAILY_SHA256)
            if row.input_hash != reconstructed_hashes[history_key]:
                raise AssertionError(f"input history hash mismatch: {history_key}")
            input_hashes.add(row.input_hash)

        score_frames = []
        max_truth_difference = max_prediction_difference = 0.0
        details = {}
        for track in ["T0", "T1", "T2"]:
            frame = _normalise_dates(pd.read_parquet(folder / f"{track.lower()}_scores.parquet"))
            expected_count = {"T0": 385 * 3 * 3, "T1": 385 * 60 * 3, "T2": 385 * 60 * 3 * 2}[track]
            score_keys = KEYS + ["update_date", "model", "window"]
            if len(frame) != expected_count or frame.duplicated(score_keys).any() or set(frame.track) != {track}:
                raise AssertionError(f"{track} cardinality/keys mismatch")
            expected_models = {"T0": {"F28", "WEEKDAY4", "B1"}, "T1": {"F28", "R28-D", "R28-W"}, "T2": {"R28-D", "R28-W"}}[track]
            if set(frame.model) != expected_models:
                raise AssertionError(f"{track} model set changed: {set(frame.model)}")
            _preserved_core(frame, core, ["update_offset", "model", "window"], track)
            if track != "T1" and set(frame.window) != set(WINDOWS):
                raise AssertionError(f"{track} window set changed")
            for row in frame.itertuples(index=False):
                offset = (day(row.update_date) - day(row.original_issue_date)).days
                if row.update_offset != offset or offset not in (range(1) if track == "T0" else range(60)):
                    raise AssertionError("wrong update offset")
                start, end = target_dates(row.original_issue_date, row.update_date, track, row.window)
                if day(row.target_start) != start or day(row.target_end) != end or row.window_days != (end - start).days + 1:
                    raise AssertionError("target includes the wrong calendar dates")
                expected_actual, truth_state = truth.total(row.shop, row.spu, start, end)
                max_truth_difference = max(max_truth_difference, _same_number(row.actual, expected_actual, "target actual"))
                if truth_state == "NOT_MATURED" and (row.truth_status != "NOT_MATURED" or row.status != "NOT_MATURED"):
                    raise AssertionError("future target was not marked NOT_MATURED")
                if truth_state == "OBSERVED" and row.truth_status != "OBSERVED":
                    raise AssertionError("known actual mislabeled as unavailable")
                if bool(row.b1_original_eligible) != ((row.original_issue_date, row.shop, row.spu) in b1_keys):
                    raise AssertionError("original B1 qualification changed")
                if (row.lifecycle, row.maturity) != frozen_classification[(row.original_issue_date, row.shop, row.spu)]:
                    raise AssertionError("original lifecycle or age classification changed")
                if track == "T0" or row.model == "F28":
                    expected_prediction = original_sum(row.original_issue_date, row.shop, row.spu, row.model, start, end)
                    expected_cutoff = day(row.original_issue_date) - dt.timedelta(days=2)
                    if track == "T0":
                        source_model = {"F28": "rolling28", "WEEKDAY4": "weekday4", "B1": "B1"}[row.model]
                        original_actual, original_prediction = original_score_values[(row.original_issue_date, row.shop, row.spu, source_model, row.window)]
                        _same_number(row.actual, original_actual, "T0 original actual exact preservation", 0.0)
                        _same_number(row.prediction, original_prediction, "T0 original prediction exact preservation", 0.0)
                else:
                    rate_row = rates_by_key[(row.original_issue_date, row.update_date, row.shop, row.spu, row.model)]
                    expected_prediction = rate_row.rate * row.window_days
                    expected_cutoff = information_dates(row.original_issue_date, row.update_date, row.model)[1]
                    if row.input_hash != rate_row.input_hash:
                        raise AssertionError("score input hash differs from its issued prediction")
                if day(row.input_cutoff) != expected_cutoff:
                    raise AssertionError("score cutoff does not match model information")
                max_prediction_difference = max(max_prediction_difference, _same_number(row.prediction, expected_prediction, "target prediction"))
                decision = "UNJUDGED"
                if pd.notna(expected_actual) and expected_actual > 0 and pd.notna(expected_prediction):
                    decision = "HIT" if abs(expected_prediction - expected_actual) <= expected_actual * .2 else "MISS"
                if row.decision != decision:
                    raise AssertionError(f"score decision differs from independently recomputed exact boundary: {row.original_issue_date} {row.shop} {row.spu} {track} {row.model} {row.window}")
            details[track] = {"rows": len(frame), "not_matured_n": int(frame.truth_status.eq("NOT_MATURED").sum()), "scored_n": int((frame.actual.notna() & frame.prediction.notna()).sum())}
            score_frames.append(frame)
        scores = pd.concat(score_frames, ignore_index=True)
        vintage_check = verify_daily_vintages(folder, rates)
        diagnostic_check = {"status": "NOT_REQUESTED_IN_ROLLING_ONLY_CHECK"} if skip_diagnostic else verify_b1_diagnostic(diagnostic_folder, archive, truth, core, b1_keys)
        paired_check = verify_pairs(folder, scores, core, output)
        joint_revision_check = verify_joint_and_revisions(folder, scores, rates)
        independent = _independent_summaries(scores)
        independent.to_csv(output / "independent_metrics.csv", index=False, encoding="utf-8-sig")
        metric_differences = {}
        metric_keys = ["track", "scope", "update_offset", "window", "model", "slice", "shop", "original_issue_date", "lifecycle", "maturity"]
        ours = independent.copy()
        ours[metric_keys] = ours[metric_keys].fillna("__ALL__")
        ours = ours.set_index(metric_keys)
        summary_rows = 0
        metric_names = list(independent_metric(scores.iloc[:0]))
        for track in ["T0", "T1", "T2"]:
            summary = pd.read_csv(folder / f"{track.lower()}_summary.csv")
            summary[metric_keys] = summary[metric_keys].fillna("__ALL__")
            if summary.duplicated(metric_keys).any():
                raise AssertionError("duplicate summary metrics")
            for record in summary.itertuples(index=False):
                lookup = tuple(getattr(record, name) for name in metric_keys)
                expected = ours.loc[lookup]
                for name in metric_names:
                    difference = _same_number(getattr(record, name), expected[name], f"summary {lookup} {name}", tolerance=1e-7)
                    metric_differences[name] = max(metric_differences.get(name, 0.0), difference)
                summary_rows += 1
        if summary_rows != len(independent):
            raise AssertionError("summary output omits or duplicates a scope/slice cell")
        t0 = scores[scores.track.eq("T0")].copy()
        t0["hit"] = t0.actual.gt(0) & (t0.prediction - t0.actual).abs().le(t0.actual * .2)
        joint = t0.groupby(KEYS + ["model"]).agg(windows=("window", "nunique"), all_three_hit=("hit", "all")).reset_index()
        if not joint.windows.eq(3).all():
            raise AssertionError("joint hit does not cover all three windows")
        joint.to_csv(output / "t0_independent_joint_hits.csv", index=False, encoding="utf-8-sig")
        after_hashes = verify_old_hashes(archive)
        if before_hashes != after_hashes:
            raise AssertionError("original archive changed while verifying")
        report.update({
            "status": "PASS_INDEPENDENT_RECONCILIATION", "core_issue_records": len(core),
            "unique_shop_spu": 174, "original_b1_eligible_records": len(b1_keys),
            "archive_end": str(truth.end), "original_hashes": after_hashes,
            "daily_rate_rows": len(rates), "tracks": details,
            "max_rate_difference": max_rate_difference, "max_truth_difference": max_truth_difference,
            "max_prediction_difference": max_prediction_difference,
            "input_hash_unique_count": len(input_hashes),
            "explicit_daily_vintages": vintage_check, "b1_diagnostic": diagnostic_check,
            "paired_comparisons": paired_check,
            "joint_and_revision": joint_revision_check,
            "independently_reconstructed_history_hashes": len(reconstructed_hashes),
            "summary_rows_checked": summary_rows, "summary_max_differences": metric_differences,
            "t0_joint_hits": joint.groupby("model").all_three_hit.sum().astype(int).to_dict(),
            "output_hashes": {path.name: sha256(path) for path in sorted(folder.glob("*.parquet"))},
            "replay_status": "RECONSTRUCTED_HISTORICAL_REPLAY",
        })
    except Exception as error:
        report.update({"status": "FAILED", "error": str(error), "exception_type": type(error).__name__, "traceback": traceback.format_exc()})
        raise
    finally:
        report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        (output / "validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", help="Must not exist; default ROOT/validation")
    parser.add_argument("--diagnostic", help="Optional checked B1 diagnostic directory; default ROOT/a_diagnostic")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--skip-diagnostic", action="store_true", help="Verify completed rolling outputs independently of B1 availability")
    selection.add_argument("--only-diagnostic", action="store_true", help="Verify B1 outputs in a separate append-only validation directory")
    arguments = parser.parse_args()
    validate(arguments.archive, arguments.root, arguments.output, arguments.diagnostic, arguments.skip_diagnostic, arguments.only_diagnostic)
