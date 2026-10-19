"""Offline B1 recursion diagnostic; original inputs/code are immutable.

This module never connects to a database. Run only after the V2 protocol gate.
Public MLForecast callbacks capture model outputs and independently control the
future feedback. Callback-returned forecasts are NOT the D1 scored outputs.
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.metadata
import json
import pathlib
import time

import numpy as np
import pandas as pd

try:
    from scripts import bc60_experiment as original
except ModuleNotFoundError:
    import bc60_experiment as original


ORIGINAL_SOURCE_SHA256 = "5e433c029ebd8fa2c60e19a389a9d2ce5cd3c344238645160cec69f7c0e4f745"
ORIGINAL_CONFIG_SHA256 = "3527e51cf6336e25706fe9cf3ea7f4c0a486341e4f6011f9e2760bd4409ebf28"
MAX_ABS_TOLERANCE = 1e-8
LAGS = (1, 7, 14, 28)
ROLLING = (7, 28)
FEATURES = ["shop_code", *[f"lag{x}" for x in LAGS],
            *[f"rolling_mean_lag1_window_size{x}" for x in ROLLING],
            "dayofweek", "month", "dayofyear"]
STAGES = {"bridge": (-1, 0), "d01_07": (1, 7), "d08_14": (8, 14),
          "d15_30": (15, 30), "d31_45": (31, 45), "d46_60": (46, 60),
          "d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
STAGE_ORDER = ("d01_07", "d08_14", "d15_30", "d31_45", "d46_60")
KEYS = ["issue_date", "shop", "spu", "ds"]
MODEL_CALLBACK_DOC = "https://nixtlaverse.nixtla.io/mlforecast/docs/how-to-guides/predict_callbacks.html"


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, data):
    pathlib.Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2,
                                           allow_nan=False, default=str), encoding="utf-8")


def require_hash(path, expected):
    actual = original.sha(path)
    if actual != expected:
        raise ValueError(f"HASH_MISMATCH:{pathlib.Path(path).name}")
    return actual


def future_template(model, terminal, horizon=62):
    """Use public future keys; ids= selects membership, not argument-list order."""
    frame = model.make_future_dataframe(horizon)
    frame = frame[frame.unique_id.isin(terminal)].reset_index(drop=True)
    order = frame.unique_id.drop_duplicates().tolist()
    if set(order) != set(terminal) or frame.duplicated(["unique_id", "ds"]).any():
        raise ValueError("FUTURE_ID_COVERAGE")
    counts = frame.groupby("unique_id").size()
    if not counts.eq(horizon).all():
        raise ValueError("FUTURE_HORIZON_COVERAGE")
    expected = pd.MultiIndex.from_product([order, range(horizon)])
    observed = pd.MultiIndex.from_arrays([frame.unique_id, frame.groupby("unique_id").cumcount()])
    if not observed.equals(expected):
        raise ValueError("FUTURE_ID_TIME_ORDER")
    return frame, order


class RecursionTrace:
    """The only mutation is our audit buffer, never MLForecast's private state."""

    def __init__(self, mode, order, terminal, idmap, cutoff, issue_date):
        if mode not in ("D0", "D1"):
            raise ValueError("UNKNOWN_DIAGNOSTIC_MODE")
        self.mode, self.order, self.cutoff = mode, list(order), pd.Timestamp(cutoff)
        self.issue_date = str(issue_date)
        self.idmap = idmap
        self.history = []
        self.shop_codes = []
        for uid in order:
            g = terminal[uid].sort_values("ds").tail(28)
            expected_dates = pd.date_range(self.cutoff - pd.Timedelta(days=27), self.cutoff)
            if (len(g) != 28 or not pd.DatetimeIndex(g.ds).equals(expected_dates)
                    or not g.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]).all()
                    or g.y.isna().any() or not g.mapped.all()):
                raise ValueError("PROXY_REQUIRES_28_KNOWN_CUTOFF_DAYS")
            if not np.isfinite(g.y.to_numpy(dtype=float)).all() or g.y.lt(0).any():
                raise ValueError("INVALID_PROXY_HISTORY")
            self.history.append(g.y.to_numpy(dtype=float))
            if g.shop_code.nunique() != 1:
                raise ValueError("SHOP_CODE_NOT_STATIC")
            self.shop_codes.append(g.shop_code.iloc[-1])
        self.state = np.asarray(self.history)
        self.initial_history = self.state.copy()
        self.proxy = self.state.mean(axis=1)
        self.rows, self.captured, self.pending = [], [], None
        self.feature_max_abs_difference = 0.0

    def before(self, x):
        if self.pending is not None:
            raise ValueError("CALLBACK_PAIR_ORDER")
        if not isinstance(x, pd.DataFrame) or list(x.columns) != FEATURES:
            raise ValueError("B1_FEATURE_SCHEMA_OR_IDENTIFIER_LEAK")
        if len(x) != len(self.order):
            raise ValueError("CALLBACK_ID_COUNT")
        np.testing.assert_array_equal(x.shop_code.to_numpy(), self.shop_codes)
        step = len(self.captured) + 1
        record = x.copy(deep=True).reset_index(drop=True)
        record["unique_id"] = self.order
        record["shop"] = [self.idmap[u][0] for u in self.order]
        record["spu"] = [self.idmap[u][1] for u in self.order]
        record["issue_date"] = self.issue_date
        record["model"] = self.mode
        record["recursive_step"] = step
        record["ds"] = self.cutoff + pd.Timedelta(days=step)
        forecast_date = self.cutoff + pd.Timedelta(days=step)
        for calendar, value in (("dayofweek", forecast_date.dayofweek),
                                ("month", forecast_date.month), ("dayofyear", forecast_date.dayofyear)):
            self._verify(record[calendar].to_numpy(), np.full(len(self.order), value))
        record["relative_day"] = step - 2
        record["cutoff_known28_mean"] = self.proxy
        record["proxy_first_date"] = self.cutoff - pd.Timedelta(days=27)
        record["proxy_last_date"] = self.cutoff
        record["future_feedback_kind"] = ("MODEL_OUTPUT" if self.mode == "D0"
                                           else "CUTOFF_KNOWN28_MEAN")
        for lag in LAGS:
            name = f"lag{lag}"
            expected = self.state[:, -lag]
            self._verify(record[name].to_numpy(), expected)
            record[f"{name}_expected"] = expected
            record[f"{name}_observed_n"] = int(step <= lag)
            record[f"{name}_feedback_n"] = int(step > lag)
            record[f"{name}_source_date"] = self.cutoff + pd.Timedelta(days=step-lag)
        for width in ROLLING:
            name = f"rolling_mean_lag1_window_size{width}"
            expected = self.state[:, -width:].mean(axis=1)
            self._verify(record[name].to_numpy(), expected)
            record[f"{name}_expected"] = expected
            record[f"{name}_observed_n"] = max(0, width-step+1)
            record[f"{name}_feedback_n"] = min(width, step-1)
        self.pending = record
        return x

    def _verify(self, actual, expected):
        if not np.isfinite(actual).all():
            raise ValueError("NONFINITE_FEATURE")
        error = float(np.max(np.abs(actual - expected)))
        self.feature_max_abs_difference = max(self.feature_max_abs_difference, error)
        if error > MAX_ABS_TOLERANCE:
            raise ValueError("FEATURE_OR_SERIES_ALIGNMENT_MISMATCH")

    def after(self, predictions):
        if self.pending is None:
            raise ValueError("CALLBACK_PAIR_ORDER")
        raw = np.asarray(predictions, dtype=float)
        if raw.shape != self.proxy.shape or not np.isfinite(raw).all():
            raise ValueError("INVALID_MODEL_OUTPUT")
        scored = np.maximum(raw, 0)
        feedback = scored.copy() if self.mode == "D0" else self.proxy.copy()
        self.captured.append(scored.copy())
        self.pending["model_output_raw"] = raw
        self.pending["model_output_nonnegative"] = scored
        self.pending["recursive_feedback"] = feedback
        self.pending["negative_clipped"] = raw < 0
        self.rows.append(self.pending)
        self.pending = None
        self.state = np.concatenate([self.state[:, -27:], feedback[:, None]], axis=1)
        return feedback

    def finish(self, returned, template, horizon=62):
        if len(self.captured) != horizon or self.pending is not None:
            raise ValueError("CALLBACK_HORIZON_MISMATCH")
        if not returned[["unique_id", "ds"]].reset_index(drop=True).equals(template):
            raise ValueError("RETURNED_TEMPLATE_ALIGNMENT")
        scored = np.asarray(self.captured).ravel("F")
        expected_return = scored if self.mode == "D0" else np.repeat(self.proxy, horizon)
        self._verify(returned.B1.to_numpy(), expected_return)
        output = template.copy()
        output["forecast"] = scored
        output["shop"] = output.unique_id.map(lambda uid: self.idmap[uid][0])
        output["spu"] = output.unique_id.map(lambda uid: self.idmap[uid][1])
        output["issue_date"] = self.issue_date
        output["model"] = self.mode
        output["relative_day"] = (output.ds - pd.Timestamp(self.issue_date)).dt.days
        return output, pd.concat(self.rows, ignore_index=True)


def predict_traced(model, mode, terminal, idmap, cutoff, issue_date):
    template, order = future_template(model, terminal)
    trace = RecursionTrace(mode, order, terminal, idmap, cutoff, issue_date)
    returned = model.predict(62, ids=list(terminal), before_predict_callback=trace.before,
                             after_predict_callback=trace.after)
    output, features = trace.finish(returned, template)
    checks = {"steps": len(trace.captured), "context_series": len(order),
              "feature_max_abs_difference": trace.feature_max_abs_difference,
              "no_identifier_model_features": True,
              "observed_history_never_replaced": True,
              "proxy_uses_cutoff_known28_only": True,
              "public_callback_only": True}
    return output, features, checks


def compare_archived(d0, archived):
    left, right = d0[KEYS + ["forecast"]], archived[KEYS + ["forecast"]]
    if left.duplicated(KEYS).any() or right.duplicated(KEYS).any():
        raise ValueError("D0_DUPLICATE_FORECAST_KEYS")
    pair = left.merge(right, on=KEYS, how="outer", indicator=True, validate="one_to_one",
                      suffixes=("_d0", "_archived"))
    complete = pair._merge.eq("both").all()
    finite = (np.isfinite(pair.forecast_d0).all()
              and np.isfinite(pair.forecast_archived).all())
    maximum = float((pair.forecast_d0 - pair.forecast_archived).abs().max()) if complete and finite and len(pair) else None
    return {"status": "PASS" if complete and finite and maximum is not None and maximum <= MAX_ABS_TOLERANCE else "FAIL",
            "archived_rows": len(right), "reproduced_rows": len(left),
            "key_coverage_equal": bool(complete), "finite": bool(finite),
            "max_abs_difference": maximum, "tolerance": MAX_ABS_TOLERANCE}


def full_core_daily(core, issue, forecasts, truth, d1_status):
    """Keep the frozen core grid, including original missing-B1 objects."""
    days = pd.DataFrame({"ds": pd.date_range(pd.Timestamp(issue)-pd.Timedelta(days=1), periods=62)})
    dimensions = [c for c in ["shop", "spu", "lifecycle", "maturity", "original_b1_available"] if c in core]
    grid = core[dimensions].merge(days, how="cross")
    grid["issue_date"] = issue
    grid["update_date"] = issue
    grid["information_cutoff"] = pd.Timestamp(issue)-pd.Timedelta(days=2)
    grid["relative_day"] = (grid.ds-pd.Timestamp(issue)).dt.days
    truth = truth[["shop", "spu", "ds", "y", "sales_state"]].rename(columns={"y": "actual"})
    grid = grid.merge(truth, on=["shop", "spu", "ds"], how="left", validate="one_to_one")
    grid["actual"] = grid.actual.where(grid.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]))
    pieces = []
    for mode in ("D0", "D1"):
        part = grid.copy()
        available = forecasts.get(mode)
        if available is not None:
            part = part.merge(available[["shop", "spu", "ds", "forecast"]],
                              on=["shop", "spu", "ds"], how="left", validate="one_to_one")
        else:
            part["forecast"] = np.nan
        part["model"] = mode
        part["prediction_state"] = np.where(part.forecast.notna(), "OK", "ORIGINAL_B1_UNAVAILABLE")
        if mode == "D1" and d1_status != "PASS":
            part["prediction_state"] = d1_status
        pieces.append(part)
    return pd.concat(pieces, ignore_index=True)


def stage_scores(daily):
    rows = []
    for (issue, shop, spu, model), g in daily.groupby(["issue_date", "shop", "spu", "model"], sort=True):
        for stage, (first, last) in STAGES.items():
            z = g[g.relative_day.between(first, last)].sort_values("ds")
            size = last-first+1
            actual = float(z.actual.sum()) if len(z) == size and z.actual.notna().all() else np.nan
            prediction = float(z.forecast.sum()) if len(z) == size and z.forecast.notna().all() else np.nan
            error = prediction-actual
            metadata = {c: g[c].iloc[0] for c in ["lifecycle", "maturity", "original_b1_available", "source_daily_sha256", "diagnostic_source_sha256"] if c in g}
            rows.append({"issue_date": issue, "shop": shop, "spu": spu, "model": model, **metadata,
                         "update_date": issue, "information_cutoff": str((pd.Timestamp(issue)-pd.Timedelta(days=2)).date()),
                         "stage": stage, "first_relative_day": first, "last_relative_day": last,
                         "target_start": str((pd.Timestamp(issue)+pd.Timedelta(days=first)).date()),
                         "target_end": str((pd.Timestamp(issue)+pd.Timedelta(days=last)).date()),
                         "days": size, "actual": actual, "prediction": prediction,
                         "signed_error": error, "daily_mean_actual": actual/size,
                         "daily_mean_prediction": prediction/size, "daily_mean_error": error/size,
                         "prediction_state": "OK" if pd.notna(prediction) else str(z.prediction_state.iloc[0]),
                         "truth_state": "OBSERVED" if pd.notna(actual) else "MISSING_OR_INVALID_DAYS"})
    result = pd.DataFrame(rows)
    result["absolute_error"] = result.signed_error.abs()
    result["relative_error"] = result.signed_error/result.actual.where(result.actual.gt(0))
    result["hit20"] = result.relative_error.abs().le(.2) & result.relative_error.notna()
    result["judgment"] = np.select([result.hit20, result.relative_error.notna()], ["HIT", "MISS"], default="UNJUDGED")
    return result


def summarize_stages(scores):
    rows = []
    slices = [(["model", "stage"], "overall"),
                        (["issue_date", "model", "stage"], "issue"),
                        (["shop", "model", "stage"], "shop")]
    slices += [([dimension, "model", "stage"], dimension) for dimension in ("lifecycle", "maturity") if dimension in scores]
    for dims, label in slices:
        for keys, group in scores.groupby(dims, dropna=False):
            rows.append({"slice": label, "scope": "FULL_FROZEN_CORE", **dict(zip(dims, keys)), **original.metric(group),
                         "hit_count": int(group.judgment.eq("HIT").sum()), "miss_count": int(group.judgment.eq("MISS").sum()),
                         "unjudged_count": int(group.judgment.eq("UNJUDGED").sum()),
                         "missing_output_count": int(group.prediction.isna().sum())})
    if "original_b1_available" in scores:
        for (model, stage), group in scores[scores.original_b1_available].groupby(["model", "stage"]):
            rows.append({"slice": "overall", "scope": "SECONDARY_ORIGINAL_369_B1_OUTPUTS", "model": model,
                         "stage": stage, **original.metric(group),
                         "hit_count": int(group.judgment.eq("HIT").sum()), "miss_count": int(group.judgment.eq("MISS").sum()),
                         "unjudged_count": int(group.judgment.eq("UNJUDGED").sum()),
                         "missing_output_count": int(group.prediction.isna().sum())})
    return pd.DataFrame(rows)


def curve_diagnostics(scores):
    """Arithmetic identities only; no causal attribution from forecast errors."""
    rows = []
    for (issue, shop, spu), group in scores.groupby(["issue_date", "shop", "spu"], sort=True):
        lookup = group.set_index(["model", "stage"])
        for stage in STAGE_ORDER:
            d0, d1 = lookup.loc[("D0", stage)], lookup.loc[("D1", stage)]
            rows.append({"issue_date": issue, "shop": shop, "spu": spu, "stage": stage,
                         "d0_daily_mean_forecast": d0.daily_mean_prediction,
                         "d1_daily_mean_forecast": d1.daily_mean_prediction,
                         "actual_daily_mean": d0.daily_mean_actual,
                         "d1_minus_d0_forecast_per_day": d1.daily_mean_prediction-d0.daily_mean_prediction,
                         "d0_signed_error_per_day": d0.daily_mean_error,
                         "d1_signed_error_per_day": d1.daily_mean_error,
                         "d1_minus_d0_absolute_stage_error": abs(d1.signed_error)-abs(d0.signed_error)})
    curves = pd.DataFrame(rows)
    growth = []
    for (issue, shop, spu, model), group in scores.groupby(["issue_date", "shop", "spu", "model"], sort=True):
        z = group.set_index("stage")
        first = z.loc[STAGE_ORDER[0]]
        for stage in STAGE_ORDER[1:]:
            target = z.loc[stage]
            forecast_change = target.daily_mean_prediction-first.daily_mean_prediction
            actual_change = target.daily_mean_actual-first.daily_mean_actual
            error_change = target.daily_mean_error-first.daily_mean_error
            growth.append({"issue_date": issue, "shop": shop, "spu": spu, "model": model,
                           "reference_stage": STAGE_ORDER[0], "target_stage": stage,
                           "forecast_daily_mean_change": forecast_change,
                           "actual_daily_mean_change": actual_change,
                           "signed_error_daily_mean_change": error_change,
                           "identity_residual": error_change-(forecast_change-actual_change),
                           "interpretation": "DESCRIPTIVE_IDENTITY_NOT_CAUSAL"})
    return curves, pd.DataFrame(growth)


def run(archive, out, config=None, models_from=None, v2_config=None):
    from lightgbm import LGBMRegressor
    from mlforecast import MLForecast
    from mlforecast.lag_transforms import RollingMean

    archive, out = pathlib.Path(archive).resolve(), pathlib.Path(out).resolve()
    config = pathlib.Path(config) if config else pathlib.Path(__file__).resolve().parents[1]/"docs/bc60_design_lock_v1.json"
    source = pathlib.Path(original.__file__).resolve()
    if archive == out or archive in out.parents or out.exists():
        raise ValueError("NEW_OUTPUT_OUTSIDE_ORIGINAL_ARCHIVE_REQUIRED")
    require_hash(source, ORIGINAL_SOURCE_SHA256)
    require_hash(config, ORIGINAL_CONFIG_SHA256)
    lock = json.loads(config.read_text(encoding="utf-8"))
    v2_config = pathlib.Path(v2_config) if v2_config else pathlib.Path(__file__).resolve().parents[1]/"docs/bc60_v2_design_lock.json"
    v2_lock = json.loads(v2_config.read_text(encoding="utf-8"))
    if (v2_lock["issue_dates"] != lock["issue_dates"] or v2_lock["reproduction_absolute_tolerance"] != MAX_ABS_TOLERANCE
            or v2_lock["max_b1_rebuilds"] != 3 or v2_lock["b1_threads"] != 2
            or v2_lock["b1_max_minutes_per_fit"] != 10 or v2_lock["available_lag_days"] != 2):
        raise ValueError("V2_DIAGNOSTIC_PROTOCOL_MISMATCH")
    versions = {name: importlib.metadata.version(name) for name in lock["candidate_libraries"]}
    if versions != lock["candidate_libraries"]:
        raise ValueError("ORIGINAL_LIBRARY_LOCK_MISMATCH")
    effects = archive/"effects_v1"
    original_manifest = json.loads((effects/"run_manifest.json").read_text(encoding="utf-8"))
    if (original_manifest["source_sha256"] != ORIGINAL_SOURCE_SHA256
            or original_manifest["config_sha256"] != ORIGINAL_CONFIG_SHA256):
        raise ValueError("ARCHIVED_CODE_CONFIG_MISMATCH")
    inputs = archive/"inputs_v1_final"
    hashes = {name: require_hash(inputs/name, expected) for name, expected in original_manifest["inputs"].items()}
    artifacts = {name: require_hash(effects/name, original_manifest["files"][name])
                 for name in ("core_lists.csv", "daily_forecasts.parquet", "scores.parquet", "fits.json")}
    if hashes["daily.parquet"] != v2_lock["source_daily_sha256"] or artifacts["core_lists.csv"] != v2_lock["core_sha256"]:
        raise ValueError("V2_SOURCE_LOCK_MISMATCH")
    daily = pd.read_parquet(inputs/"daily.parquet")
    monthly = pd.read_parquet(inputs/"monthly_reference.parquet")
    monthly["month"] = pd.PeriodIndex(monthly.month, freq="M")
    core = pd.read_csv(effects/"core_lists.csv", dtype={"shop": str, "spu": str, "issue_date": str})
    archived_daily = pd.read_parquet(effects/"daily_forecasts.parquet")
    archived_daily = archived_daily[archived_daily.model.eq("B1")].copy()
    archived_scores = pd.read_parquet(effects/"scores.parquet")
    original_core_status = archived_scores[(archived_scores.model.eq("B1"))
                                      & (archived_scores.window.eq("d01_60"))].copy()
    original_missing = original_core_status.prediction.isna().sum()
    original_core_status["original_b1_available"] = original_core_status.prediction.notna()
    core = core.merge(original_core_status[["issue_date", "shop", "spu", "original_b1_available"]],
                      on=["issue_date", "shop", "spu"], validate="one_to_one")
    if len(core) != 385 or original_missing != 16 or core.duplicated(["issue_date", "shop", "spu"]).any():
        raise ValueError("FROZEN_CORE_OR_ORIGINAL_GAP_COUNT_CHANGED")
    out.mkdir(parents=True, exist_ok=False)
    (out/"models").mkdir()
    (out/"features").mkdir()
    manifest = {"status": "RUNNING", "started_at": utc_now(), "classification": "RECONSTRUCTED_HISTORICAL_REPLAY",
                "scope": "TASK_A_RECURSIVE_FEEDBACK_MECHANISM_DIAGNOSTIC_NOT_NEW_MODEL",
                "original_source_sha256": ORIGINAL_SOURCE_SHA256, "original_config_sha256": ORIGINAL_CONFIG_SHA256,
                "diagnostic_source_sha256": original.sha(__file__), "input_hashes": hashes,
                "v2_config_sha256": original.sha(v2_config),
                "original_artifact_hashes": artifacts, "versions": versions,
                "callback_documentation": MODEL_CALLBACK_DOC,
                "frozen_core_issue_n": len(core), "original_b1_missing_core_issue_n": int(original_missing),
                "max_model_fits": 3, "actual_model_fits": 0, "issues": [], "errors": [],
                "d0_reproduction_tolerance": MAX_ABS_TOLERANCE,
                "d1_scored_value": "CLIPPED_MODEL_OUTPUT_CAPTURED_BEFORE_FEEDBACK_SUBSTITUTION",
                "d1_feedback": "EACH_SERIES_28_KNOWN_DAYS_ENDING_AT_CUTOFF_MEAN",
                "bridge_relative_days": [-1, 0], "all_claims_descriptive": True}
    write_json(out/"diagnostic_manifest.json", manifest)
    all_daily = []
    for issue in lock["issue_dates"]:
        cutoff = pd.Timestamp(issue)-pd.Timedelta(days=2)
        issue_manifest = {"issue_date": issue, "cutoff": str(cutoff.date()), "status": "STARTED"}
        manifest["issues"].append(issue_manifest)
        forecasts, d1_status = {}, "TASK_A_ISSUE_FAILED"
        issue_core = core[core.issue_date.eq(issue)]
        try:
            history = daily[daily.ds.le(cutoff)].copy()
            life = original.lifecycle(monthly, issue)
            qualified = set(map(tuple, life.loc[life.lifecycle.eq("ESTABLISHED"), ["shop", "spu"]].to_numpy()))
            train, terminal, idmap = original.make_segments(history, qualified)
            if train.empty or train.ds.max() > cutoff:
                raise ValueError("EMPTY_OR_POST_CUTOFF_TRAINING")
            fingerprint = {"issue_date": issue, "input_hashes": hashes,
                           "original_source_sha256": ORIGINAL_SOURCE_SHA256,
                           "original_config_sha256": ORIGINAL_CONFIG_SHA256, "versions": versions}
            model_dir = out/"models"/issue
            if models_from:
                saved = pathlib.Path(models_from)/issue
                metadata = json.loads((saved/"checkpoint.json").read_text(encoding="utf-8"))
                if metadata["fingerprint"] != fingerprint:
                    raise ValueError("MODEL_CHECKPOINT_FINGERPRINT_MISMATCH")
                for name, expected in metadata["model_files"].items():
                    require_hash(saved/name, expected)
                model = MLForecast.load(saved)
                issue_manifest["fit_mode"] = "RECOVERED_WITHOUT_REFIT"
                issue_manifest["recovered_model_files"] = metadata["model_files"]
            else:
                if manifest["actual_model_fits"] >= 3:
                    raise ValueError("MODEL_FIT_BUDGET_EXCEEDED")
                model = MLForecast(models={"B1": LGBMRegressor(**lock["lightgbm"])}, freq="D",
                                   lags=lock["lags"], lag_transforms={1: [RollingMean(w) for w in lock["rolling_windows_on_lag_1"]]},
                                   date_features=lock["date_features"], num_threads=2)
                manifest["actual_model_fits"] += 1
                write_json(out/"diagnostic_manifest.json", manifest)
                started = time.monotonic()
                model.fit(train[["unique_id", "ds", "y", "shop_code"]], static_features=["shop_code"])
                elapsed = time.monotonic()-started
                issue_manifest.update({"fit_mode": "ORIGINAL_B1_CONFIG_ONCE", "fit_seconds": elapsed})
            # Persist the fitted estimator BEFORE either recursive prediction.
            model_dir.mkdir()
            model.save(model_dir)
            model_hashes = {p.name: original.sha(p) for p in model_dir.iterdir() if p.is_file()}
            write_json(model_dir/"checkpoint.json", {"fingerprint": fingerprint,
                       "model_files": model_hashes, "saved_before_predictions": True, "created_at": utc_now()})
            issue_manifest["model_files"] = model_hashes
            issue_manifest.update({"train_rows_before_lag_drop": len(train),
                                   "train_segments": int(train.unique_id.nunique()), "context_prediction_series": len(terminal),
                                   "label_latest": str(train.ds.max().date()), "training_target": "RAW_ONE_DAY_COUNT",
                                   "sample_weights": None, "target_transforms": None,
                                   "model_features": list(model.models_["B1"].feature_name_)})
            if issue_manifest.get("fit_seconds", 0) > 600:
                raise ValueError("FIT_TIME_BUDGET_EXCEEDED_CHECKPOINT_SAVED")
            if issue_manifest["model_features"] != FEATURES:
                raise ValueError("FITTED_B1_FEATURE_SCHEMA_CHANGED")
            d0, features, checks = predict_traced(model, "D0", terminal, idmap, cutoff, issue)
            forecasts["D0"] = d0
            features.to_parquet(out/"features"/f"{issue}_D0.parquet", index=False)
            issue_manifest["d0_trace_checks"] = checks
            original_issue = archived_daily[archived_daily.issue_date.eq(issue)]
            d0_core = d0.merge(issue_core[["shop", "spu"]], on=["shop", "spu"], validate="many_to_one")
            reproduction = compare_archived(d0_core, original_issue)
            issue_manifest["d0_reproduction"] = reproduction
            if reproduction["status"] != "PASS":
                d1_status = "D1_BLOCKED_D0_REPRODUCTION_FAILED"
                raise ValueError(d1_status)
            d1, features, checks = predict_traced(model, "D1", terminal, idmap, cutoff, issue)
            first = d0[d0.relative_day.eq(-1)].merge(d1[d1.relative_day.eq(-1)],
                      on=KEYS, suffixes=("_d0", "_d1"), validate="one_to_one")
            first_difference = float((first.forecast_d0-first.forecast_d1).abs().max())
            if len(first) != len(terminal) or first_difference > MAX_ABS_TOLERANCE:
                raise ValueError("D0_D1_FIRST_STEP_MISMATCH")
            checks["first_step_d0_d1_max_abs_difference"] = first_difference
            forecasts["D1"] = d1
            features.to_parquet(out/"features"/f"{issue}_D1.parquet", index=False)
            issue_manifest["d1_trace_checks"] = checks
            d1_status = "PASS"
            issue_manifest["status"] = "PASS"
        except Exception as exc:
            issue_manifest["status"] = "FAIL"
            issue_manifest["error"] = f"{type(exc).__name__}: {exc}"
            manifest["errors"].append({"issue_date": issue, "message": issue_manifest["error"]})
        part = full_core_daily(issue_core, issue, forecasts, daily, d1_status)
        part["source_daily_sha256"] = hashes["daily.parquet"]
        part["diagnostic_source_sha256"] = manifest["diagnostic_source_sha256"]
        part.to_parquet(out/f"daily_{issue}.parquet", index=False)
        all_daily.append(part)
        write_json(out/"diagnostic_manifest.json", manifest)
        print(json.dumps({"issue_date": issue, "status": issue_manifest["status"],
                          "d1_status": d1_status}), flush=True)
    combined = pd.concat(all_daily, ignore_index=True)
    combined.to_parquet(out/"daily_d0_d1.parquet", index=False)
    scores = stage_scores(combined)
    scores.to_csv(out/"stage_scores.csv", index=False, encoding="utf-8-sig")
    summarize_stages(scores).to_csv(out/"stage_summary.csv", index=False, encoding="utf-8-sig")
    curves, growth = curve_diagnostics(scores)
    curves.to_csv(out/"object_curve_diagnostics.csv", index=False, encoding="utf-8-sig")
    growth.to_csv(out/"signed_error_growth.csv", index=False, encoding="utf-8-sig")
    core_n = combined[["issue_date", "shop", "spu"]].drop_duplicates().shape[0]
    d0_missing = scores[scores.model.eq("D0") & scores.stage.eq("d01_60")].prediction.isna().sum()
    checks = {"frozen_core_issue_rows_preserved": core_n == 385,
              "original_b1_output_core_issue_n": int(core.original_b1_available.sum()),
              "daily_rows_expected": 385*62*2, "daily_rows_actual": len(combined),
              "d0_missing_core_issue_n": int(d0_missing),
              "original_16_b1_gaps_preserved": int(d0_missing) == 16,
              "fit_budget_pass": manifest["actual_model_fits"] <= 3,
              "signed_error_growth_max_abs_identity_residual": float(growth.identity_residual.abs().max())
              if growth.identity_residual.notna().any() else None}
    manifest["numeric_checks"] = checks
    manifest["status"] = "COMPLETE" if not manifest["errors"] and core_n == 385 and d0_missing == 16 and len(combined) == 385*62*2 else "PARTIAL_OR_FAILED"
    manifest["finished_at"] = utc_now()
    manifest["files"] = {str(p.relative_to(out)).replace("\\", "/"): original.sha(p)
                         for p in out.rglob("*") if p.is_file() and p.name != "diagnostic_manifest.json"}
    write_json(out/"diagnostic_manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--config")
    parser.add_argument("--v2-config")
    parser.add_argument("--models-from", help="Prior local Task A models directory; all three checkpoints required, no refits")
    arguments = vars(parser.parse_args())
    result = run(**arguments)
    print(json.dumps({"status": result["status"], "actual_model_fits": result["actual_model_fits"]}))
