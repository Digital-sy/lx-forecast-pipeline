"""Pure scoring of immutable shadow forecasts against certified mature actuals.

No model, network, database, clock, or filesystem dependency is used here.
The caller owns the source/maturity gate: COMPLETE means the *entire* window
has matured and its daily source, mapping, and synchronization checks passed.
Partial actuals may be retained as evidence but never enter official scores.
Scores are suppressed for both scopes if any full-core truth in that window
is incomplete.  Optional full-store totals are coverage denominators, not a
replacement for the caller's source gate.  No missing record is made zero.

Historical multi-issue inputs may include issue_date in all three dataframes;
then it is part of the object key.  Single-issue inputs need only shop/spu.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


WINDOWS = ("d01_30", "d31_60", "d01_60")
SCOPES = ("FULL_CORE", "PAIRED")
COMPLETE = "COMPLETE"
PREDICTION_OK = "OK"


def _require(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f"{label}: missing columns {sorted(missing)}")


def _keys(frame: pd.DataFrame, key: list[str], label: str) -> None:
    if frame[key].isna().any().any():
        raise ValueError(f"{label}: NULL business key")
    if frame.duplicated(key).any():
        raise ValueError(f"{label}: duplicate business key")


def _date_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    for name in columns:
        values = pd.to_datetime(frame[name], errors="raise")
        if values.isna().any() or getattr(values.dt, "tz", None) is not None:
            raise ValueError(f"{label}: unknown or timezone-bearing date {name}")
        if not values.eq(values.dt.normalize()).all():
            raise ValueError(f"{label}: non-midnight business date {name}")
        frame[name] = values.dt.strftime("%Y-%m-%d")


def _numeric(series: pd.Series, label: str) -> pd.Series:
    result = pd.to_numeric(series, errors="raise").astype(float)
    if np.isinf(result.to_numpy()).any():
        raise ValueError(f"{label}: infinite value")
    return result


def _finite(series: pd.Series) -> pd.Series:
    return pd.Series(np.isfinite(series.to_numpy(dtype=float)), index=series.index)


def _known_sum(series: pd.Series) -> float:
    return float(series.sum(min_count=1))


def _ratio(numerator: float, denominator: float) -> float:
    return float(numerator / denominator) if np.isfinite(denominator) and denominator > 0 else np.nan


def _state(complete: pd.Series) -> str:
    if len(complete) and complete.all():
        return COMPLETE
    return "INCOMPLETE" if complete.any() else "WAITING_ACTUALS"


def _store_total(totals: Mapping[str, Any] | None, window: str) -> float:
    value = None if totals is None else totals.get(window)
    if value is None or (isinstance(value, str) and value == "UNKNOWN"):
        return np.nan
    try:
        value = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid full-store total for {window}") from exc
    if np.isnan(value):
        return np.nan
    if not np.isfinite(value) or value < 0:
        raise ValueError(f"invalid full-store total for {window}")
    return value


def _calendar(calendar: pd.DataFrame, object_key: list[str]) -> None:
    if not calendar.window.isin(WINDOWS).all():
        raise ValueError("unknown window")
    counts = calendar.groupby(object_key, dropna=False).window.nunique()
    if not counts.eq(3).all():
        raise ValueError("each full-core object must retain all three windows")
    start = calendar.pivot(index=object_key, columns="window", values="target_start")
    end = calendar.pivot(index=object_key, columns="window", values="target_end")
    for w, days in (("d01_30", 30), ("d31_60", 30), ("d01_60", 60)):
        if not (pd.to_datetime(end[w]) - pd.to_datetime(start[w])).dt.days.eq(days - 1).all():
            raise ValueError(f"wrong calendar length: {w}")
    if not (start.d01_30.eq(start.d01_60) & end.d31_60.eq(end.d01_60)).all():
        raise ValueError("cumulative window boundaries disagree")
    if not (pd.to_datetime(start.d31_60) - pd.to_datetime(end.d01_30)).dt.days.eq(1).all():
        raise ValueError("segment windows must be contiguous and non-overlapping")
    if "issue_date" in object_key:
        expected = pd.to_datetime(start.index.get_level_values("issue_date")) + pd.Timedelta(days=1)
        if not np.array_equal(pd.to_datetime(start.d01_30).to_numpy(), expected.to_numpy()):
            raise ValueError("target must start the day after issue_date")


def _assemble(predictions: pd.DataFrame, truth: pd.DataFrame, paired_keys: pd.DataFrame):
    p, t, pairs = predictions.copy(deep=True), truth.copy(deep=True), paired_keys.copy(deep=True)
    object_key = (["issue_date"] if "issue_date" in p else []) + ["shop", "spu"]
    window_key = object_key + ["window"]
    pred_key = object_key + ["model", "window"]
    if p.empty:
        raise ValueError("prediction population is empty")
    _require(p, pred_key + ["target_start", "target_end", "prediction", "prediction_status"], "predictions")
    _require(t, window_key + ["target_start", "target_end", "actual", "actual_status"], "truth")
    _require(pairs, object_key, "paired_keys")
    if "issue_date" not in object_key and ("issue_date" in t or "issue_date" in pairs):
        raise ValueError("inconsistent issue_date key")
    _keys(p, pred_key, "predictions")
    _keys(t, window_key, "truth")
    _keys(pairs, object_key, "paired_keys")
    _date_columns(p, ["target_start", "target_end"] + (["issue_date"] if "issue_date" in p else []), "predictions")
    _date_columns(t, ["target_start", "target_end"] + (["issue_date"] if "issue_date" in t else []), "truth")
    if "issue_date" in pairs:
        _date_columns(pairs, ["issue_date"], "paired_keys")
    _keys(p, pred_key, "normalized predictions")
    _keys(t, window_key, "normalized truth")
    _keys(pairs, object_key, "normalized paired_keys")
    p["prediction"] = _numeric(p.prediction, "prediction")
    t["actual"] = _numeric(t.actual, "actual")
    if p.prediction_status.isna().any() or t.actual_status.isna().any():
        raise ValueError("status must be explicit")
    if p.prediction.lt(0).any() or t.actual.lt(0).any():
        raise ValueError("negative final quantity")
    if not p.prediction_status.eq(PREDICTION_OK).eq(_finite(p.prediction)).all():
        raise ValueError("prediction status/value contradiction")
    if (t.actual_status.eq(COMPLETE) & ~_finite(t.actual)).any():
        raise ValueError("COMPLETE truth requires finite nonnegative actual")
    dates = p[window_key + ["target_start", "target_end"]].drop_duplicates()
    _keys(dates, window_key, "cross-model calendar")
    _calendar(dates, object_key)
    population = dates[object_key].drop_duplicates()
    q = pairs[object_key].merge(population, on=object_key, how="left", indicator=True, validate="one_to_one")
    if not q._merge.eq("both").all():
        raise ValueError("paired object outside full core")
    # Missing rows remain failures for that model; a model cannot shrink scope.
    skeleton = dates.merge(p[["model"]].drop_duplicates(), how="cross")
    detail = skeleton.merge(p[pred_key + ["prediction", "prediction_status"]], on=pred_key, how="left", validate="one_to_one")
    detail["prediction_status"] = detail.prediction_status.fillna("MISSING_PREDICTION_RECORD")
    compare = dates.merge(t, on=window_key, how="outer", suffixes=("", "_truth"), indicator=True, validate="one_to_one")
    if compare._merge.eq("right_only").any():
        raise ValueError("truth object outside frozen prediction core")
    present = compare._merge.eq("both")
    for name in ("target_start", "target_end"):
        if not compare.loc[present, name].eq(compare.loc[present, name + "_truth"]).all():
            raise ValueError("prediction/truth target dates disagree")
    truth_complete = compare[window_key + ["actual", "actual_status"]].copy()
    truth_complete["actual_status"] = truth_complete.actual_status.fillna("MISSING_RECORD")
    truth_complete["truth_complete"] = truth_complete.actual_status.eq(COMPLETE)
    truth_complete["scoring_actual"] = truth_complete.actual.where(truth_complete.truth_complete)
    # Actuals and cumulative forecasts must each obey the frozen two-part sum.
    tv = truth_complete.pivot(index=object_key, columns="window", values="scoring_actual")
    ready = tv.notna().all(axis=1)
    if not np.allclose((tv.loc[ready, "d01_30"] + tv.loc[ready, "d31_60"]).to_numpy(),
                       tv.loc[ready, "d01_60"].to_numpy(), rtol=0, atol=0):
        raise ValueError("actual cumulative total is not the two segment sum")
    pv = detail.pivot(index=object_key + ["model"], columns="window", values="prediction")
    both = pv[["d01_30", "d31_60"]].notna().all(axis=1)
    if not both.eq(pv.d01_60.notna()).all():
        raise ValueError("cumulative prediction output status conflicts with segments")
    # Stored floating sums can differ by a final ULP across archive formats.
    if not np.allclose((pv.loc[both, "d01_30"] + pv.loc[both, "d31_60"]).to_numpy(),
                       pv.loc[both, "d01_60"].to_numpy(), rtol=1e-14, atol=1e-12):
        raise ValueError("prediction cumulative total is not the two segment sum")
    detail = detail.merge(truth_complete, on=window_key, how="left", validate="many_to_one")
    pairs = pairs[object_key].assign(paired=True)
    detail = detail.merge(pairs, on=object_key, how="left", validate="many_to_one")
    detail["paired"] = detail.paired.eq(True)
    batch_key = (["issue_date"] if "issue_date" in object_key else []) + ["window"]
    readiness = truth_complete.groupby(batch_key, dropna=False).truth_complete.all().rename("full_core_window_ready").reset_index()
    detail = detail.merge(readiness, on=batch_key, how="left", validate="many_to_one")
    valid = detail.truth_complete & detail.prediction.notna()
    detail["scorable"] = valid
    detail["error"] = (detail.prediction - detail.scoring_actual).where(valid)
    detail["absolute_error"] = detail.error.abs()
    detail["over_units"] = detail.error.clip(lower=0)
    detail["under_units"] = (-detail.error).clip(lower=0)
    detail["relative_error"] = (detail.error / detail.scoring_actual).where(valid & detail.scoring_actual.gt(0))
    hits = valid & detail.scoring_actual.gt(0) & detail.relative_error.abs().le(0.2)
    detail["hit"] = hits.astype("boolean").mask(~detail.truth_complete, pd.NA)
    return detail, truth_complete, object_key


def _metrics(group: pd.DataFrame, object_key: list[str], total: float) -> dict[str, Any]:
    known, valid = group.truth_complete, group.scorable
    ready = bool(len(group) and group.full_core_window_ready.all())
    state = COMPLETE if ready else ("INCOMPLETE" if known.any() else "WAITING_ACTUALS")
    y = group.scoring_actual
    known_units = _known_sum(y)
    scored_units = _known_sum(y.where(valid))
    output = group.prediction.notna()
    result: dict[str, Any] = {
        "score_status": state, "formal_scoring_allowed": ready,
        "fixed_n": len(group), "output_n": int(output.sum()), "truth_n": int(known.sum()),
        "scored_n": int(valid.sum()), "zero_truth_n": int((known & y.eq(0)).sum()),
        "unknown_truth_n": int((~known).sum()), "missing_output_n": int((~output).sum()),
        "known_actual_units": known_units, "scorable_actual_units": scored_units,
        "output_prediction_units": _known_sum(group.prediction),
        "output_coverage": float(output.mean()) if len(group) else np.nan,
        "truth_coverage": float(known.mean()) if len(group) else np.nan,
        "scoring_coverage": float(valid.mean()) if len(group) else np.nan,
        "full_store_actual_units": total,
        "known_actual_full_store_share": _ratio(known_units, total) if ready else np.nan,
        "output_actual_full_store_share": _ratio(scored_units, total) if ready else np.nan,
        "products": int(group[["shop", "spu"]].drop_duplicates().shape[0]),
        "periods": int(group.issue_date.nunique()) if "issue_date" in object_key else (1 if len(group) else 0),
    }
    score_fields = ("actual_units", "prediction_units", "ae", "wape", "bias", "over_units", "under_units", "over_rate",
                    "hit_n", "hit_fixed_share", "hit_actual", "hit_actual_share", "zero_truth_ae",
                    "positive_actual_zero_prediction_n", "positive_actual_zero_prediction_actual",
                    "hit_actual_full_store_share")
    result.update({name: np.nan for name in score_fields})
    if not ready:
        return result
    e = group.loc[valid, "error"]
    actual = float(y.loc[valid].sum())
    hits = group.hit.fillna(False).astype(bool)
    hit_actual = float(y.loc[hits].sum())
    zero_pred = valid & y.gt(0) & group.prediction.eq(0)
    over, under = float(e.clip(lower=0).sum()), float((-e).clip(lower=0).sum())
    ae = float(e.abs().sum())
    result.update(actual_units=actual, prediction_units=float(group.loc[valid, "prediction"].sum()),
                  ae=ae, wape=_ratio(ae, actual), bias=_ratio(float(e.sum()), actual),
                  over_units=over, under_units=under, over_rate=_ratio(over, actual),
                  hit_n=int(hits.sum()), hit_fixed_share=float(hits.mean()), hit_actual=hit_actual,
                  hit_actual_share=_ratio(hit_actual, known_units), zero_truth_ae=float(e.loc[y.loc[valid].eq(0)].abs().sum()),
                  positive_actual_zero_prediction_n=int(zero_pred.sum()),
                  positive_actual_zero_prediction_actual=float(y.loc[zero_pred].sum()),
                  hit_actual_full_store_share=_ratio(hit_actual, total))
    return result


def _joint_objects(detail: pd.DataFrame, object_key: list[str]) -> pd.DataFrame:
    key = object_key + ["model"]
    values = ("prediction", "scoring_actual", "actual_status", "prediction_status", "full_core_window_ready")
    wide = detail.pivot(index=key, columns="window", values=list(values))
    wide.columns = [str(a) + "_" + str(b) for a, b in wide.columns]
    wide = wide.reset_index()
    wide = wide.merge(detail[object_key + ["paired"]].drop_duplicates(), on=object_key, validate="many_to_one")
    for w in WINDOWS:
        wide["actual_" + w] = pd.to_numeric(wide.pop("scoring_actual_" + w))
        wide["prediction_" + w] = pd.to_numeric(wide["prediction_" + w])
    truth_complete = wide[["actual_status_" + w for w in WINDOWS]].eq(COMPLETE).all(axis=1)
    output_complete = wide[["prediction_" + w for w in WINDOWS]].notna().all(axis=1)
    wide["truth_complete"] = truth_complete
    wide["output_complete"] = output_complete
    wide["full_core_joint_ready"] = wide[["full_core_window_ready_" + w for w in WINDOWS]].all(axis=1)
    for w in WINDOWS:
        y, p = wide["actual_" + w], wide["prediction_" + w]
        h = y.gt(0) & p.notna() & ((p - y).abs() / y).le(0.2)
        wide["hit_" + w] = h.astype("boolean").mask(wide["actual_status_" + w].ne(COMPLETE), pd.NA)
    hits = wide[["hit_" + w for w in WINDOWS]].fillna(False).all(axis=1)
    wide["joint_hit"] = hits.astype("boolean").mask(~truth_complete, pd.NA)
    wide["joint_status"] = np.select(
        [~truth_complete, ~output_complete], ["ACTUALS_NOT_COMPLETE", "MISSING_OUTPUT"], default="COMPLETE")
    return wide


def _joint_summary(group: pd.DataFrame, object_key: list[str], total: float) -> dict[str, Any]:
    ready = bool(len(group) and group.full_core_joint_ready.all())
    known_actual = _known_sum(group.actual_d01_60)
    result: dict[str, Any] = {
        "score_status": COMPLETE if ready else ("INCOMPLETE" if group.truth_complete.any() else "WAITING_ACTUALS"),
        "formal_scoring_allowed": ready, "fixed_n": len(group),
        "output_complete_n": int(group.output_complete.sum()),
        "truth_complete_n": int(group.truth_complete.sum()),
        "joint_scorable_n": int((group.truth_complete & group.output_complete).sum()),
        "known_actual": known_actual,
        "joint_hit_n": np.nan, "joint_hit_fixed_share": np.nan,
        "joint_hit_actual": np.nan, "joint_hit_actual_share": np.nan,
        "full_store_actual_units": total, "joint_hit_actual_full_store_share": np.nan,
        "products": int(group[["shop", "spu"]].drop_duplicates().shape[0]),
        "periods": int(group.issue_date.nunique()) if "issue_date" in object_key else (1 if len(group) else 0),
    }
    if ready:
        hit = group.joint_hit.fillna(False).astype(bool)
        hit_actual = float(group.loc[hit, "actual_d01_60"].sum())
        result.update(joint_hit_n=int(hit.sum()), joint_hit_fixed_share=float(hit.mean()),
                      joint_hit_actual=hit_actual, joint_hit_actual_share=_ratio(hit_actual, known_actual),
                      joint_hit_actual_full_store_share=_ratio(hit_actual, total))
    return result


def score_windows(predictions: pd.DataFrame, truth: pd.DataFrame, paired_keys: pd.DataFrame,
                  full_store_totals: Mapping[str, Any] | None = None) -> dict[str, pd.DataFrame]:
    """Return details, metrics, joint_objects, joint_summary, and coverage.

    Scope is a frozen full-core population (all objects present in predictions),
    with an explicit fixed paired subset.  No six-model intersection is built.
    Missing rows for a model become missing outputs, not omitted objects.
    Three-window joint hits always use actual d01_60 once as their sales weight.
    Hit sales share uses *all known actuals in that fixed scope*, including the
    actuals of missing-output objects, matching the accepted historic metric.
    """
    detail, truth_complete, object_key = _assemble(predictions, truth, paired_keys)
    totals = {w: _store_total(full_store_totals, w) for w in WINDOWS}
    joint = _joint_objects(detail, object_key)
    metric_rows, joint_rows = [], []
    for scope in SCOPES:
        d = detail if scope == "FULL_CORE" else detail[detail.paired]
        j = joint if scope == "FULL_CORE" else joint[joint.paired]
        for (model, window), group in d.groupby(["model", "window"], sort=True, dropna=False):
            metric_rows.append(dict(scope=scope, slice="overall", model=model, window=window,
                                    **_metrics(group, object_key, totals[window])))
        for model, group in j.groupby("model", sort=True, dropna=False):
            joint_rows.append(dict(scope=scope, slice="overall", model=model,
                                   **_joint_summary(group, object_key, totals["d01_60"])))
    batch_key = (["issue_date"] if "issue_date" in object_key else []) + ["window"]
    coverage_rows = []
    for values, group in truth_complete.groupby(batch_key, sort=True, dropna=False):
        if len(batch_key) == 1:
            values = (values,)
        coverage_rows.append(dict(zip(batch_key, values)) | dict(
            full_core_n=len(group), complete_truth_n=int(group.truth_complete.sum()),
            incomplete_truth_n=int((~group.truth_complete).sum()),
            explicit_zero_n=int(group.scoring_actual.eq(0).sum()),
            known_actual_units=_known_sum(group.scoring_actual), score_status=_state(group.truth_complete),
            actual_status_counts=group.actual_status.value_counts(dropna=False).to_dict()))
    return {"details": detail, "metrics": pd.DataFrame(metric_rows), "joint_objects": joint,
            "joint_summary": pd.DataFrame(joint_rows), "coverage": pd.DataFrame(coverage_rows)}
