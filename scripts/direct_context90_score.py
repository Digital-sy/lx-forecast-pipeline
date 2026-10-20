"""Evaluate one frozen feature increment against immutable DIRECT and F28."""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

try:
    from scripts import direct_context90_experiment as exp
except ModuleNotFoundError:
    import direct_context90_experiment as exp

KEY = ["issue_date", "shop", "spu"]
WINDOWS = {"d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
BASE, CANDIDATE = "DIRECT30x2", "DIRECT_CONTEXT90"


def metric(frame):
    valid = frame.actual.notna() & frame.prediction.notna()
    g = frame[valid]
    delta = g.prediction-g.actual
    actual = float(g.actual.sum())
    known = float(frame.actual.sum(min_count=1))
    hit = valid & frame.actual.gt(0) & ((frame.prediction-frame.actual).abs()/frame.actual).le(.2)
    return {
        "fixed_n": len(frame), "output_n": int(frame.prediction.notna().sum()),
        "truth_n": int(frame.actual.notna().sum()), "scored_n": len(g),
        "positive_truth_n": int(frame.actual.gt(0).sum()), "zero_truth_n": int(frame.actual.eq(0).sum()),
        "unknown_truth_n": int(frame.actual.isna().sum()), "missing_output_n": int(frame.prediction.isna().sum()),
        "actual_units": actual, "known_fixed_actual_units": known,
        "prediction_units": float(g.prediction.sum()), "ae": float(delta.abs().sum()),
        "wape": float(delta.abs().sum()/actual) if actual else np.nan,
        "bias": float(delta.sum()/actual) if actual else np.nan,
        "over_units": float(delta.clip(lower=0).sum()), "under_units": float((-delta).clip(lower=0).sum()),
        "over_rate": float(delta.clip(lower=0).sum()/actual) if actual else np.nan,
        "hit_n": int(hit.sum()), "hit_fixed_share": float(hit.mean()) if len(frame) else np.nan,
        "hit_positive_share": float(hit.sum()/frame.actual.gt(0).sum()) if frame.actual.gt(0).any() else np.nan,
        "hit_actual": float(frame.loc[hit, "actual"].sum()),
        "hit_actual_share": float(frame.loc[hit, "actual"].sum()/known) if known else np.nan,
        "zero_truth_ae": float(delta[g.actual.eq(0)].abs().sum()),
        "products": len(frame[["shop", "spu"]].drop_duplicates()), "periods": frame.issue_date.nunique(),
    }


def daily_truth(daily, core, grid):
    """Independent date reindex/sum, not archived aggregate truth as input."""
    columns = ["ds", "shop", "spu", "y", "mapped", "sales_state"]
    d = daily[columns].copy()
    if d.duplicated(["shop", "spu", "ds"]).any():
        raise ValueError("DAILY_DUPLICATE")
    valid = d.mapped & d.y.ge(0) & np.isfinite(d.y) & d.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"])
    d["checked_y"] = d.y.where(valid)
    groups = {key: group.set_index("ds").checked_y for key, group in d.groupby(["shop", "spu"])}
    matured = dict(zip(grid.issue_date, grid.calendar_mature))
    rows = []
    for r in core.itertuples():
        source = groups.get((r.shop, r.spu), pd.Series(dtype=float))
        for name, (a, b) in WINDOWS.items():
            dates = pd.date_range(pd.Timestamp(r.issue_date)+pd.Timedelta(days=a),
                                  pd.Timestamp(r.issue_date)+pd.Timedelta(days=b))
            values = source.reindex(dates)
            missing = int(values.isna().sum())
            status = "NOT_MATURED" if not matured[r.issue_date] else "MISSING_DAILY_TRUTH" if missing else "OBSERVED"
            actual = float(values.sum()) if status == "OBSERVED" else np.nan
            rows.append({"issue_date": r.issue_date, "shop": r.shop, "spu": r.spu,
                         "window": name, "target_start": str(dates[0].date()), "target_end": str(dates[-1].date()),
                         "actual": actual, "truth_status": status, "unknown_days": missing})
    return pd.DataFrame(rows)


def paired_objects(scores):
    pieces = []
    for model, g in scores.groupby("model"):
        pivot = g.pivot(index=KEY, columns="window", values=["actual", "prediction"])
        z = pd.DataFrame(index=pivot.index)
        for w in WINDOWS:
            z["actual_"+w] = pivot["actual"][w]
            z["prediction_"+w] = pivot["prediction"][w]
            z["hit_"+w] = (pivot["actual"][w].gt(0) &
                           ((pivot["prediction"][w]-pivot["actual"][w]).abs()/pivot["actual"][w]).le(.2))
        z["joint_hit"] = z[["hit_"+w for w in WINDOWS]].all(axis=1)
        z["output_complete"] = z[["prediction_"+w for w in WINDOWS]].notna().all(axis=1)
        z["truth_complete"] = z[["actual_"+w for w in WINDOWS]].notna().all(axis=1)
        z["model"] = model
        pieces.append(z.reset_index())
    return pd.concat(pieces, ignore_index=True)


def joint_summary(g):
    actual = float(g.actual_d01_60.sum())
    hits = g.joint_hit
    return {"fixed_n": len(g), "output_complete_n": int(g.output_complete.sum()),
            "truth_complete_n": int(g.truth_complete.sum()), "joint_hit_n": int(hits.sum()),
            "joint_hit_fixed_share": float(hits.mean()) if len(g) else np.nan,
            "joint_hit_actual": float(g.loc[hits, "actual_d01_60"].sum()),
            "known_actual": actual,
            "joint_hit_actual_share": float(g.loc[hits, "actual_d01_60"].sum()/actual) if actual else np.nan}


def score(out, diagnostic, destination):
    out, diagnostic, dest = map(lambda x: Path(x).resolve(), (out, diagnostic, destination))
    if dest.exists():
        raise ValueError("NEW_SCORING_DIRECTORY_REQUIRED")
    prepared = exp.read_json(out/"prepare_manifest.json")
    exp.verify_hashes(prepared["sources"])
    effects = exp.read_json(out/"effects_manifest.json")
    for name, value in effects["files"].items():
        if exp.sha(out/name) != value:
            raise ValueError("EFFECT_CHANGED")
    conf = exp.read_json(out/"configuration.json")
    archive = Path(conf["archive"])
    meta_path = diagnostic/"metadata_resolved.csv"
    dm = exp.read_json(diagnostic/"run_manifest.json")
    if exp.sha(meta_path) != dm["files"]["metadata_resolved.csv"]:
        raise ValueError("METADATA_CHANGED")
    meta = pd.read_csv(meta_path)[["shop", "spu", "label_basis", "current_category", "current_season"]]
    if meta.duplicated(["shop", "spu"]).any():
        raise ValueError("METADATA_DUPLICATE")
    source_files = [out/"effects_manifest.json", out/"prepare_manifest.json", meta_path,
                    diagnostic/"run_manifest.json", out/"observation_groups.parquet"]
    sources = {str(p): exp.sha(p) for p in source_files}
    started = exp.now()
    core = pd.read_parquet(out/"prepared/core.parquet")
    grid = pd.read_csv(out/"prepared/issue_grid.csv")
    truth = daily_truth(pd.read_parquet(archive/"inputs/daily.parquet"), core, grid)
    old = pd.read_csv(archive/"scoring/scores.csv")
    old = old[old.cohort_id.eq("monthly_core") & old.model.isin([BASE, "F28"])]
    prior_truth = old[old.model.eq(BASE)][KEY+["window", "actual", "truth_status"]]
    reconciled = truth.merge(prior_truth, on=KEY+["window"], suffixes=("", "_old"), validate="one_to_one")
    if len(reconciled) != len(truth) or not np.allclose(reconciled.actual, reconciled.actual_old,
                                                       rtol=0, atol=0, equal_nan=True):
        raise ValueError("DAILY_TRUTH_RECONCILIATION")
    if not reconciled.truth_status.eq(reconciled.truth_status_old).all():
        raise ValueError("TRUTH_STATUS_RECONCILIATION")
    raw_predictions = []
    for issue in grid[grid.calendar_mature].issue_date:
        tables = {}
        for segment in ["M1", "M2"]:
            p = pd.read_parquet(out/"effects"/f"{issue}_{segment}"/"predictions.parquet").set_index(["shop", "spu"])
            tables[segment] = p
        for r in core[core.issue_date.eq(issue)].itertuples():
            a, b = tables["M1"].loc[(r.shop, r.spu)], tables["M2"].loc[(r.shop, r.spu)]
            for window, parts in [("d01_30", [a]), ("d31_60", [b]), ("d01_60", [a, b])]:
                value = float(sum(p.prediction for p in parts)) if all(pd.notna(p.prediction) for p in parts) else np.nan
                status = "OK" if np.isfinite(value) else "|".join(sorted({p.prediction_status for p in parts if pd.isna(p.prediction)}))
                raw_predictions.append({"issue_date": issue, "shop": r.shop, "spu": r.spu,
                                        "window": window, "prediction": value, "prediction_status": status,
                                        "model": CANDIDATE})
    for r in core[~core.issue_date.isin(grid[grid.calendar_mature].issue_date)].itertuples():
        for window in WINDOWS:
            raw_predictions.append({"issue_date": r.issue_date, "shop": r.shop, "spu": r.spu,
                                    "window": window, "prediction": np.nan,
                                    "prediction_status": "NOT_MATURED", "model": CANDIDATE})
    predictions = pd.concat([pd.DataFrame(raw_predictions),
                             old[KEY+["window", "model", "prediction", "prediction_status"]]], ignore_index=True)
    if predictions.duplicated(KEY+["window", "model"]).any():
        raise ValueError("OUTPUT_DUPLICATE")
    scores = predictions.merge(truth, on=KEY+["window"], validate="many_to_one")
    scores = scores.merge(core[KEY+["maturity", "lifecycle", "selection_qty"]], on=KEY, validate="many_to_one")
    scores = scores.merge(meta, on=["shop", "spu"], how="left", validate="many_to_one")
    obs = pd.read_parquet(out/"observation_groups.parquet")
    scores = scores.merge(obs[KEY+["decline_group"]], on=KEY, how="left", validate="many_to_one")
    scores["current_category"] = scores.current_category.fillna("UNKNOWN")
    scores["current_season"] = scores.current_season.fillna("UNKNOWN")
    scores["decline_group"] = scores.decline_group.fillna("NOT_MATURED")
    scores["error"] = scores.prediction-scores.actual
    scores["absolute_error"] = scores.error.abs()
    scores["over_units"] = scores.error.clip(lower=0)
    scores["under_units"] = (-scores.error).clip(lower=0)
    scores["hit"] = scores.actual.gt(0) & (scores.error.abs()/scores.actual).le(.2)
    objects = paired_objects(scores)
    object_dims = scores.drop_duplicates(KEY)[KEY+["maturity", "lifecycle", "current_category", "current_season", "decline_group"]]
    objects = objects.merge(object_dims, on=KEY, validate="many_to_one")
    original_pair = objects[objects.model.eq(BASE) & objects.output_complete][KEY]
    scores = scores.merge(original_pair.assign(original_direct_output=True), on=KEY, how="left", validate="many_to_one")
    objects = objects.merge(original_pair.assign(original_direct_output=True), on=KEY, how="left", validate="many_to_one")
    scores["original_direct_output"] = scores.original_direct_output.eq(True)
    objects["original_direct_output"] = objects.original_direct_output.eq(True)
    mature_issues = set(grid[grid.calendar_mature].issue_date)
    mature = scores[scores.issue_date.isin(mature_issues)].copy()
    mo = objects[objects.issue_date.isin(mature_issues)].copy()
    dims = {"overall": [], "issue": ["issue_date"], "shop": ["shop"], "maturity": ["maturity"],
            "category": ["current_category"], "season": ["current_season"],
            "shop_category": ["shop", "current_category"], "shop_season": ["shop", "current_season"],
            "decline_group": ["decline_group"]}
    metrics, joint = [], []
    for scope, mask in [("FULL_CORE", pd.Series(True, index=mature.index)),
                        ("ORIGINAL_DIRECT_OUTPUT", mature.original_direct_output)]:
        sub = mature[mask]
        oo = mo if scope == "FULL_CORE" else mo[mo.original_direct_output]
        for sl, columns in dims.items():
            for vals, g in sub.groupby(["model", "window"]+columns, dropna=False):
                metrics.append({"scope": scope, "slice": sl, **dict(zip(["model", "window"]+columns, vals)), **metric(g)})
            for vals, g in oo.groupby(["model"]+columns, dropna=False):
                joint.append({"scope": scope, "slice": sl, **dict(zip(["model"]+columns, vals)), **joint_summary(g)})
    metrics, joint = pd.DataFrame(metrics), pd.DataFrame(joint)
    detail = mo[mo.model.eq(BASE)].drop(columns="model").merge(
        mo[mo.model.eq(CANDIDATE)][KEY+["prediction_"+w for w in WINDOWS]+["joint_hit", "output_complete"]],
        on=KEY, suffixes=("_base", "_candidate"), validate="one_to_one")
    detail["hit_migration"] = np.select(
        [detail.joint_hit_base & detail.joint_hit_candidate, ~detail.joint_hit_base & detail.joint_hit_candidate,
         detail.joint_hit_base & ~detail.joint_hit_candidate], ["RETAINED", "GAINED", "LOST"], default="NEITHER")
    for w in WINDOWS:
        actual = detail["actual_"+w]
        old_error = detail["prediction_"+w+"_base"]-actual
        new_error = detail["prediction_"+w+"_candidate"]-actual
        detail["ae_reduction_"+w] = old_error.abs()-new_error.abs()
        detail["extra_over_"+w] = new_error.clip(lower=0)-old_error.clip(lower=0)
        detail["under_reduction_"+w] = (-old_error).clip(lower=0)-(-new_error).clip(lower=0)
    detail["ae_change_status"] = np.select(
        [detail.ae_reduction_d01_60.isna(), detail.ae_reduction_d01_60.gt(0),
         detail.ae_reduction_d01_60.lt(0)], ["UNSCORED", "IMPROVED", "DEGRADED"], default="UNCHANGED")
    changes = []
    for scope, g in [("FULL_CORE", detail), ("ORIGINAL_DIRECT_OUTPUT", detail[detail.original_direct_output])]:
        total = g.actual_d01_60.sum()
        for kind in ["hit_migration", "ae_change_status"]:
            for val, subgroup in g.groupby(kind):
                changes.append({"scope": scope, "kind": kind, "state": val, "n": len(subgroup),
                                "products": len(subgroup[["shop", "spu"]].drop_duplicates()),
                                "actual_units": float(subgroup.actual_d01_60.sum()),
                                "actual_share": float(subgroup.actual_d01_60.sum()/total)})
    product = detail[detail.original_direct_output].groupby(["shop", "spu"], as_index=False).agg(
        actual_units=("actual_d01_60", "sum"), ae_reduction=("ae_reduction_d01_60", "sum"),
        extra_over=("extra_over_d01_60", "sum"), periods=("issue_date", "nunique"))
    product["status"] = np.where(product.ae_reduction.gt(0), "IMPROVED",
                                  np.where(product.ae_reduction.lt(0), "DEGRADED", "UNCHANGED"))
    paired_metric = metrics[(metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT")) & metrics.slice.eq("overall") & metrics.window.eq("d01_60")].set_index("model")
    paired_joint = joint[joint.scope.eq("ORIGINAL_DIRECT_OUTPUT") & joint.slice.eq("overall")].set_index("model")
    protection = metrics[(metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT")) & metrics.slice.eq("decline_group") &
                         metrics.decline_group.eq("PRE_ISSUE_DECLINING") & metrics.window.eq("d01_60")].set_index("model")
    breadth = bool(len(protection) and protection.loc[CANDIDATE, "fixed_n"] >= 30 and
                   protection.loc[CANDIDATE, "products"] >= 10 and protection.loc[CANDIDATE, "periods"] >= 3)
    coverage_same = bool(detail.output_complete_base.eq(detail.output_complete_candidate).all())
    gates = {
        "paired_w60_wape_lower": bool(paired_metric.loc[CANDIDATE, "wape"] < paired_metric.loc[BASE, "wape"]),
        "joint_hit_count_increased": bool(paired_joint.loc[CANDIDATE, "joint_hit_n"] > paired_joint.loc[BASE, "joint_hit_n"]),
        "joint_hit_sales_share_not_lower": bool(paired_joint.loc[CANDIDATE, "joint_hit_actual_share"] >= paired_joint.loc[BASE, "joint_hit_actual_share"]),
        "declining_w60_over_rate_not_increased": bool(protection.loc[CANDIDATE, "over_rate"] <= protection.loc[BASE, "over_rate"]) if len(protection) else None,
        "output_and_core_coverage_not_reduced": bool(coverage_same and len(detail) == len(core[core.issue_date.isin(mature_issues)])),
    }
    overall = "PASS" if all(v is True for v in gates.values()) else "FAIL" if any(v is False for v in gates.values()) else "PARTIAL"
    summary = {
        "numerical_screen": overall, "gates": gates, "decline_breadth_screen_met": breadth,
        "business_evidence": "RECONSTRUCTED_DEVELOPMENT_VALIDATION_NOT_INDEPENDENT_OOS",
        "full_core_n": len(detail), "original_direct_output_n": int(detail.original_direct_output.sum()),
        "paired_w60": paired_metric.reset_index().to_dict("records"),
        "paired_joint": paired_joint.reset_index().to_dict("records"),
        "decline_w60": protection.reset_index().to_dict("records"),
        "mature_issues": len(mature_issues), "immature_core_n": int((~core.issue_date.isin(mature_issues)).sum()),
        "metadata_limitation": "CURRENT_NOT_POINT_IN_TIME; labels not used in model",
        "future_inputs": False, "old_model_retraining": False,
        "technical_notes": "Daily truths resummed independently; finite old forecasts reused, no rounding for gates.",
    }
    dest.mkdir(exist_ok=False)
    for name, frame in [
        ("scores", scores), ("independent_daily_truth", truth), ("objects", objects),
        ("metrics", metrics), ("joint_summary", joint), ("hit_and_error_changes", pd.DataFrame(changes)),
        ("paired_details", detail), ("product_changes", product),
        ("output_loss", detail[detail.output_complete_base & ~detail.output_complete_candidate]),
        ("not_matured", scores[scores.truth_status.eq("NOT_MATURED")]),
        ("missing_output", mature[mature.prediction.isna()]),
    ]:
        frame.to_parquet(dest/(name+".parquet"), index=False)
        frame.to_csv(dest/(name+".csv"), index=False, encoding="utf-8-sig")
    # JSON null preserves unapplicable columns, not invented zero.
    import json
    clean = json.loads(json.dumps(summary, default=str).replace("NaN", "null"))
    exp.dump(dest/"summary.json", clean)
    exp.verify_hashes(prepared["sources"])
    exp.verify_hashes(sources)
    exp.dump(dest/"score_manifest.json", {
        "started_at": started, "completed_at": exp.now(), "source_sha256": exp.sha(__file__),
        "sources": sources, "source_unchanged": True, "daily_truth_difference_n": 0,
        "files": {str(p.relative_to(dest)): exp.sha(p) for p in dest.iterdir() if p.is_file()}})
    print(json.dumps({"status": "SCORED", "numerical_screen": overall, "gates": gates,
                      "full_core_n": len(detail), "paired_n": int(detail.original_direct_output.sum())}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--diagnostic", required=True)
    parser.add_argument("--destination", required=True)
    score(**vars(parser.parse_args()))
