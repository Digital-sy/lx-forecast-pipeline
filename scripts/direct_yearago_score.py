"""Same-universe evaluation; support fallbacks are disclosed, never called new fits."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts import direct_yearago_experiment as e
    from scripts import direct_context90_score as original_score
except ModuleNotFoundError:
    import direct_yearago_experiment as e
    import direct_context90_score as original_score

KEY = ["issue_date", "shop", "spu"]
WINDOWS = original_score.WINDOWS
BASE, CAND = "DIRECT30x2", "DIRECT_YEARAGO"
metric, joint_summary = original_score.metric, original_score.joint_summary


def safe_json(value):
    return json.loads(json.dumps(value, default=str).replace("NaN", "null"))


def score(out, diagnostic, destination):
    out, diagnostic, dest = [Path(x).resolve() for x in (out, diagnostic, destination)]
    if dest.exists():
        raise ValueError("SCORING_DIRECTORY_EXISTS")
    conf, prepared = e.read_json(out/"configuration.json"), e.read_json(out/"prepare_manifest.json")
    archive, context = Path(conf["archive"]), Path(conf["context_archive"])
    e.verify_hashes(prepared["sources"])
    effects = e.read_json(out/"effects_manifest.json")
    e.verify_hashes({str(out/k): v for k, v in effects["files"].items()})
    source_paths = [out/"prepare_manifest.json", out/"effects_manifest.json", out/"annual_support.csv",
                    out/"observation_groups.parquet", diagnostic/"metadata_resolved.csv",
                    diagnostic/"run_manifest.json", context/"scoring/scores.parquet"]
    sources = {str(p): e.sha(p) for p in source_paths}
    dm = e.read_json(diagnostic/"run_manifest.json")
    assert sources[str(diagnostic/"metadata_resolved.csv")] == dm["files"]["metadata_resolved.csv"]
    core = pd.read_parquet(out/"prepared/core.parquet")
    grid = pd.read_csv(out/"prepared/issue_grid.csv")
    issues = grid[grid.calendar_mature].issue_date.tolist()
    truth = original_score.daily_truth(pd.read_parquet(archive/"inputs/daily.parquet"), core, grid)
    # Preserve every archived floating prediction exactly; default CSV parsing
    # can turn unchanged fallback rows into tiny artificial improvement/decline.
    old = pd.read_csv(archive/"scoring/scores.csv", float_precision="round_trip")
    old = old[old.cohort_id.eq("monthly_core") & old.model.isin([BASE, "F28"])]
    chk = truth.merge(old[old.model.eq(BASE)][KEY+["window", "actual", "truth_status"]],
                      on=KEY+["window"], suffixes=("", "_old"), validate="one_to_one")
    np.testing.assert_array_equal(chk.actual, chk.actual_old)
    assert chk.truth_status.eq(chk.truth_status_old).all() and len(chk) == len(truth)
    old_context = pd.read_parquet(context/"scoring/scores.parquet")
    old_context = old_context[old_context.model.eq("DIRECT_CONTEXT90")]
    cols = KEY+["window", "model", "prediction", "prediction_status"]
    pred_parts, origins, annual_tables = [], [], []
    for issue in issues:
        parts = {}
        for segment in ["M1", "M2"]:
            p = pd.read_parquet(out/"effects"/f"{issue}_{segment}"/"predictions.parquet")
            a = pd.read_parquet(out/"prepared"/f"{issue}_{segment}"/"inference_windows.parquet")
            p = p.merge(a[["shop", "spu", "annual_state", "operating_comparability"]],
                        on=["shop", "spu"], validate="one_to_one")
            annual_tables.append(p)
            parts[segment] = p.set_index(["shop", "spu"])
        for r in core[core.issue_date.eq(issue)].itertuples():
            a, b = [parts[s].loc[(r.shop, r.spu)] for s in ["M1", "M2"]]
            both_annual = a.prediction_origin == e.ANNUAL and b.prediction_origin == e.ANNUAL
            obj_state = a.annual_state if a.annual_state == b.annual_state else "MIXED_SEGMENT_STATES"
            origins.append({**{k: getattr(r, k) for k in KEY},
                            "M1_origin": a.prediction_origin, "M2_origin": b.prediction_origin,
                            "M1_annual_state": a.annual_state, "M2_annual_state": b.annual_state,
                            "annual_state": obj_state, "both_annual_outputs": both_annual})
            for w, rows in [("d01_30", [a]), ("d31_60", [b]), ("d01_60", [a, b])]:
                values = [x.comparison_prediction for x in rows]
                value = float(sum(values)) if all(np.isfinite(v) for v in values) else np.nan
                pred_parts.append({**{k: getattr(r, k) for k in KEY}, "window": w, "model": CAND,
                                   "prediction": value, "prediction_status": "OK" if np.isfinite(value) else a.prediction_status+"|"+b.prediction_status})
    for r in core[~core.issue_date.isin(issues)].itertuples():
        for w in WINDOWS:
            pred_parts.append({**{k: getattr(r, k) for k in KEY}, "window": w, "model": CAND,
                               "prediction": np.nan, "prediction_status": "NOT_MATURED"})
    predictions = pd.concat([pd.DataFrame(pred_parts), old[cols], old_context[cols]], ignore_index=True)
    assert not predictions.duplicated(KEY+["model", "window"]).any()
    meta = pd.read_csv(diagnostic/"metadata_resolved.csv")[["shop", "spu", "current_category", "current_season", "label_basis"]]
    obs = pd.read_parquet(out/"observation_groups.parquet")
    origin = pd.DataFrame(origins)
    scores = predictions.merge(truth, on=KEY+["window"], validate="many_to_one")
    scores = scores.merge(core[KEY+["maturity", "lifecycle", "selection_qty"]], on=KEY, validate="many_to_one")
    scores = scores.merge(meta, on=["shop", "spu"], how="left", validate="many_to_one")
    scores = scores.merge(obs, on=KEY, how="left", validate="many_to_one")
    scores = scores.merge(origin, on=KEY, how="left", validate="many_to_one")
    for c in ["current_category", "current_season", "decline_group", "annual_state"]:
        scores[c] = scores[c].fillna("UNKNOWN")
    scores["year"] = scores.issue_date.str[:4]
    scores["error"] = scores.prediction-scores.actual
    scores["absolute_error"] = scores.error.abs()
    scores["over_units"] = scores.error.clip(lower=0)
    scores["under_units"] = (-scores.error).clip(lower=0)
    scores["hit"] = scores.actual.gt(0) & (scores.error.abs()/scores.actual).le(.2)
    objects = original_score.paired_objects(scores)
    dimscols = KEY+["year", "maturity", "lifecycle", "current_category", "current_season", "decline_group",
                   "annual_state", "M1_origin", "M2_origin", "both_annual_outputs"]
    objects = objects.merge(scores.drop_duplicates(KEY)[dimscols], on=KEY, validate="many_to_one")
    pair = objects[objects.model.eq(BASE) & objects.output_complete][KEY].assign(original_direct_output=True)
    scores = scores.merge(pair, on=KEY, how="left", validate="many_to_one")
    objects = objects.merge(pair, on=KEY, how="left", validate="many_to_one")
    for f in [scores, objects]:
        f["original_direct_output"] = f.original_direct_output.eq(True)
        f["both_annual_outputs"] = f.both_annual_outputs.eq(True)
    mature, mo = scores[scores.issue_date.isin(issues)].copy(), objects[objects.issue_date.isin(issues)].copy()
    dims = {"overall": [], "year": ["year"], "issue": ["issue_date"], "shop": ["shop"],
            "maturity": ["maturity"], "category": ["current_category"], "season": ["current_season"],
            "shop_category": ["shop", "current_category"], "shop_season": ["shop", "current_season"],
            "annual_state": ["annual_state"], "decline_group": ["decline_group"],
            "issue_annual_state": ["issue_date", "annual_state"]}
    metrics, joints = [], []
    for scope in ["FULL_CORE", "ORIGINAL_DIRECT_OUTPUT", "BOTH_ANNUAL_OUTPUTS"]:
        sub = mature if scope == "FULL_CORE" else mature[mature.original_direct_output] if scope == "ORIGINAL_DIRECT_OUTPUT" else mature[mature.both_annual_outputs]
        oo = mo if scope == "FULL_CORE" else mo[mo.original_direct_output] if scope == "ORIGINAL_DIRECT_OUTPUT" else mo[mo.both_annual_outputs]
        for name, columns in dims.items():
            for vals, g in sub.groupby(["model", "window"]+columns, dropna=False):
                metrics.append({"scope": scope, "slice": name, **dict(zip(["model", "window"]+columns, vals)), **metric(g)})
            for vals, g in oo.groupby(["model"]+columns, dropna=False):
                joints.append({"scope": scope, "slice": name, **dict(zip(["model"]+columns, vals)), **joint_summary(g)})
    metrics, joints = pd.DataFrame(metrics), pd.DataFrame(joints)
    detail = mo[mo.model.eq(BASE)].drop(columns="model").merge(
        mo[mo.model.eq(CAND)][KEY+["prediction_"+w for w in WINDOWS]+["joint_hit", "output_complete"]],
        on=KEY, suffixes=("_base", "_candidate"), validate="one_to_one")
    detail["hit_migration"] = np.select(
        [detail.joint_hit_base & detail.joint_hit_candidate, ~detail.joint_hit_base & detail.joint_hit_candidate,
         detail.joint_hit_base & ~detail.joint_hit_candidate], ["RETAINED", "GAINED", "LOST"], default="NEITHER")
    for w in WINDOWS:
        a = detail["actual_"+w]
        olderr = detail["prediction_"+w+"_base"]-a
        newerr = detail["prediction_"+w+"_candidate"]-a
        detail["ae_reduction_"+w] = olderr.abs()-newerr.abs()
        detail["extra_over_"+w] = newerr.clip(lower=0)-olderr.clip(lower=0)
        detail["under_reduction_"+w] = (-olderr).clip(lower=0)-(-newerr).clip(lower=0)
    detail["ae_state"] = np.select([detail.ae_reduction_d01_60.isna(), detail.ae_reduction_d01_60.gt(0),
                                    detail.ae_reduction_d01_60.lt(0)], ["UNSCORED", "IMPROVED", "DEGRADED"], default="UNCHANGED")
    changes = []
    for scope, q in [("FULL_CORE", detail), ("ORIGINAL_DIRECT_OUTPUT", detail[detail.original_direct_output]),
                     ("BOTH_ANNUAL_OUTPUTS", detail[detail.both_annual_outputs])]:
        for kind in ["hit_migration", "ae_state"]:
            for val, g in q.groupby(kind):
                changes.append({"scope": scope, "kind": kind, "state": val, "n": len(g),
                                "products": len(g[["shop", "spu"]].drop_duplicates()),
                                "actual": float(g.actual_d01_60.sum()),
                                "actual_share": float(g.actual_d01_60.sum()/q.actual_d01_60.sum())})
    product = detail[detail.original_direct_output].groupby(["shop", "spu"], as_index=False).agg(
        actual=("actual_d01_60", "sum"), ae_reduction=("ae_reduction_d01_60", "sum"),
        extra_over=("extra_over_d01_60", "sum"), periods=("issue_date", "nunique"))
    product["state"] = np.select([product.ae_reduction.gt(0), product.ae_reduction.lt(0)],
                                 ["IMPROVED", "DEGRADED"], default="UNCHANGED")
    paired_m = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("overall")]
    pm60 = paired_m[paired_m.window.eq("d01_60")].set_index("model")
    pj = joints[joints.scope.eq("ORIGINAL_DIRECT_OUTPUT") & joints.slice.eq("overall")].set_index("model")
    decline = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("decline_group") &
                      metrics.decline_group.eq("PRE_ISSUE_DECLINING") & metrics.window.eq("d01_60")].set_index("model")
    gates = {
        "paired_w60_wape_lower": bool(pm60.loc[CAND, "wape"] < pm60.loc[BASE, "wape"]),
        "joint_hit_count_increased": bool(pj.loc[CAND, "joint_hit_n"] > pj.loc[BASE, "joint_hit_n"]),
        "joint_hit_sales_share_not_lower": bool(pj.loc[CAND, "joint_hit_actual_share"] >= pj.loc[BASE, "joint_hit_actual_share"]),
        "declining_w60_over_rate_not_increased": bool(decline.loc[CAND, "over_rate"] <= decline.loc[BASE, "over_rate"]) if len(decline) else None,
        "output_and_core_coverage_not_reduced": bool(detail.output_complete_base.eq(detail.output_complete_candidate).all() and len(detail) == len(core[core.issue_date.isin(issues)])),
    }
    segment_predictions = pd.concat(annual_tables, ignore_index=True)
    coverage = []
    for (segment, state, ori), g in segment_predictions.groupby(["segment", "annual_state", "prediction_origin"]):
        w = "d01_30" if segment == "M1" else "d31_60"
        matched = g.merge(truth[truth.window.eq(w)][KEY+["actual"]], on=KEY, validate="one_to_one")
        denominator = truth[truth.window.eq(w) & truth.issue_date.isin(issues)].actual.sum()
        coverage.append({"segment": segment, "annual_state": state, "prediction_origin": ori,
                         "n": len(g), "actual": float(matched.actual.sum()),
                         "actual_share_of_full_segment": float(matched.actual.sum()/denominator)})
    sensitivity = []
    for label, q in [("ALL", detail[detail.original_direct_output]),
                     ("EXCLUDE_2025_11", detail[detail.original_direct_output & detail.issue_date.ne("2025-11-02")])]:
        total = q.actual_d01_60.sum()
        sensitivity.append({"scope": label, "n": len(q), "ae_reduction": q.ae_reduction_d01_60.sum(),
                            "wape_reduction": q.ae_reduction_d01_60.sum()/total,
                            "extra_over": q.extra_over_d01_60.sum(),
                            "joint_net": int(q.joint_hit_candidate.sum()-q.joint_hit_base.sum()),
                            "joint_actual_net": q.loc[q.joint_hit_candidate, "actual_d01_60"].sum()-q.loc[q.joint_hit_base, "actual_d01_60"].sum()})
    summary = {
        "numerical_screen": "PASS" if all(v is True for v in gates.values()) else "FAIL" if any(v is False for v in gates.values()) else "PARTIAL",
        "business_evidence": "PARTIAL; repeated reconstructed development, limited annual operating comparability",
        "gates": gates, "full_core_n": len(detail), "original_direct_output_n": int(detail.original_direct_output.sum()),
        "both_annual_outputs_n": int(detail.both_annual_outputs.sum()), "original_missing_output_n": int((~detail.original_direct_output).sum()),
        "immature_n": int((~core.issue_date.isin(issues)).sum()), "formal_fits": effects["formal_fits"],
        "baseline_reused_segments": int(segment_predictions.prediction_origin.eq(e.FALLBACK).sum()),
        "annual_generated_segments": int(segment_predictions.prediction_origin.eq(e.ANNUAL).sum()),
        "paired_windows": paired_m.to_dict("records"), "paired_joint": pj.reset_index().to_dict("records"),
        "decline_w60": decline.reset_index().to_dict("records"), "stability": sensitivity,
        "source_dates": "daily 2024-01-01 through 2026-10-01; issue cutoff enforced separately",
    }
    dest.mkdir(exist_ok=False)
    for name, frame in [
        ("scores", scores), ("objects", objects), ("truth", truth), ("metrics", metrics),
        ("joint_summary", joints), ("paired_details", detail), ("product_changes", product),
        ("hit_changes", pd.DataFrame(changes)), ("inference_coverage", pd.DataFrame(coverage)),
        ("segment_predictions", segment_predictions), ("origins", origin),
        ("stability", pd.DataFrame(sensitivity)), ("missing_outputs", mature[mature.prediction.isna()]),
        ("not_matured", scores[~scores.issue_date.isin(issues)]),
    ]:
        frame.to_parquet(dest/(name+".parquet"), index=False)
        frame.to_csv(dest/(name+".csv"), index=False, encoding="utf-8-sig")
    e.dump(dest/"summary.json", safe_json(summary))
    e.verify_hashes(sources)
    e.verify_hashes(prepared["sources"])
    e.dump(dest/"score_manifest.json", {
        "at": e.now(), "script_sha256": e.sha(__file__), "sources": sources,
        "truth_reconciliation_differences": 0,
        "files": {str(p.relative_to(dest)): e.sha(p) for p in dest.iterdir() if p.is_file()}})
    print(json.dumps({"status": "SCORED", "gates": gates, "both_annual": summary["both_annual_outputs_n"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for arg in ["out", "diagnostic", "destination"]:
        parser.add_argument("--"+arg, required=True)
    score(**vars(parser.parse_args()))
