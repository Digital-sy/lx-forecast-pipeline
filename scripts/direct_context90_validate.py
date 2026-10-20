"""Independent arithmetic checks; no fitting or source mutation."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import numpy as np
import pandas as pd
from lightgbm import Booster

try:
    from scripts import direct_context90_experiment as exp
except ModuleNotFoundError:
    import direct_context90_experiment as exp

KEY = ["issue_date", "shop", "spu"]


def validate(out, reviewed, destination):
    out, reviewed, destination = map(lambda x: Path(x).resolve(), (out, reviewed, destination))
    if destination.exists():
        raise ValueError("NEW_VALIDATION_DIRECTORY_REQUIRED")
    config = exp.read_json(out/"configuration.json")
    archive = Path(config["archive"])
    prepared = exp.read_json(out/"prepare_manifest.json")
    effects = exp.read_json(out/"effects_manifest.json")
    score_manifest = exp.read_json(reviewed/"score_manifest.json")
    exp.verify_hashes(prepared["sources"])
    exp.verify_hashes(score_manifest["sources"])
    for base, manifest in [(out, prepared), (out, effects), (reviewed, score_manifest)]:
        for name, digest in manifest["files"].items():
            if exp.sha(base/name) != digest:
                raise ValueError("ARTIFACT_CHANGED:" + name)
    daily = pd.read_parquet(archive/"inputs/daily.parquet")
    raw = {key: g.set_index("ds") for key, g in daily.groupby(["shop", "spu"])}
    models, feature_checks, old_checks, future_checks = 0, 0, 0, 0
    records = []
    for slot in sorted((out/"prepared").glob("*_M*")):
        train = pd.read_parquet(slot/"train.parquet")
        inference = pd.read_parquet(slot/"inference.parquet")
        issue, segment = slot.name.rsplit("_", 1)
        checkpoint = exp.read_json(out/"effects"/slot.name/"checkpoint.json")
        model = Booster(model_file=os.path.relpath(out/"effects"/slot.name/"model.txt", exp.ROOT))
        if model.feature_name() != exp.FEATURES or checkpoint["params"] != config["lightgbm"]:
            raise ValueError("MODEL_CONFIGURATION")
        models += 1
        original_audit = exp.read_json(archive/"effects/slots"/slot.name/"training_audit.json")
        assert exp.frozen.training_audit(train, exp.KEY, exp.AUDIT_COLUMNS) == original_audit
        assert len(train) == original_audit["rows"]
        assert train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all()
        shift = 30 if segment == "M1" else 60
        assert (train.simulated_issue_date+pd.Timedelta(days=shift)).eq(train.label_end).all()
        assert (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all()
        old_checks += len(train)
        take = np.unique(np.linspace(0, len(train)-1, min(64, len(train))).astype(int))
        samples = pd.concat([train.iloc[take], inference], ignore_index=True)
        for row in samples.itertuples():
            data = raw[(row.shop, row.spu)]
            for days in [56, 90]:
                dates = pd.date_range(row.input_cutoff-pd.Timedelta(days=days-1), row.input_cutoff)
                hist = data.reindex(dates)
                good = hist.mapped.eq(True) & hist.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]) & hist.y.ge(0) & np.isfinite(hist.y)
                complete = bool(good.all())
                assert getattr(row, f"history{days}_complete") == int(complete)
                avg = float(hist.y.mean()) if complete else np.nan
                assert np.isclose(avg, getattr(row, "m"+str(days)), rtol=1e-12, atol=1e-10, equal_nan=True)
                if complete:
                    recent, previous = hist.y.iloc[-28:].mean(), hist.y.iloc[:-28].mean()
                    change = (recent-previous)/(recent+previous) if recent+previous > 0 else 0.
                else:
                    change = np.nan
                assert np.isclose(change, getattr(row, "contrast"+str(days)), rtol=1e-12, atol=1e-10, equal_nan=True)
                feature_checks += 1
        # Predict from saved models, never refit. Randomness is not introduced.
        actual_pred = pd.read_parquet(out/"effects"/slot.name/"predictions.parquet")
        mask = inference.input_status.eq("OK")
        recalculated = np.maximum(model.predict(inference.loc[mask, exp.FEATURES]), 0)
        np.testing.assert_allclose(recalculated, actual_pred.loc[mask, "prediction"], rtol=0, atol=0)
        # Real-data perturbation: all post-cutoff values in each selected series.
        # Independently reconstruct model fields from raw dates before/after it.
        if issue in ["2025-01-02", "2026-08-02"]:
            for row in inference[mask].iloc[:8].itertuples():
                data = raw[(row.shop, row.spu)].copy()
                changed = data.copy()
                changed.loc[changed.index > row.input_cutoff, "y"] = 10**8
                feature_rows = []
                for frame in [data, changed]:
                    dates = pd.date_range(row.input_cutoff-pd.Timedelta(days=89), row.input_cutoff)
                    hist = frame.reindex(dates)
                    y28 = hist.y.iloc[-28:].to_numpy()
                    old = exp.direct.features_from_history(y28, exp.frozen.SHOPS.index(row.shop), row.label_start)
                    for n in [56, 90]:
                        part = hist.iloc[-n:]
                        ok = (part.mapped.eq(True) & part.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]) &
                              part.y.ge(0) & np.isfinite(part.y)).all()
                        a, b = part.y.iloc[-28:].mean(), part.y.iloc[:-28].mean()
                        old["m"+str(n)] = part.y.mean() if ok else np.nan
                        old["contrast"+str(n)] = (a-b)/(a+b) if ok and a+b > 0 else 0. if ok else np.nan
                        old[f"history{n}_complete"] = int(ok)
                    feature_rows.append(old)
                a, b = pd.DataFrame(feature_rows).reindex(columns=exp.FEATURES).iloc[[0]], pd.DataFrame(feature_rows).reindex(columns=exp.FEATURES).iloc[[1]]
                np.testing.assert_allclose(a.to_numpy(), b.to_numpy(), rtol=0, atol=0, equal_nan=True)
                np.testing.assert_allclose(a.to_numpy()[0], inference.loc[inference.shop.eq(row.shop)&inference.spu.eq(row.spu), exp.FEATURES].to_numpy()[0],
                                           rtol=1e-12, atol=1e-10, equal_nan=True)
                np.testing.assert_array_equal(model.predict(a), model.predict(b))
                future_checks += 1
        records.append({"issue_date": issue, "segment": segment, "train_n": len(train),
                        "old_columns_exact": True, "saved_model_prediction_exact": True,
                        "inference_n": len(inference), "all_inference_calendar_checked": True})
        print(json.dumps({"validated_slot": slot.name, "models": models}), flush=True)
    truth = pd.read_parquet(reviewed/"independent_daily_truth.parquet")
    truth_checks = 0
    for row in truth.itertuples():
        if row.truth_status == "NOT_MATURED":
            assert pd.isna(row.actual)
            continue
        g = raw[(row.shop, row.spu)]
        subset = g.loc[(g.index >= pd.Timestamp(row.target_start)) & (g.index <= pd.Timestamp(row.target_end))]
        length = (pd.Timestamp(row.target_end)-pd.Timestamp(row.target_start)).days+1
        known = (subset.y.ge(0) & np.isfinite(subset.y) & subset.mapped.eq(True) &
                 subset.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"])).all() and len(subset) == length
        if known:
            value = sum(float(v) for v in subset.y)
            assert value == row.actual
        else:
            assert pd.isna(row.actual)
        truth_checks += 1
    scores = pd.read_parquet(reviewed/"scores.parquet")
    objects = pd.read_parquet(reviewed/"objects.parquet")
    # Check M1/M2 summation, hit classification and one-count denominators independently.
    for r in objects.itertuples():
        pred = [getattr(r, "prediction_"+w) for w in ["d01_30", "d31_60", "d01_60"]]
        acts = [getattr(r, "actual_"+w) for w in ["d01_30", "d31_60", "d01_60"]]
        if r.model in ["DIRECT30x2", "DIRECT_CONTEXT90"] and all(np.isfinite(pred)):
            assert np.isclose(pred[0]+pred[1], pred[2], rtol=1e-15, atol=1e-9)
        if all(np.isfinite(acts)):
            assert acts[0]+acts[1] == acts[2]
        joint = all(np.isfinite(p) and np.isfinite(a) and a > 0 and abs(p-a)/a <= .2 for p, a in zip(pred, acts))
        assert joint == r.joint_hit
    metrics = pd.read_parquet(reviewed/"metrics.parquet")
    for r in metrics[metrics.slice.eq("overall")].itertuples():
        g = scores[(scores.model == r.model) & (scores.window == r.window) & scores.truth_status.ne("NOT_MATURED")]
        if r.scope == "ORIGINAL_DIRECT_OUTPUT":
            g = g[g.original_direct_output]
        valid = [(float(a), float(p)) for a, p in zip(g.actual, g.prediction) if np.isfinite(a) and np.isfinite(p)]
        denominator = sum(a for a, _ in valid)
        ae = sum(abs(p-a) for a, p in valid)
        over = sum(max(p-a, 0) for a, p in valid)
        under = sum(max(a-p, 0) for a, p in valid)
        assert len(g) == r.fixed_n
        np.testing.assert_allclose([denominator, ae, over, under, ae/denominator],
                                   [r.actual_units, r.ae, r.over_units, r.under_units, r.wape],
                                   rtol=1e-12, atol=1e-8)
    assert models == 40 == len(list((out/"effects").glob("*/fit_started.json")))
    exp.verify_hashes(prepared["sources"])
    exp.verify_hashes(score_manifest["sources"])
    destination.mkdir(exist_ok=False)
    pd.DataFrame(records).to_csv(destination/"slot_validation.csv", index=False)
    exp.dump(destination/"validation.json", {
        "status": "PASS", "completed_at": exp.now(), "source_sha256": exp.sha(__file__),
        "formal_models": models, "business_refits": 0, "saved_prediction_checks": models,
        "old_training_rows_compared": old_checks, "calendar_window_checks": feature_checks,
        "future_perturbation_prediction_checks": future_checks,
        "independent_daily_truth_windows": truth_checks, "joint_objects_checked": len(objects),
        "source_and_frozen_outputs_unchanged": True, "sample_weight_unchanged": True,
        "limits": ["Training new-window checks use 64 rows per slot; original columns hash-check every row.",
                   "Future perturbation uses 8 eligible objects in first/last issues, each M1/M2; synthetic tests supplement.",
                   "Historical acquisition availability remains reconstructed, not proven point-in-time."],
        "score_manifest_sha256": exp.sha(reviewed/"score_manifest.json"),
        "prepare_manifest_sha256": exp.sha(out/"prepare_manifest.json"),
    })
    print(json.dumps({"status": "PASS", "formal_models": models, "truth_windows": truth_checks,
                      "future_checks": future_checks}))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True); p.add_argument("--reviewed", required=True); p.add_argument("--destination", required=True)
    validate(**vars(p.parse_args()))
