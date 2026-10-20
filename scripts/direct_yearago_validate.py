"""Independent raw-calendar and accounting checks, never refits business models."""
from __future__ import annotations
import argparse
import datetime as dt
import os
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts import direct_yearago_experiment as e
except ModuleNotFoundError:
    import direct_yearago_experiment as e

KEY = ["issue_date", "shop", "spu"]


def raw_features(data, issue, segment):
    r = pd.Timestamp(issue)
    try:
        anchor = dt.date(r.year-1, r.month, r.day)
    except ValueError:
        assert (r.month, r.day) == (2, 29)
        anchor = dt.date(r.year-1, 2, 28)
    c = r-pd.Timedelta(days=2)
    x = pd.Timestamp(anchor)
    input_dates = pd.date_range(x-pd.Timedelta(days=29), periods=28)
    target_dates = pd.date_range(x+pd.Timedelta(days=1 if segment == "M1" else 31), periods=30)
    recent_dates = pd.date_range(c-pd.Timedelta(days=27), periods=28)
    visible = data[data.index <= c]

    def mean(dates):
        q = visible.reindex(dates)
        valid = q.mapped.eq(True) & q.y.ge(0) & np.isfinite(q.y) & q.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"])
        return float(q.y.sum()/len(dates)) if valid.all() else np.nan, int(valid.all())

    mi, ci = mean(input_dates)
    mt, ct = mean(target_dates)
    mn, _ = mean(recent_dates)
    history = visible[visible.index <= input_dates[0]]
    positive = (history.mapped.eq(True) & history.y.gt(0) & np.isfinite(history.y) &
                history.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"])).any()

    def ratio(a, b):
        if pd.isna(a) or pd.isna(b):
            return np.nan
        return (a-b)/(a+b) if a+b > 0 else 0.

    return [mi, mt, ratio(mn, mi), ratio(mt, mi), ci, ct, int(positive)]


def prepared(out, destination):
    out, dest = Path(out).resolve(), Path(destination).resolve()
    if dest.exists():
        raise ValueError("VALIDATION_DEST_EXISTS")
    config = e.read_json(out/"configuration.json")
    pm = e.read_json(out/"prepare_manifest.json")
    e.verify_hashes(pm["sources"])
    e.verify_hashes({str(out/k): v for k, v in pm["files"].items()})
    archive = Path(config["archive"])
    daily = pd.read_parquet(archive/"inputs/daily.parquet", columns=["shop", "spu", "ds", "y", "mapped", "sales_state"])
    raw = {key: g.set_index("ds") for key, g in daily.groupby(["shop", "spu"])}
    checks, records, futures = 0, [], 0
    for slot in sorted((out/"prepared").glob("*_M*")):
        train, inf = pd.read_parquet(slot/"train.parquet"), pd.read_parquet(slot/"inference.parquet")
        issue, segment = slot.name.rsplit("_", 1)
        old = archive/"effects/slots"/slot.name
        clean = train.drop(columns=e.EXTRA)
        assert e.frozen.frame_hash(clean) == e.read_json(old/"checkpoint.json")["training_sha256"]
        assert e.frozen.training_audit(clean, e.KEY, e.prior.AUDIT_COLUMNS) == e.read_json(old/"training_audit.json")
        assert (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all()
        offset = 30 if segment == "M1" else 60
        assert (train.simulated_issue_date+pd.Timedelta(days=offset)).eq(train.label_end).all()
        assert train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all()
        windows = pd.read_parquet(slot/"training_windows.parquet")
        assert windows.ly_target_end.le(train.input_cutoff).all()
        assert windows.ly_input_end.le(train.input_cutoff).all()
        assert (windows.ly_input_end-windows.ly_input_start).dt.days.eq(27).all()
        assert (windows.ly_target_end-windows.ly_target_start).dt.days.eq(29).all()
        mask = train[e.FLAGS].eq(1).all(axis=1)
        support = train[mask]
        expected = (support.simulated_issue_date.nunique() >= 30 and
                    len(support[["shop", "spu"]].drop_duplicates()) >= 10 and
                    support[e.NUMERIC].nunique().gt(1).any())
        check = e.read_json(slot/"training_check.json")
        assert bool(expected) == check["support"]["fit_allowed"]
        positions = np.unique(np.linspace(0, len(train)-1, min(64, len(train))).astype(int))
        samples = pd.concat([train.iloc[positions], inf], ignore_index=True)
        for row in samples.itertuples():
            data = raw.get((row.shop, row.spu))
            if data is None:
                data = pd.DataFrame(columns=["y", "mapped", "sales_state"], index=pd.DatetimeIndex([]))
            values = raw_features(data, row.simulated_issue_date, segment)
            expected_values = [getattr(row, c) for c in e.EXTRA]
            np.testing.assert_allclose(values, expected_values, rtol=1e-12, atol=1e-10, equal_nan=True)
            checks += 1
        # Actual archived series; test the input boundary, not only a mock.
        for row in inf.iloc[:2].itertuples():
            data = raw[(row.shop, row.spu)]
            changed = data.copy()
            changed.loc[changed.index > row.input_cutoff, "y"] = 10**9
            np.testing.assert_allclose(raw_features(data, row.simulated_issue_date, segment),
                                       raw_features(changed, row.simulated_issue_date, segment),
                                       rtol=0, atol=0, equal_nan=True)
            futures += 1
        records.append({"unit": slot.name, "train_rows_checked": len(train),
                        "raw_calendar_rows_checked": len(samples),
                        "fit_allowed": bool(expected), "status": "PASS"})
        print(slot.name, "PRECHECK_PASS", len(train), flush=True)
    dest.mkdir(exist_ok=False)
    pd.DataFrame(records).to_csv(dest/"unit_checks.csv", index=False, encoding="utf-8-sig")
    e.dump(dest/"validation.json", {
        "status": "PASS", "at": e.now(), "train_rows": sum(x["train_rows_checked"] for x in records),
        "calendar_rows": checks, "future_perturbations": futures, "units": len(records),
        "allowed_fits": sum(x["fit_allowed"] for x in records), "formal_fits": 0,
        "script_sha256": e.sha(__file__), "prepare_manifest_sha256": e.sha(out/"prepare_manifest.json")})


def effects(out, reviewed, destination):
    from lightgbm import Booster
    out, reviewed, dest = [Path(x).resolve() for x in (out, reviewed, destination)]
    if dest.exists():
        raise ValueError("VALIDATION_DEST_EXISTS")
    config, pm = e.read_json(out/"configuration.json"), e.read_json(out/"prepare_manifest.json")
    archive = Path(config["archive"])
    em, sm = e.read_json(out/"effects_manifest.json"), e.read_json(reviewed/"score_manifest.json")
    e.verify_hashes(pm["sources"])
    e.verify_hashes(sm["sources"])
    for root, manifest in [(out, pm), (out, em), (reviewed, sm)]:
        e.verify_hashes({str(root/k): v for k, v in manifest["files"].items()})
    fits, skips, fallback_values, annual_values = 0, 0, 0, 0
    daily = pd.read_parquet(archive/"inputs/daily.parquet", columns=["shop", "spu", "ds", "y", "mapped", "sales_state"])
    raw = {key: g.set_index("ds") for key, g in daily.groupby(["shop", "spu"])}
    future_predictions = 0
    for slot in sorted((out/"prepared").glob("*_M*")):
        check = e.read_json(slot/"training_check.json")
        inf = pd.read_parquet(slot/"inference.parquet")
        pred = pd.read_parquet(out/"effects"/slot.name/"predictions.parquet")
        eff = out/"effects"/slot.name
        good = inf.input_status.eq("OK")
        assert pred.comparison_prediction.notna().equals(good)
        if check["support"]["fit_allowed"]:
            fits += 1
            assert (eff/"fit_started.json").exists()
            checkpoint = e.read_json(eff/"checkpoint.json")
            assert checkpoint["params"] == config["lightgbm"]
            model = Booster(model_file=os.path.relpath(eff/"model.txt", e.ROOT))
            assert model.feature_name() == e.FEATURES
            got = np.maximum(model.predict(inf.loc[good, e.FEATURES]), 0)
            np.testing.assert_array_equal(got, pred.loc[good, "annual_model_prediction"])
            np.testing.assert_array_equal(pred.comparison_prediction, pred.annual_model_prediction)
            assert pred.loc[good, "prediction_origin"].eq(e.ANNUAL).all()
            annual_values += int(good.sum())
            issue, segment = slot.name.rsplit("_", 1)
            for row in inf[good].iloc[:2].itertuples():
                data = raw[(row.shop, row.spu)]
                changed = data.copy()
                changed.loc[changed.index > row.input_cutoff, "y"] = 10**9
                frames = []
                for source in [data, changed]:
                    hist = source.reindex(pd.date_range(row.input_cutoff-pd.Timedelta(days=27), row.input_cutoff))
                    old_features = e.direct.features_from_history(hist.y.to_numpy(), e.frozen.SHOPS.index(row.shop), row.label_start)
                    old_features.update(dict(zip(e.EXTRA, raw_features(source, row.simulated_issue_date, segment))))
                    frames.append(pd.DataFrame([old_features])[e.FEATURES])
                pd.testing.assert_frame_equal(frames[0], frames[1], check_exact=True)
                np.testing.assert_array_equal(model.predict(frames[0]), model.predict(frames[1]))
                future_predictions += 1
        else:
            skips += 1
            assert not (eff/"fit_started.json").exists() and not (eff/"model.txt").exists()
            assert pred.annual_model_prediction.isna().all()
            old = pd.read_parquet(check["original_prediction_path"]).set_index(["shop", "spu"])
            orig = old.loc[pd.MultiIndex.from_frame(inf[["shop", "spu"]]), "prediction"]
            np.testing.assert_array_equal(orig, pred.comparison_prediction)
            assert pred.loc[good, "prediction_origin"].eq(e.FALLBACK).all()
            assert pred.original_prediction_sha256.eq(e.sha(check["original_prediction_path"])).all()
            fallback_values += int(good.sum())
    assert fits == em["formal_fits"] <= 40
    # Independent truth summation uses exact date membership and raw valid-row counts.
    truth = pd.read_parquet(reviewed/"truth.parquet")
    truth_n = 0
    for row in truth[truth.truth_status.ne("NOT_MATURED")].itertuples():
        g = raw[(row.shop, row.spu)]
        part = g[(g.index >= pd.Timestamp(row.target_start)) & (g.index <= pd.Timestamp(row.target_end))]
        length = (pd.Timestamp(row.target_end)-pd.Timestamp(row.target_start)).days+1
        valid = part.mapped.eq(True) & part.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]) & part.y.ge(0) & np.isfinite(part.y)
        actual = float(sum(part.y)) if len(part) == length and valid.all() else np.nan
        np.testing.assert_allclose(actual, row.actual, rtol=0, atol=0, equal_nan=True)
        truth_n += 1
    scores = pd.read_parquet(reviewed/"scores.parquet")
    objects = pd.read_parquet(reviewed/"objects.parquet")
    detail = pd.read_parquet(reviewed/"paired_details.parquet")
    summary = e.read_json(reviewed/"summary.json")
    baseline_exact = 0
    for issue in detail.issue_date.unique():
        parts = {s: pd.read_parquet(archive/"effects/slots"/f"{issue}_{s}"/"segment_predictions.parquet").set_index(["shop", "spu"])
                 for s in ["M1", "M2"]}
        for row in scores[scores.model.eq("DIRECT30x2") & scores.issue_date.eq(issue)].itertuples():
            p1 = parts["M1"].loc[(row.shop, row.spu), "prediction"]
            p2 = parts["M2"].loc[(row.shop, row.spu), "prediction"]
            canonical = p1 if row.window == "d01_30" else p2 if row.window == "d31_60" else p1+p2
            np.testing.assert_allclose(canonical, row.prediction, rtol=0, atol=0, equal_nan=True)
            baseline_exact += 1
    joint_checks = 0
    for (issue, shop, spu, model), g in scores.groupby(KEY+["model"]):
        q = g.set_index("window")
        joint = all(pd.notna(q.loc[w, "prediction"]) and q.loc[w, "actual"] > 0 and
                    abs(q.loc[w, "prediction"]-q.loc[w, "actual"]) <= .2*q.loc[w, "actual"]
                    for w in ["d01_30", "d31_60", "d01_60"])
        stored = objects[(objects.issue_date == issue) & (objects.shop == shop) &
                         (objects.spu == spu) & (objects.model == model)].iloc[0]
        assert bool(stored.joint_hit) == joint
        if model == "DIRECT_YEARAGO":
            np.testing.assert_allclose(q.loc["d01_60", "prediction"],
                                       q.loc["d01_30", "prediction"]+q.loc["d31_60", "prediction"],
                                       rtol=0, atol=0, equal_nan=True)
        joint_checks += 1
    q = detail[detail.original_direct_output]
    actual = q.actual_d01_60
    olderr, newerr = q.prediction_d01_60_base-actual, q.prediction_d01_60_candidate-actual
    decline = q.decline_group.eq("PRE_ISSUE_DECLINING")
    independently = {
        "paired_w60_wape_lower": bool(newerr.abs().sum()/actual.sum() < olderr.abs().sum()/actual.sum()),
        "joint_hit_count_increased": bool(q.joint_hit_candidate.sum() > q.joint_hit_base.sum()),
        "joint_hit_sales_share_not_lower": bool(actual[q.joint_hit_candidate].sum()/actual.sum() >= actual[q.joint_hit_base].sum()/actual.sum()),
        "declining_w60_over_rate_not_increased": bool(newerr[decline].clip(lower=0).sum()/actual[decline].sum() <= olderr[decline].clip(lower=0).sum()/actual[decline].sum()),
        "output_and_core_coverage_not_reduced": bool(detail.output_complete_base.eq(detail.output_complete_candidate).all() and len(detail) == summary["full_core_n"]),
    }
    assert independently == summary["gates"]
    # Reconcile every metrics row, not only the headline.
    metrics = pd.read_parquet(reviewed/"metrics.parquet")
    for row in metrics.itertuples():
        g = scores[scores.issue_date.isin(detail.issue_date.unique()) & scores.model.eq(row.model) & scores.window.eq(row.window)]
        if row.scope == "ORIGINAL_DIRECT_OUTPUT":
            g = g[g.original_direct_output]
        elif row.scope == "BOTH_ANNUAL_OUTPUTS":
            g = g[g.both_annual_outputs]
        dimensions = {"overall": [], "year": ["year"], "issue": ["issue_date"], "shop": ["shop"],
                      "maturity": ["maturity"], "category": ["current_category"], "season": ["current_season"],
                      "shop_category": ["shop", "current_category"], "shop_season": ["shop", "current_season"],
                      "annual_state": ["annual_state"], "decline_group": ["decline_group"],
                      "issue_annual_state": ["issue_date", "annual_state"]}[row.slice]
        for col in dimensions:
            g = g[g[col].eq(getattr(row, col))]
        valid = g.prediction.notna() & g.actual.notna()
        delta = (g.prediction-g.actual)[valid]
        denom = g.loc[valid, "actual"].sum()
        assert len(g) == row.fixed_n
        np.testing.assert_allclose(delta.abs().sum(), row.ae, rtol=1e-13, atol=1e-8)
        np.testing.assert_allclose(delta.abs().sum()/denom if denom else np.nan, row.wape, equal_nan=True)
        np.testing.assert_allclose(delta.sum()/denom if denom else np.nan, row.bias, equal_nan=True)
        np.testing.assert_allclose(delta.clip(lower=0).sum(), row.over_units, rtol=1e-13, atol=1e-8)
        np.testing.assert_allclose((-delta).clip(lower=0).sum(), row.under_units, rtol=1e-13, atol=1e-8)
        hit = g.actual.gt(0) & g.prediction.notna() & ((g.prediction-g.actual).abs() <= .2*g.actual)
        assert int(hit.sum()) == row.hit_n
        np.testing.assert_allclose(g.loc[hit, "actual"].sum(), row.hit_actual, rtol=0, atol=0)
        np.testing.assert_allclose(delta[g.loc[valid, "actual"].eq(0)].abs().sum(), row.zero_truth_ae, rtol=1e-13, atol=1e-8)
    e.verify_hashes(pm["sources"])
    dest.mkdir(exist_ok=False)
    e.dump(dest/"validation.json", {
        "status": "PASS", "at": e.now(), "formal_fits": fits, "skipped_units": skips,
        "fallback_values_exact": fallback_values, "annual_values_exact": annual_values,
        "raw_truth_windows": truth_n, "joint_objects": joint_checks, "metric_rows": len(metrics),
        "actual_future_prediction_checks": future_predictions,
        "baseline_archive_values_exact": baseline_exact,
        "gates_independently_reconciled": independently, "old_hashes_unchanged": True,
        "script_sha256": e.sha(__file__), "score_manifest_sha256": e.sha(reviewed/"score_manifest.json")})
    print("INDEPENDENT_EFFECT_VALIDATION_PASS", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepared")
    p.add_argument("--out", required=True); p.add_argument("--destination", required=True)
    p = sub.add_parser("effects")
    p.add_argument("--out", required=True); p.add_argument("--reviewed", required=True)
    p.add_argument("--destination", required=True)
    args = vars(parser.parse_args()); cmd = args.pop("command")
    {"prepared": prepared, "effects": effects}[cmd](**args)
