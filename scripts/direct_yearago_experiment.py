"""One calendar-year feature increment; archived inputs only, bounded fits."""
from __future__ import annotations
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import pandas as pd

try:
    from scripts import direct_context90_experiment as prior
except ModuleNotFoundError:
    import direct_context90_experiment as prior

frozen, direct = prior.frozen, prior.direct
ROOT = Path(__file__).resolve().parents[1]
sha, dump, read_json, now, verify_hashes = prior.sha, prior.dump, prior.read_json, prior.now, prior.verify_hashes
PREREG = "78773ceaddff789aeb01afe13f3ad04e995297ff"
NUMERIC = ["ly_input_mean28", "ly_target_mean30", "ly_scale_contrast", "ly_shape_contrast"]
FLAGS = ["ly_input_complete", "ly_target_complete", "ly_positive_observed_before_input_start"]
EXTRA = NUMERIC + FLAGS
FEATURES = direct.FEATURES + EXTRA
KEY = ["shop", "spu", "simulated_issue_date"]
FALLBACK = "BASELINE_REUSED_INSUFFICIENT_ANNUAL_SUPPORT"
ANNUAL = "ANNUAL_MODEL"


def annual_bounds(issue, segment):
    r = pd.Timestamp(issue)
    try:
        anchor = r.replace(year=r.year - 1)
    except ValueError:
        if (r.month, r.day) != (2, 29):
            raise
        anchor = r.replace(year=r.year - 1, day=28)
    c = r - pd.Timedelta(days=2)
    lc = anchor - pd.Timedelta(days=2)
    offset = 1 if segment == "M1" else 31 if segment == "M2" else None
    if offset is None:
        raise ValueError("SEGMENT")
    start = anchor + pd.Timedelta(days=offset)
    end = start + pd.Timedelta(days=29)
    assert end <= c and lc <= c
    return lc - pd.Timedelta(days=27), lc, start, end


def contrast(a, b):
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    out = np.full(a.shape, np.nan)
    valid = np.isfinite(a) & np.isfinite(b) & (a >= 0) & (b >= 0)
    zero = valid & (a == 0) & (b == 0)
    positive = valid & (a + b > 0)
    out[zero] = 0
    out[positive] = (a[positive] - b[positive]) / (a[positive] + b[positive])
    return out


class DailyIndex:
    """No filling of unknown days; zero prefix entries are never treated as known."""
    def __init__(self, daily):
        d = daily[["shop", "spu", "ds", "y", "mapped", "sales_state"]].copy()
        d["ds"] = pd.to_datetime(d.ds)
        if d.duplicated(["shop", "spu", "ds"]).any():
            raise ValueError("DUPLICATE_DAILY")
        self.first, self.last = d.ds.min(), d.ds.max()
        self.dates = pd.date_range(self.first, self.last)
        self.data = {}
        for key, g in d.groupby(["shop", "spu"], sort=False):
            z = g.set_index("ds").reindex(self.dates)
            present = self.dates.isin(g.ds)
            y = z.y.to_numpy(dtype=float)
            mapped = z.mapped.eq(True).to_numpy(dtype=bool)
            known = mapped & np.isfinite(y) & (y >= 0) & z.sales_state.isin(direct.VALID).to_numpy()
            unsafe = present & ~known
            positives = np.flatnonzero(known & (y > 0))
            self.data[key] = {
                "y": y, "known": known, "present": present,
                "sum": np.r_[0., np.cumsum(np.where(known, y, 0.))],
                "invalid": np.r_[0, np.cumsum(~known)],
                "unsafe": np.r_[0, np.cumsum(unsafe)],
                "absent": np.r_[0, np.cumsum(~present)],
                "zero": np.r_[0, np.cumsum(known & (y == 0))],
                "first_positive": self.dates[positives[0]] if len(positives) else pd.NaT,
            }

    def interval(self, key, starts, ends):
        starts, ends = pd.DatetimeIndex(starts), pd.DatetimeIndex(ends)
        left = np.asarray((starts - self.first).days)
        right = np.asarray((ends - self.first).days) + 1
        width = right - left
        n = len(left)
        means = np.full(n, np.nan)
        unknown, absent = width.copy(), width.copy()
        unsafe, zero = np.zeros(n, dtype=int), np.zeros(n, dtype=int)
        entry = self.data.get(key)
        if entry is not None:
            length = len(self.dates)
            l, r = np.clip(left, 0, length), np.clip(right, 0, length)
            outside = width - (r - l)
            unknown = entry["invalid"][r] - entry["invalid"][l] + outside
            absent = entry["absent"][r] - entry["absent"][l] + outside
            unsafe = entry["unsafe"][r] - entry["unsafe"][l]
            zero = entry["zero"][r] - entry["zero"][l]
            ok = unknown == 0
            means[ok] = (entry["sum"][r[ok]] - entry["sum"][l[ok]]) / width[ok]
        return means, unknown, absent, unsafe, zero

    def features(self, key, issues, segment):
        issues = pd.DatetimeIndex(issues)
        unique = {r: annual_bounds(r, segment) for r in issues.unique()}
        bounds = [unique[r] for r in issues]
        a, b, t, u = [pd.DatetimeIndex([v[i] for v in bounds]) for i in range(4)]
        c = issues - pd.Timedelta(days=2)
        mi, ni, ai, ui, zi = self.interval(key, a, b)
        mt, nt, at, ut, zt = self.interval(key, t, u)
        mn, nn, _, un, _ = self.interval(key, c - pd.Timedelta(days=27), c)
        entry = self.data.get(key)
        first = entry["first_positive"] if entry else pd.NaT
        positive = np.asarray((a >= first) & (c >= first)) if pd.notna(first) else np.zeros(len(c), dtype=bool)
        complete_i, complete_t = ni == 0, nt == 0
        unsafe = (ui + ut + un) > 0
        state = np.select([
            unsafe, complete_i & complete_t & positive,
            complete_i & complete_t & ~positive, complete_i ^ complete_t
        ], ["UNSAFE_MAPPING_OR_SOURCE", "BOTH_COMPLETE_PRIOR_POSITIVE",
            "BOTH_COMPLETE_NO_PRIOR_POSITIVE", "ONE_WINDOW_COMPLETE"],
            default="ANNUAL_WINDOWS_MISSING")
        return pd.DataFrame({
            "ly_input_mean28": mi, "ly_target_mean30": mt,
            "ly_scale_contrast": contrast(mn, mi), "ly_shape_contrast": contrast(mt, mi),
            "ly_input_complete": complete_i.astype(np.int8),
            "ly_target_complete": complete_t.astype(np.int8),
            FLAGS[2]: positive.astype(np.int8),
            "ly_input_start": a, "ly_input_end": b,
            "ly_target_start": t, "ly_target_end": u,
            "ly_input_unknown_days": ni, "ly_input_absent_days": ai,
            "ly_input_unsafe_days": ui, "ly_input_zero_days": zi,
            "ly_target_unknown_days": nt, "ly_target_absent_days": at,
            "ly_target_unsafe_days": ut, "ly_target_zero_days": zt,
            "current28_unknown_days": nn, "current28_mean_audit": mn,
            "annual_state": state, "operating_comparability": "UNKNOWN",
        })


def augment(index, frame, segment):
    chunks = []
    for key, g in frame.groupby(["shop", "spu"], sort=False):
        q = index.features(key, g.simulated_issue_date, segment)
        q.index = g.index
        chunks.append(q)
    audit = pd.concat(chunks).sort_index()
    extended = pd.concat([frame, audit[EXTRA]], axis=1)
    pd.testing.assert_frame_equal(extended[frame.columns], frame, check_exact=True)
    assert len(extended) == len(frame)
    assert (audit.ly_target_end <= frame.input_cutoff).all()
    return extended, pd.concat([frame[KEY + ["input_cutoff"]], audit], axis=1)


def support(frame):
    mask = frame[FLAGS].eq(1).all(axis=1)
    g = frame[mask]
    dates = int(g.simulated_issue_date.nunique())
    products = len(g[["shop", "spu"]].drop_duplicates())
    varying = bool(g[NUMERIC].nunique(dropna=True).gt(1).any())
    return {
        "train_n": len(frame), "train_products": len(frame[["shop", "spu"]].drop_duplicates()),
        "train_dates": int(frame.simulated_issue_date.nunique()),
        "input_complete_n": int(frame.ly_input_complete.sum()),
        "target_complete_n": int(frame.ly_target_complete.sum()),
        "support_rows": len(g), "support_products": products, "support_dates": dates,
        "support_first": str(g.simulated_issue_date.min()) if len(g) else None,
        "support_last": str(g.simulated_issue_date.max()) if len(g) else None,
        "numerical_features_vary": varying,
        "fit_allowed": dates >= 30 and products >= 10 and varying,
    }


def prepare(archive, context, out, tests):
    archive, context, out, tests = [Path(p).resolve() for p in (archive, context, out, tests)]
    if out.exists() or archive in out.parents or context in out.parents:
        raise ValueError("NEW_EXCLUSIVE_OUTPUT_REQUIRED")
    if read_json(tests)["status"] != "PASS":
        raise ValueError("TEST_GATE")
    conf = read_json(archive/"prepared/frozen_config.json")
    runtime = {p: importlib.metadata.version(p) for p in conf["runtime"]}
    if runtime != conf["runtime"]:
        raise ValueError("RUNTIME_CHANGED")
    for rel, h in conf["source_hashes"].items():
        assert sha(ROOT/rel) == h, rel
    cp = read_json(context/"prepare_manifest.json")
    verify_hashes(cp["sources"])
    pm = read_json(archive/"prepared/prepare_manifest.json")
    assert sha(ROOT/"scripts/bc60_monthly_replay.py") == pm["script_sha256"]
    old_manifest = read_json(archive/"effects/run_manifest.json")
    sources = {}

    def protect(path, expected=None):
        h = sha(path)
        if expected is not None and h != expected:
            raise ValueError("SOURCE_CHANGED:" + str(path))
        sources[str(path)] = h

    for path in [tests, ROOT/"docs/direct_yearago_protocol.md",
                 ROOT/"scripts/direct_context90_experiment.py",
                 ROOT/"scripts/direct_context90_score.py", ROOT/"scripts/bc60_monthly_replay.py",
                 ROOT/"scripts/bc60_v3_direct.py",
                 context/"prepare_manifest.json", context/"scoring/score_manifest.json",
                 archive/"prepared/prepare_manifest.json", archive/"effects/run_manifest.json",
                 archive/"scoring/scores.csv", archive/"scoring/score_manifest.json"]:
        protect(path)
    for rel, h in pm["files"].items():
        protect(archive/"prepared"/rel, h)
    for rel in ["inputs/daily.parquet", "inputs/monthly_reference.parquet"]:
        protect(archive/rel, cp["sources"][str(archive/rel)])
    for rel in ["observation_groups.parquet"]:
        protect(context/rel, cp["files"][rel])
    sm = read_json(context/"scoring/score_manifest.json")
    protect(context/"scoring/scores.parquet", sm["files"]["scores.parquet"])
    grid = pd.read_csv(archive/"prepared/issue_grid.csv")
    core = pd.read_parquet(archive/"prepared/core.parquet")
    issues = grid[grid.calendar_mature.eq(True)].issue_date.tolist()
    if len(issues) * 2 > 40:
        raise ValueError("BUDGET")
    index = DailyIndex(pd.read_parquet(archive/"inputs/daily.parquet",
                                     columns=["shop", "spu", "ds", "y", "mapped", "sales_state"]))
    out.mkdir(exist_ok=False)
    (out/"prepared").mkdir()
    dump(out/"configuration.json", {
        "candidate": "DIRECT_YEARAGO", "preregistration_commit": PREREG,
        "features": FEATURES, "original_features": direct.FEATURES, "extra_features": EXTRA,
        "lightgbm": conf["original"]["lightgbm"], "runtime": runtime, "sample_weight": None,
        "max_formal_fits": 40, "archive": str(archive), "context_archive": str(context),
        "script_sha256": sha(__file__), "protocol_sha256": sha(ROOT/"docs/direct_yearago_protocol.md"),
        "classification": "RECONSTRUCTED_HISTORY_DEVELOPMENT_VALIDATION_NOT_OOS",
        "positive_flag_boundary": "inclusive <= annual input start",
        "missing_handling": "native NaN unchanged; zero_as_missing=false",
    })
    core.to_parquet(out/"prepared/core.parquet", index=False)
    grid.to_csv(out/"prepared/issue_grid.csv", index=False)
    obs = pd.read_parquet(context/"observation_groups.parquet")
    mature_core = core[core.issue_date.isin(issues)]
    assert set(map(tuple, obs[["issue_date", "shop", "spu"]].values)) == set(map(tuple, mature_core[["issue_date", "shop", "spu"]].values))
    obs[["issue_date", "shop", "spu", "decline_group"]].to_parquet(out/"observation_groups.parquet", index=False)
    matrix, feature_stats, slots, inference_audits = [], [], [], []
    started = now()
    for issue in issues:
        for segment in ["M1", "M2"]:
            unit = f"{issue}_{segment}"
            src = context/"prepared"/unit
            old = archive/"effects/slots"/unit
            dest = out/"prepared"/unit
            dest.mkdir(exist_ok=False)
            for name in ["train.parquet", "inference.parquet"]:
                protect(src/name, cp["files"][str((src/name).relative_to(context))])
            for name in ["training_audit.json", "checkpoint.json", "segment_predictions.parquet"]:
                protect(old/name, old_manifest["files"][str((old/name).relative_to(archive/"effects"))])
            train = pd.read_parquet(src/"train.parquet").drop(columns=prior.EXTRA)
            inf = pd.read_parquet(src/"inference.parquet").drop(columns=prior.EXTRA)
            audit = frozen.training_audit(train, KEY, prior.AUDIT_COLUMNS)
            assert audit == read_json(old/"training_audit.json")
            assert frozen.frame_hash(train) == read_json(old/"checkpoint.json")["training_sha256"]
            assert not train.duplicated(KEY).any()
            assert train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all()
            assert (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all()
            prior_inf = pd.read_parquet(old/"segment_predictions.parquet").set_index(["shop", "spu"])
            check_inf = inf.set_index(["shop", "spu"])
            pd.testing.assert_frame_equal(check_inf[direct.FEATURES+["input_status"]],
                                          prior_inf.loc[check_inf.index, direct.FEATURES+["input_status"]],
                                          check_exact=True, check_dtype=False)
            full, ta = augment(index, train, segment)
            extended, ia = augment(index, inf, segment)
            sp = support(full)
            full.to_parquet(dest/"train.parquet", index=False)
            extended.to_parquet(dest/"inference.parquet", index=False)
            ta.to_parquet(dest/"training_windows.parquet", index=False)
            ia.to_parquet(dest/"inference_windows.parquet", index=False)
            ia["issue_date"], ia["segment"] = issue, segment
            inference_audits.append(ia)
            dump(dest/"training_check.json", {
                "status": "PASS", "old_audit_exact": True, "old_frame_exact": True,
                "rows_retained": len(full), "old_features_and_weights_unchanged": True,
                "old_frame_sha256": frozen.frame_hash(train), "old_inference_exact": True,
                "labels_mature": True, "train_sha256": sha(dest/"train.parquet"),
                "inference_sha256": sha(dest/"inference.parquet"), "support": sp,
                "original_prediction_path": str(old/"segment_predictions.parquet"),
                "original_prediction_sha256": sha(old/"segment_predictions.parquet"),
            })
            matrix.append({"issue_date": issue, "segment": segment, **sp,
                           "inference_n": len(inf), "inference_eligible_n": int(inf.input_status.eq("OK").sum()),
                           "inference_input_complete": int(extended.ly_input_complete.sum()),
                           "inference_target_complete": int(extended.ly_target_complete.sum()),
                           "inference_both_prior_positive": int(extended[FLAGS].eq(1).all(axis=1).sum())})
            for kind, frame in [("training", full), ("inference", extended)]:
                for field in EXTRA:
                    q = frame[field]
                    feature_stats.append({"issue_date": issue, "segment": segment, "matrix": kind,
                                          "feature": field, "n": len(q), "missing_n": int(q.isna().sum()),
                                          "missing_share": float(q.isna().mean()), "min": q.min(),
                                          "max": q.max(), "distinct": q.nunique()})
            slots.append({"issue_date": issue, "segment": segment, "fit_allowed": sp["fit_allowed"],
                          "state": "PLANNED", "support": sp})
            print(json.dumps({"prepared": unit, **sp}), flush=True)
    pd.DataFrame(matrix).to_csv(out/"annual_support.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(feature_stats).to_csv(out/"feature_completeness.csv", index=False, encoding="utf-8-sig")
    pd.concat(inference_audits, ignore_index=True).to_parquet(out/"inference_windows.parquet", index=False)
    verify_hashes(sources)
    dump(out/"prepare_manifest.json", {
        "started_at": started, "completed_at": now(), "sources": sources,
        "script_sha256": sha(__file__), "preregistration_commit": PREREG, "formal_fits": 0,
        "files": {str(p.relative_to(out)): sha(p) for p in out.rglob("*") if p.is_file()}})
    dump(out/"formal_fit_ledger.json", {"created_at": now(), "max_formal_fits": 40, "slots": slots})
    print(json.dumps({"status": "PREPARED_NO_EFFECTS", "allowed": sum(x["fit_allowed"] for x in slots)}))


def worker(out, issue, segment, recovery=False):
    from lightgbm import LGBMRegressor, Booster
    out = Path(out).resolve()
    conf = read_json(out/"configuration.json")
    prep, slot = out/"prepared"/f"{issue}_{segment}", out/"effects"/f"{issue}_{segment}"
    slot.mkdir(parents=True, exist_ok=True)
    check = read_json(prep/"training_check.json")
    assert sha(prep/"train.parquet") == check["train_sha256"]
    assert sha(prep/"inference.parquet") == check["inference_sha256"]
    assert conf["features"] == FEATURES
    inf = pd.read_parquet(prep/"inference.parquet")
    good = inf.input_status.eq("OK")
    annual = np.full(len(inf), np.nan)
    fit_seconds = 0.
    if check["support"]["fit_allowed"]:
        relative = os.path.relpath(slot/"model.txt", ROOT)
        relative.encode("ascii")
        if recovery:
            checkpoint = read_json(slot/"checkpoint.json")
            assert sha(slot/"model.txt") == checkpoint["model_sha256"]
            model = Booster(model_file=relative)
            fit_seconds = checkpoint["fit_seconds"]
        else:
            train = pd.read_parquet(prep/"train.parquet")
            with (slot/"fit_started.json").open("x", encoding="utf-8") as f:
                json.dump({"at": now(), "features": FEATURES, "train_sha256": check["train_sha256"]}, f)
            model = LGBMRegressor(**conf["lightgbm"])
            tic = time.monotonic()
            model.fit(train[FEATURES], train.y_label)
            fit_seconds = time.monotonic()-tic
            model.booster_.save_model(relative)
            dump(slot/"checkpoint.json", {"completed_at": now(), "fit_seconds": fit_seconds,
                                         "model_sha256": sha(slot/"model.txt"), "features": FEATURES,
                                         "train_sha256": check["train_sha256"], "params": conf["lightgbm"]})
        if good.any():
            annual[good] = np.maximum(model.predict(inf.loc[good, FEATURES]), 0)
            assert np.isfinite(annual[good]).all()
        comparison = annual.copy()
        origin = np.where(good, ANNUAL, "NO_OUTPUT_ORIGINAL_QUALIFICATION")
        reason = np.where(good, "", inf.input_status)
        state = "RECOVERED" if recovery else "FITTED"
    else:
        assert not (slot/"fit_started.json").exists()
        p = Path(check["original_prediction_path"])
        assert sha(p) == check["original_prediction_sha256"]
        old = pd.read_parquet(p).set_index(["shop", "spu"])
        comparison = old.loc[pd.MultiIndex.from_frame(inf[["shop", "spu"]]), "prediction"].to_numpy()
        assert np.isfinite(comparison).tolist() == good.tolist()
        origin = np.where(good, FALLBACK, "NO_OUTPUT_ORIGINAL_QUALIFICATION")
        reason = np.where(good, "annual support screen not met", inf.input_status)
        state = FALLBACK
    inf["annual_model_prediction"], inf["comparison_prediction"] = annual, comparison
    inf["prediction_origin"], inf["fallback_or_missing_reason"] = origin, reason
    inf["original_prediction_sha256"] = check["original_prediction_sha256"]
    inf["prediction_status"] = np.where(good, "OK", inf.input_status)
    dest = slot/"predictions.parquet"
    if dest.exists():
        raise ValueError("REFUSE_OUTPUT_OVERWRITE")
    inf.to_parquet(dest, index=False)
    dump(slot/"result.json", {"status": state, "completed_at": now(), "issue_date": issue, "segment": segment,
                             "fit_seconds": fit_seconds, "n": len(inf), "output_n": int(good.sum()),
                             "annual_output_n": int(np.isfinite(annual).sum()),
                             "prediction_sha256": sha(dest),
                             "model_sha256": sha(slot/"model.txt") if (slot/"model.txt").exists() else None})


def fit(out):
    out = Path(out).resolve()
    pm = read_json(out/"prepare_manifest.json")
    conf = read_json(out/"configuration.json")
    assert sha(__file__) == pm["script_sha256"] == conf["script_sha256"]
    verify_hashes(pm["sources"])
    verify_hashes({str(out/k): v for k, v in pm["files"].items()})
    assert {p: importlib.metadata.version(p) for p in conf["runtime"]} == conf["runtime"]
    ledger = read_json(out/"formal_fit_ledger.json")
    assert len(ledger["slots"]) <= 40
    started = now()
    for e in ledger["slots"]:
        issue, segment = e["issue_date"], e["segment"]
        slot = out/"effects"/f"{issue}_{segment}"
        if (slot/"result.json").exists():
            result = read_json(slot/"result.json")
            assert sha(slot/"predictions.parquet") == result["prediction_sha256"]
            if result["model_sha256"]:
                assert sha(slot/"model.txt") == result["model_sha256"]
            e.update(state=result["status"], result=result)
            continue
        recovery = (slot/"checkpoint.json").exists()
        if (slot/"fit_started.json").exists() and not recovery:
            raise ValueError("STARTED_WITHOUT_CHECKPOINT_NO_REFIT")
        if e["state"] != "PLANNED" and not recovery:
            raise ValueError("RESERVED_NO_AUTOMATIC_RETRY")
        e.update(state="RESERVED", reserved_at=now())
        dump(out/"formal_fit_ledger.json", ledger)
        command = [sys.executable, "-B", str(Path(__file__).resolve()), "_worker",
                   "--out", str(out), "--issue", issue, "--segment", segment]
        if recovery:
            command.append("--recovery")
        process = subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                                 encoding="utf-8", errors="replace", timeout=600,
                                 env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"})
        log = out/"effects"/f"{issue}_{segment}{'.recovery' if recovery else ''}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("x", encoding="utf-8") as f:
            f.write(process.stdout+"\n"+process.stderr)
        if process.returncode:
            e.update(state="TECHNICAL_FAILURE", exit_code=process.returncode)
            dump(out/"formal_fit_ledger.json", ledger)
            raise RuntimeError("WORKER_FAILED:" + str(log))
        result = read_json(slot/"result.json")
        e.update(state=result["status"], result=result)
        dump(out/"formal_fit_ledger.json", ledger)
        print(json.dumps(result), flush=True)
    verify_hashes(pm["sources"])
    fits = len(list((out/"effects").glob("*/fit_started.json")))
    assert fits <= 40
    dump(out/"effects_manifest.json", {
        "started_at": started, "completed_at": now(), "formal_fits": fits,
        "skipped_units": 40-fits, "sources_unchanged": True, "script_sha256": sha(__file__),
        "files": {str(p.relative_to(out)): sha(p) for p in (out/"effects").rglob("*") if p.is_file()}})
    print(json.dumps({"status": "COMPLETED", "formal_fits": fits}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    for arg in ["archive", "context", "out", "tests"]:
        p.add_argument("--"+arg, required=True)
    p = sub.add_parser("fit"); p.add_argument("--out", required=True)
    p = sub.add_parser("_worker")
    for arg in ["out", "issue", "segment"]:
        p.add_argument("--"+arg, required=True)
    p.add_argument("--recovery", action="store_true")
    args = vars(parser.parse_args()); command = args.pop("command")
    {"prepare": prepare, "fit": fit, "_worker": worker}[command](**args)
