"""One preregistered local feature increment; immutable original archives."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

import numpy as np
import pandas as pd

try:
    from scripts import bc60_monthly_replay as frozen
    from scripts import bc60_v3_direct as direct
except ModuleNotFoundError:
    import bc60_monthly_replay as frozen
    import bc60_v3_direct as direct

ROOT = Path(__file__).resolve().parents[1]
EXTRA = ["m56", "m90", "contrast56", "contrast90",
         "history56_complete", "history90_complete"]
FEATURES = [*direct.FEATURES, *EXTRA]
PREREG_COMMIT = "009144ff4eea1f0d14ed20b5c92704d77eefe0be"
KEY = ["shop", "spu", "simulated_issue_date"]
AUDIT_COLUMNS = ["shop", "spu", "simulated_issue_date", "input_cutoff",
                 "label_start", "label_end", "y_label", *direct.FEATURES]
read_json, dump, sha, now = frozen.read_json, frozen.dump, frozen.sha, frozen.now


def verify_hashes(files):
    for name, expected in files.items():
        if sha(name) != expected:
            raise ValueError("SOURCE_CHANGED:" + name)


def features_at(ctx, key, cutoffs):
    """Exact calendar-date prefix sums; invalid days never become known zeros."""
    cutoffs = pd.DatetimeIndex(cutoffs)
    n = len(cutoffs)
    out = {c: np.full(n, np.nan) for c in EXTRA[:4]}
    out.update({c: np.zeros(n, dtype=np.int8) for c in EXTRA[4:]})
    out.update({c: np.full(n, np.nan) for c in ["early34", "middle28", "recent28"]})
    if key not in ctx.data:
        return pd.DataFrame(out)
    values, known, totals, invalid = ctx.data[key]
    end = np.asarray((cutoffs - ctx.first).days) + 1

    def interval(back, width):
        right = end - back
        left = right - width
        boundary = (left >= 0) & (right <= len(values)) & (right > left)
        ok = np.zeros(n, dtype=bool)
        ii = np.flatnonzero(boundary)
        ok[ii] = invalid[right[ii]] == invalid[left[ii]]
        avg = np.full(n, np.nan)
        jj = np.flatnonzero(ok)
        avg[jj] = (totals[right[jj]] - totals[left[jj]]) / width
        return avg, ok

    m28, _ = interval(0, 28)
    m56, ok56 = interval(0, 56)
    m90, ok90 = interval(0, 90)
    previous28, _ = interval(28, 28)
    previous62, _ = interval(28, 62)
    early34, _ = interval(56, 34)
    for length, mean, complete, previous in [
        (56, m56, ok56, previous28), (90, m90, ok90, previous62)
    ]:
        contrast = np.full(n, np.nan)
        denom = m28 + previous
        both_zero = complete & (m28 == 0) & (previous == 0)
        positive = complete & (denom > 0)
        contrast[both_zero] = 0.0
        contrast[positive] = (m28[positive] - previous[positive]) / denom[positive]
        out["m" + str(length)] = mean
        out["contrast" + str(length)] = contrast
        out["history" + str(length) + "_complete"] = complete.astype(np.int8)
    # These are audit/evaluation columns; not in FEATURES.
    out.update(early34=early34, middle28=previous28, recent28=m28)
    return pd.DataFrame(out)


def augment(ctx, frame):
    """Append only the preregistered model fields, preserving all old columns."""
    original = frame.copy(deep=True)
    frame = frame.copy()
    for c in EXTRA:
        frame[c] = np.int8(0) if c.endswith("_complete") else np.nan
    evaluations = []
    for key, g in frame.groupby(["shop", "spu"], sort=False):
        new = features_at(ctx, key, g.input_cutoff)
        for c in EXTRA:
            frame.loc[g.index, c] = new[c].to_numpy()
        new.index = g.index
        evaluations.append(new[["early34", "middle28", "recent28"]])
    pd.testing.assert_frame_equal(frame[original.columns], original, check_exact=True)
    if len(frame) != len(original):
        raise ValueError("ROW_RETENTION")
    audit = pd.concat(evaluations).sort_index() if evaluations else pd.DataFrame(
        index=frame.index, columns=["early34", "middle28", "recent28"])
    complete = frame.history90_complete.eq(1)
    decline = (complete & audit.early34.gt(0) & audit.middle28.gt(0) &
               audit.recent28.gt(0) & audit.early34.gt(audit.middle28) &
               audit.middle28.gt(audit.recent28))
    audit["decline_group"] = np.where(~complete, "UNKNOWN",
                                    np.where(decline, "PRE_ISSUE_DECLINING", "NOT_DECLINING"))
    return frame, audit


def independent_calendar_check(ctx, frame, sample_limit=64):
    """Direct slicing versus prefix sums, sampled rows and all inference rows."""
    if not len(frame):
        return 0
    idx = np.unique(np.linspace(0, len(frame) - 1, min(sample_limit, len(frame))).astype(int))
    for row in frame.iloc[idx].itertuples():
        key = (row.shop, row.spu)
        for width in [56, 90]:
            dates = pd.date_range(row.input_cutoff - pd.Timedelta(days=width - 1), row.input_cutoff)
            expected = pd.Series(np.nan, index=dates)
            if key in ctx.data:
                vals, known, _, _ = ctx.data[key]
                series = pd.Series(np.where(known, vals, np.nan), index=ctx.dates)
                expected = series.reindex(dates)
            ok = bool(expected.notna().all())
            assert getattr(row, f"history{width}_complete") == int(ok)
            assert np.isclose(getattr(row, f"m{width}"), expected.mean() if ok else np.nan,
                              rtol=1e-12, atol=1e-10, equal_nan=True)
            if ok:
                a = expected.iloc[-28:].mean()
                b = expected.iloc[:-28].mean()
                contrast = (a-b)/(a+b) if a+b > 0 else 0.0
            else:
                contrast = np.nan
            assert np.isclose(getattr(row, f"contrast{width}"), contrast,
                              rtol=1e-12, atol=1e-10, equal_nan=True)
    return len(idx)


def prepare(archive, out, synthetic_receipt):
    archive, out, synthetic_receipt = map(lambda p: Path(p).resolve(),
                                         (archive, out, synthetic_receipt))
    if out.exists() or archive == out or archive in out.parents:
        raise ValueError("NEW_SEPARATE_OUTPUT_REQUIRED")
    tests = read_json(synthetic_receipt)
    if tests.get("status") != "PASS" or tests.get("failures") or tests.get("errors"):
        raise ValueError("SYNTHETIC_TEST_GATE")
    conf = read_json(archive/"prepared/frozen_config.json")
    for rel, expected in conf["source_hashes"].items():
        if sha(ROOT/rel) != expected:
            raise ValueError("FROZEN_SOURCE_CHANGED:" + rel)
    pm = read_json(archive/"prepared/prepare_manifest.json")
    if sha(ROOT/"scripts/bc60_monthly_replay.py") != pm["script_sha256"]:
        raise ValueError("FROZEN_EXECUTOR_CHANGED")
    verify_hashes(pm["sources"])
    for rel, expected in pm["files"].items():
        if sha(archive/"prepared"/rel) != expected:
            raise ValueError("PREPARED_CHANGED:" + rel)
    runtime = {p: importlib.metadata.version(p) for p in conf["runtime"]}
    if runtime != conf["runtime"]:
        raise ValueError("RUNTIME_CHANGED")
    grid = pd.read_csv(archive/"prepared/issue_grid.csv")
    mature = grid[grid.calendar_mature.eq(True)]
    if len(mature) * 2 > 40:
        raise ValueError("FIT_BUDGET")
    sources = {
        str(p): sha(p) for p in [
            archive/"prepared/prepare_manifest.json", archive/"prepared/frozen_config.json",
            archive/"prepared/core.parquet", archive/"prepared/issue_grid.csv",
            archive/"inputs/daily.parquet", archive/"inputs/monthly_reference.parquet",
            archive/"scoring/scores.csv", archive/"scoring/score_manifest.json",
            archive/"effects/run_manifest.json", archive/"formal_fit_ledger.json",
            ROOT/"scripts/bc60_monthly_replay.py", ROOT/"scripts/bc60_v3_direct.py",
            ROOT/"docs/direct_context90_protocol.md", synthetic_receipt,
        ]
    }
    old_manifest = read_json(archive/"effects/run_manifest.json")
    # Protect exactly the archived train audits/checkpoints/predictions we consume.
    for issue in mature.issue_date:
        for segment in ["M1", "M2"]:
            for name in ["training_audit.json", "checkpoint.json", "segment_predictions.parquet"]:
                p = archive/"effects/slots"/f"{issue}_{segment}"/name
                rel = str(p.relative_to(archive/"effects"))
                if sha(p) != old_manifest["files"][rel]:
                    raise ValueError("ORIGINAL_SLOT_CHANGED:" + rel)
                sources[str(p)] = sha(p)
    d, m = frozen.normalize(pd.read_parquet(archive/"inputs/daily.parquet"),
                            pd.read_parquet(archive/"inputs/monthly_reference.parquet"))
    core = pd.read_parquet(archive/"prepared/core.parquet")
    out.mkdir(exist_ok=False)
    (out/"prepared").mkdir()
    core.to_parquet(out/"prepared/core.parquet", index=False)
    grid.to_csv(out/"prepared/issue_grid.csv", index=False)
    dump(out/"configuration.json", {
        "candidate": "DIRECT_CONTEXT90", "preregistration_commit": PREREG_COMMIT,
        "features": FEATURES, "original_features": direct.FEATURES, "extra_features": EXTRA,
        "lightgbm": conf["original"]["lightgbm"], "runtime": runtime,
        "sample_weight": None, "missing_mechanism": "unchanged native NaN; zero_as_missing=false",
        "max_formal_fits": 40, "max_seconds_per_slot": 600,
        "classification": "RECONSTRUCTED_HISTORY_DEVELOPMENT_VALIDATION_NOT_OOS",
        "archive": str(archive), "script_sha256": sha(__file__),
        "protocol_sha256": sha(ROOT/"docs/direct_context90_protocol.md")
    })
    rows, slots, group_rows = [], [], []
    started = now()
    for issue in mature.issue_date:
        cutoff = pd.Timestamp(issue)-pd.Timedelta(days=2)
        ctx = frozen.VectorContext(d, m, frozen.SHOPS, cutoff)
        selected = core[core.issue_date.eq(issue)]
        for segment in ["M1", "M2"]:
            tic = time.monotonic()
            slot = out/"prepared"/f"{issue}_{segment}"
            slot.mkdir(exist_ok=False)
            train, _ = ctx.training(issue, segment)
            prior_path = archive/"effects/slots"/f"{issue}_{segment}"
            prior_audit = read_json(prior_path/"training_audit.json")
            actual_audit = frozen.training_audit(train, KEY, AUDIT_COLUMNS)
            if actual_audit != prior_audit:
                raise ValueError("OLD_TRAIN_COLUMNS_DIFFER:" + issue + segment)
            prior = read_json(prior_path/"checkpoint.json")
            if frozen.frame_hash(train) != prior["training_sha256"]:
                raise ValueError("OLD_TRAIN_FRAME_DIFFER:" + issue + segment)
            if train.duplicated(KEY).any() or not train.label_end.le(cutoff).all():
                raise ValueError("KEY_OR_LABEL_CUTOFF")
            assert (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all()
            full, _ = augment(ctx, train)
            train_checks = independent_calendar_check(ctx, full)
            infer = pd.DataFrame([ctx.row((r.shop, r.spu), issue, segment, issue)
                                  for r in selected.itertuples()])
            oldinf = pd.read_parquet(prior_path/"segment_predictions.parquet").set_index(["shop", "spu"])
            indexed = infer.set_index(["shop", "spu"])
            if oldinf.index.duplicated().any():
                raise ValueError("ORIGINAL_INFERENCE_DUPLICATE")
            original = oldinf.loc[indexed.index]
            pd.testing.assert_frame_equal(indexed[direct.FEATURES+["input_status"]],
                                          original[direct.FEATURES+["input_status"]],
                                          check_exact=True, check_dtype=False)
            inf, obs = augment(ctx, infer)
            inf_checks = independent_calendar_check(ctx, inf, len(inf))
            inf.to_parquet(slot/"inference.parquet", index=False)
            full.to_parquet(slot/"train.parquet", index=False)
            dump(slot/"old_training_audit.json", actual_audit)
            dump(slot/"training_check.json", {
                "status": "PASS", "old_frame_hash": prior["training_sha256"],
                "old_audit_exact": True, "old_features_exact": True,
                "old_inference_exact": True, "rows_not_dropped": len(full) == len(train),
                "weights": "uniform implicit; no sample_weight argument",
                "train_calendar_check_n": train_checks, "inference_calendar_check_n": inf_checks,
                "row_specific_cutoff": True, "labels_mature": True,
                "max_label_end": str(train.label_end.max()),
                "train_sha256": sha(slot/"train.parquet"), "inference_sha256": sha(slot/"inference.parquet")
            })
            rows.append({
                "issue_date": issue, "segment": segment, "train_n": len(train),
                "train_products": len(train[["shop", "spu"]].drop_duplicates()),
                "train_history56_complete": int(full.history56_complete.sum()),
                "train_history90_complete": int(full.history90_complete.sum()),
                "train_nan56_retained": int(full.m56.isna().sum()),
                "train_nan90_retained": int(full.m90.isna().sum()),
                "inference_n": len(inf), "inference_eligible_n": int(inf.input_status.eq("OK").sum()),
                "inference_history56_complete": int(inf.history56_complete.sum()),
                "inference_history90_complete": int(inf.history90_complete.sum()),
                "old_columns_match": True, "old_frame_match": True, "weights_unchanged": True,
                "seconds": time.monotonic()-tic,
            })
            slots.append({"issue_date": issue, "segment": segment, "state": "PLANNED"})
            if segment == "M1":
                q = selected[["cohort_id", "issue_date", "shop", "spu", "maturity", "lifecycle"]].reset_index(drop=True)
                q = pd.concat([q, inf[EXTRA].reset_index(drop=True), obs.reset_index(drop=True)], axis=1)
                group_rows.append(q)
            print(json.dumps({"prepared": f"{issue}_{segment}", **rows[-1]}, ensure_ascii=False), flush=True)
    pd.DataFrame(rows).to_csv(out/"training_reconciliation.csv", index=False, encoding="utf-8-sig")
    pd.concat(group_rows, ignore_index=True).to_parquet(out/"observation_groups.parquet", index=False)
    pd.concat(group_rows, ignore_index=True).to_csv(out/"observation_groups.csv", index=False, encoding="utf-8-sig")
    verify_hashes(sources)
    dump(out/"prepare_manifest.json", {
        "started_at": started, "completed_at": now(), "sources": sources,
        "script_sha256": sha(__file__), "preregistration_commit": PREREG_COMMIT,
        "formal_fit_count": 0, "old_archives_unchanged": True,
        "files": {str(p.relative_to(out)): sha(p) for p in out.rglob("*") if p.is_file()}
    })
    dump(out/"formal_fit_ledger.json", {"created_at": now(), "max_formal_fits": 40, "slots": slots})
    print(json.dumps({"status": "PREPARED_NO_EFFECTS", "slots": len(slots), "formal_fits": 0}), flush=True)


def worker(out, issue, segment, recovery=False):
    from lightgbm import LGBMRegressor, Booster
    out = Path(out).resolve()
    conf = read_json(out/"configuration.json")
    prep = out/"prepared"/f"{issue}_{segment}"
    slot = out/"effects"/f"{issue}_{segment}"
    if not slot.exists():
        slot.mkdir(parents=True, exist_ok=False)
    check = read_json(prep/"training_check.json")
    if sha(prep/"train.parquet") != check["train_sha256"] or sha(prep/"inference.parquet") != check["inference_sha256"]:
        raise ValueError("PREPARED_SLOT_CHANGED")
    train = pd.read_parquet(prep/"train.parquet")
    inf = pd.read_parquet(prep/"inference.parquet")
    if conf["features"] != FEATURES or len(train) == 0:
        raise ValueError("FROZEN_FEATURES_OR_EMPTY_TRAIN")
    relative = os.path.relpath(slot/"model.txt", ROOT)
    relative.encode("ascii")
    if recovery:
        checkpoint = read_json(slot/"checkpoint.json")
        if sha(slot/"model.txt") != checkpoint["model_sha256"]:
            raise ValueError("CHECKPOINT_CHANGED")
        model = Booster(model_file=relative)
    else:
        with (slot/"fit_started.json").open("x", encoding="utf-8") as f:
            json.dump({"at": now(), "issue_date": issue, "segment": segment,
                       "train_hash": check["train_sha256"], "features": FEATURES}, f)
        model = LGBMRegressor(**conf["lightgbm"])
        tic = time.monotonic()
        model.fit(train[FEATURES], train.y_label)
        seconds = time.monotonic()-tic
        model.booster_.save_model(relative)
        checkpoint = {
            "completed_at": now(), "issue_date": issue, "segment": segment,
            "fit_seconds": seconds, "model_sha256": sha(slot/"model.txt"),
            "training_sha256": check["train_sha256"], "features": FEATURES,
            "params": conf["lightgbm"], "sample_weight": None,
            "model_features": model.booster_.feature_name(),
        }
        dump(slot/"checkpoint.json", checkpoint)
    good = inf.input_status.eq("OK")
    pred = np.full(len(inf), np.nan)
    if good.any():
        pred[good] = model.predict(inf.loc[good, FEATURES])
        if not np.isfinite(pred[good]).all():
            raise ValueError("NONFINITE_OUTPUT")
    inf["prediction_raw"] = pred
    inf["prediction"] = np.maximum(pred, 0)
    inf["prediction_status"] = np.where(good, "OK", inf.input_status)
    dest = slot/"predictions.parquet"
    if dest.exists():
        raise ValueError("PREDICTION_OUTPUT_EXISTS")
    inf.to_parquet(dest, index=False)
    dump(slot/"result.json", {
        "status": "RECOVERED" if recovery else "FITTED", "issue_date": issue, "segment": segment,
        "completed_at": now(), "fit_seconds": checkpoint["fit_seconds"],
        "n": len(inf), "output_n": int(np.isfinite(inf.prediction).sum()),
        "clipped_negative_n": int((pred < 0).sum()), "prediction_sha256": sha(dest),
        "model_sha256": checkpoint["model_sha256"],
    })


def fit(out):
    out = Path(out).resolve()
    prepared = read_json(out/"prepare_manifest.json")
    conf = read_json(out/"configuration.json")
    if prepared["script_sha256"] != sha(__file__) or conf["script_sha256"] != sha(__file__):
        raise ValueError("EXECUTOR_CHANGED_AFTER_FREEZE")
    verify_hashes(prepared["sources"])
    for rel, expected in prepared["files"].items():
        if sha(out/rel) != expected:
            raise ValueError("PRE_EFFECT_FILE_CHANGED:" + rel)
    if {p: importlib.metadata.version(p) for p in conf["runtime"]} != conf["runtime"]:
        raise ValueError("RUNTIME_CHANGED")
    ledger_path = out/"formal_fit_ledger.json"
    ledger = read_json(ledger_path)
    if len(ledger["slots"]) > 40:
        raise ValueError("FIT_BUDGET")
    started = now()
    for entry in ledger["slots"]:
        issue, segment = entry["issue_date"], entry["segment"]
        slot = out/"effects"/f"{issue}_{segment}"
        if (slot/"result.json").exists():
            result = read_json(slot/"result.json")
            if (sha(slot/"predictions.parquet") != result["prediction_sha256"]
                    or sha(slot/"model.txt") != result["model_sha256"]):
                raise ValueError("COMPLETED_RESULT_CHANGED")
            entry.update(state=result["status"], result=result)
            dump(ledger_path, ledger)
            continue
        recovery = (slot/"checkpoint.json").exists()
        if (slot/"fit_started.json").exists() and not recovery:
            raise ValueError("STARTED_SLOT_WITHOUT_CHECKPOINT_NO_REFIT")
        if entry["state"] != "PLANNED" and not recovery:
            raise ValueError("RESERVED_SLOT_NO_AUTOMATIC_RETRY")
        entry.update(state="RECOVERING" if recovery else "RESERVED", reserved_at=now())
        dump(ledger_path, ledger)
        command = [sys.executable, "-B", str(Path(__file__).resolve()), "_worker",
                   "--out", str(out), "--issue", issue, "--segment", segment]
        if recovery:
            command.append("--recovery")
        try:
            process = subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                                     encoding="utf-8", errors="replace", timeout=600,
                                     env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
                                          "PYTHONIOENCODING": "utf-8"})
            suffix = ".recovery.log" if recovery else ".log"
            log = out/"effects"/f"{issue}_{segment}{suffix}"
            log.parent.mkdir(parents=True, exist_ok=True)
            with log.open("x", encoding="utf-8") as f:
                f.write(process.stdout + "\n" + process.stderr)
            if process.returncode != 0:
                raise RuntimeError("WORKER_FAILED:" + str(process.returncode))
            result = read_json(slot/"result.json")
            entry.update(state=result["status"], result=result)
            dump(ledger_path, ledger)
            print(json.dumps(result), flush=True)
        except Exception as error:
            entry.update(state="TECHNICAL_FAILURE", error=str(error), stopped_at=now())
            dump(ledger_path, ledger)
            raise
    verify_hashes(prepared["sources"])
    fits = list((out/"effects").glob("*/fit_started.json"))
    if len(fits) > 40:
        raise ValueError("FIT_BUDGET")
    dump(out/"effects_manifest.json", {
        "started_at": started, "completed_at": now(), "formal_fits": len(fits),
        "source_unchanged": True, "script_sha256": sha(__file__),
        "prepare_manifest_sha256": sha(out/"prepare_manifest.json"),
        "files": {str(p.relative_to(out)): sha(p) for p in (out/"effects").rglob("*") if p.is_file()}
    })
    print(json.dumps({"status": "COMPLETED", "formal_fits": len(fits)}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--archive", required=True); p.add_argument("--out", required=True)
    p.add_argument("--synthetic-receipt", required=True)
    p = sub.add_parser("fit"); p.add_argument("--out", required=True)
    p = sub.add_parser("_worker")
    p.add_argument("--out", required=True); p.add_argument("--issue", required=True)
    p.add_argument("--segment", required=True); p.add_argument("--recovery", action="store_true")
    args = vars(parser.parse_args()); command = args.pop("command")
    {"prepare": prepare, "fit": fit, "_worker": worker}[command](**args)
