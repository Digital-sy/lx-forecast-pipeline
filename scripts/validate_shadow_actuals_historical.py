"""Read-only historical acceptance check for the pure mature-actual scorer.

No fit/predict, query, or future-actual access.  Inputs remain in their archive;
only new summaries, differences, and a receipt are written to a new directory.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from scripts.shadow_actuals_scoring import score_windows
except ModuleNotFoundError:
    from shadow_actuals_scoring import score_windows


MODELS = {"F28", "DIRECT", "BLEND50", "DIRECT_L1_V1", "BLEND50_L1_MEAN_V1"}
KEY = ["issue_date", "shop", "spu"]
METRIC_FIELDS = ["fixed_n", "output_n", "truth_n", "scored_n", "zero_truth_n",
    "unknown_truth_n", "missing_output_n", "actual_units", "ae", "wape", "bias",
    "over_units", "under_units", "over_rate", "hit_n", "hit_fixed_share", "hit_actual",
    "hit_actual_share", "zero_truth_ae", "products", "periods", "prediction_units",
    "output_prediction_units", "positive_actual_zero_prediction_n", "positive_actual_zero_prediction_actual"]
JOINT_FIELDS = ["fixed_n", "output_complete_n", "truth_complete_n", "joint_hit_n",
    "joint_hit_fixed_share", "joint_hit_actual", "known_actual", "joint_hit_actual_share", "products", "periods"]
INTEGER_FIELDS = {"fixed_n", "output_n", "truth_n", "scored_n", "zero_truth_n", "unknown_truth_n",
    "missing_output_n", "hit_n", "products", "periods", "positive_actual_zero_prediction_n",
    "output_complete_n", "truth_complete_n", "joint_hit_n"}


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def differences(new: pd.DataFrame, old: pd.DataFrame, key: list[str], fields: list[str], kind: str):
    if new.duplicated(key).any() or old.duplicated(key).any():
        raise ValueError("duplicate metric key")
    pairs = new[key + fields].merge(old[key + fields], on=key, how="outer", suffixes=("_new", "_archive"),
                                    indicator=True, validate="one_to_one")
    if not pairs._merge.eq("both").all():
        raise ValueError("metric population changed")
    result = []
    for _, row in pairs.iterrows():
        for field in fields:
            current, archived = float(row[field + "_new"]), float(row[field + "_archive"])
            both_nan = bool(np.isnan(current) and np.isnan(archived))
            tolerance = 0.0 if field in INTEGER_FIELDS else max(1e-9, abs(archived) * 1e-12)
            difference = current - archived
            passed = both_nan or (np.isfinite(current) and np.isfinite(archived) and abs(difference) <= tolerance)
            result.append({k: row[k] for k in key} | dict(kind=kind, field=field, archive_value=archived,
                reproduced_value=current, difference=difference, absolute_difference=abs(difference),
                tolerance=tolerance, passed=bool(passed)))
    return pd.DataFrame(result)


def validate(source: Path, out: Path) -> dict:
    source, out = source.resolve(), out.resolve()
    if out.exists():
        raise ValueError("output directory already exists; refusing overwrite")
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest_path = source / "scoring/manifest.json"
    validation_path = source / "validation/validation.json"
    manifest, old_validation = read(manifest_path), read(validation_path)
    if old_validation.get("status") != "PASS":
        raise ValueError("historical independent validation is not PASS")
    files = {"scoring/scores.parquet": manifest["files"]["scores.parquet"],
             "scoring/metrics.parquet": manifest["files"]["metrics.parquet"],
             "scoring/joint_summary.parquet": manifest["files"]["joint_summary.parquet"],
             "candidate_manifest.json": manifest["candidate_manifest_sha256"]}
    input_hashes = {str(manifest_path): sha(manifest_path), str(validation_path): sha(validation_path)}
    for relative, expected in files.items():
        path = source / relative
        actual = sha(path)
        if actual != expected:
            raise ValueError("archive hash mismatch: " + relative)
        input_hashes[str(path)] = actual
    # Link expected metrics back to the prior independent review, not just a new
    # self-consistent manifest assembled from whichever files are on disk today.
    reviewed = {str(Path(path).resolve()): digest for path, digest in old_validation["sources"].items()}
    for relative in ("scoring/metrics.parquet", "scoring/joint_summary.parquet", "candidate_manifest.json"):
        path = str((source / relative).resolve())
        if reviewed.get(path) != input_hashes[path]:
            raise ValueError("prior validation does not bind " + relative)
    archive = pd.read_parquet(source / "scoring/scores.parquet")
    if set(archive.model.unique()) != MODELS:
        raise ValueError("five historical model scope changed")
    if archive.duplicated(KEY + ["model", "window"]).any():
        raise ValueError("duplicate historical prediction")
    dates = pd.to_datetime(archive.target_end)
    if dates.max().date() >= dt.datetime.now(dt.timezone.utc).date():
        raise ValueError("this validator only accepts already completed historical targets")
    source_truth = archive[KEY + ["window", "target_start", "target_end", "actual", "truth_status"]].drop_duplicates()
    if source_truth.duplicated(KEY + ["window"]).any() or not source_truth.truth_status.eq("OBSERVED").all():
        raise ValueError("historical comparator truth conflicts or is not observed")
    truth = source_truth.rename(columns={"truth_status": "actual_status"})
    truth["actual_status"] = "COMPLETE"
    predictions = archive[KEY + ["model", "window", "target_start", "target_end", "prediction", "prediction_status"]]
    identities = archive[KEY + ["paired"]].drop_duplicates()
    if identities.duplicated(KEY).any():
        raise ValueError("historical paired flag conflicts")
    pairs = identities.loc[identities.paired, KEY]
    if len(identities) != old_validation["full_core_n"] or len(pairs) != old_validation["paired_n"]:
        raise ValueError("historical fixed scope changed")
    result = score_windows(predictions, truth, pairs)
    old_metrics = pd.read_parquet(source / "scoring/metrics.parquet")
    old_joint = pd.read_parquet(source / "scoring/joint_summary.parquet")
    old_metrics = old_metrics[old_metrics.slice.eq("overall") & old_metrics.model.isin(MODELS)]
    old_joint = old_joint[old_joint.slice.eq("overall") & old_joint.model.isin(MODELS)]
    metric_diff = differences(result["metrics"], old_metrics, ["scope", "slice", "model", "window"], METRIC_FIELDS, "window")
    joint_diff = differences(result["joint_summary"], old_joint, ["scope", "slice", "model"], JOINT_FIELDS, "joint")
    checks = pd.concat([metric_diff, joint_diff], ignore_index=True)
    all_hashes_unchanged = all(sha(Path(path)) == digest for path, digest in input_hashes.items())
    if not all_hashes_unchanged:
        raise ValueError("read-only archive changed during verification")
    out.mkdir(parents=True, exist_ok=False)
    checks.to_csv(out / "numeric_differences.csv", index=False, encoding="utf-8-sig")
    result["metrics"].to_csv(out / "reproduced_metrics.csv", index=False, encoding="utf-8-sig")
    result["joint_summary"].to_csv(out / "reproduced_joint_summary.csv", index=False, encoding="utf-8-sig")
    result["coverage"].to_csv(out / "historical_truth_coverage.csv", index=False, encoding="utf-8-sig")
    receipt = dict(status="PASS" if checks.passed.all() else "FAIL", started_at=started,
        completed_at=dt.datetime.now(dt.timezone.utc).isoformat(), models=sorted(MODELS),
        full_core_n=len(identities), paired_n=len(pairs), missing_output_n=len(identities) - len(pairs),
        periods=int(identities.issue_date.nunique()), first_issue_date=str(identities.issue_date.min()),
        last_issue_date=str(identities.issue_date.max()), window_metric_rows=len(result["metrics"]),
        joint_metric_rows=len(result["joint_summary"]), compared_numeric_fields=len(checks),
        failed_fields=int((~checks.passed).sum()), maximum_absolute_difference=float(checks.absolute_difference.max()),
        historical_truth_adapter="OBSERVED mature three-window actuals -> COMPLETE; tied to prior independent PASS and exact archive hashes",
        current_shadow_future_actuals_read=False, source_files_unchanged=all_hashes_unchanged,
        training_calls=0, model_inference_calls=0, database_queries=0, api_calls=0, production_writes=0,
        sources=input_hashes, script_sha256=sha(Path(__file__)),
        scoring_script_sha256=sha(Path(__file__).with_name("shadow_actuals_scoring.py")),
        files={path.name: sha(path) for path in out.iterdir() if path.is_file()})
    (out / "validation.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    receipt = validate(arguments.source, arguments.out)
    print(json.dumps({k: receipt[k] for k in ("status", "full_core_n", "paired_n", "compared_numeric_fields",
                     "failed_fields", "maximum_absolute_difference")}, ensure_ascii=False))
    raise SystemExit(0 if receipt["status"] == "PASS" else 1)
