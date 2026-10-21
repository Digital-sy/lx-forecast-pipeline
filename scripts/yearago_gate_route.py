"""Truth-free selection of archived predictions; never imports model code."""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd

KEY = ["issue_date", "shop", "spu"]
RULE = "DIRECT_YEARAGO_GATE_V1"
REVIEWED_SHA = "1ec7112e46c461fab01a878ca46ed3dd89b7382089c439afba87864729ed4019"
PREREG = "8d898a0"


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def save(dest, name, frame):
    frame.to_parquet(dest / (name + ".parquet"), index=False)
    frame.to_csv(dest / (name + ".csv"), index=False, encoding="utf-8-sig")


def verify(sources):
    for p, expected in sources.items():
        if sha(p) != expected:
            raise ValueError("HASH_MISMATCH: " + p)


def unique(frame, keys):
    if frame.duplicated(keys).any():
        raise ValueError("DUPLICATE_KEY: " + str(keys))


def annual_bounds(issue, segment):
    s = pd.Timestamp(issue)
    try:
        a = s.replace(year=s.year - 1)
    except ValueError:
        if (s.month, s.day) != (2, 29):
            raise
        a = s.replace(year=s.year - 1, day=28)
    c = a - pd.Timedelta(days=2)
    start = a + pd.Timedelta(days={"M1": 1, "M2": 31}[segment])
    return c - pd.Timedelta(days=27), c, start, start + pd.Timedelta(days=29)


def finite(value):
    return pd.notna(value) and bool(np.isfinite(value))


def choose(e):
    """Only these six condition fields determine the route; unknown fails closed."""
    conditions = ["direct_valid", "input_valid", "targets_valid", "prior_positive",
                  "both_supported", "both_annual"]
    failed = [k for k in conditions if pd.isna(e[k]) or not bool(e[k])]
    yes = not failed
    prefix = "annual" if yes else "direct"
    return {"gate_decision": yes, "failed_conditions": "|".join(failed),
            "main_reason": failed[0] if failed else "ALL_CONDITIONS_MET",
            "prediction_origin": "ANNUAL_BOTH" if yes else "DIRECT_BOTH",
            "selected_M1": e[prefix + "_M1"], "selected_M2": e[prefix + "_M2"]}


def route(yearago, destination):
    y, out = Path(yearago).resolve(), Path(destination).resolve()
    if out.exists() or y in out.parents:
        raise ValueError("EXCLUSIVE_NEW_OUTPUT_REQUIRED")
    conf = read_json(y / "configuration.json")
    archive = Path(conf["archive"])
    reviewed = y / "scoring_reviewed"
    if sha(reviewed / "score_manifest.json") != REVIEWED_SHA:
        raise ValueError("REVIEWED_MANIFEST_CHANGED")
    sources = {}

    def protect(path, expected=None):
        path = Path(path).resolve()
        h = sha(path)
        if expected is not None and h != expected:
            raise ValueError("HASH_MISMATCH: " + str(path))
        sources[str(path)] = h

    # Byte hashing is integrity checking, not reading truth into rule inputs.
    for path, base in [(y / "prepare_manifest.json", y),
                       (y / "effects_manifest.json", y),
                       (reviewed / "score_manifest.json", reviewed),
                       (archive / "effects/run_manifest.json", archive / "effects")]:
        protect(path)
        m = read_json(path)
        for p, h in m.get("sources", {}).items():
            protect(p, h)
        for p, h in m.get("files", {}).items():
            protect(base / p, h)
    protect(y / "configuration.json")
    protect(Path(__file__))
    protect(Path(__file__).resolve().parents[1] / "docs/yearago_gate_v1_protocol.md")
    log = []

    def parquet(path, columns, filters=None, purpose="rule evidence"):
        forbidden = {"actual", "truth_status", "error", "hit", "absolute_error", "joint_hit"}
        if forbidden.intersection(columns):
            raise ValueError("FORBIDDEN_ROUTING_COLUMN")
        if str(Path(path).resolve()) not in sources:
            protect(path)
        log.append({"at": now(), "path": str(path), "columns": columns,
                    "filters": str(filters), "purpose": purpose})
        return pd.read_parquet(path, columns=columns, filters=filters)

    core = parquet(y / "prepared/core.parquet", KEY)
    grid = pd.read_csv(y / "prepared/issue_grid.csv")
    issues = grid.loc[grid.calendar_mature, "issue_date"].tolist()
    core = core[core.issue_date.isin(issues)]
    unique(core, KEY)
    support = pd.read_csv(y / "annual_support.csv", float_precision="round_trip")
    unique(support, ["issue_date", "segment"])
    support = support.set_index(["issue_date", "segment"])
    pcols = KEY + ["segment", "annual_model_prediction", "comparison_prediction",
                   "prediction_origin", "original_prediction_sha256", "prediction_status"]
    ps = parquet(reviewed / "segment_predictions.parquet", pcols)
    unique(ps, KEY + ["segment"])
    rows = []
    for issue in issues:
        keys = core[core.issue_date.eq(issue)]
        cutoff = pd.Timestamp(issue) - pd.Timedelta(days=2)
        istart = annual_bounds(issue, "M1")[0]
        # Each issue is separately bounded; no final issue cutoff substitutes here.
        d = parquet(archive / "inputs/daily.parquet",
                    ["shop", "spu", "ds", "y", "mapped", "sales_state"],
                    [("ds", "<=", istart)], "earliest positive on/before annual input start")
        unique(d, ["shop", "spu", "ds"])
        good = d.mapped.eq(True) & d.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]) & np.isfinite(d.y) & d.y.gt(0)
        earliest = d[good].groupby(["shop", "spu"]).ds.min().to_dict()
        del d
        slots = {}
        for segment in ["M1", "M2"]:
            base = y / "prepared" / f"{issue}_{segment}"
            check = read_json(base / "training_check.json")
            sup = support.loc[(issue, segment)]
            for k, v in check["support"].items():
                if k in ["support_first", "support_last"]:
                    continue
                if sup[k] != v:
                    raise ValueError("SUPPORT_CONTRADICTION")
            allowed = sup.support_dates >= 30 and sup.support_products >= 10 and bool(sup.numerical_features_vary)
            if bool(sup.fit_allowed) != allowed:
                raise ValueError("SUPPORT_SCREEN_CONTRADICTION")
            wcols = ["shop", "spu", "input_cutoff", "ly_positive_observed_before_input_start"]
            for part in ["input", "target"]:
                wcols += [f"ly_{part}_{s}" for s in ["start", "end", "complete", "unknown_days", "absent_days", "unsafe_days", "zero_days"]]
            w = parquet(base / "inference_windows.parquet", wcols)
            unique(w, ["shop", "spu"])
            if not pd.to_datetime(w.input_cutoff).eq(cutoff).all():
                raise ValueError("CUTOFF_CONTRADICTION")
            for c, expected in zip(["ly_input_start", "ly_input_end", "ly_target_start", "ly_target_end"], annual_bounds(issue, segment)):
                if not pd.to_datetime(w[c]).eq(expected).all() or expected > cutoff:
                    raise ValueError("WINDOW_CONTRADICTION")
            for part in ["input", "target"]:
                if not w[f"ly_{part}_complete"].eq(w[f"ly_{part}_unknown_days"].eq(0)).all():
                    raise ValueError("WINDOW_STATUS_CONTRADICTION")
                if not w[f"ly_{part}_unknown_days"].eq(w[f"ly_{part}_absent_days"] + w[f"ly_{part}_unsafe_days"]).all():
                    raise ValueError("WINDOW_SOURCE_CONTRADICTION")
            bp = Path(check["original_prediction_path"])
            protect(bp, check["original_prediction_sha256"])
            old = parquet(bp, ["shop", "spu", "prediction"])
            unique(old, ["shop", "spu"])
            orig = ps[ps.issue_date.eq(issue) & ps.segment.eq(segment)].copy()
            if not orig.original_prediction_sha256.eq(sha(bp)).all():
                raise ValueError("PREDICTION_SOURCE_CONTRADICTION")
            q = keys.merge(w, on=["shop", "spu"], validate="one_to_one").merge(
                old.rename(columns={"prediction": "direct"}), on=["shop", "spu"], validate="one_to_one").merge(
                orig, on=KEY, validate="one_to_one")
            if len(q) != len(keys):
                raise ValueError("MISSING_RULE_KEY")
            q["supported"] = allowed
            q["support_dates"] = int(sup.support_dates)
            q["support_products"] = int(sup.support_products)
            q["numerical_features_vary"] = bool(sup.numerical_features_vary)
            q["window_sha256"] = sha(base / "inference_windows.parquet")
            slots[segment] = q.set_index(["shop", "spu"])
        for key in keys.itertuples(index=False):
            a, b = [slots[s].loc[(key.shop, key.spu)] for s in ["M1", "M2"]]
            ep = earliest.get((key.shop, key.spu), pd.NaT)
            prior_positive = pd.notna(ep)
            if any(bool(r.ly_positive_observed_before_input_start) != prior_positive for r in [a, b]):
                raise ValueError("POSITIVE_EVIDENCE_CONTRADICTION")
            for c in ["start", "end", "complete", "unknown_days", "absent_days", "unsafe_days", "zero_days"]:
                if a[f"ly_input_{c}"] != b[f"ly_input_{c}"]:
                    raise ValueError("INPUT_WINDOW_DISAGREEMENT")
            e = {**dict(zip(KEY, key)), "input_cutoff": cutoff,
                 "first_positive_evidence_date": ep, "prior_positive": prior_positive,
                 "direct_valid": finite(a.direct) and finite(b.direct),
                 "input_valid": bool(a.ly_input_complete) and a.ly_input_unsafe_days == 0,
                 "targets_valid": all(bool(r.ly_target_complete) and r.ly_target_unsafe_days == 0 for r in [a, b]),
                 "both_supported": bool(a.supported and b.supported),
                 "both_annual": all(r.prediction_origin == "ANNUAL_MODEL" and finite(r.annual_model_prediction) for r in [a, b])}
            for part, r in [("M1", a), ("M2", b)]:
                e[f"direct_{part}"] = r.direct
                e[f"annual_{part}"] = r.annual_model_prediction
                e[f"comparison_{part}"] = r.comparison_prediction
                e[f"origin_{part}"] = r.prediction_origin
                e[f"direct_source_sha256_{part}"] = r.original_prediction_sha256
                e[f"annual_source_sha256_{part}"] = sha(y / "effects" / f"{issue}_{part}" / "predictions.parquet")
                for c in ["supported", "support_dates", "support_products", "numerical_features_vary", "window_sha256"]:
                    e[f"{c}_{part}"] = r[c]
                for c in wcols[2:]:
                    e[f"{c}_{part}"] = r[c]
                # Validate fallback before allowing any route.
                if r.prediction_origin == "ANNUAL_MODEL":
                    if not r.supported or not finite(r.annual_model_prediction) or r.comparison_prediction != r.annual_model_prediction:
                        raise ValueError("ANNUAL_ORIGIN_CONTRADICTION")
                elif finite(r.direct):
                    if r.supported or finite(r.annual_model_prediction) or r.comparison_prediction != r.direct:
                        raise ValueError("FALLBACK_CONTRADICTION")
            e.update(choose(e))
            e["selected_60"] = e["selected_M1"] + e["selected_M2"]
            rows.append(e)
        print(json.dumps({"routing_issue": issue, "objects": len(keys)}), flush=True)
    result = pd.DataFrame(rows)
    unique(result, KEY)
    verify(sources)
    out.mkdir(parents=True, exist_ok=False)
    save(out, "routing", result)
    dump(out / "routing_access_log.json", log)
    dump(out / "routing_manifest.json", {
        "rule": RULE, "preregistration_commit": PREREG, "sealed_at": now(),
        "routing_sha256": sha(out / "routing.parquet"), "sources": sources,
        "yearago": str(y), "archive": str(archive), "rule_source_sha256": sha(__file__),
        "access_log_sha256": sha(out / "routing_access_log.json"),
        "objects": len(result), "true_objects": int(result.gate_decision.sum()),
        "new_fits": 0, "new_inference": 0, "database_queries": 0,
        "classification": "POST_HOC_RECONSTRUCTED_DEVELOPMENT_REPLAY"})
    print(json.dumps({"status": "ROUTING_SEALED", "n": len(result), "true": int(result.gate_decision.sum())}))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--yearago", required=True)
    p.add_argument("--destination", required=True)
    route(**vars(p.parse_args()))
