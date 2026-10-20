"""Read-only archived DIRECT/F28 analysis; no forecasting or source connections."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

KEY = ["cohort_id", "issue_date", "shop", "spu"]
WIN = ["d01_30", "d31_60", "d01_60"]
MODELS = {"f28": "F28", "direct": "DIRECT30x2"}
DIMENSIONS = {
    "overall": [],
    "season": ["current_season"],
    "category": ["current_category"],
    "issue": ["issue_month"],
    "calendar": ["calendar_month"],
    "shop": ["shop"],
    "maturity": ["maturity"],
    "category_issue": ["current_category", "issue_month"],
    "shop_category_issue": ["shop", "current_category", "issue_month"],
    "shop_category_issue_age": ["shop", "current_category", "issue_month", "maturity"],
    "shop_category": ["shop", "current_category"],
    "shop_category_age": ["shop", "current_category", "maturity"],
    "category_age": ["current_category", "maturity"],
    "season_issue": ["current_season", "issue_month"],
    "shop_season": ["shop", "current_season"],
    "category_season": ["current_category", "current_season"],
    "shop_category_season": ["shop", "current_category", "current_season"],
    "shop_category_season_age": ["shop", "current_category", "current_season", "maturity"],
}
UNKNOWN = {"", "UNKNOWN", "CONFLICT", "未映射"}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False), encoding="utf-8")


def csv(path, frame):
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def div(a, b):
    return float(a / b) if pd.notna(b) and b > 0 else np.nan


def groups(frame, dims):
    if not dims:
        yield {}, frame
    else:
        for values, g in frame.groupby(dims, dropna=False, sort=True):
            if not isinstance(values, tuple):
                values = (values,)
            yield dict(zip(dims, values)), g


def unique(frame, cols, label):
    if frame.duplicated(cols).any():
        raise ValueError(f"{label}: duplicate key {cols}")


def equal_series(a, b, label):
    if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
        ok = np.isclose(a, b, atol=1e-8, rtol=1e-12, equal_nan=True)
    else:
        ok = a.fillna("<NULL>").astype(str).eq(b.fillna("<NULL>").astype(str))
    if not np.all(ok):
        raise ValueError(f"{label}: unequal {int((~ok).sum())} values")


def normalize_metadata(raw):
    """Resolve conflicts field-wise without choosing an arbitrary source row."""
    rows = []
    for (shop, spu), g in raw.groupby(["shop", "spu"], dropna=False, sort=True):
        row = dict(shop=shop, spu=spu, metadata_rows=len(g), label_basis="CURRENT_NOT_POINT_IN_TIME")
        for field in ["current_category", "current_season"]:
            vals = sorted(set(g[field].dropna().astype(str)) - {""})
            # Keep the raw sentinel 未映射 in display rather than silently renaming it.
            known = sorted(set(vals) - {"", "UNKNOWN", "CONFLICT"})
            source_conflict = ("CONFLICT" in vals or
                               g.get(field + "_state", pd.Series([], dtype=str)).astype(str).str.contains("CONFLICT").any())
            row[field] = "CONFLICT" if source_conflict or len(known) > 1 else known[0] if known else "UNKNOWN"
            row[field + "_raw_values"] = json.dumps(vals, ensure_ascii=False)
            row[field + "_source_states"] = json.dumps(sorted(g.get(field + "_state", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()))
            row[field + "_source_values"] = json.dumps(sorted(g.get(field + "_values", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()), ensure_ascii=False)
        rows.append(row)
    return pd.DataFrame(rows)


def classify(values, mature=True):
    a = np.array([values["a1"], values["a2"], values["a60"]], dtype=float)
    p = np.array([values["p1"], values["p2"], values["p60"]], dtype=float)
    if not mature:
        return "NOT_MATURED"
    if not np.isfinite(a).all():
        return "MISSING_TRUTH"
    if not np.isfinite(p).all():
        return "MISSING_PREDICTION"
    if (a <= 0).any():
        return "ZERO_ACTUAL"
    e = (p - a) / a
    hits = np.abs(e) <= .2
    if hits[0] and hits[1]:
        if not hits[2] and abs(e[2]) - .2 > 1e-12:
            raise ValueError("segment hits contradict cumulative hit")
        return "BOTH_SEGMENTS_HIT"
    if hits[2]:
        return "CUMULATIVE_HIT_SEGMENTS_MISS"
    if e[0] > .2 and e[1] > .2:
        return "BOTH_OVER_20"
    if e[0] < -.2 and e[1] < -.2:
        return "BOTH_UNDER_20"
    return "MIXED_TOTAL_MISS"


def metric(g, prefix):
    p, a = g[prefix + "_prediction"], g.actual
    valid = p.notna() & a.notna()
    positive = a.gt(0)
    error = p - a
    hit = valid & positive & error.abs().le(.2 * a)
    denom = a[valid].sum(min_count=1)
    ae = error[valid].abs().sum(min_count=1)
    over = error[valid].clip(lower=0).sum(min_count=1)
    under = (-error[valid]).clip(lower=0).sum(min_count=1)
    total = a.sum(min_count=1)
    return dict(
        n=len(g), products=g[["shop", "spu"]].drop_duplicates().shape[0],
        issues=g.issue_date.nunique(), truth_n=int(a.notna().sum()),
        output_n=int(p.notna().sum()), scored_n=int(valid.sum()),
        positive_truth_n=int(positive.sum()), zero_truth_n=int(a.eq(0).sum()),
        actual_units=total, scored_actual_units=denom,
        predicted_units=p.sum(min_count=1), absolute_error=ae,
        signed_error=error[valid].sum(min_count=1), over_units=over, under_units=under,
        wape=div(ae, denom), bias=div(error[valid].sum(min_count=1), denom),
        hit_n=int(hit.sum()), hit_rate=div(hit.sum(), positive.sum()),
        hit_units=a[hit].sum(), hit_sales_share=div(a[hit].sum(), total),
        missed_positive_units=a[positive & ~hit].sum(),
        zero_actual_absolute_error=error[a.eq(0) & valid].abs().sum(min_count=1),
        output_coverage=div(p.notna().sum(), len(g)),
        category_known_n=int((~g.current_category.isin(UNKNOWN)).sum()),
        season_known_n=int((~g.current_season.isin(UNKNOWN)).sum()),
        category_unknown_n=int(g.current_category.isin(["UNKNOWN","未映射"]).sum()),
        category_conflict_n=int(g.current_category.eq("CONFLICT").sum()),
        season_unknown_n=int(g.current_season.isin(["UNKNOWN","未映射"]).sum()),
        season_conflict_n=int(g.current_season.eq("CONFLICT").sum()),
    )


def pair_stats(g):
    z = g[g.paired].copy()
    n = len(z)
    ae_f = (z.f28_prediction - z.actual).abs()
    ae_d = (z.direct_prediction - z.actual).abs()
    delta = ae_d - ae_f
    gain, loss, unchanged = delta.lt(0), delta.gt(0), delta.eq(0)
    prod = z[["shop", "spu"]].drop_duplicates().shape[0]
    periods = z.issue_date.nunique()
    return dict(
        full_n=len(g), pair_n=n, pair_products=prod, pair_issues=periods,
        pair_actual_units=z.actual.sum(min_count=1),
        excluded_pair_n=len(g)-n,
        excluded_pair_actual_units=g.loc[~g.paired, "actual"].sum(min_count=1),
        improved_n=int(gain.sum()), degraded_n=int(loss.sum()), unchanged_n=int(unchanged.sum()),
        improved_products=z.loc[gain,["shop","spu"]].drop_duplicates().shape[0],
        degraded_products=z.loc[loss,["shop","spu"]].drop_duplicates().shape[0],
        unchanged_products=z.loc[unchanged,["shop","spu"]].drop_duplicates().shape[0],
        improved_units=z.loc[gain, "actual"].sum(), degraded_units=z.loc[loss, "actual"].sum(),
        unchanged_units=z.loc[unchanged, "actual"].sum(),
        improved_sales_share=div(z.loc[gain, "actual"].sum(), z.actual.sum()),
        degraded_sales_share=div(z.loc[loss, "actual"].sum(), z.actual.sum()),
        improvement_units=(-delta[gain]).sum(), degradation_units=delta[loss].sum(),
        net_ae_improvement=(-delta).sum(min_count=1),
        support="SUPPORT_SCREEN_PASS" if n >= 30 and prod >= 10 and periods >= 3 else "LOW_SUPPORT",
        single_period_count_support=bool(n >= 30 and prod >= 10 and periods == 1),
    )


def load_archive(root):
    sources = {}
    for folder, name in [("scoring", "score_manifest.json"), ("prepared", "prepare_manifest.json"),
                         ("effects", "run_manifest.json"), ("diagnostics", "diagnostic_manifest.json"),
                         ("side_panels", "side_panels_manifest.json")]:
        p = root / folder / name
        manifest = json.loads(p.read_text(encoding="utf-8"))
        sources[str(p)] = sha(p)
        # Check consumed artifacts against their owning manifests, not historic summaries.
        wanted = {
            "scoring": ["scores.csv", "metrics.csv", "paired_f28.csv", "joint_summary.csv",
                        "joint_migrations.csv", "operating_indicators_diagnostic.csv", "f28_shape_details.csv"],
            "prepared": ["core.parquet", "issue_grid.csv", "frozen_config.json"],
            "effects": ["training_coverage.csv", "window_predictions.parquet"],
            "diagnostics": ["season_history_coverage.csv"],
            "side_panels": ["current_metadata_mapping.csv"],
        }[folder]
        for file in wanted:
            q = root / folder / file
            digest = sha(q)
            if digest != manifest["files"][file]:
                raise ValueError(f"hash mismatch {q}")
            sources[str(q)] = digest
    for rel in ["validation/validation.json", "formal_fit_ledger.json"]:
        p = root / rel
        sources[str(p)] = sha(p)
    v = json.loads((root/"validation/validation.json").read_text(encoding="utf-8"))
    if v["status"] != "PASS_INDEPENDENT_FULL_RECONCILIATION":
        raise ValueError("original numerical gate not PASS")
    scores = pd.read_csv(root/"scoring/scores.csv")
    s = scores[scores.cohort_id.eq("monthly_core") & scores.model.isin(MODELS.values())].copy()
    unique(s, KEY + ["window", "model"], "score")
    parts = {}
    common = [x for x in s.columns if x not in ["model", "prediction", "prediction_status", "decision", "absolute_error", "relative_error"]]
    for prefix, model in MODELS.items():
        g = s[s.model.eq(model)].drop(columns="model")
        parts[prefix] = g.rename(columns={x: prefix + "_" + x for x in ["prediction", "prediction_status", "decision", "absolute_error", "relative_error"]})
    b = parts["f28"].merge(parts["direct"], on=KEY+["window"], how="outer", validate="one_to_one", suffixes=("", "_check"), indicator=True)
    if not b._merge.eq("both").all():
        raise ValueError("missing model row: must retain no-output status rows")
    for col in set(common) - set(KEY + ["window"]):
        equal_series(b[col], b[col+"_check"], col)
        b = b.drop(columns=col+"_check")
    b = b.drop(columns="_merge")
    core = pd.read_parquet(root/"prepared/core.parquet")
    unique(core, KEY, "core")
    core = core[KEY + ["first_positive", "positive_months", "observed_age_months", "selection_days"]].copy()
    core["first_positive"] = core.first_positive.astype(str)
    b = b.merge(core, on=KEY, how="left", validate="many_to_one", indicator="_core")
    if not b._core.eq("both").all():
        raise ValueError("score outside original core")
    b = b.drop(columns="_core")
    meta_raw = pd.read_csv(root/"side_panels/current_metadata_mapping.csv", keep_default_na=False)
    meta = normalize_metadata(meta_raw)
    b = b.merge(meta, on=["shop", "spu"], how="left", validate="many_to_one")
    for f in ["current_category", "current_season"]:
        b[f] = b[f].fillna("UNKNOWN")
    b["label_basis"] = "CURRENT_NOT_POINT_IN_TIME"
    grid = pd.read_csv(root/"prepared/issue_grid.csv")
    b = b.merge(grid[["issue_date", "calendar_mature"]], on="issue_date", how="left", validate="many_to_one")
    b["calendar_month"] = b.issue_month.str[5:7]
    b["paired"] = b.actual.notna() & b.f28_prediction.notna() & b.direct_prediction.notna()
    oper = pd.read_csv(root/"scoring/operating_indicators_diagnostic.csv")
    unique(oper, KEY, "operating")
    b = b.merge(oper, on=KEY, how="left", validate="many_to_one")
    train = pd.read_csv(root/"effects/training_coverage.csv")
    train_cols = ["issue_date", "status", "train_rows", "train_objects", "train_dates", "train_start",
                  "train_end", "train_span_days", "zero_labels", "max_global_nonoverlap", "input_cutoff"]
    for kind in ["M1", "M2"]:
        t = train[train.kind.eq(kind)][train_cols]
        unique(t, ["issue_date"], kind)
        b = b.merge(t.rename(columns={c: kind.lower()+"_"+c for c in t if c != "issue_date"}),
                    on="issue_date", how="left", validate="many_to_one")
    history = pd.read_csv(root/"diagnostics/season_history_coverage.csv")
    unique(history, KEY, "history")
    b = b.merge(history, on=KEY, how="left", validate="many_to_one")
    unique(b, KEY + ["window"], "base")
    if len(b) != len(s) // 2:
        raise ValueError("join expanded")
    for prefix in MODELS:
        before = s[s.model.eq(MODELS[prefix])].set_index(KEY+["window"]).sort_index()
        after = b.set_index(KEY+["window"]).sort_index()
        equal_series(before.actual, after.actual, "actual conservation")
        equal_series(before.prediction, after[prefix+"_prediction"], "prediction conservation")
    return b, meta, sources


def object_table(b):
    base = b[b.window.eq("d01_60")].copy()
    drop = ["window", "prediction", "prediction_status", "decision", "absolute_error", "relative_error"]
    base = base.drop(columns=[c for c in drop if c in base])
    for suffix, win in [("1", "d01_30"), ("2", "d31_60"), ("60", "d01_60")]:
        cols = KEY + ["actual", "f28_prediction", "direct_prediction", "target_start", "target_end"]
        z = b[b.window.eq(win)][cols].rename(columns={
            "actual": "a"+suffix, "f28_prediction": "f28_p"+suffix,
            "direct_prediction": "direct_p"+suffix,
            "target_start": "target_start_"+suffix, "target_end": "target_end_"+suffix})
        base = base.merge(z, on=KEY, how="left", validate="one_to_one")
    mature = base.calendar_mature.eq(True)
    for prefix in ["a", "f28_p", "direct_p"]:
        complete = base[[prefix+"1", prefix+"2", prefix+"60"]].notna().all(axis=1)
        delta = base.loc[complete, prefix+"1"] + base.loc[complete, prefix+"2"] - base.loc[complete, prefix+"60"]
        if not np.isclose(delta, 0, atol=1e-8, rtol=0).all():
            raise ValueError("window sum mismatch " + prefix)
    for model in MODELS:
        base[model+"_class"] = [classify(dict(a1=r.a1, a2=r.a2, a60=r.a60,
                        p1=getattr(r, model+"_p1"), p2=getattr(r, model+"_p2"), p60=getattr(r, model+"_p60")),
                        bool(r.calendar_mature)) for r in base.itertuples(index=False)]
        base[model+"_ae60"] = (base[model+"_p60"]-base.a60).abs()
        base[model+"_ae_segments"] = ((base[model+"_p1"]-base.a1).abs() + (base[model+"_p2"]-base.a2).abs())
        base[model+"_joint_hit"] = base[model+"_class"].eq("BOTH_SEGMENTS_HIT")
        base[model+"_missing_prediction"] = base[[model+"_p1",model+"_p2",model+"_p60"]].isna().any(axis=1)
    base["any_zero_actual"] = base[["a1","a2","a60"]].eq(0).any(axis=1)
    base["any_missing_truth"] = base[["a1","a2","a60"]].isna().any(axis=1)
    base["paired_object"] = (~base.f28_missing_prediction & ~base.direct_missing_prediction & ~base.any_missing_truth & mature)
    return base


def layers(b, obj):
    metrics, paired, joints = [], [], []
    for level, dims in DIMENSIONS.items():
        for dim, g in groups(b, dims + ["window"]):
            for scope, z in [("FULL_CORE", g), ("MODEL_COMMON", g[g.paired])]:
                for model in MODELS:
                    metrics.append(dict(level=level, scope=scope, model=MODELS[model],
                        full_n=len(g),pair_n=int(g.paired.sum()), **dim, **metric(z, model)))
            ps = pair_stats(g)
            f, d = metric(g[g.paired], "f28"), metric(g[g.paired], "direct")
            paired.append(dict(level=level, **dim, **ps, f28_wape=f["wape"], direct_wape=d["wape"],
                delta_wape=d["wape"]-f["wape"], f28_bias=f["bias"], direct_bias=d["bias"],
                f28_ae=f["absolute_error"], direct_ae=d["absolute_error"],
                f28_over=f["over_units"], direct_over=d["over_units"],
                f28_under=f["under_units"], direct_under=d["under_units"]))
        for dim, g in groups(obj, dims):
            for model in MODELS:
                joints.append(dict(level=level, **dim, model=MODELS[model], full_n=len(g),
                    pair_n=int(g.paired_object.sum()), products=g[["shop","spu"]].drop_duplicates().shape[0],
                    periods=g.issue_date.nunique(), actual_units=g.a60.sum(min_count=1),
                    joint_hit_n=int(g[model+"_joint_hit"].sum()),
                    joint_hit_fixed_share=div(g[model+"_joint_hit"].sum(),len(g)),
                    missing_prediction_n=int(g[model+"_missing_prediction"].sum()),
                    zero_actual_n=int(g.any_zero_actual.sum()),
                    missing_truth_n=int(g.any_missing_truth.sum()),
                    output_n=int((~g[model+"_missing_prediction"]).sum()),
                    known_category_n=int((~g.current_category.isin(UNKNOWN)).sum()),
                    known_season_n=int((~g.current_season.isin(UNKNOWN)).sum())))
    return pd.DataFrame(metrics), pd.DataFrame(paired), pd.DataFrame(joints)


def class_tables(obj):
    result = []
    for model in MODELS:
        full_ae = obj[model+"_ae60"].sum(min_count=1)
        total = obj.a60.sum(min_count=1)
        for cls, g in obj.groupby(model+"_class", dropna=False):
            ae = g[model+"_ae60"].sum(min_count=1)
            result.append(dict(model=MODELS[model], error_class=cls, n=len(g),
                products=g[["shop","spu"]].drop_duplicates().shape[0],
                actual_units=g.a60.sum(min_count=1), actual_share=div(g.a60.sum(min_count=1),total),
                known_ae60_n=int(g[model+"_ae60"].notna().sum()), ae60=ae, ae60_share=div(ae,full_ae),
                ae_segments=g[model+"_ae_segments"].sum(min_count=1)))
    mig = obj.groupby(["f28_class","direct_class"], dropna=False).agg(
        n=("spu","size"), actual_units=("a60",lambda x:x.sum(min_count=1)),
        f28_ae60=("f28_ae60",lambda x:x.sum(min_count=1)),
        direct_ae60=("direct_ae60",lambda x:x.sum(min_count=1)),
        f28_ae_segments=("f28_ae_segments",lambda x:x.sum(min_count=1)),
        direct_ae_segments=("direct_ae_segments",lambda x:x.sum(min_count=1))).reset_index()
    return pd.DataFrame(result), mig


def conditional(b):
    z = b[b.window.eq("d01_60") & b.paired].copy()
    configurations = [
        ("category", "current_category", ["shop","issue_month","maturity"]),
        ("season", "current_season", ["shop","issue_month","maturity","current_category"]),
        ("issue", "issue_month", ["shop","current_category","maturity"]),
        ("calendar", "calendar_month", ["shop","current_category","maturity"]),
    ]
    output, strata_rows = [], []
    for name, dimension, strata in configurations:
        good = z[~z[dimension].isin(UNKNOWN)].copy()
        # Unknown control categories cannot establish a comparable product class.
        if "current_category" in strata:
            good = good[~good.current_category.isin(UNKNOWN)]
        records = []
        for d, g in groups(good, strata+[dimension]):
            f, q = metric(g, "f28"), metric(g, "direct")
            records.append(dict(**d, n=len(g), units=g.actual.sum(), f28_wape=f["wape"],
                direct_wape=q["wape"], f28_bias=f["bias"], direct_bias=q["bias"]))
        cells = pd.DataFrame(records)
        for a, c in itertools.combinations(sorted(good[dimension].unique()),2):
            ga, gb = good[good[dimension].eq(a)], good[good[dimension].eq(c)]
            left = cells[cells[dimension].eq(a)].drop(columns=dimension)
            right = cells[cells[dimension].eq(c)].drop(columns=dimension)
            joined = left.merge(right, on=strata, suffixes=("_a","_b"), validate="one_to_one")
            joined = joined[joined.units_a.gt(0) & joined.units_b.gt(0)].copy()
            if joined.empty:
                output.append(dict(comparison=name, group_a=a, group_b=c, status="NO_COMMON_POSITIVE_STRATA",
                    full_a_n=len(ga), full_b_n=len(gb), kept_a_n=0, kept_b_n=0, strata_n=0,
                    excluded_a_n=len(ga),excluded_b_n=len(gb),full_a_units=ga.actual.sum(),
                    full_b_units=gb.actual.sum(),kept_a_units=0,kept_b_units=0,
                    a_unit_coverage=div(0,ga.actual.sum()),b_unit_coverage=div(0,gb.actual.sum())))
                continue
            joined["weight"] = (joined.units_a+joined.units_b)/(joined.units_a+joined.units_b).sum()
            keys = joined[strata]
            kept_a, kept_b = ga.merge(keys, on=strata, validate="many_to_one"), gb.merge(keys,on=strata,validate="many_to_one")
            row = dict(comparison=name, group_a=a, group_b=c, status="DESCRIPTIVE_COMMON_STRATA",
                full_a_n=len(ga), full_b_n=len(gb), kept_a_n=len(kept_a), kept_b_n=len(kept_b),
                excluded_a_n=len(ga)-len(kept_a), excluded_b_n=len(gb)-len(kept_b),
                full_a_units=ga.actual.sum(), full_b_units=gb.actual.sum(),
                kept_a_units=kept_a.actual.sum(), kept_b_units=kept_b.actual.sum(),
                a_unit_coverage=div(kept_a.actual.sum(),ga.actual.sum()),
                b_unit_coverage=div(kept_b.actual.sum(),gb.actual.sum()),
                a_products=kept_a[["shop","spu"]].drop_duplicates().shape[0],
                b_products=kept_b[["shop","spu"]].drop_duplicates().shape[0],
                a_periods=kept_a.issue_date.nunique(), b_periods=kept_b.issue_date.nunique(),
                strata_n=len(joined), min_cell_n=int(min(joined.n_a.min(),joined.n_b.min())),
                weighting="COMMON_POOLED_ACTUAL_UNITS_ON_OBSERVED_OVERLAP")
            for model in MODELS:
                for measure in ["wape","bias"]:
                    va = (joined.weight*joined[model+"_"+measure+"_a"]).sum()
                    vb = (joined.weight*joined[model+"_"+measure+"_b"]).sum()
                    row[model+"_"+measure+"_a"] = va
                    row[model+"_"+measure+"_b"] = vb
                    row[model+"_"+measure+"_b_minus_a"] = vb-va
            row["support"] = ("SUPPORT_SCREEN_PASS" if all([
                len(kept_a)>=30,len(kept_b)>=30,row["a_products"]>=10,row["b_products"]>=10,
                row["a_periods"]>=3,row["b_periods"]>=3]) else "LOW_SUPPORT")
            output.append(row)
            for r in joined.to_dict("records"):
                strata_rows.append(dict(comparison=name, group_a=a, group_b=c, **r))
    return pd.DataFrame(output), pd.DataFrame(strata_rows)


def overlaps(obj):
    rows = []
    for name, dims in [("category_season",["current_category","current_season"]),
                       ("category_shop",["current_category","shop"]),
                       ("season_shop",["current_season","shop"])]:
        for d,g in groups(obj,dims):
            parent = obj[obj[dims[0]].eq(d[dims[0]])]
            rows.append(dict(overlap=name,**d,n=len(g),products=g[["shop","spu"]].drop_duplicates().shape[0],
                actual_units=g.a60.sum(),parent_n=len(parent),object_share=div(len(g),len(parent)),
                actual_share=div(g.a60.sum(),parent.a60.sum())))
    return pd.DataFrame(rows)


def stability(b, pairs):
    z = b[b.window.eq("d01_60") & b.paired].copy()
    rows, calendar = [], []
    pooled = pairs[pairs.level.eq("shop_category") & pairs.window.eq("d01_60")]
    for r in pooled.itertuples():
        g = z[z.shop.eq(r.shop) & z.current_category.eq(r.current_category)].copy()
        if g.empty:
            continue
        g["direct_ae"] = (g.direct_prediction-g.actual).abs()
        g["f28_ae"] = (g.f28_prediction-g.actual).abs()
        g["gain"] = g.f28_ae-g.direct_ae
        by_prod = g.groupby(["shop","spu"]).agg(direct_ae=("direct_ae","sum"),gain=("gain","sum"))
        top = by_prod.direct_ae.idxmax()
        by_issue = g.groupby("issue_month").agg(direct_ae=("direct_ae","sum"),f28_ae=("f28_ae","sum"),units=("actual","sum"))
        by_issue["delta"] = (by_issue.direct_ae-by_issue.f28_ae)/by_issue.units.replace(0,np.nan)
        gains = g.groupby(["shop","spu"]).gain.sum().clip(lower=0)
        base = dict(shop=r.shop,current_category=r.current_category,support=r.support,
                    pair_n=len(g),products=len(by_prod),periods=len(by_issue),
                    improve_periods=int(by_issue.delta.lt(0).sum()),degrade_periods=int(by_issue.delta.gt(0).sum()),
                    improve_months="|".join(by_issue.index[by_issue.delta.lt(0)]),
                    degrade_months="|".join(by_issue.index[by_issue.delta.gt(0)]),
                    top_error_spu=top[1],top_error_share=div(by_prod.loc[top,"direct_ae"],g.direct_ae.sum()),
                    top_positive_gain_share=div(gains.max(),gains.sum()),
                    november_2025_error_share=div(g.loc[g.issue_month.eq("2025-11"),"direct_ae"].sum(),g.direct_ae.sum()))
        for label,q in [("ORIGINAL",g),("EXCLUDE_2025_11",g[~g.issue_month.eq("2025-11")]),
                        ("EXCLUDE_MAX_ERROR_PRODUCT",g[~(g.shop.eq(top[0])&g.spu.eq(top[1]))])]:
            f,d = metric(q,"f28"),metric(q,"direct")
            rows.append(dict(**base,sensitivity=label,retained_n=len(q),retained_units=q.actual.sum(),
                            f28_wape=f["wape"],direct_wape=d["wape"],delta_wape=d["wape"]-f["wape"]))
        for month in range(1,9):
            a,c=f"2025-{month:02}",f"2026-{month:02}"
            if a in by_issue.index and c in by_issue.index:
                da,db = by_issue.loc[a,"delta"],by_issue.loc[c,"delta"]
                calendar.append(dict(shop=r.shop,current_category=r.current_category,calendar_month=month,
                    delta_2025=da,delta_2026=db,same_direction=bool(np.sign(da)==np.sign(db)),
                    both_improve=bool(da<0 and db<0),both_degrade=bool(da>0 and db>0),
                    scope="DIFFERENT_FROZEN_CORE_AND_TRAINING_CONTEXT_NOT_REPEATED_EXPERIMENT"))
    return pd.DataFrame(rows),pd.DataFrame(calendar)


def survivors(b):
    z=b[b.window.eq("d01_60")&b.paired].copy()
    pieces=[]
    for lag in [1,12]:
        earlier=z[["shop","spu","issue_month","actual","f28_prediction","direct_prediction"]].copy()
        earlier["later_issue_month"]=(pd.PeriodIndex(earlier.issue_month,freq="M")+lag).astype(str)
        earlier=earlier.rename(columns={c:"previous_"+c for c in earlier if c not in ["shop","spu","later_issue_month"]})
        q=z.merge(earlier,left_on=["shop","spu","issue_month"],right_on=["shop","spu","later_issue_month"],validate="one_to_one")
        q["lag_months"]=lag
        q["scope"]="SURVIVOR_SELECTED_DIFFERENT_TRAINING_AND_TRUTH_WINDOWS"
        pieces.append(q)
    return pd.concat(pieces,ignore_index=True)


def operating(obj):
    ratios = ["listing_sessions_sum_ratio","ad_spend_ratio","ad_clicks_ratio","ad_orders_ratio","sku_count_ratio"]
    z=obj.copy()
    z["price_ratio"]=z.target_volume_weighted_price/z.input_volume_weighted_price.replace(0,np.nan)
    z["conversion_ratio"]=z.target_order_session_ratio/z.input_order_session_ratio.replace(0,np.nan)
    ratios += ["price_ratio","conversion_ratio"]
    rows=[]
    for level,dims in [("error_class",["direct_class"]),("shop_category",["shop","current_category"]),
                       ("shop_category_issue",["shop","current_category","issue_month"])]:
        for dim,g in groups(z,dims):
            row=dict(level=level,**dim,n=len(g),actual_units=g.a60.sum(min_count=1),
                     evidence_grade="INDICATOR_ONLY_NOT_CAUSAL",confirmed_operations_log_n=int(
                         (g.historical_effective_operations_log.notna() & g.historical_effective_operations_log.ne("UNKNOWN")).sum()))
            for field in ratios:
                values=g[field].replace([np.inf,-np.inf],np.nan)
                row[field+"_known_n"]=int(values.notna().sum())
                row[field+"_median"]=values.median() if values.notna().any() else np.nan
            rows.append(row)
    cols=KEY+["issue_month","current_category","current_season","maturity","direct_class","f28_class","a1","a2","a60",
              "direct_p1","direct_p2","direct_p60","f28_p1","f28_p2","f28_p60","direct_ae60","f28_ae60",
              "evidence_grade","historical_effective_operations_log","actual_budget","confirmed_stockout_interval"]+ratios
    return pd.DataFrame(rows),z[cols].sort_values("direct_ae60",ascending=False)


def run(root, out):
    started=datetime.now(timezone.utc).isoformat()
    root,out=Path(root).resolve(),Path(out).resolve()
    if out == root or root in out.parents or out.exists():
        raise ValueError("output must be a new independent directory outside original archive")
    b,meta,sources=load_archive(root)
    obj_all=object_table(b)
    mature=b[b.calendar_mature.eq(True)].copy()
    obj=obj_all[obj_all.calendar_mature.eq(True)].copy()
    out.mkdir(parents=True,exist_ok=False)
    csv(out/"base_windows.csv",b);b.to_parquet(out/"base_windows.parquet",index=False)
    csv(out/"base_objects.csv",obj_all);obj_all.to_parquet(out/"base_objects.parquet",index=False)
    csv(out/"metadata_resolved.csv",meta)
    csv(out/"not_matured.csv",b[~b.calendar_mature.eq(True)])
    csv(out/"issue_grid.csv",pd.read_csv(root/"prepared/issue_grid.csv"))
    print("BASE_READY",len(b),len(obj),flush=True)
    metrics,pairs,joints=layers(mature,obj)
    csv(out/"layer_metrics.csv",metrics);csv(out/"layer_pairs.csv",pairs);csv(out/"layer_joint_once.csv",joints)
    classes,migrations=class_tables(obj)
    csv(out/"error_classes.csv",classes);csv(out/"class_migrations.csv",migrations)
    print("LAYERS_READY",len(metrics),flush=True)
    conditional_summary,conditional_cells=conditional(mature)
    csv(out/"conditional_comparisons.csv",conditional_summary);csv(out/"conditional_weights.csv",conditional_cells)
    csv(out/"dimension_overlap.csv",overlaps(obj))
    stable,calendar=stability(mature,pairs)
    csv(out/"combination_stability.csv",stable);csv(out/"calendar_repeatability.csv",calendar)
    survivor=survivors(mature)
    survivor.to_parquet(out/"survivor_details.parquet",index=False)
    csv(out/"survivor_summary.csv",survivor.groupby(["lag_months","previous_issue_month","issue_month"]).agg(
        n=("spu","size"),actual_units=("actual","sum"),previous_actual_units=("previous_actual","sum")).reset_index())
    oper,examples=operating(obj)
    csv(out/"operating_summary.csv",oper);csv(out/"operating_details.csv",examples)
    w60=metrics[metrics.scope.eq("FULL_CORE")&metrics.window.eq("d01_60")]
    rank1=w60[w60.level.eq("shop_category")].merge(
        pairs[pairs.level.eq("shop_category")&pairs.window.eq("d01_60")][["shop","current_category","support","pair_n","pair_products","pair_issues"]],
        on=["shop","current_category"],validate="many_to_one")
    rank1=rank1.sort_values(["model","wape"],ascending=[True,False])
    csv(out/"ranking_1_supported_error_rates.csv",rank1[rank1.support.eq("SUPPORT_SCREEN_PASS")])
    csv(out/"ranking_1_low_support_retained.csv",rank1[~rank1.support.eq("SUPPORT_SCREEN_PASS")])
    rank2=w60[w60.level.eq("shop_category_issue")].copy()
    rank2["error_share"]=rank2.absolute_error/rank2.groupby("model").absolute_error.transform("sum")
    rank2=rank2.sort_values(["model","absolute_error"],ascending=[True,False])
    csv(out/"ranking_2_disjoint_error_units.csv",rank2)
    overall_pair=pairs[pairs.level.eq("overall")]
    summary=dict(mature_core_n=len(obj),unique_products=obj[["shop","spu"]].drop_duplicates().shape[0],
        periods=obj.issue_date.nunique(),pair60_n=int(obj.paired.sum()),
        f28_joint=int(obj.f28_joint_hit.sum()),direct_joint=int(obj.direct_joint_hit.sum()),
        metadata_category_unknown=int(obj.current_category.isin(["UNKNOWN","未映射"]).sum()),
        metadata_season_unknown=int(obj.current_season.isin(["UNKNOWN","未映射"]).sum()),
        category_conflict=int(obj.current_category.eq("CONFLICT").sum()),
        season_conflict=int(obj.current_season.eq("CONFLICT").sum()),
        original_scores_rows=int(len(b)*2),base_rows=len(b),
        paired_metrics=overall_pair.replace({np.nan:None}).to_dict("records"))
    dump(out/"summary.json",summary)
    unchanged=all(sha(p)==digest for p,digest in sources.items())
    if not unchanged:
        raise ValueError("source archive changed during diagnosis")
    dump(out/"field_lineage.json",{
        "base_key":KEY+["window"],"object_key":KEY,
        "prediction_actual_dates_status":"scoring/scores.csv; original values retained",
        "core_age":"prepared/core.parquet, KEY; no reselection",
        "raw_category_season":"side_panels/current_metadata_mapping.csv; shop/spu; values/state arrays retained",
        "operating":"scoring/operating_indicators_diagnostic.csv; KEY; target evidence post-hoc only",
        "training":"effects/training_coverage.csv; issue_date/kind M1 and M2",
        "source_history":"diagnostics/season_history_coverage.csv; source completeness != operating comparability",
        "classification":"mutually exclusive original-value error structure; not a causal label",
        "joint":"layer_joint_once.csv object grain; never sum over windows",
        "scope":"FULL_CORE and MODEL_COMMON retained separately; unknowns are not zero"})
    dump(out/"run_manifest.json",{
        "started_at":started,"completed_at":datetime.now(timezone.utc).isoformat(),
        "source_sha256":sha(__file__),"reference_commit":"34bdc3b703c45326f3c638814ad18595c8124e83",
        "sources":sources,"source_unchanged":unchanged,"fits":0,"database_queries":0,"production_changes":0,
        "classification":"ARCHIVED_POSTHOC_CROSS_DIAGNOSTIC_NOT_CAUSAL_NOT_OOS",
        "files":{p.name:sha(p) for p in out.iterdir() if p.is_file()}})
    print(json.dumps(summary,ensure_ascii=False),flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--root",required=True)
    p.add_argument("--out",required=True)
    run(**vars(p.parse_args()))
