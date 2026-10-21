"""Score a sealed routing artifact; no training or inference dependencies."""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.yearago_gate_route import KEY, RULE, dump, now, read_json, save, sha, unique, verify
except ModuleNotFoundError:
    from yearago_gate_route import KEY, RULE, dump, now, read_json, save, sha, unique, verify

BASE, YEAR = "DIRECT30x2", "DIRECT_YEARAGO"
WINDOWS = ["d01_30", "d31_60", "d01_60"]


def metrics(g):
    valid = np.isfinite(g.actual) & np.isfinite(g.prediction)
    v = g[valid]
    e = v.prediction - v.actual
    den = v.actual.sum()
    known = g.actual.sum(min_count=1)
    hit = valid & g.actual.gt(0) & ((g.prediction - g.actual).abs() / g.actual).le(.2)
    return dict(fixed_n=len(g), output_n=int(np.isfinite(g.prediction).sum()),
                truth_n=int(np.isfinite(g.actual).sum()), scored_n=len(v),
                zero_truth_n=int(g.actual.eq(0).sum()), unknown_truth_n=int(g.actual.isna().sum()),
                missing_output_n=int(g.prediction.isna().sum()), actual_units=float(den),
                ae=float(e.abs().sum()), wape=float(e.abs().sum()/den) if den else np.nan,
                bias=float(e.sum()/den) if den else np.nan,
                over_units=float(e.clip(lower=0).sum()), under_units=float((-e).clip(lower=0).sum()),
                over_rate=float(e.clip(lower=0).sum()/den) if den else np.nan,
                hit_n=int(hit.sum()), hit_fixed_share=float(hit.mean()),
                hit_actual=float(g.loc[hit, "actual"].sum()),
                hit_actual_share=float(g.loc[hit, "actual"].sum()/known) if known else np.nan,
                zero_truth_ae=float(e[v.actual.eq(0)].abs().sum()),
                products=len(g[["shop", "spu"]].drop_duplicates()), periods=g.issue_date.nunique())


def objects(scores):
    p = scores.pivot(index=KEY + ["model"], columns="window", values=["actual", "prediction"])
    z = pd.DataFrame(index=p.index)
    for w in WINDOWS:
        z["actual_"+w] = p.actual[w]
        z["prediction_"+w] = p.prediction[w]
        z["hit_"+w] = p.actual[w].gt(0) & ((p.prediction[w]-p.actual[w]).abs()/p.actual[w]).le(.2)
    z["joint_hit"] = z[["hit_"+w for w in WINDOWS]].all(axis=1)
    z["output_complete"] = np.isfinite(z[["prediction_"+w for w in WINDOWS]]).all(axis=1)
    z["truth_complete"] = np.isfinite(z[["actual_"+w for w in WINDOWS]]).all(axis=1)
    return z.reset_index()


def joints(g):
    den = g.actual_d01_60.sum(min_count=1)
    actual = g.loc[g.joint_hit, "actual_d01_60"].sum()
    return dict(fixed_n=len(g), output_complete_n=int(g.output_complete.sum()),
                truth_complete_n=int(g.truth_complete.sum()), joint_hit_n=int(g.joint_hit.sum()),
                joint_hit_fixed_share=float(g.joint_hit.mean()), joint_hit_actual=float(actual),
                known_actual=float(den), joint_hit_actual_share=float(actual/den) if den else np.nan,
                products=len(g[["shop", "spu"]].drop_duplicates()), periods=g.issue_date.nunique())


def migration(a, b):
    return np.select([a & b, ~a & b, a & ~b], ["RETAINED", "GAINED", "LOST"], default="NEITHER")


def exit_bin(excess):
    if not np.isfinite(excess) or excess <= 0:
        return "UNCLASSIFIED"
    return "20-22%" if excess <= .02 else "22-25%" if excess <= .05 else "25-30%" if excess <= .10 else ">30%"


def entry_bin(margin):
    if not np.isfinite(margin) or margin < 0:
        return "UNCLASSIFIED"
    return "0-2pp_inside" if margin <= .02 else "2-5pp_inside" if margin <= .05 else "5-10pp_inside" if margin <= .10 else ">10pp_inside"


def score(output):
    out = Path(output).resolve()
    dest = out / "scoring"
    if dest.exists():
        raise ValueError("SCORING_EXISTS")
    rm = read_json(out / "routing_manifest.json")
    verify(rm["sources"])
    if sha(out / "routing.parquet") != rm["routing_sha256"]:
        raise ValueError("ROUTING_CHANGED")
    started = now()
    route = pd.read_parquet(out / "routing.parquet")
    y = Path(rm["yearago"])
    # First target truth read occurs ONLY after the routing seal exists and checks out.
    cols = KEY + ["window", "model", "prediction", "prediction_status", "actual", "truth_status",
                  "target_start", "target_end", "unknown_days", "maturity", "lifecycle",
                  "current_category", "current_season", "decline_group"]
    old = pd.read_parquet(y / "scoring_reviewed/scores.parquet", columns=cols,
                          filters=[("model", "in", [BASE, YEAR, "F28"])])
    unique(old, KEY + ["model", "window"])
    # Read no CONTEXT90 predictions: only exact original group membership.
    context = Path(read_json(y / "configuration.json")["context_archive"])
    obs = pd.read_parquet(context / "observation_groups.parquet", columns=KEY + ["decline_group"])
    chk = old[old.model.eq(BASE) & old.window.eq("d01_60")].merge(obs, on=KEY, suffixes=("", "_original"), validate="one_to_one")
    if not chk.decline_group.eq(chk.decline_group_original).all() or len(chk) != len(route):
        raise ValueError("DECLINE_GROUP_CHANGED")
    selected = route.melt(id_vars=KEY, value_vars=["selected_M1", "selected_M2", "selected_60"],
                          var_name="window", value_name="selected")
    selected["window"] = selected.window.map(dict(selected_M1="d01_30", selected_M2="d31_60", selected_60="d01_60"))
    candidate = old[old.model.eq(BASE)].merge(selected, on=KEY+["window"], how="left", validate="one_to_one")
    candidate["prediction"] = candidate.pop("selected")
    candidate["model"] = RULE
    candidate["prediction_status"] = np.where(candidate.prediction.notna(), "OK", candidate.prediction_status)
    scores = pd.concat([old, candidate], ignore_index=True)
    unique(scores, KEY+["model", "window"])
    metadata = old[old.model.eq(BASE) & old.window.eq("d01_60")][KEY + ["maturity", "lifecycle", "current_category", "current_season", "decline_group"]]
    obj = objects(scores).merge(metadata, on=KEY, validate="many_to_one")
    pair = obj[obj.model.eq(BASE) & obj.output_complete][KEY].assign(paired=True)
    for f in [scores, obj]:
        f["paired"] = pd.MultiIndex.from_frame(f[KEY]).isin(pd.MultiIndex.from_frame(pair[KEY]))
        f["year"] = f.issue_date.str[:4]
    flags = route[KEY+["gate_decision", "main_reason"]]
    scores = scores.merge(flags, on=KEY, how="left", validate="many_to_one")
    obj = obj.merge(flags, on=KEY, how="left", validate="many_to_one")
    mature = scores[scores.issue_date.isin(route.issue_date.unique())]
    mo = obj[obj.issue_date.isin(route.issue_date.unique())]
    met, jnt = [], []
    dimensions = {"overall": [], "issue": ["issue_date"], "shop": ["shop"], "year": ["year"],
                  "maturity": ["maturity"], "category": ["current_category"], "season": ["current_season"],
                  "shop_category": ["shop", "current_category"], "decline_group": ["decline_group"],
                  "reason": ["main_reason"]}
    for scope in ["FULL_CORE", "PAIRED", "GATE_TRUE", "GATE_FALSE"]:
        def subset(f):
            return f if scope == "FULL_CORE" else f[f.paired] if scope == "PAIRED" else f[f.paired & f.gate_decision.eq(scope == "GATE_TRUE")]
        for label, dims in dimensions.items():
            for vals, g in subset(mature).groupby(["model", "window"]+dims, dropna=False):
                met.append(dict(scope=scope, slice=label, **dict(zip(["model", "window"]+dims, vals)), **metrics(g)))
            for vals, g in subset(mo).groupby(["model"]+dims, dropna=False):
                jnt.append(dict(scope=scope, slice=label, **dict(zip(["model"]+dims, vals)), **joints(g)))
    met, jnt = pd.DataFrame(met), pd.DataFrame(jnt)
    detail = route.copy()
    for model, name in [(BASE, "direct"), (YEAR, "yearago"), (RULE, "gate"), ("F28", "f28")]:
        z = mo[mo.model.eq(model)]
        keep = KEY+["prediction_"+w for w in WINDOWS]+["joint_hit", "output_complete"]
        if name == "direct":
            keep += ["actual_"+w for w in WINDOWS]+["paired", "maturity", "lifecycle", "current_category", "current_season", "decline_group"]
        z = z[keep].rename(columns={c: name+"_"+c for c in keep if c.startswith("prediction_") or c in ["joint_hit", "output_complete"]})
        detail = detail.merge(z, on=KEY, validate="one_to_one")
    for name in ["yearago", "gate", "f28"]:
        detail[name+"_migration"] = migration(detail.direct_joint_hit, detail[name+"_joint_hit"])
    for name in ["direct", "yearago", "gate", "f28"]:
        for w in WINDOWS:
            err = detail[name+"_prediction_"+w]-detail["actual_"+w]
            detail[name+"_error_"+w] = err
            detail[name+"_relative_error_"+w] = err / detail["actual_"+w].where(detail["actual_"+w].gt(0))
            detail[name+"_absolute_relative_error_"+w] = detail[name+"_relative_error_"+w].abs()
            if name != "direct":
                be = detail["direct_error_"+w]
                detail[name+"_extra_ae_"+w] = err.abs()-be.abs()
                detail[name+"_extra_over_"+w] = err.clip(lower=0)-be.clip(lower=0)
                detail[name+"_extra_under_"+w] = (-err).clip(lower=0)-(-be).clip(lower=0)
    valid3 = detail[["actual_"+w for w in WINDOWS]].gt(0).all(axis=1)
    detail["yearago_e_max"] = detail[["yearago_absolute_relative_error_"+w for w in WINDOWS]].max(axis=1, skipna=False).where(valid3)
    detail["excess"] = detail.yearago_e_max-.20
    detail["entry_margin"] = .20-detail.yearago_e_max
    detail["failed_windows"] = detail.apply(lambda r: "|".join(w for w in WINDOWS if r["yearago_absolute_relative_error_"+w] > .20), axis=1)
    detail["cumulative_failed"] = detail.yearago_absolute_relative_error_d01_60.gt(.20)
    lost = detail[detail.yearago_migration.eq("LOST")].copy()
    gained = detail[detail.yearago_migration.eq("GAINED")].copy()
    lost["diagnostic_bin"] = lost.excess.map(exit_bin)
    gained["diagnostic_bin"] = gained.entry_margin.map(entry_bin)
    bins = []
    for kind, f in [("LOST", lost), ("GAINED", gained)]:
        for label, g in f.groupby("diagnostic_bin"):
            bins.append(dict(kind=kind, diagnostic_bin=label, n=len(g), products=len(g[["shop", "spu"]].drop_duplicates()),
                             actual60=float(g.actual_d01_60.sum()), extra_ae60=float(g.yearago_extra_ae_d01_60.sum()),
                             extra_over60=float(g.yearago_extra_over_d01_60.sum()), extra_under60=float(g.yearago_extra_under_d01_60.sum()),
                             gate_joint_n=int(g.gate_joint_hit.sum()), gate_joint_actual60=float(g.loc[g.gate_joint_hit, "actual_d01_60"].sum())))
    changes = []
    for name in ["yearago", "gate", "f28"]:
        for scope, q in [("FULL_CORE", detail), ("PAIRED", detail[detail.paired])]:
            for state, g in q.groupby(name+"_migration"):
                changes.append(dict(model=name, scope=scope, state=state, n=len(g), actual60=float(g.actual_d01_60.sum()), products=len(g[["shop", "spu"]].drop_duplicates())))
    route_summary = []
    for decision, g in detail.groupby("gate_decision"):
        pg = g[g.paired]
        route_summary.append(dict(gate_decision=decision, full_n=len(g), paired_n=len(pg),
                                  products=len(pg[["shop", "spu"]].drop_duplicates()), periods=pg.issue_date.nunique(),
                                  paired_actual60=float(pg.actual_d01_60.sum()), paired_actual_share=float(pg.actual_d01_60.sum()/detail.loc[detail.paired, "actual_d01_60"].sum()),
                                  changed_n=int((pg.gate_prediction_d01_30.ne(pg.direct_prediction_d01_30) | pg.gate_prediction_d31_60.ne(pg.direct_prediction_d31_60)).sum()),
                                  extra_ae60=float(pg.gate_extra_ae_d01_60.sum())))
    paired = detail[detail.paired]
    largest = paired.groupby(["shop", "spu"]).actual_d01_60.sum().idxmax()
    sens = []
    subsets = [("EXCLUDE_LARGEST_PRODUCT", paired[~(paired.shop.eq(largest[0]) & paired.spu.eq(largest[1]))])]
    subsets += [("EXCLUDE_"+issue, paired[paired.issue_date.ne(issue)]) for issue in sorted(paired.issue_date.unique())]
    for label, g in subsets:
        sens.append(dict(exclusion=label, n=len(g), actual60=float(g.actual_d01_60.sum()),
                         ae_reduction60=float(-g.gate_extra_ae_d01_60.sum()),
                         wape_reduction60=float(-g.gate_extra_ae_d01_60.sum()/g.actual_d01_60.sum()),
                         joint_net=int(g.gate_joint_hit.sum()-g.direct_joint_hit.sum()),
                         joint_actual_net=float(g.loc[g.gate_joint_hit, "actual_d01_60"].sum()-g.loc[g.direct_joint_hit, "actual_d01_60"].sum())))
    p60 = met[met.scope.eq("PAIRED") & met.slice.eq("overall") & met.window.eq("d01_60")].set_index("model")
    pj = jnt[jnt.scope.eq("PAIRED") & jnt.slice.eq("overall")].set_index("model")
    dg = met[met.scope.eq("PAIRED") & met.slice.eq("decline_group") & met.window.eq("d01_60") & met.decline_group.eq("PRE_ISSUE_DECLINING")].set_index("model")
    gates = dict(paired_w60_wape_lower=bool(p60.loc[RULE,"wape"] < p60.loc[BASE,"wape"]),
                 joint_hit_count_increased=bool(pj.loc[RULE,"joint_hit_n"] > pj.loc[BASE,"joint_hit_n"]),
                 joint_hit_sales_share_not_lower=bool(pj.loc[RULE,"joint_hit_actual_share"] >= pj.loc[BASE,"joint_hit_actual_share"]),
                 declining_w60_over_rate_not_increased=bool(dg.loc[RULE,"over_rate"] <= dg.loc[BASE,"over_rate"]) if len(dg) and dg.actual_units.gt(0).all() else None,
                 output_and_core_coverage_not_reduced=bool(detail.gate_output_complete.eq(detail.direct_output_complete).all()))
    summary = dict(started_at=started, routing_sealed_at=rm["sealed_at"], gates=gates,
                   numerical_screen="PASS" if all(v is True for v in gates.values()) else "FAIL" if any(v is False for v in gates.values()) else "PARTIAL",
                   evidence="PARTIAL: post-hoc reconstructed development replay; operating comparability unproven",
                   full_core_n=len(detail), paired_n=len(paired), missing_output_n=int((~detail.direct_output_complete).sum()),
                   immature_n=len(obj[obj.model.eq(BASE) & ~obj.issue_date.isin(route.issue_date.unique())]),
                   routing=route_summary, paired_windows=met[met.scope.eq("PAIRED") & met.slice.eq("overall")].to_dict("records"),
                   paired_joint=pj.reset_index().to_dict("records"), declining60=dg.reset_index().to_dict("records"),
                   original_migration=pd.DataFrame(changes).query("model == 'yearago' and scope == 'PAIRED'").to_dict("records"),
                   new_fits=0, new_inference=0, database_queries=0, production_changes=0)
    top_sales = lost.sort_values(["actual_d01_60"]+KEY, ascending=[False, True, True, True]).head(20)
    top_ae = lost.sort_values(["yearago_extra_ae_d01_60"]+KEY, ascending=[False, True, True, True]).head(20)
    dest.mkdir(exist_ok=False)
    for n, f in [("scores",scores), ("objects",obj), ("metrics",met), ("joint_summary",jnt), ("details",detail),
                 ("original_exits",lost), ("original_gains",gained), ("boundary_bins",pd.DataFrame(bins)),
                 ("top20_exits_actual",top_sales), ("top20_exits_extra_ae",top_ae),
                 ("hit_changes",pd.DataFrame(changes)), ("routing_summary",pd.DataFrame(route_summary)),
                 ("sensitivity",pd.DataFrame(sens)), ("not_matured",scores[~scores.issue_date.isin(route.issue_date.unique())])]:
        save(dest,n,f)
    dump(dest/"summary.json", summary)
    verify(rm["sources"])
    dump(dest/"manifest.json", dict(at=now(), routing_sha256=rm["routing_sha256"], script_sha256=sha(__file__),
                                    files={p.name:sha(p) for p in dest.iterdir() if p.is_file()}))
    print({"status":"SCORED", "gates":gates, "exits":len(lost), "gains":len(gained)})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--output",required=True)
    score(**vars(p.parse_args()))
