"""Independent arithmetic verification; never imports analysis helpers."""
from __future__ import annotations
import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd

K=["cohort_id","issue_date","shop","spu"]


def digest(p):
    h=hashlib.sha256()
    with Path(p).open("rb") as stream:
        for block in iter(lambda:stream.read(1048576),b""):
            h.update(block)
    return h.hexdigest()


def close(a,b):
    if pd.isna(a) and pd.isna(b):
        return True
    return bool(np.isclose(float(a),float(b),rtol=1e-10,atol=1e-7,equal_nan=True))


def independent_class(a,p,mature):
    if not mature:return "NOT_MATURED"
    if any(pd.isna(x) for x in a):return "MISSING_TRUTH"
    if any(pd.isna(x) for x in p):return "MISSING_PREDICTION"
    if min(a)<=0:return "ZERO_ACTUAL"
    # Deliberately calculate relative errors separately from the analysis helper.
    errors=[(float(y)-float(x))/float(x) for x,y in zip(a,p)]
    if max(abs(errors[0]),abs(errors[1]))<=.2:return "BOTH_SEGMENTS_HIT"
    if abs(errors[2])<=.2:return "CUMULATIVE_HIT_SEGMENTS_MISS"
    if min(errors[0],errors[1])>.2:return "BOTH_OVER_20"
    if max(errors[0],errors[1])<-.2:return "BOTH_UNDER_20"
    return "MIXED_TOTAL_MISS"


def run(root,out,receipt):
    root,out,receipt=Path(root).resolve(),Path(out).resolve(),Path(receipt).resolve()
    if receipt.exists() or root in receipt.parents:
        raise ValueError("new validation receipt outside original archive required")
    checks={}
    manifest=json.loads((out/"run_manifest.json").read_text(encoding="utf-8"))
    for source,expected in manifest["sources"].items():
        assert digest(source)==expected,source
    for name,expected in manifest["files"].items():
        assert digest(out/name)==expected,name
    checks["source_hashes_unchanged"]=len(manifest["sources"])
    checks["output_hashes_verified"]=len(manifest["files"])
    s=pd.read_csv(root/"scoring/scores.csv")
    s=s[s.cohort_id.eq("monthly_core")&s.model.isin(["F28","DIRECT30x2"])]
    assert not s.duplicated(K+["window","model"]).any()
    original=s.set_index(K+["window","model"])
    b=pd.read_csv(out/"base_windows.csv")
    o=pd.read_csv(out/"base_objects.csv")
    assert not b.duplicated(K+["window"]).any()
    assert not o.duplicated(K).any()
    assert len(b)*2==len(s)
    for r in b.itertuples():
        key=tuple(getattr(r,k) for k in K)+ (r.window,)
        for prefix,model in [("f28","F28"),("direct","DIRECT30x2")]:
            src=original.loc[key+(model,)]
            for f in ["actual","target_start","target_end","issue_month","input_cutoff","lifecycle","maturity"]:
                if f=="actual":assert close(src[f],getattr(r,f))
                else:assert src[f]==getattr(r,f)
            assert close(src.prediction,getattr(r,prefix+"_prediction"))
            assert src.prediction_status==getattr(r,prefix+"_prediction_status")
    checks["original_window_records_exact"]=len(s)
    core=pd.read_parquet(root/"prepared/core.parquet")
    assert set(map(tuple,o[K].values))==set(map(tuple,core[K].values))
    grid=pd.read_csv(root/"prepared/issue_grid.csv").set_index("issue_date")
    m=o[o.calendar_mature.eq(True)].copy()
    expected=set(map(tuple,core[core.issue_date.isin(grid.index[grid.calendar_mature])][K].values))
    assert set(map(tuple,m[K].values))==expected
    checks["mature_core_n"]=len(m)
    checks["immature_core_n"]=len(o)-len(m)
    rawmeta=pd.read_csv(root/"side_panels/current_metadata_mapping.csv",keep_default_na=False)
    for (shop,spu),g in o.groupby(["shop","spu"]):
        src=rawmeta[rawmeta.shop.eq(shop)&rawmeta.spu.eq(spu)]
        for f in ["current_category","current_season"]:
            values=set(src[f])-{""}
            known=values-{"UNKNOWN","CONFLICT"}
            conflict=("CONFLICT" in values or len(known)>1 or src[f+"_state"].str.contains("CONFLICT").any())
            expect="CONFLICT" if conflict else next(iter(known)) if known else "UNKNOWN"
            assert g[f].eq(expect).all()
        assert g.label_basis.eq("CURRENT_NOT_POINT_IN_TIME").all()
    checks["metadata_object_joins_verified"]=len(o)
    for r in o.itertuples():
        a=[r.a1,r.a2,r.a60]
        if not any(pd.isna(x) for x in a):assert close(a[0]+a[1],a[2])
        for model in ["f28","direct"]:
            p=[getattr(r,model+"_p"+x) for x in ["1","2","60"]]
            if not any(pd.isna(x) for x in p):assert close(p[0]+p[1],p[2])
            assert independent_class(a,p,bool(r.calendar_mature))==getattr(r,model+"_class")
    checks["object_classifications_verified"]=2*len(o)
    groups=pd.read_csv(out/"layer_metrics.csv")
    paired=pd.read_csv(out/"layer_pairs.csv")
    joint=pd.read_csv(out/"layer_joint_once.csv")
    dims=["current_season","current_category","issue_month","calendar_month","shop","maturity"]
    # CSV type stability: calendar month is a textual category, preserving leading zero.
    b["calendar_month"]=b.issue_month.str[5:7]
    o["calendar_month"]=o.issue_month.str[5:7]
    m["calendar_month"]=m.issue_month.str[5:7]
    for table in [groups,paired,joint]:
        table["calendar_month"]=table.calendar_month.map(lambda v:f"{int(v):02}" if pd.notna(v) else np.nan)
    mature=b[b.calendar_mature.eq(True)]
    partitions={}
    for level,g in groups.groupby("level"):
        columns=[c for c in dims if g[c].notna().any()]
        partitions[level]=columns
    cache={}
    def subset(level,row):
        key=(level,)+tuple(getattr(row,c) for c in partitions[level])
        if key not in cache:
            z=mature
            for c in partitions[level]:z=z[z[c].eq(getattr(row,c))]
            cache[key]=z
        return cache[key]
    for r in groups.itertuples():
        z=subset(r.level,r)
        z=z[z.window.eq(r.window)]
        f=z.f28_prediction;d=z.direct_prediction;a=z.actual
        pair=a.notna()&f.notna()&d.notna()
        assert r.full_n==len(z) and r.pair_n==pair.sum()
        if r.scope=="MODEL_COMMON":z=z[pair]
        p=z.f28_prediction if r.model=="F28" else z.direct_prediction
        a=z.actual
        valid=a.notna()&p.notna()
        errors=p[valid]-a[valid]
        denominator=a[valid].sum(min_count=1)
        hit=a.gt(0)&p.notna()&((p-a).abs()/a).le(.2)
        absolute=errors.abs().sum(min_count=1)
        assert r.n==len(z) and r.scored_n==valid.sum()
        assert r.output_n==p.notna().sum() and r.hit_n==hit.sum()
        assert close(r.absolute_error,absolute)
        assert close(r.actual_units,a.sum(min_count=1))
        assert close(r.scored_actual_units,denominator)
        assert close(r.wape,absolute/denominator if denominator>0 else np.nan)
        assert close(r.bias,errors.sum(min_count=1)/denominator if denominator>0 else np.nan)
    checks["all_layer_metrics_independently_verified"]=len(groups)
    for r in paired.itertuples():
        z=subset(r.level,r);z=z[z.window.eq(r.window)]
        p=z[z.actual.notna()&z.f28_prediction.notna()&z.direct_prediction.notna()]
        ae_f=(p.f28_prediction-p.actual).abs()
        ae_d=(p.direct_prediction-p.actual).abs()
        change=ae_d-ae_f
        assert r.full_n==len(z) and r.pair_n==len(p)
        assert r.improved_n==change.lt(0).sum() and r.degraded_n==change.gt(0).sum()
        assert r.unchanged_n==change.eq(0).sum()
        assert close(r.net_ae_improvement,-change.sum(min_count=1))
        assert close(r.pair_actual_units,p.actual.sum(min_count=1))
        denominator=p.actual.sum(min_count=1)
        assert close(r.f28_wape,ae_f.sum()/denominator if denominator>0 else np.nan)
        assert close(r.direct_wape,ae_d.sum()/denominator if denominator>0 else np.nan)
    checks["paired_cells_verified"]=len(paired)
    for r in joint.itertuples():
        z=m
        for c in partitions[r.level]:z=z[z[c].eq(getattr(r,c))]
        prefix="f28" if r.model=="F28" else "direct"
        assert len(z)==r.full_n
        assert z[prefix+"_class"].eq("BOTH_SEGMENTS_HIT").sum()==r.joint_hit_n
    checks["object_grain_joint_groups_verified"]=len(joint)
    classes=pd.read_csv(out/"error_classes.csv")
    migrations=pd.read_csv(out/"class_migrations.csv")
    for model in ["f28","direct"]:
        c=classes[classes.model.eq("F28" if model=="f28" else "DIRECT30x2")]
        assert c.n.sum()==len(m)
        assert close(c.actual_units.sum(),m.a60.sum())
        assert close(c.ae60.sum(),m[model+"_ae60"].sum())
        for r in c.itertuples():
            z=m[m[model+"_class"].eq(r.error_class)]
            assert r.n==len(z)
            assert close(r.ae60,z[model+"_ae60"].sum(min_count=1))
    expected_migration=Counter(zip(m.f28_class,m.direct_class))
    assert { (r.f28_class,r.direct_class):r.n for r in migrations.itertuples()}==dict(expected_migration)
    assert migrations.n.sum()==len(m)
    checks["class_and_migration_conservation"]="PASS"
    rank=pd.read_csv(out/"ranking_2_disjoint_error_units.csv")
    for model in ["F28","DIRECT30x2"]:
        z=rank[rank.model.eq(model)]
        assert z.n.sum()==len(m)
        assert close(z.error_share.sum(),1)
        assert not z.duplicated(["shop","current_category","issue_month"]).any()
    checks["disjoint_contribution_rank_conservation"]="PASS"
    # Independently reconstruct every conditional group's membership and stratum rate.
    config={
        "category":("current_category",["shop","issue_month","maturity"]),
        "season":("current_season",["shop","issue_month","maturity","current_category"]),
        "issue":("issue_month",["shop","current_category","maturity"]),
        "calendar":("calendar_month",["shop","current_category","maturity"]),
    }
    z=mature[mature.window.eq("d01_60")&mature.paired].copy()
    weights=pd.read_csv(out/"conditional_weights.csv",dtype={"group_a":str,"group_b":str,"calendar_month":str})
    summary=pd.read_csv(out/"conditional_comparisons.csv",dtype={"group_a":str,"group_b":str})
    for kind,(dim,strata) in config.items():
        good=z[~z[dim].isin(["UNKNOWN","CONFLICT","","未映射"])]
        if "current_category" in strata:good=good[~good.current_category.isin(["UNKNOWN","CONFLICT","","未映射"])]
        for r in summary[summary.comparison.eq(kind)].itertuples():
            a=good[good[dim].eq(r.group_a)];c=good[good[dim].eq(r.group_b)]
            ag={tuple(getattr(t,k) for k in strata):[] for t in a.itertuples()}
            bg={tuple(getattr(t,k) for k in strata):[] for t in c.itertuples()}
            for t in a.itertuples():ag[tuple(getattr(t,k) for k in strata)].append(t)
            for t in c.itertuples():bg[tuple(getattr(t,k) for k in strata)].append(t)
            common=[k for k in ag.keys()&bg.keys() if sum(x.actual for x in ag[k])>0 and sum(x.actual for x in bg[k])>0]
            assert r.strata_n==len(common)
            if not common:assert r.status=="NO_COMMON_POSITIVE_STRATA";continue
            total=sum(sum(t.actual for t in ag[k]+bg[k]) for k in common)
            seen=weights[weights.comparison.eq(kind)&weights.group_a.eq(r.group_a)&weights.group_b.eq(r.group_b)]
            assert close(seen.weight.sum(),1) and len(seen)==len(common)
            computed=defaultdict(float)
            for k in common:
                w=sum(t.actual for t in ag[k]+bg[k])/total
                q=seen
                for field,value in zip(strata,k):q=q[q[field].eq(value)]
                assert len(q)==1 and close(q.iloc[0].weight,w)
                for suffix,rows in [("a",ag[k]),("b",bg[k])]:
                    units=sum(t.actual for t in rows)
                    for model in ["f28","direct"]:
                        e=[getattr(t,model+"_prediction")-t.actual for t in rows]
                        computed[f"{model}_wape_{suffix}"]+=w*sum(abs(x) for x in e)/units
                        computed[f"{model}_bias_{suffix}"]+=w*sum(e)/units
            for field,value in computed.items():assert close(getattr(r,field),value)
            assert r.kept_a_n==sum(len(ag[k]) for k in common)
            assert r.kept_b_n==sum(len(bg[k]) for k in common)
    checks["conditional_weight_cells_verified"]=len(weights)
    checks["conditional_pairs_verified"]=len(summary)
    # Reconcile historical published reference values from original summaries, not constants.
    old=pd.read_csv(root/"scoring/joint_summary.csv")
    old=old[old.cohort_id.eq("monthly_core")&old.issue_date.isin(m.issue_date.unique())]
    oldpairs=pd.read_csv(root/"scoring/paired_f28.csv")
    oldpairs=oldpairs[oldpairs.cohort_id.eq("monthly_core")&oldpairs.slice.eq("overall")&
                      oldpairs.model.eq("DIRECT30x2")]
    for r in oldpairs.itertuples():
        z=paired[paired.level.eq("overall")&paired.window.eq(r.window)].iloc[0]
        assert z.pair_n==r.paired_n
        assert close(z.direct_wape,r.candidate_wape) and close(z.f28_wape,r.f28_wape)
    for prefix,model in [("f28","F28"),("direct","DIRECT30x2")]:
        ref=old[old.model.eq(model)&old.decision.eq("HIT")].records.sum()
        assert m[prefix+"_class"].eq("BOTH_SEGMENTS_HIT").sum()==ref
    checks["legacy_joint_and_paired_reference_reconciled"]=True
    summary_new=json.loads((out/"summary.json").read_text(encoding="utf-8"))
    assert summary_new["direct_joint"]==m.direct_class.eq("BOTH_SEGMENTS_HIT").sum()
    assert summary_new["f28_joint"]==m.f28_class.eq("BOTH_SEGMENTS_HIT").sum()
    assert all(digest(p)==d for p,d in manifest["sources"].items())
    result=dict(status="PASS_INDEPENDENT_CALCULATION_PATH",reviewer_scope="Separate implementation by same executing agent; no second-person claim",
        completed_at=datetime.now(timezone.utc).isoformat(),checks=checks,source_sha256=digest(__file__),
        analysis_manifest_sha256=digest(out/"run_manifest.json"),
        fits=0,database_queries=0,production_changes=0,original_sources_unchanged=True)
    receipt.parent.mkdir(exist_ok=True,parents=True)
    receipt.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False))


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--root",required=True);p.add_argument("--out",required=True);p.add_argument("--receipt",required=True)
    run(**vars(p.parse_args()))
