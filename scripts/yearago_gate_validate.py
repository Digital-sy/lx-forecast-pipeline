"""Independent scalar accounting and archived daily-truth validation; no models."""
from __future__ import annotations
import argparse
import math
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.yearago_gate_route import KEY, RULE, sha, read_json, dump, now, verify, choose
except ModuleNotFoundError:
    from yearago_gate_route import KEY, RULE, sha, read_json, dump, now, verify, choose

BASE, YEAR = "DIRECT30x2", "DIRECT_YEARAGO"
W = {"d01_30":(1,30), "d31_60":(31,60), "d01_60":(1,60)}


def eq(a,b):
    return (pd.isna(a) and pd.isna(b)) or a == b


def close(a,b):
    return eq(a,b) or (math.isfinite(a) and math.isfinite(b) and math.isclose(a,b,rel_tol=2e-13,abs_tol=1e-8))


def scalar_metrics(records):
    valid=[r for r in records if math.isfinite(r.actual) and math.isfinite(r.prediction)]
    errors=[r.prediction-r.actual for r in valid]
    den=math.fsum(r.actual for r in valid)
    hits=[r for r in valid if r.actual>0 and abs((r.prediction-r.actual)/r.actual)<=.2]
    ae=math.fsum(abs(x) for x in errors);over=math.fsum(max(x,0) for x in errors)
    return dict(fixed_n=len(records),output_n=sum(math.isfinite(r.prediction) for r in records),
                truth_n=sum(math.isfinite(r.actual) for r in records),scored_n=len(valid),
                zero_truth_n=sum(r.actual==0 for r in records),unknown_truth_n=sum(pd.isna(r.actual) for r in records),
                missing_output_n=sum(pd.isna(r.prediction) for r in records),actual_units=den,ae=ae,
                wape=ae/den if den else math.nan,bias=math.fsum(errors)/den if den else math.nan,
                over_units=over,under_units=math.fsum(max(-x,0) for x in errors),over_rate=over/den if den else math.nan,
                hit_n=len(hits),hit_actual=math.fsum(r.actual for r in hits),
                zero_truth_ae=math.fsum(abs(r.prediction) for r in valid if r.actual==0))


def validate(output):
    out=Path(output).resolve();dest=out/'validation'
    if dest.exists():raise ValueError('VALIDATION_EXISTS')
    rm=read_json(out/'routing_manifest.json');sm=read_json(out/'scoring/manifest.json')
    verify(rm['sources']);verify({str(out/'scoring'/p):h for p,h in sm['files'].items()})
    assert sha(out/'routing.parquet')==rm['routing_sha256']==sm['routing_sha256']
    assert sha(out/'routing_access_log.json')==rm['access_log_sha256']
    log=read_json(out/'routing_access_log.json')
    assert all(not set(x['columns']) & {'actual','error','hit','truth_status','absolute_error','joint_hit'} for x in log)
    assert all('/scores.' not in x['path'].replace('\\','/') and '/truth.' not in x['path'].replace('\\','/') for x in log)
    score_summary=read_json(out/'scoring/summary.json')
    assert pd.Timestamp(rm['sealed_at'])<=pd.Timestamp(score_summary['started_at'])
    route=pd.read_parquet(out/'routing.parquet')
    assert not route.duplicated(KEY).any()
    poison_checks=0
    for r in route.to_dict('records'):
        # Re-derive all conditions from primitive evidence, not stored combined flags.
        direct=all(math.isfinite(r['direct_'+s]) for s in ['M1','M2'])
        input_ok=all(r['ly_input_unknown_days_'+s]==0 and r['ly_input_unsafe_days_'+s]==0 for s in ['M1','M2'])
        targets_ok=all(r['ly_target_unknown_days_'+s]==0 and r['ly_target_unsafe_days_'+s]==0 for s in ['M1','M2'])
        positive=pd.notna(r['first_positive_evidence_date']) and r['first_positive_evidence_date']<=r['ly_input_start_M1']
        support=all(r['support_dates_'+s]>=30 and r['support_products_'+s]>=10 and r['numerical_features_vary_'+s] for s in ['M1','M2'])
        annual=all(r['origin_'+s]=='ANNUAL_MODEL' and math.isfinite(r['annual_'+s]) for s in ['M1','M2'])
        decision=bool(direct and input_ok and targets_ok and positive and support and annual)
        assert decision==r['gate_decision']
        expected_fail=[k for k,v in [('direct_valid',direct),('input_valid',input_ok),('targets_valid',targets_ok),('prior_positive',positive),('both_supported',support),('both_annual',annual)] if not v]
        assert r['failed_conditions']=='|'.join(expected_fail)
        for segment in ['M1','M2']:
            val=r[('annual_' if decision else 'direct_')+segment]
            assert eq(val,r['selected_'+segment])
            assert r['ly_target_end_'+segment]<=r['input_cutoff']
        assert eq(r['selected_60'],r['selected_M1']+r['selected_M2'])
        # Delete, reorder and poison all target/outcome columns; pure route unchanged.
        clean={k:r[k] for k in ['direct_valid','input_valid','targets_valid','prior_positive','both_supported','both_annual','annual_M1','annual_M2','direct_M1','direct_M2']}
        expected=choose(clean)
        for nuisance in [{},{'actual':0.,'hit':False},{'actual':1e80,'error':-1e90,'joint_hit':True}]:
            got=choose(dict(reversed(list({**clean,**nuisance}.items()))))
            assert got['gate_decision']==expected['gate_decision']
            assert all(eq(got[k],expected[k]) for k in ['selected_M1','selected_M2'])
            poison_checks+=1
    scores=pd.read_parquet(out/'scoring/scores.parquet')
    unique_cols=KEY+['window','model'];assert not scores.duplicated(unique_cols).any()
    mature=scores[scores.issue_date.isin(route.issue_date.unique())]
    rindex=route.set_index(KEY)
    joint={}; missing={}; actual60={}
    for (issue,shop,spu,model),g in mature.groupby(KEY+['model']):
        z=g.set_index('window');key=(issue,shop,spu)
        joint[(key,model)]=all(math.isfinite(z.loc[w,'prediction']) and z.loc[w,'actual']>0 and abs((z.loc[w,'prediction']-z.loc[w,'actual'])/z.loc[w,'actual'])<=.2 for w in W)
        missing[(key,model)]=any(not math.isfinite(z.loc[w,'prediction']) for w in W)
        actual60[key]=z.loc['d01_60','actual']
        if model in [BASE,YEAR,RULE]:
            prefix={BASE:'direct',YEAR:'comparison',RULE:'selected'}[model]
            row=rindex.loc[key]
            for w,s in [('d01_30','M1'),('d31_60','M2')]:assert eq(z.loc[w,'prediction'],row[prefix+'_'+s])
            assert eq(z.loc['d01_60','prediction'],z.loc['d01_30','prediction']+z.loc['d31_60','prediction'])
    keys=list(actual60)
    for key in keys: assert missing[(key,BASE)]==missing[(key,RULE)]
    migrations={}
    for model in [YEAR,RULE,'F28']:
        gains={k for k in keys if not joint[(k,BASE)] and joint[(k,model)]}
        lost={k for k in keys if joint[(k,BASE)] and not joint[(k,model)]}
        migrations[model]={'gains':gains,'lost':lost}
    assert migrations[RULE]['gains']<=migrations[YEAR]['gains']
    assert migrations[RULE]['lost']<=migrations[YEAR]['lost']
    archived=pd.read_parquet(Path(rm['yearago'])/'scoring_reviewed/hit_changes.parquet')
    for state,subset in [('GAINED',migrations[YEAR]['gains']),('LOST',migrations[YEAR]['lost']),('RETAINED',{k for k in keys if joint[(k,BASE)] and joint[(k,YEAR)]})]:
        old=archived[(archived.scope=='ORIGINAL_DIRECT_OUTPUT') & (archived.kind=='hit_migration') & (archived.state==state)].iloc[0]
        assert len(subset)==old.n and math.fsum(actual60[k] for k in subset)==old.actual
    # Independent raw daily truth sums. No prefix-sum or archived scoring helper.
    daily=pd.read_parquet(Path(rm['archive'])/'inputs/daily.parquet',columns=['shop','spu','ds','y','mapped','sales_state'])
    assert not daily.duplicated(['shop','spu','ds']).any()
    groups={k:g.set_index('ds') for k,g in daily.groupby(['shop','spu'])}
    truth_count=0
    for r in mature[mature.model.eq(BASE)].itertuples():
        a,b=W[r.window]; dates=pd.date_range(pd.Timestamp(r.issue_date)+pd.Timedelta(days=a),periods=b-a+1)
        source=groups.get((r.shop,r.spu))
        if source is None:
            actual=math.nan
        else:
            z=source.reindex(dates)
            ok=z.mapped.eq(True) & z.sales_state.isin(['OBSERVED','EXPLICIT_ZERO']) & np.isfinite(z.y) & z.y.ge(0)
            actual=math.fsum(z.y.tolist()) if ok.all() else math.nan
        assert eq(actual,r.actual),(r.issue_date,r.shop,r.spu,r.window)
        truth_count+=1
    # Scalar independent aggregation checks every saved metric slice.
    metric_table=pd.read_parquet(out/'scoring/metrics.parquet');metric_checks=0
    dims={'overall':[],'issue':['issue_date'],'shop':['shop'],'year':['year'],'maturity':['maturity'],
          'category':['current_category'],'season':['current_season'],'shop_category':['shop','current_category'],
          'decline_group':['decline_group'],'reason':['main_reason']}
    for scope in metric_table.scope.unique():
        f=mature if scope=='FULL_CORE' else mature[mature.paired] if scope=='PAIRED' else mature[mature.paired & mature.gate_decision.eq(scope=='GATE_TRUE')]
        for label,columns in dims.items():
            expected=metric_table[metric_table.scope.eq(scope)&metric_table.slice.eq(label)]
            grouping={k:g for k,g in f.groupby(['model','window']+columns,dropna=False)}
            for r in expected.to_dict('records'):
                k=tuple(r[c] for c in ['model','window']+columns)
                g=grouping[k]; calculated=scalar_metrics(list(g.itertuples()))
                for field,val in calculated.items():assert close(val,r[field]),(scope,label,k,field,val,r[field])
                metric_checks+=1
    jt=pd.read_parquet(out/'scoring/joint_summary.parquet');obj=pd.read_parquet(out/'scoring/objects.parquet')
    obj=obj[obj.issue_date.isin(route.issue_date.unique())];joint_checks=0
    for scope in jt.scope.unique():
        f=obj if scope=='FULL_CORE' else obj[obj.paired] if scope=='PAIRED' else obj[obj.paired & obj.gate_decision.eq(scope=='GATE_TRUE')]
        for label,columns in dims.items():
            grouping={k:g for k,g in f.groupby(['model']+columns,dropna=False)}
            for r in jt[jt.scope.eq(scope)&jt.slice.eq(label)].to_dict('records'):
                g=grouping[tuple(r[c] for c in ['model']+columns)]
                hits=[x for x in g.itertuples() if joint[((x.issue_date,x.shop,x.spu),x.model)]]
                weight=math.fsum(actual60[(x.issue_date,x.shop,x.spu)] for x in hits)
                den=math.fsum(x.actual_d01_60 for x in g.itertuples() if math.isfinite(x.actual_d01_60))
                assert len(hits)==r['joint_hit_n'] and close(weight,r['joint_hit_actual'])
                assert close(den,r['known_actual']) and close(weight/den if den else math.nan,r['joint_hit_actual_share'])
                joint_checks+=1
    exits=pd.read_parquet(out/'scoring/original_exits.parquet')
    gains=pd.read_parquet(out/'scoring/original_gains.parquet')
    for kind,f in [('LOST',exits),('GAINED',gains)]:
        for r in f.to_dict('records'):
            err=max(abs((r['yearago_prediction_'+w]-r['actual_'+w])/r['actual_'+w]) for w in W)
            excess=err-.20
            if kind=='LOST':label='20-22%' if 0<excess<=.02 else '22-25%' if .02<excess<=.05 else '25-30%' if .05<excess<=.10 else '>30%'
            else:
                m=.20-err;label='0-2pp_inside' if 0<=m<=.02 else '2-5pp_inside' if .02<m<=.05 else '5-10pp_inside' if .05<m<=.10 else '>10pp_inside'
            assert label==r['diagnostic_bin']
    verify(rm['sources'])
    dest.mkdir(exist_ok=False)
    result=dict(status='PASS',at=now(),routing_before_truth=True,routing_primitive_checks=len(route),truth_poison_checks=poison_checks,
                raw_daily_truth_checks=truth_count,independent_metric_rows=metric_checks,independent_joint_rows=joint_checks,
                gate_gains_subset=True,gate_losses_subset=True,old_sources_unchanged=len(rm['sources']),
                missing_output_n=sum(missing[(k,BASE)] for k in keys),
                original_migration_reconciled=True,new_fits=0,new_inference=0,database_queries=0,production_changes=0,
                script_sha256=sha(__file__),routing_sha256=rm['routing_sha256'],scoring_manifest_sha256=sha(out/'scoring/manifest.json'))
    dump(dest/'validation.json',result);print(result)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);validate(**vars(p.parse_args()))
