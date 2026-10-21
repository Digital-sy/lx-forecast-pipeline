"""Independent scalar validation and convexity checks, using verified truth chain."""
from __future__ import annotations
import argparse
import math
from pathlib import Path
import subprocess
import sys
import numpy as np
import pandas as pd
try:
    from scripts.yearago_gate_route import KEY,sha,now,read_json,dump,verify,save
    from scripts.yearago_gate_validate import scalar_metrics,eq,close
    from scripts.direct_yearago_blend50 import BASE,GATE,CAND,YEAR,WINDOWS,DIMS,blend_one
except ModuleNotFoundError:
    from yearago_gate_route import KEY,sha,now,read_json,dump,verify,save
    from yearago_gate_validate import scalar_metrics,eq,close
    from direct_yearago_blend50 import BASE,GATE,CAND,YEAR,WINDOWS,DIMS,blend_one


def validate(output):
    out=Path(output).resolve();dest=out/'validation'
    if dest.exists():raise ValueError('VALIDATION_EXISTS')
    cm=read_json(out/'candidate_manifest.json');sm=read_json(out/'scoring/manifest.json');ss=read_json(out/'scoring/summary.json')
    verify(cm['sources']);verify({str(out/'scoring'/p):h for p,h in sm['files'].items()})
    assert sha(out/'candidate_predictions.parquet')==cm['candidate_sha256']==sm['candidate_sha256']
    assert pd.Timestamp(cm['sealed_at']) < pd.Timestamp(ss['started_at'])
    al=read_json(out/'prediction_access_log.json')
    assert sha(out/'prediction_access_log.json')==cm['access_log_sha256']
    assert len(al)==1 and not set(al[0]['columns']) & {'actual','error','hit','joint_hit','truth_status'}
    gate=Path(cm['gate_archive']);original_validation=read_json(gate/'validation/validation.json')
    assert original_validation['status']=='PASS' and original_validation['raw_daily_truth_checks']>0
    p=pd.read_parquet(out/'candidate_predictions.parquet');r=pd.read_parquet(gate/'routing.parquet')
    assert not p.duplicated(KEY).any() and len(p)==len(r)
    x=p.merge(r[KEY+['gate_decision','direct_M1','direct_M2','selected_M1','selected_M2']],on=KEY,validate='one_to_one',suffixes=('','_original'))
    for c in ['gate_decision','direct_M1','direct_M2','selected_M1','selected_M2']:
        assert (x[c].eq(x[c+'_original']) | (x[c].isna()&x[c+'_original'].isna())).all()
    forecast_checks=0;poison_checks=0
    for z in p.to_dict('records'):
        for seg in ['M1','M2']:
            d,g,b=z['direct_'+seg],z['selected_'+seg],z['prediction_'+seg]
            expected=d if not z['gate_decision'] else d+(g-d)/2
            assert eq(expected,b)
            if not z['gate_decision'] and math.isfinite(d):
                assert np.float64(d).tobytes()==np.float64(b).tobytes()
            for nuisance in [{},{'actual':0.,'hit':False},{'actual':1e100,'error':-1e100,'hit':True}]:
                q=dict(reversed(list({'direct':d,'gate':g,'gate_decision':z['gate_decision'],**nuisance}.items())))
                assert eq(blend_one(q['direct'],q['gate'],q['gate_decision']),b)
                poison_checks+=1
            forecast_checks+=1
        assert eq(z['prediction_60'],z['prediction_M1']+z['prediction_M2'])
    scores=pd.read_parquet(out/'scoring/scores.parquet');old=pd.read_parquet(gate/'scoring/scores.parquet')
    assert not scores.duplicated(KEY+['window','model']).any()
    old_truth=old[old.model.eq(BASE)][KEY+['window','actual','truth_status','target_start','target_end','unknown_days']]
    chk=scores.merge(old_truth,on=KEY+['window'],validate='many_to_one',suffixes=('','_old'))
    assert len(chk)==len(scores)
    for c in ['actual','truth_status','target_start','target_end','unknown_days']:
        assert (chk[c].eq(chk[c+'_old']) | (chk[c].isna()&chk[c+'_old'].isna())).all()
    # Independently certified truth reuse, not rerunning old prediction validators.
    original=scores[scores.model.ne(CAND)].merge(old,on=KEY+['model','window'],validate='one_to_one',suffixes=('','_old'))
    assert len(original)==len(old)
    assert (original.prediction.eq(original.prediction_old)|(original.prediction.isna()&original.prediction_old.isna())).all()
    mature=scores[scores.issue_date.isin(p.issue_date.unique())]
    pred=p.set_index(KEY);keymap={};joint={};missing={};convex=[];epsilon=np.finfo(float).eps
    for key,q in mature.groupby(KEY):
        m=q.set_index(['model','window']);keymap[key]=m
        for model in [BASE,GATE,CAND,YEAR,'F28']:
            joint[key,model]=all(m.loc[(model,w),'actual']>0 and math.isfinite(m.loc[(model,w),'prediction']) and abs((m.loc[(model,w),'prediction']-m.loc[(model,w),'actual'])/m.loc[(model,w),'actual'])<=.2 for w in WINDOWS)
            missing[key,model]=any(not math.isfinite(m.loc[(model,w),'prediction']) for w in WINDOWS)
            if model in [BASE,GATE,CAND]:
                assert eq(m.loc[(model,'d01_60'),'prediction'],m.loc[(model,'d01_30'),'prediction']+m.loc[(model,'d31_60'),'prediction'])
        assert missing[key,BASE]==missing[key,CAND]==missing[key,GATE]
        for w,seg,a,b in [('d01_30','M1',1,30),('d31_60','M2',31,60),('d01_60','60',1,60)]:
            row=m.loc[(CAND,w)]
            assert pd.Timestamp(row.target_start)==pd.Timestamp(key[0])+pd.Timedelta(days=a)
            assert pd.Timestamp(row.target_end)==pd.Timestamp(key[0])+pd.Timedelta(days=b)
            assert eq(row.prediction,pred.loc[key,'prediction_'+seg])
            y,d,g,blend=row.actual,m.loc[(BASE,w),'prediction'],m.loc[(GATE,w),'prediction'],row.prediction
            if all(math.isfinite(v) for v in [y,d,g,blend]):
                tol=32*epsilon*max(1,abs(y),abs(d),abs(g),abs(blend))
                for loss,fn in [('absolute',abs),('over',lambda e:max(e,0)),('under',lambda e:max(-e,0))]:
                    upper=(fn(d-y)+fn(g-y))/2;value=fn(blend-y)
                    assert value<=upper+tol,(key,w,loss,value,upper)
                    convex.append(dict(zip(KEY,key),window=w,loss=loss,upper=upper,value=value,roundoff_tolerance=tol,gap=value-upper))
    paired_keys=[key for key in keymap if not missing[key,BASE]]
    groups={}
    for key in paired_keys:
        d,g=joint[key,BASE],joint[key,GATE]
        name='BOTH_HIT' if d and g else 'GATE_GAINED' if g else 'GATE_LOST' if d else 'NEITHER_HIT'
        groups.setdefault(name,[]).append(key)
        if d and g:assert joint[key,CAND],('COMMON_HIT_LOST',key)
    four=pd.read_parquet(out/'scoring/four_groups.parquet').set_index('original_group')
    for name,keys in groups.items():
        hitkeys=[k for k in keys if joint[k,CAND]]
        assert len(keys)==four.loc[name,'n'] and len(hitkeys)==four.loc[name,'blend_hit_n']
        assert math.fsum(keymap[k].loc[(BASE,'d01_60'),'actual'] for k in hitkeys)==four.loc[name,'blend_hit_actual60']
    # Compare recomputed endpoint migration with the prior archive, not reference constants.
    previous=pd.read_parquet(gate/'scoring/details.parquet');previous=previous[previous.paired]
    for name,keys in groups.items():
        mask={'BOTH_HIT':previous.direct_joint_hit&previous.gate_joint_hit,'GATE_GAINED':~previous.direct_joint_hit&previous.gate_joint_hit,
              'GATE_LOST':previous.direct_joint_hit&~previous.gate_joint_hit,'NEITHER_HIT':~previous.direct_joint_hit&~previous.gate_joint_hit}[name]
        assert set(map(tuple,previous.loc[mask,KEY].values))==set(keys)
    old_top=pd.read_parquet(gate/'scoring/top20_exits_actual.parquet')[KEY]
    new_top=pd.read_parquet(out/'scoring/fixed_top20.parquet')[KEY]
    pd.testing.assert_frame_equal(old_top.reset_index(drop=True),new_top.reset_index(drop=True))
    met=pd.read_parquet(out/'scoring/metrics.parquet');jt=pd.read_parquet(out/'scoring/joint_summary.parquet')
    obj=pd.read_parquet(out/'scoring/objects.parquet');obj=obj[obj.issue_date.isin(p.issue_date.unique())]
    metric_checks=0;joint_checks=0
    for scope in met.scope.unique():
        def sub(f):return f if scope=='FULL_CORE' else f[f.paired] if scope=='PAIRED' else f[f.paired & f.gate_decision.eq(scope=='GATE_TRUE')]
        for label,columns in DIMS.items():
            groupmap={k:q for k,q in sub(mature).groupby(['model','window']+columns,dropna=False)}
            for row in met[met.scope.eq(scope)&met.slice.eq(label)].to_dict('records'):
                k=tuple(row[c] for c in ['model','window']+columns);q=groupmap[k]
                calculated=scalar_metrics(list(q.itertuples()))
                for col,v in calculated.items():assert close(v,row[col]),(scope,label,k,col,v,row[col])
                metric_checks+=1
            groupmap={k:q for k,q in sub(obj).groupby(['model']+columns,dropna=False)}
            for row in jt[jt.scope.eq(scope)&jt.slice.eq(label)].to_dict('records'):
                q=groupmap[tuple(row[c] for c in ['model']+columns)]
                hit=[z for z in q.itertuples() if joint[((z.issue_date,z.shop,z.spu),z.model)]]
                weight=math.fsum(z.actual_d01_60 for z in hit)
                den=math.fsum(z.actual_d01_60 for z in q.itertuples() if math.isfinite(z.actual_d01_60))
                assert len(hit)==row['joint_hit_n'] and close(weight,row['joint_hit_actual'])
                assert close(weight/den if den else math.nan,row['joint_hit_actual_share'])
                joint_checks+=1
    pa=mature[mature.paired&mature.window.eq('d01_60')]
    calc={k:scalar_metrics(list(q.itertuples())) for k,q in pa.groupby('model')}
    dec=pa[pa.decline_group.eq('PRE_ISSUE_DECLINING')]
    dc={k:scalar_metrics(list(q.itertuples())) for k,q in dec.groupby('model')}
    def hitstats(model):
        keys=[k for k in paired_keys if joint[k,model]]
        return len(keys),math.fsum(keymap[k].loc[(model,'d01_60'),'actual'] for k in keys)
    dh,dw=hitstats(BASE);bh,bw=hitstats(CAND)
    gates=dict(paired_w60_wape_lower=calc[CAND]['wape']<calc[BASE]['wape'],joint_hit_count_increased=bh>dh,
               joint_hit_sales_share_not_lower=bw>=dw,
               declining_w60_over_rate_not_increased=dc[CAND]['over_rate']<=dc[BASE]['over_rate'] if dc[BASE]['actual_units']>0 else None,
               output_and_core_coverage_not_reduced=all(missing[k,BASE]==missing[k,CAND] for k in keymap))
    assert gates==ss['gates']
    assert calc[CAND]['wape']<=(calc[BASE]['wape']+calc[GATE]['wape'])/2+1e-14
    assert dc[CAND]['over_rate']<=(dc[BASE]['over_rate']+dc[GATE]['over_rate'])/2+1e-14
    immature=scores[~scores.issue_date.isin(p.issue_date.unique())]
    assert immature.truth_status.eq('NOT_MATURED').all() and immature.actual.isna().all()
    # The archived F28 side baseline already has forecasts for immature issues.
    # Preserve them byte-for-value (checked above), never score them or require
    # their deletion. New candidate and its DIRECT/GATE endpoints stay missing.
    assert immature[immature.model.isin([BASE,GATE,CAND])].prediction.isna().all()
    run_started=now();root=Path(__file__).resolve().parents[1]
    result=subprocess.run([sys.executable,'-B','-m','unittest','tests.test_direct_yearago_blend50','-v'],cwd=root,capture_output=True,text=True,encoding='utf-8')
    if result.returncode:raise ValueError(result.stdout+result.stderr)
    verify(cm['sources']);dest.mkdir(exist_ok=False)
    save(dest,'convexity_per_window',pd.DataFrame(convex))
    dump(dest/'tests_execution.json',dict(started_at=run_started,completed_at=now(),returncode=result.returncode,stdout=result.stdout,stderr=result.stderr,
                                         test_sha256=sha(root/'tests/test_direct_yearago_blend50.py')))
    receipt=dict(status='PASS',at=now(),candidate_sha256=cm['candidate_sha256'],scoring_manifest_sha256=sha(out/'scoring/manifest.json'),
        formula_checks=forecast_checks,truth_perturbation_checks=poison_checks,convexity_checks=len(convex),
        independent_metric_rows=metric_checks,independent_joint_rows=joint_checks,
        common_hits_preserved=len(groups['BOTH_HIT']),four_groups_reconciled=True,fixed_top20_unchanged=True,
        missing_output_n=sum(missing[k,BASE] for k in keymap),immature_n=len(immature[immature.model.eq(BASE)&immature.window.eq('d01_60')]),
        immature_f28_retained_forecast_rows=int(immature[immature.model.eq('F28')].prediction.notna().sum()),
        validation_repair='Removed an overstrict all-model immature-prediction-null assertion; archived F28 forecasts retained, no truth or scoring introduced',
        truth_chain=dict(method='Reuse hash-verified per-object truth independently summed from daily archive in prior GATE validation',
            source_validation_path=str(gate/'validation/validation.json'),source_validation_sha256=sha(gate/'validation/validation.json'),
            previous_daily_window_checks=original_validation['raw_daily_truth_checks'],current_truth_row_equality_checks=len(chk)),
        independent_gates=gates,exact_wape_convex_upper=(calc[BASE]['wape']+calc[GATE]['wape'])/2,
        exact_decline_over_convex_upper=(dc[BASE]['over_rate']+dc[GATE]['over_rate'])/2,
        source_hashes_unchanged=len(cm['sources']),new_fits=0,new_inference=0,database_queries=0,production_changes=0,
        script_sha256=sha(__file__))
    dump(dest/'validation.json',receipt);print(receipt,flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);validate(**vars(p.parse_args()))
