"""Independent calendar, checkpoint and scalar scoring checks; never fits models."""
from __future__ import annotations
import argparse
from collections import defaultdict
import hashlib
import json
import math
import os
from pathlib import Path
from datetime import datetime,timezone
import numpy as np
import pandas as pd

KEY=['issue_date','shop','spu']
W=['d01_30','d31_60','d01_60']
C='DIRECT_TRAFFIC_STATE_V1'
N=['traffic_t7','traffic_t28','yield_r7','yield_r28','traffic_change','yield_change']


def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def close(a,b):return (pd.isna(a) and pd.isna(b)) or math.isclose(float(a),float(b),rel_tol=1e-11,abs_tol=1e-8)


def metrics_scalar(g):
    pairs=[(r.prediction,r.actual) for r in g.itertuples() if math.isfinite(r.prediction) and math.isfinite(r.actual)]
    errors=[p-y for p,y in pairs];den=math.fsum(y for p,y in pairs)
    over=math.fsum(max(e,0) for e in errors);under=math.fsum(max(-e,0) for e in errors)
    hits=[r for r in g.itertuples() if math.isfinite(r.prediction) and math.isfinite(r.actual) and r.actual>0 and abs(r.prediction-r.actual)/r.actual<=.2]
    return dict(scored_n=len(pairs),hit_n=len(hits),ae=over+under,wape=(over+under)/den if den else np.nan,
        bias=math.fsum(errors)/den if den else np.nan,over_units=over,under_units=under,
        over_rate=over/den if den else np.nan,actual_units=den,hit_actual=math.fsum(r.actual for r in hits))


def validate(out):
    from lightgbm import Booster
    out=Path(out).resolve();dest=out/'validation'
    if dest.exists():raise ValueError('VALIDATION_EXISTS')
    config=read(out/'configuration.json');root=Path(config['root']);archive=Path(config['archive'])
    prep=read(out/'prepare_manifest.json');eff=read(out/'effects_manifest.json');sm=read(out/'scoring/manifest.json')
    for p,h in prep['sources'].items():assert sha(p)==h,p
    for rel,h in {**prep['files'],**eff['files']}.items():assert sha(out/rel)==h,rel
    for rel,h in sm['files'].items():assert sha(out/'scoring'/rel)==h,rel
    feature_order=config['features'];original_features=config['original_features']
    assert feature_order==original_features+N+[x+'_valid' for x in N]
    assert config['lightgbm']==read(archive/'prepared/frozen_config.json')['original']['lightgbm']
    raw=pd.read_parquet(archive/'inputs/daily.parquet',columns=['ds','shop','spu','y','mapped','sales_state','listing_sessions_sum','null_sessions','source_rows','date_mismatch','deleted_rows','null_qty','negative_qty'])
    assert not raw.duplicated(['shop','spu','ds']).any()
    series={key:g.set_index('ds').sort_index() for key,g in raw.groupby(['shop','spu'])}
    raw_checks=0;train_rows=0;model_output_checks=0;slot_records=[]
    ledger=read(out/'formal_fit_ledger.json')
    for item in ledger['slots']:
        issue,segment=item['issue_date'],item['segment'];folder=out/'prepared'/f'{issue}_{segment}'
        train=pd.read_parquet(folder/'train.parquet');inf=pd.read_parquet(folder/'inference.parquet')
        check=read(folder/'training_check.json');old=read(archive/'effects/slots'/f'{issue}_{segment}'/'training_audit.json')
        sort=train.sort_values(old['sort_keys'],kind='stable')
        for col,h in old['column_sha256'].items():
            actual=hashlib.sha256(pd.util.hash_pandas_object(sort[col],index=False).to_numpy().tobytes()).hexdigest()
            assert actual==h,(issue,segment,col)
            assert str(train[col].dtype)==old['dtypes'][col]
        assert len(train)==old['rows'] and not train.duplicated(old['sort_keys']).any()
        assert (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all()
        assert train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all()
        assert config['sample_weight'] is None
        train_rows+=len(train)
        sample=train.iloc[np.unique(np.linspace(0,len(train)-1,min(64,len(train))).astype(int))]
        inspect=pd.concat([sample,inf],ignore_index=True)
        for row in inspect.itertuples():
            cutoff=pd.Timestamp(row.input_cutoff);expected={}
            src=series.get((row.shop,row.spu),pd.DataFrame(columns=raw.columns).set_index('ds'))
            for width in [7,28]:
                z=src.reindex(pd.date_range(cutoff-pd.Timedelta(days=width-1),cutoff))
                common=z.mapped.eq(True)&z.source_rows.gt(0)&z.date_mismatch.eq(0)&z.deleted_rows.eq(0)
                valid_t=common&np.isfinite(z.listing_sessions_sum)&z.listing_sessions_sum.ge(0)&z.null_sessions.eq(0)
                valid_y=common&np.isfinite(z.y)&z.y.ge(0)&z.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&z.null_qty.eq(0)&z.negative_qty.eq(0)
                ss=float(z.listing_sessions_sum.sum()) if valid_t.all() else np.nan
                yy=float(z.y.sum()) if valid_y.all() else np.nan
                expected['traffic_t'+str(width)]=ss/width
                expected['yield_r'+str(width)]=yy/ss if math.isfinite(yy) and math.isfinite(ss) and ss>0 else np.nan
            for name,a,b in [('traffic_change','traffic_t7','traffic_t28'),('yield_change','yield_r7','yield_r28')]:
                x,y=expected[a],expected[b];expected[name]=(x-y)/(x+y) if math.isfinite(x) and math.isfinite(y) and x+y>0 else np.nan
            for col,val in expected.items():
                assert close(getattr(row,col),val),(issue,segment,row.shop,row.spu,row.input_cutoff,col)
                assert getattr(row,col+'_valid')==int(math.isfinite(val))
            raw_checks+=1
        ef=out/'effects'/f'{issue}_{segment}';result=read(ef/'result.json');pred=pd.read_parquet(ef/'predictions.parquet')
        assert len(pred)==len(inf) and pred.input_status.eq(inf.input_status).all()
        pd.testing.assert_frame_equal(pred[inf.columns],inf,check_exact=True)
        eligible=inf.input_status.eq('OK')
        if result['status']=='SKIPPED_NO_NEW_INFORMATION':
            assert not any(train[x].dropna().nunique()>1 for x in N)
            orig=pd.read_parquet(archive/'effects/slots'/f'{issue}_{segment}'/'segment_predictions.parquet')
            x=pred.set_index(['shop','spu']).prediction;y=orig.set_index(['shop','spu']).prediction.reindex(x.index)
            assert ((x==y)|(x.isna()&y.isna())).all()
        else:
            cp=read(ef/'checkpoint.json');assert cp['params']==config['lightgbm'] and cp['sample_weight'] is None
            assert cp['features']==feature_order and cp['train_sha256']==sha(folder/'train.parquet')
            model=Booster(model_file=os.path.relpath(ef/'model.txt',Path.cwd()))
            assert model.feature_name()==feature_order
            # Check all already-issued local historical outputs from persisted models.
            checkpred=np.maximum(model.predict(inf.loc[eligible,feature_order]),0)
            assert np.array_equal(checkpred,pred.loc[eligible,'prediction'].to_numpy())
            assert pred.loc[~eligible,'prediction'].isna().all()
            model_output_checks+=int(eligible.sum())
        slot_records.append(dict(issue_date=issue,segment=segment,status='PASS',training_rows=len(train),inference_rows=len(inf),effect_status=result['status']))
        print({'validated_slot':f'{issue}_{segment}'},flush=True)
    scores=pd.read_parquet(out/'scoring/scores.parquet');details=pd.read_parquet(out/'scoring/details.parquet')
    original=pd.read_parquet(root/'core_model_complementarity_20261028_v1/historical_windows.parquet')
    base=original[original.model.eq('DIRECT')].set_index(KEY+['window'])
    assert len(details)==2820 and details.paired.sum()==2579
    assert (~details[C+'_output_complete']).sum()==241
    for r in scores.itertuples():
        truth=base.loc[tuple(getattr(r,k) for k in KEY)+(r.window,)]
        assert r.actual==truth.actual and pd.Timestamp(r.target_start)==pd.Timestamp(truth.target_start) and pd.Timestamp(r.target_end)==pd.Timestamp(truth.target_end)
    groups=defaultdict(dict)
    for r in scores.itertuples():groups[tuple(getattr(r,k) for k in KEY)+(r.model,)][r.window]=(r.prediction,r.actual)
    scalar={}
    for key,r in groups.items():
        p1,p2,p60=(r[w][0] for w in W)
        assert (p1+p2==p60) or all(math.isnan(x) for x in [p1,p2,p60])
        scalar[key]=all(math.isfinite(p) and y>0 and abs(p-y)/y<=.2 for p,y in r.values())
    for r in details.itertuples():
        key=tuple(getattr(r,k) for k in KEY)
        for model in ['DIRECT','BLEND50','F28',C]:assert getattr(r,model+'_joint_hit')==scalar[key+(model,)]
    met=pd.read_parquet(out/'scoring/metrics.parquet');joint=pd.read_parquet(out/'scoring/joint_summary.parquet')
    dims={'overall':[],'issue':['issue_date'],'shop':['shop'],'year':['year'],'maturity':['maturity'],
          'decline_group':['decline_group'],'old_group':['old_group'],'output_origin':['origin_scope']}
    comparisons=0
    for r in met.itertuples():
        g=scores[scores.model.eq(r.model)&scores.window.eq(r.window)]
        if r.scope=='PAIRED':g=g[g.paired]
        for col in dims[r.slice]:g=g[g[col].eq(getattr(r,col))]
        expected=metrics_scalar(g)
        assert len(g)==r.fixed_n
        for col,val in expected.items():assert close(getattr(r,col),val),(r.scope,r.slice,r.model,col)
        comparisons+=1
    obj=pd.read_parquet(out/'scoring/objects.parquet')
    for r in joint.itertuples():
        g=obj[obj.model.eq(r.model)]
        if r.scope=='PAIRED':g=g[g.paired]
        for col in dims[r.slice]:g=g[g[col].eq(getattr(r,col))]
        winners=[x for x in g.itertuples() if scalar[tuple(getattr(x,k) for k in KEY)+(r.model,)]]
        assert len(winners)==r.joint_hit_n and math.fsum(x.actual_d01_60 for x in winners)==r.joint_hit_actual
    independent_gates={}
    paired=scores[scores.paired];pair_details=details[details.paired]
    def model60(model,decline=False):
        q=paired[paired.model.eq(model)&paired.window.eq('d01_60')]
        if decline:q=q[q.decline_group.eq('PRE_ISSUE_DECLINING')];assert len(q)==842
        return metrics_scalar(q)
    hit_stats={model:(int(pair_details[model+'_joint_hit'].sum()),float(pair_details.loc[pair_details[model+'_joint_hit'],'actual_d01_60'].sum())) for model in ['DIRECT','BLEND50',C]}
    for bm in ['DIRECT','BLEND50']:
        independent_gates[bm]=dict(wape60_lower=model60(C)['wape']<model60(bm)['wape'],
            joint_hits_increased=hit_stats[C][0]>hit_stats[bm][0],joint_hit_sales_share_not_lower=hit_stats[C][1]>=hit_stats[bm][1],
            decline_over_rate_not_increased=model60(C,True)['over_rate']<=model60(bm,True)['over_rate'],
            output_and_core_coverage_not_reduced=bool(details[C+'_output_complete'].eq(details[bm+'_output_complete']).all()))
    summary=read(out/'scoring/summary.json');assert independent_gates==summary['gates']
    oldobj=pd.read_parquet(root/'core_model_complementarity_20261028_v1/objects.parquet').set_index(KEY)
    for r in details.itertuples():
        q=oldobj.loc[tuple(getattr(r,k) for k in KEY)]
        group='BLEND50_ORIGINAL_HIT' if q.BLEND50_joint_hit else 'OTHER_MODELS_ORIGINAL_RESCUE' if q.blend_rescued else 'ALL_SIX_ORIGINAL_NO_JOINT_HIT' if q.all_models_output and not q.any_joint_hit else 'MISSING_OR_UNDETERMINED'
        assert r.old_group==group
    assert details.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT').sum()==1953
    assert len(list((out/'effects').glob('*/fit_started.json')))==eff['formal_fits']<=40
    for p,h in prep['sources'].items():assert sha(p)==h,p
    dest.mkdir(exist_ok=False)
    pd.DataFrame(slot_records).to_csv(dest/'slots.csv',index=False,encoding='utf-8-sig')
    result=dict(status='PASS',completed_at=datetime.now(timezone.utc).isoformat(),formal_fits=eff['formal_fits'],
        validator_fits=0,old_training_rows_reconciled=train_rows,independent_calendar_rows=raw_checks,
        saved_model_output_checks=model_output_checks,independent_truth_equality_rows=len(scores),
        joint_object_model_checks=len(scalar),metric_rows_checked=comparisons,joint_metric_rows_checked=len(joint),
        independent_gates=independent_gates,original_common_failure_group_retained=1953,original_decline_group_retained=842,
        original_sources_and_shadow_unchanged=True,source_files_checked=len(prep['sources']),
        validator_sha256=sha(__file__),scoring_manifest_sha256=sha(out/'scoring/manifest.json'),database_queries=0,production_changes=0,
        truth_chain='Exact match to frozen complementarity truth, previously independently summed from daily archive; no source revision',
        limits='All calendar checks on inference plus fixed systematic training samples; old feature/label hashes checked on ALL training rows')
    (dest/'validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();validate(a.out)
