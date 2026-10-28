"""Independent dictionary, categorical checkpoint and scalar scoring checks; never fits models."""
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
C='DIRECT_ITEM_ID_V1'
MODELS=['DIRECT','BLEND50','F28','DIRECT_TRAFFIC_STATE_V1',C]


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
    assert feature_order==original_features+['item_key'] and config['categorical_feature']==['item_key']
    assert config['lightgbm']==read(archive/'prepared/frozen_config.json')['original']['lightgbm']
    train_rows=0;model_output_checks=0;slot_records=[];dictionary_checks=0;unknown_native_checks=0
    ledger=read(out/'formal_fit_ledger.json')
    import re
    for item in ledger['slots']:
        issue,segment=item['issue_date'],item['segment'];folder=out/'prepared'/f'{issue}_{segment}'
        train=pd.read_parquet(folder/'train.parquet');inf=pd.read_parquet(folder/'inference.parquet');cat=pd.read_parquet(folder/'dictionary.parquet')
        old=read(archive/'effects/slots'/f'{issue}_{segment}'/'training_audit.json')
        sorted_train=train.sort_values(old['sort_keys'],kind='stable')
        for col,h in old['column_sha256'].items():
            actual=hashlib.sha256(pd.util.hash_pandas_object(sorted_train[col],index=False).to_numpy().tobytes()).hexdigest()
            assert actual==h and str(train[col].dtype)==old['dtypes'][col],(issue,segment,col)
        assert len(train)==old['rows'] and not train.duplicated(old['sort_keys']).any()
        assert (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all()
        assert train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all()
        assert config['sample_weight'] is None
        train_rows+=len(train)
        keys=sorted(set(zip(train.shop,train.spu)))
        assert list(zip(cat.shop,cat.spu))==keys
        assert cat.item_key.tolist()==list(range(len(keys)))
        assert str(train.item_key.dtype)==str(inf.item_key.dtype)=='int32'
        mapping=dict(zip(keys,range(len(keys))))
        for frame in [train,inf]:
            expected=np.array([mapping.get(k,-1) for k in zip(frame.shop,frame.spu)],dtype=np.int32)
            assert np.array_equal(expected,frame.item_key.to_numpy())
            dictionary_checks+=len(frame)
        assert train.item_key.ge(0).all()
        sp=pd.read_parquet(folder/'support.parquet').set_index(['shop','spu'])
        checked={}
        for key,g in train.groupby(['shop','spu']):
            intervals=sorted(set(zip(g.label_end,g.label_start)))
            end=None;n=0
            for b,a in intervals:
                if end is None or a>end:n+=1;end=b
            checked[key]=(len(g),g.simulated_issue_date.nunique(),int((g.simulated_issue_date.max()-g.simulated_issue_date.min()).days+1),n)
        for r in inf.itertuples():
            x=sp.loc[(r.shop,r.spu)];expect=checked.get((r.shop,r.spu),(0,0,0,0))
            assert tuple(int(x[c]) for c in ['training_rows','training_dates','training_span_days','nonoverlap_windows'])==expect
            assert x.seen_in_training==(r.item_key>=0)
        ef=out/'effects'/f'{issue}_{segment}';result=read(ef/'result.json');pred=pd.read_parquet(ef/'predictions.parquet')
        pd.testing.assert_frame_equal(pred[inf.columns],inf,check_exact=True)
        eligible=inf.input_status.eq('OK');cp=read(ef/'checkpoint.json')
        assert cp['params']==config['lightgbm'] and cp['sample_weight'] is None and cp['categorical_feature']==['item_key']
        assert cp['features']==feature_order and cp['train_sha256']==sha(folder/'train.parquet') and cp['dictionary_sha256']==sha(folder/'dictionary.parquet')
        model=Booster(model_file=os.path.relpath(ef/'model.txt',Path.cwd()))
        assert model.feature_name()==feature_order
        params=dict(re.findall(r'^\[([^:]+): (.*?)\]$',(ef/'model.txt').read_text(encoding='utf-8'),re.M))
        assert params['categorical_feature']==str(len(original_features))
        for k,v in config['category_defaults'].items():assert float(params[k])==v
        item_splits=0
        stack=[t['tree_structure'] for t in model.dump_model()['tree_info']]
        while stack:
            node=stack.pop()
            if 'split_index' not in node:continue
            if node['split_feature']==len(original_features):assert node['decision_type']=='==';item_splits+=1
            else:assert node['decision_type']!='=='
            stack.extend([node['left_child'],node['right_child']])
        assert item_splits==result['item_categorical_splits']
        checkpred=np.maximum(model.predict(inf.loc[eligible,feature_order]),0)
        assert np.array_equal(checkpred,pred.loc[eligible,'prediction'].to_numpy())
        assert pred.loc[~eligible,'prediction'].isna().all()
        # Exercise native missing-category behavior without fitting or issuing new forecasts.
        probe=inf.loc[eligible,feature_order].head(3).copy();a=probe.copy();b=probe.copy();c=probe.copy()
        a['item_key']=-1;b['item_key']=-2;c['item_key']=np.nan
        assert np.array_equal(model.predict(a),model.predict(b)) and np.array_equal(model.predict(a),model.predict(c))
        unknown_native_checks+=len(probe);model_output_checks+=int(eligible.sum())
        slot_records.append(dict(issue_date=issue,segment=segment,status='PASS',training_rows=len(train),inference_rows=len(inf),categories=len(cat),item_categorical_splits=item_splits,unknown_native_checks=len(probe)))
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
        for model in MODELS:assert getattr(r,model+'_joint_hit')==scalar[key+(model,)]
    met=pd.read_parquet(out/'scoring/metrics.parquet');joint=pd.read_parquet(out/'scoring/joint_summary.parquet')
    dims={'overall':[],'issue':['issue_date'],'shop':['shop'],'year':['year'],'maturity':['maturity'],
          'decline_group':['decline_group'],'old_group':['old_group'],'output_origin':['origin_scope'],'seen':['seen_group'],'support':['support_band']}
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
    migration=pd.read_parquet(out/'scoring/joint_migrations.parquet')
    for r in migration.itertuples():
        z=details if r.scope=='FULL_CORE' else details[details.paired]
        for col in {'overall':[],'old_group':['old_group'],'issue':['issue_date'],'shop':['shop']}[r.slice]:z=z[z[col].eq(getattr(r,col))]
        statuses=[]
        for x in z.itertuples():
            a=getattr(x,r.baseline+'_joint_hit');b=getattr(x,C+'_joint_hit')
            statuses.append('RETAINED' if a and b else 'GAINED' if b else 'LOST' if a else 'NEITHER')
        z=z[np.array(statuses)==r.migration]
        assert len(z)==r.records and math.fsum(z.actual_d01_60)==r.actual60

    assert len(list((out/'effects').glob('*/fit_started.json')))==eff['formal_fits']<=40
    for p,h in prep['sources'].items():assert sha(p)==h,p
    dest.mkdir(exist_ok=False)
    pd.DataFrame(slot_records).to_csv(dest/'slots.csv',index=False,encoding='utf-8-sig')
    result=dict(status='PASS',completed_at=datetime.now(timezone.utc).isoformat(),formal_fits=eff['formal_fits'],
        validator_fits=0,old_training_rows_reconciled=train_rows,dictionary_encoding_rows=dictionary_checks,native_unknown_category_checks=unknown_native_checks,
        saved_model_output_checks=model_output_checks,independent_truth_equality_rows=len(scores),
        joint_object_model_checks=len(scalar),metric_rows_checked=comparisons,joint_metric_rows_checked=len(joint),
        independent_gates=independent_gates,original_common_failure_group_retained=1953,original_decline_group_retained=842,
        original_sources_and_shadow_unchanged=True,source_files_checked=len(prep['sources']),
        validator_sha256=sha(__file__),scoring_manifest_sha256=sha(out/'scoring/manifest.json'),database_queries=0,production_changes=0,
        truth_chain='Exact match to frozen complementarity truth, previously independently summed from daily archive; no source revision',
        limits='Old feature/label hashes checked on ALL training rows; every dictionary member and encoding checked; temporal support is not independent seasonal evidence')
    (dest/'validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();validate(a.out)
