"""Independent checks for fixed historical-core weights; no fitting or inference."""
from __future__ import annotations
import argparse, datetime as dt, hashlib, json, re
from pathlib import Path
import numpy as np
import pandas as pd

CANDIDATE='DIRECT_CORE_WEIGHT2_V1'
KEY=['issue_date','shop','spu']
TRAIN_KEY=['shop','spu','simulated_issue_date','input_cutoff']
FEATURES=['shop_code','lag1','lag7','lag14','lag28',
          'rolling_mean_lag1_window_size7','rolling_mean_lag1_window_size28',
          'dayofweek','month','dayofyear']
WINDOWS=['d01_30','d31_60','d01_60']

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()

def array_sha(x):return hashlib.sha256(np.asarray(x,dtype=np.float64).tobytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')

def expected_weights(states):
    states=np.asarray(states,object)
    if not np.isin(states,['CORE','NON_CORE','UNKNOWN']).all():raise ValueError('UNKNOWN_ASSOCIATION_IS_NOT_UNKNOWN_STATE')
    raw=np.ones(len(states),float);raw[states=='CORE']=2.
    if not len(raw):raise ValueError('EMPTY_TRAINING')
    return raw,raw/raw.mean()

def validate_weight_rows(train,w):
    if len(train)!=len(w) or w.duplicated(TRAIN_KEY).any():raise ValueError('WEIGHT_ROW_COUNT_OR_KEY')
    if not np.array_equal(w.row_index,np.arange(len(train))):raise ValueError('WEIGHT_ORDER')
    for c in TRAIN_KEY:
        a=train[c].reset_index(drop=True);b=w[c].reset_index(drop=True)
        if not a.equals(b):raise ValueError('WEIGHT_KEY_ORDER:'+c)
    raw,expected=expected_weights(w.core_state)
    if not np.array_equal(raw,w.raw_weight):raise ValueError('RAW_WEIGHT')
    if not np.array_equal(expected,w.sample_weight):raise ValueError('NORMALIZED_WEIGHT')
    if not np.isfinite(expected).all() or not (expected>0).all():raise ValueError('NONPOSITIVE_WEIGHT')
    if not np.isclose(expected.sum(),len(train),rtol=1e-14,atol=1e-8):raise ValueError('WEIGHT_SUM')
    return array_sha(expected)

def verify_old_columns(train,audit):
    z=train.sort_values(audit['sort_keys']).reset_index(drop=True)
    if len(z)!=audit['rows']:raise ValueError('OLD_TRAIN_ROWS')
    for c in audit['columns']:
        h=hashlib.sha256(pd.util.hash_pandas_object(z[c],index=False).to_numpy(dtype=np.uint64).tobytes()).hexdigest()
        if str(z[c].dtype)!=audit['dtypes'][c] or h!=audit['column_sha256'][c]:raise ValueError('OLD_COLUMN_CHANGED:'+c)

def direct_core_state(daily,shop,spu,cutoff):
    """Independent group/sort implementation, rather than rolling-array helper."""
    c=pd.Timestamp(cutoff);g=daily[daily.shop.eq(shop)&daily.ds.between(c-pd.Timedelta(days=89),c)]
    known=g.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&np.isfinite(g.y)&g.y.ge(0)
    own=g[g.spu.eq(spu)]
    if (g.ds.nunique()!=90 or not known.all() or g.y.sum()<=0 or len(own)!=90 or
        own.ds.nunique()!=90 or not own.mapped.eq(True).all()):return 'UNKNOWN'
    rank=g[g.mapped.eq(True)].groupby('spu').y.sum().sort_values(ascending=False)
    total=float(g.y.sum());acc=0.;boundary=None
    for v in rank:
        acc+=v
        if acc>=.9*total:boundary=v;break
    if spu not in rank.index:return 'UNKNOWN'
    return 'CORE' if boundary is None or rank.loc[spu]>=boundary else 'NON_CORE'

def unweighted_metrics(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float)
    known=np.isfinite(y);valid=known&np.isfinite(p);a=y[valid];pred=p[valid];e=pred-a
    total=float(a.sum());ae=float(np.abs(e).sum());over=float(e[e>0].sum());under=float(-e[e<0].sum())
    hit=np.zeros(len(a),bool);positive=a>0;hit[positive]=(np.abs(e[positive])/a[positive])<=.2
    return dict(fixed_n=len(y),output_n=int(np.isfinite(p).sum()),truth_n=int(known.sum()),scored_n=int(valid.sum()),
        actual_units=total,ae=ae,wape=ae/total if total>0 else None,bias=float(e.sum()/total) if total>0 else None,
        over_units=over,under_units=under,over_rate=over/total if total>0 else None,
        hit_n=int(hit.sum()),hit_actual=float(a[hit].sum()),
        zero_truth_n=int((y==0).sum()),zero_truth_ae=float(np.abs(e[a==0]).sum()))

def joint_flags(actual,pred):
    actual,pred=np.asarray(actual,float),np.asarray(pred,float)
    if actual.shape!=pred.shape or actual.shape[1]!=3:raise ValueError('THREE_WINDOWS_REQUIRED')
    errors=np.full_like(actual,np.nan);np.divide(np.abs(actual-pred),actual,out=errors,where=actual>0)
    return (np.isfinite(actual).all(axis=1)&np.isfinite(pred).all(axis=1)&(actual>0).all(axis=1)&(errors<=.2).all(axis=1))

def compare_number(a,b,name):
    if a is None:
        if pd.notna(b):raise ValueError('EXPECTED_UNDEFINED:'+name)
    elif isinstance(a,(int,np.integer)):
        if a!=b:raise ValueError('COUNT_MISMATCH:'+name)
    elif not np.isclose(float(a),float(b),rtol=1e-12,atol=1e-8,equal_nan=True):raise ValueError('METRIC_MISMATCH:'+name)

def mapping_evidence(daily):
    states=daily.groupby(['shop','spu']).mapped.nunique(dropna=False)
    result=dict(rows=len(daily),objects=len(states),mixed_mapping_objects=int(states.gt(1).sum()),
        null_mapping_rows=int(daily.mapped.isna().sum()))
    if result['mixed_mapping_objects'] or result['null_mapping_rows']:
        raise ValueError('MAPPING_VARIATION_REQUIRES_INDEPENDENT_RULE_REVIEW')
    return result

def validate_auxiliary(root,out,dest,frozen,source):
    risk=out/'auxiliary_risk';prepared=out/'auxiliary_prepared'
    if not (risk/'manifest.json').exists():return dict(status='NOT_RUN',reason='AUXILIARY_NOT_SEALED')
    manifest=read(source(risk/'manifest.json'));pm=read(source(prepared/'manifest.json'))
    if pm['target_truth_constructed'] is not False or pm['new_fits']!=0 or manifest['new_fits']!=0:raise ValueError('AUXILIARY_CONTRACT')
    d=pd.read_parquet(source(risk/'details.parquet'));pool=pd.read_parquet(source(prepared/'pool.parquet'))
    if len(d)!=len(pool)*6 or d.duplicated(KEY+['model','window']).any():raise ValueError('AUXILIARY_JOIN')
    metrics=pd.read_csv(source(risk/'metrics.csv'),float_precision='round_trip')
    groups={'ALL_ELIGIBLE':np.ones(len(d),bool),'CURRENT_NON_CORE':d.core_state.eq('NON_CORE'),
        'CURRENT_CORE':d.core_state.eq('CORE'),'EXITED_CORE':d.transition.eq('EXITED_CORE'),
        'NEW_CORE':d.transition.eq('NEW_CORE'),'UNKNOWN_CORE_OR_TRANSITION':d.core_state.eq('UNKNOWN')|d.previous_core_state.eq('UNKNOWN')}
    checks=[]
    for group,mask in groups.items():
        for model in ['DIRECT',CANDIDATE]:
            for w in WINDOWS:
                z=d[mask&d.model.eq(model)&d.window.eq(w)];r=metrics[(metrics.scope==group)&(metrics.model==model)&(metrics.window==w)].iloc[0]
                c=unweighted_metrics(z.actual,z.prediction)
                rename={'ae':'absolute_error'}
                for k in ['fixed_n','output_n','truth_n','scored_n','actual_units','ae','wape','over_units','under_units','over_rate']:
                    compare_number(c[k],r[rename.get(k,k)],'auxiliary '+group+model+w+k)
                zero=z.actual.eq(0)&z.prediction.notna()
                compare_number(int(zero.sum()),r.zero_actual_n,'zero_count')
                compare_number(float(z.loc[zero,'prediction'].sum()),r.zero_actual_prediction_units,'zero_prediction')
                compare_number(float((z.loc[zero,'prediction']-z.loc[zero,'actual']).abs().sum()),r.zero_actual_absolute_error,'zero_ae')
                checks.append(dict(scope=group,model=model,window=w,rows=len(z),actual=c['actual_units'],ae=c['ae']))
    # Auxiliary loading of the same saved models provides a separate persisted output
    # path for main-core records. This validator makes no new prediction calls.
    shared=[]
    for model in ['DIRECT',CANDIDATE]:
        for w in WINDOWS:
            p=frozen[KEY+[model+'_prediction_'+w]].rename(columns={model+'_prediction_'+w:'main_prediction'})
            z=d[d.model.eq(model)&d.window.eq(w)].merge(p,on=KEY,validate='one_to_one',how='inner')
            valid=z.prediction.notna()&z.main_prediction.notna()
            if not np.array_equal(z.loc[valid,'prediction'],z.loc[valid,'main_prediction']):raise ValueError('AUX_MAIN_SAVED_OUTPUT_MISMATCH')
            shared.append(dict(model=model,window=w,exact_equal_records=int(valid.sum())))
    pd.DataFrame(checks).to_csv(dest/'auxiliary_metrics_recomputed.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(shared).to_csv(dest/'auxiliary_main_output_equality.csv',index=False,encoding='utf-8-sig')
    calls=pd.read_csv(source(risk/'inference_calls.csv'))
    if calls.inference_calls.sum()!=manifest['inference_calls']:raise ValueError('AUX_CALL_COUNT')
    return dict(status='PASS',metric_cells=len(checks),pool_records=len(pool),persisted_inference_calls=int(calls.inference_calls.sum()),
        shared_prediction_records=sum(r['exact_equal_records'] for r in shared),validator_inference_calls=0,
        upstream_auxiliary_status=manifest['status'])

def run(root,out):
    root,out=Path(root).resolve(),Path(out).resolve();dest=out/'validation'
    if dest.exists():raise ValueError('NO_VALIDATION_OVERWRITE')
    dest.mkdir();sources={};started=dt.datetime.now(dt.timezone.utc).isoformat()
    def source(p):
        p=Path(p).resolve();sources[str(p)]=sha(p);return p
    archive=root/'bc60_monthly_replay_20261020';item=root/'direct_item_id_v1_20261028'
    comp=root/'core_model_complementarity_20261028_v1'
    conf=read(source(out/'configuration.json'));prep=read(source(out/'prepare_manifest.json'))
    effects=read(source(out/'effects_manifest.json'));original=read(source(archive/'prepared/frozen_config.json'))
    if conf['lightgbm']!=original['original']['lightgbm']:raise ValueError('ORIGINAL_CONFIG_MISMATCH')
    if conf['features']!=FEATURES:raise ValueError('FEATURES_CHANGED')
    if effects['formal_fits']!=40:raise ValueError('EXPECTED_FORTY_FIXED_FITS')
    if len(list((out/'effects').glob('*/fit_started.json')))!=40:raise ValueError('ACTUAL_FIT_LEDGER_COUNT')
    for p,h in prep['sources'].items():
        if sha(p)!=h:raise ValueError('PREP_SOURCE_CHANGED:'+p)
        sources[p]=h
    shadow=read(source(comp/'manifest.json'))['protected_shadow_files']
    for p,h in shadow.items():
        if sha(p)!=h:raise ValueError('SHADOW_CHANGED:'+p)
        sources[p]=h
    grid=pd.read_csv(source(archive/'prepared/issue_grid.csv'));grid=grid[grid.calendar_mature.eq(True)]
    sampled=[];unit_checks=[];same_identity=[];candidate=[]
    for issue in grid.issue_date:
        previous=None
        for seg in ['M1','M2']:
            slot=f'{issue}_{seg}';t=pd.read_parquet(source(item/'prepared'/slot/'train.parquet')).drop(columns='item_key')
            w=pd.read_parquet(source(out/'prepared'/slot/'weights.parquet'))
            audit=read(source(archive/'effects/slots'/slot/'training_audit.json'));verify_old_columns(t,audit)
            weight_hash=validate_weight_rows(t,w)
            if not t.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all():raise ValueError('LABEL_NOT_MATURE')
            if not (t.input_cutoff==t.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('OWN_CUTOFF')
            if previous is not None:
                z=previous.merge(w[TRAIN_KEY+['core_state','raw_weight']],on=TRAIN_KEY,suffixes=('_m1','_m2'),validate='one_to_one')
                if not z.core_state_m1.equals(z.core_state_m2) or not z.raw_weight_m1.equals(z.raw_weight_m2):raise ValueError('M1_M2_IDENTITY_MISMATCH')
                same_identity.append(dict(issue_date=issue,common_rows=len(z)))
            else:previous=w[TRAIN_KEY+['core_state','raw_weight']]
            for _,g in w.groupby('core_state'):
                for i in np.unique(np.linspace(0,len(g)-1,min(3,len(g)),dtype=int)):
                    r=g.iloc[i];sampled.append(dict(issue_date=issue,segment=seg,shop=r.shop,spu=r.spu,input_cutoff=r.input_cutoff,core_state=r.core_state))
            cp=read(source(out/'effects'/slot/'checkpoint.json'))
            fit=read(source(out/'effects'/slot/'fit_received.json'))
            if fit['sample_weight_sha256']!=weight_hash:raise ValueError('FIT_WEIGHT_NOT_REGISTERED')
            if fit['features']!=FEATURES or fit['rows']!=len(t):raise ValueError('FIT_INPUT_SHAPE')
            if fit['stage']!='FIT_RETURNED' or fit['sample_weight_dtype']!='float64':raise ValueError('FIT_NOT_COMPLETED')
            if cp['fit_received_sha256']!=sha(out/'effects'/slot/'fit_received.json'):raise ValueError('FIT_RECEIPT_HASH')
            if cp['sample_weight_sha256']!=weight_hash:raise ValueError('CHECKPOINT_WEIGHT_HASH')
            if fit['y_sha256']!=hashlib.sha256(np.ascontiguousarray(t.y_label.to_numpy()).tobytes()).hexdigest():raise ValueError('FIT_LABEL_ORDER')
            for c in FEATURES:
                h=hashlib.sha256(np.ascontiguousarray(t[c].to_numpy()).tobytes()).hexdigest()
                if h!=fit['X_column_sha256'][c] or str(t[c].dtype)!=fit['feature_dtypes'][c]:raise ValueError('FIT_FEATURE_ORDER:'+c)
            modelpath=source(out/'effects'/slot/'model.txt')
            if sha(modelpath)!=cp['model_sha256']:raise ValueError('CANDIDATE_MODEL_HASH')
            text=modelpath.read_text(encoding='utf-8');params=dict(re.findall(r'^\[([^:]+): (.*?)\]$',text,re.M))
            names=re.search(r'^feature_names=(.*)$',text,re.M).group(1).split()
            if names!=FEATURES or params.get('objective')!='poisson':raise ValueError('MODEL_METADATA')
            pred=pd.read_parquet(source(out/'effects'/slot/'predictions.parquet'));prior=pd.read_parquet(source(item/'prepared'/slot/'inference.parquet')).drop(columns='item_key')
            for c in prior:
                pd.testing.assert_series_equal(prior[c].reset_index(drop=True),pred[c].reset_index(drop=True),check_names=False,check_exact=True)
            if not pred.prediction.notna().equals(prior.input_status.eq('OK')):raise ValueError('OUTPUT_QUALIFICATION_CHANGED')
            candidate.append(pred[KEY+['segment','prediction']]);unit_checks.append(dict(issue_date=issue,segment=seg,rows=len(t),
                sample_weight_sha256=weight_hash,core_n=int(w.core_state.eq('CORE').sum()),unknown_n=int(w.core_state.eq('UNKNOWN').sum()),
                weight_mean=float(w.sample_weight.mean()),weight_sum=float(w.sample_weight.sum()),old_columns_exact=True,
                actual_fit_receipt_checked=True,model_metadata_checked=True))
    pd.DataFrame(unit_checks).to_csv(dest/'training_checks.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(same_identity).to_csv(dest/'cross_segment_identity.csv',index=False,encoding='utf-8-sig')
    daily=pd.read_parquet(source(archive/'inputs/daily.parquet'),columns=['ds','shop','spu','y','mapped','sales_state'])
    mapping=mapping_evidence(daily)
    for r in sampled:
        r['independent_state']=direct_core_state(daily,r['shop'],r['spu'],r['input_cutoff'])
        if r['independent_state']!=r['core_state']:raise ValueError('CORE_STATE_SAMPLE')
    pd.DataFrame(sampled).to_csv(dest/'independent_core_samples.csv',index=False,encoding='utf-8-sig')
    frozen=pd.read_parquet(source(comp/'objects.parquet'));new=pd.concat(candidate).pivot(index=KEY,columns='segment',values='prediction')
    frozen=frozen.set_index(KEY);new=new.reindex(frozen.index)
    if len(new)!=2820 or int(frozen.paired.sum())!=2579:raise ValueError('FIXED_EVALUATION_SCOPE')
    frozen[CANDIDATE+'_prediction_d01_30']=new.M1;frozen[CANDIDATE+'_prediction_d31_60']=new.M2
    frozen[CANDIDATE+'_prediction_d01_60']=new.M1+new.M2
    frozen=frozen.reset_index();metric=pd.read_parquet(source(out/'scoring/metrics.parquet'));joint=pd.read_parquet(source(out/'scoring/joint_summary.parquet'))
    details=pd.read_parquet(source(out/'scoring/details.parquet')).set_index(KEY).reindex(pd.MultiIndex.from_frame(frozen[KEY])).reset_index()
    summary=read(source(out/'scoring/summary.json'));models=['DIRECT','BLEND50','F28',CANDIDATE]
    metric_checks=[];joint_checks=[];migration_checks=[];independent={}
    for scope,mask in [('FULL_CORE',np.ones(len(frozen),bool)),('PAIRED',frozen.paired.eq(True).to_numpy())]:
        z=frozen[mask];a=z[['actual_'+w for w in WINDOWS]].to_numpy();independent[scope]={}
        for model in models:
            p=z[[model+'_prediction_'+w for w in WINDOWS]].to_numpy();hit=joint_flags(a,p)
            complete=np.isfinite(p).all(axis=1);act=float(z.actual_d01_60.sum())
            result=dict(joint_hit_n=int(hit.sum()),joint_hit_actual=float(a[hit,2].sum()),
                joint_hit_actual_share=float(a[hit,2].sum()/act),output_complete_n=int(complete.sum()))
            row=joint[(joint.scope==scope)&(joint.slice=='overall')&(joint.model==model)].iloc[0]
            for name,value in result.items():compare_number(value,row[name],model+name)
            joint_checks.append(dict(scope=scope,model=model,**result));independent[scope][model]=dict(joint=result,hit=hit)
            for k,wname in enumerate(WINDOWS):
                calc=unweighted_metrics(a[:,k],p[:,k]);row=metric[(metric.scope==scope)&(metric.slice=='overall')&(metric.model==model)&(metric.window==wname)].iloc[0]
                for name,value in calc.items():compare_number(value,row[name],model+wname+name)
                metric_checks.append(dict(scope=scope,model=model,window=wname,**calc));independent[scope][model][wname]=calc
            if scope=='PAIRED':
                decline=z.decline_group.eq('PRE_ISSUE_DECLINING').to_numpy()
                if decline.sum()!=842:raise ValueError('DECLINE_SCOPE')
                independent[scope][model]['decline']=unweighted_metrics(a[decline,2],p[decline,2])
        for model in ['DIRECT','BLEND50']:
            oldhit=independent[scope][model]['hit'];newhit=independent[scope][CANDIDATE]['hit']
            states=np.select([oldhit&newhit,~oldhit&newhit,oldhit&~newhit],['RETAINED','GAINED','LOST'],default='NEITHER')
            stated=details.loc[mask,'migration_vs_'+model].to_numpy()
            if not np.array_equal(states,stated):raise ValueError('MIGRATION_MISMATCH')
            for state in ['RETAINED','GAINED','LOST','NEITHER']:
                k=states==state;migration_checks.append(dict(scope=scope,comparison=model,state=state,n=int(k.sum()),actual60=float(a[k,2].sum())))
    gates={};p=independent['PAIRED'];n=p[CANDIDATE]
    for comparator in ['DIRECT','BLEND50']:
        b=p[comparator]
        calc=dict(wape60_lower=n['d01_60']['wape']<b['d01_60']['wape'],
            joint_hits_increased=n['joint']['joint_hit_n']>b['joint']['joint_hit_n'],
            joint_hit_sales_share_not_lower=n['joint']['joint_hit_actual_share']>=b['joint']['joint_hit_actual_share'],
            decline_over_rate_not_increased=n['decline']['over_rate']<=b['decline']['over_rate'],
            output_and_core_coverage_not_reduced=n['joint']['output_complete_n']>=b['joint']['output_complete_n'] and
                independent['FULL_CORE'][CANDIDATE]['joint']['output_complete_n']>=independent['FULL_CORE'][comparator]['joint']['output_complete_n'])
        if calc!=summary['gates'][comparator]:raise ValueError('FIVE_GATES_MISMATCH:'+comparator)
        gates[comparator]=calc
    for name,rows in [('independent_metrics',metric_checks),('independent_joint',joint_checks),('independent_migrations',migration_checks)]:
        pd.DataFrame(rows).to_csv(dest/(name+'.csv'),index=False,encoding='utf-8-sig')
    auxiliary=validate_auxiliary(root,out,dest,frozen,source)
    for p,h in sources.items():
        if sha(p)!=h:raise ValueError('SOURCE_MUTATED_AT_END:'+p)
    result=dict(status='PASS',started_at=started,completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        units=len(unit_checks),training_rows_checked=sum(r['rows'] for r in unit_checks),
        independent_core_samples=len(sampled),m1_m2_common_rows_checked=sum(r['common_rows'] for r in same_identity),
        mapped_source_evidence=mapping,
        full_core_n=2820,paired_n=2579,decline_n=842,shadow_files_unchanged=len(shadow),gates=gates,
        auxiliary_validation=auxiliary,
        validator_training_calls=0,validator_inference_calls=0,sources=sources,script_sha256=sha(__file__))
    dump(dest/'validation.json',result);print(json.dumps({k:v for k,v in result.items() if k!='sources'},ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args();run(a.root,a.out)
