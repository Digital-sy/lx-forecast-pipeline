"""Independent archived-array checks for DIRECT_L1_V1; never fit or predict."""
from __future__ import annotations
import argparse, datetime as dt, hashlib, json, re
from pathlib import Path
import numpy as np
import pandas as pd

CANDIDATE='DIRECT_L1_V1'
KEY=['issue_date','shop','spu']
FEATURES=['shop_code','lag1','lag7','lag14','lag28',
    'rolling_mean_lag1_window_size7','rolling_mean_lag1_window_size28','dayofweek','month','dayofyear']
WINDOWS=['d01_30','d31_60','d01_60']
MODELS=['DIRECT','BLEND50','F28','DIRECT_CORE_WEIGHT2_V1',CANDIDATE]

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()

def array_sha(x):return hashlib.sha256(np.ascontiguousarray(x).tobytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')

def verify_configuration(original,candidate):
    expected=dict(original);expected.update(objective='regression_l1',metric='l1')
    if candidate!=expected:raise ValueError('ONLY_OBJECTIVE_AND_METRIC_MAY_CHANGE')
    if any(k in candidate for k in ['sample_weight','class_weight','init_score']):raise ValueError('WEIGHT_OR_INIT_OVERRIDE')
    return expected

def verify_old_columns(train,audit):
    z=train.sort_values(audit['sort_keys']).reset_index(drop=True)
    if len(z)!=audit['rows']:raise ValueError('OLD_TRAIN_ROWS')
    for c in audit['columns']:
        h=array_sha(pd.util.hash_pandas_object(z[c],index=False).to_numpy(dtype=np.uint64))
        if str(z[c].dtype)!=audit['dtypes'][c] or h!=audit['column_sha256'][c]:raise ValueError('OLD_COLUMN_CHANGED:'+c)

def verify_fit_receipt(train,fit):
    if fit['stage']!='FIT_RETURNED' or fit['rows']!=len(train) or fit['features']!=FEATURES:raise ValueError('FIT_SHAPE_OR_STAGE')
    if fit.get('no_sample_weight') is not True or fit.get('fit_kwargs')!=[]:raise ValueError('FIT_KWARGS_NOT_EMPTY')
    if not all(fit.get(k) is True for k in ['no_class_weight','no_eval_set','no_init_score']):raise ValueError('FIT_EXTRA_CONTROL')
    if fit.get('label_transform')!='NONE' or fit.get('zero_label_rows')!=int(train.y_label.eq(0).sum()):raise ValueError('LABEL_TRANSFORM_OR_ZERO_ROWS')
    if fit['label_dtype']!=str(train.y_label.dtype) or fit['y_sha256']!=array_sha(train.y_label.to_numpy()):raise ValueError('FIT_LABEL_CHANGED')
    for c in FEATURES:
        if fit['feature_dtypes'][c]!=str(train[c].dtype) or fit['X_column_sha256'][c]!=array_sha(train[c].to_numpy()):raise ValueError('FIT_FEATURE_OR_ORDER_CHANGED:'+c)

def verify_model_text(text,expected):
    params=dict(re.findall(r'^\[([^:]+): (.*?)\]$',text,re.M))
    names=re.search(r'^feature_names=(.*)$',text,re.M)
    if names is None or names.group(1).split()!=FEATURES:raise ValueError('MODEL_FEATURES')
    if params.get('objective')!='regression_l1' or params.get('metric')!='l1':raise ValueError('MODEL_OBJECTIVE_METRIC')
    aliases={'n_estimators':'num_iterations','num_leaves':'num_leaves','learning_rate':'learning_rate',
        'min_child_samples':'min_data_in_leaf','reg_lambda':'lambda_l2','random_state':'seed','n_jobs':'num_threads',
        'deterministic':'deterministic','force_col_wise':'force_col_wise','verbosity':'verbosity'}
    for k,v in expected.items():
        if k in {'objective','metric'}:continue
        key=aliases.get(k,k)
        if key not in params or float(params[key])!=float(v):raise ValueError('MODEL_CONFIG_CHANGED:'+k)
    if params.get('weight_column','') not in {'','-1'} or params.get('boost_from_average')!='1':raise ValueError('MODEL_WEIGHT_OR_INITIALIZATION')
    return params

def verify_predictions(inference,pred):
    for c in inference:
        pd.testing.assert_series_equal(inference[c].reset_index(drop=True),pred[c].reset_index(drop=True),check_names=False,check_exact=True)
    ok=inference.input_status.eq('OK').to_numpy()
    raw=pred.raw_prediction.to_numpy(float);out=pred.prediction.to_numpy(float)
    if not np.array_equal(np.isfinite(raw),ok) or not np.array_equal(np.isfinite(out),ok):raise ValueError('OUTPUT_SCOPE')
    if not np.array_equal(out[ok],np.maximum(raw[ok],0)):raise ValueError('NONNEGATIVE_POSTPROCESSING_CHANGED')
    reloaded=pred.raw_prediction_reloaded.to_numpy(float)
    if not np.array_equal(raw,reloaded,equal_nan=True):raise ValueError('SAVED_MODEL_PREDICTION_MISMATCH')
    return dict(output_n=int(ok.sum()),raw_negative_n=int((raw[ok]<0).sum()),raw_sha256=array_sha(raw[ok]),
        clipped_sha256=array_sha(out[ok]),saved_readback_exact=True)

def unweighted_metrics(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float);known=np.isfinite(y);valid=known&np.isfinite(p)
    a=y[valid];pred=p[valid];e=pred-a;total=float(a.sum());ae=float(np.abs(e).sum())
    over=float(e[e>0].sum());under=float(-e[e<0].sum());hit=np.zeros(len(a),bool);positive=a>0
    hit[positive]=np.abs(e[positive])/a[positive]<=.2
    return dict(fixed_n=len(y),output_n=int(np.isfinite(p).sum()),truth_n=int(known.sum()),scored_n=int(valid.sum()),
        actual_units=total,prediction_units=float(pred.sum()),ae=ae,wape=ae/total if total>0 else None,
        bias=float(e.sum()/total) if total>0 else None,over_units=over,under_units=under,over_rate=over/total if total>0 else None,
        hit_n=int(hit.sum()),hit_actual=float(a[hit].sum()),zero_truth_n=int((y==0).sum()),zero_truth_ae=float(np.abs(e[a==0]).sum()),
        positive_actual_zero_prediction_n=int(((a>0)&(pred==0)).sum()),
        positive_actual_zero_prediction_actual=float(a[(a>0)&(pred==0)].sum()))

def joint_flags(actual,pred):
    actual,pred=np.asarray(actual,float),np.asarray(pred,float)
    if actual.shape!=pred.shape or actual.ndim!=2 or actual.shape[1]!=3:raise ValueError('THREE_WINDOWS_REQUIRED')
    errors=np.full_like(actual,np.nan);np.divide(np.abs(actual-pred),actual,out=errors,where=actual>0)
    return np.isfinite(actual).all(axis=1)&np.isfinite(pred).all(axis=1)&(actual>0).all(axis=1)&(errors<=.2).all(axis=1)

def compare_number(a,b,name):
    if a is None:
        if pd.notna(b):raise ValueError('EXPECTED_UNDEFINED:'+name)
    elif isinstance(a,(int,np.integer)):
        if a!=b:raise ValueError('COUNT_MISMATCH:'+name)
    elif not np.isclose(float(a),float(b),rtol=1e-12,atol=1e-8,equal_nan=True):raise ValueError('METRIC_MISMATCH:'+name)

def validate_auxiliary(out,dest,frozen,source):
    risk=out/'auxiliary_risk'
    if not (risk/'manifest.json').exists():return dict(status='NOT_RUN',reason='AUXILIARY_NOT_SEALED')
    manifest=read(source(risk/'manifest.json'));prepared=out/'auxiliary_prepared';pm=read(source(prepared/'manifest.json'))
    if pm['membership_reselected'] is not False or pm['truth_rebuilt'] is not False or pm['new_fits']!=0 or manifest['new_fits']!=0:raise ValueError('AUXILIARY_CONTRACT')
    for p,h in pm['sources'].items():
        if sha(p)!=h:raise ValueError('AUXILIARY_SOURCE_CHANGED:'+p)
        source(p)
    d=pd.read_parquet(source(risk/'details.parquet'));pool=pd.read_parquet(source(pm['pool_path']))
    if len(d)!=len(pool)*6 or d.duplicated(KEY+['model','window']).any():raise ValueError('AUXILIARY_JOIN')
    prior=pd.read_parquet(source(pm['prior_details_path']));cols=KEY+['window','prediction','actual','truth_status','core_state','transition']
    for frame in [d[d.model.eq('DIRECT')],prior[prior.model.eq('DIRECT')]]:
        if frame.duplicated(KEY+['window']).any():raise ValueError('AUXILIARY_BASELINE_DUPLICATE')
    pd.testing.assert_frame_equal(d[d.model.eq('DIRECT')][cols].sort_values(KEY+['window']).reset_index(drop=True),
        prior[prior.model.eq('DIRECT')][cols].sort_values(KEY+['window']).reset_index(drop=True),check_exact=True)
    truth=pd.read_parquet(source(pm['truth_path']))
    joined=d.merge(truth[KEY+['window','actual']],on=KEY+['window'],validate='many_to_one',suffixes=('','_original'))
    if not np.array_equal(joined.actual,joined.actual_original,equal_nan=True):raise ValueError('AUXILIARY_TRUTH_CHANGED')
    selected=d[d.model.eq(CANDIDATE)].set_index(KEY+['window'])
    for c in ['raw_prediction','prediction','clipped_amount','negative_component_n']:
        pivot=selected[c].unstack('window')
        if not np.array_equal(pivot.d01_60,pivot.d01_30+pivot.d31_60,equal_nan=True):raise ValueError('AUXILIARY_SEGMENT_SUM:'+c)
    for w in WINDOWS[:2]:
        z=d[d.model.eq(CANDIDATE)&d.window.eq(w)]
        if not np.array_equal(z.prediction,np.maximum(z.raw_prediction,0),equal_nan=True):raise ValueError('AUXILIARY_CLIPPING_CHANGED')
    if d[d.model.eq('DIRECT')].raw_prediction.notna().any():raise ValueError('DIRECT_RAW_NOT_ARCHIVED')
    metrics=pd.read_csv(source(risk/'metrics.csv'),float_precision='round_trip')
    groups={'ALL_ELIGIBLE':np.ones(len(d),bool),'CURRENT_NON_CORE':d.core_state.eq('NON_CORE'),'CURRENT_CORE':d.core_state.eq('CORE'),
        'EXITED_CORE':d.transition.eq('EXITED_CORE'),'NEW_CORE':d.transition.eq('NEW_CORE'),
        'UNKNOWN_CORE_OR_TRANSITION':d.core_state.eq('UNKNOWN')|d.previous_core_state.eq('UNKNOWN')}
    checks=[]
    for group,mask in groups.items():
        for model in ['DIRECT',CANDIDATE]:
            for w in WINDOWS:
                z=d[mask&d.model.eq(model)&d.window.eq(w)];r=metrics[(metrics.scope==group)&(metrics.model==model)&(metrics.window==w)].iloc[0]
                c=unweighted_metrics(z.actual,z.prediction);rename={'ae':'absolute_error'}
                for k in ['fixed_n','output_n','truth_n','scored_n','actual_units','prediction_units','ae','wape','bias','over_units','under_units','over_rate']:
                    compare_number(c[k],r[rename.get(k,k)],'auxiliary '+group+model+w+k)
                zero=z.actual.eq(0)&z.prediction.notna()
                compare_number(int(zero.sum()),r.zero_actual_n,'zero_count')
                compare_number(float(z.loc[zero,'prediction'].sum()),r.zero_actual_prediction_units,'zero_prediction')
                compare_number(float(z.loc[zero,'prediction'].abs().sum()),r.zero_actual_absolute_error,'zero_ae')
                compare_number(c['positive_actual_zero_prediction_n'],r.positive_actual_zero_prediction_n,'positive_zero_n')
                compare_number(c['positive_actual_zero_prediction_actual'],r.positive_actual_zero_prediction_actual_units,'positive_zero_actual')
                compare_number(int(z.raw_prediction.lt(0).sum()),r.raw_negative_n,'aux_raw_negative_n')
                compare_number(float((-z.raw_prediction).clip(lower=0).sum()),r.raw_negative_absolute_units,'aux_raw_negative_units')
                checks.append(dict(scope=group,model=model,window=w,rows=len(z),actual=c['actual_units'],ae=c['ae']))
    shared=[]
    for model in ['DIRECT',CANDIDATE]:
        for w in WINDOWS:
            p=frozen[KEY+[model+'_prediction_'+w]].rename(columns={model+'_prediction_'+w:'main_prediction'})
            z=d[d.model.eq(model)&d.window.eq(w)].merge(p,on=KEY,validate='one_to_one',how='inner');valid=z.prediction.notna()&z.main_prediction.notna()
            if not np.array_equal(z.loc[valid,'prediction'],z.loc[valid,'main_prediction']):raise ValueError('AUX_MAIN_OUTPUT_MISMATCH')
            shared.append(dict(model=model,window=w,exact_equal_records=int(valid.sum())))
    pd.DataFrame(checks).to_csv(dest/'auxiliary_metrics_recomputed.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(shared).to_csv(dest/'auxiliary_main_output_equality.csv',index=False,encoding='utf-8-sig')
    calls=pd.read_csv(source(risk/'inference_calls.csv'))
    if calls.inference_calls.sum()!=manifest['inference_calls']:raise ValueError('AUX_CALL_COUNT')
    if calls.direct_inference_calls.sum()!=0 or manifest['direct_inference_calls']!=0:raise ValueError('OLD_MODEL_REINFERRED')
    return dict(status='PASS',metric_cells=len(checks),pool_records=len(pool),persisted_inference_calls=int(calls.inference_calls.sum()),
        shared_prediction_records=sum(r['exact_equal_records'] for r in shared),validator_inference_calls=0,upstream_auxiliary_status=manifest['status'])

def run(root,out):
    root,out=Path(root).resolve(),Path(out).resolve();dest=out/'validation'
    if dest.exists():raise ValueError('NO_VALIDATION_OVERWRITE')
    dest.mkdir();sources={};started=dt.datetime.now(dt.timezone.utc).isoformat()
    def source(p):
        p=Path(p).resolve();sources[str(p)]=sha(p);return p
    archive=root/'bc60_monthly_replay_20261020';item=root/'direct_item_id_v1_20261028';comp=root/'core_model_complementarity_20261028_v1'
    conf=read(source(out/'configuration.json'));prep=read(source(out/'prepare_manifest.json'));effects=read(source(out/'effects_manifest.json'))
    original=read(source(archive/'prepared/frozen_config.json'))
    expected=verify_configuration(original['original']['lightgbm'],conf['lightgbm'])
    if conf['features']!=FEATURES or effects['formal_fits']!=40 or conf.get('sample_weight') is not None or conf.get('fit_kwargs')!=[]:raise ValueError('CONFIG_OR_FIT_SCOPE')
    if len(list((out/'effects').glob('*/fit_started.json')))!=40:raise ValueError('ACTUAL_FIT_LEDGER_COUNT')
    for name,expected_count in [('live_prediction_calls',40),('reload_prediction_calls',40),('main_prediction_calls',80)]:
        if effects[name]!=expected_count:raise ValueError('MAIN_PREDICTION_CALL_LEDGER:'+name)
    for p,h in prep['sources'].items():
        if sha(p)!=h:raise ValueError('PREP_SOURCE_CHANGED:'+p)
        sources[p]=h
    shadow=read(source(comp/'manifest.json'))['protected_shadow_files']
    for p,h in shadow.items():
        if sha(p)!=h:raise ValueError('SHADOW_CHANGED:'+p)
        sources[p]=h
    grid=pd.read_csv(source(archive/'prepared/issue_grid.csv'));grid=grid[grid.calendar_mature.eq(True)]
    unit_checks=[];candidate=[]
    for issue in grid.issue_date:
        for seg in ['M1','M2']:
            slot=f'{issue}_{seg}';t=pd.read_parquet(source(item/'prepared'/slot/'train.parquet')).drop(columns='item_key')
            audit=read(source(archive/'effects/slots'/slot/'training_audit.json'));verify_old_columns(t,audit)
            tc=read(source(out/'prepared'/slot/'training_check.json'))
            actual_order=array_sha(pd.util.hash_pandas_object(t[['shop','spu','simulated_issue_date']],index=False).to_numpy())
            if actual_order!=tc['training_order_sha256'] or tc['sample_weight'] is not None:raise ValueError('TRAINING_ORDER_OR_WEIGHT')
            if not t.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all():raise ValueError('LABEL_NOT_MATURE')
            if not (t.input_cutoff==t.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('OWN_CUTOFF')
            cp=read(source(out/'effects'/slot/'checkpoint.json'));fit=read(source(out/'effects'/slot/'fit_received.json'));verify_fit_receipt(t,fit)
            if cp['params']!=expected or cp['fit_received_sha256']!=sha(out/'effects'/slot/'fit_received.json'):raise ValueError('CHECKPOINT_OR_FIT_RECEIPT')
            modelpath=source(out/'effects'/slot/'model.txt')
            if sha(modelpath)!=cp['model_sha256']:raise ValueError('CANDIDATE_MODEL_HASH')
            params=verify_model_text(modelpath.read_text(encoding='utf-8'),expected)
            pred=pd.read_parquet(source(out/'effects'/slot/'predictions.parquet'))
            prior=pd.read_parquet(source(item/'prepared'/slot/'inference.parquet')).drop(columns='item_key')
            predcheck=verify_predictions(prior,pred)
            result=read(source(out/'effects'/slot/'result.json'))
            if result['prediction_calls']!=2 or result.get('save_reload_exact') is not True:raise ValueError('MAIN_AND_RELOAD_CALL_COUNT')
            for name in ['native','reload']:
                pr=read(source(out/'effects'/slot/(name+'_prediction_receipt.json')))
                start=read(source(out/'effects'/slot/(name+'_started.json')))
                if pr['prediction_sha256']!=predcheck['raw_sha256'] or pr['rows']!=predcheck['output_n'] or start['rows']!=pr['rows']:raise ValueError('RAW_PREDICTION_RECEIPT')
            if result['negative_raw_n']!=predcheck['raw_negative_n']:raise ValueError('RAW_NEGATIVE_COUNT')
            candidate.append(pred[KEY+['segment','raw_prediction','prediction']])
            unit_checks.append(dict(issue_date=issue,segment=seg,rows=len(t),old_columns_exact=True,actual_fit_receipt_checked=True,
                model_metadata_checked=True,no_sample_weight=True,native_boost_from_average=params['boost_from_average'],**predcheck))
    pd.DataFrame(unit_checks).to_csv(dest/'training_checks.csv',index=False,encoding='utf-8-sig')
    frozen=pd.read_parquet(source(comp/'objects.parquet'));segments=pd.concat(candidate)
    new=segments.pivot(index=KEY,columns='segment',values='prediction');raw=segments.pivot(index=KEY,columns='segment',values='raw_prediction')
    frozen=frozen.set_index(KEY);new=new.reindex(frozen.index)
    if len(new)!=2820 or int(frozen.paired.sum())!=2579:raise ValueError('FIXED_EVALUATION_SCOPE')
    frozen[CANDIDATE+'_prediction_d01_30']=new.M1;frozen[CANDIDATE+'_prediction_d31_60']=new.M2;frozen[CANDIDATE+'_prediction_d01_60']=new.M1+new.M2
    raw=raw.reindex(frozen.index)
    for w,values in [('d01_30',raw.M1),('d31_60',raw.M2),('d01_60',raw.M1+raw.M2)]:frozen[CANDIDATE+'_raw_'+w]=values
    weighted=pd.read_parquet(source(root/'direct_core_weight2_v1_20261028/scoring/details.parquet')).set_index(KEY).reindex(frozen.index)
    for w in WINDOWS:frozen['DIRECT_CORE_WEIGHT2_V1_prediction_'+w]=weighted['DIRECT_CORE_WEIGHT2_V1_prediction_'+w]
    frozen=frozen.reset_index();metric=pd.read_parquet(source(out/'scoring/metrics.parquet'));joint=pd.read_parquet(source(out/'scoring/joint_summary.parquet'))
    details=pd.read_parquet(source(out/'scoring/details.parquet')).set_index(KEY).reindex(pd.MultiIndex.from_frame(frozen[KEY])).reset_index()
    summary=read(source(out/'scoring/summary.json'));metric_checks=[];joint_checks=[];migration_checks=[];independent={}
    for scope,mask in [('FULL_CORE',np.ones(len(frozen),bool)),('PAIRED',frozen.paired.eq(True).to_numpy())]:
        z=frozen[mask];a=z[['actual_'+w for w in WINDOWS]].to_numpy();independent[scope]={}
        for model in MODELS:
            p=z[[model+'_prediction_'+w for w in WINDOWS]].to_numpy();hit=joint_flags(a,p);complete=np.isfinite(p).all(axis=1);act=float(z.actual_d01_60.sum())
            result=dict(joint_hit_n=int(hit.sum()),joint_hit_actual=float(a[hit,2].sum()),joint_hit_actual_share=float(a[hit,2].sum()/act),output_complete_n=int(complete.sum()))
            row=joint[(joint.scope==scope)&(joint.slice=='overall')&(joint.model==model)].iloc[0]
            for name,value in result.items():compare_number(value,row[name],model+name)
            joint_checks.append(dict(scope=scope,model=model,**result));independent[scope][model]=dict(joint=result,hit=hit)
            for k,wname in enumerate(WINDOWS):
                calc=unweighted_metrics(a[:,k],p[:,k]);row=metric[(metric.scope==scope)&(metric.slice=='overall')&(metric.model==model)&(metric.window==wname)].iloc[0]
                for name,value in calc.items():compare_number(value,row[name],model+wname+name)
                if model==CANDIDATE:
                    compare_number(int(z[CANDIDATE+'_raw_'+wname].lt(0).sum()),row.raw_negative_n,'RAW_NEGATIVE_METRIC')
                metric_checks.append(dict(scope=scope,model=model,window=wname,**calc));independent[scope][model][wname]=calc
            if scope=='PAIRED':
                decline=z.decline_group.eq('PRE_ISSUE_DECLINING').to_numpy()
                if decline.sum()!=842:raise ValueError('DECLINE_SCOPE')
                independent[scope][model]['decline']=unweighted_metrics(a[decline,2],p[decline,2])
        for model in ['DIRECT','BLEND50']:
            oldhit=independent[scope][model]['hit'];newhit=independent[scope][CANDIDATE]['hit']
            states=np.select([oldhit&newhit,~oldhit&newhit,oldhit&~newhit],['RETAINED','GAINED','LOST'],default='NEITHER')
            if not np.array_equal(states,details.loc[mask,'migration_vs_'+model].to_numpy()):raise ValueError('MIGRATION_MISMATCH')
            for state in ['RETAINED','GAINED','LOST','NEITHER']:
                k=states==state;migration_checks.append(dict(scope=scope,comparison=model,state=state,n=int(k.sum()),actual60=float(a[k,2].sum())))
    gates={};p=independent['PAIRED'];n=p[CANDIDATE]
    for comparator in ['DIRECT','BLEND50']:
        b=p[comparator]
        calc=dict(wape60_lower=n['d01_60']['wape']<b['d01_60']['wape'],joint_hits_increased=n['joint']['joint_hit_n']>b['joint']['joint_hit_n'],
            joint_hit_sales_share_not_lower=n['joint']['joint_hit_actual_share']>=b['joint']['joint_hit_actual_share'],
            decline_over_rate_not_increased=n['decline']['over_rate']<=b['decline']['over_rate'],
            output_and_core_coverage_not_reduced=n['joint']['output_complete_n']>=b['joint']['output_complete_n'] and
                independent['FULL_CORE'][CANDIDATE]['joint']['output_complete_n']>=independent['FULL_CORE'][comparator]['joint']['output_complete_n'])
        if calc!=summary['gates'][comparator]:raise ValueError('FIVE_GATES_MISMATCH:'+comparator)
        gates[comparator]=calc
    for name,rows in [('independent_metrics',metric_checks),('independent_joint',joint_checks),('independent_migrations',migration_checks)]:
        pd.DataFrame(rows).to_csv(dest/(name+'.csv'),index=False,encoding='utf-8-sig')
    auxiliary=validate_auxiliary(out,dest,frozen,source)
    for p,h in sources.items():
        if sha(p)!=h:raise ValueError('SOURCE_MUTATED_AT_END:'+p)
    result=dict(status='PASS',started_at=started,completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),units=len(unit_checks),
        training_rows_checked=sum(r['rows'] for r in unit_checks),full_core_n=2820,paired_n=2579,decline_n=842,shadow_files_unchanged=len(shadow),
        gates=gates,auxiliary_validation=auxiliary,validator_training_calls=0,validator_inference_calls=0,sources=sources,script_sha256=sha(__file__))
    dump(dest/'validation.json',result);print(json.dumps({k:v for k,v in result.items() if k!='sources'},ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args();run(a.root,a.out)
