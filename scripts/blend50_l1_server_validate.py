"""Independent current-run validation: no model loading, inference or training."""
from __future__ import annotations
import argparse, datetime as dt, hashlib, importlib.metadata, json
from pathlib import Path
import numpy as np
import pandas as pd

KEY=['shop','spu']
BASE=['shop_code','lag1','lag7','lag14','lag28','rolling_mean_lag1_window_size7','rolling_mean_lag1_window_size28','dayofweek','month','dayofyear']
ANNUAL=BASE+['ly_input_mean28','ly_target_mean30','ly_scale_contrast','ly_shape_contrast','ly_input_complete','ly_target_complete','ly_positive_observed_before_input_start']
PREFIXES={'f28':'F28','direct':'DIRECT30x2','annual':'YEARAGO_COMPONENT','blend':'DIRECT_YEARAGO_BLEND50','l1':'DIRECT_L1_V1','candidate':'BLEND50_L1_MEAN_V1'}
WINDOWS={'M1':('d01_30',1,30),'M2':('d31_60',31,60),'60':('d01_60',1,60)}
SHOPS={'JQ-US','MT-US','RKZ-US','SY-US'}

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def write_new(p,v):
    with Path(p).open('x',encoding='utf-8') as f:json.dump(v,f,indent=2,ensure_ascii=False,allow_nan=False,default=str)
def unique(d,k):
    if d[k].isna().any().any() or d.duplicated(k).any():raise ValueError('DUPLICATE_OR_UNKNOWN_KEY')
def exact(a,b,label):
    a,b=np.asarray(a),np.asarray(b)
    if a.shape!=b.shape or not np.array_equal(a,b,equal_nan=True):raise ValueError('NOT_EXACT:'+label)
def verify_files(base,files):
    for name,h in files.items():
        if sha(Path(base)/name)!=h:raise ValueError('FILE_CHANGED:'+name)
def before_target(issue,at):
    z=pd.Timestamp(at)
    if z.tz is None:raise ValueError('TIMEZONE_REQUIRED')
    if z.tz_convert('Asia/Shanghai').date()!=pd.Timestamp(issue).date():raise ValueError('ACTUAL_ISSUE_DATE_MISMATCH')
    if z>=(pd.Timestamp(issue)+pd.Timedelta(days=1)).tz_localize('Asia/Shanghai'):raise ValueError('TARGET_STARTED')
def expected_values(d,y,l,gate):
    d,y,l=np.asarray(d,float),np.asarray(y,float),np.asarray(l,float);g=np.asarray(gate,bool)
    if not(d.shape==y.shape==l.shape==g.shape):raise ValueError('ENDPOINT_SHAPE')
    if any(np.isinf(v).any() or (v[np.isfinite(v)]<0).any() for v in [d,y,l]):raise ValueError('INVALID_ENDPOINT')
    if (g&(~np.isfinite(d)|~np.isfinite(y))).any():raise ValueError('MISSING_GATE_TRUE_ENDPOINT')
    b=d.copy();b[g]=d[g]+.5*(y[g]-d[g]);return b,.5*b+.5*l
def core_keys(daily,cutoff):
    cutoff=pd.Timestamp(cutoff);start=cutoff-pd.Timedelta(days=89);d=daily[pd.to_datetime(daily.ds).between(start,cutoff)].copy()
    unique(d,KEY+['ds']);keys=[];stats=[]
    for shop in sorted(SHOPS):
        z=d[d.shop.eq(shop)];valid=z.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&np.isfinite(z.y)&z.y.ge(0)
        if z.ds.nunique()!=90 or not valid.all():raise ValueError('CORE_SELECTION_SOURCE')
        total=float(z.y.sum());mapped=z[z.mapped.eq(True)].groupby(KEY,as_index=False).y.sum().sort_values(['y','spu'],ascending=[False,True])
        if total<=0 or mapped.y.sum()<.9*total:raise ValueError('CORE_SELECTION_DENOMINATOR')
        idx=int(np.flatnonzero(mapped.y.cumsum().to_numpy()>=.9*total)[0]);threshold=float(mapped.iloc[idx].y)
        selected=mapped[mapped.y.ge(threshold)];keys.extend(selected[KEY].to_dict('records'))
        stats.append(dict(shop=shop,full_shop_units=total,selected_units=float(selected.y.sum()),core_n=len(selected),selection_coverage=float(selected.y.sum()/total),unmapped_units=float(z.loc[~z.mapped.eq(True),'y'].sum())))
    return pd.DataFrame(keys).sort_values(KEY).reset_index(drop=True),stats

def run(root):
    root=Path(root).resolve();prep=root/'prepared';stage=root/'staging';target=stage/'independent_validation.json'
    if target.exists():raise ValueError('NO_VALIDATION_OVERWRITE')
    started=dt.datetime.now(dt.timezone.utc).isoformat();conf=read(prep/'config.json');before_target(conf['issue'],started)
    cutoff=pd.Timestamp(conf['issue'])-pd.Timedelta(days=2)
    if str(cutoff.date())!=conf['cutoff'] or conf['maximum_fits']!=6 or conf['future_truth_read'] is not False:raise ValueError('RUN_CONTRACT')
    release=read(conf['release_manifest']);verify_files(Path(conf['release_manifest']).parent,release['files'])
    if {name:importlib.metadata.version(name) for name in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_VERSION')
    if sha(conf['release_manifest'])!=conf['release_manifest_sha256']:raise ValueError('RELEASE_HASH')
    if release['files'].get('scripts/blend50_l1_server_validate.py')!=sha(__file__):raise ValueError('VALIDATOR_NOT_PINNED')
    for p,h in conf['sources'].items():
        if sha(p)!=h:raise ValueError('SOURCE_CHANGED:'+p)
    parity=read(conf['parity_receipt'])
    if parity['status']!='PASS' or parity['release_manifest_sha256']!=conf['release_manifest_sha256']:raise ValueError('PARITY_NOT_BOUND')
    inp=Path(conf['input_root']);im=read(inp/'manifest.json');iv=read(inp/'independent_validation.json')
    if im['status']!='INPUT_READY_NOT_ISSUED' or im['blockers'] or iv['status']!='PASS' or im['daily_sha256']!=iv['daily_sha256'] or sha(inp/'daily.parquet')!=im['daily_sha256']:raise ValueError('INPUT_GATE')
    if im['issue_date']!=conf['issue'] or im['cutoff']!=conf['cutoff']:raise ValueError('INPUT_DATE')
    prepared=read(prep/'manifest.json');verify_files(prep,prepared['files'])
    core=pd.read_parquet(prep/'core.parquet');unique(core,KEY)
    if sha(prep/'core.parquet')!=conf['core_sha256']:raise ValueError('CORE_CHANGED')
    source=pd.read_parquet(inp/'daily.parquet',columns=KEY+['ds','y','mapped','sales_state'],filters=[('ds','>=',cutoff-pd.Timedelta(days=89)),('ds','<=',cutoff)])
    actual_core,coverage=core_keys(source,cutoff);pd.testing.assert_frame_equal(core[KEY].sort_values(KEY).reset_index(drop=True),actual_core,check_exact=True)
    amounts=source[source.mapped.eq(True)].groupby(KEY).y.sum().reindex(pd.MultiIndex.from_frame(core[KEY])).to_numpy()
    exact(core.selection_qty,amounts,'CORE_HISTORICAL_UNITS')
    info={};mat={};values={};receipts=[];fit_count=0;native=0;reloads=0;training_rows=0
    for seg in ['M1','M2']:
        folder=prep/seg;meta=read(folder/'manifest.json');verify_files(folder,meta['files']);info[seg]=meta
        train=pd.read_parquet(folder/'direct_train.parquet');annual_train=pd.read_parquet(folder/'annual_train.parquet')
        pd.testing.assert_frame_equal(train,annual_train[train.columns],check_exact=True);unique(train,KEY+['simulated_issue_date'])
        if not train.label_end.le(cutoff).all() or not train.input_cutoff.eq(train.simulated_issue_date-pd.Timedelta(days=2)).all() or not train.history_end.eq(train.input_cutoff).all():raise ValueError('TRAINING_TIME_LEAK')
        if not train.label_start.eq(train.simulated_issue_date+pd.Timedelta(days=WINDOWS[seg][1])).all() or not train.label_end.eq(train.simulated_issue_date+pd.Timedelta(days=WINDOWS[seg][2])).all():raise ValueError('TRAINING_LABEL_WINDOW')
        training_rows+=len(train)
        dinf=pd.read_parquet(folder/'direct_inference.parquet');ainf=pd.read_parquet(folder/'annual_inference.parquet');aw=pd.read_parquet(folder/'inference_annual_windows.parquet')
        pd.testing.assert_frame_equal(dinf,ainf[dinf.columns],check_exact=True);pd.testing.assert_frame_equal(dinf[KEY],core[KEY],check_exact=True);pd.testing.assert_frame_equal(aw[KEY],core[KEY],check_exact=True)
        a,b=WINDOWS[seg][1:]
        for c,v in [('input_cutoff',cutoff),('simulated_issue_date',pd.Timestamp(conf['issue'])),('label_start',pd.Timestamp(conf['issue'])+pd.Timedelta(days=a)),('label_end',pd.Timestamp(conf['issue'])+pd.Timedelta(days=b))]:
            if not pd.to_datetime(dinf[c]).eq(v).all():raise ValueError('INFERENCE_DATE:'+c)
        if not pd.to_datetime(aw.ly_target_end).le(cutoff).all():raise ValueError('ANNUAL_FUTURE_INPUT')
        mat[seg]=(dinf,ainf,aw)
        support=meta['annual_support'];allowed_annual=support['support_dates']>=30 and support['support_products']>=10 and bool(support['numerical_features_vary'])
        if allowed_annual!=support['fit_allowed']:raise ValueError('ANNUAL_SUPPORT')
        for prefix in ['direct','annual','l1']:
            slot=root/'effects'/f'{prefix}_{seg}';r=read(slot/'result.json');verify_files(slot,r['files']);receipts.append(r)
            source_prefix='direct' if prefix=='l1' else prefix;features=BASE if source_prefix=='direct' else ANNUAL
            allowed=not meta['direct_support'].startswith('NOT_TRAINABLE') if source_prefix=='direct' else allowed_annual
            params=dict(conf['lightgbm'])
            if prefix=='l1':params.update(objective='regression_l1',metric='l1')
            if r['parameters']!=params or r['features']!=features or r['sample_weight']!='NOT_PASSED' or r['init_score']!='NOT_PASSED':raise ValueError('FIT_CONFIGURATION')
            fi=dinf if source_prefix=='direct' else ainf;tr=train if source_prefix=='direct' else annual_train
            if r['feature_dtypes']!={f:str(fi[f].dtype) for f in features}:raise ValueError('INFERENCE_DTYPES')
            if allowed and r['training_feature_dtypes']!={f:str(tr[f].dtype) for f in features}:raise ValueError('TRAINING_DTYPES')
            if r['training_sha256']!=sha(folder/(source_prefix+'_train.parquet')) or r['inference_sha256']!=sha(folder/(source_prefix+'_inference.parquet')):raise ValueError('FIT_INPUT_HASH')
            if r['fit_attempted']!=allowed or r['status']!=('FITTED' if allowed else 'SKIPPED_SUPPORT'):raise ValueError('FIT_STATUS')
            pred=pd.read_parquet(slot/'predictions.parquet');pd.testing.assert_frame_equal(pred[KEY],core[KEY],check_exact=True)
            if sha(slot/'predictions.parquet')!=r['prediction_sha256']:raise ValueError('PREDICTION_HASH')
            exact(pred.prediction,np.maximum(pred.prediction_raw,0),'NONNEGATIVE_POSTPROCESS')
            expected_mask=dinf.input_status.eq('OK').to_numpy()&allowed
            if not np.array_equal(np.isfinite(pred.prediction),expected_mask) or not np.array_equal(pred.output_status.eq('OK'),expected_mask):raise ValueError('OUTPUT_QUALIFICATION')
            if allowed:
                if r['save_reload_exact'] is not True or sha(slot/'model.txt')!=r['model_sha256']:raise ValueError('MODEL_SAVE')
                native_df=pd.read_parquet(slot/'native_predictions.parquet');reload_df=pd.read_parquet(slot/'reload_predictions.parquet')
                pd.testing.assert_frame_equal(native_df,reload_df,check_exact=True);exact(native_df.prediction_raw,pred.prediction_raw,'NATIVE_RAW')
                text=(slot/'model.txt').read_text(encoding='utf-8');names=next(x.split('=',1)[1].split(' ') for x in text.splitlines() if x.startswith('feature_names='))
                if names!=features:raise ValueError('SAVED_MODEL_FEATURES')
                objective=next(x.split('=',1)[1] for x in text.splitlines() if x.startswith('objective='))
                if not objective.startswith('regression_l1' if prefix=='l1' else 'poisson'):raise ValueError('SAVED_MODEL_OBJECTIVE')
            expected_calls=int(bool(expected_mask.any()) and allowed)
            if r['native_prediction_calls']!=expected_calls or r['reload_prediction_calls']!=expected_calls:raise ValueError('PREDICTION_CALL_COUNT')
            fit_count+=int(r['fit_attempted']);native+=r['native_prediction_calls'];reloads+=r['reload_prediction_calls'];values[(prefix,seg)]=pred.prediction.to_numpy()
        del train,annual_train
    if fit_count>6 or len(list((root/'effects').glob('*/fit_started.json')))!=fit_count or native!=reloads:raise ValueError('FIT_OR_PREDICT_BUDGET')
    sm=read(stage/'manifest.json');verify_files(stage,sm['files']);wide=pd.read_parquet(stage/'routing_predictions.parquet');long=pd.read_parquet(stage/'predictions.parquet')
    calculation=read(stage/'calculation_receipt.json')
    if calculation['formal_fits']!=fit_count or calculation['native_prediction_calls']!=native or calculation['reload_prediction_calls']!=reloads or calculation['future_truth_read'] is not False or calculation['production_writes']!=0 or calculation['api_calls']!=0:raise ValueError('CALCULATION_LEDGER')
    unique(wide,KEY);pd.testing.assert_frame_equal(wide[KEY],core[KEY],check_exact=True);unique(long,KEY+['model','window'])
    a,b=mat['M1'][1],mat['M2'][1];wa,wb=mat['M1'][2],mat['M2'][2]
    conditions={'direct_valid':np.isfinite(values[('direct','M1')])&np.isfinite(values[('direct','M2')]),'input_valid':a.ly_input_complete.eq(True)&wa.ly_input_unsafe_days.eq(0),'targets_valid':a.ly_target_complete.eq(True)&b.ly_target_complete.eq(True)&wa.ly_target_unsafe_days.eq(0)&wb.ly_target_unsafe_days.eq(0),'prior_positive':a.ly_positive_observed_before_input_start.eq(True),'both_supported':np.full(len(core),all(info[s]['annual_support']['fit_allowed'] for s in ['M1','M2'])),'both_annual':np.isfinite(values[('annual','M1')])&np.isfinite(values[('annual','M2')])&all(info[s]['annual_support']['fit_allowed'] for s in ['M1','M2'])}
    gate=np.ones(len(core),bool)
    for c,v in conditions.items():
        if not np.array_equal(wide[c],v):raise ValueError('GATE_CONDITION:'+c)
        gate &= np.asarray(v,bool)
    if not np.array_equal(wide.gate_decision,gate):raise ValueError('GATE')
    baseline=pd.read_parquet(prep/'f28.parquet').pivot(index=KEY,columns='window',values='prediction').reindex(pd.MultiIndex.from_frame(core[KEY]))
    generated={}
    for seg in ['M1','M2']:
        d,y,l=[values[(kind,seg)] for kind in ['direct','annual','l1']];bl,p=expected_values(d,y,l,gate)
        for prefix,v in [('direct',d),('annual',y),('l1',l),('blend',bl),('candidate',p),('f28',baseline[WINDOWS[seg][0]].to_numpy())]:generated[(prefix,seg)]=v
    for prefix in PREFIXES:generated[(prefix,'60')]=generated[(prefix,'M1')]+generated[(prefix,'M2')]
    if len(long)!=len(core)*len(PREFIXES)*3 or set(long.model)!=set(PREFIXES.values()):raise ValueError('FULL_MODEL_SCOPE')
    totals=[]
    for (prefix,seg),v in generated.items():
        exact(wide[prefix+'_'+seg],v,'WIDE:'+prefix+seg)
        win,a,b=WINDOWS[seg];z=long[long.model.eq(PREFIXES[prefix])&long.window.eq(win)].set_index(KEY).reindex(pd.MultiIndex.from_frame(core[KEY]))
        exact(z.prediction,v,'LONG:'+prefix+seg)
        for c,date in [('issue_date',conf['issue']),('input_cutoff',conf['cutoff']),('target_start',str((pd.Timestamp(conf['issue'])+pd.Timedelta(days=a)).date())),('target_end',str((pd.Timestamp(conf['issue'])+pd.Timedelta(days=b)).date()))]:
            if not pd.to_datetime(z[c]).eq(pd.Timestamp(date)).all():raise ValueError('LONG_DATE:'+c)
        if not np.array_equal(z.prediction_status.eq('OK'),np.isfinite(v)):raise ValueError('LONG_STATUS')
        full_history=sum(r['full_shop_units'] for r in coverage);eligible_history=float(amounts[np.isfinite(v)].sum())
        totals.append(dict(model=PREFIXES[prefix],window=win,core_n=len(core),output_n=int(np.isfinite(v).sum()),prediction_total=float(np.nansum(v)),object_coverage=float(np.isfinite(v).mean()),output_objects_history_units=eligible_history,output_history_sales_coverage=eligible_history/full_history))
    completed=dt.datetime.now(dt.timezone.utc).isoformat();before_target(conf['issue'],completed)
    for p,h in conf['sources'].items():
        if sha(p)!=h:raise ValueError('SOURCE_CHANGED_AT_END')
    out=dict(status='PASS',started_at=started,completed_at=completed,issue_date=conf['issue'],input_cutoff=conf['cutoff'],core_n=len(core),core_selection=coverage,formal_fits=fit_count,native_prediction_calls=native,reload_prediction_calls=reloads,training_rows_two_segments=training_rows,gate_true_n=int(gate.sum()),model_totals=totals,staging_manifest_sha256=sha(stage/'manifest.json'),prediction_sha256=sha(stage/'predictions.parquet'),release_manifest_sha256=conf['release_manifest_sha256'],validator_sha256=sha(__file__),validator_fits=0,validator_predictions=0,production_writes=0,future_actuals_read=False)
    write_new(target,out);print(json.dumps({k:out[k] for k in ['status','core_n','formal_fits','native_prediction_calls','reload_prediction_calls','gate_true_n']}));return out

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);a=p.parse_args();run(a.root)
