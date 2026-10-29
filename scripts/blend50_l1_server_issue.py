"""One isolated server shadow issue, at most six fits; no database/API clients."""
import argparse,contextlib,datetime,hashlib,importlib.metadata,json,os,pathlib,time
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
try:
    from scripts import blend50_prospective as p
    from scripts import direct_context90_experiment as context
except ModuleNotFoundError:
    import blend50_prospective as p
    import direct_context90_experiment as context

frozen,direct,annual=p.frozen,p.direct,p.annual
ROOT=pathlib.Path(__file__).resolve().parents[1]
SLOTS=('direct_M1','direct_M2','annual_M1','annual_M2','l1_M1','l1_M2')
MODELS=(('f28','F28'),('direct','DIRECT30x2'),('annual','YEARAGO_COMPONENT'),
        ('blend','DIRECT_YEARAGO_BLEND50'),('l1','DIRECT_L1_V1'),('candidate','BLEND50_L1_MEAN_V1'))
REQUIRED_CODE=('scripts/blend50_l1_server_issue.py','scripts/blend50_prospective.py',
 'scripts/bc60_monthly_replay.py','scripts/bc60_v3_direct.py','scripts/bc60_experiment.py',
 'scripts/direct_yearago_experiment.py','scripts/direct_context90_experiment.py',
 'scripts/yearago_gate_route.py','scripts/yearago_gate_score.py','scripts/direct_yearago_blend50.py')

def verify_release(manifest_path):
    manifest_path=pathlib.Path(manifest_path).resolve();release=p.read(manifest_path);base=manifest_path.parent
    if not release.get('code_commit'):raise ValueError('RELEASE_COMMIT_REQUIRED')
    for name in REQUIRED_CODE:
        if name not in release['files'] or p.sha(base/name)!=release['files'][name]:raise ValueError('RELEASE_FILE_CHANGED:'+name)
    if base!=ROOT:raise ValueError('RUNNING_CODE_NOT_RELEASE_DIRECTORY')
    pins={}
    for name,(commit,h) in p.PINS.items():
        if hashlib.sha256((base/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()!=h:raise ValueError('FROZEN_CODE_CHANGED:'+name)
        pins[name]={'commit':commit,'normalized_sha256':h,'file_sha256':p.sha(base/name)}
    return release,pins

def check_runtime(expected):
    actual={name:importlib.metadata.version(name) for name in expected}
    if actual!=expected:raise ValueError('RUNTIME_CHANGED')
    return actual

def phase_config(root):
    conf=p.read(root/'prepared/config.json');same_business_day(conf['issue'])
    if p.sha(__file__)!=conf['adapter_sha256']:raise ValueError('ADAPTER_CHANGED')
    if p.sha(conf['release_manifest'])!=conf['release_manifest_sha256']:raise ValueError('RELEASE_CHANGED')
    verify_release(conf['release_manifest']);check_runtime(conf['runtime']);p.verify_sources(conf['sources'])
    return conf

@contextlib.contextmanager
def exclusive(root,command):
    root=pathlib.Path(root).resolve();root.mkdir(parents=True,exist_ok=True);lock=root/'.operation.lock'
    with lock.open('x',encoding='utf-8') as f:json.dump({'pid':os.getpid(),'command':command,'at':p.clock()},f)
    try:yield
    finally:lock.unlink()

def model_parameters(original,prefix):
    params=dict(original)
    if prefix=='l1':params.update(objective='regression_l1',metric='l1')
    return params

def same_business_day(issue):
    today=datetime.datetime.now(datetime.timezone.utc).astimezone(ZoneInfo('Asia/Shanghai')).date()
    if str(today)!=str(issue):raise ValueError('ACTUAL_BUSINESS_DAY_CHANGED_NO_BACKDATE')

def label_check(train,cutoff):
    if not train.label_end.le(cutoff).all():raise ValueError('IMMATURE_TRAINING_LABEL')
    if not train.input_cutoff.eq(train.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('WRONG_ROW_CUTOFF')
    if not train.history_end.eq(train.input_cutoff).all():raise ValueError('FUTURE_FEATURE')

def prepare(root,input_root,monthly_input,frozen_config,release_manifest,parity_receipt,issue_date):
    root=pathlib.Path(root).resolve();input_root=pathlib.Path(input_root).resolve();same_business_day(issue_date)
    release,code=verify_release(release_manifest);parity=p.read(parity_receipt)
    if parity.get('status')!='PASS' or parity.get('release_manifest_sha256')!=p.sha(release_manifest):raise ValueError('SERVER_PARITY_REQUIRED')
    gate=p.read(input_root/'manifest.json')
    if gate['status']!='INPUT_READY_NOT_ISSUED' or gate['blockers']:raise ValueError('INPUT_GATE_NOT_READY')
    independent=p.read(input_root/'independent_validation.json')
    if independent['status']!='PASS' or independent.get('daily_sha256')!=gate['daily_sha256']:raise ValueError('INPUT_NOT_INDEPENDENTLY_VERIFIED')
    contract=p.time_contract(issue_date)
    if gate.get('issue_date')!=issue_date or gate.get('cutoff')!=contract['input_cutoff']:raise ValueError('INPUT_BATCH_DATE_CHANGED')
    dpath=input_root/'daily.parquet';mpath=pathlib.Path(monthly_input).resolve()
    if p.sha(dpath)!=gate['daily_sha256'] or p.sha(mpath)!=gate['monthly_sha256']:raise ValueError('INPUT_CHANGED')
    conf=p.read(frozen_config);runtime=check_runtime(conf['runtime'])
    frozen_config=pathlib.Path(frozen_config).resolve()
    rel=frozen_config.relative_to(ROOT).as_posix()
    if release['files'].get(rel)!=p.sha(frozen_config):raise ValueError('FROZEN_CONFIG_NOT_RELEASE_PINNED')
    out=root/'prepared';out.mkdir(exist_ok=False)
    issue=issue_date;cutoff=pd.Timestamp(contract['input_cutoff'])
    p.dump_new(root/'run_lock.json',dict(issue_date=issue,input_cutoff=contract['input_cutoff'],created_at=p.clock(),run_id=root.name))
    d=pd.read_parquet(dpath,columns=p.DAILY,filters=[('ds','<=',cutoff)])
    m=pd.read_parquet(mpath,filters=[('month','<=',str(direct.completed_month(cutoff)))]).copy()
    d,m=frozen.normalize(d,m)
    if d.ds.max()!=cutoff:raise ValueError('INPUT_CUTOFF_MISMATCH')
    universe,quality=frozen.freeze_issue(d,m,issue)
    if not quality.selection_status.eq('OK').all():raise ValueError('CORE_SELECTION_INCOMPLETE')
    core=universe[universe.is_core].copy().reset_index(drop=True);p.unique(core,p.KEY)
    universe.to_parquet(out/'universe.parquet',index=False);core.to_parquet(out/'core.parquet',index=False)
    core.to_csv(out/'core.csv',index=False,encoding='utf-8-sig');quality.to_csv(out/'selection_quality.csv',index=False,encoding='utf-8-sig')
    source_paths=[dpath,mpath,input_root/'manifest.json',input_root/'independent_validation.json',frozen_config,pathlib.Path(parity_receipt).resolve()]
    config={'run_id':root.name,'issue':issue,'cutoff':contract['input_cutoff'],'windows':contract,'runtime':runtime,
        'lightgbm':conf['original']['lightgbm'],'core_sha256':p.sha(out/'core.parquet'),
        'source_gate_sha256':p.sha(input_root/'manifest.json'),'frozen_code':code,'adapter_sha256':p.sha(__file__),
        'code_commit':release['code_commit'],'release_manifest':str(pathlib.Path(release_manifest).resolve()),'release_manifest_sha256':p.sha(release_manifest),
        'input_root':str(input_root),'input_manifest':str(input_root/'manifest.json'),'sources':{str(x):p.sha(x) for x in source_paths},
        'parity_receipt':str(pathlib.Path(parity_receipt).resolve()),'parity_receipt_sha256':p.sha(parity_receipt),
        'context_diagnostic_sha256':p.sha(ROOT/'scripts/direct_context90_experiment.py'),
        'formal_fit_slots':list(SLOTS),'maximum_fits':6,'prepared_started':p.clock(),'future_truth_read':False,
        'description':'影子试运行／人工参考，未替代生产，不自动下单'}
    p.dump_new(out/'config.json',config)
    # Core list/config are frozen before any model calculation.
    ctx=frozen.VectorContext(d,m,p.SHOPS,cutoff);index=annual.DailyIndex(d)
    decline=[]
    for r in core.itertuples():
        q=context.features_at(ctx,(r.shop,r.spu),pd.DatetimeIndex([cutoff])).iloc[0]
        complete=bool(q.history90_complete)
        flag=complete and bool(q.early34>q.middle28>q.recent28>0)
        decline.append({'shop':r.shop,'spu':r.spu,'history90_complete':complete,
                        'early34':q.early34,'middle28':q.middle28,'recent28':q.recent28,
                        'decline_group':'DECLINE' if flag else 'OTHER' if complete else 'UNKNOWN'})
    pd.DataFrame(decline).to_parquet(out/'decline.parquet',index=False)
    baseline=pd.DataFrame(frozen.baseline_values(d,issue,core));baseline=baseline[baseline.model.eq('F28')]
    baseline.to_parquet(out/'f28.parquet',index=False)
    segments={}
    for seg in ['M1','M2']:
        dest=out/seg;dest.mkdir(exist_ok=False)
        train,excluded=ctx.training(issue,seg);label_check(train,cutoff)
        inf=pd.DataFrame([ctx.row((r.shop,r.spu),issue,seg,issue) for r in core.itertuples()])
        train.to_parquet(dest/'direct_train.parquet',index=False)
        inf.to_parquet(dest/'direct_inference.parquet',index=False)
        excluded.to_csv(dest/'training_exclusions.csv',index=False)
        full,wa=annual.augment(index,train,seg);extended,wi=annual.augment(index,inf,seg)
        pd.testing.assert_frame_equal(full[train.columns],train,check_exact=True)
        full.to_parquet(dest/'annual_train.parquet',index=False);extended.to_parquet(dest/'annual_inference.parquet',index=False)
        wa.to_parquet(dest/'training_annual_windows.parquet',index=False);wi.to_parquet(dest/'inference_annual_windows.parquet',index=False)
        support=annual.support(full);direct_status=direct.support_status(train)
        audit=frozen.training_audit(train,['shop','spu','simulated_issue_date'],['shop','spu','simulated_issue_date','input_cutoff','label_start','label_end','y_label',*direct.FEATURES])
        p.dump_new(dest/'training_audit.json',audit)
        segments[seg]={'training_rows':len(train),'training_objects':len(train[['shop','spu']].drop_duplicates()),
            'latest_label_end':str(train.label_end.max()),'direct_support':direct_status,'annual_support':support,
            'original_features_labels_keys_equal':True,'per_row_cutoff_correct':True,
            'files':{f.name:p.sha(f) for f in dest.iterdir() if f.is_file()}}
        p.dump_new(dest/'manifest.json',segments[seg]);print(json.dumps({'prepared_segment':seg,'training_rows':len(train),'annual_supported':support['fit_allowed']}),flush=True)
    p.dump_new(out/'manifest.json',{'status':'PREPARED_NOT_FITTED','at':p.clock(),'segments':segments,
        'files':{str(f.relative_to(out)):p.sha(f) for f in out.rglob('*') if f.is_file()}})
    print(json.dumps({'prepared':'PASS','core_records':len(core),'shops':quality.to_dict('records')},default=str),flush=True)

def fit(root,slot):
    from lightgbm import LGBMRegressor,Booster
    root=pathlib.Path(root).resolve();prep=root/'prepared';conf=phase_config(root)
    if slot not in SLOTS:raise ValueError('FIT_SLOT_NOT_ALLOWED')
    prefix,seg=slot.split('_');meta=p.read(prep/seg/'manifest.json')
    for name,h in meta['files'].items():
        if p.sha(prep/seg/name)!=h:raise ValueError('PREPARED_CHANGED')
    out=root/'effects'/slot
    if out.exists():
        if (out/'result.json').exists():
            result=p.read(out/'result.json');verify_result(out,result)
            print(json.dumps({'slot':slot,'status':'REUSED_COMPLETE_SLOT','fit_attempted_this_call':False}));return
        raise ValueError('INCOMPLETE_SLOT_NO_AUTOMATIC_REFIT:'+slot)
    out.mkdir(parents=True,exist_ok=False)
    allowed=not meta['direct_support'].startswith('NOT_TRAINABLE') if prefix in ['direct','l1'] else bool(meta['annual_support']['fit_allowed'])
    source='direct' if prefix=='l1' else prefix
    features=direct.FEATURES if source=='direct' else annual.FEATURES
    train_path=prep/seg/(source+'_train.parquet');inf_path=prep/seg/(source+'_inference.parquet')
    inf=pd.read_parquet(inf_path);raw=np.full(len(inf),np.nan);values=raw.copy();reloaded=raw.copy()
    params=model_parameters(conf['lightgbm'],prefix);started=p.clock();seconds=0.;model_hash=None;native_calls=reload_calls=0
    receipt={'slot':slot,'training_sha256':p.sha(train_path),'inference_sha256':p.sha(inf_path),
        'features':features,'feature_dtypes':{f:str(inf[f].dtype) for f in features},'parameters':params,
        'sample_weight':'NOT_PASSED','init_score':'NOT_PASSED','input_manifest_sha256':conf['source_gate_sha256'],
        'config_sha256':p.sha(prep/'config.json'),'release_manifest_sha256':conf['release_manifest_sha256']}
    if allowed:
        attempts=list((root/'effects').glob('*/fit_started.json'))
        if len(attempts)>=6:raise ValueError('FIT_BUDGET')
        train=pd.read_parquet(train_path);label_check(train,pd.Timestamp(conf['cutoff']))
        receipt['training_feature_dtypes']={f:str(train[f].dtype) for f in features}
        p.dump_new(out/'fit_started.json',dict(receipt,at=p.clock(),formal_fit_number=len(attempts)+1))
        model=LGBMRegressor(**params);begin=time.monotonic();model.fit(train[features],train.y_label);seconds=time.monotonic()-begin
        model_path=str(out/'model.txt');model_path.encode('ascii');model.booster_.save_model(model_path)
        model_hash=p.sha(out/'model.txt')
        p.dump_new(out/'checkpoint.json',dict(receipt,at=p.clock(),model_sha256=model_hash,fit_seconds=seconds))
        valid=inf.input_status.eq('OK')
        if valid.any():
            p.dump_new(out/'native_started.json',{'at':p.clock(),'rows':int(valid.sum()),'slot':slot})
            raw[valid]=model.predict(inf.loc[valid,features]);native_calls=1
            if not np.isfinite(raw[valid]).all():raise ValueError('NONFINITE_NATIVE_OUTPUT')
        pd.DataFrame({'shop':inf.shop,'spu':inf.spu,'prediction_raw':raw}).to_parquet(out/'native_predictions.parquet',index=False)
        loaded=Booster(model_str=(out/'model.txt').read_text(encoding='utf-8'))
        if loaded.feature_name()!=features:raise ValueError('RELOADED_FEATURE_ORDER')
        if valid.any():
            p.dump_new(out/'reload_started.json',{'at':p.clock(),'rows':int(valid.sum()),'slot':slot})
            reloaded[valid]=loaded.predict(inf.loc[valid,features],num_threads=params.get('n_jobs',2),validate_features=True);reload_calls=1
            if not np.isfinite(reloaded[valid]).all():raise ValueError('NONFINITE_RELOAD_OUTPUT')
        pd.DataFrame({'shop':inf.shop,'spu':inf.spu,'prediction_raw':reloaded}).to_parquet(out/'reload_predictions.parquet',index=False)
        if not np.array_equal(raw,reloaded,equal_nan=True):raise ValueError('SAVE_RELOAD_NOT_EXACT')
        values=np.maximum(raw,0)
    result=inf[['shop','spu','input_status']].copy();result['prediction_raw']=raw;result['prediction']=values
    result['output_status']=np.where(np.isfinite(values),'OK',inf.input_status if allowed else 'MODEL_SUPPORT_INSUFFICIENT')
    result.to_parquet(out/'predictions.parquet',index=False)
    payload=dict(receipt,slot=slot,started=started,finished=p.clock(),fit_attempted=bool(allowed),
        fit_seconds=seconds,output_n=int(np.isfinite(values).sum()),status='FITTED' if allowed else 'SKIPPED_SUPPORT',
        support_reason=meta['direct_support'] if source=='direct' else meta['annual_support'],
        model_sha256=model_hash,prediction_sha256=p.sha(out/'predictions.parquet'),
        native_prediction_calls=native_calls,reload_prediction_calls=reload_calls,save_reload_exact=True if allowed else None,
        negative_raw_n=int((raw<0).sum()),negative_raw_units=float(-np.minimum(raw[np.isfinite(raw)],0).sum()),
        files={f.name:p.sha(f) for f in out.iterdir() if f.is_file()})
    p.dump_new(out/'result.json',payload)
    print(json.dumps({'slot':slot,'status':payload['status'],'fit_attempted':bool(allowed),'fit_seconds':seconds,'output_n':payload['output_n']}),flush=True)

def verify_result(slot,result):
    if result['status'] not in ['FITTED','SKIPPED_SUPPORT']:raise ValueError('SLOT_NOT_COMPLETE')
    for name,h in result['files'].items():
        if p.sha(slot/name)!=h:raise ValueError('SLOT_ARTIFACT_CHANGED:'+name)
    if p.sha(slot/'predictions.parquet')!=result['prediction_sha256']:raise ValueError('PREDICTION_CHANGED')
    if result['status']=='FITTED' and (not result['fit_attempted'] or result.get('save_reload_exact') is not True):raise ValueError('FIT_PARITY_NOT_COMPLETE')
    if result['status']=='SKIPPED_SUPPORT' and result['fit_attempted']:raise ValueError('INVALID_SKIP')

def segment_reason(prefix,seg,i,statuses,wide):
    segments=['M1','M2'] if seg=='60' else [seg]
    if prefix=='candidate':
        result=[]
        for q in segments:
            for endpoint in ['blend','l1']:
                if not np.isfinite(wide.iloc[i][endpoint+'_'+q]):result.append('MISSING_'+endpoint.upper()+':'+segment_reason(endpoint,q,i,statuses,wide))
        return '|'.join(dict.fromkeys(result)) or 'OK'
    if prefix=='blend':prefix='direct'
    result=[str(statuses[prefix+'_'+q][i]) for q in segments]
    bad=[x for x in result if x!='OK']
    return '|'.join(dict.fromkeys(bad)) or 'OK'

def combine_l1(wide,l1_predictions):
    out=wide.copy()
    for seg in ['M1','M2']:
        values=np.asarray(l1_predictions[seg],float)
        if len(values)!=len(out) or np.isinf(values).any() or (values[np.isfinite(values)]<0).any():raise ValueError('INVALID_L1_ENDPOINT')
        out['l1_'+seg]=values
        out['candidate_'+seg]=.5*out['blend_'+seg]+.5*out['l1_'+seg]
    out['l1_60']=out.l1_M1+out.l1_M2;out['candidate_60']=out.candidate_M1+out.candidate_M2
    return out

def assemble(root):
    root=pathlib.Path(root).resolve();prep=root/'prepared';conf=phase_config(root)
    core=pd.read_parquet(prep/'core.parquet')
    if p.sha(prep/'core.parquet')!=conf['core_sha256']:raise ValueError('CORE_CHANGED')
    preds={};statuses={};support={};mats={};results=[]
    for seg in ['M1','M2']:
        meta=p.read(prep/seg/'manifest.json');support[seg]=meta['annual_support']
        for name,h in meta['files'].items():
            if p.sha(prep/seg/name)!=h:raise ValueError('PREPARED_CHANGED')
        mats[seg]=(pd.read_parquet(prep/seg/'annual_inference.parquet'),pd.read_parquet(prep/seg/'inference_annual_windows.parquet'))
        for prefix in ['direct','annual','l1']:
            slot=root/'effects'/(prefix+'_'+seg);r=p.read(slot/'result.json');verify_result(slot,r)
            allowed=bool(meta['annual_support']['fit_allowed']) if prefix=='annual' else not meta['direct_support'].startswith('NOT_TRAINABLE')
            if r['fit_attempted']!=allowed or r['status']!=('FITTED' if allowed else 'SKIPPED_SUPPORT'):raise ValueError('TECHNICAL_FAILURE_CANNOT_BE_SUPPORT_FALLBACK')
            if r['config_sha256']!=p.sha(prep/'config.json'):raise ValueError('SLOT_CONFIG_CHANGED')
            df=pd.read_parquet(slot/'predictions.parquet');pd.testing.assert_frame_equal(df[['shop','spu']],core[['shop','spu']],check_dtype=False)
            preds[prefix+'_'+seg]=df.prediction.to_numpy();statuses[prefix+'_'+seg]=df.output_status.to_numpy();results.append(r)
    wide=p.assemble(core,conf['issue'],mats,preds,support)
    wide=combine_l1(wide,{s:preds['l1_'+s] for s in ['M1','M2']})
    baseline=pd.read_parquet(prep/'f28.parquet');values=baseline.pivot(index=['shop','spu'],columns='window',values='prediction')
    bs=baseline.pivot(index=['shop','spu'],columns='window',values='prediction_status');idx=pd.MultiIndex.from_frame(core[['shop','spu']])
    for seg,win in [('M1','d01_30'),('M2','d31_60')]:
        wide['f28_'+seg]=values.loc[idx,win].to_numpy();statuses['f28_'+seg]=bs.loc[idx,win].to_numpy()
    wide['f28_60']=wide.f28_M1+wide.f28_M2
    for key,val in statuses.items():wide[key+'_status']=val
    rows=[]
    for prefix,name in MODELS:
        for seg,win in [('M1','d01_30'),('M2','d31_60'),('60','d01_60')]:
            a,b=direct.WINDOWS[win]
            for i,r in enumerate(core.itertuples()):
                value=wide.iloc[i][prefix+'_'+seg]
                if np.isinf(value) or (np.isfinite(value) and value<0):raise ValueError('INVALID_FINAL_VALUE')
                reason='OK' if np.isfinite(value) else segment_reason(prefix,seg,i,statuses,wide)
                if not np.isfinite(value) and reason=='OK':raise ValueError('UNEXPLAINED_MISSING_OUTPUT')
                rows.append({'run_id':conf['run_id'],'issue_date':conf['issue'],'input_cutoff':conf['cutoff'],'shop':r.shop,'spu':r.spu,
                    'model':name,'window':win,'target_start':str((pd.Timestamp(conf['issue'])+pd.Timedelta(days=a)).date()),
                    'target_end':str((pd.Timestamp(conf['issue'])+pd.Timedelta(days=b)).date()),'prediction':value,'prediction_status':reason})
    out=root/'staging';out.mkdir(exist_ok=False)
    wide.to_parquet(out/'routing_predictions.parquet',index=False);wide.to_csv(out/'routing_predictions.csv',index=False,encoding='utf-8-sig')
    long=pd.DataFrame(rows);p.unique(long,p.KEY+['model','window'])
    if len(long)!=len(core)*len(MODELS)*3:raise ValueError('INCOMPLETE_CORE_OUTPUT_STATUS')
    long.to_parquet(out/'predictions.parquet',index=False);long.to_csv(out/'predictions.csv',index=False,encoding='utf-8-sig')
    fitted=sum(r['fit_attempted'] for r in results)
    if fitted>6 or len(results)!=6:raise ValueError('FIT_BUDGET_OR_MISSING_SLOT')
    p.dump_new(out/'calculation_receipt.json',{'state':'CALCULATED_NOT_SEALED','at':p.clock(),'run_id':conf['run_id'],'core_n':len(core),
        'formal_fits':fitted,'slots':results,'gate_true':int(wide.gate_decision.sum()),
        'native_prediction_calls':sum(r['native_prediction_calls'] for r in results),
        'reload_prediction_calls':sum(r['reload_prediction_calls'] for r in results),
        'config_sha256':p.sha(prep/'config.json'),'prediction_sha256':p.sha(out/'predictions.parquet'),
        'future_truth_read':False,'production_writes':0,'api_calls':0})
    p.dump_new(out/'manifest.json',{'status':'STAGED_NOT_ISSUED','at':p.clock(),'run_id':conf['run_id'],
        'files':{f.name:p.sha(f) for f in out.iterdir() if f.is_file()}})
    print(json.dumps({'calculated':True,'core_n':len(core),'gate_true':int(wide.gate_decision.sum())}),flush=True)

def verify_sealed(root):
    receipt=p.read(root/'issued/issue_seal.json')
    if receipt['status']!='ISSUED_SHADOW_AWAITING_ACTUALS':raise ValueError('NOT_SEALED')
    for name,h in receipt['files'].items():
        if p.sha(root/name)!=h:raise ValueError('SEALED_ARTIFACT_CHANGED:'+name)
    p.verify_sources(receipt['external_sources'])
    return receipt

def seal(root):
    root=pathlib.Path(root).resolve()
    if (root/'issued/issue_seal.json').exists():
        verify_sealed(root);print(json.dumps({'status':'ALREADY_ISSUED_VERIFIED'}));return
    conf=phase_config(root);stage=root/'staging'
    v=p.read(stage/'independent_validation.json')
    if v.get('status')!='PASS' or v.get('staging_manifest_sha256')!=p.sha(stage/'manifest.json') or v.get('prediction_sha256')!=p.sha(stage/'predictions.parquet'):raise ValueError('INDEPENDENT_CHECK_REQUIRED')
    for name,h in p.read(stage/'manifest.json')['files'].items():
        if p.sha(stage/name)!=h:raise ValueError('STAGING_CHANGED')
    at=p.clock();p.verify_seal_time(conf['issue'],at['utc'])
    if (root/'issued').exists():raise ValueError('ISSUED_DESTINATION_ALREADY_EXISTS')
    files={str(f.relative_to(root)):p.sha(f) for folder in ['prepared','effects'] for f in (root/folder).rglob('*') if f.is_file()}
    files.update({'issued/'+f.name:p.sha(f) for f in stage.iterdir() if f.is_file()})
    receipt={'status':'ISSUED_SHADOW_AWAITING_ACTUALS','run_id':conf['run_id'],'sealed_at':at,
        'issue_date':conf['issue'],'input_cutoff':conf['cutoff'],'windows':conf['windows'],'files':files,
        'source_manifest_sha256':conf['source_gate_sha256'],'code_commit':conf['code_commit'],
        'release_manifest_sha256':conf['release_manifest_sha256'],'adapter_sha256':conf['adapter_sha256'],
        'external_sources':conf['sources'],'description':conf['description'],
        'future_actuals_available':False,'current_accuracy_computed':False,'production_writes':0,'api_calls':0}
    p.dump_new(stage/'issue_seal.json',receipt)
    stage.rename(root/'issued')
    verify_sealed(root)
    print(json.dumps({'status':receipt['status'],'sealed_at':at,'issue_date':conf['issue']},ensure_ascii=False))

def main():
    q=argparse.ArgumentParser();q.add_argument('command',choices=['prepare','fit','assemble','seal','verify'])
    q.add_argument('--root',required=True);q.add_argument('--input-root');q.add_argument('--monthly-input')
    q.add_argument('--frozen-config');q.add_argument('--release-manifest');q.add_argument('--parity-receipt');q.add_argument('--issue-date')
    q.add_argument('--slot',choices=SLOTS);a=q.parse_args();root=pathlib.Path(a.root).resolve()
    if (root/'issued/issue_seal.json').exists():
        verify_sealed(root);print(json.dumps({'status':'ALREADY_ISSUED_VERIFIED','run_id':root.name}));return
    if a.command=='verify':raise ValueError('NOT_ISSUED')
    with exclusive(root,a.command):
        try:
            if a.command=='prepare':
                required=[a.input_root,a.monthly_input,a.frozen_config,a.release_manifest,a.parity_receipt,a.issue_date]
                if not all(required):raise ValueError('EXPLICIT_PREPARE_PATHS_REQUIRED')
                prepare(root,*required)
            elif a.command=='fit':fit(root,a.slot)
            elif a.command=='assemble':assemble(root)
            else:seal(root)
        except Exception as exc:
            path=root/('failure_'+str(time.time_ns())+'.json')
            p.dump_new(path,{'state':'RUN_FAILED_NOT_ISSUED','at':p.clock(),'phase':a.command,'slot':a.slot,'error_type':type(exc).__name__,'error':str(exc)})
            raise

if __name__=='__main__':main()
