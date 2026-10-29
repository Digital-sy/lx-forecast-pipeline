"""Fixed unweighted native-L1 objective contrast on immutable local archives."""
from __future__ import annotations
import argparse, hashlib, importlib.metadata, json, os, re, subprocess, sys, time
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts import direct_training_alignment as audit
except ModuleNotFoundError:
    import direct_training_alignment as audit

ROOT=Path(__file__).resolve().parents[1]
CANDIDATE='DIRECT_L1_V1'
PREREG='f44166be56416bb4a910ffac2b73600f58ba7f4f'
FEATURES=['shop_code','lag1','lag7','lag14','lag28','rolling_mean_lag1_window_size7',
          'rolling_mean_lag1_window_size28','dayofweek','month','dayofyear']
sha,read,dump,now=audit.sha,audit.read,audit.dump,audit.now

def array_sha(x):return hashlib.sha256(np.ascontiguousarray(x).tobytes()).hexdigest()

def l1_configuration(original):
    original=dict(original)
    forbidden={'objective_type','app','application','loss','metrics','metric_types',
        'sample_weight','class_weight','weight','weight_column','init_score','init_model'}
    if forbidden.intersection(original):raise ValueError('AMBIGUOUS_OR_HIDDEN_CONFIG')
    if original.get('objective')!='poisson':raise ValueError('NOT_FROZEN_POISSON_BASE')
    new=dict(original);new['objective']='regression_l1';new['metric']='l1'
    removed={}
    if 'poisson_max_delta_step' in new:removed['poisson_max_delta_step']=new.pop('poisson_max_delta_step')
    changes={k:dict(old=original.get(k),new=new.get(k)) for k in original.keys()|new.keys() if original.get(k)!=new.get(k)}
    if set(changes)-{'objective','metric','poisson_max_delta_step'}:raise ValueError('EXTRA_CONFIG_CHANGE')
    return new,dict(changes=changes,explicit_poisson_parameters_removed=removed,
        inactive_default=dict(poisson_max_delta_step=0.7),sample_weight='NOT_PASSED_IMPLICIT_EQUAL',
        initialization='NATIVE_L1_MEDIAN_NO_MANUAL_INIT_SCORE',link='NATIVE_L1_IDENTITY',leaf_update='NATIVE_L1_RESIDUAL_MEDIAN')

def fit_forward(estimator,X,y,path):
    if list(X.columns)!=FEATURES or len(X)!=len(y):raise ValueError('FIT_INPUT_CONTRACT')
    receipt=dict(at=now(),stage='RECEIVED_AT_FIT_INTERFACE',rows=len(X),features=list(X),
        feature_dtypes={c:str(X[c].dtype) for c in X},label_dtype=str(y.dtype),
        X_column_sha256={c:array_sha(X[c].to_numpy()) for c in X},y_sha256=array_sha(y.to_numpy()),
        fit_kwargs=[],no_sample_weight=True,no_class_weight=True,no_eval_set=True,no_init_score=True,
        label_transform='NONE',zero_label_rows=int(y.eq(0).sum()))
    dump(path,receipt)
    result=estimator.fit(X,y)
    receipt.update(stage='FIT_RETURNED',completed_at=now());dump(path,receipt)
    return result

def verify(files):
    for p,h in files.items():
        if sha(p)!=h:raise ValueError('SOURCE_CHANGED:'+str(p))

def model_metadata(text):
    params=dict(re.findall(r'^\[([^:]+): (.*?)\]$',text,re.M))
    objective=re.search(r'^objective=(.+)$',text,re.M)
    if not objective or objective.group(1)!='regression_l1' or params.get('objective')!='regression_l1':raise ValueError('SAVED_OBJECTIVE_NOT_L1')
    if params.get('metric')!='l1':raise ValueError('SAVED_METRIC_NOT_L1')
    return dict(objective=objective.group(1),params=params,
        poisson_max_delta_step_in_dump=params.get('poisson_max_delta_step'),poisson_parameter_effect='INACTIVE_FOR_L1')

def prepare(root,out,tests):
    root,out,tests=[Path(p).resolve() for p in [root,out,tests]]
    if out.exists() or root not in out.parents:raise ValueError('EXCLUSIVE_NEW_OUTPUT_REQUIRED')
    archive=root/'bc60_monthly_replay_20261020';cw=root/'direct_core_weight2_v1_20261028';comp=root/'core_model_complementarity_20261028_v1'
    sources=audit.Sources();fm=read(sources.add(archive/'prepared/frozen_config.json'))
    runtime={k:importlib.metadata.version(k) for k in fm['runtime']}
    if runtime!=fm['runtime'] or runtime['lightgbm']!='4.6.0':raise ValueError('PINNED_RUNTIME_CHANGED')
    cm=read(sources.add(cw/'delivery_receipt.json'))
    if cm['commit']!='3dd0b53c4b1a137c57d7c450e29b7293ab3799f9':raise ValueError('WRONG_WEIGHT_ARCHIVE')
    def cwfile(rel):return sources.add(cw/rel,cm['files'][rel])
    old=read(sources.add(archive/'effects/run_manifest.json'));oldfiles={p.replace('\\','/'):h for p,h in old['files'].items()}
    compm=read(sources.add(comp/'manifest.json'))
    for p,h in compm['protected_shadow_files'].items():sources.add(p,h)
    for name in ['objects.parquet','historical_windows.parquet','top20_products.csv','all_miss_directions_detail.csv']:
        sources.add(comp/name,compm['files'][name])
    sources.add(ROOT/'docs/direct_l1_v1_protocol.md');sources.add(ROOT/'scripts/direct_training_alignment.py')
    tr=read(sources.add(tests))
    if tr['status']!='PASS':raise ValueError('PREFLIGHT_NOT_PASS')
    sources.add(root/'direct_training_alignment_20261028_final/delivery_receipt.json')
    sources.add(root/'direct_yearago_blend50_20261021_v1/scoring/manifest.json')
    sources.add(archive/'inputs/daily.parquet','03596126d5543adc24325de12f03d4899c372e7076b01339e19656ce289bfc82')
    sources.add(archive/'inputs/monthly_reference.parquet','423ce1086fc885d7d41e087e95b048638600710c1d174d3d18a9a3f1a98228af')
    cwfile('auxiliary_prepared/manifest.json');cwfile('auxiliary_prepared/pool.parquet');cwfile('auxiliary_risk/manifest.json')
    grid=pd.read_csv(sources.add(archive/'prepared/issue_grid.csv'));grid=grid[grid.calendar_mature.eq(True)]
    core=pd.read_parquet(sources.add(archive/'prepared/core.parquet'));core=core[core.issue_date.isin(grid.issue_date)]
    if len(core)!=2820 or len(grid)!=20:raise ValueError('FIXED_SCOPE_CHANGED')
    config,delta=l1_configuration(fm['original']['lightgbm'])
    out.mkdir();(out/'prepared').mkdir();started=now()
    grid.to_csv(out/'prepared/issue_grid.csv',index=False);core.to_parquet(out/'prepared/core.parquet',index=False)
    conf=dict(candidate=CANDIDATE,preregistration_commit=PREREG,root=str(root),archive=str(archive),features=FEATURES,
        original_lightgbm=fm['original']['lightgbm'],lightgbm=config,parameter_audit=delta,sample_weight=None,fit_kwargs=[],
        runtime=runtime,max_formal_fits=40,max_seconds_per_slot=600,classification='RECONSTRUCTED_HISTORY_NOT_OOS',
        script_sha256=sha(__file__),test_receipt_sha256=sha(tests),source_model_commit='34bdc3b703c45326f3c638814ad18595c8124e83')
    dump(out/'configuration.json',conf);checks=[];slots=[]
    for issue in grid.issue_date:
        for seg in ['M1','M2']:
            name=f'{issue}_{seg}';dest=out/'prepared'/name;dest.mkdir();prior=read(cwfile(f'prepared/{name}/training_check.json'))
            trainpath=sources.add(prior['train_path'],prior['train_sha256']);train=pd.read_parquet(trainpath).drop(columns='item_key')
            oldslot=archive/'effects/slots'/name
            auditpath=sources.add(oldslot/'training_audit.json',oldfiles[f'slots/{name}/training_audit.json']);oa=read(auditpath)
            audit.original_audit(train,oa)
            checkpoint=read(sources.add(oldslot/'checkpoint.json',oldfiles[f'slots/{name}/checkpoint.json']))
            if prior['old_frame_hash']!=checkpoint['training_sha256']:raise ValueError('ORIGINAL_MATRIX_PROVENANCE')
            if not train.input_cutoff.eq(train.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('OWN_CUTOFF')
            if not train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all():raise ValueError('IMMATURE_LABEL')
            if list(train[FEATURES])!=FEATURES or train.duplicated(['shop','spu','simulated_issue_date']).any():raise ValueError('TRAIN_KEYS')
            infpath=cwfile(f'prepared/{name}/inference.parquet');inf=pd.read_parquet(infpath)
            inf.to_parquet(dest/'inference.parquet',index=False)
            info=dict(status='PASS',issue_date=issue,segment=seg,train_path=str(trainpath),train_sha256=sha(trainpath),
                old_training_audit_path=str(auditpath),old_frame_hash=checkpoint['training_sha256'],rows=len(train),
                old_features_exact=True,old_labels_keys_dtypes_exact=True,training_order_sha256=hashlib.sha256(pd.util.hash_pandas_object(train[['shop','spu','simulated_issue_date']],index=False).to_numpy().tobytes()).hexdigest(),
                inference_rows=len(inf),inference_sha256=sha(dest/'inference.parquet'),inference_source_path=str(infpath),
                zero_labels_preserved=int(train.y_label.eq(0).sum()),labels_mature=True,feature_columns=FEATURES,
                feature_dtypes={c:str(train[c].dtype) for c in FEATURES},sample_weight=None,input_cutoff=str((pd.Timestamp(issue)-pd.Timedelta(days=2)).date()))
            dump(dest/'training_check.json',info);checks.append(info);slots.append(dict(issue_date=issue,segment=seg,state='PLANNED'))
            print(name,len(train),flush=True)
    pd.DataFrame(checks).to_csv(out/'training_reconciliation.csv',index=False,encoding='utf-8-sig')
    sources.verify();dump(out/'prepare_manifest.json',dict(started_at=started,completed_at=now(),sources=sources.files,
        formal_fit_count=0,preregistration_commit=PREREG,script_sha256=sha(__file__),files={str(p.relative_to(out)).replace('\\','/'):sha(p) for p in out.rglob('*') if p.is_file()}))
    dump(out/'formal_fit_ledger.json',dict(max_formal_fits=40,created_at=now(),slots=slots))

def prediction_call(booster,X,path,kind):
    with path.with_name(kind+'_started.json').open('x',encoding='utf-8') as f:json.dump(dict(at=now(),kind=kind,rows=len(X)),f)
    result=booster.predict(X)
    dump(path,dict(kind=kind,completed_at=now(),rows=len(X),prediction_sha256=array_sha(result)))
    return result

def worker(out,issue,segment,recovery=False):
    from lightgbm import LGBMRegressor,Booster
    out=Path(out).resolve();conf=read(out/'configuration.json');folder=out/'prepared'/f'{issue}_{segment}';slot=out/'effects'/folder.name
    slot.mkdir(parents=True,exist_ok=True);check=read(folder/'training_check.json')
    if conf['script_sha256']!=sha(__file__) or conf['features']!=FEATURES or conf['sample_weight'] is not None:raise ValueError('FROZEN_CONFIG_CHANGED')
    params,_=l1_configuration(conf['original_lightgbm'])
    if conf['lightgbm']!=params:raise ValueError('PARAMETER_OVERRIDE')
    verify({check['train_path']:check['train_sha256'],str(folder/'inference.parquet'):check['inference_sha256']})
    train=pd.read_parquet(check['train_path']).drop(columns='item_key');inf=pd.read_parquet(folder/'inference.parquet');ok=inf.input_status.eq('OK');cp=slot/'checkpoint.json'
    if recovery:
        saved=read(cp);verify({str(slot/'model.txt'):saved['model_sha256'],str(slot/'native_predictions.parquet'):saved['native_predictions_sha256']})
        pred=pd.read_parquet(slot/'native_predictions.parquet');seconds=saved['fit_seconds']
    else:
        with (slot/'fit_started.json').open('x',encoding='utf-8') as f:json.dump(dict(at=now(),issue_date=issue,segment=segment,objective='regression_l1'),f)
        model=LGBMRegressor(**params);tic=time.monotonic();fit_forward(model,train[FEATURES],train.y_label,slot/'fit_received.json');seconds=time.monotonic()-tic
        booster=model.booster_;booster.save_model(os.path.relpath(slot/'model.txt',ROOT))
        if booster.feature_name()!=FEATURES:raise ValueError('FEATURES_CHANGED')
        metadata=model_metadata((slot/'model.txt').read_text(encoding='utf-8'))
        pred=inf.copy();pred['raw_prediction']=np.nan
        pred.loc[ok,'raw_prediction']=prediction_call(booster,pred.loc[ok,FEATURES],slot/'native_prediction_receipt.json','native')
        pred.to_parquet(slot/'native_predictions.parquet',index=False)
        dump(cp,dict(at=now(),model_sha256=sha(slot/'model.txt'),fit_seconds=seconds,params=params,features=FEATURES,
            train_sha256=check['train_sha256'],fit_received_sha256=sha(slot/'fit_received.json'),metadata=metadata,
            native_predictions_sha256=sha(slot/'native_predictions.parquet'),old_features_unchanged=True))
    loaded=Booster(model_file=os.path.relpath(slot/'model.txt',ROOT));pred['raw_prediction_reloaded']=np.nan
    pred.loc[ok,'raw_prediction_reloaded']=prediction_call(loaded,pred.loc[ok,FEATURES],slot/'reload_prediction_receipt.json','reload')
    if not np.array_equal(pred.raw_prediction.to_numpy(),pred.raw_prediction_reloaded.to_numpy(),equal_nan=True):raise ValueError('SAVE_RELOAD_CHANGED')
    if not np.isfinite(pred.loc[ok,'raw_prediction']).all():raise ValueError('NONFINITE_OUTPUT')
    pred['prediction']=np.maximum(pred.raw_prediction,0);pred['prediction_status']=pred.input_status
    pred['prediction_origin']=np.where(ok,'NEW_L1_MODEL','NO_OUTPUT');pred.to_parquet(slot/'predictions.parquet',index=False)
    dump(slot/'result.json',dict(status='FITTED',issue_date=issue,segment=segment,fit_seconds=seconds,completed_at=now(),
        n=len(pred),output_n=int(ok.sum()),live_prediction_calls=1,reload_prediction_calls=1,prediction_calls=2,
        save_reload_exact=True,negative_raw_n=int(pred.raw_prediction.lt(0).sum()),negative_raw_units=float(-pred.loc[pred.raw_prediction.lt(0),'raw_prediction'].sum()),
        model_sha256=sha(slot/'model.txt'),prediction_sha256=sha(slot/'predictions.parquet')))

def fit(out):
    out=Path(out).resolve();conf=read(out/'configuration.json');prep=read(out/'prepare_manifest.json');started=now()
    if conf['script_sha256']!=sha(__file__):raise ValueError('CODE_CHANGED')
    if {k:importlib.metadata.version(k) for k in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    verify(prep['sources']);verify({str(out/p):h for p,h in prep['files'].items()})
    ledgerpath=out/'formal_fit_ledger.json';ledger=read(ledgerpath)
    for entry in ledger['slots']:
        issue,seg=entry['issue_date'],entry['segment'];slot=out/'effects'/f'{issue}_{seg}'
        if (slot/'result.json').exists():
            r=read(slot/'result.json');verify({str(slot/'model.txt'):r['model_sha256'],str(slot/'predictions.parquet'):r['prediction_sha256']});continue
        recovery=(slot/'checkpoint.json').exists()
        if (slot/'fit_started.json').exists() and not recovery:raise ValueError('REFIT_FORBIDDEN')
        if len(list((out/'effects').glob('*/fit_started.json')))>=40 and not recovery:raise ValueError('FIT_BUDGET')
        entry.update(state='RUNNING',reserved_at=now());dump(ledgerpath,ledger)
        args=[sys.executable,'-B',str(Path(__file__).resolve()),'_worker','--out',str(out),'--issue',issue,'--segment',seg]
        if recovery:args.append('--recovery')
        proc=subprocess.run(args,cwd=ROOT,timeout=conf['max_seconds_per_slot'])
        if proc.returncode:entry.update(state='TECHNICAL_FAILURE',exit_code=proc.returncode);dump(ledgerpath,ledger);raise RuntimeError('WORKER_FAILED')
        r=read(slot/'result.json');entry.update(state='FITTED',result=r);dump(ledgerpath,ledger);print(json.dumps(r),flush=True)
    verify(prep['sources']);nfit=len(list((out/'effects').glob('*/fit_started.json')))
    native=len(list((out/'effects').glob('*/native_started.json')));reloads=len(list((out/'effects').glob('*/reload_started.json')))
    dump(out/'effects_manifest.json',dict(started_at=started,completed_at=now(),formal_fits=nfit,main_prediction_calls=native+reloads,
        live_prediction_calls=native,reload_prediction_calls=reloads,sources_unchanged=True,script_sha256=sha(__file__),
        files={str(p.relative_to(out)).replace('\\','/'):sha(p) for p in (out/'effects').rglob('*') if p.is_file()}))

if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True)
    a=s.add_parser('prepare');a.add_argument('--root',required=True);a.add_argument('--out',required=True);a.add_argument('--tests',required=True)
    a=s.add_parser('fit');a.add_argument('--out',required=True)
    a=s.add_parser('_worker');a.add_argument('--out',required=True);a.add_argument('--issue',required=True);a.add_argument('--segment',required=True);a.add_argument('--recovery',action='store_true')
    args=vars(p.parse_args());action=args.pop('action');{'prepare':prepare,'fit':fit,'_worker':worker}[action](**args)
