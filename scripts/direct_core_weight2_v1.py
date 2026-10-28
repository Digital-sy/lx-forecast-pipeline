"""Fixed historical-core weighting. Local archives, one fit per reserved unit."""
from __future__ import annotations
import argparse, hashlib, importlib.metadata, json, os, subprocess, sys, time
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts import direct_training_alignment as audit
except ModuleNotFoundError:
    import direct_training_alignment as audit

ROOT=Path(__file__).resolve().parents[1]
CANDIDATE='DIRECT_CORE_WEIGHT2_V1'
PREREG='1d5700d6fd93df788f930ac7b6b37549eb6dd15d'
FEATURES=['shop_code','lag1','lag7','lag14','lag28','rolling_mean_lag1_window_size7',
          'rolling_mean_lag1_window_size28','dayofweek','month','dayofyear']
KEY=['shop','spu','simulated_issue_date']
sha,read,dump,now=audit.sha,audit.read,audit.dump,audit.now

def array_sha(x):return hashlib.sha256(np.ascontiguousarray(x).tobytes()).hexdigest()

def normalized_weights(states):
    states=np.asarray(states)
    if not np.isin(states,['CORE','NON_CORE','UNKNOWN']).all():raise ValueError('BAD_OR_MISSING_CORE_STATE')
    raw=np.where(states=='CORE',2.,1.);weights=raw/raw.mean()
    if not np.isfinite(weights).all() or not (weights>0).all() or not np.isclose(weights.mean(),1,rtol=0,atol=1e-12):raise ValueError('WEIGHT_DOMAIN')
    return raw,weights

def aligned_weights(train,lookup,dates):
    if train.duplicated(KEY).any():raise ValueError('DUPLICATE_TRAIN_KEY')
    z=train[KEY+['input_cutoff']].copy().reset_index(drop=True);z.insert(0,'row_index',np.arange(len(z),dtype=np.int64))
    z['core_state']=None
    for key,idx in z.groupby(['shop','spu'],sort=False).groups.items():
        if key not in lookup:raise ValueError('MISSING_IDENTITY_JOIN:'+str(key))
        pos=(z.loc[idx,'input_cutoff']-dates[0]).dt.days.to_numpy()
        if (pos<0).any() or (pos>=len(dates)).any():raise ValueError('CUTOFF_OUTSIDE_ARCHIVE')
        z.loc[idx,'core_state']=np.array(['NON_CORE','CORE','UNKNOWN'])[lookup[key][0][pos]]
    z['raw_weight'],z['sample_weight']=normalized_weights(z.core_state)
    check_alignment(train,z);return z

def check_alignment(train,w):
    if len(train)!=len(w) or not np.array_equal(w.row_index,np.arange(len(train))):raise ValueError('WEIGHT_ROW_ORDER')
    pd.testing.assert_frame_equal(train[KEY+['input_cutoff']].reset_index(drop=True),w[KEY+['input_cutoff']].reset_index(drop=True),check_exact=True)
    raw,weights=normalized_weights(w.core_state)
    if not np.array_equal(raw,w.raw_weight) or not np.array_equal(weights,w.sample_weight):raise ValueError('WEIGHT_VALUE_CHANGED')

def fit_forward(estimator,X,y,w,expected,receipt_path):
    """Receives actual call arguments and forwards the same array unchanged."""
    weights=np.asarray(w)
    if list(X.columns)!=FEATURES or weights.dtype!=np.float64 or array_sha(weights)!=expected:raise ValueError('FIT_INTERFACE_CHANGED')
    receipt=dict(at=now(),stage='RECEIVED_AT_FIT_INTERFACE',rows=len(X),features=list(X.columns),
        feature_dtypes={c:str(X[c].dtype) for c in X},label_dtype=str(y.dtype),
        sample_weight_sha256=array_sha(weights),sample_weight_dtype=str(weights.dtype),sample_weight_n=len(weights),
        sample_weight_mean=float(weights.mean()),sample_weight_sum=float(weights.sum()),
        sample_weight_min=float(weights.min()),sample_weight_max=float(weights.max()),
        y_sha256=array_sha(y.to_numpy()),X_column_sha256={c:array_sha(X[c].to_numpy()) for c in X},
        no_class_weight=True,no_eval_set=True,no_init_score=True)
    dump(receipt_path,receipt)
    result=estimator.fit(X,y,sample_weight=weights)
    receipt.update(stage='FIT_RETURNED',completed_at=now());dump(receipt_path,receipt)
    return result

def group_weights(train,w,unit):
    z=w.copy();z['zero_label']=train.y_label.to_numpy()==0;z['shop']=train.shop.to_numpy()
    rows=[]
    for dim,values in [('core_state',['CORE','NON_CORE','UNKNOWN']),('zero_label',[False,True]),('shop',sorted(z.shop.unique()))]:
        for value in values:
            mask=z[dim].eq(value);g=z[mask]
            rows.append(dict(**unit,dimension=dim,value=str(value),rows=len(g),row_share=len(g)/len(z),
                old_weight=float(len(g)),old_weight_share=len(g)/len(z),raw_weight=float(g.raw_weight.sum()),
                sample_weight=float(g.sample_weight.sum()),weight_share=float(g.sample_weight.sum()/z.sample_weight.sum()),
                zero_label_rows=int(g.zero_label.sum()),zero_label_weight=float(g.loc[g.zero_label,'sample_weight'].sum())))
    return rows

def verify(files):
    for p,h in files.items():
        if sha(p)!=h:raise ValueError('SOURCE_CHANGED:'+str(p))

def prepare(root,out,tests):
    root,out,tests=map(lambda x:Path(x).resolve(),[root,out,tests])
    if out.exists() or root not in out.parents:raise ValueError('NEW_EXCLUSIVE_OUTPUT_REQUIRED')
    archive=root/'bc60_monthly_replay_20261020';item=root/'direct_item_id_v1_20261028';alignment=root/'direct_training_alignment_20261028_final';comp=root/'core_model_complementarity_20261028_v1'
    sources=audit.Sources();fm=read(sources.add(archive/'prepared/frozen_config.json'))
    runtime={k:importlib.metadata.version(k) for k in fm['runtime']}
    if runtime!=fm['runtime']:raise ValueError('PINNED_RUNTIME_CHANGED')
    original=read(sources.add(archive/'effects/run_manifest.json'));oldfiles={k.replace('\\','/'):v for k,v in original['files'].items()}
    itemfiles=read(sources.add(item/'delivery_manifest.json'))['files']
    def itemfile(rel):return sources.add(item/rel,itemfiles[rel])
    am=read(sources.add(alignment/'delivery_receipt.json'))
    for name in ['manifest.json','training_units.csv','training_groups.csv','independent_validation.json']:
        sources.add(alignment/name,am['files'][name])
    cm=read(sources.add(comp/'manifest.json'))
    for p,h in cm['protected_shadow_files'].items():sources.add(p,h)
    for name in ['objects.parquet','historical_windows.parquet','top20_products.csv','all_miss_directions_detail.csv']:
        sources.add(comp/name,cm['files'][name])
    for name in ['scoring/scores.parquet','scoring/manifest.json']:
        sources.add(root/'direct_yearago_blend50_20261021_v1'/name)
    sources.add(ROOT/'docs/direct_core_weight2_v1_protocol.md');sources.add(tests)
    sources.add(ROOT/'scripts/direct_training_alignment.py')
    daily=pd.read_parquet(sources.add(archive/'inputs/daily.parquet','03596126d5543adc24325de12f03d4899c372e7076b01339e19656ce289bfc82'),columns=['ds','shop','spu','y','mapped','sales_state'])
    sources.add(archive/'inputs/monthly_reference.parquet','423ce1086fc885d7d41e087e95b048638600710c1d174d3d18a9a3f1a98228af')
    dates,lookup,_=audit.historical_core(daily)
    grid=pd.read_csv(sources.add(archive/'prepared/issue_grid.csv'));grid=grid[grid.calendar_mature.eq(True)]
    core=pd.read_parquet(sources.add(archive/'prepared/core.parquet'));core=core[core.issue_date.isin(grid.issue_date)]
    if len(core)!=2820 or len(grid)!=20:raise ValueError('FROZEN_SCOPE_CHANGED')
    out.mkdir();(out/'prepared').mkdir();started=now()
    grid.to_csv(out/'prepared/issue_grid.csv',index=False);core.to_parquet(out/'prepared/core.parquet',index=False)
    conf=dict(candidate=CANDIDATE,preregistration_commit=PREREG,root=str(root),archive=str(archive),features=FEATURES,
        lightgbm=fm['original']['lightgbm'],runtime=runtime,raw_weights=dict(CORE=2.,NON_CORE=1.,UNKNOWN=1.),
        normalization='divide each full fit unit by its raw mean',max_formal_fits=40,max_seconds_per_slot=600,
        classification='RECONSTRUCTED_HISTORICAL_DEVELOPMENT_NOT_OOS',script_sha256=sha(__file__),
        identity_script_sha256=sha(ROOT/'scripts/direct_training_alignment.py'),test_receipt_sha256=sha(tests))
    if conf['lightgbm']['objective']!='poisson':raise ValueError('ORIGINAL_OBJECTIVE_CHANGED')
    dump(out/'configuration.json',conf);summaries=[];checks=[];slots=[]
    expected_groups=pd.read_csv(alignment/'training_groups.csv')
    for issue in grid.issue_date:
        m1=None
        for seg in ['M1','M2']:
            name=f'{issue}_{seg}';folder=out/'prepared'/name;folder.mkdir();oldslot=archive/'effects/slots'/name
            trainpath=itemfile(f'prepared/{name}/train.parquet');infpath=itemfile(f'prepared/{name}/inference.parquet')
            train=pd.read_parquet(trainpath).drop(columns='item_key');inf=pd.read_parquet(infpath).drop(columns='item_key')
            oa=read(sources.add(oldslot/'training_audit.json',oldfiles[f'slots/{name}/training_audit.json']))
            audit.original_audit(train,oa)
            cp=read(sources.add(oldslot/'checkpoint.json',oldfiles[f'slots/{name}/checkpoint.json']))
            sources.add(oldslot/'model.txt',cp['model_sha256'])
            check=read(itemfile(f'prepared/{name}/training_check.json'))
            if check['old_frame_hash']!=cp['training_sha256']:raise ValueError('MATRIX_PROVENANCE')
            if not train.label_end.le(pd.Timestamp(issue)-pd.Timedelta(days=2)).all():raise ValueError('IMMATURE_LABEL')
            if not (train.input_cutoff==train.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('OWN_CUTOFF')
            w=aligned_weights(train,lookup,dates)
            expected=expected_groups[(expected_groups.issue_date==issue)&(expected_groups.segment==seg)&(expected_groups.dimension=='core_state')].set_index('value')
            for state,g in w.groupby('core_state'):
                if int(expected.loc[state,'rows'])!=len(g):raise ValueError('DIAGNOSTIC_IDENTITY_CHANGED')
            if seg=='M1':m1=w[KEY+['core_state','raw_weight']].copy()
            else:
                common=m1.merge(w[KEY+['core_state','raw_weight']],on=KEY,validate='one_to_one',suffixes=('_1','_2'))
                if not (common.core_state_1==common.core_state_2).all() or not np.array_equal(common.raw_weight_1,common.raw_weight_2):raise ValueError('SEGMENT_CORE_DISAGREEMENT')
            w.to_parquet(folder/'weights.parquet',index=False);inf.to_parquet(folder/'inference.parquet',index=False)
            info=dict(status='PASS',issue_date=issue,segment=seg,train_path=str(trainpath),train_sha256=sha(trainpath),
                old_training_audit_path=str(oldslot/'training_audit.json'),old_frame_hash=cp['training_sha256'],
                old_features_exact=True,old_labels_keys_dtypes_exact=True,rows=len(train),inference_rows=len(inf),
                weights_sha256=sha(folder/'weights.parquet'),inference_sha256=sha(folder/'inference.parquet'),
                sample_weight_sha256=array_sha(w.sample_weight.to_numpy()),raw_weight_sha256=array_sha(w.raw_weight.to_numpy()),
                mean_weight=float(w.sample_weight.mean()),sum_weight=float(w.sample_weight.sum()),
                min_weight=float(w.sample_weight.min()),max_weight=float(w.sample_weight.max()),
                zero_labels_preserved=int(train.y_label.eq(0).sum()),labels_mature=True,feature_columns=FEATURES,
                input_cutoff=str((pd.Timestamp(issue)-pd.Timedelta(days=2)).date()))
            dump(folder/'training_check.json',info);checks.append(info);summaries.extend(group_weights(train,w,dict(issue_date=issue,segment=seg)))
            slots.append(dict(issue_date=issue,segment=seg,state='PLANNED'));print(name,len(train),float(w.sample_weight.mean()),flush=True)
    pd.DataFrame(summaries).to_csv(out/'weight_group_summary.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(checks).to_csv(out/'training_reconciliation.csv',index=False,encoding='utf-8-sig')
    sources.verify();dump(out/'prepare_manifest.json',dict(started_at=started,completed_at=now(),sources=sources.files,
        formal_fit_count=0,preregistration_commit=PREREG,script_sha256=sha(__file__),
        files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}))
    dump(out/'formal_fit_ledger.json',dict(max_formal_fits=40,created_at=now(),slots=slots))

def worker(out,issue,segment,recovery=False):
    from lightgbm import LGBMRegressor,Booster
    out=Path(out).resolve();conf=read(out/'configuration.json');folder=out/'prepared'/f'{issue}_{segment}';slot=out/'effects'/folder.name
    slot.mkdir(parents=True,exist_ok=True);check=read(folder/'training_check.json')
    if conf['script_sha256']!=sha(__file__) or conf['features']!=FEATURES:raise ValueError('CODE_CHANGED')
    verify({check['train_path']:check['train_sha256'],str(folder/'weights.parquet'):check['weights_sha256'],str(folder/'inference.parquet'):check['inference_sha256']})
    train=pd.read_parquet(check['train_path']).drop(columns='item_key');w=pd.read_parquet(folder/'weights.parquet');inf=pd.read_parquet(folder/'inference.parquet')
    check_alignment(train,w);X=train[FEATURES];y=train.y_label;weights=w.sample_weight.to_numpy();cp=slot/'checkpoint.json'
    if recovery:
        prior=read(cp);verify({str(slot/'model.txt'):prior['model_sha256']});booster=Booster(model_file=os.path.relpath(slot/'model.txt',ROOT));seconds=prior['fit_seconds']
    else:
        with (slot/'fit_started.json').open('x',encoding='utf-8') as f:json.dump(dict(at=now(),issue_date=issue,segment=segment,weights_sha256=check['weights_sha256']),f)
        model=LGBMRegressor(**conf['lightgbm']);tic=time.monotonic()
        fit_forward(model,X,y,weights,check['sample_weight_sha256'],slot/'fit_received.json')
        seconds=time.monotonic()-tic;booster=model.booster_;booster.save_model(os.path.relpath(slot/'model.txt',ROOT))
        if booster.feature_name()!=FEATURES:raise ValueError('FEATURE_PROJECTION_CHANGED')
        dump(cp,dict(at=now(),model_sha256=sha(slot/'model.txt'),fit_seconds=seconds,params=conf['lightgbm'],features=FEATURES,
            train_sha256=check['train_sha256'],sample_weight_sha256=check['sample_weight_sha256'],sample_weight_mean=float(weights.mean()),
            fit_received_sha256=sha(slot/'fit_received.json'),old_features_unchanged=True))
    pred=inf.copy();pred['prediction']=np.nan;ok=pred.input_status.eq('OK')
    pred.loc[ok,'prediction']=np.maximum(booster.predict(pred.loc[ok,FEATURES]),0)
    pred['prediction_status']=pred.input_status;pred['prediction_origin']=np.where(ok,'NEW_WEIGHTED_MODEL','NO_OUTPUT')
    pred.to_parquet(slot/'predictions.parquet',index=False)
    dump(slot/'result.json',dict(status='FITTED',issue_date=issue,segment=segment,fit_seconds=seconds,completed_at=now(),
        n=len(pred),output_n=int(ok.sum()),prediction_calls=1,model_sha256=sha(slot/'model.txt'),prediction_sha256=sha(slot/'predictions.parquet')))

def fit(out):
    out=Path(out).resolve();conf=read(out/'configuration.json');prep=read(out/'prepare_manifest.json')
    if sha(__file__)!=conf['script_sha256']:raise ValueError('CODE_CHANGED')
    if {k:importlib.metadata.version(k) for k in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    verify(prep['sources']);verify({str(out/k):v for k,v in prep['files'].items()})
    ledgerpath=out/'formal_fit_ledger.json';ledger=read(ledgerpath);started=now()
    for entry in ledger['slots']:
        issue,seg=entry['issue_date'],entry['segment'];slot=out/'effects'/f'{issue}_{seg}'
        if (slot/'result.json').exists():
            r=read(slot/'result.json');verify({str(slot/'model.txt'):r['model_sha256'],str(slot/'predictions.parquet'):r['prediction_sha256']});entry['state']='FITTED';dump(ledgerpath,ledger);continue
        recovery=(slot/'checkpoint.json').exists()
        if (slot/'fit_started.json').exists() and not recovery:raise ValueError('REFIT_FORBIDDEN')
        if len(list((out/'effects').glob('*/fit_started.json')))>=40 and not recovery:raise ValueError('FIT_BUDGET')
        entry.update(state='RUNNING',reserved_at=now());dump(ledgerpath,ledger)
        args=[sys.executable,'-B',str(Path(__file__).resolve()),'_worker','--out',str(out),'--issue',issue,'--segment',seg]
        if recovery:args.append('--recovery')
        proc=subprocess.run(args,cwd=ROOT,timeout=conf['max_seconds_per_slot'])
        if proc.returncode:entry.update(state='TECHNICAL_FAILURE',exit_code=proc.returncode);dump(ledgerpath,ledger);raise RuntimeError('WORKER_FAILED')
        r=read(slot/'result.json');entry.update(state='FITTED',result=r);dump(ledgerpath,ledger);print(json.dumps(r),flush=True)
    verify(prep['sources']);count=len(list((out/'effects').glob('*/fit_started.json')))
    if count>40:raise ValueError('FIT_BUDGET')
    dump(out/'effects_manifest.json',dict(started_at=started,completed_at=now(),formal_fits=count,main_prediction_calls=len(ledger['slots']),
        sources_unchanged=True,script_sha256=sha(__file__),files={str(p.relative_to(out)):sha(p) for p in (out/'effects').rglob('*') if p.is_file()}))

if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True)
    a=s.add_parser('prepare');a.add_argument('--root',required=True);a.add_argument('--out',required=True);a.add_argument('--tests',required=True)
    a=s.add_parser('fit');a.add_argument('--out',required=True)
    a=s.add_parser('_worker');a.add_argument('--out',required=True);a.add_argument('--issue',required=True);a.add_argument('--segment',required=True);a.add_argument('--recovery',action='store_true')
    v=vars(p.parse_args());action=v.pop('action');{'prepare':prepare,'fit':fit,'_worker':worker}[action](**v)
