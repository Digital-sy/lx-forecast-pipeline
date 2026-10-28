"""Fixed categorical identity increment; immutable local archives only."""
from __future__ import annotations
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import numpy as np
import pandas as pd
try:
    from scripts import bc60_monthly_replay as frozen, bc60_v3_direct as direct
except ModuleNotFoundError:
    import bc60_monthly_replay as frozen, bc60_v3_direct as direct

ROOT=Path(__file__).resolve().parents[1]
CANDIDATE='DIRECT_ITEM_ID_V1'
PREREG='94d8d02eee02000ff678b392190e354525af7fe2'
FEATURES=direct.FEATURES+['item_key']
KEY=['shop','spu','simulated_issue_date']
AUDIT=['shop','spu','simulated_issue_date','input_cutoff','label_start','label_end','y_label',*direct.FEATURES]
CAT_DEFAULTS={'min_data_per_group':100,'max_cat_threshold':32,'cat_l2':10,'cat_smooth':10,'max_cat_to_onehot':4}
dump,read,now=frozen.dump,frozen.read_json,frozen.now


def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()


def verify(files):
    for p,h in files.items():
        if sha(p)!=h:raise ValueError('IMMUTABLE_SOURCE_CHANGED:'+p)


def item_tuples(frame):
    keys=list(frame[['shop','spu']].itertuples(index=False,name=None))
    if any(not isinstance(x,str) or not x.strip() for key in keys for x in key):
        raise ValueError('INVALID_ITEM_IDENTITY')
    return keys


def dictionary(train):
    keys=sorted(set(item_tuples(train)))
    if len(keys)>=np.iinfo(np.int32).max:raise ValueError('CATEGORY_CAPACITY')
    return pd.DataFrame([(shop,spu,i) for i,(shop,spu) in enumerate(keys)],columns=['shop','spu','item_key']).astype({'item_key':'int32'})


def encode(frame,categories):
    if 'item_key' in frame:raise ValueError('IDENTITY_ALREADY_PRESENT')
    if categories.duplicated(['shop','spu']).any() or categories.item_key.duplicated().any():raise ValueError('BAD_DICTIONARY')
    if not np.array_equal(categories.item_key.to_numpy(),np.arange(len(categories))):raise ValueError('NONCONTIGUOUS_DICTIONARY')
    lookup={(r.shop,r.spu):int(r.item_key) for r in categories.itertuples()}
    out=frame.copy(deep=True);out['item_key']=np.array([lookup.get(k,-1) for k in item_tuples(frame)],dtype=np.int32)
    pd.testing.assert_frame_equal(out[frame.columns],frame,check_exact=True)
    return out


def support(train,inf):
    by={}
    for key,g in train.groupby(['shop','spu'],sort=True):
        dates=g.simulated_issue_date
        by[key]=dict(training_rows=len(g),training_dates=int(dates.nunique()),training_first=str(dates.min().date()),
            training_last=str(dates.max().date()),training_span_days=int((dates.max()-dates.min()).days+1),
            label_first=str(g.label_start.min().date()),label_last=str(g.label_end.max().date()),
            nonoverlap_windows=direct.disjoint_windows(g[['label_start','label_end']].drop_duplicates()))
    rows=[]
    for r in inf.itertuples():
        rec=dict(issue_date=r.issue_date,segment=r.segment,shop=r.shop,spu=r.spu,item_key=int(r.item_key),
            seen_in_training=r.item_key>=0,input_status=r.input_status,positive_completed_months=r.positive_completed_months)
        rec.update(by.get((r.shop,r.spu),dict(training_rows=0,training_dates=0,training_first=None,training_last=None,
            training_span_days=0,label_first=None,label_last=None,nonoverlap_windows=0)))
        rows.append(rec)
    return pd.DataFrame(rows)


def model_metadata(booster,path):
    text=Path(path).read_text(encoding='utf-8');params={k:v for k,v in re.findall(r'^\[([^:]+): (.*?)\]$',text,re.M)}
    if params.get('categorical_feature')!=str(len(direct.FEATURES)):raise ValueError('NOT_EXPLICIT_CATEGORICAL')
    for k,v in CAT_DEFAULTS.items():
        if float(params[k])!=v:raise ValueError('CATEGORY_DEFAULT_CHANGED:'+k)
    if booster.feature_name()!=FEATURES:raise ValueError('FEATURE_ORDER_CHANGED')
    tree=booster.dump_model();counts={}
    def walk(node):
        if 'split_index' not in node:return
        name=FEATURES[node['split_feature']];typ=node['decision_type'];counts[(name,typ)]=counts.get((name,typ),0)+1
        if name=='item_key' and typ!='==':raise ValueError('NUMERIC_ITEM_SPLIT')
        if name!='item_key' and typ=='==':raise ValueError('OLD_NUMERIC_CHANGED_TO_CATEGORY')
        walk(node['left_child']);walk(node['right_child'])
    for t in tree['tree_info']:walk(t['tree_structure'])
    return dict(categorical_feature=params['categorical_feature'],effective_category_parameters={k:params[k] for k in CAT_DEFAULTS},
        effective_parameters=params,item_info=tree['feature_infos'].get('item_key'),
        split_counts=[dict(feature=k[0],decision_type=k[1],count=v) for k,v in counts.items()],
        item_categorical_splits=counts.get(('item_key','=='),0),features=booster.feature_name())


def prepare(root,out,tests):
    root,out,tests=map(lambda p:Path(p).resolve(),[root,out,tests])
    if out.exists() or root not in out.parents:raise ValueError('NEW_EXCLUSIVE_OUTPUT_REQUIRED')
    test=read(tests)
    if test['status']!='PASS':raise ValueError('TESTS_REQUIRED')
    verify({str(ROOT/p):h for p,h in test['files_sha256'].items()})
    archive=root/'bc60_monthly_replay_20261020';comp=root/'core_model_complementarity_20261028_v1';traffic=root/'direct_traffic_state_v1_20261028'
    conf=read(archive/'prepared/frozen_config.json');pm=read(archive/'prepared/prepare_manifest.json')
    if sha(ROOT/'scripts/bc60_monthly_replay.py')!=pm['script_sha256']:raise ValueError('FROZEN_EXECUTOR_CHANGED')
    verify({str(ROOT/p):h for p,h in conf['source_hashes'].items()});verify(pm['sources'])
    verify({str(archive/'prepared'/p):h for p,h in pm['files'].items()})
    im=read(archive/'inputs/input_manifest.json');verify({str(archive/'inputs'/p):h for p,h in im['files'].items()})
    cm=read(comp/'manifest.json');verify(cm['sources']);verify(cm['protected_shadow_files']);verify({str(comp/p):h for p,h in cm['files'].items()})
    ts=read(traffic/'scoring/manifest.json');verify({str(traffic/'scoring'/p):h for p,h in ts['files'].items()})
    runtime={p:importlib.metadata.version(p) for p in conf['runtime']}
    if runtime!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    sources=dict(cm['sources']);sources.update(cm['protected_shadow_files'])
    for p in [archive/'prepared/prepare_manifest.json',archive/'prepared/frozen_config.json',archive/'prepared/core.parquet',
              archive/'prepared/issue_grid.csv',archive/'inputs/daily.parquet',archive/'inputs/monthly_reference.parquet',
              archive/'inputs/input_manifest.json',comp/'manifest.json',comp/'objects.parquet',comp/'historical_windows.parquet',
              comp/'all_miss_directions_detail.csv',comp/'top20_products.csv',traffic/'delivery_manifest.json',
              traffic/'scoring/manifest.json',traffic/'scoring/scores.parquet',ROOT/'scripts/bc60_monthly_replay.py',
              ROOT/'scripts/bc60_v3_direct.py',ROOT/'docs/direct_item_id_v1_protocol.md',tests]:sources[str(p)]=sha(p)
    oldmanifest=read(archive/'effects/run_manifest.json')
    grid=pd.read_csv(archive/'prepared/issue_grid.csv');grid=grid[grid.calendar_mature.eq(True)]
    core=pd.read_parquet(archive/'prepared/core.parquet');core=core[core.issue_date.isin(grid.issue_date)]
    if len(grid)!=20 or len(core)!=2820:raise ValueError('FROZEN_SCOPE_CHANGED')
    for issue in grid.issue_date:
        for seg in ['M1','M2']:
            prior=archive/'effects/slots'/f'{issue}_{seg}'
            for name in ['training_audit.json','checkpoint.json','segment_predictions.parquet','model.txt']:
                p=prior/name;h=sha(p)
                if h!=oldmanifest['files'][str(p.relative_to(archive/'effects'))]:raise ValueError('ORIGINAL_SLOT_CHANGED')
                sources[str(p)]=h
    # Original sales input only. No traffic, category, season, price or inventory columns.
    d=pd.read_parquet(archive/'inputs/daily.parquet',columns=['ds','shop','spu','y','mapped','sales_state'])
    m=pd.read_parquet(archive/'inputs/monthly_reference.parquet');d,m=frozen.normalize(d,m)
    out.mkdir(exist_ok=False);(out/'prepared').mkdir();started=now()
    core.to_parquet(out/'prepared/core.parquet',index=False);grid.to_csv(out/'prepared/issue_grid.csv',index=False)
    config=dict(candidate=CANDIDATE,preregistration_commit=PREREG,features=FEATURES,original_features=direct.FEATURES,
        categorical_feature=['item_key'],unknown_category_code=-1,category_defaults=CAT_DEFAULTS,
        dictionary_rule='sorted unique (shop,spu) strings from this actual training unit only',
        lightgbm=conf['original']['lightgbm'],runtime=runtime,sample_weight=None,max_formal_fits=40,max_seconds_per_slot=600,
        archive=str(archive),root=str(root),script_sha256=sha(__file__),classification='RECONSTRUCTED_HISTORICAL_DEVELOPMENT_NOT_OOS',
        support_bands=['0','1-3','4-11','12+'],support_count='minimum nonoverlapping target interval count across M1 and M2')
    dump(out/'configuration.json',config);reconciled=[];slots=[];supports=[]
    for issue in grid.issue_date:
        cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2);ctx=frozen.VectorContext(d,m,frozen.SHOPS,cutoff);selected=core[core.issue_date.eq(issue)]
        for seg in ['M1','M2']:
            folder=out/'prepared'/f'{issue}_{seg}';folder.mkdir(exist_ok=False);prior=archive/'effects/slots'/folder.name
            train,_=ctx.training(issue,seg);audit=frozen.training_audit(train,KEY,AUDIT);old=read(prior/'training_audit.json');cp=read(prior/'checkpoint.json')
            if audit!=old or frozen.frame_hash(train)!=cp['training_sha256']:raise ValueError('ORIGINAL_TRAIN_CHANGED')
            if train.duplicated(KEY).any() or not train.label_end.le(cutoff).all():raise ValueError('TRAIN_KEY_OR_MATURITY')
            if not (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all():raise ValueError('ROW_CUTOFF')
            cats=dictionary(train);full=encode(train,cats)
            if full.item_key.lt(0).any():raise ValueError('UNSEEN_TRAINING')
            inf=pd.DataFrame([ctx.row((r.shop,r.spu),issue,seg,issue) for r in selected.itertuples()])
            oi=pd.read_parquet(prior/'segment_predictions.parquet').set_index(['shop','spu']);ii=inf.set_index(['shop','spu'])
            pd.testing.assert_frame_equal(ii[direct.FEATURES+['input_status']],oi.loc[ii.index,direct.FEATURES+['input_status']],check_exact=True,check_dtype=False)
            inf=encode(inf,cats);sp=support(full,inf);supports.append(sp)
            full.to_parquet(folder/'train.parquet',index=False);inf.to_parquet(folder/'inference.parquet',index=False)
            cats.to_parquet(folder/'dictionary.parquet',index=False);cats.to_csv(folder/'dictionary.csv',index=False,encoding='utf-8-sig')
            sp.to_parquet(folder/'support.parquet',index=False)
            dump(folder/'old_training_audit.json',audit)
            dump(folder/'training_check.json',dict(status='PASS',old_frame_hash=cp['training_sha256'],old_audit_exact=True,
                train_rows=len(train),train_sha256=sha(folder/'train.parquet'),inference_sha256=sha(folder/'inference.parquet'),
                dictionary_sha256=sha(folder/'dictionary.parquet'),dictionary_categories=len(cats),old_features_unchanged=True,
                weights_unchanged=True,qualification_unchanged=True,row_cutoff=True,labels_mature=True,max_label_end=str(train.label_end.max())))
            rec=dict(issue_date=issue,segment=seg,train_n=len(train),categories=len(cats),inference_n=len(inf),
                eligible_n=int(inf.input_status.eq('OK').sum()),seen_n=int(inf.item_key.ge(0).sum()),
                unseen_n=int(inf.item_key.lt(0).sum()),eligible_unseen_n=int((inf.item_key.lt(0)&inf.input_status.eq('OK')).sum()),old_exact=True)
            reconciled.append(rec);slots.append(dict(issue_date=issue,segment=seg,state='PLANNED'))
            print(json.dumps({'prepared':folder.name,**rec}),flush=True)
    pd.DataFrame(reconciled).to_csv(out/'training_reconciliation.csv',index=False,encoding='utf-8-sig')
    pd.concat(supports,ignore_index=True).to_parquet(out/'identity_support.parquet',index=False)
    pd.concat(supports,ignore_index=True).to_csv(out/'identity_support.csv',index=False,encoding='utf-8-sig')
    verify(sources)
    dump(out/'prepare_manifest.json',dict(started_at=started,completed_at=now(),sources=sources,script_sha256=sha(__file__),
        preregistration_commit=PREREG,formal_fit_count=0,files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}))
    dump(out/'formal_fit_ledger.json',dict(created_at=now(),max_formal_fits=40,slots=slots))
    print(json.dumps({'status':'PREPARED_NO_EFFECTS','slots':len(slots)}),flush=True)


def worker(out,issue,segment,recovery=False):
    from lightgbm import LGBMRegressor,Booster
    out=Path(out).resolve();conf=read(out/'configuration.json');folder=out/'prepared'/f'{issue}_{segment}';slot=out/'effects'/folder.name
    slot.mkdir(parents=True,exist_ok=True);check=read(folder/'training_check.json')
    if conf['script_sha256']!=sha(__file__) or conf['features']!=FEATURES:raise ValueError('FROZEN_CODE_CHANGED')
    verify({str(folder/n):check[k] for n,k in [('train.parquet','train_sha256'),('inference.parquet','inference_sha256'),('dictionary.parquet','dictionary_sha256')]})
    train=pd.read_parquet(folder/'train.parquet');inf=pd.read_parquet(folder/'inference.parquet');cp=slot/'checkpoint.json'
    if recovery:
        c=read(cp)
        if sha(slot/'model.txt')!=c['model_sha256']:raise ValueError('CHECKPOINT_CHANGED')
        booster=Booster(model_file=os.path.relpath(slot/'model.txt',ROOT));seconds=c['fit_seconds']
    else:
        with (slot/'fit_started.json').open('x',encoding='utf-8') as f:
            json.dump(dict(at=now(),issue_date=issue,segment=segment,training_sha256=check['train_sha256'],dictionary_sha256=check['dictionary_sha256']),f)
        model=LGBMRegressor(**conf['lightgbm']);tic=time.monotonic()
        model.fit(train[FEATURES],train.y_label,feature_name=FEATURES,categorical_feature=['item_key'])
        seconds=time.monotonic()-tic;booster=model.booster_;booster.save_model(os.path.relpath(slot/'model.txt',ROOT))
        dump(cp,dict(at=now(),model_sha256=sha(slot/'model.txt'),fit_seconds=seconds,params=conf['lightgbm'],features=FEATURES,
            categorical_feature=['item_key'],sample_weight=None,train_sha256=check['train_sha256'],dictionary_sha256=check['dictionary_sha256']))
    meta=model_metadata(booster,slot/'model.txt');dump(slot/'categorical_metadata.json',meta)
    pred=inf.copy();pred['prediction']=np.nan;ok=pred.input_status.eq('OK')
    pred.loc[ok,'prediction']=np.maximum(booster.predict(pred.loc[ok,FEATURES]),0)
    pred['prediction_origin']=np.where(ok,'NEW_MODEL','NO_OUTPUT');pred['prediction_status']=pred.input_status
    pred.to_parquet(slot/'predictions.parquet',index=False)
    result=dict(status='FITTED',formal_fit=True,issue_date=issue,segment=segment,completed_at=now(),fit_seconds=seconds,
        n=len(pred),output_n=int(ok.sum()),unseen_output_n=int((ok&pred.item_key.lt(0)).sum()),
        model_sha256=sha(slot/'model.txt'),prediction_sha256=sha(slot/'predictions.parquet'),item_categorical_splits=meta['item_categorical_splits'])
    dump(slot/'result.json',result)


def fit(out):
    out=Path(out).resolve();conf=read(out/'configuration.json');p=read(out/'prepare_manifest.json')
    if conf['script_sha256']!=sha(__file__):raise ValueError('FROZEN_CODE_CHANGED')
    if {k:importlib.metadata.version(k) for k in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    verify(p['sources']);verify({str(out/k):h for k,h in p['files'].items()})
    ledgerpath=out/'formal_fit_ledger.json';ledger=read(ledgerpath);started=now()
    for entry in ledger['slots']:
        issue,seg=entry['issue_date'],entry['segment'];slot=out/'effects'/f'{issue}_{seg}'
        if (slot/'result.json').exists():
            r=read(slot/'result.json');verify({str(slot/'model.txt'):r['model_sha256'],str(slot/'predictions.parquet'):r['prediction_sha256']})
            entry.update(state='FITTED',result=r);dump(ledgerpath,ledger);continue
        recovery=(slot/'checkpoint.json').exists()
        if (slot/'fit_started.json').exists() and not recovery:raise ValueError('NO_REFIT_AFTER_STARTED_FAILURE')
        if len(list((out/'effects').glob('*/fit_started.json')))>=40 and not recovery:raise ValueError('FIT_BUDGET')
        entry.update(state='RUNNING',reserved_at=now());dump(ledgerpath,ledger)
        args=[sys.executable,'-B',str(Path(__file__).resolve()),'_worker','--out',str(out),'--issue',issue,'--segment',seg]
        if recovery:args.append('--recovery')
        proc=subprocess.run(args,cwd=ROOT,timeout=conf['max_seconds_per_slot'])
        if proc.returncode:
            entry.update(state='TECHNICAL_FAILURE',exit_code=proc.returncode);dump(ledgerpath,ledger);raise RuntimeError('WORKER_FAILED')
        r=read(slot/'result.json');entry.update(state='FITTED',result=r);dump(ledgerpath,ledger);print(json.dumps(r),flush=True)
    verify(p['sources']);fits=list((out/'effects').glob('*/fit_started.json'))
    if len(fits)>40:raise ValueError('FIT_BUDGET')
    dump(out/'effects_manifest.json',dict(started_at=started,completed_at=now(),formal_fits=len(fits),script_sha256=sha(__file__),
        sources_unchanged=True,prepare_manifest_sha256=sha(out/'prepare_manifest.json'),files={str(f.relative_to(out)):sha(f) for f in (out/'effects').rglob('*') if f.is_file()}))
    print(json.dumps({'status':'COMPLETED','formal_fits':len(fits)}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest='action',required=True)
    p=sub.add_parser('prepare');p.add_argument('--root',required=True);p.add_argument('--out',required=True);p.add_argument('--tests',required=True)
    p=sub.add_parser('fit');p.add_argument('--out',required=True)
    p=sub.add_parser('_worker');p.add_argument('--out',required=True);p.add_argument('--issue',required=True);p.add_argument('--segment',required=True);p.add_argument('--recovery',action='store_true')
    args=vars(parser.parse_args());action=args.pop('action');{'prepare':prepare,'fit':fit,'_worker':worker}[action](**args)
