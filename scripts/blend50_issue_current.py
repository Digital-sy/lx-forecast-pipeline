"""One fixed local prospective issue, at most four fits, no source-system clients.

Wraps the hash-pinned DIRECT/YEARAGO/core/GATE/BLEND functions unchanged.
Prepare, each fit slot, and final sealing are exclusive and individually recorded.
"""
import argparse,datetime,importlib.metadata,json,os,pathlib,subprocess,time
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
SLOTS=('direct_M1','direct_M2','annual_M1','annual_M2')

def same_business_day(issue):
    today=datetime.datetime.now(datetime.timezone.utc).astimezone(ZoneInfo('Asia/Shanghai')).date()
    if str(today)!=str(issue):raise ValueError('ACTUAL_BUSINESS_DAY_CHANGED_NO_BACKDATE')

def label_check(train,cutoff):
    if not train.label_end.le(cutoff).all():raise ValueError('IMMATURE_TRAINING_LABEL')
    if not train.input_cutoff.eq(train.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('WRONG_ROW_CUTOFF')
    if not train.history_end.eq(train.input_cutoff).all():raise ValueError('FUTURE_FEATURE')

def prepare(root,workspace):
    root,workspace=pathlib.Path(root).resolve(),pathlib.Path(workspace).resolve()
    lock=p.read(root/'run_lock.json');same_business_day(lock['issue_date'])
    gate=p.read(root/'inputs/manifest.json')
    if gate['status']!='INPUT_READY_NOT_ISSUED' or gate['blockers']:raise ValueError('INPUT_GATE_NOT_READY')
    independent=p.read(root/'inputs/independent_validation.json')
    if independent['status']!='PASS':raise ValueError('INPUT_NOT_INDEPENDENTLY_VERIFIED')
    dpath=root/'inputs/daily.parquet';mpath=pathlib.Path(gate['monthly_path'])
    if p.sha(dpath)!=gate['daily_sha256'] or p.sha(mpath)!=gate['monthly_sha256']:raise ValueError('INPUT_CHANGED')
    code=p.verify_code();conf=p.read(workspace/'bc60_monthly_replay_20261020/prepared/frozen_config.json')
    runtime={n:importlib.metadata.version(n) for n in conf['runtime']}
    if runtime!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    out=root/'prepared';out.mkdir(exist_ok=False)
    issue=lock['issue_date'];cutoff=pd.Timestamp(lock['input_cutoff'])
    d=pd.read_parquet(dpath,columns=p.DAILY)
    m=pd.read_parquet(mpath);m=m[m.month.le(str(direct.completed_month(cutoff)))].copy()
    d,m=frozen.normalize(d,m)
    if d.ds.max()!=cutoff:raise ValueError('INPUT_CUTOFF_MISMATCH')
    universe,quality=frozen.freeze_issue(d,m,issue)
    if not quality.selection_status.eq('OK').all():raise ValueError('CORE_SELECTION_INCOMPLETE')
    core=universe[universe.is_core].copy().reset_index(drop=True);p.unique(core,p.KEY)
    universe.to_parquet(out/'universe.parquet',index=False);core.to_parquet(out/'core.parquet',index=False)
    core.to_csv(out/'core.csv',index=False,encoding='utf-8-sig');quality.to_csv(out/'selection_quality.csv',index=False,encoding='utf-8-sig')
    config={'issue':issue,'cutoff':lock['input_cutoff'],'windows':p.time_contract(issue),'runtime':runtime,
        'lightgbm':conf['original']['lightgbm'],'core_sha256':p.sha(out/'core.parquet'),'sources':{str(dpath):p.sha(dpath),str(mpath):p.sha(mpath)},
        'source_gate_sha256':p.sha(root/'inputs/manifest.json'),'frozen_code':code,'adapter_sha256':p.sha(__file__),
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        'context_diagnostic_sha256':p.sha(ROOT/'scripts/direct_context90_experiment.py'),
        'formal_fit_slots':list(SLOTS),'maximum_fits':4,'prepared_started':p.clock(),'future_truth_read':False,
        'description':'研究影子预测，不用于自动采购，不替换生产V4'}
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
    root=pathlib.Path(root).resolve();prep=root/'prepared';conf=p.read(prep/'config.json');same_business_day(conf['issue'])
    if slot not in SLOTS:raise ValueError('FIT_SLOT_NOT_ALLOWED')
    if p.sha(__file__)!=conf['adapter_sha256']:raise ValueError('ADAPTER_CHANGED')
    p.verify_code();p.verify_sources(conf['sources'])
    if {n:importlib.metadata.version(n) for n in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    prefix,seg=slot.split('_');meta=p.read(prep/seg/'manifest.json')
    for n,h in meta['files'].items():
        if p.sha(prep/seg/n)!=h:raise ValueError('PREPARED_CHANGED')
    out=root/'effects'/slot;out.mkdir(parents=True,exist_ok=False)
    allowed=not meta['direct_support'].startswith('NOT_TRAINABLE') if prefix=='direct' else meta['annual_support']['fit_allowed']
    features=direct.FEATURES if prefix=='direct' else annual.FEATURES
    inf=pd.read_parquet(prep/seg/(prefix+'_inference.parquet'));values=np.full(len(inf),np.nan);raw=values.copy()
    started=p.clock();seconds=0.;model_hash=None
    if allowed:
        train_path=prep/seg/(prefix+'_train.parquet');train=pd.read_parquet(train_path)
        label_check(train,pd.Timestamp(conf['cutoff']))
        p.dump_new(out/'fit_started.json',{'at':p.clock(),'slot':slot,'training_sha256':p.sha(train_path),
            'features':features,'parameters':conf['lightgbm'],'formal_fit_number':1+len(list((root/'effects').glob('*/checkpoint.json')))})
        if len(list((root/'effects').glob('*/fit_started.json')))>4:raise ValueError('FIT_BUDGET')
        model=LGBMRegressor(**conf['lightgbm']);begin=time.monotonic();model.fit(train[features],train.y_label);seconds=time.monotonic()-begin
        relative=os.path.relpath(out/'model.txt',pathlib.Path.cwd());relative.encode('ascii');model.booster_.save_model(relative)
        model_hash=p.sha(out/'model.txt')
        p.dump_new(out/'checkpoint.json',{'at':p.clock(),'model_sha256':model_hash,'fit_seconds':seconds,'features':features,
            'params':conf['lightgbm'],'training_sha256':p.sha(train_path),'slot':slot})
        valid=inf.input_status.eq('OK')
        if valid.any():
            raw[valid]=model.predict(inf.loc[valid,features]);assert np.isfinite(raw[valid]).all()
            values[valid]=np.maximum(raw[valid],0)
    result=inf[['shop','spu','input_status']].copy();result['prediction_raw']=raw;result['prediction']=values
    result['output_status']=np.where(np.isfinite(values),'OK',inf.input_status if allowed else 'MODEL_SUPPORT_INSUFFICIENT')
    result.to_parquet(out/'predictions.parquet',index=False)
    p.dump_new(out/'result.json',{'slot':slot,'started':started,'finished':p.clock(),'fit_attempted':bool(allowed),
        'fit_seconds':seconds,'output_n':int(np.isfinite(values).sum()),'status':'FITTED' if allowed else 'SKIPPED_SUPPORT',
        'model_sha256':model_hash,'prediction_sha256':p.sha(out/'predictions.parquet')})
    print(json.dumps({'slot':slot,'fit_attempted':bool(allowed),'fit_seconds':seconds,'output_n':int(np.isfinite(values).sum())}),flush=True)

def assemble(root):
    root=pathlib.Path(root).resolve();prep=root/'prepared';conf=p.read(prep/'config.json');same_business_day(conf['issue'])
    p.verify_code();p.verify_sources(conf['sources']);core=pd.read_parquet(prep/'core.parquet')
    if p.sha(prep/'core.parquet')!=conf['core_sha256']:raise ValueError('CORE_CHANGED')
    preds={};support={};mats={};results=[]
    for seg in ['M1','M2']:
        meta=p.read(prep/seg/'manifest.json');support[seg]=meta['annual_support']
        mats[seg]=(pd.read_parquet(prep/seg/'annual_inference.parquet'),pd.read_parquet(prep/seg/'inference_annual_windows.parquet'))
        for prefix in ['direct','annual']:
            slot=root/'effects'/(prefix+'_'+seg);r=p.read(slot/'result.json')
            if p.sha(slot/'predictions.parquet')!=r['prediction_sha256']:raise ValueError('PREDICTION_CHANGED')
            df=pd.read_parquet(slot/'predictions.parquet');pd.testing.assert_frame_equal(df[['shop','spu']],core[['shop','spu']],check_dtype=False)
            preds[prefix+'_'+seg]=df.prediction.to_numpy();results.append(r)
    wide=p.assemble(core,conf['issue'],mats,preds,support)
    baseline=pd.read_parquet(prep/'f28.parquet').pivot(index=['shop','spu'],columns='window',values='prediction')
    idx=pd.MultiIndex.from_frame(core[['shop','spu']])
    wide['f28_M1']=baseline.loc[idx,'d01_30'].to_numpy();wide['f28_M2']=baseline.loc[idx,'d31_60'].to_numpy()
    wide['f28_60']=wide.f28_M1+wide.f28_M2
    rows=[]
    for prefix,name in [('f28','F28'),('direct','DIRECT30x2'),('annual','YEARAGO_COMPONENT'),('blend','DIRECT_YEARAGO_BLEND50')]:
        for seg,win in [('M1','d01_30'),('M2','d31_60'),('60','d01_60')]:
            a,b=direct.WINDOWS[win]
            for i,r in enumerate(core.itertuples()):
                v=wide.iloc[i][prefix+'_'+seg]
                reason='OK' if np.isfinite(v) else wide.iloc[i]['input_status_M1'] if prefix!='annual' else 'ANNUAL_SUPPORT_OR_INPUT_MISSING'
                rows.append({'issue_date':conf['issue'],'input_cutoff':conf['cutoff'],'shop':r.shop,'spu':r.spu,
                    'model':name,'window':win,'target_start':str((pd.Timestamp(conf['issue'])+pd.Timedelta(days=a)).date()),
                    'target_end':str((pd.Timestamp(conf['issue'])+pd.Timedelta(days=b)).date()),'prediction':v,'prediction_status':reason})
    out=root/'issued';out.mkdir(exist_ok=False)
    wide.to_parquet(out/'routing_predictions.parquet',index=False);wide.to_csv(out/'routing_predictions.csv',index=False,encoding='utf-8-sig')
    long=pd.DataFrame(rows);long.to_parquet(out/'predictions.parquet',index=False)
    long.to_csv(out/'predictions.csv',index=False,encoding='utf-8-sig')
    p.dump_new(out/'calculation_receipt.json',{'state':'CALCULATED_NOT_SEALED','at':p.clock(),'core_n':len(core),
        'formal_fits':sum(r['fit_attempted'] for r in results),'slots':results,'gate_true':int(wide.gate_decision.sum()),
        'config_sha256':p.sha(prep/'config.json'),'prediction_sha256':p.sha(out/'predictions.parquet')})
    print(json.dumps({'calculated':True,'core_n':len(core),'gate_true':int(wide.gate_decision.sum())}),flush=True)

def seal(root):
    root=pathlib.Path(root).resolve();conf=p.read(root/'prepared/config.json');same_business_day(conf['issue'])
    v=p.read(root/'issued/independent_validation.json')
    if v['status']!='PASS':raise ValueError('INDEPENDENT_CHECK_REQUIRED')
    p.verify_code();p.verify_sources(conf['sources']);at=p.clock();p.verify_seal_time(conf['issue'],at['utc'])
    files={str(f.relative_to(root)):p.sha(f) for folder in ['inputs','prepared','effects','issued','source'] for f in (root/folder).rglob('*') if f.is_file()}
    p.dump_new(root/'issued/issue_seal.json',{'status':'ISSUED_SHADOW_AWAITING_ACTUALS','sealed_at':at,
        'issue_date':conf['issue'],'input_cutoff':conf['cutoff'],'windows':conf['windows'],'files':files,
        'source_manifest_sha256':p.sha(root/'inputs/manifest.json'),'code_commit':conf['code_commit'],
        'adapter_sha256':conf['adapter_sha256'],'description':conf['description'],
        'future_actuals_available':False,'current_accuracy_computed':False,'production_writes':0,'api_calls':0})
    print(json.dumps({'status':'ISSUED_SHADOW_AWAITING_ACTUALS','sealed_at':at,'issue_date':conf['issue']},ensure_ascii=False))

if __name__=='__main__':
    q=argparse.ArgumentParser();q.add_argument('command',choices=['prepare','fit','assemble','seal']);q.add_argument('--root',required=True)
    q.add_argument('--workspace');q.add_argument('--slot',choices=SLOTS);a=q.parse_args()
    if a.command=='prepare':prepare(a.root,a.workspace)
    elif a.command=='fit':fit(a.root,a.slot)
    elif a.command=='assemble':assemble(a.root)
    else:seal(a.root)
