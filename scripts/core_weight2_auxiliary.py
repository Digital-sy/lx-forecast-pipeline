"""Frozen pre-issue auxiliary pool and saved-model risk evaluation; no fitting."""
from __future__ import annotations
import argparse, datetime as dt, hashlib, json, os
from pathlib import Path
import numpy as np
import pandas as pd

try:
    from scripts import bc60_v3_direct as direct
    from scripts import direct_training_alignment as alignment
except ModuleNotFoundError:
    import bc60_v3_direct as direct
    import direct_training_alignment as alignment

ROOT=Path(__file__).resolve().parents[1]
SHOPS=['JQ-US','MT-US','RKZ-US','SY-US']
KEY=['issue_date','shop','spu']
CANDIDATE='DIRECT_CORE_WEIGHT2_V1'
WINDOWS=direct.WINDOWS

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')
def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def verify(files):
    for p,h in files.items():
        if sha(p)!=h:raise ValueError('IMMUTABLE_SOURCE_CHANGED:'+p)

def transition(current,previous):
    if current not in {'CORE','NON_CORE','UNKNOWN'} or previous not in {'CORE','NON_CORE','UNKNOWN'}:raise ValueError('BAD_CORE_STATE')
    if 'UNKNOWN' in (current,previous):return 'UNKNOWN_TRANSITION'
    if current=='NON_CORE' and previous=='CORE':return 'EXITED_CORE'
    if current=='CORE' and previous=='NON_CORE':return 'NEW_CORE'
    return 'STABLE_CORE' if current=='CORE' else 'STABLE_NON_CORE'

def state_at(lookup,dates,key,c):
    if key not in lookup:raise ValueError('CORE_IDENTITY_JOIN_FAILED:'+repr(key))
    i=(pd.Timestamp(c)-dates[0]).days
    if i<0 or i>=len(dates):return 'UNKNOWN'
    return ['NON_CORE','CORE','UNKNOWN'][lookup[key][0][i]]

def prepare(root,out):
    root,out=Path(root).resolve(),Path(out).resolve()
    if root not in out.parents or not out.is_dir():raise ValueError('EXPERIMENT_OUTPUT_REQUIRED')
    dest=out/'auxiliary_prepared'
    if dest.exists():raise ValueError('REFUSE_AUXILIARY_OVERWRITE')
    archive=root/'bc60_monthly_replay_20261020';item=root/'direct_item_id_v1_20261028'
    sources={};started=now()
    def source(p,expected=None):
        p=Path(p).resolve();h=sha(p)
        if expected is not None and h!=expected:raise ValueError('SOURCE_HASH_MISMATCH:'+str(p))
        sources[str(p)]=h;return p
    im=read(source(archive/'inputs/input_manifest.json'))
    grid=pd.read_csv(source(archive/'prepared/issue_grid.csv'));grid=grid[grid.calendar_mature.eq(True)]
    if len(grid)!=20:raise ValueError('ISSUE_SCOPE_CHANGED')
    daily_path=source(archive/'inputs/daily.parquet',im['files']['daily.parquet'])
    # Membership construction reads only dates which can be an issue's past.
    maxcut=pd.to_datetime(grid.input_cutoff).max()
    d=pd.read_parquet(daily_path,columns=['ds','shop','spu','y','sales_state','mapped'],filters=[('ds','<=',maxcut)])
    m=pd.read_parquet(source(archive/'inputs/monthly_reference.parquet',im['files']['monthly_reference.parquet']))
    if d.duplicated(['shop','spu','ds']).any():raise ValueError('DUPLICATE_DAILY')
    core=pd.read_parquet(source(archive/'prepared/core.parquet'));core=core[core.issue_date.isin(grid.issue_date)]
    known_core=set(core[KEY].itertuples(index=False,name=None))
    item_manifest=read(source(item/'delivery_manifest.json'))
    dates,lookup,_=alignment.historical_core(d)
    dest.mkdir();(dest/'inference').mkdir();universes=[];reconciliations=[]
    for issue in grid.issue_date:
        cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2)
        ctx=direct.Context(d,m,SHOPS,cutoff)
        rows={seg:[] for seg in ['M1','M2']};universe=[]
        for key in sorted(ctx.data):
            current=state_at(lookup,dates,key,cutoff);previous=state_at(lookup,dates,key,cutoff-pd.Timedelta(days=30))
            a=ctx.row(key,issue,'M1',issue);b=ctx.row(key,issue,'M2',issue)
            if a['input_status']!=b['input_status']:raise ValueError('INPUT_QUALIFICATION_SEGMENT_DISAGREEMENT')
            eligible=a['input_status']=='OK'
            record=dict(issue_date=issue,input_cutoff=str(cutoff.date()),previous_identity_cutoff=str((cutoff-pd.Timedelta(days=30)).date()),
                shop=key[0],spu=key[1],core_state=current,previous_core_state=previous,transition=transition(current,previous),
                input_status=a['input_status'],eligible=eligible,positive_completed_months=a['positive_completed_months'],
                in_fixed_main_core=(issue,key[0],key[1]) in known_core)
            universe.append(record)
            if eligible:
                for seg,row in [('M1',a),('M2',b)]:
                    row.update(core_state=current,previous_core_state=previous,transition=record['transition'],in_fixed_main_core=record['in_fixed_main_core'])
                    rows[seg].append(row)
        u=pd.DataFrame(universe);universes.append(u)
        for seg in ['M1','M2']:
            z=pd.DataFrame(rows[seg])
            if z.duplicated(KEY).any():raise ValueError('DUPLICATE_AUXILIARY_KEY')
            oldpath=item/'prepared'/f'{issue}_{seg}'/'inference.parquet'
            old=pd.read_parquet(source(oldpath,item_manifest['files'][f'prepared/{issue}_{seg}/inference.parquet']))
            for col in direct.FEATURES:z[col]=z[col].astype(old[col].dtype)
            old=old[old.input_status.eq('OK')].set_index(['shop','spu'])
            compared=z.set_index(['shop','spu']).reindex(old.index)
            pd.testing.assert_frame_equal(compared[direct.FEATURES],old[direct.FEATURES],check_exact=True,check_names=True)
            if compared[direct.FEATURES].isna().any().any():raise ValueError('MAIN_CORE_AUXILIARY_MISMATCH')
            z.to_parquet(dest/'inference'/f'{issue}_{seg}.parquet',index=False)
            reconciliations.append(dict(issue_date=issue,segment=seg,eligible_pool_n=len(z),main_eligible_compared_n=len(old),
                core_feature_values_exact=True,features=direct.FEATURES))
        print(json.dumps(dict(auxiliary_prepared=issue,observed_mapped_n=len(u),eligible_n=int(u.eligible.sum()),
            eligible_noncore_n=int((u.eligible&u.core_state.eq('NON_CORE')).sum()))),flush=True)
    universe=pd.concat(universes,ignore_index=True);pool=universe[universe.eligible].copy()
    if universe.duplicated(KEY).any() or pool.duplicated(KEY).any():raise ValueError('DUPLICATE_POOL')
    universe.to_parquet(dest/'universe.parquet',index=False);pool.to_parquet(dest/'pool.parquet',index=False)
    for dims,name in [(['issue_date','core_state','input_status'],'universe_coverage'),(['issue_date','core_state','transition'],'pool_coverage')]:
        frame=universe if name=='universe_coverage' else pool
        frame.groupby(dims,dropna=False).size().rename('n').reset_index().to_csv(dest/(name+'.csv'),index=False,encoding='utf-8-sig')
    dump(dest/'feature_reconciliation.json',reconciliations)
    verify(sources)
    dump(dest/'manifest.json',dict(status='POOL_FROZEN_BEFORE_TARGET_SCORING',started_at=started,completed_at=now(),
        sources=sources,script_sha256=sha(__file__),features=direct.FEATURES,issues=list(grid.issue_date),
        observed_mapped_records=len(universe),eligible_records=len(pool),new_fits=0,model_inference_calls=0,
        target_truth_constructed=False,scope='all original-DIRECT-eligible observed mapped objects; risk groups explicit',
        files={str(p.relative_to(dest)):sha(p) for p in dest.rglob('*') if p.is_file()}))

def window_truth(values,known,dates,start,end):
    a=(pd.Timestamp(start)-dates[0]).days;b=(pd.Timestamp(end)-dates[0]).days
    expected=(pd.Timestamp(end)-pd.Timestamp(start)).days+1
    if a<0 or b>=len(dates):return np.nan,'OUTSIDE_ARCHIVE',expected
    valid=known[a:b+1];unknown=int((~valid).sum())
    if unknown:return np.nan,'UNKNOWN_DAYS',unknown
    total=float(values[a:b+1].sum())
    return total,'EXPLICIT_ZERO' if total==0 else 'OBSERVED',0

def summary(g):
    ok=g.actual.notna()&g.prediction.notna();z=g[ok];error=z.prediction-z.actual
    den=float(z.actual.sum());zero=z.actual.eq(0)
    return dict(fixed_n=len(g),output_n=int(g.prediction.notna().sum()),truth_n=int(g.actual.notna().sum()),scored_n=len(z),
        unknown_truth_n=int(g.actual.isna().sum()),actual_units=den,prediction_units=float(z.prediction.sum()),
        over_units=float(error.clip(lower=0).sum()),under_units=float((-error).clip(lower=0).sum()),
        absolute_error=float(error.abs().sum()),wape=float(error.abs().sum()/den) if den>0 else None,
        over_rate=float(error.clip(lower=0).sum()/den) if den>0 else None,
        zero_actual_n=int(zero.sum()),zero_actual_prediction_units=float(z.loc[zero,'prediction'].sum()),
        zero_actual_absolute_error=float(error[zero].abs().sum()))

def evaluate(root,out):
    """Inference is allowed only after the pool and primary candidate are sealed."""
    from lightgbm import Booster
    root,out=Path(root).resolve(),Path(out).resolve();prepared=out/'auxiliary_prepared';dest=out/'auxiliary_risk'
    # LightGBM's native Windows loader is given ASCII relative checkpoint paths.
    # Resolve CLI paths first, then use the same explicit cwd as the fit workers.
    os.chdir(ROOT)
    if dest.exists():raise ValueError('REFUSE_RISK_OVERWRITE')
    pm=read(prepared/'manifest.json');verify(pm['sources']);verify({str(prepared/k):h for k,h in pm['files'].items()})
    effectspath=out/'effects_manifest.json'
    if not effectspath.exists():raise ValueError('PRIMARY_EFFECTS_NOT_SEALED')
    effects=read(effectspath);effect_files={k.replace('\\','/'):h for k,h in effects.get('files',{}).items()};archive=root/'bc60_monthly_replay_20261020'
    oldmanifest=read(archive/'effects/run_manifest.json');oldfiles={k.replace('\\','/'):h for k,h in oldmanifest['files'].items()}
    pool=pd.read_parquet(prepared/'pool.parquet');predictions=[];calls=[];sources={str(effectspath):sha(effectspath),str(prepared/'manifest.json'):sha(prepared/'manifest.json')}
    dest.mkdir();(dest/'inference').mkdir();started=now()
    for issue in pm['issues']:
        for seg in ['M1','M2']:
            ip=prepared/'inference'/f'{issue}_{seg}.parquet';inf=pd.read_parquet(ip)
            for model in [CANDIDATE,'DIRECT']:
                status='OK'
                if model=='DIRECT':
                    path=archive/'effects/slots'/f'{issue}_{seg}'/'model.txt';expected=oldfiles.get(f'slots/{issue}_{seg}/model.txt')
                else:
                    path=out/'effects'/f'{issue}_{seg}'/'model.txt'
                    expected=effect_files.get(path.relative_to(out).as_posix())
                    if path.with_name('checkpoint.json').exists() and expected!=read(path.with_name('checkpoint.json'))['model_sha256']:raise ValueError('CANDIDATE_EFFECTS_HASH')
                if not path.exists() or expected is None:
                    status='MISSING_CHECKPOINT';values=np.full(len(inf),np.nan)
                else:
                    if sha(path)!=expected:raise ValueError('MODEL_HASH_CHANGED')
                    sources[str(path)]=expected
                    booster=Booster(model_file=os.path.relpath(path,ROOT))
                    if booster.feature_name()!=direct.FEATURES:raise ValueError('MODEL_FEATURE_MISMATCH')
                    # Same original nonnegative postprocessing, no extra weights in scoring.
                    values=np.maximum(booster.predict(inf[direct.FEATURES]),0) if len(inf) else np.array([],float)
                    if not np.isfinite(values).all():raise ValueError('INVALID_AUXILIARY_OUTPUT')
                frame=inf[KEY+['segment']].copy();frame['model']=model;frame['prediction']=values;frame['prediction_status']=status
                frame.to_parquet(dest/'inference'/f'{issue}_{seg}_{model}.parquet',index=False)
                predictions.append(frame);calls.append(dict(issue_date=issue,segment=seg,model=model,inference_calls=int(len(inf)>0 and status=='OK'),
                    predicted_rows=len(inf) if status=='OK' else 0,fixed_n=len(inf),prediction_status=status,model_sha256=expected,input_sha256=sha(ip),completed_at=now()))
    pd.DataFrame(calls).to_csv(dest/'inference_calls.csv',index=False,encoding='utf-8-sig')
    segments=pd.concat(predictions,ignore_index=True)
    if segments.duplicated(KEY+['segment','model']).any():raise ValueError('DUPLICATE_AUXILIARY_PREDICTION')
    # All membership and model outputs are frozen before the target truth is read.
    dump(dest/'prediction_seal.json',dict(sealed_at=now(),pool_manifest_sha256=sha(prepared/'manifest.json'),
        inference_calls=int(pd.DataFrame(calls).inference_calls.sum()),new_fits=0,
        files={str(p.relative_to(dest)):sha(p) for p in (dest/'inference').glob('*.parquet')}))
    daily_path=archive/'inputs/daily.parquet'
    if sha(daily_path)!=pm['sources'][str(daily_path.resolve())]:raise ValueError('TRUTH_VERSION_CHANGED')
    d=pd.read_parquet(daily_path,columns=['ds','shop','spu','y','sales_state','mapped']);dates=pd.date_range(d.ds.min(),d.ds.max())
    lookup={}
    for key,g in d.groupby(['shop','spu']):
        z=g.set_index('ds').reindex(dates);values=z.y.to_numpy(float)
        known=np.isfinite(values)&(values>=0)&z.sales_state.isin(direct.VALID).to_numpy()&z.mapped.eq(True).to_numpy()
        lookup[key]=(values,known)
    truth=[]
    for r in pool.itertuples():
        values,known=lookup[(r.shop,r.spu)]
        for window,(a,b) in WINDOWS.items():
            start=pd.Timestamp(r.issue_date)+pd.Timedelta(days=a);end=pd.Timestamp(r.issue_date)+pd.Timedelta(days=b)
            actual,status,unknown=window_truth(values,known,dates,start,end)
            truth.append(dict(issue_date=r.issue_date,shop=r.shop,spu=r.spu,window=window,actual=actual,truth_status=status,
                unknown_days=unknown,target_start=str(start.date()),target_end=str(end.date())))
    truth=pd.DataFrame(truth);truth.to_parquet(dest/'truth.parquet',index=False)
    wide=segments.pivot(index=KEY+['model'],columns='segment',values='prediction').reset_index()
    windows=[]
    for w,cols in [('d01_30',['M1']),('d31_60',['M2']),('d01_60',['M1','M2'])]:
        frame=wide[KEY+['model']].copy();frame['window']=w;frame['prediction']=wide[cols].sum(axis=1,min_count=len(cols));windows.append(frame)
    scored=pd.concat(windows,ignore_index=True).merge(truth,on=KEY+['window'],validate='many_to_one')
    scored=scored.merge(pool,on=KEY,validate='many_to_one')
    if len(scored)!=len(pool)*2*3:raise ValueError('RISK_JOIN_EXPANSION')
    scored['error']=scored.prediction-scored.actual
    scored.to_parquet(dest/'details.parquet',index=False)
    groups={'ALL_ELIGIBLE':np.ones(len(scored),bool),'CURRENT_NON_CORE':scored.core_state.eq('NON_CORE'),
        'CURRENT_CORE':scored.core_state.eq('CORE'),'EXITED_CORE':scored.transition.eq('EXITED_CORE'),
        'NEW_CORE':scored.transition.eq('NEW_CORE'),'UNKNOWN_CORE_OR_TRANSITION':scored.core_state.eq('UNKNOWN')|scored.previous_core_state.eq('UNKNOWN')}
    rows=[]
    for scope,mask in groups.items():
        for model in [CANDIDATE,'DIRECT']:
            for window in WINDOWS:
                g=scored[mask&scored.model.eq(model)&scored.window.eq(window)]
                rows.append(dict(scope=scope,model=model,window=window,**summary(g)))
    metrics=pd.DataFrame(rows);metrics.to_csv(dest/'metrics.csv',index=False,encoding='utf-8-sig')
    byperiod=[]
    for scope,mask in groups.items():
        for (issue,model,window),g in scored[mask].groupby(['issue_date','model','window']):byperiod.append(dict(scope=scope,issue_date=issue,model=model,window=window,**summary(g)))
    pd.DataFrame(byperiod).to_csv(dest/'period_metrics.csv',index=False,encoding='utf-8-sig')
    verify(sources);verify(pm['sources'])
    missing=int(pd.DataFrame(calls).prediction_status.ne('OK').sum())
    dump(dest/'manifest.json',dict(status='PARTIAL_MISSING_CHECKPOINT' if missing else 'EVALUATED_RECONSTRUCTED_HISTORY',missing_checkpoint_units=missing,started_at=started,completed_at=now(),
        sources=sources,script_sha256=sha(__file__),pool_manifest_sha256=sha(prepared/'manifest.json'),
        new_fits=0,inference_calls=int(pd.DataFrame(calls).inference_calls.sum()),predicted_segment_rows=int(pd.DataFrame(calls).predicted_rows.sum()),
        pool_records=len(pool),scoring_weights='NONE; equal original business metric definitions',
        scope='auxiliary risk only; does not replace fixed primary 2820/2579',
        files={str(p.relative_to(dest)):sha(p) for p in dest.rglob('*') if p.is_file()}))
    print(json.dumps(dict(status='AUXILIARY_COMPLETE',pool_records=len(pool),inference_calls=int(pd.DataFrame(calls).inference_calls.sum()))),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='action',required=True)
    for name in ['prepare','evaluate']:
        s=sub.add_parser(name);s.add_argument('--root',required=True);s.add_argument('--out',required=True)
    args=vars(p.parse_args());action=args.pop('action');{'prepare':prepare,'evaluate':evaluate}[action](**args)
