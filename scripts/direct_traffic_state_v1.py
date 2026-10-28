"""One local direct traffic-state increment. No network or source-system clients."""
from __future__ import annotations
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import pandas as pd
try:
    from scripts import bc60_monthly_replay as frozen, bc60_v3_direct as direct
except ModuleNotFoundError:
    import bc60_monthly_replay as frozen, bc60_v3_direct as direct

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE = 'DIRECT_TRAFFIC_STATE_V1'
PREREG = 'dd20100219aa92a3ef2cae2692f1a2ce86887f66'
NUMERIC = ['traffic_t7','traffic_t28','yield_r7','yield_r28','traffic_change','yield_change']
FLAGS = [x+'_valid' for x in NUMERIC]
EXTRA = NUMERIC + FLAGS
FEATURES = direct.FEATURES + EXTRA
KEY = ['shop','spu','simulated_issue_date']
AUDIT = ['shop','spu','simulated_issue_date','input_cutoff','label_start','label_end','y_label',*direct.FEATURES]
RAW = ['ds','shop','spu','y','mapped','sales_state','listing_sessions_sum','null_sessions',
       'null_qty','negative_qty','date_mismatch','deleted_rows','source_rows']
dump, read, now = frozen.dump, frozen.read_json, frozen.now


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()


def verify(files):
    for p,h in files.items():
        if sha(p)!=h:raise ValueError('FROZEN_SOURCE_CHANGED:'+p)


class TrafficIndex:
    """Prefix sums plus invalid-day counts at exact source calendar positions."""
    def __init__(self, daily):
        if not set(RAW).issubset(daily):raise ValueError('MISSING_TRAFFIC_SCHEMA')
        d=daily[RAW].copy()
        d['ds']=pd.to_datetime(d.ds)
        if d.duplicated(['shop','spu','ds']).any():raise ValueError('DUPLICATE_OBJECT_DAY')
        if not d.ds.eq(d.ds.dt.normalize()).all():raise ValueError('NON_DAILY_TIMESTAMP')
        self.first=d.ds.min();self.last=d.ds.max();self.dates=pd.date_range(self.first,self.last)
        self.data={}
        for key,g in d.groupby(['shop','spu'],sort=True):
            z=g.set_index('ds').reindex(self.dates)
            sessions=z.listing_sessions_sum.to_numpy(dtype=float)
            sales=z.y.to_numpy(dtype=float)
            common=(z.mapped.eq(True)&z.source_rows.gt(0)&z.date_mismatch.eq(0)&z.deleted_rows.eq(0)).to_numpy()
            valid_t=common & np.isfinite(sessions) & (sessions>=0) & z.null_sessions.eq(0).to_numpy()
            valid_y=common & np.isfinite(sales) & (sales>=0) & z.sales_state.isin(direct.VALID).to_numpy() & z.null_qty.eq(0).to_numpy() & z.negative_qty.eq(0).to_numpy()
            self.data[key]=(sessions,sales,valid_t,valid_y,
                np.r_[0.,np.cumsum(np.where(valid_t,sessions,0.))],
                np.r_[0.,np.cumsum(np.where(valid_y,sales,0.))],
                np.r_[0,np.cumsum(~valid_t)],np.r_[0,np.cumsum(~valid_y)])

    def at(self,key,cutoffs):
        cutoffs=pd.DatetimeIndex(cutoffs);n=len(cutoffs)
        out={c:np.full(n,np.nan) for c in NUMERIC}
        audit={}
        end=(cutoffs-self.first).days.to_numpy()+1
        for width in [7,28]:
            sess=np.full(n,np.nan);sales=np.full(n,np.nan)
            okt=np.zeros(n,dtype=bool);oky=np.zeros(n,dtype=bool)
            if key in self.data:
                sv,yv,st,sy,pt,py,bt,by=self.data[key]
                left=end-width;inside=(left>=0)&(end<=len(sv))
                ix=np.flatnonzero(inside)
                okt[ix]=bt[end[ix]]==bt[left[ix]];oky[ix]=by[end[ix]]==by[left[ix]]
                it=np.flatnonzero(okt);iy=np.flatnonzero(oky)
                sess[it]=pt[end[it]]-pt[left[it]];sales[iy]=py[end[iy]]-py[left[iy]]
            out['traffic_t'+str(width)]=sess/width
            okr=okt & oky & (sess>0)
            out['yield_r'+str(width)][okr]=sales[okr]/sess[okr]
            audit['sessions_sum'+str(width)]=sess
            audit['sales_sum'+str(width)]=sales
            audit['sessions_state'+str(width)]=np.where(okt,'KNOWN','UNKNOWN_MISSING_INVALID_OR_MAPPING')
            audit['yield_state'+str(width)]=np.select([~okt,~oky,sess==0],
                ['UNKNOWN_SESSIONS','UNKNOWN_SALES','ZERO_SESSIONS_UNDEFINED'],default='DEFINED')
        for name,a,b in [('traffic_change','traffic_t7','traffic_t28'),('yield_change','yield_r7','yield_r28')]:
            x,y=out[a],out[b];den=x+y;ok=np.isfinite(x)&np.isfinite(y)&(den>0)
            out[name][ok]=(x[ok]-y[ok])/den[ok]
            audit[name+'_state']=np.select([~np.isfinite(x)|~np.isfinite(y),den==0],
                ['UNKNOWN_COMPONENT','ZERO_DENOMINATOR_UNDEFINED'],default='DEFINED')
        for c in NUMERIC:out[c+'_valid']=np.isfinite(out[c]).astype(np.int8)
        return pd.DataFrame(out),pd.DataFrame(audit)


def augment(index, frame):
    out=frame.copy(deep=True)
    if set(EXTRA)&set(frame):raise ValueError('FEATURE_OVERWRITE')
    for c in EXTRA:out[c]=np.int8(0) if c in FLAGS else np.nan
    audits=[]
    for key,g in frame.groupby(['shop','spu'],sort=False):
        f,a=index.at(key,g.input_cutoff)
        for c in EXTRA:out.loc[g.index,c]=f[c].to_numpy()
        a.index=g.index;audits.append(a)
    pd.testing.assert_frame_equal(out[frame.columns],frame,check_exact=True)
    return out,pd.concat(audits).sort_index() if audits else pd.DataFrame(index=frame.index)


def supported(frame):
    """No effect or truth-based support selection; no variable numeric info => skip."""
    return [c for c in NUMERIC if frame[c].dropna().nunique()>1]


def independent_check(index, frame, limit=64):
    take=np.unique(np.linspace(0,len(frame)-1,min(limit,len(frame))).astype(int)) if len(frame) else []
    for r in frame.iloc[take].itertuples():
        key=(r.shop,r.spu);c=pd.Timestamp(r.input_cutoff);expected={}
        for width in [7,28]:
            dates=pd.date_range(c-pd.Timedelta(days=width-1),c)
            sess=pd.Series(np.nan,index=dates);sales=sess.copy()
            if key in index.data:
                sv,yv,st,sy,*_=index.data[key]
                sess=pd.Series(np.where(st,sv,np.nan),index=index.dates).reindex(dates)
                sales=pd.Series(np.where(sy,yv,np.nan),index=index.dates).reindex(dates)
            known=sess.notna().all()
            expected['traffic_t'+str(width)]=sess.sum()/width if known else np.nan
            expected['yield_r'+str(width)]=sales.sum()/sess.sum() if known and sales.notna().all() and sess.sum()>0 else np.nan
        for name,a,b in [('traffic_change','traffic_t7','traffic_t28'),('yield_change','yield_r7','yield_r28')]:
            x,y=expected[a],expected[b]
            expected[name]=(x-y)/(x+y) if np.isfinite(x) and np.isfinite(y) and x+y>0 else np.nan
        for col,val in expected.items():
            assert np.isclose(getattr(r,col),val,rtol=1e-12,atol=1e-12,equal_nan=True),(key,c,col)
            assert getattr(r,col+'_valid')==int(np.isfinite(val))
    return len(take)


def prepare(root,out,tests):
    root,out,tests=map(lambda p:Path(p).resolve(),[root,out,tests])
    if out.exists() or root not in out.parents:raise ValueError('NEW_EXCLUSIVE_OUTPUT_REQUIRED')
    if read(tests)['status']!='PASS':raise ValueError('SYNTHETIC_TEST_FAILURE')
    archive=root/'bc60_monthly_replay_20261020'
    conf=read(archive/'prepared/frozen_config.json');pm=read(archive/'prepared/prepare_manifest.json')
    if sha(ROOT/'scripts/bc60_monthly_replay.py')!=pm['script_sha256']:raise ValueError('FROZEN_EXECUTOR_CHANGED')
    verify({str(ROOT/p):h for p,h in conf['source_hashes'].items()})
    verify(pm['sources']);verify({str(archive/'prepared'/p):h for p,h in pm['files'].items()})
    im=read(archive/'inputs/input_manifest.json')
    verify({str(archive/'inputs'/p):h for p,h in im['files'].items()})
    runtime={p:importlib.metadata.version(p) for p in conf['runtime']}
    if runtime!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    comp=root/'core_model_complementarity_20261028_v1'
    cm=read(comp/'manifest.json');verify(cm['sources']);verify(cm['protected_shadow_files'])
    verify({str(comp/p):h for p,h in cm['files'].items()})
    sources=dict(cm['sources']);sources.update(cm['protected_shadow_files'])
    for p in [archive/'prepared/prepare_manifest.json',archive/'prepared/frozen_config.json',
              archive/'prepared/core.parquet',archive/'prepared/issue_grid.csv',archive/'inputs/daily.parquet',
              archive/'inputs/monthly_reference.parquet',archive/'inputs/input_manifest.json',archive/'inputs/quality.json',
              root/'bc60_experiment_20261019/inputs_v1_final/quality.json',comp/'manifest.json',
              comp/'objects.parquet',comp/'all_miss_directions_detail.csv',comp/'top20_products.csv',
              ROOT/'scripts/bc60_monthly_replay.py',ROOT/'scripts/bc60_v3_direct.py',ROOT/'scripts/bc60_monthly_acquire.py',
              ROOT/'docs/direct_traffic_state_v1_protocol.md',tests]:sources[str(p)]=sha(p)
    old_manifest=read(archive/'effects/run_manifest.json')
    grid=pd.read_csv(archive/'prepared/issue_grid.csv');grid=grid[grid.calendar_mature.eq(True)]
    if len(grid)!=20 or len(grid)*2>40:raise ValueError('ISSUE_SCOPE_OR_FIT_BUDGET')
    core=pd.read_parquet(archive/'prepared/core.parquet');core=core[core.issue_date.isin(grid.issue_date)]
    if len(core)!=2820:raise ValueError('CORE_CHANGED')
    for issue in grid.issue_date:
        for segment in ['M1','M2']:
            prior=archive/'effects/slots'/f'{issue}_{segment}'
            for name in ['training_audit.json','checkpoint.json','segment_predictions.parquet','model.txt']:
                p=prior/name;h=sha(p)
                if h!=old_manifest['files'][str(p.relative_to(archive/'effects'))]:raise ValueError('OLD_SLOT_CHANGED')
                sources[str(p)]=h
    d=pd.read_parquet(archive/'inputs/daily.parquet',columns=RAW)
    m=pd.read_parquet(archive/'inputs/monthly_reference.parquet');d,m=frozen.normalize(d,m)
    # Do not provide future feature days even to the index used by earlier cutoffs.
    feature_daily=d[d.ds.le(pd.to_datetime(grid.issue_date).max()-pd.Timedelta(days=2))].copy()
    index=TrafficIndex(feature_daily)
    out.mkdir(exist_ok=False);(out/'prepared').mkdir()
    core.to_parquet(out/'prepared/core.parquet',index=False)
    grid.to_csv(out/'prepared/issue_grid.csv',index=False)
    configuration=dict(candidate=CANDIDATE,preregistration_commit=PREREG,features=FEATURES,original_features=direct.FEATURES,
        extra_features=EXTRA,numeric_features=NUMERIC,lightgbm=conf['original']['lightgbm'],runtime=runtime,
        sample_weight=None,max_formal_fits=40,max_seconds_per_slot=600,archive=str(archive),root=str(root),
        script_sha256=sha(__file__),missing_mechanism='native NaN; unchanged use_missing/zero_as_missing defaults',
        classification='RECONSTRUCTED_HISTORICAL_DEVELOPMENT_NOT_OOS',source_fields=RAW)
    dump(out/'configuration.json',configuration)
    oldq=read(root/'bc60_experiment_20261019/inputs_v1_final/quality.json')
    quality=dict(rows=len(d),first_day=str(d.ds.min().date()),last_day=str(d.ds.max().date()),
        duplicate_object_days=int(d.duplicated(['shop','spu','ds']).sum()),
        sessions_unknown_aggregate=int(d.listing_sessions_sum.isna().sum()),sessions_negative_aggregate=int(d.listing_sessions_sum.lt(0).sum()),
        unknown_null_count=int(d.null_sessions.isna().sum()),null_components=int(d.null_sessions.sum()),
        mapped_rows=int(d.mapped.eq(True).sum()),mapped_sales=float(d.loc[d.mapped.eq(True),'y'].sum()),
        unmapped_sales_retained_in_source=float(d.loc[~d.mapped.eq(True),'y'].sum()),
        mapped_sessions=float(d.loc[d.mapped.eq(True),'listing_sessions_sum'].sum()),new_joins=0,new_aggregation_rows=0,
        historical_acquisition='2026-10-20; see immutable per-partition receipts',
        reused_v1_acquisition_min=min(x['acquired_at'] for x in oldq['raw_receipts']),
        reused_v1_acquisition_max=max(x['acquired_at'] for x in oldq['raw_receipts']),
        source_sync_at=im['source_sync_at'],historical_availability='NOT_PROVEN',
        limitations=['No raw negative-Sessions component count; nonnegative aggregate alone cannot prove component nonnegativity',
                    'Existing ODS SPU mapping reused; historical mapping effective dates not reconstructed',
                    'Sessions summed across source ASIN rows, not deduplicated visitors',
                    'Source acquired in 2026, not proof of historical cutoff availability',
                    'No new source-system queries or raw component reconstruction'])
    dump(out/'source_applicability.json',quality)
    started=now();reconciliation=[];slots=[];featcoverage=[]
    for issue in grid.issue_date:
        cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2);ctx=frozen.VectorContext(d,m,frozen.SHOPS,cutoff)
        selected=core[core.issue_date.eq(issue)]
        for segment in ['M1','M2']:
            slot=out/'prepared'/f'{issue}_{segment}';slot.mkdir(exist_ok=False)
            prior=archive/'effects/slots'/f'{issue}_{segment}'
            train,_=ctx.training(issue,segment)
            audit=frozen.training_audit(train,KEY,AUDIT);old=read(prior/'training_audit.json');cp=read(prior/'checkpoint.json')
            if audit!=old or frozen.frame_hash(train)!=cp['training_sha256']:raise ValueError('OLD_TRAIN_CHANGED:'+str(slot))
            if train.duplicated(KEY).any() or not train.label_end.le(cutoff).all():raise ValueError('TRAIN_KEYS_LABELS')
            if not (train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all():raise ValueError('ROW_CUTOFF')
            full,_=augment(index,train)
            trainchecks=independent_check(index,full)
            infer=pd.DataFrame([ctx.row((r.shop,r.spu),issue,segment,issue) for r in selected.itertuples()])
            oldinf=pd.read_parquet(prior/'segment_predictions.parquet').set_index(['shop','spu'])
            infindex=infer.set_index(['shop','spu'])
            pd.testing.assert_frame_equal(infindex[direct.FEATURES+['input_status']],
                oldinf.loc[infindex.index,direct.FEATURES+['input_status']],check_exact=True,check_dtype=False)
            inf,ia=augment(index,infer);infchecks=independent_check(index,inf,len(inf))
            variable=supported(full)
            full.to_parquet(slot/'train.parquet',index=False);inf.to_parquet(slot/'inference.parquet',index=False)
            pd.concat([inf[['shop','spu','input_cutoff']].reset_index(drop=True),ia.reset_index(drop=True)],axis=1).to_parquet(slot/'feature_audit.parquet',index=False)
            dump(slot/'training_check.json',dict(status='PASS',old_frame_hash=cp['training_sha256'],old_audit_exact=True,
                old_features_exact=True,old_inference_exact=True,weights_unchanged=True,rows_not_dropped=len(full)==len(train),
                train_rows=len(train),train_calendar_checks=trainchecks,inference_calendar_checks=infchecks,
                row_specific_cutoff=True,labels_mature=True,max_label_end=str(train.label_end.max()),
                varying_numeric_features=variable,train_sha256=sha(slot/'train.parquet'),inference_sha256=sha(slot/'inference.parquet')))
            dump(slot/'old_training_audit.json',audit)
            for scope,z in [('TRAIN',full),('INFERENCE',inf),('ELIGIBLE_INFERENCE',inf[inf.input_status.eq('OK')])]:
                for col in NUMERIC:
                    featcoverage.append(dict(issue_date=issue,segment=segment,scope=scope,feature=col,rows=len(z),
                        finite_n=int(z[col].notna().sum()),unknown_n=int(z[col].isna().sum()),distinct_finite=int(z[col].dropna().nunique()),
                        zero_n=int(z[col].eq(0).sum()),greater_than_one_n=int(z[col].gt(1).sum()) if col.startswith('yield_r') else None))
            reconciliation.append(dict(issue_date=issue,segment=segment,train_n=len(train),inference_n=len(inf),
                eligible_n=int(inf.input_status.eq('OK').sum()),old_exact=True,variable_numeric_n=len(variable),
                support='NEW_INFORMATION_SUPPORTED' if variable else 'NO_NEW_INFORMATION_SUPPORTED'))
            slots.append(dict(issue_date=issue,segment=segment,state='PLANNED',has_support=bool(variable)))
            print(json.dumps({'prepared':f'{issue}_{segment}',**reconciliation[-1]}),flush=True)
    pd.DataFrame(reconciliation).to_csv(out/'training_reconciliation.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(featcoverage).to_csv(out/'feature_coverage.csv',index=False,encoding='utf-8-sig')
    verify(sources)
    dump(out/'prepare_manifest.json',dict(started_at=started,completed_at=now(),sources=sources,script_sha256=sha(__file__),
        preregistration_commit=PREREG,formal_fit_count=0,files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}))
    dump(out/'formal_fit_ledger.json',dict(created_at=now(),max_formal_fits=40,slots=slots))
    print(json.dumps({'status':'PREPARED_NO_EFFECTS','slots':len(slots)}),flush=True)


def worker(out,issue,segment,recovery=False):
    from lightgbm import LGBMRegressor,Booster
    out=Path(out).resolve();conf=read(out/'configuration.json');prep=out/'prepared'/f'{issue}_{segment}'
    slot=out/'effects'/f'{issue}_{segment}';slot.mkdir(parents=True,exist_ok=True)
    check=read(prep/'training_check.json')
    verify({str(prep/'train.parquet'):check['train_sha256'],str(prep/'inference.parquet'):check['inference_sha256']})
    train=pd.read_parquet(prep/'train.parquet');inf=pd.read_parquet(prep/'inference.parquet')
    if conf['features']!=FEATURES:raise ValueError('FROZEN_FEATURES')
    if not check['varying_numeric_features']:
        original=pd.read_parquet(Path(conf['archive'])/'effects/slots'/f'{issue}_{segment}'/'segment_predictions.parquet').set_index(['shop','spu'])
        pos=inf.set_index(['shop','spu']).index
        inf['prediction']=original.loc[pos,'prediction'].to_numpy();inf['prediction_raw']=inf.prediction
        inf['prediction_status']=np.where(inf.prediction.notna(),'OK',inf.input_status)
        inf['prediction_origin']='REFERENCE_DIRECT_NO_NEW_INFORMATION'
        result=dict(status='SKIPPED_NO_NEW_INFORMATION',formal_fit=False,model_sha256=None,fit_seconds=0)
    else:
        relative=os.path.relpath(slot/'model.txt',ROOT);relative.encode('ascii')
        if recovery:
            cp=read(slot/'checkpoint.json')
            if sha(slot/'model.txt')!=cp['model_sha256']:raise ValueError('CHECKPOINT_CHANGED')
            model=Booster(model_file=relative)
        else:
            with (slot/'fit_started.json').open('x',encoding='utf-8') as f:
                json.dump(dict(at=now(),issue_date=issue,segment=segment,train_sha256=check['train_sha256'],features=FEATURES),f)
            model=LGBMRegressor(**conf['lightgbm']);tic=time.monotonic()
            model.fit(train[FEATURES],train.y_label);seconds=time.monotonic()-tic
            model.booster_.save_model(relative)
            cp=dict(completed_at=now(),fit_seconds=seconds,model_sha256=sha(slot/'model.txt'),
                train_sha256=check['train_sha256'],features=FEATURES,params=conf['lightgbm'],sample_weight=None,
                model_features=model.booster_.feature_name())
            dump(slot/'checkpoint.json',cp)
        valid=inf.input_status.eq('OK');pred=np.full(len(inf),np.nan)
        if valid.any():pred[valid]=model.predict(inf.loc[valid,FEATURES])
        if not np.isfinite(pred[valid]).all():raise ValueError('NONFINITE_OUTPUT')
        inf['prediction_raw']=pred;inf['prediction']=np.maximum(pred,0)
        inf['prediction_status']=np.where(valid,'OK',inf.input_status)
        inf['prediction_origin']=np.where(valid,'NEW_MODEL','NO_OUTPUT_ORIGINAL_QUALIFICATION')
        result=dict(status='RECOVERED' if recovery else 'FITTED',formal_fit=not recovery,model_sha256=cp['model_sha256'],fit_seconds=cp['fit_seconds'])
    dest=slot/'predictions.parquet'
    if dest.exists():raise ValueError('OUTPUT_EXISTS')
    inf.to_parquet(dest,index=False)
    result.update(issue_date=issue,segment=segment,completed_at=now(),n=len(inf),output_n=int(inf.prediction.notna().sum()),
        new_model_output_n=int((inf.prediction_origin.eq('NEW_MODEL')&inf.prediction.notna()).sum()),
        referenced_output_n=int((inf.prediction_origin.eq('REFERENCE_DIRECT_NO_NEW_INFORMATION')&inf.prediction.notna()).sum()),
        prediction_sha256=sha(dest))
    dump(slot/'result.json',result)


def fit(out):
    out=Path(out).resolve();p=read(out/'prepare_manifest.json');conf=read(out/'configuration.json')
    if sha(__file__)!=p['script_sha256'] or sha(__file__)!=conf['script_sha256']:raise ValueError('EXECUTOR_CHANGED')
    verify(p['sources']);verify({str(out/rel):h for rel,h in p['files'].items()})
    if {x:importlib.metadata.version(x) for x in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    ledgerpath=out/'formal_fit_ledger.json';ledger=read(ledgerpath)
    if len(ledger['slots'])>40:raise ValueError('FIT_BUDGET')
    started=now()
    for entry in ledger['slots']:
        issue,segment=entry['issue_date'],entry['segment'];slot=out/'effects'/f'{issue}_{segment}'
        if (slot/'result.json').exists():
            r=read(slot/'result.json')
            if sha(slot/'predictions.parquet')!=r['prediction_sha256']:raise ValueError('COMPLETED_OUTPUT_CHANGED')
            if r['model_sha256'] and sha(slot/'model.txt')!=r['model_sha256']:raise ValueError('COMPLETED_MODEL_CHANGED')
            entry.update(state=r['status'],result=r);dump(ledgerpath,ledger);continue
        recovery=(slot/'checkpoint.json').exists()
        if (slot/'fit_started.json').exists() and not recovery:raise ValueError('NO_REFIT_WITHOUT_CHECKPOINT')
        if entry['state']!='PLANNED' and not recovery:raise ValueError('RESERVED_SLOT_NO_RETRY')
        entry.update(state='RECOVERING' if recovery else 'RESERVED',reserved_at=now());dump(ledgerpath,ledger)
        command=[sys.executable,'-B',str(Path(__file__).resolve()),'_worker','--out',str(out),'--issue',issue,'--segment',segment]
        if recovery:command.append('--recovery')
        try:
            proc=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=600,
                env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1','PYTHONIOENCODING':'utf-8'})
            log=out/'effects'/f'{issue}_{segment}{".recovery" if recovery else ""}.log'
            log.parent.mkdir(exist_ok=True)
            with log.open('x',encoding='utf-8') as f:f.write(proc.stdout+'\n'+proc.stderr)
            if proc.returncode:raise RuntimeError('WORKER_FAILED:'+str(proc.returncode))
            r=read(slot/'result.json');entry.update(state=r['status'],result=r);dump(ledgerpath,ledger)
            print(json.dumps(r),flush=True)
        except Exception as e:
            entry.update(state='TECHNICAL_FAILURE',error=str(e),stopped_at=now());dump(ledgerpath,ledger);raise
    fits=list((out/'effects').glob('*/fit_started.json'))
    if len(fits)>40:raise ValueError('FIT_BUDGET')
    verify(p['sources'])
    dump(out/'effects_manifest.json',dict(started_at=started,completed_at=now(),formal_fits=len(fits),
        script_sha256=sha(__file__),sources_unchanged=True,prepare_manifest_sha256=sha(out/'prepare_manifest.json'),
        files={str(f.relative_to(out)):sha(f) for f in (out/'effects').rglob('*') if f.is_file()}))
    print(json.dumps({'status':'COMPLETED','formal_fits':len(fits)}),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True)
    q=s.add_parser('prepare');q.add_argument('--root',required=True);q.add_argument('--out',required=True);q.add_argument('--tests',required=True)
    q=s.add_parser('fit');q.add_argument('--out',required=True)
    q=s.add_parser('_worker');q.add_argument('--out',required=True);q.add_argument('--issue',required=True);q.add_argument('--segment',required=True);q.add_argument('--recovery',action='store_true')
    args=vars(p.parse_args());action=args.pop('action');{'prepare':prepare,'fit':fit,'_worker':worker}[action](**args)
