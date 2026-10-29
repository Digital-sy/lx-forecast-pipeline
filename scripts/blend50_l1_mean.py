"""Seal a single fixed arithmetic combination of archived predictions only."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd

CANDIDATE = 'BLEND50_L1_MEAN_V1'
KEY = ['issue_date', 'shop', 'spu']
WINDOWS = ['d01_30', 'd31_60', 'd01_60']
BASE = ['issue_date','shop','spu','input_cutoff']
BLEND_COLUMNS = BASE + ['prediction_status','prediction_M1','prediction_M2','prediction_60'] + [a+'_'+w for w in WINDOWS for a in ['target_start','target_end']]
L1_COLUMNS = BASE + ['segment','label_start','label_end','prediction','prediction_status']
ARCHIVES = ['direct_l1_v1_20261028','direct_yearago_blend50_20261021_v1',
            'bc60_monthly_replay_20261020','core_model_complementarity_20261028_v1']


def now(): return dt.datetime.now(dt.timezone.utc).isoformat()
def read(p): return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p, value):
    with Path(p).open('x', encoding='utf-8') as f:
        json.dump(value, f, ensure_ascii=False, indent=2, allow_nan=False)
def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304), b''): h.update(b)
    return h.hexdigest()
def verify(files):
    for p,h in files.items():
        if sha(p)!=h: raise ValueError('HASH_CHANGED:'+str(p))
def equal(a,b): return np.array_equal(np.asarray(a),np.asarray(b),equal_nan=True)


def mature_core(core):
    """Apply the preregistered dates, not prediction success or later truth."""
    dates=pd.to_datetime(core.issue_date,errors='raise')
    mask=dates.ge('2025-01-01')&dates.lt('2026-09-01')
    included=core.loc[mask].copy();excluded=core.loc[~mask].copy()
    periods=pd.to_datetime(included.issue_date).dt.to_period('M')
    if set(periods)!=set(pd.period_range('2025-01','2026-08',freq='M')): raise ValueError('MATURE_PERIOD_RANGE')
    return included,excluded


def combine(blend, l1):
    """Truth-free whitelist; no learned models, weights, selection or fallback."""
    b=blend.loc[:,BLEND_COLUMNS].copy()
    l=l1.loc[:,L1_COLUMNS].copy()
    if b[KEY].isna().any().any() or l[KEY].isna().any().any(): raise ValueError('NULL_KEY')
    if b.duplicated(KEY).any() or l.duplicated(KEY+['segment']).any(): raise ValueError('DUPLICATE_KEY')
    if not l.segment.isin(['M1','M2']).all(): raise ValueError('SEGMENT_RANGE')
    if not equal(b.prediction_60,b.prediction_M1+b.prediction_M2): raise ValueError('BLEND_SUM_CHANGED')
    result=[]
    for seg,w,lo,hi in [('M1','d01_30',1,30),('M2','d31_60',31,60)]:
        cols=BASE+['prediction_status','prediction_'+seg,'target_start_'+w,'target_end_'+w]
        x=b[cols].rename(columns={'prediction_status':'blend50_status','prediction_'+seg:'blend50_prediction',
            'input_cutoff':'blend_cutoff','target_start_'+w:'target_start','target_end_'+w:'target_end'})
        y=l[l.segment.eq(seg)].rename(columns={'prediction_status':'l1_status','prediction':'l1_prediction','input_cutoff':'l1_cutoff'})
        z=x.merge(y,on=KEY,how='outer',validate='one_to_one',indicator=True)
        if not z['_merge'].eq('both').all(): raise ValueError('ENDPOINT_SCOPE_MISMATCH')
        dates=pd.to_datetime(z.issue_date,errors='raise')
        for a,c in [('target_start','label_start'),('target_end','label_end'),('blend_cutoff','l1_cutoff')]:
            if not pd.to_datetime(z[a]).eq(pd.to_datetime(z[c])).all(): raise ValueError('ENDPOINT_DATE_MISMATCH')
        if not pd.to_datetime(z.target_start).eq(dates+pd.Timedelta(days=lo)).all(): raise ValueError('TARGET_START')
        if not pd.to_datetime(z.target_end).eq(dates+pd.Timedelta(days=hi)).all(): raise ValueError('TARGET_END')
        if not pd.to_datetime(z.blend_cutoff).eq(dates-pd.Timedelta(days=2)).all(): raise ValueError('CUTOFF')
        for p,s in [('blend50_prediction','blend50_status'),('l1_prediction','l1_status')]:
            vals=z[p].to_numpy(float)
            if np.isinf(vals).any() or (vals<0).any(): raise ValueError('NONFINAL_PREDICTION')
            if not np.array_equal(np.isfinite(vals),z[s].eq('OK').to_numpy()): raise ValueError('STATUS_CONFLICT')
        if not z.blend50_prediction.notna().eq(z.l1_prediction.notna()).all(): raise ValueError('OUTPUT_MASK_MISMATCH')
        z['prediction']=0.5*z.blend50_prediction+0.5*z.l1_prediction
        z['prediction_status']=np.where(z.prediction.notna(),'OK','ORIGINAL_MISSING_OUTPUT')
        z['input_cutoff']=pd.to_datetime(z.blend_cutoff);z['window']=w
        z=z[KEY+['window','input_cutoff','target_start','target_end','blend50_prediction','l1_prediction','prediction','prediction_status','blend50_status','l1_status']]
        result.append(z)
    first=result[0].set_index(KEY);second=result[1].set_index(KEY).reindex(first.index)
    if not first.prediction.notna().eq(second.prediction.notna()).all(): raise ValueError('PARTIAL_SEGMENT_OUTPUT')
    sixty=first.copy();sixty['window']='d01_60';sixty['target_end']=second.target_end
    for c in ['blend50_prediction','l1_prediction','prediction']: sixty[c]=first[c]+second[c]
    result.append(sixty.reset_index())
    candidate=pd.concat(result,ignore_index=True).sort_values(KEY+['window']).reset_index(drop=True)
    if candidate.duplicated(KEY+['window']).any(): raise ValueError('DUPLICATE_RESULT')
    return candidate


def generate(root,out,preregistration_commit):
    root=Path(root).resolve();out=Path(out).resolve();repo=Path(__file__).resolve().parents[1]
    if out.exists(): raise ValueError('OUTPUT_ALREADY_EXISTS')
    started=now()
    ldir=root/ARCHIVES[0];bdir=root/ARCHIVES[1];cdir=root/ARCHIVES[2]
    bm=read(bdir/'candidate_manifest.json');lm=read(ldir/'delivery_receipt.json')
    bp=bdir/'candidate_predictions.parquet';lp=ldir/'scoring/segment_predictions.parquet';cp=cdir/'prepared/core.parquet'
    if sha(bp)!=bm['candidate_sha256']: raise ValueError('BLEND_SEAL_CHANGED')
    for rel in ['scoring/segment_predictions.parquet','scoring/scores.parquet','scoring/manifest.json','validation/validation.json']:
        if sha(ldir/rel)!=lm['files'][rel]: raise ValueError('L1_SEAL_CHANGED:'+rel)
    if sha(cp)!=bm['sources'][str(cp)]: raise ValueError('CORE_SEAL_CHANGED')
    protected={}
    for directory in ARCHIVES+['blend50_shadow_20261028_v1']:
        for p in sorted((root/directory).rglob('*')):
            if p.is_file(): protected[str(p)]=sha(p)
    shadow=[p for p in protected if str(root/'blend50_shadow_20261028_v1') in p]
    old_shadow={p:h for p,h in lm['source_files'].items() if str(root/'blend50_shadow_20261028_v1') in p}
    if len(shadow)!=112 or len(old_shadow)!=112: raise ValueError('SHADOW_FILE_SET_CHANGED')
    for p in shadow:
        if p not in old_shadow or protected[p]!=old_shadow[p]: raise ValueError('SHADOW_CHANGED:'+p)
    protocol=repo/'docs/blend50_l1_mean_v1_protocol.md'
    sources={str(p):sha(p) for p in [bp,lp,cp,bdir/'candidate_manifest.json',ldir/'delivery_receipt.json',
        ldir/'scoring/scores.parquet',ldir/'scoring/manifest.json',root/ARCHIVES[3]/'manifest.json',
        root/ARCHIVES[3]/'historical_windows.parquet',root/ARCHIVES[3]/'objects.parquet',
        root/ARCHIVES[3]/'all_miss_directions_detail.csv',root/ARCHIVES[3]/'top20_products.csv',protocol,Path(__file__)]}
    out.mkdir(parents=True,exist_ok=False)
    config={'candidate':CANDIDATE,'recorded_at':now(),'rule':'0.5 * BLEND50 + 0.5 * DIRECT_L1 per segment; 60=sum of segments',
        'weight_each':0.5,'root':str(root),'preregistration_commit':preregistration_commit,
        'input_columns':{'blend50':BLEND_COLUMNS,'l1':L1_COLUMNS,'core':KEY},
        'sources':sources,'evidence_grade':'RECONSTRUCTED_HISTORICAL_DEVELOPMENT_AFTER_ENDPOINT_RESULTS',
        'new_fits':0,'model_inference_calls':0,'business_database_api_queries':0,'production_changes':0,'new_shadow_issues':0}
    dump(out/'configuration.json',config)
    # File hashes and schemas may have been inspected; generation never projects truth.
    blend=pd.read_parquet(bp,columns=BLEND_COLUMNS);l1=pd.read_parquet(lp,columns=L1_COLUMNS)
    core,not_matured=mature_core(pd.read_parquet(cp,columns=KEY))
    candidate=combine(blend,l1)
    keys=candidate[KEY].drop_duplicates()
    align=core.merge(keys,on=KEY,how='outer',validate='one_to_one',indicator=True)
    if not align['_merge'].eq('both').all() or len(core)!=2820: raise ValueError('CORE_SCOPE_CHANGED')
    if len(candidate)!=8460 or candidate.issue_date.nunique()!=20: raise ValueError('ISSUE_SCOPE_CHANGED')
    counts=candidate.groupby('window').prediction.count()
    if not counts.eq(2579).all(): raise ValueError('OUTPUT_COVERAGE_CHANGED')
    candidate.to_parquet(out/'candidate_predictions.parquet',index=False)
    candidate.to_csv(out/'candidate_predictions.csv',index=False,encoding='utf-8-sig')
    dump(out/'prediction_access_log.json',{'started_at':started,'completed_at':now(),
        'read_paths':[str(bp),str(lp),str(cp)],'columns':config['input_columns'],
        'truth_columns_projected':[],'ranking_or_error_inputs':False,'model_files_loaded':False})
    verify(sources);verify(protected)
    seal={'candidate':CANDIDATE,'root':str(root),'started_at':started,'sealed_at':now(),
        'preregistration_commit':preregistration_commit,
        'implementation_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
        'script_sha256':sha(__file__),'formula':config['rule'],'sources':sources,'protected_files':protected,
        'source_projection_columns':config['input_columns'],'shadow_file_count':len(shadow),
        'excluded_periods_not_matured':{str(k):int(v) for k,v in not_matured.groupby('issue_date').size().items()},
        'full_core_n':len(core),'paired_n':int(counts.iloc[0]),'missing_output_n':len(core)-int(counts.iloc[0]),
        'new_fits':0,'model_inference_calls':0,'business_database_api_queries':0,'production_changes':0,'new_shadow_issues':0,
        'files':{p.name:sha(p) for p in out.iterdir() if p.is_file()}}
    dump(out/'candidate_manifest.json',seal)
    print(json.dumps({k:seal[k] for k in ['candidate','sealed_at','full_core_n','paired_n','missing_output_n','new_fits','model_inference_calls','shadow_file_count']}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);p.add_argument('--preregistration-commit',required=True)
    args=p.parse_args();generate(args.root,args.out,args.preregistration_commit)
