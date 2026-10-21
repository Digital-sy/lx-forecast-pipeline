"""Isolated truth-free BLEND50 shadow gates; never invokes production jobs."""
from __future__ import annotations
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
try:
    from scripts import bc60_monthly_replay as frozen
    from scripts import bc60_v3_direct as direct
    from scripts import direct_yearago_experiment as annual
    from scripts import yearago_gate_route as gate
    from scripts.direct_yearago_blend50 import blend_one
except ModuleNotFoundError:
    import bc60_monthly_replay as frozen
    import bc60_v3_direct as direct
    import direct_yearago_experiment as annual
    import yearago_gate_route as gate
    from direct_yearago_blend50 import blend_one

ROOT=Path(__file__).resolve().parents[1]
SHOPS=frozen.SHOPS
KEY=['issue_date','shop','spu']
DAILY=['ds','shop','spu','y','mapped','sales_state']
PINS={
 'scripts/bc60_v3_direct.py':('34bdc3b703c45326f3c638814ad18595c8124e83','97cbb1c3fa7c0e49ebd0dbac849ee723dc4c5ebd51fd2ccfd39b0860ccf36513'),
 'scripts/bc60_monthly_replay.py':('34bdc3b703c45326f3c638814ad18595c8124e83','29115e0fd7c161c4d2d4574a2cfd175e670a37a2de985cc37fc8650b8c348027'),
 'scripts/bc60_experiment.py':('34bdc3b703c45326f3c638814ad18595c8124e83','5e433c029ebd8fa2c60e19a389a9d2ce5cd3c344238645160cec69f7c0e4f745'),
 'scripts/direct_yearago_experiment.py':('83e0baa32f6de5cea6ff38e19c87f56fba67daba','928d50717421819c352d8a9693dd404f73342e37615f9a4238ec8bd29ab53b9d'),
 'scripts/yearago_gate_route.py':('601e3d2c1f592165141d526eb9b6571dd557b0aa','88bc81da97d26d2a88a83dc55de9fe745587c0e0ca2bd79791f8eb72b8239a2c'),
 'scripts/direct_yearago_blend50.py':('4ea35ad10b1c6d059e0b7f8249136b11b49f1fbb','38bb8812a2d8314e7669bad599c87d166d2fe69e18608d3c2993dbdf7f617a10')}

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def clock():
    utc=datetime.now(timezone.utc)
    return {'utc':utc.isoformat(),'asia_shanghai':utc.astimezone(ZoneInfo('Asia/Shanghai')).isoformat()}
def dump_new(p,obj):
    with Path(p).open('x',encoding='utf-8') as f:json.dump(obj,f,ensure_ascii=False,indent=2,default=str,allow_nan=False)
def unique(frame,keys):
    if frame.duplicated(keys).any():raise ValueError('DUPLICATE_KEYS')
def verify_sources(sources):
    for p,h in sources.items():
        if sha(p)!=h:raise ValueError('SOURCE_HASH_CHANGED: '+str(p))
def protect(p,sources,expected=None):
    p=Path(p).resolve();h=sha(p)
    if expected is not None and h!=expected:raise ValueError('ARCHIVE_HASH_MISMATCH: '+str(p))
    sources[str(p)]=h
def verify_code():
    result={}
    for name,(commit,h) in PINS.items():
        b=(ROOT/name).read_bytes().replace(b'\r\n',b'\n')
        historical=subprocess.check_output(['git','show',commit+':'+name],cwd=ROOT)
        if hashlib.sha256(b).hexdigest()!=h or b!=historical:raise ValueError('FROZEN_CODE_CHANGED: '+name)
        result[name]={'commit':commit,'normalized_sha256':h,'file_sha256':sha(ROOT/name)}
    return result
def time_contract(issue):
    s=pd.Timestamp(issue).normalize();c=s-pd.Timedelta(days=2)
    return {'issue_date':str(s.date()),'input_cutoff':str(c.date()),
            'W1':[str((s+pd.Timedelta(days=n)).date()) for n in [1,30]],
            'W2':[str((s+pd.Timedelta(days=n)).date()) for n in [31,60]],
            'W60':[str((s+pd.Timedelta(days=n)).date()) for n in [1,60]],
            'earliest_checks':[str((s+pd.Timedelta(days=n)).date()) for n in [32,62]]}
def verify_seal_time(issue,sealed_at):
    z=pd.Timestamp(sealed_at)
    if z.tz is None:raise ValueError('TIMESTAMP_TIMEZONE_REQUIRED')
    start=(pd.Timestamp(issue)+pd.Timedelta(days=1)).tz_localize('Asia/Shanghai')
    if z>=start:raise ValueError('TARGET_ALREADY_STARTED_NO_BACKDATE')
def matrices(daily,monthly,objects,issue):
    c=pd.Timestamp(issue)-pd.Timedelta(days=2)
    # Filter before all feature/index construction, including earliest-positive evidence.
    d=daily[daily.ds.le(c)].copy()
    m=monthly[monthly.month.astype(str).le(str(direct.completed_month(c)))].copy()
    ctx=frozen.VectorContext(d,m,SHOPS,c);index=annual.DailyIndex(d)
    result={}
    for seg in ['M1','M2']:
        inf=pd.DataFrame([ctx.row((r.shop,r.spu),issue,seg,issue) for r in objects.itertuples()])
        ext,audit=annual.augment(index,inf,seg)
        if not ext.input_cutoff.eq(c).all() or not audit.ly_target_end.le(c).all():raise ValueError('FEATURE_CUTOFF')
        result[seg]=(ext,audit)
    return ctx,result
def infer_model(path,frame,features):
    from lightgbm import Booster
    model=Booster(model_str=Path(path).read_text(encoding='utf-8'))
    if model.feature_name()!=features:raise ValueError('MODEL_FEATURE_ORDER')
    values=np.full(len(frame),np.nan);valid=frame.input_status.eq('OK')
    if valid.any():
        raw=model.predict(frame.loc[valid,features],validate_features=True,num_threads=2)
        if not np.isfinite(raw).all():raise ValueError('NONFINITE_MODEL_OUTPUT')
        values[valid]=np.maximum(raw,0)  # original frozen postprocessing only
    return values
def assemble(objects,issue,mats,preds,support):
    rows=[]
    for i,r in enumerate(objects.itertuples()):
        a,b=[mats[s][0].iloc[i] for s in ['M1','M2']]
        wa,wb=[mats[s][1].iloc[i] for s in ['M1','M2']]
        e={'issue_date':issue,'shop':r.shop,'spu':r.spu,'input_cutoff':a.input_cutoff,
           'direct_valid':all(np.isfinite(preds['direct_'+s][i]) for s in ['M1','M2']),
           'input_valid':bool(a.ly_input_complete) and wa.ly_input_unsafe_days==0,
           'targets_valid':all(bool(x.ly_target_complete) and w.ly_target_unsafe_days==0 for x,w in [(a,wa),(b,wb)]),
           'prior_positive':bool(a.ly_positive_observed_before_input_start),
           'both_supported':all(support[s]['fit_allowed'] for s in ['M1','M2']),
           'both_annual':all(support[s]['fit_allowed'] and np.isfinite(preds['annual_'+s][i]) for s in ['M1','M2']),
           'input_status_M1':a.input_status,'input_status_M2':b.input_status}
        for s in ['M1','M2']:
            e['direct_'+s]=preds['direct_'+s][i];e['annual_'+s]=preds['annual_'+s][i]
        e.update(gate.choose(e))
        for s in ['M1','M2']:e['blend_'+s]=blend_one(e['direct_'+s],e['selected_'+s],e['gate_decision'])
        for prefix in ['direct','annual','selected','blend']:e[prefix+'_60']=e[prefix+'_M1']+e[prefix+'_M2']
        rows.append(e)
    out=pd.DataFrame(rows);unique(out,KEY)
    return out
def assert_numeric(left,right):
    a,b=np.asarray(left,dtype=float),np.asarray(right,dtype=float)
    if not np.array_equal(np.isnan(a),np.isnan(b)):raise ValueError('OUTPUT_SCOPE_MISMATCH')
    if not np.allclose(a,b,rtol=1e-12,atol=1e-9,equal_nan=True):raise ValueError('NUMERIC_REPRODUCTION_FAILED')
    return float(np.nanmax(np.abs(a-b))) if np.isfinite(a).any() else 0.

def reproduce(workspace,output,issue='2026-08-02'):
    base,out=Path(workspace).resolve(),Path(output).resolve()
    if issue!='2026-08-02':raise ValueError('ONLY_PREREGISTERED_ISSUE')
    out.mkdir(exist_ok=False);sources={};start=clock();access=[]
    try:
        code=verify_code();arc=base/'bc60_monthly_replay_20261020';yr=base/'direct_yearago_20261020_v1'
        conf=read(arc/'prepared/frozen_config.json');runtime={n:importlib.metadata.version(n) for n in conf['runtime']}
        if runtime!=conf['runtime']:raise ValueError('RUNTIME_MISMATCH')
        pm=read(arc/'prepared/prepare_manifest.json');em=read(arc/'effects/run_manifest.json')
        yp=read(yr/'prepare_manifest.json');ye=read(yr/'effects_manifest.json')
        for p in [arc/'prepared/frozen_config.json',arc/'prepared/prepare_manifest.json',arc/'effects/run_manifest.json',yr/'prepare_manifest.json',yr/'effects_manifest.json']:
            protect(p,sources)
        for p in [arc/'inputs/daily.parquet',arc/'inputs/monthly_reference.parquet']:
            protect(p,sources,pm['sources'][str(p)])
        corepath=arc/'prepared/core.parquet';protect(corepath,sources,pm['files']['core.parquet'])
        dump_new(out/'preregistration.json',dict(started=start,issue=issue,code=code,runtime=runtime,
                    tolerance={'rtol':1e-12,'atol':1e-9},fits_allowed=0,query_count=0,protocol_sha256=sha(ROOT/'docs/blend50_prospective_v1_protocol.md')))
        c=pd.Timestamp(issue)-pd.Timedelta(days=2)
        d=pd.read_parquet(arc/'inputs/daily.parquet',columns=DAILY,filters=[('ds','<=',c)])
        m=pd.read_parquet(arc/'inputs/monthly_reference.parquet',filters=[('month','<=',str(direct.completed_month(c)))])
        objects=pd.read_parquet(corepath,columns=KEY,filters=[('issue_date','==',issue)])
        unique(objects,KEY);assert d.ds.max()<=c
        access.append(dict(at=clock(),purpose='generation',daily_columns=DAILY,daily_filter='ds <= '+str(c.date()),monthly_max=str(direct.completed_month(c)),core_columns=KEY,truth_read=False))
        _,mats=matrices(d,m,objects,issue);preds={};support={};inference_calls=0
        for seg in ['M1','M2']:
            old=arc/'effects/slots'/f'{issue}_{seg}';ys=yr/'effects'/f'{issue}_{seg}';prep=yr/'prepared'/f'{issue}_{seg}'
            for p in [old/'checkpoint.json',old/'model.txt']:
                protect(p,sources,em['files'][str(p.relative_to(arc/'effects'))])
            for p in [ys/'checkpoint.json',ys/'model.txt',prep/'training_check.json',prep/'train.parquet']:
                man=ye if 'effects' in p.parts else yp;protect(p,sources,man['files'][str(p.relative_to(yr))])
            train=pd.read_parquet(prep/'train.parquet')
            if not train.label_end.le(c).all() or not train.input_cutoff.eq(train.simulated_issue_date-pd.Timedelta(days=2)).all():raise ValueError('TRAIN_LABEL_LEAK')
            support[seg]=annual.support(train)
            if support[seg]!=read(prep/'training_check.json')['support']:raise ValueError('SUPPORT_REPRODUCTION')
            del train
            ext,audit=mats[seg];ext.to_parquet(out/f'features_{seg}.parquet',index=False);audit.to_parquet(out/f'windows_{seg}.parquet',index=False)
            preds['direct_'+seg]=infer_model(old/'model.txt',ext,direct.FEATURES);inference_calls+=1
            preds['annual_'+seg]=infer_model(ys/'model.txt',ext,annual.FEATURES);inference_calls+=1
        generated=assemble(objects,issue,mats,preds,support)
        generated.to_parquet(out/'generated.parquet',index=False)
        dump_new(out/'prediction_seal.json',dict(at=clock(),sha256=sha(out/'generated.parquet'),support=support,
                source_hashes=sources,access=access,formal_fits=0,historical_inference_calls=inference_calls,source_code=code))
        # Comparison phase starts only after generated output is sealed.
        diffs=[]
        for seg in ['M1','M2']:
            prep=yr/'prepared'/f'{issue}_{seg}';old=arc/'effects/slots'/f'{issue}_{seg}'
            for path in [prep/'inference.parquet',yr/'effects'/f'{issue}_{seg}'/'predictions.parquet']:
                man=yp if 'prepared' in path.parts else ye;protect(path,sources,man['files'][str(path.relative_to(yr))])
            archived=pd.read_parquet(prep/'inference.parquet').set_index(['shop','spu'])
            actual=mats[seg][0].set_index(['shop','spu'])
            pd.testing.assert_frame_equal(actual[annual.FEATURES+['input_status']],archived.loc[actual.index,annual.FEATURES+['input_status']],check_exact=True)
            p=old/'segment_predictions.parquet';protect(p,sources,em['files'][str(p.relative_to(arc/'effects'))])
            dp=pd.read_parquet(p,columns=['shop','spu','prediction']).set_index(['shop','spu'])
            ap=pd.read_parquet(yr/'effects'/f'{issue}_{seg}'/'predictions.parquet',columns=['shop','spu','annual_model_prediction']).set_index(['shop','spu'])
            idx=pd.MultiIndex.from_frame(generated[['shop','spu']])
            for name,vals in [('direct',dp.loc[idx,'prediction']),('annual',ap.loc[idx,'annual_model_prediction'])]:
                diffs.append(dict(stage=name,segment=seg,max_abs_diff=assert_numeric(generated[name+'_'+seg],vals)))
        gp=base/'yearago_gate_v1_20261020';gm=read(gp/'routing_manifest.json');protect(gp/'routing.parquet',sources,gm['routing_sha256'])
        g=pd.read_parquet(gp/'routing.parquet',columns=KEY+['gate_decision','selected_M1','selected_M2','selected_60'],filters=[('issue_date','==',issue)]).set_index(KEY)
        bp=base/'direct_yearago_blend50_20261021_v1';bm=read(bp/'candidate_manifest.json');protect(bp/'candidate_predictions.parquet',sources,bm['candidate_sha256'])
        b=pd.read_parquet(bp/'candidate_predictions.parquet',columns=KEY+['prediction_M1','prediction_M2','prediction_60'],filters=[('issue_date','==',issue)]).set_index(KEY)
        idx=pd.MultiIndex.from_frame(generated[KEY]);assert generated.gate_decision.tolist()==g.loc[idx,'gate_decision'].tolist()
        for seg in ['M1','M2','60']:
            diffs.append(dict(stage='gate',segment=seg,max_abs_diff=assert_numeric(generated['selected_'+seg],g.loc[idx,'selected_'+seg])))
            diffs.append(dict(stage='blend',segment=seg,max_abs_diff=assert_numeric(generated['blend_'+seg],b.loc[idx,'prediction_'+seg])))
        verify_sources(sources)
        receipt=dict(status='PASS',state='READY_OFFLINE_ONLY',started=start,finished=clock(),issue=issue,objects=len(generated),
            output_n=int(generated.blend_60.notna().sum()),missing_n=int(generated.blend_60.isna().sum()),gate_true=int(generated.gate_decision.sum()),
            old_feature_values_dtypes_exact=True,comparisons=diffs,historical_inference_calls=inference_calls,formal_fits=0,database_queries=0,
            generation_seal_sha256=sha(out/'prediction_seal.json'),sources_unchanged=True,source_hashes=sources,source_code=code)
        dump_new(out/'reproduction_receipt.json',receipt);print(json.dumps({k:receipt[k] for k in ['status','state','objects','output_n','missing_n','gate_true','comparisons']},ensure_ascii=False))
    except Exception as exc:
        dump_new(out/'failure.json',dict(status='BLOCKED_SOURCE_OR_REPRODUCTION',at=clock(),error_type=type(exc).__name__,error=str(exc),formal_fits=0,database_queries=0))
        raise

def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('reproduce');a.add_argument('--workspace',required=True);a.add_argument('--output',required=True)
    args=vars(p.parse_args());command=args.pop('command')
    if command=='reproduce':reproduce(**args)
if __name__=='__main__':main()
