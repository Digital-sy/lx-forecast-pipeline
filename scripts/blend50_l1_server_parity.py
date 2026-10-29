"""Export truth-free archived fixtures and verify six saved models; never fits."""
from __future__ import annotations
import argparse, datetime as dt, hashlib, importlib.metadata, json, shutil, sys
from pathlib import Path
import numpy as np
import pandas as pd

KEY=['issue_date','shop','spu']
BASE=['shop_code','lag1','lag7','lag14','lag28','rolling_mean_lag1_window_size7','rolling_mean_lag1_window_size28','dayofweek','month','dayofyear']
ANNUAL=BASE+['ly_input_mean28','ly_target_mean30','ly_scale_contrast','ly_shape_contrast','ly_input_complete','ly_target_complete','ly_positive_observed_before_input_start']
META=KEY+['simulated_issue_date','input_cutoff','segment','label_start','label_end','history_start','history_end','positive_completed_months','input_status']
CONDITIONS=['direct_valid','input_valid','targets_valid','prior_positive','both_supported','both_annual']
ABS_TOL=1e-6
REL_TOL=1e-8

def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p,x):Path(p).write_text(json.dumps(x,indent=2,ensure_ascii=False,default=str,allow_nan=False),encoding='utf-8')
def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()
def unique(d,k):
    if d[k].isna().any().any() or d.duplicated(k).any():raise ValueError('DUPLICATE_OR_UNKNOWN_KEY')
def compare(a,b,label):
    a,b=np.asarray(a,float),np.asarray(b,float)
    if a.shape!=b.shape or not np.array_equal(np.isnan(a),np.isnan(b)) or np.isinf(a).any() or np.isinf(b).any():raise ValueError('NONFINITE_OR_SHAPE:'+label)
    good=np.isfinite(b);diff=np.abs(a[good]-b[good]);limit=ABS_TOL+REL_TOL*np.abs(b[good]);excess=int((diff>limit).sum())
    result=dict(field=label,finite_n=int(good.sum()),unknown_n=int((~good).sum()),exact=bool(np.array_equal(a,b,equal_nan=True)),max_abs_diff=float(diff.max()) if len(diff) else 0.,outside_tolerance_n=excess)
    if excess:raise ValueError('PARITY_NUMERIC:'+json.dumps(result))
    return result
def gate_from_conditions(frame):
    if not set(CONDITIONS)<=set(frame):raise ValueError('GATE_EVIDENCE_MISSING')
    return frame[CONDITIONS].notna().all(axis=1)&frame[CONDITIONS].eq(True).all(axis=1)
def combine(d,y,l,gate):
    d,y,l=np.asarray(d,float),np.asarray(y,float),np.asarray(l,float);g=np.asarray(gate,bool)
    if not(d.shape==y.shape==l.shape==g.shape):raise ValueError('SHAPE')
    if any(np.isinf(v).any() or (v[np.isfinite(v)]<0).any() for v in [d,y,l]):raise ValueError('INVALID_ENDPOINT')
    if (g&(~np.isfinite(d)|~np.isfinite(y))).any():raise ValueError('GATE_TRUE_WITH_MISSING_ENDPOINT')
    b=d.copy();b[g]=d[g]+.5*(y[g]-d[g]);p=.5*b+.5*l
    return b,p
def validate_dates(frame,issue,segment):
    issue=pd.Timestamp(issue);start=issue+pd.Timedelta(days=1 if segment=='M1' else 31);end=start+pd.Timedelta(days=29)
    for c,v in [('issue_date',issue),('simulated_issue_date',issue),('input_cutoff',issue-pd.Timedelta(days=2)),('label_start',start),('label_end',end)]:
        if not pd.to_datetime(frame[c]).eq(v).all():raise ValueError('DATE:'+c)
    if not frame.segment.eq(segment).all():raise ValueError('SEGMENT')

def export_fixture(root,out,issue='2026-08-02'):
    root,out=Path(root).resolve(),Path(out).resolve()
    if out.exists():raise ValueError('NO_OVERWRITE')
    sources={};started=now()
    def protected(p,expected=None):
        p=Path(p);h=sha(p)
        if expected and h!=expected:raise ValueError('OLD_HASH_CHANGED:'+str(p))
        sources[str(p)]=h;return p
    ldir=root/'direct_l1_v1_20261028';bdir=root/'direct_yearago_blend50_20261021_v1';ydir=root/'direct_yearago_20261020_v1';ddir=root/'bc60_monthly_replay_20261020'
    bm=read(protected(bdir/'candidate_manifest.json'));bl=pd.read_parquet(protected(bdir/'candidate_predictions.parquet',bm['candidate_sha256']),columns=KEY+['gate_decision','prediction_status','prediction_M1','prediction_M2','prediction_60'])
    bl=bl[pd.to_datetime(bl.issue_date).eq(issue)].sort_values(KEY).reset_index(drop=True);unique(bl,KEY)
    routepath=root/'yearago_gate_v1_20261020';rm=read(protected(routepath/'routing_manifest.json'))
    routing=pd.read_parquet(protected(routepath/'routing.parquet',rm['routing_sha256']));routing=routing[pd.to_datetime(routing.issue_date).eq(issue)].sort_values(KEY).reset_index(drop=True)
    unique(routing,KEY);pd.testing.assert_frame_equal(bl[KEY],routing[KEY],check_exact=True)
    if not np.array_equal(gate_from_conditions(routing),routing.gate_decision) or not np.array_equal(bl.gate_decision,routing.gate_decision):raise ValueError('ARCHIVED_GATE_MISMATCH')
    meanroot=root/'blend50_l1_mean_v1_20261029_run2';mm=read(protected(meanroot/'candidate_manifest.json'))
    mean=pd.read_parquet(protected(meanroot/'candidate_predictions.parquet',mm['files']['candidate_predictions.parquet']),columns=KEY+['window','prediction','prediction_status'])
    mean=mean[pd.to_datetime(mean.issue_date).eq(issue)].sort_values(KEY+['window']).reset_index(drop=True)
    out.mkdir(parents=True);routing.to_parquet(out/'routing.parquet',index=False);bl.to_parquet(out/'blend50_reference.parquet',index=False);mean.to_parquet(out/'mean_reference.parquet',index=False)
    slots=[]
    for segment in ['M1','M2']:
        slot=f'{issue}_{segment}';lp=ldir/'prepared'/slot;yp=ydir/'prepared'/slot
        lc=read(protected(lp/'training_check.json'));yc=read(protected(yp/'training_check.json'))
        base=pd.read_parquet(protected(lp/'inference.parquet',lc['inference_sha256']),columns=META+BASE)
        annual=pd.read_parquet(protected(yp/'inference.parquet',yc['inference_sha256']),columns=META+ANNUAL)
        unique(base,KEY);unique(annual,KEY);base=base.sort_values(KEY).reset_index(drop=True);annual=annual.sort_values(KEY).reset_index(drop=True)
        pd.testing.assert_frame_equal(base[META+BASE],annual[META+BASE],check_exact=True)
        pd.testing.assert_frame_equal(base[KEY],bl[KEY],check_exact=True);validate_dates(base,issue,segment)
        for kind,home,inf,features in [('DIRECT',ddir/'effects/slots'/slot,base,BASE),('YEARAGO',ydir/'effects'/slot,annual,ANNUAL),('L1',ldir/'effects'/slot,base,BASE)]:
            checkpoint=read(protected(home/'checkpoint.json'));model=protected(home/'model.txt',checkpoint['model_sha256'])
            ppath=home/('segment_predictions.parquet' if kind=='DIRECT' else 'predictions.parquet')
            cols=KEY+(['prediction_raw','prediction'] if kind=='DIRECT' else ['annual_model_prediction','prediction_status','prediction_origin'] if kind=='YEARAGO' else ['raw_prediction','prediction','prediction_status'])
            if kind!='DIRECT':
                result=read(protected(home/'result.json'));protected(ppath,result['prediction_sha256'])
            else:protected(ppath)
            pred=pd.read_parquet(ppath,columns=cols).sort_values(KEY).reset_index(drop=True);pd.testing.assert_frame_equal(pred[KEY],base[KEY],check_exact=True)
            reference=base[META].copy();reference['reference_raw']=pred['prediction_raw'] if kind=='DIRECT' else pred['raw_prediction'] if kind=='L1' else np.nan
            reference['reference_prediction']=pred['annual_model_prediction'] if kind=='YEARAGO' else pred['prediction'];reference['reference_status']=np.where(np.isfinite(reference.reference_prediction),'OK',base.input_status)
            if not np.array_equal(reference.reference_prediction.notna(),base.input_status.eq('OK')):raise ValueError('OUTPUT_STATUS_MISMATCH')
            folder=out/f'{kind}_{segment}';folder.mkdir();inf.to_parquet(folder/'inference.parquet',index=False);reference.to_parquet(folder/'reference.parquet',index=False);shutil.copyfile(model,folder/'model.txt')
            if kind=='YEARAGO':
                windows=pd.read_parquet(protected(yp/'inference_windows.parquet')).sort_values(KEY[1:]).reset_index(drop=True)
                pd.testing.assert_frame_equal(windows[KEY[1:]],inf[KEY[1:]],check_exact=True)
                windows.to_parquet(folder/'inference_windows.parquet',index=False)
            meta=dict(kind=kind,segment=segment,features=features,dtypes={c:str(inf[c].dtype) for c in features},rows=len(inf),eligible_n=int(inf.input_status.eq('OK').sum()),source_model_sha256=sha(model),support=yc['support'] if kind=='YEARAGO' else None,raw_reference_available=kind!='YEARAGO')
            dump(folder/'slot.json',meta);slots.append(meta|{'path':folder.name})
    for p,h in sources.items():
        if sha(p)!=h:raise ValueError('SOURCE_CHANGED_DURING_EXPORT')
    manifest=dict(status='EXPORTED_NOT_SERVER_VERIFIED',issue_date=issue,started_at=started,sealed_at=now(),objects=len(bl),gate_true_n=int(bl.gate_decision.sum()),gate_false_n=int((~bl.gate_decision).sum()),slots=slots,sources=sources,truth_columns_exported=[],fits=0,prediction_calls=0,absolute_tolerance=ABS_TOL,relative_tolerance=REL_TOL,synthetic_required=['qualification_insufficient','explicit_zero','unknown'],files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()})
    dump(out/'fixture_manifest.json',manifest);print(json.dumps({k:manifest[k] for k in ['status','objects','gate_true_n','gate_false_n','fits','prediction_calls']}));return manifest

def verify_fixture(fixture,out,release_manifest):
    from lightgbm import Booster
    import blend50_l1_server_issue as deployed
    fixture,out=Path(fixture).resolve(),Path(out).resolve();release_manifest=Path(release_manifest).resolve()
    if out.exists():raise ValueError('NO_OVERWRITE')
    release=read(release_manifest)
    if not release.get('code_commit') or release['files'].get('scripts/blend50_l1_server_parity.py')!=sha(__file__):raise ValueError('PARITY_CODE_NOT_RELEASE_PINNED')
    for p,h in release['files'].items():
        if sha(release_manifest.parent/p)!=h:raise ValueError('RELEASE_FILE_CHANGED:'+p)
    manifest=read(fixture/'fixture_manifest.json')
    if manifest['absolute_tolerance']!=ABS_TOL or manifest['relative_tolerance']!=REL_TOL:raise ValueError('TOLERANCE_CHANGED')
    if importlib.metadata.version('lightgbm')!='4.6.0':raise ValueError('LIGHTGBM_VERSION')
    for p,h in manifest['files'].items():
        if sha(fixture/p)!=h:raise ValueError('FIXTURE_HASH:'+p)
    out.mkdir();started=now();checks=[];results={};calls=0
    for slot in manifest['slots']:
        folder=fixture/slot['path'];inf=pd.read_parquet(folder/'inference.parquet');ref=pd.read_parquet(folder/'reference.parquet');unique(inf,KEY);unique(ref,KEY)
        pd.testing.assert_frame_equal(inf[META],ref[META],check_exact=True);validate_dates(inf,manifest['issue_date'],slot['segment'])
        if {c:str(inf[c].dtype) for c in slot['features']}!=slot['dtypes']:raise ValueError('DTYPE')
        # model_str avoids platform-specific filesystem path encoding behavior.
        booster=Booster(model_str=(folder/'model.txt').read_text(encoding='utf-8'))
        if booster.feature_name()!=slot['features']:raise ValueError('MODEL_FEATURE_ORDER')
        raw=np.full(len(inf),np.nan);good=inf.input_status.eq('OK');raw[good]=booster.predict(inf.loc[good,slot['features']],num_threads=2);calls+=1
        clipped=np.maximum(raw,0)
        checks.append(compare(clipped,ref.reference_prediction,slot['path']+':clipped'))
        if slot['raw_reference_available']:checks.append(compare(raw,ref.reference_raw,slot['path']+':raw'))
        if not np.array_equal(np.isfinite(clipped),ref.reference_status.eq('OK')):raise ValueError('DISCRETE_STATUS')
        q=inf[META].copy();q['kind']=slot['kind'];q['raw_prediction']=raw;q['prediction']=clipped;q['prediction_status']=ref.reference_status
        q.to_parquet(out/(slot['path']+'.parquet'),index=False);results[(slot['kind'],slot['segment'])]=q
    route=pd.read_parquet(fixture/'routing.parquet');gate=gate_from_conditions(route)
    if not np.array_equal(gate,route.gate_decision):raise ValueError('GATE_DECISION')
    bl=pd.read_parquet(fixture/'blend50_reference.parquet');mean=pd.read_parquet(fixture/'mean_reference.parquet');pieces=[]
    mats={seg:(pd.read_parquet(fixture/f'YEARAGO_{seg}/inference.parquet'),pd.read_parquet(fixture/f'YEARAGO_{seg}/inference_windows.parquet')) for seg in ['M1','M2']}
    supports={slot['segment']:slot['support'] for slot in manifest['slots'] if slot['kind']=='YEARAGO'}
    raw_endpoints={kind.lower()+'_'+seg:results[(kind,seg)].prediction.to_numpy() for kind in ['DIRECT','YEARAGO'] for seg in ['M1','M2']}
    for seg in ['M1','M2']:raw_endpoints['annual_'+seg]=raw_endpoints.pop('yearago_'+seg)
    deployed_wide=deployed.p.assemble(bl[KEY],manifest['issue_date'],mats,raw_endpoints,supports)
    deployed_wide=deployed.combine_l1(deployed_wide,{seg:results[('L1',seg)].prediction.to_numpy() for seg in ['M1','M2']})
    pd.testing.assert_frame_equal(deployed_wide[KEY],bl[KEY],check_exact=True)
    for c in CONDITIONS+['gate_decision','failed_conditions','main_reason']:
        if not np.array_equal(deployed_wide[c],route[c]):raise ValueError('DEPLOYED_DISCRETE_GATE:'+c)
    for seg,window in [('M1','d01_30'),('M2','d31_60')]:
        d,y,l=[results[(kind,seg)] for kind in ['DIRECT','YEARAGO','L1']]
        for q in [d,y,l]:pd.testing.assert_frame_equal(q[KEY],route[KEY],check_exact=True)
        b,p=combine(d.prediction,y.prediction,l.prediction,gate);checks.append(compare(b,bl['prediction_'+seg],'BLEND50_'+seg))
        checks.append(compare(deployed_wide['blend_'+seg],b,'DEPLOYED_BLEND50_'+seg));checks.append(compare(deployed_wide['candidate_'+seg],p,'DEPLOYED_MEAN_'+seg))
        m=mean[mean.window.eq(window)].sort_values(KEY).reset_index(drop=True);pd.testing.assert_frame_equal(m[KEY],d[KEY],check_exact=True);checks.append(compare(p,m.prediction,'MEAN_'+seg))
        q=d[META].copy();q['window']=window;q['gate_decision']=gate;q['blend50_prediction']=b;q['prediction']=p;pieces.append(q)
    total=pieces[0].copy();total['window']='d01_60';total['label_end']=pieces[1].label_end
    for c in ['blend50_prediction','prediction']:total[c]=pieces[0][c]+pieces[1][c]
    checks.append(compare(total.blend50_prediction,bl.prediction_60,'BLEND50_60'));checks.append(compare(total.prediction,mean[mean.window.eq('d01_60')].sort_values(KEY).prediction,'MEAN_60'))
    checks.append(compare(deployed_wide.blend_60,total.blend50_prediction,'DEPLOYED_BLEND50_60'));checks.append(compare(deployed_wide.candidate_60,total.prediction,'DEPLOYED_MEAN_60'))
    pd.concat(pieces+[total],ignore_index=True).to_parquet(out/'combined_predictions.parquet',index=False)
    deployed_wide.to_parquet(out/'deployed_combination.parquet',index=False)
    for p,h in manifest['files'].items():
        if sha(fixture/p)!=h:raise ValueError('FIXTURE_CHANGED')
    result=dict(status='PASS',started_at=started,completed_at=now(),issue_date=manifest['issue_date'],objects=manifest['objects'],historical_fits=0,model_loads=len(manifest['slots']),prediction_calls=calls,checks=checks,discrete_keys_dates_status_gate_exact=True,deployed_pure_composition_verified=True,gate_true_n=int(gate.sum()),gate_false_n=int((~gate).sum()),synthetic_only=manifest['synthetic_required'],fixture_manifest_sha256=sha(fixture/'fixture_manifest.json'),release_manifest_sha256=sha(release_manifest),code_commit=release['code_commit'],python=sys.version,lightgbm=importlib.metadata.version('lightgbm'),numpy=np.__version__,pandas=pd.__version__,script_sha256=sha(__file__),files={p.name:sha(p) for p in out.iterdir() if p.is_file()})
    dump(out/'parity_result.json',result);print(json.dumps({k:result[k] for k in ['status','objects','historical_fits','model_loads','prediction_calls']}));return result

if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='command',required=True)
    e=s.add_parser('export');e.add_argument('--root',required=True);e.add_argument('--out',required=True);e.add_argument('--issue',default='2026-08-02')
    v=s.add_parser('verify');v.add_argument('--fixture',required=True);v.add_argument('--out',required=True);v.add_argument('--release-manifest',required=True)
    a=vars(p.parse_args());command=a.pop('command');export_fixture(**a) if command=='export' else verify_fixture(**a)
