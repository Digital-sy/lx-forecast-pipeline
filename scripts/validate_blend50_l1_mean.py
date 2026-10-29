"""Independent fixed-mean provenance and arithmetic checks; no model operations."""
from __future__ import annotations
import argparse, ast, datetime as dt, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd

CANDIDATE='BLEND50_L1_MEAN_V1'
KEY=['issue_date','shop','spu']
WINDOWS=['d01_30','d31_60','d01_60']
MODELS=['DIRECT','BLEND50','DIRECT_L1_V1','F28',CANDIDATE]

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')
def unique(frame,keys):
    if frame.duplicated(keys).any() or frame[keys].isna().any().any():raise ValueError('DUPLICATE_OR_MISSING_KEY')

def independent_candidate(blend,l1):
    """Read endpoint projections only; no truth, errors, hits or routing input."""
    unique(blend,KEY);unique(l1,KEY+['segment'])
    if set(l1.segment)!=set(['M1','M2']):raise ValueError('SEGMENT_SET')
    b=blend.set_index(KEY).sort_index();lp=l1.pivot(index=KEY,columns='segment',values='prediction').sort_index()
    if not b.index.equals(lp.index):raise ValueError('ENDPOINT_KEY_SCOPE')
    if not np.array_equal(b.prediction_60,b.prediction_M1+b.prediction_M2,equal_nan=True):raise ValueError('BLEND50_SEGMENT_SUM')
    pieces=[];segments={}
    for seg,w,a,z in [('M1','d01_30',1,30),('M2','d31_60',31,60)]:
        data=l1[l1.segment.eq(seg)].set_index(KEY).reindex(b.index)
        issue=pd.Series(pd.to_datetime(b.index.get_level_values('issue_date')),index=b.index)
        cutoff=issue-pd.Timedelta(days=2);start=issue+pd.Timedelta(days=a);end=issue+pd.Timedelta(days=z)
        if not pd.to_datetime(b.input_cutoff).equals(cutoff) or not pd.to_datetime(data.input_cutoff).equals(cutoff):raise ValueError('INPUT_CUTOFF')
        for original,expected in [(b['target_start_'+w],start),(b['target_end_'+w],end),(data.label_start,start),(data.label_end,end)]:
            if not pd.to_datetime(original).equals(expected):raise ValueError('TARGET_DATE')
        bv=b['prediction_'+seg].to_numpy(float);lv=lp[seg].to_numpy(float)
        if np.isinf(bv).any() or np.isinf(lv).any() or (bv[np.isfinite(bv)]<0).any() or (lv[np.isfinite(lv)]<0).any():raise ValueError('INVALID_ENDPOINT')
        if not np.array_equal(np.isfinite(bv),np.isfinite(lv)):raise ValueError('ASYMMETRIC_ENDPOINT_OUTPUT')
        predicted=.5*bv+.5*lv
        frame=b.index.to_frame(index=False);frame['window']=w;frame['input_cutoff']=cutoff.to_numpy();frame['target_start']=start.to_numpy();frame['target_end']=end.to_numpy()
        frame['blend50_prediction']=bv;frame['l1_prediction']=lv;frame['prediction']=predicted
        pieces.append(frame);segments[seg]=frame
    m1,m2=segments['M1'],segments['M2']
    if not np.array_equal(m1.prediction.notna(),m2.prediction.notna()):raise ValueError('PARTIAL_SEGMENT_OUTPUT')
    total=m1.copy();total['window']='d01_60';total['target_end']=m2.target_end
    for col in ['blend50_prediction','l1_prediction','prediction']:total[col]=m1[col]+m2[col]
    pieces.append(total)
    return pd.concat(pieces,ignore_index=True).sort_values(KEY+['window']).reset_index(drop=True)

def verify_candidate_frame(expected,actual):
    unique(actual,KEY+['window']);a=actual.sort_values(KEY+['window']).reset_index(drop=True)
    if len(a)!=len(expected):raise ValueError('CANDIDATE_ROW_COUNT')
    pd.testing.assert_frame_equal(a[KEY+['window']],expected[KEY+['window']],check_exact=True)
    for c in ['input_cutoff','target_start','target_end']:
        if not pd.to_datetime(a[c]).equals(pd.to_datetime(expected[c])):raise ValueError('CANDIDATE_DATE:'+c)
    for c in ['blend50_prediction','l1_prediction','prediction']:
        if not np.array_equal(a[c],expected[c],equal_nan=True):raise ValueError('MEAN_OR_ENDPOINT_CHANGED:'+c)
    if not a.prediction_status.eq('OK').equals(expected.prediction.notna()):raise ValueError('OUTPUT_STATUS')
    return a

def metrics(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float);known=np.isfinite(y);valid=known&np.isfinite(p)
    a=y[valid];pred=p[valid];e=pred-a;den=float(a.sum());ae=float(np.abs(e).sum());over=float(e[e>0].sum());under=float(-e[e<0].sum())
    positive=a>0;hit=np.zeros(len(a),bool);hit[positive]=np.abs(e[positive])/a[positive]<=.2
    return dict(fixed_n=len(y),output_n=int(np.isfinite(p).sum()),truth_n=int(known.sum()),scored_n=int(valid.sum()),
        actual_units=den,prediction_units=float(pred.sum()),ae=ae,wape=ae/den if den>0 else None,bias=float(e.sum()/den) if den>0 else None,
        over_units=over,under_units=under,over_rate=over/den if den>0 else None,hit_n=int(hit.sum()),hit_actual=float(a[hit].sum()),
        zero_truth_n=int((y==0).sum()),zero_truth_ae=float(np.abs(e[a==0]).sum()))

def joint_flags(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float)
    if y.shape!=p.shape or y.ndim!=2 or y.shape[1]!=3:raise ValueError('THREE_WINDOWS_REQUIRED')
    ratios=np.full_like(y,np.nan);np.divide(np.abs(y-p),y,out=ratios,where=y>0)
    return np.isfinite(y).all(axis=1)&np.isfinite(p).all(axis=1)&(y>0).all(axis=1)&(ratios<=.2).all(axis=1)

def compare_number(a,b,name):
    if a is None:
        if pd.notna(b):raise ValueError('EXPECTED_UNDEFINED:'+name)
    elif isinstance(a,(int,np.integer)):
        if a!=b:raise ValueError('COUNT_MISMATCH:'+name)
    elif not np.isclose(float(a),float(b),rtol=1e-12,atol=1e-8,equal_nan=True):raise ValueError('METRIC_MISMATCH:'+name)

def convexity(y,b,l,m):
    y,b,l,m=[np.asarray(v,float) for v in [y,b,l,m]]
    valid=np.isfinite(y)&np.isfinite(b)&np.isfinite(l)&np.isfinite(m)
    eb,el,em=b[valid]-y[valid],l[valid]-y[valid],m[valid]-y[valid]
    if not np.allclose(em,.5*eb+.5*el,rtol=1e-12,atol=1e-9):raise ValueError('MEAN_SIGNED_ERROR_IDENTITY')
    result={}
    for name,fn in [('ae',np.abs),('over',lambda e:np.maximum(e,0)),('under',lambda e:np.maximum(-e,0))]:
        bound=.5*fn(eb)+.5*fn(el);candidate=fn(em);tol=1e-9+1e-12*np.maximum(1,bound)
        if (candidate>bound+tol).any():raise ValueError('CONVEXITY_VIOLATION:'+name)
        result[name+'_upper']=float(bound.sum());result[name+'_candidate']=float(candidate.sum());result[name+'_slack']=float((bound-candidate).sum())
    result.update(records=int(valid.sum()),signed_error_sum=float(em.sum()),signed_error_endpoint_mean=float(.5*eb.sum()+.5*el.sum()))
    return result

def endpoint_group(bhit,lhit,complete):
    return np.select([~np.asarray(complete,bool),bhit&lhit,bhit&~lhit,~bhit&lhit],['MISSING','BOTH_HIT','BLEND_ONLY','L1_ONLY'],default='NEITHER')

def cancellation_arrays(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float);valid=np.isfinite(y).all(axis=1)&np.isfinite(p).all(axis=1)
    error=p-y;e1,error2=error[:,0],error[:,1]
    segment=np.abs(e1)+np.abs(error2);cumulative=np.abs(error[:,2]);cancel=segment-cumulative
    if (cancel[valid]<-1e-8).any():raise ValueError('NEGATIVE_CANCELLATION')
    return valid,e1,error2,segment,cumulative,cancel

def cancellation_summary(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float)
    valid,e1,e2,sa,ca,cc=cancellation_arrays(y,p)
    relative=np.full_like(y,np.nan)
    np.divide(np.abs(p-y),y,out=relative,where=y>0)
    hits=np.isfinite(p)&np.isfinite(y)&(y>0)&(relative<=.2)
    return dict(records=len(y),scored_n=int(valid.sum()),unknown_n=int((~valid).sum()),
        e1=float(e1[valid].sum()),e2=float(e2[valid].sum()),
        segment_ae=float(sa[valid].sum()),cumulative_ae=float(ca[valid].sum()),cancellation=float(cc[valid].sum()),
        opposite_segment_signs_n=int(((e1*e2<0)&valid).sum()),
        cumulative_hit_but_segment_miss_n=int((hits[:,2]&~(hits[:,0]&hits[:,1])&valid).sum()))

def source_code_review(path):
    text=Path(path).read_text(encoding='utf-8-sig');tree=ast.parse(text);blocked=[]
    methods={'fit','train','predict','load_model','Booster','LGBMRegressor','connect','create_engine','urlopen','request','post','get_token'}
    for node in ast.walk(tree):
        if isinstance(node,ast.Import):
            blocked.extend((node.lineno,a.name) for a in node.names if a.name.split('.')[0] in {'lightgbm','requests','httpx','pymysql','sqlalchemy','paramiko'})
        elif isinstance(node,ast.ImportFrom) and (node.module or '').split('.')[0] in {'lightgbm','requests','httpx','pymysql','sqlalchemy','paramiko'}:blocked.append((node.lineno,node.module))
        elif isinstance(node,ast.Call):
            name=node.func.attr if isinstance(node.func,ast.Attribute) else node.func.id if isinstance(node.func,ast.Name) else ''
            if name in methods:blocked.append((node.lineno,name))
    if blocked:raise ValueError('MODEL_OR_REMOTE_ENTRY:'+repr(blocked))
    return dict(status='PASS',file=str(path),sha256=sha(path),model_operations=0)

def run(root,out):
    root,out=Path(root).resolve(),Path(out).resolve();dest=out/'validation'
    if dest.exists():raise ValueError('NO_VALIDATION_OVERWRITE')
    manifest=read(out/'candidate_manifest.json')
    if manifest['candidate']!=CANDIDATE:raise ValueError('WRONG_CANDIDATE')
    dest.mkdir();sources={};started=dt.datetime.now(dt.timezone.utc).isoformat()
    def source(p):
        p=Path(p).resolve();sources[str(p)]=sha(p);return p
    source(out/'candidate_manifest.json')
    for dictionary in ['sources','protected_files']:
        for p,h in manifest[dictionary].items():
            if sha(p)!=h:raise ValueError('SOURCE_CHANGED:'+p)
            sources[p]=h
    for p,h in manifest['files'].items():
        if sha(out/p)!=h:raise ValueError('CANDIDATE_FILE_CHANGED:'+p)
        source(out/p)
    shadow_root=(root/'blend50_shadow_20261028_v1').resolve()
    shadow_paths=[p for p in manifest['protected_files'] if shadow_root in Path(p).resolve().parents]
    if len(shadow_paths)!=112 or manifest['shadow_file_count']!=112:raise ValueError('SHADOW_SCOPE')
    blendcols=KEY+['input_cutoff','prediction_M1','prediction_M2','prediction_60']+[c+'_'+w for w in WINDOWS for c in ['target_start','target_end']]
    blend=pd.read_parquet(source(root/'direct_yearago_blend50_20261021_v1/candidate_predictions.parquet'),columns=blendcols)
    l1=pd.read_parquet(source(root/'direct_l1_v1_20261028/scoring/segment_predictions.parquet'),columns=KEY+['segment','input_cutoff','label_start','label_end','prediction'])
    expected=independent_candidate(blend,l1);actual=pd.read_parquet(source(out/'candidate_predictions.parquet'));actual=verify_candidate_frame(expected,actual)
    full=expected[expected.window.eq('d01_60')]
    if len(full)!=2820 or full.prediction.notna().sum()!=2579 or full.prediction.isna().sum()!=241 or full.issue_date.nunique()!=20:raise ValueError('FROZEN_SCOPE')
    core=pd.read_parquet(source(root/'bc60_monthly_replay_20261020/prepared/core.parquet'),columns=KEY)
    unique(core,KEY);period=pd.to_datetime(core.issue_date).dt.to_period('M')
    mature=period.isin(pd.period_range('2025-01','2026-08',freq='M'));excluded=core[~mature]
    match=core[mature].merge(full[KEY],on=KEY,how='outer',validate='one_to_one',indicator=True)
    if len(match)!=2820 or not match['_merge'].eq('both').all():raise ValueError('CORE_KEY_SCOPE')
    if len(excluded)!=274 or set(pd.to_datetime(excluded.issue_date).dt.to_period('M').astype(str))!={'2026-09','2026-10'}:raise ValueError('UNMATURED_SCOPE')
    # Truth access occurs only here, after sealed candidate and endpoint equality.
    frozen=pd.read_parquet(source(root/'core_model_complementarity_20261028_v1/objects.parquet'))
    unique(frozen,KEY);frozen=frozen.set_index(KEY)
    for model,col in [('BLEND50','blend50_prediction'),('DIRECT_L1_V1','l1_prediction'),(CANDIDATE,'prediction')]:
        pivot=expected.pivot(index=KEY,columns='window',values=col).reindex(frozen.index)
        for w in WINDOWS:
            if model=='BLEND50' and not np.array_equal(frozen[model+'_prediction_'+w],pivot[w],equal_nan=True):raise ValueError('OLD_BLEND_SOURCE_CONFLICT')
            frozen[model+'_prediction_'+w]=pivot[w]
    frozen=frozen.reset_index();metric=pd.read_parquet(source(out/'scoring/metrics.parquet'));joint=pd.read_parquet(source(out/'scoring/joint_summary.parquet'))
    details=pd.read_parquet(source(out/'scoring/details.parquet')).set_index(KEY).reindex(pd.MultiIndex.from_frame(frozen[KEY])).reset_index()
    summary=read(source(out/'scoring/summary.json'));independent={};metricchecks=[];jointchecks=[];migrationchecks=[];bounds=[];cancelrows=[];groupchecks=[];cancelsummary=[]
    for scope,mask in [('FULL_CORE',np.ones(len(frozen),bool)),('PAIRED',frozen.paired.eq(True).to_numpy())]:
        z=frozen[mask];y=z[['actual_'+w for w in WINDOWS]].to_numpy();independent[scope]={}
        for model in MODELS:
            p=z[[model+'_prediction_'+w for w in WINDOWS]].to_numpy();hit=joint_flags(y,p);complete=np.isfinite(p).all(axis=1);den=float(z.actual_d01_60.sum())
            result=dict(joint_hit_n=int(hit.sum()),joint_hit_actual=float(y[hit,2].sum()),joint_hit_actual_share=float(y[hit,2].sum()/den),output_complete_n=int(complete.sum()))
            r=joint[(joint.scope==scope)&(joint.slice=='overall')&(joint.model==model)].iloc[0]
            for name,value in result.items():compare_number(value,r[name],model+name)
            jointchecks.append(dict(scope=scope,model=model,**result));independent[scope][model]=dict(joint=result,hit=hit,complete=complete)
            for k,w in enumerate(WINDOWS):
                calc=metrics(y[:,k],p[:,k]);r=metric[(metric.scope==scope)&(metric.slice=='overall')&(metric.model==model)&(metric.window==w)].iloc[0]
                for name,value in calc.items():compare_number(value,r[name],model+w+name)
                metricchecks.append(dict(scope=scope,model=model,window=w,**calc));independent[scope][model][w]=calc
            if scope=='PAIRED':
                decline=z.decline_group.eq('PRE_ISSUE_DECLINING').to_numpy()
                if decline.sum()!=842:raise ValueError('DECLINE_SCOPE')
                independent[scope][model]['decline']=metrics(y[decline,2],p[decline,2])
            valid,e1,e2,sa,ca,cc=cancellation_arrays(y,p)
            if model in ['BLEND50','DIRECT_L1_V1',CANDIDATE]:cancelsummary.append(dict(scope=scope,model=model,**cancellation_summary(y,p)))
            for i,key in enumerate(z[KEY].itertuples(index=False,name=None)):
                cancelrows.append(dict(zip(KEY,key))|dict(scope=scope,model=model,valid=bool(valid[i]),e1=float(e1[i]),e2=float(e2[i]),segment_ae=float(sa[i]),cumulative_ae=float(ca[i]),cancellation=float(cc[i])))
        mean=independent[scope][CANDIDATE]
        for baseline in ['BLEND50','DIRECT_L1_V1','DIRECT']:
            oldhit=independent[scope][baseline]['hit'];newhit=mean['hit']
            states=np.select([oldhit&newhit,~oldhit&newhit,oldhit&~newhit],['RETAINED','GAINED','LOST'],default='NEITHER')
            if not np.array_equal(states,details.loc[mask,'migration_vs_'+baseline]):raise ValueError('MIGRATION_MISMATCH:'+baseline)
            for state in ['RETAINED','GAINED','LOST','NEITHER']:
                k=states==state;migrationchecks.append(dict(scope=scope,baseline=baseline,state=state,n=int(k.sum()),actual60=float(y[k,2].sum())))
        bhit=independent[scope]['BLEND50']['hit'];lhit=independent[scope]['DIRECT_L1_V1']['hit']
        both=bhit&lhit
        if (both&~mean['hit']).any():raise ValueError('COMMON_HIT_LOST')
        groups=endpoint_group(bhit,lhit,independent[scope]['BLEND50']['complete']&independent[scope]['DIRECT_L1_V1']['complete'])
        for group in ['BOTH_HIT','BLEND_ONLY','L1_ONLY','NEITHER','MISSING']:
            k=groups==group;hit=k&mean['hit'];groupchecks.append(dict(scope=scope,group=group,records=int(k.sum()),candidate_hit_n=int(hit.sum()),candidate_hit_actual=float(y[hit,2].sum())))
        for k,w in enumerate(WINDOWS):
            b=z['BLEND50_prediction_'+w].to_numpy();l=z['DIRECT_L1_V1_prediction_'+w].to_numpy();m=z[CANDIDATE+'_prediction_'+w].to_numpy()
            bounds.append(dict(scope=scope,window=w,**convexity(y[:,k],b,l,m)))
    gates={};paired=independent['PAIRED'];n=paired[CANDIDATE]
    for comparator in ['BLEND50','DIRECT_L1_V1','DIRECT']:
        b=paired[comparator]
        calc=dict(wape60_lower=n['d01_60']['wape']<b['d01_60']['wape'],joint_hits_increased=n['joint']['joint_hit_n']>b['joint']['joint_hit_n'],
            joint_hit_sales_share_not_lower=n['joint']['joint_hit_actual_share']>=b['joint']['joint_hit_actual_share'],
            decline_over_rate_not_increased=n['decline']['over_rate']<=b['decline']['over_rate'],
            output_and_core_coverage_not_reduced=np.all(n['complete']>=b['complete']) and np.all(independent['FULL_CORE'][CANDIDATE]['complete']>=independent['FULL_CORE'][comparator]['complete']))
        calc={k:bool(v) for k,v in calc.items()}
        if calc!=summary['gates'][comparator]:raise ValueError('FIVE_GATES_MISMATCH:'+comparator)
        gates[comparator]=calc
    groups=pd.read_parquet(source(out/'scoring/endpoint_groups.parquet'))
    for row in groupchecks:
        found=groups[(groups.scope==row['scope'])&(groups.endpoint_group==row['group'])]
        if not row['records'] and found.empty:continue
        for k in ['records','candidate_hit_n','candidate_hit_actual']:compare_number(row[k],found.iloc[0][k],'ENDPOINT_GROUP:'+k)
    cancels=pd.DataFrame(cancelrows);reported=pd.read_parquet(source(out/'scoring/cancellation_details.parquet'))
    check=cancels[cancels.scope.eq('FULL_CORE')].merge(reported,on=KEY+['model'],validate='one_to_one',suffixes=('','_reported'))
    if len(check)!=len(reported):raise ValueError('CANCELLATION_SCOPE')
    for c in ['e1','e2','segment_ae','cumulative_ae','cancellation']:
        if not np.allclose(check[c],check[c+'_reported'],rtol=1e-12,atol=1e-8,equal_nan=True):raise ValueError('CANCELLATION_VALUE:'+c)
    reported_summary=pd.read_parquet(source(out/'scoring/cancellation_summary.parquet'))
    unique(reported_summary,['scope','model'])
    if len(reported_summary)!=len(cancelsummary):raise ValueError('CANCELLATION_SUMMARY_SCOPE')
    for row in cancelsummary:
        found=reported_summary[(reported_summary.scope==row['scope'])&(reported_summary.model==row['model'])]
        if len(found)!=1:raise ValueError('CANCELLATION_SUMMARY_KEY')
        for k,v in row.items():
            if k not in ['scope','model']:compare_number(v,found.iloc[0][k],'CANCELLATION_SUMMARY:'+k)
    changes=pd.read_parquet(source(out/'scoring/cancellation_changes.parquet'))
    for base in changes.baseline.unique():
        q=check[check.model.eq(CANDIDATE)][KEY+['segment_ae','cumulative_ae','cancellation']].merge(check[check.model.eq(base)][KEY+['segment_ae','cumulative_ae','cancellation']],on=KEY,validate='one_to_one',suffixes=('_candidate','_base'))
        q=q.merge(changes[changes.baseline.eq(base)],on=KEY,validate='one_to_one')
        expected_changes=dict(segment_ae_improvement=q.segment_ae_base-q.segment_ae_candidate,cancellation_gain=q.cancellation_candidate-q.cancellation_base,cumulative_ae_improvement=q.cumulative_ae_base-q.cumulative_ae_candidate)
        for c,values in expected_changes.items():
            if not np.allclose(values,q[c],rtol=1e-12,atol=1e-8,equal_nan=True):raise ValueError('CANCELLATION_CHANGE:'+c)
        if not np.allclose(q.cumulative_ae_improvement,q.segment_ae_improvement+q.cancellation_gain,rtol=1e-12,atol=1e-8,equal_nan=True):raise ValueError('CANCELLATION_IDENTITY')
    for name,rows in [('metrics',metricchecks),('joint',jointchecks),('migrations',migrationchecks),('endpoint_groups',groupchecks),('convexity',bounds),('cancellation',cancelrows),('cancellation_summary',cancelsummary)]:
        pd.DataFrame(rows).to_csv(dest/('independent_'+name+'.csv'),index=False,encoding='utf-8-sig')
    candidate_code=Path(__file__).resolve().parent/'blend50_l1_mean.py';review=source_code_review(candidate_code)
    for p,h in sources.items():
        if sha(p)!=h:raise ValueError('SOURCE_CHANGED_AT_END:'+p)
    result=dict(status='PASS',started_at=started,completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        full_core_n=2820,paired_n=2579,missing_output_n=241,not_matured_n=len(excluded),decline_n=842,shadow_files_unchanged=len(shadow_paths),protected_files_unchanged=len(manifest['protected_files']),gates=gates,
        candidate_formula='0.5 * BLEND50 + 0.5 * L1; cumulative=sum of combined segments',
        independent_endpoints=True,common_hits_preserved=True,convexity_passed=True,cancellation_identity_passed=True,
        code_review=review,validator_training_calls=0,validator_inference_calls=0,sources=sources,script_sha256=sha(__file__))
    dump(dest/'validation.json',result);print(json.dumps({k:v for k,v in result.items() if k!='sources'},ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args();run(a.root,a.out)
