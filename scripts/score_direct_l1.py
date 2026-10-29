"""Score sealed fixed L1 outputs, with no fitting or model inference."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.yearago_gate_score import metrics as base_metrics, objects, joints
except ModuleNotFoundError:
    from yearago_gate_score import metrics as base_metrics, objects, joints

CANDIDATE='DIRECT_L1_V1'
KEY=['issue_date','shop','spu']
WINDOWS=['d01_30','d31_60','d01_60']
SIDE_WEIGHT='DIRECT_CORE_WEIGHT2_V1'
MODELS=['DIRECT','BLEND50','F28',SIDE_WEIGHT,CANDIDATE]

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def clean(x):
    if isinstance(x,dict):return {k:clean(v) for k,v in x.items()}
    if isinstance(x,list):return [clean(v) for v in x]
    if isinstance(x,np.generic):x=x.item()
    if isinstance(x,float) and not np.isfinite(x):return None
    return x
def dump(p,x):Path(p).write_text(json.dumps(clean(x),ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')
def verify(files):
    for p,h in files.items():
        if sha(p)!=h:raise ValueError('HASH_CHANGED:'+str(p))
def scope(g,name):return g if name=='FULL_CORE' else g[g.paired]

def metrics(g):
    result=base_metrics(g)
    valid=np.isfinite(g.actual)&np.isfinite(g.prediction)
    zero=g.actual.gt(0)&g.prediction.eq(0)&valid
    result.update(prediction_units=float(g.loc[valid,'prediction'].sum()),
        output_prediction_units=float(g.prediction.sum(min_count=1)),
        positive_actual_zero_prediction_n=int(zero.sum()),
        positive_actual_zero_prediction_actual=float(g.loc[zero,'actual'].sum()),
        raw_negative_n=int(g.raw_prediction.lt(0).sum()) if 'raw_prediction' in g else 0,
        raw_negative_segment_count=int(g.raw_negative_segment_count.sum()) if 'raw_negative_segment_count' in g else 0)
    return result

def assemble_candidate(base,segments):
    """Seal comes before truth; joins preserve exact objects and calendar bounds."""
    if segments.duplicated(KEY+['segment']).any():raise ValueError('DUPLICATE_SEGMENT')
    if not segments.segment.isin(['M1','M2']).all():raise ValueError('BAD_SEGMENT')
    for seg,w in [('M1','d01_30'),('M2','d31_60')]:
        z=segments[segments.segment.eq(seg)]
        q=base[base.window.eq(w)][KEY+['target_start','target_end']].merge(z,on=KEY,validate='one_to_one',how='outer',indicator=True)
        if not q['_merge'].eq('both').all():raise ValueError('SEGMENT_OBJECT_SCOPE')
        for a,b in [('target_start','label_start'),('target_end','label_end')]:
            if not pd.to_datetime(q[a]).eq(pd.to_datetime(q[b])).all():raise ValueError('SEGMENT_DATE_CHANGED')
    p=segments.pivot(index=KEY,columns='segment',values='prediction')
    raw=segments.pivot(index=KEY,columns='segment',values='raw_prediction')
    if np.isinf(p.to_numpy(float)).any() or np.isinf(raw.to_numpy(float)).any():
        raise ValueError('INFINITE_PREDICTION')
    s=segments.pivot(index=KEY,columns='segment',values='prediction_status')
    records=[]
    for k,r in p.iterrows():
        rr=raw.loc[k]
        if not np.array_equal(np.maximum(rr[['M1','M2']].to_numpy(float),0),r[['M1','M2']].to_numpy(float),equal_nan=True):
            raise ValueError('POSTPROCESS_CHANGED')
        if np.isfinite([r.M1,r.M2]).any() and ((np.isfinite(r.M1) and r.M1<0) or (np.isfinite(r.M2) and r.M2<0)):
            raise ValueError('NEGATIVE_PREDICTION')
        for w,value,raw_value,neg_count in [('d01_30',r.M1,rr.M1,int(rr.M1<0)),('d31_60',r.M2,rr.M2,int(rr.M2<0)),
                ('d01_60',r.M1+r.M2,rr.M1+rr.M2,int(rr.M1<0)+int(rr.M2<0))]:
            records.append(dict(zip(KEY,k))|dict(window=w,new_prediction=float(value),new_raw_prediction=float(raw_value),
                new_raw_negative_segment_count=neg_count,
                new_status='OK' if np.isfinite(value) else str(s.loc[k,'M1'])+'|'+str(s.loc[k,'M2'])))
    z=base.merge(pd.DataFrame(records),on=KEY+['window'],validate='one_to_one',how='outer',indicator=True)
    if not z['_merge'].eq('both').all():raise ValueError('CANDIDATE_SCOPE')
    z.drop(columns='_merge',inplace=True);z['prediction']=z.pop('new_prediction');z['prediction_status']=z.pop('new_status');z['model']=CANDIDATE
    z['raw_prediction']=z.pop('new_raw_prediction');z['raw_negative_segment_count']=z.pop('new_raw_negative_segment_count')
    z['prediction_origin']=np.where(z.prediction.notna(),'NEW_L1_MODEL','NO_OUTPUT')
    return z

def score(out):
    out=Path(out).resolve();dest=out/'scoring'
    if dest.exists():raise ValueError('SCORING_EXISTS')
    conf=read(out/'configuration.json');prep=read(out/'prepare_manifest.json');effects=read(out/'effects_manifest.json')
    if not 0<effects['formal_fits']<=40:raise ValueError('FIT_BUDGET')
    verify(prep['sources']);verify({str(out/p):h for p,h in effects['files'].items()})
    root=Path(conf['root']);oldroot=root/'core_model_complementarity_20261028_v1'
    om=read(oldroot/'manifest.json')
    source_files=['historical_windows.parquet','objects.parquet','all_miss_directions_detail.csv','top20_products.csv']
    verify({str(oldroot/p):om['files'][p] for p in source_files})
    # Target truths are first accessed only after the candidate effects seal above.
    original=pd.read_parquet(oldroot/'historical_windows.parquet')
    oldobjects=pd.read_parquet(oldroot/'objects.parquet')
    old=original[original.model.isin(MODELS)].copy()
    weight_root=root/'direct_core_weight2_v1_20261028'
    weight_manifest=read(weight_root/'scoring/manifest.json')
    weight_file=weight_root/'scoring/scores.parquet'
    verify({str(weight_file):weight_manifest['files']['scores.parquet']})
    side=pd.read_parquet(weight_file,filters=[('model','==',SIDE_WEIGHT)])
    old=pd.concat([old,side],ignore_index=True)
    core=pd.read_parquet(out/'prepared/core.parquet');grid=pd.read_csv(out/'prepared/issue_grid.csv')
    if len(core)!=2820 or len(grid)!=20:raise ValueError('FROZEN_CORE_SCOPE')
    pieces=[]
    for issue in grid.issue_date:
        for segment in ['M1','M2']:
            z=pd.read_parquet(out/'effects'/f'{issue}_{segment}'/'predictions.parquet');z['segment']=segment;pieces.append(z)
    segments=pd.concat(pieces,ignore_index=True)
    candidate=assemble_candidate(old[old.model.eq('DIRECT')],segments)
    scores=pd.concat([old,candidate],ignore_index=True)
    if len(scores)!=2820*3*len(MODELS) or scores.duplicated(KEY+['model','window']).any():raise ValueError('SCORE_SCOPE_OR_DUPLICATES')
    groups=oldobjects[KEY+['paired','maturity','lifecycle','current_category','decline_group','BLEND50_joint_hit',
        'blend_rescued','all_models_output','any_joint_hit','union_status']].copy()
    groups['old_group']=np.select([groups.BLEND50_joint_hit,groups.blend_rescued,groups.all_models_output&~groups.any_joint_hit],
        ['BLEND50_ORIGINAL_HIT','OTHER_MODELS_ORIGINAL_RESCUE','ALL_SIX_ORIGINAL_NO_JOINT_HIT'],default='MISSING_OR_UNDETERMINED')
    if len(groups)!=2820 or groups.paired.sum()!=2579 or groups.BLEND50_joint_hit.sum()!=284 or groups.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT').sum()!=1953:
        raise ValueError('ORIGINAL_GROUPS_CHANGED')
    obj=objects(scores).merge(groups,on=KEY,validate='many_to_one')
    drop=[c for c in groups.columns if c not in KEY and c in scores]
    scores=scores.drop(columns=drop).merge(groups,on=KEY,validate='many_to_one')
    for _,g in scores.groupby(KEY+['window']):
        if g.actual.nunique(dropna=False)!=1:raise ValueError('COMPARATOR_TRUTH_CONFLICT')
    scores['error']=scores.prediction-scores.actual
    scores['absolute_error']=scores.error.abs();scores['over_units']=scores.error.clip(lower=0);scores['under_units']=(-scores.error).clip(lower=0)
    scores['hit']=scores.actual.gt(0)&scores.prediction.notna()&(scores.error.abs()/scores.actual).le(.2)
    for d in [scores,obj]:d['year']=d.issue_date.str[:4]
    for model,g in scores.groupby('model'):
        if g[g.paired].shape[0]!=2579*3:raise ValueError('PAIR_SCOPE:'+model)
    dims={'overall':[],'issue':['issue_date'],'shop':['shop'],'year':['year'],'maturity':['maturity'],
        'decline_group':['decline_group'],'old_group':['old_group'],'category':['current_category']}
    met=[];joint=[]
    for sc in ['FULL_CORE','PAIRED']:
        for label,cols in dims.items():
            for vals,g in scope(scores,sc).groupby(['model','window']+cols,dropna=False):
                met.append(dict(scope=sc,slice=label,**dict(zip(['model','window']+cols,vals)),**metrics(g)))
            for vals,g in scope(obj,sc).groupby(['model']+cols,dropna=False):
                joint.append(dict(scope=sc,slice=label,**dict(zip(['model']+cols,vals)),**joints(g)))
    met,joint=pd.DataFrame(met),pd.DataFrame(joint)
    details=groups.copy()
    for model in MODELS:
        cols=KEY+['joint_hit','output_complete']+['prediction_'+w for w in WINDOWS]
        if model=='DIRECT':cols+=['actual_'+w for w in WINDOWS]
        z=obj[obj.model.eq(model)][cols].rename(columns={c:model+'_'+c for c in cols if c in ['joint_hit','output_complete'] or c.startswith('prediction_')})
        details=details.drop(columns=model+'_joint_hit',errors='ignore').merge(z,on=KEY,validate='one_to_one')
    if not details[CANDIDATE+'_output_complete'].eq(details.DIRECT_output_complete).all():raise ValueError('ORIGINAL_OUTPUT_QUALIFICATION_CHANGED')
    migrations=[]
    for baseline in ['DIRECT','BLEND50']:
        a,b=details[baseline+'_joint_hit'],details[CANDIDATE+'_joint_hit']
        details['migration_vs_'+baseline]=np.select([a&b,~a&b,a&~b],['RETAINED','GAINED','LOST'],default='NEITHER')
        for sc in ['FULL_CORE','PAIRED']:
            for label,cols in {'overall':[],'old_group':['old_group'],'issue':['issue_date'],'shop':['shop']}.items():
                for vals,g in scope(details,sc).groupby(cols+['migration_vs_'+baseline],dropna=False):
                    migrations.append(dict(scope=sc,baseline=baseline,slice=label,**dict(zip(cols+['migration'],vals)),records=len(g),actual60=float(g.actual_d01_60.sum())))
    pm=met[met.scope.eq('PAIRED')&met.slice.eq('overall')&met.window.eq('d01_60')].set_index('model')
    pj=joint[joint.scope.eq('PAIRED')&joint.slice.eq('overall')].set_index('model')
    dg=met[met.scope.eq('PAIRED')&met.slice.eq('decline_group')&met.window.eq('d01_60')&met.decline_group.eq('PRE_ISSUE_DECLINING')].set_index('model')
    if not dg.fixed_n.eq(842).all():raise ValueError('DECLINE_SCOPE')
    gates={}
    for baseline in ['DIRECT','BLEND50']:
        gates[baseline]=dict(wape60_lower=bool(pm.loc[CANDIDATE,'wape']<pm.loc[baseline,'wape']),
          joint_hits_increased=bool(pj.loc[CANDIDATE,'joint_hit_n']>pj.loc[baseline,'joint_hit_n']),
          joint_hit_sales_share_not_lower=bool(pj.loc[CANDIDATE,'joint_hit_actual_share']>=pj.loc[baseline,'joint_hit_actual_share']),
          decline_over_rate_not_increased=bool(dg.loc[CANDIDATE,'over_rate']<=dg.loc[baseline,'over_rate']),
          output_and_core_coverage_not_reduced=bool(details[CANDIDATE+'_output_complete'].ge(details[baseline+'_output_complete']).all()))
    dirs=pd.read_csv(oldroot/'all_miss_directions_detail.csv',float_precision='round_trip')
    dirs=dirs[dirs.scope.eq('PAIRED')][KEY+['window','direction','error_band']]
    dr=scores.merge(dirs,on=KEY+['window'],how='inner',validate='many_to_one');direction_metrics=[]
    for dim in ['direction','error_band']:
        for vals,g in dr.groupby(['model','window',dim]):
            direction_metrics.append(dict(group_kind=dim,**dict(zip(['model','window','old_direction'],vals)),**metrics(g)))
    direction_changes=[]
    for (window,direction),group in dr.groupby(['window','direction']):
        cand=group[group.model.eq(CANDIDATE)].set_index(KEY)
        for baseline in ['DIRECT','BLEND50']:
            oldgroup=group[group.model.eq(baseline)].set_index(KEY).reindex(cand.index)
            delta=cand.prediction-oldgroup.prediction
            direction_changes.append(dict(window=window,old_direction=direction,baseline=baseline,records=len(cand),
                prediction_change=float(delta.sum()),prediction_lower_n=int(delta.lt(0).sum()),prediction_higher_n=int(delta.gt(0).sum()),
                under_change=float(cand.under_units.sum()-oldgroup.under_units.sum()),
                over_change=float(cand.over_units.sum()-oldgroup.over_units.sum()),
                ae_change=float(cand.absolute_error.sum()-oldgroup.absolute_error.sum()),
                hit_gained=int((cand.hit&~oldgroup.hit).sum()),hit_lost=int((~cand.hit&oldgroup.hit).sum())))
    top=pd.read_csv(oldroot/'top20_products.csv')[['shop','spu','rank']]
    if len(top)!=20 or top.duplicated(['shop','spu']).any():raise ValueError('ORIGINAL_TOP20')
    topdetails=details[details.paired].merge(top,on=['shop','spu'],how='inner',validate='many_to_one');toprows=[]
    for (rank,shop,spu),g in topdetails.groupby(['rank','shop','spu']):
        row=dict(rank=int(rank),shop=shop,spu=spu,records=len(g),actual60=float(g.actual_d01_60.sum()))
        for model in MODELS:
            e=g[model+'_prediction_d01_60']-g.actual_d01_60
            row[model+'_ae60']=float(e.abs().sum());row[model+'_over60']=float(e.clip(lower=0).sum());row[model+'_under60']=float((-e).clip(lower=0).sum());row[model+'_joint_hit_n']=int(g[model+'_joint_hit'].sum())
            row[model+'_prediction60']=float(g[model+'_prediction_d01_60'].sum())
        newae=(g[CANDIDATE+'_prediction_d01_60']-g.actual_d01_60).abs()
        for baseline in ['DIRECT','BLEND50']:
            change=newae-(g[baseline+'_prediction_d01_60']-g.actual_d01_60).abs()
            row['periods_improved_vs_'+baseline]=int(change.lt(0).sum());row['periods_worse_vs_'+baseline]=int(change.gt(0).sum())
            row['largest_ae_increase_vs_'+baseline]=float(change.max());row['largest_ae_increase_issue_vs_'+baseline]=g.loc[change.idxmax(),'issue_date']
        toprows.append(row)
    # Product contribution covers the full paired population, never only the original Top20.
    product_contrib=[]
    for (shop,spu),g in details[details.paired].groupby(['shop','spu']):
        newae=(g[CANDIDATE+'_prediction_d01_60']-g.actual_d01_60).abs()
        for baseline in ['DIRECT','BLEND50']:
            oldae=(g[baseline+'_prediction_d01_60']-g.actual_d01_60).abs();gain=oldae-newae
            pred_change=g[CANDIDATE+'_prediction_d01_60']-g[baseline+'_prediction_d01_60']
            product_contrib.append(dict(shop=shop,spu=spu,baseline=baseline,records=len(g),actual60=float(g.actual_d01_60.sum()),
                ae_reduction=float(gain.sum()),improved_periods=int(gain.gt(0).sum()),worse_periods=int(gain.lt(0).sum()),
                new_joint_hits=int(g[CANDIDATE+'_joint_hit'].sum()),baseline_joint_hits=int(g[baseline+'_joint_hit'].sum()),
                prediction_change=float(pred_change.sum()),prediction_lower_periods=int(pred_change.lt(0).sum()),
                prediction_higher_periods=int(pred_change.gt(0).sum()),candidate_prediction=float(g[CANDIDATE+'_prediction_d01_60'].sum()),
                baseline_prediction=float(g[baseline+'_prediction_d01_60'].sum()),
                candidate_under=float((g.actual_d01_60-g[CANDIDATE+'_prediction_d01_60']).clip(lower=0).sum()),
                baseline_under=float((g.actual_d01_60-g[baseline+'_prediction_d01_60']).clip(lower=0).sum())))
    dest.mkdir(exist_ok=False)
    tables={'scores':scores,'objects':obj,'details':details,'metrics':met,'joint_summary':joint,'joint_migrations':pd.DataFrame(migrations),
      'old_direction_metrics':pd.DataFrame(direction_metrics),'original_top20':pd.DataFrame(toprows),'original_top20_issue_detail':topdetails,
      'segment_predictions':segments,'product_contributions':pd.DataFrame(product_contrib),'old_direction_changes':pd.DataFrame(direction_changes)}
    for name,d in tables.items():d.to_parquet(dest/(name+'.parquet'),index=False);d.to_csv(dest/(name+'.csv'),index=False,encoding='utf-8-sig')
    summary=dict(candidate=CANDIDATE,at=now(),full_core_n=len(details),paired_n=int(details.paired.sum()),
       missing_output_n=int((~details[CANDIDATE+'_output_complete']).sum()),formal_fits=effects['formal_fits'],gates=gates,
       business_screen={k:'PASS' if all(v.values()) else 'FAIL' for k,v in gates.items()},
       evidence='PARTIAL: reconstructed historical development, not independent OOS',
       scoring_weights='NONE; original unweighted business scores retained',
       common_failure_group_n=int(details.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT').sum()),
       paired_metrics=pm.reset_index().to_dict('records'),paired_joint=pj.reset_index().to_dict('records'),decline60=dg.reset_index().to_dict('records'),
       source_files={**{str(oldroot/p):sha(oldroot/p) for p in source_files},str(weight_file):sha(weight_file)},database_queries=0,api_calls=0,production_changes=0)
    dump(dest/'summary.json',summary);verify(prep['sources'])
    dump(dest/'manifest.json',dict(at=now(),script_sha256=sha(__file__),effects_manifest_sha256=sha(out/'effects_manifest.json'),
        files={p.name:sha(p) for p in dest.iterdir() if p.is_file()}))
    print(json.dumps({'gates':gates,'formal_fits':effects['formal_fits']},ensure_ascii=False),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();score(a.out)
