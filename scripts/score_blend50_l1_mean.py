"""Unweighted evaluation of sealed arithmetic predictions; no fit or inference."""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.score_direct_l1 import sha,read,now,dump,verify,scope,metrics,objects,joints
except ModuleNotFoundError:
    from score_direct_l1 import sha,read,now,dump,verify,scope,metrics,objects,joints

C='BLEND50_L1_MEAN_V1';L='DIRECT_L1_V1';B='BLEND50'
KEY=['issue_date','shop','spu'];W=['d01_30','d31_60','d01_60']
MODELS=['DIRECT',B,L,'F28',C];BASELINES=[B,'DIRECT',L]

def exact(a,b,label):
    if not np.array_equal(np.asarray(a,float),np.asarray(b,float),equal_nan=True):raise ValueError(label)

def assemble_candidate(base,candidate):
    """Dates, mean inputs, cumulative sum and scope are independent of truth."""
    k=KEY+['window']
    if base.duplicated(k).any() or candidate.duplicated(k).any():raise ValueError('DUPLICATE_KEY')
    z=base.merge(candidate,on=k,how='outer',validate='one_to_one',suffixes=('_base','_candidate'),indicator=True)
    if not z['_merge'].eq('both').all():raise ValueError('OBJECT_SCOPE')
    for field in ['target_start','target_end']:
        if not pd.to_datetime(z[field+'_base']).eq(pd.to_datetime(z[field+'_candidate'])).all():raise ValueError('WINDOW_CHANGED')
    for f in ['blend50_prediction','l1_prediction','prediction_candidate']:
        if np.isinf(z[f].to_numpy(float)).any() or z[f].lt(0).any():raise ValueError('INVALID_PREDICTION')
    segmented=z.window.isin(W[:2]);p=z.loc[segmented]
    exact(p.prediction_candidate,.5*p.blend50_prediction+.5*p.l1_prediction,'MEAN_FORMULA')
    pivot=z.pivot(index=KEY,columns='window',values='prediction_candidate')
    exact(pivot[W[2]],pivot[W[0]]+pivot[W[1]],'CUMULATIVE_NOT_SEGMENT_SUM')
    z['prediction']=z.prediction_candidate;z['prediction_status']=z.prediction_status_candidate;z['model']=C
    z['target_start']=z.target_start_base;z['target_end']=z.target_end_base
    if 'input_cutoff_candidate' in z:z['input_cutoff']=z.input_cutoff_candidate
    z['prediction_origin']=np.where(z.prediction.notna(),'FIXED_ARITHMETIC_MEAN','NO_OUTPUT')
    z=z.drop(columns=[x for x in z if x.endswith('_candidate') or x.endswith('_base')]+['_merge'])
    # Raw predictions of the endpoint model do not become new model outputs.
    z=z.drop(columns=['raw_prediction','raw_negative_segment_count'],errors='ignore')
    return z

def cancellation_frame(obj):
    z=obj[obj.model.isin([B,L,C])].copy()
    z['e1']=z.prediction_d01_30-z.actual_d01_30;z['e2']=z.prediction_d31_60-z.actual_d31_60
    z['segment_ae']=z.e1.abs()+z.e2.abs()
    z['cumulative_ae']=(z.prediction_d01_60-z.actual_d01_60).abs()
    z['cancellation']=z.segment_ae-z.cumulative_ae
    if z.cancellation.lt(-1e-8).any():raise ValueError('TRIANGLE_INEQUALITY')
    z['opposite_segment_signs']=z.e1*z.e2<0
    z['cumulative_hit_but_segment_miss']=z.hit_d01_60&~(z.hit_d01_30&z.hit_d31_60)
    return z

def score(out):
    out=Path(out).resolve();dest=out/'scoring'
    if dest.exists():raise ValueError('SCORING_EXISTS')
    cm=read(out/'candidate_manifest.json')
    root=Path(cm['root']);verify(cm['sources'])
    for p,h in cm['files'].items():
        if sha(out/p)!=h:raise ValueError('CANDIDATE_HASH_CHANGED:'+p)
    cand=pd.read_parquet(out/'candidate_predictions.parquet')
    oldroot=root/'core_model_complementarity_20261028_v1';om=read(oldroot/'manifest.json')
    files=['historical_windows.parquet','objects.parquet','all_miss_directions_detail.csv','top20_products.csv']
    verify({str(oldroot/p):om['files'][p] for p in files})
    lr=root/'direct_l1_v1_20261028';lm=read(lr/'scoring/manifest.json')
    verify({str(lr/'scoring/scores.parquet'):lm['files']['scores.parquet']})
    # First actual-sales read is strictly after the candidate seal checks above.
    old=pd.read_parquet(lr/'scoring/scores.parquet',filters=[('model','in',MODELS[:-1])])
    hist=pd.read_parquet(oldroot/'historical_windows.parquet')
    oldobjects=pd.read_parquet(oldroot/'objects.parquet')
    k=KEY+['model','window']
    chk=old[old.model.isin(['DIRECT',B,'F28'])].merge(hist[hist.model.isin(['DIRECT',B,'F28'])],on=k,validate='one_to_one',suffixes=('_new','_old'),how='outer',indicator=True)
    if not chk['_merge'].eq('both').all():raise ValueError('ORIGINAL_SCOPE')
    for field in ['prediction','actual']:exact(chk[field+'_new'],chk[field+'_old'],'ORIGINAL_'+field)
    for field in ['target_start','target_end']:
        if not pd.to_datetime(chk[field+'_new']).eq(pd.to_datetime(chk[field+'_old'])).all():raise ValueError('ORIGINAL_DATE')
    base=old[old.model.eq('DIRECT')]
    for model,field in [(B,'blend50_prediction'),(L,'l1_prediction')]:
        q=cand.merge(old[old.model.eq(model)][KEY+['window','prediction']],on=KEY+['window'],validate='one_to_one',how='outer',indicator=True)
        if not q['_merge'].eq('both').all():raise ValueError('ENDPOINT_SCOPE')
        exact(q[field],q.prediction_y,'ENDPOINT_VALUE_CHANGED')
    candidate=assemble_candidate(base,cand)
    scores=pd.concat([old,candidate],ignore_index=True)
    if len(scores)!=2820*3*len(MODELS) or scores.duplicated(k).any():raise ValueError('SCORED_SCOPE')
    groups=oldobjects[KEY+['paired','maturity','lifecycle','current_category','decline_group','BLEND50_joint_hit','blend_rescued','all_models_output','any_joint_hit','union_status']].copy()
    groups['old_group']=np.select([groups.BLEND50_joint_hit,groups.blend_rescued,groups.all_models_output&~groups.any_joint_hit],
       ['BLEND50_ORIGINAL_HIT','OTHER_MODELS_ORIGINAL_RESCUE','ALL_SIX_ORIGINAL_NO_JOINT_HIT'],default='MISSING_OR_UNDETERMINED')
    if len(groups)!=2820 or groups.paired.sum()!=2579 or groups.BLEND50_joint_hit.sum()!=284 or groups.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT').sum()!=1953:raise ValueError('FIXED_GROUPS_CHANGED')
    scores=scores.drop(columns=[c for c in groups if c not in KEY and c in scores]).merge(groups,on=KEY,validate='many_to_one')
    for _,g in scores.groupby(KEY+['window']):
        if g.actual.nunique(dropna=False)!=1:raise ValueError('TRUTH_CONFLICT')
        for field in ['target_start','target_end']:
            if pd.to_datetime(g[field]).nunique(dropna=False)!=1:raise ValueError('DATE_CONFLICT')
    scores['error']=scores.prediction-scores.actual;scores['absolute_error']=scores.error.abs()
    scores['over_units']=scores.error.clip(lower=0);scores['under_units']=(-scores.error).clip(lower=0)
    scores['hit']=scores.actual.gt(0)&scores.prediction.notna()&(scores.error.abs()/scores.actual).le(.2)
    convex=scores[scores.model.isin([B,L,C])].pivot(index=KEY+['window'],columns='model',values=['absolute_error','over_units','under_units'])
    convexity={}
    for field in ['absolute_error','over_units','under_units']:
        excess=convex[field][C]-(convex[field][B]+convex[field][L])*.5
        if excess.gt(1e-8).any():raise ValueError('CONVEXITY_'+field)
        convexity[field]=dict(max_raw_excess=float(excess.max()),finite_n=int(excess.notna().sum()),numerical_assertion_tolerance=1e-8)
    obj=objects(scores).merge(groups,on=KEY,validate='many_to_one')
    for g in [scores,obj]:g['year']=g.issue_date.str[:4]
    dims={'overall':[],'issue':['issue_date'],'shop':['shop'],'year':['year'],'maturity':['maturity'],'decline_group':['decline_group'],'old_group':['old_group'],'category':['current_category']}
    met=[];joint=[]
    for sc in ['FULL_CORE','PAIRED']:
        for label,cols in dims.items():
            for vals,g in scope(scores,sc).groupby(['model','window']+cols,dropna=False):met.append(dict(scope=sc,slice=label,**dict(zip(['model','window']+cols,vals)),**metrics(g)))
            for vals,g in scope(obj,sc).groupby(['model']+cols,dropna=False):joint.append(dict(scope=sc,slice=label,**dict(zip(['model']+cols,vals)),**joints(g)))
    met,joint=pd.DataFrame(met),pd.DataFrame(joint)
    details=groups.copy()
    for model in MODELS:
        cols=KEY+['joint_hit','output_complete']+['prediction_'+w for w in W]
        if model=='DIRECT':cols+=['actual_'+w for w in W]
        z=obj[obj.model.eq(model)][cols].rename(columns={c:model+'_'+c for c in cols if c in ['joint_hit','output_complete'] or c.startswith('prediction_')})
        details=details.drop(columns=model+'_joint_hit',errors='ignore').merge(z,on=KEY,validate='one_to_one')
    if not details[C+'_output_complete'].eq(details.DIRECT_output_complete).all():raise ValueError('QUALIFICATION_CHANGED')
    a,b=details[B+'_joint_hit'],details[L+'_joint_hit'];complete=details[B+'_output_complete']&details[L+'_output_complete']
    details['endpoint_group']=np.select([~complete,a&b,a&~b,~a&b],['MISSING','BOTH_HIT','BLEND_ONLY','L1_ONLY'],default='NEITHER')
    if not details.loc[details.endpoint_group.eq('BOTH_HIT'),C+'_joint_hit'].all():raise ValueError('BOTH_HIT_NOT_RETAINED')
    endgroups=[];migrations=[]
    for sc in ['FULL_CORE','PAIRED']:
        for name in ['BOTH_HIT','BLEND_ONLY','L1_ONLY','NEITHER','MISSING']:
            g=scope(details,sc);g=g[g.endpoint_group.eq(name)];hit=g[C+'_joint_hit']
            endgroups.append(dict(scope=sc,endpoint_group=name,records=len(g),candidate_hit_n=int(hit.sum()),candidate_hit_actual=float(g.loc[hit,'actual_d01_60'].sum()),actual_units=float(g.actual_d01_60.sum()),candidate_missing_n=int((~g[C+'_output_complete']).sum())))
    for baseline in BASELINES:
        a,b=details[baseline+'_joint_hit'],details[C+'_joint_hit'];details['migration_vs_'+baseline]=np.select([a&b,~a&b,a&~b],['RETAINED','GAINED','LOST'],default='NEITHER')
        for sc in ['FULL_CORE','PAIRED']:
            for label,cols in {'overall':[],'issue':['issue_date'],'shop':['shop'],'old_group':['old_group']}.items():
                for vals,g in scope(details,sc).groupby(cols+['migration_vs_'+baseline],dropna=False):migrations.append(dict(scope=sc,baseline=baseline,slice=label,**dict(zip(cols+['migration'],vals)),records=len(g),actual60=float(g.actual_d01_60.sum())))
    pm=met[met.scope.eq('PAIRED')&met.slice.eq('overall')&met.window.eq(W[2])].set_index('model')
    pj=joint[joint.scope.eq('PAIRED')&joint.slice.eq('overall')].set_index('model')
    dg=met[met.scope.eq('PAIRED')&met.slice.eq('decline_group')&met.window.eq(W[2])&met.decline_group.eq('PRE_ISSUE_DECLINING')].set_index('model')
    if not dg.fixed_n.eq(842).all():raise ValueError('DECLINE_SCOPE')
    gates={b:dict(wape60_lower=bool(pm.loc[C,'wape']<pm.loc[b,'wape']),joint_hits_increased=bool(pj.loc[C,'joint_hit_n']>pj.loc[b,'joint_hit_n']),
        joint_hit_sales_share_not_lower=bool(pj.loc[C,'joint_hit_actual_share']>=pj.loc[b,'joint_hit_actual_share']),decline_over_rate_not_increased=bool(dg.loc[C,'over_rate']<=dg.loc[b,'over_rate']),
        output_and_core_coverage_not_reduced=bool(details[C+'_output_complete'].ge(details[b+'_output_complete']).all())) for b in BASELINES}
    dirs=pd.read_csv(oldroot/'all_miss_directions_detail.csv',float_precision='round_trip');dirs=dirs[dirs.scope.eq('PAIRED')][KEY+['window','direction','error_band']]
    dr=scores.merge(dirs,on=KEY+['window'],how='inner',validate='many_to_one');direction_metrics=[]
    for dim in ['direction','error_band']:
        for vals,g in dr.groupby(['model','window',dim]):direction_metrics.append(dict(group_kind=dim,**dict(zip(['model','window','old_direction'],vals)),**metrics(g)))
    top=pd.read_csv(oldroot/'top20_products.csv')[['shop','spu','rank']];td=details[details.paired].merge(top,on=['shop','spu'],how='inner',validate='many_to_one')
    product=[];toprows=[]
    for (shop,spu),g in details[details.paired].groupby(['shop','spu']):
        newe=g[C+'_prediction_d01_60']-g.actual_d01_60
        for b in BASELINES:
            be=g[b+'_prediction_d01_60']-g.actual_d01_60;gain=be.abs()-newe.abs();pdiff=g[C+'_prediction_d01_60']-g[b+'_prediction_d01_60']
            product.append(dict(shop=shop,spu=spu,baseline=b,records=len(g),actual60=float(g.actual_d01_60.sum()),ae_reduction=float(gain.sum()),improved_periods=int(gain.gt(0).sum()),worse_periods=int(gain.lt(0).sum()),prediction_change=float(pdiff.sum()),prediction_lower_periods=int(pdiff.lt(0).sum()),candidate_under=float((-newe).clip(lower=0).sum()),baseline_under=float((-be).clip(lower=0).sum()),new_joint_hits=int(g[C+'_joint_hit'].sum()),baseline_joint_hits=int(g[b+'_joint_hit'].sum())))
    for (rank,shop,spu),g in td.groupby(['rank','shop','spu']):
        row=dict(rank=int(rank),shop=shop,spu=spu,records=len(g),actual60=float(g.actual_d01_60.sum()))
        for model in MODELS:
            er=g[model+'_prediction_d01_60']-g.actual_d01_60
            row.update({model+'_ae60':float(er.abs().sum()),model+'_under60':float((-er).clip(lower=0).sum()),model+'_over60':float(er.clip(lower=0).sum()),model+'_prediction60':float(g[model+'_prediction_d01_60'].sum()),model+'_joint_hit_n':int(g[model+'_joint_hit'].sum())})
        for b in BASELINES:
            change=(g[C+'_prediction_d01_60']-g.actual_d01_60).abs()-(g[b+'_prediction_d01_60']-g.actual_d01_60).abs()
            row.update({'periods_improved_vs_'+b:int(change.lt(0).sum()),'periods_worse_vs_'+b:int(change.gt(0).sum()),'prediction_lower_periods_vs_'+b:int(g[C+'_prediction_d01_60'].lt(g[b+'_prediction_d01_60']).sum())})
        toprows.append(row)
    cancel=cancellation_frame(obj);cs=[];changes=[]
    for sc in ['FULL_CORE','PAIRED']:
        for model,g in scope(cancel,sc).groupby('model'):
            valid=g.cumulative_ae.notna()&g.segment_ae.notna();q=g[valid]
            cs.append(dict(scope=sc,model=model,records=len(g),scored_n=len(q),unknown_n=int((~valid).sum()),e1=float(q.e1.sum()),e2=float(q.e2.sum()),segment_ae=float(q.segment_ae.sum()),cumulative_ae=float(q.cumulative_ae.sum()),cancellation=float(q.cancellation.sum()),opposite_segment_signs_n=int(q.opposite_segment_signs.sum()),cumulative_hit_but_segment_miss_n=int(q.cumulative_hit_but_segment_miss.sum())))
    nc=cancel[cancel.model.eq(C)].set_index(KEY)
    for b in [B,L]:
        bc=cancel[cancel.model.eq(b)].set_index(KEY).reindex(nc.index)
        q=nc[[]].copy();q['baseline']=b;q['paired']=nc.paired;q['segment_ae_improvement']=bc.segment_ae-nc.segment_ae
        q['cancellation_gain']=nc.cancellation-bc.cancellation;q['cumulative_ae_improvement']=bc.cumulative_ae-nc.cumulative_ae
        if not np.allclose(q.segment_ae_improvement+q.cancellation_gain,q.cumulative_ae_improvement,rtol=0,atol=1e-8,equal_nan=True):raise ValueError('CANCELLATION_IDENTITY')
        changes.append(q.reset_index())
    dest.mkdir(exist_ok=False)
    tables={'scores':scores,'objects':obj,'details':details,'metrics':met,'joint_summary':joint,'joint_migrations':pd.DataFrame(migrations),'endpoint_groups':pd.DataFrame(endgroups),
        'old_direction_metrics':pd.DataFrame(direction_metrics),'original_top20':pd.DataFrame(toprows),'original_top20_issue_detail':td,'product_contributions':pd.DataFrame(product),
        'cancellation_details':cancel,'cancellation_summary':pd.DataFrame(cs),'cancellation_changes':pd.concat(changes,ignore_index=True)}
    for name,g in tables.items():g.to_parquet(dest/(name+'.parquet'),index=False);g.to_csv(dest/(name+'.csv'),index=False,encoding='utf-8-sig')
    summary=dict(candidate=C,at=now(),full_core_n=len(details),paired_n=int(details.paired.sum()),missing_output_n=int((~details[C+'_output_complete']).sum()),
       gates=gates,business_screen={b:'PASS' if all(g.values()) else 'FAIL' for b,g in gates.items()},evidence='PARTIAL: reconstructed historical development, not independent OOS',
       paired_metrics=pm.reset_index().to_dict('records'),paired_joint=pj.reset_index().to_dict('records'),decline60=dg.reset_index().to_dict('records'),
       endpoint_groups=endgroups,convexity=convexity,scoring_weights='NONE',formal_fits=0,inference_calls=0,database_queries=0,api_calls=0,production_changes=0)
    dump(dest/'summary.json',summary);verify(cm['sources'])
    dump(dest/'manifest.json',dict(at=now(),script_sha256=sha(__file__),candidate_manifest_sha256=sha(out/'candidate_manifest.json'),files={p.name:sha(p) for p in dest.iterdir() if p.is_file()}))
    print(gates)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();score(a.out)
