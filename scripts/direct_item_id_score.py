"""Score the sealed item-identity increment against unchanged historical comparators."""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.direct_item_id_v1 import CANDIDATE, read, dump, sha, verify, now
    from scripts.yearago_gate_score import metrics, objects, joints
except ModuleNotFoundError:
    from direct_item_id_v1 import CANDIDATE, read, dump, sha, verify, now
    from yearago_gate_score import metrics, objects, joints

KEY=['issue_date','shop','spu']
WINDOWS=['d01_30','d31_60','d01_60']
MODELS=['DIRECT','BLEND50','F28','DIRECT_TRAFFIC_STATE_V1',CANDIDATE]


def scope(g,name):return g if name=='FULL_CORE' else g[g.paired]


def score(out):
    out=Path(out).resolve();dest=out/'scoring'
    if dest.exists():raise ValueError('SCORING_EXISTS')
    conf=read(out/'configuration.json');prep=read(out/'prepare_manifest.json');effects=read(out/'effects_manifest.json')
    verify(prep['sources']);verify({str(out/p):h for p,h in effects['files'].items()})
    root=Path(conf['root']);oldroot=root/'core_model_complementarity_20261028_v1'
    om=read(oldroot/'manifest.json');verify({str(oldroot/p):h for p,h in om['files'].items()})
    original=pd.read_parquet(oldroot/'historical_windows.parquet')
    oldobjects=pd.read_parquet(oldroot/'objects.parquet')
    old=original[original.model.isin(MODELS)].copy()
    traffic=pd.read_parquet(root/'direct_traffic_state_v1_20261028/scoring/scores.parquet')
    traffic=traffic[traffic.model.eq('DIRECT_TRAFFIC_STATE_V1')]
    old=pd.concat([old,traffic],ignore_index=True)
    core=pd.read_parquet(out/'prepared/core.parquet')
    grid=pd.read_csv(out/'prepared/issue_grid.csv')
    pieces=[]
    for issue in grid.issue_date:
        for segment in ['M1','M2']:
            p=out/'effects'/f'{issue}_{segment}'/'predictions.parquet'
            z=pd.read_parquet(p);z['segment']=segment
            pieces.append(z)
    segment_predictions=pd.concat(pieces,ignore_index=True)
    if segment_predictions.duplicated(KEY+['segment']).any():raise ValueError('DUPLICATE_SEGMENT')
    p=segment_predictions.pivot(index=KEY,columns='segment',values='prediction')
    origin=segment_predictions.pivot(index=KEY,columns='segment',values='prediction_origin')
    status=segment_predictions.pivot(index=KEY,columns='segment',values='prediction_status')
    records=[]
    for k,r in p.iterrows():
        origin1,origin2=origin.loc[k,'M1'],origin.loc[k,'M2']
        complete=bool(np.isfinite(r.M1)&np.isfinite(r.M2))
        provenance=('NO_OUTPUT' if not complete else 'NEW_MODEL_BOTH_SEGMENTS' if origin1==origin2=='NEW_MODEL'
                    else 'REFERENCE_DIRECT_BOTH_SEGMENTS' if origin1==origin2=='REFERENCE_DIRECT_NO_NEW_INFORMATION'
                    else 'MIXED_NEW_AND_REFERENCE')
        for w,value in [('d01_30',r.M1),('d31_60',r.M2),('d01_60',r.M1+r.M2)]:
            records.append(dict(zip(KEY,k))|dict(window=w,item_prediction=float(value),prediction_origin=provenance,
                origin_M1=origin1,origin_M2=origin2,candidate_prediction_status='OK' if np.isfinite(value) else str(status.loc[k,'M1'])+'|'+str(status.loc[k,'M2'])))
    preds=pd.DataFrame(records)
    base=old[old.model.eq('DIRECT')].drop(columns=['prediction_origin','origin_M1','origin_M2'],errors='ignore')
    candidate=base.merge(preds,on=KEY+['window'],how='left',validate='one_to_one')
    if len(candidate)!=len(core)*3 or candidate.prediction_origin.isna().any():raise ValueError('CANDIDATE_SCOPE')
    candidate['prediction']=candidate.pop('item_prediction');candidate['model']=CANDIDATE
    candidate['prediction_status']=candidate.pop('candidate_prediction_status')
    scores=pd.concat([old,candidate],ignore_index=True)
    unique=KEY+['model','window']
    if scores.duplicated(unique).any():raise ValueError('DUPLICATE_SCORES')
    # Original groups are labels for post-hoc evaluation only.
    groups=oldobjects[KEY+['paired','maturity','lifecycle','current_category','decline_group','BLEND50_joint_hit',
        'blend_rescued','all_models_output','any_joint_hit','union_status']].copy()
    groups['old_group']=np.select([groups.BLEND50_joint_hit,groups.blend_rescued,
        groups.all_models_output&~groups.any_joint_hit],['BLEND50_ORIGINAL_HIT','OTHER_MODELS_ORIGINAL_RESCUE',
        'ALL_SIX_ORIGINAL_NO_JOINT_HIT'],default='MISSING_OR_UNDETERMINED')
    support=pd.read_parquet(out/'identity_support.parquet')
    if support.duplicated(KEY+['segment']).any():raise ValueError('DUPLICATE_SUPPORT')
    by=support.groupby(KEY).agg(seen_segments=('seen_in_training','sum'),training_dates_min=('training_dates','min'),
        training_span_days_min=('training_span_days','min'),nonoverlap_min=('nonoverlap_windows','min')).reset_index()
    by['seen_group']=np.select([by.seen_segments.eq(2),by.seen_segments.eq(1)],['SEEN_BOTH','SEEN_ONE_SEGMENT'],default='UNSEEN_BOTH')
    by['support_band']=pd.cut(by.nonoverlap_min,[-1,0,3,11,np.inf],labels=['0','1-3','4-11','12+']).astype(str)
    groups=groups.merge(by,on=KEY,validate='one_to_one')
    obj=objects(scores).merge(groups,on=KEY,validate='many_to_one')
    for d in [scores,obj]:
        if 'paired' in d:d.drop(columns='paired',inplace=True)
        d['year']=d.issue_date.str[:4]
    obj=obj.merge(groups[KEY+['paired']],on=KEY,validate='many_to_one')
    scores=scores.drop(columns=['old_group'],errors='ignore').merge(groups[KEY+['paired','old_group','seen_group','support_band','training_dates_min','training_span_days_min','nonoverlap_min']],on=KEY,validate='many_to_one')
    for d in [scores,obj]:
        d['origin_scope']='EXISTING_COMPARATOR'
    origin_keys=preds[preds.window.eq('d01_60')][KEY+['prediction_origin']]
    obj=obj.merge(origin_keys,on=KEY,validate='many_to_one')
    obj.loc[obj.model.eq(CANDIDATE),'origin_scope']=obj.loc[obj.model.eq(CANDIDATE),'prediction_origin']
    scores.loc[scores.model.eq(CANDIDATE),'origin_scope']=scores.loc[scores.model.eq(CANDIDATE),'prediction_origin']
    dims={'overall':[],'issue':['issue_date'],'shop':['shop'],'year':['year'],'maturity':['maturity'],
        'decline_group':['decline_group'],'old_group':['old_group'],'output_origin':['origin_scope'],'seen':['seen_group'],'support':['support_band']}
    met=[];joint=[]
    for scope_name in ['FULL_CORE','PAIRED']:
        for label,cols in dims.items():
            for vals,g in scope(scores,scope_name).groupby(['model','window']+cols,dropna=False):
                met.append(dict(scope=scope_name,slice=label,**dict(zip(['model','window']+cols,vals)),**metrics(g)))
            for vals,g in scope(obj,scope_name).groupby(['model']+cols,dropna=False):
                joint.append(dict(scope=scope_name,slice=label,**dict(zip(['model']+cols,vals)),**joints(g)))
    met,joint=pd.DataFrame(met),pd.DataFrame(joint)
    details=groups.copy()
    for model in MODELS:
        cols=KEY+['joint_hit','output_complete']+['prediction_'+w for w in WINDOWS]
        if model=='DIRECT':cols+=['actual_'+w for w in WINDOWS]
        d=obj[obj.model.eq(model)][cols].rename(columns={x:model+'_'+x for x in cols if x in ['joint_hit','output_complete'] or x.startswith('prediction_')})
        details=details.drop(columns=model+'_joint_hit',errors='ignore').merge(d,on=KEY,validate='one_to_one')
    details=details.merge(origin_keys,on=KEY,validate='one_to_one')
    mig=[]
    for base_model in ['DIRECT','BLEND50']:
        a,b=details[base_model+'_joint_hit'],details[CANDIDATE+'_joint_hit']
        details['migration_vs_'+base_model]=np.select([a&b,~a&b,a&~b],['RETAINED','GAINED','LOST'],default='NEITHER')
        for sc in ['FULL_CORE','PAIRED']:
            q=scope(details,sc)
            for label,cols in {'overall':[],'old_group':['old_group'],'issue':['issue_date'],'shop':['shop']}.items():
                for vals,g in q.groupby(cols+['migration_vs_'+base_model],dropna=False):
                    mig.append(dict(scope=sc,baseline=base_model,slice=label,**dict(zip(cols+['migration'],vals)),
                        records=len(g),actual60=float(g.actual_d01_60.sum())))
    gates={}
    pm=met[met.scope.eq('PAIRED')&met.slice.eq('overall')&met.window.eq('d01_60')].set_index('model')
    pj=joint[joint.scope.eq('PAIRED')&joint.slice.eq('overall')].set_index('model')
    dg=met[met.scope.eq('PAIRED')&met.slice.eq('decline_group')&met.window.eq('d01_60')&met.decline_group.eq('PRE_ISSUE_DECLINING')].set_index('model')
    if not dg.fixed_n.eq(842).all():raise ValueError('ORIGINAL_DECLINE_SCOPE_CHANGED')
    for bm in ['DIRECT','BLEND50']:
        c=CANDIDATE
        gates[bm]=dict(wape60_lower=bool(pm.loc[c,'wape']<pm.loc[bm,'wape']),
            joint_hits_increased=bool(pj.loc[c,'joint_hit_n']>pj.loc[bm,'joint_hit_n']),
            joint_hit_sales_share_not_lower=bool(pj.loc[c,'joint_hit_actual_share']>=pj.loc[bm,'joint_hit_actual_share']),
            decline_over_rate_not_increased=bool(dg.loc[c,'over_rate']<=dg.loc[bm,'over_rate']),
            output_and_core_coverage_not_reduced=bool(details[c+'_output_complete'].eq(details[bm+'_output_complete']).all()))
    dirs=pd.read_csv(oldroot/'all_miss_directions_detail.csv',float_precision='round_trip')
    dirs=dirs[dirs.scope.eq('PAIRED')][KEY+['window','direction','error_band']]
    dr=scores.merge(dirs,on=KEY+['window'],how='inner',validate='many_to_one')
    direction_metrics=[]
    for dim in ['direction','error_band']:
        for vals,g in dr.groupby(['model','window',dim]):
            direction_metrics.append(dict(group_kind=dim,**dict(zip(['model','window','old_direction'],vals)),**metrics(g)))
    top=pd.read_csv(oldroot/'top20_products.csv')[['shop','spu','rank']]
    topdetails=details[details.paired].merge(top,on=['shop','spu'],how='inner',validate='many_to_one')
    topp=[]
    for (rank,shop,spu),g in topdetails.groupby(['rank','shop','spu']):
        row=dict(rank=int(rank),shop=shop,spu=spu,records=len(g),actual60=float(g.actual_d01_60.sum()))
        for model in MODELS:
            error=g[model+'_prediction_d01_60']-g.actual_d01_60
            row[model+'_ae60']=float(error.abs().sum());row[model+'_joint_hit_n']=int(g[model+'_joint_hit'].sum())
        row['candidate_gained_vs_blend']=int(g.migration_vs_BLEND50.eq('GAINED').sum())
        row['candidate_lost_vs_blend']=int(g.migration_vs_BLEND50.eq('LOST').sum())
        ca=(g[CANDIDATE+'_prediction_d01_60']-g.actual_d01_60).abs()
        for bm in ['DIRECT','BLEND50']:
            change=ca-(g[bm+'_prediction_d01_60']-g.actual_d01_60).abs()
            row['periods_improved_vs_'+bm]=int(change.lt(0).sum())
            row['periods_worse_vs_'+bm]=int(change.gt(0).sum())
            row['largest_ae_increase_vs_'+bm]=float(change.max())
            row['largest_ae_increase_issue_vs_'+bm]=g.loc[change.idxmax(),'issue_date']
        topp.append(row)
    dest.mkdir(exist_ok=False)
    tables={'scores':scores,'objects':obj,'details':details,'metrics':met,'joint_summary':joint,
        'joint_migrations':pd.DataFrame(mig),'old_direction_metrics':pd.DataFrame(direction_metrics),
        'original_top20':pd.DataFrame(topp),'original_top20_issue_detail':topdetails,'segment_predictions':segment_predictions}
    for name,d in tables.items():
        d.to_parquet(dest/(name+'.parquet'),index=False);d.to_csv(dest/(name+'.csv'),index=False,encoding='utf-8-sig')
    summary=dict(candidate=CANDIDATE,at=now(),full_core_n=len(details),paired_n=int(details.paired.sum()),
        missing_output_n=int((~details[CANDIDATE+'_output_complete']).sum()),formal_fits=effects['formal_fits'],identity_seen_counts=details.seen_group.value_counts().to_dict(),
        gates=gates,business_screen={k:'PASS' if all(g.values()) else 'FAIL' for k,g in gates.items()},
        evidence='PARTIAL: reconstructed historical development; no independent OOS or deployment claim',
        common_failure_group_n=int(details.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT').sum()),
        candidate_origin_counts=details.prediction_origin.value_counts().to_dict(),
        full_metrics=met[met.scope.eq('FULL_CORE')&met.slice.eq('overall')].to_dict('records'),
        paired_metrics=pm.reset_index().to_dict('records'),paired_joint=pj.reset_index().to_dict('records'),
        decline60=dg.reset_index().to_dict('records'),database_queries=0,api_calls=0,production_changes=0)
    # Preserve unrounded floats; optional dimensions become JSON null.
    def clean(x):
        if isinstance(x,dict):return {k:clean(v) for k,v in x.items()}
        if isinstance(x,list):return [clean(v) for v in x]
        if isinstance(x,np.generic):x=x.item()
        if isinstance(x,float) and not np.isfinite(x):return None
        return x
    summary=clean(summary)
    dump(dest/'summary.json',summary)
    verify(prep['sources'])
    dump(dest/'manifest.json',dict(at=now(),script_sha256=sha(__file__),effects_manifest_sha256=sha(out/'effects_manifest.json'),
        files={p.name:sha(p) for p in dest.iterdir() if p.is_file()}))
    print({'gates':gates,'formal_fits':effects['formal_fits']},flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();score(a.out)
