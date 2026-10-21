"""One immutable arithmetic candidate; no model or database dependencies."""
from __future__ import annotations
import argparse
import importlib.metadata
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.yearago_gate_route import KEY, sha, now, dump, read_json, verify, unique, save
    from scripts.yearago_gate_score import metrics, objects, joints, migration
except ModuleNotFoundError:
    from yearago_gate_route import KEY, sha, now, dump, read_json, verify, unique, save
    from yearago_gate_score import metrics, objects, joints, migration

BASE='DIRECT30x2'
GATE='DIRECT_YEARAGO_GATE_V1'
CAND='DIRECT_YEARAGO_BLEND50'
YEAR='DIRECT_YEARAGO'
WINDOWS=['d01_30','d31_60','d01_60']
PREREG='d4bcdff72f5014913d0638868a32ca478ce4ff93'
PINNED={
    'routing_manifest.json':'a3ce4dc918a5e0039c0cb2f820d28fd4ce644e8fc7aafd92470f9b218c17c825',
    'scoring/manifest.json':'a610aee50283d2e688973456ffd77499ef4e5ca62a56d6c27ce75ab967cdadcd',
    'validation/validation.json':'6df002496da1570e3b1b51ebcd1571ae6e76846fc347e1487407fa38ef6f4735',
    'final_review/review.json':'101b90736153c3c4c3e22fcefc8d4dfde90c4e6ed8aac37fd9806940132dc9ab',
}
PRED_COLS=KEY+['input_cutoff','gate_decision','direct_M1','direct_M2','selected_M1','selected_M2','selected_60',
    'annual_M1','annual_M2','origin_M1','origin_M2','direct_source_sha256_M1','direct_source_sha256_M2',
    'annual_source_sha256_M1','annual_source_sha256_M2','failed_conditions','main_reason']
DIMS={'overall':[],'shop':['shop'],'issue':['issue_date'],'year':['year'],'maturity':['maturity'],
      'decline_group':['decline_group'],'category':['current_category'],'season':['current_season']}


def equal(a,b):
    return (pd.isna(a) and pd.isna(b)) or a==b


def blend_one(direct,gate,gate_decision):
    """No truth argument; FALSE returns the identical scalar without arithmetic."""
    if pd.isna(gate_decision):raise ValueError('MISSING_ORIGINAL_GATE_DECISION')
    if pd.isna(direct):
        if gate_decision or pd.notna(gate):raise ValueError('MISSING_DIRECT_SOURCE_CONFLICT')
        return direct
    if not np.isfinite(direct) or not np.isfinite(gate):raise ValueError('INVALID_ENDPOINT')
    if not gate_decision:
        if direct!=gate:raise ValueError('FALSE_GATE_DIFFERS_FROM_DIRECT')
        return direct
    return direct + 0.5 * (gate-direct)


def prepare(gate_archive,output):
    g,out=Path(gate_archive).resolve(),Path(output).resolve()
    if out.exists() or g in out.parents:raise ValueError('NEW_EXCLUSIVE_OUTPUT_REQUIRED')
    for rel,h in PINNED.items():
        if sha(g/rel)!=h:raise ValueError('PINNED_GATE_SOURCE_CHANGED:'+rel)
    rm=read_json(g/'routing_manifest.json');sm=read_json(g/'scoring/manifest.json')
    sources=rm['sources'].copy()
    verify(sources)
    if sha(g/'routing.parquet')!=rm['routing_sha256']:raise ValueError('ROUTING_CHANGED')
    verify({str(g/'scoring'/rel):h for rel,h in sm['files'].items()})
    for p in g.rglob('*'):
        if p.is_file():sources[str(p)]=sha(p)
    root=Path(__file__).resolve().parents[1]
    if sha(root/'scripts/yearago_gate_score.py')!=sm['script_sha256']:raise ValueError('HELPER_VERSION_CHANGED')
    for p in [Path(__file__),root/'scripts/yearago_gate_score.py',root/'docs/direct_yearago_blend50_protocol.md']:
        sources[str(p)]=sha(p)
    read_started=now()
    # The only parsed business input in this stage: an explicit forecast-only projection.
    r=pd.read_parquet(g/'routing.parquet',columns=PRED_COLS)
    unique(r,KEY)
    if r.gate_decision.isna().any():raise ValueError('GATE_DECISION_UNKNOWN')
    p=r.copy()
    for seg in ['M1','M2']:
        true=p[p.gate_decision]
        if not true['origin_'+seg].eq('ANNUAL_MODEL').all() or not true['selected_'+seg].eq(true['annual_'+seg]).all():
            raise ValueError('ANNUAL_ORIGIN_CONFLICT')
        p['prediction_'+seg]=[blend_one(a,b,c) for a,b,c in zip(p['direct_'+seg],p['selected_'+seg],p.gate_decision)]
    p['prediction_60']=p.prediction_M1+p.prediction_M2
    p['prediction_origin']=np.where(p.gate_decision,'FIXED_HALF_GATE_ADJUSTMENT','DIRECT_EXACT_COPY')
    p['prediction_status']=np.where(np.isfinite(p.prediction_M1)&np.isfinite(p.prediction_M2),'OK','ORIGINAL_MISSING_OUTPUT')
    p['candidate']=CAND
    for w,(a,b) in {'d01_30':(1,30),'d31_60':(31,60),'d01_60':(1,60)}.items():
        p['target_start_'+w]=pd.to_datetime(p.issue_date)+pd.to_timedelta(a,unit='d')
        p['target_end_'+w]=pd.to_datetime(p.issue_date)+pd.to_timedelta(b,unit='d')
    assert (pd.to_datetime(p.issue_date)-pd.Timedelta(days=2)).eq(p.input_cutoff).all()
    verify(sources)
    out.mkdir(exist_ok=False)
    save(out,'candidate_predictions',p)
    dump(out/'prediction_access_log.json',[dict(at=read_started,path=str(g/'routing.parquet'),columns=PRED_COLS,
                                               purpose='forecast only; no truth, error or hit parsed')])
    dump(out/'candidate_manifest.json',dict(candidate=CAND,weight=.5,formula='DIRECT + 0.5 * (GATE - DIRECT); FALSE exact DIRECT copy',
        preregistration_commit=PREREG,sealed_at=now(),gate_archive=str(g),sources=sources,
        candidate_sha256=sha(out/'candidate_predictions.parquet'),script_sha256=sha(__file__),
        access_log_sha256=sha(out/'prediction_access_log.json'),input_columns=PRED_COLS,
        runtime={n:importlib.metadata.version(n) for n in ['pandas','numpy','pyarrow']},
        new_fits=0,new_inference=0,database_queries=0,production_changes=0))
    print({'status':'CANDIDATE_SEALED','records':len(p),'formula':CAND},flush=True)


def four_group(direct_hit,gate_hit):
    return np.select([direct_hit&gate_hit,~direct_hit&gate_hit,direct_hit&~gate_hit],
                     ['BOTH_HIT','GATE_GAINED','GATE_LOST'],default='NEITHER_HIT')


def scope_frame(f,scope):
    return f if scope=='FULL_CORE' else f[f.paired] if scope=='PAIRED' else f[f.paired & f.gate_decision.eq(scope=='GATE_TRUE')]


def score(output):
    out=Path(output).resolve();dest=out/'scoring'
    if dest.exists():raise ValueError('SCORING_EXISTS')
    cm=read_json(out/'candidate_manifest.json');verify(cm['sources'])
    if sha(out/'candidate_predictions.parquet')!=cm['candidate_sha256']:raise ValueError('CANDIDATE_CHANGED')
    g=Path(cm['gate_archive']);p=pd.read_parquet(out/'candidate_predictions.parquet')
    score_started=now()
    cols=KEY+['window','model','prediction','prediction_status','actual','truth_status','target_start','target_end',
              'unknown_days','maturity','lifecycle','current_category','current_season','decline_group']
    old=pd.read_parquet(g/'scoring/scores.parquet',columns=cols)
    assert set(old.model)=={BASE,GATE,YEAR,'F28'}
    unique(old,KEY+['model','window'])
    cp=p.melt(id_vars=KEY,value_vars=['prediction_M1','prediction_M2','prediction_60'],var_name='window',value_name='candidate_prediction')
    cp['window']=cp.window.map(dict(prediction_M1='d01_30',prediction_M2='d31_60',prediction_60='d01_60'))
    candidate=old[old.model.eq(BASE)].merge(cp,on=KEY+['window'],how='left',validate='one_to_one')
    candidate['prediction']=candidate.pop('candidate_prediction');candidate['model']=CAND
    candidate['prediction_status']=np.where(candidate.prediction.notna(),'OK',candidate.prediction_status)
    scores=pd.concat([old,candidate],ignore_index=True)
    unique(scores,KEY+['model','window'])
    dims=KEY+['maturity','lifecycle','current_category','current_season','decline_group']
    obj=objects(scores).merge(old[old.model.eq(BASE)&old.window.eq('d01_60')][dims],on=KEY,validate='many_to_one')
    pair=obj[obj.model.eq(BASE)&obj.output_complete][KEY]
    for f in [scores,obj]:
        f['paired']=pd.MultiIndex.from_frame(f[KEY]).isin(pd.MultiIndex.from_frame(pair))
        f['year']=f.issue_date.str[:4]
    flags=p[KEY+['gate_decision']]
    scores=scores.merge(flags,on=KEY,how='left',validate='many_to_one');obj=obj.merge(flags,on=KEY,how='left',validate='many_to_one')
    mature=scores[scores.issue_date.isin(p.issue_date.unique())];mo=obj[obj.issue_date.isin(p.issue_date.unique())]
    met=[];jnt=[]
    for scope in ['FULL_CORE','PAIRED','GATE_TRUE','GATE_FALSE']:
        for label,columns in DIMS.items():
            for vals,q in scope_frame(mature,scope).groupby(['model','window']+columns,dropna=False):
                met.append(dict(scope=scope,slice=label,**dict(zip(['model','window']+columns,vals)),**metrics(q)))
            for vals,q in scope_frame(mo,scope).groupby(['model']+columns,dropna=False):
                jnt.append(dict(scope=scope,slice=label,**dict(zip(['model']+columns,vals)),**joints(q)))
    met,jnt=pd.DataFrame(met),pd.DataFrame(jnt)
    detail=p[KEY+['gate_decision','prediction_origin','prediction_status','input_cutoff']].copy()
    aliases={BASE:'direct',GATE:'gate',CAND:'blend',YEAR:'yearago','F28':'f28'}
    for model,name in aliases.items():
        keep=KEY+['prediction_'+w for w in WINDOWS]+['joint_hit','output_complete']
        if name=='direct':keep+=['actual_'+w for w in WINDOWS]+['paired','maturity','lifecycle','current_category','current_season','decline_group']
        z=mo[mo.model.eq(model)][keep].rename(columns={c:name+'_'+c for c in keep if c.startswith('prediction_') or c in ['joint_hit','output_complete']})
        detail=detail.merge(z,on=KEY,validate='one_to_one')
    detail['original_group']=four_group(detail.direct_joint_hit,detail.gate_joint_hit)
    detail['direct_blend_migration']=migration(detail.direct_joint_hit,detail.blend_joint_hit)
    detail['gate_blend_migration']=migration(detail.gate_joint_hit,detail.blend_joint_hit)
    for name in aliases.values():
        for w in WINDOWS:
            e=detail[name+'_prediction_'+w]-detail['actual_'+w]
            detail[name+'_error_'+w]=e
            detail[name+'_relative_error_'+w]=e/detail['actual_'+w].where(detail['actual_'+w].gt(0))
            detail[name+'_absolute_relative_error_'+w]=detail[name+'_relative_error_'+w].abs()
            detail[name+'_direction_'+w]=np.select([e.isna(),e.gt(0),e.lt(0)],['UNKNOWN','OVER','UNDER'],default='EXACT')
        detail[name+'_e_max']=detail[[name+'_absolute_relative_error_'+w for w in WINDOWS]].max(axis=1,skipna=False)
        detail=detail.copy()
    for endpoint in ['direct','gate']:
        for w in WINDOWS:
            b,e=detail['blend_error_'+w],detail[endpoint+'_error_'+w]
            detail['blend_vs_'+endpoint+'_extra_ae_'+w]=b.abs()-e.abs()
            detail['blend_vs_'+endpoint+'_extra_over_'+w]=b.clip(lower=0)-e.clip(lower=0)
            detail['blend_vs_'+endpoint+'_extra_under_'+w]=(-b).clip(lower=0)-(-e).clip(lower=0)
    paired=detail[detail.paired]
    transitions=[]
    for group,q in paired.groupby('original_group'):
        hit=q.blend_joint_hit
        transitions.append(dict(original_group=group,n=len(q),products=len(q[['shop','spu']].drop_duplicates()),
            actual60=float(q.actual_d01_60.sum()),blend_hit_n=int(hit.sum()),blend_fail_n=int((~hit).sum()),
            blend_hit_actual60=float(q.loc[hit,'actual_d01_60'].sum()),blend_fail_actual60=float(q.loc[~hit,'actual_d01_60'].sum()),
            blend_hit_share=float(hit.mean()),blend_hit_actual_share=float(q.loc[hit,'actual_d01_60'].sum()/q.actual_d01_60.sum()) if q.actual_d01_60.sum() else np.nan,
            extra_ae_vs_direct=float(q.blend_vs_direct_extra_ae_d01_60.sum()),extra_ae_vs_gate=float(q.blend_vs_gate_extra_ae_d01_60.sum())))
    transitions=pd.DataFrame(transitions)
    # Keep exactly the previous list and order; do not select a new favorable top20.
    old_top=pd.read_parquet(g/'scoring/top20_exits_actual.parquet',columns=KEY).assign(original_rank=lambda x:np.arange(1,len(x)+1))
    top=old_top.merge(detail,on=KEY,validate='one_to_one').sort_values('original_rank')
    bounds=[]
    for scope in ['PAIRED','DECLINING']:
        q=paired if scope=='PAIRED' else paired[paired.decline_group.eq('PRE_ISSUE_DECLINING')]
        for w in WINDOWS:
            den=q['actual_'+w].sum()
            for metric,fn in [('ae',lambda e:e.abs()),('over',lambda e:e.clip(lower=0)),('under',lambda e:(-e).clip(lower=0))]:
                a=fn(q['direct_error_'+w]).sum();b=fn(q['gate_error_'+w]).sum();c=fn(q['blend_error_'+w]).sum()
                bounds.append(dict(scope=scope,window=w,metric=metric,n=len(q),actual=den,direct=a,gate=b,upper_bound=(a+b)/2,blend=c,
                                   upper_rate=(a+b)/2/den if den else np.nan,blend_rate=c/den if den else np.nan))
    pmet=met[met.scope.eq('PAIRED')&met.slice.eq('overall')];p60=pmet[pmet.window.eq('d01_60')].set_index('model')
    pj=jnt[jnt.scope.eq('PAIRED')&jnt.slice.eq('overall')].set_index('model')
    decline=met[met.scope.eq('PAIRED')&met.slice.eq('decline_group')&met.window.eq('d01_60')&met.decline_group.eq('PRE_ISSUE_DECLINING')].set_index('model')
    gates=dict(paired_w60_wape_lower=bool(p60.loc[CAND,'wape']<p60.loc[BASE,'wape']),
               joint_hit_count_increased=bool(pj.loc[CAND,'joint_hit_n']>pj.loc[BASE,'joint_hit_n']),
               joint_hit_sales_share_not_lower=bool(pj.loc[CAND,'joint_hit_actual_share']>=pj.loc[BASE,'joint_hit_actual_share']),
               declining_w60_over_rate_not_increased=bool(decline.loc[CAND,'over_rate']<=decline.loc[BASE,'over_rate']) if len(decline) and decline.actual_units.gt(0).all() else None,
               output_and_core_coverage_not_reduced=bool(detail.direct_output_complete.eq(detail.blend_output_complete).all()))
    summary=dict(candidate=CAND,weight=.5,started_at=score_started,candidate_sealed_at=cm['sealed_at'],gates=gates,
        numerical_screen='PASS' if all(x is True for x in gates.values()) else 'FAIL' if any(x is False for x in gates.values()) else 'PARTIAL',
        evidence='PARTIAL: reconstructed historical development, inspired by already-observed GATE results; not OOS',
        full_core_n=len(detail),paired_n=len(paired),missing_n=int((~detail.direct_output_complete).sum()),
        immature_n=len(obj[obj.model.eq(BASE)&~obj.issue_date.isin(p.issue_date.unique())]),
        paired_windows=pmet.to_dict('records'),paired_joint=pj.reset_index().to_dict('records'),decline60=decline.reset_index().to_dict('records'),
        four_groups=transitions.to_dict('records'),new_fits=0,new_inference=0,database_queries=0,production_changes=0)
    dest.mkdir(exist_ok=False)
    for name,frame in [('scores',scores),('objects',obj),('metrics',met),('joint_summary',jnt),('details',detail),
                       ('four_groups',transitions),('fixed_top20',top),('convexity_bounds',pd.DataFrame(bounds)),
                       ('not_matured',scores[~scores.issue_date.isin(p.issue_date.unique())])]:save(dest,name,frame)
    dump(dest/'summary.json',summary)
    verify(cm['sources'])
    dump(dest/'manifest.json',dict(at=now(),candidate_sha256=cm['candidate_sha256'],script_sha256=sha(__file__),
                                   files={f.name:sha(f) for f in dest.iterdir() if f.is_file()}))
    print({'status':'SCORED','gates':gates,'four_groups':transitions[['original_group','n','blend_hit_n']].to_dict('records')},flush=True)


if __name__=='__main__':
    a=argparse.ArgumentParser();sub=a.add_subparsers(dest='action',required=True)
    p=sub.add_parser('prepare');p.add_argument('--gate-archive',required=True);p.add_argument('--output',required=True)
    p=sub.add_parser('score');p.add_argument('--output',required=True)
    args=vars(a.parse_args());action=args.pop('action');globals()[action](**args)
