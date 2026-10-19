"""Limited retrospective sensitivity; local archived inputs only, no DB or shadow entry.

Requires a committed preregistration and an explicit business-gate receipt. The
frozen candidate is imported unchanged; calibration is reused, never reselected.
"""
import argparse
import collections
import calendar
import csv
import datetime as dt
import hashlib
import json
import random
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT)not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.a16_candidate_v3 import Candidate,CutoffHistory,SHOPS,shift
from scripts.run_a16_shadow_once import verify_freeze
from scripts.audit_a7_down_batch import diagnostic_flags
from scripts.diagnose_a16_frozen_trace import A3,A7,A16

MODELS=('v4','a16','last_month','a3','w_half','w_zero')
MONTHS=('2025-10','2025-11','2025-12','2026-01','2026-02','2026-03','2026-07','2026-08','2026-09')
PREREG='cb4a0b723c7b2f04f50469c85a3394649464b5c2'


def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def key(r):return (r['target_month'],r['horizon'],r['shop'],r['spu'])
def fraction(a,b):return a/b if b else None


def trust_state(proof=None):
    """Descriptive flags deliberately are not accepted as comparability proof."""
    needed=('scope_verified','effective_dates_verified','operating_evidence_verified')
    if proof and all(proof.get(k)is True for k in needed):
        if proof.get('conclusion')in('COMPARABLE','NON_COMPARABLE'):
            return proof['conclusion'],proof.get('reference','EXPLICIT_EVIDENCE')
    return 'UNKNOWN','Historical supply/listing/effective metadata comparability unverified'


def protection(lifecycle,recent):
    if len(recent)!=3:raise ValueError('Protection requires exactly three months')
    p1=lifecycle=='衰退'
    p2=all(x is not None and x>0 for x in recent)and recent[0]>recent[1]>recent[2]
    return p1,p2,p1 or p2


def sensitive_value(a3,candidate,a7,a16,rule,gamma,weight,missing):
    if weight not in (1,.5,0):raise ValueError('Weight outside preregistered grid')
    if weight==1 or rule!='DOWN' or gamma==0:return a16,'UNCHANGED'
    if a16!=a7:raise ValueError('Frozen DOWN no longer KEEP_A7')
    if missing:return None,'MISSING_REQUIRED_INPUT'
    if candidate is None:raise ValueError('DOWN requires seasonal candidate')
    value=max(0,int(round((1-gamma*weight)*a3+gamma*weight*candidate)))
    if value<a16:raise ValueError('Reducing DOWN trust must not lower prediction')
    return value,'SENSITIVITY_ONLY'


def score(rows,model):
    known=[r for r in rows if r.get('truth')is not None and r.get(model)is not None]
    actual=sum(r['truth']for r in known);pred=sum(r[model]for r in known)
    over=sum(max(r[model]-r['truth'],0)for r in known);under=sum(max(r['truth']-r[model],0)for r in known)
    pos=[r for r in known if r['truth']>0];zero=[r for r in known if r['truth']==0]
    zero_error=sum(abs(r[model])for r in zero)
    return {'scope_n':len(rows),'n':len(known),'prediction_n':sum(r.get(model)is not None for r in rows),
      'actual_sum':actual,'prediction_sum':pred,'absolute_error':over+under,'over_units':over,'under_units':under,
      'wape':fraction(over+under,actual),'bias':fraction(over-under,actual),
      'over_rate':fraction(over,actual),'under_rate':fraction(under,actual),
      'positive_n':len(pos),'hit30':fraction(sum(abs(r[model]-r['truth'])<=.3*r['truth']for r in pos),len(pos)),
      'zero_n':len(zero),'zero_absolute_error':zero_error,'zero_mae':fraction(zero_error,len(zero))}


def load_history(paths):
    sku=collections.defaultdict(lambda:collections.defaultdict(int));obs=collections.defaultdict(int)
    mapping=collections.defaultdict(set);seasons={};categories=[]
    for line in paths['monthly_and_seasons'].open(encoding='utf-8-sig'):
        if not line.startswith('{'):continue
        x=json.loads(line)
        if x['kind']=='seasons':seasons={r['SPU']:r['季节']for r in x['value']}
        if x['kind']!='sales':continue
        r=x['value'];shop,sk,sp=(str(r.get(k)or'').strip()for k in ('店铺','SKU','SPU'))
        month=shift(r['统计日期'],0);q=r['销量']
        if shop not in SHOPS or not sk or not sp or q is None or int(q)<0:raise ValueError('Bad archived identity/quantity')
        if not shift('2024-01',0)<=month<shift('2026-10',0):raise ValueError('History window changed')
        q=int(q);sku[shop,sk,sp][month]+=q;obs[shop+'|'+sp,month]+=q;mapping[shop,sk].add(sp)
    if any(len(v)>1 for v in mapping.values()):raise ValueError('Conflicting SKU mapping')
    for line in paths['categories_and_daily'].open(encoding='utf-8-sig'):
        if line.startswith('{'):
            x=json.loads(line)
            if x['kind']=='category_rows':categories=x['value']
    categories.sort(key=lambda r:(bool(str(r.get('product_category')or'').strip()),str(r.get('update_time')or''),str(r.get('etl_load_time')or''),int(r.get('product_id')or 0)),reverse=True)
    cats={}
    for r in categories:cats.setdefault(str(r.get('spu')or'').strip(),str(r.get('product_category')or'').strip()or'未映射')
    return sku,dict(obs),cats,seasons


def confirmed_truth(paths):
    zero={};conflicts={}
    old=read(paths['old_truth_classification'])['records'];fresh=read(paths['fresh_truth_classification'])
    for rows,field,reason in [(old,'actual_reconciled','reason'),(fresh,'actual_v3','reason_v3')]:
        for r in rows:
            if r.get(field)==0:
                if r[reason]!='DAILY_COMPLETE_ZERO':raise ValueError('Unregistered zero classification')
                validate_daily_zero(r,r.get('daily_v3',r.get('daily')))
                zero[r['shop'],r['spu'],r['target_month']]=r[reason]
    with paths['truth_conflicts'].open(encoding='utf-8-sig',newline='')as f:
        for r in csv.DictReader(f):
            if int(r['difference']):conflicts[r['shop'],r['spu'],r['target_month']]=dict(r)
    return zero,conflicts


def validate_daily_zero(row,daily):
    if not daily:raise ValueError('Daily zero lacks archived evidence')
    start=shift(row['target_month'],0);days=calendar.monthrange(start.year,start.month)[1]
    end=start.replace(day=days)
    expected=(row['shop'],row['spu'],row['target_month'])
    if (daily['store_name'],daily['spu'],daily['target_month'])!=expected:raise ValueError('Daily identity mismatch')
    if int(daily['days'])!=days or daily['first_dt']!=str(start) or daily['last_dt']!=str(end):raise ValueError('Incomplete daily window')
    if int(daily['asin_country_n'])<=0 or int(daily['n'])!=days*int(daily['asin_country_n']):raise ValueError('Incomplete daily grid')
    if any(int(daily[k])!=0 for k in ('volume','null_volume','negative_volume','deleted_rows','date_mismatch')):raise ValueError('Unconfirmed daily zero')


def value_state(value):
    return 'MISSING_RECORD'if value is None else'EXPLICIT_ZERO'if value==0 else'OBSERVED_VALUE'


def check_approval(approval,started):
    if approval.get('gate_confirmed')is not True or approval.get('prereg_commit')!=PREREG:raise ValueError('Business gate or preregistration not confirmed')
    if not str(approval.get('user_confirmation','')).strip():raise ValueError('Actual user confirmation required')
    confirmed=dt.datetime.fromisoformat(approval['confirmed_at'])
    if confirmed.tzinfo is None or confirmed>dt.datetime.fromisoformat(started):raise ValueError('Invalid confirmation time')


def fixed_predict(candidate,target,h,meta):
    cutoff=shift(target,-h)
    if meta['cutoff']!=str(cutoff)[:7] or not all(m<str(cutoff)[:7]for m in meta['training_months']):raise ValueError('Calibration cutoff mismatch')
    train=[];month=shift('2025-01',0)
    while month<cutoff:
        features,_=candidate.features(month,h)
        train.extend(r for r in features if r['实际销量']is not None)
        month=shift(month,1)
    if len(train)!=meta['training_rows']:raise ValueError('Calibration training context changed')
    if sorted({r['目标月']for r in train})!=meta['training_months']:raise ValueError('Training months changed')
    features,_=candidate.features(target,h)
    clean=[{k:v for k,v in r.items()if k!='实际销量'}for r in features]
    rows=candidate.r5.add_a7(clean,meta['gamma_down'],meta['gamma_up'])
    history=CutoffHistory(candidate.observations,cutoff);first=candidate.r11.first_sale_map(history)
    out=[]
    for shop in SHOPS:
        eligible=[r for r in rows if r['shop']==shop and r['可预测性']=='ESTABLISHED']
        support=candidate.r12.category_support(history,first,eligible)
        out.extend(candidate.r12.apply_trust_gates(history,first,eligible,support))
    if len(out)!=meta['output_rows']:raise ValueError('Eligible cohort changed')
    return {r['SPU']:r for r in out},{r['SPU']:r for r in features}


def grouped_metrics(details):
    result=[]
    dimensions=[(),('horizon',),('target_month','horizon'),('shop','horizon'),('lifecycle','horizon'),
                ('trust_state','horizon'),('period','horizon'),('age_class','horizon')]
    common=[r for r in details if r['common_score']]
    for fields in dimensions:
        groups=collections.defaultdict(list)
        for r in common:groups[tuple(r[k]for k in fields)].append(r)
        for values,rs in groups.items():
            for model in MODELS:result.append({'population':'common','dimensions':list(fields),**dict(zip(fields,values)),'model':model,**score(rs,model)})
    for label,predicate in [('protection_union',lambda r:r['protected']),('protection_p1',lambda r:r['protection_p1']),
        ('protection_p2',lambda r:r['protection_p2']),('protected_down',lambda r:r['protected']and r['a7_rule']=='DOWN'),
        ('down',lambda r:r['a7_rule']=='DOWN'),('exclude_conflicts',lambda r:not r['truth_conflict'])]:
        for horizon in ('ALL','H2','H3'):
            rs=[r for r in common if predicate(r)and(horizon=='ALL'or r['horizon']==horizon)]
            for model in MODELS:result.append({'population':label,'dimensions':['horizon'],'horizon':horizon,'model':model,**score(rs,model)})
    return result


def changes(rows):
    groups=collections.defaultdict(list)
    for r in rows:
        if r['common_score']:groups[r['shop'],r['spu']].append(r)
    out=[]
    for (shop,spu),rs in groups.items():
        b=score(rs,'a16')
        for model in ('w_half','w_zero'):
            x=score(rs,model);delta=x['absolute_error']-b['absolute_error']
            out.append({'shop':shop,'spu':spu,'model':model,'n':len(rs),'actual_sum':x['actual_sum'],
              'delta_absolute_error':delta,'delta_over_units':x['over_units']-b['over_units'],
              'delta_under_units':x['under_units']-b['under_units'],'outcome':'IMPROVED'if delta<0 else'DEGRADED'if delta>0 else'TIE'})
    return out


def bootstrap(rows,model,protected=False):
    groups=collections.defaultdict(list)
    for r in rows:
        if r['common_score']and(not protected or r['protected']):groups[r['shop'],r['spu']].append(r)
    clusters=[]
    for rs in groups.values():
        b=score(rs,'a16');x=score(rs,model)
        clusters.append((x['actual_sum'],x['absolute_error']-b['absolute_error'],x['over_units']-b['over_units']))
    rng=random.Random(20261019);values=[]
    for _ in range(200):
        drawn=[clusters[rng.randrange(len(clusters))]for _ in clusters]if clusters else[]
        denominator=sum(x[0]for x in drawn)
        if denominator:values.append(sum(x[2 if protected else 1]for x in drawn)/denominator)
    values.sort()
    return {'clusters':len(clusters),'draws':200,'valid_draws':len(values),'interval_type':'cluster_percentile_descriptive_95',
            'lower':values[int(.025*(len(values)-1))]if values else None,'upper':values[int(.975*(len(values)-1))]if values else None}


def csvout(path,rows):
    if not rows:return
    cols=list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w',encoding='utf-8-sig',newline='')as f:
        w=csv.DictWriter(f,fieldnames=cols);w.writeheader()
        w.writerows({k:json.dumps(v,ensure_ascii=False)if isinstance(v,(list,dict))else v for k,v in r.items()}for r in rows)


def run(inventory_path,approval_path,output,validation_only=False):
    out=Path(output).resolve()
    if out.exists():raise ValueError('Output must be new, not an old archive')
    started=dt.datetime.now(dt.timezone.utc).isoformat()
    approval=None if validation_only else read(approval_path)
    if not validation_only:check_approval(approval,started)
    committed=subprocess.check_output(['git','show',PREREG+':docs/a7_down_sensitivity_v1_inputs.json'],cwd=ROOT)
    manifest=json.loads(committed)
    if read(ROOT/'docs/a7_down_sensitivity_v1_inputs.json')!=manifest:raise ValueError('Preregistration changed')
    protocol='docs/a7_down_sensitivity_v1_preregistration.md'
    registered=subprocess.check_output(['git','show',PREREG+':'+protocol],cwd=ROOT).decode('utf-8')
    if (ROOT/protocol).read_text(encoding='utf-8').replace('\r\n','\n')!=registered.replace('\r\n','\n'):raise ValueError('Registered protocol changed')
    verify_freeze();inv=read(inventory_path);paths={k:Path(v['path'])for k,v in inv['versions'].items()}
    for k,p in paths.items():
        if sha(p)!=manifest['file_sha256'][k]:raise ValueError('Input fingerprint mismatch: '+k)
    sku,obs,cats,seasons=load_history(paths);zero,conflicts=confirmed_truth(paths)
    item_skus=collections.defaultdict(dict)
    for (shop,sk,sp),history in sku.items():item_skus[shop,sp][sk,sp]=history
    candidate=Candidate(obs,cats,ROOT)
    def forbidden(*args,**kwargs):raise RuntimeError('Parameter reselection/full shadow prohibited')
    candidate.predict=forbidden;candidate.r5.choose_params=forbidden
    original=read(paths['historical_outputs']);archive=[r for r in original if r['horizon']in('H2','H3')and r['target_month']in MONTHS]
    if len({key(r)for r in archive})!=len(archive):raise ValueError('Duplicate historical key')
    choices=read(paths['historical_calibration']);cal={(c['target'],c['horizon']):c for c in choices}
    details=[];parity=0;contexts=[]
    for target in MONTHS:
        for h in (2,3):
            meta=cal[target,h];pred,features=fixed_predict(candidate,target,h,meta)
            cutoff=shift(target,-h);rs=[r for r in archive if r['target_month']==target and r['horizon']==f'H{h}']
            contexts.append({**meta,'feature_n':len(features),'historical_outer_n':len(rs),'retuned':False})
            for r in rs:
                item=r['shop']+'|'+r['spu'];p=pred.get(item);f=features.get(item)
                if not f or f['可预测性']!=r['forecastability']:raise ValueError('Qualification parity failed')
                for field,column in [('a3',A3),('a7',A7),('a16',A16)]:
                    if (p[column]if p else None)!=r[field]:raise ValueError('Frozen stage mismatch '+field)
                if obs.get((item,shift(target,0)))!=r['actual']:raise ValueError('Archived truth changed')
                if obs.get((item,shift(cutoff,-1)))!=r['last_month']:raise ValueError('Baseline changed')
                parity+=1
                flags=diagnostic_flags(obs,item,cutoff,shift(target,0),item_skus[r['shop'],r['spu']])
                flags.update({'recent_states':[value_state(v)for v in flags['recent_sales']],
                 'ly_recent_states':[value_state(v)for v in flags['ly_recent_sales']],
                 'ly_target_state':value_state(flags['ly_target_sales'])})
                p1,p2,protected=protection(r['lifecycle'],flags['recent_sales'])
                cell=(r['shop'],r['spu'],target);truth=r['actual'];truth_status='MONTHLY_OBSERVED'if truth is not None else'UNKNOWN'
                if truth is None and cell in zero:truth=0;truth_status='DAILY_CONFIRMED_ZERO'
                trust,reason=trust_state()
                x={k:r[k]for k in ('target_month','cutoff_month','horizon','shop','spu','age','age_class','forecastability','lifecycle','v4','a16','last_month','a3','a7','a7_rule','a16_rule','current_category','current_season')}
                x.update({'period':'winter_replay'if target<'2026-07'else'development_reference','truth':truth,'monthly_actual':r['actual'],
                 'truth_status':truth_status,'truth_conflict':cell in conflicts,'daily_conflict_value':int(conflicts[cell]['daily'])if cell in conflicts else None,
                 'gamma_down':meta['gamma_down'],'gamma_up':meta['gamma_up'],'seasonal_candidate':p.get('季节候选')if p else None,
                 'trust_state':trust,'trust_basis':reason,'metadata_effectivity':'CURRENT_NOT_POINT_IN_TIME','source_effective_dates':'UNVERIFIED',
                 'protection_p1':p1,'protection_p2':p2,'protected':protected,**flags})
                for w,col in ([]if validation_only else[(.5,'w_half'),(0,'w_zero')]):
                    if p:x[col],x[col+'_status']=sensitive_value(p[A3],p.get('季节候选'),p[A7],p[A16],p['A7规则'],meta['gamma_down'],w,flags['key_input_missing_count'])
                    else:x[col]=None;x[col+'_status']='INELIGIBLE'
                x['common_score']=False if validation_only else r['forecastability']=='ESTABLISHED'and truth is not None and all(x[m]is not None for m in MODELS)
                details.append(x)
            print(json.dumps({'target':target,'horizon':h,'eligible':len(pred),'outer':len(rs),'phase':'fixed_full_context_complete'}),flush=True)
    # Tag the original batch without rerunning the completed four-object trace.
    traces=read(paths['original_trace'])
    with paths['original_predictions'].open(encoding='utf-8-sig',newline='')as f:baseline={(r['shop'],r['spu'],r['horizon']):r for r in csv.DictReader(f)if r['qualification']=='ESTABLISHED'}
    tagged=[]
    for r in traces:
        if int(baseline[r['shop'],r['spu'],r['horizon']]['a16_shadow'])!=r['a16']:raise ValueError('Original tag parity failed')
        state,reason=trust_state();x={**r,'trust_state':state,'trust_basis':reason,'truth':None,'evaluation':'FUTURE_TARGET_NO_ACCURACY'}
        for w,col in ([]if validation_only else[(.5,'w_half'),(0,'w_zero')]):x[col],x[col+'_status']=sensitive_value(r['a3'],r['seasonal_candidate'],r['a7'],r['a16'],r['a7_rule'],r['gamma_down'],w,r['key_input_missing_count'])
        tagged.append(x)
    if len(tagged)!=len(baseline)or len(tagged)!=1000:raise ValueError('Original batch coverage changed')
    for k,p in paths.items():
        if sha(p)!=manifest['file_sha256'][k]:raise ValueError('Archived input changed during run: '+k)
    if validation_only:
        summary={'kind':'BASELINE_VALIDATION_ONLY_NO_SENSITIVITY_COMPUTED','started_at':started,
          'completed_at':dt.datetime.now(dt.timezone.utc).isoformat(),'prereg_commit':PREREG,
          'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
          'historical_parity_rows':parity,'historical_eligible':sum(r['forecastability']=='ESTABLISHED'for r in details),
          'original_tag_parity_rows':len(tagged),'original_tag_states':dict(collections.Counter(r['trust_state']for r in tagged)),
          'source_inventory_sha256':sha(inventory_path),'source_hashes_unchanged':True,'sensitivity_executed':False}
        out.mkdir(parents=True)
        for name,value in [('validation_summary',summary),('baseline_details',details),('contexts',contexts),('original_batch_tagged',tagged)]:
            (out/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
            if isinstance(value,list):csvout(out/(name+'.csv'),value)
        return summary
    metrics=grouped_metrics(details);changed=changes(details);coverage=[]
    for period in ('ALL','winter_replay','development_reference'):
        for h in ('ALL','H2','H3'):
            rs=[r for r in details if(period=='ALL'or r['period']==period)and(h=='ALL'or r['horizon']==h)]
            eligible=[r for r in rs if r['forecastability']=='ESTABLISHED'];common=[r for r in rs if r['common_score']]
            waterfall=collections.Counter()
            for r in rs:
                reason=('ineligible'if r['forecastability']!='ESTABLISHED'else'truth_unknown'if r['truth']is None else
                    'candidate_missing_input'if any(r[m]is None for m in ('w_half','w_zero'))else
                    'baseline_missing'if any(r[m]is None for m in ('v4','a16','last_month','a3'))else'common_scored')
                waterfall[reason]+=1
            coverage.append({'period':period,'horizon':h,'outer_n':len(rs),'eligible_n':len(eligible),'common_n':len(common),
             'truth_states':dict(collections.Counter(r['truth_status']for r in eligible)),
             'candidate_missing_n':sum(r['w_half_status']=='MISSING_REQUIRED_INPUT'for r in eligible),
             'truth_conflict_n':sum(r['truth_conflict']for r in eligible),'waterfall':dict(waterfall),
             'model_coverage':{m:score(eligible,m)for m in MODELS}})
    common=[r for r in details if r['common_score']];protect=[r for r in common if r['protected']]
    protection_breadth={'items':len({(r['shop'],r['spu'])for r in protect}),
      'affected_months':sorted({r['target_month']for r in protect if r['w_zero']!=r['a16']}),
      'confirmed_operating_decline':0,'definition':'Pre-cutoff risk proxies, not confirmed causal decline'}
    summary={'kind':'RECONSTRUCTED_RETROSPECTIVE_SENSITIVITY','started_at':started,'completed_at':dt.datetime.now(dt.timezone.utc).isoformat(),
      'prereg_commit':PREREG,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
      'source_inventory_sha256':sha(inventory_path),'business_gate_receipt':approval,
      'historical_parity_rows':parity,'original_tag_parity_rows':len(tagged),'coverage':coverage,'protection_breadth':protection_breadth,
      'states':dict(collections.Counter(r['trust_state']for r in details)),
      'bootstrap':{m:{'overall_wape_delta':bootstrap(details,m),'protected_over_rate_delta':bootstrap(details,m,True)}for m in('w_half','w_zero')}}
    out.mkdir(parents=True)
    for name,value in [('details',details),('metrics',metrics),('changes',changed),('summary',summary),('contexts',contexts),('original_batch_tagged',tagged)]:
        (out/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
        if isinstance(value,list):csvout(out/(name+'.csv'),value)
    return summary


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--inventory',required=True);p.add_argument('--approval');p.add_argument('--output',required=True);p.add_argument('--validation-only',action='store_true')
    a=p.parse_args()
    if not a.validation_only and not a.approval:p.error('Sensitivity requires --approval; no approval is needed for --validation-only')
    r=run(a.inventory,a.approval,a.output,a.validation_only)
    print(json.dumps({k:r[k]for k in ('kind','historical_parity_rows','original_tag_parity_rows')},ensure_ascii=False))
