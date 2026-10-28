"""Offline archive diagnostic. Deliberately imports no model or source client."""
from __future__ import annotations
import argparse, datetime as dt, hashlib, json, re
from pathlib import Path
import numpy as np
import pandas as pd

VALID={'OBSERVED','EXPLICIT_ZERO'}
KEY=['issue_date','shop','spu']
WINDOWS=['d01_30','d31_60','d01_60']

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def dump(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')
def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def csv(out,name,d):d.to_csv(out/(name+'.csv'),index=False,encoding='utf-8-sig')

class Sources:
    def __init__(self):self.files={}
    def add(self,p,expected=None):
        p=Path(p).resolve();h=sha(p)
        if expected is not None and h!=expected:raise ValueError('HASH_CHANGED:'+str(p))
        self.files[str(p)]=h;return p
    def verify(self):
        for p,h in self.files.items():
            if sha(p)!=h:raise ValueError('SOURCE_MUTATED:'+p)

def core_mask(qty,total):
    """Exact original boundary-tie rule on observed mapped quantity."""
    qty=np.asarray(qty,float)
    if total<=0:return np.zeros(len(qty),bool)
    order=np.argsort(-qty,kind='stable');cs=np.cumsum(qty[order])
    k=np.flatnonzero(cs>=.9*total)
    boundary=qty[order[k[0]]] if len(k) else -np.inf
    return qty>=boundary

def disjoint(starts):
    count=0;last=None
    for s in sorted(pd.to_datetime(starts).unique()):
        s=pd.Timestamp(s)
        if last is None or s>last:count+=1;last=s+pd.Timedelta(days=29)
    return count

def deviance(y,p):
    y,p=np.asarray(y,float),np.asarray(p,float)
    out=np.full(len(y),np.nan)
    valid=np.isfinite(y)&np.isfinite(p)&(y>=0)&(p>=0)
    z=valid&(y==0);out[z]=2*p[z]
    positive=valid&(y>0)&(p>0)
    out[positive]=2*(y[positive]*np.log(y[positive]/p[positive])-y[positive]+p[positive])
    out[valid&(y>0)&(p==0)]=np.inf
    return out

def canonical_scope(scores,objects):
    """Scope belongs to frozen objects, never nullable per-model report flags."""
    scope=objects[KEY+['paired','decline_group']]
    if scope.duplicated(KEY).any():raise ValueError('DUPLICATE_SCOPE')
    result=scores.drop(columns=['paired','decline_group'],errors='ignore').merge(scope,on=KEY,validate='many_to_one',how='left')
    if len(result)!=len(scores) or result.paired.isna().any():raise ValueError('SCOPE_JOIN')
    for model,g in result.groupby('model'):
        if len(g)!=3*len(scope) or len(g[g.paired])!=3*int(scope.paired.sum()):raise ValueError('MODEL_SCOPE:'+model)
    return result

def age_band(age):
    return np.select([age<=0,age<=3,age<=5,age<=11,age>=12],['UNKNOWN','0_3','4_5','6_11','12_plus'],default='UNKNOWN')

def historical_core(daily):
    """Calendar-aligned arrays, no missing date converted to a known zero."""
    dates=pd.date_range(daily.ds.min(),daily.ds.max());result={};quality=[]
    for shop,g in daily.groupby('shop'):
        raw=g.pivot(index='ds',columns='spu',values='y').reindex(dates)
        valid=g.assign(ok=g.sales_state.isin(VALID)&np.isfinite(g.y)&g.y.ge(0))
        known=valid.pivot(index='ds',columns='spu',values='ok').reindex(index=dates,columns=raw.columns).eq(True)
        mapped=g.pivot(index='ds',columns='spu',values='mapped').reindex(index=dates,columns=raw.columns).eq(True)
        totals=g.groupby('ds').y.sum(min_count=1).reindex(dates).rolling(90,min_periods=90).sum()
        invalid=(~valid.ok).groupby(valid.ds).sum().reindex(dates).rolling(90,min_periods=90).sum()
        sums=raw.where(known).rolling(90,min_periods=1).sum()
        n=known.astype(int).rolling(90,min_periods=90).sum()
        map_n=mapped.astype(int).rolling(90,min_periods=90).sum()
        map_any=mapped.astype(int).rolling(90,min_periods=1).sum()>0
        state=np.full(raw.shape,2,np.uint8)  # core=1, noncore=0, UNKNOWN=2
        original=np.full(raw.shape,2,np.uint8)
        for i,c in enumerate(dates):
            if pd.isna(totals.iloc[i]) or invalid.iloc[i]!=0 or totals.iloc[i]<=0:continue
            available=map_any.iloc[i].to_numpy()&sums.iloc[i].notna().to_numpy()
            core=core_mask(sums.iloc[i].to_numpy()[available],totals.iloc[i])
            original[i,available]=core.astype(np.uint8)
            eligible=available&(n.iloc[i].to_numpy()==90)&(map_n.iloc[i].to_numpy()==90)
            state[i,eligible]=original[i,eligible]
            quality.append(dict(shop=shop,input_cutoff=str(c.date()),shop_units=float(totals.iloc[i]),
                original_core_n=int(core.sum()),known_core_n=int((state[i]==1).sum()),
                unknown_objects=int((state[i]==2).sum()),mapped_units=float(sums.iloc[i].to_numpy()[available].sum())))
        for j,spu in enumerate(raw.columns):result[(shop,spu)]=(state[:,j],original[:,j])
    return dates,result,pd.DataFrame(quality)

def original_audit(t,audit):
    z=t.sort_values(audit['sort_keys']).reset_index(drop=True)
    if len(z)!=audit['rows']:raise ValueError('TRAIN_ROWS_CHANGED')
    for c in audit['columns']:
        h=hashlib.sha256(pd.util.hash_pandas_object(z[c],index=False).to_numpy(dtype=np.uint64).tobytes()).hexdigest()
        if h!=audit['column_sha256'][c] or str(z[c].dtype)!=audit['dtypes'][c]:raise ValueError('ORIGINAL_TRAIN_COLUMN_CHANGED:'+c)

def aggregate_train(t,group,unit):
    rows=[];total=t.y_label.sum()
    for value,g in t.groupby(group,dropna=False,observed=True):
        counts=g.groupby(['shop','spu']).size()
        rows.append(dict(**unit,dimension=group,value=str(value),rows=len(g),row_share=len(g)/len(t),
          effective_weight=float(len(g)),weight_share=len(g)/len(t),label_units=float(g.y_label.sum()),
          label_share=float(g.y_label.sum()/total),zero_rows=int(g.y_label.eq(0).sum()),zero_share=float(g.y_label.eq(0).mean()),
          objects=len(counts),median_rows_per_object=float(counts.median()),p90_rows_per_object=float(counts.quantile(.9)),
          max_rows_per_object=int(counts.max())))
    return rows

def trace_reason(dates,known,months,actual_cutoff,segment):
    """Frozen exclusion order, diagnostic only; never builds features or models."""
    sim=pd.date_range(dates[0]+pd.Timedelta(days=29),actual_cutoff)
    cuts=sim-pd.Timedelta(days=2);last=(cuts+pd.Timedelta(days=1)).to_period('M').asi8-1
    counts=np.searchsorted(months,last,side='right');qual=counts>=4
    end=sim+pd.Timedelta(days=30 if segment=='M1' else 60)
    start=sim+pd.Timedelta(days=1 if segment=='M1' else 31)
    reason=np.full(len(sim),'INSUFFICIENT_QUALIFICATION',object)
    reason[qual]='LABEL_NOT_MATURE';mature=qual&(end<=actual_cutoff)
    miss=np.r_[0,np.cumsum(~known)];pos=(cuts-dates[0]).days.to_numpy()
    hist=miss[pos+1]==miss[pos-27]
    reason[mature]='MISSING_HISTORY';ok=mature&hist
    lo=(start-dates[0]).days.to_numpy();hi=(end-dates[0]).days.to_numpy()+1
    lab=miss[np.minimum(hi,len(known))]==miss[np.minimum(lo,len(known))]
    reason[ok]='MISSING_LABEL_DAYS';reason[ok&lab]='INCLUDED'
    return sim,reason,qual,hist

def score_models(scores,objects):
    metrics=[];joints=[]
    for scope,mask in [('FULL',np.ones(len(scores),bool)),('PAIRED',scores.paired.eq(True)),
                       ('DECLINE_PAIRED',scores.paired.eq(True)&scores.decline_group.eq('PRE_ISSUE_DECLINING'))]:
        for (model,window),g in scores[mask].groupby(['model','window']):
            ok=g.actual.notna()&g.prediction.notna();z=g[ok];y=z.actual.to_numpy();p=z.prediction.to_numpy();e=p-y;v=deviance(y,p)
            if np.isnan(v).any():raise ValueError('INVALID_SCORING_VALUE')
            metrics.append(dict(scope=scope,model=model,window=window,fixed_n=len(g),output_n=int(g.prediction.notna().sum()),
                truth_n=int(g.actual.notna().sum()),scored_n=len(z),zero_actual_n=int((y==0).sum()),zero_prediction_n=int((p==0).sum()),
                actual_units=float(y.sum()),absolute_error=float(abs(e).sum()),wape=float(abs(e).sum()/y.sum()),
                bias=float(e.sum()/y.sum()),over_units=float(e.clip(0).sum()),under_units=float((-e).clip(0).sum()),
                hit_n=int(((y>0)&(abs(e)<=.2*y)).sum()),
                poisson_deviance_mean=float(v.mean()) if np.isfinite(v).all() else 'INFINITY',
                poisson_infinite_n=int(np.isinf(v).sum()),strict_poisson_domain_n=int((p>0).sum())))
        for model,g in objects[objects.scope.eq(scope)].groupby('model'):
            joints.append(dict(scope=scope,model=model,fixed_n=len(g),joint_n=int(g.joint_hit.sum()),
                joint_fixed_share=float(g.joint_hit.mean()),joint_actual_units=float(g.loc[g.joint_hit,'actual'].sum()),
                actual_units=float(g.actual.sum()),joint_sales_share=float(g.loc[g.joint_hit,'actual'].sum()/g.actual.sum())))
    return pd.DataFrame(metrics),pd.DataFrame(joints)

def run(root,out):
    root,out=Path(root).resolve(),Path(out).resolve()
    if out.exists() or root not in out.parents:raise ValueError('NEW_OUTPUT_REQUIRED')
    source=Sources();archive=root/'bc60_monthly_replay_20261020';item=root/'direct_item_id_v1_20261028';comp=root/'core_model_complementarity_20261028_v1'
    manifest=read(source.add(comp/'manifest.json'));shadow=manifest['protected_shadow_files']
    for p,h in shadow.items():source.add(p,h)
    item_manifest=read(source.add(item/'delivery_manifest.json'));files=item_manifest['files']
    original_manifest=read(source.add(archive/'effects/run_manifest.json'))
    original_manifest['files']={k.replace('\\','/'):v for k,v in original_manifest['files'].items()}
    def itemfile(rel):
        expected=files.get(rel,files.get(str(item/rel)))
        if expected is None:raise ValueError('ITEM_MANIFEST_KEY:'+rel)
        return source.add(item/rel,expected)
    daily=pd.read_parquet(source.add(archive/'inputs/daily.parquet','03596126d5543adc24325de12f03d4899c372e7076b01339e19656ce289bfc82'),columns=['ds','shop','spu','y','mapped','sales_state'])
    # Explicit frozen input hash; value also checked against archived provenance.
    monthly=pd.read_parquet(source.add(archive/'inputs/monthly_reference.parquet','423ce1086fc885d7d41e087e95b048638600710c1d174d3d18a9a3f1a98228af'))
    core=pd.read_parquet(source.add(archive/'prepared/core.parquet'))
    grid=pd.read_csv(source.add(archive/'prepared/issue_grid.csv'));grid=grid[grid.calendar_mature.eq(True)]
    core=core[core.issue_date.isin(grid.issue_date)]
    if len(core)!=2820 or len(grid)!=20:raise ValueError('SCOPE_CHANGED')
    config=read(itemfile('configuration.json'))
    if config['sample_weight'] is not None or config['lightgbm']['objective']!='poisson':raise ValueError('WEIGHT_OBJECTIVE_CHANGED')
    out.mkdir();started=now();dump(out/'run_contract.json',dict(started_at=started,training=0,inference=0,database_queries=0,api_calls=0,
        source_classification='RECONSTRUCTED_HISTORY_NOT_OOS',scope=[2820,2579],weight='IMPLICIT_UNIFORM',diagnostic_script=sha(__file__)))
    dates,lookup,quality=historical_core(daily);csv(out,'historical_core_calendar',quality)
    monthly['period']=pd.PeriodIndex(monthly.month,freq='M')
    positives={k:np.array(sorted(g.period.unique().asi8),dtype=np.int64) for k,g in monthly[monthly.qty.gt(0)].groupby(['shop','spu'])}
    first={k:int(v[0]) for k,v in positives.items()}
    check=[]
    for r in core.itertuples():
        i=(pd.Timestamp(r.input_cutoff)-dates[0]).days;flags=lookup.get((r.shop,r.spu))
        check.append(dict(issue_date=r.issue_date,shop=r.shop,spu=r.spu,archived_core=True,
            reconstructed_original_core=bool(flags is not None and flags[1][i]==1),
            strict_status=['NON_CORE','CORE','UNKNOWN'][flags[0][i]] if flags is not None else 'UNKNOWN'))
    checks=pd.DataFrame(check);csv(out,'core_reconstruction_check',checks)
    if not checks.reconstructed_original_core.all():raise ValueError('CORE_RULE_MISMATCH')
    units=[];groups=[];products=[];sample=[];models=[];evidence=[]
    for issue in grid.issue_date:
      for segment in ['M1','M2']:
        slot=f'{issue}_{segment}';unit=dict(issue_date=issue,segment=segment)
        t=pd.read_parquet(itemfile(f'prepared/{slot}/train.parquet')).drop(columns='item_key')
        p=archive/'effects/slots'/slot
        a=read(source.add(p/'training_audit.json',original_manifest['files'][f'slots/{slot}/training_audit.json']))
        original_audit(t,a)
        checkpoint=read(source.add(p/'checkpoint.json',original_manifest['files'][f'slots/{slot}/checkpoint.json']))
        check_input=read(itemfile(f'prepared/{slot}/training_check.json'))
        if check_input['old_frame_hash']!=checkpoint['training_sha256']:raise ValueError('TRAIN_FRAME_SOURCE_CHAIN')
        modelpath=source.add(p/'model.txt',checkpoint['model_sha256']);text=modelpath.read_text(encoding='utf-8')
        params=dict(re.findall(r'^\[([^:]+): (.*?)\]$',text,re.M))
        if params.get('objective')!='poisson':raise ValueError('SAVED_OBJECTIVE')
        models.append(dict(**unit,objective=params['objective'],metric=params.get('metric'),
            poisson_max_delta_step=params.get('poisson_max_delta_step'),model_sha256=sha(modelpath),sample_weight='IMPLICIT_UNIFORM'))
        t['core_state']='UNKNOWN';t['historical_maturity']='UNKNOWN'
        for key,idx in t.groupby(['shop','spu'],sort=False).groups.items():
            c=t.loc[idx,'input_cutoff'];pos=(c-dates[0]).dt.days.to_numpy();flag=lookup[key][0][pos]
            t.loc[idx,'core_state']=np.array(['NON_CORE','CORE','UNKNOWN'])[flag]
            completed=(c+pd.Timedelta(days=1)).dt.to_period('M').array.asi8-1
            age=completed-first[key]+1;t.loc[idx,'historical_maturity']=age_band(age)
        total=float(t.y_label.sum());counts=t.groupby(['shop','spu']).size();ps=[]
        for key,g in t.groupby(['shop','spu']):
            n=disjoint(g.label_start);ps.append(dict(**unit,shop=key[0],spu=key[1],rows=len(g),
                core_rows=int(g.core_state.eq('CORE').sum()),unknown_rows=int(g.core_state.eq('UNKNOWN').sum()),
                zero_rows=int(g.y_label.eq(0).sum()),label_units=float(g.y_label.sum()),
                first_simulated_issue=str(g.simulated_issue_date.min().date()),last_simulated_issue=str(g.simulated_issue_date.max().date()),
                max_nonoverlap_windows=n,overlap_excess_rows=len(g)-n))
        products.extend(ps)
        units.append(dict(**unit,rows=len(t),objects=len(counts),simulated_dates=int(t.simulated_issue_date.nunique()),
          zero_rows=int(t.y_label.eq(0).sum()),zero_share=float(t.y_label.eq(0).mean()),label_units=total,
          core_share=float(t.core_state.eq('CORE').mean()),core_label_share=float(t.loc[t.core_state.eq('CORE'),'y_label'].sum()/total),
          unknown_share=float(t.core_state.eq('UNKNOWN').mean()),mean_rows_per_item=float(counts.mean()),
          min_rows_per_item=int(counts.min()),p25_rows_per_item=float(counts.quantile(.25)),median_rows_per_item=float(counts.median()),
          p75_rows_per_item=float(counts.quantile(.75)),p90_rows_per_item=float(counts.quantile(.9)),max_rows_per_item=int(counts.max()),
          top10pct_items_row_share=float(counts.nlargest(int(np.ceil(.1*len(counts)))).sum()/len(t)),
          sum_item_disjoint_windows=sum(x['max_nonoverlap_windows'] for x in ps),
          overlap_excess_rows=sum(x['overlap_excess_rows'] for x in ps),max_global_disjoint_windows=disjoint(t.label_start),
          sample_weight='IMPLICIT_UNIFORM',train_audit_pass=True))
        for group in ['core_state','shop','historical_maturity']:groups.extend(aggregate_train(t,group,unit))
        for status,g in t.groupby('core_state'):
            sample.extend(g.iloc[np.unique(np.linspace(0,len(g)-1,min(3,len(g)),dtype=int))][['issue_date','segment','shop','spu','simulated_issue_date','input_cutoff','core_state','historical_maturity','y_label']].to_dict('records'))
        evidence.append(dict(**unit,train_sha256=sha(item/f'prepared/{slot}/train.parquet'),all_old_columns_exact=True,
            max_label_end=str(t.label_end.max()),actual_cutoff=str(pd.Timestamp(issue)-pd.Timedelta(days=2))))
        print(slot,len(t),round(units[-1]['core_share'],4),round(units[-1]['zero_share'],4),flush=True)
    for name,values in [('training_units',units),('training_groups',groups),('product_support',products),('sample_audit_keys',sample),('model_objective_evidence',models),('training_verification',evidence)]:csv(out,name,pd.DataFrame(values))
    support=pd.read_parquet(itemfile('identity_support.parquet'))
    obj=pd.read_parquet(itemfile('scoring/objects.parquet'));base=obj[obj.model.eq('DIRECT')]
    unseen=base[base.paired.eq(True)&base.seen_group.eq('UNSEEN_BOTH')]
    if len(unseen)!=418:raise ValueError('UNSEEN_SCOPE_CHANGED')
    dslookup={}
    for key,g in daily[daily.mapped.eq(True)].groupby(['shop','spu']):
        g=g.set_index('ds').reindex(dates);known=g.sales_state.isin(VALID).to_numpy()&np.isfinite(g.y.to_numpy())&g.y.ge(0).to_numpy()&g.mapped.eq(True).to_numpy()
        dslookup[key]=(known,g.y.to_numpy())
    trace=[]
    for r in unseen.itertuples():
        key=(r.shop,r.spu);cut=pd.Timestamp(r.issue_date)-pd.Timedelta(days=2);known,qty=dslookup[key]
        months=positives.get(key,np.array([],dtype=np.int64));completed=(cut+pd.Timedelta(days=1)).to_period('M').ordinal-1
        available=months[months<=completed];first_qual=pd.Period(ordinal=int(available[3]),freq='M').end_time.normalize()+pd.Timedelta(days=2) if len(available)>=4 else pd.NaT
        k=(dates<=cut);positive=k&known&(qty>0);hist_dates=dates[positive]
        for seg in ['M1','M2']:
            sim,reasons,qual,hist=trace_reason(dates,known,months,cut,seg)
            found=support[(support.issue_date==r.issue_date)&(support.shop==r.shop)&(support.spu==r.spu)&(support.segment==seg)].iloc[0]
            n=int((reasons=='INCLUDED').sum())
            if n!=found.training_rows:raise ValueError('UNSEEN_TRACE_DISAGREES')
            mature_qual=int(((sim+pd.Timedelta(days=30 if seg=='M1' else 60)<=cut)&qual).sum())
            cause='NO_MATURE_QUALIFIED_LABEL' if mature_qual==0 and len(available)>=4 else 'MISSING_HISTORY_OR_LABEL' if n==0 else 'UNEXPECTED'
            trace.append(dict(issue_date=r.issue_date,shop=r.shop,spu=r.spu,segment=seg,maturity=r.maturity,
                actual_cutoff=str(cut.date()),first_positive_month=str(pd.Period(ordinal=int(available[0]),freq='M')),
                positive_completed_months=len(available),known_daily_history_days=int((k&known).sum()),
                observed_positive_days=int(positive.sum()),first_positive_day=str(hist_dates.min().date()) if len(hist_dates) else None,
                past_sales_units=float(qty[k&known].sum()),first_eligible_simulated_issue=str(first_qual.date()),
                earliest_eligible_label_end=str((first_qual+pd.Timedelta(days=30 if seg=='M1' else 60)).date()),
                qualified_candidate_rows=int(qual.sum()),qualified_mature_rows=mature_qual,
                excluded_qualification=int((reasons=='INSUFFICIENT_QUALIFICATION').sum()),
                excluded_immature=int((reasons=='LABEL_NOT_MATURE').sum()),
                excluded_history=int((reasons=='MISSING_HISTORY').sum()),excluded_label_days=int((reasons=='MISSING_LABEL_DAYS').sum()),
                included_rows=n,latest28_known=bool(known[(dates>=cut-pd.Timedelta(days=27))&(dates<=cut)].all()),cause=cause))
    csv(out,'unseen_418_segment_causes',pd.DataFrame(trace))
    # Eight archived models; neither model files nor predictions are executed.
    old=pd.read_parquet(source.add(comp/'historical_windows.parquet',manifest['files']['historical_windows.parquet']))
    latest=pd.read_parquet(itemfile('scoring/scores.parquet'))
    scores=pd.concat([old,latest[latest.model.isin(['DIRECT_TRAFFIC_STATE_V1','DIRECT_ITEM_ID_V1'])]],ignore_index=True)
    frozen_objects=pd.read_parquet(source.add(comp/'objects.parquet',manifest['files']['objects.parquet']))
    scores=canonical_scope(scores,frozen_objects)
    if scores.duplicated(KEY+['model','window']).any() or scores.model.nunique()!=8:raise ValueError('PREDICTION_SCOPE')
    # Shared truth, not a model-dependent subset.
    for _,g in scores.groupby(KEY+['window']):
        if g.actual.nunique(dropna=False)!=1:raise ValueError('TRUTH_CONFLICT')
    objects=[]
    for (model,issue,shop,spu),g in scores.groupby(['model']+KEY):
        g=g.set_index('window').reindex(WINDOWS);y=g.actual.to_numpy();p=g.prediction.to_numpy()
        if np.isfinite(p).all() and not np.isclose(p[0]+p[1],p[2],rtol=1e-14,atol=1e-10):raise ValueError('SEGMENT_SUM')
        hit=bool(np.isfinite(p).all() and np.isfinite(y).all() and (y>0).all() and (abs(p-y)<=.2*y).all())
        rec=dict(model=model,issue_date=issue,shop=shop,spu=spu,joint_hit=hit,actual=float(y[2]))
        objects.append(dict(**rec,scope='FULL'))
        if g.paired.iloc[0]:
            objects.append(dict(**rec,scope='PAIRED'))
            if g.decline_group.iloc[0]=='PRE_ISSUE_DECLINING':objects.append(dict(**rec,scope='DECLINE_PAIRED'))
    metrics,joint=score_models(scores,pd.DataFrame(objects));csv(out,'model_metrics',metrics);csv(out,'model_joint',joint)
    seen=latest[latest.paired.eq(True)&latest.seen_group.eq('SEEN_BOTH')]
    seen_scores=seen.copy();seen_scores['paired']=True
    so=[]
    for r in obj[obj.paired.eq(True)&obj.seen_group.eq('SEEN_BOTH')].itertuples():
        so.append(dict(model=r.model,scope='PAIRED',joint_hit=r.joint_hit,actual=r.actual_d01_60))
    sm,sj=score_models(seen_scores,pd.DataFrame(so));csv(out,'seen_both_metrics',sm[sm.scope.eq('PAIRED')]);csv(out,'seen_both_joint',sj)
    # Independent sklearn numerical check only (no model import or inference).
    from sklearn.metrics import mean_poisson_deviance
    independent=[]
    for (model,win),g in scores[scores.paired.eq(True)].groupby(['model','window']):
        z=g[g.prediction.gt(0)&g.actual.notna()];v=float(deviance(z.actual,z.prediction).mean());ref=float(mean_poisson_deviance(z.actual,z.prediction))
        if not np.isclose(v,ref,rtol=1e-12,atol=1e-9):raise ValueError('POISSON_INDEPENDENT_MISMATCH')
        independent.append(dict(model=model,window=win,n=len(z),own=v,sklearn=ref,absolute_difference=abs(v-ref)))
    csv(out,'independent_poisson_check',pd.DataFrame(independent))
    source.verify()
    dump(out/'manifest.json',dict(started_at=started,completed_at=now(),sources=source.files,shadow_protected_files=len(shadow),
       old_archives_unchanged=True,training=0,inference=0,database_queries=0,api_calls=0,production_changes=0,
       full_core_n=len(core),paired_n=2579,unseen_both_n=len(unseen),units=len(units),
       files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}))
    print('DIAGNOSTIC_COMPLETE',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args();run(a.root,a.out)
