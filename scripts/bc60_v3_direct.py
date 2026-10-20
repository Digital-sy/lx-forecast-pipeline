"""One preregistered direct cumulative experiment; local immutable archives only."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import importlib.metadata
import json
from pathlib import Path
import shutil
import time
import traceback

import numpy as np
import pandas as pd

VALID = {'OBSERVED', 'EXPLICIT_ZERO'}
WINDOWS = {'d01_30': (1, 30), 'd31_60': (31, 60), 'd01_60': (1, 60)}
FEATURES = ['shop_code','lag1','lag7','lag14','lag28',
            'rolling_mean_lag1_window_size7','rolling_mean_lag1_window_size28',
            'dayofweek','month','dayofyear']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                    default=str, allow_nan=False), encoding='utf-8')


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def completed_month(cutoff):
    return (pd.Timestamp(cutoff).normalize()+pd.Timedelta(days=1)).to_period('M')-1


def bounds(issue, segment):
    a,b = {'M1':(1,30),'M2':(31,60)}[segment]
    r=pd.Timestamp(issue)
    return r-pd.Timedelta(days=2),r+pd.Timedelta(days=a),r+pd.Timedelta(days=b)


def native_mean(values):
    values=np.asarray(values,dtype=np.float32)
    return np.add.accumulate(values,dtype=np.float32)[-1]/np.float32(len(values))


def features_from_history(values, shop_code, target_start):
    values=np.asarray(values,dtype=np.float32)
    if len(values)!=28 or not np.isfinite(values).all() or (values<0).any():
        raise ValueError('MISSING_HISTORY')
    start=pd.Timestamp(target_start)
    return dict(zip(FEATURES,[shop_code,*[values[-lag] for lag in (1,7,14,28)],
        native_mean(values[-7:]),native_mean(values),start.dayofweek,start.month,start.dayofyear]))


def disjoint_windows(frame):
    end=None;count=0
    for row in frame.sort_values(['label_end','label_start']).itertuples():
        if end is None or row.label_start>end:
            count+=1;end=row.label_end
    return count


def support_status(frame):
    if frame.empty:return 'NOT_TRAINABLE'
    if not frame.y_label.gt(0).any():return 'NOT_TRAINABLE_ZERO_LABEL_SUPPORT'
    return 'LIMITED_TEMPORAL_SUPPORT' if disjoint_windows(frame[['label_start','label_end']].drop_duplicates())<2 else 'TRAINABLE_REPLAY_ONLY'


class Context:
    def __init__(self,daily,monthly,shops,cutoff):
        self.cutoff=pd.Timestamp(cutoff)
        d=daily[daily.ds.le(self.cutoff)&daily.shop.isin(shops)].copy()
        if d.duplicated(['shop','spu','ds']).any():raise ValueError('DUPLICATE_DAILY')
        self.first=pd.Timestamp(d.ds.min());self.dates=pd.date_range(self.first,self.cutoff)
        self.shops=shops;self.data={};self.months={}
        monthly=monthly.copy();monthly['period']=pd.PeriodIndex(monthly.month,freq='M')
        monthly=monthly[monthly.period.le(completed_month(self.cutoff))&monthly.qty.gt(0)]
        for key,g in monthly.groupby(['shop','spu']):
            self.months[key]=sorted(set(g.period))
        for key,g in d[d.mapped].groupby(['shop','spu'],sort=True):
            z=g.set_index('ds').reindex(self.dates)
            vals=z.y.to_numpy(dtype=float)
            known=np.isfinite(vals)&(vals>=0)&z.sales_state.isin(VALID).to_numpy()&z.mapped.fillna(False).to_numpy(dtype=bool)
            self.data[key]=(vals,known,np.r_[0.,np.cumsum(np.where(known,vals,0.))],np.r_[0,np.cumsum(~known)])

    def qualification(self,key,cutoff):
        end=completed_month(cutoff)
        return sum(m<=end for m in self.months.get(key,[]))

    def row(self,key,issue,segment,actual_issue):
        c,a,b=bounds(issue,segment);h=c-pd.Timedelta(days=27)
        row={'issue_date':str(pd.Timestamp(actual_issue).date()),'simulated_issue_date':pd.Timestamp(issue),
             'input_cutoff':c,'shop':key[0],'spu':key[1],'segment':segment,'label_start':a,'label_end':b,
             'history_start':h,'history_end':c,'positive_completed_months':self.qualification(key,c)}
        row.update({k:np.nan for k in FEATURES});row['input_status']='OK'
        if row['positive_completed_months']<4:row['input_status']='INSUFFICIENT_QUALIFICATION'
        elif key not in self.data or h<self.first or c>self.cutoff:row['input_status']='MISSING_HISTORY'
        else:
            vals,known,_,_=self.data[key];i=(c-self.first).days
            if not known[i-27:i+1].all():row['input_status']='MISSING_HISTORY'
            else:row.update(features_from_history(vals[i-27:i+1],self.shops.index(key[0]),a))
        return row

    def training(self,actual_issue,segment):
        rows=[];excluded=[]
        # Candidate days include immature labels to make exclusions explicit.
        simulated=pd.date_range(self.first+pd.Timedelta(days=29),self.cutoff)
        for key in sorted(self.data):
            vals,known,totals,missing=self.data[key]
            for r in simulated:
                c,a,b=bounds(r,segment)
                qual=self.qualification(key,c)
                if qual<4:reason='INSUFFICIENT_QUALIFICATION'
                elif b>self.cutoff:reason='LABEL_NOT_MATURE'
                else:
                    i=(c-self.first).days
                    if not known[i-27:i+1].all():reason='MISSING_HISTORY'
                    else:
                        ia=(a-self.first).days;ib=(b-self.first).days+1
                        reason='MISSING_LABEL_DAYS' if missing[ib]!=missing[ia] else 'INCLUDED'
                if reason!='INCLUDED':
                    excluded.append((str(r.date()),reason));continue
                row=self.row(key,r,segment,actual_issue)
                row['y_label']=float(totals[ib]-totals[ia]);rows.append(row)
        columns=['issue_date','simulated_issue_date','input_cutoff','shop','spu','segment','label_start','label_end',
                 'history_start','history_end','positive_completed_months','input_status',*FEATURES,'y_label']
        frame=pd.DataFrame(rows,columns=columns)
        exc=pd.DataFrame(excluded,columns=['simulated_issue_date','reason'])
        exc=exc.groupby(['simulated_issue_date','reason']).size().reset_index(name='rows') if len(exc) else pd.DataFrame(columns=['simulated_issue_date','reason','rows'])
        exc['issue_date']=str(pd.Timestamp(actual_issue).date());exc['segment']=segment
        return frame,exc


def prepare(v1,v2,config,out):
    v1,v2,config,out=map(Path,(v1,v2,config,out))
    lock=json.loads(config.read_text());base=Path(lock['base_config'])
    if sha(base)!=lock['base_config_sha256']:raise ValueError('BASE_CONFIG_HASH')
    if sha(Path('scripts/bc60_experiment.py'))!=lock['original_source_sha256']:raise ValueError('BASE_SOURCE_HASH')
    paths=[v1/'inputs_v1_final/daily.parquet',v1/'inputs_v1_final/monthly_reference.parquet',
           *[v1/'effects_v1'/x for x in ['core_lists.csv','pre_effect_universe.csv','daily_forecasts.parquet','scores.csv','run_manifest.json','fits.json']],
           v2/'a_diagnostic_final/stage_scores.csv',v2/'a_diagnostic_final/daily_d0_d1.parquet',v2/'a_diagnostic_final/diagnostic_manifest.json']
    sources={str(p.resolve()):sha(p) for p in paths}
    if sha(paths[0])!=lock['daily_sha256'] or sha(paths[2])!=lock['core_sha256']:raise ValueError('INPUT_HASH')
    baseconf=json.loads(base.read_text())
    runtime={p:importlib.metadata.version(p) for p in baseconf['candidate_libraries']}
    if runtime!=baseconf['candidate_libraries']:raise ValueError('RUNTIME_MISMATCH')
    out.mkdir(exist_ok=False);(out/'train').mkdir();(out/'inference').mkdir()
    start=now();clock=time.monotonic()
    d=pd.read_parquet(paths[0]);m=pd.read_parquet(paths[1]);core=pd.read_csv(paths[2]);core=core[core.is_core].copy()
    original=pd.read_csv(v1/'effects_v1/scores.csv')
    eligibility=original[(original.model=='B1')&(original.window=='d01_60')][['issue_date','shop','spu','prediction']].copy()
    eligibility['b1_original_eligible']=eligibility.prediction.notna();eligibility=eligibility.drop(columns='prediction')
    core=core.merge(eligibility,on=['issue_date','shop','spu'],validate='one_to_one')
    if len(core)!=385 or core[['shop','spu']].drop_duplicates().shape[0]!=174 or core.b1_original_eligible.sum()!=369:raise ValueError('CORE_GRID')
    core.to_csv(out/'fixed_core.csv',index=False,encoding='utf-8-sig')
    coverage=[];exclusions=[];obj=[]
    for issue in lock['issue_dates']:
        cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2);ctx=Context(d,m,lock['shops'],cutoff)
        for segment in ('M1','M2'):
            train,exc=ctx.training(issue,segment);exclusions.append(exc)
            train.to_parquet(out/'train'/f'{issue}_{segment}.parquet',index=False)
            infer=pd.DataFrame([ctx.row((r.shop,r.spu),issue,segment,issue) for r in core[core.issue_date.eq(issue)].itertuples()])
            infer.to_parquet(out/'inference'/f'{issue}_{segment}.parquet',index=False)
            for key,g in train.groupby(['shop','spu']):
                obj.append({'issue_date':issue,'segment':segment,'shop':key[0],'spu':key[1],
                            'train_rows':len(g),'nonoverlap_windows':disjoint_windows(g)})
            coverage.append({'issue_date':issue,'segment':segment,'input_cutoff':str(cutoff.date()),
                'context_objects':len(ctx.data),'train_rows':len(train),'train_objects':len(train[['shop','spu']].drop_duplicates()),
                'train_issue_dates':train.simulated_issue_date.nunique(),'first_simulated_issue':str(train.simulated_issue_date.min()),
                'last_simulated_issue':str(train.simulated_issue_date.max()),'first_label':str(train.label_start.min()),
                'last_label':str(train.label_end.max()),'positive_labels':int(train.y_label.gt(0).sum()),
                'zero_labels':int(train.y_label.eq(0).sum()),'max_global_nonoverlap':disjoint_windows(train) if len(train) else 0,
                'support_status':support_status(train),'inference_ok':int(infer.input_status.eq('OK').sum())})
            print(issue,segment,len(train),coverage[-1]['train_issue_dates'],coverage[-1]['support_status'],flush=True)
    pd.DataFrame(coverage).to_csv(out/'coverage.csv',index=False,encoding='utf-8-sig')
    pd.concat(exclusions).to_csv(out/'exclusions.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(obj).to_csv(out/'per_object_windows.csv',index=False,encoding='utf-8-sig')
    dump(out/'frozen_config.json',{'v3':lock,'lightgbm':baseconf['lightgbm'],'runtime':runtime})
    files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}
    dump(out/'prepare_manifest.json',{'started_at':start,'completed_at':now(),'elapsed_seconds':time.monotonic()-clock,
        'sources':sources,'config_sha256':sha(config),'script_sha256':sha(__file__),'files':files,
        'classification':lock['classification'],'fit_count':0})


def score(actual,prediction,truth_status,prediction_status):
    ok=pd.notna(actual) and pd.notna(prediction)
    rel=(prediction-actual)/actual if ok and actual>0 else np.nan
    return {'actual':actual,'prediction':prediction,'truth_status':truth_status,'prediction_status':prediction_status,
            'decision':'UNJUDGED' if pd.isna(rel) else ('HIT' if abs(rel)<=.2 else 'MISS'),
            'relative_error':rel,'absolute_error':abs(prediction-actual) if ok else np.nan}


def metrics(g):
    z=g[g.actual.notna()&g.prediction.notna()];e=z.prediction-z.actual;total=float(z.actual.sum());allactual=float(g.actual.sum())
    hit=g.decision.eq('HIT')
    return {'fixed_n':len(g),'output_n':int(g.prediction.notna().sum()),'truth_n':int(g.actual.notna().sum()),
      'scored_n':len(z),'hit_n':int(hit.sum()),'miss_n':int(g.decision.eq('MISS').sum()),'unjudged_n':int(g.decision.eq('UNJUDGED').sum()),
      'positive_truth_n':int(g.actual.gt(0).sum()),'hit_rate':float(hit.sum()/g.actual.gt(0).sum()) if g.actual.gt(0).any() else np.nan,
      'hit_sales_share':float(g.loc[hit,'actual'].sum()/allactual) if allactual else np.nan,
      'actual_units':total,'known_fixed_actual_units':allactual,'forecast_units':float(z.prediction.sum()),
      'wape':float(e.abs().sum()/total) if total else np.nan,'bias':float(e.sum()/total) if total else np.nan,
      'over_units':float(e.clip(lower=0).sum()),'under_units':float((-e).clip(lower=0).sum()),
      'zero_actual_n':int(g.actual.eq(0).sum()),'zero_actual_absolute_error':float(z.loc[z.actual.eq(0),'absolute_error'].sum())}


def summarize(scores):
    groups=[]
    for scope,sub in [('full_core',scores),('original_b1_369',scores[scores.b1_original_eligible]),
                      ('common_complete',scores[scores.common_complete])]:
        for sl,keys in [('overall',[]),('issue',['issue_date']),('shop',['shop']),('maturity',['maturity']),('lifecycle',['lifecycle'])]:
            for vals,g in sub.groupby(['model','window']+keys,dropna=False):
                groups.append({'scope':scope,'slice':sl,**dict(zip(['model','window']+keys,vals)),**metrics(g)})
    return pd.DataFrame(groups)


def evaluate(prepared,predictions,out):
    manifest=json.loads((prepared/'prepare_manifest.json').read_text());paths=list(manifest['sources'])
    d=pd.read_parquet(next(p for p in paths if p.endswith('daily.parquet')))
    core=pd.read_csv(prepared/'fixed_core.csv')
    orig=pd.read_csv(next(p for p in paths if p.endswith('effects_v1\\scores.csv') or p.endswith('effects_v1/scores.csv')))
    diag=pd.read_csv(next(p for p in paths if p.endswith('stage_scores.csv')))
    truth={(s,p):g.set_index('ds') for (s,p),g in d.groupby(['shop','spu'])}
    op=orig.set_index(['issue_date','shop','spu','window','model'])
    dp=diag[diag.model.eq('D1')].set_index(['issue_date','shop','spu','stage'])
    pp=predictions.set_index(['issue_date','shop','spu','segment'])
    records=[]
    for c in core.itertuples():
        key=(c.shop,c.spu);issue=pd.Timestamp(c.issue_date)
        for win,(a,b) in WINDOWS.items():
            dates=pd.date_range(issue+pd.Timedelta(days=a),issue+pd.Timedelta(days=b))
            t=truth[key].reindex(dates)
            known=np.isfinite(t.y).all() and t.y.ge(0).all() and t.sales_state.isin(VALID).all() and t.mapped.eq(True).all()
            actual=float(t.y.sum()) if known else np.nan
            for model in ['F28','B1','D1','DIRECT30x2']:
                if model in ['F28','B1']:
                    r=op.loc[(c.issue_date,*key,win,'rolling28' if model=='F28' else 'B1')]
                    value=r.prediction;status=r.prediction_state
                elif model=='D1':
                    r=dp.loc[(c.issue_date,*key,win)];value=r.prediction;status=r.prediction_state
                else:
                    names=['M1'] if win=='d01_30' else ['M2'] if win=='d31_60' else ['M1','M2']
                    r=pp.loc[[(c.issue_date,*key,k) for k in names]]
                    value=float(r.prediction.sum()) if r.prediction.notna().all() else np.nan
                    status='OK' if pd.notna(value) else '|'.join(sorted(set(r.prediction_status[r.prediction.isna()])))
                records.append({'issue_date':c.issue_date,'shop':c.shop,'spu':c.spu,'window':win,'model':model,
                    'lifecycle':c.lifecycle,'maturity':c.maturity,'b1_original_eligible':c.b1_original_eligible,
                    'input_cutoff':str((issue-pd.Timedelta(days=2)).date()),'target_start':str(dates[0].date()),'target_end':str(dates[-1].date()),
                    **score(actual,value,'OBSERVED' if known else 'UNKNOWN_TRUTH',status)})
    scores=pd.DataFrame(records)
    complete=scores.groupby(['issue_date','shop','spu']).apply(lambda g:bool(g.prediction.notna().all() and g.actual.notna().all()),include_groups=False)
    scores['common_complete']=[bool(complete.loc[(r.issue_date,r.shop,r.spu)]) for r in scores.itertuples()]
    scores.to_csv(out/'scores.csv',index=False,encoding='utf-8-sig');scores.to_parquet(out/'scores.parquet',index=False)
    summarize(scores).to_csv(out/'summary.csv',index=False,encoding='utf-8-sig')
    joints=[]
    for key,g in scores.groupby(['issue_date','shop','spu','model']):
        joints.append({**dict(zip(['issue_date','shop','spu','model'],key)),
            'joint_decision':'UNJUDGED' if g.decision.eq('UNJUDGED').any() else 'HIT' if g.decision.eq('HIT').all() else 'MISS',
            'actual60':float(g.loc[g.window.eq('d01_60'),'actual'].iloc[0]),'b1_original_eligible':bool(g.b1_original_eligible.iloc[0]),
            'common_complete':bool(g.common_complete.iloc[0])})
    joints=pd.DataFrame(joints);joints.to_csv(out/'joint_hits.csv',index=False,encoding='utf-8-sig')
    pairs=[];migrations=[]
    for scope,sub in [('full_core',scores),('original_b1_369',scores[scores.b1_original_eligible]),('common_complete',scores[scores.common_complete])]:
        left=sub[sub.model.eq('F28')];right=sub[sub.model.eq('DIRECT30x2')]
        pair=left.merge(right,on=['issue_date','shop','spu','window'],suffixes=('_f28','_direct'),validate='one_to_one')
        pair['scope']=scope;pair['absolute_error_change']=pair.absolute_error_direct-pair.absolute_error_f28
        pair['change']=np.where(pair.absolute_error_change.isna(),'UNJUDGED',np.where(pair.absolute_error_change<0,'IMPROVED',np.where(pair.absolute_error_change>0,'DEGRADED','UNCHANGED')))
        pairs.append(pair)
        j=joints if scope=='full_core' else joints[joints.b1_original_eligible] if scope=='original_b1_369' else joints[joints.common_complete]
        j=j[j.model.eq('F28')].merge(j[j.model.eq('DIRECT30x2')],on=['issue_date','shop','spu'],suffixes=('_f28','_direct'),validate='one_to_one')
        def migration(row):
            a,b=row.joint_decision_f28,row.joint_decision_direct
            if 'UNJUDGED' in (a,b):return 'UNJUDGED'
            return 'RETAINED_HIT' if a==b=='HIT' else 'NEW_HIT' if b=='HIT' else 'EXITED_HIT' if a=='HIT' else 'BOTH_MISS'
        j['migration']=j.apply(migration,axis=1);j['scope']=scope;migrations.append(j)
    pd.concat(pairs).to_csv(out/'paired_details.csv',index=False,encoding='utf-8-sig')
    pd.concat(migrations).to_csv(out/'joint_migrations.csv',index=False,encoding='utf-8-sig')
    pairsummary=[]
    for scope,p in pd.concat(pairs).groupby('scope'):
        for sl,keys in [('overall',[]),('issue',['issue_date']),('shop',['shop'])]:
            for vals,g in p.groupby(['window']+keys):
                valid=g.absolute_error_change.notna();z=g[valid];den=float(z.actual_f28.sum())
                pairsummary.append({'scope':scope,'slice':sl,**dict(zip(['window']+keys,vals)),
                    'fixed_n':len(g),'paired_n':len(z),'unjudged_n':int((~valid).sum()),
                    **{k.lower()+'_n':int(z.change.eq(k).sum()) for k in ['IMPROVED','DEGRADED','UNCHANGED']},
                    'improved_sales_share':float(z.loc[z.change.eq('IMPROVED'),'actual_f28'].sum()/den) if den else np.nan,
                    'degraded_sales_share':float(z.loc[z.change.eq('DEGRADED'),'actual_f28'].sum()/den) if den else np.nan,
                    'wape_change':float(z.absolute_error_change.sum()/den) if den else np.nan})
    pd.DataFrame(pairsummary).to_csv(out/'paired_summary.csv',index=False,encoding='utf-8-sig')


def reserve_fit_run(ledger,out):
    """Exclusive reservation survives a crash; another output dir cannot refit."""
    with Path(ledger).open('x',encoding='utf-8') as f:
        json.dump({'reserved_at':now(),'output':str(Path(out).resolve()),'fits':[]},f)


def run(prepared,out,models_from=None):
    from lightgbm import LGBMRegressor, Booster
    prepared,out=map(Path,(prepared,out));manifest=json.loads((prepared/'prepare_manifest.json').read_text())
    for rel,digest in manifest['files'].items():
        if sha(prepared/rel)!=digest:raise ValueError('PREPARED_FILE_CHANGED:'+rel)
    for path,digest in manifest['sources'].items():
        if sha(path)!=digest:raise ValueError('ORIGINAL_FILE_CHANGED')
    conf=json.loads((prepared/'frozen_config.json').read_text());lock=conf['v3']
    core=pd.read_csv(prepared/'fixed_core.csv')
    for issue in lock['issue_dates']:
        for segment in ('M1','M2'):
            inf=pd.read_parquet(prepared/'inference'/f'{issue}_{segment}.parquet')
            z=inf.merge(core[['issue_date','shop','spu','b1_original_eligible']],on=['issue_date','shop','spu'],validate='one_to_one')
            if (z.input_status.eq('OK')&~z.b1_original_eligible).any():raise ValueError('ORIGINAL_16_QUALIFICATION_CHANGED')
    ledger=prepared.parent/'formal_fit_ledger.json'
    recovery={}
    if models_from:
        models_from=Path(models_from)
        if not ledger.exists():raise ValueError('NO_PRIOR_FIT_LEDGER')
        for f in json.loads((models_from/'fits.json').read_text()):recovery[(f['issue_date'],f['segment'])]=f
    elif ledger.exists():raise RuntimeError('FORMAL_FITS_ALREADY_RESERVED_USE_CHECKPOINT_RECOVERY')
    out.mkdir(exist_ok=False);(out/'models').mkdir();start=now();clock=time.monotonic();fits=[];predictions=[]
    if not models_from:reserve_fit_run(ledger,out)
    coverage=pd.read_csv(prepared/'coverage.csv').set_index(['issue_date','segment'])
    for issue in lock['issue_dates']:
        for segment in ('M1','M2'):
            t=pd.read_parquet(prepared/'train'/f'{issue}_{segment}.parquet')
            inf=pd.read_parquet(prepared/'inference'/f'{issue}_{segment}.parquet')
            fit={'issue_date':issue,'segment':segment,'status':str(coverage.loc[(issue,segment),'support_status']),
                 'started_at':now(),'fit_attempted':False,'training_sha256':sha(prepared/'train'/f'{issue}_{segment}.parquet')}
            model=None;raw={}
            try:
                if fit['status'].startswith('NOT_TRAINABLE'):pass
                elif models_from:
                    prior=recovery.get((issue,segment),{})
                    modelpath=models_from/'models'/f'{issue}_{segment}.txt'
                    if not prior.get('model_sha256') or not modelpath.exists():raise ValueError('MISSING_CHECKPOINT_NO_REFIT')
                    if sha(modelpath)!=prior['model_sha256']:raise ValueError('CHECKPOINT_HASH')
                    if prior.get('fit_seconds',0)>lock['max_seconds_per_fit']:raise ValueError('ORIGINAL_FIT_TIME_BUDGET')
                    if prior.get('training_sha256')!=fit['training_sha256']:raise ValueError('CHECKPOINT_TRAINING_HASH')
                    shutil.copy2(modelpath,out/'models'/modelpath.name)
                    model=Booster(model_file=str(modelpath));fit['model_sha256']=sha(modelpath)
                    fit['recovered_from']=str(modelpath.resolve());fit['actual_trees']=model.num_trees()
                else:
                    if len([f for f in fits if f['fit_attempted']])>=6:raise RuntimeError('FIT_BUDGET')
                    model=LGBMRegressor(**conf['lightgbm']);fit['fit_attempted']=True
                    dump(out/'fit_in_progress.json',fit)
                    tic=time.monotonic();model.fit(t[FEATURES],t.y_label)
                    fit['fit_seconds']=time.monotonic()-tic
                    modelpath=out/'models'/f'{issue}_{segment}.txt';model.booster_.save_model(str(modelpath))
                    fit['model_sha256']=sha(modelpath);fit['actual_trees']=model.booster_.num_trees()
                    dump(out/'models'/f'{issue}_{segment}_meta.json',fit)
                    if fit['fit_seconds']>lock['max_seconds_per_fit']:raise RuntimeError('FIT_TIME_BUDGET')
                if model is not None:
                    valid=inf.input_status.eq('OK')
                    if valid.any():
                        values=model.predict(inf.loc[valid,FEATURES])
                        if not np.isfinite(values).all():raise ValueError('NONFINITE_PREDICTIONS')
                        raw=dict(zip(inf.index[valid],values))
                    fit['status']='RECOVERED' if models_from else 'FITTED';fit['negative_clipped_n']=int(sum(x<0 for x in raw.values()))
            except Exception as error:
                fit['status']='TECHNICAL_FAILURE';fit['error']=str(error);fit['traceback']=traceback.format_exc();raw={}
            fit['completed_at']=now();fits.append(fit);dump(out/'fits.json',fits)
            if not models_from:dump(ledger,{'reserved_at':start,'output':str(out.resolve()),'fits':fits})
            for i,row in inf.iterrows():
                value=raw.get(i,np.nan);status='OK' if pd.notna(value) else row.input_status if row.input_status!='OK' else fit['status']
                predictions.append({'issue_date':issue,'shop':row.shop,'spu':row.spu,'segment':segment,
                    'prediction_raw':value,'prediction':max(value,0) if pd.notna(value) else np.nan,
                    'prediction_status':status,'model_sha256':fit.get('model_sha256'),
                    'input_cutoff':row.input_cutoff,'target_start':row.label_start,'target_end':row.label_end})
            pd.DataFrame(predictions).to_csv(out/'predictions.csv',index=False,encoding='utf-8-sig')
            print(issue,segment,fit['status'],fit.get('fit_seconds'),flush=True)
    predictions=pd.DataFrame(predictions);evaluate(prepared,predictions,out)
    original_unchanged=all(sha(p)==h for p,h in manifest['sources'].items())
    dump(out/'run_manifest.json',{'started_at':start,'completed_at':now(),'elapsed_seconds':time.monotonic()-clock,
        'formal_fit_count':sum(x['fit_attempted'] for x in fits),'originals_unchanged':original_unchanged,
        'prepared_manifest_sha256':sha(prepared/'prepare_manifest.json'),'script_sha256':sha(__file__),
        'classification':lock['classification'],'files':{str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}})
    if not original_unchanged:raise ValueError('ORIGINAL_CHANGED')


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('prepare');a.add_argument('--v1',required=True);a.add_argument('--v2',required=True);a.add_argument('--config',required=True);a.add_argument('--out',required=True)
    a=sub.add_parser('run');a.add_argument('--prepared',required=True);a.add_argument('--out',required=True)
    a=sub.add_parser('recover');a.add_argument('--prepared',required=True);a.add_argument('--models-from',required=True);a.add_argument('--out',required=True)
    a=p.parse_args()
    if a.command=='prepare':prepare(a.v1,a.v2,a.config,a.out)
    else:run(a.prepared,a.out,getattr(a,'models_from',None))


if __name__=='__main__':main()
