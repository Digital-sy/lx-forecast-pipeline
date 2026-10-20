"""Finite monthly replay executor. Local files only; no source-system clients.

Preparation freezes cohorts before effects. Each learning slot is attempted at
most once under an exclusive archive ledger; recovery never fits a model.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import traceback

import numpy as np
import pandas as pd

try:
    from scripts import bc60_experiment as original, bc60_v3_direct as direct
except ModuleNotFoundError:
    import bc60_experiment as original
    import bc60_v3_direct as direct

ROOT = Path(__file__).resolve().parents[1]
SOURCE_HASHES = {
    'scripts/bc60_experiment.py': '5e433c029ebd8fa2c60e19a389a9d2ce5cd3c344238645160cec69f7c0e4f745',
    'scripts/bc60_v3_direct.py': '97cbb1c3fa7c0e49ebd0dbac849ee723dc4c5ebd51fd2ccfd39b0860ccf36513',
    'docs/bc60_design_lock_v1.json': '3527e51cf6336e25706fe9cf3ea7f4c0a486341e4f6011f9e2760bd4409ebf28',
}
SHOPS = ['JQ-US', 'MT-US', 'RKZ-US', 'SY-US']
VALID = {'OBSERVED', 'EXPLICIT_ZERO'}
KINDS = ['B1', 'B2', 'B3', 'M1', 'M2']
WINDOWS = direct.WINDOWS
PRED_COLS = ['cohort_id','issue_month','issue_date','input_cutoff','shop','spu','model','window',
             'target_start','target_end','prediction','prediction_status']
EXOG = {'B1': [], 'B2': ['session_mean28','order_session_ratio28'],
        'B3': ['session_mean28','order_session_ratio28','ad_spend_mean28','ad_clicks_mean28','ad_orders_mean28']}
RAW_EXOG = ['listing_sessions_sum','null_sessions','order_qty','ad_spend','ad_clicks','ad_orders']
ORIGINAL_CORE_SHA256 = 'f634f9e1ee857be71db2e4f31e7136a4d7f6ff08761fb227edea972cc71f32cf'


class NotTrainable(ValueError):
    """A valid insufficient-support result, never a technical fit failure."""


def after_lag_drop_status(processed):
    if processed.empty:return 'NOT_TRAINABLE_AFTER_LAG_DROP'
    if not processed.y.gt(0).any():return 'NOT_TRAINABLE_ZERO_LABEL_SUPPORT'
    return 'OK'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False), encoding='utf-8')


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def frame_hash(frame):
    schema = json.dumps([(c,str(frame[c].dtype)) for c in frame], ensure_ascii=False).encode()
    return hashlib.sha256(schema + pd.util.hash_pandas_object(frame,index=False).to_numpy().tobytes()).hexdigest()


def training_audit(frame,sort_keys,columns):
    canonical=frame.sort_values(sort_keys,kind='stable')[columns]
    return {'rows':len(frame),'sort_keys':sort_keys,'columns':columns,
            'method':'sha256(pandas.util.hash_pandas_object(series,index=False).uint64.tobytes); native dtype recorded separately',
            'dtypes':{c:str(canonical[c].dtype) for c in columns},
            'column_sha256':{c:hashlib.sha256(pd.util.hash_pandas_object(canonical[c],index=False).to_numpy().tobytes()).hexdigest() for c in columns}}


def issue_grid():
    rows=[]
    for month in pd.period_range('2025-01','2026-10',freq='M'):
        issue=month.start_time+pd.Timedelta(days=1)
        rows.append({'issue_month':str(month),'issue_date':str(issue.date()),
                     'input_cutoff':str((issue-pd.Timedelta(days=2)).date()),
                     'target_start':str((issue+pd.Timedelta(days=1)).date()),
                     'target_end':str((issue+pd.Timedelta(days=60)).date()),
                     'calendar_mature':issue+pd.Timedelta(days=60)<=pd.Timestamp('2026-10-01')})
    return pd.DataFrame(rows)


def normalize(daily, monthly):
    d=daily.copy(); d['ds']=pd.to_datetime(d.ds).dt.normalize()
    if d.duplicated(['shop','spu','ds']).any():raise ValueError('DUPLICATE_DAILY')
    required={'shop','spu','ds','y','sales_state','mapped'}
    if not required.issubset(d):raise ValueError('DAILY_SCHEMA')
    known=d.sales_state.isin(VALID)&d.mapped.eq(True)
    y=pd.to_numeric(d.y,errors='coerce')
    if (~np.isfinite(y[known])|y[known].lt(0)|(y[known]%1).ne(0)).any():
        raise ValueError('KNOWN_SALES_MUST_BE_NONNEGATIVE_INTEGER')
    m=monthly.copy();m['month']=pd.PeriodIndex(m.month,freq='M')
    if m.duplicated(['shop','spu','month']).any():raise ValueError('DUPLICATE_MONTHLY')
    return d,m


def freeze_issue(d,m,issue):
    """Unknown shop calendar => no invented list; mapping gaps stay denominator."""
    issue=pd.Timestamp(issue);cutoff=issue-pd.Timedelta(days=2)
    first=cutoff-pd.Timedelta(days=89)
    window=d[d.ds.between(first,cutoff)]
    life=original.lifecycle(m,issue)
    all_rows=[];quality=[]
    for shop in SHOPS:
        z=window[window.shop.eq(shop)]
        days=z.ds.nunique()
        valid=z.sales_state.isin(VALID)&np.isfinite(z.y)&z.y.ge(0)
        invalid_days=z.loc[~valid,'ds'].nunique()
        status='OK' if days==90 and invalid_days==0 else 'INSUFFICIENT_HISTORY' if days!=90 else 'INVALID_SELECTION_INPUT'
        q={'issue_date':str(issue.date()),'issue_month':str(issue.to_period('M')),'shop':shop,
           'input_cutoff':str(cutoff.date()),'selection_start':str(first.date()),'selection_end':str(cutoff.date()),
           'expected_days':90,'present_days':int(days),'invalid_days':int(invalid_days),
           'selection_status':status,'core_n':None,'full_shop_units':None,'mapped_units':None,
           'unmapped_units':None,'selected_units':None,'selection_coverage':None}
        if status=='OK':
            total=float(z.y.sum());mapped=z[z.mapped.eq(True)]
            rank=mapped.groupby(['shop','spu']).agg(selection_qty=('y','sum'),selection_days=('ds','nunique')).reset_index()
            rank=original.select_core(rank,total,.9)
            rank=rank.merge(life,on=['shop','spu'],how='left',validate='one_to_one')
            rank['lifecycle']=rank.lifecycle.fillna('UNKNOWN');rank['maturity']=rank.maturity.fillna('UNKNOWN')
            rank['category']='UNKNOWN';rank['issue_date']=str(issue.date());rank['issue_month']=str(issue.to_period('M'))
            rank['input_cutoff']=str(cutoff.date());rank['cohort_id']='monthly_core';rank['shop_selection_total']=total
            selected=float(rank.loc[rank.is_core,'selection_qty'].sum())
            q.update(core_n=int(rank.is_core.sum()),full_shop_units=total,mapped_units=float(mapped.y.sum()),
                     unmapped_units=float(z.loc[~z.mapped.eq(True),'y'].sum()),selected_units=selected,
                     selection_coverage=selected/total if total>0 else None)
            if total<=0:q['selection_status']='NO_POSITIVE_SELECTION_SALES'
            elif selected<.9*total:q['selection_status']='MAPPING_COVERAGE_GAP'
            all_rows.append(rank)
        quality.append(q)
    columns=['shop','spu','selection_qty','selection_days','is_core','first_positive','positive_months',
             'observed_age_months','lifecycle','maturity','category','issue_date','issue_month','input_cutoff','cohort_id','shop_selection_total']
    return pd.concat(all_rows,ignore_index=True) if all_rows else pd.DataFrame(columns=columns),pd.DataFrame(quality)


def prepare(inputs,archive,out):
    inputs,archive,out=map(Path,(inputs,archive,out))
    for name,digest in SOURCE_HASHES.items():
        if sha(ROOT/name)!=digest:raise ValueError('FROZEN_SOURCE_CHANGED:'+name)
    lock=read_json(ROOT/'docs/bc60_design_lock_v1.json')
    runtime={p:importlib.metadata.version(p) for p in lock['candidate_libraries']}
    if runtime!=lock['candidate_libraries']:raise ValueError('RUNTIME_MISMATCH')
    source_paths=[inputs/'daily.parquet',inputs/'monthly_reference.parquet',archive/'effects_v1/core_lists.csv']
    if sha(source_paths[2])!=ORIGINAL_CORE_SHA256:raise ValueError('ORIGINAL_CORE_CHANGED')
    sources={str(p.resolve()):sha(p) for p in source_paths}
    d,m=normalize(pd.read_parquet(source_paths[0]),pd.read_parquet(source_paths[1]))
    out.mkdir(exist_ok=False);grid=issue_grid();grid.to_csv(out/'issue_grid.csv',index=False)
    universes=[];qualities=[]
    for issue in grid.issue_date:
        u,q=freeze_issue(d,m,issue);universes.append(u);qualities.append(q)
    universe=pd.concat(universes,ignore_index=True);core=universe[universe.is_core.eq(True)].copy()
    for name,frame in [('universe',universe),('core',core)]:
        frame.to_parquet(out/f'{name}.parquet',index=False);frame.to_csv(out/f'{name}.csv',index=False,encoding='utf-8-sig')
    pd.concat(qualities,ignore_index=True).to_csv(out/'selection_quality.csv',index=False,encoding='utf-8-sig')
    old=pd.read_csv(source_paths[2]);old=old[old.is_core.eq(True)].copy()
    old['cohort_id']='original_fixed_385';old['issue_month']=old.issue_date.str[:7]
    old['input_cutoff']=(pd.to_datetime(old.issue_date)-pd.Timedelta(days=2)).dt.strftime('%Y-%m-%d')
    old.to_csv(out/'original_core_panel.csv',index=False,encoding='utf-8-sig')
    conf={'original':lock,'runtime':runtime,'truth_end':'2026-10-01','max_fits':100,'max_seconds_per_slot':600,
          'source_hashes':SOURCE_HASHES,'inputs':str(inputs.resolve()),'classification':'EXTENDED_HISTORY_RETRAIN_REPLAY'}
    dump(out/'frozen_config.json',conf)
    dump(out/'prepare_manifest.json',{'created_at':now(),'sources':sources,'script_sha256':sha(__file__),
         'files':{p.name:sha(p) for p in out.iterdir() if p.is_file()},'fit_count':0,
         'daily_first':str(d.ds.min()),'daily_last':str(d.ds.max()),'original_panel_rows':len(old),
         'daily_rows':len(d),'daily_y_dtype':str(d.y.dtype),'source_fields':list(d.columns)})


class VectorContext(direct.Context):
    """Same V3 context/history rules; vectorize dates, never compress the calendar."""
    def training(self,actual_issue,segment):
        columns=['issue_date','simulated_issue_date','input_cutoff','shop','spu','segment','label_start','label_end',
                 'history_start','history_end','positive_completed_months','input_status',*direct.FEATURES,'y_label']
        dates=pd.date_range(self.first+pd.Timedelta(days=29),self.cutoff)
        if not len(dates):return pd.DataFrame(columns=columns),pd.DataFrame(columns=['simulated_issue_date','reason','rows','issue_date','segment'])
        cutoffs=dates-pd.Timedelta(days=2);positions=(cutoffs-self.first).days.to_numpy()
        a,b={'M1':(1,30),'M2':(31,60)}[segment]
        start=dates+pd.Timedelta(days=a);end=dates+pd.Timedelta(days=b)
        left=(start-self.first).days.to_numpy();right=(end-self.first).days.to_numpy()+1
        completed=(cutoffs+pd.Timedelta(days=1)).to_period('M').asi8-1
        reasons={r:np.zeros(len(dates),dtype=np.int64) for r in ['INSUFFICIENT_QUALIFICATION','LABEL_NOT_MATURE','MISSING_HISTORY','MISSING_LABEL_DAYS']}
        parts=[]
        for key in sorted(self.data):
            vals,known,totals,missing=self.data[key]
            counts=np.searchsorted(np.array([p.ordinal for p in self.months.get(key,[])],dtype=np.int64),completed,side='right')
            valid=counts>=4;reasons['INSUFFICIENT_QUALIFICATION']+=~valid
            mature=np.asarray(end<=self.cutoff);reasons['LABEL_NOT_MATURE']+=valid&~mature;valid&=mature
            hist_ok=missing[positions+1]==missing[positions-27]
            reasons['MISSING_HISTORY']+=valid&~hist_ok;valid&=hist_ok
            label_ok=missing[np.minimum(right,len(vals))]==missing[np.minimum(left,len(vals))]
            reasons['MISSING_LABEL_DAYS']+=valid&~label_ok;valid&=label_ok
            take=np.flatnonzero(valid)
            if not len(take):continue
            histories=np.lib.stride_tricks.sliding_window_view(vals.astype(np.float32),28)[positions[take]-27]
            selected_start=start[take]
            frame=pd.DataFrame({'issue_date':str(pd.Timestamp(actual_issue).date()),'simulated_issue_date':dates[take],
                'input_cutoff':cutoffs[take],'shop':key[0],'spu':key[1],'segment':segment,'label_start':selected_start,
                'label_end':end[take],'history_start':cutoffs[take]-pd.Timedelta(days=27),'history_end':cutoffs[take],
                'positive_completed_months':counts[take],'input_status':'OK','shop_code':self.shops.index(key[0]),
                **{f'lag{lag}':histories[:,-lag] for lag in [1,7,14,28]},
                'rolling_mean_lag1_window_size7':np.add.accumulate(histories[:,-7:],axis=1,dtype=np.float32)[:,-1]/np.float32(7),
                'rolling_mean_lag1_window_size28':np.add.accumulate(histories,axis=1,dtype=np.float32)[:,-1]/np.float32(28),
                'dayofweek':selected_start.dayofweek,'month':selected_start.month,'dayofyear':selected_start.dayofyear,
                'y_label':totals[right[take]]-totals[left[take]]})
            parts.append(frame[columns])
        exc=pd.DataFrame([{'simulated_issue_date':str(dates[i].date()),'reason':r,'rows':int(n),
                           'issue_date':str(pd.Timestamp(actual_issue).date()),'segment':segment}
                          for r,counts in reasons.items() for i,n in enumerate(counts) if n])
        return pd.concat(parts,ignore_index=True) if parts else pd.DataFrame(columns=columns),exc


def legacy_training(d,m,issue):
    cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2)
    hist=d[d.ds.le(cutoff)&d.shop.isin(SHOPS)].copy()
    for col in RAW_EXOG:
        if col not in hist:hist[col]=np.nan
    life=original.lifecycle(m,pd.Timestamp(issue))
    qualified=set(life.loc[life.lifecycle.eq('ESTABLISHED'),['shop','spu']].itertuples(index=False,name=None))
    train,terminal,idmap=original.make_segments(hist,qualified)
    # NULL elsewhere must not promote the original integer-label native state.
    if len(train):train['y']=train.y.astype(np.int64)
    # Original source assumes hist.ds.max == actual cutoff. Do not predict from a
    # stale terminal series when a later shop/date is absent in an extension.
    terminal={uid:g for uid,g in terminal.items() if g.ds.max()==cutoff}
    idmap={uid:idmap[uid] for uid in terminal}
    return train,terminal,idmap,hist,qualified


def inference_objects(prepared,issue):
    core=pd.read_parquet(prepared/'core.parquet');old=pd.read_csv(prepared/'original_core_panel.csv')
    rows=pd.concat([core[core.issue_date.eq(issue)],old[old.issue_date.eq(issue)]],ignore_index=True)
    return rows,rows[['shop','spu']].drop_duplicates()


def training_stats(frame,kind):
    if frame.empty:return {'train_rows':0,'train_objects':0,'train_dates':0,'positive_labels':0,'zero_labels':0}
    date='ds' if kind.startswith('B') else 'simulated_issue_date';label='y' if kind.startswith('B') else 'y_label'
    return {'train_rows':len(frame),'train_objects':len(frame[['shop','spu']].drop_duplicates()) if 'shop' in frame else frame.unique_id.nunique(),
            'train_dates':frame[date].nunique(),'train_start':str(frame[date].min()),'train_end':str(frame[date].max()),
            'train_span_days':int((frame[date].max()-frame[date].min()).days+1),
            'positive_labels':int(frame[label].gt(0).sum()),'zero_labels':int(frame[label].eq(0).sum()),
            'training_sha256':frame_hash(frame)}


def nonoverlap_count(starts):
    starts=np.sort(np.unique(np.asarray(starts,dtype='datetime64[ns]')))
    index=count=0
    while index<len(starts):
        count+=1
        index=int(np.searchsorted(starts,starts[index]+np.timedelta64(29,'D'),side='right'))
    return count


def worker(prepared,issue,kind,out,recovery=None):
    """One process/slot. The orchestrator enforces a 600-second hard timeout."""
    from lightgbm import LGBMRegressor,Booster
    from mlforecast import MLForecast
    from mlforecast.lag_transforms import RollingMean
    prepared,out=Path(prepared),Path(out);out.mkdir(exist_ok=False)
    conf=read_json(prepared/'frozen_config.json');inputs=Path(conf['inputs']);lock=conf['original']
    d,m=normalize(pd.read_parquet(inputs/'daily.parquet'),pd.read_parquet(inputs/'monthly_reference.parquet'))
    cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2);_,objects=inference_objects(prepared,issue)
    result={'issue_date':issue,'kind':kind,'started_at':now(),'fit_attempted':False,'status':'PREFLIGHT',
            'input_cutoff':str(cutoff.date()),'source_first_day':str(d.ds.min().date()),
            'available_calendar_days':max(0,int((cutoff-d.ds.min()).days)+1)}
    outputs=[]
    try:
        if kind.startswith('B'):
            required=[] if kind=='B1' else ['listing_sessions_sum','null_sessions','order_qty']+(['ad_spend','ad_clicks','ad_orders'] if kind=='B3' else [])
            available=d[d.ds.le(cutoff)&d.shop.isin(SHOPS)]
            absent=[c for c in required if c not in available or not available[c].notna().any()]
            if absent:result.update(status='NOT_AVAILABLE_EXOGENOUS',unavailable_fields=absent)
            else:
                train,terminal,idmap,hist,qualified=legacy_training(d,m,issue)
                result.update(training_stats(train,kind));result['qualified_context_objects']=len(qualified)
                result['training_stats_basis']='before_lag_drop'
                result['latest_history_date']=str(hist.ds.max())
                result['inference_context_objects']=len(terminal)
                if not len(train) or not terminal:result['status']='NOT_TRAINABLE'
                elif not train.y.gt(0).any():result['status']='NOT_TRAINABLE_ZERO_LABEL_SUPPORT'
                else:
                    cols=['unique_id','ds','y','shop_code']+EXOG[kind]
                    result['target_dtype']=str(train.y.dtype)
                    result['missing_exogenous_cells']={c:int(train[c].isna().sum()) for c in EXOG[kind]}
                    model=MLForecast(models={kind:LGBMRegressor(**lock['lightgbm'])},freq='D',lags=lock['lags'],
                        lag_transforms={1:[RollingMean(n) for n in lock['rolling_windows_on_lag_1']]},
                        date_features=lock['date_features'],num_threads=2)
                    # Inspect the exact public default lag/dropna transform in
                    # either route; never preprocess the loaded recovery model.
                    processed=model.preprocess(train[cols],static_features=['shop_code'])
                    dump(out/'training_audit.json',training_audit(processed,['unique_id','ds'],list(processed.columns)))
                    result.update(rows_after_lag_drop=len(processed),
                        effective_label_first=str(processed.ds.min()),effective_label_last=str(processed.ds.max()),
                        effective_label_dates=processed.ds.nunique(),effective_positive_labels=int(processed.y.gt(0).sum()),
                        effective_zero_labels=int(processed.y.eq(0).sum()),effective_target_dtype=str(processed.y.dtype))
                    support=after_lag_drop_status(processed)
                    if support!='OK':raise NotTrainable(support)
                    if recovery:
                        prior=read_json(Path(recovery)/'checkpoint.json')
                        if prior.get('training_sha256')!=result['training_sha256']:raise ValueError('RECOVERY_TRAINING_CHANGED')
                        for name,digest in prior['checkpoint_hashes'].items():
                            if sha(Path(recovery)/'model'/name)!=digest:raise ValueError('CHECKPOINT_CHANGED')
                        model=MLForecast.load(Path(recovery)/'model')
                        shutil.copytree(Path(recovery)/'model',out/'model')
                    else:
                        result['fit_attempted']=True;dump(out/'fit_started.json',result)
                        tic=time.monotonic();model.fit(train[cols],static_features=['shop_code']);result['fit_seconds']=time.monotonic()-tic
                        model.save(out/'model')
                    result['checkpoint_hashes']={p.name:sha(p) for p in (out/'model').iterdir() if p.is_file()}
                    dump(out/'checkpoint.json',result)
                    days=pd.date_range(cutoff+pd.Timedelta(days=1),pd.Timestamp(issue)+pd.Timedelta(days=60));future=[]
                    if EXOG[kind]:
                        for uid,g in terminal.items():
                            last=g.tail(28);sessions=last.listing_sessions_sum.where(last.null_sessions.eq(0))
                            values={'session_mean28':sessions.mean() if sessions.notna().all() else np.nan,
                                    'order_session_ratio28':last.order_qty.sum()/sessions.sum() if sessions.notna().all() and sessions.sum()>0 else np.nan}
                            for c in ['ad_spend','ad_clicks','ad_orders']:values[c+'_mean28']=last[c].mean() if last[c].notna().all() else np.nan
                            future.extend({'unique_id':uid,'ds':day,**{c:values[c] for c in EXOG[kind]}} for day in days)
                    result['negative_clipped_n']=0;result['raw_prediction_min']=None
                    def nonnegative_feedback(values):
                        result['negative_clipped_n']+=int(np.sum(values<0))
                        minimum=float(np.min(values)) if len(values) else None
                        if minimum is not None and np.isfinite(minimum):
                            result['raw_prediction_min']=minimum if result['raw_prediction_min'] is None else min(result['raw_prediction_min'],minimum)
                        return np.maximum(values,0)
                    pred=model.predict(len(days),ids=list(terminal),X_df=pd.DataFrame(future) if EXOG[kind] else None,after_predict_callback=nonnegative_feedback)
                    if not np.isfinite(pred[kind]).all():raise ValueError('NONFINITE_PREDICTION')
                    pred['shop']=pred.unique_id.map(lambda u:idmap[u][0]);pred['spu']=pred.unique_id.map(lambda u:idmap[u][1])
                    pred.to_parquet(out/'daily_forecasts.parquet',index=False)
                    available_predictions={(s,p):g.set_index('ds')[kind] for (s,p),g in pred.groupby(['shop','spu'])}
                    for obj in objects.itertuples():
                        key=(obj.shop,obj.spu)
                        for window,(a,b) in WINDOWS.items():
                            dates=pd.date_range(pd.Timestamp(issue)+pd.Timedelta(days=a),pd.Timestamp(issue)+pd.Timedelta(days=b))
                            values=available_predictions[key].reindex(dates) if key in available_predictions else None
                            value=float(values.sum()) if values is not None and values.notna().all() else np.nan
                            status='OK' if np.isfinite(value) else 'INSUFFICIENT_QUALIFICATION' if key not in qualified else 'MISSING_HISTORY'
                            outputs.append({'shop':obj.shop,'spu':obj.spu,'window':window,'prediction':value,'prediction_status':status})
                    result['status']='RECOVERED' if recovery else 'FITTED'
        else:
            if d[d.ds.le(cutoff)&d.mapped.eq(True)].empty:result['status']='NOT_TRAINABLE'
            else:
                ctx=VectorContext(d,m,SHOPS,cutoff);train,exc=ctx.training(issue,kind)
                audit_columns=['shop','spu','simulated_issue_date','input_cutoff','label_start','label_end','y_label',*direct.FEATURES]
                dump(out/'training_audit.json',training_audit(train,['shop','spu','simulated_issue_date'],audit_columns))
                exc.to_csv(out/'training_exclusions.csv',index=False)
                result.update(training_stats(train,kind));result['status']=direct.support_status(train)
                result['training_stats_basis']='fully_valid_mature_cumulative_labels'
                result['max_global_nonoverlap']=direct.disjoint_windows(train[['label_start','label_end']].drop_duplicates()) if len(train) else 0
                obj_stats=[]
                for key,g in train.groupby(['shop','spu'],sort=True):
                    obj_stats.append({'issue_date':issue,'segment':kind,'shop':key[0],'spu':key[1],
                        'train_rows':len(g),'simulated_dates':g.simulated_issue_date.nunique(),
                        'first_simulated_issue':str(g.simulated_issue_date.min()),'last_simulated_issue':str(g.simulated_issue_date.max()),
                        'nonoverlap_windows':nonoverlap_count(g.label_start)})
                pd.DataFrame(obj_stats,columns=['issue_date','segment','shop','spu','train_rows','simulated_dates',
                    'first_simulated_issue','last_simulated_issue','nonoverlap_windows']).to_csv(out/'per_object_windows.csv',index=False)
                if len(train):result.update(first_label=str(train.label_start.min()),last_label=str(train.label_end.max()))
                if not result['status'].startswith('NOT_TRAINABLE'):
                    if recovery:
                        prior=read_json(Path(recovery)/'checkpoint.json')
                        if prior.get('training_sha256')!=result['training_sha256']:raise ValueError('RECOVERY_TRAINING_CHANGED')
                        path=Path(recovery)/'model.txt'
                        if sha(path)!=prior['model_sha256']:raise ValueError('CHECKPOINT_CHANGED')
                        model=Booster(model_file=os.path.relpath(path,Path.cwd()));shutil.copyfile(path,out/'model.txt')
                    else:
                        path=out/'model.txt';relative=os.path.relpath(path,Path.cwd());relative.encode('ascii')
                        model=LGBMRegressor(**lock['lightgbm']);result['fit_attempted']=True;dump(out/'fit_started.json',result)
                        tic=time.monotonic();model.fit(train[direct.FEATURES],train.y_label);result['fit_seconds']=time.monotonic()-tic
                        model.booster_.save_model(relative)
                    result['model_sha256']=sha(out/'model.txt');dump(out/'checkpoint.json',result)
                    inf=pd.DataFrame([ctx.row((r.shop,r.spu),issue,kind,issue) for r in objects.itertuples()])
                    valid=inf.input_status.eq('OK');raw={}
                    if valid.any():
                        vals=model.predict(inf.loc[valid,direct.FEATURES])
                        if not np.isfinite(vals).all():raise ValueError('NONFINITE_PREDICTION')
                        raw=dict(zip(inf.index[valid],vals))
                    inf['prediction_raw']=[raw.get(i,np.nan) for i in inf.index]
                    inf['prediction']=inf.prediction_raw.clip(lower=0)
                    result['negative_clipped_n']=int(inf.prediction_raw.lt(0).sum())
                    result['raw_prediction_min']=float(inf.prediction_raw.min()) if inf.prediction_raw.notna().any() else None
                    inf.to_parquet(out/'segment_predictions.parquet',index=False)
                    for r in inf.itertuples():outputs.append({'shop':r.shop,'spu':r.spu,'window':'d01_30' if kind=='M1' else 'd31_60',
                        'prediction':r.prediction,'prediction_status':'OK' if np.isfinite(r.prediction) else r.input_status})
                    result['support_status']=result['status'];result['status']='RECOVERED' if recovery else 'FITTED'
        pd.DataFrame(outputs,columns=['shop','spu','window','prediction','prediction_status']).to_parquet(out/'predictions.parquet',index=False)
    except NotTrainable as error:
        result.update(status=str(error),fit_attempted=False)
        pd.DataFrame(outputs,columns=['shop','spu','window','prediction','prediction_status']).to_parquet(out/'predictions.parquet',index=False)
    except Exception as error:
        result.update(status='TECHNICAL_FAILURE',error=str(error),traceback=traceback.format_exc())
    result['completed_at']=now();dump(out/'result.json',result)


def baseline_values(d,issue,objects):
    cutoff=pd.Timestamp(issue)-pd.Timedelta(days=2);dates=pd.date_range(cutoff-pd.Timedelta(days=27),cutoff)
    history=d[d.ds.between(dates[0],dates[-1])]
    groups={(s,p):g.set_index('ds') for (s,p),g in history.groupby(['shop','spu'])}
    output=[]
    for row in objects.itertuples():
        g=groups.get((row.shop,row.spu));known=False
        if g is not None:
            z=g.reindex(dates);known=z.y.notna().all() and z.sales_state.isin(VALID).all() and z.mapped.eq(True).all()
        for model in ['F28','WEEKDAY4']:
            for window,(a,b) in WINDOWS.items():
                value=np.nan
                if known:
                    targets=pd.date_range(pd.Timestamp(issue)+pd.Timedelta(days=a),pd.Timestamp(issue)+pd.Timedelta(days=b))
                    value=float(z.y.mean()*(b-a+1)) if model=='F28' else float(sum(z.groupby(z.index.dayofweek).y.mean().loc[t.dayofweek] for t in targets))
                output.append({'shop':row.shop,'spu':row.spu,'model':model,'window':window,'prediction':value,
                               'prediction_status':'OK' if known else 'MISSING_HISTORY'})
    return output


def run(prepared,out,models_from=None):
    prepared,out=Path(prepared),Path(out);manifest=read_json(prepared/'prepare_manifest.json')
    if sha(__file__)!=manifest['script_sha256']:raise ValueError('EXECUTOR_CHANGED_AFTER_PREPARE')
    for name,digest in manifest['files'].items():
        if sha(prepared/name)!=digest:raise ValueError('PREPARED_CHANGED:'+name)
    for path,digest in manifest['sources'].items():
        if sha(path)!=digest:raise ValueError('SOURCE_CHANGED')
    for name,digest in SOURCE_HASHES.items():
        if sha(ROOT/name)!=digest:raise ValueError('FROZEN_SOURCE_CHANGED')
    conf=read_json(prepared/'frozen_config.json');inputs=Path(conf['inputs'])
    if {p:importlib.metadata.version(p) for p in conf['runtime']}!=conf['runtime']:raise ValueError('RUNTIME_CHANGED')
    d,m=normalize(pd.read_parquet(inputs/'daily.parquet'),pd.read_parquet(inputs/'monthly_reference.parquet'))
    if out.exists():raise ValueError('OUTPUT_EXISTS')
    ledger=prepared.parent/'formal_fit_ledger.json'
    if models_from:
        if not ledger.exists():raise ValueError('NO_ORIGINAL_LEDGER')
    else:
        with ledger.open('x',encoding='utf-8') as f:json.dump({'reserved_at':now(),'output':str(out.resolve()),'attempts':[]},f)
    out.mkdir(exist_ok=False);(out/'slots').mkdir();fits=[];predictions=[];start=now()
    for issue_row in issue_grid().itertuples():
        issue=issue_row.issue_date;cohorts,objects=inference_objects(prepared,issue)
        values=baseline_values(d,issue,objects);slot_values={}
        for kind in KINDS:
            slot=out/'slots'/f'{issue}_{kind}'
            result={'issue_date':issue,'kind':kind,'fit_attempted':False,'status':'NOT_MATURED' if not issue_row.calendar_mature else 'NO_CORE_OBJECTS'}
            if issue_row.calendar_mature and len(objects):
                recovery=Path(models_from)/'slots'/f'{issue}_{kind}' if models_from else None
                if recovery and not (recovery/'checkpoint.json').exists():result['status']='MISSING_CHECKPOINT_NO_REFIT'
                else:
                    command=[sys.executable,'-B',str(Path(__file__).resolve()),'_worker','--prepared',str(prepared.resolve()),'--issue',issue,'--kind',kind,'--out',str(slot.resolve())]
                    if recovery:command+=['--recovery',str(recovery.resolve())]
                    before={'issue_date':issue,'kind':kind,'status':'SLOT_RESERVED','fit_attempted':False,'reserved_at':now()}
                    if not models_from:dump(ledger,{'reserved_at':start,'output':str(out.resolve()),'attempts':fits+[before]})
                    try:
                        process=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=600,cwd=ROOT,
                                               env={**os.environ,'PYTHONIOENCODING':'utf-8','PYTHONDONTWRITEBYTECODE':'1'})
                        result=read_json(slot/'result.json') if (slot/'result.json').exists() else {**result,'status':'WORKER_FAILURE','exit_code':process.returncode}
                        (out/'slots'/f'{issue}_{kind}.log').write_text(process.stdout+'\n'+process.stderr,encoding='utf-8')
                    except subprocess.TimeoutExpired:
                        result.update(status='TIME_BUDGET_EXCEEDED',error='600-second slot limit; no retry')
                    result['fit_attempted']=(slot/'fit_started.json').exists() and not models_from
                    if (slot/'predictions.parquet').exists():slot_values[kind]=pd.read_parquet(slot/'predictions.parquet')
            fits.append(result)
            if not models_from:dump(ledger,{'reserved_at':start,'output':str(out.resolve()),'attempts':fits})
            dump(out/'fits.json',fits)
            if sum(bool(f.get('fit_attempted')) for f in fits)>100:raise RuntimeError('FIT_BUDGET_EXCEEDED')
            print(issue,kind,result['status'],flush=True)
        lookup={(kind,r.shop,r.spu,r.window):(r.prediction,r.prediction_status) for kind,table in slot_values.items() for r in table.itertuples()}
        state={f['kind']:f['status'] for f in fits if f['issue_date']==issue}
        for obj in objects.itertuples():
            for model in ['B1','B2','B3','DIRECT30x2']:
                for window in WINDOWS:
                    if model=='DIRECT30x2':
                        parts=['M1'] if window=='d01_30' else ['M2'] if window=='d31_60' else ['M1','M2']
                        components=[lookup.get((k,obj.shop,obj.spu,'d01_30' if k=='M1' else 'd31_60'),(np.nan,state[k])) for k in parts]
                        value=float(sum(x[0] for x in components)) if all(np.isfinite(x[0]) for x in components) else np.nan
                        status='OK' if np.isfinite(value) else '|'.join(sorted({x[1] for x in components if not np.isfinite(x[0])}))
                    else:value,status=lookup.get((model,obj.shop,obj.spu,window),(np.nan,state[model]))
                    values.append({'shop':obj.shop,'spu':obj.spu,'model':model,'window':window,'prediction':value,'prediction_status':status})
        for c in cohorts.itertuples():
            for r in values:
                if (r['shop'],r['spu'])!=(c.shop,c.spu):continue
                a,b=WINDOWS[r['window']]
                predictions.append({'cohort_id':c.cohort_id,'issue_month':issue[:7],'issue_date':issue,'input_cutoff':issue_row.input_cutoff,
                    'target_start':str((pd.Timestamp(issue)+pd.Timedelta(days=a)).date()),'target_end':str((pd.Timestamp(issue)+pd.Timedelta(days=b)).date()),**r})
        frame=pd.DataFrame(predictions,columns=PRED_COLS);frame.to_parquet(out/'window_predictions.parquet',index=False)
        frame.to_csv(out/'window_predictions.csv',index=False,encoding='utf-8-sig')
        pd.DataFrame(fits).to_csv(out/'training_coverage.csv',index=False,encoding='utf-8-sig')
        pd.DataFrame(fits).to_csv(out/'fit_status.csv',index=False,encoding='utf-8-sig')
    unchanged=all(sha(path)==digest for path,digest in manifest['sources'].items())
    dump(out/'run_manifest.json',{'started_at':start,'completed_at':now(),'classification':conf['classification'],
         'formal_fit_count':sum(bool(f.get('fit_attempted')) for f in fits),'source_unchanged':unchanged,
         'script_sha256':sha(__file__),'prepared_manifest_sha256':sha(prepared/'prepare_manifest.json'),
         'recovery_only':bool(models_from),'files':{str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}})
    if not unchanged:raise ValueError('SOURCE_CHANGED_DURING_RUN')


def main():
    parser=argparse.ArgumentParser(description=__doc__);subs=parser.add_subparsers(dest='command',required=True)
    p=subs.add_parser('prepare');p.add_argument('--inputs',required=True);p.add_argument('--archive',required=True);p.add_argument('--out',required=True)
    p=subs.add_parser('run');p.add_argument('--prepared',required=True);p.add_argument('--out',required=True)
    p=subs.add_parser('recover');p.add_argument('--prepared',required=True);p.add_argument('--out',required=True);p.add_argument('--models-from',required=True)
    p=subs.add_parser('_worker');p.add_argument('--prepared',required=True);p.add_argument('--out',required=True);p.add_argument('--issue',required=True);p.add_argument('--kind',choices=KINDS,required=True);p.add_argument('--recovery')
    args=vars(parser.parse_args());command=args.pop('command')
    if command=='prepare':prepare(**args)
    elif command=='_worker':worker(**args)
    else:run(**args)


if __name__=='__main__':main()
