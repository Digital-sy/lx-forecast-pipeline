"""Fixed daily B+C experiment. Offline files only; never imports production jobs."""
from __future__ import annotations
import argparse, datetime, gzip, hashlib, json, pathlib, time
import numpy as np
import pandas as pd

INVALID_SPU = {'', '-', 'DEFAULT', '无', 'None', 'null'}
WINDOWS = {'d01_30':(1,30), 'd31_60':(31,60), 'd01_60':(1,60)}


def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def dump(path, value):
    pathlib.Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')


def select_core(frame, total_units, fraction=.9):
    """Use full-shop denominator, include all boundary ties; never target-period sales."""
    ranked=frame.sort_values(['selection_qty','spu'],ascending=[False,True]).copy()
    if total_units<=0 or ranked.empty:
        ranked['is_core']=False
        return ranked
    crossed=ranked['selection_qty'].cumsum().ge(total_units*fraction)
    boundary=ranked.loc[crossed,'selection_qty'].iloc[0] if crossed.any() else -np.inf
    ranked['is_core']=ranked['selection_qty'].ge(boundary)
    return ranked


def lifecycle(monthly, issue):
    end=(pd.Timestamp(issue)-pd.Timedelta(days=2)).to_period('M')
    # In this lock issue is day 2: available date is previous complete month-end.
    m=monthly[(monthly.month<=end)&monthly.qty.gt(0)]
    g=m.groupby(['shop','spu']).agg(first_positive=('month','min'),positive_months=('month','nunique')).reset_index()
    g['observed_age_months']=g.first_positive.map(lambda x:end.ordinal-x.ordinal+1)
    g['lifecycle']=np.where(g.positive_months.ge(4),'ESTABLISHED', 'INSUFFICIENT_POSITIVE_HISTORY')
    g['maturity']=pd.cut(g.observed_age_months,[0,3,5,11,999],labels=['0_3','4_5','6_11','12_plus']).astype(str)
    return g


def prepare(archive, monthly_archive, out):
    out=pathlib.Path(out);out.mkdir(exist_ok=False)
    archive=pathlib.Path(archive)
    pieces=[];receipts=[]
    first=archive/'evidence/ods_jan01_07.json'
    d=json.loads(first.read_text(encoding='utf-8'));pieces.append(pd.DataFrame(d['rows']))
    receipts.append({'path':str(first),'sha256':sha(first),'acquired_at':d['acquired_at']})
    for p in sorted((archive/'daily_raw').glob('*.json.gz')):
        with gzip.open(p,'rb') as f: raw=f.read()
        receipt=json.loads((archive/'evidence'/p.name.replace('.json.gz','.json')).read_text(encoding='utf-8'))
        if hashlib.sha256(raw).hexdigest()!=receipt['rows_sha256']: raise ValueError('RAW_HASH_MISMATCH')
        pieces.append(pd.DataFrame(json.loads(raw)))
        receipts.append({'path':str(p),'sha256':sha(p),'acquired_at':receipt['acquired_at']})
    df=pd.concat(pieces,ignore_index=True).rename(columns={'dt':'ds','store_name':'shop','sales_qty':'y'})
    df['ds']=pd.to_datetime(df.ds)
    numeric=[c for c in df if c not in ['ds','shop','spu','min_created_at','max_sync']]
    for c in numeric: df[c]=pd.to_numeric(df[c],errors='coerce')
    feature_invalid={}
    for c in ['ad_spend','ad_clicks','ad_orders','listing_sessions_sum','order_qty']:
        feature_invalid[c]=int(df[c].lt(0).sum())
        df[c]=df[c].where(df[c].ge(0))  # negative counts are UNKNOWN, never zero.
    if df.duplicated(['ds','shop','spu']).any(): raise ValueError('DUPLICATE_SOURCE_GRAIN')
    df['mapped']=df.spu.notna()&~df.spu.isin(INVALID_SPU)&~df.spu.astype(str).str.contains('[|,，;；]',regex=True)
    df['sales_state']=np.select([df.null_qty.gt(0)|df.y.isna(),df.negative_qty.gt(0)|df.date_mismatch.gt(0)|df.deleted_rows.gt(0),df.y.eq(0)],['UNKNOWN','INVALID','EXPLICIT_ZERO'],default='OBSERVED')
    df['category']='UNKNOWN'
    df['source_asof']=df['ds'].dt.strftime('%Y-%m-%d')
    df.to_parquet(out/'daily.parquet',index=False)
    rows=[]
    with open(monthly_archive,encoding='utf-8') as f:
        for line in f:
            x=json.loads(line)
            if x['kind']=='sales': rows.append(x['value'])
    m=pd.DataFrame(rows).rename(columns={'店铺':'shop','SPU':'spu','统计日期':'month','销量':'qty'})
    # Use the supplied SPU field, not a string split of SKU.
    m=m[m.spu.notna()&~m.spu.isin(INVALID_SPU)].copy()
    m['month']=pd.to_datetime(m.month).dt.to_period('M')
    m=m.groupby(['shop','spu','month'],dropna=False).qty.sum(min_count=1).reset_index()
    m['month']=m.month.astype(str);m.to_parquet(out/'monthly_reference.parquet',index=False)
    quality={'source_rows':int(df.source_rows.sum()),'daily_aggregate_rows':len(df),
             'first_day':str(df.ds.min().date()),'last_day':str(df.ds.max().date()),
             'shops':sorted(df.shop.unique()),'source_state_counts':df.sales_state.value_counts().to_dict(),
             'unmapped_units':float(df.loc[~df.mapped,'y'].sum()),'all_units':float(df.y.sum()),
             'max_countries_per_object_day':int(df.country_count.max()),
             'classification':'RECONSTRUCTED_HISTORICAL_REPLAY',
             'monthly_source_limitation':'previous authorized archive excludes missing SKU; never infer ineligible solely from absence',
             'raw_receipts':receipts,'monthly_archive_sha256':sha(monthly_archive),
             'negative_covariates_preserved_as_unknown':feature_invalid}
    daily_part=df.groupby(['shop','ds']).agg(objects=('spu','size'),units=('y','sum')).reset_index()
    daily_part.to_csv(out/'partition_coverage.csv',index=False,encoding='utf-8-sig')
    expected=pd.date_range('2026-01-01','2026-10-01')
    quality['missing_shop_dates']=[f'{s}/{x.date()}' for s,g in daily_part.groupby('shop') for x in expected.difference(g.ds)]
    if quality['missing_shop_dates']: quality['gate']='PARTIAL'
    else: quality['gate']='PASS_FOR_RECONSTRUCTED_OBSERVED_FACTS'
    dump(out/'quality.json',quality)
    dump(out/'input_manifest.json',{'created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'files':{p.name:sha(p) for p in out.iterdir() if p.is_file()}})
    print(json.dumps({k:v for k,v in quality.items() if k not in ['raw_receipts']},ensure_ascii=False),flush=True)


def metric(g, denominator=None):
    valid=g.actual.notna()&g.prediction.notna()
    z=g[valid]; actual=z.actual.to_numpy();pred=z.prediction.to_numpy();err=pred-actual
    total=float(actual.sum());pos=actual>0; hit=np.abs(err[pos])<=.2*actual[pos]
    return {'fixed_core_n':len(g) if denominator is None else denominator,'output_n':int(g.prediction.notna().sum()),
      'truth_n':int(g.actual.notna().sum()),'scored_n':len(z),'positive_truth_n':int(pos.sum()),
      'hit_n':int(hit.sum()),'hit_rate':float(hit.sum()/g.actual.gt(0).sum()) if g.actual.gt(0).any() else None,
      'scored_only_hit_rate':float(hit.mean()) if pos.any() else None,
      'fixed_core_hit_share':float(hit.sum()/len(g)) if len(g) else None,
      'hit_sales_share':float(actual[pos][hit].sum()/g.actual.sum()) if g.actual.sum()>0 else None,
      'known_fixed_core_actual_units':float(g.actual.sum()),
      'actual_units':total,'forecast_units':float(pred.sum()),
      'wape':float(np.abs(err).sum()/total) if total else None,'bias':float(err.sum()/total) if total else None,
      'over_units':float(np.maximum(err,0).sum()),'under_units':float(np.maximum(-err,0).sum()),
      'zero_actual_n':int((actual==0).sum()),'zero_actual_absolute_error':float(np.abs(err[actual==0]).sum())}


def make_segments(hist, qualified):
    """No imputation: split at unknown days so a lag never bridges a calendar gap."""
    result=[]; terminal={};idmap={}
    known=hist[hist.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&hist.mapped]
    cutoff=hist.ds.max()
    for key,g in known.groupby(['shop','spu'],sort=True):
        if key not in qualified:continue
        g=g.sort_values('ds').copy()
        parts=g.ds.diff().ne(pd.Timedelta(days=1)).cumsum()
        for i,s in g.groupby(parts):
            if len(s)<56:continue
            uid=f'{key[0]}|{key[1]}|run{int(i)}'; s=s.copy()
            s['unique_id']=uid;s['shop_code']=['JQ-US','MT-US','RKZ-US','SY-US'].index(key[0])
            sessions=s.listing_sessions_sum.where(s.null_sessions.eq(0))
            s['session_mean28']=sessions.rolling(28,min_periods=28).mean().shift(1)
            ss=sessions.rolling(28,min_periods=28).sum().shift(1)
            s['order_session_ratio28']=s.order_qty.rolling(28,min_periods=28).sum().shift(1)/ss.where(ss.gt(0))
            for c in ['ad_spend','ad_clicks','ad_orders']:
                s[c+'_mean28']=s[c].rolling(28,min_periods=28).mean().shift(1)
            result.append(s)
            if s.ds.max()==cutoff:
                terminal[uid]=s;idmap[uid]=key
    return (pd.concat(result,ignore_index=True) if result else pd.DataFrame()),terminal,idmap


def run(inputs, config, out, allow_exogenous=False):
    from lightgbm import LGBMRegressor
    from mlforecast import MLForecast
    from mlforecast.lag_transforms import RollingMean
    inputs=pathlib.Path(inputs);out=pathlib.Path(out);out.mkdir(exist_ok=False)
    lock=json.loads(pathlib.Path(config).read_text(encoding='utf-8'))
    d=pd.read_parquet(inputs/'daily.parquet')
    m=pd.read_parquet(inputs/'monthly_reference.parquet');m['month']=pd.PeriodIndex(m.month,freq='M')
    all_scores=[];all_core=[];coverage=[];fits=[];forecast_parts=[]
    modelnames=['rolling28','weekday4','B1']+(['B2','B3'] if allow_exogenous else [])
    dump(out/'run_manifest.json',{'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
      'config_sha256':sha(config),'source_sha256':sha(__file__),'inputs':{p.name:sha(p) for p in inputs.glob('*.parquet')},
      'modelnames':modelnames,'status':'RUNNING'})
    for issue_text in lock['issue_dates']:
        issue=pd.Timestamp(issue_text);cutoff=issue-pd.Timedelta(days=2)
        hist=d[d.ds.le(cutoff)].copy();selection=hist[hist.ds.gt(cutoff-pd.Timedelta(days=90))]
        life=lifecycle(m,issue)
        qualified=set(map(tuple,life.loc[life.lifecycle.eq('ESTABLISHED'),['shop','spu']].to_numpy()))
        cores=[]
        for shop in lock['shops']:
            ss=selection[selection.shop.eq(shop)]
            rank=ss[ss.mapped].groupby(['shop','spu']).agg(selection_qty=('y','sum'),selection_days=('ds','nunique'),
                   invalid_selection_days=('sales_state',lambda x:int((~x.isin(['OBSERVED','EXPLICIT_ZERO'])).sum()))).reset_index()
            selected=select_core(rank,float(ss.y.sum()),lock['core_fraction'])
            selected=selected.merge(life,on=['shop','spu'],how='left',validate='one_to_one')
            selected['lifecycle']=selected.lifecycle.fillna('UNKNOWN');selected['maturity']=selected.maturity.fillna('UNKNOWN')
            selected['category']='UNKNOWN';selected['issue_date']=issue_text
            selected['shop_selection_total']=float(ss.y.sum())
            cores.append(selected)
        universe=pd.concat(cores,ignore_index=True);all_core.append(universe)
        core=universe[universe.is_core].copy()
        train,terminal,idmap=make_segments(hist,qualified)
        forecasts={};days=pd.date_range(cutoff+pd.Timedelta(days=1),issue+pd.Timedelta(days=60))
        # Baselines for all fixed core objects, including new/unknown lifecycle.
        for row in core.itertuples():
            key=(row.shop,row.spu);g=hist[(hist.shop==row.shop)&(hist.spu==row.spu)].set_index('ds')
            recent=g.reindex(pd.date_range(cutoff-pd.Timedelta(days=27),cutoff))
            if recent.y.notna().all() and recent.sales_state.isin(['OBSERVED','EXPLICIT_ZERO']).all():
                avg=float(recent.y.mean());wd=recent.groupby(recent.index.dayofweek).y.mean()
                forecasts[('rolling28',key)]=pd.Series(avg,index=days)
                forecasts[('weekday4',key)]=pd.Series([float(wd[t.dayofweek]) for t in days],index=days)
        for name in [n for n in modelnames if n.startswith('B')]:
            cols=['session_mean28','order_session_ratio28'] if name in ['B2','B3'] else []
            if name=='B3':cols+=['ad_spend_mean28','ad_clicks_mean28','ad_orders_mean28']
            fit_cols=['unique_id','ds','y','shop_code']+cols
            model=MLForecast(models={name:LGBMRegressor(**lock['lightgbm'])},freq='D',lags=lock['lags'],
                lag_transforms={1:[RollingMean(x) for x in lock['rolling_windows_on_lag_1']]},
                date_features=lock['date_features'],num_threads=2)
            start=time.monotonic()
            model.fit(train[fit_cols],static_features=['shop_code'])
            xdf=None
            if cols:
                fut=[]
                for uid,g in terminal.items():
                    last=g.tail(28);sessions=last.listing_sessions_sum.where(last.null_sessions.eq(0))
                    values={'session_mean28':sessions.mean() if sessions.notna().all() else np.nan,
                      'order_session_ratio28':last.order_qty.sum()/sessions.sum() if sessions.notna().all() and sessions.sum()>0 else np.nan}
                    for c in ['ad_spend','ad_clicks','ad_orders']:
                        values[c+'_mean28']=last[c].mean() if last[c].notna().all() else np.nan
                    for day in days:fut.append({'unique_id':uid,'ds':day,**{c:values[c] for c in cols}})
                xdf=pd.DataFrame(fut)
            pred=model.predict(len(days),ids=list(terminal),X_df=xdf,after_predict_callback=lambda a:np.maximum(a,0))
            for uid,g in pred.groupby('unique_id'):
                forecasts[(name,idmap[uid])]=g.set_index('ds')[name]
            elapsed=time.monotonic()-start
            fits.append({'issue_date':issue_text,'model':name,'train_rows_before_lag_drop':len(train),'train_segments':train.unique_id.nunique(),
                'qualified_context_objects':len(qualified),'prediction_objects':len(terminal),'elapsed_seconds':elapsed,
                'feature_latest':str(cutoff.date()),'label_latest':str(train.ds.max().date()),'training_target':'one_day_recursive',
                'full_60day_labels_used_for_training_or_tuning':False})
            if elapsed>600:raise RuntimeError('FIT_TIME_BUDGET_EXCEEDED')
            print(issue_text,name,len(pred),round(elapsed,2),flush=True)
        for row in core.itertuples():
            key=(row.shop,row.spu)
            future=d[(d.shop==row.shop)&(d.spu==row.spu)].set_index('ds')
            for window,(a,b) in WINDOWS.items():
                dates=pd.date_range(issue+pd.Timedelta(days=a),issue+pd.Timedelta(days=b))
                truth=future.reindex(dates)
                ok=truth.y.notna().all() and truth.sales_state.isin(['OBSERVED','EXPLICIT_ZERO']).all()
                actual=float(truth.y.sum()) if ok else np.nan
                for name in modelnames:
                    series=forecasts.get((name,key))
                    value=float(series.reindex(dates).sum()) if series is not None and series.reindex(dates).notna().all() else np.nan
                    reason='OK' if pd.notna(value) else ('NEW_OR_QUALIFICATION_UNKNOWN' if name.startswith('B') and key not in qualified else 'INSUFFICIENT_CONSECUTIVE_HISTORY')
                    all_scores.append({'issue_date':issue_text,'shop':row.shop,'spu':row.spu,'window':window,'model':name,
                      'actual':actual,'prediction':value,'prediction_state':reason,'truth_state':'OBSERVED' if ok else 'MISSING_OR_INVALID_DAYS',
                      'lifecycle':row.lifecycle,'maturity':row.maturity,'category':'UNKNOWN','selection_qty':row.selection_qty})
            for name in modelnames:
                series=forecasts.get((name,key))
                if series is not None:
                    forecast_parts.append(pd.DataFrame({'issue_date':issue_text,'shop':key[0],'spu':key[1],'model':name,'ds':series.index,'forecast':series.values}))
        for window,(a,b) in WINDOWS.items():
            dates=pd.date_range(issue+pd.Timedelta(days=a),issue+pd.Timedelta(days=b))
            target=d[d.ds.isin(dates)]
            for shop,g in target.groupby('shop'):
                ck=set(core.loc[core.shop.eq(shop),'spu'])
                coverage.append({'issue_date':issue_text,'shop':shop,'window':window,'fixed_core_n':len(ck),
                    'selection_total_units':float(universe.loc[universe.shop.eq(shop),'shop_selection_total'].iloc[0]),
                    'selection_core_units':float(core.loc[core.shop.eq(shop),'selection_qty'].sum()),
                    'target_observed_shop_units':float(g.y.sum()),'target_observed_core_units':float(g.loc[g.spu.isin(ck),'y'].sum()),
                    'target_shop_present_days':g.ds.nunique()})
    details=pd.DataFrame(all_scores);core_table=pd.concat(all_core,ignore_index=True)
    details['absolute_error']=(details.prediction-details.actual).abs()
    details['relative_error']=(details.prediction-details.actual)/details.actual.where(details.actual.gt(0))
    details['hit20']=details.relative_error.abs().le(.2)&details.relative_error.notna()
    details.to_parquet(out/'scores.parquet',index=False)
    details.to_csv(out/'scores.csv',index=False,encoding='utf-8-sig')
    core_table.to_csv(out/'core_universe.csv',index=False,encoding='utf-8-sig')
    core_table[core_table.is_core].to_csv(out/'core_lists.csv',index=False,encoding='utf-8-sig')
    pd.concat(forecast_parts,ignore_index=True).to_parquet(out/'daily_forecasts.parquet',index=False)
    pd.DataFrame(coverage).to_csv(out/'coverage.csv',index=False,encoding='utf-8-sig')
    metrics=[]
    for dims,label in [(['model','window'],'overall'),(['model','window','issue_date'],'issue'),(['model','window','shop'],'shop'),
                        (['model','window','lifecycle'],'lifecycle'),(['model','window','maturity'],'maturity'),(['model','window','category'],'category')]:
        for keys,g in details.groupby(dims,dropna=False):metrics.append({'slice':label,**dict(zip(dims,keys)),**metric(g)})
    pd.DataFrame(metrics).to_csv(out/'metrics.csv',index=False,encoding='utf-8-sig')
    # Secondary fair comparison only; never replace the fixed-list acceptance metrics.
    keys=['issue_date','shop','spu','window']
    available=details.pivot(index=keys,columns='model',values='prediction').notna().all(axis=1)
    common=details.merge(available.rename('common_output').reset_index(),on=keys,validate='many_to_one')
    common=common[common.common_output&common.actual.notna()]
    paired=[]
    for (model,window),g in common.groupby(['model','window']):
        paired.append({'model':model,'window':window,'scope':'SECONDARY_COMMON_OUTPUT_NOT_TARGET_ACCEPTANCE',**metric(g)})
    pd.DataFrame(paired).to_csv(out/'secondary_common_metrics.csv',index=False,encoding='utf-8-sig')
    details[details.relative_error.abs().gt(.2)|details.prediction.isna()|details.actual.isna()].to_csv(out/'failed_core_details.csv',index=False,encoding='utf-8-sig')
    dump(out/'fits.json',fits)
    manifest=json.loads((out/'run_manifest.json').read_text(encoding='utf-8'))
    manifest.update({'status':'COMPLETE','finished_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
      'files':{p.name:sha(p) for p in out.iterdir() if p.is_file() and p.name!='run_manifest.json'}})
    dump(out/'run_manifest.json',manifest)


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('prepare');a.add_argument('--archive',required=True);a.add_argument('--monthly-archive',required=True);a.add_argument('--out',required=True)
    a=sub.add_parser('run');a.add_argument('--inputs',required=True);a.add_argument('--config',required=True);a.add_argument('--out',required=True);a.add_argument('--allow-exogenous',action='store_true')
    args=vars(p.parse_args());command=args.pop('command')
    (prepare if command=='prepare' else run)(**args)
