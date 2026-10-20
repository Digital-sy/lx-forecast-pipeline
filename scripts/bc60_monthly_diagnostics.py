"""Descriptive monthly/operating diagnostics after scoring. Never fits or queries."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.bc60_monthly_score import metric, sha, dump, csv, KEY
except ModuleNotFoundError:
    from bc60_monthly_score import metric, sha, dump, csv, KEY


def run(root,out,current_metadata=None):
    root,out=Path(root),Path(out);out.mkdir(exist_ok=False)
    s=pd.read_parquet(root/'scoring/scores.parquet');s=s[s.cohort_id.eq('monthly_core')].copy()
    s['issue_date']=pd.to_datetime(s.issue_date).dt.strftime('%Y-%m-%d')
    s['current_category']='UNKNOWN';s['current_season']='UNKNOWN'
    if current_metadata:
        meta=pd.read_csv(current_metadata).fillna('UNKNOWN')
        if meta.duplicated(['shop','spu']).any():raise ValueError('METADATA_NOT_UNIQUE')
        cols=['shop','spu','current_category','current_season']
        s=s.drop(columns=['current_category','current_season']).merge(meta[cols],on=['shop','spu'],how='left',validate='many_to_one')
        s[['current_category','current_season']]=s[['current_category','current_season']].fillna('UNKNOWN')
    d=pd.read_parquet(root/'inputs/daily.parquet',columns=['shop','spu','ds','y','mapped','sales_state'])
    groups={(a,b):g.set_index('ds').sort_index() for (a,b),g in d.groupby(['shop','spu'])}
    hist=[]
    for r in s[KEY].drop_duplicates().itertuples(index=False):
        C=pd.Timestamp(r[1])-pd.Timedelta(days=2);g=groups.get((r[2],r[3]))
        z=g.reindex(pd.date_range(C-pd.Timedelta(days=364),C)) if g is not None else None
        known=(np.isfinite(z.y)&z.y.ge(0)&z.mapped.fillna(False)&z.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])) if z is not None else pd.Series([],dtype=bool)
        hist.append(dict(zip(KEY,r),known_history_365_days=int(known.sum()),complete_365_days=len(known)==365 and bool(known.all())))
    h=pd.DataFrame(hist);csv(out/'season_history_coverage.csv',h)
    s=s.merge(h,on=KEY,validate='many_to_one')
    slices=[]
    for field in ['current_category','current_season','maturity','lifecycle','complete_365_days']:
        for keys,g in s.groupby(['issue_month','model','window',field],dropna=False):
            slices.append(dict(zip(['issue_month','model','window','group_value'],keys),dimension=field,scope='DESCRIPTIVE_NOT_CAUSAL',**metric(g)))
    csv(out/'descriptive_slices.csv',pd.DataFrame(slices))
    # Survivor-selected adjacent and same-calendar-month comparisons are auxiliary.
    adjacent=[]
    for lag,label in [(1,'ADJACENT_ISSUES'),(12,'SAME_CALENDAR_MONTH_SURVIVORS')]:
        previous=s[['issue_month','shop','spu','model','window','prediction','actual','absolute_error','decision']].copy()
        previous['later_issue_month']=(pd.PeriodIndex(previous.issue_month,freq='M')+lag).astype(str)
        previous=previous.rename(columns={x:'previous_'+x for x in ['issue_month','prediction','actual','absolute_error','decision']})
        z=s.merge(previous,left_on=['issue_month','shop','spu','model','window'],right_on=['later_issue_month','shop','spu','model','window'],validate='one_to_one')
        z['comparison']=label;z['scope']='DESCRIPTIVE_SURVIVOR_SELECTED_NOT_CAUSAL'
        z['paired_known_truth']=z.actual.notna()&z.previous_actual.notna()
        z['paired_output']=z.prediction.notna()&z.previous_prediction.notna()
        adjacent.append(z)
    pd.concat(adjacent,ignore_index=True).to_parquet(out/'survivor_adjacent_yoy.parquet',index=False)
    m=pd.read_csv(root/'scoring/metrics.csv');m=m[m.cohort_id.eq('monthly_core')&m.slice.eq('issue')]
    extremes=[]
    for (model,window),g in m.groupby(['model','window']):
        for field in ['wape','hit_rate']:
            z=g[g[field].notna()]
            if z.empty:continue
            best=z[field].min() if field=='wape' else z[field].max();worst=z[field].max() if field=='wape' else z[field].min()
            for kind,value in [('BEST',best),('WORST',worst)]:
                for r in z[z[field].eq(value)].itertuples():
                    extremes.append(dict(model=model,window=window,criterion=field,kind=kind,issue_month=r.issue_month,value=value,core_n=r.core_n,scored_n=r.scored_n,hit_n=r.hit_n,scope='DESCRIPTIVE_EXTREMES_NOT_A_ROUTING_STRATEGY'))
    csv(out/'best_worst_months.csv',pd.DataFrame(extremes))
    # Same-source day/month reconciliation: no filling absent source months with 0.
    daily=d[d.mapped&d.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&np.isfinite(d.y)&d.y.ge(0)].copy()
    daily['month']=pd.to_datetime(daily.ds).dt.to_period('M').astype(str)
    monthly=daily.groupby(['shop','spu','month']).agg(daily_units=('y','sum'),known_days=('ds','nunique')).reset_index()
    monthly['expected_days']=pd.PeriodIndex(monthly.month,freq='M').days_in_month
    ref=pd.read_parquet(root/'inputs/monthly_reference.parquet');ref['month']=ref.month.astype(str)
    rec=monthly.merge(ref,on=['shop','spu','month'],how='outer',validate='one_to_one',indicator=True)
    rec['difference']=rec.daily_units-rec.qty
    rec['status']=np.select([rec._merge.eq('left_only'),rec._merge.eq('right_only'),rec.known_days.ne(rec.expected_days),rec.difference.ne(0)],['MONTHLY_REFERENCE_ABSENT','DAILY_ABSENT','DAILY_PARTIAL_MONTH','SOURCE_DIFFERENCE'],default='EXACT')
    csv(out/'daily_monthly_reconciliation.csv',rec)
    csv(out/'daily_monthly_reconciliation_summary.csv',rec.groupby(['month','status'],dropna=False).agg(
        objects=('spu','size'),known_daily_n=('daily_units','count'),known_monthly_n=('qty','count'),
        comparable_n=('difference','count'),daily_units=('daily_units',lambda v:v.sum(min_count=1)),
        monthly_units=('qty',lambda v:v.sum(min_count=1)),difference=('difference',lambda v:v.sum(min_count=1))).reset_index())
    # Ex-post operating signals are not pre-cutoff features or proof of decisions.
    oper=pd.read_csv(root/'scoring/operating_indicators_diagnostic.csv')
    bad=s[s.window.eq('d01_60')].merge(oper,on=KEY,validate='many_to_one')
    bad=bad.sort_values('absolute_error',ascending=False)
    csv(out/'top_error_operating_evidence.csv',bad.head(500))
    dump(out/'diagnostic_manifest.json',{'classification':'RECONSTRUCTED_DESCRIPTIVE_ONLY','source_sha256':sha(__file__),
        'metadata':str(current_metadata) if current_metadata else 'UNKNOWN','metadata_historical_effective_date':'UNKNOWN',
        'mapping_and_metadata_not_used_as_new_model_features':True,
        'daily_monthly_differences_do_not_change_frozen_truth':True,
        'files':{p.name:sha(p) for p in out.iterdir() if p.is_file()}})
    print(json.dumps({'scores':len(s),'reconciliation':len(rec),'extremes':len(extremes)}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);p.add_argument('--current-metadata');run(**vars(p.parse_args()))
