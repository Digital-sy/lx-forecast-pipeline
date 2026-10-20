"""Frozen monthly replay scoring and descriptive diagnostics; no fitting or SQL."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd

WINDOWS={'d01_30':(1,30),'d31_60':(31,60),'d01_60':(1,60)}
VALID={'OBSERVED','EXPLICIT_ZERO'}
MODELS=['F28','WEEKDAY4','B1','B2','B3','DIRECT30x2']
KEY=['cohort_id','issue_date','shop','spu']


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def dump(p,x):Path(p).write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str,allow_nan=False),encoding='utf-8')
def csv(p,x):x.to_csv(p,index=False,encoding='utf-8-sig')


def decision(a,p):
    if not np.isfinite(a) or not np.isfinite(p) or a<=0:return 'UNJUDGED'
    return 'HIT' if abs(p-a)<=.2*a else 'MISS'


def metric(g):
    z=g[g.actual.notna()&g.prediction.notna()];e=z.prediction-z.actual
    total=float(z.actual.sum());known=float(g.actual.sum());hit=g.decision.eq('HIT')
    pos=int(g.actual.gt(0).sum());zero=z.actual.eq(0)
    return dict(core_n=len(g),output_n=int(g.prediction.notna().sum()),truth_n=int(g.actual.notna().sum()),
        missing_output_n=int(g.prediction.isna().sum()),missing_truth_n=int(g.actual.isna().sum()),
        unknown_truth_n=int((g.actual.isna() & g.truth_status.ne('NOT_MATURED')).sum()) if 'truth_status' in g else int(g.actual.isna().sum()),
        not_matured_n=int(g.truth_status.eq('NOT_MATURED').sum()) if 'truth_status' in g else 0,
        scored_n=len(z),positive_truth_n=pos,hit_n=int(hit.sum()),miss_n=int(g.decision.eq('MISS').sum()),
        unjudged_n=int(g.decision.eq('UNJUDGED').sum()),hit_rate=hit.sum()/pos if pos else np.nan,
        hit_fixed_share=hit.sum()/len(g) if len(g) else np.nan,
        hit_sales_share=g.loc[hit,'actual'].sum()/known if known>0 else np.nan,
        actual_units=total,known_core_actual_units=known,forecast_units=float(z.prediction.sum()),
        wape=e.abs().sum()/total if total>0 else np.nan,bias=e.sum()/total if total>0 else np.nan,
        over_units=float(e.clip(lower=0).sum()),under_units=float((-e).clip(lower=0).sum()),
        zero_actual_n=int(g.actual.eq(0).sum()),scored_zero_actual_n=int(zero.sum()),zero_absolute_error=float(e[zero].abs().sum()))


def window_truth(g,start,end,truth_end):
    if end>truth_end:return np.nan,'NOT_MATURED'
    q=g.reindex(pd.date_range(start,end)) if g is not None else None
    if q is None:return np.nan,'NO_SOURCE_OBJECT'
    ok=np.isfinite(q.y)&q.y.ge(0)&q.sales_state.isin(VALID)&q.mapped.fillna(False)
    if not ok.all():return np.nan,'MISSING_OR_INVALID_DAYS'
    return float(q.y.sum()),'OBSERVED'


def make_side_panels(out,monthly_archive,v2):
    info={}
    if monthly_archive:
        path=Path(monthly_archive);raw=json.loads(path.read_text(encoding='utf-8'))
        rows=raw.get('rows',raw.get('details',[])) if isinstance(raw,dict) else raw
        # Original evidence envelope has one level named data in some revisions.
        if not rows and isinstance(raw,dict) and isinstance(raw.get('data'),list):rows=raw['data']
        f=pd.DataFrame(rows)
        needed={'shop','spu','target_month','cutoff_month','horizon','actual','v4','a16'}
        if not needed.issubset(f.columns):
            info['monthly']={'status':'SCHEMA_BLOCKED','path':str(path),'sha256':sha(path),'columns':list(f.columns)}
        else:
            records=[]
            for model,col in [('V4_NATIVE','v4'),('FROZEN_A16_NATIVE','a16')]:
                z=f.copy();z['model']=model;z['prediction']=pd.to_numeric(z[col],errors='coerce')
                z['actual']=pd.to_numeric(z.actual,errors='coerce')
                z['decision']=[decision(a,p) for a,p in zip(z.actual,z.prediction)]
                for keys,g in z.groupby(['target_month','cutoff_month','horizon'],dropna=False):
                    records.append(dict(zip(['target_month','cutoff_month','horizon'],keys),model=model,
                        scope='NATIVE_MONTHLY_OLD_ELIGIBLE_NOT_DAILY_CORE',**metric(g)))
            csv(out/'native_monthly_side_panel.csv',pd.DataFrame(records))
            info['monthly']={'status':'REUSED_ONLY_RECOMPUTED_HIT20','path':str(path),'sha256':sha(path),
                'rows':len(f),'targets':sorted(f.target_month.astype(str).unique()),'not_ranked_with_60day':True}
    if v2:
        info['rolling']={}
        for name in ('t1_scores.parquet','t2_scores.parquet'):
            p=Path(v2)/'b_rolling'/name
            if p.exists():
                z=pd.read_parquet(p)
                info['rolling'][name]={'status':'REUSED_ARCHIVE_REFERENCE_ONLY','path':str(p),
                    'sha256':sha(p),'rows':len(z),'columns':list(z.columns),
                    'scope':'FIXED_ENDPOINT_REMAINDER' if name.startswith('t1') else 'UPDATED_NEW_WINDOW',
                    'not_in_initial_issue_ranking':True}
    dump(out/'side_panel_provenance.json',info)


def run(inputs,prepared,effects,out,monthly_archive=None,v2=None):
    inputs,prepared,effects,out=map(Path,(inputs,prepared,effects,out))
    out.mkdir(exist_ok=False);started=dt.datetime.now(dt.timezone.utc).isoformat()
    d=pd.read_parquet(inputs/'daily.parquet');d['ds']=pd.to_datetime(d.ds)
    if d.duplicated(['shop','spu','ds']).any():raise ValueError('DUPLICATE_SOURCE')
    truth_end=pd.Timestamp('2026-10-01')
    maps={k:g.set_index('ds').sort_index() for k,g in d.groupby(['shop','spu'],dropna=False)}
    p=pd.read_parquet(effects/'window_predictions.parquet')
    p['issue_date']=pd.to_datetime(p.issue_date).dt.strftime('%Y-%m-%d')
    if p.duplicated(KEY+['model','window']).any():raise ValueError('DUPLICATE_PREDICTIONS')
    if (p.prediction.notna()&(~np.isfinite(p.prediction)|p.prediction.lt(0))).any():raise ValueError('INVALID_PREDICTION')
    actual=[]
    for row in p[KEY+['window']].drop_duplicates().itertuples(index=False):
        cohort,issue,shop,spu,w=row;S=pd.Timestamp(issue);a,b=WINDOWS[w]
        value,state=window_truth(maps.get((shop,spu)),S+pd.Timedelta(days=a),S+pd.Timedelta(days=b),truth_end)
        actual.append(dict(zip(KEY,[cohort,issue,shop,spu]),window=w,actual=value,truth_status=state))
    scores=p.merge(pd.DataFrame(actual),on=KEY+['window'],validate='many_to_one')
    core=pd.concat([pd.read_parquet(prepared/'core.parquet'),pd.read_csv(prepared/'original_core_panel.csv')],ignore_index=True)
    core['issue_date']=pd.to_datetime(core.issue_date).dt.strftime('%Y-%m-%d')
    extras=['lifecycle','maturity','category','selection_qty']
    meta=core[KEY+[x for x in extras if x in core]].drop_duplicates(KEY)
    for x in extras:
        if x in scores:scores=scores.drop(columns=x)
    scores=scores.merge(meta,on=KEY,how='left',validate='many_to_one')
    for x in ['lifecycle','maturity','category']:
        if x not in scores:scores[x]='UNKNOWN'
        scores[x]=scores[x].fillna('UNKNOWN')
    scores['issue_month']=scores.issue_date.str[:7]
    scores['decision']=[decision(a,p) for a,p in zip(scores.actual,scores.prediction)]
    scores['absolute_error']=(scores.prediction-scores.actual).abs()
    scores['relative_error']=(scores.prediction-scores.actual)/scores.actual.where(scores.actual.gt(0))
    scores.to_parquet(out/'scores.parquet',index=False);csv(out/'scores.csv',scores)
    metrics=[]
    for dims,label in [(['cohort_id','model','window'],'overall_overlapping_issue_records'),
            (['cohort_id','issue_month','model','window'],'issue'),
            (['cohort_id','issue_month','shop','model','window'],'shop_issue'),
            (['cohort_id','issue_month','maturity','model','window'],'maturity_issue'),
            (['cohort_id','issue_month','lifecycle','model','window'],'qualification_issue'),
            (['cohort_id','issue_month','category','model','window'],'category_issue')]:
        for keys,g in scores.groupby(dims,dropna=False):metrics.append({'slice':label,**dict(zip(dims,keys)),**metric(g)})
    mature=scores[pd.to_datetime(scores.target_end).le(truth_end)]
    for keys,g in mature.groupby(['cohort_id','model','window'],dropna=False):
        metrics.append({'slice':'overall_mature_overlapping_issue_records',**dict(zip(['cohort_id','model','window'],keys)),**metric(g)})
    csv(out/'metrics.csv',pd.DataFrame(metrics))
    joint=[]
    for keys,g in scores.groupby(KEY+['model'],dropna=False):
        state='HIT' if len(g)==3 and g.decision.eq('HIT').all() else ('MISS' if len(g)==3 and g.decision.ne('UNJUDGED').all() else 'UNJUDGED')
        joint.append(dict(zip(KEY+['model'],keys),decision=state,output_windows=int(g.prediction.notna().sum()),truth_windows=int(g.actual.notna().sum())))
    j=pd.DataFrame(joint);csv(out/'joint_hits.csv',j)
    csv(out/'joint_summary.csv',j.groupby(['cohort_id','issue_date','model','decision']).size().reset_index(name='records'))
    baseline=scores[scores.model.eq('F28')][KEY+['window','prediction','decision']].rename(columns={'prediction':'baseline_prediction','decision':'baseline_decision'})
    pairs=scores[~scores.model.eq('F28')].merge(baseline,on=KEY+['window'],validate='many_to_one')
    pairs['paired']=pairs.actual.notna()&pairs.prediction.notna()&pairs.baseline_prediction.notna()
    pairs['error_change']=pairs.absolute_error-(pairs.baseline_prediction-pairs.actual).abs()
    pairs['pair_state']=np.select([~pairs.paired,pairs.error_change.lt(0),pairs.error_change.gt(0)],['UNJUDGED','IMPROVED','DEGRADED'],default='UNCHANGED')
    pairs.to_parquet(out/'paired_details.parquet',index=False)
    pm=[]
    for dims,label in [(['cohort_id','model','window'],'overall'),(['cohort_id','issue_month','model','window'],'issue'),(['cohort_id','issue_month','shop','model','window'],'shop_issue')]:
        for keys,g in pairs.groupby(dims,dropna=False):
            z=g[g.paired];base=z.copy();base['prediction']=base.baseline_prediction;base['decision']=base.baseline_decision
            a=metric(z);b=metric(base);den=float(z.actual.sum())
            rec={'slice':label,**dict(zip(dims,keys)),'full_core_n':len(g),'paired_n':len(z)}
            rec.update({'candidate_'+k:v for k,v in a.items()});rec.update({'f28_'+k:v for k,v in b.items()})
            rec['wape_change']=a['wape']-b['wape']
            for state in ('IMPROVED','DEGRADED','UNCHANGED','UNJUDGED'):
                hit=g.pair_state.eq(state);rec[state.lower()+'_n']=int(hit.sum())
                rec[state.lower()+'_sales_weight']=g.loc[hit&g.paired,'actual'].sum()/den if den>0 else np.nan
            pm.append(rec)
    csv(out/'paired_f28.csv',pd.DataFrame(pm))
    shared=scores.pivot(index=KEY+['window'],columns='model',values='prediction').reindex(columns=MODELS).notna().all(axis=1)
    common=scores.merge(shared.rename('all_model_output').reset_index(),on=KEY+['window'])
    common=common[common.all_model_output&common.actual.notna()];cm=[]
    for keys,g in common.groupby(['cohort_id','issue_month','model','window']):
        cm.append(dict(zip(['cohort_id','issue_month','model','window'],keys),scope='AUXILIARY_ALL_MODEL_COMMON',**metric(g)))
    csv(out/'all_model_common.csv',pd.DataFrame(cm) if cm else pd.DataFrame(columns=['cohort_id','issue_month','model','window','scope',*metric(scores.iloc[:0]).keys()]))
    jb=j[j.model.eq('F28')].drop(columns='model').rename(columns={'decision':'baseline_decision','output_windows':'baseline_outputs','truth_windows':'baseline_truth'})
    jm=j[j.model.ne('F28')].merge(jb,on=KEY,validate='many_to_one')
    jm['migration']=np.select([jm.decision.eq('UNJUDGED')|jm.baseline_decision.eq('UNJUDGED'),jm.decision.eq('HIT')&jm.baseline_decision.eq('HIT'),jm.decision.eq('HIT'),jm.baseline_decision.eq('HIT')],['UNJUDGED','RETAINED_HIT','NEW_HIT','EXITED_HIT'],default='BOTH_MISS')
    csv(out/'joint_migrations.csv',jm)
    shapes=[];oper=[]
    actual_lookup={(r.cohort_id,r.issue_date,r.shop,r.spu,r.window):r.actual for r in pd.DataFrame(actual).itertuples()}
    for row in p[KEY].drop_duplicates().itertuples(index=False):
        cohort,issue,shop,spu=row;S=pd.Timestamp(issue);C=S-pd.Timedelta(days=2);g=maps.get((shop,spu))
        dates=pd.date_range(C-pd.Timedelta(days=27),C);recent=g.reindex(dates) if g is not None else pd.DataFrame()
        known=len(recent)==28 and np.isfinite(recent.y).all() and recent.y.ge(0).all() and recent.sales_state.isin(VALID).all() and recent.mapped.fillna(False).all()
        mean=float(recent.y.mean()) if known else np.nan
        a1=actual_lookup[(*row,'d01_30')];a2=actual_lookup[(*row,'d31_60')]
        positive=np.isfinite(a1) and np.isfinite(a2) and min(a1,a2)>0
        q=dict(zip(KEY,row),sum28=mean*28,mean28=mean,p1=mean*30,p2=mean*30,p60=mean*60,a1=a1,a2=a2,
            shape_ratio=max(a1,a2)/min(a1,a2) if positive else np.nan,
            equal_segments_cannot_both_hit=bool(2*max(a1,a2)>3*min(a1,a2)) if positive else None,
            shape_status='POSITIVE_BOTH' if positive else ('UNKNOWN_ACTUAL' if not(np.isfinite(a1) and np.isfinite(a2)) else 'ZERO_ACTUAL'),
            first_level_ratio=a1/30/mean if mean>0 and np.isfinite(a1) else np.nan,
            second_level_ratio=a2/30/mean if mean>0 and np.isfinite(a2) else np.nan)
        shapes.append(q)
        if g is not None:
            future=g.reindex(pd.date_range(S+pd.Timedelta(days=1),S+pd.Timedelta(days=60)))
            o=dict(zip(KEY,row),evidence_grade='INDICATOR_ONLY_NOT_CAUSAL',historical_effective_operations_log='UNKNOWN',
                actual_budget='UNKNOWN',confirmed_stockout_interval='UNKNOWN',historical_category='UNKNOWN')
            for col in ['listing_sessions_sum','order_qty','ad_spend','ad_clicks','ad_orders','sales_amt','sku_count','promotion_units']:
                x=recent[col] if col in recent else pd.Series(dtype=float);y=future[col] if col in future else pd.Series(dtype=float)
                before=float(x.mean()) if len(x)==28 and x.notna().all() else np.nan
                after=float(y.mean()) if len(y)==60 and y.notna().all() else np.nan
                o[col+'_input_daily_mean']=before;o[col+'_target_daily_mean']=after
                o[col+'_ratio']=after/before if before>0 and np.isfinite(after) else np.nan
            for prefix,frame in [('input',recent),('target',future)]:
                expected=28 if prefix=='input' else 60
                def total(field):
                    v=frame[field] if field in frame else pd.Series(dtype=float)
                    return float(v.sum()) if len(v)==expected and np.isfinite(v).all() and v.ge(0).all() else np.nan
                def ratio(numerator,denominator):
                    n=total(numerator);den=total(denominator)
                    return n/den if np.isfinite(n) and den>0 else np.nan
                o[prefix+'_order_session_ratio']=ratio('order_qty','listing_sessions_sum')
                o[prefix+'_ad_cost_sales_ratio']=ratio('ad_spend','ad_sales_amt')
                o[prefix+'_revenue_per_sold_unit_proxy']=ratio('sales_amt','y')
                o[prefix+'_volume_weighted_price']=ratio('volume_weighted_price_numerator','priced_units')
                o[prefix+'_zero_inventory_row_observations']=total('zero_inventory_observation_rows')
            o['price_definition']='weighted available source prices; revenue/unit is only a proxy, neither proves a historical price decision'
            o['inventory_definition']='zero row observations, not confirmed stockout duration; unknown historical coverage retained'
            oper.append(o)
    shape=pd.DataFrame(shapes);csv(out/'f28_shape_details.csv',shape);csv(out/'operating_indicators_diagnostic.csv',pd.DataFrame(oper))
    ss=[]
    for keys,g in shape.groupby(['cohort_id','issue_date']):
        eligible=g.shape_status.eq('POSITIVE_BOTH');flag=g.equal_segments_cannot_both_hit.fillna(False).astype(bool)
        den=float((g.a1+g.a2).sum());posden=float((g.loc[eligible,'a1']+g.loc[eligible,'a2']).sum())
        bad=float((g.loc[flag,'a1']+g.loc[flag,'a2']).sum())
        ss.append(dict(zip(['cohort_id','issue_date'],keys),core_n=len(g),positive_both_n=int(eligible.sum()),
            incompatible_shape_n=int(flag.sum()),object_share=flag.sum()/eligible.sum() if eligible.any() else np.nan,
            sales_share_known_core=bad/den if den else np.nan,sales_share_positive_both=bad/posden if posden else np.nan,
            zero_or_unknown_n=int((~eligible).sum())))
    csv(out/'f28_shape_summary.csv',pd.DataFrame(ss))
    coverage=[]
    for (cohort,issue,shop),g in p[KEY].drop_duplicates().groupby(['cohort_id','issue_date','shop']):
        for w,(a,b) in WINDOWS.items():
            start=pd.Timestamp(issue)+pd.Timedelta(days=a);end=pd.Timestamp(issue)+pd.Timedelta(days=b)
            z=d[d.shop.eq(shop)&d.ds.between(start,end)];valid=z.sales_state.isin(VALID)&z.y.ge(0)&np.isfinite(z.y)
            denom=float(z.loc[valid,'y'].sum());num=float(z.loc[valid&z.mapped&z.spu.isin(g.spu),'y'].sum())
            complete_values=[actual_lookup[(cohort,issue,shop,spu,w)] for spu in g.spu]
            complete_units=float(np.nansum(complete_values))
            state='NOT_MATURED' if end>truth_end else ('PARTIAL_SOURCE' if z.ds.nunique()!=b-a+1 or not valid.all() else 'OBSERVED_SOURCE_COVERAGE')
            coverage.append(dict(cohort_id=cohort,issue_date=issue,shop=shop,window=w,core_n=len(g),status=state,
                observed_shop_units=denom,observed_core_units=num,complete_window_core_units=complete_units,
                complete_window_core_n=int(np.isfinite(complete_values).sum()),
                coverage=complete_units/denom if denom>0 and state=='OBSERVED_SOURCE_COVERAGE' else np.nan,
                observed_partial_coverage=num/denom if denom>0 and state!='NOT_MATURED' else np.nan,
                unmapped_units=float(z.loc[valid&~z.mapped,'y'].sum()),present_days=z.ds.nunique(),expected_days=b-a+1))
    csv(out/'future_coverage.csv',pd.DataFrame(coverage))
    issuegrid=pd.read_csv(prepared/'issue_grid.csv');quality=pd.read_csv(prepared/'selection_quality.csv')
    fits=pd.read_csv(effects/'fit_status.csv');gridrows=[]
    grouped={k:g for k,g in scores[scores.cohort_id.eq('monthly_core')].groupby(['issue_date','shop','model','window'])}
    for issue in issuegrid.itertuples():
        for shop in ['JQ-US','MT-US','RKZ-US','SY-US']:
            qr=quality[quality.issue_date.eq(issue.issue_date)&quality.shop.eq(shop)].iloc[0]
            for model in MODELS:
                for window,(a,b) in WINDOWS.items():
                    key=(issue.issue_date,shop,model,window);g=grouped.get(key)
                    kinds=['M1'] if model=='DIRECT30x2' and window=='d01_30' else ['M2'] if model=='DIRECT30x2' and window=='d31_60' else ['M1','M2'] if model=='DIRECT30x2' else [model]
                    fs=fits[fits.issue_date.eq(issue.issue_date)&fits.kind.isin(kinds)]
                    rec=dict(issue_month=issue.issue_month,issue_date=issue.issue_date,input_cutoff=issue.input_cutoff,shop=shop,model=model,window=window,
                        target_start=str((pd.Timestamp(issue.issue_date)+pd.Timedelta(days=a)).date()),target_end=str((pd.Timestamp(issue.issue_date)+pd.Timedelta(days=b)).date()),
                        selection_status=qr.selection_status,selection_core_n=qr.core_n,
                        fit_status='BASELINE_NO_FIT' if model in ['F28','WEEKDAY4'] else '|'.join(sorted(fs.status.unique())))
                    if g is not None:rec.update(metric(g))
                    else:rec.update({k:np.nan for k in metric(scores.iloc[:0])});rec['core_n']=qr.core_n
                    rec['cell_status']='NOT_MATURED' if pd.Timestamp(rec['target_end'])>truth_end else qr.selection_status if g is None else 'HAS_SCORES' if (g.prediction.notna() & g.actual.notna()).any() else 'NO_PAIRED_OUTPUT_OR_TRUTH'
                    gridrows.append(rec)
    csv(out/'full_issue_status_grid.csv',pd.DataFrame(gridrows))
    make_side_panels(out,monthly_archive,v2)
    used=[inputs/'daily.parquet',inputs/'monthly_reference.parquet',prepared/'core.parquet',effects/'window_predictions.parquet']
    dump(out/'score_manifest.json',{'started_at':started,'finished_at':dt.datetime.now(dt.timezone.utc).isoformat(),
         'source_sha256':sha(__file__),'sources':{str(x.resolve()):sha(x) for x in used},
         'classification':'RECONSTRUCTED_HISTORICAL_REPLAY','fits':0,
         'files':{x.name:sha(x) for x in out.iterdir() if x.is_file()}})
    print(json.dumps({'scores':len(scores),'joint_records':len(j),'pairs':len(pairs),'shape_records':len(shape)}))


if __name__=='__main__':
    ap=argparse.ArgumentParser()
    for x in ('inputs','prepared','effects','out'):ap.add_argument('--'+x,required=True)
    ap.add_argument('--monthly-archive');ap.add_argument('--v2');run(**vars(ap.parse_args()))
