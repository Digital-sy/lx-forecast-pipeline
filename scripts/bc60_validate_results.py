"""Independent reconciliation of fixed-list scores; reads local archives only."""
import argparse,hashlib,json,pathlib,datetime
import numpy as np
import pandas as pd


def validate(root):
    p=pathlib.Path(root).resolve();out=p/'validation_v1_checked';out.mkdir(exist_ok=False)
    d=pd.read_parquet(p/'inputs_v1_final/daily.parquet')
    f=pd.read_parquet(p/'effects_v1/daily_forecasts.parquet');f['ds']=pd.to_datetime(f.ds)
    scores=pd.read_parquet(p/'effects_v1/scores.parquet')
    core=pd.read_csv(p/'effects_v1/core_lists.csv');universe=pd.read_csv(p/'effects_v1/pre_effect_universe.csv')
    keys=['issue_date','shop','spu','window','model']
    assert not scores.duplicated(keys).any()
    assert len(core)==len(scores[['issue_date','shop','spu']].drop_duplicates())
    assert hashlib.sha256((p/'effects_v1/pre_effect_universe.csv').read_bytes()).hexdigest()==hashlib.sha256((p/'effects_v1/core_universe.csv').read_bytes()).hexdigest()
    # Verify exact input hashes again, without trusting recorded metrics.
    gate=json.loads((p/'pre_effect_gate.json').read_text(encoding='utf-8'))
    for name,h in gate['files'].items():
        actual=p.parent/name
        assert hashlib.sha256(actual.read_bytes()).hexdigest()==h,name
    truth=d.set_index(['shop','spu','ds']).y
    preds=f.set_index(['issue_date','shop','spu','model','ds']).forecast
    max_true_diff=0.;max_prediction_diff=0.
    windows={'d01_30':(1,30),'d31_60':(31,60),'d01_60':(1,60)}
    for row in scores.itertuples():
        a,b=windows[row.window];issue=pd.Timestamp(row.issue_date)
        dates=pd.date_range(issue+pd.Timedelta(days=a),issue+pd.Timedelta(days=b))
        tv=truth.reindex(pd.MultiIndex.from_tuples([(row.shop,row.spu,t) for t in dates]))
        if pd.notna(row.actual):
            assert tv.notna().all();max_true_diff=max(max_true_diff,abs(float(tv.sum())-row.actual))
        if pd.notna(row.prediction):
            pv=preds.reindex(pd.MultiIndex.from_tuples([(row.issue_date,row.shop,row.spu,row.model,t) for t in dates]))
            assert pv.notna().all();max_prediction_diff=max(max_prediction_diff,abs(float(pv.sum())-row.prediction))
    assert max_true_diff<1e-8 and max_prediction_diff<1e-6
    independent=[]
    for (model,window),g in scores.groupby(['model','window']):
        z=g[g.actual.notna()&g.prediction.notna()];e=z.prediction-z.actual;t=z.actual.sum()
        hit=z.actual.gt(0)&e.abs().le(z.actual*.2)
        independent.append({'model':model,'window':window,'wape':float(e.abs().sum()/t),'bias':float(e.sum()/t),
            'hit_n':int(hit.sum()),'hit_rate':float(hit.sum()/g.actual.gt(0).sum()),
            'hit_sales_share':float(z.loc[hit,'actual'].sum()/g.actual.sum()),'scored_n':len(z)})
    independent=pd.DataFrame(independent)
    original=pd.read_csv(p/'effects_v1/metrics.csv').query("slice=='overall'")
    compare=independent.merge(original,on=['model','window'],suffixes=('_check','_original'))
    errors={k:float((compare[k+'_check']-compare[k+'_original']).abs().max()) for k in ['wape','bias','hit_n','hit_rate','hit_sales_share','scored_n']}
    assert max(errors.values())<1e-10
    # All three windows must hit; a cumulative hit is not sufficient.
    joint=scores.assign(ok=scores.actual.gt(0)&scores.relative_error.abs().le(.2)).groupby(['model','issue_date','shop','spu']).ok.all().reset_index()
    joint.to_csv(out/'all_three_windows_hits.csv',index=False,encoding='utf-8-sig')
    joint_summary=joint.groupby('model').agg(core_issue_objects=('ok','size'),all_three_hit=('ok','sum')).reset_index()
    joint_summary.to_csv(out/'all_three_windows_summary.csv',index=False,encoding='utf-8-sig')
    # Descriptive current category is never fed to training or selection.
    e=json.loads((p/'evidence/dwd_reconcile_dates.json').read_text(encoding='utf-8'))['rows']
    cat=pd.DataFrame(e);cat=cat[cat.category_count.eq(1)&cat.current_category.notna()&cat.current_category.ne('')]
    cg=cat.groupby(['store_name','spu']).current_category.agg(lambda x:sorted(set(x)))
    category={(s,k):v[0] for (s,k),v in cg.items() if len(v)==1}
    scores['current_category_not_pit']=[category.get((s,k),'UNKNOWN') for s,k in zip(scores.shop,scores.spu)]
    category_rows=[]
    for (model,window,group),g in scores.groupby(['model','window','current_category_not_pit']):
        z=g.dropna(subset=['actual','prediction']);total=z.actual.sum();error=z.prediction-z.actual
        category_rows.append({'model':model,'window':window,'current_category_not_pit':group,'core_n':len(g),'scored_n':len(z),
            'wape':float(error.abs().sum()/total) if total else None,'bias':float(error.sum()/total) if total else None})
    pd.DataFrame(category_rows).to_csv(out/'current_category_descriptive.csv',index=False,encoding='utf-8-sig')
    # Fair incremental comparisons with identical issue/product/window truth.
    pv=scores.pivot(index=['issue_date','shop','spu','window'],columns='model',values='prediction')
    actual=scores.drop_duplicates(['issue_date','shop','spu','window']).set_index(['issue_date','shop','spu','window']).actual
    inc=[];risk=[]
    for before,after in [('rolling28','B1'),('B1','B2'),('B2','B3')]:
        z=pv[[before,after]].join(actual).dropna()
        for window,g in z.groupby(level='window'):
            e0=g[before]-g.actual;e1=g[after]-g.actual;change=e1.abs()-e0.abs();total=g.actual.sum()
            inc.append({'before':before,'after':after,'window':window,'common_n':len(g),
                'delta_wape':float(change.sum()/total),'delta_over_units':float(e1.clip(lower=0).sum()-e0.clip(lower=0).sum()),
                'delta_under_units':float((-e1).clip(lower=0).sum()-(-e0).clip(lower=0).sum()),
                'improved_n':int(change.lt(-1e-8).sum()),'degraded_n':int(change.gt(1e-8).sum()),
                'unchanged_n':int(change.abs().le(1e-8).sum()),'improved_sales_weight':float(g.loc[change.lt(-1e-8),'actual'].sum()/total),
                'degraded_sales_weight':float(g.loc[change.gt(1e-8),'actual'].sum()/total)})
        t=z.copy();t['extra_absolute_error']=(t[after]-t.actual).abs()-(t[before]-t.actual).abs();t['comparison']=before+'->'+after
        risk.append(t.reset_index())
    pd.DataFrame(inc).to_csv(out/'increments.csv',index=False,encoding='utf-8-sig')
    pd.concat(risk,ignore_index=True).to_csv(out/'increment_product_details.csv',index=False,encoding='utf-8-sig')
    # Source conflicts remain annotations, not silent changes to the fixed scoring set.
    monthly=pd.read_csv(p/'monthly_comparison.csv')
    conflict=monthly[monthly.difference.notna()&monthly.difference.ne(0)]
    conflict[['shop','spu','month','difference']].to_csv(out/'monthly_truth_conflicts.csv',index=False,encoding='utf-8-sig')
    common=pd.read_csv(p/'effects_v1/secondary_common_metrics.csv')
    core_coverage=pd.read_csv(p/'effects_v1/coverage.csv')
    core_coverage['selection_coverage']=core_coverage.selection_core_units/core_coverage.selection_total_units
    core_coverage['target_coverage']=core_coverage.target_observed_core_units/core_coverage.target_observed_shop_units
    assert core_coverage.selection_coverage.ge(.9-1e-12).all()
    core_coverage.to_csv(out/'coverage_with_rates.csv',index=False,encoding='utf-8-sig')
    check={'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'PASS_FOR_CALCULATION_RECONCILIATION',
       'unique_core_shop_spu':len(core[['shop','spu']].drop_duplicates()),'core_issue_objects':len(core),
       'truth_max_difference':max_true_diff,'forecast_window_max_difference':max_prediction_diff,
       'metric_max_differences':errors,'all_core_lists_frozen_before_fit':True,
       'gate_input_hashes_unchanged':True,'core_list_bytes_unchanged':True,
       'joint_hit_counts':joint_summary.to_dict('records'),
       'new_or_unknown_core_issue_objects':int(core.lifecycle.ne('ESTABLISHED').sum()),
       'no_independent_oos_claim':True,'business_target':'FAIL','production_changed':False}
    (out/'validation.json').write_text(json.dumps(check,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(check,ensure_ascii=False,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');validate(p.parse_args().root)
