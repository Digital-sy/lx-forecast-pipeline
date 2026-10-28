"""Separate arithmetic/selection validation; never imports diagnostic aggregates."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import numpy as np
import pandas as pd

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''):h.update(b)
    return h.hexdigest()

def run(root,out):
    root,out=Path(root),Path(out);checks={};audit_sources={}
    def load(p,**kw):
        audit_sources[str(p.resolve())]=sha(p);return pd.read_parquet(p,**kw)
    m=json.loads((out/'manifest.json').read_text(encoding='utf-8'))
    for p,h in m['sources'].items():
        if sha(p)!=h:raise ValueError('SOURCE_MUTATED:'+p)
    checks['source_hashes_unchanged']=len(m['sources']);checks['shadow_files_unchanged']=m['shadow_protected_files']
    d=load(root/'bc60_monthly_replay_20261020/inputs/daily.parquet',columns=['ds','shop','spu','y','mapped','sales_state'])
    sample=pd.read_csv(out/'sample_audit_keys.csv');independent=[]
    for (shop,c),s in sample.groupby(['shop','input_cutoff']):
        c=pd.Timestamp(c);g=d[d.shop.eq(shop)&d.ds.between(c-pd.Timedelta(days=89),c)]
        known=g.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&g.y.notna()&g.y.ge(0)
        shop_ok=g.ds.nunique()==90 and known.all() and g.y.sum()>0
        rank=g[g.mapped.eq(True)].groupby('spu').y.sum().sort_values(ascending=False)
        target=float(g.y.sum()*.9);running=0.;boundary=None
        for _,v in rank.items():
            running+=v
            if running>=target:boundary=v;break
        keys=set(rank.index[rank.ge(boundary)]) if boundary is not None else set(rank.index)
        for r in s.itertuples():
            own=g[g.spu.eq(r.spu)]
            complete=shop_ok and len(own)==90 and own.ds.nunique()==90 and own.mapped.eq(True).all()
            state='UNKNOWN' if not complete else 'CORE' if r.spu in keys else 'NON_CORE'
            if state!=r.core_state:raise ValueError('INDEPENDENT_CORE:'+r.spu)
            independent.append(dict(shop=shop,input_cutoff=str(c.date()),spu=r.spu,expected=r.core_state,actual=state))
    pd.DataFrame(independent).to_csv(out/'independent_core_samples.csv',index=False,encoding='utf-8-sig')
    checks['independent_core_sample_rows']=len(independent)
    units=pd.read_csv(out/'training_units.csv');groups=pd.read_csv(out/'training_groups.csv');products=pd.read_csv(out/'product_support.csv')
    exclusions=[]
    for r in units.itertuples():
        slot=f'{r.issue_date}_{r.segment}';p=root/'direct_item_id_v1_20261028/prepared'/slot/'train.parquet'
        t=load(p,columns=['shop','spu','y_label','simulated_issue_date','label_start','label_end'])
        assert len(t)==r.rows and t.y_label.eq(0).sum()==r.zero_rows
        assert t[['shop','spu']].drop_duplicates().shape[0]==r.objects
        assert t.simulated_issue_date.nunique()==r.simulated_dates
        assert t.y_label.sum()==r.label_units
        assert t.label_end.le(pd.Timestamp(r.issue_date)-pd.Timedelta(days=2)).all()
        for _,g in groups[(groups.issue_date==r.issue_date)&(groups.segment==r.segment)].groupby('dimension'):
            assert g.rows.sum()==r.rows and g.zero_rows.sum()==r.zero_rows and g.label_units.sum()==r.label_units
            assert np.isclose(g.row_share.sum(),1) and np.isclose(g.label_share.sum(),1)
        ps=products[(products.issue_date==r.issue_date)&(products.segment==r.segment)]
        priorpath=root/'bc60_monthly_replay_20261020/effects/slots'/slot/'per_object_windows.csv'
        audit_sources[str(priorpath.resolve())]=sha(priorpath);prior=pd.read_csv(priorpath)
        z=ps.merge(prior,on=['issue_date','segment','shop','spu'],validate='one_to_one')
        assert len(z)==len(ps)==len(prior)
        assert (z.rows==z.train_rows).all() and (z.max_nonoverlap_windows==z.nonoverlap_windows).all()
        ep=priorpath.with_name('training_exclusions.csv');audit_sources[str(ep.resolve())]=sha(ep);e=pd.read_csv(ep)
        for reason,g in e.groupby('reason'):exclusions.append(dict(issue_date=r.issue_date,segment=r.segment,reason=reason,excluded_rows=int(g.rows.sum())))
    pd.DataFrame(exclusions).to_csv(out/'original_exclusion_summary.csv',index=False,encoding='utf-8-sig')
    checks['training_unit_reconciliation']=len(units);checks['archived_nonoverlap_product_rows']=len(products)
    # Directly pivot archived windows, independently count all-three hits with explicit loop.
    old=load(root/'core_model_complementarity_20261028_v1/historical_windows.parquet')
    new=load(root/'direct_item_id_v1_20261028/scoring/scores.parquet')
    s=pd.concat([old,new[new.model.isin(['DIRECT_TRAFFIC_STATE_V1','DIRECT_ITEM_ID_V1'])]])
    frozen=load(root/'core_model_complementarity_20261028_v1/objects.parquet')
    index=pd.MultiIndex.from_frame(frozen[['issue_date','shop','spu']])
    for col in ['paired','decline_group']:
        lookup=pd.Series(frozen[col].to_numpy(),index=index)
        s[col]=lookup.reindex(pd.MultiIndex.from_frame(s[['issue_date','shop','spu']])).to_numpy()
    assert s.paired.notna().all() and s.model.nunique()==8
    assert s[s.paired].groupby('model').size().eq(2579*3).all()
    joint=pd.read_csv(out/'model_joint.csv');metric=pd.read_csv(out/'model_metrics.csv');results=[]
    for scope in ['FULL','PAIRED','DECLINE_PAIRED']:
        z=s if scope=='FULL' else s[s.paired.eq(True)]
        if scope=='DECLINE_PAIRED':z=z[z.decline_group.eq('PRE_ISSUE_DECLINING')]
        for model,g in z.groupby('model'):
            p=g.pivot(index=['issue_date','shop','spu'],columns='window',values='prediction')
            y=g.pivot(index=['issue_date','shop','spu'],columns='window',values='actual')
            count=0;units_hit=0.
            for key in p.index:
                passed=True
                for w in ['d01_30','d31_60','d01_60']:
                    a=y.loc[key,w];v=p.loc[key,w]
                    if pd.isna(v) or pd.isna(a) or a<=0 or abs(v-a)>.2*a:passed=False
                if passed:count+=1;units_hit+=float(y.loc[key,'d01_60'])
            row=joint[(joint.scope==scope)&(joint.model==model)].iloc[0]
            assert count==row.joint_n and units_hit==row.joint_actual_units
            results.append(dict(scope=scope,model=model,joint_n=count,joint_actual_units=units_hit))
            for w in ['d01_30','d31_60','d01_60']:
                valid=p[w].notna()&y[w].notna();error=p.loc[valid,w]-y.loc[valid,w]
                row=metric[(metric.scope==scope)&(metric.model==model)&(metric.window==w)].iloc[0]
                assert np.isclose(abs(error).sum(),row.absolute_error,rtol=1e-14)
                assert np.isclose(error[error>0].sum(),row.over_units,rtol=1e-14)
                assert np.isclose(-error[error<0].sum(),row.under_units,rtol=1e-14)
                assert np.isclose(abs(error).sum()/y.loc[valid,w].sum(),row.wape,rtol=1e-14)
    assert len(results)==24
    pd.DataFrame(results).to_csv(out/'independent_joint_check.csv',index=False,encoding='utf-8-sig');checks['independent_model_scopes']=len(results)
    tr=pd.read_csv(out/'unseen_418_segment_causes.csv')
    assert len(tr)==836 and tr[['issue_date','shop','spu']].drop_duplicates().shape[0]==418
    assert (pd.to_datetime(tr.earliest_eligible_label_end)>pd.to_datetime(tr.actual_cutoff)).all()
    assert (tr.known_daily_history_days>0).all() and (tr.observed_positive_days>0).all() and tr.latest28_known.all()
    assert tr.maturity.eq('4_5').all() and tr.qualified_mature_rows.eq(0).all()
    checks['unseen_trace_checked']=len(tr)
    for p,h in {**m['sources'],**audit_sources}.items():assert sha(p)==h
    path=out/'independent_validation.json'
    if path.exists():raise ValueError('REFUSE_VALIDATION_OVERWRITE')
    path.write_text(json.dumps(dict(status='PASS',checks=checks,additional_sources=audit_sources,training=0,inference=0,
        script_sha256=sha(__file__)),ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(checks,ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args();run(a.root,a.out)
