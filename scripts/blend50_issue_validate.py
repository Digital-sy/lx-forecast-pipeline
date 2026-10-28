"""Independent file-only input/prediction checks; never fits or queries."""
import argparse,collections,datetime,gzip,hashlib,json,pathlib
import numpy as np
import pandas as pd

def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(pathlib.Path(p).read_text(encoding='utf8'))
def save(p,x):
    with pathlib.Path(p).open('x',encoding='utf8') as f:json.dump(x,f,ensure_ascii=False,indent=2,default=str)

def input_check(root):
    root=pathlib.Path(root).resolve();lock=read(root/'run_lock.json');m=read(root/'inputs/manifest.json')
    d=pd.read_parquet(root/'inputs/daily.parquet');old=pd.read_parquet(lock['cache_path'])
    assert sha(lock['cache_path'])==lock['cache_sha256'] and sha(root/'inputs/daily.parquet')==m['daily_sha256']
    keys=['ds','shop','spu'];left=old[old.ds.lt(lock['scope_start'])].sort_values(keys,na_position='first').reset_index(drop=True)
    right=d[d.ds.lt(lock['scope_start'])].sort_values(keys,na_position='first').reset_index(drop=True)
    pd.testing.assert_frame_equal(left,right,check_dtype=False)
    expected={};source_rows=0;units=0;rawkeys=set();times=[]
    for path in sorted((root/'source').glob('daily_*.receipt.json')):
        rec=read(path);blob=gzip.decompress((root/'source'/rec['raw_path']).read_bytes())
        assert hashlib.sha256(blob).hexdigest()==rec['payload_sha256'];data=json.loads(blob)
        times.append(rec['started_at'])
        for row in data['rows']:
            pk=tuple(row[n] for n in ['dt','store_name','country','asin']);assert pk not in rawkeys;rawkeys.add(pk)
            assert row['volume'] is not None and row['volume']>=0 and row['stat_date']==row['dt'] and row['delete_flag']==0
            key=(row['dt'],row['store_name'],row['spu']);v=expected.setdefault(key,[0,0]);v[0]+=1;v[1]+=row['volume']
            source_rows+=1;units+=row['volume']
    actual=d[d.ds.ge(lock['scope_start'])]
    assert len(actual)==len(expected)
    invalid_names={'','-','DEFAULT','无','None','null'}
    for r in actual.itertuples():
        sp=None if pd.isna(r.spu) else r.spu;key=(str(r.ds.date()),r.shop,sp)
        assert expected[key]==[r.source_rows,r.y]
        mapped=sp is not None and sp not in invalid_names and not any(c in sp for c in '|,，;；')
        assert r.mapped==mapped and r.sales_state==('EXPLICIT_ZERO' if r.y==0 else 'OBSERVED')
    evidence=read(root/'inputs/recent_completion_evidence.json')
    first_read=min(datetime.datetime.fromisoformat(t) for t in times)
    for e in evidence:
        assert e['state']=='READ_AND_SYNC_RECONCILED'
        sync=datetime.datetime.fromisoformat(e['completion']['sync_completed_at'])
        assert sync<first_read
    repairs=old.ds.isin(pd.to_datetime(['2026-10-10','2026-10-11']))
    pd.testing.assert_frame_equal(old.loc[repairs].sort_values(keys,na_position='first').reset_index(drop=True),
        d.loc[d.ds.isin(pd.to_datetime(['2026-10-10','2026-10-11']))].sort_values(keys,na_position='first').reset_index(drop=True),check_dtype=False)
    assert not d.duplicated(keys).any()
    result={'status':'PASS','recent_store_days':len(evidence),'source_rows_compared':source_rows,'source_units':units,
        'aggregated_groups_compared':len(expected),'outside_query_range_rows_compared':len(left),
        'seven_repairs_unchanged':True,'duplicate_keys':0,'completion_precedes_reads':True,
        'method':'Independent raw-record dictionary accumulation, not primary normalize/merge functions.',
        'daily_sha256':sha(root/'inputs/daily.parquet')}
    save(root/'inputs/independent_validation.json',result);print(json.dumps(result),flush=True)

def prediction_check(root):
    from lightgbm import Booster
    root=pathlib.Path(root).resolve();conf=read(root/'prepared/config.json')
    core=pd.read_parquet(root/'prepared/core.parquet');wide=pd.read_parquet(root/'issued/routing_predictions.parquet')
    d=pd.read_parquet(root/'inputs/daily.parquet',columns=['ds','shop','spu','y','mapped','sales_state'])
    c=pd.Timestamp(conf['cutoff']);s=pd.Timestamp(conf['issue']);assert c==s-pd.Timedelta(days=2)
    assert d.ds.max()==c and not wide.duplicated(['issue_date','shop','spu']).any()
    assert wide[['shop','spu']].equals(core[['shop','spu']]);checks=[]
    for prefix in ['direct','annual']:
        for seg in ['M1','M2']:
            slot=root/'effects'/(prefix+'_'+seg);r=read(slot/'result.json');prediction=pd.read_parquet(slot/'predictions.parquet')
            if not r['fit_attempted']:continue
            cp=read(slot/'checkpoint.json');assert sha(slot/'model.txt')==cp['model_sha256']
            inf=pd.read_parquet(root/'prepared'/seg/(prefix+'_inference.parquet'))
            good=inf.input_status.eq('OK');model=Booster(model_str=(slot/'model.txt').read_text(encoding='utf8'))
            assert model.feature_name()==cp['features']
            vals=model.predict(inf.loc[good,cp['features']],validate_features=True,num_threads=2)
            assert np.allclose(vals,prediction.loc[good,'prediction_raw'],rtol=1e-12,atol=1e-9)
            assert np.allclose(np.maximum(vals,0),wide.loc[good,prefix+'_'+seg],rtol=1e-12,atol=1e-9)
            train=pd.read_parquet(root/'prepared'/seg/(prefix+'_train.parquet'))
            assert train.label_end.le(c).all() and train.input_cutoff.eq(train.simulated_issue_date-pd.Timedelta(days=2)).all()
            assert train.label_start.eq(train.simulated_issue_date+pd.Timedelta(days=1 if seg=='M1' else 31)).all()
            assert train.label_end.eq(train.simulated_issue_date+pd.Timedelta(days=30 if seg=='M1' else 60)).all()
            # Independent label sums for every training key, using preserved known-state masks.
            by={k:g for k,g in d[d.mapped].groupby(['shop','spu'],sort=False)}
            label_count=0
            for key,t in train.groupby(['shop','spu'],sort=False):
                g=by[key].set_index('ds');grid=pd.date_range(g.index.min(),c);z=g.reindex(grid)
                known=z.y.notna()&z.sales_state.isin(['OBSERVED','EXPLICIT_ZERO'])&z.mapped.eq(True)
                sums=np.r_[0.,np.cumsum(np.where(known,z.y,0.))];missing=np.r_[0,np.cumsum(~known)]
                a=(pd.DatetimeIndex(t.label_start)-grid[0]).days.to_numpy();b=(pd.DatetimeIndex(t.label_end)-grid[0]).days.to_numpy()+1
                assert (a>=0).all() and (missing[b]==missing[a]).all()
                assert np.array_equal(sums[b]-sums[a],t.y_label.to_numpy());label_count+=len(t)
            checks.append({'slot':prefix+'_'+seg,'prediction_values_recomputed':int(good.sum()),'training_labels_independently_summed':label_count})
    for r in wide.itertuples():
        conditions=[r.direct_valid,r.input_valid,r.targets_valid,r.prior_positive,r.both_supported,r.both_annual]
        assert bool(r.gate_decision)==all(bool(v) for v in conditions)
        for seg in ['M1','M2']:
            direct=getattr(r,'direct_'+seg);annual=getattr(r,'annual_'+seg);got=getattr(r,'blend_'+seg)
            expected=direct+.5*(annual-direct) if r.gate_decision else direct
            assert (pd.isna(got) and pd.isna(expected)) or got==expected
        for prefix in ['direct','annual','blend','f28']:
            a=getattr(r,prefix+'_M1');b=getattr(r,prefix+'_M2');total=getattr(r,prefix+'_60')
            assert (pd.isna(total) and pd.isna(a+b)) or total==a+b
        hist=d[(d.shop==r.shop)&(d.spu==r.spu)&d.ds.between(c-pd.Timedelta(days=27),c)].set_index('ds').reindex(pd.date_range(c-pd.Timedelta(days=27),c))
        known=hist.y.notna().all() and hist.sales_state.isin(['OBSERVED','EXPLICIT_ZERO']).all() and hist.mapped.eq(True).all()
        expected=float(hist.y.mean()*30) if known else np.nan
        assert (pd.isna(r.f28_M1) and pd.isna(expected)) or r.f28_M1==expected
    results=[read(root/'effects'/slot/'result.json') for slot in ['direct_M1','direct_M2','annual_M1','annual_M2']]
    assert sum(r['fit_attempted'] for r in results)<=4
    out={'status':'PASS','checked_core_records':len(core),'model_checks':checks,'gate_formula_and_false_copy':'PASS',
        'all_60_day_values_equal_two_segments':True,'f28_independent_sum':'PASS','formal_fits':sum(r['fit_attempted'] for r in results),
        'future_truth_used':False,'current_accuracy_computed':False,'prediction_sha256':sha(root/'issued/predictions.parquet')}
    save(root/'issued/independent_validation.json',out);print(json.dumps(out),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('phase',choices=['input','prediction']);p.add_argument('--root',required=True);a=p.parse_args()
    (input_check if a.phase=='input' else prediction_check)(a.root)
