"""Offline audit of acquired receipts; never queries, fits, fills or issues."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
try:
    from scripts.blend50_prospective import read,sha,clock,dump_new,time_contract,verify_sources,SHOPS,DAILY
    from scripts.bc60_monthly_overlap_audit import compare
except ModuleNotFoundError:
    from blend50_prospective import read,sha,clock,dump_new,time_contract,verify_sources,SHOPS,DAILY
    from bc60_monthly_overlap_audit import compare

INVALID_SPU={'','-','DEFAULT','无','None','null'}
NUMERIC=['sales_qty','source_rows','null_qty','negative_qty','date_mismatch','stat_date_missing',
         'sku_count','asin_count','country_count','deleted_rows']

def normalize(records):
    d=pd.DataFrame(records)
    for col in NUMERIC:
        numeric=pd.to_numeric(d[col],errors='coerce')
        if (d[col].notna()&numeric.isna()).any():raise ValueError('NUMERIC_PARSE_FAILURE')
        d[col]=numeric
    d=d.rename(columns={'dt':'ds','store_name':'shop','sales_qty':'y'})
    d['ds']=pd.to_datetime(d.ds)
    if d.duplicated(['ds','shop','spu']).any():raise ValueError('DUPLICATE_GRAIN')
    d['mapped']=d.spu.notna()&~d.spu.isin(INVALID_SPU)&~d.spu.astype(str).str.contains('[|,，;；]',regex=True)
    unknown=d.null_qty.gt(0)|d.y.isna()
    invalid=d.negative_qty.gt(0)|d.date_mismatch.gt(0)|d.deleted_rows.gt(0)
    d['sales_state']=np.select([unknown,invalid,d.y.eq(0)],['UNKNOWN','INVALID','EXPLICIT_ZERO'],default='OBSERVED')
    return d

def audit(root,archive,issue,out):
    root,arc,out=map(lambda p:Path(p).resolve(),[root,archive,out]);out.mkdir(exist_ok=False)
    contract=time_contract(issue);cutoff=pd.Timestamp(contract['input_cutoff'])
    sources={};rows=[];receipts=[]
    oldpath=arc/'inputs/daily.parquet';oldhash=sha(oldpath)
    pm=read(arc/'prepared/prepare_manifest.json')
    if pm['sources'][str(oldpath)]!=oldhash:raise ValueError('ARCHIVED_INPUT_CHANGED')
    sources[str(oldpath)]=oldhash
    old=pd.read_parquet(oldpath,columns=DAILY+['source_rows'])
    oldlast=old.ds.max();overlap_start=oldlast-pd.Timedelta(days=6)
    for path in sorted((root/'source/receipts').glob('*.json')):
        rec=read(path);receipts.append(rec);sources[str(path)]=sha(path)
        if not path.stem.startswith('daily_'):continue
        if rec['status']!='OK' or rec.get('select_count')!=1:raise ValueError('SOURCE_ATTEMPT_FAILED')
        raw=root/rec['raw_path'];sources[str(raw)]=sha(raw)
        if sha(raw)!=rec['raw_sha256']:raise ValueError('RAW_FILE_TAMPERED')
        payload=gzip.decompress(raw.read_bytes())
        if hashlib.sha256(payload).hexdigest()!=rec['payload_sha256']:raise ValueError('RAW_PAYLOAD_TAMPERED')
        part=json.loads(payload)
        if len(part)!=rec['row_count']:raise ValueError('ROW_COUNT_MISMATCH')
        d=normalize(part);d['receipt']=path.stem;d['source_acquired_at']=rec['received_at']['utc']
        rows.append(d)
    d=pd.concat(rows,ignore_index=True)
    if d.duplicated(['ds','shop','spu']).any():raise ValueError('PARTITION_OVERLAP')
    if not d.ds.between(overlap_start,cutoff).all():raise ValueError('SOURCE_DATE_BOUNDARY')
    d.to_parquet(out/'new_daily_diagnostic.parquet',index=False)
    z=d.groupby(['ds','shop']).agg(spu_rows=('spu','size'),source_rows=('source_rows','sum'),units=('y','sum'),
       null_qty=('null_qty','sum'),negative_qty=('negative_qty','sum'),date_mismatch=('date_mismatch','sum'),
       deleted_rows=('deleted_rows','sum'),source_modify_min=('max_sync','min'),source_modify_max=('max_sync','max'))
    idx=pd.MultiIndex.from_product([pd.date_range(overlap_start,cutoff),SHOPS],names=['ds','shop'])
    coverage=z.reindex(idx).reset_index();coverage['state']=np.where(coverage.source_rows.isna(),'NO_SOURCE_ROWS','PRESENT_NOT_SYNC_PROOF')
    coverage.to_csv(out/'daily_shop_coverage.csv',index=False,encoding='utf-8-sig')
    missing=coverage[coverage.source_rows.isna()].copy();missing.to_csv(out/'missing_shop_days.csv',index=False,encoding='utf-8-sig')
    # This is descriptive evidence, not a threshold-based completeness decision.
    neighbors=[]
    for (shop,spu),g in d.groupby(['shop','spu'],dropna=False,sort=False):
        available=set(g.ds)
        for day in pd.date_range(overlap_start+pd.Timedelta(days=1),cutoff-pd.Timedelta(days=1)):
            if day not in available and day-pd.Timedelta(days=1) in available and day+pd.Timedelta(days=1) in available:
                before=g[g.ds.eq(day-pd.Timedelta(days=1))].iloc[0]
                after=g[g.ds.eq(day+pd.Timedelta(days=1))].iloc[0]
                neighbors.append({'shop':shop,'spu':spu,'missing_day':day,'previous_day_units':before.y,
                    'next_day_units':after.y,'meaning':'MISSING_RECORD_NOT_ZERO'})
    gaps=pd.DataFrame(neighbors);gaps.to_csv(out/'neighbor_confirmed_missing_objects.csv',index=False,encoding='utf-8-sig')
    left=old[old.ds.between(overlap_start,oldlast)][DAILY]
    right=d[d.ds.between(overlap_start,oldlast)][DAILY]
    overlap,differences=compare(left,right);differences.to_csv(out/'overlap_differences.csv',index=False,encoding='utf-8-sig')
    dump_new(out/'overlap_audit.json',overlap)
    # Combined ledger is diagnostic only. Never invent rows for missing dates.
    merged=pd.concat([old[old.ds.lt(overlap_start)][DAILY],d[DAILY]],ignore_index=True)
    if merged.duplicated(['ds','shop','spu']).any():raise ValueError('MERGE_DUPLICATION')
    expected_units=float(old.loc[old.ds.lt(overlap_start),'y'].sum()+d.y.sum())
    if float(merged.y.sum())!=expected_units:raise ValueError('UNIT_CONSERVATION')
    selection_start=cutoff-pd.Timedelta(days=89)
    recent=merged[merged.ds.between(selection_start,cutoff)]
    observed=recent.groupby('shop').agg(observed_days=('ds','nunique'),observed_units=('y','sum')).reset_index()
    observed['expected_days']=90;observed['core_frozen']=False
    observed.to_csv(out/'selection_input_diagnostic.csv',index=False,encoding='utf-8-sig')
    unknown=d[~d.mapped];unknown.to_csv(out/'unmapped_observations.csv',index=False,encoding='utf-8-sig')
    business=[r for r in receipts if r.get('select_count',0)>0]
    budget={'business_selects':sum(r.get('select_count',0) for r in receipts),
       'native_explain':sum(r.get('explain_count',0) for r in receipts),'metadata_selects':sum(r.get('metadata_count',0) for r in receipts),
       'estimated_scan_sum':sum(r['estimated_scan'] for r in business),'max_single_estimated_scan':max(r['estimated_scan'] for r in business),
       'business_seconds_sum':sum(t['seconds'] for r in receipts for t in r.get('timings',[]) if t['kind']=='select'),
       'max_business_seconds':max(t['seconds'] for r in receipts for t in r.get('timings',[]) if t['kind']=='select'),
       'failed_transport_attempts':sum(r['status']!='OK' for r in receipts)}
    verify_sources(sources)
    blockers=['SOURCE_SYNC_COMPLETION_NOT_PROVEN']
    if len(missing):blockers.append('MISSING_REQUIRED_SHOP_DAYS')
    if len(gaps):blockers.append('NEIGHBOR_OBJECT_GAPS_REQUIRE_RECONCILIATION')
    summary={'at':clock(),'status':'BLOCKED_SOURCE_OR_REPRODUCTION','reproduction':'PASS','current_fits':0,
       'current_predictions_generated':False,'prediction_sealed_at':None,'contract':contract,'core_count':None,
       'blockers':blockers,
       'shop_day_expected':len(idx),'shop_day_present':len(z),'shop_day_missing':missing[['ds','shop']].astype(str).to_dict('records'),
       'source_rows':int(d.source_rows.sum()),'normalized_rows':len(d),'observed_units':float(d.y.sum()),
       'null_qty':int(d.null_qty.sum()),'negative_qty':int(d.negative_qty.sum()),'date_mismatch':int(d.date_mismatch.sum()),
       'deleted_rows':int(d.deleted_rows.sum()),'explicit_zero_rows':int(d.sales_state.eq('EXPLICIT_ZERO').sum()),
       'unmapped_rows':len(unknown),'unmapped_units':float(unknown.y.sum()),'neighbor_missing_objects':len(gaps),
       'source_sync_success':'UNKNOWN','source_timestamp_timezone':'UNKNOWN',
       'source_modify_is_not_sync_receipt':True,'mapping_uniqueness_audit':'NOT_RUN_AFTER_HARD_SOURCE_GAP',
       'monthly_refresh':'NOT_RUN_AFTER_HARD_SOURCE_GAP','overlap_start':str(overlap_start.date()),'overlap_end':str(oldlast.date()),
       'overlap':overlap,'diagnostic_merge_unit_difference':float(merged.y.sum())-expected_units,
       'core_selection':'NOT_FROZEN_DUE_TO_INCOMPLETE_SOURCE','budget':budget,'sources_unchanged':True,'sources':sources}
    dump_new(out/'source_gate.json',summary)
    print(json.dumps({k:summary[k] for k in ['status','shop_day_missing','normalized_rows','unmapped_units','overlap','budget']},ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ['root','archive','issue','out']:p.add_argument('--'+n,required=True)
    audit(**vars(p.parse_args()))
