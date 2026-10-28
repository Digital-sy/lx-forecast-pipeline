"""Reconcile bounded raw ODS receipts into a new immutable prospective input."""
import argparse,datetime,gzip,hashlib,json,pathlib
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
try:
    from scripts.blend50_prospective import dump_new,sha,SHOPS
    from scripts.blend50_source_audit import normalize
except ModuleNotFoundError:
    from blend50_prospective import dump_new,sha,SHOPS
    from blend50_source_audit import normalize

PK=['dt','store_name','country','asin']
KEY=['ds','shop','spu']
CHECK=['y','mapped','sales_state','source_rows']

def read(p):return json.loads(pathlib.Path(p).read_text(encoding='utf8'))

def normalized(raw,receipt,acquired):
    if raw.duplicated(PK).any():raise ValueError('DUPLICATE_ASIN_KEY')
    records=[]
    for (day,shop,spu),g in raw.groupby(['dt','store_name','spu'],dropna=False,sort=True):
        records.append(dict(dt=day,store_name=shop,spu=spu,sales_qty=g.volume.sum(min_count=1),source_rows=len(g),
            null_qty=int(g.volume.isna().sum()),negative_qty=int(g.volume.lt(0).sum()),
            date_mismatch=int((g.stat_date.notna()&g.stat_date.ne(g.dt)).sum()),stat_date_missing=int(g.stat_date.isna().sum()),
            sku_count=g.sku.nunique(),asin_count=g.asin.nunique(),country_count=g.country.nunique(),
            deleted_rows=int(g.delete_flag.ne(0).sum()),min_created_at=g.create_time.min(),max_sync=g.modify_time.max()))
    result=normalize(records);result['receipt']=receipt;result['source_acquired_at']=acquired
    return result

def completion(logs,day,shop):
    # The newest refresh covering a day supersedes old completion evidence.
    files=[f for f in logs['files'] if any(a<=day<=b for a,b in f['windows'])]
    if not files:return None
    file=files[0];events=[e for e in logs['events'] if e['path']==file['path']]
    if not file['complete'] or not any('全部完成' in e['markers'] for e in events):return None
    ev=[e for e in events if e['shops']==[shop] and e['dates'][-2:]==[day,day]]
    starts=[e['line'] for e in ev if '开始抓取' in e['markers']]
    if not starts:return None
    ev=[e for e in ev if e['line']>=max(starts)]
    if any('分片店铺抓取失败' in e['markers'] for e in ev):return None
    done=[e for e in ev if '分片店铺抓取并写库完成' in e['markers']]
    if len(done)!=1:return None
    done=done[0];pages=[e for e in ev if '当前页已写入数据库' in e['markers'] and e['line']<done['line']]
    count=0
    for page in pages:
        n=page['numbers']
        if int(n.get('offset',-1))!=count or int(n.get('page_rows',-1))<0:return None
        count+=int(n['page_rows'])
    if not pages or count!=int(done['numbers']['rows']):return None
    return {'date':day,'shop':shop,'task':pathlib.Path(file['path']).stem,'log_path':file['path'],
        'log_sha256':file['sha256'],'completion_line':done['line'],'page_lines':[e['line'] for e in pages],
        'page_count':len(pages),'rows':count,'sync_completed_at':done['times'][-1]+'+08:00',
        'basis':'NORMAL_SYNC_FULL_RUN_AND_CONTIGUOUS_PAGE_WRITES; not independent API truth'}

def compare_overlap(old,new):
    a=old[KEY+CHECK].copy();b=new[KEY+CHECK].copy()
    if a.duplicated(KEY).any() or b.duplicated(KEY).any():raise ValueError('DUPLICATE_OVERLAP')
    q=a.merge(b,on=KEY,how='outer',suffixes=('_old','_new'),indicator=True,validate='one_to_one')
    changed=q['_merge'].ne('both')
    for c in CHECK:
        same=q[c+'_old'].eq(q[c+'_new'])|(q[c+'_old'].isna()&q[c+'_new'].isna())
        q['changed_'+c]=~same;changed|=~same
    return q.loc[changed].copy(),{'old_groups':len(a),'new_groups':len(b),
        'old_units':float(a.y.sum()),'new_units':float(b.y.sum()),'changed_or_unpaired':int(changed.sum())}

def build(root,workspace):
    root,workspace=pathlib.Path(root).resolve(),pathlib.Path(workspace).resolve();out=root/'inputs';out.mkdir(exist_ok=False)
    lock=read(root/'run_lock.json');oldpath=pathlib.Path(lock['cache_path'])
    if sha(oldpath)!=lock['cache_sha256']:raise ValueError('REPAIRED_CACHE_CHANGED')
    before=read(root/'source/logs_before.receipt.json')['logs'];after=read(root/'source/logs_after.receipt.json')['logs']
    if before['active_known_pids'] or after['active_known_pids']:raise ValueError('KNOWN_COLLECTOR_ACTIVE')
    if [(f['path'],f['sha256']) for f in before['files']]!=[(f['path'],f['sha256']) for f in after['files']]:raise ValueError('SYNC_CHANGED_DURING_READ')
    old=pd.read_parquet(oldpath);parts=[];rawparts=[];receipts=[]
    for p in sorted((root/'source').glob('daily_*.receipt.json')):
        r=read(p);assert r['status']=='OK' and r['mode']=='data'
        compressed=root/'source'/r['raw_path'];assert sha(compressed)==r['raw_sha256']
        blob=gzip.decompress(compressed.read_bytes());assert hashlib.sha256(blob).hexdigest()==r['payload_sha256']
        raw=pd.DataFrame(json.loads(blob)['rows']);assert len(raw)==r['row_count']
        for col in ['volume','delete_flag']:raw[col]=pd.to_numeric(raw[col],errors='raise')
        assert raw.dt.ge(lock['scope_start']).all() and raw.dt.lt(lock['scope_end_exclusive']).all()
        assert raw.store_name.isin(SHOPS).all()
        raw.to_parquet(out/(p.stem+'.raw.parquet'),index=False)
        parts.append(normalized(raw,p.name,r['finished_at']));rawparts.append(raw);receipts.append(r)
    raw=pd.concat(rawparts,ignore_index=True);new=pd.concat(parts,ignore_index=True)
    assert not raw.duplicated(PK).any() and not new.duplicated(KEY).any()
    assert raw.country.eq('美国').all()
    expected_sid={'JQ-US':'11548','MT-US':'11545','RKZ-US':'11550','SY-US':'11547'}
    assert all(str(s)==expected_sid[shop] for s,shop in zip(raw.sid,raw.store_name))
    quantity_invalid=raw.volume.isna()|raw.volume.lt(0)|raw.volume.mod(1).ne(0)
    date_invalid=raw.stat_date.isna()|raw.stat_date.ne(raw.dt)
    blockers=[]
    if quantity_invalid.any():blockers.append('UNKNOWN_OR_INVALID_QUANTITY')
    if date_invalid.any() or raw.delete_flag.ne(0).any():blockers.append('DATE_OR_DELETE_ANOMALY')
    grouped=raw.groupby(['dt','store_name']).agg(rows=('asin','size'),units=('volume',lambda s:s.sum(min_count=1)))
    evidence=[]
    for day in pd.date_range(lock['missing_start'],lock['input_cutoff']):
        for shop in SHOPS:
            key=(str(day.date()),shop);e=completion(before,*key)
            row=grouped.loc[key] if key in grouped.index else None
            if row is None:state='PRODUCTION_NO_RECORDS';blockers.append(state+':'+str(key))
            elif e is None:state='SYNC_COMPLETION_UNKNOWN';blockers.append(state+':'+str(key))
            elif int(row.rows)!=e['rows']:state='SYNC_COUNT_MISMATCH';blockers.append(state+':'+str(key))
            else:state='READ_AND_SYNC_RECONCILED'
            evidence.append({'date':key[0],'shop':shop,'state':state,'source_rows':int(row.rows) if row is not None else None,
                'source_units':float(row.units) if row is not None else None,'completion':e})
    dump_new(out/'recent_completion_evidence.json',evidence)
    overlap=old.ds.between(lock['scope_start'],lock['overlap_end'])
    diffs,ov=compare_overlap(old.loc[overlap],new[new.ds.le(lock['overlap_end'])])
    diffs.to_csv(out/'overlap_revisions.csv',index=False,encoding='utf-8-sig');dump_new(out/'overlap_audit.json',ov)
    # Explicitly replace the bounded overlap, retaining differences; no last-row dedupe.
    keep=old[old.ds.lt(lock['scope_start'])].copy()
    merged=pd.concat([keep,new],ignore_index=True).sort_values(KEY,na_position='first').reset_index(drop=True)
    assert not merged.duplicated(KEY).any() and merged.ds.max()==pd.Timestamp(lock['input_cutoff'])
    assert float(new.y.sum())==float(raw.volume.sum())
    assert float(merged.y.sum())==float(keep.y.sum()+raw.volume.sum())
    repair_mask=old.ds.isin(pd.to_datetime(['2026-10-10','2026-10-11']))
    pd.testing.assert_frame_equal(old.loc[repair_mask].sort_values(KEY,na_position='first').reset_index(drop=True),
        merged.loc[merged.ds.isin(pd.to_datetime(['2026-10-10','2026-10-11']))].sort_values(KEY,na_position='first').reset_index(drop=True),check_dtype=False)
    merged.to_parquet(out/'daily.parquet',index=False)
    raw_stats=grouped.reset_index();local=new.groupby(['ds','shop']).agg(source_rows=('source_rows','sum'),units=('y',lambda s:s.sum(min_count=1))).reset_index()
    local['dt']=local.ds.dt.strftime('%Y-%m-%d');local['store_name']=local.shop
    match=raw_stats.merge(local,on=['dt','store_name'],suffixes=('_ods','_local'),validate='one_to_one')
    assert (match.rows==match.source_rows).all() and (match.units_ods==match.units_local).all()
    match.to_csv(out/'store_day_conservation.csv',index=False,encoding='utf-8-sig')
    new.loc[~new.mapped].to_csv(out/'mapping_unknown.csv',index=False,encoding='utf-8-sig')
    cov=merged[merged.ds.ge(pd.Timestamp(lock['input_cutoff'])-pd.Timedelta(days=89))].groupby(['ds','shop']).size()
    idx=pd.MultiIndex.from_product([pd.date_range(pd.Timestamp(lock['input_cutoff'])-pd.Timedelta(days=89),lock['input_cutoff']),SHOPS])
    missing=[(str(d.date()),s) for d,s in idx if (d,s) not in cov.index]
    if missing:blockers.append('MISSING_SELECTION_SHOP_DAYS')
    monthly=workspace/'bc60_monthly_replay_20261020/inputs/monthly_reference.parquet'
    manifest={'status':'INPUT_INCOMPLETE' if blockers else 'INPUT_READY_NOT_ISSUED','blockers':blockers,
        'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'issue_date':lock['issue_date'],'cutoff':lock['input_cutoff'],
        'daily_sha256':sha(out/'daily.parquet'),'daily_rows':len(merged),'base_cache_sha256':sha(oldpath),
        'monthly_path':str(monthly),'monthly_sha256':sha(monthly),'recent_shop_days':len(evidence),
        'recent_completed':sum(e['state']=='READ_AND_SYNC_RECONCILED' for e in evidence),'raw_rows':len(raw),
        'raw_units':float(raw.volume.sum()),'unknown_mapping_groups':int((~new.mapped).sum()),
        'unknown_mapping_units':float(new.loc[~new.mapped,'y'].sum()),'source_rows_conserved':True,
        'overlap':ov,'seven_repaired_slices_preserved':True,'selection_missing_days':missing,
        'inherited_limitations':['Historical acquisition was reconstructed, not point-in-time proof.',
            'Accepted MT platform/API differences remain unresolved; no renewed platform-export requirement.',
            'Recent normal-sync logs and contiguous writes are operational completeness evidence, not independent upstream truth.',
            'Historical monthly qualification keeps the original archive limitations; current category/season unused.',
            'Other unobserved writers cannot be excluded solely by process inspection.'],
        'source_receipts':{str(p.relative_to(root)):sha(p) for p in (root/'source').glob('*.receipt.json')},
        'business_selects':len(receipts),'estimated_scan_sum':sum(r['estimated_scan'] for r in receipts),
        'select_seconds':sum(r['select_seconds'] for r in receipts),'max_select_seconds':max(r['select_seconds'] for r in receipts),
        'production_writes':0,'api_calls':0,'fits':0}
    dump_new(out/'manifest.json',manifest)
    print(json.dumps({k:manifest[k] for k in ['status','blockers','recent_shop_days','recent_completed','raw_rows','raw_units','overlap','unknown_mapping_units']},ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--workspace',required=True);a=p.parse_args();build(a.root,a.workspace)
