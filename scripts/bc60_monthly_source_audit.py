"""Offline source receipts and immutable-archive audit; no DB/model imports.

Raw timestamp checks are explicitly sampled. Source modification timestamps
never become synchronization-success timestamps or historical availability.
"""
from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import re

LIMITS = {'select_attempts':400, 'explain_attempts':450, 'metadata_selects':30,
          'cumulative_estimated_scan':240000000, 'executed_plan_max_scan':2000000,
          'rows_per_query':500000, 'new_raw_and_input_bytes':10*1024**3}


def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):digest.update(block)
    return digest.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def dump(path,value):
    Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')


def stamp(value):
    return dt.datetime.fromisoformat(value)


def receipt_group(rows):
    dates=[]
    for row in rows:
        matched=re.search(r'\bdt\s+IN\s*\(([^)]+)\)',row.get('request',{}).get('sql',''),re.I)
        if matched:dates.extend(re.findall(r"'(\d{4}-\d{2}-\d{2})'",matched.group(1)))
    count=Counter(dates)
    first,last=(min(count),max(count)) if count else (None,None)
    expected=(dt.date.fromisoformat(last)-dt.date.fromisoformat(first)).days+1 if count else 0
    return {'receipts':len(rows),'successful':sum(r.get('status')=='OK' for r in rows),
            'business_first':first,'business_last':last,'distinct_business_days':len(count),
            'calendar_missing_days':expected-len(count),'overlapping_partition_days':sum(n>1 for n in count.values()),
            'returned_aggregate_rows':sum(r.get('row_count',0) for r in rows),
            'estimated_scan':sum(r.get('estimated_scan',0) for r in rows),
            'max_estimated_scan':max((r.get('estimated_scan',0) for r in rows),default=0),
            'server_elapsed_seconds_sum':sum(r.get('elapsed_seconds',0) for r in rows),
            'server_elapsed_seconds_max':max((r.get('elapsed_seconds',0) for r in rows),default=0),
            'started_at':min((r['started_at'] for r in rows),default=None),
            'received_at':max((r['received_at'] for r in rows),default=None)}


def timestamp_sample(root,receipts):
    history=sorted((r for r in receipts if r['_name'].startswith('daily_missing_') and r.get('status')=='OK'),key=lambda r:r['_name'])
    chosen=[] if not history else [history[0],history[len(history)//2],history[-1]]
    chosen += [r for r in receipts if r['_name'].startswith('overlap_revision_') and r.get('status')=='OK']
    rows=[]
    for receipt in chosen:
        path=root/receipt['raw_path']
        with gzip.open(path,'rb') as stream:payload=stream.read()
        records=json.loads(payload)
        created=[str(r['min_created_at']) for r in records if r.get('min_created_at')]
        modified=[str(r['max_sync']) for r in records if r.get('max_sync')]
        rows.append({'receipt':receipt['_name'],'sampled_rows':len(records),
            'payload_sha256_matches':hashlib.sha256(payload).hexdigest()==receipt['rows_sha256'],
            'source_created_present':len(created),'source_created_min_in_sample':min(created) if created else None,
            'source_created_max_in_sample':max(created) if created else None,
            'source_modify_present':len(modified),'source_modify_max_in_sample':max(modified) if modified else None})
    minima=[r['source_created_min_in_sample'] for r in rows if r['source_created_min_in_sample']]
    return {'method':'first/middle/last historical partition plus overlap partition; not exhaustive timestamp scan',
            'partition_count':len(rows),'sampled_rows':sum(r['sampled_rows'] for r in rows),
            'earliest_source_created_in_sample':min(minima) if minima else None,'records':rows,
            'source_timestamp_timezone':'NOT_PROVEN_BY_ARCHIVE',
            'source_sync_success':'UNKNOWN',
            'max_sync_is':'MAX(modify_time) alias, not source synchronization-success timestamp',
            'historical_availability':'NOT_PROVEN; backfilled business dates and creation dates cannot establish past cutoff availability'}


def archive_hashes(v1,v2,v3):
    checks=[];sources={}
    def check(path,expected,manifest,role):
        path=Path(path);actual=sha(path) if path.is_file() else None
        checks.append({'path':str(path.resolve()),'role':role,'reference_manifest':str(manifest.resolve()),
                       'expected_sha256':expected,'actual_sha256':actual,'unchanged':actual==expected})
    manifest=v1/'inputs_v1_final/input_manifest.json';sources[str(manifest.resolve())]=sha(manifest)
    for name,digest in read_json(manifest)['files'].items():check(manifest.parent/name,digest,manifest,'V1_frozen_input')
    manifest=v3/'prepared/prepare_manifest.json';sources[str(manifest.resolve())]=sha(manifest)
    prep=read_json(manifest)
    # This manifest's references are already-reviewed V1 inputs/effects and V2
    # diagnostic files. Never traverse model directories or arbitrary archives.
    for name,digest in prep['sources'].items():
        path=Path(name)
        if not (path.is_relative_to(v1) or path.is_relative_to(v2)):
            raise ValueError('UNEXPECTED_V3_SOURCE_REFERENCE')
        if 'models' in path.parts or 'model' in path.parts:raise ValueError('MODEL_NOT_IN_AUDIT_SCOPE')
        check(path,digest,manifest,'V1_V2_inputs_referenced_by_V3')
    for name,digest in prep['files'].items():
        relative=Path(name)
        if relative.is_absolute() or '..' in relative.parts or 'model' in relative.parts or 'models' in relative.parts:
            raise ValueError('UNEXPECTED_PREPARED_FILE')
        check(manifest.parent/relative,digest,manifest,'V3_frozen_prepared_input')
    manifest=v2/'b_rolling/run_manifest.json';sources[str(manifest.resolve())]=sha(manifest)
    files=read_json(manifest)['files']
    for name in ['fixed_core.csv','t1_scores.parquet','t2_scores.parquet','t1_summary.csv','t2_summary.csv']:
        check(manifest.parent/name,files[name],manifest,'V2_reused_rolling_input')
    return {'all_unchanged':all(r['unchanged'] for r in checks),'checks':checks,'reference_manifests':sources,
            'scope':'explicit key-input manifests only; no model-file traversal'}


def run(root,v1,v2,v3,out):
    root,v1,v2,v3,out=[Path(p).resolve() for p in [root,v1,v2,v3,out]]
    if out.exists():raise ValueError('OUTPUT_EXISTS')
    quality_path=root/'inputs/quality.json';input_path=root/'inputs/input_manifest.json'
    quality=read_json(quality_path);input_manifest=read_json(input_path)
    if input_manifest.get('status')!='COMPLETE':raise ValueError('INPUTS_NOT_COMPLETE')
    paths=sorted((root/'evidence').glob('*.json'))
    receipts=[{**read_json(path),'_name':path.stem} for path in paths]
    selects=[r for r in receipts if r.get('mode')=='select']
    executed=[r for r in selects if r.get('status')=='OK' and 'raw_path' in r]
    probe=Path(__file__).with_name('bc60_readonly_probe.py')
    probe_digest=sha(probe)
    rejected=[]
    for r in selects:
        if r.get('estimated_scan',0)>LIMITS['executed_plan_max_scan']:
            proven=r.get('probe_sha256')==probe_digest and r.get('status')=='ERROR' and 'explain' in r and 'raw_path' not in r
            rejected.append({'receipt':r['_name'],'estimated_scan':r['estimated_scan'],
                'data_select_executed':False if proven else 'UNKNOWN',
                'evidence':'verified probe guard precedes data cursor.execute; receipt has error/explain and no raw result' if proven else 'INSUFFICIENT_GUARD_EVIDENCE'})
    history=[r for r in receipts if r['_name'].startswith('daily_missing_')]
    overlap=[r for r in receipts if r['_name'].startswith('overlap_revision_')]
    raw_paths=sorted((root/'raw').glob('*.json.gz'))
    input_files=[p for p in (root/'inputs').iterdir() if p.is_file()]
    raw_bytes=sum(p.stat().st_size for p in raw_paths)
    input_bytes=sum(p.stat().st_size for p in input_files)
    used={'select_attempts':len(selects),'explain_attempts':sum(r.get('mode') in ['select','plan'] for r in receipts),
          'metadata_selects':sum(3*len(r.get('metadata',{})) for r in receipts if r.get('mode')=='metadata'),
          'cumulative_estimated_scan':sum(r.get('estimated_scan',0) for r in selects),
          'executed_plan_max_scan':max((r.get('estimated_scan',0) for r in executed),default=0),
          'rows_per_query':max((r.get('row_count',0) for r in executed),default=0),
          'new_raw_and_input_bytes':raw_bytes+input_bytes}
    budget={'limits':LIMITS,'used':used,'within_frozen_limits':all(used[k]<=LIMITS[k] for k in LIMITS),
            'successful_data_selects':len(executed),'explain_receipts_present':sum('explain' in r for r in receipts),
            'metadata_count_basis':'three information_schema SELECTs per table in verified probe; five tables per completed metadata receipt',
            'executed_plan_estimated_scan':sum(r.get('estimated_scan',0) for r in executed),
            'estimated_scan_is_not_actual_rows_examined':True,'failed_plans_counted_in_total':True,
            'returned_rows':sum(r.get('row_count',0) for r in executed),
            'server_elapsed_seconds_sum':sum(r.get('elapsed_seconds',0) for r in receipts),
            'server_elapsed_seconds_max':max(r.get('elapsed_seconds',0) for r in receipts),
            'server_elapsed_basis':'combined per-request probe time, including EXPLAIN/metadata; not pure SELECT timing',
            'wall_seconds':(max(stamp(r['received_at']) for r in receipts)-min(stamp(r['started_at']) for r in receipts)).total_seconds(),
            'acquisition_first_utc':min(r['started_at'] for r in receipts),
            'acquisition_last_utc':max(r['received_at'] for r in receipts),
            'raw_gzip_bytes':raw_bytes,'normalized_input_bytes':input_bytes}
    inputs={'quality_gate':quality['gate'],'daily_rows':quality['daily_aggregate_rows'],
            'first_business_day':quality['first_day'],'last_business_day':quality['last_day'],
            'source_state_counts':quality['source_state_counts'],'missing_shop_date_count':len(quality['missing_shop_dates']),
            'numeric_normalization':quality.get('numeric_normalization',{}),'source_sync_success':quality.get('source_sync_success','UNKNOWN'),
            'monthly_reference':quality['monthly_reference'],'overlap_policy':quality['old_and_new_overlap'],
            'quality_sha256':sha(quality_path),'input_manifest_sha256':sha(input_path)}
    hashes=archive_hashes(v1,v2,v3)
    result={'status':'PASS' if budget['within_frozen_limits'] and hashes['all_unchanged'] else 'NEEDS_REVIEW',
            'created_at':dt.datetime.now(dt.timezone.utc).isoformat(),'audit_script_sha256':sha(__file__),
            'audit_added_queries':0,'audit_added_fits':0,'probe_sha256':probe_digest,'receipt_count':len(receipts),
            'receipt_statuses':dict(Counter(r.get('status') for r in receipts)),
            'query_budget':budget,'rejected_before_data_execution':rejected,
            'historical_chunks':receipt_group(history),'overlap_chunks':receipt_group(overlap),
            'sampled_source_timestamps':timestamp_sample(root,receipts),'normalized_inputs':inputs,
            'original_key_inputs':hashes,
            'receipt_hashes':{p.name:sha(p) for p in paths},'raw_compressed_hashes':{p.name:sha(p) for p in raw_paths}}
    if not all(r['payload_sha256_matches'] for r in result['sampled_source_timestamps']['records']):result['status']='NEEDS_REVIEW'
    out.mkdir(exist_ok=False);dump(out/'source_audit.json',result)
    h=result['historical_chunks'];o=result['overlap_chunks'];sample=result['sampled_source_timestamps']
    lines=['# 本地来源证据摘要','',f"审计状态：{result['status']}。本工具新增数据库查询 0 次、拟合 0 次；仅核对本地回执和归档。",'',
        f"- 查询尝试 {used['select_attempts']}/{LIMITS['select_attempts']}，EXPLAIN {used['explain_attempts']}/{LIMITS['explain_attempts']}，元数据 SELECT {used['metadata_selects']}/{LIMITS['metadata_selects']}。成功数据 SELECT {len(executed)} 次。",
        f"- 含被拒绝计划的累计估算扫描 {used['cumulative_estimated_scan']:,}/{LIMITS['cumulative_estimated_scan']:,}；实际执行查询的计划估算合计 {budget['executed_plan_estimated_scan']:,}，单次最大 {used['executed_plan_max_scan']:,}。这是优化器估算，不是实际扫描行数。",
        *[f"- {r['receipt']} 的计划估算 {r['estimated_scan']:,}；数据 SELECT 是否执行：{r['data_select_executed']}。源码哈希与先校验后执行顺序已留证，不能将此计划误报为已扫描。" for r in rejected],
        f"- 历史补齐 {h['receipts']} 个分片，{h['business_first']} 至 {h['business_last']}，{h['distinct_business_days']} 个业务日；分片日期缺口 {h['calendar_missing_days']}、重复日 {h['overlapping_partition_days']}。返回聚合行 {h['returned_aggregate_rows']:,}。",
        f"- 重叠修订核查 {o['receipts']} 个分片，{o['business_first']} 至 {o['business_last']}，返回 {o['returned_aggregate_rows']:,} 行；不覆盖原 V1 输入、不混入补历史输入。",
        f"- 服务端回执耗时合计 {budget['server_elapsed_seconds_sum']:.3f} 秒（含计划/元数据），单次最大 {budget['server_elapsed_seconds_max']:.3f} 秒；首末回执墙钟跨度 {budget['wall_seconds']:.3f} 秒。",
        f"- 原始压缩归档 {raw_bytes:,} 字节，标准化输入 {input_bytes:,} 字节，合计 {(raw_bytes+input_bytes)/1024**3:.4f} GiB。",
        f"- 标准化覆盖 {inputs['first_business_day']} 至 {inputs['last_business_day']}，{inputs['daily_rows']:,} 行；质量门禁 {inputs['quality_gate']}，缺店日 {inputs['missing_shop_date_count']}。",
        f"- 抽样 {sample['partition_count']} 个原始分片、{sample['sampled_rows']:,} 行，样本中最早源创建时间 {sample['earliest_source_created_in_sample']}；仅为抽样最早值，不声称全库最早。",
        '- 源同步成功时间为 UNKNOWN。max_sync 实为 MAX(modify_time) 字段别名，不是同步任务成功时间；本地抽取时间、源创建时间均不能证明历史截点当时已可见。',
        f"- V1/V2/V3 明确清单内关键输入 {len(hashes['checks'])} 项哈希核验：{'全部未变' if hashes['all_unchanged'] else '存在不匹配，见 JSON'}；未遍历或重算巨大模型。",'',
        '详细预算、各分片范围、样本来源、逐文件 SHA256 与原清单引用见 [source_audit.json](source_audit.json)。','']
    (out/'来源证据摘要.md').write_text('\n'.join(lines),encoding='utf-8')
    print(json.dumps({'status':result['status'],'receipt_count':len(receipts),'hash_checks':len(hashes['checks']),
                      'source_audit_sha256':sha(out/'source_audit.json')},ensure_ascii=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for arg in ['root','v1','v2','v3','out']:parser.add_argument('--'+arg,required=True)
    run(**vars(parser.parse_args()))
