"""Versioned, read-only actuals collection for one immutable shadow issue.

No connection or production configuration is imported by this module.  The
default transport sends the reviewed bounded probe through the existing SSH
path.  A real server clock is read before any data request.  Imported callers
may inject clocks/transports only for offline tests; the CLI has no date override.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

try:
    from scripts.blend50_recent_probe import FIELDS, SHOPS, SOURCE, plans
except ModuleNotFoundError:
    from blend50_recent_probe import FIELDS, SHOPS, SOURCE, plans

# Pure functions below preserve the reviewed conversion/completion semantics.
# Embedding avoids importing the unrelated historical model dependency chain.
ADAPTER_BASELINES = {
    'scripts/blend50_recent_input.py': '23d989ea57a995dd61414983ee3e69e627fd9ec3e3c348cfb2652b243d2e9b8f',
    'scripts/blend50_source_audit.py': 'b10a4ab1fbc694154454a5f6e46d64b9812e2a8fc7d726f1af07c6fd1e2b53a3',
    'scripts/blend50_recent_probe.py': '3282b2ccb2f7822fb00286d3339901c5e543f3a6a9064cc31868731ee13f658a',
}
INVALID_SPU = {'', '-', 'DEFAULT', '无', 'None', 'null'}
NUMERIC = ['sales_qty', 'source_rows', 'null_qty', 'negative_qty', 'date_mismatch',
           'stat_date_missing', 'sku_count', 'asin_count', 'country_count', 'deleted_rows']


def normalize(records):
    d = pd.DataFrame(records)
    for col in NUMERIC:
        numeric = pd.to_numeric(d[col], errors='coerce')
        if (d[col].notna() & numeric.isna()).any():
            raise ValueError('NUMERIC_PARSE_FAILURE')
        d[col] = numeric
    d = d.rename(columns={'dt': 'ds', 'store_name': 'shop', 'sales_qty': 'y'})
    d['ds'] = pd.to_datetime(d.ds)
    if d.duplicated(['ds', 'shop', 'spu']).any():
        raise ValueError('DUPLICATE_GRAIN')
    d['mapped'] = d.spu.notna() & ~d.spu.isin(INVALID_SPU) & ~d.spu.astype(str).str.contains('[|,，;；]', regex=True)
    unknown = d.null_qty.gt(0) | d.y.isna()
    invalid = d.negative_qty.gt(0) | d.date_mismatch.gt(0) | d.deleted_rows.gt(0)
    d['sales_state'] = np.select([unknown, invalid, d.y.eq(0)], ['UNKNOWN', 'INVALID', 'EXPLICIT_ZERO'], default='OBSERVED')
    return d


def normalized(raw, receipt, acquired):
    if raw.duplicated(PK).any():
        raise ValueError('DUPLICATE_ASIN_KEY')
    records = []
    for (day, shop, spu), g in raw.groupby(['dt', 'store_name', 'spu'], dropna=False, sort=True):
        records.append(dict(dt=day, store_name=shop, spu=spu, sales_qty=g.volume.sum(min_count=1), source_rows=len(g),
                            null_qty=int(g.volume.isna().sum()), negative_qty=int(g.volume.lt(0).sum()),
                            date_mismatch=int((g.stat_date.notna() & g.stat_date.ne(g.dt)).sum()), stat_date_missing=int(g.stat_date.isna().sum()),
                            sku_count=g.sku.nunique(), asin_count=g.asin.nunique(), country_count=g.country.nunique(),
                            deleted_rows=int(g.delete_flag.ne(0).sum()), min_created_at=g.create_time.min(), max_sync=g.modify_time.max()))
    result = normalize(records)
    result['receipt'] = receipt
    result['source_acquired_at'] = acquired
    return result


def completion(logs, day, shop):
    files = [f for f in logs['files'] if any(a <= day <= b for a, b in f['windows'])]
    if not files:
        return None
    file = files[0]
    events = [e for e in logs['events'] if e['path'] == file['path']]
    if not file['complete'] or not any('全部完成' in e['markers'] for e in events):
        return None
    ev = [e for e in events if e['shops'] == [shop] and e['dates'][-2:] == [day, day]]
    starts = [e['line'] for e in ev if '开始抓取' in e['markers']]
    if not starts:
        return None
    ev = [e for e in ev if e['line'] >= max(starts)]
    if any('分片店铺抓取失败' in e['markers'] for e in ev):
        return None
    done = [e for e in ev if '分片店铺抓取并写库完成' in e['markers']]
    if len(done) != 1:
        return None
    done = done[0]
    pages = [e for e in ev if '当前页已写入数据库' in e['markers'] and e['line'] < done['line']]
    count = 0
    for page in pages:
        n = page['numbers']
        if int(n.get('offset', -1)) != count or int(n.get('page_rows', -1)) < 0:
            return None
        count += int(n['page_rows'])
    if not pages or count != int(done['numbers']['rows']):
        return None
    return {'date': day, 'shop': shop, 'task': Path(file['path']).stem, 'log_path': file['path'],
            'log_sha256': file['sha256'], 'completion_line': done['line'], 'page_lines': [e['line'] for e in pages],
            'page_count': len(pages), 'rows': count, 'sync_completed_at': done['times'][-1] + '+08:00',
            'basis': 'NORMAL_SYNC_FULL_RUN_AND_CONTIGUOUS_PAGE_WRITES; not independent API truth'}

PK = ['dt', 'store_name', 'country', 'asin']
CORE_KEY = ['ds', 'shop', 'spu']
SSH = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
       '-o', 'StrictHostKeyChecking=yes', 'root@8.138.102.103']
REMOTE = 'cd /opt/apps/pythondata && PYTHONDONTWRITEBYTECODE=1 venv-ml/bin/python -B -'
DEFAULT_BUDGET = dict(business_selects=30, total_scan=10000000,
                      per_query_scan=1000000, per_query_timeout_ms=10000)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_new(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False,
                  default=lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x))


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def server_clock():
    """Read only the server clock; never imports project code or credentials."""
    code = b'import datetime,json\nprint(json.dumps({"server_now":datetime.datetime.now(datetime.timezone.utc).isoformat()}))\n'
    result = subprocess.run(SSH + [REMOTE], input=code, capture_output=True,
                            timeout=30, check=False)
    if result.returncode:
        raise ValueError('SERVER_CLOCK_TRANSPORT_FAILED')
    value = read_clock(json.loads(result.stdout)['server_now'])
    return value


def read_clock(value):
    value = dt.datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError('AWARE_SERVER_CLOCK_REQUIRED')
    return value


def issue_contract(run_dir):
    run = Path(run_dir).resolve()
    seal_path = run / 'issued/issue_seal.json'
    seal = read(seal_path)
    if seal.get('status') != 'ISSUED_SHADOW_AWAITING_ACTUALS':
        raise ValueError('NOT_SEALED_SHADOW')
    core_path = run / 'prepared/core.parquet'
    files = {k.replace('\\', '/'): v for k, v in seal['files'].items()}
    if sha(core_path) != files.get('prepared/core.parquet'):
        raise ValueError('CORE_CHANGED')
    core = pd.read_parquet(core_path, columns=['shop', 'spu'])
    if core.duplicated(['shop', 'spu']).any() or not core.shop.isin(SHOPS).all() or core.spu.isna().any():
        raise ValueError('CORE_KEY_INVALID')
    start, end = map(dt.date.fromisoformat, seal['windows']['W60'])
    if (end-start).days != 59:
        raise ValueError('TARGET_WINDOW_CHANGED')
    return seal, core, {'run_id': seal['run_id'], 'issue_seal_sha256': sha(seal_path),
                        'forecast_input_cutoff': seal['input_cutoff'],
                        'core_sha256': sha(core_path), 'target_start': str(start),
                        'target_end': str(end)}


def plan(contract, now, previous_end=None):
    now = read_clock(now)
    today = now.astimezone(ZoneInfo('Asia/Shanghai')).date()
    available = today - dt.timedelta(days=2)
    start = dt.date.fromisoformat(contract['target_start'])
    final = dt.date.fromisoformat(contract['target_end'])
    if available < start:
        return dict(status='WAITING_TARGET_START', server_now=now.isoformat(),
                    server_business_date=str(today), source_available_through=str(available),
                    chunks=[], collection_start=None, collection_end=None)
    end = min(available, final)
    # One-day boundary overlap is compared explicitly; never last-row deduped.
    begin = max(start, dt.date.fromisoformat(previous_end)) if previous_end else start
    if begin > end:
        raise ValueError('PREVIOUS_VERSION_FROM_FUTURE')
    chunks = []
    day = begin
    while day <= end:
        after = min(day + dt.timedelta(days=2), end + dt.timedelta(days=1))
        chunks.append([str(day), str(after)])
        day = after
    return dict(status='READ_PLANNED', server_now=now.isoformat(), server_business_date=str(today),
                source_available_through=str(available), collection_start=str(begin),
                collection_end=str(end), chunks=chunks)


class SSHProbeTransport:
    def __init__(self):
        self.probe = Path(__file__).with_name('blend50_recent_probe.py').read_bytes()
        self.probe_sha256 = hashlib.sha256(self.probe).hexdigest()

    def __call__(self, request):
        result = subprocess.run(SSH + [REMOTE], input=self.probe + b'\nmain(' +
                                repr(request).encode('utf-8') + b')\n',
                                capture_output=True, timeout=110, check=False)
        if result.returncode:
            # Deliberately do not return stderr or any configuration/environment.
            raise ValueError('SOURCE_TRANSPORT_FAILED')
        envelope = json.loads(result.stdout)
        compressed = base64.b64decode(envelope['gzip_base64'], validate=True)
        payload = gzip.decompress(compressed)
        if hashlib.sha256(payload).hexdigest() != envelope['json_sha256']:
            raise ValueError('TRANSPORT_HASH_MISMATCH')
        return json.loads(payload)


def save_probe(folder, name, request, result, probe_hash):
    payload = json.dumps(result, ensure_ascii=False, separators=(',', ':'), default=str).encode('utf-8')
    raw = gzip.compress(payload, mtime=0)
    rawpath = folder / (name + '.json.gz')
    rawpath.write_bytes(raw)
    receipt = {k: v for k, v in result.items() if k != 'rows'}
    receipt.update(request=request, probe_sha256=probe_hash, raw_path=rawpath.name,
                   raw_sha256=sha(rawpath), payload_sha256=hashlib.sha256(payload).hexdigest(),
                   local_received_at=utc_now())
    write_new(folder / (name + '.receipt.json'), receipt)
    return receipt


def load_payload(folder, name):
    receipt_path = folder / (name + '.receipt.json')
    receipt = read(receipt_path)
    rel = Path(receipt['raw_path'])
    if rel.name != str(rel):
        raise ValueError('RAW_PATH_SCOPE')
    raw = folder / rel
    if sha(raw) != receipt['raw_sha256']:
        raise ValueError('RAW_HASH_CHANGED')
    blob = gzip.decompress(raw.read_bytes())
    if hashlib.sha256(blob).hexdigest() != receipt['payload_sha256']:
        raise ValueError('PAYLOAD_HASH_CHANGED')
    payload = json.loads(blob)
    if any(receipt.get(k) != v for k, v in payload.items() if k != 'rows'):
        raise ValueError('RECEIPT_PAYLOAD_MISMATCH')
    if payload.get('status') != 'OK' or payload.get('mode') != receipt['request']['mode']:
        raise ValueError('PROBE_FAILED_OR_MODE_CHANGED')
    return receipt, payload


def _log_state(folder):
    _, a = load_payload(folder, 'logs_before')
    _, b = load_payload(folder, 'logs_after')
    a, b = a['logs'], b['logs']
    if a['active_known_pids'] or b['active_known_pids']:
        return a, 'KNOWN_COLLECTOR_ACTIVE'
    if [(f['path'], f['sha256']) for f in a['files']] != [(f['path'], f['sha256']) for f in b['files']]:
        return a, 'SYNC_CHANGED_DURING_READ'
    return a, None


def _frame(rows):
    frame = pd.DataFrame(rows, columns=FIELDS)
    for name in ['dt', 'stat_date']:
        parsed = pd.to_datetime(frame[name], errors='coerce')
        frame[name] = parsed.dt.strftime('%Y-%m-%d')
    for name in ['volume', 'delete_flag']:
        original = frame[name]
        frame[name] = pd.to_numeric(original, errors='coerce')
        if (original.notna() & frame[name].isna()).any():
            raise ValueError('NUMERIC_PARSE_FAILURE')
    if frame[PK].isna().any().any() or frame.duplicated(PK).any():
        raise ValueError('INVALID_OR_DUPLICATE_ASIN_KEY')
    return frame


def mapping_drift(raw, anchor_path=None, anchor_sha256=None):
    columns = ['dt', 'store_name', 'country', 'asin', 'spu', 'anchor_spu', 'mapping_state']
    if anchor_path is None:
        out = raw[['dt', 'store_name', 'country', 'asin', 'spu']].copy()
        out['anchor_spu'] = None
        out['mapping_state'] = 'ANCHOR_UNAVAILABLE'
        return out[columns], 'UNKNOWN'
    if not anchor_sha256 or sha(anchor_path) != anchor_sha256:
        raise ValueError('FROZEN_MAPPING_HASH_MISMATCH')
    anchor = pd.read_parquet(anchor_path, columns=PK + ['spu'])
    if anchor.duplicated(PK).any() or anchor[PK].isna().any().any():
        raise ValueError('FROZEN_MAPPING_DUPLICATE')
    anchor['dt'] = pd.to_datetime(anchor.dt).dt.strftime('%Y-%m-%d')
    # Latest observed date per exact ASIN identity, never first SKU/prefix parsing.
    keys = ['store_name', 'country', 'asin']
    latest = anchor.groupby(keys).dt.transform('max')
    anchor = anchor.loc[anchor.dt.eq(latest), keys + ['spu']].rename(columns={'spu': 'anchor_spu'})
    merged = raw[['dt'] + keys + ['spu']].merge(anchor, on=keys, how='left',
                                              validate='many_to_one', indicator=True)
    same = merged.spu.eq(merged.anchor_spu) | (merged.spu.isna() & merged.anchor_spu.isna())
    merged['mapping_state'] = np.select([merged['_merge'].eq('left_only'), same],
                                       ['NEW_ASIN_MAPPING', 'UNCHANGED'], default='MAPPING_CHANGED')
    return merged[columns], 'ANCHORED_CURRENT_OBSERVATION'


def aggregate(raw, core, start, end, evidence, drift, mapping_version):
    if len(raw):
        norm = normalized(raw, 'HASH_BOUND_RECEIPTS', utc_now())
    else:
        norm = pd.DataFrame(columns=['ds', 'shop', 'spu', 'y', 'mapped', 'sales_state', 'source_rows'])
    norm['ds'] = pd.to_datetime(norm.ds).dt.strftime('%Y-%m-%d')
    state_by = {(x['ds'], x['shop']): x for x in evidence}
    shops, objects = [], []
    changed = drift[drift.mapping_state.ne('UNCHANGED')]
    for day in pd.date_range(start, end):
        ds = str(day.date())
        for shop in SHOPS:
            part = norm[norm.ds.eq(ds) & norm.shop.eq(shop)]
            source_state = state_by[(ds, shop)]['source_state']
            valid_source = source_state == 'SOURCE_READY'
            observed = float(part.y.sum(min_count=1)) if len(part) else (0.0 if valid_source else None)
            invalid = bool(len(part) and ~part.sales_state.isin(['OBSERVED', 'EXPLICIT_ZERO']).all())
            unknown_units = float(part.loc[~part.mapped.astype(bool), 'y'].sum()) if len(part) else None
            shops.append(dict(ds=ds, shop=shop, y=observed if valid_source and not invalid else None,
                              observed_units=observed, source_state=source_state,
                              sales_state=('UNKNOWN' if invalid or not valid_source else
                                           'EXPLICIT_ZERO' if observed == 0 else 'OBSERVED'),
                              source_rows=int(part.source_rows.sum()) if len(part) else 0,
                              unknown_mapping_units=unknown_units, mapping_version=mapping_version))
            changed_here = changed[changed.dt.eq(ds) & changed.store_name.eq(shop)]
            affected = set(changed_here.spu.dropna()) | set(changed_here.anchor_spu.dropna())
            for obj in core[core.shop.eq(shop)].itertuples(index=False):
                g = part[part.spu.eq(obj.spu) & part.mapped.astype(bool)]
                value = float(g.y.iloc[0]) if len(g) and pd.notna(g.y.iloc[0]) else None
                if not valid_source:
                    state = 'MISSING_SOURCE' if source_state == 'PRODUCTION_NO_RECORDS' else 'SOURCE_NOT_PROVEN'
                elif mapping_version == 'UNKNOWN':
                    state = 'MAPPING_EVIDENCE_UNKNOWN'
                elif obj.spu in affected:
                    state = 'MAPPING_CHANGED_OR_NEW'
                elif not len(g):
                    state = 'MISSING_RECORD_NOT_ZERO'
                else:
                    state = g.sales_state.iloc[0]
                y = value if state in ('OBSERVED', 'EXPLICIT_ZERO') else None
                objects.append(dict(ds=ds, shop=shop, spu=obj.spu, y=y, observed_units=value,
                                    sales_state=state, source_state=source_state,
                                    source_rows=int(g.source_rows.sum()) if len(g) else 0,
                                    mapping_version=mapping_version))
    return pd.DataFrame(shops), pd.DataFrame(objects), norm


def revision_diff(old, new):
    if old.duplicated(PK).any() or new.duplicated(PK).any():
        raise ValueError('REVISION_DUPLICATE_KEY')
    merged = old.merge(new, on=PK, how='outer', suffixes=('_old', '_new'),
                       validate='one_to_one', indicator=True)
    changed = merged['_merge'].ne('both')
    fields = [c for c in FIELDS if c not in PK]
    for col in fields:
        a, b = merged[col + '_old'], merged[col + '_new']
        equal = a.eq(b) | (a.isna() & b.isna())
        merged['changed_' + col] = ~equal
        changed |= ~equal
    return merged.loc[changed].copy()


def _source_rows(folder, frozen_plan):
    _, metadata = load_payload(folder, 'metadata')
    if set(x['COLUMN_NAME'] for x in metadata['columns']) != set(FIELDS):
        raise ValueError('SOURCE_FIELDS_CHANGED')
    frames = []
    scan = 0
    for i, (start, end) in enumerate(frozen_plan['chunks']):
        rec, result = load_payload(folder, f'daily_{i:02d}')
        request = rec['request']
        if request['start'] != start or request['end_exclusive'] != end:
            raise ValueError('RECEIPT_SCOPE_MISMATCH')
        expected_params = [start, end] + list(SHOPS)
        if list(result['params']) != expected_params:
            raise ValueError('QUERY_PARAMETER_MISMATCH')
        expected_sql = ('SELECT ' + ','.join(FIELDS) + ' FROM ' + SOURCE +
                        ' FORCE INDEX(PRIMARY) WHERE dt>=%s AND dt<%s AND store_name IN (%s,%s,%s,%s) ORDER BY dt,store_name,country,asin')
        if result['sql'] != expected_sql:
            raise ValueError('QUERY_SQL_CHANGED')
        if int(result['estimated_scan']) > 1000000:
            raise ValueError('SCAN_BUDGET')
        tables = plans(result['explain'])
        if not tables or any(t.get('access_type') == 'ALL' for t in tables):
            raise ValueError('UNBOUNDED_EXECUTION_PLAN')
        if sum(int(t['rows_examined_per_scan']) for t in tables) != int(result['estimated_scan']):
            raise ValueError('EXPLAIN_SCAN_MISMATCH')
        scan += int(result['estimated_scan'])
        frame = _frame(result['rows'])
        if len(frame) != result['row_count'] or len(frame) > 250000:
            raise ValueError('RESULT_COUNT_MISMATCH')
        if not (frame.dt.ge(start) & frame.dt.lt(end) & frame.store_name.isin(SHOPS)).all():
            raise ValueError('ROW_OUT_OF_SCOPE')
        frames.append(frame)
    if scan > 10000000:
        raise ValueError('TOTAL_SCAN_BUDGET')
    raw = pd.concat(frames, ignore_index=True) if frames else _frame([])
    if raw.duplicated(PK).any():
        raise ValueError('CROSS_CHUNK_DUPLICATE')
    return raw


def derive_completion(raw, logs, log_error, frozen_plan):
    evidence = []
    expected_sid = {'JQ-US': '11548', 'MT-US': '11545', 'RKZ-US': '11550', 'SY-US': '11547'}
    if not raw.country.eq('美国').all() or any(str(s) != expected_sid[shop] for s, shop in zip(raw.sid, raw.store_name)):
        raise ValueError('SHOP_SID_COUNTRY_CHANGED')
    for day in pd.date_range(frozen_plan['collection_start'], frozen_plan['collection_end']):
        for shop in SHOPS:
            ds = str(day.date())
            part = raw[raw.dt.eq(ds) & raw.store_name.eq(shop)]
            proof = completion(logs, ds, shop) if not log_error else None
            if log_error:
                state = log_error
            elif proof is None:
                state = 'PRODUCTION_NO_RECORDS' if not len(part) else 'SYNC_COMPLETION_UNKNOWN'
            elif proof['rows'] != len(part):
                state = 'SYNC_COUNT_MISMATCH'
            elif part.volume.isna().any() or part.volume.lt(0).any() or part.volume.mod(1).ne(0).any():
                state = 'QUANTITY_INVALID'
            elif part.stat_date.isna().any() or part.stat_date.ne(part.dt).any() or part.delete_flag.ne(0).any():
                state = 'DATE_OR_DELETE_INVALID'
            else:
                state = 'SOURCE_READY'
            evidence.append(dict(ds=ds, shop=shop, source_state=state,
                                 proof_basis='NORMAL_SYNC_FULL_RUN_AND_CONTIGUOUS_PAGE_WRITES' if state == 'SOURCE_READY' else None,
                                 rows=len(part), known_units=float(part.volume.sum()), completion=proof))
    return evidence


def build_package(out, core, contract, frozen_plan, anchor_path=None, anchor_sha256=None, previous=None):
    out = Path(out)
    raw = _source_rows(out / 'source', frozen_plan)
    logs, log_error = _log_state(out / 'source')
    evidence = derive_completion(raw, logs, log_error, frozen_plan)
    if anchor_path:
        if sha(anchor_path) != anchor_sha256:
            raise ValueError('FROZEN_MAPPING_HASH_MISMATCH')
        days = pd.read_parquet(anchor_path, columns=['dt']).dt
        if pd.to_datetime(days).max() > pd.Timestamp(contract['forecast_input_cutoff']):
            raise ValueError('MAPPING_ANCHOR_AFTER_FORECAST_CUTOFF')
        with (out / 'frozen_mapping.parquet').open('xb') as stream:
            stream.write(Path(anchor_path).read_bytes())
        anchor_path = out / 'frozen_mapping.parquet'
    drift, mapping_version = mapping_drift(raw, anchor_path, anchor_sha256)
    shops, objects, norm = aggregate(raw, core, frozen_plan['collection_start'], frozen_plan['collection_end'], evidence, drift, mapping_version)
    raw.to_parquet(out / 'collected_raw.parquet', index=False)
    drift.to_csv(out / 'mapping_drift.csv', index=False, encoding='utf-8-sig')
    norm.loc[~norm.mapped.astype(bool)].to_csv(out / 'mapping_unknown.csv', index=False, encoding='utf-8-sig')
    write_new(out / 'completion_evidence.json', evidence)
    revised = pd.DataFrame(columns=PK)
    if previous:
        prev = verify_package(previous, contract['run_id'], core, contract)
        old = pd.read_parquet(Path(previous) / 'raw.parquet')
        overlap = old.dt.ge(frozen_plan['collection_start'])
        revised = revision_diff(old.loc[overlap], raw[raw.dt.le(str(old.dt.max()))])
        raw = pd.concat([old.loc[~overlap], raw], ignore_index=True)
        for data, name in ((shops, 'shop_daily'), (objects, 'core_daily')):
            previous_data = prev[name]
            keep = previous_data[previous_data.ds.lt(frozen_plan['collection_start'])]
            combined = pd.concat([keep, data], ignore_index=True)
            if name == 'shop_daily':
                shops = combined
            else:
                objects = combined
    if raw.duplicated(PK).any() or objects.duplicated(CORE_KEY).any() or shops.duplicated(['ds', 'shop']).any():
        raise ValueError('VERSION_MERGE_DUPLICATE')
    raw.to_parquet(out / 'raw.parquet', index=False)
    shops.to_csv(out / 'shop_daily.csv', index=False, encoding='utf-8-sig', float_format='%.17g')
    objects.to_csv(out / 'core_daily.csv', index=False, encoding='utf-8-sig', float_format='%.17g')
    revised.to_csv(out / 'revision_diff.csv', index=False, encoding='utf-8-sig')
    # Independent regrouping of raw quantities checks conservation, including unknown mappings.
    observed = raw.groupby(['dt', 'store_name']).volume.sum(min_count=1)
    for row in shops.itertuples(index=False):
        key = row.ds, row.shop
        if key in observed.index and pd.notna(row.observed_units) and observed.loc[key] != row.observed_units:
            raise ValueError('SHOP_QUANTITY_CONSERVATION')
    files = {str(p.relative_to(out)).replace('\\', '/'): sha(p) for p in out.rglob('*') if p.is_file()}
    manifest = dict(contract, status='SOURCE_READY' if shops.source_state.eq('SOURCE_READY').all() else 'SOURCE_INCOMPLETE',
                    created_at=utc_now(), collection_end=frozen_plan['collection_end'],
                    source_start=contract['target_start'], source_end=frozen_plan['collection_end'],
                    files=files, fixed_core_count=len(core), collected_rows=len(pd.read_parquet(out / 'collected_raw.parquet')),
                    raw_rows=len(raw), core_daily_rows=len(objects), shop_daily_rows=len(shops),
                    source_complete_shop_days=int(shops.source_state.eq('SOURCE_READY').sum()),
                    core_known_days=int(objects.sales_state.isin(['OBSERVED', 'EXPLICIT_ZERO']).sum()),
                    core_unknown_days=int((~objects.sales_state.isin(['OBSERVED', 'EXPLICIT_ZERO'])).sum()),
                    mapping_version=mapping_version, frozen_mapping_sha256=anchor_sha256,
                    revisions=len(revised), previous_manifest_sha256=sha(Path(previous) / 'manifest.json') if previous else None,
                    previous_package=str(Path(previous).resolve()) if previous else None,
                    production_writes=0, api_calls=0, model_fits=0, model_inference=0,
                    business_queries=len(frozen_plan['chunks']), limitations=[
                        'Normal-sync completion and contiguous page logs are operational evidence, not independent upstream truth.',
                        'No process check proves absence of every other writer.',
                        'Current source SPU is observed mapping, not retroactive replacement of the frozen core.',
                        'Missing object records are unknown, never synthetic zero.',
                        'Unknown mapping units remain in the full-shop denominator.'])
    write_new(out / 'manifest.json', manifest)
    return manifest


def verify_package(path, expected_run_id, core, expected_contract=None, _visited=None):
    """Hash-check the archived evidence and return fixed-core/store daily frames."""
    path = Path(path).resolve()
    visited = set() if _visited is None else set(_visited)
    if path in visited:
        raise ValueError('CYCLIC_SOURCE_VERSION')
    visited.add(path)
    m = read(path / 'manifest.json')
    if m.get('run_id') != expected_run_id or m.get('status') not in ('SOURCE_READY', 'SOURCE_INCOMPLETE'):
        raise ValueError('SOURCE_PACKAGE_RUN_OR_STATUS')
    bound_fields = ('issue_seal_sha256', 'core_sha256', 'target_start', 'target_end')
    if any(not m.get(key) for key in bound_fields):
        raise ValueError('SOURCE_ISSUE_BINDING_MISSING')
    if expected_contract is not None and any(m.get(key) != expected_contract.get(key) for key in bound_fields):
        raise ValueError('SOURCE_ISSUE_BINDING_MISMATCH')
    required = {'raw.parquet', 'core_daily.csv', 'shop_daily.csv', 'completion_evidence.json',
                'source/logs_before.receipt.json', 'source/logs_after.receipt.json', 'source/metadata.receipt.json', 'plan.json'}
    if not required.issubset(m['files']):
        raise ValueError('SOURCE_EVIDENCE_MISSING')
    for rel, digest in m['files'].items():
        item = (path / rel).resolve()
        if not item.is_relative_to(path) or sha(item) != digest:
            raise ValueError('SOURCE_PACKAGE_CHANGED')
    frozen_plan = read(path / 'plan.json')
    if any(m.get(key) != frozen_plan.get(key) for key in bound_fields + ('run_id',)):
        raise ValueError('SOURCE_PLAN_BINDING_MISMATCH')
    available = read_clock(frozen_plan['server_now']).astimezone(ZoneInfo('Asia/Shanghai')).date() - dt.timedelta(days=2)
    if not (m['target_start'] <= m['source_start'] <= m['source_end'] <= min(str(available), m['target_end'])):
        raise ValueError('SOURCE_DATE_NOT_AVAILABLE')
    collected_raw = _source_rows(path / 'source', frozen_plan)
    logs, log_error = _log_state(path / 'source')
    evidence = derive_completion(collected_raw, logs, log_error, frozen_plan)
    if read(path / 'completion_evidence.json') != evidence:
        raise ValueError('COMPLETION_EVIDENCE_DERIVATION_MISMATCH')
    anchor = path / 'frozen_mapping.parquet' if m['frozen_mapping_sha256'] else None
    drift, mapping_version = mapping_drift(collected_raw, anchor, m['frozen_mapping_sha256'])
    expected_sd, expected_cd, _ = aggregate(collected_raw, core, frozen_plan['collection_start'],
                                          frozen_plan['collection_end'], evidence, drift, mapping_version)
    expected_raw = collected_raw
    if m.get('previous_package'):
        oldpath = Path(m['previous_package'])
        if sha(oldpath / 'manifest.json') != m['previous_manifest_sha256']:
            raise ValueError('PREVIOUS_SOURCE_MANIFEST_CHANGED')
        previous = verify_package(oldpath, expected_run_id, core, expected_contract, visited)
        oldraw = pd.read_parquet(oldpath / 'raw.parquet')
        expected_raw = pd.concat([oldraw[oldraw.dt.lt(frozen_plan['collection_start'])], collected_raw], ignore_index=True)
        for name in ['core_daily', 'shop_daily']:
            old = previous[name]
            current = expected_cd if name == 'core_daily' else expected_sd
            combined = pd.concat([old[old.ds.lt(frozen_plan['collection_start'])], current], ignore_index=True)
            if name == 'core_daily':
                expected_cd = combined
            else:
                expected_sd = combined
    cd = pd.read_csv(path / 'core_daily.csv', dtype={'ds': str, 'shop': str, 'spu': str}, float_precision='round_trip')
    sd = pd.read_csv(path / 'shop_daily.csv', dtype={'ds': str, 'shop': str}, float_precision='round_trip')
    dates = [str(x.date()) for x in pd.date_range(m['source_start'], m['source_end'])]
    expected = {(day, row.shop, row.spu) for day in dates for row in core.itertuples(index=False)}
    if cd.duplicated(CORE_KEY).any() or set(cd[CORE_KEY].itertuples(index=False, name=None)) != expected:
        raise ValueError('FIXED_CORE_CHANGED_OR_INCOMPLETE')
    if sd.duplicated(['ds', 'shop']).any() or set(sd[['ds', 'shop']].itertuples(index=False, name=None)) != {(d, s) for d in dates for s in SHOPS}:
        raise ValueError('SHOP_DATE_COVERAGE')
    known = cd.sales_state.isin(['OBSERVED', 'EXPLICIT_ZERO'])
    if cd.loc[~known, 'y'].notna().any() or not np.isfinite(cd.loc[known, 'y']).all() or cd.loc[known, 'y'].lt(0).any():
        raise ValueError('UNKNOWN_WAS_FILLED_OR_KNOWN_INVALID')
    if not cd.loc[cd.sales_state.eq('EXPLICIT_ZERO'), 'y'].eq(0).all() or not cd.loc[cd.sales_state.eq('OBSERVED'), 'y'].gt(0).all():
        raise ValueError('ZERO_STATE_INCONSISTENT')
    if m['status'] == 'SOURCE_READY' and not sd.source_state.eq('SOURCE_READY').all():
        raise ValueError('HAND_FILLED_SOURCE_PASS')
    pd.testing.assert_frame_equal(cd.sort_values(CORE_KEY).reset_index(drop=True),
                                  expected_cd.sort_values(CORE_KEY).reset_index(drop=True),
                                  check_dtype=False, check_exact=True)
    pd.testing.assert_frame_equal(sd.sort_values(['ds', 'shop']).reset_index(drop=True),
                                  expected_sd.sort_values(['ds', 'shop']).reset_index(drop=True),
                                  check_dtype=False, check_exact=True)
    pd.testing.assert_frame_equal(pd.read_parquet(path / 'raw.parquet').sort_values(PK).reset_index(drop=True),
                                  expected_raw.sort_values(PK).reset_index(drop=True),
                                  check_dtype=False, check_exact=True)
    return {'manifest': m, 'core_daily': cd, 'shop_daily': sd}


def collect(run_dir, out, *, clock_provider=server_clock, transport=None,
            previous=None, frozen_mapping=None, frozen_mapping_sha256=None, execute_read=False):
    """Create one new version; a premature target returns before DB transport."""
    _, core, contract = issue_contract(run_dir)
    now = read_clock(clock_provider())
    previous_end = None
    if previous:
        previous_end = verify_package(previous, contract['run_id'], core, contract)['manifest']['source_end']
    frozen_plan = plan(contract, now, previous_end)
    out = Path(out).resolve()
    if out == Path(run_dir).resolve() or Path(run_dir).resolve() in out.parents:
        raise ValueError('OUTPUT_INSIDE_IMMUTABLE_RUN')
    out.mkdir(parents=True, exist_ok=False)
    write_new(out / 'plan.json', dict(frozen_plan, **contract, budget=DEFAULT_BUDGET))
    if frozen_plan['status'] == 'WAITING_TARGET_START':
        result = dict(contract, **frozen_plan, production_writes=0, api_calls=0,
                      database_queries=0, business_queries=0, model_fits=0,
                      model_inference=0, files={'plan.json': sha(out / 'plan.json')})
        write_new(out / 'manifest.json', result)
        return result
    if not execute_read:
        result = dict(contract, status='READ_PLANNED_NOT_EXECUTED', database_queries=0,
                      business_queries=0, production_writes=0, api_calls=0,
                      files={'plan.json': sha(out / 'plan.json')})
        write_new(out / 'manifest.json', result)
        return result
    if len(frozen_plan['chunks']) > DEFAULT_BUDGET['business_selects']:
        raise ValueError('QUERY_COUNT_BUDGET')
    folder = out / 'source'
    folder.mkdir()
    client = transport if transport is not None else SSHProbeTransport()
    probe_hash = getattr(client, 'probe_sha256', 'OFFLINE_TEST_DOUBLE')
    scan, attempts = 0, []
    requests = [('logs_before', dict(mode='logs')), ('metadata', dict(mode='metadata'))]
    for i, (start, end) in enumerate(frozen_plan['chunks']):
        requests.append((f'daily_{i:02d}', dict(mode='data', start=start, end_exclusive=end,
                        scope_start=frozen_plan['collection_start'],
                        scope_end_exclusive=str(dt.date.fromisoformat(frozen_plan['collection_end']) + dt.timedelta(days=1)))))
    requests.append(('logs_after', dict(mode='logs')))
    try:
        for name, request in requests:
            request = dict(request, remaining_scan=DEFAULT_BUDGET['total_scan']-scan)
            if request['mode'] == 'data' and request['remaining_scan'] <= 0:
                raise ValueError('TOTAL_SCAN_BUDGET')
            attempt = dict(name=name, request=request, started_at=utc_now())
            attempts.append(attempt)
            write_new(folder / (name + '.attempt.json'), attempt)
            response = client(request)
            save_probe(folder, name, request, response, probe_hash)
            if response.get('status') != 'OK':
                raise ValueError('SOURCE_PROBE_FAILED')
            scan += int(response.get('estimated_scan', 0))
        return build_package(out, core, contract, frozen_plan, frozen_mapping,
                             frozen_mapping_sha256, previous)
    except Exception as exc:
        failure = dict(contract, status='SOURCE_INCOMPLETE', error_type=type(exc).__name__,
                       guard=str(exc) if isinstance(exc, ValueError) else 'CONTROLLED_FAILURE',
                       created_at=utc_now(), request_attempts=len(attempts),
                       business_queries=sum(x['request']['mode'] == 'data' for x in attempts),
                       estimated_scan=scan, production_writes=0, api_calls=0,
                       model_fits=0, model_inference=0)
        if not (out / 'manifest.json').exists():
            write_new(out / 'manifest.json', failure)
        return failure


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--previous')
    parser.add_argument('--frozen-mapping')
    parser.add_argument('--frozen-mapping-sha256')
    parser.add_argument('--execute-read', action='store_true')
    args = parser.parse_args(argv)
    result = collect(args.run, args.out, previous=args.previous,
                     frozen_mapping=args.frozen_mapping,
                     frozen_mapping_sha256=args.frozen_mapping_sha256,
                     execute_read=args.execute_read)
    print(json.dumps({k: result.get(k) for k in ('run_id', 'status', 'source_end', 'business_queries', 'guard')}, ensure_ascii=False))
    return 0 if result['status'] in ('WAITING_TARGET_START', 'SOURCE_READY', 'READ_PLANNED_NOT_EXECUTED') else 2


if __name__ == '__main__':
    raise SystemExit(main())
