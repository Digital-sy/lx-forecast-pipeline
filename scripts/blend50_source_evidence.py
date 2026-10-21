"""Offline evidence gate v1. No network, SQL, training, or prediction capability.

Input is an approved adapter's canonical archive, NOT the native Lingxing API
schema. The live adapter and independent platform-export acquisition still need
separate acceptance. Hashes detect changes, not a fabricated external origin.
"""
import argparse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path

SCHEMA = 'source_slice_evidence_v1'
PK = ('dt', 'store_name', 'country', 'asin')
FIELDS = PK + ('sid', 'sku', 'spu', 'stat_date', 'delete_flag', 'volume')
SOURCE = 'ods_db.ods_lx_product_performance'


class EvidenceError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise EvidenceError(code)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def timestamp(value):
    require(isinstance(value, str), 'TIMESTAMP_UNKNOWN')
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceError('TIMESTAMP_INVALID') from exc
    require(dt.tzinfo is not None and dt.utcoffset() is not None, 'TIMEZONE_UNKNOWN')
    return dt


def load_artifact(root, ref):
    rel = ref['path']
    p = (root / rel).resolve()
    require(not Path(rel).is_absolute() and p.is_relative_to(root), 'PATH_ESCAPE')
    require(p.is_file(), 'ARTIFACT_MISSING')
    blob = p.read_bytes()
    require(digest(blob) == ref['sha256'], 'ARTIFACT_TAMPERED_OR_REVISED')
    return json.loads(blob)


def project_payload(raw, request):
    """Strict minimal projection of shapes inspected in the deployed collector.

    No synthetic fields are injected into the retained response. This adapter
    fails closed for unsupported shapes and must be tested on future real pages.
    """
    data = raw['data']
    if isinstance(data, list):
        native, total = data, raw.get('total')
    else:
        require(isinstance(data, dict), 'RAW_DATA_SHAPE')
        native = next((data[k] for k in ('records', 'list', 'rows') if isinstance(data.get(k), list)), None)
        total = raw.get('total', data.get('total'))
    require(isinstance(native, list) and type(total) is int, 'RAW_DATA_SHAPE')
    def first(value):
        if isinstance(value, dict):return value
        if isinstance(value, list) and value and isinstance(value[0], dict):return value[0]
        return {}
    def score(item):
        v = item.get('volume')
        try:v = float(v or 0)
        except (ValueError, TypeError):v = 0.0
        return (int(str(item.get('is_delete') or '0') == '0'),
                int(not str(item.get('seller_sku') or '').startswith('Amazon.Found.')),
                int(item.get('price') not in (None, '')), v)
    rows = []
    for row in native:
        asin = first(row.get('asins'))
        require(asin.get('sid') is None or str(asin['sid']) == str(request['sid']), 'NATIVE_SID_MISMATCH')
        price = row.get('price_list')
        if isinstance(price, dict):chosen = price
        else:chosen = max([p for p in (price or []) if isinstance(p, dict)], key=score, default={})
        spus = row.get('spu_spu_names')
        spu = '|'.join(str(s['spu']) for s in spus if isinstance(s, dict) and s.get('spu') not in (None, '')) if isinstance(spus, list) else None
        rows.append(dict(dt=request['date'], store_name=request['shop'], country=chosen.get('country'),
                         asin=asin.get('asin') or row.get('asin'), sid=row.get('sid'),
                         sku=chosen.get('local_sku') or row.get('sku'), spu=spu or None,
                         stat_date=request['date'], delete_flag=0, volume=row.get('volume')))
    return {'rows': rows, 'total': total}


def rows_signature(rows, day, shop, sid, strict_model_mapping=True):
    """No missing-to-zero conversion; includes mapping and all model-used fields."""
    by_key = {}
    total = Decimal(0)
    for row in rows:
        require(all(k in row for k in FIELDS), 'ROW_FIELD_MISSING')
        require(row['dt'] == day and row['store_name'] == shop and
                str(row['sid']) == str(sid), 'ROW_SCOPE_MISMATCH')
        key = tuple(row[k] for k in PK)
        require(all(isinstance(k, str) and k.strip() for k in key), 'PRIMARY_KEY_INVALID')
        require(key not in by_key, 'DUPLICATE_PRIMARY_KEY')
        require(row['stat_date'] == day and row['delete_flag'] == 0, 'DATE_OR_DELETE_INVALID')
        require(row['volume'] is not None and not isinstance(row['volume'], bool), 'UNKNOWN_NOT_ZERO')
        try:
            qty = Decimal(str(row['volume']))
        except (InvalidOperation, ValueError) as exc:
            raise EvidenceError('QUANTITY_INVALID') from exc
        require(qty.is_finite() and qty >= 0 and qty == qty.to_integral_value(), 'QUANTITY_INVALID')
        # v1 deliberately blocks unresolved mappings. No row is silently discarded.
        for field in (('sku', 'spu') if strict_model_mapping else ()):
            v = row[field]
            require(isinstance(v, str) and v.strip() and v not in ('-', 'DEFAULT', '无', 'None', 'null')
                    and not any(c in v for c in '|,，;；'), 'MAPPING_UNRESOLVED')
        clean = {k: row[k] for k in FIELDS}
        clean.update(volume=int(qty), sid=str(sid))
        by_key[key] = clean
        total += qty
    ordered = [by_key[k] for k in sorted(by_key)]
    return {'row_count': len(rows), 'unique_keys': len(by_key), 'units': str(total),
            'content_sha256': digest(canonical(ordered))}


def validate_pages(pages, day, shop, sid, strict_model_mapping=True):
    """Validate canonical page chain; a short/empty page is not automatic success."""
    require(bool(pages), 'NO_PAGE_RECEIPTS')
    expected = 0
    reported = None
    request_ids = set()
    rows = []
    first = last = None
    for page in pages:
        req = page['request']
        require(req['date'] == day and req['shop'] == shop and
                str(req['sid']) == str(sid) and req['summary_field'] == 'asin', 'PAGE_SCOPE_MISMATCH')
        require(req['offset'] == expected and type(req['length']) is int and req['length'] > 0,
                'OFFSET_OR_LENGTH_INVALID')
        started, finished = timestamp(page['started_at']), timestamp(page['acquired_at'])
        require(started <= finished and (last is None or started >= last), 'PAGE_TIME_ORDER')
        first = first or started
        last = finished
        require(type(page['code']) is int and page['code'] == 0, 'API_ERROR_OR_RETRY_EXHAUSTED')
        rid = page['request_id']
        require(isinstance(rid, str) and rid and rid not in request_ids, 'DUPLICATE_OR_MISSING_REQUEST_ID')
        request_ids.add(rid)
        n = page['total']
        require(type(n) is int and n >= 0, 'TOTAL_UNKNOWN')
        reported = n if reported is None else reported
        require(n == reported, 'TOTAL_DRIFT')
        part = page['rows']
        require(isinstance(part, list) and len(part) <= req['length'], 'PAGE_LENGTH_INVALID')
        expected += len(part)
        require(expected <= reported, 'TOTAL_EXCEEDED')
        if not part:
            require(reported == 0 and len(pages) == 1, 'EMPTY_PAGE_BEFORE_COMPLETION')
        rows.extend(part)
    require(expected == reported, 'PAGINATION_INCOMPLETE')
    signature = rows_signature(rows, day, shop, sid, strict_model_mapping)
    return rows, signature, first, last


def scope_cells(scope):
    require(isinstance(scope, list) and bool(scope), 'EMPTY_SCOPE')
    cells = set()
    for s in scope:
        date.fromisoformat(s['date'])
        cell = (s['date'], s['shop'], str(s['sid']))
        require(cell not in cells and s['shop'] and str(s['sid']), 'DUPLICATE_SCOPE')
        cells.add(cell)
    return cells


def check_bundle(root, manifest_hash, scope, current_ref):
    root = Path(root).resolve()
    manifest = load_artifact(root, {'path': 'manifest.json', 'sha256': manifest_hash})
    required = scope_cells(scope)
    require(manifest['schema'] == SCHEMA and manifest['source'] == SOURCE, 'CONTRACT_MISMATCH')
    require(scope_cells(manifest['scope']) == required, 'MANIFEST_SCOPE_MISMATCH')
    # The adapter code's identity must be pinned in the review, not a status field.
    require(len(manifest['adapter_sha256']) == 64 and len(manifest['collector_sha256']) == 64,
            'SOURCE_VERSION_MISSING')
    current = load_artifact(root, current_ref)
    require(current['source'] == SOURCE and scope_cells(current['scope']) == required, 'CURRENT_SCOPE_MISMATCH')
    current_at = timestamp(current['acquired_at'])
    seen = set()
    evidence = []
    for sl in manifest['slices']:
        day, shop, sid = sl['date'], sl['shop'], str(sl['sid'])
        key = (day, shop, sid)
        require(key in required and key not in seen, 'SLICE_MISSING_DUPLICATE_OR_EXTRA')
        seen.add(key)
        pages = []
        for ref in sl['pages']:
            page = load_artifact(root, ref)
            # Original credential-free response bytes retained separately; actual
            # response-to-canonical adapter remains a reviewed external boundary.
            raw = load_artifact(root, page['raw_response'])
            require(raw['code'] == page['code'] and raw['request_id'] == page['request_id'], 'RAW_RESPONSE_MISMATCH')
            require(digest(canonical(project_payload(raw, page['request']))) == digest(canonical({
                'rows': page['rows'], 'total': page['total']})), 'RAW_ROWS_MISMATCH')
            pages.append(page)
        rows, sig, started, ended = validate_pages(pages, day, shop, sid)
        export = load_artifact(root, sl['platform_export'])
        require(export['source'] == 'LINGXING_PLATFORM_EXPORT' and export['export_id'], 'INDEPENDENT_SOURCE_MISSING')
        require(export['date'] == day and export['shop'] == shop and str(export['sid']) == sid,
                'EXPORT_SCOPE_MISMATCH')
        export_at = timestamp(export['acquired_at'])
        require(export_at >= ended, 'EXPORT_PREDATES_FETCH')
        require(rows_signature(export['rows'], day, shop, sid) == sig, 'PLATFORM_RECONCILIATION_FAILED')
        publication = load_artifact(root, sl['publication'])
        require(publication['transaction_id'] and publication['task_id'] and
                publication['date'] == day and publication['shop'] == shop and str(publication['sid']) == sid,
                'PUBLICATION_SCOPE_MISMATCH')
        committed = timestamp(publication['committed_at'])
        require(committed >= export_at and current_at >= committed, 'SYNC_TIME_ORDER')
        require(publication['content_sha256'] == sig['content_sha256'] and
                publication['page_manifest_sha256'] == digest(canonical(sl['pages'])), 'PUBLICATION_NOT_BOUND')
        nowrows = [r for r in current['rows'] if r['dt'] == day and r['store_name'] == shop and str(r['sid']) == sid]
        require(rows_signature(nowrows, day, shop, sid) == sig, 'CURRENT_SOURCE_REVISED')
        evidence.append(dict(date=day, shop=shop, sid=sid, **sig,
                             acquired_at=ended.isoformat(), source_sync_at=committed.isoformat(),
                             source_business_asof=day, source_version=manifest['collector_sha256']))
    require(seen == required, 'REQUIRED_SLICE_WITHOUT_RECEIPT')
    require(all((r['dt'], r['store_name'], str(r['sid'])) in required for r in current['rows']), 'CURRENT_EXTRA_SCOPE')
    return {'status': 'PASS_SOURCE_EVIDENCE_ONLY', 'slices': evidence,
            'manifest_sha256': manifest_hash, 'current_snapshot_sha256': current_ref['sha256'],
            'training_performed': False, 'forecast_performed': False,
            'provenance_limit': 'Hashes are integrity checks; approved acquisition provenance must be reviewed separately.'}


def audit_bundle(root, manifest_hash, scope, current_ref):
    try:
        return check_bundle(root, manifest_hash, scope, current_ref)
    except (EvidenceError, KeyError, TypeError, ValueError, OSError) as exc:
        return {'status': 'BLOCKED_SOURCE_EVIDENCE',
                'reason': str(exc) if isinstance(exc, EvidenceError) else type(exc).__name__,
                'training_performed': False, 'forecast_performed': False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for flag in ('root', 'manifest-sha256', 'scope', 'current-ref', 'out'):
        p.add_argument('--' + flag, required=True)
    a = p.parse_args()
    scope = json.loads(Path(a.scope).read_text(encoding='utf-8'))
    ref = json.loads(Path(a.current_ref).read_text(encoding='utf-8'))
    result = audit_bundle(a.root, a.manifest_sha256, scope, ref)
    with Path(a.out).open('x', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(result['status'])


if __name__ == '__main__':
    main()
