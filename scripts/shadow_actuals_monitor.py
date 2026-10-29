"""Immutable manual actuals monitoring; no model loading, inference, DB or API client."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

KEY = ['shop', 'spu']
WINDOWS = {'d01_30': 'W1', 'd31_60': 'W2', 'd01_60': 'W60'}
CANDIDATE = 'BLEND50_L1_MEAN_V1'
SHOPS = {'JQ-US', 'MT-US', 'RKZ-US', 'SY-US'}
KNOWN = {'OBSERVED', 'EXPLICIT_ZERO'}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def write_new(path, value):
    with Path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False, default=str)
        stream.write('\n')


def instant(now=None):
    t = pd.Timestamp(now if now is not None else dt.datetime.now(dt.timezone.utc))
    if t.tz is None:
        raise ValueError('TIMEZONE_REQUIRED')
    return t.tz_convert('UTC')


def no_duplicates(frame, keys, label):
    if frame[keys].isna().any().any() or frame.duplicated(keys).any():
        raise ValueError('DUPLICATE_OR_UNKNOWN_' + label)


def load_run(run_root):
    root = Path(run_root).resolve()
    seal = read(root / 'issued/issue_seal.json')
    if seal['status'] != 'ISSUED_SHADOW_AWAITING_ACTUALS':
        raise ValueError('NOT_SEALED')
    for name, digest in seal['files'].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root) or sha(path) != digest:
            raise ValueError('SEALED_FILE_CHANGED:' + name)
    s = pd.Timestamp(seal['issue_date'])
    for w, (lo, hi) in {'W1': (1, 30), 'W2': (31, 60), 'W60': (1, 60)}.items():
        if list(seal['windows'][w]) != [str((s + pd.Timedelta(days=n)).date()) for n in [lo, hi]]:
            raise ValueError('WINDOW_CHANGED')
    core = pd.read_parquet(root / 'prepared/core.parquet')
    pred = pd.read_parquet(root / 'issued/predictions.parquet')
    decline = pd.read_parquet(root / 'prepared/decline.parquet')
    no_duplicates(core, KEY, 'CORE'); no_duplicates(decline, KEY, 'DECLINE')
    no_duplicates(pred, KEY + ['model', 'window'], 'PREDICTION')
    if set(core.shop) != SHOPS or not pred.run_id.eq(seal['run_id']).all():
        raise ValueError('BATCH_OR_SHOPS')
    if not pred.issue_date.eq(seal['issue_date']).all() or not pred.input_cutoff.eq(seal['input_cutoff']).all():
        raise ValueError('BATCH_DATE')
    keys = set(core[KEY].itertuples(index=False, name=None))
    if set(decline[KEY].itertuples(index=False, name=None)) != keys:
        raise ValueError('DECLINE_SCOPE')
    models = set(pred.model)
    if CANDIDATE not in models:
        raise ValueError('MISSING_CANDIDATE')
    for (model, window), g in pred.groupby(['model', 'window']):
        if window not in WINDOWS or set(g[KEY].itertuples(index=False, name=None)) != keys:
            raise ValueError('PREDICTION_SCOPE')
        start, end = seal['windows'][WINDOWS[window]]
        if not g.target_start.eq(start).all() or not g.target_end.eq(end).all():
            raise ValueError('PREDICTION_WINDOW')
    if len(pred) != len(core) * len(models) * len(WINDOWS):
        raise ValueError('MISSING_MODEL_WINDOW')
    vals = pred.prediction.to_numpy(float)
    if np.isinf(vals).any() or (vals[np.isfinite(vals)] < 0).any():
        raise ValueError('INVALID_PREDICTION')
    if not pred.prediction.notna().eq(pred.prediction_status.eq('OK')).all():
        raise ValueError('PREDICTION_STATUS')
    pivot = pred.pivot(index=KEY + ['model'], columns='window', values='prediction')
    if not np.array_equal(pivot.d01_60, pivot.d01_30 + pivot.d31_60, equal_nan=True):
        raise ValueError('CUMULATIVE_CHANGED')
    return seal, core, pred, decline


def _daily(frame, kind):
    columns = ['ds', 'shop', 'y', 'source_state'] + ([] if kind == 'STORE' else ['spu', 'sales_state'])
    if frame is None:
        return pd.DataFrame(columns=columns)
    q = frame.copy()
    if set(columns) - set(q):
        raise ValueError('DAILY_SCHEMA_' + kind)
    q['ds'] = pd.to_datetime(q.ds, errors='raise')
    if q.ds.isna().any() or not q.ds.eq(q.ds.dt.normalize()).all():
        raise ValueError('DAILY_DATE')
    no_duplicates(q, ['ds', 'shop'] + ([] if kind == 'STORE' else ['spu']), kind)
    if not q.shop.isin(SHOPS).all():
        raise ValueError('DAILY_SHOP')
    q['y'] = pd.to_numeric(q.y, errors='raise')
    if np.isinf(q.y).any() or q.y.lt(0).any():
        raise ValueError('INVALID_ACTUAL')
    if kind != 'STORE':
        known = q.sales_state.isin(KNOWN)
        if (known & q.y.isna()).any() or (q.sales_state.eq('EXPLICIT_ZERO') & q.y.ne(0)).any():
            raise ValueError('ACTUAL_STATE')
    return q


def window_truth(core, seal, now, core_daily=None, shop_daily=None):
    """Only complete calendar windows become truth. Observed partial units are separate."""
    now = instant(now); cutoff = pd.Timestamp(now.tz_convert('Asia/Shanghai').date()) - pd.Timedelta(days=2)
    daily, stores = _daily(core_daily, 'CORE'), _daily(shop_daily, 'STORE')
    start60, end60 = map(pd.Timestamp, seal['windows']['W60'])
    for frame in [daily, stores]:
        if not frame.empty and (frame.ds.lt(start60) | frame.ds.gt(min(cutoff, end60))).any():
            raise ValueError('FUTURE_OR_OUTSIDE_TARGET_ACTUAL')
    keys = set(core[KEY].itertuples(index=False, name=None))
    if set(daily[KEY].itertuples(index=False, name=None)) - keys:
        raise ValueError('CORE_LIST_CHANGED')
    rows, statuses, coverage = [], [], []
    for w, sw in WINDOWS.items():
        start, end = map(pd.Timestamp, seal['windows'][sw])
        n = (end - start).days + 1; through = min(end, cutoff)
        dates = pd.date_range(start, through) if through >= start else pd.DatetimeIndex([])
        d = daily[daily.ds.isin(dates)]; st = stores[stores.ds.isin(dates)]
        expected = pd.MultiIndex.from_product([dates, sorted(SHOPS)], names=['ds', 'shop'])
        si = st.set_index(['ds', 'shop']).reindex(expected)
        store_good = si.source_state.eq('SOURCE_READY') & si.y.notna()
        all_store_known = len(expected) > 0 and bool(store_good.all())
        calendar_mature = cutoff >= end
        complete_store_window = calendar_mature and len(si) == n * len(SHOPS) and all_store_known
        per_object = []
        for shop, spu in core[KEY].itertuples(index=False, name=None):
            g = d[d.shop.eq(shop) & d.spu.eq(spu)].set_index('ds').reindex(dates)
            same_store = si.loc[(slice(None), shop), :].droplevel('shop').reindex(dates) if len(dates) else si.iloc[:0]
            known = g.sales_state.isin(KNOWN) & g.y.notna() & g.source_state.eq('SOURCE_READY')
            if len(dates):
                known &= same_store.source_state.eq('SOURCE_READY') & same_store.y.notna()
            completed = int(known.sum())
            complete = calendar_mature and complete_store_window and completed == n
            state = ('COMPLETE' if complete else 'NOT_MATURED' if cutoff < end else 'SOURCE_INCOMPLETE')
            if not len(dates):
                state = 'NOT_STARTED'
            per_object.append(dict(run_id=seal['run_id'], issue_date=seal['issue_date'], shop=shop, spu=spu, window=w,
                target_start=str(start.date()), target_end=str(end.date()), actual=float(g.y.sum()) if complete else np.nan,
                actual_status=state, eligible_days=len(dates), archived_complete_days=completed,
                observed_units=float(g.loc[known, 'y'].sum()) if completed else np.nan,
                unknown_or_missing_eligible_days=len(dates)-completed,
                observed_through=str(through.date()) if len(dates) else None))
        all_core_known = all(r['actual_status'] == 'COMPLETE' for r in per_object)
        official = calendar_mature and complete_store_window and all_core_known
        if not dates.size:
            window_state = 'WAITING_TARGET_START'
        elif not all_store_known or any(r['unknown_or_missing_eligible_days'] for r in per_object):
            window_state = 'SOURCE_INCOMPLETE'
        elif not calendar_mature:
            window_state = 'NOT_MATURED'
        else:
            window_state = 'MATURE_COMPLETE' if official else 'SOURCE_INCOMPLETE'
        rows.extend(per_object)
        statuses.append(dict(window=w, target_start=str(start.date()), target_end=str(end.date()),
            earliest_maturity_check=str((end + pd.Timedelta(days=2)).date()), state=window_state,
            eligible_days=len(dates), expected_days=n, fully_archived_shop_days=int(store_good.sum()),
            expected_eligible_shop_days=len(expected), core_objects=len(core),
            complete_core_objects=sum(r['actual_status'] == 'COMPLETE' for r in per_object),
            official_score_allowed=official))
        coverage.append(dict(window=w, status='COMPLETE' if official else window_state,
            full_store_actual=float(si.y.sum()) if complete_store_window else np.nan,
            observed_store_units=float(si.loc[store_good, 'y'].sum()) if store_good.any() else np.nan,
            core_actual=float(sum(r['actual'] for r in per_object)) if official else np.nan,
            observed_store_days=int(store_good.sum())))
    return pd.DataFrame(rows), pd.DataFrame(statuses), pd.DataFrame(coverage)


def fixed_paired(pred):
    p = pred[pred.model.eq(CANDIDATE)]
    z = p.groupby(KEY).prediction.apply(lambda s: len(s) == len(WINDOWS) and s.notna().all())
    return z[z].reset_index()[KEY]


def journal_events(journal):
    if journal is None or not Path(journal).exists():
        return []
    events = []
    for path in sorted(Path(journal).glob('*.json')):
        value = read(path)
        whole = {k: v for k, v in value.items() if k != 'event_sha256'}
        if value.get('event_sha256') != hashlib.sha256(json.dumps(whole, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()).hexdigest():
            raise ValueError('REVIEW_METADATA_TAMPERED')
        if value.get('record_sha256') != hashlib.sha256(json.dumps(value['record'], sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest():
            raise ValueError('REVIEW_EVENT_TAMPERED')
        events.append(value)
    ids = {e['event_id']: e for e in events}
    if len(ids) != len(events):
        raise ValueError('DUPLICATE_REVIEW_EVENT')
    for e in events:
        prev = e.get('supersedes_event_id')
        if prev and (prev not in ids or ids[prev]['record_sha256'] != e['supersedes_record_sha256']):
            raise ValueError('REVIEW_HISTORY_LINK')
        if prev:
            if tuple(ids[prev]['record'][k] for k in ['run_id','shop','spu','window']) != tuple(e['record'][k] for k in ['run_id','shop','spu','window']):
                raise ValueError('REVIEW_LINK_WRONG_OBJECT')
            if instant(ids[prev]['ingested_at']) > instant(e['ingested_at']):
                raise ValueError('REVIEW_LINK_REVERSED')
    previous = [e['supersedes_event_id'] for e in events if e.get('supersedes_event_id')]
    if len(previous) != len(set(previous)):
        raise ValueError('REVIEW_HISTORY_FORK')
    return sorted(events, key=lambda e: (e['ingested_at'], e['event_id']))


def _append_review_locked(run_root, journal, record_path, now=None):
    seal, core, pred, decline = load_run(run_root)
    record = read(record_path); now = instant(now)
    allowed = {'run_id', 'shop', 'spu', 'window', 'original_candidate', 'manual_reference_demand',
               'adjustment_reason', 'evidence', 'owner', 'recorded_at', 'confirmed_at',
               'planned_effective_at', 'actual_occurred_at', 'actual_occurrence', 'supersedes_event_id'}
    if set(record) - allowed or record.get('run_id') != seal['run_id']:
        raise ValueError('MANUAL_SCHEMA_OR_RUN')
    p = pred[pred.model.eq(CANDIDATE) & pred.shop.eq(record.get('shop')) & pred.spu.eq(record.get('spu')) & pred.window.eq(record.get('window'))]
    if len(p) != 1:
        raise ValueError('MANUAL_OBJECT')
    original = p.prediction.iloc[0]; supplied = record.get('original_candidate')
    if not (pd.isna(original) and supplied is None) and original != supplied:
        raise ValueError('ORIGINAL_PREDICTION_CHANGED')
    value = record.get('manual_reference_demand')
    if isinstance(value, bool) or value is None or not np.isfinite(value) or value < 0:
        raise ValueError('INVALID_MANUAL_VALUE')
    if any(not str(record.get(k, '')).strip() for k in ['owner', 'adjustment_reason', 'evidence']):
        raise ValueError('MANUAL_EVIDENCE_REQUIRED')
    for field in ['recorded_at', 'confirmed_at', 'actual_occurred_at']:
        if record.get(field) and instant(record[field]) > now:
            raise ValueError('FUTURE_MANUAL_EVENT_TIME')
    if record.get('planned_effective_at'):
        instant(record['planned_effective_at'])
    # The submitted timestamp is a declaration, never the audit clock.
    record['declared_recorded_at'] = record.pop('recorded_at', None)
    record['recorded_at'] = now.isoformat()
    old = journal_events(journal); key = tuple(record[k] for k in ['run_id', 'shop', 'spu', 'window'])
    same = [e for e in old if tuple(e['record'][k] for k in ['run_id', 'shop', 'spu', 'window']) == key]
    previous = record.pop('supersedes_event_id', None)
    if bool(same) != bool(previous) or (same and same[-1]['event_id'] != previous):
        raise ValueError('EXPLICIT_REVIEW_REVISION_REQUIRED')
    if old and now < instant(old[-1]['ingested_at']):
        raise ValueError('REVIEW_CLOCK_REVERSED')
    event = dict(event_id=str(uuid.uuid4()), ingested_at=now.isoformat(), record=record,
        record_sha256=hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest(),
        supersedes_event_id=previous, supersedes_record_sha256=same[-1]['record_sha256'] if same else None,
        timing='POST_ISSUE_RECORD_NOT_FAIR_HEAD_TO_HEAD', approval_by_codex=False)
    event['event_sha256'] = hashlib.sha256(json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    folder = Path(journal).resolve()
    write_new(folder / (now.strftime('%Y%m%dT%H%M%S%fZ') + '_' + event['event_id'] + '.json'), event)
    return event


def append_review(run_root, journal, record_path, now=None):
    folder = Path(journal).resolve()
    if folder.is_relative_to(Path(run_root).resolve()):
        raise ValueError('JOURNAL_MUST_BE_SEPARATE')
    folder.mkdir(exist_ok=True, parents=True)
    lock = folder / '.writer.lock'
    token = str(uuid.uuid4())
    # An unresolved lock is an explicit blocker, never automatically removed.
    with lock.open('x', encoding='utf-8') as stream:
        stream.write(token)
    try:
        return _append_review_locked(run_root, folder, record_path, now)
    finally:
        if lock.read_text(encoding='utf-8') == token:
            lock.unlink()


def review_status(pred, run_id, journal=None):
    p = pred[pred.model.eq(CANDIDATE)][KEY + ['window', 'prediction']].copy()
    p['run_id'] = run_id; p['review_state'] = 'PENDING'; p['event_id'] = None
    p['recorded_at'] = None; p['manual_reference_demand'] = np.nan
    for e in journal_events(journal):
        r = e['record']
        if r['run_id'] != run_id:
            raise ValueError('OTHER_BATCH_REVIEW')
        mask = p.shop.eq(r['shop']) & p.spu.eq(r['spu']) & p.window.eq(r['window'])
        if mask.sum() != 1:
            raise ValueError('UNKNOWN_REVIEW_OBJECT')
        p.loc[mask, ['review_state', 'event_id', 'recorded_at', 'manual_reference_demand']] = [
            'RECORDED_NOT_PROCUREMENT_APPROVAL', e['event_id'], e['ingested_at'], r['manual_reference_demand']]
    return p


def run_monitor(run_root, out, source_package=None, review_template=None, journal=None, now=None):
    t = instant(now); root, out = Path(run_root).resolve(), Path(out).resolve()
    if out.is_relative_to(root):
        raise ValueError('OUTPUT_MUST_BE_SEPARATE')
    seal, core, pred, decline = load_run(root)
    source_hashes = {str(root / 'issued/issue_seal.json'): sha(root / 'issued/issue_seal.json')}
    out.mkdir(parents=True, exist_ok=False)
    cutoff = t.tz_convert('Asia/Shanghai').date() - dt.timedelta(days=2)
    waiting = cutoff < dt.date.fromisoformat(seal['windows']['W60'][0])
    source_info = {}; daily = stores = None
    if not waiting and source_package is not None:
        try:
            from scripts.shadow_actuals_source import verify_package
        except ModuleNotFoundError:
            from shadow_actuals_source import verify_package
        contract = dict(run_id=seal['run_id'], issue_seal_sha256=sha(root/'issued/issue_seal.json'),
            core_sha256=sha(root/'prepared/core.parquet'), target_start=seal['windows']['W60'][0],
            target_end=seal['windows']['W60'][1])
        verified = verify_package(Path(source_package), seal['run_id'], core, expected_contract=contract)
        daily, stores, source_info = verified['core_daily'], verified['shop_daily'], verified['manifest']
        if any(source_info.get(k) != v for k,v in contract.items()):
            raise ValueError('SOURCE_PACKAGE_NOT_BOUND_TO_SEALED_RUN')
        source_hashes[str(Path(source_package) / 'manifest.json')] = sha(Path(source_package) / 'manifest.json')
    truth, status, cover = window_truth(core, seal, t, daily, stores)
    paired = fixed_paired(pred)
    truth['paired'] = pd.MultiIndex.from_frame(truth[KEY]).isin(pd.MultiIndex.from_frame(paired))
    for i, r in cover.iterrows():
        q = truth[truth.window.eq(r.window)]
        allowed = r.status == 'COMPLETE'
        cover.loc[i, 'output_actual'] = float(q.loc[q.paired, 'actual'].sum()) if allowed else np.nan
        for field in ['core', 'output']:
            cover.loc[i, field + '_future_sales_coverage'] = cover.loc[i, field + '_actual'] / r.full_store_actual if allowed and r.full_store_actual > 0 else np.nan
    scores = {}
    if status.official_score_allowed.any():
        try:
            from scripts.shadow_actuals_scoring import score_windows
        except ModuleNotFoundError:
            from shadow_actuals_scoring import score_windows
        pp = pred.drop(columns=['issue_date'])
        tt = truth.drop(columns=['issue_date'])
        scores = score_windows(pp, tt, paired, full_store_totals=dict(zip(cover.window, cover.full_store_actual)))
        for name, frame in scores.items():
            frame.to_csv(out / ('scores_' + name + '.csv'), index=False, encoding='utf-8-sig')
    reviews = review_status(pred, seal['run_id'], journal)
    if review_template is not None:
        template = pd.read_csv(review_template, float_precision='round_trip')
        no_duplicates(template, ['run_id'] + KEY + ['window'], 'REVIEW_TEMPLATE')
        match = template.merge(pred[pred.model.eq(CANDIDATE)][['run_id'] + KEY + ['window', 'prediction']], on=['run_id']+KEY+['window'], how='outer', validate='one_to_one', indicator=True)
        if not match['_merge'].eq('both').all() or not np.array_equal(match.original_candidate, match.prediction, equal_nan=True):
            raise ValueError('REVIEW_TEMPLATE_CHANGED')
        source_hashes[str(Path(review_template).resolve())] = sha(review_template)
    display = pred.merge(truth.drop(columns=['run_id', 'issue_date', 'target_start', 'target_end']), on=KEY+['window'], validate='many_to_one')
    # No partial-day/full-month error, prorated forecast, or zero placeholder is produced.
    display['score_state'] = display.actual_status.map({'COMPLETE':'MATURE_READY', 'NOT_STARTED':'待目标期开始', 'NOT_MATURED':'待成熟', 'SOURCE_INCOMPLETE':'来源未完整'})
    display.to_csv(out / 'monitoring_status.csv', index=False, encoding='utf-8-sig')
    truth.to_csv(out / 'window_truth_status.csv', index=False, encoding='utf-8-sig')
    status.to_csv(out / 'window_status.csv', index=False, encoding='utf-8-sig')
    cover.to_csv(out / 'future_coverage.csv', index=False, encoding='utf-8-sig')
    reviews.to_csv(out / 'manual_review_status.csv', index=False, encoding='utf-8-sig')
    pairfile = out / 'frozen_paired_keys.csv'; paired.to_csv(pairfile, index=False, encoding='utf-8-sig')
    if daily is not None:
        daily.to_parquet(out / 'archived_core_daily.parquet', index=False)
        stores.to_parquet(out / 'archived_shop_daily.parquet', index=False)
    overall = ('WAITING_TARGET_START' if waiting else 'SOURCE_INCOMPLETE' if status.state.eq('SOURCE_INCOMPLETE').any()
        else 'ALL_WINDOWS_SCORED' if status.official_score_allowed.all() else 'PARTIALLY_MATURED' if status.official_score_allowed.any() else 'TRACKING_NOT_MATURED')
    receipt = dict(status=overall, executed_at=t.isoformat(), server_business_date=str(t.tz_convert('Asia/Shanghai').date()),
        acquisition_eligible_cutoff=str(cutoff), run_id=seal['run_id'], fixed_core_objects=len(core), fixed_paired_objects=len(paired),
        fixed_missing_candidate_objects=len(core)-len(paired), windows=seal['windows'], source_package=source_info,
        actually_archived_dates=sorted(pd.to_datetime(daily.ds).dt.strftime('%Y-%m-%d').unique().tolist()) if daily is not None and len(daily) else [],
        official_scored_windows=status.loc[status.official_score_allowed, 'window'].tolist(), actual_scores_generated=bool(scores),
        manual_review_records_completed=int(reviews.review_state.ne('PENDING').sum()), manual_review_records_pending=int(reviews.review_state.eq('PENDING').sum()),
        manual_review_objects_completed=int(reviews.loc[reviews.review_state.ne('PENDING'),KEY].drop_duplicates().shape[0]),
        review_template_reference=str(review_template) if review_template else None, sources=source_hashes,
        new_fits=0, model_inference_calls=0, database_queries=0, api_calls=0, production_writes=0,
        enabled_automations=0, original_prediction_modified=False, score_code_sha256=sha(Path(__file__).with_name('shadow_actuals_scoring.py')),
        script_sha256=sha(__file__), limitations=['No future truth is fabricated.', 'Historical scoring validation is TEST_ONLY, not this batch performance.', 'Manual judgments are separate time-stamped references, not procurement orders or fair earlier predictions.'])
    for p, h in source_hashes.items():
        if sha(p) != h:
            raise ValueError('SOURCE_CHANGED_DURING_MONITOR')
    for name, digest in seal['files'].items():
        if sha(root / name) != digest:
            raise ValueError('SEALED_RUN_CHANGED')
    write_new(out / 'manual_run_receipt.json', receipt)
    write_new(out / 'manifest.json', dict(status=overall, run_id=seal['run_id'], created_at=dt.datetime.now(dt.timezone.utc).isoformat(), files={p.name:sha(p) for p in out.iterdir() if p.is_file()}, sources=source_hashes))
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__); sub = p.add_subparsers(dest='command', required=True)
    r = sub.add_parser('run'); r.add_argument('--run-root', required=True); r.add_argument('--out', required=True)
    r.add_argument('--source-package'); r.add_argument('--review-template'); r.add_argument('--journal')
    j = sub.add_parser('append-review'); j.add_argument('--run-root', required=True); j.add_argument('--journal', required=True); j.add_argument('--record', required=True)
    a = p.parse_args()
    if a.command == 'run':
        result = run_monitor(a.run_root,a.out,a.source_package,a.review_template,a.journal)
        print(json.dumps({k:result[k] for k in ['status','executed_at','acquisition_eligible_cutoff','fixed_core_objects','fixed_paired_objects','actual_scores_generated','official_scored_windows']},ensure_ascii=False))
    else:
        result = append_review(a.run_root,a.journal,a.record)
        print(json.dumps({'event_id':result['event_id'],'ingested_at':result['ingested_at'],'approval_by_codex':False}))


if __name__ == '__main__':
    main()
