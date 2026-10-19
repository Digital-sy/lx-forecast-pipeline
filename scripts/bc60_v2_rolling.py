"""Frozen-list daily rolling experiment using authorized local V1 artifacts only."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

VALID_STATES = {'OBSERVED', 'EXPLICIT_ZERO'}
WINDOWS = {'d01_30': (1, 30), 'd31_60': (31, 60), 'd01_60': (1, 60)}
EPS = 1e-8


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save_json(path, data):
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def iso(value):
    return pd.Timestamp(value).strftime('%Y-%m-%d')


def information_dates(issue, update_offset, model):
    issue = pd.Timestamp(issue)
    offset = 0 if model == 'F28' else (update_offset // 7 * 7 if model == 'R28-W' else update_offset)
    estimate = issue + pd.Timedelta(days=offset)
    return estimate, estimate - pd.Timedelta(days=2)


def target_dates(issue, update_offset, track, window='remaining'):
    issue = pd.Timestamp(issue)
    update = issue + pd.Timedelta(days=update_offset)
    if track == 'T1':
        return update + pd.Timedelta(days=1), issue + pd.Timedelta(days=60)
    a, b = WINDOWS[window]
    return update + pd.Timedelta(days=a), update + pd.Timedelta(days=b)


class CalendarStore:
    """Missing dates remain unknown; numeric prefix sums are used only behind masks."""

    def __init__(self, daily, source_sha):
        self.source_sha = source_sha
        self.first = pd.Timestamp(daily.ds.min())
        self.last = pd.Timestamp(daily.ds.max())
        self.dates = pd.date_range(self.first, self.last)
        self.data = {}
        self.rate_cache = {}
        for key, group in daily.groupby(['shop', 'spu']):
            aligned = group.set_index('ds').reindex(self.dates)
            y = aligned.y.to_numpy(dtype=float)
            states = aligned.sales_state.fillna('MISSING').astype(str).to_numpy()
            known = np.isfinite(y) & (y >= 0) & np.isin(states, list(VALID_STATES))
            self.data[key] = (y, states, np.r_[0., np.cumsum(np.where(known, y, 0.))],
                              np.r_[0, np.cumsum(~known)])

    def total(self, key, start, end):
        start, end = pd.Timestamp(start), pd.Timestamp(end)
        if end > self.last:
            return None, 'NOT_MATURED'
        if start < self.first or end < start or key not in self.data:
            return None, 'UNKNOWN_TRUTH'
        a, b = (start - self.first).days, (end - self.first).days + 1
        _, _, totals, missing = self.data[key]
        if missing[b] != missing[a]:
            return None, 'UNKNOWN_TRUTH'
        return float(totals[b] - totals[a]), 'OBSERVED'

    def mean28(self, key, cutoff):
        cutoff = pd.Timestamp(cutoff)
        cache_key = (*key, iso(cutoff))
        if cache_key in self.rate_cache:
            return self.rate_cache[cache_key]
        start = cutoff - pd.Timedelta(days=27)
        values = []
        for day in pd.date_range(start, cutoff):
            idx = (day - self.first).days
            if key not in self.data or idx < 0 or idx >= len(self.dates):
                value, state = None, 'MISSING'
            else:
                y, states, _, _ = self.data[key]
                value = float(y[idx]) if np.isfinite(y[idx]) else None
                state = str(states[idx])
            values.append([iso(day), value, state])
        payload = {'version': 'bc60-v2-history-v1-canonical-json', 'source_sha256': self.source_sha,
                   'shop': key[0], 'spu': key[1], 'history_start': iso(start),
                   'history_end': iso(cutoff), 'days': values}
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                         separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()
        valid = all(v is not None and v >= 0 and s in VALID_STATES for _, v, s in values)
        result = (sum(x[1] for x in values) / 28 if valid else None,
                  'OK' if valid else 'MISSING_INPUT', digest)
        self.rate_cache[cache_key] = result
        return result


def score_row(actual, prediction, truth_status, prediction_status):
    known = actual is not None and np.isfinite(actual)
    output = prediction is not None and np.isfinite(prediction)
    relative = ((prediction - actual) / actual) if known and output and actual > 0 else None
    error = (prediction - actual) if known and output else None
    decision = 'UNJUDGED' if relative is None else ('HIT' if abs(relative) <= .2 else 'MISS')
    status = truth_status if truth_status != 'OBSERVED' else (prediction_status if not output else ('ZERO_ACTUAL' if actual == 0 else 'OK'))
    return {'actual': actual, 'prediction': prediction, 'truth_status': truth_status,
            'prediction_status': prediction_status, 'status': status, 'decision': decision,
            'relative_error': relative, 'absolute_error': abs(error) if error is not None else None}


def aggregate_metrics(group):
    actual_known = group.actual.notna()
    output = group.prediction.notna()
    valid = actual_known & output
    z = group[valid]
    error = z.prediction - z.actual
    total = float(z.actual.sum())
    all_known = float(group.actual.sum())
    positive = group.actual.gt(0)
    hits = group.decision.eq('HIT')
    return {'fixed_core_n': len(group), 'output_n': int(output.sum()),
            'truth_n': int(actual_known.sum()), 'scored_n': len(z),
            'positive_truth_n': int(positive.sum()), 'hit_n': int(hits.sum()),
            'miss_n': int(group.decision.eq('MISS').sum()), 'unjudged_n': int(group.decision.eq('UNJUDGED').sum()),
            'hit_rate': float(hits.sum() / positive.sum()) if positive.any() else None,
            'full_core_hit_share': float(hits.mean()),
            'hit_sales_share': float(group.loc[hits, 'actual'].sum() / all_known) if all_known > 0 else None,
            'known_fixed_core_actual_units': all_known, 'actual_units': total,
            'forecast_units': float(z.prediction.sum()),
            'wape': float(error.abs().sum() / total) if total > 0 else None,
            'bias': float(error.sum() / total) if total > 0 else None,
            'over_units': float(error.clip(lower=0).sum()), 'under_units': float((-error).clip(lower=0).sum()),
            'zero_actual_n': int(group.actual.eq(0).sum()),
            'zero_actual_absolute_error': float(z.loc[z.actual.eq(0), 'absolute_error'].sum()),
            'not_matured_n': int(group.truth_status.eq('NOT_MATURED').sum()),
            'unknown_truth_n': int(group.truth_status.eq('UNKNOWN_TRUTH').sum()),
            'missing_output_n': int((~output).sum())}


def scopes(frame):
    yield 'full_core', frame
    yield 'original_b1_369', frame[frame.b1_original_eligible]
    if frame.track.iloc[0] == 'T2':
        yield 't2_balanced_279', frame[frame.original_issue_date.isin(['2026-04-02', '2026-06-02'])]


def summarize(frame):
    result = []
    base = ['track', 'update_offset', 'window', 'model']
    for scope, subset in scopes(frame):
        for label, extra in [('overall', []), ('shop', ['shop']), ('issue', ['original_issue_date']),
                             ('lifecycle', ['lifecycle']), ('maturity', ['maturity'])]:
            keys = base + extra
            for values, group in subset.groupby(keys, dropna=False, sort=True):
                result.append({**dict(zip(keys, values)), 'scope': scope, 'slice': label, **aggregate_metrics(group)})
    return pd.DataFrame(result)


def pairwise(frame):
    keys = ['original_issue_date', 'update_date', 'update_offset', 'shop', 'spu', 'window', 'track',
            'target_start', 'target_end', 'window_days', 'b1_original_eligible', 'lifecycle', 'maturity']
    comparisons = [('F28', 'R28-D'), ('F28', 'R28-W'), ('R28-W', 'R28-D')] if frame.track.iloc[0] == 'T1' else [('R28-W', 'R28-D')]
    detail = []
    for before, after in comparisons:
        a = frame[frame.model.eq(before)][keys + ['actual', 'prediction', 'decision']]
        b = frame[frame.model.eq(after)][keys + ['actual', 'prediction', 'decision']]
        g = a.merge(b, on=keys, validate='one_to_one', suffixes=('_before', '_after'))
        assert (g.actual_before.fillna(-1) == g.actual_after.fillna(-1)).all()
        g['actual'] = g.actual_before
        g['comparison'] = before + '->' + after
        g['error_before'] = g.prediction_before - g.actual
        g['error_after'] = g.prediction_after - g.actual
        g['delta_absolute_error'] = g.error_after.abs() - g.error_before.abs()
        g['change'] = np.select([g.delta_absolute_error.isna(), g.delta_absolute_error.lt(-EPS),
                                 g.delta_absolute_error.gt(EPS)], ['UNJUDGED', 'IMPROVED', 'DEGRADED'], default='UNCHANGED')
        detail.append(g)
    detail = pd.concat(detail, ignore_index=True)
    rows = []
    for scope, subset in scopes(detail):
        for label, extra in [('overall', []), ('shop', ['shop']), ('issue', ['original_issue_date']),
                             ('lifecycle', ['lifecycle']), ('maturity', ['maturity'])]:
            keys2 = ['track', 'update_offset', 'window', 'comparison'] + extra
            for values, group in subset.groupby(keys2, dropna=False):
                z = group[group.change.ne('UNJUDGED')]
                total = float(z.actual.sum())
                rows.append({**dict(zip(keys2, values)), 'scope': scope, 'slice': label,
                             'fixed_core_n': len(group), 'paired_n': len(z), 'unjudged_n': len(group)-len(z),
                             'paired_actual_units': total,
                             'delta_wape': float(z.delta_absolute_error.sum()/total) if total > 0 else None,
                             'delta_over_units': float(z.error_after.clip(lower=0).sum()-z.error_before.clip(lower=0).sum()),
                             'delta_under_units': float((-z.error_after).clip(lower=0).sum()-(-z.error_before).clip(lower=0).sum()),
                             'improved_n': int(z.change.eq('IMPROVED').sum()), 'degraded_n': int(z.change.eq('DEGRADED').sum()),
                             'unchanged_n': int(z.change.eq('UNCHANGED').sum()),
                             'improved_sales_weight': float(z.loc[z.change.eq('IMPROVED'),'actual'].sum()/total) if total > 0 else None,
                             'degraded_sales_weight': float(z.loc[z.change.eq('DEGRADED'),'actual'].sum()/total) if total > 0 else None,
                             'delta_hit_n': int(z.decision_after.eq('HIT').sum()-z.decision_before.eq('HIT').sum())})
    return detail, pd.DataFrame(rows)


def verify_archive(archive, config):
    assert sha(archive/'inputs_v1_final/daily.parquet') == config['source_daily_sha256']
    assert sha(archive/'effects_v1/core_lists.csv') == config['core_sha256']
    original = json.loads((archive/'effects_v1/run_manifest.json').read_text(encoding='utf-8'))
    for name, expected in original['files'].items():
        assert sha(archive/'effects_v1'/name) == expected, name
    for name, expected in original['inputs'].items():
        assert sha(archive/'inputs_v1_final'/name) == expected, name
    return original


def run(archive, config_path, out):
    archive, out = Path(archive).resolve(), Path(out).resolve()
    config = json.loads(Path(config_path).read_text(encoding='utf-8'))
    original = verify_archive(archive, config)
    out.mkdir(parents=True, exist_ok=False)
    start = time.monotonic()
    manifest = {'started_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'status': 'RUNNING',
                'source_sha256': sha(__file__), 'config_sha256': sha(config_path),
                'input_daily_sha256': config['source_daily_sha256'], 'core_sha256': config['core_sha256'],
                'classification': config['classification'], 'new_database_queries': 0, 'model_fits': 0}
    save_json(out/'run_manifest.json', manifest)
    core = pd.read_csv(archive/'effects_v1/core_lists.csv')
    original_scores = pd.read_parquet(archive/'effects_v1/scores.parquet')
    eligible = set(map(tuple, original_scores[(original_scores.model=='B1') & original_scores.prediction.notna()]
                       [['issue_date','shop','spu']].drop_duplicates().to_numpy()))
    core['b1_original_eligible'] = [tuple(x) in eligible for x in core[['issue_date','shop','spu']].to_numpy()]
    assert len(core)==385 and core.b1_original_eligible.sum()==369
    daily = pd.read_parquet(archive/'inputs_v1_final/daily.parquet')
    core_keys = set(map(tuple, core[['shop','spu']].drop_duplicates().to_numpy()))
    daily = daily[[tuple(x) in core_keys for x in daily[['shop','spu']].to_numpy()]].copy()
    calendar = CalendarStore(daily, config['source_daily_sha256'])
    oldf = pd.read_parquet(archive/'effects_v1/daily_forecasts.parquet')
    oldf['ds'] = pd.to_datetime(oldf.ds)
    originals = {(i,s,p): g.set_index('ds').forecast for (i,s,p),g in oldf[oldf.model.eq('rolling28')].groupby(['issue_date','shop','spu'])}
    checks = []
    for row in core.itertuples():
        rate, status, digest = calendar.mean28((row.shop,row.spu), pd.Timestamp(row.issue_date)-pd.Timedelta(days=2))
        series = originals[(row.issue_date,row.shop,row.spu)]
        difference = float((series-rate).abs().max()) if rate is not None else None
        checks.append({'issue_date':row.issue_date,'shop':row.shop,'spu':row.spu,'rate_status':status,
                       'original_daily_max_difference':difference,'input_hash':digest})
    check = pd.DataFrame(checks)
    check.to_csv(out/'initial_baseline_checks.csv',index=False,encoding='utf-8-sig')
    if check.original_daily_max_difference.isna().any() or check.original_daily_max_difference.gt(EPS).any():
        manifest.update(status='BLOCKED_INITIAL_BASELINE_MISMATCH');save_json(out/'run_manifest.json',manifest)
        raise ValueError('Do not mix discrepant initial baseline with update gains')
    core.to_csv(out/'fixed_core.csv',index=False,encoding='utf-8-sig')
    (out/'daily_vintages').mkdir()
    t1, t2, rates = [], [], []
    t0 = []
    coremeta = core.set_index(['issue_date','shop','spu'])
    for row in original_scores[original_scores.model.isin(['rolling28','weekday4','B1'])].itertuples():
        issue = pd.Timestamp(row.issue_date)
        m = coremeta.loc[(row.issue_date,row.shop,row.spu)]
        a,b = target_dates(issue,0,'T0',row.window)
        pred = None if pd.isna(row.prediction) else float(row.prediction)
        actual = None if pd.isna(row.actual) else float(row.actual)
        t0.append({'original_issue_date':row.issue_date,'update_date':row.issue_date,'update_offset':0,
                   'input_cutoff':iso(issue-pd.Timedelta(days=2)),'available_cutoff':iso(issue-pd.Timedelta(days=2)),
                   'estimate_issue_date':row.issue_date,'shop':row.shop,'spu':row.spu,
                   'model':{'rolling28':'F28','weekday4':'WEEKDAY4','B1':'B1'}[row.model],
                   'track':'T0','window':row.window,'target_start':iso(a),'target_end':iso(b),'window_days':(b-a).days+1,
                   'input_hash':config['source_daily_sha256'],'source_daily_sha256':config['source_daily_sha256'],
                   'lifecycle':m.lifecycle,'maturity':m.maturity,'b1_original_eligible':bool(m.b1_original_eligible),
                   **score_row(actual,pred,'OBSERVED' if actual is not None else 'UNKNOWN_TRUTH',row.prediction_state)})
    for issue_text, group in core.groupby('issue_date'):
        issue = pd.Timestamp(issue_text)
        for model in config['rolling_models']:
            writer = None
            path = out/'daily_vintages'/f'{issue_text}_{model}.parquet'
            for offset in range(60):
                update = issue+pd.Timedelta(days=offset)
                estimate, cutoff = information_dates(issue,offset,model)
                future_rows = []
                for row in group.itertuples():
                    key = (row.shop,row.spu)
                    rate, prediction_status, input_hash = calendar.mean28(key,cutoff)
                    common = {'original_issue_date':issue_text,'update_date':iso(update),'update_offset':offset,
                              'input_cutoff':iso(cutoff),'available_cutoff':iso(update-pd.Timedelta(days=2)),
                              'estimate_issue_date':iso(estimate),'shop':row.shop,'spu':row.spu,'model':model,
                              'input_hash':input_hash,'source_daily_sha256':config['source_daily_sha256'],
                              'lifecycle':row.lifecycle,'maturity':row.maturity,'b1_original_eligible':bool(row.b1_original_eligible)}
                    rates.append({**common,'history_start':iso(cutoff-pd.Timedelta(days=27)),
                                  'history_end':iso(cutoff),'rate':rate,'status':prediction_status})
                    a,b = target_dates(issue,offset,'T1')
                    actual, truth_status = calendar.total(key,a,b)
                    pred = float(originals[(issue_text,*key)].reindex(pd.date_range(a,b)).sum(min_count=(b-a).days+1)) if model=='F28' else (rate*((b-a).days+1) if rate is not None else None)
                    t1.append({**common,'track':'T1','window':'remaining','target_start':iso(a),'target_end':iso(b),
                               'window_days':(b-a).days+1,**score_row(actual,pred,truth_status,prediction_status)})
                    future_end = b if model=='F28' else update+pd.Timedelta(days=60)
                    for day in pd.date_range(update+pd.Timedelta(days=1),future_end):
                        daily_value = float(originals[(issue_text,*key)].loc[day]) if model=='F28' else rate
                        future_rows.append({'original_issue_date':issue_text,'update_date':iso(update),'update_offset':offset,
                                            'input_cutoff':iso(cutoff),'shop':row.shop,'spu':row.spu,'model':model,
                                            'target_date':iso(day),'forecast':daily_value,'status':prediction_status,'input_hash':input_hash})
                    if model!='F28':
                        for window in WINDOWS:
                            a,b = target_dates(issue,offset,'T2',window)
                            actual, truth_status = calendar.total(key,a,b)
                            pred = rate*((b-a).days+1) if rate is not None else None
                            t2.append({**common,'track':'T2','window':window,'target_start':iso(a),'target_end':iso(b),
                                       'window_days':(b-a).days+1,**score_row(actual,pred,truth_status,prediction_status)})
                table = pa.Table.from_pylist(future_rows)
                if writer is None: writer = pq.ParquetWriter(path,table.schema,compression='zstd')
                writer.write_table(table)
            writer.close()
        print('rolling',issue_text,'complete',flush=True)
    rate_frame = pd.DataFrame(rates)
    rate_frame.to_parquet(out/'predictions_daily_rates.parquet',index=False)
    scores = {}
    for name, values in [('t0',t0),('t1',t1),('t2',t2)]:
        frame = pd.DataFrame(values)
        frame.to_parquet(out/f'{name}_scores.parquet',index=False)
        frame.to_csv(out/f'{name}_scores.csv',index=False,encoding='utf-8-sig')
        summary = summarize(frame)
        summary.to_csv(out/f'{name}_summary.csv',index=False,encoding='utf-8-sig')
        scores[name] = frame
        print(name,'scored',len(frame),flush=True)
    pair_detail, pair_summaries = [], []
    for name in ['t1','t2']:
        detail, summary = pairwise(scores[name]); pair_detail.append(detail);pair_summaries.append(summary)
    pd.concat(pair_detail,ignore_index=True).to_parquet(out/'paired_details.parquet',index=False)
    pd.concat(pair_summaries,ignore_index=True).to_csv(out/'paired_summary.csv',index=False,encoding='utf-8-sig')
    joint_rows=[]
    for name in ['t0','t2']:
        f=scores[name]
        for keys,g in f.groupby(['original_issue_date','update_date','update_offset','shop','spu','model']):
            known = g.status.eq('OK').all()
            joint_rows.append(dict(zip(['original_issue_date','update_date','update_offset','shop','spu','model'],keys))|
                              {'track':name.upper(),'all_three_hit':bool(g.decision.eq('HIT').all()),
                               'joint_state':'KNOWN' if known else 'UNJUDGED','b1_original_eligible':bool(g.b1_original_eligible.iloc[0])})
    pd.DataFrame(joint_rows).to_csv(out/'joint_hits.csv',index=False,encoding='utf-8-sig')
    revision=[]
    for keys,g in rate_frame.groupby(['original_issue_date','shop','spu','model']):
        g=g.sort_values('update_offset');changes=g.rate.diff();active=changes[changes.abs()>EPS].dropna()
        signs=np.sign(active.to_numpy());flips=int((signs[1:]!=signs[:-1]).sum())
        revision.append(dict(zip(['original_issue_date','shop','spu','model'],keys))|
                        {'known_updates':int(g.rate.notna().sum()),'nonzero_revisions':len(active),
                         'direction_reversals':flips,'rate_total_absolute_revision':float(changes.abs().sum()),
                         'standardized60_absolute_revision':float(changes.abs().sum()*60),
                         'max_standardized60_revision':float(changes.abs().max()*60)})
    pd.DataFrame(revision).to_csv(out/'revision_summary.csv',index=False,encoding='utf-8-sig')
    rate_frame=rate_frame.sort_values(['original_issue_date','shop','spu','model','update_offset'])
    rate_frame['delta_rate']=rate_frame.groupby(['original_issue_date','shop','spu','model']).rate.diff()
    rate_frame['delta_standardized60']=rate_frame.delta_rate*60
    rate_frame.to_parquet(out/'revision_details.parquet',index=False)
    verify_archive(archive,config)
    manifest.update(status='COMPLETE',finished_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                    elapsed_seconds=time.monotonic()-start,fixed_core_rows=len(core),
                    distinct_shop_spu=len(core[['shop','spu']].drop_duplicates()),
                    original_b1_eligible=int(core.b1_original_eligible.sum()),
                    initial_baseline_max_abs_difference=float(check.original_daily_max_difference.max()),
                    original_archive_hashes_unchanged=True,
                    rows={'rates':len(rates),'t0':len(t0),'t1':len(t1),'t2':len(t2)},
                    files={str(p.relative_to(out)):sha(p) for p in sorted(out.rglob('*')) if p.is_file() and p.name!='run_manifest.json'})
    save_json(out/'run_manifest.json',manifest)
    print(json.dumps({k:v for k,v in manifest.items() if k!='files'},ensure_ascii=False,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',required=True)
    parser.add_argument('--config',required=True)
    parser.add_argument('--out',required=True)
    args=parser.parse_args();run(args.archive,args.config,args.out)
