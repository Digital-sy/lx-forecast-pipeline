"""Reuse the frozen auxiliary pool; infer L1 checkpoints only, without fitting."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
KEY = ['issue_date', 'shop', 'spu']
CANDIDATE = 'DIRECT_L1_V1'
FEATURES = ['shop_code', 'lag1', 'lag7', 'lag14', 'lag28',
            'rolling_mean_lag1_window_size7', 'rolling_mean_lag1_window_size28',
            'dayofweek', 'month', 'dayofyear']
WINDOWS = {'d01_30': (1, 30), 'd31_60': (31, 60), 'd01_60': (1, 60)}


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(4194304), b''):
            h.update(b)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                    default=str, allow_nan=False), encoding='utf-8')


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def normalized_files(manifest):
    return {k.replace('\\', '/'): v for k, v in manifest['files'].items()}


def verify(sources):
    for path, expected in sources.items():
        if sha(path) != expected:
            raise ValueError('IMMUTABLE_SOURCE_CHANGED:' + str(path))


def unique(frame, keys, name):
    if frame.duplicated(keys).any() or frame[keys].isna().any().any():
        raise ValueError('INVALID_UNIQUE_KEYS:' + name)


def prepare(root, out):
    """Seal references only; do not rebuild membership, features, or target truth."""
    root, out = Path(root).resolve(), Path(out).resolve()
    if root not in out.parents or not out.is_dir():
        raise ValueError('EXPERIMENT_OUTPUT_REQUIRED')
    dest = out / 'auxiliary_prepared'
    if dest.exists():
        raise ValueError('REFUSE_AUXILIARY_OVERWRITE')
    previous = root / 'direct_core_weight2_v1_20261028'
    prepared, risk = previous / 'auxiliary_prepared', previous / 'auxiliary_risk'
    pm, rm = read(prepared / 'manifest.json'), read(risk / 'manifest.json')
    if sha(prepared / 'manifest.json') != rm['pool_manifest_sha256']:
        raise ValueError('PRIOR_POOL_MANIFEST_CHANGED')
    if pm['features'] != FEATURES or len(pm['issues']) != 20:
        raise ValueError('PRIOR_FEATURE_OR_ISSUE_SCOPE_CHANGED')
    sources = {str(prepared / 'manifest.json'): sha(prepared / 'manifest.json'),
               str(risk / 'manifest.json'): sha(risk / 'manifest.json')}
    pf, rf = normalized_files(pm), normalized_files(rm)

    def add(base, rel, index):
        path = base / rel
        expected = index.get(rel)
        if expected is None or sha(path) != expected:
            raise ValueError('PRIOR_SOURCE_HASH_MISMATCH:' + str(path))
        sources[str(path)] = expected
        return path

    pool_path = add(prepared, 'pool.parquet', pf)
    truth_path = add(risk, 'truth.parquet', rf)
    detail_path = add(risk, 'details.parquet', rf)
    pool = pd.read_parquet(pool_path)
    unique(pool, KEY, 'pool')
    if len(pool) != pm['eligible_records'] or len(pool) != 14283:
        raise ValueError('FROZEN_POOL_SCOPE_CHANGED')
    units = []
    for issue in pm['issues']:
        for segment in ['M1', 'M2']:
            rel = f'inference/{issue}_{segment}.parquet'
            ip = add(prepared, rel, pf)
            dp = add(risk, f'inference/{issue}_{segment}_DIRECT.parquet', rf)
            units.append(dict(issue_date=issue, segment=segment,
                              inference_path=str(ip), direct_prediction_path=str(dp)))
    dest.mkdir()
    dump(dest / 'manifest.json', dict(
        status='FROZEN_PRIOR_POOL_REFERENCES', frozen_at=now(),
        script_sha256=sha(__file__), sources=sources, features=FEATURES,
        issues=pm['issues'], pool_records=len(pool),
        pool_path=str(pool_path), truth_path=str(truth_path),
        prior_details_path=str(detail_path), units=units,
        membership_reselected=False, truth_rebuilt=False,
        new_fits=0, model_inference_calls=0,
        prior_pool_sealed_at=pm['completed_at']))
    print(json.dumps(dict(status='AUXILIARY_REFERENCES_FROZEN', pool_records=len(pool))), flush=True)


def candidate_frame(inference, raw, status='OK'):
    frame = inference[KEY + ['segment']].copy()
    unique(frame, KEY + ['segment'], 'candidate_inference')
    raw = np.asarray(raw, dtype=float)
    if len(raw) != len(frame) or (status == 'OK' and not np.isfinite(raw).all()):
        raise ValueError('INVALID_RAW_PREDICTION')
    frame['model'] = CANDIDATE
    frame['raw_prediction'] = raw
    frame['prediction'] = np.maximum(raw, 0)
    frame['raw_prediction_available'] = np.isfinite(raw)
    frame['clipped_amount'] = np.maximum(-raw, 0)
    frame['negative_component_n'] = np.where(np.isfinite(raw), (raw < 0).astype(float), np.nan)
    frame['prediction_status'] = status
    return frame


def reused_direct_frame(inference, previous):
    unique(previous, KEY + ['segment'], 'prior_direct')
    unique(inference, KEY + ['segment'], 'inference')
    if not previous.model.eq('DIRECT').all():
        raise ValueError('WRONG_BASELINE_MODEL')
    aligned = inference[KEY + ['segment']].merge(previous, on=KEY + ['segment'],
                                               how='outer', validate='one_to_one', indicator=True)
    if not aligned._merge.eq('both').all():
        raise ValueError('DIRECT_INFERENCE_KEY_MISMATCH')
    aligned = aligned.drop(columns='_merge')
    # Prior auxiliary archive retained only postprocessed predictions, not raw values.
    aligned['raw_prediction'] = np.nan
    aligned['raw_prediction_available'] = False
    aligned['clipped_amount'] = np.nan
    aligned['negative_component_n'] = np.nan
    aligned['prediction_source'] = 'REUSED_FROZEN_DIRECT_AUXILIARY'
    return aligned


def window_predictions(segments):
    unique(segments, KEY + ['model', 'segment'], 'segments')
    index = KEY + ['model']
    fields = ['prediction', 'raw_prediction', 'clipped_amount', 'negative_component_n']
    wide = {field: segments.pivot(index=index, columns='segment', values=field)
            for field in fields}
    if set(wide['prediction'].columns) != {'M1', 'M2'}:
        raise ValueError('SEGMENT_SCOPE_CHANGED')
    results = []
    for window, parts in [('d01_30', ['M1']), ('d31_60', ['M2']), ('d01_60', ['M1', 'M2'])]:
        frame = wide['prediction'].index.to_frame(index=False)
        frame['window'] = window
        for field in fields:
            frame[field] = wide[field][parts].sum(axis=1, min_count=len(parts)).to_numpy()
        frame['raw_prediction_available'] = frame.raw_prediction.notna()
        results.append(frame)
    return pd.concat(results, ignore_index=True)


def summary(g):
    ok = g.actual.notna() & g.prediction.notna()
    z = g.loc[ok]
    error = z.prediction - z.actual
    den = float(z.actual.sum())
    zero = z.actual.eq(0)
    zero_pred = z.prediction.eq(0) & z.actual.gt(0)
    raw = g.raw_prediction if 'raw_prediction' in g else pd.Series(np.nan, index=g.index)
    return dict(
        fixed_n=len(g), output_n=int(g.prediction.notna().sum()),
        truth_n=int(g.actual.notna().sum()), scored_n=len(z),
        unknown_truth_n=int(g.actual.isna().sum()), actual_units=den,
        prediction_units=float(z.prediction.sum()),
        over_units=float(error.clip(lower=0).sum()), under_units=float((-error).clip(lower=0).sum()),
        absolute_error=float(error.abs().sum()),
        wape=float(error.abs().sum() / den) if den > 0 else None,
        bias=float(error.sum() / den) if den > 0 else None,
        over_rate=float(error.clip(lower=0).sum() / den) if den > 0 else None,
        zero_actual_n=int(zero.sum()),
        zero_actual_prediction_units=float(z.loc[zero, 'prediction'].sum()),
        zero_actual_absolute_error=float(error[zero].abs().sum()),
        positive_actual_zero_prediction_n=int(zero_pred.sum()),
        positive_actual_zero_prediction_actual_units=float(z.loc[zero_pred, 'actual'].sum()),
        raw_available_n=int(raw.notna().sum()), raw_negative_n=int(raw.lt(0).sum()),
        raw_negative_absolute_units=float((-raw).clip(lower=0).sum()),
        clipped_amount=float(g.clipped_amount.sum(min_count=1)) if 'clipped_amount' in g and g.clipped_amount.notna().any() else None,
        clipped_any_component_n=int(g.negative_component_n.gt(0).sum()) if 'negative_component_n' in g else 0)


def group_masks(frame):
    return {'ALL_ELIGIBLE': np.ones(len(frame), bool),
            'CURRENT_NON_CORE': frame.core_state.eq('NON_CORE'),
            'CURRENT_CORE': frame.core_state.eq('CORE'),
            'EXITED_CORE': frame.transition.eq('EXITED_CORE'),
            'NEW_CORE': frame.transition.eq('NEW_CORE'),
            'UNKNOWN_CORE_OR_TRANSITION': frame.core_state.eq('UNKNOWN') | frame.previous_core_state.eq('UNKNOWN')}


def evaluate(root, out):
    """Forty L1 saved-model inference calls maximum; baseline and truth are reused."""
    from lightgbm import Booster

    root, out = Path(root).resolve(), Path(out).resolve()
    os.chdir(ROOT)
    prepared, dest = out / 'auxiliary_prepared', out / 'auxiliary_risk'
    if dest.exists():
        raise ValueError('REFUSE_RISK_OVERWRITE')
    pm = read(prepared / 'manifest.json')
    verify(pm['sources'])
    effects_path = out / 'effects_manifest.json'
    if not effects_path.exists():
        raise ValueError('PRIMARY_EFFECTS_NOT_SEALED')
    effects = read(effects_path)
    effect_files = normalized_files(effects)
    pool = pd.read_parquet(pm['pool_path'])
    sources = dict(pm['sources'])
    sources[str(effects_path)] = sha(effects_path)
    sources[str(prepared / 'manifest.json')] = sha(prepared / 'manifest.json')
    predictions, calls = [], []
    dest.mkdir()
    (dest / 'inference').mkdir()
    started = now()
    for unit in pm['units']:
        issue, segment = unit['issue_date'], unit['segment']
        ip = Path(unit['inference_path'])
        inference = pd.read_parquet(ip)
        if not inference.issue_date.eq(issue).all() or not inference.segment.eq(segment).all():
            raise ValueError('INFERENCE_UNIT_MISMATCH')
        model_path = out / 'effects' / f'{issue}_{segment}' / 'model.txt'
        expected = effect_files.get(model_path.relative_to(out).as_posix())
        status = 'OK'
        if not model_path.exists() or expected is None:
            status = 'MISSING_CHECKPOINT'
            raw = np.full(len(inference), np.nan)
        else:
            if sha(model_path) != expected:
                raise ValueError('MODEL_HASH_CHANGED')
            checkpoint = read(model_path.with_name('checkpoint.json'))
            if checkpoint['model_sha256'] != expected:
                raise ValueError('MODEL_CHECKPOINT_HASH_CHANGED')
            sources[str(model_path)] = expected
            booster = Booster(model_file=os.path.relpath(model_path, ROOT))
            if booster.feature_name() != FEATURES:
                raise ValueError('MODEL_FEATURE_MISMATCH')
            raw = booster.predict(inference[FEATURES]) if len(inference) else np.array([], float)
        candidate = candidate_frame(inference, raw, status)
        candidate['prediction_source'] = 'NEW_SAVED_L1_MODEL_INFERENCE'
        direct = reused_direct_frame(inference, pd.read_parquet(unit['direct_prediction_path']))
        for frame in [candidate, direct]:
            model = frame.model.iloc[0]
            frame.to_parquet(dest / 'inference' / f'{issue}_{segment}_{model}.parquet', index=False)
            predictions.append(frame)
        calls.append(dict(issue_date=issue, segment=segment, model=CANDIDATE,
                          inference_calls=int(len(inference) > 0 and status == 'OK'),
                          predicted_rows=len(inference) if status == 'OK' else 0,
                          reused_direct_rows=len(direct), direct_inference_calls=0,
                          fixed_n=len(inference), prediction_status=status,
                          model_sha256=expected, input_sha256=sha(ip), completed_at=now()))
    ledger = pd.DataFrame(calls)
    ledger.to_csv(dest / 'inference_calls.csv', index=False, encoding='utf-8-sig')
    segments = pd.concat(predictions, ignore_index=True)
    dump(dest / 'prediction_seal.json', dict(
        sealed_at=now(), pool_manifest_sha256=sha(prepared / 'manifest.json'),
        inference_calls=int(ledger.inference_calls.sum()), new_fits=0,
        direct_inference_calls=0,
        files={p.relative_to(dest).as_posix(): sha(p) for p in (dest / 'inference').glob('*.parquet')}))
    # Target truth is the previous independently validated immutable artifact.
    truth = pd.read_parquet(pm['truth_path'])
    unique(truth, KEY + ['window'], 'truth')
    for window, (start, end) in WINDOWS.items():
        t = truth[truth.window.eq(window)]
        issue = pd.to_datetime(t.issue_date)
        if not (pd.to_datetime(t.target_start) == issue + pd.Timedelta(days=start)).all():
            raise ValueError('TRUTH_START_CHANGED')
        if not (pd.to_datetime(t.target_end) == issue + pd.Timedelta(days=end)).all():
            raise ValueError('TRUTH_END_CHANGED')
    scored = window_predictions(segments).merge(truth, on=KEY + ['window'], how='left', validate='many_to_one')
    if scored.truth_status.isna().any():
        raise ValueError('TRUTH_JOIN_MISSING')
    scored = scored.merge(pool, on=KEY, validate='many_to_one')
    if len(scored) != len(pool) * 6:
        raise ValueError('RISK_JOIN_EXPANSION')
    scored['error'] = scored.prediction - scored.actual
    # Reused predictions, truth and membership must agree with the earlier verified details.
    prior = pd.read_parquet(pm['prior_details_path'])
    cols = KEY + ['window', 'prediction', 'actual', 'truth_status', 'core_state', 'transition']
    left = scored[scored.model.eq('DIRECT')][cols].sort_values(KEY + ['window']).reset_index(drop=True)
    right = prior[prior.model.eq('DIRECT')][cols].sort_values(KEY + ['window']).reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_exact=True)
    scored.to_parquet(dest / 'details.parquet', index=False)
    groups = group_masks(scored)
    rows, periods = [], []
    for scope, mask in groups.items():
        for model in [CANDIDATE, 'DIRECT']:
            for window in WINDOWS:
                frame = scored[mask & scored.model.eq(model) & scored.window.eq(window)]
                rows.append(dict(scope=scope, model=model, window=window, **summary(frame)))
                for issue in pm['issues']:
                    periods.append(dict(scope=scope, issue_date=issue, model=model, window=window,
                                        **summary(frame[frame.issue_date.eq(issue)])))
    pd.DataFrame(rows).to_csv(dest / 'metrics.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(periods).to_csv(dest / 'period_metrics.csv', index=False, encoding='utf-8-sig')
    clipping = []
    for (issue, segment), g in segments[segments.model.eq(CANDIDATE)].groupby(['issue_date', 'segment']):
        clipping.append(dict(issue_date=issue, segment=segment, records=len(g),
                             negative_raw_n=int(g.raw_prediction.lt(0).sum()),
                             raw_prediction_sum=float(g.raw_prediction.sum()),
                             final_prediction_sum=float(g.prediction.sum()),
                             clipped_amount=float(g.clipped_amount.sum())))
    pd.DataFrame(clipping).to_csv(dest / 'clipping_summary.csv', index=False, encoding='utf-8-sig')
    verify(sources)
    missing = int(ledger.prediction_status.ne('OK').sum())
    dump(dest / 'manifest.json', dict(
        status='PARTIAL_MISSING_CHECKPOINT' if missing else 'EVALUATED_RECONSTRUCTED_HISTORY',
        missing_checkpoint_units=missing, started_at=started, completed_at=now(),
        sources=sources, script_sha256=sha(__file__), new_fits=0,
        inference_calls=int(ledger.inference_calls.sum()), direct_inference_calls=0,
        predicted_segment_rows=int(ledger.predicted_rows.sum()),
        reused_direct_segment_rows=int(ledger.reused_direct_rows.sum()), pool_records=len(pool),
        scoring_weights='NONE; original business metrics',
        truth_validation_chain='Exact reuse of prior independently validated truth.parquet; hash checked; DIRECT details reconciled exactly',
        raw_direct_status='NOT_ARCHIVED; not reconstructed or inferred',
        scope='same frozen auxiliary pool; does not replace primary 2820/2579',
        files={p.relative_to(dest).as_posix(): sha(p) for p in dest.rglob('*') if p.is_file()}))
    print(json.dumps(dict(status='AUXILIARY_COMPLETE', pool_records=len(pool),
                          inference_calls=int(ledger.inference_calls.sum()), direct_inference_calls=0)), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['prepare', 'evaluate'])
    parser.add_argument('--root', required=True)
    parser.add_argument('--out', required=True)
    args = vars(parser.parse_args())
    action = args.pop('action')
    {'prepare': prepare, 'evaluate': evaluate}[action](**args)
