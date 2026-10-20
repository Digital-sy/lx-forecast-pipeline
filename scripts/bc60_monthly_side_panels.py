"""Read-only archive adapters: native monthly and rolling panels stay separate.

No model imports, fitting, parameter selection, source queries or history repair.
The output directory must be new. Current metadata is descriptive only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

MONTHS = pd.period_range('2025-01', '2026-09', freq='M')
MODELS = {'V4': 'v4', 'A16': 'a16'}
KEYS = ['target_month', 'cutoff_month', 'horizon', 'shop', 'spu']
METADATA_FIELDS = ['current_category', 'current_season']
MONTHLY_PANEL = 'NATIVE_MONTHLY_ARCHIVE_NOT_MONTHLY_CORE'
ROLLING_PANEL = 'V2_ROLLING_ARCHIVE_NOT_INITIAL_ISSUE_RANKING'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def save_json(path, data):
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str, allow_nan=False), encoding='utf-8')


def write_table(frame, path):
    frame.to_csv(path.with_suffix('.csv'), index=False, encoding='utf-8-sig')
    frame.to_parquet(path.with_suffix('.parquet'), index=False)


def prepare_monthly(source):
    """Keep source objects/truth unchanged; only derive dates and ±20% scores."""
    required = set(KEYS + ['actual', 'v4', 'a16', 'forecastability', 'a16_status', *METADATA_FIELDS])
    if not required.issubset(source):
        raise ValueError('MONTHLY_ARCHIVE_SCHEMA')
    if source.duplicated(KEYS).any():
        raise ValueError('DUPLICATE_MONTHLY_ARCHIVE_KEY')
    frame = source.copy()
    target = pd.PeriodIndex(frame.target_month, freq='M')
    cutoff = pd.PeriodIndex(frame.cutoff_month, freq='M')
    horizon = frame.horizon.str.extract(r'^H([123])$', expand=False)
    if horizon.isna().any():
        raise ValueError('UNKNOWN_MONTHLY_HORIZON')
    if not np.array_equal(target.asi8 - cutoff.asi8, horizon.astype(int).to_numpy()):
        raise ValueError('MONTHLY_CUTOFF_HORIZON_MISMATCH')
    if not target.isin(MONTHS).all():
        raise ValueError('MONTHLY_ARCHIVE_OUTSIDE_FROZEN_TARGET_GRID')
    for col in ['actual', 'v4', 'a16']:
        numeric = pd.to_numeric(frame[col], errors='raise')
        if (numeric.notna() & (~np.isfinite(numeric) | numeric.lt(0))).any():
            raise ValueError('INVALID_NONNEGATIVE_SOURCE_VALUE:' + col)
        frame[col] = numeric
    frame['input_cutoff'] = (cutoff.to_timestamp() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    frame['target_start'] = target.to_timestamp().strftime('%Y-%m-%d')
    frame['target_end'] = target.to_timestamp(how='end').strftime('%Y-%m-%d')
    frame['panel'] = MONTHLY_PANEL
    frame['cohort_definition'] = 'original archived object scope; not trailing-90-day sales core'
    frame['metadata_basis'] = 'CURRENT_ARCHIVED_METADATA_NOT_HISTORICAL_INPUT'
    frame['truth_status'] = np.where(frame.actual.notna(), 'ARCHIVED_KNOWN', 'UNKNOWN_ARCHIVED_TRUTH')
    frame['paired_eligible'] = frame[['actual', 'v4', 'a16']].notna().all(axis=1)
    score_columns = KEYS + ['actual', 'v4', 'a16', 'forecastability', 'a16_status', *METADATA_FIELDS,
        'input_cutoff', 'target_start', 'target_end', 'panel', 'cohort_definition', 'metadata_basis',
        'truth_status', 'paired_eligible'] + [c for c in ['age', 'age_class', 'lifecycle', 'profile'] if c in frame]
    long = []
    for model, prediction in MODELS.items():
        part = frame[score_columns].copy()
        part['model'] = model
        part['prediction'] = part[prediction]
        known = part.actual.notna() & part.prediction.notna()
        positive = known & part.actual.gt(0)
        part['prediction_status'] = np.where(part.prediction.notna(), 'ARCHIVED_OUTPUT', 'NO_ARCHIVED_OUTPUT')
        part['relative_error'] = (part.prediction - part.actual).where(positive) / part.actual.where(positive)
        part['absolute_error'] = (part.prediction - part.actual).abs().where(known)
        part['decision'] = np.where(positive, np.where(part.relative_error.abs().le(.2), 'HIT', 'MISS'), 'UNJUDGED')
        long.append(part)
    return frame, pd.concat(long, ignore_index=True)


def monthly_metrics(group):
    known = group.actual.notna()
    output = group.prediction.notna()
    scored = group[known & output]
    error = scored.prediction - scored.actual
    actual = float(scored.actual.sum())
    all_actual = float(group.actual.sum())
    positive = group.actual.gt(0)
    hit = group.decision.eq('HIT')
    return {'cohort_n': len(group), 'output_n': int(output.sum()), 'known_truth_n': int(known.sum()),
            'scored_n': len(scored), 'positive_truth_n': int(positive.sum()),
            'hit_n': int(hit.sum()), 'miss_n': int(group.decision.eq('MISS').sum()),
            'unjudged_n': int(group.decision.eq('UNJUDGED').sum()),
            'unknown_truth_n': int((~known).sum()), 'missing_output_n': int((~output).sum()),
            'zero_truth_n': int(group.actual.eq(0).sum()),
            'hit_rate': float(hit.sum()/positive.sum()) if positive.any() else None,
            'full_scope_hit_share': float(hit.mean()) if len(group) else None,
            'known_scope_actual_units': all_actual, 'actual_units': actual,
            'forecast_units': float(scored.prediction.sum()),
            'wape': float(error.abs().sum()/actual) if actual > 0 else None,
            'bias': float(error.sum()/actual) if actual > 0 else None,
            'over_units': float(error.clip(lower=0).sum()), 'under_units': float((-error).clip(lower=0).sum()),
            'zero_truth_absolute_error': float(error[scored.actual.eq(0)].abs().sum())}


def monthly_summary(scores):
    """Unknown missing-month cohorts are NULL, not known empty populations."""
    shops = ['ALL_SHOPS'] + sorted(scores.shop.unique().tolist())
    rows = []
    empty_metrics = {name: None for name in monthly_metrics(scores.iloc[:0])}
    available = set(zip(scores.target_month, scores.horizon))
    for month in MONTHS:
        for horizon in ['H1', 'H2', 'H3']:
            cutoff = month - int(horizon[1:])
            for shop in shops:
                subset = scores[scores.target_month.eq(str(month)) & scores.horizon.eq(horizon)]
                if shop != 'ALL_SHOPS':
                    subset = subset[subset.shop.eq(shop)]
                for scope in ['original_archive_scope', 'pairwise_v4_a16']:
                    part = subset if scope == 'original_archive_scope' else subset[subset.paired_eligible]
                    for model in MODELS:
                        group = part[part.model.eq(model)]
                        status = 'AVAILABLE' if (str(month), horizon) in available else 'NOT_AVAILABLE_ARCHIVE'
                        if status == 'AVAILABLE' and group.empty:
                            status = 'NO_PAIRED_ROWS' if scope == 'pairwise_v4_a16' else 'NO_ARCHIVED_ROWS_FOR_SHOP'
                        rows.append({'panel': MONTHLY_PANEL, 'target_month': str(month), 'cutoff_month': str(cutoff),
                            'input_cutoff': str((cutoff.start_time-pd.Timedelta(days=1)).date()),
                            'target_start': str(month.start_time.date()), 'target_end': str(month.end_time.date()),
                            'horizon': horizon, 'shop': shop, 'scope': scope, 'model': model, 'status': status,
                            'tolerance': .2, 'metadata_basis': 'CURRENT_ARCHIVED_METADATA_NOT_HISTORICAL_INPUT',
                            **(monthly_metrics(group) if status != 'NOT_AVAILABLE_ARCHIVE' else empty_metrics)})
    return pd.DataFrame(rows)


def metadata_tables(source):
    """Do not choose an arbitrary current value where archived rows conflict."""
    mapping, conflicts = [], []
    for (shop, spu), group in source.groupby(['shop', 'spu'], sort=True):
        row = {'shop': shop, 'spu': spu, 'metadata_basis': 'CURRENT_ARCHIVED_METADATA_NOT_HISTORICAL_INPUT',
               'allowed_use': 'descriptive_report_only', 'archived_row_count': len(group)}
        for field in METADATA_FIELDS:
            valid = group[field].notna() & group[field].astype(str).str.strip().ne('')
            values = sorted(group.loc[valid, field].astype(str).unique().tolist())
            row[field] = values[0] if len(values) == 1 else None
            row[field + '_state'] = 'CONSISTENT_CURRENT' if len(values) == 1 else 'UNKNOWN' if not values else 'CONFLICT'
            row[field + '_values'] = json.dumps(values, ensure_ascii=False)
            row[field + '_missing_rows'] = int((~valid).sum())
            if len(values) > 1:
                for value in values:
                    z = group[group[field].astype(str).eq(value)]
                    conflicts.append({'shop': shop, 'spu': spu, 'field': field, 'value': value,
                        'archived_rows': len(z), 'target_months': '|'.join(sorted(z.target_month.unique())),
                        'metadata_basis': 'CURRENT_ARCHIVED_METADATA_NOT_HISTORICAL_INPUT', 'resolution': 'UNRESOLVED_NO_BACKFILL'})
        mapping.append(row)
    return pd.DataFrame(mapping), pd.DataFrame(conflicts, columns=['shop','spu','field','value','archived_rows',
        'target_months','metadata_basis','resolution'])


def recompute_rolling_summary(scores):
    """Fallback only: sum matched archived scores, retaining each update/target."""
    keys = ['track', 'original_issue_date', 'update_date', 'update_offset', 'input_cutoff',
            'window', 'target_start', 'target_end', 'model', 'shop']
    rows = []
    for values, group in scores.groupby(keys, dropna=False, sort=True):
        rows.append({**dict(zip(keys, values)), 'scope': 'full_archived_core', 'slice': 'issue_update_shop',
            **monthly_metrics(group), 'not_matured_n': int(group.truth_status.eq('NOT_MATURED').sum())})
    return pd.DataFrame(rows)


def build(monthly_details, monthly_choices, rolling, out):
    monthly_details, monthly_choices, rolling, out = map(Path, (monthly_details, monthly_choices, rolling, out))
    if out.exists():
        raise ValueError('OUTPUT_EXISTS')
    input_paths = [monthly_details, monthly_choices]
    input_paths += [rolling / (track + '_scores.parquet') for track in ['t1', 't2']]
    input_paths += [p for p in [rolling/'t1_summary.csv', rolling/'t2_summary.csv', rolling/'run_manifest.json'] if p.exists()]
    hashes = {str(p.resolve()): sha(p) for p in input_paths}
    source, scores = prepare_monthly(pd.DataFrame(read_json(monthly_details)))
    choices = pd.DataFrame(read_json(monthly_choices))
    summary = monthly_summary(scores)
    mapping, conflicts = metadata_tables(source)
    out.mkdir(exist_ok=False)
    write_table(scores, out/'native_monthly_scores')
    write_table(summary, out/'native_monthly_summary')
    write_table(mapping, out/'current_metadata_mapping')
    write_table(conflicts, out/'current_metadata_conflicts')
    # Frozen choices are provenance, never executed to generate any predictions.
    choices.to_json(out/'native_monthly_choices.json', orient='records', indent=2, force_ascii=False)
    rolling_modes = {}
    for track in ['t1', 't2']:
        table = pd.read_parquet(rolling/(track+'_scores.parquet'))
        if not table.track.eq(track.upper()).all():
            raise ValueError('ROLLING_TRACK_MISMATCH')
        unique = ['original_issue_date','update_date','update_offset','shop','spu','model','window','target_start','target_end']
        if table.duplicated(unique).any():
            raise ValueError('DUPLICATE_ROLLING_SCORE_KEY')
        table['panel'] = ROLLING_PANEL
        table['reuse_mode'] = 'UNCHANGED_ARCHIVED_SCORES'
        table['source_sha256'] = hashes[str((rolling/(track+'_scores.parquet')).resolve())]
        table.to_parquet(out/('rolling_'+track+'_scores.parquet'), index=False)
        summary_path = rolling/(track+'_summary.csv')
        metrics = pd.read_csv(summary_path) if summary_path.exists() else recompute_rolling_summary(table)
        mode = 'UNCHANGED_ARCHIVED_METRICS' if summary_path.exists() else 'RECOMPUTED_FROM_ARCHIVED_SCORES'
        metrics['panel'] = ROLLING_PANEL
        metrics['reuse_mode'] = mode
        metrics['target_semantics'] = 'remaining_to_original_endpoint' if track == 't1' else 'reset_full_window_at_update'
        metrics['source_sha256'] = hashes[str(summary_path.resolve())] if summary_path.exists() else table.source_sha256.iloc[0]
        write_table(metrics, out/('rolling_'+track+'_metrics'))
        rolling_modes[track] = mode
    unchanged = all(sha(path) == digest for path, digest in hashes.items())
    manifest = {'completed_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'status': 'COMPLETE' if unchanged else 'SOURCE_CHANGED',
        'script_sha256': sha(__file__), 'source_hashes': hashes, 'source_unchanged': unchanged,
        'fit_count': 0, 'database_queries': 0, 'parameter_searches': 0, 'monthly_hit_tolerance': .2,
        'monthly_source_scope': 'original archive only; not newly selected monthly sales core',
        'monthly_available_targets': sorted(source.target_month.unique().tolist()),
        'monthly_unavailable_targets': sorted(set(map(str, MONTHS))-set(source.target_month)),
        'rolling_modes': rolling_modes,
        'cautions': ['No pooled rank with initial-issue daily models.',
            'No daily proration of monthly forecasts.', 'Unknown archive truth stays unknown, not zero.',
            'Current category/season has no point-in-time proof; never historical model input.',
            'Rolling target intervals and cutoffs remain those in the archived score rows.',
            'Overlapping rolling updates are not independent unique business units.'],
        'files': {str(p.relative_to(out)): sha(p) for p in out.iterdir() if p.is_file()}}
    save_json(out/'side_panels_manifest.json', manifest)
    if not unchanged:
        raise ValueError('SOURCE_CHANGED_DURING_ADAPTATION')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--monthly-details', required=True)
    parser.add_argument('--monthly-choices', required=True)
    parser.add_argument('--rolling', required=True)
    parser.add_argument('--out', required=True)
    build(**vars(parser.parse_args()))


if __name__ == '__main__':
    main()
