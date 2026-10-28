"""Read-only six-model historical diagnostic. No forecasting or network imports."""
from __future__ import annotations
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd

KEY = ['issue_date', 'shop', 'spu']
WINDOWS = ['d01_30', 'd31_60', 'd01_60']
ALIASES = {'F28': 'F28', 'DIRECT30x2': 'DIRECT', 'DIRECT_CONTEXT90': 'CONTEXT90',
           'DIRECT_YEARAGO': 'YEARAGO', 'DIRECT_YEARAGO_GATE_V1': 'GATE',
           'DIRECT_YEARAGO_BLEND50': 'BLEND50'}
MODELS = list(ALIASES.values())
OTHERS = [x for x in MODELS if x != 'BLEND50']
OFFSETS = {'d01_30': (1, 30), 'd31_60': (31, 60), 'd01_60': (1, 60)}


def sha(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b''): h.update(block)
    return h.hexdigest()


def read(p): return json.loads(Path(p).read_text(encoding='utf-8'))
def now(): return datetime.now(timezone.utc).isoformat()
def dump(p, x): Path(p).write_text(json.dumps(x, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def unique(d, key):
    if d.duplicated(key).any() or d[key].isna().any().any(): raise ValueError('INVALID_BUSINESS_KEY')


def mature(d):
    d = d.copy()
    d['issue_date'] = pd.to_datetime(d.issue_date).dt.strftime('%Y-%m-%d')
    return d[d.issue_date.ge('2025-01-01') & d.issue_date.lt('2026-09-01')].copy()


def ratio(a, b): return float(a / b) if b else None


def hit(pred, actual):
    if not np.isfinite(pred) or not np.isfinite(actual) or actual <= 0: return False
    return bool(abs(pred - actual) / actual <= .2)


def classify(predictions, actual):
    if not np.isfinite(actual): return 'UNKNOWN_TRUTH', 'UNKNOWN_TRUTH'
    if not np.isfinite(predictions).all(): return 'INCOMPLETE_OUTPUT', 'INCOMPLETE_OUTPUT'
    if actual == 0: return 'ZERO_ACTUAL', 'ZERO_ACTUAL'
    e = np.asarray(predictions) - actual
    if (e > 0).all(): direction = 'ALL_OVER'
    elif (e < 0).all(): direction = 'ALL_UNDER'
    elif (e > 0).any() and (e < 0).any(): direction = 'OPPOSITE'
    elif (e == 0).all(): direction = 'ALL_EXACT'
    elif (e >= 0).all(): direction = 'OVER_OR_EXACT'
    else: direction = 'UNDER_OR_EXACT'
    relative = e / actual
    if (relative > .2).all(): band = 'ALL_ABOVE_20'
    elif (relative < -.2).all(): band = 'ALL_BELOW_MINUS20'
    elif (relative > .2).any() and (relative < -.2).any(): band = 'OPPOSITE_OUTSIDE_20'
    else: band = 'AT_LEAST_ONE_WITHIN_20'
    return direction, band


def verify_file(manifest_path, rel):
    m = read(manifest_path)
    p = manifest_path.parent / rel
    if sha(p) != m['files'][rel]: raise ValueError('ARCHIVE_FILE_CHANGED:' + str(p))
    return p


def load_sources(root):
    blend = root / 'direct_yearago_blend50_20261021_v1'
    context = root / 'direct_context90_20261020_v1'
    replay = root / 'bc60_monthly_replay_20261020'
    paths = []
    for archive, mrel, vrel, field in [
        (blend, 'scoring/manifest.json', 'validation/validation.json', 'scoring_manifest_sha256'),
        (context, 'scoring/score_manifest.json', 'validation/validation.json', 'score_manifest_sha256')]:
        m, v = archive / mrel, archive / vrel
        val = read(v)
        if val['status'] != 'PASS' or val[field] != sha(m): raise ValueError('VALIDATION_CHAIN_CHANGED')
        paths += [m, v, verify_file(m, 'scores.parquet')]
    core_p = verify_file(replay / 'prepared/prepare_manifest.json', 'core.parquet')
    truth_p = verify_file(context / 'scoring/score_manifest.json', 'independent_daily_truth.csv')
    old_csv = verify_file(replay / 'scoring/score_manifest.json', 'scores.csv')
    paths += [core_p, truth_p, replay / 'prepared/prepare_manifest.json', old_csv,
              replay / 'scoring/score_manifest.json', context / 'configuration.json']
    bv = read(blend / 'validation/validation.json')['truth_chain']
    chain = Path(bv['source_validation_path'])
    if sha(chain) != bv['source_validation_sha256']: raise ValueError('TRUTH_CHAIN_CHANGED')
    paths.append(chain)
    before = {str(p): sha(p) for p in paths}
    b = mature(pd.read_parquet(blend / 'scoring/scores.parquet'))
    c = mature(pd.read_parquet(context / 'scoring/scores.parquet'))
    core = mature(pd.read_parquet(core_p)); unique(core, KEY)
    truth = mature(pd.read_csv(truth_p, float_precision='round_trip')); unique(truth, KEY + ['window'])
    old_exact = mature(pd.read_csv(old_csv, float_precision='round_trip'))
    old_legacy = mature(pd.read_csv(old_csv))
    old_exact = old_exact[old_exact.cohort_id.eq('monthly_core')]
    old_legacy = old_legacy[old_legacy.cohort_id.eq('monthly_core')]
    float_copy_audit = []
    # Confirm previously frozen baseline copies before combining archives.
    compare_cols = ['prediction', 'actual', 'target_start', 'target_end']
    for model in ['DIRECT30x2', 'F28']:
        l = b[b.model.eq(model)].set_index(KEY + ['window']).sort_index()
        r = c[c.model.eq(model)].set_index(KEY + ['window']).sort_index()
        if not l.index.equals(r.index): raise ValueError('BASELINE_SCOPE_CHANGED')
        for col in compare_cols:
            x, y = l[col], r[col]
            if col.startswith('target_'): x, y = pd.to_datetime(x), pd.to_datetime(y)
            exact = (x == y) | (x.isna() & y.isna())
            if not exact.all():
                if col != 'prediction': raise ValueError('BASELINE_COPY_CHANGED:' + col)
                raw = old_exact[old_exact.model.eq(model)].set_index(KEY + ['window']).sort_index().prediction
                legacy = old_legacy[old_legacy.model.eq(model)].set_index(KEY + ['window']).sort_index().prediction
                if not ((x == raw) | (x.isna() & raw.isna())).all(): raise ValueError('CANONICAL_FLOAT_COPY_CHANGED')
                if not ((y == legacy) | (y.isna() & legacy.isna())).all(): raise ValueError('LEGACY_PARSER_NOT_EXPLANATION')
                hx = l.actual.gt(0) & ((x-l.actual).abs()/l.actual).le(.2)
                hy = r.actual.gt(0) & ((y-r.actual).abs()/r.actual).le(.2)
                if not hx.eq(hy).all(): raise ValueError('COPY_PRECISION_CHANGES_HIT')
                float_copy_audit.append(dict(model=model, changed_float_copies=int((~exact).sum()),
                    max_absolute_difference=float((x-y).abs().max()), hit_changes=int((hx!=hy).sum()),
                    cause='Exact reproduction of legacy default CSV parser versus round_trip; no tolerance-based hit change',
                    selected='canonical round_trip baseline retained; only CONTEXT90 candidate read from context archive'))
    s = pd.concat([b, c[c.model.eq('DIRECT_CONTEXT90')]], ignore_index=True)
    s['model'] = s.model.map(ALIASES); unique(s, KEY + ['model', 'window'])
    if s.model.isna().any() or set(s.model) != set(MODELS): raise ValueError('MODEL_SET_CHANGED')
    core_index = pd.MultiIndex.from_frame(core[KEY])
    for (model, window), g in s.groupby(['model', 'window']):
        if set(pd.MultiIndex.from_frame(g[KEY])) != set(core_index): raise ValueError('CORE_SCOPE_CHANGED')
    q = s.merge(truth, on=KEY + ['window'], how='left', suffixes=('', '_independent'), validate='many_to_one')
    for col in ['actual', 'unknown_days', 'truth_status', 'target_start', 'target_end']:
        x, y = q[col], q[col + '_independent']
        if col.startswith('target_'): x, y = pd.to_datetime(x), pd.to_datetime(y)
        if not ((x == y) | (x.isna() & y.isna())).all(): raise ValueError('INDEPENDENT_TRUTH_CHANGED:' + col)
    for w, (a, z) in OFFSETS.items():
        g = s[s.window.eq(w)]; issue = pd.to_datetime(g.issue_date)
        if not pd.to_datetime(g.target_start).eq(issue + pd.Timedelta(days=a)).all(): raise ValueError('START_CHANGED')
        if not pd.to_datetime(g.target_end).eq(issue + pd.Timedelta(days=z)).all(): raise ValueError('END_CHANGED')
    finite = s.prediction.dropna()
    if not np.isfinite(finite).all() or finite.lt(0).any() or s.actual.lt(0).any(): raise ValueError('INVALID_QUANTITY')
    p = s.pivot(index=KEY + ['model'], columns='window', values='prediction')
    # Do not recompute or round archived 60-day predictions.
    if not ((p.d01_30 + p.d31_60).eq(p.d01_60) | p.isna().all(axis=1)).all(): raise ValueError('SEGMENT_SUM_CHANGED')
    if len(core) != 2820 or core.issue_date.nunique() != 20: raise ValueError('EXPECTED_ARCHIVE_SCOPE_CHANGED')
    return s, core, before, float_copy_audit


def protect_shadow(path):
    seal = path / 'issued/issue_seal.json'
    m = read(seal)
    if m['status'] != 'ISSUED_SHADOW_AWAITING_ACTUALS': raise ValueError('SHADOW_SEAL_STATUS_CHANGED')
    for rel, digest in m['files'].items():
        if sha(path / rel) != digest: raise ValueError('SHADOW_SEAL_FILE_CHANGED:' + rel)
    return {str(p): sha(p) for p in path.rglob('*') if p.is_file()}


def build_objects(scores):
    records = []
    for keys, g in scores.groupby(KEY, sort=True):
        row = dict(zip(KEY, keys))
        base = g[g.model.eq('DIRECT')].set_index('window')
        for w in WINDOWS: row['actual_' + w] = float(base.loc[w, 'actual'])
        for col in ['maturity', 'lifecycle', 'current_category', 'decline_group']:
            row[col] = base.iloc[0][col]
        for model in MODELS:
            r = g[g.model.eq(model)].set_index('window')
            for w in WINDOWS:
                row[model + '_prediction_' + w] = float(r.loc[w, 'prediction'])
                row[model + '_hit_' + w] = hit(row[model + '_prediction_' + w], row['actual_' + w])
            row[model + '_output_complete'] = bool(np.isfinite(r.prediction).all())
            row[model + '_joint_hit'] = all(row[model + '_hit_' + w] for w in WINDOWS)
        row['paired'] = row['DIRECT_output_complete']
        row['truth_complete'] = all(np.isfinite(row['actual_' + w]) for w in WINDOWS)
        row['positive_all_windows'] = all(row['actual_' + w] > 0 for w in WINDOWS)
        row['all_models_output'] = all(row[m + '_output_complete'] for m in MODELS)
        winners = [m for m in MODELS if row[m + '_joint_hit']]
        row['joint_hit_models'] = '|'.join(winners)
        row['any_joint_hit'] = bool(winners)
        row['blend_rescued'] = not row['BLEND50_joint_hit'] and bool(winners)
        row['rescue_models'] = '|'.join(m for m in winners if m != 'BLEND50') if row['blend_rescued'] else ''
        if not row['truth_complete']: row['union_status'] = 'UNKNOWN_TRUTH'
        elif row['any_joint_hit']: row['union_status'] = 'AT_LEAST_ONE_JOINT_HIT'
        elif not row['all_models_output']: row['union_status'] = 'INCOMPLETE_MODEL_OUTPUT'
        elif not row['positive_all_windows']: row['union_status'] = 'ZERO_ACTUAL_UNDEFINED_RELATIVE_ERROR'
        else: row['union_status'] = 'ALL_SIX_EVALUATED_JOINT_MISS'
        records.append(row)
    return pd.DataFrame(records)


def aggregate(g):
    actual = float(g.actual_d01_60.sum(min_count=1))
    def selected(mask, prefix):
        return {prefix + '_n': int(mask.sum()), prefix + '_actual60': float(g.loc[mask, 'actual_d01_60'].sum())}
    out = dict(records=len(g), products=len(g[['shop', 'spu']].drop_duplicates()), actual60=actual)
    out.update(selected(g.BLEND50_joint_hit, 'blend_hit'))
    out.update(selected(~g.BLEND50_joint_hit, 'blend_non_hit_including_missing'))
    out.update(selected(g.blend_rescued, 'rescue_union'))
    out.update(selected(g.blend_rescued & g.BLEND50_output_complete, 'rescue_with_blend_output'))
    out.update(selected(g.blend_rescued & ~g.BLEND50_output_complete, 'rescue_blend_missing'))
    out.update(selected(g.any_joint_hit, 'any_model_hit'))
    out.update(selected(g.all_models_output & ~g.any_joint_hit, 'all_six_output_no_joint_hit'))
    for status in ['ALL_SIX_EVALUATED_JOINT_MISS', 'ZERO_ACTUAL_UNDEFINED_RELATIVE_ERROR', 'INCOMPLETE_MODEL_OUTPUT', 'UNKNOWN_TRUTH']:
        out.update(selected(g.union_status.eq(status), status.lower()))
    return out


def longest_run(dates):
    periods = sorted({pd.Period(d, freq='M').ordinal for d in dates})
    best = run = 0; previous = None
    for p in periods:
        run = run + 1 if previous is not None and p == previous + 1 else 1
        best = max(best, run); previous = p
    return best


def product_ranking(o):
    d = o[o.paired].copy(); records = []
    for (shop, spu), g in d.groupby(['shop', 'spu']):
        e = g.BLEND50_prediction_d01_60 - g.actual_d01_60
        ae = e.abs(); known = e.notna()
        segment_ae = sum((g['BLEND50_prediction_' + w] - g['actual_' + w]).abs().sum() for w in WINDOWS[:2])
        records.append(dict(shop=shop, spu=spu, issues=len(g), scored_issues=int(known.sum()),
            joint_hit_n=int(g.BLEND50_joint_hit.sum()), joint_miss_n=int((~g.BLEND50_joint_hit).sum()),
            actual60=float(g.actual_d01_60.sum()), absolute_error60=float(ae.sum()),
            nonjoint_absolute_error60=float(ae[~g.BLEND50_joint_hit].sum()),
            segment_absolute_error_sum=float(segment_ae), over60=float(e.clip(lower=0).sum()),
            under60=float((-e).clip(lower=0).sum()), over_issues=int(e.gt(0).sum()), under_issues=int(e.lt(0).sum()),
            largest_issue_error_share=ratio(ae.max(), ae.sum()), top3_issue_error_share=ratio(ae.nlargest(3).sum(), ae.sum()),
            largest_error_issue=g.loc[ae.idxmax(), 'issue_date'],
            longest_joint_miss_month_run=longest_run(g.loc[~g.BLEND50_joint_hit, 'issue_date']),
            rescue_issues=int(g.blend_rescued.sum()), rescue_actual60=float(g.loc[g.blend_rescued, 'actual_d01_60'].sum())))
    return pd.DataFrame(records).sort_values(['absolute_error60', 'shop', 'spu'], ascending=[False, True, True]).assign(rank=lambda x: range(1, len(x) + 1))


def analyze(o):
    tables = {}
    summary, coverage, alternatives, direction, combinations = [], [], [], [], []
    for scope, q in [('FULL_CORE', o), ('PAIRED', o[o.paired])]:
        summary.append(dict(scope=scope, **aggregate(q)))
        for model in MODELS:
            for w in WINDOWS:
                p, y = q[model + '_prediction_' + w], q['actual_' + w]
                valid = np.isfinite(p) & np.isfinite(y); e = p[valid] - y[valid]
                h = q[model + '_hit_' + w]
                coverage.append(dict(scope=scope, model=model, window=w, fixed_n=len(q),
                    output_n=int(np.isfinite(p).sum()), truth_n=int(np.isfinite(y).sum()), scored_n=int(valid.sum()),
                    zero_truth_n=int(y.eq(0).sum()), missing_output_n=int(p.isna().sum()),
                    hit_n=int(h.sum()), joint_hit_n=int(q[model + '_joint_hit'].sum()),
                    joint_hit_actual60=float(q.loc[q[model + '_joint_hit'], 'actual_d01_60'].sum()),
                    actual_scored=float(y[valid].sum()), ae=float(e.abs().sum()),
                    wape=ratio(e.abs().sum(), y[valid].sum()), bias=ratio(e.sum(), y[valid].sum()),
                    zero_truth_absolute_error=float(e[y[valid].eq(0)].abs().sum())))
        for m in OTHERS:
            rescued = ~q.BLEND50_joint_hit & q[m + '_joint_hit']
            exclusive = rescued & q.rescue_models.eq(m)
            alternatives.append(dict(scope=scope, model=m, rescue_n=int(rescued.sum()),
                rescue_actual60=float(q.loc[rescued, 'actual_d01_60'].sum()),
                exclusive_rescue_n=int(exclusive.sum()), exclusive_rescue_actual60=float(q.loc[exclusive, 'actual_d01_60'].sum())))
        for models, r in q[q.blend_rescued].groupby('rescue_models'):
            combinations.append(dict(scope=scope, rescuing_models=models, records=len(r), actual60=float(r.actual_d01_60.sum())))
        # Unknown forecasts are kept in object output, never treated as six observed failures.
        fails = q[q.all_models_output & q.truth_complete & ~q.any_joint_hit]
        for w in WINDOWS:
            for keys, r in fails.iterrows():
                dr, band = classify([r[m + '_prediction_' + w] for m in MODELS], r['actual_' + w])
                direction.append(dict(scope=scope, **{k: r[k] for k in KEY}, window=w, direction=dr,
                    error_band=band, actual=float(r['actual_' + w]), actual60=float(r.actual_d01_60),
                    blend_absolute_error=float(abs(r['BLEND50_prediction_' + w] - r['actual_' + w]))))
    tables['scope_summary'] = pd.DataFrame(summary)
    tables['model_coverage'] = pd.DataFrame(coverage)
    tables['rescue_by_model'] = pd.DataFrame(alternatives)
    tables['rescue_overlap_combinations'] = pd.DataFrame(combinations)
    dirs = pd.DataFrame(direction); tables['all_miss_directions_detail'] = dirs
    for dim in ['direction', 'error_band']:
        tables['all_miss_' + dim + '_summary'] = dirs.groupby(['scope', 'window', dim], as_index=False).agg(
            records=('spu', 'size'), actual=('actual', 'sum'), actual60=('actual60', 'sum'), blend_absolute_error=('blend_absolute_error', 'sum'))
    for dim in ['issue_date', 'shop']:
        tables['complementarity_by_' + dim] = pd.DataFrame([
            dict(scope=scope, **{dim: key}, **aggregate(g))
            for scope, q in [('FULL_CORE', o), ('PAIRED', o[o.paired])]
            for key, g in q.groupby(dim)])
    ranks = product_ranking(o); top = ranks.head(20)
    periods = o[o.paired].merge(top[['shop', 'spu', 'rank']], on=['shop', 'spu'], validate='many_to_one')
    tables['product_error_ranking'] = ranks
    tables['top20_products'] = top
    tables['top20_issue_detail'] = periods.sort_values(['rank', 'issue_date'])
    stable = []
    for (shop, spu), g in periods.groupby(['shop', 'spu']):
        for period, q in [('ALL', g)] + [(year, z) for year, z in g.groupby(g.issue_date.str[:4])]:
            for m in MODELS:
                e = q[m + '_prediction_d01_60'] - q.actual_d01_60
                b = q.BLEND50_prediction_d01_60 - q.actual_d01_60
                valid = np.isfinite(e) & np.isfinite(b)
                delta = e.abs() - b.abs()
                rescue = ~q.BLEND50_joint_hit & q[m + '_joint_hit']
                lost = q.BLEND50_joint_hit & ~q[m + '_joint_hit']
                stable.append(dict(shop=shop, spu=spu, period=period, model=m, issues=len(q), paired_issues=int(valid.sum()),
                    lower_ae_issues=int(delta[valid].lt(0).sum()), higher_ae_issues=int(delta[valid].gt(0).sum()),
                    tied_ae_issues=int(delta[valid].eq(0).sum()), absolute_error60=float(e[valid].abs().sum()),
                    blend_absolute_error60=float(b[valid].abs().sum()), delta_absolute_error60=float(delta[valid].sum()),
                    opposite_error_direction_issues=int(((e * b < 0) & valid).sum()),
                    joint_hit_n=int(q[m + '_joint_hit'].sum()), rescue_issues=int(rescue.sum()), lost_joint_hits=int(lost.sum()),
                    rescued_actual60=float(q.loc[rescue, 'actual_d01_60'].sum())))
    tables['top20_model_stability'] = pd.DataFrame(stable)
    return tables


def run(root, output, shadow):
    if output.exists(): raise ValueError('OUTPUT_ALREADY_EXISTS')
    if root.resolve() not in output.resolve().parents: raise ValueError('OUTPUT_OUTSIDE_APPROVED_WORKSPACE')
    started = now()
    s, core, sources, float_copy_audit = load_sources(root)
    shadow_hashes = protect_shadow(shadow)
    objects = build_objects(s)
    if int(objects.paired.sum()) != 2579: raise ValueError('ORIGINAL_PAIR_SCOPE_CHANGED')
    tables = analyze(objects)
    output.mkdir(parents=False, exist_ok=False)
    dump(output / 'float_copy_audit.json', float_copy_audit)
    objects.to_parquet(output / 'objects.parquet', index=False)
    objects.to_csv(output / 'objects.csv', index=False, encoding='utf-8-sig')
    s.to_parquet(output / 'historical_windows.parquet', index=False)
    for name, d in tables.items(): d.to_csv(output / (name + '.csv'), index=False, encoding='utf-8-sig')
    for p, digest in {**sources, **shadow_hashes}.items():
        if sha(p) != digest: raise ValueError('READ_ONLY_SOURCE_CHANGED:' + p)
    dump(output / 'manifest.json', dict(status='COMPUTED_PENDING_INDEPENDENT_CHECK', started_at=started, completed_at=now(),
        classification='RECONSTRUCTED_HISTORICAL_POSTHOC_DIAGNOSTIC_NOT_OOS_NOT_DEPLOYABLE_ROUTING',
        scripts_sha256={str(Path(__file__).name): sha(__file__)}, sources=sources,
        protected_shadow_files=shadow_hashes, shadow_used_for_scoring=False,
        full_core_n=len(core), paired_n=int(objects.paired.sum()),
        immature_excluded_n=274, issue_months=sorted(core.issue_date.str[:7].unique()),
        fits=0, inference=0, generated_forecasts=0, database_queries=0, api_calls=0, production_changes=0,
        files={p.name: sha(p) for p in output.iterdir() if p.is_file()}))
    print(tables['scope_summary'].to_json(orient='records'), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--archive-root', required=True); p.add_argument('--output', required=True)
    p.add_argument('--protected-shadow', required=True)
    a = p.parse_args()
    run(Path(a.archive_root).resolve(), Path(a.output).resolve(), Path(a.protected_shadow).resolve())
