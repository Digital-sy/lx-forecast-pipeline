"""Bounded pure-local extension of frozen native-month V4, never daily proration.

The original SKU context and native H1/H2/H3 semantics are retained. No production
module is imported: only verified pure definitions and literal constants execute.
Unknown truth remains unknown. Existing nine target months are not recomputed.
"""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess

import pandas as pd

SHOPS = ('JQ-US', 'MT-US', 'RKZ-US', 'SY-US')
MONTHS = tuple(f'2025-{i:02}' for i in range(1, 10)) + ('2026-04', '2026-05', '2026-06')
EXISTING = ('2025-10', '2025-11', '2025-12', '2026-01', '2026-02', '2026-03', '2026-07', '2026-08', '2026-09')
FROZEN_V4_LF = '211eadfafc34953a09083bb04a9471160a4174d01c75f0b00a8516957047e97c'
FROZEN_INPUT = '1951f218bc79b68be17cbfce78c9e4ec9c6750e97bc2bbf1e539697b45a4ac67'


def sha(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def add_month(month, offset):
    n = month.year * 12 + month.month - 1 + offset
    return dt.date(n // 12, n % 12 + 1, 1)


def label(month, forecast=False):
    return f'{month.year % 100:02}年{month.month}月' + ('预计销量' if forecast else '销量')


def age_class(first, cutoff):
    age = (cutoff.year - first.year) * 12 + cutoff.month - first.month
    return age, ('OLD_12P' if age >= 12 else 'OLD_6_TO_11' if age >= 6 else
                 'ESTABLISHED_4_TO_5' if age >= 4 else 'RECENT_OBSERVED_0_TO_3')


def pure_v4(path):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest() != FROZEN_V4_LF:
        raise ValueError('FROZEN_V4_HASH_MISMATCH')
    tree = ast.parse(raw.decode('utf-8-sig'))
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            if [n.name for n in node.names] != ['re']:
                raise ValueError('UNEXPECTED_IMPORT')
            nodes.append(node)
        elif isinstance(node, ast.ImportFrom) and node.module in ('datetime', 'typing'):
            nodes.append(node)
        elif isinstance(node, ast.Assign) and all(isinstance(n, ast.Name) and n.id.isupper() for n in node.targets):
            ast.literal_eval(node.value)  # Reject calls and side effects in constants.
            nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name != 'load_spu_season_map':
            nodes.append(node)
    namespace = {'__name__': 'isolated_frozen_v4_pure'}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    assert 'db_cursor' not in namespace and 'load_spu_season_map' not in namespace
    return namespace['compute_forecast_for_shop']


def mapping_functions(path):
    tree = ast.parse(Path(path).read_text(encoding='utf-8-sig'))
    wanted = ('remove_psc_pattern', 'extract_spu_from_sku')
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    if len(nodes) != 2:
        raise ValueError('MAPPING_FUNCTIONS_MISSING')
    import re
    namespace = {'re': re}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), namespace)
    return namespace['extract_spu_from_sku']


def archive_records(path):
    with Path(path).open(encoding='utf-8-sig') as stream:
        yield from (json.loads(line) for line in stream)


def read_archive(path, fallback):
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    first = {}
    mappings = defaultdict(set)
    source = defaultdict(lambda: [0, 0])
    seasons, receipts = [], []
    counts = {'sales_rows': 0, 'fallback_mapping_rows': 0, 'explicit_zero_rows': 0,
              'invalid_rows': 0}
    for record in archive_records(path):
        kind, value = record.get('kind'), record.get('value')
        if kind == 'seasons':
            seasons = value
        elif kind == 'completed':
            receipts.append(value if value is not None else record)
        elif kind == 'sales':
            shop, sku = str(value.get('店铺') or '').strip(), str(value.get('SKU') or '').strip()
            spu = str(value.get('SPU') or '').strip()
            if not spu:
                counts['fallback_mapping_rows'] += 1
                spu = fallback(sku)
            if shop not in SHOPS or not sku or not spu or value.get('销量') is None:
                raise ValueError('SOURCE_ROW_UNKNOWN_OR_OUT_OF_SCOPE')
            qty = value['销量']
            if float(qty) < 0 or float(qty) != int(qty):
                raise ValueError('NEGATIVE_OR_NONINTEGER_SALES')
            qty = int(qty)
            month = dt.date.fromisoformat(str(value['统计日期'])[:10]).replace(day=1)
            data[shop][sku, spu][month] += qty
            mappings[shop, sku].add(spu)
            source[shop, month][0] += 1
            source[shop, month][1] += qty
            counts['sales_rows'] += 1
            counts['explicit_zero_rows'] += qty == 0
            if qty > 0 and (shop, spu) not in first:
                first[shop, spu] = month
            elif qty > 0:
                first[shop, spu] = min(first[shop, spu], month)
    if any(len(v) != 1 for v in mappings.values()):
        raise ValueError('SKU_SPU_MAPPING_CONFLICT')
    season_map = {}
    for row in seasons:
        spu, season = row['SPU'], row['季节']
        if spu in season_map and season_map[spu] != season:
            raise ValueError('SEASON_CONFLICT')
        season_map[spu] = season
    if not season_map:
        raise ValueError('NO_EXPLICIT_ARCHIVED_SEASON_MAP')
    counts.update(season_rows=len(seasons), season_spus=len(season_map), source_months=len(source),
                  first_month=str(min(k[1] for k in source)), last_month=str(max(k[1] for k in source)))
    return data, first, source, season_map, counts, receipts


def replay_target(data, first, seasons, target, horizon, predict):
    cutoff = add_month(target, -horizon)
    rows, calculations = [], []
    for shop in SHOPS:
        xhistory, actual, observed = {}, defaultdict(int), set()
        expected = defaultdict(set)
        for (sku, spu), series in data.get(shop, {}).items():
            history = {'SPU': spu}
            history.update({label(month): qty for month, qty in series.items() if month < cutoff})
            if len(history) > 1:
                xhistory[sku] = history
                expected[spu].add(sku)
            if target in series:
                actual[spu] += series[target]
                observed.add(spu)
        if not xhistory:
            continue
        labels = [label(add_month(cutoff, i), True) for i in range(horizon + 1)]
        output = predict(xhistory, labels, dt.datetime(cutoff.year, cutoff.month, 1), spu_season_map=seasons)
        target_label = label(target, True)
        forecasts, valid_skus = defaultdict(int), defaultdict(set)
        for sku, values in output.items():
            if sku not in xhistory:
                raise ValueError('UNEXPECTED_SKU_OUTPUT')
            value = values.get(target_label)
            if value is not None:
                if int(value) != value or value < 0:
                    raise ValueError('INVALID_NATIVE_PREDICTION')
                spu = xhistory[sku]['SPU']
                forecasts[spu] += int(value)
                valid_skus[spu].add(sku)
        calculations.append(dict(target_month=target.isoformat()[:7], cutoff_month=cutoff.isoformat()[:7],
            horizon=f'H{horizon}', shop=shop, input_skus=len(xhistory), output_skus=len(output),
            complete_input_month_end=str(cutoff - dt.timedelta(days=1)), kind='DETERMINISTIC_NATIVE_CALL_NO_FIT'))
        for spu in sorted(expected):
            first_positive = first.get((shop, spu))
            if first_positive is None or first_positive >= cutoff:
                rows.append(dict(target_month=target.isoformat()[:7],cutoff_month=cutoff.isoformat()[:7],
                    horizon=f'H{horizon}',shop=shop,spu=spu,qualification='NO_PRIOR_POSITIVE',
                    prediction=None,actual=actual[spu] if spu in observed else None,
                    truth_status='OBSERVED' if spu in observed else 'UNKNOWN_NO_MONTH_ROW'))
                continue
            age, cls = age_class(first_positive, cutoff)
            complete = valid_skus[spu] == expected[spu]
            predicted = forecasts[spu] if complete else None
            truth = actual[spu] if spu in observed else None
            error = predicted - truth if predicted is not None and truth is not None else None
            rows.append(dict(target_month=target.isoformat()[:7],cutoff_month=cutoff.isoformat()[:7],
                input_cutoff=str(cutoff-dt.timedelta(days=1)),horizon=f'H{horizon}',shop=shop,spu=spu,
                observed_first_positive_month=str(first_positive),observed_age_months=age,age_class=cls,
                qualification='ESTABLISHED_OBSERVED_4P' if age >= 4 else 'RECENT_OBSERVED_0_TO_3',
                prediction=predicted,prediction_status='OUTPUT' if complete else 'INCOMPLETE_SKU_OUTPUT',
                input_sku_n=len(expected[spu]),output_sku_n=len(valid_skus[spu]),
                actual=truth,truth_status='EXPLICIT_ZERO' if truth == 0 else 'OBSERVED' if truth is not None else 'UNKNOWN_NO_MONTH_ROW',
                current_season=seasons.get(spu,'未知'),error=error,
                absolute_error=abs(error) if error is not None else None,
                hit20=(abs(error)*5 <= truth) if error is not None and truth > 0 else None,
                hit30=(abs(error)*10 <= 3*truth) if error is not None and truth > 0 else None,
                decision='UNJUDGED' if error is None else 'ZERO_ACTUAL_ABSOLUTE_ERROR' if truth == 0 else 'HIT' if abs(error)*5 <= truth else 'MISS'))
    return rows, calculations


def summary_rows(frame):
    mature = frame[frame.qualification.eq('ESTABLISHED_OBSERVED_4P')]
    rows = []
    for dimensions in (['target_month','horizon'], ['target_month','horizon','shop']):
        for key, group in mature.groupby(dimensions):
            scored = group[group.prediction.notna() & group.actual.notna()]
            actual = float(scored.actual.sum())
            over = float(scored.error.clip(lower=0).sum())
            under = float((-scored.error.clip(upper=0)).sum())
            positive = group[group.actual.gt(0)]
            rows.append(dict(zip(dimensions,key)) | dict(scope='ESTABLISHED_OBSERVED_4P',model='V4_NATIVE_RECONSTRUCTED_EXTENSION',
                group_by='+'.join(dimensions),eligible_n=len(group),output_n=int(group.prediction.notna().sum()),
                truth_n=int(group.actual.notna().sum()),scored_n=len(scored),actual_units=actual,
                forecast_units=float(scored.prediction.sum()),over_units=over,under_units=under,
                wape=(over+under)/actual if actual else None,bias=(over-under)/actual if actual else None,
                positive_truth_n=len(positive),hit20_n=int(positive.hit20.eq(True).sum()),
                hit20_rate=float(positive.hit20.eq(True).sum()/len(positive)) if len(positive) else None,
                zero_actual_n=int(group.actual.eq(0).sum()),unknown_actual_n=int(group.actual.isna().sum()),
                missing_output_n=int(group.prediction.isna().sum())))
    return rows


def run(archive, repo, out):
    archive, repo, out = Path(archive).resolve(), Path(repo).resolve(), Path(out).resolve()
    if out.exists():
        raise ValueError('OUTPUT_EXISTS_NO_OVERWRITE')
    source = repo/'jobs/feishu/forecast_sales_improved.py'
    mapping = repo/'jobs/feishu/generate_forecast_comparison.py'
    wrapper = repo/'scripts/audit_mature_spu_accuracy_readonly.py'
    candidate = repo/'docs/a16_candidate_v3_manifest.json'
    files = [archive, source, mapping, wrapper, candidate, Path(__file__).resolve()]
    before = {str(p):sha(p) for p in files}
    if before[str(archive)] != FROZEN_INPUT:
        raise ValueError('ARCHIVE_HASH_MISMATCH')
    manifest = json.loads(candidate.read_text(encoding='utf-8-sig'))
    if manifest['sources']['jobs/feishu/forecast_sales_improved.py']['normalized_sha256'] != FROZEN_V4_LF:
        raise ValueError('CANDIDATE_SOURCE_BINDING_MISMATCH')
    commit = subprocess.run(['git','rev-parse','HEAD'],cwd=repo,text=True,check=True,capture_output=True).stdout.strip()
    predict = pure_v4(source)
    data, first, monthly_source, seasons, quality, receipts = read_archive(archive,mapping_functions(mapping))
    targets = [dt.date.fromisoformat(m+'-01') for m in MONTHS]
    if any(monthly_source[shop,target][0] == 0 for target in targets for shop in SHOPS):
        raise ValueError('ENTIRE_SHOP_MONTH_TRUTH_MISSING')
    out.mkdir(parents=True)
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    execution = dict(status='STARTED',started_at=started,source_commit=commit,source_hashes=before,
        normalized_v4_sha256=FROZEN_V4_LF,classification='RECONSTRUCTED_NATIVE_MONTHLY_REPLAY_CURRENT_METADATA',
        target_months=list(MONTHS),existing_months_not_rerun=list(EXISTING),horizons=[1,2,3],shops=list(SHOPS),
        source_quality=quality,original_acquisition_receipts=receipts,native_call_limit=144,
        fits=0,database_queries=0,parameter_searches=0,a16_recomputed=False,
        limitations=['Archived current season and source mappings are not historical point-in-time metadata.',
          'Observed sales history begins in 2024 and may left-truncate observed age.',
          'Native V4 internal missing-month defaults remain frozen; missing actual rows are never scored as zero.',
          'Full SKU context is used before SPU aggregation; not trailing-90-day sales-core scope.',
          'No daily proration, no pooled daily-60-day or different-scope A16 ranking.'])
    dump(out/'execution_manifest.json',execution)
    rows, calls = [], []
    for target in targets:
        for horizon in (1,2,3):
            output, details = replay_target(data,first,seasons,target,horizon,predict)
            rows.extend(output);calls.extend(details)
            print(json.dumps({'target':str(target),'horizon':horizon,'objects':len(output),'native_calls':len(details)}),flush=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(out/'native_v4_detail.csv',index=False,encoding='utf-8-sig')
    frame.to_parquet(out/'native_v4_detail.parquet',index=False)
    pd.DataFrame(summary_rows(frame)).to_csv(out/'native_v4_summary.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(calls).to_csv(out/'native_calculations.csv',index=False,encoding='utf-8-sig')
    if any(sha(p) != before[str(p)] for p in files):
        raise ValueError('READ_ONLY_SOURCE_CHANGED')
    if len(calls) > 144:
        raise ValueError('NATIVE_CALL_BUDGET_EXCEEDED')
    execution.update(status='COMPLETE',finished_at=dt.datetime.now(dt.timezone.utc).isoformat(),source_unchanged=True,
        native_calculation_count=len(calls),output_rows=len(frame),
        files={p.name:sha(p) for p in out.iterdir() if p.is_file() and p.name!='execution_manifest.json'})
    dump(out/'execution_manifest.json',execution)
    return {'status':'COMPLETE','output_rows':len(frame),'native_calculations':len(calls),'fits':0}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('archive','repo','out'):
        parser.add_argument('--'+name,required=True,type=Path)
    print(json.dumps(run(**vars(parser.parse_args())),ensure_ascii=False))
