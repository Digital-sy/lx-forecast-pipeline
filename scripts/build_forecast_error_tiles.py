"""Offline presentation adapter. Never fit, infer, or query a forecast model."""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

KEY = ['issue_date', 'shop', 'spu', 'window']
WINDOWS = {'d01_30': '前30天', 'd31_60': '后30天', 'd01_60': '累计60天'}
MODELS = {
    'F28': '近28日均销基线', 'WEEKDAY4': '近4周星期基线',
    'B1': 'B1 日级候选', 'B2': 'B2 扩展候选', 'B3': 'B3 扩展候选',
    'DIRECT30x2': '直接分段预测', 'DIRECT_CONTEXT90': '中期销量背景',
    'DIRECT_YEARAGO': '年度参考', 'DIRECT_YEARAGO_GATE_V1': '年度适用筛选',
    'DIRECT_YEARAGO_BLEND50': '年度半量修正',
}
W, H, PAD, GAP = 480, 320, 6, 1


def sha(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''): h.update(b)
    return h.hexdigest()


def read_json(p): return json.loads(Path(p).read_text(encoding='utf-8'))


def dump(p, obj):
    Path(p).write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def visible(actual, window):
    return actual is not None and math.isfinite(actual) and actual >= (20 if window == 'd01_60' else 10)


def stats(rows, model):
    known = [r for r in rows if r['actual'] is not None and r['truth'] == 'OBSERVED']
    valid = [r for r in known if r['values'][model]['state'] == 'OK']
    den = math.fsum(r['actual'] for r in valid)
    ae = math.fsum(abs(r['values'][model]['p'] - r['actual']) for r in valid)
    hit = [r for r in valid if r['actual'] > 0 and abs(r['values'][model]['p'] - r['actual']) / r['actual'] <= .2]
    black = [r for r in valid if r['actual'] > 0 and abs(r['values'][model]['p'] - r['actual']) / r['actual'] > 1]
    return dict(n=len(known), products=len({(r['shop'], r['spu']) for r in known}),
        output=len(valid), missing=sum(r['values'][model]['state'] == 'NO_PREDICTION' for r in known),
        abnormal=sum(r['values'][model]['state'] not in ['OK', 'NO_PREDICTION'] for r in known),
        actual=math.fsum(r['actual'] for r in known), scoredActual=den, ae=ae, wape=ae / den if den else None,
        hit=len(hit), hitRate=len(hit) / len(known) if known else None, black=len(black),
        blackActual=math.fsum(r['actual'] for r in black))


def pack(rows, k):
    """Stable descending shelves; side squared equals k*actual, never floored."""
    x, y, shelf = float(PAD), float(PAD), 0.
    out = {}
    for r in sorted(rows, key=lambda q: (-q['actual'], q['shop'], q['spu'], q['issue'])):
        s = math.sqrt(k * r['actual'])
        if x + s > W - PAD + 1e-10: x, y, shelf = float(PAD), y + shelf + GAP, 0.
        if y + s > H - PAD + 1e-10 or s > W - 2 * PAD: return None
        out[r['id']] = [x, y, s]
        x += s + GAP
        shelf = max(shelf, s)
    return out


def make_layout(rows):
    selected = [r for r in rows if visible(r['actual'], r['window']) and r['truth'] == 'OBSERVED']
    groups = {}
    for r in selected: groups.setdefault(r['month'], []).append(r)
    if not selected: return dict(k=0, coordinates={})
    # Feasibility is monotone for this sorted shelf packing with fixed spacing.
    lo, hi = 0., min(W - 2 * PAD, H - 2 * PAD) ** 2 / max(r['actual'] for r in selected)
    for _ in range(55):
        mid = (lo + hi) / 2
        if all(pack(v, mid) is not None for v in groups.values()): lo = mid
        else: hi = mid
    k = lo * .985  # one global margin, not a per-month adjustment
    coords = {}
    for v in groups.values(): coords.update(pack(v, k))
    return dict(k=k, coordinates=coords)


def build(workspace, output):
    root, out = Path(workspace).resolve(), Path(output).resolve()
    if not root.is_dir() or out.exists(): raise ValueError('Existing parent and new exclusive output required')
    repo = Path(__file__).resolve().parents[1]
    sources, frames, receipts, audits = {}, {}, [], []
    configs = [
        ('monthly', 'bc60_monthly_replay_20261020', 'scoring', 'score_manifest.json', 'validation/validation.json', list(MODELS)[:6], '34bdc3b703c45326f3c638814ad18595c8124e83'),
        ('context', 'direct_context90_20261020_v1', 'scoring', 'score_manifest.json', 'validation/validation.json', ['DIRECT_CONTEXT90'], None),
        ('annual', 'direct_yearago_20261020_v1', 'scoring_reviewed', 'score_manifest.json', 'validation_reviewed/validation.json', ['DIRECT_YEARAGO'], '83e0baa32f6de5cea6ff38e19c87f56fba67daba'),
        ('gate', 'yearago_gate_v1_20261020', 'scoring', 'manifest.json', 'validation/validation.json', ['DIRECT_YEARAGO_GATE_V1'], '601e3d2c1f592165141d526eb9b6571dd557b0aa'),
        ('blend', 'direct_yearago_blend50_20261021_v1', 'scoring', 'manifest.json', 'validation/validation.json', ['DIRECT_YEARAGO_BLEND50'], '4ea35ad10b1c6d059e0b7f8249136b11b49f1fbb'),
    ]
    model_info = []
    for name, dirname, sub, mfname, vf, keep, commit in configs:
        base = root / dirname
        if name == 'blend' and not (base / vf).exists():
            audits.append(dict(kind='MODEL_UNAVAILABLE', model=keep[0], note='尚无已验收结果'))
            continue
        manifest_path, validation_path = base / sub / mfname, base / vf
        m, v = read_json(manifest_path), read_json(validation_path)
        if not str(v['status']).startswith('PASS'): raise ValueError('Validation not passed:' + dirname)
        recorded_manifest = v.get('score_manifest_sha256', v.get('scoring_manifest_sha256'))
        if recorded_manifest is None and name == 'monthly': recorded_manifest = v['scores']['score_manifest_sha256']
        if recorded_manifest and sha(manifest_path) != recorded_manifest: raise ValueError('Validation manifest mismatch:' + dirname)
        for file, h in m['files'].items():
            p = base / sub / file
            if sha(p) != h: raise ValueError('Changed archived file:' + str(p))
            sources[str(p)] = h
        for p in [manifest_path, validation_path]: sources[str(p)] = sha(p)
        if name == 'gate':
            p = base / 'final_review/review.json'; rv = read_json(p)
            if rv['status'] != 'PASS': raise ValueError('Final GATE review incomplete')
            sources[str(p)] = sha(p)
        f = pd.read_parquet(base / sub / 'scores.parquet')
        if name == 'monthly': f = f[f.cohort_id.eq('monthly_core')].copy()
        if f.duplicated(KEY + ['model']).any(): raise ValueError('Duplicate compound key:' + dirname)
        frames[name] = f
        receipts.append(dict(source=name, archive=dirname, scores=str(base / sub / 'scores.parquet'),
            scores_sha256=sha(base / sub / 'scores.parquet'), manifest_sha256=sha(manifest_path),
            validation_sha256=sha(validation_path), validation_status=v['status'], reference_commit=commit))
        for model in keep:
            q = f[f.model.eq(model)]
            if q.empty: raise ValueError('Expected model absent:' + model)
            model_info.append(dict(id=model, label=MODELS[model], source=name,
                mature_periods=int(q.loc[q.truth_status.eq('OBSERVED'), 'issue_date'].nunique()),
                periods=int(q.issue_date.nunique()), manifest=sha(manifest_path), reference_commit=commit,
                status='已验收归档；历史重建回放'))
    original = frames['monthly']
    master = original[original.model.eq('F28')].copy().set_index(KEY).sort_index()
    baseline = original[original.model.eq('DIRECT30x2')].set_index(KEY).sort_index()
    anomalies = []
    quarantined = set()
    for name, f in frames.items():
        for model, q0 in f.groupby('model'):
            q = q0.set_index(KEY).sort_index()
            if not q.index.equals(master.index): raise ValueError('Scope not identical:' + name + ':' + model)
            for field in ['target_start', 'target_end', 'actual', 'truth_status']:
                ok = q[field].eq(master[field]) | (q[field].isna() & master[field].isna())
                for key in q.index[~ok]:
                    quarantined.add(key)
                    anomalies.append(dict(kind='TRUTH_OR_WINDOW_CONFLICT', source=name, model=model, field=field, key=list(key)))
            if model == 'DIRECT30x2':
                eq = q.prediction.eq(baseline.prediction) | (q.prediction.isna() & baseline.prediction.isna())
                diff = (q.prediction - baseline.prediction).abs()
                audits.append(dict(kind='DUPLICATE_DIRECT', source=name, identical_n=int(eq.sum()),
                    binary_difference_n=int((~eq).sum()), max_absolute_difference=float(diff.max()),
                    selected='monthly original, never replace with copied duplicate'))
                if (diff > 1e-8).any() or q.prediction.isna().ne(baseline.prediction.isna()).any():
                    raise ValueError('Material duplicate DIRECT conflict')
    meta = frames['annual'][frames['annual'].model.eq('DIRECT30x2')].set_index(KEY).sort_index()
    selected = {}
    for info in model_info:
        selected[info['id']] = frames[info['source']].loc[lambda f: f.model.eq(info['id'])].set_index(KEY)
    routing = None
    rp = root / 'yearago_gate_v1_20261020/routing.parquet'
    rm = read_json(rp.parent / 'routing_manifest.json')
    if sha(rp) != rm['routing_sha256']: raise ValueError('Routing origin evidence changed')
    sources[str(rp)] = sha(rp); sources[str(rp.parent / 'routing_manifest.json')] = sha(rp.parent / 'routing_manifest.json')
    routing = pd.read_parquet(rp).set_index(KEY[:3])
    rows = []
    for rid, (key, row) in enumerate(master.iterrows()):
        issue, shop, spu, win = key
        actual = None if pd.isna(row.actual) else float(row.actual)
        truth = str(row.truth_status)
        if key in quarantined: actual, truth = None, 'SOURCE_CONFLICT'
        if actual is not None and (not math.isfinite(actual) or actual < 0): actual, truth = None, 'INVALID_ACTUAL'
        mr = meta.loc[key]
        item = dict(id=rid, issue=issue, month=issue[:7], shop=shop, spu=spu, window=win,
            start=str(row.target_start), end=str(row.target_end), cutoff=str(row.input_cutoff), actual=actual, truth=truth,
            category=str(mr.current_category) if pd.notna(mr.current_category) else 'UNKNOWN',
            season=str(mr.current_season) if pd.notna(mr.current_season) else 'UNKNOWN',
            maturity=str(mr.maturity) if pd.notna(mr.maturity) else 'UNKNOWN', values={})
        for model, f in selected.items():
            z = f.loc[key]; p = None if pd.isna(z.prediction) else float(z.prediction)
            status = 'NO_PREDICTION' if p is None else 'OK' if math.isfinite(p) and p >= 0 else 'INVALID_PREDICTION'
            if status == 'INVALID_PREDICTION': anomalies.append(dict(kind=status, model=model, key=list(key), value=str(p))); p = None
            if key in quarantined: status = 'SOURCE_CONFLICT'
            origin = str(z.prediction_status)
            if model == 'DIRECT_YEARAGO':
                origin = str(z.M1_origin) if win == 'd01_30' else str(z.M2_origin) if win == 'd31_60' else str(z.M1_origin) + ' / ' + str(z.M2_origin)
            if model in ['DIRECT_YEARAGO_GATE_V1', 'DIRECT_YEARAGO_BLEND50'] and key[:3] in routing.index:
                rr = routing.loc[key[:3]]
                origin = ('年度适用 / ' if rr.gate_decision else 'DIRECT原值 / ') + str(rr.main_reason)
                if model.endswith('BLEND50') and rr.gate_decision: origin = '固定半量年度修正 / ' + str(rr.main_reason)
            item['values'][model] = dict(p=p, state=status, archivedStatus=str(z.prediction_status), origin=origin)
        rows.append(item)
    layouts = {win: make_layout([r for r in rows if r['window'] == win]) for win in WINDOWS}
    filter_audit, unfiltered = [], []
    for win in WINDOWS:
        wr = [r for r in rows if r['window'] == win]
        known = [r for r in wr if r['actual'] is not None and r['truth'] == 'OBSERVED']
        hidden = [r for r in known if not visible(r['actual'], win)]
        kept = [r for r in known if visible(r['actual'], win)]
        total = math.fsum(r['actual'] for r in known)
        filter_audit.append(dict(window=win, all_object_issues=len(wr), mature_known=len(known),
            not_matured=sum(r['truth'] == 'NOT_MATURED' for r in wr), unknown=sum(r['truth'] not in ['OBSERVED', 'NOT_MATURED'] for r in wr),
            hidden_n=len(hidden), hidden_actual=math.fsum(r['actual'] for r in hidden), shown_n=len(kept),
            kept_actual=math.fsum(r['actual'] for r in kept), known_actual=total,
            kept_actual_share=math.fsum(r['actual'] for r in kept) / total if total else None))
        for model in selected: unfiltered.append(dict(window=win, model=model, **stats(known, model)))
    for p, h in sources.items():
        if sha(p) != h: raise ValueError('Source changed during build:' + p)
    at = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
    data = dict(version='forecast_error_tiles_v1', generatedAt=at, models=model_info, windows=WINDOWS,
        width=W, height=H, rows=rows, layouts=layouts, filterAudit=filter_audit, unfiltered=unfiltered,
        sources=receipts, anomalies=anomalies, duplicateAudit=audits,
        evidence='RECONSTRUCTED_HISTORICAL_DEVELOPMENT_NOT_INDEPENDENT_OOS',
        budgets=dict(new_fits=0, new_inference=0, database_queries=0, production_changes=0))
    out.mkdir(exist_ok=False)
    dump(out / 'reviewed_data.json', data)
    dump(out / '数据来源及模型覆盖清单.json', dict(models=model_info, sources=receipts, duplicates=audits,
        anomalies=anomalies, source_hashes=sources, generatedAt=at))
    pd.DataFrame(filter_audit).to_csv(out / '过滤审计.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(unfiltered).to_csv(out / '原全量指标复核.csv', index=False, encoding='utf-8-sig')
    assets = repo / 'assets/forecast_error_tiles'
    template = (assets / 'index.html').read_text(encoding='utf-8')
    raw = json.dumps(data, ensure_ascii=False, separators=(',', ':'), allow_nan=False).replace('&', '\\u0026').replace('<', '\\u003c').replace('>', '\\u003e').replace('\u2028', '\\u2028').replace('\u2029', '\\u2029')
    html = template.replace('/*__STYLE__*/', (assets / 'style.css').read_text(encoding='utf-8')).replace('/*__APP__*/', (assets / 'app.js').read_text(encoding='utf-8')).replace('__REVIEWED_JSON__', raw)
    (out / '销量预测误差方块图.html').write_text(html, encoding='utf-8')
    dump(out / 'build_manifest.json', dict(at=at, source_hashes=sources, code_sha256=sha(__file__),
        assets={p.name: sha(p) for p in assets.iterdir() if p.is_file()}, rows=len(rows), models=len(model_info),
        files={p.name: sha(p) for p in out.iterdir() if p.is_file()}, budgets=data['budgets']))
    print(json.dumps(dict(output=str(out), models=len(model_info), rows=len(rows), filter_audit=filter_audit), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('--workspace', required=True); p.add_argument('--output', required=True)
    build(**vars(p.parse_args()))
