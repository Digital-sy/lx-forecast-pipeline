"""Offline, fixed-parameter tracing of an existing A16 run; no DB or prediction entrypoint.

Full input context is retained. Only requested object traces are persisted. This
never calls Candidate.predict/choose_params and never overwrites an output folder.
"""
from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.a16_candidate_v3 import Candidate, CutoffHistory, shift
from scripts.run_a16_shadow_once import prepare, verify_freeze

A3 = 'A3_SPU生命周期收缩'
A7 = 'A7_非对称季节门控'
A16 = 'A16_广度否决_高集中半衰减'


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def state(history, item, month):
    value = history.get((item, month))
    return {'month': str(month)[:7], 'value': value,
            'state': 'MISSING_UNKNOWN' if value is None else ('OBSERVED_ZERO' if value == 0 else 'OBSERVED_POSITIVE')}


def scalar_locals(values):
    def safe(x):
        if x is None or isinstance(x, (str, int, float, bool)):
            return x
        if isinstance(x, (dt.date, dt.datetime)):
            return str(x)
        if isinstance(x, (list, tuple)):
            return [safe(y) for y in x]
        return '<excluded>'
    return {k: safe(v) for k, v in values.items()
            if k not in ('actual', 'history', 'self') and not isinstance(v, dict)}


def fixed_stages(candidate, target, horizon, gamma_down, gamma_up):
    """Preserve the full candidate and full shop/category support, never retune."""
    features, audit = candidate.features(target, horizon)
    candidates = candidate.r5.add_a7(features, gamma_down, gamma_up)
    hist = CutoffHistory(candidate.observations, shift(target, -horizon))
    first = candidate.r11.first_sale_map(hist)
    eligible = [r for r in candidates if r['shop'] == 'JQ-US' and r['可预测性'] == 'ESTABLISHED']
    support = candidate.r12.category_support(hist, first, eligible)
    outputs = candidate.r12.apply_trust_gates(hist, first, eligible, support)
    return outputs, features, audit, hist, first


def independent_a7(a3, cand, phase, gd, gu):
    if a3 <= 0 or cand is None:
        return float(a3), 'A3_ONLY'
    ratio = cand / a3
    if ratio < .90:
        return (1-gd)*a3+gd*cand, 'DOWN'
    if ratio <= 1.10:
        return float(a3), 'NEUTRAL'
    if phase == 'RISING':
        return (1-gu)*a3+gu*min(cand, a3*1.50), 'UP_RISING'
    return float(a3), 'UP_BLOCKED'


def diagnose(input_path, original_dir, spus, output_dir):
    output_dir = Path(output_dir).resolve()
    original_dir = Path(original_dir).resolve()
    if output_dir.exists() or output_dir == original_dir or original_dir in output_dir.parents:
        raise ValueError('Require a new directory outside the original run')
    manifest = verify_freeze()
    data = json.loads(Path(input_path).read_text(encoding='utf-8'))
    meta = json.loads((original_dir/'shadow_run_meta.json').read_text(encoding='utf-8'))
    original_path = original_dir/'a16_v4_shadow_rows.csv'
    before_hash = sha(original_path)
    acceptance = json.loads((original_dir/'shadow_acceptance_summary.json').read_text(encoding='utf-8'))
    if before_hash != acceptance['csv_sha256']:
        raise ValueError('Original CSV fingerprint mismatch')
    if meta['candidate'] != manifest['candidate']:
        raise ValueError('Candidate mismatch')
    original = list(csv.DictReader(original_path.open(encoding='utf-8-sig', newline='')))
    idx = {(r['shop']+'|'+r['spu'], r['horizon']):r for r in original}
    if len(idx) != len(original):
        raise ValueError('Duplicate original business key')
    sku, hist, cats, seasons, months, input_hash = prepare(data['monthly'], data['catalog'], data['season'], meta['issue_month'])
    candidate = Candidate(hist, cats, ROOT)
    # Hard guard: this diagnostic cannot accidentally select parameters again.
    def forbidden(*args, **kwargs):
        raise RuntimeError('Parameter reselection / full shadow prediction forbidden')
    candidate.r5.choose_params = forbidden
    candidate.predict = forbidden
    items = {'JQ-US|'+s for s in spus}
    call_traces = {}
    def profile(frame, event, result):
        if event != 'return' or frame.f_code.co_name not in ('a2_direct_spu','a3_lifecycle_shrink','seasonal_candidate'):
            return
        loc = frame.f_locals
        if loc.get('spu') not in items:
            return
        key = (loc['spu'], str(loc['target'])[:7], frame.f_code.co_name)
        call_traces[key] = {'source': frame.f_code.co_filename,
                            'definition_line': frame.f_code.co_firstlineno,
                            'locals': scalar_locals(loc), 'returned': result}
    traces, contexts = [], []
    for model in meta['models']:
        h = int(model['horizon'][1:]); target = shift(model['target_month'], 0)
        cutoff = shift(target, -h)
        if str(cutoff)[:7] != meta['issue_month']:
            raise ValueError('Cutoff mismatch')
        gd, gu = float(model['gamma_down']), float(model['gamma_up'])
        old_profile = sys.getprofile()
        sys.setprofile(profile)
        try:
            outputs, features, audit, safe, first = fixed_stages(candidate,target,h,gd,gu)
        finally:
            sys.setprofile(old_profile)
        # Count original calibration eligibility without computing historical predictions.
        training = [(i,m) for i,m in hist if shift('2025-01',0)<=m<cutoff
                    and i in candidate.first and candidate.first[i]<shift(m,-h)]
        context = {'horizon':model['horizon'], 'feature_scope':len(features),
                   'eligible_full_shop_cohort':len(outputs), 'training_label_rows':len(training),
                   'training_min_target':str(min(m for i,m in training))[:7],
                   'training_max_target':str(max(m for i,m in training))[:7],
                   'gamma_down':gd,'gamma_up':gu,'parameters_source':'original shadow_run_meta.json',
                   'feature_missing_reads':audit['missing_reads'],
                   'scope_matches_original':len(features)==model['candidate_scope'] and len(outputs)==model['eligible_count'],
                   'training_count_matches_original':len(training)==model['training_rows']}
        contexts.append(context)
        for r in outputs:
            item = r['SPU']
            if item not in items:
                continue
            spu = item.split('|',1)[1]
            a2_trace = call_traces[(item,model['target_month'],'a2_direct_spu')]
            a3_trace = call_traces[(item,model['target_month'],'a3_lifecycle_shrink')]
            season_trace = call_traces[(item,model['target_month'],'seasonal_candidate')]
            loc = season_trace['locals']; a3loc = a3_trace['locals']
            recent = [state(hist,item,shift(cutoff,-i)) for i in (3,2,1)]
            ly_recent = [state(hist,item,shift(cutoff,-i-12)) for i in (3,2,1)]
            ly_target = state(hist,item,shift(target,-12))
            raw_growth = loc['recent_sum']/loc['ly_recent_sum']
            raw_season = loc['yoy_target']*loc['growth']
            pre_a7, rule = independent_a7(r[A3],r['季节候选'],r['历史季节阶段'],gd,gu)
            baseline = idx[(item,model['horizon'])]
            history_list = [state(hist,item,shift('2024-01',i)) for i in range((cutoff.year-2024)*12+cutoff.month-1)]
            source_months = recent+ly_recent+[ly_target]
            anomaly = candidate.r11.spu_history_features(safe,first,item,target)
            row = {'shop':'JQ-US','spu':spu,'horizon':model['horizon'],'target_month':model['target_month'],
                   'issue_month':meta['issue_month'],'input_kind':data['kind'],'input_hash':input_hash,
                   'input_hash_matches_original':input_hash==meta['input_hash'],
                   'observed_first_positive':str(candidate.first[item])[:7],
                   'observable_age_months':(cutoff.year-candidate.first[item].year)*12+cutoff.month-candidate.first[item].month,
                   'forecastability':r['可预测性'],'lifecycle':r['生命周期'],'demand_profile':r['需求形态'],
                   'category_current':cats.get(spu),'season_current':seasons.get(spu),
                   'metadata_basis':'CURRENT_NOT_HISTORICAL_POINT_IN_TIME',
                   'recent_months':recent,'last_year_comparable_months':ly_recent,'last_year_target':ly_target,
                   'recent_sum':loc['recent_sum'],'ly_recent_sum':loc['ly_recent_sum'],
                   'growth_raw':raw_growth,'growth_clipped':loc['growth'],'growth_bounds':[.6,1.6],
                   'seasonal_candidate_pre_round':raw_season,'seasonal_candidate':r['季节候选'],
                   'seasonal_ratio_to_a3':r['季节候选比A3'],'historical_phase':r['历史季节阶段'],
                   'a0':a3loc['a0'],'a2':a2_trace['returned'],'a3_weight':a3loc['w'],
                   'a2_after_lifecycle_rule':a3loc['a2'],
                   'a3_bounds_ratio':[a3loc.get('lo'),a3loc.get('hi')],
                   'a3_pre_clip':a3loc['a0']+a3loc['w']*(a3loc['a2']-a3loc['a0']),
                   'a3_pre_round':a3loc['pred'],'a3':r[A3],
                   'gamma_down':gd,'gamma_up':gu,'a7_rule':r['A7规则'],'a7_pre_round':pre_a7,'a7':r[A7],
                   'a16_rule':r['A16规则'],'a16':r[A16],
                   'a7_minus_a3':r[A7]-r[A3],'a16_minus_a7':r[A16]-r[A7],
                   'a16_minus_a3':r[A16]-r[A3],
                   'original_a16':int(baseline['a16_shadow']),'original_v4_separate_path':int(baseline['v4_replay']),
                   'a16_match_original':r[A16]==int(baseline['a16_shadow']),
                   'independent_a7_check':int(round(pre_a7))==r[A7] and rule==r['A7规则'],
                   'independent_a3_check':int(round(max(a3loc['lo']*a3loc['a0'],min(a3loc['hi']*a3loc['a0'],a3loc['a0']+a3loc['w']*(a3loc['a2']-a3loc['a0'])))))==r[A3],
                   'independent_seasonal_check':all(x['value'] is not None for x in source_months) and int(round(ly_target['value']*max(.6,min(1.6,sum(x['value'] for x in recent)/sum(x['value'] for x in ly_recent)))))==r['季节候选'],
                   'delta_conservation':r[A7]-r[A3]+r[A16]-r[A7]==r[A16]-r[A3],
                   'key_numeric_input_missing_count':sum(x['state']=='MISSING_UNKNOWN' for x in source_months),
                   'category_support':{k:v for k,v in r.items() if k.startswith('类目_')},
                   'spu_historical_flags':anomaly,'observed_history':history_list,
                   'original_function_traces':{'a2':a2_trace,'a3':a3_trace,'seasonal':season_trace}}
            traces.append(row)
    if len(traces)!=len(spus)*len(meta['models']):
        raise ValueError('Requested output missing; no zero filling')
    if sha(original_path)!=before_hash:
        raise RuntimeError('Original file changed')
    summary = {'classification':'RECONSTRUCTED_INPUT_DIAGNOSTIC',
               'original_input_archive_found':False,
               'original_archival_reproduction':'NOT_PASSED_ORIGINAL_INPUT_NOT_PERSISTED',
               'canonical_input_hash_match':input_hash==meta['input_hash'],
               'input_hash':input_hash,'original_input_hash':meta['input_hash'],
               'raw_reconstructed_file_sha256':sha(input_path),
               'original_csv_sha256':before_hash,'original_csv_unchanged':True,
               'source_manifest_passed':True,'parameter_reselection':False,'historical_backtest_rerun':False,
               'contexts':contexts,'trace_rows':len(traces),
               'all_final_match_original':all(r['a16_match_original'] for r in traces),
               'all_stage_conservation':all(r['delta_conservation'] for r in traces),
               'all_independent_a3_check':all(r['independent_a3_check'] for r in traces),
               'all_independent_seasonal_check':all(r['independent_seasonal_check'] for r in traces),
               'all_independent_a7_check':all(r['independent_a7_check'] for r in traces)}
    output_dir.mkdir(parents=True)
    (output_dir/'trace.json').write_text(json.dumps(traces,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    (output_dir/'audit.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    flat=[]
    for row in traces:
        flat.append({k:json.dumps(v,ensure_ascii=False,default=str) if isinstance(v,(list,dict)) else v for k,v in row.items()})
    with (output_dir/'trace.csv').open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(flat[0]));writer.writeheader();writer.writerows(flat)
    return summary


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--input-json',required=True)
    ap.add_argument('--original-dir',required=True)
    ap.add_argument('--spus',required=True,help='Comma separated; filter only after full group context')
    ap.add_argument('--output-dir',required=True)
    args=ap.parse_args()
    print(json.dumps(diagnose(args.input_json,args.original_dir,args.spus.split(','),args.output_dir),ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
