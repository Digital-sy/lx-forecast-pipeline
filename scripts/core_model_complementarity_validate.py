"""Independent scalar reconciliation; does not import analysis helpers."""
import argparse
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

MODELS = ['F28','DIRECT','CONTEXT90','YEARAGO','GATE','BLEND50']
WINDOWS = ['d01_30','d31_60','d01_60']
KEY = ['issue_date','shop','spu']


def sha(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4194304),b''): h.update(b)
    return h.hexdigest()


def validate(out):
    m = json.loads((out/'manifest.json').read_text(encoding='utf-8'))
    for p, digest in {**m['sources'], **m['protected_shadow_files']}.items(): assert sha(p)==digest, p
    for rel, digest in m['files'].items(): assert sha(out/rel)==digest, rel
    d = pd.read_parquet(out/'historical_windows.parquet')
    # Reload exact original forecasts and independently verified truth: an output
    # copy alone is not sufficient evidence of source fidelity.
    aliases={'DIRECT30x2':'DIRECT','DIRECT_CONTEXT90':'CONTEXT90','DIRECT_YEARAGO':'YEARAGO',
             'DIRECT_YEARAGO_GATE_V1':'GATE','DIRECT_YEARAGO_BLEND50':'BLEND50','F28':'F28'}
    source_records = {}
    for p in m['sources']:
        if not p.endswith('scores.parquet'): continue
        original = pd.read_parquet(p)
        for row in original.to_dict('records'):
            if 'direct_context90_' in p and row['model']!='DIRECT_CONTEXT90':continue
            date = str(row['issue_date'])[:10]
            if not '2025-01-01' <= date < '2026-09-01': continue
            k=(date,row['shop'],row['spu'],aliases[row['model']],row['window'])
            values=(row['prediction'],row['actual'],str(row['target_start'])[:10],str(row['target_end'])[:10])
            if k in source_records:
                assert all(a==b or (isinstance(a,float) and isinstance(b,float) and math.isnan(a) and math.isnan(b))
                           for a,b in zip(values,source_records[k])), k
            source_records[k]=values
    truth_path = next(p for p in m['sources'] if p.endswith('independent_daily_truth.csv'))
    truths={tuple(r[k] for k in KEY)+(r['window'],):r['actual']
            for r in pd.read_csv(truth_path,float_precision='round_trip').to_dict('records')}
    groups=defaultdict(dict)
    for r in d.to_dict('records'):
        key=tuple(r[k] for k in KEY)
        assert '2025-01-01' <= key[0] < '2026-09-01'
        p,y=r['prediction'],r['actual']; original=source_records[key+(r['model'],r['window'])]
        assert (p==original[0] or math.isnan(p) and math.isnan(original[0])) and y==original[1]
        assert str(r['target_start'])[:10]==original[2] and str(r['target_end'])[:10]==original[3]
        assert y==truths[key+(r['window'],)]
        groups[key][(r['model'],r['window'])]=(p,y)
    output_objects=pd.read_parquet(out/'objects.parquet').set_index(KEY)
    summary=defaultdict(lambda:defaultdict(float)); signed=defaultdict(lambda:[0,0.0,0.0])
    rank=defaultdict(float); model_rescues=defaultdict(lambda:[0,0.0]); n=0
    for key, rows in groups.items():
        assert len(rows)==18
        complete={mod:all(math.isfinite(rows[mod,w][0]) for w in WINDOWS) for mod in MODELS}
        joint={mod:all(math.isfinite(rows[mod,w][0]) and rows[mod,w][1]>0 and
                      abs(rows[mod,w][0]-rows[mod,w][1])/rows[mod,w][1]<=.2 for w in WINDOWS) for mod in MODELS}
        actual60=rows['DIRECT','d01_60'][1]; rescued=not joint['BLEND50'] and any(joint.values())
        o=output_objects.loc[key]
        assert o.blend_rescued==rescued and o.any_joint_hit==any(joint.values())
        for mod in MODELS: assert o[mod+'_joint_hit']==joint[mod] and o[mod+'_output_complete']==complete[mod]
        for scope in ['FULL_CORE']+(['PAIRED'] if complete['DIRECT'] else []):
            z=summary[scope]; z['records']+=1; z['actual60']+=actual60
            for flag,prefix in [(joint['BLEND50'],'blend_hit'),(rescued,'rescue_union'),(any(joint.values()),'any_model_hit'),
                                (all(complete.values()) and not any(joint.values()),'all_six_output_no_joint_hit')]:
                if flag:z[prefix+'_n']+=1;z[prefix+'_actual60']+=actual60
            for mod in MODELS[:-1]:
                if not joint['BLEND50'] and joint[mod]:
                    model_rescues[scope,mod][0]+=1;model_rescues[scope,mod][1]+=actual60
            if all(complete.values()) and not any(joint.values()):
                for w in WINDOWS:
                    y=rows['DIRECT',w][1]; errors=[rows[mod,w][0]-y for mod in MODELS]
                    if y==0: direction='ZERO_ACTUAL'
                    elif min(errors)>0:direction='ALL_OVER'
                    elif max(errors)<0:direction='ALL_UNDER'
                    elif min(errors)<0<max(errors):direction='OPPOSITE'
                    elif min(errors)==max(errors)==0:direction='ALL_EXACT'
                    elif min(errors)>=0:direction='OVER_OR_EXACT'
                    else:direction='UNDER_OR_EXACT'
                    v=signed[scope,w,direction];v[0]+=1;v[1]+=y;v[2]+=actual60
        if complete['DIRECT']:rank[key[1:]]+=abs(rows['BLEND50','d01_60'][0]-actual60)
        n+=1
    actual_summary=pd.read_csv(out/'scope_summary.csv',float_precision='round_trip').set_index('scope')
    for scope, r in summary.items():
        for field,v in r.items(): assert actual_summary.loc[scope,field]==v,(scope,field)
    dr=pd.read_csv(out/'all_miss_direction_summary.csv',float_precision='round_trip').set_index(['scope','window','direction'])
    for k,(count,actual,actual60) in signed.items():
        assert dr.loc[k,'records']==count and dr.loc[k,'actual']==actual and dr.loc[k,'actual60']==actual60,k
    mr=pd.read_csv(out/'rescue_by_model.csv',float_precision='round_trip').set_index(['scope','model'])
    for k,(count,actual) in model_rescues.items():assert mr.loc[k,'rescue_n']==count and mr.loc[k,'rescue_actual60']==actual
    expected=sorted(rank,key=lambda k:(-rank[k],*k))[:20]
    top=pd.read_csv(out/'top20_products.csv',float_precision='round_trip')
    assert list(zip(top.shop,top.spu))==expected
    for row in top.itertuples():assert math.isclose(row.absolute_error60,rank[row.shop,row.spu],rel_tol=1e-12,abs_tol=1e-8)
    res=dict(status='PASS',at=datetime.now(timezone.utc).isoformat(),method='independent scalar reconstruction without analysis helper imports',
        source_forecast_windows_checked=len(d), object_periods_checked=n, independent_truth_windows=len(groups)*3,
        scopes=list(summary), direction_cells_checked=len(signed), top20_recomputed=20,
        sources_unchanged=len(m['sources']), protected_shadow_files_unchanged=len(m['protected_shadow_files']),
        fits=0,inference=0,queries=0,validator_sha256=sha(__file__), manifest_sha256=sha(out/'manifest.json'))
    target=out/'independent_validation.json'
    if target.exists():raise ValueError('VALIDATION_ALREADY_EXISTS')
    target.write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(res),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();validate(Path(a.output))
