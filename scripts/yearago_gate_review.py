"""Final source-origin and threshold review; no recomputation of model outputs."""
import argparse
import json
import math
from pathlib import Path
import subprocess
import sys
import pandas as pd
try:
    from scripts.yearago_gate_route import KEY,RULE,sha,read_json,dump,now,verify
except ModuleNotFoundError:
    from yearago_gate_route import KEY,RULE,sha,read_json,dump,now,verify


def review(output):
    out=Path(output).resolve();dest=out/'final_review'
    if dest.exists():raise ValueError('REVIEW_EXISTS')
    rm=read_json(out/'routing_manifest.json');y=Path(rm['yearago'])
    routing=pd.read_parquet(out/'routing.parquet')
    count=0
    for issue in sorted(routing.issue_date.unique()):
        for seg in ['M1','M2']:
            p=y/'effects'/f'{issue}_{seg}'/'predictions.parquet'
            a=pd.read_parquet(p,columns=['shop','spu','annual_model_prediction','comparison_prediction','prediction_origin'])
            r=routing[routing.issue_date.eq(issue)].merge(a,on=['shop','spu'],validate='one_to_one')
            assert len(r)==len(routing[routing.issue_date.eq(issue)])
            for old,new in [('annual_model_prediction','annual_'+seg),('comparison_prediction','comparison_'+seg)]:
                assert ((r[old]==r[new]) | (r[old].isna() & r[new].isna())).all()
            assert r.prediction_origin_y.eq(r['origin_'+seg]).all()
            count+=len(r)
    detail=pd.read_parquet(out/'scoring/details.parquet');d=detail[detail.paired]
    def evaluate(f,name):
        actual=f.actual_d01_60.tolist();pred=f[name+'_prediction_d01_60'].tolist()
        den=math.fsum(actual);err=[a-b for a,b in zip(pred,actual)]
        hit=f[name+'_joint_hit']
        return {'wape':math.fsum(abs(x) for x in err)/den if den else None,
                'over':math.fsum(max(x,0) for x in err)/den if den else None,
                'hits':int(hit.sum()),'weight':math.fsum(f.loc[hit,'actual_d01_60'])/den if den else None}
    a,b=evaluate(d,'direct'),evaluate(d,'gate')
    dec=d[d.decline_group.eq('PRE_ISSUE_DECLINING')]
    da,db=evaluate(dec,'direct'),evaluate(dec,'gate')
    gates={'paired_w60_wape_lower':b['wape']<a['wape'],'joint_hit_count_increased':b['hits']>a['hits'],
           'joint_hit_sales_share_not_lower':b['weight']>=a['weight'],
           'declining_w60_over_rate_not_increased':db['over']<=da['over'] if da['over'] is not None else None,
           'output_and_core_coverage_not_reduced':bool(detail.direct_output_complete.eq(detail.gate_output_complete).all())}
    assert gates==read_json(out/'scoring/summary.json')['gates']
    started=now()
    root=Path(__file__).resolve().parents[1]
    proc=subprocess.run([sys.executable,'-B','-m','unittest','tests.test_yearago_gate','-v'],cwd=root,capture_output=True,text=True,encoding='utf-8')
    if proc.returncode:raise RuntimeError(proc.stdout+proc.stderr)
    verify(rm['sources'])
    dest.mkdir(exist_ok=False)
    dump(out/'tests_execution.json',{'started_at':started,'completed_at':now(),'returncode':proc.returncode,'stdout':proc.stdout,'stderr':proc.stderr,'test_file_sha256':sha(root/'tests/test_yearago_gate.py')})
    dump(dest/'review.json',{'status':'PASS','at':now(),'archived_annual_origin_rows':count,
                           'independent_unrounded_gates':gates,'scalar_direct':a,'scalar_gate':b,
                           'tests_returncode':0,'old_sources_unchanged':len(rm['sources']),
                           'script_sha256':sha(__file__),'new_fits':0,'new_inference':0})
    print(json.dumps({'status':'PASS','canonical_annual_rows':count,'gates':gates}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);review(**vars(p.parse_args()))
