"""Independent source, filter, score and geometric verification; no model calls."""
from __future__ import annotations
import argparse,json,math,hashlib,itertools
from pathlib import Path
from datetime import datetime,timezone
import pandas as pd


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def validate(output):
    out=Path(output);data=json.loads((out/'reviewed_data.json').read_text(encoding='utf-8'))
    m=json.loads((out/'build_manifest.json').read_text(encoding='utf-8'))
    for p,h in m['source_hashes'].items():assert sha(p)==h,p
    for p,h in m['files'].items():assert sha(out/p)==h,p
    rows=data['rows'];assert len({(r['issue'],r['shop'],r['spu'],r['window']) for r in rows})==len(rows)
    truth_checks=forecast_checks=area_checks=overlap_checks=0
    source_tables={s['source']:pd.read_parquet(s['scores']) for s in data['sources']}
    if 'cohort_id' in source_tables['monthly']:source_tables['monthly']=source_tables['monthly'].loc[lambda f:f.cohort_id.eq('monthly_core')]
    for info in data['models']:
        f=source_tables[info['source']];f=f[f.model.eq(info['id'])].set_index(['issue_date','shop','spu','window'])
        for r in rows:
            z=f.loc[(r['issue'],r['shop'],r['spu'],r['window'])]
            if r['truth']=='SOURCE_CONFLICT':continue
            assert (pd.isna(z.actual) and r['actual'] is None) or z.actual==r['actual']
            assert str(z.target_start)==r['start'] and str(z.target_end)==r['end']
            v=r['values'][info['id']];assert (pd.isna(z.prediction) and v['p'] is None) or float(z.prediction)==v['p']
            truth_checks+=1;forecast_checks+=1
    stats_records=[]
    for win in data['windows']:
        wr=[r for r in rows if r['window']==win];known=[r for r in wr if r['actual'] is not None and r['truth']=='OBSERVED']
        cutoff=20 if win=='d01_60' else 10
        low=[r for r in known if r['actual']<cutoff];kept=[r for r in known if r['actual']>=cutoff]
        audit=next(a for a in data['filterAudit'] if a['window']==win)
        assert audit['hidden_n']==len(low) and audit['hidden_actual']==sum(r['actual'] for r in low)
        assert audit['shown_n']==len(kept) and audit['not_matured']==sum(r['truth']=='NOT_MATURED' for r in wr)
        layout=data['layouts'][win];coords=layout['coordinates'];assert set(coords)==set(str(r['id']) for r in kept)
        for month in sorted({r['month'] for r in kept}):
            local=[r for r in kept if r['month']==month]
            for r in local:
                x,y,s=coords[str(r['id'])];assert s>0 and min(x,y)>=0 and x+s<=data['width']+1e-9 and y+s<=data['height']+1e-9
                assert math.isclose(s*s,layout['k']*r['actual'],rel_tol=1e-12);area_checks+=1
            for a,b in itertools.combinations(local,2):
                x,y,s=coords[str(a['id'])];u,v,t=coords[str(b['id'])]
                assert x+s<=u+1e-8 or u+t<=x+1e-8 or y+s<=v+1e-8 or v+t<=y+1e-8
                assert math.isclose((s*s)/(t*t),a['actual']/b['actual'],rel_tol=1e-12);overlap_checks+=1
        for model in [z['id'] for z in data['models']]:
            for label,rs in [('UNFILTERED',known),('VISIBLE',kept)]:
                eligible=[r for r in rs if r['values'][model]['state']=='OK']
                den=math.fsum(r['actual'] for r in eligible);ae=math.fsum(abs(r['values'][model]['p']-r['actual']) for r in eligible)
                hits=sum(r['actual']>0 and abs(r['values'][model]['p']-r['actual'])/r['actual']<=.2 for r in eligible)
                wape=ae/den if den else None
                stats_records.append(dict(window=win,model=model,scope=label,n=len(rs),output=len(eligible),actual=math.fsum(r['actual'] for r in rs),denominator=den,ae=ae,wape=wape,hits=hits))
                if label=='UNFILTERED':
                    ref=next(x for x in data['unfiltered'] if x['model']==model and x['window']==win)
                    assert ref['n']==len(rs) and ref['output']==len(eligible) and ref['hit']==hits
                    assert math.isclose(ref['wape'],wape,rel_tol=1e-12)
    # Reconcile the selected long-history original models to their archived metric file.
    root=Path(data['sources'][0]['scores']).parent
    old=pd.read_csv(root/'metrics.csv',float_precision='round_trip')
    old=old[old.cohort_id.eq('monthly_core')&old.slice.eq('overall_overlapping_issue_records')]
    archive_checks=0
    for r in stats_records:
        if r['scope']!='UNFILTERED' or r['model'] not in source_tables['monthly'].model.unique():continue
        z=old[old.model.eq(r['model'])&old.window.eq(r['window'])].iloc[0]
        assert r['hits']==z.hit_n and math.isclose(r['wape'],z.wape,rel_tol=1e-12);archive_checks+=1
    # The latest reviewed five-model receipt independently verifies the same final truth.
    blend_info=next((s for s in data['sources'] if s['source']=='blend'),None)
    if blend_info:
        bm=pd.read_parquet(Path(blend_info['scores']).parent/'metrics.parquet')
        bm=bm[bm.scope.eq('FULL_CORE')&bm.slice.eq('overall')]
        for r in stats_records:
            if r['scope']!='UNFILTERED' or r['model'] not in bm.model.unique():continue
            z=bm[bm.model.eq(r['model'])&bm.window.eq(r['window'])].iloc[0]
            assert r['hits']==z.hit_n and math.isclose(r['wape'],z.wape,rel_tol=1e-12);archive_checks+=1
    html=(out/'销量预测误差方块图.html').read_text(encoding='utf-8')
    assert '<script src=' not in html and 'fetch(' not in html and 'https://cdn' not in html
    dest=out/'validation';dest.mkdir(exist_ok=False)
    pd.DataFrame(stats_records).to_csv(dest/'独立汇总.csv',index=False,encoding='utf-8-sig')
    receipt=dict(status='PASS',at=datetime.now(timezone.utc).isoformat(),source_hashes=len(m['source_hashes']),rows=len(rows),
        truth_checks=truth_checks,forecast_checks=forecast_checks,area_checks=area_checks,pairwise_overlap_and_area_ratio_checks=overlap_checks,
        archived_metric_checks=archive_checks,statistic_rows=len(stats_records),anomaly_count=len(data['anomalies']),
        unknown_kept_separate=True,all_model_layout_shared=True,html_sha256=sha(out/'销量预测误差方块图.html'),
        scope='Numerical and geometry validation only; browser results are separate')
    (dest/'数值及图形验收.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(receipt,ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);validate(**vars(p.parse_args()))
