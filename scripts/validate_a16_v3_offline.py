"""Offline validation runner. Explicit evidence paths; no network or DB clients."""
import argparse, collections, csv, datetime, hashlib, json, pathlib
from a16_candidate_v3 import Candidate, SOURCE_FILES, load_pure, shift


def score(rows, model, truth='actual'):
    known=[r for r in rows if r.get(truth) is not None and r.get(model) is not None]
    actual=sum(r[truth] for r in known);pred=sum(r[model] for r in known)
    error=sum(abs(r[model]-r[truth]) for r in known)
    positive=[r for r in known if r[truth]>0]
    return {'scope_n':len(rows),'n':len(known),'prediction_n':sum(r.get(model) is not None for r in rows),
            'actual_sum':actual,'prediction_sum':pred,'absolute_error':error,
            'wape':error/actual if actual else None,'bias':(pred-actual)/actual if actual else None,
            'hit30':sum(abs(r[model]-r[truth])<=.3*r[truth] for r in positive)/len(positive) if positive else None,
            'positive_actual_n':len(positive),'zero_actual_n':sum(r[truth]==0 for r in known)}


def run(inputs, daily_evidence, output):
    output.mkdir(parents=True,exist_ok=True)
    root=pathlib.Path(__file__).resolve().parents[1]
    manifest=json.loads((root/'docs/a16_candidate_v3_manifest.json').read_text())
    # The pre-test adapter was frozen as an LF Git blob. Normalize checkout EOLs only.
    assert hashlib.sha256((root/'scripts/a16_candidate_v3.py').read_text(encoding='utf-8-sig').encode()).hexdigest()==manifest['adapter_sha256']
    for path,h in manifest['sources'].items():
        assert hashlib.sha256((root/path).read_text(encoding='utf-8-sig').encode()).hexdigest()==h['normalized_sha256']
    hist=collections.defaultdict(dict);total=collections.defaultdict(dict);seasons={};mapping=collections.defaultdict(set)
    for line in inputs.open(encoding='utf-8-sig'):
        if not line.startswith('{'):continue
        x=json.loads(line)
        if x['kind']=='seasons':seasons={r['SPU']:r['季节'] for r in x['value']}
        if x['kind']!='sales':continue
        r=x['value'];s,sku,p=(str(r.get(k) or '').strip() for k in ('店铺','SKU','SPU'))
        d=shift(r['统计日期'],0);q=int(r['销量']);assert p and q>=0
        hist[s,sku,p][d]=hist[s,sku,p].get(d,0)+q
        total[s,p][d]=total[s,p].get(d,0)+q;mapping[s,sku].add(p)
    assert not any(len(v)>1 for v in mapping.values())
    cats=[]
    for line in daily_evidence.open(encoding='utf-8-sig'):
        if line.startswith('{'):
            x=json.loads(line)
            if x['kind']=='category_rows':cats=x['value']
    cats.sort(key=lambda r:(bool(str(r.get('product_category') or '').strip()),str(r.get('update_time') or ''),str(r.get('etl_load_time') or ''),int(r.get('product_id') or 0)),reverse=True)
    catmap={}
    for r in cats:
        catmap.setdefault(str(r.get('spu') or '').strip(),str(r.get('product_category') or '').strip() or '未映射')
    obs={(s+'|'+p,d):q for (s,p),v in total.items() for d,q in v.items()}
    candidate=Candidate(obs,catmap,root)
    v4=load_pure(root,SOURCE_FILES[-1])
    dates=[k[1] for k in obs]
    input_summary={'first_month':str(min(dates))[:7],'last_month':str(max(dates))[:7],
                   'shop_spu_count':len(total),'sku_count':len(hist),'current_categories':len(catmap),
                   'input_hash':hashlib.sha256(inputs.read_bytes()).hexdigest()}
    label=lambda d,forecast=False:f'{d.year%100:02d}年{d.month}月'+('预计销量' if forecast else '销量')
    details=[];choices=[]
    for target in manifest['test_months']+manifest['regression_months']:
        for h in (1,2,3):
            td=shift(target,0);cut=shift(td,-h)
            ar,features,meta=candidate.predict(td,h);choices.append(meta)
            byshop=collections.defaultdict(dict)
            for (s,sku,p),values in hist.items():
                visible={label(d):q for d,q in values.items() if d<cut}
                if visible:byshop[s][sku]={'SPU':p,**visible}
            predictions=collections.Counter();methods=collections.defaultdict(list);seen=set()
            for s,sv in byshop.items():
                out=v4.compute_forecast_for_shop(sv,[label(shift(cut,i),True) for i in range(h+1)],datetime.datetime.combine(cut,datetime.time()),spu_season_map=seasons)
                for sku,vals in out.items():
                    p=sv[sku]['SPU'];predictions[s,p]+=vals[label(td,True)];seen.add((s,p))
                    methods[s,p].append(vals['预测方法'].split('；')[-1])
            for item,first in candidate.first.items():
                age=(cut.year-first.year)*12+cut.month-first.month
                if age<4:continue
                s,p=item.split('|',1);f=features.get(item);a=ar.get(item);history=total[s,p]
                details.append({'target_month':target,'cutoff_month':str(cut)[:7],'horizon':f'H{h}','shop':s,'spu':p,
                    'age':age,'age_class':'4-5' if age<6 else '6-11' if age<12 else '12+',
                    'actual':history.get(td),'v4':predictions[s,p] if (s,p) in seen else None,
                    'last_month':history.get(shift(cut,-1)),'a16':a[candidate.r12.A16] if a else None,
                    'forecastability':f['可预测性'] if f else 'NO_FEATURE',
                    'lifecycle':f['生命周期'] if f else None,'profile':f['需求形态'] if f else None,
                    'a16_status':'OUTPUT' if a else ('INELIGIBLE' if f and f['可预测性']!='ESTABLISHED' else 'MISSING_OUTPUT'),
                    'a16_rule':a.get('A16规则') if a else None,'a7_rule':a.get('A7规则') if a else None,
                    'a3':a.get(candidate.r12.A3) if a else None,'a7':a.get(candidate.r12.A7) if a else None,
                    'ly_target':history.get(shift(td,-12)),
                    'prior_3':[history.get(shift(cut,-i)) for i in (1,2,3)],
                    'current_season':seasons.get(p),'current_category':catmap.get(p),
                    'v4_branches':dict(collections.Counter(methods[s,p]))})
            print(json.dumps({'completed':target,'h':h,'output':len(ar),'scope_so_far':len(details)}),flush=True)
            (output/'choices.json').write_text(json.dumps(choices,ensure_ascii=False,indent=2),encoding='utf-8')
    (output/'expanded_details.json').write_text(json.dumps(details,ensure_ascii=False),encoding='utf-8')
    result={'input':input_summary,'kind':'RETROSPECTIVE_NOT_INDEPENDENT_OOS','scope':len(details),'metrics':[],'coverage':[]}
    for group in [('target_month','horizon'),('shop','age_class','horizon'),('current_season','horizon'),('horizon',)]:
        groups=collections.defaultdict(list)
        for r in details:groups[tuple(r[k] for k in group)].append(r)
        for key,rows in groups.items():
            paired=[r for r in rows if all(r[x] is not None for x in ('actual','v4','last_month','a16'))]
            for mode,rs in [('full_scope',rows),('paired',paired)]:
                for model in ('v4','last_month','a16'):
                    result['metrics'].append({'dimensions':list(group),**dict(zip(group,key)),'mode':mode,'model':model,'original_scope':len(rows),**score(rs,model)})
    for target in manifest['test_months']+manifest['regression_months']:
        for h in ('H1','H2','H3'):
            rows=[r for r in details if r['target_month']==target and r['horizon']==h]
            result['coverage'].append({'target':target,'horizon':h,'scope':len(rows),'actual_known':sum(r['actual'] is not None for r in rows),'a16_output':sum(r['a16'] is not None for r in rows),'states':dict(collections.Counter(r['a16_status'] for r in rows)),'qualification':dict(collections.Counter(r['forecastability'] for r in rows))})
    (output/'expanded_results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'done':True,'scope':len(details),'input':input_summary}),flush=True)


if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--inputs',type=pathlib.Path,required=True);ap.add_argument('--categories',type=pathlib.Path,required=True);ap.add_argument('--output',type=pathlib.Path,required=True)
    a=ap.parse_args();run(a.inputs,a.categories,a.output)
