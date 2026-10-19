"""Explain an existing frozen batch offline; never tune or generate a shadow run."""
import argparse,collections,csv,hashlib,json,statistics
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.diagnose_a16_frozen_trace import fixed_stages,A3,A7,A16,sha
from scripts.a16_candidate_v3 import Candidate,shift
from scripts.run_a16_shadow_once import prepare,verify_freeze


def ratio(a,b):return None if b==0 else a/b


def aggregate(rows):
    down=[r for r in rows if r['a7_rule']=='DOWN']
    drops=sum(max(0,r['a3']-r['a7']) for r in rows)
    return {'objects':len(rows),'down_objects':len(down),
            'full_replacement_objects':sum(r['gamma_down']==1 for r in down),
            'object_share':ratio(len(down),len(rows)),
            'a3':sum(r['a3']for r in rows),'seasonal_known_sum':sum(r['seasonal_candidate']for r in rows if r['seasonal_candidate'] is not None),
            'seasonal_known_objects':sum(r['seasonal_candidate']is not None for r in rows),
            'a7':sum(r['a7']for r in rows),'a16':sum(r['a16']for r in rows),
            'down_a3':sum(r['a3']for r in down),'down_seasonal':sum(r['seasonal_candidate']for r in down),
            'down_a7':sum(r['a7']for r in down),'down_a16':sum(r['a16']for r in down),
            'down_a3_quantity_share':ratio(sum(r['a3']for r in down),sum(r['a3']for r in rows)),
            'down_final_quantity_share':ratio(sum(r['a16']for r in down),sum(r['a16']for r in rows)),
            'gross_a7_reduction':drops,'down_a7_reduction':sum(r['a3']-r['a7']for r in down),
            'down_reduction_contribution':ratio(sum(r['a3']-r['a7']for r in down),drops),
            'a7_net_change':sum(r['a7']-r['a3']for r in rows),'a16_net_change':sum(r['a16']-r['a7']for r in rows)}


def diagnostic_flags(hist,item,cut,target,sku_hist):
    get=lambda m:hist.get((item,m))
    rec=[get(shift(cut,-i)) for i in (3,2,1)]
    ly=[get(shift(cut,-12-i))for i in (3,2,1)]
    ref=get(shift(target,-12));neigh=[get(shift(target,-12+i))for i in (-2,-1,1,2)]
    med=statistics.median(neigh)if all(x is not None for x in neigh)else None
    spu=item.split('|',1)[1]
    current={k for k,v in sku_hist.items()if k[1]==spu and any(shift(cut,-3)<=m<cut for m in v)}
    prior={k for k,v in sku_hist.items()if k[1]==spu and any(shift(cut,-15)<=m<shift(cut,-12) for m in v)}
    jac=ratio(len(current&prior),len(current|prior))
    cov=[get(shift(cut,-i))for i in range(1,25)]
    return {'recent_sales':rec,'ly_recent_sales':ly,'ly_target_sales':ref,'ly_neighbor_sales':neigh,
            'ly_target_missing':ref is None,'ly_target_zero':ref==0,'ly_target_low_base':ref is not None and 0<ref<30,
            'ly_neighbor_median':med,'ly_trough_ratio':None if ref is None or med is None else ratio(ref,med),
            'descriptive_trough_flag':None if ref is None or med is None or med<=0 else ref<.5*med,
            'recent_recovery_flag':all(x is not None and x>0 for x in rec) and rec[0]<rec[1]<rec[2] and rec[2]>=1.15*rec[0],
            'sku_current_count':len(current),'sku_prior_count':len(prior),'sku_jaccard':jac,
            'sku_composition_change_flag':None if jac is None else jac<.8,
            'history_24m_present':sum(x is not None for x in cov),'history_24m_zero':sum(x==0 for x in cov),
            'key_input_missing_count':sum(x is None for x in rec+ly+[ref]),
            'historical_operating_evidence':'UNKNOWN_NOT_IN_FROZEN_INPUT'}


def run(input_path,original_dir,out_dir):
    out=Path(out_dir).resolve();orig=Path(original_dir).resolve()
    if out.exists() or out==orig or orig in out.parents:raise ValueError('New independent output directory required')
    manifest=verify_freeze()
    raw=json.loads(Path(input_path).read_text(encoding='utf-8'))
    meta=json.loads((orig/'shadow_run_meta.json').read_text(encoding='utf-8'))
    if meta['candidate']!=manifest['candidate']:raise ValueError('Candidate mismatch')
    receipt=json.loads((orig/'shadow_acceptance_summary.json').read_text(encoding='utf-8'))
    csv_file=orig/'a16_v4_shadow_rows.csv';csv_hash=sha(csv_file)
    if csv_hash!=receipt['csv_sha256']:raise ValueError('Frozen output hash changed')
    original=list(csv.DictReader(csv_file.open(encoding='utf-8-sig')))
    expected={(r['shop']+'|'+r['spu'],r['horizon']):r for r in original if r['qualification']=='ESTABLISHED'}
    if len(expected)!=sum(r['qualification']=='ESTABLISHED'for r in original):raise ValueError('Duplicate original key')
    sku,hist,cats,seasons,months,input_hash=prepare(raw['monthly'],raw['catalog'],raw['season'],meta['issue_month'])
    if input_hash!=meta['input_hash']:raise ValueError('Input hash differs; do not silently change batch')
    candidate=Candidate(hist,cats,ROOT)
    def forbidden(*a,**k):raise RuntimeError('Retuning/full shadow entry forbidden')
    candidate.predict=forbidden;candidate.r5.choose_params=forbidden
    details=[];contexts=[]
    for model in meta['models']:
        h=int(model['horizon'][1:]);target=shift(model['target_month'],0);cut=shift(target,-h)
        outputs,features,_,_,_=fixed_stages(candidate,target,h,model['gamma_down'],model['gamma_up'])
        if len(features)!=model['candidate_scope'] or len(outputs)!=model['eligible_count']:raise ValueError('Context changed')
        contexts.append({'horizon':model['horizon'],'features':len(features),'eligible':len(outputs),'retuned':False})
        for r in outputs:
            key=(r['SPU'],model['horizon']);original_row=expected[key]
            if r[A16]!=int(original_row['a16_shadow']):raise ValueError('Frozen output mismatch')
            details.append({'shop':r['shop'],'spu':r['SPU'].split('|',1)[1],'target_month':model['target_month'],
                'horizon':model['horizon'],'lifecycle':r['生命周期'],'category_current':r['品类'],
                'a3':r[A3],'seasonal_candidate':r['季节候选'],'a7':r[A7],'a16':r[A16],
                'a7_rule':r['A7规则'],'a16_rule':r['A16规则'],'gamma_down':model['gamma_down'],'gamma_up':model['gamma_up'],
                'a7_reduction':max(0,r[A3]-r[A7]),'a7_delta':r[A7]-r[A3],'a16_delta':r[A16]-r[A7],
                'delta_conservation':(r[A7]-r[A3])+(r[A16]-r[A7])==r[A16]-r[A3],
                **diagnostic_flags(hist,r['SPU'],cut,target,sku)})
    if len(details)!=len(expected):raise ValueError('Coverage mismatch')
    groups=[];summaries={}
    for hs in meta['horizons']:
        rs=[r for r in details if r['horizon']==hs];summaries[hs]=aggregate(rs)
        for field in ['a7_rule','lifecycle','category_current']:
            for val in sorted({r[field]for r in rs}):
                group=[r for r in rs if r[field]==val]
                groups.append({'horizon':hs,'dimension':field,'value':val,**aggregate(group)})
        summaries[hs]['down_flags']={k:{'true':sum(r[k]is True for r in rs if r['a7_rule']=='DOWN'),
          'unknown':sum(r[k]is None for r in rs if r['a7_rule']=='DOWN')}for k in ['ly_target_missing','ly_target_zero','ly_target_low_base','descriptive_trough_flag','recent_recovery_flag','sku_composition_change_flag']}
    result={'kind':'RECONSTRUCTED_INPUT_DIAGNOSTIC','input_hash':input_hash,'raw_input_sha256':sha(input_path),
      'original_csv_sha256':csv_hash,'contexts':contexts,'matched_rows':len(details),'horizons':summaries,
      'down_unique_spus':len({r['spu']for r in details if r['a7_rule']=='DOWN'}),
      'source_original_unchanged':sha(csv_file)==csv_hash,'diagnostic_thresholds':{'low_base':'0 < LY < 30','trough':'LY < 0.5 * median(4 observed neighboring months)','recovery':'3 positive increasing months; last >= 1.15 * first','sku_change':'observed SKU set Jaccard < 0.8'},
      'threshold_use':'Descriptive screens only, not causality, model parameters, or business policy'}
    out.mkdir(parents=True)
    for name,obj in [('summary.json',result),('details.json',details),('groups.json',groups)]:
        (out/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')
    for name,rs in [('details.csv',details),('groups.csv',groups)]:
        with(out/name).open('w',encoding='utf-8-sig',newline='')as f:
            w=csv.DictWriter(f,fieldnames=list(rs[0]));w.writeheader();w.writerows({k:json.dumps(v,ensure_ascii=False)if isinstance(v,(dict,list))else v for k,v in r.items()}for r in rs)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input-json',required=True);p.add_argument('--original-dir',required=True);p.add_argument('--output-dir',required=True);a=p.parse_args()
    print(json.dumps(run(a.input_json,a.original_dir,a.output_dir),ensure_ascii=False,indent=2))
