"""Independent assessment of an existing preregistered run; never predicts.

No model imports, database, network or threshold search. All business outputs
belong in a fresh, authorized local directory, never in this source repository.
"""
import argparse
import collections
import csv
import hashlib
import json
from fractions import Fraction
from pathlib import Path

MODELS=('v4','a16','last_month','a3','w_half','w_zero')
CANDIDATES=('w_half','w_zero')
FOCUS=('LTY316','ZQZ354','LTY277','BX389','BX517','BX422','KZ230','KZ312','BX316')


def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def key(r):return tuple(r[k]for k in ('shop','spu','target_month','horizon'))
def ratio(a,b):return None if b==0 else a/b


def measure(rows,model):
    valid=[r for r in rows if r['truth']is not None and r[model]is not None]
    actual=over=under=pred=positive=hit=zero=zero_error=0
    for r in valid:
        a,p=r['truth'],r[model]
        if a<0 or p<0 or int(a)!=a or int(p)!=p:raise ValueError('Unexpected quantity')
        actual+=a;pred+=p;over+=max(p-a,0);under+=max(a-p,0)
        if a>0:positive+=1;hit+=abs(p-a)*10<=a*3
        else:zero+=1;zero_error+=abs(p)
    return dict(n=len(valid),actual=actual,prediction=pred,absolute_error=over+under,over=over,under=under,
      wape=ratio(over+under,actual),bias=ratio(over-under,actual),over_rate=ratio(over,actual),under_rate=ratio(under,actual),
      positive_n=positive,hit_n=hit,hit30=ratio(hit,positive),zero_n=zero,zero_absolute_error=zero_error,
      zero_mae=ratio(zero_error,zero),output_n=sum(r[model]is not None for r in rows))


def numerical_gate(base,candidate,protect_base,protect_candidate):
    if not base['actual']or not protect_base['actual']:return '不可判断'
    if base['actual']!=candidate['actual']or protect_base['actual']!=protect_candidate['actual']:raise ValueError('Denominator changed')
    improved=Fraction(candidate['absolute_error'],candidate['actual'])<Fraction(base['absolute_error'],base['actual'])
    safe=Fraction(protect_candidate['over'],protect_candidate['actual'])<=Fraction(protect_base['over'],protect_base['actual'])
    return '通过'if improved and safe else'不通过'


def compare(rows,label):
    result=[];base=measure(rows,'a16')
    for model in MODELS:
        m=measure(rows,model)
        if m['n']!=len(rows):raise ValueError('Not a shared scoring population')
        result.append(dict(group=label,model=model,**m,
          delta_wape=ratio(m['absolute_error']-base['absolute_error'],m['actual']),
          delta_over=m['over']-base['over'],under_reduction=base['under']-m['under'],
          delta_over_rate=ratio(m['over']-base['over'],m['actual'])))
    return result


def write_tables(out,values):
    out.mkdir(parents=True)
    for name,value in values.items():
        (out/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
        if isinstance(value,list)and value:
            columns=list(dict.fromkeys(k for r in value for k in r))
            with (out/(name+'.csv')).open('w',encoding='utf-8-sig',newline='')as f:
                w=csv.DictWriter(f,fieldnames=columns);w.writeheader()
                w.writerows({k:json.dumps(v,ensure_ascii=False)if isinstance(v,(list,dict))else v for k,v in r.items()}for r in value)


def assess(run_dir,output):
    p=Path(run_dir);out=Path(output)
    if out.exists():raise ValueError('Independent assessment requires a new output directory')
    rows=read(p/'details.json');common=[r for r in rows if r['common_score']]
    eligible=[r for r in rows if r['forecastability']=='ESTABLISHED']
    summary=read(p/'summary.json');recorded=read(p/'metrics.json')
    if len({key(r)for r in rows})!=len(rows):raise ValueError('Duplicate key')
    rule_changes=collections.Counter();missing=[]
    for r in rows:
        expected=r['forecastability']=='ESTABLISHED'and r['truth']is not None and all(r[m]is not None for m in MODELS)
        if r['common_score']!=expected:raise ValueError('Common-sample predicate changed')
        recent=r['recent_sales']
        p1=r['lifecycle']=='衰退';p2=all(v is not None and v>0 for v in recent)and recent[0]>recent[1]>recent[2]
        if (p1,p2,p1 or p2)!=(r['protection_p1'],r['protection_p2'],r['protected']):raise ValueError('Protection definition changed')
        for model in CANDIDATES:
            if r[model]is not None and r[model]!=r['a16']:
                if r['a7_rule']!='DOWN' or r[model]<r['a16']:raise ValueError('Unexpected rule/value change')
                rule_changes[model]+=1
        if r['w_half_status']=='MISSING_REQUIRED_INPUT':
            if not(r['a7_rule']=='DOWN'and r['gamma_down']>0 and r['key_input_missing_count']>0 and r['w_half']is None and r['w_zero']is None):raise ValueError('Invalid missing-input route')
            missing.append(r)
    # Compare independent arithmetic against the experiment's native scorer.
    checks=0
    for metric in recorded:
        if metric['population']=='common'and metric['dimensions']in([],['horizon']):
            rs=common if not metric['dimensions']else[r for r in common if r['horizon']==metric['horizon']]
            m=measure(rs,metric['model'])
            for a,b in [('n','n'),('actual','actual_sum'),('over','over_units'),('under','under_units'),('absolute_error','absolute_error'),('zero_absolute_error','zero_absolute_error')]:
                if m[a]!=metric[b]:raise ValueError('Independent arithmetic mismatch')
            checks+=1
    groups={'ALL':common}
    for field in ('horizon','period','shop','target_month','lifecycle','age_class','trust_state'):
        for value in sorted({r[field]for r in common}):
            groups[field+':'+value]=[r for r in common if r[field]==value]
    for label,predicate in [('主保护组',lambda r:r['protected']),('P1',lambda r:r['protection_p1']),('P2',lambda r:r['protection_p2']),
      ('主保护组_DOWN',lambda r:r['protected']and r['a7_rule']=='DOWN'),('P1_DOWN',lambda r:r['protection_p1']and r['a7_rule']=='DOWN'),('P2_DOWN',lambda r:r['protection_p2']and r['a7_rule']=='DOWN'),
      ('全部DOWN',lambda r:r['a7_rule']=='DOWN'),('排除冲突真值',lambda r:not r['truth_conflict'])]:
        groups[label]=[r for r in common if predicate(r)]
        for horizon in ('H2','H3'):
            groups[label+'|'+horizon]=[r for r in groups[label]if r['horizon']==horizon]
    comparisons=[m for label,rs in groups.items()for m in compare(rs,label)]
    items=collections.defaultdict(list)
    for r in common:items[r['shop'],r['spu']].append(r)
    item_effects=[];outcomes=[];total_actual=sum(r['truth']for r in common)
    for model in CANDIDATES:
        for (shop,spu),rs in items.items():
            b=measure(rs,'a16');c=measure(rs,model);delta=c['absolute_error']-b['absolute_error']
            item_effects.append(dict(shop=shop,spu=spu,model=model,n=len(rs),actual=c['actual'],delta_error=delta,
              delta_over=c['over']-b['over'],under_reduction=b['under']-c['under'],
              outcome='改善'if delta<0 else'退化'if delta>0 else'不变',protected_n=sum(r['protected']for r in rs)))
        for state in ('改善','退化','不变'):
            selected=[r for r in item_effects if r['model']==model and r['outcome']==state]
            actual=sum(r['actual']for r in selected)
            outcomes.append(dict(model=model,outcome=state,items=len(selected),item_share=ratio(len(selected),len(items)),actual=actual,actual_weight=ratio(actual,total_actual)))
    top_risk=[]
    for model in CANDIDATES:
        ranked=sorted([r for r in item_effects if r['model']==model and r['delta_over']>0],key=lambda r:(-r['delta_over'],r['shop'],r['spu']))
        total=sum(r['delta_over']for r in ranked)
        top_risk.extend(dict(**r,rank=i+1,over_increase_share=ratio(r['delta_over'],total))for i,r in enumerate(ranked[:20]))
    pairs=focus_pairs(eligible)
    focus=[r for r in common if(r['shop'],r['spu'])in pairs]
    focus_metrics=[m for shop,spu in pairs for m in compare([r for r in focus if(r['shop'],r['spu'])==(shop,spu)],shop+'|'+spu)]
    focus_coverage=[]
    for shop,spu in pairs:
        rs=[r for r in eligible if(r['shop'],r['spu'])==(shop,spu)]
        focus_coverage.append(dict(shop=shop,spu=spu,eligible_n=len(rs),truth_known=sum(r['truth']is not None for r in rs),common_n=sum(r['common_score']for r in rs),candidate_missing=sum(r['w_half']is None for r in rs)))
    base=measure(common,'a16');protected=groups['主保护组'];pb=measure(protected,'a16');gates=[]
    for model in CANDIDATES:
        c=measure(common,model);pc=measure(protected,model)
        gates.append(dict(model=model,numeric_gate=numerical_gate(base,c,pb,pc),
          wape_strictly_lower=c['absolute_error']<base['absolute_error'],protection_over_not_increased=pc['over']<=pb['over'],
          overall_actual=base['actual'],baseline_error=base['absolute_error'],candidate_error=c['absolute_error'],
          protected_actual=pb['actual'],protected_base_over=pb['over'],protected_candidate_over=pc['over'],
          business_evidence='PARTIAL: UNKNOWN comparability; reconstructed historical diagnostics, not independent OOS'))
    coverage=[]
    for model in MODELS:
        m=measure(eligible,model)
        coverage.append(dict(model=model,eligible_n=len(eligible),output_n=m['output_n'],output_rate=ratio(m['output_n'],len(eligible)),
          truth_known_n=sum(r['truth']is not None for r in eligible),truth_rate=ratio(sum(r['truth']is not None for r in eligible),len(eligible)),
          scored_own_n=m['n'],scored_own_actual=m['actual'],common_n=len(common),common_output_rate=ratio(len(common),len(eligible))))
    protection_breadth={label:dict(n=len(groups[label]),items=len({(r['shop'],r['spu'])for r in groups[label]}),
      affected_months=sorted({r['target_month']for r in groups[label]if r['w_zero']!=r['a16']}))for label in('主保护组','P1','P2','主保护组_DOWN','P1_DOWN','P2_DOWN')}
    # Include every month/shop even if unchanged; never condition the report on improvement.
    sample_signature=hashlib.sha256(json.dumps(sorted(key(r)for r in common),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
    assessment=dict(gates=gates,checks=dict(arithmetic_comparisons=checks,all_population_rows_checked=len(rows),non_down_changed=0,common_key_sha256=sample_signature,
      source_details_sha256=hashlib.sha256((p/'details.json').read_bytes()).hexdigest()),
      scope=dict(outer_n=len(rows),eligible_n=len(eligible),common_n=len(common),common_items=len(items),truth_known_n=sum(r['truth']is not None for r in eligible),
        common_actual=total_actual,common_positive_n=sum(r['truth']>0 for r in common),common_zero_n=sum(r['truth']==0 for r in common),
        changed_output_counts=dict(rule_changes),missing_candidate_count=len(missing),truth_conflict_common_n=sum(r['truth_conflict']for r in common)),
      protection_breadth=protection_breadth,bootstrap=summary['bootstrap'],run=summary,
      controlled_conditions='Same nine months, frozen model and gamma; only preregistered DOWN weights; unrounded gate via exact rational comparison')
    write_tables(out,dict(assessment=assessment,comparisons=comparisons,item_effects=item_effects,outcomes=outcomes,top_risk=top_risk,
      focus_metrics=focus_metrics,focus_coverage=focus_coverage,focus_detail=focus,coverage=coverage,missing_candidates=missing))
    return assessment


def focus_pairs(rows):
    # Four exploration items are JQ-US; other focus SPUs retain their real shop.
    required={('JQ-US',spu)for spu in FOCUS[:4]}
    return sorted(required|{(r['shop'],r['spu'])for r in rows if r['spu']in FOCUS[4:]})


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();s=assess(a.run_dir,a.output)
    print(json.dumps({k:s[k]for k in ('gates','checks','scope','protection_breadth')},ensure_ascii=False))
