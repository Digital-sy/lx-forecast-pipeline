"""Refresh archived 14-fabric plans with a bounded current suggestion snapshot.

Offline only: no database, API, production collectors, forecast or inventory calls.
Exact identity joins retain original/manual mappings; unknown new plans stay unknown.
"""
from __future__ import annotations
import argparse,copy,csv,hashlib,json,math
from collections import defaultdict,Counter
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
from pathlib import Path

MONTHS=('2026-11','2026-12')
QFIELDS=('26年11月建议下单','26年12月建议下单')
MAPPING_FIELDS=('material_id','review_id','shop','spu','system','color_code','garment_color_name','original_material','current_material',
 'confirmed_part','previously_accepted','unique_sku_resolved','business_physical_confirmation_flag','physical_color_resolved',
 'physical_color_number','physical_color_name_raw','physical_mapping_origin','selected_lingxing_sku','confirmation_id','confirmation_version',
 'full_garment_recipe_confirmed','aggregate_status')
class RefreshError(ValueError):pass
def require(ok,msg):
 if not ok:raise RefreshError(msg)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def load(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def read_csv(p):
 with Path(p).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))
def write_json(p,x):
 with Path(p).open('x',encoding='utf-8') as f:json.dump(x,f,ensure_ascii=False,indent=2,allow_nan=False)
def write_csv(p,rows,empty_fields=('status',)):
 fields=list(dict.fromkeys(k for r in rows for k in r)) or list(empty_fields)
 with Path(p).open('x',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
  for r in rows:w.writerow({k:json.dumps(v,ensure_ascii=False,sort_keys=True) if isinstance(v,(dict,list)) else v for k,v in r.items()})
def flag(v):
 if v in (True,1,'1','True','true'):return True
 if v in (False,0,'0','False','false','',None):return False
 raise RefreshError('Invalid boolean')
def qty(v):
 if v is None or v=='':return None
 require(not isinstance(v,bool),'Boolean is not quantity')
 try:d=Decimal(str(v))
 except InvalidOperation as e:raise RefreshError('Invalid quantity') from e
 require(d.is_finite() and d>=0 and d==d.to_integral_value(),'Negative, noninteger or nonfinite quantity')
 return int(d)
def sums(values):
 values=list(values);unknown=sum(v is None for v in values);known=sum(v for v in values if v is not None)
 return {'value':None if unknown else known,'known_sum':known,'unknown_records':unknown}
def identity(r):
 key=tuple(r.get(f) for f in ('店铺','SPU','颜色体系','颜色缩写'))
 require(all(isinstance(v,str) and v for v in key),'Missing business identity; do not infer source color system/code')
 return key
def demand_id(key):return hashlib.sha256(json.dumps(tuple(key),ensure_ascii=False).encode()).hexdigest()[:20]
def source_index(rows):
 out={};ids=set()
 for raw in rows:
  key=identity(raw);require(key not in out,'Duplicate source business key')
  require(raw.get('id') is not None,'Missing source trace id')
  require(raw.get('id') not in ids,'Duplicate source row id');ids.add(raw.get('id'))
  q=[qty(raw[f]) for f in QFIELDS]
  out[key]={'raw':copy.deepcopy(raw),'demand_id':demand_id(key),'nov_qty':q[0],'dec_qty':q[1],
   'total_qty':None if any(v is None for v in q) else sum(q)}
 return out
def qstats(rows):
 d={}
 for r in rows:
  did=r['demand_id'];q=tuple(qty(r[k]) for k in ('nov_qty','dec_qty','total_qty'))
  require(did not in d or d[did]==q,'Demand quantities differ across material components')
  d[did]=q
 out={'component_rows':len(rows),'unique_demand_rows':len(d),'review_items':len({r.get('review_id') for r in rows if r.get('review_id')})}
 for i,k in enumerate(('nov_qty','dec_qty','total_qty')):
  z=sums(v[i] for v in d.values());out[k]=z['value'];out[k+'_known_sum']=z['known_sum'];out[k+'_unknown_records']=z['unknown_records']
 return out

def audit_sources(old_rows,new_rows,known_components,full_material_rows):
 old=source_index(old_rows);new=source_index(new_rows)
 known_ids={r['demand_id'] for r in known_components};known_spu={(r['shop'],r['spu']) for r in known_components}
 full=defaultdict(set)
 for r in full_material_rows:full[r['demand_id']].add(r['material_name'])
 audit=[];exceptions=[]
 for key in sorted(set(old)|set(new)):
  before,after=old.get(key),new.get(key);did=demand_id(key)
  state='SAME_KEY' if before and after else ('NEW_SOURCE_KEY' if after else 'SOURCE_KEY_ABSENT')
  before_zero=before is not None and before['total_qty']==0
  after_positive=after is not None and after['total_qty'] is not None and after['total_qty']>0
  row={'demand_id':did,'shop':key[0],'spu':key[1],'system':key[2],'color_code':key[3],
   'source_key_state':state,'old_source_id':before['raw']['id'] if before else None,'new_source_id':after['raw']['id'] if after else None,
   'old_nov_qty':before['nov_qty'] if before else None,'old_dec_qty':before['dec_qty'] if before else None,'old_total_qty':before['total_qty'] if before else None,
   'new_nov_qty':after['nov_qty'] if after else None,'new_dec_qty':after['dec_qty'] if after else None,'new_total_qty':after['total_qty'] if after else None,
   'old_updated_at':before['raw'].get('更新时间') if before else None,'new_updated_at':after['raw'].get('更新时间') if after else None,
   'old_forecast_model':before['raw'].get('预测模型') if before else None,'new_forecast_model':after['raw'].get('预测模型') if after else None,
   'old_quantity_basis':before['raw'].get('新品数量口径') if before else None,'new_quantity_basis':after['raw'].get('新品数量口径') if after else None,
   'forecast_model_changed':before is not None and after is not None and before['raw'].get('预测模型')!=after['raw'].get('预测模型'),
   'quantity_basis_changed':before is not None and after is not None and before['raw'].get('新品数量口径')!=after['raw'].get('新品数量口径'),
   'previously_zero_now_positive':before_zero and after_positive,'quantity_changed':before is not None and after is not None and (before['nov_qty'],before['dec_qty'])!=(after['nov_qty'],after['dec_qty']),
   'in_existing_14_material_mapping':did in known_ids,'same_shop_spu_has_14_material_plan':(key[0],key[1]) in known_spu,
   'archived_exact_color_materials':sorted(full.get(did,set())),
   'material_scope_status':'EXISTING_14_MATERIAL_PLAN' if did in known_ids else ('ARCHIVED_OTHER_MATERIAL_PLAN' if full.get(did) else 'UNKNOWN_NO_EXACT_COLOR_MATERIAL_PLAN')}
  row['unresolved_plan_reason']=('同店铺SPU已有14款材料，但该颜色材料方案待确认' if row['same_shop_spu_has_14_material_plan'] else '无该颜色既有材料方案，14款范围归属UNKNOWN') if row['material_scope_status']=='UNKNOWN_NO_EXACT_COLOR_MATERIAL_PLAN' else None
  audit.append(row)
  if after_positive and did not in known_ids and not full.get(did):exceptions.append(copy.deepcopy(row))
 def st(index):return {k:sums(r[k] for r in index.values()) for k in ('nov_qty','dec_qty','total_qty')}
 stats={'old_source_rows':len(old_rows),'current_source_rows':len(new_rows),'old_unique_keys':len(old),'current_unique_keys':len(new),
  'common_keys':len(set(old)&set(new)),'new_keys':len(set(new)-set(old)),'missing_old_keys':len(set(old)-set(new)),
  'source_ids_changed':sum(r['old_source_id']!=r['new_source_id'] for r in audit if r['source_key_state']=='SAME_KEY'),
  'quantity_changed_keys':sum(r['quantity_changed'] for r in audit),
  'forecast_model_changed_keys':sum(r['forecast_model_changed'] for r in audit),'quantity_basis_changed_keys':sum(r['quantity_basis_changed'] for r in audit),'old_totals':st(old),'current_totals':st(new),
  'previously_zero_now_positive_keys':sum(r['previously_zero_now_positive'] for r in audit),
  'positive_unclassified_keys':len(exceptions),'positive_unclassified_total':sums(r['new_total_qty'] for r in exceptions)}
 partitions={}
 for state in ('EXISTING_14_MATERIAL_PLAN','ARCHIVED_OTHER_MATERIAL_PLAN','UNKNOWN_NO_EXACT_COLOR_MATERIAL_PLAN'):
  rows=[r for r in audit if r['material_scope_status']==state and r['source_key_state']!='SOURCE_KEY_ABSENT']
  partitions[state]={'rows':len(rows),'nov_qty':sums(r['new_nov_qty'] for r in rows),'dec_qty':sums(r['new_dec_qty'] for r in rows),'total_qty':sums(r['new_total_qty'] for r in rows)}
 require(sum(p['rows'] for p in partitions.values())==len(new),'Source partition row conservation failed')
 for field in ('nov_qty','dec_qty','total_qty'):
  require(sum(p[field]['known_sum'] for p in partitions.values())==stats['current_totals'][field]['known_sum'],'Source quantity partition failed')
  require(sum(p[field]['unknown_records'] for p in partitions.values())==stats['current_totals'][field]['unknown_records'],'Source unknown conservation failed')
 stats['exclusive_source_partitions']=partitions;stats['partition_conservation_pass']=True
 return old,new,audit,exceptions,stats

def active_confirmations(receipt):
 require(receipt.get('status') in ('COMMITTED_VERIFIED','ALREADY_MATCHED_VERIFIED'),'Unverified database receipt')
 require(receipt.get('final_readback',{}).get('state')=='MATCH','Database readback not exact')
 groups=defaultdict(list)
 for r in receipt['tables']['ods_manual_sm_garment_color_confirmation']:groups[r['review_id']].append(r)
 selected={}
 for rid,rs in groups.items():
  rs=sorted(rs,key=lambda r:r['confirmation_version']);require(len({r['confirmation_version'] for r in rs})==len(rs),'Duplicate manual version')
  for previous,current in zip(rs,rs[1:]):require(current['supersedes_confirmation_id']==previous['confirmation_id'],'Broken manual version chain')
  selected[rid]=rs[-1]
 return selected

def refresh_components(base_rows,old,new,manual):
 out=[];missing=[];seen=set()
 for r in base_rows:
  key=(r['shop'],r['spu'],r['system'],r['color_code']);did=demand_id(key)
  require(did==r['demand_id'],'Component business key differs from demand id')
  component_key=(did,r['material_id']);require(component_key not in seen,'Duplicate baseline component');seen.add(component_key)
  require(key in old,'Baseline component missing in original source')
  for f in ('nov_qty','dec_qty','total_qty'):require(qty(r[f])==old[key][f],'Baseline component quantity differs from archived raw source')
  m=manual.get(r.get('review_id'))
  if r.get('confirmation_id'):
   require(m is not None and m['confirmation_id']==r['confirmation_id'],'Component does not use latest confirmed version')
   s=m['scope_json'];s=json.loads(s) if isinstance(s,str) else s
   shops=s['shops'].split('、') if isinstance(s['shops'],str) else s['shops']
   require(s['review_id']==r['review_id'] and s['spu']==r['spu'] and s['source_system']==r['system'] and s['source_code']==r['color_code'] and r['shop'] in shops and s['months']==list(MONTHS),'Manual scope mismatch')
  z=copy.deepcopy(r);z['quantity_source_status']='CURRENT_SOURCE_KEY_PRESENT' if key in new else 'SOURCE_KEY_ABSENT'
  z['prior_nov_qty']=qty(r['nov_qty']);z['prior_dec_qty']=qty(r['dec_qty']);z['prior_total_qty']=qty(r['total_qty'])
  z['source_id_old']=old[key]['raw']['id']
  if key in new:
   n=new[key]
   for f in ('nov_qty','dec_qty','total_qty'):z[f]=n[f]
   z.update(source_id_current=n['raw']['id'],source_updated_at_current=n['raw'].get('更新时间'),source_model_current=n['raw'].get('预测模型'),source_quantity_basis_current=n['raw'].get('新品数量口径'))
   if any(z[f] is None for f in ('nov_qty','dec_qty')):z['quantity_source_status']='CURRENT_SOURCE_VALUE_UNKNOWN'
  else:
   for f in ('nov_qty','dec_qty','total_qty'):z[f]=None
   z.update(source_id_current=None,source_updated_at_current=None,source_model_current=None,source_quantity_basis_current=None)
   missing.append(copy.deepcopy(z))
  require(all(z.get(f)==r.get(f) for f in MAPPING_FIELDS),'Quantity refresh changed mapping fields')
  out.append(z)
 return out,missing

def aggregate_components(rows,material_names):
 pools=defaultdict(list)
 for r in rows:
  require(r['current_material'] in material_names,'Material escaped fixed scope')
  pools[(r['demand_id'],r['current_material'])].append(r)
 atoms=[];blocked=[]
 for (did,mat),rs in sorted(pools.items()):
  cs={(r['physical_color_number'],r['physical_color_name_raw']) for r in rs if flag(r['physical_color_resolved'])}
  if len(cs)!=1 or not all(flag(r['physical_color_resolved']) and r['aggregate_status']=='RESOLVED' for r in rs):
   blocked.extend(rs);continue
  number,name=next(iter(cs))
  for month,f in zip(MONTHS,('nov_qty','dec_qty')):
   vals={qty(r[f]) for r in rs};require(len(vals)==1,'Duplicate pool quantity conflict')
   atoms.append({'demand_id':did,'material':mat,'physical_color_number':number,'physical_color_name_raw':name,'month':month,
    'garment_order_qty':next(iter(vals)),'component_count':len(rs),'mapping_origins':sorted({r['physical_mapping_origin'] for r in rs}),
    'quantity_source_status':sorted({r['quantity_source_status'] for r in rs})})
 groups=defaultdict(list)
 for a in atoms:groups[(a['material'],a['physical_color_number'],a['physical_color_name_raw'],a['month'])].append(a)
 color=[]
 for key,rs in sorted(groups.items()):
  q=sums(r['garment_order_qty'] for r in rs)
  color.append({'material':key[0],'physical_color_number':key[1],'physical_color_name_raw':key[2],'month':key[3],
   'garment_order_qty':q['value'],'known_garment_order_qty':q['known_sum'],'unknown_quantity_records':q['unknown_records'],'unique_demand_rows':len(rs),
   'mapping_origins':sorted({o for r in rs for o in r['mapping_origins']})})
 blocked_keys={(r['demand_id'],r['current_material']) for r in blocked}
 pending=[r for r in rows if not flag(r['unique_sku_resolved']) or r['aggregate_status']!='RESOLVED' or (r['demand_id'],r['current_material']) in blocked_keys]
 material=[]
 for name in material_names:
  subset=[r for r in rows if r['current_material']==name];ps=[r for r in pending if r['current_material']==name]
  pids={r['demand_id'] for r in ps};known=[r for r in subset if r['demand_id'] not in pids]
  allq=qstats(subset);pq=qstats(ps);kq=qstats(known)
  material.append({'面料':name,'11月参与件数':allq['nov_qty'],'12月参与件数':allq['dec_qty'],'两月参与件数':allq['total_qty'],
   '已匹配11月':kq['nov_qty'],'已匹配12月':kq['dec_qty'],'已匹配合计':kq['total_qty'],
   '待确认11月':pq['nov_qty'],'待确认12月':pq['dec_qty'],'待确认合计':pq['total_qty'],
   '待确认核对项':pq['review_items'],'需求对象数':allq['unique_demand_rows'],
   '未知数量需求数':allq['total_qty_unknown_records'],'范围说明':'既有精确材料方案可识别范围；新正量无方案另列'})
 return {'components':rows,'pending':pending,'atoms':atoms,'color_months':color,'material_summary':material,
 'all_scope':qstats(rows),'pending_scope':qstats(pending),'aggregation_blocked':qstats(blocked)}

def run(current_path,output,base_dir,old_path,full_conversion):
 output=Path(output);require(not output.exists(),'Refuse overwrite existing refresh output')
 previous=load(base_dir/'summary.json');receipt_path=Path(previous['source_production_receipt'])
 paths={'current':Path(current_path),'old':Path(old_path),'components':base_dir/'components.csv','summary':base_dir/'summary.json','full_conversion':Path(full_conversion),'manual_receipt':receipt_path}
 before={k:sha(p) for k,p in paths.items()}
 require(before['manual_receipt']==previous['source_production_receipt_sha256'],'Manual receipt hash mismatch')
 current=load(current_path);old_doc=load(old_path)
 require(current.get('status')=='SNAPSHOT_READ_COMPLETED','Current source is not a completed snapshot read')
 require(current['business_months']==list(MONTHS),'Unexpected target months')
 require(len(previous['material_names'])==14 and len(set(previous['material_names']))==14,'Frozen material scope must contain 14 distinct names')
 base=read_csv(paths['components']);all_materials=load(full_conversion)['material_rows']
 old,new,audit,exceptions,source_stats=audit_sources(old_doc['datasets']['suggestions']['rows'],current['datasets']['suggestions']['rows'],base,all_materials)
 manual=active_confirmations(load(receipt_path));refreshed,missing=refresh_components(base,old,new,manual)
 result=aggregate_components(refreshed,previous['material_names'])
 baseline_stats=qstats(base)
 same_shop=sum(r['same_shop_spu_has_14_material_plan'] for r in exceptions if r['previously_zero_now_positive'])
 new_positive=[r for r in audit if r['previously_zero_now_positive']]
 updated_times=sorted({str(r['raw'].get('更新时间')) for r in new.values() if r['raw'].get('更新时间')})
 code_only=[]
 code_only_source=previous.get('physical_code_only',[])
 if isinstance(code_only_source,dict):code_only_source=[code_only_source]
 for item in code_only_source:
  # Preserve code-only evidence and get current quantities from exact confirmed scope.
  z=copy.deepcopy(item);rid=item.get('核对ID') or item.get('review_id');rs=[r for r in refreshed if r['review_id']==rid]
  require(bool(rs),'Code-only evidence has no original exact demand scope')
  require(all(not flag(r['unique_sku_resolved']) and r['aggregate_status']!='RESOLVED' for r in rs),'Code-only evidence unexpectedly resolved')
  stat=qstats(rs)
  z['prior_11月件数']=qstats([r for r in base if r['review_id']==rid])['nov_qty'];z['prior_12月件数']=qstats([r for r in base if r['review_id']==rid])['dec_qty']
  for key,field in [('11月件数','nov_qty'),('12月件数','dec_qty'),('两月件数','total_qty')]:z[key]=stat[field]
  z['quantity_basis']='本次建议快照；物理映射状态未升级';code_only.append(z)
 summary={'status':'CURRENT_SUGGESTION_QUANTITY_REFRESH_PARTIAL_MATERIAL_COVERAGE','generated_at_utc':datetime.now(timezone.utc).isoformat(),
  'business_months':list(MONTHS),'material_names':previous['material_names'],'unit':'成衣件','source_acquired_at':current.get('acquired_at'),
  'source_finished_at':current.get('finished_at_utc'),'source_business_update_min':updated_times[0] if updated_times else None,'source_business_update_max':updated_times[-1] if updated_times else None,
  'source_snapshot_status':'当前只读取得的可见快照；未运行上游更新，获取日期不冒充业务更新日期',
  'source_audit':source_stats,'prior_identifiable_14_scope':baseline_stats,'current_identifiable_14_scope':result['all_scope'],
  'current_pending_scope':result['pending_scope'],'old_pending_scope':previous['pending_unique_sku'],
  'material_participation_qty':sums(r['两月参与件数'] for r in result['material_summary']),
  'previously_zero_now_positive':{'rows':len(new_positive),'nov_qty':sums(r['new_nov_qty'] for r in new_positive),'dec_qty':sums(r['new_dec_qty'] for r in new_positive),'total_qty':sums(r['new_total_qty'] for r in new_positive),
   'same_shop_spu_in_14_rows':same_shop,'same_shop_spu_in_14_qty':sums(r['new_total_qty'] for r in new_positive if r['same_shop_spu_has_14_material_plan'])},
  'missing_prior_14_components':len(missing),'current_zero_14_demand_rows':len({r['demand_id'] for r in refreshed if r['total_qty']==0}),
  'current_zero_pending_rows':len({r['demand_id'] for r in result['pending'] if r['total_qty']==0}),
  'physical_code_only':code_only,'sources':{k:{'path':str(p),'sha256':before[k]} for k,p in paths.items()},
  'mapping_identity_and_manual_scope_unchanged':True,'current_new_plan_assignment':'NOT_PERFORMED',
  'limitations':['14款数值仅覆盖既有精确材料方案可识别对象，新正量但无该颜色材料方案的对象另列，不能宣称覆盖当前全部14款相关需求。',
   '原颜色体系、部位、实物色号、SKU和人工确认有效版本未随数量刷新修改。',
   '无材料方案不等于不属于14款；同店铺SPU其他色已用14款也不自动证明该新颜色的材料。',
   '未知和缺源不补零；明确零需求保留原对象和映射。','数据是原净建议月度分配；不重新扣库存、生产中，不计算米/条，不刷新预测。',
   '本次未更新核价/材料/色卡原归档，当前来源数量不代表所有映射来源已实时更新。'],
  'operations':{'database_connections':0,'database_writes':0,'api_calls':0,'training':0,'model_predictions':0}}
 output.mkdir(parents=True,exist_ok=False)
 for filename,rows in [('components.csv',result['components']),('pending.csv',result['pending']),('atoms.csv',result['atoms']),
 ('14款面料月度汇总.csv',result['material_summary']),('面料实物色号月度成衣件数.csv',result['color_months']),
 ('全源4901业务键新旧核对.csv',audit),('原零需求现正量_未自动补材料方案.csv',new_positive),
 ('当前正量但材料范围未确定.csv',exceptions),('旧范围当前源缺失.csv',missing),('仅实物号已确认仍待SKU.csv',code_only)]:
  write_csv(output/filename,rows)
 write_json(output/'summary.json',summary)
 differences=[]
 for k,p in paths.items():require(sha(p)==before[k],'Original source changed: '+k)
 for row in audit:
  if row['quantity_changed'] or row['quantity_basis_changed'] or row['forecast_model_changed'] or row['source_key_state']!='SAME_KEY':differences.append(row)
 write_csv(output/'建议数量变化.csv',differences)
 write_json(output/'manifest.json',{'status':summary['status'],'code_sha256':sha(__file__),'sources':summary['sources'],
  'outputs':{p.name:sha(p) for p in output.iterdir() if p.is_file()},'original_sources_unchanged':True})
 print(json.dumps({k:summary[k] for k in ['status','source_business_update_min','source_business_update_max','current_identifiable_14_scope','current_pending_scope','previously_zero_now_positive']},ensure_ascii=False))
 return summary

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--current',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
 p.add_argument('--base-dir',type=Path,required=True);p.add_argument('--old',type=Path,required=True);p.add_argument('--full-conversion',type=Path,required=True)
 a=p.parse_args();run(a.current,a.output,a.base_dir,a.old,a.full_conversion)
if __name__=='__main__':main()
