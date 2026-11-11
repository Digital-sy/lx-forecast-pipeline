from __future__ import annotations
import csv,json,hashlib,re,unicodedata,collections,datetime
from pathlib import Path
def parse_args():
 import argparse
 p=argparse.ArgumentParser(description="Read-only pending material-color source follow-up. No database or remote calls.")
 p.add_argument('--output-root',type=Path,required=True)
 for k in ['receipt','receipt-completion','pending','scope-summary','manual','new-raw','special-review']:
  p.add_argument('--'+k,type=Path,required=True)
 p.add_argument('--image-review',type=Path)
 p.add_argument('--special-review-spu',required=True)
 return p.parse_args()
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def jd(v,default=None):
 if v is None:return default
 if isinstance(v,str):
  try:return json.loads(v)
  except (ValueError,TypeError):return default
 return v
def norm(v):return re.sub(r'\s+','',unicodedata.normalize('NFKC',str(v or '')))
def color_norm(v):
 a=norm(v)
 return a[:-1] if a.endswith('色') else a
def match(q,r,linked):
 if q['spu']!=r['spu']:return None
 code=norm(q['color_code']); rc=norm(r.get('garment_color_code'))
 name=norm(q['garment_color_name']); rn=norm(r.get('garment_color_name'))
 if r['source_line_id'] in linked:return 'PRIOR_REVIEW_LINE_REFERENCE'
 if code and code==rc:
  return 'SAME_CODE_AND_NAME' if name and color_norm(name)==color_norm(rn) else 'SAME_CODE_NAME_NOT_PROVEN'
 if name and rn and color_norm(name)==color_norm(rn):return 'SAME_NAME_CODE_NOT_PROVEN'
 return None
def uniq(xs):return list(dict.fromkeys(x for x in xs if x is not None and x!=''))
def csvout(p,rows,fields=None):
 fields=fields or list(rows[0])
 with p.open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
  for x in rows:w.writerow({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(dict,list)) else v for k,v in x.items()})
def jwrite(p,data):p.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
def rawtext(r):
 f=jd(r.get('field_sources_json'),{})
 arr=f.get('fabric_assignment_sources') or []
 readable=[];images=[]
 for a in arr:
  if a.get('source_kind')!='fabric_column' or a.get('scope_confirmed') is not True:continue
  c={'sheet':a.get('sheet'),'cell':a.get('cell'),'header':a.get('header'),'text':a.get('text'),'scope':a.get('scope'),'assignment_status':a.get('assignment_status')}
  if 'DISPIMG' in str(a.get('text')) or a.get('material_relevance')=='image_reference':images.append(c)
  else:readable.append(c)
 return readable,images

def main():
 args=parse_args();ROOT=args.output_root
 SOURCES={k:getattr(args,k) for k in ['receipt','receipt_completion','pending','scope_summary','manual','new_raw','special_review']}
 if args.image_review:SOURCES['image_review']=args.image_review
 image_review=json.loads(args.image_review.read_text(encoding='utf-8-sig')) if args.image_review else []
 image_by_line={x['source_line_id']:x for x in image_review}
 special={x['核对编号']:x for x in csv.DictReader(args.special_review.open(encoding='utf-8-sig',newline=''))}
 hashes={k:sha(p) for k,p in SOURCES.items()}
 complete=json.loads(SOURCES['receipt_completion'].read_text(encoding='utf-8'))
 assert complete['production_receipt_sha256']==hashes['receipt']
 rec=json.loads(SOURCES['receipt'].read_text(encoding='utf-8'));tables=rec['tables']
 raw=tables['dwd_sm_garment_order_color_evidence']; mats=tables['dwd_sm_garment_material_color_evidence']
 versions={x['version_id']:x for x in tables['ods_sm_garment_order_document_version']}
 observations=collections.defaultdict(list)
 for x in tables['ods_sm_garment_order_document_observation']:observations[x['version_id']].append(x)
 docvers=collections.defaultdict(set)
 for v in versions.values():docvers[v['document_id']].add(v['version_id'])
 raw_spu=collections.defaultdict(list)
 for x in raw:raw_spu[x['spu']].append(x)
 matraw=collections.defaultdict(list)
 for x in mats:matraw[x['order_evidence_id']].append(x)
 new=[json.loads(s) for s in SOURCES['new_raw'].read_text(encoding='utf-8').splitlines() if s.strip()];new_ids={x['evidence_id'] for x in new}
 manual=json.loads(SOURCES['manual'].read_text(encoding='utf-8'));masters={x['id']:x for x in manual['master']}
 pending=list(csv.DictReader(SOURCES['pending'].open(encoding='utf-8-sig',newline='')))
 groups=collections.defaultdict(list)
 for q in pending:groups[q['review_id']].append(q)
 scope=json.loads(SOURCES['scope_summary'].read_text(encoding='utf-8'))['pending_unique_sku']
 assert len(groups)==scope['review_items'] and len(pending)==scope['component_rows']
 all_rows=[];evidence=[];priorities=[];itemjson=[]
 for review_id,qs in groups.items():
  q=qs[0];prior=masters.get(review_id,{});links=set(prior.get('order_line_ids') or [])
  # Never infer identity by SPU prefix, inherited future default, or a raw order quantity.
  matches=[(r,match(q,r,links)) for r in raw_spu[q['spu']] if match(q,r,links)]
  source_rows=[];has_new=False;allcandidates=[];physical=[];image_count=0
  for r,basis in matches:
   rr=jd(r.get('raw_json'),{});mm=matraw[r['evidence_id']];readable,images=rawtext(r);image_count+=len(images)
   ver=versions[r['version_id']];obs=observations[r['version_id']]
   parts=[];candidates=[];conflicts=[];archivednumbers=[]
   for m in mm:
    conflicts+=jd(m.get('conflicts_json'),[]) or []
    cc=jd(m.get('candidates_json'),[]) or []
    candidates+=[c for c in cc if c.get('material')==q['current_material']]
    src=jd(m.get('field_sources_json'),{}).get('source',{})
    if m['component_locator']=='source_line':
     parts+=src.get('parts') or [];archivednumbers+=src.get('physical_numbers') or []
   # Direct text tokens are clues only. Material number in a fabric header is never a physical color.
   tokens=[]
   for a in readable:
    if '色号' in str(a.get('header')):
     for n in re.findall(r'(?<![\dA-Za-z])(\d{1,3}(?:\.\d+)?)\s*#',str(a.get('text',''))):
      tokens.append({'number':n,'source_cell':a.get('cell'),'sheet':a.get('sheet'),'raw':a['text'],'material_binding':'UNKNOWN_NOT_AUTO_ASSIGNED'})
   allcandidates+=candidates;physical+=tokens+archivednumbers
   origin='LATEST_INCREMENT_74' if r['evidence_id'] in new_ids else ('ARCHIVED_PRIOR_REVIEW' if r['source_line_id'] in links else ('ARCHIVED_SPECIAL_REVIEW_CONTEXT' if review_id in special else 'HISTORY_RAW_NOT_PRIOR_REVIEW_LINKED'))
   has_new=has_new or origin=='LATEST_INCREMENT_74'
   shop=jd(r.get('shop_evidence_json'),{}) or {};status=jd(r.get('order_status_evidence_json'),{})
   source={
    'review_id':review_id,'spu':q['spu'],'demand_shops':uniq(z['shop'] for z in qs),'target_system':q['system'],'target_color_code':q['color_code'],'target_color_name':q['garment_color_name'],'target_material':q['current_material'],
    'evidence_id':r['evidence_id'],'source_line_id':r['source_line_id'],'version_id':r['version_id'],'source_sha256':ver['content_sha256'],'source_path':ver.get('archive_uri') or rr.get('local_path'),
    'file_name':rr.get('file_name') or (obs[-1].get('file_name') if obs else None),'sheet':r['sheet_name'],'row':r['row_number'],'source_url':rr.get('source_url'),
    'origin':origin,'color_link_basis':basis,'source_color_system':r.get('garment_color_system'),'source_color_code':r.get('garment_color_code'),'source_color_name':r.get('garment_color_name'),'source_sku_raw':r.get('sku_raw'),'indexed_spu':r.get('spu'),'row_spu_identity':('ROW_SKU_EXACT_PREFIX' if norm(r.get('sku_raw')).startswith(q['spu']+'-') or norm(r.get('sku_raw'))==q['spu'] else ('ROW_SKU_UNKNOWN' if not r.get('sku_raw') else 'ROW_SKU_DIFFERENT_OR_UNVERIFIED')),
    'identity_status':'SYSTEM_UNPROVEN_INDEX_MATCH_IS_CLUE_ONLY','shop_evidence':shop,'source_order_id':r.get('order_id'),'order_date':r.get('order_date'),'order_date_source':r.get('order_date_source'),'market_raw':r.get('market_raw'),'status_evidence':status,
    'fabric_header_raw':r.get('fabric_header_raw'),'fabric_row_note_raw':r.get('fabric_row_note_raw'),'fabric_cells':readable,'image_cells':images,
    'archived_parts':parts,'archived_physical_numbers':archivednumbers,'readable_physical_tokens':tokens,'same_exact_material_catalog_candidates':candidates,'conflicts':uniq(conflicts),
    'material_evidence_rows':len(mm),'source_multiple_archived_versions':len(docvers[ver['document_id']])>1,'source_version_count':len(docvers[ver['document_id']]),
    'future_applicability':'NOT_CONFIRMED_BY_THIS_FOLLOWUP','formal_mapping_updated':False,'followup_image_review':image_by_line.get(r['source_line_id']),
   }
   source_rows.append(source);evidence.append(source)
  cand_unique={(c.get('sku'),c.get('name'),c.get('part'),c.get('number')):c for c in allcandidates}
  newmatch=[x for x in source_rows if x['origin']=='LATEST_INCREMENT_74'];unlinked=[x for x in source_rows if x['origin']=='HISTORY_RAW_NOT_PRIOR_REVIEW_LINKED']
  oldmatch=[x for x in source_rows if x['origin']=='ARCHIVED_PRIOR_REVIEW'];quant=sum(int(z['total_qty']) for z in qs)
  if q['manual_status']:queue='01_已采用但仍待具体映射'
  elif q['spu']==args.special_review_spu:queue='03_已有专项核查待确认'
  else:queue='02_人工尚未填写'
  if matches:
   coverage='索引同款下找到同码_同名或原引用线索'
   reason='下单原始行未标A2023/B2024；店铺、部位、材料版本及未来适用性需逐项核验'
  elif review_id in special and '参考页' in special[review_id]['证据类型']:
   coverage='仅既有参考页线索';reason='已有专项参考页明确保留；未找到本款本色实际下单行，不当作新发现或未来采用'
  elif raw_spu[q['spu']]:
   coverage='找到本款其他颜色_未找到本色';reason='同SPU有归档行，但没有同码同名或既有逐行引用；不借其他色补填'
  elif prior.get('reference_evidence'):
   coverage='仅既有参考附件线索';reason='本次数据库原始下单行未找到同SPU；保留旧参考附件，不能视为实际下单依据'
  else:
   coverage='现有结构化下单源未找到本款';reason='受控归档范围内无该SPU原始下单行；不能推断全系统没有下单表'
  nums=uniq(str(x.get('number',x.get('query',''))) for x in physical)
  row={'核对ID':review_id,'优先队列':queue,'店铺':'；'.join(uniq(z['shop'] for z in qs)),'SPU':q['spu'],'原颜色体系':q['system'],'原色码':q['color_code'],'成衣颜色':q['garment_color_name'],'当前待核面料':q['current_material'],
   '11月件数':sum(int(z['nov_qty']) for z in qs),'12月件数':sum(int(z['dec_qty']) for z in qs),'合计件数':quant,'需求行数':len(qs),'原人工状态':q['manual_status'],'原待确认原因':q['remaining'],'证据覆盖状态':coverage,'本款全部原始行数':len(raw_spu[q['spu']]),
   '同码同名或原引用线索行数':len(source_rows),'新增74行命中数':len(newmatch),'已在原人工核查引用行数':len(oldmatch),'历史源新增检索线索行数':len(unlinked),'已有材料解释行数':sum(x['material_evidence_rows'] for x in source_rows),
   '可读数字色号线索':'；'.join(nums),'同完整面料目录候选数':len(cand_unique),'同完整面料目录候选':list(cand_unique.values()),'图片待核单元格数':image_count,
   '既有专项核查':special.get(review_id),'本轮定点核查':[x['followup_image_review'] for x in source_rows if x['followup_image_review']],
   '多版本来源线索数':sum(x['source_multiple_archived_versions'] for x in source_rows),'行内款号待核线索数':sum(x['row_spu_identity']=='ROW_SKU_DIFFERENT_OR_UNVERIFIED' for x in source_rows),'材料及色号冲突':uniq([c for x in source_rows for c in x['conflicts']]+(prior.get('detailed_conflicts') or [])+(['ROW_SKU_DIFFERENT_OR_UNVERIFIED'] if any(x['row_spu_identity']=='ROW_SKU_DIFFERENT_OR_UNVERIFIED' for x in source_rows) else [])),
   '尚缺证据':reason,'本轮正式解决':False,'原人工结论是否改写':False,
  }
  all_rows.append(row)
  itemjson.append({'summary':row,'demand_atoms':qs,'prior_review':{'evidence_status':prior.get('evidence_status'),'order_line_ids':list(links),'flags':prior.get('flags',[]),'reference_evidence':prior.get('reference_evidence'),'manual_resolution':prior.get('manual_resolution'),'special_review':special.get(review_id)},'evidence':source_rows})
  usable=[x for x in source_rows if x['image_cells'] or x['fabric_cells'] or x['archived_physical_numbers']]
  if usable:
   # Prioritize a newly acquired relevant line, then original manual-adopted items, then quantity.
   ranked=sorted(usable,key=lambda x:(x['origin']=='LATEST_INCREMENT_74',bool(x['image_cells']),bool(x['same_exact_material_catalog_candidates']),x['order_date'] or ''),reverse=True)
   pick=ranked[0]
   priorities.append({'review_id':review_id,'queue':queue,'spu':q['spu'],'system':q['system'],'code':q['color_code'],'color':q['garment_color_name'],'material':q['current_material'],'demand_quantity':quant,'is_new_increment':has_new,
    'source_line_id':pick['source_line_id'],'source_path':pick['source_path'],'sha256':pick['source_sha256'],'file_name':pick['file_name'],'sheet':pick['sheet'],'row':pick['row'],'image_cells':pick['image_cells'],'fabric_cells':pick['fabric_cells'],
    'same_exact_material_catalog_candidates':pick['same_exact_material_catalog_candidates'],'original_remaining':q['remaining'],'check_question':'核对本色图片/行内主辅料与色号；仅对同完整材料目录查候选，保留店铺/体系/未来方案待核','origin':pick['origin'],'source_sku_raw':pick['source_sku_raw'],'row_spu_identity':pick['row_spu_identity']})
  
 all_rows.sort(key=lambda x:(x['优先队列'],-x['合计件数'],x['核对ID']))
 priorities.sort(key=lambda x:(not x['is_new_increment'],x['queue'],-x['demand_quantity'],x['review_id']))
 priorities=priorities[:15]
 grouped={}
 for field in ['优先队列','证据覆盖状态']:
  grouped[field]={}
  for k in uniq(x[field] for x in all_rows):
   ss=[x for x in all_rows if x[field]==k];grouped[field][k]={'review_items':len(ss),'demand_rows':sum(x['需求行数'] for x in ss),'nov_qty':sum(x['11月件数'] for x in ss),'dec_qty':sum(x['12月件数'] for x in ss),'total_qty':sum(x['合计件数'] for x in ss)}
 summary={'generated_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'READ_ONLY_CANDIDATES_NO_MAPPING_WRITE','special_review_spu':args.special_review_spu,'scope_review_items':len(groups),'scope_demand_rows':len(pending),'scope_demand_total':sum(int(x['total_qty']) for x in pending),'source_raw_rows':len(raw),'source_material_rows':len(mats),'latest_increment_raw_rows':len(new),'latest_increment_matching_spu_rows':sum(x['spu'] in {q['spu'] for q in pending} for x in new),'new_same_color_review_items':sum(x['新增74行命中数']>0 for x in all_rows),'new_same_color_raw_rows':len({x['evidence_id'] for x in evidence if x['origin']=='LATEST_INCREMENT_74'}),'new_same_color_demand_qty':sum(x['合计件数'] for x in all_rows if x['新增74行命中数']),'review_items_with_any_same_color_clue':sum(x['同码同名或原引用线索行数']>0 for x in all_rows),'review_items_with_unlinked_history_clue':sum(x['历史源新增检索线索行数']>0 for x in all_rows),'special_review_reused_items':sum(x['核对ID'] in special for x in all_rows),'history_extra_review_qty':sum(x['合计件数'] for x in all_rows if x['历史源新增检索线索行数']),'review_items_with_material_candidates':sum(x['同完整面料目录候选数']>0 for x in all_rows),'all_raw_color_system_null':all(x['garment_color_system'] is None for x in raw),'new_formal_resolutions':0,'group_statistics':grouped,'source_receipt_finished_at_utc':rec['finished_at_utc'],'source_hashes':hashes,'business_quantity_meaning':'Sealed garment recommendation participation; never raw order quantities','source_business_date':json.loads(SOURCES['scope_summary'].read_text(encoding='utf-8'))['source_business_date'],'limitations':['Source SPU indexing may disagree with row SKU. The raw SKU is retained and never silently corrected.','All raw color-system fields are NULL; same code or name does not prove A2023/B2024 identity.','Archived catalog candidates are retained as candidates only; exact material string required.','History lookup additions are not new source business events.','No future default or manual decision is replaced.','No remote/API/database access or writes were executed.']}
 for n,data in [('candidate_summary.json',summary),('candidate_all_297.json',itemjson),('candidate_priority_15.json',priorities)]:jwrite(ROOT/'outputs'/n,data)
 csvout(ROOT/'outputs/candidate_all_297.csv',all_rows)
 csvout(ROOT/'outputs/candidate_source_line_evidence.csv',evidence)
 csvout(ROOT/'outputs/candidate_priority_15.csv',priorities)
 endhash={k:sha(p) for k,p in SOURCES.items()}
 assert hashes==endhash
 checks={'source_hashes_unchanged':hashes==endhash,'pending_review_count':len(groups),'pending_rows':len(pending),'pending_qty':sum(int(x['total_qty']) for x in pending),'unique_review_ids':len(set(x['核对ID'] for x in all_rows))==len(all_rows),'raw_quantities_never_used_for_demand':True,'no_formal_mapping_updated':True,'source_sha_verified':True,'script_sha256':sha(Path(__file__)),'outputs':{p.name:sha(p) for p in (ROOT/'outputs').glob('candidate_*') if p.is_file()}}
 jwrite(ROOT/'verification/candidate_validation.json',checks)
 print(json.dumps(summary,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
