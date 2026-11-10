"""Offline inventory/database-evidence gap planner. No downloads, DB, or scheduling."""
from __future__ import annotations
import argparse, hashlib, importlib.util, json
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
VERSION='1.0.0'
T_DOC='dim_sm_garment_order_document'
T_VER='ods_sm_garment_order_document_version'
T_EXT='ods_it_garment_order_extraction_run'
T_RAW='dwd_sm_garment_order_color_evidence'
T_MAT='dwd_sm_garment_material_color_evidence'
T_MAN='ods_manual_sm_garment_color_confirmation'
T_REF='dwd_sm_garment_confirmation_evidence_ref'
PARSED={'ok','parsed_with_review','no_order_lines_review'}
def require(v,msg):
 if not v:raise ValueError(msg)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def canon(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def lines(p):return [json.loads(x) for x in Path(p).read_text(encoding='utf-8-sig').splitlines() if x.strip()]
def unique(rows,key,label):
 d={}
 for r in rows:
  k=key(r);require(k not in d,label+' duplicate key');d[k]=r
 return d
def load_indices(paths):
 result={'documents':[],'raw':[],'materials':[],'manual':[]};proof=[]
 for p in paths:
  p=Path(p);m=read(p/'manifest.json');require(m.get('status')=='LOCAL_INDEX_COMPLETE','Index not complete')
  proof.append({'path':str(p/'manifest.json'),'sha256':sha(p/'manifest.json'),'built_at':m.get('built_at'),'source_freshness':m.get('source_freshness')})
  for f,k in [('documents.jsonl','documents'),('order_rows.jsonl','raw'),('enriched_rows.jsonl','materials'),('confirmations.jsonl','manual')]:
   spec=m['outputs'][f];require(sha(p/f)==spec['sha256'],'Index output hash mismatch: '+f)
   rr=lines(p/f);require(len(rr)==spec['rows'],'Index count mismatch');result[k].extend(rr)
   proof.append({'path':str(p/f),'sha256':spec['sha256']})
 return result,proof

def plan(index,snapshot,actions=()):
 require(isinstance(snapshot.get('tables'),dict),'Snapshot needs complete tables, not row counts')
 require(snapshot.get('status') in {'SNAPSHOT_VERIFIED','COMMITTED_VERIFIED'},'Unverified snapshot')
 tabs=snapshot['tables'];require(all(t in tabs for t in (T_DOC,T_VER,T_EXT,T_RAW,T_MAT,T_MAN,T_REF)),'Snapshot table coverage incomplete')
 docids={r['document_id']:r.get('source_id') for r in tabs[T_DOC]}
 # A historical migration timestamp never chooses a current source version.
 versions=unique(tabs[T_VER],lambda r:r['version_id'],'database version')
 versions_by_source=defaultdict(set)
 for v in versions.values():versions_by_source[docids[v['document_id']]].add(v['content_sha256'])
 ext_by_version=defaultdict(list)
 for r in tabs[T_EXT]:ext_by_version[r['version_id']].append(r)
 raw_by_version=defaultdict(list)
 for r in tabs[T_RAW]:raw_by_version[r['version_id']].append(r)
 db_raw_ids={r['evidence_id']:(r['version_id'],r['source_line_id']) for r in tabs[T_RAW]}
 mat_raw_keys={db_raw_ids[r['order_evidence_id']] for r in tabs[T_MAT] if r.get('component_locator')=='source_line' and r['order_evidence_id'] in db_raw_ids}
 raw_index=defaultdict(list)
 for r in index['raw']:raw_index[r['evidence_id']].append(r)
 for key,rr in raw_index.items():require(len({(r['source_id'],r['version_id'],r['raw']['line_id']) for r in rr})==1,'Legacy evidence identity conflict')
 material_index={r['raw']['line_id'] for r in index['materials']}
 manual=unique(index['manual'],lambda r:r['review_id'],'manual event')
 queues={k:[] for k in ('fetch','content_change','unparsed','evidence_review','append_gap','remote_verification')}
 seen={}
 for d in index['documents']:
  key=(d['source_id'],d.get('version_id'))
  if key in seen:
   require(seen[key]==d.get('content_sha256'),'Same source/version has conflicting hashes');continue
  seen[key]=d.get('content_sha256')
  base={'source_id':d['source_id'],'version_id':d.get('version_id'),'content_sha256':d.get('content_sha256'),'file_name':d.get('file_name'),'archive_source':'LOCAL_ARCHIVED_NOT_CURRENT_REMOTE'}
  if d.get('version_id') is None:
   queues['fetch'].append({**base,'reason':'NO_LOCAL_CONTENT','action':'REVIEW_RESOURCE_TYPE_BEFORE_FETCH','automatically_fetchable':False});continue
  if d.get('file_verification')!='HASH_MATCH':queues['evidence_review'].append({**base,'reason':'SOURCE_HASH_NOT_VERIFIED','action':'VERIFY_ORIGINAL_BYTES'})
  if d.get('parse_status') not in PARSED:queues['unparsed'].append({**base,'reason':d.get('parse_status') or 'PARSE_STATUS_UNKNOWN','action':'LOCAL_PARSE_AFTER_SOURCE_VALIDATION'})
  elif d.get('parse_status')!='ok':queues['evidence_review'].append({**base,'reason':'PARSER_REVIEW_FLAGS','action':'REVIEW_ORIGINAL_PARSE_AUDIT'})
  if d['version_id'] not in versions:queues['append_gap'].append({**base,'reason':'VERSION_NOT_IN_READONLY_SNAPSHOT'})
  else:require(versions[d['version_id']]['content_sha256']==d['content_sha256'],'Same version/hash conflict')
  vr={r['raw']['line_id'] for r in index['raw'] if r['version_id']==d['version_id']}
  missing=[line for line in vr if line not in material_index and (d['version_id'],line) not in mat_raw_keys]
  if missing:queues['evidence_review'].append({**base,'reason':'MATERIAL_INTERPRETATION_NOT_PRESENT','raw_evidence_rows':len(missing),'action':'USE_GARMENT_ORDER_COLOR_EVIDENCE_NO_AUTO_CONFIRM'})
  dbraw={r['source_line_id'] for r in raw_by_version.get(d['version_id'],[])}
  missing_db=sorted(vr-dbraw)
  if missing_db:queues['append_gap'].append({**base,'reason':'RAW_EVIDENCE_NOT_IN_READONLY_SNAPSHOT','count':len(missing_db)})
 for r in manual.values():
  if r.get('confirmation_reference_status')!='REFERENCED_FILE_VERSIONS_MATCH':queues['evidence_review'].append({'review_id':r['review_id'],'reason':'MANUAL_REFERENCE_REVIEW_REQUIRED','reference_status':r.get('confirmation_reference_status'),'business_flags_preserved':True,'action':'REVIEW_REFERENCES_WITHOUT_CLEARING_BUSINESS_CONFIRMATION'})
 for a in actions:
  source=a['source_id'];known=versions_by_source.get(source,set());newhash=a.get('current_content_sha256')
  item={**a,'planning_only':True,'known_archived_hashes':sorted(known)}
  if a['state']=='CONTENT_CHANGED':queues['content_change'].append(item)
  if a['action']=='FETCH_REQUIRED':
   if newhash and newhash in known:queues['remote_verification'].append({**item,'reason':'KNOWN_CONTENT_VERSION_METADATA_OBSERVATION_REQUIRED'})
   else:queues['fetch'].append({**item,'automatically_fetchable':a.get('resource_type')=='file','reason':'REMOTE_PLAN_REQUIRES_NEW_CONTENT_VERIFICATION'})
  elif a['state'] not in {'CONTENT_UNCHANGED_VERIFIED'}:queues['remote_verification'].append(item)
  if a['state']=='CONTENT_CHANGED':
   affected=[]
   for r in manual.values():
    if any(z.get('indexed_source_sha256') in known and any(q['source_id']==source and q['raw']['line_id']==z['line_id'] for q in index['raw']) for z in r.get('source_version_links',[])):affected.append(r['review_id'])
   if affected:queues['evidence_review'].append({'source_id':source,'reason':'SOURCE_CONTENT_CHANGED_CONFIRMATION_REVIEW','review_ids':affected,'action':'PRESERVE_ORIGINAL_CONFIRMATIONS_REVIEW_NEW_VERSION'})
 return queues,{'snapshot_status':snapshot['status'],'snapshot_started_at':snapshot.get('started_at_utc'),'snapshot_finished_at':snapshot.get('finished_at_utc'),'source_versions_indexed':len(seen),'distinct_source_ids':len({x[0] for x in seen}),'raw_index_observations':len(index['raw']),'unique_legacy_raw_ids':len(raw_index),'material_index_roots':len(material_index),'manual_events':len(manual),'queue_counts':{k:len(v) for k,v in queues.items()},'current_remote_validated':False,'database_connections':0,'remote_calls':0,'source_or_manual_mutations':0}

def run(indices,snapshot_path,skill_dir,output,previous=None,current=None,parser_version=None):
 out=Path(output);require(not out.exists(),'Refuse output overwrite')
 skill=Path(skill_dir);release=read(skill/'release_manifest.json')
 p=skill/'scripts/plan_refresh.py';require(sha(p)==release['files']['scripts/plan_refresh.py'],'Skill planner hash mismatch')
 spec=importlib.util.spec_from_file_location('_approved_refresh_plan',p);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 data,proof=load_indices(indices);snap=read(snapshot_path);proof.append({'path':str(Path(snapshot_path).resolve()),'sha256':sha(snapshot_path)})
 actions=[];remote={'status':'NOT_REQUESTED_NO_NEW_REMOTE_INVENTORY'}
 require(bool(previous)==bool(current),'Both inventories required together')
 if previous:
  actions,remote=module.plan_documents(read(previous),read(current),previous_parser_version=parser_version,parser_version=parser_version)
  proof.extend({'path':str(Path(x).resolve()),'sha256':sha(x)} for x in (previous,current))
 queues,summary=plan(data,snap,actions);summary['inventory_comparison']=remote
 for pr in proof:require(sha(pr['path'])==pr['sha256'],'Source changed during plan')
 out.mkdir(parents=True,exist_ok=False);files={}
 for k,rows in queues.items():
  q=out/(k+'.jsonl');q.write_text(''.join(canon(r)+'\n' for r in rows),encoding='utf-8');files[q.name]={'sha256':sha(q),'rows':len(rows)}
 config={'schema':'garment_increment_refresh_config/1','status':'TEMPLATE_REQUIRES_NEW_OBSERVATION_AND_EXPLICIT_INPUTS','operations_enabled':False,'source_skill':str(skill.resolve()),'snapshot':str(Path(snapshot_path).resolve()),'source_indexes':[str(Path(x).resolve()) for x in indices],'current_inventory':None,'previous_inventory':current or previous,'allowlist':None,'download_output_dir':None,'parser_path':None,'parser_sha256':None,'helper_path':None,'helper_sha256':None,'parsed_output_dir':None,'new_index_config':{'inventory':None,'downloads':None,'file_audit':None,'order_lines':None,'enriched_evidence':None,'manual_evidence':None,'status_index':None,'verify_raw_files':True},'schedule_enabled':False,'schedule_time':None,'default_mode':'CHECK_ONLY','publication_policy':{'on_failure':'RETAIN_LAST_VERIFIED_BATCH','partial_failure':'PRESERVE_FAILED_ARTIFACT_AND_DO_NOT_ADVANCE_BUSINESS_VERSION','append_mode':'INSERT_ONLY_COMPARE_EXISTING','business_preview_requires':['source_complete','parse_hash_validated','material_manual_scope_validated','append_readback_verified','quantity_and_unknown_conservation_verified']},'required_steps':['Bounded current directory enumeration via existing discover_order_files.py','plan_refresh.py: same-scope inventory comparison','Review explicit file allowlist; fetch_increment.py max15/30 CLI invocations','parse_incremental_refresh.py explicit plan, parser and helper hashes; default preflight only','order_index.py build in new immutable directory','Material interpretation using garment-order-color-evidence; manual events preserved','Build append package; independent read-only snapshot compatibility; controlled append and independent readback','Verify raw quantities, NULL, evidence and confirmation scope conservation','Generate business preview from verified database evidence; do not auto-confirm future recipes'], 'limitations':['Not an execution authorization or a live source state.','Parser helper is still pinned to legacy dependency layout; validate exact dependency before local parse.','No automatic image/material interpretation or future recipe confirmation.','General material/manual increment package adapter still needs explicit per-version evidence contract.']}
 q=out/'run_config_template.json';q.write_text(json.dumps(config,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');files[q.name]={'sha256':sha(q)}
 manifest={'status':'PLAN_ONLY_NOT_EXECUTED','version':VERSION,'built_at_utc':datetime.now(timezone.utc).isoformat(),'builder_sha256':sha(__file__),'skill_planner_sha256':sha(p),'sources':proof,'summary':summary,'outputs':files,'writes':'NEW_LOCAL_PLAN_ARTIFACTS_ONLY','schedule_enabled':False}
 (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return manifest

def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--index',action='append',required=True);p.add_argument('--snapshot',required=True);p.add_argument('--source-skill',required=True);p.add_argument('--output',required=True);p.add_argument('--previous-inventory');p.add_argument('--current-inventory');p.add_argument('--parser-version')
 a=p.parse_args(argv);m=run(a.index,a.snapshot,a.source_skill,a.output,a.previous_inventory,a.current_inventory,a.parser_version);print(json.dumps(m['summary'],ensure_ascii=False));return 0
if __name__=='__main__':raise SystemExit(main())
