"""Explicit, immutable garment source stages. No database clients or publish command.

CHECK_ONLY: verify local pinned inputs; no CLI execution.
DISCOVER --execute-read: one bounded directory inventory, delta plan and reference impacts.
PREPARE --execute-read --discovery-run: fetch explicit delta, pinned raw parser, index, package.
The last stage never writes a database or promotes material/manual interpretations.
"""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

VERSION = '1.1.0-date-evidence-gate'
TOOL_NAMES = ('discover_order_files.py','plan_refresh.py','fetch_increment.py',
              'parse_incremental_refresh.py','order_index.py','build_increment_package.py',
              'build_load_package.py','mysql_load_core.py','append_load_core.py')
TABLES = ('ods_it_garment_order_ingestion_run','dim_sm_garment_order_document',
          'ods_sm_garment_order_document_version','ods_sm_garment_order_document_observation',
          'ods_it_garment_order_extraction_run','dwd_sm_garment_order_color_evidence',
          'dwd_sm_garment_material_color_evidence','ods_manual_sm_garment_color_confirmation',
          'dwd_sm_garment_confirmation_evidence_ref')
STAGES = ('CHECK_ONLY','DISCOVER','PREPARE')

class PipelineError(ValueError):
    def __init__(self, code): self.code=code; super().__init__(code)

def need(ok, code):
    if not ok: raise PipelineError(code)

def now(): return datetime.now(timezone.utc).isoformat()
def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()
def read(path): return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def lines(path): return [json.loads(v) for v in Path(path).read_text(encoding='utf-8-sig').splitlines() if v.strip()]
def dump(path,value):
    with Path(path).open('x',encoding='utf-8',newline='\n') as f:
        json.dump(value,f,ensure_ascii=False,indent=2,allow_nan=False); f.write('\n')
def write_lines(path,rows):
    with Path(path).open('x',encoding='utf-8',newline='\n') as f:
        for r in rows: f.write(json.dumps(r,ensure_ascii=False,allow_nan=False)+'\n')
def inside(path,root):
    p,r=Path(path).resolve(),Path(root).resolve()
    need(p!=r and r in p.parents,'OUTPUT_OUTSIDE_TASK'); return p

def verify_spec(spec,label):
    need(isinstance(spec,dict) and set(spec)=={'path','sha256'},'INVALID_SPEC:'+label)
    p=Path(spec['path']).resolve()
    need(p.is_file(),'MISSING_DEPENDENCY:'+label)
    need(sha(p)==spec['sha256'],'HASH_MISMATCH:'+label)
    return p

def validate_config(path):
    cfg=read(path)
    need(set(cfg)=={'schema_version','task_root','python','tools','sources','discovery'},'CONFIG_FIELDS')
    need(cfg['schema_version']=='garment_source_pipeline/1','CONFIG_VERSION')
    need(set(cfg['tools'])==set(TOOL_NAMES),'TOOL_SCOPE')
    need(set(cfg['sources'])=={'previous_inventory','publication_receipt','parser_dependencies','ddl'},'SOURCE_SCOPE')
    tools={k:verify_spec(v,k) for k,v in cfg['tools'].items()}
    sources={k:verify_spec(v,k) for k,v in cfg['sources'].items()}
    need(len({p.parent for p in tools.values()})==1,'STABLE_MODULES_MUST_SHARE_DIRECTORY')
    need(Path(cfg['python']).is_file(),'MISSING_DEPENDENCY:python')
    inv=read(sources['previous_inventory'])
    need(inv.get('complete') is True and not inv.get('errors') and isinstance(inv.get('root'),str),'BASELINE_NOT_COMPLETE_DIRECTORY')
    need(inv.get('status')=='COMPLETE' and isinstance(inv.get('pages'),int) and inv['pages']>0,'INCREMENT_IS_NOT_FULL_INVENTORY')
    need(len({r['token'] for r in inv['items']})==len(inv['items']),'BASELINE_DUPLICATE_SOURCE')
    d=cfg['discovery']
    need(set(d)=={'root_token','cli_executable','cli_script','max_pages','max_folders','max_items','page_size','timeout_seconds'},'DISCOVERY_FIELDS')
    need(inv['root']==d['root_token'],'ROOT_SCOPE_CHANGED')
    for key,cap in [('max_pages',80),('max_folders',80),('max_items',5000),('page_size',200),('timeout_seconds',30)]:
        need(isinstance(d[key],int) and 1<=d[key]<=cap,'BUDGET_OUT_OF_RANGE:'+key)
    deps=read(sources['parser_dependencies'])
    for key in ('parser','helper'):
        verify_spec({'path':deps[key+'_path'],'sha256':deps[key+'_sha256']},key)
    return cfg,tools,sources

def checked_receipt(path,normalizer=None,digester=None):
    receipt=read(path)
    need(receipt.get('status')=='COMMITTED_VERIFIED','PUBLICATION_NOT_VERIFIED')
    need(receipt.get('final_readback',{}).get('state')=='MATCH','INDEPENDENT_READBACK_NOT_MATCHED')
    need(receipt.get('loader_receipt',{}).get('independent_readback',{}).get('state')=='MATCH','LOADER_READBACK_NOT_MATCHED')
    need(set(receipt.get('tables',{}))==set(TABLES) and set(receipt.get('metadata',{}))==set(TABLES),'SNAPSHOT_TABLE_SCOPE')
    for t in TABLES:
        details=receipt['final_readback']['tables'][t]
        need(details['matched'] is True and details['actual_after_rows']==len(receipt['tables'][t])==receipt['row_counts'][t],'SNAPSHOT_ROW_COUNT:'+t)
        need(all(details.get(k,0)==0 for k in ('missing_keys','extra_keys','old_fields_changed_including_audit','new_fields_changed','new_audit_invalid')),'SNAPSHOT_DIFFERENCES:'+t)
    if normalizer is not None:
        norm=normalizer(receipt['tables'],receipt['metadata'])
        for t in TABLES:
            need(digester(norm[t])==receipt['final_readback']['tables'][t]['after_sha256'],'SNAPSHOT_CONTENT_HASH:'+t)
    return receipt

def load_pure_normalizer(tools):
    # These exact pinned modules have no imports of DB drivers and no connection side effects.
    modules={}
    for name in ('mysql_load_core','append_load_core'):
        spec=importlib.util.spec_from_file_location(name,tools[name+'.py'])
        module=importlib.util.module_from_spec(spec); sys.modules[name]=module;spec.loader.exec_module(module);modules[name]=module
    return modules['append_load_core'].normalize_snapshot,modules['mysql_load_core'].sha

def impact_rows(actions,tables):
    changed={a['source_id']:a for a in actions if a['state'] in ('METADATA_CHANGED','CONTENT_CHANGED','SOURCE_MISSING','SOURCE_PRESENCE_UNKNOWN','REPARSE_REQUIRED') and a['resource_type']!='folder'}
    docs={r['document_id']:r for r in tables[TABLES[1]]}
    confirmations={r['confirmation_id']:r for r in tables[TABLES[7]]}
    versions={r['version_id']:r for r in tables[TABLES[2]]}
    latest={}
    for c in confirmations.values(): latest[c['review_id']]=max(latest.get(c['review_id'],0),c['confirmation_version'])
    result=[]
    for ref in tables[TABLES[8]]:
        need(ref['document_id'] in docs and ref['confirmation_id'] in confirmations,'BROKEN_REFERENCE_ID')
        source_id=docs[ref['document_id']]['source_id']
        if source_id not in changed: continue
        c=confirmations[ref['confirmation_id']]; a=changed[source_id]
        result.append({'source_id':source_id,'source_state':a['state'],'reference_id':ref['reference_id'],
          'confirmation_id':c['confirmation_id'],'review_id':c['review_id'],'confirmation_version':c['confirmation_version'],
          'latest_confirmation_version':c['confirmation_version']==latest[c['review_id']],
          'document_id':ref['document_id'],'version_id':ref['version_id'],
          'referenced_content_sha256':versions.get(ref['version_id'],{}).get('content_sha256'),
          'order_evidence_id':ref.get('order_evidence_id'),'material_evidence_id':ref.get('material_evidence_id'),
          'action':'HUMAN_REVIEW_REQUIRED_PRESERVE_OLD_CONFIRMATION','automatic_rebind':False})
    return result

def select_fetch(actions):
    selected=[a for a in actions if a['action']=='FETCH_REQUIRED']
    unsupported=[a['source_id'] for a in selected if a['resource_type']!='file' or Path(a['current_metadata'].get('name','')).suffix.lower() not in ('.xlsx','.xlsm','.xls')]
    need(not unsupported,'UNSUPPORTED_FETCH_SOURCE_REQUIRES_REVIEW')
    need(len(selected)<=15,'FETCH_FILE_BUDGET_EXCEEDED')
    need(len({a['source_id'] for a in selected})==len(selected),'FETCH_DUPLICATE_SOURCE')
    return selected

def execute_command(argv,cwd,timeout):
    env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1')
    p=subprocess.run(argv,cwd=cwd,env=env,shell=False,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    return {'returncode':p.returncode,'stdout_sha256':hashlib.sha256(p.stdout.encode()).hexdigest(),
            'stderr_sha256':hashlib.sha256(p.stderr.encode()).hexdigest()}

def run(config_path,output_dir,*,stage='CHECK_ONLY',execute_read=False,discovery_run=None,runner=execute_command):
    need(stage in STAGES,'INVALID_STAGE')
    need(stage=='CHECK_ONLY' or execute_read,'EXPLICIT_READ_FLAG_REQUIRED')
    cfg,tools,sources=validate_config(config_path)
    root=Path(cfg['task_root']).resolve();out=inside(output_dir,root)
    need(not out.exists(),'OUTPUT_EXISTS_REFUSE_OVERWRITE')
    protected={str(p):sha(p) for p in [Path(config_path),*tools.values(),*sources.values()]}
    deps=read(sources['parser_dependencies'])
    protected.update({str(Path(deps[k+'_path'])):deps[k+'_sha256'] for k in ('parser','helper')})
    for n in ('order_date_evidence.py','source_date_gate.py','source_material_candidates.py'):
        dep=Path(__file__).with_name(n); protected[str(dep)]=sha(dep)
    out.mkdir(parents=True,exist_ok=False)
    receipt={'schema_version':'garment_source_pipeline_run/1','version':VERSION,'orchestrator_sha256':sha(__file__),'stage':stage,'started_at_utc':now(),
      'status':'RUNNING','commands':[],'protected_sources':protected,'database_operations':0,'remote_writes':0,
      'manual_rows_created':0,'manual_rows_updated':0,'cursor_advanced':False,
      'limits':['Metadata unchanged does not verify unchanged bytes.','New raw evidence does not approve material, part, physical color or future recipes.','No DB publication is available in this entrypoint.']}
    dump(out/'run_started.json',receipt)
    def command(name,args,timeout=120):
        cmd=[cfg['python'],'-B',str(tools[name]),*map(str,args)]
        r=runner(cmd,str(root),timeout); receipt['commands'].append({'script':name,'argv':cmd,'result':r})
        need(r['returncode']==0,'STAGE_COMMAND_FAILED:'+name)
    try:
        normalizer,digester=load_pure_normalizer(tools)
        current=checked_receipt(sources['publication_receipt'],normalizer,digester)
        receipt['current_support_rows']={t:len(v) for t,v in current['tables'].items()}
        d=cfg['discovery']; missing=[k for k in ('cli_executable','cli_script') if not Path(d[k]).is_file()]
        receipt['cli_dependency_status']='MISSING_DEPENDENCY' if missing else 'AVAILABLE'
        receipt['missing_dependencies']=missing
        if stage=='CHECK_ONLY':
            receipt['status']='MISSING_DEPENDENCY' if missing else 'CHECK_ONLY_READY'
        else:
            need(not missing,'MISSING_DEPENDENCY:cli')
            if stage=='DISCOVER':
                invdir=out/'inventory';plan=out/'plan'
                args=['--folder-token',d['root_token'],'--cli-executable',d['cli_executable'],'--cli-script',d['cli_script'],'--output-dir',invdir,'--execute-read']
                for key in ('max_pages','max_folders','max_items','page_size'): args += ['--'+key.replace('_','-'),d[key]]
                args += ['--timeout',d['timeout_seconds']]
                command('discover_order_files.py',args,d['max_pages']*d['timeout_seconds']+90)
                inv=read(invdir/'inventory_current.json')
                need(inv.get('complete') is True and inv.get('status')=='COMPLETE','DISCOVERY_INCOMPLETE')
                command('plan_refresh.py',['--previous',sources['previous_inventory'],'--current',invdir/'inventory_current.json','--output-dir',plan])
                actions=lines(plan/'actions.jsonl'); impacted=impact_rows(actions,current['tables'])
                write_lines(out/'manual_reference_impacts.jsonl',impacted)
                receipt.update(status='DISCOVERED_NOT_PREPARED',inventory=str(invdir/'inventory_current.json'),inventory_sha256=sha(invdir/'inventory_current.json'),
                  plan=str(plan/'actions.jsonl'),plan_sha256=sha(plan/'actions.jsonl'),states=dict(Counter(a['state'] for a in actions)),
                  affected_reference_count=len(impacted),affected_confirmation_count=len({r['confirmation_id'] for r in impacted}),
                  affected_latest_confirmation_count=len({r['confirmation_id'] for r in impacted if r['latest_confirmation_version']}),
                  source_missing_count=sum(a['state']=='SOURCE_MISSING' for a in actions),
                  candidate_fetch_count=sum(a['action']=='FETCH_REQUIRED' for a in actions))
            else:
                need(discovery_run is not None,'DISCOVERY_RUN_REQUIRED')
                prior=Path(discovery_run).resolve();pr=read(prior/'pipeline_manifest.json')
                need(pr.get('status')=='DISCOVERED_NOT_PREPARED','DISCOVERY_RUN_NOT_READY')
                need(pr['protected_sources']==protected,'DISCOVERY_CONFIGURATION_CHANGED')
                inventory_path=Path(pr['inventory']);plan_path=Path(pr['plan'])
                need(sha(inventory_path)==pr['inventory_sha256'] and sha(plan_path)==pr['plan_sha256'],'DISCOVERY_ARTIFACT_CHANGED')
                actions=lines(plan_path); selected=select_fetch(actions)
                dump(out/'fetch_allowlist.json',selected)
                if not selected:
                    receipt.update(status='NO_INCREMENT_NOT_PUBLISHED',new_files=0,next_inventory_candidate=str(inventory_path),next_inventory_sha256=sha(inventory_path))
                else:
                    acq=out/'acquisition';parsed=out/'parsed';index=out/'index';package=out/'package'
                    command('fetch_increment.py',['--allowlist',out/'fetch_allowlist.json','--output-dir',acq,'--cli-executable',d['cli_executable'],'--cli-script',d['cli_script'],'--timeout',120,'--execute-read'],len(selected)*250+90)
                    acquisition=read(acq/'download_manifest.json');need(acquisition['status']=='COMPLETE','FETCH_INCOMPLETE')
                    selected_plan=out/'parse_actions.jsonl';write_lines(selected_plan,selected)
                    command('parse_incremental_refresh.py',['--task-root',root,'--output-dir',parsed,'--inventory',acq/'inventory_increment.json','--downloads',acq/'downloads.jsonl','--plan',selected_plan,'--dependency-manifest',sources['parser_dependencies'],'--execute-local-parse'],960)
                    parser=read(parsed/'parse_manifest.json');need(parser['status'] in ('PARSED','PARSED_WITH_REVIEW'),'PARSE_FAILED')
                    from source_date_gate import inspect_rows
                    date_check=inspect_rows(lines(parsed/'order_lines.jsonl'))
                    dump(out/'date_evidence_gate.json',date_check)
                    receipt['date_evidence_gate']={'status':date_check['status'],'rows_checked':date_check['rows_checked'],'requires_reparse_rows':date_check['requires_reparse_rows']}
                    need(date_check['requires_reparse_rows']==0,'DATE_EVIDENCE_REPARSE_REQUIRED')
                    from source_material_candidates import build as build_material_candidates
                    material=build_material_candidates(parsed/'order_lines.jsonl',out/'material_candidates')
                    receipt['material_candidates']=material
                    receipt['material_candidate_builder_sha256']=sha(Path(__file__).with_name('source_material_candidates.py'))
                    inc=read(acq/'inventory_increment.json');inc.update(root=d['root_token'],errors=[])
                    dump(out/'index_inventory.json',inc)
                    index_config={'inventory':str(out/'index_inventory.json'),'downloads':str(acq/'downloads.jsonl'),'file_audit':str(parsed/'order_file_audit.jsonl'),'order_lines':str(parsed/'order_lines.jsonl'),'verify_raw_files':True}
                    dump(out/'index_config.json',index_config)
                    command('order_index.py',['build','--config',out/'index_config.json','--output-dir',index],960)
                    snapshot={'status':'SNAPSHOT_VERIFIED','tables':current['tables'],'metadata':current['metadata'],
                      'source_kind':'COMMITTED_INDEPENDENT_READBACK','original_status':current['status'],
                      'source_receipt':{'path':str(sources['publication_receipt']),'sha256':sha(sources['publication_receipt']),'finished_at_utc':current.get('finished_at_utc')},
                      'derived_at_utc':now(),'new_database_queries':0}
                    dump(out/'baseline_snapshot.json',snapshot)
                    paths={'index_dir':index,'acquisition_manifest':acq/'download_manifest.json','fetch_plan':acq/'fetch_plan.json','parse_manifest':parsed/'parse_manifest.json','downloads':acq/'downloads.jsonl','fetch_allowlist':out/'fetch_allowlist.json','acquisition_inventory':acq/'inventory_increment.json','parse_inventory':acq/'inventory_increment.json','parse_actions':selected_plan}
                    dump(out/'package_config.json',{'schema_version':'garment_raw_increment_package_config/1','paths':{k:str(v) for k,v in paths.items()}})
                    command('build_increment_package.py',['--config',out/'package_config.json','--production-before',out/'baseline_snapshot.json','--ddl',sources['ddl'],'--output',package],960)
                    pm=read(package/'manifest.json')
                    need(pm['status']=='LOCAL_PACKAGE_VALIDATED_NOT_IMPORTED','PACKAGE_NOT_VALIDATED')
                    need(all(pm['tables'][t]['row_count']==0 for t in TABLES[6:]),'SEMANTIC_OR_MANUAL_ROWS_FORBIDDEN')
                    receipt.update(status='PREPARED_NOT_PUBLISHED',package=str(package),package_manifest_sha256=sha(package/'manifest.json'),append_preview=pm['append_preview'],
                      parse_status=parser['status'],new_files=len(selected),next_inventory_candidate=str(inventory_path),next_inventory_sha256=sha(inventory_path))
        for p,h in protected.items(): need(sha(p)==h,'INPUT_CHANGED_DURING_RUN:'+Path(p).name)
        receipt['protected_sources_unchanged']=True
    except Exception as exc:
        receipt.update(status='BLOCKED',error_code=exc.code if isinstance(exc,PipelineError) else type(exc).__name__)
        raise
    finally:
        receipt['finished_at_utc']=now();dump(out/'pipeline_manifest.json',receipt)
    return receipt

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',required=True);p.add_argument('--output-dir',required=True)
    p.add_argument('--stage',choices=STAGES,default='CHECK_ONLY');p.add_argument('--execute-read',action='store_true');p.add_argument('--discovery-run')
    a=p.parse_args(argv)
    try: r=run(a.config,a.output_dir,stage=a.stage,execute_read=a.execute_read,discovery_run=a.discovery_run)
    except Exception as exc:
        print(json.dumps({'status':'BLOCKED','reason':exc.code if isinstance(exc,PipelineError) else type(exc).__name__}));return 2
    print(json.dumps({k:r.get(k) for k in ('status','stage','candidate_fetch_count','affected_reference_count','new_files','package')},ensure_ascii=False))
    return 0 if r['status']!='MISSING_DEPENDENCY' else 2

if __name__=='__main__': raise SystemExit(main())
