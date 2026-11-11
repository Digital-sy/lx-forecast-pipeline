"""Focused offline tests for source stage orchestration and conservative evidence."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
RUNTIME=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(RUNTIME))
sys.path.insert(0,str(RUNTIME/'tools'))
import shutil
import mysql_load_core as core
import append_load_core as app
import source_pipeline_v2 as p
import source_material_candidates as m

def digest(v):return core.sha(v)

def receipt_fixture():
    metadata={};structure={}
    for table in p.TABLES:
        columns=[]
        for i,(name,dtype,ctype) in enumerate(((core.PK[table],'char','char(64)'),('create_time','datetime','datetime(6)'),('update_time','datetime','datetime(6)')),1):
            columns.append({'TABLE_NAME':table,'COLUMN_NAME':name,'ORDINAL_POSITION':i,'DATA_TYPE':dtype,'COLUMN_TYPE':ctype,'IS_NULLABLE':'NO','COLUMN_DEFAULT':None,'EXTRA':'','CHARACTER_SET_NAME':'utf8mb4' if dtype=='char' else None,'COLLATION_NAME':'utf8mb4_bin' if dtype=='char' else None})
        metadata[table]=columns
        structure[table]={'columns':[{k:v for k,v in c.items() if k not in ('TABLE_NAME','DATA_TYPE')} for c in columns],'options':{'ENGINE':'InnoDB'}}
    tables={t:[] for t in p.TABLES};normal=app.normalize_snapshot(tables,metadata)
    return {'status':'READ_ONLY_SNAPSHOT_VERIFIED','operation':'NINE_SUPPORT_TABLES_READ_ONLY','database_writes':0,'schema':'lingxing','server_uuid':'5ee7874b-89d2-11ef-83b6-00163e025678','structure':structure,'tables':tables,'metadata':metadata,'row_counts':{t:0 for t in p.TABLES},'total_rows':0,'canonical_sha256':core.sha(normal),'canonical_table_sha256':{t:core.sha(normal[t]) for t in p.TABLES},'started_at_utc':'2026-11-11T00:00:00+00:00','finished_at_utc':'2026-11-11T00:00:01+00:00'}

class PipelineTests(unittest.TestCase):
    def setUp(self):
        temp=Path(__file__).resolve().parents[1]/'temp';temp.mkdir(exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=temp,prefix='source_pipeline_test_');self.root=Path(self.tmp.name)
        self.tools=self.root/'tools';shutil.copytree(RUNTIME/'tools',self.tools)
        def spec(name,value=''):
            path=self.tools/name
            if isinstance(value,(dict,list)):p.dump(path,value)
            elif not path.exists():path.write_text(value,encoding='utf-8')
            return {'path':str(path),'sha256':p.sha(path)}
        self.cfg={'schema_version':'garment_source_pipeline/2','task_root':str(self.root),'python':sys.executable,
          'tools':{n:spec(n) for n in p.TOOL_NAMES},'sources':{},'reparse_queue':None,
          'discovery':{'identity':'bot','root_token':'ApprovedFolder','cli_executable':str(self.tools/'node'),'cli_script':str(self.tools/'cli.js'),
                       'max_pages':80,'max_folders':80,'max_items':5000,'page_size':200,'timeout_seconds':30}}
        spec('node');spec('cli.js')
        parser=spec('extract_order_lines.py');helper=spec('extract_fields.py')
        self.cfg['sources']={
          'previous_inventory':spec('baseline.json',{'root':'ApprovedFolder','status':'COMPLETE','complete':True,'pages':58,'items':[]}),
          'publication_receipt':spec('receipt.json',receipt_fixture()),
          'parser_dependencies':spec('dependency_manifest.json'),
          'ddl':spec('schema.sql','-- synthetic only')}
        self.config=self.root/'config.json';self.save_config()
        self.normalizer=patch.object(p,'load_pure_normalizer',return_value=(lambda rows,meta:rows,digest));self.normalizer.start()
    def tearDown(self):self.normalizer.stop();self.tmp.cleanup()
    def save_config(self):self.config.write_text(json.dumps(self.cfg),encoding='utf-8')
    def test_check_only_never_dispatches(self):
        def forbidden(*a):self.fail('CHECK_ONLY invoked process')
        r=p.run(self.config,self.root/'out',runner=forbidden)
        self.assertEqual(r['status'],'CHECK_ONLY_READY');self.assertEqual(r['commands'],[])
        self.assertEqual(r['database_operations'],0)
    def test_missing_cli_has_explicit_status(self):
        self.cfg['discovery']['cli_script']=str(self.root/'missing.js');self.save_config()
        self.assertEqual(p.run(self.config,self.root/'out')['status'],'MISSING_DEPENDENCY')
    def test_missing_read_flag_prevents_output(self):
        with self.assertRaisesRegex(p.PipelineError,'EXPLICIT_READ_FLAG_REQUIRED'):p.run(self.config,self.root/'out',stage='DISCOVER')
        self.assertFalse((self.root/'out').exists())
    def test_partial_inventory_cannot_be_baseline(self):
        f=Path(self.cfg['sources']['previous_inventory']['path']);o=p.read(f);o['status']='FETCHED_INCREMENT_ONLY';f.write_text(json.dumps(o));self.cfg['sources']['previous_inventory']['sha256']=p.sha(f);self.save_config()
        with self.assertRaisesRegex(p.PipelineError,'INCREMENT_IS_NOT_FULL'):p.run(self.config,self.root/'out')
    def test_tool_hash_drift_blocks(self):
        Path(self.cfg['tools']['fetch_increment.py']['path']).write_text('changed')
        with self.assertRaisesRegex(p.PipelineError,'HASH_MISMATCH'):p.run(self.config,self.root/'out')
    def test_scope_and_budget_cannot_expand(self):
        self.cfg['discovery']['max_pages']=81;self.save_config()
        with self.assertRaisesRegex(p.PipelineError,'BUDGET_OUT_OF_RANGE'):p.run(self.config,self.root/'out')
    def test_no_output_overwrite(self):
        p.run(self.config,self.root/'out')
        with self.assertRaisesRegex(p.PipelineError,'OUTPUT_EXISTS'):p.run(self.config,self.root/'out')
    def test_receipt_must_be_committed_and_readback_matched(self):
        rec=receipt_fixture();rec['status']='COMMIT_UNKNOWN';f=self.root/'bad.json';p.dump(f,rec)
        with self.assertRaisesRegex(p.PipelineError,'BASELINE_NOT_VERIFIED'):p.checked_receipt(f)
    def test_receipt_table_hash_checked(self):
        rec=receipt_fixture();rec['canonical_sha256']='bad';f=self.root/'bad.json';p.dump(f,rec)
        with self.assertRaisesRegex(p.PipelineError,'SNAPSHOT_CONTENT_HASH'):p.checked_receipt(f,lambda r,m:r,digest)
    def test_discover_only_reads_inventory_then_plans(self):
        calls=[]
        def fake(argv,cwd,timeout):
            name=Path(argv[2]).name;calls.append(name);out=Path(argv[argv.index('--output-dir')+1]);out.mkdir()
            if name=='discover_order_files.py':p.dump(out/'inventory_current.json',{'status':'COMPLETE','complete':True,'root':'ApprovedFolder','items':[]})
            elif name=='plan_refresh.py':p.write_lines(out/'actions.jsonl',[])
            else:self.fail('unexpected command')
            return {'returncode':0}
        r=p.run(self.config,self.root/'out',stage='DISCOVER',execute_read=True,runner=fake)
        self.assertEqual(calls,['discover_order_files.py','plan_refresh.py']);self.assertEqual(r['status'],'DISCOVERED_NOT_PREPARED');self.assertFalse(r['cursor_advanced'])
    def test_incomplete_discovery_never_plans(self):
        calls=[]
        def fake(argv,cwd,timeout):
            calls.append(Path(argv[2]).name);out=Path(argv[argv.index('--output-dir')+1]);out.mkdir();p.dump(out/'inventory_current.json',{'status':'INCOMPLETE','complete':False});return {'returncode':0}
        with self.assertRaisesRegex(p.PipelineError,'DISCOVERY_INCOMPLETE'):p.run(self.config,self.root/'out',stage='DISCOVER',execute_read=True,runner=fake)
        self.assertEqual(calls,['discover_order_files.py']);self.assertEqual(p.read(self.root/'out/pipeline_manifest.json')['status'],'BLOCKED')
    def test_prepare_config_mismatch_no_fetch(self):
        prior=self.root/'prior';prior.mkdir();p.dump(prior/'pipeline_manifest.json',{'status':'DISCOVERED_NOT_PREPARED','protected_sources':{}})
        with self.assertRaisesRegex(p.PipelineError,'DISCOVERY_CONFIGURATION_CHANGED'):p.run(self.config,self.root/'out',stage='PREPARE',execute_read=True,discovery_run=prior,runner=lambda *a:self.fail('fetch called'))
    def test_fetch_budget_and_unsupported_not_silent(self):
        a={'action':'FETCH_REQUIRED','source_id':'x','resource_type':'sheet','current_metadata':{'name':'source'}}
        with self.assertRaisesRegex(p.PipelineError,'UNSUPPORTED_FETCH'):p.select_fetch([a])
        a['resource_type']='file';a['current_metadata']['name']='a.xlsx'
        with self.assertRaisesRegex(p.PipelineError,'FETCH_FILE_BUDGET'):p.select_fetch([dict(a,source_id=str(i)) for i in range(16)])
    def test_latest_and_superseded_manual_impacts_preserved(self):
        t={n:[] for n in p.TABLES};t[p.TABLES[1]]=[{'document_id':'d','source_id':'s'}]
        t[p.TABLES[2]]=[{'version_id':'v','content_sha256':'hash'}]
        t[p.TABLES[7]]=[{'confirmation_id':f'c{v}','review_id':'r','confirmation_version':v} for v in (1,2)]
        t[p.TABLES[8]]=[{'reference_id':f'ref{v}','document_id':'d','version_id':'v','confirmation_id':f'c{v}'} for v in (1,2)]
        before=copy.deepcopy(t);r=p.impact_rows([{'source_id':'s','resource_type':'file','state':'CONTENT_CHANGED'}],t)
        self.assertEqual(len(r),2);self.assertEqual([v['latest_confirmation_version'] for v in r],[False,True]);self.assertEqual(t,before)
        self.assertTrue(all(not v['automatic_rebind'] for v in r))

    def prepare_prior(self):
        prior=self.root/'prior';prior.mkdir()
        inv=prior/'inventory.json';p.dump(inv,{'status':'COMPLETE','complete':True,'root':'ApprovedFolder','items':[]})
        actions=prior/'actions.jsonl';p.write_lines(actions,[{'source_id':'source','token':'source','resource_type':'file','state':'METADATA_CHANGED','action':'FETCH_REQUIRED','current_metadata':{'name':'source.xlsx'}}])
        config,tools,sources=p.validate_config(self.config)
        protected=p.protected_files(self.config,config,tools,sources)
        p.dump(prior/'pipeline_manifest.json',{'status':'DISCOVERED_NOT_PREPARED','protected_sources':protected,'inventory':str(inv),'inventory_sha256':p.sha(inv),'plan':str(actions),'plan_sha256':p.sha(actions)})
        return prior
    def test_fetch_failure_never_parses_or_packages(self):
        prior=self.prepare_prior();calls=[]
        def fail(argv,cwd,timeout):calls.append(Path(argv[2]).name);return {'returncode':2}
        with self.assertRaisesRegex(p.PipelineError,'STAGE_COMMAND_FAILED:fetch_increment'):
            p.run(self.config,self.root/'out',stage='PREPARE',execute_read=True,discovery_run=prior,runner=fail)
        self.assertEqual(calls,['fetch_increment.py']);self.assertFalse((self.root/'out/package').exists())
    def test_prepare_orders_stable_stages_and_preserves_manual_scope(self):
        prior=self.prepare_prior();calls=[]
        def fake(argv,cwd,timeout):
            name=Path(argv[2]).name;calls.append(name)
            flag='--output' if name=='build_increment_package.py' else '--output-dir'
            out=Path(argv[argv.index(flag)+1]);out.mkdir()
            if name=='fetch_increment.py':
                p.dump(out/'download_manifest.json',{'status':'COMPLETE'})
                p.dump(out/'inventory_increment.json',{'items':[],'complete':True})
            elif name=='parse_incremental_refresh.py':
                p.dump(out/'parse_manifest.json',{'status':'PARSED_WITH_REVIEW'})
                p.write_lines(out/'order_lines.jsonl',[{'line_id':'x','row':1,'fabric_assignment_sources':[]}])
            elif name=='build_increment_package.py':
                p.dump(out/'manifest.json',{'status':'LOCAL_PACKAGE_VALIDATED_NOT_IMPORTED','tables':{t:{'row_count':0} for t in p.TABLES},'append_preview':{'insert_count':0}})
            return {'returncode':0}
        r=p.run(self.config,self.root/'out',stage='PREPARE',execute_read=True,discovery_run=prior,runner=fake)
        self.assertEqual(calls,['fetch_increment.py','parse_incremental_refresh.py','order_index.py','build_increment_package.py'])
        self.assertEqual(r['status'],'PREPARED_NOT_PUBLISHED');self.assertEqual(r['database_operations'],0)
        self.assertEqual(r['manual_rows_created'],0);self.assertFalse(r['cursor_advanced'])
        self.assertFalse((self.root/'out/baseline_snapshot.json').exists())
        self.assertEqual(r['baseline_receipt_status'],'READ_ONLY_SNAPSHOT_VERIFIED')
        command=r['commands'][-1]['argv'];self.assertEqual(command[command.index('--production-before')+1],self.cfg['sources']['publication_receipt']['path'])
    def test_changed_discovery_plan_blocks_before_fetch(self):
        prior=self.prepare_prior();(prior/'actions.jsonl').write_text('changed')
        with self.assertRaisesRegex(p.PipelineError,'DISCOVERY_ARTIFACT_CHANGED'):
            p.run(self.config,self.root/'out',stage='PREPARE',execute_read=True,discovery_run=prior,runner=lambda *a:self.fail('fetch called'))

class MaterialTests(unittest.TestCase):
    def row(self,text='18#英伦庄园蓝',header='主布色号',**overrides):
        s={'text':text,'header':header,'scope_confirmed':True,'applies_to_rows':[6],'sheet':'sheet','cell':'D6','scope':'row'};s.update(overrides)
        return {'line_id':'source:sheet:6','token':'source','source_sha256':'hash','row':6,'fabric_header':'明确面料原名','fabric_assignment_sources':[s],'evidence_cells':[],'garment_color_raw':'成衣黑色'}
    def assignment(self,row):return [r for r in m.extract([row]) if r['candidate_kind']=='SOURCE_ASSIGNMENT'][0]
    def test_literal_color_and_component_retained_no_automatic_mapping(self):
        r=self.assignment(self.row());self.assertEqual(r['physical_color_number_raw'],'18#');self.assertEqual(r['physical_color_name_raw'],'英伦庄园蓝');self.assertEqual(r['part_raw'],'主布');self.assertFalse(r['human_confirmation']);self.assertIsNone(r['lingxing_candidate'])
    def test_image_and_scope_unknown_not_interpreted(self):
        for row in (self.row('=DISPIMG("x",1)'),self.row(scope_confirmed=False)):
            self.assertIsNone(self.assignment(row)['physical_color_number_raw'])
    def test_multiple_color_numbers_conflict_not_first(self):
        r=self.assignment(self.row('1#黑 2#白'));self.assertIsNone(r['physical_color_number_raw']);self.assertIn('MULTIPLE_COLOR_NUMBERS_CONFLICT',r['reasons'])
    def test_garment_color_never_substitutes_physical_color(self):
        r=self.assignment(self.row('无色号'));self.assertIsNone(r['physical_color_name_raw'])
    def test_header_context_not_component_assignment(self):
        h=m.extract([self.row()])[0];self.assertEqual(h['material_name_raw'],'明确面料原名');self.assertEqual(h['scope'],'HEADER_CONTEXT_ONLY');self.assertIsNone(h['part_raw'])
    def test_same_part_conflicting_cells_kept(self):
        row=self.row();row['fabric_assignment_sources'].append(dict(row['fabric_assignment_sources'][0],text='19#蓝',cell='E6'))
        r=[v for v in m.extract([row]) if v['candidate_kind']=='SOURCE_ASSIGNMENT'];self.assertEqual(len(r),2);self.assertTrue(all(v['status']=='CONFLICT_REQUIRES_REVIEW' for v in r))
    def test_duplicate_raw_line_rejected(self):
        row=self.row()
        with self.assertRaisesRegex(ValueError,'DUPLICATE'):m.extract([row,row])

if __name__=='__main__':unittest.main()
