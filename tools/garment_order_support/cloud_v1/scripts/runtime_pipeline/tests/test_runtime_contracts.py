"""Only affected runtime/bot/snapshot/reparse behavior; all transports are synthetic."""
import copy
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch
import test_pipeline_regression as regression
from test_pipeline_regression import p,receipt_fixture,core,app
import snapshot_contract as contract
import discover_order_files as discover
import fetch_increment as fetch
import build_increment_package as builder

class SnapshotContractTests(unittest.TestCase):
    def test_actual_readonly_shape_passes_without_commit_receipts(self):
        value=receipt_fixture();before=copy.deepcopy(value)
        norm=contract.validate_snapshot(value)
        self.assertEqual(sum(map(len,norm.values())),0);self.assertEqual(value,before)
        self.assertNotIn('loader_receipt',value);self.assertNotIn('final_readback',value)
        existing,norm2=builder.business_existing(value)
        self.assertEqual(norm,norm2)
    def test_wrong_identity_blocked(self):
        for key,value in (('schema','other'),('server_uuid','other')):
            row=receipt_fixture();row[key]=value
            with self.assertRaisesRegex(ValueError,'BASELINE_TARGET_MISMATCH'):contract.validate_snapshot(row)
    def test_row_count_and_total_changes_blocked(self):
        row=receipt_fixture();row['row_counts'][core.TABLES[0]]=1
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_ROW_COUNT'):contract.validate_snapshot(row)
        row=receipt_fixture();row['total_rows']=1
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_TOTAL_ROWS'):contract.validate_snapshot(row)
    def test_metadata_drift_blocked_even_with_empty_tables(self):
        row=receipt_fixture();row['metadata'][core.TABLES[0]][0]['COLUMN_TYPE']='varchar(64)'
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_METADATA_STRUCTURE'):contract.validate_snapshot(row)
    def test_datatype_drift_blocked(self):
        row=receipt_fixture();row['metadata'][core.TABLES[0]][0]['DATA_TYPE']='text'
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_METADATA_TYPE'):contract.validate_snapshot(row)
    def test_table_digest_drift_blocked(self):
        row=receipt_fixture();row['canonical_table_sha256'][core.TABLES[0]]='bad'
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_TABLE_CONTENT_HASH'):contract.validate_snapshot(row)
    def test_content_drift_blocked_after_count_is_updated(self):
        row=receipt_fixture();t=core.TABLES[0]
        row['tables'][t]=[{core.PK[t]:'a'*64,'create_time':'2026-11-11 00:00:00','update_time':'2026-11-11 00:00:00'}]
        row['row_counts'][t]=1;row['total_rows']=1
        with self.assertRaisesRegex(ValueError,'SNAPSHOT_CONTENT_HASH'):contract.validate_snapshot(row)
    def test_committed_status_requires_readback(self):
        row=receipt_fixture();row['status']='COMMITTED_VERIFIED'
        with self.assertRaisesRegex(ValueError,'INDEPENDENT_READBACK_NOT_MATCHED'):contract.validate_snapshot(row)
    def test_readonly_cannot_claim_writes(self):
        row=receipt_fixture();row['database_writes']=1
        with self.assertRaisesRegex(ValueError,'READ_ONLY_RECEIPT_CONTRADICTION'):contract.validate_snapshot(row)

class BotTests(unittest.TestCase):
    def test_discovery_request_and_envelope_are_explicit_bot(self):
        args=discover.make_argv(['python','shim.py'],'Folder',None,200,'bot')
        self.assertEqual(args[args.index('--as')+1],'bot')
        data={'ok':True,'identity':'bot','data':{'files':[],'has_more':False,'next_page_token':None}}
        proc=subprocess.CompletedProcess(args,0,json.dumps(data),'')
        self.assertEqual(discover.parse_page(proc,'Folder','',identity='bot')[0],[])
        data['identity']='user';proc.stdout=json.dumps(data)
        with self.assertRaisesRegex(discover.DiscoveryError,'CLI_IDENTITY_MISMATCH'):discover.parse_page(proc,'Folder','')
    def test_fetch_transport_rejects_identity_substitution(self):
        transport=fetch.CliTransport('python','shim.py','.',identity='bot')
        proc=subprocess.CompletedProcess([],0,json.dumps({'ok':True,'identity':'bot','data':{'token':'x'}}),'')
        with patch.object(fetch.subprocess,'run',return_value=proc) as call:
            self.assertEqual(transport(['drive','+inspect','--as','bot']),{'token':'x'})
            self.assertEqual(call.call_args[0][0][-2:],['--as','bot'])
        proc.stdout=json.dumps({'ok':True,'identity':'user','data':{}})
        with patch.object(fetch.subprocess,'run',return_value=proc):
            with self.assertRaisesRegex(fetch.FetchError,'CLI_IDENTITY_OR_SHAPE_INVALID'):transport(['drive','+inspect','--as','bot'])
    def test_user_identity_cannot_be_selected(self):
        with self.assertRaisesRegex(fetch.FetchError,'BOT_IDENTITY_REQUIRED'):fetch.CliTransport('python','shim.py','.',identity='user')

class RuntimeConfigTests(unittest.TestCase):
    setUp=regression.PipelineTests.setUp
    tearDown=regression.PipelineTests.tearDown
    save_config=regression.PipelineTests.save_config
    prepare_prior=regression.PipelineTests.prepare_prior
    def test_config_relative_paths_resolve_against_config_not_cwd(self):
        for group in ('tools','sources'):
            for spec in self.cfg[group].values():spec['path']=str(Path(spec['path']).relative_to(self.root))
        for key in ('cli_executable','cli_script'):self.cfg['discovery'][key]=str(Path(self.cfg['discovery'][key]).relative_to(self.root))
        self.cfg['task_root']='.';self.save_config()
        result=p.run(self.config,self.root/'out',runner=lambda *a:self.fail('no dispatch'))
        self.assertEqual(result['status'],'CHECK_ONLY_READY')
    def add_queue(self):
        queue=self.root/'reparse_queue.json'
        p.dump(queue,{'schema_version':'garment_reparse_queue/1','items':[{'source_id':'ZsyExample','resource_type':'file','status':'PENDING','content_sha256':'a'*64,'previous_parser_version':'1.0','required_parser_version':'1.1.0-body-yyyymmdd','reason':'EXPLICIT_BODY_DATE_FIX'}]})
        self.cfg['reparse_queue']={'path':'reparse_queue.json','sha256':p.sha(queue)};self.save_config();return queue
    def test_known_reparse_never_becomes_no_increment(self):
        queue=self.add_queue();original=queue.read_bytes();prior=self.prepare_prior()
        p.write_lines(prior/'empty.jsonl',[]);manifest=p.read(prior/'pipeline_manifest.json');manifest['plan']=str(prior/'empty.jsonl');manifest['plan_sha256']=p.sha(prior/'empty.jsonl');(prior/'pipeline_manifest.json').write_text(json.dumps(manifest))
        result=p.run(self.config,self.root/'out',stage='PREPARE',execute_read=True,discovery_run=prior,runner=lambda *a:self.fail('reparse is not executed'))
        self.assertEqual(result['status'],'REPARSE_REQUIRED_NOT_PREPARED');self.assertEqual(result['pending_reparse_count'],1);self.assertFalse(result['reparse_queue_modified']);self.assertEqual(queue.read_bytes(),original)
    def test_queue_version_mismatch_blocks(self):
        queue=self.add_queue();q=p.read(queue);q['items'][0]['required_parser_version']='other';queue.write_text(json.dumps(q));self.cfg['reparse_queue']['sha256']=p.sha(queue);self.save_config()
        with self.assertRaisesRegex(p.PipelineError,'REPARSE_QUEUE_VERSION'):p.run(self.config,self.root/'out')
    def test_bot_is_required_by_pipeline(self):
        self.cfg['discovery']['identity']='user';self.save_config()
        with self.assertRaisesRegex(p.PipelineError,'BOT_IDENTITY_REQUIRED'):p.run(self.config,self.root/'out')
    def test_tool_chain_passes_identity_to_both_reads(self):
        calls=[]
        def fake(argv,cwd,timeout):
            name=Path(argv[2]).name;calls.append(argv)
            out=Path(argv[argv.index('--output-dir')+1]);out.mkdir()
            if name=='discover_order_files.py':p.dump(out/'inventory_current.json',{'status':'COMPLETE','complete':True,'root':'ApprovedFolder','items':[]})
            elif name=='plan_refresh.py':p.write_lines(out/'actions.jsonl',[])
            return {'returncode':0}
        p.run(self.config,self.root/'out',stage='DISCOVER',execute_read=True,runner=fake)
        self.assertEqual(calls[0][calls[0].index('--identity')+1],'bot')
        prior=self.prepare_prior();caught=[]
        def fail_fetch(argv,cwd,timeout):caught.append(argv);return {'returncode':2}
        with self.assertRaises(p.PipelineError):p.run(self.config,self.root/'prepare',stage='PREPARE',execute_read=True,discovery_run=prior,runner=fail_fetch)
        self.assertEqual(caught[0][caught[0].index('--identity')+1],'bot')


class FetchFailStopTests(unittest.TestCase):
    def rows(self):
        return [{'source_id':token,'token':token,'resource_type':'file','state':'NEW','action':'FETCH_REQUIRED','current_metadata':{'type':'file','name':token+'.xlsx','url':'https://example.feishu.cn/file/'+token}} for token in ('SourceA','SourceB')]
    def run_failed(self,error):
        import tempfile
        root=Path(__file__).resolve().parents[1]/'temp';root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='fetch_stop_',dir=root) as tmp:
            base=Path(tmp);calls=[]
            def fail(args):calls.append(args);raise error
            result=fetch.execute(self.rows(),base/'out',base,fail,execute_read=True,identity='bot')
            rows=p.lines(base/'out/downloads.jsonl')
            self.assertEqual(len(calls),1);self.assertEqual(result['failed'],1);self.assertEqual(result['not_run'],1)
            self.assertEqual(rows[1]['error']['reason'],'STOPPED_AFTER_SOURCE_FAILURE')
            return rows[0]
    def test_non_auth_api_failure_stops_later_sources(self):
        row=self.run_failed(fetch.FetchError('CLI_READ_FAILED',auth=False,code=999,error_type='api'))
        self.assertEqual(row['error']['type'],'api');self.assertEqual(row['error']['code'],999)
    def test_auth_error_keeps_actual_type_and_stops(self):
        row=self.run_failed(fetch.FetchError('CLI_READ_FAILED',auth=True,code=403,error_type='permission'))
        self.assertEqual(row['error']['type'],'permission')
    def test_local_processing_error_also_stops_safely(self):
        row=self.run_failed(ValueError('synthetic'))
        self.assertEqual(row['error']['reason'],'LOCAL_PROCESSING_ERROR')

if __name__=='__main__':unittest.main()
