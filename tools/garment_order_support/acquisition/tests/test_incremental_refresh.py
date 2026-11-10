"""Synthetic offline tests; no real parser, services, or business files."""
import importlib.util,json,os,unittest,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def module(name):
 s=importlib.util.spec_from_file_location(name,ROOT/'scripts'/(name+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
P=module('plan_incremental_refresh');W=module('parse_incremental_refresh')
class PlanTests(unittest.TestCase):
 def setUp(self):
  self.d={'source_id':'a','version_id':'v','content_sha256':'a'*64,'file_name':'test.xlsx','parse_status':'ok','file_verification':'HASH_MATCH'}
  self.r={'evidence_id':'r','version_id':'v','source_id':'a','raw':{'line_id':'l'}}
  self.i={'documents':[self.d],'raw':[self.r],'materials':[],'manual':[]}
  self.s={'status':'SNAPSHOT_VERIFIED','tables':{t:[] for t in [P.T_DOC,P.T_VER,P.T_EXT,P.T_RAW,P.T_MAT,P.T_MAN,P.T_REF]}}
  self.s['tables'][P.T_DOC]=[{'document_id':'d','source_id':'a'}]
  self.s['tables'][P.T_VER]=[{'document_id':'d','version_id':'v','content_sha256':'a'*64}]
  self.s['tables'][P.T_RAW]=[{'version_id':'v','evidence_id':'db-r','source_line_id':'l'}]
 def test_no_new_live_inventory_claim(self):
  q,s=P.plan(self.i,self.s);self.assertFalse(s['current_remote_validated']);self.assertEqual(len(q['append_gap']),0)
 def test_material_missing_queued_not_confirmed(self):
  q,_=P.plan(self.i,self.s);self.assertEqual(q['evidence_review'][0]['reason'],'MATERIAL_INTERPRETATION_NOT_PRESENT')
 def test_no_file_does_not_invent_version(self):
  self.d.update(version_id=None,content_sha256=None);q,_=P.plan(self.i,self.s);self.assertFalse(q['fetch'][0]['automatically_fetchable']);self.assertIsNone(q['fetch'][0]['version_id'])
 def test_same_version_observations_do_not_duplicate(self):
  self.i['documents'].append(dict(self.d));_,s=P.plan(self.i,self.s);self.assertEqual(s['source_versions_indexed'],1)
 def test_distinct_parser_observations_not_duplicate_missing(self):
  self.i['raw'].append(dict(self.r,extra='new status context'));q,s=P.plan(self.i,self.s);self.assertEqual(s['raw_index_observations'],2);self.assertEqual(s['unique_legacy_raw_ids'],1);self.assertEqual(len(q['append_gap']),0)
 def test_version_hash_conflict_fails(self):
  self.d['content_sha256']='b'*64
  with self.assertRaisesRegex(ValueError,'version/hash'):P.plan(self.i,self.s)
 def test_missing_manual_ref_not_business_clear(self):
  self.i['manual']=[{'review_id':'x','confirmation_reference_status':'REVIEW_REQUIRED','manual_resolution':{'lingxing_sku_confirmed':True}}]
  q,_=P.plan(self.i,self.s);self.assertTrue(q['evidence_review'][-1]['business_flags_preserved']);self.assertTrue(self.i['manual'][0]['manual_resolution']['lingxing_sku_confirmed'])
 def test_content_change_queues_fetch(self):
  a={'source_id':'a','state':'CONTENT_CHANGED','action':'FETCH_REQUIRED','resource_type':'file','current_content_sha256':'b'*64}
  q,_=P.plan(self.i,self.s,[a]);self.assertEqual(len(q['content_change']),1);self.assertEqual(len(q['fetch']),1)
 def test_already_archived_hash_not_refetched(self):
  a={'source_id':'a','state':'METADATA_CHANGED','action':'FETCH_REQUIRED','resource_type':'file','current_content_sha256':'a'*64}
  q,_=P.plan(self.i,self.s,[a]);self.assertEqual(len(q['fetch']),0);self.assertEqual(len(q['remote_verification']),1)
 def test_snapshot_counts_only_rejected(self):
  with self.assertRaisesRegex(ValueError,'complete tables'):P.plan(self.i,{'status':'COMMITTED_VERIFIED','counts':{}})
class WrapperTests(unittest.TestCase):
 def setUp(self):
  self.root=Path(os.environ['GARMENT_INCREMENT_TEST_TMP'])/'incremental_refresh_tests'/uuid.uuid4().hex;self.root.mkdir(parents=True)
  self.source=self.root/'source.bin';self.source.write_bytes(b'test-only')
  self.inv=self.root/'inventory.json';self.down=self.root/'downloads.jsonl';self.plan=self.root/'plan.jsonl'
  self.inv.write_text(json.dumps({'items':[{'token':'a','type':'file','name':'test.xlsx'}]}),encoding='utf-8')
  self.row={'token':'a','status':'ok','local_path':str(self.source),'sha256':W.sha256(self.source),'bytes':self.source.stat().st_size}
  self.down.write_text(json.dumps(self.row)+'\n',encoding='utf-8');self.plan.write_text(json.dumps({'token':'a','resource_type':'file','state':'CONTENT_CHANGED'})+'\n',encoding='utf-8')
  helper=self.root/'legacy'/'scripts'/'extract_fields.py';helper.parent.mkdir(parents=True);helper.write_text('# synthetic helper only\n',encoding='utf-8')
  parser=self.root/'synthetic_parser.py'
  parser.write_text('from pathlib import Path\nimport argparse,json,hashlib\nOLD=Path('+repr(str(self.root/'legacy'))+')\np=argparse.ArgumentParser()\nfor n in ("inventory","downloads","output-lines","output-audit"):p.add_argument("--"+n,required=True)\na=p.parse_args()\nr=json.loads(Path(a.downloads).read_text())\nrow={"token":"a","source_sha256":r["sha256"],"line_id":"l","quantity":None}\nPath(a.output_lines).write_text(json.dumps(row)+"\\n")\nPath(a.output_audit).write_text(json.dumps({"token":"a","source_sha256":r["sha256"],"status":"ok"})+"\\n")\n',encoding='utf-8')
  self.deps=self.root/'dependencies.json';self.depobj={'parser_path':str(parser),'parser_sha256':W.sha256(parser),'helper_path':str(helper),'helper_sha256':W.sha256(helper)};self.deps.write_text(json.dumps(self.depobj),encoding='utf-8')
 def argv(self,out):return ['--task-root',str(self.root),'--inventory',str(self.inv),'--downloads',str(self.down),'--plan',str(self.plan),'--dependency-manifest',str(self.deps),'--output-dir',str(out)]
 def test_content_changed_is_accepted(self):self.assertEqual(len(W.validate_inputs(self.inv,self.down,self.plan)[0]),1)
 def test_default_preflight_no_parser(self):
  out=self.root/'pre';self.assertEqual(W.main(self.argv(out)),0);self.assertEqual(json.loads((out/'parse_manifest.json').read_text())['parser_executions'],0);self.assertFalse((out/'order_lines.jsonl').exists())
 def test_actual_wrapper_calls_only_explicit_synthetic_parser(self):
  out=self.root/'parsed';self.assertEqual(W.main(self.argv(out)+['--execute-local-parse']),0);r=json.loads((out/'order_lines.jsonl').read_text());self.assertIsNone(r['quantity']);self.assertEqual(r['source_sha256'],self.row['sha256'])
 def test_wrong_dependency_hash_stops(self):
  self.depobj['helper_sha256']='0'*64;self.deps.write_text(json.dumps(self.depobj))
  with self.assertRaisesRegex(ValueError,'dependency hash'):W.main(self.argv(self.root/'out'))
 def test_wrong_helper_binding_stops(self):
  p=self.root/'other.py';p.write_text('# test');self.depobj.update(helper_path=str(p),helper_sha256=W.sha256(p));self.deps.write_text(json.dumps(self.depobj))
  with self.assertRaisesRegex(ValueError,'helper binding'):W.main(self.argv(self.root/'out'))
 def test_original_bytes_changed_stops(self):
  self.source.write_bytes(b'changed')
  with self.assertRaisesRegex(ValueError,'hash mismatch'):W.main(self.argv(self.root/'out'))
 def test_whitelist_not_expanded(self):
  self.plan.write_text(self.plan.read_text()+json.dumps({'token':'b','resource_type':'file','state':'NEW'})+'\n')
  with self.assertRaisesRegex(ValueError,'scope differs'):W.main(self.argv(self.root/'out'))
 def test_duplicate_success_stops(self):
  self.down.write_text((json.dumps(self.row)+'\n')*2)
  with self.assertRaisesRegex(ValueError,'Multiple successful'):W.main(self.argv(self.root/'out'))
 def test_no_overwrite(self):
  out=self.root/'exists';out.mkdir()
  with self.assertRaises(FileExistsError):W.main(self.argv(out))
 def test_outside_output_stops(self):
  with self.assertRaisesRegex(ValueError,'child'):W.main(self.argv(self.root.parent/'outside'))
if __name__=='__main__':unittest.main(verbosity=2)
