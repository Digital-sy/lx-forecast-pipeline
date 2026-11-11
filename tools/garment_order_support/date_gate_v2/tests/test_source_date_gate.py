import sys,unittest,copy,json,hashlib
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import source_date_gate as g
import source_pipeline_v2 as p
import test_source_pipeline_v2 as fixtures

def row(raw='20260916',stored='2026-10-21',flag=False):
 return {'line_id':'synthetic:1:4:6','row':6,'filename_date':'2026-10-21','order_date':stored,'order_date_conflict':flag,
  'body_order_dates':[{'raw':raw,'value':None,'source':'inline_label','cell':'L2'}],'fabric_assignment_sources':[]}
class DateGateTests(unittest.TestCase):
 def test_compact_conflict_blocks(self):self.assertEqual(g.inspect_rows([row()])['requires_reparse_rows'],1)
 def test_honest_conflict_kept(self):self.assertEqual(g.inspect_rows([row(stored=None,flag=True)])['requires_reparse_rows'],0)
 def test_unknown_not_filled(self):
  x=row(raw='待定',stored=None);before=copy.deepcopy(x);self.assertEqual(g.inspect_rows([x])['requires_reparse_rows'],0);self.assertEqual(x,before)
 def test_unknown_body_does_not_certify_filename(self):self.assertEqual(g.inspect_rows([row(raw='待定')])['requires_reparse_rows'],1)
 def test_invalid_calendar_rejected(self):self.assertEqual(g.inspect_rows([row(raw='20260230')])['requires_reparse_rows'],1)
 def test_existing_serial_date_not_lost(self):
  x=row(raw='46316');x['body_order_dates'][0]['value']='2026-10-21';self.assertEqual(g.inspect_rows([x])['requires_reparse_rows'],0)
 def test_same_dates_ok_not_effectiveness(self):self.assertEqual(g.inspect_rows([row(raw='20261021')])['requires_reparse_rows'],0)
 def test_missing_body_does_not_invent(self):
  x=row();x['body_order_dates']=[];a=g.inspect_rows([x]);self.assertEqual(a['rows'][0]['known_body_dates'],[])
 def test_duplicate_rows_block(self):
  with self.assertRaises(ValueError):g.inspect_rows([row(),row()])
 def test_actual_orchestration_stops_before_index_or_package(self):
  f=fixtures.PipelineTests(methodName='test_check_only_never_dispatches');f.setUp()
  try:
   prior=f.prepare_prior();calls=[]
   def fake(argv,cwd,timeout):
    name=Path(argv[2]).name;calls.append(name);out=Path(argv[argv.index('--output-dir')+1]);out.mkdir()
    if name=='fetch_increment.py':p.dump(out/'download_manifest.json',{'status':'COMPLETE'});p.dump(out/'inventory_increment.json',{'items':[],'complete':True})
    elif name=='parse_incremental_refresh.py':p.dump(out/'parse_manifest.json',{'status':'PARSED'});p.write_lines(out/'order_lines.jsonl',[row()])
    else:self.fail('Index/package called after date conflict')
    return {'returncode':0}
   with self.assertRaisesRegex(p.PipelineError,'DATE_EVIDENCE_REPARSE_REQUIRED'):p.run(f.config,f.root/'out',stage='PREPARE',execute_read=True,discovery_run=prior,runner=fake)
   self.assertEqual(calls,['fetch_increment.py','parse_incremental_refresh.py']);self.assertFalse((f.root/'out/package').exists())
   self.assertEqual(p.read(f.root/'out/date_evidence_gate.json')['requires_reparse_rows'],1)
  finally:f.tearDown()
if __name__=='__main__':unittest.main()
