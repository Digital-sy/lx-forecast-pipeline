"""Synthetic contract tests for the offline quantity refresh; no external clients."""
import copy
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import refresh_fabric14_demand as m


def source(id=1,shop='S',spu='P',system='A2023',code='BK',nov=2,dec=3,basis='H48'):
 return {'id':id,'店铺':shop,'SPU':spu,'颜色体系':system,'颜色缩写':code,'26年11月建议下单':nov,'26年12月建议下单':dec,'新品数量口径':basis,'预测模型':'M','更新时间':'2026-11-09 09:02:28'}
def component(s,mat='MAT',mid='M1',resolved=True):
 return {'demand_id':m.demand_id(m.identity(s)),'material_id':mid,'review_id':'R1','shop':s['店铺'],'spu':s['SPU'],'system':s['颜色体系'],'color_code':s['颜色缩写'],
  'nov_qty':s['26年11月建议下单'],'dec_qty':s['26年12月建议下单'],'total_qty':s['26年11月建议下单']+s['26年12月建议下单'],
  'current_material':mat,'physical_color_number':'1#' if resolved else '', 'physical_color_name_raw':'黑色' if resolved else '',
  'physical_color_resolved':resolved,'unique_sku_resolved':resolved,'aggregate_status':'RESOLVED' if resolved else 'PENDING',
  'physical_mapping_origin':'ARCHIVED','quantity_source_status':'CURRENT_SOURCE_KEY_PRESENT','confirmation_id':''}
def manual(r,shops=None):
 return {'confirmation_id':'C1','scope_json':{'review_id':r['review_id'],'shops':shops or [r['shop']],'spu':r['spu'],'source_system':r['system'],'source_code':r['color_code'],'months':list(m.MONTHS)}}

class RefreshTests(unittest.TestCase):
 def test_numeric_id_is_not_business_key(self):
  a,b=source(),source(id=999,nov=7,dec=9);c=component(a)
  rows,_=m.refresh_components([c],m.source_index([a]),m.source_index([b]),{})
  self.assertEqual(rows[0]['total_qty'],16);self.assertEqual(rows[0]['demand_id'],c['demand_id']);self.assertEqual(c['total_qty'],5)
 def test_duplicate_business_key_rejected(self):
  with self.assertRaises(m.RefreshError):m.source_index([source(),source(id=2)])
 def test_duplicate_trace_id_rejected(self):
  with self.assertRaises(m.RefreshError):m.source_index([source(),source(code='WH')])
 def test_unknown_quantity_not_zero(self):
  r=next(iter(m.source_index([source(nov=None)]).values()));self.assertIsNone(r['nov_qty']);self.assertIsNone(r['total_qty'])
 def test_invalid_quantities_rejected(self):
  for value in (-1,1.5,True,'NaN','Infinity'):
   with self.subTest(value=value),self.assertRaises(m.RefreshError):m.qty(value)
 def test_missing_source_key_remains_unknown(self):
  a=source();c=component(a);r,missing=m.refresh_components([c],m.source_index([a]),{},{});
  self.assertEqual(len(missing),1);self.assertIsNone(r[0]['total_qty']);self.assertEqual(r[0]['quantity_source_status'],'SOURCE_KEY_ABSENT');self.assertEqual(r[0]['physical_color_number'],'1#')
 def test_zero_to_positive_does_not_inherit_other_color(self):
  a=source();z=source(id=2,code='WH',nov=0,dec=0);n=source(id=20,code='WH',nov=3,dec=4)
  _,_,audit,unknown,stats=m.audit_sources([a,z],[source(id=10),n],[component(a)],[{'demand_id':m.demand_id(m.identity(a)),'material_name':'MAT'}])
  self.assertEqual(len(unknown),1);self.assertTrue(unknown[0]['same_shop_spu_has_14_material_plan']);self.assertEqual(unknown[0]['material_scope_status'],'UNKNOWN_NO_EXACT_COLOR_MATERIAL_PLAN');self.assertTrue(stats['partition_conservation_pass'])
 def test_source_absence_no_fabric_outside_assertion(self):
  a=source(nov=0,dec=0);b=source(id=10,nov=3)
  *_,unknown,stats=m.audit_sources([a],[b],[],[])
  self.assertEqual(unknown[0]['material_scope_status'],'UNKNOWN_NO_EXACT_COLOR_MATERIAL_PLAN');self.assertIn('UNKNOWN',unknown[0]['unresolved_plan_reason'])
 def test_latest_manual_scope_enforced(self):
  a=source();c=component(a);c['confirmation_id']='C1';d=manual(c,shops=['OTHER'])
  with self.assertRaises(m.RefreshError):m.refresh_components([c],m.source_index([a]),m.source_index([a]),{'R1':d})
 def test_manual_effective_version_enforced(self):
  a=source();c=component(a);c['confirmation_id']='C0';d=manual(c)
  with self.assertRaises(m.RefreshError):m.refresh_components([c],m.source_index([a]),m.source_index([a]),{'R1':d})
 def test_unresolved_source_system_preserved(self):
  a=source(system='待定');c=component(a,resolved=False);r,_=m.refresh_components([c],m.source_index([a]),m.source_index([source(id=9,system='待定',nov=4)]),{})
  self.assertEqual(r[0]['system'],'待定');self.assertFalse(r[0]['unique_sku_resolved'])
 def test_duplicate_pool_quantity_counted_once(self):
  c=component(source());d=copy.deepcopy(c);d['material_id']='M2'
  x=m.aggregate_components([c,d],['MAT']);self.assertEqual(x['all_scope']['total_qty'],5);self.assertEqual(sum(r['garment_order_qty'] for r in x['atoms']),5)
 def test_conflicting_physical_color_blocked(self):
  c=component(source());d=copy.deepcopy(c);d['material_id']='M2';d['physical_color_number']='2#'
  x=m.aggregate_components([c,d],['MAT']);self.assertEqual(x['atoms'],[]);self.assertEqual(x['aggregation_blocked']['total_qty'],5);self.assertEqual(x['pending_scope']['total_qty'],5)
 def test_zero_keeps_original_objects(self):
  a=source();c=component(a);r,_=m.refresh_components([c],m.source_index([a]),m.source_index([source(id=9,nov=0,dec=0)]),{})
  x=m.aggregate_components(r,['MAT']);self.assertEqual(x['all_scope']['unique_demand_rows'],1);self.assertEqual(x['all_scope']['total_qty'],0);self.assertEqual(len(x['atoms']),2)
 def test_new_material_not_added_to_fixed_scope(self):
  with self.assertRaises(m.RefreshError):m.aggregate_components([component(source(),mat='NEW')],['MAT'])
 def test_global_demand_dedup_across_materials(self):
  c=component(source());d=copy.deepcopy(c);d['current_material']='MAT2';d['material_id']='M2'
  x=m.aggregate_components([c,d],['MAT','MAT2']);self.assertEqual(x['all_scope']['total_qty'],5);self.assertEqual(sum(r['两月参与件数'] for r in x['material_summary']),10)
 def test_changed_basis_audited_without_qty_change(self):
  a=source(basis='H48');b=source(id=9,basis='H60');_,_,audit,_,stats=m.audit_sources([a],[b],[],[])
  self.assertFalse(audit[0]['quantity_changed']);self.assertTrue(audit[0]['quantity_basis_changed']);self.assertEqual(stats['quantity_basis_changed_keys'],1)
 def test_broken_confirmation_chain_rejected(self):
  receipt={'status':'COMMITTED_VERIFIED','final_readback':{'state':'MATCH'},'tables':{'ods_manual_sm_garment_color_confirmation':[
   {'review_id':'R','confirmation_version':1,'confirmation_id':'C1'}, {'review_id':'R','confirmation_version':2,'confirmation_id':'C2','supersedes_confirmation_id':'OTHER'}]}}
  with self.assertRaises(m.RefreshError):m.active_confirmations(receipt)
 def test_unknown_totals_retained_in_source_conservation(self):
  *_,stats=m.audit_sources([source()],[source(id=9,nov=None)],[],[])
  self.assertIsNone(stats['current_totals']['nov_qty']['value']);self.assertEqual(stats['current_totals']['nov_qty']['unknown_records'],1);self.assertTrue(stats['partition_conservation_pass'])
 def test_baseline_quantity_mismatch_rejected(self):
  a=source();c=component(a);c['nov_qty']=99
  with self.assertRaises(m.RefreshError):m.refresh_components([c],m.source_index([a]),m.source_index([a]),{})
 def test_missing_color_cannot_be_guessed(self):
  with self.assertRaises(m.RefreshError):m.source_index([source(code='')])
 def test_code_only_evidence_remains_pending(self):
  c=component(source(system='待定'),resolved=False);c['business_physical_confirmation_flag']='True'
  x=m.aggregate_components([c],['MAT']);self.assertEqual(x['atoms'],[]);self.assertEqual(x['pending_scope']['total_qty'],5)

if __name__=='__main__':unittest.main(verbosity=2)
