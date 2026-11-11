"""Synthetic checks only; no archived business rows or credentials."""
import unittest,copy
from candidate_audit import match,rawtext,color_norm
class CandidateMatchTests(unittest.TestCase):
 def setUp(self):
  self.q={'spu':'STYLE_A','color_code':'BK','garment_color_name':'黑色'}
  self.r={'spu':'STYLE_A','garment_color_code':'BK','garment_color_name':'黑','source_line_id':'line-1','garment_color_system':None}
 def test_exact_spu_required(self):
  r=dict(self.r,spu='STYLE_AB');self.assertIsNone(match(self.q,r,set()))
 def test_same_name_optional_trailing_se(self):self.assertEqual(match(self.q,self.r,set()),'SAME_CODE_AND_NAME')
 def test_code_conflict_not_silently_resolved(self):
  r=dict(self.r,garment_color_name='粉色');self.assertEqual(match(self.q,r,set()),'SAME_CODE_NAME_NOT_PROVEN')
 def test_name_match_with_different_code_stays_unproven(self):
  r=dict(self.r,garment_color_code='OTHER');self.assertEqual(match(self.q,r,set()),'SAME_NAME_CODE_NOT_PROVEN')
 def test_missing_color_not_zero(self):
  r=dict(self.r,garment_color_code=None,garment_color_name=None);self.assertIsNone(match(self.q,r,set()))
 def test_link_reference_not_business_approval(self):self.assertEqual(match(self.q,self.r,{'line-1'}),'PRIOR_REVIEW_LINE_REFERENCE')
 def test_unrelated_link_not_matching_different_spu(self):self.assertIsNone(match(self.q,dict(self.r,spu='OTHER'),{'line-1'}))
 def test_system_not_mutated(self):
  old=copy.deepcopy(self.r);match(self.q,self.r,set());self.assertEqual(self.r,old);self.assertIsNone(self.r['garment_color_system'])
 def test_quantity_has_no_role(self):
  self.assertEqual(match(self.q,dict(self.r,quantity=0),set()),match(self.q,dict(self.r,quantity=999999),set()))
 def test_no_other_synonym_automatic_merge(self):
  self.assertNotEqual(color_norm('亮白'),color_norm('白色'))
class SourceTextTests(unittest.TestCase):
 def source(self,**kw):
  a={'source_kind':'fabric_column','scope_confirmed':True,'sheet':'S','cell':'D6','header':'主布色号','text':'7#','scope':'row'};a.update(kw);return a
 def test_readable_explicit_field(self):
  t,i=rawtext({'field_sources_json':{'fabric_assignment_sources':[self.source()]}});self.assertEqual(len(t),1);self.assertEqual(i,[])
 def test_general_note_not_color_source(self):
  t,i=rawtext({'field_sources_json':{'fabric_assignment_sources':[self.source(source_kind='row_note')]}});self.assertEqual(t,[])
 def test_unknown_scope_excluded(self):
  t,i=rawtext({'field_sources_json':{'fabric_assignment_sources':[self.source(scope_confirmed=False)]}});self.assertEqual(t,[])
 def test_image_no_color_number_inference(self):
  t,i=rawtext({'field_sources_json':{'fabric_assignment_sources':[self.source(text='=DISPIMG("ID",1)')]}});self.assertEqual(t,[]);self.assertEqual(len(i),1)
 def test_json_encoded_db_field(self):
  import json
  obj={'fabric_assignment_sources':[self.source()]};self.assertEqual(rawtext({'field_sources_json':obj}),rawtext({'field_sources_json':json.dumps(obj)}))
 def test_missing_source_preserved(self):self.assertEqual(rawtext({'field_sources_json':None}),([],[]))
if __name__=='__main__':unittest.main(verbosity=2)
