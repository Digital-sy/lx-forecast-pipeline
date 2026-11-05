"""Synthetic tests for the pure physical-color reference resolver."""
from pathlib import Path
import copy
import importlib.util
import unittest

MODULE=Path(__file__).resolve().parents[1]/'jobs/feishu/fabric_lingxing_color_reference.py'
spec=importlib.util.spec_from_file_location('isolated_fabric_color_reference',MODULE)
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
build_aliases=module.build_aliases
resolve_reference=module.resolve_reference


def inventory(rid='r1',fabric='FAB-KNIT-JER-0017',old='WH',new='BW',**extras):
    row={'record_id':rid,'颜色缩写':old,'领星新颜色缩写':new,
         '面料编号-颜色缩写':fabric+'-'+new}
    row.update(extras)
    return row


def demand(fabric='FAB-KNIT-JER-0017',system='A2023',code='WH',**extras):
    row={'统计类型':'带颜色','面料编号':fabric,'颜色体系':system,'颜色缩写':code,
         '面料颜色编号':fabric+'-'+system+'-'+code,'中文颜色名称':'不应作为领星颜色'}
    row.update(extras)
    return row


class PhysicalColorTests(unittest.TestCase):
    def test_wh_maps_bw_then_exact_source_sku(self):
        aliases,issues=build_aliases([inventory()])
        out=resolve_reference(demand(),aliases,{'FAB-KNIT-JER-0017-BW':'2#亮白','FAB-KNIT-JER-0017-WH':'错误色'})
        self.assertEqual(out,{'value':'2#亮白','status':'MATCHED','source_sku':'FAB-KNIT-JER-0017-BW','source_record_id':'r1'})
        self.assertEqual(issues,[])
    def test_same_code_other_system_does_not_get_legacy_fallback(self):
        aliases,_=build_aliases([inventory()])
        out=resolve_reference(demand(system='B2024'),aliases,{'FAB-KNIT-JER-0017-WH':'错误色'})
        self.assertEqual(out['status'],'UNMAPPED_ALIAS')
        self.assertEqual(out['value'],'')
    def test_a_b_same_text_can_resolve_different_records(self):
        aliases,_=build_aliases([inventory(old='BK',new='BO'),inventory(rid='r2',old='XX',new='BK')])
        colors={'FAB-KNIT-JER-0017-BO':'1#黑','FAB-KNIT-JER-0017-BK':'9#别色'}
        self.assertEqual(resolve_reference(demand(code='BK'),aliases,colors)['value'],'1#黑')
        self.assertEqual(resolve_reference(demand(system='B2024',code='BK'),aliases,colors)['value'],'9#别色')
    def test_original_and_optimized_fabrics_are_separate(self):
        aliases,_=build_aliases([inventory(),inventory(rid='r2',fabric='FAB-KNIT-JER-0019',new='BW')])
        colors={'FAB-KNIT-JER-0017-BW':'2#亮白','FAB-KNIT-JER-0019-BW':'3#优化色'}
        self.assertEqual(resolve_reference(demand(),aliases,colors)['value'],'2#亮白')
        self.assertEqual(resolve_reference(demand(fabric='FAB-KNIT-JER-0019'),aliases,colors)['value'],'3#优化色')
    def test_zero_stock_still_maps(self):
        aliases,_=build_aliases([inventory(**{'库存成品数量（条）':0,'备货中数量（条）':0})])
        self.assertEqual(resolve_reference(demand(),aliases,{'FAB-KNIT-JER-0017-BW':'2#亮白'})['status'],'MATCHED')
    def test_two_records_same_alias_are_conflict_even_same_sku(self):
        aliases,issues=build_aliases([inventory(),inventory(rid='r2')])
        self.assertEqual(resolve_reference(demand(),aliases,{})['status'],'ALIAS_CONFLICT')
        self.assertNotIn(('FAB-KNIT-JER-0017','A2023','WH'),aliases)
        self.assertTrue(any(x['status']=='ALIAS_CONFLICT' for x in issues))
    def test_old_code_two_new_colors_is_conflict(self):
        aliases,_=build_aliases([inventory(),inventory(rid='r2',new='AB')])
        self.assertEqual(resolve_reference(demand(),aliases,{})['status'],'ALIAS_CONFLICT')
    def test_identical_repeated_record_does_not_duplicate_alias(self):
        aliases,issues=build_aliases([inventory(),inventory()])
        self.assertEqual(len(aliases),2)
        self.assertEqual(issues,[])
    def test_inconsistent_same_record_blocks_all_its_aliases(self):
        aliases,issues=build_aliases([inventory(),inventory(new='AB')])
        self.assertEqual(len(aliases),0)
        self.assertEqual(resolve_reference(demand(system='B2024',code='BW'),aliases,{})['status'],'ALIAS_CONFLICT')
    def test_total_never_resolves(self):
        aliases,_=build_aliases([inventory()])
        out=resolve_reference(demand(统计类型='总量'),aliases,{'FAB-KNIT-JER-0017-BW':'2#亮白'})
        self.assertEqual(out['status'],'TOTAL_ROW');self.assertEqual(out['value'],'')
    def test_unknown_system_is_blank(self):
        aliases,_=build_aliases([inventory()])
        self.assertEqual(resolve_reference(demand(system='待定'),aliases,{})['status'],'UNKNOWN_COLOR_SYSTEM')
    def test_unknown_stat_type_is_blank(self):
        self.assertEqual(resolve_reference(demand(统计类型='未知'),{}, {})['status'],'UNKNOWN_STAT_TYPE')
    def test_missing_fabric_is_not_reconstructed_from_other_fields(self):
        self.assertEqual(resolve_reference(demand(面料编号=''),{}, {})['status'],'MISSING_OR_INVALID_FABRIC_CODE')
    def test_compound_old_code_not_split_but_new_alias_kept(self):
        aliases,issues=build_aliases([inventory(old='WH,WH')])
        self.assertEqual(set(aliases),{('FAB-KNIT-JER-0017','B2024','BW')})
        self.assertEqual(resolve_reference(demand(),aliases,{})['status'],'UNMAPPED_ALIAS')
        self.assertTrue(any(x['status']=='OLD_CODE_NOT_SINGLE' for x in issues))
    def test_invalid_new_code_cannot_establish_fabric_prefix(self):
        for code in ['取消','BW,WH','BW/WH','BW WH']:
            aliases,issues=build_aliases([inventory(new=code)])
            self.assertEqual(len(aliases),0);self.assertTrue(issues)
    def test_formula_must_match_exact_new_code_suffix(self):
        aliases,issues=build_aliases([inventory(**{'面料编号-颜色缩写':'FAB-KNIT-JER-0017-WH'})])
        self.assertEqual(len(aliases),0)
        self.assertEqual(issues[0]['status'],'SKU_NEW_CODE_SUFFIX_MISMATCH')
    def test_complete_fabric_identifier_kept(self):
        aliases,_=build_aliases([inventory(fabric='FAB-KNIT-FLEECE-0018-V1')])
        self.assertIn(('FAB-KNIT-FLEECE-0018-V1','A2023','WH'),aliases)
        self.assertNotIn(('FAB','A2023','WH'),aliases)
    def test_real_api_shaped_text_and_flat_normalize_equally(self):
        flat=inventory()
        fields={k:v for k,v in flat.items() if k!='record_id'}
        fields['面料编号-颜色缩写']=[{'text':fields['面料编号-颜色缩写'],'type':'text'}]
        a,ai=build_aliases([flat]);b,bi=build_aliases([{'record_id':'r1','fields':fields}])
        self.assertEqual(a,b);self.assertEqual(ai,bi)
    def test_declared_text_fragments_concatenate(self):
        row=inventory();row['面料编号-颜色缩写']=[{'text':'FAB-KNIT-','type':'text'},{'text':'JER-0017-BW','type':'text'}]
        aliases,_=build_aliases([row])
        self.assertIn(('FAB-KNIT-JER-0017','A2023','WH'),aliases)
    def test_ambiguous_multivalue_not_joined(self):
        for value in [['WH','BW'],[{'text':'W'},{'text':'H'}],[{'text':'WH','type':'mention'}]]:
            aliases,issues=build_aliases([inventory(**{'颜色缩写':value})])
            self.assertNotIn(('FAB-KNIT-JER-0017','A2023','WH'),aliases)
            self.assertTrue(issues)
    def test_missing_record_id_prevents_alias(self):
        row=inventory();row.pop('record_id')
        aliases,issues=build_aliases([row])
        self.assertEqual(len(aliases),0);self.assertEqual(issues[0]['status'],'MISSING_RECORD_ID')
    def test_empty_input(self):
        aliases,issues=build_aliases([])
        self.assertEqual(aliases,{});self.assertEqual(issues,[])
    def test_missing_source_color_preserves_provenance_and_blank(self):
        aliases,_=build_aliases([inventory()])
        out=resolve_reference(demand(),aliases,{})
        self.assertEqual(out['status'],'SOURCE_COLOR_UNAVAILABLE')
        self.assertEqual(out['value'],'');self.assertEqual(out['source_record_id'],'r1')
    def test_standard_chinese_field_is_not_a_fallback(self):
        aliases,_=build_aliases([inventory()])
        self.assertEqual(resolve_reference(demand(中文颜色名称='亮白'),aliases,{})['value'],'')
    def test_inputs_and_inventory_quantities_are_not_modified(self):
        rows=[inventory(**{'库存成品数量（条）':0})];before=copy.deepcopy(rows)
        row=demand();row_before=copy.deepcopy(row)
        aliases,_=build_aliases(rows);resolve_reference(row,aliases,{})
        self.assertEqual(rows,before);self.assertEqual(row,row_before)


if __name__=='__main__':
    unittest.main(verbosity=2)