"""Final B-only lookup must return a unique real source, never the candidate."""
from __future__ import annotations

import copy
import unittest
from pathlib import Path
from types import SimpleNamespace
from test_named_color_name_fallback_writer import (
    NameHarness, FAB, FAB_NAME, row, source, info, inventory, pure_name_helper,
)

BEFORE = Path(__file__).parent / 'fixtures/named_color_b2024_correction_before.py.txt'


class B2024SourceWriterTests(unittest.TestCase):
    def test_255_bo_b_candidate_black_maps_real_source_without_system_change(self):
        h=NameHarness(rows=[row('BO')],names=[source('BO','1#黑玛瑙')],
                      infos={('A2023','BO'):info('亮橘','亮橘','黑玛瑙')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'1#黑玛瑙')
        self.assertEqual(h.output[0]['颜色体系'],'A2023')
        self.assertEqual(h.output[0]['颜色缩写'],'BO')
        self.assertEqual(h.output[0]['中文颜色名称'],'亮橘')
        self.assertEqual(h.output[0]['B2024中文候选'],'黑玛瑙')
        self.assertEqual(len(h.b2024_calls),1)

    def test_candidate_without_source_must_remain_blank(self):
        h=NameHarness(infos={('A2023','EM'):info('缺失主名','','不存在候选')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'')
        self.assertEqual(h.output[0]['B2024中文候选'],'不存在候选')

    def test_confirmed_code_has_priority_over_real_b_source(self):
        h=NameHarness(rows=[row('BLN')],names=[source('BO','1#黑玛瑙')],
                      infos={('A2023','BLN'):info('亮橘','','黑玛瑙')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'18#英伦庄园蓝')
        self.assertEqual(h.b2024_calls,[])

    def test_normal_unique_name_has_priority_over_b_source(self):
        h=NameHarness(names=[source(),source('BO','1#黑玛瑙')],
                      infos={('A2023','EM'):info('英伦庄园蓝','','黑玛瑙')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'18#英伦庄园蓝')
        self.assertEqual(h.b2024_calls,[])

    def test_conflicting_b_source_names_remain_blank(self):
        h=NameHarness(names=[source('BO','1#黑玛瑙'),source('BX','2#黑玛瑙色')],
                      infos={('A2023','EM'):info('未匹配主名','','黑玛瑙')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_missing_real_source_color_never_synthesizes_number(self):
        h=NameHarness(names=[source('BO','1#黑玛瑙')],
                      infos={('A2023','EM'):info('未匹配主名','','黑玛瑙')})
        h.base.parse_lingxing_color=lambda text:''
        h.run()
        self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_success_without_source_sku_is_rejected(self):
        h=NameHarness(infos={('A2023','EM'):info('未匹配主名','','黑玛瑙')})
        h.env['resolve_b2024_name_reference']=lambda *args:{'status':'NAME_MATCHED','value':'1#黑玛瑙','source_sku':''}
        h.run()
        self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_conflict_status_with_value_is_rejected(self):
        h=NameHarness(infos={('A2023','EM'):info('未匹配主名','','黑玛瑙')})
        h.env['resolve_b2024_name_reference']=lambda *args:{'status':'NAME_CONFLICT','value':'1#黑玛瑙','source_sku':FAB+'-BO'}
        h.run()
        self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_terminal_se_matching_uses_real_source_text(self):
        h=NameHarness(names=[source('DK','7#深咖')],
                      infos={('A2023','EM'):info('未匹配主名','','深咖色')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'7#深咖')
        self.assertEqual(h.output[0]['B2024中文候选'],'深咖色')

    def test_empty_null_invalid_b_candidates_remain_blank(self):
        for value in ['',None,'   ',0,[],['红']]:
            with self.subTest(value=value):
                h=NameHarness(infos={('A2023','EM'):info('未匹配主名','',value)}).run()
                self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_total_row_remains_blank(self):
        h=NameHarness(rows=[row(kind='总量')]).run()
        self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_code_conflict_only_filled_with_unique_b_source_evidence(self):
        inv=[inventory('EM','X1','TEST_X1'),inventory('EM','X2','TEST_X2'),inventory()]
        cmap={FAB+'-EM':'18#英伦庄园蓝',FAB+'-X1':'1#红',FAB+'-X2':'2#绿'}
        h=NameHarness(inv=inv,color_map=cmap).run()
        self.assertEqual(h.output[0]['颜色-领星'],'18#英伦庄园蓝')
        self.assertEqual(h.fallback_calls,[])
        self.assertEqual(len(h.b2024_calls),1)

    def test_all_other_fields_schema_sql_and_calculations_unchanged(self):
        rows=[row('BLN'),row('BO'),row('QQ'),row(kind='总量')]
        names=[source(),source('BO','1#黑玛瑙')]
        infos={('A2023','BO'):info('亮橘','亮橘','黑玛瑙'),('A2023','QQ'):info('缺主名','','不存在候选')}
        old=NameHarness(path=BEFORE,rows=rows,names=names,infos=infos).run()
        new=NameHarness(rows=rows,names=names,infos=infos).run()
        strip=lambda rs:[{k:v for k,v in r.items() if k!='颜色-领星'} for r in rs]
        self.assertEqual(strip(old.output),strip(new.output))
        self.assertEqual(old.fields,new.fields)
        self.assertEqual(old.sql,new.sql)
        self.assertEqual(old.rows,new.rows)
        self.assertEqual(old.requested,new.requested)
        self.assertEqual(old.name_requests,new.name_requests)
        self.assertEqual(old.output[0]['颜色-领星'],new.output[0]['颜色-领星'])
        self.assertEqual(old.output[1]['颜色-领星'],'黑玛瑙')
        self.assertEqual(new.output[1]['颜色-领星'],'1#黑玛瑙')
        self.assertEqual(old.output[2]['颜色-领星'],'不存在候选')
        self.assertEqual(new.output[2]['颜色-领星'],'')

    def test_source_sku_and_classified_counts_logged(self):
        rows=[row('BLN'),row(),row('BO'),row('QQ')]
        infos={('A2023','BO'):info('亮橘','','黑玛瑙'),('A2023','QQ'):info('缺主名','','不存在候选')}
        h=NameHarness(rows=rows,names=[source(),source('BO','1#黑玛瑙')],infos=infos)
        logs=[]
        h.env['logger']=SimpleNamespace(info=lambda m,*args,**kwargs:logs.append(m%args if args else m),warning=lambda *a,**k:None)
        h.run()
        self.assertIn('领星颜色参考已核验匹配：代码 1 条，唯一名称 1 条',logs)
        self.assertTrue(any('B2024候选领星来源唯一匹配：1 条' in x for x in logs))
        self.assertTrue(any('来源SKU='+FAB+'-BO' in x for x in logs))
        self.assertTrue(any('NAME_NOT_FOUND' in x for x in logs))
        self.assertFalse(any('展示兜底' in x for x in logs))

    def test_source_failure_still_prevents_delete(self):
        h=NameHarness(name_error=RuntimeError('TEST_SOURCE_FAILURE'))
        with self.assertRaisesRegex(RuntimeError,'TEST_SOURCE_FAILURE'):h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))


class PureB2024SourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.helper=pure_name_helper()

    def resolve(self,r,sources=None,colors=None,pairs=None):
        sources=[source('BO','1#黑玛瑙')] if sources is None else sources
        colors={FAB+'-BO':'1#黑玛瑙'} if colors is None else colors
        pairs=[(FAB,FAB_NAME)] if pairs is None else pairs
        idx=self.helper['build_name_index'](sources,pairs,colors)
        return self.helper['resolve_b2024_name_reference'](r,idx)

    def test_ignores_primary_and_a_but_retains_original_record(self):
        r=row('BO');r.update(info('亮橘','粉色','黑玛瑙'));before=copy.deepcopy(r)
        got=self.resolve(r)
        self.assertEqual(got['value'],'1#黑玛瑙')
        self.assertEqual(got['source_sku'],FAB+'-BO')
        self.assertEqual(got['reference_basis'],'B2024_ONLY')
        self.assertEqual(got['original_system'],'A2023')
        self.assertEqual(r,before)

    def test_wrapped_fields_supported_without_mutating_input(self):
        r={'record_id':'TEST_REC','fields':row('BO')};r['fields'].update(info('亮橘','','黑玛瑙'))
        before=copy.deepcopy(r)
        self.assertEqual(self.resolve(r)['value'],'1#黑玛瑙')
        self.assertEqual(r,before)

    def test_no_b_candidate_does_not_use_primary_or_a(self):
        r=row();r.update(info('黑玛瑙','黑玛瑙',''))
        got=self.resolve(r)
        self.assertEqual(got['value'],'')
        self.assertEqual(got['status'],'MISSING_COLOR_NAME')

    def test_wrong_fabric_code_and_name_do_not_cross_match(self):
        for field,value in [('面料编号','FAB-KNIT-JER-0006'),('面料','其他面料')]:
            r=row();r.update(info('','','黑玛瑙'));r[field]=value
            with self.subTest(field=field):
                self.assertEqual(self.resolve(r)['value'],'')

    def test_multiple_source_skus_conflict(self):
        r=row();r.update(info('','','黑玛瑙'))
        got=self.resolve(r,sources=[source('BO','1#黑玛瑙'),source('BX','2#黑玛瑙色')])
        self.assertEqual(got['status'],'NAME_CONFLICT')
        self.assertEqual(got['value'],'')

    def test_same_sku_inconsistent_names_conflict(self):
        r=row();r.update(info('','','黑玛瑙'))
        got=self.resolve(r,sources=[source('BO','1#黑玛瑙'),source('BO','1#红色')])
        self.assertEqual(got['status'],'NAME_CONFLICT')
        self.assertEqual(got['value'],'')

    def test_missing_color_map_retains_sku_but_no_output(self):
        r=row();r.update(info('','','黑玛瑙'))
        got=self.resolve(r,colors={})
        self.assertEqual(got['status'],'SOURCE_COLOR_UNAVAILABLE')
        self.assertEqual(got['source_sku'],FAB+'-BO')
        self.assertEqual(got['value'],'')

    def test_returns_parsed_source_value_not_candidate_or_reconstructed_number(self):
        r=row();r.update(info('','','黑玛瑙色'))
        got=self.resolve(r,colors={FAB+'-BO':'001#黑玛瑙'})
        self.assertEqual(got['value'],'001#黑玛瑙')
        self.assertEqual(got['match_method'],'NAME_TRAILING_SE')

    def test_no_fuzzy_internal_se_or_contains_rule(self):
        for candidate in ['玛瑙','黑色玛瑙','黑玛瑙色号']:
            r=row();r.update(info('','',candidate))
            with self.subTest(candidate=candidate):
                self.assertEqual(self.resolve(r)['status'],'NAME_NOT_FOUND')


if __name__=='__main__':unittest.main(verbosity=2)
