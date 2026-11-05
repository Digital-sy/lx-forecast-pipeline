"""Synthetic actual-writer tests for final B2024 display fallback only."""
from __future__ import annotations

import copy
import unittest
from pathlib import Path
from types import SimpleNamespace
from test_named_color_name_fallback_writer import NameHarness, FAB, row, source, info, inventory

BEFORE = Path(__file__).parent / 'fixtures/named_color_b2024_display_before.py.txt'


class B2024DisplayFallbackTests(unittest.TestCase):
    def test_final_blank_copies_candidate_without_creating_number(self):
        h = NameHarness(infos={('A2023','EM'):info('未匹配标准名','','靛蓝色')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'靛蓝色')
        self.assertNotIn('#',h.output[0]['颜色-领星'])
        self.assertEqual(h.output[0]['颜色体系'],'A2023')
        self.assertEqual(h.output[0]['颜色缩写'],'EM')

    def test_code_match_keeps_original_numbered_value(self):
        h = NameHarness(rows=[row('BLN')], infos={('A2023','BLN'):info('', '', '不同候选')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'18#英伦庄园蓝')
        self.assertEqual(h.fallback_calls,[])

    def test_unique_name_match_keeps_original_source_value(self):
        h = NameHarness(infos={('A2023','EM'):info('英伦庄园蓝','','不同候选')}).run()
        self.assertEqual(h.output[0]['颜色-领星'],'18#英伦庄园蓝')

    def test_empty_null_whitespace_candidates_stay_blank(self):
        for candidate in ['',None,'   ']:
            with self.subTest(candidate=candidate):
                h=NameHarness(infos={('A2023','EM'):info('未匹配标准名','',candidate)}).run()
                self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_invalid_nontext_candidate_not_stringified(self):
        for candidate in [0,[],['红'],{'text':'红'}]:
            with self.subTest(candidate=candidate):
                h=NameHarness(infos={('A2023','EM'):info('未匹配标准名','',candidate)}).run()
                self.assertEqual(h.output[0]['颜色-领星'],'')

    def test_display_copies_candidate_exactly_not_normalizes_it(self):
        value='  合成深咖色  '
        h=NameHarness(infos={('A2023','EM'):info('未匹配标准名','',value)}).run()
        self.assertEqual(h.output[0]['颜色-领星'],value)
        self.assertEqual(h.output[0]['B2024中文候选'],value)

    def test_total_row_remains_blank(self):
        h=NameHarness(rows=[row(kind='总量')]).run()
        self.assertEqual(h.output[0]['颜色-领星'],'')
        self.assertEqual(h.output[0]['B2024中文候选'],'')

    def test_all_other_fields_and_existing_nonempty_references_preserved(self):
        rows=[row('BLN'),row(),row('QQ'),row('ZZ'),row(kind='总量')]
        names={('A2023','QQ'):info('缺标准名','','合成紫色'),('A2023','ZZ'):info('缺标准名','','')}
        old=NameHarness(path=BEFORE,rows=rows,infos=names).run()
        new=NameHarness(rows=rows,infos=names).run()
        strip=lambda rs:[{k:v for k,v in r.items() if k!='颜色-领星'} for r in rs]
        self.assertEqual(strip(new.output),strip(old.output))
        self.assertEqual(old.fields,new.fields)
        self.assertEqual(old.sql,new.sql)
        self.assertEqual(old.requested,new.requested)
        self.assertEqual(old.name_requests,new.name_requests)
        self.assertEqual(old.rows,new.rows)
        self.assertEqual(old.table_args,new.table_args)
        for before,after in zip(old.output,new.output):
            if before['颜色-领星']:
                self.assertEqual(after['颜色-领星'],before['颜色-领星'])
            else:
                self.assertEqual(after['颜色-领星'],before['B2024中文候选'])

    def test_matching_and_display_counts_are_separate(self):
        rows=[row('BLN'),row(),row('QQ'),row('ZZ'),row(kind='总量')]
        infos={('A2023','QQ'):info('不匹配','','合成紫'),('A2023','ZZ'):info('不匹配','','')}
        h=NameHarness(rows=rows,infos=infos)
        logs=[]
        def log(message,*args,**kwargs):
            logs.append(message % args if args else message)
        h.env['logger']=SimpleNamespace(info=log,warning=log)
        h.run()
        self.assertIn('领星颜色参考已核验匹配：代码 1 条，唯一名称 1 条',logs)
        self.assertIn('B2024中文候选展示兜底：1 条；非已验证领星匹配，不生成编号',logs)

    def test_conflicting_alias_still_not_claimed_as_matching(self):
        inv=[inventory('EM','X1','TEST_X1'),inventory('EM','X2','TEST_X2'),inventory()]
        cmap={FAB+'-EM':'18#英伦庄园蓝',FAB+'-X1':'1#红',FAB+'-X2':'2#绿'}
        h=NameHarness(inv=inv,color_map=cmap)
        logs=[]
        h.env['logger']=SimpleNamespace(info=lambda m,*args,**kwargs:logs.append(m % args if args else m),warning=lambda *a,**k:None)
        h.run()
        self.assertEqual(h.output[0]['颜色-领星'],'英伦庄园蓝')
        self.assertEqual(h.fallback_calls,[])
        self.assertIn('领星颜色参考已核验匹配：代码 0 条，唯一名称 0 条',logs)
        self.assertIn('B2024中文候选展示兜底：1 条；非已验证领星匹配，不生成编号',logs)

    def test_read_failure_still_blocks_deletion_and_display_write(self):
        h=NameHarness(name_error=RuntimeError('TEST_SOURCE_FAILURE'))
        with self.assertRaisesRegex(RuntimeError,'TEST_SOURCE_FAILURE'):
            h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))

    def test_repeat_same_input_keeps_same_display_and_counts(self):
        infos={('A2023','EM'):info('未匹配','','合成蓝')}
        a=NameHarness(infos=infos).run()
        b=NameHarness(infos=infos).run()
        self.assertEqual(a.output,b.output)
        self.assertEqual(a.fields,b.fields)


if __name__=='__main__':
    unittest.main(verbosity=2)
