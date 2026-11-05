"""Actual AST writer with pure name helper and synthetic I/O substitutes."""
from __future__ import annotations

import ast
import copy
import re
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from test_named_color_reference_writer import Harness, NAMED, ROOT, NUMERIC, load_function_definitions

BEFORE = Path(__file__).parent / 'fixtures/named_color_name_fallback_before.py.txt'
NAME_HELPER = ROOT / 'jobs/feishu/fabric_lingxing_name_reference.py'
FAB = 'FAB-KNIT-JER-0005'
FAB_NAME = '255单六面料'


def pure_name_helper():
    tree = ast.parse(NAME_HELPER.read_text(encoding='utf-8-sig'))
    for node in tree.body:
        if isinstance(node, ast.Import):
            assert all(x.name in {'re'} for x in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.module in {'__future__', 'collections', 'typing'}
        else:
            assert isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Expr))
            if isinstance(node, ast.Expr):
                assert isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    env = {'__name__': 'offline_pure_name_helper'}
    exec(compile(tree, str(NAME_HELPER), 'exec'), env)
    return env


def row(code='EM', system='A2023', fabric=FAB, kind='带颜色', name=FAB_NAME):
    r = {'统计类型': kind, '面料': name, '面料编号': fabric,
         '颜色体系': system, '颜色缩写': code, '颜色汇总代码': system+':'+code,
         '面料颜色编号': fabric+'-'+system+'-'+code,
         '颜色': '不改原颜色', '库存归属状态': 'TEST', '用量信息缺失SPU': 'TEST_ONLY'}
    for i, k in enumerate(NUMERIC):
        r[k] = [14, None, 0, 1.125][i % 4]
    return r


def inventory(old='BLN', new='EM', rid='TEST_INV'):
    return {'record_id': rid, 'fields': {'颜色缩写': old, '领星新颜色缩写': new,
            '面料编号-颜色缩写': FAB+'-'+new, '库存量/条': 0}}


def info(primary='', a='', b='英伦庄园蓝'):
    return {'中文颜色名称': primary, '颜色显示名称': '原显示名称',
            'A2023中文候选': a, 'B2024中文候选': b, '颜色映射状态': '原状态不改'}


def source(code='EM', name='18#英伦庄园蓝', fabric=FAB, fabric_name=FAB_NAME):
    return {'sku': fabric+'-'+code, 'product_name': fabric_name+'-'+name, 'synced_at': 'TEST'}


def parse_color(value):
    match = re.search(r'(\d+\s*#\s*[^#，,;/|]+)', value)
    return re.sub(r'\s+', '', match.group(1)) if match else ''


class NameHarness(Harness):
    def __init__(self, path=NAMED, rows=None, inv=None, color_map=None, names=None,
                 infos=None, name_error=None):
        super().__init__(path=path, rows=[row()] if rows is None else rows,
                         inventory=[inventory()] if inv is None else inv,
                         color_map={FAB+'-EM': '18#英伦庄园蓝'} if color_map is None else color_map)
        self.names = copy.deepcopy([source()] if names is None else names)
        self.name_requests = []
        self.fallback_calls = []
        self.name_error = name_error
        self.infos = infos or {}
        helper = pure_name_helper()
        self.base.parse_lingxing_color = parse_color
        self.env['build_name_index'] = helper['build_name_index']
        def resolve(r, idx):
            self.fallback_calls.append(copy.deepcopy(r))
            return helper['resolve_name_reference'](r, idx)
        self.env['resolve_name_reference'] = resolve
        def describe(system, code):
            return copy.deepcopy(self.infos.get((system, code), info()))
        self.env['ColorMappingCatalog'] = SimpleNamespace(from_runtime=lambda:SimpleNamespace(describe=describe))
        def read_names(codes):
            self.events.append('name_read')
            self.name_requests.append(copy.deepcopy(codes))
            if self.name_error:
                raise self.name_error
            return copy.deepcopy(self.names) if codes else []
        self.env['read_lingxing_name_rows'] = read_names


class NameWriterTests(unittest.TestCase):
    def test_em_uses_catalog_b_candidate_without_relabeling(self):
        h = NameHarness().run()
        self.assertEqual(h.output[0]['颜色-领星'], '18#英伦庄园蓝')
        self.assertEqual(h.output[0]['颜色体系'], 'A2023')
        self.assertEqual(h.output[0]['颜色缩写'], 'EM')
        self.assertEqual(h.output[0]['库存量/条'], 14)
        self.assertEqual(h.fallback_calls[0]['B2024中文候选'], '英伦庄园蓝')
        self.assertEqual(h.name_requests, [[FAB]])

    def test_single_terminal_se_suffix_matches(self):
        h = NameHarness(names=[source('DK', '7#深咖')], infos={('A2023','EM'):info('深咖色', '', '')}).run()
        self.assertEqual(h.output[0]['颜色-领星'], '7#深咖')
        self.assertEqual(h.output[0]['中文颜色名称'], '深咖色')

    def test_confirmed_code_match_never_calls_fallback(self):
        h = NameHarness(rows=[row('BLN')], names=[source('OTHER','99#英伦庄园蓝')]).run()
        self.assertEqual(h.output[0]['颜色-领星'], '18#英伦庄园蓝')
        self.assertEqual(h.fallback_calls, [])

    def test_alias_conflict_not_bypassed(self):
        inv = [inventory('EM','X1','TEST_X1'), inventory('EM','X2','TEST_X2'), inventory()]
        cmap = {FAB+'-EM':'18#英伦庄园蓝', FAB+'-X1':'1#红', FAB+'-X2':'2#绿'}
        h = NameHarness(inv=inv, color_map=cmap).run()
        self.assertEqual(h.output[0]['颜色-领星'], '')
        self.assertEqual(h.fallback_calls, [])

    def test_known_alias_missing_source_not_bypassed(self):
        inv = [inventory('EM','MISS','TEST_MISS'), inventory()]
        h = NameHarness(inv=inv).run()
        self.assertEqual(h.output[0]['颜色-领星'], '')
        self.assertEqual(h.fallback_calls, [])

    def test_unknown_system_can_use_unambiguous_catalog_candidate(self):
        h = NameHarness(rows=[row(system='待定')]).run()
        self.assertEqual(h.output[0]['颜色-领星'], '18#英伦庄园蓝')
        self.assertEqual(h.output[0]['颜色体系'], '待定')

    def test_multiple_candidate_names_remain_blank(self):
        h = NameHarness(infos={('A2023','EM'):info('', '红', '英伦庄园蓝')}).run()
        self.assertEqual(h.output[0]['颜色-领星'], '')

    def test_duplicate_name_skus_remain_blank(self):
        h = NameHarness(names=[source(), source('OTHER')]).run()
        self.assertEqual(h.output[0]['颜色-领星'], '')

    def test_total_row_never_gets_name_reference(self):
        h = NameHarness(rows=[row(kind='总量')]).run()
        self.assertEqual(h.output[0]['颜色-领星'], '')
        self.assertEqual(h.fallback_calls, [])
        self.assertEqual(h.name_requests, [[]])

    def test_exact_deployed_nonreference_schema_values_and_dates_preserved(self):
        rows = [row(), row('BLN'), row(system='待定'), row(kind='总量'), row(fabric='0005')]
        original = copy.deepcopy(rows)
        old, new = NameHarness(path=BEFORE, rows=rows).run(), NameHarness(rows=rows).run()
        strip = lambda rs:[{k:v for k,v in r.items() if k != '颜色-领星'} for r in rs]
        self.assertEqual(strip(new.output), strip(old.output))
        self.assertEqual(new.fields, old.fields)
        self.assertEqual(new.table_args, old.table_args)
        self.assertEqual(new.sql, old.sql)
        self.assertEqual(rows, original)
        self.assertEqual(new.rows, old.rows)
        self.assertEqual(len(new.output), len(old.output))
        self.assertIn('2027年1月预估/米', new.output[0])
        self.assertIn('运营2027年3月预估/米', new.output[0])
        self.assertEqual(new.output[1]['颜色-领星'], old.output[1]['颜色-领星'])

    def test_name_source_failure_before_delete(self):
        h = NameHarness(name_error=RuntimeError('TEST source failed'))
        with self.assertRaisesRegex(RuntimeError, 'TEST source failed'):
            h.run()
        self.assertEqual((h.deleted, h.writes), (0,0))
        self.assertNotIn('delete', h.events)

    def test_only_colored_valid_code_nonempty_name_gets_source_scope(self):
        rows=[row(), row(kind='总量',fabric='FAB-OTHER-0001'), row(fabric='0005'), row(fabric='FAB-X-001',name='')]
        h = NameHarness(rows=rows).run()
        self.assertEqual(h.name_requests, [[FAB]])

    def test_ast_loading_does_not_import_or_initialize_production(self):
        h = NameHarness()
        self.assertEqual(h.imports, ['__future__'])
        self.assertEqual(h.events, [])
        self.assertEqual((h.deleted,h.writes), (0,0))


class ReaderHarness:
    def __init__(self, batches=None, error=None):
        self.batches = list(batches or [])
        self.error = error
        self.opened = 0
        self.calls = []
        env = {'re':re, 'db_cursor':self.cursor}
        load_function_definitions(NAMED, env)
        self.read = env['read_lingxing_name_rows']

    @contextmanager
    def cursor(self):
        self.opened += 1
        yield self

    def execute(self, sql, params):
        self.calls.append((sql,copy.deepcopy(params)))
        if self.error:
            raise self.error

    def fetchall(self):
        return self.batches.pop(0)


class BoundedNameReaderTests(unittest.TestCase):
    def test_empty_input_never_connects(self):
        h = ReaderHarness()
        self.assertEqual(h.read([]), [])
        self.assertEqual((h.opened,len(h.calls)), (0,0))

    def test_invalid_prefixes_rejected_before_db(self):
        for bad in ['FAB-X%', "FAB-X' OR 1=1", '0005', 'FAB-', 'FAB-x', 'FAB-X_', None, 3]:
            h = ReaderHarness()
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                h.read([FAB,bad])
            self.assertEqual(h.opened,0)

    def test_41_prefixes_chunk_20_20_1_parameterized_readonly(self):
        codes = [f'FAB-KNIT-TEST-{i:04d}' for i in range(41)]
        h = ReaderHarness([[source()], [source()], [source()]])
        rows = h.read(list(reversed(codes))+[codes[0]])
        self.assertEqual(len(rows),3)
        self.assertEqual([len(p) for _,p in h.calls], [20,20,1])
        self.assertEqual(h.opened,1)
        self.assertEqual([p for _,ps in h.calls for p in ps],[c+'-%' for c in sorted(codes)])
        for sql,params in h.calls:
            self.assertTrue(sql.startswith('SELECT sku, product_name, synced_at '))
            self.assertEqual(sql.count('sku LIKE %s'),len(params))
            self.assertTrue(sql.endswith('ORDER BY sku LIMIT 5001'))
            self.assertNotIn(FAB,sql)
            self.assertFalse(any(x in sql.upper() for x in ['DELETE','INSERT','UPDATE','TRUNCATE']))

    def test_per_query_budget_rejects_5001(self):
        h = ReaderHarness([[source()]*5001])
        with self.assertRaisesRegex(RuntimeError,'5000'):
            h.read([FAB])
        self.assertEqual(len(h.calls),1)

    def test_total_budget_rejects_more_than_5000(self):
        h = ReaderHarness([[source()]*3000,[source()]*2001])
        with self.assertRaisesRegex(RuntimeError,'5000'):
            h.read([f'FAB-X-{i}' for i in range(21)])
        self.assertEqual(len(h.calls),2)

    def test_nonempty_scope_empty_result_is_fail_closed(self):
        h = ReaderHarness([[]])
        with self.assertRaisesRegex(RuntimeError,'来源为空'):
            h.read([FAB])

    def test_db_failure_propagates_without_retry(self):
        h = ReaderHarness(error=RuntimeError('TEST db read failure'))
        with self.assertRaisesRegex(RuntimeError,'TEST db read failure'):
            h.read([FAB])
        self.assertEqual(len(h.calls),1)

    def test_actual_reader_empty_failure_prevents_writer_delete(self):
        h = NameHarness()
        reader = ReaderHarness([[]])
        h.env['read_lingxing_name_rows'] = reader.read
        with self.assertRaisesRegex(RuntimeError,'来源为空'):
            h.run()
        self.assertEqual((h.deleted,h.writes), (0,0))


if __name__ == '__main__':
    unittest.main(verbosity=2)
