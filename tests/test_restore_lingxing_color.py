"""Offline regression of actual restored functions using AST-only loading.
No production modules, credentials, network clients or database drivers are imported.
Baseline fixtures contain only audited function definitions from source/*.before.py.
"""
from pathlib import Path
import ast
import asyncio
import copy
import re
import sys
import unittest
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
NAMED = ROOT / 'jobs/feishu/generate_procurement_report_named_colors.py'
HELPER = ROOT / 'jobs/feishu/generate_procurement_report_lx_color.py'
FIXTURES = Path(__file__).parent / 'fixtures'
BEFORE_NAMED = FIXTURES / 'named_writer_before.py.txt'
BEFORE_HELPER = FIXTURES / 'legacy_reader_before.py.txt'


def load_functions(path, names, env):
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    selected = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    if {n.name for n in selected} != set(names):
        raise AssertionError('Missing requested actual function')
    for node in selected:
        if any(isinstance(x, (ast.Import, ast.ImportFrom)) for x in ast.walk(node)):
            raise AssertionError('Unexpected import in isolated function')
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), 'exec'), env)
    return env


def environment():
    return dict(Any=Any, Dict=Dict, List=List, Mapping=Mapping, Sequence=Sequence,
                datetime=datetime, re=re,
                logger=SimpleNamespace(info=lambda *_: None, warning=lambda *_: None))


class DatabaseDouble:
    def __init__(self, rows, table_exists=True):
        self.rows=copy.deepcopy(rows)
        self.table_exists=table_exists
        self.connections=0
        self.calls=[]
        self.current=[]
    @contextmanager
    def cursor(self):
        self.connections += 1
        yield self
    def execute(self, sql, params=None):
        self.calls.append((sql, copy.deepcopy(params)))
        if 'COUNT(*)' in sql:
            self.current=[]
        elif params is None:
            self.current=copy.deepcopy(self.rows)
        else:
            keys=set(params)
            self.current=[copy.deepcopy(r) for r in self.rows if r.get('sku') in keys]
    def fetchone(self):
        return {'cnt':int(self.table_exists)}
    def fetchall(self):
        return self.current


class ClientDouble:
    def __init__(self):
        self.fields=None
        self.rows=None
        self.delete_calls=0
        self.write_calls=0
    async def delete_all_records(self):
        self.delete_calls+=1
    async def write_records(self, rows, batch_size):
        assert batch_size == 500
        self.rows=copy.deepcopy(rows)
        self.write_calls+=1
        return len(rows)


def helper_env(path, rows=(), table_exists=True):
    db=DatabaseDouble(rows,table_exists)
    env=environment()
    env['db_cursor']=db.cursor
    load_functions(path, {'parse_lingxing_color','read_lingxing_color_map'}, env)
    return env,db


def sample_rows():
    rows=[]
    for index,system,code in [(0,'B2024','BK'),(1,'A2023','WH'),(2,'待定','XX')]:
        row={'统计类型':'带颜色','面料':'测试面料','面料编号':'013','颜色体系':system,
             '颜色缩写':code,'面料颜色编号':f'013-{system}-{code}',
             '颜色汇总代码':f'{system}:{code}','颜色':'旧展示不应取代',
             '库存归属状态':'唯一或未确认','用量信息缺失SPU':'SYNTHETIC_ONLY'}
        for j,name in enumerate(['库存量/条','库存量/米','待到货量/条','待到货量/米',
                     '当月已下单消耗/米','当月完整预估/米','当月剩余预估/米',
                     'T+1月预估/米','T+2月预估/米','T+3月预估/米',
                     '运营当月预估/米','运营T+1月预估/米','运营T+2月预估/米','运营T+3月预估/米']):
            row[name]=[Decimal('1.25'),0,None][(index+j)%3]
        rows.append(row)
    total=copy.deepcopy(rows[0]);total.update(统计类型='总量',颜色体系='',颜色缩写='',面料颜色编号='')
    rows.append(total)
    return rows


def run_writer(path, rows, color_map=None, actual_helper=False):
    db=DatabaseDouble(rows)
    client=ClientDouble()
    requested=[]
    catalog_calls=[]
    async def ensure(app_token, table_name, field_list, remove_extra):
        assert app_token=='[TEST_ONLY]' and table_name=='面料预估明细' and remove_extra is True
        client.fields=copy.deepcopy(field_list)
        return client
    def read_map(keys=None):
        requested.append(copy.deepcopy(keys))
        return dict(color_map or {})
    def describe(system,code):
        catalog_calls.append((system,code))
        return {'中文颜色名称':f'标准-{code}', '颜色显示名称':f'标准-{code}|{system}',
                'A2023中文候选':f'A-{code}','B2024中文候选':f'B-{code}',
                '颜色映射状态':'颜色编制表主映射' if system!='待定' else '体系待定，需人工确认'}
    env=environment()
    if actual_helper:
        src=[{'sku':k,'product_name':v} for k,v in (color_map or {}).items()]
        he,hdb=helper_env(HELPER,src)
        read_map=he['read_lingxing_color_map']
    else:
        hdb=None
    env.update(db_cursor=db.cursor,
       base=SimpleNamespace(base=SimpleNamespace(_get_or_create_table=ensure,FEISHU_APP_TOKEN='[TEST_ONLY]'), read_lingxing_color_map=read_map),
       ColorMappingCatalog=SimpleNamespace(from_runtime=lambda:SimpleNamespace(describe=describe)),
       _month_label=lambda date,delta:f'{((date.month-1+delta)%12)+1}月')
    load_functions(path,{'write_fabric_detail_to_feishu'},env)
    asyncio.run(env['write_fabric_detail_to_feishu'](datetime(2026,11,5)))
    return SimpleNamespace(client=client,db=db,requested=requested,catalog_calls=catalog_calls,helper_db=hdb)


class RestoredWriterTests(unittest.TestCase):
    def test_all_existing_fields_values_and_order_equal_before_writer(self):
        rows=sample_rows();before=copy.deepcopy(rows)
        old=run_writer(BEFORE_NAMED,rows)
        new=run_writer(NAMED,rows,{'013-B2024-BK':'1#黑','013-WH':'2#白'})
        stripped=[{k:v for k,v in r.items() if k!='颜色-领星'} for r in new.client.rows]
        self.assertEqual(stripped,old.client.rows)
        self.assertEqual(rows,before)
        self.assertEqual(new.catalog_calls,old.catalog_calls)
        self.assertEqual(new.db.calls,old.db.calls)
    def test_schema_adds_exactly_one_text_field(self):
        old=run_writer(BEFORE_NAMED,sample_rows())
        new=run_writer(NAMED,sample_rows())
        self.assertEqual([f for f in new.client.fields if f['name']!='颜色-领星'],old.client.fields)
        self.assertEqual([f for f in new.client.fields if f['name']=='颜色-领星'],[{'name':'颜色-领星','type':'text'}])
    def test_exact_precedes_legacy_fallback(self):
        out=run_writer(NAMED,sample_rows()[:1],{'013-B2024-BK':'1#黑','013-BK':'9#其他'})
        self.assertEqual(out.client.rows[0]['颜色-领星'],'1#黑')
    def test_legacy_fallback_is_preserved(self):
        out=run_writer(NAMED,sample_rows()[1:2],{'013-WH':'2#白'})
        self.assertEqual(out.client.rows[0]['颜色-领星'],'2#白')
    def test_unmatched_does_not_use_catalog_chinese_as_fallback(self):
        out=run_writer(NAMED,sample_rows()[2:3],{})
        self.assertEqual(out.client.rows[0]['颜色-领星'],'')
        self.assertEqual(out.client.rows[0]['中文颜色名称'],'标准-XX')
    def test_total_row_always_blank_and_not_a_lookup_key(self):
        total=sample_rows()[-1];total['面料颜色编号']='SHOULD_NOT_QUERY'
        out=run_writer(NAMED,[total],{'SHOULD_NOT_QUERY':'9#错误'})
        self.assertEqual(out.client.rows[0]['颜色-领星'],'')
        self.assertEqual(out.requested,[[]])
    def test_queries_only_sorted_unique_exact_and_legacy_keys(self):
        rows=sample_rows()+[copy.deepcopy(sample_rows()[0])]
        out=run_writer(NAMED,rows)
        expected=sorted({'013-B2024-BK','013-BK','013-A2023-WH','013-WH','013-待定-XX','013-XX'})
        self.assertEqual(out.requested,[expected])
    def test_actual_writer_calls_actual_filtered_helper(self):
        out=run_writer(NAMED,sample_rows(),{'013-B2024-BK':'1#黑','013-WH':'2#白','OUTSIDE':'9#外'},actual_helper=True)
        self.assertEqual([r['颜色-领星'] for r in out.client.rows],['1#黑','2#白','',''])
        params=[p for s,p in out.helper_db.calls if 'COUNT(*)' not in s]
        self.assertNotIn('OUTSIDE',sum(params,[]))
        self.assertEqual(out.client.delete_calls,1)
        self.assertEqual(out.client.write_calls,1)
    def test_empty_writer_has_no_color_database_connection(self):
        out=run_writer(NAMED,[],{},actual_helper=True)
        self.assertEqual(out.helper_db.connections,0)
        self.assertEqual(out.client.rows,[])


class FilteredHelperTests(unittest.TestCase):
    def test_empty_list_avoids_database_connection(self):
        env,db=helper_env(HELPER)
        self.assertEqual(env['read_lingxing_color_map']([]),{})
        self.assertEqual(db.connections,0)
        self.assertEqual(db.calls,[])
    def test_blank_keys_avoid_database_connection(self):
        env,db=helper_env(HELPER)
        self.assertEqual(env['read_lingxing_color_map'](['','  ']),{})
        self.assertEqual(db.connections,0)
    def test_key_parameterization_no_interpolation(self):
        key="SKU' OR 1=1 --"
        env,db=helper_env(HELPER,[{'sku':key,'product_name':'3#绿'}])
        self.assertEqual(env['read_lingxing_color_map']([key]),{key:'3#绿'})
        sql,params=db.calls[1]
        self.assertNotIn(key,sql)
        self.assertEqual(sql.count('%s'),1)
        self.assertEqual(params,[key])
    def test_chunking_500_500_1_without_overlap(self):
        keys=[f'K{i:04d}' for i in range(1001)]
        env,db=helper_env(HELPER,[{'sku':key,'product_name':'1#黑'} for key in keys])
        self.assertEqual(len(env['read_lingxing_color_map'](keys)),1001)
        params=[p for s,p in db.calls if 'COUNT(*)' not in s]
        self.assertEqual([len(p) for p in params],[500,500,1])
        self.assertEqual(sum(params,[]),keys)
        self.assertTrue(all(s.count('%s')==len(p) for s,p in db.calls if p is not None))
    def test_keys_deduplicate_strip_and_stable_sort(self):
        env,db=helper_env(HELPER)
        env['read_lingxing_color_map']([' B ','A','B',''])
        self.assertEqual(db.calls[1][1],['A','B'])
    def test_default_call_matches_old_reader_and_query(self):
        rows=[{'sku':'013-BK','product_name':' 12 # 黑 色,附注'},
              {'sku':'UNPARSEABLE','product_name':'普通名称'}, {'sku':'A','product_name':'1#白'}]
        new,ndb=helper_env(HELPER,rows);old,odb=helper_env(BEFORE_HELPER,rows)
        self.assertEqual(new['read_lingxing_color_map'](),old['read_lingxing_color_map']())
        self.assertEqual([(' '.join(q.split()),p) for q,p in ndb.calls],[(' '.join(q.split()),p) for q,p in odb.calls])
    def test_explicit_none_preserves_default(self):
        rows=[{'sku':'A','product_name':'1#白'}]
        env,db=helper_env(HELPER,rows)
        self.assertEqual(env['read_lingxing_color_map'](None),{'A':'1#白'})
        self.assertNotIn(' IN (',db.calls[1][0])
    def test_absent_source_table_returns_empty_without_data_query(self):
        env,db=helper_env(HELPER,[],False)
        self.assertEqual(env['read_lingxing_color_map'](['A']),{})
        self.assertEqual(len(db.calls),1)
    def test_parse_function_ast_is_unchanged(self):
        def node(path):
            return next(n for n in ast.parse(path.read_text(encoding='utf-8-sig')).body if isinstance(n,ast.FunctionDef) and n.name=='parse_lingxing_color')
        self.assertEqual(ast.dump(node(HELPER),include_attributes=False),ast.dump(node(BEFORE_HELPER),include_attributes=False))
    def test_pipeline_main_dispatch_ast_unchanged(self):
        def node(path):
            return next(n for n in ast.parse(path.read_text(encoding='utf-8-sig')).body if isinstance(n,ast.FunctionDef) and n.name=='main')
        self.assertEqual(ast.dump(node(NAMED),include_attributes=False),ast.dump(node(BEFORE_NAMED),include_attributes=False))


if __name__=='__main__':
    unittest.main(verbosity=2)