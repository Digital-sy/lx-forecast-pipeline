"""Synthetic tests only. No network, MySQL or production configuration."""
import copy
import hashlib
import tempfile
from datetime import datetime
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import re
import sys
import unittest
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'scripts'))
import mysql_load_core as m

# Tiny synthetic schema exercises the actual transactional function. It is not a
# claim that real production table constraints or driver transport were tested.
def metadata():
    result={}
    for table in m.TABLES:
        def c(name,typ,nullable='NO',default=None):
            return {'TABLE_NAME':table,'COLUMN_NAME':name,'DATA_TYPE':typ,'COLUMN_TYPE':typ,'IS_NULLABLE':nullable,'COLUMN_DEFAULT':default,'EXTRA':'','ORDINAL_POSITION':1,'CHARACTER_SET_NAME':None,'COLLATION_NAME':None}
        result[table]=[c(m.PK[table],'char')]+[c(x,'char') for x in ('run_id','document_id','version_id','extraction_id') if x!=m.PK[table]]+[c('value_json','json'),c('quantity','decimal','YES'),c('optional_text','text','YES'),c('event_date','date','YES'),c('event_time','datetime','YES'),c('delete_flag','tinyint',default=0),c('create_time','datetime'),c('update_time','datetime')]
    return result

def package():
    result={t:[] for t in m.TABLES}
    for i,t in enumerate(m.TABLES[:6]):
        result[t]=[{m.PK[t]:format(i+1,'064x'),'value_json':{'zero':0,'unknown':None,'list':['中文',1]},'quantity':'123.40000000','optional_text':None,'event_date':'2026-11-09','event_time':'2026-11-09 01:02:03.000004','delete_flag':0}]
    for t,rows in result.items():
        for r in rows:
            for k,v in [('run_id',1),('document_id',2),('version_id',3),('extraction_id',5)]: r[k]=format(v,'064x')
    return result

def write_synthetic_package(directory, ddl_sha256):
    """Local fixture only: never read a private production import package."""
    rows_by_table=package()
    manifest={'ddl_sha256':ddl_sha256,'table_order':list(m.TABLES),'tables':{},
              'total_rows':sum(map(len,rows_by_table.values()))}
    for table,rows in rows_by_table.items():
        data=''.join(json.dumps(row,ensure_ascii=False,sort_keys=True)+'\n' for row in rows).encode('utf-8')
        name=table+'.jsonl'
        (directory/name).write_bytes(data)
        manifest['tables'][table]={'file':name,'sha256':hashlib.sha256(data).hexdigest(),'row_count':len(rows)}
    (directory/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
    return rows_by_table


class Store:
    def __init__(self):
        self.tables={t:[] for t in m.TABLES}; self.meta=metadata(); self.connections=[]; self.queries=[]; self.commit_fail=None; self.after_read_mutator=None; self.release_fail=False
    def connect(self):
        x=Connection(self); self.connections.append(x); return x
class Connection:
    def __init__(self,store): self.s=store; self.tx=None; self.closed=False
    def cursor(self): return Cursor(self)
    def commit(self):
        if self.s.commit_fail=='before': raise OSError('synthetic secret must not leak')
        if self.tx is not None: self.s.tables=copy.deepcopy(self.tx)
        self.tx=None
        if self.s.commit_fail=='after': raise OSError('synthetic secret must not leak')
    def rollback(self): self.tx=None
    def close(self): self.closed=True; self.tx=None
class Cursor:
    def __init__(self,c): self.c=c; self.rows=[]; self.description=[]
    def __enter__(self): return self
    def __exit__(self,*args): return False
    def execute(self,sql,args=None):
        self.c.s.queries.append((sql,args)); self.rows=[]
        if sql.startswith('SET '): return
        if sql.startswith('START TRANSACTION'): self.c.tx=copy.deepcopy(self.c.s.tables); return
        if sql=='SELECT DATABASE()': self.rows=[('go_test_synthetic',)]; return
        if sql=='SELECT @@SESSION.sql_mode': self.rows=[('STRICT_TRANS_TABLES',)]; return
        if sql=='SELECT @@SESSION.foreign_key_checks': self.rows=[(1,)]; return
        if 'RELEASE_LOCK' in sql and self.c.s.release_fail: raise OSError('synthetic release failure')
        if 'GET_LOCK' in sql or 'RELEASE_LOCK' in sql: self.rows=[(1,)]; return
        if 'information_schema.COLUMNS' in sql:
            self.rows=[x for t in m.TABLES for x in self.c.s.meta[t]]; return
        if 'information_schema.TABLES' in sql:
            self.rows=[{'TABLE_NAME':t,'ENGINE':'InnoDB','ROW_FORMAT':'Dynamic','TABLE_COLLATION':'utf8mb4_bin'} for t in m.TABLES]; return
        table=re.search(r'`go_test_synthetic`\.`([a-z_]+)`',sql).group(1)
        data=self.c.tx if self.c.tx is not None else self.c.s.tables
        if sql.startswith('SELECT COUNT(*)'):
            self.rows=[(len(data[table]),)]; return
        if sql.startswith('SELECT '): self.rows=copy.deepcopy(sorted(data[table],key=lambda x:x[m.PK[table]])[:args[0]]); return
        if sql.startswith('INSERT INTO '):
            columns=re.search(r'\(([^)]+)\) VALUES',sql).group(1).replace('`','').split(',')
            row=dict(zip(columns,args))
            for col in self.c.s.meta[table]:
                k=col['COLUMN_NAME']
                if k not in row: row[k]=col['COLUMN_DEFAULT']
                if col['DATA_TYPE']=='decimal' and row[k] is not None: row[k]=Decimal(row[k])
                if col['DATA_TYPE']=='datetime' and row[k] is not None: row[k]=datetime.fromisoformat(row[k])
            row['create_time']=row['update_time']=datetime(2026,11,9,10,0,0)
            data[table].append(row); return
        raise AssertionError(sql)
    def fetchall(self): return self.rows
    def fetchone(self): return self.rows[0]

class CoreTests(unittest.TestCase):
    def test_schema_allowlist(self):
        for x in ('mysql','information_schema','lingxing;DROP DATABASE x','go_test_bad-name'):
            with self.assertRaises(m.LoadContractError): m.allowed_schema(x)
    def test_table_allowlist(self):
        with self.assertRaises(m.LoadContractError): m.qtable('lingxing','产品信息')
    def test_production_fault_is_forbidden_before_connection(self):
        with self.assertRaises(m.LoadContractError): m.load_initial(lambda: self.fail('connected'),'lingxing',package(),fault_after_table=m.TABLES[0])
    def test_normal_explicit_insert_independent_readback(self):
        s=Store(); r=m.load_initial(s.connect,'go_test_synthetic',package())
        self.assertEqual(r['status'],'COMMITTED_VERIFIED'); self.assertEqual(r['write_statements'],6)
        self.assertEqual(r['commit_attempts'],1); self.assertEqual(len(s.connections),2)
        self.assertTrue(all(c.closed for c in s.connections)); self.assertTrue(all(x['changed_fields']==0 for x in r['independent_readback']['tables'].values()))
        self.assertFalse(any(re.match(r'(DELETE|UPDATE|CREATE|DROP|ALTER|TRUNCATE)',q) for q,a in s.queries))
        self.assertTrue(all(' (' in q and ' VALUES (' in q for q,a in s.queries if q.startswith('INSERT')))
    def test_repeat_is_read_only_no_second_commit(self):
        s=Store(); m.load_initial(s.connect,'go_test_synthetic',package()); before=copy.deepcopy(s.tables)
        r=m.load_initial(s.connect,'go_test_synthetic',package())
        self.assertEqual(r['status'],'ALREADY_MATCHED_VERIFIED'); self.assertEqual(r['write_statements'],0); self.assertEqual(before,s.tables)
    def test_partial_target_blocks_without_overwrite(self):
        s=Store(); m.load_initial(s.connect,'go_test_synthetic',package()); s.tables[m.TABLES[2]]=[]; before=copy.deepcopy(s.tables)
        r=m.load_initial(s.connect,'go_test_synthetic',package())
        self.assertEqual(r['status'],'BLOCKED_NO_WRITE'); self.assertEqual(before,s.tables)
    def test_content_conflict_blocks(self):
        s=Store(); m.load_initial(s.connect,'go_test_synthetic',package()); s.tables[m.TABLES[1]][0]['quantity']=Decimal('999')
        r=m.load_initial(s.connect,'go_test_synthetic',package()); self.assertEqual(r['status'],'BLOCKED_NO_WRITE'); self.assertEqual(r['write_statements'],0)
    def test_mid_import_failure_rolls_back_new_tables(self):
        s=Store(); r=m.load_initial(s.connect,'go_test_synthetic',package(),fault_after_table=m.TABLES[3])
        self.assertEqual(r['status'],'ROLLED_BACK_VERIFIED'); self.assertEqual(r['commit_attempts'],0)
        self.assertTrue(all(not v for v in s.tables.values())); self.assertEqual(r['write_statements'],4)
    def test_commit_lost_after_persist_stays_unknown_but_resolves(self):
        s=Store(); s.commit_fail='after'; r=m.load_initial(s.connect,'go_test_synthetic',package())
        self.assertEqual(r['status'],'COMMIT_UNKNOWN'); self.assertEqual(r['commit_resolution'],'COMMITTED_VERIFIED')
        self.assertEqual(r['commit_attempts'],1); self.assertNotIn('synthetic secret',json.dumps(r))
    def test_commit_lost_before_persist_stays_unknown_not_rollback_claim(self):
        s=Store(); s.commit_fail='before'; r=m.load_initial(s.connect,'go_test_synthetic',package())
        self.assertEqual(r['status'],'COMMIT_UNKNOWN'); self.assertEqual(r['commit_resolution'],'NOT_COMMITTED_VERIFIED')
        self.assertTrue(all(not v for v in s.tables.values())); self.assertEqual(r['commit_attempts'],1)
    def test_duplicate_key_blocked(self):
        s=Store(); p=package(); p[m.TABLES[0]].append(copy.deepcopy(p[m.TABLES[0]][0])); r=m.load_initial(s.connect,'go_test_synthetic',p)
        self.assertEqual(r['status'],'BLOCKED_NO_WRITE'); self.assertEqual(r['reason_code'],'DUPLICATE_PRIMARY_KEY')
    def test_unknown_json_null_and_numeric_zero_distinct(self):
        self.assertIsNone(m._normalize(None,'json')); self.assertEqual(m._normalize('null','json'),{'json_value':None})
        self.assertEqual(m._normalize('{"x":null,"y":0}','json'),{'json_value':{'x':None,'y':0}})
        self.assertNotEqual(m._normalize(None,'decimal'),m._normalize('0','decimal'))
    def test_decimal_readback_precision(self):
        self.assertEqual(m._normalize('123.40000000','decimal'),m._normalize(Decimal('123.4'),'decimal'))
    def test_bad_audit_timestamp_not_ignored(self):
        s=Store(); m.load_initial(s.connect,'go_test_synthetic',package()); s.tables[m.TABLES[1]][0]['update_time']=datetime(2000,1,1)
        r=m.load_initial(s.connect,'go_test_synthetic',package()); self.assertEqual(r['status'],'BLOCKED_NO_WRITE')
    def test_package_auto_timestamp_override_forbidden(self):
        s=Store(); p=package(); p[m.TABLES[0]][0]['create_time']='2020-01-01 00:00:00.000000'
        r=m.load_initial(s.connect,'go_test_synthetic',p); self.assertEqual(r['status'],'BLOCKED_NO_WRITE')
    def test_orphan_graph_is_blocked_before_insert(self):
        s=Store(); p=package(); p[m.TABLES[5]][0]['extraction_id']='f'*64
        r=m.load_initial(s.connect,'go_test_synthetic',p)
        self.assertEqual(r['status'],'BLOCKED_NO_WRITE'); self.assertEqual(r['reason_code'],'ORPHAN_COLOR_EVIDENCE')
    def test_sql_null_and_json_null_are_not_equal(self):
        self.assertNotEqual(m._normalize(None,'json'),m._normalize('null','json'))
    def test_cleanup_error_does_not_hide_committed_receipt(self):
        s=Store(); s.release_fail=True
        r=m.load_initial(s.connect,'go_test_synthetic',package())
        self.assertEqual(r['status'],'COMMITTED_VERIFIED'); self.assertEqual(r['lock_release_error_type'],'OSError')
        self.assertIn('finished_at_utc',r)
    def test_superseded_package_ddl_hash_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            write_synthetic_package(Path(folder),'0'*64)
            with self.assertRaisesRegex(m.LoadContractError,'PACKAGE_DDL_HASH_MISMATCH'):
                m.load_package(folder)
    def test_synthetic_package_hashes_load_without_database(self):
        with tempfile.TemporaryDirectory() as folder:
            expected=write_synthetic_package(Path(folder),m.DDL_SHA256)
            p,manifest=m.load_package(folder)
            self.assertEqual(p,expected)
            self.assertEqual(sum(map(len,p.values())),manifest['total_rows'])
            self.assertEqual(set(p),set(m.TABLES))

if __name__=='__main__': unittest.main(verbosity=2)
