"""Synthetic append tests. No service, authentication, network or real DB access."""
import copy
from datetime import datetime
from decimal import Decimal
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
import append_load_core as m
import build_load_package as builder


def make_metadata():
    ddl = Path(os.environ['GARMENT_SOURCE_DDL']).read_text(encoding='utf-8-sig')
    schema = builder.parse_schema(ddl)
    result = {}
    for table in m.TABLES:
        cols = []
        for ordinal,(name,spec) in enumerate(schema[table]['columns'].items(),1):
            default = 0 if name == 'delete_flag' else ('UNKNOWN' if spec['has_default'] and not spec['nullable'] and name not in m.AUTO_TIME else None)
            cols.append({'TABLE_NAME':table,'COLUMN_NAME':name,'ORDINAL_POSITION':ordinal,
                         'DATA_TYPE':spec['base_type'].lower(),
                         'COLUMN_TYPE':spec['type'].lower()+(' unsigned' if spec['unsigned'] else ''),
                         'IS_NULLABLE':'YES' if spec['nullable'] else 'NO','COLUMN_DEFAULT':default,
                         'EXTRA':'','CHARACTER_SET_NAME':None,'COLLATION_NAME':None})
        result[table] = cols
    return result

META = make_metadata()

def ident(i):
    return format(i,'064x')


def blank_package():
    return {t:[] for t in m.TABLES}


def make_package(seed=100):
    result = blank_package()
    for i,t in enumerate(m.TABLES):
        row = {}
        for col in META[t]:
            name, dt = col['COLUMN_NAME'], col['DATA_TYPE']
            if name in m.AUTO_TIME:
                continue
            if name == 'delete_flag': value = 0
            elif col['IS_NULLABLE']=='YES': value = None
            elif name.endswith('_id') or name.endswith('_sha256'): value = ident(seed+i)
            elif dt == 'json': value = {}
            elif dt in ('int','tinyint','bigint'): value = 1
            elif dt == 'datetime': value = '2026-01-01 00:00:00.000000'
            elif dt == 'date': value = '2026-01-01'
            elif dt == 'decimal': value = '1.00000000'
            else: value = 'synthetic'
            row[name] = value
        row[m.PK[t]] = ident(seed+i)
        result[t] = [row]
    rows = [result[t][0] for t in m.TABLES]
    run,doc,ver,obs,ext,order,material,conf,ref = rows
    doc.update(source_system='feishu_drive',resource_type='file',source_id='synthetic'+str(seed))
    ver.update(document_id=doc['document_id'],content_sha256=ident(seed+500),archive_uri='synthetic://archive/'+str(seed))
    obs.update(run_id=run['run_id'],document_id=doc['document_id'],version_id=ver['version_id'])
    ext.update(run_id=run['run_id'],version_id=ver['version_id'],attempt_no=1)
    order.update(extraction_id=ext['extraction_id'],version_id=ver['version_id'],source_line_id='line'+str(seed),quantity='12.34000000')
    material.update(extraction_id=ext['extraction_id'],version_id=ver['version_id'],order_evidence_id=order['evidence_id'],source_line_id=order['source_line_id'],component_locator='main')
    conf.update(review_id='synthetic-review'+str(seed),confirmation_version=1,recorded_at_utc='2026-11-10 01:00:00.000000',user_confirmed_at_utc=None,user_time_status='UNKNOWN')
    ref.update(confirmation_id=conf['confirmation_id'],document_id=doc['document_id'],version_id=ver['version_id'],order_evidence_id=order['evidence_id'],material_evidence_id=material['material_evidence_id'])
    return result


def baseline():
    data = make_package(1)
    for t in m.TABLES[6:]: data[t]=[]
    for rows in data.values():
        for row in rows:
            row['create_time']=datetime(2026,1,1,1,2,3,4)
            row['update_time']=datetime(2026,1,2,1,2,3,4)
    return data


class Store:
    def __init__(self):
        self.rows=baseline(); self.calls=[]; self.connections=[]; self.lock_owner=None
        self.commit_mode='normal'; self.fail_independent=False
        self.corrupt_new=False; self.mutate_old_audit=False
        self.commit_count=0; self.insert_count=0
    def connect(self):
        if self.fail_independent and self.connections:
            raise RuntimeError('synthetic independent failure')
        con=Connection(self); self.connections.append(con); return con


class Connection:
    def __init__(self,store):
        self.store=store; self.tx=None; self.closed=False
    def cursor(self): return Cursor(self)
    def rollback(self): self.tx=None
    def commit(self):
        self.store.commit_count+=1
        if self.store.commit_mode=='before_error': raise RuntimeError('synthetic lost-before-commit')
        self.store.rows=copy.deepcopy(self.tx); self.tx=None
        if self.store.commit_mode=='after_error': raise RuntimeError('synthetic ack-loss')
    def close(self):
        self.tx=None; self.closed=True
        if self.store.lock_owner is self: self.store.lock_owner=None


class Cursor:
    def __init__(self,conn): self.conn=conn; self.rows=[]; self.description=None
    def __enter__(self): return self
    def __exit__(self,*args): return False
    def fetchall(self): return copy.deepcopy(self.rows)
    def fetchone(self): return copy.deepcopy(self.rows[0]) if self.rows else None
    def execute(self,sql,args=None):
        s=self.conn.store; s.calls.append((sql,args)); self.rows=[]
        if sql.startswith('SET '): return
        if sql.startswith('START TRANSACTION'):
            self.conn.tx=copy.deepcopy(s.rows); return
        if sql=='SELECT DATABASE()': self.rows=[{'v':'go_test_synthetic'}]; return
        if sql=='SELECT @@SESSION.sql_mode': self.rows=[{'v':'STRICT_TRANS_TABLES'}]; return
        if sql=='SELECT @@SESSION.foreign_key_checks': self.rows=[{'v':1}]; return
        if sql.startswith('SELECT GET_LOCK'):
            ok=s.lock_owner is None or s.lock_owner is self.conn
            if ok:s.lock_owner=self.conn
            self.rows=[{'v':int(ok)}]; return
        if sql.startswith('SELECT RELEASE_LOCK'):
            if s.lock_owner is self.conn:s.lock_owner=None
            self.rows=[{'v':1}]; return
        if 'information_schema.COLUMNS' in sql:
            self.rows=[copy.deepcopy(col) for t in m.TABLES for col in META[t]];return
        if 'information_schema.TABLES' in sql:
            self.rows=[{'TABLE_NAME':t,'ENGINE':'InnoDB','ROW_FORMAT':'Dynamic','TABLE_COLLATION':'utf8mb4_bin'} for t in m.TABLES];return
        if sql.startswith('SELECT '):
            table=re.search(r'FROM `go_test_synthetic`\.`([a-z_]+)`',sql).group(1)
            data=self.conn.tx if self.conn.tx is not None else s.rows
            self.rows=sorted(copy.deepcopy(data[table]),key=lambda r:r[m.PK[table]])[:args[0]];return
        if sql.startswith('INSERT INTO '):
            assert self.conn.tx is not None
            table,cols=re.search(r'INSERT INTO `go_test_synthetic`\.`([a-z_]+)` \((.*?)\)',sql).groups()
            keys=re.findall(r'`([a-z_0-9]+)`',cols)
            row={x['COLUMN_NAME']:m._default_value(x) for x in META[table]}
            row.update(zip(keys,args))
            row['create_time']=row['update_time']=datetime(2026,11,10,2,0,0,s.insert_count)
            self.conn.tx[table].append(row);s.insert_count+=1
            if s.corrupt_new and table==m.TABLES[5]:row['quantity']='999'
            if s.mutate_old_audit:self.conn.tx[m.TABLES[0]][0]['update_time']=datetime(2026,11,10,3,0,0)
            return
        raise AssertionError('Unexpected SQL '+sql)


class AppendTests(unittest.TestCase):
    def setUp(self):
        self.store=Store();self.before=copy.deepcopy(self.store.rows);self.pkg=make_package()
    def run_load(self,**kw):
        return m.load_append(self.store.connect,'go_test_synthetic',self.pkg,self.before,**kw)
    def assert_blocked(self,result,reason):
        self.assertEqual(result['status'],'BLOCKED_NO_WRITE',result)
        self.assertIn(reason,result.get('reason_code',''))
        self.assertEqual(self.store.insert_count,0)
        self.assertEqual(self.store.rows,self.before)
    def test_all_nine_tables_append_and_preserve_old_audit(self):
        r=self.run_load();self.assertEqual(r['status'],'COMMITTED_VERIFIED',r)
        self.assertEqual(r['inserted_rows_attempted'],9)
        for t in m.TABLES:
            for old in self.before[t]:self.assertIn(old,self.store.rows[t])
        self.assertTrue(all(x['old_fields_changed_including_audit']==0 for x in r['independent_readback']['tables'].values()))
        self.assertEqual(r['ddl']+r['update_statements']+r['delete_statements'],0)
    def test_repeat_skips_identical_rows_no_second_commit(self):
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.before=copy.deepcopy(self.store.rows)
        r=self.run_load();self.assertEqual(r['status'],'ALREADY_MATCHED_VERIFIED',r)
        self.assertEqual(r['write_statements'],0);self.assertEqual(self.store.commit_count,1)
    def test_stale_expected_before_blocks_even_when_new_rows_exist(self):
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        r=self.run_load();self.assertEqual(r['status'],'BLOCKED_NO_WRITE')
        self.assertIn('TARGET_CHANGED',r['reason_code'])
        self.assertEqual(self.store.commit_count,1)
    def test_existing_same_pk_different_content_blocks(self):
        old=copy.deepcopy(self.before[m.TABLES[1]][0]);old.pop('create_time');old.pop('update_time')
        old['source_id']='changed';self.pkg[m.TABLES[1]].append(old)
        self.assert_blocked(self.run_load(),'EXISTING_PRIMARY_KEY_CONTENT_CONFLICT')
    def test_existing_identical_document_is_reference_not_duplicate(self):
        old=copy.deepcopy(self.before[m.TABLES[1]][0]);old.pop('create_time');old.pop('update_time')
        self.pkg[m.TABLES[1]].append(old)
        r=self.run_load();self.assertEqual(r['status'],'COMMITTED_VERIFIED',r)
        self.assertEqual(r['existing_identical_rows'][m.TABLES[1]],1)
        self.assertEqual(len(self.store.rows[m.TABLES[1]]),2)
    def test_natural_document_identity_conflict_blocks(self):
        self.pkg[m.TABLES[1]][0]['source_id']=self.before[m.TABLES[1]][0]['source_id']
        self.assert_blocked(self.run_load(),'NATURAL_UNIQUE_KEY_CONFLICT')
    def test_same_hash_different_document_preserved(self):
        self.pkg[m.TABLES[2]][0]['content_sha256']=self.before[m.TABLES[2]][0]['content_sha256']
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.assertEqual(len(self.store.rows[m.TABLES[2]]),2)
    def test_duplicate_content_version_same_document_blocks(self):
        v=self.pkg[m.TABLES[2]][0];old=self.before[m.TABLES[2]][0]
        v.update(document_id=old['document_id'],content_sha256=old['content_sha256'])
        self.assert_blocked(self.run_load(),'NATURAL_UNIQUE_KEY_CONFLICT')
    def test_duplicate_package_pk_blocks(self):
        self.pkg[m.TABLES[5]].append(copy.deepcopy(self.pkg[m.TABLES[5]][0]))
        self.assert_blocked(self.run_load(),'DUPLICATE_PACKAGE_PRIMARY_KEY')
    def test_cross_version_material_reference_blocks(self):
        self.pkg[m.TABLES[6]][0]['order_evidence_id']=self.before[m.TABLES[5]][0]['evidence_id']
        self.assert_blocked(self.run_load(),'ORPHAN_OR_CROSS_VERSION_REFERENCE')
    def test_orphan_confirmation_reference_blocks(self):
        self.pkg[m.TABLES[8]][0]['confirmation_id']=ident(99999)
        self.assert_blocked(self.run_load(),'ORPHAN_OR_CROSS_VERSION_REFERENCE')
    def test_confirmation_topological_order_preserves_pk(self):
        first=self.pkg[m.TABLES[7]][0];second=copy.deepcopy(first)
        second.update(confirmation_id=ident(999),confirmation_version=2,supersedes_confirmation_id=first['confirmation_id'])
        self.pkg[m.TABLES[7]]=[second,first]
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.assertEqual([x['confirmation_id'] for x in self.store.rows[m.TABLES[7]]],[first['confirmation_id'],second['confirmation_id']])
    def test_confirmation_nonincreasing_version_blocks(self):
        first=self.pkg[m.TABLES[7]][0];second=copy.deepcopy(first)
        second.update(confirmation_id=ident(999),confirmation_version=2)
        first.update(supersedes_confirmation_id=second['confirmation_id'])
        self.pkg[m.TABLES[7]]=[first,second]
        self.assert_blocked(self.run_load(),'CONFIRMATION_VERSION_NOT_INCREASING')
    def test_unknown_user_time_and_part_remain_null(self):
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        c=self.store.rows[m.TABLES[7]][0];mat=self.store.rows[m.TABLES[6]][0]
        self.assertIsNone(c['user_confirmed_at_utc']);self.assertIsNone(mat['part_raw'])
        self.assertIsNone(mat['future_default_confirmed'])
    def test_decimal_roundtrip_without_float(self):
        self.pkg[m.TABLES[5]][0]['quantity']='0.12345678'
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.assertEqual(self.store.rows[m.TABLES[5]][-1]['quantity'],'0.12345678')
    def test_zero_and_null_quantities_distinct(self):
        self.pkg[m.TABLES[5]][0]['quantity']=None
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.assertIsNone(self.store.rows[m.TABLES[5]][-1]['quantity'])
    def test_native_json_string_scalar_roundtrip(self):
        self.pkg[m.TABLES[7]][0]['remaining_json']='需要核验，不是数组'
        r=self.run_load();self.assertEqual(r['status'],'COMMITTED_VERIFIED',r)
        raw=self.store.rows[m.TABLES[7]][0]['remaining_json']
        self.assertEqual(json.loads(raw),'需要核验，不是数组')
    def test_native_json_numeric_and_array_roundtrip(self):
        self.pkg[m.TABLES[7]][0]['remaining_json']=3.5
        self.pkg[m.TABLES[6]][0]['candidates_json']=['synthetic',None,0,{'known':False}]
        r=self.run_load();self.assertEqual(r['status'],'COMMITTED_VERIFIED',r)
        self.assertEqual(json.loads(self.store.rows[m.TABLES[7]][0]['remaining_json']),3.5)
        self.assertEqual(json.loads(self.store.rows[m.TABLES[6]][0]['candidates_json']),['synthetic',None,0,{'known':False}])
    def test_json_literal_looking_strings_stay_strings(self):
        for value in ('null','0','[]','true','{"example":1}'):
            with self.subTest(value=value):
                self.setUp()
                self.pkg[m.TABLES[7]][0]['remaining_json']=value
                r=self.run_load();self.assertEqual(r['status'],'COMMITTED_VERIFIED',r)
                self.assertEqual(json.loads(self.store.rows[m.TABLES[7]][0]['remaining_json']),value)
    def test_sql_null_json_null_and_string_null_are_distinct(self):
        col={'COLUMN_NAME':'nullable_json','DATA_TYPE':'json','COLUMN_DEFAULT':None}
        self.assertEqual(m.normalize_row({'nullable_json':None},[col],audit=False,native_json=True),{'nullable_json':None})
        self.assertEqual(m.normalize_row({'nullable_json':'null'},[col],audit=True),{'nullable_json':{'json_value':None}})
        self.assertEqual(m.normalize_row({'nullable_json':'null'},[col],audit=False,native_json=True),{'nullable_json':{'json_value':'null'}})
        self.assertEqual(m.normalize_row({'nullable_json':'"null"'},[col],audit=True),{'nullable_json':{'json_value':'null'}})
    def test_existing_db_json_string_matches_native_package_string(self):
        self.pkg[m.TABLES[7]][0]['remaining_json']='synthetic pending'
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.before=copy.deepcopy(self.store.rows)
        r=self.run_load();self.assertEqual(r['status'],'ALREADY_MATCHED_VERIFIED',r)
    def test_nonfinite_native_json_rejected(self):
        self.pkg[m.TABLES[7]][0]['remaining_json']=float('nan')
        r=self.run_load();self.assertEqual(r['status'],'BLOCKED_NO_WRITE',r)
        self.assertEqual(self.store.insert_count,0)
    def test_audit_override_forbidden(self):
        self.pkg[m.TABLES[1]][0]['update_time']='2026-01-01 00:00:00'
        self.assert_blocked(self.run_load(),'PACKAGE_MUST_NOT_OVERRIDE_AUDIT_TIMES')
    def test_target_change_before_locked_read_blocks(self):
        self.store.rows[m.TABLES[1]][0]['source_id']='concurrent-change'
        r=self.run_load();self.assertEqual(r['status'],'BLOCKED_NO_WRITE')
        self.assertIn('TARGET_CHANGED_SINCE_FROZEN_BASELINE',r['reason_code']);self.assertEqual(self.store.insert_count,0)
    def test_fault_after_partial_insert_restores_old_snapshot(self):
        r=self.run_load(fault_after_table=m.TABLES[5])
        self.assertEqual(r['status'],'ROLLED_BACK_VERIFIED',r)
        self.assertEqual(self.store.rows,self.before);self.assertEqual(self.store.commit_count,0)
    def test_in_transaction_new_field_mismatch_rolls_back(self):
        self.store.corrupt_new=True;r=self.run_load()
        self.assertEqual(r['status'],'ROLLED_BACK_VERIFIED',r)
        self.assertEqual(self.store.rows,self.before)
    def test_in_transaction_old_audit_change_rolls_back(self):
        self.store.mutate_old_audit=True;r=self.run_load()
        self.assertEqual(r['status'],'ROLLED_BACK_VERIFIED',r)
        self.assertEqual(self.store.rows,self.before)
    def test_commit_ack_loss_resolved_to_committed_no_retry(self):
        self.store.commit_mode='after_error';r=self.run_load()
        self.assertEqual(r['status'],'COMMIT_UNKNOWN',r)
        self.assertEqual(r['commit_resolution'],'COMMITTED_VERIFIED')
        self.assertEqual(self.store.commit_count,1);self.assertEqual(self.store.insert_count,9)
    def test_commit_not_sent_resolved_to_before_no_retry(self):
        self.store.commit_mode='before_error';r=self.run_load()
        self.assertEqual(r['status'],'COMMIT_UNKNOWN',r)
        self.assertEqual(r['commit_resolution'],'NOT_COMMITTED_VERIFIED')
        self.assertEqual(self.store.rows,self.before);self.assertEqual(self.store.commit_count,1)
    def test_post_commit_read_failure_not_reported_rollback(self):
        self.store.fail_independent=True;r=self.run_load()
        self.assertEqual(r['status'],'COMMITTED_READBACK_FAILED',r)
        self.assertEqual(self.store.commit_count,1)
    def test_lock_busy_blocks_before_write(self):
        self.store.lock_owner='other';self.assert_blocked(self.run_load(),'IMPORT_LOCK_BUSY')
    def test_uses_same_lock_namespace_and_bounded_range_locks(self):
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        locks=[args[0] for sql,args in self.store.calls if sql.startswith('SELECT GET_LOCK')]
        self.assertTrue(locks[0].startswith('garment_initial:'))
        sqls=[sql for sql,args in self.store.calls if sql.endswith('FOR UPDATE')]
        self.assertEqual(len(sqls),9)
        self.assertTrue(all('ORDER BY' in sql and 'LIMIT %s' in sql for sql in sqls))
    def test_empty_target_tables_still_range_locked(self):
        self.store.rows=blank_package();self.before=blank_package()
        self.assertEqual(self.run_load()['status'],'COMMITTED_VERIFIED')
        self.assertEqual(sum(sql.endswith('FOR UPDATE') for sql,args in self.store.calls),9)
    def test_no_blind_overwrite_sql(self):
        self.run_load()
        for sql,args in self.store.calls:
            self.assertFalse(sql.startswith(('UPDATE ','DELETE ','TRUNCATE ','ALTER ','CREATE ','REPLACE ')))
            self.assertNotIn('INSERT IGNORE',sql)
            self.assertNotIn('ON DUPLICATE',sql)
    def test_malicious_schema_rejected_before_connect(self):
        with self.assertRaises(m.core.LoadContractError):
            m.load_append(self.store.connect,'lingxing;bad',self.pkg,self.before)
        self.assertFalse(self.store.connections)
    def test_production_fault_injection_forbidden(self):
        with self.assertRaises(m.core.LoadContractError):
            m.load_append(self.store.connect,'lingxing',self.pkg,self.before,fault_after_table=m.TABLES[0])
        self.assertFalse(self.store.connections)
    def test_incomplete_baseline_columns_block(self):
        self.before[m.TABLES[0]][0].pop('update_time')
        r=self.run_load();self.assertEqual(r['status'],'BLOCKED_NO_WRITE')
        self.assertIn('BASELINE_COLUMN_SET_MISMATCH',r['reason_code'])
    def test_result_row_budget_applies_before_write(self):
        old=m.MAX_ROWS
        try:
            m.MAX_ROWS=7
            self.assert_blocked(self.run_load(),'PACKAGE_ROW_BUDGET_EXCEEDED')
        finally:m.MAX_ROWS=old
    def test_snapshot_budget_rejects_extra_row(self):
        conn=self.store.connect()
        with self.assertRaises(m.core.LoadContractError):
            m.read_snapshot(conn,'go_test_synthetic',META,max_rows=5)
        conn.close()

if __name__=='__main__':unittest.main()
