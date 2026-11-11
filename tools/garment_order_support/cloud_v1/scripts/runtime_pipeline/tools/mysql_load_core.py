"""Initial garment-order package INSERT and independent read-back. No credentials or CLI.

Call only with an externally verified connection factory. This module never creates,
alters, deletes or updates tables. Tests use explicit fakes; production forbids faults.
"""
from __future__ import annotations
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

VERSION = '1.0.1'
TABLES = (
 'ods_it_garment_order_ingestion_run', 'dim_sm_garment_order_document',
 'ods_sm_garment_order_document_version', 'ods_sm_garment_order_document_observation',
 'ods_it_garment_order_extraction_run', 'dwd_sm_garment_order_color_evidence',
 'dwd_sm_garment_material_color_evidence', 'ods_manual_sm_garment_color_confirmation',
 'dwd_sm_garment_confirmation_evidence_ref')
PK = dict(zip(TABLES, ('run_id','document_id','version_id','observation_id','extraction_id','evidence_id','material_evidence_id','confirmation_id','reference_id')))
AUTO_TIME = {'create_time','update_time'}
DDL_SHA256 = '20cfeb64ac250bfd9dfc4fc740898bdf3e63a262fbb1d3f039f1e0f84361a4c1'

class LoadContractError(ValueError):
    pass

class InjectedIsolationFailure(RuntimeError):
    pass

def require(value, code):
    if not value:
        raise LoadContractError(code)

def now():
    return datetime.now(timezone.utc).isoformat()

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)

def sha(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()

def allowed_schema(schema):
    require(schema == 'lingxing' or re.fullmatch(r'go_test_[a-z0-9_]{1,48}',schema), 'SCHEMA_NOT_ALLOWLISTED')
    return schema

def identifier(value):
    require(re.fullmatch(r'[a-z][a-z0-9_]{0,63}',value), 'INVALID_IDENTIFIER')
    return '`'+value+'`'

def qtable(schema, table):
    allowed_schema(schema)
    require(table in TABLES, 'TABLE_NOT_ALLOWLISTED')
    return identifier(schema)+'.'+identifier(table)

def _dict_rows(cursor):
    rows = cursor.fetchall()
    if not rows: return []
    if isinstance(rows[0],dict): return rows
    names=[x[0] for x in cursor.description]
    return [dict(zip(names,row)) for row in rows]

def scalar(cursor, sql, args=None):
    cursor.execute(sql,args)
    row=cursor.fetchone()
    return next(iter(row.values())) if isinstance(row,dict) else row[0]

def load_package(directory):
    root=Path(directory).resolve()
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8-sig'))
    require(manifest.get('ddl_sha256')==DDL_SHA256,'PACKAGE_DDL_HASH_MISMATCH')
    require(tuple(manifest['table_order'])==TABLES,'PACKAGE_TABLE_ORDER')
    package={}
    for table in TABLES:
        info=manifest['tables'][table]
        p=(root/info['file']).resolve()
        require(p.parent==root,'PACKAGE_FILE_OUTSIDE_DIRECTORY')
        content=p.read_bytes()
        require(hashlib.sha256(content).hexdigest()==info['sha256'],'PACKAGE_FILE_HASH_MISMATCH')
        rows=[json.loads(line) for line in content.decode('utf-8-sig').splitlines() if line.strip()]
        require(len(rows)==info['row_count'],'PACKAGE_ROW_COUNT_MISMATCH')
        package[table]=rows
    return package, manifest

def schema_metadata(connection,schema):
    allowed_schema(schema)
    with connection.cursor() as c:
        placeholders=','.join(['%s']*len(TABLES))
        c.execute('SELECT TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME IN ('+placeholders+') ORDER BY TABLE_NAME,ORDINAL_POSITION',(schema,*TABLES))
        columns=_dict_rows(c)
        c.execute('SELECT TABLE_NAME,ENGINE,ROW_FORMAT,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME IN ('+placeholders+')',(schema,*TABLES))
        tables=_dict_rows(c)
    by_table={table:[] for table in TABLES}
    for row in columns: by_table[row['TABLE_NAME']].append(row)
    require(set(x['TABLE_NAME'] for x in tables)==set(TABLES),'NINE_TABLES_REQUIRED')
    require(all(x['ENGINE']=='InnoDB' for x in tables),'NON_TRANSACTIONAL_TABLE')
    for table in TABLES:
        require(by_table[table],'MISSING_TABLE_COLUMNS')
        names={x['COLUMN_NAME'] for x in by_table[table]}
        require(PK[table] in names and AUTO_TIME <= names,'KEY_OR_AUDIT_COLUMNS_MISSING')
    return by_table

def _normalize(value, data_type):
    if value is None: return None
    if data_type=='json':
        if isinstance(value,(str,bytes)): value=json.loads(value)
        # JSON preserves missing vs null. Canonical key order only.
        return {'json_value':value}
    if data_type in ('decimal','numeric'):
        d=Decimal(str(value)); require(d.is_finite(),'NONFINITE_DECIMAL')
        return format(d.normalize(),'f') if d else '0'
    if data_type in ('datetime','timestamp'):
        if isinstance(value,str): value=datetime.fromisoformat(value)
        require(isinstance(value,datetime),'BAD_DATETIME')
        require(value.tzinfo is None,'EXPECTED_NAIVE_UTC_DATETIME')
        return value.isoformat(sep=' ',timespec='microseconds')
    if data_type=='date':
        if isinstance(value,str): value=date.fromisoformat(value)
        return value.isoformat()
    if data_type in ('tinyint','smallint','mediumint','int','bigint'):
        require(isinstance(value,(int,bool,Decimal)),'BAD_INTEGER')
        require(int(value)==value,'FRACTIONAL_INTEGER')
        return int(value)
    return value

def validate_package(package, metadata):
    require(tuple(package)==TABLES,'PACKAGE_SCOPE_OR_ORDER')
    for table in TABLES:
        rows=package[table]; fields={x['COLUMN_NAME']:x for x in metadata[table]}
        require(len(rows)<=100000,'INITIAL_PACKAGE_TOO_LARGE')
        keys=set()
        for row in rows:
            require(not (set(row)-set(fields)),'UNKNOWN_PACKAGE_COLUMN')
            require(not (set(row)&AUTO_TIME),'PACKAGE_MUST_NOT_OVERRIDE_AUDIT_TIMES')
            require(PK[table] in row,'MISSING_PRIMARY_KEY')
            key=row[PK[table]]
            require(key not in keys,'DUPLICATE_PRIMARY_KEY'); keys.add(key)
            require(re.fullmatch('[0-9a-f]{64}',str(key)),'INVALID_PRIMARY_KEY')
            for name,col in fields.items():
                if name in AUTO_TIME: continue
                if name not in row:
                    require(col['IS_NULLABLE']=='YES' or col['COLUMN_DEFAULT'] is not None,'MISSING_REQUIRED_COLUMN')
                elif row[name] is None:
                    require(col['IS_NULLABLE']=='YES','NULL_REQUIRED_COLUMN')
                else:
                    _normalize(row[name],col['DATA_TYPE'])
            require(row.get('delete_flag',0)==0,'DELETED_SOURCE_NOT_INITIAL_IMPORT')
        if rows:
            require(all(set(r)==set(rows[0]) for r in rows),'INCONSISTENT_ROW_COLUMNS')
    require(all(not package[t] for t in TABLES[6:]),'MATERIAL_MANUAL_IMPORT_OUT_OF_SCOPE')
    # The frozen first package has no material/manual rows. Its six-table
    # relationship graph is checked before any INSERT, in addition to MySQL FKs.
    require(len(package[TABLES[0]])==1,'ONE_SOURCE_RUN_REQUIRED')
    runs={r['run_id'] for r in package[TABLES[0]]}
    docs={r['document_id'] for r in package[TABLES[1]]}
    version_pairs={(r['version_id'],r['document_id']) for r in package[TABLES[2]]}
    versions={v for v,d in version_pairs}
    extraction_pairs={(r['extraction_id'],r['version_id']) for r in package[TABLES[4]]}
    for r in package[TABLES[2]]: require(r['document_id'] in docs,'ORPHAN_CONTENT_VERSION')
    for r in package[TABLES[3]]:
        require(r['run_id'] in runs and (r['version_id'],r['document_id']) in version_pairs,'ORPHAN_DOCUMENT_OBSERVATION')
    for r in package[TABLES[4]]:
        require(r['run_id'] in runs and r['version_id'] in versions,'ORPHAN_EXTRACTION')
    for r in package[TABLES[5]]:
        require((r['extraction_id'],r['version_id']) in extraction_pairs,'ORPHAN_COLOR_EVIDENCE')

def _expected_row(row, columns):
    result={}
    for col in columns:
        name=col['COLUMN_NAME']
        if name in AUTO_TIME: continue
        value=row[name] if name in row else col['COLUMN_DEFAULT']
        if name not in row and col['IS_NULLABLE']=='YES' and value is None: value=None
        result[name]=_normalize(value,col['DATA_TYPE'])
    return result

def readback(connection,schema,package,metadata):
    """Bounded full payload comparison; report contains hashes/counts, not business rows."""
    table_results={}; all_match=True; all_empty=True
    with connection.cursor() as c:
        for table in TABLES:
            columns=metadata[table]; names=[x['COLUMN_NAME'] for x in columns]
            expected={row[PK[table]]:_expected_row(row,columns) for row in package[table]}
            count=int(scalar(c,'SELECT COUNT(*) FROM '+qtable(schema,table)))
            all_empty = all_empty and count==0
            # Never scan unexpected large tables. Only expected + one rows maximum.
            c.execute('SELECT '+','.join(identifier(x) for x in names)+' FROM '+qtable(schema,table)+' ORDER BY '+identifier(PK[table])+' LIMIT %s',(len(expected)+1,))
            actual_rows=_dict_rows(c); actual={}; audit_ok=True
            for row in actual_rows:
                ct,ut=row['create_time'],row['update_time']
                try:
                    ct=_normalize(ct,'datetime'); ut=_normalize(ut,'datetime')
                    audit_ok=audit_ok and ct is not None and ut is not None and ut>=ct
                except (ValueError,TypeError): audit_ok=False
                actual[row[PK[table]]]={col['COLUMN_NAME']:_normalize(row[col['COLUMN_NAME']],col['DATA_TYPE']) for col in columns if col['COLUMN_NAME'] not in AUTO_TIME}
            common=set(expected)&set(actual)
            changed_fields=sum(sum(expected[key][col]!=actual[key][col] for col in expected[key]) for key in common)
            matched=count==len(expected) and set(expected)==set(actual) and changed_fields==0 and audit_ok
            all_match=all_match and matched
            table_results[table]={'expected_rows':len(expected),'actual_rows':count,'missing_keys':len(set(expected)-set(actual)),'extra_keys_in_bounded_read':len(set(actual)-set(expected)),'changed_fields':changed_fields,'audit_times_valid':audit_ok,'bounded_rows_read':len(actual_rows),'expected_payload_sha256':sha(expected),'readback_payload_sha256':sha(actual),'matched':matched}
    return {'state':'MATCH' if all_match else ('EMPTY' if all_empty else 'CONFLICT'),'tables':table_results}

def _new_readback(connect_factory,schema,package,metadata):
    conn=connect_factory()
    try:
        with conn.cursor() as c:
            c.execute("SET SESSION time_zone='+00:00'")
            require(scalar(c,'SELECT DATABASE()')==schema,'CONNECTION_DATABASE_MISMATCH')
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            c.execute('START TRANSACTION READ ONLY')
        result=readback(conn,schema,package,metadata)
        conn.rollback()
        return result
    finally:
                try: conn.close()
                except Exception: pass

def load_initial(connect_factory,schema,package,*,fault_after_table=None):
    """One INSERT transaction, independent read-back; never retries writes.

    fault_after_table: isolation-only after a named table has inserted, before commit.
    connect_factory must return a *new*, verified-target PyMySQL connection per call.
    """
    allowed_schema(schema)
    if fault_after_table is not None:
        require(schema.startswith('go_test_') and fault_after_table in TABLES,'FAULT_FORBIDDEN_OUTSIDE_ISOLATION')
    receipt={'loader_version':VERSION,'schema':schema,'started_at_utc':now(),'status':'NOT_RUN','write_statements':0,'inserted_rows_attempted':0,'commit_attempts':0,'rollback_attempts':0,'fault_injection':fault_after_table,'ddl':0,'update_statements':0,'delete_statements':0}
    conn=None; locked=False; commit_started=False; metadata=None
    try:
        conn=connect_factory()
        with conn.cursor() as c:
            c.execute("SET SESSION time_zone='+00:00'")
            c.execute('SET SESSION innodb_lock_wait_timeout=10')
            c.execute('SET SESSION MAX_EXECUTION_TIME=10000')
            require(scalar(c,'SELECT DATABASE()')==schema,'CONNECTION_DATABASE_MISMATCH')
            strict_mode=str(scalar(c,'SELECT @@SESSION.sql_mode'))
            require('STRICT_TRANS_TABLES' in strict_mode.split(',') or 'STRICT_ALL_TABLES' in strict_mode.split(','),'STRICT_SQL_MODE_REQUIRED')
            require(scalar(c,'SELECT @@SESSION.foreign_key_checks')==1,'FOREIGN_KEY_CHECKS_REQUIRED')
            lock_name='garment_initial:'+hashlib.sha256(schema.encode()).hexdigest()[:32]
            require(scalar(c,'SELECT GET_LOCK(%s,0)',(lock_name,))==1,'IMPORT_LOCK_BUSY'); locked=True
        metadata=schema_metadata(conn,schema)
        validate_package(package,metadata)
        conn.rollback()
        with conn.cursor() as c:
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            c.execute('START TRANSACTION')
        receipt['before']=readback(conn,schema,package,metadata)
        if receipt['before']['state']=='MATCH':
            conn.rollback(); receipt['rollback_attempts']+=1
            receipt['independent_readback']=_new_readback(connect_factory,schema,package,metadata)
            require(receipt['independent_readback']['state']=='MATCH','ALREADY_MATCHED_CHANGED_DURING_READBACK')
            receipt['status']='ALREADY_MATCHED_VERIFIED'
            return receipt
        require(receipt['before']['state']=='EMPTY','NONEMPTY_PARTIAL_OR_CONFLICTING_TARGET')
        for table in TABLES:
            rows=package[table]
            if rows:
                columns=list(rows[0]); fields={x['COLUMN_NAME']:x for x in metadata[table]}
                sql='INSERT INTO '+qtable(schema,table)+' ('+','.join(identifier(x) for x in columns)+') VALUES ('+','.join(['%s']*len(columns))+')'
                with conn.cursor() as c:
                    # One row per call preserves bounded statement size and exact failure scope.
                    for row in rows:
                        vals=[canonical(row[k]) if row[k] is not None and fields[k]['DATA_TYPE']=='json' else row[k] for k in columns]
                        c.execute(sql,tuple(vals)); receipt['write_statements']+=1; receipt['inserted_rows_attempted']+=1
            if table==fault_after_table: raise InjectedIsolationFailure('AFTER_TABLE_INSERT')
        receipt['transaction_readback']=readback(conn,schema,package,metadata)
        require(receipt['transaction_readback']['state']=='MATCH','IN_TRANSACTION_PAYLOAD_MISMATCH')
        commit_started=True; receipt['commit_attempts']+=1
        try:
            conn.commit()
        except Exception as error:
            receipt['status']='COMMIT_UNKNOWN'; receipt['error_type']=type(error).__name__
            if error.args and isinstance(error.args[0],int): receipt['mysql_errno']=error.args[0]
            # Never claim rollback, retry INSERT, or trust the same broken connection.
            try:
                conn.close(); conn=None; locked=False
                receipt['independent_readback']=_new_readback(connect_factory,schema,package,metadata)
                state=receipt['independent_readback']['state']
                receipt['commit_resolution']='COMMITTED_VERIFIED' if state=='MATCH' else ('NOT_COMMITTED_VERIFIED' if state=='EMPTY' else 'UNRESOLVED_CONFLICT')
            except Exception as verify_error:
                receipt['commit_resolution']='UNRESOLVED_READBACK_FAILED'; receipt['readback_error_type']=type(verify_error).__name__
            return receipt
        receipt['independent_readback']=_new_readback(connect_factory,schema,package,metadata)
        receipt['status']='COMMITTED_VERIFIED' if receipt['independent_readback']['state']=='MATCH' else 'COMMITTED_READBACK_MISMATCH'
        return receipt
    except Exception as error:
        receipt['error_type']=type(error).__name__
        if isinstance(error,LoadContractError): receipt['reason_code']=str(error)
        elif error.args and isinstance(error.args[0],int): receipt['mysql_errno']=error.args[0]
        if conn and not commit_started:
            try:
                conn.rollback(); receipt['rollback_attempts']+=1
                if metadata and receipt['write_statements']:
                    receipt['independent_readback']=_new_readback(connect_factory,schema,package,metadata)
                    receipt['status']='ROLLED_BACK_VERIFIED' if receipt['independent_readback']['state']=='EMPTY' else 'ROLLBACK_VERIFICATION_CONFLICT'
                else: receipt['status']='BLOCKED_NO_WRITE'
            except Exception as rollback_error:
                receipt['status']='ROLLBACK_UNCONFIRMED'; receipt['rollback_error_type']=type(rollback_error).__name__
        elif commit_started:
            receipt['status']='COMMITTED_READBACK_FAILED'
        else: receipt['status']='BLOCKED_NO_WRITE'
        return receipt
    finally:
        if conn:
            try:
                if locked:
                    with conn.cursor() as c: c.execute('SELECT RELEASE_LOCK(%s)',('garment_initial:'+hashlib.sha256(schema.encode()).hexdigest()[:32],))
            except Exception as cleanup_error:
                receipt['lock_release_error_type']=type(cleanup_error).__name__
            finally:
                try: conn.close()
                except Exception as close_error: receipt['close_error_type']=type(close_error).__name__
        receipt['finished_at_utc']=now()

