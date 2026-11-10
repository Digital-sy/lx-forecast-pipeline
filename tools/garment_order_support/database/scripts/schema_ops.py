"""Reviewed DDL parser and bounded structural readback. No connection credentials."""
import hashlib,json,re
from datetime import datetime,timezone

TABLES=('ods_it_garment_order_ingestion_run','dim_sm_garment_order_document','ods_sm_garment_order_document_version','ods_sm_garment_order_document_observation','ods_it_garment_order_extraction_run','dwd_sm_garment_order_color_evidence','dwd_sm_garment_material_color_evidence','ods_manual_sm_garment_color_confirmation','dwd_sm_garment_confirmation_evidence_ref')
DDL_HASH='9b59997d300b61b6286487b55dfcaf64b9275fcc5ce6bdc0770518edc510c1d3'

def stamp(): return datetime.now(timezone.utc).isoformat()
def canonical(v): return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str)
def sha(v): return hashlib.sha256(canonical(v).encode()).hexdigest()
def require(ok,code):
    if not ok: raise ValueError(code)
def schema_ok(schema): require(schema=='lingxing' or re.fullmatch(r'go_test_[a-z0-9_]+',schema),'SCHEMA_NOT_ALLOWED')

def statements(raw,schema):
    schema_ok(schema); require(hashlib.sha256(raw).hexdigest()==DDL_HASH,'DDL_HASH_CHANGED')
    text=raw.decode('utf-8-sig')
    clean='\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    patt=r'CREATE TABLE `lingxing`\.`([a-z_]+)` \(.*?\n\) ENGINE=[^\n]+;'
    found=list(re.finditer(patt,clean,re.S))
    require(tuple(x.group(1) for x in found)==TABLES,'DDL_TABLE_ORDER_OR_SCOPE')
    require(not re.sub(patt,'',clean,flags=re.S).strip(),'DDL_UNPARSED_CONTENT')
    return [(m.group(1),m.group(0).replace('`lingxing`.','`'+schema+'`.')) for m in found]

def read_schema(conn,schema):
    import pymysql
    schema_ok(schema); out={}
    with conn.cursor(pymysql.cursors.DictCursor) as c:
        for table in TABLES:
            c.execute('SHOW CREATE TABLE `'+schema+'`.`'+table+'`'); create=c.fetchone()
            c.execute('SELECT COLUMN_NAME,ORDINAL_POSITION,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,CHARACTER_SET_NAME,COLLATION_NAME,GENERATION_EXPRESSION,COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION',(schema,table)); columns=c.fetchall()
            c.execute('SELECT INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,SUB_PART,INDEX_TYPE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY INDEX_NAME,SEQ_IN_INDEX',(schema,table)); indexes=c.fetchall()
            c.execute('SELECT tc.CONSTRAINT_NAME,tc.CONSTRAINT_TYPE,kcu.COLUMN_NAME,kcu.ORDINAL_POSITION,kcu.REFERENCED_TABLE_SCHEMA,kcu.REFERENCED_TABLE_NAME,kcu.REFERENCED_COLUMN_NAME FROM information_schema.TABLE_CONSTRAINTS tc LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_SCHEMA=kcu.CONSTRAINT_SCHEMA AND tc.TABLE_NAME=kcu.TABLE_NAME AND tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME WHERE tc.CONSTRAINT_SCHEMA=%s AND tc.TABLE_NAME=%s ORDER BY tc.CONSTRAINT_NAME,kcu.ORDINAL_POSITION',(schema,table)); constraints=c.fetchall()
            c.execute('SELECT TABLE_TYPE,ENGINE,ROW_FORMAT,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',(schema,table)); options=c.fetchone()
            c.execute('SELECT tc.CONSTRAINT_NAME,tc.ENFORCED,cc.CHECK_CLAUSE FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc ON tc.CONSTRAINT_SCHEMA=cc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME=cc.CONSTRAINT_NAME WHERE tc.CONSTRAINT_SCHEMA=%s AND tc.TABLE_NAME=%s ORDER BY tc.CONSTRAINT_NAME',(schema,table)); checks=c.fetchall()
            require(options['ENGINE']=='InnoDB' and options['ROW_FORMAT']=='Dynamic' and options['TABLE_COLLATION']=='utf8mb4_bin','TABLE_OPTIONS_MISMATCH')
            require(checks and all(r['ENFORCED']=='YES' for r in checks),'CHECK_NOT_ENFORCED')
            c.execute('SELECT CONSTRAINT_NAME,UPDATE_RULE,DELETE_RULE,UNIQUE_CONSTRAINT_SCHEMA,REFERENCED_TABLE_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=%s AND TABLE_NAME=%s ORDER BY CONSTRAINT_NAME',(schema,table)); fk_rules=c.fetchall()
            require(all(r['UNIQUE_CONSTRAINT_SCHEMA']==schema and r['UPDATE_RULE']=='RESTRICT' and r['DELETE_RULE']=='RESTRICT' for r in fk_rules),'FOREIGN_KEY_SCOPE_OR_ACTION_MISMATCH')
            out[table]={'columns':columns,'indexes':indexes,'constraints':constraints,'checks':checks,'fk_rules':fk_rules,'options':options,'source_schema':schema,'show_create':create['Create Table']}
    require(sum(len(x['checks']) for x in out.values())==22,'CHECK_COUNT_MISMATCH')
    require(sum(len(x['fk_rules']) for x in out.values())==15,'FOREIGN_KEY_COUNT_MISMATCH')
    return out

def structure_projection(meta):
    result={}
    for table,spec in meta.items():
        row=json.loads(json.dumps({k:v for k,v in spec.items() if k not in ('show_create','source_schema')},default=str))
        for key in ('constraints','fk_rules'):
            for obj in row[key]:
                for field in ('REFERENCED_TABLE_SCHEMA','UNIQUE_CONSTRAINT_SCHEMA'):
                    if obj.get(field)==spec['source_schema']: obj[field]='__SELF_SCHEMA__'
        result[table]=row
    return result

def create_new_schema_tables(conn,schema,raw,emit):
    import pymysql
    steps=statements(raw,schema)
    with conn.cursor(pymysql.cursors.DictCursor) as c:
        names=','.join(['%s']*len(TABLES))
        c.execute('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME IN ('+names+')',(schema,*TABLES))
        require(not c.fetchall(),'PREEXISTING_TABLE_STOP_NO_OVERWRITE')
        c.execute('SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=%s AND (CONSTRAINT_NAME LIKE %s OR CONSTRAINT_NAME LIKE %s)',(schema,'fk_garment_source_%','ck_garment_source_%'))
        require(not c.fetchall(),'PREEXISTING_CONSTRAINT_STOP')
        c.execute('SET SESSION lock_wait_timeout=10')
        c.execute("SET SESSION time_zone='+00:00'")
        for table,sql in steps:
            emit({'event':'DDL_START','table':table,'sql_sha256':hashlib.sha256(sql.encode()).hexdigest(),'at':stamp()})
            c.execute(sql)
            c.execute('SHOW WARNINGS'); warnings=c.fetchall()
            require(all(w['Code']==1681 for w in warnings),'UNEXPECTED_DDL_WARNING')
            c.execute('SHOW CREATE TABLE `'+schema+'`.`'+table+'`'); row=c.fetchone()
            emit({'event':'DDL_CREATED_READBACK','table':table,'warnings':warnings,'show_create':row['Create Table'],'at':stamp()})
    metadata=read_schema(conn,schema)
    emit({'event':'SCHEMA_READBACK','tables':metadata,'structure_sha256':sha(structure_projection(metadata)),'at':stamp()})
    return metadata
