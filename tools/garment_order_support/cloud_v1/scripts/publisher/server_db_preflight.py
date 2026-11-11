"""Bounded metadata-only preflight; credentials stay on approved server."""
import ast, hashlib, json, os, re, sys
from pathlib import Path
from datetime import datetime, timezone

TARGET_HOST = 'rm-wz91237y91oasq45fco.mysql.rds.aliyuncs.com'
TARGET_USER = 'SYSJ001'
CONFIG_PATH = Path('/opt/apps/pythondata/common/config.py')
CONFIG_SHA = 'f5c9a00d7f75791a14a9a89bfbc9f35aaea29c5647c971cfd5e2a2e8c54ffa77'
TABLES = ['ods_it_garment_order_ingestion_run','dim_sm_garment_order_document','ods_sm_garment_order_document_version','ods_sm_garment_order_document_observation','ods_it_garment_order_extraction_run','dwd_sm_garment_order_color_evidence','dwd_sm_garment_material_color_evidence','ods_manual_sm_garment_color_confirmation','dwd_sm_garment_confirmation_evidence_ref']

def load_connection_config():
    from dotenv.main import DotEnv
    raw = CONFIG_PATH.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == CONFIG_SHA, 'CONFIG_VERSION_CHANGED'
    tree = ast.parse(raw)
    env_assign = next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ENV_FILE' for t in n.targets))
    assert isinstance(env_assign.value,ast.BinOp) and isinstance(env_assign.value.left,ast.Name) and env_assign.value.left.id=='BASE_DIR'
    filename = ast.literal_eval(env_assign.value.right)
    env_path = (CONFIG_PATH.parent.parent / filename).resolve()
    assert env_path.parent == CONFIG_PATH.parent.parent and env_path.is_file(), 'CONFIG_PATH_UNEXPECTED'
    env_values = DotEnv(str(env_path), verbose=False, interpolate=True, override=False).dict()
    init = next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='__init__')
    values, fields = {}, {}
    for n in init.body:
        if not isinstance(n,ast.Assign) or len(n.targets)!=1 or not isinstance(n.targets[0],ast.Attribute): continue
        attr=n.targets[0].attr
        if not attr.startswith('DB_'): continue
        expr=n.value; to_int=False
        if isinstance(expr,ast.Call) and isinstance(expr.func,ast.Name) and expr.func.id=='int':
            to_int=True; expr=expr.args[0]
        assert isinstance(expr,ast.Call) and isinstance(expr.func,ast.Attribute) and expr.func.attr=='getenv'
        key=ast.literal_eval(expr.args[0]); fields[attr]=key
        val=os.environ.get(key,env_values.get(key))
        if val is None and attr not in {'DB_PASSWORD','DB_HOST','DB_USER'} and len(expr.args)>1: val=ast.literal_eval(expr.args[1])
        assert val is not None and str(val)!='', 'MISSING_DB_SETTING_'+attr
        values[attr]=int(val) if to_int else val
    prop=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='db_config')
    ret=next(n for n in prop.body if isinstance(n,ast.Return)).value
    kwargs={ast.literal_eval(k):values[v.attr] for k,v in zip(ret.keys,ret.values)}
    assert values['DB_HOST']==TARGET_HOST, 'HOST_DOES_NOT_MATCH_AUTHORIZED_TARGET'
    assert values['DB_USER']==TARGET_USER, 'USER_DOES_NOT_MATCH_AUTHORIZED_TARGET'
    assert values['DB_DATABASE']=='lingxing', 'DATABASE_DOES_NOT_MATCH_AUTHORIZED_TARGET'
    kwargs.update(connect_timeout=10,read_timeout=15,write_timeout=15,autocommit=False)
    meta={'provider_path':str(CONFIG_PATH),'provider_sha256':CONFIG_SHA,'config_path':str(env_path),'environment_field_names':fields,'expected_host_match':True,'expected_user_match':True,'expected_database_match':True,'password_present':bool(values['DB_PASSWORD'])}
    return kwargs,meta

def main():
    result={'started_at_utc':datetime.now(timezone.utc).isoformat(),'operation':'METADATA_ONLY_PREFLIGHT','business_data_reads':0,'ddl':0,'dml':0}
    conn=None
    try:
        import pymysql
        kwargs,meta=load_connection_config(); result['connection_source']=meta
        conn=pymysql.connect(**kwargs)
        with conn.cursor(pymysql.cursors.DictCursor) as c:
            c.execute('SET SESSION MAX_EXECUTION_TIME=5000')
            c.execute('START TRANSACTION READ ONLY')
            c.execute('SELECT VERSION() AS version, @@version_comment AS version_comment, @@server_uuid AS server_uuid, @@innodb_page_size AS innodb_page_size, @@transaction_isolation AS transaction_isolation, @@autocommit AS autocommit, @@sql_mode AS sql_mode, @@time_zone AS time_zone, @@system_time_zone AS system_time_zone, DATABASE() AS database_name, CURRENT_USER() AS effective_user, @@read_only AS read_only')
            result['server']=c.fetchone()
            c.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'"); result['transport']=c.fetchall()
            c.execute("SELECT SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s",('lingxing',)); result['schema']=c.fetchall()
            c.execute("SELECT ENGINE,SUPPORT,TRANSACTIONS FROM information_schema.ENGINES WHERE ENGINE='InnoDB'"); result['engine']=c.fetchall()
            p=','.join(['%s']*len(TABLES))
            c.execute('SELECT TABLE_NAME,TABLE_TYPE,ENGINE,ROW_FORMAT,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME IN ('+p+')',('lingxing',*TABLES)); result['existing_tables']=c.fetchall()
            c.execute("SELECT TABLE_NAME,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=%s AND (CONSTRAINT_NAME LIKE %s OR CONSTRAINT_NAME LIKE %s)",('lingxing','fk_garment_source_%','ck_garment_source_%')); result['constraint_collisions']=c.fetchall()
            c.execute('SHOW GRANTS FOR CURRENT_USER')
            grants=[]
            for row in c.fetchall():
                s=str(next(iter(row.values())))
                m=re.match(r'^GRANT (.+?) ON (.+?) TO ',s)
                grants.append({'privileges':m.group(1),'scope':m.group(2)} if m else {'unparsed_grant':True})
            result['grants']=grants
        conn.rollback(); result['status']='PREFLIGHT_READ_COMPLETED'
    except Exception as e:
        result['status']='BLOCKED'; result['error_type']=type(e).__name__
        if isinstance(e,AssertionError): result['reason_code']=str(e)
        elif e.args and isinstance(e.args[0],int): result['mysql_errno']=e.args[0]
    finally:
        if conn: conn.close()
    result['finished_at_utc']=datetime.now(timezone.utc).isoformat()
    print(json.dumps(result,ensure_ascii=False,default=str))

if __name__=='__main__': main()

