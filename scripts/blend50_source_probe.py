"""Ephemeral, restricted SELECT probe. Credentials never leave existing config."""
import datetime
import hashlib
import json
import re
import time

TABLES=(('ods_db','ods_lx_product_performance'),('lingxing','销量统计_msku月度'))

def validate(sql):
    if not re.match(r'^SELECT\s',sql.strip(),re.I):raise ValueError('SELECT_ONLY')
    if ';' in sql or re.search(r'\b(INTO|OUTFILE|DUMPFILE|FOR\s+UPDATE|SLEEP|BENCHMARK|JOIN|UNION|GET_LOCK)\b',sql,re.I):raise ValueError('UNSAFE_SQL')
    if not re.search(r'\bWHERE\b',sql,re.I):raise ValueError('BOUNDED_ONLY')
    refs=re.findall(r'\bFROM\s+`?(\w+)`?\.`?([^`\s,()]+)`?',sql,re.I)
    if len(refs)!=1 or tuple(refs[0]) not in TABLES:raise ValueError('SOURCE_NOT_ALLOWED')

def scans(x):
    if isinstance(x,dict):return int(x.get('rows_examined_per_scan',0))+sum(scans(v) for v in x.values())
    if isinstance(x,list):return sum(scans(v) for v in x)
    return 0

def main(req):
    out={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'mode':req['mode'],
         'metadata_count':0,'explain_count':0,'select_count':0,'timings':[]}
    conn=None
    try:
        from common.database import get_db_connection
        import pymysql
        conn=get_db_connection()
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute('SET SESSION MAX_EXECUTION_TIME=10000')
            cur.execute('START TRANSACTION READ ONLY')
            def run(sql,params=None,kind='metadata'):
                out[kind+'_count']+=1;start=time.monotonic()
                try:
                    cur.execute(sql,params);rows=cur.fetchmany(250001)
                    if len(rows)>250000:raise ValueError('RESULT_LIMIT')
                    return rows
                finally:out['timings'].append({'kind':kind,'seconds':time.monotonic()-start})
            if req['mode']=='metadata':
                out['metadata']={}
                for schema,table in TABLES:
                    pars=(schema,table)
                    out['metadata'][schema+'.'+table]={
                      'table':run('SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,DATA_LENGTH,UPDATE_TIME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s',pars),
                      'columns':run('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION',pars),
                      'indexes':run('SELECT INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,CARDINALITY FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY INDEX_NAME,SEQ_IN_INDEX',pars)}
            elif req['mode'] in ['select','plan']:
                sql=req['sql'];validate(sql)
                plan=run('EXPLAIN FORMAT=JSON '+sql,kind='explain')
                out['explain']=json.loads(next(iter(plan[0].values())))
                out['estimated_scan']=scans(out['explain'])
                if out['estimated_scan']>min(1000000,req['remaining_scan']):raise ValueError('EXPLAIN_SCAN_LIMIT')
                if req['mode']=='select':
                    out['rows']=run(sql,kind='select');out['row_count']=len(out['rows'])
            else:raise ValueError('INVALID_MODE')
        out['status']='OK'
    except Exception as exc:
        out['status']='ERROR';out['error_class']=type(exc).__name__
        out['error_code']=exc.args[0] if exc.args and isinstance(exc.args[0],int) else 'CONTROLLED_FAILURE'
        if isinstance(exc,ValueError):out['guard']=str(exc)
    finally:
        if conn is not None:conn.rollback();conn.close()
    out['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps(out,ensure_ascii=False,default=str))
