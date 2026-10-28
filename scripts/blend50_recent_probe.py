"""Ephemeral read-only ODS probe; no API client, production entry or writes.

Only the reviewed project database factory is imported, in main, server-side.
The caller injects a bounded request; output contains only business rows and
allowlisted log evidence, never environment/config contents.
"""
import base64
import datetime as dt
import gzip
import hashlib
import json
import pathlib
import re
import time

SHOPS = ('JQ-US', 'MT-US', 'RKZ-US', 'SY-US')
FIELDS = ('dt','store_name','country','asin','sid','spu','sku','stat_date',
          'delete_flag','volume','create_time','modify_time')
SOURCE = 'ods_db.ods_lx_product_performance'
ENTRY = '/data/bi_scripts/BI/BI数据获取/ODS/ods_lx_product_performance_asin_refresh.py'
LOGROOT = pathlib.Path('/opt/dolphinscheduler/dolphinscheduler-3.4.2/worker-server/logs')
WORKFLOW = '178757034402048'


def clock():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def plans(x):
    if isinstance(x, dict):
        return ([x] if 'rows_examined_per_scan' in x else []) + sum((plans(v) for v in x.values()), [])
    return sum((plans(v) for v in x), []) if isinstance(x, list) else []


def logs():
    result = {'at':clock(), 'files':[], 'events':[], 'active_known_pids':[],
              'all_other_writers_excluded':False}
    for p in pathlib.Path('/proc').iterdir():
        if not p.name.isdigit():continue
        try: cmd = (p/'cmdline').read_bytes().decode('utf8',errors='replace')
        except (OSError, PermissionError):continue
        if ENTRY in cmd:result['active_known_pids'].append(int(p.name))
    files=[]
    for back in range(7):
        date=dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()-dt.timedelta(days=back)
        files.extend((LOGROOT/date.strftime('%Y%m%d')/WORKFLOW).glob('*/*/*.log'))
    used=0
    for p in sorted(files,key=lambda x:x.stat().st_mtime,reverse=True)[:16]:
        limit=min(2097152,8388608-used)
        if limit<=0:break
        with p.open('rb') as f:raw=f.read(limit)
        used+=len(raw); text=raw.decode('utf8',errors='replace')
        if pathlib.Path(ENTRY).name not in text:continue
        result['files'].append({'path':str(p),'bytes':len(raw),'complete':len(raw)==p.stat().st_size,
            'sha256':hashlib.sha256(raw).hexdigest(),'mtime_ns':p.stat().st_mtime_ns,
            'windows':re.findall(r'START_DATE=(\d{4}-\d{2}-\d{2}), END_DATE=(\d{4}-\d{2}-\d{2})',text)})
        for line_no,line in enumerate(text.splitlines(),1):
            markers=[m for m in ('开始抓取','当前页已写入数据库','分片店铺抓取并写库完成',
                                '分片店铺抓取失败','全部完成','3001008','清理','删除') if m in line]
            if not markers:continue
            stamps=re.findall(r'20\d\d-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:[.,]\d+)?(?:\s*[+-]\d{4})?',line)
            result['events'].append({'path':str(p),'line':line_no,'markers':markers,
                'times':stamps,'shops':[s for s in SHOPS if re.search(r'(?<![A-Z-])'+s+r'(?![A-Z-])',line)],
                'dates':re.findall(r'\b20\d\d-\d\d-\d\d\b',line),
                'numbers':dict(re.findall(r'\b(offset|length|rows|total|page_rows|sid)[=:：]\s*(\d+)\b',line))})
    result['bytes_read']=used
    return result


def main(req):
    out={'started_at':clock(),'mode':req['mode'],'production_writes':0,'api_calls':0}
    conn=None
    try:
        if req['mode']=='logs':out['logs']=logs()
        else:
            from common.database import get_db_connection
            import pymysql
            conn=get_db_connection()
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute('SET SESSION MAX_EXECUTION_TIME=10000')
                cur.execute('START TRANSACTION READ ONLY')
                cur.execute('SELECT @@server_uuid AS uuid, VERSION() AS version, @@session.transaction_isolation AS isolation')
                out['instance']=cur.fetchone()
                if out['instance']['uuid']!='5ee7874b-89d2-11ef-83b6-00163e025678':raise ValueError('INSTANCE_CHANGED')
                if req['mode']=='metadata':
                    cur.execute('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION',('ods_db','ods_lx_product_performance'))
                    out['columns']=[r for r in cur.fetchall() if r['COLUMN_NAME'] in FIELDS]
                    cur.execute('SELECT INDEX_NAME,SEQ_IN_INDEX,COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY INDEX_NAME,SEQ_IN_INDEX',('ods_db','ods_lx_product_performance'))
                    out['indexes']=cur.fetchall()
                elif req['mode']=='data':
                    start,end=[dt.date.fromisoformat(req[n]) for n in ('start','end_exclusive')]
                    if not 0<(end-start).days<=2:raise ValueError('DATE_CHUNK')
                    if not req['scope_start']<=str(start)<str(end)<=req['scope_end_exclusive']:raise ValueError('DATE_SCOPE')
                    sql='SELECT '+','.join(FIELDS)+' FROM '+SOURCE+' FORCE INDEX(PRIMARY) WHERE dt>=%s AND dt<%s AND store_name IN (%s,%s,%s,%s) ORDER BY dt,store_name,country,asin'
                    params=(str(start),str(end))+SHOPS
                    cur.execute('EXPLAIN FORMAT=JSON '+sql,params)
                    plan=json.loads(next(iter(cur.fetchone().values()))); tables=plans(plan)
                    scan=sum(int(t['rows_examined_per_scan']) for t in tables)
                    out.update(sql=sql,params=params,explain=plan,estimated_scan=scan)
                    if scan>min(1000000,req['remaining_scan']) or any(t.get('access_type')=='ALL' for t in tables):raise ValueError('SCAN_BUDGET')
                    begin=time.monotonic();cur.execute(sql,params);rows=cur.fetchmany(250001)
                    if len(rows)>250000:raise ValueError('RESULT_BUDGET')
                    out.update(rows=rows,row_count=len(rows),select_seconds=time.monotonic()-begin)
                else:raise ValueError('MODE')
        out['status']='OK'
    except Exception as exc:
        out.update(status='ERROR',error_type=type(exc).__name__,error_code=exc.args[0] if exc.args and isinstance(exc.args[0],int) else 'CONTROLLED_FAILURE')
        if isinstance(exc,ValueError):out['guard']=str(exc)
    finally:
        if conn:conn.rollback();conn.close()
    out['finished_at']=clock()
    blob=json.dumps(out,ensure_ascii=False,default=str,separators=(',',':')).encode()
    print(json.dumps({'gzip_base64':base64.b64encode(gzip.compress(blob)).decode(),'json_sha256':hashlib.sha256(blob).hexdigest()}))
