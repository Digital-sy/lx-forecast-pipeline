"""Ephemeral readonly source probe; no credentials, files or job execution remotely."""
import datetime
import hashlib
import json
import re
import time

TABLES = (
    ('dwd_db', 'dwd_sale_product_performance_daily'),
    ('ads_db', 'ads_op_listing_traffic_daily'),
    ('dws_db', 'dws_sale_sku_price_cvr_daily'),
    ('ods_db', 'ods_lx_product_performance'),
    ('lingxing', '销量统计_msku月度'),
)


def validate(sql):
    if not re.match(r'^SELECT\s', sql.strip(), re.I):
        raise ValueError('SELECT_ONLY')
    if ';' in sql or re.search(r'\b(INTO|OUTFILE|DUMPFILE|FOR\s+UPDATE|SLEEP|BENCHMARK)\b', sql, re.I):
        raise ValueError('UNSAFE_STATEMENT')
    if not re.search(r'\bWHERE\b', sql, re.I):
        raise ValueError('BOUND_REQUIRED')
    refs = re.findall(r'\b(?:FROM|JOIN)\s+`?(\w+)`?\.`?([^`\s,()]+)`?', sql, re.I)
    if not refs or any(tuple(t) not in TABLES for t in refs):
        raise ValueError('SOURCE_NOT_ALLOWED')
    if re.search(r'\bJOIN\b', sql, re.I):
        raise ValueError('NO_SOURCE_JOIN')


def scan_rows(node):
    if isinstance(node, dict):
        return int(node.get('rows_examined_per_scan', 0)) + sum(scan_rows(v) for v in node.values())
    if isinstance(node, list):
        return sum(scan_rows(v) for v in node)
    return 0


def main(request):
    from common.database import get_db_connection
    import pymysql
    out = {'acquired_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
           'request': request, 'mode': request['mode']}
    started = time.monotonic()
    c = get_db_connection()
    try:
        with c.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute('SET SESSION MAX_EXECUTION_TIME=10000')
            cur.execute('START TRANSACTION READ ONLY')
            if request['mode'] == 'metadata':
                out['metadata'] = {}
                for schema, table in TABLES:
                    cur.execute('SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE,TABLE_ROWS,DATA_LENGTH,INDEX_LENGTH,UPDATE_TIME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s', (schema, table))
                    info = {'table': cur.fetchall()}
                    cur.execute('SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION', (schema, table))
                    info['columns'] = cur.fetchall()
                    cur.execute('SELECT INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,CARDINALITY FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY INDEX_NAME,SEQ_IN_INDEX', (schema, table))
                    info['indexes'] = cur.fetchall()
                    out['metadata'][schema+'.'+table] = info
            elif request['mode'] in ('plan', 'select'):
                sql = request['sql']
                validate(sql)
                out['sql_sha256'] = hashlib.sha256(sql.encode()).hexdigest()
                cur.execute('EXPLAIN FORMAT=JSON '+sql)
                out['explain'] = json.loads(next(iter(cur.fetchone().values())))
                out['estimated_scan'] = scan_rows(out['explain'])
                if request['mode'] == 'select':
                    if out['estimated_scan'] > 2000000:
                        raise ValueError('PLAN_OVER_BUDGET')
                    cur.execute(sql)
                    rows = cur.fetchmany(500001)
                    if len(rows) > 500000:
                        raise ValueError('RESULT_OVER_BUDGET')
                    out['rows'] = rows
                    out['row_count'] = len(rows)
            else:
                raise ValueError('MODE_NOT_ALLOWED')
            out['status'] = 'OK'
    except Exception as exc:
        out['status'] = 'ERROR'
        out['error_class'] = type(exc).__name__
        out['error_code'] = exc.args[0] if exc.args and isinstance(exc.args[0], int) else 'CONTROLLED_FAILURE'
    finally:
        c.rollback()
        c.close()
    out['elapsed_seconds'] = time.monotonic()-started
    print(json.dumps(out, ensure_ascii=False, default=str))
