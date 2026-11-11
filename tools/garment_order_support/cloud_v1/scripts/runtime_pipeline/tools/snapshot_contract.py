"""Validate publisher-issued support snapshot evidence without DB access or status rewriting."""
from datetime import datetime
import mysql_load_core as core
import append_load_core as app

SCHEMA='lingxing'
SERVER_UUID='5ee7874b-89d2-11ef-83b6-00163e025678'

def require(ok,code):
    if not ok:raise ValueError(code)

def validate_snapshot(value):
    status=value.get('status')
    require(status in ('READ_ONLY_SNAPSHOT_VERIFIED','COMMITTED_VERIFIED','ALREADY_MATCHED_VERIFIED'),'BASELINE_NOT_VERIFIED')
    require(value.get('schema')==SCHEMA and value.get('server_uuid')==SERVER_UUID,'BASELINE_TARGET_MISMATCH')
    require(set(value.get('tables',{}))==set(core.TABLES) and set(value.get('metadata',{}))==set(core.TABLES) and set(value.get('structure',{}))==set(core.TABLES),'SNAPSHOT_TABLE_SCOPE')
    for field in ('started_at_utc','finished_at_utc'):
        stamp=datetime.fromisoformat(value[field]);require(stamp.tzinfo is not None,'SNAPSHOT_TIMEZONE_MISSING')
    require(value['started_at_utc']<=value['finished_at_utc'],'SNAPSHOT_TIME_ORDER')
    if status=='READ_ONLY_SNAPSHOT_VERIFIED':
        require(value.get('database_writes')==0 and value.get('operation')=='NINE_SUPPORT_TABLES_READ_ONLY','READ_ONLY_RECEIPT_CONTRADICTION')
    else:
        require(value.get('final_readback',{}).get('state')=='MATCH','INDEPENDENT_READBACK_NOT_MATCHED')
    for table in core.TABLES:
        meta=value['metadata'][table];structure=value['structure'][table]
        require(meta and structure.get('options',{}).get('ENGINE')=='InnoDB','SNAPSHOT_STRUCTURE_REQUIRED')
        columns=structure.get('columns',[])
        require([c['COLUMN_NAME'] for c in meta]==[c['COLUMN_NAME'] for c in columns],'SNAPSHOT_METADATA_COLUMNS')
        for m,c in zip(meta,columns):
            require(m.get('TABLE_NAME')==table,'SNAPSHOT_METADATA_TABLE')
            for key in ('COLUMN_NAME','ORDINAL_POSITION','COLUMN_TYPE','IS_NULLABLE','COLUMN_DEFAULT','EXTRA','CHARACTER_SET_NAME','COLLATION_NAME'):
                require(m.get(key)==c.get(key),'SNAPSHOT_METADATA_STRUCTURE:'+key)
            require(m['DATA_TYPE'].lower()==m['COLUMN_TYPE'].split('(')[0].split(' ')[0].lower(),'SNAPSHOT_METADATA_TYPE')
        require(value.get('row_counts',{}).get(table)==len(value['tables'][table]),'SNAPSHOT_ROW_COUNT:'+table)
    normal=app.normalize_snapshot(value['tables'],value['metadata'])
    total=sum(len(v) for v in normal.values())
    require(total==value.get('total_rows') and total<=app.MAX_ROWS,'SNAPSHOT_TOTAL_ROWS')
    require(core.sha(normal)==value.get('canonical_sha256'),'SNAPSHOT_CONTENT_HASH')
    require({t:core.sha(normal[t]) for t in core.TABLES}==value.get('canonical_table_sha256'),'SNAPSHOT_TABLE_CONTENT_HASH')
    return normal
