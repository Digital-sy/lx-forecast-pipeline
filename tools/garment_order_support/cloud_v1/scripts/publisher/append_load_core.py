"""Append-only nine-table garment evidence load; no credentials or connection side effects.

package: ordered TABLES -> rows, excluding technical create/update times.
expected_before: complete TABLES -> database rows, including technical times.
All old rows and their technical times are compared exactly after normalization.
The caller must archive expected_before and verify target identity before invoking.
"""
from __future__ import annotations
import hashlib
import re
from datetime import datetime
import mysql_load_core as core

VERSION = '2.0.1'
TABLES, PK = core.TABLES, core.PK
MAX_ROWS = 25000
AUTO_TIME = core.AUTO_TIME

# These mirror the approved schema; MySQL constraints remain enabled as a second check.
UNIQUE_KEYS = {
 TABLES[1]: [('source_system', 'resource_type', 'source_id')],
 TABLES[2]: [('document_id', 'content_sha256')],
 TABLES[3]: [('run_id', 'document_id')],
 TABLES[4]: [('run_id', 'version_id', 'parser_sha256', 'config_sha256', 'attempt_no')],
 TABLES[5]: [('extraction_id', 'source_line_id')],
 TABLES[6]: [('extraction_id', 'source_line_id', 'component_locator')],
 TABLES[7]: [('review_id', 'confirmation_version')],
}
FK = [
 (2, ('document_id',), 1, ('document_id',)),
 (3, ('run_id',), 0, ('run_id',)),
 (3, ('document_id',), 1, ('document_id',)),
 (3, ('version_id','document_id'), 2, ('version_id','document_id')),
 (4, ('run_id',), 0, ('run_id',)),
 (4, ('version_id',), 2, ('version_id',)),
 (5, ('extraction_id','version_id'), 4, ('extraction_id','version_id')),
 (6, ('extraction_id','version_id'), 4, ('extraction_id','version_id')),
 (6, ('order_evidence_id','version_id'), 5, ('evidence_id','version_id')),
 (7, ('supersedes_confirmation_id','review_id'), 7, ('confirmation_id','review_id')),
 (8, ('confirmation_id',), 7, ('confirmation_id',)),
 (8, ('version_id','document_id'), 2, ('version_id','document_id')),
 (8, ('order_evidence_id','version_id'), 5, ('evidence_id','version_id')),
 (8, ('material_evidence_id','version_id'), 6, ('material_evidence_id','version_id')),
 (8, ('material_evidence_id','version_id','order_evidence_id'), 6,
     ('material_evidence_id','version_id','order_evidence_id')),
]
OBJECT_JSON = {
 TABLES[0]: {'scope_json','counts_json'}, TABLES[5]: {'raw_json'},
 TABLES[6]: {'raw_json'}, TABLES[7]: {'scope_json'},
}


def require(ok, code):
    core.require(ok, code)


def _default_value(col):
    value = col['COLUMN_DEFAULT']
    if value is not None and col['DATA_TYPE'] in ('tinyint','smallint','mediumint','int','bigint'):
        return int(value)
    return value


def normalize_row(row, columns, *, audit, native_json=False):
    out = {}
    for col in columns:
        name = col['COLUMN_NAME']
        if name in AUTO_TIME and not audit:
            continue
        require(name in row or not audit, 'BASELINE_FIELD_MISSING')
        value = row[name] if name in row else _default_value(col)
        if native_json and col['DATA_TYPE'] == 'json' and value is not None:
            # Incoming package JSON uses native Python values, including scalar str.
            # DB snapshots use encoded JSON strings and are decoded by core instead.
            core.canonical(value)
            out[name] = {'json_value': value}
        else:
            out[name] = core._normalize(value, col['DATA_TYPE'])
    return out


def normalize_snapshot(snapshot, metadata):
    require(set(snapshot) == set(TABLES), 'BASELINE_NINE_TABLE_SCOPE')
    result, total = {}, 0
    for table in TABLES:
        rows = snapshot[table]
        require(isinstance(rows, list), 'BASELINE_ROWS_NOT_LIST')
        total += len(rows)
        require(total <= MAX_ROWS, 'BASELINE_ROW_BUDGET_EXCEEDED')
        result[table] = {}
        expected_columns = {x['COLUMN_NAME'] for x in metadata[table]}
        for row in rows:
            require(set(row) == expected_columns, 'BASELINE_COLUMN_SET_MISMATCH')
            key = row[PK[table]]
            require(key not in result[table], 'BASELINE_DUPLICATE_PRIMARY_KEY')
            result[table][key] = normalize_row(row, metadata[table], audit=True)
    return result


def read_snapshot(connection, schema, metadata, *, max_rows=MAX_ROWS, for_update=False):
    """Read only the nine allowlisted small tables; never uses SELECT * or unbounded fetch."""
    require(type(max_rows) is int and 0 <= max_rows <= MAX_ROWS, 'INVALID_SNAPSHOT_BUDGET')
    total, result = 0, {}
    with connection.cursor() as cursor:
        for table in TABLES:
            columns = [x['COLUMN_NAME'] for x in metadata[table]]
            limit = max_rows - total + 1
            sql = ('SELECT ' + ','.join(core.identifier(x) for x in columns)
                   + ' FROM ' + core.qtable(schema, table)
                   + ' ORDER BY ' + core.identifier(PK[table]) + ' LIMIT %s')
            if for_update:
                sql += ' FOR UPDATE'
            cursor.execute(sql, (limit,))
            rows = core._dict_rows(cursor)
            total += len(rows)
            require(total <= max_rows, 'SNAPSHOT_ROW_BUDGET_EXCEEDED')
            result[table] = rows
    return result


def _validate_value(value, col):
    if value is None:
        require(col['IS_NULLABLE'] == 'YES', 'NULL_REQUIRED_COLUMN')
        return
    dtype, ctype = col['DATA_TYPE'], col['COLUMN_TYPE']
    if dtype == 'json':
        core.canonical(value)
    else:
        core._normalize(value, dtype)
    if dtype in ('char','varchar'):
        match = re.search(r'\((\d+)\)', ctype)
        require(isinstance(value, str) and match is not None and len(value) <= int(match[1]), 'STRING_LENGTH_OR_TYPE')
    if dtype in ('tinyint','smallint','mediumint','int','bigint') and 'unsigned' in ctype.lower():
        require(value >= 0, 'NEGATIVE_UNSIGNED_VALUE')
    if dtype == 'json':
        core.canonical(value)


def prepare_plan(package, metadata, before):
    """Validate the complete post-append graph and return only genuinely new rows."""
    require(tuple(package) == TABLES, 'PACKAGE_SCOPE_OR_ORDER')
    expected = {t: {} for t in TABLES}
    inserts = {t: [] for t in TABLES}
    merged = {t: {k: {n:v for n,v in r.items() if n not in AUTO_TIME}
                  for k,r in before[t].items()} for t in TABLES}
    package_total, skipped = 0, {t:0 for t in TABLES}
    for table in TABLES:
        rows = package[table]
        require(isinstance(rows, list), 'PACKAGE_ROWS_NOT_LIST')
        package_total += len(rows)
        require(package_total <= MAX_ROWS, 'PACKAGE_ROW_BUDGET_EXCEEDED')
        columns = metadata[table]
        fields = {c['COLUMN_NAME']:c for c in columns}
        for row in rows:
            require(not (set(row) - set(fields)), 'UNKNOWN_PACKAGE_COLUMN')
            require(not (set(row) & AUTO_TIME), 'PACKAGE_MUST_NOT_OVERRIDE_AUDIT_TIMES')
            key = row.get(PK[table])
            require(re.fullmatch(r'[0-9a-f]{64}', str(key)) is not None, 'INVALID_PRIMARY_KEY')
            require(key not in expected[table], 'DUPLICATE_PACKAGE_PRIMARY_KEY')
            require(row.get('delete_flag', 0) == 0, 'APPEND_CANNOT_DELETE_SOURCE')
            for name,col in fields.items():
                if name in AUTO_TIME:
                    continue
                if name not in row:
                    require(col['IS_NULLABLE'] == 'YES' or col['COLUMN_DEFAULT'] is not None, 'MISSING_REQUIRED_COLUMN')
                else:
                    _validate_value(row[name], col)
            for name in OBJECT_JSON.get(table, set()):
                require(isinstance(row.get(name), dict), 'OBJECT_JSON_REQUIRED')
            for name in ('quantity_reliable','future_default_confirmed','material_color_mapping_confirmed',
                         'lingxing_sku_confirmed','full_garment_recipe_confirmed','applied_to_current_source'):
                if name in row:
                    require(row[name] is None or type(row[name]) is int and row[name] in (0,1), 'INVALID_TRISTATE_FLAG')
            if table == TABLES[4]:
                require(type(row.get('attempt_no')) is int and row['attempt_no'] > 0, 'INVALID_ATTEMPT')
            if table == TABLES[7]:
                require(type(row.get('confirmation_version')) is int and row['confirmation_version'] > 0, 'INVALID_CONFIRMATION_VERSION')
            wanted = normalize_row(row, columns, audit=False, native_json=True)
            expected[table][key] = wanted
            if key in merged[table]:
                require(merged[table][key] == wanted, 'EXISTING_PRIMARY_KEY_CONTENT_CONFLICT:' + table)
                skipped[table] += 1
            else:
                merged[table][key] = wanted
                inserts[table].append(row)
    require(sum(len(rows) for rows in merged.values()) <= MAX_ROWS, 'RESULT_ROW_BUDGET_EXCEEDED')
    # Compare ordinary scalar unique/FK keys, not JSON values.
    for table, key_sets in UNIQUE_KEYS.items():
        for names in key_sets:
            seen = set()
            for row in merged[table].values():
                key = tuple(row[n] for n in names)
                if any(x is None for x in key):
                    continue  # MySQL unique constraints allow multiple NULL combinations.
                require(key not in seen, 'NATURAL_UNIQUE_KEY_CONFLICT:' + table)
                seen.add(key)
    for child_i, child_fields, parent_i, parent_fields in FK:
        parent_keys = {tuple(r[x] for x in parent_fields) for r in merged[TABLES[parent_i]].values()}
        for row in merged[TABLES[child_i]].values():
            key = tuple(row[x] for x in child_fields)
            if any(x is None for x in key):
                continue
            require(key in parent_keys, 'ORPHAN_OR_CROSS_VERSION_REFERENCE:' + TABLES[child_i])
    confirmations = merged[TABLES[7]]
    for row in confirmations.values():
        parent = row['supersedes_confirmation_id']
        if parent is not None:
            require(confirmations[parent]['confirmation_version'] < row['confirmation_version'], 'CONFIRMATION_VERSION_NOT_INCREASING')
    # Parent confirmation events must exist before a new revision is inserted.
    remaining = {r[PK[TABLES[7]]]:r for r in inserts[TABLES[7]]}
    available = set(before[TABLES[7]])
    ordered = []
    while remaining:
        ready = sorted(k for k,r in remaining.items() if r['supersedes_confirmation_id'] is None
                       or r['supersedes_confirmation_id'] in available)
        require(ready, 'CONFIRMATION_REFERENCE_CYCLE')
        for key in ready:
            ordered.append(remaining.pop(key)); available.add(key)
    inserts[TABLES[7]] = ordered
    return {'expected':expected,'inserts':inserts,'skipped':skipped,
            'insert_count':sum(len(x) for x in inserts.values()),'merged':merged}


def compare_after(actual_raw, metadata, before, plan):
    """Old rows include exact audit-time comparison; new audit times are checked, never invented."""
    actual = normalize_snapshot(actual_raw, metadata)
    results, all_match = {}, True
    for table in TABLES:
        want = plan['merged'][table]
        seen = actual[table]
        missing, extra = set(want) - set(seen), set(seen) - set(want)
        old_changes, new_changes, audit_invalid = 0, 0, 0
        for key in set(want) & set(seen):
            if key in before[table]:
                old_changes += sum(seen[key][c] != v for c,v in before[table][key].items())
            else:
                new_changes += sum(seen[key][c] != v for c,v in want[key].items())
                ct, ut = seen[key]['create_time'], seen[key]['update_time']
                if ct is None or ut is None or ut < ct:
                    audit_invalid += 1
        match = not (missing or extra or old_changes or new_changes or audit_invalid)
        all_match = all_match and match
        results[table] = {'before_rows':len(before[table]),'expected_after_rows':len(want),
                         'actual_after_rows':len(seen),'missing_keys':len(missing),'extra_keys':len(extra),
                         'old_fields_changed_including_audit':old_changes,'new_fields_changed':new_changes,
                         'new_audit_invalid':audit_invalid,'matched':match,
                         'before_sha256':core.sha(before[table]),'after_sha256':core.sha(seen)}
    return {'state':'MATCH' if all_match else 'CONFLICT','tables':results}


def _independent_snapshot(connect_factory, schema, metadata):
    conn = connect_factory()
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION time_zone='+00:00'")
            cur.execute('SET SESSION MAX_EXECUTION_TIME=10000')
            require(core.scalar(cur, 'SELECT DATABASE()') == schema, 'CONNECTION_DATABASE_MISMATCH')
            cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cur.execute('START TRANSACTION READ ONLY')
        result = read_snapshot(conn, schema, metadata)
        conn.rollback()
        return result
    finally:
        try:
            conn.close()
        except Exception:
            pass


def load_append(connect_factory, schema, package, expected_before, *, fault_after_table=None):
    """One bounded INSERT-only transaction; no write retry, DDL, UPDATE or DELETE."""
    core.allowed_schema(schema)
    if fault_after_table is not None:
        require(schema.startswith('go_test_') and fault_after_table in TABLES, 'FAULT_FORBIDDEN_OUTSIDE_ISOLATION')
    receipt = {'loader_version':VERSION,'schema':schema,'started_at_utc':core.now(),'status':'NOT_RUN',
               'write_statements':0,'inserted_rows_attempted':0,'commit_attempts':0,'rollback_attempts':0,
               'ddl':0,'update_statements':0,'delete_statements':0,'fault_injection':fault_after_table,
               'max_scope_rows':MAX_ROWS}
    conn = None
    locked = commit_started = False
    metadata = before = plan = None
    lock_name = 'garment_initial:' + hashlib.sha256(schema.encode()).hexdigest()[:32]
    try:
        conn = connect_factory()
        with conn.cursor() as cur:
            cur.execute("SET SESSION time_zone='+00:00'")
            cur.execute('SET SESSION innodb_lock_wait_timeout=10')
            cur.execute('SET SESSION MAX_EXECUTION_TIME=10000')
            require(core.scalar(cur,'SELECT DATABASE()') == schema, 'CONNECTION_DATABASE_MISMATCH')
            mode = str(core.scalar(cur,'SELECT @@SESSION.sql_mode')).split(',')
            require('STRICT_TRANS_TABLES' in mode or 'STRICT_ALL_TABLES' in mode, 'STRICT_SQL_MODE_REQUIRED')
            require(core.scalar(cur,'SELECT @@SESSION.foreign_key_checks') == 1, 'FOREIGN_KEY_CHECKS_REQUIRED')
            require(core.scalar(cur,'SELECT GET_LOCK(%s,0)',(lock_name,)) == 1, 'IMPORT_LOCK_BUSY')
            locked = True
        metadata = core.schema_metadata(conn, schema)
        before = normalize_snapshot(expected_before, metadata)
        plan = prepare_plan(package, metadata, before)
        receipt['planned_insert_rows'] = plan['insert_count']
        receipt['existing_identical_rows'] = plan['skipped']
        receipt['before_sha256'] = core.sha(before)
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
            cur.execute('START TRANSACTION')
        locked_before = read_snapshot(conn, schema, metadata, for_update=True)
        require(normalize_snapshot(locked_before, metadata) == before, 'TARGET_CHANGED_SINCE_FROZEN_BASELINE')
        receipt['locked_baseline_verified'] = True
        if not plan['insert_count']:
            conn.rollback(); receipt['rollback_attempts'] += 1
            readback = _independent_snapshot(connect_factory, schema, metadata)
            receipt['independent_readback'] = compare_after(readback, metadata, before, plan)
            require(receipt['independent_readback']['state'] == 'MATCH', 'ALREADY_MATCHED_READBACK_CHANGED')
            receipt['status'] = 'ALREADY_MATCHED_VERIFIED'
            return receipt
        for table in TABLES:
            with conn.cursor() as cur:
                fields = {x['COLUMN_NAME']:x for x in metadata[table]}
                for row in plan['inserts'][table]:
                    columns = list(row)
                    sql = ('INSERT INTO ' + core.qtable(schema, table) + ' ('
                           + ','.join(core.identifier(x) for x in columns) + ') VALUES ('
                           + ','.join(['%s'] * len(columns)) + ')')
                    values = [core.canonical(row[x]) if row[x] is not None and fields[x]['DATA_TYPE'] == 'json'
                              else row[x] for x in columns]
                    cur.execute(sql, tuple(values))
                    receipt['write_statements'] += 1
                    receipt['inserted_rows_attempted'] += 1
            if table == fault_after_table:
                raise core.InjectedIsolationFailure('AFTER_TABLE_INSERT')
        receipt['transaction_readback'] = compare_after(read_snapshot(conn,schema,metadata),metadata,before,plan)
        require(receipt['transaction_readback']['state'] == 'MATCH', 'IN_TRANSACTION_PAYLOAD_MISMATCH')
        commit_started = True
        receipt['commit_attempts'] += 1
        try:
            conn.commit()
        except Exception as error:
            receipt['status'] = 'COMMIT_UNKNOWN'
            receipt['error_type'] = type(error).__name__
            if error.args and isinstance(error.args[0], int):
                receipt['mysql_errno'] = error.args[0]
            try:
                conn.close(); conn = None; locked = False
                actual_raw = _independent_snapshot(connect_factory,schema,metadata)
                receipt['independent_readback'] = compare_after(actual_raw,metadata,before,plan)
                if receipt['independent_readback']['state'] == 'MATCH':
                    receipt['commit_resolution'] = 'COMMITTED_VERIFIED'
                elif normalize_snapshot(actual_raw,metadata) == before:
                    receipt['commit_resolution'] = 'NOT_COMMITTED_VERIFIED'
                else:
                    receipt['commit_resolution'] = 'UNRESOLVED_CONFLICT'
            except Exception as verify_error:
                receipt['commit_resolution'] = 'UNRESOLVED_READBACK_FAILED'
                receipt['readback_error_type'] = type(verify_error).__name__
            return receipt
        actual_raw = _independent_snapshot(connect_factory,schema,metadata)
        receipt['independent_readback'] = compare_after(actual_raw,metadata,before,plan)
        receipt['status'] = ('COMMITTED_VERIFIED' if receipt['independent_readback']['state'] == 'MATCH'
                             else 'COMMITTED_READBACK_MISMATCH')
        return receipt
    except Exception as error:
        receipt['error_type'] = type(error).__name__
        if isinstance(error, core.LoadContractError):
            receipt['reason_code'] = str(error)
        elif error.args and isinstance(error.args[0], int):
            receipt['mysql_errno'] = error.args[0]
        if conn is not None and not commit_started:
            try:
                conn.rollback(); receipt['rollback_attempts'] += 1
                if receipt['write_statements'] and before is not None:
                    actual_raw = _independent_snapshot(connect_factory,schema,metadata)
                    identical = normalize_snapshot(actual_raw,metadata) == before
                    receipt['rollback_baseline_unchanged'] = identical
                    receipt['status'] = 'ROLLED_BACK_VERIFIED' if identical else 'ROLLBACK_VERIFICATION_CONFLICT'
                else:
                    receipt['status'] = 'BLOCKED_NO_WRITE'
            except Exception as rollback_error:
                receipt['status'] = 'ROLLBACK_UNCONFIRMED'
                receipt['rollback_error_type'] = type(rollback_error).__name__
        elif commit_started:
            receipt['status'] = 'COMMITTED_READBACK_FAILED'
        else:
            receipt['status'] = 'BLOCKED_NO_WRITE'
        return receipt
    finally:
        if conn is not None:
            try:
                if locked:
                    with conn.cursor() as cur:
                        cur.execute('SELECT RELEASE_LOCK(%s)',(lock_name,))
            except Exception as error:
                receipt['lock_release_error_type'] = type(error).__name__
            finally:
                try:
                    conn.close()
                except Exception as error:
                    receipt['close_error_type'] = type(error).__name__
        receipt['finished_at_utc'] = core.now()
