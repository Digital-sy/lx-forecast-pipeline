"""Bounded server worker. Imports are inert; credentials are loaded only in factory()."""
import json
import re
import mysql_load_core as core
import append_load_core as app
import schema_ops as ops
import server_db_preflight as provider

SCHEMA = "lingxing"
UUID = "5ee7874b-89d2-11ef-83b6-00163e025678"
MAX_ROWS = 25000

class WorkerContractError(ValueError):
    pass

def require(ok, code):
    if not ok:
        raise WorkerContractError(code)

def json_value(value):
    return json.loads(json.dumps(value, ensure_ascii=False, default=str, allow_nan=False))

def safe_error(exc):
    out = {"error_type": type(exc).__name__}
    reason = str(exc)
    if type(exc) is WorkerContractError and re.fullmatch(r"[A-Z][A-Z0-9_:]{0,199}", reason):
        out["reason_code"] = reason
    elif exc.args and isinstance(exc.args[0], int):
        out["mysql_errno"] = exc.args[0]
    return out

def factory():
    import pymysql
    kwargs, _safe_meta = provider.load_connection_config()
    conn = pymysql.connect(**kwargs)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@server_uuid,DATABASE(),VERSION()")
            identity = cursor.fetchone()
            require(identity[0] == UUID and identity[1] == SCHEMA and identity[2].startswith("8.0."),
                    "TARGET_IDENTITY_CHANGED")
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            require(bool(cursor.fetchone()[1]), "TLS_REQUIRED")
            cursor.execute("SET SESSION time_zone='+00:00'")
            cursor.execute("SET SESSION MAX_EXECUTION_TIME=10000")
        conn.rollback()
        return conn
    except Exception:
        conn.close()
        raise

def structure_guard(conn, expected):
    require(set(expected) == set(core.TABLES), "EXPECTED_STRUCTURE_SCOPE")
    actual = ops.structure_projection(ops.read_schema(conn, SCHEMA))
    require(actual == expected, "STRUCTURE_CHANGED")
    with conn.cursor() as cursor:
        cursor.execute("SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
                       "WHERE TRIGGER_SCHEMA=%s AND EVENT_OBJECT_TABLE IN (" +
                       ",".join(["%s"] * len(core.TABLES)) + ")", (SCHEMA, *core.TABLES))
        require(not cursor.fetchall(), "UNREVIEWED_TRIGGER_PRESENT")
    return actual

def explain_tables(conn, metadata):
    plans = {}
    with conn.cursor() as cursor:
        for table in core.TABLES:
            columns = [item["COLUMN_NAME"] for item in metadata[table]]
            sql = "SELECT " + ",".join(core.identifier(x) for x in columns) + " FROM " + core.qtable(SCHEMA, table)
            sql += " ORDER BY " + core.identifier(core.PK[table]) + " LIMIT %s"
            cursor.execute("EXPLAIN FORMAT=JSON " + sql, (MAX_ROWS + 1,))
            plan = json.loads(cursor.fetchone()[0])
            scans = []
            def visit(value):
                if isinstance(value, dict):
                    if "rows_examined_per_scan" in value:
                        scans.append(int(value["rows_examined_per_scan"]))
                    for child in value.values():
                        visit(child)
                elif isinstance(value, list):
                    for child in value:
                        visit(child)
            visit(plan)
            # Missing native estimates are not silently treated as zero.
            require(scans and all(0 <= x <= MAX_ROWS for x in scans), "EXPLAIN_BUDGET_OR_SHAPE")
            plans[table] = plan
    return plans

def evidence_fields(rows, metadata):
    clean = json_value(rows)
    normalized = app.normalize_snapshot(clean, metadata)
    return {"tables": clean, "metadata": json_value(metadata),
            "row_counts": {table: len(clean[table]) for table in core.TABLES},
            "total_rows": sum(len(clean[table]) for table in core.TABLES),
            "canonical_sha256": core.sha(normalized),
            "canonical_table_sha256": {table: core.sha(normalized[table]) for table in core.TABLES}}

def read_only_snapshot(expected_structure):
    conn = None
    result = {"operation": "NINE_SUPPORT_TABLES_READ_ONLY", "status": "NOT_RUN",
              "started_at_utc": core.now(), "database_writes": 0,
              "schema": SCHEMA, "server_uuid": UUID, "max_total_rows": MAX_ROWS}
    try:
        conn = factory()
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION READ ONLY")
        structure = structure_guard(conn, expected_structure)
        metadata = core.schema_metadata(conn, SCHEMA)
        plans = explain_tables(conn, metadata)
        rows = app.read_snapshot(conn, SCHEMA, metadata, max_rows=MAX_ROWS)
        result.update(evidence_fields(rows, metadata))
        result.update(structure=structure, plans=plans, status="READ_ONLY_SNAPSHOT_VERIFIED")
        conn.rollback()
    except Exception as exc:
        result.update(status="BLOCKED_READ_ONLY", **safe_error(exc))
    finally:
        if conn:
            conn.close()
        result["finished_at_utc"] = core.now()
    return result

def publish_payload(payload):
    result = {"operation": "APPEND_RAW_SUPPORT_EVIDENCE", "status": "NOT_RUN",
              "started_at_utc": core.now(), "schema": SCHEMA, "server_uuid": UUID,
              "package_manifest_sha256": payload["package_manifest_sha256"],
              "baseline_file_sha256": payload["baseline_file_sha256"],
              "ddl": 0, "update_delete_statements": 0}
    conn = None
    loader_started = False
    try:
        require(set(payload["package"]) == set(core.TABLES), "PACKAGE_SCOPE")
        payload["package"] = {t: payload["package"][t] for t in core.TABLES}
        require(all(not payload["package"][t] for t in core.TABLES[6:]), "MATERIAL_MANUAL_REFERENCE_INSERT_FORBIDDEN")
        require(payload["package_content_sha256"] == core.sha(payload["package"]), "PAYLOAD_CONTENT_CHANGED")
        conn = factory()
        result["structure"] = structure_guard(conn, payload["expected_structure"])
        metadata = core.schema_metadata(conn, SCHEMA)
        result["plans"] = explain_tables(conn, metadata)
        normalized = app.normalize_snapshot(payload["before"], metadata)
        require(core.sha(normalized) == payload["baseline_canonical_sha256"], "BASELINE_CONTENT_CHANGED")
        plan = app.prepare_plan(payload["package"], metadata, normalized)
        require(sum(len(x) for x in normalized.values()) + plan["insert_count"] <= MAX_ROWS,
                "PROJECTED_ROW_BUDGET_EXCEEDED")
        conn.rollback()
        conn.close()
        conn = None
        # The existing core repeats locked baseline checks and is the sole writer.
        loader_started = True
        receipt = app.load_append(factory, SCHEMA, payload["package"], payload["before"])
        result.update(loader_receipt=receipt, status=receipt["status"])
        if receipt["status"] in ("COMMITTED_VERIFIED", "ALREADY_MATCHED_VERIFIED"):
            conn = factory()
            with conn.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cursor.execute("START TRANSACTION READ ONLY")
            structure_guard(conn, payload["expected_structure"])
            final_metadata = core.schema_metadata(conn, SCHEMA)
            require(json_value(final_metadata) == json_value(metadata), "METADATA_CHANGED_AFTER_COMMIT")
            after = app.read_snapshot(conn, SCHEMA, metadata)
            verify = app.compare_after(after, metadata, normalized, plan)
            result["final_readback"] = verify
            require(verify["state"] == "MATCH", "FINAL_READBACK_CHANGED")
            result.update(evidence_fields(after, metadata))
            conn.rollback()
    except Exception as exc:
        if result["status"] in ("COMMITTED_VERIFIED", "ALREADY_MATCHED_VERIFIED"):
            result["status"] = "POST_COMMIT_VERIFICATION_FAILED"
        elif result["status"] == "NOT_RUN":
            result["status"] = "COMMIT_UNKNOWN" if loader_started else "BLOCKED_NO_WRITE"
        result.update(safe_error(exc))
    finally:
        if conn:
            conn.close()
        result["finished_at_utc"] = core.now()
    return result

def execute_payload(payload):
    require(payload.get("schema") == SCHEMA and payload.get("server_uuid") == UUID, "PAYLOAD_TARGET_MISMATCH")
    if payload["action"] == "snapshot":
        return read_only_snapshot(payload["expected_structure"])
    if payload["action"] == "publish":
        return publish_payload(payload)
    raise ValueError("ACTION_NOT_ALLOWLISTED")
