"""Parameterised support-source snapshot/publish adapter; no scheduling or secrets.

CLI: python -m publisher.adapter snapshot --structure APPROVED.json --output NEW --transport server-local
     python -m publisher.adapter publish --package DIR --before RECEIPT.json
       --structure APPROVED.json --output NEW [--execute] --transport server-local
Default publish is a local-only dry run. No automatic transport or write retries.
"""
from __future__ import annotations
import argparse
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import types

ROOT = Path(__file__).resolve().parent
VERSION = "1.0.0"
PINNED_ORDER = ("server_db_preflight", "mysql_load_core", "schema_ops", "append_load_core")
SCHEMA = "lingxing"
UUID = "5ee7874b-89d2-11ef-83b6-00163e025678"

def sha_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)

def sha_value(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

def require(ok, code):
    if not ok:
        raise ValueError(code)

def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))

def durable_json(path, value):
    path = Path(path)
    data = (json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False) + "\n").encode()
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    if os.name != "nt":
        fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

def dependencies():
    pins = read_json(ROOT / "dependency_pins.json")
    require(set(pins["sha256"]) == {x + ".py" for x in PINNED_ORDER}, "PIN_SCOPE")
    modules, hashes = {}, {}
    for name in PINNED_ORDER:
        path = ROOT / (name + ".py")
        require(sha_file(path) == pins["sha256"][path.name], "PINNED_DEPENDENCY_CHANGED")
        hashes[path.name] = sha_file(path)
        # Deliberately no import of production config or DB driver here.
        module = types.ModuleType(name)
        module.__file__ = str(path)
        sys.modules[name] = module
        exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
        modules[name] = module
    return modules, hashes

def load_worker():
    dependencies()
    module = types.ModuleType("scheduled_publisher_worker")
    path = ROOT / "worker.py"
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module

def load_structure(path_or_object):
    value = read_json(path_or_object) if isinstance(path_or_object, (str, Path)) else path_or_object
    # Accept the immutable approved isolation receipt or a standalone structure projection.
    value = value.get("structure", value)
    modules, _ = dependencies()
    require(set(value) == set(modules["mysql_load_core"].TABLES), "EXPECTED_STRUCTURE_SCOPE")
    return value

def payload_base(action, structure):
    return {"action": action, "schema": SCHEMA, "server_uuid": UUID,
            "expected_structure": structure}

def transport_run(payload, mode):
    """Exactly one attempt. The local/remote worker alone owns DB connections."""
    require(mode in ("server-local", "ssh"), "TRANSPORT_NOT_ALLOWLISTED")
    modules, hashes = dependencies()
    if mode == "server-local":
        require(os.name != "nt", "SERVER_LOCAL_REQUIRES_AUTHORIZED_LINUX_RUNTIME")
        output, error = io.StringIO(), io.StringIO()
        try:
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
                result = load_worker().execute_payload(payload)
            transport = {"mode": mode, "attempts": 1, "exit_code": 0, "timeout": False}
        except Exception as exc:
            result = {"status": "COMMIT_UNKNOWN" if payload["action"] == "publish" else "TRANSPORT_FAILED",
                      "error_type": type(exc).__name__}
            transport = {"mode": mode, "attempts": 1, "exit_code": 1, "timeout": False}
        transport.update(stderr_sha256=hashlib.sha256(error.getvalue().encode()).hexdigest(),
                         stderr_bytes=len(error.getvalue().encode()),
                         discarded_stdout_sha256=hashlib.sha256(output.getvalue().encode()).hexdigest(),
                         discarded_stdout_bytes=len(output.getvalue().encode()))
        return result, transport
    # Only audited source code and nonsecret business payload cross SSH stdin.
    bundle = {name: base64.b64encode((ROOT / (name + ".py")).read_bytes()).decode() for name in PINNED_ORDER}
    bundle["scheduled_publisher_worker"] = base64.b64encode((ROOT / "worker.py").read_bytes()).decode()
    envelope = base64.b64encode(canonical({"sources": bundle, "payload": payload}).encode()).decode()
    remote = ("import base64,json,sys,types\n"
              "p=json.loads(base64.b64decode(" + repr(envelope) + "))\n"
              "for n,b in p['sources'].items():\n"
              " m=types.ModuleType(n);m.__file__=n+'.py';sys.modules[n]=m;exec(compile(base64.b64decode(b),n+'.py','exec'),m.__dict__)\n"
              "r=sys.modules['scheduled_publisher_worker'].execute_payload(p['payload'])\n"
              "print(json.dumps(r,ensure_ascii=False,default=str,allow_nan=False),flush=True)\n").encode()
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes",
               "root@8.138.102.103",
               "cd /opt/apps/pythondata && PYTHONDONTWRITEBYTECODE=1 venv-ml/bin/python -B -"]
    stdout, stderr, timeout, code = b"", b"", False, None
    try:
        proc = subprocess.run(command, input=remote, capture_output=True, timeout=600 if payload["action"] == "publish" else 180)
        stdout, stderr, code = proc.stdout, proc.stderr, proc.returncode
    except subprocess.TimeoutExpired as exc:
        timeout = True
        stdout, stderr = exc.stdout or b"", exc.stderr or b""
    except Exception as exc:
        # Exception text may contain external process context; type alone is safe.
        return {"status": "COMMIT_UNKNOWN" if payload["action"] == "publish" else "TRANSPORT_FAILED",
                "error_type": type(exc).__name__}, {"mode": mode, "attempts": 1, "timeout": False}
    info = {"mode": mode, "attempts": 1, "exit_code": code, "timeout": timeout,
            "stdout_sha256": hashlib.sha256(stdout).hexdigest(), "stdout_bytes": len(stdout),
            "stderr_sha256": hashlib.sha256(stderr).hexdigest(), "stderr_bytes": len(stderr)}
    try:
        require(not timeout and code == 0, "TRANSPORT_DID_NOT_FINISH")
        result = json.loads(stdout.decode("utf-8-sig"))
        require(isinstance(result, dict) and isinstance(result.get("status"), str), "INVALID_WORKER_RESULT")
    except Exception:
        result = {"status": "COMMIT_UNKNOWN" if payload["action"] == "publish" else "TRANSPORT_FAILED",
                  "reason_code": "TRANSPORT_RESULT_UNAVAILABLE", "automatic_retry": False}
    return result, info

def validate_snapshot(value, structure, core, app):
    require(value.get("status") in ("READ_ONLY_SNAPSHOT_VERIFIED", "COMMITTED_VERIFIED", "ALREADY_MATCHED_VERIFIED"),
            "BASELINE_NOT_VERIFIED")
    require(value.get("schema") == SCHEMA and value.get("server_uuid") == UUID, "BASELINE_TARGET_MISMATCH")
    require(value["structure"] == structure, "BASELINE_STRUCTURE_CHANGED")
    normal = app.normalize_snapshot(value["tables"], value["metadata"])
    require(sum(len(rows) for rows in normal.values()) <= app.MAX_ROWS, "BASELINE_ROW_BUDGET")
    require(value.get("row_counts") == {t: len(normal[t]) for t in core.TABLES}, "BASELINE_ROW_COUNTS_CHANGED")
    require(value.get("total_rows") == sum(len(rows) for rows in normal.values()), "BASELINE_TOTAL_CHANGED")
    require(core.sha(normal) == value["canonical_sha256"], "BASELINE_DIGEST_CHANGED")
    require({t: core.sha(normal[t]) for t in core.TABLES} == value["canonical_table_sha256"],
            "BASELINE_TABLE_DIGEST_CHANGED")
    if value["status"] != "READ_ONLY_SNAPSHOT_VERIFIED":
        require(value.get("final_readback", {}).get("state") == "MATCH", "BASELINE_READBACK_MISSING")
    return normal

def snapshot(output_dir, expected_structure, *, transport="server-local"):
    modules, hashes = dependencies()
    structure = load_structure(expected_structure)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=False)
    payload = payload_base("snapshot", structure)
    durable_json(out / "execution_manifest.json",
                 {"adapter_version": VERSION, "action": "snapshot", "started_at_utc": modules["mysql_load_core"].now(),
                  "code_hashes": hashes, "worker_sha256": sha_file(ROOT / "worker.py"),
                  "adapter_sha256": sha_file(__file__), "payload_sha256": sha_value(payload), "database_writes": 0})
    result, info = transport_run(payload, transport)
    if result.get("status") == "READ_ONLY_SNAPSHOT_VERIFIED":
        validate_snapshot(result, structure, modules["mysql_load_core"], modules["append_load_core"])
    durable_json(out / "transport.json", info)
    durable_json(out / "receipt.json", result)
    return result

def prepare_publish(package_dir, before_receipt_path, expected_structure):
    modules, hashes = dependencies()
    core, app = modules["mysql_load_core"], modules["append_load_core"]
    structure = load_structure(expected_structure)
    before = read_json(before_receipt_path)
    normal = validate_snapshot(before, structure, core, app)
    package, manifest = core.load_package(package_dir)
    require(all(not package[t] for t in core.TABLES[6:]), "MATERIAL_MANUAL_REFERENCE_INSERT_FORBIDDEN")
    require(manifest["production_before"]["sha256"] == sha_file(before_receipt_path), "PACKAGE_BASELINE_FILE_MISMATCH")
    require(bool(manifest.get("source_files")), "PACKAGE_SOURCE_FILES_REQUIRED")
    for path, expected_hash in manifest["source_files"].items():
        require(sha_file(path) == expected_hash, "PACKAGE_SOURCE_CHANGED")
    plan = app.prepare_plan(package, before["metadata"], normal)
    require(sum(len(x) for x in normal.values()) + plan["insert_count"] <= app.MAX_ROWS,
            "PROJECTED_ROW_BUDGET_EXCEEDED")
    payload = payload_base("publish", structure)
    payload.update(package=package, before=before["tables"],
                   package_manifest_sha256=sha_file(Path(package_dir) / "manifest.json"),
                   package_content_sha256=core.sha(package),
                   baseline_file_sha256=sha_file(before_receipt_path),
                   baseline_canonical_sha256=core.sha(normal))
    return payload, before, hashes, plan

def publish(package_dir, before_receipt_path, expected_structure, output_dir, *, execute=False, transport="server-local"):
    payload, before, hashes, plan = prepare_publish(package_dir, before_receipt_path, expected_structure)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=False)
    # This complete backup must reach durable storage before any transport.
    durable_json(out / "before_snapshot.json", before)
    durable_json(out / "frozen_payload.json", payload)
    durable_json(out / "execution_manifest.json",
                 {"adapter_version": VERSION, "action": "publish", "execute": execute,
                  "code_hashes": hashes, "worker_sha256": sha_file(ROOT / "worker.py"),
                  "adapter_sha256": sha_file(__file__), "payload_sha256": sha_value(payload),
                  "baseline_file_sha256": payload["baseline_file_sha256"],
                  "package_manifest_sha256": payload["package_manifest_sha256"],
                  "planned_insert_rows": plan["insert_count"],
                  "automatic_retry": False, "schema": SCHEMA, "server_uuid": UUID})
    if not execute:
        result = {"status": "DRY_RUN_NOT_WRITTEN", "planned_insert_rows": plan["insert_count"], "database_writes": 0}
        durable_json(out / "receipt.json", result)
        return result
    durable_json(out / "write_attempt_started.json",
                 {"status": "PENDING_RECONCILIATION_UNTIL_VERIFIED", "payload_sha256": sha_value(payload),
                  "package_manifest_sha256": payload["package_manifest_sha256"], "automatic_retry": False})
    try:
        result, info = transport_run(payload, transport)
    except Exception as exc:
        result = {"status": "COMMIT_UNKNOWN", "error_type": type(exc).__name__, "automatic_retry": False}
        info = {"mode": transport, "attempts": 1, "result_unavailable": True}
    if result.get("status") in ("COMMITTED_VERIFIED", "ALREADY_MATCHED_VERIFIED"):
        try:
            modules, _ = dependencies()
            require(result.get("package_manifest_sha256") == payload["package_manifest_sha256"], "RETURN_PACKAGE_MISMATCH")
            validate_snapshot(result, payload["expected_structure"], modules["mysql_load_core"], modules["append_load_core"])
        except Exception as exc:
            result["status"] = "POST_COMMIT_VERIFICATION_FAILED"
            result["error_type"] = type(exc).__name__
    durable_json(out / "transport.json", info)
    durable_json(out / "receipt.json", result)
    return result

def reconcile(publish_output_dir, expected_structure, output_dir, *, transport="server-local"):
    """Read-only resolution. Never retries the write or edits the original attempt."""
    source = Path(publish_output_dir)
    frozen = read_json(source / "frozen_payload.json")
    execution = read_json(source / "execution_manifest.json")
    pending = read_json(source / "write_attempt_started.json")
    require(frozen.get("action") == "publish", "RECONCILIATION_ACTION")
    require(sha_value(frozen) == execution["payload_sha256"] == pending["payload_sha256"],
            "RECONCILIATION_PAYLOAD_CHANGED")
    require(frozen["expected_structure"] == load_structure(expected_structure), "RECONCILIATION_STRUCTURE_CHANGED")
    result = snapshot(output_dir, expected_structure, transport=transport)
    review = {"operation": "READ_ONLY_COMMIT_RECONCILIATION", "automatic_retry": False,
              "database_writes": 0, "original_publish_directory": str(source),
              "payload_sha256": execution["payload_sha256"], "status": "UNRESOLVED_READBACK_FAILED"}
    if result["status"] == "READ_ONLY_SNAPSHOT_VERIFIED":
        modules, _ = dependencies()
        core, app = modules["mysql_load_core"], modules["append_load_core"]
        before = app.normalize_snapshot(frozen["before"], result["metadata"])
        require(core.sha(before) == frozen["baseline_canonical_sha256"], "RECONCILIATION_BASELINE_CHANGED")
        package = {t: frozen["package"][t] for t in core.TABLES}
        plan = app.prepare_plan(package, result["metadata"], before)
        verify = app.compare_after(result["tables"], result["metadata"], before, plan)
        review["comparison"] = verify
        if verify["state"] == "MATCH":
            review["status"] = "RECONCILED_EXPECTED_STATE_VERIFIED"
            review["semantic_note"] = "Expected committed content observed; no database transaction ID invented."
        elif app.normalize_snapshot(result["tables"], result["metadata"]) == before:
            review["status"] = "RECONCILED_BASELINE_UNCHANGED"
        else:
            review["status"] = "UNRESOLVED_CONTENT_CONFLICT"
    durable_json(Path(output_dir) / "reconciliation.json", review)
    return review


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("snapshot", "publish"):
        p = sub.add_parser(name)
        p.add_argument("--structure", required=True)
        p.add_argument("--output", required=True)
        p.add_argument("--transport", choices=("server-local", "ssh"), default="server-local")
        if name == "publish":
            p.add_argument("--package", required=True)
            p.add_argument("--before", required=True)
            p.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.action == "snapshot":
            result = snapshot(args.output, args.structure, transport=args.transport)
        else:
            result = publish(args.package, args.before, args.structure, args.output, execute=args.execute, transport=args.transport)
        print(json.dumps({k: result.get(k) for k in ("status", "row_counts", "total_rows", "planned_insert_rows")}, ensure_ascii=False))
        return 0 if result["status"] in ("READ_ONLY_SNAPSHOT_VERIFIED", "COMMITTED_VERIFIED", "ALREADY_MATCHED_VERIFIED", "DRY_RUN_NOT_WRITTEN") else 1
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED", "error_type": type(exc).__name__}))
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
