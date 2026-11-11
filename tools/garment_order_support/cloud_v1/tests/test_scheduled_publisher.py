"""Adapter-only synthetic tests: no SSH, API, or database connections."""
import copy
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock
import sys

TASK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TASK / "scripts"))
from publisher import adapter

class Cursor:
    def __init__(self, connection): self.connection = connection
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql, args=None): self.connection.statements.append((sql, args))
    def fetchall(self): return []
    def fetchone(self): return ("ignored",)
class Connection:
    def __init__(self): self.statements = []; self.closed = False; self.rollbacks = 0
    def cursor(self, *args): return Cursor(self)
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed = True

class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.mods, self.hashes = adapter.dependencies()
        self.core = self.mods["mysql_load_core"]
        self.app = self.mods["append_load_core"]
        self.structure = {t: {} for t in self.core.TABLES}
        self.rows = {t: [] for t in self.core.TABLES}
        self.metadata = {t: [] for t in self.core.TABLES}
        normal = self.app.normalize_snapshot(self.rows, self.metadata)
        self.before = {"status": "READ_ONLY_SNAPSHOT_VERIFIED", "schema": adapter.SCHEMA,
                       "server_uuid": adapter.UUID, "structure": self.structure,
                       "tables": self.rows, "metadata": self.metadata, "row_counts": {t:0 for t in self.core.TABLES}, "total_rows": 0, "canonical_sha256": self.core.sha(normal),
                       "canonical_table_sha256": {t: self.core.sha(normal[t]) for t in self.core.TABLES}}
        self.bp = self.root / "before.json"
        self.bp.write_text(json.dumps(self.before), encoding="utf-8")
        self.package = self.root / "package"
        self.package.mkdir()
        source = self.root / "original.xlsx"
        source.write_bytes(b"SYNTHETIC_ORIGINAL_NOT_BUSINESS_DATA")
        self.manifest = {"ddl_sha256": self.core.DDL_SHA256, "table_order": list(self.core.TABLES),
                         "production_before": {"sha256": adapter.sha_file(self.bp)},
                         "source_files": {str(source): adapter.sha_file(source)}, "tables": {}}
        for table in self.core.TABLES:
            file = self.package / (table + ".jsonl")
            file.write_bytes(b"")
            self.manifest["tables"][table] = {"file": file.name, "sha256": adapter.sha_file(file), "row_count": 0}
        self.save_manifest()
    def tearDown(self): self.tmp.cleanup()
    def save_manifest(self):
        (self.package / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")
    def worker(self):
        worker = adapter.load_worker()
        conn = Connection()
        patches = [
            patch.object(worker, "factory", return_value=conn),
            patch.object(worker, "structure_guard", return_value=self.structure),
            patch.object(worker, "explain_tables", return_value={}),
            patch.object(worker.core, "schema_metadata", return_value=self.metadata),
            patch.object(worker.app, "read_snapshot", return_value=self.rows),
        ]
        for item in patches:
            item.start(); self.addCleanup(item.stop)
        return worker, conn

    def test_01_pins_exact_unchanged(self):
        self.assertEqual(self.hashes["append_load_core.py"], "196b7d4b8c805bd14dca7531be0c26651853c187697b61abe2e251db5e3b594a")
    def test_02_import_inert(self):
        with patch("socket.create_connection", side_effect=AssertionError("NO_NETWORK")), patch("subprocess.run", side_effect=AssertionError("NO_PROCESS")):
            adapter.dependencies(); adapter.load_worker()
    def test_03_dry_run_never_transports(self):
        with patch.object(adapter, "transport_run") as network:
            receipt = adapter.publish(self.package, self.bp, self.structure, self.root / "dry")
            self.assertEqual(receipt["status"], "DRY_RUN_NOT_WRITTEN"); network.assert_not_called()
    def test_04_existing_output_refused(self):
        with self.assertRaises(FileExistsError):
            adapter.publish(self.package, self.bp, self.structure, self.root)
    def test_05_manual_rows_blocked(self):
        table = self.core.TABLES[7]
        file = self.package / (table + ".jsonl")
        file.write_text('{"confirmation_id":"not-authorized"}\n', encoding="utf-8")
        self.manifest["tables"][table].update(row_count=1, sha256=adapter.sha_file(file))
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, "MATERIAL_MANUAL_REFERENCE_INSERT_FORBIDDEN"):
            adapter.prepare_publish(self.package, self.bp, self.structure)
    def test_06_baseline_file_binding(self):
        self.bp.write_text(json.dumps(self.before, indent=2), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "PACKAGE_BASELINE_FILE_MISMATCH"):
            adapter.prepare_publish(self.package, self.bp, self.structure)
    def test_07_source_hash_drift(self):
        Path(next(iter(self.manifest["source_files"]))).write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "PACKAGE_SOURCE_CHANGED"):
            adapter.prepare_publish(self.package, self.bp, self.structure)
    def test_08_incomplete_scope_rejected(self):
        bad = dict(self.structure); bad.pop(next(iter(bad)))
        with self.assertRaisesRegex(ValueError, "EXPECTED_STRUCTURE_SCOPE"):
            adapter.load_structure(bad)
    def test_09_tampered_baseline_digest(self):
        bad = dict(self.before, canonical_sha256="0" * 64)
        with self.assertRaisesRegex(ValueError, "BASELINE_DIGEST_CHANGED"):
            adapter.validate_snapshot(bad, self.structure, self.core, self.app)
    def test_10_backup_and_pending_exist_before_attempt(self):
        out = self.root / "run"
        def check(payload, transport):
            self.assertTrue((out / "before_snapshot.json").is_file())
            self.assertTrue((out / "frozen_payload.json").is_file())
            self.assertTrue((out / "write_attempt_started.json").is_file())
            return {"status": "COMMIT_UNKNOWN"}, {"mode": transport, "attempts": 1}
        with patch.object(adapter, "transport_run", side_effect=check) as run:
            result = adapter.publish(self.package, self.bp, self.structure, out, execute=True)
            self.assertEqual(result["status"], "COMMIT_UNKNOWN"); self.assertEqual(run.call_count, 1)
    def test_11_transport_exception_never_retried(self):
        with patch.object(adapter, "transport_run", side_effect=TimeoutError("secret never emitted")) as run:
            result = adapter.publish(self.package, self.bp, self.structure, self.root / "unknown", execute=True)
            self.assertEqual(result["status"], "COMMIT_UNKNOWN"); self.assertEqual(run.call_count, 1)
            self.assertNotIn("secret", json.dumps(result))
    def test_12_read_only_snapshot_not_committed(self):
        worker, conn = self.worker()
        receipt = worker.read_only_snapshot(self.structure)
        self.assertEqual(receipt["status"], "READ_ONLY_SNAPSHOT_VERIFIED")
        self.assertEqual(receipt["database_writes"], 0)
        self.assertTrue(any(sql == "START TRANSACTION READ ONLY" for sql, _ in conn.statements))
        self.assertFalse(any(sql.startswith(("INSERT", "DELETE", "UPDATE", "CREATE")) for sql, _ in conn.statements))
    def test_13_worker_calls_existing_core_once(self):
        payload, _, _, _ = adapter.prepare_publish(self.package, self.bp, self.structure)
        worker, _ = self.worker()
        with patch.object(worker.app, "load_append", return_value={"status": "COMMIT_UNKNOWN", "commit_resolution": "COMMITTED_VERIFIED"}) as load:
            result = worker.execute_payload(payload)
            self.assertEqual(result["status"], "COMMIT_UNKNOWN")
            load.assert_called_once()
            self.assertEqual(load.call_args.args[1], "lingxing")
    def test_14_guard_failure_does_not_call_writer(self):
        payload, _, _, _ = adapter.prepare_publish(self.package, self.bp, self.structure)
        worker, _ = self.worker()
        with patch.object(worker, "structure_guard", side_effect=ValueError("STRUCTURE_CHANGED")), patch.object(worker.app, "load_append") as load:
            result = worker.execute_payload(payload)
            self.assertEqual(result["status"], "BLOCKED_NO_WRITE"); load.assert_not_called()
    def test_15_payload_hash_checked_before_connection(self):
        payload, _, _, _ = adapter.prepare_publish(self.package, self.bp, self.structure)
        payload["package_content_sha256"] = "wrong"
        worker = adapter.load_worker()
        with patch.object(worker, "factory") as connect:
            result = worker.execute_payload(payload)
            self.assertEqual(result["status"], "BLOCKED_NO_WRITE"); connect.assert_not_called()
    def test_16_target_mismatch_no_connection(self):
        worker = adapter.load_worker()
        with patch.object(worker, "factory") as connect, self.assertRaisesRegex(ValueError, "PAYLOAD_TARGET_MISMATCH"):
            worker.execute_payload({"schema": "other", "server_uuid": adapter.UUID, "action": "snapshot"})
        connect.assert_not_called()
    def test_17_unknown_error_sanitized(self):
        worker = adapter.load_worker()
        self.assertEqual(worker.safe_error(RuntimeError("password=secret")), {"error_type": "RuntimeError"})
    def test_18_ssh_timeout_hash_only(self):
        import subprocess
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("ssh", 600, output=b"sensitive stdout", stderr=b"secret stderr")) as run:
            result, info = adapter.transport_run(adapter.payload_base("publish", self.structure), "ssh")
            self.assertEqual(result["status"], "COMMIT_UNKNOWN"); self.assertEqual(run.call_count, 1)
            self.assertNotIn("secret stderr", json.dumps(info)); self.assertNotIn("sensitive stdout", json.dumps(info))
    def test_19_declared_success_missing_readback_rejected(self):
        fake = dict(self.before, status="COMMITTED_VERIFIED", package_manifest_sha256=adapter.sha_file(self.package / "manifest.json"))
        with patch.object(adapter, "transport_run", return_value=(fake, {"attempts": 1})):
            result = adapter.publish(self.package, self.bp, self.structure, self.root / "bad-success", execute=True)
            self.assertEqual(result["status"], "POST_COMMIT_VERIFICATION_FAILED")
    def test_20_no_provider_fallback_or_schema_override(self):
        worker = adapter.load_worker()
        with patch.object(worker.provider, "load_connection_config", side_effect=ValueError("CONFIG_VERSION_CHANGED")), patch.dict(sys.modules, {"pymysql": Mock()}) as _:
            with self.assertRaisesRegex(ValueError, "CONFIG_VERSION_CHANGED"):
                worker.factory()
    def test_21_package_order_survives_transport_serialization(self):
        payload, _, _, _ = adapter.prepare_publish(self.package, self.bp, self.structure)
        roundtrip = json.loads(adapter.canonical(payload))
        worker, _ = self.worker()
        with patch.object(worker.app, "load_append", return_value={"status": "COMMIT_UNKNOWN"}) as load:
            worker.execute_payload(roundtrip)
            self.assertEqual(tuple(load.call_args.args[2]), self.core.TABLES)
    def test_22_explain_missing_estimate_blocks(self):
        worker = adapter.load_worker()
        conn = Connection()
        class BadCursor(Cursor):
            def fetchone(self): return ('{"query_block":{}}',)
        conn.cursor = lambda: BadCursor(conn)
        metadata = {t: [{"COLUMN_NAME": self.core.PK[t]}] for t in self.core.TABLES}
        with self.assertRaisesRegex(ValueError, "EXPLAIN_BUDGET_OR_SHAPE"):
            worker.explain_tables(conn, metadata)
    def test_23_stale_schema_stops_snapshot(self):
        worker, _ = self.worker()
        with patch.object(worker, "structure_guard", side_effect=ValueError("STRUCTURE_CHANGED")):
            result = worker.read_only_snapshot(self.structure)
            self.assertEqual(result["status"], "BLOCKED_READ_ONLY")
    def test_24_post_commit_readback_mismatch_not_success(self):
        payload, _, _, _ = adapter.prepare_publish(self.package, self.bp, self.structure)
        worker, _ = self.worker()
        with patch.object(worker.app, "load_append", return_value={"status": "COMMITTED_VERIFIED"}), patch.object(worker.app, "compare_after", return_value={"state":"MISMATCH"}):
            result = worker.execute_payload(payload)
            self.assertEqual(result["status"], "POST_COMMIT_VERIFICATION_FAILED")

    def test_25_reconcile_no_write_or_retry(self):
        out = self.root / "unknown-publish"
        with patch.object(adapter, "transport_run", return_value=({"status":"COMMIT_UNKNOWN"}, {"attempts":1})):
            adapter.publish(self.package, self.bp, self.structure, out, execute=True)
        target = self.root / "reconcile"
        def readonly(output_dir, structure, **kwargs):
            Path(output_dir).mkdir()
            return self.before
        with patch.object(adapter, "snapshot", side_effect=readonly) as read, patch.object(adapter, "publish") as write:
            review = adapter.reconcile(out, self.structure, target)
            self.assertEqual(review["status"], "RECONCILED_EXPECTED_STATE_VERIFIED")
            self.assertEqual(review["database_writes"], 0); write.assert_not_called(); read.assert_called_once()
    def test_26_reconcile_payload_tampering_stops_before_read(self):
        out = self.root / "tamper-publish"
        with patch.object(adapter, "transport_run", return_value=({"status":"COMMIT_UNKNOWN"}, {"attempts":1})):
            adapter.publish(self.package, self.bp, self.structure, out, execute=True)
        payload = adapter.read_json(out / "frozen_payload.json")
        payload["server_uuid"] = "tampered"
        (out / "frozen_payload.json").write_text(json.dumps(payload), encoding="utf-8")
        with patch.object(adapter, "snapshot") as read, self.assertRaisesRegex(ValueError, "RECONCILIATION_PAYLOAD_CHANGED"):
            adapter.reconcile(out, self.structure, self.root / "never-read")
        read.assert_not_called()
    def test_27_factory_checks_uuid_and_tls(self):
        worker = adapter.load_worker()
        class IdentityCursor(Cursor):
            def fetchone(self):
                sql = self.connection.statements[-1][0]
                if sql.startswith("SELECT @@server_uuid"):
                    return ("wrong-instance", "lingxing", "8.0.39")
                return ("Ssl_cipher", "cipher")
        conn = Connection()
        conn.cursor = lambda: IdentityCursor(conn)
        fake = Mock(); fake.connect.return_value = conn
        with patch.dict(sys.modules, {"pymysql":fake}), patch.object(worker.provider,"load_connection_config",return_value=({}, {})):
            with self.assertRaisesRegex(ValueError,"TARGET_IDENTITY_CHANGED"):
                worker.factory()
        self.assertTrue(conn.closed)
    def test_28_factory_no_tls_rejected(self):
        worker = adapter.load_worker()
        class IdentityCursor(Cursor):
            def fetchone(self):
                sql = self.connection.statements[-1][0]
                if sql.startswith("SELECT @@server_uuid"):
                    return (adapter.UUID, "lingxing", "8.0.39")
                return ("Ssl_cipher", "")
        conn = Connection()
        conn.cursor = lambda: IdentityCursor(conn)
        fake = Mock(); fake.connect.return_value = conn
        with patch.dict(sys.modules, {"pymysql":fake}), patch.object(worker.provider,"load_connection_config",return_value=({}, {})):
            with self.assertRaisesRegex(ValueError,"TLS_REQUIRED"):
                worker.factory()
        self.assertTrue(conn.closed)

    def test_29_declared_rowcounts_cannot_disagree(self):
        bad = copy.deepcopy(self.before)
        bad["row_counts"][self.core.TABLES[0]] = 1
        with self.assertRaisesRegex(ValueError, "BASELINE_ROW_COUNTS_CHANGED"):
            adapter.validate_snapshot(bad, self.structure, self.core, self.app)
    def test_30_declared_total_cannot_disagree(self):
        bad = dict(self.before, total_rows=1)
        with self.assertRaisesRegex(ValueError, "BASELINE_TOTAL_CHANGED"):
            adapter.validate_snapshot(bad, self.structure, self.core, self.app)

    def test_31_external_uppercase_valueerror_cannot_be_echoed(self):
        worker = adapter.load_worker()
        self.assertEqual(worker.safe_error(ValueError("PRIVATESECRETABC123")), {"error_type":"ValueError"})
        self.assertEqual(worker.safe_error(AssertionError("PRIVATESECRETABC123")), {"error_type":"AssertionError"})
    def test_32_only_local_worker_contract_reason_is_returned(self):
        worker = adapter.load_worker()
        error = worker.WorkerContractError("STRUCTURE_CHANGED")
        self.assertEqual(worker.safe_error(error)["reason_code"],"STRUCTURE_CHANGED")

if __name__ == "__main__":
    unittest.main()
