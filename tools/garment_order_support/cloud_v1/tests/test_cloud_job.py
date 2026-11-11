"""Pure injected-operation tests for cloud_job. No live clients or DB connections."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

TASK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TASK / "scripts"))
import cloud_job as job

class SyntheticOperations:
    """Explicit test double; files are synthetic manifests, never business evidence."""
    def __init__(self, before_status="READ_ONLY_SNAPSHOT_VERIFIED", discovery_status="DISCOVERED_NOT_PREPARED",
                 prepared=True, publish_status="COMMITTED_VERIFIED"):
        self.calls = []
        self.before_status = before_status
        self.discovery_status = discovery_status
        self.prepared = prepared
        self.publish_status = publish_status
        self.throw = None
        self.mutate_after_publish = None
        self.snapshot_file = None
        self.pipeline_configs = []
    def preflight(self, *args, **kwargs):
        self.calls.append(("preflight", args))
        return {"status":"DIRECTORY_READABLE", "access_ok":True, "synthetic":True}
    def snapshot(self, out, structure):
        self.calls.append(("snapshot", out))
        self.snapshot_file = Path(out) / "receipt.json"
        data = {"status":self.before_status, "synthetic":True, "tables":{}, "metadata":{}}
        job.write(self.snapshot_file, data)
        return data
    def stage(self, cfg, out, stage, prior=None):
        self.calls.append((stage, prior))
        self.pipeline_configs.append(job.read(cfg))
        Path(out).mkdir(parents=True, exist_ok=False)
        if stage == "DISCOVER":
            return {"status":self.discovery_status, "plan":{"review_required":["SOURCE_CHANGED_PRESERVE_MANUAL"]}}
        inventory = Path(out) / "next_inventory.json"
        job.write(inventory, {"status":"COMPLETE", "synthetic":True, "objects":[{"token":"synthetic-next"}]})
        response = {"status":"PREPARED_NOT_PUBLISHED" if self.prepared else "NO_INCREMENT_NOT_PUBLISHED",
                    "next_inventory_candidate":str(inventory), "next_inventory_sha256":job.sha(inventory)}
        if self.prepared:
            package = Path(out) / "package"
            package.mkdir()
            job.write(package / "manifest.json", {"synthetic":True,
                      "production_before":job.spec(self.snapshot_file)})
            response.update(package=str(package), package_manifest_sha256=job.sha(package / "manifest.json"))
        return response
    def publish(self, package, before, structure, out):
        self.calls.append(("publish", package))
        self.before_received = Path(before)
        self.package_received = Path(package)
        if self.throw:
            raise self.throw
        if self.mutate_after_publish:
            self.mutate_after_publish.write_text("changed", encoding="utf-8")
        Path(out).mkdir(parents=True, exist_ok=False)
        data = {"status":self.publish_status, "synthetic":True}
        job.write(Path(out) / "receipt.json", data)
        return data

class CloudJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.state_root = self.root / "state"; self.state_root.mkdir()
        self.pin = self.root / "immutable_code.py"; self.pin.write_text("# synthetic code\n", encoding="utf-8")
        self.initial = self.root / "initial.json"; job.write(self.initial, {"status":"COMPLETE", "synthetic":True})
        self.structure = self.root / "structure.json"; job.write(self.structure, {"synthetic":True})
        self.template = self.root / "template.json"
        job.write(self.template, {"task_root":"unused", "sources":{}, "synthetic":True})
        self.cfg = {"schema_version":"garment_cloud_job/1", "state_root":str(self.state_root),
                    "code_pins":[job.spec(self.pin)], "runtime_entry":job.spec(self.pin),
                    "template":job.spec(self.template), "structure":job.spec(self.structure),
                    "initial_inventory":job.spec(self.initial), "execution_enabled":True}
        self.config = self.root / "config.json"; job.write(self.config, self.cfg)
    def tearDown(self): self.tmp.cleanup()
    def save_config(self):
        self.config.write_text(json.dumps(self.cfg), encoding="utf-8")
    def test_01_default_check_is_offline(self):
        ops = SyntheticOperations()
        with patch.object(job, "LiveOperations", side_effect=AssertionError("NO_LIVE_INIT")):
            receipt = job.run(self.config, ops=ops)
        self.assertEqual(receipt["status"], "CHECK_ONLY"); self.assertEqual(ops.calls, [])
        self.assertFalse((self.state_root / "runs").exists())
    def test_02_execution_requires_config_enabled(self):
        self.cfg["execution_enabled"] = False; self.save_config()
        ops = SyntheticOperations()
        with self.assertRaisesRegex(ValueError, "EXECUTION_DISABLED"):
            job.run(self.config, execute=True, ops=ops)
        self.assertEqual(ops.calls, [])
    def test_03_existing_job_lock_blocks_without_calls(self):
        ops = SyntheticOperations()
        with job.JobLock(self.state_root / "job.lock"):
            with self.assertRaises(FileExistsError):
                job.run(self.config, execute=True, ops=ops)
        self.assertEqual(ops.calls, [])
    def test_04_pending_from_previous_run_blocks(self):
        job.write(self.state_root / "pending_publication.json", {"synthetic":True, "status":"COMMIT_UNKNOWN"})
        ops = SyntheticOperations()
        with self.assertRaisesRegex(ValueError, "PENDING_PUBLICATION_REQUIRES_RECONCILIATION"):
            job.run(self.config, execute=True, ops=ops)
        self.assertEqual(ops.calls, [])
        self.assertTrue((self.state_root / "pending_publication.json").is_file())
    def test_05_verified_publish_advances_and_clears_pending(self):
        ops = SyntheticOperations()
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"], "COMMITTED_VERIFIED")
        self.assertTrue(receipt["cursor_advanced"])
        self.assertFalse((self.state_root / "pending_publication.json").exists())
        state = job.read(self.state_root / "state.json")
        self.assertEqual(job.sha(state["inventory"]["path"]), state["inventory"]["sha256"])
        self.assertTrue((Path(state["last_run"]) / "pending_resolved.json").is_file())
    def test_06_same_fresh_snapshot_flows_to_package_and_publisher(self):
        ops = SyntheticOperations()
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"], "COMMITTED_VERIFIED")
        for cfg in ops.pipeline_configs:
            self.assertEqual(cfg["sources"]["publication_receipt"], job.spec(ops.snapshot_file))
        self.assertEqual(ops.before_received, ops.snapshot_file)
        package_manifest = job.read(ops.package_received / "manifest.json")
        self.assertEqual(package_manifest["production_before"], job.spec(ops.snapshot_file))
        self.assertEqual(job.read(ops.snapshot_file)["status"], "READ_ONLY_SNAPSHOT_VERIFIED")
    def test_07_commit_unknown_is_sticky_across_run(self):
        ops = SyntheticOperations(publish_status="COMMIT_UNKNOWN")
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"], "BLOCKED"); self.assertFalse(receipt["cursor_advanced"])
        self.assertFalse((self.state_root / "state.json").exists())
        again = SyntheticOperations()
        with self.assertRaisesRegex(ValueError,"PENDING_PUBLICATION_REQUIRES_RECONCILIATION"):
            job.run(self.config, execute=True, ops=again)
        self.assertEqual(again.calls, [])
    def test_08_failed_publish_keeps_old_inventory(self):
        prior = {"inventory":job.spec(self.initial), "last_status":"PREVIOUS_SYNTHETIC"}
        job.write(self.state_root / "state.json", prior)
        before = (self.state_root / "state.json").read_bytes()
        ops = SyntheticOperations(publish_status="ROLLED_BACK_VERIFIED")
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertEqual(before, (self.state_root / "state.json").read_bytes())
    def test_09_failed_snapshot_stops_before_discovery(self):
        ops = SyntheticOperations(before_status="BLOCKED_READ_ONLY")
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertFalse(any(x[0] in ("DISCOVER","PREPARE","publish") for x in ops.calls))
        self.assertFalse((self.state_root / "state.json").exists())
    def test_10_incomplete_discovery_cannot_publish_or_advance(self):
        ops = SyntheticOperations(discovery_status="PARTIAL")
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertFalse(any(x[0] in ("PREPARE","publish") for x in ops.calls))
        self.assertFalse((self.state_root / "state.json").exists())
    def test_11_no_increment_never_publishes_and_keeps_review_queue(self):
        ops = SyntheticOperations(prepared=False)
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertNotEqual(receipt["status"],"BLOCKED")
        self.assertTrue(receipt["cursor_advanced"])
        self.assertFalse(any(x[0] == "publish" for x in ops.calls))
        self.assertIn("SOURCE_CHANGED_PRESERVE_MANUAL", json.dumps(receipt))
        self.assertFalse((self.state_root / "pending_publication.json").exists())
    def test_12_code_changes_after_commit_block_cursor(self):
        ops = SyntheticOperations()
        ops.mutate_after_publish = self.pin
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"], "BLOCKED")
        self.assertFalse(receipt["cursor_advanced"])
        self.assertTrue((self.state_root / "pending_publication.json").is_file())
        self.assertFalse((self.state_root / "state.json").exists())
    def test_13_process_interruption_leaves_pending(self):
        ops = SyntheticOperations(); ops.throw = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            job.run(self.config, execute=True, ops=ops)
        self.assertTrue((self.state_root / "pending_publication.json").is_file())
        self.assertFalse((self.state_root / "state.json").exists())
        self.assertFalse((self.state_root / "job.lock").exists())
    def test_14_existing_manual_artifact_never_changed(self):
        human = self.state_root / "manual_review.json"
        job.write(human, {"synthetic":True,"confirmed_physical_code":"C10","sku_confirmed":False})
        before = human.read_bytes()
        ops = SyntheticOperations()
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"COMMITTED_VERIFIED")
        self.assertEqual(before, human.read_bytes())
        self.assertFalse(receipt["manual_tables_modified"])
    def test_15_external_error_text_not_forwarded(self):
        ops = SyntheticOperations(); ops.throw = ValueError("PRIVATESECRETABC123")
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertNotIn("PRIVATESECRETABC123", json.dumps(receipt))
    def test_16_pending_durable_before_publication(self):
        ops = SyntheticOperations()
        original = ops.publish
        def checked_publish(*args):
            pending = job.read(self.state_root / "pending_publication.json")
            self.assertEqual(pending["before"],job.spec(ops.snapshot_file))
            self.assertTrue(Path(pending["package"]["path"]).is_file())
            return original(*args)
        ops.publish = checked_publish
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"COMMITTED_VERIFIED")
    def test_17_already_matched_is_verified_success(self):
        ops = SyntheticOperations(publish_status="ALREADY_MATCHED_VERIFIED")
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"ALREADY_MATCHED_VERIFIED")
        self.assertTrue(receipt["cursor_advanced"])
    def test_18_tampered_inventory_blocks_before_publish(self):
        ops = SyntheticOperations()
        stage = ops.stage
        def bad(*args, **kwargs):
            response = stage(*args, **kwargs)
            if response.get("next_inventory_candidate"):
                Path(response["next_inventory_candidate"]).write_text("changed",encoding="utf-8")
            return response
        ops.stage = bad
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertFalse(any(x[0]=="publish" for x in ops.calls))
        self.assertFalse((self.state_root / "state.json").exists())

    def test_19_permission_failure_precedes_all_db_calls(self):
        ops = SyntheticOperations()
        def deny():
            ops.calls.append(("preflight", "DENIED"))
            return {"status":"DIRECTORY_DENIED"}
        ops.preflight = deny
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertEqual([x[0] for x in ops.calls], ["preflight"])
        self.assertFalse((self.state_root / "state.json").exists())
    def test_20_permission_exception_never_initializes_snapshot(self):
        ops = SyntheticOperations()
        def deny(): raise RuntimeError("sensitive authentication context")
        ops.preflight = deny
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertEqual(ops.calls, [])
        self.assertNotIn("sensitive", json.dumps(receipt))
    def test_21_finish_failure_preserves_commit_receipt_and_pending(self):
        ops = SyntheticOperations()
        def fail(out): raise OSError("synthetic archive unavailable")
        ops.finish = fail
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertEqual(receipt["publication_status"],"COMMITTED_VERIFIED")
        self.assertFalse(receipt["cursor_advanced"])
        self.assertTrue((self.state_root / "pending_publication.json").exists())
        self.assertFalse((self.state_root / "state.json").exists())
        runs = list((self.state_root / "runs").iterdir())
        self.assertEqual(job.read(runs[0] / "receipt.json")["publication_status"], "COMMITTED_VERIFIED")
    def test_22_finish_failure_does_not_replace_primary_failure(self):
        ops = SyntheticOperations(before_status="BLOCKED_READ_ONLY")
        def fail(out): raise OSError("synthetic close failure")
        ops.finish = fail
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"],"BLOCKED")
        self.assertEqual(receipt["error_code"],"FRESH_BASELINE_FAILED")
        self.assertEqual(receipt["archive_error_type"],"OSError")
        self.assertFalse((self.state_root / "state.json").exists())

    def cloud_inprocess(self):
        import io, zipfile
        from contextlib import contextmanager
        from types import SimpleNamespace
        from cloud_read_cli import CloudReadClient
        file_token, root_token = "SyntheticFile123", "SyntheticRoot123"
        url = "https://example.feishu.cn/file/" + file_token
        data = io.BytesIO()
        with zipfile.ZipFile(data,"w") as z:
            z.writestr("xl/workbook.xml","<workbook/>")
            z.writestr("[Content_Types].xml","<Types/>")
        blob = data.getvalue()
        class Response:
            status_code = 200
            def __init__(self, body, content_type="application/json"):
                self.body=body
                self.headers={"content-type":content_type,"content-length":str(len(body))}
            def iter_bytes(self): yield self.body
        class Transport:
            def __init__(self): self.calls=[]; self.closed=False
            @contextmanager
            def stream(self,method,path,**kwargs):
                self.calls.append((method,path))
                if path.endswith("/drive/v1/files"):
                    value={"files":[{"token":file_token,"type":"file","name":"Synthetic.xlsx","url":url,
                            "parent_token":root_token,"created_time":"1","modified_time":"2"}],"has_more":False}
                elif path.endswith("/drive/v1/metas/batch_query"):
                    value={"metas":[{"doc_token":file_token,"doc_type":"file","title":"Synthetic.xlsx"}]}
                elif path.endswith("/drive/v1/files/"+file_token+"/download"):
                    yield Response(blob,"application/octet-stream"); return
                else: raise AssertionError("UNEXPECTED_SYNTHETIC_ROUTE")
                yield Response(json.dumps({"code":0,"data":value}).encode())
            def close(self): self.closed=True
        transport=Transport()
        client=CloudReadClient({"schema_version":"garment_cloud_read/1",
            "allowed_hosts":["example.feishu.cn"],"root_tokens":[root_token],
            "output_root":str(self.state_root),"max_requests":10},lambda:"SYNTHETIC_TEST_TOKEN",transport)
        live=job.LiveOperations.__new__(job.LiveOperations)
        live.reader=client
        live.template={"discovery":{"root_token":root_token}}
        live.pipeline=SimpleNamespace(execute_command=lambda *args: self.fail("unexpected subprocess fallback"))
        tools=TASK/"scripts/runtime_pipeline/tools"
        out=self.state_root/"discovery"
        argv=["python","-B",str(tools/"discover_order_files.py"),"--folder-token",root_token,
              "--output-dir",str(out),"--cli-executable","synthetic","--cli-script","synthetic",
              "--max-pages","3","--max-folders","3","--max-items","10","--page-size","10","--timeout","10"]
        result=live._runner(argv,str(self.state_root),30)
        self.assertEqual(result["returncode"],0)
        return live,transport,out,file_token,url,tools

    def test_23_actual_discovery_runner_uses_inprocess_cloud_client(self):
        with patch("subprocess.run",side_effect=AssertionError("NO_SUBPROCESS")), patch("socket.create_connection",side_effect=AssertionError("NO_NETWORK")):
            live,transport,out,token,url,tools=self.cloud_inprocess()
        inventory=job.read(out/"inventory_current.json")
        self.assertEqual(inventory["status"],"COMPLETE")
        self.assertEqual(inventory["items"][0]["token"],token)
        self.assertEqual(len(transport.calls),1)
        self.assertEqual(live.reader.requests,1)
        self.assertNotIn("SYNTHETIC_TEST_TOKEN", (out/"inventory_current.json").read_text(encoding="utf-8"))
        live.reader.close()
    def test_24_actual_fetch_runner_uses_bound_inventory_and_writes_bytes(self):
        with patch("subprocess.run",side_effect=AssertionError("NO_SUBPROCESS")), patch("socket.create_connection",side_effect=AssertionError("NO_NETWORK")):
            live,transport,out,token,url,tools=self.cloud_inprocess()
            inventory=out/"inventory_current.json"
            live.reader.bind_inventory(inventory,job.sha(inventory))
            allow=self.state_root/"allowlist.json"
            job.write(allow,[{"source_id":token,"token":token,"resource_type":"file","action":"FETCH_REQUIRED",
                             "state":"NEW","current_metadata":{"type":"file","name":"Synthetic.xlsx","url":url}}])
            target=self.state_root/"fetch"
            argv=["python","-B",str(tools/"fetch_increment.py"),"--allowlist",str(allow),
                  "--output-dir",str(target),"--cli-executable","synthetic","--cli-script","synthetic"]
            result=live._runner(argv,str(self.state_root),60)
        self.assertEqual(result["returncode"],0)
        receipt=job.read(target/"download_manifest.json")
        self.assertEqual(receipt["status"],"COMPLETE"); self.assertEqual(receipt["downloaded"],1)
        self.assertEqual(len(transport.calls),3)
        journal=(target/"downloads.jsonl").read_text(encoding="utf-8")
        self.assertNotIn("SYNTHETIC_TEST_TOKEN",journal)
        row=json.loads(journal.strip())
        self.assertEqual(job.sha(row["local_path"]),row["sha256"])
        live.reader.close()
    def test_25_live_finish_closes_reader_when_receipt_write_fails(self):
        from types import SimpleNamespace
        live=job.LiveOperations.__new__(job.LiveOperations)
        closed=[]
        live.reader=SimpleNamespace(requests=0,_token=None,receipts=[],close=lambda:closed.append(True))
        with patch.object(job,"write",side_effect=OSError("synthetic disk failure")), self.assertRaises(OSError):
            live.finish(self.root)
        self.assertEqual(closed,[True])

    def test_26_pending_reparse_advances_only_scan_cursor_without_publish(self):
        ops = SyntheticOperations(prepared=False)
        prior = ops.stage
        items = [{"source_id":"SYNTHETIC_FILE", "reason":"DATE_CONFLICT_REPARSE_REQUIRED", "old_evidence_rows":6}]
        def pending(*args, **kwargs):
            result = prior(*args, **kwargs)
            if result.get("status") == "NO_INCREMENT_NOT_PUBLISHED":
                result.update(status="REPARSE_REQUIRED_NOT_PREPARED", pending_reparse_count=1, pending_reparse_items=items)
            return result
        ops.stage = pending
        receipt = job.run(self.config, execute=True, ops=ops)
        self.assertEqual(receipt["status"], "NO_INCREMENT_PENDING_REPARSE")
        self.assertTrue(receipt["cursor_advanced"])
        self.assertFalse(any(x[0] == "publish" for x in ops.calls))
        self.assertEqual(receipt["pending_reparse_items"], items)
        self.assertEqual(receipt["pending_reparse_count"], 1)
        self.assertFalse(receipt["content_verified_for_metadata_unchanged_files"])
        self.assertFalse((self.state_root / "pending_publication.json").exists())

if __name__ == "__main__":
    unittest.main()
