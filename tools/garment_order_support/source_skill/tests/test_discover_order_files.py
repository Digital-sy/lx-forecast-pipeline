import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / "scripts" / "discover_order_files.py"
spec = importlib.util.spec_from_file_location("discover_order_files", MODULE)
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


def item(token="file1", name="synthetic.xlsx", kind="file", **extra):
    return {"token": token, "name": name, "type": kind, **extra}


def page(files=None, more=False, nxt=None, **extra):
    data = {"files": files or [], "has_more": more, **extra}
    if nxt is not None:
        data["next_page_token"] = nxt
    return subprocess.CompletedProcess([], 0, json.dumps({"ok": True, "data": data}), "")


class QueueRunner:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def __call__(self, argv, timeout):
        self.calls.append((argv, timeout))
        result = self.pages.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        root = Path(os.environ.get("GARMENT_SOURCE_TEST_TMP", str(Path(__file__).parents[1] / ".test_tmp")))
        root.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name) / "run"

    def run_pages(self, pages, **kwargs):
        runner = QueueRunner(pages)
        result = d.discover(folder_token="synthetic_root", output_dir=self.out,
                            cli_prefix=["node", "cli.js"], execute_read=True, runner=runner, **kwargs)
        return result, runner

    def test_default_is_plan_and_never_dispatches(self):
        def forbidden(*args):
            self.fail("default plan dispatched network")
        result = d.discover(folder_token="root", output_dir=self.out, runner=forbidden)
        self.assertEqual(result["status"], "READ_PLAN_ONLY")
        self.assertEqual(result["external_calls"], 0)
        self.assertIsNone(result["acquired_at"])

    def test_recursive_pagination_and_shortcut_scope(self):
        result, runner = self.run_pages([
            page([item(), item("folder2", "child", "folder"),
                  item("shortcut3", "link", "shortcut", shortcut_info={"target_token":"external", "target_type":"folder"})], True, "cursor1"),
            page([item("sheet4", "sheet", "sheet")]), page([item("file5")])])
        self.assertTrue(result["complete"])
        self.assertEqual(len(result["items"]), 5)
        params = [json.loads(a[a.index("--params") + 1]) for a, _ in runner.calls]
        self.assertEqual([x["folder_token"] for x in params], ["synthetic_root", "synthetic_root", "folder2"])
        self.assertEqual(params[1]["page_token"], "cursor1")
        self.assertEqual(result["transport"], "INJECTED_TEST_RUNNER")
        self.assertEqual(result["external_calls"], 0)
        self.assertEqual(result["runner_calls"], 3)

    def test_written_file_hashes_and_canonical_hashes_are_distinct_and_reproducible(self):
        result, _ = self.run_pages([page([item()])])
        manifest = json.loads((self.out / "manifest.json").read_text(encoding="utf-8"))
        for stem, name in [("config", "query_config.json"), ("inventory", "inventory_current.json"),
                           ("page_receipts", "page_receipts.json")]:
            raw = (self.out / name).read_bytes()
            self.assertEqual(manifest[stem + "_file_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(manifest[stem + "_canonical_sha256"], d.digest(json.loads(raw)))
            self.assertNotEqual(manifest[stem + "_file_sha256"], manifest[stem + "_canonical_sha256"])
        receipt = json.loads((self.out / "page_receipts.json").read_text(encoding="utf-8"))[0]
        raw = (self.out / receipt["page_file"]).read_bytes()
        self.assertEqual(receipt["file_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(receipt["content_canonical_sha256"], d.digest(json.loads(raw)))

    def test_plan_manifest_hashes_exact_written_bytes(self):
        d.discover(folder_token="root", output_dir=self.out)
        manifest = json.loads((self.out / "manifest.json").read_text(encoding="utf-8"))
        for stem, name in [("config", "query_config.json"), ("inventory", "inventory_current.json")]:
            raw = (self.out / name).read_bytes()
            self.assertEqual(manifest[stem + "_file_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(manifest[stem + "_canonical_sha256"], d.digest(json.loads(raw)))

    def test_paths_preserve_legacy_root_and_parent_convention(self):
        result, _ = self.run_pages([page([item("child", "US", "folder")]), page([item()])])
        self.assertEqual(result["root"], result["root_token"])
        self.assertEqual(result["items"][0]["parent_path"], "")
        self.assertEqual(result["items"][0]["path"], "/US")
        self.assertEqual(result["items"][1]["parent_path"], "/US")
        self.assertEqual(result["items"][1]["path"], "/US/synthetic.xlsx")

    def test_missing_page_cursor_is_incomplete(self):
        result, runner = self.run_pages([page([item()], True)])
        self.assertFalse(result["complete"])
        self.assertEqual(result["items"], [])
        self.assertEqual(result["errors"][0]["kind"], "NEXT_PAGE_TOKEN_MISSING")
        self.assertEqual(len(runner.calls), 1)

    def test_repeated_page_cursor_is_incomplete(self):
        result, runner = self.run_pages([page([item()], True, "x"), page([item("b")], True, "x")])
        self.assertEqual(result["errors"][0]["kind"], "REPEATED_PAGE_TOKEN")
        self.assertEqual(len(result["items"]), 1)

    def test_duplicate_token_same_page_blocked(self):
        result, _ = self.run_pages([page([item(), item()])])
        self.assertEqual(result["errors"][0]["kind"], "DUPLICATE_TOKEN_IN_PAGE")
        self.assertEqual(len(result["items"]), 0)

    def test_duplicate_token_across_pages_blocked(self):
        result, _ = self.run_pages([page([item()], True, "x"), page([item()])])
        self.assertEqual(result["errors"][0]["kind"], "DUPLICATE_TOKEN_ACROSS_PAGES")
        self.assertEqual(len(result["items"]), 1)

    def test_page_budget_checkpoint_not_complete(self):
        result, runner = self.run_pages([page([item()], True, "x")], max_pages=1)
        self.assertEqual(len(runner.calls), 1)
        self.assertEqual(result["errors"][0]["kind"], "PAGE_BUDGET_EXHAUSTED")
        self.assertEqual(result["pending_folders"][0]["page_token"], "x")

    def test_folder_budget_checkpoint_not_complete(self):
        result, runner = self.run_pages([page([item("folder2", "child", "folder")])], max_folders=1)
        self.assertFalse(result["complete"])
        self.assertEqual(len(runner.calls), 1)
        self.assertEqual(result["pending_folders"][0]["folder_token"], "folder2")

    def test_item_budget_does_not_partially_apply_page(self):
        result, _ = self.run_pages([page([item(), item("b")])], max_items=1)
        self.assertEqual(result["errors"][0]["kind"], "ITEM_BUDGET_EXHAUSTED")
        self.assertEqual(result["items"], [])

    def test_permission_failure_no_retry_no_secret_echo(self):
        secret = "test_secret_must_not_escape"
        error = subprocess.CompletedProcess([], 1, "", json.dumps({"ok":False,"error":{"code":403,"message":secret}}))
        result, runner = self.run_pages([error])
        self.assertEqual(len(runner.calls), 1)
        self.assertEqual(result["errors"][0]["code"], 403)
        for p in self.out.rglob("*.json"):
            self.assertNotIn(secret, p.read_text(encoding="utf-8"))

    def test_output_directory_cannot_be_overwritten(self):
        self.out.mkdir()
        p = self.out / "protected.txt"
        p.write_text("original")
        with self.assertRaises(FileExistsError):
            d.discover(folder_token="root", output_dir=self.out)
        self.assertEqual(p.read_text(), "original")

    def test_explicit_root_scope_required(self):
        with self.assertRaises(d.DiscoveryError):
            d.discover(folder_token="", output_dir=self.out)
        self.assertFalse(self.out.exists())

    def test_wrong_parent_blocked(self):
        result, _ = self.run_pages([page([item(parent_token="not_requested")])])
        self.assertEqual(result["errors"][0]["kind"], "ITEM_PARENT_OUT_OF_SCOPE")

    def test_missing_has_more_not_assumed_last_page(self):
        proc = subprocess.CompletedProcess([],0,json.dumps({"ok":True,"data":{"files":[]}}),"")
        result, _ = self.run_pages([proc])
        self.assertEqual(result["errors"][0]["kind"], "PAGE_CONTRACT_INVALID")

    def test_empty_complete_folder_is_valid(self):
        result, _ = self.run_pages([page()])
        self.assertTrue(result["complete"])
        self.assertEqual(result["items"], [])

    def test_cycle_blocked(self):
        result, _ = self.run_pages([page([item("synthetic_root", "back", "folder")])])
        self.assertEqual(result["errors"][0]["kind"], "FOLDER_CYCLE_OR_SELF_REFERENCE")

    def test_same_token_conflict_in_different_folder_is_preserved_failure(self):
        result, _ = self.run_pages([page([item(),item("child", "c", "folder")]),page([item(name="changed.xlsx")])])
        self.assertEqual(result["errors"][0]["kind"], "TOKEN_METADATA_CONFLICT")
        self.assertEqual(result["items"][0]["name"], "synthetic.xlsx")

    def test_same_metadata_alias_kept_without_second_resource(self):
        result, _ = self.run_pages([page([item(),item("child", "c", "folder")]),page([item()])])
        self.assertTrue(result["complete"])
        self.assertEqual(len(result["items"]),2)
        self.assertEqual(result["items"][0]["aliases"],[{"parent_token":"child","path":"/c/synthetic.xlsx"}])

    def test_schema_page_size_limit(self):
        with self.assertRaises(d.DiscoveryError):
            d.discover(folder_token="root",output_dir=self.out,page_size=201)
        self.assertFalse(self.out.exists())

    def test_shell_wrapper_refused_for_real_execution(self):
        with self.assertRaises(d.DiscoveryError):
            d.discover(folder_token="root",output_dir=self.out,execute_read=True,cli_prefix=["lark-cli.cmd"])
        self.assertFalse(self.out.exists())

    def test_timeout_records_failure_not_complete(self):
        result, _ = self.run_pages([d.DiscoveryError("CLI_TIMEOUT")])
        self.assertEqual(result["errors"][0]["kind"],"CLI_TIMEOUT")
        self.assertFalse(result["complete"])

    def test_only_expected_cli_operation_dispatched(self):
        result, runner = self.run_pages([page()])
        argv, timeout = runner.calls[0]
        self.assertEqual(argv[2:5],["drive","files","list"])
        self.assertIn("--as",argv)
        self.assertEqual(argv[argv.index("--as")+1],"user")
        self.assertNotIn("--page-all",argv)
        self.assertEqual(timeout,120)

    def test_untrusted_url_credentials_blocked(self):
        result, _ = self.run_pages([page([item(url="https://name:password@example.com/file/a")])])
        self.assertEqual(result["errors"][0]["kind"],"INVALID_ITEM_URL")

    def test_unknown_metadata_remains_unknown_and_url_query_not_saved(self):
        result,_ = self.run_pages([page([item(url="https://example.feishu.cn/file/a?auth=secret", access_token="hidden")])])
        row=result["items"][0]
        self.assertIsNone(row["modified_time"])
        self.assertEqual(row["url"],"https://example.feishu.cn/file/a")
        for p in self.out.rglob("*.json"):
            self.assertNotIn("secret",p.read_text(encoding="utf-8"))
            self.assertNotIn("hidden",p.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
