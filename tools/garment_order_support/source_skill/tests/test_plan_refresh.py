"""Behavioral tests for read-only incremental planning; synthetic inputs only."""
from __future__ import annotations
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import os
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("plan_refresh", Path(__file__).parents[1] / "scripts" / "plan_refresh.py")
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)
HASH_A, HASH_B = "a" * 64, "b" * 64


def item(token="doc-a", **fields):
    return {"token": token, "type": "file", "name": "synthetic.xlsx", "modified_time": "100", **fields}


def inv(items=None, **fields):
    return {"root": "synthetic-root", "items": [] if items is None else items, "complete": True, "errors": [], **fields}


def one(before, after, **kwargs):
    return m.plan_documents(inv([before]), inv([after]), **kwargs)[0][0]


class PlanTests(unittest.TestCase):
    def test_new_document_requires_fetch(self):
        rows, stats = m.plan_documents(inv(), inv([item()]))
        self.assertEqual((rows[0]["state"], rows[0]["action"]), ("NEW", "FETCH_REQUIRED"))
        self.assertEqual(stats["current_items"], 1)

    def test_metadata_changed_even_if_local_exists(self):
        before = item(local_path="already-downloaded.xlsx")
        after = {**before, "modified_time": "200"}
        row = one(before, after)
        self.assertEqual(row["state"], "METADATA_CHANGED")
        self.assertEqual(row["action"], "FETCH_REQUIRED")

    def test_name_changed_requires_verification(self):
        row = one(item(), item(name="new-name.xlsx"))
        self.assertEqual(row["action"], "FETCH_REQUIRED")

    def test_same_metadata_does_not_prove_content(self):
        row = one(item(), item())
        self.assertEqual(row["state"], "METADATA_UNCHANGED_CONTENT_UNVERIFIED")
        self.assertEqual(row["action"], "VERIFY_CONTENT_REQUIRED")

    def test_missing_modified_time_not_reused(self):
        row = one(item(modified_time=None), item(modified_time=None))
        self.assertIn("MODIFIED_TIME_UNKNOWN", row["flags"])
        self.assertEqual(row["action"], "VERIFY_CONTENT_REQUIRED")

    def test_confirmed_content_unchanged(self):
        row = one(item(sha256=HASH_A), item(sha256=HASH_A), previous_parser_version="v1", parser_version="v1")
        self.assertEqual(row["state"], "CONTENT_UNCHANGED_VERIFIED")
        self.assertEqual(row["action"], "REUSE_VERIFIED_CONTENT")

    def test_new_content_with_same_metadata(self):
        row = one(item(sha256=HASH_A), item(sha256=HASH_B))
        self.assertEqual(row["state"], "CONTENT_CHANGED")
        self.assertEqual(row["action"], "FETCH_REQUIRED")

    def test_equal_hash_does_not_skip_changed_metadata(self):
        row = one(item(sha256=HASH_A), item(sha256=HASH_A, modified_time="200"))
        self.assertEqual(row["action"], "FETCH_REQUIRED")
        self.assertIn("SAME_CONTENT_HASH_BUT_METADATA_CHANGED", row["flags"])

    def test_parser_change_requires_reparse_verified_bytes(self):
        row = one(item(sha256=HASH_A), item(sha256=HASH_A), previous_parser_version="v1", parser_version="v2")
        self.assertEqual(row["state"], "REPARSE_REQUIRED")

    def test_parser_change_does_not_make_unknown_bytes_verified(self):
        row = one(item(), item(), previous_parser_version="v1", parser_version="v2")
        self.assertEqual(row["action"], "VERIFY_CONTENT_REQUIRED")
        self.assertIn("REPARSE_AFTER_CONTENT_VERIFIED", row["flags"])

    def test_unknown_parser_blocks_unconditional_reuse(self):
        row = one(item(sha256=HASH_A), item(sha256=HASH_A))
        self.assertEqual(row["action"], "VERIFY_PARSER_BEFORE_REUSE")

    def test_complete_same_scope_absence_is_flag_not_delete(self):
        rows, _ = m.plan_documents(inv([item()]), inv())
        self.assertEqual(rows[0]["state"], "SOURCE_MISSING")
        self.assertEqual(rows[0]["action"], "FLAG_SOURCE_MISSING_RETAIN_ARCHIVE")
        self.assertFalse(rows[0]["executed"])

    def test_partial_inventory_absence_unknown(self):
        rows, _ = m.plan_documents(inv([item()]), inv(complete=False))
        self.assertEqual(rows[0]["state"], "SOURCE_PRESENCE_UNKNOWN")

    def test_errors_override_complete_flag(self):
        rows, _ = m.plan_documents(inv([item()]), inv(errors=[{"folder": "failed"}]))
        self.assertEqual(rows[0]["state"], "SOURCE_PRESENCE_UNKNOWN")

    def test_different_root_absence_unknown(self):
        rows, stats = m.plan_documents(inv([item()]), inv(root="different-root"))
        self.assertFalse(stats["same_scope"])
        self.assertEqual(rows[0]["state"], "SOURCE_PRESENCE_UNKNOWN")

    def test_changed_filters_absence_unknown(self):
        rows, _ = m.plan_documents(inv([item()], filters={"kind": "all"}), inv(filters={"kind": "xlsx"}))
        self.assertEqual(rows[0]["state"], "SOURCE_PRESENCE_UNKNOWN")

    def test_unknown_root_absence_unknown(self):
        rows, _ = m.plan_documents(inv([item()], root=None), inv(root=None))
        self.assertEqual(rows[0]["state"], "SOURCE_PRESENCE_UNKNOWN")

    def test_exact_duplicates_are_counted_not_multiplied(self):
        rows, stats = m.plan_documents(inv(), inv([item(), item()]))
        self.assertEqual(len(rows), 1)
        self.assertEqual(stats["current_exact_duplicate_items"], 1)

    def test_conflicting_duplicate_token_rejected(self):
        with self.assertRaises(m.PlanError):
            m.plan_documents(inv(), inv([item(), item(name="conflict.xlsx")]))

    def test_invalid_or_conflicting_hash_rejected(self):
        for row in [item(sha256="short"), item(sha256=HASH_A, content_sha256=HASH_B)]:
            with self.subTest(row=row), self.assertRaises(m.PlanError):
                m.plan_documents(inv(), inv([row]))

    def test_folders_never_imply_child_content_reuse(self):
        row = one(item(type="folder"), item(type="folder"))
        self.assertEqual(row["action"], "DISCOVER_CHILDREN_REQUIRED")
        self.assertIn("FOLDER_METADATA_DOES_NOT_PROVE_CHILD_CONTENT", row["flags"])

    def test_type_change_does_not_guess_download_method(self):
        row = one(item(type="file"), item(type="sheet"))
        self.assertEqual(row["action"], "REVIEW_RESOURCE_TYPE_REQUIRED")

    def test_cancellation_is_separate_status_review(self):
        old = {"orders": {"PO-test": {"order_status_text": "待到货"}}, "source_acquired_at": "2026-01-01T00:00:00Z"}
        new = {"orders": {"PO-test": {"order_status_text": "已作废"}}, "source_acquired_at": "2026-01-02T00:00:00Z"}
        rows, _ = m.plan_status(old, new)
        self.assertEqual(rows[0]["state"], "STATUS_CHANGED")
        self.assertEqual(rows[0]["document_action"], "NO_IMPLIED_CONTENT_CHANGE")
        self.assertEqual(rows[0]["action"], "REVIEW_ELIGIBILITY_AND_REFERENCED_CONFIRMATIONS")

    def test_new_acquisition_time_is_not_business_change(self):
        old = {"orders": {"P": {"order_status_text": "已完成", "source_row_indices": [1], "source_acquired_at": "a"}}}
        new = {"orders": {"P": {"order_status_text": "已完成", "source_row_indices": [9], "source_acquired_at": "b"}}}
        rows, _ = m.plan_status(old, new)
        self.assertEqual(rows[0]["state"], "STATUS_UNCHANGED")

    def test_status_absence_never_proves_cancellation(self):
        rows, _ = m.plan_status({"orders": {"P": {"order_status_text": "已完成"}}}, {"orders": {}})
        self.assertEqual(rows[0]["state"], "STATUS_PRESENCE_UNKNOWN")
        self.assertIsNone(rows[0]["current_status"])

    def test_shop_proof_change_requires_review(self):
        old = {"orders": {"P": {"by_spu": {"S": {"shops": {"shop-a": {}}}}}}}
        new = {"orders": {"P": {"by_spu": {"S": {"shops": {"shop-b": {}}}}}}}
        rows, _ = m.plan_status(old, new)
        self.assertEqual(rows[0]["state"], "STATUS_CHANGED")

    def test_half_status_pair_is_invalid(self):
        with self.assertRaises(m.PlanError):
            m.plan_status({"orders": {}}, None)

    def test_inputs_not_mutated(self):
        previous, current = inv([item()]), inv([item(modified_time="200")])
        originals = copy.deepcopy([previous, current])
        m.plan_documents(previous, current)
        self.assertEqual([previous, current], originals)

    def test_requires_explicit_complete_boolean(self):
        for flag in [None, 1, "true"]:
            with self.subTest(flag=flag), self.assertRaises(m.PlanError):
                m.plan_documents(inv(), inv(complete=flag))


class FileTests(unittest.TestCase):
    def setUp(self):
        base = os.environ.get("ORDER_SOURCE_TEST_ROOT")
        if not base:
            raise RuntimeError("Set ORDER_SOURCE_TEST_ROOT to an authorized task-specific test directory")
        root = Path(base).resolve() / "refresh"
        root.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix="case_", dir=root)
        self.base = Path(self.tmp.name)
        self.prev, self.curr = self.base / "previous.json", self.base / "current.json"
        self.prev.write_text(json.dumps(inv([item()])), encoding="utf-8")
        self.curr.write_text(json.dumps(inv([item(modified_time="200")])), encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def test_output_contains_hashes_and_sources_unchanged(self):
        oldhash = [m.file_hash(self.prev), m.file_hash(self.curr)]
        result = m.run(self.prev, self.curr, self.base / "out")
        self.assertEqual(result["status"], "PLAN_ONLY_NOT_SYNCED")
        self.assertEqual(oldhash, [m.file_hash(self.prev), m.file_hash(self.curr)])
        self.assertFalse(result["cursor_advanced"])
        self.assertFalse(result["manual_confirmation_mutated"])
        self.assertEqual(result["database_writes"], 0)
        for name, sha in result["outputs"].items():
            self.assertEqual(m.file_hash(self.base / "out" / name), sha)

    def test_refuses_existing_output_and_preserves_it(self):
        out = self.base / "out"
        out.mkdir()
        keep = out / "original.txt"
        keep.write_text("unchanged", encoding="utf-8")
        with self.assertRaises(m.PlanError):
            m.run(self.prev, self.curr, out)
        self.assertEqual(keep.read_text(), "unchanged")

    def test_repeat_plan_has_same_digest_no_new_actions(self):
        first = m.run(self.prev, self.curr, self.base / "first")
        second = m.run(self.prev, self.curr, self.base / "second")
        self.assertEqual(first["plan_digest"], second["plan_digest"])
        self.assertEqual((self.base / "first" / "actions.jsonl").read_bytes(), (self.base / "second" / "actions.jsonl").read_bytes())

    def test_changed_input_detected_before_output(self):
        real = m.plan_documents
        def mutation(*args, **kwargs):
            result = real(*args, **kwargs)
            self.curr.write_text(json.dumps(inv()), encoding="utf-8")
            return result
        with patch.object(m, "plan_documents", mutation), self.assertRaises(m.PlanError):
            m.run(self.prev, self.curr, self.base / "out")
        self.assertFalse((self.base / "out").exists())

    def test_duplicate_json_key_rejected(self):
        self.curr.write_text('{"root":"a","root":"b","items":[],"complete":true}', encoding="utf-8")
        with self.assertRaises(m.PlanError):
            m.run(self.prev, self.curr, self.base / "out")
        self.assertFalse((self.base / "out").exists())

    def test_cli_runs_actual_path(self):
        with patch("builtins.print") as captured:
            result = m.main(["--previous", str(self.prev), "--current", str(self.curr), "--output-dir", str(self.base / "cli")])
        self.assertEqual(result, 0)
        self.assertTrue((self.base / "cli" / "manifest.json").is_file())
        self.assertIn("PLAN_ONLY_NOT_SYNCED", captured.call_args[0][0])

    def test_status_pair_only_option_rejected(self):
        with self.assertRaises(m.PlanError):
            m.run(self.prev, self.curr, self.base / "out", previous_status_path=self.prev)
        self.assertFalse((self.base / "out").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
