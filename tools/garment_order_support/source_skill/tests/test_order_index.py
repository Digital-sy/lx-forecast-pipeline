"""Synthetic local tests; each run is isolated and retained under explicit E: temp root."""
from __future__ import annotations
import copy
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch
import uuid

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "order_index.py"
spec = importlib.util.spec_from_file_location("order_index", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

class IndexTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base = os.environ.get("ORDER_SOURCE_TEST_ROOT")
        if not base:
            raise RuntimeError("Set ORDER_SOURCE_TEST_ROOT to an authorized task-specific test directory")
        cls.root = Path(base).resolve() / ("run_" + uuid.uuid4().hex)
        cls.root.mkdir(parents=True, exist_ok=False)

    def setUp(self):
        self.work = self.root / self._testMethodName
        self.work.mkdir()
        self.raw = self.work / "source.xlsx"
        self.raw.write_bytes(b"synthetic source bytes; not a real Excel workbook")
        self.sha = mod.sha256_file(self.raw)
        self.paths = {key: self.work / (key + (".jsonl" if key in ("downloads", "file_audit", "order_lines") else ".json")) for key in mod.REQUIRED + mod.OPTIONAL}
        self.inventory = {"root": "synthetic-root", "items": [{"token": "f1", "type": "file", "name": "BX285 order.xlsx", "url": "https://example.invalid/file/f1", "modified_time": "100"}], "complete": True, "errors": [], "as_of_utc": "2026-01-01T00:00:00+00:00"}
        self.downloads = [{"token": "f1", "status": "ok", "sha256": self.sha, "local_path": str(self.raw), "modified_time": "100", "type": "file"}]
        self.audits = [{"token": "f1", "local_path": str(self.raw), "source_sha256": self.sha, "spus": ["BX285"], "order_ids": ["PO1"], "status": "ok", "market": "美国站"}]
        self.rows = [{"line_id": "f1:0:1:2", "token": "f1", "source_sha256": self.sha, "spu": "BX285", "spu_candidates": ["BX285"], "order_id": "PO1", "order_id_candidates": ["PO1"], "sheet": "Order", "row": 2, "quantity": 0, "color_raw": "黑色", "color_code": "BK", "fabric_header": "013普通", "fabric_row_note": "7#", "market": "美国站", "order_date": None, "issues": [], "duplicate_line_of": None}]
        self.enriched = {"orders": [{"line_id": "f1:0:1:2", "source_sha256": self.sha, "spu": "BX285", "order_id": "PO1", "color_code": "BK", "order_color": "黑色", "header_material_raw": "013普通", "physical_numbers": [{"number": "7", "part": "主布"}], "conflicts": ["future applicability unknown"], "future_default_confirmed": False}]}
        self.manual = {"master": [{"id": "review-1", "spu": "BX285", "source_color": "A/BK", "material": "013普通", "order_line_ids": ["f1:0:1:2"], "future_default_confirmed": False, "manual_resolution": {"decision_verbatim": "采用候选", "confirmed_physical_color": "7#黑色", "full_garment_recipe_confirmed": False}}]}
        self.status = {"source_acquired_at": "2026-01-01T00:00:00+00:00", "orders": {"PO1": {"order_status_text": "已完成", "by_spu": {"BX285": {"shops": {"JQ-US": [0]}, "unmapped_sid_row_indices": []}}}}}
        self.config = self.work / "config.json"
        self.save()

    def save(self):
        for key, value in [("inventory", self.inventory), ("enriched_evidence", self.enriched), ("manual_evidence", self.manual), ("status_index", self.status)]:
            mod.write_json(self.paths[key], value)
        for key, rows in [("downloads", self.downloads), ("file_audit", self.audits), ("order_lines", self.rows)]:
            self.paths[key].write_text("".join(mod.canonical(row) + "\n" for row in rows), encoding="utf-8")
        mod.write_json(self.config, {**{key: path.name for key, path in self.paths.items()}, "verify_raw_files": True})

    def build(self):
        output = self.work / "index"
        result = mod.build_index(self.config, output)
        return output, result

    def query(self, index, filters, name="query", limit=1000):
        return mod.query_index(index, self.work / name, filters, limit)

    def test_import_has_no_network_or_subprocess_side_effects(self):
        with patch.object(socket, "socket", side_effect=AssertionError("network")), patch.object(subprocess, "run", side_effect=AssertionError("subprocess")):
            sp = importlib.util.spec_from_file_location("isolated_index_import", SCRIPT)
            sp.loader.exec_module(importlib.util.module_from_spec(sp))

    def test_build_keeps_zero_unknown_and_manual_separate(self):
        before = {str(p): mod.sha256_file(p) for p in [self.raw, self.config, *self.paths.values()]}
        index, result = self.build()
        rows = mod.read_jsonl(index / "order_rows.jsonl")
        self.assertEqual(rows[0]["raw"]["quantity"], 0)
        self.assertIsNone(rows[0]["raw"]["order_date"])
        self.assertNotIn("manual_resolution", rows[0]["raw"])
        confirmed = mod.read_jsonl(index / "confirmations.jsonl")[0]
        self.assertFalse(confirmed["original_future_default_confirmed"])
        self.assertEqual(confirmed["manual_resolution"], self.manual["master"][0]["manual_resolution"])
        self.assertEqual(before, {p: mod.sha256_file(p) for p in before})
        self.assertEqual(result["summary"]["order_rows"], 1)

    def test_old_manual_confirmation_flags_changed_source_version(self):
        self.manual["orders"] = [{"line_id": "f1:0:1:2", "source_sha256": "0" * 64}]
        self.save()
        index, _ = self.build()
        confirmation = mod.read_jsonl(index / "confirmations.jsonl")[0]
        self.assertEqual(confirmation["confirmation_reference_status"], "REVIEW_REQUIRED")
        self.assertEqual(confirmation["source_version_links"][0]["version_status"], "MISMATCH")
        self.assertFalse(confirmation["confirmation_applied_to_current_source"])
        result = self.query(index, {"spu": ["BX285"]})
        self.assertTrue(result["confirmations"][0]["query_time_review_required"])
        self.assertEqual(result["confirmations"][0]["manual_resolution"], self.manual["master"][0]["manual_resolution"])

    def test_matching_manual_source_version_is_recorded_not_applied(self):
        self.manual["orders"] = [{"line_id": "f1:0:1:2", "source_sha256": self.sha}]
        self.save()
        index, _ = self.build()
        confirmation = mod.read_jsonl(index / "confirmations.jsonl")[0]
        self.assertEqual(confirmation["confirmation_reference_status"], "REFERENCED_FILE_VERSIONS_MATCH")
        self.assertFalse(confirmation["confirmation_applied_to_current_source"])

    def test_incomplete_inventory_schema_cannot_claim_complete(self):
        self.inventory.pop("items")
        self.save()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Inventory requires"):
            self.build()

    def test_same_token_conflicting_success_is_rejected(self):
        self.downloads.append({**self.downloads[0], "sha256": "f" * 64})
        self.save()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Conflicting successful versions"):
            self.build()
        self.assertFalse((self.work / "index").exists())

    def test_retry_same_content_does_not_duplicate_rows(self):
        self.downloads.insert(0, {"token": "f1", "status": "failed", "error": "synthetic failure"})
        self.downloads.append(copy.deepcopy(self.downloads[1]))
        self.save()
        _, result = self.build()
        self.assertEqual(result["summary"]["documents"], 1)
        self.assertEqual(result["summary"]["order_rows"], 1)

    def test_missing_raw_is_not_validated(self):
        self.audits[0]["local_path"] = str(self.work / "absent.xlsx")
        self.downloads[0]["local_path"] = self.audits[0]["local_path"]
        self.save()
        index, result = self.build()
        self.assertEqual(result["summary"]["file_verification"], {"LOCAL_MISSING": 1})
        self.assertFalse(mod.read_jsonl(index / "order_rows.jsonl")[0]["snapshot_validated"])

    def test_changed_raw_is_not_validated(self):
        self.raw.write_bytes(b"changed")
        index, result = self.build()
        self.assertEqual(result["summary"]["file_verification"], {"HASH_MISMATCH": 1})
        self.assertFalse(mod.read_jsonl(index / "order_rows.jsonl")[0]["snapshot_validated"])

    def test_source_changed_after_index_detected_during_query(self):
        index, _ = self.build()
        self.raw.write_bytes(b"changed after indexing")
        result = self.query(index, {"spu": ["BX285"]})
        self.assertEqual(result["documents"][0]["query_time_file_verification"], "HASH_MISMATCH")
        self.assertFalse(result["order_rows"][0]["snapshot_validated"])

    def test_exact_spu_does_not_match_prefix_or_country(self):
        self.rows[0]["spu"] = "EUBX285"
        self.rows[0]["spu_candidates"] = ["EUBX285"]
        self.audits[0]["spus"] = ["EUBX285"]
        self.enriched["orders"][0]["spu"] = "EUBX285"
        self.save()
        index, _ = self.build()
        result = self.query(index, {"spu": ["BX285"]})
        self.assertEqual(result["totals_before_limit"]["documents"], 0)
        self.assertEqual(result["totals_before_limit"]["order_rows"], 0)

    def test_and_filters_do_not_join_different_rows(self):
        second = copy.deepcopy(self.rows[0]); second.update(line_id="f1:0:1:3", row=3, spu="BX286", spu_candidates=["BX286"], color_raw="白色", color_code="WH")
        self.rows.append(second); self.audits[0]["spus"].append("BX286")
        self.save()
        index, _ = self.build()
        result = self.query(index, {"spu": ["BX285"], "color": ["白色"]})
        self.assertEqual(result["totals_before_limit"]["order_rows"], 0)
        self.assertEqual(result["totals_before_limit"]["documents"], 0)

    def test_or_same_dimension_and_text_search_not_identity(self):
        index, _ = self.build()
        result = self.query(index, {"spu": ["BX999", "BX285"], "material": ["013"]})
        self.assertEqual(result["totals_before_limit"]["order_rows"], 1)
        self.assertIn("TEXT_RETRIEVAL_ONLY_NOT_IDENTITY_MATCH", result["order_rows"][0]["query_match_reasons"])

    def test_duplicate_order_line_identity_rejected(self):
        self.rows.append(copy.deepcopy(self.rows[0])); self.save()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Duplicate order_line"):
            self.build()

    def test_mismatched_enriched_version_rejected(self):
        self.enriched["orders"][0]["source_sha256"] = "0" * 64; self.save()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Enriched source version mismatch"):
            self.build()

    def test_mismatched_raw_row_version_rejected(self):
        self.rows[0]["source_sha256"] = "0" * 64; self.save()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Row source hash differs"):
            self.build()

    def test_no_filter_is_rejected(self):
        index, _ = self.build()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "At least one explicit"):
            self.query(index, {})

    def test_immutable_outputs(self):
        index, _ = self.build()
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Output already exists"):
            mod.build_index(self.config, index)
        self.query(index, {"spu": ["BX285"]})
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Output already exists"):
            self.query(index, {"spu": ["BX285"]})

    def test_index_tampering_is_rejected(self):
        index, _ = self.build()
        with (index / "order_rows.jsonl").open("a", encoding="utf-8") as stream:
            stream.write("{}\n")
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Index was modified"):
            self.query(index, {"spu": ["BX285"]})

    def test_metadata_change_requires_recheck(self):
        self.inventory["items"][0]["modified_time"] = "200"; self.save()
        index, _ = self.build()
        doc = mod.read_jsonl(index / "documents.jsonl")[0]
        self.assertEqual(doc["metadata_status"], "CHANGED_RECHECK_REQUIRED")
        self.assertEqual(doc["metadata_changes"], ["modified_time"])

    def test_declared_source_hash_is_enforced(self):
        config = mod.read_json(self.config); config["expected_sha256"] = {"order_lines": "0" * 64}; mod.write_json(self.config, config)
        with self.assertRaisesRegex(mod.IndexErrorDetail, "Input checksum mismatch"):
            self.build()

    def test_no_hash_mode_cannot_claim_snapshot_validated(self):
        config = mod.read_json(self.config); config["verify_raw_files"] = False; mod.write_json(self.config, config)
        index, _ = self.build()
        self.assertFalse(mod.read_jsonl(index / "order_rows.jsonl")[0]["snapshot_validated"])

    def test_source_registry_change_during_build_rejected(self):
        original = mod.sha256_file
        calls = CounterLike()
        def changing(path):
            key = str(Path(path).resolve())
            calls[key] += 1
            if key == str(self.paths["inventory"].resolve()) and calls[key] > 1:
                return "0" * 64
            return original(path)
        with patch.object(mod, "sha256_file", side_effect=changing):
            with self.assertRaisesRegex(mod.IndexErrorDetail, "Archive changed during build"):
                self.build()

    def test_source_status_does_not_become_default_approval(self):
        index, _ = self.build()
        row = mod.read_jsonl(index / "order_rows.jsonl")[0]
        self.assertEqual(row["order_status_evidence"]["order_status_text"], "已完成")
        self.assertEqual(row["future_applicability"], "NOT_DECIDED_BY_INDEX")
        self.assertEqual(row["shop_evidence"]["shops"], {"JQ-US": [0]})
        self.assertNotIn("shop", row["raw"])

    def test_limit_reports_untruncated_totals(self):
        other = copy.deepcopy(self.rows[0]); other.update(line_id="f1:0:1:3", row=3)
        self.rows.append(other); self.save()
        index, _ = self.build()
        result = self.query(index, {"spu": ["BX285"]}, limit=1)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["totals_before_limit"]["order_rows"], 2)
        self.assertEqual(len(result["order_rows"]), 1)

    def test_same_content_different_resource_preserved_without_row_fanout(self):
        self.inventory["items"].append({**self.inventory["items"][0], "token": "f2"})
        self.downloads.append({**self.downloads[0], "token": "f2"})
        self.audits.append({**self.audits[0], "token": "f2", "spus": [], "order_ids": []})
        self.save()
        index, manifest = self.build()
        self.assertEqual(manifest["summary"]["documents"], 2)
        self.assertEqual(manifest["summary"]["distinct_content_hashes"], 1)
        self.assertEqual(manifest["summary"]["order_rows"], 1)
        self.assertEqual(mod.read_jsonl(index / "documents.jsonl")[0]["same_content_source_ids"], ["f1", "f2"])

    def test_filename_only_is_explicit_and_not_spu_inference(self):
        self.rows=[]; self.enriched={"orders": []}; self.audits[0]["spus"]=[]; self.manual={"master": []}; self.save()
        index, _ = self.build()
        result = self.query(index, {"file_name": ["BX285"]})
        self.assertEqual(result["totals_before_limit"]["documents"], 1)
        self.assertEqual(result["documents"][0]["query_match_scope"], "FILE_AUDIT_CANDIDATE_ONLY")
        exact = self.query(index, {"spu": ["BX285"]}, name="exact")
        self.assertEqual(exact["totals_before_limit"]["documents"], 0)

class CounterLike(dict):
    def __missing__(self, key):
        return 0

if __name__ == "__main__":
    unittest.main()
