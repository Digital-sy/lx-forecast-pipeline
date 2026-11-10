"""Synthetic regression tests; no database, network, or source mutation."""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/build_load_package.py"
spec = importlib.util.spec_from_file_location("load_builder", SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
DDL = Path(__file__).resolve().parents[1] / "sql/lingxing_garment_order_schema_v1_1.sql"

def fixture():
    digest = "a" * 64
    docs, downloads, rows = [], [], []
    for token in ("synthetic_a", "synthetic_b"):
        version = m.stable_id("synthetic_version", token, digest)
        metadata = {"token": token, "name": token + ".xlsx", "path": "/synthetic/" + token,
                    "modified_time": "1794208444", "created_time": "1794208443", "type": "file"}
        dl = {"token": token, "status": "ok", "sha256": digest, "bytes": 10, "local_path": "synthetic/" + token + ".xlsx",
              "format": "xlsx", "inspect": {"checked_at": "2026-11-09T10:00:00+00:00"}, "acquired_at": "2026-11-09T10:01:00+00:00"}
        audit = {"source_sha256": digest, "extraction_version": "test/1", "sheet_audits": []}
        docs.append({"source_id": token, "content_sha256": digest, "version_id": version, "file_verification": "HASH_MATCH",
                     "parse_status": "ok", "file_audit": audit, "inventory": metadata,
                     "file_name": token + ".xlsx", "source_url": "https://example.invalid/" + token,
                     "same_content_source_ids": ["synthetic_a", "synthetic_b"], "metadata_status": "ARCHIVED_ONLY",
                     "inventory_as_of": "2026-11-09T09:59:00+00:00", "metadata_changes": []})
        downloads.append(dl)
        rows.append({"source_id": token, "version_id": version, "content_sha256": digest, "evidence_id": m.stable_id("legacy", token),
                     "future_applicability": "NOT_DECIDED_BY_INDEX", "raw": {"token": token, "source_sha256": digest,
                        "line_id": token + ":1:2:3", "line_type": "order_color", "quantity": None if token.endswith("a") else 0,
                        "quantity_reliable": None if token.endswith("a") else True, "sheet": "test", "sheet_index": 1,
                        "row": 3, "table_header_row": 2, "order_date": None, "filename_date": None, "color_code": "BK",
                        "spu": "TEST", "evidence_cells": [], "fabric_assignment_sources": [], "issues": ["synthetic"],
                        "file_issues": [], "order_date_source": "unresolved"}})
    plan = {"scope": [{"token": x} for x in ("synthetic_a", "synthetic_b")], "created_at": "2026-11-09T10:00:00+00:00"}
    acquisition = {"status": "COMPLETE", "failed": 0, "not_run": 0, "source_count": 2, "downloaded": 2,
                   "finished_at": "2026-11-09T10:02:00+00:00", "snapshot_limitations": "synthetic"}
    parser = {"status": "PARSED", "returncode": 0, "line_count": 2, "parser_sha256": "b"*64, "helper_sha256": "c"*64,
              "inventory_sha256": "d"*64, "downloads_sha256": "e"*64, "plan_sha256": "f"*64, "command": ["synthetic"],
              "output_sha256": {"order_lines.jsonl": "1"*64}, "started_at": "2026-11-09T10:03:00+00:00",
              "finished_at": "2026-11-09T10:04:00+00:00"}
    index = {"summary": {"documents": 2, "order_rows": 2, "enriched_rows": 0, "confirmations": 0}}
    hashes = {"download_manifest": "2"*64, "parse_manifest": "3"*64, "index_manifest": "4"*64}
    paths = {"download_manifest": "synthetic/receipt.json", "order_lines": "synthetic/lines.jsonl"}
    return docs, rows, downloads, plan, acquisition, parser, index, hashes, paths

class LoadPackageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = m.parse_schema(DDL.read_text(encoding="utf-8-sig"))

    def test_complete_contract(self):
        package = m.convert_data(*fixture())
        m.validate_rows(package, self.schema)
        self.assertEqual([len(v) for v in package.values()], [1,2,2,2,2,2,0,0,0])

    def test_backtick_quoted_column_contract(self):
        quoted = DDL.read_text(encoding="utf-8-sig")
        self.assertIn("    `row_number` INT", quoted)
        unquoted = quoted.replace("    `row_number` INT", "    row_number INT")
        self.assertNotEqual(unquoted, quoted)
        self.assertEqual(m.parse_schema(unquoted), m.parse_schema(quoted))

    def test_deterministic_rows(self):
        self.assertEqual(m.convert_data(*fixture()), m.convert_data(*fixture()))

    def test_same_bytes_different_source_stay_separate(self):
        package = m.convert_data(*fixture())
        versions = package[m.TABLES[2]]
        self.assertEqual(versions[0]["content_sha256"], versions[1]["content_sha256"])
        self.assertNotEqual(versions[0]["version_id"], versions[1]["version_id"])
        self.assertNotEqual(versions[0]["document_id"], versions[1]["document_id"])

    def test_metadata_change_does_not_redefine_content(self):
        args = fixture()
        original = m.convert_data(*args)
        args[0][0]["file_name"] = "renamed.xlsx"
        args[0][0]["inventory"]["name"] = "renamed.xlsx"
        changed = m.convert_data(*args)
        self.assertEqual(original[m.TABLES[2]], changed[m.TABLES[2]])
        self.assertNotEqual(original[m.TABLES[3]], changed[m.TABLES[3]])

    def test_parse_version_keeps_original_evidence_separate(self):
        args = fixture()
        original = m.convert_data(*args)
        args[7]["parse_manifest"] = "9"*64
        changed = m.convert_data(*args)
        self.assertNotEqual(original[m.TABLES[5]][0]["evidence_id"], changed[m.TABLES[5]][0]["evidence_id"])
        self.assertEqual(original[m.TABLES[2]], changed[m.TABLES[2]])

    def test_unknown_and_zero_distinct(self):
        package = m.convert_data(*fixture())
        unknown, zero = package[m.TABLES[5]]
        self.assertIsNone(unknown["quantity"])
        self.assertEqual(unknown["quantity_state"], "UNKNOWN")
        self.assertEqual(zero["quantity"], "0")
        self.assertEqual(zero["quantity_state"], "KNOWN_ZERO")

    def test_no_inferred_color_system_or_manual_decisions(self):
        package = m.convert_data(*fixture())
        self.assertTrue(all(row["garment_color_system"] is None for row in package[m.TABLES[5]]))
        self.assertTrue(all(not package[t] for t in m.TABLES[6:]))

    def test_unknown_historical_times_not_backfilled(self):
        package = m.convert_data(*fixture())
        self.assertIsNone(package[m.TABLES[1]][0]["first_observed_at_utc"])
        self.assertIsNone(package[m.TABLES[2]][0]["first_acquired_at_utc"])
        self.assertIsNone(package[m.TABLES[4]][0]["started_at_utc"])
        self.assertIsNotNone(package[m.TABLES[3]][0]["acquired_at_utc"])

    def test_version_mismatch_fails(self):
        args = fixture()
        args[1][0]["version_id"] = "0"*64
        with self.assertRaises(m.ContractError):
            m.convert_data(*args)

    def test_outside_scope_fails(self):
        args = fixture()
        args[3]["scope"].pop()
        with self.assertRaises(m.ContractError):
            m.convert_data(*args)

    def test_duplicate_source_line_fails(self):
        package = m.convert_data(*fixture())
        duplicate = copy.deepcopy(package[m.TABLES[5]][0])
        duplicate["evidence_id"] = "0"*64
        package[m.TABLES[5]].append(duplicate)
        with self.assertRaises(m.ContractError):
            m.validate_rows(package, self.schema)

    def test_orphan_fk_fails(self):
        package = m.convert_data(*fixture())
        package[m.TABLES[5]][0]["extraction_id"] = "0"*64
        with self.assertRaises(m.ContractError):
            m.validate_rows(package, self.schema)

    def test_unknown_column_fails(self):
        package = m.convert_data(*fixture())
        package[m.TABLES[5]][0]["guessed_mapping"] = True
        with self.assertRaises(m.ContractError):
            m.validate_rows(package, self.schema)

    def test_no_silent_decimal_rounding(self):
        self.assertEqual(m.quantity_value("1.00000001"), ("1.00000001", "KNOWN_VALUE"))
        for value in ("1.000000001", "NaN", True, "10000000000000000"):
            with self.assertRaises(m.ContractError):
                m.quantity_value(value)

    def test_invalid_business_date_not_repaired(self):
        for value in ("202601109", "2026/11/09", "2026-02-30"):
            with self.assertRaises(ValueError):
                m.date_value(value)

    def test_unzoned_timestamp_rejected(self):
        with self.assertRaises(m.ContractError):
            m.utc_value("2026-11-09T10:00:00")

    def test_overwrite_refused_before_source_reads(self):
        with tempfile.TemporaryDirectory() as output:
            with self.assertRaisesRegex(m.ContractError, "refusing overwrite"):
                m.build_package("does-not-exist", "does-not-exist", output)

    def test_unexpected_ddl_rejected(self):
        with self.assertRaises(m.ContractError):
            m.parse_schema("CREATE TABLE bad (id INT);")

if __name__ == "__main__":
    unittest.main()
