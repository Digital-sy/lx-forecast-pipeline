"""Synthetic-only tests of immutable historical-input normalization."""
import copy
import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import numpy as np
import pandas as pd
import pyarrow as pa

SPEC = importlib.util.spec_from_file_location("monthly_inputs", Path(__file__).parents[1] / "scripts/bc60_monthly_inputs.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def raw_row(**changes):
    row = {"dt": "2024-01-01", "store_name": "JQ-US", "spu": "P001", "sales_qty": "4",
           "source_rows": 1, "null_qty": "0", "negative_qty": "0", "date_mismatch": "0",
           "deleted_rows": "0", "ad_spend": "1.2", "ad_clicks": "2", "ad_orders": "1",
           "listing_sessions_sum": "9", "order_qty": "2", "min_created_at": "2026-07-01 00:00:00",
           "max_sync": "2026-10-20 10:00:00", "promotion_units": None, "null_promotion": "1"}
    row.update(changes)
    return row


class MonthlyInputsTests(unittest.TestCase):
    def test_integer_units_and_unchanged_raw(self):
        source = [raw_row(), raw_row(spu="P002", sales_qty="0")]
        original = copy.deepcopy(source)
        frame, counts = MODULE.normalize(source)
        self.assertEqual(str(frame.y.dtype), "int64")
        self.assertEqual(frame.sales_state.tolist(), ["OBSERVED", "EXPLICIT_ZERO"])
        self.assertEqual(source, original)
        self.assertTrue(frame.promotion_units.isna().all())

    def test_unknown_not_zero_and_v1_precedence(self):
        frame, _ = MODULE.normalize([raw_row(sales_qty=None), raw_row(spu="P002", null_qty="1", negative_qty="1")])
        self.assertTrue(pd.isna(frame.y.iloc[0]))
        self.assertEqual(frame.sales_state.tolist(), ["UNKNOWN", "UNKNOWN"])
        self.assertEqual(str(frame.y.dtype), "Int64")

    def test_fractional_units_never_truncated(self):
        frame, _ = MODULE.normalize([raw_row(sales_qty="3.5")])
        self.assertEqual(frame.y.iloc[0], 3.5)
        self.assertEqual(str(frame.y.dtype), "float64")

    def test_flags_mapping_and_negative_features(self):
        frame, counts = MODULE.normalize([raw_row(spu="A|B", date_mismatch="1", ad_spend="-3"),
                                         raw_row(spu=None, sales_qty="0")])
        self.assertEqual(frame.sales_state.tolist(), ["INVALID", "EXPLICIT_ZERO"])
        self.assertFalse(frame.mapped.any())
        self.assertTrue(pd.isna(frame.ad_spend.iloc[0]))
        self.assertEqual(counts["negative_covariates_to_unknown"]["ad_spend"], 1)

    def test_duplicate_or_wrong_date_rejected(self):
        with self.assertRaisesRegex(ValueError, "DUPLICATE"):
            MODULE.normalize([raw_row(), raw_row()])
        with self.assertRaisesRegex(ValueError, "INVALID_BUSINESS_DATE"):
            MODULE.normalize([raw_row(dt="2024-01-01 01:00:00")])

    def test_partition_gaps_overlap_and_leap_day(self):
        MODULE.verify_partition_dates([(pd.Timestamp("2024-02-28"), pd.Timestamp("2024-03-01"))], "2024-02-28", "2024-03-01")
        with self.assertRaisesRegex(ValueError, "COVERAGE_MISMATCH"):
            MODULE.verify_partition_dates([(pd.Timestamp("2024-02-28"), pd.Timestamp("2024-02-28"))], "2024-02-28", "2024-03-01")
        with self.assertRaisesRegex(ValueError, "OVERLAPPING"):
            MODULE.verify_partition_dates([(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")),
                                           (pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-03"))], "2024-01-01", "2024-01-03")

    def test_arrow_alignment_unknown_extra_and_integer_preservation(self):
        frame = pd.DataFrame({"y": [1, 2], "spu": ["A", "B"]})
        original = frame.copy(deep=True)
        schema = pa.schema([pa.field("y", pa.int64()), pa.field("spu", pa.string()), pa.field("extra", pa.float64())])
        table = MODULE.align(frame, schema)
        self.assertEqual(table.column("y").type, pa.int64())
        self.assertEqual(table.column("extra").null_count, 2)
        pd.testing.assert_frame_equal(frame, original)
        with self.assertRaisesRegex(ValueError, "NONINTEGRAL"):
            MODULE.align(pd.DataFrame({"y": [1.5]}), pa.schema([pa.field("y", pa.int64())]))

    def test_receipt_hash_range_and_timezone(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            path = folder / "daily_missing_2024-01-01_2024-01-01.json.gz"
            rows = [raw_row()]
            payload = json.dumps(rows).encode()
            sql = "SELECT dt FROM test WHERE dt IN ('2024-01-01')"
            receipt = {"status": "OK", "mode": "select", "acquired_at": "2026-10-20T03:00:00+00:00",
                       "rows_sha256": hashlib.sha256(payload).hexdigest(), "row_count": 1,
                       "request": {"sql": sql}, "sql_sha256": hashlib.sha256(sql.encode()).hexdigest()}
            path.write_bytes(gzip.compress(payload))
            metadata = folder / path.name.replace(".json.gz", ".json")
            metadata.write_text(json.dumps(receipt), encoding="utf-8")
            result = MODULE.read_partition(path, folder)
            self.assertEqual(len(result[0]), 1)
            receipt["acquired_at"] = "2026-10-20T03:00:00"
            metadata.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "TIMEZONE"):
                MODULE.read_partition(path, folder)

    def test_partition_unknown_quantity_is_not_zero_total(self):
        frame, _ = MODULE.normalize([raw_row(sales_qty=None)])
        frame["archive_segment"] = "SYNTHETIC_TEST"
        result = MODULE.partition_summary(frame)
        self.assertTrue(pd.isna(result.raw_aggregate_units.iloc[0]))
        self.assertTrue(pd.isna(result.known_mapped_units.iloc[0]))
        self.assertEqual(result.unknown_rows.iloc[0], 1)

    def test_synthetic_end_to_end_preserves_old_segment_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw, evidence, old = root / "raw", root / "evidence", root / "v1"
            for folder in (raw, evidence, old):
                folder.mkdir()
            source_frame, _ = MODULE.normalize([raw_row(dt="2026-01-01", spu="OLD001", sales_qty="7")])
            source_frame = source_frame.drop(columns=["promotion_units", "null_promotion"])
            source_frame.to_parquet(old / "daily.parquet", index=False)
            pd.DataFrame({"shop": ["JQ-US"], "spu": ["OLD001"], "month": ["2025-12"], "qty": [7]}).to_parquet(old / "monthly_reference.parquet", index=False)
            MODULE.dump(old / "quality.json", {"synthetic": True})
            MODULE.dump(old / "input_manifest.json", {"files": {p.name: MODULE.sha(p) for p in old.iterdir()}})
            rows = [raw_row()]
            payload = json.dumps(rows).encode()
            path = raw / "daily_missing_2024-01-01_2024-01-01.json.gz"
            path.write_bytes(gzip.compress(payload))
            sql = "SELECT dt FROM test WHERE dt IN ('2024-01-01')"
            MODULE.dump(evidence / path.name.replace(".json.gz", ".json"), {
                "status": "OK", "mode": "select", "acquired_at": "2026-10-20T03:00:00+00:00",
                "rows_sha256": hashlib.sha256(payload).hexdigest(), "row_count": 1,
                "request": {"sql": sql}, "sql_sha256": hashlib.sha256(sql.encode()).hexdigest()})
            daily_hash = MODULE.sha(old / "daily.parquet")
            monthly_hash = MODULE.sha(old / "monthly_reference.parquet")
            out = root / "output"
            with mock.patch.object(MODULE, "DAILY_SHA", daily_hash), mock.patch.object(MODULE, "MONTHLY_SHA", monthly_hash), contextlib.redirect_stdout(io.StringIO()):
                result = MODULE.prepare(root, old, out, "2024-01-01", "2024-01-01")
                with self.assertRaisesRegex(ValueError, "OUTPUT_ALREADY_EXISTS"):
                    MODULE.prepare(root, old, out, "2024-01-01", "2024-01-01")
            combined = pd.read_parquet(out / "daily.parquet")
            self.assertEqual(result["status"], "COMPLETE")
            self.assertEqual(str(combined.y.dtype), "int64")
            self.assertEqual(combined.y.tolist(), [4, 7])
            reused = combined[combined.archive_segment.eq("V1_FINAL_READONLY_REUSED")].reset_index(drop=True)
            pd.testing.assert_frame_equal(reused[source_frame.columns], source_frame.reset_index(drop=True))
            self.assertTrue(pd.isna(reused.promotion_units.iloc[0]))
            self.assertEqual(MODULE.sha(old / "daily.parquet"), daily_hash)
            self.assertEqual(MODULE.sha(out / "monthly_reference.parquet"), monthly_hash)


if __name__ == "__main__":
    unittest.main()
