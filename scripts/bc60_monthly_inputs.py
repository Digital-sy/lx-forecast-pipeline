"""Prepare immutable extended-history inputs from verified local extraction receipts.

No database access, model fitting, source revision, or production imports.
The V1 normalization rules are retained; additional operating columns are only
descriptive evidence, never newly introduced model features.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DAILY_SHA = "ff69063d3c4e506b2b2b8c701db6afa95b4f8af7ce7af3b7c17a898e70bfc7da"
MONTHLY_SHA = "423ce1086fc885d7d41e087e95b048638600710c1d174d3d18a9a3f1a98228af"
INVALID_SPU = {"", "-", "DEFAULT", "无", "None", "null"}
SHOPS = ("JQ-US", "MT-US", "RKZ-US", "SY-US")
KEYS = ["ds", "shop", "spu"]
NONNEGATIVE_COVARIATES = ["ad_spend", "ad_clicks", "ad_orders", "listing_sessions_sum", "order_qty"]
SOURCE_STRINGS = {"ds", "shop", "spu", "min_created_at", "max_sync"}
LINEAGE_FIELDS = ("archive_segment", "source_receipt_id", "archive_acquired_at")
RAW_RE = re.compile(r"daily_missing_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json\.gz$")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str, allow_nan=False), encoding="utf-8")


def integral_or_float(series):
    """Do not let NULL-containing concatenations silently turn all unit facts float."""
    result = pd.to_numeric(series, errors="coerce")
    finite = result.dropna()
    if len(finite) and not np.isfinite(finite.to_numpy(dtype=float)).all():
        raise ValueError("NONFINITE_NUMERIC_SOURCE")
    if finite.empty or finite.eq(np.floor(finite)).all():
        return result.astype("Int64" if result.isna().any() else "int64")
    return result.astype("float64")


def normalize(rows):
    """V1 prepare semantics, plus exact integral y preservation and extra fields."""
    frame = pd.DataFrame(rows).rename(columns={"dt": "ds", "store_name": "shop", "sales_qty": "y"})
    required = {*KEYS, "y", "null_qty", "negative_qty", "date_mismatch", "deleted_rows", "source_rows", *NONNEGATIVE_COVARIATES}
    missing = required - set(frame)
    if missing:
        raise ValueError("MISSING_RAW_COLUMNS:" + ",".join(sorted(missing)))
    frame["ds"] = pd.to_datetime(frame.ds, errors="raise")
    if frame.ds.isna().any() or not frame.ds.eq(frame.ds.dt.normalize()).all():
        raise ValueError("INVALID_BUSINESS_DATE")
    if not frame.shop.isin(SHOPS).all():
        raise ValueError("UNEXPECTED_SHOP")
    numeric = [name for name in frame if name not in SOURCE_STRINGS]
    parse_failures = {}
    for name in numeric:
        original = frame[name]
        frame[name] = pd.to_numeric(original, errors="coerce")
        parse_failures[name] = int((original.notna() & frame[name].isna()).sum())
    frame["y"] = integral_or_float(frame.y)
    negatives = {}
    for name in NONNEGATIVE_COVARIATES:
        negatives[name] = int(frame[name].lt(0).sum())
        frame[name] = frame[name].where(frame[name].ge(0))
    if frame.duplicated(KEYS).any():
        raise ValueError("DUPLICATE_SOURCE_GRAIN")
    frame["mapped"] = frame.spu.notna() & ~frame.spu.isin(INVALID_SPU) & ~frame.spu.astype(str).str.contains("[|,，;；]", regex=True)
    # Match V1 precedence exactly: unknown source quantities precede invalid flags.
    unknown = frame.null_qty.gt(0) | frame.y.isna()
    invalid = frame.negative_qty.gt(0) | frame.date_mismatch.gt(0) | frame.deleted_rows.gt(0)
    zero = frame.y.eq(0).fillna(False)
    frame["sales_state"] = np.select([unknown.to_numpy(dtype=bool), invalid.to_numpy(dtype=bool), zero.to_numpy(dtype=bool)],
                                     ["UNKNOWN", "INVALID", "EXPLICIT_ZERO"], default="OBSERVED")
    frame["category"] = "UNKNOWN"
    frame["source_asof"] = frame.ds.dt.strftime("%Y-%m-%d")
    return frame, {"numeric_parse_failures": parse_failures, "negative_covariates_to_unknown": negatives}


def read_partition(path, evidence):
    match = RAW_RE.fullmatch(path.name)
    if not match:
        raise ValueError("UNEXPECTED_RAW_FILENAME")
    first, last = map(pd.Timestamp, match.groups())
    if first > last:
        raise ValueError("REVERSED_PARTITION")
    receipt_path = evidence / path.name.replace(".json.gz", ".json")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if receipt.get("status") != "OK" or receipt.get("mode") != "select":
        raise ValueError("RECEIPT_NOT_SUCCESSFUL_READ")
    acquired = receipt.get("acquired_at")
    if not acquired or dt.datetime.fromisoformat(acquired.replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("ACQUIRED_AT_MISSING_TIMEZONE")
    payload = gzip.decompress(path.read_bytes())
    if hashlib.sha256(payload).hexdigest() != receipt["rows_sha256"]:
        raise ValueError("RAW_PAYLOAD_HASH_MISMATCH")
    rows = json.loads(payload)
    if not isinstance(rows, list) or len(rows) != receipt["row_count"]:
        raise ValueError("RAW_ROW_COUNT_MISMATCH")
    sql = receipt["request"]["sql"]
    if hashlib.sha256(sql.encode()).hexdigest() != receipt["sql_sha256"]:
        raise ValueError("RECEIPT_SQL_HASH_MISMATCH")
    # This collector uses exact IN dates. Verify the receipt, not only the filename.
    date_clause = re.search(r"\bdt\s+IN\s*\(([^)]*)\)", sql, flags=re.I)
    if date_clause is None:
        raise ValueError("NO_BOUNDED_DATE_RECEIPT")
    claimed = set(re.findall(r"'(\d{4}-\d{2}-\d{2})'", date_clause.group(1)))
    expected = {str(day.date()) for day in pd.date_range(first, last)}
    if claimed != expected:
        raise ValueError("RECEIPT_DATE_RANGE_MISMATCH")
    if any(str(row.get("dt"))[:10] not in expected for row in rows):
        raise ValueError("SOURCE_ROW_OUTSIDE_PARTITION")
    return rows, receipt, receipt_path, first, last


def verify_partition_dates(intervals, expected_first, expected_last):
    observed = []
    for first, last in intervals:
        observed.extend(pd.date_range(first, last))
    if len(observed) != len(set(observed)):
        raise ValueError("OVERLAPPING_RAW_PARTITIONS")
    expected = set(pd.date_range(expected_first, expected_last))
    if set(observed) != expected:
        missing = sorted(expected - set(observed))
        extra = sorted(set(observed) - expected)
        raise ValueError(f"RAW_PARTITION_COVERAGE_MISMATCH missing={len(missing)} extra={len(extra)}")


def align(frame, schema):
    """Nullable extra evidence remains unknown in the unchanged V1 segment."""
    out = frame.copy()
    for field in schema:
        if field.name not in out:
            out[field.name] = None
        if pa.types.is_integer(field.type):
            values = pd.to_numeric(out[field.name], errors="coerce")
            known = values.dropna()
            if not known.eq(np.floor(known)).all():
                raise ValueError("NONINTEGRAL_IN_INTEGER_FIELD:" + field.name)
            out[field.name] = values.astype("Int64" if values.isna().any() else "int64")
        elif pa.types.is_floating(field.type):
            out[field.name] = pd.to_numeric(out[field.name], errors="coerce").astype("float64")
    return pa.Table.from_pandas(out[schema.names], schema=schema, preserve_index=False, safe=True)


def partition_summary(frame):
    z = frame.copy()
    z["known_units"] = z.y.where(z.sales_state.isin(["OBSERVED", "EXPLICIT_ZERO"]) & z.mapped)
    z["unknown_rows"] = z.sales_state.eq("UNKNOWN").astype(int)
    z["invalid_rows"] = z.sales_state.eq("INVALID").astype(int)
    z["unmapped_rows"] = (~z.mapped).astype(int)
    return z.groupby(["shop", "ds", "archive_segment"], dropna=False).agg(
        aggregate_objects=("spu", "size"), source_rows=("source_rows", "sum"),
        raw_aggregate_units=("y", lambda values: values.sum(min_count=1)),
        known_mapped_units=("known_units", lambda values: values.sum(min_count=1)),
        unknown_rows=("unknown_rows", "sum"), invalid_rows=("invalid_rows", "sum"),
        unmapped_rows=("unmapped_rows", "sum")).reset_index()


def prepare(root, v1_inputs, out, expected_first="2024-01-01", expected_last="2025-12-31"):
    root, v1_inputs, out = [Path(p).resolve() for p in (root, v1_inputs, out)]
    if out.exists():
        raise ValueError("OUTPUT_ALREADY_EXISTS")
    source_daily, monthly = v1_inputs / "daily.parquet", v1_inputs / "monthly_reference.parquet"
    if sha(source_daily) != DAILY_SHA or sha(monthly) != MONTHLY_SHA:
        raise ValueError("V1_FROZEN_INPUT_HASH_MISMATCH")
    old_manifest_path, old_quality_path = v1_inputs / "input_manifest.json", v1_inputs / "quality.json"
    old_manifest = json.loads(old_manifest_path.read_text(encoding="utf-8"))
    for name, digest in old_manifest["files"].items():
        if sha(v1_inputs / name) != digest:
            raise ValueError("V1_INPUT_MANIFEST_MISMATCH:" + name)
    raw_paths = sorted((root / "raw").glob("daily_missing_*.json.gz"))
    if not raw_paths:
        raise ValueError("NO_VERIFIED_NEW_RAW")
    old_schema = pq.ParquetFile(source_daily).schema_arrow.remove_metadata()
    columns, receipts, intervals = set(), [], []
    any_fractional, any_null = False, False
    clock = time.monotonic()
    for path in raw_paths:
        rows, receipt, receipt_path, first, last = read_partition(path, root / "evidence")
        intervals.append((first, last))
        if rows:
            names = set(rows[0])
            if any(set(row) != names for row in rows):
                raise ValueError("WITHIN_PARTITION_SCHEMA_DRIFT")
            if columns and names != columns:
                raise ValueError("BETWEEN_PARTITION_SCHEMA_DRIFT")
            columns = names
        values = pd.to_numeric(pd.Series([row.get("sales_qty") for row in rows]), errors="coerce")
        any_null |= bool(values.isna().any())
        known = values.dropna()
        if not np.isfinite(known.to_numpy(dtype=float)).all():
            raise ValueError("NONFINITE_RAW_QUANTITY")
        any_fractional |= bool((known != np.floor(known)).any())
        receipts.append({"file": str(path), "file_sha256": sha(path), "receipt": str(receipt_path),
                         "receipt_sha256": sha(receipt_path), "payload_sha256": receipt["rows_sha256"],
                         "sql_sha256": receipt["sql_sha256"], "acquired_at": receipt["acquired_at"],
                         "received_at": receipt.get("received_at"), "row_count": len(rows),
                         "source_commit": receipt.get("source_commit"),
                         "estimated_scan": receipt.get("estimated_scan"),
                         "query_elapsed_seconds": receipt.get("elapsed_seconds"),
                         "first_business_day": str(first.date()), "last_business_day": str(last.date()),
                         "source_row_created_min": min((row["min_created_at"] for row in rows if row.get("min_created_at")), default=None),
                         "source_row_modified_max": max((row["max_sync"] for row in rows if row.get("max_sync")), default=None),
                         "source_row_modified_max_field": "max_sync = MAX(modify_time), not synchronization job success",
                         "source_naive_timestamp_timezone": "UNKNOWN; not substituted for receipt acquisition timezone",
                         "source_sync_at": None})
    verify_partition_dates(intervals, expected_first, expected_last)
    old_min = pq.read_table(source_daily, columns=["ds"]).column("ds").to_pandas().min()
    if pd.Timestamp(expected_last) >= old_min:
        raise ValueError("NEW_HISTORY_OVERLAPS_FROZEN_V1")
    renamed = {"dt": "ds", "store_name": "shop", "sales_qty": "y"}
    normalized_columns = {renamed.get(c, c) for c in columns} | {"mapped", "sales_state", "category", "source_asof"}
    additional = sorted(normalized_columns - set(old_schema.names))
    fields = [pa.field(f.name, pa.float64() if f.name == "y" and any_fractional else f.type, nullable=True) for f in old_schema]
    fields += [pa.field(c, pa.float64(), nullable=True) for c in additional]
    fields += [pa.field(c, pa.string(), nullable=True) for c in LINEAGE_FIELDS]
    schema = pa.schema(fields)
    out.mkdir(parents=True, exist_ok=False)
    manifest = {"status": "RUNNING", "started_at": now(), "classification": "RECONSTRUCTED_HISTORICAL_REPLAY",
                "model_comparison_classification": "EXTENDED_HISTORY_RETRAIN_REPLAY",
                "source_script_sha256": sha(__file__), "new_queries": 0, "model_fits": 0,
                "v1_daily_sha256": DAILY_SHA, "v1_monthly_reference_sha256": MONTHLY_SHA,
                "v1_input_manifest_sha256": sha(old_manifest_path), "v1_quality_sha256": sha(old_quality_path),
                "new_raw_partitions": len(receipts), "sources": receipts,
                "additional_operating_fields_not_model_features": additional,
                "source_sync_at": None,
                "pit_limitation": "Acquired in 2026; row create/modify times and backfilled business dates do not prove historical availability.",
                "max_sync_semantics": "Retained V1 alias of MAX(modify_time); not a synchronization task success timestamp.",
                "original_v1_acquired_at": "See immutable V1 quality.json raw_receipts; not replaced with archive creation time.",
                "y_arrow_type": str(schema.field("y").type), "new_raw_has_null_qty_values": any_null,
                "new_raw_has_nonintegral_qty_values": any_fractional}
    dump(out / "input_manifest.json", manifest)
    summaries, diagnostic_counts, state_counts = [], {}, {}
    writer = pq.ParquetWriter(out / "daily.parquet", schema, compression="zstd")
    total_rows = 0
    try:
        for path, source in zip(raw_paths, receipts):
            rows, receipt, receipt_path, _, _ = read_partition(path, root / "evidence")
            if sha(path) != source["file_sha256"]:
                raise ValueError("RAW_CHANGED_AFTER_PREFLIGHT")
            if sha(receipt_path) != source["receipt_sha256"]:
                raise ValueError("RECEIPT_CHANGED_AFTER_PREFLIGHT")
            if not rows:
                continue
            frame, counts = normalize(rows)
            frame["archive_segment"] = "NEW_HISTORICAL_EXTRACTION"
            frame["source_receipt_id"] = path.name.replace(".json.gz", "")
            frame["archive_acquired_at"] = receipt["acquired_at"]
            writer.write_table(align(frame, schema))
            total_rows += len(frame)
            summaries.append(partition_summary(frame))
            for state, n in frame.sales_state.value_counts().items():
                state_counts[state] = state_counts.get(state, 0) + int(n)
            for kind, values in counts.items():
                target = diagnostic_counts.setdefault(kind, {})
                for field, n in values.items():
                    target[field] = target.get(field, 0) + n
        for batch in pq.ParquetFile(source_daily).iter_batches(batch_size=100000):
            frame = batch.to_pandas()
            frame["archive_segment"] = "V1_FINAL_READONLY_REUSED"
            frame["source_receipt_id"] = "V1_INPUTS_FINAL_RECEIPTS"
            frame["archive_acquired_at"] = None
            writer.write_table(align(frame, schema))
            total_rows += len(frame)
            summaries.append(partition_summary(frame))
            for state, n in frame.sales_state.value_counts().items():
                state_counts[state] = state_counts.get(state, 0) + int(n)
    except Exception as error:
        manifest.update(status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=now())
        dump(out / "input_manifest.json", manifest)
        raise
    finally:
        writer.close()
    # Existing rows may split across record batches; consolidate partition metadata.
    coverage = pd.concat(summaries, ignore_index=True)
    values = [c for c in coverage if c not in ["shop", "ds", "archive_segment"]]
    coverage = coverage.groupby(["shop", "ds", "archive_segment"], as_index=False)[values].sum(min_count=1)
    coverage.to_csv(out / "partition_coverage.csv", index=False, encoding="utf-8-sig")
    full_last = coverage.ds.max()
    missing_dates = []
    for shop in SHOPS:
        present = coverage.loc[coverage.shop.eq(shop), "ds"]
        missing_dates.extend(f"{shop}/{d.date()}" for d in pd.date_range(expected_first, full_last).difference(present))
    shutil.copyfile(monthly, out / "monthly_reference.parquet")
    if sha(source_daily) != DAILY_SHA or sha(monthly) != MONTHLY_SHA:
        raise ValueError("ORIGINAL_V1_CHANGED")
    if sha(out / "monthly_reference.parquet") != MONTHLY_SHA:
        raise ValueError("MONTHLY_COPY_HASH_MISMATCH")
    quality = {"gate": "PARTIAL_MISSING_SHOP_DATES" if missing_dates else "PASS_FOR_RECONSTRUCTED_OBSERVED_FACTS",
               "daily_aggregate_rows": total_rows, "first_day": str(coverage.ds.min().date()), "last_day": str(full_last.date()),
               "source_state_counts": state_counts, "missing_shop_dates": missing_dates,
               "source_rows": int(coverage.source_rows.sum()),
               "raw_aggregate_units_not_all_verified": float(coverage.raw_aggregate_units.sum(min_count=1)),
               "known_mapped_units": float(coverage.known_mapped_units.sum(min_count=1)),
               "numeric_normalization": diagnostic_counts, "additional_fields": additional,
               "category": "UNKNOWN unless separate current descriptive metadata is joined later; never historical input",
               "old_and_new_overlap": "DISJOINT; separately acquired revision audit is never merged",
               "source_sync_success": "UNKNOWN", "snapshot_acquisition_is_not_historical_availability": True,
               "monthly_reference": "Exact original V1 bytes reused; prior missing-SKU limitation retained"}
    dump(out / "quality.json", quality)
    manifest.update(status="COMPLETE", completed_at=now(), elapsed_seconds=time.monotonic() - clock,
                    originals_unchanged=True, daily_rows=total_rows,
                    files={p.name: sha(p) for p in sorted(out.iterdir()) if p.is_file() and p.name != "input_manifest.json"})
    dump(out / "input_manifest.json", manifest)
    print(json.dumps({k: v for k, v in manifest.items() if k != "sources"}, ensure_ascii=False, indent=2))
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--v1-inputs", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--expected-first", default="2024-01-01")
    parser.add_argument("--expected-last", default="2025-12-31")
    prepare(**vars(parser.parse_args()))
