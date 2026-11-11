"""Build an immutable, local-only JSONL load package. Never connects to a database."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re

VERSION = "1.0.1"
DDL_SHA256 = "20cfeb64ac250bfd9dfc4fc740898bdf3e63a262fbb1d3f039f1e0f84361a4c1"
TABLES = (
    "ods_it_garment_order_ingestion_run",
    "dim_sm_garment_order_document",
    "ods_sm_garment_order_document_version",
    "ods_sm_garment_order_document_observation",
    "ods_it_garment_order_extraction_run",
    "dwd_sm_garment_order_color_evidence",
    "dwd_sm_garment_material_color_evidence",
    "ods_manual_sm_garment_color_confirmation",
    "dwd_sm_garment_confirmation_evidence_ref",
)
LIMITATIONS = [
    "Increment-only source evidence; baseline, enriched material evidence and historical manual decisions are not imported.",
    "Archive URIs are existing private local paths. Database users cannot assume server accessibility or a permanent archive service.",
    "First-ever observation/acquisition and per-file parsing timestamps are unknown; current acquisition and batch bounds are recorded separately.",
    "Directory metadata and downloaded content were not acquired atomically; this package is not current remote validation.",
    "Parsed candidates are not approved color mappings, effective purchase orders or future material recipes.",
    "No exact shop is inferred from folder market; no color system is inferred from color abbreviations.",
    "Images were not semantically interpreted and spreadsheet formulas were not recalculated.",
    "The package contains no database connection, DDL, DML, scheduler or remote operation.",
]
class ContractError(ValueError):
    pass

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)

def sha_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def stable_id(kind, *parts):
    return hashlib.sha256(canonical(["garment_source_load/1", kind, *parts]).encode("utf-8")).hexdigest()

def require(test, message):
    if not test:
        raise ContractError(message)

def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))

def load_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8-sig").splitlines() if line.strip()]

def utc_value(value):
    if value is None or value == "":
        return None
    value = str(value)
    if re.fullmatch(r"\d{10}", value):
        dt = datetime.fromtimestamp(int(value), timezone.utc)
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(dt.tzinfo is not None, "Timestamp without timezone is not accepted")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

def date_value(value):
    if value is None or value == "":
        return None
    require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value), "Date is not a parsed ISO date")
    return date.fromisoformat(value).isoformat()

def quantity_value(value):
    if value is None:
        return None, "UNKNOWN"
    require(not isinstance(value, bool), "Boolean is not quantity")
    try:
        quantity = Decimal(str(value))
    except InvalidOperation as exc:
        raise ContractError("Invalid numeric quantity") from exc
    require(quantity.is_finite(), "Nonfinite quantity")
    require(quantity.as_tuple().exponent >= -8, "Quantity exceeds DECIMAL scale")
    require(abs(quantity) < Decimal("1e16"), "Quantity exceeds DECIMAL precision")
    state = "KNOWN_ZERO" if quantity == 0 else ("NEGATIVE_SOURCE_VALUE" if quantity < 0 else "KNOWN_VALUE")
    return format(quantity, "f"), state

def parse_schema(text):
    result = {}
    for match in re.finditer(r"CREATE TABLE \x60lingxing\x60\.\x60([a-z_]+)\x60 \((.*?)\n\) ENGINE=", text, re.S):
        name, body = match.groups()
        fields = {}
        for line in body.splitlines():
            column = re.match(r"\s{4}`?([a-z_][a-z0-9_]*)`? (CHAR|VARCHAR|TEXT|JSON|DATETIME|DATE|BIGINT|INT|TINYINT|DECIMAL)(\([0-9,]+\))?(.*)", line)
            if not column:
                continue
            key, base, size, rest = column.groups()
            fields[key] = {
                "type": base + (size or ""), "base_type": base,
                "nullable": "NOT NULL" not in rest,
                "has_default": "DEFAULT" in rest,
                "unsigned": "UNSIGNED" in rest,
            }
        pk = re.search(r"PRIMARY KEY \((.*?)\)", body)
        result[name] = {"columns": fields, "primary_key": [s.strip() for s in pk.group(1).split(",")]}
    require(tuple(result) == TABLES, "DDL table list differs from reviewed contract")
    return result

def validate_rows(package, schema):
    require(tuple(package) == TABLES, "Package contains unexpected table scope")
    keysets = {}
    for table, rows in package.items():
        fields = schema[table]["columns"]
        seen = set()
        for row in rows:
            require(set(row) <= set(fields), f"Unknown columns in {table}: {set(row)-set(fields)}")
            for key, field in fields.items():
                if not field["nullable"] and not field["has_default"]:
                    require(key in row and row[key] is not None, f"Missing required {table}.{key}")
                if key not in row or row[key] is None:
                    require(key not in row or field["nullable"], f"NULL in non-null {table}.{key}")
                    continue
                val = row[key]
                base = field["base_type"]
                if base in ("CHAR", "VARCHAR"):
                    maximum = int(re.search(r"\((\d+)\)", field["type"]).group(1))
                    require(isinstance(val, str) and len(val) <= maximum, f"Invalid string length {table}.{key}")
                elif base == "JSON":
                    canonical(val)
                elif base in ("INT", "TINYINT", "BIGINT"):
                    require(isinstance(val, int) and not isinstance(val, bool), f"Non-integer {table}.{key}")
                    require(not field["unsigned"] or val >= 0, f"Negative unsigned {table}.{key}")
                elif base == "DECIMAL":
                    quantity_value(val)
                elif base == "DATE":
                    date_value(val)
                elif base == "DATETIME":
                    datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
                if key.endswith("sha256") or key in ("run_id", "document_id", "version_id", "extraction_id", "observation_id", "evidence_id"):
                    require(re.fullmatch(r"[0-9a-f]{64}", str(val)) is not None, f"Invalid SHA/ID {table}.{key}")
            key = tuple(row[k] for k in schema[table]["primary_key"])
            require(key not in seen, f"Duplicate primary key {table}")
            seen.add(key)
        keysets[table] = seen
    runs = {x["run_id"] for x in package[TABLES[0]]}
    documents = {x["document_id"] for x in package[TABLES[1]]}
    versions = {(x["version_id"], x["document_id"]) for x in package[TABLES[2]]}
    version_ids = {x[0] for x in versions}
    extracts = {(x["extraction_id"], x["version_id"]) for x in package[TABLES[4]]}
    unique = set()
    for row in package[TABLES[2]]:
        require(row["document_id"] in documents, "Orphan version")
        key = (row["document_id"], row["content_sha256"])
        require(key not in unique, "Duplicate document content version")
        unique.add(key)
    unique = set()
    for row in package[TABLES[3]]:
        require(row["run_id"] in runs and (row["version_id"], row["document_id"]) in versions, "Orphan observation")
        key = (row["run_id"], row["document_id"])
        require(key not in unique, "Duplicate observation")
        unique.add(key)
    for row in package[TABLES[4]]:
        require(row["run_id"] in runs and row["version_id"] in version_ids, "Orphan extraction")
    unique = set()
    for row in package[TABLES[5]]:
        require((row["extraction_id"], row["version_id"]) in extracts, "Orphan color row")
        key = (row["extraction_id"], row["source_line_id"])
        require(key not in unique, "Duplicate extraction source line")
        unique.add(key)
    require(all(not package[t] for t in TABLES[6:]), "Material/manual expansion is outside this package scope")

def convert_data(documents, order_rows, downloads, plan, acquisition, parser, index, hashes, paths):
    """Pure transformation; callers must verify frozen inputs before invoking."""
    require(acquisition["status"] == "COMPLETE" and acquisition["failed"] == 0 and acquisition["not_run"] == 0, "Incomplete acquisition")
    require(parser["status"] == "PARSED" and parser["returncode"] == 0, "Incomplete extraction")
    scope = [x["token"] for x in plan["scope"]]
    require(len(scope) == len(set(scope)), "Duplicate scope identity")
    by_source = {d["source_id"]: d for d in documents}
    by_download = {d["token"]: d for d in downloads}
    require(len(by_source) == len(documents) and len(by_download) == len(downloads), "Duplicate source in archive")
    require(set(scope) == set(by_source) == set(by_download), "Scope mismatch")
    require(len(documents) == index["summary"]["documents"] == acquisition["source_count"] == acquisition["downloaded"], "Document count mismatch")
    require(len(order_rows) == index["summary"]["order_rows"] == parser["line_count"], "Row count mismatch")
    require(index["summary"]["enriched_rows"] == index["summary"]["confirmations"] == 0, "Increment unexpectedly includes semantic/manual data")
    config_hash = stable_id("acquisition_config", plan)
    run_id = stable_id("source_acquisition_run", hashes["download_manifest"], config_hash)
    parse_config = {
        "parser_sha256": parser["parser_sha256"], "helper_sha256": parser["helper_sha256"],
        "inventory_sha256": parser["inventory_sha256"], "downloads_sha256": parser["downloads_sha256"],
        "plan_sha256": parser["plan_sha256"], "command": parser["command"],
    }
    parse_config_hash = stable_id("extraction_config", parse_config)
    result = {t: [] for t in TABLES}
    result[TABLES[0]].append({
        "run_id": run_id, "run_kind": "SOURCE_ACQUISITION_ARCHIVE", "run_status": "ARCHIVED_COMPLETE",
        "started_at_utc": utc_value(plan["created_at"]), "finished_at_utc": utc_value(acquisition["finished_at"]),
        "scope_json": {"source_ids": sorted(scope), "scope_kind": "INCREMENT_ONLY", "source_system": "feishu_drive"},
        "config_sha256": config_hash, "manifest_uri": paths["download_manifest"],
        "manifest_sha256": hashes["download_manifest"], "source_business_asof_raw": None, "source_sync_at_utc": None,
        "completeness_status": "ARCHIVED_FETCH_PARSE_COMPLETE",
        "counts_json": {"documents": len(documents), "raw_color_rows": len(order_rows), "material_rows": 0, "manual_rows": 0},
        "limitations_json": LIMITATIONS + [acquisition["snapshot_limitations"]],
        "receipt_json": {"source_acquisition": acquisition, "fetch_plan": plan, "parser_manifest_sha256": hashes["parse_manifest"],
                         "index_manifest_sha256": hashes["index_manifest"], "database_import_status": "NOT_A_DATABASE_EXECUTION_RECEIPT"},
        "delete_flag": 0,
    })
    extracts = {}
    for source in sorted(scope):
        doc, dl = by_source[source], by_download[source]
        require(dl["status"] == "ok" and doc["file_verification"] == "HASH_MATCH" and doc["parse_status"] == "ok", "Unverified source")
        require(doc["content_sha256"] == dl["sha256"] == doc["file_audit"]["source_sha256"], "Source content mismatch")
        require(doc["inventory"]["token"] == source, "Metadata identity mismatch")
        document_id = stable_id("document", "feishu_drive", "file", source)
        version_id = doc["version_id"]
        extraction_id = stable_id("extraction", hashes["parse_manifest"], version_id, parse_config_hash, 1)
        extracts[source] = (extraction_id, version_id, doc["content_sha256"])
        metadata = doc["inventory"]
        result[TABLES[1]].append({
            "document_id": document_id, "source_system": "feishu_drive", "resource_type": "file", "source_id": source,
            "first_observed_at_utc": None, "identity_basis": "FILE_RESOURCE_NOT_PURCHASE_ORDER", "delete_flag": 0,
        })
        result[TABLES[2]].append({
            "version_id": version_id, "document_id": document_id, "content_sha256": dl["sha256"],
            "byte_count": dl["bytes"], "archive_uri": dl["local_path"], "file_format": dl["format"],
            "first_acquired_at_utc": None, "acquisition_time_status": "FIRST_EVER_UNKNOWN",
            "verification_status": "LOCAL_BYTES_SHA256_MATCH",
            "origin_receipt_json": {"current_acquisition": dl, "index_version_id": version_id,
                "same_content_source_ids": doc["same_content_source_ids"],
                "first_acquired_at_note": "Historical first acquisition is not inferred from this new observation.",
                "archive_storage_status": "LOCAL_PRIVATE_PATH_NOT_SERVER_STORAGE"},
            "delete_flag": 0,
        })
        result[TABLES[3]].append({
            "observation_id": stable_id("observation", run_id, document_id),
            "run_id": run_id, "document_id": document_id, "version_id": version_id,
            "observed_at_utc": utc_value(dl["inspect"]["checked_at"]), "acquired_at_utc": utc_value(dl["acquired_at"]),
            "file_name": doc["file_name"], "source_url": doc["source_url"], "source_path": metadata.get("path"),
            "parent_source_id": metadata.get("parent_token"), "source_modified_at_utc": utc_value(metadata.get("modified_time")),
            "source_modified_at_raw": metadata.get("modified_time"), "source_created_at_raw": metadata.get("created_time"),
            "metadata_sha256": stable_id("metadata", metadata), "presence_status": "OBSERVED_AT_ACQUISITION",
            "download_status": "ARCHIVED_OK", "content_status": "LOCAL_HASH_MATCH_REMOTE_NOT_CURRENT",
            "metadata_json": metadata,
            "receipt_json": {"download": dl, "metadata_status": doc["metadata_status"],
                "inventory_as_of": doc["inventory_as_of"], "metadata_changes": doc["metadata_changes"],
                "observation_time_basis": "inspect.checked_at; directory metadata may be earlier",
                "limitation": acquisition["snapshot_limitations"]},
            "delete_flag": 0,
        })
        audit = doc["file_audit"]
        result[TABLES[4]].append({
            "extraction_id": extraction_id, "run_id": run_id, "version_id": version_id,
            "extraction_kind": "RAW_ORDER_COLOR", "parser_version": audit["extraction_version"],
            "parser_sha256": parser["parser_sha256"], "config_sha256": parse_config_hash, "attempt_no": 1,
            "started_at_utc": None, "finished_at_utc": None, "extraction_status": "ARCHIVED_PARSE_OK",
            "time_evidence_status": "BATCH_BOUNDS_ONLY", "artifact_uri": paths["order_lines"],
            "artifact_sha256": parser["output_sha256"]["order_lines.jsonl"],
            "legacy_import_json": {"index_version_id": version_id, "index_manifest_sha256": hashes["index_manifest"]},
            "sheet_image_coverage_json": {"sheet_audits": audit.get("sheet_audits", []), "image_interpretation": "NOT_PERFORMED",
                "formula_recalculation": "NOT_PERFORMED"},
            "audit_json": {"file_audit": audit, "configuration": parse_config,
                "batch_started_at": parser["started_at"], "batch_finished_at": parser["finished_at"],
                "batch_manifest_sha256": hashes["parse_manifest"]},
            "delete_flag": 0,
        })
    for item in sorted(order_rows, key=lambda x: (x["source_id"], x["raw"]["line_id"])):
        raw = item["raw"]
        require(item["source_id"] in extracts, "Out of scope evidence")
        extraction_id, version_id, content_hash = extracts[item["source_id"]]
        require(item["version_id"] == version_id and item["content_sha256"] == content_hash == raw["source_sha256"], "Evidence version mismatch")
        require(raw["token"] == item["source_id"], "Evidence source mismatch")
        quantity, quantity_state = quantity_value(raw.get("quantity"))
        reliable = raw.get("quantity_reliable")
        require(reliable is None or type(reliable) is bool, "Unknown reliability encoding")
        result[TABLES[5]].append({
            "evidence_id": stable_id("raw_color_evidence", extraction_id, raw["line_id"]),
            "legacy_evidence_id": item["evidence_id"], "extraction_id": extraction_id, "version_id": version_id,
            "source_line_id": raw["line_id"], "line_type": raw["line_type"], "sheet_name": raw.get("sheet"),
            "sheet_index": raw.get("sheet_index"), "table_header_row": raw.get("table_header_row"), "row_number": raw.get("row"),
            "spu": raw.get("spu"), "sku_raw": raw.get("sku_raw"), "order_id": raw.get("order_id"),
            "order_date": date_value(raw.get("order_date")), "order_date_source": raw.get("order_date_source"),
            "filename_date": date_value(raw.get("filename_date")), "market_raw": raw.get("market"), "factory_raw": raw.get("factory"),
            "shop_evidence_json": item.get("shop_evidence"), "garment_color_system": None,
            "garment_color_code": raw.get("color_code"), "garment_color_name": raw.get("color_name"), "garment_color_raw": raw.get("garment_color_raw"),
            "quantity": quantity, "quantity_state": quantity_state, "quantity_source": raw.get("quantity_source"),
            "quantity_reliable": None if reliable is None else int(reliable),
            "supplier_raw": raw.get("supplier"), "fabric_header_raw": raw.get("fabric_header"), "fabric_row_note_raw": raw.get("fabric_row_note"),
            "field_sources_json": {k: raw.get(k) for k in (
                "color_source_cell", "color_source_role", "color_alternatives", "fabric_assignment_sources", "filename_date_raw",
                "filename_date_status", "order_date_source", "body_order_dates", "order_date_conflict", "spu_candidates", "order_id_candidates")},
            "source_cells_json": raw.get("evidence_cells", []),
            "duplicate_refs_json": {"duplicate_line_of": raw.get("duplicate_line_of"), "duplicate_source_of": raw.get("duplicate_source_of")},
            "issues_json": {"file_issues": raw.get("file_issues", []), "row_issues": raw.get("issues", []),
                "include_in_original_summary": raw.get("include_in_summary"), "quantity_state": quantity_state},
            "status_snapshot_sha256": item.get("status_source_sha256"), "status_acquired_at_utc": utc_value(item.get("status_source_acquired_at")),
            "order_status_evidence_json": item.get("order_status_evidence"), "future_applicability": item["future_applicability"],
            "raw_json": raw, "delete_flag": 0,
        })
    return result

def build_package(source_root, ddl_path, output):
    source_root, ddl_path, output = map(Path, (source_root, ddl_path, output))
    require(not output.exists(), "Output already exists; refusing overwrite")
    tracked = {}
    def verify(path, expected=None):
        path = Path(path)
        observed = sha_file(path)
        require(expected is None or observed == expected, f"Hash mismatch: {path.name}")
        tracked[str(path)] = observed
        return observed
    verify(ddl_path, DDL_SHA256)
    schema = parse_schema(ddl_path.read_text(encoding="utf-8-sig"))
    private = source_root / "private"
    paths = {
        "registry": private / "source_registry_manifest.json", "index_manifest": private / "increment_index/manifest.json",
        "download_manifest": private / "download_manifest.json", "fetch_plan": private / "fetch_plan.json",
        "parse_manifest": private / "parsed_increment_v1/parse_manifest.json", "downloads": private / "downloads.jsonl",
        "order_lines": private / "parsed_increment_v1/order_lines.jsonl",
    }
    registry = load_json(paths["registry"])
    verify(paths["registry"])
    verify(paths["index_manifest"], registry["parts"]["increment_index_manifest"]["sha256"])
    verify(paths["download_manifest"], registry["parts"]["acquisition_manifest"]["sha256"])
    index, acquisition = load_json(paths["index_manifest"]), load_json(paths["download_manifest"])
    plan, parser = load_json(paths["fetch_plan"]), load_json(paths["parse_manifest"])
    verify(paths["fetch_plan"])
    # parse_increment.py pins BASE_PLAN to the original refresh actions, not fetch_plan.json.
    verify(source_root.parent / "garment_order_source_v1_20261109/outputs/refresh_plan/actions.jsonl", parser["plan_sha256"])
    verify(private / "fetch_allowlist.json", acquisition["allowlist_sha256"])
    verify(private / "inventory_increment.json", acquisition["inventory_sha256"])
    verify(paths["parse_manifest"])
    verify(Path(index["config"]["path"]), index["config"]["sha256"])
    for spec in index["sources"].values():
        verify(spec["path"], spec["sha256"])
    for name, spec in index["outputs"].items():
        verify(private / "increment_index" / name, spec["sha256"])
    verify(paths["downloads"], acquisition["downloads_sha256"])
    verify(paths["order_lines"], parser["output_sha256"]["order_lines.jsonl"])
    verify(parser["parser_path"], parser["parser_sha256"])
    verify(parser["helper_path"], parser["helper_sha256"])
    downloads = load_jsonl(paths["downloads"])
    for dl in downloads:
        verify(dl["local_path"], dl["sha256"])
        require(Path(dl["local_path"]).stat().st_size == dl["bytes"], "Raw file byte count mismatch")
        require(parser["source_manifest"][dl["token"]]["sha256"] == dl["sha256"], "Parser source mismatch")
    documents = load_jsonl(private / "increment_index/documents.jsonl")
    rows = load_jsonl(private / "increment_index/order_rows.jsonl")
    hashes = {key: tracked[str(path)] for key, path in paths.items() if str(path) in tracked}
    converted = convert_data(documents, rows, downloads, plan, acquisition, parser, index, hashes, {k: str(v) for k,v in paths.items()})
    validate_rows(converted, schema)
    for path, before in tracked.items():
        require(sha_file(path) == before, "Source changed during package construction")
    output.mkdir(parents=True, exist_ok=False)
    table_files = {}
    for table, data in converted.items():
        path = output / (table + ".jsonl")
        path.write_text("".join(canonical(row) + "\n" for row in data), encoding="utf-8", newline="\n")
        table_files[table] = {
            "file": path.name, "row_count": len(data), "sha256": sha_file(path),
            "primary_key": schema[table]["primary_key"],
            "json_columns": [key for key, spec in schema[table]["columns"].items() if spec["base_type"] == "JSON"],
            "columns": schema[table]["columns"],
        }
    manifest = {
        "schema_version": "garment_source_load_package/1", "builder_version": VERSION,
        "built_at_utc": datetime.now(timezone.utc).isoformat(), "status": "LOCAL_PACKAGE_VALIDATED_NOT_IMPORTED",
        "target_schema": "lingxing", "ddl_sha256": DDL_SHA256, "builder_sha256": sha_file(__file__),
        "scope": "INCREMENT_RAW_EVIDENCE_ONLY", "table_order": list(TABLES), "tables": table_files,
        "total_rows": sum(len(rows) for rows in converted.values()), "source_files": tracked,
        "quantity_reconciliation": {"source_raw_quantity_sum": str(sum((Decimal(str(x["raw"]["quantity"])) for x in rows if x["raw"].get("quantity") is not None), Decimal(0))),
            "package_raw_quantity_sum": str(sum((Decimal(x["quantity"]) for x in converted[TABLES[5]] if x["quantity"] is not None), Decimal(0))),
            "meaning": "Raw parsed evidence quantity; not approved orders or deduplicated business demand"},
        "load_contract": {"mode": "INSERT_ONLY_COMPARE_EXISTING", "on_existing_identical": "SKIP",
            "on_existing_different": "FAIL_NO_UPDATE", "database_default_columns": ["create_time", "update_time"],
            "json_values": "JSON encode listed JSON columns once; preserve JSON null vs SQL NULL",
            "decimal_values": "Decimal strings bind as DECIMAL without binary-float conversion",
            "utc_connection_required": True, "foreign_keys_required": True, "no_disable_checks": True},
        "limitations": LIMITATIONS, "database_operations": 0, "remote_operations": 0,
    }
    require(manifest["quantity_reconciliation"]["source_raw_quantity_sum"] == manifest["quantity_reconciliation"]["package_raw_quantity_sum"], "Quantity reconciliation failed")
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--ddl", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    result = build_package(args.source_root, args.ddl, args.output)
    print(json.dumps({"status": result["status"], "counts": {k:v["row_count"] for k,v in result["tables"].items()},
        "total_rows": result["total_rows"], "output": args.output, "database_operations": 0}, ensure_ascii=False))
if __name__ == "__main__":
    main()
