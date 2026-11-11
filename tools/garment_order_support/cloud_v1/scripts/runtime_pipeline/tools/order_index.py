#!/usr/bin/env python3
"""Read-only, versioned order-document index. No database or network clients.

Build uses explicit local archive paths. Query performs retrieval only, not
material/color identity resolution or effective-order selection.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

VERSION = "1.0.1"
SCHEMA_VERSION = "garment_order_source_index/1"
REQUIRED = ("inventory", "downloads", "file_audit", "order_lines")
OPTIONAL = ("enriched_evidence", "manual_evidence", "status_index")

class IndexErrorDetail(ValueError):
    pass

def sha256_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)

def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise IndexErrorDetail(f"Duplicate JSON key: {key}")
        result[key] = value
    return result

def decode(value):
    return json.loads(value, object_pairs_hook=unique_object,
                      parse_constant=lambda x: (_ for _ in ()).throw(IndexErrorDetail(f"Invalid JSON number: {x}")))

def read_json(path):
    return decode(Path(path).read_text(encoding="utf-8-sig"))

def read_jsonl(path):
    result = []
    for n, text in enumerate(Path(path).read_text(encoding="utf-8-sig").splitlines(), 1):
        if text.strip():
            row = decode(text)
            if not isinstance(row, dict):
                raise IndexErrorDetail(f"Expected object: {path}:{n}")
            result.append(row)
    return result

def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")

def write_jsonl(path, values):
    with Path(path).open("x", encoding="utf-8", newline="\n") as stream:
        for value in values:
            stream.write(canonical(value) + "\n")

def nonempty(value, label):
    if not isinstance(value, str) or not value.strip():
        raise IndexErrorDetail(f"Missing {label}")
    return value

def unique_rows(rows, key, label):
    index = {}
    for row in rows:
        value = nonempty(row.get(key), label + "." + key)
        if value in index:
            raise IndexErrorDetail(f"Duplicate {label} identity: {value}")
        index[value] = row
    return index

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def resolved_path(value, relative_to):
    p = Path(value)
    return (p if p.is_absolute() else relative_to / p).resolve()

def stat_signature(path):
    st = Path(path).stat()
    return (st.st_size, st.st_mtime_ns)

def validate_new_output(output, source_paths):
    output = Path(output).resolve()
    if output.exists():
        raise IndexErrorDetail(f"Output already exists; use a new directory: {output}")
    for path in source_paths:
        path = Path(path).resolve()
        if output == path or output in path.parents:
            raise IndexErrorDetail("Output may not contain or replace a source")
    return output

def build_index(config_path, output_dir):
    config_path = Path(config_path).resolve(strict=True)
    config = read_json(config_path)
    allowed = set(REQUIRED + OPTIONAL + ("verify_raw_files", "expected_sha256"))
    if not isinstance(config, dict) or set(config) - allowed:
        raise IndexErrorDetail("Unsupported config keys")
    if any(not config.get(key) for key in REQUIRED):
        raise IndexErrorDetail("Config requires inventory, downloads, file_audit, order_lines")
    if not isinstance(config.get("verify_raw_files", True), bool):
        raise IndexErrorDetail("verify_raw_files must be boolean")
    sources = {key: resolved_path(config[key], config_path.parent) for key in REQUIRED + OPTIONAL if config.get(key)}
    output = validate_new_output(output_dir, [config_path, *sources.values()])
    source_hashes = {key: sha256_file(path) for key, path in sources.items()}
    config_hash = sha256_file(config_path)
    for key, expected in config.get("expected_sha256", {}).items():
        if key not in source_hashes or source_hashes[key].lower() != str(expected).lower():
            raise IndexErrorDetail(f"Input checksum mismatch: {key}")
    inventory = read_json(sources["inventory"])
    if (not isinstance(inventory, dict) or not isinstance(inventory.get("items"), list)
        or not isinstance(inventory.get("complete"), bool)
        or not isinstance(inventory.get("errors", []), list)
        or not isinstance(inventory.get("root"), str) or not inventory["root"].strip()):
        raise IndexErrorDetail("Inventory requires root, items list, complete boolean and errors list")
    items = unique_rows(inventory["items"], "token", "inventory")
    files = {key: val for key, val in items.items() if val.get("type") != "folder"}
    downloads = defaultdict(list)
    for row in read_jsonl(sources["downloads"]):
        downloads[nonempty(row.get("token"), "download.token")].append(row)
    audits = unique_rows(read_jsonl(sources["file_audit"]), "token", "audit")
    if set(audits) - set(files):
        raise IndexErrorDetail("File audit contains sources outside inventory")
    lines = read_jsonl(sources["order_lines"])
    unique_rows(lines, "line_id", "order_line")
    verify_files = config.get("verify_raw_files", True)
    documents = []
    raw_signatures = {}
    for token, item in files.items():
        history = downloads.get(token, [])
        successes = [x for x in history if x.get("status") in ("ok", "success", "downloaded")]
        hashes = {nonempty(x.get("sha256"), "download.sha256").lower() for x in successes}
        if len(hashes) > 1:
            raise IndexErrorDetail(f"Conflicting successful versions require explicit selection: {token}")
        audit = audits.get(token)
        expected = next(iter(hashes), None)
        if audit and audit.get("source_sha256"):
            audit_hash = audit["source_sha256"].lower()
            if expected and expected != audit_hash:
                raise IndexErrorDetail(f"Audit and download hash differ: {token}")
            expected = audit_hash
        paths = []
        for row in ([audit] if audit else []) + successes:
            if row.get("local_path"):
                value = str(resolved_path(row["local_path"], sources["downloads"].parent))
                if value not in paths:
                    paths.append(value)
        existing = [x for x in paths if Path(x).is_file()]
        path = existing[0] if existing else (paths[0] if paths else None)
        state, observed = "NO_LOCAL_FILE", None
        if path and Path(path).is_file():
            raw_signatures[path] = stat_signature(path)
            if verify_files:
                observed = sha256_file(path)
                state = "HASH_MATCH" if expected and observed == expected else "HASH_MISMATCH" if expected else "HASH_UNREGISTERED"
            else:
                state = "NOT_VERIFIED"
        elif path:
            state = "LOCAL_MISSING"
        if raw_signatures.get(path) and stat_signature(path) != raw_signatures[path]:
            raise IndexErrorDetail(f"Raw source changed while hashing: {token}")
        metadata_changes = []
        if successes:
            # Every successful record belongs to the selected content. Differing
            # metadata is retained and requires refresh rather than guessed latest.
            for field in ("modified_time", "type"):
                stored = {str(x[field]) for x in successes if x.get(field) is not None}
                if item.get(field) is not None and stored and str(item[field]) not in stored:
                    metadata_changes.append(field)
        documents.append({
            "source_id": token,
            "version_id": digest([token, expected]) if expected else None,
            "content_sha256": expected,
            "observed_sha256": observed,
            "file_verification": state,
            "metadata_status": "CHANGED_RECHECK_REQUIRED" if metadata_changes else "ARCHIVED_METADATA_ONLY",
            "metadata_changes": metadata_changes,
            "file_name": item.get("name") or (audit or {}).get("file_name"),
            "source_url": item.get("url") or (audit or {}).get("source_url"),
            "local_path": path,
            "local_path_aliases": paths,
            "acquired_at": next((x.get("acquired_at") for x in successes if x.get("acquired_at")), None),
            "inventory_as_of": inventory.get("as_of_utc"),
            "source_modified_at_raw": item.get("modified_time"),
            "market": (audit or {}).get("market"),
            "factory": (audit or {}).get("factory"),
            "spu_candidates": (audit or {}).get("spus", []),
            "order_id_candidates": (audit or {}).get("order_ids", []),
            "parse_status": (audit or {}).get("status", "NO_AUDIT"),
            "source_identity_basis": "FILE_RESOURCE_NOT_PURCHASE_ORDER",
            "inventory": item,
            "download_history": history,
            "file_audit": audit,
        })
    doc_by_id = {d["source_id"]: d for d in documents}
    same_content = defaultdict(list)
    for d in documents:
        if d["content_sha256"]:
            same_content[d["content_sha256"]].append(d["source_id"])
    for d in documents:
        d["same_content_source_ids"] = same_content.get(d["content_sha256"], [])
    status = read_json(sources["status_index"]) if "status_index" in sources else None
    statuses = status.get("orders", {}) if status else {}
    order_rows = []
    for row in lines:
        token = nonempty(row.get("token"), "order_line.token")
        if token not in doc_by_id:
            raise IndexErrorDetail(f"Unregistered row source: {token}")
        doc = doc_by_id[token]
        if row.get("source_sha256") != doc["content_sha256"]:
            raise IndexErrorDetail(f"Row source hash differs from document: {row['line_id']}")
        order_state = statuses.get(row.get("order_id"))
        shop_evidence = (order_state or {}).get("by_spu", {}).get(row.get("spu"))
        order_rows.append({
            "evidence_id": digest([doc["version_id"], row["line_id"]]),
            "source_id": token, "version_id": doc["version_id"],
            "content_sha256": doc["content_sha256"],
            "file_verification": doc["file_verification"],
            "snapshot_validated": doc["file_verification"] == "HASH_MATCH",
            "current_remote_version_verified": False,
            "raw": row,
            "order_status_evidence": order_state,
            "shop_evidence": shop_evidence,
            "status_source_acquired_at": (status or {}).get("source_acquired_at"),
            "status_source_sha256": source_hashes.get("status_index"),
            "future_applicability": "NOT_DECIDED_BY_INDEX",
        })
    row_index = {x["raw"]["line_id"]: x for x in order_rows}
    enriched_rows = []
    if "enriched_evidence" in sources:
        enriched = read_json(sources["enriched_evidence"])
        unique_rows(enriched.get("orders", []), "line_id", "enriched")
        for row in enriched.get("orders", []):
            original = row_index.get(row["line_id"])
            if original is None:
                raise IndexErrorDetail(f"Enriched row has no indexed source row: {row['line_id']}")
            if row.get("source_sha256") != original["content_sha256"]:
                raise IndexErrorDetail(f"Enriched source version mismatch: {row['line_id']}")
            enriched_rows.append({
                "evidence_id": original["evidence_id"],
                "source_id": original["source_id"], "version_id": original["version_id"],
                "content_sha256": original["content_sha256"],
                "file_verification": original["file_verification"],
                "snapshot_validated": original["snapshot_validated"],
                "current_remote_version_verified": False,
                "market": original["raw"].get("market"),
                "raw": row,
            })
    confirmations = []
    if "manual_evidence" in sources:
        manual = read_json(sources["manual_evidence"])
        unique_rows(manual.get("master", []), "id", "manual_master")
        manual_source_rows = unique_rows(manual.get("orders", []), "line_id", "manual_evidence_order")
        for row in manual.get("master", []):
            if not row.get("manual_resolution"):
                continue
            source_links = []
            for line_id in row.get("order_line_ids", []):
                prior = manual_source_rows.get(line_id)
                current = row_index.get(line_id)
                prior_hash = (prior or {}).get("source_sha256")
                current_hash = (current or {}).get("content_sha256")
                link_state = ("UNRESOLVED" if current is None else "UNVERSIONED" if not prior_hash
                              else "MATCH" if prior_hash == current_hash else "MISMATCH")
                source_links.append({"line_id": line_id, "confirmation_evidence_sha256": prior_hash,
                                     "indexed_source_sha256": current_hash, "version_status": link_state})
            confirmation_state = ("REFERENCED_FILE_VERSIONS_MATCH" if source_links and all(x["version_status"] == "MATCH" for x in source_links)
                                  else "REVIEW_REQUIRED")
            confirmations.append({
                "review_id": row["id"], "spu": row.get("spu"),
                "source_color": row.get("source_color"), "material_before_review": row.get("material"),
                "manual_resolution": row["manual_resolution"],
                "manual_archive_sha256": source_hashes["manual_evidence"],
                "source_line_ids": row.get("order_line_ids", []),
                "source_version_links": source_links,
                "confirmation_reference_status": confirmation_state,
                "confirmation_applied_to_current_source": False,
                "unresolved_source_line_ids": [v for v in row.get("order_line_ids", []) if v not in row_index],
                "original_future_default_confirmed": row.get("future_default_confirmed"),
            })
    # Check input registries and raw file stats again before publishing any index.
    if sha256_file(config_path) != config_hash:
        raise IndexErrorDetail("Config changed during build")
    for key, path in sources.items():
        if sha256_file(path) != source_hashes[key]:
            raise IndexErrorDetail(f"Archive changed during build: {key}")
    for path, signature in raw_signatures.items():
        if not Path(path).is_file() or stat_signature(path) != signature:
            raise IndexErrorDetail(f"Raw source changed during build: {path}")
    summary = {
        "documents": len(documents),
        "distinct_content_hashes": len(same_content),
        "order_rows": len(order_rows), "enriched_rows": len(enriched_rows),
        "confirmations": len(confirmations),
        "file_verification": dict(Counter(d["file_verification"] for d in documents)),
        "parse_status": dict(Counter(d["parse_status"] for d in documents)),
        "raw_spu_identifiers": len({x["raw"].get("spu") for x in order_rows if x["raw"].get("spu")}),
        "raw_nonempty_order_ids": len({x["raw"].get("order_id") for x in order_rows if x["raw"].get("order_id")}),
        "unknown_order_date_rows": sum(not x["raw"].get("order_date") for x in order_rows),
        "duplicate_marked_rows": sum(bool(x["raw"].get("duplicate_line_of")) for x in order_rows),
        "inventory_complete_as_archived": inventory.get("complete") is True and not inventory.get("errors"),
    }
    output.mkdir(parents=True, exist_ok=False)
    layers = {"documents.jsonl": documents, "order_rows.jsonl": order_rows,
              "enriched_rows.jsonl": enriched_rows, "confirmations.jsonl": confirmations}
    for name, rows in layers.items():
        write_jsonl(output / name, rows)
    manifest = {
        "schema_version": SCHEMA_VERSION, "code_version": VERSION,
        "script_sha256": sha256_file(__file__), "built_at": utc_now(),
        "status": "LOCAL_INDEX_COMPLETE" if summary["inventory_complete_as_archived"] else "LOCAL_INDEX_PARTIAL_SCOPE",
        "source_freshness": "ARCHIVED_NOT_LIVE",
        "business_validation": "PARTIAL_NOT_EFFECTIVE_ORDER_APPROVAL",
        "config": {"path": str(config_path), "sha256": config_hash},
        "sources": {k: {"path": str(v), "sha256": source_hashes[k]} for k, v in sources.items()},
        "raw_files_hash_checked": verify_files,
        "summary": summary,
        "outputs": {k: {"sha256": sha256_file(output / k), "rows": len(v)} for k, v in layers.items()},
        "limitations": [
            "No current remote revalidation; source timestamps retain their original meaning.",
            "Retrieval does not resolve color identity, source eligibility or future applicability.",
            "Row and sheet positions identify this file version, not a cross-version business key.",
            "Reference-only evidence and image contents remain in their original archive; they are not new semantic extraction.",
            "No database tables, schedule or remote writes are created.",
        ],
    }
    write_json(output / "manifest.json", manifest)
    return manifest

def flatten_text(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(flatten_text(v) for v in value)
    return str(value)

def exact_any(values, choices):
    return not choices or bool({str(v) for v in values if v is not None} & set(choices))

def text_any(value, choices):
    text = flatten_text(value).casefold()
    return not choices or any(x.casefold() in text for x in choices)

def row_matches(row, document, filters, enriched=False):
    raw = row["raw"]
    reasons = []
    spus = [raw.get("spu"), *raw.get("spu_candidates", [])]
    orders = [raw.get("order_id"), *raw.get("order_id_candidates", [])]
    if not exact_any(spus, filters.get("spu")) or not exact_any(orders, filters.get("order_id")):
        return None
    if not exact_any([row["source_id"]], filters.get("source_id")):
        return None
    if not exact_any([raw.get("market") or row.get("market") or document.get("market")], filters.get("market")):
        return None
    material = [raw.get(k) for k in ("fabric_header", "fabric_row_note", "fabric_assignment_sources", "header_material_raw", "row_material_raw", "material_interpretation", "parts")]
    color = [raw.get(k) for k in ("color_raw", "garment_color_raw", "color_code", "color_name", "color_alternatives", "order_color", "physical_numbers", "candidates")]
    if not text_any(material, filters.get("material")) or not text_any(color, filters.get("color")):
        return None
    if not text_any(document.get("file_name"), filters.get("file_name")):
        return None
    if filters.get("spu"):
        reasons.append("EXACT_PARSED_SPU_OR_EXPLICIT_CANDIDATE; inspect conflicts")
    if filters.get("order_id"):
        reasons.append("EXACT_PARSED_ORDER_ID_OR_EXPLICIT_CANDIDATE")
    if filters.get("material") or filters.get("color"):
        reasons.append("TEXT_RETRIEVAL_ONLY_NOT_IDENTITY_MATCH")
    return reasons or ["EXPLICIT_SOURCE_OR_MARKET_OR_FILE_FILTER"]

def query_index(index_dir, output_dir, filters, limit=1000):
    index = Path(index_dir).resolve(strict=True)
    if not any(filters.values()):
        raise IndexErrorDetail("At least one explicit query filter is required")
    if not isinstance(limit, int) or limit <= 0:
        raise IndexErrorDetail("limit must be a positive integer")
    output = validate_new_output(output_dir, [index / "manifest.json"])
    manifest_path = index / "manifest.json"
    manifest_before = sha256_file(manifest_path)
    manifest = read_json(manifest_path)
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise IndexErrorDetail("Unsupported index schema")
    layers = {}
    for name, receipt in manifest["outputs"].items():
        if Path(name).name != name or name not in ("documents.jsonl", "order_rows.jsonl", "enriched_rows.jsonl", "confirmations.jsonl"):
            raise IndexErrorDetail("Unexpected index output path")
        path = index / name
        if sha256_file(path) != receipt["sha256"]:
            raise IndexErrorDetail(f"Index was modified: {name}")
        layers[name] = read_jsonl(path)
        if len(layers[name]) != receipt["rows"]:
            raise IndexErrorDetail(f"Index row count mismatch: {name}")
    documents = {x["source_id"]: x for x in layers["documents.jsonl"]}
    result_rows, result_enriched = [], []
    matched_sources, matched_lines = set(), set()
    for name, destination in (("order_rows.jsonl", result_rows), ("enriched_rows.jsonl", result_enriched)):
        for row in layers[name]:
            document = documents[row["source_id"]]
            reasons = row_matches(row, document, filters, name == "enriched_rows.jsonl")
            if reasons is not None:
                destination.append({**row, "query_match_reasons": reasons})
                matched_sources.add(row["source_id"])
                matched_lines.add(row["raw"]["line_id"])
    # File-only candidates may exist even where no order table was identified.
    # They never get material/color eligibility or guessed SPU from a filename.
    doc_only = set()
    if not filters.get("material") and not filters.get("color"):
        for source_id, doc in documents.items():
            if (exact_any(doc.get("spu_candidates", []), filters.get("spu"))
                and exact_any(doc.get("order_id_candidates", []), filters.get("order_id"))
                and exact_any([source_id], filters.get("source_id"))
                and exact_any([doc.get("market")], filters.get("market"))
                and text_any(doc.get("file_name"), filters.get("file_name"))):
                if source_id not in matched_sources:
                    doc_only.add(source_id)
                matched_sources.add(source_id)
    selected_docs = []
    current_checks = {}
    for source_id in sorted(matched_sources):
        doc = documents[source_id]
        path = doc.get("local_path")
        if not path or not Path(path).is_file():
            current = "LOCAL_MISSING"
        else:
            actual = sha256_file(path)
            current = "HASH_MATCH" if actual == doc.get("content_sha256") else "HASH_MISMATCH"
        current_checks[source_id] = current
        selected_docs.append({**doc, "query_time_file_verification": current,
                              "query_match_scope": "FILE_AUDIT_CANDIDATE_ONLY" if source_id in doc_only else "ROW_EVIDENCE"})
    for row in result_rows + result_enriched:
        row["query_time_file_verification"] = current_checks[row["source_id"]]
        row["snapshot_validated"] = current_checks[row["source_id"]] == "HASH_MATCH"
    # Confirmations are associated through explicit evidence line IDs only. No
    # broad SPU propagation can imply confirmation of another color or month.
    confirmations = []
    line_sources = {r["raw"]["line_id"]: r["source_id"] for r in layers["order_rows.jsonl"]}
    for confirmation in layers["confirmations.jsonl"]:
        if not set(confirmation["source_line_ids"]) & matched_lines:
            continue
        current_statuses = {line_id: current_checks.get(line_sources.get(line_id), "NOT_CHECKED_BY_THIS_QUERY")
                            for line_id in confirmation["source_line_ids"]}
        confirmations.append({**confirmation, "query_time_source_checks": current_statuses,
                              "query_time_review_required": confirmation.get("confirmation_reference_status") != "REFERENCED_FILE_VERSIONS_MATCH"
                              or any(v != "HASH_MATCH" for v in current_statuses.values())})
    if sha256_file(manifest_path) != manifest_before:
        raise IndexErrorDetail("Index manifest changed during query")
    for name, receipt in manifest["outputs"].items():
        if sha256_file(index / name) != receipt["sha256"]:
            raise IndexErrorDetail(f"Index layer changed during query: {name}")
    output.mkdir(parents=True, exist_ok=False)
    result = {
        "schema_version": "garment_order_source_query/1", "queried_at": utc_now(),
        "index_manifest_sha256": manifest_before,
        "index_source_freshness": manifest["source_freshness"],
        "filters": filters, "limit_per_layer": limit,
        "totals_before_limit": {"documents": len(selected_docs), "order_rows": len(result_rows), "enriched_rows": len(result_enriched), "confirmations": len(confirmations)},
        "truncated": any(len(x) > limit for x in (selected_docs, result_rows, result_enriched, confirmations)),
        "documents": selected_docs[:limit], "order_rows": result_rows[:limit],
        "enriched_rows": result_enriched[:limit], "confirmations": confirmations[:limit],
        "decision": "RETRIEVAL_ONLY_NO_AUTOMATIC_BUSINESS_CONFIRMATION",
        "remote_calls": 0, "database_operations": 0,
    }
    write_json(output / "query.json", result)
    write_json(output / "manifest.json", {"script_sha256": sha256_file(__file__), "index_manifest_sha256": manifest_before,
              "query_sha256": sha256_file(output / "query.json"), "totals_before_limit": result["totals_before_limit"],
              "truncated": result["truncated"], "queried_at": result["queried_at"]})
    return result

def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build", help="Build immutable local index from explicit source archives")
    b.add_argument("--config", required=True)
    b.add_argument("--output-dir", required=True)
    q = sub.add_parser("query", help="Retrieve evidence; exact SPU/order IDs, textual material/color clues")
    q.add_argument("--index", required=True)
    q.add_argument("--output-dir", required=True)
    for name in ("spu", "order-id", "material", "color", "market", "source-id", "file-name"):
        q.add_argument("--" + name, action="append")
    q.add_argument("--limit", type=int, default=1000)
    args = p.parse_args(argv)
    try:
        if args.command == "build":
            result = build_index(args.config, args.output_dir)
            print(canonical({"status": result["status"], "summary": result["summary"], "output_dir": str(Path(args.output_dir).resolve())}))
        else:
            filters = {name: getattr(args, name) for name in ("spu", "order_id", "material", "color", "market", "source_id", "file_name")}
            result = query_index(args.index, args.output_dir, filters, args.limit)
            print(canonical({"totals": result["totals_before_limit"], "truncated": result["truncated"], "output_dir": str(Path(args.output_dir).resolve())}))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(canonical({"status": "FAILED", "error": str(exc)}), file=sys.stderr)
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
