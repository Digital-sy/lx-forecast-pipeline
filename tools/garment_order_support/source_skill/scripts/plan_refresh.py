#!/usr/bin/env python3
"""Plan immutable, read-only order-document refreshes. No network or database code."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

VERSION = "1.0.0"
SHA_RE = re.compile(r"^[0-9a-fA-F]{64}$")
META_KEYS = ("name", "type", "url", "created_time", "modified_time", "parent_token", "parent_path", "path", "revision", "version", "etag")
HASH_KEYS = ("sha256", "content_sha256", "source_sha256")
SCOPE_KEYS = ("scope", "query_scope", "filters", "recursive")
VOLATILE_STATUS_KEYS = {"source_acquired_at", "acquired_at", "source_row_indices", "unmapped_sid_row_indices", "source_row_index", "source_path", "source_sha256"}

class PlanError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def no_duplicate_keys(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise PlanError(f"Duplicate JSON object key: {key}")
        obj[key] = value
    return obj


def read_json(path):
    path = Path(path).resolve(strict=True)
    raw = path.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=no_duplicate_keys,
                           parse_constant=lambda value: (_ for _ in ()).throw(PlanError(f"Invalid number: {value}")))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PlanError(f"Invalid JSON input: {path.name}") from exc
    if not isinstance(value, dict):
        raise PlanError(f"Expected JSON object: {path.name}")
    return value, {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}


def content_hash(item):
    values = {str(item[k]).lower() for k in HASH_KEYS if item.get(k) not in (None, "")}
    if len(values) > 1:
        raise PlanError(f"Conflicting content hash fields for token {item.get('token')}")
    if values and not SHA_RE.fullmatch(next(iter(values))):
        raise PlanError(f"Invalid SHA256 for token {item.get('token')}")
    return next(iter(values), None)


def inventory_index(inv):
    if not isinstance(inv.get("items"), list):
        raise PlanError("inventory.items must be a list")
    if not isinstance(inv.get("complete"), bool):
        raise PlanError("inventory.complete must be an explicit boolean")
    if "errors" in inv and not isinstance(inv["errors"], list):
        raise PlanError("inventory.errors must be a list")
    rows, repeats = {}, 0
    for item in inv["items"]:
        if not isinstance(item, dict) or not isinstance(item.get("token"), str) or not item["token"].strip():
            raise PlanError("Every inventory item requires a nonempty token")
        token = item["token"]
        if token in rows:
            if canonical(rows[token]) != canonical(item):
                raise PlanError(f"Conflicting duplicate token: {token}")
            repeats += 1
            continue
        content_hash(item)
        rows[token] = item
    return rows, repeats


def scope_identity(inv):
    root = inv.get("root")
    if not isinstance(root, str) or not root.strip():
        return None
    return {"root": root, **{k: inv[k] for k in SCOPE_KEYS if k in inv}}


def is_complete(inv):
    return inv.get("complete") is True and not inv.get("errors", [])


def metadata(item):
    return {k: item.get(k) for k in META_KEYS}


def _action(token, previous, current, state, action, flags, previous_parser_version, parser_version):
    old_hash = content_hash(previous) if previous else None
    new_hash = content_hash(current) if current else None
    return {
        "source_id": token, "token": token,
        "resource_type": (current or previous).get("type"),
        "state": state, "action": action, "flags": flags,
        "previous_metadata": metadata(previous) if previous else None,
        "current_metadata": metadata(current) if current else None,
        "previous_content_sha256": old_hash, "current_content_sha256": new_hash,
        "previous_parser_version": previous_parser_version, "parser_version": parser_version,
        "business_effect": "PRESERVE_EXISTING_FACTS_AND_MANUAL_CONFIRMATIONS",
        "executed": False,
    }


def plan_documents(previous, current, *, previous_parser_version=None, parser_version=None):
    old, old_repeats = inventory_index(previous)
    new, new_repeats = inventory_index(current)
    old_scope, new_scope = scope_identity(previous), scope_identity(current)
    same_scope = old_scope is not None and old_scope == new_scope
    parser_changed = bool(previous_parser_version and parser_version and previous_parser_version != parser_version)
    actions = []
    for token in sorted(old.keys() | new.keys()):
        before, after = old.get(token), new.get(token)
        flags = []
        if not same_scope:
            flags.append("SCOPE_NOT_COMPARABLE")
        if after is None:
            if same_scope and is_complete(current):
                state, action = "SOURCE_MISSING", "FLAG_SOURCE_MISSING_RETAIN_ARCHIVE"
            else:
                state, action = "SOURCE_PRESENCE_UNKNOWN", "KEEP_AND_RECHECK_SCOPE"
        elif before is None:
            state = "NEW"
            action = "DISCOVER_CHILDREN_REQUIRED" if after.get("type") == "folder" else "FETCH_REQUIRED"
        elif before.get("type") != after.get("type"):
            state, action = "METADATA_CHANGED", "REVIEW_RESOURCE_TYPE_REQUIRED"
            flags.append("RESOURCE_TYPE_CHANGED")
        elif after.get("type") == "folder":
            state = "METADATA_CHANGED" if metadata(before) != metadata(after) else "METADATA_UNCHANGED_CONTENT_UNVERIFIED"
            action = "DISCOVER_CHILDREN_REQUIRED"
            flags.append("FOLDER_METADATA_DOES_NOT_PROVE_CHILD_CONTENT")
        else:
            old_hash, new_hash = content_hash(before), content_hash(after)
            meta_changed = metadata(before) != metadata(after)
            modified_unknown = before.get("modified_time") in (None, "") or after.get("modified_time") in (None, "")
            if modified_unknown:
                flags.append("MODIFIED_TIME_UNKNOWN")
            if parser_changed:
                flags.append("PARSER_CHANGED")
            if old_hash and new_hash and old_hash != new_hash:
                state, action = "CONTENT_CHANGED", "FETCH_REQUIRED"
            elif meta_changed:
                # Even a repeated token/local path must not bypass source verification.
                state, action = "METADATA_CHANGED", "FETCH_REQUIRED"
                if old_hash and new_hash and old_hash == new_hash:
                    flags.append("SAME_CONTENT_HASH_BUT_METADATA_CHANGED")
            elif old_hash and new_hash and old_hash == new_hash:
                if parser_changed:
                    state, action = "REPARSE_REQUIRED", "REPARSE_VERIFIED_CONTENT"
                else:
                    state, action = "CONTENT_UNCHANGED_VERIFIED", "REUSE_VERIFIED_CONTENT"
                if not previous_parser_version or not parser_version:
                    flags.append("PARSER_VERSION_UNKNOWN")
                    action = "VERIFY_PARSER_BEFORE_REUSE"
            else:
                state, action = "METADATA_UNCHANGED_CONTENT_UNVERIFIED", "VERIFY_CONTENT_REQUIRED"
                if parser_changed:
                    flags.append("REPARSE_AFTER_CONTENT_VERIFIED")
            if modified_unknown and not (old_hash and new_hash and old_hash == new_hash):
                flags.append("DO_NOT_REUSE_ON_TOKEN_OR_LOCAL_EXISTS")
        actions.append(_action(token, before, after, state, action, flags, previous_parser_version, parser_version))
    return actions, {
        "same_scope": same_scope, "previous_scope": old_scope, "current_scope": new_scope,
        "previous_inventory_complete": is_complete(previous), "current_inventory_complete": is_complete(current),
        "previous_items": len(old), "current_items": len(new),
        "previous_exact_duplicate_items": old_repeats, "current_exact_duplicate_items": new_repeats,
        "state_counts": dict(Counter(a["state"] for a in actions)),
        "action_counts": dict(Counter(a["action"] for a in actions)),
    }


def business_status(value):
    if isinstance(value, dict):
        return {k: business_status(v) for k, v in value.items() if k not in VOLATILE_STATUS_KEYS}
    if isinstance(value, list):
        return [business_status(v) for v in value]
    return value


def plan_status(previous, current):
    if previous is None and current is None:
        return [], {"status": "NOT_PROVIDED", "actions": 0}
    if previous is None or current is None:
        raise PlanError("Both previous-status and current-status are required together")
    old, new = previous.get("orders"), current.get("orders")
    if not isinstance(old, dict) or not isinstance(new, dict):
        raise PlanError("Status snapshots require orders object keyed by order ID")
    actions = []
    for key in sorted(old.keys() | new.keys()):
        if not key.strip() or (key in old and not isinstance(old[key], dict)) or (key in new and not isinstance(new[key], dict)):
            raise PlanError("Status order keys and records must be valid")
        before, after = old.get(key), new.get(key)
        old_hash = digest(business_status(before)) if before is not None else None
        new_hash = digest(business_status(after)) if after is not None else None
        if before is None:
            state, action = "STATUS_NEW_EVIDENCE", "REVIEW_ELIGIBILITY"
        elif after is None:
            state, action = "STATUS_PRESENCE_UNKNOWN", "RETAIN_PREVIOUS_STATUS_MARK_UNKNOWN"
        elif old_hash != new_hash:
            state, action = "STATUS_CHANGED", "REVIEW_ELIGIBILITY_AND_REFERENCED_CONFIRMATIONS"
        else:
            state, action = "STATUS_UNCHANGED", "KEEP_BUSINESS_STATE_RECORD_NEW_OBSERVATION"
        actions.append({"order_id": key, "state": state, "action": action,
                        "previous_business_hash": old_hash, "current_business_hash": new_hash,
                        "previous_status": before.get("order_status_text") if before else None,
                        "current_status": after.get("order_status_text") if after else None,
                        "previous_acquired_at": previous.get("source_acquired_at"),
                        "current_acquired_at": current.get("source_acquired_at"),
                        "document_action": "NO_IMPLIED_CONTENT_CHANGE", "executed": False})
    return actions, {"status": "PLANNED_ONLY", "actions": len(actions),
                     "state_counts": dict(Counter(a["state"] for a in actions))}


def require_unchanged(receipts):
    for receipt in receipts:
        if file_hash(receipt["path"]) != receipt["sha256"]:
            raise PlanError(f"Source changed during planning: {Path(receipt['path']).name}")


def run(previous_path, current_path, output_dir, *, previous_parser_version=None, parser_version=None,
        previous_status_path=None, current_status_path=None):
    output = Path(output_dir).resolve()
    if output.exists():
        raise PlanError("Output directory already exists; refusing to overwrite")
    previous, prev_receipt = read_json(previous_path)
    current, curr_receipt = read_json(current_path)
    receipts = [dict(role="previous_inventory", **prev_receipt), dict(role="current_inventory", **curr_receipt)]
    actions, summary = plan_documents(previous, current, previous_parser_version=previous_parser_version, parser_version=parser_version)
    previous_status = current_status = None
    if bool(previous_status_path) != bool(current_status_path):
        raise PlanError("Both status paths are required together")
    if previous_status_path:
        previous_status, receipt = read_json(previous_status_path)
        receipts.append(dict(role="previous_status", **receipt))
        current_status, receipt = read_json(current_status_path)
        receipts.append(dict(role="current_status", **receipt))
    status_actions, status_summary = plan_status(previous_status, current_status)
    require_unchanged(receipts)
    output.mkdir(parents=True, exist_ok=False)
    for name, rows in [("actions.jsonl", actions), ("status_actions.jsonl", status_actions)]:
        with (output / name).open("x", encoding="utf-8", newline="\n") as stream:
            for row in rows:
                stream.write(canonical(row) + "\n")
    manifest = {
        "schema_version": "order_refresh_plan_v1", "planner_version": VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(), "status": "PLAN_ONLY_NOT_SYNCED",
        "sources": receipts, "documents": summary, "order_status": status_summary,
        "parser_versions": {"previous": previous_parser_version, "current": parser_version},
        "plan_digest": digest({"actions": actions, "status_actions": status_actions}),
        "outputs": {name: file_hash(output / name) for name in ("actions.jsonl", "status_actions.jsonl")},
        "external_calls": 0, "database_writes": 0, "source_deletions": 0,
        "cursor_advanced": False, "manual_confirmation_mutated": False,
        "limits": ["No retrieval, parsing or synchronization is performed by this planner.",
                   "Current metadata without a content hash does not prove unchanged file bytes.",
                   "Missing source means absent from comparable complete inventory, not authorized deletion.",
                   "Order status evidence cannot establish file content or future material applicability."],
    }
    try:
        require_unchanged(receipts)
    except PlanError:
        manifest["status"] = "FAILED_SOURCE_CHANGED"
        with (output / "manifest.json").open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        raise
    with (output / "manifest.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--previous-parser-version")
    parser.add_argument("--parser-version")
    parser.add_argument("--previous-status")
    parser.add_argument("--current-status")
    args = parser.parse_args(argv)
    try:
        result = run(args.previous, args.current, args.output_dir,
                     previous_parser_version=args.previous_parser_version, parser_version=args.parser_version,
                     previous_status_path=args.previous_status, current_status_path=args.current_status)
    except (PlanError, OSError) as exc:
        print(json.dumps({"status": "ERROR", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps({"status": result["status"], "documents": result["documents"]["state_counts"],
                      "order_status": result["order_status"], "plan_digest": result["plan_digest"]}, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
