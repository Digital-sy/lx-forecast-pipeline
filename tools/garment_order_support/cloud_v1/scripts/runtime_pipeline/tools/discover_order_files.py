#!/usr/bin/env python3
"""Bounded read-only Feishu folder discovery. No download, auth setup or DB access.

Default is a local read plan. --execute-read explicitly enables the existing
explicit bot read transport. The only dispatched operation is drive files list.
"""
from __future__ import annotations

import argparse
from collections import Counter, deque
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlsplit, urlunsplit

VERSION = "1.1.0-explicit-bot"
TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
ITEM_FIELDS = ("token", "name", "type", "created_time", "modified_time", "url")


class DiscoveryError(ValueError):
    def __init__(self, kind, code=None):
        self.kind = kind
        self.code = code if isinstance(code, int) and not isinstance(code, bool) else None
        super().__init__(kind)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def sha256_file(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8", newline="\n") as f:
        json.dump(value, f, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        f.write("\n")


def safe_error_payload(proc):
    # Never forward stderr, error messages, hints, authorization links or headers.
    try:
        obj = json.loads(proc.stderr or "{}")
        error = obj.get("error", {}) if isinstance(obj, dict) else {}
        code = error.get("code") if isinstance(error, dict) else None
    except (ValueError, TypeError):
        code = None
    return DiscoveryError("CLI_REQUEST_FAILED", code)


def subprocess_runner(argv, timeout):
    env = dict(os.environ, LARKSUITE_CLI_NO_UPDATE_NOTIFIER="1", LARKSUITE_CLI_NO_SKILLS_NOTIFIER="1")
    try:
        return subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", errors="strict", timeout=timeout, shell=False, env=env)
    except subprocess.TimeoutExpired:
        raise DiscoveryError("CLI_TIMEOUT") from None
    except (OSError, UnicodeError):
        raise DiscoveryError("CLI_UNAVAILABLE_OR_OUTPUT_INVALID") from None


def checked_token(value, label):
    if not isinstance(value, str) or not TOKEN.fullmatch(value):
        raise DiscoveryError(label)
    return value


def public_url(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise DiscoveryError("INVALID_ITEM_URL")
    u = urlsplit(value)
    if u.scheme != "https" or not u.netloc or u.username is not None or u.password is not None:
        raise DiscoveryError("INVALID_ITEM_URL")
    # Drive files.list returns browser links, not signed download URLs. Avoid
    # retaining arbitrary query parameters if a provider adds them later.
    return urlunsplit((u.scheme, u.netloc, u.path, "", ""))


def normalize_item(raw, folder_token, parent_path):
    if not isinstance(raw, dict):
        raise DiscoveryError("INVALID_ITEM")
    checked_token(raw.get("token"), "INVALID_ITEM_TOKEN")
    for key in ("name", "type"):
        if not isinstance(raw.get(key), str) or not raw[key]:
            raise DiscoveryError("INVALID_ITEM_" + key.upper())
    parent = raw.get("parent_token")
    if parent is not None and parent != folder_token:
        raise DiscoveryError("ITEM_PARENT_OUT_OF_SCOPE")
    item = {key: raw.get(key) for key in ITEM_FIELDS}
    item["url"] = public_url(item["url"])
    for key in ("created_time", "modified_time"):
        if item[key] is not None and not isinstance(item[key], str):
            raise DiscoveryError("INVALID_ITEM_TIME")
    if "shortcut_info" in raw:
        s = raw["shortcut_info"]
        if not isinstance(s, dict):
            raise DiscoveryError("INVALID_SHORTCUT")
        checked_token(s.get("target_token"), "INVALID_SHORTCUT_TOKEN")
        if not isinstance(s.get("target_type"), str) or not s["target_type"]:
            raise DiscoveryError("INVALID_SHORTCUT_TYPE")
        item["shortcut_info"] = {"target_token": s["target_token"], "target_type": s["target_type"]}
    item.update(parent_token=folder_token, parent_path=parent_path, path=parent_path + "/" + raw["name"])
    return item


def parse_page(proc, folder_token, parent_path, identity="bot"):
    if proc.returncode != 0:
        raise safe_error_payload(proc)
    try:
        obj = json.loads(proc.stdout)
    except (ValueError, TypeError):
        raise DiscoveryError("CLI_JSON_INVALID") from None
    if not isinstance(obj, dict) or obj.get("ok") is not True or not isinstance(obj.get("data"), dict):
        raise DiscoveryError("CLI_SUCCESS_ENVELOPE_INVALID")
    if obj.get("identity") != identity:
        raise DiscoveryError("CLI_IDENTITY_MISMATCH")
    data = obj["data"]
    if type(data.get("has_more")) is not bool or not isinstance(data.get("files"), list):
        raise DiscoveryError("PAGE_CONTRACT_INVALID")
    nxt = data.get("next_page_token")
    if data["has_more"]:
        if not isinstance(nxt, str) or not nxt:
            raise DiscoveryError("NEXT_PAGE_TOKEN_MISSING")
        # Pagination tokens are opaque strings; never shell-interpolate them.
        if any(ord(c) < 32 for c in nxt):
            raise DiscoveryError("NEXT_PAGE_TOKEN_INVALID")
    items = [normalize_item(x, folder_token, parent_path) for x in data["files"]]
    if len({x["token"] for x in items}) != len(items):
        raise DiscoveryError("DUPLICATE_TOKEN_IN_PAGE")
    return items, data["has_more"], nxt


def make_argv(prefix, folder_token, page_token, page_size, identity="bot"):
    params = {"folder_token": folder_token, "page_size": page_size}
    if page_token:
        params["page_token"] = page_token
    return [*prefix, "drive", "files", "list", "--params", json.dumps(params, ensure_ascii=False), "--as", identity, "--format", "json"]


def discover(*, folder_token, output_dir, cli_prefix=None, execute_read=False,
             max_pages=100, max_folders=100, max_items=10000, page_size=200,
             timeout=120, runner=None, identity="bot"):
    if identity != "bot":raise DiscoveryError("BOT_IDENTITY_REQUIRED")
    checked_token(folder_token, "EXPLICIT_FOLDER_TOKEN_REQUIRED")
    for value, name in ((max_pages, "MAX_PAGES"), (max_folders, "MAX_FOLDERS"),
                        (max_items, "MAX_ITEMS"), (page_size, "PAGE_SIZE"), (timeout, "TIMEOUT")):
        if type(value) is not int or value <= 0:
            raise DiscoveryError("INVALID_" + name)
    if page_size > 200:
        raise DiscoveryError("PAGE_SIZE_EXCEEDS_SCHEMA_MAXIMUM")
    prefix = list(cli_prefix or ["lark-cli"])
    if not prefix or any(not isinstance(x, str) or not x or "\x00" in x for x in prefix):
        raise DiscoveryError("INVALID_CLI_PREFIX")
    if execute_read and Path(prefix[0]).suffix.lower() in (".cmd", ".bat"):
        # Avoid implicit command-shell handling of opaque continuation tokens.
        raise DiscoveryError("USE_NODE_WITH_CLI_SCRIPT_NOT_CMD_WRAPPER")
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=False)
    acquired = utc_now()
    config = {"root_token": folder_token, "identity": identity, "operation": "drive.files.list",
              "recursive": True, "follow_shortcuts": False, "page_size": page_size,
              "max_pages": max_pages, "max_folders": max_folders, "max_items": max_items,
              "timeout_seconds": timeout, "execute_read": execute_read,
              "cli_prefix": prefix, "version": VERSION}
    write_json(out / "query_config.json", config)
    if not execute_read:
        result = {"schema_version": "1.0", "root_token": folder_token, "root": folder_token,
                  "complete": False, "status": "READ_PLAN_ONLY", "items": [], "pages": 0,
                  "acquired_at": None, "planned_at": acquired, "errors": [], "external_calls": 0}
        write_json(out / "inventory_current.json", result)
        write_json(out / "manifest.json", {"status": "READ_PLAN_ONLY",
                                           "config_canonical_sha256": digest(config),
                                           "config_file_sha256": sha256_file(out / "query_config.json"),
                                           "inventory_canonical_sha256": digest(result),
                                           "inventory_file_sha256": sha256_file(out / "inventory_current.json"),
                                           "hash_contract": "file_sha256 hashes exact bytes; canonical_sha256 hashes normalized JSON",
                                           "network_executed": False})
        return result
    transport = runner or subprocess_runner
    transport_name = "INJECTED_TEST_RUNNER" if runner else "LARK_CLI"
    (out / "pages").mkdir()
    queue = deque([{"folder_token": folder_token, "path": "", "page_token": None}])
    scheduled = {folder_token}
    folders_started = set()
    requested_pages = set()
    tokens_in_folder = {}
    items_by_token = {}
    receipts = []
    errors = []
    requests = 0
    current = None
    try:
        while queue:
            current = queue.popleft()
            folder, path, page_token = current["folder_token"], current["path"], current["page_token"]
            if folder not in folders_started:
                if len(folders_started) >= max_folders:
                    raise DiscoveryError("FOLDER_BUDGET_EXHAUSTED")
                folders_started.add(folder)
                tokens_in_folder[folder] = set()
            while True:
                current["page_token"] = page_token
                if requests >= max_pages:
                    raise DiscoveryError("PAGE_BUDGET_EXHAUSTED")
                page_key = (folder, page_token)
                if page_key in requested_pages:
                    raise DiscoveryError("REPEATED_PAGE_TOKEN")
                requested_pages.add(page_key)
                argv = make_argv(prefix, folder, page_token, page_size, identity)
                requests += 1
                received_at = utc_now()
                receipt = {"request_index": requests, "folder_token": folder, "page_token": page_token,
                           "received_at": None, "request_started_at": received_at, "status": "REQUESTED"}
                receipts.append(receipt)
                proc = transport(argv, timeout)
                receipt["received_at"] = utc_now()
                page_items, more, nxt = parse_page(proc, folder, path, identity)
                if more and (folder, nxt) in requested_pages:
                    raise DiscoveryError("REPEATED_PAGE_TOKEN")
                incoming = {x["token"] for x in page_items}
                if incoming & tokens_in_folder[folder]:
                    raise DiscoveryError("DUPLICATE_TOKEN_ACROSS_PAGES")
                if len(set(items_by_token) | incoming) > max_items:
                    raise DiscoveryError("ITEM_BUDGET_EXHAUSTED")
                # Validate the entire page before applying it to the inventory.
                for item in page_items:
                    token = item["token"]
                    if token == folder_token or token == folder:
                        raise DiscoveryError("FOLDER_CYCLE_OR_SELF_REFERENCE")
                    old = items_by_token.get(token)
                    if old:
                        fields = (*ITEM_FIELDS, "shortcut_info")
                        if any(old.get(k) != item.get(k) for k in fields):
                            raise DiscoveryError("TOKEN_METADATA_CONFLICT")
                safe_page = {"folder_token": folder, "page_token": page_token,
                             "acquired_at": receipt["received_at"], "files": page_items,
                             "has_more": more, "next_page_token": nxt}
                filename = f"page_{requests:05d}.json"
                write_json(out / "pages" / filename, safe_page)
                receipt.update(status="VALIDATED", item_count=len(page_items), page_file="pages/" + filename,
                               content_canonical_sha256=digest(safe_page),
                               file_sha256=sha256_file(out / "pages" / filename),
                               has_more=more, next_page_token=nxt)
                tokens_in_folder[folder].update(incoming)
                for item in page_items:
                    token = item["token"]
                    if token not in items_by_token:
                        item["aliases"] = []
                        items_by_token[token] = item
                    else:
                        items_by_token[token]["aliases"].append({"parent_token": folder, "path": item["path"]})
                    if item["type"] == "folder" and token not in scheduled:
                        scheduled.add(token)
                        queue.append({"folder_token": token, "path": item["path"], "page_token": None})
                if not more:
                    current = None
                    break
                page_token = nxt
    except DiscoveryError as exc:
        errors.append({"kind": exc.kind, "code": exc.code,
                       "folder_token": current["folder_token"] if current else None,
                       "page_token": current["page_token"] if current else None})
        if receipts and receipts[-1]["status"] == "REQUESTED":
            receipts[-1]["status"] = "FAILED_VALIDATION_OR_REQUEST"
    except Exception:
        # Do not serialize an arbitrary exception containing credentials or headers.
        errors.append({"kind": "UNEXPECTED_TRANSPORT_OR_PROCESSING_ERROR", "code": None})
        if receipts and receipts[-1]["status"] == "REQUESTED":
            receipts[-1]["status"] = "FAILED_VALIDATION_OR_REQUEST"
    pending = ([current] if current else []) + list(queue)
    result = {"schema_version": "1.0", "root_token": folder_token, "root": folder_token,
              "complete": not errors and not pending, "status": "COMPLETE" if not errors and not pending else "INCOMPLETE",
              "items": list(items_by_token.values()), "errors": errors, "pages": requests,
              "validated_pages": sum(r["status"] == "VALIDATED" for r in receipts),
              "acquired_at": acquired, "as_of_utc": utc_now(), "pending_folders": pending,
              "transport": transport_name, "external_calls": requests if runner is None else 0,
              "runner_calls": requests, "scope_limit": "Explicit folder descendants; shortcut targets not followed",
              "snapshot_atomicity": "NOT_PROVEN; multi-page listing may change during acquisition"}
    write_json(out / "page_receipts.json", receipts)
    write_json(out / "inventory_current.json", result)
    write_json(out / "manifest.json", {"version": VERSION, "status": result["status"],
                                      "config_canonical_sha256": digest(config),
                                      "config_file_sha256": sha256_file(out / "query_config.json"),
                                      "inventory_canonical_sha256": digest(result),
                                      "inventory_file_sha256": sha256_file(out / "inventory_current.json"),
                                      "page_receipts_canonical_sha256": digest(receipts),
                                      "page_receipts_file_sha256": sha256_file(out / "page_receipts.json"),
                                      "hash_contract": "file_sha256 hashes exact bytes; canonical_sha256 hashes normalized JSON",
                                      "transport": transport_name,
                                      "network_executed": runner is None and requests > 0,
                                      "content_downloaded": False, "database_access": False,
                                      "shortcuts_followed": False})
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--folder-token", required=True)
    p.add_argument("--cli-executable", default="lark-cli")
    p.add_argument("--cli-script", help="Existing CLI JavaScript entrypoint; use with a verified node executable")
    p.add_argument("--identity", choices=("bot",), required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--max-pages", type=int, default=100)
    p.add_argument("--max-folders", type=int, default=100)
    p.add_argument("--max-items", type=int, default=10000)
    p.add_argument("--page-size", type=int, default=200)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--execute-read", action="store_true")
    a = p.parse_args(argv)
    prefix = [a.cli_executable] + ([a.cli_script] if a.cli_script else [])
    try:
        result = discover(folder_token=a.folder_token, output_dir=a.output_dir, cli_prefix=prefix,
                          execute_read=a.execute_read, max_pages=a.max_pages, max_folders=a.max_folders,
                          max_items=a.max_items, page_size=a.page_size, timeout=a.timeout, identity=a.identity)
    except (DiscoveryError, OSError) as exc:
        print(json.dumps({"status": "NOT_RUN", "reason": exc.kind if isinstance(exc, DiscoveryError) else type(exc).__name__}))
        return 2
    print(json.dumps({"status": result["status"], "items": len(result["items"]), "pages": result["pages"],
                      "types": dict(Counter(x["type"] for x in result["items"])),
                      "external_calls": result["external_calls"]}, ensure_ascii=False))
    return 0 if result["status"] in ("READ_PLAN_ONLY", "COMPLETE") else 2


if __name__ == "__main__":
    raise SystemExit(main())
