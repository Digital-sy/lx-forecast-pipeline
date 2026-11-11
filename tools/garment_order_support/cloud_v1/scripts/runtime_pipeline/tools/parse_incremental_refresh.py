#!/usr/bin/env python3
"""Pinned, local-only incremental order workbook parser. Never downloads or writes sources."""
from __future__ import annotations
import argparse
import ast
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

SUCCESS = {"ok", "success", "downloaded"}

def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))

def read_jsonl(path):
    rows = []
    for n, line in enumerate(Path(path).read_text(encoding="utf-8-sig").splitlines(), 1):
        if line.strip():
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"JSONL record {n} is not an object")
            rows.append(value)
    return rows

def write_json(path, obj):
    Path(path).write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def token(row):
    return row.get("token") or row.get("source_token") or row.get("source_id")

def require_inside(path, root):
    p, r = Path(path).resolve(), Path(root).resolve()
    if p == r or r not in p.parents:
        raise ValueError("Output must be a child of the independent task directory")
    return p

def validate_inputs(inventory_path, downloads_path, plan_path):
    inv = read_json(inventory_path)
    items = inv if isinstance(inv, list) else inv.get("items", inv.get("files", []))
    if not items or any(x.get("type") not in ("file", "sheet") for x in items):
        raise ValueError("Increment inventory must contain only source documents")
    ids = [token(x) for x in items]
    if any(not x for x in ids) or len(set(ids)) != len(ids):
        raise ValueError("Missing or duplicate inventory source identity")
    actions = [x for x in read_jsonl(plan_path) if x.get("resource_type") in ("file", "sheet") and x.get("state") in ("NEW", "METADATA_CHANGED", "CONTENT_CHANGED", "REPARSE_REQUIRED")]
    action_ids = [token(x) for x in actions]
    if len(set(action_ids)) != len(action_ids) or set(ids) != set(action_ids):
        raise ValueError("Increment scope differs from the explicit source document plan")
    rows = read_jsonl(downloads_path)
    if any(token(x) not in set(ids) for x in rows):
        raise ValueError("Download manifest contains an out-of-scope source")
    selected = {}
    for row in rows:
        if row.get("status") not in SUCCESS:
            continue
        key = token(row)
        if key in selected:
            raise ValueError("Multiple successful versions for one source require explicit selection")
        path = Path(row.get("local_path") or "")
        expected = row.get("sha256") or row.get("source_sha256")
        if not path.is_file() or not expected or len(expected) != 64:
            raise ValueError("Successful download lacks existing original or SHA256")
        actual = sha256(path)
        if actual.lower() != expected.lower():
            raise ValueError(f"Source hash mismatch for {key}")
        if row.get("bytes") is not None and path.stat().st_size != row["bytes"]:
            raise ValueError(f"Source byte count mismatch for {key}")
        selected[key] = {"local_path": str(path.resolve()), "sha256": actual, "bytes": path.stat().st_size}
    if set(selected) != set(ids):
        raise ValueError("Incomplete acquisition: every scoped document needs one verified successful version")
    return items, selected

def resolve_dependency(manifest_path, value):
    path=Path(value)
    return (path if path.is_absolute() else Path(manifest_path).resolve().parent/path).resolve()


def validate_dependencies(manifest_path, deps):
    """Pin actual parser and helper, and statically verify their relative binding."""
    if deps.get("binding_contract") != "adjacent_helper_v2":
        raise ValueError("Expected portable adjacent_helper_v2 dependency contract")
    wrapper_path=resolve_dependency(manifest_path,deps["wrapper_path"])
    if wrapper_path!=Path(__file__).resolve() or sha256(wrapper_path)!=deps["wrapper_sha256"]:
        raise ValueError("Invoked wrapper differs from the pinned dependency manifest")
    parser_path=resolve_dependency(manifest_path,deps["parser_path"])
    helper_path=resolve_dependency(manifest_path,deps["helper_path"])
    for path,expected in ((parser_path,deps["parser_sha256"]),(helper_path,deps["helper_sha256"])):
        if sha256(path)!=expected:raise ValueError("Pinned parser dependency hash changed: "+path.name)
    bundle=Path(manifest_path).resolve().parent
    for relative, expected in deps.get("vendored_files",{}).items():
        dependency=(bundle/relative).resolve()
        if bundle not in dependency.parents or sha256(dependency)!=expected:
            raise ValueError("Vendored dependency path/hash differs from manifest")
    tree=ast.parse(parser_path.read_text(encoding="utf-8-sig"))
    assignments={t.id:n.value for n in tree.body if isinstance(n,ast.Assign) for t in n.targets if isinstance(t,ast.Name)}
    expected_root=ast.parse("Path(__file__).resolve().parent",mode="eval").body
    expected_binding=ast.parse("BUNDLE_ROOT/HELPER_FILENAME",mode="eval").body
    root=assignments.get("BUNDLE_ROOT"); filename=assignments.get("HELPER_FILENAME"); spec=assignments.get("_spec")
    if root is None or ast.dump(root)!=ast.dump(expected_root) or not isinstance(filename,ast.Constant) or filename.value!="extract_fields.py":
        raise ValueError("Parser does not declare the pinned adjacent helper")
    if not isinstance(spec,ast.Call) or len(spec.args)<2 or ast.dump(spec.args[1])!=ast.dump(expected_binding):
        raise ValueError("Parser import binding differs from declared helper")
    if helper_path!=(parser_path.parent/filename.value).resolve():
        raise ValueError("Helper is not adjacent to the actual parser")
    return parser_path,helper_path


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--task-root", type=Path, default=Path(__file__).resolve().parent.parent)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--inventory",type=Path,required=True)
    p.add_argument("--downloads",type=Path,required=True)
    p.add_argument("--plan",type=Path,required=True)
    p.add_argument("--dependency-manifest",type=Path,required=True)
    p.add_argument("--execute-local-parse",action="store_true")
    args = p.parse_args(argv)
    root = args.task_root.resolve()
    out = require_inside(args.output_dir, root)
    if out.exists():
        raise FileExistsError("Refusing to overwrite existing parse output")
    inventory,downloads,plan_path=args.inventory.resolve(),args.downloads.resolve(),args.plan.resolve()
    manifest_path=args.dependency_manifest.resolve()
    deps=read_json(manifest_path)
    parser_path,helper_path=validate_dependencies(manifest_path,deps)
    items,sources=validate_inputs(inventory,downloads,plan_path)
    protected={str(p):sha256(p) for p in (inventory,downloads,plan_path,manifest_path,Path(__file__),parser_path,helper_path)}
    protected.update({str(resolve_dependency(manifest_path,rel)):expected for rel,expected in deps.get("vendored_files",{}).items()})
    if not args.execute_local_parse:
        out.mkdir(parents=True,exist_ok=False)
        write_json(out/"parse_manifest.json",{"status":"PREFLIGHT_ONLY_NOT_PARSED","sources":sources,"protected":protected,"network_calls":0,"database_calls":0,"parser_executions":0})
        print(json.dumps({"status":"PREFLIGHT_ONLY_NOT_PARSED","files":len(items)},ensure_ascii=False));return 0
    out.mkdir(parents=True, exist_ok=False)
    started = datetime.now(timezone.utc).isoformat()
    cmd = [sys.executable, "-B", str(parser_path), "--inventory", str(inventory), "--downloads", str(downloads), "--output-lines", str(out / "order_lines.jsonl"), "--output-audit", str(out / "order_file_audit.jsonl")]
    receipt = {"status": "RUNNING", "started_at": started, "scope_count": len(items), "parser_version": deps["parser_version"], "dependency_manifest_sha256": sha256(manifest_path), "wrapper_sha256": sha256(Path(__file__)), "source_manifest": sources, "parser_path": str(parser_path), "parser_sha256": deps["parser_sha256"], "helper_path": str(helper_path), "helper_sha256": deps["helper_sha256"], "inventory_sha256": sha256(inventory), "downloads_sha256": sha256(downloads), "plan_sha256": sha256(plan_path), "command": cmd, "network_calls": 0, "database_calls": 0, "scope": "INCREMENT_ONLY", "limitations": ["Formula caches are read; formulas are not recalculated", "Images are not interpreted", "Cross-document duplicate review covers only this increment", "SPU, color-code and filename-date fields are parsed source candidates, not confirmed business mappings", "Market derived from folder is not a precise shop", "Existing manual confirmations are not read or changed", "Portable parser uses an adjacent helper pinned by manifest and statically verified binding; legacy sources remain unchanged"]}
    write_json(out / "parse_manifest.json", receipt)
    try:
        run = subprocess.run(cmd, cwd=root, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=900, check=False)
        (out / "parser_stdout.txt").write_text(run.stdout, encoding="utf-8")
        (out / "parser_stderr.txt").write_text(run.stderr, encoding="utf-8")
        receipt["returncode"] = run.returncode
        if run.returncode != 0:
            raise RuntimeError("Pinned parser did not complete successfully; see local stderr")
        audits = read_jsonl(out / "order_file_audit.jsonl")
        lines = read_jsonl(out / "order_lines.jsonl")
        if len(audits) != len(items) or {token(x) for x in audits} != set(sources):
            raise ValueError("Audit source coverage differs from fixed input")
        if len({x["line_id"] for x in lines}) != len(lines):
            raise ValueError("Increment contains duplicate parser line IDs")
        for entry in [*audits, *lines]:
            src = sources[token(entry)]
            if entry.get("source_sha256") != src["sha256"]:
                raise ValueError("Parsed evidence has missing or conflicting source hash")
        for path,h in protected.items():
            if sha256(path)!=h:raise ValueError("Pinned input or dependency changed during parse")
        changed = [k for k, v in sources.items() if sha256(v["local_path"]) != v["sha256"]]
        if changed:
            raise ValueError("Original source changed during read")
        status_counts = dict(Counter(x.get("status", "UNKNOWN") for x in audits))
        read_fail = [x for x in audits if x.get("status") not in ("ok", "parsed_with_review", "no_order_lines_review")]
        receipt.update(status="FAILED" if read_fail else "PARSED_WITH_REVIEW" if any(x.get("status") != "ok" for x in audits) else "PARSED", file_status_counts=status_counts, line_count=len(lines), source_originals_unchanged=True, output_sha256={n: sha256(out / n) for n in ("order_lines.jsonl", "order_file_audit.jsonl")})
        if read_fail:
            raise ValueError("One or more source files could not be parsed")
    except Exception as exc:
        receipt.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(out / "parse_manifest.json", receipt)
    print(json.dumps({"status": receipt["status"], "files": len(items), "lines": receipt["line_count"], "output_dir": str(out)}, ensure_ascii=False))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
