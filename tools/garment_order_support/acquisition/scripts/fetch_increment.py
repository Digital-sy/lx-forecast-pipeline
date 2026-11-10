"""Bounded, read-only Feishu source fetches. Never applies business mappings or DB writes."""
from __future__ import annotations
import argparse, hashlib, json, os, re, subprocess, sys, uuid, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

VERSION = "1.0.0"
MAX_FILES = 15
MAX_CALLS = 30
METADATA_FIELDS = ("name", "type", "url", "created_time", "modified_time", "parent_token", "parent_path", "path", "revision", "version", "etag")
LIMITATION = "Directory metadata is an earlier observation. Inspect validates identity/title only; content is downloaded at acquired_at. No atomic remote metadata/content snapshot is claimed."

def now():
    return datetime.now(timezone.utc).isoformat()

def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")

class FetchError(Exception):
    def __init__(self, reason, *, auth=False, code=None, error_type=None):
        super().__init__(reason)
        self.reason, self.auth, self.code, self.error_type = reason, auth, code, error_type
    def safe(self):
        return {"reason": self.reason, "code": self.code, "type": self.error_type}

def validate_allowlist(rows):
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_FILES:
        raise FetchError("ALLOWLIST_SIZE_INVALID")
    seen = set()
    result = []
    for row in rows:
        token = row.get("source_id")
        md = row.get("current_metadata", {})
        if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9]+", token) or token in seen:
            raise FetchError("DUPLICATE_OR_INVALID_SOURCE_ID")
        if row.get("token") != token or row.get("resource_type") != "file" or md.get("type") != "file" or row.get("action") != "FETCH_REQUIRED":
            raise FetchError("ALLOWLIST_SCOPE_INVALID")
        if row.get("state") not in ("NEW", "METADATA_CHANGED", "CONTENT_CHANGED"):
            raise FetchError("ALLOWLIST_STATE_INVALID")
        u = urlsplit(md.get("url", ""))
        if (u.scheme != "https" or not u.hostname or not (u.hostname == "feishu.cn" or u.hostname.endswith(".feishu.cn") or u.hostname == "larksuite.com" or u.hostname.endswith(".larksuite.com"))
            or u.username or u.password or u.query or u.fragment or u.port or u.path.rstrip("/") not in ("/file/" + token, "/drive/file/" + token)):
            raise FetchError("SOURCE_URL_INVALID")
        if Path(md.get("name", "")).suffix.lower() not in (".xlsx", ".xlsm", ".xls"):
            raise FetchError("SOURCE_NOT_SUPPORTED_SPREADSHEET")
        seen.add(token)
        result.append({"token": token, "state": row["state"], "metadata": {k: md.get(k) for k in METADATA_FIELDS}})
    return result

class CliTransport:
    def __init__(self, executable, script, cwd, timeout=120):
        self.executable, self.script, self.cwd, self.timeout = str(executable), str(script), str(cwd), timeout
    def __call__(self, args):
        env = os.environ.copy()
        env.update(LARKSUITE_CLI_NO_UPDATE_NOTIFIER="1", LARKSUITE_CLI_NO_SKILLS_NOTIFIER="1")
        try:
            p = subprocess.run([self.executable, self.script, *args], cwd=self.cwd, env=env, shell=False, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=self.timeout)
        except subprocess.TimeoutExpired:
            raise FetchError("CLI_TIMEOUT") from None
        except OSError:
            raise FetchError("CLI_START_FAILED") from None
        try:
            obj = json.loads(p.stdout if p.returncode == 0 else p.stderr)
        except (ValueError, TypeError):
            raise FetchError("CLI_NON_JSON_RESPONSE") from None
        if p.returncode != 0 or obj.get("ok") is not True:
            err = obj.get("error", {})
            typ = err.get("type")
            # No raw message, hint, URL, header or arbitrary field is persisted.
            safe_type = typ if typ in ("authorization", "authentication", "permission", "validation", "api", "network", "rate_limit") else "other"
            code = err.get("code") if isinstance(err.get("code"), (int, float)) else None
            raise FetchError("CLI_READ_FAILED", auth=safe_type in ("authorization", "authentication", "permission"), code=code, error_type=safe_type)
        if obj.get("identity") != "user" or not isinstance(obj.get("data"), dict):
            raise FetchError("CLI_IDENTITY_OR_SHAPE_INVALID", auth=True)
        return obj["data"]

def check_file(path, declared_name, max_bytes=100 * 1024 * 1024):
    if not path.is_file() or path.is_symlink() or not 0 < path.stat().st_size <= max_bytes:
        raise FetchError("DOWNLOADED_FILE_INVALID")
    suffix = Path(declared_name).suffix.lower()
    with path.open("rb") as f:
        magic = f.read(8)
    if suffix == ".xls":
        if magic != bytes.fromhex("d0cf11e0a1b11ae1"):
            raise FetchError("XLS_SIGNATURE_MISMATCH")
    else:
        if not zipfile.is_zipfile(path):
            raise FetchError("XLSX_SIGNATURE_MISMATCH")
        with zipfile.ZipFile(path) as z:
            if "xl/workbook.xml" not in z.namelist() or "[Content_Types].xml" not in z.namelist():
                raise FetchError("XLSX_WORKBOOK_PART_MISSING")
    return {"sha256": sha(path), "bytes": path.stat().st_size, "format": suffix.lstrip(".")}

def execute(rows, output_dir, cwd, transport, *, execute_read=False, allowlist_sha256=None):
    items = validate_allowlist(rows)
    output_dir, cwd = Path(output_dir).resolve(), Path(cwd).resolve()
    if not output_dir.is_relative_to(cwd):
        raise FetchError("OUTPUT_OUTSIDE_TASK")
    reserved = [output_dir / name for name in ("downloads.jsonl", "download_manifest.json", "inventory_increment.json", "fetch_plan.json")]
    if any(p.exists() for p in reserved):
        raise FetchError("OUTPUT_EXISTS")
    output_dir.mkdir(parents=True, exist_ok=True)
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid.uuid4().hex[:8]
    plan = {"script_version": VERSION, "created_at": now(), "source_count": len(items), "max_cli_invocations": MAX_CALLS, "automatic_retries": 0, "timeout_seconds": getattr(transport, "timeout", None), "allowlist_sha256": allowlist_sha256, "execute_read": execute_read, "version": version, "snapshot_limitations": LIMITATION, "scope": items}
    write_json(output_dir / "fetch_plan.json", plan)
    if not execute_read:
        return {"status": "READ_PLAN_ONLY", "source_count": len(items), "cli_invocations": 0}
    calls, records, successes, stopped = 0, [], [], False
    with (output_dir / "downloads.jsonl").open("x", encoding="utf-8") as journal:
        for item in items:
            md, token = item["metadata"], item["token"]
            rec = {**md, "token": token, "file_name": md["name"], "source_url": md["url"], "refresh_state": item["state"], "fetch_version": version, "started_at": now(), "business_date": None, "snapshot_limitations": LIMITATION}
            if stopped:
                rec.update(status="not_run", error={"reason": "STOPPED_AFTER_AUTH_FAILURE"})
            else:
                directory = output_dir / "downloads" / token / version
                try:
                    directory.mkdir(parents=True, exist_ok=False)
                    if calls >= MAX_CALLS:
                        raise FetchError("CALL_BUDGET_EXHAUSTED")
                    calls += 1
                    meta = transport(["drive", "+inspect", "--url", md["url"], "--as", "user"])
                    if meta.get("token") != token or meta.get("type") != "file" or meta.get("title") != md["name"]:
                        raise FetchError("IDENTITY_OR_TITLE_CHANGED")
                    rec["inspect"] = {"token": token, "type": "file", "title": meta["title"], "checked_at": now()}
                    out_rel = (directory / "source").relative_to(cwd).as_posix()
                    if calls >= MAX_CALLS:
                        raise FetchError("CALL_BUDGET_EXHAUSTED")
                    calls += 1
                    data = transport(["drive", "+preview", "--file-token", token, "--type", "source_file", "--output", out_rel, "--if-exists", "error", "--as", "user"])
                    if data.get("file_token", token) != token or data.get("selected_type", "source_file") != "source_file":
                        raise FetchError("DOWNLOAD_IDENTITY_MISMATCH")
                    returned = data.get("output_path")
                    if not isinstance(returned, str):
                        raise FetchError("DOWNLOAD_PATH_MISSING")
                    path = Path(returned)
                    path = (cwd / path).resolve() if not path.is_absolute() else path.resolve()
                    if not path.is_relative_to(directory.resolve()):
                        raise FetchError("DOWNLOAD_PATH_OUTSIDE_VERSION")
                    info = check_file(path, md["name"])
                    rec.update(status="ok", local_path=str(path), acquired_at=now(), **info)
                    successes.append({**md, "token": token, "content_sha256": info["sha256"], "acquired_at": rec["acquired_at"]})
                except FetchError as exc:
                    rec.update(status="failed", error=exc.safe())
                    stopped = exc.auth
                except Exception as exc:
                    rec.update(status="failed", error={"reason": "LOCAL_PROCESSING_ERROR", "exception_type": type(exc).__name__})
            rec["finished_at"] = now()
            records.append(rec)
            journal.write(json.dumps(rec, ensure_ascii=False) + "\n")
            journal.flush()
            os.fsync(journal.fileno())
            print(json.dumps({"source_id": token, "status": rec["status"], "completed": len(records), "total": len(items)}, ensure_ascii=False), flush=True)
    inv = {"items": successes, "status": "FETCHED_INCREMENT_ONLY", "complete": len(successes) == len(items), "scope": "EXPLICIT_ALLOWLIST_ONLY_NOT_FULL_DIRECTORY", "acquired_at": now(), "allowlist_sha256": allowlist_sha256}
    write_json(output_dir / "inventory_increment.json", inv)
    result = {"status": "COMPLETE" if len(successes) == len(items) else "PARTIAL", "script_version": VERSION, "finished_at": now(), "fetch_version": version, "source_count": len(items), "downloaded": len(successes), "failed": sum(r["status"] == "failed" for r in records), "not_run": sum(r["status"] == "not_run" for r in records), "cli_invocations": calls, "cli_internal_http_requests": "NOT_INSTRUMENTED", "automatic_retries": 0, "remote_writes": 0, "database_operations": 0, "allowlist_sha256": allowlist_sha256, "downloads_sha256": sha(output_dir / "downloads.jsonl"), "inventory_sha256": sha(output_dir / "inventory_increment.json"), "snapshot_limitations": LIMITATION}
    write_json(output_dir / "download_manifest.json", result)
    return result

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--allowlist", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--cli-executable", required=True)
    p.add_argument("--cli-script", required=True)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--execute-read", action="store_true")
    a = p.parse_args()
    if not 1 <= a.timeout <= 180:
        p.error("timeout must be 1..180 seconds")
    source = Path(a.allowlist)
    rows = json.loads(source.read_text(encoding="utf-8-sig"))
    transport = CliTransport(a.cli_executable, a.cli_script, Path.cwd(), a.timeout)
    result = execute(rows, a.output_dir, Path.cwd(), transport, execute_read=a.execute_read, allowlist_sha256=sha(source))
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] in ("COMPLETE", "READ_PLAN_ONLY") else 2

if __name__ == "__main__":
    raise SystemExit(main())
