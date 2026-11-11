"""Strict read-only Feishu Drive adapter; no import-time auth, DB or network.
Injected into the approved source runner. Source failures never switch identity,
endpoint or transport. Token state is process-local and never persisted.
"""
from __future__ import annotations
import argparse, ast, asyncio, hashlib, json, os, re, sys, uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace
from urllib.parse import urlsplit

VERSION = "1.0.0"
API_BASE = "https://open.feishu.cn/open-apis"
MAX_FILE_BYTES = 100 * 1024 * 1024
MAX_JSON_BYTES = 4 * 1024 * 1024
TOKEN_RE = re.compile(r"[A-Za-z0-9_-]+\Z")
CONFIG_PATH = Path("/opt/apps/pythondata/common/config.py")
CLIENT_PATH = Path("/opt/apps/pythondata/common/feishu.py")
ENV_PATH = Path("/opt/apps/pythondata/.env")
PINNED = {
    "config": "f5c9a00d7f75791a14a9a89bfbc9f35aaea29c5647c971cfd5e2a2e8c54ffa77",
    "client": "fdcc7fb60ed77eb76678849c28d6e8d9bbe74d18c3ae72d0591fdcda57014fbb",
}

class ReadError(Exception):
    def __init__(self, reason, *, kind="validation", api_code=None, http_status=None):
        super().__init__(reason)
        self.reason, self.kind = reason, kind
        self.api_code = api_code if type(api_code) is int else None
        self.http_status = http_status if type(http_status) is int else None
    def envelope(self):
        return {"ok": False, "identity": "bot", "error": {
            "type": self.kind, "reason": self.reason,
            "code": self.api_code, "http_status": self.http_status}}

def require(condition, reason):
    if not condition:
        raise ReadError(reason)

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def token_value(value):
    require(isinstance(value, str) and TOKEN_RE.fullmatch(value) is not None, "TOKEN_INVALID")
    return value

def fixed_server_token_provider():
    """Return one-job lazy provider using only the previously reviewed method.
    Never imports production modules, reads their globals, or writes a cache.
    Pins are checked before reading .env. One provider invocation, no adapter retry.
    Actual provider HTTP attempts remain the pinned method's implementation.
    """
    cached = []
    def acquire():
        if cached:
            return cached[0]
        for key, path in (("config", CONFIG_PATH), ("client", CLIENT_PATH)):
            require(path.is_file() and sha(path) == PINNED[key], "AUTH_SOURCE_PIN_CHANGED")
        tree = ast.parse(CLIENT_PATH.read_text(encoding="utf-8"))
        classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "FeishuClient"]
        require(len(classes) == 1, "AUTH_CLASS_INVALID")
        methods = [n for n in classes[0].body if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_access_token"]
        require(len(methods) == 1 and not methods[0].decorator_list, "AUTH_METHOD_INVALID")
        try:
            import httpx
            from dotenv import dotenv_values
            class Quiet:
                def __getattr__(self, key):
                    return lambda *args, **kwargs: None
            namespace = {"httpx": httpx, "asyncio": asyncio, "logger": Quiet()}
            module = ast.fix_missing_locations(ast.Module(body=methods, type_ignores=[]))
            exec(compile(module, str(CLIENT_PATH), "exec"), namespace)
            values = dotenv_values(ENV_PATH)
            app = SimpleNamespace(app_id=values.get("FEISHU_APP_ID"),
                app_secret=values.get("FEISHU_APP_SECRET"), api_base=API_BASE, _access_token=None)
            require(bool(app.app_id) and bool(app.app_secret), "AUTH_CONFIG_INCOMPLETE")
            result = asyncio.run(namespace["get_access_token"](app, retry_count=1))
            require(isinstance(result, str) and bool(result) and not any(ord(x) < 32 for x in result),
                "AUTH_TOKEN_UNAVAILABLE")
            cached.append(result)
            return result
        except ReadError:
            raise
        except Exception:
            raise ReadError("AUTH_PROVIDER_FAILED", kind="authentication") from None
    return acquire

def load_server_token():
    return fixed_server_token_provider()()

class HttpTransport:
    """No redirects, ambient proxies or automatic HTTP retries."""
    def __init__(self):
        import httpx
        self._client = httpx.Client(timeout=httpx.Timeout(90, connect=15),
            follow_redirects=False, trust_env=False)
    def stream(self, method, url, **kwargs):
        return self._client.stream(method, url, **kwargs)
    def close(self):
        self._client.close()

class CloudReadClient:
    def __init__(self, scope, token_provider, transport=None):
        require(scope.get("schema_version") == "garment_cloud_read/1", "SCOPE_VERSION_INVALID")
        require(scope.get("api_base", API_BASE) == API_BASE, "API_HOST_NOT_ALLOWED")
        hosts = scope.get("allowed_hosts", [])
        require(isinstance(hosts, list) and hosts and all(isinstance(h, str) and
            re.fullmatch(r"[a-z0-9-]+\.feishu\.cn", h) for h in hosts), "HOST_SCOPE_INVALID")
        self.hosts = set(hosts)
        roots = scope.get("root_tokens", [])
        require(isinstance(roots, list) and roots, "ROOT_SCOPE_REQUIRED")
        self.roots = {token_value(t) for t in roots}
        # Folder expansion is only from this client's successful listing responses.
        supplied = {token_value(t) for t in scope.get("folder_tokens", roots)}
        require(supplied == self.roots, "UNPROVEN_FOLDER_SCOPE")
        require(not scope.get("file_tokens"), "INVENTORY_BIND_REQUIRED")
        self.folders = set(self.roots)
        self.observed_files = {}
        self.files = set()
        self.inventory_hash = None
        self.output_root = Path(scope["output_root"]).resolve()
        require(self.output_root.is_dir(), "OUTPUT_ROOT_MISSING")
        self.max_requests = scope.get("max_requests", 500)
        require(type(self.max_requests) is int and 1 <= self.max_requests <= 1000, "REQUEST_BUDGET_INVALID")
        self.max_bytes = scope.get("max_file_bytes", MAX_FILE_BYTES)
        require(type(self.max_bytes) is int and 0 < self.max_bytes <= MAX_FILE_BYTES, "DOWNLOAD_BUDGET_INVALID")
        self.transport = transport
        self.provider = token_provider
        self._token = None
        self.requests = 0
        self.receipts = []

    def close(self):
        if self.transport is not None and hasattr(self.transport, "close"):
            self.transport.close()

    def _headers(self):
        if self._token is None:
            try:
                self._token = self.provider()
            except ReadError:
                raise
            except Exception:
                raise ReadError("AUTH_PROVIDER_FAILED", kind="authentication") from None
            require(isinstance(self._token, str) and bool(self._token) and
                not any(ord(x) < 32 for x in self._token), "AUTH_TOKEN_UNAVAILABLE")
        return {"Authorization": "Bearer " + self._token, "Accept-Encoding": "identity"}

    @contextmanager
    def _request(self, method, path, **kwargs):
        allowed = (method == "GET" and path == "/drive/v1/files") or (
            method == "POST" and path == "/drive/v1/metas/batch_query") or (
            method == "GET" and re.fullmatch(r"/drive/v1/files/[A-Za-z0-9_-]+/download", path))
        require(allowed, "HTTP_OPERATION_NOT_ALLOWED")
        require(self.requests < self.max_requests, "REQUEST_BUDGET_EXHAUSTED")
        headers = self._headers()
        self.requests += 1
        receipt = {"sequence": self.requests, "method": method, "path": path,
            "started_at": datetime.now(timezone.utc).isoformat(), "status": "REQUESTED"}
        self.receipts.append(receipt)
        if self.transport is None:
            self.transport = HttpTransport()
        try:
            with self.transport.stream(method, API_BASE + path, headers=headers, **kwargs) as response:
                status = response.status_code
                receipt["http_status"] = status
                if 300 <= status < 400:
                    raise ReadError("REDIRECT_REFUSED", kind="permission", http_status=status)
                if status in (401, 403):
                    raise ReadError("ACCESS_DENIED", kind="authorization", http_status=status)
                if status != 200:
                    raise ReadError("HTTP_FAILED", kind="network", http_status=status)
                encoding = response.headers.get("content-encoding", "identity").lower()
                require(encoding in ("", "identity"), "CONTENT_ENCODING_NOT_ALLOWED")
                yield response
                receipt["status"] = "SUCCESS"
        except ReadError as exc:
            receipt["status"] = "FAILED"
            receipt["error"] = exc.envelope()["error"]
            raise
        except Exception:
            receipt["status"] = "FAILED"
            receipt["error"] = {"reason": "TRANSPORT_OR_IO_FAILED"}
            raise ReadError("TRANSPORT_OR_IO_FAILED", kind="network") from None
        finally:
            receipt["finished_at"] = datetime.now(timezone.utc).isoformat()

    def _read_bytes(self, response, maximum):
        declared = response.headers.get("content-length")
        if declared is not None:
            require(str(declared).isdigit(), "CONTENT_LENGTH_INVALID")
            require(int(declared) <= maximum, "RESPONSE_TOO_LARGE")
        chunks, count = [], 0
        for chunk in response.iter_bytes():
            count += len(chunk)
            require(count <= maximum, "RESPONSE_TOO_LARGE")
            chunks.append(chunk)
        if declared is not None:
            require(count == int(declared), "CONTENT_LENGTH_MISMATCH")
        return b"".join(chunks)

    def _json(self, method, path, **kwargs):
        with self._request(method, path, **kwargs) as response:
            raw = self._read_bytes(response, MAX_JSON_BYTES)
            try:
                obj = json.loads(raw)
            except (ValueError, UnicodeError):
                raise ReadError("API_JSON_INVALID") from None
            require(isinstance(obj, dict) and type(obj.get("code")) is int, "API_ENVELOPE_INVALID")
            if obj["code"] != 0:
                raise ReadError("API_READ_FAILED", kind="api", api_code=obj["code"])
            require(isinstance(obj.get("data"), dict), "API_DATA_INVALID")
            return obj["data"]

    def _url_token(self, url):
        require(isinstance(url, str), "FILE_URL_INVALID")
        try:
            u = urlsplit(url)
            match = re.fullmatch(r"/(?:drive/)?file/([A-Za-z0-9_-]+)/?", u.path)
            require(u.scheme == "https" and u.hostname in self.hosts and not u.username and
                not u.password and not u.port and not u.query and not u.fragment and match,
                "FILE_URL_INVALID")
            return token_value(match.group(1))
        except ValueError:
            raise ReadError("FILE_URL_INVALID") from None

    def files_list(self, params):
        require(isinstance(params, dict) and set(params) <= {"folder_token", "page_size", "page_token"},
            "LIST_PARAMS_INVALID")
        folder = token_value(params.get("folder_token"))
        require(folder in self.folders, "FOLDER_NOT_AUTHORIZED")
        size = params.get("page_size", 200)
        require(type(size) is int and 1 <= size <= 200, "PAGE_SIZE_INVALID")
        page = params.get("page_token")
        if page is not None:
            require(isinstance(page, str) and 0 < len(page) <= 4096 and
                not any(ord(x) < 32 for x in page), "PAGE_TOKEN_INVALID")
        data = self._json("GET", "/drive/v1/files", params={**params, "page_size": size})
        require(type(data.get("has_more")) is bool and isinstance(data.get("files"), list),
            "LIST_RESPONSE_INVALID")
        if data["has_more"]:
            require(isinstance(data.get("next_page_token"), str) and bool(data["next_page_token"]),
                "NEXT_PAGE_TOKEN_MISSING")
        normalized, seen, folders, files = [], set(), set(), {}
        for item in data["files"]:
            require(isinstance(item, dict), "LIST_ITEM_INVALID")
            tok = token_value(item.get("token"))
            require(tok not in seen, "DUPLICATE_PAGE_TOKEN")
            seen.add(tok)
            require(isinstance(item.get("type"), str) and isinstance(item.get("name"), str),
                "LIST_ITEM_INVALID")
            require(item.get("parent_token", folder) == folder, "ITEM_PARENT_MISMATCH")
            keep = {k: item[k] for k in ("token", "name", "type", "url", "created_time", "modified_time")
                if k in item}
            if item.get("url") is not None:
                require(isinstance(item["url"], str), "ITEM_URL_INVALID")
                url = urlsplit(item["url"])
                require(url.scheme == "https" and url.hostname in self.hosts and
                    not url.username and not url.password and not url.port and
                    not url.query and not url.fragment, "ITEM_URL_INVALID")
            if item["type"] == "folder":
                folders.add(tok)
            elif item["type"] == "file":
                require(self._url_token(item.get("url")) == tok, "FILE_URL_TOKEN_MISMATCH")
                files[tok] = {"parent_token": folder, "name": item["name"], "url": item["url"]}
            # Shortcuts are retained as metadata but never followed.
            if "shortcut_info" in item:
                info = item["shortcut_info"]
                require(isinstance(info, dict) and isinstance(info.get("target_type"), str),
                    "SHORTCUT_METADATA_INVALID")
                keep["shortcut_info"] = {"target_token": token_value(info.get("target_token")),
                    "target_type": info["target_type"]}
            normalized.append(keep)
        self.folders.update(folders)
        self.observed_files.update(files)
        return {"files": normalized, "has_more": data["has_more"],
            "next_page_token": data.get("next_page_token")}

    def bind_inventory(self, path, expected_sha256):
        path = Path(path)
        require(path.is_file() and not path.is_symlink() and sha(path) == expected_sha256,
            "INVENTORY_PIN_CHANGED")
        obj = json.loads(path.read_text(encoding="utf-8-sig"))
        require(obj.get("complete") is True and obj.get("status") == "COMPLETE" and
            obj.get("root_token", obj.get("root")) in self.roots and
            not obj.get("errors") and not obj.get("pending_folders"), "INVENTORY_INCOMPLETE")
        require(isinstance(obj.get("items"), list), "INVENTORY_SHAPE_INVALID")
        files, seen = set(), set()
        for row in obj["items"]:
            tok = token_value(row.get("token"))
            require(tok not in seen, "INVENTORY_DUPLICATE_TOKEN")
            seen.add(tok)
            if row.get("type") == "file":
                observed = self.observed_files.get(tok)
                require(observed is not None and row.get("parent_token") == observed["parent_token"]
                    and row.get("name") == observed["name"] and row.get("url") == observed["url"],
                    "INVENTORY_NOT_OBSERVED")
                files.add(tok)
        self.files = files
        self.inventory_hash = expected_sha256
        return {"status": "INVENTORY_BOUND", "file_count": len(files),
            "inventory_sha256": expected_sha256}

    def inspect(self, url):
        tok = self._url_token(url)
        require(tok in self.files, "FILE_NOT_AUTHORIZED")
        data = self._json("POST", "/drive/v1/metas/batch_query",
            json={"request_docs": [{"doc_token": tok, "doc_type": "file"}]})
        require(not data.get("failed_list"), "METADATA_PARTIAL_FAILURE")
        metas = data.get("metas")
        require(isinstance(metas, list) and len(metas) == 1, "METADATA_SHAPE_INVALID")
        meta = metas[0]
        require(meta.get("doc_token") == tok and meta.get("doc_type") == "file" and
            isinstance(meta.get("title"), str), "METADATA_IDENTITY_INVALID")
        return {"input_url": url, "type": "file", "title": meta["title"], "token": tok, "url": url}

    def _target(self, output):
        require(isinstance(output, str) and bool(output) and not Path(output).is_absolute()
            and not PureWindowsPath(output).is_absolute() and "\\" not in output,
            "OUTPUT_RELATIVE_REQUIRED")
        parts = Path(output).parts
        require(".." not in parts and "." not in parts, "OUTPUT_TRAVERSAL")
        target = self.output_root.joinpath(*parts)
        require(target.resolve().is_relative_to(self.output_root), "OUTPUT_OUTSIDE_ROOT")
        walk = target
        while walk != self.output_root:
            require(not walk.is_symlink(), "OUTPUT_SYMLINK_REFUSED")
            walk = walk.parent
        require(not target.exists(), "OUTPUT_EXISTS")
        require(target.parent.is_dir(), "OUTPUT_PARENT_MISSING")
        return target

    def download(self, token, output, selected_type="source_file", if_exists="error"):
        tok = token_value(token)
        require(tok in self.files, "FILE_NOT_AUTHORIZED")
        require(selected_type == "source_file" and if_exists == "error", "DOWNLOAD_MODE_INVALID")
        target = self._target(output)
        tmp = target.with_name("." + target.name + "." + uuid.uuid4().hex + ".partial")
        count, hasher = 0, hashlib.sha256()
        try:
            with self._request("GET", "/drive/v1/files/" + tok + "/download") as response:
                ct = response.headers.get("content-type", "").lower()
                require("json" not in ct and "text/html" not in ct, "DOWNLOAD_NOT_BINARY")
                declared = response.headers.get("content-length")
                if declared is not None:
                    require(str(declared).isdigit(), "CONTENT_LENGTH_INVALID")
                    require(int(declared) <= self.max_bytes, "RESPONSE_TOO_LARGE")
                with tmp.open("xb") as handle:
                    if os.name != "nt":
                        os.chmod(tmp, 0o600)
                    for block in response.iter_bytes():
                        count += len(block)
                        require(count <= self.max_bytes, "RESPONSE_TOO_LARGE")
                        handle.write(block)
                        hasher.update(block)
                    require(count > 0, "EMPTY_DOWNLOAD")
                    if declared is not None:
                        require(count == int(declared), "CONTENT_LENGTH_MISMATCH")
                    handle.flush()
                    os.fsync(handle.fileno())
                self._target(output)
                # Same-filesystem link is atomic and refuses to replace an existing name.
                os.link(tmp, target)
                tmp.unlink()
                if os.name != "nt":
                    fd = os.open(target.parent, os.O_RDONLY)
                    try: os.fsync(fd)
                    finally: os.close(fd)
            return {"mode": "download", "file_token": tok, "selected_type": "source_file",
                "output_path": str(target), "status": "downloaded", "bytes": count,
                "sha256": hasher.hexdigest()}
        finally:
            if tmp.is_file():
                tmp.unlink()

    def handle_cli(self, argv):
        try:
            data = self._dispatch(argv)
            return {"ok": True, "identity": "bot", "data": data}
        except ReadError as exc:
            return exc.envelope()
        except Exception:
            return ReadError("LOCAL_VALIDATION_FAILED").envelope()

    def _dispatch(self, argv):
        require(isinstance(argv, (list, tuple)) and all(isinstance(x, str) for x in argv),
            "CLI_ARGUMENTS_INVALID")
        argv = list(argv)
        if argv[:3] == ["drive", "files", "list"]:
            command, rest = "list", argv[3:]
            allowed = {"--params", "--as", "--format"}
        elif argv[:2] == ["drive", "+inspect"]:
            command, rest = "inspect", argv[2:]
            allowed = {"--url", "--as", "--format"}
        elif argv[:2] == ["drive", "+preview"]:
            command, rest = "download", argv[2:]
            allowed = {"--file-token", "--type", "--output", "--if-exists", "--as", "--format"}
        else:
            raise ReadError("CLI_OPERATION_NOT_ALLOWED")
        require(len(rest) % 2 == 0, "CLI_ARGUMENTS_INVALID")
        options = {}
        for key, val in zip(rest[::2], rest[1::2]):
            require(key in allowed and key not in options, "CLI_ARGUMENTS_INVALID")
            options[key] = val
        require(options.get("--as") == "bot", "BOT_IDENTITY_REQUIRED")
        require(options.get("--format", "json") == "json", "JSON_FORMAT_REQUIRED")
        if command == "list":
            return self.files_list(json.loads(options["--params"]))
        if command == "inspect":
            return self.inspect(options["--url"])
        return self.download(options["--file-token"], options["--output"],
            options.get("--type"), options.get("--if-exists"))

def main(argv=None):
    # Standalone mode intentionally has no automatic auth/config resolution.
    # Approved job uses CloudReadClient directly and binds current inventory.
    envelope = ReadError("INPROCESS_APPROVED_JOB_REQUIRED").envelope()
    print(json.dumps(envelope), file=sys.stderr)
    return 2

if __name__ == "__main__":
    raise SystemExit(main())

