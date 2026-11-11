"""Synthetic-only tests: no real HTTP, credentials or database."""
import contextlib, hashlib, importlib.util, json, os, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import cloud_read_cli as m

class Response:
    def __init__(self, payload=None, *, status=200, headers=None, chunks=None):
        self.status_code = status
        self.headers = headers or {}
        self.chunks = chunks if chunks is not None else [json.dumps(payload).encode()]
    def iter_bytes(self):
        yield from self.chunks

def api(data):
    return Response({"code": 0, "data": data})

class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
    @contextlib.contextmanager
    def stream(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise RuntimeError("No synthetic fixture")
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        yield value

class CloudReadTests(unittest.TestCase):
    def setUp(self):
        tmp = ROOT / "private" / "cloud_adapter_tests"
        tmp.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=tmp)
        self.path = Path(self.temp.name)
        self.provider_calls = 0
        self.scope = {
            "schema_version": "garment_cloud_read/1",
            "api_base": m.API_BASE, "allowed_hosts": ["example.feishu.cn"],
            "root_tokens": ["ROOT"], "folder_tokens": ["ROOT"],
            "file_tokens": [], "output_root": str(self.path), "max_requests": 20}
    def tearDown(self):
        self.temp.cleanup()
    def provider(self):
        self.provider_calls += 1
        return "SYNTHETIC_TEST_TOKEN"
    def client(self, responses, **changes):
        return m.CloudReadClient({**self.scope, **changes}, self.provider, FakeTransport(responses))
    def listing(self, rows=None):
        return api({"files": rows or [], "has_more": False})
    def file_row(self, token="FILE"):
        return {"token": token, "name": "test.xlsx", "type": "file",
            "url": "https://example.feishu.cn/file/" + token}
    def bind(self, c, rows=None):
        rows = rows or [self.file_row()]
        c.files_list({"folder_token": "ROOT", "page_size": 1})
        path = self.path / "inventory.json"
        path.write_text(json.dumps({"root_token": "ROOT", "complete": True, "status": "COMPLETE",
            "errors": [], "pending_folders": [], "items": [{**r, "parent_token": "ROOT"} for r in rows]}))
        c.bind_inventory(path, m.sha(path))
        return path
    def reason(self, code, callable_, *args, **kwargs):
        with self.assertRaises(m.ReadError) as caught:
            callable_(*args, **kwargs)
        self.assertEqual(caught.exception.reason, code)
    def test_list_envelope_and_one_auth_per_client(self):
        c=self.client([self.listing(), self.listing()])
        args=["drive","files","list","--params",'{"folder_token":"ROOT"}',"--as","bot","--format","json"]
        self.assertTrue(c.handle_cli(args)["ok"])
        self.assertTrue(c.handle_cli(args)["ok"])
        self.assertEqual(self.provider_calls,1)
        self.assertEqual(c.requests,2)
    def test_bot_required_no_auth(self):
        c=self.client([])
        result=c.handle_cli(["drive","files","list","--params","{}","--as","user"])
        self.assertFalse(result["ok"]);self.assertEqual(self.provider_calls,0)
    def test_operation_rejected(self):
        c=self.client([])
        self.assertFalse(c.handle_cli(["drive","files","delete","--as","bot"])["ok"])
        self.assertEqual(c.requests,0)
    def test_duplicate_arg_rejected(self):
        c=self.client([])
        self.assertFalse(c.handle_cli(["drive","+inspect","--url","x","--as","bot","--as","bot"])["ok"])
    def test_unknown_folder_no_request(self):
        c=self.client([])
        self.reason("FOLDER_NOT_AUTHORIZED",c.files_list,{"folder_token":"OTHER"})
        self.assertEqual(self.provider_calls,0)
    def test_new_folder_only_from_successful_parent(self):
        c=self.client([self.listing([{"token":"SUB","name":"sub","type":"folder"}]),self.listing()])
        c.files_list({"folder_token":"ROOT"})
        c.files_list({"folder_token":"SUB"})
        self.assertEqual(c.requests,2)
    def test_malformed_page_does_not_extend_scope(self):
        c=self.client([self.listing([{"token":"SUB","name":"sub","type":"folder"},
            {"token":"BAD","name":"bad","type":"file","url":"https://evil.test/file/BAD"}])])
        self.reason("ITEM_URL_INVALID",c.files_list,{"folder_token":"ROOT"})
        self.assertNotIn("SUB",c.folders)
    def test_shortcut_not_followed(self):
        c=self.client([self.listing([{"token":"SHORT","name":"shortcut","type":"shortcut",
            "shortcut_info":{"target_token":"OTHER","target_type":"folder"}}])])
        c.files_list({"folder_token":"ROOT"})
        self.reason("FOLDER_NOT_AUTHORIZED",c.files_list,{"folder_token":"OTHER"})
    def test_permission_error_no_fallback(self):
        c=self.client([Response(status=403),self.listing()])
        self.reason("ACCESS_DENIED",c.files_list,{"folder_token":"ROOT"})
        self.assertEqual(len(c.transport.calls),1)
    def test_redirect_refused(self):
        c=self.client([Response(status=302,headers={"location":"https://evil.test"})])
        self.reason("REDIRECT_REFUSED",c.files_list,{"folder_token":"ROOT"})
        self.assertEqual(len(c.transport.calls),1)
    def test_api_error_drops_raw_message(self):
        c=self.client([Response({"code":1061004,"msg":"SECRET_STRING"})])
        result=c.handle_cli(["drive","files","list","--params",'{"folder_token":"ROOT"}',"--as","bot"])
        self.assertEqual(result["error"]["code"],1061004)
        self.assertNotIn("SECRET",json.dumps(result))
    def test_transport_exception_redacted(self):
        c=self.client([RuntimeError("Authorization: SECRET")])
        result=c.handle_cli(["drive","files","list","--params",'{"folder_token":"ROOT"}',"--as","bot"])
        self.assertNotIn("SECRET",json.dumps(result))
    def test_budget_no_extra_auth_or_call(self):
        c=self.client([self.listing()],max_requests=1)
        c.files_list({"folder_token":"ROOT"})
        self.reason("REQUEST_BUDGET_EXHAUSTED",c.files_list,{"folder_token":"ROOT"})
        self.assertEqual(c.requests,1)
    def test_default_files_forbidden(self):
        c=self.client([])
        self.reason("FILE_NOT_AUTHORIZED",c.inspect,"https://example.feishu.cn/file/FILE")
        self.reason("FILE_NOT_AUTHORIZED",c.download,"FILE","source")
        self.assertEqual(self.provider_calls,0)
    def test_inventory_requires_real_observation_and_complete(self):
        c=self.client([])
        p=self.path/"inventory.json"
        obj={"complete":True,"status":"COMPLETE","root_token":"ROOT","items":[{**self.file_row(),"parent_token":"ROOT"}]}
        p.write_text(json.dumps(obj))
        self.reason("INVENTORY_NOT_OBSERVED",c.bind_inventory,p,m.sha(p))
        obj["complete"]=False;p.write_text(json.dumps(obj))
        self.reason("INVENTORY_INCOMPLETE",c.bind_inventory,p,m.sha(p))
    def test_inventory_tamper_invalidates(self):
        c=self.client([self.listing([self.file_row()])])
        p=self.bind(c);old=m.sha(p);p.write_text("{}")
        self.reason("INVENTORY_PIN_CHANGED",c.bind_inventory,p,old)
    def test_inspect_contract(self):
        c=self.client([self.listing([self.file_row()]),api({"metas":[{"doc_token":"FILE","doc_type":"file","title":"test.xlsx"}]})])
        self.bind(c)
        d=c.inspect("https://example.feishu.cn/file/FILE")
        self.assertEqual(d["title"],"test.xlsx")
        self.assertEqual(c.transport.calls[1][2]["json"],{"request_docs":[{"doc_token":"FILE","doc_type":"file"}]})
    def test_inspect_wrong_identity_rejected(self):
        c=self.client([self.listing([self.file_row()]),api({"metas":[{"doc_token":"OTHER","doc_type":"file","title":"test.xlsx"}]})])
        self.bind(c)
        self.reason("METADATA_IDENTITY_INVALID",c.inspect,"https://example.feishu.cn/file/FILE")
    def test_url_domain_and_signed_query_rejected(self):
        c=self.client([])
        for u in ["https://example.feishu.cn.evil.test/file/FILE",
            "https://user:SECRET@example.feishu.cn/file/FILE",
            "https://example.feishu.cn/file/FILE?signature=secret","http://example.feishu.cn/file/FILE"]:
            self.reason("FILE_URL_INVALID",c.inspect,u)
        self.assertEqual(c.requests,0)
    def test_binary_download_and_hash(self):
        c=self.client([self.listing([self.file_row()]),Response(chunks=[b"abc",b"def"],headers={"content-length":"6"})])
        self.bind(c)
        d=c.download("FILE","source")
        self.assertEqual(Path(d["output_path"]).read_bytes(),b"abcdef")
        self.assertEqual(d["sha256"],hashlib.sha256(b"abcdef").hexdigest())
    def test_declared_size_limit(self):
        c=self.client([self.listing([self.file_row()]),Response(chunks=[b"x"],headers={"content-length":"101"})],max_file_bytes=100)
        self.bind(c);self.reason("RESPONSE_TOO_LARGE",c.download,"FILE","source")
        self.assertFalse((self.path/"source").exists())
    def test_stream_limit_no_partial_destination(self):
        c=self.client([self.listing([self.file_row()]),Response(chunks=[b"123",b"456"])],max_file_bytes=5)
        self.bind(c);self.reason("RESPONSE_TOO_LARGE",c.download,"FILE","source")
        self.assertFalse((self.path/"source").exists())
        self.assertEqual(list(self.path.glob("*.partial")),[])
    def test_truncated_download_blocked(self):
        c=self.client([self.listing([self.file_row()]),Response(chunks=[b"123"],headers={"content-length":"4"})])
        self.bind(c);self.reason("CONTENT_LENGTH_MISMATCH",c.download,"FILE","source")
        self.assertFalse((self.path/"source").exists())
    def test_json_error_not_written_as_file(self):
        c=self.client([self.listing([self.file_row()]),Response(chunks=[b'{"code":1}'],headers={"content-type":"application/json"})])
        self.bind(c);self.reason("DOWNLOAD_NOT_BINARY",c.download,"FILE","source")
    def test_existing_file_preserved(self):
        c=self.client([self.listing([self.file_row()])]);self.bind(c)
        p=self.path/"source";p.write_bytes(b"old")
        self.reason("OUTPUT_EXISTS",c.download,"FILE","source")
        self.assertEqual(p.read_bytes(),b"old");self.assertEqual(c.requests,1)
    def test_output_escape_blocked(self):
        c=self.client([self.listing([self.file_row()])]);self.bind(c)
        for dest in ("../outside","C:/outside","/outside","..\\outside"):
            with self.assertRaises(m.ReadError):c.download("FILE",dest)
        self.assertEqual(c.requests,1)
    def test_symlink_escape_blocked(self):
        c=self.client([self.listing([self.file_row()])]);self.bind(c)
        try:(self.path/"link").symlink_to(self.path,target_is_directory=True)
        except OSError:self.skipTest("OS does not permit symlinks for this test user")
        self.reason("OUTPUT_SYMLINK_REFUSED",c.download,"FILE","link/source")
    def test_no_arbitrary_http_operations(self):
        c=self.client([])
        with self.assertRaises(m.ReadError):
            with c._request("DELETE","/drive/v1/files/FILE"):pass
        self.assertEqual(c.requests,0)
    def test_scope_cannot_preapprove_arbitrary_files(self):
        with self.assertRaises(m.ReadError):self.client([],file_tokens=["FILE"])
        with self.assertRaises(m.ReadError):self.client([],folder_tokens=["ROOT","OTHER"])
    def test_auth_pin_mismatch_blocks_before_config_read(self):
        with patch.object(m,"CONFIG_PATH",self.path/"missing"):
            self.reason("AUTH_SOURCE_PIN_CHANGED",m.fixed_server_token_provider())
    def test_pins_match_saved_probe(self):
        probe=json.loads((ROOT/"verification"/"cloud_feishu_probe.json").read_text(encoding="utf-8-sig"))
        self.assertEqual(m.PINNED["config"],probe["config_sha256"])
        self.assertEqual(m.PINNED["client"],probe["client_sha256"])
    def test_import_no_auth_network_or_database(self):
        path=ROOT/"scripts"/"cloud_read_cli.py"
        with patch("socket.socket",side_effect=AssertionError("No network")):
            spec=importlib.util.spec_from_file_location("isolated_adapter_import",path)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        self.assertTrue(hasattr(module,"CloudReadClient"))
    def test_folder_url_credentials_not_forwarded(self):
        c=self.client([self.listing([{"token":"SUB","name":"sub","type":"folder",
            "url":"https://example.feishu.cn/drive/folder/SUB?token=SECRET"}])])
        self.reason("ITEM_URL_INVALID",c.files_list,{"folder_token":"ROOT"})
        self.assertNotIn("SUB",c.folders)
    def test_tuple_cli_arguments(self):
        c=self.client([self.listing()])
        self.assertTrue(c.handle_cli(("drive","files","list","--params",'{"folder_token":"ROOT"}',"--as","bot"))["ok"])
    def test_standalone_rejects_without_auth(self):
        import io
        with contextlib.redirect_stderr(io.StringIO()) as out:
            result=m.main([])
        self.assertEqual(result,2);self.assertNotIn("TOKEN",out.getvalue())

if __name__=="__main__":unittest.main(verbosity=2)

