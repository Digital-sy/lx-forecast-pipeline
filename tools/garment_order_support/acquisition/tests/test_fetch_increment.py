import copy, importlib.util, io, json, os, tempfile, unittest, zipfile
from contextlib import redirect_stdout
from pathlib import Path

S = Path(__file__).resolve().parents[1] / "scripts" / "fetch_increment.py"
spec = importlib.util.spec_from_file_location("fetch_increment", S)
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)

def row(token="Test1", name="order.xlsx"):
    return {"source_id":token,"token":token,"resource_type":"file","action":"FETCH_REQUIRED","state":"NEW","current_metadata":{"name":name,"type":"file","url":"https://example.feishu.cn/file/"+token,"parent_path":"/test","modified_time":"123"}}

class Fake:
    timeout = 5
    def __init__(self, root, *, failure=None, title="order.xlsx", escaped=False, invalid=False):
        self.root,self.failure,self.title,self.escaped,self.invalid=root,failure,title,escaped,invalid
        self.calls=[]
    def __call__(self,args):
        self.calls.append(args)
        if self.failure:
            raise self.failure
        if args[1] == "+inspect":
            return {"token":args[3].split("/")[-1],"type":"file","title":self.title,"token_secret":"DO_NOT_SAVE"}
        p = self.root / args[args.index("--output")+1]
        p = p.with_suffix(".xlsx")
        if self.escaped: p=self.root / "escaped.xlsx"
        p.parent.mkdir(parents=True,exist_ok=True)
        if self.invalid: p.write_bytes(b"not a workbook")
        else:
            with zipfile.ZipFile(p,"w") as z:
                z.writestr("[Content_Types].xml","x")
                z.writestr("xl/workbook.xml","x")
        return {"output_path":str(p),"selected_type":"source_file","file_token":args[3],"secret":"DO_NOT_SAVE"}

class FetchTests(unittest.TestCase):
    def setUp(self):
        base=Path(os.environ["FETCH_INCREMENT_TEST_ROOT"]);base.mkdir(parents=True,exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=base); self.root=Path(self.tmp.name)
    def tearDown(self): self.tmp.cleanup()
    def run_fetch(self, rows=None, tr=None, read=True):
        with redirect_stdout(io.StringIO()):
            return f.execute(rows or [row()], self.root/"private",self.root,tr or Fake(self.root),execute_read=read)
    def test_success_hash_and_compatible_log(self):
        t=Fake(self.root);r=self.run_fetch(tr=t)
        self.assertEqual(r["downloaded"],1);self.assertEqual(r["cli_invocations"],2)
        v=json.loads((self.root/"private/downloads.jsonl").read_text(encoding="utf-8"))
        self.assertEqual(v["status"],"ok");self.assertEqual(v["sha256"],f.sha(v["local_path"]))
        self.assertIsNone(v["business_date"])
        self.assertNotIn("DO_NOT_SAVE",(self.root/"private/downloads.jsonl").read_text(encoding="utf-8"))
    def test_default_plan_does_not_call_transport(self):
        t=Fake(self.root);r=self.run_fetch(tr=t,read=False)
        self.assertEqual(r["status"],"READ_PLAN_ONLY");self.assertEqual(t.calls,[])
    def test_no_existing_output_reuse(self):
        self.run_fetch(read=False)
        with self.assertRaisesRegex(f.FetchError,"OUTPUT_EXISTS"): self.run_fetch()
    def test_duplicate_id_rejected_before_call(self):
        t=Fake(self.root)
        with self.assertRaises(f.FetchError): self.run_fetch([row(),row()],t)
        self.assertFalse(t.calls)
    def test_scope_limit(self):
        with self.assertRaisesRegex(f.FetchError,"SIZE"): self.run_fetch([row("T"+str(n)) for n in range(16)])
    def test_url_token_mismatch(self):
        x=row();x["current_metadata"]["url"]="https://example.feishu.cn/file/Other"
        with self.assertRaisesRegex(f.FetchError,"URL"): self.run_fetch([x])
    def test_authenticated_url_rejected(self):
        x=row();x["current_metadata"]["url"]="https://secret@example.feishu.cn/file/Test1"
        with self.assertRaises(f.FetchError): self.run_fetch([x])
    def test_query_url_rejected(self):
        x=row();x["current_metadata"]["url"]+="?token=secret"
        with self.assertRaises(f.FetchError): self.run_fetch([x])
    def test_sheet_resource_rejected(self):
        x=row();x["resource_type"]="sheet"
        with self.assertRaisesRegex(f.FetchError,"SCOPE"): self.run_fetch([x])
    def test_title_change_stops_file_before_preview(self):
        t=Fake(self.root,title="changed.xlsx");r=self.run_fetch(tr=t)
        self.assertEqual(r["failed"],1);self.assertEqual(len(t.calls),1)
    def test_authorization_stops_remaining(self):
        t=Fake(self.root,failure=f.FetchError("CLI_READ_FAILED",auth=True,error_type="authorization"))
        r=self.run_fetch([row("T1"),row("T2")],t)
        self.assertEqual(r["failed"],1);self.assertEqual(r["not_run"],1);self.assertEqual(len(t.calls),1)
    def test_timeout_no_retry(self):
        t=Fake(self.root,failure=f.FetchError("CLI_TIMEOUT"));r=self.run_fetch(tr=t)
        self.assertEqual(r["failed"],1);self.assertEqual(len(t.calls),1)
    def test_failed_download_not_inventory_success(self):
        r=self.run_fetch(tr=Fake(self.root,invalid=True))
        self.assertEqual(r["downloaded"],0)
        inv=json.loads((self.root/"private/inventory_increment.json").read_text())
        self.assertEqual(inv["items"],[]);self.assertFalse(inv["complete"])
    def test_download_path_escape_is_blocked(self):
        r=self.run_fetch(tr=Fake(self.root,escaped=True))
        self.assertEqual(r["failed"],1)
    def test_ole_header_check(self):
        p=self.root/"source.xls";p.write_bytes(bytes.fromhex("d0cf11e0a1b11ae1")+b"test")
        self.assertEqual(f.check_file(p,"a.xls")["format"],"xls")
        with self.assertRaisesRegex(f.FetchError,"XLSX"):f.check_file(p,"a.xlsx")
    def test_success_inventory_no_join_amplification(self):
        r=self.run_fetch([row("T1"),row("T2")])
        inv=json.loads((self.root/"private/inventory_increment.json").read_text())
        self.assertEqual(len(inv["items"]),2)
        self.assertEqual(len({v["token"] for v in inv["items"]}),2)
    def test_output_must_be_in_task(self):
        with self.assertRaisesRegex(f.FetchError,"OUTSIDE_TASK"):
            f.execute([row()],self.root.parent/"outside",self.root,Fake(self.root))
    def test_source_file_not_overwritten(self):
        self.run_fetch()
        files=list((self.root/"private/downloads").rglob("source.xlsx"));old=f.sha(files[0])
        with self.assertRaises(f.FetchError): self.run_fetch()
        self.assertEqual(old,f.sha(files[0]))

if __name__ == "__main__": unittest.main(verbosity=2)
