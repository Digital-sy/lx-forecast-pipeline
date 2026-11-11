"""Daily source orchestrator. Explicit execution; append-only publisher; sticky ambiguity.
No scheduler is installed by this module. Run one cycle and exit.
"""
from __future__ import annotations
import argparse, contextlib, hashlib, importlib, importlib.util, io, json, os, sys, uuid
from types import SimpleNamespace
from datetime import datetime, timezone
from pathlib import Path

VERSION='1.0.0'
SUCCESS={'COMMITTED_VERIFIED','ALREADY_MATCHED_VERIFIED'}

def now(): return datetime.now(timezone.utc).isoformat()
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p): return json.loads(Path(p).read_text(encoding='utf-8-sig'))
class JobContractError(ValueError): pass
def require(ok, code):
    if not ok: raise JobContractError(code)
def write(p, obj, replace=False):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    data=(json.dumps(obj,ensure_ascii=False,indent=2,allow_nan=False)+'\n').encode()
    tmp=p.with_name(p.name+'.'+uuid.uuid4().hex+'.tmp') if replace else p
    with tmp.open('xb') as f: f.write(data); f.flush();os.fsync(f.fileno())
    if replace: os.replace(tmp,p)
    if os.name!='nt':
        fd=os.open(str(p.parent),os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
def spec(p): return {'path':str(Path(p).resolve()),'sha256':sha(p)}
def checked(s):
    p=Path(s['path']).resolve();require(p.is_file() and sha(p)==s['sha256'],'PIN_CHANGED');return p

class JobLock:
    """Conservative file lock. A process crash requires explicit stale-lock review."""
    def __init__(self,p):self.p=Path(p)
    def __enter__(self):
        write(self.p,{'pid':os.getpid(),'at':now()});return self
    def __exit__(self,*args):self.p.unlink()

class LiveOperations:
    def __init__(self,cfg):
        self.cfg=cfg
        rt=checked(cfg['runtime_entry']).parent
        sys.path.insert(0,str(rt));sys.path.insert(0,str(Path(__file__).parent))
        self.pipeline=importlib.import_module('source_pipeline_v2')
        require(Path(self.pipeline.__file__).resolve()==checked(cfg['runtime_entry']),'LOADED_RUNTIME_PATH_MISMATCH')
        self.publisher=importlib.import_module('publisher.adapter')
        from cloud_read_cli import CloudReadClient,fixed_server_token_provider
        self.template=read(checked(cfg['template']))
        root=self.template['discovery']['root_token']
        self.reader=CloudReadClient({'schema_version':'garment_cloud_read/1',
            'allowed_hosts':cfg['allowed_hosts'],'root_tokens':[root],
            'output_root':cfg['state_root'],'max_requests':111},fixed_server_token_provider())
    def preflight(self):
        root=self.template['discovery']['root_token']
        self.reader.files_list({'folder_token':root,'page_size':1})
        return {'status':'DIRECTORY_READABLE','identity':'bot'}
    def snapshot(self,out,structure):return self.publisher.snapshot(out,structure,transport='server-local')
    def _module(self,path):
        name='cloud_tool_'+Path(path).stem
        spec_=importlib.util.spec_from_file_location(name,path)
        mod=importlib.util.module_from_spec(spec_);sys.modules[name]=mod;spec_.loader.exec_module(mod);return mod
    def _runner(self,argv,cwd,timeout):
        name=Path(argv[2]).name
        if name not in ('discover_order_files.py','fetch_increment.py'):
            return self.pipeline.execute_command(argv,cwd,timeout)
        mod=self._module(argv[2]);args=argv[3:]
        def val(k):return args[args.index(k)+1]
        output=io.StringIO();error=io.StringIO()
        try:
            with contextlib.redirect_stdout(output),contextlib.redirect_stderr(error):
                if name=='discover_order_files.py':
                    def cli_runner(cmd,timeout):
                        envelope=self.reader.handle_cli(cmd[2:])
                        ok=envelope.get('ok') is True
                        return SimpleNamespace(returncode=0 if ok else 2,
                            stdout=json.dumps(envelope) if ok else '',stderr='' if ok else json.dumps(envelope))
                    result=mod.discover(folder_token=val('--folder-token'),output_dir=val('--output-dir'),
                        cli_prefix=[val('--cli-executable'),val('--cli-script')],execute_read=True,
                        max_pages=int(val('--max-pages')),max_folders=int(val('--max-folders')),
                        max_items=int(val('--max-items')),page_size=int(val('--page-size')),
                        timeout=int(val('--timeout')),runner=cli_runner,identity='bot')
                    require(result['status']=='COMPLETE','DIRECTORY_DISCOVERY_INCOMPLETE')
                else:
                    client=self.reader
                    class Transport:
                        timeout=120
                        def __call__(self,cmd):
                            env=client.handle_cli(cmd)
                            if not env.get('ok'):
                                err=env.get('error',{})
                                raise mod.FetchError('CLOUD_READ_FAILED',auth=err.get('type') in ('authorization','permission','authentication'),code=err.get('code'),error_type=err.get('type'))
                            return env['data']
                    source=Path(val('--allowlist'))
                    result=mod.execute(read(source),val('--output-dir'),cwd,Transport(),execute_read=True,
                        allowlist_sha256=sha(source),identity='bot')
                    require(result['status']=='COMPLETE','SOURCE_FETCH_INCOMPLETE')
            code=0
        except Exception as exc:
            code=2;error.write(type(exc).__name__)
        return {'returncode':code,'stdout_sha256':hashlib.sha256(output.getvalue().encode()).hexdigest(),
            'stderr_sha256':hashlib.sha256(error.getvalue().encode()).hexdigest(),'transport':'INPROCESS_CLOUD_APP'}
    def stage(self,cfg,out,stage,prior=None):
        if stage=='PREPARE':
            d=read(Path(prior)/'pipeline_manifest.json')
            self.reader.bind_inventory(d['inventory'],d['inventory_sha256'])
        return self.pipeline.run(cfg,out,stage=stage,execute_read=True,discovery_run=prior,runner=self._runner)
    def publish(self,package,before,structure,out):
        return self.publisher.publish(package,before,structure,out,execute=True,transport='server-local')
    def finish(self,out):
        try:
            write(out/'cloud_read_receipts.json',{'identity':'bot','requests':self.reader.requests,
                'authentication_provider_invocations':1 if self.reader._token else 0,'receipts':self.reader.receipts,
                'credentials_persisted':False,'network_retries':0})
        finally:self.reader.close()

def run(config_path,execute=False,ops=None):
    cfg=read(config_path); require(cfg['schema_version']=='garment_cloud_job/1','CONFIG_VERSION')
    root=Path(cfg['state_root']).resolve();require(root.is_dir(),'STATE_ROOT_NOT_INITIALIZED')
    protected={str(checked(s)):s['sha256'] for s in cfg['code_pins']}
    checked(cfg['runtime_entry']);checked(cfg['template']);checked(cfg['structure']);checked(cfg['initial_inventory'])
    if not execute:return {'status':'CHECK_ONLY','configured_schedule':'09:30 Asia/Shanghai','network_calls':0,'database_writes':0}
    require(cfg.get('execution_enabled') is True,'EXECUTION_DISABLED')
    if ops is None:require(os.name!='nt','CLOUD_ONLY')
    with JobLock(root/'job.lock'):
        require(not (root/'pending_publication.json').exists(),'PENDING_PUBLICATION_REQUIRES_RECONCILIATION')
        run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'_'+uuid.uuid4().hex[:8]
        out=root/'runs'/run_id;out.mkdir(parents=True,exist_ok=False)
        receipt={'run_id':run_id,'started_at':now(),'version':VERSION,'status':'RUNNING','scheduler_created_by_entry':False,'manual_tables_modified':False,'cursor_advanced':False}
        write(out/'started.json',receipt)
        finish_attempted=False
        try:
            operations=ops or LiveOperations(cfg)
            access=operations.preflight()
            write(out/'access_preflight.json',access)
            require(access['status']=='DIRECTORY_READABLE','DIRECTORY_PERMISSION_NOT_VERIFIED')
            structure=checked(cfg['structure'])
            state=read(root/'state.json') if (root/'state.json').exists() else {'inventory':cfg['initial_inventory']}
            previous=checked(state['inventory'])
            baseline=operations.snapshot(out/'snapshot',structure)
            require(baseline['status']=='READ_ONLY_SNAPSHOT_VERIFIED','FRESH_BASELINE_FAILED')
            before=out/'snapshot'/'receipt.json'
            pcfg=read(checked(cfg['template']))
            pcfg['task_root']=str(root)
            pcfg['sources']['previous_inventory']=spec(previous)
            pcfg['sources']['publication_receipt']=spec(before)
            write(out/'pipeline_config.json',pcfg)
            disc=operations.stage(out/'pipeline_config.json',out/'discovery','DISCOVER')
            require(disc['status']=='DISCOVERED_NOT_PREPARED','DISCOVERY_FAILED')
            prep=operations.stage(out/'pipeline_config.json',out/'prepare','PREPARE',out/'discovery')
            require(prep['status'] in ('NO_INCREMENT_NOT_PUBLISHED','PREPARED_NOT_PUBLISHED','REPARSE_REQUIRED_NOT_PREPARED'),'PREPARATION_FAILED')
            inventory=Path(prep['next_inventory_candidate'])
            require(sha(inventory)==prep['next_inventory_sha256'],'INVENTORY_CHANGED')
            # Keep every per-run action/review queue immutable, even when no new raw rows are publishable.
            receipt['review_queue']=disc.get('plan')
            receipt['pending_reparse_count']=prep.get('pending_reparse_count',0)
            receipt['pending_reparse_items']=prep.get('pending_reparse_items',[])
            receipt['content_verified_for_metadata_unchanged_files']=False
            if prep['status']=='PREPARED_NOT_PUBLISHED':
                package=Path(prep['package']);require(sha(package/'manifest.json')==prep['package_manifest_sha256'],'PACKAGE_CHANGED')
                pending={'run_id':run_id,'publication_path':str(out/'publication'),'package':spec(package/'manifest.json'),'before':spec(before),'at':now(),'status':'MUST_RECONCILE_IF_INTERRUPTED'}
                write(root/'pending_publication.json',pending)
                pub=operations.publish(package,before,structure,out/'publication')
                receipt['publication_status']=pub['status']
                require(pub['status'] in SUCCESS,'PUBLICATION_NOT_VERIFIED')
                receipt['status']=pub['status']
            else:receipt['status']='NO_INCREMENT_PENDING_REPARSE' if prep['status']=='REPARSE_REQUIRED_NOT_PREPARED' else 'NO_INCREMENT_NOT_PUBLISHED'
            # Do not advance on code mutation, post-commit uncertainty or partial directory discovery.
            require(all(sha(p)==h for p,h in protected.items()),'CODE_CHANGED_DURING_RUN')
            if hasattr(operations,'finish'):
                finish_attempted=True
                operations.finish(out)
            newstate={'inventory':spec(inventory),'last_run':str(out),'last_status':receipt['status'],'at':now()}
            write(root/'state.json',newstate,replace=True)
            receipt['cursor_advanced']=True
            if (root/'pending_publication.json').exists():
                write(out/'pending_resolved.json',{'at':now(),'status':receipt['status'],'pending':read(root/'pending_publication.json')})
                (root/'pending_publication.json').unlink()
        except Exception as e:
            receipt['status']='BLOCKED';receipt['error_code']=str(e) if type(e) is JobContractError else type(e).__name__
            # Exceptions from external code are not forwarded with secret-bearing messages.
            if type(e) is JobContractError and (len(receipt['error_code'])>120 or not all(x.isupper() or x in '_0123456789' for x in receipt['error_code'])):receipt['error_code']='VALIDATION_ERROR'
            receipt['pending_publication']=(root/'pending_publication.json').exists()
        finally:
            if not finish_attempted and 'operations' in locals() and hasattr(operations,'finish'):
                try:operations.finish(out)
                except Exception as exc:
                    receipt['archive_error_type']=type(exc).__name__
                    receipt['status']='BLOCKED'
            receipt['finished_at']=now();write(out/'receipt.json',receipt)
        return receipt

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--execute',action='store_true');a=p.parse_args()
    try:r=run(a.config,a.execute)
    except Exception as e:r={'status':'BLOCKED','error_type':type(e).__name__}
    print(json.dumps(r,ensure_ascii=False));return 2 if r['status']=='BLOCKED' else 0
if __name__=='__main__':raise SystemExit(main())


