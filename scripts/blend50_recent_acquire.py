"""Local serial transport. Restricts queries to the frozen issue's recent tail."""
import argparse,base64,datetime,gzip,hashlib,json,pathlib,subprocess
from zoneinfo import ZoneInfo

def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
def dump(p,obj):
    with pathlib.Path(p).open('x',encoding='utf8') as f:json.dump(obj,f,ensure_ascii=False,indent=2)

def initialize(root,workspace):
    now=datetime.datetime.now(datetime.timezone.utc);s=now.astimezone(ZoneInfo('Asia/Shanghai')).date();c=s-datetime.timedelta(days=2)
    root.mkdir(exist_ok=False)
    cache=workspace/'six_slice_retry_20261027_v1/cache_repaired_v2/daily_input.parquet'
    manifest=cache.with_name('manifest.json');m=json.loads(manifest.read_text(encoding='utf8'))
    assert sha(cache)==m['output_sha256']
    last=datetime.date.fromisoformat(m['last_date'][:10]);start=last-datetime.timedelta(days=6)
    proof=workspace/'blend50_prospective_shadow_20261021_v1/reproduction/reproduction_receipt.json'
    pr=json.loads(proof.read_text(encoding='utf8'));assert pr['status']=='PASS' and pr['objects']==106
    state={'started_at':now.isoformat(),'issue_date':str(s),'input_cutoff':str(c),
        'windows':{'M1':[str(s+datetime.timedelta(days=x)) for x in (1,30)],'M2':[str(s+datetime.timedelta(days=x)) for x in (31,60)]},
        'cache_path':str(cache),'cache_sha256':sha(cache),'cache_manifest_sha256':sha(manifest),
        'scope_start':str(start),'scope_end_exclusive':str(c+datetime.timedelta(days=1)),
        'missing_start':str(last+datetime.timedelta(days=1)),'overlap_end':str(last),
        'reproduction_receipt':str(proof),'reproduction_sha256':sha(proof),
        'budget':{'business_selects':30,'per_query_timeout_ms':10000,'per_query_scan':1000000,'total_scan':10000000,'max_formal_fits':4},
        'authorization':'Current user authorized recent ODS read, then one fixed local shadow issue if gates pass.',
        'production_writes':0,'api_calls':0}
    dump(root/'run_lock.json',state);(root/'source').mkdir();print(json.dumps(state,ensure_ascii=False))

def acquire(root,name,mode,start=None,end=None):
    lock=json.loads((root/'run_lock.json').read_text(encoding='utf8'));folder=root/'source';receipts=list(folder.glob('*.receipt.json'))
    old=[json.loads(p.read_text(encoding='utf8')) for p in receipts]
    for p,x in zip(receipts,old):
        if x['status']=='OK':continue
        resolution=p.with_name(p.name.replace('.receipt.json','.transport_resolution.json'))
        if not resolution.exists():raise ValueError('UNRESOLVED_PREVIOUS_ATTEMPT')
        resolved=json.loads(resolution.read_text(encoding='utf8'))
        if not (x['status']=='TRANSPORT_FAILED' and resolved['receipt_sha256']==sha(p)
                and resolved['reason']=='HOST_KEY_CHECK_STOPPED_BEFORE_REMOTE_EXECUTION'):
            raise ValueError('INVALID_TRANSPORT_RESOLUTION')
    if list(folder.glob(name+'.*')):raise ValueError('NO_OVERWRITE')
    used=sum(x.get('estimated_scan',0) for x in old)
    if mode=='data' and (sum(x['mode']=='data' for x in old)>=30 or used>=10000000):raise ValueError('QUERY_BUDGET')
    req={'mode':mode,'scope_start':lock['scope_start'],'scope_end_exclusive':lock['scope_end_exclusive'],'remaining_scan':10000000-used}
    if mode=='data':req.update(start=start,end_exclusive=end)
    code=pathlib.Path(__file__).with_name('blend50_recent_probe.py').read_bytes()
    dump(folder/(name+'.attempt.json'),{'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'request':req,'probe_sha256':hashlib.sha256(code).hexdigest()})
    p=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','StrictHostKeyChecking=yes','root@8.138.102.103',
        'cd /opt/apps/pythondata && PYTHONDONTWRITEBYTECODE=1 venv-ml/bin/python -B -'],
        input=code+b'\nmain('+repr(req).encode('utf8')+b')\n',capture_output=True,timeout=110)
    if p.returncode:
        dump(folder/(name+'.receipt.json'),{'mode':mode,'status':'TRANSPORT_FAILED','exit':p.returncode,'stderr_sha256':hashlib.sha256(p.stderr).hexdigest()})
        raise SystemExit('TRANSPORT_FAILED_'+str(p.returncode))
    env=json.loads(p.stdout);raw=base64.b64decode(env['gzip_base64']);blob=gzip.decompress(raw)
    assert hashlib.sha256(blob).hexdigest()==env['json_sha256'];result=json.loads(blob)
    with (folder/(name+'.json.gz')).open('xb') as f:f.write(raw)
    receipt={k:v for k,v in result.items() if k!='rows'}
    receipt.update(raw_path=name+'.json.gz',raw_sha256=sha(folder/(name+'.json.gz')),payload_sha256=env['json_sha256'])
    dump(folder/(name+'.receipt.json'),receipt)
    print(json.dumps({k:receipt.get(k) for k in ('mode','status','row_count','estimated_scan','select_seconds','guard')},ensure_ascii=False))
    if result['status']!='OK':raise SystemExit(1)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);p.add_argument('--workspace',type=pathlib.Path)
    p.add_argument('--mode',choices=['initialize','metadata','logs','data'],required=True);p.add_argument('--name');p.add_argument('--start');p.add_argument('--end')
    a=p.parse_args()
    if a.mode=='initialize':initialize(a.root.resolve(),a.workspace.resolve())
    else:acquire(a.root.resolve(),a.name,a.mode,a.start,a.end)
