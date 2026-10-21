"""Serial bounded acquisition after reproduction PASS; no remote installation."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
try:
    from scripts.blend50_prospective import read,sha,clock,dump_new
except ModuleNotFoundError:
    from blend50_prospective import read,sha,clock,dump_new

def acquire(root,name,sql_file=None,plan_only=False,proof_root=None):
    root=Path(root).resolve()
    proof_base=Path(proof_root).resolve() if proof_root else root
    proof=read(proof_base/'reproduction/reproduction_receipt.json')
    if proof['status']!='PASS':raise ValueError('REPRODUCTION_REQUIRED')
    if sha(proof_base/'reproduction/prediction_seal.json')!=proof['generation_seal_sha256']:raise ValueError('REPRODUCTION_TAMPERED')
    for rel in ['source','source/attempts','source/receipts','source/raw']:(root/rel).mkdir(exist_ok=True)
    ev=root/'source/receipts';attempts=root/'source/attempts'
    if not name.replace('_','').replace('-','').isalnum():raise ValueError('NAME')
    if (attempts/(name+'.json')).exists():raise ValueError('NO_OVERWRITE_OR_SILENT_RETRY')
    locks=list(attempts.glob('*.json'));receipts=[read(p) for p in ev.glob('*.json')]
    if len(locks)!=len(receipts):raise ValueError('PREVIOUS_ATTEMPT_UNRESOLVED')
    # A failed transport conservatively consumes its maximum allowance.
    business=sum(x.get('select_count',1 if x.get('mode')=='select' else 0) for x in receipts)
    spent=sum(x.get('estimated_scan',1000000) for x in receipts if x.get('select_count',int(x.get('mode')=='select'))>0)
    if business>=30 or spent>=10000000:raise ValueError('QUERY_BUDGET_EXHAUSTED')
    req={'mode':'metadata','remaining_scan':10000000-spent}
    if sql_file:req.update(mode='plan' if plan_only else 'select',sql=Path(sql_file).read_text(encoding='utf-8').strip())
    if req['mode']=='metadata' and any(r['mode']=='metadata' and r.get('metadata_count',0)>0 for r in receipts):raise ValueError('METADATA_ALREADY_READ')
    probe=Path(__file__).with_name('blend50_source_probe.py').read_bytes()
    reservation={'at':clock(),'mode':req['mode'],'request':req,'probe_sha256':hashlib.sha256(probe).hexdigest(),
                 'reproduction_receipt':str(proof_base/'reproduction/reproduction_receipt.json'),
                 'reproduction_receipt_sha256':sha(proof_base/'reproduction/reproduction_receipt.json'),
                 'previous_selects':business,'previous_estimated_scan':spent}
    dump_new(attempts/(name+'.json'),reservation)
    receipt=dict(reservation,status='TRANSPORT_FAILED')
    try:
        cp=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','root@8.138.102.103',
             'cd /opt/apps/pythondata && PYTHONDONTWRITEBYTECODE=1 venv-ml/bin/python -'],
             input=probe+b'\nmain('+repr(req).encode('utf-8')+b')\n',capture_output=True,timeout=90)
        receipt['transport_exit_code']=cp.returncode
        receipt['stderr_sha256']=hashlib.sha256(cp.stderr).hexdigest()
        stderr=cp.stderr.decode('utf-8',errors='replace').lower()
        receipt['transport_diagnostic']=[tag for tag,phrase in [('AUTHENTICATION_DENIED','permission denied'),
            ('NETWORK_TIMEOUT','timed out'),('CONNECTION_REFUSED','connection refused'),('NETWORK_UNREACHABLE','network is unreachable'),
            ('HOST_KEY_FAILED','host key verification failed'),('LOCAL_SOCKET_DENIED','operation not permitted'),
            ('LOCAL_CONFIG_ACCESS','bad owner or permissions')] if phrase in stderr]
        if cp.returncode==0:
            receipt.update(json.loads(cp.stdout))
            if 'rows' in receipt:
                blob=json.dumps(receipt.pop('rows'),ensure_ascii=False,separators=(',',':')).encode('utf-8')
                raw=root/'source/raw'/(name+'.json.gz')
                with gzip.open(raw,'xb') as f:f.write(blob)
                receipt.update(raw_path=str(raw.relative_to(root)),raw_sha256=sha(raw),payload_sha256=hashlib.sha256(blob).hexdigest())
    except Exception as exc:receipt.update(error_class=type(exc).__name__,status='TRANSPORT_OR_PARSE_FAILED')
    receipt['received_at']=clock();dump_new(ev/(name+'.json'),receipt)
    print(json.dumps({k:receipt.get(k) for k in ['status','mode','metadata_count','explain_count','select_count','row_count','estimated_scan','error_class','error_code','guard','transport_exit_code','transport_diagnostic']},ensure_ascii=False))
    if receipt['status']!='OK':raise SystemExit(1)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',required=True);p.add_argument('--name',required=True)
    p.add_argument('--sql-file');p.add_argument('--proof-root');p.add_argument('--plan-only',action='store_true');acquire(**vars(p.parse_args()))
