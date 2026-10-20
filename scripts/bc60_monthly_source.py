"""Bounded read-only transport using the already authorized source probe.

Remote code is streamed to stdin, never installed. Credentials remain in the
existing secure server configuration and are never serialized by this tool.
"""
import argparse
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import subprocess


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', required=True)
    ap.add_argument('--name', required=True)
    ap.add_argument('--sql-file')
    ap.add_argument('--plan-only', action='store_true')
    a = ap.parse_args()
    root = Path(a.root).resolve()
    if root.name != 'bc60_monthly_replay_20261020':
        raise ValueError('NEW_ARCHIVE_ONLY')
    ev = root / 'evidence'; ev.mkdir(exist_ok=True)
    raw = root / 'raw'; raw.mkdir(exist_ok=True)
    dest = ev / (a.name + '.json')
    if dest.exists() or (raw/(a.name+'.json.gz')).exists():
        raise ValueError('NO_OVERWRITE')
    if not a.name.replace('_','').replace('-','').isalnum():
        raise ValueError('INVALID_NAME')
    old = [json.loads(p.read_text(encoding='utf-8')) for p in ev.glob('*.json')]
    selects = [x for x in old if x.get('mode') == 'select']
    plans = sum(x.get('mode') in ('select','plan') for x in old)
    meta = sum(x.get('mode') == 'metadata' for x in old)*15
    spent = sum(x.get('estimated_scan',2000000 if x.get('status')!='OK' else 0) for x in selects)
    if len(selects)>=400 or plans>=450 or meta>=30 or spent+2000000>240000000:
        raise ValueError('FROZEN_QUERY_BUDGET_EXHAUSTED')
    used = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    if used >= 10*1024**3:
        raise ValueError('LOCAL_ARCHIVE_BUDGET_EXHAUSTED')
    req = {'mode':'metadata'}
    if a.sql_file:
        req = {'mode':'plan' if a.plan_only else 'select',
               'sql':Path(a.sql_file).read_text(encoding='utf-8').strip()}
    source = Path(__file__).with_name('bc60_readonly_probe.py').read_bytes()
    acquired = dt.datetime.now(dt.timezone.utc).isoformat()
    # Reserve an attempt before touching the network, including failed attempts.
    receipt = {'mode':req['mode'],'request':req,'status':'RESERVED','started_at':acquired,
               'probe_sha256':sha(source),'estimated_scan':2000000 if req['mode']=='select' else 0}
    dest.write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8')
    try:
        cp = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',
            'root@8.138.102.103','cd /opt/apps/pythondata && PYTHONDONTWRITEBYTECODE=1 venv-ml/bin/python -'],
            input=source+b'\nmain('+repr(req).encode('utf-8')+b')\n',capture_output=True,timeout=90)
        if cp.returncode:
            receipt.update(status='TRANSPORT_FAILED',exit_code=cp.returncode,
                           stderr_sha256=sha(cp.stderr))
        else:
            result=json.loads(cp.stdout)
            receipt.update(result)
            if 'rows' in receipt:
                payload=json.dumps(receipt.pop('rows'),ensure_ascii=False,separators=(',',':')).encode('utf-8')
                with gzip.open(raw/(a.name+'.json.gz'),'xb') as fh: fh.write(payload)
                receipt.update(rows_sha256=sha(payload),raw_path='raw/'+a.name+'.json.gz')
    except Exception as exc:
        receipt.update(status='TRANSPORT_OR_PARSE_FAILED',error_class=type(exc).__name__)
    receipt['received_at']=dt.datetime.now(dt.timezone.utc).isoformat()
    receipt['source_commit']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parent.parent,text=True).strip()
    dest.write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:receipt.get(k) for k in ('status','mode','row_count','estimated_scan','elapsed_seconds','error_class','error_code','exit_code')},ensure_ascii=False))
    if receipt['status']!='OK': raise SystemExit(1)


if __name__=='__main__': main()
