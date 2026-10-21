"""Future validation preflight only; never invents truth or changes predictions.

The independently validated historical daily scorer remains the numerical
reference. A future issued-batch adapter requires an actual sealed issuance.
This entrypoint cannot score the blocked first-attempt archive.
"""
import argparse
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
try:
    from scripts.blend50_prospective import read,sha,clock,dump_new,time_contract,verify_seal_time,SHOPS
except ModuleNotFoundError:
    from blend50_prospective import read,sha,clock,dump_new,time_contract,verify_seal_time,SHOPS

def check(manifest,at,source):
    if manifest.get('state')!='ISSUED_SHADOW_AWAITING_ACTUALS':
        return {'status':'NOT_ISSUED','scorable_windows':[]}
    contract=time_contract(manifest['issue_date'])
    verify_seal_time(manifest['issue_date'],manifest['prediction_sealed_at'])
    t=pd.Timestamp(at)
    if t.tz is None:raise ValueError('TIMEZONE_REQUIRED')
    day=t.tz_convert('Asia/Shanghai').date()
    ready=[];states={}
    for window,date in [('W1',contract['earliest_checks'][0]),('W2',contract['earliest_checks'][1]),('W60',contract['earliest_checks'][1])]:
        if day<pd.Timestamp(date).date():states[window]='NOT_MATURED';continue
        evidence=source.get('windows',{}).get(window,{})
        required=['complete','sync_completed','reconciliation_passed','mapping_checked','immutable_truth_version']
        keys=['source_business_through','source_acquired_at','source_sync_evidence_id','truth_sha256']
        valid=all(evidence.get(k) is True for k in required) and all(evidence.get(k) for k in keys)
        valid=valid and sorted(evidence.get('shops',[]))==sorted(SHOPS)
        valid=valid and evidence.get('source_business_through','')>=contract[window][1]
        if not valid:states[window]='TRUTH_INCOMPLETE';continue
        if pd.Timestamp(evidence['source_acquired_at']).tz is None:raise ValueError('TRUTH_ACQUIRED_TIMEZONE_REQUIRED')
        if pd.Timestamp(evidence['source_acquired_at'])>t:raise ValueError('FUTURE_ACQUISITION_TIME')
        states[window]='READY_FOR_INDEPENDENT_SCORING';ready.append(window)
    status='READY_FOR_INDEPENDENT_SCORING' if ready else 'NOT_MATURED' if all(s=='NOT_MATURED' for s in states.values()) else 'TRUTH_INCOMPLETE'
    return {'status':status,'windows':states,'scorable_windows':ready,'contract':contract,'accuracy_computed':False}

def main(manifest,source_receipt,out):
    manifest,source_receipt,out=map(Path,[manifest,source_receipt,out]);out.mkdir(exist_ok=False)
    original={str(manifest.resolve()):sha(manifest),str(source_receipt.resolve()):sha(source_receipt)}
    result=check(read(manifest),datetime.now(timezone.utc),read(source_receipt))
    if any(sha(p)!=h for p,h in original.items()):raise ValueError('INPUT_CHANGED')
    result.update(at=clock(),input_hashes=original,predictions_modified=False)
    dump_new(out/'maturity_receipt.json',result);print(result['status'])

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ['manifest','source-receipt','out']:p.add_argument('--'+n,required=True)
    main(**vars(p.parse_args()))
