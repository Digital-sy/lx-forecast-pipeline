"""Pure offline contract prototype. Does not collect, archive, or run a model.

The new canonical format is versioned; it is not the frozen V3 input-hash format.
Supplied timestamps are assertions, not independently established provenance.
"""
from datetime import datetime, date
import hashlib
import json
import re


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def aware(value):
    parsed=datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:raise ValueError('Timezone required')
    return parsed


def validate(manifest, inputs, artifacts):
    """artifacts is a caller-supplied mapping of safe relative names to bytes."""
    errors=[]; limitations=[]
    def require(ok, message):
        if not ok:errors.append(message)
    try:
        require(manifest['schema_version']==1, 'Unsupported schema')
        require(manifest['input_format']=='normalized-v1', 'Input format required')
        require(manifest['provenance'] in ('ORIGINAL_CAPTURE','RECONSTRUCTED_INPUT_DIAGNOSTIC'), 'Provenance invalid')
        if manifest['provenance']!='ORIGINAL_CAPTURE':limitations.append('Original capture not established')
        captured=aware(manifest['acquired_at']);decision=aware(manifest['decision_at'])
        if captured>decision:limitations.append('Captured after decision; not point-in-time available')
        require(manifest['completeness'] in ('COMPLETE','PARTIAL','FAILED'), 'Completeness invalid')
        if manifest['completeness']!='COMPLETE':limitations.append('Inputs incomplete or failed')
        require(isinstance(manifest['failures'],list), 'Failure list required')
        if manifest['failures']:limitations.append('Acquisition failures recorded')
        require(bool(manifest['run_id']), 'Run id required')
        require(set(inputs)=={'cutoff','shop','monthly','mapping','categories','seasons'}, 'Complete normalized components required')
        cutoff=date.fromisoformat(inputs['cutoff'])
        keys=set()
        for row in inputs['monthly']:
            key=(row['shop'],row['sku'],row['spu'],row['month'])
            require(key not in keys,'Duplicate monthly key');keys.add(key)
            require(date.fromisoformat(row['month'])<cutoff,'Future/incomplete month input')
            require(row['state'] in ('OBSERVED_ZERO','OBSERVED_VALUE','UNKNOWN'), 'Observation state invalid')
            require((row['value'] is None)==(row['state']=='UNKNOWN'),'Missing must not become zero')
            if row['state']=='OBSERVED_ZERO':require(row['value']==0,'Observed zero mismatch')
            if row['state']=='OBSERVED_VALUE':require(isinstance(row['value'],(int,float)) and row['value']>0,'Observed value invalid')
        require(canonical_hash(inputs)==manifest['input_sha256'],'Input hash mismatch')
        require(canonical_hash(manifest['parameters'])==manifest['parameters_sha256'],'Parameter hash mismatch')
        require(re.fullmatch('[0-9a-f]{40}',manifest['source_commit']) is not None,'Commit required')
        require(bool(manifest['artifacts']) and bool(manifest['sources']), 'Artifacts and source evidence required')
        require({'source','raw_extract','input','output','mapping','category','season'} <= {r['role']for r in manifest['artifacts']},'Artifact roles incomplete')
        names=set()
        input_files=[]
        for row in manifest['artifacts']:
            name=row['name'];require(name not in names,'Duplicate artifact');names.add(name)
            require(not name.startswith(('/', '\\')) and ':'not in name and '..'not in name.replace('\\','/').split('/'),'Unsafe artifact path')
            require(name in artifacts,'Artifact missing')
            if name in artifacts:require(hashlib.sha256(artifacts[name]).hexdigest()==row['sha256'],'Artifact hash mismatch')
            if row['role']=='input':input_files.append(name)
        require(len(input_files)==1,'One normalized input artifact required')
        if len(input_files)==1 and input_files[0]in artifacts:
            require(json.loads(artifacts[input_files[0]].decode('utf-8'))==inputs,'Normalized artifact differs from supplied input')
        for source in manifest['sources']:
            require(bool(source['query_text']) and isinstance(source['parameters'],dict),'Extraction evidence required')
            require(source['raw_artifact']in names,'Raw artifact not manifested')
            require(isinstance(source['row_count'],int) and source['row_count']>=0,'Source row count invalid')
            require(not source['truncated'],'Truncated extraction')
            aware(source['acquired_at']);aware(source['business_asof'])
            if source['source_sync_at'] is not None:aware(source['source_sync_at'])
            if source['sync_status']!='VERIFIED' or source['source_sync_at']is None:limitations.append('Source synchronization unverified')
            if source['historical_effectivity']!='VERIFIED':limitations.append('Historical metadata effectivity unverified')
        return {'valid_contract':not errors,'replay_ready':not errors and manifest['completeness']=='COMPLETE' and not manifest['failures'],
                'point_in_time_claim_supported':not errors and not limitations,
                'errors':errors,'limitations':sorted(set(limitations)),
                'caveat':'Schema checks do not authenticate timestamps or source truth; external capture receipts required'}
    except (KeyError,ValueError,TypeError) as exc:
        return {'valid_contract':False,'replay_ready':False,'point_in_time_claim_supported':False,
                'errors':errors+[type(exc).__name__+': '+str(exc)],'limitations':sorted(set(limitations))}
