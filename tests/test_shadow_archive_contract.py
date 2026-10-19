import copy
import hashlib
import json
import unittest
from scripts.validate_shadow_archive_contract import canonical_hash,validate


def fixture():
    inp={'cutoff':'2030-03-01','shop':'SYNTHETIC','monthly':[{'shop':'SYNTHETIC','sku':'X','spu':'X','month':'2030-02-01','state':'OBSERVED_ZERO','value':0}], 'mapping':[],'categories':[],'seasons':[]}
    artifacts={r+'.bin':r.encode()for r in ('source','raw_extract','input','output','mapping','category','season')}
    artifacts['input.bin']=json.dumps(inp).encode()
    m={'schema_version':1,'input_format':'normalized-v1','provenance':'ORIGINAL_CAPTURE','run_id':'synthetic',
       'acquired_at':'2030-03-01T01:00:00+00:00','decision_at':'2030-03-01T02:00:00+00:00',
       'completeness':'COMPLETE','failures':[],'input_sha256':canonical_hash(inp),
       'parameters':{},'parameters_sha256':canonical_hash({}),'source_commit':'a'*40,
       'artifacts':[{'name':n,'role':n[:-4],'sha256':hashlib.sha256(b).hexdigest()}for n,b in artifacts.items()],
       'sources':[{'query_text':'SELECT bounded_fields FROM synthetic_source WHERE period = :period','parameters':{'period':'2030-02'},
        'raw_artifact':'raw_extract.bin','row_count':1,'truncated':False,'acquired_at':'2030-03-01T01:00:00Z',
        'business_asof':'2030-02-28T23:59:59Z','source_sync_at':'2030-03-01T00:30:00Z','sync_status':'VERIFIED','historical_effectivity':'VERIFIED'}]}
    return m,inp,artifacts


class ArchiveTests(unittest.TestCase):
    def test_complete(self):self.assertTrue(validate(*fixture())['replay_ready'])
    def test_hash_order(self):self.assertEqual(canonical_hash({'a':1,'b':2}),canonical_hash({'b':2,'a':1}))
    def test_tamper(self):
        m,i,a=fixture();a['output.bin']=b'changed';self.assertFalse(validate(m,i,a)['valid_contract'])
    def test_unknown_sync_not_readiness(self):
        m,i,a=fixture();m['sources'][0]['source_sync_at']=None
        r=validate(m,i,a);self.assertTrue(r['replay_ready']);self.assertFalse(r['point_in_time_claim_supported'])
    def test_reconstruction_not_original(self):
        m,i,a=fixture();m['provenance']='RECONSTRUCTED_INPUT_DIAGNOSTIC'
        self.assertFalse(validate(m,i,a)['point_in_time_claim_supported'])
    def test_late_acquisition(self):
        m,i,a=fixture();m['acquired_at']='2030-03-02T00:00:00Z';self.assertFalse(validate(m,i,a)['point_in_time_claim_supported'])
    def test_incomplete_retained(self):
        m,i,a=fixture();m['completeness']='FAILED';m['failures']=['source unavailable']
        r=validate(m,i,a);self.assertTrue(r['valid_contract']);self.assertFalse(r['replay_ready'])
    def test_future_rejected(self):
        m,i,a=fixture();i['monthly'][0]['month']='2030-03-01';m['input_sha256']=canonical_hash(i)
        r=validate(m,i,a);self.assertFalse(r['valid_contract']);self.assertIn('Future/incomplete month input',r['errors'])
    def test_unknown_not_zero(self):
        m,i,a=fixture();i['monthly'][0]['state']='UNKNOWN';m['input_sha256']=canonical_hash(i)
        r=validate(m,i,a);self.assertFalse(r['valid_contract']);self.assertIn('Missing must not become zero',r['errors'])
    def test_duplicate_key(self):
        m,i,a=fixture();i['monthly'].append(copy.deepcopy(i['monthly'][0]));m['input_sha256']=canonical_hash(i)
        self.assertFalse(validate(m,i,a)['valid_contract'])
    def test_timezone_required(self):
        m,i,a=fixture();m['acquired_at']='2030-03-01T01:00:00';self.assertFalse(validate(m,i,a)['valid_contract'])
    def test_truncation(self):
        m,i,a=fixture();m['sources'][0]['truncated']=True;self.assertFalse(validate(m,i,a)['valid_contract'])


if __name__=='__main__':unittest.main()
