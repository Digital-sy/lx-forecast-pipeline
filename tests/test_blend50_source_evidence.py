"""Synthetic evidence only; never connects to an external system."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from scripts.blend50_source_evidence import (SCHEMA,SOURCE,canonical,digest,
    rows_signature,validate_pages,EvidenceError,audit_bundle,project_payload)


def fixture(root):
    day,shop,sid='2001-01-02','TEST-US','999'
    scope=[dict(date=day,shop=shop,sid=sid)]
    def put(name,obj):
        blob=canonical(obj);(root/name).write_bytes(blob)
        return dict(path=name,sha256=digest(blob))
    rows=[dict(dt=day,store_name=shop,country='TEST',asin='TEST'+str(i),sid=sid,
               sku='SKU'+str(i),spu='SPU'+str(i),stat_date=day,delete_flag=0,volume=i) for i in range(2)]
    refs=[]
    for i,row in enumerate(rows):
        req=dict(date=day,shop=shop,sid=sid,offset=i,length=1,summary_field='asin')
        native=dict(sid=sid,asins=[dict(asin=row['asin'],sid=sid)],
                    price_list=[dict(country='TEST',local_sku=row['sku'])],
                    spu_spu_names=[dict(spu=row['spu'])],volume=row['volume'])
        raw=dict(code=0,request_id='test-request-'+str(i),data=[native],total=2)
        rawref=put('raw'+str(i)+'.json',raw)
        page=dict(request=req,code=0,request_id=raw['request_id'],total=2,rows=[row],
                  started_at='2001-01-03T00:0%d:00+00:00'%i,
                  acquired_at='2001-01-03T00:0%d:01+00:00'%i,raw_response=rawref)
        refs.append(put('page'+str(i)+'.json',page))
    sig=rows_signature(rows,day,shop,sid)
    export=put('export.json',dict(source='LINGXING_PLATFORM_EXPORT',export_id='synthetic-export',
               date=day,shop=shop,sid=sid,acquired_at='2001-01-03T00:02:00+00:00',rows=rows))
    pub=put('pub.json',dict(transaction_id='test-tx',task_id='test-task',date=day,shop=shop,sid=sid,
               committed_at='2001-01-03T00:03:00+00:00',content_sha256=sig['content_sha256'],
               page_manifest_sha256=digest(canonical(refs))))
    m=dict(schema=SCHEMA,source=SOURCE,scope=scope,adapter_sha256='a'*64,collector_sha256='b'*64,
           slices=[dict(date=day,shop=shop,sid=sid,pages=refs,platform_export=export,publication=pub)])
    put('manifest.json',m)
    current=put('current.json',dict(source=SOURCE,scope=scope,acquired_at='2001-01-03T00:04:00+00:00',rows=rows))
    return m,scope,current,put


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.m,self.scope,self.current,self.put=fixture(self.root)
        self.scope=copy.deepcopy(self.scope)
    def tearDown(self):self.tmp.cleanup()
    def run_gate(self):
        mref=self.put('manifest.json',self.m)
        return audit_bundle(self.root,mref['sha256'],self.scope,self.current)
    def pages(self):return [json.loads((self.root/r['path']).read_bytes()) for r in self.m['slices'][0]['pages']]
    def page_error(self,mutation,code):
        pages=self.pages();mutation(pages)
        with self.assertRaisesRegex(EvidenceError,code):validate_pages(pages,'2001-01-02','TEST-US','999')
    def test_complete(self):self.assertEqual(self.run_gate()['status'],'PASS_SOURCE_EVIDENCE_ONLY')
    def test_no_receipt(self):
        self.m['slices']=[];self.assertEqual(self.run_gate()['reason'],'REQUIRED_SLICE_WITHOUT_RECEIPT')
    def test_typed_pass_not_evidence(self):
        self.m['slices'][0]['pages']=[];self.m['status']='PASS'
        self.assertEqual(self.run_gate()['reason'],'NO_PAGE_RECEIPTS')
    def test_partial(self):self.page_error(lambda p:p.pop(),'PAGINATION_INCOMPLETE')
    def test_unknown_not_zero(self):self.page_error(lambda p:p[0]['rows'][0].update(volume=None),'UNKNOWN_NOT_ZERO')
    def test_explicit_zero_allowed(self):self.assertEqual(self.pages()[0]['rows'][0]['volume'],0)
    def test_wrong_date(self):self.page_error(lambda p:p[0]['request'].update(date='2001-01-01'),'PAGE_SCOPE_MISMATCH')
    def test_wrong_shop(self):self.page_error(lambda p:p[0]['rows'][0].update(store_name='OTHER'),'ROW_SCOPE_MISMATCH')
    def test_wrong_sid(self):self.page_error(lambda p:p[0]['rows'][0].update(sid='998'),'ROW_SCOPE_MISMATCH')
    def test_duplicate_page(self):self.page_error(lambda p:p[1].update(request_id=p[0]['request_id']),'DUPLICATE_OR_MISSING_REQUEST_ID')
    def test_duplicate_key(self):self.page_error(lambda p:p[1]['rows'][0].update(asin='TEST0'),'DUPLICATE_PRIMARY_KEY')
    def test_missing_page(self):self.page_error(lambda p:p[1]['request'].update(offset=3),'OFFSET_OR_LENGTH_INVALID')
    def test_early_empty(self):self.page_error(lambda p:p[0].update(rows=[]),'EMPTY_PAGE_BEFORE_COMPLETION')
    def test_drifting_total(self):self.page_error(lambda p:p[1].update(total=3),'TOTAL_DRIFT')
    def test_rate_limit(self):self.page_error(lambda p:p[1].update(code=3001008),'API_ERROR_OR_RETRY_EXHAUSTED')
    def test_negative(self):self.page_error(lambda p:p[1]['rows'][0].update(volume=-1),'QUANTITY_INVALID')
    def test_mapping(self):self.page_error(lambda p:p[1]['rows'][0].update(spu='A|B'),'MAPPING_UNRESOLVED')
    def test_unzoned(self):self.page_error(lambda p:p[0].update(acquired_at='2001-01-03T00:01:00'),'TIMEZONE_UNKNOWN')
    def test_tampered_receipt(self):
        (self.root/'pub.json').write_text('{}');self.assertEqual(self.run_gate()['reason'],'ARTIFACT_TAMPERED_OR_REVISED')
    def test_source_revision_even_if_rehashed(self):
        c=json.loads((self.root/'current.json').read_bytes());c['rows'][1]['volume']=5
        self.current=self.put('current.json',c)
        self.assertEqual(self.run_gate()['reason'],'CURRENT_SOURCE_REVISED')
    def test_platform_disagrees(self):
        e=json.loads((self.root/'export.json').read_bytes());e['rows'][1]['volume']=5
        self.m['slices'][0]['platform_export']=self.put('export.json',e)
        self.assertEqual(self.run_gate()['reason'],'PLATFORM_RECONCILIATION_FAILED')
    def test_raw_response_changes(self):
        r=json.loads((self.root/'raw1.json').read_bytes());r['data'][0]['volume']=100
        pg=self.pages()[1];pg['raw_response']=self.put('raw1.json',r)
        self.m['slices'][0]['pages'][1]=self.put('page1.json',pg)
        self.assertEqual(self.run_gate()['reason'],'RAW_ROWS_MISMATCH')
    def test_scope_extra(self):
        self.scope.append(dict(date='2001-01-01',shop='TEST-US',sid='999'))
        self.assertEqual(self.run_gate()['reason'],'MANIFEST_SCOPE_MISMATCH')
    def test_path_escape(self):
        self.current=dict(path='../outside.json',sha256='0'*64)
        self.assertEqual(self.run_gate()['reason'],'PATH_ESCAPE')
    def test_pinned_manifest_tamper(self):
        mref=self.put('manifest.json',self.m)
        self.m['source']='OTHER';self.put('manifest.json',self.m)
        self.assertEqual(audit_bundle(self.root,mref['sha256'],self.scope,self.current)['reason'],'ARTIFACT_TAMPERED_OR_REVISED')
    def test_publication_not_bound(self):
        p=json.loads((self.root/'pub.json').read_bytes());p['page_manifest_sha256']='0'*64
        self.m['slices'][0]['publication']=self.put('pub.json',p)
        self.assertEqual(self.run_gate()['reason'],'PUBLICATION_NOT_BOUND')
    def test_source_unknown_mapping_preserved_not_accepted_for_model(self):
        pages=self.pages();pages[0]['rows'][0]['spu']=None
        rows,*_=validate_pages(pages,'2001-01-02','TEST-US','999',False)
        self.assertIsNone(rows[0]['spu'])
        with self.assertRaisesRegex(EvidenceError,'MAPPING_UNRESOLVED'):validate_pages(pages,'2001-01-02','TEST-US','999')


if __name__=='__main__':unittest.main()
