import unittest
import pandas as pd
from scripts.blend50_recent_input import completion,compare_overlap,normalized

class RecentInputTests(unittest.TestCase):
    def log(self):
        ev=[]
        for line,marker,nums in [(1,'开始抓取',{}),(2,'当前页已写入数据库',{'offset':'0','page_rows':'2'}),
                                (3,'当前页已写入数据库',{'offset':'2','page_rows':'1'}),
                                (4,'分片店铺抓取并写库完成',{'rows':'3'}),(5,'全部完成',{})]:
            ev.append({'path':'fixture','line':line,'markers':[marker],'numbers':nums,
                'shops':['JQ-US'],'dates':['2026-01-01']*2,'times':['2026-01-03 10:00:00']})
        return {'files':[{'path':'fixture','windows':[['2026-01-01','2026-01-01']],
                         'complete':True,'sha256':'synthetic'}],'events':ev}
    def test_completed_contiguous_chain(self):
        self.assertEqual(completion(self.log(),'2026-01-01','JQ-US')['rows'],3)
    def test_missing_page_blocks(self):
        x=self.log();x['events'].pop(1);self.assertIsNone(completion(x,'2026-01-01','JQ-US'))
    def test_duplicate_page_blocks(self):
        x=self.log();x['events'][2]['numbers']['offset']='0';self.assertIsNone(completion(x,'2026-01-01','JQ-US'))
    def test_incomplete_run_blocks(self):
        x=self.log();x['events'].pop();self.assertIsNone(completion(x,'2026-01-01','JQ-US'))
    def test_failed_slice_blocks(self):
        x=self.log();x['events'][2]['markers'].append('分片店铺抓取失败');self.assertIsNone(completion(x,'2026-01-01','JQ-US'))
    def test_different_scope_blocks(self):
        self.assertIsNone(completion(self.log(),'2026-01-02','JQ-US'))
    def test_revision_kept_not_deduped(self):
        a=pd.DataFrame([dict(ds=pd.Timestamp('2026-01-01'),shop='JQ-US',spu='X',y=1,mapped=True,sales_state='OBSERVED',source_rows=1)])
        b=a.copy();b.loc[0,'y']=2;diff,summary=compare_overlap(a,b)
        self.assertEqual(len(diff),1);self.assertEqual(summary['old_units'],1);self.assertEqual(summary['new_units'],2)
    def test_zero_unknown_mapping_and_volume_separate(self):
        rows=[]
        for i,(spu,v) in enumerate([('X',0),('Y',None),('A|B',7)]):
            rows.append(dict(dt='2026-01-01',store_name='JQ-US',country='US',asin=str(i),sid='1',spu=spu,sku='not-a-mapping',
                stat_date='2026-01-01',delete_flag=0,volume=v,create_time='2026-01-02',modify_time='2026-01-02'))
        out=normalized(pd.DataFrame(rows),'synthetic','2026-01-02T00:00:00Z').set_index('spu')
        self.assertEqual(out.loc['X','sales_state'],'EXPLICIT_ZERO');self.assertEqual(out.loc['Y','sales_state'],'UNKNOWN')
        self.assertFalse(out.loc['A|B','mapped']);self.assertEqual(out.loc['A|B','y'],7)
    def test_duplicate_asin_rejected(self):
        row=dict(dt='2026-01-01',store_name='JQ-US',country='US',asin='same')
        with self.assertRaisesRegex(ValueError,'DUPLICATE_ASIN_KEY'):normalized(pd.DataFrame([row,row]),'x','x')

if __name__=='__main__':unittest.main()
