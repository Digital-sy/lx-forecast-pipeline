"""Synthetic-only checks for the isolated shadow gates."""
import tempfile
import unittest
from pathlib import Path
import numpy as np
import pandas as pd
from scripts import blend50_prospective as p
from scripts import blend50_source_probe as probe
from scripts import blend50_source_audit as source_audit
from scripts import blend50_maturity_gate as maturity


class ShadowTests(unittest.TestCase):
    def test_arbitrary_issue_cross_year(self):
        c=p.time_contract('2026-10-21')
        self.assertEqual(c['input_cutoff'],'2026-10-19')
        self.assertEqual(c['W1'],['2026-10-22','2026-11-20'])
        self.assertEqual(c['W2'],['2026-11-21','2026-12-20'])
        self.assertEqual(c['earliest_checks'],['2026-11-22','2026-12-22'])
        self.assertEqual(p.time_contract('2026-12-31')['W60'][1],'2027-03-01')

    def test_seal_before_target_only(self):
        p.verify_seal_time('2026-10-21','2026-10-21T23:59:59+08:00')
        with self.assertRaises(ValueError):p.verify_seal_time('2026-10-21','2026-10-22T00:00:00+08:00')
        with self.assertRaises(ValueError):p.verify_seal_time('2026-10-21','2026-10-21T12:00:00')

    def test_leap_annual_anchor(self):
        a,b,c,d=p.annual.annual_bounds('2024-02-29','M1')
        self.assertEqual(str(b.date()),'2023-02-26')
        self.assertEqual(str(c.date()),'2023-03-01')
        self.assertEqual(str(d.date()),'2023-03-30')

    def test_zero_missing_and_unsafe(self):
        d=pd.DataFrame({'ds':pd.date_range('2024-01-01',periods=10),'shop':'JQ-US','spu':'SYNTHETIC',
                        'y':0.,'mapped':True,'sales_state':'EXPLICIT_ZERO'})
        idx=p.annual.DailyIndex(d)
        m,n,_,_,z=idx.interval(('JQ-US','SYNTHETIC'),['2024-01-01'],['2024-01-10'])
        self.assertEqual((m[0],n[0],z[0]),(0,0,10))
        idx=p.annual.DailyIndex(d.drop(index=4))
        m,n,a,_,_=idx.interval(('JQ-US','SYNTHETIC'),['2024-01-01'],['2024-01-10'])
        self.assertTrue(np.isnan(m[0]));self.assertEqual((n[0],a[0]),(1,1))

    def test_gate_and_fixed_blend(self):
        e={x:True for x in ['direct_valid','input_valid','targets_valid','prior_positive','both_supported','both_annual']}
        e.update(direct_M1=100.,direct_M2=120.,annual_M1=200.,annual_M2=60.)
        r=p.gate.choose(e)
        self.assertTrue(r['gate_decision'])
        self.assertEqual(p.blend_one(100.,r['selected_M1'],True),150.)
        for field in ['targets_valid','both_supported','prior_positive']:
            r=p.gate.choose(dict(e,**{field:False}))
            self.assertFalse(r['gate_decision'])
            self.assertEqual(p.blend_one(100.,r['selected_M1'],False),100.)
        self.assertTrue(np.isnan(p.blend_one(np.nan,np.nan,False)))
        self.assertEqual(p.blend_one(0.,0.,False),0.)

    def test_future_perturbation_no_feature_effect(self):
        dates=pd.date_range('2023-01-01','2024-07-15')
        d=pd.DataFrame({'ds':dates,'shop':'JQ-US','spu':'SYNTHETIC','y':10,'mapped':True,'sales_state':'OBSERVED'})
        m=pd.DataFrame({'month':pd.period_range('2023-01','2024-07',freq='M').astype(str),
                        'shop':'JQ-US','spu':'SYNTHETIC','qty':300})
        obj=pd.DataFrame({'issue_date':['2024-06-17'],'shop':['JQ-US'],'spu':['SYNTHETIC']})
        _,first=p.matrices(d,m,obj,'2024-06-17')
        d.loc[d.ds.gt('2024-06-15'),'y']=999999
        m.loc[m.month.ge('2024-06'),'qty']=999999
        _,second=p.matrices(d,m,obj,'2024-06-17')
        for seg in ['M1','M2']:
            pd.testing.assert_frame_equal(first[seg][0],second[seg][0],check_exact=True)
            pd.testing.assert_frame_equal(first[seg][1],second[seg][1],check_exact=True)

    def test_exclusive_and_tamper(self):
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder)/'receipt.json';p.dump_new(target,{'status':'TEST'})
            sources={};p.protect(target,sources);p.verify_sources(sources)
            with self.assertRaises(FileExistsError):p.dump_new(target,{})
            target.write_text('{}',encoding='utf-8')
            with self.assertRaises(ValueError):p.verify_sources(sources)

    def test_output_scope_and_tolerance(self):
        self.assertEqual(p.assert_numeric([1,np.nan],[1,np.nan]),0)
        with self.assertRaises(ValueError):p.assert_numeric([np.nan],[0])
        with self.assertRaises(ValueError):p.assert_numeric([1],[1.00001])

    def test_sql_boundary_and_scan_accounting(self):
        probe.validate("SELECT dt FROM ods_db.ods_lx_product_performance WHERE dt='2026-01-01'")
        for sql in ['DELETE FROM ods_db.ods_lx_product_performance',
                    'SELECT * FROM ods_db.ods_lx_product_performance',
                    'SELECT * FROM wrong.secret WHERE x=1',
                    'SELECT * FROM ods_db.ods_lx_product_performance WHERE x=1; SELECT 1']:
            with self.assertRaises(ValueError):probe.validate(sql)
        self.assertEqual(probe.scans({'nested':[{'rows_examined_per_scan':15},{'rows_examined_per_scan':25}]}),40)

    def test_normalization_never_fills_unknown(self):
        records=[]
        for name,quantity,null in [('ZERO','0','0'),('MISSING',None,'1'),('POSITIVE','3','0')]:
            r={k:'0' for k in source_audit.NUMERIC}
            r.update(dt='2026-01-01',store_name='JQ-US',spu=name,sales_qty=quantity,null_qty=null,source_rows=1)
            records.append(r)
        d=source_audit.normalize(records).set_index('spu')
        self.assertEqual(d.loc['ZERO','sales_state'],'EXPLICIT_ZERO')
        self.assertEqual(d.loc['MISSING','sales_state'],'UNKNOWN')
        self.assertTrue(pd.isna(d.loc['MISSING','y']))
        self.assertEqual(d.y.sum(),3)

    def test_maturity_not_automatic_or_backdated(self):
        m={'state':'ISSUED_SHADOW_AWAITING_ACTUALS','issue_date':'2026-10-21',
           'prediction_sealed_at':'2026-10-21T18:00:00+08:00'}
        self.assertEqual(maturity.check(m,'2026-10-22T00:00:00+08:00',{})['status'],'NOT_MATURED')
        self.assertEqual(maturity.check(m,'2026-11-22T10:00:00+08:00',{})['status'],'TRUTH_INCOMPLETE')
        self.assertEqual(maturity.check({'state':'BLOCKED_SOURCE_OR_REPRODUCTION'},'2026-12-23T10:00:00+08:00',{})['status'],'NOT_ISSUED')
        e={'complete':True,'sync_completed':True,'reconciliation_passed':True,'mapping_checked':True,
           'immutable_truth_version':True,'source_business_through':'2026-11-20','source_acquired_at':'2026-11-22T09:00:00+08:00',
           'source_sync_evidence_id':'SYNTHETIC_ONLY','truth_sha256':'synthetic','shops':p.SHOPS}
        r=maturity.check(m,'2026-11-22T10:00:00+08:00',{'windows':{'W1':e}})
        self.assertEqual(r['scorable_windows'],['W1'])
        self.assertFalse(r['accuracy_computed'])


if __name__=='__main__':unittest.main()
