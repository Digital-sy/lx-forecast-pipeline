"""Synthetic, truth-free fixed combination tests; no model or source calls."""
import sys
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.blend50_l1_mean import combine,WINDOWS,mature_core


def fixture():
    key=dict(issue_date='2025-01-02',shop='SHOP',spu='ITEM',input_cutoff=pd.Timestamp('2024-12-31'))
    b=key|dict(prediction_status='OK',prediction_M1=10.,prediction_M2=30.,prediction_60=40.)
    for w,lo,hi in [('d01_30',1,30),('d31_60',31,60),('d01_60',1,60)]:
        b['target_start_'+w]=pd.Timestamp(key['issue_date'])+pd.Timedelta(days=lo)
        b['target_end_'+w]=pd.Timestamp(key['issue_date'])+pd.Timedelta(days=hi)
    l=[key|dict(segment=s,label_start=b['target_start_'+w],label_end=b['target_end_'+w],prediction=v,prediction_status='OK')
       for s,w,v in [('M1','d01_30',30.),('M2','d31_60',10.)]]
    return pd.DataFrame([b]),pd.DataFrame(l)


class MeanTests(unittest.TestCase):
    def test_fixed_mature_periods_preserve_future_core(self):
        dates=pd.date_range('2025-01-01','2026-10-01',freq='MS')+pd.Timedelta(days=1)
        core=pd.DataFrame({'issue_date':dates.astype(str),'shop':'SHOP','spu':'ITEM'})
        mature,pending=mature_core(core)
        self.assertEqual(len(mature),20);self.assertEqual(len(pending),2)
        self.assertEqual(len(core),22)
    def test_fixed_formula_and_sum(self):
        b,l=fixture();r=combine(b,l).set_index('window')
        self.assertEqual(r.loc['d01_30','prediction'],20.)
        self.assertEqual(r.loc['d31_60','prediction'],20.)
        self.assertEqual(r.loc['d01_60','prediction'],40.)
    def test_no_truth_projection(self):
        b,l=fixture();a=combine(b,l);b['actual']=999999.;l['error']=-99999
        pd.testing.assert_frame_equal(a,combine(b,l))
    def test_missing_not_zero(self):
        b,l=fixture();b[['prediction_M1','prediction_M2','prediction_60']]=np.nan;b['prediction_status']='ORIGINAL_MISSING_OUTPUT'
        l['prediction']=np.nan;l['prediction_status']='INSUFFICIENT_QUALIFICATION'
        self.assertTrue(combine(b,l).prediction.isna().all())
    def test_unequal_output_masks_block(self):
        b,l=fixture();l.loc[0,'prediction']=np.nan;l.loc[0,'prediction_status']='MISSING'
        with self.assertRaisesRegex(ValueError,'OUTPUT_MASK'):combine(b,l)
    def test_duplicate_and_scope(self):
        b,l=fixture()
        with self.assertRaisesRegex(ValueError,'DUPLICATE'):combine(b,pd.concat([l,l]))
        l.loc[0,'spu']='OTHER'
        with self.assertRaisesRegex(ValueError,'SCOPE'):combine(b,l)
    def test_dates_and_cutoff(self):
        b,l=fixture();l.loc[0,'label_end']+=pd.Timedelta(days=1)
        with self.assertRaisesRegex(ValueError,'DATE'):combine(b,l)
    def test_final_nonnegative_required(self):
        b,l=fixture();l.loc[0,'prediction']=-1
        with self.assertRaisesRegex(ValueError,'NONFINAL'):combine(b,l)
    def test_status_mismatch_blocks(self):
        b,l=fixture();l.loc[0,'prediction_status']='MISSING'
        with self.assertRaisesRegex(ValueError,'STATUS'):combine(b,l)
    def test_distinct_shops_and_endpoint_bounds(self):
        b,l=fixture();b2=b.copy();l2=l.copy();b2['shop']='SECOND';l2['shop']='SECOND';l2['prediction']*=2
        r=combine(pd.concat([b,b2]),pd.concat([l,l2]))
        self.assertEqual(len(r),6)
        self.assertTrue((r.prediction>=r[['blend50_prediction','l1_prediction']].min(axis=1)).all())
        self.assertTrue((r.prediction<=r[['blend50_prediction','l1_prediction']].max(axis=1)).all())
    def test_old_sum_mismatch_block(self):
        b,l=fixture();b['prediction_60']=42
        with self.assertRaisesRegex(ValueError,'SUM'):combine(b,l)


if __name__=='__main__':unittest.main()
