import importlib.util
from pathlib import Path
import sys
import unittest
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
import score_direct_l1 as s

def example():
    base=pd.DataFrame([dict(issue_date='2025-01-02',shop='S',spu='A',window=w,model='DIRECT',actual=a,prediction=1.,target_start=start,target_end=end)
        for w,a,start,end in [('d01_30',10,'2025-01-03','2025-02-01'),('d31_60',20,'2025-02-02','2025-03-03'),('d01_60',30,'2025-01-03','2025-03-03')]])
    seg=pd.DataFrame([dict(issue_date='2025-01-02',shop='S',spu='A',segment=k,prediction=p,raw_prediction=p,prediction_status='OK',label_start=a,label_end=b)
        for k,p,a,b in [('M1',11.,'2025-01-03','2025-02-01'),('M2',22.,'2025-02-02','2025-03-03')]])
    return base,seg

class ScoringTests(unittest.TestCase):
    def test_additive_sum(self):
        b,z=example();r=s.assemble_candidate(b,z).set_index('window')
        self.assertEqual(r.loc['d01_60','prediction'],33.);self.assertEqual(len(r),3)
    def test_truth_does_not_change_candidate(self):
        b,z=example();x=s.assemble_candidate(b,z);b['actual']=100000
        y=s.assemble_candidate(b,z);np.testing.assert_array_equal(x.prediction,y.prediction)
    def test_duplicate_rejected(self):
        b,z=example()
        with self.assertRaisesRegex(ValueError,'DUPLICATE'):s.assemble_candidate(b,pd.concat([z,z.iloc[:1]]))
    def test_wrong_object_rejected(self):
        b,z=example();z.loc[0,'spu']='B'
        with self.assertRaisesRegex(ValueError,'SCOPE'):s.assemble_candidate(b,z)
    def test_changed_dates_rejected(self):
        b,z=example();z.loc[0,'label_end']='2025-02-02'
        with self.assertRaisesRegex(ValueError,'DATE'):s.assemble_candidate(b,z)
    def test_missing_segment_keeps_missing_cumulative(self):
        b,z=example();z.loc[1,['prediction','raw_prediction']]=np.nan;z.loc[1,'prediction_status']='MISSING_HISTORY'
        r=s.assemble_candidate(b,z).set_index('window');self.assertTrue(pd.isna(r.loc['d01_60','prediction']))
        self.assertEqual(r.loc['d01_30','prediction'],11.)
    def test_unweighted_metrics_boundary(self):
        b,z=example();b.prediction=[12,16,36];b['sample_weight']=[100000,0.001,1]
        r=s.metrics(b);self.assertEqual(r['hit_n'],3);self.assertAlmostEqual(r['wape'],12/60)
    def test_joint_zero_actual_never_percentage_hit(self):
        b,z=example();b['actual']=0;b['prediction']=0
        r=s.objects(b);self.assertFalse(r.joint_hit.iloc[0]);self.assertTrue(r.output_complete.iloc[0])
    def test_negative_raw_clip_is_per_segment_before_addition(self):
        b,z=example();z.loc[0,['raw_prediction','prediction']]=[-3.,0.]
        r=s.assemble_candidate(b,z).set_index('window')
        self.assertEqual(r.loc['d01_60','prediction'],22.)
        self.assertEqual(r.loc['d01_60','raw_prediction'],19.)
        self.assertEqual(r.loc['d01_60','raw_negative_segment_count'],1)
    def test_changed_postprocess_rejected(self):
        b,z=example();z.loc[0,'raw_prediction']=-3.
        with self.assertRaisesRegex(ValueError,'POSTPROCESS'):s.assemble_candidate(b,z)
    def test_infinite_output_rejected(self):
        b,z=example();z.loc[0,['prediction','raw_prediction']]=np.inf
        with self.assertRaisesRegex(ValueError,'INFINITE'):s.assemble_candidate(b,z)
    def test_positive_actual_zero_prediction_separate_from_missing(self):
        b,z=example();b['prediction']=[0,np.nan,5]
        r=s.metrics(b)
        self.assertEqual(r['positive_actual_zero_prediction_n'],1)
        self.assertEqual(r['positive_actual_zero_prediction_actual'],10)
        self.assertEqual(r['missing_output_n'],1)
        self.assertEqual(r['prediction_units'],5)
    def test_hit_sales_weight_preserved_not_training_weight(self):
        b,z=example();b['prediction']=[10.,40.,30.];b['sample_weight']=[.01,10000,.01]
        r=s.metrics(b);self.assertEqual(r['hit_actual'],40.)
        self.assertEqual(r['hit_actual_share'],40/60)

if __name__=='__main__':unittest.main()
