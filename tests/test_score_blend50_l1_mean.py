import sys
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import score_blend50_l1_mean as s

def example():
    rows=[];candidate=[]
    for w,a,p,q,start,end in [('d01_30',10,8.,12.,'2025-01-03','2025-02-01'),('d31_60',20,24.,16.,'2025-02-02','2025-03-03'),('d01_60',30,32.,28.,'2025-01-03','2025-03-03')]:
        k=dict(issue_date='2025-01-02',shop='S',spu='A',window=w,target_start=start,target_end=end)
        rows.append(k|dict(model='DIRECT',actual=a,prediction=3.,prediction_status='OK'))
        candidate.append(k|dict(blend50_prediction=p,l1_prediction=q,prediction=.5*p+.5*q,prediction_status='OK'))
    return pd.DataFrame(rows),pd.DataFrame(candidate)

class ScoreTests(unittest.TestCase):
    def test_mean_and_additive(self):
        b,c=example();p=s.assemble_candidate(b,c).set_index('window')
        self.assertEqual(p.loc['d01_60','prediction'],30.)
    def test_target_truth_perturb_does_not_change_predictions(self):
        b,c=example();x=s.assemble_candidate(b,c).prediction;b.actual=9999
        np.testing.assert_array_equal(x,s.assemble_candidate(b,c).prediction)
    def test_duplicate_refused(self):
        b,c=example()
        with self.assertRaisesRegex(ValueError,'DUPLICATE'):s.assemble_candidate(b,pd.concat([c,c.iloc[:1]]))
    def test_date_refused(self):
        b,c=example();c.loc[0,'target_end']='2025-02-02'
        with self.assertRaisesRegex(ValueError,'WINDOW'):s.assemble_candidate(b,c)
    def test_scope_refused(self):
        b,c=example();c.loc[0,'spu']='B'
        with self.assertRaisesRegex(ValueError,'SCOPE'):s.assemble_candidate(b,c)
    def test_missing_prediction_stays_unknown(self):
        b,c=example();c.loc[0,['l1_prediction','prediction']]=np.nan;c.loc[2,['l1_prediction','prediction']]=np.nan
        q=s.assemble_candidate(b,c).set_index('window');self.assertTrue(pd.isna(q.loc['d01_60','prediction']))
        self.assertEqual(q.loc['d31_60','prediction'],20.)
    def test_wrong_mean_refused(self):
        b,c=example();c.loc[0,'prediction']=11
        with self.assertRaisesRegex(ValueError,'MEAN'):s.assemble_candidate(b,c)
    def test_wrong_cumulative_refused(self):
        b,c=example();c.loc[2,'prediction']=31
        with self.assertRaisesRegex(ValueError,'CUMULATIVE'):s.assemble_candidate(b,c)
    def test_no_weighted_scoring(self):
        b,c=example();b['prediction']=[12.,40.,36.];b['sample_weight']=[1.,10000.,1.]
        m=s.metrics(b);self.assertEqual(m['ae'],28.);self.assertEqual(m['hit_n'],2)
    def test_shared_hits_remain_joint(self):
        b,c=example();p=s.assemble_candidate(b,c)
        self.assertTrue(s.objects(p).joint_hit.iloc[0])
    def test_zero_actual_is_not_percentage_hit(self):
        b,c=example();b.actual=0;b.prediction=0
        self.assertFalse(s.objects(b).joint_hit.iloc[0])
    def test_cancellation_does_not_manufacture_joint_hit(self):
        b,c=example();b['model']=s.C;b['prediction']=[15,15,30]
        z=s.objects(b);z['paired']=True;r=s.cancellation_frame(z).iloc[0]
        self.assertEqual(r.segment_ae,10);self.assertEqual(r.cumulative_ae,0);self.assertEqual(r.cancellation,10)
        self.assertTrue(r.cumulative_hit_but_segment_miss);self.assertFalse(r.joint_hit)
    def test_same_direction_has_no_cancellation(self):
        b,c=example();b['model']=s.C;b['prediction']=[15,25,40]
        z=s.objects(b);z['paired']=True;r=s.cancellation_frame(z).iloc[0]
        self.assertEqual(r.cancellation,0)

if __name__=='__main__':unittest.main()
