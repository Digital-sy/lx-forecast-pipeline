import importlib.util, tempfile
from pathlib import Path
import unittest
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parents[1]/'scripts/validate_blend50_l1_mean.py'
spec=importlib.util.spec_from_file_location('vmean',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def endpoints():
    b=pd.DataFrame(dict(issue_date=['2025-01-02']*2,shop=['S']*2,spu=['A','B'],input_cutoff=pd.to_datetime(['2024-12-31']*2),prediction_M1=[10.,np.nan],prediction_M2=[20.,np.nan],prediction_60=[30.,np.nan]))
    for w,a,z in [('d01_30',1,30),('d31_60',31,60),('d01_60',1,60)]:
        b['target_start_'+w]=pd.to_datetime(b.issue_date)+pd.Timedelta(days=a);b['target_end_'+w]=pd.to_datetime(b.issue_date)+pd.Timedelta(days=z)
    rows=[]
    for seg,w,qty in [('M1','d01_30',30.),('M2','d31_60',10.)]:
        q=b[m.KEY+['input_cutoff']].copy();q['segment']=seg;q['prediction']=[qty,np.nan];q['label_start']=b['target_start_'+w];q['label_end']=b['target_end_'+w];rows.append(q)
    return b,pd.concat(rows,ignore_index=True)

class IndependentMeanTests(unittest.TestCase):
    def test_exact_fixed_mean_and_segment_sum(self):
        b,l=endpoints();r=m.independent_candidate(b,l);q=r[r.spu.eq('A')].set_index('window')
        self.assertEqual(q.loc['d01_30','prediction'],20.);self.assertEqual(q.loc['d31_60','prediction'],15.);self.assertEqual(q.loc['d01_60','prediction'],35.)
        self.assertTrue(r[r.spu.eq('B')].prediction.isna().all())
    def test_no_truth_dependency(self):
        b,l=endpoints();r=m.independent_candidate(b,l);b['actual']=1e9;l['error']=-999
        pd.testing.assert_frame_equal(r,m.independent_candidate(b,l),check_exact=True)
    def test_duplicate_endpoint_rejected(self):
        b,l=endpoints()
        with self.assertRaises(ValueError):m.independent_candidate(pd.concat([b,b.iloc[:1]]),l)
    def test_missing_endpoint_not_silent_fallback(self):
        b,l=endpoints();l.loc[l.spu.eq('A')&l.segment.eq('M1'),'prediction']=np.nan
        with self.assertRaises(ValueError):m.independent_candidate(b,l)
    def test_wrong_dates_rejected(self):
        b,l=endpoints();l.loc[0,'label_start']+=pd.Timedelta(days=1)
        with self.assertRaises(ValueError):m.independent_candidate(b,l)
    def test_negative_and_infinite_rejected(self):
        for value in [-1.,np.inf]:
            b,l=endpoints();l.loc[0,'prediction']=value
            with self.assertRaises(ValueError):m.independent_candidate(b,l)
    def test_common_hits_preserved(self):
        y=np.array([[10.,10.,20.]]);b=np.array([[8.,12.,20.]]);l=np.array([[12.,8.,20.]])
        self.assertTrue(m.joint_flags(y,.5*b+.5*l)[0])
    def test_neither_endpoint_can_mean_hit(self):
        y=np.array([[10.,10.,20.]]);b=np.array([[5.,5.,10.]]);l=np.array([[15.,15.,30.]])
        self.assertFalse(m.joint_flags(y,b)[0]);self.assertFalse(m.joint_flags(y,l)[0]);self.assertTrue(m.joint_flags(y,.5*b+.5*l)[0])
    def test_convexity_and_bias(self):
        y=[10.,0.,20.];b=np.array([20.,5.,10.]);l=np.array([0.,1.,30.]);r=m.convexity(y,b,l,.5*b+.5*l)
        self.assertLessEqual(r['ae_candidate'],r['ae_upper']);self.assertEqual(r['signed_error_sum'],r['signed_error_endpoint_mean'])
        with self.assertRaises(ValueError):m.convexity(y,b,l,np.array([99.,99.,99.]))
    def test_cancellation_is_separate_from_segment_gain(self):
        y=np.array([[10.,10.,20.]]);p=np.array([[15.,5.,20.]])
        valid,e1,e2,sa,ca,cc=m.cancellation_arrays(y,p);self.assertEqual(sa[0],10);self.assertEqual(ca[0],0);self.assertEqual(cc[0],10)
    def test_cancellation_summary_keeps_unknown_and_counts_segment_miss(self):
        y=np.array([[10,10,20],[10,10,20],[0,0,0]],float);p=np.array([[15,5,20],[np.nan,np.nan,np.nan],[0,0,0]])
        q=m.cancellation_summary(y,p)
        self.assertEqual(q['records'],3);self.assertEqual(q['scored_n'],2);self.assertEqual(q['unknown_n'],1)
        self.assertEqual(q['opposite_segment_signs_n'],1);self.assertEqual(q['cumulative_hit_but_segment_miss_n'],1)
        self.assertEqual(q['cancellation'],10)
    def test_zero_truth_not_relative_hit(self):
        self.assertFalse(m.joint_flags([[0,0,0]],[[0,0,0]])[0]);self.assertIsNone(m.metrics([0.],[1.])['wape'])
    def test_boundary_unrounded(self):
        self.assertTrue(m.joint_flags([[10,10,20]],[[12,12,24]])[0]);self.assertFalse(m.joint_flags([[10,10,20]],[[12.000001,12,24]])[0])
    def test_no_model_or_network_entry(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'candidate.py';p.write_text('import numpy as np\ndef mean(a,b):return .5*a+.5*b\n')
            self.assertEqual(m.source_code_review(p)['status'],'PASS');p.write_text('import lightgbm\n')
            with self.assertRaises(ValueError):m.source_code_review(p)

if __name__=='__main__':unittest.main()
