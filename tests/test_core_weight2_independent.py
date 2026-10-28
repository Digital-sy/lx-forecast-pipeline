import importlib.util
from pathlib import Path
import unittest
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parents[1]/'scripts/validate_core_weight2.py'
spec=importlib.util.spec_from_file_location('validate',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class IndependentWeightTests(unittest.TestCase):
    def test_normalization_and_unknown(self):
        raw,w=m.expected_weights(['CORE','NON_CORE','UNKNOWN']);np.testing.assert_array_equal(raw,[2.,1.,1.]);self.assertEqual(w.mean(),1.)
        self.assertEqual(w[0]/w[1],2.);self.assertTrue((w>0).all())
    def test_join_failure_not_unknown(self):
        with self.assertRaises(ValueError):m.expected_weights(['CORE',None])
    def test_label_does_not_enter_weights(self):
        raw,w=m.expected_weights(['CORE','NON_CORE']);y=np.array([0.,1e9])
        raw2,w2=m.expected_weights(['CORE','NON_CORE']);np.testing.assert_array_equal(w,w2);self.assertGreater(w[0],w[1])
    def test_row_alignment(self):
        t=pd.DataFrame({'shop':['A','A'],'spu':['X','Y'],'simulated_issue_date':pd.to_datetime(['2024-01-01']*2),'input_cutoff':pd.to_datetime(['2023-12-30']*2)})
        w=t.copy();w['row_index']=[0,1];w['core_state']=['CORE','NON_CORE'];w['raw_weight'],w['sample_weight']=m.expected_weights(w.core_state)
        m.validate_weight_rows(t,w)
        with self.assertRaises(ValueError):m.validate_weight_rows(t,w.iloc[::-1].reset_index(drop=True))
    def test_joint_zero_and_missing_not_hit(self):
        y=np.array([[10.,10.,20.],[0.,0.,0.],[10.,10.,20.]])
        p=np.array([[12.,12.,24.],[0.,0.,0.],[10.,np.nan,20.]])
        np.testing.assert_array_equal(m.joint_flags(y,p),[True,False,False])
    def test_unrounded_twenty_percent(self):
        self.assertTrue(m.joint_flags([[10,10,20]],[[12,12,24]])[0])
        self.assertFalse(m.joint_flags([[10,10,20]],[[12.000001,12,24]])[0])
    def test_scoring_is_unweighted(self):
        a=m.unweighted_metrics([10,100],[20,100]);self.assertAlmostEqual(a['wape'],10/110);self.assertEqual(a['over_units'],10)
    def test_zero_denominator_not_wape_zero(self):
        a=m.unweighted_metrics([0,0],[1,2]);self.assertIsNone(a['wape']);self.assertEqual(a['zero_truth_ae'],3)
    def test_future_changes_do_not_change_core(self):
        dates=pd.date_range('2024-01-01',periods=95);d=pd.DataFrame(dict(ds=dates,shop='S',spu='A',y=1.,mapped=True,sales_state='OBSERVED'))
        state=m.direct_core_state(d,'S','A',dates[89]);d.loc[d.ds>dates[89],'y']=999999
        self.assertEqual(state,m.direct_core_state(d,'S','A',dates[89]))
    def test_insufficient_history_unknown(self):
        dates=pd.date_range('2024-01-01',periods=89);d=pd.DataFrame(dict(ds=dates,shop='S',spu='A',y=1.,mapped=True,sales_state='OBSERVED'))
        self.assertEqual(m.direct_core_state(d,'S','A',dates[-1]),'UNKNOWN')
    def test_mixed_mapping_requires_review(self):
        dates=pd.date_range('2024-01-01',periods=90)
        parts=[]
        for spu,units in [('A',5.),('B',95.)]:
            parts.append(pd.DataFrame(dict(ds=dates,shop='S',spu=spu,y=units,mapped=True,sales_state='OBSERVED')))
        d=pd.concat(parts,ignore_index=True)
        d.loc[d.spu.eq('B') & d.ds.ne(dates[0]),'mapped']=False
        with self.assertRaises(ValueError):m.mapping_evidence(d)

if __name__=='__main__':unittest.main()
