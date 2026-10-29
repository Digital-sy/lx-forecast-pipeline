import importlib.util
from pathlib import Path
import unittest
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parents[1]/'scripts/validate_direct_l1.py'
spec=importlib.util.spec_from_file_location('validate_l1',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class IndependentL1Tests(unittest.TestCase):
    def test_only_loss_changes(self):
        a=dict(objective='poisson',num_leaves=31);b=dict(objective='regression_l1',metric='l1',num_leaves=31)
        self.assertEqual(m.verify_configuration(a,b),b)
        with self.assertRaises(ValueError):m.verify_configuration(a,b|dict(num_leaves=32))
    def test_weight_or_manual_initialization_rejected(self):
        a=dict(objective='poisson')
        for k in ['sample_weight','class_weight','init_score']:
            with self.assertRaises(ValueError):m.verify_configuration(a,dict(objective='regression_l1',metric='l1',**{k:1}))
    def frame(self):return pd.DataFrame({f:np.array([1.,2.],dtype=np.float32) for f in m.FEATURES}).assign(y_label=[0.,20.])
    def receipt(self,t):return dict(stage='FIT_RETURNED',rows=len(t),features=m.FEATURES,no_sample_weight=True,fit_kwargs=[],label_dtype=str(t.y_label.dtype),
        no_class_weight=True,no_eval_set=True,no_init_score=True,label_transform='NONE',zero_label_rows=int(t.y_label.eq(0).sum()),
        y_sha256=m.array_sha(t.y_label.to_numpy()),feature_dtypes={c:str(t[c].dtype) for c in m.FEATURES},X_column_sha256={c:m.array_sha(t[c].to_numpy()) for c in m.FEATURES})
    def test_actual_fit_array_order(self):
        t=self.frame();r=self.receipt(t);m.verify_fit_receipt(t,r)
        with self.assertRaises(ValueError):m.verify_fit_receipt(t.iloc[::-1].reset_index(drop=True),r)
    def test_no_weight_keyword(self):
        t=self.frame();r=self.receipt(t);r['fit_kwargs']=['sample_weight']
        with self.assertRaises(ValueError):m.verify_fit_receipt(t,r)
    def test_label_dtype_guard(self):
        t=self.frame();r=self.receipt(t);t.y_label=t.y_label.astype(np.float32)
        with self.assertRaises(ValueError):m.verify_fit_receipt(t,r)
    def test_manual_initialization_or_eval_set_rejected(self):
        t=self.frame()
        for k in ['no_class_weight','no_init_score','no_eval_set']:
            r=self.receipt(t);r[k]=False
            with self.assertRaises(ValueError):m.verify_fit_receipt(t,r)
    def predictions(self):
        i=pd.DataFrame(dict(input_status=['OK','OK','MISSING_HISTORY'],shop=['S']*3,spu=['A','B','C']))
        p=i.copy();p['raw_prediction']=[-2.,3.,np.nan];p['raw_prediction_reloaded']=p.raw_prediction;p['prediction']=[0.,3.,np.nan]
        return i,p
    def test_negative_raw_is_preserved_and_clipped_only(self):
        i,p=self.predictions();r=m.verify_predictions(i,p);self.assertEqual(r['raw_negative_n'],1);self.assertEqual(r['output_n'],2)
        p.loc[0,'prediction']=2.
        with self.assertRaises(ValueError):m.verify_predictions(i,p)
    def test_saved_model_output_must_match(self):
        i,p=self.predictions();p.loc[1,'raw_prediction_reloaded']=3.0000001
        with self.assertRaises(ValueError):m.verify_predictions(i,p)
    def test_missing_not_zero(self):
        i,p=self.predictions();p.loc[2,'prediction']=0
        with self.assertRaises(ValueError):m.verify_predictions(i,p)
    def test_unweighted_zero_and_positive_zero(self):
        r=m.unweighted_metrics([10.,100.,0.],[0.,100.,2.]);self.assertAlmostEqual(r['wape'],12/110)
        self.assertEqual(r['positive_actual_zero_prediction_n'],1);self.assertEqual(r['zero_truth_ae'],2.)
    def test_joint_boundary_unrounded(self):
        self.assertTrue(m.joint_flags([[10,10,20]],[[12,12,24]])[0]);self.assertFalse(m.joint_flags([[10,10,20]],[[12.00001,12,24]])[0])
    def test_zero_and_missing_not_joint_hits(self):
        np.testing.assert_array_equal(m.joint_flags([[0,0,0],[10,10,20]],[[0,0,0],[10,np.nan,20]]),[False,False])
    def test_model_meta_enforces_l1(self):
        text='feature_names='+' '.join(m.FEATURES)+'\n[objective: regression_l1]\n[metric: l1]\n[num_leaves: 31]\n[boost_from_average: 1]\n[weight_column: ]\n'
        m.verify_model_text(text,dict(objective='regression_l1',metric='l1',num_leaves=31))
        with self.assertRaises(ValueError):m.verify_model_text(text.replace('regression_l1','poisson'),dict(objective='regression_l1',metric='l1',num_leaves=31))

if __name__=='__main__':unittest.main()
