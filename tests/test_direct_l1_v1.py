import importlib.util,json,tempfile,sys,unittest
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
import direct_l1_v1 as m

class L1Tests(unittest.TestCase):
    def original(self):return dict(objective='poisson',n_estimators=200,random_state=20261019,learning_rate=.05)
    def frame(self):return pd.DataFrame({f:np.array([1,2],dtype=np.float32) for f in m.FEATURES})
    def test_objective_really_changes_and_metric(self):
        o=self.original();p,d=m.l1_configuration(o)
        self.assertEqual(p['objective'],'regression_l1');self.assertEqual(p['metric'],'l1');self.assertEqual(o['objective'],'poisson')
        self.assertEqual(set(d['changes']),{'objective','metric'})
        for k in o.keys()-{'objective'}:self.assertEqual(o[k],p[k])
    def test_poisson_only_parameter_removed(self):
        o=self.original();o['poisson_max_delta_step']=.7;p,d=m.l1_configuration(o)
        self.assertNotIn('poisson_max_delta_step',p);self.assertEqual(d['explicit_poisson_parameters_removed'],{'poisson_max_delta_step':.7})
    def test_wrong_baseline_rejected(self):
        with self.assertRaises(ValueError):m.l1_configuration({'objective':'regression_l1'})
    def test_aliases_and_hidden_weights_rejected(self):
        for key in ['loss','application','objective_type','metrics','sample_weight','class_weight','weight_column','init_score','init_model']:
            o=self.original();o[key]='BAD'
            with self.assertRaises(ValueError):m.l1_configuration(o)
    def test_actual_fit_receives_no_kwargs_and_raw_labels(self):
        class Spy:
            def fit(self,X,y,**kwargs):self.X=X;self.y=y;self.kwargs=kwargs;return self
        X=self.frame();y=pd.Series([0.,1000000.]);s=Spy()
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'fit.json';m.fit_forward(s,X,y,path);r=json.loads(path.read_text())
            self.assertIs(s.X,X);self.assertIs(s.y,y);self.assertEqual(s.kwargs,{})
            self.assertEqual(r['fit_kwargs'],[]);self.assertTrue(r['no_sample_weight']);self.assertEqual(r['zero_label_rows'],1)
            self.assertEqual(r['y_sha256'],m.array_sha(y.to_numpy()));self.assertEqual(r['stage'],'FIT_RETURNED')
    def test_wrong_feature_order_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):m.fit_forward(None,self.frame()[m.FEATURES[::-1]],pd.Series([0.,1.]),Path(d)/'r')
    def test_failed_fit_not_success(self):
        class Broken:
            def fit(self,*a,**kw):raise RuntimeError('synthetic')
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'r'
            with self.assertRaises(RuntimeError):m.fit_forward(Broken(),self.frame(),pd.Series([0.,1.]),p)
            self.assertEqual(json.loads(p.read_text())['stage'],'RECEIVED_AT_FIT_INTERFACE')
    def test_metadata_checks_objective_not_just_metric(self):
        t='objective=poisson\n[objective: poisson]\n[metric: l1]\n'
        with self.assertRaises(ValueError):m.model_metadata(t)
    def test_metadata_accepts_inactive_poisson_default(self):
        t='objective=regression_l1\n[objective: regression_l1]\n[metric: l1]\n[poisson_max_delta_step: 0.7]\n'
        r=m.model_metadata(t);self.assertEqual(r['poisson_parameter_effect'],'INACTIVE_FOR_L1')
    def test_prediction_call_count_no_repeat(self):
        class Fake:
            def predict(self,X):return np.array([-0.2,2.4])
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'native_prediction_receipt.json';v=m.prediction_call(Fake(),self.frame(),p,'native')
            np.testing.assert_array_equal(v,[-.2,2.4]);np.testing.assert_array_equal(np.maximum(v,0),[0,2.4])
            with self.assertRaises(FileExistsError):m.prediction_call(Fake(),self.frame(),p,'native')
    def test_raw_missing_and_negative_separate(self):
        x=np.array([np.nan,-1.,0.,1.2]);y=np.maximum(x,0)
        self.assertTrue(np.isnan(y[0]));np.testing.assert_array_equal(y[1:],[0,0,1.2])
    def test_no_new_feature_or_weight_input(self):
        self.assertEqual(len(m.FEATURES),10)
        self.assertFalse({'sample_weight','core_state','item_key','T7'}.intersection(m.FEATURES))

if __name__=='__main__':unittest.main()
