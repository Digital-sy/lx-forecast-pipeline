import importlib.util,json,tempfile,unittest,sys
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
import direct_core_weight2_v1 as m

class WeightTests(unittest.TestCase):
    def test_weights_ratio_and_mean(self):
        r,w=m.normalized_weights(['CORE','NON_CORE','UNKNOWN','CORE']);np.testing.assert_array_equal(r,[2,1,1,2]);self.assertAlmostEqual(w.mean(),1);self.assertAlmostEqual(w[0]/w[1],2)
    def test_all_unknown_kept_positive(self):
        _,w=m.normalized_weights(['UNKNOWN']*3);np.testing.assert_array_equal(w,[1,1,1])
    def test_invalid_state_not_unknown(self):
        for s in [None,'BAD',np.nan]:
            with self.assertRaises(ValueError):m.normalized_weights(['CORE',s])
    def test_labels_never_enter_weight_function(self):
        states=['CORE','NON_CORE'];a=m.normalized_weights(states)[1]
        labels=[0,1000000];labels.reverse();np.testing.assert_array_equal(a,m.normalized_weights(states)[1])
    def frame(self):
        return pd.DataFrame(dict(shop=['S','S'],spu=['A','B'],simulated_issue_date=pd.to_datetime(['2024-04-02']*2),input_cutoff=pd.to_datetime(['2024-03-31']*2)))
    def test_order_alignment(self):
        t=self.frame();dates=pd.date_range('2024-03-31',periods=1);lookup={('S','A'):(np.array([1]),None),('S','B'):(np.array([0]),None)}
        w=m.aligned_weights(t,lookup,dates);self.assertEqual(w.core_state.tolist(),['CORE','NON_CORE'])
        with self.assertRaises((ValueError,AssertionError)):m.check_alignment(t,w.iloc[::-1])
    def test_join_failure_is_error(self):
        with self.assertRaises(ValueError):m.aligned_weights(self.frame(),{},pd.date_range('2024-03-31',periods=1))
    def test_duplicate_keys_error(self):
        t=pd.concat([self.frame(),self.frame()]);
        with self.assertRaises(ValueError):m.aligned_weights(t,{},pd.date_range('2024-03-31',periods=1))
    def test_same_raw_states_different_normalization(self):
        r1,w1=m.normalized_weights(['CORE','NON_CORE','NON_CORE']);r2,w2=m.normalized_weights(['CORE','NON_CORE'])
        np.testing.assert_array_equal(r1[:2],r2);self.assertNotEqual(w1[0],w2[0])
    def test_actual_fit_forward_exact_args(self):
        class Spy:
            def fit(self,X,y,**kw):self.X=X;self.y=y;self.kw=kw;return self
        X=pd.DataFrame({f:np.array([1,2],dtype=np.float32) for f in m.FEATURES});y=pd.Series([0.,9999.]);_,w=m.normalized_weights(['CORE','NON_CORE']);spy=Spy()
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'received.json';m.fit_forward(spy,X,y,w,m.array_sha(w),p);rec=json.loads(p.read_text())
            self.assertIs(spy.kw['sample_weight'],w);self.assertEqual(set(spy.kw),{'sample_weight'});self.assertEqual(rec['stage'],'FIT_RETURNED')
            self.assertEqual(rec['sample_weight_sha256'],m.array_sha(w));self.assertIs(spy.X,X);self.assertIs(spy.y,y)
    def test_failure_not_marked_fit_returned(self):
        class Broken:
            def fit(self,*a,**k):raise RuntimeError('synthetic')
        X=pd.DataFrame({f:[1.] for f in m.FEATURES});y=pd.Series([1.]);w=np.array([1.])
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'received.json'
            with self.assertRaises(RuntimeError):m.fit_forward(Broken(),X,y,w,m.array_sha(w),p)
            self.assertEqual(json.loads(p.read_text())['stage'],'RECEIVED_AT_FIT_INTERFACE')
    def test_wrong_features_or_weight_blocked_before_fit(self):
        class Never:
            def fit(self,*a,**k):raise AssertionError('must not call')
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):m.fit_forward(Never(),pd.DataFrame({'item_key':[0]}),pd.Series([1.]),np.array([1.]),'BAD',Path(tmp)/'x')
    def test_future_perturbation_same_raw_state(self):
        d=pd.DataFrame(dict(shop='S',spu='A',ds=pd.date_range('2024-01-01',periods=95),y=1.,mapped=True,sales_state='OBSERVED'))
        dates,lookup,_=m.audit.historical_core(d);t=pd.DataFrame(dict(shop=['S'],spu=['A'],simulated_issue_date=[dates[89]+pd.Timedelta(days=2)],input_cutoff=[dates[89]]))
        w=m.aligned_weights(t,lookup,dates);d.loc[d.ds>dates[89],'y']=99999;dates,b,_=m.audit.historical_core(d)
        pd.testing.assert_frame_equal(w,m.aligned_weights(t,b,dates))

if __name__=='__main__':unittest.main()
