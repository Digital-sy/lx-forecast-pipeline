import contextlib,json,sys,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import blend50_l1_server_issue as r

class FakeBooster:
    def __init__(self,model_str=None,features=None):self.features=json.loads(model_str)['features'] if model_str else features
    def save_model(self,path):Path(path).write_text(json.dumps({'features':self.features}),encoding='utf-8')
    def feature_name(self):return self.features
    def predict(self,X,**kwargs):return np.arange(len(X),dtype=float)-1

class FakeRegressor:
    fits=0;last_params=None
    def __init__(self,**params):self.params=params;FakeRegressor.last_params=params
    def fit(self,X,y):FakeRegressor.fits+=1;self.booster_=FakeBooster(features=list(X.columns));return self
    def predict(self,X):return np.arange(len(X),dtype=float)-1

def slot_fixture(root,support=True):
    prep=root/'prepared';folder=prep/'M1';folder.mkdir(parents=True)
    cutoff=pd.Timestamp('2026-10-27');features=r.direct.FEATURES
    inf=pd.DataFrame({k:[1.,2.] for k in features})
    inf['shop']=['S','S'];inf['spu']=['A','B'];inf['input_status']=['OK','NO_HISTORY']
    train=inf[features].copy();train['y_label']=[1.,5.]
    train['input_cutoff']=pd.Timestamp('2026-07-31');train['history_end']=train.input_cutoff
    train['simulated_issue_date']=pd.Timestamp('2026-08-02');train['label_end']=pd.Timestamp('2026-09-01')
    for prefix in ['direct','annual']:
        inf.to_parquet(folder/(prefix+'_inference.parquet'),index=False)
        train.to_parquet(folder/(prefix+'_train.parquet'),index=False)
    meta={'direct_support':'TRAINABLE_REPLAY_ONLY' if support else 'NOT_TRAINABLE',
          'annual_support':{'fit_allowed':support},'files':{p.name:r.p.sha(p) for p in folder.iterdir()}}
    r.p.dump_new(folder/'manifest.json',meta)
    conf={'issue':'2026-10-29','cutoff':str(cutoff.date()),'lightgbm':{'objective':'poisson','n_estimators':2,'random_state':42,'n_jobs':2},
          'source_gate_sha256':'source','release_manifest_sha256':'release','sources':{}}
    r.p.dump_new(prep/'config.json',conf);return conf

class RunnerTests(unittest.TestCase):
    def test_l1_parameter_delta_only(self):
        old={'objective':'poisson','metric':'poisson','n_estimators':200,'random_state':3}
        new=r.model_parameters(old,'l1');self.assertEqual(new,dict(old,objective='regression_l1',metric='l1'))
        self.assertEqual(r.model_parameters(old,'direct'),old);self.assertEqual(old['objective'],'poisson')
    def test_gate_false_still_blends_l1(self):
        w=pd.DataFrame({'blend_M1':[100.],'blend_M2':[200.],'gate_decision':[False]})
        z=r.combine_l1(w,{'M1':[20.],'M2':[40.]})
        self.assertEqual(z.candidate_M1.iloc[0],60.);self.assertEqual(z.candidate_60.iloc[0],180.)
    def test_missing_l1_no_endpoint_fallback(self):
        w=pd.DataFrame({'blend_M1':[100.],'blend_M2':[200.]})
        z=r.combine_l1(w,{'M1':[np.nan],'M2':[40.]})
        self.assertTrue(pd.isna(z.candidate_60.iloc[0]));self.assertEqual(z.candidate_M2.iloc[0],120.)
    def test_invalid_endpoint_rejected(self):
        w=pd.DataFrame({'blend_M1':[1.],'blend_M2':[2.]})
        with self.assertRaisesRegex(ValueError,'INVALID_L1'):r.combine_l1(w,{'M1':[-1.],'M2':[2.]})
    def test_label_cutoff_rejected(self):
        x=pd.DataFrame({'label_end':[pd.Timestamp('2030-01-01')],'input_cutoff':[pd.Timestamp('2026-01-01')],
          'simulated_issue_date':[pd.Timestamp('2026-01-03')],'history_end':[pd.Timestamp('2026-01-01')]})
        with self.assertRaisesRegex(ValueError,'IMMATURE'):r.label_check(x,pd.Timestamp('2026-10-27'))
    def test_exclusive_lock_prevents_parallel(self):
        with tempfile.TemporaryDirectory() as t:
            with r.exclusive(Path(t),'fit'):
                with self.assertRaises(FileExistsError):
                    with r.exclusive(Path(t),'fit'):pass
            self.assertFalse((Path(t)/'.operation.lock').exists())
    def test_actual_fit_function_uses_l1_and_reload(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);conf=slot_fixture(root);FakeRegressor.fits=0
            fake=types.SimpleNamespace(LGBMRegressor=FakeRegressor,Booster=FakeBooster)
            with patch.object(r,'phase_config',return_value=conf),patch.dict(sys.modules,{'lightgbm':fake}):r.fit(root,'l1_M1')
            receipt=r.p.read(root/'effects/l1_M1/result.json');self.assertEqual(FakeRegressor.fits,1)
            self.assertEqual(FakeRegressor.last_params['objective'],'regression_l1')
            self.assertEqual(receipt['native_prediction_calls'],1);self.assertEqual(receipt['reload_prediction_calls'],1)
            self.assertTrue(receipt['save_reload_exact']);self.assertEqual(receipt['sample_weight'],'NOT_PASSED')
            q=pd.read_parquet(root/'effects/l1_M1/predictions.parquet');self.assertEqual(q.prediction_raw.iloc[0],-1.)
            self.assertEqual(q.prediction.iloc[0],0.);self.assertTrue(pd.isna(q.prediction.iloc[1]))
            with patch.object(r,'phase_config',return_value=conf),patch.dict(sys.modules,{'lightgbm':fake}):r.fit(root,'l1_M1')
            self.assertEqual(FakeRegressor.fits,1)
    def test_normal_support_skip_no_fit(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);conf=slot_fixture(root,support=False);FakeRegressor.fits=0
            with patch.object(r,'phase_config',return_value=conf),patch.dict(sys.modules,{'lightgbm':types.SimpleNamespace(LGBMRegressor=FakeRegressor,Booster=FakeBooster)}):r.fit(root,'l1_M1')
            z=r.p.read(root/'effects/l1_M1/result.json');self.assertEqual(z['status'],'SKIPPED_SUPPORT');self.assertEqual(FakeRegressor.fits,0)
    def test_fit_error_not_support_fallback(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);conf=slot_fixture(root)
            with patch.object(r,'phase_config',return_value=conf),patch.dict(sys.modules,{'lightgbm':types.SimpleNamespace(LGBMRegressor=FakeRegressor,Booster=FakeBooster)}),patch.object(FakeRegressor,'fit',side_effect=RuntimeError('synthetic failure')):
                with self.assertRaisesRegex(RuntimeError,'synthetic'):r.fit(root,'l1_M1')
            self.assertFalse((root/'effects/l1_M1/result.json').exists());self.assertTrue((root/'effects/l1_M1/fit_started.json').exists())
    def test_incomplete_slot_never_refits(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);conf=slot_fixture(root);(root/'effects/l1_M1').mkdir(parents=True)
            with patch.object(r,'phase_config',return_value=conf),patch.dict(sys.modules,{'lightgbm':types.SimpleNamespace(LGBMRegressor=FakeRegressor,Booster=FakeBooster)}):
                with self.assertRaisesRegex(ValueError,'INCOMPLETE_SLOT'):r.fit(root,'l1_M1')
    def test_budget_counts_attempts_not_only_successes(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);conf=slot_fixture(root)
            for i in range(6):
                d=root/'effects'/str(i);d.mkdir(parents=True);(d/'fit_started.json').write_text('{}')
            with patch.object(r,'phase_config',return_value=conf),patch.dict(sys.modules,{'lightgbm':types.SimpleNamespace(LGBMRegressor=FakeRegressor,Booster=FakeBooster)}):
                with self.assertRaisesRegex(ValueError,'FIT_BUDGET'):r.fit(root,'l1_M1')
    def test_seal_requires_independent_hash_binding(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);stage=root/'staging';stage.mkdir();(stage/'manifest.json').write_text('{}');(stage/'predictions.parquet').write_bytes(b'synthetic')
            r.p.dump_new(stage/'independent_validation.json',{'status':'PASS','staging_manifest_sha256':'wrong'})
            with patch.object(r,'phase_config',return_value={}):
                with self.assertRaisesRegex(ValueError,'INDEPENDENT'):r.seal(root)
            self.assertFalse((root/'issued').exists())

if __name__=='__main__':unittest.main()
