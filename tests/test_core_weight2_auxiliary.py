import ast, importlib.util
from pathlib import Path
import sys, unittest
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
spec=importlib.util.spec_from_file_location('aux',ROOT/'scripts/core_weight2_auxiliary.py');aux=importlib.util.module_from_spec(spec);spec.loader.exec_module(aux)

class AuxiliaryTests(unittest.TestCase):
    def test_transitions_do_not_use_truth(self):
        self.assertEqual(aux.transition('NON_CORE','CORE'),'EXITED_CORE')
        self.assertEqual(aux.transition('CORE','NON_CORE'),'NEW_CORE')
        self.assertEqual(aux.transition('UNKNOWN','CORE'),'UNKNOWN_TRANSITION')
        self.assertEqual(aux.transition('NON_CORE','UNKNOWN'),'UNKNOWN_TRANSITION')
    def test_join_failure_not_unknown(self):
        with self.assertRaisesRegex(ValueError,'JOIN_FAILED'):aux.state_at({},pd.date_range('2024-01-01',periods=2),('A','B'),'2024-01-01')
    def test_unknown_history_range(self):
        dates=pd.date_range('2024-01-01',periods=2);lookup={('A','B'):(np.array([1,0]),None)}
        self.assertEqual(aux.state_at(lookup,dates,('A','B'),'2023-12-31'),'UNKNOWN')
        self.assertEqual(aux.state_at(lookup,dates,('A','B'),'2024-01-02'),'NON_CORE')
    def test_truth_missing_not_zero(self):
        dates=pd.date_range('2024-01-01',periods=60);known=np.ones(60,bool);known[5]=False
        value,status,count=aux.window_truth(np.zeros(60),known,dates,'2024-01-01','2024-01-30')
        self.assertTrue(np.isnan(value));self.assertEqual(status,'UNKNOWN_DAYS');self.assertEqual(count,1)
    def test_explicit_zero_truth_and_boundaries(self):
        dates=pd.date_range('2024-01-01',periods=60);v=np.r_[np.zeros(30),np.ones(30)];known=np.ones(60,bool)
        self.assertEqual(aux.window_truth(v,known,dates,dates[0],dates[29]),(0.,'EXPLICIT_ZERO',0))
        self.assertEqual(aux.window_truth(v,known,dates,dates[30],dates[-1]),(30.,'OBSERVED',0))
    def test_zero_actual_absolute_risk(self):
        g=pd.DataFrame(dict(actual=[0.,0.,np.nan],prediction=[3.,5.,7.]))
        result=aux.summary(g)
        self.assertEqual(result['zero_actual_absolute_error'],8);self.assertIsNone(result['wape']);self.assertIsNone(result['over_rate'])
        self.assertEqual(result['unknown_truth_n'],1)
    def test_risk_summary_unweighted(self):
        g=pd.DataFrame(dict(actual=[10.,20.],prediction=[15.,15.],sample_weight=[1000.,1.]))
        r=aux.summary(g);self.assertEqual(r['over_units'],5);self.assertEqual(r['under_units'],5);self.assertAlmostEqual(r['wape'],1/3)
    def test_no_training_calls(self):
        tree=ast.parse((ROOT/'scripts/core_weight2_auxiliary.py').read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):self.assertNotIn(node.func.attr,{'fit','train','connect','create_engine'})

if __name__=='__main__':unittest.main()
