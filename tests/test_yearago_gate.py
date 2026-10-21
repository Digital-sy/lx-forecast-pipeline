"""Pure selection/accounting tests: zero model fitting and inference."""
import ast
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
from scripts.yearago_gate_route import choose, annual_bounds, unique
from scripts.yearago_gate_score import exit_bin, entry_bin, metrics, objects


class GateTests(unittest.TestCase):
    def fixture(self):
        return dict(direct_valid=True,input_valid=True,targets_valid=True,prior_positive=True,
                    both_supported=True,both_annual=True,direct_M1=11.123456789,direct_M2=21.,
                    annual_M1=7.123456789,annual_M2=17.)

    def test_true_both(self):
        x=self.fixture(); r=choose(x)
        self.assertTrue(r['gate_decision']); self.assertEqual(r['selected_M1'],x['annual_M1'])
        self.assertEqual(r['selected_M2'],x['annual_M2'])

    def test_each_false_unknown_both_fallback(self):
        for field in ['direct_valid','input_valid','targets_valid','prior_positive','both_supported','both_annual']:
            for state in [False,None,np.nan]:
                x=self.fixture(); x[field]=state; r=choose(x)
                self.assertFalse(r['gate_decision']); self.assertIn(field,r['failed_conditions'])
                self.assertEqual(r['selected_M1'],x['direct_M1']); self.assertEqual(r['selected_M2'],x['direct_M2'])

    def test_truth_deleted_shuffled_perturbed(self):
        x=self.fixture(); expected=choose(x)
        for truth in [{}, {'actual':0,'hit':False}, {'actual':1e50,'hit':True}, {'error':-1e30,'actual':100}]:
            self.assertEqual(choose(dict(reversed(list({**x,**truth}.items())))),expected)

    def test_explicit_zero_not_missing(self):
        x=self.fixture(); x['annual_M1']=0.; self.assertEqual(choose(x)['selected_M1'],0.)
        x['annual_M1']=np.nan; x['both_annual']=False
        self.assertEqual(choose(x)['selected_M1'],x['direct_M1'])

    def test_missing_direct_retained(self):
        x=self.fixture(); x.update(direct_valid=False,direct_M1=np.nan,direct_M2=np.nan)
        self.assertTrue(np.isnan(choose(x)['selected_M1']))

    def test_windows_leap_and_boundaries(self):
        a,b,t,u=annual_bounds('2024-02-29','M2')
        self.assertEqual((str(a.date()),str(b.date()),str(t.date()),str(u.date())),
                         ('2023-01-30','2023-02-26','2023-03-31','2023-04-29'))
        self.assertEqual((b-a).days+1,28);self.assertEqual((u-t).days+1,30)
        self.assertLessEqual(u,pd.Timestamp('2024-02-27'))

    def test_duplicate_is_error(self):
        with self.assertRaises(ValueError): unique(pd.DataFrame({'key':[1,1]}),['key'])

    def test_exit_entry_boundaries_unrounded(self):
        self.assertEqual(exit_bin(.02),'20-22%')
        self.assertEqual(exit_bin(np.nextafter(.02,1)),'22-25%')
        self.assertEqual(exit_bin(.05),'22-25%');self.assertEqual(exit_bin(.10),'25-30%')
        self.assertEqual(exit_bin(0),'UNCLASSIFIED');self.assertEqual(entry_bin(0),'0-2pp_inside')

    def test_zero_unknown_denominators(self):
        f=pd.DataFrame(dict(actual=[0.,np.nan,100.],prediction=[10.,20.,120.],shop=['A']*3,spu=['X']*3,issue_date=['x']*3))
        r=metrics(f); self.assertEqual(r['zero_truth_ae'],10.);self.assertEqual(r['hit_n'],1)
        self.assertEqual(r['scored_n'],2);self.assertEqual(r['wape'],.3)

    def test_no_model_calls_or_imports(self):
        root=Path(__file__).resolve().parents[1]/'scripts'
        for p in root.glob('yearago_gate_*.py'):
            tree=ast.parse(p.read_text(encoding='utf-8'))
            for n in ast.walk(tree):
                if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute):
                    self.assertNotIn(n.func.attr,['fit','predict','train'])
                if isinstance(n,ast.Import):
                    self.assertFalse(any(x.name.startswith(('lightgbm','sklearn','mlforecast')) for x in n.names))


if __name__=='__main__':unittest.main()
