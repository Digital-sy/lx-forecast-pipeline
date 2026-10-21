"""Pure arithmetic tests. Never instantiate, fit or query a forecast model."""
import ast
import math
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
from scripts.direct_yearago_blend50 import blend_one,PRED_COLS,four_group
from scripts.yearago_gate_route import unique
from scripts.yearago_gate_score import metrics


class BlendTests(unittest.TestCase):
    def test_true_formula_both_directions(self):
        for d,g in [(123.456789123,198.239847123),(900.,70.),(0.,15.),(15.,0.)]:
            self.assertEqual(blend_one(d,g,True),d+.5*(g-d))

    def test_false_exact_without_arithmetic(self):
        for d in [np.float64(-0.),np.float64(0.),np.float64(1.234567891234567),np.float64(1e250)]:
            self.assertIs(blend_one(d,d,False),d)
            self.assertEqual(blend_one(d,d,False).tobytes(),d.tobytes())

    def test_missing_original_retained(self):
        x=float('nan');self.assertIs(blend_one(x,x,False),x)

    def test_integrity_not_silent_fallback(self):
        for d,g,b in [(5.,np.nan,True),(5.,np.nan,False),(5.,6.,False),(np.nan,1.,False),(np.nan,np.nan,True),(3.,3.,None),(np.inf,2.,True)]:
            with self.assertRaises(ValueError):blend_one(d,g,b)

    def test_same_weight_segments_total(self):
        d=[10.101,201.991];g=[45.654,1.537]
        p=[blend_one(x,y,True) for x,y in zip(d,g)]
        self.assertEqual(p[0]+p[1],(d[0]+.5*(g[0]-d[0]))+(d[1]+.5*(g[1]-d[1])))

    def test_truth_not_in_generation_fields(self):
        forbidden={'actual','error','hit','joint_hit','truth_status','category','maturity'}
        self.assertFalse(set(PRED_COLS)&forbidden)
        x={'d':10.,'g':50.,'gate_decision':True}
        before=blend_one(x['d'],x['g'],x['gate_decision'])
        for truth in [None,0.,1e99,-999.]:
            z=dict(reversed(list({**x,'actual':truth,'hit':truth is not None}.items())))
            self.assertEqual(blend_one(z['d'],z['g'],z['gate_decision']),before)

    def test_zero_actual_not_relative_hit(self):
        actual=0.;prediction=blend_one(2.,4.,True)
        self.assertEqual(abs(prediction-actual),3.)
        self.assertFalse(actual>0 and abs(prediction-actual)/actual<=.2)

    def test_immature_side_forecast_is_not_scored(self):
        f=pd.DataFrame({'shop':['A'],'spu':['X'],'issue_date':['s'],'actual':[np.nan],'prediction':[100.]})
        m=metrics(f)
        self.assertEqual(m['output_n'],1)
        self.assertEqual(m['scored_n'],0)
        self.assertEqual(m['hit_n'],0)
        self.assertTrue(math.isnan(m['wape']))

    def test_strict_boundary_no_display_rounding(self):
        self.assertTrue(abs(120.-100.)/100.<=.2)
        self.assertFalse(abs(np.nextafter(120.,math.inf)-100.)/100.<=.2)

    def test_common_hit_preserved(self):
        for d,g in [(80.,120.),(120.,80.),(95.,119.)]:
            self.assertLessEqual(abs(blend_one(d,g,True)-100.)/100.,.2)

    def test_neither_endpoint_hit_can_enter(self):
        d,g,y=60.,140.,100.
        self.assertGreater(abs(d-y)/y,.2);self.assertGreater(abs(g-y)/y,.2)
        self.assertEqual(blend_one(d,g,True),y)

    def test_convexity_all_three_losses(self):
        for d,g,y in [(20.,90.,50.),(50.,10.,80.),(0.,10.,0.),(12.3456,890.123,400.)]:
            b=blend_one(d,g,True)
            for fn in [abs,lambda e:max(e,0.),lambda e:max(-e,0.)]:
                bound=(fn(d-y)+fn(g-y))/2
                self.assertLessEqual(fn(b-y),bound+32*np.finfo(float).eps*max(1,abs(d),abs(g),abs(y)))

    def test_key_isolation_and_duplicates(self):
        z=pd.DataFrame({'shop':['A','B'],'spu':['X','X'],'issue_date':['s','s']})
        unique(z,['issue_date','shop','spu'])
        with self.assertRaises(ValueError):unique(pd.concat([z,z]),['issue_date','shop','spu'])

    def test_four_groups(self):
        a=pd.Series([True,False,True,False]);b=pd.Series([True,True,False,False])
        self.assertEqual(list(four_group(a,b)),['BOTH_HIT','GATE_GAINED','GATE_LOST','NEITHER_HIT'])

    def test_no_model_calls(self):
        root=Path(__file__).resolve().parents[1]/'scripts'
        for f in root.glob('direct_yearago_blend50*.py'):
            for n in ast.walk(ast.parse(f.read_text(encoding='utf-8'))):
                if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute):self.assertNotIn(n.func.attr,['fit','predict','train'])
                if isinstance(n,ast.Import):self.assertFalse(any(x.name.startswith(('lightgbm','mlforecast','sklearn')) for x in n.names))


if __name__=='__main__':unittest.main()
