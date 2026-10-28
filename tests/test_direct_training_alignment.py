import ast
import importlib.util
from pathlib import Path
import unittest
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parents[1]/'scripts/direct_training_alignment.py'
spec=importlib.util.spec_from_file_location('alignment',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class DiagnosticTests(unittest.TestCase):
    def test_boundary_ties(self):
        self.assertEqual(m.core_mask([80,10,10],100).tolist(),[True,True,True])
    def test_full_shop_unmapped_denominator(self):
        self.assertEqual(m.core_mask([80,5,5],100).tolist(),[True,True,True])
        self.assertEqual(m.core_mask([80,5,5],90).tolist(),[True,True,True])
        self.assertEqual(m.core_mask([81,5,4],90).tolist(),[True,False,False])
    def test_poisson_zeros_missing(self):
        d=m.deviance([0,0,1,1,np.nan],[0,2,0,1,1]);self.assertEqual(d[0],0);self.assertEqual(d[1],4)
        self.assertTrue(np.isinf(d[2]));self.assertEqual(d[3],0);self.assertTrue(np.isnan(d[4]))
    def test_poisson_formula(self):
        self.assertAlmostEqual(m.deviance([2],[1])[0],2*(2*np.log(2)-1))
    def test_disjoint_inclusive_boundaries(self):
        self.assertEqual(m.disjoint(['2024-01-01','2024-01-30','2024-01-31']),2)
    def test_own_cutoff_and_future_perturbation(self):
        dates=pd.date_range('2024-01-01',periods=95)
        d=pd.DataFrame(dict(shop='S',spu='A',ds=dates,y=1.,mapped=True,sales_state='OBSERVED'))
        _,a,_=m.historical_core(d);d.loc[d.ds>dates[89],'y']=10000
        _,b,_=m.historical_core(d)
        self.assertEqual(a[('S','A')][0][89],1);self.assertEqual(a[('S','A')][0][89],b[('S','A')][0][89])
        self.assertEqual(a[('S','A')][0][88],2)
    def test_missing_day_unknown_not_noncore(self):
        dates=pd.date_range('2024-01-01',periods=90)
        d=pd.DataFrame(dict(shop='S',spu='A',ds=dates,y=1.,mapped=True,sales_state='OBSERVED'))
        e=d.copy();e['spu']='B';e=e.iloc[1:];_,x,_=m.historical_core(pd.concat([d,e]))
        self.assertEqual(x[('S','B')][0][-1],2)
    def test_zero_shop_unknown(self):
        d=pd.DataFrame(dict(shop='S',spu='A',ds=pd.date_range('2024-01-01',periods=90),y=0.,mapped=True,sales_state='EXPLICIT_ZERO'))
        _,x,_=m.historical_core(d);self.assertEqual(x[('S','A')][0][-1],2)
    def test_four_positive_months_insufficient_mature_label(self):
        dates=pd.date_range('2024-01-01','2024-06-30');known=np.ones(len(dates),bool)
        months=pd.period_range('2024-01','2024-04',freq='M').asi8
        sim,r,_,_=m.trace_reason(dates,known,months,pd.Timestamp('2024-05-31'),'M1')
        self.assertEqual((r=='INCLUDED').sum(),0);self.assertEqual((r=='LABEL_NOT_MATURE').sum(),30)
        sim,r,_,_=m.trace_reason(dates,known,months,pd.Timestamp('2024-06-01'),'M1')
        self.assertEqual((r=='INCLUDED').sum(),1)
    def test_second_segment_matures_later(self):
        dates=pd.date_range('2024-01-01','2024-07-01');known=np.ones(len(dates),bool)
        months=pd.period_range('2024-01','2024-04',freq='M').asi8
        _,r,_,_=m.trace_reason(dates,known,months,pd.Timestamp('2024-06-30'),'M2');self.assertEqual((r=='INCLUDED').sum(),0)
        _,r,_,_=m.trace_reason(dates,known,months,pd.Timestamp('2024-07-01'),'M2');self.assertEqual((r=='INCLUDED').sum(),1)
    def test_missing_label_separate_from_immaturity(self):
        dates=pd.date_range('2024-01-01','2024-06-01');known=np.ones(len(dates),bool);known[-1]=False
        months=pd.period_range('2024-01','2024-04',freq='M').asi8
        _,r,_,_=m.trace_reason(dates,known,months,pd.Timestamp('2024-06-01'),'M1');self.assertEqual((r=='MISSING_LABEL_DAYS').sum(),1)
    def test_no_model_or_service_execution(self):
        tree=ast.parse(P.read_text(encoding='utf-8'))
        forbidden={'fit','predict','train','connect','create_engine','urlopen','get_token'}
        for n in ast.walk(tree):
            if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute):self.assertNotIn(n.func.attr,forbidden)
    def test_nullable_model_scope_uses_frozen_objects(self):
        obj=pd.DataFrame(dict(issue_date=['2025-01-02'],shop=['S'],spu=['A'],paired=[True],decline_group=['D']))
        scores=pd.DataFrame(dict(issue_date=['2025-01-02']*3,shop=['S']*3,spu=['A']*3,model=['M']*3,paired=[np.nan]*3))
        z=m.canonical_scope(scores,obj);self.assertEqual(len(z),3);self.assertTrue(z.paired.all())

if __name__=='__main__':unittest.main()
