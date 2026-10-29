import importlib.util, sys, unittest
from pathlib import Path
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parents[1]/'scripts/blend50_l1_server_parity.py'
spec=importlib.util.spec_from_file_location('parity',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class ParityTests(unittest.TestCase):
    def test_gate_false_still_combines_l1(self):
        b,p=m.combine([10.],[99.],[20.],[False]);self.assertEqual(b[0],10.);self.assertEqual(p[0],15.)
    def test_gate_true_uses_original_blend_arithmetic(self):
        b,p=m.combine([10.],[30.],[4.],[True]);self.assertEqual(b[0],20.);self.assertEqual(p[0],12.)
    def test_qualification_missing_stays_missing(self):
        b,p=m.combine([np.nan],[np.nan],[np.nan],[False]);self.assertTrue(np.isnan(b[0]));self.assertTrue(np.isnan(p[0]))
    def test_required_endpoint_missing_no_single_model(self):
        b,p=m.combine([5.],[np.nan],[np.nan],[False]);self.assertEqual(b[0],5.);self.assertTrue(np.isnan(p[0]))
    def test_gate_true_missing_is_error(self):
        with self.assertRaises(ValueError):m.combine([5.],[np.nan],[5.],[True])
    def test_explicit_zero_distinct_from_unknown(self):
        b,p=m.combine([0.,np.nan],[0.,np.nan],[0.,np.nan],[True,False]);self.assertEqual(p[0],0.);self.assertTrue(np.isnan(p[1]))
    def test_unknown_gate_evidence_fails_closed(self):
        d=pd.DataFrame({c:[True,True] for c in m.CONDITIONS}).astype(object);d.loc[1,'prior_positive']=None
        self.assertEqual(m.gate_from_conditions(d).tolist(),[True,False])
    def test_fixed_tolerance_not_rounded(self):
        self.assertEqual(m.compare([10.+1e-7],[10.],'x')['outside_tolerance_n'],0)
        with self.assertRaises(ValueError):m.compare([10.+1e-4],[10.],'x')
    def test_unknown_masks_exact(self):
        with self.assertRaises(ValueError):m.compare([0.],[np.nan],'x')
    def test_negative_and_infinite_rejected(self):
        for x in [-1.,np.inf]:
            with self.assertRaises(ValueError):m.combine([x],[1.],[1.],[False])
    def test_duplicate_keys_rejected(self):
        d=pd.DataFrame({'issue_date':['x','x'],'shop':['s','s'],'spu':['p','p']})
        with self.assertRaises(ValueError):m.unique(d,m.KEY)
    def test_dates_exact(self):
        d=pd.DataFrame(dict(issue_date=['2026-08-02'],simulated_issue_date=['2026-08-02'],input_cutoff=['2026-07-31'],label_start=['2026-08-03'],label_end=['2026-09-01'],segment=['M1']))
        m.validate_dates(d,'2026-08-02','M1');d['input_cutoff']='2026-08-01'
        with self.assertRaises(ValueError):m.validate_dates(d,'2026-08-02','M1')
    def test_deployed_pure_combination_uses_both_gate_routes(self):
        sys.path.insert(0,str(P.parent));import blend50_l1_server_issue as deployed
        objects=pd.DataFrame({'issue_date':['2026-08-02']*2,'shop':['S']*2,'spu':['A','B']})
        inf=objects.copy();inf['input_cutoff']=pd.Timestamp('2026-07-31');inf['input_status']='OK'
        inf['ly_input_complete']=True;inf['ly_target_complete']=True;inf['ly_positive_observed_before_input_start']=[True,False]
        audit=pd.DataFrame({'ly_input_unsafe_days':[0,0],'ly_target_unsafe_days':[0,0]})
        mats={seg:(inf.copy(),audit.copy()) for seg in ['M1','M2']};support={seg:{'fit_allowed':True} for seg in ['M1','M2']}
        preds={kind+'_'+seg:np.array([10.,10.]) if kind=='direct' else np.array([30.,30.]) for kind in ['direct','annual'] for seg in ['M1','M2']}
        wide=deployed.p.assemble(objects,'2026-08-02',mats,preds,support);wide=deployed.combine_l1(wide,{seg:np.array([8.,8.]) for seg in ['M1','M2']})
        self.assertEqual(wide.gate_decision.tolist(),[True,False]);self.assertEqual(wide.candidate_M1.tolist(),[14.,9.]);self.assertEqual(wide.candidate_60.tolist(),[28.,18.])

if __name__=='__main__':unittest.main()
