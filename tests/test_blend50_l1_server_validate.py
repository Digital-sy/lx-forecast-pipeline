import ast, importlib.util, json, tempfile, unittest
from pathlib import Path
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parents[1]/'scripts/blend50_l1_server_validate.py'
spec=importlib.util.spec_from_file_location('validate_server',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def daily():
    rows=[]
    for shop in sorted(m.SHOPS):
        for day in pd.date_range('2026-01-01',periods=90):
            for spu,y in [('A',50.),('B',25.),('C',25.)]:rows.append(dict(shop=shop,spu=spu,ds=day,y=y,mapped=True,sales_state='OBSERVED'))
    return pd.DataFrame(rows)

class ServerValidateTests(unittest.TestCase):
    def test_unmapped_null_spu_preserves_denominator_and_unknown(self):
        d=daily();unknown=d[d.spu.eq('A')].copy();unknown['spu']=None;unknown['mapped']=False;unknown['y']=5.
        keys,stats=m.core_keys(pd.concat([d,unknown],ignore_index=True),'2026-03-31')
        self.assertEqual(len(keys),12);self.assertFalse(keys.spu.isna().any())
        for row in stats:
            self.assertEqual(row['unmapped_units'],450.);self.assertEqual(row['full_shop_units'],9450.)
            self.assertEqual(row['selected_units'],9000.);self.assertEqual(row['selection_coverage'],9000/9450)
        with self.assertRaises(ValueError):m.core_keys(pd.concat([d,unknown,unknown.iloc[:1]],ignore_index=True),'2026-03-31')
        unknown['mapped']=True
        with self.assertRaises(ValueError):m.core_keys(pd.concat([d,unknown],ignore_index=True),'2026-03-31')
    def test_unknown_shop_or_date_not_filtered_away(self):
        for column in ['shop','ds']:
            d=daily();d.loc[0,column]=None
            with self.assertRaises(ValueError):m.core_keys(d,'2026-03-31')
    def test_separate_validator_release_does_not_rewrite_compute_pin(self):
        with tempfile.TemporaryDirectory() as folder:
            base=Path(folder);old=base/'compute';new=base/'validator'
            for p in [old,new]:(p/'scripts').mkdir(parents=True)
            name='scripts/blend50_l1_server_validate.py';(old/name).write_text('old',encoding='utf-8');(new/name).write_text('new',encoding='utf-8')
            for path,commit in [(old,'compute_commit'),(new,'validator_commit')]:
                (path/'manifest.json').write_text(json.dumps({'code_commit':commit,'files':{name:m.sha(path/name)}}),encoding='utf-8')
            conf={'release_manifest':str(old/'manifest.json'),'release_manifest_sha256':m.sha(old/'manifest.json'),'code_commit':'compute_commit'}
            before=(old/'manifest.json').read_bytes();r=m.release_binding(conf,new/'manifest.json',new/name)
            self.assertEqual(r['compute_code_commit'],'compute_commit');self.assertEqual(r['validator_code_commit'],'validator_commit');self.assertTrue(r['separate_validation_release'])
            self.assertEqual((old/'manifest.json').read_bytes(),before)
            with self.assertRaises(ValueError):m.release_binding(conf,None,new/name)
            (new/name).write_text('tampered',encoding='utf-8')
            with self.assertRaises(ValueError):m.release_binding(conf,new/'manifest.json',new/name)
            (new/name).write_text('new',encoding='utf-8');(old/name).write_text('changed compute',encoding='utf-8')
            with self.assertRaises(ValueError):m.release_binding(conf,new/'manifest.json',new/name)
    def test_gate_false_remains_direct_plus_l1(self):
        b,p=m.expected_values([10.],[99.],[30.],[False]);self.assertEqual(b[0],10.);self.assertEqual(p[0],20.)
    def test_gate_true_formula(self):
        b,p=m.expected_values([10.],[30.],[8.],[True]);self.assertEqual(b[0],20.);self.assertEqual(p[0],14.)
    def test_required_missing_not_silent_fallback(self):
        b,p=m.expected_values([10.],[np.nan],[np.nan],[False]);self.assertEqual(b[0],10.);self.assertTrue(np.isnan(p[0]))
    def test_true_gate_missing_rejected(self):
        with self.assertRaises(ValueError):m.expected_values([10.],[np.nan],[8.],[True])
    def test_negative_endpoint_rejected(self):
        with self.assertRaises(ValueError):m.expected_values([10.],[9.],[-1.],[False])
    def test_zero_not_unknown(self):
        b,p=m.expected_values([0.,np.nan],[0.,np.nan],[0.,np.nan],[True,False]);self.assertEqual(p[0],0.);self.assertTrue(np.isnan(p[1]))
    def test_before_target_requires_actual_date_timezone(self):
        m.before_target('2026-10-29','2026-10-29T01:00:00+00:00')
        for stamp in ['2026-10-29T01:00:00','2026-10-29T16:00:00+00:00','2026-10-28T01:00:00+00:00']:
            with self.assertRaises(ValueError):m.before_target('2026-10-29',stamp)
    def test_core_boundary_ties_all_included(self):
        keys,stats=m.core_keys(daily(),'2026-03-31');self.assertEqual(len(keys),12);self.assertTrue(all(x['selection_coverage']==1 for x in stats))
    def test_core_missing_calendar_not_zero(self):
        d=daily();d=d[~d.ds.eq('2026-01-01')]
        with self.assertRaises(ValueError):m.core_keys(d,'2026-03-31')
    def test_unmapped_denominator_preserved(self):
        d=daily();d.loc[d.spu.eq('A'),'mapped']=False
        with self.assertRaises(ValueError):m.core_keys(d,'2026-03-31')
    def test_future_perturbation_does_not_change_core(self):
        d=daily();keys,stats=m.core_keys(d,'2026-03-31');future=d.iloc[:1].copy();future.ds=pd.Timestamp('2026-04-01');future.y=1e12
        got,_=m.core_keys(pd.concat([d,future]),'2026-03-31');pd.testing.assert_frame_equal(keys,got)
    def test_exact_comparison_does_not_round(self):
        m.exact([0.,np.nan],[0.,np.nan],'x')
        with self.assertRaises(ValueError):m.exact([1.0000001],[1.],'x')
    def test_validator_has_no_model_execution(self):
        tree=ast.parse(P.read_text(encoding='utf-8'))
        for n in ast.walk(tree):
            if isinstance(n,ast.Call):self.assertNotIn(n.func.attr if isinstance(n.func,ast.Attribute) else getattr(n.func,'id',''),['fit','predict','Booster','LGBMRegressor','connect','urlopen'])

if __name__=='__main__':unittest.main()
