import ast
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('review', ROOT/'scripts/blend50_l1_shadow_review.py')
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


def fixtures():
    issue = '2030-01-01'
    core = pd.DataFrame(dict(shop=['A', 'B'], spu=['S', 'S'], selection_qty=[90., 10.]))
    routing = core[['shop','spu']].copy()
    routing['input_status_M1'] = ['OK', 'INSUFFICIENT_AGE']
    routing['input_status_M2'] = ['OK', 'INSUFFICIENT_AGE']
    routing['gate_decision'] = [False, False]
    routing['main_reason'] = ['ANNUAL_NOT_SUPPORTED', 'NO_DIRECT_OUTPUT']
    rows = []
    for obj in core.itertuples():
        for model in review.MODELS:
            for window, (a,b) in review.WINDOWS.items():
                value = (1.2345678912345678 if window != 'd01_60' else 2.4691357824691356)
                if obj.shop == 'B' and model != 'F28':
                    value = np.nan
                rows.append(dict(issue_date=issue, shop=obj.shop, spu=obj.spu, model=model, window=window,
                    input_cutoff='2029-12-30', target_start=str((pd.Timestamp(issue)+pd.Timedelta(days=a)).date()),
                    target_end=str((pd.Timestamp(issue)+pd.Timedelta(days=b)).date()), prediction=value,
                    prediction_status='OK' if pd.notna(value) else 'INSUFFICIENT_AGE'))
    return core, pd.DataFrame(rows), routing, issue


class ShadowReviewTests(unittest.TestCase):
    def rows(self):
        c,p,r,i=fixtures()
        return review.assemble_rows(c,p,r,i,'synthetic_run')

    def test_complete_core_missing_not_removed_or_zero(self):
        rows=self.rows()
        self.assertEqual(len(rows),6)
        missing=rows[rows.shop.eq('B')]
        self.assertTrue(missing[review.CANDIDATE].isna().all())
        self.assertTrue(missing.F28.notna().all())
        self.assertTrue(missing.qualification.eq('候选不覆盖').all())

    def test_future_truth_always_pending(self):
        r=self.rows()
        for field in ['actual_status','error_status','hit_status']:
            self.assertTrue(r[field].eq('待成熟').all())
        self.assertNotIn('actual',r.columns)

    def test_duplicate_business_key_rejected(self):
        c,p,r,i=fixtures()
        with self.assertRaisesRegex(ValueError,'INVALID_KEYS'):
            review.assemble_rows(c,pd.concat([p,p.iloc[:1]]),r,i,'run')

    def test_missing_model_object_state_rejected(self):
        c,p,r,i=fixtures()
        with self.assertRaisesRegex(ValueError,'INCOMPLETE'):
            review.assemble_rows(c,p.iloc[:-1],r,i,'run')

    def test_wrong_date_rejected(self):
        c,p,r,i=fixtures();p.loc[0,'target_start']='2030-01-03'
        with self.assertRaisesRegex(ValueError,'WINDOW_START'):
            review.assemble_rows(c,p,r,i,'run')

    def test_zero_denominator_no_infinite_percentage(self):
        c,p,r,i=fixtures()
        p.loc[(p.model==review.BLEND)&(p.shop=='A'),'prediction']=0
        rows=review.assemble_rows(c,p,r,i,'run')
        q=rows[rows.shop.eq('A')]
        self.assertTrue(q.model_disagreement_ratio.isna().all())
        self.assertTrue(q.model_disagreement_status.eq('ZERO_DENOMINATOR').all())

    def test_manual_confirm_fields_blank(self):
        r=self.rows();m=review.manual_template(r)
        self.assertEqual(list(m),review.MANUAL_COLUMNS)
        self.assertTrue(m[review.MANUAL_COLUMNS[5:]].eq('').all().all())
        np.testing.assert_array_equal(m.original_candidate,r[review.CANDIDATE])

    def test_monthly_production_is_not_compared(self):
        doc={'status':'NOT_COMPARABLE','reason':'自然月','records':[dict(shop='A',spu='S',target_month='2030-01',
             forecast_qty=300,unit='件',quantity_definition='销量预测件数')]}
        ref=review.production_reference(doc,self.rows())
        self.assertEqual(ref['records'][0]['comparability'],'NOT_COMPARABLE')
        self.assertIsNone(ref['records'][0]['model_disagreement_units'])

    def test_same_window_quantity_diff_and_zero_denominator(self):
        row=self.rows().iloc[0]
        record=dict(shop=row.shop,spu=row.spu,target_start=row.target_start,target_end=row.target_end,
                    forecast_qty=0,unit='件',quantity_definition='销量预测件数')
        ref=review.production_reference({'status':'AVAILABLE','records':[record]},self.rows())['records'][0]
        self.assertEqual(ref['difference_status'],'ZERO_DENOMINATOR')
        self.assertEqual(ref['model_disagreement_units'],row[review.CANDIDATE])
        self.assertIsNone(ref['model_disagreement_ratio'])
        record['quantity_definition']='采购数量'
        self.assertEqual(review.production_reference({'status':'AVAILABLE','records':[record]},self.rows())['records'][0]['comparability'],'NOT_COMPARABLE')

    def test_production_missing_is_explicit(self):
        self.assertEqual(review.production_reference(None,self.rows())['status'],'NOT_AVAILABLE')

    def test_multiple_comparable_production_batches_rejected(self):
        row=self.rows().iloc[0]
        record=dict(shop=row.shop,spu=row.spu,target_start=row.target_start,target_end=row.target_end,
                    forecast_qty=1,unit='件',quantity_definition='销量预测件数')
        with self.assertRaisesRegex(ValueError,'AMBIGUOUS'):
            review.production_reference({'status':'AVAILABLE','records':[record,record]},self.rows())

    def test_json_retains_float_and_escapes_script(self):
        value=1.2345678912345678
        self.assertEqual(review.records(pd.DataFrame({'x':[value]}))[0]['x'],value)
        html=review.render_html({'note':'</script><script>alert(1)</script>','value':value})
        self.assertNotIn('</script><script>alert(1)',html)
        self.assertIn('\\u003c/script\\u003e',html)
        self.assertIn("connect-src 'none'",html)

    def test_no_model_database_or_network_entry(self):
        text=(ROOT/'scripts/blend50_l1_shadow_review.py').read_text(encoding='utf8')
        for node in ast.walk(ast.parse(text)):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                self.assertNotIn(node.func.attr,{'fit','predict','connect','create_engine','urlopen'})
        self.assertNotIn('fetch(',review.SCRIPT)
        self.assertNotIn('https://',review.HTML)

    def test_build_requires_success_seal(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)/'run';(root/'issued').mkdir(parents=True)
            review.dump(root/'issued/issue_seal.json',{'status':'CALCULATED_NOT_SEALED'})
            with self.assertRaisesRegex(ValueError,'SUCCESSFUL_ISSUE'):
                review.build(root,Path(folder)/'review')

    def test_synthetic_build_and_protect_sealed_files(self):
        with tempfile.TemporaryDirectory() as folder:
            run=Path(folder)/'run';(run/'issued').mkdir(parents=True);(run/'prepared').mkdir()
            c,p,r,i=fixtures()
            c.to_parquet(run/'prepared/core.parquet',index=False)
            p.to_parquet(run/'issued/predictions.parquet',index=False)
            r.to_parquet(run/'issued/routing_predictions.parquet',index=False)
            pd.DataFrame(dict(shop=['A','B'],full_shop_units=[100,10],selected_units=[90,10],selection_coverage=[.9,1.])).to_csv(run/'prepared/selection_quality.csv',index=False)
            files={f.relative_to(run).as_posix():review.sha(f) for f in run.rglob('*') if f.is_file()}
            review.dump(run/'issued/issue_seal.json',dict(status='ISSUED_SHADOW_AWAITING_ACTUALS',
                files=files,issue_date=i,input_cutoff='2029-12-30',sealed_at={'utc':'2030-01-01T01:00:00+00:00'},code_commit='SYNTHETIC_ONLY'))
            out=Path(folder)/'review';review.build(run,out)
            manifest=review.read(out/'review_manifest.json')
            self.assertEqual(manifest['rows'],6)
            self.assertEqual(manifest['model_inference_calls'],0)
            self.assertTrue((out/'核心款滚动销量预测_人工参考.html').is_file())
            for rel,expected in files.items():self.assertEqual(review.sha(run/rel),expected)
            with self.assertRaisesRegex(ValueError,'OUTPUT_MUST_BE_NEW'):
                review.build(run,out)


if __name__=='__main__':unittest.main()
