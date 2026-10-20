"""Synthetic report integration checks. No business inputs, DB or fitting."""
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from scripts import bc60_monthly_report as report


class MonthlyReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='bc60_report_synthetic_')
        cls.root=Path(cls.temp.name)
        for folder in ['scoring','prepared','effects','inputs','diagnostics']:
            (cls.root/folder).mkdir()
        dates=[];scores=[];quality=[];cover=[];pairs=[];joint=[];fits=[];shape=[]
        shops=['synthetic_a','synthetic_b','synthetic_c','synthetic_d']
        for month in pd.period_range('2025-01','2026-10',freq='M'):
            issue=month.start_time+pd.Timedelta(days=1)
            issue_string=str(issue.date());mature=issue+pd.Timedelta(days=60)<=pd.Timestamp('2026-10-01')
            dates.append({'issue_month':str(month),'issue_date':issue_string,
                'input_cutoff':str((issue-pd.Timedelta(days=2)).date()),'calendar_mature':mature})
            shape.append({'cohort_id':'monthly_core','issue_date':issue_string,'positive_both_n':4 if mature else 0,
                'incompatible_shape_n':2 if mature else 0})
            for shop in shops:
                quality.append({'issue_date':issue_string,'shop':shop,'core_n':1,'selection_status':'OK'})
                for window in report.WINDOWS:
                    cover.append({'cohort_id':'monthly_core','issue_date':issue_string,'shop':shop,'window':window,
                        'observed_shop_units':100. if mature else 0.,'complete_window_core_units':100. if mature else 0.,
                        'status':'OBSERVED_SOURCE_COVERAGE' if mature else 'NOT_MATURED'})
            for kind in ['B1','B2','B3','M1','M2']:
                fits.append({'issue_date':issue_string,'kind':kind,'status':'SYNTHETIC_NO_FIT',
                    'available_calendar_days':366,'train_rows':100,'train_objects':4,'train_dates':60,
                    'effective_label_dates':32,'first_label':'2024-01-01','last_label':'2024-12-31'})
            for model in report.MODELS:
                for index,shop in enumerate(shops):
                    missing=model=='B1' and index==0
                    joint.append({'cohort_id':'monthly_core','issue_date':issue_string,'shop':shop,'spu':'synthetic_spu',
                        'model':model,'decision':'UNJUDGED' if missing or not mature else 'HIT'})
                    for window,(start,end) in report.WINDOWS.items():
                        scores.append({'cohort_id':'monthly_core','issue_month':str(month),'issue_date':issue_string,
                            'shop':shop,'spu':'synthetic_spu','model':model,'window':window,
                            'target_start':str((issue+pd.Timedelta(days=start)).date()),
                            'target_end':str((issue+pd.Timedelta(days=end)).date()),
                            'actual':100. if mature else np.nan,'prediction':np.nan if missing else 110.,
                            'decision':'UNJUDGED' if missing or not mature else 'HIT',
                            'truth_status':'OBSERVED' if mature else 'NOT_MATURED'})
                if model!='F28':
                    for window in report.WINDOWS:
                        pairs.append({'cohort_id':'monthly_core','slice':'issue','issue_month':str(month),
                            'model':model,'window':window,'paired_n':(3 if model=='B1' else 4) if mature else 0})
        def save(relative,frame):
            path=cls.root/relative
            if path.suffix=='.parquet':pd.DataFrame(frame).to_parquet(path,index=False)
            else:pd.DataFrame(frame).to_csv(path,index=False)
        save('prepared/issue_grid.csv',dates);save('prepared/selection_quality.csv',quality)
        save('scoring/scores.parquet',scores);save('scoring/future_coverage.csv',cover)
        save('scoring/paired_f28.csv',pairs);save('scoring/joint_hits.csv',joint)
        save('scoring/f28_shape_summary.csv',shape);save('effects/fit_status.csv',fits)
        save('scoring/metrics.csv',[{'synthetic_unused':True}])
        save('diagnostics/best_worst_months.csv',[{'model':'F28','window':'d01_60','criterion':'wape',
             'kind':'BEST','issue_month':'2025-01','value':.1,'core_n':4,'scored_n':4}])
        (cls.root/'effects/run_manifest.json').write_text(json.dumps({'formal_fit_count':0}),encoding='utf-8')
        (cls.root/'inputs/quality.json').write_text(json.dumps({'first_day':'2024-01-01','last_day':'2026-10-01',
             'daily_aggregate_rows':0,'missing_shop_dates':[]}),encoding='utf-8')
        (cls.root/'prepared/prepare_manifest.json').write_text('{"synthetic_test_only":true}',encoding='utf-8')
        (cls.root/'scoring/score_manifest.json').write_text('{"synthetic_test_only":true}',encoding='utf-8')
        (cls.root/'validation.json').write_text(json.dumps({'status':'PASS_INDEPENDENT_FULL_RECONCILIATION',
            'synthetic_test_only':True,
            'prepared':{'prepare_manifest_sha256':report.sha(cls.root/'prepared/prepare_manifest.json')},
            'predictions':{'effect_manifest_sha256':report.sha(cls.root/'effects/run_manifest.json')},
            'scores':{'score_manifest_sha256':report.sha(cls.root/'scoring/score_manifest.json')}}),encoding='utf-8')
        cls.out=cls.root/'report'
        report.run(cls.root,cls.out,cls.root/'validation.json')
        cls.main=pd.read_csv(cls.out/'monthly_main_table.csv')
        cls.narrative=(cls.out/'2025年起逐月预测效果与季节经营差异报告.md').read_text(encoding='utf-8')

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_twenty_two_issue_grid_exact_calendar_not_natural_month(self):
        self.assertEqual(len(self.main),22*6*3)
        row=self.main[(self.main.issue_month=='2025-01')&(self.main.model=='F28')&
                      (self.main.window=='d01_60')].iloc[0]
        self.assertEqual(row.input_cutoff,'2024-12-31')
        self.assertEqual(row.target_start,'2025-01-03')
        self.assertEqual(row.target_end,'2025-03-03')

    def test_future_periods_stay_not_matured_and_na_not_zero(self):
        rows=self.main[self.main.issue_month.isin(['2026-09','2026-10'])]
        self.assertEqual(len(rows),2*6*3)
        self.assertTrue(rows.status.eq('NOT_MATURED').all())
        self.assertTrue(rows[['wape','bias','hit_rate','target_all_shop_units','target_core_sales_coverage']].isna().all().all())
        self.assertTrue(rows[['actual_units','forecast_units','over_units','under_units',
            'known_core_actual_units','zero_absolute_error']].isna().all().all())
        sep=rows[(rows.issue_month=='2026-09')&(rows.window=='d01_30')].iloc[0]
        self.assertEqual(sep.target_end,'2026-10-02')
        heat=(self.out/'monthly_heatmap_status.csv').read_text(encoding='utf-8-sig')
        self.assertIn('NOT_MATURED',heat)

    def test_complete_core_retained_while_pair_denominators_differ(self):
        rows=self.main[(self.main.issue_month=='2025-01')&(self.main.window=='d01_60')].set_index('model')
        self.assertEqual(rows.loc['B1','core_n'],4)
        self.assertEqual(rows.loc['B1','truth_n'],4)
        self.assertEqual(rows.loc['B1','output_n'],3)
        self.assertEqual(rows.loc['B1','scored_n'],3)
        self.assertEqual(rows.loc['B1','paired_f28_n'],3)
        self.assertEqual(rows.loc['F28','paired_f28_n'],4)
        self.assertEqual(rows.loc['B1','hit_rate'],.75)
        self.assertAlmostEqual(rows.loc['B1','wape'],.1)

    def test_report_discloses_unequal_scopes_and_noncausal_limits(self):
        for text in ['各模型输出范围不同','不能当作严格同样本排名','幸存者','不回填为历史输入',
                     '目标期信息只用于事后解释','不是独立前瞻OOS','未声称浏览器点击验收']:
            self.assertIn(text,self.narrative)

    def test_na_formatting_never_silently_becomes_zero(self):
        result=report.text_table(pd.DataFrame({'value':[np.nan,0.,.25]}),['value'],['value'])
        self.assertIn('| N/A |',result)
        self.assertIn('| 0.00% |',result)
        self.assertIn('| 25.00% |',result)

    def test_unvalidated_or_changed_artifact_cannot_publish_report(self):
        evidence=json.loads((self.root/'validation.json').read_text(encoding='utf-8'))
        path=self.root/'invalid_validation.json'
        evidence['status']='PASS_PREPARED_ONLY'
        path.write_text(json.dumps(evidence),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'FINAL_NUMERIC_VALIDATION_NOT_PASS'):
            report.run(self.root,self.root/'must_not_exist',path)
        evidence['status']='PASS_INDEPENDENT_FULL_RECONCILIATION'
        evidence['scores']['score_manifest_sha256']='changed'
        path.write_text(json.dumps(evidence),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'FINAL_VALIDATION_BINDING_MISMATCH:scores'):
            report.run(self.root,self.root/'must_not_exist',path)
        self.assertFalse((self.root/'must_not_exist').exists())


if __name__=='__main__':unittest.main()
