"""Synthetic-only checks of result presentation, never experimental evidence."""
import importlib.util
from pathlib import Path
import unittest
from unittest import mock

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('monthly_notebook', ROOT / 'scripts/bc60_monthly_notebook.py')
NOTEBOOK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NOTEBOOK)


def metric_row(month, actual, over, under, hit, positive, **changes):
    row = dict(cohort_id='monthly_core', slice='issue', issue_month=month, model='F28',window='d01_60',
        core_n=5,output_n=4,truth_n=5,scored_n=4,positive_truth_n=positive,hit_n=hit,
        miss_n=3,unjudged_n=1,actual_units=actual,known_core_actual_units=actual,
        forecast_units=actual+over-under,over_units=over,under_units=under,zero_actual_n=0,
        zero_absolute_error=0,wape=(over+under)/actual,bias=(over-under)/actual,
        hit_rate=hit/positive,hit_sales_share=.2)
    row.update(changes)
    return row


def review_fixture():
    metrics = pd.DataFrame([metric_row('2025-01',100,10,0,1,5),metric_row('2025-02',900,270,0,2,5),
                            metric_row('2025-03',100,90,0,1,5),
                            metric_row('2025-01',1000,900,0,1,5,cohort_id='original_core_385')])
    return {'frames': {
        'grid': pd.DataFrame({'issue_month':['2025-01','2025-02','2025-03'],'calendar_mature':[True,True,False]}),
        'metrics':metrics,
        'joint':pd.DataFrame([{'cohort_id':'monthly_core','issue_date':'2025-01-02','model':'F28','decision':'MISS','records':4},
                              {'cohort_id':'monthly_core','issue_date':'2025-01-02','model':'F28','decision':'UNJUDGED','records':1}]),
        'shape':pd.DataFrame([{'cohort_id':'monthly_core','issue_date':'2025-01-02','object_share':np.nan}]),
        'pairs':pd.DataFrame([{'cohort_id':'monthly_core','slice':'issue','issue_month':'2025-01'},
                              {'cohort_id':'original_core_385','slice':'issue','issue_month':'2025-01'}]),
    }}


class PresentationTests(unittest.TestCase):
    def test_native_validation_is_bound_and_does_not_claim_prediction_replay(self):
        value={'status':'PASS_INDEPENDENT_NATIVE_TRUTH_SUMMARY_HASHES_NOT_PREDICTION_REPLAY',
               'native_predictions_regenerated':False,'execution_manifest_sha256':'binding'}
        with mock.patch.object(NOTEBOOK,'sha',return_value='binding'):
            NOTEBOOK.require_native_validation(value,'synthetic.json')
            with self.assertRaisesRegex(ValueError,'SCOPE_MISMATCH'):
                NOTEBOOK.require_native_validation(value|{'native_predictions_regenerated':True},'synthetic.json')
        with mock.patch.object(NOTEBOOK,'sha',return_value='wrong'):
            with self.assertRaisesRegex(ValueError,'MANIFEST_MISMATCH'):
                NOTEBOOK.require_native_validation(value,'synthetic.json')

    def test_partial_validation_cannot_be_mistaken_for_full_pass(self):
        for status in ['PASS_COMPLETED_SLOTS_ONLY_NOT_FINAL','PASS_INDEPENDENT_PREPARED_RECONCILIATION']:
            with self.assertRaisesRegex(ValueError,'FULL_VALIDATION_NOT_PASS'):
                NOTEBOOK.require_final_validation({'status':status},{})

    def test_weighted_wape_not_mean_of_monthly_rates(self):
        output=NOTEBOOK.display_tables(review_fixture())['summary'].iloc[0]
        self.assertAlmostEqual(output.wape,.28)
        self.assertAlmostEqual(output.bias,.28)
        self.assertEqual(output.issue_count,2)
        self.assertEqual(output.core_n,10)
        self.assertAlmostEqual(output.hit_rate,.3)

    def test_immature_and_original_cohort_not_in_mature_summary(self):
        tables=NOTEBOOK.display_tables(review_fixture())
        self.assertEqual(len(tables['primary']),3)
        self.assertEqual(int(tables['primary'].calendar_mature.sum()),2)
        self.assertEqual(len(tables['pair_display']),1)
        self.assertEqual(tables['summary'].iloc[0].actual_units,1000)

    def test_joint_missing_category_is_count_zero_not_dropped_unknown(self):
        row=NOTEBOOK.display_tables(review_fixture())['joint_display'].iloc[0]
        self.assertEqual(row.HIT,0)
        self.assertEqual(row.UNJUDGED,1)
        self.assertEqual(row.core_n,5)
        self.assertEqual(row.joint_fixed_share,0)

    def test_shape_unknown_remains_missing(self):
        value=NOTEBOOK.display_tables(review_fixture())['shape_display'].object_share.iloc[0]
        self.assertTrue(pd.isna(value))

    def test_best_and_worst_not_a_combined_strategy(self):
        values=NOTEBOOK.display_tables(review_fixture())['extremes']
        self.assertEqual(len(values),4)
        wape=values[values.metric.eq('wape')].set_index('extreme')
        self.assertEqual(wape.loc['BEST','issue_month'],'2025-01')
        self.assertEqual(wape.loc['WORST','issue_month'],'2025-02')

    def test_official_mature_summary_reconciles_and_is_used(self):
        review=review_fixture()
        official=NOTEBOOK.display_tables(review)['summary'].copy()
        official['cohort_id']='monthly_core'
        official['slice']='overall_mature_overlapping_issue_records'
        review['frames']['metrics']=pd.concat([review['frames']['metrics'],official],ignore_index=True)
        result=NOTEBOOK.display_tables(review)['summary']
        self.assertEqual(result.iloc[0]['slice'],'overall_mature_overlapping_issue_records')
        self.assertEqual(result.iloc[0].core_n,10)

    def test_official_mature_summary_mismatch_stops(self):
        review=review_fixture()
        official=NOTEBOOK.display_tables(review)['summary'].copy()
        official['cohort_id']='monthly_core'
        official['slice']='overall_mature_overlapping_issue_records'
        official['core_n']=11
        review['frames']['metrics']=pd.concat([review['frames']['metrics'],official],ignore_index=True)
        with self.assertRaisesRegex(ValueError,'RECONCILIATION_FAILED:core_n'):
            NOTEBOOK.display_tables(review)


if __name__ == '__main__':
    unittest.main()
