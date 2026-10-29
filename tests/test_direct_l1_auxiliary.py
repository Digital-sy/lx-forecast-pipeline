import ast
import importlib.util
from pathlib import Path
import unittest

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('l1aux', ROOT / 'scripts/direct_l1_auxiliary.py')
aux = importlib.util.module_from_spec(spec)
spec.loader.exec_module(aux)


def inference(segment='M1'):
    return pd.DataFrame(dict(issue_date=['2025-01-02'] * 2, shop=['A', 'B'],
                             spu=['S', 'S'], segment=[segment] * 2))


class L1AuxiliaryTests(unittest.TestCase):
    def test_raw_negative_preserved_then_nonnegative(self):
        f = aux.candidate_frame(inference(), [-3, 4])
        np.testing.assert_array_equal(f.raw_prediction, [-3, 4])
        np.testing.assert_array_equal(f.prediction, [0, 4])
        np.testing.assert_array_equal(f.clipped_amount, [3, 0])

    def test_60_sum_after_segment_clip_not_raw_total_clip(self):
        a = aux.candidate_frame(inference(), [-3, 4])
        b = aux.candidate_frame(inference('M2'), [2, -1])
        f = aux.window_predictions(pd.concat([a, b]))
        g = f[f.window.eq('d01_60')].sort_values('shop')
        np.testing.assert_array_equal(g.raw_prediction, [-1, 3])
        np.testing.assert_array_equal(g.prediction, [2, 4])
        np.testing.assert_array_equal(g.clipped_amount, [3, 1])
        np.testing.assert_array_equal(g.negative_component_n, [1, 1])

    def test_prior_direct_exact_and_no_invented_raw(self):
        inf = inference()
        prior = inf.copy()
        prior['model'] = 'DIRECT'
        prior['prediction'] = [0.12345678912345678, 4.0]
        prior['prediction_status'] = 'OK'
        f = aux.reused_direct_frame(inf, prior)
        np.testing.assert_array_equal(f.prediction, prior.prediction)
        self.assertTrue(f.raw_prediction.isna().all())
        self.assertFalse(f.raw_prediction_available.any())

    def test_baseline_wrong_keys_not_silent_unknown(self):
        prior = inference()
        prior['model'] = 'DIRECT'
        prior['prediction'] = [1, 2]
        prior.loc[0, 'spu'] = 'WRONG'
        with self.assertRaisesRegex(ValueError, 'KEY_MISMATCH'):
            aux.reused_direct_frame(inference(), prior)

    def test_duplicate_keys_rejected(self):
        f = inference()
        with self.assertRaisesRegex(ValueError, 'UNIQUE_KEYS'):
            aux.candidate_frame(pd.concat([f, f]), [1, 2, 3, 4])

    def test_missing_checkpoint_not_zero(self):
        f = aux.candidate_frame(inference(), [np.nan, np.nan], 'MISSING_CHECKPOINT')
        self.assertTrue(f.prediction.isna().all())
        self.assertTrue(f.negative_component_n.isna().all())

    def test_ok_nan_rejected(self):
        with self.assertRaisesRegex(ValueError, 'INVALID_RAW'):
            aux.candidate_frame(inference(), [np.nan, 0])

    def test_positive_truth_zero_prediction_separate_unknown(self):
        g = pd.DataFrame(dict(actual=[10., 0., 4., np.nan, 3.], prediction=[0., 0., np.nan, 0., 5.]))
        r = aux.summary(g)
        self.assertEqual(r['positive_actual_zero_prediction_n'], 1)
        self.assertEqual(r['positive_actual_zero_prediction_actual_units'], 10)
        self.assertEqual(r['scored_n'], 3)
        self.assertEqual(r['zero_actual_n'], 1)
        self.assertEqual(r['unknown_truth_n'], 1)
        self.assertAlmostEqual(r['bias'], -8 / 13)

    def test_unweighted_summary(self):
        g = pd.DataFrame(dict(actual=[10., 20.], prediction=[15., 15.], sample_weight=[100., 1.]))
        r = aux.summary(g)
        self.assertEqual(r['over_units'], 5)
        self.assertEqual(r['under_units'], 5)
        self.assertAlmostEqual(r['wape'], 1 / 3)

    def test_empty_or_zero_denominator_not_pass(self):
        r = aux.summary(pd.DataFrame(dict(actual=[], prediction=[])))
        self.assertEqual(r['fixed_n'], 0)
        self.assertIsNone(r['wape'])
        self.assertIsNone(r['bias'])
        r = aux.summary(pd.DataFrame(dict(actual=[0.], prediction=[5.])))
        self.assertEqual(r['zero_actual_absolute_error'], 5)
        self.assertIsNone(r['over_rate'])

    def test_main_pool_not_reselected_and_no_fit(self):
        source = (ROOT / 'scripts/direct_l1_auxiliary.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                self.assertNotIn(node.func.attr, {'fit', 'train', 'connect', 'historical_core', 'Context'})
        self.assertEqual(source.count('booster.predict('), 1)


if __name__ == '__main__':
    unittest.main()
