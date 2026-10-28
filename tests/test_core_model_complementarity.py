import unittest
import math
import pandas as pd
from scripts.core_model_complementarity import (
    KEY, WINDOWS, MODELS, hit, classify, unique, build_objects, aggregate,
    longest_run, product_ranking, mature,
)


def fixture(predictions, actual=(100., 100., 200.), issue='2025-01-02'):
    rows = []
    for model in MODELS:
        vals = predictions.get(model, (150., 150., 300.))
        for w, p, y in zip(WINDOWS, vals, actual):
            rows.append(dict(issue_date=issue, shop='TEST', spu='SYNTHETIC', model=model,
                window=w, prediction=p, actual=y, maturity='TEST', lifecycle='TEST',
                current_category='TEST', decline_group='UNKNOWN'))
    return pd.DataFrame(rows)


class ComplementarityTests(unittest.TestCase):
    def test_overlap_counted_once(self):
        o = build_objects(fixture({'DIRECT': (100,100,200), 'F28': (110,110,220)}))
        a = aggregate(o)
        self.assertEqual(a['rescue_union_n'], 1)
        self.assertEqual(a['rescue_union_actual60'], 200)
        self.assertEqual(o.rescue_models.iloc[0], 'F28|DIRECT')

    def test_no_segment_stitching(self):
        o = build_objects(fixture({'DIRECT': (100,150,250), 'F28': (150,100,250)}))
        self.assertFalse(o.any_joint_hit.iloc[0])

    def test_missing_is_not_observed_failure(self):
        o = build_objects(fixture({'CONTEXT90': (math.nan,math.nan,math.nan)}))
        self.assertEqual(o.union_status.iloc[0], 'INCOMPLETE_MODEL_OUTPUT')

    def test_missing_blend_separate_rescue(self):
        o = build_objects(fixture({'BLEND50': (math.nan,math.nan,math.nan), 'F28': (100,100,200)}))
        a = aggregate(o)
        self.assertEqual(a['rescue_blend_missing_n'], 1)
        self.assertEqual(a['rescue_with_blend_output_n'], 0)

    def test_zero_actual_not_relative_hit(self):
        self.assertFalse(hit(0., 0.))
        o = build_objects(fixture({m:(0,0,0) for m in MODELS}, (0,0,0)))
        self.assertEqual(o.union_status.iloc[0], 'ZERO_ACTUAL_UNDEFINED_RELATIVE_ERROR')

    def test_unknown_truth_kept(self):
        o = build_objects(fixture({}, (math.nan,100,math.nan)))
        self.assertEqual(o.union_status.iloc[0], 'UNKNOWN_TRUTH')

    def test_boundary_unrounded(self):
        self.assertTrue(hit(120.,100.)); self.assertTrue(hit(80.,100.))
        self.assertFalse(hit(120.000000001,100.)); self.assertFalse(hit(79.999999999,100.))

    def test_duplicate_keys_raise(self):
        d = fixture({})
        with self.assertRaises(ValueError): unique(pd.concat([d,d.iloc[:1]]), KEY+['model','window'])

    def test_direction_and_existing_bands(self):
        self.assertEqual(classify([130]*6, 100), ('ALL_OVER','ALL_ABOVE_20'))
        self.assertEqual(classify([70]*6, 100), ('ALL_UNDER','ALL_BELOW_MINUS20'))
        self.assertEqual(classify([70,130,100,90,100,110],100), ('OPPOSITE','OPPOSITE_OUTSIDE_20'))
        self.assertEqual(classify([110]*6,100), ('ALL_OVER','AT_LEAST_ONE_WITHIN_20'))
        self.assertEqual(classify([0]*6,0), ('ZERO_ACTUAL','ZERO_ACTUAL'))

    def test_missing_direction_not_zero(self):
        self.assertEqual(classify([math.nan]*6,100)[0], 'INCOMPLETE_OUTPUT')

    def test_scope_excludes_prospective_and_immature(self):
        d = pd.DataFrame({'issue_date':['2025-01-02','2026-08-02','2026-09-02','2026-10-28']})
        self.assertEqual(len(mature(d)), 2)

    def test_persistence_requires_calendar_consecutiveness(self):
        self.assertEqual(longest_run(['2025-01-02','2025-03-02','2025-04-02']), 2)

    def test_product_aggregation_no_window_double_count(self):
        d = pd.concat([fixture({}), fixture({},issue='2025-02-02')],ignore_index=True)
        r = product_ranking(build_objects(d)).iloc[0]
        self.assertEqual(r.absolute_error60, 200)
        self.assertEqual(r.segment_absolute_error_sum, 200)
        self.assertEqual(r.issues, 2)


if __name__ == '__main__': unittest.main()
