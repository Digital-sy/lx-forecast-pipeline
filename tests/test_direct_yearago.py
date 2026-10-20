"""Synthetic-only checks; no production reads and no business fits."""
import unittest
import numpy as np
import pandas as pd
from scripts import direct_yearago_experiment as e


def daily(value=2.):
    ds = pd.date_range("2023-01-01", "2026-06-01")
    return pd.DataFrame({"shop": "S", "spu": "A", "ds": ds, "y": value,
                         "mapped": True, "sales_state": "OBSERVED"})


class YearAgoTests(unittest.TestCase):
    def test_anchor_leap_and_year(self):
        for date in ["2024-02-29", "2025-01-02", "2026-12-31"]:
            for s in ["M1", "M2"]:
                a, b, t, u = e.annual_bounds(date, s)
                self.assertEqual((b-a).days+1, 28)
                self.assertEqual((u-t).days+1, 30)
                self.assertLessEqual(u, pd.Timestamp(date)-pd.Timedelta(days=2))
        self.assertEqual(e.annual_bounds("2024-02-29", "M1")[2], pd.Timestamp("2023-03-01"))
        self.assertEqual(e.annual_bounds("2025-01-30", "M1")[0], pd.Timestamp("2024-01-01"))

    def test_zero_missing_null_and_unsafe(self):
        d = daily(0.)
        z = e.DailyIndex(d).features(("S", "A"), ["2025-07-02"], "M1").iloc[0]
        self.assertEqual(z.ly_shape_contrast, 0.)
        self.assertEqual(z.annual_state, "BOTH_COMPLETE_NO_PRIOR_POSITIVE")
        self.assertEqual(z.ly_input_zero_days, 28)
        d = d[d.ds.ne(pd.Timestamp("2024-06-15"))]
        q = e.DailyIndex(d).features(("S", "A"), ["2025-07-02"], "M1").iloc[0]
        self.assertTrue(np.isnan(q.ly_input_mean28))
        self.assertEqual(q.ly_input_absent_days, 1)
        self.assertEqual(q.annual_state, "ONE_WINDOW_COMPLETE")
        d.loc[d.ds.eq("2024-07-05"), "y"] = np.nan
        q = e.DailyIndex(d).features(("S", "A"), ["2025-07-02"], "M1").iloc[0]
        self.assertEqual(q.annual_state, "UNSAFE_MAPPING_OR_SOURCE")
        self.assertEqual(q.ly_target_unsafe_days, 1)
        self.assertTrue(np.isnan(q.ly_shape_contrast))

    def test_positive_inclusive_and_no_future(self):
        d = daily(0.)
        issue = "2025-07-02"
        start = e.annual_bounds(issue, "M1")[0]
        d.loc[d.ds.eq(start), "y"] = 1
        q = e.DailyIndex(d).features(("S", "A"), [issue], "M1")
        self.assertEqual(q[e.FLAGS[2]].iloc[0], 1)
        d.loc[d.ds.eq(start), "y"] = 0
        d.loc[d.ds.gt(pd.Timestamp(issue)), "y"] = 999
        q = e.DailyIndex(d).features(("S", "A"), [issue], "M1")
        self.assertEqual(q[e.FLAGS[2]].iloc[0], 0)

    def test_future_perturbation_and_isolation(self):
        d = daily()
        issues = pd.to_datetime(["2025-05-02", "2025-07-02"])
        original = e.DailyIndex(d)
        for issue in issues:
            other = d.copy()
            other.loc[other.ds.gt(issue-pd.Timedelta(days=2)), "y"] = 999999
            for segment in ["M1", "M2"]:
                pd.testing.assert_frame_equal(
                    original.features(("S", "A"), [issue], segment),
                    e.DailyIndex(other).features(("S", "A"), [issue], segment))
        cross = d.assign(shop="OTHER", y=999)
        q = e.DailyIndex(pd.concat([d, cross])).features(("S", "A"), issues, "M2")
        self.assertTrue(q.ly_target_mean30.eq(2).all())
        self.assertTrue(e.DailyIndex(d).features(("OTHER", "A"), issues, "M1").ly_target_mean30.isna().all())

    def test_contrast(self):
        q = e.contrast([0, 1, 0, np.nan, 4], [0, 0, 1, 0, 6])
        np.testing.assert_allclose(q, [0, 1, -1, np.nan, -.2], equal_nan=True)

    def test_segment_reference_is_distinct(self):
        d = daily(1.)
        t, u = e.annual_bounds("2025-07-02", "M2")[2:]
        d.loc[d.ds.between(t, u), "y"] = 7
        index = e.DailyIndex(d)
        self.assertEqual(index.features(("S", "A"), ["2025-07-02"], "M1").ly_target_mean30.iloc[0], 1)
        self.assertEqual(index.features(("S", "A"), ["2025-07-02"], "M2").ly_target_mean30.iloc[0], 7)

    def test_support_and_rows_retained(self):
        dates = pd.date_range("2025-01-30", periods=30)
        frame = pd.MultiIndex.from_product([range(10), dates], names=["spu", "simulated_issue_date"]).to_frame(index=False)
        frame["shop"] = "S"
        for c in e.FLAGS:
            frame[c] = 1
        for c in e.NUMERIC:
            frame[c] = np.arange(len(frame))
        self.assertTrue(e.support(frame)["fit_allowed"])
        self.assertFalse(e.support(frame[frame.spu.ne(9)])["fit_allowed"])
        self.assertFalse(e.support(frame[frame.simulated_issue_date.ne(dates[-1])])["fit_allowed"])
        frame[e.NUMERIC] = 0
        self.assertFalse(e.support(frame)["fit_allowed"])

    def test_native_nan_and_future_fit_equivalence_synthetic(self):
        from lightgbm import LGBMRegressor
        d = daily()
        idx = e.DailyIndex(d)
        issues = pd.date_range("2023-03-01", "2025-03-01", freq="7D")
        x = idx.features(("S", "A"), issues, "M1")[e.EXTRA]
        y = np.arange(len(x)) % 13 + 1
        params = dict(n_estimators=5, min_child_samples=2, n_jobs=1,
                      verbosity=-1, random_state=20261019, deterministic=True, force_col_wise=True)
        model = LGBMRegressor(**params).fit(x, y)
        changed = d.copy()
        changed.loc[changed.ds.gt("2025-02-27"), "y"] = 123456
        xx = e.DailyIndex(changed).features(("S", "A"), issues, "M1")[e.EXTRA]
        pd.testing.assert_frame_equal(x, xx)
        self.assertTrue(x.ly_input_mean28.isna().any())
        np.testing.assert_array_equal(model.predict(x), model.predict(xx))


if __name__ == "__main__":
    import argparse
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", required=True)
    args = parser.parse_args()
    from tests.test_direct_context90_score import ScoringTests
    suite = unittest.TestSuite([
        unittest.defaultTestLoader.loadTestsFromTestCase(YearAgoTests),
        unittest.defaultTestLoader.loadTestsFromTestCase(ScoringTests)])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    path = Path(args.receipt)
    if path.exists():
        raise ValueError("RECEIPT_EXISTS")
    e.dump(path, {"status": "PASS" if result.wasSuccessful() else "FAIL",
                  "tests": result.testsRun, "failures": len(result.failures),
                  "errors": len(result.errors), "at": e.now(), "test_sha256": e.sha(__file__),
                  "synthetic_fits": 1, "business_fits": 0})
    raise SystemExit(0 if result.wasSuccessful() else 1)
