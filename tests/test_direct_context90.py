"""Synthetic checks only: no original archives, DB or business training."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import direct_context90_experiment as experiment
from scripts import bc60_v3_direct as direct


def context(values=None, drop=None, invalid=None, cutoff="2024-06-30"):
    dates = pd.date_range("2024-01-01", "2024-07-31")
    if values is None:
        values = (np.arange(len(dates)) % 17 + 1).astype(float)
    frame = pd.DataFrame({"ds": dates, "y": values, "shop": "TEST", "spu": "SYNTHETIC",
                          "sales_state": "OBSERVED", "mapped": True})
    if drop is not None:
        frame = frame[frame.ds.ne(pd.Timestamp(drop))]
    if invalid is not None:
        date, value = invalid
        frame.loc[frame.ds.eq(pd.Timestamp(date)), "y"] = value
    monthly = pd.DataFrame({"shop": ["TEST"]*6, "spu": ["SYNTHETIC"]*6,
                            "month": pd.date_range("2023-07-01", periods=6, freq="MS"),
                            "qty": [100]*6})
    return direct.Context(frame, monthly, ["TEST"], cutoff), frame, monthly


class Context90Tests(unittest.TestCase):
    def test_calendar_edges_and_row_specific_cutoffs(self):
        ctx, d, _ = context()
        cuts = pd.to_datetime(["2024-03-30", "2024-04-11", "2024-06-30"])
        got = experiment.features_at(ctx, ("TEST", "SYNTHETIC"), cuts)
        s = d.set_index("ds").y
        for i, c in enumerate(cuts):
            for n in (56, 90):
                window = s.reindex(pd.date_range(c-pd.Timedelta(days=n-1), c))
                self.assertAlmostEqual(got.loc[i, "m"+str(n)], window.mean())
                recent, earlier = window.iloc[-28:].mean(), window.iloc[:-28].mean()
                self.assertAlmostEqual(got.loc[i, "contrast"+str(n)],
                                       (recent-earlier)/(recent+earlier))
        self.assertNotEqual(got.loc[0, "m56"], got.loc[2, "m56"])

    def test_missing_day_does_not_become_zero_or_shrink_window(self):
        ctx, _, _ = context(drop="2024-03-01")
        result = experiment.features_at(ctx, ("TEST", "SYNTHETIC"), ["2024-04-30"]).iloc[0]
        self.assertEqual(result.history90_complete, 0)
        self.assertTrue(np.isnan(result.m90))
        self.assertTrue(np.isnan(result.contrast90))
        self.assertEqual(result.history56_complete, 1)

    def test_negative_nan_invalid(self):
        for bad in [np.nan, -1.0]:
            ctx, _, _ = context(invalid=("2024-04-20", bad))
            result = experiment.features_at(ctx, ("TEST", "SYNTHETIC"), ["2024-04-30"]).iloc[0]
            self.assertEqual(result.history56_complete, 0)
            self.assertTrue(np.isnan(result.contrast56))

    def test_explicit_zero_and_denominator(self):
        ctx, _, _ = context(values=np.zeros(213))
        result = experiment.features_at(ctx, ("TEST", "SYNTHETIC"), ["2024-06-30"]).iloc[0]
        self.assertEqual(result.history90_complete, 1)
        self.assertEqual(result.m90, 0)
        self.assertEqual(result.contrast90, 0)

    def test_boundary_unknown(self):
        ctx, _, _ = context()
        z = experiment.features_at(ctx, ("TEST", "SYNTHETIC"), ["2024-02-25"]).iloc[0]
        self.assertEqual(z.history56_complete, 1)
        self.assertEqual(z.history90_complete, 0)
        self.assertTrue(np.isnan(z.m90))

    def test_augmentation_retains_old_rows_columns_and_labels(self):
        ctx, _, _ = context()
        old = pd.DataFrame([ctx.row(("TEST", "SYNTHETIC"), day, "M1", "2024-06-30")
                            for day in ["2024-02-10", "2024-04-02", "2024-05-02"]])
        old["y_label"] = [20., 30., 40.]
        new, _ = experiment.augment(ctx, old)
        pd.testing.assert_frame_equal(old, new[old.columns], check_exact=True)
        self.assertEqual(len(new), 3)
        self.assertTrue(np.isnan(new.loc[0, "m90"]))
        self.assertEqual(set(new.columns)-set(old.columns), set(experiment.EXTRA))

    def test_labels_and_segment_independence(self):
        ctx, _, _ = context()
        for segment, shift in [("M1", 30), ("M2", 60)]:
            train, _ = ctx.training("2024-06-30", segment)
            self.assertTrue(train.label_end.le(ctx.cutoff).all())
            self.assertTrue((train.simulated_issue_date+pd.Timedelta(days=shift)).eq(train.label_end).all())
            self.assertTrue((train.simulated_issue_date-pd.Timedelta(days=2)).eq(train.input_cutoff).all())
        m1 = ctx.row(("TEST", "SYNTHETIC"), "2024-04-02", "M1", "2024-04-02")
        m2 = ctx.row(("TEST", "SYNTHETIC"), "2024-04-02", "M2", "2024-04-02")
        self.assertEqual(m1["lag1"], m2["lag1"])
        self.assertNotEqual(m1["label_start"], m2["label_start"])

    def test_decline_group_uses_pre_cutoff_only(self):
        vals = np.r_[np.ones(34)*30, np.ones(28)*20, np.ones(28)*10, np.ones(123)*999]
        ctx, _, _ = context(values=vals)
        frame = pd.DataFrame([ctx.row(("TEST", "SYNTHETIC"), "2024-04-01", "M1", "2024-04-01")])
        _, obs = experiment.augment(ctx, frame)
        self.assertEqual(obs.iloc[0].decline_group, "PRE_ISSUE_DECLINING")

    def test_native_nan_fit_keeps_rows_and_future_perturbation_predictions(self):
        from lightgbm import LGBMRegressor
        ctx, daily, monthly = context()
        dates = pd.date_range("2024-02-01", "2024-04-30")
        old = pd.DataFrame([ctx.row(("TEST", "SYNTHETIC"), day, "M1", "2024-06-30") for day in dates])
        new, _ = experiment.augment(ctx, old)
        params = dict(objective="poisson", n_estimators=200, learning_rate=.05,
                      num_leaves=31, min_child_samples=40, reg_lambda=1.,
                      random_state=20261019, n_jobs=2, deterministic=True,
                      force_col_wise=True, verbosity=-1)
        model = LGBMRegressor(**params).fit(new[experiment.FEATURES], np.arange(len(new))%21+1)
        self.assertTrue(np.isfinite(model.predict(new[experiment.FEATURES])).all())
        self.assertTrue(new.m90.isna().any())
        tree = model.booster_.dump_model()["tree_info"][0]["tree_structure"]
        def leaf_count(node):
            return node["leaf_count"] if "leaf_count" in node else leaf_count(node["left_child"])+leaf_count(node["right_child"])
        self.assertEqual(leaf_count(tree), len(new))
        self.assertNotIn("zero_as_missing", params)
        daily.loc[daily.ds.gt("2024-03-31"), "y"] = 999999
        changed = direct.Context(daily, monthly, ["TEST"], "2024-06-30")
        raw1 = pd.DataFrame([ctx.row(("TEST", "SYNTHETIC"), "2024-04-02", "M2", "2024-04-02")])
        raw2 = pd.DataFrame([changed.row(("TEST", "SYNTHETIC"), "2024-04-02", "M2", "2024-04-02")])
        a, _ = experiment.augment(ctx, raw1)
        b, _ = experiment.augment(changed, raw2)
        pd.testing.assert_frame_equal(a, b, check_exact=True)
        np.testing.assert_array_equal(model.predict(a[experiment.FEATURES]), model.predict(b[experiment.FEATURES]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", required=True)
    args = parser.parse_args()
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Context90Tests))
    receipt = {"at": dt.datetime.now(dt.timezone.utc).isoformat(),
               "status": "PASS" if result.wasSuccessful() else "FAIL",
               "tests": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
               "synthetic_fits": 1, "business_fits": 0,
               "test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    with Path(args.receipt).open("x", encoding="utf-8") as f:
        json.dump(receipt, f, indent=2)
    sys.exit(0 if result.wasSuccessful() else 1)
