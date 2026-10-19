"""Synthetic only: no estimator fit, production inputs, database, or network."""
import unittest

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator

from scripts.bc60_v2_b1_diagnostic import (
    FEATURES, RecursionTrace, compare_archived, curve_diagnostics,
    full_core_daily, future_template, predict_traced, stage_scores,
    summarize_stages,
)


class NeverFitPredictor(BaseEstimator):
    def fit(self, *args, **kwargs):
        raise AssertionError("NO_MODEL_FIT_ALLOWED")

    def predict(self, x):
        return x.lag1.to_numpy()+.25*x.rolling_mean_lag1_window_size7.to_numpy()-3


def fixture():
    from mlforecast import MLForecast
    from mlforecast.lag_transforms import RollingMean
    history = pd.DataFrame([
        {"unique_id": uid, "ds": day, "y": float(offset+j % 11), "shop_code": shop,
         "shop": f"synthetic_shop_{shop}", "spu": uid, "sales_state": "OBSERVED", "mapped": True}
        for uid, offset, shop in [("synthetic_b", 20, 1), ("synthetic_a", 5, 0)]
        for j, day in enumerate(pd.date_range("1999-10-01", periods=70))
    ])
    model = MLForecast(models={"B1": NeverFitPredictor()}, freq="D", lags=[1,7,14,28],
                       lag_transforms={1: [RollingMean(7), RollingMean(28)]},
                       date_features=["dayofweek", "month", "dayofyear"])
    processed = model.preprocess(history[["unique_id", "ds", "y", "shop_code"]], static_features=["shop_code"])
    # Synthetic fixture injection only, not the production implementation: no fit.
    model.models_ = {"B1": NeverFitPredictor()}
    terminal = {uid: g.copy() for uid, g in history.groupby("unique_id", sort=False)}
    idmap = {uid: (g.shop.iloc[0], g.spu.iloc[0]) for uid, g in terminal.items()}
    cutoff = history.ds.max()
    issue = str((cutoff+pd.Timedelta(days=2)).date())
    return model, history, processed, terminal, idmap, cutoff, issue


class B1CallbackTests(unittest.TestCase):
    def test_raw_target_identifier_exclusion_and_shifted_features(self):
        _, history, processed, _, _, _, _ = fixture()
        self.assertEqual([c for c in processed if c not in ["unique_id", "ds", "y"]], FEATURES)
        for uid, g in history.groupby("unique_id"):
            g = g.sort_values("ds").set_index("ds")
            z = processed[processed.unique_id.eq(uid)].set_index("ds")
            np.testing.assert_array_equal(z.y, g.y.reindex(z.index))
            for lag in (1,7,14,28):
                np.testing.assert_allclose(z[f"lag{lag}"], g.y.shift(lag).reindex(z.index))
            for width in (7,28):
                np.testing.assert_allclose(z[f"rolling_mean_lag1_window_size{width}"],
                                           g.y.shift(1).rolling(width).mean().reindex(z.index))

    def test_public_callbacks_separate_output_and_feedback_62_steps(self):
        model, _, _, terminal, idmap, cutoff, issue = fixture()
        template, order = future_template(model, terminal)
        self.assertEqual(order, ["synthetic_a", "synthetic_b"])
        self.assertEqual(list(terminal), ["synthetic_b", "synthetic_a"])
        d0, f0, _ = predict_traced(model, "D0", terminal, idmap, cutoff, issue)
        d1, f1, _ = predict_traced(model, "D1", terminal, idmap, cutoff, issue)
        np.testing.assert_allclose(d0[d0.relative_day.eq(-1)].forecast, d1[d1.relative_day.eq(-1)].forecast)
        self.assertFalse(np.allclose(d0.forecast, d1.forecast))
        self.assertFalse(np.allclose(f1.model_output_nonnegative, f1.recursive_feedback))
        self.assertTrue(f1.recursive_feedback.eq(f1.cutoff_known28_mean).all())
        self.assertTrue(f0.recursive_feedback.eq(f0.model_output_nonnegative).all())
        self.assertEqual(sorted(d1.relative_day.unique()), list(range(-1,61)))
        self.assertEqual(len(d1), 124)
        for lag in (1,7,14,28):
            self.assertTrue(f1.loc[f1.recursive_step.le(lag), f"lag{lag}_observed_n"].eq(1).all())
            self.assertTrue(f1.loc[f1.recursive_step.gt(lag), f"lag{lag}_feedback_n"].eq(1).all())
            z = f1[f1.recursive_step.eq(lag+1)]
            np.testing.assert_allclose(z[f"lag{lag}"], z.cutoff_known28_mean)
        for width in (7,28):
            name = f"rolling_mean_lag1_window_size{width}"
            self.assertTrue((f1[name+"_observed_n"]+f1[name+"_feedback_n"]).eq(width).all())
            z = f1[f1.recursive_step.eq(width+1)]
            np.testing.assert_allclose(z[name], z.cutoff_known28_mean)
        repeated, _, _ = predict_traced(model, "D0", terminal, idmap, cutoff, issue)
        np.testing.assert_allclose(repeated.forecast, d0.forecast)
        self.assertTrue(d0[["unique_id", "ds"]].equals(template))

    def test_unknown_proxy_day_is_rejected_not_zero_filled(self):
        _, _, _, terminal, idmap, cutoff, issue = fixture()
        terminal["synthetic_a"].loc[terminal["synthetic_a"].index[-1], "sales_state"] = "UNKNOWN"
        with self.assertRaisesRegex(ValueError, "28_KNOWN"):
            RecursionTrace("D1", list(terminal), terminal, idmap, cutoff, issue)

    def test_misaligned_series_and_unclipped_feedback_rejected_or_detected(self):
        model, _, _, terminal, idmap, cutoff, issue = fixture()
        _, order = future_template(model, terminal)
        trace = RecursionTrace("D1", order, terminal, idmap, cutoff, issue)
        captured = []
        def capture(x):
            captured.append(x.copy())
            return x
        model.predict(1, before_predict_callback=capture)
        bad = captured[0].iloc[::-1].reset_index(drop=True)
        with self.assertRaises(AssertionError):
            trace.before(bad)
        trace.before(captured[0])
        feedback = trace.after(np.array([-4., 2.]))
        np.testing.assert_allclose(trace.captured[0], [0.,2.])
        np.testing.assert_allclose(feedback, trace.proxy)

    def test_reproduction_requires_identical_keys_and_tolerance(self):
        base = pd.DataFrame({"issue_date": ["2000-01-01"], "shop": ["synthetic"],
                             "spu": ["synthetic_a"], "ds": [pd.Timestamp("2000-01-02")], "forecast": [1.]})
        self.assertEqual(compare_archived(base, base)["status"], "PASS")
        changed = base.copy(); changed["forecast"] += 1e-6
        self.assertEqual(compare_archived(changed, base)["status"], "FAIL")
        changed = base.copy(); changed["spu"] = "different"
        self.assertEqual(compare_archived(changed, base)["status"], "FAIL")

    def test_fixed_core_missing_outputs_bridge_and_error_identity(self):
        _, _, _, _, _, cutoff, issue = fixture()
        core = pd.DataFrame({"shop": ["s", "s"], "spu": ["a", "b"],
                             "original_b1_available": [True, False]})
        dates = pd.date_range(cutoff+pd.Timedelta(days=1), periods=62)
        truth = pd.DataFrame([{"shop": "s", "spu": spu, "ds": day, "y": float(i+1), "sales_state": "OBSERVED"}
                              for spu in ["a", "b"] for i, day in enumerate(dates)])
        predictions = pd.DataFrame({"shop": "s", "spu": "a", "ds": dates, "forecast": 10.})
        daily = full_core_daily(core, issue, {"D0": predictions, "D1": predictions}, truth, "PASS")
        scores = stage_scores(daily)
        self.assertEqual(len(daily), 248)
        self.assertEqual(len(scores), 36)
        self.assertTrue(scores[scores.spu.eq("b")].prediction.isna().all())
        self.assertEqual(scores[(scores.spu.eq("a")) & scores.stage.eq("bridge")].iloc[0].actual, 3.)
        self.assertEqual(scores[(scores.spu.eq("a")) & scores.stage.eq("d01_60")].iloc[0].actual, sum(range(3,63)))
        summaries = summarize_stages(scores)
        overall = summaries[(summaries.slice.eq("overall")) & summaries.scope.eq("FULL_FROZEN_CORE")]
        self.assertTrue(overall.fixed_core_n.eq(2).all())
        self.assertTrue((overall.hit_count+overall.miss_count+overall.unjudged_count).eq(2).all())
        curves, growth = curve_diagnostics(scores)
        self.assertEqual(len(curves), 10)
        np.testing.assert_allclose(growth.identity_residual.dropna(), 0., atol=1e-12)


if __name__ == "__main__":
    unittest.main()
