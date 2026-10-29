"""Synthetic unit tests.  These are not actuals of any live shadow batch."""
import unittest

import numpy as np
import pandas as pd

from scripts.shadow_actuals_scoring import WINDOWS, score_windows


def fixture(issue="2026-10-29", multi=False):
    day = pd.Timestamp(issue)
    calendar = {"d01_30": (1, 30), "d31_60": (31, 60), "d01_60": (1, 60)}
    truths = {"A": (10.0, 20.0, 30.0), "B": (0.0, 0.0, 0.0), "C": (5.0, 5.0, 10.0)}
    candidate = {"A": (12.0, 16.0, 28.0), "B": (2.0, 3.0, 5.0), "C": (np.nan,) * 3}
    pred, truth = [], []
    for spu in truths:
        for i, window in enumerate(WINDOWS):
            begin, end = calendar[window]
            row = dict(shop="S", spu=spu, window=window,
                       target_start=(day + pd.Timedelta(days=begin)).strftime("%Y-%m-%d"),
                       target_end=(day + pd.Timedelta(days=end)).strftime("%Y-%m-%d"))
            if multi:
                row["issue_date"] = issue
            truth.append(row | dict(actual=truths[spu][i], actual_status="COMPLETE"))
            for model, values in (("BASE", candidate), ("F28", truths)):
                value = values[spu][i]
                pred.append(row | dict(model=model, prediction=value,
                                       prediction_status="OK" if np.isfinite(value) else "NO_QUALIFICATION"))
    pair = pd.DataFrame({"shop": ["S", "S"], "spu": ["A", "B"]})
    if multi:
        pair["issue_date"] = issue
    return pd.DataFrame(pred), pd.DataFrame(truth), pair


def row(frame, model="BASE", window="d01_30", scope="FULL_CORE"):
    query = frame.model.eq(model) & frame.scope.eq(scope)
    if "window" in frame:
        query &= frame.window.eq(window)
    return frame.loc[query].iloc[0]


class ShadowScoringTests(unittest.TestCase):
    def test_fixed_denominators_and_missing_output(self):
        p, t, pairs = fixture()
        r = score_windows(p, t, pairs, dict(d01_30=30, d31_60=50, d01_60=80))
        m = row(r["metrics"])
        self.assertEqual((m.fixed_n, m.output_n, m.truth_n, m.scored_n), (3, 2, 3, 2))
        self.assertEqual((m.actual_units, m.known_actual_units, m.ae), (10, 15, 4))
        self.assertEqual(m.wape, 0.4)
        self.assertEqual(m.bias, 0.4)
        self.assertEqual(m.hit_n, 1)
        self.assertEqual(m.hit_fixed_share, 1 / 3)
        self.assertEqual(m.hit_actual_share, 10 / 15)
        self.assertEqual(m.hit_actual_full_store_share, 10 / 30)
        self.assertEqual(row(r["metrics"], scope="PAIRED").hit_actual_share, 1)

    def test_joint_hits_once_and_actual60_weight(self):
        p, t, pairs = fixture()
        r = score_windows(p, t, pairs)
        m = row(r["joint_summary"])
        self.assertEqual((m.fixed_n, m.joint_hit_n, m.joint_hit_actual, m.known_actual), (3, 1, 30, 40))
        self.assertEqual(m.joint_hit_actual_share, 0.75)
        self.assertEqual(row(r["joint_summary"], model="F28").joint_hit_n, 2)

    def test_no_future_actual_produces_no_scores_or_zero_hits(self):
        p, t, pairs = fixture()
        t["actual"] = np.nan
        t["actual_status"] = "NOT_MATURED"
        r = score_windows(p, t, pairs)
        self.assertTrue(r["metrics"].wape.isna().all())
        self.assertTrue(r["metrics"].hit_n.isna().all())
        self.assertTrue(r["metrics"].actual_units.isna().all())
        self.assertTrue(r["joint_summary"].joint_hit_n.isna().all())
        self.assertTrue(r["joint_objects"].joint_hit.isna().all())
        self.assertTrue(r["metrics"].score_status.eq("WAITING_ACTUALS").all())

    def test_finite_partial_actual_not_scored_and_pair_cannot_bypass_gate(self):
        p, t, pairs = fixture()
        t.loc[t.spu.eq("C"), "actual_status"] = "MISSING_DAYS"
        r = score_windows(p, t, pairs)
        self.assertTrue(r["metrics"].wape.isna().all())
        self.assertTrue(r["joint_summary"].joint_hit_n.isna().all())
        self.assertTrue(r["metrics"].score_status.eq("INCOMPLETE").all())
        self.assertTrue(r["joint_summary"].score_status.eq("INCOMPLETE").all())
        self.assertFalse(r["metrics"].formal_scoring_allowed.any())
        self.assertTrue(r["details"].loc[r["details"].spu.eq("C"), "scoring_actual"].isna().all())

    def test_first_segment_can_mature_without_joint_scoring(self):
        p, t, pairs = fixture()
        t.loc[t.window.ne("d01_30"), "actual_status"] = "NOT_MATURED"
        r = score_windows(p, t, pairs)
        self.assertEqual(row(r["metrics"]).wape, 0.4)
        self.assertTrue(r["joint_summary"].joint_hit_n.isna().all())
        self.assertTrue(r["metrics"].loc[r["metrics"].window.ne("d01_30"), "wape"].isna().all())

    def test_zero_actual_has_absolute_error_not_percentage(self):
        p, t, pairs = fixture()
        r = score_windows(p, t, pairs)
        self.assertEqual(row(r["metrics"]).zero_truth_ae, 2)
        self.assertTrue(r["details"].loc[r["details"].spu.eq("B"), "relative_error"].isna().all())
        p = p[p.spu.eq("B")]
        t = t[t.spu.eq("B")]
        r = score_windows(p, t, pairs[pairs.spu.eq("B")])
        self.assertTrue(r["metrics"].wape.isna().all())
        self.assertEqual(row(r["metrics"]).ae, 2)

    def test_missing_truth_record_is_retained_unknown(self):
        p, t, pairs = fixture()
        r = score_windows(p, t.iloc[1:], pairs)
        self.assertEqual(len(r["details"]), len(p))
        self.assertEqual((r["details"].actual_status == "MISSING_RECORD").sum(), 2)
        self.assertTrue(np.isnan(row(r["metrics"]).wape))

    def test_missing_prediction_record_does_not_shrink_fixed_scope(self):
        p, t, pairs = fixture()
        p = p[~(p.model.eq("BASE") & p.spu.eq("C"))]
        r = score_windows(p, t, pairs)
        self.assertEqual(len(r["details"]), 18)
        self.assertEqual(row(r["metrics"]).fixed_n, 3)
        self.assertEqual(row(r["metrics"]).missing_output_n, 1)

    def test_duplicate_keys_rejected(self):
        p, t, pairs = fixture()
        for bad in [(pd.concat([p, p.iloc[[0]]]), t, pairs),
                    (p, pd.concat([t, t.iloc[[0]]]), pairs),
                    (p, t, pd.concat([pairs, pairs.iloc[[0]]]))]:
            with self.assertRaisesRegex(ValueError, "duplicate"):
                score_windows(*bad)

    def test_date_mismatch_and_bad_window_rejected(self):
        p, t, pairs = fixture()
        t.loc[0, "target_end"] = "2026-11-27"
        with self.assertRaisesRegex(ValueError, "dates disagree"):
            score_windows(p, t, pairs)
        p, t, pairs = fixture()
        p.loc[p.window.eq("d31_60"), "target_start"] = "2026-11-28"
        with self.assertRaisesRegex(ValueError, "calendar length"):
            score_windows(p, t, pairs)

    def test_actual_and_prediction_sums_are_checked(self):
        p, t, pairs = fixture()
        t.loc[t.spu.eq("A") & t.window.eq("d01_60"), "actual"] = 31
        with self.assertRaisesRegex(ValueError, "actual cumulative"):
            score_windows(p, t, pairs)
        p, t, pairs = fixture()
        p.loc[p.spu.eq("A") & p.model.eq("BASE") & p.window.eq("d01_60"), "prediction"] = 29
        with self.assertRaisesRegex(ValueError, "prediction cumulative"):
            score_windows(p, t, pairs)

    def test_status_contradictions_and_negative_values_rejected(self):
        p, t, pairs = fixture()
        p.loc[0, "prediction"] = np.nan
        with self.assertRaisesRegex(ValueError, "status/value"):
            score_windows(p, t, pairs)
        p, t, pairs = fixture()
        t.loc[0, "actual"] = np.nan
        with self.assertRaisesRegex(ValueError, "COMPLETE truth"):
            score_windows(p, t, pairs)
        p, t, pairs = fixture()
        t.loc[0, "actual"] = -1
        with self.assertRaisesRegex(ValueError, "negative"):
            score_windows(p, t, pairs)

    def test_boundary_is_unrounded_and_inclusive(self):
        p, t, pairs = fixture()
        r = score_windows(p, t, pairs)
        q = r["details"].query("model == 'BASE' and spu == 'A' and window == 'd01_30'")
        self.assertTrue(q.hit.iloc[0])
        p.loc[p.model.eq("BASE") & p.spu.eq("A") & p.window.eq("d01_30"), "prediction"] = 12.000000001
        p.loc[p.model.eq("BASE") & p.spu.eq("A") & p.window.eq("d01_60"), "prediction"] = 28.000000001
        r = score_windows(p, t, pairs)
        self.assertFalse(r["details"].query("model == 'BASE' and spu == 'A' and window == 'd01_30'").hit.iloc[0])

    def test_multiperiod_does_not_merge_same_product(self):
        p, t, pairs = fixture(multi=True)
        p2, t2, pair2 = fixture("2026-11-29", multi=True)
        r = score_windows(pd.concat([p, p2]), pd.concat([t, t2]), pd.concat([pairs, pair2]))
        m = row(r["metrics"])
        self.assertEqual((m.fixed_n, m.products, m.periods), (6, 3, 2))
        self.assertEqual(row(r["joint_summary"]).joint_hit_n, 2)
        self.assertEqual(len(r["coverage"]), 6)

    def test_inputs_unmodified_and_truth_perturbation_does_not_change_forecast(self):
        p, t, pairs = fixture()
        saved = [x.copy(deep=True) for x in (p, t, pairs)]
        score_windows(p, t, pairs)
        for before, after in zip(saved, (p, t, pairs)):
            pd.testing.assert_frame_equal(before, after)
        t["actual"] *= 2
        r = score_windows(p, t, pairs)
        q = r["details"].sort_values(["shop", "spu", "model", "window"])
        old = p.sort_values(["shop", "spu", "model", "window"])
        np.testing.assert_array_equal(q.prediction.to_numpy(), old.prediction.to_numpy())

    def test_unknown_store_denominator_not_zero(self):
        p, t, pairs = fixture()
        r = score_windows(p, t, pairs, {w: "UNKNOWN" for w in WINDOWS})
        self.assertTrue(r["metrics"].full_store_actual_units.isna().all())
        self.assertTrue(r["metrics"].hit_actual_full_store_share.isna().all())
        with self.assertRaisesRegex(ValueError, "invalid full-store"):
            score_windows(p, t, pairs, {"d01_30": -1})

    def test_outside_pair_and_outside_truth_rejected(self):
        p, t, pairs = fixture()
        pairs.loc[0, "spu"] = "OUTSIDE"
        with self.assertRaisesRegex(ValueError, "outside full core"):
            score_windows(p, t, pairs)
        p, t, pairs = fixture()
        extra = t.iloc[[0]].copy()
        extra["spu"] = "OUTSIDE"
        with self.assertRaisesRegex(ValueError, "truth object outside"):
            score_windows(p, pd.concat([t, extra]), pairs)


if __name__ == "__main__":
    unittest.main()
