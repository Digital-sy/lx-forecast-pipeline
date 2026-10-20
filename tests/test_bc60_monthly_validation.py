import datetime as dt
import unittest
from unittest.mock import patch
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.bc60_monthly_validate import (issue_grid, target_window, direct_dates, positive_completed_months,
    daily_total, baseline_predictions, rank_core, selection_snapshot, score_record, metrics,
    paired_with_f28, paired_metrics, f28_shape, joint_decision, direct_features)
from scripts.bc60_monthly_validate import (CalendarOracle, monthly_positive_ends,
    reconstruct_direct_training, reconstruct_recursive_training, nonoverlap_count, slot_evidence_key)


class MonthlyIndependentValidationTests(unittest.TestCase):
    def source(self, first="2024-01-01", last="2026-10-01", objects=None):
        objects = objects or [("JQ-US", "a", 10., True)]
        return pd.DataFrame([{"shop": shop, "spu": spu, "ds": day, "y": quantity,
                              "mapped": mapped, "sales_state": "OBSERVED" if quantity else "EXPLICIT_ZERO"}
            for shop, spu, quantity, mapped in objects for day in pd.date_range(first, last)])

    def test_exact_twenty_mature_and_two_immature_issue_months(self):
        grid = issue_grid()
        self.assertEqual(len(grid), 22 * 3)
        annual = grid[grid.window.eq("d01_60")]
        self.assertEqual(annual.calendar_mature.sum(), 20)
        self.assertEqual(annual.loc[~annual.calendar_mature, "issue_month"].tolist(), ["2026-09", "2026-10"])
        self.assertFalse(grid[grid.issue_month.isin(["2026-09", "2026-10"])].calendar_mature.any())

    def test_targets_are_exact_days_not_calendar_months(self):
        self.assertEqual(target_window("2025-01-02", "d01_30"), (dt.date(2025, 1, 3), dt.date(2025, 2, 1)))
        self.assertEqual(target_window("2025-01-02", "d31_60"), (dt.date(2025, 2, 2), dt.date(2025, 3, 3)))
        self.assertEqual(target_window("2025-02-02", "d01_60")[1], dt.date(2025, 4, 3))
        self.assertEqual(target_window("2026-08-02", "d01_60")[1], dt.date(2026, 10, 1))
        self.assertEqual(target_window("2026-09-02", "d01_30")[1], dt.date(2026, 10, 2))

    def test_separate_direct_maturity_and_two_bridge_days(self):
        one = direct_dates("2025-01-02", "2024-12-01", "M1")
        two = direct_dates("2025-01-02", "2024-11-01", "M2")
        self.assertTrue(one["label_mature"])
        self.assertTrue(two["label_mature"])
        self.assertEqual(one["label_end"], dt.date(2024, 12, 31))
        self.assertEqual(two["label_end"], dt.date(2024, 12, 31))
        self.assertFalse(direct_dates("2025-01-02", "2024-12-02", "M1")["label_mature"])
        self.assertFalse(direct_dates("2025-01-02", "2024-11-02", "M2")["label_mature"])
        self.assertEqual((one["label_start"] - one["input_cutoff"]).days, 3)

    def test_leap_day_and_year_boundary_feature_calendar(self):
        row = direct_features(range(1, 29), "JQ-US", "2024-02-29")
        self.assertEqual(row["dayofyear"], 60)
        self.assertEqual(row["dayofweek"], 3)
        self.assertEqual(direct_dates("2025-01-02", "2024-02-02", "M1")["label_end"], dt.date(2024, 3, 3))

    def test_qualification_uses_only_completed_positive_months(self):
        monthly = pd.DataFrame({"shop": ["JQ-US"] * 6, "spu": ["a"] * 6,
            "month": ["2024-09", "2024-10", "2024-11", "2024-12", "2025-01", "2026-01"], "qty": [1, 1, 1, 1, 10000, 10000]})
        self.assertEqual(positive_completed_months(monthly, "JQ-US", "a", "2024-12-30"), 3)
        self.assertEqual(positive_completed_months(monthly, "JQ-US", "a", "2025-01-02"), 4)
        self.assertEqual(positive_completed_months(monthly, "JQ-US", "a", "2025-01-20"), 4)
        self.assertEqual(positive_completed_months(monthly, "JQ-US", "missing", "2025-01-02"), 0)

    def test_january_28_and_90_day_support_dates(self):
        data = self.source()
        baseline = baseline_predictions(data, "JQ-US", "a", "2025-01-02")
        _, selection = selection_snapshot(data, "JQ-US", "2025-01-02")
        self.assertEqual((baseline["history_start"], baseline["history_end"]), ("2024-12-04", "2024-12-31"))
        self.assertEqual((selection["selection_start"], selection["selection_end"]), ("2024-10-03", "2024-12-31"))

    def test_full_shop_denominator_and_boundary_ties(self):
        totals = pd.DataFrame({"spu": ["a", "b", "c"], "selection_qty": [80., 10., 10.]})
        core, boundary = rank_core(totals, 100.)
        self.assertEqual(boundary, 10.)
        self.assertTrue(core.is_core.all())
        core, boundary = rank_core(totals, 200.)
        self.assertIsNone(boundary)
        self.assertTrue(core.is_core.all())
        data = self.source(objects=[("JQ-US", "a", 8., True), ("JQ-US", "unknown", 2., False)])
        _, selection = selection_snapshot(data, "JQ-US", "2025-01-02")
        self.assertEqual(selection["selection_status"], "MAPPED_BELOW_TARGET")
        self.assertEqual(selection["selection_coverage"], .8)
        self.assertEqual(selection["unmapped_units"], 180.)

    def test_zero_total_is_known_empty_not_unknown_core(self):
        _, selection = selection_snapshot(self.source(objects=[("JQ-US", "a", 0., True)]), "JQ-US", "2025-01-02")
        self.assertEqual(selection["core_n"], 0)
        self.assertEqual(selection["selection_status"], "NO_POSITIVE_SALES")
        self.assertIsNone(selection["selection_coverage"])

    def test_missing_store_day_makes_core_unknown_not_zero(self):
        data = self.source()
        data = data[data.ds.ne(pd.Timestamp("2024-11-12"))]
        core, selection = selection_snapshot(data, "JQ-US", "2025-01-02")
        self.assertTrue(core.empty)
        self.assertIsNone(selection["core_n"])
        self.assertEqual(selection["selection_status"], "INSUFFICIENT_HISTORY")
        self.assertEqual(selection["present_days"], 89)

    def test_new_object_with_fewer_than90_days_remains_selectable(self):
        data = self.source()
        new = self.source("2024-12-20", "2024-12-31", [("JQ-US", "new", 1000., True)])
        core, selection = selection_snapshot(pd.concat([data, new]), "JQ-US", "2025-01-02")
        self.assertEqual(selection["selection_status"], "OK")
        self.assertTrue(core.set_index("spu").loc["new", "is_core"])

    def test_invalid_selection_day_is_not_skipped(self):
        data = self.source()
        data.loc[data.ds.eq(pd.Timestamp("2024-11-12")), "sales_state"] = "UNKNOWN"
        _, selection = selection_snapshot(data, "JQ-US", "2025-01-02")
        self.assertEqual(selection["selection_status"], "INVALID_SELECTION_INPUT")
        self.assertIsNone(selection["core_n"])

    def test_target_perturbation_cannot_change_core_or_baselines(self):
        data = self.source()
        before_core, before_quality = selection_snapshot(data, "JQ-US", "2025-01-02")
        before_baseline = baseline_predictions(data, "JQ-US", "a", "2025-01-02")
        data.loc[data.ds.gt(pd.Timestamp("2024-12-31")), "y"] = 1e9
        after_core, after_quality = selection_snapshot(data, "JQ-US", "2025-01-02")
        pd.testing.assert_frame_equal(before_core, after_core)
        self.assertEqual(before_quality, after_quality)
        self.assertEqual(before_baseline, baseline_predictions(data, "JQ-US", "a", "2025-01-02"))

    def test_f28_equal_segments_sum_and_weekday_exact_four_observations(self):
        data = self.source()
        data["y"] = data.ds.dt.dayofweek + 1
        result = baseline_predictions(data, "JQ-US", "a", "2025-01-02")
        self.assertEqual(result["mean28"], 4.)
        self.assertEqual(result["F28"], {"d01_30": 120., "d31_60": 120., "d01_60": 240.})
        for window in ["d01_30", "d31_60", "d01_60"]:
            a, b = target_window("2025-01-02", window)
            self.assertEqual(result["WEEKDAY4"][window], sum(day.weekday() + 1 for day in pd.date_range(a, b)))

    def test_one_missing_history_day_cannot_use_older_observation(self):
        data = self.source()
        data = data[data.ds.ne(pd.Timestamp("2024-12-20"))]
        result = baseline_predictions(data, "JQ-US", "a", "2025-01-02")
        self.assertEqual(result["input_status"], "MISSING_INPUT")
        self.assertIsNone(result["F28"]["d01_60"])
        self.assertIsNone(result["WEEKDAY4"]["d01_60"])

    def test_missing_null_invalid_mapping_and_explicit_zero_are_distinct(self):
        data = self.source("2025-01-03", "2025-02-01")
        data.loc[0, ["y", "sales_state"]] = [0., "EXPLICIT_ZERO"]
        self.assertEqual(daily_total(data, "JQ-US", "a", "2025-01-03", "2025-02-01")["actual"], 290.)
        for bad in [data.iloc[1:], data.assign(y=data.y.where(data.index != 0)), data.assign(mapped=data.index != 0), data.assign(sales_state=np.where(data.index == 0, "INVALID", data.sales_state))]:
            total = daily_total(bad, "JQ-US", "a", "2025-01-03", "2025-02-01")
            self.assertIsNone(total["actual"])
            self.assertEqual(total["truth_status"], "UNKNOWN_TRUTH")

    def test_not_matured_takes_precedence_over_partial_window_sum(self):
        total = daily_total(self.source(), "JQ-US", "a", "2026-09-03", "2026-10-02", "2026-10-01")
        self.assertIsNone(total["actual"])
        self.assertEqual(total["truth_status"], "NOT_MATURED")
        self.assertEqual(total["expected_days"], 30)

    def test_twenty_calendar_mature_does_not_guarantee_complete_truth(self):
        data = self.source()
        data = data[data.ds.ne(pd.Timestamp("2026-10-01"))]
        total = daily_total(data, "JQ-US", "a", "2026-08-03", "2026-10-01", "2026-10-01")
        self.assertEqual(total["truth_status"], "UNKNOWN_TRUTH")

    def test_shape_strict_boundary_exactly_one_point_five_can_hit(self):
        boundary = f28_shape(4., 100., 150.)
        self.assertEqual(boundary["shape_ratio"], 1.5)
        self.assertFalse(boundary["equal_prediction_cannot_hit_both"])
        self.assertEqual(boundary["level_status"], "WITHIN_FEASIBLE_LEVEL")
        self.assertEqual(score_record(100., 120.)["decision"], "HIT")
        self.assertEqual(score_record(150., 120.)["decision"], "HIT")
        self.assertTrue(f28_shape(4., 100., 150.000001)["equal_prediction_cannot_hit_both"])

    def test_shape_above_boundary_impossible_for_every_equal_prediction(self):
        self.assertTrue(f28_shape(10., 100., 151.)["equal_prediction_cannot_hit_both"])
        for prediction in np.linspace(0, 250, 1001):
            self.assertFalse(score_record(100., prediction)["decision"] == score_record(151., prediction)["decision"] == "HIT")

    def test_shape_zero_unknown_and_input_zero_never_infinite(self):
        self.assertEqual(f28_shape(1., 0., 100.)["shape_status"], "ZERO_ACTUAL")
        self.assertIsNone(f28_shape(1., 0., 100.)["shape_ratio"])
        self.assertEqual(f28_shape(1., np.nan, 100.)["shape_status"], "UNKNOWN_TRUTH")
        zero = f28_shape(0., 100., 100.)
        self.assertIsNone(zero["actual_mean1_to_input_mean"])
        self.assertEqual(zero["level_status"], "BELOW_FEASIBLE_LEVEL")
        self.assertIsNone(f28_shape(None, 100., 100.)["prediction60"])

    def test_segment_sum_and_positive_dual_hits_imply_cumulative_hit(self):
        for a, b, p, q in [(100., 150., 120., 120.), (1., 100., .8, 120.), (1000., 20., 800., 24.)]:
            self.assertEqual(score_record(a, p)["decision"], "HIT")
            self.assertEqual(score_record(b, q)["decision"], "HIT")
            self.assertEqual(score_record(a + b, p + q)["decision"], "HIT")
        self.assertEqual(joint_decision(["HIT", "HIT", "UNJUDGED"]), "UNJUDGED")

    def test_metrics_retain_missing_and_zero_in_fixed_denominators(self):
        result = metrics(pd.DataFrame({"actual": [100., 100., 0., np.nan], "prediction": [100., np.nan, 10., 100.]}))
        self.assertEqual((result["fixed_core_n"], result["positive_truth_n"], result["zero_actual_n"]), (4, 2, 1))
        self.assertEqual((result["hit_n"], result["unjudged_n"]), (1, 3))
        self.assertEqual(result["hit_rate"], .5)
        self.assertEqual(result["full_core_hit_share"], .25)
        self.assertEqual(result["hit_sales_share"], .5)
        self.assertEqual(result["zero_actual_absolute_error"], 10.)
        self.assertEqual(result["wape"], .1)

    def test_paired_comparator_is_recomputed_on_each_candidate_scope(self):
        table = pd.DataFrame({"object": [1, 2, 1, 2], "model": ["F28", "F28", "B1", "B1"],
                              "actual": [100., 100., 100., 100.], "prediction": [100., 200., 90., np.nan]})
        pairs = paired_with_f28(table, "B1", keys=["object"])
        result = paired_metrics(pairs)
        self.assertEqual((result["fixed_core_n"], result["paired_n"], result["unpaired_n"]), (2, 1, 1))
        self.assertEqual(result["f28"]["wape"], 0.)
        self.assertEqual(result["candidate"]["wape"], .1)
        self.assertEqual(metrics(table[table.model.eq("F28")])["wape"], .5)
        with self.assertRaises(ValueError):
            paired_with_f28(table.iloc[:-1], "B1", keys=["object"])

    def test_weighted_wape_is_not_average_month_or_object_percentage(self):
        result = metrics(pd.DataFrame({"actual": [1., 99.], "prediction": [2., 99.]}))
        self.assertEqual(result["wape"], .01)
        self.assertNotEqual(result["wape"], .5)

    def test_native_feature_anchors_and_no_second_segment_feedback(self):
        one = direct_features(range(1, 29), "JQ-US", "2025-01-03")
        two = direct_features(range(1, 29), "JQ-US", "2025-02-02")
        self.assertEqual([one[f"lag{lag}"] for lag in [1, 7, 14, 28]], [28., 22., 15., 1.])
        self.assertEqual(one["rolling_mean_lag1_window_size28"], 14.5)
        for field in ["lag1", "lag7", "lag14", "lag28", "rolling_mean_lag1_window_size7", "rolling_mean_lag1_window_size28"]:
            self.assertEqual(one[field], two[field])

    def test_full_direct_training_reconstruction_leap_maturity(self):
        source = self.source("2024-01-01", "2024-10-01")
        monthly = pd.DataFrame({"shop": ["JQ-US"] * 4, "spu": ["a"] * 4,
                               "month": ["2023-09", "2023-10", "2023-11", "2023-12"], "qty": [1] * 4})
        frame = reconstruct_direct_training(CalendarOracle(source), monthly_positive_ends(monthly), "2024-04-02", "M2")
        self.assertEqual(frame.simulated_issue_date.dt.strftime("%Y-%m-%d").tolist(), ["2024-01-30", "2024-01-31"])
        self.assertEqual(frame.y_label.tolist(), [300., 300.])
        self.assertEqual(frame.lag1.dtype, np.dtype("float32"))
        source.loc[source.ds.gt(pd.Timestamp("2024-03-31")), "y"] = 999999.
        later = reconstruct_direct_training(CalendarOracle(source), monthly_positive_ends(monthly), "2024-04-02", "M2")
        pd.testing.assert_frame_equal(frame, later)

    def test_recursive_independent_features_equal_public_zero_fit_transform(self):
        from lightgbm import LGBMRegressor
        from mlforecast import MLForecast
        from mlforecast.lag_transforms import RollingMean
        source = self.source("2024-01-01", "2024-03-31")
        source["y"] = np.arange(len(source), dtype=np.int64) * 7 + 1
        monthly = pd.DataFrame({"shop": ["JQ-US"] * 4, "spu": ["a"] * 4,
                               "month": ["2023-09", "2023-10", "2023-11", "2023-12"], "qty": [1] * 4})
        expected = reconstruct_recursive_training(source, monthly_positive_ends(monthly), "2024-04-02")
        training = source[["ds", "y"]].copy()
        training["unique_id"] = "JQ-US|a|run1"
        training["shop_code"] = 0
        training = training[["unique_id", "ds", "y", "shop_code"]]
        model = MLForecast(models={"B1": LGBMRegressor()}, freq="D", lags=[1, 7, 14, 28],
                           lag_transforms={1: [RollingMean(7), RollingMean(28)]}, date_features=["dayofweek", "month", "dayofyear"])
        # Public preprocessing only. No estimator fit/predict occurs in this test.
        observed = model.preprocess(training, static_features=["shop_code"])
        pd.testing.assert_frame_equal(observed.reset_index(drop=True), expected[observed.columns].reset_index(drop=True), check_exact=True)

    def test_unmapped_null_spu_is_not_a_direct_training_candidate(self):
        source = self.source("2024-01-01", "2024-10-01")
        monthly = pd.DataFrame({"shop": ["JQ-US"] * 4, "spu": ["a"] * 4,
                               "month": ["2023-09", "2023-10", "2023-11", "2023-12"], "qty": [1] * 4})
        expected = reconstruct_direct_training(CalendarOracle(source), monthly_positive_ends(monthly), "2024-04-02", "M2")
        mixed = pd.concat([source, source.assign(spu=np.nan, mapped=False)], ignore_index=True)
        observed = reconstruct_direct_training(CalendarOracle(mixed), monthly_positive_ends(monthly), "2024-04-02", "M2")
        pd.testing.assert_frame_equal(observed, expected)

    def test_recursive_segment_number_and_56_day_guard(self):
        source = self.source("2024-01-01", "2024-04-30")
        source = source[source.ds.ne(pd.Timestamp("2024-01-11"))]
        monthly = pd.DataFrame({"shop": ["JQ-US"] * 4, "spu": ["a"] * 4,
                               "month": ["2023-09", "2023-10", "2023-11", "2023-12"], "qty": [1] * 4})
        frame = reconstruct_recursive_training(source, monthly_positive_ends(monthly), "2024-05-02")
        self.assertEqual(set(frame.unique_id), {"JQ-US|a|run2"})
        self.assertEqual(frame.ds.min(), pd.Timestamp("2024-02-09"))

    def test_training_window_independence_uses_actual_30_day_intervals(self):
        self.assertEqual(nonoverlap_count(pd.date_range("2024-01-01", periods=30)), 1)
        self.assertEqual(nonoverlap_count(pd.date_range("2024-01-01", periods=31)), 2)
        self.assertEqual(nonoverlap_count(["2024-01-01", "2024-01-01", "2024-01-30", "2024-01-31"]), 2)

    def test_changed_frozen_constants_cannot_reuse_checkpoint_receipts(self):
        with patch("scripts.bc60_monthly_validate.SHOPS", ["SY-US", "MT-US", "RKZ-US", "JQ-US"]):
            with self.assertRaisesRegex(AssertionError, "constants changed"):
                slot_evidence_key(Path("not_read_for_this_test"), {"issue_date": "2025-01-02", "kind": "B1"})


if __name__ == "__main__":
    unittest.main()
