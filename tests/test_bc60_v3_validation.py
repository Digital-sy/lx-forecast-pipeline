import datetime as dt
import unittest

import numpy as np
import pandas as pd

from scripts.bc60_v3_validate import (DirectCalendar, Qualification, label_dates, label_mature,
    completed_month_end, independent_nonoverlap_count, support_status, candidate_status, add_segments,
    independent_decision, independent_joint, independent_migration, assert_key_grid, reconstruct_training)
from scripts.bc60_v2_validate import independent_metric


class V3IndependentOracleTests(unittest.TestCase):
    def daily(self):
        return pd.DataFrame([{"shop": shop, "spu": spu, "ds": date, "y": float(i + adjustment),
                              "sales_state": "OBSERVED", "mapped": True}
            for shop, spu, adjustment in [("JQ-US", "p", 1), ("MT-US", "p", 101), ("JQ-US", "q", 11)]
            for i, date in enumerate(pd.date_range("2026-01-01", "2026-10-01"))])

    def monthly(self):
        return pd.DataFrame([{"shop": "JQ-US", "spu": "p", "month": month, "qty": 10}
                             for month in ["2025-10", "2025-11", "2025-12", "2026-01"]])

    def test_original_two_bridges_and_disjoint_exact_targets(self):
        first = label_dates("2026-04-02", "M1")
        second = label_dates("2026-04-02", "M2")
        self.assertEqual(first, (dt.date(2026, 4, 3), dt.date(2026, 5, 2)))
        self.assertEqual(second, (dt.date(2026, 5, 3), dt.date(2026, 6, 1)))
        self.assertEqual((second[1] - first[0]).days + 1, 60)
        self.assertEqual((second[0] - first[1]).days, 1)

    def test_separate_training_maturity_boundaries(self):
        self.assertTrue(label_mature("2026-04-02", "2026-03-01", "M1"))
        self.assertFalse(label_mature("2026-04-02", "2026-03-02", "M1"))
        self.assertTrue(label_mature("2026-04-02", "2026-01-30", "M2"))
        self.assertFalse(label_mature("2026-04-02", "2026-01-31", "M2"))

    def test_completed_month_qualification_does_not_use_future_month_total(self):
        qualification = Qualification(self.monthly())
        self.assertEqual(qualification.count("JQ-US", "p", "2026-01-30"), 3)
        self.assertFalse(qualification.eligible("JQ-US", "p", "2026-01-30"))
        self.assertTrue(qualification.eligible("JQ-US", "p", "2026-02-02"))
        self.assertEqual(completed_month_end("2026-01-28"), dt.date(2025, 12, 31))
        self.assertEqual(completed_month_end("2026-01-31"), dt.date(2026, 1, 31))

    def test_qualification_is_historical_not_final(self):
        table = self.monthly()
        extra = pd.DataFrame([{"shop": "JQ-US", "spu": "p", "month": "2026-09", "qty": 100000}])
        self.assertEqual(Qualification(table).count("JQ-US", "p", "2026-01-30"), Qualification(pd.concat([table, extra])).count("JQ-US", "p", "2026-01-30"))

    def test_feature_anchor_and_segment_calendar(self):
        calendar = DirectCalendar(self.daily())
        first = calendar.features("JQ-US", "p", "2026-01-30", "M1")
        second = calendar.features("JQ-US", "p", "2026-01-30", "M2")
        self.assertEqual([first[f"lag{x}"] for x in [1, 7, 14, 28]], [28., 22., 15., 1.])
        self.assertEqual(first["rolling_mean_lag1_window_size28"], 14.5)
        for field in first:
            if field not in {"dayofweek", "month", "dayofyear"}:
                self.assertEqual(first[field], second[field])
        self.assertEqual(first["dayofweek"], dt.date(2026, 1, 31).weekday())
        self.assertEqual(second["dayofyear"], dt.date(2026, 3, 2).timetuple().tm_yday)

    def test_missing_calendar_day_not_last28_observations(self):
        data = self.daily()
        mask = (data.shop == "JQ-US") & (data.spu == "p") & (data.ds == pd.Timestamp("2026-01-20"))
        self.assertIsNone(DirectCalendar(data[~mask]).features("JQ-US", "p", "2026-01-30", "M1"))
        data.loc[mask, "y"] = np.nan
        self.assertIsNone(DirectCalendar(data).features("JQ-US", "p", "2026-01-30", "M1"))

    def test_explicit_zero_valid_unknown_or_unsafe_invalid(self):
        data = self.daily()
        mask = (data.shop == "JQ-US") & (data.spu == "p") & (data.ds == pd.Timestamp("2026-01-20"))
        data.loc[mask, ["y", "sales_state"]] = [0., "EXPLICIT_ZERO"]
        self.assertIsNotNone(DirectCalendar(data).features("JQ-US", "p", "2026-01-30", "M1"))
        data.loc[mask, "sales_state"] = "UNKNOWN"
        self.assertIsNone(DirectCalendar(data).features("JQ-US", "p", "2026-01-30", "M1"))
        data.loc[mask, ["sales_state", "mapped"]] = ["OBSERVED", False]
        self.assertIsNone(DirectCalendar(data).features("JQ-US", "p", "2026-01-30", "M1"))

    def test_target_perturbations_cannot_change_either_segment_input(self):
        data = self.daily()
        old = DirectCalendar(data)
        data.loc[data.ds > pd.Timestamp("2026-03-31"), "y"] = 1e9
        changed = DirectCalendar(data)
        for segment in ["M1", "M2"]:
            self.assertEqual(old.features("JQ-US", "p", "2026-04-02", segment), changed.features("JQ-US", "p", "2026-04-02", segment))
        self.assertNotEqual(old.label("JQ-US", "p", "2026-04-02", "M1"), changed.label("JQ-US", "p", "2026-04-02", "M1"))

    def test_shop_and_spu_history_isolation(self):
        calendar = DirectCalendar(self.daily())
        first = calendar.features("JQ-US", "p", "2026-01-30", "M2")
        self.assertEqual(calendar.features("MT-US", "p", "2026-01-30", "M2")["lag1"] - first["lag1"], 100)
        self.assertEqual(calendar.features("JQ-US", "q", "2026-01-30", "M2")["lag1"] - first["lag1"], 10)

    def test_zero_support_and_correlated_label_support(self):
        self.assertEqual(support_status([], "M1"), "NOT_TRAINABLE")
        self.assertEqual(support_status(pd.date_range("2026-01-30", periods=30), "M1"), "LIMITED_TEMPORAL_SUPPORT")
        self.assertEqual(independent_nonoverlap_count(pd.date_range("2026-01-30", periods=31), "M1"), 2)
        self.assertEqual(independent_nonoverlap_count(["2026-01-30", "2026-01-31", "2026-03-02"], "M2"), 2)

    def test_no_valid_label_is_retained_as_failure(self):
        calendar = DirectCalendar(self.daily())
        reasons, features, value = candidate_status(calendar, Qualification(self.monthly()), "2026-04-02", "2026-01-30", "JQ-US", "p", "M2")
        self.assertIn("INSUFFICIENT_POSITIVE_MONTHS", reasons)
        self.assertIsNotNone(features)
        self.assertTrue(np.isfinite(value))

    def test_segment_sum_never_replaces_missing_with_truth_or_zero(self):
        self.assertEqual(add_segments(1.25, 2.75), 4.)
        self.assertTrue(np.isnan(add_segments(np.nan, 2.75)))
        self.assertTrue(np.isnan(add_segments(1.25, np.nan)))

    def test_new_common_scope_metrics_must_recompute_baseline(self):
        baseline = pd.DataFrame({"actual": [100., 100.], "prediction": [100., 200.]})
        self.assertEqual(independent_metric(baseline)["wape"], .5)
        self.assertEqual(independent_metric(baseline.iloc[:1])["wape"], 0.)
        result = independent_metric(pd.DataFrame({"actual": [0., 100.], "prediction": [0., np.nan]}))
        self.assertEqual(result["hit_n"], 0)
        self.assertEqual(result["unjudged_n"], 2)

    def test_hit_boundaries_and_unjudged_zero_missing(self):
        self.assertEqual(independent_decision(100., 80.), "HIT")
        self.assertEqual(independent_decision(100., 120.), "HIT")
        self.assertEqual(independent_decision(100., 120.000001), "MISS")
        self.assertEqual(independent_decision(0., 0.), "UNJUDGED")
        self.assertEqual(independent_decision(100., np.nan), "UNJUDGED")
        self.assertEqual(independent_decision(np.nan, 100.), "UNJUDGED")

    def test_joint_cannot_pass_incomplete_window(self):
        self.assertEqual(independent_joint(["HIT"] * 3), "HIT")
        self.assertEqual(independent_joint(["HIT", "MISS", "HIT"]), "MISS")
        self.assertEqual(independent_joint(["HIT", "UNJUDGED", "HIT"]), "UNJUDGED")
        self.assertEqual(independent_joint(["HIT", "HIT"]), "UNJUDGED")

    def test_migration_excludes_unjudged_from_new_hits(self):
        self.assertEqual(independent_migration("MISS", "HIT"), "NEW_HIT")
        self.assertEqual(independent_migration("HIT", "MISS"), "EXITED_HIT")
        self.assertEqual(independent_migration("HIT", "HIT"), "RETAINED_HIT")
        self.assertEqual(independent_migration("MISS", "MISS"), "BOTH_MISS")
        self.assertEqual(independent_migration("UNJUDGED", "HIT"), "UNJUDGED")

    def test_exact_grid_detects_omission_extra_and_duplicate(self):
        table = pd.DataFrame({"k": [1, 2]})
        assert_key_grid(table, ["k"], {(1,), (2,)}, "test")
        for bad in [table.iloc[:1], pd.DataFrame({"k": [1, 2, 3]}), pd.DataFrame({"k": [1, 1, 2]})]:
            with self.assertRaises(AssertionError):
                assert_key_grid(bad, ["k"], {(1,), (2,)}, "test")

    def test_exhaustive_training_context_and_ordered_funnel(self):
        calendar = DirectCalendar(self.daily())
        historical_months = pd.DataFrame([{"shop": shop, "spu": spu, "month": month, "qty": 1}
            for shop, spu in [("JQ-US", "p"), ("MT-US", "p")]
            for month in ["2025-09", "2025-10", "2025-11", "2025-12"]])
        keys, funnel, objects, excluded = reconstruct_training(calendar, Qualification(historical_months), "2026-04-02", "M2")
        self.assertEqual(set(keys), {("JQ-US", "p", "2026-01-30"), ("MT-US", "p", "2026-01-30")})
        self.assertEqual(funnel["eligible_n"], 2)
        self.assertEqual(funnel["candidate_n"], sum(value for key, value in funnel.items() if key != "candidate_n"))
        self.assertEqual(len(objects), 3)
        self.assertEqual(int(objects.nonoverlapping_windows.sum()), 2)
        self.assertEqual(int(excluded.rows.sum()), funnel["candidate_n"] - 2)


if __name__ == "__main__":
    unittest.main()
