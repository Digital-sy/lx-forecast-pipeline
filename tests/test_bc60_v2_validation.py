"""Independent oracle edge cases; these do not import experiment arithmetic."""
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.bc60_v2_validate import CalendarTruth, information_dates, target_dates, independent_metric, independent_native_mean, validate


class IndependentCalendarTests(unittest.TestCase):
    def daily(self):
        dates = pd.date_range("2025-12-01", "2026-04-01")
        return pd.DataFrame([
            {"shop": shop, "spu": spu, "ds": date, "y": float(i + multiplier), "sales_state": "OBSERVED"}
            for shop, spu, multiplier in [("A", "p", 1), ("A", "q", 10), ("B", "p", 100)]
            for i, date in enumerate(dates)
        ])

    def test_two_day_lag_and_weekly_boundaries_across_year(self):
        self.assertEqual(information_dates("2025-12-31", "2026-01-01", "R28-D"), (dt.date(2026, 1, 1), dt.date(2025, 12, 30)))
        self.assertEqual(information_dates("2025-12-31", "2026-01-06", "R28-W"), (dt.date(2025, 12, 31), dt.date(2025, 12, 29)))
        self.assertEqual(information_dates("2025-12-31", "2026-01-07", "R28-W"), (dt.date(2026, 1, 7), dt.date(2026, 1, 5)))
        self.assertEqual(information_dates("2025-12-31", "2026-01-20", "F28"), (dt.date(2025, 12, 31), dt.date(2025, 12, 29)))

    def test_bridge_days_excluded_and_remaining_dates_future_only(self):
        self.assertEqual(target_dates("2026-04-02", "2026-04-02", "T0", "d01_30"), (dt.date(2026, 4, 3), dt.date(2026, 5, 2)))
        self.assertEqual(target_dates("2026-04-02", "2026-04-02", "T0", "d31_60"), (dt.date(2026, 5, 3), dt.date(2026, 6, 1)))
        self.assertEqual(target_dates("2026-04-02", "2026-05-31", "T1", "remaining"), (dt.date(2026, 6, 1), dt.date(2026, 6, 1)))
        with self.assertRaises(ValueError):
            target_dates("2026-04-02", "2026-06-01", "T1", "remaining")

    def test_fixed_length_cross_month_and_leap_year(self):
        first, last = target_dates("2024-01-31", "2024-02-28", "T2", "d01_60")
        self.assertEqual(first, dt.date(2024, 2, 29))
        self.assertEqual((last - first).days + 1, 60)

    def test_exact_mean_and_shop_spu_isolation(self):
        truth = CalendarTruth(self.daily())
        a = truth.mean28("A", "p", "2025-12-28")[0]
        self.assertEqual(a, sum(range(1, 29)) / 28)
        self.assertEqual(truth.mean28("A", "q", "2025-12-28")[0] - a, 9)
        self.assertEqual(truth.mean28("B", "p", "2025-12-28")[0] - a, 99)

    def test_missing_calendar_day_and_null_never_imputed(self):
        daily = self.daily()
        missing = daily[~((daily.shop == "A") & (daily.spu == "p") & (daily.ds == pd.Timestamp("2025-12-15")))]
        self.assertTrue(np.isnan(CalendarTruth(missing).mean28("A", "p", "2025-12-28")[0]))
        daily.loc[(daily.shop == "A") & (daily.spu == "p") & (daily.ds == pd.Timestamp("2025-12-15")), "y"] = np.nan
        self.assertTrue(np.isnan(CalendarTruth(daily).mean28("A", "p", "2025-12-28")[0]))

    def test_explicit_zero_valid_unknown_state_invalid(self):
        daily = self.daily()
        take = (daily.shop == "A") & (daily.spu == "p") & (daily.ds == pd.Timestamp("2025-12-15"))
        daily.loc[take, ["y", "sales_state"]] = [0, "EXPLICIT_ZERO"]
        self.assertEqual(CalendarTruth(daily).mean28("A", "p", "2025-12-28")[0], (sum(range(1, 29)) - 15) / 28)
        daily.loc[take, "sales_state"] = "UNKNOWN"
        self.assertTrue(np.isnan(CalendarTruth(daily).mean28("A", "p", "2025-12-28")[0]))

    def test_future_perturbation_cannot_change_asof_prediction(self):
        daily = self.daily()
        original = CalendarTruth(daily).mean28("A", "p", "2026-01-03")
        daily.loc[daily.ds > pd.Timestamp("2026-01-03"), "y"] = 1e12
        changed = CalendarTruth(daily).mean28("A", "p", "2026-01-03")
        self.assertEqual(original, changed)
        self.assertNotEqual(CalendarTruth(daily).total("A", "p", "2026-01-06", "2026-02-04")[0], CalendarTruth(self.daily()).total("A", "p", "2026-01-06", "2026-02-04")[0])

    def test_initial_rules_have_identical_input(self):
        cutoffs = [information_dates("2026-01-05", "2026-01-05", model)[1] for model in ["F28", "R28-D", "R28-W"]]
        self.assertEqual(len(set(cutoffs)), 1)

    def test_input_hash_preserves_state_identity_and_ignores_later_rows(self):
        daily = self.daily()
        truth = CalendarTruth(daily)
        original = truth.history_hash("A", "p", "2025-12-28", "a" * 64)
        daily.loc[daily.ds > pd.Timestamp("2025-12-28"), "y"] = 1e12
        self.assertEqual(original, CalendarTruth(daily).history_hash("A", "p", "2025-12-28", "a" * 64))
        self.assertNotEqual(original, truth.history_hash("B", "p", "2025-12-28", "a" * 64))
        self.assertNotEqual(original, truth.history_hash("A", "p", "2025-12-28", "b" * 64))
        daily.loc[(daily.shop == "A") & (daily.spu == "p") & (daily.ds == pd.Timestamp("2025-12-15")), "sales_state"] = "UNKNOWN"
        self.assertNotEqual(original, CalendarTruth(daily).history_hash("A", "p", "2025-12-28", "a" * 64))

    def test_not_matured_not_truncated(self):
        truth = CalendarTruth(self.daily())
        self.assertEqual(truth.total("A", "p", "2026-04-01", "2026-04-02")[1], "NOT_MATURED")
        self.assertTrue(np.isnan(truth.total("A", "p", "2026-04-01", "2026-04-02")[0]))
        self.assertEqual(truth.total("A", "p", "2026-04-01", "2026-04-01")[1], "OBSERVED")

    def test_duplicate_dates_refused(self):
        daily = self.daily()
        with self.assertRaises(ValueError):
            CalendarTruth(pd.concat([daily, daily.iloc[[0]]]))

    def test_native_float32_mean_rounds_each_addition(self):
        values = [16777216., 1., 1., 1., 1., 1., 1.]
        native = independent_native_mean(values, np.float32)
        self.assertEqual(float(native), 2396745.25)
        self.assertNotEqual(native, np.float32(np.mean(values, dtype=np.float64)))

    def test_negative_or_infinite_day_invalidates_input(self):
        daily = self.daily()
        for value in [-1.0, np.inf]:
            daily.loc[(daily.shop == "A") & (daily.spu == "p") & (daily.ds == pd.Timestamp("2025-12-15")), "y"] = value
            self.assertTrue(np.isnan(CalendarTruth(daily).mean28("A", "p", "2025-12-28")[0]))

    def test_independent_metric_missing_zero_and_exact_hit_boundary(self):
        frame = pd.DataFrame({"actual": [100., 100., 100., 0., np.nan], "prediction": [120., 79., np.nan, 5., 8.]})
        metric = independent_metric(frame)
        self.assertEqual(metric["fixed_core_n"], 5)
        self.assertEqual(metric["scored_n"], 3)
        self.assertEqual(metric["hit_n"], 1)
        self.assertEqual(metric["miss_n"], 1)
        self.assertEqual(metric["unjudged_n"], 3)
        self.assertAlmostEqual(metric["hit_rate"], 1 / 3)
        self.assertAlmostEqual(metric["hit_sales_share"], 1 / 3)
        self.assertAlmostEqual(metric["wape"], 46 / 200)
        self.assertAlmostEqual(metric["bias"], 4 / 200)
        self.assertEqual(metric["zero_actual_absolute_error"], 5)

    def test_existing_validation_destination_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "validation"
            output.mkdir()
            with self.assertRaises(FileExistsError):
                validate(root, root, output)

    def test_failed_verification_keeps_failure_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(FileNotFoundError):
                validate(root, root)
            report = json.loads((root / "validation/validation.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["exception_type"], "FileNotFoundError")
            with self.assertRaises(FileExistsError):
                validate(root, root)


if __name__ == "__main__":
    unittest.main()
