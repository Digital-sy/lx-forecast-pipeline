import importlib.util
import pathlib
import unittest

p = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "audit_a16_shadow_cohort_history.py"
spec = importlib.util.spec_from_file_location("cohort_history", p)
a = importlib.util.module_from_spec(spec)
spec.loader.exec_module(a)

def fixture():
    cohort = {"P": "背心/吊带", "Q": "连衣裙"}
    pred = {"H2": {"P": {"v4": 500, "a16": 250}, "Q": {"v4": 100, "a16": 30}},
            "H3": {"P": {"v4": 600, "a16": 280}, "Q": {"v4": 80, "a16": 25}}}
    source = [
        {"SPU": "P", "month_key": m, "total_units": q, "null_rows": 0, "negative_rows": 0}
        for m, q in [("2025-12", 200), ("2026-01", 180),
                     ("2026-07", 100), ("2026-08", 120), ("2026-09", 150)]
    ]
    source += [{"SPU": "Q", "month_key": "2026-09",
                "total_units": 90, "null_rows": 0, "negative_rows": 0}]
    return cohort, pred, source

class TestCohortHistory(unittest.TestCase):
    def test_missing_is_not_zero(self):
        cohort, pred, source = fixture()
        sales, first = a.monthly_data(source, cohort)
        status, qty = a.actual_state("Q", "2025-12", sales, first)
        self.assertEqual(status, "NO_PRIOR_POSITIVE_OBSERVED")
        self.assertIsNone(qty)
        detail, summary, cats = a.analyze(cohort, pred, sales, first)
        self.assertEqual(len(detail), 4)
        self.assertEqual(summary[0]["year_ago_observed_n"], 1)
        self.assertEqual(summary[0]["v4_future_units_same_year_ago_observed_spus"], 500)
        self.assertEqual(summary[0]["year_ago_observed_units"], 200)
        self.assertEqual(summary[0]["v4_to_year_ago_ratio_paired"], 2.5)
        self.assertEqual(summary[0]["recent_jul_sep_all3_observed_n"], 1)

    def test_unknown_after_first_sale(self):
        cohort, pred, source = fixture()
        source += [{"SPU": "Q", "month_key": "2025-10", "total_units": 5,
                    "null_rows": 0, "negative_rows": 0}]
        sales, first = a.monthly_data(source, cohort)
        self.assertEqual(a.actual_state("Q", "2025-12", sales, first),
                         ("MONTHLY_ABSENT_UNKNOWN", None))

    def test_explicit_zero_is_known(self):
        cohort, pred, source = fixture()
        source += [{"SPU": "Q", "month_key": "2025-12", "total_units": 0,
                    "null_rows": 0, "negative_rows": 0}]
        sales, first = a.monthly_data(source, cohort)
        self.assertEqual(a.actual_state("Q", "2025-12", sales, first),
                         ("MONTHLY_OBSERVED", 0))

    def test_reject_bad_source_and_duplicates(self):
        cohort, pred, source = fixture()
        extra = {"SPU": "Q", "month_key": "2025-10",
                 "total_units": 10, "null_rows": 1, "negative_rows": 0}
        with self.assertRaisesRegex(ValueError, "NULL"):
            a.monthly_data(source + [extra], cohort)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            a.monthly_data(source + source[:1], cohort)

    def test_explain_estimates(self):
        plan = {"query_block": {"nested_loop": [
            {"table": {"rows_examined_per_scan": 300}},
            {"table": {"rows_examined_per_scan": 500}}]}}
        self.assertEqual(a.scan_estimate(plan), 800)
        self.assertIsNone(a.scan_estimate({}))

if __name__ == "__main__":
    unittest.main()
