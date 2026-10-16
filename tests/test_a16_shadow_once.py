#!/usr/bin/env python3
"""Offline safety/unit checks for manually triggered A16 shadow runner."""
import unittest
from datetime import date
from scripts.run_a16_shadow_once import (
    MAX_PLAN_ROWS, checked_qty, estimate_plan_rows, prepare, shift,
    verify_freeze, SHOP,
)


def row(month, qty, sku="BX999-BK-S", spu="BX999"):
    return {"店铺": SHOP, "SKU": sku, "SPU": spu,
            "统计日期": month + "-01", "销量": qty}


def fixture():
    return (
        [row("2026-06", 20), row("2026-07", 30),
         row("2026-08", 40), row("2026-09", 50)],
        [{"spu": "BX999", "product_category": "Dress",
          "update_time": "2026-09-01", "etl_load_time": "2026-09-02", "product_id": 1}],
        [{"SPU": "BX999", "季节": "春夏"}],
    )


class SafetyTests(unittest.TestCase):
    def test_manifest_source_freeze(self):
        self.assertIn("candidate", verify_freeze())

    def test_plan_blocks_excessive_scan(self):
        plan = {"query_block": {"table": {"rows_examined_per_scan": 999},
                                "other": {"table": {"rows_examined_per_scan": 11}}}}
        self.assertEqual(estimate_plan_rows(plan), 1010)
        self.assertTrue(1010 < MAX_PLAN_ROWS["monthly"])
        self.assertIsNone(estimate_plan_rows({"query_block": {}}))

    def test_month_window_and_hash(self):
        prepared = prepare(*fixture(), issue="2026-10")
        sku, hist, cats, seasons, months, digest = prepared
        self.assertEqual(hist[("JQ-US|BX999", date(2026, 9, 1))], 50)
        self.assertEqual(seasons["BX999"], "春夏")
        self.assertEqual(cats["BX999"], "Dress")
        self.assertEqual(len(digest), 64)
        self.assertNotIn(date(2026, 10, 1), months)

    def test_rejects_missing_recent_source(self):
        sales, cat, season = fixture()
        sales = [x for x in sales if x["统计日期"] != "2026-09-01"]
        with self.assertRaisesRegex(RuntimeError, "最近完整月份"):
            prepare(sales, cat, season, "2026-10")

    def test_rejects_future_month(self):
        sales, cat, season = fixture()
        with self.assertRaisesRegex(RuntimeError, "月份范围违规"):
            prepare(sales + [row("2026-10", 99999)], cat, season, "2026-10")

    def test_rejects_conflicting_mapping(self):
        sales, cat, season = fixture()
        with self.assertRaisesRegex(RuntimeError, "映射多个SPU"):
            prepare(sales + [row("2026-09", 3, spu="BX998")], cat, season, "2026-10")

    def test_zero_valid_none_invalid(self):
        self.assertEqual(checked_qty(0), 0)
        with self.assertRaises(ValueError):
            checked_qty(None)
        with self.assertRaises(ValueError):
            checked_qty(-1)
        with self.assertRaises(ValueError):
            checked_qty("1.1")

    def test_h2_h3_target_months(self):
        self.assertEqual(shift("2026-10", 2), date(2026, 12, 1))
        self.assertEqual(shift("2026-10", 3), date(2027, 1, 1))


if __name__ == "__main__":
    unittest.main()
