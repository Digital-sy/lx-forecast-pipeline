import json
import unittest
from datetime import datetime

from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu.procurement_color_logic import (
    build_reports,
    future_months,
    largest_remainder_allocate,
)


class ColorResolverTests(unittest.TestCase):
    def test_same_code_in_two_systems_is_not_merged(self):
        rows = [
            {
                "sku": "SP1-BK-S",
                "spu": "SP1",
                "custom_fields_json": json.dumps([
                    {"name": "颜色体系", "val_text": "A2023"}
                ], ensure_ascii=False),
            },
            {
                "sku": "SP2-BK-S",
                "spu": "SP2",
                "custom_fields_json": json.dumps([
                    {"name": "颜色体系", "val_text": "B2024"}
                ], ensure_ascii=False),
            },
        ]
        resolver = ColorSystemResolver(rows)
        a = resolver.resolve("SP1-BK-M")
        b = resolver.resolve("SP2-BK-M")
        self.assertEqual(a.aggregate_code, "A2023:BK")
        self.assertEqual(b.aggregate_code, "B2024:BK")
        self.assertNotEqual(a.aggregate_code, b.aggregate_code)

    def test_style_segment_uses_real_color_code(self):
        rows = [{
            "sku": "KZ291-SHORT-BK-S",
            "spu": "KZ291",
            "custom_fields_json": json.dumps([
                {"name": "颜色体系", "val_text": "A2023"}
            ], ensure_ascii=False),
        }]
        identity = ColorSystemResolver(rows).resolve("KZ291-SHORT-BK-M")
        self.assertEqual(identity.color_code, "BK")
        self.assertEqual(identity.color_system, "A2023")


class ProcurementLogicTests(unittest.TestCase):
    def test_future_months_only(self):
        self.assertEqual(
            future_months(datetime(2026, 7, 22), 4),
            [
                ("2026-07-01", "26年7月"),
                ("2026-08-01", "26年8月"),
                ("2026-09-01", "26年9月"),
                ("2026-10-01", "26年10月"),
            ],
        )

    def test_largest_remainder_preserves_total(self):
        result = largest_remainder_allocate(10, {"a": 1, "b": 1, "c": 1})
        self.assertEqual(sum(result.values()), 10)

    def test_inventory_does_not_cross_shop(self):
        from jobs.feishu.procurement_color_logic import get_inventory

        inventory = {
            ("SP1", "A2023", "BK", "SHOP-A"): {"库存": 100, "待到货": 20}
        }
        self.assertEqual(
            get_inventory(inventory, ("SP1", "A2023", "BK", "SHOP-B")),
            {"库存": 0, "待到货": 0},
        )

    def test_custom_coverage_and_weighted_fabric_usage(self):
        months = ["26年7月", "26年8月", "26年9月", "26年10月"]
        forecast = {
            ("SP1", "A2023", "BK", "SHOP"): {m: 100 for m in months},
            ("SP1", "B2024", "BK", "SHOP"): {m: 100 for m in months},
        }
        fabric_info = {
            "SP1": {
                "fabric_type": "定制面料",
                "fabrics": [("F1", 1.2, 1.1)],
            }
        }
        order_records, fabric_records = build_reports(
            forecast_map=forecast,
            month_order=months,
            inventory_map={},
            fabric_info=fabric_info,
            factory_map={},
            op_forecast_map={},
        )
        self.assertEqual(len(order_records), 2)
        for row in order_records:
            self.assertEqual(row["建议下单量"], 300)
            self.assertEqual(row["26年7月建议下单"], 100)
            self.assertEqual(row["26年8月建议下单"], 100)
            self.assertEqual(row["26年9月建议下单"], 100)
            self.assertEqual(row["26年10月建议下单"], 0)
        self.assertEqual(fabric_records[0]["建议下单量合计"], 600)
        self.assertAlmostEqual(fabric_records[0]["预计用量(米)"], 792.0)
        self.assertAlmostEqual(fabric_records[0]["单件用量(米)"], 1.32)


if __name__ == "__main__":
    unittest.main()
