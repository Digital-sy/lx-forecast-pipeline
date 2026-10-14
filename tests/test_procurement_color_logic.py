import json
import ast
import unittest
from datetime import datetime
from pathlib import Path

from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu.procurement_color_logic import (
    COVERAGE_MONTHS_CUSTOM,
    COVERAGE_MONTHS_STOCK,
    build_reports,
    future_months,
    largest_remainder_allocate,
)
from jobs.feishu.new_visible_procurement_overlay import apply_new_visible_overlay


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
        a = resolver.resolve("SP1-BK-S")
        b = resolver.resolve("SP2-BK-S")
        self.assertEqual(a.aggregate_code, "A2023:BK")
        self.assertEqual(b.aggregate_code, "B2024:BK")
        self.assertNotEqual(a.aggregate_code, b.aggregate_code)

    def test_empty_sku_does_not_inherit_same_spu_system(self):
        rows = [
            {
                "sku": "SB011-BR-S",
                "spu": "SB011",
                "custom_fields_json": json.dumps([
                    {"name": "颜色体系", "val_text": "A2023"}
                ], ensure_ascii=False),
            },
            {
                "sku": "SB011-BW-S",
                "spu": "SB011",
                "custom_fields_json": json.dumps([
                    {"name": "颜色体系", "val_text": ""}
                ], ensure_ascii=False),
            },
        ]
        identity = ColorSystemResolver(rows).resolve("SB011-BW-S", "SB011")
        self.assertEqual(identity.color_system, "待定")
        self.assertEqual(identity.aggregate_code, "待定:BW")
        self.assertEqual(identity.source, "SKU颜色体系为空或无有效标签")

    def test_missing_size_does_not_inherit_same_spu_same_color(self):
        rows = [{
            "sku": "SP1-BK-S",
            "spu": "SP1",
            "custom_fields_json": json.dumps([
                {"name": "颜色体系", "val_text": "A2023"}
            ], ensure_ascii=False),
        }]
        identity = ColorSystemResolver(rows).resolve("SP1-BK-M", "SP1")
        self.assertEqual(identity.color_system, "待定")
        self.assertEqual(identity.aggregate_code, "待定:BK")

    def test_style_segment_uses_real_color_code_for_exact_sku(self):
        rows = [{
            "sku": "KZ291-SHORT-BK-S",
            "spu": "KZ291",
            "custom_fields_json": json.dumps([
                {"name": "颜色体系", "val_text": "A2023"}
            ], ensure_ascii=False),
        }]
        identity = ColorSystemResolver(rows).resolve("KZ291-SHORT-BK-S")
        self.assertEqual(identity.color_code, "BK")
        self.assertEqual(identity.color_system, "A2023")

    def test_invalid_or_multiple_system_values_are_pending(self):
        rows = [
            {
                "sku": "SP3-WH-S",
                "spu": "SP3",
                "custom_fields_json": json.dumps([
                    {"name": "颜色体系", "val_text": "A2023"},
                    {"name": "颜色体系", "val_text": "B2024"},
                ], ensure_ascii=False),
            },
            {
                "sku": "SP4-WH-S",
                "spu": "SP4",
                "custom_fields_json": json.dumps([
                    {"name": "颜色体系", "val_text": "历史体系"}
                ], ensure_ascii=False),
            },
        ]
        resolver = ColorSystemResolver(rows)
        self.assertEqual(resolver.resolve("SP3-WH-S").color_system, "待定")
        self.assertEqual(resolver.resolve("SP4-WH-S").color_system, "待定")


class ProcurementLogicTests(unittest.TestCase):
    def test_all_monthly_entrypoints_use_three_months(self):
        self.assertEqual(COVERAGE_MONTHS_CUSTOM, 3)
        self.assertEqual(COVERAGE_MONTHS_STOCK, 3)
        # Inspect the legacy entrypoint without importing its database/logger setup.
        path = Path(__file__).resolve().parents[1] / "jobs/feishu/generate_procurement_report.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        constants = {
            target.id: ast.literal_eval(node.value)
            for node in tree.body if isinstance(node, ast.Assign)
            for target in node.targets if isinstance(target, ast.Name)
            and target.id in {"COVERAGE_MONTHS_CUSTOM", "COVERAGE_MONTHS_STOCK"}
        }
        self.assertEqual(constants, {"COVERAGE_MONTHS_CUSTOM": 3, "COVERAGE_MONTHS_STOCK": 3})

    def test_window_includes_current_month_across_year_end(self):
        self.assertEqual(
            future_months(datetime(2026, 11, 14), COVERAGE_MONTHS_STOCK),
            [("2026-11-01", "26年11月"), ("2026-12-01", "26年12月"), ("2027-01-01", "27年1月")],
        )

    def test_stock_and_custom_three_month_net_quantity_conserves_total(self):
        months = ["26年10月", "26年11月", "26年12月", "27年1月"]
        key = ("SP1", "A2023", "BK", "SHOP")
        for fabric_type in ("现货面料", "定制面料"):
            with self.subTest(fabric_type=fabric_type):
                rows, _ = build_reports(
                    forecast_map={key: dict(zip(months, [100, 200, 300, 99999]))},
                    month_order=months,
                    inventory_map={key: {"库存": 120, "待到货": 30}},
                    fabric_info={"SP1": {"fabric_type": fabric_type}},
                    factory_map={}, op_forecast_map={},
                )
                row = rows[0]
                self.assertEqual(row["覆盖月数"], 3)
                self.assertEqual(row["建议下单量"], 450)
                self.assertEqual(row["建议下单合计"], 450)
                self.assertEqual([row[f"{m}建议下单"] for m in months], [75, 150, 225, 0])
                self.assertEqual(sum(row[f"{m}建议下单"] for m in months), 450)
                self.assertEqual(row["库存"], 120)
                self.assertEqual(row["待到货"], 30)

    def test_stock_third_month_alone_is_included(self):
        months = ["26年10月", "26年11月", "26年12月", "27年1月"]
        key = ("SP1", "A2023", "BK", "SHOP")
        rows, _ = build_reports(
            forecast_map={key: dict(zip(months, [0, 0, 101, 999]))},
            month_order=months, inventory_map={},
            fabric_info={"SP1": {"fabric_type": "现货面料"}},
            factory_map={}, op_forecast_map={},
        )
        self.assertEqual(rows[0]["建议下单量"], 101)
        self.assertEqual([rows[0][f"{m}建议下单"] for m in months], [0, 0, 101, 0])

    def test_inventory_covers_three_months_no_negative_order(self):
        months = ["26年10月", "26年11月", "26年12月"]
        key = ("SP1", "A2023", "BK", "SHOP")
        rows, _ = build_reports(
            forecast_map={key: {m: 100 for m in months}}, month_order=months,
            inventory_map={key: {"库存": 250, "待到货": 80}},
            fabric_info={}, factory_map={}, op_forecast_map={},
        )
        self.assertEqual(rows[0]["覆盖月数"], 3)
        self.assertEqual(rows[0]["建议下单量"], 0)
        self.assertEqual(sum(rows[0][f"{m}建议下单"] for m in months), 0)

    def test_new_visible_keeps_approved_total_and_hold(self):
        months = ["26年10月", "26年11月", "26年12月", "27年1月"]
        for fabric_type, quantity, status in (
            ("现货面料", 7, "ORDER_NOW_NET_Q50_GAP"),
            ("定制面料", 0, "HOLD_CUSTOM_H90"),
            ("现货面料", 0, "BLOCK_TEST"),
        ):
            with self.subTest(fabric_type=fabric_type, status=status):
                keys = [("SP1", system, "BK", "SHOP") for system in ("A2023", "B2024")]
                forecast = {key: dict(zip(months, [100, 100, 100, 99999])) for key in keys}
                fabrics = {"SP1": {"fabric_type": fabric_type}}
                rows, _ = build_reports(forecast, months, {}, fabrics, {}, {})
                recs = {("SP1", "SHOP"): {
                    "fabric_type": fabric_type, "recommended_qty_q50": quantity,
                    "recommendation_status": status,
                    "model_version": "NV_PROCUREMENT_CHAMPION_V2_H48_H60_NET_GAP",
                    "qty_basis": "TEST_H60_NET_GAP",
                }}
                result, _, summary = apply_new_visible_overlay(rows, forecast, months, fabrics, recs)
                self.assertEqual(sum(row["建议下单量"] for row in result), quantity)
                self.assertEqual(summary["allocated_q50_sum"], quantity)
                self.assertEqual(summary["expected_q50_sum"], quantity)
                for row in result:
                    self.assertEqual(sum(row[f"{m}建议下单"] for m in months), row["建议下单量"])
                    self.assertEqual(row["27年1月建议下单"], 0)
                    self.assertEqual(row["新品动作状态"], status)
                    self.assertEqual(row["新品数量口径"], "TEST_H60_NET_GAP")
                if quantity:
                    self.assertGreater(sum(row["26年12月建议下单"] for row in result), 0)

    def test_new_visible_fallback_weights_include_third_month(self):
        months = ["26年10月", "26年11月", "26年12月", "27年1月"]
        keys = [("SP1", "A2023", color, "SHOP") for color in ("BK", "WH")]
        forecast = {keys[0]: dict(zip(months, [0, 0, 10, 999])), keys[1]: {m: 0 for m in months}}
        fabrics = {"SP1": {"fabric_type": "现货面料"}}
        inventory = {key: {"库存": 100, "待到货": 0} for key in keys}
        rows, _ = build_reports(forecast, months, inventory, fabrics, {}, {})
        recs = {("SP1", "SHOP"): {"fabric_type": "现货面料", "recommended_qty_q50": 5}}
        result, _, _ = apply_new_visible_overlay(rows, forecast, months, fabrics, recs)
        self.assertEqual([row["建议下单量"] for row in result], [5, 0])
        self.assertEqual(result[0]["26年12月建议下单"], 5)

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
