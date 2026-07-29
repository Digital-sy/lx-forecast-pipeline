import json
import tempfile
import unittest
from pathlib import Path

from openpyxl import load_workbook

from jobs.feishu.color_mapping_catalog import ColorMappingCatalog
from jobs.feishu.fabric_color_stocking import (
    MATCH_CATALOG_LABEL,
    MATCH_CODE,
    MATCH_NAME,
    MATCH_SYSTEM,
    REASON_AMBIGUOUS,
    REASON_PENDING_SYSTEM,
    CatalogIndex,
    CatalogRow,
    ForecastSku,
    aggregate_stocking,
    export_workbook,
    match_catalog_row,
    parse_feishu_catalog_records,
)


def forecast(**overrides):
    values = {
        "sku": "SP1-BK-S",
        "spu": "SP1",
        "product_name": "示例品名-1#黑色",
        "color_code": "BK",
        "color_name": "1#黑色",
        "color_system": "A2023",
        "forecast_qty": 100,
    }
    values.update(overrides)
    return ForecastSku(**values)


class FabricColorMatchingTests(unittest.TestCase):
    def setUp(self):
        self.governance = ColorMappingCatalog(
            [
                (1, "BK", "Black", "黑色", "", "A2023"),
                (2, "BK", "Black Onyx", "黑玛瑙", "", "B2024"),
                (3, "WH", "White", "白色", "", "A2023"),
            ]
        )

    def test_chinese_name_has_priority_over_conflicting_code(self):
        rows = [
            CatalogRow("面料A", "1#黑色", "NEW-BK"),
            CatalogRow("面料A", "别的颜色", "BK"),
        ]
        decision = match_catalog_row(
            forecast(), "面料A", CatalogIndex(rows), self.governance
        )
        self.assertEqual(decision.method, MATCH_NAME)
        self.assertEqual(decision.row.color_name, "1#黑色")

    def test_code_is_second_priority(self):
        rows = [CatalogRow("面料A", "黑色现用名", "BK")]
        decision = match_catalog_row(
            forecast(color_name="旧中文名"),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.method, MATCH_CODE)

    def test_code_precedes_catalog_label_when_both_match(self):
        rows = [
            CatalogRow("面料A", "2#黑色", "OTHER"),
            CatalogRow("面料A", "清单当前名称", "BK"),
        ]
        decision = match_catalog_row(
            forecast(
                color_name="黑色",
                color_code="BK",
                color_system="待定",
            ),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.method, MATCH_CODE)
        self.assertEqual(decision.row.color_name, "清单当前名称")

    def test_catalog_number_prefix_is_explicit_match_method(self):
        rows = [CatalogRow("面料A", "2#黑色", "NEW-BK")]
        decision = match_catalog_row(
            forecast(
                color_name="黑色",
                color_code="OLD",
                color_system="待定",
            ),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.method, MATCH_CATALOG_LABEL)
        self.assertEqual(decision.row.color_name, "2#黑色")

    def test_catalog_dash_prefix_is_explicit_match_method(self):
        rows = [CatalogRow("面料A", "037-拿铁", "")]
        decision = match_catalog_row(
            forecast(
                color_name="拿铁",
                color_code="OLD",
                color_system="待定",
            ),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.method, MATCH_CATALOG_LABEL)

    def test_catalog_parsed_name_does_not_hide_ambiguity(self):
        rows = [
            CatalogRow("面料A", "1#黑色", ""),
            CatalogRow("面料A", "2#黑色", ""),
        ]
        decision = match_catalog_row(
            forecast(
                color_name="黑色",
                color_code="OLD",
                color_system="待定",
            ),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.reason_code, REASON_AMBIGUOUS)

    def test_system_mapping_resolves_code_to_current_chinese(self):
        rows = [CatalogRow("面料A", "黑色", "NEW-BK")]
        decision = match_catalog_row(
            forecast(color_name="旧中文名"),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.method, MATCH_SYSTEM)
        self.assertEqual(decision.row.color_name, "黑色")

    def test_system_and_chinese_name_can_resolve_to_governed_code(self):
        governance = ColorMappingCatalog(
            [(1, "NEW-BK", "Black", "治理黑色", "", "A2023")]
        )
        rows = [CatalogRow("面料A", "当前清单黑色", "NEW-BK")]
        decision = match_catalog_row(
            forecast(color_name="治理黑色", color_code="OLD-BK"),
            "面料A",
            CatalogIndex(rows),
            governance,
        )
        self.assertEqual(decision.method, MATCH_SYSTEM)
        self.assertEqual(decision.row.lingxing_code, "NEW-BK")

    def test_pending_system_stays_unmatched(self):
        rows = [CatalogRow("面料A", "黑色", "NEW-BK")]
        decision = match_catalog_row(
            forecast(color_name="旧中文名", color_code="OLD", color_system="待定"),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.reason_code, REASON_PENDING_SYSTEM)

    def test_no_space_or_case_normalization_is_applied(self):
        rows = [CatalogRow("面料A", "1#黑色", "BK")]
        decision = match_catalog_row(
            forecast(color_name="1# 黑色", color_code="bk", color_system="待定"),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.reason_code, REASON_PENDING_SYSTEM)

    def test_governance_name_lookup_does_not_trim_input(self):
        governance = ColorMappingCatalog(
            [(1, "NEW-BK", "Black", "治理黑色", "", "A2023")]
        )
        rows = [CatalogRow("面料A", "当前清单黑色", "NEW-BK")]
        decision = match_catalog_row(
            forecast(color_name=" 治理黑色 ", color_code="OLD-BK"),
            "面料A",
            CatalogIndex(rows),
            governance,
        )
        self.assertNotEqual(decision.method, MATCH_SYSTEM)

    def test_same_code_pointing_to_two_colors_is_ambiguous(self):
        rows = [
            CatalogRow("面料A", "黑色一", "BK"),
            CatalogRow("面料A", "黑色二", "BK"),
        ]
        decision = match_catalog_row(
            forecast(color_name="", color_system="待定"),
            "面料A",
            CatalogIndex(rows),
            self.governance,
        )
        self.assertEqual(decision.reason_code, REASON_AMBIGUOUS)

    def test_exact_duplicate_catalog_rows_are_consolidated_and_reported(self):
        rows = [
            CatalogRow("面料A", "1#黑色", "BK", ("rec1",)),
            CatalogRow("面料A", "1#黑色", "BK", ("rec2",)),
        ]
        index = CatalogIndex(rows, source_record_count=2)
        decision = match_catalog_row(
            forecast(), "面料A", index, self.governance
        )
        self.assertEqual(decision.method, MATCH_NAME)
        self.assertEqual(
            index.audit()["exact_duplicate_business_group_count"], 1
        )
        self.assertEqual(index.rows[0].raw_row_count, 2)

    def test_aggregation_counts_distinct_skus_and_methods(self):
        rows = [
            CatalogRow("面料A", "1#黑色", "BK"),
            CatalogRow("面料A", "白色现用名", "WH"),
        ]
        forecasts = [
            forecast(forecast_qty=100),
            forecast(
                sku="SP1-WH-M",
                color_code="WH",
                color_name="旧白色",
                forecast_qty=50,
            ),
        ]
        result = aggregate_stocking(
            forecasts,
            {"SP1": ["面料A"]},
            rows,
            self.governance,
        )
        self.assertEqual(len(result.main_rows), 2)
        self.assertEqual(result.metrics["fully_matched_sku_count"], 2)
        self.assertEqual(result.metrics["match_method_counts"][MATCH_NAME], 1)
        self.assertEqual(result.metrics["match_method_counts"][MATCH_CODE], 1)
        self.assertEqual(result.metrics["catalog_fabric_name_count"], 1)
        self.assertEqual(result.metrics["forecast_fabric_name_count"], 1)
        self.assertEqual(
            result.metrics["in_scope_fabric_assignment_match_rate_pct"],
            100.0,
        )

    def test_pending_subset_is_sorted_by_forecast_qty(self):
        rows = [CatalogRow("面料A", "黑色", "NEW")]
        forecasts = [
            forecast(
                sku="SP1-OLD-S",
                color_code="OLD",
                color_name="旧色",
                color_system="待定",
                forecast_qty=20,
            ),
            forecast(
                sku="SP2-OLD-S",
                spu="SP2",
                color_code="OLD",
                color_name="旧色",
                color_system="待定",
                forecast_qty=80,
            ),
        ]
        result = aggregate_stocking(
            forecasts,
            {"SP1": ["面料A"], "SP2": ["面料A"]},
            rows,
            self.governance,
        )
        self.assertEqual(
            [row["SKU"] for row in result.priority_rows],
            ["SP2-OLD-S", "SP1-OLD-S"],
        )
        self.assertEqual(
            result.metrics["pending_system_unmatched_forecast_qty"], 100
        )

    def test_feishu_link_ids_are_resolved_to_fabric_names(self):
        raw = [
            {
                "record_id": "rec-color",
                "fields": {
                    "面料名": [{"record_id": "rec-fabric"}],
                    "颜色": "1#黑色",
                    "领星新颜色缩写": "BK",
                },
            }
        ]
        rows, audit = parse_feishu_catalog_records(
            raw, {"rec-fabric": "面料A"}
        )
        self.assertEqual(rows[0].fabric_name, "面料A")
        self.assertEqual(audit["invalid_record_count"], 0)

    def test_export_contains_required_business_sheets_and_metrics(self):
        result = aggregate_stocking(
            [forecast()],
            {"SP1": ["面料A"]},
            [CatalogRow("面料A", "1#黑色", "BK")],
            self.governance,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path, metrics_path = export_workbook(
                result, Path(temp_dir)
            )
            workbook = load_workbook(workbook_path, read_only=True)
            self.assertEqual(
                workbook.sheetnames,
                ["面料-颜色备货", "未匹配清单", "优先补标清单", "核对摘要"],
            )
            self.assertEqual(
                [cell.value for cell in next(workbook["面料-颜色备货"].rows)],
                [
                    "面料名",
                    "颜色(中文)",
                    "领星新颜色缩写",
                    "预估备货量",
                    "关联SKU数",
                    "匹配方式",
                ],
            )
            workbook.close()
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            self.assertEqual(metrics["fully_matched_sku_count"], 1)


if __name__ == "__main__":
    unittest.main()
