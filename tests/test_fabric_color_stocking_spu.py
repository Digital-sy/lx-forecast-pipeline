import csv
import tempfile
import unittest
from pathlib import Path

from jobs.feishu.color_mapping_catalog import ColorMappingCatalog
from jobs.feishu.fabric_color_stocking import (
    CatalogIndex,
    CatalogRow,
    FabricUsage,
    ForecastSku,
    ManualMappingCatalog,
)
from jobs.feishu.fabric_color_stocking_spu import (
    MATCH_SPU_MANUAL,
    REASON_SPU_MANUAL_AMBIGUOUS,
    REASON_SPU_MANUAL_TARGET_MISSING,
    SpuManualMapping,
    SpuManualMappingCatalog,
    aggregate_stocking_with_spu,
    load_spu_manual_mapping_catalog,
)


class SpuManualFabricColorTests(unittest.TestCase):
    def setUp(self):
        self.governance = ColorMappingCatalog(
            [
                (1, "DB", "Dark Brown", "深棕色", "", "A2023"),
                (2, "BK", "Black", "黑色", "", "A2023"),
            ]
        )

    @staticmethod
    def forecast(**overrides):
        values = {
            "sku": "BX422-DB-S",
            "spu": "BX422",
            "product_name": "BX422-深蓝-S",
            "color_code": "DB",
            "color_name": "深棕色",
            "color_system": "A2023",
            "forecast_qty": 100,
        }
        values.update(overrides)
        return ForecastSku(**values)

    def test_spu_mapping_ignores_color_system_and_resolves_exact_feishu_color(self):
        forecast = self.forecast(color_system="待定")
        index = CatalogIndex(
            [
                CatalogRow("013仿棉拉架-优化", "41#深蓝", "TB"),
                CatalogRow("013仿棉拉架-优化", "7#深棕", "DB"),
            ]
        )
        catalog = SpuManualMappingCatalog(
            [
                SpuManualMapping(
                    "013仿棉拉架-优化",
                    "DB",
                    "BX422",
                    "41#深蓝",
                )
            ]
        )
        decision = catalog.decision(
            forecast,
            "013仿棉拉架-优化",
            index,
        )
        self.assertIsNotNone(decision)
        self.assertEqual(decision.method, MATCH_SPU_MANUAL)
        self.assertEqual(decision.row.color_name, "41#深蓝")
        self.assertEqual(forecast.color_system, "待定")

    def test_spu_mapping_overrides_existing_deterministic_code_match(self):
        rows = [
            CatalogRow("013仿棉拉架-优化", "41#深蓝", "TB"),
            CatalogRow("013仿棉拉架-优化", "7#深棕", "DB"),
        ]
        spu_catalog = SpuManualMappingCatalog(
            [
                SpuManualMapping(
                    "013仿棉拉架-优化",
                    "DB",
                    "BX422",
                    "41#深蓝",
                )
            ]
        )
        result = aggregate_stocking_with_spu(
            forecasts=[self.forecast()],
            fabric_usage_by_spu={
                "BX422": [
                    FabricUsage(
                        "BX422",
                        "013仿棉拉架-优化",
                        0.5,
                        1.1,
                    )
                ]
            },
            catalog_rows=rows,
            governance_catalog=self.governance,
            manual_catalog=ManualMappingCatalog(),
            spu_manual_catalog=spu_catalog,
            target_fabrics=["013仿棉拉架-优化"],
        )
        self.assertEqual(result.auto_merge_rows[0]["飞书颜色原值"], "41#深蓝")
        self.assertEqual(result.auto_merge_rows[0]["匹配方式"], MATCH_SPU_MANUAL)
        self.assertEqual(
            result.auto_merge_rows[0]["匹配依据"],
            "SPU级人工审核精确键（面料+原始颜色编码+SPU）",
        )
        self.assertEqual(result.metrics["calculable_total_meters"], 55.0)
        self.assertEqual(result.metrics["confirmed_color_meters"], 55.0)
        self.assertEqual(result.metrics["spu_manual_match_meters"], 55.0)

    def test_unmapped_spu_falls_back_to_existing_rules(self):
        rows = [CatalogRow("面料A", "7#深棕", "DB")]
        spu_catalog = SpuManualMappingCatalog(
            [SpuManualMapping("面料A", "DB", "OTHER", "不存在颜色")]
        )
        result = aggregate_stocking_with_spu(
            forecasts=[self.forecast(spu="BX422")],
            fabric_usage_by_spu={
                "BX422": [FabricUsage("BX422", "面料A", 1.0, 1.0)]
            },
            catalog_rows=rows,
            governance_catalog=self.governance,
            manual_catalog=ManualMappingCatalog(),
            spu_manual_catalog=spu_catalog,
            target_fabrics=["面料A"],
        )
        self.assertEqual(result.auto_merge_rows[0]["飞书颜色原值"], "7#深棕")
        self.assertNotEqual(result.auto_merge_rows[0]["匹配方式"], MATCH_SPU_MANUAL)

    def test_missing_spu_manual_target_is_hard_failure_not_fallback(self):
        rows = [CatalogRow("面料A", "7#深棕", "DB")]
        spu_catalog = SpuManualMappingCatalog(
            [SpuManualMapping("面料A", "DB", "BX422", "41#深蓝")]
        )
        result = aggregate_stocking_with_spu(
            forecasts=[self.forecast()],
            fabric_usage_by_spu={
                "BX422": [FabricUsage("BX422", "面料A", 1.0, 1.0)]
            },
            catalog_rows=rows,
            governance_catalog=self.governance,
            manual_catalog=ManualMappingCatalog(),
            spu_manual_catalog=spu_catalog,
            target_fabrics=["面料A"],
        )
        self.assertEqual(len(result.auto_merge_rows), 0)
        self.assertEqual(len(result.manual_review_rows), 1)
        self.assertIn(
            REASON_SPU_MANUAL_TARGET_MISSING,
            result.manual_review_rows[0]["未自动归并原因"],
        )

    def test_conflicting_same_key_is_reported(self):
        catalog = SpuManualMappingCatalog(
            [
                SpuManualMapping("面料A", "DB", "BX422", "41#深蓝"),
                SpuManualMapping("面料A", "DB", "BX422", "42#酸性蓝"),
            ]
        )
        audit = catalog.audit()
        self.assertEqual(audit["mapping_key_count"], 1)
        self.assertEqual(audit["conflicting_mapping_key_count"], 1)
        decision = catalog.decision(
            self.forecast(),
            "面料A",
            CatalogIndex(
                [
                    CatalogRow("面料A", "41#深蓝", "TB"),
                    CatalogRow("面料A", "42#酸性蓝", "AB"),
                ]
            ),
        )
        self.assertEqual(decision.reason_code, REASON_SPU_MANUAL_AMBIGUOUS)

    def test_loader_accepts_current_529_file_schema(self):
        headers = [
            "面料名",
            "原始颜色编码",
            "SPU",
            "SKU品名颜色识别",
            "最终飞书颜色",
            "确认来源",
            "分组主键",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "spu.csv"
            with path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                writer.writerow(
                    {
                        "面料名": "013仿棉拉架-优化",
                        "原始颜色编码": "DB",
                        "SPU": "BX422",
                        "SKU品名颜色识别": "深蓝",
                        "最终飞书颜色": "41#深蓝",
                        "确认来源": "已确认-人工",
                        "分组主键": "013仿棉拉架-优化|DB|深蓝",
                    }
                )
            catalog = load_spu_manual_mapping_catalog(path)
            self.assertEqual(catalog.audit()["read_mapping_count"], 1)
            self.assertEqual(catalog.audit()["mapping_key_count"], 1)
            self.assertEqual(catalog.entries[0].spu, "BX422")
            self.assertEqual(catalog.entries[0].catalog_color_name, "41#深蓝")


if __name__ == "__main__":
    unittest.main()
