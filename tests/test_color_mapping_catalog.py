import unittest

from jobs.feishu.color_mapping_catalog import ColorMappingCatalog


ROWS = [
    (1, "BK", "Black", "黑色", "19-0303", "A2023"),
    (2, "BK", "Jet Black", "黑玛瑙", "19-0303", "B2024"),
    (3, "WH", "White", "白色", "11-0601", "A2023"),
    (4, "WH", "White", "白色", "11-0601", "B2024"),
]


class ColorMappingCatalogTests(unittest.TestCase):
    def test_resolved_display_uses_chinese_and_system(self):
        catalog = ColorMappingCatalog(ROWS)
        info = catalog.describe("A2023", "BK")
        self.assertEqual(info["中文颜色名称"], "黑色")
        self.assertEqual(info["颜色显示名称"], "黑色｜A2023")

    def test_unresolved_keeps_both_candidates(self):
        catalog = ColorMappingCatalog(ROWS)
        info = catalog.describe("待定", "BK")
        self.assertEqual(info["A2023中文候选"], "黑色")
        self.assertEqual(info["B2024中文候选"], "黑玛瑙")
        self.assertIn("A2023:黑色", info["颜色显示名称"])
        self.assertIn("B2024:黑玛瑙", info["颜色显示名称"])

    def test_unresolved_same_name_is_not_forced_to_a_system(self):
        catalog = ColorMappingCatalog(ROWS)
        info = catalog.describe("待定", "WH")
        self.assertEqual(info["中文颜色名称"], "白色")
        self.assertIn("待定", info["颜色显示名称"])


if __name__ == "__main__":
    unittest.main()
