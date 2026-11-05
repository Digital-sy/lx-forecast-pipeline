"""AST-only writer regression. Synthetic records; no production imports or I/O clients."""
from __future__ import annotations

import ast
import asyncio
import builtins
import copy
import hashlib
import re
import unittest
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
NAMED = ROOT / "jobs/feishu/generate_procurement_report_named_colors.py"
HELPER = ROOT / "jobs/feishu/fabric_lingxing_color_reference.py"
BEFORE = Path(__file__).parent / "fixtures/named_color_reference_before.py.txt"
NUMERIC = ["库存量/条", "库存量/米", "待到货量/条", "待到货量/米",
           "当月已下单消耗/米", "当月完整预估/米", "当月剩余预估/米",
           "T+1月预估/米", "T+2月预估/米", "T+3月预估/米",
           "运营当月预估/米", "运营T+1月预估/米", "运营T+2月预估/米", "运营T+3月预估/米"]
FABRIC = "TEST-FAB-017"


def pure_helper():
    tree = ast.parse(HELPER.read_text(encoding="utf-8-sig"))
    for node in tree.body:
        if isinstance(node, ast.Import):
            assert all(x.name in {"re"} for x in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.module in {"__future__", "collections", "typing"}
        else:
            assert isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Expr))
            if isinstance(node, ast.Expr):
                assert isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    env = {"__name__": "offline_pure_alias_helper"}
    exec(compile(tree, str(HELPER), "exec"), env)
    return env


def load_function_definitions(path, env):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    funcs = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.fix_missing_locations(ast.Module(body=[future] + funcs, type_ignores=[]))
    exec(compile(module, str(path), "exec"), env)
    return env


def inventory_rows():
    return [
        {"record_id": "TEST_INV_1", "fields": {"颜色缩写": "HP", "领星新颜色缩写": "BL",
            "面料编号-颜色缩写": FABRIC+"-BL", "库存量/条": 0}},
        {"record_id": "TEST_INV_2", "fields": {"颜色缩写": "ZX", "领星新颜色缩写": "HP",
            "面料编号-颜色缩写": FABRIC+"-HP", "库存量/条": 0}},
        {"record_id": "TEST_INV_3", "fields": {"颜色缩写": "CF", "领星新颜色缩写": "C1",
            "面料编号-颜色缩写": FABRIC+"-C1"}},
        {"record_id": "TEST_INV_4", "fields": {"颜色缩写": "CF", "领星新颜色缩写": "C2",
            "面料编号-颜色缩写": FABRIC+"-C2"}},
    ]


def business_rows():
    specs = [("带颜色", "A2023", "HP"), ("带颜色", "B2024", "HP"),
             ("带颜色", "待定", "HP"), ("总量", "A2023", "HP"),
             ("带颜色", "A2023", "CF"), ("带颜色", "A2023", "QQ"),
             ("带颜色", "A2023", "HP")]
    rows = []
    for i, (kind, system, code) in enumerate(specs):
        fabric = FABRIC if i != 6 else "017"
        row = {"统计类型": kind, "面料": "合成测试面料", "面料编号": fabric,
               "颜色体系": system, "颜色缩写": code, "颜色汇总代码": system+":"+code,
               "面料颜色编号": fabric+"-"+system+"-"+code, "颜色": "仅合成旧显示",
               "库存归属状态": "SYNTHETIC", "用量信息缺失SPU": "TEST_ONLY"}
        for j, name in enumerate(NUMERIC):
            row[name] = [Decimal("1.25"), 0, None, Decimal("999.125")][(i+j) % 4]
        rows.append(row)
    return rows


def colors():
    return {FABRIC+"-BL": "101#测试蓝", FABRIC+"-HP": "202#测试粉",
            FABRIC+"-C1": "303#测试红", FABRIC+"-C2": "404#测试绿",
            FABRIC+"-A2023-CF": "999#错误回退诱饵", "017-HP": "888#截短面料诱饵"}


class Harness:
    """Explicit Feishu/DB substitutes; production function definitions are executed."""
    def __init__(self, path=NAMED, rows=None, inventory=None, color_map=None,
                 alias_error=None, color_error=None, db_error=None):
        self.path = path
        self.rows = copy.deepcopy(business_rows() if rows is None else rows)
        self.inventory = copy.deepcopy(inventory_rows() if inventory is None else inventory)
        self.color_map = dict(colors() if color_map is None else color_map)
        self.events = []
        self.requested = []
        self.imports = []
        self.sql = []
        self.fields = None
        self.output = None
        self.alias_error = alias_error
        self.color_error = color_error
        self.db_error = db_error
        self.deleted = 0
        self.writes = 0
        self.table_args = None
        owner = self

        class InventoryDouble:
            def __init__(self, app_token, table_id):
                assert app_token == "[TEST_ONLY_APP]" and table_id == "[TEST_ONLY_TABLE]"
            async def read_records(self, page_size):
                assert page_size == 500
                owner.events.append("alias_read")
                if owner.alias_error:
                    raise owner.alias_error
                return copy.deepcopy(owner.inventory)

        def isolated_import(name, globals=None, locals=None, fromlist=(), level=0):
            owner.imports.append(name)
            if name == "__future__":
                return builtins.__import__(name, globals, locals, fromlist, level)
            if name == "common.feishu":
                return SimpleNamespace(FeishuClient=InventoryDouble)
            if name == "jobs.feishu.fetch_fabric_inventory_from_feishu":
                return SimpleNamespace(FEISHU_APP_TOKEN="[TEST_ONLY_APP]",
                                       FEISHU_TABLE_ID="[TEST_ONLY_TABLE]")
            raise AssertionError("Production import blocked: "+name)

        async def ensure(app_token, table_name, field_list, remove_extra):
            assert app_token == "[TEST_ONLY_APP]"
            self.table_args = (table_name, remove_extra)
            self.fields = copy.deepcopy(field_list)
            self.events.append("ensure_table")
            return self

        def read_colors(keys):
            self.events.append("color_read")
            self.requested.append(copy.deepcopy(keys))
            if self.color_error:
                raise self.color_error
            return {key: self.color_map[key] for key in keys if key in self.color_map}

        def label(date, delta):
            m = date.year*12+date.month-1+delta
            return f"{m//12}年{m%12+1}月"

        def describe(system, code):
            return {"中文颜色名称": "标准-"+code,
                    "颜色显示名称": "标准-"+system+"-"+code,
                    "A2023中文候选": "A-"+code, "B2024中文候选": "B-"+code,
                    "颜色映射状态": "颜色编制表主映射" if system != "待定" else "体系待定"}
        helper = pure_helper()
        self.base = SimpleNamespace(
            base=SimpleNamespace(_get_or_create_table=ensure, FEISHU_APP_TOKEN="[TEST_ONLY_APP]"),
            _month_label=label, read_lingxing_color_map=read_colors)
        self.env = dict(base=self.base, db_cursor=self.cursor, re=re,
                        build_name_index=lambda *args: {},
                        resolve_name_reference=lambda *args: {"status":"NAME_UNMAPPED","value":""},
                        ColorMappingCatalog=SimpleNamespace(from_runtime=lambda:SimpleNamespace(describe=describe)),
                        logger=SimpleNamespace(info=lambda *a, **k:None, warning=lambda *a, **k:None),
                        build_aliases=helper["build_aliases"], resolve_reference=helper["resolve_reference"],
                        named_fabric=SimpleNamespace(main=lambda:None),
                        __builtins__={**vars(builtins), "__import__": isolated_import})
        load_function_definitions(path, self.env)

    @contextmanager
    def cursor(self):
        self.events.append("db_read")
        yield self

    def execute(self, sql):
        self.sql.append(sql)
        assert "FROM" in sql and "面料预估表" in sql
        if self.db_error:
            raise self.db_error

    def fetchall(self):
        return copy.deepcopy(self.rows)

    async def delete_all_records(self):
        self.events.append("delete")
        self.deleted += 1

    async def write_records(self, rows, batch_size):
        assert batch_size == 500
        self.events.append("write")
        self.output = copy.deepcopy(rows)
        self.writes += 1
        return len(rows)

    def run(self, date=datetime(2026, 12, 5)):
        asyncio.run(self.env["write_fabric_detail_to_feishu"](date))
        return self


class NamedReferenceWriterTests(unittest.TestCase):
    def test_all_nonreference_values_schema_sql_and_order_unchanged(self):
        old = Harness(BEFORE).run()
        new = Harness().run()
        strip = lambda xs:[{k:v for k,v in row.items() if k != "颜色-领星"} for row in xs]
        self.assertEqual(strip(new.output), strip(old.output))
        self.assertEqual(new.fields, old.fields)
        self.assertEqual(new.table_args, old.table_args)
        self.assertEqual(new.sql, old.sql)
        self.assertEqual(len(new.output), len(old.output))
        self.assertEqual((new.deleted, new.writes), (1, 1))

    def test_cross_system_alias_uses_physical_source_not_legacy_fallback(self):
        old = Harness(BEFORE).run()
        new = Harness().run()
        self.assertEqual(old.output[0]["颜色-领星"], "202#测试粉")
        self.assertEqual(new.output[0]["颜色-领星"], "101#测试蓝")
        self.assertEqual(new.output[1]["颜色-领星"], "202#测试粉")

    def test_total_conflict_unknown_unmapped_and_wrong_fabric_remain_blank(self):
        result = Harness().run()
        for i in [2, 3, 4, 5, 6]:
            self.assertEqual(result.output[i]["颜色-领星"], "", i)
        self.assertEqual(result.output[3]["颜色映射状态"], "总量行")

    def test_zero_inventory_does_not_remove_reference_or_business_rows(self):
        result = Harness().run()
        self.assertEqual(result.output[0]["颜色-领星"], "101#测试蓝")
        self.assertEqual(len(result.output), len(business_rows()))

    def test_month_fields_and_numeric_aggregates_equal_across_year(self):
        old = Harness(BEFORE).run()
        new = Harness().run()
        for name in [f["name"] for f in new.fields if f["type"] == "number"]:
            self.assertEqual([x[name] for x in new.output], [x[name] for x in old.output])
            self.assertEqual(sum(x[name] for x in new.output), sum(x[name] for x in old.output))
        names={f["name"] for f in new.fields}
        self.assertIn("2026年12月完整预估/米", names)
        self.assertIn("2027年1月预估/米", names)
        self.assertIn("2027年3月预估/米", names)

    def test_requested_reference_keys_are_unique_physical_source_skus_only(self):
        h=Harness().run()
        expected=sorted({row["fields"]["面料编号-颜色缩写"] for row in inventory_rows()})
        self.assertEqual(h.requested, [expected])
        self.assertNotIn(FABRIC+"-A2023-CF", h.requested[0])

    def test_alias_read_error_never_deletes_or_writes(self):
        h=Harness(alias_error=RuntimeError("SYNTHETIC_ALIAS_FAILURE"))
        with self.assertRaisesRegex(RuntimeError, "SYNTHETIC_ALIAS_FAILURE"): h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))
        self.assertNotIn("color_read",h.events)

    def test_empty_alias_inventory_never_deletes_or_writes(self):
        h=Harness(inventory=[])
        with self.assertRaises(RuntimeError): h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))

    def test_all_conflicted_aliases_never_deletes_or_writes(self):
        first=inventory_rows()[0]
        second=copy.deepcopy(first);second["record_id"]="TEST_INV_DUPLICATE"
        h=Harness(inventory=[first,second])
        with self.assertRaises(RuntimeError): h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))

    def test_color_source_total_failure_never_deletes_or_writes(self):
        h=Harness(color_map={})
        with self.assertRaises(RuntimeError): h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))

    def test_partial_reference_missing_blanks_only_that_column(self):
        available=colors();del available[FABRIC+"-BL"]
        result=Harness(color_map=available).run()
        self.assertEqual(result.output[0]["颜色-领星"],"")
        self.assertEqual(result.output[1]["颜色-领星"],"202#测试粉")
        self.assertEqual(len(result.output),len(business_rows()))

    def test_color_source_exception_never_deletes_or_writes(self):
        h=Harness(color_error=RuntimeError("SYNTHETIC_COLOR_FAILURE"))
        with self.assertRaisesRegex(RuntimeError,"SYNTHETIC_COLOR_FAILURE"): h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))

    def test_business_read_failure_never_deletes_or_writes(self):
        h=Harness(db_error=RuntimeError("SYNTHETIC_DB_FAILURE"))
        with self.assertRaisesRegex(RuntimeError,"SYNTHETIC_DB_FAILURE"): h.run()
        self.assertEqual((h.deleted,h.writes),(0,0))

    def test_actual_loader_and_reads_precede_delete(self):
        h=Harness().run()
        self.assertLess(h.events.index("alias_read"),h.events.index("delete"))
        self.assertLess(h.events.index("color_read"),h.events.index("delete"))
        self.assertLess(h.events.index("db_read"),h.events.index("delete"))
        self.assertEqual(h.events[-2:],["delete","write"])

    def test_ast_loading_does_not_import_or_execute_production_modules(self):
        h=Harness()
        self.assertEqual([x for x in h.imports if x!="__future__"],[])
        self.assertEqual(h.events,[])
        h.run()
        self.assertEqual([x for x in h.imports if x!="__future__"],
            ["common.feishu","jobs.feishu.fetch_fabric_inventory_from_feishu"])
        # These imports are served only by isolated_import, never Python's importer.

    def test_main_dispatches_named_writer_and_restores_originals(self):
        h=Harness()
        old_writer=object();old_detail=object();named_detail=object()
        h.base.fabric_detail=SimpleNamespace(main=old_detail)
        h.base.write_fabric_detail_to_feishu=old_writer
        h.env["named_fabric"]=SimpleNamespace(main=named_detail)
        seen=[]
        def main():
            seen.append((h.base.fabric_detail.main,h.base.write_fabric_detail_to_feishu))
            asyncio.run(h.base.write_fabric_detail_to_feishu(datetime(2026,12,5)))
        h.base.main=main
        h.env["main"]()
        self.assertEqual(seen,[(named_detail,h.env["write_fabric_detail_to_feishu"])])
        self.assertIs(h.base.fabric_detail.main,old_detail)
        self.assertIs(h.base.write_fabric_detail_to_feishu,old_writer)
        self.assertEqual(h.writes,1)

    def test_main_restores_bindings_after_failure(self):
        h=Harness()
        old_writer=object();old_detail=object()
        h.base.fabric_detail=SimpleNamespace(main=old_detail)
        h.base.write_fabric_detail_to_feishu=old_writer
        h.base.main=lambda:(_ for _ in ()).throw(RuntimeError("SYNTHETIC_MAIN_FAILURE"))
        with self.assertRaisesRegex(RuntimeError,"SYNTHETIC_MAIN_FAILURE"):h.env["main"]()
        self.assertIs(h.base.fabric_detail.main,old_detail)
        self.assertIs(h.base.write_fabric_detail_to_feishu,old_writer)


if __name__ == "__main__":
    unittest.main(verbosity=2)
