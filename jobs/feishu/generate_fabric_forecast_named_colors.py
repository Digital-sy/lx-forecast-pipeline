#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""用颜色编制表中文名称增强颜色体系面料预估。

此模块复用 ``generate_fabric_forecast_color_system`` 的全部计算，只替换颜色展示：
``颜色`` 字段写成“中文颜色名称｜颜色体系”，不再读取领星产品名称颜色。
"""
from __future__ import annotations

from typing import Any, List

from common import get_logger
from jobs.feishu import generate_fabric_forecast_color_system as base
from jobs.feishu.color_mapping_catalog import ColorMappingCatalog
from jobs.feishu.color_system_resolver import ColorSystemResolver

logger = get_logger("fabric_forecast_named_colors")

# 在任何包装层临时改写 base.main 之前，保留原始函数引用。
# 否则 generate_procurement_report_named_colors 将 base.main 指向本模块 main 后，
# 这里再次调用 base.main 会无限递归。
_ORIGINAL_MAIN = base.main
_ORIGINAL_GENERATE_RECORDS = base.generate_records


def generate_records(*args: Any, **kwargs: Any) -> List[dict[str, Any]]:
    records = _ORIGINAL_GENERATE_RECORDS(*args, **kwargs)
    catalog = ColorMappingCatalog.from_runtime()
    matched = 0
    unresolved = 0
    for record in records:
        if record.get("统计类型") != "带颜色":
            record["颜色"] = ""
            continue
        system = str(record.get("颜色体系") or "待定")
        code = str(record.get("颜色缩写") or "")
        info = catalog.describe(system, code)
        record["颜色"] = info["颜色显示名称"]
        if info["颜色映射状态"] == "颜色编制表主映射":
            matched += 1
        if system == "待定":
            unresolved += 1
    logger.info(
        f"颜色编制表名称写入：主映射 {matched} 条，体系待定 {unresolved} 条，"
        f"未读取领星颜色名称"
    )
    return records


def main(resolver: ColorSystemResolver | None = None) -> List[dict[str, Any]]:
    original_generate = base.generate_records
    original_color_map = base.base.get_color_map
    base.generate_records = generate_records
    base.base.get_color_map = lambda: {}
    try:
        # 必须调用模块加载时保存的原始 main，不能调用可能已被包装层替换的 base.main。
        return _ORIGINAL_MAIN(resolver)
    finally:
        base.generate_records = original_generate
        base.base.get_color_map = original_color_map


if __name__ == "__main__":
    main()
