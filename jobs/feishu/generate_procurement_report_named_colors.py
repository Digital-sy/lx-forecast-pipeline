#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""采购建议主入口：颜色名称来自 A2023/B2024 颜色编制表。

颜色-领星优先展示已核验代码/唯一名称匹配；最终空值使用 B2024
中文候选原文展示兜底。该兜底不是已验证领星匹配，不生成数字编号。
"""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Dict, List

from common import get_logger
from common.database import db_cursor
from jobs.feishu import generate_procurement_report_lx_color as base
from jobs.feishu import generate_fabric_forecast_named_colors as named_fabric
from jobs.feishu.color_mapping_catalog import ColorMappingCatalog
from jobs.feishu.fabric_lingxing_color_reference import build_aliases, resolve_reference
from jobs.feishu.fabric_lingxing_name_reference import build_name_index, resolve_name_reference

logger = get_logger("procurement_report_named_colors")


def _month_label(current_date: datetime, delta: int) -> str:
    return base._month_label(current_date, delta)


async def _read_lingxing_reference_aliases():
    # Reuse the approved manual inventory source only for its old/new color relationship.
    from common.feishu import FeishuClient
    from jobs.feishu.fetch_fabric_inventory_from_feishu import FEISHU_APP_TOKEN, FEISHU_TABLE_ID

    client = FeishuClient(app_token=FEISHU_APP_TOKEN, table_id=FEISHU_TABLE_ID)
    rows = await client.read_records(page_size=500)
    if not rows:
        raise RuntimeError("颜色参考台账为空，停止写表，保留现有结果")
    aliases, issues = build_aliases(rows)
    if not aliases:
        raise RuntimeError("颜色参考台账无有效映射，停止写表，保留现有结果")
    logger.info("领星颜色参考：唯一体系别名 %s 个，台账待核查项 %s 个", len(aliases), len(issues))
    return aliases


def read_lingxing_name_rows(fabric_codes):
    """Read complete bounded fabric prefixes for name uniqueness; never write."""
    if not fabric_codes:
        return []
    codes = list(fabric_codes)
    if any(not isinstance(code, str) or not re.fullmatch(r"FAB-[A-Z0-9-]+", code) for code in codes):
        raise ValueError("面料名称匹配只允许合法完整FAB编号")
    codes = sorted(set(codes))
    rows = []
    with db_cursor() as cursor:
        for offset in range(0, len(codes), 20):
            chunk = codes[offset:offset + 20]
            sql = (
                "SELECT sku, product_name, synced_at "
                "FROM lingxing.lxpm_product_category_snapshot WHERE ("
                + " OR ".join(["sku LIKE %s"] * len(chunk))
                + ") ORDER BY sku LIMIT 5001"
            )
            cursor.execute(sql, [code + "-%" for code in chunk])
            batch = cursor.fetchall()
            if len(batch) > 5000 or len(rows) + len(batch) > 5000:
                raise RuntimeError("面料名称来源超过5000行预算，停止写表")
            rows.extend(batch)
    if not rows:
        raise RuntimeError("面料名称来源为空，停止写表，保留现有结果")
    return rows


async def write_fabric_detail_to_feishu(current_date: datetime) -> None:
    m0 = _month_label(current_date, 0)
    m1 = _month_label(current_date, 1)
    m2 = _month_label(current_date, 2)
    m3 = _month_label(current_date, 3)
    field_list = [
        {"name": "统计类型", "type": "text"},
        {"name": "面料", "type": "text"},
        {"name": "面料编号", "type": "text"},
        {"name": "颜色体系", "type": "text"},
        {"name": "颜色缩写", "type": "text"},
        {"name": "中文颜色名称", "type": "text"},
        {"name": "颜色名称+体系", "type": "text"},
        {"name": "A2023中文候选", "type": "text"},
        {"name": "B2024中文候选", "type": "text"},
        {"name": "颜色映射状态", "type": "text"},
        {"name": "颜色汇总代码", "type": "text"},
        {"name": "面料颜色编号", "type": "text"},
        {"name": "颜色-领星", "type": "text"},
        {"name": "库存归属状态", "type": "text"},
        {"name": "库存量/条", "type": "number", "precision": 2},
        {"name": "库存量/米", "type": "number", "precision": 2},
        {"name": "待到货量/条", "type": "number", "precision": 2},
        {"name": "待到货量/米", "type": "number", "precision": 2},
        {"name": f"{m0}已下单消耗/米", "type": "number", "precision": 2},
        {"name": f"{m0}完整预估/米", "type": "number", "precision": 2},
        {"name": f"{m0}剩余预估/米", "type": "number", "precision": 2},
        {"name": f"{m1}预估/米", "type": "number", "precision": 2},
        {"name": f"{m2}预估/米", "type": "number", "precision": 2},
        {"name": f"{m3}预估/米", "type": "number", "precision": 2},
        {"name": f"运营{m0}预估/米", "type": "number", "precision": 2},
        {"name": f"运营{m1}预估/米", "type": "number", "precision": 2},
        {"name": f"运营{m2}预估/米", "type": "number", "precision": 2},
        {"name": f"运营{m3}预估/米", "type": "number", "precision": 2},
        {"name": "用量信息缺失SPU", "type": "text"},
    ]
    client = await base.base._get_or_create_table(
        base.base.FEISHU_APP_TOKEN, "面料预估明细", field_list, remove_extra=True
    )
    catalog = ColorMappingCatalog.from_runtime()

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT 统计类型, 面料, 面料编号, 颜色体系, 颜色缩写, 颜色汇总代码,
                   颜色, 面料颜色编号, 库存归属状态,
                   `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                   `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
                   `T+1月预估/米`, `T+2月预估/米`, `T+3月预估/米`,
                   `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`,
                   `运营T+3月预估/米`, 用量信息缺失SPU
            FROM `面料预估表`
            ORDER BY 统计类型, 面料, 颜色体系, 颜色缩写
        """)
        db_rows = cursor.fetchall()

    # Same physical fabric row supplies the A2023 old code and B2024 current SKU.
    # Never guess cross-system identity by equal abbreviations.
    lx_aliases = await _read_lingxing_reference_aliases()
    lx_keys = sorted({item["sku"] for item in lx_aliases.values()})
    lx_color_map = base.read_lingxing_color_map(lx_keys)
    if lx_keys and not lx_color_map:
        raise RuntimeError("领星颜色来源未取得有效值，停止写表，保留现有结果")

    fabric_pairs = sorted({
        (str(record.get("面料编号") or "").strip(), str(record.get("面料") or "").strip())
        for record in db_rows
        if record.get("统计类型") == "带颜色"
        and re.fullmatch(r"FAB-[A-Z0-9-]+", str(record.get("面料编号") or "").strip())
        and str(record.get("面料") or "").strip()
    })
    name_rows = read_lingxing_name_rows(sorted({code for code, _ in fabric_pairs}))
    name_color_map = {}
    for name_row in name_rows:
        name_sku = str(name_row.get("sku") or "").strip()
        parsed_color = base.parse_lingxing_color(str(name_row.get("product_name") or ""))
        if name_sku and parsed_color:
            name_color_map[name_sku] = parsed_color
    name_index = build_name_index(name_rows, fabric_pairs, name_color_map)

    feishu_records: List[Dict[str, Any]] = []
    mapped = 0
    pending = 0
    name_matched = 0
    code_matched = 0
    b2024_display_fallback = 0
    for record in db_rows:
        system = str(record.get("颜色体系") or "").strip()
        code = str(record.get("颜色缩写") or "").strip()
        lx_reference = resolve_reference(record, lx_aliases, lx_color_map)
        lx_color = lx_reference["value"]
        if lx_color:
            code_matched += 1
        if record.get("统计类型") == "带颜色":
            info = catalog.describe(system, code)
            if info["颜色映射状态"] == "颜色编制表主映射":
                mapped += 1
            if system == "待定":
                pending += 1
        else:
            info = {
                "中文颜色名称": "",
                "颜色显示名称": "",
                "A2023中文候选": "",
                "B2024中文候选": "",
                "颜色映射状态": "总量行",
            }
        if lx_reference["status"] in {"UNMAPPED_ALIAS", "UNKNOWN_COLOR_SYSTEM"}:
            name_record = dict(record)
            for name_field in ("中文颜色名称", "A2023中文候选", "B2024中文候选"):
                name_record[name_field] = info[name_field]
            name_reference = resolve_name_reference(name_record, name_index)
            if name_reference["status"] == "NAME_MATCHED":
                lx_color = name_reference["value"]
                name_matched += 1
        # Display-only fallback: do not treat the candidate name as verified
        # Lingxing identity, infer a number, or change the record color system.
        b2024_candidate = info.get("B2024中文候选")
        if not lx_color and isinstance(b2024_candidate, str) and b2024_candidate.strip():
            lx_color = b2024_candidate
            b2024_display_fallback += 1
        feishu_records.append({
            "统计类型": record.get("统计类型") or "",
            "面料": record.get("面料") or "",
            "面料编号": record.get("面料编号") or "",
            "颜色体系": system,
            "颜色缩写": code,
            "中文颜色名称": info["中文颜色名称"],
            "颜色名称+体系": info["颜色显示名称"],
            "A2023中文候选": info["A2023中文候选"],
            "B2024中文候选": info["B2024中文候选"],
            "颜色映射状态": info["颜色映射状态"],
            "颜色汇总代码": record.get("颜色汇总代码") or "",
            "面料颜色编号": record.get("面料颜色编号") or "",
            "颜色-领星": lx_color,
            "库存归属状态": record.get("库存归属状态") or "",
            "库存量/条": float(record.get("库存量/条") or 0),
            "库存量/米": float(record.get("库存量/米") or 0),
            "待到货量/条": float(record.get("待到货量/条") or 0),
            "待到货量/米": float(record.get("待到货量/米") or 0),
            f"{m0}已下单消耗/米": float(record.get("当月已下单消耗/米") or 0),
            f"{m0}完整预估/米": float(record.get("当月完整预估/米") or 0),
            f"{m0}剩余预估/米": float(record.get("当月剩余预估/米") or 0),
            f"{m1}预估/米": float(record.get("T+1月预估/米") or 0),
            f"{m2}预估/米": float(record.get("T+2月预估/米") or 0),
            f"{m3}预估/米": float(record.get("T+3月预估/米") or 0),
            f"运营{m0}预估/米": float(record.get("运营当月预估/米") or 0),
            f"运营{m1}预估/米": float(record.get("运营T+1月预估/米") or 0),
            f"运营{m2}预估/米": float(record.get("运营T+2月预估/米") or 0),
            f"运营{m3}预估/米": float(record.get("运营T+3月预估/米") or 0),
            "用量信息缺失SPU": record.get("用量信息缺失SPU") or "",
        })

    logger.info(f"颜色编制表中文名称匹配：{mapped}/{len(db_rows)} 条；体系待定 {pending} 条")
    logger.info("领星颜色参考已核验匹配：代码 %s 条，唯一名称 %s 条", code_matched, name_matched)
    logger.info("B2024中文候选展示兜底：%s 条；非已验证领星匹配，不生成编号", b2024_display_fallback)
    await client.delete_all_records()
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书面料预估明细写入 {written} 条（中文颜色名称+颜色体系）")


def main() -> None:
    original_detail_main = base.fabric_detail.main
    original_writer = base.write_fabric_detail_to_feishu
    base.fabric_detail.main = named_fabric.main
    base.write_fabric_detail_to_feishu = write_fabric_detail_to_feishu
    try:
        base.main()
    finally:
        base.fabric_detail.main = original_detail_main
        base.write_fabric_detail_to_feishu = original_writer


if __name__ == "__main__":
    main()
