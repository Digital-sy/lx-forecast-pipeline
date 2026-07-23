#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""采购建议主入口：颜色名称来自 A2023/B2024 颜色编制表。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from common import get_logger
from common.database import db_cursor
from jobs.feishu import generate_procurement_report_lx_color as base
from jobs.feishu import generate_fabric_forecast_named_colors as named_fabric
from jobs.feishu.color_mapping_catalog import ColorMappingCatalog

logger = get_logger("procurement_report_named_colors")


def _month_label(current_date: datetime, delta: int) -> str:
    return base._month_label(current_date, delta)


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

    feishu_records: List[Dict[str, Any]] = []
    mapped = 0
    pending = 0
    for record in db_rows:
        system = str(record.get("颜色体系") or "").strip()
        code = str(record.get("颜色缩写") or "").strip()
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
