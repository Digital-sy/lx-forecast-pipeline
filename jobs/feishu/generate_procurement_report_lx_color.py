#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""颜色体系感知的采购建议与面料预估主入口。"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any, Dict, List, Mapping, Sequence

from common import get_logger
from common.database import db_cursor
from jobs.feishu import generate_procurement_report as base
from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu import procurement_color_logic as logic
from jobs.feishu import generate_fabric_forecast_color_system as fabric_detail

logger = get_logger("procurement_report_lx_color")


def parse_lingxing_color(product_name: str) -> str:
    text = (product_name or "").strip()
    if not text:
        return ""
    match = re.search(r"(\d+\s*#\s*[^#，,;/|]+)", text)
    return re.sub(r"\s+", "", match.group(1).strip()) if match else ""


def read_lingxing_color_map() -> Dict[str, str]:
    color_map: Dict[str, str] = {}
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME='lxpm_product_category_snapshot'
            """)
            if not cursor.fetchone().get("cnt", 0):
                return color_map
            cursor.execute("""
                SELECT sku, product_name
                FROM `lxpm_product_category_snapshot`
                WHERE sku IS NOT NULL AND sku != ''
                  AND product_name IS NOT NULL AND product_name != ''
            """)
            for row in cursor.fetchall():
                sku = str(row.get("sku") or "").strip()
                color = parse_lingxing_color(str(row.get("product_name") or ""))
                if sku and color:
                    color_map[sku] = color
    except Exception as exc:
        logger.warning(f"读取颜色-领星映射失败: {exc}")
    return color_map


def _month_label(current_date: datetime, delta: int) -> str:
    year, month = logic.add_months(current_date.year, current_date.month, delta)
    return f"{month}月"


async def write_order_to_feishu(
    records: Sequence[Mapping[str, Any]],
    month_order: Sequence[str],
    current_date: datetime,
) -> None:
    field_list = [
        {"name": "SPU", "type": "text"},
        {"name": "颜色体系", "type": "text"},
        {"name": "颜色缩写", "type": "text"},
        {"name": "颜色汇总代码", "type": "text"},
        {"name": "店铺", "type": "text"},
        {"name": "工厂", "type": "text"},
        {"name": "面料类型", "type": "text"},
        {"name": "统计月份", "type": "text"},
        {"name": "T月建议下单", "type": "number"},
        {"name": "T月运营预计", "type": "number"},
        {"name": "T+1月建议下单", "type": "number"},
        {"name": "T+1月运营预计", "type": "number"},
        {"name": "T+2月建议下单", "type": "number"},
        {"name": "T+2月运营预计", "type": "number"},
        {"name": "T+3月建议下单", "type": "number"},
        {"name": "T+3月运营预计", "type": "number"},
        {"name": "建议下单合计", "type": "number"},
        {"name": "运营预计合计", "type": "number"},
        {"name": "库存", "type": "number"},
        {"name": "待到货", "type": "number"},
    ]
    client = await base._get_or_create_table(
        base.FEISHU_APP_TOKEN, "建议下单量", field_list, remove_extra=True
    )
    t_keys = ["T月", "T+1月", "T+2月", "T+3月"]
    feishu_records: List[Dict[str, Any]] = []
    for record in records:
        row: Dict[str, Any] = {
            "SPU": record["SPU"],
            "颜色体系": record["颜色体系"],
            "颜色缩写": record["颜色缩写"],
            "颜色汇总代码": record["颜色汇总代码"],
            "店铺": record["店铺"],
            "工厂": record.get("工厂", ""),
            "面料类型": record["面料类型"],
            "统计月份": current_date.strftime("%Y-%m"),
            "建议下单合计": record["建议下单合计"],
            "运营预计合计": record["运营预计合计"],
            "库存": record["库存"],
            "待到货": record["待到货"],
        }
        for index, month in enumerate(month_order):
            key = t_keys[index]
            row[f"{key}建议下单"] = record.get(f"{month}建议下单", 0)
            row[f"{key}运营预计"] = record.get(f"{month}运营预计", 0)
        feishu_records.append(row)

    await client.delete_all_records()
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书建议下单量表写入 {written} 条（含颜色体系）")


async def write_fabric_to_feishu(
    records: Sequence[Mapping[str, Any]], current_date: datetime
) -> None:
    field_list = [
        {"name": "统计月份", "type": "text"},
        {"name": "面料", "type": "text"},
        {"name": "SPU数量", "type": "number"},
        {"name": "建议下单量合计", "type": "number"},
        {"name": "单件用量(米)", "type": "number", "precision": 3},
        {"name": "原始单耗加权均值", "type": "number", "precision": 3},
        {"name": "预计用量(米)", "type": "number", "precision": 2},
        {"name": "计算口径", "type": "text"},
    ]
    client = await base._get_or_create_table(
        base.FEISHU_APP_TOKEN, "面料预计用量", field_list, remove_extra=True
    )
    rows = [{
        "统计月份": current_date.strftime("%Y-%m"),
        "面料": record["面料"],
        "SPU数量": record["SPU数量"],
        "建议下单量合计": record["建议下单量合计"],
        "单件用量(米)": record["单件用量(米)"],
        "原始单耗加权均值": record["原始单耗加权均值"],
        "预计用量(米)": record["预计用量(米)"],
        "计算口径": record["计算口径"],
    } for record in records]
    await client.delete_all_records()
    written = await client.write_records(rows, batch_size=500)
    logger.info(f"✓ 飞书面料预计用量表写入 {written} 条（逐SPU单耗×损耗）")


async def write_fabric_detail_to_feishu(current_date: datetime) -> None:
    m0 = _month_label(current_date, 0)
    m1 = _month_label(current_date, 1)
    m2 = _month_label(current_date, 2)
    field_list = [
        {"name": "统计类型", "type": "text"},
        {"name": "面料", "type": "text"},
        {"name": "面料编号", "type": "text"},
        {"name": "颜色体系", "type": "text"},
        {"name": "颜色缩写", "type": "text"},
        {"name": "颜色汇总代码", "type": "text"},
        {"name": "颜色", "type": "text"},
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
        {"name": f"运营{m0}预估/米", "type": "number", "precision": 2},
        {"name": f"运营{m1}预估/米", "type": "number", "precision": 2},
        {"name": f"运营{m2}预估/米", "type": "number", "precision": 2},
        {"name": "用量信息缺失SPU", "type": "text"},
    ]
    client = await base._get_or_create_table(
        base.FEISHU_APP_TOKEN, "面料预估明细", field_list, remove_extra=True
    )
    lx_color_map = read_lingxing_color_map()

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT 统计类型, 面料, 面料编号, 颜色体系, 颜色缩写, 颜色汇总代码,
                   颜色, 面料颜色编号, 库存归属状态,
                   `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                   `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
                   `T+1月预估/米`, `T+2月预估/米`,
                   `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`,
                   用量信息缺失SPU
            FROM `面料预估表`
            ORDER BY 统计类型, 面料, 颜色体系, 颜色缩写
        """)
        db_rows = cursor.fetchall()

    feishu_records: List[Dict[str, Any]] = []
    matched = 0
    for record in db_rows:
        fcc = str(record.get("面料颜色编号") or "").strip()
        system = str(record.get("颜色体系") or "").strip()
        color_code = str(record.get("颜色缩写") or "").strip()
        fabric_code = str(record.get("面料编号") or "").strip()
        legacy_fcc = f"{fabric_code}-{color_code}" if fabric_code and color_code else fcc
        lx_color = lx_color_map.get(fcc) or lx_color_map.get(legacy_fcc, "")
        if lx_color:
            matched += 1
        feishu_records.append({
            "统计类型": record.get("统计类型") or "",
            "面料": record.get("面料") or "",
            "面料编号": fabric_code,
            "颜色体系": system,
            "颜色缩写": color_code,
            "颜色汇总代码": record.get("颜色汇总代码") or "",
            "颜色": record.get("颜色") or "",
            "面料颜色编号": fcc,
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
            f"运营{m0}预估/米": float(record.get("运营当月预估/米") or 0),
            f"运营{m1}预估/米": float(record.get("运营T+1月预估/米") or 0),
            f"运营{m2}预估/米": float(record.get("运营T+2月预估/米") or 0),
            "用量信息缺失SPU": record.get("用量信息缺失SPU") or "",
        })

    logger.info(f"颜色-领星匹配：{matched}/{len(db_rows)} 条")
    await client.delete_all_records()
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书面料预估明细写入 {written} 条（含颜色体系）")


def main() -> None:
    logger.info("=" * 70)
    logger.info("采购建议报告生成（未来月份 + 颜色体系感知）")
    logger.info("  定制面料覆盖未来3个月 | 现货面料覆盖未来2个月")
    logger.info("=" * 70)
    current_date = datetime.now()
    resolver = ColorSystemResolver.from_database()

    forecast_map, month_order = logic.read_system_forecast(resolver, current_date)
    if not forecast_map:
        raise RuntimeError("当前月起未来4个月没有系统预测数据")
    logger.info(f"系统预测读取完成：{len(forecast_map)} 个颜色身份+店铺，月份：{month_order}")

    inventory_map = logic.read_inventory(resolver)
    factory_map = base.read_last_factory()
    op_forecast_map = base.read_op_forecast_by_month()
    fabric_info = logic.read_fabric_info()

    order_records, fabric_records = logic.build_reports(
        forecast_map=forecast_map,
        month_order=month_order,
        inventory_map=inventory_map,
        fabric_info=fabric_info,
        factory_map=factory_map,
        op_forecast_map=op_forecast_map,
    )
    logger.info(
        f"建议下单计算完成：{len(order_records)} 个颜色身份+店铺，"
        f"{len(fabric_records)} 种定制面料"
    )

    logic.save_order_suggest(order_records, month_order)
    logger.info(f"✓ 写入 {len(order_records)} 条记录到 `{logic.TABLE_ORDER_SUGGEST}`")
    logic.save_fabric_usage(fabric_records)
    logger.info(f"✓ 写入 {len(fabric_records)} 条记录到 `{logic.TABLE_FABRIC_USAGE}`")

    # 先使用刚生成的建议下单量刷新详细面料预估，再统一输出飞书。
    detail_records = fabric_detail.main(resolver)

    logger.info("正在写入飞书多维表...")
    asyncio.run(write_order_to_feishu(order_records, month_order, current_date))
    asyncio.run(write_fabric_to_feishu(fabric_records, current_date))
    asyncio.run(write_fabric_detail_to_feishu(current_date))

    custom_rows = [row for row in order_records if row["面料类型"] == "定制面料"]
    stock_rows = [row for row in order_records if row["面料类型"] == "现货面料"]
    need_order = [row for row in order_records if row["建议下单量"] > 0]
    pending_color = [row for row in order_records if row["颜色体系"] == "待定"]
    logger.info("=" * 70)
    logger.info("完成")
    logger.info(f"  定制面料颜色身份+店铺：{len(custom_rows)}")
    logger.info(f"  现货面料颜色身份+店铺：{len(stock_rows)}")
    logger.info(f"  建议补单：{len(need_order)}")
    logger.info(f"  颜色体系待定：{len(pending_color)}")
    logger.info(f"  面料汇总：{len(fabric_records)} 种")
    logger.info(f"  面料详细记录：{len(detail_records)} 条")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
