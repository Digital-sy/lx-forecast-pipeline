#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""按业务固定表头导出最终“面料预估表”。

计算完全复用 ``export_fabric_color_order_forecast_final.build_final_rows``，只调整
Excel 展示结构，不改变颜色匹配、A2023/B2024 判定、库存归属和面料用量口径。

主表固定为 21 列：
SKU、面料颜色编号、面料、颜色、领星颜色、库存量/米、待到货量/米、统计类型、
面料编号、颜色缩写、库存量/条、待到货量/条、用量信息缺失SPU、当月完整预估/米、
当月已下单消耗/米、当月剩余预估/米、T+1月预估/米、T+2月预估/米、
运营当月预估/米、运营T+1月预估/米、运营T+2月预估/米。

月份标题随运行月份动态滚动。例如 2026-08 运行时显示 8月、9月、10月。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from openpyxl import Workbook

from common import get_logger
from common.feishu import FeishuClient
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu.fabric_color_stocking_spu import DEFAULT_SPU_MANUAL_MAPPING_PATH

logger = get_logger("export_fabric_color_order_forecast_business")


def _month_number(label: str) -> str:
    """26年8月 -> 8月；无法解析时原样返回。"""
    match = re.search(r"(\d{1,2})月", str(label or ""))
    return f"{int(match.group(1))}月" if match else str(label or "")


def _find_month_labels(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """从最终计算结果字段名提取 T/T+1/T+2/T+3 月份标签。"""
    labels: list[str] = []
    if rows:
        for key in rows[0].keys():
            text = str(key)
            match = re.fullmatch(r"(.+月)完整预估/米", text)
            if match:
                labels.append(match.group(1))
                break
        current = labels[0] if labels else ""
        following: list[str] = []
        for key in rows[0].keys():
            text = str(key)
            match = re.fullmatch(r"(.+月)预估/米", text)
            if not match or text.startswith("运营"):
                continue
            label = match.group(1)
            if label != current and label not in following:
                following.append(label)
        labels.extend(following)
    return labels[:4]


async def _load_lingxing_name_by_record_id() -> dict[str, str]:
    """读取飞书字段“领星新颜色名称”，仅用于展示列“领星颜色”。"""
    client = FeishuClient(
        app_token=os.getenv(
            "FABRIC_COLOR_CATALOG_BASE_TOKEN", stocking.DEFAULT_BASE_TOKEN
        ),
        table_id=os.getenv(
            "FABRIC_COLOR_CATALOG_TABLE_ID", stocking.DEFAULT_CATALOG_TABLE_ID
        ),
        view_id=os.getenv(
            "FABRIC_COLOR_CATALOG_VIEW_ID", stocking.DEFAULT_CATALOG_VIEW_ID
        ) or None,
    )
    field_map = await client.get_table_fields()
    actual_fields = set(field_map.values())
    target_field = "领星新颜色名称"
    if target_field not in actual_fields:
        logger.warning("飞书清单缺少字段“领星新颜色名称”，主表“领星颜色”将为空")
        return {}

    records = await client.read_records(page_size=500)
    result: dict[str, str] = {}
    for record in records:
        record_id = str(record.get("record_id") or "")
        values = stocking._texts((record.get("fields") or {}).get(target_field))
        if record_id and values:
            result[record_id] = values[0]
    return result


def _lingxing_name(row: Mapping[str, Any], by_record_id: Mapping[str, str]) -> str:
    ids = [
        value.strip()
        for value in str(row.get("飞书记录ID") or "").split("、")
        if value.strip()
    ]
    values = [by_record_id[record_id] for record_id in ids if record_id in by_record_id]
    return "、".join(dict.fromkeys(values))


def _business_headers(month_labels: Sequence[str]) -> list[str]:
    m0 = _month_number(month_labels[0]) if len(month_labels) > 0 else "当月"
    m1 = _month_number(month_labels[1]) if len(month_labels) > 1 else "T+1月"
    m2 = _month_number(month_labels[2]) if len(month_labels) > 2 else "T+2月"
    return [
        "SKU",
        "面料颜色编号",
        "面料",
        "颜色",
        "领星颜色",
        "库存量/米",
        "待到货量/米",
        "统计类型",
        "面料编号",
        "颜色缩写",
        "库存量/条",
        "待到货量/条",
        "用量信息缺失SPU",
        f"{m0}完整预估/米",
        f"{m0}已下单消耗/米",
        f"{m0}剩余预估/米",
        f"{m1}预估/米",
        f"{m2}预估/米",
        f"运营{m0}预估/米",
        f"运营{m1}预估/米",
        f"运营{m2}预估/米",
    ]


def _demand_value(row: Mapping[str, Any], label: str, suffix: str) -> Any:
    return row.get(f"{label}{suffix}", 0)


def _to_business_color_row(
    row: Mapping[str, Any],
    labels: Sequence[str],
    lingxing_names: Mapping[str, str],
) -> dict[str, Any]:
    m0 = labels[0]
    m1 = labels[1]
    m2 = labels[2]
    return {
        # 当前最终预计下单表是“面料+飞书颜色”聚合结果，一行会包含多个 SKU；
        # 为保持旧预估表兼容，聚合行 SKU 留空，不拼接超长 SKU 列表。
        "SKU": "",
        "面料颜色编号": row.get("库存匹配键") or "",
        "面料": row.get("面料") or "",
        "颜色": row.get("最终飞书颜色") or "",
        "领星颜色": _lingxing_name(row, lingxing_names),
        "库存量/米": row.get("库存量/米") or 0,
        "待到货量/米": row.get("待到货量/米") or 0,
        "统计类型": "带颜色",
        "面料编号": row.get("面料编号") or "",
        "颜色缩写": row.get("领星新颜色缩写") or "",
        "库存量/条": row.get("库存量/条") or 0,
        "待到货量/条": row.get("待到货量/条") or 0,
        "用量信息缺失SPU": row.get("用量信息缺失SPU") or "",
        f"{_month_number(m0)}完整预估/米": _demand_value(row, m0, "完整预估/米"),
        f"{_month_number(m0)}已下单消耗/米": _demand_value(row, m0, "已下单消耗/米"),
        f"{_month_number(m0)}剩余预估/米": _demand_value(row, m0, "剩余预估/米"),
        f"{_month_number(m1)}预估/米": _demand_value(row, m1, "预估/米"),
        f"{_month_number(m2)}预估/米": _demand_value(row, m2, "预估/米"),
        f"运营{_month_number(m0)}预估/米": _demand_value(row, f"运营{m0}", "预估/米"),
        f"运营{_month_number(m1)}预估/米": _demand_value(row, f"运营{m1}", "预估/米"),
        f"运营{_month_number(m2)}预估/米": _demand_value(row, f"运营{m2}", "预估/米"),
    }


def _to_business_total_row(
    row: Mapping[str, Any],
    labels: Sequence[str],
) -> dict[str, Any]:
    m0 = labels[0]
    m1 = labels[1]
    m2 = labels[2]
    return {
        "SKU": "",
        "面料颜色编号": "",
        "面料": row.get("面料") or "",
        "颜色": "",
        "领星颜色": "",
        "库存量/米": row.get("库存量/米") or 0,
        "待到货量/米": row.get("待到货量/米") or 0,
        "统计类型": "总量",
        "面料编号": row.get("面料编号") or "",
        "颜色缩写": "",
        "库存量/条": row.get("库存量/条") or 0,
        "待到货量/条": row.get("待到货量/条") or 0,
        "用量信息缺失SPU": row.get("用量信息缺失SPU") or "",
        f"{_month_number(m0)}完整预估/米": _demand_value(row, m0, "完整预估/米"),
        f"{_month_number(m0)}已下单消耗/米": _demand_value(row, m0, "已下单消耗/米"),
        f"{_month_number(m0)}剩余预估/米": _demand_value(row, m0, "剩余预估/米"),
        f"{_month_number(m1)}预估/米": _demand_value(row, m1, "预估/米"),
        f"{_month_number(m2)}预估/米": _demand_value(row, m2, "预估/米"),
        f"运营{_month_number(m0)}预估/米": _demand_value(row, f"运营{m0}", "预估/米"),
        f"运营{_month_number(m1)}预估/米": _demand_value(row, f"运营{m1}", "预估/米"),
        f"运营{_month_number(m2)}预估/米": _demand_value(row, f"运营{m2}", "预估/米"),
    }


def export_business_workbook(
    color_rows: Sequence[Mapping[str, Any]],
    total_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any],
    lingxing_names: Mapping[str, str],
    output_dir: Path,
) -> Path:
    labels = _find_month_labels(color_rows or total_rows)
    if len(labels) < 3:
        raise RuntimeError(f"无法识别连续3个月份字段，实际识别到: {labels}")

    headers = _business_headers(labels)
    business_rows: list[dict[str, Any]] = []
    colors_by_fabric: dict[str, list[Mapping[str, Any]]] = {}
    for row in color_rows:
        colors_by_fabric.setdefault(str(row.get("面料") or ""), []).append(row)

    # 每个面料先放总量，再放该面料各颜色，便于业务查看。
    for total in total_rows:
        fabric = str(total.get("面料") or "")
        business_rows.append(_to_business_total_row(total, labels))
        for color in colors_by_fabric.get(fabric, ()):
            business_rows.append(_to_business_color_row(color, labels, lingxing_names))

    workbook = Workbook()
    ws = workbook.active
    ws.title = "面料预估表"
    final_export._write_rows(ws, headers, business_rows)

    # 待确认和核对摘要继续保留，避免格式收口后丢失治理信息。
    pending_ws = workbook.create_sheet("待确认颜色")
    pending_headers = list(pending_rows[0].keys()) if pending_rows else [
        "面料", "颜色体系", "原始颜色编码", "未确认原因"
    ]
    final_export._write_rows(pending_ws, pending_headers, pending_rows, pending=True)

    summary_ws = workbook.create_sheet("核对摘要")
    final_export._style_header(summary_ws, ["指标", "值"])
    summary_metrics = dict(metrics)
    summary_metrics["business_sheet_headers"] = headers
    summary_metrics["business_sheet_row_count"] = len(business_rows)
    summary_metrics["business_sheet_month_policy"] = "主表仅展示当月、T+1、T+2三个月；月份标题随运行月份动态滚动"
    row_index = 2
    for key, value in summary_metrics.items():
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        summary_ws.cell(row_index, 1, key)
        summary_ws.cell(row_index, 2, value)
        for cell in summary_ws[row_index]:
            cell.border = final_export.BORDER
            cell.alignment = final_export.LEFT
        row_index += 1
    summary_ws.freeze_panes = "A2"
    final_export._autosize(summary_ws, maximum=80)

    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"面料预估表_最终版_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    workbook.save(output)
    logger.info(
        "业务表头预计下单表完成：%s；主表 %d 行，颜色 %d 行，总量 %d 行，待确认 %d 行，覆盖率 %.2f%%",
        output,
        len(business_rows),
        len(color_rows),
        len(total_rows),
        len(pending_rows),
        float(metrics.get("confirmed_coverage_pct") or 0),
    )
    return output


async def run(
    output_dir: Path,
    manual_mapping_path: Path,
    spu_manual_mapping_path: Path,
) -> Path:
    color_rows, total_rows, pending_rows, metrics = await final_export.build_final_rows(
        manual_mapping_path=manual_mapping_path,
        spu_manual_mapping_path=spu_manual_mapping_path,
    )
    lingxing_names = await _load_lingxing_name_by_record_id()
    return export_business_workbook(
        color_rows,
        total_rows,
        pending_rows,
        metrics,
        lingxing_names,
        output_dir,
    )


def main() -> Path:
    parser = argparse.ArgumentParser(description="按业务21列表头生成最终面料预估表")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.getenv("PROCUREMENT_EXPORT_DIR", "/opt/apps/pythondata/exports")),
    )
    parser.add_argument(
        "--manual-mapping",
        type=Path,
        default=stocking.DEFAULT_MANUAL_MAPPING_PATH,
        help="历史四字段人工映射 CSV",
    )
    parser.add_argument(
        "--spu-manual-mapping",
        type=Path,
        default=DEFAULT_SPU_MANUAL_MAPPING_PATH,
        help="SPU级人工映射 CSV；键=面料名+原始颜色编码+SPU",
    )
    args = parser.parse_args()
    return asyncio.run(
        run(
            output_dir=args.output_dir,
            manual_mapping_path=args.manual_mapping,
            spu_manual_mapping_path=args.spu_manual_mapping,
        )
    )


if __name__ == "__main__":
    main()
