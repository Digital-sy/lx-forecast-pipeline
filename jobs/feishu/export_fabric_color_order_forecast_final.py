#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""生成最终版“面料-颜色预计下单表”。

最终颜色口径统一使用飞书当前有效颜色：

1. SPU 级人工映射（面料名+原始颜色编码+SPU）优先；
2. 未命中时复用 ``fabric_color_stocking`` 的确定性规则；
3. 再未命中时复用历史四字段人工映射；
4. 模糊候选绝不进入最终预计下单表，只进入“待确认颜色”；
5. A2023/B2024 仅来自 SKU 自身显式主数据，任何飞书颜色或人工映射都不得反推颜色体系；
6. 颜色库存按“面料编号+领星新颜色缩写”精确匹配，同一飞书颜色只扣一次库存。

需求口径沿用现有生产面料预估：建议下单量按 SKU 预测颜色/尺码比例分摊，
米数=数量×单件用量×单件损耗；单耗缺失仍沿用现有生产表的平均单耗兜底并显式标记。
本模块只生成 Excel，不回写 MySQL/飞书。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from common import get_logger
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu import generate_fabric_forecast as fabric_base
from jobs.feishu import generate_fabric_forecast_color_system as color_system
from jobs.feishu.color_system_resolver import ColorSystemResolver, normalize_sku
from jobs.feishu.fabric_color_stocking_spu import (
    DEFAULT_SPU_MANUAL_MAPPING_PATH,
    MATCH_SPU_MANUAL,
    SpuManualMappingCatalog,
    load_spu_manual_mapping_catalog,
)

logger = get_logger("export_fabric_color_order_forecast_final")

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(name="Microsoft YaHei", bold=True, color="FFFFFF")
PENDING_FILL = PatternFill("solid", fgColor="FFF2CC")
THIN = Side(style="thin", color="D9E1F2")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
RIGHT = Alignment(horizontal="right", vertical="center")
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _meters(value: float) -> float:
    return round(float(value or 0), 2)


def _quantity(value: float) -> int | float:
    value = round(float(value or 0), 4)
    return int(value) if value.is_integer() else value


def _empty_bucket() -> dict[str, Any]:
    return {
        "purchase_m": 0.0,
        "sys_month_m": [0.0, 0.0, 0.0, 0.0],
        "op_month_m": [0.0, 0.0, 0.0, 0.0],
        "缺失SPU": set(),
        "methods": set(),
        "systems": set(),
        "codes": set(),
        "spus": set(),
        "skus": set(),
    }


def _month_delta(stat_date: Any, current_date: datetime) -> int | None:
    try:
        if isinstance(stat_date, str):
            value = datetime.strptime(stat_date[:10], "%Y-%m-%d")
        else:
            value = stat_date
        for delta in range(4):
            year, month = color_system.add_months(
                current_date.year, current_date.month, delta
            )
            if value.year == year and value.month == month:
                return delta
    except Exception:
        pass
    return None


def _fallback_forecast(
    sku: str,
    forecast_by_sku: Mapping[str, stocking.ForecastSku],
    resolver: ColorSystemResolver,
    snapshot_by_sku: Mapping[str, Mapping[str, Any]],
    governance_catalog: stocking.ColorMappingCatalog,
) -> stocking.ForecastSku:
    normalized = normalize_sku(sku)
    if normalized in forecast_by_sku:
        return forecast_by_sku[normalized]

    identity = resolver.resolve(normalized)
    snapshot = snapshot_by_sku.get(normalized, {})
    product_name = str(snapshot.get("product_name") or "")
    color_name = stocking.parse_lingxing_color(product_name)
    if not color_name and identity.color_system in stocking.SUPPORTED_SYSTEMS:
        entry = governance_catalog.lookup(identity.color_system, identity.color_code)
        color_name = entry.chinese if entry else ""

    return stocking.ForecastSku(
        sku=normalized,
        spu=str(snapshot.get("spu") or identity.spu or "").strip(),
        product_name=product_name,
        color_code=str(identity.color_code or "").strip(),
        color_name=str(color_name or "").strip(),
        color_system=str(identity.color_system or stocking.UNKNOWN_SYSTEM).strip()
        or stocking.UNKNOWN_SYSTEM,
        forecast_qty=0,
    )


def _resolve_final_color(
    forecast: stocking.ForecastSku,
    fabric_name: str,
    index: stocking.CatalogIndex,
    governance_catalog: stocking.ColorMappingCatalog,
    manual_catalog: stocking.ManualMappingCatalog,
    spu_catalog: SpuManualMappingCatalog,
) -> stocking.MatchDecision:
    """SPU 人工规则是优先级 0；无规则时完全复用既有确定性匹配。"""
    spu_decision = spu_catalog.decision(forecast, fabric_name, index)
    if spu_decision is not None:
        return spu_decision
    return stocking.match_catalog_row(
        forecast,
        fabric_name,
        index,
        governance_catalog,
        manual_catalog=manual_catalog,
    )


def _style_header(ws: Any, headers: Sequence[str]) -> None:
    for column, header in enumerate(headers, 1):
        cell = ws.cell(1, column, header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER
        cell.border = BORDER


def _autosize(ws: Any, minimum: int = 10, maximum: int = 45) -> None:
    for cells in ws.columns:
        length = max(len(str(cell.value or "")) for cell in cells)
        ws.column_dimensions[get_column_letter(cells[0].column)].width = min(
            max(length + 2, minimum), maximum
        )


def _write_rows(
    ws: Any,
    headers: Sequence[str],
    rows: Sequence[Mapping[str, Any]],
    pending: bool = False,
) -> None:
    _style_header(ws, headers)
    for row_index, row in enumerate(rows, 2):
        for column, header in enumerate(headers, 1):
            value = row.get(header, "")
            cell = ws.cell(row_index, column, value)
            cell.border = BORDER
            cell.alignment = RIGHT if isinstance(value, (int, float)) else LEFT
            if isinstance(value, (int, float)):
                cell.number_format = "#,##0.00"
            if pending:
                cell.fill = PENDING_FILL
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    _autosize(ws)


async def build_final_rows(
    manual_mapping_path: Path,
    spu_manual_mapping_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    current_date = datetime.now()
    target_fabrics = tuple(stocking.TARGET_FABRICS)
    target_set = set(target_fabrics)

    governance_catalog = stocking.ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, catalog_source_audit = await stocking.load_catalog_from_feishu(
        base_token=os.getenv(
            "FABRIC_COLOR_CATALOG_BASE_TOKEN", stocking.DEFAULT_BASE_TOKEN
        ),
        table_id=os.getenv(
            "FABRIC_COLOR_CATALOG_TABLE_ID", stocking.DEFAULT_CATALOG_TABLE_ID
        ),
        view_id=os.getenv(
            "FABRIC_COLOR_CATALOG_VIEW_ID", stocking.DEFAULT_CATALOG_VIEW_ID
        ),
    )
    target_catalog = [row for row in catalog_rows if row.fabric_name in target_set]
    index = stocking.CatalogIndex(
        target_catalog,
        source_record_count=catalog_source_audit.get("source_record_count"),
    )

    manual_catalog = stocking.load_manual_mapping_catalog(manual_mapping_path)
    spu_catalog = load_spu_manual_mapping_catalog(spu_manual_mapping_path)

    # 与经过验证的 dry-run 使用同一 ForecastSku 构造逻辑。
    forecasts, forecast_audit = stocking.load_forecast_skus(governance_catalog)
    forecast_by_sku = {row.sku: row for row in forecasts}
    snapshot_rows = stocking._load_snapshot_rows()
    snapshot_by_sku, _ = stocking._snapshot_index(snapshot_rows)
    resolver = ColorSystemResolver(snapshot_rows)

    fabric_params = fabric_base.get_fabric_params()
    fabric_usage = fabric_base.get_fabric_price_data()
    primary_fabric_by_spu = fabric_base.get_primary_fabric_by_spu(fabric_usage)
    purchase_order_data = fabric_base.get_purchase_order_data()
    system_forecast_data = fabric_base.get_system_forecast_data()
    suggest_data = color_system.get_suggest_order_data_color(
        resolver, current_date=current_date
    )
    operation_forecast_data = fabric_base.get_forecast_order_data()
    effective_qty, _ = color_system._effective_sku_quantities(
        resolver,
        system_forecast_data,
        suggest_data,
        current_date,
    )

    legacy_merge_map = fabric_base.get_fabric_color_merge_mapping()
    inventory_data, pending_data = fabric_base.get_inventory_data(legacy_merge_map)
    inv_by_fabric, pend_by_fabric = fabric_base.get_inventory_by_fabric(
        inventory_data,
        pending_data,
        fabric_params,
    )

    usage_by_spu: MutableMapping[str, list[tuple[str, Mapping[str, Any]]]] = defaultdict(list)
    for (spu, fabric_name), usage_data in fabric_usage.items():
        if fabric_name in target_set:
            usage_by_spu[str(spu).strip()].append((fabric_name, usage_data))

    total_agg: MutableMapping[str, dict[str, Any]] = defaultdict(_empty_bucket)
    color_agg: MutableMapping[tuple[str, str, str], dict[str, Any]] = defaultdict(_empty_bucket)
    pending_agg: MutableMapping[tuple[str, str, str, str], dict[str, Any]] = defaultdict(_empty_bucket)

    match_method_counts: MutableMapping[str, int] = defaultdict(int)
    unmatched_reason_counts: MutableMapping[str, int] = defaultdict(int)

    def add_usage(sku: str, qty: int, target: str, delta: int | None = None) -> None:
        if qty <= 0:
            return
        forecast = _fallback_forecast(
            sku,
            forecast_by_sku,
            resolver,
            snapshot_by_sku,
            governance_catalog,
        )
        spu = str(forecast.spu or "").strip()
        if not spu:
            return

        for fabric_name, usage_data in usage_by_spu.get(spu, ()):
            meters, missing = fabric_base._calc_usage_meters(
                qty,
                usage_data.get("单件用量"),
                usage_data.get("单件损耗"),
                fabric_name,
                spu,
                fabric_usage,
            )
            if meters <= 0:
                continue

            total_bucket = total_agg[fabric_name]
            if target == "purchase":
                total_bucket["purchase_m"] += meters
            else:
                total_bucket[target][delta] += meters
            if missing:
                total_bucket["缺失SPU"].add(spu)

            # 与现有面料预估一致：只有主面料拆具体颜色，其他面料只计总量。
            if primary_fabric_by_spu.get(spu) != fabric_name:
                continue

            decision = _resolve_final_color(
                forecast,
                fabric_name,
                index,
                governance_catalog,
                manual_catalog,
                spu_catalog,
            )
            if decision.row:
                row = decision.row
                bucket = color_agg[row.identity]
                bucket["methods"].add(decision.method)
                bucket["systems"].add(forecast.color_system)
                bucket["codes"].add(forecast.color_code)
                bucket["spus"].add(spu)
                bucket["skus"].add(forecast.sku)
                if target == "purchase":
                    bucket["purchase_m"] += meters
                else:
                    bucket[target][delta] += meters
                if missing:
                    bucket["缺失SPU"].add(spu)
                match_method_counts[decision.method] += 1
                continue

            reason = str(decision.reason or decision.reason_code or "待确认")
            key = (
                fabric_name,
                forecast.color_system,
                forecast.color_code,
                reason,
            )
            bucket = pending_agg[key]
            bucket["systems"].add(forecast.color_system)
            bucket["codes"].add(forecast.color_code)
            bucket["spus"].add(spu)
            bucket["skus"].add(forecast.sku)
            if target == "purchase":
                bucket["purchase_m"] += meters
            else:
                bucket[target][delta] += meters
            if missing:
                bucket["缺失SPU"].add(spu)
            unmatched_reason_counts[reason] += 1

    for sku, qty in purchase_order_data.items():
        add_usage(sku, int(qty), "purchase")

    for (sku, delta), qty in effective_qty.items():
        add_usage(sku, int(qty), "sys_month_m", delta)

    for (sku, stat_date), qty in operation_forecast_data.items():
        delta = _month_delta(stat_date, current_date)
        if delta is not None:
            add_usage(sku, int(qty), "op_month_m", delta)

    month_labels = [label for _, label in color_system.future_months(current_date, 4)]

    def demand_fields(bucket: Mapping[str, Any]) -> dict[str, Any]:
        purchase_m = _meters(bucket["purchase_m"])
        system = [_meters(value) for value in bucket["sys_month_m"]]
        operation = [_meters(value) for value in bucket["op_month_m"]]
        return {
            f"{month_labels[0]}已下单消耗/米": purchase_m,
            f"{month_labels[0]}完整预估/米": system[0],
            f"{month_labels[0]}剩余预估/米": _meters(max(0.0, system[0] - purchase_m)),
            f"{month_labels[1]}预估/米": system[1],
            f"{month_labels[2]}预估/米": system[2],
            f"{month_labels[3]}预估/米": system[3],
            f"运营{month_labels[0]}预估/米": operation[0],
            f"运营{month_labels[1]}预估/米": operation[1],
            f"运营{month_labels[2]}预估/米": operation[2],
            f"运营{month_labels[3]}预估/米": operation[3],
            "未来4月系统预估合计/米": _meters(sum(system)),
            "用量信息缺失SPU": "、".join(sorted(bucket["缺失SPU"])),
        }

    color_rows: list[dict[str, Any]] = []
    for identity, bucket in color_agg.items():
        fabric_name, color_name, lx_code = identity
        catalog_row = next(row for row in index.rows if row.identity == identity)
        params = fabric_params.get(fabric_name, {})
        fabric_code = str(params.get("面料编号") or "").strip().upper()
        meters_per_roll = float(params.get("米数每条") or 0)
        inventory_key = f"{fabric_code}-{lx_code}" if fabric_code and lx_code else ""
        inventory_rolls = int(inventory_data.get(inventory_key, 0) or 0) if inventory_key else 0
        pending_rolls = int(pending_data.get(inventory_key, 0) or 0) if inventory_key else 0
        inventory_status = (
            "飞书颜色库存精确匹配"
            if inventory_key
            else "飞书颜色缺少领星新颜色缩写，未分配颜色库存"
        )
        color_rows.append({
            "面料": fabric_name,
            "面料编号": fabric_code,
            "最终飞书颜色": color_name,
            "领星新颜色缩写": lx_code,
            "飞书记录ID": "、".join(catalog_row.record_ids),
            "原颜色体系": "、".join(sorted(x for x in bucket["systems"] if x)),
            "原颜色编码": "、".join(sorted(x for x in bucket["codes"] if x)),
            "匹配方式": "、".join(
                method for method in (MATCH_SPU_MANUAL, *stocking.MATCH_METHOD_ORDER)
                if method in bucket["methods"]
            ),
            "关联SPU数": len(bucket["spus"]),
            "关联SKU数": len(bucket["skus"]),
            "库存匹配键": inventory_key,
            "库存归属状态": inventory_status,
            "库存量/条": inventory_rolls,
            "库存量/米": _meters(inventory_rolls * meters_per_roll),
            "待到货量/条": pending_rolls,
            "待到货量/米": _meters(pending_rolls * meters_per_roll),
            **demand_fields(bucket),
        })

    color_rows.sort(key=lambda row: (
        stocking.TARGET_FABRIC_ORDER.get(str(row["面料"]), 999),
        str(row["最终飞书颜色"]),
    ))

    total_rows: list[dict[str, Any]] = []
    for fabric_name in target_fabrics:
        if fabric_name not in total_agg:
            continue
        bucket = total_agg[fabric_name]
        params = fabric_params.get(fabric_name, {})
        meters_per_roll = float(params.get("米数每条") or 0)
        inventory_rolls = int(inv_by_fabric.get(fabric_name, 0) or 0)
        pending_rolls = int(pend_by_fabric.get(fabric_name, 0) or 0)
        total_rows.append({
            "面料": fabric_name,
            "面料编号": str(params.get("面料编号") or ""),
            "库存量/条": inventory_rolls,
            "库存量/米": _meters(inventory_rolls * meters_per_roll),
            "待到货量/条": pending_rolls,
            "待到货量/米": _meters(pending_rolls * meters_per_roll),
            **demand_fields(bucket),
        })

    pending_rows: list[dict[str, Any]] = []
    for (fabric_name, system, code, reason), bucket in pending_agg.items():
        pending_rows.append({
            "面料": fabric_name,
            "颜色体系": system,
            "原始颜色编码": code,
            "未确认原因": reason,
            "关联SPU数": len(bucket["spus"]),
            "关联SKU数": len(bucket["skus"]),
            "涉及SPU": "、".join(sorted(bucket["spus"])),
            **demand_fields(bucket),
        })
    pending_rows.sort(key=lambda row: (
        stocking.TARGET_FABRIC_ORDER.get(str(row["面料"]), 999),
        str(row["颜色体系"]),
        str(row["原始颜色编码"]),
    ))

    confirmed_system_meters = sum(
        sum(float(v) for v in bucket["sys_month_m"])
        for bucket in color_agg.values()
    )
    pending_system_meters = sum(
        sum(float(v) for v in bucket["sys_month_m"])
        for bucket in pending_agg.values()
    )
    primary_total = confirmed_system_meters + pending_system_meters
    metrics = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "target_fabric_count": len(target_fabrics),
        "color_output_row_count": len(color_rows),
        "pending_output_row_count": len(pending_rows),
        "confirmed_system_meters_4m": _meters(confirmed_system_meters),
        "pending_system_meters_4m": _meters(pending_system_meters),
        "primary_fabric_system_meters_4m": _meters(primary_total),
        "confirmed_coverage_pct": (
            round(confirmed_system_meters / primary_total * 100, 2)
            if primary_total else 0.0
        ),
        "match_method_counts": dict(sorted(match_method_counts.items())),
        "unmatched_reason_counts": dict(sorted(unmatched_reason_counts.items())),
        "manual_mapping_audit": manual_catalog.audit(),
        "spu_manual_mapping_audit": spu_catalog.audit(),
        "forecast_audit": forecast_audit,
        "catalog_source_audit": catalog_source_audit,
        "inventory_policy": "颜色库存仅按面料编号+飞书领星新颜色缩写精确匹配；同一飞书颜色只分配一次",
        "color_system_policy": "A2023/B2024仅使用SKU自身明确标签；飞书颜色和人工映射绝不反推颜色体系",
    }
    return color_rows, total_rows, pending_rows, metrics


def export_workbook(
    color_rows: Sequence[Mapping[str, Any]],
    total_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any],
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()

    color_ws = workbook.active
    color_ws.title = "面料-颜色预计下单"
    color_headers = list(color_rows[0].keys()) if color_rows else [
        "面料", "面料编号", "最终飞书颜色", "领星新颜色缩写"
    ]
    _write_rows(color_ws, color_headers, color_rows)

    total_ws = workbook.create_sheet("面料总量")
    total_headers = list(total_rows[0].keys()) if total_rows else ["面料", "面料编号"]
    _write_rows(total_ws, total_headers, total_rows)

    pending_ws = workbook.create_sheet("待确认颜色")
    pending_headers = list(pending_rows[0].keys()) if pending_rows else [
        "面料", "颜色体系", "原始颜色编码", "未确认原因"
    ]
    _write_rows(pending_ws, pending_headers, pending_rows, pending=True)

    summary_ws = workbook.create_sheet("核对摘要")
    _style_header(summary_ws, ["指标", "值"])
    row_index = 2
    for key, value in metrics.items():
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        summary_ws.cell(row_index, 1, key)
        summary_ws.cell(row_index, 2, value)
        for cell in summary_ws[row_index]:
            cell.border = BORDER
            cell.alignment = LEFT
        row_index += 1
    summary_ws.freeze_panes = "A2"
    _autosize(summary_ws, maximum=80)

    output = output_dir / f"面料-颜色预计下单表_最终版_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    workbook.save(output)
    logger.info(
        "最终预计下单表完成：%s；飞书颜色 %d 行，面料总量 %d 行，待确认 %d 行，4月颜色覆盖率 %.2f%%",
        output,
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
    rows = await build_final_rows(
        manual_mapping_path=manual_mapping_path,
        spu_manual_mapping_path=spu_manual_mapping_path,
    )
    return export_workbook(*rows, output_dir=output_dir)


def main() -> Path:
    parser = argparse.ArgumentParser(description="生成SPU人工映射收口后的最终面料-颜色预计下单表")
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
