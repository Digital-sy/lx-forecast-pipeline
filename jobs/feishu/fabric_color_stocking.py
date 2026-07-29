#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""把 SKU 级预估销量只读聚合到“面料名+当前有效颜色”。

本模块是现有预测/采购管线后的核对层：

1. 从 ``预测对比表_SKU`` 读取当前月起四个月的 SKU 预估销量；
2. 优先读取预测表已存在的 ``品名/颜色编码/颜色中文名/颜色体系``；
   当前仓库主表尚未持久化这些列时，复用产品快照、SKU 颜色体系解析器和
   既有领星品名颜色解析规则补齐；
3. 复用 ``面料核价表`` 的 SPU→面料关系；
4. 从飞书当前有效清单读取 ``面料名/颜色/领星新颜色缩写``；
5. 严格按中文名、缩写、颜色体系映射的优先级匹配并聚合。

“预估备货量”是 SKU 预估销量件数，不在本层换算面料米数。同一 SPU 关联
多种面料时，件数会分别计入每个面料关联；米数仍由既有面料预估层负责。

匹配阶段不做模糊匹配、别名归一、大小写归一或空格删除。默认执行是
read-only dry-run：只读取 MySQL/飞书并生成本地 Excel/JSON 对照，不回写远端。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from common import get_logger
from common.database import db_cursor
from common.feishu import FeishuClient
from jobs.feishu import generate_fabric_forecast as fabric_base
from jobs.feishu.color_mapping_catalog import (
    SUPPORTED_SYSTEMS,
    UNKNOWN_SYSTEM,
    ColorMappingCatalog,
)
from jobs.feishu.color_system_resolver import ColorSystemResolver, normalize_sku
from jobs.feishu.generate_procurement_report_lx_color import parse_lingxing_color

logger = get_logger("fabric_color_stocking")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FORECAST_TABLE = "预测对比表_SKU"
SNAPSHOT_TABLE = "lxpm_product_category_snapshot"

# 这些是飞书资源坐标，不是认证凭证；认证仍只从 FEISHU_APP_ID /
# FEISHU_APP_SECRET 环境变量读取。默认值复用现有面料台账同步任务。
DEFAULT_BASE_TOKEN = "XT6pbXxmdas4rdsme0XctyefnGu"
DEFAULT_CATALOG_TABLE_ID = "tblMxScMbTyLQbyj"
DEFAULT_CATALOG_VIEW_ID = "vewHPceQyu"
DEFAULT_FABRIC_NAME_FIELD = "面料名"
DEFAULT_COLOR_FIELD = "颜色"
DEFAULT_LX_CODE_FIELD = "领星新颜色缩写"
DEFAULT_LINKED_FABRIC_FIELD = "面料品名"

MATCH_NAME = "中文名直连"
MATCH_CODE = "缩写"
MATCH_SYSTEM = "体系消歧"
MATCH_METHOD_ORDER = (MATCH_NAME, MATCH_CODE, MATCH_SYSTEM)

REASON_NO_FABRIC = "品名/SPU无面料映射"
REASON_FABRIC_INACTIVE = "面料不在当前清单"
REASON_PENDING_SYSTEM = "颜色体系待定无法消歧"
REASON_NAME_UNKNOWN = "中文名与清单无对应"
REASON_COLOR_INACTIVE = "颜色不在当前清单/疑似停用"
REASON_SYSTEM_MAPPING = "颜色体系+中文名无映射，无法消歧"
REASON_AMBIGUOUS = "当前清单一对多，无法唯一定位"
REASON_FORECAST_CONFLICT = "预估输出颜色字段冲突"

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(name="Microsoft YaHei", bold=True, color="FFFFFF")
PENDING_FILL = PatternFill("solid", fgColor="FFF2CC")
THIN = Side(style="thin", color="D9E1F2")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
RIGHT = Alignment(horizontal="right", vertical="center")
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _quantity(value: float) -> int | float:
    value = round(float(value or 0), 4)
    return int(value) if value.is_integer() else value


@dataclass(frozen=True)
class ForecastSku:
    sku: str
    spu: str
    product_name: str
    color_code: str
    color_name: str
    color_system: str
    forecast_qty: float
    field_issue: str = ""


@dataclass(frozen=True)
class CatalogRow:
    fabric_name: str
    color_name: str
    lingxing_code: str
    record_ids: tuple[str, ...] = ()
    raw_row_count: int = 1

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.fabric_name, self.color_name, self.lingxing_code


@dataclass(frozen=True)
class MatchDecision:
    row: CatalogRow | None
    method: str = ""
    reason_code: str = ""
    reason: str = ""


@dataclass
class StockingResult:
    main_rows: list[dict[str, Any]]
    unmatched_rows: list[dict[str, Any]]
    priority_rows: list[dict[str, Any]]
    metrics: dict[str, Any]


class CatalogIndex:
    """严格匹配索引，并把完全相同的飞书业务行显式折叠。"""

    def __init__(self, raw_rows: Iterable[CatalogRow], source_record_count: int | None = None):
        raw_rows = list(raw_rows)
        grouped: dict[tuple[str, str, str], list[CatalogRow]] = defaultdict(list)
        for row in raw_rows:
            if row.fabric_name and row.color_name:
                grouped[row.identity].append(row)

        rows: list[CatalogRow] = []
        for identity, duplicates in grouped.items():
            record_ids = tuple(
                sorted({rid for item in duplicates for rid in item.record_ids if rid})
            )
            rows.append(
                CatalogRow(
                    fabric_name=identity[0],
                    color_name=identity[1],
                    lingxing_code=identity[2],
                    record_ids=record_ids,
                    raw_row_count=sum(item.raw_row_count for item in duplicates),
                )
            )
        self.rows = sorted(rows, key=lambda row: row.identity)
        self.source_record_count = source_record_count if source_record_count is not None else len(raw_rows)
        self.raw_association_count = len(raw_rows)

        self.by_name: MutableMapping[tuple[str, str], list[CatalogRow]] = defaultdict(list)
        self.by_code: MutableMapping[tuple[str, str], list[CatalogRow]] = defaultdict(list)
        self.by_fabric: MutableMapping[str, list[CatalogRow]] = defaultdict(list)
        self.global_names: set[str] = set()
        pair_identities: MutableMapping[tuple[str, str], list[CatalogRow]] = defaultdict(list)

        for row in self.rows:
            self.by_name[(row.fabric_name, row.color_name)].append(row)
            if row.lingxing_code:
                self.by_code[(row.fabric_name, row.lingxing_code)].append(row)
            self.by_fabric[row.fabric_name].append(row)
            self.global_names.add(row.color_name)
            pair_identities[(row.fabric_name, row.color_name)].append(row)

        self.duplicate_business_rows = [
            {
                "面料名": row.fabric_name,
                "颜色": row.color_name,
                "领星新颜色缩写": row.lingxing_code,
                "原始行数": row.raw_row_count,
                "record_ids": list(row.record_ids),
            }
            for row in self.rows
            if row.raw_row_count > 1
        ]
        self.ambiguous_pairs = [
            {
                "面料名": fabric,
                "颜色": color,
                "候选缩写": sorted({row.lingxing_code for row in candidates}),
            }
            for (fabric, color), candidates in pair_identities.items()
            if len(candidates) > 1
        ]

    def audit(self) -> dict[str, Any]:
        rows_with_code = sum(row.raw_row_count for row in self.rows if row.lingxing_code)
        return {
            "source_record_count": self.source_record_count,
            "raw_fabric_association_count": self.raw_association_count,
            "unique_business_identity_count": len(self.rows),
            "unique_fabric_color_count": len(
                {(row.fabric_name, row.color_name) for row in self.rows}
            ),
            "rows_with_lingxing_code": rows_with_code,
            "exact_duplicate_business_group_count": len(self.duplicate_business_rows),
            "exact_duplicate_business_rows": self.duplicate_business_rows,
            "ambiguous_fabric_color_pair_count": len(self.ambiguous_pairs),
            "ambiguous_fabric_color_pairs": self.ambiguous_pairs,
            "strict_matching": True,
            "implicit_normalization": False,
        }


def _unique(candidates: Iterable[CatalogRow]) -> list[CatalogRow]:
    by_identity = {row.identity: row for row in candidates}
    return list(by_identity.values())


def match_catalog_row(
    forecast: ForecastSku,
    fabric_name: str,
    index: CatalogIndex,
    governance_catalog: ColorMappingCatalog,
) -> MatchDecision:
    """严格按中文名→缩写→体系映射匹配单个 SKU-面料关联。"""
    if forecast.field_issue:
        return MatchDecision(
            None,
            reason_code=REASON_FORECAST_CONFLICT,
            reason=forecast.field_issue,
        )

    if fabric_name not in index.by_fabric:
        return MatchDecision(
            None,
            reason_code=REASON_FABRIC_INACTIVE,
            reason=REASON_FABRIC_INACTIVE,
        )

    name_candidates = (
        _unique(index.by_name.get((fabric_name, forecast.color_name), ()))
        if forecast.color_name
        else []
    )
    if len(name_candidates) == 1:
        return MatchDecision(name_candidates[0], method=MATCH_NAME)

    code_candidates = (
        _unique(index.by_code.get((fabric_name, forecast.color_code), ()))
        if forecast.color_code
        else []
    )
    if len(code_candidates) == 1:
        return MatchDecision(code_candidates[0], method=MATCH_CODE)

    system_candidates: list[CatalogRow] = []
    mapping_found = False
    if forecast.color_system in SUPPORTED_SYSTEMS:
        mapped_by_code = governance_catalog.lookup(
            forecast.color_system, forecast.color_code
        )
        if mapped_by_code:
            mapping_found = True
            if mapped_by_code.chinese:
                system_candidates.extend(
                    index.by_name.get((fabric_name, mapped_by_code.chinese), ())
                )

        if forecast.color_name:
            mapped_by_name = governance_catalog.entries_for_name(
                forecast.color_system, forecast.color_name
            )
            if mapped_by_name:
                mapping_found = True
            for entry in mapped_by_name:
                system_candidates.extend(
                    index.by_code.get((fabric_name, entry.code), ())
                )

        system_candidates = _unique(system_candidates)
        if len(system_candidates) == 1:
            return MatchDecision(system_candidates[0], method=MATCH_SYSTEM)

    if len(name_candidates) > 1 or len(code_candidates) > 1 or len(system_candidates) > 1:
        return MatchDecision(
            None,
            reason_code=REASON_AMBIGUOUS,
            reason=REASON_AMBIGUOUS,
        )
    if forecast.color_system == UNKNOWN_SYSTEM:
        return MatchDecision(
            None,
            reason_code=REASON_PENDING_SYSTEM,
            reason=REASON_PENDING_SYSTEM,
        )
    if not mapping_found:
        return MatchDecision(
            None,
            reason_code=REASON_SYSTEM_MAPPING,
            reason=REASON_SYSTEM_MAPPING,
        )
    if forecast.color_name and forecast.color_name not in index.global_names:
        return MatchDecision(
            None,
            reason_code=REASON_NAME_UNKNOWN,
            reason=REASON_NAME_UNKNOWN,
        )
    return MatchDecision(
        None,
        reason_code=REASON_COLOR_INACTIVE,
        reason=REASON_COLOR_INACTIVE,
    )


def aggregate_stocking(
    forecasts: Sequence[ForecastSku],
    fabrics_by_spu: Mapping[str, Sequence[str]],
    catalog_rows: Sequence[CatalogRow],
    governance_catalog: ColorMappingCatalog,
    source_record_count: int | None = None,
) -> StockingResult:
    """聚合预估销量并保留所有未匹配 SKU。"""
    index = CatalogIndex(catalog_rows, source_record_count=source_record_count)
    buckets: dict[
        tuple[str, str, str],
        dict[str, Any],
    ] = {}
    sku_failures: dict[str, list[tuple[str, MatchDecision]]] = defaultdict(list)
    sku_matched_assignments: Counter[str] = Counter()
    method_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    total_assignments = 0
    matched_assignments = 0
    matched_assignment_qty = 0.0
    unmatched_assignment_qty = 0.0

    for forecast in forecasts:
        fabrics = list(dict.fromkeys(fabrics_by_spu.get(forecast.spu, ())))
        if not fabrics:
            total_assignments += 1
            decision = MatchDecision(
                None,
                reason_code=REASON_NO_FABRIC,
                reason=REASON_NO_FABRIC,
            )
            sku_failures[forecast.sku].append(("", decision))
            reason_counts[decision.reason_code] += 1
            unmatched_assignment_qty += forecast.forecast_qty
            continue

        for fabric_name in fabrics:
            total_assignments += 1
            decision = match_catalog_row(
                forecast, fabric_name, index, governance_catalog
            )
            if not decision.row:
                sku_failures[forecast.sku].append((fabric_name, decision))
                reason_counts[decision.reason_code] += 1
                unmatched_assignment_qty += forecast.forecast_qty
                continue

            matched_assignments += 1
            matched_assignment_qty += forecast.forecast_qty
            sku_matched_assignments[forecast.sku] += 1
            method_counts[decision.method] += 1
            key = decision.row.identity
            bucket = buckets.setdefault(
                key,
                {
                    "row": decision.row,
                    "qty": 0.0,
                    "skus": set(),
                    "methods": set(),
                },
            )
            bucket["qty"] += forecast.forecast_qty
            bucket["skus"].add(forecast.sku)
            bucket["methods"].add(decision.method)

    main_rows: list[dict[str, Any]] = []
    for key in sorted(buckets):
        bucket = buckets[key]
        row: CatalogRow = bucket["row"]
        methods = [
            method for method in MATCH_METHOD_ORDER if method in bucket["methods"]
        ]
        main_rows.append(
            {
                "面料名": row.fabric_name,
                "颜色(中文)": row.color_name,
                "领星新颜色缩写": row.lingxing_code,
                "预估备货量": _quantity(bucket["qty"]),
                "关联SKU数": len(bucket["skus"]),
                "匹配方式": "、".join(methods),
            }
        )

    forecast_by_sku = {row.sku: row for row in forecasts}
    unmatched_rows: list[dict[str, Any]] = []
    priority_rows: list[dict[str, Any]] = []
    for sku in sorted(sku_failures):
        forecast = forecast_by_sku[sku]
        failures = sku_failures[sku]
        reason_parts = []
        for fabric_name, decision in failures:
            prefix = f"{fabric_name}：" if fabric_name else ""
            reason_parts.append(f"{prefix}{decision.reason}")
        unmatched_rows.append(
            {
                "SKU": forecast.sku,
                "品名": forecast.product_name,
                "颜色编码": forecast.color_code,
                "颜色中文名": forecast.color_name,
                "颜色体系": forecast.color_system,
                "未匹配原因": "；".join(dict.fromkeys(reason_parts)),
            }
        )
        if (
            forecast.color_system == UNKNOWN_SYSTEM
            and any(
                decision.reason_code == REASON_PENDING_SYSTEM
                for _, decision in failures
            )
        ):
            priority_rows.append(
                {
                    "SKU": forecast.sku,
                    "品名": forecast.product_name,
                    "颜色编码": forecast.color_code,
                    "颜色中文名": forecast.color_name,
                    "预估销量": _quantity(forecast.forecast_qty),
                }
            )

    priority_rows.sort(key=lambda row: (-float(row["预估销量"]), row["SKU"]))
    input_skus = {row.sku for row in forecasts}
    unmatched_skus = set(sku_failures)
    partially_matched = {
        sku for sku in unmatched_skus if sku_matched_assignments.get(sku, 0) > 0
    }
    fully_matched = input_skus - unmatched_skus
    pending_qty = sum(float(row["预估销量"]) for row in priority_rows)
    success_rate = round(
        len(fully_matched) / len(input_skus) * 100, 2
    ) if input_skus else 0.0
    assignment_rate = round(
        matched_assignments / total_assignments * 100, 2
    ) if total_assignments else 0.0

    metrics = {
        "input_sku_count": len(input_skus),
        "input_forecast_qty": _quantity(
            sum(row.forecast_qty for row in forecasts)
        ),
        "fully_matched_sku_count": len(fully_matched),
        "partially_matched_sku_count": len(partially_matched),
        "unmatched_sku_count": len(unmatched_skus),
        "sku_full_match_rate_pct": success_rate,
        "fabric_assignment_count": total_assignments,
        "matched_fabric_assignment_count": matched_assignments,
        "unmatched_fabric_assignment_count": total_assignments - matched_assignments,
        "fabric_assignment_match_rate_pct": assignment_rate,
        "matched_fabric_assignment_forecast_qty": _quantity(
            matched_assignment_qty
        ),
        "unmatched_fabric_assignment_forecast_qty": _quantity(
            unmatched_assignment_qty
        ),
        "match_method_counts": {
            method: method_counts.get(method, 0) for method in MATCH_METHOD_ORDER
        },
        "unmatched_reason_counts": dict(sorted(reason_counts.items())),
        "pending_system_unmatched_sku_count": len(priority_rows),
        "pending_system_unmatched_forecast_qty": _quantity(pending_qty),
        "main_output_row_count": len(main_rows),
        "catalog_audit": index.audit(),
    }
    return StockingResult(main_rows, unmatched_rows, priority_rows, metrics)


def _table_columns(table_name: str) -> set[str]:
    with db_cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s
            """,
            (table_name,),
        )
        return {str(row.get("COLUMN_NAME") or "") for row in cursor.fetchall()}


def _month_start(value: date, delta: int) -> date:
    month_index = value.year * 12 + value.month - 1 + delta
    return date(month_index // 12, month_index % 12 + 1, 1)


def _load_snapshot_rows() -> list[dict[str, Any]]:
    if not _table_columns(SNAPSHOT_TABLE):
        logger.warning(f"{SNAPSHOT_TABLE} 不存在，无法补齐 SKU 颜色主数据")
        return []
    with db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT sku, spu, product_name, custom_fields_json
            FROM `{SNAPSHOT_TABLE}`
            WHERE sku IS NOT NULL AND CHAR_LENGTH(TRIM(CAST(sku AS CHAR))) > 0
            """
        )
        return list(cursor.fetchall())


def _snapshot_index(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], set[str]]:
    result: dict[str, Mapping[str, Any]] = {}
    conflicts: set[str] = set()
    for row in rows:
        sku = normalize_sku(row.get("sku"))
        if not sku:
            continue
        previous = result.get(sku)
        if previous is None:
            result[sku] = row
            continue
        old_values = (
            str(previous.get("product_name") or ""),
            str(previous.get("custom_fields_json") or ""),
        )
        new_values = (
            str(row.get("product_name") or ""),
            str(row.get("custom_fields_json") or ""),
        )
        if old_values != new_values:
            conflicts.add(sku)
    return result, conflicts


def load_forecast_skus(
    governance_catalog: ColorMappingCatalog,
    as_of: date | None = None,
) -> tuple[list[ForecastSku], dict[str, Any]]:
    """读取并核对 SKU 预估输出字段，缺列时复用现有主数据派生。"""
    as_of = as_of or date.today()
    columns = _table_columns(FORECAST_TABLE)
    required = {"SKU", "SPU", "统计日期", "系统预测销量"}
    missing_required = sorted(required - columns)
    if missing_required:
        raise RuntimeError(
            f"{FORECAST_TABLE} 缺少核心字段: {', '.join(missing_required)}"
        )

    color_fields = ("品名", "颜色编码", "颜色中文名", "颜色体系")
    available_color_fields = [field for field in color_fields if field in columns]
    select_fields = ["SKU", "SPU", *available_color_fields]
    select_sql = ", ".join(f"`{field}`" for field in select_fields)
    group_sql = ", ".join(f"`{field}`" for field in select_fields)
    start = _month_start(as_of, 0)
    end = _month_start(as_of, 4)
    with db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT {select_sql}, SUM(`系统预测销量`) AS `预估销量`
            FROM `{FORECAST_TABLE}`
            WHERE `统计日期` >= %s AND `统计日期` < %s
              AND `系统预测销量` > 0
            GROUP BY {group_sql}
            """,
            (start, end),
        )
        rows = list(cursor.fetchall())

    snapshot_rows = _load_snapshot_rows()
    snapshot_by_sku, snapshot_conflicts = _snapshot_index(snapshot_rows)
    resolver = ColorSystemResolver(snapshot_rows)
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        sku = normalize_sku(row.get("SKU"))
        if sku:
            grouped[sku].append(row)

    forecasts: list[ForecastSku] = []
    field_conflict_count = 0
    for sku, sku_rows in grouped.items():
        snapshot = snapshot_by_sku.get(sku, {})
        identity = resolver.resolve(
            sku,
            str(sku_rows[0].get("SPU") or snapshot.get("spu") or ""),
        )
        values: dict[str, set[str]] = {
            field_name: {
                str(row.get(field_name))
                for row in sku_rows
                if row.get(field_name) not in (None, "")
            }
            for field_name in color_fields
        }
        conflicts = [name for name, candidates in values.items() if len(candidates) > 1]
        if sku in snapshot_conflicts:
            conflicts.append("产品快照")
        field_issue = ""
        if conflicts:
            field_conflict_count += 1
            field_issue = (
                f"{REASON_FORECAST_CONFLICT}：{','.join(sorted(set(conflicts)))}"
            )

        def direct(field_name: str) -> str:
            candidates = values.get(field_name) or set()
            return next(iter(candidates)) if len(candidates) == 1 else ""

        product_name = direct("品名") or str(snapshot.get("product_name") or "")
        color_code = direct("颜色编码") or identity.color_code
        color_system = direct("颜色体系")
        if color_system not in SUPPORTED_SYSTEMS:
            color_system = identity.color_system
        color_name = direct("颜色中文名") or parse_lingxing_color(product_name)
        if not color_name and color_system in SUPPORTED_SYSTEMS:
            entry = governance_catalog.lookup(color_system, color_code)
            color_name = entry.chinese if entry else ""

        forecasts.append(
            ForecastSku(
                sku=sku,
                spu=str(
                    sku_rows[0].get("SPU")
                    or snapshot.get("spu")
                    or identity.spu
                ),
                product_name=product_name,
                color_code=color_code,
                color_name=color_name,
                color_system=color_system or UNKNOWN_SYSTEM,
                forecast_qty=sum(float(row.get("预估销量") or 0) for row in sku_rows),
                field_issue=field_issue,
            )
        )

    audit = {
        "forecast_table": FORECAST_TABLE,
        "forecast_core_fields": sorted(required),
        "requested_color_fields": list(color_fields),
        "persisted_color_fields_found": available_color_fields,
        "color_field_mode": (
            "预测表直接读取"
            if len(available_color_fields) == len(color_fields)
            else "产品快照+现有解析器兼容补齐"
        ),
        "forecast_window_start": start.isoformat(),
        "forecast_window_end_exclusive": end.isoformat(),
        "forecast_grouped_sku_count": len(forecasts),
        "forecast_color_field_conflict_sku_count": field_conflict_count,
        "sample": [
            {
                "SKU": row.sku,
                "品名": row.product_name,
                "颜色编码": row.color_code,
                "颜色中文名": row.color_name,
                "颜色体系": row.color_system,
                "预估销量": _quantity(row.forecast_qty),
            }
            for row in forecasts[:5]
        ],
    }
    return sorted(forecasts, key=lambda row: row.sku), audit


def load_fabrics_by_spu() -> dict[str, list[str]]:
    """复用现有面料核价表读取结构，保留一个 SPU 的全部面料。"""
    usage = fabric_base.get_fabric_price_data()
    result: dict[str, list[str]] = defaultdict(list)
    for spu, fabric_name in usage:
        if fabric_name not in result[spu]:
            result[spu].append(fabric_name)
    return dict(result)


def _texts(value: Any) -> list[str]:
    """提取飞书展示文本；不 strip/casefold，不把 record_id 当文本。"""
    if value is None:
        return []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_texts(item))
        return result
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            if value.get(key) is not None and not isinstance(
                value.get(key), (dict, list)
            ):
                return [str(value.get(key))]
        return []
    return [str(value)]


def _link_record_ids(value: Any) -> list[str]:
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_link_record_ids(item))
        return result
    if isinstance(value, dict):
        for key in ("record_id", "id", "link_record_id"):
            raw = value.get(key)
            if raw:
                return [str(raw)]
    return []


def _linked_table_id(field_info: Mapping[str, Any]) -> str:
    prop = field_info.get("property") or {}
    candidates = (
        field_info.get("link_table"),
        field_info.get("link_table_id"),
        prop.get("table_id") if isinstance(prop, Mapping) else None,
        prop.get("tableId") if isinstance(prop, Mapping) else None,
    )
    return next((str(value) for value in candidates if value), "")


def parse_feishu_catalog_records(
    raw_records: Sequence[Mapping[str, Any]],
    linked_fabric_names: Mapping[str, str],
    fabric_field: str = DEFAULT_FABRIC_NAME_FIELD,
    color_field: str = DEFAULT_COLOR_FIELD,
    code_field: str = DEFAULT_LX_CODE_FIELD,
) -> tuple[list[CatalogRow], dict[str, Any]]:
    rows: list[CatalogRow] = []
    invalid_records: list[str] = []
    for record in raw_records:
        fields = record.get("fields") or {}
        record_id = str(record.get("record_id") or "")
        fabric_value = fields.get(fabric_field)
        fabric_names = _texts(fabric_value)
        if not fabric_names:
            fabric_names = [
                linked_fabric_names[record_id]
                for record_id in _link_record_ids(fabric_value)
                if record_id in linked_fabric_names
            ]
        color_names = _texts(fields.get(color_field))
        color_name = color_names[0] if color_names else ""
        code_values = _texts(fields.get(code_field))
        code = code_values[0] if code_values else ""

        if not fabric_names or not color_name:
            invalid_records.append(record_id)
            continue
        for fabric_name in dict.fromkeys(fabric_names):
            rows.append(
                CatalogRow(
                    fabric_name=fabric_name,
                    color_name=color_name,
                    lingxing_code=code,
                    record_ids=(record_id,) if record_id else (),
                )
            )
    return rows, {
        "source_record_count": len(raw_records),
        "parsed_fabric_association_count": len(rows),
        "invalid_record_count": len(invalid_records),
        "invalid_record_ids": invalid_records,
    }


async def load_catalog_from_feishu(
    base_token: str,
    table_id: str,
    view_id: str,
    fabric_field: str = DEFAULT_FABRIC_NAME_FIELD,
    color_field: str = DEFAULT_COLOR_FIELD,
    code_field: str = DEFAULT_LX_CODE_FIELD,
    linked_fabric_field: str = DEFAULT_LINKED_FABRIC_FIELD,
) -> tuple[list[CatalogRow], dict[str, Any]]:
    client = FeishuClient(
        app_token=base_token,
        table_id=table_id,
        view_id=view_id or None,
    )
    field_map = await client.get_table_fields()
    actual_fields = set(field_map.values())
    required = {fabric_field, color_field, code_field}
    missing = sorted(required - actual_fields)
    if missing:
        raise RuntimeError(f"飞书清单缺少字段: {', '.join(missing)}")

    raw_records = await client.read_records(page_size=500)
    fabric_info = await client.get_field_info(fabric_field)
    linked_table_id = (
        os.getenv("FABRIC_COLOR_LINKED_TABLE_ID")
        or _linked_table_id(fabric_info)
    )
    linked_names: dict[str, str] = {}
    unresolved_link_ids = {
        record_id
        for record in raw_records
        for record_id in _link_record_ids((record.get("fields") or {}).get(fabric_field))
    }
    if unresolved_link_ids:
        if not linked_table_id:
            raise RuntimeError(
                "面料名为关联字段但无法解析关联表；请配置 FABRIC_COLOR_LINKED_TABLE_ID"
            )
        linked_client = FeishuClient(
            app_token=base_token,
            table_id=linked_table_id,
        )
        linked_records = await linked_client.read_records(page_size=500)
        for record in linked_records:
            values = _texts((record.get("fields") or {}).get(linked_fabric_field))
            if values:
                linked_names[str(record.get("record_id") or "")] = values[0]

    rows, parse_audit = parse_feishu_catalog_records(
        raw_records,
        linked_names,
        fabric_field=fabric_field,
        color_field=color_field,
        code_field=code_field,
    )
    audit = {
        "base_token": base_token,
        "table_id": table_id,
        "view_id": view_id,
        "view_scope": "指定视图（视图过滤为空时等同全表）" if view_id else "全表",
        "actual_fields": sorted(actual_fields),
        "field_mapping": {
            "面料": fabric_field,
            "中文颜色": color_field,
            "SKU颜色编码": code_field,
            "关联面料展示字段": linked_fabric_field,
            "关联面料表": linked_table_id,
        },
        **parse_audit,
    }
    return rows, audit


def _style_header(ws: Any, headers: Sequence[str]) -> None:
    for column, header in enumerate(headers, 1):
        cell = ws.cell(1, column, header)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER
        cell.border = BORDER


def _autosize(ws: Any, minimum: int = 10, maximum: int = 50) -> None:
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
                cell.number_format = "#,##0.####"
            if pending:
                cell.fill = PENDING_FILL
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    _autosize(ws)


def _flatten_metrics(
    value: Any,
    prefix: str = "",
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(child, (Mapping, list, tuple)):
                rows.extend(_flatten_metrics(child, child_prefix))
            else:
                rows.append((child_prefix, str(child)))
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            if isinstance(child, (Mapping, list, tuple)):
                rows.extend(_flatten_metrics(child, child_prefix))
            else:
                rows.append((child_prefix, str(child)))
    else:
        rows.append((prefix, str(value)))
    return rows


def export_workbook(
    result: StockingResult,
    output_dir: Path,
    generated_at: datetime | None = None,
) -> tuple[Path, Path]:
    generated_at = generated_at or datetime.now()
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"面料-颜色备货对照_{generated_at.strftime('%Y%m%d_%H%M%S')}"
    workbook_path = output_dir / f"{stem}.xlsx"
    metrics_path = output_dir / f"{stem}_核对指标.json"

    workbook = Workbook()
    main_ws = workbook.active
    main_ws.title = "面料-颜色备货"
    _write_rows(
        main_ws,
        [
            "面料名",
            "颜色(中文)",
            "领星新颜色缩写",
            "预估备货量",
            "关联SKU数",
            "匹配方式",
        ],
        result.main_rows,
    )

    unmatched_ws = workbook.create_sheet("未匹配清单")
    _write_rows(
        unmatched_ws,
        ["SKU", "品名", "颜色编码", "颜色中文名", "颜色体系", "未匹配原因"],
        result.unmatched_rows,
    )

    priority_ws = workbook.create_sheet("优先补标清单")
    _write_rows(
        priority_ws,
        ["SKU", "品名", "颜色编码", "颜色中文名", "预估销量"],
        result.priority_rows,
        pending=True,
    )

    summary_ws = workbook.create_sheet("核对摘要")
    _style_header(summary_ws, ["指标", "值"])
    for row_index, (key, value) in enumerate(
        _flatten_metrics(result.metrics), 2
    ):
        summary_ws.cell(row_index, 1, key)
        summary_ws.cell(row_index, 2, value)
        for cell in summary_ws[row_index]:
            cell.border = BORDER
            cell.alignment = LEFT
    summary_ws.freeze_panes = "A2"
    _autosize(summary_ws, maximum=80)

    workbook.save(workbook_path)
    metrics_path.write_text(
        json.dumps(result.metrics, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return workbook_path, metrics_path


async def run_read_only(
    output_dir: Path,
    as_of: date | None = None,
    base_token: str | None = None,
    table_id: str | None = None,
    view_id: str | None = None,
) -> tuple[StockingResult, Path, Path]:
    """执行只读 dry-run 并生成对照文件。"""
    logger.info("开始面料-颜色备货只读 dry-run（不写 MySQL/飞书）")
    governance_catalog = ColorMappingCatalog.from_runtime(strict=True)
    catalog_rows, catalog_source_audit = await load_catalog_from_feishu(
        base_token=base_token
        or os.getenv("FABRIC_COLOR_CATALOG_BASE_TOKEN", DEFAULT_BASE_TOKEN),
        table_id=table_id
        or os.getenv("FABRIC_COLOR_CATALOG_TABLE_ID", DEFAULT_CATALOG_TABLE_ID),
        view_id=(
            view_id
            if view_id is not None
            else os.getenv("FABRIC_COLOR_CATALOG_VIEW_ID", DEFAULT_CATALOG_VIEW_ID)
        ),
    )
    forecasts, forecast_audit = load_forecast_skus(
        governance_catalog, as_of=as_of
    )
    fabrics_by_spu = load_fabrics_by_spu()
    result = aggregate_stocking(
        forecasts=forecasts,
        fabrics_by_spu=fabrics_by_spu,
        catalog_rows=catalog_rows,
        governance_catalog=governance_catalog,
        source_record_count=catalog_source_audit["source_record_count"],
    )
    result.metrics["forecast_audit"] = forecast_audit
    result.metrics["catalog_source_audit"] = catalog_source_audit
    result.metrics["fabric_mapping_spu_count"] = len(fabrics_by_spu)
    result.metrics["source_adapter_disclosures"] = [
        "SKU标识复用 normalize_sku：NFKC、转大写、下划线转连字符、合并重复连字符",
        "预测表缺少颜色字段时复用 parse_lingxing_color：从品名提取“数字#中文名”并删除其中空白",
        "SPU→面料复用 get_fabric_price_data：仅 strip 首尾空白",
        "体系消歧复用 ColorMappingCatalog：颜色编码转大写并删除空白；匹配方式单列为“体系消歧”",
        "清单直接中文名/缩写匹配不做 strip、casefold、模糊匹配或别名归一",
    ]
    result.metrics["generated_at"] = datetime.now().isoformat(timespec="seconds")
    workbook_path, metrics_path = export_workbook(result, output_dir)
    logger.info(
        "dry-run 完成：SKU完整匹配率 %.2f%%，面料关联匹配率 %.2f%%，"
        "未匹配 SKU %s，待定体系优先补标 %s（预估销量 %s）",
        result.metrics["sku_full_match_rate_pct"],
        result.metrics["fabric_assignment_match_rate_pct"],
        result.metrics["unmatched_sku_count"],
        result.metrics["pending_system_unmatched_sku_count"],
        result.metrics["pending_system_unmatched_forecast_qty"],
    )
    logger.info(f"Excel: {workbook_path}")
    logger.info(f"核对指标: {metrics_path}")
    return result, workbook_path, metrics_path


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="只读生成 SKU 颜色销量→面料-颜色备货量对照"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            os.getenv("PROCUREMENT_EXPORT_DIR", str(PROJECT_ROOT / "exports"))
        ),
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        help="预测窗口基准日 YYYY-MM-DD；默认今天",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="只读模式（当前唯一支持模式，不写数据库或飞书）",
    )
    args = parser.parse_args()
    asyncio.run(run_read_only(output_dir=args.output_dir, as_of=args.as_of))


if __name__ == "__main__":
    main()
