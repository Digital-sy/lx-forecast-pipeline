#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""A/B 同源正式入口 + 业务可判断未确认颜色。

在已经验证的 ``export_fabric_color_order_forecast_business_ab`` 外再叠加一层
颜色展示政策，不改变销量、单耗、损耗、库存总量或 A/B 拆色范围：

1. 业务已经确认的 ``面料+原始颜色编码+SPU -> 飞书颜色`` 由版本化 CSV 管理，
   优先于服务器旧 SPU 人工映射；
2. 仍无法唯一确认的颜色不再只藏在“待确认颜色”页，而是同时进入正式业务主表；
3. 未确认颜色在主表中只写中文/原始识别名，不带 ``#``，颜色缩写、颜色库存、
   飞书记录 ID 均留空，避免伪装成已确认色；
4. “待确认颜色”页仍完整保留，供后续业务追溯和确认；
5. 方案 A / B 使用同一套颜色治理政策，二者差异仍只在“是否仅主面料拆色”。

本模块继续复用原 A/B 入口的：同源快照、飞书数字精度兼容和写后回读校验。
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from common import get_logger
from jobs.feishu import export_fabric_color_order_forecast_business as business
from jobs.feishu import export_fabric_color_order_forecast_business_ab as base_ab
from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu.fabric_color_stocking_spu import (
    DEFAULT_SPU_MANUAL_MAPPING_PATH,
    MATCH_SPU_MANUAL,
)

logger = get_logger("export_fabric_color_order_forecast_business_ab_reviewable")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIRMED_OVERRIDE_PATH = Path(
    os.getenv(
        "FABRIC_COLOR_BUSINESS_CONFIRMED_OVERRIDE_PATH",
        str(PROJECT_ROOT / "config" / "fabric_color_business_confirmed_overrides.csv"),
    )
)

REASON_BUSINESS_TARGET_MISSING = "业务确认目标颜色不在当前飞书清单"
REASON_BUSINESS_TARGET_AMBIGUOUS = "业务确认目标颜色在当前飞书清单存在多条，禁止自动选择"
UNCONFIRMED_METHOD = "待业务判断（无#）"
UNCONFIRMED_INVENTORY_STATUS = "待业务判断颜色：未分配颜色库存"


def _load_confirmed_overrides(path: Path) -> dict[tuple[str, str, str], str]:
    """读取版本化业务确认；键严格为 面料+原色码+SPU。"""
    if not path.exists():
        logger.warning("业务确认映射不存在，按 0 条处理: %s", path)
        return {}

    required = {"面料名", "原始颜色编码", "SPU", "最终飞书颜色"}
    result: dict[tuple[str, str, str], str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(required - set(reader.fieldnames or ()))
        if missing:
            raise RuntimeError(
                "业务确认映射缺少字段: " + ", ".join(missing)
            )
        for row in reader:
            fabric = str(row.get("面料名") or "").strip()
            code = str(row.get("原始颜色编码") or "").strip()
            spu = str(row.get("SPU") or "").strip()
            target = str(row.get("最终飞书颜色") or "").strip()
            if not all((fabric, code, spu, target)):
                continue
            key = (fabric, code, spu)
            if key in result and result[key] != target:
                raise RuntimeError(
                    f"业务确认映射冲突: {key} -> {result[key]!r} / {target!r}"
                )
            result[key] = target
    logger.info("读取业务确认颜色映射：%d 条", len(result))
    return result


def _clean_unconfirmed_label(value: Any) -> str:
    """未确认颜色绝不带 #；仅移除前导色号，不做色相猜测。"""
    text = str(value or "").strip()
    text = re.sub(r"^\s*\d+\s*#\s*", "", text)
    return text.replace("#", "").strip()


def _manual_target_from_reason(reason: str) -> str:
    markers = (
        "SPU人工确认目标颜色不在当前飞书清单：",
        f"{REASON_BUSINESS_TARGET_MISSING}：",
    )
    for marker in markers:
        if marker in reason:
            return _clean_unconfirmed_label(reason.split(marker, 1)[1])
    return ""


def _governance_label(
    governance: stocking.ColorMappingCatalog,
    system: str,
    code: str,
) -> str:
    system = str(system or "").strip()
    code = str(code or "").strip()
    if not code:
        return ""

    if system in stocking.SUPPORTED_SYSTEMS:
        entry = governance.lookup(system, code)
        if entry and entry.chinese:
            return _clean_unconfirmed_label(entry.chinese)

    names: set[str] = set()
    for candidate_system in stocking.SUPPORTED_SYSTEMS:
        for entry in governance.entries_for_code(candidate_system, code):
            if entry.chinese:
                label = _clean_unconfirmed_label(entry.chinese)
                if label:
                    names.add(label)
    if len(names) == 1:
        return next(iter(names))
    return ""


def _pending_display_label(
    row: Mapping[str, Any],
    governance: stocking.ColorMappingCatalog,
) -> str:
    """为未确认需求生成业务可读名称，但不猜测具体飞书编号色。"""
    reason = str(row.get("未确认原因") or "")
    manual_target = _manual_target_from_reason(reason)
    if manual_target:
        return manual_target

    system = str(row.get("颜色体系") or "").strip()
    code = str(row.get("原始颜色编码") or "").strip()
    governed = _governance_label(governance, system, code)
    if governed:
        return governed

    return _clean_unconfirmed_label(code) or "待判断"


def _join_unique(values: Sequence[Any]) -> str:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in str(value or "").split("、"):
            part = part.strip()
            if part and part not in seen:
                seen.add(part)
                result.append(part)
    return "、".join(result)


def _promote_pending_rows(
    color_rows: Sequence[Mapping[str, Any]],
    total_rows: Sequence[Mapping[str, Any]],
    pending_rows: Sequence[Mapping[str, Any]],
    governance: stocking.ColorMappingCatalog,
) -> list[dict[str, Any]]:
    """把待确认需求复制到正式颜色主表；原 pending_rows 不删除。"""
    result = [dict(row) for row in color_rows]
    total_by_fabric = {
        str(row.get("面料") or ""): row
        for row in total_rows
    }

    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for pending in pending_rows:
        fabric = str(pending.get("面料") or "").strip()
        label = _pending_display_label(pending, governance)
        if not fabric or not label:
            continue

        key = (fabric, label)
        if key not in groups:
            total = total_by_fabric.get(fabric, {})
            groups[key] = {
                "面料": fabric,
                "面料编号": str(total.get("面料编号") or ""),
                "最终飞书颜色": label,
                "领星新颜色缩写": "",
                "飞书记录ID": "",
                "原颜色体系": "",
                "原颜色编码": "",
                "匹配方式": UNCONFIRMED_METHOD,
                "关联SPU数": 0,
                "关联SKU数": 0,
                "库存匹配键": "",
                "库存归属状态": UNCONFIRMED_INVENTORY_STATUS,
                "库存量/条": 0,
                "库存量/米": 0.0,
                "待到货量/条": 0,
                "待到货量/米": 0.0,
                "用量信息缺失SPU": "",
            }

        target = groups[key]
        target["原颜色体系"] = _join_unique(
            [target.get("原颜色体系"), pending.get("颜色体系")]
        )
        target["原颜色编码"] = _join_unique(
            [target.get("原颜色编码"), pending.get("原始颜色编码")]
        )
        target["关联SPU数"] = int(target.get("关联SPU数") or 0) + int(
            pending.get("关联SPU数") or 0
        )
        target["关联SKU数"] = int(target.get("关联SKU数") or 0) + int(
            pending.get("关联SKU数") or 0
        )
        target["用量信息缺失SPU"] = _join_unique(
            [target.get("用量信息缺失SPU"), pending.get("用量信息缺失SPU")]
        )

        for name, value in pending.items():
            if not str(name).endswith("/米"):
                continue
            target[name] = round(
                float(target.get(name) or 0) + float(value or 0),
                2,
            )

    result.extend(groups.values())
    result.sort(
        key=lambda row: (
            stocking.TARGET_FABRIC_ORDER.get(str(row.get("面料") or ""), 999),
            str(row.get("最终飞书颜色") or ""),
            str(row.get("领星新颜色缩写") or ""),
        )
    )
    return result


@contextmanager
def reviewable_color_policy(
    confirmed_override_path: Path = DEFAULT_CONFIRMED_OVERRIDE_PATH,
) -> Iterator[None]:
    """局部叠加“已确认覆盖 + 未确认无#写主表”政策。"""
    overrides = _load_confirmed_overrides(confirmed_override_path)
    governance = stocking.ColorMappingCatalog.from_runtime(strict=True)

    original_resolve = final_export._resolve_final_color
    original_build = final_export.build_final_rows

    def resolve_with_business_confirmed(
        forecast: stocking.ForecastSku,
        fabric_name: str,
        index: stocking.CatalogIndex,
        governance_catalog: stocking.ColorMappingCatalog,
        manual_catalog: stocking.ManualMappingCatalog,
        spu_catalog: Any,
    ) -> stocking.MatchDecision:
        key = (
            str(fabric_name or "").strip(),
            str(forecast.color_code or "").strip(),
            str(forecast.spu or "").strip(),
        )
        target = overrides.get(key)
        if target:
            candidates = stocking._unique(
                index.by_name.get((key[0], target), ())
            )
            if len(candidates) == 1:
                return stocking.MatchDecision(
                    candidates[0],
                    method=MATCH_SPU_MANUAL,
                )
            if len(candidates) > 1:
                return stocking.MatchDecision(
                    None,
                    reason_code=REASON_BUSINESS_TARGET_AMBIGUOUS,
                    reason=f"{REASON_BUSINESS_TARGET_AMBIGUOUS}：{target}",
                    candidates=tuple(candidates),
                )
            return stocking.MatchDecision(
                None,
                reason_code=REASON_BUSINESS_TARGET_MISSING,
                reason=f"{REASON_BUSINESS_TARGET_MISSING}：{target}",
            )

        return original_resolve(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog,
            spu_catalog,
        )

    async def build_with_reviewable_pending(*args, **kwargs):
        color_rows, total_rows, pending_rows, metrics = await original_build(
            *args, **kwargs
        )
        confirmed_count = len(color_rows)
        promoted = _promote_pending_rows(
            color_rows,
            total_rows,
            pending_rows,
            governance,
        )
        promoted_count = len(promoted) - confirmed_count
        metrics = dict(metrics)
        metrics["confirmed_color_output_row_count"] = confirmed_count
        metrics["business_unconfirmed_color_row_count"] = promoted_count
        metrics["color_output_row_count"] = len(promoted)
        metrics["business_unconfirmed_color_policy"] = (
            "未确认颜色同时进入正式主表；颜色名不带#，颜色缩写/库存匹配留空；"
            "待确认页继续保留"
        )
        logger.info(
            "业务颜色展示政策：已确认 %d 行；未确认无#写入主表 %d 行；主表颜色合计 %d 行",
            confirmed_count,
            promoted_count,
            len(promoted),
        )
        return promoted, total_rows, pending_rows, metrics

    final_export._resolve_final_color = resolve_with_business_confirmed
    final_export.build_final_rows = build_with_reviewable_pending
    try:
        yield
    finally:
        final_export._resolve_final_color = original_resolve
        final_export.build_final_rows = original_build


async def run(
    output_dir: Path,
    manual_mapping_path: Path,
    spu_manual_mapping_path: Path,
    write_feishu: bool = False,
    feishu_table_name: str = business.DEFAULT_FEISHU_TABLE_NAME,
    scheme_a_history_table: str = business.SCHEME_A_HISTORY_TABLE,
    confirmed_override_path: Path = DEFAULT_CONFIRMED_OVERRIDE_PATH,
) -> Path:
    with reviewable_color_policy(confirmed_override_path):
        return await base_ab.run(
            output_dir=output_dir,
            manual_mapping_path=manual_mapping_path,
            spu_manual_mapping_path=spu_manual_mapping_path,
            write_feishu=write_feishu,
            feishu_table_name=feishu_table_name,
            scheme_a_history_table=scheme_a_history_table,
        )


def main() -> Path:
    parser = argparse.ArgumentParser(
        description="同源A/B正式面料预估：确认色归正式编号色，未确认色无#写主表"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(os.getenv("PROCUREMENT_EXPORT_DIR", "/opt/apps/pythondata/exports")),
    )
    parser.add_argument(
        "--manual-mapping",
        type=Path,
        default=stocking.DEFAULT_MANUAL_MAPPING_PATH,
    )
    parser.add_argument(
        "--spu-manual-mapping",
        type=Path,
        default=DEFAULT_SPU_MANUAL_MAPPING_PATH,
    )
    parser.add_argument(
        "--write-feishu",
        action="store_true",
    )
    parser.add_argument(
        "--feishu-table-name",
        default=business.DEFAULT_FEISHU_TABLE_NAME,
    )
    parser.add_argument(
        "--scheme-a-history-table",
        default=business.SCHEME_A_HISTORY_TABLE,
    )
    parser.add_argument(
        "--confirmed-override",
        type=Path,
        default=DEFAULT_CONFIRMED_OVERRIDE_PATH,
    )
    args = parser.parse_args()
    return asyncio.run(
        run(
            output_dir=args.output_dir,
            manual_mapping_path=args.manual_mapping,
            spu_manual_mapping_path=args.spu_manual_mapping,
            write_feishu=args.write_feishu,
            feishu_table_name=args.feishu_table_name,
            scheme_a_history_table=args.scheme_a_history_table,
            confirmed_override_path=args.confirmed_override,
        )
    )


if __name__ == "__main__":
    main()
