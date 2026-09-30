#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""正式 A/B 同源入口：业务确认映射 A/B 共用，未确认无#仅进入方案 B。

目的：
1. 版本化业务确认映射（面料+原始颜色编码+SPU -> 当前飞书颜色）同时作用于 A/B；
2. 方案 B：仍无法唯一确认的颜色也写入正式主表，颜色名不带 #，颜色缩写、
   颜色库存和飞书记录 ID 留空，同时继续保留在“待确认颜色”页；
3. 方案 A：保持历史对照口径，不把未确认颜色提升到颜色主表；
4. 继续复用已验证的 A/B 同源快照、飞书数字精度兼容和写后回读校验。
"""
from __future__ import annotations

import argparse
import asyncio
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from common import get_logger
from jobs.feishu import export_fabric_color_order_forecast_business as business
from jobs.feishu import export_fabric_color_order_forecast_business_ab as base_ab
from jobs.feishu import export_fabric_color_order_forecast_business_ab_reviewable as reviewable
from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu.fabric_color_stocking_spu import (
    DEFAULT_SPU_MANUAL_MAPPING_PATH,
    MATCH_SPU_MANUAL,
)

logger = get_logger("export_fabric_color_order_forecast_business_ab_reviewable_b")


@contextmanager
def reviewable_b_only_policy(
    confirmed_override_path: Path = reviewable.DEFAULT_CONFIRMED_OVERRIDE_PATH,
) -> Iterator[None]:
    """业务确认映射作用于 A/B；未确认无#提升仅作用于方案 B。"""
    overrides = reviewable._load_confirmed_overrides(confirmed_override_path)
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
            candidates = stocking._unique(index.by_name.get((key[0], target), ()))
            if len(candidates) == 1:
                return stocking.MatchDecision(candidates[0], method=MATCH_SPU_MANUAL)
            if len(candidates) > 1:
                return stocking.MatchDecision(
                    None,
                    reason_code=reviewable.REASON_BUSINESS_TARGET_AMBIGUOUS,
                    reason=f"{reviewable.REASON_BUSINESS_TARGET_AMBIGUOUS}：{target}",
                    candidates=tuple(candidates),
                )
            return stocking.MatchDecision(
                None,
                reason_code=reviewable.REASON_BUSINESS_TARGET_MISSING,
                reason=f"{reviewable.REASON_BUSINESS_TARGET_MISSING}：{target}",
            )

        return original_resolve(
            forecast,
            fabric_name,
            index,
            governance_catalog,
            manual_catalog,
            spu_catalog,
        )

    async def build_with_b_only_reviewable(*args, **kwargs):
        color_rows, total_rows, pending_rows, metrics = await original_build(*args, **kwargs)

        mode = str(kwargs.get("color_split_mode") or "B").strip().upper()
        if mode != "B":
            metrics = dict(metrics)
            metrics["business_unconfirmed_color_policy"] = (
                "方案A历史对照：未确认颜色不提升到颜色主表；业务已确认映射仍生效"
            )
            logger.info(
                "方案A历史对照：已确认颜色 %d 行；未确认 %d 行保持待确认，不写无#颜色主表",
                len(color_rows),
                len(pending_rows),
            )
            return color_rows, total_rows, pending_rows, metrics

        confirmed_count = len(color_rows)
        promoted = reviewable._promote_pending_rows(
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
            "方案B：未确认颜色同时进入正式主表；颜色名不带#，颜色缩写/库存匹配留空；"
            "待确认页继续保留"
        )
        logger.info(
            "方案B业务颜色政策：已确认 %d 行；未确认无#写入主表 %d 行；主表颜色合计 %d 行",
            confirmed_count,
            promoted_count,
            len(promoted),
        )
        return promoted, total_rows, pending_rows, metrics

    final_export._resolve_final_color = resolve_with_business_confirmed
    final_export.build_final_rows = build_with_b_only_reviewable
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
    confirmed_override_path: Path = reviewable.DEFAULT_CONFIRMED_OVERRIDE_PATH,
) -> Path:
    with reviewable_b_only_policy(confirmed_override_path):
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
        description="同源A/B正式面料预估：确认映射A/B共用，未确认无#仅写方案B主表"
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
    parser.add_argument("--write-feishu", action="store_true")
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
        default=reviewable.DEFAULT_CONFIRMED_OVERRIDE_PATH,
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
