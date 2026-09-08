#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""方案 A/B 同源快照入口。

正式业务仍由 ``export_fabric_color_order_forecast_business`` 负责：
- 方案 B：写 Excel + 飞书业务 21 列；
- 方案 A：仅保留 MySQL 每日快照。

本入口只解决 A/B 两次计算的数据一致性问题。它在一次进程内缓存两套计算共同
依赖的只读源数据，并冻结本轮计算时间，使 A/B 严格使用同一批：
SKU 主数据、预测、面料核价、颜色目录、人工映射、库存/在途和运行月份。

缓存仅在本次 ``run`` 的上下文内有效，结束后恢复原函数，不影响其他任务。
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import os
from contextlib import contextmanager
from datetime import datetime as real_datetime
from pathlib import Path
from typing import Any, Iterator

from common import get_logger
from jobs.feishu import export_fabric_color_order_forecast_business as business
from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu import generate_fabric_forecast as fabric_base
from jobs.feishu import generate_fabric_forecast_color_system as color_system
from jobs.feishu.fabric_color_stocking_spu import DEFAULT_SPU_MANUAL_MAPPING_PATH

logger = get_logger("export_fabric_color_order_forecast_business_ab")


class _FrozenDateTime(real_datetime):
    """让 A/B 两次 build 使用完全相同的 datetime.now()。"""

    _fixed_now = real_datetime.now()

    @classmethod
    def set_fixed_now(cls, value: real_datetime) -> None:
        cls._fixed_now = value

    @classmethod
    def now(cls, tz=None):
        value = cls._fixed_now
        if tz is not None:
            if value.tzinfo is None:
                value = value.astimezone()
            return value.astimezone(tz)
        return value.replace(tzinfo=None) if value.tzinfo else value


@contextmanager
def shared_ab_source_snapshot() -> Iterator[dict[str, dict[str, int]]]:
    """缓存 A/B 两次计算使用的只读数据源，并在退出时恢复所有函数。"""
    originals: list[tuple[Any, str, Any]] = []
    stats: dict[str, dict[str, int]] = {}
    fixed_now = real_datetime.now()
    _FrozenDateTime.set_fixed_now(fixed_now)

    def patch_once(obj: Any, name: str) -> None:
        original = getattr(obj, name)
        originals.append((obj, name, original))
        state: dict[str, Any] = {"loaded": False, "value": None}
        stats[name] = {"source_reads": 0, "cache_hits": 0}

        if inspect.iscoroutinefunction(original):
            async def async_cached(*args, **kwargs):
                if not state["loaded"]:
                    state["value"] = await original(*args, **kwargs)
                    state["loaded"] = True
                    stats[name]["source_reads"] += 1
                else:
                    stats[name]["cache_hits"] += 1
                return state["value"]

            setattr(obj, name, async_cached)
            return

        def sync_cached(*args, **kwargs):
            if not state["loaded"]:
                state["value"] = original(*args, **kwargs)
                state["loaded"] = True
                stats[name]["source_reads"] += 1
            else:
                stats[name]["cache_hits"] += 1
            return state["value"]

        setattr(obj, name, sync_cached)

    originals.append((final_export, "datetime", final_export.datetime))
    final_export.datetime = _FrozenDateTime
    originals.append((business, "datetime", business.datetime))
    business.datetime = _FrozenDateTime

    for obj, name in (
        (stocking, "load_catalog_from_feishu"),
        (stocking, "load_manual_mapping_catalog"),
        (final_export, "load_spu_manual_mapping_catalog"),
        (stocking, "load_forecast_skus"),
        (stocking, "_load_snapshot_rows"),
        (fabric_base, "get_fabric_params"),
        (fabric_base, "get_fabric_price_data"),
        (fabric_base, "get_primary_fabric_by_spu"),
        (fabric_base, "get_purchase_order_data"),
        (fabric_base, "get_system_forecast_data"),
        (color_system, "get_suggest_order_data_color"),
        (fabric_base, "get_forecast_order_data"),
        (color_system, "_effective_sku_quantities"),
        (fabric_base, "get_fabric_color_merge_mapping"),
        (fabric_base, "get_inventory_data"),
        (fabric_base, "get_inventory_by_fabric"),
    ):
        patch_once(obj, name)

    try:
        logger.info(
            "A/B 同源快照已冻结：%s；方案B与方案A将复用同一批只读输入",
            fixed_now.isoformat(timespec="seconds"),
        )
        yield stats
    finally:
        for obj, name, original in reversed(originals):
            setattr(obj, name, original)


async def run(
    output_dir: Path,
    manual_mapping_path: Path,
    spu_manual_mapping_path: Path,
    write_feishu: bool = False,
    feishu_table_name: str = business.DEFAULT_FEISHU_TABLE_NAME,
    scheme_a_history_table: str = business.SCHEME_A_HISTORY_TABLE,
) -> Path:
    with shared_ab_source_snapshot() as stats:
        output = await business.run(
            output_dir=output_dir,
            manual_mapping_path=manual_mapping_path,
            spu_manual_mapping_path=spu_manual_mapping_path,
            write_feishu=write_feishu,
            feishu_table_name=feishu_table_name,
            scheme_a_history_table=scheme_a_history_table,
        )

    duplicate_reads = {
        name: values
        for name, values in stats.items()
        if values["cache_hits"] > 0
    }
    logger.info(
        "A/B 同源快照完成：复用 %d 个数据入口；缓存命中明细=%s",
        len(duplicate_reads),
        duplicate_reads,
    )
    return output


def main() -> Path:
    parser = argparse.ArgumentParser(
        description="同一数据快照计算方案B正式面料预估，并保留方案A MySQL快照"
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
        help="历史四字段人工映射 CSV",
    )
    parser.add_argument(
        "--spu-manual-mapping",
        type=Path,
        default=DEFAULT_SPU_MANUAL_MAPPING_PATH,
        help="SPU级人工映射 CSV",
    )
    parser.add_argument(
        "--write-feishu",
        action="store_true",
        help="将方案B最终业务21列表全量覆盖写入飞书面料预估明细",
    )
    parser.add_argument(
        "--feishu-table-name",
        default=business.DEFAULT_FEISHU_TABLE_NAME,
    )
    parser.add_argument(
        "--scheme-a-history-table",
        default=business.SCHEME_A_HISTORY_TABLE,
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
        )
    )


if __name__ == "__main__":
    main()
