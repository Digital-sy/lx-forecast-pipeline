#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""方案 A/B 同源快照入口。

正式业务仍由 ``export_fabric_color_order_forecast_business`` 负责：
- 方案 B：写 Excel + 飞书业务 21 列；
- 方案 A：仅保留 MySQL 每日快照。

本入口解决两类生产一致性问题：
1. A/B 两次计算复用同一批只读源数据，并冻结运行时间；
2. 兼容飞书数字字段实际使用 ``property.formatter`` 表示小数位的 API 语义，
   并在 B 写入后回读飞书，逐行核对所有数字字段，防止“写入成功但精度丢失”。

缓存和飞书兼容补丁仅在本次 ``run`` 上下文内有效，结束后恢复原函数，
不影响仓库里其他飞书任务。
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import os
from contextlib import contextmanager
from datetime import datetime as real_datetime
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import httpx

from common import get_logger
from common.feishu import FeishuClient
from jobs.feishu import export_fabric_color_order_forecast_business as business
from jobs.feishu import export_fabric_color_order_forecast_final as final_export
from jobs.feishu import fabric_color_stocking as stocking
from jobs.feishu import generate_fabric_forecast as fabric_base
from jobs.feishu import generate_fabric_forecast_color_system as color_system
from jobs.feishu import generate_procurement_report as procurement_base
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


def _formatter_for_precision(precision: int) -> str:
    """飞书数字字段用 formatter 而不是 precision 表示小数位。"""
    digits = max(0, int(precision or 0))
    return "0" if digits == 0 else "0." + ("0" * digits)


def _precision_from_formatter(formatter: Any) -> int:
    """从 0 / 0.00 / 1,000.00 / ¥0.00 / 0.00% 推导显示小数位。"""
    value = str(formatter or "0").strip().replace(",", "")
    if value.endswith("%"):
        value = value[:-1]
    if "." not in value:
        return 0
    fractional = value.rsplit(".", 1)[1]
    return len(fractional)


def _field_with_compat_precision(field_info: Mapping[str, Any] | None) -> dict[str, Any]:
    """给旧公共封装补一个“推导 precision”，仅供本入口比较字段精度。"""
    if not field_info:
        return {}
    result = dict(field_info)
    if result.get("type") in (2, 1002):
        prop = dict(result.get("property") or {})
        prop["precision"] = _precision_from_formatter(prop.get("formatter"))
        result["property"] = prop
    return result


@contextmanager
def feishu_number_precision_compat() -> Iterator[None]:
    """局部修复 FeishuClient 的数字精度识别/更新，不修改公共模块全局行为。"""
    original_get_field_info = FeishuClient.get_field_info
    original_get_field_info_by_id = FeishuClient.get_field_info_by_id
    original_update_field = FeishuClient.update_field

    async def get_field_info_compat(self: FeishuClient, field_name: str) -> dict[str, Any]:
        info = await original_get_field_info(self, field_name)
        return _field_with_compat_precision(info)

    async def get_field_info_by_id_compat(self: FeishuClient, field_id: str) -> dict[str, Any]:
        info = await original_get_field_info_by_id(self, field_id)
        return _field_with_compat_precision(info)

    async def update_field_compat(
        self: FeishuClient,
        field_id: str,
        field_name: str | None = None,
        field_type: str | None = None,
        precision: int | None = None,
    ) -> bool:
        # 非数字精度更新仍完全走公共封装原逻辑，例如主字段重命名。
        if precision is None:
            return await original_update_field(
                self,
                field_id,
                field_name=field_name,
                field_type=field_type,
                precision=precision,
            )

        raw_info = await original_get_field_info_by_id(self, field_id)
        if not raw_info or raw_info.get("type") not in (2, 1002):
            logger.warning("字段 %s 不是普通数字字段，拒绝按小数位更新", field_id)
            return False

        existing_ui_type = str(raw_info.get("ui_type") or "Number")
        if existing_ui_type not in {"", "Number"}:
            logger.warning(
                "字段 %s 的 ui_type=%s，不属于普通数字字段，避免覆盖特殊属性",
                field_id,
                existing_ui_type,
            )
            return False

        if not self._access_token:
            await self.get_access_token()

        target_name = field_name or str(raw_info.get("field_name") or "")
        if not target_name:
            logger.warning("字段 %s 缺少 field_name，无法安全执行全量字段更新", field_id)
            return False

        if field_type and str(field_type).isdigit():
            target_type = int(field_type)
        else:
            target_type = int(raw_info.get("type") or 2)

        formatter = _formatter_for_precision(precision)
        data = {
            "field_name": target_name,
            "type": target_type,
            "property": {"formatter": formatter},
        }
        url = (
            f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/"
            f"{self.table_id}/fields/{field_id}"
        )
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        timeout = httpx.Timeout(60.0, connect=10.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.put(url, headers=headers, json=data)
                result = response.json()
        except Exception as exc:
            logger.warning("更新飞书数字字段 %s 异常: %s", target_name, exc)
            return False

        if result.get("code") != 0:
            logger.warning(
                "更新飞书数字字段 %s 失败 [code: %s]: %s；请求formatter=%s",
                target_name,
                result.get("code"),
                result.get("msg"),
                formatter,
            )
            return False

        verify_info = await original_get_field_info_by_id(self, field_id)
        actual_formatter = str((verify_info.get("property") or {}).get("formatter") or "")
        if actual_formatter != formatter:
            logger.warning(
                "飞书数字字段 %s 更新后formatter不一致：期望=%s，实际=%s",
                target_name,
                formatter,
                actual_formatter,
            )
            return False

        logger.info("✓ 飞书数字字段 %s 显示格式已更新为 %s", target_name, formatter)
        return True

    FeishuClient.get_field_info = get_field_info_compat
    FeishuClient.get_field_info_by_id = get_field_info_by_id_compat
    FeishuClient.update_field = update_field_compat
    try:
        yield
    finally:
        FeishuClient.get_field_info = original_get_field_info
        FeishuClient.get_field_info_by_id = original_get_field_info_by_id
        FeishuClient.update_field = original_update_field


def _text_value(value: Any) -> str:
    """兼容飞书文本字段可能返回字符串或富文本数组。"""
    if value is None:
        return ""
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                text = item.get("text") or item.get("name") or item.get("value") or ""
                parts.append(str(text))
            else:
                parts.append(str(item))
        return "".join(parts)
    if isinstance(value, Mapping):
        return str(value.get("text") or value.get("name") or value.get("value") or "")
    return str(value)


def _row_identity(row: Mapping[str, Any]) -> tuple[str, str, str, str, str, str]:
    """业务21列表的稳定行键：总量/带颜色均可唯一定位。"""
    return (
        _text_value(row.get("统计类型")),
        _text_value(row.get("面料")),
        _text_value(row.get("颜色")),
        _text_value(row.get("面料颜色编号")),
        _text_value(row.get("面料编号")),
        _text_value(row.get("颜色缩写")),
    )


async def verify_business_rows_in_feishu(
    headers: Sequence[str],
    business_rows: Sequence[Mapping[str, Any]],
    table_name: str,
) -> None:
    """写后回读：核对行数、数字字段 formatter 和每一行数字值。"""
    locator = FeishuClient(
        app_token=procurement_base.FEISHU_APP_TOKEN,
        table_id="",
    )
    tables = await locator.get_tables()
    table_id = tables.get(table_name)
    if not table_id:
        raise RuntimeError(f"飞书写后校验失败：找不到数据表 {table_name!r}")

    client = FeishuClient(
        app_token=procurement_base.FEISHU_APP_TOKEN,
        table_id=table_id,
    )
    number_headers = [
        header
        for header in headers
        if header.endswith("/米") or header.endswith("/条")
    ]

    fields = await client.get_ordered_fields()
    field_by_name = {str(field.get("field_name") or ""): field for field in fields}
    wrong_formatters: list[str] = []
    for header in number_headers:
        info = field_by_name.get(header) or {}
        formatter = str((info.get("property") or {}).get("formatter") or "")
        if info.get("type") not in (2, 1002) or formatter != "0.00":
            wrong_formatters.append(f"{header}:type={info.get('type')},formatter={formatter!r}")
    if wrong_formatters:
        raise RuntimeError(
            "飞书写后校验失败：数字字段未统一为2位小数：" + "; ".join(wrong_formatters)
        )

    records = await client.read_records(page_size=500)
    if len(records) != len(business_rows):
        raise RuntimeError(
            f"飞书写后校验失败：期望 {len(business_rows)} 行，实际读取 {len(records)} 行"
        )

    expected_by_key: dict[tuple[str, str, str, str, str, str], Mapping[str, Any]] = {}
    for row in business_rows:
        key = _row_identity(row)
        if key in expected_by_key:
            raise RuntimeError(f"飞书写后校验无法建立唯一键，业务结果存在重复行: {key}")
        expected_by_key[key] = row

    actual_by_key: dict[tuple[str, str, str, str, str, str], Mapping[str, Any]] = {}
    for record in records:
        row = record.get("fields") or {}
        key = _row_identity(row)
        if key in actual_by_key:
            raise RuntimeError(f"飞书写后校验发现重复行: {key}")
        actual_by_key[key] = row

    missing_keys = sorted(set(expected_by_key) - set(actual_by_key))
    extra_keys = sorted(set(actual_by_key) - set(expected_by_key))
    if missing_keys or extra_keys:
        raise RuntimeError(
            "飞书写后校验失败：行键不一致；"
            f"缺失={missing_keys[:5]}，多出={extra_keys[:5]}"
        )

    mismatches: list[str] = []
    for key, expected in expected_by_key.items():
        actual = actual_by_key[key]
        for header in number_headers:
            expected_value = float(expected.get(header) or 0)
            try:
                actual_value = float(actual.get(header) or 0)
            except (TypeError, ValueError):
                mismatches.append(f"{key} | {header}: 实际值={actual.get(header)!r}")
                continue
            if abs(expected_value - actual_value) > 0.005:
                mismatches.append(
                    f"{key} | {header}: 期望={expected_value:.2f}, 实际={actual_value:.6f}"
                )
            if len(mismatches) >= 20:
                break
        if len(mismatches) >= 20:
            break

    if mismatches:
        raise RuntimeError(
            "飞书写后校验失败：数字值出现差异（最多展示20项）：" + "; ".join(mismatches)
        )

    logger.info(
        "✓ 飞书%s写后回读校验通过：%d/%d 行，%d 个数字字段均为2位小数且数值一致",
        table_name,
        len(records),
        len(business_rows),
        len(number_headers),
    )


@contextmanager
def verified_feishu_business_writer() -> Iterator[None]:
    """仅在本入口中给方案B飞书写入增加强制回读校验。"""
    original_writer = business.write_business_rows_to_feishu

    async def verified_writer(
        headers: Sequence[str],
        business_rows: Sequence[Mapping[str, Any]],
        table_name: str = business.DEFAULT_FEISHU_TABLE_NAME,
    ) -> int:
        written = await original_writer(headers, business_rows, table_name=table_name)
        await verify_business_rows_in_feishu(headers, business_rows, table_name)
        return written

    business.write_business_rows_to_feishu = verified_writer
    try:
        yield
    finally:
        business.write_business_rows_to_feishu = original_writer


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
    # 兼容修复、写后校验、A/B同源快照都只作用于本次正式入口。
    with feishu_number_precision_compat():
        with verified_feishu_business_writer():
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
