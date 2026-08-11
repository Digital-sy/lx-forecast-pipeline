#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""货件箱明细增量同步入口。

复用原有 ``Shipment_Number.py`` 的货件列表、箱明细加工和飞书写入逻辑，
但把高耗时的箱明细接口改为“按需抓取”：

1. 每次都调用货件列表接口，刷新货件状态、时间、地址等货件级信息；
2. 飞书中已有该货件箱记录时，默认直接复用，不再调用箱明细接口；
3. 飞书没有该货件历史箱记录时，调用一次箱明细接口建立缓存；
4. WORKING / READY_TO_SHIP 状态若 gmt_modified 发生变化，补抓一次箱明细；
5. SHIPPED 及之后的状态只刷新货件列表数据，不再重复读取箱结构；
6. 最终仍全量刷新飞书，避免缓存货件记录丢失。
"""
from __future__ import annotations

import asyncio
import copy
from collections import Counter, defaultdict
from datetime import datetime
from types import SimpleNamespace
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set

from jobs.feishu import Shipment_Number as legacy


# 这两个状态下仍可能修改装箱结构；仅在 gmt_modified 比缓存更新时补抓。
BOX_MUTABLE_STATUSES: Set[str] = {
    "WORKING",
    "READY_TO_SHIP",
}

DATE_FIELDS = {
    "创建时间",
    "修改时间",
    "工作时间",
    "发货时间",
    "接收时间",
    "关闭时间",
    "同步时间",
    "预计送达开始",
    "预计送达结束",
}

NUMERIC_FIELDS = {
    "单箱数量",
    "单箱总数",
    "箱子长度cm",
    "箱子宽度cm",
    "箱子高度cm",
    "箱体积m3",
    "箱子重量kg",
    "箱子体积重kg",
}

SHIPMENT_LEVEL_FIELDS = {
    "店铺",
    "货件单号",
    "货件名称",
    "货件状态",
    "创建时间",
    "修改时间",
    "工作时间",
    "发货时间",
    "接收时间",
    "关闭时间",
    "同步时间",
    "预计送达开始",
    "预计送达结束",
    "目的仓库",
    "运输方式",
    "发货国家",
    "发货城市",
    "收货国家",
    "收货城市",
    "收货仓库代码",
}


class IncrementalState:
    """保存当前运行中识别到的货件、飞书缓存和增量统计。"""

    def __init__(self) -> None:
        self.rows_by_shipment: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.shipment_by_sta_id: Dict[str, Dict[str, Any]] = {}
        self.cached_in_progress: Set[str] = set()
        self.status_counts: Counter[str] = Counter()

        self.reused_shipments = 0
        self.reused_rows = 0
        self.cache_misses = 0
        self.mutable_refreshes = 0
        self.skip_next_throttle_sleep = False
        self.skipped_throttle_sleeps = 0


STATE = IncrementalState()
ORIGINAL_REQUEST = legacy.OpenApiBase.request
ORIGINAL_PROCESS_BOX_INFO = legacy.process_box_info
ORIGINAL_ASYNCIO_MODULE = legacy.asyncio


class _AsyncioProxy:
    """只跳过缓存命中后原脚本固定的2秒箱接口节流等待。"""

    def __getattr__(self, name: str) -> Any:
        return getattr(ORIGINAL_ASYNCIO_MODULE, name)

    async def sleep(self, delay: float, *args: Any, **kwargs: Any) -> Any:
        if STATE.skip_next_throttle_sleep and float(delay) == 2.0:
            STATE.skip_next_throttle_sleep = False
            STATE.skipped_throttle_sleeps += 1
            return None
        return await ORIGINAL_ASYNCIO_MODULE.sleep(delay, *args, **kwargs)


ASYNCIO_PROXY = _AsyncioProxy()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link"):
            result = _text(value.get(key))
            if result:
                return result
        return ""
    if isinstance(value, list):
        return "".join(_text(item) for item in value).strip()
    return str(value).strip()


def _hyperlink(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("link") or value.get("url"))
    if isinstance(value, list):
        for item in value:
            result = _hyperlink(item)
            if result:
                return result
        return ""
    return _text(value)


def _date_text(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        stamp = float(value)
        if stamp > 10_000_000_000:
            stamp /= 1000
        try:
            return datetime.fromtimestamp(stamp).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, OverflowError, ValueError):
            return ""
    return _text(value)


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = _date_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _number(value: Any, default: float = 0) -> Any:
    if value in (None, ""):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return int(number) if number.is_integer() else number


def _normalize_existing_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
    """把飞书原始字段转换成原脚本 processed_data 的结构。"""
    normalized: Dict[str, Any] = {}
    for name, value in fields.items():
        if name == "图片链接":
            normalized[name] = _hyperlink(value)
        elif name in DATE_FIELDS:
            normalized[name] = _date_text(value)
        elif name in NUMERIC_FIELDS:
            normalized[name] = _number(value)
        else:
            normalized[name] = _text(value)
    return normalized


def _shipment_status(item: Dict[str, Any]) -> str:
    value = (
        item.get("shipment_status")
        or item.get("shipmentStatus")
        or item.get("status")
        or item.get("shipment_state")
        or item.get("shipmentState")
        or ""
    )
    return _text(value)


def _shipment_modified(item: Dict[str, Any]) -> Optional[datetime]:
    value = (
        item.get("gmt_modified")
        or item.get("update_time")
        or item.get("updated_at")
        or item.get("gmtModified")
        or item.get("updateTime")
        or item.get("updatedAt")
    )
    return _parse_datetime(value)


def _cached_modified(cached_rows: List[Dict[str, Any]]) -> Optional[datetime]:
    values = [
        _parse_datetime(row.get("修改时间"))
        for row in cached_rows
        if isinstance(row, dict)
    ]
    valid = [value for value in values if value is not None]
    return max(valid) if valid else None


def _should_reuse_cache(
    shipment: Dict[str, Any],
    cached_rows: List[Dict[str, Any]],
) -> bool:
    """判断是否可直接复用历史箱记录。"""
    if not cached_rows:
        return False

    status = _shipment_status(shipment).upper()

    # 发货前可能继续改箱；仅在列表修改时间比缓存更新时补抓。
    if status in BOX_MUTABLE_STATUSES:
        current_modified = _shipment_modified(shipment)
        previous_modified = _cached_modified(cached_rows)

        # 缺少可比较时间时采用保守策略，重新抓取箱明细。
        if current_modified is None or previous_modified is None:
            return False

        if current_modified > previous_modified:
            return False

    # SHIPPED及之后、CANCELLED/CLOSED等状态，只刷新列表数据，箱结构永久复用。
    return True


def _shipment_fields(item: Dict[str, Any], seller_name: str) -> Dict[str, Any]:
    create_time = (
        item.get("gmt_create")
        or item.get("create_time")
        or item.get("created_at")
        or item.get("create_date")
        or item.get("gmtCreate")
        or item.get("createTime")
        or item.get("createdAt")
        or ""
    )
    return {
        "店铺": seller_name,
        "货件单号": _text(item.get("shipment_id")),
        "货件名称": _text(item.get("shipment_name")),
        "货件状态": _shipment_status(item),
        "创建时间": _date_text(create_time),
        "修改时间": _date_text(
            item.get("gmt_modified")
            or item.get("update_time")
            or item.get("updated_at")
            or item.get("gmtModified")
            or item.get("updateTime")
            or item.get("updatedAt")
        ),
        "工作时间": _date_text(item.get("working_time")),
        "发货时间": _date_text(item.get("shipped_time")),
        "接收时间": _date_text(item.get("receiving_time")),
        "关闭时间": _date_text(item.get("closed_time")),
        "同步时间": _date_text(item.get("sync_time")),
        "预计送达开始": _date_text(item.get("sta_delivery_start_date")),
        "预计送达结束": _date_text(item.get("sta_delivery_end_date")),
        "目的仓库": _text(item.get("destination_fulfillment_center_id")),
        "运输方式": _text(item.get("shipping_mode")),
        "发货国家": _text((item.get("ship_from_address") or {}).get("country_code")),
        "发货城市": _text((item.get("ship_from_address") or {}).get("city")),
        "收货国家": _text((item.get("ship_to_address") or {}).get("country_code")),
        "收货城市": _text((item.get("ship_to_address") or {}).get("city")),
        "收货仓库代码": _text((item.get("ship_to_address") or {}).get("name")),
    }


def _register_shipments(items: Iterable[Dict[str, Any]]) -> None:
    for item in items:
        if not isinstance(item, dict):
            continue
        sta_id = _text(item.get("sta_shipment_id") or item.get("staShipmentId"))
        status = _shipment_status(item) or "<空>"
        STATE.status_counts[status] += 1
        if sta_id:
            STATE.shipment_by_sta_id[sta_id] = item


def _response_shipments(response: Any) -> List[Dict[str, Any]]:
    if getattr(response, "code", None) != 0:
        return []
    data = getattr(response, "data", None)
    if not isinstance(data, dict):
        return []
    rows = data.get("list") or data.get("data") or []
    return rows if isinstance(rows, list) else []


def _cached_response(row_count: int) -> SimpleNamespace:
    """生成原脚本可识别的占位箱响应，实际行由缓存包装器返回。"""
    packing_list = [
        {
            "boxId": f"CACHE-{index + 1}",
            "localBoxId": str(index + 1),
            "length": 0,
            "width": 0,
            "height": 0,
            "weight": 0,
            "lengthUnit": "cm",
            "weightUnit": "kg",
            "total": 0,
            "productList": [],
        }
        for index in range(max(row_count, 1))
    ]
    return SimpleNamespace(
        code=0,
        data={"shipmentList": [{"shipmentPackingList": packing_list}]},
        message="incremental cache hit",
        error_details=None,
        request_id="incremental-cache",
    )


async def _load_existing_feishu_rows() -> None:
    client = legacy.FeishuClient(
        app_token=legacy.FEISHU_APP_TOKEN,
        table_id=legacy.FEISHU_TABLE_ID,
    )
    try:
        records = await client.read_records()
    except Exception as exc:
        # 缓存不可用时安全降级为原来的全量抓取。
        print(f"⚠️  读取飞书历史货件失败，将按原逻辑全量抓取箱明细: {exc}")
        return

    invalid = 0
    for record in records:
        fields = record.get("fields") or {}
        if not isinstance(fields, dict):
            invalid += 1
            continue
        normalized = _normalize_existing_fields(fields)
        shipment_id = _text(normalized.get("货件单号"))
        if not shipment_id:
            invalid += 1
            continue
        STATE.rows_by_shipment[shipment_id].append(normalized)

    print(
        "✅ 已读取飞书历史货件缓存: "
        f"{len(records)} 条记录 / {len(STATE.rows_by_shipment)} 个货件"
    )
    if invalid:
        print(f"⚠️  缓存中有 {invalid} 条记录缺少有效货件单号，已忽略")


async def _request_with_incremental_cache(
    self: Any,
    access_token: str,
    path: str,
    method: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    req_body = kwargs.get("req_body")
    if req_body is None and args:
        req_body = args[0]

    if path.endswith("/listShipmentBoxes") and isinstance(req_body, dict):
        sta_ids = req_body.get("shipmentIdList") or []
        sta_id = _text(sta_ids[0]) if sta_ids else ""
        shipment = STATE.shipment_by_sta_id.get(sta_id)
        shipment_id = _text((shipment or {}).get("shipment_id"))
        cached_rows = STATE.rows_by_shipment.get(shipment_id, [])

        if shipment and _should_reuse_cache(shipment, cached_rows):
            STATE.cached_in_progress.add(shipment_id)
            STATE.reused_shipments += 1
            STATE.reused_rows += len(cached_rows)
            STATE.skip_next_throttle_sleep = True
            print(
                "  ♻️  复用飞书历史箱记录，不请求箱明细接口: "
                f"status={_shipment_status(shipment)}, rows={len(cached_rows)}"
            )
            return _cached_response(len(cached_rows))

        if shipment and cached_rows:
            STATE.mutable_refreshes += 1
            print(
                "  🔄 发货前货件有更新，重新抓取箱明细: "
                f"status={_shipment_status(shipment)}, "
                f"current_modified={_date_text(_shipment_modified(shipment))}, "
                f"cached_modified={_date_text(_cached_modified(cached_rows))}"
            )
        else:
            STATE.cache_misses += 1
            print("  🆕 飞书无历史箱记录，本次抓取箱明细并建立缓存")

    response = await ORIGINAL_REQUEST(
        self,
        access_token,
        path,
        method,
        *args,
        **kwargs,
    )

    if path.endswith("/shipmentList"):
        _register_shipments(_response_shipments(response))

    return response


def _process_box_info_with_cache(
    box_data: Any,
    shipment_id: str,
    sid: Any,
    seller_name: str = "",
    shipment_info: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    if shipment_id in STATE.cached_in_progress:
        STATE.cached_in_progress.discard(shipment_id)
        cached_rows = STATE.rows_by_shipment.get(shipment_id, [])
        refreshed_fields = _shipment_fields(shipment_info or {}, seller_name)
        result: List[Dict[str, Any]] = []

        for cached in cached_rows:
            row = copy.deepcopy(cached)
            # 每次用货件列表的最新值刷新货件级字段，箱子和商品字段保持历史快照。
            for field in SHIPMENT_LEVEL_FIELDS:
                value = refreshed_fields.get(field)
                if value not in (None, ""):
                    row[field] = value
            row["货件单号"] = shipment_id
            result.append(row)
        return result

    return ORIGINAL_PROCESS_BOX_INFO(
        box_data,
        shipment_id,
        sid,
        seller_name,
        shipment_info,
    )


async def main() -> None:
    await _load_existing_feishu_rows()

    legacy.OpenApiBase.request = _request_with_incremental_cache
    legacy.process_box_info = _process_box_info_with_cache
    legacy.asyncio = ASYNCIO_PROXY
    try:
        await legacy.main()
    finally:
        legacy.OpenApiBase.request = ORIGINAL_REQUEST
        legacy.process_box_info = ORIGINAL_PROCESS_BOX_INFO
        legacy.asyncio = ORIGINAL_ASYNCIO_MODULE

    print("\n" + "=" * 60)
    print("📈 货件箱明细按需抓取统计")
    print(f"  ♻️  复用已有货件箱缓存: {STATE.reused_shipments} 个")
    print(f"  📦 复用历史箱记录: {STATE.reused_rows} 条")
    print(f"  🆕 无缓存首次抓取: {STATE.cache_misses} 个")
    print(f"  🔄 发货前修改后补抓: {STATE.mutable_refreshes} 个")
    print(f"  ⏭️  跳过固定2秒等待: {STATE.skipped_throttle_sleeps} 次")
    if STATE.status_counts:
        summary = "，".join(
            f"{status}={count}"
            for status, count in STATE.status_counts.most_common()
        )
        print(f"  📊 最近30天货件状态: {summary}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
