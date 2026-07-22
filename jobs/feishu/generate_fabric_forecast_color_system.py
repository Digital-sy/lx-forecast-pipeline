#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""颜色体系感知的面料预估表生成。

关键原则：
1. 建议下单量按 SPU + 颜色体系 + 颜色代码读取，不再退回 SPU 后重新混色。
2. A2023/B2024 相同裸颜色码分别汇总。
3. 面料库存无法唯一归属颜色体系时不重复分配，标记为“跨体系冲突”。
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from common import get_logger
from common.database import db_cursor
from jobs.feishu import generate_fabric_forecast as base
from jobs.feishu.color_system_resolver import ColorSystemResolver
from jobs.feishu.procurement_color_logic import add_months, future_months, largest_remainder_allocate

logger = get_logger("fabric_forecast_color_system")


def get_suggest_order_data_color(
    resolver: ColorSystemResolver,
    current_date: datetime | None = None,
) -> Dict[Tuple[str, str, str, str], int]:
    """返回 {(SPU, 颜色体系, 颜色代码, 月份): 建议下单量}，包含零值。"""
    current_date = current_date or datetime.now()
    month_order = [label for _, label in future_months(current_date, 4)]
    result: Dict[Tuple[str, str, str, str], int] = {}

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='建议下单量表'
        """)
        if not cursor.fetchone().get("cnt", 0):
            return result

        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='建议下单量表'
              AND COLUMN_NAME='颜色体系'
        """)
        has_system = bool(cursor.fetchone().get("cnt", 0))

        for month in month_order:
            column = f"{month}建议下单"
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='建议下单量表'
                  AND COLUMN_NAME=%s
            """, (column,))
            if not cursor.fetchone().get("cnt", 0):
                continue

            if has_system:
                cursor.execute(f"""
                    SELECT SPU, 颜色体系, 颜色缩写, SUM(`{column}`) AS qty
                    FROM `建议下单量表`
                    WHERE SPU IS NOT NULL AND SPU != ''
                    GROUP BY SPU, 颜色体系, 颜色缩写
                """)
                for row in cursor.fetchall():
                    spu = str(row.get("SPU") or "").strip().upper()
                    system = str(row.get("颜色体系") or "待定").strip() or "待定"
                    code = str(row.get("颜色缩写") or "UNKNOWN").strip().upper() or "UNKNOWN"
                    result[(spu, system, code, month)] = int(row.get("qty") or 0)
            else:
                cursor.execute(f"""
                    SELECT SPU, 颜色缩写, SUM(`{column}`) AS qty
                    FROM `建议下单量表`
                    WHERE SPU IS NOT NULL AND SPU != ''
                    GROUP BY SPU, 颜色缩写
                """)
                for row in cursor.fetchall():
                    spu = str(row.get("SPU") or "").strip().upper()
                    code = str(row.get("颜色缩写") or "UNKNOWN").strip().upper() or "UNKNOWN"
                    identity = resolver.resolve(f"{spu}-{code}-SIZE", spu)
                    result[(spu, identity.color_system, code, month)] = int(row.get("qty") or 0)

    logger.info(f"颜色体系建议下单量读取完成：{len(result)} 个颜色身份+月份")
    return result


def load_fabric_merge_maps() -> Tuple[
    Dict[Tuple[str, str, str], str],
    Dict[Tuple[str, str], str],
]:
    """优先读取带颜色体系的规则；同时保留旧版面料编号+颜色码规则。"""
    system_map: Dict[Tuple[str, str, str], str] = {}
    legacy_map: Dict[Tuple[str, str], str] = {}
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料颜色归并对照'
            """)
            if not cursor.fetchone().get("cnt", 0):
                return system_map, legacy_map
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料颜色归并对照'
                  AND COLUMN_NAME='颜色体系'
            """)
            has_system = bool(cursor.fetchone().get("cnt", 0))
            if has_system:
                cursor.execute("""
                    SELECT 面料编号, 颜色体系, 原始颜色缩写, 归并颜色缩写
                    FROM `面料颜色归并对照`
                    WHERE 面料编号!='' AND 颜色体系!=''
                      AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                      AND 是否启用=1
                """)
                for row in cursor.fetchall():
                    key = (
                        str(row.get("面料编号") or "").strip().upper(),
                        str(row.get("颜色体系") or "").strip(),
                        str(row.get("原始颜色缩写") or "").strip().upper(),
                    )
                    system_map[key] = str(row.get("归并颜色缩写") or "").strip().upper()

            cursor.execute("""
                SELECT 面料编号, 原始颜色缩写, 归并颜色缩写
                FROM `面料颜色归并对照`
                WHERE 面料编号!='' AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                  AND 是否启用=1
            """)
            for row in cursor.fetchall():
                key = (
                    str(row.get("面料编号") or "").strip().upper(),
                    str(row.get("原始颜色缩写") or "").strip().upper(),
                )
                legacy_map[key] = str(row.get("归并颜色缩写") or "").strip().upper()
    except Exception as exc:
        logger.warning(f"读取颜色体系面料归并规则失败: {exc}")
    return system_map, legacy_map


def resolve_merged_color(
    fabric_code: str,
    color_system: str,
    raw_color: str,
    system_map: Mapping[Tuple[str, str, str], str],
    legacy_map: Mapping[Tuple[str, str], str],
) -> str:
    fabric_code = (fabric_code or "").strip().upper()
    raw_color = (raw_color or "").strip().upper()
    return (
        system_map.get((fabric_code, color_system, raw_color))
        or legacy_map.get((fabric_code, raw_color))
        or raw_color
    )


def _effective_sku_quantities(
    resolver: ColorSystemResolver,
    system_forecast_data: Mapping[Tuple[str, str], int],
    suggest_data: Mapping[Tuple[str, str, str, str], int],
    current_date: datetime,
) -> Tuple[Dict[Tuple[str, int], int], Dict[Tuple[str, int], Any]]:
    """把颜色级建议下单量按尺码SKU预测占比分配，合计严格守恒。"""
    sku_month_qty: Dict[Tuple[str, int], int] = {}
    identities: Dict[Tuple[str, int], Any] = {}
    grouped: Dict[Tuple[str, str, str, int], Dict[str, int]] = defaultdict(dict)

    for (sku, stat_date), qty in system_forecast_data.items():
        if qty <= 0:
            continue
        try:
            stat_dt = datetime.strptime(str(stat_date)[:10], "%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        delta = None
        for current_delta in range(4):
            year, month = add_months(current_date.year, current_date.month, current_delta)
            if stat_dt.year == year and stat_dt.month == month:
                delta = current_delta
                break
        if delta is None:
            continue
        identity = resolver.resolve(sku)
        identities[(sku, delta)] = identity
        grouped[(identity.spu, identity.color_system, identity.color_code, delta)][sku] = int(qty)

    for (spu, system, code, delta), sku_weights in grouped.items():
        year, month = add_months(current_date.year, current_date.month, delta)
        month_label = f"{str(year)[2:]}年{month}月"
        suggest_key = (spu, system, code, month_label)
        if suggest_key in suggest_data:
            allocated = largest_remainder_allocate(suggest_data[suggest_key], sku_weights)
            for sku, qty in allocated.items():
                sku_month_qty[(sku, delta)] = qty
        else:
            for sku, qty in sku_weights.items():
                sku_month_qty[(sku, delta)] = int(qty)

    return sku_month_qty, identities


def generate_records(
    resolver: ColorSystemResolver,
    fabric_params: Dict[str, Dict[str, Any]],
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]],
    purchase_order_data: Dict[str, int],
    system_forecast_data: Dict[Tuple[str, str], int],
    suggest_data: Dict[Tuple[str, str, str, str], int],
    operation_forecast_data: Dict[Tuple[str, str], int],
    inventory_data: Dict[str, int],
    pending_data: Dict[str, int],
    inv_by_fabric: Dict[str, int],
    pend_by_fabric: Dict[str, int],
    color_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    current_date = datetime.now()
    primary_fabric_by_spu = base.get_primary_fabric_by_spu(fabric_usage)
    system_merge_map, legacy_merge_map = load_fabric_merge_maps()
    effective_qty, _ = _effective_sku_quantities(
        resolver, system_forecast_data, suggest_data, current_date
    )

    def empty_bucket() -> Dict[str, Any]:
        return {
            "purchase_m": 0.0,
            "sys_month_m": [0.0, 0.0, 0.0, 0.0],
            "op_month_m": [0.0, 0.0, 0.0, 0.0],
            "缺失SPU": set(),
        }

    total_agg: Dict[str, Dict[str, Any]] = defaultdict(empty_bucket)
    color_agg: Dict[Tuple[str, str, str], Dict[str, Any]] = defaultdict(empty_bucket)

    def add_usage(
        sku: str,
        qty: int,
        target: str,
        delta: int | None = None,
    ) -> None:
        if qty <= 0:
            return
        identity = resolver.resolve(sku)
        spu = identity.spu
        for (candidate_spu, fabric_name), usage_data in fabric_usage.items():
            if candidate_spu != spu or fabric_name not in fabric_params:
                continue
            meters, missing = base._calc_usage_meters(
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

            if primary_fabric_by_spu.get(spu) != fabric_name:
                continue
            fabric_code = str(fabric_params[fabric_name].get("面料编号") or "").strip().upper()
            merged_color = resolve_merged_color(
                fabric_code,
                identity.color_system,
                identity.color_code,
                system_merge_map,
                legacy_merge_map,
            )
            if not merged_color:
                continue
            color_bucket = color_agg[(fabric_name, identity.color_system, merged_color)]
            if target == "purchase":
                color_bucket["purchase_m"] += meters
            else:
                color_bucket[target][delta] += meters
            if missing:
                color_bucket["缺失SPU"].add(spu)

    for sku, qty in purchase_order_data.items():
        add_usage(sku, int(qty), "purchase")

    for (sku, delta), qty in effective_qty.items():
        add_usage(sku, int(qty), "sys_month_m", delta)

    for (sku, stat_date), qty in operation_forecast_data.items():
        if qty <= 0:
            continue
        try:
            stat_dt = datetime.strptime(str(stat_date)[:10], "%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        for delta in range(4):
            year, month = add_months(current_date.year, current_date.month, delta)
            if stat_dt.year == year and stat_dt.month == month:
                add_usage(sku, int(qty), "op_month_m", delta)
                break

    systems_by_inventory_key: Dict[str, set[str]] = defaultdict(set)
    for fabric_name, system, merged_color in color_agg:
        fabric_code = str(fabric_params.get(fabric_name, {}).get("面料编号") or "").strip().upper()
        if fabric_code and merged_color:
            systems_by_inventory_key[f"{fabric_code}-{merged_color}"].add(system)

    month_labels = [label for _, label in future_months(current_date, 4)]
    now = datetime.now()

    def build_record(
        fabric_name: str,
        color_system: str,
        color_code: str,
        bucket: Mapping[str, Any],
        stat_type: str,
    ) -> Dict[str, Any]:
        params = fabric_params.get(fabric_name, {})
        fabric_code = str(params.get("面料编号") or "").strip().upper()
        meters_per_roll = float(params.get("米数每条") or 0)

        if stat_type == "总量":
            inventory_rolls = int(inv_by_fabric.get(fabric_name, 0) or 0)
            pending_rolls = int(pend_by_fabric.get(fabric_name, 0) or 0)
            fabric_color_code = ""
            inventory_status = "总量库存"
        else:
            legacy_key = f"{fabric_code}-{color_code}" if fabric_code else ""
            collision = len(systems_by_inventory_key.get(legacy_key, set())) > 1
            if collision:
                inventory_rolls = 0
                pending_rolls = 0
                inventory_status = "跨颜色体系冲突，未自动分配"
            else:
                inventory_rolls = int(inventory_data.get(legacy_key, 0) or 0)
                pending_rolls = int(pending_data.get(legacy_key, 0) or 0)
                inventory_status = "颜色体系唯一，可匹配"
            fabric_color_code = (
                f"{fabric_code}-{color_system}-{color_code}"
                if fabric_code else f"{color_system}-{color_code}"
            )

        purchase_m = round(float(bucket["purchase_m"]), 2)
        system_months = [round(float(value), 2) for value in bucket["sys_month_m"]]
        operation_months = [round(float(value), 2) for value in bucket["op_month_m"]]

        return {
            "统计类型": stat_type,
            "SKU": "",
            "SPU": "",
            "面料": fabric_name,
            "面料编号": fabric_code,
            "颜色体系": color_system,
            "颜色缩写": color_code,
            "颜色汇总代码": f"{color_system}:{color_code}" if color_code else "",
            "颜色": color_map.get(color_code, ""),
            "面料颜色编号": fabric_color_code,
            "库存归属状态": inventory_status,
            "统计日期": now.date(),
            "月份": month_labels[0],
            "库存量/条": inventory_rolls,
            "库存量/米": round(inventory_rolls * meters_per_roll, 2),
            "待到货量/条": pending_rolls,
            "待到货量/米": round(pending_rolls * meters_per_roll, 2),
            "当月已下单消耗/米": purchase_m,
            "当月完整预估/米": system_months[0],
            "当月剩余预估/米": round(max(0.0, system_months[0] - purchase_m), 2),
            "当月月份": month_labels[0],
            "T+1月预估/米": system_months[1],
            "T+1月份": month_labels[1],
            "T+2月预估/米": system_months[2],
            "T+2月份": month_labels[2],
            "T+3月预估/米": system_months[3],
            "T+3月份": month_labels[3],
            "运营当月预估/米": operation_months[0],
            "运营T+1月预估/米": operation_months[1],
            "运营T+2月预估/米": operation_months[2],
            "运营T+3月预估/米": operation_months[3],
            "用量信息缺失SPU": ",".join(sorted(bucket["缺失SPU"])),
            "创建时间": now,
            "更新时间": now,
        }

    records: List[Dict[str, Any]] = []
    for fabric_name, bucket in total_agg.items():
        records.append(build_record(fabric_name, "", "", bucket, "总量"))
    for (fabric_name, system, color_code), bucket in color_agg.items():
        records.append(build_record(fabric_name, system, color_code, bucket, "带颜色"))

    logger.info(
        f"颜色体系面料预估生成完成：{len(records)} 条，"
        f"总量 {len(total_agg)}，带颜色 {len(color_agg)}"
    )
    return records


def _column_exists(cursor: Any, column: str) -> bool:
    cursor.execute("""
        SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料预估表' AND COLUMN_NAME=%s
    """, (column,))
    return bool(cursor.fetchone().get("cnt", 0))


def _index_exists(cursor: Any, index_name: str) -> bool:
    cursor.execute("""
        SELECT COUNT(*) AS cnt FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料预估表' AND INDEX_NAME=%s
    """, (index_name,))
    return bool(cursor.fetchone().get("cnt", 0))


def create_or_migrate_table() -> None:
    base.create_or_migrate_table()
    with db_cursor() as cursor:
        additions = [
            ("颜色体系", "VARCHAR(30) NOT NULL DEFAULT '待定' AFTER `面料编号`"),
            ("颜色汇总代码", "VARCHAR(150) NOT NULL DEFAULT '' AFTER `颜色缩写`"),
            ("库存归属状态", "VARCHAR(100) NOT NULL DEFAULT '' AFTER `面料颜色编号`"),
        ]
        for column, definition in additions:
            if not _column_exists(cursor, column):
                cursor.execute(f"ALTER TABLE `面料预估表` ADD COLUMN `{column}` {definition}")

        if _index_exists(cursor, "uk_type_fabric_color"):
            cursor.execute("ALTER TABLE `面料预估表` DROP INDEX `uk_type_fabric_color`")
        if not _index_exists(cursor, "uk_type_fabric_system_color"):
            cursor.execute("""
                ALTER TABLE `面料预估表`
                ADD UNIQUE KEY `uk_type_fabric_system_color`
                (`统计类型`(20), `面料`(100), `颜色体系`(20), `颜色缩写`(50))
            """)


def save_records(records: Sequence[Mapping[str, Any]]) -> None:
    if not records:
        logger.warning("无颜色体系面料预估数据需写入")
        return
    with db_cursor() as cursor:
        cursor.execute("DELETE FROM `面料预估表`")
        sql = """
            INSERT INTO `面料预估表` (
                `统计类型`, `SKU`, `SPU`, `面料`, `面料编号`, `颜色体系`,
                `颜色缩写`, `颜色汇总代码`, `颜色`, `面料颜色编号`, `库存归属状态`,
                `统计日期`, `月份`, `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`, `当月月份`,
                `T+1月预估/米`, `T+1月份`, `T+2月预估/米`, `T+2月份`,
                `T+3月预估/米`, `T+3月份`, `运营当月预估/米`, `运营T+1月预估/米`,
                `运营T+2月预估/米`, `运营T+3月预估/米`, `用量信息缺失SPU`,
                `创建时间`, `更新时间`
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """
        rows = [(
            row["统计类型"], row["SKU"], row["SPU"], row["面料"], row["面料编号"],
            row["颜色体系"], row["颜色缩写"], row["颜色汇总代码"], row["颜色"],
            row["面料颜色编号"], row["库存归属状态"], row["统计日期"], row["月份"],
            row["库存量/条"], row["库存量/米"], row["待到货量/条"], row["待到货量/米"],
            row["当月已下单消耗/米"], row["当月完整预估/米"], row["当月剩余预估/米"],
            row["当月月份"], row["T+1月预估/米"], row["T+1月份"], row["T+2月预估/米"],
            row["T+2月份"], row["T+3月预估/米"], row["T+3月份"],
            row["运营当月预估/米"], row["运营T+1月预估/米"], row["运营T+2月预估/米"],
            row["运营T+3月预估/米"], row["用量信息缺失SPU"], row["创建时间"], row["更新时间"],
        ) for row in records]
        for index in range(0, len(rows), 200):
            cursor.executemany(sql, rows[index:index + 200])
    logger.info(f"✓ 写入 {len(records)} 条颜色体系面料预估记录")


def main(resolver: ColorSystemResolver | None = None) -> List[Dict[str, Any]]:
    logger.info("=" * 80)
    logger.info("面料预估生成（颜色体系感知版）")
    logger.info("=" * 80)
    resolver = resolver or ColorSystemResolver.from_database()
    create_or_migrate_table()

    fabric_params = base.get_fabric_params()
    if not fabric_params:
        raise RuntimeError("定制面料参数为空")
    fabric_usage = base.get_fabric_price_data()
    purchase_order_data = base.get_purchase_order_data()
    system_forecast_data = base.get_system_forecast_data()
    suggest_data = get_suggest_order_data_color(resolver)
    operation_forecast_data = base.get_forecast_order_data()
    legacy_merge_map = base.get_fabric_color_merge_mapping()
    color_map = base.get_color_map()
    inventory_data, pending_data = base.get_inventory_data(legacy_merge_map)
    inv_by_fabric, pend_by_fabric = base.get_inventory_by_fabric(
        inventory_data, pending_data, fabric_params
    )

    if not system_forecast_data and not purchase_order_data and not operation_forecast_data:
        raise RuntimeError("面料预估所有数据源均为空")

    records = generate_records(
        resolver=resolver,
        fabric_params=fabric_params,
        fabric_usage=fabric_usage,
        purchase_order_data=purchase_order_data,
        system_forecast_data=system_forecast_data,
        suggest_data=suggest_data,
        operation_forecast_data=operation_forecast_data,
        inventory_data=inventory_data,
        pending_data=pending_data,
        inv_by_fabric=inv_by_fabric,
        pend_by_fabric=pend_by_fabric,
        color_map=color_map,
    )
    save_records(records)
    return records


if __name__ == "__main__":
    main()
