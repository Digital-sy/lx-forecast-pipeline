#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""颜色体系感知的建议下单量与面料汇总逻辑。"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from jobs.feishu.color_system_resolver import ColorSystemResolver

COVERAGE_MONTHS_CUSTOM = 3
COVERAGE_MONTHS_STOCK = 2
TABLE_ORDER_SUGGEST = "建议下单量表"
TABLE_FABRIC_USAGE = "面料预计用量表"

ProcurementKey = Tuple[str, str, str, str]  # SPU, 颜色体系, 颜色代码, 店铺


def add_months(year: int, month: int, delta: int) -> Tuple[int, int]:
    month += delta
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return year, month


def future_months(current_date: datetime | date, count: int = 4) -> List[Tuple[str, str]]:
    output: List[Tuple[str, str]] = []
    for delta in range(count):
        year, month = add_months(current_date.year, current_date.month, delta)
        output.append((f"{year}-{month:02d}-01", f"{str(year)[2:]}年{month}月"))
    return output


def largest_remainder_allocate(total: int, weights: Mapping[str, float]) -> Dict[str, int]:
    """整数分摊并保证分摊合计严格等于 total。"""
    total = max(0, int(total or 0))
    keys = list(weights)
    if not keys:
        return {}
    positive = {key: max(0.0, float(weights.get(key, 0) or 0)) for key in keys}
    weight_sum = sum(positive.values())
    if total == 0:
        return {key: 0 for key in keys}
    if weight_sum <= 0:
        return {key: (total if index == 0 else 0) for index, key in enumerate(keys)}

    raw = {key: total * value / weight_sum for key, value in positive.items()}
    result = {key: int(value) for key, value in raw.items()}
    remainder = total - sum(result.values())
    order = sorted(keys, key=lambda key: (raw[key] - result[key], positive[key], key), reverse=True)
    for key in order[:remainder]:
        result[key] += 1
    return result


def read_system_forecast(
    resolver: ColorSystemResolver,
    current_date: datetime | None = None,
) -> Tuple[Dict[ProcurementKey, Dict[str, int]], List[str]]:
    from common.database import db_cursor

    current_date = current_date or datetime.now()
    month_pairs = future_months(current_date, 4)
    month_order = [label for _, label in month_pairs]
    start_date = month_pairs[0][0]
    end_year, end_month = add_months(current_date.year, current_date.month, 4)
    end_date = f"{end_year}-{end_month:02d}-01"

    result: Dict[ProcurementKey, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT SKU, SPU, 店铺, 月份, 统计日期, SUM(系统预测销量) AS 总量
            FROM `预测对比表_SKU`
            WHERE SKU IS NOT NULL AND SKU != ''
              AND 店铺 IS NOT NULL AND 店铺 != ''
              AND 统计日期 >= %s AND 统计日期 < %s
              AND 系统预测销量 > 0
            GROUP BY SKU, SPU, 店铺, 月份, 统计日期
        """, (start_date, end_date))
        rows = cursor.fetchall()

    valid_labels = set(month_order)
    for row in rows:
        sku = str(row.get("SKU") or "").strip()
        shop = str(row.get("店铺") or "").strip()
        month = str(row.get("月份") or "").strip()
        if not sku or not shop or month not in valid_labels:
            continue
        identity = resolver.resolve(sku, str(row.get("SPU") or ""))
        key: ProcurementKey = (
            identity.spu,
            identity.color_system,
            identity.color_code,
            shop,
        )
        result[key][month] += int(row.get("总量") or 0)

    return {key: dict(value) for key, value in result.items()}, month_order


def read_inventory(resolver: ColorSystemResolver) -> Dict[ProcurementKey, Dict[str, int]]:
    from common.database import db_cursor

    result: Dict[ProcurementKey, Dict[str, int]] = defaultdict(lambda: {"库存": 0, "待到货": 0})

    def add_rows(rows: Iterable[Mapping[str, Any]], stock_field: str, pending_field: str) -> None:
        for row in rows:
            sku = str(row.get("SKU") or "").strip()
            shop = str(row.get("店铺") or "").strip()
            if not sku or not shop:
                continue
            identity = resolver.resolve(sku)
            key: ProcurementKey = (
                identity.spu,
                identity.color_system,
                identity.color_code,
                shop,
            )
            result[key]["库存"] += int(row.get(stock_field) or 0)
            result[key]["待到货"] += int(row.get(pending_field) or 0)

    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='FBA库存明细'
            """)
            if cursor.fetchone().get("cnt", 0):
                cursor.execute("""
                    SELECT SKU, 店铺, SUM(`FBA可售`) AS 可售, SUM(`在途`) AS 在途
                    FROM `FBA库存明细`
                    WHERE SKU IS NOT NULL AND 店铺 IS NOT NULL
                    GROUP BY SKU, 店铺
                """)
                add_rows(cursor.fetchall(), "可售", "在途")
    except Exception:
        pass

    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='库存预估表'
            """)
            if cursor.fetchone().get("cnt", 0):
                cursor.execute("""
                    SELECT SKU, 店铺,
                           SUM(CASE WHEN 库存状态='本地可用量' THEN 数量 ELSE 0 END) AS 可用,
                           SUM(CASE WHEN 库存状态='本地待到货' THEN 数量 ELSE 0 END) AS 待入库
                    FROM `库存预估表`
                    WHERE SKU IS NOT NULL AND 店铺 IS NOT NULL
                    GROUP BY SKU, 店铺
                """)
                add_rows(cursor.fetchall(), "可用", "待入库")
    except Exception:
        pass

    return dict(result)


def get_inventory(
    inventory_map: Mapping[ProcurementKey, Mapping[str, int]],
    key: ProcurementKey,
) -> Dict[str, int]:
    if key in inventory_map:
        value = inventory_map[key]
        return {"库存": int(value.get("库存", 0)), "待到货": int(value.get("待到货", 0))}

    identity = key[:3]
    candidates = [value for candidate, value in inventory_map.items() if candidate[:3] == identity]
    # 仅有一个其他店铺来源时才兜底；多个店铺时不跨店重复扣减库存。
    if len(candidates) == 1:
        value = candidates[0]
        return {"库存": int(value.get("库存", 0)), "待到货": int(value.get("待到货", 0))}
    return {"库存": 0, "待到货": 0}


def read_fabric_info() -> Dict[str, Dict[str, Any]]:
    from common.database import db_cursor

    custom_fabrics: set[str] = set()
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='定制面料参数'
        """)
        if cursor.fetchone().get("cnt", 0):
            cursor.execute("SELECT 面料 FROM `定制面料参数` WHERE 面料 IS NOT NULL AND 面料!=''")
            custom_fabrics = {str(row.get("面料") or "").strip() for row in cursor.fetchall()}

        cursor.execute("""
            SELECT SPU, 面料, COALESCE(单件用量, 0) AS 单件用量,
                   COALESCE(单件损耗, 1) AS 单件损耗
            FROM `面料核价表`
            WHERE SPU IS NOT NULL AND SPU != ''
              AND 面料 IS NOT NULL AND 面料 != ''
        """)
        rows = cursor.fetchall()

    spu_fabrics: Dict[str, list[Tuple[str, float, float]]] = defaultdict(list)
    for row in rows:
        spu = str(row.get("SPU") or "").strip().upper()
        fabric = str(row.get("面料") or "").strip()
        usage = float(row.get("单件用量") or 0)
        loss = float(row.get("单件损耗") or 1.0)
        if spu and fabric:
            spu_fabrics[spu].append((fabric, usage, loss if loss > 0 else 1.0))

    output: Dict[str, Dict[str, Any]] = {}
    for spu, fabrics in spu_fabrics.items():
        fabrics.sort(key=lambda item: item[1] * item[2], reverse=True)
        primary = fabrics[0][0]
        output[spu] = {
            "fabric_type": "定制面料" if primary in custom_fabrics else "现货面料",
            "fabrics": fabrics,
        }
    return output


def build_reports(
    forecast_map: Mapping[ProcurementKey, Mapping[str, int]],
    month_order: Sequence[str],
    inventory_map: Mapping[ProcurementKey, Mapping[str, int]],
    fabric_info: Mapping[str, Mapping[str, Any]],
    factory_map: Mapping[Tuple[str, str], str],
    op_forecast_map: Mapping[Tuple[str, str], Mapping[str, int]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    order_records: List[Dict[str, Any]] = []

    # 运营预计按每个月各颜色的系统预测占比分配，保持每个 SPU+店铺+月合计不变。
    color_keys_by_spu_shop: Dict[Tuple[str, str], list[ProcurementKey]] = defaultdict(list)
    for key in forecast_map:
        color_keys_by_spu_shop[(key[0], key[3])].append(key)

    op_allocations: Dict[Tuple[ProcurementKey, str], int] = {}
    for (spu, shop), keys in color_keys_by_spu_shop.items():
        op_monthly = op_forecast_map.get((spu, shop), {})
        for month in month_order:
            total = int(op_monthly.get(month, 0) or 0)
            weights = {str(index): forecast_map[key].get(month, 0) for index, key in enumerate(keys)}
            allocated = largest_remainder_allocate(total, weights)
            for index, key in enumerate(keys):
                op_allocations[(key, month)] = allocated.get(str(index), 0)

    fabric_agg: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "spu_set": set(),
            "建议下单量合计": 0,
            "原始单耗加权和": 0.0,
            "预计用量(米)": 0.0,
        }
    )

    for key, monthly_forecast in forecast_map.items():
        spu, color_system, color_code, shop = key
        info = fabric_info.get(spu, {})
        fabric_type = str(info.get("fabric_type") or "现货面料")
        coverage = COVERAGE_MONTHS_CUSTOM if fabric_type == "定制面料" else COVERAGE_MONTHS_STOCK
        selected_months = list(month_order[:coverage])
        forecast_total = sum(int(monthly_forecast.get(month, 0) or 0) for month in selected_months)

        inventory = get_inventory(inventory_map, key)
        stock = int(inventory.get("库存", 0) or 0)
        pending = int(inventory.get("待到货", 0) or 0)
        suggested = max(0, forecast_total - stock - pending)

        month_weights = {month: int(monthly_forecast.get(month, 0) or 0) for month in selected_months}
        monthly_suggest = largest_remainder_allocate(suggested, month_weights)
        for month in month_order:
            monthly_suggest.setdefault(month, 0)

        record: Dict[str, Any] = {
            "SPU": spu,
            "颜色体系": color_system,
            "颜色缩写": color_code,
            "颜色汇总代码": f"{color_system}:{color_code}",
            "店铺": shop,
            "工厂": factory_map.get((spu, shop), ""),
            "面料类型": fabric_type,
            "覆盖月数": coverage,
            "建议下单合计": suggested,
            "运营预计合计": sum(op_allocations.get((key, month), 0) for month in month_order),
            "库存": stock,
            "待到货": pending,
            "建议下单量": suggested,
        }
        for month in month_order:
            record[f"{month}建议下单"] = monthly_suggest[month]
            record[f"{month}运营预计"] = op_allocations.get((key, month), 0)
        order_records.append(record)

        if fabric_type == "定制面料" and suggested > 0:
            for fabric, unit_usage, unit_loss in info.get("fabrics", []):
                if unit_usage <= 0:
                    continue
                loss = unit_loss if unit_loss and unit_loss > 0 else 1.0
                meters = suggested * float(unit_usage) * float(loss)
                bucket = fabric_agg[fabric]
                bucket["spu_set"].add(spu)
                bucket["建议下单量合计"] += suggested
                bucket["原始单耗加权和"] += suggested * float(unit_usage)
                bucket["预计用量(米)"] += meters

    fabric_records: List[Dict[str, Any]] = []
    for fabric, bucket in sorted(
        fabric_agg.items(), key=lambda item: item[1]["预计用量(米)"], reverse=True
    ):
        qty = int(bucket["建议下单量合计"] or 0)
        meters = float(bucket["预计用量(米)"] or 0)
        effective_unit = meters / qty if qty > 0 else 0.0
        raw_unit = float(bucket["原始单耗加权和"] or 0) / qty if qty > 0 else 0.0
        fabric_records.append({
            "面料": fabric,
            "SPU数量": len(bucket["spu_set"]),
            "建议下单量合计": qty,
            "单件用量(米)": round(effective_unit, 3),
            "原始单耗加权均值": round(raw_unit, 3),
            "预计用量(米)": round(meters, 2),
            "计算口径": "Σ(建议下单量×单件用量×单件损耗)",
        })

    return order_records, fabric_records


def _column_exists(cursor: Any, table: str, column: str) -> bool:
    cursor.execute("""
        SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s
    """, (table, column))
    return bool(cursor.fetchone().get("cnt", 0))


def _index_exists(cursor: Any, table: str, index: str) -> bool:
    cursor.execute("""
        SELECT COUNT(*) AS cnt FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND INDEX_NAME=%s
    """, (table, index))
    return bool(cursor.fetchone().get("cnt", 0))


def _ensure_column(cursor: Any, table: str, column: str, definition: str) -> None:
    if not _column_exists(cursor, table, column):
        cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN `{column}` {definition}")


def save_order_suggest(records: Sequence[Mapping[str, Any]], month_order: Sequence[str]) -> None:
    from common.database import db_cursor

    with db_cursor() as cursor:
        month_cols = "\n".join(
            f"`{month}运营预计` INT NOT NULL DEFAULT 0, `{month}建议下单` INT NOT NULL DEFAULT 0,"
            for month in month_order
        )
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_ORDER_SUGGEST}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `SPU` VARCHAR(200) NOT NULL,
                `颜色体系` VARCHAR(30) NOT NULL DEFAULT '待定',
                `颜色缩写` VARCHAR(100) NOT NULL DEFAULT '',
                `颜色汇总代码` VARCHAR(150) NOT NULL DEFAULT '',
                `店铺` VARCHAR(200) NOT NULL,
                `工厂` VARCHAR(200) NOT NULL DEFAULT '',
                `面料类型` VARCHAR(20) NOT NULL,
                `覆盖月数` TINYINT NOT NULL,
                `建议下单合计` INT NOT NULL DEFAULT 0,
                `运营预计合计` INT NOT NULL DEFAULT 0,
                `库存` INT NOT NULL DEFAULT 0,
                `待到货` INT NOT NULL DEFAULT 0,
                `建议下单量` INT NOT NULL DEFAULT 0,
                {month_cols}
                `更新时间` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_spu_system_color_shop` (`SPU`, `颜色体系`, `颜色缩写`, `店铺`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='采购建议下单量（颜色体系+颜色维度）'
        """)

        _ensure_column(cursor, TABLE_ORDER_SUGGEST, "颜色体系", "VARCHAR(30) NOT NULL DEFAULT '待定' AFTER `SPU`")
        _ensure_column(cursor, TABLE_ORDER_SUGGEST, "颜色缩写", "VARCHAR(100) NOT NULL DEFAULT '' AFTER `颜色体系`")
        _ensure_column(cursor, TABLE_ORDER_SUGGEST, "颜色汇总代码", "VARCHAR(150) NOT NULL DEFAULT '' AFTER `颜色缩写`")
        _ensure_column(cursor, TABLE_ORDER_SUGGEST, "工厂", "VARCHAR(200) NOT NULL DEFAULT '' AFTER `店铺`")
        _ensure_column(cursor, TABLE_ORDER_SUGGEST, "建议下单合计", "INT NOT NULL DEFAULT 0 AFTER `覆盖月数`")
        _ensure_column(cursor, TABLE_ORDER_SUGGEST, "运营预计合计", "INT NOT NULL DEFAULT 0 AFTER `建议下单合计`")
        for month in month_order:
            _ensure_column(cursor, TABLE_ORDER_SUGGEST, f"{month}运营预计", "INT NOT NULL DEFAULT 0")
            _ensure_column(cursor, TABLE_ORDER_SUGGEST, f"{month}建议下单", "INT NOT NULL DEFAULT 0")

        for legacy_index in ("uk_spu_color_shop", "uk_spu_shop"):
            if _index_exists(cursor, TABLE_ORDER_SUGGEST, legacy_index):
                cursor.execute(f"ALTER TABLE `{TABLE_ORDER_SUGGEST}` DROP INDEX `{legacy_index}`")
        if not _index_exists(cursor, TABLE_ORDER_SUGGEST, "uk_spu_system_color_shop"):
            cursor.execute(f"""
                ALTER TABLE `{TABLE_ORDER_SUGGEST}`
                ADD UNIQUE KEY `uk_spu_system_color_shop`
                (`SPU`, `颜色体系`, `颜色缩写`, `店铺`)
            """)

        cursor.execute(f"DELETE FROM `{TABLE_ORDER_SUGGEST}`")
        if not records:
            return

        month_columns = ", ".join(
            f"`{month}运营预计`, `{month}建议下单`" for month in month_order
        )
        placeholders = ", ".join(["%s, %s"] * len(month_order))
        sql = f"""
            INSERT INTO `{TABLE_ORDER_SUGGEST}`
            (`SPU`, `颜色体系`, `颜色缩写`, `颜色汇总代码`, `店铺`, `工厂`,
             `面料类型`, `覆盖月数`, `建议下单合计`, `运营预计合计`,
             `库存`, `待到货`, `建议下单量`, {month_columns})
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,{placeholders})
        """
        rows = []
        for record in records:
            month_values: list[int] = []
            for month in month_order:
                month_values.extend([
                    int(record.get(f"{month}运营预计", 0) or 0),
                    int(record.get(f"{month}建议下单", 0) or 0),
                ])
            rows.append((
                record["SPU"], record["颜色体系"], record["颜色缩写"],
                record["颜色汇总代码"], record["店铺"], record.get("工厂", ""),
                record["面料类型"], record["覆盖月数"], record["建议下单合计"],
                record["运营预计合计"], record["库存"], record["待到货"],
                record["建议下单量"], *month_values,
            ))
        for index in range(0, len(rows), 500):
            cursor.executemany(sql, rows[index:index + 500])


def save_fabric_usage(records: Sequence[Mapping[str, Any]]) -> None:
    from common.database import db_cursor

    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_FABRIC_USAGE}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `面料` VARCHAR(500) NOT NULL,
                `SPU数量` INT NOT NULL DEFAULT 0,
                `建议下单量合计` INT NOT NULL DEFAULT 0,
                `单件用量(米)` DECIMAL(10,3) NOT NULL DEFAULT 0,
                `原始单耗加权均值` DECIMAL(10,3) NOT NULL DEFAULT 0,
                `预计用量(米)` DECIMAL(14,2) NOT NULL DEFAULT 0,
                `计算口径` VARCHAR(200) NOT NULL DEFAULT '',
                `更新时间` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='定制面料预计用量（逐SPU加权）'
        """)
        _ensure_column(cursor, TABLE_FABRIC_USAGE, "原始单耗加权均值", "DECIMAL(10,3) NOT NULL DEFAULT 0 AFTER `单件用量(米)`")
        _ensure_column(cursor, TABLE_FABRIC_USAGE, "计算口径", "VARCHAR(200) NOT NULL DEFAULT '' AFTER `预计用量(米)`")
        cursor.execute(f"DELETE FROM `{TABLE_FABRIC_USAGE}`")
        if not records:
            return
        sql = f"""
            INSERT INTO `{TABLE_FABRIC_USAGE}`
            (`面料`, `SPU数量`, `建议下单量合计`, `单件用量(米)`,
             `原始单耗加权均值`, `预计用量(米)`, `计算口径`)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """
        rows = [(
            row["面料"], row["SPU数量"], row["建议下单量合计"],
            row["单件用量(米)"], row["原始单耗加权均值"],
            row["预计用量(米)"], row["计算口径"],
        ) for row in records]
        cursor.executemany(sql, rows)
