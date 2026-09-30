#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Daily shadow monitoring for the sales forecast project.

This job intentionally does NOT modify the existing production forecast tables or
forecast algorithm. It only reads current operational sources and writes new
`forecast_*` snapshot/monitoring tables so future models can be trained with true
point-in-time data.

Daily responsibilities
----------------------
1. Snapshot current FBA inventory (inventory has no historical daily table today).
2. Build SPU-level sales/traffic/CVR features using date-grained product performance.
3. Snapshot the current production SPU forecast so issue-date history is never lost.
4. Produce a transparent NEW_VISIBLE breakout watch (monitor only, not a forecast model).

Important leakage rule
----------------------
A snapshot only uses information available on or before `as_of_date`; by default the
latest complete source date no later than yesterday. Inventory is current-state data and
therefore may only be captured for today's snapshot date.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import get_logger
from common.database import db_cursor

logger = get_logger("forecast_daily_monitor")

FBA_TABLE = "ods_db.ods_lx_fba_warehouse_detail"
PRODUCT_TABLE = "ods_db.ods_lx_product_management"
STORE_TABLE = "ods_db.ods_lx_store_lists"
MONTHLY_SALES_TABLE = "销量统计_msku月度"
PRODUCTION_FORECAST_TABLE = "预测对比表"

INVENTORY_SNAPSHOT_TABLE = "forecast_inventory_snapshot_daily"
FEATURE_SNAPSHOT_TABLE = "forecast_feature_snapshot_daily"
PREDICTION_SNAPSHOT_TABLE = "forecast_prediction_snapshot"
BREAKOUT_MONITOR_TABLE = "forecast_breakout_monitor_daily"

PERFORMANCE_TABLE_CANDIDATES = [
    "ods_db.ods_lx_product_performance_asin",
    "ods_db.ods_lx_product_performance",
]

COLUMN_CANDIDATES = {
    "date": ["dt", "stat_date", "report_date", "date"],
    "sid": ["sid", "store_id"],
    "store": ["store_name", "shop_name", "store"],
    "sku": ["sku", "seller_sku", "msku"],
    "sales": ["volume", "units", "units_ordered", "sales_volume", "order_quantity", "ordered_units"],
    "sessions": ["sessions", "session", "sessions_total", "session_total", "visits", "traffic"],
    "delete_flag": ["delete_flag", "is_deleted"],
}

BATCH_SIZE = 500


def text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def num(v: Any) -> float:
    if v in (None, "", "None"):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def month_diff(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def safe_ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return float(a) / float(b)


def md5_key(parts: Sequence[Any]) -> str:
    raw = "|".join(text(x) for x in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def split_table(full_name: str) -> Tuple[str, str]:
    if "." in full_name:
        return tuple(full_name.split(".", 1))  # type: ignore[return-value]
    return "", full_name


def table_exists(full_name: str) -> bool:
    schema, table = split_table(full_name)
    with db_cursor() as cursor:
        if schema:
            cursor.execute(
                """SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                   WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s""",
                (schema, table),
            )
        else:
            cursor.execute(
                """SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                   WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s""",
                (table,),
            )
        row = cursor.fetchone() or {}
    return int(row.get("cnt", 0) or 0) > 0


def get_columns(full_name: str) -> List[str]:
    schema, table = split_table(full_name)
    with db_cursor() as cursor:
        if schema:
            cursor.execute(
                """SELECT COLUMN_NAME FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s""",
                (schema, table),
            )
        else:
            cursor.execute(
                """SELECT COLUMN_NAME FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s""",
                (table,),
            )
        return [str(r["COLUMN_NAME"]) for r in cursor.fetchall()]


def pick_col(columns: Iterable[str], candidates: Sequence[str], required: bool = False, label: str = "") -> Optional[str]:
    actual = set(columns)
    for c in candidates:
        if c in actual:
            return c
    if required:
        raise RuntimeError(f"未找到{label or '必需字段'}，候选={list(candidates)}，实际字段数={len(actual)}")
    return None


def ensure_tables() -> None:
    """Create only new monitoring tables. Existing production tables are untouched."""
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{INVENTORY_SNAPSHOT_TABLE}` (
              `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
              `snapshot_key` CHAR(32) NOT NULL,
              `snapshot_date` DATE NOT NULL,
              `sid` VARCHAR(32) NOT NULL DEFAULT '',
              `store_name` VARCHAR(200) NOT NULL DEFAULT '',
              `asin` VARCHAR(32) NOT NULL DEFAULT '',
              `fnsku` VARCHAR(64) NOT NULL DEFAULT '',
              `msku` VARCHAR(200) NOT NULL DEFAULT '',
              `sku` VARCHAR(200) NOT NULL DEFAULT '',
              `spu` VARCHAR(200) NOT NULL DEFAULT '',
              `product_category` VARCHAR(200) DEFAULT NULL,
              `develop_year` VARCHAR(50) DEFAULT NULL,
              `season` VARCHAR(100) DEFAULT NULL,
              `afn_fulfillable_quantity` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `afn_reserved_quantity` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `reserved_fc_transfers` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `afn_inbound_shipped_quantity` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `afn_inbound_receiving_quantity` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `fba_total_inventory` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `fba_available_inventory` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `captured_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY `uk_snapshot_key` (`snapshot_key`),
              INDEX `idx_inventory_date_spu` (`snapshot_date`,`spu`),
              INDEX `idx_inventory_date_store` (`snapshot_date`,`store_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='销量预测项目：每日FBA库存点时快照，只从启用日起积累'
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{FEATURE_SNAPSHOT_TABLE}` (
              `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
              `snapshot_date` DATE NOT NULL,
              `as_of_date` DATE NOT NULL,
              `sid` VARCHAR(32) NOT NULL DEFAULT '',
              `store_name` VARCHAR(200) NOT NULL DEFAULT '',
              `spu` VARCHAR(200) NOT NULL,
              `product_category` VARCHAR(200) DEFAULT NULL,
              `develop_year` VARCHAR(50) DEFAULT NULL,
              `season` VARCHAR(100) DEFAULT NULL,
              `first_sale_date` DATE DEFAULT NULL,
              `months_since_first_sale` INT DEFAULT NULL,
              `forecastability` VARCHAR(40) NOT NULL DEFAULT 'UNKNOWN',
              `sales_7d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_prev_7d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_14d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_prev_14d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_30d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sessions_7d` DECIMAL(20,2) DEFAULT NULL,
              `sessions_prev_7d` DECIMAL(20,2) DEFAULT NULL,
              `sessions_14d` DECIMAL(20,2) DEFAULT NULL,
              `sessions_prev_14d` DECIMAL(20,2) DEFAULT NULL,
              `sessions_30d` DECIMAL(20,2) DEFAULT NULL,
              `cvr_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_prev_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_14d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_30d` DECIMAL(12,6) DEFAULT NULL,
              `sales_growth_7d` DECIMAL(12,6) DEFAULT NULL,
              `sessions_growth_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_ratio_7d` DECIMAL(12,6) DEFAULT NULL,
              `fba_available_inventory` DECIMAL(18,2) DEFAULT NULL,
              `inventory_days_supply` DECIMAL(18,2) DEFAULT NULL,
              `source_performance_table` VARCHAR(200) NOT NULL,
              `sales_column` VARCHAR(100) NOT NULL,
              `sessions_column` VARCHAR(100) DEFAULT NULL,
              `captured_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY `uk_feature_snapshot` (`snapshot_date`,`sid`,`store_name`,`spu`),
              INDEX `idx_feature_spu_date` (`spu`,`snapshot_date`),
              INDEX `idx_feature_forecastability` (`snapshot_date`,`forecastability`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='销量预测项目：每日SPU可见特征快照，供严格时序训练/回测'
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{PREDICTION_SNAPSHOT_TABLE}` (
              `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
              `issue_date` DATE NOT NULL,
              `captured_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `spu` VARCHAR(200) NOT NULL,
              `shop` VARCHAR(200) NOT NULL,
              `target_month` DATE NOT NULL,
              `horizon` VARCHAR(10) NOT NULL,
              `forecastability` VARCHAR(40) DEFAULT NULL,
              `model_name` VARCHAR(100) NOT NULL,
              `model_version` VARCHAR(100) NOT NULL,
              `forecast_qty` INT NOT NULL DEFAULT 0,
              `forecast_low` INT DEFAULT NULL,
              `forecast_base` INT DEFAULT NULL,
              `forecast_high` INT DEFAULT NULL,
              `breakout_probability` DECIMAL(10,6) DEFAULT NULL,
              `risk_level` VARCHAR(20) DEFAULT NULL,
              `reason_code` VARCHAR(500) DEFAULT NULL,
              `source_table` VARCHAR(100) DEFAULT NULL,
              UNIQUE KEY `uk_prediction_snapshot`
                (`issue_date`,`spu`,`shop`,`target_month`,`model_name`,`model_version`),
              INDEX `idx_prediction_target` (`target_month`,`horizon`),
              INDEX `idx_prediction_spu_issue` (`spu`,`issue_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='销量预测项目：永久保留每个issue date的预测，禁止覆盖历史快照'
        """)

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{BREAKOUT_MONITOR_TABLE}` (
              `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
              `snapshot_date` DATE NOT NULL,
              `sid` VARCHAR(32) NOT NULL DEFAULT '',
              `store_name` VARCHAR(200) NOT NULL DEFAULT '',
              `spu` VARCHAR(200) NOT NULL,
              `first_sale_date` DATE DEFAULT NULL,
              `months_since_first_sale` INT DEFAULT NULL,
              `sales_7d` DECIMAL(18,2) NOT NULL DEFAULT 0,
              `sales_growth_7d` DECIMAL(12,6) DEFAULT NULL,
              `sessions_7d` DECIMAL(20,2) DEFAULT NULL,
              `sessions_growth_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_7d` DECIMAL(12,6) DEFAULT NULL,
              `cvr_ratio_7d` DECIMAL(12,6) DEFAULT NULL,
              `fba_available_inventory` DECIMAL(18,2) DEFAULT NULL,
              `inventory_days_supply` DECIMAL(18,2) DEFAULT NULL,
              `breakout_score` DECIMAL(8,2) NOT NULL DEFAULT 0,
              `breakout_probability` DECIMAL(10,6) DEFAULT NULL,
              `risk_level` VARCHAR(20) NOT NULL,
              `reason_code` VARCHAR(500) DEFAULT NULL,
              `monitor_version` VARCHAR(100) NOT NULL,
              `captured_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UNIQUE KEY `uk_breakout_snapshot` (`snapshot_date`,`sid`,`store_name`,`spu`),
              INDEX `idx_breakout_risk` (`snapshot_date`,`risk_level`),
              INDEX `idx_breakout_spu` (`spu`,`snapshot_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='NEW_VISIBLE每日爆发风险监控；V0仅规则监控，不作为生产预测'
        """)


def latest_product_cte() -> str:
    return f"""
        pm AS (
            SELECT sku, spu, product_category, develop_year, season
            FROM (
                SELECT
                    sku, spu, product_category, develop_year, season,
                    ROW_NUMBER() OVER (
                        PARTITION BY sku
                        ORDER BY update_time DESC, etl_load_time DESC, product_id DESC
                    ) AS rn
                FROM {PRODUCT_TABLE}
                WHERE sku IS NOT NULL AND TRIM(sku) <> ''
            ) x
            WHERE rn = 1
        )
    """


def capture_inventory_snapshot(snapshot_date: date, dry_run: bool = False) -> int:
    if snapshot_date != date.today():
        raise RuntimeError(
            "库存源没有日历史，禁止把当前库存伪装成历史snapshot。"
            "如需回填历史，只能使用真实历史库存源。"
        )
    if not table_exists(FBA_TABLE):
        raise RuntimeError(f"库存源表不存在: {FBA_TABLE}")

    fba_cols = set(get_columns(FBA_TABLE))
    required = {
        "sid", "asin", "fnsku", "seller_sku", "sku",
        "afn_fulfillable_quantity", "afn_reserved_quantity", "reserved_fc_transfers",
        "afn_inbound_shipped_quantity", "afn_inbound_receiving_quantity",
    }
    missing = sorted(required - fba_cols)
    if missing:
        raise RuntimeError(f"{FBA_TABLE} 缺少库存快照必需字段: {missing}")

    store_join = ""
    store_expr = "''"
    if table_exists(STORE_TABLE):
        store_cols = set(get_columns(STORE_TABLE))
        if {"sid", "store_name"}.issubset(store_cols):
            store_join = f"LEFT JOIN {STORE_TABLE} st ON st.sid=f.sid"
            store_expr = "COALESCE(st.store_name,'')"

    sql = f"""
        WITH {latest_product_cte()}
        SELECT
            f.sid, {store_expr} AS store_name,
            COALESCE(f.asin,'') AS asin,
            COALESCE(f.fnsku,'') AS fnsku,
            COALESCE(f.seller_sku,'') AS msku,
            COALESCE(f.sku,'') AS sku,
            COALESCE(pm.spu,'') AS spu,
            pm.product_category, pm.develop_year, pm.season,
            COALESCE(f.afn_fulfillable_quantity,0) AS fulfillable,
            COALESCE(f.afn_reserved_quantity,0) AS reserved,
            COALESCE(f.reserved_fc_transfers,0) AS transfers,
            COALESCE(f.afn_inbound_shipped_quantity,0) AS inbound_shipped,
            COALESCE(f.afn_inbound_receiving_quantity,0) AS inbound_receiving
        FROM {FBA_TABLE} f
        LEFT JOIN pm ON pm.sku=f.sku
        {store_join}
    """
    with db_cursor() as cursor:
        cursor.execute(sql)
        rows = list(cursor.fetchall())

    payload = []
    for r in rows:
        fulfillable = num(r.get("fulfillable"))
        reserved = num(r.get("reserved"))
        transfers = num(r.get("transfers"))
        inbound_shipped = num(r.get("inbound_shipped"))
        inbound_receiving = num(r.get("inbound_receiving"))
        # Keep the currently approved business formulas. Raw components are also stored
        # so this can be recomputed later if reserved/transfers semantics are revised.
        total_inventory = fulfillable + reserved + transfers + inbound_shipped + inbound_receiving
        available_inventory = fulfillable + reserved + transfers + inbound_receiving
        key = md5_key([
            snapshot_date, r.get("sid"), r.get("asin"), r.get("fnsku"),
            r.get("msku"), r.get("sku"),
        ])
        payload.append((
            key, snapshot_date, text(r.get("sid")), text(r.get("store_name")),
            text(r.get("asin")), text(r.get("fnsku")), text(r.get("msku")),
            text(r.get("sku")), text(r.get("spu")), r.get("product_category"),
            r.get("develop_year"), r.get("season"), fulfillable, reserved, transfers,
            inbound_shipped, inbound_receiving, total_inventory, available_inventory,
        ))

    logger.info(f"库存快照准备完成: {len(payload)} 行")
    if dry_run or not payload:
        return len(payload)

    insert_sql = f"""
        INSERT INTO `{INVENTORY_SNAPSHOT_TABLE}`
        (`snapshot_key`,`snapshot_date`,`sid`,`store_name`,`asin`,`fnsku`,`msku`,`sku`,`spu`,
         `product_category`,`develop_year`,`season`,`afn_fulfillable_quantity`,
         `afn_reserved_quantity`,`reserved_fc_transfers`,`afn_inbound_shipped_quantity`,
         `afn_inbound_receiving_quantity`,`fba_total_inventory`,`fba_available_inventory`)
        VALUES ({','.join(['%s'] * 19)})
        ON DUPLICATE KEY UPDATE
          `store_name`=VALUES(`store_name`), `spu`=VALUES(`spu`),
          `product_category`=VALUES(`product_category`), `develop_year`=VALUES(`develop_year`),
          `season`=VALUES(`season`),
          `afn_fulfillable_quantity`=VALUES(`afn_fulfillable_quantity`),
          `afn_reserved_quantity`=VALUES(`afn_reserved_quantity`),
          `reserved_fc_transfers`=VALUES(`reserved_fc_transfers`),
          `afn_inbound_shipped_quantity`=VALUES(`afn_inbound_shipped_quantity`),
          `afn_inbound_receiving_quantity`=VALUES(`afn_inbound_receiving_quantity`),
          `fba_total_inventory`=VALUES(`fba_total_inventory`),
          `fba_available_inventory`=VALUES(`fba_available_inventory`),
          `captured_at`=CURRENT_TIMESTAMP
    """
    with db_cursor() as cursor:
        for i in range(0, len(payload), BATCH_SIZE):
            cursor.executemany(insert_sql, payload[i:i + BATCH_SIZE])
    return len(payload)


def resolve_performance_source(snapshot_date: date) -> Dict[str, Any]:
    errors = []
    for table in PERFORMANCE_TABLE_CANDIDATES:
        if not table_exists(table):
            continue
        cols = get_columns(table)
        try:
            mapping = {
                "table": table,
                "date": pick_col(cols, COLUMN_CANDIDATES["date"], True, "日期字段"),
                "sid": pick_col(cols, COLUMN_CANDIDATES["sid"], True, "店铺ID字段"),
                "store": pick_col(cols, COLUMN_CANDIDATES["store"], False),
                "sku": pick_col(cols, COLUMN_CANDIDATES["sku"], True, "SKU字段"),
                "sales": pick_col(cols, COLUMN_CANDIDATES["sales"], True, "销量字段"),
                "sessions": pick_col(cols, COLUMN_CANDIDATES["sessions"], False),
                "delete_flag": pick_col(cols, COLUMN_CANDIDATES["delete_flag"], False),
            }
            dcol = mapping["date"]
            with db_cursor() as cursor:
                cursor.execute(
                    f"SELECT MAX(`{dcol}`) AS max_dt FROM {table} WHERE `{dcol}` <= %s",
                    (snapshot_date - timedelta(days=1),),
                )
                row = cursor.fetchone() or {}
            max_dt = row.get("max_dt")
            if max_dt is None:
                errors.append(f"{table}: snapshot前无数据")
                continue
            if isinstance(max_dt, datetime):
                max_dt = max_dt.date()
            elif not isinstance(max_dt, date):
                max_dt = datetime.strptime(str(max_dt)[:10], "%Y-%m-%d").date()
            mapping["as_of_date"] = max_dt
            if not mapping["sessions"]:
                logger.warning(
                    f"{table} 未识别到Sessions字段；销量特征仍会保存，但流量/CVR保持NULL。"
                    f"实际字段={cols}"
                )
            logger.info(f"产品表现源: {table}; 字段映射={mapping}")
            return mapping
        except Exception as exc:
            errors.append(f"{table}: {exc}")
    raise RuntimeError("未找到可用的日维度产品表现源；" + "; ".join(errors))


def load_first_sale_months() -> Dict[str, date]:
    """Use the existing monthly sales table as a stable, cheap first-sale anchor."""
    if not table_exists(MONTHLY_SALES_TABLE):
        logger.warning(f"{MONTHLY_SALES_TABLE} 不存在，first_sale_date将为空")
        return {}
    cols = set(get_columns(MONTHLY_SALES_TABLE))
    if not {"统计日期", "销量"}.issubset(cols):
        logger.warning(f"{MONTHLY_SALES_TABLE} 缺统计日期/销量，first_sale_date将为空")
        return {}

    if "SPU" in cols:
        sql = f"""
            SELECT SPU, MIN(`统计日期`) AS first_sale
            FROM `{MONTHLY_SALES_TABLE}`
            WHERE SPU IS NOT NULL AND TRIM(SPU)<>'' AND COALESCE(`销量`,0)>0
            GROUP BY SPU
        """
    elif "SKU" in cols:
        sql = f"""
            SELECT SUBSTRING_INDEX(SKU,'-',1) AS SPU, MIN(`统计日期`) AS first_sale
            FROM `{MONTHLY_SALES_TABLE}`
            WHERE SKU IS NOT NULL AND TRIM(SKU)<>'' AND COALESCE(`销量`,0)>0
            GROUP BY SUBSTRING_INDEX(SKU,'-',1)
        """
    else:
        logger.warning(f"{MONTHLY_SALES_TABLE} 无SPU/SKU字段，first_sale_date将为空")
        return {}

    with db_cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    out: Dict[str, date] = {}
    for r in rows:
        spu = text(r.get("SPU"))
        d = r.get("first_sale")
        if not spu or d is None:
            continue
        if isinstance(d, datetime):
            d = d.date()
        elif not isinstance(d, date):
            d = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
        out[spu] = d
    return out


def inventory_by_spu(snapshot_date: date) -> Dict[Tuple[str, str, str], float]:
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT sid, store_name, spu, SUM(fba_available_inventory) AS inv
            FROM `{INVENTORY_SNAPSHOT_TABLE}`
            WHERE snapshot_date=%s AND spu<>''
            GROUP BY sid, store_name, spu
        """, (snapshot_date,))
        rows = cursor.fetchall()
    return {
        (text(r.get("sid")), text(r.get("store_name")), text(r.get("spu"))): num(r.get("inv"))
        for r in rows
    }


def build_feature_rows(snapshot_date: date) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    source = resolve_performance_source(snapshot_date)
    table = source["table"]
    dcol = source["date"]
    sid_col = source["sid"]
    store_col = source["store"]
    sku_col = source["sku"]
    sales_col = source["sales"]
    sessions_col = source["sessions"]
    delete_col = source["delete_flag"]
    as_of: date = source["as_of_date"]

    start_30 = as_of - timedelta(days=29)
    start_prev14 = as_of - timedelta(days=27)
    end_prev14 = as_of - timedelta(days=14)
    start_14 = as_of - timedelta(days=13)
    start_prev7 = as_of - timedelta(days=13)
    end_prev7 = as_of - timedelta(days=7)
    start_7 = as_of - timedelta(days=6)

    store_expr = f"COALESCE(p.`{store_col}`,'')" if store_col else "COALESCE(st.store_name,'')"
    store_join = ""
    if not store_col and table_exists(STORE_TABLE):
        store_join = f"LEFT JOIN {STORE_TABLE} st ON st.sid=p.`{sid_col}`"

    delete_filter = f"AND COALESCE(p.`{delete_col}`,0)=0" if delete_col else ""
    sessions_select = ""
    if sessions_col:
        sessions_select = f""",
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sessions_col}`,0) ELSE 0 END) AS sessions_7d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sessions_col}`,0) ELSE 0 END) AS sessions_prev_7d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sessions_col}`,0) ELSE 0 END) AS sessions_14d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sessions_col}`,0) ELSE 0 END) AS sessions_prev_14d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sessions_col}`,0) ELSE 0 END) AS sessions_30d
        """

    sql = f"""
        WITH {latest_product_cte()}
        SELECT
            p.`{sid_col}` AS sid,
            {store_expr} AS store_name,
            pm.spu,
            MAX(pm.product_category) AS product_category,
            MAX(pm.develop_year) AS develop_year,
            MAX(pm.season) AS season,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sales_col}`,0) ELSE 0 END) AS sales_7d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sales_col}`,0) ELSE 0 END) AS sales_prev_7d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sales_col}`,0) ELSE 0 END) AS sales_14d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sales_col}`,0) ELSE 0 END) AS sales_prev_14d,
            SUM(CASE WHEN p.`{dcol}` BETWEEN %s AND %s THEN COALESCE(p.`{sales_col}`,0) ELSE 0 END) AS sales_30d
            {sessions_select}
        FROM {table} p
        LEFT JOIN pm ON pm.sku=p.`{sku_col}`
        {store_join}
        WHERE p.`{dcol}` BETWEEN %s AND %s
          AND pm.spu IS NOT NULL AND TRIM(pm.spu)<>''
          {delete_filter}
        GROUP BY p.`{sid_col}`, {store_expr}, pm.spu
    """

    params: List[Any] = [
        start_7, as_of,
        start_prev7, end_prev7,
        start_14, as_of,
        start_prev14, end_prev14,
        start_30, as_of,
    ]
    if sessions_col:
        params += [
            start_7, as_of,
            start_prev7, end_prev7,
            start_14, as_of,
            start_prev14, end_prev14,
            start_30, as_of,
        ]
    params += [start_30, as_of]

    with db_cursor() as cursor:
        cursor.execute(sql, params)
        raw = list(cursor.fetchall())

    first_sale = load_first_sale_months()
    inventory = inventory_by_spu(snapshot_date)
    out: List[Dict[str, Any]] = []
    for r in raw:
        sid = text(r.get("sid"))
        store = text(r.get("store_name"))
        spu = text(r.get("spu"))
        if not spu:
            continue
        s7 = num(r.get("sales_7d"))
        sp7 = num(r.get("sales_prev_7d"))
        s14 = num(r.get("sales_14d"))
        sp14 = num(r.get("sales_prev_14d"))
        s30 = num(r.get("sales_30d"))
        sess7 = num(r.get("sessions_7d")) if sessions_col else None
        sessp7 = num(r.get("sessions_prev_7d")) if sessions_col else None
        sess14 = num(r.get("sessions_14d")) if sessions_col else None
        sessp14 = num(r.get("sessions_prev_14d")) if sessions_col else None
        sess30 = num(r.get("sessions_30d")) if sessions_col else None
        cvr7 = safe_ratio(s7, sess7)
        cvrp7 = safe_ratio(sp7, sessp7)
        cvr14 = safe_ratio(s14, sess14)
        cvr30 = safe_ratio(s30, sess30)
        fs = first_sale.get(spu)
        age_months = month_diff(month_start(fs), month_start(as_of)) if fs else None
        if fs is None:
            forecastability = "UNKNOWN"
        elif age_months <= 3:
            forecastability = "NEW_VISIBLE"
        else:
            forecastability = "ESTABLISHED"
        inv = inventory.get((sid, store, spu))
        if inv is None:
            # Some store-name mappings differ; fallback to sid+spu across store_name.
            candidates = [v for (s, _st, p), v in inventory.items() if s == sid and p == spu]
            inv = sum(candidates) if candidates else None
        dos = None
        if inv is not None and s30 > 0:
            dos = inv / (s30 / 30.0)
        out.append({
            "snapshot_date": snapshot_date,
            "as_of_date": as_of,
            "sid": sid,
            "store_name": store,
            "spu": spu,
            "product_category": r.get("product_category"),
            "develop_year": r.get("develop_year"),
            "season": r.get("season"),
            "first_sale_date": fs,
            "months_since_first_sale": age_months,
            "forecastability": forecastability,
            "sales_7d": s7,
            "sales_prev_7d": sp7,
            "sales_14d": s14,
            "sales_prev_14d": sp14,
            "sales_30d": s30,
            "sessions_7d": sess7,
            "sessions_prev_7d": sessp7,
            "sessions_14d": sess14,
            "sessions_prev_14d": sessp14,
            "sessions_30d": sess30,
            "cvr_7d": cvr7,
            "cvr_prev_7d": cvrp7,
            "cvr_14d": cvr14,
            "cvr_30d": cvr30,
            "sales_growth_7d": safe_ratio(s7, sp7),
            "sessions_growth_7d": safe_ratio(sess7, sessp7),
            "cvr_ratio_7d": safe_ratio(cvr7, cvrp7),
            "fba_available_inventory": inv,
            "inventory_days_supply": dos,
            "source_performance_table": table,
            "sales_column": sales_col,
            "sessions_column": sessions_col,
        })
    logger.info(
        f"日特征准备完成: {len(out)} 个SPU+店铺；as_of={as_of}; "
        f"sessions_field={sessions_col or 'NONE'}"
    )
    return out, source


def save_feature_rows(rows: Sequence[Mapping[str, Any]], dry_run: bool = False) -> int:
    if dry_run or not rows:
        return len(rows)
    cols = [
        "snapshot_date", "as_of_date", "sid", "store_name", "spu", "product_category",
        "develop_year", "season", "first_sale_date", "months_since_first_sale", "forecastability",
        "sales_7d", "sales_prev_7d", "sales_14d", "sales_prev_14d", "sales_30d",
        "sessions_7d", "sessions_prev_7d", "sessions_14d", "sessions_prev_14d", "sessions_30d",
        "cvr_7d", "cvr_prev_7d", "cvr_14d", "cvr_30d", "sales_growth_7d",
        "sessions_growth_7d", "cvr_ratio_7d", "fba_available_inventory", "inventory_days_supply",
        "source_performance_table", "sales_column", "sessions_column",
    ]
    sql = f"""
        INSERT INTO `{FEATURE_SNAPSHOT_TABLE}` ({','.join(f'`{c}`' for c in cols)})
        VALUES ({','.join(['%s'] * len(cols))})
        ON DUPLICATE KEY UPDATE
          `as_of_date`=VALUES(`as_of_date`), `product_category`=VALUES(`product_category`),
          `develop_year`=VALUES(`develop_year`), `season`=VALUES(`season`),
          `first_sale_date`=VALUES(`first_sale_date`),
          `months_since_first_sale`=VALUES(`months_since_first_sale`),
          `forecastability`=VALUES(`forecastability`),
          `sales_7d`=VALUES(`sales_7d`), `sales_prev_7d`=VALUES(`sales_prev_7d`),
          `sales_14d`=VALUES(`sales_14d`), `sales_prev_14d`=VALUES(`sales_prev_14d`),
          `sales_30d`=VALUES(`sales_30d`),
          `sessions_7d`=VALUES(`sessions_7d`), `sessions_prev_7d`=VALUES(`sessions_prev_7d`),
          `sessions_14d`=VALUES(`sessions_14d`), `sessions_prev_14d`=VALUES(`sessions_prev_14d`),
          `sessions_30d`=VALUES(`sessions_30d`),
          `cvr_7d`=VALUES(`cvr_7d`), `cvr_prev_7d`=VALUES(`cvr_prev_7d`),
          `cvr_14d`=VALUES(`cvr_14d`), `cvr_30d`=VALUES(`cvr_30d`),
          `sales_growth_7d`=VALUES(`sales_growth_7d`),
          `sessions_growth_7d`=VALUES(`sessions_growth_7d`),
          `cvr_ratio_7d`=VALUES(`cvr_ratio_7d`),
          `fba_available_inventory`=VALUES(`fba_available_inventory`),
          `inventory_days_supply`=VALUES(`inventory_days_supply`),
          `source_performance_table`=VALUES(`source_performance_table`),
          `sales_column`=VALUES(`sales_column`), `sessions_column`=VALUES(`sessions_column`),
          `captured_at`=CURRENT_TIMESTAMP
    """
    payload = [tuple(r.get(c) for c in cols) for r in rows]
    with db_cursor() as cursor:
        for i in range(0, len(payload), BATCH_SIZE):
            cursor.executemany(sql, payload[i:i + BATCH_SIZE])
    return len(payload)


def snapshot_live_predictions(snapshot_date: date, dry_run: bool = False) -> int:
    if not table_exists(PRODUCTION_FORECAST_TABLE):
        logger.warning(f"{PRODUCTION_FORECAST_TABLE} 不存在，跳过生产预测快照")
        return 0
    cols = set(get_columns(PRODUCTION_FORECAST_TABLE))
    required = {"SPU", "店铺", "统计日期", "系统预测销量"}
    if not required.issubset(cols):
        logger.warning(f"{PRODUCTION_FORECAST_TABLE} 缺字段 {sorted(required-cols)}，跳过预测快照")
        return 0

    issue_month = month_start(snapshot_date)
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT SPU, 店铺, 统计日期, 系统预测销量
            FROM `{PRODUCTION_FORECAST_TABLE}`
            WHERE 统计日期 >= %s
        """, (issue_month,))
        rows = list(cursor.fetchall())
        cursor.execute(f"""
            SELECT store_name, spu, forecastability
            FROM `{FEATURE_SNAPSHOT_TABLE}` WHERE snapshot_date=%s
        """, (snapshot_date,))
        feature_rows = cursor.fetchall()
    f_map = {(text(r.get("store_name")), text(r.get("spu"))): text(r.get("forecastability")) for r in feature_rows}

    payload = []
    for r in rows:
        spu = text(r.get("SPU"))
        shop = text(r.get("店铺"))
        target = r.get("统计日期")
        if isinstance(target, datetime):
            target = target.date()
        elif not isinstance(target, date):
            target = datetime.strptime(str(target)[:10], "%Y-%m-%d").date()
        h = month_diff(issue_month, month_start(target))
        if h < 0:
            continue
        payload.append((
            snapshot_date, spu, shop, month_start(target), f"H{h}",
            f_map.get((shop, spu)), "production_current", "legacy_v4_live",
            int(r.get("系统预测销量") or 0), int(r.get("系统预测销量") or 0),
            "LIVE_TABLE_SNAPSHOT", PRODUCTION_FORECAST_TABLE,
        ))

    logger.info(f"生产预测快照准备完成: {len(payload)} 行")
    if dry_run or not payload:
        return len(payload)
    sql = f"""
        INSERT INTO `{PREDICTION_SNAPSHOT_TABLE}`
        (`issue_date`,`spu`,`shop`,`target_month`,`horizon`,`forecastability`,
         `model_name`,`model_version`,`forecast_qty`,`forecast_base`,`reason_code`,`source_table`)
        VALUES ({','.join(['%s'] * 12)})
        ON DUPLICATE KEY UPDATE
          `forecastability`=VALUES(`forecastability`),
          `forecast_qty`=VALUES(`forecast_qty`), `forecast_base`=VALUES(`forecast_base`),
          `reason_code`=VALUES(`reason_code`), `source_table`=VALUES(`source_table`),
          `captured_at`=CURRENT_TIMESTAMP
    """
    with db_cursor() as cursor:
        for i in range(0, len(payload), BATCH_SIZE):
            cursor.executemany(sql, payload[i:i + BATCH_SIZE])
    return len(payload)


def breakout_watch(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Transparent monitor-only rules. No probability is claimed at V0."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        if text(r.get("forecastability")) != "NEW_VISIBLE":
            continue
        sg = r.get("sales_growth_7d")
        tg = r.get("sessions_growth_7d")
        cg = r.get("cvr_ratio_7d")
        sales7 = num(r.get("sales_7d"))
        score = 0.0
        reasons: List[str] = []

        if sg is not None and float(sg) >= 2.0:
            score += 30; reasons.append("SALES_SURGE_2X")
        elif sg is not None and float(sg) >= 1.2:
            score += 15; reasons.append("SALES_RISING")

        if tg is not None and float(tg) >= 2.0:
            score += 30; reasons.append("TRAFFIC_SURGE_2X")
        elif tg is not None and float(tg) >= 1.2:
            score += 15; reasons.append("TRAFFIC_RISING")

        if cg is not None and float(cg) >= 1.10:
            score += 15; reasons.append("CVR_IMPROVING")
        elif cg is not None and float(cg) >= 0.85:
            score += 8; reasons.append("CVR_HOLDING")
        elif cg is not None and float(cg) < 0.70:
            score -= 10; reasons.append("CVR_WEAKENING")

        if sales7 >= 1000:
            score += 15; reasons.append("HIGH_7D_VOLUME")
        elif sales7 >= 300:
            score += 8; reasons.append("MEDIUM_7D_VOLUME")

        dos = r.get("inventory_days_supply")
        if dos is not None and float(dos) >= 45:
            score += 10; reasons.append("INVENTORY_SUPPORT_45D")
        elif dos is not None and float(dos) < 14:
            score -= 10; reasons.append("INVENTORY_CONSTRAINED")

        score = max(0.0, min(100.0, score))
        if score >= 70:
            risk = "HIGH"
        elif score >= 45:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        x = dict(r)
        x["breakout_score"] = score
        x["breakout_probability"] = None  # do not pretend a rule score is calibrated probability
        x["risk_level"] = risk
        x["reason_code"] = ",".join(reasons) if reasons else "NO_STRONG_SIGNAL"
        x["monitor_version"] = "RULE_V0_MONITOR_ONLY"
        out.append(x)
    return out


def save_breakout_rows(rows: Sequence[Mapping[str, Any]], dry_run: bool = False) -> int:
    if dry_run or not rows:
        return len(rows)
    cols = [
        "snapshot_date", "sid", "store_name", "spu", "first_sale_date", "months_since_first_sale",
        "sales_7d", "sales_growth_7d", "sessions_7d", "sessions_growth_7d", "cvr_7d",
        "cvr_ratio_7d", "fba_available_inventory", "inventory_days_supply", "breakout_score",
        "breakout_probability", "risk_level", "reason_code", "monitor_version",
    ]
    sql = f"""
        INSERT INTO `{BREAKOUT_MONITOR_TABLE}` ({','.join(f'`{c}`' for c in cols)})
        VALUES ({','.join(['%s'] * len(cols))})
        ON DUPLICATE KEY UPDATE
          `first_sale_date`=VALUES(`first_sale_date`),
          `months_since_first_sale`=VALUES(`months_since_first_sale`),
          `sales_7d`=VALUES(`sales_7d`), `sales_growth_7d`=VALUES(`sales_growth_7d`),
          `sessions_7d`=VALUES(`sessions_7d`), `sessions_growth_7d`=VALUES(`sessions_growth_7d`),
          `cvr_7d`=VALUES(`cvr_7d`), `cvr_ratio_7d`=VALUES(`cvr_ratio_7d`),
          `fba_available_inventory`=VALUES(`fba_available_inventory`),
          `inventory_days_supply`=VALUES(`inventory_days_supply`),
          `breakout_score`=VALUES(`breakout_score`),
          `breakout_probability`=VALUES(`breakout_probability`),
          `risk_level`=VALUES(`risk_level`), `reason_code`=VALUES(`reason_code`),
          `monitor_version`=VALUES(`monitor_version`), `captured_at`=CURRENT_TIMESTAMP
    """
    payload = [tuple(r.get(c) for c in cols) for r in rows]
    with db_cursor() as cursor:
        for i in range(0, len(payload), BATCH_SIZE):
            cursor.executemany(sql, payload[i:i + BATCH_SIZE])
    return len(payload)


def maybe_notify(snapshot_date: date, features: Sequence[Mapping[str, Any]], alerts: Sequence[Mapping[str, Any]]) -> None:
    try:
        from scripts.notify_feishu import send_notify
    except Exception as exc:
        logger.warning(f"无法导入飞书通知模块，跳过通知: {exc}")
        return
    high = [r for r in alerts if text(r.get("risk_level")) == "HIGH"]
    medium = [r for r in alerts if text(r.get("risk_level")) == "MEDIUM"]
    new_visible = [r for r in features if text(r.get("forecastability")) == "NEW_VISIBLE"]
    top = sorted(alerts, key=lambda r: num(r.get("breakout_score")), reverse=True)[:8]
    lines = [
        f"**快照日期：** {snapshot_date}",
        f"**NEW_VISIBLE：** {len(new_visible)} 款",
        f"**高风险：** {len(high)} 款；**中风险：** {len(medium)} 款",
    ]
    if top:
        lines.append("\n**Top监控：**")
        for r in top:
            lines.append(
                f"- {text(r.get('spu'))} | {text(r.get('store_name'))} | "
                f"score={num(r.get('breakout_score')):.0f} | "
                f"7d销量={num(r.get('sales_7d')):.0f} | "
                f"销量动量={r.get('sales_growth_7d')} | 流量动量={r.get('sessions_growth_7d')} | "
                f"{text(r.get('reason_code'))}"
            )
    send_notify("销量预测每日监控", "success", "\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser(description="销量预测动态监控：每日影子快照")
    ap.add_argument("--snapshot-date", default=date.today().isoformat())
    ap.add_argument("--dry-run", action="store_true", help="只读取/计算，不写forecast_*表")
    ap.add_argument("--notify", action="store_true", help="完成后发送飞书监控摘要")
    args = ap.parse_args()

    snapshot_date = datetime.strptime(args.snapshot_date, "%Y-%m-%d").date()
    logger.info("=" * 72)
    logger.info(f"销量预测每日影子监控 snapshot={snapshot_date} dry_run={args.dry_run}")
    logger.info("=" * 72)

    if not args.dry_run:
        ensure_tables()

    inv_n = capture_inventory_snapshot(snapshot_date, dry_run=args.dry_run)
    features, source = build_feature_rows(snapshot_date)
    feat_n = save_feature_rows(features, dry_run=args.dry_run)
    pred_n = snapshot_live_predictions(snapshot_date, dry_run=args.dry_run)
    alerts = breakout_watch(features)
    alert_n = save_breakout_rows(alerts, dry_run=args.dry_run)

    high = sum(1 for r in alerts if r.get("risk_level") == "HIGH")
    medium = sum(1 for r in alerts if r.get("risk_level") == "MEDIUM")
    new_visible = sum(1 for r in features if r.get("forecastability") == "NEW_VISIBLE")
    summary = {
        "snapshot_date": snapshot_date.isoformat(),
        "as_of_date": str(source.get("as_of_date")),
        "performance_source": source.get("table"),
        "sales_column": source.get("sales"),
        "sessions_column": source.get("sessions"),
        "inventory_rows": inv_n,
        "feature_rows": feat_n,
        "prediction_rows": pred_n,
        "new_visible_rows": new_visible,
        "breakout_rows": alert_n,
        "high_risk": high,
        "medium_risk": medium,
        "dry_run": bool(args.dry_run),
    }
    logger.info("MONITOR_SUMMARY=" + json.dumps(summary, ensure_ascii=False, default=str))

    if args.notify and not args.dry_run:
        maybe_notify(snapshot_date, features, alerts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
