#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V4.

Deployment fixes on top of V3:
1. First dry-run never depends on any forecast_* table already existing.
2. Monitoring scope is restricted to the four forecast-pipeline shops:
   JQ-US / RKZ-US / SY-US / MT-US.
3. UNKNOWN lifecycle diagnostics are printed by store so shop-name / first-sale mapping
   issues are visible before production scheduling.

Still shadow-only: only forecast_* monitoring tables are written. Existing production
forecast/procurement tables are never modified.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v3 as v3

TARGET_SHOPS = ("JQ-US", "RKZ-US", "SY-US", "MT-US")
TARGET_SHOP_SET = set(TARGET_SHOPS)


def capture_inventory_snapshot(snapshot_date: date, dry_run: bool = False) -> int:
    """V3 inventory snapshot, but only for the four target shops."""
    if snapshot_date != date.today():
        raise RuntimeError(
            "库存源没有日历史，禁止把当前库存伪装成历史snapshot。"
            "如需回填历史，只能使用真实历史库存源。"
        )
    if not base.table_exists(base.FBA_TABLE):
        raise RuntimeError(f"库存源表不存在: {base.FBA_TABLE}")

    fba_cols = set(base.get_columns(base.FBA_TABLE))
    required = {
        "sid", "asin", "fnsku", "sku",
        "afn_fulfillable_quantity", "afn_reserved_quantity", "reserved_fc_transfers",
        "afn_inbound_shipped_quantity", "afn_inbound_receiving_quantity",
    }
    missing = sorted(required - fba_cols)
    if missing:
        raise RuntimeError(f"{base.FBA_TABLE} 缺少库存快照必需字段: {missing}")

    if "msku" in fba_cols:
        msku_col = "msku"
    elif "seller_sku" in fba_cols:
        msku_col = "seller_sku"
    else:
        raise RuntimeError(f"{base.FBA_TABLE} 既没有 msku 也没有 seller_sku")

    params: List[Any] = []
    if "store_name" in fba_cols:
        store_expr = "COALESCE(f.store_name,'')"
        store_join = ""
        store_filter = "AND f.store_name IN (%s,%s,%s,%s)"
        params.extend(TARGET_SHOPS)
    else:
        store_expr = "COALESCE(st.store_name,'')"
        store_join = (
            f"LEFT JOIN {base.STORE_TABLE} st ON st.sid=f.sid"
            if base.table_exists(base.STORE_TABLE) else ""
        )
        if store_join:
            store_filter = "AND st.store_name IN (%s,%s,%s,%s)"
            params.extend(TARGET_SHOPS)
        else:
            store_expr = "''"
            store_filter = ""

    sql = f"""
        WITH {base.latest_product_cte()}
        SELECT
            f.sid,
            {store_expr} AS store_name,
            COALESCE(f.asin,'') AS asin,
            COALESCE(f.fnsku,'') AS fnsku,
            COALESCE(f.`{msku_col}`,'') AS msku,
            COALESCE(f.sku,'') AS sku,
            COALESCE(pm.spu,'') AS spu,
            pm.product_category,
            pm.develop_year,
            pm.season,
            COALESCE(f.afn_fulfillable_quantity,0) AS fulfillable,
            COALESCE(f.afn_reserved_quantity,0) AS reserved,
            COALESCE(f.reserved_fc_transfers,0) AS transfers,
            COALESCE(f.afn_inbound_shipped_quantity,0) AS inbound_shipped,
            COALESCE(f.afn_inbound_receiving_quantity,0) AS inbound_receiving
        FROM {base.FBA_TABLE} f
        LEFT JOIN pm ON pm.sku=f.sku
        {store_join}
        WHERE 1=1
        {store_filter}
    """
    with db_cursor() as cursor:
        cursor.execute(sql, params)
        rows = list(cursor.fetchall())

    payload = []
    for r in rows:
        store = base.text(r.get("store_name"))
        if store not in TARGET_SHOP_SET:
            continue
        fulfillable = base.num(r.get("fulfillable"))
        reserved = base.num(r.get("reserved"))
        transfers = base.num(r.get("transfers"))
        inbound_shipped = base.num(r.get("inbound_shipped"))
        inbound_receiving = base.num(r.get("inbound_receiving"))
        total_inventory = fulfillable + reserved + transfers + inbound_shipped + inbound_receiving
        available_inventory = fulfillable + reserved + transfers + inbound_receiving
        key = base.md5_key([
            snapshot_date, r.get("sid"), r.get("asin"), r.get("fnsku"),
            r.get("msku"), r.get("sku"),
        ])
        payload.append((
            key, snapshot_date, base.text(r.get("sid")), store,
            base.text(r.get("asin")), base.text(r.get("fnsku")), base.text(r.get("msku")),
            base.text(r.get("sku")), base.text(r.get("spu")), r.get("product_category"),
            r.get("develop_year"), r.get("season"), fulfillable, reserved, transfers,
            inbound_shipped, inbound_receiving, total_inventory, available_inventory,
        ))

    base.logger.info(
        f"库存快照V4准备完成: {len(payload)} 行; target_shops={list(TARGET_SHOPS)}; "
        f"msku_source={msku_col}"
    )
    if dry_run or not payload:
        return len(payload)

    insert_sql = f"""
        INSERT INTO `{base.INVENTORY_SNAPSHOT_TABLE}`
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
        for i in range(0, len(payload), base.BATCH_SIZE):
            cursor.executemany(insert_sql, payload[i:i + base.BATCH_SIZE])
    return len(payload)


def build_feature_rows(snapshot_date: date):
    rows, source = v3.build_feature_rows(snapshot_date)
    rows = [r for r in rows if base.text(r.get("store_name")) in TARGET_SHOP_SET]

    by_state = Counter(base.text(r.get("forecastability")) or "UNKNOWN" for r in rows)
    unknown_by_store = Counter(
        base.text(r.get("store_name"))
        for r in rows
        if base.text(r.get("forecastability")) == "UNKNOWN"
    )
    total_by_store = Counter(base.text(r.get("store_name")) for r in rows)

    base.logger.info(
        "V4目标店铺特征范围: "
        f"rows={len(rows)}, states={dict(by_state)}, target_shops={list(TARGET_SHOPS)}"
    )
    if unknown_by_store:
        detail = {
            shop: {
                "unknown": int(unknown_by_store.get(shop, 0)),
                "total": int(total_by_store.get(shop, 0)),
                "unknown_rate": round(
                    unknown_by_store.get(shop, 0) / total_by_store.get(shop, 1), 4
                ),
            }
            for shop in TARGET_SHOPS
            if total_by_store.get(shop, 0) > 0
        }
        base.logger.warning(f"V4首销UNKNOWN按店铺: {detail}")
    return rows, source


def snapshot_live_predictions(snapshot_date: date, dry_run: bool = False) -> int:
    """Snapshot only target shops; dry-run has no dependency on feature snapshot table."""
    if not base.table_exists(base.PRODUCTION_FORECAST_TABLE):
        base.logger.warning(f"{base.PRODUCTION_FORECAST_TABLE} 不存在，跳过生产预测快照")
        return 0
    cols = set(base.get_columns(base.PRODUCTION_FORECAST_TABLE))
    required = {"SPU", "店铺", "统计日期", "系统预测销量"}
    if not required.issubset(cols):
        base.logger.warning(
            f"{base.PRODUCTION_FORECAST_TABLE} 缺字段 {sorted(required-cols)}，跳过预测快照"
        )
        return 0

    issue_month = base.month_start(snapshot_date)
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT SPU, 店铺, 统计日期, 系统预测销量
            FROM `{base.PRODUCTION_FORECAST_TABLE}`
            WHERE 统计日期 >= %s
              AND 店铺 IN (%s,%s,%s,%s)
        """, (issue_month, *TARGET_SHOPS))
        rows = list(cursor.fetchall())

    f_map: Dict[Tuple[str, str], str] = {}
    if not dry_run and base.table_exists(base.FEATURE_SNAPSHOT_TABLE):
        with db_cursor() as cursor:
            cursor.execute(f"""
                SELECT store_name, spu, forecastability
                FROM `{base.FEATURE_SNAPSHOT_TABLE}`
                WHERE snapshot_date=%s
                  AND store_name IN (%s,%s,%s,%s)
            """, (snapshot_date, *TARGET_SHOPS))
            feature_rows = cursor.fetchall()
        f_map = {
            (base.text(r.get("store_name")), base.text(r.get("spu"))):
                base.text(r.get("forecastability"))
            for r in feature_rows
        }

    payload = []
    for r in rows:
        spu = base.text(r.get("SPU"))
        shop = base.text(r.get("店铺"))
        if shop not in TARGET_SHOP_SET:
            continue
        target = r.get("统计日期")
        if isinstance(target, datetime):
            target = target.date()
        elif not isinstance(target, date):
            target = datetime.strptime(str(target)[:10], "%Y-%m-%d").date()
        h = base.month_diff(issue_month, base.month_start(target))
        if h < 0:
            continue
        qty = int(r.get("系统预测销量") or 0)
        payload.append((
            snapshot_date, spu, shop, base.month_start(target), f"H{h}",
            f_map.get((shop, spu)), "production_current", "legacy_v4_live",
            qty, qty, "LIVE_TABLE_SNAPSHOT", base.PRODUCTION_FORECAST_TABLE,
        ))

    if dry_run:
        base.logger.info(
            f"生产预测快照V4 dry-run准备完成: {len(payload)} 行；"
            "未读取forecast_feature_snapshot_daily"
        )
        return len(payload)

    base.logger.info(f"生产预测快照V4准备完成: {len(payload)} 行")
    if not payload:
        return 0
    sql = f"""
        INSERT INTO `{base.PREDICTION_SNAPSHOT_TABLE}`
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
        for i in range(0, len(payload), base.BATCH_SIZE):
            cursor.executemany(sql, payload[i:i + base.BATCH_SIZE])
    return len(payload)


def install_patch() -> None:
    v3.install_patch()
    base.capture_inventory_snapshot = capture_inventory_snapshot
    base.build_feature_rows = build_feature_rows
    base.snapshot_live_predictions = snapshot_live_predictions


def main() -> int:
    install_patch()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
