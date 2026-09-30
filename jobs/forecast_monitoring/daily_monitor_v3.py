#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Forecast daily shadow monitoring V3.

V3 keeps V2 freshness selection and adds two deployment-safety fixes discovered by
source audit on 2026-09-30:
1. FBA source uses `msku` (not `seller_sku`) on the current schema. Inventory snapshot
   now auto-detects either field and uses the source table's own store_name when present.
2. NEW_VISIBLE lifecycle is evaluated at store+SPU level. A SPU sold earlier in another
   shop must not make a later launch in the current shop look ESTABLISHED.

Still shadow-only: writes only forecast_* tables; never modifies production forecast or
procurement tables.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base
from jobs.forecast_monitoring import daily_monitor_v2 as v2

_ORIGINAL_BUILD_FEATURE_ROWS = base.build_feature_rows


def capture_inventory_snapshot(snapshot_date: date, dry_run: bool = False) -> int:
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

    # Current audited FBA schema already contains store_name. Prefer the source value;
    # only fall back to store-list mapping for older schemas.
    if "store_name" in fba_cols:
        store_expr = "COALESCE(f.store_name,'')"
        store_join = ""
    else:
        store_expr = "COALESCE(st.store_name,'')"
        store_join = f"LEFT JOIN {base.STORE_TABLE} st ON st.sid=f.sid" if base.table_exists(base.STORE_TABLE) else ""
        if not store_join:
            store_expr = "''"

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
    """
    with db_cursor() as cursor:
        cursor.execute(sql)
        rows = list(cursor.fetchall())

    payload = []
    for r in rows:
        fulfillable = base.num(r.get("fulfillable"))
        reserved = base.num(r.get("reserved"))
        transfers = base.num(r.get("transfers"))
        inbound_shipped = base.num(r.get("inbound_shipped"))
        inbound_receiving = base.num(r.get("inbound_receiving"))

        # Keep the project's currently approved inventory formulas. Store components so
        # historical snapshots can be recomputed later if semantics are revised.
        total_inventory = fulfillable + reserved + transfers + inbound_shipped + inbound_receiving
        available_inventory = fulfillable + reserved + transfers + inbound_receiving
        key = base.md5_key([
            snapshot_date, r.get("sid"), r.get("asin"), r.get("fnsku"),
            r.get("msku"), r.get("sku"),
        ])
        payload.append((
            key, snapshot_date, base.text(r.get("sid")), base.text(r.get("store_name")),
            base.text(r.get("asin")), base.text(r.get("fnsku")), base.text(r.get("msku")),
            base.text(r.get("sku")), base.text(r.get("spu")), r.get("product_category"),
            r.get("develop_year"), r.get("season"), fulfillable, reserved, transfers,
            inbound_shipped, inbound_receiving, total_inventory, available_inventory,
        ))

    base.logger.info(
        f"库存快照V3准备完成: {len(payload)} 行; msku_source={msku_col}; "
        f"store_source={'FBA.store_name' if 'store_name' in fba_cols else 'store_list'}"
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


def load_first_sale_by_store_spu() -> Dict[Tuple[str, str], date]:
    """First positive-sale month at shop+SPU grain using existing monthly history."""
    if not base.table_exists(base.MONTHLY_SALES_TABLE):
        base.logger.warning(f"{base.MONTHLY_SALES_TABLE} 不存在，店铺首销将无法识别")
        return {}
    cols = set(base.get_columns(base.MONTHLY_SALES_TABLE))
    required = {"店铺", "SPU", "统计日期", "销量"}
    if not required.issubset(cols):
        base.logger.warning(
            f"{base.MONTHLY_SALES_TABLE} 缺字段 {sorted(required-cols)}，店铺首销将无法识别"
        )
        return {}

    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT `店铺`, `SPU`, MIN(`统计日期`) AS first_sale
            FROM `{base.MONTHLY_SALES_TABLE}`
            WHERE `店铺` IS NOT NULL AND TRIM(`店铺`)<>''
              AND `SPU` IS NOT NULL AND TRIM(`SPU`)<>''
              AND COALESCE(`销量`,0)>0
            GROUP BY `店铺`, `SPU`
        """)
        rows = cursor.fetchall()

    out: Dict[Tuple[str, str], date] = {}
    for r in rows:
        shop = base.text(r.get("店铺"))
        spu = base.text(r.get("SPU"))
        d = r.get("first_sale")
        if not shop or not spu or d is None:
            continue
        if isinstance(d, datetime):
            d = d.date()
        elif not isinstance(d, date):
            d = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
        out[(shop, spu)] = d
    return out


def build_feature_rows(snapshot_date: date):
    rows, source = _ORIGINAL_BUILD_FEATURE_ROWS(snapshot_date)
    first_sale = load_first_sale_by_store_spu()
    as_of = source["as_of_date"]
    changed = 0
    unknown = 0

    for r in rows:
        key = (base.text(r.get("store_name")), base.text(r.get("spu")))
        fs = first_sale.get(key)
        old_fs = r.get("first_sale_date")
        old_cls = base.text(r.get("forecastability"))

        if fs is None:
            r["first_sale_date"] = None
            r["months_since_first_sale"] = None
            r["forecastability"] = "UNKNOWN"
            unknown += 1
        else:
            age_months = base.month_diff(base.month_start(fs), base.month_start(as_of))
            r["first_sale_date"] = fs
            r["months_since_first_sale"] = age_months
            r["forecastability"] = "NEW_VISIBLE" if age_months <= 3 else "ESTABLISHED"

        if r.get("first_sale_date") != old_fs or base.text(r.get("forecastability")) != old_cls:
            changed += 1

    base.logger.info(
        f"店铺级首销V3修正完成: rows={len(rows)}, changed={changed}, unknown={unknown}"
    )
    return rows, source


def install_patch() -> None:
    v2.install_patch()  # freshness selector
    base.capture_inventory_snapshot = capture_inventory_snapshot
    base.build_feature_rows = build_feature_rows


def main() -> int:
    install_patch()
    return base.main()


if __name__ == "__main__":
    raise SystemExit(main())
