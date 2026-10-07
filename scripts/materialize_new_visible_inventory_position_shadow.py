#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Materialize current inventory position for NEW_VISIBLE H48 shadow.

Shadow-only. Uses the same business inventory sources as current production procurement:
- FBA库存明细: FBA可售 + 在途
- 库存预估表: 本地可用量 + 本地待到货

The forecast_inventory_snapshot_daily source is retained only as a diagnostic comparison
and is never added on top of the production inventory sources, preventing double count.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as mon

PRED_TABLE = "forecast_new_visible_h48_prediction_daily"
DEST_TABLE = "forecast_new_visible_inventory_position_daily"
FBA_FALLBACK_TABLE = "FBA库存明细"
LOCAL_TABLE = "库存预估表"
BUILD_VERSION = "NV_INVENTORY_POSITION_V1_PROD_SOURCE_ALIGNED"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def cols(table: str):
    return set(mon.get_columns(table)) if mon.table_exists(table) else set()


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
              snapshot_date DATE NOT NULL,
              store_name VARCHAR(200) NOT NULL,
              spu VARCHAR(200) NOT NULL,

              fba_sellable DECIMAL(18,2) NOT NULL DEFAULT 0,
              fba_inbound DECIMAL(18,2) NOT NULL DEFAULT 0,
              fba_actual_inbound DECIMAL(18,2) DEFAULT NULL,
              local_available DECIMAL(18,2) NOT NULL DEFAULT 0,
              local_pending DECIMAL(18,2) NOT NULL DEFAULT 0,

              on_hand_position DECIMAL(18,2) NOT NULL DEFAULT 0,
              total_inventory_position DECIMAL(18,2) NOT NULL DEFAULT 0,

              prod_fba_rows INT NOT NULL DEFAULT 0,
              local_rows INT NOT NULL DEFAULT 0,
              latest_product_sku_count INT NOT NULL DEFAULT 0,

              diagnostic_snapshot_rows INT NOT NULL DEFAULT 0,
              diagnostic_fulfillable DECIMAL(18,2) NOT NULL DEFAULT 0,
              diagnostic_reserved DECIMAL(18,2) NOT NULL DEFAULT 0,
              diagnostic_transfers DECIMAL(18,2) NOT NULL DEFAULT 0,
              diagnostic_inbound_shipped DECIMAL(18,2) NOT NULL DEFAULT 0,
              diagnostic_inbound_receiving DECIMAL(18,2) NOT NULL DEFAULT 0,

              inventory_usable TINYINT(1) NOT NULL DEFAULT 0,
              source_status VARCHAR(100) NOT NULL,
              build_version VARCHAR(100) NOT NULL,
              materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (snapshot_date,store_name,spu),
              INDEX idx_nv_inv_shop (snapshot_date,store_name),
              INDEX idx_nv_inv_usable (snapshot_date,inventory_usable)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def current_scope():
    if not mon.table_exists(PRED_TABLE):
        raise RuntimeError(f"{PRED_TABLE} does not exist; persist H48 shadow predictions first")
    d = one(f"SELECT MAX(snapshot_date) AS d FROM {PRED_TABLE}").get("d")
    if not d:
        raise RuntimeError(f"{PRED_TABLE} is empty")
    rows = q(
        f"SELECT store_name,spu FROM {PRED_TABLE} WHERE snapshot_date=%s",
        (d,),
    )
    keys = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip())
        for r in rows if r.get("store_name") and r.get("spu")
    }
    return d, sorted(keys)


def product_sku_counts():
    rows = q(
        f"""
        WITH {mon.latest_product_cte()}
        SELECT spu,COUNT(*) AS sku_n
        FROM pm
        WHERE spu IS NOT NULL AND TRIM(spu)<>''
        GROUP BY spu
        """
    )
    return {
        str(r.get("spu") or "").strip(): int(r.get("sku_n", 0) or 0)
        for r in rows
    }


def read_prod_fba():
    if not mon.table_exists(FBA_FALLBACK_TABLE):
        raise RuntimeError(f"{FBA_FALLBACK_TABLE} missing")
    c = cols(FBA_FALLBACK_TABLE)
    required = {"SKU","店铺","FBA可售"}
    if not required.issubset(c):
        raise RuntimeError(f"{FBA_FALLBACK_TABLE} missing columns: {sorted(required-c)}")

    actual_col = "实际在途" if "实际在途" in c else None
    planned_col = "在途" if "在途" in c else None
    # Match the more mature inventory-estimate implementation: actual transit is
    # the canonical inbound quantity when available. Planned/accounting transit is
    # retained only as a diagnostic fallback.
    canonical_col = actual_col or planned_col
    transit_expr = f"SUM(COALESCE(x.{canonical_col},0))" if canonical_col else "0"
    actual_expr = f"SUM(COALESCE(x.{actual_col},0))" if actual_col else "NULL"

    rows = q(
        f"""
        WITH {mon.latest_product_cte()}
        SELECT x.店铺 AS store_name,pm.spu AS spu,
               COUNT(*) AS rows_n,
               SUM(COALESCE(x.FBA可售,0)) AS sellable,
               {transit_expr} AS inbound,
               {actual_expr} AS actual_inbound
        FROM {FBA_FALLBACK_TABLE} x
        LEFT JOIN pm ON pm.sku=x.SKU
        WHERE x.店铺 IS NOT NULL AND x.SKU IS NOT NULL
        GROUP BY x.店铺,pm.spu
        """
    )
    return {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in rows if r.get("spu")
    }


def read_local():
    if not mon.table_exists(LOCAL_TABLE):
        raise RuntimeError(f"{LOCAL_TABLE} missing")
    c = cols(LOCAL_TABLE)
    required = {"SKU","店铺","库存状态","数量"}
    if not required.issubset(c):
        raise RuntimeError(f"{LOCAL_TABLE} missing columns: {sorted(required-c)}")

    rows = q(
        f"""
        WITH {mon.latest_product_cte()}
        SELECT x.店铺 AS store_name,pm.spu AS spu,
               COUNT(*) AS rows_n,
               SUM(CASE WHEN x.库存状态='本地可用量' THEN COALESCE(x.数量,0) ELSE 0 END) AS local_available,
               SUM(CASE WHEN x.库存状态='本地待到货' THEN COALESCE(x.数量,0) ELSE 0 END) AS local_pending
        FROM {LOCAL_TABLE} x
        LEFT JOIN pm ON pm.sku=x.SKU
        WHERE x.店铺 IS NOT NULL AND x.SKU IS NOT NULL
        GROUP BY x.店铺,pm.spu
        """
    )
    return {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in rows if r.get("spu")
    }


def read_diagnostic_snapshot():
    if not mon.table_exists(mon.INVENTORY_SNAPSHOT_TABLE):
        return {}, None
    d = one(f"SELECT MAX(snapshot_date) AS d FROM {mon.INVENTORY_SNAPSHOT_TABLE}").get("d")
    if not d:
        return {}, None
    rows = q(
        f"""
        SELECT store_name,spu,COUNT(*) AS rows_n,
               SUM(afn_fulfillable_quantity) AS fulfillable,
               SUM(afn_reserved_quantity) AS reserved,
               SUM(reserved_fc_transfers) AS transfers,
               SUM(afn_inbound_shipped_quantity) AS inbound_shipped,
               SUM(afn_inbound_receiving_quantity) AS inbound_receiving
        FROM {mon.INVENTORY_SNAPSHOT_TABLE}
        WHERE snapshot_date=%s
          AND spu IS NOT NULL AND TRIM(spu)<>''
        GROUP BY store_name,spu
        """,
        (d,),
    )
    out = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in rows
    }
    return out, d


def main() -> int:
    snapshot_date, keys = current_scope()
    sku_counts = product_sku_counts()
    prod_fba = read_prod_fba()
    local = read_local()
    diag, diag_date = read_diagnostic_snapshot()

    output = []
    status_counts = {}
    for shop, spu in keys:
        f = prod_fba.get((shop,spu), {})
        l = local.get((shop,spu), {})
        d = diag.get((shop,spu), {})
        sku_n = int(sku_counts.get(spu, 0) or 0)

        fba_rows = int(f.get("rows_n", 0) or 0)
        local_rows = int(l.get("rows_n", 0) or 0)

        fba_sellable = float(f.get("sellable", 0) or 0)
        fba_inbound = float(f.get("inbound", 0) or 0)
        actual_inbound = f.get("actual_inbound")
        actual_inbound = None if actual_inbound is None else float(actual_inbound or 0)
        local_available = float(l.get("local_available", 0) or 0)
        local_pending = float(l.get("local_pending", 0) or 0)

        on_hand = fba_sellable + local_available
        total_position = on_hand + fba_inbound + local_pending

        if sku_n <= 0:
            status = "BLOCK_PRODUCT_MAPPING_MISSING"
            usable = 0
        elif fba_rows > 0 or local_rows > 0:
            status = "USABLE_PROD_PROCUREMENT_SOURCES"
            usable = 1
        elif d:
            status = "BLOCK_ONLY_DIAGNOSTIC_SNAPSHOT_PRESENT"
            usable = 0
        else:
            status = "BLOCK_NO_CURRENT_INVENTORY_EVIDENCE"
            usable = 0

        status_counts[status] = status_counts.get(status, 0) + 1
        output.append({
            "snapshot_date": snapshot_date,
            "store_name": shop,
            "spu": spu,
            "fba_sellable": fba_sellable,
            "fba_inbound": fba_inbound,
            "fba_actual_inbound": actual_inbound,
            "local_available": local_available,
            "local_pending": local_pending,
            "on_hand_position": on_hand,
            "total_inventory_position": total_position,
            "prod_fba_rows": fba_rows,
            "local_rows": local_rows,
            "latest_product_sku_count": sku_n,
            "diagnostic_snapshot_rows": int(d.get("rows_n", 0) or 0),
            "diagnostic_fulfillable": float(d.get("fulfillable", 0) or 0),
            "diagnostic_reserved": float(d.get("reserved", 0) or 0),
            "diagnostic_transfers": float(d.get("transfers", 0) or 0),
            "diagnostic_inbound_shipped": float(d.get("inbound_shipped", 0) or 0),
            "diagnostic_inbound_receiving": float(d.get("inbound_receiving", 0) or 0),
            "inventory_usable": usable,
            "source_status": status,
            "build_version": BUILD_VERSION,
        })

    print("NV_INVENTORY_POSITION_SCOPE=" + json.dumps({
        "snapshot_date": str(snapshot_date),
        "diagnostic_snapshot_date": str(diag_date or ""),
        "shop_spu": len(output),
        "usable": sum(x["inventory_usable"] for x in output),
        "usable_rate": round(sum(x["inventory_usable"] for x in output)/len(output),6) if output else None,
        "status_counts": status_counts,
        "source_rule": "FBA可售 + 实际在途(缺失时回退在途) + 本地可用量 + 本地待到货; diagnostic snapshot is comparison only",
    }, ensure_ascii=False))

    totals = {
        "fba_sellable": sum(x["fba_sellable"] for x in output),
        "fba_inbound": sum(x["fba_inbound"] for x in output),
        "local_available": sum(x["local_available"] for x in output),
        "local_pending": sum(x["local_pending"] for x in output),
        "on_hand_position": sum(x["on_hand_position"] for x in output),
        "total_inventory_position": sum(x["total_inventory_position"] for x in output),
    }
    print("NV_INVENTORY_POSITION_TOTALS=" + json.dumps(
        {k: round(v,2) for k,v in totals.items()}, ensure_ascii=False
    ))

    blocked = [x for x in output if not x["inventory_usable"]]
    for x in blocked[:30]:
        print("NV_INVENTORY_POSITION_BLOCKED=" + json.dumps(x, ensure_ascii=False))

    ensure_table()
    cols_out = [
        "snapshot_date","store_name","spu",
        "fba_sellable","fba_inbound","fba_actual_inbound",
        "local_available","local_pending",
        "on_hand_position","total_inventory_position",
        "prod_fba_rows","local_rows","latest_product_sku_count",
        "diagnostic_snapshot_rows","diagnostic_fulfillable","diagnostic_reserved",
        "diagnostic_transfers","diagnostic_inbound_shipped","diagnostic_inbound_receiving",
        "inventory_usable","source_status","build_version",
    ]
    sql = (
        f"INSERT INTO {DEST_TABLE} ({','.join(cols_out)}) VALUES "
        f"({','.join(['%s']*len(cols_out))}) ON DUPLICATE KEY UPDATE "
        + ",".join(
            f"{c}=VALUES({c})"
            for c in cols_out if c not in ("snapshot_date","store_name","spu")
        )
    )
    payload = [tuple(x.get(c) for c in cols_out) for x in output]
    with db_cursor() as c:
        c.executemany(sql, payload)

    print("NV_INVENTORY_POSITION_PERSISTED=" + json.dumps({
        "table": DEST_TABLE,
        "snapshot_date": str(snapshot_date),
        "rows": len(output),
        "blocked": len(blocked),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
