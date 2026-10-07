#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Audit current NEW_VISIBLE inventory coverage gaps.

Read-only. Distinguishes true no-row/zero situations from SKU->SPU mapping gaps and
fallback/local inventory evidence. Never converts an unexplained missing row to zero.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as mon
from jobs.forecast_monitoring.daily_monitor_v4 import TARGET_SHOPS

SPECIAL_EXCLUSION_TABLE = "forecast_special_spu_exclusion"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def table_cols(name: str):
    return set(mon.get_columns(name)) if mon.table_exists(name) else set()


def latest_product_cte() -> str:
    return mon.latest_product_cte()


def current_missing_keys() -> Tuple[Any, Any, List[Tuple[str, str]]]:
    feature_date = one(
        f"SELECT MAX(snapshot_date) AS d FROM {mon.FEATURE_SNAPSHOT_TABLE} "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    ).get("d")
    inv_date = one(
        f"SELECT MAX(snapshot_date) AS d FROM {mon.INVENTORY_SNAPSHOT_TABLE} "
        "WHERE store_name IN (%s,%s,%s,%s)",
        TARGET_SHOPS,
    ).get("d")
    if not feature_date or not inv_date:
        raise RuntimeError("missing feature/inventory snapshot date")

    nv = q(
        f"SELECT store_name,spu FROM {mon.FEATURE_SNAPSHOT_TABLE} "
        "WHERE snapshot_date=%s AND store_name IN (%s,%s,%s,%s) "
        "AND forecastability='NEW_VISIBLE'",
        (feature_date, *TARGET_SHOPS),
    )
    keys = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip())
        for r in nv if r.get("store_name") and r.get("spu")
    }

    excluded = set()
    if mon.table_exists(SPECIAL_EXCLUSION_TABLE):
        ex = q(
            f"SELECT DISTINCT UPPER(TRIM(spu)) AS spu FROM {SPECIAL_EXCLUSION_TABLE} "
            "WHERE exclusion_code IN (%s,%s)",
            ("LCS_SPECIAL_LOW_PRICE", "XH_PREFIX_EXCLUSION"),
        )
        excluded = {str(r.get("spu") or "").strip().upper() for r in ex}
    keys = {
        k for k in keys
        if k[1].upper() not in excluded and not k[1].upper().startswith("XH")
    }

    inv = q(
        f"SELECT DISTINCT store_name,spu FROM {mon.INVENTORY_SNAPSHOT_TABLE} "
        "WHERE snapshot_date=%s AND store_name IN (%s,%s,%s,%s) "
        "AND spu IS NOT NULL AND TRIM(spu)<>''",
        (inv_date, *TARGET_SHOPS),
    )
    inv_keys = {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip())
        for r in inv
    }
    return feature_date, inv_date, sorted(keys - inv_keys)


def raw_fba_by_spu():
    fcols = table_cols(mon.FBA_TABLE)
    if not fcols:
        return {}
    if "msku" in fcols:
        msku = "msku"
    elif "seller_sku" in fcols:
        msku = "seller_sku"
    else:
        msku = None

    if "store_name" in fcols:
        store_expr = "COALESCE(f.store_name,'')"
        store_join = ""
    elif mon.table_exists(mon.STORE_TABLE):
        store_expr = "COALESCE(st.store_name,'')"
        store_join = f"LEFT JOIN {mon.STORE_TABLE} st ON st.sid=f.sid"
    else:
        return {}

    sql = f"""
    WITH {latest_product_cte()}
    SELECT
      {store_expr} AS store_name,
      COALESCE(pm.spu,'') AS spu,
      COUNT(*) AS raw_rows,
      COUNT(DISTINCT f.sku) AS raw_skus,
      SUM(COALESCE(f.afn_fulfillable_quantity,0)) AS fulfillable,
      SUM(COALESCE(f.afn_reserved_quantity,0)) AS reserved,
      SUM(COALESCE(f.reserved_fc_transfers,0)) AS transfers,
      SUM(COALESCE(f.afn_inbound_shipped_quantity,0)) AS inbound_shipped,
      SUM(COALESCE(f.afn_inbound_receiving_quantity,0)) AS inbound_receiving
    FROM {mon.FBA_TABLE} f
    LEFT JOIN pm ON pm.sku=f.sku
    {store_join}
    WHERE {store_expr} IN (%s,%s,%s,%s)
    GROUP BY {store_expr}, COALESCE(pm.spu,'')
    """
    rows = q(sql, TARGET_SHOPS)
    return {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in rows
    }


def product_skus_by_spu():
    rows = q(
        f"""
        WITH {latest_product_cte()}
        SELECT spu, COUNT(*) AS sku_n
        FROM pm
        WHERE spu IS NOT NULL AND TRIM(spu)<>''
        GROUP BY spu
        """
    )
    return {
        str(r.get("spu") or "").strip(): int(r.get("sku_n", 0) or 0)
        for r in rows
    }


def fallback_fba():
    table = "FBA库存明细"
    if not mon.table_exists(table):
        return {}
    c = table_cols(table)
    if not {"SKU","店铺"}.issubset(c):
        return {}
    sale_col = "FBA可售" if "FBA可售" in c else None
    transit_col = "实际在途" if "实际在途" in c else ("在途" if "在途" in c else None)
    if not sale_col and not transit_col:
        return {}
    sale_expr = f"SUM(COALESCE(x.{sale_col},0))" if sale_col else "0"
    transit_expr = f"SUM(COALESCE(x.{transit_col},0))" if transit_col else "0"
    rows = q(
        f"""
        WITH {latest_product_cte()}
        SELECT x.店铺 AS store_name, pm.spu AS spu,
               COUNT(*) AS rows_n,
               {sale_expr} AS fba_sellable,
               {transit_expr} AS fba_transit
        FROM {table} x
        LEFT JOIN pm ON pm.sku=x.SKU
        WHERE x.店铺 IN (%s,%s,%s,%s)
        GROUP BY x.店铺, pm.spu
        """,
        TARGET_SHOPS,
    )
    return {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in rows if r.get("spu")
    }


def local_inventory():
    table = "库存预估表"
    if not mon.table_exists(table):
        return {}
    c = table_cols(table)
    if not {"SKU","店铺","库存状态","数量"}.issubset(c):
        return {}
    rows = q(
        f"""
        WITH {latest_product_cte()}
        SELECT x.店铺 AS store_name, pm.spu AS spu,
               COUNT(*) AS rows_n,
               SUM(CASE WHEN x.库存状态='本地可用量' THEN COALESCE(x.数量,0) ELSE 0 END) AS local_available,
               SUM(CASE WHEN x.库存状态='本地待到货' THEN COALESCE(x.数量,0) ELSE 0 END) AS local_pending
        FROM {table} x
        LEFT JOIN pm ON pm.sku=x.SKU
        WHERE x.店铺 IN (%s,%s,%s,%s)
        GROUP BY x.店铺, pm.spu
        """,
        TARGET_SHOPS,
    )
    return {
        (str(r.get("store_name") or "").strip(), str(r.get("spu") or "").strip()): r
        for r in rows if r.get("spu")
    }


def main() -> int:
    feature_date, inv_date, missing = current_missing_keys()
    raw = raw_fba_by_spu()
    sku_counts = product_skus_by_spu()
    fba2 = fallback_fba()
    local = local_inventory()

    detail = []
    counts = {}
    for shop, spu in missing:
        r = raw.get((shop, spu))
        f = fba2.get((shop, spu))
        l = local.get((shop, spu))
        mapped_skus = int(sku_counts.get(spu, 0))

        if r:
            status = "RAW_FBA_PRESENT_BUT_SNAPSHOT_MISSING"
        elif f or l:
            status = "FALLBACK_OR_LOCAL_PRESENT"
        elif mapped_skus <= 0:
            status = "PRODUCT_SKU_MAPPING_MISSING"
        else:
            status = "NO_FBA_OR_LOCAL_ROW_WITH_VALID_PRODUCT_MAPPING"

        counts[status] = counts.get(status, 0) + 1
        detail.append({
            "store_name": shop,
            "spu": spu,
            "status": status,
            "latest_product_sku_count": mapped_skus,
            "raw_fba": {
                "rows": int((r or {}).get("raw_rows", 0) or 0),
                "fulfillable": float((r or {}).get("fulfillable", 0) or 0),
                "reserved": float((r or {}).get("reserved", 0) or 0),
                "transfers": float((r or {}).get("transfers", 0) or 0),
                "inbound_shipped": float((r or {}).get("inbound_shipped", 0) or 0),
                "inbound_receiving": float((r or {}).get("inbound_receiving", 0) or 0),
            },
            "fallback_fba": {
                "rows": int((f or {}).get("rows_n", 0) or 0),
                "sellable": float((f or {}).get("fba_sellable", 0) or 0),
                "transit": float((f or {}).get("fba_transit", 0) or 0),
            },
            "local": {
                "rows": int((l or {}).get("rows_n", 0) or 0),
                "available": float((l or {}).get("local_available", 0) or 0),
                "pending": float((l or {}).get("local_pending", 0) or 0),
            },
        })

    print("NV_INVENTORY_GAP_SCOPE=" + json.dumps({
        "feature_snapshot_date": str(feature_date),
        "inventory_snapshot_date": str(inv_date),
        "missing_shop_spu": len(missing),
        "status_counts": counts,
        "fallback_fba_table_exists": mon.table_exists("FBA库存明细"),
        "local_inventory_table_exists": mon.table_exists("库存预估表"),
        "no_db_write": True,
    }, ensure_ascii=False))
    for row in detail:
        print("NV_INVENTORY_GAP_DETAIL=" + json.dumps(row, ensure_ascii=False))

    unsafe = [
        r for r in detail
        if r["status"] in (
            "RAW_FBA_PRESENT_BUT_SNAPSHOT_MISSING",
            "PRODUCT_SKU_MAPPING_MISSING",
        )
    ]
    print("NV_INVENTORY_GAP_DECISION=" + json.dumps({
        "safe_to_treat_all_missing_as_zero": False,
        "unsafe_mapping_or_snapshot_gaps": len(unsafe),
        "zero_candidate_count": sum(
            r["status"] == "NO_FBA_OR_LOCAL_ROW_WITH_VALID_PRODUCT_MAPPING"
            for r in detail
        ),
        "rule": (
            "Only NO_FBA_OR_LOCAL_ROW_WITH_VALID_PRODUCT_MAPPING may become an explicit "
            "zero-inventory candidate after business approval; mapping/snapshot gaps remain blocked."
        ),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
