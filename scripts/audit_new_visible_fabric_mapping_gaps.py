#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Audit UNKNOWN fabric mappings in NEW_VISIBLE shadow.

Read-only. Explains whether an UNKNOWN SPU is missing from 面料核价表,
has only blank/invalid fabric rows, or has another normalization issue.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from common.database import db_cursor

SRC = "forecast_new_visible_h60_fabric_shadow_daily"
COST_TABLE = "面料核价表"
CUSTOM_TABLE = "定制面料参数"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def table_exists(name: str) -> bool:
    r = one(
        "SELECT COUNT(*) AS n FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
        (name,),
    )
    return int(r.get("n", 0) or 0) > 0


def main() -> int:
    if not table_exists(SRC):
        raise RuntimeError(f"{SRC} missing")
    d = one(f"SELECT MAX(snapshot_date) AS d FROM {SRC}").get("d")
    if not d:
        raise RuntimeError(f"{SRC} empty")

    unknown = q(
        f"""
        SELECT snapshot_date,store_name,spu,priority_tier,
               h60_coverage_status,h48_risk_level
        FROM {SRC}
        WHERE snapshot_date=%s AND fabric_type='UNKNOWN'
        ORDER BY priority_tier,store_name,spu
        """,
        (d,),
    )

    custom_fabrics = set()
    if table_exists(CUSTOM_TABLE):
        rows = q(
            f"SELECT TRIM(面料) AS fabric FROM {CUSTOM_TABLE} "
            "WHERE 面料 IS NOT NULL AND TRIM(面料)<>''"
        )
        custom_fabrics = {
            str(r.get("fabric") or "").strip()
            for r in rows
            if r.get("fabric")
        }

    status_counts = {}
    for x in unknown:
        spu = str(x.get("spu") or "").strip().upper()

        raw_rows = []
        if table_exists(COST_TABLE):
            raw_rows = q(
                f"""
                SELECT SPU,面料,
                       COALESCE(单件用量,0) AS 单件用量,
                       COALESCE(单件损耗,1) AS 单件损耗
                FROM {COST_TABLE}
                WHERE UPPER(TRIM(SPU))=%s
                ORDER BY COALESCE(单件用量,0)*COALESCE(单件损耗,1) DESC
                """,
                (spu,),
            )

        valid = [
            r for r in raw_rows
            if str(r.get("面料") or "").strip()
        ]

        if not raw_rows:
            reason = "NO_FABRIC_COSTING_ROW"
        elif not valid:
            reason = "FABRIC_COSTING_ROWS_BUT_FABRIC_BLANK"
        else:
            reason = "VALID_FABRIC_ROW_EXISTS_BUT_CLASSIFIER_MISSED"

        status_counts[reason] = status_counts.get(reason, 0) + 1

        detail = {
            "snapshot_date": str(d),
            "store_name": x.get("store_name"),
            "spu": spu,
            "priority_tier": x.get("priority_tier"),
            "h60_coverage_status": x.get("h60_coverage_status"),
            "h48_risk_level": x.get("h48_risk_level"),
            "reason": reason,
            "fabric_costing_row_count": len(raw_rows),
            "valid_fabric_row_count": len(valid),
            "rows": [
                {
                    "SPU": r.get("SPU"),
                    "面料": r.get("面料"),
                    "单件用量": float(r.get("单件用量", 0) or 0),
                    "单件损耗": float(r.get("单件损耗", 1) or 1),
                    "is_custom_fabric": str(r.get("面料") or "").strip() in custom_fabrics,
                }
                for r in raw_rows[:20]
            ],
        }
        print("FABRIC_MAPPING_GAP_DETAIL=" + json.dumps(
            detail, ensure_ascii=False, default=str
        ))

    print("FABRIC_MAPPING_GAP_SCOPE=" + json.dumps({
        "snapshot_date": str(d),
        "unknown_rows": len(unknown),
        "reason_counts": status_counts,
        "read_only": True,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
