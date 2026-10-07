#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Build NEW_VISIBLE procurement action queue from H48 risk + H60 fabric shadow.

Shadow-only. No production PO writes.

Rules:
- stock fabric:
  P0 -> emergency lead-time risk + H60 replenishment range
  P1 -> verify inbound / replenish range
  P2 -> safety-stock review
  P3 -> covered / observe
- custom fabric:
  P0/P1 -> urgent manual action, but NO H90 quantity while H90 remains research-only
  P2/P3 -> observe / hold
- unknown fabric -> block
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from common.database import db_cursor

SRC = "forecast_new_visible_h60_fabric_shadow_daily"
DEST = "forecast_new_visible_procurement_action_shadow_daily"
BUILD_VERSION = "NV_PROCUREMENT_ACTION_SHADOW_V1"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST} (
          snapshot_date DATE NOT NULL,
          store_name VARCHAR(200) NOT NULL,
          spu VARCHAR(200) NOT NULL,
          age_days INT NOT NULL,
          fabric_type VARCHAR(30) NOT NULL,
          primary_fabric VARCHAR(255) NOT NULL DEFAULT '',
          priority_tier VARCHAR(20) NOT NULL,
          h48_risk_level VARCHAR(50) NOT NULL DEFAULT '',
          h60_coverage_status VARCHAR(50) NOT NULL DEFAULT '',
          q50_gap_low DECIMAL(18,2) NOT NULL DEFAULT 0,
          q50_gap_high DECIMAL(18,2) NOT NULL DEFAULT 0,
          q75_gap_low DECIMAL(18,2) NOT NULL DEFAULT 0,
          q75_gap_high DECIMAL(18,2) NOT NULL DEFAULT 0,
          action_type VARCHAR(80) NOT NULL,
          action_note VARCHAR(500) NOT NULL,
          normal_po_qty_released TINYINT(1) NOT NULL DEFAULT 0,
          h90_qty_released TINYINT(1) NOT NULL DEFAULT 0,
          build_version VARCHAR(100) NOT NULL,
          materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (snapshot_date,store_name,spu),
          INDEX idx_nv_action (snapshot_date,priority_tier,action_type),
          INDEX idx_nv_action_fabric (snapshot_date,fabric_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)


def action_for(row: Dict[str, Any]):
    fabric = str(row.get("fabric_type") or "")
    tier = str(row.get("priority_tier") or "WATCH")
    h48 = str(row.get("h48_risk_level") or "")
    h60 = str(row.get("h60_coverage_status") or "")

    if fabric == "UNKNOWN":
        return (
            "BLOCK_FABRIC_MAPPING",
            "面料类型缺失，先修复面料核价/定制面料映射；不释放任何采购数量。",
        )

    if fabric == "现货面料":
        if tier == "P0":
            return (
                "STOCK_P0_EMERGENCY_AND_REPLENISH_SHADOW",
                "H48/H60均存在高风险：先查调拨/加急/催在途，同时保留H60 Q50/Q75缺口区间供补货shadow；不自动下单。",
            )
        if tier == "P1":
            return (
                "STOCK_P1_VERIFY_INBOUND_AND_REPLENISH_SHADOW",
                "核实实际在途ETA，并参考H60缺口区间做正常补货shadow；不自动下单。",
            )
        if tier == "P2":
            return (
                "STOCK_P2_SAFETY_REVIEW",
                "基础需求大体覆盖，重点评估Q75安全缺口与在途时效。",
            )
        if tier == "P3":
            return (
                "STOCK_P3_COVERED_OBSERVE",
                "当前库存结构覆盖较好，继续观察。",
            )
        return ("STOCK_WATCH", "仅观察。")

    if fabric == "定制面料":
        if tier == "P0":
            return (
                "CUSTOM_P0_EMERGENCY_NO_H90_QTY",
                "H48已是紧急缺货风险；立即人工处理调拨/加急/现有面料库存，但H90尚未验证，不释放3个月正常采购量。",
            )
        if tier == "P1":
            return (
                "CUSTOM_P1_URGENT_REVIEW_NO_H90_QTY",
                "定制面料存在明显风险；优先核实在途/面料现存/生产状态，H90未成熟前不释放正常采购量。",
            )
        if tier == "P2":
            return (
                "CUSTOM_P2_HOLD_H90",
                "继续观察；等待H90研究成熟后再进入定制面料正常采购量。",
            )
        return (
            "CUSTOM_P3_HOLD_H90",
            "库存较安全；继续H90研究观察。",
        )

    return ("BLOCK_FABRIC_TYPE_OTHER", "未知面料类型，阻断。")


def main() -> int:
    d = one(f"SELECT MAX(snapshot_date) AS d FROM {SRC}").get("d")
    if not d:
        raise RuntimeError(f"{SRC} is empty")

    rows = q(f"SELECT * FROM {SRC} WHERE snapshot_date=%s", (d,))
    out = []
    counts = {}
    tier_counts = {}
    stock_p0_q50_low = 0.0
    stock_p0_q50_high = 0.0

    for r in rows:
        action_type, note = action_for(r)
        counts[action_type] = counts.get(action_type, 0) + 1
        tier = str(r.get("priority_tier") or "WATCH")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

        if action_type == "STOCK_P0_EMERGENCY_AND_REPLENISH_SHADOW":
            stock_p0_q50_low += float(r.get("q50_gap_low") or 0)
            stock_p0_q50_high += float(r.get("q50_gap_high") or 0)

        out.append({
            "snapshot_date": r["snapshot_date"],
            "store_name": r["store_name"],
            "spu": r["spu"],
            "age_days": int(r.get("age_days", 0) or 0),
            "fabric_type": str(r.get("fabric_type") or ""),
            "primary_fabric": str(r.get("primary_fabric") or ""),
            "priority_tier": tier,
            "h48_risk_level": str(r.get("h48_risk_level") or ""),
            "h60_coverage_status": str(r.get("h60_coverage_status") or ""),
            "q50_gap_low": float(r.get("q50_gap_low") or 0),
            "q50_gap_high": float(r.get("q50_gap_high") or 0),
            "q75_gap_low": float(r.get("q75_gap_low") or 0),
            "q75_gap_high": float(r.get("q75_gap_high") or 0),
            "action_type": action_type,
            "action_note": note,
            "normal_po_qty_released": 0,
            "h90_qty_released": 0,
            "build_version": BUILD_VERSION,
        })

    print("PROCUREMENT_ACTION_SCOPE=" + json.dumps({
        "snapshot_date": str(d),
        "rows": len(out),
        "tier_counts": tier_counts,
        "action_counts": counts,
        "stock_p0_q50_gap_low_sum": round(stock_p0_q50_low, 2),
        "stock_p0_q50_gap_high_sum": round(stock_p0_q50_high, 2),
        "production_po_written": False,
        "h90_qty_released": False,
    }, ensure_ascii=False))

    rank = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "WATCH": 4}
    ordered = sorted(
        out,
        key=lambda x: (
            rank.get(x["priority_tier"], 9),
            0 if x["fabric_type"] == "现货面料" else 1,
            -x["q50_gap_low"],
            -x["q75_gap_low"],
        )
    )
    for x in ordered[:50]:
        print("PROCUREMENT_ACTION_TOP=" + json.dumps(
            x, ensure_ascii=False, default=str
        ))

    ensure_table()
    if out:
        cols = list(out[0].keys())
        sql = (
            f"INSERT INTO {DEST} ({','.join(cols)}) VALUES "
            f"({','.join(['%s']*len(cols))}) ON DUPLICATE KEY UPDATE "
            + ",".join(
                f"{c}=VALUES({c})"
                for c in cols if c not in ("snapshot_date","store_name","spu")
            )
        )
        payload = [tuple(x.get(c) for c in cols) for x in out]
        with db_cursor() as c:
            c.executemany(sql, payload)

    print("PROCUREMENT_ACTION_PERSISTED=" + json.dumps({
        "table": DEST,
        "snapshot_date": str(d),
        "rows": len(out),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
