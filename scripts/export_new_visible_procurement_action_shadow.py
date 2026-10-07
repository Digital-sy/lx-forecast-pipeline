#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Export latest NEW_VISIBLE procurement action shadow to human-readable CSV files.

Shadow-only. Reads forecast_new_visible_procurement_action_shadow_daily and writes
UTF-8-SIG CSV/report files under reports_analysis. No production tables are modified.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Sequence

from common.database import db_cursor

SRC = "forecast_new_visible_procurement_action_shadow_daily"
DEFAULT_OUT = "reports_analysis/new_visible_procurement_shadow"

TIER_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "WATCH": 4}

COLUMN_MAP = {
    "snapshot_date": "快照日期",
    "store_name": "店铺",
    "spu": "SPU",
    "age_days": "上新天数",
    "fabric_type": "面料类型",
    "primary_fabric": "主面料",
    "priority_tier": "优先级",
    "h48_risk_level": "H48到仓前风险",
    "h60_coverage_status": "H60库存覆盖状态",
    "q50_gap_low": "Q50缺口下限",
    "q50_gap_high": "Q50缺口上限",
    "q75_gap_low": "Q75缺口下限",
    "q75_gap_high": "Q75缺口上限",
    "action_type": "建议动作类型",
    "action_note": "建议动作说明",
    "normal_po_qty_released": "正常PO数量已释放",
    "h90_qty_released": "H90数量已释放",
}


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(COLUMN_MAP.keys())
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=[COLUMN_MAP[x] for x in fields])
        w.writeheader()
        for r in rows:
            w.writerow({
                COLUMN_MAP[k]: r.get(k)
                for k in fields
            })


def main() -> int:
    d = one(f"SELECT MAX(snapshot_date) AS d FROM {SRC}").get("d")
    if not d:
        raise RuntimeError(f"{SRC} empty")

    rows = q(
        f"SELECT * FROM {SRC} WHERE snapshot_date=%s",
        (d,),
    )
    rows.sort(
        key=lambda r: (
            TIER_ORDER.get(str(r.get("priority_tier") or "WATCH"), 9),
            0 if str(r.get("fabric_type") or "") == "现货面料" else 1,
            -float(r.get("q50_gap_low") or 0),
            -float(r.get("q75_gap_low") or 0),
            str(r.get("store_name") or ""),
            str(r.get("spu") or ""),
        )
    )

    out_dir = Path(DEFAULT_OUT) / str(d)
    out_dir.mkdir(parents=True, exist_ok=True)

    p0 = [r for r in rows if str(r.get("priority_tier")) == "P0"]
    p1 = [r for r in rows if str(r.get("priority_tier")) == "P1"]
    custom = [r for r in rows if str(r.get("fabric_type")) == "定制面料"]
    unknown = [r for r in rows if str(r.get("fabric_type")) == "UNKNOWN"]

    write_csv(out_dir / "01_全部动作队列.csv", rows)
    write_csv(out_dir / "02_P0紧急.csv", p0)
    write_csv(out_dir / "03_P1重点关注.csv", p1)
    write_csv(out_dir / "04_定制面料_H90暂缓.csv", custom)
    write_csv(out_dir / "05_面料映射缺失.csv", unknown)

    tier_counts = Counter(str(r.get("priority_tier") or "WATCH") for r in rows)
    action_counts = Counter(str(r.get("action_type") or "") for r in rows)

    stock_p0 = [
        r for r in p0
        if str(r.get("fabric_type") or "") == "现货面料"
    ]
    summary = {
        "snapshot_date": str(d),
        "rows": len(rows),
        "tier_counts": dict(tier_counts),
        "action_counts": dict(action_counts),
        "stock_p0_rows": len(stock_p0),
        "stock_p0_q50_gap_low_sum": round(
            sum(float(r.get("q50_gap_low") or 0) for r in stock_p0), 2
        ),
        "stock_p0_q50_gap_high_sum": round(
            sum(float(r.get("q50_gap_high") or 0) for r in stock_p0), 2
        ),
        "custom_p0_rows": sum(
            1 for r in p0 if str(r.get("fabric_type") or "") == "定制面料"
        ),
        "unknown_fabric_rows": len(unknown),
        "production_po_written": False,
        "output_dir": str(out_dir),
    }
    (out_dir / "00_汇总.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("PROCUREMENT_ACTION_EXPORT_SCOPE=" + json.dumps(
        summary, ensure_ascii=False
    ))
    for r in p0:
        print("PROCUREMENT_ACTION_EXPORT_P0=" + json.dumps({
            "store_name": r.get("store_name"),
            "spu": r.get("spu"),
            "fabric_type": r.get("fabric_type"),
            "primary_fabric": r.get("primary_fabric"),
            "q50_gap_low": float(r.get("q50_gap_low") or 0),
            "q50_gap_high": float(r.get("q50_gap_high") or 0),
            "action_type": r.get("action_type"),
        }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
