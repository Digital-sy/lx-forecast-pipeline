#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Compare Sep-1 NEW_VISIBLE Q50 recommendation with actual Lingxing purchase orders.

Main output:
店铺 | SPU | 建议下单Q50 | 9月实际下单量 | 0815-1007实际下单量

Actual quantity source:
采购单.实际数量, grouped by parsed SPU + 店铺 + 创建时间.
Voided orders are excluded defensively.
Read-only against DB. Writes CSV files only.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd

from common.database import db_cursor
from jobs.feishu.generate_order_comparison import extract_spu_from_sku
from utils import normalize_shop_name


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def load_recommendations(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"recommendation csv missing: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    required = {"store_name", "spu", "base_order_qty_q50"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"recommendation csv missing columns: {sorted(missing)}")

    out = df[["store_name", "spu", "base_order_qty_q50"]].copy()
    out["store_name"] = out["store_name"].astype(str).map(normalize_shop_name).str.strip()
    out["spu"] = out["spu"].astype(str).str.strip()
    out["base_order_qty_q50"] = pd.to_numeric(
        out["base_order_qty_q50"], errors="coerce"
    ).fillna(0).round().astype(int)

    return (
        out.groupby(["store_name", "spu"], as_index=False)["base_order_qty_q50"]
        .sum()
    )


def load_purchase_rows(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    return q(
        """
        SELECT
          订单号,
          SKU,
          店铺,
          实际数量,
          创建时间,
          状态
        FROM 采购单
        WHERE 创建时间 IS NOT NULL
          AND DATE(创建时间) BETWEEN %s AND %s
          AND SKU IS NOT NULL
          AND TRIM(SKU)<>''
          AND 店铺 IS NOT NULL
          AND TRIM(店铺)<>''
          AND 实际数量 IS NOT NULL
          AND 实际数量 > 0
          AND COALESCE(状态,'') <> '已作废'
        ORDER BY 创建时间,订单号,SKU
        """,
        (start_date, end_date),
    )


def parse_date(v: Any):
    if isinstance(v, datetime):
        return v.date()
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--recommendation-csv",
        default=(
            "reports_analysis/new_visible_historical_replay/2026-09-01/"
            "NEW_VISIBLE_0901历史回放_需下单.csv"
        ),
    )
    ap.add_argument("--sep-start", default="2026-09-01")
    ap.add_argument("--sep-end", default="2026-09-30")
    ap.add_argument("--wide-start", default="2026-08-15")
    ap.add_argument("--wide-end", default="2026-10-07")
    ap.add_argument(
        "--output-dir",
        default="reports_analysis/new_visible_historical_replay/2026-09-01",
    )
    args = ap.parse_args()

    rec = load_recommendations(Path(args.recommendation_csv))
    rec_keys = set(zip(rec["store_name"], rec["spu"]))

    table_scope = one(
        """
        SELECT
          MIN(DATE(创建时间)) AS min_date,
          MAX(DATE(创建时间)) AS max_date,
          COUNT(*) AS rows_n
        FROM 采购单
        WHERE 创建时间 IS NOT NULL
        """
    )

    po_rows = load_purchase_rows(args.wide_start, args.wide_end)

    sep_start = datetime.strptime(args.sep_start, "%Y-%m-%d").date()
    sep_end = datetime.strptime(args.sep_end, "%Y-%m-%d").date()

    wide_qty = defaultdict(int)
    sep_qty = defaultdict(int)
    matched_detail: List[Dict[str, Any]] = []
    unparsed_spu_rows = 0

    for r in po_rows:
        sku = str(r.get("SKU") or "").strip()
        shop = normalize_shop_name(str(r.get("店铺") or "").strip())
        spu = extract_spu_from_sku(sku).strip()
        qty = int(float(r.get("实际数量") or 0))
        created = parse_date(r.get("创建时间"))

        if not spu:
            unparsed_spu_rows += 1
            continue

        key = (shop, spu)
        wide_qty[key] += qty
        if sep_start <= created <= sep_end:
            sep_qty[key] += qty

        if key in rec_keys:
            matched_detail.append({
                "订单号": r.get("订单号"),
                "创建日期": str(created),
                "店铺": shop,
                "SKU": sku,
                "SPU": spu,
                "实际数量": qty,
                "状态": r.get("状态") or "",
                "是否9月": "是" if sep_start <= created <= sep_end else "否",
            })

    main_rows: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    for r in rec.to_dict("records"):
        key = (r["store_name"], r["spu"])
        q50 = int(r["base_order_qty_q50"])
        sep = int(sep_qty.get(key, 0))
        wide = int(wide_qty.get(key, 0))

        main_rows.append({
            "店铺": r["store_name"],
            "SPU": r["spu"],
            "建议下单Q50": q50,
            "9月实际下单量": sep,
            "0815-1007实际下单量": wide,
        })
        audit_rows.append({
            "店铺": r["store_name"],
            "SPU": r["spu"],
            "建议下单Q50": q50,
            "9月实际下单量": sep,
            "9月较Q50差额": sep - q50,
            "9月完成率": round(sep / q50, 4) if q50 else None,
            "0815-1007实际下单量": wide,
            "0815-1007较Q50差额": wide - q50,
            "0815-1007完成率": round(wide / q50, 4) if q50 else None,
        })

    main_rows.sort(key=lambda x: (-x["建议下单Q50"], x["店铺"], x["SPU"]))
    audit_rows.sort(key=lambda x: (-x["建议下单Q50"], x["店铺"], x["SPU"]))
    matched_detail.sort(
        key=lambda x: (x["创建日期"], x["店铺"], x["SPU"], str(x["订单号"]))
    )

    out_dir = Path(args.output_dir)
    main_path = out_dir / "NEW_VISIBLE_Q50_vs_领星实际下单_主表.csv"
    audit_path = out_dir / "NEW_VISIBLE_Q50_vs_领星实际下单_差异分析.csv"
    detail_path = out_dir / "NEW_VISIBLE_Q50_vs_领星实际下单_采购单明细.csv"

    write_csv(
        main_path,
        ["店铺", "SPU", "建议下单Q50", "9月实际下单量", "0815-1007实际下单量"],
        main_rows,
    )
    write_csv(
        audit_path,
        [
            "店铺", "SPU", "建议下单Q50",
            "9月实际下单量", "9月较Q50差额", "9月完成率",
            "0815-1007实际下单量", "0815-1007较Q50差额", "0815-1007完成率",
        ],
        audit_rows,
    )
    write_csv(
        detail_path,
        ["订单号", "创建日期", "店铺", "SKU", "SPU", "实际数量", "状态", "是否9月"],
        matched_detail,
    )

    summary = {
        "recommendation_rows": len(main_rows),
        "recommendation_q50_sum": sum(x["建议下单Q50"] for x in main_rows),
        "sep_actual_sum": sum(x["9月实际下单量"] for x in main_rows),
        "wide_actual_sum": sum(x["0815-1007实际下单量"] for x in main_rows),
        "sep_with_actual_rows": sum(x["9月实际下单量"] > 0 for x in main_rows),
        "wide_with_actual_rows": sum(x["0815-1007实际下单量"] > 0 for x in main_rows),
        "purchase_table_min_date": str(table_scope.get("min_date") or ""),
        "purchase_table_max_date": str(table_scope.get("max_date") or ""),
        "purchase_table_rows": int(table_scope.get("rows_n", 0) or 0),
        "purchase_rows_in_wide_window": len(po_rows),
        "matched_purchase_detail_rows": len(matched_detail),
        "unparsed_purchase_spu_rows": unparsed_spu_rows,
        "sep_window": f"{args.sep_start}..{args.sep_end}",
        "wide_window": f"{args.wide_start}..{args.wide_end}",
        "main_csv": str(main_path),
        "audit_csv": str(audit_path),
        "detail_csv": str(detail_path),
    }

    print("Q50_ACTUAL_ORDER_COMPARE_SCOPE=" + json.dumps(summary, ensure_ascii=False))
    for x in main_rows:
        print("Q50_ACTUAL_ORDER_COMPARE_ROW=" + json.dumps(x, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
