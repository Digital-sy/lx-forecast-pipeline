#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Inspect Amazon Category Insights year-over-year series for one Browse Node.

Read-only diagnostic. Purpose: determine whether pr_ye/pv_ye contain two aligned
monthly year curves that can extend the l12m history for strict forecast experiments.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor

SCHEMA = "amazon_category_insights"
TABLE = "performance_series"


def fetch_all(sql, params=()):
    with db_cursor(dictionary=True) as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_$]+", name):
        raise ValueError(name)
    return f"`{name}`"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="1044544")
    ap.add_argument("--schema", default=SCHEMA)
    ap.add_argument("--table", default=TABLE)
    args = ap.parse_args()

    sq = ident(args.schema)
    tq = ident(args.table)
    like = f"%_{args.node}_%"

    paths = [
        "demand.yearOnYearUnitSold.pr_ye",
        "demand.yearOnYearUnitSold.pv_ye",
        "demand.unitSoldYOY.pr_ye",
        "demand.unitSoldYOY.pv_ye",
        "demand.yearOnYearGlanceViews.pr_ye",
        "demand.yearOnYearGlanceViews.pv_ye",
        "demand.clickCount.l12m",
        "demand.unitSold.mly",
        "demand.unitSold.l12m",
    ]
    ph = ",".join(["%s"] * len(paths))
    rows = fetch_all(
        f"""
        SELECT id,batch_id,node_id,source_row,retrieved_at,metric_path,range_key,
               series_id,point_label,value_num,value_text,unit
        FROM {sq}.{tq}
        WHERE series_id LIKE %s
          AND metric_path IN ({ph})
        ORDER BY metric_path,retrieved_at,batch_id,source_row,id
        """,
        [like] + paths,
    )

    print(f"=== Browse Node {args.node}: selected series ===")
    current = None
    for r in rows:
        key = (r.get("metric_path"), r.get("batch_id"), r.get("retrieved_at"), r.get("series_id"))
        if key != current:
            current = key
            print("\n---", {
                "metric_path": r.get("metric_path"),
                "batch_id": r.get("batch_id"),
                "retrieved_at": r.get("retrieved_at"),
                "series_id": r.get("series_id"),
                "unit": r.get("unit"),
            })
        print({
            "source_row": r.get("source_row"),
            "point_label": r.get("point_label"),
            "value_num": r.get("value_num"),
            "value_text": r.get("value_text"),
        })

    print("\n判定重点：")
    print("1) pr_ye / pv_ye 的 point_label 是否为同一组月份；")
    print("2) 两条 yearOnYearUnitSold 是否分别代表当前年/上一年绝对销量；")
    print("3) unitSoldYOY 是否是比率/百分比，而非绝对销量；")
    print("4) clickCount.l12m 是否为13个月月度点击量。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
