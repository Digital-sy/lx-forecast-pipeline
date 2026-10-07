#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Send NEW_VISIBLE procurement shadow summary to Feishu.

Manual/test first. Does not change procurement tables or create orders.
"""
from __future__ import annotations

import argparse
from typing import Any, Dict, List, Sequence

from common.database import db_cursor
from scripts.notify_feishu import send_notify

SRC = "forecast_new_visible_procurement_action_shadow_daily"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    d = one(f"SELECT MAX(snapshot_date) AS d FROM {SRC}").get("d")
    if not d:
        raise RuntimeError(f"{SRC} empty")

    rows = q(
        f"""
        SELECT *
        FROM {SRC}
        WHERE snapshot_date=%s
        ORDER BY
          CASE priority_tier
            WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2
            WHEN 'P3' THEN 3 ELSE 4 END,
          store_name,spu
        """,
        (d,),
    )

    def cnt(tier: str) -> int:
        return sum(1 for r in rows if str(r.get("priority_tier")) == tier)

    stock_p0 = [
        r for r in rows
        if str(r.get("priority_tier")) == "P0"
        and str(r.get("fabric_type")) == "现货面料"
    ]
    custom_p0 = [
        r for r in rows
        if str(r.get("priority_tier")) == "P0"
        and str(r.get("fabric_type")) == "定制面料"
    ]
    unknown = [
        r for r in rows
        if str(r.get("fabric_type")) == "UNKNOWN"
    ]

    stock_low = sum(float(r.get("q50_gap_low") or 0) for r in stock_p0)
    stock_high = sum(float(r.get("q50_gap_high") or 0) for r in stock_p0)
    custom_low = sum(float(r.get("q50_gap_low") or 0) for r in custom_p0)
    custom_high = sum(float(r.get("q50_gap_high") or 0) for r in custom_p0)

    stock_lines = "\n".join(
        f"- {r.get('store_name')} / {r.get('spu')} / {r.get('primary_fabric')}："
        f"Q50缺口 {float(r.get('q50_gap_low') or 0):.0f}–{float(r.get('q50_gap_high') or 0):.0f}"
        for r in stock_p0
    ) or "- 无"

    custom_lines = "\n".join(
        f"- {r.get('store_name')} / {r.get('spu')} / {r.get('primary_fabric')}："
        f"Q50缺口 {float(r.get('q50_gap_low') or 0):.0f}–{float(r.get('q50_gap_high') or 0):.0f}"
        for r in custom_p0
    ) or "- 无"

    unknown_lines = "\n".join(
        f"- {r.get('store_name')} / {r.get('spu')} / {r.get('priority_tier')} / "
        f"{r.get('h48_risk_level')}"
        for r in unknown
    ) or "- 无"

    detail = (
        f"**快照日期：** {d}\n"
        f"**分层：** P0={cnt('P0')}，P1={cnt('P1')}，P2={cnt('P2')}，"
        f"P3={cnt('P3')}，WATCH={cnt('WATCH')}\n\n"
        f"**现货面料 P0：{len(stock_p0)} 款**\n"
        f"Q50缺口合计：{stock_low:.0f}–{stock_high:.0f} 件\n"
        f"{stock_lines}\n\n"
        f"**定制面料 P0：{len(custom_p0)} 款**\n"
        f"Q50缺口合计：{custom_low:.0f}–{custom_high:.0f} 件\n"
        f"H90仍未释放数量，仅人工紧急处理。\n"
        f"{custom_lines}\n\n"
        f"**面料映射缺失：{len(unknown)} 款**\n"
        f"{unknown_lines}\n\n"
        f"**报表目录：** reports_analysis/new_visible_procurement_shadow/{d}\n"
        f"**状态：** shadow only；未写生产PO。"
    )

    if args.dry_run:
        print(detail)
        return 0

    send_notify(
        "NEW_VISIBLE采购Shadow日报",
        "success",
        detail,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
