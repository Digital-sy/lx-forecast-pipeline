#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Production bridge for approved NEW_VISIBLE procurement recommendations.

Environment switch:
  NEW_VISIBLE_PROCUREMENT_MODE=primary  (default)
  NEW_VISIBLE_PROCUREMENT_MODE=off      (rollback to legacy procurement logic)

When primary is enabled, missing/stale recommendation data is a hard failure. This
prevents silent fallback to the old NEW_VISIBLE logic.
"""
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Dict, Tuple

from common import get_logger
from common.database import db_cursor

logger = get_logger("new_visible_procurement_bridge")

TABLE = "forecast_new_visible_procurement_recommendation_daily"
MODEL = "NV_PROCUREMENT_CHAMPION_V2_H48_H60_NET_GAP"
DEFAULT_MAX_STALE_DAYS = 2

RecommendationKey = Tuple[str, str]  # SPU, store


def _one(sql: str, params=()) -> Dict[str, Any]:
    with db_cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return row or {}


def _rows(sql: str, params=()):
    with db_cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def _to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def mode() -> str:
    value = os.getenv("NEW_VISIBLE_PROCUREMENT_MODE", "primary").strip().lower()
    if value not in {"primary", "off"}:
        raise RuntimeError(
            "NEW_VISIBLE_PROCUREMENT_MODE must be 'primary' or 'off'"
        )
    return value


def load_recommendations(
    current_date: datetime | date | None = None,
) -> Dict[RecommendationKey, Dict[str, Any]]:
    if mode() == "off":
        logger.warning(
            "NEW_VISIBLE采购Champion已关闭：使用旧采购逻辑（rollback mode）"
        )
        return {}

    current = current_date or datetime.now()
    current_day = current.date() if isinstance(current, datetime) else current
    max_stale = int(
        os.getenv("NEW_VISIBLE_RECOMMENDATION_MAX_STALE_DAYS", str(DEFAULT_MAX_STALE_DAYS))
    )

    exists = _one(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s
        """,
        (TABLE,),
    )
    if not int(exists.get("cnt", 0) or 0):
        raise RuntimeError(
            f"{TABLE}不存在；NEW_VISIBLE primary模式禁止静默回退旧模型"
        )

    latest = _one(f"SELECT MAX(snapshot_date) AS d FROM {TABLE}").get("d")
    if not latest:
        raise RuntimeError(f"{TABLE}为空")
    snapshot = _to_date(latest)
    stale_days = (current_day - snapshot).days
    if stale_days < 0 or stale_days > max_stale:
        raise RuntimeError(
            f"NEW_VISIBLE推荐快照过期/异常: snapshot={snapshot}, "
            f"current={current_day}, stale_days={stale_days}, max={max_stale}"
        )

    rows = _rows(
        f"""
        SELECT
          snapshot_date,as_of_date,store_name,spu,age_days,
          fabric_type,primary_fabric,h48_risk_level,h60_coverage_status,
          h60_q50,h60_q75,on_hand_position,total_inventory_position,
          q50_gap_low,q50_gap_high,q75_gap_low,q75_gap_high,qty_basis,
          days_cover_q50,latest_order_date,recommendation_status,
          recommended_qty_q50,safety_qty_q75,override_active,
          source_action_type,model_version
        FROM {TABLE}
        WHERE snapshot_date=%s
          AND override_active=1
        """,
        (snapshot,),
    )
    if not rows:
        raise RuntimeError(
            f"{TABLE} latest snapshot has no override_active rows: {snapshot}"
        )

    result: Dict[RecommendationKey, Dict[str, Any]] = {}
    for row in rows:
        spu = str(row.get("spu") or "").strip()
        shop = str(row.get("store_name") or "").strip()
        if not spu or not shop:
            continue
        key = (spu, shop)
        if key in result:
            raise RuntimeError(f"duplicate NEW_VISIBLE recommendation key: {key}")
        result[key] = dict(row)

    logger.info(
        "NEW_VISIBLE采购Champion加载完成：snapshot=%s, overrides=%s, model=%s",
        snapshot, len(result), MODEL,
    )
    return result
