#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only multi-day effectiveness audit for forecast dynamic monitoring.

This script does NOT create/update/delete any table.
It reviews:
1) snapshot continuity and data completeness;
2) forecastability-state transitions;
3) HIGH/MEDIUM breakout signal persistence and subsequent 7d sales/traffic trajectory;
4) prediction-snapshot accumulation.

Important: only a few days of history are available initially, so this audits monitoring
quality and early directional signal value. It does not claim H2/H3 monthly forecast accuracy.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_monitoring import daily_monitor as base

TARGET_SHOPS = ("JQ-US", "RKZ-US", "SY-US", "MT-US")
WINDOW_DAYS = 14


def text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def num(v: Any) -> float:
    if v is None:
        return 0.0
    return float(v)


def to_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params=()):
    rows = q(sql, params)
    return rows[0] if rows else {}


def table_exists(name: str) -> bool:
    return base.table_exists(name)


def dates_between(a: date, b: date) -> List[date]:
    out = []
    d = a
    while d <= b:
        out.append(d)
        d += timedelta(days=1)
    return out


def pct(a: float, b: float) -> float | None:
    if not b:
        return None
    return round(a / b, 4)


def ratio(a: float, b: float) -> float | None:
    if b <= 0:
        return None
    return round(a / b, 4)


def fmt_ratio(v: float | None) -> str:
    return "NA" if v is None else f"{v:.2f}x"


def main() -> int:
    required = [
        base.INVENTORY_SNAPSHOT_TABLE,
        base.FEATURE_SNAPSHOT_TABLE,
        base.PREDICTION_SNAPSHOT_TABLE,
        base.BREAKOUT_MONITOR_TABLE,
    ]
    missing = [t for t in required if not table_exists(t)]
    if missing:
        raise RuntimeError(f"缺少监控表，无法审计: {missing}")

    latest_row = one(
        f"SELECT MAX(snapshot_date) AS d FROM `{base.FEATURE_SNAPSHOT_TABLE}`"
    )
    latest = latest_row.get("d")
    if not latest:
        raise RuntimeError("forecast_feature_snapshot_daily 尚无数据")
    latest = to_date(latest)

    min_row = one(
        f"SELECT MIN(snapshot_date) AS d FROM `{base.FEATURE_SNAPSHOT_TABLE}`"
    )
    first = to_date(min_row["d"])
    start = max(first, latest - timedelta(days=WINDOW_DAYS - 1))

    print("=" * 96)
    print("销量预测动态监控：多日效果审计（只读）")
    print(f"审计窗口: {start} ~ {latest}")
    print("=" * 96)

    inv_rows = q(
        f"""
        SELECT snapshot_date,
               COUNT(*) AS rows_n,
               COUNT(DISTINCT spu) AS spu_n,
               COUNT(DISTINCT store_name) AS shops,
               ROUND(SUM(fba_available_inventory),0) AS available_inventory
        FROM `{base.INVENTORY_SNAPSHOT_TABLE}`
        WHERE snapshot_date BETWEEN %s AND %s
        GROUP BY snapshot_date
        ORDER BY snapshot_date
        """,
        (start, latest),
    )

    feat_rows = q(
        f"""
        SELECT snapshot_date,
               MAX(as_of_date) AS as_of_date,
               COUNT(*) AS rows_n,
               SUM(forecastability='ESTABLISHED') AS established_n,
               SUM(forecastability='NEW_VISIBLE') AS new_visible_n,
               SUM(forecastability='COLD_NO_HISTORY') AS cold_n,
               SUM(forecastability='UNKNOWN') AS unknown_n,
               ROUND(SUM(sales_7d),0) AS sales_7d,
               ROUND(SUM(sessions_7d),0) AS sessions_7d,
               SUM(fba_available_inventory IS NULL) AS inventory_null,
               SUM(sessions_7d IS NULL) AS sessions_null
        FROM `{base.FEATURE_SNAPSHOT_TABLE}`
        WHERE snapshot_date BETWEEN %s AND %s
        GROUP BY snapshot_date
        ORDER BY snapshot_date
        """,
        (start, latest),
    )

    pred_rows = q(
        f"""
        SELECT issue_date,
               COUNT(*) AS rows_n,
               SUM(horizon='H0') AS h0_n,
               SUM(horizon='H1') AS h1_n,
               SUM(horizon='H2') AS h2_n,
               SUM(horizon='H3') AS h3_n,
               SUM(forecast_qty) AS forecast_qty
        FROM `{base.PREDICTION_SNAPSHOT_TABLE}`
        WHERE issue_date BETWEEN %s AND %s
        GROUP BY issue_date
        ORDER BY issue_date
        """,
        (start, latest),
    )

    risk_rows = q(
        f"""
        SELECT snapshot_date,
               SUM(risk_level='HIGH') AS high_n,
               SUM(risk_level='MEDIUM') AS medium_n,
               SUM(risk_level='LOW') AS low_n,
               COUNT(*) AS rows_n,
               ROUND(AVG(breakout_score),2) AS avg_score
        FROM `{base.BREAKOUT_MONITOR_TABLE}`
        WHERE snapshot_date BETWEEN %s AND %s
        GROUP BY snapshot_date
        ORDER BY snapshot_date
        """,
        (start, latest),
    )

    def by_date(rows):
        return {to_date(r[next(iter(r.keys()))]): r for r in rows}

    inv_map = {to_date(r["snapshot_date"]): r for r in inv_rows}
    feat_map = {to_date(r["snapshot_date"]): r for r in feat_rows}
    pred_map = {to_date(r["issue_date"]): r for r in pred_rows}
    risk_map = {to_date(r["snapshot_date"]): r for r in risk_rows}

    expected = dates_between(start, latest)
    print("\n=== 1. 每日运行连续性 ===")
    for d in expected:
        print(json.dumps({
            "date": str(d),
            "inventory": int(inv_map.get(d, {}).get("rows_n") or 0),
            "features": int(feat_map.get(d, {}).get("rows_n") or 0),
            "predictions": int(pred_map.get(d, {}).get("rows_n") or 0),
            "breakout": int(risk_map.get(d, {}).get("rows_n") or 0),
            "HIGH": int(risk_map.get(d, {}).get("high_n") or 0),
            "MEDIUM": int(risk_map.get(d, {}).get("medium_n") or 0),
        }, ensure_ascii=False))

    missing_days = {
        "inventory": [str(d) for d in expected if d not in inv_map],
        "features": [str(d) for d in expected if d not in feat_map],
        "predictions": [str(d) for d in expected if d not in pred_map],
        "breakout": [str(d) for d in expected if d not in risk_map],
    }

    print("\n=== 2. 特征质量趋势 ===")
    for r in feat_rows:
        n = int(r["rows_n"] or 0)
        print(json.dumps({
            "date": str(r["snapshot_date"]),
            "as_of": str(r["as_of_date"]),
            "rows": n,
            "ESTABLISHED": int(r["established_n"] or 0),
            "NEW_VISIBLE": int(r["new_visible_n"] or 0),
            "COLD_NO_HISTORY": int(r["cold_n"] or 0),
            "UNKNOWN": int(r["unknown_n"] or 0),
            "sales_7d": float(r["sales_7d"] or 0),
            "sessions_7d": float(r["sessions_7d"] or 0),
            "inventory_null_rate": round(int(r["inventory_null"] or 0) / n, 4) if n else None,
            "sessions_null_rate": round(int(r["sessions_null"] or 0) / n, 4) if n else None,
        }, ensure_ascii=False))

    transitions = q(
        f"""
        SELECT a.snapshot_date AS from_date,
               b.snapshot_date AS to_date,
               a.forecastability AS from_state,
               b.forecastability AS to_state,
               COUNT(*) AS n
        FROM `{base.FEATURE_SNAPSHOT_TABLE}` a
        JOIN `{base.FEATURE_SNAPSHOT_TABLE}` b
          ON b.snapshot_date = DATE_ADD(a.snapshot_date, INTERVAL 1 DAY)
         AND b.store_name = a.store_name
         AND b.spu = a.spu
        WHERE a.snapshot_date BETWEEN %s AND DATE_SUB(%s, INTERVAL 1 DAY)
          AND a.forecastability <> b.forecastability
        GROUP BY a.snapshot_date, b.snapshot_date, a.forecastability, b.forecastability
        ORDER BY a.snapshot_date, n DESC
        """,
        (start, latest),
    )
    print("\n=== 3. 生命周期状态变化 ===")
    if not transitions:
        print("无状态变化")
    else:
        for r in transitions:
            print(json.dumps({
                "from_date": str(r["from_date"]),
                "to_date": str(r["to_date"]),
                "transition": f"{r['from_state']} -> {r['to_state']}",
                "n": int(r["n"]),
            }, ensure_ascii=False))

    breakout_rows = q(
        f"""
        SELECT snapshot_date, store_name, spu,
               months_since_first_sale,
               sales_7d, sales_growth_7d,
               sessions_7d, sessions_growth_7d,
               cvr_7d, cvr_ratio_7d,
               fba_available_inventory, inventory_days_supply,
               breakout_score, risk_level, reason_code
        FROM `{base.BREAKOUT_MONITOR_TABLE}`
        WHERE snapshot_date BETWEEN %s AND %s
        ORDER BY snapshot_date, store_name, spu
        """,
        (start, latest),
    )

    hist: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in breakout_rows:
        hist[(text(r["store_name"]), text(r["spu"]))].append(r)

    high_keys = []
    for k, arr in hist.items():
        if any(text(x.get("risk_level")) == "HIGH" for x in arr):
            high_keys.append(k)

    alert_results = []
    for k in high_keys:
        arr = sorted(hist[k], key=lambda x: to_date(x["snapshot_date"]))
        first_high = next(x for x in arr if text(x.get("risk_level")) == "HIGH")
        latest_r = arr[-1]
        first_d = to_date(first_high["snapshot_date"])
        latest_d = to_date(latest_r["snapshot_date"])
        elapsed = (latest_d - first_d).days

        s0 = num(first_high.get("sales_7d"))
        s1 = num(latest_r.get("sales_7d"))
        t0 = num(first_high.get("sessions_7d"))
        t1 = num(latest_r.get("sessions_7d"))
        sr = ratio(s1, s0)
        tr = ratio(t1, t0)
        high_days = sum(1 for x in arr if text(x.get("risk_level")) == "HIGH")
        medium_days = sum(1 for x in arr if text(x.get("risk_level")) == "MEDIUM")

        dos = latest_r.get("inventory_days_supply")
        dos_v = None if dos is None else num(dos)
        inv_v = latest_r.get("fba_available_inventory")
        inv_v = None if inv_v is None else num(inv_v)

        if elapsed < 2:
            verdict = "TOO_EARLY"
        elif sr is not None and sr >= 1.20:
            verdict = "EARLY_CONFIRMED_ACCELERATION"
        elif sr is not None and sr <= 0.80:
            verdict = "COOLING"
        else:
            verdict = "HOLDING"

        if dos_v is not None and dos_v < 7:
            verdict += "+STOCK_CONSTRAINED"

        alert_results.append({
            "shop": k[0],
            "spu": k[1],
            "first_high_date": str(first_d),
            "latest_date": str(latest_d),
            "elapsed_days": elapsed,
            "high_days": high_days,
            "medium_days": medium_days,
            "first_score": num(first_high.get("breakout_score")),
            "latest_score": num(latest_r.get("breakout_score")),
            "latest_risk": text(latest_r.get("risk_level")),
            "first_sales_7d": s0,
            "latest_sales_7d": s1,
            "sales_7d_ratio": sr,
            "first_sessions_7d": t0,
            "latest_sessions_7d": t1,
            "sessions_7d_ratio": tr,
            "latest_inventory": inv_v,
            "latest_dos": dos_v,
            "latest_reason": text(latest_r.get("reason_code")),
            "verdict": verdict,
        })

    alert_results.sort(
        key=lambda x: (
            0 if x["verdict"].startswith("EARLY_CONFIRMED") else
            1 if x["verdict"].startswith("HOLDING") else
            2 if x["verdict"].startswith("COOLING") else 3,
            -x["latest_score"],
        )
    )

    print("\n=== 4. HIGH风险后续表现 ===")
    if not alert_results:
        print("窗口内无HIGH")
    else:
        for x in alert_results[:50]:
            y = dict(x)
            y["sales_7d_ratio"] = fmt_ratio(x["sales_7d_ratio"])
            y["sessions_7d_ratio"] = fmt_ratio(x["sessions_7d_ratio"])
            print(json.dumps(y, ensure_ascii=False))

    confirmed = sum(1 for x in alert_results if x["verdict"].startswith("EARLY_CONFIRMED"))
    cooling = sum(1 for x in alert_results if x["verdict"].startswith("COOLING"))
    holding = sum(1 for x in alert_results if x["verdict"].startswith("HOLDING"))
    too_early = sum(1 for x in alert_results if x["verdict"].startswith("TOO_EARLY"))
    stock_constrained = sum(1 for x in alert_results if "STOCK_CONSTRAINED" in x["verdict"])

    print("\n=== 5. 预测快照积累 ===")
    for r in pred_rows:
        print(json.dumps({
            "issue_date": str(r["issue_date"]),
            "rows": int(r["rows_n"] or 0),
            "H0": int(r["h0_n"] or 0),
            "H1": int(r["h1_n"] or 0),
            "H2": int(r["h2_n"] or 0),
            "H3": int(r["h3_n"] or 0),
            "forecast_qty": int(r["forecast_qty"] or 0),
        }, ensure_ascii=False))

    latest_feat = feat_rows[-1] if feat_rows else {}
    latest_n = int(latest_feat.get("rows_n") or 0)
    latest_inv_null = int(latest_feat.get("inventory_null") or 0)
    latest_sess_null = int(latest_feat.get("sessions_null") or 0)
    latest_unknown = int(latest_feat.get("unknown_n") or 0)

    continuity_ok = all(len(v) == 0 for v in missing_days.values())
    quality_ok = (
        latest_unknown == 0
        and latest_sess_null == 0
        and (latest_inv_null / latest_n if latest_n else 1) < 0.02
    )

    mature_alerts = [x for x in alert_results if x["elapsed_days"] >= 2]
    mature_confirmed = sum(
        1 for x in mature_alerts if x["verdict"].startswith("EARLY_CONFIRMED")
    )

    summary = {
        "window": {"start": str(start), "end": str(latest), "days": len(expected)},
        "continuity_ok": continuity_ok,
        "missing_days": missing_days,
        "latest_feature_rows": latest_n,
        "latest_unknown": latest_unknown,
        "latest_sessions_null": latest_sess_null,
        "latest_inventory_null_rate": round(latest_inv_null / latest_n, 4) if latest_n else None,
        "data_quality_ok": quality_ok,
        "unique_high_alerts": len(alert_results),
        "high_alerts_elapsed_ge_2d": len(mature_alerts),
        "early_confirmed_acceleration": confirmed,
        "holding": holding,
        "cooling": cooling,
        "too_early": too_early,
        "stock_constrained": stock_constrained,
        "mature_high_confirmation_rate": (
            round(mature_confirmed / len(mature_alerts), 4) if mature_alerts else None
        ),
        "prediction_snapshot_days": len(pred_rows),
        "assessment_boundary": "few-day audit: validates monitoring/data/signal direction only; H2/H3 accuracy requires completed target months",
    }
    print("\n=== 6. MONITOR_EFFECT_SUMMARY ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    print("\n=== 判读提示 ===")
    print("1. continuity_ok=true 且 data_quality_ok=true：说明每日资产连续、可用于后续训练。")
    print("2. EARLY_CONFIRMED_ACCELERATION：首次HIGH后至少2天，滚动7日销量较报警日继续提高>=20%。")
    print("3. HOLDING：尚未明显扩张或衰退；COOLING：滚动7日销量较首次HIGH下降>=20%。")
    print("4. STOCK_CONSTRAINED：最新库存覆盖<7天，需求信号可能被缺货压低。")
    print("5. 当前只有数天数据，不能据此宣称H2/H3月度预测准确率提升。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
