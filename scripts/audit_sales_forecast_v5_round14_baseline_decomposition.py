#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 Round-14: decompose mapped-category forecast error by model layer.

Read-only diagnostic. No production tables or forecast logic are modified.

Purpose
-------
Round-13C showed that Amazon market guards only marginally changed the large T-shirt
July error. This diagnostic separates where the error already exists:
    A0 last-month baseline
 -> A3 lifecycle baseline
 -> A7 seasonal uplift
 -> A16 internal trust gate
 -> A17 Amazon market guard

It also prints the top mapped SPUs for July H2/H3 with snapshot-visible M1/M2/M3
sales so we can distinguish baseline persistence/lifecycle problems from seasonal-uplift
problems.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base
from scripts import audit_sales_forecast_horizons_v2 as output_fix
from scripts import audit_sales_forecast_v5_round2 as r2
from scripts import audit_sales_forecast_v5_round5_asymmetric as r5
from scripts import audit_sales_forecast_v5_round6_segments as r6
from scripts import audit_sales_forecast_v5_round7_router as r7
from scripts import audit_sales_forecast_v5_round9_category_diagnostics as r9
from scripts import audit_sales_forecast_v5_round10_current_momentum_veto as r10
from scripts import audit_sales_forecast_v5_round11_concentration_anomaly as r11
from scripts import audit_sales_forecast_v5_round12_robust_season_trust as r12
from scripts import audit_sales_forecast_v5_round13_amazon_market_signal as r13
from scripts.amazon_category_insights_performance_adapter_v3 import (
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    load_monthly_market_24m,
)

A0 = "A0_上月延续"
A3 = "A3_SPU生命周期收缩"
A7 = "A7_非对称季节门控"
A16 = "A16_广度否决_高集中半衰减"
A17 = "A17_Amazon市场刹车"

MODELS = [(A0, A0), (A3, A3), (A7, A7), (A16, A16), (A17, A17)]


def parse_month(v: Any) -> date:
    if isinstance(v, datetime):
        return date(v.year, v.month, 1)
    if isinstance(v, date):
        return date(v.year, v.month, 1)
    return datetime.strptime(str(v)[:7], "%Y-%m").date().replace(day=1)


def build_rows(actual, targets, max_horizon: int, category_map_path: Path, schema: str, table: str):
    detail = r2.build_detail(actual, targets, max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r10.walk_forward_oos(actual, detail, targets, max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    rows = [r for r in oos if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")]
    rows = r9.enrich_category(rows, r9.load_spu_category_map())
    first_sale = r11.first_sale_map(actual)
    support = r12.category_support(actual, first_sale, rows)
    rows = r12.apply_trust_gates(actual, first_sale, rows, support)

    category_map = r13.load_category_node_map(category_map_path)
    node_ids = sorted({str(v["browse_node_id"]) for v in category_map.values()})
    market, meta = load_monthly_market_24m(
        schema=schema,
        table=table,
        browse_node_ids=node_ids,
        start_month=date(2025, 1, 1),
        end_month=max(targets),
    )
    rows = r13.enrich_market(rows, market, category_map)
    rows = r13.apply_market_models(rows)
    return rows, meta


def layer_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs and r.get("Amazon映射")]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            mr = [r for r in hr if str(r.get("目标月")) == month]
            actual_sum = sum(float(r.get("实际销量", 0) or 0) for r in mr)
            sums = {col: sum(float(r.get(col, 0) or 0) for r in mr) for _name, col in MODELS}
            out.append({
                "Horizon": hs,
                "目标月": month,
                "记录数": len(mr),
                "实际销量": int(round(actual_sum)),
                "A0预测": int(round(sums[A0])),
                "A3预测": int(round(sums[A3])),
                "A7预测": int(round(sums[A7])),
                "A16预测": int(round(sums[A16])),
                "A17预测": int(round(sums[A17])),
                "A3相对实际误差": int(round(sums[A3] - actual_sum)),
                "A7相对A3增量": int(round(sums[A7] - sums[A3])),
                "A16相对A7改变量": int(round(sums[A16] - sums[A7])),
                "A17相对A16改变量": int(round(sums[A17] - sums[A16])),
                "A3高估占最终A7高估比": None if sums[A7] <= actual_sum else round(max(0.0, sums[A3] - actual_sum) / max(1.0, sums[A7] - actual_sum), 6),
            })
    return out


def model_metrics(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    mapped = [r for r in rows if r.get("Amazon映射")]
    for hs in ("H2", "H3"):
        hr = [r for r in mapped if str(r.get("Horizon")) == hs]
        for name, col in MODELS:
            out.append({"Horizon": hs, "模型": name, **base.metric(hr, col)})
    return out


def add_recent(actual, row: Dict[str, Any]) -> Dict[str, Any]:
    x = dict(row)
    snapshot = parse_month(row.get("快照月"))
    spu = str(row.get("SPU") or "")
    m1 = float(actual.get((spu, base.add_months(snapshot, -1)), 0) or 0)
    m2 = float(actual.get((spu, base.add_months(snapshot, -2)), 0) or 0)
    m3 = float(actual.get((spu, base.add_months(snapshot, -3)), 0) or 0)
    x["快照M1"] = int(m1)
    x["快照M2"] = int(m2)
    x["快照M3"] = int(m3)
    denom = (m2 + m3) / 2.0
    x["当前趋势M1_均值M2M3"] = None if denom <= 0 else round(m1 / denom, 6)
    act = float(row.get("实际销量", 0) or 0)
    a0 = float(row.get(A0, 0) or 0)
    a3 = float(row.get(A3, 0) or 0)
    a7 = float(row.get(A7, 0) or 0)
    a16 = float(row.get(A16, 0) or 0)
    a17 = float(row.get(A17, 0) or 0)
    x["A0误差"] = int(round(a0 - act))
    x["A3误差"] = int(round(a3 - act))
    x["A7季节增量"] = int(round(a7 - a3))
    x["A16改变量"] = int(round(a16 - a7))
    x["A17改变量"] = int(round(a17 - a16))
    return x


def july_top(actual, rows: Sequence[Dict[str, Any]], topn: int = 30) -> List[Dict[str, Any]]:
    fields = [
        "目标月", "快照月", "Horizon", "SPU", "品类", "生命周期", "需求画像", "ABC",
        "实际销量", "快照M1", "快照M2", "快照M3", "当前趋势M1_均值M2M3",
        A0, A3, A7, A16, A17,
        "A0误差", "A3误差", "A7季节增量", "A16改变量", "A17改变量",
        "A7规则", "A16规则", "A17规则",
        "Amazon_LY销量环比", "Amazon_LY销量阶段",
        "Amazon_当前销量动量", "Amazon_当前搜索动量", "Amazon_当前点击动量",
        "Amazon_历史强支持上涨", "Amazon_历史反对上涨", "Amazon_当前市场偏弱",
    ]
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        seg = [
            add_recent(actual, r) for r in rows
            if r.get("Amazon映射") and str(r.get("目标月")) == "2026-07" and str(r.get("Horizon")) == hs
        ]
        seg.sort(key=lambda r: (float(r.get(A3, 0) or 0) - float(r.get("实际销量", 0) or 0)), reverse=True)
        for rank, r in enumerate(seg[:topn], 1):
            x = {"Rank": rank}
            for f in fields:
                v = r.get(f)
                if isinstance(v, (list, tuple, set, dict)):
                    v = str(v)
                x[f] = v
            out.append(x)
    return out


def rule_counts(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    mapped = [r for r in rows if r.get("Amazon映射")]
    for hs in ("H2", "H3"):
        for month in sorted({str(r.get("目标月")) for r in mapped if str(r.get("Horizon")) == hs}):
            mr = [r for r in mapped if str(r.get("Horizon")) == hs and str(r.get("目标月")) == month]
            rules = sorted({str(r.get("A7规则") or "UNKNOWN") for r in mr})
            for rule in rules:
                seg = [r for r in mr if str(r.get("A7规则") or "UNKNOWN") == rule]
                out.append({
                    "Horizon": hs,
                    "目标月": month,
                    "A7规则": rule,
                    "记录数": len(seg),
                    "实际销量": int(sum(float(r.get("实际销量", 0) or 0) for r in seg)),
                    "A3预测": int(sum(float(r.get(A3, 0) or 0) for r in seg)),
                    "A7预测": int(sum(float(r.get(A7, 0) or 0) for r in seg)),
                })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--node-map", default="config/amazon_category_node_map.json")
    ap.add_argument("--market-db", default=DEFAULT_SCHEMA)
    ap.add_argument("--market-table", default=DEFAULT_TABLE)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    ap.add_argument("--topn", type=int, default=30)
    args = ap.parse_args()

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    sales_end = base.add_months(max(targets), 1)
    print("读取店内销量:", history_start, "~", sales_end)
    sales_rows = base.read_sales(history_start, sales_end)
    actual = base.actual_spu_month(sales_rows)

    print("构建 A0/A3/A7/A16/A17 分层诊断...")
    rows, market_meta = build_rows(
        actual, targets, args.max_horizon,
        PROJECT_ROOT / args.node_map,
        args.market_db,
        args.market_table,
    )
    print("Market adapter mode:", market_meta.get("mode"))

    summary = base.norm(model_metrics(rows))
    layers = base.norm(layer_summary(rows))
    top = base.norm(july_top(actual, rows, args.topn))
    rules = base.norm(rule_counts(rows))

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十四轮基线误差拆解_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_模型总览.csv"), summary)
    output_fix.write_csv(root.with_name(root.name + "_逐月分层.csv"), layers)
    output_fix.write_csv(root.with_name(root.name + "_7月TOP_SPUs.csv"), top)
    output_fix.write_csv(root.with_name(root.name + "_A7规则拆分.csv"), rules)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("模型总览", summary),
        ("逐月分层", layers),
        ("7月TOP_SPUs", top),
        ("A7规则拆分", rules),
    ])

    print("\n=== Amazon已映射 A0/A3/A7/A16/A17 总览 ===")
    for r in summary:
        print(r)
    print("\n=== 逐月误差分层 ===")
    for r in layers:
        print(r)
    print("\n=== 2026-07 A3高估 TOP SPUs ===")
    for r in top:
        print(r)
    print("\n=== 2026-07 A7规则拆分 ===")
    for r in rules:
        if str(r.get("目标月")) == "2026-07":
            print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：若7月高估在A3阶段已经形成，则停止继续调A7/A17门控，转而诊断A3基线/生命周期/近期水平。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
