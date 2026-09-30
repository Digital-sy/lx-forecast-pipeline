#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""V5 round-13: Amazon Browse Node external-market trust signal.

Read-only experiment. No production tables or forecast logic are modified.

Purpose
-------
Round-12 proved that internal category breadth/concentration can improve seasonal trust,
but it sometimes over-blocks real rising demand. This round adds Amazon Category
Insights as an independent market referee.

External data is NEVER multiplied directly into SPU demand and NEVER increases a
forecast above A7. It only:
1) guards/attenuates an existing A7 seasonal uplift when Amazon market history does not
   support the same target-month rise; or
2) restores up to 50% of an A7 uplift that A16 removed when Amazon market evidence is
   strongly supportive and currently visible market momentum is not weak.

Leakage rule
------------
For a target month T and snapshot S:
- target-month seasonality uses Amazon node data from T-12 and T-13 only;
- current market momentum uses completed months S-1/S-2/S-3 only;
- no Amazon data after S-1 is used.

Mapping rule
------------
Internal product_category -> Browse Node mapping is manual-only via
config/amazon_category_node_map.json. Unmapped categories fall back to A16 unchanged.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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
from scripts.amazon_category_insights_adapter import (
    DEFAULT_SCHEMA,
    describe_schema,
    load_monthly_metrics,
)

A3 = "A3_SPU生命周期收缩"
A7 = "A7_非对称季节门控"
A16 = "A16_广度否决_高集中半衰减"
A17 = "A17_Amazon市场刹车"
A18 = "A18_Amazon市场仲裁"

MODELS = [(A7, A7), (A16, A16), (A17, A17), (A18, A18)]


def parse_month(value: Any) -> date:
    if isinstance(value, datetime):
        return date(value.year, value.month, 1)
    if isinstance(value, date):
        return date(value.year, value.month, 1)
    return datetime.strptime(str(value)[:7], "%Y-%m").date().replace(day=1)


def load_category_node_map(path: Path) -> Dict[str, Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    mappings = data.get("mappings") or {}
    out: Dict[str, Dict[str, Any]] = {}
    for cat, meta in mappings.items():
        if not isinstance(meta, dict):
            continue
        node = str(meta.get("browse_node_id") or "").strip()
        if node:
            out[str(cat).strip()] = dict(meta)
    return out


def metric(
    market: Mapping[Tuple[str, date], Mapping[str, float]],
    node: str,
    month: date,
    name: str,
) -> Optional[float]:
    v = market.get((str(node), month), {}).get(name)
    return None if v is None else float(v)


def ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return a / b


def momentum3(
    market: Mapping[Tuple[str, date], Mapping[str, float]],
    node: str,
    snapshot: date,
    metric_name: str,
) -> Optional[float]:
    m1 = metric(market, node, base.add_months(snapshot, -1), metric_name)
    m2 = metric(market, node, base.add_months(snapshot, -2), metric_name)
    m3 = metric(market, node, base.add_months(snapshot, -3), metric_name)
    if m1 is None or m2 is None or m3 is None:
        return None
    denom = (m2 + m3) / 2.0
    return None if denom <= 0 else m1 / denom


def phase(v: Optional[float]) -> str:
    if v is None:
        return "UNKNOWN"
    if v >= 1.05:
        return "RISING"
    if v <= 0.95:
        return "FALLING"
    return "FLAT"


def market_features(
    market: Mapping[Tuple[str, date], Mapping[str, float]],
    node: str,
    target: date,
    snapshot: date,
) -> Dict[str, Any]:
    ly_target = base.add_months(target, -12)
    ly_prev = base.add_months(target, -13)

    ly_units = ratio(
        metric(market, node, ly_target, "units_sold"),
        metric(market, node, ly_prev, "units_sold"),
    )
    ly_search = ratio(
        metric(market, node, ly_target, "search_volume"),
        metric(market, node, ly_prev, "search_volume"),
    )
    ly_clicks = ratio(
        metric(market, node, ly_target, "search_clicks"),
        metric(market, node, ly_prev, "search_clicks"),
    )
    ly_views = ratio(
        metric(market, node, ly_target, "page_views"),
        metric(market, node, ly_prev, "page_views"),
    )

    current_units = momentum3(market, node, snapshot, "units_sold")
    current_search = momentum3(market, node, snapshot, "search_volume")
    current_clicks = momentum3(market, node, snapshot, "search_clicks")

    historical_support = ly_units is not None and ly_units >= 1.05
    historical_against = ly_units is not None and ly_units <= 0.95

    confirms = [x for x in (ly_search, ly_clicks, ly_views) if x is not None]
    strong_support = historical_support and (not confirms or sum(x >= 1.00 for x in confirms) >= max(1, len(confirms) // 2))

    current_signals = [x for x in (current_units, current_search, current_clicks) if x is not None]
    current_weak = len(current_signals) >= 2 and sum(x < 0.90 for x in current_signals) >= 2
    current_not_weak = not current_weak

    return {
        "Amazon_LY销量环比": ly_units,
        "Amazon_LY销量阶段": phase(ly_units),
        "Amazon_LY搜索环比": ly_search,
        "Amazon_LY点击环比": ly_clicks,
        "Amazon_LY浏览环比": ly_views,
        "Amazon_当前销量动量": current_units,
        "Amazon_当前搜索动量": current_search,
        "Amazon_当前点击动量": current_clicks,
        "Amazon_历史强支持上涨": 1 if strong_support else 0,
        "Amazon_历史反对上涨": 1 if historical_against else 0,
        "Amazon_当前市场偏弱": 1 if current_weak else 0,
        "Amazon_当前市场非弱": 1 if current_not_weak else 0,
    }


def enrich_market(
    rows: Sequence[Dict[str, Any]],
    market: Mapping[Tuple[str, date], Mapping[str, float]],
    category_map: Mapping[str, Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        cat = str(r.get("品类") or "未映射")
        mapping = category_map.get(cat)
        x["Amazon映射"] = 0
        x["BrowseNodeId"] = None
        x["BrowseNodeName"] = None
        if mapping:
            node = str(mapping.get("browse_node_id"))
            target = parse_month(r.get("目标月"))
            snapshot = parse_month(r.get("快照月"))
            f = market_features(market, node, target, snapshot)
            x["Amazon映射"] = 1
            x["BrowseNodeId"] = node
            x["BrowseNodeName"] = mapping.get("browse_node_name")
            x.update(f)
        out.append(x)
    return out


def apply_market_models(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        x = dict(r)
        a3 = float(r.get(A3, 0) or 0)
        a7 = float(r.get(A7, 0) or 0)
        a16 = float(r.get(A16, 0) or 0)
        uplift = max(0.0, a7 - a3)
        mapped = bool(r.get("Amazon映射"))
        a7_rule = str(r.get("A7规则") or "")
        a16_rule = str(r.get("A16规则") or "")

        p17 = a16
        p18 = a16
        r17 = "KEEP_A16"
        r18 = "KEEP_A16"

        if mapped and a7_rule == "UP_RISING" and uplift > 0:
            hist_phase = str(r.get("Amazon_LY销量阶段") or "UNKNOWN")
            hist_against = bool(r.get("Amazon_历史反对上涨", 0))
            hist_support = bool(r.get("Amazon_历史强支持上涨", 0))
            current_weak = bool(r.get("Amazon_当前市场偏弱", 0))

            if hist_against or current_weak:
                p17 = min(a16, a3 + 0.25 * uplift)
                r17 = "MARKET_STRONG_BRAKE"
            elif hist_phase == "FLAT":
                p17 = min(a16, a3 + 0.50 * uplift)
                r17 = "MARKET_HALF_BRAKE"
            elif hist_support:
                r17 = "MARKET_SUPPORT_KEEP_A16"

            p18 = p17
            r18 = r17
            if (
                a16_rule == "BLOCK_CATEGORY_NOT_BROAD"
                and hist_support
                and not current_weak
            ):
                restore = a3 + 0.50 * uplift
                p18 = min(a7, max(p17, restore))
                r18 = "MARKET_RESTORE_HALF_INTERNAL_BLOCK"

        x[A17] = max(0, int(round(p17)))
        x[A18] = max(0, int(round(p18)))
        x["A17规则"] = r17
        x["A18规则"] = r18
        out.append(x)
    return out


def summarize(rows: Sequence[Dict[str, Any]], scope: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for name, col in MODELS:
            out.append({"范围": scope, "Horizon": hs, "模型": name, **base.metric(hr, col)})
    return out


def monthly(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        for month in sorted({str(r.get("目标月")) for r in hr}):
            mr = [r for r in hr if str(r.get("目标月")) == month]
            mapped = [r for r in mr if r.get("Amazon映射")]
            for scope, seg in (("全部", mr), ("Amazon已映射", mapped)):
                if not seg:
                    continue
                for name, col in MODELS:
                    out.append({"目标月": month, "范围": scope, "Horizon": hs, "模型": name, **base.metric(seg, col)})
    return out


def quadrant_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    mapped = [r for r in rows if r.get("Amazon映射") and str(r.get("A7规则") or "") == "UP_RISING"]
    for hs in ("H2", "H3"):
        hr = [r for r in mapped if str(r.get("Horizon")) == hs]
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in hr:
            internal_broad = bool(r.get("类目_类目广泛上涨", 0))
            market_phase = str(r.get("Amazon_LY销量阶段") or "UNKNOWN")
            if market_phase == "RISING":
                mkt = "Amazon↑"
            elif market_phase == "FALLING":
                mkt = "Amazon↓"
            else:
                mkt = "Amazon平/未知"
            key = ("内部↑" if internal_broad else "内部弱") + " / " + mkt
            groups[key].append(r)
        for key, seg in sorted(groups.items()):
            for name, col in MODELS:
                out.append({"Horizon": hs, "象限": key, "模型": name, **base.metric(seg, col)})
    return out


def mapping_coverage(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for hs in ("H2", "H3"):
        hr = [r for r in rows if str(r.get("Horizon")) == hs]
        actual = sum(float(r.get("实际销量", 0) or 0) for r in hr)
        mapped = [r for r in hr if r.get("Amazon映射")]
        mapped_actual = sum(float(r.get("实际销量", 0) or 0) for r in mapped)
        mapped_categories = sorted({str(r.get("品类")) for r in mapped})
        out.append({
            "Horizon": hs,
            "记录数": len(hr),
            "已映射记录数": len(mapped),
            "记录覆盖率": None if not hr else len(mapped) / len(hr),
            "实际销量": actual,
            "已映射实际销量": mapped_actual,
            "实际销量覆盖率": None if actual <= 0 else mapped_actual / actual,
            "已映射品类": ",".join(mapped_categories),
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True)
    ap.add_argument("--max-horizon", type=int, default=3)
    ap.add_argument("--output-dir", default="reports_analysis/forecast_audit")
    ap.add_argument("--node-map", default="config/amazon_category_node_map.json")
    ap.add_argument("--market-db", default=DEFAULT_SCHEMA)
    ap.add_argument("--market-table", default=None)
    ap.add_argument("--market-node-col", default=None)
    ap.add_argument("--market-date-col", default=None)
    ap.add_argument("--market-metric-col", default=None)
    ap.add_argument("--market-value-col", default=None)
    ap.add_argument("--market-audit-col", default=None)
    ap.add_argument("--describe-market-schema", action="store_true")
    args = ap.parse_args()

    if args.describe_market_schema:
        describe_schema(args.market_db, 30)
        return 0

    targets = base.parse_months(args.months)
    min_snapshot = base.add_months(min(targets), -args.max_horizon)
    history_start = base.add_months(min_snapshot, -24)
    sales_end = base.add_months(max(targets), 1)

    node_map_path = PROJECT_ROOT / args.node_map
    category_map = load_category_node_map(node_map_path)
    node_ids = sorted({str(v["browse_node_id"]) for v in category_map.values()})
    if not node_ids:
        raise RuntimeError("Browse Node 映射为空，无法运行 Round-13")

    print("读取店内销量:", history_start, "~", sales_end)
    sales_rows = base.read_sales(history_start, sales_end)
    actual = base.actual_spu_month(sales_rows)
    first_sale = r11.first_sale_map(actual)

    print("构建 A7/A16 expanding walk-forward OOS...")
    detail = r2.build_detail(actual, targets, args.max_horizon)
    detail = r5.enrich(actual, detail)
    _choices, oos = r10.walk_forward_oos(actual, detail, targets, args.max_horizon, 6)
    oos = r6.enrich_segments(actual, oos)
    oos = r7.enrich_forecastability(oos)
    rows = [r for r in oos if r.get("可预测性") == "ESTABLISHED" and str(r.get("Horizon")) in ("H2", "H3")]
    rows = r9.enrich_category(rows, r9.load_spu_category_map())
    support = r12.category_support(actual, first_sale, rows)
    rows = r12.apply_trust_gates(actual, first_sale, rows, support)

    print("读取 Amazon Category Insights，只读取已人工映射 Browse Node:", node_ids)
    try:
        market, market_meta = load_monthly_metrics(
            schema=args.market_db,
            table=args.market_table,
            start_month=date(2025, 1, 1),
            end_month=max(targets),
            node_ids=node_ids,
            node_col=args.market_node_col,
            date_col=args.market_date_col,
            metric_col=args.market_metric_col,
            value_col=args.market_value_col,
            audit_col=args.market_audit_col,
        )
    except Exception:
        print("\n自动读取失败。下面输出候选表结构，便于一次性校准适配器：")
        try:
            describe_schema(args.market_db, 30)
        finally:
            raise

    print("Market adapter:", market_meta)
    rows = enrich_market(rows, market, category_map)
    rows = apply_market_models(rows)

    mapped = [r for r in rows if r.get("Amazon映射")]
    summary_rows = base.norm(summarize(rows, "全部ESTABLISHED") + summarize(mapped, "Amazon已映射"))
    monthly_rows = base.norm(monthly(rows))
    quadrant_rows = base.norm(quadrant_summary(rows))
    coverage_rows = base.norm(mapping_coverage(rows))
    detail_rows = base.norm(rows)

    out = Path(args.output_dir)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = out / f"V5第十三轮Amazon节点市场信号_{stamp}"
    output_fix.write_csv(root.with_name(root.name + "_总览.csv"), summary_rows)
    output_fix.write_csv(root.with_name(root.name + "_逐月.csv"), monthly_rows)
    output_fix.write_csv(root.with_name(root.name + "_市场象限.csv"), quadrant_rows)
    output_fix.write_csv(root.with_name(root.name + "_映射覆盖.csv"), coverage_rows)
    output_fix.write_csv(root.with_name(root.name + "_OOS明细.csv"), detail_rows)
    xlsx = root.with_suffix(".xlsx")
    output_fix.write_xlsx(xlsx, [
        ("总览", summary_rows),
        ("逐月", monthly_rows),
        ("市场象限", quadrant_rows),
        ("映射覆盖", coverage_rows),
        ("OOS明细", detail_rows),
    ])

    print("\n=== Amazon节点映射覆盖 ===")
    for r in coverage_rows:
        print(r)
    print("\n=== Amazon已映射 H2/H3：A7/A16/A17/A18 ===")
    for r in summary_rows:
        if r.get("范围") == "Amazon已映射":
            print(r)
    print("\n=== Amazon市场象限 ===")
    for r in quadrant_rows:
        print(r)
    print("\n=== Amazon已映射逐月 ===")
    for r in monthly_rows:
        if r.get("范围") == "Amazon已映射":
            print(r)
    print("\nExcel:", xlsx.resolve())
    print("判定：先验证外部市场信号是否能减少A16在上升期的误杀、同时保留7-8月的刹车收益；未通过OOS前不进入生产。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
