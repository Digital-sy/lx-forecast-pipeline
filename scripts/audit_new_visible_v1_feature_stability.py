#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only stability audit for NEW_VISIBLE Stage-1 optional/enrichment features.

Purpose
-------
Before trusting ML gains, verify that price/ads/promotion features are not merely
encoding source rollout or historical data availability changes.

Reads only the current strict snapshot dataset.  No tables are modified.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as v1
from jobs.forecast_research import build_new_visible_snapshots_v2 as v2

AGES = (7, 14, 30, 60, 90)
FEATURES = [
    "clicks_7d",
    "impressions_7d",
    "ad_spend_7d",
    "ad_orders_7d",
    "ad_sales_7d",
    "promotion_units_7d",
    "avg_price_7d",
    "ad_spend_30d",
    "promotion_units_30d",
    "avg_price_30d",
]


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def temporal_block(ts: pd.Timestamp) -> str:
    return f"{ts.year}H{1 if ts.month <= 6 else 2}"


def rate(n: int, d: int):
    return round(n / d, 4) if d else None


def summarize(rows: pd.DataFrame, group: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "group": group,
        "rows": int(len(rows)),
        "launches": int(rows["launch_key"].nunique()),
    }
    for col in FEATURES:
        s = pd.to_numeric(rows[col], errors="coerce")
        nonnull = int(s.notna().sum())
        positive = int((s.fillna(0) > 0).sum())
        out[col + "_nonnull_rate"] = rate(nonnull, len(rows))
        out[col + "_positive_rate"] = rate(positive, len(rows))
        if nonnull:
            out[col + "_median_nonnull"] = round(float(s.dropna().median()), 4)
        else:
            out[col + "_median_nonnull"] = None
    return out


def main() -> int:
    rows = q(
        f"""
        SELECT snapshot_date, store_name, spu, age_days,
               {','.join('`' + f + '`' for f in FEATURES)}
        FROM `{v1.SNAPSHOT_TABLE}`
        WHERE dataset_version=%s
          AND age_days IN ({','.join(['%s'] * len(AGES))})
        ORDER BY snapshot_date, store_name, spu, age_days
        """,
        (v2.DATASET_VERSION, *AGES),
    )
    if not rows:
        raise RuntimeError("当前dataset_version没有固定年龄snapshot")

    df = pd.DataFrame(rows)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["launch_key"] = df["store_name"].astype(str) + "|" + df["spu"].astype(str)
    df["block"] = df["snapshot_date"].map(temporal_block)

    print("FEATURE_STABILITY_SCOPE=" + json.dumps({
        "dataset_version": v2.DATASET_VERSION,
        "ages": list(AGES),
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "features": FEATURES,
    }, ensure_ascii=False))

    print("\n=== OPTIONAL_FEATURES_BY_TIME ===")
    by_time = []
    for block in sorted(df["block"].unique()):
        result = summarize(df[df["block"] == block], block)
        by_time.append(result)
        print(json.dumps(result, ensure_ascii=False))

    print("\n=== OPTIONAL_FEATURES_BY_STORE ===")
    for store in sorted(df["store_name"].astype(str).unique()):
        print(json.dumps(summarize(df[df["store_name"].astype(str) == store], store), ensure_ascii=False))

    print("\n=== OPTIONAL_FEATURE_DRIFT_FLAGS ===")
    flags = []
    for col in FEATURES:
        nonnull_rates = [r[col + "_nonnull_rate"] for r in by_time if r[col + "_nonnull_rate"] is not None]
        positive_rates = [r[col + "_positive_rate"] for r in by_time if r[col + "_positive_rate"] is not None]
        nonnull_span = max(nonnull_rates) - min(nonnull_rates) if nonnull_rates else None
        positive_span = max(positive_rates) - min(positive_rates) if positive_rates else None
        whole_missing_blocks = [
            r["group"] for r in by_time if (r[col + "_nonnull_rate"] or 0) == 0
        ]
        flag = {
            "feature": col,
            "nonnull_span": round(nonnull_span, 4) if nonnull_span is not None else None,
            "positive_span": round(positive_span, 4) if positive_span is not None else None,
            "whole_missing_blocks": whole_missing_blocks,
            "risk": (
                "HIGH" if whole_missing_blocks or (nonnull_span is not None and nonnull_span >= 0.30)
                else "MEDIUM" if (positive_span is not None and positive_span >= 0.40)
                else "LOW"
            ),
        }
        flags.append(flag)
        print(json.dumps(flag, ensure_ascii=False))

    print("\n=== GUIDANCE ===")
    print("1. HIGH: 不应直接进入首版core模型；优先做core-vs-enriched消融。")
    print("2. MEDIUM: 可能是真实投放/促销变化，也可能是覆盖漂移；需要结合时间块判断。")
    print("3. avg_price若早期整块缺失，首版core模型建议先去掉price，再单独验证增益。")
    print("4. 本审计只判断稳定性，不证明特征因果关系。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
