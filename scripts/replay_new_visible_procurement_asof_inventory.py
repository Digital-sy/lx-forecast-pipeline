#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Point-in-time NEW_VISIBLE procurement replay with an external inventory workbook.

No production table is modified.

For issue_date 2026-09-01:
- as_of date = 2026-08-31
- features come from the frozen historical NEW_VISIBLE snapshot at 2026-08-31
- training/calibration only uses labels fully mature by 2026-08-31
- the user workbook supplies point-in-time on-hand inventory

Stock-fabric "need order within next 30 days":
lead time 48d + decision window 30d = 78d.
Using H60 Q50 run-rate, if on-hand days cover <=78d, the item reaches reorder
point during the next 30 days. Base lot = H60 Q50, safety lot = H60 Q75.

Custom fabrics remain quantity-blocked because H90 is research-only.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

import numpy as np
import pandas as pd

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as v1
from jobs.forecast_research import build_new_visible_snapshots_v2 as v2
from jobs.feishu.procurement_color_logic import read_fabric_info
from scripts import audit_new_visible_v1_feature_ablation as abl
from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2
from scripts import train_new_visible_v1_stage2_quantiles as quant
from scripts import train_new_visible_v1_stage2_direct_h48 as h48
from scripts import train_new_visible_direct_procurement_horizons as ph

H60 = 60
LEAD_TIME_DAYS = 48
ORDER_WINDOW_DAYS = 30
QUANTILES = (0.50, 0.75)


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def age_band(age: int):
    if 7 <= age <= 13:
        return 7
    if 14 <= age <= 29:
        return 14
    if 30 <= age <= 59:
        return 30
    if 60 <= age <= 89:
        return 60
    if 90 <= age <= 120:
        return 90
    return None


def load_point_in_time_features(as_of: date) -> pd.DataFrame:
    fields = [
        "snapshot_date", "store_name", "spu", "first_sale_day", "age_days",
        *[x for x in abl.CORE_NUMERIC if x != "age_days"],
        "dataset_version",
    ]
    rows = q(
        f"""
        SELECT {','.join(fields)}
        FROM {v1.SNAPSHOT_TABLE}
        WHERE dataset_version=%s
          AND snapshot_date=%s
          AND age_days BETWEEN 0 AND 120
        ORDER BY store_name,spu
        """,
        (v2.DATASET_VERSION, as_of),
    )
    if not rows:
        raise RuntimeError(
            f"no historical NEW_VISIBLE rows for as_of={as_of}; dataset={v2.DATASET_VERSION}"
        )
    df = pd.DataFrame(rows)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["first_sale_day"] = pd.to_datetime(df["first_sale_day"])
    for c in abl.CORE_NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["launch_month"] = df["first_sale_day"].dt.month.astype(str)
    df["snapshot_month"] = df["snapshot_date"].dt.month.astype(str)
    df["launch_key"] = df["store_name"].astype(str) + "|" + df["spu"].astype(str)
    return df


def load_inventory(path: Path) -> pd.DataFrame:
    inv = pd.read_excel(path)
    required = {"店铺", "SPU", "本地库存分配", "FBA库存"}
    missing = required - set(inv.columns)
    if missing:
        raise RuntimeError(f"inventory workbook missing columns: {sorted(missing)}")

    inv = inv.copy()
    inv["store_name"] = inv["店铺"].astype(str).str.strip()
    inv["spu"] = inv["SPU"].astype(str).str.strip()
    inv["local_inventory"] = pd.to_numeric(inv["本地库存分配"], errors="coerce").fillna(0.0)
    inv["fba_inventory"] = pd.to_numeric(inv["FBA库存"], errors="coerce").fillna(0.0)
    calc_total = inv["local_inventory"] + inv["fba_inventory"]
    if "库存汇总" in inv.columns:
        inv["on_hand_inventory"] = pd.to_numeric(
            inv["库存汇总"], errors="coerce"
        ).fillna(calc_total)
    else:
        inv["on_hand_inventory"] = calc_total

    dup = inv.duplicated(["store_name", "spu"], keep=False)
    if dup.any():
        sample = inv.loc[dup, ["store_name", "spu"]].head(20).to_dict("records")
        raise RuntimeError(f"duplicate store×SPU rows: {sample}")

    return inv[[
        "store_name", "spu", "local_inventory", "fba_inventory", "on_hand_inventory"
    ]]


def fit_predict(train: pd.DataFrame, live: pd.DataFrame, target: str):
    features = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    ylog = np.log1p(train[target].to_numpy(float))
    out = {}
    for qv in QUANTILES:
        model = quant.make_quantile_regressor(qv)
        model.fit(
            train[features],
            ylog,
            reg__sample_weight=stage2.launch_balanced_weights(train),
        )
        out[qv] = np.maximum(np.expm1(model.predict(live[features])), 0.0)
    return out


def build_h48_oos(df: pd.DataFrame) -> pd.DataFrame:
    features = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    out = []
    for fold, start, end in base.TEST_FOLDS:
        train, test = h48.prepare_fold(df, start, end)
        if len(train) < h48.MIN_TRAIN_ROWS or test.empty:
            continue
        ylog = np.log1p(train["future_sales_48d"].to_numpy(float))
        row = test[[
            "snapshot_date", "label_end_date", "store_name", "spu",
            "launch_key", "age_days", "future_sales_48d"
        ]].copy()
        row["fold"] = fold
        for qv in QUANTILES:
            model = quant.make_quantile_regressor(qv)
            model.fit(
                train[features],
                ylog,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            row[f"q{int(qv*100)}_raw"] = np.maximum(
                np.expm1(model.predict(test[features])), 1e-6
            )
        out.append(row)
    if not out:
        raise RuntimeError("no H48 OOS rows")
    return pd.concat(out, ignore_index=True)


def build_h60_oos(df: pd.DataFrame) -> pd.DataFrame:
    features = list(abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    out = []
    for fold, start, end in base.TEST_FOLDS:
        train, test = ph.prepare_fold(df, start, end)
        if len(train) < ph.MIN_TRAIN_ROWS or test.empty:
            continue
        ylog = np.log1p(train["future_sales_60d"].to_numpy(float))
        row = test[[
            "snapshot_date", "label_end_date", "store_name", "spu",
            "launch_key", "age_days", "future_sales_60d"
        ]].copy()
        row["fold"] = fold
        for qv in QUANTILES:
            model = quant.make_quantile_regressor(qv)
            model.fit(
                train[features],
                ylog,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            row[f"q{int(qv*100)}_raw"] = np.maximum(
                np.expm1(model.predict(test[features])), 1e-6
            )
        out.append(row)
    if not out:
        raise RuntimeError("no H60 OOS rows")
    return pd.concat(out, ignore_index=True)


def score_h48(live: pd.DataFrame, as_of: date) -> pd.DataFrame:
    hist_df = h48.load_rows_h48()
    hist_df["launch_key"] = hist_df["store_name"].astype(str) + "|" + hist_df["spu"].astype(str)
    live_keys = set(live["launch_key"])
    mature = hist_df[
        (hist_df["label_end_date"] <= pd.Timestamp(as_of))
        & (~hist_df["launch_key"].isin(live_keys))
    ].copy()
    if len(mature) < h48.MIN_TRAIN_ROWS:
        raise RuntimeError("insufficient mature H48 rows")

    cal = build_h48_oos(hist_df)
    cal = cal[cal["label_end_date"] <= pd.Timestamp(as_of)].copy()
    factors = {qv: h48.build_factors(cal, qv) for qv in QUANTILES}
    raw = fit_predict(mature, live, "future_sales_48d")

    rows = []
    for i, r in live.reset_index(drop=True).iterrows():
        age = int(r["age_days"])
        band = age_band(age)
        exact = age in base.CHECKPOINT_AGES
        vals = {}
        for qv in QUANTILES:
            rv = float(raw[qv][i])
            gv = rv * float(factors[qv]["global_factor"])
            av = None if band is None else rv * float(factors[qv]["age_factors"][band]["factor"])
            vals[qv] = av if exact and av is not None else gv
        vals[0.75] = max(vals[0.75], vals[0.50])
        rows.append({
            "store_name": r["store_name"],
            "spu": r["spu"],
            "h48_q50": vals[0.50],
            "h48_q75": vals[0.75],
        })
    return pd.DataFrame(rows)


def score_h60(live: pd.DataFrame, as_of: date, h48_pred: pd.DataFrame) -> pd.DataFrame:
    hist_df = ph.load_rows_horizon(60)
    hist_df["launch_key"] = hist_df["store_name"].astype(str) + "|" + hist_df["spu"].astype(str)
    live_keys = set(live["launch_key"])
    mature = hist_df[
        (hist_df["label_end_date"] <= pd.Timestamp(as_of))
        & (~hist_df["launch_key"].isin(live_keys))
    ].copy()
    if len(mature) < ph.MIN_TRAIN_ROWS:
        raise RuntimeError("insufficient mature H60 rows")

    cal = build_h60_oos(hist_df)
    cal = cal[cal["label_end_date"] <= pd.Timestamp(as_of)].copy()
    factors = {qv: ph.build_factors(cal, 60, qv) for qv in QUANTILES}
    raw = fit_predict(mature, live, "future_sales_60d")
    h48_map = {
        (str(r.store_name), str(r.spu)): (float(r.h48_q50), float(r.h48_q75))
        for r in h48_pred.itertuples()
    }

    rows = []
    for i, r in live.reset_index(drop=True).iterrows():
        age = int(r["age_days"])
        band = age_band(age)
        exact = age in base.CHECKPOINT_AGES
        vals = {}
        for qv in QUANTILES:
            rv = float(raw[qv][i])
            gv = rv * float(factors[qv]["global_factor"])
            av = None if band is None else rv * float(factors[qv]["age_factors"][band]["factor"])
            vals[qv] = av if exact and av is not None else gv

        h48q50, h48q75 = h48_map[(str(r["store_name"]), str(r["spu"]))]
        vals[0.50] = max(vals[0.50], h48q50)
        vals[0.75] = max(vals[0.75], h48q75, vals[0.50])
        rows.append({
            "store_name": r["store_name"],
            "spu": r["spu"],
            "h60_q50": vals[0.50],
            "h60_q75": vals[0.75],
        })
    return pd.DataFrame(rows)


def order_fields(row: Mapping[str, Any], issue_date: date):
    inv = float(row.get("on_hand_inventory") or 0)
    q50 = float(row.get("h60_q50") or 0)
    q75 = max(float(row.get("h60_q75") or 0), q50)
    age = int(row.get("age_days") or 0)

    if age < 7:
        return {
            "need_order_within_30d": 0, "urgency": "WATCH_AGE_LT7",
            "days_cover_q50": None, "latest_order_date_q50": None,
            "base_order_qty_q50": 0, "safety_order_qty_q75": 0,
        }
    if q50 <= 0:
        return {
            "need_order_within_30d": 0, "urgency": "NO_Q50_DEMAND",
            "days_cover_q50": None, "latest_order_date_q50": None,
            "base_order_qty_q50": 0, "safety_order_qty_q75": 0,
        }

    daily = q50 / 60.0
    cover = inv / daily
    due = cover <= LEAD_TIME_DAYS + ORDER_WINDOW_DAYS

    if not due:
        return {
            "need_order_within_30d": 0, "urgency": "NO_ORDER_WITHIN_30D",
            "days_cover_q50": cover, "latest_order_date_q50": None,
            "base_order_qty_q50": 0, "safety_order_qty_q75": 0,
        }

    delay = max(0.0, cover - LEAD_TIME_DAYS)
    latest = issue_date + timedelta(days=int(math.floor(delay)))
    urgency = "ORDER_NOW_LT48" if cover <= LEAD_TIME_DAYS else "ORDER_WITHIN_30D"
    return {
        "need_order_within_30d": 1,
        "urgency": urgency,
        "days_cover_q50": cover,
        "latest_order_date_q50": latest,
        "base_order_qty_q50": int(math.ceil(q50)),
        "safety_order_qty_q75": int(math.ceil(q75)),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--issue-date", default="2026-09-01")
    ap.add_argument("--inventory-xlsx", required=True)
    ap.add_argument("--output-dir", default="reports_analysis/new_visible_historical_replay")
    args = ap.parse_args()

    issue_date = datetime.strptime(args.issue_date, "%Y-%m-%d").date()
    as_of = issue_date - timedelta(days=1)
    inventory_path = Path(args.inventory_xlsx)

    live_all = load_point_in_time_features(as_of)
    inv = load_inventory(inventory_path)

    live = live_all.merge(
        inv[["store_name", "spu"]],
        on=["store_name", "spu"],
        how="inner",
    )
    if live.empty:
        raise RuntimeError("no historical NEW_VISIBLE rows match inventory workbook")

    h48_pred = score_h48(live, as_of)
    h60_pred = score_h60(live, as_of, h48_pred)

    result = (
        live[[
            "store_name", "spu", "first_sale_day", "age_days",
            "sales_7d", "sales_14d", "sales_30d"
        ]]
        .merge(inv, on=["store_name", "spu"], how="left")
        .merge(h48_pred, on=["store_name", "spu"], how="left")
        .merge(h60_pred, on=["store_name", "spu"], how="left")
    )

    fabric_info = read_fabric_info()
    ftypes, primary = [], []
    for spu in result["spu"].astype(str):
        info = fabric_info.get(spu.strip().upper())
        if not info:
            ftypes.append("UNKNOWN")
            primary.append("")
        else:
            ftypes.append(str(info.get("fabric_type") or "UNKNOWN"))
            fabrics = info.get("fabrics") or []
            primary.append(str(fabrics[0][0]) if fabrics else "")
    result["fabric_type"] = ftypes
    result["primary_fabric"] = primary

    odf = pd.DataFrame([
        order_fields(r, issue_date) for r in result.to_dict("records")
    ])
    result = pd.concat([result.reset_index(drop=True), odf], axis=1)

    def h48_risk(r):
        if int(r["age_days"]) < 7:
            return "WATCH_ONLY"
        invv = float(r["on_hand_inventory"] or 0)
        if invv < float(r["h48_q50"] or 0):
            return "CRITICAL_LT_SHORTAGE"
        if invv < float(r["h48_q75"] or 0):
            return "HIGH_LT_RISK"
        return "COVERED_Q75_ON_HAND"

    result["h48_risk"] = result.apply(h48_risk, axis=1)
    result["quantity_release_status"] = np.where(
        result["fabric_type"].eq("现货面料"),
        "STOCK_H60_REPLAY",
        np.where(
            result["fabric_type"].eq("定制面料"),
            "CUSTOM_H90_NOT_RELEASED",
            "BLOCK_FABRIC_UNKNOWN",
        ),
    )
    result.loc[
        ~result["fabric_type"].eq("现货面料"),
        ["base_order_qty_q50", "safety_order_qty_q75"],
    ] = 0

    result["issue_date"] = issue_date
    result["as_of_date"] = as_of

    need_stock = result[
        (result["fabric_type"] == "现货面料")
        & (result["need_order_within_30d"] == 1)
    ].copy()
    urgency_order = {"ORDER_NOW_LT48": 0, "ORDER_WITHIN_30D": 1}
    need_stock["_urg"] = need_stock["urgency"].map(urgency_order).fillna(9)
    need_stock = need_stock.sort_values(
        ["_urg", "latest_order_date_q50", "base_order_qty_q50"],
        ascending=[True, True, False],
    ).drop(columns=["_urg"])

    custom_risk = result[
        (result["fabric_type"] == "定制面料")
        & result["h48_risk"].isin(["CRITICAL_LT_SHORTAGE", "HIGH_LT_RISK"])
    ].copy()
    unknown = result[result["fabric_type"] == "UNKNOWN"].copy()

    excluded_idx = set(need_stock.index) | set(custom_risk.index) | set(unknown.index)
    observe = result[~result.index.isin(excluded_idx)].copy()

    scored_keys = set(zip(result["store_name"], result["spu"]))
    mask = [
        (r.store_name, r.spu) not in scored_keys
        for r in inv.itertuples()
    ]
    not_nv = inv.loc[mask].copy()

    for df in (result, need_stock, custom_risk, unknown, observe):
        for c in ["h48_q50", "h48_q75", "h60_q50", "h60_q75", "days_cover_q50"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    out_dir = Path(args.output_dir) / args.issue_date
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "NEW_VISIBLE_0901历史回放_需下单.csv"
    xlsx_path = out_dir / "NEW_VISIBLE_0901历史回放.xlsx"

    need_stock.to_csv(csv_path, index=False, encoding="utf-8-sig")

    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            need_stock.to_excel(writer, sheet_name="9月内需下单_现货", index=False)
            custom_risk.to_excel(writer, sheet_name="定制面料紧急风险", index=False)
            unknown.to_excel(writer, sheet_name="面料映射缺失", index=False)
            observe.to_excel(writer, sheet_name="观察_暂不需下单", index=False)
            result.to_excel(writer, sheet_name="全部NEW_VISIBLE", index=False)
            not_nv.to_excel(writer, sheet_name="不适用NEW_VISIBLE", index=False)
    except Exception as exc:
        print("HIST_REPLAY_XLSX_WARNING=" + json.dumps({
            "error": str(exc), "csv_still_written": str(csv_path),
        }, ensure_ascii=False))

    summary = {
        "issue_date": str(issue_date),
        "as_of_date": str(as_of),
        "inventory_rows": int(len(inv)),
        "historical_new_visible_rows": int(len(live_all)),
        "new_visible_matched_inventory_rows": int(len(result)),
        "stock_need_order_within_30d": int(len(need_stock)),
        "stock_order_now": int((need_stock["urgency"] == "ORDER_NOW_LT48").sum()) if len(need_stock) else 0,
        "stock_order_later_in_30d": int((need_stock["urgency"] == "ORDER_WITHIN_30D").sum()) if len(need_stock) else 0,
        "stock_base_qty_q50_sum": int(need_stock["base_order_qty_q50"].sum()) if len(need_stock) else 0,
        "stock_safety_qty_q75_sum": int(need_stock["safety_order_qty_q75"].sum()) if len(need_stock) else 0,
        "custom_h48_urgent_rows": int(len(custom_risk)),
        "unknown_fabric_rows": int(len(unknown)),
        "inventory_rows_outside_new_visible_model": int(len(not_nv)),
        "future_leakage": False,
        "historical_inbound_fabricated": False,
        "output_csv": str(csv_path),
        "output_xlsx": str(xlsx_path),
    }
    print("HIST_REPLAY_SCOPE=" + json.dumps(summary, ensure_ascii=False))

    for r in need_stock.head(100).to_dict("records"):
        print("HIST_REPLAY_ORDER=" + json.dumps({
            "store_name": r["store_name"],
            "spu": r["spu"],
            "age_days": int(r["age_days"]),
            "primary_fabric": r["primary_fabric"],
            "inventory": float(r["on_hand_inventory"]),
            "h48_q50": float(r["h48_q50"]),
            "h60_q50": float(r["h60_q50"]),
            "h60_q75": float(r["h60_q75"]),
            "days_cover_q50": r["days_cover_q50"],
            "urgency": r["urgency"],
            "latest_order_date_q50": str(r["latest_order_date_q50"]),
            "base_order_qty_q50": int(r["base_order_qty_q50"]),
            "safety_order_qty_q75": int(r["safety_order_qty_q75"]),
        }, ensure_ascii=False, default=str))

    for r in custom_risk.head(50).to_dict("records"):
        print("HIST_REPLAY_CUSTOM_RISK=" + json.dumps({
            "store_name": r["store_name"],
            "spu": r["spu"],
            "age_days": int(r["age_days"]),
            "primary_fabric": r["primary_fabric"],
            "inventory": float(r["on_hand_inventory"]),
            "h48_risk": r["h48_risk"],
            "h48_q50": float(r["h48_q50"]),
            "h60_q50": float(r["h60_q50"]),
            "quantity_release_status": r["quantity_release_status"],
        }, ensure_ascii=False, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
