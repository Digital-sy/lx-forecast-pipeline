#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Score current NEW_VISIBLE with DIRECT60 Q50/Q75 for stock-fabric replenishment shadow.

Research/shadow only. No production procurement table writes.

H60 is selected because current production procurement covers stock fabrics for 2 months.
Exact validated checkpoints use AGE calibration; interpolated ages 7..120 use GLOBAL.
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd

from common.database import db_cursor
from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2
from scripts import train_new_visible_v1_stage2_quantiles as quant
from scripts import train_new_visible_direct_procurement_horizons as ph
from scripts.materialize_new_visible_live_core import DEST_TABLE as CORE_TABLE

HORIZON = 60
PRED_TABLE = "forecast_new_visible_h60_prediction_daily"
H48_PRED_TABLE = "forecast_new_visible_h48_prediction_daily"
MODEL_VERSION = "DIRECT60_CORE_SHADOW_V1_MONOTONIC_H48"
QUANTILES = (0.50, 0.75)


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


def table_exists(name: str) -> bool:
    row = one(
        "SELECT COUNT(*) AS n FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s",
        (name,),
    )
    return int(row.get("n", 0) or 0) > 0


def ensure_table() -> None:
    with db_cursor() as c:
        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PRED_TABLE} (
              snapshot_date DATE NOT NULL,
              as_of_date DATE NOT NULL,
              store_name VARCHAR(200) NOT NULL,
              spu VARCHAR(200) NOT NULL,
              first_sale_day DATE NOT NULL,
              age_days INT NOT NULL,
              age_band_checkpoint INT DEFAULT NULL,
              checkpoint_validated TINYINT(1) NOT NULL DEFAULT 0,
              shadow_quantity_eligible TINYINT(1) NOT NULL DEFAULT 0,
              raw_q50 DECIMAL(18,2) DEFAULT NULL,
              global_q50 DECIMAL(18,2) DEFAULT NULL,
              age_q50 DECIMAL(18,2) DEFAULT NULL,
              raw_q75 DECIMAL(18,2) DEFAULT NULL,
              global_q75 DECIMAL(18,2) DEFAULT NULL,
              age_q75 DECIMAL(18,2) DEFAULT NULL,
              selected_q50 DECIMAL(18,2) DEFAULT NULL,
              selected_q75 DECIMAL(18,2) DEFAULT NULL,
              selected_calibration VARCHAR(40) NOT NULL,
              model_version VARCHAR(100) NOT NULL,
              calibration_history_rows INT NOT NULL DEFAULT 0,
              reason_code VARCHAR(100) NOT NULL,
              materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (snapshot_date,store_name,spu),
              INDEX idx_h60_shop (snapshot_date,store_name),
              INDEX idx_h60_age (snapshot_date,age_days)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def load_live() -> pd.DataFrame:
    latest = one(f"SELECT MAX(snapshot_date) AS d FROM {CORE_TABLE}").get("d")
    if not latest:
        raise RuntimeError(f"{CORE_TABLE} is empty")
    rows = q(f"SELECT * FROM {CORE_TABLE} WHERE snapshot_date=%s", (latest,))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no live CORE rows")
    for c in ("snapshot_date","as_of_date","first_sale_day"):
        df[c] = pd.to_datetime(df[c])
    for c in quant.abl.CORE_NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


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


def build_strict_oos(df: pd.DataFrame) -> pd.DataFrame:
    features = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    ycol = f"future_sales_{HORIZON}d"
    out = []
    for fold, start, end in base.TEST_FOLDS:
        train, test = ph.prepare_fold(df, start, end)
        if len(train) < ph.MIN_TRAIN_ROWS or test.empty:
            continue
        ylog = np.log1p(train[ycol].to_numpy(float))
        row = test[[
            "snapshot_date","label_end_date","store_name","spu",
            "launch_key","age_days",ycol
        ]].copy()
        row["fold"] = fold
        for qv in QUANTILES:
            model = quant.make_quantile_regressor(qv)
            model.fit(
                train[features],
                ylog,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            pred = np.maximum(np.expm1(model.predict(test[features])), 1e-6)
            row[f"q{int(qv*100)}_raw"] = pred
        out.append(row)
    if not out:
        raise RuntimeError("no H60 strict OOS rows")
    return pd.concat(out, ignore_index=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    live = load_live()
    snapshot_date = pd.Timestamp(live["snapshot_date"].max())
    as_of = pd.Timestamp(live["as_of_date"].max())
    live_keys = set(live["store_name"].astype(str) + "|" + live["spu"].astype(str))

    hist_df = ph.load_rows_horizon(HORIZON)
    hist_df["launch_key"] = hist_df["store_name"].astype(str) + "|" + hist_df["spu"].astype(str)
    ycol = f"future_sales_{HORIZON}d"

    mature_train = hist_df[
        (hist_df["label_end_date"] <= as_of)
        & (~hist_df["launch_key"].isin(live_keys))
    ].copy()
    if len(mature_train) < ph.MIN_TRAIN_ROWS:
        raise RuntimeError("insufficient mature H60 training rows")

    oos = build_strict_oos(hist_df)
    cal_history = oos[oos["label_end_date"] <= as_of].copy()
    if cal_history.empty:
        raise RuntimeError("no mature H60 OOS calibration history")

    factors = {
        qv: ph.build_factors(cal_history, HORIZON, qv)
        for qv in QUANTILES
    }

    features = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    ylog = np.log1p(mature_train[ycol].to_numpy(float))
    models = {}
    raw = {}
    for qv in QUANTILES:
        model = quant.make_quantile_regressor(qv)
        model.fit(
            mature_train[features],
            ylog,
            reg__sample_weight=stage2.launch_balanced_weights(mature_train),
        )
        models[qv] = model
        raw[qv] = np.maximum(np.expm1(model.predict(live[features])), 0.0)

    h48_map = {}
    if table_exists(H48_PRED_TABLE):
        h48_rows = q(
            f"SELECT snapshot_date,store_name,spu,checkpoint_validated,"
            f"global_q50,age_q50,global_q75,age_q75 "
            f"FROM {H48_PRED_TABLE} WHERE snapshot_date=%s",
            (snapshot_date.date(),),
        )
        for x in h48_rows:
            use_age = int(x.get("checkpoint_validated", 0) or 0) == 1
            q50 = x.get("age_q50") if use_age else x.get("global_q50")
            q75 = x.get("age_q75") if use_age else x.get("global_q75")
            h48_map[(str(x.get("store_name") or ""), str(x.get("spu") or ""))] = (
                float(q50 or 0), float(q75 or 0)
            )

    rows = []
    monotonic_adjusted_q50 = 0
    monotonic_adjusted_q75 = 0
    for i, r in live.reset_index(drop=True).iterrows():
        age = int(r["age_days"])
        band = age_band(age)
        checkpoint_validated = int(age in base.CHECKPOINT_AGES)
        shadow_quantity_eligible = int(7 <= age <= 120)

        vals = {}
        for qv in QUANTILES:
            info = factors[qv]
            raw_v = float(raw[qv][i])
            global_v = raw_v * float(info["global_factor"])
            if band is None:
                age_v = None
            else:
                age_v = raw_v * float(info["age_factors"][band]["factor"])
            vals[qv] = {"raw": raw_v, "global": global_v, "age": age_v}

        vals[0.75]["global"] = max(vals[0.75]["global"], vals[0.50]["global"])
        if vals[0.75]["age"] is not None and vals[0.50]["age"] is not None:
            vals[0.75]["age"] = max(vals[0.75]["age"], vals[0.50]["age"])

        if checkpoint_validated and vals[0.50]["age"] is not None:
            selected_cal = "AGE_EXACT_CHECKPOINT"
            selected_q50 = vals[0.50]["age"]
            selected_q75 = vals[0.75]["age"]
            reason = "H60_VALIDATED_CHECKPOINT_SHADOW"
        elif shadow_quantity_eligible:
            selected_cal = "GLOBAL_INTERPOLATED"
            selected_q50 = vals[0.50]["global"]
            selected_q75 = vals[0.75]["global"]
            reason = "H60_INTERPOLATED_SHADOW"
        else:
            selected_cal = "WATCH_ONLY"
            selected_q50 = vals[0.50]["global"]
            selected_q75 = vals[0.75]["global"]
            reason = "WATCH_ONLY_AGE_OUTSIDE_7_120"

        h48_pair = h48_map.get((str(r["store_name"]), str(r["spu"])))
        if h48_pair is not None:
            before50, before75 = selected_q50, selected_q75
            selected_q50 = max(selected_q50, h48_pair[0])
            selected_q75 = max(selected_q75, h48_pair[1], selected_q50)
            if selected_q50 > before50 + 1e-9:
                monotonic_adjusted_q50 += 1
            if selected_q75 > before75 + 1e-9:
                monotonic_adjusted_q75 += 1

        rows.append({
            "snapshot_date": snapshot_date.date(),
            "as_of_date": as_of.date(),
            "store_name": str(r["store_name"]),
            "spu": str(r["spu"]),
            "first_sale_day": pd.Timestamp(r["first_sale_day"]).date(),
            "age_days": age,
            "age_band_checkpoint": band,
            "checkpoint_validated": checkpoint_validated,
            "shadow_quantity_eligible": shadow_quantity_eligible,
            "raw_q50": vals[0.50]["raw"],
            "global_q50": vals[0.50]["global"],
            "age_q50": vals[0.50]["age"],
            "raw_q75": vals[0.75]["raw"],
            "global_q75": vals[0.75]["global"],
            "age_q75": vals[0.75]["age"],
            "selected_q50": selected_q50,
            "selected_q75": selected_q75,
            "selected_calibration": selected_cal,
            "model_version": MODEL_VERSION,
            "calibration_history_rows": int(len(cal_history)),
            "reason_code": reason,
        })

    print("LIVE_H60_SCOPE=" + json.dumps({
        "snapshot_date": str(snapshot_date.date()),
        "as_of_date": str(as_of.date()),
        "live_rows": len(rows),
        "shadow_quantity_eligible_rows": sum(x["shadow_quantity_eligible"] for x in rows),
        "checkpoint_validated_rows": sum(x["checkpoint_validated"] for x in rows),
        "training_rows": int(len(mature_train)),
        "training_launches": int(mature_train["launch_key"].nunique()),
        "calibration_history_rows": int(len(cal_history)),
        "global_factor_q50": factors[0.50]["global_factor"],
        "global_factor_q75": factors[0.75]["global_factor"],
        "h48_rows_joined": len(h48_map),
        "monotonic_adjusted_q50_rows": monotonic_adjusted_q50,
        "monotonic_adjusted_q75_rows": monotonic_adjusted_q75,
        "dry_run": args.dry_run,
    }, ensure_ascii=False))

    for x in rows[:20]:
        print("LIVE_H60_SAMPLE=" + json.dumps({
            k: x[k] for k in (
                "store_name","spu","age_days","selected_calibration",
                "selected_q50","selected_q75","reason_code"
            )
        }, ensure_ascii=False))

    if not args.dry_run:
        ensure_table()
        cols = [
            "snapshot_date","as_of_date","store_name","spu","first_sale_day",
            "age_days","age_band_checkpoint","checkpoint_validated",
            "shadow_quantity_eligible",
            "raw_q50","global_q50","age_q50",
            "raw_q75","global_q75","age_q75",
            "selected_q50","selected_q75","selected_calibration",
            "model_version","calibration_history_rows","reason_code",
        ]
        sql = (
            f"INSERT INTO {PRED_TABLE} ({','.join(cols)}) VALUES "
            f"({','.join(['%s']*len(cols))}) ON DUPLICATE KEY UPDATE "
            + ",".join(
                f"{c}=VALUES({c})"
                for c in cols if c not in ("snapshot_date","store_name","spu")
            )
        )
        payload = [tuple(x.get(c) for c in cols) for x in rows]
        with db_cursor() as c:
            c.executemany(sql, payload)
        print("LIVE_H60_PERSISTED=" + json.dumps({
            "table": PRED_TABLE,
            "snapshot_date": str(snapshot_date.date()),
            "rows": len(rows),
        }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
