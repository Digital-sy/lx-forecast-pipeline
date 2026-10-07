#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Score current NEW_VISIBLE with DIRECT48 Q50/Q75 in shadow.

Requires scripts/materialize_new_visible_live_core.py to have materialized the latest
training-identical CORE feature rows.

Writes only forecast_new_visible_h48_prediction_daily.
No production forecast/procurement table is touched.
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from common.database import db_cursor
from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2
from scripts import train_new_visible_v1_stage2_quantiles as quant
from scripts import train_new_visible_v1_stage2_direct_h48 as h48
from scripts.materialize_new_visible_live_core import DEST_TABLE as CORE_TABLE

PRED_TABLE = "forecast_new_visible_h48_prediction_daily"
MODEL_VERSION = "DIRECT48_CORE_SHADOW_V1"
QUANTILES = (0.50, 0.75)


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def one(sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    rows = q(sql, params)
    return rows[0] if rows else {}


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
              action_eligible TINYINT(1) NOT NULL DEFAULT 0,
              raw_q50 DECIMAL(18,2) DEFAULT NULL,
              global_q50 DECIMAL(18,2) DEFAULT NULL,
              age_q50 DECIMAL(18,2) DEFAULT NULL,
              raw_q75 DECIMAL(18,2) DEFAULT NULL,
              global_q75 DECIMAL(18,2) DEFAULT NULL,
              age_q75 DECIMAL(18,2) DEFAULT NULL,
              global_factor_q50 DECIMAL(16,6) DEFAULT NULL,
              age_factor_q50 DECIMAL(16,6) DEFAULT NULL,
              global_factor_q75 DECIMAL(16,6) DEFAULT NULL,
              age_factor_q75 DECIMAL(16,6) DEFAULT NULL,
              model_version VARCHAR(100) NOT NULL,
              calibration_history_rows INT NOT NULL DEFAULT 0,
              reason_code VARCHAR(100) NOT NULL,
              materialized_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (snapshot_date, store_name, spu),
              INDEX idx_h48_asof (as_of_date, store_name),
              INDEX idx_h48_age (snapshot_date, age_band_checkpoint)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def load_live() -> pd.DataFrame:
    latest = one(f"SELECT MAX(snapshot_date) AS d FROM {CORE_TABLE}").get("d")
    if not latest:
        raise RuntimeError(f"{CORE_TABLE} is empty; materialize live CORE first")
    rows = q(f"SELECT * FROM {CORE_TABLE} WHERE snapshot_date=%s", (latest,))
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no live CORE rows")
    for c in ("snapshot_date","as_of_date","first_sale_day"):
        df[c] = pd.to_datetime(df[c])
    for c in h48.quant.abl.CORE_NUMERIC:
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
    out = []
    for fold, start, end in base.TEST_FOLDS:
        train, test = h48.prepare_fold(df, start, end)
        if len(train) < h48.MIN_TRAIN_ROWS or test.empty:
            continue
        ylog = np.log1p(train["future_sales_48d"].to_numpy(float))
        row = test[[
            "snapshot_date","label_end_date","store_name","spu",
            "launch_key","age_days","future_sales_48d"
        ]].copy()
        row["fold"] = fold
        for qv in QUANTILES:
            model = quant.make_quantile_regressor(qv)
            model.fit(
                train[features],
                ylog,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            pred = np.expm1(model.predict(test[features]))
            row[f"q{int(qv*100)}_raw"] = np.maximum(pred, 1e-6)
        out.append(row)
    if not out:
        raise RuntimeError("no strict OOS H48 rows for live calibration")
    return pd.concat(out, ignore_index=True)


def fit_live_models(train: pd.DataFrame):
    features = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    ylog = np.log1p(train["future_sales_48d"].to_numpy(float))
    models = {}
    for qv in QUANTILES:
        model = quant.make_quantile_regressor(qv)
        model.fit(
            train[features],
            ylog,
            reg__sample_weight=stage2.launch_balanced_weights(train),
        )
        models[qv] = model
    return models, features


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    live = load_live()
    snapshot_date = pd.Timestamp(live["snapshot_date"].max())
    as_of = pd.Timestamp(live["as_of_date"].max())
    live_keys = set(
        live["store_name"].astype(str) + "|" + live["spu"].astype(str)
    )

    hist_df = h48.load_rows_h48()
    hist_df["launch_key"] = (
        hist_df["store_name"].astype(str) + "|" + hist_df["spu"].astype(str)
    )
    mature_train = hist_df[
        (hist_df["label_end_date"] <= as_of)
        & (~hist_df["launch_key"].isin(live_keys))
    ].copy()
    if len(mature_train) < h48.MIN_TRAIN_ROWS:
        raise RuntimeError("insufficient mature H48 training rows")

    oos = build_strict_oos(hist_df)
    cal_history = oos[oos["label_end_date"] <= as_of].copy()
    if cal_history.empty:
        raise RuntimeError("no mature OOS calibration history")

    factors = {
        qv: h48.build_factors(cal_history, qv)
        for qv in QUANTILES
    }

    models, features = fit_live_models(mature_train)
    raw = {}
    for qv in QUANTILES:
        raw[qv] = np.maximum(
            np.expm1(models[qv].predict(live[features])),
            0.0,
        )

    rows = []
    for i, r in live.reset_index(drop=True).iterrows():
        age = int(r["age_days"])
        band = age_band(age)
        action_eligible = int(band is not None)

        values = {}
        for qv in QUANTILES:
            info = factors[qv]
            raw_v = float(raw[qv][i])
            global_f = float(info["global_factor"])
            if band is None:
                age_f = None
                age_v = None
            else:
                age_f = float(info["age_factors"][band]["factor"])
                age_v = raw_v * age_f
            values[qv] = {
                "raw": raw_v,
                "global": raw_v * global_f,
                "age": age_v,
                "global_factor": global_f,
                "age_factor": age_f,
            }

        if values[0.75]["global"] < values[0.50]["global"]:
            values[0.75]["global"] = values[0.50]["global"]
        if (
            values[0.75]["age"] is not None
            and values[0.50]["age"] is not None
            and values[0.75]["age"] < values[0.50]["age"]
        ):
            values[0.75]["age"] = values[0.50]["age"]

        reason = (
            "H48_SHADOW_ELIGIBLE"
            if action_eligible
            else "WATCH_ONLY_AGE_OUTSIDE_7_120"
        )
        rows.append({
            "snapshot_date": snapshot_date.date(),
            "as_of_date": as_of.date(),
            "store_name": str(r["store_name"]),
            "spu": str(r["spu"]),
            "first_sale_day": pd.Timestamp(r["first_sale_day"]).date(),
            "age_days": age,
            "age_band_checkpoint": band,
            "action_eligible": action_eligible,
            "raw_q50": values[0.50]["raw"],
            "global_q50": values[0.50]["global"],
            "age_q50": values[0.50]["age"],
            "raw_q75": values[0.75]["raw"],
            "global_q75": values[0.75]["global"],
            "age_q75": values[0.75]["age"],
            "global_factor_q50": values[0.50]["global_factor"],
            "age_factor_q50": values[0.50]["age_factor"],
            "global_factor_q75": values[0.75]["global_factor"],
            "age_factor_q75": values[0.75]["age_factor"],
            "model_version": MODEL_VERSION,
            "calibration_history_rows": int(len(cal_history)),
            "reason_code": reason,
        })

    print("LIVE_H48_SCOPE=" + json.dumps({
        "snapshot_date": str(snapshot_date.date()),
        "as_of_date": str(as_of.date()),
        "live_rows": len(live),
        "action_eligible_rows": sum(x["action_eligible"] for x in rows),
        "training_rows": int(len(mature_train)),
        "training_launches": int(mature_train["launch_key"].nunique()),
        "calibration_history_rows": int(len(cal_history)),
        "excluded_live_launches_from_training": len(live_keys),
        "global_factors": {
            "q50": factors[0.50]["global_factor"],
            "q75": factors[0.75]["global_factor"],
        },
        "age_factors_q50": factors[0.50]["age_factors"],
        "age_factors_q75": factors[0.75]["age_factors"],
        "dry_run": args.dry_run,
    }, ensure_ascii=False))

    for x in rows[:20]:
        print("LIVE_H48_SAMPLE=" + json.dumps({
            k: x.get(k) for k in (
                "store_name","spu","age_days","age_band_checkpoint",
                "global_q50","age_q50","global_q75","age_q75","reason_code"
            )
        }, ensure_ascii=False))

    if not args.dry_run:
        ensure_table()
        cols = [
            "snapshot_date","as_of_date","store_name","spu","first_sale_day",
            "age_days","age_band_checkpoint","action_eligible",
            "raw_q50","global_q50","age_q50",
            "raw_q75","global_q75","age_q75",
            "global_factor_q50","age_factor_q50",
            "global_factor_q75","age_factor_q75",
            "model_version","calibration_history_rows","reason_code",
        ]
        sql = (
            f"INSERT INTO {PRED_TABLE} ({','.join(cols)}) VALUES "
            f"({','.join(['%s'] * len(cols))}) ON DUPLICATE KEY UPDATE "
            + ",".join(
                f"{c}=VALUES({c})"
                for c in cols
                if c not in ("snapshot_date","store_name","spu")
            )
        )
        payload = [tuple(r.get(c) for c in cols) for r in rows]
        with db_cursor() as c:
            c.executemany(sql, payload)
        print("LIVE_H48_PERSISTED=" + json.dumps({
            "table": PRED_TABLE,
            "rows": len(rows),
            "snapshot_date": str(snapshot_date.date()),
        }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
