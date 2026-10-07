#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Strict temporal OOS DIRECT full-volume Q50/Q75 for NEW_VISIBLE future48 demand.

Research-only. No DB writes, no model persistence, no production promotion.

Uses frozen point-in-time snapshot features, but derives a true 48-day label from
forecast_research_spu_daily_history:
    future_sales_48d = sum(snapshot_date+1 ... snapshot_date+48)

Only fully observed 48-day windows are eligible. The existing snapshot table is not
modified. Forward calibration uses only prior OOS rows whose 48-day labels are mature
before the next test fold starts.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd

from common.database import db_cursor
from jobs.forecast_research import build_new_visible_snapshots as v1
from jobs.forecast_research import build_new_visible_snapshots_v2 as v2
from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2
from scripts import train_new_visible_v1_stage2_quantiles as quant

HORIZON_DAYS = 48
QUANTILES = (0.50, 0.75)
METHODS = ("RAW", "GLOBAL", "AGE")
MODEL_PREFIX = "NV-ML-V1-STAGE2-DIRECT48-CORE"
MIN_AGE_CAL_ROWS = 40
MIN_TRAIN_ROWS = 200

def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())

def load_rows_h48() -> pd.DataFrame:
    df = base.load_rows().copy()
    max_row = q(f"SELECT MAX(dt) AS max_dt FROM `{v1.DAILY_TABLE}`")
    if not max_row or not max_row[0].get("max_dt"):
        raise RuntimeError("daily research history is empty")
    max_dt = pd.Timestamp(max_row[0]["max_dt"])

    ph = ",".join(["%s"] * len(base.CHECKPOINT_AGES))
    sql = f"""
        SELECT s.snapshot_date, s.store_name, s.spu,
               SUM(COALESCE(d.sales_units,0)) AS future_sales_48d
        FROM `{v1.SNAPSHOT_TABLE}` s
        LEFT JOIN `{v1.DAILY_TABLE}` d
          ON d.store_name=s.store_name
         AND d.spu=s.spu
         AND d.dt > s.snapshot_date
         AND d.dt <= DATE_ADD(s.snapshot_date, INTERVAL {HORIZON_DAYS} DAY)
        WHERE s.dataset_version=%s
          AND s.age_days IN ({ph})
          AND s.snapshot_date <= DATE_SUB(%s, INTERVAL {HORIZON_DAYS} DAY)
        GROUP BY s.snapshot_date, s.store_name, s.spu
    """
    labels = pd.DataFrame(q(
        sql, (v2.DATASET_VERSION, *base.CHECKPOINT_AGES, max_dt.date())
    ))
    if labels.empty:
        raise RuntimeError("no fully observed H48 labels")
    labels["snapshot_date"] = pd.to_datetime(labels["snapshot_date"])
    labels["future_sales_48d"] = pd.to_numeric(
        labels["future_sales_48d"], errors="coerce"
    ).fillna(0.0)

    out = df.merge(
        labels,
        on=["snapshot_date", "store_name", "spu"],
        how="inner",
        validate="one_to_one",
    )
    out["label_end_date"] = out["snapshot_date"] + pd.to_timedelta(
        HORIZON_DAYS, unit="D"
    )
    return out

def prepare_fold(df: pd.DataFrame, start: Any, end: Any):
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    test = df[(df["snapshot_date"] >= s) & (df["snapshot_date"] <= e)].copy()
    test_launches = set(test["launch_key"])
    train = df[
        (df["label_end_date"] < s)
        & (~df["launch_key"].isin(test_launches))
    ].copy()
    return train, test

def ratio_factor(history: pd.DataFrame, pred_col: str, qv: float) -> float:
    ratio = (
        history["future_sales_48d"].astype(float)
        / history[pred_col].astype(float).clip(lower=1e-6)
    ).replace([np.inf, -np.inf], np.nan).dropna()
    return float(np.quantile(ratio, qv)) if len(ratio) else 1.0

def build_factors(history: pd.DataFrame, qv: float) -> Dict[str, Any]:
    col = f"q{int(qv*100)}_raw"
    global_factor = ratio_factor(history, col, qv)
    ages = {}
    for age in base.CHECKPOINT_AGES:
        g = history[history["age_days"].astype(int) == int(age)]
        if len(g) >= MIN_AGE_CAL_ROWS:
            f = ratio_factor(g, col, qv)
            source = "age"
        else:
            f = global_factor
            source = "global_fallback"
        ages[int(age)] = {"rows": int(len(g)), "factor": round(f, 6), "source": source}
    return {
        "quantile": qv,
        "history_rows": int(len(history)),
        "global_factor": round(global_factor, 6),
        "age_factors": ages,
    }

def apply_factor(df: pd.DataFrame, info: Mapping[str, Any], method: str):
    qv = float(info["quantile"])
    raw = df[f"q{int(qv*100)}_raw"].to_numpy(dtype=float)
    if method == "RAW":
        return raw
    if method == "GLOBAL":
        return raw * float(info["global_factor"])
    factors = np.asarray([
        float(info["age_factors"][int(a)]["factor"])
        for a in df["age_days"].astype(int)
    ])
    return raw * factors

def main() -> int:
    df = load_rows_h48()
    features = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES

    print("DIRECT48_SCOPE=" + json.dumps({
        "model_prefix": MODEL_PREFIX,
        "horizon_days": HORIZON_DAYS,
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "min_snapshot": str(df["snapshot_date"].min().date()),
        "max_snapshot": str(df["snapshot_date"].max().date()),
        "checkpoint_ages": list(base.CHECKPOINT_AGES),
        "label_source": v1.DAILY_TABLE,
        "snapshot_table_modified": False,
        "no_db_write": True,
    }, ensure_ascii=False))

    oos_rows = []
    for fold, start, end in base.TEST_FOLDS:
        train, test = prepare_fold(df, start, end)
        if len(train) < MIN_TRAIN_ROWS or test.empty:
            print("DIRECT48_FOLD_SKIPPED=" + json.dumps({
                "fold": fold, "train_rows": len(train), "test_rows": len(test)
            }))
            continue
        print("DIRECT48_FOLD_SCOPE=" + json.dumps({
            "fold": fold,
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "train_max_label_end": str(train["label_end_date"].max().date()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "test_max_snapshot": str(test["snapshot_date"].max().date()),
            "launch_overlap": int(len(set(train["launch_key"]) & set(test["launch_key"]))),
        }, ensure_ascii=False))

        ylog = np.log1p(train["future_sales_48d"].to_numpy(float))
        y = test["future_sales_48d"].to_numpy(float)
        w = stage2.launch_balanced_weights(test)
        row = test[[
            "snapshot_date","label_end_date","store_name","spu",
            "launch_key","age_days","future_sales_48d"
        ]].copy()
        row["fold"] = fold

        preds = {}
        for qv in QUANTILES:
            model = quant.make_quantile_regressor(qv)
            model.fit(
                train[features], ylog,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            pred = np.maximum(np.expm1(model.predict(test[features])), 1e-6)
            row[f"q{int(qv*100)}_raw"] = pred
            preds[qv] = pred
            print("DIRECT48_RAW_OOS=" + json.dumps({
                "fold": fold,
                "quantile": qv,
                "launch_balanced_metrics": quant.quantile_metrics(y, pred, qv, w),
                "volume_metrics": stage2.volume_metrics(y, pred, w) if qv == 0.50 else None,
            }, ensure_ascii=False))

        crossing = preds[0.75] < preds[0.50]
        print("DIRECT48_CROSSING=" + json.dumps({
            "fold": fold,
            "rows": int(len(test)),
            "crossing_rows": int(crossing.sum()),
            "crossing_rate": round(float(crossing.mean()), 6),
        }))
        oos_rows.append(row)

    if not oos_rows:
        raise RuntimeError("no H48 OOS rows")
    oos = pd.concat(oos_rows, ignore_index=True)

    fold_order = [x[0] for x in base.TEST_FOLDS]
    fold_dates = {n: (pd.Timestamp(s), pd.Timestamp(e)) for n,s,e in base.TEST_FOLDS}
    pooled = {(qv,m): [] for qv in QUANTILES for m in METHODS}

    for test_fold in fold_order[1:]:
        test_start, _ = fold_dates[test_fold]
        idx = fold_order.index(test_fold)
        prior = fold_order[:idx]
        hist0 = oos[oos["fold"].isin(prior)].copy()
        hist = hist0[hist0["label_end_date"] < test_start].copy()
        test = oos[oos["fold"] == test_fold].copy()
        if hist.empty or test.empty:
            continue

        factors = {qv: build_factors(hist, qv) for qv in QUANTILES}
        print("DIRECT48_CAL_FOLD_SCOPE=" + json.dumps({
            "test_fold": test_fold,
            "test_start": str(test_start.date()),
            "history_candidate_rows": int(len(hist0)),
            "history_rows": int(len(hist)),
            "history_max_label_end": str(hist["label_end_date"].max().date()),
            "maturity_guard_pass": bool(hist["label_end_date"].max() < test_start),
            "test_rows": int(len(test)),
            "factors": factors,
        }, ensure_ascii=False))

        predmap = {}
        for qv in QUANTILES:
            for method in METHODS:
                predmap[(qv,method)] = apply_factor(test, factors[qv], method)
        for method in METHODS:
            predmap[(0.75,method)] = np.maximum(
                predmap[(0.75,method)], predmap[(0.50,method)]
            )

        y = test["future_sales_48d"].to_numpy(float)
        w = stage2.launch_balanced_weights(test)
        ages = test["age_days"].astype(int).to_numpy()

        for qv in QUANTILES:
            for method in METHODS:
                pred = predmap[(qv,method)]
                print("DIRECT48_CAL_RESULT=" + json.dumps({
                    "test_fold": test_fold,
                    "quantile": qv,
                    "method": method,
                    "launch_balanced_metrics": quant.quantile_metrics(y, pred, qv, w),
                    "volume_metrics": stage2.volume_metrics(y, pred, w) if qv == 0.50 else None,
                }, ensure_ascii=False))
                for age in base.CHECKPOINT_AGES:
                    mask = ages == int(age)
                    if not mask.any():
                        continue
                    g = test.loc[mask].copy()
                    gw = stage2.launch_balanced_weights(g)
                    print("DIRECT48_CAL_FOLD_AGE=" + json.dumps({
                        "test_fold": test_fold,
                        "quantile": qv,
                        "method": method,
                        "age": int(age),
                        "rows": int(mask.sum()),
                        "launch_balanced_metrics": quant.quantile_metrics(
                            y[mask], pred[mask], qv, gw
                        ),
                    }, ensure_ascii=False))

                tmp = test[["fold","launch_key","age_days","future_sales_48d"]].copy()
                tmp["pred"] = pred
                pooled[(qv,method)].append(tmp)

    print("\n=== DIRECT48 FORWARD CALIBRATION POOLED ===")
    for qv in QUANTILES:
        for method in METHODS:
            frames = pooled[(qv,method)]
            if not frames:
                continue
            d = pd.concat(frames, ignore_index=True)
            y = d["future_sales_48d"].to_numpy(float)
            pred = d["pred"].to_numpy(float)
            w = stage2.launch_balanced_weights(d)
            print("DIRECT48_CAL_POOLED=" + json.dumps({
                "quantile": qv,
                "method": method,
                "folds": sorted(d["fold"].unique().tolist()),
                "launch_balanced_metrics": quant.quantile_metrics(y, pred, qv, w),
                "volume_metrics": stage2.volume_metrics(y, pred, w) if qv == 0.50 else None,
            }, ensure_ascii=False))
            for age in base.CHECKPOINT_AGES:
                g = d[d["age_days"].astype(int) == int(age)].copy()
                if g.empty:
                    continue
                gy = g["future_sales_48d"].to_numpy(float)
                gp = g["pred"].to_numpy(float)
                gw = stage2.launch_balanced_weights(g)
                print("DIRECT48_CAL_AGE=" + json.dumps({
                    "quantile": qv,
                    "method": method,
                    "age": int(age),
                    "rows": int(len(g)),
                    "launch_balanced_metrics": quant.quantile_metrics(gy, gp, qv, gw),
                }, ensure_ascii=False))

    print("\n=== DIRECT48_GUIDANCE ===")
    print("1. H48 is the procurement lead-time horizon; do not scale H30 by 48/30.")
    print("2. Prefer Q50 coverage near 50% and Q75 near 75%, especially Day14/30.")
    print("3. Check every forward fold; pooled metrics must not hide fold failures.")
    print("4. 2026H2 sample reduction from the 48-day maturity window is expected right-censoring.")
    print("5. If H48 passes, move to live inventory/inbound procurement shadow; never fabricate historical inventory.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
