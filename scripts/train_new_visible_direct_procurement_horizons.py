#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Strict temporal OOS DIRECT quantile benchmark for NEW_VISIBLE procurement horizons.

Research-only. No DB writes, no production promotion.

Business alignment
------------------
Current production procurement logic covers:
- stock fabrics: 2 months
- custom fabrics: 3 months

For NEW_VISIBLE, test direct future-demand horizons:
- H60 ~= 2 months
- H90 ~= 3 months

H48 remains the lead-time shortage / expedite-risk horizon.

Each horizon:
1) derives a true future label from daily history;
2) keeps only fully observed labels;
3) fits DIRECT Q50/Q75 with frozen CORE features;
4) runs strict temporal OOS;
5) compares RAW / GLOBAL / AGE forward calibration using only prior mature OOS rows.
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

HORIZONS = (60, 90)
QUANTILES = (0.50, 0.75)
METHODS = ("RAW", "GLOBAL", "AGE")
MIN_TRAIN_ROWS = 200
MIN_AGE_CAL_ROWS = 40


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def load_rows_horizon(horizon: int) -> pd.DataFrame:
    df = base.load_rows().copy()
    max_row = q(f"SELECT MAX(dt) AS max_dt FROM {v1.DAILY_TABLE}")
    max_dt = pd.Timestamp(max_row[0]["max_dt"])

    ph = ",".join(["%s"] * len(base.CHECKPOINT_AGES))
    label_col = f"future_sales_{horizon}d"
    rows = q(
        f"""
        SELECT
          s.snapshot_date,s.store_name,s.spu,
          SUM(COALESCE(d.sales_units,0)) AS {label_col}
        FROM {v1.SNAPSHOT_TABLE} s
        LEFT JOIN {v1.DAILY_TABLE} d
          ON d.store_name=s.store_name
         AND d.spu=s.spu
         AND d.dt>s.snapshot_date
         AND d.dt<=DATE_ADD(s.snapshot_date,INTERVAL {horizon} DAY)
        WHERE s.dataset_version=%s
          AND s.age_days IN ({ph})
          AND s.snapshot_date<=DATE_SUB(%s,INTERVAL {horizon} DAY)
        GROUP BY s.snapshot_date,s.store_name,s.spu
        """,
        (v2.DATASET_VERSION, *base.CHECKPOINT_AGES, max_dt.date()),
    )
    labels = pd.DataFrame(rows)
    if labels.empty:
        raise RuntimeError(f"no fully observed H{horizon} labels")

    labels["snapshot_date"] = pd.to_datetime(labels["snapshot_date"])
    labels[label_col] = pd.to_numeric(labels[label_col], errors="coerce").fillna(0.0)

    out = df.merge(
        labels,
        on=["snapshot_date","store_name","spu"],
        how="inner",
        validate="one_to_one",
    )
    out["label_end_date"] = (
        out["snapshot_date"] + pd.to_timedelta(horizon, unit="D")
    )
    return out


def prepare_fold(df: pd.DataFrame, start: Any, end: Any):
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    test = df[(df["snapshot_date"] >= s) & (df["snapshot_date"] <= e)].copy()
    test_launches = set(test["launch_key"].unique())
    train = df[
        (df["label_end_date"] < s)
        & (~df["launch_key"].isin(test_launches))
    ].copy()
    return train, test


def ratio_factor(history: pd.DataFrame, pred_col: str, y_col: str, qv: float) -> float:
    ratio = (
        history[y_col].astype(float)
        / history[pred_col].astype(float).clip(lower=1e-6)
    ).replace([np.inf, -np.inf], np.nan).dropna()
    return float(np.quantile(ratio, qv)) if len(ratio) else 1.0


def build_factors(history: pd.DataFrame, horizon: int, qv: float):
    y_col = f"future_sales_{horizon}d"
    pred_col = f"q{int(qv*100)}_raw"
    global_factor = ratio_factor(history, pred_col, y_col, qv)
    ages = {}
    for age in base.CHECKPOINT_AGES:
        g = history[history["age_days"].astype(int) == int(age)]
        if len(g) >= MIN_AGE_CAL_ROWS:
            factor = ratio_factor(g, pred_col, y_col, qv)
            source = "age"
        else:
            factor = global_factor
            source = "global_fallback"
        ages[int(age)] = {
            "rows": int(len(g)),
            "factor": round(float(factor), 6),
            "source": source,
        }
    return {
        "quantile": qv,
        "global_factor": round(float(global_factor), 6),
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
        float(info["age_factors"][int(age)]["factor"])
        for age in df["age_days"].astype(int)
    ])
    return raw * factors


def run_horizon(horizon: int) -> None:
    df = load_rows_horizon(horizon)
    features = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES
    y_col = f"future_sales_{horizon}d"

    print("PROC_HORIZON_SCOPE=" + json.dumps({
        "horizon_days": horizon,
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "min_snapshot": str(df["snapshot_date"].min().date()),
        "max_snapshot": str(df["snapshot_date"].max().date()),
        "target": y_col,
        "model": f"NV-ML-V1-DIRECT{horizon}-CORE",
        "no_db_write": True,
    }, ensure_ascii=False))

    oos_rows = []
    for fold, start, end in base.TEST_FOLDS:
        train, test = prepare_fold(df, start, end)
        if len(train) < MIN_TRAIN_ROWS or test.empty:
            print("PROC_HORIZON_FOLD_SKIPPED=" + json.dumps({
                "horizon_days": horizon,
                "fold": fold,
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
            }, ensure_ascii=False))
            continue

        print("PROC_HORIZON_FOLD_SCOPE=" + json.dumps({
            "horizon_days": horizon,
            "fold": fold,
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "train_max_label_end": str(train["label_end_date"].max().date()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "test_max_snapshot": str(test["snapshot_date"].max().date()),
            "launch_overlap": int(len(set(train["launch_key"]) & set(test["launch_key"]))),
        }, ensure_ascii=False))

        ylog = np.log1p(train[y_col].to_numpy(dtype=float))
        row = test[[
            "snapshot_date","label_end_date","store_name","spu",
            "launch_key","age_days",y_col
        ]].copy()
        row["fold"] = fold

        y = test[y_col].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(test)

        for qv in QUANTILES:
            model = quant.make_quantile_regressor(qv)
            model.fit(
                train[features],
                ylog,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )
            pred = np.maximum(np.expm1(model.predict(test[features])), 1e-6)
            row[f"q{int(qv*100)}_raw"] = pred
            print("PROC_HORIZON_RAW_OOS=" + json.dumps({
                "horizon_days": horizon,
                "fold": fold,
                "quantile": qv,
                "launch_balanced_metrics": quant.quantile_metrics(y, pred, qv, w),
                "volume_metrics": stage2.volume_metrics(y, pred, w) if qv == 0.50 else None,
            }, ensure_ascii=False))
        oos_rows.append(row)

    if not oos_rows:
        raise RuntimeError(f"no OOS rows for H{horizon}")
    oos = pd.concat(oos_rows, ignore_index=True)

    fold_order = [x[0] for x in base.TEST_FOLDS]
    fold_dates = {n: (pd.Timestamp(s), pd.Timestamp(e)) for n,s,e in base.TEST_FOLDS}
    pooled = {(qv,m): [] for qv in QUANTILES for m in METHODS}

    for test_fold in fold_order[1:]:
        test_start, _ = fold_dates[test_fold]
        prior = fold_order[:fold_order.index(test_fold)]
        hist0 = oos[oos["fold"].isin(prior)].copy()
        hist = hist0[hist0["label_end_date"] < test_start].copy()
        test = oos[oos["fold"] == test_fold].copy()
        if hist.empty or test.empty:
            continue

        factors = {qv: build_factors(hist, horizon, qv) for qv in QUANTILES}
        print("PROC_HORIZON_CAL_FOLD_SCOPE=" + json.dumps({
            "horizon_days": horizon,
            "test_fold": test_fold,
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
                predmap[(0.75,method)],
                predmap[(0.50,method)],
            )

        y = test[y_col].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(test)
        for qv in QUANTILES:
            for method in METHODS:
                pred = predmap[(qv,method)]
                print("PROC_HORIZON_CAL_RESULT=" + json.dumps({
                    "horizon_days": horizon,
                    "test_fold": test_fold,
                    "quantile": qv,
                    "method": method,
                    "launch_balanced_metrics": quant.quantile_metrics(y, pred, qv, w),
                    "volume_metrics": stage2.volume_metrics(y, pred, w) if qv == 0.50 else None,
                }, ensure_ascii=False))

                tmp = test[["fold","launch_key","age_days",y_col]].copy()
                tmp["pred"] = pred
                pooled[(qv,method)].append(tmp)

    for qv in QUANTILES:
        for method in METHODS:
            frames = pooled[(qv,method)]
            if not frames:
                continue
            d = pd.concat(frames, ignore_index=True)
            y = d[y_col].to_numpy(dtype=float)
            pred = d["pred"].to_numpy(dtype=float)
            w = stage2.launch_balanced_weights(d)
            print("PROC_HORIZON_CAL_POOLED=" + json.dumps({
                "horizon_days": horizon,
                "quantile": qv,
                "method": method,
                "folds": sorted(d["fold"].unique().tolist()),
                "launch_balanced_metrics": quant.quantile_metrics(y, pred, qv, w),
                "volume_metrics": stage2.volume_metrics(y, pred, w) if qv == 0.50 else None,
            }, ensure_ascii=False))

            for age in base.CHECKPOINT_AGES:
                g = d[d["age_days"].astype(int) == int(age)]
                if g.empty:
                    continue
                gy = g[y_col].to_numpy(dtype=float)
                gp = g["pred"].to_numpy(dtype=float)
                gw = stage2.launch_balanced_weights(g)
                print("PROC_HORIZON_CAL_AGE=" + json.dumps({
                    "horizon_days": horizon,
                    "quantile": qv,
                    "method": method,
                    "age": int(age),
                    "rows": int(len(g)),
                    "launch_balanced_metrics": quant.quantile_metrics(gy, gp, qv, gw),
                }, ensure_ascii=False))


def main() -> int:
    for horizon in HORIZONS:
        run_horizon(horizon)
    print("PROC_HORIZON_GUIDANCE=" + json.dumps({
        "H60": "stock-fabric standard replenishment research horizon",
        "H90": "custom-fabric standard replenishment research horizon",
        "H48": "lead-time shortage / expedite risk only",
        "selection_gate": "Q50~50%, Q75~75%, prioritize Day14/30 and forward-fold stability",
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
