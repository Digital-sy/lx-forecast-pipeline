#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Strict temporal OOS quantile benchmark for full NEW_VISIBLE future30 demand.

Research-only. No DB writes, no model persistence, no production promotion.

Chosen full-volume architecture
-------------------------------
Point forecast benchmark selected DIRECT over Stage1-probability MIXTURE because DIRECT
had lower WAPE in every temporal fold and lower pooled WAPE, while MIXTURE's main
advantage was less-negative bias.

This script therefore estimates the full NEW_VISIBLE demand distribution directly:

    Q50 = median future_sales_30d across all NEW_VISIBLE rows
    Q75 = upper planning quantile across all NEW_VISIBLE rows

Stage-1 remains available as an opportunity/ranking layer but is not required inside
the quantity forecast formula.

Leakage controls
----------------
- fixed checkpoint ages 7/14/30/60/90
- train label_end < test_start
- remove every test store x SPU launch from training
- CORE point-in-time features only
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd

from scripts import train_new_visible_v1_stage1 as base
from scripts import train_new_visible_v1_stage2_volume as stage2
from scripts import train_new_visible_v1_stage2_quantiles as quant


QUANTILES: Sequence[float] = (0.50, 0.75)
MODEL_PREFIX = "NV-ML-V1-STAGE2-DIRECT30-CORE"
MIN_TRAIN_ROWS = 200


def empirical_age_quantile(
    train: pd.DataFrame,
    test: pd.DataFrame,
    q: float,
) -> np.ndarray:
    global_q = float(train["future_sales_30d"].quantile(q))
    by_age = (
        train.groupby("age_days")["future_sales_30d"]
        .quantile(q)
        .to_dict()
    )
    return np.asarray(
        [float(by_age.get(int(a), global_q)) for a in test["age_days"]],
        dtype=float,
    )


def main() -> int:
    df = base.load_rows()
    feature_cols = list(quant.abl.CORE_NUMERIC) + base.CATEGORICAL_FEATURES

    print("DIRECT_QUANTILE_SCOPE=" + json.dumps({
        "model_prefix": MODEL_PREFIX,
        "target": "future_sales_30d (all NEW_VISIBLE)",
        "quantiles": list(QUANTILES),
        "rows": int(len(df)),
        "launches": int(df["launch_key"].nunique()),
        "checkpoint_ages": list(base.CHECKPOINT_AGES),
        "feature_set": "CORE",
        "target_transform": "log1p(future_sales_30d)",
        "leakage_rule": "train label_end < test_start and remove all test launches from train",
        "no_db_write": True,
    }, ensure_ascii=False))

    pooled: Dict[float, Dict[str, List[Any]]] = {
        q: {
            "y": [], "baseline": [], "model": [],
            "launch_key": [], "age": [], "fold": []
        }
        for q in QUANTILES
    }
    crossing_rows: List[Dict[str, Any]] = []

    for fold_name, start, end in base.TEST_FOLDS:
        train, test = stage2.prepare_fold(df, fold_name, start, end)
        if len(train) < MIN_TRAIN_ROWS or test.empty:
            print("DIRECT_QUANTILE_FOLD_SKIPPED=" + json.dumps({
                "fold": fold_name,
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
            }, ensure_ascii=False))
            continue

        print("DIRECT_QUANTILE_FOLD_SCOPE=" + json.dumps({
            "fold": fold_name,
            "test_start": str(pd.Timestamp(start).date()),
            "test_end": str(pd.Timestamp(end).date()),
            "train_rows": int(len(train)),
            "train_launches": int(train["launch_key"].nunique()),
            "train_max_label_end": str(train["label_end_date"].max().date()),
            "test_rows": int(len(test)),
            "test_launches": int(test["launch_key"].nunique()),
            "launch_overlap": int(
                len(set(train["launch_key"]) & set(test["launch_key"]))
            ),
        }, ensure_ascii=False))

        y = test["future_sales_30d"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(test)
        preds_by_q: Dict[float, np.ndarray] = {}

        for q in QUANTILES:
            model = quant.make_quantile_regressor(q)
            y_train_log = np.log1p(
                train["future_sales_30d"].to_numpy(dtype=float)
            )
            model.fit(
                train[feature_cols],
                y_train_log,
                reg__sample_weight=stage2.launch_balanced_weights(train),
            )

            pred = np.expm1(model.predict(test[feature_cols]))
            pred = np.maximum(pred, 0.0)
            baseline = empirical_age_quantile(train, test, q)
            preds_by_q[q] = pred

            print("DIRECT_QUANTILE_OOS=" + json.dumps({
                "fold": fold_name,
                "quantile": q,
                "baseline": {
                    "name": f"train_all_age_q{int(q*100)}",
                    "row_metrics": quant.quantile_metrics(y, baseline, q),
                    "launch_balanced_metrics": quant.quantile_metrics(
                        y, baseline, q, w
                    ),
                },
                "lightgbm": {
                    "model": f"{MODEL_PREFIX}-Q{int(q*100)}",
                    "row_metrics": quant.quantile_metrics(y, pred, q),
                    "launch_balanced_metrics": quant.quantile_metrics(
                        y, pred, q, w
                    ),
                    "volume_metrics": (
                        stage2.volume_metrics(y, pred, w)
                        if q == 0.50 else None
                    ),
                },
            }, ensure_ascii=False))

            pooled[q]["y"].extend(y.tolist())
            pooled[q]["baseline"].extend(baseline.tolist())
            pooled[q]["model"].extend(pred.tolist())
            pooled[q]["launch_key"].extend(test["launch_key"].astype(str).tolist())
            pooled[q]["age"].extend(test["age_days"].astype(int).tolist())
            pooled[q]["fold"].extend([fold_name] * len(test))

        if 0.50 in preds_by_q and 0.75 in preds_by_q:
            crossing = preds_by_q[0.75] < preds_by_q[0.50]
            diff = preds_by_q[0.75] - preds_by_q[0.50]
            crossing_rows.append({
                "fold": fold_name,
                "rows": int(len(test)),
                "crossing_rows": int(crossing.sum()),
                "crossing_rate": round(float(crossing.mean()), 6),
                "median_band_width": round(float(np.median(diff)), 3),
                "p75_band_width": round(float(np.quantile(diff, 0.75)), 3),
            })
            print("DIRECT_QUANTILE_CROSSING=" + json.dumps(
                crossing_rows[-1], ensure_ascii=False
            ))

    print("\n=== DIRECT FULL-VOLUME QUANTILE POOLED ===")
    for q in QUANTILES:
        d = pd.DataFrame(pooled[q])
        if d.empty:
            continue

        y = d["y"].to_numpy(dtype=float)
        b = d["baseline"].to_numpy(dtype=float)
        m = d["model"].to_numpy(dtype=float)
        w = stage2.launch_balanced_weights(d)

        pooled_result = {
            "quantile": q,
            "folds": sorted(d["fold"].unique().tolist()),
            "baseline": {
                "row_metrics": quant.quantile_metrics(y, b, q),
                "launch_balanced_metrics": quant.quantile_metrics(y, b, q, w),
            },
            "lightgbm": {
                "row_metrics": quant.quantile_metrics(y, m, q),
                "launch_balanced_metrics": quant.quantile_metrics(y, m, q, w),
            },
        }
        if q == 0.50:
            pooled_result["lightgbm"]["volume_metrics"] = stage2.volume_metrics(
                y, m, w
            )
        print("DIRECT_QUANTILE_POOLED=" + json.dumps(
            pooled_result, ensure_ascii=False
        ))

        for age in base.CHECKPOINT_AGES:
            g = d[d["age"].astype(int) == int(age)].copy()
            if g.empty:
                continue
            gy = g["y"].to_numpy(dtype=float)
            gb = g["baseline"].to_numpy(dtype=float)
            gm = g["model"].to_numpy(dtype=float)
            gw = stage2.launch_balanced_weights(g)
            out = {
                "quantile": q,
                "age": int(age),
                "rows": int(len(g)),
                "launches": int(g["launch_key"].nunique()),
                "baseline": quant.quantile_metrics(gy, gb, q, gw),
                "lightgbm": quant.quantile_metrics(gy, gm, q, gw),
            }
            if q == 0.50:
                out["lightgbm_volume_metrics"] = stage2.volume_metrics(
                    gy, gm, gw
                )
            print("DIRECT_QUANTILE_AGE=" + json.dumps(
                out, ensure_ascii=False
            ))

    print("\n=== DIRECT QUANTILE CROSSING SUMMARY ===")
    print("DIRECT_QUANTILE_CROSSING_POOLED=" + json.dumps({
        "folds": crossing_rows,
        "postprocess": "Q75=max(Q75,Q50) if crossing is non-trivial",
    }, ensure_ascii=False))

    print("\n=== DIRECT_QUANTILE_GUIDANCE ===")
    print("1. Q50重点看coverage≈50%、WAPE/Bias和Day14/30；它将成为完整future30常规需求线候选。")
    print("2. Q75重点看coverage≈75%，尤其Day14/30；通过后才可作为完整需求安全采购线候选。")
    print("3. 比较age-quantile baseline；若LightGBM pinball loss未稳定改善，不强行复杂化。")
    print("4. 若Q75 crossing明显，统一后处理Q75=max(Q75,Q50)。")
    print("5. 本轮通过后，下一步才接库存/在途/供应链提前期做采购回放，而不是继续增加模型层。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
