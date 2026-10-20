"""Check supplementary report tables against an independently verified base.

No imports from the report generator, no forecasting, and no network access.
This extends, not replaces, the raw-score validation in cross_validate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def equal(actual, expected):
    if pd.isna(expected):
        assert pd.isna(actual), (actual, expected)
    elif isinstance(expected, (int, float, np.number)):
        assert np.isclose(actual, expected, rtol=1e-10, atol=1e-7), (actual, expected)
    else:
        assert actual == expected, (actual, expected)


def run(data, report, receipt):
    data, report, receipt = map(lambda p: Path(p).resolve(), (data, report, receipt))
    if receipt.exists():
        raise ValueError("new receipt path required")
    validation = json.loads((data / "validation.json").read_text(encoding="utf-8"))
    assert validation["status"] == "PASS_INDEPENDENT_CALCULATION_PATH"
    manifest = json.loads((data / "run_manifest.json").read_text(encoding="utf-8"))
    rm = json.loads((report / "report_manifest.json").read_text(encoding="utf-8"))
    assert rm["analysis_manifest_sha256"] == digest(data / "run_manifest.json")
    assert rm["validation_sha256"] == digest(data / "validation.json")
    for name, sha in manifest["files"].items():
        assert digest(data / name) == sha
    for name, sha in rm["files"].items():
        assert digest(report / name) == sha
    for name, sha in manifest["sources"].items():
        assert digest(name) == sha
    all_objects = pd.read_parquet(data / "base_objects.parquet")
    objects = all_objects[all_objects.calendar_mature].copy()
    pair = objects[objects.paired_object]
    checks = {}

    classes = pd.read_csv(report / "same_pair_error_classes.csv")
    for model, exported in classes.groupby("model"):
        assert exported.n.sum() == len(pair)
        equal(exported.actual_units.sum(), pair.a60.sum())
        for r in exported.itertuples():
            g = pair[pair[model + "_class"].eq(r.error_class)]
            equal(r.n, len(g))
            equal(r.actual_units, g.a60.sum())
            equal(r.ae60, g[model + "_ae60"].sum(min_count=1))
            equal(r.ae_segments, g[model + "_ae_segments"].sum(min_count=1))
    checks["same_pair_class_rows"] = len(classes)

    products = pd.read_csv(report / "paired_product_change.csv")
    assert not products.duplicated(["shop", "spu"]).any()
    assert len(products) == len(pair[["shop", "spu"]].drop_duplicates())
    assert products.records.sum() == len(pair)
    for r in products.itertuples():
        g = pair[pair.shop.eq(r.shop) & pair.spu.eq(r.spu)]
        change = sum(g.f28_ae60.to_numpy() - g.direct_ae60.to_numpy())
        equal(r.ae_improvement, change)
        equal(r.actual_units, g.a60.sum())
        equal(r.records, len(g))
        equal(r.change, "IMPROVED" if change > 0 else "DEGRADED" if change < 0 else "UNCHANGED")
    checks["distinct_product_changes"] = len(products)

    combinations = pd.read_csv(report / "combination_error_classes.csv")
    assert combinations.n.sum() == len(objects)
    equal(combinations.actual_units.sum(), objects.a60.sum())
    for r in combinations.itertuples():
        g = objects[objects.shop.eq(r.shop) &
                    objects.current_category.eq(r.current_category) &
                    objects.issue_month.eq(r.issue_month) &
                    objects.direct_class.eq(r.direct_class)]
        equal(r.n, len(g))
        equal(r.actual_units, g.a60.sum())
        equal(r.ae60, g.direct_ae60.sum(min_count=1))
        equal(r.ae_segments, g.direct_ae_segments.sum(min_count=1))
    checks["disjoint_error_class_cells"] = len(combinations)

    migrations = pd.read_csv(report / "paired_joint_migrations.csv")
    for r in migrations.itertuples():
        g = pair[pair.issue_month.eq(r.issue_month)]
        equal(r.n, len(g))
        equal(r.retained_hit, sum(bool(a) and bool(b) for a, b in zip(g.f28_joint_hit, g.direct_joint_hit)))
        equal(r.gained_hit, sum(not bool(a) and bool(b) for a, b in zip(g.f28_joint_hit, g.direct_joint_hit)))
        equal(r.lost_hit, sum(bool(a) and not bool(b) for a, b in zip(g.f28_joint_hit, g.direct_joint_hit)))
    assert migrations.n.sum() == len(pair)
    checks["joint_migration_periods"] = len(migrations)

    examples = pd.read_csv(report / "top_combination_examples.csv")
    assert not examples.duplicated(["shop", "spu", "issue_date"]).any()
    for r in examples.itertuples():
        g = objects[objects.shop.eq(r.shop) & objects.spu.eq(r.spu) &
                    objects.issue_date.eq(r.issue_date)]
        assert len(g) == 1
        for col in examples.columns.difference(["combination_label"]):
            equal(getattr(r, col), g.iloc[0][col])
    checks["traceable_examples"] = len(examples)

    supported = pd.read_csv(data / "ranking_1_supported_error_rates.csv")
    for r in pd.read_csv(report / "ranking_support_coverage.csv").itertuples():
        g = supported[supported.model.eq(r.model)]
        equal(r.supported_groups, len(g))
        equal(r.core_records, g.n.sum())
        equal(r.output_records, g.output_n.sum())
        equal(r.actual_units, g.actual_units.sum())
        equal(r.absolute_error, g.absolute_error.sum())
        equal(r.full_core_records, len(objects))
        equal(r.full_actual_units, objects.a60.sum())
    repeat = pd.read_csv(data / "calendar_repeatability.csv")
    for r in pd.read_csv(report / "repeatability_summary.csv").itertuples():
        g = repeat[repeat.shop.eq(r.shop) & repeat.current_category.eq(r.current_category)]
        equal(r.compared_months, len(g))
        for col in ["same_direction", "both_improve", "both_degrade"]:
            equal(getattr(r, col), g[col].sum())
    survivors = pd.read_parquet(data / "survivor_details.parquet")
    survivor_metrics = pd.read_csv(report / "survivor_paired_metrics.csv")
    for r in survivor_metrics.itertuples():
        g = survivors[survivors.lag_months.eq(r.lag_months) &
                      survivors.previous_issue_month.eq(r.previous_issue_month) &
                      survivors.issue_month.eq(r.issue_month)]
        equal(r.n, len(g))
        equal(r.products, len(g[["shop", "spu"]].drop_duplicates()))
        for period, truthcol in [("previous", "previous_actual"), ("current", "actual")]:
            denom = sum(g[truthcol])
            equal(getattr(r, period + "_units"), denom)
            for model in ["f28", "direct"]:
                predcol = ("previous_" if period == "previous" else "") + model + "_prediction"
                ae = sum(abs(p - a) for p, a in zip(g[predcol], g[truthcol]))
                equal(getattr(r, model + "_" + period + "_wape"), ae / denom if denom > 0 else np.nan)
    checks["survivor_comparisons"] = len(survivor_metrics)
    checks["report_tables_verified"] = len(list(report.glob("*.csv")))
    result = {
        "status": "PASS_SUPPLEMENTARY_REPORT_TABLES",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "report_manifest_sha256": digest(report / "report_manifest.json"),
        "analysis_validation_sha256": digest(data / "validation.json"),
        "source_sha256": digest(__file__),
        "fits": 0, "database_queries": 0, "production_changes": 0,
        "reviewer_scope": "Separate implementation by same executing agent; not a second-person review",
    }
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--receipt", required=True)
    run(**vars(parser.parse_args()))
