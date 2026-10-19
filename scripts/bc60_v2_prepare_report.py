"""Package already-computed V2 evidence, never fit models or query sources."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd


CHECKPOINTS = [0, 7, 14, 21, 30, 45]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def records(frame):
    return json.loads(frame.to_json(orient="records", date_format="iso", double_precision=15))


def run(archive, root, diagnostic, validation):
    archive, root, diagnostic, validation = map(Path, (archive, root, diagnostic, validation))
    b = root / "b_rolling"
    out = root / "report_evidence"
    if out.exists():
        raise ValueError("new report evidence directory required")
    bm = json.loads((b / "run_manifest.json").read_text(encoding="utf-8"))
    am = json.loads((diagnostic / "diagnostic_manifest.json").read_text(encoding="utf-8"))
    vm = json.loads((validation / "validation.json").read_text(encoding="utf-8"))
    avm = json.loads((root / "validation_b1" / "validation.json").read_text(encoding="utf-8"))
    if bm["status"] != "COMPLETE" or am["status"] != "COMPLETE" or vm["status"] != "PASS_INDEPENDENT_RECONCILIATION":
        raise ValueError("numeric/independent acceptance incomplete")
    if not str(avm["status"]).startswith("PASS"):
        raise ValueError("independent B1 diagnosis acceptance incomplete")
    out.mkdir()
    t0, t1, t2 = [pd.read_csv(b / f"{t}_summary.csv") for t in ("t0", "t1", "t2")]
    pair = pd.read_csv(b / "paired_summary.csv")
    detail = pd.read_parquet(b / "paired_details.parquet")
    stage = pd.read_csv(diagnostic / "stage_summary.csv")
    scores = pd.read_csv(diagnostic / "stage_scores.csv")
    revision = pd.read_csv(b / "revision_summary.csv")
    joint = pd.read_csv(b / "joint_hits.csv")
    coverage = pd.read_csv(archive / "effects_v1" / "coverage.csv")
    cov = coverage[coverage.window.eq("d01_60")].groupby("issue_date").agg(
        core_records=("fixed_core_n", "sum"), selected_sales=("selection_core_units", "sum"),
        selection_sales=("selection_total_units", "sum"), core_future=("target_observed_core_units", "sum"),
        shop_future=("target_observed_shop_units", "sum")).reset_index()
    cov["selection_coverage"] = cov.selected_sales / cov.selection_sales
    cov["target_observed_coverage"] = cov.core_future / cov.shop_future

    frames = {
        "t0_full": t0[t0.scope.eq("full_core") & t0.slice.eq("overall")],
        "t0_common369": t0[t0.scope.eq("original_b1_369") & t0.slice.eq("overall")],
        "t1_checkpoint": t1[t1.scope.eq("full_core") & t1.slice.eq("overall") & t1.update_offset.isin(CHECKPOINTS)],
        "t2_balanced_checkpoint": t2[t2.scope.eq("t2_balanced_279") & t2.slice.eq("overall") & t2.update_offset.isin(CHECKPOINTS)],
        "t2_maturity": t2[t2.scope.eq("full_core") & t2.slice.eq("overall") & t2.update_offset.isin(CHECKPOINTS)],
        "paired_checkpoints": pair[pair.slice.eq("overall") & pair.update_offset.isin(CHECKPOINTS)],
        "shop_changes": pair[pair.slice.eq("shop") & pair.update_offset.isin(CHECKPOINTS)],
        "issue_changes": pair[pair.slice.eq("issue") & pair.update_offset.isin(CHECKPOINTS)],
        "coverage": cov,
        "b1_stage": stage,
    }
    a_daily = pd.read_parquet(diagnostic / "daily_d0_d1.parquet")
    first = a_daily[a_daily.relative_day.eq(-1) & a_daily.model.eq("D0") & a_daily.forecast.notna()]
    frames["b1_first_step"] = pd.DataFrame([{
        "scored_n": len(first), "forecast_units": first.forecast.sum(), "actual_units": first.actual.sum(),
        "bias": (first.forecast.sum() - first.actual.sum()) / first.actual.sum(),
        "scope": "First bridge day before any forecast feedback; not scored in original target",
    }])
    diagnostic_pairs = []
    for window in ("d01_30", "d31_60", "d01_60"):
        wide = scores[scores.stage.eq(window)].pivot(index=["issue_date", "shop", "spu"], columns="model", values=["actual", "prediction"]).dropna()
        before = (wide.prediction.D0 - wide.actual.D0).abs()
        after = (wide.prediction.D1 - wide.actual.D1).abs()
        delta = after - before
        diagnostic_pairs.append({"window": window, "scored_n": len(wide), "actual_units": wide.actual.D0.sum(),
            "improved_n": int(delta.lt(-1e-8).sum()), "degraded_n": int(delta.gt(1e-8).sum()),
            "unchanged_n": int(delta.abs().le(1e-8).sum()),
            "improved_sales_weight": wide.actual.D0[delta.lt(-1e-8)].sum()/wide.actual.D0.sum(),
            "degraded_sales_weight": wide.actual.D0[delta.gt(1e-8)].sum()/wide.actual.D0.sum()})
    frames["b1_d1_pairs"] = pd.DataFrame(diagnostic_pairs)
    worst = detail[detail.track.eq("T1") & detail.comparison.eq("F28->R28-D") & detail.update_offset.isin([30, 45])].copy()
    frames["t1_degraded_objects"] = worst[worst.delta_absolute_error.gt(1e-8)].sort_values(["update_offset", "delta_absolute_error"], ascending=[True, False])
    frames["b1_error_objects"] = scores[scores.model.eq("D0") & scores.stage.eq("d31_60")].sort_values("absolute_error", ascending=False)
    rev = revision.groupby("model").agg(
        core_records=("spu", "size"), mean_nonzero_revisions=("nonzero_revisions", "mean"),
        mean_direction_reversals=("direction_reversals", "mean"),
        mean_standardized60_absolute_revision=("standardized60_absolute_revision", "mean"),
        mean_largest_standardized60_revision=("max_standardized60_revision", "mean")).reset_index()
    frames["revision"] = rev
    j = joint[joint.update_offset.isin(CHECKPOINTS)].copy()
    j["scope"] = "full_core"
    j_balanced = j[j.track.eq("T2") & j.original_issue_date.ne("2026-08-02")].copy()
    j_balanced["scope"] = "t2_balanced_279"
    frames["joint_hits"] = pd.concat([j, j_balanced]).groupby(["track", "scope", "update_offset", "model"]).agg(
        fixed_core_n=("spu", "size"), joint_hit_n=("all_three_hit", "sum"),
        known_n=("joint_state", lambda z: z.eq("KNOWN").sum())).reset_index()
    frames["joint_hits"]["joint_full_core_hit_share"] = frames["joint_hits"].joint_hit_n / frames["joint_hits"].fixed_core_n

    # Describe all fixed offsets; these are correlated vintages, NOT independent samples.
    shifts = pair[pair.scope.eq("t2_balanced_279") & pair.slice.eq("overall") & pair.window.eq("d01_60")]
    overview = {
        "classification": "RECONSTRUCTED_HISTORICAL_REPLAY",
        "fixed_core_records": bm["fixed_core_rows"], "distinct_shop_spu": bm["distinct_shop_spu"],
        "t2_daily_vs_weekly_correlated_offsets": {
            "better": int(shifts.delta_wape.lt(-1e-12).sum()),
            "equal": int(shifts.delta_wape.abs().le(1e-12).sum()),
            "worse": int(shifts.delta_wape.gt(1e-12).sum()),
            "minimum_delta_wape": float(shifts.delta_wape.min()),
            "maximum_delta_wape": float(shifts.delta_wape.max()),
        },
        "run": bm, "diagnostic": am, "independent_validation": vm, "independent_diagnostic_validation": avm,
    }
    for name, frame in frames.items():
        frame.to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")
    (out / "overview.json").write_text(json.dumps(overview, ensure_ascii=False, indent=2), encoding="utf-8")
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    queries = {}
    for name, frame in frames.items():
        # App uses bounded summaries; complete object evidence remains local CSV.
        if name.endswith("objects"):
            frame = frame.head(12)
        queries[name] = {"rows": records(frame), "source": {
            "type": "local-archived-experiment", "label": "B+C V2 本地历史滚动实验",
            "files": [f"report_evidence/{name}.csv"], "executedAt": now,
            "evidenceFlow": [
                {"title": "冻结输入", "detail": "V1 inputs_v1_final 与原核心名单，哈希核验；本轮没有新增取数。"},
                {"title": "离线计算", "detail": "T0 原签发不变；T1 同固定终点剩余需求；T2 各更新日全长度窗口。原数据缺失和未成熟状态保留。"},
                {"title": "独立核验", "detail": vm["status"]},
            ],
            "metricDefinitions": [
                {"label": "WAPE", "definition": "可评分同目标窗口的绝对误差合计 / 实际销量合计；不是逐款±20%达标率。"},
                {"label": "±20%命中", "definition": "明确正实销且有预测，abs(预测−实际)/实际≤20%；无预测不剔出固定名单。"},
                {"label": "重复窗口", "definition": "不同更新日高度重叠，不是独立样本，也不能把误差合计解释成采购损失。"},
            ],
        }}
    snapshot = {"surface": "report", "title": "B1远期高估诊断与28日均销滚动纠偏实际效果报告",
                "generatedAt": now, "buildStatus": "creating", "status": "reviewed",
                "report": {"asOf": "2026-10-01", "audience": "预测实验评审"}, "filters": [], "queries": queries}
    (out / "snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    files = {p.name: digest(p) for p in out.iterdir() if p.is_file()}
    (out / "manifest.json").write_text(json.dumps({"created_at": now, "source_sha256": digest(__file__), "files": files}, indent=2), encoding="utf-8")
    print(json.dumps({"status": "PREPARED", "directory": str(out), "tables": len(frames)}))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("archive", "root", "diagnostic", "validation"):
        p.add_argument(f"--{name}", required=True)
    run(**vars(p.parse_args()))
