#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Manually triggered, ONE-SHOT, read-only A16 shadow for JQ-US H2/H3.

No production forecast/procurement/BI writes. All business data remains on
the operator's server in a newly created, permission-restricted research folder.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import traceback
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.a16_candidate_v3 import Candidate, VERSION, SOURCE_FILES, load_pure, shift

SHOP = "JQ-US"
HORIZONS = (2, 3)
MANIFEST = ROOT / "docs/a16_candidate_v3_manifest.json"
MAX_ROWS = {"monthly": 400000, "catalog": 100000, "season": 10000}
MAX_PLAN_ROWS = {"monthly": 1000000, "catalog": 200000, "season": 20000}


def normalized_hash(path):
    return hashlib.sha256(path.read_text(encoding="utf-8-sig").encode("utf-8")).hexdigest()


def verify_freeze():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if manifest["candidate"] != VERSION:
        raise RuntimeError("冻结模型版本不匹配")
    checks = {str(ROOT / "scripts/a16_candidate_v3.py"): manifest["adapter_sha256"]}
    checks.update({str(ROOT / p): v["normalized_sha256"]
                   for p, v in manifest["sources"].items()})
    for path, want in checks.items():
        if normalized_hash(Path(path)) != want:
            raise RuntimeError("SOURCE_FINGERPRINT_MISMATCH: " + str(Path(path).relative_to(ROOT)))
    return manifest


def estimate_plan_rows(tree):
    """Collect MySQL JSON plan leaf scan estimates, not actual scanned rows."""
    found = []
    def walk(o):
        if isinstance(o, dict):
            if "rows_examined_per_scan" in o:
                found.append(int(o["rows_examined_per_scan"]))
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(tree)
    return sum(found) if found else None


def guarded_select(conn, name, sql, params):
    import pymysql
    with conn.cursor() as c:
        c.execute("EXPLAIN FORMAT=JSON " + sql, params)
        explain = c.fetchone()[0]
    plan = json.loads(explain) if isinstance(explain, str) else explain
    estimated = estimate_plan_rows(plan)
    if estimated is None or estimated > MAX_PLAN_ROWS[name]:
        raise RuntimeError(f"PLAN_BLOCKED:{name}:estimated={estimated}, limit={MAX_PLAN_ROWS[name]}")
    print(f"[EXPLAIN] {name} planned scan rows={estimated}", flush=True)
    result = []
    with conn.cursor(pymysql.cursors.SSDictCursor) as c:
        c.execute(sql, params)
        for r in c:
            result.append(r)
            if len(result) > MAX_ROWS[name]:
                raise RuntimeError(f"ROW_LIMIT:{name}:{len(result)}")
    print(f"[SOURCE] {name} rows={len(result)}", flush=True)
    return result, estimated


def read_sources(issue):
    """One MySQL read-only consistent snapshot; no joins against huge daily table."""
    from common.database import get_db_connection
    conn = get_db_connection()
    cutoff = shift(issue, 0)
    try:
        with conn.cursor() as c:
            c.execute("SET SESSION TRANSACTION READ ONLY")
            c.execute("SET SESSION MAX_EXECUTION_TIME=60000")
            c.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        month_sql = (
            "SELECT /*+ MAX_EXECUTION_TIME(60000) */ "
            "`店铺`,`SKU`,`SPU`,`统计日期`,`销量` "
            "FROM `销量统计_msku月度` "
            "WHERE `店铺`=%s AND `统计日期` >= %s AND `统计日期` < %s "
            "AND `SKU` IS NOT NULL AND `SPU` IS NOT NULL"
        )
        sales, ep1 = guarded_select(conn, "monthly", month_sql,
                                    (SHOP, "2024-01-01", cutoff.isoformat()))
        catalog_sql = (
            "SELECT /*+ MAX_EXECUTION_TIME(60000) */ "
            "spu,product_category,update_time,etl_load_time,product_id "
            "FROM ods_db.ods_lx_product_management WHERE spu IS NOT NULL"
        )
        catalog, ep2 = guarded_select(conn, "catalog", catalog_sql, ())
        season_sql = "SELECT `SPU`,`季节` FROM `SPU季节表`"
        season, ep3 = guarded_select(conn, "season", season_sql, ())
        conn.rollback()
    finally:
        conn.close()
    return sales, catalog, season, {"monthly": ep1, "catalog": ep2, "season": ep3}


def to_date(value):
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.replace(day=1)
    return date.fromisoformat(str(value)[:7] + "-01")


def checked_qty(value):
    if value is None:
        raise ValueError("销量存在NULL：拒绝按零补齐")
    n = Decimal(str(value))
    if n != n.to_integral_value() or n < 0:
        raise ValueError("销量存在负数或非整数")
    return int(n)


def prepare(sales, catalog, season, issue):
    cutoff = shift(issue, 0)
    sku_hist = defaultdict(lambda: defaultdict(int))
    spu_hist = defaultdict(int)
    identities = defaultdict(set)
    months = Counter()
    bad = 0
    for row in sales:
        shop = str(row.get("店铺") or "").strip()
        sku = str(row.get("SKU") or "").strip()
        spu = str(row.get("SPU") or "").strip()
        if shop != SHOP or not sku or not spu:
            bad += 1
            continue
        d = to_date(row["统计日期"])
        if d >= cutoff or d < date(2024, 1, 1):
            raise RuntimeError("月份范围违规")
        qty = checked_qty(row["销量"])
        sku_hist[(sku, spu)][d] += qty
        spu_hist[(SHOP + "|" + spu, d)] += qty
        identities[sku].add(spu)
        months[d] += 1
    if bad:
        raise RuntimeError(f"源数据缺少有效店铺/SKU/SPU：{bad}条，停止")
    conflicts = [sku for sku, sp in identities.items() if len(sp) > 1]
    if conflicts:
        raise RuntimeError(f"同店SKU映射多个SPU：{len(conflicts)}条，停止")
    for i in (1, 2, 3):
        m = shift(cutoff, -i)
        if months[m] == 0:
            raise RuntimeError(f"最近完整月份{m}无记录，停止")
    # Match V3 offline category selection: nonempty category first, then newest.
    def rank(r):
        return (bool(str(r.get("product_category") or "").strip()),
                str(r.get("update_time") or ""), str(r.get("etl_load_time") or ""),
                int(r.get("product_id") or 0))
    categories = {}
    for r in sorted(catalog, key=rank, reverse=True):
        spu = str(r.get("spu") or "").strip()
        if spu:
            categories.setdefault(spu, str(r.get("product_category") or "").strip() or "未映射")
    seasons = {str(r["SPU"]).strip(): str(r["季节"] or "").strip()
               for r in season if str(r.get("SPU") or "").strip()}
    if not spu_hist or not categories:
        raise RuntimeError("历史销量或品类元数据为空")
    safe_hist = {(k, d): q for (k, d), q in spu_hist.items()}
    # Hash every relevant input including SKU grain and metadata; no future truths.
    identity = {
        "cutoff": str(cutoff), "shop": SHOP,
        "monthly": sorted((sku, spu, str(d), q)
                          for (sku, spu), vals in sku_hist.items() for d, q in vals.items()),
        "categories": sorted(categories.items()), "seasons": sorted(seasons.items()),
    }
    input_hash = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return sku_hist, safe_hist, categories, seasons, months, input_hash


def sales_label(d, pred=False):
    return f"{d.year % 100:02d}年{d.month}月" + ("预计销量" if pred else "销量")


def compute_shadow(issue, sku_hist, spu_hist, categories, seasons, source_hash, manifest):
    cutoff = shift(issue, 0)
    candidate = Candidate(spu_hist, categories, ROOT)
    v4 = load_pure(ROOT, SOURCE_FILES[-1])
    sku_input = {
        sku: {"SPU": spu, **{sales_label(d): q for d, q in sorted(vals.items()) if d < cutoff}}
        for (sku, spu), vals in sorted(sku_hist.items())
    }
    sku_input = {sku: x for sku, x in sku_input.items() if len(x) > 1}
    if not sku_input:
        raise RuntimeError("V4无可用输入")
    records = []
    summary = []
    for h in HORIZONS:
        target = shift(cutoff, h)
        labels = [sales_label(shift(cutoff, i), pred=True) for i in range(h + 1)]
        v4sku = v4.compute_forecast_for_shop(
            sku_input, labels, datetime(cutoff.year, cutoff.month, 1),
            spu_season_map=seasons)
        v4_by_spu = Counter()
        for sku, entry in v4sku.items():
            if sku not in sku_input:
                raise RuntimeError("V4返回未知SKU")
            v4_by_spu[sku_input[sku]["SPU"]] += int(entry.get(sales_label(target, True), 0) or 0)
        ar, features, meta = candidate.predict(target, h)
        # Snapshot input contains only months before cutoff. Target truth is absent.
        if any(r.get("实际销量") is not None for r in features.values()):
            raise RuntimeError("TARGET_TRUTH_PRESENT：中止影子运行")
        keys = sorted(set(v4_by_spu) | set(x.split("|", 1)[1] for x in features))
        output_count = 0
        eligible_count = 0
        for spu in keys:
            key = SHOP + "|" + spu
            feature = features.get(key)
            eligibility = feature.get("可预测性") if feature else "NO_FEATURE"
            eligible = eligibility == "ESTABLISHED"
            eligible_count += int(eligible)
            result = ar.get(key)
            raw = result.get(candidate.r12.A16) if result else None
            if result and not eligible:
                raise RuntimeError("A16输出了未授权生命周期对象")
            if eligible and raw is None:
                raise RuntimeError(f"A16缺少合格对象输出:{spu}")
            a16 = int(raw) if raw is not None else None
            if a16 is not None and a16 < 0:
                raise RuntimeError("A16出现负预测")
            output_count += a16 is not None
            v4_qty = v4_by_spu.get(spu) if spu in v4_by_spu else None
            records.append({
                "issue_month": str(cutoff)[:7], "target_month": str(target)[:7],
                "horizon": "H" + str(h), "shop": SHOP, "spu": spu,
                "qualification": eligibility, "status": "OUTPUT" if a16 is not None else "INELIGIBLE",
                "v4_replay": v4_qty, "a16_shadow": a16,
                "delta": None if v4_qty is None or a16 is None else a16 - v4_qty,
                "category": categories.get(spu, "未映射"), "source_hash": source_hash,
                "candidate_version": manifest["candidate"],
            })
        summary.append({
            "horizon": "H" + str(h), "target_month": str(target)[:7],
            "candidate_scope": len(keys), "eligible_count": eligible_count,
            "a16_output_count": output_count, "v4_output_count": len(v4_by_spu),
            "training_max_target": max(meta["training_months"]),
            "training_rows": meta["training_rows"],
            "gamma_down": meta["gamma_down"], "gamma_up": meta["gamma_up"],
            "feature_missing_reads": meta["feature_audit"]["missing_reads"],
            "trust_missing_reads": meta["trust_missing_reads"],
        })
        if output_count != eligible_count:
            raise RuntimeError("合格对象A16输出覆盖率未达100%")
        if max(meta["training_months"]) >= str(cutoff)[:7]:
            raise RuntimeError("训练月份越过截点")
        print(f"[SHADOW] H{h}, target={target}, eligible={eligible_count}, A16={output_count}", flush=True)
    return records, summary


def atomic_json(path, obj):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temp, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--issue-month", required=True, help="YYYY-MM; must be current Asia/Shanghai month")
    parser.add_argument("--output-base", default=str(ROOT / "reports_analysis/a16_shadow_manual"))
    args = parser.parse_args()
    os.umask(0o077)
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    issue = shift(args.issue_month, 0)
    if str(issue)[:7] != now.strftime("%Y-%m"):
        parser.error("仅允许当前业务自然月的单次前瞻影子测试，禁止回填历史批次")
    root_output = (ROOT / "reports_analysis").resolve()
    base = Path(args.output_base).resolve()
    if base != root_output / "a16_shadow_manual":
        parser.error("仅允许隔离研究目录 reports_analysis/a16_shadow_manual")
    base.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="manual_", dir=base))
    status = {"status": "FAILED_CLOSED", "shop": SHOP, "horizons": ["H2", "H3"],
              "issued_at": now.isoformat(), "issue_month": str(issue)[:7],
              "candidate": VERSION, "production_modified": False,
              "source_sync_at": "UNKNOWN_NOT_VERIFIED", "published_v4_compared": False}
    try:
        manifest = verify_freeze()
        status["manifest_base_commit"] = manifest["base_commit"]
        sales, catalog, season, planned = read_sources(issue)
        sku, history, cats, seasons, months, input_hash = prepare(sales, catalog, season, issue)
        records, summary = compute_shadow(issue, sku, history, cats, seasons, input_hash, manifest)
        import csv
        csvpath = run / "a16_v4_shadow_rows.csv"
        with csvpath.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(records[0]), extrasaction="raise")
            writer.writeheader()
            writer.writerows(records)
        status.update({
            "status": "COMPLETE_OBSERVATION_ONLY", "read_only": True,
            "source": "monthly table + current category/season; NOT historical point-in-time",
            "historical_sales_latest_month": str(max(months))[:7],
            "history_monthly_rows": len(sales), "input_hash": input_hash,
            "planned_scan_rows": planned, "models": summary, "output_rows": len(records),
            "source_sync_at": "UNKNOWN_NOT_VERIFIED",
            "limitations": [
                "V4 is replayed using same frozen inputs, NOT compared against actual issued V4 snapshot",
                "Metadata is current, not historically point-in-time",
                "No completed future actuals: WAPE cannot yet be scored",
                "No production forecasting, procurement, or BI artifact updated",
            ],
        })
        atomic_json(run / "shadow_run_meta.json", status)
        print("[PASS] shadow output:", run, flush=True)
        print("[PASS] status=COMPLETE_OBSERVATION_ONLY; no production writes", flush=True)
        return 0
    except Exception as exc:
        status["error_class"] = type(exc).__name__
        # Don't persist raw exception contents: may contain business data or DSN.
        atomic_json(run / "shadow_run_meta.json", status)
        print(f"[BLOCKED] {type(exc).__name__}: {str(exc)[:180]}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
