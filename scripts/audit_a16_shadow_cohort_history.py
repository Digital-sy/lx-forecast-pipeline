#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only monthly sales evidence for SAME 500 JQ-US A16 shadow-eligible SPUs.

Compare frozen Dec-2026/Jan-2027 projections against Dec-2025,
Jan-2026 and completed Jul-Sep-2026 sales. Historical references are NOT
future actuals and NOT forecast accuracy measurements.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
ALLOWED_BASE = ROOT / "reports_analysis" / "a16_shadow_manual"
HORIZONS = ("H2", "H3")
MONTHS = ("2025-12", "2026-01", "2026-07", "2026-08", "2026-09")
ANNUAL = {"H2": "2025-12", "H3": "2026-01"}
MAX_PLAN_ROWS = 1000000
MAX_RETURN_GROUPS = 25000


def number(value, label, allow_missing=False):
    if value is None or str(value).strip() == "":
        if allow_missing:
            return None
        raise ValueError(f"{label}: missing")
    try:
        n = Decimal(str(value))
        if not n.is_finite() or n != n.to_integral_value() or n < 0:
            raise ValueError()
        return int(n)
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"{label}: invalid nonnegative integer") from e


def month(text):
    if isinstance(text, (date, datetime)):
        return text.strftime("%Y-%m")
    x = str(text)
    if len(x) != 7 or date.fromisoformat(x + "-01").strftime("%Y-%m") != x:
        raise ValueError("invalid month")
    return x


def scan_estimate(plan):
    estimates = []
    def traverse(node):
        if isinstance(node, dict):
            if "rows_examined_per_scan" in node:
                estimates.append(int(node["rows_examined_per_scan"]))
            for value in node.values():
                traverse(value)
        elif isinstance(node, list):
            for value in node:
                traverse(value)
    traverse(plan)
    return sum(estimates) if estimates else None


def load_shadow(run):
    sys.path.insert(0, str(ROOT / "scripts"))
    from audit_a16_shadow_result import REQUIRED, audit
    csvfile = run / "a16_v4_shadow_rows.csv"
    original = csvfile.read_bytes()
    with csvfile.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        missing = REQUIRED - set(reader.fieldnames or ())
        if missing:
            raise ValueError("shadow CSV missing columns: " + repr(sorted(missing)))
        rows = list(reader)
    meta = json.loads((run / "shadow_run_meta.json").read_text(encoding="utf-8"))
    audit(rows, meta, hashlib.sha256(original).hexdigest())
    if meta.get("issue_month") != "2026-10" or meta.get("candidate") != "a16_shop_cutoff_v3_20261016":
        raise ValueError("not the approved Oct 2026 candidate batch")
    cohort = {}
    by_h = {h: {} for h in HORIZONS}
    for row in rows:
        if row["qualification"] != "ESTABLISHED":
            continue
        h, spu = row["horizon"], row["spu"]
        cohort.setdefault(spu, row["category"])
        if cohort[spu] != row["category"]:
            raise ValueError("category differs between horizons")
        by_h[h][spu] = {
            "v4": number(row["v4_replay"], "v4"),
            "a16": number(row["a16_shadow"], "a16"),
        }
    if len(cohort) != 500 or set(by_h["H2"]) != set(cohort) or set(by_h["H3"]) != set(cohort):
        raise ValueError("cohort mismatch: expected same 500 SPUs for H2 and H3")
    return meta, cohort, by_h, hashlib.sha256(original).hexdigest()


def fetch_monthly(cohort):
    import pymysql
    from common.database import get_db_connection
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
            cursor.execute("SET SESSION MAX_EXECUTION_TIME=60000")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        names = sorted(cohort)
        placeholders = ",".join(["%s"] * len(names))
        sql = (
            "SELECT /*+ MAX_EXECUTION_TIME(60000) */ \`SPU\`, "
            "DATE_FORMAT(\`统计日期\`, '%%Y-%%m') AS month_key, "
            "SUM(\`销量\`) AS total_units, COUNT(*) AS source_rows, "
            "SUM(CASE WHEN \`销量\` IS NULL THEN 1 ELSE 0 END) AS null_rows, "
            "SUM(CASE WHEN \`销量\` < 0 THEN 1 ELSE 0 END) AS negative_rows "
            "FROM \`销量统计_msku月度\` "
            "WHERE \`店铺\`=%s AND \`统计日期\` >= %s AND \`统计日期\` < %s "
            f"AND \`SPU\` IN ({placeholders}) "
            "GROUP BY \`SPU\`, DATE_FORMAT(\`统计日期\`, '%%Y-%%m')"
        )
        params = ("JQ-US", "2024-01-01", "2026-10-01", *names)
        with conn.cursor() as c:
            c.execute("EXPLAIN FORMAT=JSON " + sql, params)
            plan_raw = c.fetchone()[0]
        plan = json.loads(plan_raw) if isinstance(plan_raw, str) else plan_raw
        estimated = scan_estimate(plan)
        if estimated is None or estimated > MAX_PLAN_ROWS:
            raise RuntimeError("SQL_PLAN_BLOCKED (no safe bounded scan estimate)")
        print(f"[EXPLAIN] estimated_scan_rows={estimated:,}, limit={MAX_PLAN_ROWS:,}", flush=True)
        result = []
        with conn.cursor(pymysql.cursors.SSDictCursor) as c:
            c.execute(sql, params)
            for row in c:
                result.append(row)
                if len(result) > MAX_RETURN_GROUPS:
                    raise RuntimeError("MONTHLY_GROUP_ROW_LIMIT")
        conn.rollback()
        print(f"[READ] grouped_shop_spu_month_rows={len(result):,}", flush=True)
        return result, estimated
    finally:
        conn.close()


def monthly_data(rows, cohort):
    sales = {}
    earliest = {}
    seen_months = set()
    for r in rows:
        spu = str(r.get("SPU") or "").strip()
        if spu not in cohort:
            raise ValueError("non-cohort SPU in SQL results")
        m = month(r["month_key"])
        if not ("2024-01" <= m < "2026-10"):
            raise ValueError("month outside read-only cutoff")
        if number(r["null_rows"], "NULL source count") or number(r["negative_rows"], "negative source count"):
            raise ValueError("NULL or negative monthly sales source")
        q = number(r["total_units"], "monthly quantity")
        if (spu, m) in sales:
            raise ValueError("duplicate group for SPU/month")
        sales[(spu, m)] = q
        seen_months.add(m)
        if q > 0:
            earliest[spu] = min(earliest.get(spu, m), m)
    for m in ("2026-07", "2026-08", "2026-09"):
        if m not in seen_months:
            raise ValueError("no source data in completed month: " + m)
    if not sales:
        raise ValueError("no monthly source rows")
    return sales, earliest


def actual_state(spu, m, sales, earliest):
    qty = sales.get((spu, m))
    if qty is not None:
        return "MONTHLY_OBSERVED", qty
    first = earliest.get(spu)
    if first is None or m < first:
        return "NO_PRIOR_POSITIVE_OBSERVED", None
    return "MONTHLY_ABSENT_UNKNOWN", None


def analyze(cohort, pred, sales, earliest):
    detail = []
    for spu, cat in sorted(cohort.items()):
        historic = {}
        for m in MONTHS:
            st, q = actual_state(spu, m, sales, earliest)
            historic[m] = {"state": st, "units": q}
        recent = [historic[m]["units"] for m in ("2026-07", "2026-08", "2026-09")]
        recent_complete = all(q is not None for q in recent)
        for h in HORIZONS:
            ref = ANNUAL[h]
            item = pred[h][spu]
            last = historic[ref]["units"]
            detail.append({
                "shop": "JQ-US", "spu": spu, "category_current": cat, "horizon": h,
                "target_future": "2026-12" if h == "H2" else "2027-01",
                "observed_first_positive_since_2024": earliest.get(spu, ""),
                "v4_future_replay": item["v4"], "a16_future_shadow": item["a16"],
                "a16_minus_v4": item["a16"] - item["v4"],
                "reference_month": ref, "reference_state": historic[ref]["state"],
                "reference_actual": last,
                "v4_over_reference": None if last in (None, 0) else item["v4"] / last,
                "a16_over_reference": None if last in (None, 0) else item["a16"] / last,
                "recent_three_months_known": int(recent_complete),
                "recent_3m_avg": None if not recent_complete else sum(recent) / 3,
                "jul26_state": historic["2026-07"]["state"], "jul26": recent[0],
                "aug26_state": historic["2026-08"]["state"], "aug26": recent[1],
                "sep26_state": historic["2026-09"]["state"], "sep26": recent[2],
            })
    summary = []
    category_summary = []
    for h in HORIZONS:
        rows = [x for x in detail if x["horizon"] == h]
        hist_known = [x for x in rows if x["reference_actual"] is not None]
        fully_recent = [x for x in rows if x["recent_three_months_known"]]
        valid_positive = [x for x in hist_known if x["reference_actual"] > 0]
        ref_total = sum(x["reference_actual"] for x in hist_known)
        v4_on_ref = sum(x["v4_future_replay"] for x in hist_known)
        a16_on_ref = sum(x["a16_future_shadow"] for x in hist_known)
        r3 = sum(x["recent_3m_avg"] for x in fully_recent)
        v4_recent = sum(x["v4_future_replay"] for x in fully_recent)
        a16_recent = sum(x["a16_future_shadow"] for x in fully_recent)
        summary.append({
            "horizon": h, "target_future": rows[0]["target_future"],
            "cohort_spus": len(rows),
            "v4_total_future": sum(x["v4_future_replay"] for x in rows),
            "a16_total_future": sum(x["a16_future_shadow"] for x in rows),
            "year_ago_month": ANNUAL[h], "year_ago_observed_n": len(hist_known),
            "year_ago_no_prior_positive_n": sum(x["reference_state"] == "NO_PRIOR_POSITIVE_OBSERVED" for x in rows),
            "year_ago_unknown_n": sum(x["reference_state"] == "MONTHLY_ABSENT_UNKNOWN" for x in rows),
            "year_ago_observed_zero_n": len(hist_known) - len(valid_positive),
            "year_ago_observed_units": ref_total,
            "v4_future_units_same_year_ago_observed_spus": v4_on_ref,
            "a16_future_units_same_year_ago_observed_spus": a16_on_ref,
            "v4_to_year_ago_ratio_paired": None if ref_total == 0 else v4_on_ref / ref_total,
            "a16_to_year_ago_ratio_paired": None if ref_total == 0 else a16_on_ref / ref_total,
            "recent_jul_sep_all3_observed_n": len(fully_recent),
            "recent_jul_sep_avg_month_units_on_fully_observed_subset": r3,
            "v4_future_units_same_recent_observed_subset": v4_recent,
            "a16_future_units_same_recent_observed_subset": a16_recent,
            "v4_to_recent_avg_paired": None if r3 == 0 else v4_recent / r3,
            "a16_to_recent_avg_paired": None if r3 == 0 else a16_recent / r3,
        })
        cats = defaultdict(list)
        for x in rows:
            cats[x["category_current"]].append(x)
        for cat, rs in sorted(cats.items()):
            yes = [x for x in rs if x["reference_actual"] is not None]
            cat_hist = sum(x["reference_actual"] for x in yes)
            cat_v4 = sum(x["v4_future_replay"] for x in yes)
            cat_a16 = sum(x["a16_future_shadow"] for x in yes)
            category_summary.append({
                "horizon": h, "category_current": cat, "spus": len(rs),
                "v4_future": sum(x["v4_future_replay"] for x in rs),
                "a16_future": sum(x["a16_future_shadow"] for x in rs),
                "year_ago_observed_spus": len(yes),
                "year_ago_observed_units": cat_hist,
                "v4_on_year_ago_observed_spus": cat_v4,
                "a16_on_year_ago_observed_spus": cat_a16,
                "v4_to_year_ago_on_paired": None if cat_hist == 0 else cat_v4 / cat_hist,
                "a16_to_year_ago_on_paired": None if cat_hist == 0 else cat_a16 / cat_hist,
            })
    return detail, summary, category_summary


def write_csv(path, rows):
    if not rows:
        raise ValueError("refuse empty CSV")
    with path.open("x", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def main():
    p = argparse.ArgumentParser(description="A16同批500 SPU历史实销参考，非未来准确率")
    p.add_argument("--run-dir", required=True)
    args = p.parse_args()
    run = Path(args.run_dir).resolve()
    if run.parent != ALLOWED_BASE.resolve() or not run.name.startswith("manual_"):
        p.error("only approved reports_analysis/a16_shadow_manual/manual_* run dirs")
    os.umask(0o077)
    meta, cohort, pred, digest = load_shadow(run)
    output = Path(tempfile.mkdtemp(prefix="cohort_history_", dir=run))
    status = {
        "status": "FAILED_CLOSED", "shop": "JQ-US", "cohort_spus": len(cohort),
        "shadow_run_dir": run.name, "shadow_input_hash": meta["input_hash"],
        "shadow_csv_sha256": digest, "source": "销量统计_msku月度",
        "start": "2024-01", "end_exclusive": "2026-10",
        "source_sync_at": "UNKNOWN_NOT_VERIFIED",
        "comparison_kind": "HISTORIC_SALES_REFERENCE_NOT_FUTURE_ACCURACY",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        sql_rows, plan_rows = fetch_monthly(cohort)
        sales, earliest = monthly_data(sql_rows, cohort)
        detail, summary, categories = analyze(cohort, pred, sales, earliest)
        if len(detail) != 1000 or len(summary) != 2:
            raise ValueError("cohort row-count conservation violation")
        write_csv(output / "cohort_500_history_detail.csv", detail)
        write_csv(output / "cohort_500_history_by_category.csv", categories)
        status.update({
            "status": "COMPLETE_HISTORICAL_REFERENCE_ONLY",
            "sql_plan_estimated_scan_rows": plan_rows,
            "sql_grouped_rows": len(sql_rows),
            "forecast_vs_hist_summary": summary,
            "limitations": [
                "Prior-year actuals are NOT future actuals and do not measure future forecast accuracy.",
                "Month-absent stays UNKNOWN; no positive before month is NOT confirmed launch date.",
                "Never divide full-cohort future forecasts by observed-subset historical sales.",
                "Current shadow category labels are not historical as-of categories.",
                "Historical monthly source may have been restated after original issue month.",
                "Source synchronization freshness is not separately verified.",
            ],
        })
        (output / "cohort_history_summary.json").write_text(
            json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[PASS] historical reference built; production unchanged", flush=True)
        for x in summary:
            h = x["horizon"]
            print(f"[{h}] cohort=500, year_ago_observed={x['year_ago_observed_n']}, "
                  f"pre_first_positive={x['year_ago_no_prior_positive_n']}, "
                  f"unknown={x['year_ago_unknown_n']}, "
                  f"year_ago_units={x['year_ago_observed_units']:,}, "
                  f"V4_on_matched={x['v4_future_units_same_year_ago_observed_spus']:,}, "
                  f"A16_on_matched={x['a16_future_units_same_year_ago_observed_spus']:,}, "
                  f"recent_all3={x['recent_jul_sep_all3_observed_n']}", flush=True)
        print("Research-only output:", output, flush=True)
        return 0
    except Exception as error:
        status["error_class"] = type(error).__name__
        (output / "cohort_history_summary.json").write_text(
            json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[BLOCKED] {type(error).__name__}; result dir: {output}",
              file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
