#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline JQ-US A16 shadow zero-transition, concentration and cross-H audit.

Only reads one existing manual shadow CSV and status JSON; never connects to DB.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import csv
from hashlib import sha256
import json
import os
from pathlib import Path
import sys

BASE = Path(__file__).resolve().parents[1] / "reports_analysis/a16_shadow_manual"
sys.path.insert(0, str(Path(__file__).resolve().parent))
from audit_a16_shadow_result import REQUIRED, audit, integer


def diagnose(rows, meta, digest):
    # Re-use strict validation of the previous round before analyzing business changes.
    prior = audit(rows, meta, digest)
    grouped = defaultdict(dict)
    for raw in rows:
        if raw["qualification"] != "ESTABLISHED":
            continue
        h, spu = raw["horizon"], raw["spu"]
        grouped[h][spu] = {
            "v4": integer(raw["v4_replay"], "v4"),
            "a16": integer(raw["a16_shadow"], "a16"),
            "delta": integer(raw["delta"], "delta", signed=True),
            "category": raw["category"],
        }
    result = {
        "status": "PASS_OFFLINE_RISK_DIAGNOSTIC_ONLY",
        "issue_month": meta["issue_month"],
        "shop": "JQ-US",
        "source_hash": meta["input_hash"],
        "shadow_csv_sha256": digest,
        "published_v4_compared": False,
        "scope_note": "only 500 A16-eligible mature SPUs; no future actuals or model WAPE",
        "horizons": {},
    }
    for h in ("H2", "H3"):
        eligible = grouped[h]
        if len(eligible) != prior["horizons"][h]["qualified_spu_count"]:
            raise RuntimeError("eligible scope drift")
        quadrants = Counter()
        zeroed = []
        activated = []
        large_cuts = []
        category_stats = defaultdict(lambda: {"n": 0, "v4": 0, "a16": 0})
        for spu, x in eligible.items():
            v, a = x["v4"], x["a16"]
            c = category_stats[x["category"]]
            c["n"] += 1
            c["v4"] += v
            c["a16"] += a
            if v == 0 and a == 0:
                quadrants["both_zero"] += 1
            elif v > 0 and a == 0:
                quadrants["positive_to_zero"] += 1
                zeroed.append({"spu": spu, "v4": v, "a16": a, "delta": -v})
            elif v == 0 and a > 0:
                quadrants["zero_to_positive"] += 1
                activated.append({"spu": spu, "v4": v, "a16": a, "delta": a})
            else:
                quadrants["both_positive"] += 1
            if v >= 100 and v - a >= 100 and a <= 0.5*v:
                large_cuts.append({"spu": spu, "v4": v, "a16": a,
                                   "delta": a-v, "reduction_pct": 1-a/v})
        if sum(quadrants.values()) != len(eligible):
            raise RuntimeError("quadrant reconciliation failed")
        if (quadrants["both_zero"] + quadrants["positive_to_zero"]
                != prior["horizons"][h]["a16_zero_count"]):
            raise RuntimeError("A16 zero counts disagree with prior acceptance")
        if (quadrants["both_zero"] + quadrants["zero_to_positive"]
                != prior["horizons"][h]["v4_zero_count"]):
            raise RuntimeError("V4 zero counts disagree with prior acceptance")
        total_v = sum(v["v4"] for v in eligible.values())
        total_a = sum(v["a16"] for v in eligible.values())
        if total_v != prior["horizons"][h]["v4_units_on_qualified"]:
            raise RuntimeError("V4 totals drift")
        if total_a != prior["horizons"][h]["a16_units_on_qualified"]:
            raise RuntimeError("A16 totals drift")
        cats = [
            {"category": k, **v, "delta": v["a16"]-v["v4"],
             "reduction_pct": None if v["v4"] == 0 else 1-v["a16"]/v["v4"]}
            for k,v in category_stats.items()
        ]
        zeroed.sort(key=lambda v:v["v4"], reverse=True)
        activated.sort(key=lambda v:v["a16"], reverse=True)
        large_cuts.sort(key=lambda v:v["v4"]-v["a16"], reverse=True)
        result["horizons"][h] = {
            "target_month": prior["horizons"][h]["target_month"],
            "eligible_count": len(eligible),
            "zero_transition": dict(quadrants),
            "zero_transition_net": quadrants["positive_to_zero"] - quadrants["zero_to_positive"],
            "v4_units_lost_when_a16_zero": sum(v["v4"] for v in zeroed),
            "new_a16_units_when_v4_zero": sum(v["a16"] for v in activated),
            "positive_to_zero_v4_ge100": sum(v["v4"]>=100 for v in zeroed),
            "positive_to_zero_v4_ge1000": sum(v["v4"]>=1000 for v in zeroed),
            "large_cuts_count": len(large_cuts),
            "large_cuts_total_reduction": sum(-v["delta"] for v in large_cuts),
            "positive_to_zero_top15": zeroed[:15],
            "zero_to_positive_top15": activated[:15],
            "large_cuts_top20": large_cuts[:20],
            "category_top10_abs_delta": sorted(cats,key=lambda v:abs(v["delta"]),reverse=True)[:10],
        }
    h2, h3 = grouped["H2"], grouped["H3"]
    common = sorted(h2.keys() & h3.keys())
    result["cross_horizon"] = {
        "common_eligible": len(common),
        "only_H2": len(h2.keys()-h3.keys()),
        "only_H3": len(h3.keys()-h2.keys()),
        "a16_same_quantity": sum(h2[k]["a16"]==h3[k]["a16"] for k in common),
        "a16_both_zero": sum(h2[k]["a16"]==h3[k]["a16"]==0 for k in common),
        "a16_same_positive": sum(h2[k]["a16"]==h3[k]["a16"]>0 for k in common),
        "v4_both_zero": sum(h2[k]["v4"]==h3[k]["v4"]==0 for k in common),
        "a16_h3_less_than_h2": sum(h3[k]["a16"]<h2[k]["a16"] for k in common),
        "a16_h3_greater_than_h2": sum(h3[k]["a16"]>h2[k]["a16"] for k in common),
    }
    if result["cross_horizon"]["only_H2"] or result["cross_horizon"]["only_H3"]:
        result["status"] = "PARTIAL_ELIGIBLE_SET_CHANGED"
    result["attention_spus"] = {
        k: {
            h: {"v4": grouped[h][k]["v4"], "a16": grouped[h][k]["a16"],
                "delta": grouped[h][k]["delta"]}
            for h in ("H2","H3") if k in grouped[h]
        }
        for k in ("ZQZ399", "BX517", "BX422", "BX389", "LTY316", "ZSY904")
    }
    return result


def main():
    ap = argparse.ArgumentParser(description="只读本地已有A16影子CSV，分解零预测转换及大额降幅")
    ap.add_argument("--run-dir", required=True)
    args = ap.parse_args()
    run = Path(args.run_dir).resolve()
    if run.parent != BASE.resolve() or not run.name.startswith("manual_"):
        ap.error("仅允许隔离研究目录 reports_analysis/a16_shadow_manual/manual_*")
    src = run/"a16_v4_shadow_rows.csv"
    raw = src.read_bytes()
    with src.open(encoding="utf-8-sig", newline="") as file:
        rd = csv.DictReader(file)
        if missing := REQUIRED - set(rd.fieldnames or []):
            raise ValueError("missing columns: " + repr(sorted(missing)))
        rows = list(rd)
    meta = json.loads((run/"shadow_run_meta.json").read_text(encoding="utf-8"))
    report = diagnose(rows, meta, sha256(raw).hexdigest())
    os.umask(0o077)
    temp = run/"shadow_risk_breakdown.json.tmp"
    dest = run/"shadow_risk_breakdown.json"
    temp.write_text(json.dumps(report,ensure_ascii=False,indent=2), encoding="utf-8")
    os.replace(temp,dest)
    print("[PASS] offline zero-transition and multi-horizon risk review; no DB/BI/procurement access")
    for h, v in report["horizons"].items():
        print(f"{h}: zero-transitions={v['zero_transition']}, "
              f"positive->zero V4 units={v['v4_units_lost_when_a16_zero']:,}, "
              f"V4>=100 zeroed={v['positive_to_zero_v4_ge100']}, "
              f"large cuts={v['large_cuts_count']}")
        if v["positive_to_zero_top15"]:
            print(h,"top zeroed:", " ; ".join(
                f"{x['spu']} (V4 {x['v4']})" for x in v["positive_to_zero_top15"][:10]))
    print("Cross-H:",report["cross_horizon"])
    print("Research-only output:",dest)
    return 0


if __name__=="__main__":
    sys.exit(main())
