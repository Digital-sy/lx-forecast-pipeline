#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline verification of one-shot A16 shadow outputs. No network/DB/BI access."""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import sys

REQUIRED = {"issue_month","target_month","horizon","shop","spu","qualification",
            "status","v4_replay","a16_shadow","delta","category","source_hash",
            "candidate_version"}
HORIZONS = ("H2","H3")

def integer(x, name, missing_ok=False, signed=False):
    if x is None or str(x).strip()=="":
        if missing_ok:
            return None
        raise ValueError(name+" missing")
    try:
        n=Decimal(str(x))
        if not n.is_finite() or n!=n.to_integral_value() or (not signed and n<0):
            raise ValueError()
        return int(n)
    except (InvalidOperation,ValueError) as e:
        raise ValueError(name+" invalid integer") from e

def next_month(month, step):
    y,m=map(int,month.split("-"))
    d=date(y,m,1)
    z=d.year*12+d.month-1+step
    return f"{z//12:04d}-{z%12+1:02d}"

def audit(rows, meta, digest):
    if meta.get("status")!="COMPLETE_OBSERVATION_ONLY" or meta.get("production_modified") is not False:
        raise ValueError("shadow status is not complete")
    if meta.get("shop")!="JQ-US" or tuple(meta.get("horizons",()))!=HORIZONS:
        raise ValueError("out of authorized shadow scope")
    if meta.get("published_v4_compared") is not False:
        raise ValueError("unexpected published-V4 comparison state")
    issue=meta["issue_month"]
    if len(issue)!=7 or len(meta["input_hash"])!=64:
        raise ValueError("invalid issue/input hash")
    model={m["horizon"]:m for m in meta["models"]}
    if set(model)!=set(HORIZONS):
        raise ValueError("missing horizons")
    groups=defaultdict(list)
    seen=set()
    for r in rows:
        h=r["horizon"]
        if h not in HORIZONS or r["shop"]!="JQ-US" or r["issue_month"]!=issue:
            raise ValueError("row out of scope")
        if r["target_month"]!=next_month(issue,int(h[1:])):
            raise ValueError("target month mismatch")
        if r["source_hash"]!=meta["input_hash"] or r["candidate_version"]!=meta["candidate"]:
            raise ValueError("source hash or model version mismatch")
        key=(h,r["shop"],r["spu"])
        if not r["spu"] or key in seen:
            raise ValueError("empty/duplicate SPU key")
        seen.add(key)
        v=integer(r["v4_replay"],"V4",missing_ok=True)
        a=integer(r["a16_shadow"],"A16",missing_ok=True)
        delta=integer(r["delta"],"delta",missing_ok=True,signed=True)
        eligible=r["qualification"]=="ESTABLISHED"
        if eligible!=(a is not None) or eligible!=(r["status"]=="OUTPUT"):
            raise ValueError("qualification/output state mismatch")
        if eligible and v is None:
            raise ValueError("V4 missing on A16 eligible item")
        if (v is None or a is None)!=(delta is None):
            raise ValueError("delta presence mismatch")
        if delta is not None and delta!=a-v:
            raise ValueError("delta arithmetic wrong")
        groups[h].append(dict(r,v=v,a=a,change=delta))
    if len(rows)!=meta["output_rows"]:
        raise ValueError("output row count mismatch")
    result={"status":"PASS_OUTPUT_INTEGRITY_ONLY",
            "scope":"JQ-US H2/H3", "issue_month":issue,
            "candidate":meta["candidate"],"input_hash":meta["input_hash"],
            "csv_sha256":digest,"source_sync_at":meta.get("source_sync_at"),
            "published_v4_compared":False,
            "note":"V4 is replay; no future actuals or accuracy score; output is local sensitive data",
            "horizons":{}}
    for h in HORIZONS:
        m=model[h]
        rows_h=groups[h]
        accepted=[r for r in rows_h if r["a"] is not None]
        if len(rows_h)!=m["candidate_scope"] or len(accepted)!=m["eligible_count"]:
            raise ValueError(h+" scope/eligibility mismatch")
        if len(accepted)!=m["a16_output_count"] or sum(r["v"] is not None for r in rows_h)!=m["v4_output_count"]:
            raise ValueError(h+" prediction coverage mismatch")
        if m["training_max_target"]>=issue:
            raise ValueError("training target crosses issue cutoff")
        if not accepted:
            raise ValueError(h+" no A16 eligible items")
        total_v=sum(r["v"] for r in accepted)
        total_a=sum(r["a"] for r in accepted)
        total_abs=sum(abs(r["change"]) for r in accepted)
        by_size=sorted(accepted,key=lambda r:abs(r["change"]),reverse=True)
        big=[r for r in accepted if r["v"]>=100 and abs(r["change"])>=100 and abs(r["change"])/r["v"]>=.5]
        result["horizons"][h]={
            "target_month":m["target_month"],
            "candidate_scope":len(rows_h),"qualified_spu_count":len(accepted),
            "noneligible_count":len(rows_h)-len(accepted),
            "qualification_counts":dict(Counter(r["qualification"] for r in rows_h)),
            "qualified_a16_coverage":1.0,
            "v4_units_on_qualified":total_v,"a16_units_on_qualified":total_a,
            "net_unit_delta":total_a-total_v,
            "net_delta_pct_of_v4":None if total_v==0 else (total_a-total_v)/total_v,
            "sum_abs_spu_delta":total_abs,
            "a16_lower_count":sum(r["change"]<0 for r in accepted),
            "a16_higher_count":sum(r["change"]>0 for r in accepted),
            "a16_equal_count":sum(r["change"]==0 for r in accepted),
            "v4_zero_count":sum(r["v"]==0 for r in accepted),
            "a16_zero_count":sum(r["a"]==0 for r in accepted),
            "over_50pct_and_100units_count":len(big),
            "top10_share_of_abs_delta":None if not total_abs else sum(abs(r["change"]) for r in by_size[:10])/total_abs,
            "top20_abs_changes":[{"spu":r["spu"],"v4":r["v"],"a16":r["a"],"delta":r["change"],
                                   "ratio":None if r["v"]==0 else r["change"]/r["v"]}
                                  for r in by_size[:20]],
            "gamma_down":m.get("gamma_down"),"gamma_up":m.get("gamma_up"),
            "feature_missing_reads":m.get("feature_missing_reads"),
            "trust_missing_reads":m.get("trust_missing_reads"),
        }
    return result

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--run-dir",required=True)
    args=p.parse_args()
    root=Path(__file__).resolve().parents[1]/"reports_analysis"/"a16_shadow_manual"
    run=Path(args.run_dir).resolve()
    if run.parent!=root.resolve() or not run.name.startswith("manual_"):
        p.error("only isolated manual_* directory allowed")
    path=run/"a16_v4_shadow_rows.csv"
    raw=path.read_bytes()
    with path.open("r",encoding="utf-8-sig",newline="") as f:
        reader=csv.DictReader(f)
        missing=REQUIRED-set(reader.fieldnames or [])
        if missing:
            raise ValueError("missing columns: "+repr(sorted(missing)))
        rows=list(reader)
    meta=json.loads((run/"shadow_run_meta.json").read_text(encoding="utf-8"))
    report=audit(rows,meta,hashlib.sha256(raw).hexdigest())
    os.umask(0o077)
    temp=run/"shadow_acceptance_summary.json.tmp"
    final=run/"shadow_acceptance_summary.json"
    temp.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    os.replace(temp,final)
    print("[PASS] Local shadow data integrity; no DB or production writes")
    for h,g in report["horizons"].items():
        pct=g["net_delta_pct_of_v4"]
        print(f"{h}: qualified={g['qualified_spu_count']}, "
              f"V4={g['v4_units_on_qualified']:,}, A16={g['a16_units_on_qualified']:,}, "
              f"delta={g['net_unit_delta']:+,}, pct={pct:+.2%}, "
              f"lower/higher/same={g['a16_lower_count']}/{g['a16_higher_count']}/{g['a16_equal_count']}, "
              f"large_moves={g['over_50pct_and_100units_count']}")
    print("Local sensitive report:",final)
    return 0

if __name__=="__main__":
    sys.exit(main())
