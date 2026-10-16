#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mature-SPU legacy-V4 forecast retrospective backtest; read-only MySQL.

This program REPLAYS today's repository v4 on historical completed-month inputs.
It is NOT a measurement of immutable historical, originally-issued forecasts.
One bounded monthly-sales SELECT, then all projections/aggregations are in Python.
Missing target-month actual rows are UNKNOWN by default, not assumed to be zero.

Run from repository root:
  python3 scripts/audit_mature_spu_accuracy_readonly.py --months 2026-07,2026-08,2026-09 --horizons 1,2,3
"""
from __future__ import annotations
import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

import pymysql
from common.database import get_db_connection
from jobs.feishu.forecast_sales_improved import compute_forecast_for_shop, load_spu_season_map
from jobs.feishu.generate_forecast_comparison import extract_spu_from_sku

SHOPS = ("JQ-US", "MT-US", "RKZ-US", "SY-US")

def add_months(d, offset):
    n = d.year * 12 + d.month - 1 + offset
    return date(n // 12, n % 12 + 1, 1)

def mdiff(a, b):
    return (b.year - a.year) * 12 + (b.month - a.month)

def parsed_month(v):
    if isinstance(v, (datetime, date)):
        return v.replace(day=1).date() if isinstance(v, datetime) else v.replace(day=1)
    return datetime.strptime(str(v)[:7], "%Y-%m").date().replace(day=1)

def sales_label(d):
    return f"{str(d.year)[-2:]}年{d.month}月销量"

def pred_label(d):
    return f"{str(d.year)[-2:]}年{d.month}月预计销量"

def dump_csv(path, rows):
    if not rows:
        return
    cols = list(rows[0])
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def ratio(v):
    return "N/A" if v is None else f"{v:.2%}"

def metrics(rows, pred_key):
    n = len(rows)
    actual = sum(x["actual"] for x in rows)
    pred = sum(x[pred_key] for x in rows)
    ae = sum(abs(x[pred_key] - x["actual"]) for x in rows)
    pos = [x for x in rows if x["actual"] > 0]
    mape = sum(abs(x[pred_key] - x["actual"]) / x["actual"] for x in pos) / len(pos) if pos else None
    return {
        "rows": n,
        "actual_units": actual,
        "pred_units": pred,
        "wape": ae / actual if actual > 0 else None,
        "accuracy_1_minus_wape": 1 - ae / actual if actual > 0 else None,
        "bias": (pred - actual) / actual if actual > 0 else None,
        "mape_nonzero": mape,
        "hit_20_nonzero": sum(abs(x[pred_key]-x["actual"])/x["actual"] <= .20 for x in pos) / len(pos) if pos else None,
        "hit_30_nonzero": sum(abs(x[pred_key]-x["actual"])/x["actual"] <= .30 for x in pos) / len(pos) if pos else None,
        "abs_error_units": ae,
    }

def read_data(start, end, max_execution_ms):
    conn = get_db_connection()
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    first_positive = {}
    observed_month = set()
    monthly_source = defaultdict(lambda: [0, 0])
    sku_spu_conflicts = defaultdict(set)
    count = 0
    try:
        # This session is read-only at the MySQL transaction layer.
        # It does not change production tables, sessions of other users, or schedules.
        with conn.cursor() as c:
            c.execute("SET SESSION TRANSACTION READ ONLY")
            c.execute("SET SESSION MAX_EXECUTION_TIME=%s", (max_execution_ms,))
            c.execute("SELECT COUNT(*) FROM information_schema.COLUMNS "
                      "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='销量统计_msku月度' AND COLUMN_NAME='SPU'")
            has_spu = c.fetchone()[0] > 0
            c.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        spu_col = ", `SPU`" if has_spu else ""
        shops = ','.join(['%s'] * len(SHOPS))
        sql = ("SELECT /*+ MAX_EXECUTION_TIME(%d) */ `SKU`,`店铺`,`统计日期`,`销量`%s "
               "FROM `销量统计_msku月度` WHERE `统计日期` >= %%s AND `统计日期` < %%s "
               "AND `店铺` IN (%s) AND `SKU` IS NOT NULL AND `SKU` NOT IN ('','无')") % (
                   max_execution_ms, spu_col, shops)
        print("[SQL] 单次只读查询销量源:", start.isoformat(), "至", end.isoformat(), "店铺", SHOPS, flush=True)
        with conn.cursor(pymysql.cursors.SSDictCursor) as cur:
            cur.execute(sql, (start.isoformat(), end.isoformat(), *SHOPS))
            for row in cur:
                sku = str(row.get("SKU") or "").strip()
                shop = str(row.get("店铺") or "").strip()
                raw_spu = str(row.get("SPU") or "").strip()
                spu = raw_spu or extract_spu_from_sku(sku)
                if not (shop and sku and spu):
                    continue
                month = parsed_month(row.get("统计日期"))
                value = int(row.get("销量") or 0)
                if value < 0:
                    raise ValueError(f"检测到负月销量，停止：{shop}/{sku}/{month}")
                data[shop][(sku, spu)][month] += value
                sku_spu_conflicts[shop, sku].add(spu)
                observed_month.add((shop, spu, month))
                monthly_source[shop, month][0] += 1
                monthly_source[shop, month][1] += value
                if value > 0:
                    key = (shop, spu)
                    if key not in first_positive or month < first_positive[key]:
                        first_positive[key] = month
                count += 1
                if count % 100000 == 0:
                    print(f"[READ] {count:,} 行", flush=True)
        conn.rollback()  # read-only transaction; no writes to commit
    finally:
        conn.close()
    conflicts = [(shop, sku, sorted(v)) for (shop, sku), v in sku_spu_conflicts.items() if len(v) > 1]
    return data, first_positive, observed_month, monthly_source, count, conflicts

def classify_age(earliest, snapshot):
    if earliest is None:
        return "NO_PRIOR_POSITIVE"
    months = mdiff(earliest, snapshot)
    if months >= 12: return "OLD_12P"
    if months >= 6: return "OLD_6_TO_11"
    if months >= 4: return "ESTABLISHED_4_TO_5"
    return "RECENT_OBSERVED_0_TO_3"

def demand_band(actual):
    if actual == 0: return "0"
    if actual < 50: return "1-49"
    if actual < 200: return "50-199"
    if actual < 1000: return "200-999"
    return "1000+"

def main():
    p = argparse.ArgumentParser(description="老品SPU销量预测准确率（历史回放，仅只读）")
    p.add_argument("--months", default="2026-07,2026-08,2026-09", help="已完成自然月，逗号分隔YYYY-MM")
    p.add_argument("--horizons", default="1,2,3", help="H1,H2,H3对应1,2,3")
    p.add_argument("--sales-start", default="2024-01", help="用于证实老品历史的起始月；越早越完整")
    p.add_argument("--timeout-ms", type=int, default=60000, help="数据库只读单查询最大毫秒数")
    p.add_argument("--output-dir", default="reports_analysis/old_spu_accuracy_readonly")
    args = p.parse_args()
    months = sorted(set(parsed_month(z.strip()) for z in args.months.split(',') if z.strip()))
    horizons = sorted(set(int(z.strip()) for z in args.horizons.split(',') if z.strip()))
    if not months or not horizons or any(h not in (1,2,3) for h in horizons):
        p.error("至少指定一个完整目标月及H1/H2/H3")
    today = date.today()
    if any(m >= today.replace(day=1) for m in months):
        p.error("不可比较尚未完成的自然月；请检查业务时区")
    earliest_snapshot = min(add_months(m, -max(horizons)) for m in months)
    start = parsed_month(args.sales_start)
    if start >= earliest_snapshot:
        p.error("--sales-start 必须早于最早快照月")
    end = add_months(max(months), 1)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    print("[INFO] 审计为当前代码的历史回放，不代表历史实际签发！",flush=True)
    data, first_positive, observed, source, rows_read, sku_conflicts = read_data(start, end, args.timeout_ms)
    if sku_conflicts:
        dump_csv(output / "sku_spu_mapping_conflicts.csv", [dict(shop=a,sku=b,spus='|'.join(c)) for a,b,c in sku_conflicts])
        raise RuntimeError(f"同店铺+SKU出现{len(sku_conflicts)}个多SPU映射冲突，已导出，停止计算")
    if not rows_read:
        raise RuntimeError("销量月表无有效返回，停止计算")
    # Materialize source facts once, then forecast only from observations strictly before snapshot.
    rows = []
    # Per SKU key is (sku, source-validated-spu), with at most one SPU per shop+SKU.
    detail = []
    season_map = load_spu_season_map()  # Current metadata, so historical season attribution is approximate.
    for target in months:
        target_summaries = [source.get((shop, target), [0, 0]) for shop in SHOPS]
        if sum(x[0] for x in target_summaries) == 0:
            raise RuntimeError(f"目标月{target}完全无实际销量数据，停止，不能按0算")
        for h in horizons:
            snapshot = add_months(target, -h)
            print(f"[REPLAY] target={target} H{h} snapshot={snapshot}",flush=True)
            # Data dictionary already contains original SPU link along with SKU.
            forecasts = defaultdict(int)
            eligible = set()
            actual = defaultdict(int)
            actual_known = set()
            naive = defaultdict(int)
            prior_year = defaultdict(int)
            for shop, skus in data.items():
                xhistory = {}
                for sku_spu, sm in skus.items():
                    sku, spu = sku_spu
                    history = {"SPU": spu}
                    for d, qty in sm.items():
                        if d < snapshot:
                            history[sales_label(d)] = qty
                        if d == target:
                            actual[shop,spu] += qty
                            actual_known.add((shop,spu))
                        if d == add_months(snapshot,-1):
                            naive[shop,spu] += qty
                        if d == add_months(target,-12):
                            prior_year[shop,spu] += qty
                    if len(history) > 1:
                        xhistory[sku] = history
                        earliest = first_positive.get((shop,spu))
                        if earliest and earliest < snapshot:
                            eligible.add((shop,spu))
                if not xhistory: continue
                labels=[pred_label(add_months(snapshot,i)) for i in range(h+1)]
                out=compute_forecast_for_shop(xhistory,labels,datetime(snapshot.year,snapshot.month,1),spu_season_map=season_map)
                tlabel=pred_label(target)
                for sku, val in out.items():
                    spu=xhistory[sku]["SPU"]
                    forecasts[shop,spu]+=int(val.get(tlabel,0) or 0)
            for shop,spu in sorted(eligible):
                first=first_positive.get((shop,spu))
                cls=classify_age(first,snapshot)
                a = actual[shop,spu]
                pqty = forecasts[shop,spu]
                detail.append({
                    "target_month":target.isoformat(),"snapshot_month":snapshot.isoformat(),"horizon":f"H{h}",
                    "shop":shop,"spu":spu,"age_class":cls,"observed_age_months":mdiff(first,snapshot),
                    "actual_row_present": int((shop,spu) in actual_known),"actual":a,
                    "pred_v4_replay":pqty,"baseline_last_completed_month":naive[shop,spu],
                    "baseline_last_year_same_target":prior_year[shop,spu],
                    "season_CURRENT_METADATA":season_map.get(spu,"未知"),
                    "actual_sales_band":demand_band(a),
                    "error_v4":pqty-a,"abs_error_v4":abs(pqty-a),
                })
    dump_csv(output / "old_spu_detail_all_coverage.csv",detail)
    for key, (cnt, qty) in sorted(source.items()):
        rows.append({"shop":key[0],"month":key[1].isoformat(),"source_rows":cnt,"source_units":qty})
    dump_csv(output / "source_month_coverage.csv",rows)
    mature = [x for x in detail if x['age_class'] in ('OLD_12P','OLD_6_TO_11','ESTABLISHED_4_TO_5')]
    scored = [x for x in mature if x['actual_row_present'] == 1]
    # Proven-zero rows are included if source recorded them. Source-absent rows remain UNKNOWN, excluded from the primary score.
    scored_12 = [x for x in scored if x['age_class'] == 'OLD_12P']
    print(f"[COVERAGE] Mature forecast records={len(mature)}, actual-observed={len(scored)}, actual-unknown={len(mature)-len(scored)}",flush=True)
    def groups(rows_, fields):
        buckets=defaultdict(list)
        for x in rows_:
            buckets[tuple(x[k] for k in fields)].append(x)
        for key, sub in sorted(buckets.items()):
            for label, col in [('V4_回放','pred_v4_replay'),('上月延续','baseline_last_completed_month'),('去年同月','baseline_last_year_same_target')]:
                yield dict(zip(fields,key)) | {"model":label} | metrics(sub,col)
    summary=[]
    for scope, sample in [("OLD_12P",scored_12),("ESTABLISHED_OBSERVED_4P",scored)]:
        for fields in [[],['horizon'],['target_month','horizon'],['shop','horizon'],['age_class','horizon'],['actual_sales_band','horizon'],['season_CURRENT_METADATA','horizon']]:
            for r in groups(sample,fields):
                summary.append({'scope':scope,'group_by':'+'.join(fields) if fields else 'OVERALL',**r})
    dump_csv(output / "summary_old_accuracy.csv",summary)
    top=sorted([x for x in scored_12],key=lambda x:x['abs_error_v4'],reverse=True)[:100]
    dump_csv(output / "top100_old_spu_errors.csv",top)
    summary_json={
        "kind":"RETROSPECTIVE_CURRENT_V4_REPLAY_NOT_ORIGINAL_FORECAST",
        "target_months":[x.isoformat() for x in months],"horizons":horizons,
        "sales_source":"销量统计_msku月度","sales_start":start.isoformat(),"source_rows":rows_read,
        "mature_prediction_rows":len(mature),"mature_rows_with_actual":len(scored),"mature_rows_actual_unknown":len(mature)-len(scored),
        "confirmed_old_12plus_scored":len(scored_12),
        "notes":[
            "Classification is based on earliest observed positive sales month since sales_start, as-of snapshot, by shop+SPU.",
            "Only >=12 months is strong positive evidence of maturity; younger estimated ages may be history-truncated.",
            "Forecast model uses current code and current season_map, not immutable historical binary/config.",
            "Target-month actual rows absent are UNKNOWN, not zero. Observed-only scores can be selection biased.",
            "WAPE can exceed 100%, thus 1-WAPE may be negative; not a bounded accuracy percentage.",
            "Historical month sales and metadata may be revised after that month; cannot prove point-in-time source completeness.",
            "Run end-to-end forecast model only on completed pre-snapshot-month sales; no target leakage in sales quantities.",
        ],
    }
    (output/'audit_meta.json').write_text(json.dumps(summary_json,ensure_ascii=False,indent=2),encoding='utf-8')
    for scope,sample in [('老品12个月以上（可观测）',scored_12),('成熟4个月以上（可观测）',scored)]:
        print('\n===',scope,'===',flush=True)
        for h in horizons:
            rs=[x for x in sample if x['horizon']==f'H{h}']
            m=metrics(rs,'pred_v4_replay')
            b=metrics(rs,'baseline_last_completed_month')
            print(f"H{h}: 样本={m['rows']:,} 实销={m['actual_units']:,} V4回放预测={m['pred_units']:,} "
                  f"WAPE={ratio(m['wape'])} Bias={ratio(m['bias'])} 命中±30%={ratio(m['hit_30_nonzero'])} "
                  f"上月延续WAPE={ratio(b['wape'])}",flush=True)
    print('\n输出目录:',output.resolve(),flush=True)
    print('注意：审计为历史回放；实际缺记录不补0；还需要核验月份数据完整性与模型版本。',flush=True)

if __name__ == '__main__':
    sys.exit(main())