#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""只读试算：不同产品类型 × 工厂的最新核价 vs 实际下单价。

口径：
1) 飞书核价：同一 SPU 创建时间最新记录的「总计金额」。
2) 实际下单价：领星采购单 item_list[].price（含税单价）。
3) 工厂：采购单 supplier_name。
4) 无核价 SPU 单独输出，不进入价差汇总。
5) 默认不写数据库，仅生成 CSV/JSON 结果文件。
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common import settings, get_logger
from common.database import db_cursor
from common.feishu import FeishuClient
from lingxing import OpenApiBase

logger = get_logger("factory_price_variance")

FEISHU_APP_TOKEN = "PItubmXkWarkqosFanGcxKJGnvT"
FEISHU_TABLE_ID = "tblWgIHRbvi9uWvR"
FEISHU_VIEW_ID = "vew7QHqIW2"
LX_PURCHASE_ROUTE = "/erp/sc/routing/data/local_inventory/purchaseOrderList"

PRODUCT_TABLE_CANDIDATES: Sequence[Tuple[str, str]] = (
    ("ods_db", "ods_lx_product_management_add"),
    ("dim_db", "dim_product_mapping"),
    ("", "产品管理"),
)

FIELD_ALIASES: Mapping[str, Sequence[str]] = {
    "sku": ("SKU", "sku"),
    "spu": ("SPU", "spu"),
    "category": ("品类", "product_category", "category", "产品品类"),
    "product_line": ("品线", "product_line"),
    "season": ("季节", "season"),
    "develop_year": ("开发年份", "develop_year", "year"),
    "brand": ("品牌", "brand"),
    "product_name": ("品名", "product_name", "产品名称"),
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="核价-下单价差异只读试算")
    p.add_argument("--start", default="2026-01-01", help="采购开始日期 YYYY-MM-DD")
    p.add_argument("--end", default=date.today().isoformat(), help="采购结束日期 YYYY-MM-DD")
    p.add_argument("--page-size", type=int, default=500)
    p.add_argument("--anomaly-rate", type=float, default=0.05, help="异常价差率阈值，默认 5%")
    p.add_argument("--output-dir", default="reports_analysis/factory_price_variance")
    p.add_argument("--limit-orders", type=int, default=0, help="调试：最多保留采购单数，0=不限制")
    return p.parse_args()


def clean_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v).strip()
    if isinstance(v, list):
        return "".join(x for x in (clean_text(i) for i in v) if x)
    if isinstance(v, dict):
        for key in ("text", "name", "value", "label"):
            if key in v:
                return clean_text(v.get(key))
        return "".join(x for x in (clean_text(i) for i in v.values()) if x)
    return str(v).strip()


def norm_spu(v: Any) -> str:
    return clean_text(v).upper()


def parse_number(v: Any) -> Optional[float]:
    if v is None or v == "" or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    if isinstance(v, list):
        for x in v:
            n = parse_number(x)
            if n is not None:
                return n
        return None
    if isinstance(v, dict):
        for key in ("value", "text", "amount", "number"):
            if key in v:
                n = parse_number(v.get(key))
                if n is not None:
                    return n
        for x in v.values():
            n = parse_number(x)
            if n is not None:
                return n
        return None
    s = str(v).strip().replace(",", "")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def parse_time_key(v: Any, fallback: Any = None) -> float:
    for candidate in (v, fallback):
        if candidate in (None, ""):
            continue
        if isinstance(candidate, (int, float)):
            x = float(candidate)
            return x / 1000.0 if x > 10_000_000_000 else x
        s = clean_text(candidate)
        if not s:
            continue
        if s.isdigit():
            x = float(s)
            return x / 1000.0 if x > 10_000_000_000 else x
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
            try:
                return datetime.strptime(s, fmt).timestamp()
            except ValueError:
                pass
    return 0.0


def fmt_time(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else ""


async def load_latest_quotes() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    client = FeishuClient(
        app_token=FEISHU_APP_TOKEN,
        table_id=FEISHU_TABLE_ID,
        view_id=FEISHU_VIEW_ID,
    )
    await client.get_access_token()
    field_map = await client.get_table_fields()  # 仅作诊断；字段接口本身可能分页
    records = await client.read_records(page_size=500)
    record_field_names = {str(k) for r in records for k in (r.get("fields", {}) or {}).keys()}
    required = {"款号", "总计金额"}
    missing_fields = sorted(required - record_field_names)
    if missing_fields:
        raise RuntimeError(
            f"飞书核价记录缺少必要字段: {missing_fields}; 实际记录字段={sorted(record_field_names)}"
        )

    latest: Dict[str, Dict[str, Any]] = {}
    blank_spu = 0
    for rec in records:
        fields = rec.get("fields", {}) or {}
        spu = norm_spu(fields.get("款号"))
        if not spu:
            blank_spu += 1
            continue
        total = parse_number(fields.get("总计金额"))
        ts = parse_time_key(fields.get("创建时间"), rec.get("created_time"))
        current = latest.get(spu)
        if current is None or ts >= current["_ts"]:
            latest[spu] = {
                "SPU": spu,
                "最终核价": total,
                "核价类型": clean_text(fields.get("核价类型")),
                "季节_核价表": clean_text(fields.get("季节")),
                "核价时间": fmt_time(ts),
                "飞书record_id": rec.get("record_id", ""),
                "_ts": ts,
            }

    invalid_total = sum(
        1 for row in latest.values()
        if row.get("最终核价") is None or float(row.get("最终核价") or 0) <= 0
    )
    return latest, {
        "feishu_record_count": len(records),
        "latest_spu_count": len(latest),
        "blank_spu_records": blank_spu,
        "latest_spu_invalid_total": invalid_total,
        "field_count_api": len(field_map),
        "record_field_count": len(record_field_names),
    }


async def fetch_purchase_orders(start: str, end: str, page_size: int, limit_orders: int = 0) -> List[Dict[str, Any]]:
    config = settings.lingxing_config
    api = OpenApiBase(
        host=config["host"],
        app_id=config["app_id"],
        app_secret=config["app_secret"],
        proxy_url=config.get("proxy_url") or None,
    )
    token_resp = await api.generate_access_token()
    token = token_resp.access_token
    orders: List[Dict[str, Any]] = []
    offset = 0
    page_size = max(1, min(int(page_size), 500))
    while True:
        body = {
            "start_date": start,
            "end_date": end,
            "search_field_time": "create_time",
            "offset": offset,
            "length": page_size,
        }
        resp = await api.request(token, LX_PURCHASE_ROUTE, "POST", req_body=body, timeout=60)
        try:
            result = resp.model_dump()
        except AttributeError:
            result = resp.dict()
        if result.get("code") != 0:
            raise RuntimeError(
                f"领星采购单接口失败: code={result.get('code')} message={result.get('message')}"
            )
        page = result.get("data") or []
        if not isinstance(page, list):
            raise RuntimeError(f"领星采购单 data 类型异常: {type(page).__name__}")
        orders.extend(x for x in page if isinstance(x, dict))
        if limit_orders and len(orders) >= limit_orders:
            return orders[:limit_orders]
        if len(page) < page_size:
            break
        offset += page_size
    return orders


def flatten_purchase_items(orders: Sequence[Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    skipped_void = skipped_not_ordered = missing_price = missing_spu = 0
    for order in orders:
        status_raw = order.get("status")
        try:
            status = int(status_raw)
        except (TypeError, ValueError):
            status = status_raw
        status_text = clean_text(order.get("status_text"))
        if status in (-1, 124) or "作废" in status_text:
            skipped_void += 1
            continue
        order_time = clean_text(order.get("order_time"))
        if status not in (2, 9) and not order_time:
            skipped_not_ordered += 1
            continue
        supplier = clean_text(order.get("supplier_name"))
        order_sn = clean_text(order.get("order_sn"))
        currency = clean_text(order.get("purchase_currency")) or "CNY"
        rate = parse_number(order.get("purchase_rate")) or 1.0
        for item in order.get("item_list") or []:
            if not isinstance(item, dict):
                continue
            qty = parse_number(item.get("quantity_real")) or 0.0
            price = parse_number(item.get("price"))
            spu = norm_spu(item.get("spu"))
            sku = clean_text(item.get("sku"))
            if not spu and sku:
                spu = norm_spu(sku.split("-", 1)[0])
            if not spu:
                missing_spu += 1
            if price is None:
                missing_price += 1
            if qty <= 0:
                continue
            rows.append({
                "采购单号": order_sn,
                "下单时间": order_time or clean_text(order.get("create_time")),
                "工厂": supplier,
                "SKU": sku,
                "SPU": spu,
                "实际采购量": qty,
                "下单价": price,
                "采购币种": currency,
                "采购汇率": rate,
                "下单价_CNY": (price * rate) if price is not None else None,
                "接口价税合计": parse_number(item.get("amount")),
                "采购单状态": status_text,
            })
    return rows, {
        "order_count_raw": len(orders),
        "purchase_item_rows": len(rows),
        "skipped_void_orders": skipped_void,
        "skipped_not_ordered_orders": skipped_not_ordered,
        "item_missing_price": missing_price,
        "item_missing_spu": missing_spu,
    }


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def table_exists(schema: str, table: str) -> bool:
    sql = """
        SELECT COUNT(*) AS n
        FROM information_schema.tables
        WHERE table_schema = COALESCE(NULLIF(%s, ''), DATABASE())
          AND table_name = %s
    """
    with db_cursor() as cur:
        cur.execute(sql, (schema, table))
        row = cur.fetchone() or {}
        return int(row.get("n") or 0) > 0


def get_columns(schema: str, table: str) -> List[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = COALESCE(NULLIF(%s, ''), DATABASE())
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with db_cursor() as cur:
        cur.execute(sql, (schema, table))
        return [str(r["column_name"]) for r in cur.fetchall()]


def resolve_col(columns: Iterable[str], aliases: Sequence[str]) -> Optional[str]:
    cols = list(columns)
    exact = {c: c for c in cols}
    lower = {c.lower(): c for c in cols}
    for a in aliases:
        if a in exact:
            return exact[a]
        if a.lower() in lower:
            return lower[a.lower()]
    return None


def choose_product_table() -> Tuple[str, str, Dict[str, Optional[str]]]:
    for schema, table in PRODUCT_TABLE_CANDIDATES:
        if not table_exists(schema, table):
            continue
        columns = get_columns(schema, table)
        mapping = {key: resolve_col(columns, aliases) for key, aliases in FIELD_ALIASES.items()}
        if mapping["spu"] or mapping["sku"]:
            return schema, table, mapping
    raise RuntimeError(f"没有找到可用产品维表，候选={PRODUCT_TABLE_CANDIDATES}")


def load_product_dimension() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Any]]:
    schema, table, m = choose_product_table()
    selected = [x for x in dict.fromkeys(v for v in m.values() if v)]
    fields = ", ".join(quote_ident(c) for c in selected)
    full_table = f"{quote_ident(schema)}.{quote_ident(table)}" if schema else quote_ident(table)
    sql = f"SELECT {fields} FROM {full_table}"
    by_spu: Dict[str, Dict[str, Any]] = {}
    by_sku: Dict[str, Dict[str, Any]] = {}
    total_rows = 0
    with db_cursor() as cur:
        cur.execute(sql)
        for raw in cur.fetchall():
            total_rows += 1
            sku = clean_text(raw.get(m["sku"])) if m.get("sku") else ""
            spu = norm_spu(raw.get(m["spu"])) if m.get("spu") else ""
            if not spu and sku:
                spu = norm_spu(sku.split("-", 1)[0])
            dim = {
                "品类": clean_text(raw.get(m["category"])) if m.get("category") else "",
                "品线": clean_text(raw.get(m["product_line"])) if m.get("product_line") else "",
                "季节": clean_text(raw.get(m["season"])) if m.get("season") else "",
                "开发年份": clean_text(raw.get(m["develop_year"])) if m.get("develop_year") else "",
                "品牌": clean_text(raw.get(m["brand"])) if m.get("brand") else "",
                "品名": clean_text(raw.get(m["product_name"])) if m.get("product_name") else "",
            }
            if spu and (spu not in by_spu or any(dim.values())):
                by_spu[spu] = dim
            if sku:
                by_sku[sku] = dim
    return by_spu, by_sku, {
        "product_table": f"{schema + '.' if schema else ''}{table}",
        "product_rows_read": total_rows,
        "product_spu_count": len(by_spu),
        "product_sku_count": len(by_sku),
        "product_column_mapping": m,
    }


def join_and_calculate(
    purchase_rows: Sequence[Mapping[str, Any]],
    quotes: Mapping[str, Mapping[str, Any]],
    product_by_spu: Mapping[str, Mapping[str, Any]],
    product_by_sku: Mapping[str, Mapping[str, Any]],
    anomaly_rate: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    matched: List[Dict[str, Any]] = []
    missing_quote: List[Dict[str, Any]] = []
    anomalies: List[Dict[str, Any]] = []
    for p in purchase_rows:
        spu = norm_spu(p.get("SPU"))
        sku = clean_text(p.get("SKU"))
        dim = product_by_spu.get(spu) or product_by_sku.get(sku) or {}
        q = quotes.get(spu)
        row = dict(p)
        row.update({
            "品类": clean_text(dim.get("品类")),
            "品线": clean_text(dim.get("品线")),
            "季节": clean_text(dim.get("季节")),
            "开发年份": clean_text(dim.get("开发年份")),
            "品牌": clean_text(dim.get("品牌")),
            "品名": clean_text(dim.get("品名")),
            "最终核价": q.get("最终核价") if q else None,
            "核价类型": q.get("核价类型", "") if q else "",
            "核价时间": q.get("核价时间", "") if q else "",
            "飞书record_id": q.get("飞书record_id", "") if q else "",
        })
        quote_value = row.get("最终核价")
        actual = p.get("下单价_CNY")
        qty = float(p.get("实际采购量") or 0)
        if quote_value is None or float(quote_value or 0) <= 0:
            row.update({"核价状态": "无核价", "单件价差": None, "价差率": None, "金额影响": None})
            missing_quote.append(row)
            continue
        if actual is None:
            row.update({"核价状态": "有核价_缺下单价", "单件价差": None, "价差率": None, "金额影响": None})
            missing_quote.append(row)
            continue
        quote_value = float(quote_value)
        actual = float(actual)
        diff = actual - quote_value
        rate = diff / quote_value if quote_value else None
        impact = diff * qty
        row.update({"核价状态": "有核价", "单件价差": diff, "价差率": rate, "金额影响": impact})
        matched.append(row)
        if rate is not None and abs(rate) >= anomaly_rate:
            anomalies.append(row)

    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in matched:
        key = (clean_text(r.get("品类")) or "未分类", clean_text(r.get("工厂")) or "未知工厂")
        g = groups.setdefault(key, {
            "品类": key[0], "工厂": key[1], "采购明细行数": 0, "SPU集合": set(),
            "采购数量": 0.0, "核价金额基准": 0.0, "实际采购金额": 0.0, "金额影响": 0.0,
        })
        qty = float(r.get("实际采购量") or 0)
        q = float(r.get("最终核价") or 0)
        actual = float(r.get("下单价_CNY") or 0)
        g["采购明细行数"] += 1
        if r.get("SPU"):
            g["SPU集合"].add(r["SPU"])
        g["采购数量"] += qty
        g["核价金额基准"] += q * qty
        g["实际采购金额"] += actual * qty
        g["金额影响"] += (actual - q) * qty

    summary: List[Dict[str, Any]] = []
    for g in groups.values():
        qty = g["采购数量"]
        quote_amt = g["核价金额基准"]
        actual_amt = g["实际采购金额"]
        summary.append({
            "品类": g["品类"], "工厂": g["工厂"], "SPU数": len(g["SPU集合"]),
            "采购明细行数": g["采购明细行数"], "采购数量": qty,
            "加权核价": quote_amt / qty if qty else None,
            "加权下单价": actual_amt / qty if qty else None,
            "加权单件价差": (actual_amt - quote_amt) / qty if qty else None,
            "价差率": (actual_amt - quote_amt) / quote_amt if quote_amt else None,
            "金额影响": g["金额影响"],
        })
    summary.sort(key=lambda x: abs(float(x.get("金额影响") or 0)), reverse=True)
    anomalies.sort(key=lambda x: abs(float(x.get("金额影响") or 0)), reverse=True)
    return matched, missing_quote, anomalies, summary


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    columns: List[str] = []
    seen = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                columns.append(str(k))
                seen.add(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: round(row.get(k), 6) if isinstance(row.get(k), float) else row.get(k) for k in columns})


async def main() -> int:
    a = parse_args()
    datetime.strptime(a.start, "%Y-%m-%d")
    datetime.strptime(a.end, "%Y-%m-%d")
    if a.start > a.end:
        raise ValueError("--start 不能晚于 --end")

    outdir = Path(a.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    logger.info("1/4 读取飞书最新核价（总计金额）...")
    quotes, quote_stats = await load_latest_quotes()
    logger.info("飞书：%s", quote_stats)

    logger.info("2/4 拉取领星采购单 %s ~ %s ...", a.start, a.end)
    orders = await fetch_purchase_orders(a.start, a.end, a.page_size, a.limit_orders)
    purchase_rows, purchase_stats = flatten_purchase_items(orders)
    logger.info("采购：%s", purchase_stats)

    logger.info("3/4 读取产品维度...")
    product_by_spu, product_by_sku, product_stats = load_product_dimension()
    logger.info("产品维：%s", product_stats)

    logger.info("4/4 计算价差并输出...")
    matched, missing_quote, anomalies, summary = join_and_calculate(
        purchase_rows, quotes, product_by_spu, product_by_sku, a.anomaly_rate
    )

    prefix = f"factory_price_variance_{a.start.replace('-', '')}_{a.end.replace('-', '')}"
    paths = {
        "matched": outdir / f"{prefix}_有核价明细.csv",
        "missing": outdir / f"{prefix}_无核价及缺价明细.csv",
        "anomaly": outdir / f"{prefix}_价差异常.csv",
        "summary": outdir / f"{prefix}_品类工厂汇总.csv",
        "meta": outdir / f"{prefix}_运行摘要.json",
    }
    write_csv(paths["matched"], matched)
    write_csv(paths["missing"], missing_quote)
    write_csv(paths["anomaly"], anomalies)
    write_csv(paths["summary"], summary)

    meta = {
        "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start": a.start,
        "end": a.end,
        "quote_source": {
            "app_token": FEISHU_APP_TOKEN,
            "table_id": FEISHU_TABLE_ID,
            "view_id": FEISHU_VIEW_ID,
            "final_quote_field": "总计金额",
            **quote_stats,
        },
        "purchase_source": {"route": LX_PURCHASE_ROUTE, **purchase_stats},
        "product_source": product_stats,
        "result": {
            "matched_rows": len(matched),
            "missing_quote_or_price_rows": len(missing_quote),
            "anomaly_rows": len(anomalies),
            "summary_groups": len(summary),
            "matched_spu": len({r.get("SPU") for r in matched if r.get("SPU")}),
            "missing_quote_spu": len({r.get("SPU") for r in missing_quote if r.get("SPU") and r.get("核价状态") == "无核价"}),
            "included_purchase_qty": sum(float(r.get("实际采购量") or 0) for r in matched),
            "included_price_impact_cny": sum(float(r.get("金额影响") or 0) for r in matched),
        },
        "output_files": {k: str(v) for k, v in paths.items() if k != "meta"},
    }
    paths["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
