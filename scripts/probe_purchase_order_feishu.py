#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""只读探查领星采购单接口与指定飞书多维表，不执行任何写入。"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import httpx

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import settings
from lingxing import OpenApiBase

WIKI_TOKEN = "XlmewVybKiwwbYkHwxucZHKjnRe"
TABLE_ID = "tblpmbdZo37wLVd2"
VIEW_ID = "vewvgszonZ"
FEISHU_BASE = "https://open.feishu.cn/open-apis"
LX_ROUTE = "/erp/sc/routing/data/local_inventory/purchaseOrderList"

FIELD_TYPES = {
    1: "文本", 2: "数字", 3: "单选", 4: "多选", 5: "日期", 7: "复选框",
    11: "人员", 13: "电话", 15: "超链接", 17: "附件", 18: "关联",
    19: "查找引用", 20: "公式", 21: "双向关联", 1001: "创建时间",
    1002: "最后更新时间", 1003: "创建人", 1004: "修改人", 1005: "自动编号",
}

ALIASES = {
    "order_sn": ["采购单号", "订单号", "采购订单号", "单号"],
    "custom_order_sn": ["自定义采购单号", "自定义单号"],
    "create_time": ["创建时间", "采购单创建时间", "下单时间"],
    "gmt_modified": ["修改时间", "更新时间", "最后更新时间"],
    "status_text": ["采购单状态", "状态"],
    "status_shipped_text": ["到货状态", "发货状态"],
    "ware_house_name": ["仓库", "仓库名称"],
    "supplier_name": ["供应商", "供应商名称"],
    "opt_realname": ["采购员", "创建人", "负责人"],
    "total_price": ["采购金额", "总金额", "含税金额"],
    "purchase_currency": ["币种", "采购币种"],
    "remark": ["备注", "采购备注"],
    "item_list[].sku": ["SKU"],
    "item_list[].fnsku": ["FNSKU"],
    "item_list[].msku": ["MSKU"],
    "item_list[].product_name": ["产品名称", "商品名称", "品名"],
    "item_list[].quantity_real": ["实际数量", "采购数量", "下单数量", "数量"],
    "item_list[].quantity_arrival": ["到货数量", "已到货数量"],
    "item_list[].quantity_receive": ["入库数量", "已入库数量"],
    "item_list[].sid": ["店铺ID", "SID"],
    "item_list[].seller_name": ["店铺", "店铺名称"],
    "item_list[].price": ["采购单价", "单价"],
}


def args():
    p = argparse.ArgumentParser(description="探查领星采购单接口与飞书采购单表")
    p.add_argument("--wiki-token", default=WIKI_TOKEN)
    p.add_argument("--table-id", default=TABLE_ID)
    p.add_argument("--view-id", default=VIEW_ID)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--page-size", type=int, default=50)
    p.add_argument("--sample-records", type=int, default=10)
    p.add_argument("--output-dir", default="")
    a = p.parse_args()
    if not 1 <= a.days <= 366:
        p.error("--days 必须在1到366之间")
    if not 1 <= a.page_size <= 500:
        p.error("--page-size 必须在1到500之间")
    if not 0 <= a.sample_records <= 100:
        p.error("--sample-records 必须在0到100之间")
    return a


def plain(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    raise TypeError(type(value).__name__)


def safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(k): safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [safe(v) for v in value]
    return str(value)


def value_type(v: Any) -> str:
    if v is None: return "null"
    if isinstance(v, bool): return "bool"
    if isinstance(v, int): return "int"
    if isinstance(v, float): return "float"
    if isinstance(v, str): return "str"
    if isinstance(v, list): return "list"
    if isinstance(v, dict): return "dict"
    return type(v).__name__


def example(v: Any) -> Any:
    v = safe(v)
    if isinstance(v, str) and len(v) > 120:
        return v[:117] + "..."
    if isinstance(v, list):
        return v[:3]
    if isinstance(v, dict):
        return dict(list(v.items())[:8])
    return v


def schema(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    total = len(rows)
    for key in sorted({str(k) for row in rows for k in row}):
        values = [row.get(key) for row in rows]
        nonempty = [v for v in values if v not in (None, "", [], {})]
        result.append({
            "field": key,
            "non_empty": len(nonempty),
            "total": total,
            "non_empty_rate": round(len(nonempty) / total, 4) if total else 0,
            "types": dict(Counter(value_type(v) for v in nonempty)),
            "example": example(nonempty[0]) if nonempty else None,
        })
    return result


def norm(s: str) -> str:
    return re.sub(r"[\s_\-（）()\[\]【】/\\]+", "", str(s or "")).lower()


def mapping(fields, order_schema, item_schema):
    sources = {x["field"] for x in order_schema}
    sources |= {"item_list[]." + x["field"] for x in item_schema}
    normalized = {norm(x): x for x in sources}
    output = []
    for f in fields:
        target = f.get("field_name", "")
        candidates = []
        if target in sources:
            candidates.append((100, target, "完全同名"))
        if norm(target) in normalized:
            candidates.append((95, normalized[norm(target)], "规范化后同名"))
        for src, names in ALIASES.items():
            if src not in sources:
                continue
            for name in names:
                if norm(target) == norm(name):
                    candidates.append((90, src, f"语义别名：{name}"))
                elif norm(name) and (norm(name) in norm(target) or norm(target) in norm(name)):
                    candidates.append((70, src, f"名称包含：{name}"))
        best = {}
        for score, src, reason in candidates:
            if src not in best or score > best[src][0]:
                best[src] = (score, reason)
        ranked = sorted(
            [{"source": src, "score": v[0], "reason": v[1]} for src, v in best.items()],
            key=lambda x: (-x["score"], x["source"]),
        )
        output.append({
            "target": target,
            "field_id": f.get("field_id"),
            "type": f.get("type"),
            "type_name": FIELD_TYPES.get(f.get("type"), "未知"),
            "candidates": ranked[:5],
        })
    return output


async def feishu_probe(a) -> Dict[str, Any]:
    if not settings.FEISHU_APP_ID or not settings.FEISHU_APP_SECRET:
        raise RuntimeError("FEISHU_APP_ID / FEISHU_APP_SECRET 未配置")
    timeout = httpx.Timeout(60, connect=15)
    async with httpx.AsyncClient(timeout=timeout) as c:
        r = await c.post(f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal", json={
            "app_id": settings.FEISHU_APP_ID,
            "app_secret": settings.FEISHU_APP_SECRET,
        })
        r.raise_for_status()
        token_result = r.json()
        if token_result.get("code") != 0:
            raise RuntimeError(f"获取飞书Token失败: {token_result}")
        headers = {"Authorization": "Bearer " + token_result["tenant_access_token"]}

        r = await c.get(f"{FEISHU_BASE}/wiki/v2/spaces/get_node", headers=headers,
                        params={"token": a.wiki_token})
        r.raise_for_status()
        node_result = r.json()
        if node_result.get("code") != 0:
            raise RuntimeError(f"解析Wiki节点失败: {node_result}")
        node = node_result.get("data", {}).get("node", {})
        app_token = node.get("obj_token")
        if node.get("obj_type") != "bitable" or not app_token:
            raise RuntimeError(f"Wiki节点不是多维表: {node}")

        async def paged(url: str):
            items, page_token = [], None
            while True:
                params = {"page_size": 100}
                if page_token: params["page_token"] = page_token
                res = await c.get(url, headers=headers, params=params)
                res.raise_for_status()
                body = res.json()
                if body.get("code") != 0: raise RuntimeError(str(body))
                data = body.get("data", {}) or {}
                items.extend(data.get("items", []) or [])
                page_token = data.get("page_token")
                if not data.get("has_more") or not page_token: break
            return items

        tables = await paged(f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables")
        fields = await paged(
            f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{a.table_id}/fields"
        )
        records = {"items": [], "total": None}
        if a.sample_records:
            params = {"page_size": min(a.sample_records, 100)}
            if a.view_id: params["view_id"] = a.view_id
            r = await c.get(
                f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{a.table_id}/records",
                headers=headers, params=params,
            )
            r.raise_for_status()
            body = r.json()
            if body.get("code") != 0: raise RuntimeError(f"读取样例失败: {body}")
            records = body.get("data", {}) or records

    target = next((x for x in tables if x.get("table_id") == a.table_id), None)
    names = {norm(x.get("field_name", "")) for x in fields}
    item_hits = names & {norm(x) for x in ["SKU", "FNSKU", "MSKU", "产品名称", "采购数量", "实际数量"]}
    order_hits = names & {norm(x) for x in ["采购单号", "订单号", "供应商", "仓库", "状态"]}
    grain = "采购单商品明细级" if len(item_hits) >= 2 else ("采购单级" if order_hits else "待确认")
    return {
        "wiki_node": node,
        "app_token": app_token,
        "target_table": target,
        "all_tables": tables,
        "record_count": records.get("total"),
        "fields": fields,
        "field_summary": [{
            "field_id": x.get("field_id"), "field_name": x.get("field_name"),
            "type": x.get("type"), "type_name": FIELD_TYPES.get(x.get("type"), "未知"),
            "is_primary": x.get("is_primary"), "property": x.get("property"),
        } for x in fields],
        "sample_records": records.get("items", []) or [],
        "sample_value_schema": schema([x.get("fields", {}) for x in records.get("items", []) or []]),
        "grain": grain,
    }


async def lingxing_probe(a) -> Dict[str, Any]:
    if not settings.LINGXING_APP_ID or not settings.LINGXING_APP_SECRET:
        raise RuntimeError("LINGXING_APP_ID / LINGXING_APP_SECRET 未配置")
    api = OpenApiBase(settings.LINGXING_HOST, settings.LINGXING_APP_ID,
                      settings.LINGXING_APP_SECRET, settings.LINGXING_PROXY_URL or None)
    token = await api.generate_access_token()
    end = datetime.now().date()
    start = end - timedelta(days=a.days - 1)
    body = {
        "start_date": start.strftime("%Y-%m-%d"), "end_date": end.strftime("%Y-%m-%d"),
        "search_field_time": "create_time", "offset": 0, "length": a.page_size,
    }
    result = plain(await api.request(token.access_token, LX_ROUTE, "POST", req_body=body, timeout=60))
    raw = result.get("data", [])
    if isinstance(raw, list):
        orders, meta = raw, {}
    elif isinstance(raw, dict):
        orders = raw.get("list") or raw.get("items") or raw.get("data") or []
        meta = {k: v for k, v in raw.items() if k not in {"list", "items", "data"}}
    else:
        orders, meta = [], {"unexpected_data_type": value_type(raw)}
    orders = [x for x in orders if isinstance(x, dict)]
    items = [item for order in orders for item in (order.get("item_list", []) or []) if isinstance(item, dict)]
    return {
        "route": LX_ROUTE, "request": body, "response_code": result.get("code"),
        "response_message": result.get("message") or result.get("msg"), "response_meta": meta,
        "order_count": len(orders), "item_count": len(items),
        "status_distribution": dict(Counter(str(x.get("status_text") or "<空>") for x in orders)),
        "arrival_status_distribution": dict(Counter(str(x.get("status_shipped_text") or "<空>") for x in orders)),
        "order_schema": schema(orders), "item_schema": schema(items),
        "sample_orders": orders[:5], "sample_items": items[:10],
    }


def md_table(headers, rows):
    def cell(x): return str("" if x is None else x).replace("|", "\\|").replace("\n", " ")
    return "\n".join([
        "| " + " | ".join(map(cell, headers)) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
        *["| " + " | ".join(map(cell, row)) + " |" for row in rows],
    ])


def render(report):
    f, l, m = report.get("feishu"), report.get("lingxing"), report.get("mapping", [])
    out = ["# 领星采购单 → 飞书多维表探查报告", "", f"生成时间：{report['generated_at']}",
           "", "模式：只读，未创建、删除、新增或更新任何飞书记录。", ""]
    if report.get("errors"):
        out += ["## 错误", ""] + [f"- {k}: {v}" for k, v in report["errors"].items()] + [""]
    if f:
        out += ["## 飞书目标表", "", f"- Wiki标题：{f['wiki_node'].get('title')}",
                f"- app_token：`{f['app_token']}`", f"- 表名：{(f.get('target_table') or {}).get('name')}",
                f"- table_id：`{report['config']['table_id']}`", f"- 记录数：{f.get('record_count')}",
                f"- 初步粒度：{f.get('grain')}", "",
                md_table(["字段", "field_id", "类型", "主字段"], [[x['field_name'], x['field_id'],
                         f"{x['type_name']}({x['type']})", x['is_primary']] for x in f['field_summary']]), ""]
    if l:
        out += ["## 领星采购单接口", "", f"- 接口：`{l['route']}`",
                f"- 查询范围：{l['request']['start_date']} 至 {l['request']['end_date']}",
                f"- 采购单：{l['order_count']}，商品明细：{l['item_count']}",
                f"- 状态：{json.dumps(l['status_distribution'], ensure_ascii=False)}",
                f"- 到货状态：{json.dumps(l['arrival_status_distribution'], ensure_ascii=False)}", "",
                "### 采购单字段", "", md_table(["字段", "非空率", "类型", "样例"], [[x['field'],
                x['non_empty_rate'], json.dumps(x['types'], ensure_ascii=False), json.dumps(x['example'], ensure_ascii=False)]
                for x in l['order_schema']]), "", "### 商品明细字段", "",
                md_table(["字段", "非空率", "类型", "样例"], [[x['field'], x['non_empty_rate'],
                json.dumps(x['types'], ensure_ascii=False), json.dumps(x['example'], ensure_ascii=False)]
                for x in l['item_schema']]), ""]
    if m:
        out += ["## 初步字段映射", "", md_table(["飞书字段", "类型", "建议源字段", "分数", "依据"], [[x['target'],
                x['type_name'], (x['candidates'][0]['source'] if x['candidates'] else "待确认"),
                (x['candidates'][0]['score'] if x['candidates'] else ""),
                (x['candidates'][0]['reason'] if x['candidates'] else "")] for x in m]), ""]
    out += ["## 处理原则建议", "", "1. 正式同步采用唯一键增量新增/更新，不采用先清空再全量写入。",
            "2. 若接口有明细ID，唯一键优先为采购单号+明细ID；否则验证采购单号+SKU/FNSKU是否唯一。",
            "3. 采购单状态和到货状态会变化，先采用最近90天滚动回刷，再根据状态增加缓存。",
            "4. 正式脚本先实现 --dry-run，输出新增、更新、跳过、冲突数量后再开放写入。", ""]
    return "\n".join(out)


async def main_async():
    a = args()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = Path(a.output_dir).resolve() if a.output_dir else ROOT / "reports" / "purchase_order_probe" / stamp
    output.mkdir(parents=True, exist_ok=True)
    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "config": {"wiki_token": a.wiki_token, "table_id": a.table_id, "view_id": a.view_id,
                         "days": a.days, "page_size": a.page_size}, "errors": {}}
    print("只读探查开始，不会写入或删除飞书数据")
    try:
        print("[1/3] 飞书表结构与样例...")
        report["feishu"] = await feishu_probe(a)
        print(f"      表名={(report['feishu'].get('target_table') or {}).get('name')}，字段={len(report['feishu']['fields'])}")
    except Exception as e:
        report["errors"]["feishu"] = str(e); print("      失败：", e)
    try:
        print("[2/3] 领星采购单接口...")
        report["lingxing"] = await lingxing_probe(a)
        print(f"      采购单={report['lingxing']['order_count']}，明细={report['lingxing']['item_count']}")
    except Exception as e:
        report["errors"]["lingxing"] = str(e); print("      失败：", e)
    print("[3/3] 字段映射与报告...")
    if report.get("feishu") and report.get("lingxing"):
        report["mapping"] = mapping(report["feishu"]["fields"], report["lingxing"]["order_schema"],
                                    report["lingxing"]["item_schema"])
    else:
        report["mapping"] = []
    (output / "probe_report.json").write_text(json.dumps(safe(report), ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "probe_report.md").write_text(render(report), encoding="utf-8")
    print("报告：", output / "probe_report.md")
    print("JSON：", output / "probe_report.json")
    return 2 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
