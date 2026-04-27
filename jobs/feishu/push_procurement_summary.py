#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购建议月度摘要推送
每月8日8:00自动运行，推送给生产经理和产品经理

推送内容：
  生产经理：建议下单量摘要 + 实际下单量 + 运营填报完成率
  产品经理：定制面料预计用量摘要

Crontab 配置（在服务器上执行 crontab -e 添加）：
  0 8 8 * * /opt/apps/pythondata/venv/bin/python -m jobs.feishu.push_procurement_summary >> /opt/apps/pythondata/logs/push_procurement_summary.log 2>&1
"""

import json
import os
import re
import sys
import time
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger('push_procurement_summary')

# ── 飞书 App 配置（复用 pythondata .env 里已有的） ────────────────────────
try:
    from common.config import settings
    FEISHU_APP_ID     = settings.FEISHU_APP_ID
    FEISHU_APP_SECRET = settings.FEISHU_APP_SECRET
except Exception:
    FEISHU_APP_ID     = os.getenv('FEISHU_APP_ID', '')
    FEISHU_APP_SECRET = os.getenv('FEISHU_APP_SECRET', '')

FEISHU_BASE = "https://open.feishu.cn/open-apis"

# ── 收件人（open_id） ─────────────────────────────────────────────────────
# 目前两份报告都发给刘宗霖，后续可拆分
RECEIVER_PRODUCTION = "ou_45d24eddffa044503caf29d6c8a2e003"  # 生产经理
RECEIVER_PRODUCT    = "ou_45d24eddffa044503caf29d6c8a2e003"  # 产品经理


# ── Token 缓存 ────────────────────────────────────────────────────────────
_token: str = ""
_token_expires_at: float = 0.0

def _get_token() -> str:
    global _token, _token_expires_at
    if _token and time.time() < _token_expires_at:
        return _token
    import requests
    resp = requests.post(
        f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    ).json()
    if resp.get("code") != 0:
        raise RuntimeError(f"获取飞书 token 失败: {resp.get('msg')}")
    _token = resp["tenant_access_token"]
    _token_expires_at = time.time() + resp.get("expire", 7200) - 300
    return _token

def _get_headers() -> dict:
    return {"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"}


def send_card(user_id: str, card: dict) -> bool:
    """向指定飞书用户发送消息卡片（open_id）"""
    import requests
    try:
        resp = requests.post(
            f"{FEISHU_BASE}/im/v1/messages",
            headers=_get_headers(),
            params={"receive_id_type": "open_id"},
            json={
                "receive_id": user_id,
                "msg_type": "interactive",
                "content": json.dumps(card, ensure_ascii=False),
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            logger.error(f"发送卡片失败: {data.get('msg')} (code={data.get('code')})")
            return False
        logger.info(f"✓ 卡片发送成功 → {user_id}")
        return True
    except Exception as e:
        logger.error(f"发送卡片请求失败: {e}")
        return False

def extract_spu(sku: str) -> str:
    if not sku:
        return ''
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    sku = re.sub(r'-+', '-', sku).strip('-')
    idx = sku.find('-')
    return sku[:idx] if idx > 0 else sku


def send_feishu_message(webhook: str, title: str, content: str) -> bool:
    """发送飞书卡片消息"""
    import json, urllib.request
    if not webhook:
        logger.warning(f"Webhook 未配置，跳过推送：{title}")
        return False

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue"
            },
            "elements": [
                {"tag": "markdown", "content": content}
            ]
        }
    }
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(webhook, data=data,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get('code') == 0 or result.get('StatusCode') == 0:
                logger.info(f"✓ 飞书推送成功：{title}")
                return True
            else:
                logger.warning(f"飞书推送返回异常：{result}")
                return False
    except Exception as e:
        logger.error(f"飞书推送失败：{e}")
        return False


# ────────────────────────────────────────────────────────────────────────────
# 数据读取
# ────────────────────────────────────────────────────────────────────────────

def read_order_suggest_summary(current_date: datetime):
    """读取建议下单量摘要，含各月分拆"""
    year = current_date.year
    month = current_date.month

    # 生成4个月标签
    month_labels = []
    for i in range(4):
        m = month + i
        y = year
        while m > 12:
            m -= 12
            y += 1
        month_labels.append(f"{str(y)[-2:]}年{m}月")

    with db_cursor() as cursor:
        # 总览
        cursor.execute("""
            SELECT
                COUNT(*) AS 总款数,
                SUM(CASE WHEN 建议下单量 > 0 THEN 1 ELSE 0 END) AS 需补单款数,
                SUM(建议下单量) AS 建议下单总量
            FROM `建议下单量表`
        """)
        overview = cursor.fetchone()

        # 各月建议下单合计
        monthly_suggest = {}
        monthly_op = {}
        for label in month_labels:
            sys_col = f"`{label}建议下单`"
            op_col  = f"`{label}运营预计`"
            try:
                cursor.execute(f"SELECT SUM({sys_col}) AS s, SUM({op_col}) AS o FROM `建议下单量表`")
                row = cursor.fetchone()
                monthly_suggest[label] = int(row['s'] or 0)
                monthly_op[label]      = int(row['o'] or 0)
            except Exception:
                monthly_suggest[label] = 0
                monthly_op[label]      = 0

        # 按面料类型
        cursor.execute("""
            SELECT 面料类型,
                   COUNT(*) AS 款数,
                   SUM(建议下单量) AS 建议下单量
            FROM `建议下单量表`
            WHERE 建议下单量 > 0
            GROUP BY 面料类型
        """)
        by_type = cursor.fetchall()

        # 缺口最大TOP5
        cursor.execute("""
            SELECT SPU, 店铺, 工厂, 建议下单量
            FROM `建议下单量表`
            WHERE 建议下单量 > 0
            ORDER BY 建议下单量 DESC
            LIMIT 5
        """)
        top5 = cursor.fetchall()

    return overview, by_type, top5, month_labels, monthly_suggest, monthly_op


def read_actual_order_by_month(current_date: datetime):
    """
    从采购单表读取未来4个月各月实际下单量（按创建时间月份，状态≠已作废）
    返回：{月份label: 实际下单量}, 总量
    """
    year = current_date.year
    month = current_date.month

    monthly_actual = {}
    total = 0

    try:
        with db_cursor() as cursor:
            for i in range(4):
                m = month + i
                y = year
                while m > 12:
                    m -= 12
                    y += 1
                label = f"{str(y)[-2:]}年{m}月"
                m_start = f"{y}-{m:02d}-01"
                m_end   = f"{y}-{m+1:02d}-01" if m < 12 else f"{y+1}-01-01"

                cursor.execute("""
                    SELECT SUM(实际数量) AS 总量
                    FROM `采购单`
                    WHERE 状态 != '已作废'
                      AND 创建时间 >= %s AND 创建时间 < %s
                      AND SKU IS NOT NULL AND SKU != ''
                """, (m_start, m_end))
                row = cursor.fetchone()
                val = int(row['总量'] or 0)
                monthly_actual[label] = val
                total += val
    except Exception as e:
        logger.warning(f"读取采购单失败: {e}")

    return monthly_actual, total


def read_fill_rate_stats(current_date: datetime):
    """
    填报完成率统计（按运营维度）：
    - 分母：预测对比表里系统预测>0 的唯一 SPU+店铺 组合（去重，不按月份重复计）
    - 分子：运营预计下单表里，该 SPU+店铺 在未来4个月内有任意一条记录 = 已填
    - 判断依据：有记录=填了（包括填0），无记录=未填

    返回：{运营: {'should': N, 'filled': M, 'rate': R}}
    """
    year = current_date.year
    month = current_date.month

    forecast_dates = []
    for i in range(4):
        m = month + i
        y = year
        while m > 12:
            m -= 12
            y += 1
        forecast_dates.append(f"{y}-{m:02d}-01")

    stats = defaultdict(lambda: {'should': 0, 'filled': 0})

    try:
        with db_cursor() as cursor:
            placeholders = ','.join(['%s'] * len(forecast_dates))

            # 分母：系统预测>0 的唯一 SPU+店铺（去重，不按月份）
            cursor.execute(f"""
                SELECT DISTINCT p.SPU, p.店铺, l.负责人 AS 运营
                FROM `预测对比表` p
                LEFT JOIN `listing` l
                    ON l.SKU COLLATE utf8mb4_unicode_ci
                       LIKE CONCAT(p.SPU COLLATE utf8mb4_unicode_ci, '%%')
                    AND l.店铺 COLLATE utf8mb4_unicode_ci
                        = p.店铺 COLLATE utf8mb4_unicode_ci
                WHERE p.系统预测销量 > 0
                  AND DATE_FORMAT(p.统计日期, '%%Y-%%m-%%d') IN ({placeholders})
            """, forecast_dates)
            should_rows = cursor.fetchall()

            # 分子：运营预计下单表里有任意记录的 SPU+店铺（在未来4个月内，有记录即算填了）
            cursor.execute(f"""
                SELECT DISTINCT
                    SKU, 店铺
                FROM `运营预计下单表`
                WHERE DATE_FORMAT(统计日期, '%%Y-%%m-%%d') IN ({placeholders})
            """, forecast_dates)
            filled_raw = cursor.fetchall()

        # 已填集合：(SPU, 店铺)
        filled_set = set()
        for row in filled_raw:
            spu = extract_spu((row['SKU'] or '').strip())
            shop = (row['店铺'] or '').strip()
            if spu and shop:
                filled_set.add((spu, shop))

        # 统计每个运营的分母和分子（已去重的SPU+店铺）
        seen = set()
        for row in should_rows:
            spu = (row['SPU'] or '').strip()
            shop = (row['店铺'] or '').strip()
            operator = (row['运营'] or '未知').strip()
            if not spu or not shop:
                continue
            key = (operator, spu, shop)
            if key in seen:
                continue
            seen.add(key)
            stats[operator]['should'] += 1
            if (spu, shop) in filled_set:
                stats[operator]['filled'] += 1

    except Exception as e:
        logger.warning(f"读取填报完成率失败: {e}")

    result = {}
    for operator, data in stats.items():
        should = data['should']
        filled = data['filled']
        rate = round(filled / should * 100, 1) if should > 0 else 0
        result[operator] = {'should': should, 'filled': filled, 'rate': rate}

    return result


def read_fabric_usage_summary():
    """读取面料用量摘要，含运营预估用量"""
    with db_cursor() as cursor:
        # 系统预计用量（来自面料预计用量表）
        cursor.execute("""
            SELECT f.面料, f.SPU数量, f.建议下单量合计,
                   f.`单件用量(米)`, f.`预计用量(米)` AS 系统预计用量
            FROM `面料预计用量表` f
            ORDER BY f.`预计用量(米)` DESC
        """)
        rows = cursor.fetchall()

        # 运营预估用量：运营预计合计 × 单件用量（从建议下单量表关联面料核价表）
        cursor.execute("""
            SELECT k.面料,
                   SUM(b.运营预计合计) AS 运营预计下单合计,
                   MAX(k.单件用量)     AS 单件用量
            FROM `建议下单量表` b
            JOIN `面料核价表` k ON k.SPU = b.SPU
            WHERE b.面料类型 = '定制面料'
            GROUP BY k.面料
        """)
        op_map = {
            r['面料']: int(r['运营预计下单合计'] or 0) * float(r['单件用量'] or 0)
            for r in cursor.fetchall()
        }

    total_usage = sum(float(r['系统预计用量'] or 0) for r in rows)
    total_order = sum(int(r['建议下单量合计'] or 0) for r in rows)
    return rows, total_usage, total_order, op_map


# ────────────────────────────────────────────────────────────────────────────
# 消息组装
# ────────────────────────────────────────────────────────────────────────────

def build_production_card(
    current_date: datetime,
    overview, by_type, top5,
    month_labels, monthly_suggest, monthly_op,
    monthly_actual: dict,
) -> dict:
    """组装生产卡片"""
    month_label = f"{current_date.year}年{current_date.month}月"

    # 各月明细：系统建议 / 运营预计 / 实际已下单
    monthly_lines = []
    for label in month_labels:
        sys_v = monthly_suggest.get(label, 0)
        op_v  = monthly_op.get(label, 0)
        act_v = monthly_actual.get(label, 0)
        monthly_lines.append(
            f"**{label}**　系统建议 {sys_v:,}　运营预计 {op_v:,}　实际已下单 {act_v:,}"
        )
    monthly_text = "\n".join(monthly_lines)

    # 按面料类型分月
    type_monthly_lines = []
    for label in month_labels:
        sys_col = f"`{label}建议下单`"
        try:
            with db_cursor() as cursor:
                cursor.execute(f"""
                    SELECT 面料类型, SUM({sys_col}) AS 建议量
                    FROM `建议下单量表`
                    GROUP BY 面料类型
                """)
                rows = cursor.fetchall()
            parts = "　".join(
                f"{r['面料类型']} {int(r['建议量'] or 0):,} 件"
                for r in rows
            )
            type_monthly_lines.append(f"**{label}**　{parts}")
        except Exception:
            type_monthly_lines.append(f"**{label}**　暂无数据")
    type_monthly_text = "\n".join(type_monthly_lines)

    # TOP5 分月展示
    top5_monthly_lines = []
    for label in month_labels:
        sys_col = f"`{label}建议下单`"
        try:
            with db_cursor() as cursor:
                cursor.execute(f"""
                    SELECT SPU, 店铺, 工厂, {sys_col} AS 建议量
                    FROM `建议下单量表`
                    WHERE {sys_col} > 0
                    ORDER BY {sys_col} DESC
                    LIMIT 5
                """)
                rows = cursor.fetchall()
            if rows:
                top5_monthly_lines.append(f"**{label}**")
                for i, r in enumerate(rows, 1):
                    top5_monthly_lines.append(
                        f"  {i}. **{r['SPU']}** · {r['店铺']}"
                        f"（{r['工厂'] or '未记录'}）：{int(r['建议量'] or 0):,} 件"
                    )
        except Exception:
            top5_monthly_lines.append(f"**{label}**　暂无数据")
    top5_monthly_text = "\n".join(top5_monthly_lines) or "暂无数据"

    return {
        "header": {
            "title": {"tag": "plain_text", "content": f"📦 {month_label} 采购建议 · 生产"},
            "template": "blue"
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"**总览**\n"
                f"总款数：{int(overview['总款数']):,} 款　"
                f"需补单：{int(overview['需补单款数']):,} 款\n"
                f"建议下单总量：**{int(overview['建议下单总量'] or 0):,} 件**"
            }},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**各月明细**\n{monthly_text}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**按面料类型（分月）**\n{type_monthly_text}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**各月缺口 TOP5**\n{top5_monthly_text}"}},
            {"tag": "note", "elements": [
                {"tag": "plain_text", "content": "详细数据请查看飞书「建议下单量表」"}
            ]},
        ]
    }


def build_fabric_card(
    current_date: datetime,
    fabric_rows, total_usage: float, total_order: int,
    op_map: dict,
) -> dict:
    """
    组装面料卡片（产品经理）
    TOP10 面料表格：系统预计用量 / 运营预估用量 / 现有库存（待接入）
    不分月：面料采购按总量与供应商谈判，无需月度拆分
    """
    month_label = f"{current_date.year}年{current_date.month}月"

    # markdown 表格
    table_header = "| 面料 | 系统预计(米) | 运营预估(米) | 现有库存 |"
    table_sep    = "|------|------------|------------|--------|"
    table_rows = []
    for r in fabric_rows[:10]:
        fabric = r['面料']
        sys_usage = float(r['系统预计用量'] or 0)
        op_usage  = op_map.get(fabric, 0.0)
        table_rows.append(
            f"| {fabric} | {sys_usage:,.0f} | {op_usage:,.0f} | 待接入 |"
        )
    table_text = "\n".join([table_header, table_sep] + table_rows)

    return {
        "header": {
            "title": {"tag": "plain_text", "content": f"🧵 {month_label} 定制面料用量 · 面料"},
            "template": "green"
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"**总览**\n"
                f"定制面料种类：{len(fabric_rows)} 种\n"
                f"总建议下单量：{total_order:,} 件\n"
                f"系统预计总用量：**{total_usage:,.0f} 米**"
            }},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**TOP10 面料用量**\n{table_text}"}},
            {"tag": "note", "elements": [
                {"tag": "plain_text", "content": "详细数据请查看飞书「面料预计用量表」"}
            ]},
        ]
    }


# ────────────────────────────────────────────────────────────────────────────
# 主函数
# ────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("采购建议月度摘要推送")
    logger.info("=" * 60)

    current_date = datetime.now()

    overview, by_type, top5, month_labels, monthly_suggest, monthly_op = \
        read_order_suggest_summary(current_date)
    monthly_actual, _ = read_actual_order_by_month(current_date)
    fabric_rows, total_usage, total_order, op_map = read_fabric_usage_summary()

    prod_card = build_production_card(
        current_date, overview, by_type, top5,
        month_labels, monthly_suggest, monthly_op,
        monthly_actual,
    )
    send_card(RECEIVER_PRODUCTION, prod_card)

    fabric_card = build_fabric_card(
        current_date, fabric_rows, total_usage, total_order, op_map,
    )
    send_card(RECEIVER_PRODUCT, fabric_card)

    logger.info("推送完成")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
