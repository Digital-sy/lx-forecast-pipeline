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

def read_order_suggest_summary():
    """
    从建议下单量表读取摘要数据：
    - 总款数、需补单款数
    - 定制/现货分类统计
    - 建议下单量TOP5
    """
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

    return overview, by_type, top5


def read_actual_order_this_month(current_date: datetime):
    """
    从采购单表读取本月实际下单量（按创建时间在本月内，状态≠已作废）
    返回：{(SPU, 店铺): 实际数量}  以及总量
    """
    year = current_date.year
    month = current_date.month
    month_start = f"{year}-{month:02d}-01"
    if month == 12:
        month_end = f"{year+1}-01-01"
    else:
        month_end = f"{year}-{month+1:02d}-01"

    actual_map = defaultdict(int)
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT SKU, 店铺, SUM(实际数量) AS 总量
                FROM `采购单`
                WHERE 状态 != '已作废'
                  AND 创建时间 >= %s AND 创建时间 < %s
                  AND SKU IS NOT NULL AND SKU != ''
                GROUP BY SKU, 店铺
            """, (month_start, month_end))
            for row in cursor.fetchall():
                spu = extract_spu((row['SKU'] or '').strip())
                shop = (row['店铺'] or '').strip()
                if spu and shop:
                    actual_map[(spu, shop)] += int(row['总量'] or 0)
    except Exception as e:
        logger.warning(f"读取采购单失败: {e}")

    total = sum(actual_map.values())
    return actual_map, total


def read_fill_rate_stats(current_date: datetime):
    """
    填报完成率统计（按运营维度）：
    - 分母：预测对比表里系统预测 > 0 的 SPU+店铺+月 组合（本月起未来4月）
    - 分子：运营预计下单表里有记录的 SPU+店铺+月（无论值是多少，有记录=填了）
    - 判断依据：记录存在 = 填了（包括填0），不存在 = 未填

    返回：{运营: {'should': N, 'filled': M, 'rate': R}}
    """
    year = current_date.year
    month = current_date.month

    # 生成未来4个月的统计日期
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
            # 分母：系统预测>0 的 SPU+店铺+月（从预测对比表）
            placeholders = ','.join(['%s'] * len(forecast_dates))
            cursor.execute(f"""
                SELECT p.SPU, p.店铺, p.统计日期, l.负责人 AS 运营
                FROM `预测对比表` p
                LEFT JOIN `listing` l
                    ON l.SKU LIKE CONCAT(p.SPU, '%%')
                    AND l.店铺 = p.店铺
                WHERE p.系统预测销量 > 0
                  AND DATE_FORMAT(p.统计日期, '%%Y-%%m-%%d') IN ({placeholders})
            """, forecast_dates)
            should_rows = cursor.fetchall()

            # 分子：运营预计下单表里有记录的 SPU+店铺+月
            cursor.execute(f"""
                SELECT DISTINCT
                    s.SPU,
                    o.店铺,
                    DATE_FORMAT(o.统计日期, '%%Y-%%m-%%d') AS 统计日期
                FROM `运营预计下单表` o
                JOIN (
                    SELECT SKU, SPU FROM `运营预计下单表`
                    WHERE SPU IS NOT NULL AND SPU != ''
                ) s ON o.SKU = s.SKU
                WHERE DATE_FORMAT(o.统计日期, '%%Y-%%m-%%d') IN ({placeholders})
            """, forecast_dates)

            # 简化：直接按SKU匹配，转成SPU+店铺+月集合
            cursor.execute(f"""
                SELECT DISTINCT
                    SKU, 店铺,
                    DATE_FORMAT(统计日期, '%%Y-%%m-%%d') AS 月份
                FROM `运营预计下单表`
                WHERE DATE_FORMAT(统计日期, '%%Y-%%m-%%d') IN ({placeholders})
            """, forecast_dates)
            filled_raw = cursor.fetchall()

        # 已填集合：(SPU, 店铺, 月份)
        filled_set = set()
        for row in filled_raw:
            spu = extract_spu((row['SKU'] or '').strip())
            shop = (row['店铺'] or '').strip()
            if spu and shop:
                filled_set.add((spu, shop, row['月份']))

        # 统计每个运营的分母和分子
        for row in should_rows:
            spu = (row['SPU'] or '').strip()
            shop = (row['店铺'] or '').strip()
            date_str = str(row['统计日期'])[:10]
            operator = (row['运营'] or '未知').strip()

            if not spu or not shop:
                continue

            stats[operator]['should'] += 1
            if (spu, shop, date_str) in filled_set:
                stats[operator]['filled'] += 1

    except Exception as e:
        logger.warning(f"读取填报完成率失败: {e}")

    # 计算完成率
    result = {}
    for operator, data in stats.items():
        should = data['should']
        filled = data['filled']
        rate = round(filled / should * 100, 1) if should > 0 else 0
        result[operator] = {'should': should, 'filled': filled, 'rate': rate}

    return result


def read_fabric_usage_summary():
    """读取面料用量摘要"""
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT 面料, SPU数量, 建议下单量合计, `单件用量(米)`, `预计用量(米)`
            FROM `面料预计用量表`
            ORDER BY `预计用量(米)` DESC
        """)
        rows = cursor.fetchall()

    total_usage = sum(float(r['预计用量(米)'] or 0) for r in rows)
    total_order = sum(int(r['建议下单量合计'] or 0) for r in rows)
    return rows, total_usage, total_order


# ────────────────────────────────────────────────────────────────────────────
# 消息组装
# ────────────────────────────────────────────────────────────────────────────

def build_production_card(
    current_date: datetime,
    overview, by_type, top5,
    actual_total: int,
    fill_stats: dict,
) -> dict:
    """组装生产经理飞书卡片"""
    month_label = f"{current_date.year}年{current_date.month}月"

    # 按面料类型
    type_text = "\n".join(
        f"· {r['面料类型']}：{r['款数']} 款，{int(r['建议下单量'] or 0):,} 件"
        for r in by_type
    ) or "暂无数据"

    # TOP5
    top5_text = "\n".join(
        f"{i}. **{r['SPU']}** · {r['店铺']}"
        f"（{r['工厂'] or '工厂未记录'}）：{int(r['建议下单量']):,} 件"
        for i, r in enumerate(top5, 1)
    ) or "暂无数据"

    # 填报完成率
    fill_lines = []
    for operator, data in sorted(fill_stats.items(), key=lambda x: -x[1]['rate']):
        emoji = "✅" if data['rate'] >= 80 else "⚠️" if data['rate'] >= 50 else "❌"
        fill_lines.append(
            f"{emoji} **{operator}**：{data['filled']}/{data['should']} 个SPU（{data['rate']}%）"
        )
    fill_text = "\n".join(fill_lines) or "暂无数据"

    return {
        "header": {
            "title": {"tag": "plain_text", "content": f"📦 {month_label} 采购建议摘要 · 生产经理"},
            "template": "blue"
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"**总览**\n"
                f"总款数：{int(overview['总款数']):,} 款　"
                f"需补单：{int(overview['需补单款数']):,} 款\n"
                f"建议下单总量：**{int(overview['建议下单总量'] or 0):,} 件**\n"
                f"本月实际已下单：{actual_total:,} 件"
            }},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**按面料类型**\n{type_text}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**缺口最大 TOP5**\n{top5_text}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**运营填报完成率**\n{fill_text}"}},
            {"tag": "note", "elements": [
                {"tag": "plain_text", "content": "详细数据请查看飞书「建议下单量表」"}
            ]},
        ]
    }


def build_product_card(
    current_date: datetime,
    fabric_rows, total_usage: float, total_order: int,
) -> dict:
    """组装产品经理飞书卡片"""
    month_label = f"{current_date.year}年{current_date.month}月"

    top5_text = "\n".join(
        f"· **{r['面料']}**：{float(r['预计用量(米)'] or 0):,.0f} 米"
        f"（{int(r['建议下单量合计'] or 0):,} 件 × {float(r['单件用量(米)'] or 0):.2f} 米/件）"
        for r in fabric_rows[:5]
    ) or "暂无数据"

    return {
        "header": {
            "title": {"tag": "plain_text", "content": f"🧵 {month_label} 定制面料用量预估 · 产品经理"},
            "template": "green"
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"**总览**\n"
                f"涉及面料种类：{len(fabric_rows)} 种\n"
                f"总建议下单量：{total_order:,} 件\n"
                f"总预计用量：**{total_usage:,.0f} 米**"
            }},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**用量 TOP5 面料**\n{top5_text}"}},
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

    # 读取数据
    overview, by_type, top5 = read_order_suggest_summary()
    _, actual_total = read_actual_order_this_month(current_date)
    fill_stats = read_fill_rate_stats(current_date)
    fabric_rows, total_usage, total_order = read_fabric_usage_summary()

    # 组装卡片并发送
    prod_card = build_production_card(
        current_date, overview, by_type, top5, actual_total, fill_stats
    )
    send_card(RECEIVER_PRODUCTION, prod_card)

    fabric_card = build_product_card(
        current_date, fabric_rows, total_usage, total_order
    )
    send_card(RECEIVER_PRODUCT, fabric_card)

    logger.info("推送完成")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
