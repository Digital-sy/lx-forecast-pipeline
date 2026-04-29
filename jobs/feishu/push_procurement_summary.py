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
            col = f"`{label}建议下单`"
            op_col = f"`{label}运营预计`"
            try:
                cursor.execute(f"SELECT SUM({col}) AS v FROM `建议下单量表`")
                r = cursor.fetchone()
                monthly_suggest[label] = int(r['v'] or 0)
            except Exception:
                monthly_suggest[label] = 0
            try:
                cursor.execute(f"SELECT SUM({op_col}) AS v FROM `建议下单量表`")
                r = cursor.fetchone()
                monthly_op[label] = int(r['v'] or 0)
            except Exception:
                monthly_op[label] = 0

        # 按面料类型
        cursor.execute("""
            SELECT 面料类型, SUM(建议下单量) AS 建议量
            FROM `建议下单量表`
            GROUP BY 面料类型
        """)
        by_type = cursor.fetchall()

        # TOP5
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
    """读取实际采购下单量（按月汇总）"""
    year = current_date.year
    month = current_date.month

    month_labels = []
    month_dates = []
    for i in range(4):
        m = month + i
        y = year
        while m > 12:
            m -= 12
            y += 1
        month_labels.append(f"{str(y)[-2:]}年{m}月")
        month_dates.append(f"{y}-{m:02d}")

    monthly_actual = {label: 0 for label in month_labels}

    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='采购单'
            """)
            if not cursor.fetchone().get('cnt', 0):
                return monthly_actual, month_labels

            cursor.execute("""
                SELECT DATE_FORMAT(创建时间, '%Y-%m') AS 月份, SUM(实际数量) AS 总量
                FROM `采购单`
                WHERE 实际数量 > 0
                GROUP BY DATE_FORMAT(创建时间, '%Y-%m')
            """)
            for row in cursor.fetchall():
                ym = row['月份']
                qty = int(row['总量'] or 0)
                for label, date_prefix in zip(month_labels, month_dates):
                    if ym == date_prefix:
                        monthly_actual[label] += qty
    except Exception as e:
        logger.warning(f"读取实际采购数据失败: {e}")

    return monthly_actual, month_labels


def read_fabric_usage_summary():
    """读取面料用量摘要，含运营预估用量和现有库存"""
    with db_cursor() as cursor:
        # 系统预计用量
        cursor.execute("""
            SELECT 面料, SPU数量, 建议下单量合计,
                   `单件用量(米)`, `预计用量(米)` AS 系统预计用量
            FROM `面料预计用量表`
            ORDER BY `预计用量(米)` DESC
        """)
        rows = cursor.fetchall()

        # 运营预估用量：运营预计合计 × 单件用量
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

        # 现有库存：库存量/米 + 待到货量/米，按面料名聚合（取最新统计日期）
        cursor.execute("""
            SELECT 面料,
                   SUM(`库存量/米`)   AS 库存米,
                   SUM(`待到货量/米`) AS 待到货米
            FROM `面料预估表`
            WHERE 统计日期 = (
                SELECT MAX(统计日期) FROM `面料预估表`
            )
            GROUP BY 面料
        """)
        stock_map = {
            r['面料']: float(r['库存米'] or 0) + float(r['待到货米'] or 0)
            for r in cursor.fetchall()
        }

    total_usage = sum(float(r['系统预计用量'] or 0) for r in rows)
    total_order = sum(int(r['建议下单量合计'] or 0) for r in rows)
    return rows, total_usage, total_order, op_map, stock_map


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
    op_map: dict, stock_map: dict,
) -> dict:
    """
    组装面料卡片
    TOP10 面料表格：系统预计用量 / 运营预估用量 / 现有库存（含待到货）
    使用飞书原生 table 元素，视觉更清晰
    """
    month_label = f"{current_date.year}年{current_date.month}月"

    # 构建飞书原生 table 行数据
    table_rows = []
    for r in fabric_rows[:10]:
        fabric    = r['面料']
        sys_usage = float(r['系统预计用量'] or 0)
        op_usage  = op_map.get(fabric, 0.0)
        stock     = stock_map.get(fabric, 0.0)
        table_rows.append({
            "面料":     fabric,
            "系统预计": f"{sys_usage:,.0f}",
            "运营预估": f"{op_usage:,.0f}",
            "现有库存": f"{stock:,.0f}",
        })

    table_element = {
        "tag": "table",
        "page_size": 10,
        "row_height": "low",
        "header_style": {
            "text_align": "left",
            "text_size": "normal",
            "background_color": "grey",
            "text_color": "default",
            "bold": True,
            "lines": 1,
        },
        "columns": [
            {"name": "面料",     "display_name": "面料",        "width": "auto", "horizontal_align": "left"},
            {"name": "系统预计", "display_name": "系统预计(米)", "width": "auto", "horizontal_align": "right"},
            {"name": "运营预估", "display_name": "运营预估(米)", "width": "auto", "horizontal_align": "right"},
            {"name": "现有库存", "display_name": "现有库存(米)", "width": "auto", "horizontal_align": "right"},
        ],
        "rows": table_rows,
    }

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
            {"tag": "div", "text": {"tag": "lark_md", "content": "**TOP10 面料用量**（单位：米）"}},
            table_element,
            {"tag": "note", "elements": [
                {"tag": "plain_text", "content": "现有库存 = 库存量/米 + 待到货量/米　详细数据请查看飞书「面料预计用量表」"}
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
    fabric_rows, total_usage, total_order, op_map, stock_map = read_fabric_usage_summary()

    prod_card = build_production_card(
        current_date, overview, by_type, top5,
        month_labels, monthly_suggest, monthly_op,
        monthly_actual,
    )
    send_card(RECEIVER_PRODUCTION, prod_card)

    fabric_card = build_fabric_card(
        current_date, fabric_rows, total_usage, total_order, op_map, stock_map,
    )
    send_card(RECEIVER_PRODUCT, fabric_card)

    logger.info("推送完成")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
