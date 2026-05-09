#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购建议月度摘要推送
每月8日8:00自动运行，推送给生产负责人和产品/面料负责人。

推送内容：
  1. 生产负责人：采购建议摘要
  2. 生产负责人：填报情况-数据（月份 -> 店铺 -> 运营）
  3. 产品/面料负责人：定制面料预计用量摘要

Crontab 示例：
  0 8 8 * * /opt/apps/pythondata/venv/bin/python -m jobs.feishu.push_procurement_summary >> /opt/apps/pythondata/logs/push_procurement_summary.log 2>&1
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger("push_procurement_summary")

try:
    from common.config import settings
    FEISHU_APP_ID = settings.FEISHU_APP_ID
    FEISHU_APP_SECRET = settings.FEISHU_APP_SECRET
except Exception:
    FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
    FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")

FEISHU_BASE = "https://open.feishu.cn/open-apis"

# 收件人 open_id；当前两个都指向刘宗霖，如后续拆分负责人，只改这里。
RECEIVER_PRODUCTION = "ou_45d24eddffa044503caf29d6c8a2e003"  # 生产负责人
RECEIVER_PRODUCT = "ou_45d24eddffa044503caf29d6c8a2e003"     # 产品/面料负责人

_token = ""
_token_expires_at = 0.0


def _get_token() -> str:
    global _token, _token_expires_at
    if _token and time.time() < _token_expires_at:
        return _token

    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise RuntimeError("FEISHU_APP_ID / FEISHU_APP_SECRET 未配置")

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


def _get_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"}


def send_card(user_id: str, card: Dict[str, Any]) -> bool:
    """向指定飞书用户发送消息卡片（open_id）。"""
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
        if resp.status_code >= 400:
            logger.error(f"飞书发送失败状态码: {resp.status_code}")
            logger.error(f"飞书发送失败响应体: {resp.text}")

        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            logger.error(f"发送卡片失败: {data.get('msg')} (code={data.get('code')})")
            return False
        logger.info(f"✓ 卡片发送成功 → {user_id}")
        return True
    except Exception as e:
        logger.error(f"发送卡片请求失败: {e}", exc_info=True)
        return False


def extract_spu(sku: str) -> str:
    """从 SKU/MSKU 提取 SPU，兼容 4PSC/1PCS 等组合装标记。"""
    if not sku:
        return ""
    sku = re.sub(r"\d+(?:PSC|PCS)", "", sku, flags=re.IGNORECASE)
    sku = re.sub(r"-+", "-", sku).strip("-")
    idx = sku.find("-")
    return sku[:idx] if idx > 0 else sku


def month_sort_key(label: str) -> int:
    m = re.match(r"(\d{2})年(\d{1,2})月", label or "")
    if not m:
        return 0
    return (2000 + int(m.group(1))) * 100 + int(m.group(2))


def month_label_to_year_month(label: str) -> Tuple[int, int, str, str]:
    """26年5月 -> (2026, 5, 2026-05, 2026-05-01)。"""
    m = re.match(r"(\d{2})年(\d{1,2})月", label or "")
    if not m:
        raise ValueError(f"无法解析月份标签: {label}")
    year = 2000 + int(m.group(1))
    month = int(m.group(2))
    return year, month, f"{year}-{month:02d}", f"{year}-{month:02d}-01"


def make_month_labels(current_date: datetime, months: int = 4) -> List[str]:
    labels = []
    for i in range(months):
        y = current_date.year
        m = current_date.month + i
        while m > 12:
            m -= 12
            y += 1
        labels.append(f"{str(y)[-2:]}年{m}月")
    return labels


def fmt_int(value: Any) -> str:
    try:
        return f"{int(float(value or 0)):,}"
    except Exception:
        return "0"


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
        """,
        (table_name,),
    )
    return bool((cursor.fetchone() or {}).get("cnt", 0))


def read_fill_status_data_updated_at() -> str:
    """
    读取填报情况卡片引用数据的真实最近更新时间。

    严格规则：
    - 只读取 `运营预计下单表`.`飞书同步时间`；
    - 如果字段不存在或没有值，返回“未记录”；
    - 不允许退化为卡片生成时间。
    """
    try:
        with db_cursor() as cursor:
            if not table_exists(cursor, "运营预计下单表"):
                logger.warning("运营预计下单表不存在，填报情况数据更新时间返回“未记录”")
                return "未记录"

            cursor.execute("""
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '运营预计下单表'
                  AND COLUMN_NAME = '飞书同步时间'
            """)
            exists = (cursor.fetchone() or {}).get("cnt", 0)

            if not exists:
                logger.warning("运营预计下单表缺少字段“飞书同步时间”，填报情况数据更新时间返回“未记录”")
                return "未记录"

            cursor.execute("""
                SELECT MAX(`飞书同步时间`) AS latest_sync_time
                FROM `运营预计下单表`
            """)
            latest = (cursor.fetchone() or {}).get("latest_sync_time")

            if not latest:
                logger.warning("运营预计下单表.飞书同步时间没有有效值，填报情况数据更新时间返回“未记录”")
                return "未记录"

            return latest.strftime("%Y-%m-%d %H:%M:%S") if hasattr(latest, "strftime") else str(latest)

    except Exception as e:
        logger.warning(f"读取填报情况数据更新时间失败: {e}", exc_info=True)
        return "未记录"


# ────────────────────────────────────────────────────────────────────────────
# 数据读取
# ────────────────────────────────────────────────────────────────────────────


def read_order_suggest_summary(current_date: datetime):
    """读取建议下单量摘要，含各月系统建议/运营预计。"""
    month_labels = make_month_labels(current_date, 4)

    with db_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS 总款数,
                SUM(CASE WHEN 建议下单量 > 0 THEN 1 ELSE 0 END) AS 需补单款数,
                SUM(建议下单量) AS 建议下单总量
            FROM `建议下单量表`
            """
        )
        overview = cursor.fetchone() or {"总款数": 0, "需补单款数": 0, "建议下单总量": 0}

        monthly_suggest: Dict[str, int] = {}
        monthly_op: Dict[str, int] = {}
        for label in month_labels:
            suggest_col = f"`{label}建议下单`"
            op_col = f"`{label}运营预计`"
            try:
                cursor.execute(f"SELECT SUM({suggest_col}) AS v FROM `建议下单量表`")
                monthly_suggest[label] = int((cursor.fetchone() or {}).get("v") or 0)
            except Exception as e:
                logger.warning(f"读取 {label}建议下单 失败: {e}")
                monthly_suggest[label] = 0

            try:
                cursor.execute(f"SELECT SUM({op_col}) AS v FROM `建议下单量表`")
                monthly_op[label] = int((cursor.fetchone() or {}).get("v") or 0)
            except Exception as e:
                logger.warning(f"读取 {label}运营预计 失败: {e}")
                monthly_op[label] = 0

        cursor.execute(
            """
            SELECT 面料类型, SUM(建议下单量) AS 建议量
            FROM `建议下单量表`
            GROUP BY 面料类型
            """
        )
        by_type = cursor.fetchall()

        cursor.execute(
            """
            SELECT SPU, 店铺, 工厂, 建议下单量
            FROM `建议下单量表`
            WHERE 建议下单量 > 0
            ORDER BY 建议下单量 DESC
            LIMIT 5
            """
        )
        top5 = cursor.fetchall()

    return overview, by_type, top5, month_labels, monthly_suggest, monthly_op


def read_actual_order_by_month(current_date: datetime):
    """读取实际采购下单量（按采购单创建月份汇总）。"""
    month_labels = make_month_labels(current_date, 4)
    month_dates = [month_label_to_year_month(label)[2] for label in month_labels]
    monthly_actual = {label: 0 for label in month_labels}

    try:
        with db_cursor() as cursor:
            if not table_exists(cursor, "采购单"):
                return monthly_actual, month_labels
            cursor.execute(
                """
                SELECT DATE_FORMAT(创建时间, '%%Y-%%m') AS 月份, SUM(实际数量) AS 总量
                FROM `采购单`
                WHERE 实际数量 > 0
                GROUP BY DATE_FORMAT(创建时间, '%%Y-%%m')
                """
            )
            for row in cursor.fetchall():
                ym = row.get("月份")
                qty = int(row.get("总量") or 0)
                for label, date_prefix in zip(month_labels, month_dates):
                    if ym == date_prefix:
                        monthly_actual[label] += qty
    except Exception as e:
        logger.warning(f"读取实际采购数据失败: {e}", exc_info=True)

    return monthly_actual, month_labels


def read_actual_order_by_shop_month(month_labels: Iterable[str]) -> Dict[Tuple[str, str], int]:
    """读取实际采购下单量：{(月份标签, 店铺): 实际数量}。"""
    result: Dict[Tuple[str, str], int] = defaultdict(int)
    ym_to_label = {}
    for label in month_labels:
        _, _, ym, _ = month_label_to_year_month(label)
        ym_to_label[ym] = label

    if not ym_to_label:
        return result

    placeholders = ",".join(["%s"] * len(ym_to_label))
    try:
        with db_cursor() as cursor:
            if not table_exists(cursor, "采购单"):
                return result
            cursor.execute(
                f"""
                SELECT DATE_FORMAT(创建时间, '%%Y-%%m') AS 月份,
                       店铺,
                       SUM(实际数量) AS 实际已下单
                FROM `采购单`
                WHERE 实际数量 > 0
                  AND DATE_FORMAT(创建时间, '%%Y-%%m') IN ({placeholders})
                GROUP BY DATE_FORMAT(创建时间, '%%Y-%%m'), 店铺
                """,
                tuple(ym_to_label.keys()),
            )
            for row in cursor.fetchall():
                label = ym_to_label.get(row.get("月份"))
                shop = (row.get("店铺") or "").strip()
                if label and shop:
                    result[(label, shop)] += int(row.get("实际已下单") or 0)
    except Exception as e:
        logger.warning(f"读取店铺实际采购数据失败: {e}", exc_info=True)
    return result


def read_fabric_usage_summary():
    """读取面料用量摘要，含运营预估用量和现有库存。"""
    with db_cursor() as cursor:
        cursor.execute(
            """
            SELECT 面料, SPU数量, 建议下单量合计,
                   `单件用量(米)`, `预计用量(米)` AS 系统预计用量
            FROM `面料预计用量表`
            ORDER BY `预计用量(米)` DESC
            """
        )
        rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT k.面料,
                   SUM(b.运营预计合计) AS 运营预计下单合计,
                   MAX(k.单件用量) AS 单件用量
            FROM `建议下单量表` b
            JOIN `面料核价表` k ON k.SPU = b.SPU
            WHERE b.面料类型 = '定制面料'
            GROUP BY k.面料
            """
        )
        op_map = {
            r["面料"]: int(r["运营预计下单合计"] or 0) * float(r["单件用量"] or 0)
            for r in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT 面料,
                   SUM(`库存量/米`) AS 库存米,
                   SUM(`待到货量/米`) AS 待到货米
            FROM `面料预估表`
            WHERE 统计日期 = (SELECT MAX(统计日期) FROM `面料预估表`)
            GROUP BY 面料
            """
        )
        stock_map = {
            r["面料"]: float(r["库存米"] or 0) + float(r["待到货米"] or 0)
            for r in cursor.fetchall()
        }

    total_usage = sum(float(r["系统预计用量"] or 0) for r in rows)
    total_order = sum(int(r["建议下单量合计"] or 0) for r in rows)
    return rows, total_usage, total_order, op_map, stock_map


def read_fill_status_rows(month_labels: List[str]) -> List[Dict[str, Any]]:
    """
    读取“填报情况-数据”卡片数据。

    层级：月份汇总 -> 店铺汇总 -> 运营明细。
    - 系统建议：来自 `建议下单量表` 的各月建议下单列。
    - 运营预计：月份/店铺汇总来自 `建议下单量表`，运营明细来自 `运营预计下单表`。
    - 本卡片不展示实际已下单。
    """
    rows: List[Dict[str, Any]] = []

    with db_cursor() as cursor:
        for label in month_labels:
            suggest_col = f"`{label}建议下单`"
            op_col = f"`{label}运营预计`"

            year, month, _ym, stat_date = month_label_to_year_month(label)
            full_month_label = f"{year}年{month}月"

            # 1. 月份总计
            cursor.execute(
                f"""
                SELECT
                    SUM({suggest_col}) AS 系统建议,
                    SUM({op_col}) AS 运营预计
                FROM `建议下单量表`
                """
            )
            total = cursor.fetchone() or {}

            rows.append({
                "层级": "month",
                "月份": full_month_label,
                "店铺": "",
                "运营": "",
                "系统建议": int(total.get("系统建议") or 0),
                "运营预计": int(total.get("运营预计") or 0),
            })

            # 2. 店铺汇总
            cursor.execute(
                f"""
                SELECT
                    店铺,
                    SUM({suggest_col}) AS 系统建议,
                    SUM({op_col}) AS 运营预计
                FROM `建议下单量表`
                GROUP BY 店铺
                ORDER BY 店铺
                """
            )
            shop_rows = cursor.fetchall()

            for shop_row in shop_rows:
                shop = (shop_row.get("店铺") or "").strip()
                if not shop:
                    continue

                rows.append({
                    "层级": "shop",
                    "月份": "",
                    "店铺": shop,
                    "运营": "",
                    "系统建议": int(shop_row.get("系统建议") or 0),
                    "运营预计": int(shop_row.get("运营预计") or 0),
                })

                # 3. 运营明细：先按运营预计下单表聚合，保证运营行一定出来
                cursor.execute(
                    """
                    SELECT
                        COALESCE(NULLIF(TRIM(运营), ''), '未记录') AS 运营,
                        SUM(预计下单量) AS 运营预计
                    FROM `运营预计下单表`
                    WHERE TRIM(店铺) COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
                      AND DATE(统计日期) = %s
                    GROUP BY COALESCE(NULLIF(TRIM(运营), ''), '未记录')
                    ORDER BY 运营
                    """,
                    (shop, stat_date),
                )
                op_rows = cursor.fetchall()

                for op_row in op_rows:
                    operator = (op_row.get("运营") or "未记录").strip() or "未记录"

                    # 用该运营负责的 DISTINCT SPU+店铺 去建议下单量表取系统建议
                    # 显式 COLLATE，避免 utf8mb4_0900_ai_ci / utf8mb4_unicode_ci 混用报错
                    system_suggest = 0
                    try:
                        cursor.execute(
                            f"""
                            SELECT SUM(COALESCE(s.{suggest_col}, 0)) AS 系统建议
                            FROM (
                                SELECT DISTINCT SPU, 店铺
                                FROM `运营预计下单表`
                                WHERE TRIM(店铺) COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
                                  AND DATE(统计日期) = %s
                                  AND COALESCE(NULLIF(TRIM(运营), ''), '未记录') COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
                            ) o
                            LEFT JOIN `建议下单量表` s
                              ON TRIM(s.SPU) COLLATE utf8mb4_unicode_ci = TRIM(o.SPU) COLLATE utf8mb4_unicode_ci
                             AND TRIM(s.店铺) COLLATE utf8mb4_unicode_ci = TRIM(o.店铺) COLLATE utf8mb4_unicode_ci
                            """,
                            (shop, stat_date, operator),
                        )
                        system_suggest = int((cursor.fetchone() or {}).get("系统建议") or 0)
                    except Exception as e:
                        logger.warning(f"读取运营系统建议失败: {label} {shop} {operator}: {e}")
                        system_suggest = 0

                    rows.append({
                        "层级": "operator",
                        "月份": "",
                        "店铺": shop,
                        "运营": operator,
                        "系统建议": system_suggest,
                        "运营预计": int(op_row.get("运营预计") or 0),
                    })

    return rows


# ────────────────────────────────────────────────────────────────────────────
# 消息组装
# ────────────────────────────────────────────────────────────────────────────


def build_production_card(
    current_date: datetime,
    overview,
    by_type,
    top5,
    month_labels,
    monthly_suggest,
    monthly_op,
    monthly_actual: Dict[str, int],
) -> Dict[str, Any]:
    """组装生产负责人采购建议摘要卡片。"""
    month_label = f"{current_date.year}年{current_date.month}月"

    monthly_lines = []
    for label in month_labels:
        monthly_lines.append(
            f"**{label}**　系统建议 {monthly_suggest.get(label, 0):,}　"
            f"运营预计 {monthly_op.get(label, 0):,}　"
            f"实际已下单 {monthly_actual.get(label, 0):,}"
        )
    monthly_text = "\n".join(monthly_lines)

    type_monthly_lines = []
    for label in month_labels:
        suggest_col = f"`{label}建议下单`"
        try:
            with db_cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT 面料类型, SUM({suggest_col}) AS 建议量
                    FROM `建议下单量表`
                    GROUP BY 面料类型
                    ORDER BY 面料类型
                    """
                )
                rows = cursor.fetchall()
            parts = "　".join(
                f"{r['面料类型']} {int(r['建议量'] or 0):,} 件"
                for r in rows
            )
            type_monthly_lines.append(f"**{label}**　{parts}")
        except Exception as e:
            logger.warning(f"读取按面料类型分月失败: {label}: {e}")
            type_monthly_lines.append(f"**{label}**　暂无数据")
    type_monthly_text = "\n".join(type_monthly_lines)

    top5_monthly_lines = []
    for label in month_labels:
        suggest_col = f"`{label}建议下单`"
        try:
            with db_cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT SPU, 店铺, 工厂, {suggest_col} AS 建议量
                    FROM `建议下单量表`
                    WHERE {suggest_col} > 0
                    ORDER BY {suggest_col} DESC
                    LIMIT 5
                    """
                )
                rows = cursor.fetchall()
            if rows:
                top5_monthly_lines.append(f"**{label}**")
                for i, r in enumerate(rows, 1):
                    top5_monthly_lines.append(
                        f"  {i}. **{r['SPU']}** · {r['店铺']}"
                        f"（{r['工厂'] or '未记录'}）：{int(r['建议量'] or 0):,} 件"
                    )
        except Exception as e:
            logger.warning(f"读取各月缺口 TOP5 失败: {label}: {e}")
            top5_monthly_lines.append(f"**{label}**　暂无数据")
    top5_monthly_text = "\n".join(top5_monthly_lines) or "暂无数据"

    return {
        "header": {
            "title": {"tag": "plain_text", "content": f"📦 {month_label} 采购建议 · 生产"},
            "template": "blue",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**总览**\n"
                        f"总款数：{int(overview['总款数'] or 0):,} 款　"
                        f"需补单：{int(overview['需补单款数'] or 0):,} 款\n"
                        f"建议下单总量：**{int(overview['建议下单总量'] or 0):,} 件**"
                    ),
                },
            },
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**各月明细**\n{monthly_text}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**按面料类型（分月）**\n{type_monthly_text}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**各月缺口 TOP5**\n{top5_monthly_text}"}},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": "详细数据请查看飞书「建议下单量表」"}]},
        ],
    }


def build_fill_status_card(current_date: datetime, fill_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    组装“填报情况-数据”卡片。

    排版：
    1. 所有月份的店铺总览；
    2. 运营明细使用当前月份起连续三个月横向窄列展示。
    """
    month_label = f"{current_date.year}年{current_date.month}月"
    data_updated_at = read_fill_status_data_updated_at()

    detail_month_labels = make_month_labels(current_date, 3)
    detail_month_full_labels = []
    detail_month_names = []
    for label in detail_month_labels:
        y, m, _ym, _stat = month_label_to_year_month(label)
        detail_month_full_labels.append(f"{y}年{m}月")
        detail_month_names.append(f"{m}月")

    overview_rows = []
    active_month = ""

    for r in fill_rows:
        level = r.get("层级")

        if level == "month":
            active_month = r.get("月份", "")
            overview_rows.append({
                "月份": active_month,
                "店铺": "--",
                "系统建议": fmt_int(r.get("系统建议")),
                "运营预计": fmt_int(r.get("运营预计")),
            })
            continue

        if level == "shop":
            overview_rows.append({
                "月份": active_month,
                "店铺": r.get("店铺", ""),
                "系统建议": fmt_int(r.get("系统建议")),
                "运营预计": fmt_int(r.get("运营预计")),
            })

    detail_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    active_month = ""

    for r in fill_rows:
        level = r.get("层级")

        if level == "month":
            active_month = r.get("月份", "")
            continue

        if level != "operator":
            continue

        if active_month not in detail_month_full_labels:
            continue

        shop = r.get("店铺", "") or "未记录店铺"
        operator = r.get("运营", "") or "未记录"
        key = (shop, operator)

        if key not in detail_map:
            row = {"店铺": shop, "运营": operator}
            for idx in range(3):
                row[f"m{idx}_系统建议"] = "0"
                row[f"m{idx}_运营预计"] = "0"
            detail_map[key] = row

        matched_idx = detail_month_full_labels.index(active_month)
        detail_map[key][f"m{matched_idx}_系统建议"] = fmt_int(r.get("系统建议"))
        detail_map[key][f"m{matched_idx}_运营预计"] = fmt_int(r.get("运营预计"))

    detail_rows = list(detail_map.values())
    detail_rows.sort(key=lambda x: (x.get("店铺", ""), x.get("运营", "")))

    return {
        "header": {
            "title": {
                "tag": "plain_text",
                "content": f"📋 {month_label} 填报情况 · 数据",
            },
            "template": "purple",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**数据更新时间：** {data_updated_at}",
                },
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "**所有月份 · 店铺总览**",
                },
            },
            {
                "tag": "table",
                "page_size": 20,
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
                    {"name": "月份", "display_name": "月份", "width": "auto", "horizontal_align": "left"},
                    {"name": "店铺", "display_name": "店铺", "width": "auto", "horizontal_align": "left"},
                    {"name": "系统建议", "display_name": "系统", "width": "auto", "horizontal_align": "right"},
                    {"name": "运营预计", "display_name": "运营", "width": "auto", "horizontal_align": "right"},
                ],
                "rows": overview_rows,
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**{detail_month_names[0]}起 · 运营明细**（系=系统建议，运=运营预计）",
                },
            },
            {
                "tag": "table",
                "page_size": 20,
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
                    {"name": "店铺", "display_name": "店铺", "width": "auto", "horizontal_align": "left"},
                    {"name": "运营", "display_name": "运营", "width": "auto", "horizontal_align": "left"},
                    {"name": "m0_系统建议", "display_name": f"{detail_month_names[0].replace('月', '')}系", "width": "auto", "horizontal_align": "right"},
                    {"name": "m0_运营预计", "display_name": f"{detail_month_names[0].replace('月', '')}运", "width": "auto", "horizontal_align": "right"},
                    {"name": "m1_系统建议", "display_name": f"{detail_month_names[1].replace('月', '')}系", "width": "auto", "horizontal_align": "right"},
                    {"name": "m1_运营预计", "display_name": f"{detail_month_names[1].replace('月', '')}运", "width": "auto", "horizontal_align": "right"},
                    {"name": "m2_系统建议", "display_name": f"{detail_month_names[2].replace('月', '')}系", "width": "auto", "horizontal_align": "right"},
                    {"name": "m2_运营预计", "display_name": f"{detail_month_names[2].replace('月', '')}运", "width": "auto", "horizontal_align": "right"},
                ],
                "rows": detail_rows,
            },
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": "说明：上方为所有月份店铺总览；下方为当前月份起连续三个月的运营明细。"
                    }
                ],
            },
        ],
    }


def build_fabric_card(
    current_date: datetime,
    fabric_rows,
    total_usage: float,
    total_order: int,
    op_map: Dict[str, float],
    stock_map: Dict[str, float],
) -> Dict[str, Any]:
    """组装产品/面料负责人卡片。"""
    month_label = f"{current_date.year}年{current_date.month}月"

    table_rows = []
    for r in fabric_rows[:10]:
        fabric = r["面料"]
        sys_usage = float(r["系统预计用量"] or 0)
        op_usage = op_map.get(fabric, 0.0)
        stock = stock_map.get(fabric, 0.0)
        table_rows.append({
            "面料": fabric,
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
            {"name": "面料", "display_name": "面料", "width": "auto", "horizontal_align": "left"},
            {"name": "系统预计", "display_name": "系统预计(米)", "width": "auto", "horizontal_align": "right"},
            {"name": "运营预估", "display_name": "运营预估(米)", "width": "auto", "horizontal_align": "right"},
            {"name": "现有库存", "display_name": "现有库存(米)", "width": "auto", "horizontal_align": "right"},
        ],
        "rows": table_rows,
    }

    return {
        "header": {
            "title": {"tag": "plain_text", "content": f"🧵 {month_label} 定制面料用量 · 面料"},
            "template": "green",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**总览**\n"
                        f"定制面料种类：{len(fabric_rows)} 种\n"
                        f"总建议下单量：{total_order:,} 件\n"
                        f"系统预计总用量：**{total_usage:,.0f} 米**"
                    ),
                },
            },
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**TOP10 面料用量**（单位：米）"}},
            table_element,
            {
                "tag": "note",
                "elements": [
                    {"tag": "plain_text", "content": "现有库存 = 库存量/米 + 待到货量/米　详细数据请查看飞书「面料预计用量表」"}
                ],
            },
        ],
    }


# ────────────────────────────────────────────────────────────────────────────
# 主函数
# ────────────────────────────────────────────────────────────────────────────


def main() -> None:
    logger.info("=" * 60)
    logger.info("采购建议月度摘要推送")
    logger.info("=" * 60)

    current_date = datetime.now()

    overview, by_type, top5, month_labels, monthly_suggest, monthly_op = read_order_suggest_summary(current_date)
    monthly_actual, _ = read_actual_order_by_month(current_date)
    fabric_rows, total_usage, total_order, op_map, stock_map = read_fabric_usage_summary()

    prod_card = build_production_card(
        current_date,
        overview,
        by_type,
        top5,
        month_labels,
        monthly_suggest,
        monthly_op,
        monthly_actual,
    )
    send_card(RECEIVER_PRODUCTION, prod_card)

    fill_rows = read_fill_status_rows(month_labels)
    fill_card = build_fill_status_card(current_date, fill_rows)
    send_card(RECEIVER_PRODUCTION, fill_card)

    fabric_card = build_fabric_card(current_date, fabric_rows, total_usage, total_order, op_map, stock_map)
    send_card(RECEIVER_PRODUCT, fabric_card)

    logger.info("推送完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
