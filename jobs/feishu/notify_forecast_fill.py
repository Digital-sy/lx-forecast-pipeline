#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
通知运营去飞书多维表填写预估下单量

发送流程：
  1. 核查飞书多维表数据（表已更新、填写月份正确）
  2. 从数据库获取运营名单及其负责店铺
     + 自动从人员多维表拉取在职运营的 open_id（无需手动维护）
  3. 推送「发送预览确认卡片」给管理员（刘宗霖），
     在飞书内回复「确认发送」/「取消」来触发或中止
  4. 确认后正式发送通知给各运营，并推送发送结果汇报

用法：
  # 正式模式（推卡到飞书，等管理员回复确认，默认）
  python -m jobs.feishu.notify_forecast_fill

  # 预览模式（不发送，仅打印预览信息）
  python -m jobs.feishu.notify_forecast_fill --dry-run

  # 跳过飞书确认直接发送
  python -m jobs.feishu.notify_forecast_fill --yes
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor
from common.feishu import FeishuClient

logger = get_logger("notify_forecast_fill")

# ── 飞书 App 配置 ──────────────────────────────────────────────────────────────
try:
    from common.config import settings
    FEISHU_APP_ID     = settings.FEISHU_APP_ID
    FEISHU_APP_SECRET = settings.FEISHU_APP_SECRET
except Exception:
    FEISHU_APP_ID     = os.getenv("FEISHU_APP_ID", "")
    FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")

FEISHU_BASE      = "https://open.feishu.cn/open-apis"
FEISHU_APP_TOKEN = "A1oCb6elda8Q76s0vNKcHYEznCg"   # 销量预估多维表
FEISHU_TABLE_URL = "https://dkh9m74cxl.feishu.cn/base/A1oCb6elda8Q76s0vNKcHYEznCg"

# ── 管理员 ─────────────────────────────────────────────────────────────────────
ADMIN_OPEN_ID = "ou_45d24eddffa044503caf29d6c8a2e003"   # 刘宗霖
ADMIN_NAME    = "刘宗霖"

# ── 人员多维表（自动拉取在职运营 open_id，无需手动维护）────────────────────────
STAFF_APP_TOKEN = "QYQTb0gWxaRAARsTpMYcvzxAnjh"
STAFF_TABLE_ID  = "tblby8J4GqbYcqkV"

# 等待管理员飞书确认的超时（秒）
CONFIRM_TIMEOUT = 600

# ── 不通知的店铺 ────────────────────────────────────────────────────────────────
EXCLUDED_SHOPS = {
    "TEMU半托管-A店", "TEMU半托管-C店", "TEMU半托管-M店",
    "TEMU半托管-P店", "TEMU半托管-V店", "TEMU半托管-本土店-R店",
    "TK本土店-1店", "TK跨境店-2店", "CY-US", "DX-US", "MT-CA",
}

# ── Token 缓存 ──────────────────────────────────────────────────────────────────
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


def _h() -> dict:
    return {"Authorization": f"Bearer {_get_token()}", "Content-Type": "application/json"}


def _section(title: str):
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}\n")


# ══════════════════════════════════════════════════════════════════════════════
# Step 1：核查飞书多维表数据
# ══════════════════════════════════════════════════════════════════════════════

def get_forecast_order_labels(current_date: datetime = None) -> List[str]:
    """本月起未来 4 个月的「预计下单量(运营填写)」字段标签"""
    if current_date is None:
        current_date = datetime.now()
    labels = []
    for i in range(4):
        m = current_date.month + i
        y = current_date.year
        while m > 12:
            m -= 12
            y += 1
        labels.append(f"{str(y)[-2:]}年{m}月预计下单量(运营填写)")
    return labels


async def verify_feishu_table_data() -> Tuple[bool, List[str], Dict[str, dict]]:
    """
    核查飞书多维表：字段是否对齐当前月份、各店铺填写进度。
    Returns: (通过?, 可填写月份标签列表, {店铺名: 填写统计})
    """
    _section("Step 1  核查飞书多维表数据")

    expected_labels = get_forecast_order_labels()
    print(f"  当前应填写月份（共 {len(expected_labels)} 个）：")
    for lbl in expected_labels:
        print(f"    ✦ {lbl}")

    try:
        client = FeishuClient(app_token=FEISHU_APP_TOKEN, table_id="")
        tables = await client.get_tables()
    except Exception as e:
        print(f"\n  ✗ 连接飞书多维表失败：{e}")
        return False, expected_labels, {}

    active = {k: v for k, v in tables.items() if k not in EXCLUDED_SHOPS}
    print(f"\n  共 {len(tables)} 个表，有效店铺 {len(active)} 个")

    if not active:
        print("  ✗ 无有效店铺表，请先运行 write_sales_to_feishu.py")
        return False, expected_labels, {}

    shop_stats: Dict[str, dict] = {}
    check_shops = list(active.keys())[:3]
    fields_ok = True

    for shop_name in check_shops:
        table_id = active[shop_name]
        try:
            sc = FeishuClient(app_token=FEISHU_APP_TOKEN, table_id=table_id)
            field_map = await sc.get_table_fields()
            # field_map 结构为 {字段ID: 字段名}，需在 values() 中查找
            missing = [l for l in expected_labels if l not in field_map.values()]
            if missing:
                print(f"\n  ⚠  [{shop_name}] 缺少字段：{missing}")
                print("     → 请先运行 write_sales_to_feishu.py 更新表结构")
                fields_ok = False
                continue

            records = await sc.read_records()
            total = len(records)
            current_label = expected_labels[0]
            filled = sum(
                1 for r in records
                if r.get("fields", {}).get(current_label, 0) not in (None, 0, "")
            )
            rate = filled / total if total else 0.0
            shop_stats[shop_name] = {"total": total, "filled": filled, "rate": rate}
            bar = "█" * int(rate * 10) + "░" * (10 - int(rate * 10))
            print(
                f"\n  ✓ [{shop_name}] 共 {total} 行  "
                f"{current_label[:10]}… 已填 {filled} 行  {bar} {rate*100:.0f}%"
            )

        except Exception as e:
            print(f"\n  ✗ 读取 [{shop_name}] 失败：{e}")
            fields_ok = False

    if not fields_ok:
        print("\n  ✗ 数据核查未通过，请先运行 write_sales_to_feishu.py")
        return False, expected_labels, shop_stats

    print(f"\n  ✓ 数据核查通过，抽查 {len(check_shops)} 个店铺正常")
    return True, expected_labels, shop_stats


# ══════════════════════════════════════════════════════════════════════════════
# Step 2：获取运营名单 + 自动拉取 open_id
# ══════════════════════════════════════════════════════════════════════════════

def fetch_operator_openid_map() -> Dict[str, str]:
    """
    从飞书人员多维表自动拉取在职人员的 open_id。
    过滤「是否离职=True」的记录，支持翻页。
    Returns: {姓名: open_id}
    """
    import requests
    try:
        headers = _h()
        result = {}
        page_token = None

        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token

            resp = requests.get(
                f"{FEISHU_BASE}/bitable/v1/apps/{STAFF_APP_TOKEN}/tables/{STAFF_TABLE_ID}/records",
                headers=headers,
                params=params,
                timeout=15,
            ).json()

            if resp.get("code") != 0:
                logger.warning(f"拉取人员表失败: {resp.get('msg')}")
                break

            for item in resp.get("data", {}).get("items", []):
                fields = item.get("fields", {})
                if fields.get("是否离职"):
                    continue
                users = fields.get("人员", [])
                if not users:
                    continue
                name = users[0].get("name", "").strip()
                oid  = users[0].get("id", "").strip()
                if name and oid:
                    result[name] = oid

            if not resp.get("data", {}).get("has_more"):
                break
            page_token = resp.get("data", {}).get("page_token")

        print(f"  ✓ 人员表：拉取到 {len(result)} 位在职人员 open_id")
        return result

    except Exception as e:
        logger.warning(f"拉取人员 open_id 失败: {e}")
        return {}


def get_operator_shop_map() -> Dict[str, List[str]]:
    """运营 → 负责店铺列表（从产品信息表 / listing 表）"""
    op_map: Dict[str, List[str]] = defaultdict(list)
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT 运营, 店铺名 FROM `产品信息`
                WHERE 运营 IS NOT NULL AND 运营 != ''
                  AND 店铺名 IS NOT NULL AND 店铺名 != ''
                ORDER BY 运营, 店铺名
            """)
            for row in cursor.fetchall():
                op = (row.get("运营") or "").strip()
                shop = (row.get("店铺名") or "").strip()
                if op and shop and shop not in EXCLUDED_SHOPS:
                    if shop not in op_map[op]:
                        op_map[op].append(shop)
    except Exception as e:
        logger.warning(f"产品信息表读取失败: {e}")

    if not op_map:
        try:
            with db_cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT 负责人, 店铺 FROM `listing`
                    WHERE 负责人 IS NOT NULL AND 负责人 != '' AND 负责人 != '无'
                      AND 店铺 IS NOT NULL AND 店铺 != ''
                    ORDER BY 负责人, 店铺
                """)
                for row in cursor.fetchall():
                    op = (row.get("负责人") or "").strip()
                    shop = (row.get("店铺") or "").strip()
                    if op and shop and shop not in EXCLUDED_SHOPS:
                        if shop not in op_map[op]:
                            op_map[op].append(shop)
        except Exception as e:
            logger.warning(f"listing 表读取失败: {e}")

    return dict(op_map)


# 月销量低于此阈值的运营不发送通知
MIN_MONTHLY_SALES = 100


def get_shop_monthly_sales() -> Dict[str, int]:
    """
    从销量统计_msku月度表查询当月各店铺总销量。
    Returns: {店铺名: 当月总销量}
    """
    from datetime import datetime
    now = datetime.now()
    # 取当月第一天，匹配表中统计日期字段
    month_start = now.strftime("%Y-%m-01")

    shop_sales: Dict[str, int] = {}
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT 店铺, SUM(销量) AS 月销量
                FROM `销量统计_msku月度`
                WHERE 统计日期 = %s
                  AND 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
                GROUP BY 店铺
            """, (month_start,))
            for row in cursor.fetchall():
                shop = (row.get("店铺") or "").strip()
                sales = int(row.get("月销量") or 0)
                if shop:
                    shop_sales[shop] = sales
        print(f"  ✓ 销量数据：查到 {len(shop_sales)} 个店铺当月销量（{now.strftime('%Y年%m月')}）")
    except Exception as e:
        logger.warning(f"查询当月销量失败: {e}，将跳过销量筛选")
    return shop_sales


def build_send_tasks(
    op_shop_map: Dict[str, List[str]],
    openid_map: Dict[str, str],
    shop_sales: Dict[str, int] = None,
) -> List[Dict]:
    """
    构建待发送任务列表。
    - open_id 来自人员多维表
    - 当月负责店铺总销量 < MIN_MONTHLY_SALES 的运营跳过（低销量无需填写预估）
    """
    tasks = []
    shop_sales = shop_sales or {}

    for name, shops in sorted(op_shop_map.items()):
        open_id = openid_map.get(name, "")

        # 汇总该运营所有负责店铺的当月销量
        total_sales = sum(shop_sales.get(s, 0) for s in shops)

        # 销量过低则跳过
        if shop_sales and total_sales < MIN_MONTHLY_SALES:
            print(f"  ✗ 跳过 {name}（当月销量 {total_sales} < {MIN_MONTHLY_SALES}）")
            continue

        tasks.append({
            "name": name,
            "open_id": open_id,
            "shops": shops,
            "has_openid": bool(open_id),
            "monthly_sales": total_sales,
        })
    return tasks


# ══════════════════════════════════════════════════════════════════════════════
# Step 3：推送确认卡片给管理员，并轮询飞书消息等待回复
# ══════════════════════════════════════════════════════════════════════════════

def build_admin_confirm_card(
    send_tasks: List[Dict],
    month_labels: List[str],
    shop_stats: Dict[str, dict],
) -> dict:
    """构建发给管理员的发送前确认卡片（含完整预览）"""
    now    = datetime.now()
    valid  = [t for t in send_tasks if t["has_openid"]]
    no_id  = [t for t in send_tasks if not t["has_openid"]]

    list_lines = []
    for t in send_tasks:
        shops_str = "、".join(t["shops"][:3]) + ("…" if len(t["shops"]) > 3 else "")
        icon = "✅" if t["has_openid"] else "⚠️"
        list_lines.append(f"{icon} **{t['name']}**  →  {shops_str}")
    list_text = "\n".join(list_lines) or "（未从数据库找到运营名单）"

    no_id_warn = ""
    if no_id:
        names = "、".join(t["name"] for t in no_id)
        no_id_warn = f"\n\n⚠️ **未找到 open_id（将跳过）：** {names}\n可能已离职或不在人员表中。"

    months_text = "　".join(
        f"`{lbl.replace('预计下单量(运营填写)', '')}`" for lbl in month_labels
    )

    stat_lines = []
    for shop, stat in shop_stats.items():
        bar = "█" * int(stat["rate"] * 10) + "░" * (10 - int(stat["rate"] * 10))
        stat_lines.append(
            f"**{shop}**  {bar}  {stat['rate']*100:.0f}%  ({stat['filled']}/{stat['total']} 行)"
        )
    stat_text = "\n".join(stat_lines) if stat_lines else "（抽查店铺暂无填写数据）"

    expire_min = CONFIRM_TIMEOUT // 60
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "📋 预估填写通知 · 请确认"},
            "template": "orange",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"Hi **{ADMIN_NAME}**，填写通知已准备完毕，"
                        "请确认下方信息后在**此对话**中回复关键词执行。"
                    ),
                },
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**📅 需填写月份（共 {len(month_labels)} 个）**\n{months_text}",
                },
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**👥 发送名单（共 {len(send_tasks)} 位，{len(valid)} 位可发送）**"
                        f"{no_id_warn}\n{list_text}"
                    ),
                },
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**📊 当月已填进度（抽查 {len(shop_stats)} 个店铺）**\n{stat_text}",
                },
            },
            {"tag": "hr"},
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "🔗 预览飞书多维表"},
                        "type": "default",
                        "url": FEISHU_TABLE_URL,
                    }
                ],
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**↓ 请在此对话中回复以下关键词 ↓**\n\n"
                        "　✅  **`确认发送`**  →  向所有运营发送填写通知\n\n"
                        "　❌  **`取消`**  →  放弃本次发送\n\n"
                        f"⏱ {expire_min} 分钟内有效，超时自动取消"
                    ),
                },
            },
            {
                "tag": "note",
                "elements": [
                    {"tag": "plain_text", "content": f"由系统于 {now.strftime('%Y-%m-%d %H:%M:%S')} 生成"}
                ],
            },
        ],
    }


def _send_interactive(receive_id: str, card: dict, id_type: str = "open_id") -> Optional[dict]:
    """发送卡片，返回响应 data（含 message_id、chat_id）"""
    import requests
    resp = requests.post(
        f"{FEISHU_BASE}/im/v1/messages",
        headers=_h(),
        params={"receive_id_type": id_type},
        json={
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(card, ensure_ascii=False),
        },
        timeout=15,
    ).json()
    if resp.get("code") != 0:
        logger.error(f"发送卡片失败: {resp.get('msg')} (code={resp.get('code')})")
        return None
    return resp.get("data")


def _send_text(receive_id: str, text: str, id_type: str = "open_id") -> Optional[dict]:
    """发送纯文本消息"""
    import requests
    resp = requests.post(
        f"{FEISHU_BASE}/im/v1/messages",
        headers=_h(),
        params={"receive_id_type": id_type},
        json={
            "receive_id": receive_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        },
        timeout=15,
    ).json()
    if resp.get("code") != 0:
        logger.error(f"发送文本失败: {resp.get('msg')}")
        return None
    return resp.get("data")


def _update_card(message_id: str, card: dict) -> bool:
    """更新（patch）已发出的卡片内容"""
    import requests
    resp = requests.patch(
        f"{FEISHU_BASE}/im/v1/messages/{message_id}/content",
        headers=_h(),
        json={"content": json.dumps(card, ensure_ascii=False)},
        timeout=15,
    ).json()
    return resp.get("code") == 0


def poll_for_admin_reply(chat_id: str, sent_at: float) -> str:
    """
    每 3 秒轮询 chat_id 内的新消息，等待管理员回复关键词。
    Returns: "confirm" | "cancel" | "timeout"
    """
    import requests
    deadline = sent_at + CONFIRM_TIMEOUT
    last_ts  = sent_at

    print(f"\n  ⏳ 等待飞书确认（最多 {CONFIRM_TIMEOUT // 60} 分钟）...", flush=True)
    print("     请在飞书对话中回复「确认发送」或「取消」\n", flush=True)

    while time.time() < deadline:
        time.sleep(3)
        try:
            resp = requests.get(
                f"{FEISHU_BASE}/im/v1/messages",
                headers=_h(),
                params={
                    "container_id_type": "chat",
                    "container_id": chat_id,
                    "page_size": 10,
                    "sort_type": "ByCreateTimeDesc",
                },
                timeout=10,
            ).json()

            for msg in resp.get("data", {}).get("items", []):
                create_ts = int(msg.get("create_time", "0")) / 1000
                if create_ts <= last_ts:
                    continue

                sender_id = msg.get("sender", {}).get("id", "")
                if sender_id != ADMIN_OPEN_ID:
                    continue

                msg_type = msg.get("msg_type", "")
                body     = msg.get("body", {}).get("content", "")
                text = ""
                if msg_type == "text":
                    try:
                        text = json.loads(body).get("text", "").strip()
                    except Exception:
                        text = body.strip()

                text_clean = text.replace(" ", "").replace("\u3000", "")
                remaining  = int(deadline - time.time())
                print(f"  📩 收到：「{text}」", flush=True)

                if "确认发送" in text_clean:
                    return "confirm"
                elif "取消" in text_clean:
                    return "cancel"
                else:
                    remaining_str = f"{remaining // 60}分{remaining % 60}秒"
                    _send_text(
                        chat_id,
                        f"❓ 未识别指令「{text}」\n请回复「确认发送」或「取消」\n（剩余 {remaining_str}）",
                        id_type="chat",
                    )
                last_ts = max(last_ts, create_ts)

        except Exception as e:
            logger.warning(f"轮询消息失败: {e}")

    return "timeout"


def _patch_confirmed(message_id: str, op_count: int):
    now = datetime.now().strftime("%H:%M:%S")
    _update_card(message_id, {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": "✅ 已确认，正在发送..."}, "template": "green"},
        "elements": [{
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**{ADMIN_NAME}** 于 **{now}** 确认，"
                    f"正在向 **{op_count}** 位运营发送通知...\n\n发送完成后将推送结果汇报。"
                ),
            },
        }],
    })


def _patch_cancelled(message_id: str, reason: str):
    now = datetime.now().strftime("%H:%M:%S")
    _update_card(message_id, {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": "❌ 已取消"}, "template": "red"},
        "elements": [{
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"本次发送已于 **{now}** 取消（{reason}）。"},
        }],
    })


# ══════════════════════════════════════════════════════════════════════════════
# Step 4：发送运营通知卡片 & 结果汇报
# ══════════════════════════════════════════════════════════════════════════════

def build_operator_notify_card(
    op_name: str,
    shops: List[str],
    month_labels: List[str],
    shop_stats: Dict[str, dict],
) -> dict:
    """构建发给运营的填写通知卡片"""
    now_str = datetime.now().strftime("%Y年%m月%d日")
    months_text = "　".join(
        f"**{lbl.replace('预计下单量(运营填写)', '')}**" for lbl in month_labels
    )
    shop_lines = []
    for shop in shops:
        stat = shop_stats.get(shop)
        if stat and stat["total"] > 0:
            bar = "█" * int(stat["rate"] * 10) + "░" * (10 - int(stat["rate"] * 10))
            shop_lines.append(
                f"**{shop}**  {bar}  {stat['rate']*100:.0f}%（已填 {stat['filled']}/{stat['total']} 行）"
            )
        else:
            shop_lines.append(f"**{shop}**")
    shop_text = "\n".join(shop_lines) if shop_lines else "（暂无数据）"

    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": "📋 请填写预估下单量"}, "template": "orange"},
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"Hi **{op_name}**，\n\n"
                        f"请在飞书多维表中填写以下月份的**预计下单量**：\n"
                        f"{months_text}"
                    ),
                },
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**📦 您负责的店铺（当月填写进度）**\n{shop_text}",
                },
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**填写说明**\n"
                        "① 打开多维表，切换到您负责的店铺标签\n"
                        "② 找到 `XX年X月预计下单量(运营填写)` 列\n"
                        "③ 按 SKU 行填入本期预计下单数量"
                    ),
                },
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "🔗 打开多维表填写"},
                        "type": "primary",
                        "url": FEISHU_TABLE_URL,
                    }
                ],
            },
            {
                "tag": "note",
                "elements": [
                    {"tag": "plain_text", "content": f"由系统于 {now_str} 自动推送 · 如有疑问请联系数据团队"}
                ],
            },
        ],
    }


def execute_send(
    send_tasks: List[Dict],
    month_labels: List[str],
    shop_stats: Dict[str, dict],
    dry_run: bool,
) -> Tuple[int, int, int]:
    """执行发送，返回 (成功, 跳过, 失败)"""
    import requests
    ok, skip, fail = 0, 0, 0

    for task in send_tasks:
        name = task["name"]
        oid  = task["open_id"]

        if not oid:
            print(f"  ⚠  跳过 {name}（未在人员表找到，可能已离职）")
            skip += 1
            continue

        card = build_operator_notify_card(name, task["shops"], month_labels, shop_stats)

        if dry_run:
            print(f"  [dry-run] → {name}")
            ok += 1
            continue

        try:
            resp = requests.post(
                f"{FEISHU_BASE}/im/v1/messages",
                headers=_h(),
                params={"receive_id_type": "open_id"},
                json={
                    "receive_id": oid,
                    "msg_type": "interactive",
                    "content": json.dumps(card, ensure_ascii=False),
                },
                timeout=15,
            ).json()
            if resp.get("code") == 0:
                print(f"  ✓ 已发送 → {name}")
                ok += 1
            else:
                print(f"  ✗ 失败   → {name}  {resp.get('msg')}")
                fail += 1
        except Exception as e:
            print(f"  ✗ 异常   → {name}  {e}")
            fail += 1

        time.sleep(0.3)

    return ok, skip, fail


def send_admin_result(ok: int, skip: int, fail: int, month_labels: List[str]):
    """发送完成后向管理员推送结果汇报卡片"""
    now_str = datetime.now().strftime("%H:%M:%S")
    months_text = "、".join(lbl.replace("预计下单量(运营填写)", "") for lbl in month_labels)
    all_ok = fail == 0

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "✅ 通知发送完成" if all_ok else "⚠️ 通知发送完成（有失败）",
            },
            "template": "green" if all_ok else "yellow",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**发送月份：** {months_text}\n"
                        f"**完成时间：** {now_str}\n\n"
                        f"✅ 成功 **{ok}** 人\n"
                        f"⏭ 跳过 **{skip}** 人（未在人员表找到）\n"
                        f"❌ 失败 **{fail}** 人"
                        + (
                            "\n\n⚠️ 失败的运营请手动在飞书@通知。"
                            if fail > 0 else ""
                        )
                    ),
                },
            }
        ],
    }
    _send_interactive(ADMIN_OPEN_ID, card)


# ══════════════════════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="通知运营填写飞书多维表预估下单量")
    parser.add_argument("--dry-run", action="store_true", help="预览模式，不发送")
    parser.add_argument("--yes", "-y", action="store_true", help="跳过飞书确认直接发送")
    args = parser.parse_args()
    dry_run  = args.dry_run
    auto_yes = args.yes

    print()
    print("══════════════════════════════════════════════════════════")
    print("  飞书预估下单量填写通知")
    if dry_run:
        print("  ⚙  预览模式（--dry-run）")
    print("══════════════════════════════════════════════════════════")

    # ── Step 1：核查飞书多维表 ─────────────────────────────────────────────────
    data_ok, month_labels, shop_stats = await verify_feishu_table_data()
    if not data_ok:
        print("\n✗ 数据核查未通过，终止。请先运行 write_sales_to_feishu.py 再重试。\n")
        sys.exit(1)

    # ── Step 2：获取运营名单 + 自动拉取 open_id ───────────────────────────────
    _section("Step 2  获取运营名单")
    openid_map  = fetch_operator_openid_map()
    op_shop_map = get_operator_shop_map()
    shop_sales  = get_shop_monthly_sales()
    send_tasks  = build_send_tasks(op_shop_map, openid_map, shop_sales)
    valid_count = sum(t["has_openid"] for t in send_tasks)

    for t in send_tasks:
        icon = "✓" if t["has_openid"] else "⚠"
        sales_str = f"  月销量 {t['monthly_sales']}" if shop_sales else ""
        print(f"  {icon} {t['name']}  →  {', '.join(t['shops'])}{sales_str}")

    print(f"\n  共 {len(send_tasks)} 位运营，{valid_count} 位有 open_id 可发送")

    if not send_tasks:
        print("  ✗ 无运营名单，退出。\n")
        sys.exit(0)

    # ── Step 3：飞书确认 ───────────────────────────────────────────────────────
    _section("Step 3  发送确认卡片给管理员")

    message_id: Optional[str] = None
    confirm_result: str

    if dry_run:
        print("  [dry-run] 跳过飞书确认，直接进入发送预览")
        confirm_result = "confirm"

    elif auto_yes:
        print("  --yes 模式：跳过飞书确认")
        confirm_result = "confirm"

    else:
        print(f"  正在推送确认卡片给 {ADMIN_NAME}（{ADMIN_OPEN_ID[:12]}...）...", flush=True)
        data = _send_interactive(
            ADMIN_OPEN_ID,
            build_admin_confirm_card(send_tasks, month_labels, shop_stats),
        )
        if not data:
            print("  ✗ 确认卡片发送失败，退出。")
            sys.exit(1)

        message_id = data.get("message_id")
        chat_id    = data.get("chat_id")
        sent_at    = time.time()
        print(f"  ✓ 卡片已发出 → message_id={message_id}", flush=True)

        confirm_result = poll_for_admin_reply(chat_id, sent_at)

    # ── 根据回复结果处理 ──────────────────────────────────────────────────────
    if confirm_result == "confirm":
        if message_id:
            _patch_confirmed(message_id, valid_count)
        _section("Step 4  正式发送通知")
        ok, skip, fail = execute_send(send_tasks, month_labels, shop_stats, dry_run)
        print(f"\n  完成：✅ {ok}  ⏭ {skip}  ❌ {fail}")
        if not dry_run:
            send_admin_result(ok, skip, fail, month_labels)
            print(f"  ✓ 结果汇报已推送给 {ADMIN_NAME}")

    elif confirm_result == "cancel":
        if message_id:
            _patch_cancelled(message_id, "手动取消")
        print("\n  已取消发送。\n")

    else:  # timeout
        if message_id:
            _patch_cancelled(message_id, f"超时未确认（{CONFIRM_TIMEOUT // 60} 分钟）")
        print(f"\n  超时未收到确认，已自动取消。\n")

    print()


if __name__ == "__main__":
    asyncio.run(main())
