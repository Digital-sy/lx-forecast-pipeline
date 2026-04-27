#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
费用单自动化统一调度入口

两种模式：
  【每日自动】cron 定时执行，处理本月累计数据
    python fee_automation.py

  【月度修正】头程单价更新后手动执行，重跑上月
    python fee_automation.py --monthly

  【指定月份】
    python fee_automation.py --month 2026-03

Cron 示例（每天凌晨 2:30 执行）：
  30 2 * * * /opt/apps/pythondata/venv/bin/python3 /opt/apps/pythondata/jobs/Sync_data/fee_automation.py >> /var/log/fee_automation.log 2>&1
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime, date, timedelta
from calendar import monthrange

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import get_logger

logger = get_logger('fee_automation')

FEISHU_USER_ID = 'ou_45d24eddffa044503caf29d6c8a2e003'  # 刘宗霖

def notify_feishu(results: dict, month: str, elapsed: int, error_msg: str = None):
    """发送任务完成通知到飞书（直接调用API，不依赖feishu-dev-bot）"""
    try:
        import requests, json
        from common.config import settings

        # 获取飞书token
        token_resp = requests.post(
            'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
            json={'app_id': settings.FEISHU_APP_ID, 'app_secret': settings.FEISHU_APP_SECRET},
            timeout=10
        ).json()
        token = token_resp.get('tenant_access_token')
        if not token:
            logger.warning(f"⚠️  获取飞书token失败: {token_resp}")
            return

        status_map = {True: '✅ 成功', False: '❌ 失败', 'skipped': '⏭️ 跳过'}
        all_success = all(v is not False for v in results.values())
        title = f"{'✅ 费用单同步完成' if all_success else '❌ 费用单同步失败'} | {month}"

        step_text = (
            f"**步骤1 拉取利润报表：** {status_map.get(results.get('fetch'), '—')}\n"
            f"**步骤2 更新计算字段：** {status_map.get(results.get('update'), '—')}\n"
            f"**步骤3 生成Excel：** {status_map.get(results.get('generate'), '—')}\n"
            f"**步骤4 上传领星：** {status_map.get(results.get('upload'), '—')}\n"
            f"**总耗时：** {elapsed//60}分{elapsed%60}秒"
        )

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "green" if all_success else "red"
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": step_text}}
            ]
        }

        if error_msg:
            card["elements"].append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"**错误信息：**\n{error_msg}"}
            })

        resp = requests.post(
            'https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id',
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            json={
                'receive_id': FEISHU_USER_ID,
                'msg_type': 'interactive',
                'content': json.dumps(card, ensure_ascii=False)
            },
            timeout=15
        ).json()

        if resp.get('code') == 0:
            logger.info("✅ 飞书通知已发送")
        else:
            logger.warning(f"⚠️  飞书通知发送失败: {resp.get('msg')}")
    except Exception as e:
        logger.warning(f"⚠️  飞书通知发送失败: {e}")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fee_excel_output')


# ─────────────────────────────────────────────────────────────────────────────
def get_current_month() -> str:
    return datetime.now().strftime('%Y-%m')


def get_last_month() -> str:
    today = date.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.strftime('%Y-%m')


def month_to_date_range(month: str):
    year, mon = map(int, month.split('-'))
    last_day = monthrange(year, mon)[1]
    return f"{year:04d}-{mon:02d}-01", f"{year:04d}-{mon:02d}-{last_day:02d}"


# ─────────────────────────────────────────────────────────────────────────────
# 步骤 A：拉取利润报表（仅每日模式需要）
# ─────────────────────────────────────────────────────────────────────────────
async def step_fetch(start_date: str, end_date: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"步骤 1/4  拉取利润报表数据  [{start_date} ~ {end_date}]")
    logger.info("=" * 70)
    try:
        import fetch_profit_report_msku_daily as fetcher
        await fetcher.main(start_date=start_date, end_date=end_date, monthly=True)
        logger.info("✅ 步骤 1 完成")
        return True
    except Exception as e:
        logger.error(f"❌ 步骤 1 失败: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 步骤 B：更新计算字段
# ─────────────────────────────────────────────────────────────────────────────
def step_update(start_date: str, end_date: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"步骤 2/4  更新计算字段  [{start_date} ~ {end_date}]")
    logger.info("=" * 70)
    try:
        import update_profit_report_calculated_fields as updater
        updater.main(start_date=start_date, end_date=end_date)
        logger.info("✅ 步骤 2 完成")
        return True
    except Exception as e:
        logger.error(f"❌ 步骤 2 失败: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 步骤 C：生成Excel文件
# ─────────────────────────────────────────────────────────────────────────────
def step_generate(month: str) -> list:
    logger.info("=" * 70)
    logger.info(f"步骤 3/4  生成Excel文件  [{month}]")
    logger.info("=" * 70)
    try:
        import generate_fee_excel as gen
        files = gen.generate(month=month, output_dir=OUTPUT_DIR)
        if files:
            logger.info(f"✅ 步骤 3 完成，生成 {len(files)} 个文件")
        else:
            logger.warning("⚠️  步骤 3：没有生成任何文件（可能该月没有费用数据）")
        return files
    except Exception as e:
        logger.error(f"❌ 步骤 3 失败: {e}", exc_info=True)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# 步骤 D：上传Excel（作废旧单 + 上传新文件）
# ─────────────────────────────────────────────────────────────────────────────
async def step_upload(files: list, month: str) -> bool:
    logger.info("=" * 70)
    logger.info(f"步骤 4/4  上传Excel到领星  [{month}，共{len(files)}个文件]")
    logger.info("=" * 70)
    try:
        import upload_fee_excel as uploader
        ok = await uploader.upload_files(
            excel_files=files,
            month=month,
            skip_discard=False
        )
        if ok:
            logger.info("✅ 步骤 4 完成")
        else:
            logger.error("❌ 步骤 4：部分文件上传失败")
        return ok
    except Exception as e:
        logger.error(f"❌ 步骤 4 失败: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────────────────────
async def main(month: str, is_monthly_correction: bool, skip_fetch: bool):
    start_time = datetime.now()

    start_date, end_date = month_to_date_range(month)

    logger.info("=" * 70)
    if is_monthly_correction:
        logger.info("🔄  月度修正模式（使用真实头程单价重跑）")
    else:
        logger.info("🚀  每日自动模式")
    logger.info(f"    目标月份：{month}  ({start_date} ~ {end_date})")
    logger.info(f"    开始时间：{start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    results = {}

    # ── 步骤1：拉取数据（月度修正不需要，数据已在库里）──
    if is_monthly_correction or skip_fetch:
        logger.info("⏭️  跳过步骤 1（月度修正模式，数据已在库中）")
        results['fetch'] = 'skipped'
    else:
        # 每日模式：拉取本月1号到昨天（monthly模式，3天一批）
        yesterday = (date.today() - __import__('datetime').timedelta(days=1)).strftime('%Y-%m-%d')
        results['fetch'] = await step_fetch(start_date=start_date, end_date=yesterday)
        if not results['fetch']:
            logger.error("⛔  步骤 1 失败，中止")
            _summary(results, start_time, month)
            sys.exit(1)

    # ── 步骤2：更新计算字段（整月重算）──
    results['update'] = step_update(start_date=start_date, end_date=end_date)
    if not results['update']:
        logger.error("⛔  步骤 2 失败，中止")
        _summary(results, start_time, month)
        sys.exit(1)

    # ── 步骤3：生成Excel ──
    files = step_generate(month=month)
    results['generate'] = bool(files)
    if not files:
        logger.error("⛔  步骤 3 失败，中止")
        _summary(results, start_time, month)
        sys.exit(1)

    # ── 步骤4：上传（含作废旧单）──
    results['upload'] = await step_upload(files=files, month=month)
    # 上传完成后清理本地Excel文件
    if results['upload']:
        import glob
        for f in glob.glob(os.path.join(OUTPUT_DIR, f'fee_{month}_*.xlsx')):
            os.remove(f)
            logger.info(f"  🗑️  已删除本地文件: {os.path.basename(f)}")

    _summary(results, start_time, month)

    if any(v is False for v in results.values()):
        sys.exit(1)


def _summary(results: dict, start_time: datetime, month: str):
    elapsed = int((datetime.now() - start_time).total_seconds())
    s = {True: '✅ 成功', False: '❌ 失败', 'skipped': '⏭️  跳过'}
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"📋  任务汇总  |  月份：{month}  |  耗时：{elapsed}秒")
    logger.info("=" * 70)
    logger.info(f"  步骤 1 - 拉取利润报表 : {s.get(results.get('fetch'), '—')}")
    logger.info(f"  步骤 2 - 更新计算字段 : {s.get(results.get('update'), '—')}")
    logger.info(f"  步骤 3 - 生成Excel   : {s.get(results.get('generate'), '—')}")
    logger.info(f"  步骤 4 - 上传领星    : {s.get(results.get('upload'), '—')}")
    logger.info("=" * 70)
    # 发送飞书通知
    notify_feishu(results, month, elapsed)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='费用单自动化统一调度')
    parser.add_argument(
        '--month', type=str, default=None,
        help='目标月份，格式：YYYY-MM，默认：本月'
    )
    parser.add_argument(
        '--monthly', action='store_true',
        help='月度修正模式：跳过拉取，重新计算上月并上传（头程单价更新后使用）'
    )
    parser.add_argument(
        '--skip-fetch', action='store_true',
        help='跳过拉取步骤（数据已在库中时使用）'
    )
    args = parser.parse_args()

    # 月度修正模式默认用上个月
    if args.monthly:
        months_to_run = [args.month or get_last_month()]
    elif args.month:
        months_to_run = [args.month]
    else:
        # 默认逻辑：每月1-7号同时更新上月和本月，8号起只更新本月
        today = date.today()
        if today.day <= 7:
            months_to_run = [get_last_month(), get_current_month()]
            logger.info(f"📅 双月模式：{months_to_run[0]} + {months_to_run[1]}")
        else:
            months_to_run = [get_current_month()]

    try:
        for target_month in months_to_run:
            logger.info(f"\n{'='*60}\n开始处理月份：{target_month}\n{'='*60}")
            asyncio.run(main(
                month=target_month,
                is_monthly_correction=args.monthly,
                skip_fetch=args.skip_fetch,
            ))
            args.skip_fetch = False  # 第二个月不跳过拉取
    except KeyboardInterrupt:
        logger.warning("⚠️  用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 异常退出: {e}", exc_info=True)
        sys.exit(1)
