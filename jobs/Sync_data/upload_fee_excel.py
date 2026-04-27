#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
自动上传费用单Excel到领星ERP（Playwright）

流程：
  1. 登录领星
  2. 进入费用管理页
  3. 作废本月「已分摊」状态的费用单（API）
  4. 逐个上传Excel文件

使用方式：
  python upload_fee_excel.py --files /path/fee_2026-04_01of03.xlsx /path/fee_2026-04_02of03.xlsx
  python upload_fee_excel.py --dir /path/to/fee_excel_output --month 2026-04

依赖：
  pip install playwright --break-system-packages
  playwright install chromium
"""

import os
import sys
import asyncio
import argparse
import glob
from datetime import datetime
from typing import List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import get_logger
from common.config import settings

logger = get_logger('upload_fee_excel')

# ── 配置 ─────────────────────────────────────────────────────────────────────
LINGXING_URL      = 'https://erp.lingxing.com'
FEE_MGMT_URL      = 'https://erp.lingxing.com/#/erp/feeManagemant'
LOGIN_URL         = 'https://erp.lingxing.com/#/login'

# 从 settings / 环境变量读取账号密码
LINGXING_USERNAME = os.getenv("LINGXING_USERNAME", "")
LINGXING_PASSWORD = os.getenv("LINGXING_PASSWORD", "")

# 超时设置（毫秒）
NAV_TIMEOUT    = 60_000
ACTION_TIMEOUT = 30_000
UPLOAD_TIMEOUT = 300_000   # 上传大文件给更多时间（5分钟）


# ── 登录 ─────────────────────────────────────────────────────────────────────
async def login(page) -> bool:
    """登录领星ERP，成功返回True"""
    logger.info("🔐 正在登录领星ERP...")

    await page.goto(LOGIN_URL, timeout=NAV_TIMEOUT)
    await page.wait_for_load_state('domcontentloaded')
    await asyncio.sleep(5)
    logger.info(f"当前URL: {page.url}")
    buttons = await page.query_selector_all('button')
    for b in buttons:
        t = await b.inner_text()
        if t.strip():
            logger.info(f"  button: {t.strip()!r}")
    await asyncio.sleep(2)

    # 填写账号密码
    await page.fill('input[type="text"]', LINGXING_USERNAME)
    await page.fill('input[type="password"]', LINGXING_PASSWORD)
    await asyncio.sleep(1)

    # 点击登录按钮（兼容中英文）
    for btn_text in ['登录', 'Login']:
        try:
            btn = page.locator(f'button:has-text("{btn_text}")')
            if await btn.count() > 0:
                await btn.click(timeout=10_000)
                logger.info(f"点击登录按钮: {btn_text}")
                break
        except Exception:
            continue

    # 等待登录成功（URL变化或主页元素出现）
    try:
        await page.wait_for_url(lambda url: 'login' not in url, timeout=30_000)
        logger.info("✅ 登录成功")
        return True
    except Exception:
        # 检查是否有错误提示
        error = await page.query_selector('.el-message--error, .error-msg')
        if error:
            msg = await error.inner_text()
            logger.error(f"❌ 登录失败: {msg}")
        else:
            logger.error("❌ 登录超时，未能跳转到主页")
        return False


# ── 进入费用管理 ──────────────────────────────────────────────────────────────
async def navigate_to_fee_mgmt(page) -> bool:
    """跳转到费用管理页面（中英文兼容）"""
    logger.info("🗂️  跳转到费用管理页面...")

    # 跳转首页
    await page.goto('https://erp.lingxing.com', timeout=NAV_TIMEOUT)
    await page.wait_for_load_state('domcontentloaded', timeout=NAV_TIMEOUT)
    await asyncio.sleep(5)

    # 关闭弹窗（中英文）
    for close_text in ['稍后处理', '关闭', 'Close', 'Later']:
        try:
            btn = page.locator(f'button:has-text("{close_text}")').last
            if await btn.is_visible(timeout=2000):
                await btn.click()
                await asyncio.sleep(1)
                logger.info(f"  关闭弹窗: {close_text}")
        except Exception:
            pass

    # 点左侧财务/Finance菜单
    for menu_text in ['财务', 'Finance']:
        try:
            btn = page.locator(f'text={menu_text}').first
            if await btn.is_visible(timeout=3000):
                await btn.click()
                logger.info(f"  点击菜单: {menu_text}")
                await asyncio.sleep(2)
                break
        except Exception:
            continue

    # 点费用管理/Expense Management
    for menu_text in ['费用管理', 'Expense Management']:
        try:
            btn = page.locator(f'text={menu_text}').first
            if await btn.is_visible(timeout=3000):
                await btn.click()
                logger.info(f"  点击菜单: {menu_text}")
                await asyncio.sleep(5)
                break
        except Exception:
            continue

    # 等待导入按钮出现（中英文）
    for btn_text in ['导入费用', 'Import Expense']:
        try:
            await page.wait_for_selector(f'button:has-text("{btn_text}")', timeout=30_000)
            # 页面加载完成后，立即用JS隐藏公告弹窗
            await page.evaluate('''() => {
                const hide = () => {
                    document.querySelectorAll(
                        ".auto-dialog, .auto-height, [class*=auto-dialog]"
                    ).forEach(el => {
                        el.style.display = "none";
                        el.style.pointerEvents = "none";
                        el.style.zIndex = "-9999";
                    });
                };
                hide();
                // 持续监听并隐藏
                new MutationObserver(hide).observe(document.body, {childList: true, subtree: true});
            }''')
            await asyncio.sleep(0.5)
            logger.info(f"✅ 费用管理页面加载完成（按钮: {btn_text}）")
            return True
        except Exception:
            continue

    buttons = await page.query_selector_all('button')
    btn_texts = [await b.inner_text() for b in buttons]
    btn_texts = [t.strip() for t in btn_texts if t.strip()]
    logger.error(f"❌ 费用管理页面加载超时，当前按钮: {btn_texts[:20]}")
    return False



# ── 作废本月费用单（API方式）─────────────────────────────────────────────────
async def discard_current_month_fees(month: str) -> bool:
    """
    调用 create_fee_management 中的作废逻辑，作废指定月份已有费用单
    month: 格式 YYYY-MM
    """
    logger.info(f"🗑️  作废 {month} 已有费用单...")
    try:
        from calendar import monthrange
        year, mon = map(int, month.split('-'))
        last_day = monthrange(year, mon)[1]
        start_date = f"{year:04d}-{mon:02d}-01"
        end_date   = f"{year:04d}-{mon:02d}-{last_day:02d}"

        import create_fee_management as fee_mgr
        await fee_mgr.discard_fee_orders_by_date_range(start_date, end_date)
        logger.info("✅ 作废完成")
        return True
    except Exception as e:
        logger.error(f"❌ 作废失败: {e}", exc_info=True)
        return False


# ── 上传单个Excel文件 ─────────────────────────────────────────────────────────
async def upload_single_file(page, filepath: str, file_index: int, total: int) -> bool:
    filename = os.path.basename(filepath)
    logger.info(f"📤 上传第 {file_index}/{total} 个文件: {filename}")
    try:
        # 0. 关闭可能存在的公告弹窗，关闭后重新确认在费用管理页
        try:
            # 检查是否有公告弹窗iframe
            iframe = page.locator('#que-iframe')
            if await iframe.count() > 0 and await iframe.is_visible(timeout=2000):
                logger.info("  检测到公告弹窗，尝试关闭...")
                # 点击弹窗右上角X按钮
                close_btn = page.locator('.auto-dialog .el-dialog__headerbtn, .auto-height .el-dialog__headerbtn')
                if await close_btn.count() > 0:
                    await close_btn.first.click()
                else:
                    await page.keyboard.press('Escape')
                await asyncio.sleep(2)
                logger.info("  公告弹窗已关闭，重新导航到费用管理页")
                # 重新导航到费用管理页
                if not await navigate_to_fee_mgmt(page):
                    raise Exception("关闭公告后重新导航费用管理页失败")
        except Exception as e:
            if "关闭公告后" in str(e):
                raise
            pass

        # 1. 强制点击「导入费用」按钮（force=True跳过遮挡检测）
        clicked = False
        for btn_text in ['导入费用', 'Import Expense']:
            try:
                btn = page.locator(f'button:has-text("{btn_text}")')
                cnt = await btn.count()
                logger.info(f"  找到 {cnt} 个'{btn_text}'按钮")
                if cnt > 0:
                    await btn.first.click(force=True, timeout=ACTION_TIMEOUT)
                    logger.info(f"  已点击: {btn_text}")
                    clicked = True
                    break
            except Exception as e:
                logger.info(f"  点击{btn_text}失败: {e}")
                continue
        if not clicked:
            raise Exception("找不到导入按钮")
        await asyncio.sleep(2)
        await page.screenshot(path=f'/tmp/after_click_{file_index}.png')
        # 等待导入弹窗出现且可见
        await page.wait_for_selector('[aria-label="导入"]:visible, [aria-label="Import"]:visible', timeout=ACTION_TIMEOUT)
        await asyncio.sleep(2)

        # 2. 点击弹窗内蓝色导入按钮触发文件选择器
        dialog = page.locator('[aria-label="导入"]:visible, [aria-label="Import"]:visible').first
        async with page.expect_file_chooser(timeout=ACTION_TIMEOUT) as fc_info:
            for btn_text in ['导入费用', 'Import Expense']:
                try:
                    btn = dialog.locator(f'button:has-text("{btn_text}")')
                    if await btn.count() > 0:
                        await btn.click(timeout=ACTION_TIMEOUT)
                        break
                except Exception:
                    continue

        file_chooser = await fc_info.value
        await file_chooser.set_files(filepath)
        logger.info(f"  📁 文件已选择: {filename}")

        # 3. 等待上传完成（等待「全部导入成功」文字出现）
        try:
            # 等待进度条消失或成功文字出现
            await asyncio.sleep(10)  # 先等10秒让上传开始
            # 轮询检查是否成功（最多等5分钟）
            success = False
            for _ in range(60):
                try:
                    ok = await page.locator('text=全部导入成功').is_visible(timeout=3000)
                    if ok:
                        success = True
                        break
                except Exception:
                    pass
                try:
                    ok = await page.locator('text=Import Success').is_visible(timeout=1000)
                    if ok:
                        success = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(5)

            if not success:
                raise Exception("上传超时，未检测到成功提示")
            logger.info(f"  ✅ 文件上传成功: {filename}")

            # 关闭弹窗（点右上角×）
            try:
                await page.locator('.el-dialog__headerbtn, .el-icon-close').first.click()
            except Exception:
                await page.keyboard.press('Escape')

            # 等待弹窗完全关闭，且「导入费用」按钮重新可点击
            await asyncio.sleep(3)
            for btn_text in ['导入费用', 'Import Expense']:
                try:
                    await page.wait_for_selector(
                        f'button:has-text("{btn_text}")',
                        state='visible', timeout=15_000
                    )
                    break
                except Exception:
                    continue
            await asyncio.sleep(1)
            return True

        except Exception:
            # 检查是否有失败提示
            try:
                fail = await page.locator('text=全部导入失败, text=Import Failed').is_visible(timeout=3000)
                if fail:
                    logger.error(f"  ❌ 上传失败：领星返回导入失败")
            except Exception:
                logger.error(f"  ❌ 上传超时，未收到成功确认")

            # 关闭弹窗
            try:
                await page.locator('.el-dialog__headerbtn, .el-icon-close').first.click()
                await asyncio.sleep(3)
                await page.wait_for_selector('.el-dialog', state='hidden', timeout=10_000)
            except Exception:
                await page.keyboard.press('Escape')
                await asyncio.sleep(2)
            return False

    except Exception as e:
        logger.error(f"  ❌ 上传异常: {e}")
        try:
            await page.keyboard.press('Escape')
            await asyncio.sleep(1)
        except Exception:
            pass
        return False

async def upload_files(excel_files: List[str], month: str, skip_discard: bool = False) -> bool:
    """
    完整上传流程：作废旧单 → 登录 → 逐个上传

    Args:
        excel_files:   Excel文件路径列表（已排序）
        month:         目标月份，格式 YYYY-MM
        skip_discard:  是否跳过作废步骤
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("❌ 请先安装 playwright: pip install playwright --break-system-packages && playwright install chromium")
        return False

    if not LINGXING_USERNAME or not LINGXING_PASSWORD:
        logger.error("❌ 请在 .env 文件中配置 LINGXING_USERNAME 和 LINGXING_PASSWORD")
        return False

    # ── 步骤1：作废旧费用单（API）──
    if not skip_discard:
        ok = await discard_current_month_fees(month)
        if not ok:
            logger.error("❌ 作废失败，中止上传（避免数据重复）")
            return False
    else:
        logger.info("⏭️  跳过作废步骤（--skip-discard）")

    # ── 步骤2：浏览器上传 ──
    total = len(excel_files)
    success_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,       # 服务器环境用无头模式
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        context = await browser.new_context(
            viewport={'width': 1440, 'height': 900},
            locale='zh-CN'
        )
        page = await context.new_page()

        try:
            # 登录
            if not await login(page):
                return False

            # 跳转费用管理页
            if not await navigate_to_fee_mgmt(page):
                return False

            # 逐个上传
            results = []
            for i, filepath in enumerate(excel_files, start=1):
                ok = await upload_single_file(page, filepath, i, total)
                results.append(ok)
                if ok:
                    success_count += 1
                else:
                    logger.warning(f"  ⚠️  第 {i} 个文件失败，继续下一个...")

                # 每个文件之间等待3秒，让领星处理完成
                if i < total:
                    await asyncio.sleep(3)

        finally:
            await context.close()
            await browser.close()

    # ── 重试失败的文件（最多2次）──
    if success_count < total:
        failed_files = [f for f, ok in zip(excel_files, results) if not ok]
        logger.info(f"\n🔄 开始重试 {len(failed_files)} 个失败文件...")
        
        for retry_round in range(2):
            if not failed_files:
                break
            logger.info(f"  第 {retry_round + 1} 次重试，共 {len(failed_files)} 个文件")
            still_failed = []
            for i, filepath in enumerate(failed_files, 1):
                await asyncio.sleep(5)
                ok = await upload_single_file(page, filepath, i, len(failed_files))
                if ok:
                    success_count += 1
                    logger.info(f"  ✅ 重试成功: {os.path.basename(filepath)}")
                else:
                    still_failed.append(filepath)
                    logger.warning(f"  ⚠️  重试失败: {os.path.basename(filepath)}")
            failed_files = still_failed

        if failed_files:
            logger.error(f"❌ 以下文件重试后仍失败:")
            for f in failed_files:
                logger.error(f"   {os.path.basename(f)}")

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"📋 上传汇总：成功 {success_count}/{total} 个文件")
    logger.info("=" * 60)

    return success_count == total


# ── 入口 ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='自动上传费用单Excel到领星ERP')
    parser.add_argument(
        '--files', nargs='+', default=None,
        help='指定要上传的Excel文件路径（空格分隔）'
    )
    parser.add_argument(
        '--dir', type=str, default=None,
        help='从目录中自动查找Excel文件（与 --month 配合使用）'
    )
    parser.add_argument(
        '--month', type=str, default=None,
        help='目标月份，格式：YYYY-MM，默认：本月（用于作废和文件筛选）'
    )
    parser.add_argument(
        '--skip-discard', action='store_true',
        help='跳过作废旧费用单步骤'
    )
    args = parser.parse_args()

    target_month = args.month or datetime.now().strftime('%Y-%m')

    # 确定要上传的文件列表
    if args.files:
        files = sorted(args.files)
    elif args.dir:
        pattern = os.path.join(args.dir, f"fee_{target_month}_*.xlsx")
        files = sorted(glob.glob(pattern))
        if not files:
            logger.error(f"❌ 在 {args.dir} 中找不到 {target_month} 的Excel文件")
            sys.exit(1)
    else:
        # 默认在脚本同目录的 fee_excel_output 文件夹里找
        default_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fee_excel_output')
        pattern = os.path.join(default_dir, f"fee_{target_month}_*.xlsx")
        files = sorted(glob.glob(pattern))
        if not files:
            logger.error(f"❌ 找不到可上传的文件，请用 --files 或 --dir 指定")
            sys.exit(1)

    logger.info(f"📂 找到 {len(files)} 个文件待上传：")
    for f in files:
        logger.info(f"   {os.path.basename(f)}")

    try:
        ok = asyncio.run(upload_files(
            excel_files=files,
            month=target_month,
            skip_discard=args.skip_discard
        ))
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        logger.warning("⚠️  用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 执行失败: {e}", exc_info=True)
        sys.exit(1)
