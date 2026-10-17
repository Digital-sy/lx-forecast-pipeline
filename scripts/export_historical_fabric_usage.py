#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""重新拉取领星采购单并导出 2025-06 起历史采购实际面料用量。"""
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common import get_logger, settings
from common.database import db_cursor
from jobs.feishu.historical_fabric_usage import (
    LOSS_FACTOR,
    aggregate_usage,
    expand_purchase_orders,
    export_excel,
    month_range,
    normalize_fabric_map,
)
from lingxing import LingxingTokenProvider, OpenApiBase

logger = get_logger('historical_fabric_usage_export')

ROUTE = '/erp/sc/routing/data/local_inventory/purchaseOrderList'
TOKEN_EXPIRED_CODES = {2001003}
DEFAULT_API_START = '2024-01-01'
DEFAULT_ANALYSIS_START = '2025-06-01'
MAX_PAGE_SIZE = 500
MAX_RETRIES = 5


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='重新拉取领星采购单，按采购下单月份计算截至当前到货成衣对应的实际面料用量。'
    )
    p.add_argument('--api-start-date', default=DEFAULT_API_START,
                   help='接口按 create_time 查询的开始日期，默认 2024-01-01')
    p.add_argument('--analysis-start-date', default=DEFAULT_ANALYSIS_START,
                   help='最终按 order_time 纳入统计的开始日期，默认 2025-06-01')
    p.add_argument('--end-date', default=date.today().strftime('%Y-%m-%d'),
                   help='接口和统计结束日期，默认今天')
    p.add_argument('--page-size', type=int, default=MAX_PAGE_SIZE,
                   help='接口分页大小，默认/最大 500')
    p.add_argument('--delay', type=float, default=settings.COLLECTION_DELAY_SECONDS,
                   help='分页请求之间的安全间隔秒数，默认使用 COLLECTION_DELAY_SECONDS')
    p.add_argument('--estimate-only', action='store_true',
                   help='只请求一次 length=1 获取 total 并估算页数，不执行全量拉取')
    p.add_argument('--output', default='', help='Excel 输出路径；默认 exports/历史采购实际面料用量_YYYYMMDD.xlsx')
    args = p.parse_args()

    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        p.error('--page-size 必须在 1~500 之间')
    if args.delay < 0:
        p.error('--delay 不能小于 0')
    for name in ('api_start_date', 'analysis_start_date', 'end_date'):
        try:
            datetime.strptime(getattr(args, name), '%Y-%m-%d')
        except ValueError:
            p.error(f'--{name.replace("_", "-")} 必须是 YYYY-MM-DD')
    if args.api_start_date > args.end_date:
        p.error('--api-start-date 不能晚于 --end-date')
    if args.analysis_start_date > args.end_date:
        p.error('--analysis-start-date 不能晚于 --end-date')
    return args


def _plain_response(resp: Any) -> Dict[str, Any]:
    if hasattr(resp, 'model_dump'):
        return resp.model_dump()
    if hasattr(resp, 'dict'):
        return resp.dict()
    if isinstance(resp, Mapping):
        return dict(resp)
    raise TypeError(f'无法识别的响应类型: {type(resp).__name__}')


async def request_page(
    api: OpenApiBase,
    token_provider: LingxingTokenProvider,
    start_date: str,
    end_date: str,
    offset: int,
    length: int,
) -> Tuple[List[Dict[str, Any]], int]:
    body = {
        'search_field_time': 'create_time',
        'start_date': start_date,
        'end_date': end_date,
        'offset': offset,
        'length': length,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            token = await token_provider.get_token()
            resp = await api.request(token, ROUTE, 'POST', req_body=body, timeout=120)
            data = _plain_response(resp)
            code = data.get('code')
            if code == 0:
                rows = data.get('data') or []
                if not isinstance(rows, list):
                    raise RuntimeError(f'接口 data 不是数组: {type(rows).__name__}')
                total = int(data.get('total') or 0)
                return [x for x in rows if isinstance(x, dict)], total

            if code in TOKEN_EXPIRED_CODES:
                logger.warning('Token 已失效，强制刷新后重试')
                await token_provider.get_token(force_refresh=True)
                continue
            raise RuntimeError(
                f"领星接口返回失败 code={code}, message={data.get('message')}, request_id={data.get('request_id')}"
            )
        except Exception as exc:
            last_error = exc
            if attempt >= MAX_RETRIES:
                break
            sleep_s = min(30.0, 2.0 ** (attempt - 1))
            logger.warning(f'请求失败，第 {attempt}/{MAX_RETRIES} 次：{exc}；{sleep_s:.0f}s 后重试')
            await asyncio.sleep(sleep_s)
    raise RuntimeError(f'采购单接口请求最终失败 offset={offset}: {last_error}')


async def estimate_total(
    api: OpenApiBase,
    token_provider: LingxingTokenProvider,
    start_date: str,
    end_date: str,
    page_size: int,
    delay: float,
) -> int:
    started = time.perf_counter()
    _, total = await request_page(api, token_provider, start_date, end_date, 0, 1)
    elapsed = time.perf_counter() - started
    pages = math.ceil(total / page_size) if total else 0
    enforced_wait = max(0, pages - 1) * delay
    logger.info('=' * 72)
    logger.info(f'采购单总数 total={total:,}')
    logger.info(f'正式分页：page_size={page_size}，预计 {pages:,} 页')
    logger.info(f'本次 length=1 探测请求耗时：{elapsed:.2f}s')
    logger.info(f'仅分页安全等待的理论下限：{enforced_wait / 60:.2f} 分钟')
    logger.info('真实全量时间还会叠加每页接口响应、网络传输、重试和 Excel 生成时间。')
    logger.info('=' * 72)
    return total


def read_fabric_sources() -> Tuple[Dict[str, List[Dict[str, Any]]], Sequence[str]]:
    logger.info('读取面料核价表与定制面料参数...')
    with db_cursor(dictionary=True) as cur:
        cur.execute("""
            SELECT SPU, 面料, 单件用量
            FROM `面料核价表`
            WHERE SPU IS NOT NULL AND SPU != ''
              AND 面料 IS NOT NULL AND 面料 != ''
        """)
        fabric_rows = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT 面料
            FROM `定制面料参数`
            WHERE 面料 IS NOT NULL AND 面料 != ''
        """)
        custom_rows = cur.fetchall()

    fabric_map = normalize_fabric_map(fabric_rows)
    custom_fabrics = [str(r.get('面料') or '').strip() for r in custom_rows if str(r.get('面料') or '').strip()]
    logger.info(f'面料核价映射：{len(fabric_map):,} 个 SPU；定制面料：{len(custom_fabrics):,} 种')
    return fabric_map, custom_fabrics


async def fetch_and_calculate(args: argparse.Namespace) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    fabric_map, custom_fabrics = read_fabric_sources()
    analysis_start = datetime.strptime(args.analysis_start_date, '%Y-%m-%d').date()
    analysis_end = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    config = settings.lingxing_config
    api = OpenApiBase(
        host=config['host'],
        app_id=config['app_id'],
        app_secret=config['app_secret'],
        proxy_url=config['proxy_url'] or None,
    )
    token_provider = LingxingTokenProvider(api, refresh_margin_seconds=300, logger=logger)

    detail_rows: List[Dict[str, Any]] = []
    unmapped_rows: List[Dict[str, Any]] = []
    offset = 0
    total = None
    started = time.perf_counter()
    page_no = 0

    while total is None or offset < total:
        page_started = time.perf_counter()
        orders, page_total = await request_page(
            api, token_provider, args.api_start_date, args.end_date, offset, args.page_size
        )
        page_no += 1
        if total is None:
            total = page_total
            total_pages = math.ceil(total / args.page_size) if total else 0
            logger.info(f'采购单 total={total:,}，预计 {total_pages:,} 页')
        elif page_total != total:
            logger.warning(f'接口 total 在拉取过程中变化：首次={total}, 当前={page_total}')
            total = max(total, page_total)

        page_detail, page_unmapped = expand_purchase_orders(
            orders=orders,
            analysis_start=analysis_start,
            analysis_end=analysis_end,
            fabric_map=fabric_map,
            custom_fabrics=custom_fabrics,
            loss_factor=LOSS_FACTOR,
        )
        detail_rows.extend(page_detail)
        unmapped_rows.extend(page_unmapped)

        elapsed = time.perf_counter() - started
        page_elapsed = time.perf_counter() - page_started
        fetched_until = min(offset + len(orders), total or 0)
        total_pages = math.ceil((total or 0) / args.page_size) if total else 0
        avg_page = elapsed / page_no
        remaining_pages = max(0, total_pages - page_no)
        rough_remaining = remaining_pages * (avg_page + args.delay)
        logger.info(
            f'页 {page_no}/{total_pages or "?"}：采购单 {fetched_until:,}/{(total or 0):,}，'
            f'本页 {page_elapsed:.2f}s，核算明细累计 {len(detail_rows):,}，'
            f'未映射 {len(unmapped_rows):,}，粗估剩余 {rough_remaining/60:.1f} 分钟'
        )

        if not orders:
            if total and offset < total:
                raise RuntimeError(f'offset={offset} 返回空页，但 total={total}，为避免漏数停止执行')
            break

        offset += args.page_size
        if total and offset < total and args.delay > 0:
            await asyncio.sleep(args.delay)

    logger.info(f'API 拉取+计算完成，耗时 {(time.perf_counter()-started)/60:.2f} 分钟')
    return detail_rows, unmapped_rows, int(total or 0)


async def main_async() -> int:
    args = parse_args()
    if not settings.validate():
        return 2

    config = settings.lingxing_config
    api = OpenApiBase(
        host=config['host'],
        app_id=config['app_id'],
        app_secret=config['app_secret'],
        proxy_url=config['proxy_url'] or None,
    )
    token_provider = LingxingTokenProvider(api, refresh_margin_seconds=300, logger=logger)

    if args.estimate_only:
        await estimate_total(
            api, token_provider, args.api_start_date, args.end_date, args.page_size, args.delay
        )
        return 0

    detail_rows, unmapped_rows, total = await fetch_and_calculate(args)
    analysis_start = datetime.strptime(args.analysis_start_date, '%Y-%m-%d').date()
    analysis_end = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    months = month_range(analysis_start, analysis_end)
    fabric_rows, spu_rows = aggregate_usage(detail_rows, months)

    output = Path(args.output).expanduser() if args.output else (
        ROOT / 'exports' / f'历史采购实际面料用量_{datetime.now().strftime("%Y%m%d")}.xlsx'
    )
    if not output.is_absolute():
        output = (ROOT / output).resolve()

    export_excel(output, fabric_rows, spu_rows, detail_rows, unmapped_rows, months)
    logger.info('=' * 72)
    logger.info(f'采购单总数（接口 create_time 范围）：{total:,}')
    logger.info(f'纳入面料核算明细：{len(detail_rows):,} 行')
    logger.info(f'未映射采购明细：{len(unmapped_rows):,} 行')
    logger.info(f'面料汇总：{len(fabric_rows):,} 行；款号+面料：{len(spu_rows):,} 行')
    logger.info(f'Excel：{output}')
    logger.info('=' * 72)
    return 0


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.warning('用户中断执行')
        return 130
    except Exception as exc:
        logger.error(f'历史采购实际面料用量执行失败：{exc}', exc_info=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
