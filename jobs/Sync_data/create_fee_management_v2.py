#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
费用单管理 - 从利润报表创建费用单（按月汇总）
参考其他文件的API和token逻辑，使用更简洁的实现
"""
import asyncio
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from calendar import monthrange
from collections import defaultdict

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase

# 获取日志记录器
logger = get_logger('fee_management_v2')

# 重试配置
MAX_RETRIES = 5  # 最大重试次数
RETRY_DELAY = 10  # 重试延迟（秒）
REQUEST_DELAY = 8  # 请求间隔（秒）
MAX_FEE_ITEMS_PER_ORDER = 50  # 每个费用单最多包含的费用明细项数量
REST_BATCH_INTERVAL = 20  # 每创建N批后休息一次
REST_DURATION = 30  # 休息时长（秒）


async def get_fee_types(op_api: OpenApiBase, token: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    查询费用类型列表
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        
    Returns:
        Tuple[Optional[List], Optional[str]]: (费用类型列表, 错误信息)
        如果token过期，返回 (None, "TOKEN_EXPIRED")
    """
    req_body = {}
    
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"查询费用类型列表，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token,
                "/bd/fee/management/open/feeManagement/otherFee/type",
                "POST",
                req_body=req_body
            )
            
            try:
                result = resp.model_dump()
            except AttributeError:
                result = resp.dict()
            
            code = result.get('code', 0)
            message = result.get('msg', '') or result.get('message', '')
            
            if code == 3001008:
                wait_time = RETRY_DELAY * (2 ** retry)
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                await asyncio.sleep(wait_time)
                continue
            
            if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                logger.error(f"🔑 Token错误 (code={code}): {message}")
                return None, "TOKEN_EXPIRED"
            
            if code != 0:
                logger.error(f"❌ API返回错误: code={code}, message={message}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    return None, f"API_ERROR: {message}"
            
            data = result.get('data', [])
            if data is None:
                data = []
            
            logger.info(f"✅ 查询费用类型列表成功，共 {len(data)} 个费用类型")
            return data, None
            
        except Exception as e:
            logger.error(f"❌ 查询费用类型列表异常: {str(e)}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                await asyncio.sleep(wait_time)
                continue
            else:
                return None, f"EXCEPTION: {str(e)}"
    
    return None, "MAX_RETRIES_EXCEEDED"


async def get_fee_list(
    op_api: OpenApiBase,
    token: str,
    offset: int = 0,
    length: int = 20,
    date_type: str = "date",
    start_date: str = None,
    end_date: str = None,
    other_fee_type_ids: List[int] = None,
    status_order: int = None
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    查询费用明细列表
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        offset: 分页偏移量
        length: 分页长度
        date_type: 时间类型：gmt_create 创建日期，date 分摊日期
        start_date: 开始时间，格式：Y-m-d
        end_date: 结束时间，格式：Y-m-d
        other_fee_type_ids: 费用类型id列表
        status_order: 单据状态：1 待提交, 2 待审批, 3 已处理, 4 已驳回, 5 已作废
        
    Returns:
        Tuple[Optional[Dict], Optional[str]]: (查询结果, 错误信息)
        如果token过期，返回 (None, "TOKEN_EXPIRED")
    """
    req_body = {
        "offset": offset,
        "length": length,
        "date_type": date_type,
        "start_date": start_date,
        "end_date": end_date
    }
    
    if other_fee_type_ids:
        req_body["other_fee_type_ids"] = other_fee_type_ids
    if status_order is not None:
        req_body["status_order"] = status_order
    
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"查询费用列表，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token,
                "/bd/fee/management/open/feeManagement/otherFee/list",
                "POST",
                req_body=req_body
            )
            
            try:
                result = resp.model_dump()
            except AttributeError:
                result = resp.dict()
            
            code = result.get('code', 0)
            message = result.get('msg', '') or result.get('message', '')
            
            if code == 3001008:
                wait_time = RETRY_DELAY * (2 ** retry)
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                await asyncio.sleep(wait_time)
                continue
            
            if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                logger.error(f"🔑 Token错误 (code={code}): {message}")
                return None, "TOKEN_EXPIRED"
            
            if code != 0:
                logger.error(f"❌ API返回错误: code={code}, message={message}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    return None, f"API_ERROR: {message}"
            
            return result, None
            
        except Exception as e:
            logger.error(f"❌ 查询费用列表异常: {str(e)}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                await asyncio.sleep(wait_time)
                continue
            else:
                return None, f"EXCEPTION: {str(e)}"
    
    return None, "MAX_RETRIES_EXCEEDED"


async def discard_fee_orders(
    op_api: OpenApiBase,
    token: str,
    numbers: List[str]
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    作废费用单
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        numbers: 费用单号列表，上限200
        
    Returns:
        Tuple[Optional[Dict], Optional[str]]: (作废结果, 错误信息)
        如果token过期，返回 (None, "TOKEN_EXPIRED")
    """
    req_body = {
        "numbers": numbers
    }
    
    logger.info(f"准备作废 {len(numbers)} 个费用单: {numbers}")
    
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"作废费用单，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token,
                "/bd/fee/management/open/feeManagement/otherFee/discard",
                "POST",
                req_body=req_body
            )
            
            try:
                result = resp.model_dump()
            except AttributeError:
                result = resp.dict()
            
            code = result.get('code', 0)
            message = result.get('msg', '') or result.get('message', '')
            
            if code == 3001008:
                wait_time = RETRY_DELAY * (2 ** retry)
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                await asyncio.sleep(wait_time)
                continue
            
            if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                logger.error(f"🔑 Token错误 (code={code}): {message}")
                return None, "TOKEN_EXPIRED"
            
            if code != 0:
                logger.error(f"❌ API返回错误: code={code}, message={message}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    return None, f"API_ERROR: {message}"
            
            logger.info(f"✅ 费用单作废成功!")
            return result, None
            
        except Exception as e:
            logger.error(f"❌ 作废费用单异常: {str(e)}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                await asyncio.sleep(wait_time)
                continue
            else:
                return None, f"EXCEPTION: {str(e)}"
    
    return None, "MAX_RETRIES_EXCEEDED"


async def create_fee_order(
    op_api: OpenApiBase,
    token: str,
    submit_type: int,
    dimension: int,
    apportion_rule: int,
    is_request_pool: int,
    remark: str,
    fee_items: List[Dict[str, Any]]
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    创建费用单
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        submit_type: 提交类型：1 暂存，2 提交
        dimension: 分摊维度：1 msku, 2 asin, 3 店铺, 4 父asin, 5 sku, 6 企业
        apportion_rule: 分摊规则：0 无, 1 按销售额, 2 按销量, 3 店铺均摊后按销售额占比分摊, 4 店铺均摊后按销量占比分摊
        is_request_pool: 是否请款：0 否，1 是
        remark: 费用单备注
        fee_items: 费用明细项列表
        
    Returns:
        Tuple[Optional[Dict], Optional[str]]: (创建结果, 错误信息)
        如果token过期，返回 (None, "TOKEN_EXPIRED")
    """
    req_body = {
        "submit_type": submit_type,
        "dimension": dimension,
        "apportion_rule": apportion_rule,
        "is_request_pool": is_request_pool,
        "remark": remark,
        "fee_items": fee_items
    }
    
    logger.info(f"📝 准备创建费用单: {remark}, 包含 {len(fee_items)} 个费用明细项")
    
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"创建费用单，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token,
                "/bd/fee/management/open/feeManagement/otherFee/create",
                "POST",
                req_body=req_body
            )
            
            try:
                result = resp.model_dump()
            except AttributeError:
                result = resp.dict()
            
            code = result.get('code', 0)
            message = result.get('msg', '') or result.get('message', '')
            
            if code == 3001008:
                wait_time = RETRY_DELAY * (2 ** retry)
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                await asyncio.sleep(wait_time)
                continue
            
            if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                logger.error(f"🔑 Token/签名错误 (code={code}): {message}")
                # 前2次重试：可能是速率限制，等待后重试（不刷新token）
                if retry < 2:
                    wait_time = RETRY_DELAY * (retry + 1) * 2
                    logger.warning(f"⚠️  签名错误可能是速率限制，等待 {wait_time} 秒后重试（不刷新token）...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # 第3次及以后：返回TOKEN_EXPIRED，让主函数处理
                    return None, "TOKEN_EXPIRED"
            
            if code != 0:
                logger.error(f"❌ API返回错误: code={code}, message={message}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    return None, f"API_ERROR: {message}"
            
            logger.info(f"✅ 费用单创建成功: {remark}")
            return result, None
            
        except Exception as e:
            logger.error(f"❌ 创建费用单异常: {str(e)}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                await asyncio.sleep(wait_time)
                continue
            else:
                return None, f"EXCEPTION: {str(e)}"
    
    return None, "MAX_RETRIES_EXCEEDED"


def fetch_profit_report_data(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    从数据库读取利润报表数据（按月汇总）
    
    Args:
        start_date: 开始日期，格式：Y-m-d
        end_date: 结束日期，格式：Y-m-d
    
    Returns:
        List[Dict]: 利润报表数据列表，已按月汇总
    """
    try:
        with db_cursor() as cursor:
            sql = """
                SELECT 
                    `MSKU`,
                    `店铺id`,
                    DATE_FORMAT(`统计日期`, '%%Y-%%m') AS `年月`,
                    SUM(`商品成本附加费`) AS `商品成本附加费`,
                    SUM(`头程成本附加费`) AS `头程成本附加费`,
                    SUM(`录入费用单头程`) AS `录入费用单头程`,
                    SUM(`汇损`) AS `汇损`
                FROM `利润报表`
                WHERE `统计日期` >= %s
                  AND `统计日期` <= %s
                  AND (
                      (`商品成本附加费` IS NOT NULL AND `商品成本附加费` != 0) OR
                      (`头程成本附加费` IS NOT NULL AND `头程成本附加费` != 0) OR
                      (`录入费用单头程` IS NOT NULL AND `录入费用单头程` != 0) OR
                      (`汇损` IS NOT NULL AND `汇损` != 0)
                  )
                GROUP BY `MSKU`, `店铺id`, DATE_FORMAT(`统计日期`, '%%Y-%%m')
                ORDER BY `年月`, `店铺id`, `MSKU`
            """
            cursor.execute(sql, (start_date, end_date))
            records = cursor.fetchall()
            
            if records:
                unique_msku_shop = set()
                unique_months = set()
                for record in records:
                    msku = record.get('MSKU', '').strip()
                    shop_id = record.get('店铺id')
                    year_month = record.get('年月')
                    
                    if msku and shop_id:
                        key = (msku, str(shop_id))
                        unique_msku_shop.add(key)
                    
                    if year_month:
                        unique_months.add(year_month)
                
                logger.info(f"✅ 从数据库读取到 {len(records)} 条按月汇总的利润报表数据")
                logger.info(f"   包含 {len(unique_msku_shop)} 个不同的(MSKU, 店铺ID)组合")
                logger.info(f"   涉及月份: {sorted(unique_months)}")
            
            return records
    except Exception as e:
        logger.error(f"❌ 读取利润报表数据失败: {str(e)}")
        return []


async def discard_existing_fee_orders(
    op_api: OpenApiBase,
    token_resp,
    start_date: str,
    end_date: str,
    fee_type_ids: List[int]
) -> Tuple[bool, Optional[str]]:
    """
    作废指定日期范围内指定费用类型的费用单
    
    Args:
        op_api: OpenAPI客户端
        token_resp: Token响应对象
        start_date: 开始日期
        end_date: 结束日期
        fee_type_ids: 费用类型ID列表
        
    Returns:
        Tuple[bool, Optional[str]]: (是否成功, 错误信息)
    """
    logger.info("=" * 80)
    logger.info("步骤1: 查询并作废已有费用单")
    logger.info(f"将作废日期范围 {start_date} 至 {end_date} 内所有【已处理】状态的费用单")
    logger.info(f"费用类型: 商品成本附加费、头程成本附加费、头程费用、汇损（仅这四个费用类型）")
    logger.info(f"费用类型ID: {fee_type_ids}")
    logger.info("=" * 80)
    
    token = token_resp.access_token
    query_batch_size = 500
    discard_batch_size = 200
    
    all_numbers = []
    offset = 0
    
    # 查询所有需要作废的费用单
    while True:
        result, error = await get_fee_list(
            op_api, token,
            offset=offset,
            length=query_batch_size,
            date_type="date",
            start_date=start_date,
            end_date=end_date,
            other_fee_type_ids=fee_type_ids,
            status_order=3  # 只查询"已处理"状态
        )
        
        if error == "TOKEN_EXPIRED":
            return False, "TOKEN_EXPIRED"
        elif error:
            logger.error(f"❌ 查询费用单列表失败: {error}")
            return False, error
        
        if not result:
            break
        
        data = result.get('data', {})
        records = data.get('records', [])
        total = data.get('total', 0)
        
        if not records:
            break
        
        for record in records:
            number = record.get('number')
            if number:
                all_numbers.append(number)
        
        logger.info(f"  已查询 {len(all_numbers)}/{total} 个费用单")
        
        if len(records) < query_batch_size:
            break
        
        offset += query_batch_size
        await asyncio.sleep(REQUEST_DELAY)
    
    if not all_numbers:
        logger.info("  没有需要作废的费用单")
        return True, None
    
    # 分批作废
    logger.info(f"\n开始作废 {len(all_numbers)} 个费用单...")
    total_discarded = 0
    
    for i in range(0, len(all_numbers), discard_batch_size):
        batch_numbers = all_numbers[i:i + discard_batch_size]
        batch_num = i // discard_batch_size + 1
        total_batches = (len(all_numbers) + discard_batch_size - 1) // discard_batch_size
        
        logger.info(f"  准备作废第 {batch_num}/{total_batches} 批，共 {len(batch_numbers)} 个费用单...")
        
        result, error = await discard_fee_orders(op_api, token, batch_numbers)
        
        if error == "TOKEN_EXPIRED":
            return False, "TOKEN_EXPIRED"
        elif error:
            logger.error(f"  ❌ 作废失败: {error}")
            return False, error
        
        total_discarded += len(batch_numbers)
        logger.info(f"  ✅ 成功作废 {len(batch_numbers)} 个费用单（累计 {total_discarded} 个）")
        
        await asyncio.sleep(REQUEST_DELAY)
    
    logger.info(f"\n✅ 作废完成：成功作废 {total_discarded} 个费用单")
    return True, None


async def create_fee_orders_from_profit_report(
    op_api: OpenApiBase,
    token_resp,
    profit_data: List[Dict[str, Any]],
    fee_type_ids: Dict[str, int],
    progress_info: Optional[Dict] = None
) -> Tuple[int, Optional[str], Optional[Dict]]:
    """
    根据利润报表数据创建费用单（按月汇总）
    
    Args:
        op_api: OpenAPI客户端
        token_resp: Token响应对象
        profit_data: 利润报表数据列表（已按月汇总）
        fee_type_ids: 费用类型ID字典
        progress_info: 进度信息，用于从失败点继续。格式：
            {
                'processed_groups': set((year_month, shop_id), ...),  # 已完全处理的组
                'current_group': (year_month, shop_id),  # 当前处理的组
                'current_batch_idx': int,  # 当前组已处理的批次索引（从1开始）
            }
        
    Returns:
        Tuple[int, Optional[str], Optional[Dict]]: (成功创建的费用单数量, 错误信息, 进度信息)
    """
    logger.info("=" * 80)
    logger.info("步骤2: 根据利润报表数据创建费用单（按月汇总）")
    logger.info("=" * 80)
    
    token = token_resp.access_token
    
    # 初始化进度信息
    if progress_info is None:
        progress_info = {
            'processed_groups': set(),
            'current_group': None,
            'current_batch_idx': 0
        }
    
    processed_groups = progress_info.get('processed_groups', set())
    current_group = progress_info.get('current_group')
    current_batch_idx = progress_info.get('current_batch_idx', 0)
    
    # 按年月和店铺ID分组
    grouped_data = defaultdict(list)
    
    for record in profit_data:
        year_month = record.get('年月')
        shop_id = record.get('店铺id')
        msku = record.get('MSKU', '').strip()
        
        if not year_month or not shop_id or not msku:
            continue
        
        key = (str(year_month), str(shop_id))
        grouped_data[key].append(record)
    
    logger.info(f"  共 {len(profit_data)} 条数据，按年月和店铺分组后共 {len(grouped_data)} 组")
    if processed_groups:
        logger.info(f"  📍 从进度恢复：已处理 {len(processed_groups)} 个组，当前组: {current_group}, 当前批次: {current_batch_idx}")
    
    success_count = 0
    total_count = 0
    skip_to_current = current_group is not None
    
    # 按年月和店铺分组创建费用单
    for (year_month, shop_id), records in grouped_data.items():
        group_key = (str(year_month), str(shop_id))
        
        # 如果这个组已经处理完成，跳过
        if group_key in processed_groups:
            continue
        
        # 如果还没到当前组，跳过
        if skip_to_current:
            if group_key != current_group:
                continue
            else:
                skip_to_current = False  # 找到当前组，开始处理
                logger.info(f"\n📍 从进度恢复：继续处理组 {group_key}，从第 {current_batch_idx + 1} 批开始")
        
        total_count += 1
        
        logger.info(f"\n处理第 {total_count}/{len(grouped_data)} 组：年月={year_month}, 店铺ID={shop_id}, 包含 {len(records)} 条记录")
        
        # 构建费用明细项
        msku_fees = {}
        for record in records:
            msku = record.get('MSKU', '').strip()
            if not msku:
                continue
            
            msku_fees[msku] = {
                '商品成本附加费': float(record.get('商品成本附加费', 0) or 0),
                '头程成本附加费': float(record.get('头程成本附加费', 0) or 0),
                '录入费用单头程': float(record.get('录入费用单头程', 0) or 0),
                '汇损': float(record.get('汇损', 0) or 0)
            }
        
        fee_items = []
        for msku, fees in msku_fees.items():
            # 商品成本附加费
            if fees['商品成本附加费'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": year_month,
                    "other_fee_type_id": fee_type_ids['商品成本附加费_id'],
                    "fee": fees['商品成本附加费'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-ProductCost"
                })
            
            # 头程成本附加费
            if fees['头程成本附加费'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": year_month,
                    "other_fee_type_id": fee_type_ids['头程成本附加费_id'],
                    "fee": fees['头程成本附加费'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-InboundCost"
                })
            
            # 录入费用单头程（对应头程费用）
            if fees['录入费用单头程'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": year_month,
                    "other_fee_type_id": fee_type_ids['头程费用_id'],
                    "fee": fees['录入费用单头程'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-InboundFee"
                })
            
            # 汇损
            if fees['汇损'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": year_month,
                    "other_fee_type_id": fee_type_ids['汇损_id'],
                    "fee": fees['汇损'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-ExchangeLoss"
                })
        
        if not fee_items:
            logger.warning(f"  ⚠️  跳过：该组没有需要创建的费用项")
            continue
        
        # 分批创建
        if len(fee_items) > MAX_FEE_ITEMS_PER_ORDER:
            logger.info(f"  ⚠️  费用明细项数量({len(fee_items)})超过限制({MAX_FEE_ITEMS_PER_ORDER})，将分批创建")
            
            # 如果是从进度恢复，跳过已处理的批次
            start_batch_idx = current_batch_idx if group_key == current_group else 0
            
            batch_idx = 0
            for i in range(0, len(fee_items), MAX_FEE_ITEMS_PER_ORDER):
                batch_idx += 1
                
                # 跳过已成功处理的批次（从start_batch_idx+1开始，因为start_batch_idx是已处理的最后一个批次）
                if batch_idx <= start_batch_idx:
                    logger.info(f"  ⏭️  跳过第 {batch_idx} 批（已处理）")
                    continue
                
                batch_items = fee_items[i:i + MAX_FEE_ITEMS_PER_ORDER]
                total_batches = (len(fee_items) + MAX_FEE_ITEMS_PER_ORDER - 1) // MAX_FEE_ITEMS_PER_ORDER
                
                logger.info(f"  创建第 {batch_idx}/{total_batches} 批，包含 {len(batch_items)} 个费用明细项")
                
                result, error = await create_fee_order(
                    op_api, token,
                    submit_type=2,
                    dimension=1,
                    apportion_rule=2,
                    is_request_pool=0,
                    remark=f"Auto-{year_month}-{batch_idx}",
                    fee_items=batch_items
                )
                
                if error == "TOKEN_EXPIRED":
                    # 保存进度信息：当前批次失败，需要重试当前批次
                    progress_info = {
                        'processed_groups': processed_groups.copy(),
                        'current_group': group_key,
                        'current_batch_idx': batch_idx - 1  # 已处理到batch_idx-1，需要从batch_idx开始（重试当前批次）
                    }
                    return success_count, "TOKEN_EXPIRED", progress_info
                elif error:
                    logger.error(f"  ❌ 第 {batch_idx} 批费用单创建失败: {error}")
                    # 保存进度信息：当前批次失败，需要重试当前批次
                    progress_info = {
                        'processed_groups': processed_groups.copy(),
                        'current_group': group_key,
                        'current_batch_idx': batch_idx - 1
                    }
                    return success_count, error, progress_info
                
                success_count += 1
                
                # 每N批后休息一次
                if batch_idx % REST_BATCH_INTERVAL == 0:
                    logger.info(f"  ⏸️  已创建 {batch_idx} 批，休息 {REST_DURATION} 秒...")
                    await asyncio.sleep(REST_DURATION)
                
                await asyncio.sleep(REQUEST_DELAY)
            
            # 当前组处理完成
            processed_groups.add(group_key)
        else:
            # 如果是从进度恢复且是当前组，且已经处理过（单批），跳过
            if group_key == current_group and current_batch_idx >= 1:
                logger.info(f"  ⏭️  跳过：该组已处理完成（单批）")
                processed_groups.add(group_key)
                continue
            
            result, error = await create_fee_order(
                op_api, token,
                submit_type=2,
                dimension=1,
                apportion_rule=2,
                is_request_pool=0,
                remark=f"Auto-{year_month}",
                fee_items=fee_items
            )
            
            if error == "TOKEN_EXPIRED":
                # 保存进度信息
                progress_info = {
                    'processed_groups': processed_groups.copy(),
                    'current_group': group_key,
                    'current_batch_idx': 0
                }
                return success_count, "TOKEN_EXPIRED", progress_info
            elif error:
                logger.error(f"  ❌ 费用单创建失败: {error}")
                progress_info = {
                    'processed_groups': processed_groups.copy(),
                    'current_group': group_key,
                    'current_batch_idx': 0
                }
                return success_count, error, progress_info
            
            success_count += 1
            processed_groups.add(group_key)
            await asyncio.sleep(REQUEST_DELAY)
    
    logger.info(f"\n✅ 费用单创建完成：成功创建 {success_count} 个费用单")
    return success_count, None, None


async def main(start_date: str = None, end_date: str = None):
    """
    主函数 - 从数据库读取利润报表并创建费用单（按月汇总）
    
    Args:
        start_date: 开始日期，格式：Y-m-d，默认：上个月1号
        end_date: 结束日期，格式：Y-m-d，默认：上个月最后一天
    """
    # 确定日期范围（默认为上个月）
    if end_date is None or start_date is None:
        today = date.today()
        
        if today.month == 1:
            last_month_year = today.year - 1
            last_month = 12
        else:
            last_month_year = today.year
            last_month = today.month - 1
        
        last_month_first_day = date(last_month_year, last_month, 1)
        last_day_of_month = monthrange(last_month_year, last_month)[1]
        last_month_last_day = date(last_month_year, last_month, last_day_of_month)
        
        if start_date is None:
            start_date = last_month_first_day.strftime('%Y-%m-%d')
        if end_date is None:
            end_date = last_month_last_day.strftime('%Y-%m-%d')
    
    logger.info("=" * 80)
    logger.info("🚀 费用单管理 - 从利润报表创建费用单（按月汇总）")
    logger.info("=" * 80)
    logger.info(f"日期范围: {start_date} 至 {end_date}")
    logger.info("=" * 80)
    
    # 验证配置
    if not settings.validate():
        logger.error("配置验证失败，请检查.env文件")
        return
    
    # 创建OpenAPI客户端
    config = settings.lingxing_config
    op_api = OpenApiBase(
        host=config['host'],
        app_id=config['app_id'],
        app_secret=config['app_secret'],
        proxy_url=config['proxy_url']
    )
    
    # 获取访问令牌
    try:
        token_resp = await op_api.generate_access_token()
        logger.info(f"✅ Token获取成功，有效期: {token_resp.expires_in}秒")
    except Exception as e:
        logger.error(f"❌ 获取访问令牌失败: {e}")
        return
    
    # 步骤0: 查询费用类型列表
    logger.info("\n查询费用类型列表...")
    fee_types, error = await get_fee_types(op_api, token_resp.access_token)
    
    if error == "TOKEN_EXPIRED":
        logger.info("Token已过期，正在刷新...")
        try:
            token_resp = await op_api.refresh_token(token_resp.refresh_token)
            logger.info(f"✅ Token刷新成功，有效期: {token_resp.expires_in}秒")
            fee_types, error = await get_fee_types(op_api, token_resp.access_token)
        except:
            try:
                token_resp = await op_api.generate_access_token()
                logger.info(f"✅ Token重新获取成功，有效期: {token_resp.expires_in}秒")
                fee_types, error = await get_fee_types(op_api, token_resp.access_token)
            except Exception as e:
                logger.error(f"❌ Token刷新失败: {e}")
                return
    
    if error or not fee_types:
        logger.error(f"❌ 无法获取费用类型列表: {error}")
        return
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 从费用类型列表中找到需要的四个费用类型
    fee_type_map = {ft.get('name'): ft.get('id') for ft in fee_types}
    
    商品成本附加费_id = fee_type_map.get('商品成本附加费')
    头程成本附加费_id = fee_type_map.get('头程成本附加费')
    头程费用_id = fee_type_map.get('头程费用')
    汇损_id = fee_type_map.get('汇损')
    
    if not 商品成本附加费_id or not 头程成本附加费_id or not 头程费用_id or not 汇损_id:
        logger.error("❌ 无法找到所需的费用类型ID")
        logger.error(f"  商品成本附加费_id: {商品成本附加费_id}")
        logger.error(f"  头程成本附加费_id: {头程成本附加费_id}")
        logger.error(f"  头程费用_id: {头程费用_id}")
        logger.error(f"  汇损_id: {汇损_id}")
        logger.error(f"  可用的费用类型: {list(fee_type_map.keys())}")
        return
    
    fee_type_ids = {
        '商品成本附加费_id': 商品成本附加费_id,
        '头程成本附加费_id': 头程成本附加费_id,
        '头程费用_id': 头程费用_id,
        '汇损_id': 汇损_id
    }
    
    logger.info(f"✅ 找到所需的费用类型ID（仅使用以下四个费用类型）:")
    logger.info(f"  商品成本附加费: {商品成本附加费_id}")
    logger.info(f"  头程成本附加费: {头程成本附加费_id}")
    logger.info(f"  头程费用: {头程费用_id}")
    logger.info(f"  汇损: {汇损_id}")
    
    # 步骤1: 作废已有费用单
    success, error = await discard_existing_fee_orders(
        op_api, token_resp, start_date, end_date,
        [商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id]
    )
    
    if error == "TOKEN_EXPIRED":
        logger.info("Token已过期，正在刷新...")
        try:
            token_resp = await op_api.refresh_token(token_resp.refresh_token)
            logger.info(f"✅ Token刷新成功，有效期: {token_resp.expires_in}秒")
            success, error = await discard_existing_fee_orders(
                op_api, token_resp, start_date, end_date,
                [商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id]
            )
        except:
            try:
                token_resp = await op_api.generate_access_token()
                logger.info(f"✅ Token重新获取成功，有效期: {token_resp.expires_in}秒")
                success, error = await discard_existing_fee_orders(
                    op_api, token_resp, start_date, end_date,
                    [商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id]
                )
            except Exception as e:
                logger.error(f"❌ Token刷新失败: {e}")
                return
    
    if not success:
        logger.error(f"❌ 作废费用单失败: {error}")
        return
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 步骤2: 从数据库读取利润报表数据（按月汇总）
    logger.info("\n" + "=" * 80)
    logger.info("步骤2: 从数据库读取利润报表数据（按月汇总）")
    logger.info("=" * 80)
    
    profit_data = fetch_profit_report_data(start_date, end_date)
    
    if not profit_data:
        logger.warning("⚠️  未找到需要创建费用单的数据")
        return
    
    # 步骤3: 创建费用单
    success_count, error, progress_info = await create_fee_orders_from_profit_report(
        op_api, token_resp, profit_data, fee_type_ids
    )
    
    if error == "TOKEN_EXPIRED":
        logger.info("Token已过期，正在刷新...")
        try:
            token_resp = await op_api.refresh_token(token_resp.refresh_token)
            logger.info(f"✅ Token刷新成功，有效期: {token_resp.expires_in}秒")
            logger.info(f"📍 从进度恢复：已成功创建 {success_count} 个费用单，继续处理...")
            # 从进度恢复，传入progress_info
            new_success_count, error, _ = await create_fee_orders_from_profit_report(
                op_api, token_resp, profit_data, fee_type_ids, progress_info
            )
            success_count += new_success_count
        except:
            try:
                token_resp = await op_api.generate_access_token()
                logger.info(f"✅ Token重新获取成功，有效期: {token_resp.expires_in}秒")
                logger.info(f"📍 从进度恢复：已成功创建 {success_count} 个费用单，继续处理...")
                new_success_count, error, _ = await create_fee_orders_from_profit_report(
                    op_api, token_resp, profit_data, fee_type_ids, progress_info
                )
                success_count += new_success_count
            except Exception as e:
                logger.error(f"❌ Token刷新失败: {e}")
                return
    
    if error:
        logger.error(f"❌ 创建费用单失败: {error}")
        return
    
    logger.info("=" * 80)
    logger.info(f"✅ 费用单管理任务完成，成功创建 {success_count} 个费用单")
    logger.info("=" * 80)


if __name__ == "__main__":
    import sys
    
    start_date = None
    end_date = None
    
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
    if len(sys.argv) > 2:
        end_date = sys.argv[2]
    
    asyncio.run(main(start_date, end_date))

