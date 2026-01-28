#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
仓库库存明细数据采集任务
从领星API获取仓库库存明细数据并存入数据库
API: /erp/sc/routing/data/local_inventory/inventoryDetails
"""
import asyncio
import json
import traceback
import httpx
from typing import List, Dict, Any, Tuple
from datetime import datetime

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase
from utils import normalize_shop_name
from .shop_mapping import get_shop_mapping

# 获取日志记录器
logger = get_logger('inventory_details')

# 飞书webhook地址（与 main.py 保持一致）
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/00640680-6577-4a95-b25a-35c34864ff45"


async def send_feishu_message(message: str) -> bool:
    """
    发送消息到飞书群
    
    Args:
        message: 要发送的消息内容
        
    Returns:
        bool: 是否发送成功
    """
    try:
        data = {
            "msg_type": "text",
            "content": {
                "text": message
            }
        }
        
        timeout = httpx.Timeout(10.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(FEISHU_WEBHOOK_URL, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                logger.info("飞书消息发送成功")
                return True
            else:
                logger.error(f"飞书消息发送失败: {result.get('msg')}")
                return False
    except Exception as e:
        logger.error(f"发送飞书消息异常: {e}")
        return False

# 重试配置（优先保证数据完整性）
# 根据领星令牌桶算法优化：
# - 令牌桶维度：appId + 接口url
# - 每个请求消耗1个令牌
# - 令牌在请求完成/异常/超时(2min)后自动回收
# - 无令牌时返回错误码3001008
MAX_RETRIES = 5  # 最大重试次数（增加到5次）
RETRY_DELAY = 10  # 重试延迟（秒，增加到10秒）
REQUEST_DELAY = 2  # 请求间隔（秒）- 给令牌回收时间，API响应快时令牌回收也快
TOKEN_BUCKET_CAPACITY = 5  # 令牌桶容量（推测值，用于计算安全间隔）


async def fetch_page_with_retry(op_api: OpenApiBase, token: str, 
                                 req_body: dict, offset: int) -> Tuple[List[Dict[str, Any]], int, str, bool]:
    """
    带重试机制的单页数据获取（指数退避策略，令牌桶感知）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        req_body: 请求体
        offset: 分页偏移量
        
    Returns:
        Tuple[List, int, str, bool]: (数据列表, 总数, 错误信息, 是否遇到限流)
    """
    was_rate_limited = False
    all_retries_rate_limited = True  # 标记是否所有重试都遇到限流
    
    for retry in range(MAX_RETRIES):
        try:
            if retry > 0:
                logger.debug(f"offset={offset}，第 {retry + 1}/{MAX_RETRIES} 次尝试")
            
            resp = await op_api.request(
                token, 
                "/erp/sc/routing/data/local_inventory/inventoryDetails", 
                "POST", 
                req_body=req_body
            )
            
            # 兼容Pydantic v1和v2
            try:
                result = resp.model_dump()  # Pydantic v2
            except AttributeError:
                result = resp.dict()  # Pydantic v1
            
            code = result.get('code', 0)
            message = result.get('message', '')
            
            # 检查是否请求过于频繁（使用指数退避）
            if code == 3001008:  # 请求过于频繁（令牌桶无令牌）
                was_rate_limited = True
                wait_time = RETRY_DELAY * (2 ** retry)  # 指数退避：10, 20, 40, 80, 160秒
                logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒让令牌回收...")
                await asyncio.sleep(wait_time)
                continue
            
            # 如果成功获取数据，说明不是所有重试都限流
            all_retries_rate_limited = False
            
            # 检查是否token过期
            # 错误码：401, 403, 2001003, 3001001, 3001002 都表示Token过期或无效
            if code in [401, 403, 2001003, 3001001, 3001002]:  # token相关错误
                logger.error(f"🔑 Token错误 (code={code}): {message}，需要刷新token")
                return [], 0, "TOKEN_EXPIRED", False
            
            # 检查其他错误
            if code != 0:
                logger.warning(f"⚠️  API返回错误: code={code}, message={message}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数，放弃该页")
                    return [], 0, f"API_ERROR: {message}", was_rate_limited
            
            items = result.get('data', [])
            total = result.get('total', 0)
            
            # 确保items不为None
            if items is None:
                items = []
            
            # 成功获取数据
            if retry > 0:
                logger.info(f"✅ 重试成功！")
            
            return items, total, "", was_rate_limited
            
        except Exception as e:
            all_retries_rate_limited = False  # 异常不是限流问题
            logger.error(f"❌ 请求异常（第 {retry + 1}/{MAX_RETRIES} 次尝试）: {e}")
            if retry < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (retry + 1)
                logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"❌ 达到最大重试次数，请求失败")
                return [], 0, f"EXCEPTION: {str(e)}", was_rate_limited
    
    # 如果所有重试都遇到限流，返回特殊错误码，让外层函数知道这是限流问题
    if all_retries_rate_limited:
        return [], 0, "RATE_LIMITED_EXCEEDED", True
    return [], 0, "MAX_RETRIES_EXCEEDED", was_rate_limited


async def fetch_warehouse_list(op_api: OpenApiBase, token_resp) -> Dict[str, str]:
    """
    获取仓库列表，建立仓库ID到仓库名称的映射
    
    Args:
        op_api: OpenAPI客户端
        token_resp: 访问令牌响应对象
        
    Returns:
        Dict[str, str]: 仓库ID到仓库名称的映射
    """
    wid_to_name_map = {}
    offset = 0
    length = 1000  # 每页最多1000条
    token = token_resp.access_token
    
    logger.info("正在获取仓库列表（本地仓）...")
    
    while True:
        req_body = {
            "type": 1,  # 只查询本地仓
            "offset": offset,
            "length": length
        }
        
        try:
            resp = await op_api.request(
                token,
                "/erp/sc/data/local_inventory/warehouse",
                "POST",
                req_body=req_body
            )
            
            # 兼容Pydantic v1和v2
            try:
                result = resp.model_dump()  # Pydantic v2
            except AttributeError:
                result = resp.dict()  # Pydantic v1
            
            code = result.get('code', 0)
            message = result.get('message', '')
            total = result.get('total', 0)
            
            logger.info(f"API响应: code={code}, message={message}, total={total}")
            
            if code != 0:
                logger.warning(f"获取仓库列表失败: code={code}, message={message}")
                break
            
            warehouses = result.get('data', [])
            logger.info(f"API返回 {len(warehouses)} 个仓库记录（total={total}）")
            
            if not warehouses:
                logger.info("仓库列表为空，退出循环")
                break
            
            # 建立映射（只保留未删除的仓库）
            skipped_count = 0
            for warehouse in warehouses:
                wid = warehouse.get('wid')
                name = warehouse.get('name', '')
                is_delete = warehouse.get('is_delete', '0')
                
                # 统一处理is_delete：可能是字符串'0'/'1'或整数0/1
                is_deleted = False
                if isinstance(is_delete, str):
                    is_deleted = is_delete.strip() == '1'
                elif isinstance(is_delete, (int, float)):
                    is_deleted = int(is_delete) != 0
                
                # 只保留未删除的仓库
                if wid and not is_deleted:
                    wid_to_name_map[str(wid)] = name or f'仓库{wid}'
                else:
                    skipped_count += 1
            
            if skipped_count > 0:
                logger.info(f"跳过了 {skipped_count} 个已删除或无效的仓库")
            
            logger.info(f"已获取 {len(wid_to_name_map)} 个有效仓库映射...")
            
            # 如果这一页没有获取到任何有效仓库，且已经处理了所有数据，退出
            if len(warehouses) < length:
                break
            
            # 如果total有值且已获取的数量达到total，退出
            if total > 0 and len(wid_to_name_map) >= total:
                break
            
            offset += length
            await asyncio.sleep(REQUEST_DELAY)
            
        except Exception as e:
            logger.error(f"获取仓库列表失败: {e}")
            break
    
    logger.info(f"✅ 共获取 {len(wid_to_name_map)} 个仓库映射")
    # 打印所有仓库映射
    if wid_to_name_map:
        logger.info("仓库映射列表：")
        for wid, name in sorted(wid_to_name_map.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0):
            logger.info(f"    wid={wid} → {name}")
    return wid_to_name_map


async def fetch_all_inventory_details(op_api: OpenApiBase, token_resp) -> List[Dict[str, Any]]:
    """
    获取所有库存明细数据（分页处理，支持重试和token刷新）
    
    Args:
        op_api: OpenAPI客户端
        token_resp: 访问令牌响应对象（包含access_token和refresh_token）
        
    Returns:
        List[Dict[str, Any]]: 库存明细列表
    """
    all_items = []
    offset = 0
    length = 800  # 每页最多800条（API上限）
    token = token_resp.access_token
    total_records = None
    
    # 令牌桶感知：动态调整请求间隔
    current_delay = REQUEST_DELAY  # 当前请求间隔
    consecutive_success = 0  # 连续成功次数
    consecutive_rate_limited = 0  # 连续限流次数
    
    while True:
        req_body = {
            "offset": offset,
            "length": length
        }
        
        # 带重试机制获取数据（令牌桶感知）
        items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
        
        # 处理token过期
        if error == "TOKEN_EXPIRED":
            logger.info("Token已过期，正在刷新...")
            try:
                token_resp = await op_api.generate_access_token()
                token = token_resp.access_token
                logger.info(f"Token刷新成功，有效期: {token_resp.expires_in}秒")
                # 重试当前页
                items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
            except Exception as e:
                logger.error(f"Token刷新失败: {e}")
                break
        
        # 处理令牌桶限流：5次重试后仍然限流，可能是token过期，先尝试刷新token
        if error == "RATE_LIMITED_EXCEEDED":
            logger.warning(f"⚠️  offset={offset} 在5次重试后仍然遇到令牌桶限流")
            logger.info("🔍 怀疑可能是token过期，先尝试刷新token...")
            
            # 先尝试刷新token（以防token过期被误判为限流）
            try:
                logger.info("🔄 正在刷新token...")
                token_resp = await op_api.generate_access_token()
                token = token_resp.access_token
                logger.info(f"✅ Token刷新成功，有效期: {token_resp.expires_in}秒")
                logger.info("🔄 使用新token重新尝试获取数据...")
                items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
                
                # 如果刷新token后成功了，说明确实是token过期
                if not error:
                    logger.info("✅ 刷新token后成功获取数据，确认是token过期问题")
                    # 继续正常流程，不break
                elif error == "RATE_LIMITED_EXCEEDED":
                    # 刷新token后仍然限流，说明不是token问题，等待令牌桶恢复
                    logger.warning("⚠️  刷新token后仍然限流，说明不是token过期，等待令牌桶恢复...")
                    logger.info("💤 等待180秒（3分钟）让令牌桶完全恢复后继续重试...")
                    await asyncio.sleep(180)
                    
                    # 再次尝试获取数据
                    logger.info("🔄 重新尝试获取数据...")
                    items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
                    
                    # 如果仍然失败，再等待一次
                    if error == "RATE_LIMITED_EXCEEDED":
                        logger.warning("⚠️  仍然遇到令牌桶限流，再等待180秒...")
                        await asyncio.sleep(180)
                        logger.info("🔄 最后一次尝试获取数据...")
                        items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
            except Exception as e:
                logger.error(f"❌ Token刷新失败: {e}")
                logger.warning("⚠️  将尝试等待令牌桶恢复...")
                await asyncio.sleep(180)
                logger.info("🔄 重新尝试获取数据...")
                items, total, error, was_rate_limited = await fetch_page_with_retry(op_api, token, req_body, offset)
            
            # 如果最终仍然失败，记录详细错误并停止
            if error == "RATE_LIMITED_EXCEEDED":
                logger.error(f"❌ offset={offset} 在多次尝试（刷新token + 长时间等待）后仍然无法获取数据")
                logger.error(f"   已尝试：5次常规重试 + token刷新 + 2次长时间等待重试")
                logger.error(f"   建议：请检查API服务状态，或稍后手动重试该任务")
                logger.error(f"   当前已获取 {len(all_items)} 条数据，缺失从offset={offset}开始的数据")
                break
        
        # 处理其他错误
        if error and error != "TOKEN_EXPIRED" and error != "RATE_LIMITED_EXCEEDED":
            logger.error(f"获取offset={offset}数据失败: {error}")
            break
        
        # 记录总数
        if total_records is None and total > 0:
            total_records = total
            logger.info(f"📊 总共需要获取 {total_records} 条数据")
        
        logger.info(f"✓ offset={offset}: 获取 {len(items)} 条数据")
        
        if not items:
            if offset == 0:
                logger.warning("第一页就没有数据")
            break
        
        all_items.extend(items)
        
        # 显示进度
        if total_records:
            progress = (len(all_items) / total_records) * 100
            logger.info(f"📈 进度: {len(all_items)}/{total_records} ({progress:.1f}%)")
        
        # 如果已经获取所有数据，退出循环
        if total_records and len(all_items) >= total_records:
            logger.info("✅ 所有数据获取完成")
            break
        
        # 如果返回的数据少于length，说明已经是最后一页
        if len(items) < length:
            break
        
        offset += length
        
        # 令牌桶感知：动态调整请求间隔
        if was_rate_limited:
            # 遇到限流，增加间隔（给令牌更多时间回收）
            consecutive_rate_limited += 1
            consecutive_success = 0
            # 间隔递增：2秒 → 3秒 → 4秒 → 5秒（最大5秒）
            current_delay = min(REQUEST_DELAY + consecutive_rate_limited, 5)
            logger.info(f"🪣 检测到限流，调整请求间隔为 {current_delay} 秒")
        else:
            # 成功获取，重置限流计数
            consecutive_rate_limited = 0
            consecutive_success += 1
            # 连续成功3次后，可以稍微减少间隔（但不少于基础间隔）
            if consecutive_success >= 3:
                current_delay = max(REQUEST_DELAY - 0.5, 1.5)
                consecutive_success = 0  # 重置计数
            else:
                current_delay = REQUEST_DELAY
        
        # 请求间隔延时（动态调整，令牌桶感知）
        await asyncio.sleep(current_delay)
    
    # 数据完整性检查
    if total_records and len(all_items) < total_records:
        logger.warning(f"⚠️  数据可能不完整: 预期 {total_records} 条，实际获取 {len(all_items)} 条")
        logger.warning(f"   缺失: {total_records - len(all_items)} 条数据")
    elif total_records:
        logger.info(f"✅ 数据完整性验证通过: {len(all_items)}/{total_records} 条")
    
    return all_items


def convert_inventory_data(items: List[Dict[str, Any]], 
                           wid_to_name_map: Dict[str, str] = None,
                           sid_to_name_map: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """
    转换库存明细数据
    过滤掉预估总量为0的记录
    
    Args:
        items: 库存明细列表
        wid_to_name_map: 仓库ID到仓库名称的映射（可选）
        sid_to_name_map: 店铺ID到店铺名称的映射（可选）
        
    Returns:
        List[Dict[str, Any]]: 转换后的数据列表（已过滤预估总量为0的记录）
    """
    inventory_list = []
    filtered_count = 0  # 记录被过滤的数量
    
    for item in items:
        # SKU
        sku = item.get('sku', '') or '无'
        
        # 店铺ID转店铺名
        seller_id = item.get('seller_id', '')
        if seller_id and sid_to_name_map:
            shop_name = sid_to_name_map.get(str(seller_id), '无')
            shop_name = normalize_shop_name(shop_name) if shop_name != '无' else '无'
        else:
            shop_name = '无'
        
        # 仓库ID转仓库名（有映射就用，没有给兜底）
        wid = item.get('wid', 0)
        if wid and wid_to_name_map:
            warehouse_name = wid_to_name_map.get(str(wid), '未知仓库')
        else:
            warehouse_name = '未知仓库'
        
        # 基础字段
        fnsku = item.get('fnsku', '') or ''
        
        # 库存数量相关字段
        product_total = item.get('product_total', 0) or 0
        product_valid_num = item.get('product_valid_num', 0) or 0
        product_qc_num = item.get('product_qc_num', 0) or 0
        
        # 成本相关字段（字符串转数字）
        stock_cost_total = item.get('stock_cost_total', '0') or '0'
        try:
            stock_cost_total_float = float(stock_cost_total)
        except:
            stock_cost_total_float = 0.0
        
        stock_cost = item.get('stock_cost', '0') or '0'
        try:
            stock_cost_float = float(stock_cost)
        except:
            stock_cost_float = 0.0
        
        # 待到货量
        quantity_receive = item.get('quantity_receive', '0') or '0'
        try:
            quantity_receive_int = int(float(quantity_receive))
        except:
            quantity_receive_int = 0
        
        # 调拨在途相关
        product_onway = item.get('product_onway', 0) or 0
        transit_head_cost = item.get('transit_head_cost', '0') or '0'
        try:
            transit_head_cost_float = float(transit_head_cost)
        except:
            transit_head_cost_float = 0.0
        
        # 平均库龄
        average_age = item.get('average_age', 0) or 0
        
        # 库龄信息（展开为多个独立字段）
        stock_age_list = item.get('stock_age_list', []) or []
        
        # 初始化库龄字段
        age_0_15 = 0
        age_16_30 = 0
        age_31_60 = 0
        age_61_90 = 0
        age_91_120 = 0
        age_121_180 = 0
        age_181_360 = 0
        age_361_plus = 0
        
        # 从库龄列表中提取数据（按索引匹配，与JavaScript代码一致）
        if isinstance(stock_age_list, list) and len(stock_age_list) > 0:
            # 按索引访问，对应 ageList[0]?.qty || 0 的逻辑
            if len(stock_age_list) > 0 and isinstance(stock_age_list[0], dict):
                age_0_15 = stock_age_list[0].get('qty', 0) or 0
            if len(stock_age_list) > 1 and isinstance(stock_age_list[1], dict):
                age_16_30 = stock_age_list[1].get('qty', 0) or 0
            if len(stock_age_list) > 2 and isinstance(stock_age_list[2], dict):
                age_31_60 = stock_age_list[2].get('qty', 0) or 0
            if len(stock_age_list) > 3 and isinstance(stock_age_list[3], dict):
                age_61_90 = stock_age_list[3].get('qty', 0) or 0
            if len(stock_age_list) > 4 and isinstance(stock_age_list[4], dict):
                age_91_120 = stock_age_list[4].get('qty', 0) or 0
            if len(stock_age_list) > 5 and isinstance(stock_age_list[5], dict):
                age_121_180 = stock_age_list[5].get('qty', 0) or 0
            if len(stock_age_list) > 6 and isinstance(stock_age_list[6], dict):
                age_181_360 = stock_age_list[6].get('qty', 0) or 0
            if len(stock_age_list) > 7 and isinstance(stock_age_list[7], dict):
                age_361_plus = stock_age_list[7].get('qty', 0) or 0
        
        # 预估总量 = 可用量 + 待到货量
        estimated_total = product_valid_num + quantity_receive_int
        
        # 如果预估总量为0，跳过该记录
        if estimated_total == 0:
            filtered_count += 1
            continue
        
        # 构建库存明细记录（只保留需要的字段）
        inventory_record = {
            'SKU': sku,
            '店铺': shop_name,
            '仓库': warehouse_name,
            'FNSKU': fnsku,
            '实际库存总量': product_total,
            '可用量': product_valid_num,
            '待到货量': quantity_receive_int,
            '在途数量': product_onway,
            '待检待上架量': product_qc_num,
            '调拨在途头程成本': transit_head_cost_float,
            '每件库存成本': stock_cost_float,
            '总库存成本': stock_cost_total_float,
            '平均库龄': average_age,
            '0-15天库龄': age_0_15,
            '16-30天库龄': age_16_30,
            '31-60天库龄': age_31_60,
            '61-90天库龄': age_61_90,
            '91-120天库龄': age_91_120,
            '121-180天库龄': age_121_180,
            '181-360天库龄': age_181_360,
            '361天以上库龄': age_361_plus,
        }
        
        inventory_list.append(inventory_record)
    
    if filtered_count > 0:
        logger.info(f"已过滤 {filtered_count} 条预估总量为0的记录")
    
    return inventory_list


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any]) -> None:
    """
    创建数据表（如果不存在），如果表存在但结构不匹配则报错
    
    Args:
        table_name: 表名
        sample_row: 样本数据行
        
    Raises:
        ValueError: 如果表存在但结构不匹配
    """
    with db_cursor(dictionary=False) as cursor:
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()
        
        if exists:
            # 检查表结构是否匹配
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0] for row in cursor.fetchall()]
            expected = ['id'] + list(sample_row.keys())
            
            if columns == expected:
                logger.info(f"表 {table_name} 结构正确")
                return
            else:
                # 表存在但结构不匹配，报错而不是重建
                error_msg = (
                    f"表 {table_name} 已存在但结构不匹配！\n"
                    f"  期望字段: {expected}\n"
                    f"  实际字段: {columns}\n"
                    f"  请手动处理表结构问题，不要自动重建表"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # 表不存在，创建表
        fields = []
        for k, v in sample_row.items():
            if isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                # 对于可能包含JSON的字段（如库龄信息），使用TEXT类型
                if '信息' in k or 'JSON' in k.upper():
                    fields.append(f"`{k}` TEXT")
                else:
                    fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql})"
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def delete_all_data(table_name: str) -> int:
    """
    删除表中的所有数据（全量更新）
    
    Args:
        table_name: 表名
        
    Returns:
        int: 删除的记录数
    """
    with db_cursor(dictionary=False) as cursor:
        cursor.execute(f"DELETE FROM `{table_name}`")
        deleted_count = cursor.rowcount
        logger.info(f"已删除 {deleted_count} 条旧数据")
        return deleted_count


def insert_data_batch(table_name: str, data_list: List[Dict[str, Any]]) -> None:
    """
    批量插入数据
    
    Args:
        table_name: 表名
        data_list: 数据列表
    """
    if not data_list:
        return
    
    with db_cursor(dictionary=False) as cursor:
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values_placeholder = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `{table_name}` ({fields}) VALUES ({values_placeholder})"
        
        batch_size = 200
        for i in range(0, len(data_list), batch_size):
            batch = [tuple(row.values()) for row in data_list[i:i+batch_size]]
            cursor.executemany(sql, batch)
            logger.info(f"已录入 {min(i+batch_size, len(data_list))} 条...")
    
    logger.info(f"成功写入 {len(data_list)} 条数据到表 {table_name}")


async def main():
    """主函数"""
    logger.info("="*80)
    logger.info("仓库库存明细数据采集")
    logger.info("="*80)
    
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
        logger.info(f"Token有效期: {token_resp.expires_in}秒")
    except Exception as e:
        logger.error(f"获取访问令牌失败: {e}")
        return
    
    # 1. 获取仓库列表映射
    logger.info("正在获取仓库列表...")
    wid_to_name_map = await fetch_warehouse_list(op_api, token_resp)
    
    # 2. 获取店铺映射
    logger.info("正在获取店铺映射...")
    sid_to_name_map = {}
    try:
        from .shop_mapping import get_shop_mapping
        shop_mapping = await get_shop_mapping()
        # shop_mapping 已经是 {seller_id: shop_name} 的格式，直接使用
        sid_to_name_map = {str(k): v for k, v in shop_mapping.items()}
        logger.info(f"✅ 共获取 {len(sid_to_name_map)} 个店铺映射")
    except Exception as e:
        logger.warning(f"获取店铺映射失败: {e}，将使用店铺ID")
    
    # 3. 获取库存明细数据（全仓数据，只使用offset和length参数）
    logger.info("正在获取库存明细数据（全仓）...")
    logger.info(f"⏱️  配置参数（优先保证数据完整性）:")
    logger.info(f"   - 请求间隔: {REQUEST_DELAY}秒")
    logger.info(f"   - 最大重试: {MAX_RETRIES}次")
    logger.info(f"   - 重试延迟: {RETRY_DELAY}秒（指数退避）")
    logger.info("="*80)
    
    items = await fetch_all_inventory_details(op_api, token_resp)
    logger.info(f"✅ 共获取 {len(items)} 条库存明细数据")
    
    if not items:
        logger.warning("没有数据需要保存")
        return
    
    # 4. 转换为标准格式
    logger.info("正在转换数据格式...")
    inventory_data_list = convert_inventory_data(items, wid_to_name_map, sid_to_name_map)
    logger.info(f"共生成 {len(inventory_data_list)} 条库存明细记录")
    
    if not inventory_data_list:
        logger.warning("没有数据需要保存")
        return
    
    # 处理数据库
    table_name = '仓库库存明细'
    logger.info(f"正在处理数据库表 {table_name}...")
    
    try:
        # 创建或检查表结构
        create_table_if_needed(table_name, inventory_data_list[0])
        
        # 删除所有旧数据（全量更新）
        logger.info("正在删除所有旧数据（全量更新）...")
        deleted_count = delete_all_data(table_name)
        
        # 插入新数据
        logger.info("正在写入新数据...")
        insert_data_batch(table_name, inventory_data_list)
        
        # 输出统计信息
        logger.info("="*80)
        logger.info("📊 统计信息：")
        logger.info(f"  更新策略: 全量更新")
        logger.info(f"  删除旧记录: {deleted_count} 条")
        logger.info(f"  原始数据: {len(items)} 条")
        filtered_count = len(items) - len(inventory_data_list)
        if filtered_count > 0:
            logger.info(f"  过滤记录: {filtered_count} 条（预估总量为0）")
        logger.info(f"  新增记录: {len(inventory_data_list)} 条")
        
        # 查询数据库获取最终统计
        try:
            with db_cursor() as cursor:
                # 统计总记录数
                cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
                total_in_db = cursor.fetchone()['total']
                
                # 统计总可用量和预估总量（预估总量 = 可用量 + 待到货量）
                cursor.execute(f"""
                    SELECT 
                        SUM(`可用量`) as total_available,
                        SUM(`待到货量`) as total_pending,
                        SUM(`可用量` + `待到货量`) as total_estimated
                    FROM `{table_name}`
                """)
                inventory_stats = cursor.fetchone()
                total_available = inventory_stats['total_available'] or 0
                total_pending = inventory_stats['total_pending'] or 0
                total_estimated = inventory_stats['total_estimated'] or 0
                
                # 统计各仓库记录数
                cursor.execute(f"""
                    SELECT 
                        `仓库`, 
                        COUNT(*) as count, 
                        SUM(`可用量`) as total_available,
                        SUM(`待到货量`) as total_pending,
                        SUM(`可用量` + `待到货量`) as total_estimated
                    FROM `{table_name}`
                    GROUP BY `仓库`
                    ORDER BY count DESC
                """)
                warehouse_stats = cursor.fetchall()
                
                logger.info(f"  数据库总记录: {total_in_db} 条")
                logger.info(f"  总可用量: {total_available}")
                logger.info(f"  总待到货量: {total_pending}")
                logger.info(f"  总预估总量: {total_estimated} (可用量+待到货量)")
                logger.info("  各仓库统计：")
                for warehouse in warehouse_stats:
                    logger.info(f"    {warehouse['仓库']}: {warehouse['count']} 条记录, "
                              f"可用量={warehouse['total_available']}, "
                              f"待到货量={warehouse['total_pending']}, "
                              f"预估总量={warehouse['total_estimated']}")
        except Exception as e:
            logger.warning(f"查询数据库统计失败: {e}")
        
        logger.info("="*80)
        logger.info("✅ 数据采集完成！")
        logger.info("="*80)
        
    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"数据库操作失败: {e}", exc_info=True)
        
        # 发送错误消息到飞书
        feishu_message = f"""❌ 库存明细数据采集任务执行失败

📋 错误类型: {type(e).__name__}
📝 错误原因: {str(e)}
⏰ 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📄 完整错误堆栈:
```
{error_traceback}
```

请及时检查并处理！"""
        
        try:
            await send_feishu_message(feishu_message)
        except Exception as feishu_error:
            logger.error(f"发送飞书消息失败: {feishu_error}")
        
        raise


if __name__ == '__main__':
    asyncio.run(main())

