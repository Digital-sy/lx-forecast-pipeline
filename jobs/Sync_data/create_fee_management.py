#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
费用单管理任务
包括查询费用类型列表和创建费用单
API: 
  - /bd/fee/management/open/feeManagement/otherFee/type (查询费用类型列表)
  - /bd/fee/management/open/feeManagement/otherFee/create (创建费用单)
"""
import asyncio
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase

# 获取日志记录器
logger = get_logger('fee_management')

# 重试配置（令牌桶容量为1，需要更长的间隔）
MAX_RETRIES = 5  # 最大重试次数
RETRY_DELAY = 10  # 重试延迟（秒）
REQUEST_DELAY = 3  # 请求间隔（秒）- 令牌桶容量为1，需要保守一些
TOKEN_BUCKET_CAPACITY = 1  # 令牌桶容量

# 费用单创建配置
MAX_FEE_ITEMS_PER_ORDER = 100  # 每个费用单最多包含的费用明细项数量（避免超过API限制）


class FeeManagement:
    """费用单管理类"""
    
    def __init__(self):
        """初始化费用单管理"""
        # 处理proxy_url：只有非空字符串才传递，否则传None
        proxy_url = settings.LINGXING_PROXY_URL if settings.LINGXING_PROXY_URL else None
        
        # 修复：如果proxy_url是空字符串，不要传递它
        if proxy_url == '':
            proxy_url = None
        
        self.op_api = OpenApiBase(
            host=settings.LINGXING_HOST,
            app_id=settings.LINGXING_APP_ID,
            app_secret=settings.LINGXING_APP_SECRET,
            proxy_url=proxy_url
        )
        self.token = None
        self.refresh_token_str = None
        
    async def init_token(self, force_new: bool = False):
        """
        初始化或刷新访问令牌
        
        Args:
            force_new: 是否强制生成新token（忽略refresh_token）
        """
        try:
            # 如果强制生成新token，或者没有refresh_token，直接生成新token
            if force_new or not self.refresh_token_str:
                logger.info("🔑 生成新的访问令牌")
                token_dto = await self.op_api.generate_access_token()
            else:
                # 尝试使用refresh_token刷新
                try:
                    logger.info("🔄 使用refresh_token刷新访问令牌")
                    token_dto = await self.op_api.refresh_token(self.refresh_token_str)
                except Exception as refresh_error:
                    # refresh_token失败，清除并生成新token
                    error_msg = str(refresh_error)
                    if 'invalid' in error_msg.lower() or 'expired' in error_msg.lower():
                        logger.warning(f"⚠️  refresh_token无效或已过期: {error_msg}，将生成新token")
                        self.refresh_token_str = None  # 清除无效的refresh_token
                        token_dto = await self.op_api.generate_access_token()
                    else:
                        # 其他错误，重新抛出
                        raise
            
            # 兼容Pydantic v1和v2
            try:
                token_data = token_dto.model_dump()  # Pydantic v2
            except AttributeError:
                token_data = token_dto.dict()  # Pydantic v1
            
            self.token = token_data.get('access_token')
            self.refresh_token_str = token_data.get('refresh_token')
            
            logger.info(f"✅ 令牌获取成功")
            return True
            
        except Exception as e:
            logger.error(f"❌ 令牌获取失败: {str(e)}")
            # 如果失败，清除token和refresh_token，下次会重新生成
            self.token = None
            self.refresh_token_str = None
            return False
    
    async def get_fee_types(self) -> Optional[List[Dict[str, Any]]]:
        """
        查询费用类型列表
        
        Returns:
            List[Dict]: 费用类型列表，包含id, name, sort, fpoft_id等字段
            None: 查询失败
        """
        if not self.token:
            if not await self.init_token():
                return None
        
        for retry in range(MAX_RETRIES):
            try:
                if retry > 0:
                    logger.debug(f"查询费用类型列表，第 {retry + 1}/{MAX_RETRIES} 次尝试")
                
                # 费用类型查询接口：POST请求，无请求参数，无请求体
                resp = await self.op_api.request(
                    self.token,
                    "/bd/fee/management/open/feeManagement/otherFee/type",
                    "POST"
                )
                
                # 兼容Pydantic v1和v2
                try:
                    result = resp.model_dump()  # Pydantic v2
                except AttributeError:
                    result = resp.dict()  # Pydantic v1
                
                code = result.get('code', 0)
                message = result.get('msg', '') or result.get('message', '')
                
                # 检查是否请求过于频繁（使用指数退避）
                if code == 3001008:  # 请求过于频繁（令牌桶无令牌）
                    wait_time = RETRY_DELAY * (2 ** retry)  # 指数退避
                    logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                    await asyncio.sleep(wait_time)
                    continue
                
                # 检查是否token过期
                if code in [401, 403, 2001003, 2001005, 3001001, 3001002]:
                    logger.warning(f"🔑 Token错误 (code={code}): {message}，尝试刷新token")
                    # 如果refresh_token也无效，会尝试生成新token
                    if await self.init_token():
                        continue
                    else:
                        logger.error(f"❌ Token刷新失败，尝试强制生成新token")
                        # 最后一次尝试：强制生成新token
                        if await self.init_token(force_new=True):
                            continue
                        else:
                            logger.error(f"❌ 无法获取有效token")
                            return None
                
                # 检查其他错误
                if code != 0:
                    logger.warning(f"⚠️  API返回错误: code={code}, message={message}")
                    if retry < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (retry + 1)
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ 达到最大重试次数，查询费用类型列表失败")
                        return None
                
                # 获取数据
                data = result.get('data', [])
                if data is None:
                    data = []
                
                logger.info(f"✅ 查询费用类型列表成功，共 {len(data)} 个费用类型")
                
                # 打印费用类型列表供参考
                if data:
                    logger.info("=" * 50)
                    logger.info("费用类型列表:")
                    for fee_type in data:
                        logger.info(f"  ID: {fee_type.get('id')} | 名称: {fee_type.get('name')} | 排序: {fee_type.get('sort')}")
                    logger.info("=" * 50)
                
                return data
                
            except Exception as e:
                logger.error(f"❌ 查询费用类型列表异常: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数")
                    return None
        
        return None
    
    async def get_fee_list(
        self,
        offset: int = 0,
        length: int = 20,
        date_type: str = "date",
        start_date: str = None,
        end_date: str = None,
        sids: List[int] = None,
        other_fee_type_ids: List[int] = None,
        status_order: int = None,
        dimensions: List[int] = None,
        search_field: str = None,
        search_value: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        查询费用明细列表
        
        Args:
            offset: 分页偏移量，默认0
            length: 分页长度，默认20
            date_type: 时间类型：gmt_create 创建日期，date 分摊日期
            start_date: 开始时间，格式：Y-m-d
            end_date: 结束时间，格式：Y-m-d
            sids: 店铺id列表
            other_fee_type_ids: 费用类型id列表
            status_order: 单据状态：1 待提交, 2 待审批, 3 已处理, 4 已驳回, 5 已作废
            dimensions: 分摊维度id列表：1 msku, 2 asin, 3 店铺, 4 父asin, 5 sku, 6 企业
            search_field: 搜索类型：number/msku/asin/create_name/remark_order/remark_item
            search_value: 搜索值
        
        Returns:
            Dict: 查询结果，包含total和records
            None: 查询失败
        """
        if not self.token:
            if not await self.init_token():
                return None
        
        # 构建请求体
        req_body = {
            "offset": offset,
            "length": length,
            "date_type": date_type,
            "start_date": start_date,
            "end_date": end_date
        }
        
        # 添加可选参数
        if sids:
            req_body["sids"] = sids
        if other_fee_type_ids:
            req_body["other_fee_type_ids"] = other_fee_type_ids
        if status_order:
            req_body["status_order"] = status_order
        if dimensions:
            req_body["dimensions"] = dimensions
        if search_field and search_value:
            req_body["search_field"] = search_field
            req_body["search_value"] = search_value
        
        for retry in range(MAX_RETRIES):
            try:
                if retry > 0:
                    logger.debug(f"查询费用列表，第 {retry + 1}/{MAX_RETRIES} 次尝试")
                
                resp = await self.op_api.request(
                    self.token,
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
                
                if code in [401, 403, 2001003, 2001005, 3001001, 3001002]:
                    logger.warning(f"🔑 Token错误 (code={code}): {message}，尝试刷新token")
                    if await self.init_token():
                        continue
                    else:
                        logger.error(f"❌ Token刷新失败")
                        return None
                
                if code != 0:
                    logger.warning(f"⚠️  API返回错误: code={code}, message={message}")
                    if retry < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (retry + 1)
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ 达到最大重试次数，查询费用列表失败")
                        return None
                
                data = result.get('data', {})
                if data is None:
                    data = {}
                
                total = data.get('total', 0)
                records = data.get('records', [])
                
                logger.info(f"✅ 查询费用列表成功，共 {total} 条记录，当前返回 {len(records)} 条")
                
                return result
                
            except Exception as e:
                logger.error(f"❌ 查询费用列表异常: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数")
                    return None
        
        return None
    
    async def discard_fee_orders(
        self,
        numbers: List[str]
    ) -> Optional[Dict[str, Any]]:
        """
        作废费用单
        
        Args:
            numbers: 费用单号列表，上限200
        
        Returns:
            Dict: 作废结果
            None: 作废失败
        """
        if not self.token:
            if not await self.init_token():
                return None
        
        req_body = {
            "numbers": numbers
        }
        
        logger.info(f"准备作废 {len(numbers)} 个费用单: {numbers}")
        
        for retry in range(MAX_RETRIES):
            try:
                if retry > 0:
                    logger.debug(f"作废费用单，第 {retry + 1}/{MAX_RETRIES} 次尝试")
                
                resp = await self.op_api.request(
                    self.token,
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
                
                if code in [401, 403, 2001003, 2001005, 3001001, 3001002]:
                    logger.warning(f"🔑 Token错误 (code={code}): {message}，尝试刷新token")
                    if await self.init_token():
                        continue
                    else:
                        logger.error(f"❌ Token刷新失败")
                        return None
                
                if code != 0:
                    logger.error(f"❌ API返回错误: code={code}, message={message}")
                    if retry < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (retry + 1)
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ 达到最大重试次数，作废费用单失败")
                        return None
                
                logger.info(f"✅ 费用单作废成功!")
                logger.info(f"返回结果: {json.dumps(result, ensure_ascii=False, indent=2)}")
                
                return result
                
            except Exception as e:
                logger.error(f"❌ 作废费用单异常: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数")
                    return None
        
        return None
    
    async def create_fee_order(
        self,
        submit_type: int,
        dimension: int,
        apportion_rule: int,
        is_request_pool: int,
        remark: str,
        fee_items: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        创建费用单
        
        Args:
            submit_type: 提交类型：1 暂存，2 提交
            dimension: 分摊维度：1 msku, 2 asin, 3 店铺, 4 父asin, 5 sku, 6 企业
            apportion_rule: 分摊规则：0 无, 1 按销售额, 2 按销量, 3 店铺均摊后按销售额占比分摊, 4 店铺均摊后按销量占比分摊
            is_request_pool: 是否请款：0 否，1 是
            remark: 费用单备注
            fee_items: 费用明细项列表，每项包含：
                - sids: 店铺id列表
                - dimension_value: 纬度值，例如ASIN值
                - date: 分摊日期，格式：Y-m-d 或 Y-m
                - other_fee_type_id: 费用类型id
                - fee: 金额（原币金额，注意正负数）
                - currency_code: 币种代码
                - remark: 费用子项备注
        
        Returns:
            Dict: 创建结果
            None: 创建失败
        """
        if not self.token:
            if not await self.init_token():
                return None
        
        # 构建请求体
        req_body = {
            "submit_type": submit_type,
            "dimension": dimension,
            "apportion_rule": apportion_rule,
            "is_request_pool": is_request_pool,
            "remark": remark,
            "fee_items": fee_items
        }
        
        logger.info("=" * 50)
        logger.info("📝 准备创建费用单:")
        logger.info(f"  提交类型: {submit_type} (1=暂存, 2=提交)")
        logger.info(f"  分摊维度: {dimension} (1=msku, 2=asin, 3=店铺, 4=父asin, 5=sku, 6=企业)")
        logger.info(f"  分摊规则: {apportion_rule} (0=无, 1=按销售额, 2=按销量, 3=店铺均摊后按销售额占比分摊, 4=店铺均摊后按销量占比分摊)")
        logger.info(f"  是否请款: {is_request_pool} (0=否, 1=是)")
        logger.info(f"  备注: {remark}")
        logger.info(f"  费用明细项数量: {len(fee_items)}")
        logger.info("=" * 50)
        
        for retry in range(MAX_RETRIES):
            try:
                if retry > 0:
                    logger.debug(f"创建费用单，第 {retry + 1}/{MAX_RETRIES} 次尝试")
                
                resp = await self.op_api.request(
                    self.token,
                    "/bd/fee/management/open/feeManagement/otherFee/create",
                    "POST",
                    req_body=req_body
                )
                
                # 兼容Pydantic v1和v2
                try:
                    result = resp.model_dump()  # Pydantic v2
                except AttributeError:
                    result = resp.dict()  # Pydantic v1
                
                code = result.get('code', 0)
                message = result.get('msg', '') or result.get('message', '')
                
                # 检查是否请求过于频繁（使用指数退避）
                if code == 3001008:  # 请求过于频繁（令牌桶无令牌）
                    wait_time = RETRY_DELAY * (2 ** retry)  # 指数退避
                    logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                    await asyncio.sleep(wait_time)
                    continue
                
                # 检查是否token过期
                if code in [401, 403, 2001003, 2001005, 3001001, 3001002]:
                    logger.warning(f"🔑 Token错误 (code={code}): {message}，尝试刷新token")
                    # 如果refresh_token也无效，会尝试生成新token
                    if await self.init_token():
                        continue
                    else:
                        logger.error(f"❌ Token刷新失败，尝试强制生成新token")
                        # 最后一次尝试：强制生成新token
                        if await self.init_token(force_new=True):
                            continue
                        else:
                            logger.error(f"❌ 无法获取有效token")
                            return None
                
                # 检查其他错误
                if code != 0:
                    logger.error(f"❌ API返回错误: code={code}, message={message}")
                    if retry < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (retry + 1)
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ 达到最大重试次数，创建费用单失败")
                        return None
                
                # 创建成功
                data = result.get('data')
                logger.info(f"✅ 费用单创建成功!")
                logger.info(f"返回结果: {json.dumps(result, ensure_ascii=False, indent=2)}")
                
                return result
                
            except Exception as e:
                logger.error(f"❌ 创建费用单异常: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数")
                    return None
        
        return None




async def create_custom_fee_order(
    submit_type: int,
    dimension: int,
    apportion_rule: int,
    is_request_pool: int,
    remark: str,
    fee_items: List[Dict[str, Any]],
    fetch_fee_types: bool = True
):
    """
    创建自定义费用单的便捷函数
    
    Args:
        submit_type: 提交类型：1 暂存，2 提交
        dimension: 分摊维度：1 msku, 2 asin, 3 店铺, 4 父asin, 5 sku, 6 企业
        apportion_rule: 分摊规则：0 无, 1 按销售额, 2 按销量, 3 店铺均摊后按销售额占比分摊, 4 店铺均摊后按销量占比分摊
        is_request_pool: 是否请款：0 否，1 是
        remark: 费用单备注
        fee_items: 费用明细项列表
        fetch_fee_types: 是否先查询费用类型列表（用于验证费用类型ID是否有效）
    
    Returns:
        创建结果
    """
    fee_mgmt = FeeManagement()
    
    # 可选：先查询费用类型列表
    if fetch_fee_types:
        logger.info("🔍 查询费用类型列表...")
        fee_types = await fee_mgmt.get_fee_types()
        if fee_types:
            await asyncio.sleep(REQUEST_DELAY)
    
    # 创建费用单
    result = await fee_mgmt.create_fee_order(
        submit_type=submit_type,
        dimension=dimension,
        apportion_rule=apportion_rule,
        is_request_pool=is_request_pool,
        remark=remark,
        fee_items=fee_items
    )
    
    return result


async def create_test_fee_order(
    shop_sid: int,
    dimension_value: str,
    fee_type_id: int,
    date: str = "2026-01-01",
    fee: float = 100,
    currency_code: str = "CNY",
    submit_type: int = 1,
    dimension: int = 1,
    apportion_rule: int = 2,
    is_request_pool: int = 1,
    remark: str = "测试"
):
    """
    创建费用单
    
    Args:
        shop_sid: 店铺ID（必填）
        dimension_value: 维度值，如MSKU值（必填）
        fee_type_id: 费用类型ID（必填）
        date: 分摊日期，格式：Y-m-d 或 Y-m，默认：2026-01-01
        fee: 金额，默认：100
        currency_code: 币种代码，默认：CNY
        submit_type: 提交类型：1=暂存，2=提交，默认：1
        dimension: 分摊维度：1=msku, 2=asin, 3=店铺, 4=父asin, 5=sku, 6=企业，默认：1
        apportion_rule: 分摊规则：0=无, 1=按销售额, 2=按销量, 3=店铺均摊后按销售额占比分摊, 4=店铺均摊后按销量占比分摊，默认：2
        is_request_pool: 是否请款：0=否，1=是，默认：1
        remark: 费用单备注，默认：测试
    
    Returns:
        创建结果
    """
    fee_mgmt = FeeManagement()
    
    logger.info("=" * 80)
    logger.info("📝 创建费用单")
    logger.info("=" * 80)
    
    # 构建费用子项备注
    fee_item_remark = f"{dimension_value}-费用类型ID:{fee_type_id}"
    
    # 构建费用明细项
    fee_items = [
        {
            "sids": [shop_sid],
            "dimension_value": dimension_value,
            "date": date,
            "other_fee_type_id": fee_type_id,
            "fee": fee,
            "currency_code": currency_code,
            "remark": fee_item_remark
        }
    ]
    
    logger.info(f"参数信息:")
    logger.info(f"  店铺ID: {shop_sid}")
    logger.info(f"  维度值: {dimension_value}")
    logger.info(f"  费用类型ID: {fee_type_id}")
    logger.info(f"  日期: {date}")
    logger.info(f"  金额: {fee} {currency_code}")
    logger.info(f"  提交类型: {submit_type} ({'暂存' if submit_type == 1 else '提交'})")
    logger.info("=" * 80)
    
    # 创建费用单
    result = await fee_mgmt.create_fee_order(
        submit_type=submit_type,
        dimension=dimension,
        apportion_rule=apportion_rule,
        is_request_pool=is_request_pool,
        remark=remark,
        fee_items=fee_items
    )
    
    return result


def fetch_profit_report_data(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    从数据库读取利润报表数据
    
    Args:
        start_date: 开始日期，格式：Y-m-d
        end_date: 结束日期，格式：Y-m-d
    
    Returns:
        List[Dict]: 利润报表数据列表
    """
    try:
        with db_cursor() as cursor:
            sql = """
                SELECT 
                    `MSKU`,
                    `店铺id`,
                    `统计日期`,
                    `商品成本附加费`,
                    `头程成本附加费`,
                    `录入费用单头程`
                FROM `利润报表`
                WHERE `统计日期` >= %s 
                  AND `统计日期` <= %s
                  AND (
                      (`商品成本附加费` IS NOT NULL AND `商品成本附加费` != 0) OR
                      (`头程成本附加费` IS NOT NULL AND `头程成本附加费` != 0) OR
                      (`录入费用单头程` IS NOT NULL AND `录入费用单头程` != 0)
                  )
                ORDER BY `统计日期`, `店铺id`, `MSKU`
            """
            cursor.execute(sql, (start_date, end_date))
            records = cursor.fetchall()
            
            # 统计信息
            unique_msku_shop = set()
            msku_shop_details = {}  # 用于详细统计
            
            for record in records:
                msku = record.get('MSKU', '').strip()
                shop_id = record.get('店铺id')
                if msku and shop_id:
                    key = (msku, str(shop_id))
                    unique_msku_shop.add(key)
                    
                    # 记录每个MSKU在不同店铺下的详情
                    if msku not in msku_shop_details:
                        msku_shop_details[msku] = []
                    msku_shop_details[msku].append({
                        'shop_id': str(shop_id),
                        '商品成本附加费': record.get('商品成本附加费', 0),
                        '头程成本附加费': record.get('头程成本附加费', 0),
                        '录入费用单头程': record.get('录入费用单头程', 0)
                    })
            
            # 特别检查特定MSKU（用于调试）
            debug_msku = "RRZQZ369-BO-S-FBA-TD-SY043"
            if debug_msku in msku_shop_details:
                logger.info(f"  🔍 调试：MSKU {debug_msku} 在以下店铺下:")
                for detail in msku_shop_details[debug_msku]:
                    logger.info(f"    店铺ID={detail['shop_id']}, 商品成本附加费={detail['商品成本附加费']}, "
                              f"头程成本附加费={detail['头程成本附加费']}, 录入费用单头程={detail['录入费用单头程']}")
            
            logger.info(f"✅ 从数据库读取到 {len(records)} 条利润报表数据（日期范围：{start_date} 至 {end_date}）")
            logger.info(f"   包含 {len(unique_msku_shop)} 个不同的(MSKU, 店铺ID)组合")
            
            # 检查是否有MSKU在多个店铺下
            multi_shop_mskus = {msku: shops for msku, shops in msku_shop_details.items() if len(set(s['shop_id'] for s in shops)) > 1}
            if multi_shop_mskus:
                logger.info(f"   发现 {len(multi_shop_mskus)} 个MSKU在多个店铺下:")
                for msku, shops in list(multi_shop_mskus.items())[:10]:  # 只显示前10个
                    shop_ids = list(set(s['shop_id'] for s in shops))
                    logger.info(f"     MSKU={msku}, 店铺IDs={shop_ids}")
            
            return records
    except Exception as e:
        logger.error(f"❌ 读取利润报表数据失败: {str(e)}")
        return []


async def discard_existing_fee_orders(
    fee_mgmt: FeeManagement,
    start_date: str,
    end_date: str,
    fee_type_ids: List[int]
) -> bool:
    """
    作废指定日期范围内指定费用类型的费用单（分批查询和作废）
    
    Args:
        fee_mgmt: 费用管理实例
        start_date: 开始日期
        end_date: 结束日期
        fee_type_ids: 费用类型ID列表
    
    Returns:
        bool: 是否成功
    """
    logger.info("=" * 80)
    logger.info("步骤1: 查询并作废已有费用单（分批处理）")
    logger.info("=" * 80)
    
    # 分批处理配置
    query_batch_size = 500  # 每次查询500条
    discard_batch_size = 200  # 每次作废200个
    
    offset = 0
    total_queried = 0
    total_discarded = 0
    total_already_discarded = 0
    pending_numbers = []  # 累积待作废的费用单号
    
    # 先查询一次获取总数
    first_query = await fee_mgmt.get_fee_list(
        offset=0,
        length=1,
        date_type="date",
        start_date=start_date,
        end_date=end_date,
        other_fee_type_ids=fee_type_ids
    )
    
    total_count = 0
    if first_query:
        data = first_query.get('data', {})
        total_count = data.get('total', 0)
        logger.info(f"总共需要处理 {total_count} 条费用单，将分批查询和作废")
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 分批查询和作废
    while True:
        # 查询一批费用单
        query_result = await fee_mgmt.get_fee_list(
            offset=offset,
            length=query_batch_size,
            date_type="date",
            start_date=start_date,
            end_date=end_date,
            other_fee_type_ids=fee_type_ids
        )
        
        if not query_result:
            break
        
        data = query_result.get('data', {})
        records = data.get('records', [])
        
        if not records:
            break
        
        total_queried += len(records)
        
        # 过滤出需要作废的费用单
        batch_pending_numbers = []
        for record in records:
            status_order = record.get('status_order')
            if status_order == 5 or str(status_order) == "已作废":
                total_already_discarded += 1
                continue
            number = record.get('number')
            if number:
                batch_pending_numbers.append(number)
        
        pending_numbers.extend(batch_pending_numbers)
        
        logger.info(f"  已查询 {total_queried}/{total_count if total_count > 0 else '?'} 条费用单，"
                   f"累积 {len(pending_numbers)} 个待作废，{total_already_discarded} 个已作废")
        
        # 如果累积的待作废数量达到批次大小，或者这是最后一批，执行作废
        if len(pending_numbers) >= discard_batch_size or total_queried >= total_count:
            # 分批作废
            while len(pending_numbers) >= discard_batch_size:
                batch_numbers = pending_numbers[:discard_batch_size]
                pending_numbers = pending_numbers[discard_batch_size:]
                
                logger.info(f"  准备作废 {len(batch_numbers)} 个费用单...")
                discard_result = await fee_mgmt.discard_fee_orders(batch_numbers)
                
                if discard_result:
                    total_discarded += len(batch_numbers)
                    logger.info(f"  ✅ 成功作废 {len(batch_numbers)} 个费用单（累计 {total_discarded} 个）")
                else:
                    logger.error(f"  ❌ 作废失败，{len(batch_numbers)} 个费用单未作废")
                
                await asyncio.sleep(REQUEST_DELAY)
        
        # 检查是否还有更多记录
        if total_count > 0 and total_queried >= total_count:
            break
        
        if len(records) < query_batch_size:
            break
        
        offset += query_batch_size
        await asyncio.sleep(REQUEST_DELAY)
    
    # 处理剩余的待作废费用单
    if pending_numbers:
        logger.info(f"  处理剩余的 {len(pending_numbers)} 个费用单...")
        # 分批作废剩余的费用单
        for i in range(0, len(pending_numbers), discard_batch_size):
            batch_numbers = pending_numbers[i:i + discard_batch_size]
            logger.info(f"  准备作废剩余的第 {i // discard_batch_size + 1} 批，共 {len(batch_numbers)} 个费用单...")
            discard_result = await fee_mgmt.discard_fee_orders(batch_numbers)
            
            if discard_result:
                total_discarded += len(batch_numbers)
                logger.info(f"  ✅ 成功作废 {len(batch_numbers)} 个费用单（累计 {total_discarded} 个）")
            else:
                logger.error(f"  ❌ 作废失败，{len(batch_numbers)} 个费用单未作废")
            
            await asyncio.sleep(REQUEST_DELAY)
    
    logger.info("=" * 80)
    logger.info(f"✅ 作废完成：")
    logger.info(f"   总查询: {total_queried} 条费用单")
    logger.info(f"   已作废: {total_discarded} 个费用单")
    logger.info(f"   无需作废: {total_already_discarded} 个费用单（已经是作废状态）")
    logger.info("=" * 80)
    
    return True


async def create_fee_orders_from_profit_report(
    fee_mgmt: FeeManagement,
    profit_data: List[Dict[str, Any]],
    fee_type_ids: Dict[str, int]
) -> int:
    """
    根据利润报表数据创建费用单
    
    Args:
        fee_mgmt: 费用管理实例
        profit_data: 利润报表数据列表
        fee_type_ids: 费用类型ID字典，包含：商品成本附加费_id, 头程成本附加费_id, 头程费用_id
    
    Returns:
        int: 成功创建的费用单数量
    """
    logger.info("=" * 80)
    logger.info("步骤2: 根据利润报表数据创建费用单")
    logger.info("=" * 80)
    
    # 按统计日期和店铺ID分组
    from collections import defaultdict
    from datetime import date, datetime
    
    grouped_data = defaultdict(list)
    skipped_records = []  # 记录被跳过的记录
    
    for record in profit_data:
        stat_date = record.get('统计日期')
        shop_id = record.get('店铺id')
        msku = record.get('MSKU', '').strip()
        
        # 检查数据完整性
        if not stat_date:
            skipped_records.append({
                'reason': '统计日期为空',
                'record': record
            })
            continue
        
        if not shop_id:
            skipped_records.append({
                'reason': '店铺ID为空',
                'record': record
            })
            continue
        
        if not msku:
            skipped_records.append({
                'reason': 'MSKU为空',
                'record': record
            })
            continue
        
        # 处理日期格式：如果是datetime或date对象，转换为字符串
        if isinstance(stat_date, (date, datetime)):
            stat_date_str = stat_date.strftime('%Y-%m-%d')
        else:
            stat_date_str = str(stat_date)
        
        key = (stat_date_str, str(shop_id))
        grouped_data[key].append(record)
    
    # 报告被跳过的记录
    if skipped_records:
        logger.warning(f"  ⚠️  发现 {len(skipped_records)} 条记录被跳过（数据不完整）:")
        skip_reasons = {}
        for skip in skipped_records:
            reason = skip['reason']
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        for reason, count in skip_reasons.items():
            logger.warning(f"    {reason}: {count} 条")
    
    logger.info(f"  共 {len(profit_data)} 条数据，按日期和店铺分组后共 {len(grouped_data)} 组")
    
    # 统计信息：检查是否有重复的MSKU，以及每个MSKU在哪些店铺下
    msku_shop_count = {}
    msku_shops_map = {}  # 记录每个MSKU在哪些店铺下
    
    for record in profit_data:
        msku = record.get('MSKU', '').strip()
        shop_id = record.get('店铺id')
        if msku and shop_id:
            key = (msku, str(shop_id))
            msku_shop_count[key] = msku_shop_count.get(key, 0) + 1
            
            # 记录每个MSKU的店铺列表
            if msku not in msku_shops_map:
                msku_shops_map[msku] = set()
            msku_shops_map[msku].add(str(shop_id))
    
    # 找出有重复的记录
    duplicates = {k: v for k, v in msku_shop_count.items() if v > 1}
    if duplicates:
        logger.warning(f"  ⚠️  发现 {len(duplicates)} 个MSKU在同一店铺下有重复记录，将合并金额")
        for (msku, shop_id), count in list(duplicates.items())[:10]:  # 只显示前10个
            logger.warning(f"    MSKU={msku}, 店铺ID={shop_id}, 重复{count}次")
    
    # 检查每个MSKU在哪些店铺下，以及分组后是否都在
    logger.info(f"  数据中包含 {len(msku_shops_map)} 个不同的MSKU")
    multi_shop_mskus = {msku: shops for msku, shops in msku_shops_map.items() if len(shops) > 1}
    if multi_shop_mskus:
        logger.info(f"  其中 {len(multi_shop_mskus)} 个MSKU在多个店铺下:")
        for msku, shops in list(multi_shop_mskus.items())[:20]:  # 显示前20个
            logger.info(f"    MSKU={msku}, 店铺IDs={sorted(shops)}")
    
    success_count = 0
    total_count = 0
    failed_groups = []  # 记录创建失败的组
    skipped_groups = []  # 记录被跳过的组
    
    # 按日期和店铺分组创建费用单
    for (stat_date_str, shop_id), records in grouped_data.items():
        total_count += 1
        
        # 统计该组包含的MSKU
        group_mskus = set()
        for record in records:
            msku = record.get('MSKU', '').strip()
            if msku:
                group_mskus.add(msku)
        
        logger.info(f"\n处理第 {total_count}/{len(grouped_data)} 组：日期={stat_date_str}, 店铺ID={shop_id}, 包含 {len(records)} 条记录, {len(group_mskus)} 个MSKU")
        
        # 特别检查特定MSKU（用于调试）
        debug_msku = "RRZQZ369-BO-S-FBA-TD-SY043"
        if debug_msku in group_mskus:
            logger.info(f"  🔍 调试：该组包含MSKU {debug_msku}")
            for record in records:
                if record.get('MSKU', '').strip() == debug_msku:
                    logger.info(f"    记录详情: 店铺ID={record.get('店铺id')}, "
                              f"商品成本附加费={record.get('商品成本附加费', 0)}, "
                              f"头程成本附加费={record.get('头程成本附加费', 0)}, "
                              f"录入费用单头程={record.get('录入费用单头程', 0)}")
        
        # 按MSKU合并金额（如果同一MSKU有多条记录，合并金额）
        from collections import defaultdict
        msku_fees = defaultdict(lambda: {
            '商品成本附加费': 0.0,
            '头程成本附加费': 0.0,
            '录入费用单头程': 0.0
        })
        
        for record in records:
            msku = record.get('MSKU', '').strip()
            if not msku:
                continue
            
            # 累加金额
            cg_price_additional_fee = float(record.get('商品成本附加费', 0) or 0)
            cg_transport_additional_fee = float(record.get('头程成本附加费', 0) or 0)
            recorded_freight = float(record.get('录入费用单头程', 0) or 0)
            
            msku_fees[msku]['商品成本附加费'] += cg_price_additional_fee
            msku_fees[msku]['头程成本附加费'] += cg_transport_additional_fee
            msku_fees[msku]['录入费用单头程'] += recorded_freight
        
        # 构建费用明细项（按MSKU合并后的金额）
        fee_items = []
        msku_count = 0
        
        for msku, fees in msku_fees.items():
            msku_count += 1
            
            # 商品成本附加费
            if fees['商品成本附加费'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": stat_date_str,
                    "other_fee_type_id": fee_type_ids['商品成本附加费_id'],
                    "fee": fees['商品成本附加费'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-商品成本附加费"
                })
            
            # 头程成本附加费
            if fees['头程成本附加费'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": stat_date_str,
                    "other_fee_type_id": fee_type_ids['头程成本附加费_id'],
                    "fee": fees['头程成本附加费'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-头程成本附加费"
                })
            
            # 录入费用单头程（对应头程费用）
            if fees['录入费用单头程'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": stat_date_str,
                    "other_fee_type_id": fee_type_ids['头程费用_id'],
                    "fee": fees['录入费用单头程'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-头程费用"
                })
        
        logger.info(f"  该组包含 {msku_count} 个不同的MSKU，生成 {len(fee_items)} 个费用明细项")
        
        # 特别检查特定MSKU的费用明细（用于调试）
        debug_msku = "RRZQZ369-BO-S-FBA-TD-SY043"
        if debug_msku in msku_fees:
            logger.info(f"  🔍 调试：MSKU {debug_msku} 的费用明细:")
            fees = msku_fees[debug_msku]
            logger.info(f"    商品成本附加费={fees['商品成本附加费']}, "
                      f"头程成本附加费={fees['头程成本附加费']}, "
                      f"录入费用单头程={fees['录入费用单头程']}")
            # 检查该MSKU生成了多少个费用明细项
            msku_fee_items = [item for item in fee_items if item['dimension_value'] == debug_msku]
            logger.info(f"    为该MSKU生成了 {len(msku_fee_items)} 个费用明细项")
        
        if not fee_items:
            logger.warning(f"  ⚠️  跳过：该组没有需要创建的费用项（可能所有费用都为0）")
            skipped_groups.append({
                'date': stat_date_str,
                'shop_id': shop_id,
                'msku_count': msku_count,
                'reason': '所有费用都为0'
            })
            # 如果该组包含调试MSKU，显示详细信息
            if debug_msku in group_mskus:
                logger.warning(f"  ⚠️  调试：该组包含MSKU {debug_msku}，但费用明细项为空！")
                for msku, fees in msku_fees.items():
                    if msku == debug_msku:
                        logger.warning(f"    MSKU {msku} 的费用: 商品成本附加费={fees['商品成本附加费']}, "
                                     f"头程成本附加费={fees['头程成本附加费']}, "
                                     f"录入费用单头程={fees['录入费用单头程']}")
            continue
        
        logger.info(f"  准备创建费用单，包含 {len(fee_items)} 个费用明细项")
        
        # 如果费用明细项数量超过限制，分批创建
        if len(fee_items) > MAX_FEE_ITEMS_PER_ORDER:
            logger.info(f"  ⚠️  费用明细项数量({len(fee_items)})超过限制({MAX_FEE_ITEMS_PER_ORDER})，将分批创建")
            
            # 分批创建费用单
            batch_count = (len(fee_items) + MAX_FEE_ITEMS_PER_ORDER - 1) // MAX_FEE_ITEMS_PER_ORDER
            logger.info(f"  将分成 {batch_count} 批创建费用单")
            
            for batch_idx in range(batch_count):
                start_idx = batch_idx * MAX_FEE_ITEMS_PER_ORDER
                end_idx = min(start_idx + MAX_FEE_ITEMS_PER_ORDER, len(fee_items))
                batch_fee_items = fee_items[start_idx:end_idx]
                
                logger.info(f"  创建第 {batch_idx + 1}/{batch_count} 批，包含 {len(batch_fee_items)} 个费用明细项")
                
                result = await fee_mgmt.create_fee_order(
                    submit_type=2,  # 2=提交
                    dimension=1,  # 1=msku
                    apportion_rule=2,  # 2=按销量
                    is_request_pool=0,  # 0=否
                    remark=f"利润报表自动创建-{stat_date_str} (第{batch_idx + 1}/{batch_count}批)",
                    fee_items=batch_fee_items
                )
                
                if result:
                    success_count += 1
                    logger.info(f"  ✅ 第 {batch_idx + 1}/{batch_count} 批费用单创建成功")
                else:
                    logger.error(f"  ❌ 第 {batch_idx + 1}/{batch_count} 批费用单创建失败")
                    # 记录失败的批次
                    failed_groups.append({
                        'date': stat_date_str,
                        'shop_id': shop_id,
                        'batch': f"{batch_idx + 1}/{batch_count}",
                        'fee_items_count': len(batch_fee_items),
                        'fee_items': batch_fee_items  # 保存失败的数据，以便后续重试
                    })
                
                await asyncio.sleep(REQUEST_DELAY)
        else:
            # 费用明细项数量在限制内，直接创建
            result = await fee_mgmt.create_fee_order(
                submit_type=2,  # 2=提交
                dimension=1,  # 1=msku
                apportion_rule=2,  # 2=按销量
                is_request_pool=0,  # 0=否
                remark=f"利润报表自动创建-{stat_date_str}",
                fee_items=fee_items
            )
            
            if result:
                success_count += 1
                logger.info(f"  ✅ 费用单创建成功")
            else:
                logger.error(f"  ❌ 费用单创建失败")
                # 记录失败的组
                failed_groups.append({
                    'date': stat_date_str,
                    'shop_id': shop_id,
                    'batch': '1/1',
                    'fee_items_count': len(fee_items),
                    'fee_items': fee_items  # 保存失败的数据，以便后续重试
                })
            
            await asyncio.sleep(REQUEST_DELAY)
    
    # 最终统计：验证所有店铺的数据是否都被处理
    processed_shops = set()
    processed_msku_shops = set()
    
    for (stat_date_str, shop_id), records in grouped_data.items():
        processed_shops.add(str(shop_id))
        for record in records:
            msku = record.get('MSKU', '').strip()
            if msku:
                processed_msku_shops.add((msku, str(shop_id)))
    
    logger.info(f"\n✅ 费用单创建完成：成功创建 {success_count} 个费用单（来自 {total_count} 个日期+店铺组合）")
    logger.info(f"   处理了 {len(processed_shops)} 个不同的店铺ID")
    logger.info(f"   处理了 {len(processed_msku_shops)} 个(MSKU, 店铺ID)组合")
    
    # 报告被跳过的组
    if skipped_groups:
        logger.warning(f"\n  ⚠️  警告：有 {len(skipped_groups)} 个组被跳过（所有费用都为0）")
        for skip in skipped_groups[:10]:  # 只显示前10个
            logger.warning(f"    日期={skip['date']}, 店铺ID={skip['shop_id']}, MSKU数量={skip['msku_count']}")
    
    # 报告创建失败的组
    if failed_groups:
        logger.error(f"\n  ❌ 错误：有 {len(failed_groups)} 个费用单创建失败:")
        total_failed_items = 0
        for fail in failed_groups:
            logger.error(f"    日期={fail['date']}, 店铺ID={fail['shop_id']}, "
                        f"批次={fail['batch']}, 费用明细项数量={fail['fee_items_count']}")
            total_failed_items += fail['fee_items_count']
        logger.error(f"    共 {total_failed_items} 个费用明细项未成功创建，需要重试")
    
    # 验证是否有遗漏
    all_msku_shops = set()
    for record in profit_data:
        msku = record.get('MSKU', '').strip()
        shop_id = record.get('店铺id')
        if msku and shop_id:
            all_msku_shops.add((msku, str(shop_id)))
    
    missing = all_msku_shops - processed_msku_shops
    if missing:
        logger.warning(f"\n  ⚠️  警告：发现 {len(missing)} 个(MSKU, 店铺ID)组合未被处理:")
        for msku, shop_id in list(missing)[:20]:  # 只显示前20个
            logger.warning(f"    MSKU={msku}, 店铺ID={shop_id}")
    else:
        logger.info(f"\n  ✅ 所有数据都已处理，无遗漏")
    
    # 最终统计
    if failed_groups or skipped_groups or missing:
        logger.warning(f"\n  ⚠️  总结：")
        logger.warning(f"    成功创建: {success_count} 个费用单")
        logger.warning(f"    被跳过: {len(skipped_groups)} 个组（费用为0）")
        logger.warning(f"    创建失败: {len(failed_groups)} 个费用单（需要重试）")
        logger.warning(f"    数据遗漏: {len(missing)} 个(MSKU, 店铺ID)组合")
    else:
        logger.info(f"\n  ✅ 所有数据都已成功处理，无遗漏、无失败")
    
    return success_count


async def main(start_date: str = None, end_date: str = None, daily: bool = False):
    """
    主函数 - 从数据库读取利润报表并创建费用单
    
    Args:
        start_date: 开始日期，格式：Y-m-d，默认：前15天
        end_date: 结束日期，格式：Y-m-d，默认：今天
        daily: 是否按天处理，默认：False
    """
    from datetime import datetime, date, timedelta
    
    # 确定日期范围（默认前5天到今天）
    if end_date is None:
        end_date = date.today().strftime('%Y-%m-%d')
    
    if start_date is None:
        # 默认更新前5天的数据
        start_date = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')
    
    logger.info("=" * 80)
    logger.info("🚀 费用单管理 - 从利润报表创建费用单")
    logger.info("=" * 80)
    logger.info(f"日期范围: {start_date} 至 {end_date}")
    logger.info(f"处理模式: {'按天处理' if daily else '批量处理'}")
    logger.info("=" * 80)
    
    # 初始化费用管理
    fee_mgmt = FeeManagement()
    
    # 步骤0: 查询费用类型列表
    logger.info("查询费用类型列表...")
    fee_types = await fee_mgmt.get_fee_types()
    
    if not fee_types:
        logger.error("❌ 无法获取费用类型列表")
        return
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 从费用类型列表中找到需要的三个费用类型
    fee_type_map = {ft.get('name'): ft.get('id') for ft in fee_types}
    
    商品成本附加费_id = fee_type_map.get('商品成本附加费')
    头程成本附加费_id = fee_type_map.get('头程成本附加费')
    头程费用_id = fee_type_map.get('头程费用')
    
    if not 商品成本附加费_id or not 头程成本附加费_id or not 头程费用_id:
        logger.error("❌ 无法找到所需的费用类型ID")
        logger.error(f"  商品成本附加费_id: {商品成本附加费_id}")
        logger.error(f"  头程成本附加费_id: {头程成本附加费_id}")
        logger.error(f"  头程费用_id: {头程费用_id}")
        return
    
    fee_type_ids = {
        '商品成本附加费_id': 商品成本附加费_id,
        '头程成本附加费_id': 头程成本附加费_id,
        '头程费用_id': 头程费用_id
    }
    
    # 如果启用按天处理，循环处理每一天
    if daily:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        current_date = start_dt
        total_success = 0
        
        while current_date <= end_dt:
            current_date_str = current_date.strftime('%Y-%m-%d')
            
            logger.info("\n" + "=" * 80)
            logger.info(f"📅 处理日期: {current_date_str}")
            logger.info("=" * 80)
            
            # 步骤1: 作废已有费用单
            await discard_existing_fee_orders(
                fee_mgmt,
                current_date_str,
                current_date_str,
                [商品成本附加费_id, 头程成本附加费_id, 头程费用_id]
            )
            
            await asyncio.sleep(REQUEST_DELAY)
            
            # 步骤2: 从数据库读取利润报表数据
            logger.info(f"从数据库读取 {current_date_str} 的利润报表数据")
            profit_data = fetch_profit_report_data(current_date_str, current_date_str)
            
            if not profit_data:
                logger.warning(f"⚠️  {current_date_str} 未找到需要创建费用单的数据")
            else:
                # 步骤3: 创建费用单
                success_count = await create_fee_orders_from_profit_report(
                    fee_mgmt,
                    profit_data,
                    fee_type_ids
                )
                total_success += success_count
                logger.info(f"✅ {current_date_str} 完成，成功创建 {success_count} 个费用单")
            
            # 移动到下一天
            current_date += timedelta(days=1)
            
            # 如果不是最后一天，等待一下
            if current_date <= end_dt:
                await asyncio.sleep(REQUEST_DELAY)
        
        logger.info("\n" + "=" * 80)
        logger.info(f"✅ 所有日期处理完成，总共成功创建 {total_success} 个费用单")
        logger.info("=" * 80)
    else:
        # 批量处理模式（原有逻辑）
        # 步骤1: 作废已有费用单
        await discard_existing_fee_orders(
            fee_mgmt,
            start_date,
            end_date,
            [商品成本附加费_id, 头程成本附加费_id, 头程费用_id]
        )
        
        await asyncio.sleep(REQUEST_DELAY)
        
        # 步骤2: 从数据库读取利润报表数据
        logger.info("=" * 80)
        logger.info("步骤2: 从数据库读取利润报表数据")
        logger.info("=" * 80)
        
        profit_data = fetch_profit_report_data(start_date, end_date)
        
        if not profit_data:
            logger.warning("⚠️  未找到需要创建费用单的数据")
            return
        
        # 步骤3: 创建费用单
        success_count = await create_fee_orders_from_profit_report(
            fee_mgmt,
            profit_data,
            fee_type_ids
        )
        
        logger.info("=" * 80)
        logger.info(f"✅ 费用单管理任务完成，成功创建 {success_count} 个费用单")
        logger.info("=" * 80)


async def discard_fee_orders_by_date_range(start_date: str, end_date: str):
    """
    作废指定日期范围内的三个费用类型的费用单
    
    Args:
        start_date: 开始日期，格式：Y-m-d
        end_date: 结束日期，格式：Y-m-d
    """
    from datetime import date
    
    logger.info("=" * 80)
    logger.info("🗑️  作废费用单任务")
    logger.info("=" * 80)
    logger.info(f"日期范围: {start_date} 至 {end_date}")
    logger.info("费用类型: 商品成本附加费, 头程成本附加费, 头程费用")
    logger.info("=" * 80)
    
    # 初始化费用管理
    fee_mgmt = FeeManagement()
    
    # 查询费用类型列表
    logger.info("查询费用类型列表...")
    fee_types = await fee_mgmt.get_fee_types()
    
    if not fee_types:
        logger.error("❌ 无法获取费用类型列表")
        return
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 从费用类型列表中找到需要的三个费用类型
    fee_type_map = {ft.get('name'): ft.get('id') for ft in fee_types}
    
    商品成本附加费_id = fee_type_map.get('商品成本附加费')
    头程成本附加费_id = fee_type_map.get('头程成本附加费')
    头程费用_id = fee_type_map.get('头程费用')
    
    if not 商品成本附加费_id or not 头程成本附加费_id or not 头程费用_id:
        logger.error("❌ 无法找到所需的费用类型ID")
        logger.error(f"  商品成本附加费_id: {商品成本附加费_id}")
        logger.error(f"  头程成本附加费_id: {头程成本附加费_id}")
        logger.error(f"  头程费用_id: {头程费用_id}")
        return
    
    fee_type_ids = [商品成本附加费_id, 头程成本附加费_id, 头程费用_id]
    
    logger.info(f"找到费用类型ID:")
    logger.info(f"  商品成本附加费_id: {商品成本附加费_id}")
    logger.info(f"  头程成本附加费_id: {头程成本附加费_id}")
    logger.info(f"  头程费用_id: {头程费用_id}")
    
    # 作废费用单
    await discard_existing_fee_orders(
        fee_mgmt,
        start_date,
        end_date,
        fee_type_ids
    )
    
    logger.info("=" * 80)
    logger.info("✅ 作废费用单任务完成")
    logger.info("=" * 80)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='从利润报表创建费用单')
    parser.add_argument('--start-date', type=str, default=None, 
                       help='开始日期，格式：Y-m-d，默认：前5天')
    parser.add_argument('--end-date', type=str, default=None,
                       help='结束日期，格式：Y-m-d，默认：今天')
    parser.add_argument('--daily', action='store_true',
                       help='按天处理（每天单独处理，避免一次性处理太多数据）')
    parser.add_argument('--discard-only', action='store_true',
                       help='仅作废费用单，不创建新费用单')
    
    args = parser.parse_args()
    
    try:
        if args.discard_only:
            # 仅作废模式
            from datetime import date
            start_date = args.start_date or '2026-01-01'
            end_date = args.end_date or date.today().strftime('%Y-%m-%d')
            asyncio.run(discard_fee_orders_by_date_range(start_date, end_date))
        else:
            # 正常模式：创建费用单
            asyncio.run(main(start_date=args.start_date, end_date=args.end_date, daily=args.daily))
    except KeyboardInterrupt:
        logger.info("⚠️  任务被用户中断")
    except Exception as e:
        logger.error(f"❌ 任务执行失败: {str(e)}", exc_info=True)
        raise

