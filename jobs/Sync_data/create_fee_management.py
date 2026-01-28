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
        
    async def init_token(self):
        """初始化或刷新访问令牌"""
        try:
            if self.refresh_token_str:
                logger.info("🔄 使用refresh_token刷新访问令牌")
                token_dto = await self.op_api.refresh_token(self.refresh_token_str)
            else:
                logger.info("🔑 生成新的访问令牌")
                token_dto = await self.op_api.generate_access_token()
            
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
                    if await self.init_token():
                        continue
                    else:
                        logger.error(f"❌ Token刷新失败")
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
                    if await self.init_token():
                        continue
                    else:
                        logger.error(f"❌ Token刷新失败")
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


async def main():
    """主函数 - 示例"""
    logger.info("=" * 80)
    logger.info("🚀 费用单管理 - 示例")
    logger.info("=" * 80)
    
    # 先查询费用类型列表（可选，用于获取费用类型ID）
    fee_mgmt = FeeManagement()
    logger.info("查询费用类型列表...")
    fee_types = await fee_mgmt.get_fee_types()
    
    if not fee_types:
        logger.error("❌ 无法获取费用类型列表")
        return
    
    # 等待一下避免请求过快
    await asyncio.sleep(3)
    
    # 从费用类型列表中找到需要的三个费用类型
    fee_type_map = {ft.get('name'): ft.get('id') for ft in fee_types}
    
    # 需要的三个费用类型
    商品成本附加费_id = fee_type_map.get('商品成本附加费')
    头程成本附加费_id = fee_type_map.get('头程成本附加费')
    头程费用_id = fee_type_map.get('头程费用')
    
    # 步骤1: 查询费用明细列表
    logger.info("=" * 80)
    logger.info("步骤1: 查询费用明细列表")
    logger.info("=" * 80)
    
    query_result = await fee_mgmt.get_fee_list(
        offset=0,
        length=20,
        date_type="date",
        start_date="2026-01-01",
        end_date="2026-01-01",
        other_fee_type_ids=[商品成本附加费_id, 头程成本附加费_id, 头程费用_id]
    )
    
    if not query_result:
        logger.error("❌ 查询费用列表失败")
        return
    
    # 显示查询结果
    data = query_result.get('data', {})
    records = data.get('records', [])
    
    if records:
        logger.info(f"\n找到 {len(records)} 个符合条件的费用单:")
        for i, record in enumerate(records, 1):
            fee_id = record.get('id')
            number = record.get('number')
            fee = record.get('fee')
            status = record.get('status_order')
            create_time = record.get('create_time')
            logger.info(f"  {i}. 费用单号: {number}")
            logger.info(f"     费用单ID: {fee_id}")
            logger.info(f"     金额: {fee}")
            logger.info(f"     状态: {status}")
            logger.info(f"     创建时间: {create_time}")
    else:
        logger.info("未找到符合条件的费用单")
    
    # 等待
    await asyncio.sleep(REQUEST_DELAY)
    
    # 步骤2: 作废所有查询到的费用单
    if records:
        logger.info("=" * 80)
        logger.info("步骤2: 作废所有查询到的费用单")
        logger.info("=" * 80)
        
        # 收集所有费用单号
        all_numbers = [record.get('number') for record in records]
        
        logger.info(f"\n准备作废 {len(all_numbers)} 个费用单:")
        for i, number in enumerate(all_numbers, 1):
            logger.info(f"  {i}. {number}")
        
        # 批量作废
        discard_result = await fee_mgmt.discard_fee_orders(all_numbers)
        
        if discard_result:
            logger.info(f"\n✅ 已成功作废 {len(all_numbers)} 个费用单")
        else:
            logger.error(f"\n❌ 作废费用单失败")
        
        # 等待
        await asyncio.sleep(REQUEST_DELAY)
    else:
        logger.info("\n没有费用单需要作废，跳过步骤2")
    
    # 步骤3: 创建新费用单（可选，取消注释以启用）
    logger.info("=" * 80)
    logger.info("步骤3: 创建包含多个费用明细项的费用单")
    logger.info("=" * 80)
    
    msku = "RRZQZ369-BO-M-FBA-TD-SY043"
    shop_sid = 11548  # JQ-US
    date = "2026-01-01"
    
    fee_items = [
        {
            "sids": [shop_sid],
            "dimension_value": msku,
            "date": date,
            "other_fee_type_id": 商品成本附加费_id,
            "fee": 15,
            "currency_code": "CNY",
            "remark": f"{msku}-商品成本附加费"
        },
        {
            "sids": [shop_sid],
            "dimension_value": msku,
            "date": date,
            "other_fee_type_id": 头程成本附加费_id,
            "fee": 15,
            "currency_code": "CNY",
            "remark": f"{msku}-头程成本附加费"
        },
        {
            "sids": [shop_sid],
            "dimension_value": msku,
            "date": date,
            "other_fee_type_id": 头程费用_id,
            "fee": 100,
            "currency_code": "CNY",
            "remark": f"{msku}-头程费用"
        }
    ]
    
    logger.info(f"\n参数信息:")
    logger.info(f"  店铺ID: {shop_sid} (JQ-US)")
    logger.info(f"  MSKU: {msku}")
    logger.info(f"  日期: {date}")
    logger.info(f"  分摊规则: 2 (按销量)")
    logger.info(f"  费用明细项数量: {len(fee_items)}")
    for i, item in enumerate(fee_items, 1):
        logger.info(f"    {i}. 费用类型ID={item['other_fee_type_id']}, 金额={item['fee']} CNY")
    logger.info("")
    
    result = await fee_mgmt.create_fee_order(
        submit_type=2,
        dimension=1,
        apportion_rule=2,  # 2=按销量
        is_request_pool=0,
        remark="测试批量费用",
        fee_items=fee_items
    )
    
    if result:
        logger.info("=" * 80)
        logger.info("✅ 费用单管理任务完成")
        logger.info("=" * 80)
    else:
        logger.error("=" * 80)
        logger.error("❌ 费用单创建失败")
        logger.error("=" * 80)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⚠️  任务被用户中断")
    except Exception as e:
        logger.error(f"❌ 任务执行失败: {str(e)}", exc_info=True)
        raise

