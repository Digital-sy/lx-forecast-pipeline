#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
费用单管理任务
包括查询费用类型列表和创建费用单
API: 
  - /bd/fee/management/open/feeManagement/otherFee/type (查询费用类型列表)
  - /bd/fee/management/open/feeManagement/otherFee/create (创建费用单)

Token管理策略：
  采用与其他文件（fetch_listing.py, fetch_fba_inventory.py等）一致的简化策略：
  1. 启动时获取一次token
  2. token自然使用到过期
  3. 只在API明确返回token错误时才刷新一次
  4. 避免频繁刷新导致API缓存问题
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
RETRY_DELAY = 15  # 重试延迟（秒），增加到15秒以避免频繁请求
REQUEST_DELAY = 8  # 请求间隔（秒），增加到8秒以避免触发API累积限流
TOKEN_BUCKET_CAPACITY = 1  # 令牌桶容量

# 费用单创建配置
MAX_FEE_ITEMS_PER_ORDER = 50  # 每个费用单最多包含的费用明细项数量（恢复为50，因为之前100也能成功）
MAX_FORMATTED_PARAMS_LENGTH = 8000  # 格式化参数的最大长度（字符数），超过此值将减小批次
REST_BATCH_INTERVAL = 20  # 每创建N批后休息一次，避免累积速率限制
REST_DURATION = 30  # 休息时长（秒）


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
        # 初始化时不获取token，让调用者在外部管理token（学习其他文件）
        self.token_resp = None  # 存储 token_resp 对象
        self.token = None  # 存储 access_token 字符串
        
    async def init_token(self, force_new: bool = False, retry_count: int = 0):
        """
        初始化或刷新访问令牌
        
        Args:
            force_new: 是否强制生成新token（而不是使用refresh_token）
            retry_count: 内部重试计数（避免无限递归）
            
        Returns:
            bool: 成功返回True，失败返回False
        """
        try:
            # 防止无限递归
            if retry_count >= 3:
                logger.warning(f"⚠️  Token刷新已达到最大重试次数，接受当前token（即使相同）")
                if self.token:
                    return True  # 如果有token，即使相同也接受
                return False
            
            old_token = self.token[:10] if self.token else "None"
            
            # 如果有token_resp且有refresh_token，优先使用refresh_token
            if not force_new and self.token_resp and hasattr(self.token_resp, 'refresh_token'):
                try:
                    logger.info(f"🔄 使用refresh_token刷新访问令牌（旧token前10位: {old_token}...）")
                    self.token_resp = await self.op_api.refresh_token(self.token_resp.refresh_token)
                    self.token = self.token_resp.access_token
                    
                    new_token = self.token[:10] if self.token else "None"
                    logger.info(f"✅ Token刷新成功，有效期: {self.token_resp.expires_in}秒")
                    logger.info(f"   旧token: {old_token}... -> 新token: {new_token}...")
                    
                    if old_token != "None" and old_token == new_token:
                        logger.warning(f"⚠️  警告：刷新后token相同，可能是服务器缓存")
                        logger.warning(f"⚠️  但token可能仍然有效（之前能成功），将接受此token")
                        # 即使相同也接受，因为之前能成功说明token有效
                        return True
                    
                    return True
                except Exception as e:
                    logger.warning(f"⚠️  refresh_token失败: {str(e)}，将尝试生成新token")
                    # 失败则继续下面的generate_access_token
            
            # 生成新的access_token
            logger.info(f"🔑 生成新的访问令牌（旧token前10位: {old_token}...）")
            self.token_resp = await self.op_api.generate_access_token()
            self.token = self.token_resp.access_token
            
            new_token = self.token[:10] if self.token else "None"
            logger.info(f"✅ Token获取成功，有效期: {self.token_resp.expires_in}秒")
            logger.info(f"   旧token: {old_token}... -> 新token: {new_token}...")
            
            if old_token != "None" and old_token == new_token:
                logger.warning(f"⚠️  警告：API返回了相同的token！这可能是服务器缓存")
                if retry_count < 2:
                    logger.warning(f"⚠️  等待5秒后重试（第{retry_count + 1}次）...")
                    await asyncio.sleep(5)
                    # 清除token，强制下次重新生成
                    self.token = None
                    self.token_resp = None
                    return await self.init_token(force_new=True, retry_count=retry_count + 1)
                else:
                    logger.warning(f"⚠️  已达到最大重试次数，接受当前token（即使相同）")
                    # 即使相同也接受，因为之前能成功说明token可能仍然有效
                    return True
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 令牌获取失败: {str(e)}")
            self.token = None
            self.token_resp = None
            return False
    
    async def _handle_api_request(
        self,
        url: str,
        method: str = "POST",
        req_body: Dict[str, Any] = None,
        operation_name: str = "API请求"
    ) -> Optional[Dict[str, Any]]:
        """
        通用API请求处理方法，包含重试和token刷新逻辑
        
        Args:
            url: API路径
            method: 请求方法
            req_body: 请求体
            operation_name: 操作名称（用于日志）
            
        Returns:
            Dict: API响应结果
            None: 请求失败
        """
        for retry in range(MAX_RETRIES):
            try:
                if retry > 0:
                    logger.debug(f"{operation_name}，第 {retry + 1}/{MAX_RETRIES} 次尝试")
                
                resp = await self.op_api.request(
                    self.token,
                    url,
                    method,
                    req_body=req_body
                )
                
                # 兼容Pydantic v1和v2
                try:
                    result = resp.model_dump()
                except AttributeError:
                    result = resp.dict()
                
                code = result.get('code', 0)
                message = result.get('msg', '') or result.get('message', '')
                
                # 检查是否请求过于频繁（使用指数退避）
                if code == 3001008:
                    wait_time = RETRY_DELAY * (2 ** retry)
                    logger.warning(f"⚠️  令牌桶无令牌（第 {retry + 1}/{MAX_RETRIES} 次），等待 {wait_time} 秒...")
                    await asyncio.sleep(wait_time)
                    continue
                
                # 检查是否token过期或签名错误
                if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                    logger.warning(f"🔑 Token/签名错误 (code={code}): {message}")
                    # 只在第一次遇到时刷新token
                    if retry == 0:
                        logger.info(f"Token已过期，正在刷新...")
                        logger.info(f"当前token前10位: {self.token[:10] if self.token else 'None'}...")
                        if await self.init_token():
                            logger.info(f"Token刷新成功，新token前10位: {self.token[:10]}...")
                            logger.info(f"⏱️  等待10秒让服务器端token缓存更新...")
                            await asyncio.sleep(10)  # 增加到10秒
                            continue
                        else:
                            logger.error(f"Token刷新失败")
                            return None
                    else:
                        # 已经刷新过，但还是失败，说明有其他问题
                        logger.error(f"⚠️  Token已刷新过但还是签名错误！当前token前10位: {self.token[:10] if self.token else 'None'}...")
                        logger.error(f"⚠️  这可能是API服务器端缓存问题或时间同步问题")
                        wait_time = RETRY_DELAY * retry
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                
                # 检查其他错误
                if code != 0:
                    logger.warning(f"⚠️  {operation_name}返回错误: code={code}, message={message}")
                    if retry < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (retry + 1)
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ 达到最大重试次数，{operation_name}失败")
                        return None
                
                # 请求成功
                return result
                
            except Exception as e:
                logger.error(f"❌ {operation_name}异常: {str(e)}")
                if retry < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (retry + 1)
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ 达到最大重试次数")
                    return None
        
        return None
    
    async def get_fee_types(self) -> Optional[List[Dict[str, Any]]]:
        """
        查询费用类型列表
        
        Returns:
            List[Dict]: 费用类型列表，包含id, name, sort, fpoft_id等字段
            None: 查询失败
        """
        result = await self._handle_api_request(
            "/bd/fee/management/open/feeManagement/otherFee/type",
            "POST",
            None,
            "查询费用类型列表"
        )
        
        if not result:
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
        
        注意：调用此方法前，请确保已经调用init_token()获取了token
        
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
        if status_order is not None:
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
                
                if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                    logger.warning(f"🔑 Token/签名错误 (code={code}): {message}")
                    # 只在第一次遇到时刷新token
                    if retry == 0:
                        logger.info(f"Token已过期，正在刷新...")
                        if await self.init_token():
                            logger.info(f"Token刷新成功")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.error(f"Token刷新失败")
                            return None
                    else:
                        # 已经刷新过，等待后重试
                        wait_time = RETRY_DELAY * retry
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                
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
        
        注意：调用此方法前，请确保已经调用init_token()获取了token
        
        Args:
            numbers: 费用单号列表，上限200
        
        Returns:
            Dict: 作废结果
            None: 作废失败
        """
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
                
                if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                    logger.warning(f"🔑 Token/签名错误 (code={code}): {message}")
                    # 只在第一次遇到时刷新token
                    if retry == 0:
                        logger.info(f"Token已过期，正在刷新...")
                        if await self.init_token():
                            logger.info(f"Token刷新成功")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.error(f"Token刷新失败")
                            return None
                    else:
                        # 已经刷新过，等待后重试
                        wait_time = RETRY_DELAY * retry
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                
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
        
        注意：调用此方法前，请确保已经调用init_token()获取了token
        
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
        logger.info(f"  当前token前10位: {self.token[:10] if self.token else 'None'}...")
        
        # 详细诊断：打印前3个费用项的完整内容
        if fee_items:
            logger.info(f"  📋 前3个费用项的详细信息:")
            for idx, item in enumerate(fee_items[:3]):
                logger.info(f"    [{idx+1}]:")
                logger.info(f"      - MSKU: {item.get('dimension_value', 'N/A')}")
                logger.info(f"      - 店铺ID: {item.get('sids', 'N/A')} (类型: {type(item.get('sids')).__name__})")
                logger.info(f"      - 日期: {item.get('date', 'N/A')}")
                logger.info(f"      - 费用类型ID: {item.get('other_fee_type_id', 'N/A')} (类型: {type(item.get('other_fee_type_id')).__name__})")
                logger.info(f"      - 金额: {item.get('fee', 'N/A')} (类型: {type(item.get('fee')).__name__})")
                logger.info(f"      - 币种: {item.get('currency_code', 'N/A')}")
                logger.info(f"      - 备注: {item.get('remark', 'N/A')}")
                
                # 检查是否有特殊字符
                msku = str(item.get('dimension_value', ''))
                if any(ord(c) > 127 for c in msku):
                    logger.warning(f"      ⚠️  包含非ASCII字符！")
                
                # 检查金额精度
                fee_value = item.get('fee')
                if isinstance(fee_value, float):
                    logger.info(f"      - 金额原始值: {repr(fee_value)}")
        
        # 输出完整请求体大小
        import json
        req_body_json = json.dumps(req_body, ensure_ascii=False)
        logger.info(f"  📦 请求体大小: {len(req_body_json)} 字节")
        
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
                
                # 检查是否token过期或签名错误
                if code in [401, 403, 2001003, 2001005, 2001006, 3001001, 3001002]:
                    logger.error(f"🔑 Token/签名错误 (code={code}): {message}")
                    logger.error(f"📋 完整错误响应: {json.dumps(result, ensure_ascii=False, indent=2)}")
                    logger.error(f"🔍 诊断信息:")
                    logger.error(f"  - 当前token前10位: {self.token[:10] if self.token else 'None'}...")
                    logger.error(f"  - token_resp有效期: {self.token_resp.expires_in if self.token_resp else 'N/A'}秒")
                    logger.error(f"  - 重试次数: {retry + 1}/{MAX_RETRIES}")
                    logger.error(f"  - 请求体大小: {len(req_body_json)} 字节")
                    
                    # 🔍 添加签名计算的详细调试
                    import time
                    import copy
                    from lingxing.sign import SignBase
                    
                    timestamp = int(time.time())
                    gen_sign_params = copy.deepcopy(req_body)
                    gen_sign_params.update({
                        "app_key": self.op_api.app_id,
                        "access_token": self.token,
                        "timestamp": f'{timestamp}',
                    })
                    
                    # 格式化参数（用于签名）
                    formatted_params = SignBase.format_params(gen_sign_params)
                    
                    logger.error(f"🔐 签名计算详情:")
                    logger.error(f"  - timestamp: {timestamp}")
                    logger.error(f"  - 格式化参数总长度: {len(formatted_params)} 字符")
                    logger.error(f"  - 前300字符: {formatted_params[:300]}")
                    logger.error(f"  - 后300字符: {formatted_params[-300:]}")
                    
                    # 检查特殊字符
                    special_chars = set()
                    for char in req_body_json:
                        if ord(char) > 127:
                            special_chars.add(char)
                    if special_chars:
                        logger.error(f"  ⚠️  包含非ASCII字符: {list(special_chars)[:20]}")
                    
                    # 检查每个费用项的数据类型
                    logger.error(f"  📋 费用项数据类型检查（前3个）:")
                    for idx, item in enumerate(req_body['fee_items'][:3], 1):
                        logger.error(f"    [{idx}] sids类型={type(item['sids']).__name__}, fee类型={type(item['fee']).__name__}")
                        logger.error(f"        date类型={type(item['date']).__name__}, other_fee_type_id类型={type(item['other_fee_type_id']).__name__}")
                    
                    # 先重试几次（可能是速率限制），最后再刷新token
                    if retry < 2:
                        # 前2次重试：可能是速率限制，等待后重试（不刷新token）
                        wait_time = RETRY_DELAY * (retry + 1) * 2  # 增加等待时间：5秒、10秒
                        logger.warning(f"⚠️  签名错误可能是速率限制，等待 {wait_time} 秒后重试（不刷新token）...")
                        await asyncio.sleep(wait_time)
                        continue
                    elif retry == 2:
                        # 第3次：尝试刷新token
                        logger.info(f"Token已过期，正在刷新...")
                        if await self.init_token():
                            logger.info(f"Token刷新成功，新token前10位: {self.token[:10]}...")
                            logger.info(f"⏱️  等待10秒让服务器端token缓存更新...")
                            await asyncio.sleep(10)
                            continue
                        else:
                            logger.error(f"Token刷新失败")
                            # 继续重试，不返回None
                            wait_time = RETRY_DELAY * (retry + 1)
                            logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                            await asyncio.sleep(wait_time)
                            continue
                    else:
                        # 已经刷新过，但还是失败
                        logger.error(f"⚠️  Token已刷新过但还是签名错误！")
                        logger.error(f"⚠️  这可能是数据或签名计算问题，而不是token问题")
                        wait_time = RETRY_DELAY * retry
                        logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                        await asyncio.sleep(wait_time)
                        continue
                
                # 检查其他错误
                if code != 0:
                    logger.error(f"❌ API返回错误: code={code}, message={message}")
                    logger.error(f"📋 完整错误响应: {json.dumps(result, ensure_ascii=False, indent=2)}")
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
                logger.error(f"📋 异常详情:", exc_info=True)
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
    fee_item_remark = f"{dimension_value}-FeeTypeID:{fee_type_id}"
    
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
    从数据库读取利润报表数据（按月汇总）
    
    Args:
        start_date: 开始日期，格式：Y-m-d
        end_date: 结束日期，格式：Y-m-d
    
    Returns:
        List[Dict]: 利润报表数据列表，按月汇总
    """
    try:
        with db_cursor() as cursor:
            sql = """
                SELECT 
                    `MSKU`,
                    `店铺id`,
                    DATE_FORMAT(`统计日期`, '%%Y-%%m') as `年月`,
                    SUM(`商品成本附加费`) as `商品成本附加费`,
                    SUM(`头程成本附加费`) as `头程成本附加费`,
                    SUM(`录入费用单头程`) as `录入费用单头程`,
                    SUM(`汇损`) as `汇损`
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
            
            # 统计信息
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
            
            logger.info(f"✅ 从数据库读取到 {len(records)} 条按月汇总的利润报表数据（日期范围：{start_date} 至 {end_date}）")
            logger.info(f"   包含 {len(unique_msku_shop)} 个不同的(MSKU, 店铺ID)组合")
            logger.info(f"   涉及月份: {sorted(unique_months)}")
            
            return records
    except Exception as e:
        logger.error(f"❌ 读取利润报表数据失败: {str(e)}")
        return []


def fetch_profit_report_data_daily(target_date: str) -> List[Dict[str, Any]]:
    """
    从数据库读取利润报表数据（按日，不汇总）
    
    Args:
        target_date: 目标日期，格式：Y-m-d
    
    Returns:
        List[Dict]: 利润报表数据列表，按日返回
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
                    `录入费用单头程`,
                    `汇损`
                FROM `利润报表`
                WHERE `统计日期` = %s
                  AND (
                      (`商品成本附加费` IS NOT NULL AND `商品成本附加费` != 0) OR
                      (`头程成本附加费` IS NOT NULL AND `头程成本附加费` != 0) OR
                      (`录入费用单头程` IS NOT NULL AND `录入费用单头程` != 0) OR
                      (`汇损` IS NOT NULL AND `汇损` != 0)
                  )
                ORDER BY `店铺id`, `MSKU`
            """
            cursor.execute(sql, (target_date,))
            records = cursor.fetchall()
            
            if records:
                unique_msku_shop = set()
                for record in records:
                    msku = record.get('MSKU', '').strip()
                    shop_id = record.get('店铺id')
                    if msku and shop_id:
                        unique_msku_shop.add((msku, str(shop_id)))
                
                logger.info(f"✅ 从数据库读取到 {len(records)} 条日期为 {target_date} 的利润报表数据")
                logger.info(f"   包含 {len(unique_msku_shop)} 个不同的(MSKU, 店铺ID)组合")
            else:
                logger.info(f"ℹ️  日期 {target_date} 没有需要创建费用单的数据")
            
            return records
    except Exception as e:
        logger.error(f"❌ 读取日期 {target_date} 的利润报表数据失败: {str(e)}")
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
    logger.info(f"将作废日期范围 {start_date} 至 {end_date} 内所有【已处理】状态的费用单")
    logger.info("=" * 80)
    
    # 分批处理配置
    query_batch_size = 500  # 每次查询500条
    discard_batch_size = 200  # 每次作废200个
    
    total_queried = 0
    total_discarded = 0
    
    # 查询需要作废的费用单（只查询"已处理"状态）
    all_pending_numbers = []
    
    status = 3  # 只查询已处理状态
    status_name = "已处理"
    logger.info(f"查询状态为【{status_name}】的费用单...")
    
    # 先查询总数
    first_query = await fee_mgmt.get_fee_list(
        offset=0,
        length=1,
        date_type="date",
        start_date=start_date,
        end_date=end_date,
        other_fee_type_ids=fee_type_ids,
        status_order=status
    )
    
    status_total = 0
    if first_query:
        data = first_query.get('data', {})
        status_total = data.get('total', 0)
        logger.info(f"  状态【{status_name}】共有 {status_total} 条费用单")
    
    if status_total > 0:
        await asyncio.sleep(REQUEST_DELAY)
        
        # 分批查询该状态的费用单
        status_offset = 0
        status_queried = 0
        
        while status_queried < status_total:
            query_result = await fee_mgmt.get_fee_list(
                offset=status_offset,
                length=query_batch_size,
                date_type="date",
                start_date=start_date,
                end_date=end_date,
                other_fee_type_ids=fee_type_ids,
                status_order=status
            )
            
            if not query_result:
                break
            
            data = query_result.get('data', {})
            records = data.get('records', [])
            
            if not records:
                break
            
            status_queried += len(records)
            total_queried += len(records)
            
            # 收集费用单号
            for record in records:
                number = record.get('number')
                if number:
                    all_pending_numbers.append(number)
            
            logger.info(f"  已查询 {status_queried}/{status_total} 条【{status_name}】费用单，"
                       f"累计收集 {len(all_pending_numbers)} 个待作废")
            
            if len(records) < query_batch_size:
                break
            
            status_offset += query_batch_size
            await asyncio.sleep(REQUEST_DELAY)
    
    logger.info(f"\n总计查询: {total_queried} 条【{status_name}】费用单")
    logger.info(f"待作废: {len(all_pending_numbers)} 个费用单")
    
    # 分批作废收集到的所有费用单
    if all_pending_numbers:
        logger.info(f"\n开始作废 {len(all_pending_numbers)} 个费用单...")
        for i in range(0, len(all_pending_numbers), discard_batch_size):
            batch_numbers = all_pending_numbers[i:i + discard_batch_size]
            batch_num = i // discard_batch_size + 1
            total_batches = (len(all_pending_numbers) + discard_batch_size - 1) // discard_batch_size
            
            logger.info(f"  准备作废第 {batch_num}/{total_batches} 批，共 {len(batch_numbers)} 个费用单...")
            discard_result = await fee_mgmt.discard_fee_orders(batch_numbers)
            
            if discard_result:
                total_discarded += len(batch_numbers)
                logger.info(f"  ✅ 成功作废 {len(batch_numbers)} 个费用单（累计 {total_discarded} 个）")
            else:
                logger.error(f"  ❌ 作废失败，{len(batch_numbers)} 个费用单未作废")
            
            await asyncio.sleep(REQUEST_DELAY)
    else:
        logger.info("  没有需要作废的费用单")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"✅ 作废完成：")
    logger.info(f"   总查询: {total_queried} 条费用单（已排除已作废状态）")
    logger.info(f"   成功作废: {total_discarded} 个费用单")
    logger.info("=" * 80)
    
    return True


async def create_fee_orders_from_profit_report(
    fee_mgmt: FeeManagement,
    profit_data: List[Dict[str, Any]],
    fee_type_ids: Dict[str, int]
) -> int:
    """
    根据利润报表数据创建费用单（按月汇总）
    
    Args:
        fee_mgmt: 费用管理实例
        profit_data: 利润报表数据列表（已按月汇总）
        fee_type_ids: 费用类型ID字典，包含：商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id
    
    Returns:
        int: 成功创建的费用单数量
    """
    logger.info("=" * 80)
    logger.info("步骤2: 根据利润报表数据创建费用单（按月汇总）")
    logger.info("=" * 80)
    
    # 按年月和店铺ID分组
    from collections import defaultdict
    from datetime import date, datetime
    
    grouped_data = defaultdict(list)
    skipped_records = []  # 记录被跳过的记录
    
    for record in profit_data:
        year_month = record.get('年月')
        shop_id = record.get('店铺id')
        msku = record.get('MSKU', '').strip()
        
        # 检查数据完整性
        if not year_month:
            skipped_records.append({
                'reason': '年月为空',
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
        
        key = (str(year_month), str(shop_id))
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
    
    logger.info(f"  共 {len(profit_data)} 条数据（已按月汇总），按年月和店铺分组后共 {len(grouped_data)} 组")
    
    success_count = 0
    total_count = 0
    skipped_groups = []  # 记录被跳过的组
    
    # 按年月和店铺分组创建费用单
    for (year_month, shop_id), records in grouped_data.items():
        total_count += 1
        
        # 统计该组包含的MSKU
        group_mskus = set()
        for record in records:
            msku = record.get('MSKU', '').strip()
            if msku:
                group_mskus.add(msku)
        
        logger.info(f"\n处理第 {total_count}/{len(grouped_data)} 组：年月={year_month}, 店铺ID={shop_id}, 包含 {len(records)} 条记录, {len(group_mskus)} 个MSKU")
        
        # 数据已经按月汇总，直接构建费用明细项
        msku_fees = {}
        
        for record in records:
            msku = record.get('MSKU', '').strip()
            if not msku:
                continue
            
            # 获取汇总后的金额（数据库已经SUM了）
            cg_price_additional_fee = float(record.get('商品成本附加费', 0) or 0)
            cg_transport_additional_fee = float(record.get('头程成本附加费', 0) or 0)
            recorded_freight = float(record.get('录入费用单头程', 0) or 0)
            exchange_loss = float(record.get('汇损', 0) or 0)
            
            msku_fees[msku] = {
                '商品成本附加费': cg_price_additional_fee,
                '头程成本附加费': cg_transport_additional_fee,
                '录入费用单头程': recorded_freight,
                '汇损': exchange_loss
            }
        
        # 构建费用明细项（数据已按月汇总）
        fee_items = []
        msku_count = 0
        
        for msku, fees in msku_fees.items():
            msku_count += 1
            
            # 商品成本附加费
            if fees['商品成本附加费'] != 0:
                fee_items.append({
                    "sids": [int(shop_id)],
                    "dimension_value": msku,
                    "date": year_month,  # 使用年月格式：Y-m
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
                    "date": year_month,  # 使用年月格式：Y-m
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
                    "date": year_month,  # 使用年月格式：Y-m
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
                    "date": year_month,  # 使用年月格式：Y-m
                    "other_fee_type_id": fee_type_ids['汇损_id'],
                    "fee": fees['汇损'],
                    "currency_code": "CNY",
                    "remark": f"{msku}-ExchangeLoss"
                })
        
        logger.info(f"  该组包含 {msku_count} 个MSKU，生成 {len(fee_items)} 个费用明细项")
        
        if not fee_items:
            logger.warning(f"  ⚠️  跳过：该组没有需要创建的费用项（可能所有费用都为0）")
            skipped_groups.append({
                'year_month': year_month,
                'shop_id': shop_id,
                'msku_count': msku_count,
                'reason': '所有费用都为0'
            })
            continue
        
        logger.info(f"  准备创建费用单，包含 {len(fee_items)} 个费用明细项")
        
        # 如果费用明细项数量超过限制，分批创建
        if len(fee_items) > MAX_FEE_ITEMS_PER_ORDER:
            logger.info(f"  ⚠️  费用明细项数量({len(fee_items)})超过限制({MAX_FEE_ITEMS_PER_ORDER})，将分批创建")
            
            # 动态调整批次大小：根据格式化参数长度
            def estimate_formatted_params_length(batch_items, batch_num):
                """估算格式化参数的长度"""
                import time
                import copy
                from lingxing.sign import SignBase
                
                # 构建临时请求体
                temp_req_body = {
                    "submit_type": 2,
                    "dimension": 1,
                    "apportion_rule": 2,
                    "is_request_pool": 0,
                    "remark": f"Auto-{year_month}-{batch_num}",
                    "fee_items": batch_items
                }
                
                # 模拟签名计算
                timestamp = int(time.time())
                gen_sign_params = copy.deepcopy(temp_req_body)
                gen_sign_params.update({
                    "app_key": fee_mgmt.op_api.app_id,
                    "access_token": "temp_token_for_length_check",
                    "timestamp": f'{timestamp}',
                })
                
                formatted_params = SignBase.format_params(gen_sign_params)
                return len(formatted_params)
            
            # 智能分批：动态调整批次大小
            current_batch_size = MAX_FEE_ITEMS_PER_ORDER
            batch_count = (len(fee_items) + current_batch_size - 1) // current_batch_size
            logger.info(f"  初始计划分成 {batch_count} 批，每批 {current_batch_size} 项")
            
            batch_idx = 0
            processed_count = 0
            
            while processed_count < len(fee_items):
                # 尝试当前批次大小
                start_idx = processed_count
                end_idx = min(start_idx + current_batch_size, len(fee_items))
                batch_fee_items = fee_items[start_idx:end_idx]
                
                # 估算格式化参数长度
                estimated_length = estimate_formatted_params_length(
                    batch_fee_items, 
                    batch_idx + 1
                )
                
                # 如果超过限制，减小批次大小
                if estimated_length > MAX_FORMATTED_PARAMS_LENGTH:
                    logger.warning(f"  ⚠️  批次 {batch_idx + 1} 的格式化参数长度({estimated_length})超过限制({MAX_FORMATTED_PARAMS_LENGTH})")
                    logger.warning(f"  ⚠️  将减小批次大小: {current_batch_size} -> {current_batch_size - 5}")
                    current_batch_size = max(10, current_batch_size - 5)  # 最小10项
                    batch_count = (len(fee_items) - processed_count + current_batch_size - 1) // current_batch_size
                    continue  # 重新计算当前批次
                
                batch_idx += 1
                logger.info(f"  创建第 {batch_idx} 批，包含 {len(batch_fee_items)} 个费用明细项（格式化参数长度: {estimated_length} 字符）")
                
                # 创建费用单（内部已有重试和token刷新逻辑）
                result = await fee_mgmt.create_fee_order(
                    submit_type=2,  # 2=提交
                    dimension=1,  # 1=msku
                    apportion_rule=2,  # 2=按销量
                    is_request_pool=0,  # 0=否
                    remark=f"Auto-{year_month}-{batch_idx}",
                    fee_items=batch_fee_items
                )
                
                if result:
                    success_count += 1
                    logger.info(f"  ✅ 第 {batch_idx} 批费用单创建成功")
                    processed_count = end_idx  # 更新已处理数量
                    
                    # 每N批后休息一次，避免累积速率限制
                    if batch_idx % REST_BATCH_INTERVAL == 0:
                        logger.info(f"  ⏸️  已创建 {batch_idx} 批，休息 {REST_DURATION} 秒以避免累积速率限制...")
                        await asyncio.sleep(REST_DURATION)
                else:
                    error_msg = f"第 {batch_idx} 批费用单创建失败 (年月={year_month}, 店铺ID={shop_id})"
                    logger.error(f"  ❌ {error_msg}")
                    logger.error(f"  包含 {len(batch_fee_items)} 个费用明细项")
                    logger.error(f"  已成功创建 {success_count} 个费用单，现在停止执行")
                    raise RuntimeError(f"费用单创建失败: {error_msg}，请检查日志并重试")
                
                await asyncio.sleep(REQUEST_DELAY)
        else:
            # 费用明细项数量在限制内，直接创建
            result = await fee_mgmt.create_fee_order(
                submit_type=2,  # 2=提交
                dimension=1,  # 1=msku
                apportion_rule=2,  # 2=按销量
                is_request_pool=0,  # 0=否
                remark=f"Auto-{year_month}",
                fee_items=fee_items
            )
            
            if result:
                success_count += 1
                logger.info(f"  ✅ 费用单创建成功")
            else:
                error_msg = f"费用单创建失败 (年月={year_month}, 店铺ID={shop_id})"
                logger.error(f"  ❌ {error_msg}")
                logger.error(f"  包含 {len(fee_items)} 个费用明细项")
                logger.error(f"  已成功创建 {success_count} 个费用单，现在停止执行")
                raise RuntimeError(f"费用单创建失败: {error_msg}，请检查日志并重试")
            
            await asyncio.sleep(REQUEST_DELAY)
    
    # 最终统计：验证所有店铺的数据是否都被处理
    processed_shops = set()
    processed_msku_shops = set()
    
    for (year_month, shop_id), records in grouped_data.items():
        processed_shops.add(str(shop_id))
        for record in records:
            msku = record.get('MSKU', '').strip()
            if msku:
                processed_msku_shops.add((msku, str(shop_id)))
    
    logger.info(f"\n✅ 费用单创建完成：成功创建 {success_count} 个费用单（来自 {total_count} 个年月+店铺组合）")
    logger.info(f"   处理了 {len(processed_shops)} 个不同的店铺ID")
    logger.info(f"   处理了 {len(processed_msku_shops)} 个(MSKU, 店铺ID)组合")
    
    # 报告被跳过的组
    if skipped_groups:
        logger.warning(f"\n  ⚠️  警告：有 {len(skipped_groups)} 个组被跳过（所有费用都为0）")
        for skip in skipped_groups[:10]:  # 只显示前10个
            logger.warning(f"    年月={skip['year_month']}, 店铺ID={skip['shop_id']}, MSKU数量={skip['msku_count']}")
    
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
    if skipped_groups or missing:
        logger.warning(f"\n  ⚠️  总结：")
        logger.warning(f"    成功创建: {success_count} 个费用单")
        logger.warning(f"    被跳过: {len(skipped_groups)} 个组（费用为0）")
        logger.warning(f"    数据遗漏: {len(missing)} 个(MSKU, 店铺ID)组合")
    else:
        logger.info(f"\n  ✅ 所有数据都已成功处理，无遗漏")
    
    return success_count


async def main(start_date: str = None, end_date: str = None, daily: bool = False):
    """
    主函数 - 从数据库读取利润报表并创建费用单（按月汇总）
    
    Args:
        start_date: 开始日期，格式：Y-m-d，默认：上个月1号
        end_date: 结束日期，格式：Y-m-d，默认：上个月最后一天
        daily: 是否按天处理（已废弃，现在按月处理），默认：False
    """
    from datetime import datetime, date, timedelta
    from calendar import monthrange
    
    # 确定日期范围（默认为上个月）
    if end_date is None or start_date is None:
        today = date.today()
        
        # 计算上个月
        # 如果当前是1月，上个月就是去年12月
        if today.month == 1:
            last_month_year = today.year - 1
            last_month = 12
        else:
            last_month_year = today.year
            last_month = today.month - 1
        
        # 上个月的第一天
        last_month_first_day = date(last_month_year, last_month, 1)
        
        # 上个月的最后一天
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
    logger.info(f"处理模式: 按月汇总处理（默认：上个月）")
    logger.info("=" * 80)
    
    # 初始化费用管理
    fee_mgmt = FeeManagement()
    
    # 获取访问令牌（学习其他文件的做法）
    logger.info("获取访问令牌...")
    if not await fee_mgmt.init_token():
        logger.error("❌ 无法获取访问令牌")
        return
    
    # 步骤0: 查询费用类型列表
    logger.info("查询费用类型列表...")
    fee_types = await fee_mgmt.get_fee_types()
    
    if not fee_types:
        logger.error("❌ 无法获取费用类型列表")
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
        return
    
    fee_type_ids = {
        '商品成本附加费_id': 商品成本附加费_id,
        '头程成本附加费_id': 头程成本附加费_id,
        '头程费用_id': 头程费用_id,
        '汇损_id': 汇损_id
    }
    
    # 步骤1: 作废已有费用单
    await discard_existing_fee_orders(
        fee_mgmt,
        start_date,
        end_date,
        [商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id]
    )
    
    await asyncio.sleep(REQUEST_DELAY)
    
    # 步骤2: 从数据库读取利润报表数据（按月汇总）
    logger.info("=" * 80)
    logger.info("步骤2: 从数据库读取利润报表数据（按月汇总）")
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
    作废指定日期范围内的四个费用类型的费用单
    
    Args:
        start_date: 开始日期，格式：Y-m-d
        end_date: 结束日期，格式：Y-m-d
    """
    from datetime import date
    
    logger.info("=" * 80)
    logger.info("🗑️  作废费用单任务")
    logger.info("=" * 80)
    logger.info(f"日期范围: {start_date} 至 {end_date}")
    logger.info("费用类型: 商品成本附加费, 头程成本附加费, 头程费用, 汇损")
    logger.info(f"说明: 作废指定日期范围内所有相关费用类型的费用单（默认：上个月）")
    logger.info("=" * 80)
    
    # 初始化费用管理
    fee_mgmt = FeeManagement()
    
    # 获取访问令牌
    logger.info("获取访问令牌...")
    if not await fee_mgmt.init_token():
        logger.error("❌ 无法获取访问令牌")
        return
    
    # 查询费用类型列表
    logger.info("查询费用类型列表...")
    fee_types = await fee_mgmt.get_fee_types()
    
    if not fee_types:
        logger.error("❌ 无法获取费用类型列表")
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
        return
    
    fee_type_ids = [商品成本附加费_id, 头程成本附加费_id, 头程费用_id, 汇损_id]
    
    logger.info(f"找到费用类型ID:")
    logger.info(f"  商品成本附加费_id: {商品成本附加费_id}")
    logger.info(f"  头程成本附加费_id: {头程成本附加费_id}")
    logger.info(f"  头程费用_id: {头程费用_id}")
    logger.info(f"  汇损_id: {汇损_id}")
    
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
    
    parser = argparse.ArgumentParser(description='从利润报表创建费用单（按月汇总）')
    parser.add_argument('--start-date', type=str, default=None, 
                       help='开始日期，格式：Y-m-d，默认：上个月1号')
    parser.add_argument('--end-date', type=str, default=None,
                       help='结束日期，格式：Y-m-d，默认：上个月最后一天')
    parser.add_argument('--daily', action='store_true',
                       help='（已废弃）现在统一按月汇总处理')
    parser.add_argument('--discard-only', action='store_true',
                       help='仅作废费用单，不创建新费用单')
    
    args = parser.parse_args()
    
    try:
        if args.discard_only:
            # 仅作废模式 - 默认作废上个月的费用单
            from datetime import date
            from calendar import monthrange
            
            if args.start_date or args.end_date:
                # 用户指定了日期
                start_date = args.start_date
                end_date = args.end_date
            else:
                # 默认使用上个月
                today = date.today()
                
                # 计算上个月
                if today.month == 1:
                    last_month_year = today.year - 1
                    last_month = 12
                else:
                    last_month_year = today.year
                    last_month = today.month - 1
                
                # 上个月的第一天和最后一天
                last_month_first_day = date(last_month_year, last_month, 1)
                last_day_of_month = monthrange(last_month_year, last_month)[1]
                last_month_last_day = date(last_month_year, last_month, last_day_of_month)
                
                start_date = last_month_first_day.strftime('%Y-%m-%d')
                end_date = last_month_last_day.strftime('%Y-%m-%d')
            
            asyncio.run(discard_fee_orders_by_date_range(start_date, end_date))
        else:
            # 正常模式：创建费用单
            asyncio.run(main(start_date=args.start_date, end_date=args.end_date, daily=args.daily))
    except KeyboardInterrupt:
        logger.info("⚠️  任务被用户中断")
    except Exception as e:
        logger.error(f"❌ 任务执行失败: {str(e)}", exc_info=True)
        raise

