#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
飞书API封装模块
提供飞书多维表格的读写操作
"""
import httpx
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime

try:
    import lark_oapi as lark
    from lark_oapi.api.bitable.v1 import *
    LARK_SDK_AVAILABLE = True
except ImportError:
    LARK_SDK_AVAILABLE = False

from .config import settings
from .logger import get_logger

logger = get_logger('feishu')


class FeishuClient:
    """飞书API客户端"""
    
    def __init__(self, 
                 app_token: str,
                 table_id: str,
                 app_id: Optional[str] = None,
                 app_secret: Optional[str] = None,
                 view_id: Optional[str] = None):
        """
        初始化飞书客户端
        
        Args:
            app_token: 多维表格ID（必填）
            table_id: 表格ID（必填）
            app_id: 飞书应用ID（如果为None则从配置读取）
            app_secret: 飞书应用密钥（如果为None则从配置读取）
            view_id: 视图ID（可选）
        """
        # 全局认证信息（从配置读取）
        self.app_id = app_id or settings.FEISHU_APP_ID
        self.app_secret = app_secret or settings.FEISHU_APP_SECRET
        self.api_base = settings.FEISHU_API_BASE
        
        # 表级配置（必须显式传入）
        self.app_token = app_token
        self.table_id = table_id
        self.view_id = view_id
        
        self._access_token = None
    
    async def get_access_token(self, retry_count: int = 3) -> str:
        """
        获取飞书访问令牌（带重试机制）
        
        Args:
            retry_count: 重试次数（默认3次）
        """
        url = f"{self.api_base}/auth/v3/tenant_access_token/internal"
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        last_error = None
        for attempt in range(retry_count):
            try:
                timeout = httpx.Timeout(60.0, connect=10.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(url, headers=headers, json=data)
                    result = response.json()
                    
                    if result.get("code") == 0:
                        self._access_token = result.get("tenant_access_token")
                        logger.info("获取飞书访问令牌成功")
                        return self._access_token
                    else:
                        error_msg = f"获取飞书访问令牌失败: {result.get('msg')}"
                        logger.error(error_msg)
                        raise Exception(error_msg)
            except asyncio.CancelledError:
                logger.warning("获取访问令牌时被中断")
                raise  # 重新抛出，让调用者知道被中断了
            except KeyboardInterrupt:
                logger.warning("获取访问令牌时被用户中断")
                raise  # 重新抛出，让调用者知道被中断了
            except Exception as e:
                last_error = e
                if attempt < retry_count - 1:
                    wait_time = (attempt + 1) * 2  # 指数退避：2秒、4秒、6秒
                    logger.warning(f"获取访问令牌失败（尝试 {attempt + 1}/{retry_count}），{wait_time}秒后重试: {e}")
                    try:
                        await asyncio.sleep(wait_time)
                    except asyncio.CancelledError:
                        raise
                else:
                    logger.error(f"获取访问令牌失败，已重试 {retry_count} 次")
        
        # 所有重试都失败了
        raise Exception(f"获取飞书访问令牌失败（已重试 {retry_count} 次）: {last_error}")
    
    async def get_table_fields(self) -> Dict[str, str]:
        """
        获取表格字段信息
        
        Returns:
            Dict[str, str]: 字段ID到字段名称的映射
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            result = response.json()
            
            if result.get("code") == 0:
                fields = result.get("data", {}).get("items", [])
                field_map = {}
                for field in fields:
                    field_name = field.get("field_name", "")
                    field_id = field.get("field_id", "")
                    field_type = field.get("type", "未知")
                    field_map[field_id] = field_name
                    # 调试：打印字段详细信息
                    logger.debug(f"字段: {field_name} (ID: {field_id}, 类型: {field_type})")
                logger.info(f"获取到 {len(field_map)} 个字段")
                return field_map
            else:
                error_msg = f"获取表格字段失败: {result.get('msg')}"
                logger.error(error_msg)
                raise Exception(error_msg)
    
    async def get_field_info(self, field_name: str) -> Dict[str, Any]:
        """
        获取字段的详细信息（包括精度）
        
        Args:
            field_name: 字段名
            
        Returns:
            Dict[str, Any]: 字段信息，包含 field_id, type, property 等
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            result = response.json()
            
            if result.get("code") == 0:
                fields = result.get("data", {}).get("items", [])
                for field in fields:
                    if field.get("field_name") == field_name:
                        return field
            return {}
    
    async def read_records(self, page_size: int = 500) -> List[Dict[str, Any]]:
        """
        读取飞书多维表格数据
        
        Args:
            page_size: 每页数据量（最大500）
            
        Returns:
            List[Dict[str, Any]]: 记录列表
        """
        if not self._access_token:
            await self.get_access_token()
        
        all_records = []
        page_token = None
        timeout = httpx.Timeout(60.0, connect=10.0)
        
        while True:
            url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            params = {"page_size": page_size}
            if self.view_id:
                params["view_id"] = self.view_id
            if page_token:
                params["page_token"] = page_token
            
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers, params=params)
                result = response.json()
                
                if result.get("code") == 0:
                    data = result.get("data", {})
                    records = data.get("items", [])
                    if records:
                        all_records.extend(records)
                    
                    has_more = data.get("has_more", False)
                    page_token = data.get("page_token")
                    
                    logger.debug(f"已读取 {len(all_records)} 条记录...")
                    
                    if not has_more or not page_token:
                        break
                else:
                    error_msg = f"读取表格数据失败: {result.get('msg')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        
        logger.info(f"共读取 {len(all_records)} 条记录")
        return all_records
    
    async def delete_all_records(self) -> int:
        """
        清空飞书多维表格的所有记录
        
        Returns:
            int: 删除的记录数量
        """
        if not self._access_token:
            await self.get_access_token()
        
        logger.info("正在获取所有记录ID...")
        all_record_ids = []
        page_token = None
        # 增加超时时间，因为可能有很多记录需要获取
        timeout = httpx.Timeout(120.0, connect=30.0)
        
        # 获取所有记录ID
        while True:
            url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url, headers=headers, params=params)
                result = response.json()
                
                if result.get("code") == 0:
                    data = result.get("data", {}) or {}
                    records = data.get("items")
                    
                    # 确保 records 是列表类型
                    if records is None:
                        records = []
                    elif not isinstance(records, list):
                        logger.warning(f"records 不是列表类型: {type(records)}, 值: {records}")
                        records = []
                    
                    for record in records:
                        if record and isinstance(record, dict):
                            record_id = record.get("record_id")
                            if record_id:
                                all_record_ids.append(record_id)
                    
                    has_more = data.get("has_more", False)
                    page_token = data.get("page_token")
                    
                    if not has_more or not page_token:
                        break
                else:
                    error_code = result.get("code")
                    error_msg = result.get("msg", "未知错误")
                    logger.error(f"获取记录ID失败 [code: {error_code}]: {error_msg}")
                    logger.error(f"完整响应: {result}")
                    raise Exception(f"获取记录ID失败 [code: {error_code}]: {error_msg}")
        
        if not all_record_ids:
            logger.info("没有需要删除的记录")
            return 0
        
        logger.info(f"找到 {len(all_record_ids)} 条记录需要删除")
        
        # 批量删除记录（每次最多500条）
        batch_size = 500
        deleted_count = 0
        total_batches = (len(all_record_ids) + batch_size - 1) // batch_size
        
        logger.info(f"开始批量删除，共 {total_batches} 批...")
        
        # 优先使用SDK，如果不可用则使用HTTP方式
        if LARK_SDK_AVAILABLE:
            # 使用SDK方式删除
            def delete_with_sdk(batch_ids: List[str], access_token: str) -> bool:
                """使用SDK同步删除记录"""
                try:
                    client = lark.Client.builder() \
                        .enable_set_token(True) \
                        .log_level(lark.LogLevel.INFO) \
                        .build()
                    
                    request = BatchDeleteAppTableRecordRequest.builder() \
                        .app_token(self.app_token) \
                        .table_id(self.table_id) \
                        .request_body(BatchDeleteAppTableRecordRequestBody.builder()
                            .records(batch_ids)
                            .build()) \
                        .build()
                    
                    option = lark.RequestOption.builder().tenant_access_token(access_token).build()
                    response = client.bitable.v1.app_table_record.batch_delete(request, option)
                    
                    if response.success():
                        return True
                    else:
                        logger.error(f"SDK删除失败: code={response.code}, msg={response.msg}")
                        return False
                except Exception as e:
                    logger.error(f"SDK删除异常: {e}")
                    return False
            
            # 使用线程池执行同步SDK调用
            loop = asyncio.get_event_loop()
            for batch_idx, i in enumerate(range(0, len(all_record_ids), batch_size), 1):
                batch_ids = all_record_ids[i:i+batch_size]
                success = await loop.run_in_executor(None, delete_with_sdk, batch_ids, self._access_token)
                
                if success:
                    deleted_count += len(batch_ids)
                    progress = (deleted_count / len(all_record_ids)) * 100
                    logger.info(f"  删除进度: {batch_idx}/{total_batches} 批，已删除 {deleted_count}/{len(all_record_ids)} 条 ({progress:.1f}%)")
                else:
                    error_msg = f"删除记录失败（批次 {batch_idx}）"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        else:
            # 使用HTTP方式删除（原有方式）
            delete_url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_delete"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            
            for batch_idx, i in enumerate(range(0, len(all_record_ids), batch_size), 1):
                batch_ids = all_record_ids[i:i+batch_size]
                data = {"records": batch_ids}
                
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(delete_url, headers=headers, json=data)
                    result = response.json()
                    
                    if result.get("code") == 0:
                        deleted_count += len(batch_ids)
                        progress = (deleted_count / len(all_record_ids)) * 100
                        logger.info(f"  删除进度: {batch_idx}/{total_batches} 批，已删除 {deleted_count}/{len(all_record_ids)} 条 ({progress:.1f}%)")
                    else:
                        error_msg = f"删除记录失败: {result.get('msg')}"
                        logger.error(error_msg)
                        raise Exception(error_msg)
        
        logger.info(f"✓ 成功清空 {deleted_count} 条记录")
        return deleted_count
    
    async def write_records(self, records: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        批量写入记录到飞书多维表格
        
        Args:
            records: 记录列表，每个记录是一个字典，键为字段名，值为字段值
            batch_size: 每批写入的记录数（最大500）
            
        Returns:
            int: 成功写入的记录数量
        """
        if not self._access_token:
            await self.get_access_token()
        
        if not records:
            logger.warning("没有需要写入的记录")
            return 0
        
        # 获取字段映射（字段名 -> 字段ID）
        logger.info("正在获取表格字段信息...")
        field_id_to_name = await self.get_table_fields()
        field_name_to_id = {name: fid for fid, name in field_id_to_name.items()}
        
        # 转换数据格式：直接使用字段名（参考官方SDK示例）
        logger.info("正在转换数据格式...")
        
        # 定义需要跳过的系统字段
        system_fields = {'多行文本', '名称', '创建时间', '更新时间', '创建人', '更新人'}
        
        converted_records = []
        for record in records:
            fields = {}
            for field_name, field_value in record.items():
                # 跳过系统字段
                if field_name in system_fields:
                    continue
                    
                # 检查字段是否存在
                if field_name in field_name_to_id:
                    # 直接使用字段名（参考官方SDK示例，使用字段名而不是字段ID）
                    # 转换值为飞书API格式
                    fields[field_name] = self._convert_value_to_feishu_format(field_value)
                else:
                    logger.warning(f"字段 '{field_name}' 在表格中不存在，跳过")
            
            if fields:
                converted_records.append({"fields": fields})
        
        if not converted_records:
            logger.warning("没有有效的数据需要写入")
            return 0
        
        logger.info(f"准备写入 {len(converted_records)} 条记录")
        
        # 批量写入
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        written_count = 0
        invalid_field_names = set()  # 记录无效的字段名
        
        for i in range(0, len(converted_records), batch_size):
            batch = converted_records[i:i+batch_size]
            
            # 过滤掉已知的无效字段
            if invalid_field_names:
                for record in batch:
                    fields = record.get("fields", {})
                    for invalid_name in invalid_field_names:
                        fields.pop(invalid_name, None)
            
            data = {"records": batch}
            
            # 调试：打印第一条记录的详细信息
            if i == 0 and batch:
                logger.debug(f"第一条记录的字段名: {list(batch[0].get('fields', {}).keys())}")
                for field_name, field_value in batch[0].get('fields', {}).items():
                    logger.debug(f"  字段 {field_name}: {field_value} (类型: {type(field_value).__name__})")
            
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=data)
                result = response.json()
                
                if result.get("code") == 0:
                    written_count += len(batch)
                    logger.info(f"已写入 {written_count}/{len(converted_records)} 条记录...")
                else:
                    error_msg = f"写入记录失败: {result.get('msg')}"
                    
                    # 如果是字段未找到错误，尝试识别并过滤无效字段
                    if result.get("code") == 1254045:  # FieldNameNotFound
                        error_detail = result.get("error", {}).get("message", "")
                        logger.warning(f"字段未找到错误: {error_detail}")
                        
                        # 尝试从错误信息中提取字段名或字段ID
                        import re
                        # 先尝试匹配字段ID（如果错误信息中包含字段ID）
                        field_id_match = re.search(r"fields\.(fld\w+)", error_detail)
                        if field_id_match:
                            invalid_field_id = field_id_match.group(1)
                            invalid_field_name = field_id_to_name.get(invalid_field_id, invalid_field_id)
                            logger.warning(f"检测到无效字段: {invalid_field_name} ({invalid_field_id})，将过滤此字段后重试...")
                            
                            # 记录无效字段名，以便后续批次自动过滤
                            invalid_field_names.add(invalid_field_name)
                            
                            # 过滤掉无效字段
                            for record in batch:
                                fields = record.get("fields", {})
                                if invalid_field_name in fields:
                                    del fields[invalid_field_name]
                            
                            # 重新尝试写入
                            data = {"records": batch}
                            response = await client.post(url, headers=headers, json=data)
                            result = response.json()
                            
                            if result.get("code") == 0:
                                written_count += len(batch)
                                logger.info(f"已写入 {written_count}/{len(converted_records)} 条记录（已过滤无效字段 {invalid_field_name}）...")
                                continue
                            else:
                                logger.error(f"过滤字段后仍然失败: {result.get('msg')}")
                        else:
                            # 如果错误信息中没有字段ID，尝试匹配字段名
                            field_name_match = re.search(r"'([^']+)'", error_detail)
                            if field_name_match:
                                invalid_field_name = field_name_match.group(1)
                                logger.warning(f"检测到无效字段名: {invalid_field_name}，将过滤此字段后重试...")
                                invalid_field_names.add(invalid_field_name)
                                
                                # 过滤掉无效字段
                                for record in batch:
                                    fields = record.get("fields", {})
                                    if invalid_field_name in fields:
                                        del fields[invalid_field_name]
                                
                                # 重新尝试写入
                                data = {"records": batch}
                                response = await client.post(url, headers=headers, json=data)
                                result = response.json()
                                
                                if result.get("code") == 0:
                                    written_count += len(batch)
                                    logger.info(f"已写入 {written_count}/{len(converted_records)} 条记录（已过滤无效字段 {invalid_field_name}）...")
                                    continue
                        
                        logger.error(f"字段映射: {field_name_to_id}")
                        logger.error(f"字段ID到名称映射: {field_id_to_name}")
                        if batch:
                            logger.error(f"第一条记录的字段名: {list(batch[0].get('fields', {}).keys())}")
                    
                    # 如果是记录数超过限制的错误，提供更详细的错误信息
                    if result.get("code") == 1254103:  # RecordExceedLimit
                        error_detail = result.get("error", {}).get("message", "")
                        logger.error(f"记录数超过限制: {error_detail}")
                        logger.error(f"当前尝试写入: {len(converted_records)} 条记录")
                        logger.error(f"已成功写入: {written_count} 条记录")
                        logger.error(f"飞书多维表的最大记录数为 20000 条")
                        logger.error(f"建议：请先清空表中的旧数据，或减少要写入的记录数")
                        raise Exception(f"记录数超过限制: 飞书多维表最大支持 20000 条记录，当前尝试写入 {len(converted_records)} 条记录（已写入 {written_count} 条）。请先清空表中的旧数据。")
                    
                    # 如果是"Data not ready"错误（1254607），添加重试机制
                    if result.get("code") == 1254607:  # DataNotReady
                        max_retries = 3
                        retry_delay = 5  # 等待5秒后重试
                        for retry in range(max_retries):
                            logger.warning(f"数据未就绪错误（尝试 {retry + 1}/{max_retries}），{retry_delay}秒后重试...")
                            await asyncio.sleep(retry_delay)
                            
                            # 重新尝试写入
                            response = await client.post(url, headers=headers, json=data)
                            result = response.json()
                            
                            if result.get("code") == 0:
                                written_count += len(batch)
                                logger.info(f"已写入 {written_count}/{len(converted_records)} 条记录（重试成功）...")
                                break
                            elif retry == max_retries - 1:
                                # 最后一次重试也失败
                                logger.error(f"重试 {max_retries} 次后仍然失败: {result.get('msg')}")
                                raise Exception(f"写入记录失败（重试{max_retries}次）: {result.get('msg')}")
                        continue  # 重试成功，继续下一批
                    
                    logger.error(f"失败详情: {result}")
                    raise Exception(error_msg)
        
        logger.info(f"成功写入 {written_count} 条记录")
        return written_count
    
    def _convert_value_to_feishu_format(self, value: Any) -> Any:
        """
        将Python值转换为飞书API格式
        
        Args:
            value: Python值
            
        Returns:
            Any: 飞书API格式的值
        """
        if value is None:
            return None
        
        # 日期时间（datetime对象或date对象）
        if isinstance(value, datetime):
            # 飞书日期字段需要毫秒时间戳
            return int(value.timestamp() * 1000)
        
        # 处理date对象
        from datetime import date
        if isinstance(value, date):
            # 将date转换为datetime（当天0点），然后转换为毫秒时间戳
            dt = datetime.combine(value, datetime.min.time())
            # 飞书日期字段需要毫秒时间戳
            return int(dt.timestamp() * 1000)
        
        # 字符串
        if isinstance(value, str):
            return value
        
        # 数字
        if isinstance(value, (int, float)):
            return value
        
        # 布尔值
        if isinstance(value, bool):
            return value
        
        # 列表（多选等）
        if isinstance(value, list):
            return value
        
        # 字典（链接等）
        if isinstance(value, dict):
            return value
        
        # 其他类型转换为字符串
        return str(value)
    
    async def update_records(self, records: List[Dict[str, Any]], batch_size: int = 500) -> int:
        """
        批量更新记录到飞书多维表格
        
        Args:
            records: 记录列表，每个记录必须包含 'record_id' 字段，其他字段为要更新的字段
            batch_size: 每批更新的记录数（最大500）
            
        Returns:
            int: 成功更新的记录数量
        """
        if not self._access_token:
            await self.get_access_token()
        
        if not records:
            logger.warning("没有需要更新的记录")
            return 0
        
        # 获取字段映射
        logger.info("正在获取表格字段信息...")
        field_name_to_id = {}
        field_id_to_name = await self.get_table_fields()
        for field_id, field_name in field_id_to_name.items():
            field_name_to_id[field_name] = field_id
        
        # 转换数据格式
        logger.info("正在转换数据格式...")
        converted_records = []
        for record in records:
            if 'record_id' not in record:
                logger.warning("记录缺少 'record_id' 字段，跳过")
                continue
            
            record_id = record['record_id']
            fields = {}
            for field_name, field_value in record.items():
                if field_name == 'record_id':
                    continue
                if field_name in field_name_to_id:
                    field_id = field_name_to_id[field_name]
                    fields[field_id] = self._convert_value_to_feishu_format(field_value)
                else:
                    logger.warning(f"字段 '{field_name}' 在表格中不存在，跳过")
            
            if fields:
                converted_records.append({
                    "record_id": record_id,
                    "fields": fields
                })
        
        if not converted_records:
            logger.warning("没有有效的数据需要更新")
            return 0
        
        logger.info(f"准备更新 {len(converted_records)} 条记录")
        
        # 批量更新
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_update"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        updated_count = 0
        
        for i in range(0, len(converted_records), batch_size):
            batch = converted_records[i:i+batch_size]
            data = {"records": batch}
            
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=data)
                result = response.json()
                
                if result.get("code") == 0:
                    updated_count += len(batch)
                    logger.info(f"已更新 {updated_count}/{len(converted_records)} 条记录...")
                else:
                    error_msg = f"更新记录失败: {result.get('msg')}"
                    logger.error(error_msg)
                    logger.error(f"失败详情: {result}")
                    raise Exception(error_msg)
        
        logger.info(f"成功更新 {updated_count} 条记录")
        return updated_count
    
    async def create_table(self, name: str) -> str:
        """
        创建数据表
        
        Args:
            name: 表名
            
        Returns:
            str: 创建的表ID
        """
        if not self._access_token:
            await self.get_access_token()
        
        # 优先使用SDK方式创建表（如果SDK可用）
        if LARK_SDK_AVAILABLE:
            try:
                def create_with_sdk(table_name: str, access_token: str) -> str:
                    """使用SDK同步创建表"""
                    try:
                        client = lark.Client.builder() \
                            .enable_set_token(True) \
                            .log_level(lark.LogLevel.INFO) \
                            .build()
                        
                        # 创建表时设置默认字段为SKU、SPU和运营
                        request = CreateAppTableRequest.builder() \
                            .app_token(self.app_token) \
                            .request_body(CreateAppTableRequestBody.builder()
                                .table(ReqTable.builder()
                                    .name(table_name)
                                    .default_view_name(f"{table_name}的默认视图")
                                    .fields([
                                        AppTableCreateHeader.builder()
                                            .field_name("SKU")
                                            .type(1)  # 1=多行文本
                                            .build(),
                                        AppTableCreateHeader.builder()
                                            .field_name("SPU")
                                            .type(1)  # 1=多行文本
                                            .build(),
                                        AppTableCreateHeader.builder()
                                            .field_name("运营")
                                            .type(1)  # 1=多行文本
                                            .build()
                                    ])
                                    .build())
                                .build()) \
                            .build()
                        
                        option = lark.RequestOption.builder().tenant_access_token(access_token).build()
                        response = client.bitable.v1.app_table.create(request, option)
                        
                        if response.success():
                            # 尝试不同的响应结构访问方式
                            try:
                                if hasattr(response.data, 'table') and hasattr(response.data.table, 'table_id'):
                                    table_id = response.data.table.table_id
                                elif hasattr(response.data, 'table_id'):
                                    table_id = response.data.table_id
                                else:
                                    # 尝试从原始数据获取
                                    table_id = getattr(response.data, 'table_id', None)
                                    if not table_id:
                                        raise AttributeError("无法从响应中获取table_id")
                                logger.info(f"创建数据表成功（SDK）: {table_name} (ID: {table_id})")
                                return table_id
                            except AttributeError as e:
                                logger.warning(f"SDK响应结构异常: {e}, 响应数据: {response.data if hasattr(response, 'data') else 'N/A'}")
                                raise
                        else:
                            raise Exception(f"SDK创建表失败: code={response.code}, msg={response.msg}")
                    except Exception as e:
                        # 如果是表名重复错误，尝试获取已存在的表ID
                        error_str = str(e)
                        if "1254013" in error_str or "TableNameDuplicated" in error_str:
                            logger.warning(f"表 {table_name} 已存在，尝试获取已存在的表ID...")
                            # 在同步函数中无法直接调用异步方法，所以抛出特殊异常
                            raise Exception("TABLE_EXISTS")
                        logger.warning(f"SDK创建表失败，将尝试HTTP方式: {e}")
                        raise
                
                # 使用线程池执行同步SDK调用
                loop = asyncio.get_event_loop()
                try:
                    table_id = await loop.run_in_executor(None, create_with_sdk, name, self._access_token)
                    return table_id
                except Exception as e:
                    if "TABLE_EXISTS" in str(e):
                        # 表已存在，获取表ID
                        tables = await self.get_tables()
                        if name in tables:
                            table_id = tables[name]
                            logger.info(f"找到已存在的表 {name} (ID: {table_id})")
                            return table_id
                    raise
            except Exception as e:
                # 如果是表名重复错误，尝试获取已存在的表ID
                if "TABLE_EXISTS" in str(e):
                    tables = await self.get_tables()
                    if name in tables:
                        table_id = tables[name]
                        logger.info(f"找到已存在的表 {name} (ID: {table_id})")
                        return table_id
                logger.warning(f"SDK方式创建表失败，改用HTTP方式: {e}")
                # 继续使用HTTP方式
        
        # HTTP方式创建表
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # 飞书API创建表需要传入table对象
        # 根据官方SDK示例，需要包含name、default_view_name和fields
        data = {
            "table": {
                "name": name,
                "default_view_name": f"{name}的默认视图",  # 默认视图名称
                "fields": [
                    {
                        "field_name": "SKU",
                        "type": 1  # 1=多行文本
                    },
                    {
                        "field_name": "SPU",
                        "type": 1  # 1=多行文本
                    },
                    {
                        "field_name": "运营",
                        "type": 1  # 1=多行文本
                    }
                ]
            }
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, headers=headers, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                table_id = result.get("data", {}).get("table", {}).get("table_id", "")
                logger.info(f"创建数据表成功: {name} (ID: {table_id})")
                return table_id
            else:
                error_code = result.get("code")
                error_msg = result.get("msg", "未知错误")
                error_detail = result.get("error", {})
                
                # 如果表名重复，尝试获取已存在的表ID
                if error_code == 1254013 or "TableNameDuplicated" in error_msg:
                    logger.warning(f"表 {name} 已存在，尝试获取已存在的表ID...")
                    tables = await self.get_tables()
                    if name in tables:
                        table_id = tables[name]
                        logger.info(f"找到已存在的表 {name} (ID: {table_id})")
                        return table_id
                    else:
                        logger.error(f"表 {name} 已存在但无法获取表ID")
                
                logger.error(f"创建数据表失败 [code: {error_code}]: {error_msg}")
                logger.error(f"请求数据: {data}")
                logger.error(f"完整响应: {result}")
                raise Exception(f"创建数据表失败: {error_msg}")
    
    async def get_tables(self) -> Dict[str, str]:
        """
        获取多维表格中的所有数据表
        
        Returns:
            Dict[str, str]: 表名到表ID的映射
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            result = response.json()
            
            if result.get("code") == 0:
                tables = result.get("data", {}).get("items", [])
                table_map = {}
                for table in tables:
                    table_name = table.get("name", "")
                    table_id = table.get("table_id", "")
                    table_map[table_name] = table_id
                logger.info(f"获取到 {len(table_map)} 个数据表")
                return table_map
            else:
                error_msg = f"获取数据表列表失败: {result.get('msg')}"
                logger.error(error_msg)
                raise Exception(error_msg)
    
    async def create_field(self, field_name: str, field_type: str = "1", precision: int = 0) -> str:
        """
        创建字段
        
        Args:
            field_name: 字段名
            field_type: 字段类型，1=多行文本，2=数字，3=单选，4=多选，5=日期，6=复选框，7=人员，8=电话号码，9=链接，11=附件，13=双向关联，15=地理位置，17=公式，18=创建时间，19=最后更新时间，20=创建人，21=修改人，22=自动编号，1001=文本，1002=数字，1004=日期，1005=复选框，1006=人员，1007=附件，1008=链接，1009=公式，1010=双向关联，1011=地理位置，1012=单选，1013=多选，1014=创建时间，1015=最后更新时间，1016=创建人，1017=修改人，1018=自动编号
            precision: 数字字段的小数位数（仅对数字类型有效，默认0）
            
        Returns:
            str: 创建的字段ID
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # 根据字段类型设置字段配置
        field_config = {
            "field_name": field_name
        }
        
        # 数字类型需要特殊配置
        if field_type == "2" or field_type == "1002":  # 数字类型
            field_config["type"] = int(field_type)
            field_config["property"] = {
                "precision": precision,  # 小数位数
                "formatter": "0"  # 数字格式
            }
        else:
            field_config["type"] = int(field_type) if field_type.isdigit() else 1
        
        data = field_config
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, headers=headers, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                field_id = result.get("data", {}).get("field", {}).get("field_id", "")
                logger.info(f"创建字段成功: {field_name} (ID: {field_id})")
                return field_id
            else:
                error_msg = f"创建字段失败: {result.get('msg')}"
                logger.error(error_msg)
                # 如果字段已存在，返回错误但不抛出异常
                if "already exists" in error_msg.lower() or "已存在" in error_msg:
                    logger.warning(f"字段 {field_name} 已存在，跳过创建")
                    # 尝试获取现有字段ID
                    field_map = await self.get_table_fields()
                    for fid, fname in field_map.items():
                        if fname == field_name:
                            return fid
                raise Exception(error_msg)
    
    async def update_field(self, field_id: str, field_name: str = None, field_type: str = None, precision: int = None) -> bool:
        """
        更新字段（重命名、修改类型或精度）
        
        Args:
            field_id: 字段ID
            field_name: 新字段名（可选）
            field_type: 新字段类型（可选）
            precision: 数字字段的小数位数（可选，仅对数字类型有效）
            
        Returns:
            bool: 是否成功
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        data = {}
        if field_name:
            data["field_name"] = field_name
        if field_type:
            data["type"] = int(field_type) if field_type.isdigit() else 1
        # 如果指定了精度，且字段类型是数字类型，则更新精度
        if precision is not None:
            data["property"] = {
                "precision": precision,
                "formatter": "0"
            }
        
        if not data:
            return False
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.put(url, headers=headers, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                logger.info(f"更新字段成功 (ID: {field_id}, 新名称: {field_name})")
                return True
            else:
                error_msg = f"更新字段失败: {result.get('msg')}"
                logger.warning(error_msg)
                return False
    
    async def update_field(self, field_id: str, field_name: str = None) -> bool:
        """
        更新字段（重命名）
        
        Args:
            field_id: 字段ID
            field_name: 新字段名
            
        Returns:
            bool: 是否成功
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        data = {
            "field_name": field_name
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.put(url, headers=headers, json=data)
            result = response.json()
            
            if result.get("code") == 0:
                logger.info(f"重命名字段成功 (ID: {field_id}, 新名称: {field_name})")
                return True
            else:
                error_msg = f"重命名字段失败: {result.get('msg')}"
                logger.warning(error_msg)
                return False
    
    async def delete_field(self, field_id: str) -> bool:
        """
        删除字段
        
        Args:
            field_id: 字段ID
            
        Returns:
            bool: 是否成功
        """
        if not self._access_token:
            await self.get_access_token()
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/fields/{field_id}"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.delete(url, headers=headers)
            result = response.json()
            
            if result.get("code") == 0:
                logger.info(f"删除字段成功 (ID: {field_id})")
                return True
            else:
                error_msg = f"删除字段失败: {result.get('msg')}"
                logger.warning(error_msg)
                return False
    
    async def ensure_table_and_fields(self, table_name: str, field_names: List[Dict[str, str]], 
                                      remove_extra_fields: bool = False) -> str:
        """
        确保数据表和字段存在，如果不存在则创建
        可以删除不在列表中的字段（用于动态更新月份字段）
        
        Args:
            table_name: 表名
            field_names: 字段列表，每个字段是一个字典，包含 'name' 和 'type' 键
                        type: 'text'=文本, 'number'=数字
            remove_extra_fields: 是否删除不在field_names中的字段（默认False）
                                设置为True时，会删除不在列表中的字段（保留SKU字段）
            
        Returns:
            str: 表ID
        """
        # 获取所有表
        tables = await self.get_tables()
        
        # 检查表是否存在
        if table_name in tables:
            table_id = tables[table_name]
            logger.info(f"数据表 {table_name} 已存在 (ID: {table_id})")
        else:
            # 创建表
            table_id = await self.create_table(table_name)
        
        # 更新实例的table_id（确保后续操作使用正确的table_id）
        self.table_id = table_id
        
        # 获取现有字段
        existing_fields = await self.get_table_fields()  # {field_id: field_name}
        existing_field_names = set(existing_fields.values())
        required_field_names = {field_info.get('name', '') for field_info in field_names}
        
        # 检查是否有默认的"多行文本"字段，如果有且SKU字段不存在，则重命名为"SKU"
        default_field_names = {'多行文本', '名称'}
        sku_field_name = 'SKU'
        
        if sku_field_name not in existing_field_names:
            # 查找需要重命名的默认字段
            for field_id, field_name in existing_fields.items():
                if field_name in default_field_names:
                    logger.info(f"将默认字段 '{field_name}' 重命名为 '{sku_field_name}'")
                    try:
                        success = await self.update_field(field_id, field_name=sku_field_name)
                        if success:
                            # 更新existing_fields映射
                            existing_fields[field_id] = sku_field_name
                            existing_field_names.remove(field_name)
                            existing_field_names.add(sku_field_name)
                            break
                    except Exception as e:
                        logger.warning(f"重命名字段失败: {e}")
                        # 如果重命名失败，继续创建新字段
                    break
        
        # 如果需要删除多余字段
        if remove_extra_fields:
            # 系统字段和SKU字段不删除
            # 注意：飞书多维表默认有"多行文本"或"名称"作为主字段，不能删除
            protected_fields = {
                'SKU', 
                '名称', 
                '多行文本',  # 默认主字段
                '创建时间', 
                '更新时间', 
                '创建人', 
                '更新人',
                '最后更新时间',
                '最后更新人'
            }
            
            for field_id, field_name in existing_fields.items():
                # 如果字段不在需要的列表中，且不是受保护字段，则删除
                if field_name not in required_field_names and field_name not in protected_fields:
                    logger.info(f"删除多余字段: {field_name}")
                    try:
                        await self.delete_field(field_id)
                    except Exception as e:
                        logger.warning(f"删除字段 {field_name} 失败: {e}")
                        # 继续处理其他字段
                elif field_name in protected_fields:
                    logger.debug(f"跳过受保护字段: {field_name}")
        
        # 创建缺失的字段，或更新已存在字段的精度
        for field_info in field_names:
            field_name = field_info.get('name', '')
            field_type_str = field_info.get('type', 'text')
            
            # 获取精度参数（如果字段定义中有precision键）
            precision = field_info.get('precision', 0)
            
            if field_name in existing_field_names:
                # 字段已存在，检查是否需要更新精度
                if field_type_str == 'number' and precision is not None:
                    # 获取字段详细信息
                    existing_field_info = await self.get_field_info(field_name)
                    if existing_field_info:
                        field_id = existing_field_info.get('field_id')
                        existing_type = existing_field_info.get('type')
                        existing_property = existing_field_info.get('property', {})
                        existing_precision = existing_property.get('precision', 0)
                        
                        # 如果是数字类型字段，且精度不同，则更新
                        if existing_type in [2, 1002] and existing_precision != precision:
                            logger.info(f"字段 {field_name} 已存在，但精度不同（当前: {existing_precision}, 需要: {precision}），正在更新...")
                            try:
                                success = await self.update_field(field_id, precision=precision)
                                if success:
                                    logger.info(f"成功更新字段 {field_name} 的精度为 {precision}")
                                else:
                                    logger.warning(f"更新字段 {field_name} 的精度失败")
                            except Exception as e:
                                logger.warning(f"更新字段 {field_name} 的精度失败: {e}")
                        else:
                            logger.debug(f"字段 {field_name} 已存在，精度正确，跳过更新")
                    else:
                        logger.debug(f"字段 {field_name} 已存在，但无法获取详细信息，跳过更新")
                else:
                    logger.debug(f"字段 {field_name} 已存在，跳过创建")
                continue
            
            # 转换字段类型
            if field_type_str == 'number':
                field_type = "2"  # 数字类型
            else:
                field_type = "1"  # 多行文本类型
            
            try:
                await self.create_field(field_name, field_type, precision)
            except Exception as e:
                logger.warning(f"创建字段 {field_name} 失败: {e}")
                # 继续处理其他字段
        return table_id


# 便捷函数

async def get_feishu_client(app_token: str, table_id: str, view_id: Optional[str] = None) -> FeishuClient:
    """
    获取飞书客户端实例
    
    Args:
        app_token: 多维表格ID
        table_id: 表格ID
        view_id: 视图ID（可选）
        
    Returns:
        FeishuClient: 飞书客户端实例
    """
    return FeishuClient(app_token=app_token, table_id=table_id, view_id=view_id)

