#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
飞书API封装模块
提供飞书多维表格的读写操作
"""
import httpx
from typing import Optional, List, Dict, Any
from datetime import datetime

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
    
    async def get_access_token(self) -> str:
        """获取飞书访问令牌"""
        url = f"{self.api_base}/auth/v3/tenant_access_token/internal"
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
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
                    field_map[field_id] = field_name
                logger.info(f"获取到 {len(field_map)} 个字段")
                return field_map
            else:
                error_msg = f"获取表格字段失败: {result.get('msg')}"
                logger.error(error_msg)
                raise Exception(error_msg)
    
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
        timeout = httpx.Timeout(60.0, connect=10.0)
        
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
                    data = result.get("data", {})
                    records = data.get("items", [])
                    
                    for record in records:
                        record_id = record.get("record_id")
                        if record_id:
                            all_record_ids.append(record_id)
                    
                    has_more = data.get("has_more", False)
                    page_token = data.get("page_token")
                    
                    if not has_more or not page_token:
                        break
                else:
                    error_msg = f"获取记录ID失败: {result.get('msg')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        
        if not all_record_ids:
            logger.info("没有需要删除的记录")
            return 0
        
        logger.info(f"找到 {len(all_record_ids)} 条记录需要删除")
        
        # 批量删除记录（每次最多500条）
        delete_url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_delete"
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        batch_size = 500
        deleted_count = 0
        
        for i in range(0, len(all_record_ids), batch_size):
            batch_ids = all_record_ids[i:i+batch_size]
            data = {"records": batch_ids}
            
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(delete_url, headers=headers, json=data)
                result = response.json()
                
                if result.get("code") == 0:
                    deleted_count += len(batch_ids)
                    logger.debug(f"已删除 {deleted_count} 条记录...")
                else:
                    error_msg = f"删除记录失败: {result.get('msg')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
        
        logger.info(f"成功清空 {deleted_count} 条记录")
        return deleted_count


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

