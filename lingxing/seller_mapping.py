#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
店铺映射模块
提供固定的店铺ID到店铺名称的映射
"""
from typing import Dict


# 固定的店铺映射（根据实际业务配置）
FIXED_SHOP_MAPPINGS = {
    '110521897148377600': 'TK本土店-1店',
    '122513373670998016': 'RR-EU',
    '110521891393331200': 'TK跨境店-2店',
}


async def fetch_sid_to_name_map(op_api, access_token) -> Dict[str, str]:
    """
    获取店铺ID到店铺名称的映射
    
    注意：灵星API的 /erp/sc/data/seller/allMarketplace 接口
    返回的是市场/国家列表（美国、加拿大等），不包含店铺信息。
    因此这里直接返回固定的店铺映射。
    
    Args:
        op_api: OpenApiBase实例（未使用）
        access_token: 访问令牌（未使用）
        
    Returns:
        Dict[str, str]: 店铺ID到店铺名称的映射字典
    """
    return FIXED_SHOP_MAPPINGS.copy()
    
    # 以下是API调用代码（暂时禁用）
    # try:
    #     resp = await op_api.request(
    #         access_token,
    #         "/erp/sc/routing/shop/list",  # 需要确认正确的API端点
    #         "POST",
    #         req_body={}
    #     )
    #     
    #     result = resp.model_dump()
    #     
    #     if result.get('code') != 200:
    #         print(f"获取店铺列表失败: {result.get('message')}")
    #         return predefined_mappings
    #     
    #     shops = result.get('data', [])
    #     
    #     # 构建映射字典
    #     sid_to_name_map = {}
    #     for shop in shops:
    #         sid = str(shop.get('sid', ''))
    #         shop_name = shop.get('shop_name', '') or shop.get('name', '')
    #         if sid and shop_name:
    #             sid_to_name_map[sid] = shop_name
    #     
    #     # 合并预定义映射
    #     sid_to_name_map.update(predefined_mappings)
    #     return sid_to_name_map
    #     
    # except Exception as e:
    #     print(f"获取店铺映射异常: {e}")
    #     return predefined_mappings

