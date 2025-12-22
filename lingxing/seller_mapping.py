#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
店铺映射模块（简化版，保持兼容性）

注意：此模块已弃用，建议使用 jobs.purchase_analysis.shop_mapping 模块
该模块提供了更完善的店铺映射功能。
"""
from typing import Dict


# 固定的店铺映射
FIXED_SHOP_MAPPINGS = {
    '110521897148377600': 'TK本土店-1店',
    '122513373670998016': 'RR-EU',
    '110521891393331200': 'TK跨境店-2店',
}


async def fetch_sid_to_name_map(op_api, access_token) -> Dict[str, str]:
    """
    获取店铺ID到店铺名称的映射（兼容旧版本）
    
    注意：此函数已弃用，仅返回固定映射以保持兼容性。
    建议使用 jobs.purchase_analysis.shop_mapping.get_shop_mapping() 获取完整映射。
    
    Args:
        op_api: OpenApiBase实例（未使用）
        access_token: 访问令牌（未使用）
        
    Returns:
        Dict[str, str]: 店铺ID到店铺名称的映射字典
    """
    return FIXED_SHOP_MAPPINGS.copy()

