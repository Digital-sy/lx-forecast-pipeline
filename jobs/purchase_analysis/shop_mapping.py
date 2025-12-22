#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
店铺映射管理模块
负责获取和管理店铺ID到店铺名称的映射关系
"""
from typing import Dict
from common import get_logger
from lingxing import OpenApiBase

logger = get_logger('purchase_analysis.shop_mapping')


# 固定的店铺映射（优先级最高）
FIXED_SHOP_MAPPINGS = {
    '110521897148377600': 'TK本土店-1店',
    '122513373670998016': 'RR-EU',
    '110521891393331200': 'TK跨境店-2店',
}


async def fetch_marketplace_list(op_api: OpenApiBase, token: str) -> list:
    """
    从灵星API获取亚马逊市场列表（仅供参考）
    
    注意：此API返回的是市场/国家列表（美国、加拿大等），不是店铺列表！
    
    Args:
        op_api: OpenAPI客户端实例
        token: 访问令牌
        
    Returns:
        list: 市场列表
    """
    try:
        logger.info("正在获取亚马逊市场列表...")
        
        resp = await op_api.request(
            token,
            "/erp/sc/data/seller/allMarketplace",
            "GET",
            req_body={}
        )
        
        result = resp.model_dump()
        
        if result.get('code') != 0:
            logger.warning(f"获取市场列表失败: {result.get('message')}")
            return []
        
        marketplaces = result.get('data', [])
        logger.info(f"获取到 {len(marketplaces)} 个亚马逊市场")
        
        # 输出市场信息（调试用）
        for mp in marketplaces[:5]:  # 只显示前5个
            logger.debug(f"  市场: {mp.get('country')} ({mp.get('code')}) - {mp.get('region')}")
        
        return marketplaces
        
    except Exception as e:
        logger.warning(f"获取市场列表异常: {e}")
        return []


async def fetch_shops_from_api(op_api: OpenApiBase, token: str) -> Dict[str, str]:
    """
    从灵星API获取已授权的店铺列表
    
    使用接口：/erp/sc/data/seller/lists
    获取企业已授权到领星ERP的全部亚马逊店铺信息
    
    Args:
        op_api: OpenAPI客户端实例
        token: 访问令牌
        
    Returns:
        Dict[str, str]: 店铺ID(sid)到店铺名称(name)的映射字典
    """
    try:
        logger.info("正在从灵星API获取已授权店铺列表...")
        
        # 调用店铺列表API
        resp = await op_api.request(
            token,
            "/erp/sc/data/seller/lists",
            "GET",
            req_body={}
        )
        
        result = resp.model_dump()
        
        # 检查返回码
        if result.get('code') != 0:
            logger.warning(f"获取店铺列表失败: {result.get('message')}")
            return {}
        
        shops = result.get('data', [])
        
        if not shops:
            logger.info("API返回的店铺列表为空")
            return {}
        
        logger.info(f"✓ 从API获取到 {len(shops)} 个已授权店铺")
        
        # 构建映射字典
        shop_mapping = {}
        for shop in shops:
            sid = str(shop.get('sid', ''))
            shop_name = shop.get('name', '')
            region = shop.get('region', '')
            country = shop.get('country', '')
            status = shop.get('status', 0)
            
            if not sid or not shop_name:
                continue
            
            # 跳过状态异常的店铺（可选）
            if status not in [1]:  # 1=正常
                logger.debug(f"  跳过异常状态店铺: {shop_name} (sid={sid}, status={status})")
                continue
            
            # 处理RR开头的店铺，统一改为RR-EU
            if shop_name.startswith('RR-') or shop_name.startswith('RR_'):
                normalized_name = 'RR-EU'
                logger.debug(f"  店铺名称规范化: {shop_name} → {normalized_name}")
                shop_name = normalized_name
            
            shop_mapping[sid] = shop_name
            logger.debug(f"  店铺映射: {sid} → {shop_name} ({region}/{country})")
        
        logger.info(f"✓ 成功构建 {len(shop_mapping)} 个店铺映射")
        return shop_mapping
        
    except Exception as e:
        logger.error(f"从API获取店铺列表异常: {e}", exc_info=True)
        return {}


def normalize_shop_id(shop_id_or_name: str, shop_mapping: Dict[str, str]) -> str:
    """
    规范化店铺ID或名称
    
    规则：
    1. 如果在映射表中找到，使用映射的名称
    2. 如果是RR开头的，统一转换为RR-EU
    3. 其他情况保持原样
    
    Args:
        shop_id_or_name: 店铺ID或店铺名称
        shop_mapping: 店铺映射字典
        
    Returns:
        str: 规范化后的店铺名称
    """
    if not shop_id_or_name:
        return ''
    
    shop_str = str(shop_id_or_name).strip()
    
    # 1. 先查映射表
    if shop_str in shop_mapping:
        return shop_mapping[shop_str]
    
    # 2. 处理RR开头的店铺（包括RR-和RR_）
    if shop_str.startswith('RR-') or shop_str.startswith('RR_'):
        return 'RR-EU'
    
    # 3. 其他情况保持原样
    return shop_str


async def get_shop_mapping() -> Dict[str, str]:
    """
    获取完整的店铺映射（固定映射 + API获取）
    
    优先级：
    1. 固定映射（FIXED_SHOP_MAPPINGS）- 最高优先级
    2. API获取的映射
    
    Returns:
        Dict[str, str]: 完整的店铺ID到店铺名称的映射字典
    """
    logger.info("开始获取店铺映射...")
    
    # 1. 从固定映射开始
    all_mappings = FIXED_SHOP_MAPPINGS.copy()
    logger.info(f"✓ 加载固定映射: {len(all_mappings)} 个店铺")
    
    # 2. 尝试从API获取更多店铺
    try:
        from common import settings
        
        op_api = OpenApiBase(
            host=settings.LINGXING_HOST,
            app_id=settings.LINGXING_APP_ID,
            app_secret=settings.LINGXING_APP_SECRET,
            proxy_url=settings.LINGXING_PROXY_URL
        )
        
        # 获取访问令牌
        token_dto = await op_api.generate_access_token()
        token = token_dto.access_token
        
        api_mappings = await fetch_shops_from_api(op_api, token)
        
        if api_mappings:
            # 合并映射（固定映射优先，不会被覆盖）
            new_count = 0
            for sid, name in api_mappings.items():
                if sid not in all_mappings:
                    all_mappings[sid] = name
                    new_count += 1
            
            if new_count > 0:
                logger.info(f"✓ 从API新增: {new_count} 个店铺映射")
        else:
            logger.info("⚠ API未返回店铺数据，仅使用固定映射")
            
    except Exception as e:
        logger.warning(f"⚠ 获取API店铺映射失败: {e}，仅使用固定映射")
    
    # 3. 输出映射详情
    logger.info(f"✓ 店铺映射加载完成，共 {len(all_mappings)} 个店铺:")
    for sid, name in sorted(all_mappings.items(), key=lambda x: x[1]):
        logger.info(f"   {sid} → {name}")
    
    return all_mappings


# 简化的同步版本（如果不需要异步）
def get_fixed_shop_mapping() -> Dict[str, str]:
    """
    获取固定的店铺映射（不调用API）
    
    Returns:
        Dict[str, str]: 固定的店铺ID到店铺名称的映射字典
    """
    logger.info(f"使用固定店铺映射: {len(FIXED_SHOP_MAPPINGS)} 个店铺")
    return FIXED_SHOP_MAPPINGS.copy()


if __name__ == "__main__":
    # 测试代码
    import asyncio
    
    async def test():
        print("="*60)
        print("店铺映射模块测试")
        print("="*60)
        
        # 获取店铺映射
        mapping = await get_shop_mapping()
        print(f"\n✅ 获取到 {len(mapping)} 个店铺映射:")
        for sid, name in sorted(mapping.items(), key=lambda x: x[1]):
            print(f"  {sid} → {name}")
        
        # 测试规范化
        print("\n✅ 测试店铺ID规范化:")
        test_cases = [
            ('110521897148377600', '固定映射的店铺ID'),
            ('122513373670998016', '固定映射的RR-EU'),
            ('RR-US', 'RR-开头的店铺'),
            ('RR_UK', 'RR_开头的店铺'),
            ('JQ-US', '其他普通店铺'),
            ('999999', '未知的店铺ID'),
        ]
        
        for test_id, description in test_cases:
            normalized = normalize_shop_id(test_id, mapping)
            print(f"  {test_id:<25} → {normalized:<15} ({description})")
        
        print("\n"+"="*60)
        print("测试完成")
        print("="*60)
    
    asyncio.run(test())

