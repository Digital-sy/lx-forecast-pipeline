#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""获取货件单号列表"""
import asyncio
import json
from datetime import datetime, timedelta

from openapi import OpenApiBase
from feishu_bitable import write_to_feishu_bitable_by_date_range
from feishu_config import FEISHU_CONFIG


def process_box_info(box_data, shipment_id, sid, seller_name="", shipment_info=None):
    """
    处理箱子信息，按SPU+箱号聚合数据
    每个箱号只保留单箱数量最多的SPU，并添加单箱总数维度
    
    参数:
        box_data: boxInfo API返回的数据
        shipment_id: 货件单号
        sid: 店铺ID
        seller_name: 店铺名称
        shipment_info: 货件级别信息（状态、时间、地址等）
    """
    result = []
    
    if not box_data:
        return result
    
    # 提取货件级别信息
    shipment_data = {}
    if shipment_info:
        shipment_data = {
            "货件名称": shipment_info.get("shipment_name", ""),
            "货件状态": shipment_info.get("shipment_status", ""),
            "创建时间": shipment_info.get("gmt_create", ""),
            "修改时间": shipment_info.get("gmt_modified", ""),
            "工作时间": shipment_info.get("working_time", ""),
            "发货时间": shipment_info.get("shipped_time", ""),
            "接收时间": shipment_info.get("receiving_time", ""),
            "关闭时间": shipment_info.get("closed_time", ""),
            "同步时间": shipment_info.get("sync_time", ""),
            "预计送达开始": shipment_info.get("sta_delivery_start_date", ""),
            "预计送达结束": shipment_info.get("sta_delivery_end_date", ""),
            "目的仓库": shipment_info.get("destination_fulfillment_center_id", ""),
            "运输方式": shipment_info.get("shipping_mode", ""),
            "发货国家": shipment_info.get("ship_from_address", {}).get("country_code", ""),
            "发货城市": shipment_info.get("ship_from_address", {}).get("city", ""),
            "收货国家": shipment_info.get("ship_to_address", {}).get("country_code", ""),
            "收货城市": shipment_info.get("ship_to_address", {}).get("city", ""),
            "收货仓库代码": shipment_info.get("ship_to_address", {}).get("name", ""),
        }
    
    box_list = box_data.get("box_list", [])
    
    # 按「SPU + 箱号」聚合
    spu_map = {}
    # 记录每个箱号的总数量（从box_total字段获取）
    box_total_qty = {}
    
    for box_index, box in enumerate(box_list):
        # 箱子尺寸信息
        box_length = float(box.get("box_length", 0) or 0)
        box_width = float(box.get("box_width", 0) or 0)
        box_height = float(box.get("box_height", 0) or 0)
        box_weight = float(box.get("box_weight", 0) or 0)
        # 使用数组索引作为箱号，因为API的box_num字段可能重复
        box_num = box_index
        
        # 单位转换（如果需要）
        dimensions_unit = box.get("box_dimensions_unit", "in")
        weight_unit = box.get("box_weight_unit", "lb")
        
        # 转换为厘米和千克
        if dimensions_unit == "in":
            box_length_cm = round(box_length * 2.54, 2)
            box_width_cm = round(box_width * 2.54, 2)
            box_height_cm = round(box_height * 2.54, 2)
        else:
            box_length_cm = box_length
            box_width_cm = box_width
            box_height_cm = box_height
        
        if weight_unit == "lb":
            box_weight_kg = round(box_weight * 0.453592, 2)
        else:
            box_weight_kg = box_weight
        
        # 体积（m³）
        cbm = round((box_length_cm * box_width_cm * box_height_cm) / 1000000, 4)
        # 体积重（kg）
        vol_weight = round((box_length_cm * box_width_cm * box_height_cm) / 6000, 2)
        
        box_mskus = box.get("box_mskus", [])
        
        # 直接使用box中的box_id字段（新接口已提供）
        formatted_box_num = box.get('box_id', '')
        if not formatted_box_num:
            # 如果没有box_id，生成一个
            box_code = str(box_index + 1)
            formatted_box_num = f"{shipment_id}U{box_code.zfill(6)}"
        else:
            # 从box_id中提取数字部分作为box_code用于统计
            box_code = formatted_box_num.split('U')[-1].lstrip('0') or '0'
        
        # 从box_total字段获取单箱总数（新接口直接提供）
        box_total = int(box.get("box_total", 0) or 0)
        if box_code not in box_total_qty:
            box_total_qty[box_code] = box_total
        
        for item in box_mskus:
            raw_msku = item.get("msku", "")
            sku = item.get("sku", "")  # 新接口提供的sku字段
            qty = int(item.get("quantity_in_case", 0) or 0)
            if not qty:
                continue
            
            # 使用sku提取SPU（ZSY503-BE-S → ZSY503）
            spu = sku.split('-')[0] if sku else (raw_msku.split('-')[0] if raw_msku else "")
            fnsku = item.get("fulfillment_network_sku", "")
            
            # 获取品名、图片、父ASIN、title（新接口提供）
            product_name = item.get("productName", "")
            title = item.get("title", "")
            parent_asin = item.get("parentAsin", "")
            image_url = item.get("url", "")
            
            # key = SPU + 箱号（保证一个箱子内的同一 SPU 聚合）
            key = f"{spu}|{formatted_box_num}"
            
            if key not in spu_map:
                spu_map[key] = {
                    "店铺": seller_name,
                    "货件单号": shipment_id,
                    **shipment_data,  # 添加货件级别信息
                    "箱号": formatted_box_num,  # 直接使用boxId
                    "SPU": spu,
                    "品名": product_name if product_name else "",
                    "Title": title if title else "",  # 添加title字段
                    "父ASIN": parent_asin if parent_asin else "",  # 添加父ASIN字段
                    "图片链接": image_url if image_url else "",
                    "FNSKU": fnsku if fnsku else "",
                    "MSKU": raw_msku if raw_msku else "",
                    "单箱数量": 0,
                    "箱子长度cm": box_length_cm,
                    "箱子宽度cm": box_width_cm,
                    "箱子高度cm": box_height_cm,
                    "箱体积m3": cbm,
                    "箱子重量kg": box_weight_kg,
                    "箱子体积重kg": vol_weight,
                }
            else:
                # 如果当前字段为空，尝试用新的非空值填充
                if not spu_map[key]["FNSKU"] and fnsku:
                    spu_map[key]["FNSKU"] = fnsku
                if not spu_map[key]["MSKU"] and raw_msku:
                    spu_map[key]["MSKU"] = raw_msku
                if not spu_map[key]["品名"] and product_name:
                    spu_map[key]["品名"] = product_name
                if not spu_map[key]["Title"] and title:
                    spu_map[key]["Title"] = title
                if not spu_map[key]["父ASIN"] and parent_asin:
                    spu_map[key]["父ASIN"] = parent_asin
                if not spu_map[key]["图片链接"] and image_url:
                    spu_map[key]["图片链接"] = image_url
            
            # 累加 SPU 的单箱数量
            spu_map[key]["单箱数量"] += qty
    
    # 按箱号分组，每个箱号只保留单箱数量最多的SPU
    box_max_spu = {}
    for key, data in spu_map.items():
        box_code = data["箱号"]
        if box_code not in box_max_spu:
            box_max_spu[box_code] = data
        else:
            # 如果当前SPU的单箱数量更多，则替换
            if data["单箱数量"] > box_max_spu[box_code]["单箱数量"]:
                box_max_spu[box_code] = data
    
    # 添加单箱总数
    for box_code, row in box_max_spu.items():
        # 从原始箱号中提取数字部分用于查找总数
        # 格式化箱号格式: FBA19489G7MZU000001
        original_box_code = box_code.split('U')[-1].lstrip('0') or '0'
        row["单箱总数"] = box_total_qty.get(original_box_code, 0)
        
        result.append(row)
    
    return result


def convert_to_timestamp(date_str):
    """
    将日期字符串转换为Unix时间戳（毫秒）
    
    参数:
        date_str: 日期字符串，格式如 "2025-10-10 16:27" 或 "2025-10-10 16:27:30"
    
    返回:
        Unix时间戳（毫秒），如果转换失败返回None
    """
    if not date_str or date_str == "":
        return None
    
    try:
        # 尝试多种日期格式
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                # 转换为Unix时间戳（毫秒）
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue
        return None
    except Exception:
        return None


def format_for_feishu(processed_data):
    """
    将处理后的数据格式化为飞书多维表格式
    
    参数:
        processed_data: process_box_info 返回的数据列表
    
    返回:
        [{ "fields": {...} }, ...]
    """
    result = []
    
    for item in processed_data:
        # 构建飞书字段映射（基础字段）
        fields = {
            "店铺": item.get("店铺", ""),
            "货件单号": item.get("货件单号", ""),
            "货件名称": item.get("货件名称", ""),
            "货件状态": item.get("货件状态", ""),
            "目的仓库": item.get("目的仓库", ""),
            "运输方式": item.get("运输方式", ""),
            "发货国家": item.get("发货国家", ""),
            "发货城市": item.get("发货城市", ""),
            "收货国家": item.get("收货国家", ""),
            "收货城市": item.get("收货城市", ""),
            "收货仓库代码": item.get("收货仓库代码", ""),
            "箱号": item.get("箱号", ""),
            "SPU": item.get("SPU", ""),
            "品名": item.get("品名", ""),
            "Title": item.get("Title", ""),
            "父ASIN": item.get("父ASIN", ""),
            "FNSKU": item.get("FNSKU", ""),
            "MSKU": item.get("MSKU", ""),
            "单箱数量": item.get("单箱数量", 0),
            "单箱总数": item.get("单箱总数", 0),
            "箱子长度cm": item.get("箱子长度cm", 0),
            "箱子宽度cm": item.get("箱子宽度cm", 0),
            "箱子高度cm": item.get("箱子高度cm", 0),
            "箱体积m3": item.get("箱体积m3", 0),
            "箱子重量kg": item.get("箱子重量kg", 0),
            "箱子体积重kg": item.get("箱子体积重kg", 0),
        }
        
        # 处理图片链接字段（飞书超链接字段需要对象格式）
        image_url = item.get("图片链接", "")
        if image_url:
            fields["图片链接"] = {
                "text": "查看图片",
                "link": image_url
            }
        
        # 添加日期字段（只在有值时添加）
        date_fields = {
            "创建时间": item.get("创建时间", ""),
            "修改时间": item.get("修改时间", ""),
            "工作时间": item.get("工作时间", ""),
            "发货时间": item.get("发货时间", ""),
            "接收时间": item.get("接收时间", ""),
            "关闭时间": item.get("关闭时间", ""),
            "同步时间": item.get("同步时间", ""),
            "预计送达开始": item.get("预计送达开始", ""),
            "预计送达结束": item.get("预计送达结束", ""),
        }
        
        for field_name, date_str in date_fields.items():
            timestamp = convert_to_timestamp(date_str)
            if timestamp is not None:
                fields[field_name] = timestamp
        
        result.append({"fields": fields})
    
    return result


async def main():
    """主函数：获取货件单号列表"""
    
    print("🚀 开始获取货件单号列表")
    print("=" * 60)
    
    # ========== 第一步：初始化API客户端 ==========
    print("\n📥 第一步：初始化API客户端")
    
    # 配置代理URL
    proxy_url = "http://961FF0C7:619538E43E6F@27.150.170.64:16324"
    
    print(f"🔗 使用代理: {proxy_url}")
    
    # 创建OpenApiBase实例时传入代理URL
    try:
        op_api = OpenApiBase("https://openapi.lingxing.com", "ak_6KzWYKC12WvhY", "1UizOvCHkv8vOBpTr0g0IQ==", proxy_url=proxy_url)
    except Exception as e:
        print(f"❌ 创建API客户端失败: {e}")
        return
    
    # 获取访问令牌（带重试机制）
    max_token_retries = 3
    token_retry_count = 0
    
    while token_retry_count < max_token_retries:
        try:
            token_resp = await op_api.generate_access_token()
            print(f"✅ 领星API访问令牌获取成功")
            await asyncio.sleep(0.5)  # API调用后延时
            break
        except Exception as e:
            token_retry_count += 1
            print(f"❌ 获取访问令牌失败 (尝试 {token_retry_count}/{max_token_retries}): {e}")
            
            if token_retry_count >= max_token_retries:
                print(f"❌ 获取访问令牌失败，已达到最大重试次数")
                print(f"💡 建议检查:")
                print(f"  1. 网络连接是否正常")
                print(f"  2. 代理服务器是否可用")
                print(f"  3. 领星API服务是否正常")
                return
            
            # 等待后重试
            import time
            wait_time = 5 * token_retry_count
            print(f"⏳ 等待 {wait_time} 秒后重试...")
            time.sleep(wait_time)
    
    # ========== 第二步：获取所有店铺ID ==========
    print(f"\n🏪 第二步：获取所有店铺ID")
    
    sid_list = []
    
    try:
        # 调用店铺列表API
        seller_resp = await op_api.request(
            token_resp.access_token,
            "/erp/sc/data/seller/lists",
            "GET"
        )
        await asyncio.sleep(0.5)  # API调用后延时
        
        if seller_resp.code == 0 and seller_resp.data:
            # 处理不同的数据结构
            if isinstance(seller_resp.data, dict):
                # 如果data是字典，尝试获取'data'字段
                if 'data' in seller_resp.data:
                    sellers = seller_resp.data['data']
                else:
                    # 如果没有'data'字段，可能整个dict就是店铺列表的容器
                    sellers = [seller_resp.data]
            elif isinstance(seller_resp.data, list):
                sellers = seller_resp.data
            else:
                sellers = []
            
            # 提取所有店铺的sid
            for seller in sellers:
                if isinstance(seller, dict):
                    if 'sid' in seller:
                        sid_list.append(seller['sid'])
                    else:
                        print(f"⚠️  店铺对象缺少sid字段，可用字段: {list(seller.keys())[:5]}")
            
            print(f"✅ 成功获取 {len(sid_list)} 个店铺ID")
            await asyncio.sleep(0.5)  # API调用后延时
    except Exception as e:
        print(f"❌ 获取店铺列表异常: {e}")
        print(f"⚠️  使用默认店铺ID列表")
        # 使用默认列表
        sid_list = [
            11545, 11546, 11548, 11549, 11563, 11562, 11547, 11550, 11561, 
            11551, 11552, 11553, 11554, 11555, 13247, 11544, 
            110521897148377600, 122513373670998016, 110521891393331200
        ]
    
    # ========== 第三步：设置查询参数 ==========
    print(f"\n📋 第三步：设置查询参数")
    
    # 设置日期范围为前3天
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    # 格式化日期为 Y-m-d 格式
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"📅 查询时间范围: {start_date_str} 到 {end_date_str}")
    print(f"🏪 将查询 {len(sid_list)} 个店铺")
    
    # ========== 第四步：获取货件单号列表 ==========
    print(f"\n📦 第四步：获取货件单号列表")
    
    all_shipments = []
    store_stats = {}  # 记录每个店铺的统计信息
    
    for sid_index, sid in enumerate(sid_list, 1):
        print(f"\n正在查询店铺 {sid_index}/{len(sid_list)}: SID={sid}")
        store_count = 0  # 当前店铺的记录数
        
        # 分页获取数据
        offset = 0
        length = 1000  # 每页1000条
        page_num = 1
        
        while True:
            print(f"  正在获取第 {page_num} 页...", end="")
            
            # 添加网络请求重试机制
            api_retry_count = 0
            max_api_retries = 3
            resp = None
            
            while api_retry_count < max_api_retries:
                try:
                    resp = await op_api.request(
                        token_resp.access_token, 
                        "/erp/sc/data/fba_report/shipmentList", 
                        "POST",
                        req_body={
                            "start_date": start_date_str,
                            "end_date": end_date_str,
                            "offset": offset,
                            "length": length,
                            "sid": sid
                        }
                    )
                    await asyncio.sleep(0.5)  # API调用后延时
                    break  # 成功则跳出重试循环
                    
                except Exception as e:
                    api_retry_count += 1
                    print(f"\n    ❌ 网络请求失败 (尝试 {api_retry_count}/{max_api_retries}): {e}")
                    
                    if api_retry_count >= max_api_retries:
                        print(f"    ❌ 网络请求失败，已达到最大重试次数")
                        break
                    
                    # 等待后重试
                    import time
                    wait_time = 5 * api_retry_count
                    print(f"    ⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
            
            if resp is None:
                print(f"  ❌ 无法获取API响应，跳过该店铺")
                break
            
            # 领星API成功响应码是0
            if resp.code == 0:
                if resp.data and isinstance(resp.data, dict):
                    current_shipments = resp.data.get('list', [])
                    current_count = len(current_shipments)
                    print(f" ✅ 获取到 {current_count} 条记录")
                    
                    # 保存数据
                    all_shipments.extend(current_shipments)
                    store_count += current_count
                    
                    # 如果当前页数据少于页面大小，说明已经是最后一页
                    if current_count < length:
                        break
                    
                    # 准备下一页
                    offset += length
                    page_num += 1
                    
                    # 添加请求间隔，避免过于频繁的请求
                    import time
                    time.sleep(0.5)
                else:
                    print(f" ⚠️ 没有数据")
                    break
            else:
                print(f"  ❌ API请求失败: {resp.error_details}")
                break
        
        # 保存该店铺的统计信息
        store_stats[sid] = store_count
        print(f"  📊 店铺 {sid} 共获取 {store_count} 条记录")
    
    # ========== 第五步：保存结果 ==========
    print(f"\n💾 第五步：保存结果")
    print(f"✅ 共获取到 {len(all_shipments)} 条货件单号记录")
    
    # 检查重复数据
    shipment_ids = []
    duplicate_count = 0
    for item in all_shipments:
        if 'shipment_id' in item:
            sid = item['shipment_id']
            if sid in shipment_ids:
                duplicate_count += 1
                print(f"⚠️  发现重复货件单号: {sid}")
            else:
                shipment_ids.append(sid)
    
    if duplicate_count > 0:
        print(f"⚠️  共发现 {duplicate_count} 条重复记录")
        print(f"📊 去重后唯一记录数: {len(shipment_ids)}")
    
    # 保存到JSON文件
    output_file = 'shipment_list.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_shipments, f, ensure_ascii=False, indent=2)
    print(f"💾 数据已保存到: {output_file}")
    
    # 统计信息
    if all_shipments:
        print(f"\n📊 数据统计:")
        print(f"  总记录数: {len(all_shipments)}")
        print(f"  唯一货件单号数: {len(set(shipment_ids))}")
        
        # 显示各店铺统计
        print(f"\n📊 各店铺数据统计:")
        total_from_stores = 0
        for sid, count in store_stats.items():
            if count > 0:
                print(f"  店铺 {sid}: {count} 条")
                total_from_stores += count
        print(f"  ───────────────")
        print(f"  各店铺合计: {total_from_stores} 条")
        
        # 显示前10条记录示例
        print(f"\n📋 前10条记录示例:")
        for i, item in enumerate(all_shipments[:10], 1):
            shipment_id = item.get('shipment_id', 'N/A')
            shipment_name = item.get('shipment_name', 'N/A')
            print(f"  {i}. 货件单号: {shipment_id}, 货件名称: {shipment_name}")
    
    # ========== 第六步：获取所有货件的箱子信息 ==========
    if all_shipments:
        print(f"\n📦 第六步：获取所有货件的箱子信息并写入飞书")
        
        # 存储所有处理后的箱子数据
        all_processed_data = []
        success_count = 0
        fail_count = 0
        skip_no_sta_id = 0  # 跳过：无STA货件ID
        skip_no_tracking = 0  # 跳过：无tracking_number_list
        skip_no_box_data = 0  # 跳过：无箱子数据
        
        print(f"\n📦 开始处理 {len(all_shipments)} 个货件...")
        
        for idx, shipment_item in enumerate(all_shipments, 1):
            shipment_id = shipment_item.get('shipment_id')
            sid = shipment_item.get('sid')
            seller_full = shipment_item.get('seller', '')
            seller_name = seller_full.split(' ')[0] if seller_full else ''
            
            if not shipment_id or not sid:
                continue
            
            print(f"\n处理货件 {idx}/{len(all_shipments)}: {shipment_id} (店铺: {seller_name})")
            
            try:
                # 获取STA相关参数
                inbound_plan_id = shipment_item.get('sta_inbound_plan_id', '')
                sta_shipment_id = shipment_item.get('sta_shipment_id', '')
                
                # 如果没有sta_shipment_id，跳过
                if not sta_shipment_id:
                    print(f"  ⏭️  跳过：无STA货件ID（sta_shipment_id为空）")
                    skip_no_sta_id += 1
                    continue
                
                # 添加重试机制，特别是针对频率限制错误、source错误和token过期
                max_retries = 5  # 增加重试次数到5次
                retry_count = 0
                box_resp = None
                rate_limit_error = False
                source_error = False
                token_expired = False
                
                # 定义可能的source值列表（按优先级排序）
                possible_sources = [
                    shipment_item.get('source'),  # 优先使用货件数据中的source
                    "OPENAPI",
                    "API",
                    "SYSTEM",
                    "OPEN_API"
                ]
                # 过滤掉None值，获取所有可能的source值
                valid_sources = [s for s in possible_sources if s]
                if not valid_sources:
                    valid_sources = ["OPENAPI"]  # 默认值
                
                source_index = 0  # 当前使用的source索引
                source_value = valid_sources[source_index]
                
                while retry_count < max_retries:
                    try:
                        req_body_data = {
                            "shipmentIdList": [sta_shipment_id],  # 使用数组格式
                            "sid": sid,
                            "inboundPlanId": inbound_plan_id,
                            "source": source_value  # 数据来源标识，必需参数
                        }
                        
                        # 调试：打印请求参数（仅在第一次重试时）
                        if retry_count == 0:
                            print(f"  🔍 请求参数: source={source_value}, sta_shipment_id={sta_shipment_id[:20]}..., sid={sid}")
                        
                        box_resp = await op_api.request(
                            token_resp.access_token,
                            "/amzStaServer/openapi/inbound-shipment/listShipmentBoxes",
                            "POST",
                            req_body=req_body_data
                        )
                        
                        # 检查是否是"Source must not be null"错误
                        if box_resp.code == -1 and box_resp.message and "Source must not be null" in box_resp.message:
                            source_error = True
                            source_index += 1
                            if source_index < len(valid_sources):
                                # 尝试下一个source值
                                source_value = valid_sources[source_index]
                                print(f"  🔄 Source错误，尝试新的source值: {source_value}")
                                retry_count += 1
                                await asyncio.sleep(2)  # 短暂等待后重试
                                continue
                            else:
                                # 所有source值都尝试过了
                                print(f"  ⚠️  已尝试所有source值，仍然失败")
                                break
                        
                        # 检查是否是token过期错误（错误码 2001003）
                        if box_resp.code == 2001003 or (box_resp.message and ("access token" in box_resp.message.lower() and ("missing" in box_resp.message.lower() or "expire" in box_resp.message.lower()))):
                            token_expired = True
                            try:
                                # 尝试刷新token（使用外层作用域的token_resp）
                                print(f"  🔄 Token已过期，正在刷新...")
                                if hasattr(token_resp, 'refresh_token') and token_resp.refresh_token:
                                    token_resp = await op_api.refresh_token(token_resp.refresh_token)
                                    print(f"  ✅ Token刷新成功")
                                else:
                                    # 如果没有refresh_token，重新获取
                                    print(f"  🔄 无refresh_token，重新获取Token...")
                                    token_resp = await op_api.generate_access_token()
                                    print(f"  ✅ Token重新获取成功")
                                await asyncio.sleep(1)  # 刷新后短暂等待
                                # 使用新token重试（不增加retry_count，因为这是token问题，不是请求问题）
                                continue
                            except Exception as refresh_error:
                                print(f"  ⚠️  Token刷新失败: {refresh_error}")
                                # 如果刷新失败，尝试重新获取token
                                try:
                                    print(f"  🔄 尝试重新获取Token...")
                                    token_resp = await op_api.generate_access_token()
                                    print(f"  ✅ Token重新获取成功")
                                    await asyncio.sleep(1)
                                    continue
                                except Exception as gen_error:
                                    print(f"  ❌ Token重新获取失败: {gen_error}")
                                    break
                        
                        # 检查是否是频率限制错误（错误码 3001008）
                        if box_resp.code == 3001008 or (box_resp.message and "too frequently" in box_resp.message.lower()):
                            rate_limit_error = True
                            retry_count += 1
                            if retry_count < max_retries:
                                # 频率限制错误，等待更长时间（递增等待时间）
                                wait_time = 10 * retry_count  # 10秒、20秒、30秒、40秒、50秒
                                print(f"  ⏳ 频率限制，等待 {wait_time} 秒后重试 ({retry_count}/{max_retries})...")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                # 达到最大重试次数，跳出循环
                                break
                        
                        # 如果成功或不是需要重试的错误，跳出重试循环
                        if box_resp.code == 0:
                            break
                        
                        # 其他错误，跳出重试循环
                        break
                        
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 5 * retry_count  # 5秒、10秒、15秒、20秒、25秒
                            print(f"  ⏳ 请求异常，等待 {wait_time} 秒后重试 ({retry_count}/{max_retries}): {e}")
                            await asyncio.sleep(wait_time)
                        else:
                            raise
                
                # 请求后延迟（避免请求过快）
                await asyncio.sleep(2.0)  # 增加到2秒延迟
                
                if box_resp and box_resp.code == 0:
                    # 新接口的数据结构：shipmentList -> shipmentPackingList
                    shipment_list = box_resp.data.get('shipmentList', [])
                    if not shipment_list:
                        print(f"  ⚠️  无货件数据")
                        continue
                    
                    # 获取第一个货件的箱子列表
                    packing_list = shipment_list[0].get('shipmentPackingList', [])
                    box_count = len(packing_list)
                    
                    # 转换为旧格式的box_list结构，以便复用process_box_info函数
                    box_data = {
                        'box_type': 'MULTIPLE' if box_count > 1 else 'SINGLE',
                        'box_list': []
                    }
                    
                    for packing in packing_list:
                        box_item = {
                            'box_length': packing.get('length', 0),
                            'box_width': packing.get('width', 0),
                            'box_height': packing.get('height', 0),
                            'box_weight': packing.get('weight', 0),
                            'box_dimensions_unit': packing.get('lengthUnit', 'IN'),
                            'box_weight_unit': packing.get('weightUnit', 'LB'),
                            'box_num': packing.get('localBoxId', '1'),
                            'box_id': packing.get('boxId', ''),  # 新接口直接提供boxId
                            'box_total': packing.get('total', 0),  # 箱子总数量
                            'box_mskus': []
                        }
                        
                        # 转换产品列表
                        for product in packing.get('productList', []):
                            box_item['box_mskus'].append({
                                'fulfillment_network_sku': product.get('fnsku', ''),
                                'msku': product.get('msku', ''),
                                'sku': product.get('sku', ''),  # 添加sku字段
                                'productName': product.get('productName', ''),  # 添加品名
                                'title': product.get('title', ''),  # 添加标题
                                'parentAsin': product.get('parentAsin', ''),  # 添加父ASIN
                                'url': product.get('url', ''),  # 添加图片链接
                                'quantity_in_case': product.get('quantityInBox', 0)
                            })
                        
                        box_data['box_list'].append(box_item)
                    
                    # 处理箱子信息（使用转换后的数据）
                    processed_data = process_box_info(
                        box_data,
                        shipment_id,
                        sid,
                        seller_name,
                        shipment_item
                    )
                    
                    if processed_data:
                        all_processed_data.extend(processed_data)
                        print(f"  ✅ 原始 {box_count} 个箱子 → 处理后 {len(processed_data)} 条记录")
                        success_count += 1
                    else:
                        # 检查是否因为没有tracking_number_list而跳过
                        tracking_list = shipment_item.get('tracking_number_list', [])
                        if not tracking_list:
                            print(f"  ⏭️  跳过：无系统箱号（tracking_number_list为空）")
                            skip_no_tracking += 1
                        else:
                            print(f"  ⚠️  无箱子数据")
                            skip_no_box_data += 1
                elif box_resp is None:
                    print(f"  ❌ 获取失败: 请求失败，无法获取响应")
                    fail_count += 1
                else:
                    # 收集详细的错误信息
                    error_info = []
                    error_info.append(f"错误码: {box_resp.code}")
                    if box_resp.message:
                        error_info.append(f"错误信息: {box_resp.message}")
                    if box_resp.error_details:
                        error_info.append(f"错误详情: {box_resp.error_details}")
                    if box_resp.data:
                        error_info.append(f"响应数据: {box_resp.data}")
                    if box_resp.request_id:
                        error_info.append(f"请求ID: {box_resp.request_id}")
                    
                    # 显示请求参数（用于调试）
                    error_info.append(f"请求参数: sta_shipment_id={sta_shipment_id}, sid={sid}, inbound_plan_id={inbound_plan_id}, source={source_value}")
                    
                    # 如果是频率限制错误，添加提示
                    if rate_limit_error:
                        error_info.append("💡 提示: 已达到最大重试次数，建议稍后手动重试该货件")
                    
                    # 如果是source错误，添加提示
                    if source_error:
                        error_info.append(f"💡 提示: 已尝试所有source值 ({', '.join(valid_sources)})，均失败")
                    
                    # 如果是token过期错误，添加提示
                    if token_expired:
                        error_info.append("💡 提示: Token已过期，已尝试刷新但可能刷新失败，建议检查网络连接或稍后重试")
                    
                    error_msg = " | ".join(error_info) if error_info else "未知错误"
                    print(f"  ❌ 获取失败: {error_msg}")
                    fail_count += 1
                    
            except Exception as e:
                print(f"  ❌ 异常: {e}")
                fail_count += 1
                # 异常后也添加延迟
                await asyncio.sleep(2.0)
        
        total_skipped = skip_no_sta_id + skip_no_tracking + skip_no_box_data
        
        print(f"\n📊 处理完成:")
        print(f"  ✅ 成功处理: {success_count} 个货件")
        print(f"  ⏭️  跳过总计: {total_skipped} 个货件")
        print(f"     - 无STA货件ID: {skip_no_sta_id} 个")
        print(f"     - 无系统箱号: {skip_no_tracking} 个")
        print(f"     - 无箱子数据: {skip_no_box_data} 个")
        print(f"  ❌ 失败: {fail_count} 个货件")
        print(f"  📦 总箱子记录: {len(all_processed_data)} 条")
        
        # ========== 第七步：写入飞书多维表 ==========
        if all_processed_data:
            print(f"\n📝 第七步：写入飞书多维表")
            
            # 格式化数据
            feishu_data = format_for_feishu(all_processed_data)
            
            print(f"📋 准备写入 {len(feishu_data)} 条记录到飞书...")
            
            # 计算日期范围的毫秒时间戳用于删除（只删除查询日期范围内的旧数据）
            start_timestamp = int(start_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            end_timestamp = int(end_date.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
            
            print(f"🗑️  将删除日期范围内的旧数据: {start_date_str} 到 {end_date_str}")
            
            try:
                result = write_to_feishu_bitable_by_date_range(
                    processed_data=feishu_data,
                    app_id=FEISHU_CONFIG["app_id"],
                    app_secret=FEISHU_CONFIG["app_secret"],
                    app_token=FEISHU_CONFIG["app_token"],
                    table_id="tbl4iOz64WPm6Qhh",
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp
                )
                
                if result:
                    print(f"✅ 数据已成功写入飞书多维表")
                    print(f"📊 写入统计: {len(feishu_data)} 条记录")
                else:
                    print(f"❌ 写入飞书失败")
                    
            except Exception as e:
                print(f"❌ 写入飞书异常: {e}")
        else:
            print(f"\n⚠️  没有数据需要写入飞书")
    
    # ========== 完成 ==========
    print(f"\n" + "=" * 60)
    print(f"🎉 任务完成！")
    
    return all_shipments


if __name__ == '__main__':
    # 使用Python 3.13推荐的异步运行方式
    result = asyncio.run(main())
