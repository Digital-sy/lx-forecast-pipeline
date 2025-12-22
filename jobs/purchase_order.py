#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购单数据采集任务
从领星API获取采购单数据并存入数据库
"""
import asyncio
from datetime import datetime
from typing import List, Dict, Any

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase, fetch_sid_to_name_map
from utils import normalize_shop_name

# 获取日志记录器
logger = get_logger('purchase_order')


async def fetch_all_purchase_orders(op_api: OpenApiBase, token: str, 
                                    start_date_str: str, end_date_str: str) -> List[Dict[str, Any]]:
    """
    获取所有采购单数据（分页处理）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        start_date_str: 开始日期
        end_date_str: 结束日期
        
    Returns:
        List[Dict[str, Any]]: 采购单列表
    """
    all_orders = []
    offset = 0
    length = 500  # 每页最多500条
    
    while True:
        req_body = {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "search_field_time": "create_time",
            "offset": offset,
            "length": length
        }
        
        try:
            resp = await op_api.request(
                token, 
                "/erp/sc/routing/data/local_inventory/purchaseOrderList", 
                "POST", 
                req_body=req_body
            )
            result = resp.model_dump()
            orders = result.get('data', [])
            
            if not orders:
                break
                
            all_orders.extend(orders)
            logger.info(f"已获取 {len(all_orders)} 条采购单...")
            
            # 如果返回的数据少于length，说明已经是最后一页
            if len(orders) < length:
                break
                
            offset += length
            await asyncio.sleep(settings.COLLECTION_DELAY_SECONDS)
            
        except Exception as e:
            logger.error(f"获取采购单数据失败（offset={offset}）: {e}")
            break
    
    return all_orders


def convert_to_sku_dimension(orders: List[Dict[str, Any]], 
                            sid_to_name_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    将采购单数据转换为以SKU为维度的数据
    
    Args:
        orders: 采购单列表
        sid_to_name_map: 店铺ID到店铺名称的映射
        
    Returns:
        List[Dict[str, Any]]: SKU维度数据列表
    """
    sku_data_list = []
    
    for order in orders:
        # 采购单主表信息
        order_sn = order.get('order_sn', '')
        create_time = order.get('create_time', '')
        status = order.get('status_text', '')
        status_shipped = order.get('status_shipped_text', '')
        warehouse_name = order.get('ware_house_name', '')
        supplier_name = order.get('supplier_name', '')
        
        # 遍历商品明细
        item_list = order.get('item_list', [])
        for item in item_list:
            # 获取店铺名称
            sid = item.get('sid')
            shop_name = ''
            if sid:
                sid_str = str(sid)
                shop_name = sid_to_name_map.get(sid_str, sid_str)
                # 规范化店铺名称
                shop_name = normalize_shop_name(shop_name)
            
            # 构建SKU维度数据
            sku_record = {
                '订单号': order_sn,
                'SKU': item.get('sku', ''),
                'FNSKU': item.get('fnsku', ''),
                '实际数量': item.get('quantity_real', 0),
                '店铺名': shop_name,
                '仓库': warehouse_name,
                '供应商': supplier_name,
                '创建时间': create_time,
                '状态': status,
                '到货状态': status_shipped,
                '产品名称': item.get('product_name', ''),
                'MSKU': ','.join(item.get('msku', [])),
            }
            
            sku_data_list.append(sku_record)
    
    return sku_data_list


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any]) -> None:
    """
    创建或重建数据表
    
    Args:
        table_name: 表名
        sample_row: 样本数据行
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
                logger.warning(f"表 {table_name} 结构不符，正在重建...")
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # 创建表
        fields = []
        for k, v in sample_row.items():
            if isinstance(v, int):
                fields.append(f"`{k}` INT")
            elif isinstance(v, float):
                fields.append(f"`{k}` DOUBLE")
            else:
                fields.append(f"`{k}` VARCHAR(500)")
        
        fields_sql = ", ".join(fields)
        sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql})"
        cursor.execute(sql)
        logger.info(f"表 {table_name} 创建成功")


def delete_by_date_range(table_name: str, start_date_str: str, end_date_str: str) -> int:
    """
    删除指定日期范围内的数据
    
    Args:
        table_name: 表名
        start_date_str: 开始日期
        end_date_str: 结束日期
        
    Returns:
        int: 删除的记录数
    """
    with db_cursor(dictionary=False) as cursor:
        sql = f"DELETE FROM `{table_name}` WHERE DATE(`创建时间`) >= %s AND DATE(`创建时间`) <= %s"
        cursor.execute(sql, (start_date_str, end_date_str))
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
    logger.info("采购单全量更新（从5月1日开始）")
    logger.info("注意：全量更新将处理大量数据，可能需要较长时间，请耐心等待...")
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
    
    # 设置日期范围（从当前年份的5月1日到现在）
    end_date = datetime.now()
    start_date = datetime(end_date.year, 5, 1)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    logger.info(f"查询日期范围：{start_date_str} 至 {end_date_str}（全量更新，从5月开始）")
    
    # 获取店铺映射
    logger.info("正在加载店铺映射...")
    try:
        sid_to_name_map = await fetch_sid_to_name_map(op_api, token_resp.access_token)
        logger.info(f"已加载 {len(sid_to_name_map)} 个店铺映射")
    except Exception as e:
        logger.warning(f"获取店铺映射失败: {e}，使用预定义映射")
        sid_to_name_map = {
            '110521897148377600': 'TK本土店-1店',
            '122513373670998016': 'RR-EU-EU',
            '110521891393331200': 'TK跨境店-2店'
        }
    
    # 获取采购单数据
    logger.info("正在获取采购单数据...")
    orders = await fetch_all_purchase_orders(op_api, token_resp.access_token, start_date_str, end_date_str)
    logger.info(f"共获取 {len(orders)} 条采购单")
    
    if not orders:
        logger.warning("没有数据需要保存")
        return
    
    # 转换为SKU维度数据
    logger.info("正在转换为SKU维度数据...")
    sku_data_list = convert_to_sku_dimension(orders, sid_to_name_map)
    logger.info(f"共生成 {len(sku_data_list)} 条SKU维度数据")
    
    if not sku_data_list:
        logger.warning("没有SKU数据需要保存")
        return
    
    # 处理数据库
    table_name = '采购单'
    logger.info(f"正在处理数据库表 {table_name}...")
    
    try:
        # 创建或检查表结构
        create_table_if_needed(table_name, sku_data_list[0])
        
        # 删除旧数据
        logger.info(f"正在删除 {start_date_str} 至今的旧数据...")
        delete_by_date_range(table_name, start_date_str, end_date_str)
        
        # 插入新数据
        logger.info("正在写入新数据...")
        insert_data_batch(table_name, sku_data_list)
        
        # 输出统计信息
        logger.info("="*80)
        logger.info("统计信息：")
        logger.info(f"  数据范围: {start_date_str} 至 {end_date_str}")
        logger.info(f"  采购单数量: {len(orders)}")
        logger.info(f"  SKU记录数: {len(sku_data_list)}")
        
        if sku_data_list:
            logger.info(f"  平均每个采购单包含: {len(sku_data_list) / len(orders):.2f} 个SKU")
            
            # 统计各店铺的记录数
            shop_counts = {}
            for record in sku_data_list:
                shop_name = record.get('店铺名', '未知')
                shop_counts[shop_name] = shop_counts.get(shop_name, 0) + 1
            
            logger.info("  各店铺SKU记录数：")
            for shop_name, count in sorted(shop_counts.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    {shop_name}: {count} 条")
        
        logger.info("="*80)
        logger.info("全量更新完成！")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"数据库操作失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    asyncio.run(main())

