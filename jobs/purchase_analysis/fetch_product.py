#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
产品管理数据采集任务
从领星API获取产品管理数据并存入数据库
"""
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any

# 导入公共模块
from common import settings, get_logger
from common.database import db_cursor
from lingxing import OpenApiBase
from utils.date_utils import convert_timestamp_to_datetime

# 获取日志记录器
logger = get_logger('product_management')


def extract_spu_from_sku(sku: str) -> str:
    """
    从SKU中提取SPU（第一个"-"之前的字符）
    
    Args:
        sku: SKU字符串
        
    Returns:
        str: SPU字符串，如果没有"-"则返回原SKU
    """
    if not sku:
        return ''
    
    sku_str = str(sku).strip()
    if '-' in sku_str:
        return sku_str.split('-')[0]
    return sku_str


def extract_supplier_names(supplier_quote: List[Dict[str, Any]]) -> str:
    """
    从供应商报价列表中提取供应商名称
    
    Args:
        supplier_quote: 供应商报价列表
        
    Returns:
        str: 供应商名称，多个用逗号分隔
    """
    if not supplier_quote:
        return ''
    
    supplier_names = []
    for quote in supplier_quote:
        supplier_name = quote.get('supplier_name', '')
        if supplier_name:
            supplier_names.append(supplier_name)
    
    return ','.join(supplier_names)


async def fetch_products_by_time_range(op_api: OpenApiBase, token: str, 
                                       time_start: int, time_end: int,
                                       use_create_time: bool = False) -> List[Dict[str, Any]]:
    """
    获取指定时间范围内的产品数据（分页处理）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        time_start: 开始时间（时间戳，单位：秒）
        time_end: 结束时间（时间戳，单位：秒）
        use_create_time: 是否使用创建时间，False则使用更新时间
        
    Returns:
        List[Dict[str, Any]]: 产品列表
    """
    all_products = []
    offset = 0
    length = 1000  # 每页最多1000条
    
    while True:
        req_body = {
            "offset": offset,
            "length": length
        }
        
        # 根据参数决定使用创建时间还是更新时间
        if use_create_time:
            req_body["create_time_start"] = time_start
            req_body["create_time_end"] = time_end
        else:
            req_body["update_time_start"] = time_start
            req_body["update_time_end"] = time_end
        
        try:
            resp = await op_api.request(
                token, 
                "/erp/sc/routing/data/local_inventory/productList", 
                "POST", 
                req_body=req_body
            )
            # 兼容Pydantic v1和v2
            try:
                result = resp.model_dump()  # Pydantic v2
            except AttributeError:
                result = resp.dict()  # Pydantic v1
            
            products = result.get('data', [])
            
            if not products:
                break
                
            all_products.extend(products)
            time_type = "创建时间" if use_create_time else "更新时间"
            logger.info(f"已获取 {len(all_products)} 条产品（基于{time_type}）...")
            
            # 如果返回的数据少于length，说明已经是最后一页
            if len(products) < length:
                break
                
            offset += length
            await asyncio.sleep(settings.COLLECTION_DELAY_SECONDS)
            
        except Exception as e:
            logger.error(f"获取产品数据失败（offset={offset}）: {e}")
            break
    
    return all_products


async def fetch_all_products(op_api: OpenApiBase, token: str, 
                             time_start: int, time_end: int) -> List[Dict[str, Any]]:
    """
    获取最近30天内创建或更新的所有产品数据（合并去重）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        time_start: 开始时间（时间戳，单位：秒）
        time_end: 结束时间（时间戳，单位：秒）
        
    Returns:
        List[Dict[str, Any]]: 产品列表（已去重）
    """
    # 分别查询创建时间和更新时间范围内的产品
    logger.info("正在查询创建时间范围内的产品...")
    products_by_create = await fetch_products_by_time_range(
        op_api, token, time_start, time_end, use_create_time=True
    )
    logger.info(f"基于创建时间获取到 {len(products_by_create)} 条产品")
    
    logger.info("正在查询更新时间范围内的产品...")
    products_by_update = await fetch_products_by_time_range(
        op_api, token, time_start, time_end, use_create_time=False
    )
    logger.info(f"基于更新时间获取到 {len(products_by_update)} 条产品")
    
    # 合并并去重（使用产品ID作为唯一标识）
    products_dict = {}
    for product in products_by_create + products_by_update:
        product_id = product.get('id')
        if product_id:
            # 如果已存在，保留更新时间更近的记录
            if product_id not in products_dict:
                products_dict[product_id] = product
            else:
                existing_update_time = products_dict[product_id].get('update_time', 0)
                current_update_time = product.get('update_time', 0)
                if current_update_time > existing_update_time:
                    products_dict[product_id] = product
    
    all_products = list(products_dict.values())
    logger.info(f"合并去重后共 {len(all_products)} 条产品")
    
    return all_products


def convert_product_data(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    将产品数据转换为所需格式
    
    Args:
        products: 产品列表
        
    Returns:
        List[Dict[str, Any]]: 转换后的产品数据列表
    """
    converted_data = []
    
    for product in products:
        sku = product.get('sku', '')
        
        # 提取SPU（从SKU第一个"-"之前）
        spu = extract_spu_from_sku(sku)
        
        # 提取供应商名称
        supplier_quote = product.get('supplier_quote', [])
        supplier_names = extract_supplier_names(supplier_quote)
        
        # 转换时间戳
        create_time = product.get('create_time', 0)
        update_time = product.get('update_time', 0)
        create_time_str = convert_timestamp_to_datetime(create_time) if create_time else ''
        update_time_str = convert_timestamp_to_datetime(update_time) if update_time else ''
        
        # 构建数据记录
        record = {
            'SKU': sku,
            'SPU': spu,
            '品名': product.get('product_name', ''),
            '图片链接': product.get('pic_url', ''),
            '采购成本': product.get('cg_price', 0),
            '状态文本': product.get('status_text', ''),
            '创建时间': create_time_str,
            '更新时间': update_time_str,
            '开发人员名称': product.get('product_developer', ''),
            '采购员名称': product.get('cg_opt_username', ''),
            '供应商名称': supplier_names,
        }
        
        converted_data.append(record)
    
    return converted_data


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


def delete_by_sku_list(table_name: str, sku_list: List[str]) -> int:
    """
    根据SKU列表删除数据
    
    Args:
        table_name: 表名
        sku_list: SKU列表
        
    Returns:
        int: 删除的记录数
    """
    if not sku_list:
        return 0
    
    with db_cursor(dictionary=False) as cursor:
        # 使用IN子句批量删除
        placeholders = ','.join(['%s'] * len(sku_list))
        sql = f"DELETE FROM `{table_name}` WHERE `SKU` IN ({placeholders})"
        cursor.execute(sql, sku_list)
        deleted_count = cursor.rowcount
        logger.info(f"已删除 {deleted_count} 条旧数据（基于SKU列表）")
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
    logger.info("产品管理数据增量更新（最近30天）")
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
    
    # 计算最近30天的日期范围
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # 转换为时间戳（秒）
    time_start = int(start_date.timestamp())
    time_end = int(end_date.timestamp())
    
    logger.info(f"查询时间范围：{start_date_str} 至 {end_date_str}（最近30天）")
    logger.info(f"时间戳范围：{time_start} 至 {time_end}")
    logger.info(f"查询策略：同时查询创建时间和更新时间范围内的产品，然后合并去重")
    
    # 获取产品数据（同时查询创建时间和更新时间）
    logger.info("正在获取产品数据...")
    products = await fetch_all_products(op_api, token_resp.access_token, time_start, time_end)
    logger.info(f"共获取 {len(products)} 条产品")
    
    if not products:
        logger.warning("没有数据需要保存")
        return
    
    # 转换为所需格式
    logger.info("正在转换数据格式...")
    converted_data = convert_product_data(products)
    logger.info(f"共生成 {len(converted_data)} 条数据")
    
    if not converted_data:
        logger.warning("没有数据需要保存")
        return
    
    # 处理数据库
    table_name = '产品管理'
    logger.info(f"正在处理数据库表 {table_name}...")
    
    try:
        # 创建或检查表结构
        create_table_if_needed(table_name, converted_data[0])
        
        # 提取SKU列表用于删除旧数据
        sku_list = [record.get('SKU', '') for record in converted_data if record.get('SKU')]
        logger.info(f"正在删除 {len(sku_list)} 个SKU的旧数据...")
        deleted_count = delete_by_sku_list(table_name, sku_list)
        
        # 插入新数据
        logger.info("正在写入新数据...")
        insert_data_batch(table_name, converted_data)
        
        # 输出统计信息
        logger.info("="*80)
        logger.info("统计信息：")
        logger.info(f"  更新策略: 增量更新（最近30天，基于创建时间或更新时间）")
        logger.info(f"  数据范围: {start_date_str} 至 {end_date_str}")
        logger.info(f"  删除旧记录: {deleted_count} 条")
        logger.info(f"  新增产品记录: {len(converted_data)} 条")
        
        # 统计各状态的记录数
        status_counts = {}
        for record in converted_data:
            status = record.get('状态文本', '未知')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info("  各状态产品数量：")
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"    {status}: {count} 条")
        
        logger.info("="*80)
        logger.info("增量更新完成！（保留30天前的历史数据）")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"数据库操作失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    asyncio.run(main())

