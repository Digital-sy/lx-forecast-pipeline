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


def get_skus_without_weight(table_name: str) -> List[str]:
    """
    从数据库读取毛重为空的SKU列表（只读取需要更新的SKU）
    
    Args:
        table_name: 表名
        
    Returns:
        List[str]: SKU列表（只包含毛重为空的SKU）
    """
    sku_list = []
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute(f"""
                SELECT DISTINCT `SKU` 
                FROM `{table_name}` 
                WHERE `SKU` IS NOT NULL 
                  AND `SKU` != '' 
                  AND (`单品毛重` IS NULL 
                   OR `单品毛重` = '' 
                   OR `单品毛重` = 0
                   OR `单品毛重` = '0')
            """)
            results = cursor.fetchall()
            sku_list = [row['SKU'] for row in results if row.get('SKU')]
            logger.info(f"从数据库读取到 {len(sku_list)} 个毛重为空的SKU（需要更新）")
    except Exception as e:
        logger.error(f"读取数据库SKU列表失败: {e}")
    return sku_list


async def batch_get_product_info(op_api: OpenApiBase, token: str, skus: List[str], 
                                 token_refresh_callback=None) -> List[Dict[str, Any]]:
    """
    批量获取产品详情（每次最多100个SKU）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        skus: SKU列表
        token_refresh_callback: Token刷新回调函数（当Token过期时调用）
        
    Returns:
        List[Dict[str, Any]]: 产品详情列表
    """
    all_products = []
    batch_size = 100  # 每次最多100个
    current_token = token
    token_refreshed = False  # 标记是否已刷新过Token
    
    for i in range(0, len(skus), batch_size):
        batch_skus = skus[i:i+batch_size]
        req_body = {
            "skus": batch_skus
        }
        
        try:
            resp = await op_api.request(
                current_token,
                "/erp/sc/routing/data/local_inventory/batchGetProductInfo",
                "POST",
                req_body=req_body
            )
            
            # 兼容Pydantic v1和v2
            try:
                result = resp.model_dump()  # Pydantic v2
            except AttributeError:
                result = resp.dict()  # Pydantic v1
            
            # 记录接口返回状态
            code = result.get('code', -1)
            message = result.get('message', '')
            products = result.get('data', [])
            
            # 如果Token过期，尝试刷新Token并重试
            if code == 2001005 and 'token' in message.lower() and token_refresh_callback and not token_refreshed:
                logger.warning(f"Token已过期，正在刷新Token并重试（SKU批次 {i//batch_size + 1}）...")
                try:
                    new_token = await token_refresh_callback()
                    if new_token:
                        current_token = new_token
                        token_refreshed = True
                        # 重试当前批次
                        resp = await op_api.request(
                            current_token,
                            "/erp/sc/routing/data/local_inventory/batchGetProductInfo",
                            "POST",
                            req_body=req_body
                        )
                        try:
                            result = resp.model_dump()
                        except AttributeError:
                            result = resp.dict()
                        code = result.get('code', -1)
                        message = result.get('message', '')
                        products = result.get('data', [])
                        logger.info(f"Token刷新成功，重试成功（SKU批次 {i//batch_size + 1}）")
                    else:
                        logger.error(f"Token刷新失败，跳过当前批次")
                except Exception as e:
                    logger.error(f"Token刷新失败: {e}")
            
            # 如果返回错误码，记录详细信息
            if code != 0:
                logger.warning(f"批量查询产品详情返回错误（SKU批次 {i//batch_size + 1}）: code={code}, message={message}, 查询SKU数={len(batch_skus)}")
            
            # 如果data为空，记录详细信息
            if not products:
                logger.debug(f"批量查询产品详情返回空数据（SKU批次 {i//batch_size + 1}）: code={code}, message={message}, 查询SKU数={len(batch_skus)}, 前3个SKU={batch_skus[:3]}")
            else:
                all_products.extend(products)
                logger.info(f"批量查询产品详情：已获取 {len(all_products)} 条（本次 {len(products)} 条）...")
            
            # 避免请求过快
            if i + batch_size < len(skus):
                await asyncio.sleep(settings.COLLECTION_DELAY_SECONDS)
                
        except Exception as e:
            logger.error(f"批量查询产品详情失败（SKU批次 {i//batch_size + 1}）: {e}", exc_info=True)
            continue
    
    return all_products


async def fetch_products_by_time_range(op_api: OpenApiBase, token: str, 
                                       time_start: int, time_end: int,
                                       use_create_time: bool = False,
                                       test_mode: bool = False) -> List[Dict[str, Any]]:
    """
    获取指定时间范围内的产品数据（分页处理）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        time_start: 开始时间（时间戳，单位：秒）
        time_end: 结束时间（时间戳，单位：秒）
        use_create_time: 是否使用创建时间，False则使用更新时间
        test_mode: 测试模式，只获取一页数据
        
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
            
            # 测试模式：只获取一页就返回
            if test_mode:
                break
            
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
        op_api, token, time_start, time_end, use_create_time=True, test_mode=False
    )
    logger.info(f"基于创建时间获取到 {len(products_by_create)} 条产品")
    
    logger.info("正在查询更新时间范围内的产品...")
    products_by_update = await fetch_products_by_time_range(
        op_api, token, time_start, time_end, use_create_time=False, test_mode=False
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


async def fetch_all_products_full(op_api: OpenApiBase, token: str) -> List[Dict[str, Any]]:
    """
    全量获取所有产品数据（不限制时间范围）
    
    Args:
        op_api: OpenAPI客户端
        token: 访问令牌
        
    Returns:
        List[Dict[str, Any]]: 产品列表（已去重）
    """
    all_products = []
    offset = 0
    length = 1000  # 每页最多1000条
    
    logger.info("开始全量获取所有产品数据（不限制时间范围）...")
    
    while True:
        req_body = {
            "offset": offset,
            "length": length
        }
        
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
            logger.info(f"已获取 {len(all_products)} 条产品...")
            
            # 如果返回的数据少于length，说明已经是最后一页
            if len(products) < length:
                break
                
            offset += length
            await asyncio.sleep(settings.COLLECTION_DELAY_SECONDS)
            
        except Exception as e:
            logger.error(f"获取产品数据失败（offset={offset}）: {e}")
            break
    
    # 去重（使用产品ID作为唯一标识）
    products_dict = {}
    for product in all_products:
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
    
    unique_products = list(products_dict.values())
    logger.info(f"全量获取完成，共 {len(unique_products)} 条产品（去重后）")
    
    return unique_products


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
        
        # 获取毛重和包装规格（从批量查询接口返回的详细数据）
        gross_weight = product.get('cg_product_gross_weight', None)  # 产品毛重（G）
        package_length = product.get('cg_package_length', None)  # 包装长度（CM）
        package_width = product.get('cg_package_width', None)  # 包装宽度（CM）
        package_height = product.get('cg_package_height', None)  # 包装高度（CM）
        
        # 转换数字类型：空值使用None（数据库会存储为NULL），非空值转换为float
        def to_float_or_none(value):
            if value is None or value == '':
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        gross_weight_float = to_float_or_none(gross_weight)
        package_length_float = to_float_or_none(package_length)
        package_width_float = to_float_or_none(package_width)
        package_height_float = to_float_or_none(package_height)
        
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
            '单品毛重': gross_weight_float,  # 产品毛重（G），None表示NULL
            '包装长度': package_length_float,  # 包装长度（CM），None表示NULL
            '包装宽度': package_width_float,  # 包装宽度（CM），None表示NULL
            '包装高度': package_height_float,  # 包装高度（CM），None表示NULL
        }
        
        converted_data.append(record)
    
    return converted_data


def create_table_if_needed(table_name: str, sample_row: Dict[str, Any]) -> None:
    """
    创建或更新数据表结构（支持添加新字段）
    
    Args:
        table_name: 表名
        sample_row: 样本数据行
    """
    with db_cursor(dictionary=False) as cursor:
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = cursor.fetchone()
        
        if not exists:
            # 创建新表
            fields = []
            for k, v in sample_row.items():
                # 包装规格相关字段应该是数字类型
                if k in ['包装长度', '包装宽度', '包装高度', '单品毛重']:
                    fields.append(f"`{k}` DOUBLE")
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    if isinstance(v, float):
                        fields.append(f"`{k}` DOUBLE")
                    else:
                        fields.append(f"`{k}` INT")
                else:
                    fields.append(f"`{k}` VARCHAR(500)")
            
            fields_sql = ", ".join(fields)
            sql = f"CREATE TABLE `{table_name}` (id INT AUTO_INCREMENT PRIMARY KEY, {fields_sql})"
            cursor.execute(sql)
            logger.info(f"表 {table_name} 创建成功")
        else:
            # 检查并添加缺失的字段
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            existing_columns = {row[0] for row in cursor.fetchall()}
            expected_columns = set(sample_row.keys())
            
            missing_columns = expected_columns - existing_columns
            if missing_columns:
                logger.info(f"表 {table_name} 需要添加 {len(missing_columns)} 个新字段")
                for col_name in missing_columns:
                    col_value = sample_row[col_name]
                    # 包装规格相关字段应该是数字类型
                    if col_name in ['包装长度', '包装宽度', '包装高度', '单品毛重']:
                        col_type = "DOUBLE"
                    elif isinstance(col_value, (int, float)) and not isinstance(col_value, bool):
                        if isinstance(col_value, float):
                            col_type = "DOUBLE"
                        else:
                            col_type = "INT"
                    else:
                        col_type = "VARCHAR(500)"
                    
                    try:
                        cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}")
                        logger.info(f"  已添加字段: {col_name} ({col_type})")
                    except Exception as e:
                        logger.warning(f"  添加字段 {col_name} 失败: {e}")
            else:
                logger.info(f"表 {table_name} 结构完整，无需更新")


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


def delete_all_data(table_name: str) -> int:
    """
    删除表中的所有数据（全量更新时使用）
    
    Args:
        table_name: 表名
        
    Returns:
        int: 删除的记录数
    """
    with db_cursor(dictionary=False) as cursor:
        # 先查询记录数
        cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
        total_count = cursor.fetchone()[0]
        
        # 删除所有数据
        cursor.execute(f"DELETE FROM `{table_name}`")
        deleted_count = cursor.rowcount
        logger.info(f"已删除所有数据：{deleted_count} 条（原总数：{total_count} 条）")
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
    # 测试模式：只获取一页数据
    TEST_MODE = False
    # 全量获取模式：获取所有产品数据（不限制时间范围）
    FULL_SYNC = False  # 设置为True进行全量获取
    
    logger.info("="*80)
    if TEST_MODE:
        logger.info("【测试模式】产品管理数据 - 仅获取一页数据")
    elif FULL_SYNC:
        logger.info("【全量获取模式】产品管理数据 - 获取所有产品数据")
    else:
        logger.info("产品管理数据增量更新（最近7天）")
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
    
    # 计算时间范围
    if FULL_SYNC:
        # 全量获取：不限制时间范围
        logger.info("全量获取模式：不限制时间范围，获取所有产品")
    else:
        # 增量更新：最近7天
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # 转换为时间戳（秒）
        time_start = int(start_date.timestamp())
        time_end = int(end_date.timestamp())
        
        logger.info(f"查询时间范围：{start_date_str} 至 {end_date_str}（最近7天）")
        logger.info(f"时间戳范围：{time_start} 至 {time_end}")
    
    if TEST_MODE:
        # 测试模式：只获取一页数据（基于更新时间）
        logger.info("【测试模式】只获取第一页数据（基于更新时间）...")
        products = await fetch_products_by_time_range(
            op_api, token_resp.access_token, time_start, time_end, 
            use_create_time=False, test_mode=True
        )
        logger.info(f"共获取 {len(products)} 条产品（第一页）")
        
        if not products:
            logger.warning("没有获取到数据")
            return
        
        # 打印原始数据的第一条（完整内容）
        if products:
            logger.info("="*80)
            logger.info("产品完整数据（第一条）：")
            logger.info("="*80)
            import json
            first_product = products[0]
            
            # 打印完整数据（包含所有嵌套内容）
            logger.info(json.dumps(first_product, ensure_ascii=False, indent=2))
            logger.info("="*80)
            
            # 打印所有字段的键名（便于快速查看）
            logger.info("")
            logger.info("字段列表（共{}个）：".format(len(first_product.keys())))
            all_keys = sorted(first_product.keys())
            for i, key in enumerate(all_keys, 1):
                value = first_product[key]
                value_type = type(value).__name__
                if isinstance(value, list):
                    logger.info(f"  {i:2d}. {key:25s} (类型: {value_type}, 长度: {len(value)})")
                elif isinstance(value, dict):
                    logger.info(f"  {i:2d}. {key:25s} (类型: {value_type}, 键数: {len(value)})")
                else:
                    logger.info(f"  {i:2d}. {key:25s} (类型: {value_type})")
            
            # 检查是否有重量相关字段
            logger.info("")
            weight_keys = [k for k in all_keys if 'weight' in k.lower() or '重量' in k or '重' in k]
            if weight_keys:
                logger.info(f"找到重量相关字段：{', '.join(weight_keys)}")
            else:
                logger.info("未找到重量相关字段（weight/重量）")
            
            logger.info("="*80)
        
        # 转换为所需格式
        logger.info("正在转换数据格式...")
        converted_data = convert_product_data(products)
        logger.info(f"共生成 {len(converted_data)} 条转换后的数据")
        
        # 打印转换后的数据
        if converted_data:
            logger.info("="*80)
            logger.info("转换后的数据示例（前3条）：")
            logger.info("="*80)
            import json
            for i, record in enumerate(converted_data[:3], 1):
                logger.info(f"\n第 {i} 条数据：")
                logger.info(json.dumps(record, ensure_ascii=False, indent=2))
            logger.info("="*80)
        
        logger.info("测试完成！")
        return
    
    # 正常模式：获取数据
    if FULL_SYNC:
        # 全量获取：获取所有产品数据
        logger.info("正在全量获取所有产品数据...")
        products = await fetch_all_products_full(op_api, token_resp.access_token)
        logger.info(f"共获取 {len(products)} 条产品")
    else:
        # 增量更新：获取最近30天的数据
        logger.info(f"查询策略：同时查询创建时间和更新时间范围内的产品，然后合并去重")
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
        
        # 根据模式决定删除策略
        if FULL_SYNC:
            # 全量更新：删除所有数据
            logger.info("全量更新模式：正在删除所有旧数据...")
            deleted_count = delete_all_data(table_name)
        else:
            # 增量更新：只删除本次更新的SKU的旧数据
            sku_list = [record.get('SKU', '') for record in converted_data if record.get('SKU')]
            logger.info(f"增量更新模式：正在删除 {len(sku_list)} 个SKU的旧数据...")
            deleted_count = delete_by_sku_list(table_name, sku_list)
        
        # 插入新数据
        logger.info("正在写入新数据...")
        insert_data_batch(table_name, converted_data)
        
        # 输出统计信息
        logger.info("="*80)
        logger.info("统计信息：")
        if FULL_SYNC:
            logger.info(f"  更新策略: 全量更新（获取所有产品数据）")
        else:
            logger.info(f"  更新策略: 增量更新（最近7天，基于创建时间或更新时间）")
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
        if FULL_SYNC:
            logger.info("全量更新完成！")
        else:
            logger.info("增量更新完成！（保留7天前的历史数据）")
        logger.info("="*80)
        
        # 批量查询并更新产品毛重和包装规格
        logger.info("")
        logger.info("="*80)
        logger.info("开始批量更新产品毛重和包装规格...")
        logger.info("="*80)
        
        # 测试模式：只处理前100条
        TEST_BATCH_UPDATE = False
        TEST_LIMIT = 100
        
        try:
            # 从数据库读取毛重为空的SKU（只读取需要更新的）
            all_skus = get_skus_without_weight(table_name)
            if not all_skus:
                logger.warning("数据库中没有毛重为空的SKU，跳过批量更新")
            else:
                # 测试模式：只处理前N条
                if TEST_BATCH_UPDATE:
                    original_count = len(all_skus)
                    all_skus = all_skus[:TEST_LIMIT]
                    logger.info(f"【测试模式】限制处理数量：{len(all_skus)} 条（原总数：{original_count} 条）")
                
                # 转换数字类型的辅助函数
                def to_float_or_none(value):
                    if value is None or value == '':
                        return None
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None
                
                # 分批处理：每批1000个SKU，查询后立即更新
                query_batch_size = 1000  # 每批查询1000个SKU
                update_batch_size = 500  # 每批更新500条记录
                total_skus = len(all_skus)
                total_updated = 0
                total_skipped = 0
                total_processed = 0
                
                logger.info(f"开始分批处理，共 {total_skus} 个SKU，每批 {query_batch_size} 个")
                
                # 使用可变对象来存储当前Token，以便在刷新后更新
                current_token = [token_resp.access_token]  # 使用列表以便在回调中修改
                
                # Token刷新回调函数（所有批次共享）
                async def refresh_token():
                    try:
                        new_token_resp = await op_api.generate_access_token()
                        logger.info(f"Token已刷新，新Token有效期: {new_token_resp.expires_in}秒")
                        current_token[0] = new_token_resp.access_token  # 更新共享的Token
                        return new_token_resp.access_token
                    except Exception as e:
                        logger.error(f"刷新Token失败: {e}")
                        return None
                
                for batch_start in range(0, total_skus, query_batch_size):
                    batch_end = min(batch_start + query_batch_size, total_skus)
                    batch_skus = all_skus[batch_start:batch_end]
                    batch_num = batch_start // query_batch_size + 1
                    total_batches = (total_skus + query_batch_size - 1) // query_batch_size
                    
                    logger.info(f"处理第 {batch_num}/{total_batches} 批：SKU {batch_start+1} - {batch_end}（共 {len(batch_skus)} 个）")
                    
                    # 查询这一批的产品详情（传入Token刷新回调）
                    detailed_products = await batch_get_product_info(
                        op_api, current_token[0], batch_skus, 
                        token_refresh_callback=refresh_token
                    )
                    
                    if not detailed_products:
                        logger.warning(f"  查询到 0 条产品详情（本批 {len(batch_skus)} 个SKU，前3个SKU: {batch_skus[:3]}）")
                        logger.warning(f"  可能原因：1) 这些SKU在领星系统中不存在 2) 接口返回空数据 3) 接口报错（请查看上方日志）")
                        total_processed += len(batch_skus)
                        continue
                    else:
                        logger.info(f"  查询到 {len(detailed_products)} 条产品详情")
                    
                    # 转换为字典，以SKU为键
                    product_dict = {}
                    for product in detailed_products:
                        sku = product.get('sku', '')
                        if sku:
                            product_dict[sku] = product
                    
                    # 查询这一批中需要更新的SKU（因为已经筛选过，这里只需要确认SKU存在即可）
                    with db_cursor(dictionary=True) as cursor:
                        placeholders = ','.join(['%s'] * len(batch_skus))
                        cursor.execute(f"""
                            SELECT `SKU`, `单品毛重`, `更新时间` 
                            FROM `{table_name}` 
                            WHERE `SKU` IN ({placeholders})
                              AND (`单品毛重` IS NULL OR `单品毛重` = '' OR `单品毛重` = 0)
                        """, batch_skus)
                        records_to_update = cursor.fetchall()
                    
                    if not records_to_update:
                        total_processed += len(batch_skus)
                        logger.info(f"  本批无需更新的记录")
                        continue
                    
                    # 准备更新数据
                    update_data = []
                    skipped_count = 0
                    
                    for record in records_to_update:
                        sku = record['SKU']
                        if sku not in product_dict:
                            skipped_count += 1
                            continue
                        
                        product = product_dict[sku]
                        
                        # 获取毛重和包装规格
                        gross_weight = product.get('cg_product_gross_weight', None)
                        package_length = product.get('cg_package_length', None)
                        package_width = product.get('cg_package_width', None)
                        package_height = product.get('cg_package_height', None)
                        
                        # 转换数字类型
                        gross_weight_float = to_float_or_none(gross_weight)
                        package_length_float = to_float_or_none(package_length)
                        package_width_float = to_float_or_none(package_width)
                        package_height_float = to_float_or_none(package_height)
                        
                        update_data.append((
                            gross_weight_float,
                            package_length_float,
                            package_width_float,
                            package_height_float,
                            sku
                        ))
                    
                    logger.info(f"  准备更新 {len(update_data)} 条，跳过 {skipped_count} 条（无产品详情）")
                    
                    # 批量执行更新
                    if update_data:
                        with db_cursor(dictionary=False) as cursor:
                            sql = f"""
                                UPDATE `{table_name}` 
                                SET `单品毛重` = %s, 
                                    `包装长度` = %s, 
                                    `包装宽度` = %s, 
                                    `包装高度` = %s 
                                WHERE `SKU` = %s
                            """
                            
                            for i in range(0, len(update_data), update_batch_size):
                                batch = update_data[i:i+update_batch_size]
                                try:
                                    cursor.executemany(sql, batch)
                                    batch_updated = cursor.rowcount
                                    total_updated += batch_updated
                                    logger.info(f"  已更新 {min(i+update_batch_size, len(update_data))} / {len(update_data)} 条记录（本批累计：{batch_updated} 条）")
                                except Exception as e:
                                    logger.error(f"  批量更新失败（批次 {i//update_batch_size + 1}）: {e}")
                                    # 如果批量更新失败，尝试逐条更新这一批
                                    for data in batch:
                                        try:
                                            cursor.execute(sql, data)
                                            if cursor.rowcount > 0:
                                                total_updated += 1
                                        except Exception as e2:
                                            logger.warning(f"  更新SKU {data[4]} 失败: {e2}")
                    
                    total_skipped += skipped_count
                    total_processed += len(batch_skus)
                    logger.info(f"  第 {batch_num} 批完成，累计处理 {total_processed}/{total_skus} 个SKU，累计更新 {total_updated} 条记录")
                
                logger.info("="*80)
                logger.info(f"批量更新完成！")
                logger.info(f"  总SKU数: {total_skus}")
                logger.info(f"  已处理: {total_processed}")
                logger.info(f"  已更新: {total_updated} 条记录")
                logger.info(f"  跳过: {total_skipped} 条（无产品详情）")
                if TEST_BATCH_UPDATE:
                    logger.info(f"【测试模式】本次仅处理了前 {TEST_LIMIT} 个SKU")
                logger.info("="*80)
                    
        except Exception as e:
            logger.error(f"批量更新产品毛重和包装规格失败: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"数据库操作失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    asyncio.run(main())

