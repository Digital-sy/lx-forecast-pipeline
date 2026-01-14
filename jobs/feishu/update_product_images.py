#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
商品图片更新任务
从飞书多维表读取SKU，查询数据库listing表的商品缩略图，更新飞书多维表的图片字段
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import httpx

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import settings, get_logger
from common.database import db_cursor
from common.feishu import FeishuClient

logger = get_logger('feishu_update_images')

# 飞书多维表格配置
FEISHU_APP_TOKEN = "ERyub7DVlaNhHMs4QPYcQd09ndc"  # 飞书多维表格的app_token
FEISHU_TABLE_ID = "tbl6FsWSjOdldYYn"  # 飞书多维表格的table_id（原表）
FEISHU_VIEW_ID = "vew6LHqDx4"  # 飞书多维表格的view_id（原表视图）

# 图片变更记录表配置（同一个多维表）
FEISHU_CHANGE_TABLE_ID = "tbld4TMeXogYdgwC"  # 图片变更记录表的table_id
FEISHU_CHANGE_VIEW_ID = "vew6LHqDx4"  # 图片变更记录表的view_id


def get_listing_thumbnail_map() -> Dict[str, str]:
    """
    从数据库listing表读取SKU到商品缩略图的映射
    
    Returns:
        Dict[str, str]: {SKU: 商品缩略图URL} 的映射字典
    """
    thumbnail_map = {}
    
    try:
        with db_cursor() as cursor:
            # 查询listing表的SKU和商品缩略图
            # 注意：数据库字段名是SKU，不是MSKU
            sql = """
            SELECT 
                SKU as sku,
                商品缩略图 as thumbnail
            FROM `listing`
            WHERE SKU IS NOT NULL 
              AND SKU != '' 
              AND SKU != '无'
              AND 商品缩略图 IS NOT NULL
              AND 商品缩略图 != ''
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            for row in results:
                sku = row.get('sku', '').strip()
                thumbnail = row.get('thumbnail', '').strip()
                
                if sku and thumbnail:
                    # 如果同一个SKU有多个缩略图，保留第一个非空的
                    if sku not in thumbnail_map:
                        thumbnail_map[sku] = thumbnail
            
            logger.info(f"从listing表读取到 {len(thumbnail_map)} 条SKU到商品缩略图的映射")
    except Exception as e:
        logger.error(f"从listing表读取商品缩略图映射失败: {e}")
        raise
    
    return thumbnail_map


async def update_feishu_images():
    """
    更新飞书多维表的商品图片
    1. 读取飞书多维表的所有记录
    2. 根据SKU查询数据库listing表的商品缩略图
    3. 将"现在图"字段内容写到"上次图"字段
    4. 将找到的商品缩略图写入"现在图"字段
    5. 更新"更新日期"字段为当前日期
    """
    # 初始化飞书客户端
    feishu_client = FeishuClient(
        app_token=FEISHU_APP_TOKEN,
        table_id=FEISHU_TABLE_ID,
        view_id=FEISHU_VIEW_ID
    )
    
    # 获取访问令牌
    await feishu_client.get_access_token()
    
    # 获取字段映射（字段名 -> 字段ID）
    logger.info("正在获取飞书多维表字段信息...")
    field_id_to_name = await feishu_client.get_table_fields()
    field_name_to_id = {name: fid for fid, name in field_id_to_name.items()}
    
    # 打印所有字段名用于调试
    logger.info(f"飞书多维表字段列表: {', '.join(field_name_to_id.keys())}")
    
    # 检查必要的字段是否存在（不区分大小写）
    required_fields = ['sku', '现在图', '上次图']
    missing_fields = []
    field_name_mapping = {}  # 用于存储实际字段名到标准字段名的映射
    
    # 创建不区分大小写的字段名映射
    field_name_lower_map = {name.lower(): name for name in field_name_to_id.keys()}
    
    for required_field in required_fields:
        # 先尝试精确匹配
        if required_field in field_name_to_id:
            field_name_mapping[required_field] = required_field
        # 再尝试不区分大小写匹配
        elif required_field.lower() in field_name_lower_map:
            actual_field_name = field_name_lower_map[required_field.lower()]
            field_name_mapping[required_field] = actual_field_name
            logger.info(f"字段名匹配: '{required_field}' -> '{actual_field_name}'")
        else:
            missing_fields.append(required_field)
    
    if missing_fields:
        logger.error(f"飞书多维表中缺少必要字段: {', '.join(missing_fields)}")
        logger.info(f"可用字段: {', '.join(field_name_to_id.keys())}")
        raise Exception(f"飞书多维表中缺少必要字段: {', '.join(missing_fields)}")
    
    # 检查"更新日期"字段是否存在（可选字段，不区分大小写）
    has_update_date_field = False
    update_date_field_name = None
    if '更新日期' in field_name_to_id:
        has_update_date_field = True
        update_date_field_name = '更新日期'
    else:
        # 尝试不区分大小写匹配
        for field_name in field_name_to_id.keys():
            if '更新日期' in field_name or '更新' in field_name and '日期' in field_name:
                has_update_date_field = True
                update_date_field_name = field_name
                logger.info(f"找到更新日期字段: '{field_name}'")
                break
    
    if not has_update_date_field:
        logger.warning("飞书多维表中没有'更新日期'字段，将跳过更新日期")
    
    # 读取飞书多维表的所有记录
    logger.info("正在读取飞书多维表记录...")
    records = await feishu_client.read_records()
    logger.info(f"共读取到 {len(records)} 条记录")
    
    if not records:
        logger.warning("飞书多维表中没有记录，无需更新")
        return
    
    # 调试：打印第一条记录的详细信息
    if records:
        first_record = records[0]
        logger.info(f"第一条记录ID: {first_record.get('record_id')}")
        logger.info(f"第一条记录的字段ID列表: {list(first_record.get('fields', {}).keys())}")
        # 打印所有字段的值（用于调试）
        for field_id, field_value in first_record.get('fields', {}).items():
            field_name = field_id_to_name.get(field_id, field_id)
            logger.info(f"  字段 '{field_name}' ({field_id}): {type(field_value).__name__} = {repr(field_value)[:200]}")
    
    # 从数据库读取SKU到商品缩略图的映射
    logger.info("正在从数据库读取商品缩略图映射...")
    thumbnail_map = get_listing_thumbnail_map()
    
    # 获取当前日期（用于更新"更新日期"字段）
    current_date = datetime.now()
    
    # 准备更新的记录
    update_records = []
    updated_count = 0
    not_found_count = 0
    
    for record in records:
        record_id = record.get('record_id')
        if not record_id:
            logger.warning("记录缺少record_id，跳过")
            continue
        
        fields = record.get('fields', {})
        
        # 获取SKU字段值
        # 注意：飞书API返回的fields字典，键可能是字段名而不是字段ID
        sku = None
        sku_field_name = field_name_mapping.get('sku', 'sku')
        
        # 先尝试用字段名作为键（因为从日志看，fields的键就是字段名）
        sku_value = fields.get(sku_field_name)
        
        # 如果找不到，再尝试用字段ID作为键
        if sku_value is None and sku_field_name in field_name_to_id:
            sku_field_id = field_name_to_id[sku_field_name]
            sku_value = fields.get(sku_field_id)
        
        # 调试：打印SKU字段的原始值
        logger.info(f"记录 {record_id} 的SKU字段原始值: {repr(sku_value)} (类型: {type(sku_value).__name__ if sku_value is not None else 'NoneType'})")
        
        if sku_value is not None:
            # 处理飞书API返回的字段值格式
            if isinstance(sku_value, list):
                # 数组类型：可能是多行文本或单选
                if len(sku_value) > 0:
                    if isinstance(sku_value[0], dict):
                        sku = sku_value[0].get('text', '') or sku_value[0].get('name', '')
                    else:
                        sku = str(sku_value[0])
                else:
                    # 空数组
                    sku = None
            elif isinstance(sku_value, dict):
                # 对象类型：可能包含text或name字段
                sku = sku_value.get('text', '') or sku_value.get('name', '')
            elif isinstance(sku_value, str):
                sku = sku_value
            elif isinstance(sku_value, (int, float)):
                # 数字类型，转换为字符串
                sku = str(sku_value)
            else:
                sku = str(sku_value) if sku_value else None
        else:
            logger.warning(f"记录 {record_id} 的SKU字段值为None或不存在（字段名: {sku_field_name}）")
        
        # 调试：打印解析后的SKU值
        logger.info(f"记录 {record_id} 解析后的SKU值: {repr(sku)}")
        
        if not sku or (isinstance(sku, str) and not sku.strip()):
            logger.warning(f"记录 {record_id} 的SKU为空或无效，跳过。解析后: {repr(sku)}")
            continue
        
        sku = sku.strip()
        logger.debug(f"记录 {record_id} 的SKU: {sku}")
        
        # 获取"现在图"字段值（链接字段）
        current_image_field_name = field_name_mapping.get('现在图', '现在图')
        # 先尝试用字段名作为键
        current_image_value = fields.get(current_image_field_name)
        # 如果找不到，再尝试用字段ID作为键
        if current_image_value is None and current_image_field_name in field_name_to_id:
            current_image_field_id = field_name_to_id[current_image_field_name]
            current_image_value = fields.get(current_image_field_id)
        current_image = None
        
        if current_image_value:
            # 处理链接字段格式
            if isinstance(current_image_value, list) and len(current_image_value) > 0:
                # 数组类型：链接字段可能是数组
                if isinstance(current_image_value[0], dict):
                    current_image = current_image_value[0].get('link', '') or current_image_value[0].get('text', '')
                else:
                    current_image = str(current_image_value[0])
            elif isinstance(current_image_value, dict):
                # 对象类型：可能包含link字段
                current_image = current_image_value.get('link', '') or current_image_value.get('text', '')
            elif isinstance(current_image_value, str):
                current_image = current_image_value
            else:
                current_image = str(current_image_value) if current_image_value else None
        
        # 查询数据库中的商品缩略图
        thumbnail = thumbnail_map.get(sku)
        
        # 如果找不到，尝试不区分大小写查找
        if not thumbnail:
            # 尝试不区分大小写匹配
            for map_sku, map_thumbnail in thumbnail_map.items():
                if map_sku.upper() == sku.upper():
                    thumbnail = map_thumbnail
                    logger.info(f"SKU {sku} 通过不区分大小写匹配找到: {map_sku}")
                    break
        
        # 如果还是找不到，检查数据库中是否有这个SKU（即使缩略图为空）
        if not thumbnail:
            # 查询数据库中是否有这个SKU（无论缩略图是否为空）
            with db_cursor() as cursor:
                check_sql = """
                SELECT SKU, 商品缩略图
                FROM `listing`
                WHERE SKU = %s
                LIMIT 1
                """
                cursor.execute(check_sql, (sku,))
                result = cursor.fetchone()
                if result:
                    db_sku = result.get('SKU', '')
                    db_thumbnail = result.get('商品缩略图', '')
                    if not db_thumbnail or db_thumbnail.strip() == '':
                        logger.warning(f"SKU {sku} 在listing表中存在，但商品缩略图为空")
                    else:
                        logger.warning(f"SKU {sku} 在listing表中存在，但查询时未匹配到（可能原因：SKU值有差异）")
                        logger.warning(f"  数据库中的SKU: {repr(db_sku)}, 缩略图: {repr(db_thumbnail)[:100]}")
                else:
                    logger.warning(f"SKU {sku} 在listing表中不存在")
        
        # 获取"创建人"字段值（用于后续写入变更记录表）
        creator_value = None
        # 尝试查找创建人字段（可能是"创建人"或其他名称）
        for field_name in field_name_to_id.keys():
            if '创建人' in field_name or '创建者' in field_name or 'creator' in field_name.lower():
                creator_field_value = fields.get(field_name)
                if creator_field_value:
                    # 处理人员字段格式
                    if isinstance(creator_field_value, dict):
                        # 人员字段可能是对象，提取name或id
                        creator_value = creator_field_value.get('name', '') or creator_field_value.get('id', '')
                    elif isinstance(creator_field_value, list) and len(creator_field_value) > 0:
                        if isinstance(creator_field_value[0], dict):
                            creator_value = creator_field_value[0].get('name', '') or creator_field_value[0].get('id', '')
                        else:
                            creator_value = str(creator_field_value[0])
                    else:
                        creator_value = str(creator_field_value) if creator_field_value else None
                break
        
        # 准备更新记录
        update_record = {
            'record_id': record_id
        }
        
        # 获取实际字段名
        last_image_field_name = field_name_mapping.get('上次图', '上次图')
        current_image_field_name = field_name_mapping.get('现在图', '现在图')
        
        # 将"现在图"字段内容写到"上次图"字段（无论是否有新缩略图都要执行）
        # 如果"现在图"为空，则"上次图"也设置为空
        update_record[last_image_field_name] = current_image if current_image else ''
        
        if not thumbnail:
            logger.debug(f"SKU {sku} 在listing表中未找到商品缩略图")
            not_found_count += 1
            # 即使没有找到缩略图，也要更新"上次图"字段（将"现在图"的内容移过去）
            # "现在图"字段保持为空或原有值（这里我们设置为空，因为没找到新的）
            update_record[current_image_field_name] = ''
        else:
            # 将找到的商品缩略图写入"现在图"字段
            update_record[current_image_field_name] = thumbnail
            updated_count += 1
        
        # 更新"更新日期"字段为当前日期（如果字段存在）
        if has_update_date_field and update_date_field_name:
            update_record[update_date_field_name] = current_date
        
        # 正常更新原表（先更新，后续再检查是否需要移动到变更记录表）
        update_records.append(update_record)
        
        if updated_count % 100 == 0:
            logger.info(f"已准备 {updated_count} 条更新记录...")
    
    logger.info(f"共准备更新 {len(update_records)} 条记录（找到缩略图: {updated_count}，未找到: {not_found_count}）")
    
    if not update_records:
        logger.warning("没有需要更新的记录")
        return
    
    # 批量更新飞书多维表
    logger.info("正在批量更新飞书多维表...")
    try:
        updated = await feishu_client.update_records(update_records, batch_size=500)
        logger.info(f"✓ 成功更新 {updated} 条记录")
    except Exception as e:
        logger.error(f"更新飞书多维表失败: {e}")
        raise
    
    # 更新完成后，重新读取第一个表，检查是否有图片链接不一致的记录
    logger.info("正在检查更新后的记录，查找图片链接不一致的记录...")
    updated_records = await feishu_client.read_records()
    
    # 检查"更新次数"字段是否存在（可选字段，用于记录图片变更次数）
    update_count_field_name = None
    for field_name in field_name_to_id.keys():
        if '更新次数' in field_name or '变更次数' in field_name or 'update_count' in field_name.lower():
            update_count_field_name = field_name
            logger.info(f"找到更新次数字段: '{field_name}'")
            break
    
    if not update_count_field_name:
        logger.warning("飞书多维表中没有'更新次数'字段，将无法记录变更次数")
    
    # 准备需要移动到变更记录表的记录（图片链接不一致的记录）
    change_records = []  # 需要写入变更记录表的记录
    update_count_records = []  # 需要更新"更新次数"字段的记录（在原表中）
    
    for record in updated_records:
        record_id = record.get('record_id')
        if not record_id:
            continue
        
        fields = record.get('fields', {})
        
        # 获取SKU
        sku = None
        sku_field_name = field_name_mapping.get('sku', 'sku')
        sku_value = fields.get(sku_field_name)
        if sku_value:
            if isinstance(sku_value, str):
                sku = sku_value.strip()
            elif isinstance(sku_value, list) and len(sku_value) > 0:
                if isinstance(sku_value[0], dict):
                    sku = sku_value[0].get('text', '') or sku_value[0].get('name', '')
                else:
                    sku = str(sku_value[0])
            elif isinstance(sku_value, dict):
                sku = sku_value.get('text', '') or sku_value.get('name', '')
        
        if not sku:
            continue
        
        # 获取"上次图"和"现在图"的值
        last_image_field_name = field_name_mapping.get('上次图', '上次图')
        current_image_field_name = field_name_mapping.get('现在图', '现在图')
        
        last_image_value = fields.get(last_image_field_name)
        current_image_value = fields.get(current_image_field_name)
        
        # 提取链接URL
        last_image_url = None
        if last_image_value:
            if isinstance(last_image_value, dict):
                last_image_url = last_image_value.get('link', '') or last_image_value.get('text', '')
            elif isinstance(last_image_value, str):
                last_image_url = last_image_value
            elif isinstance(last_image_value, list) and len(last_image_value) > 0:
                if isinstance(last_image_value[0], dict):
                    last_image_url = last_image_value[0].get('link', '') or last_image_value[0].get('text', '')
                else:
                    last_image_url = str(last_image_value[0])
        
        current_image_url = None
        if current_image_value:
            if isinstance(current_image_value, dict):
                current_image_url = current_image_value.get('link', '') or current_image_value.get('text', '')
            elif isinstance(current_image_value, str):
                current_image_url = current_image_value
            elif isinstance(current_image_value, list) and len(current_image_value) > 0:
                if isinstance(current_image_value[0], dict):
                    current_image_url = current_image_value[0].get('link', '') or current_image_value[0].get('text', '')
                else:
                    current_image_url = str(current_image_value[0])
        
        # 比较链接是否不一样（去除首尾空格后比较）
        last_image_url = str(last_image_url).strip() if last_image_url else ''
        current_image_url = str(current_image_url).strip() if current_image_url else ''
        
        if last_image_url and current_image_url and last_image_url != current_image_url:
            # 图片链接不一样，需要移动到变更记录表
            logger.info(f"SKU {sku} 的图片链接不一致：上次图={last_image_url[:50]}..., 现在图={current_image_url[:50]}...")
            
            # 获取创建人（保持原始格式，因为人员字段需要对象格式）
            creator_value = None
            for field_name in field_name_to_id.keys():
                if '创建人' in field_name or '创建者' in field_name or 'creator' in field_name.lower():
                    creator_field_value = fields.get(field_name)
                    if creator_field_value:
                        # 保持原始格式（dict或list），因为人员字段需要对象格式
                        # 如果是dict，可能需要转换为数组格式（飞书API人员字段通常是数组）
                        if isinstance(creator_field_value, dict):
                            # 如果是单个用户对象，转换为数组格式
                            creator_value = [creator_field_value]
                        else:
                            # 已经是数组或其他格式，直接使用
                            creator_value = creator_field_value
                    break
            
            # 调试：打印创建人字段的值
            if creator_value:
                logger.info(f"SKU {sku} 的创建人字段值: {type(creator_value).__name__} = {repr(creator_value)[:200]}")
            else:
                logger.warning(f"SKU {sku} 的创建人字段为空或不存在")
            
            # 获取更新日期
            update_date_value = None
            if has_update_date_field and update_date_field_name:
                update_date_value = fields.get(update_date_field_name)
                if isinstance(update_date_value, (int, float)):
                    # 时间戳，转换为datetime（使用已导入的datetime）
                    update_date_value = datetime.fromtimestamp(update_date_value / 1000)
            
            # 准备写入变更记录表的记录
            change_record = {
                'sku': sku,
                '上次图': last_image_url,
                '现在图': current_image_url,
            }
            
            # 添加更新日期
            if update_date_value:
                change_record['更新日期'] = update_date_value
            elif has_update_date_field and update_date_field_name:
                change_record['更新日期'] = current_date
            
            # 添加创建人（如果存在）
            if creator_value:
                change_record['创建人'] = creator_value
            
            change_records.append(change_record)
            
            # 更新第一个表的"更新次数"字段（加1）
            if update_count_field_name:
                # 获取当前更新次数
                current_count_value = fields.get(update_count_field_name)
                current_count = 0
                
                if current_count_value is not None:
                    if isinstance(current_count_value, (int, float)):
                        current_count = int(current_count_value)
                    elif isinstance(current_count_value, str):
                        try:
                            current_count = int(current_count_value.strip()) if current_count_value.strip() else 0
                        except ValueError:
                            current_count = 0
                
                # 加1
                new_count = current_count + 1
                
                # 准备更新记录
                update_count_record = {
                    'record_id': record_id,
                    update_count_field_name: new_count
                }
                update_count_records.append(update_count_record)
                
                logger.info(f"SKU {sku} 的更新次数将从 {current_count} 增加到 {new_count}")
    
    # 处理图片链接不一致的记录：写入变更记录表并删除原表记录
    if change_records:
        logger.info(f"发现 {len(change_records)} 条图片链接不一致的记录，正在处理...")
        
        # 创建变更记录表的客户端
        change_feishu_client = FeishuClient(
            app_token=FEISHU_APP_TOKEN,
            table_id=FEISHU_CHANGE_TABLE_ID,
            view_id=FEISHU_CHANGE_VIEW_ID
        )
        await change_feishu_client.get_access_token()
        
        # 写入变更记录表
        logger.info("正在写入图片变更记录到新表...")
        try:
            written = await change_feishu_client.write_records(change_records, batch_size=500)
            logger.info(f"✓ 成功写入 {written} 条变更记录到新表")
        except Exception as e:
            logger.error(f"写入变更记录表失败: {e}")
            raise
        
        # 更新第一个表的"更新次数"字段（加1）
        if update_count_records:
            logger.info(f"正在更新第一个表的'更新次数'字段，共 {len(update_count_records)} 条记录...")
            try:
                updated_count = await feishu_client.update_records(update_count_records, batch_size=500)
                logger.info(f"✓ 成功更新 {updated_count} 条记录的'更新次数'字段")
            except Exception as e:
                logger.error(f"更新'更新次数'字段失败: {e}")
                raise


async def main():
    """主函数"""
    try:
        await update_feishu_images()
        logger.info("商品图片更新任务完成")
    except Exception as e:
        logger.error(f"商品图片更新任务失败: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())

