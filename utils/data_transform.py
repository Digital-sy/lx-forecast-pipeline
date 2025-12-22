#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
数据转换工具函数
"""
from typing import Dict, List, Any
from .date_utils import convert_timestamp_to_datetime


def normalize_shop_name(shop_name: str) -> str:
    """
    规范化店铺名称
    
    规则：
    - 所有RR-开头的店铺统一转换为RR-EU
    - 其他店铺保持原样
    
    Args:
        shop_name: 店铺名称或店铺ID
        
    Returns:
        str: 规范化后的店铺名称
    """
    if not shop_name:
        return shop_name
    
    # 所有RR-开头的店铺统一转换为RR-EU
    if str(shop_name).startswith('RR-'):
        return 'RR-EU'
    
    return shop_name


def convert_feishu_record_to_dict(records: List[Dict[str, Any]], 
                                   field_map: Dict[str, str],
                                   time_fields: List[str] = None) -> List[Dict[str, Any]]:
    """
    将飞书多维表格记录转换为字典列表
    
    Args:
        records: 飞书记录列表
        field_map: 字段ID到字段名称的映射
        time_fields: 需要转换时间戳的字段名列表
        
    Returns:
        List[Dict[str, Any]]: 转换后的字典列表
    """
    if time_fields is None:
        time_fields = ['下单时间', '创建时间', '更新时间', '订单时间', '修改时间']
    
    data_list = []
    
    for record in records:
        fields_data = record.get("fields", {})
        record_dict = {}
        
        # 将字段ID转换为字段名
        for field_id, value in fields_data.items():
            field_name = field_map.get(field_id, field_id)
            
            # 处理不同类型的值
            if isinstance(value, list):
                # 数组类型
                if value:
                    if isinstance(value[0], dict):
                        # 检查是否是包含text字段的字典数组（如所属部门）
                        if 'text' in value[0]:
                            texts = [item.get('text', '') for item in value]
                            record_dict[field_name] = ', '.join(filter(None, texts))
                        else:
                            # 普通对象数组，提取name字段
                            record_dict[field_name] = ','.join([str(item.get('name', item)) for item in value])
                    else:
                        record_dict[field_name] = ','.join([str(v) for v in value])
                else:
                    record_dict[field_name] = ''
                    
            elif isinstance(value, dict):
                # 对象类型
                if 'text' in value:
                    record_dict[field_name] = value.get('text', '')
                elif 'users' in value:
                    # 人员字段
                    users = value.get('users', [])
                    if users:
                        names = [user.get('name', user.get('enName', '')) for user in users]
                        record_dict[field_name] = ', '.join(filter(None, names))
                    else:
                        record_dict[field_name] = ''
                elif 'name' in value:
                    record_dict[field_name] = value.get('name', '')
                else:
                    record_dict[field_name] = str(value)
                    
            elif isinstance(value, (int, float)) and field_name in time_fields:
                # 时间字段：转换时间戳为日期时间格式
                record_dict[field_name] = convert_timestamp_to_datetime(value)
            else:
                # 其他类型
                str_value = str(value) if value is not None else ''
                
                # 如果是字符串形式的字典
                if str_value.startswith('{') and str_value.endswith('}'):
                    try:
                        import ast
                        parsed_value = ast.literal_eval(str_value)
                        if isinstance(parsed_value, dict):
                            if 'text' in parsed_value:
                                record_dict[field_name] = parsed_value.get('text', '')
                            elif 'name' in parsed_value:
                                record_dict[field_name] = parsed_value.get('name', '')
                            else:
                                record_dict[field_name] = str_value
                        else:
                            record_dict[field_name] = str_value
                    except:
                        record_dict[field_name] = str_value
                else:
                    record_dict[field_name] = str_value
        
        # 确保店铺字段存在
        if '店铺' not in record_dict and '店铺名' not in record_dict:
            for key in record_dict.keys():
                if '店铺' in key or 'store' in key.lower():
                    record_dict['店铺'] = record_dict.get(key, '')
                    break
        
        data_list.append(record_dict)
    
    return data_list

