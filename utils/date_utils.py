#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
日期处理工具函数
"""
from datetime import datetime
from typing import List, Optional


def parse_month(date_str) -> Optional[str]:
    """
    解析日期字符串，返回月份格式 YYYY-MM
    
    Args:
        date_str: 日期字符串
        
    Returns:
        str: 月份字符串，格式为 YYYY-MM，解析失败返回None
    """
    if not date_str:
        return None
    try:
        if isinstance(date_str, str):
            # 格式：2025-12-20 12:29:53 或 2025-12-20
            date_obj = datetime.strptime(date_str.split()[0], '%Y-%m-%d')
            return date_obj.strftime('%Y-%m')
        return None
    except:
        return None


def get_valid_months(months_before: int = 2, months_after: int = 3) -> List[str]:
    """
    获取有效的月份范围
    
    Args:
        months_before: 当前月之前的月份数量（默认2）
        months_after: 当前月之后的月份数量（默认3）
        
    Returns:
        List[str]: 月份列表，格式为 ['YYYY-MM', ...]
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    valid_months = []
    
    # 计算月份范围
    for i in range(-months_before, months_after + 1):
        target_month = current_month + i
        target_year = current_year
        
        # 处理跨年情况
        while target_month > 12:
            target_month -= 12
            target_year += 1
        while target_month < 1:
            target_month += 12
            target_year -= 1
        
        month_str = f"{target_year:04d}-{target_month:02d}"
        valid_months.append(month_str)
    
    return valid_months


def convert_timestamp_to_datetime(timestamp) -> str:
    """
    将时间戳转换为可读的日期时间格式
    
    Args:
        timestamp: 时间戳（秒或毫秒）
        
    Returns:
        str: 格式化的日期时间字符串，格式为 YYYY-MM-DD HH:MM:SS
    """
    if not timestamp:
        return ''
    try:
        if isinstance(timestamp, (int, float)):
            # 如果是毫秒级时间戳，转换为秒
            if timestamp > 10000000000:
                timestamp = timestamp / 1000
            dt = datetime.fromtimestamp(timestamp)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return str(timestamp)
    except:
        return str(timestamp)

