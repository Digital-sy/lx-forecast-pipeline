#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
统一日志系统
提供彩色控制台输出和文件日志功能
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    import colorlog
    HAS_COLORLOG = True
except ImportError:
    HAS_COLORLOG = False

from .config import settings


class Logger:
    """日志管理器"""
    
    _loggers = {}
    
    @classmethod
    def get_logger(cls, name: str, log_file: Optional[str] = None) -> logging.Logger:
        """
        获取日志记录器
        
        Args:
            name: 日志记录器名称
            log_file: 日志文件名（不含路径），如果为None则使用name作为文件名
            
        Returns:
            logging.Logger: 日志记录器实例
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, settings.LOG_LEVEL))
        logger.propagate = False
        
        # 清除现有的处理器
        logger.handlers.clear()
        
        # 控制台处理器（彩色输出）
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        
        if HAS_COLORLOG:
            console_formatter = colorlog.ColoredFormatter(
                '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
        else:
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # 文件处理器
        if log_file is None:
            log_file = f"{name}.log"
        
        # 按日期组织日志文件
        today = datetime.now().strftime('%Y-%m-%d')
        log_path = settings.LOG_DIR / today / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        cls._loggers[name] = logger
        return logger


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    获取日志记录器（便捷函数）
    
    Args:
        name: 日志记录器名称
        log_file: 日志文件名（可选）
        
    Returns:
        logging.Logger: 日志记录器实例
    """
    return Logger.get_logger(name, log_file)

