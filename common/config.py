#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
统一配置管理模块
使用环境变量和.env文件管理配置
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# 加载.env文件
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / '.env'

if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    print(f"警告: .env文件不存在于 {ENV_FILE}")
    print("请复制 config.example.env 为 .env 并配置相关参数")


class Settings:
    """配置类"""
    
    def __init__(self):
        # ===== 基础配置 =====
        self.BASE_DIR = BASE_DIR
        self.ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_DIR = BASE_DIR / os.getenv('LOG_DIR', 'logs')
        
        # ===== 灵星API配置 =====
        self.LINGXING_HOST = os.getenv('LINGXING_HOST', 'https://openapi.lingxing.com')
        self.LINGXING_APP_ID = os.getenv('LINGXING_APP_ID', '')
        self.LINGXING_APP_SECRET = os.getenv('LINGXING_APP_SECRET', '')
        self.LINGXING_PROXY_URL = os.getenv('LINGXING_PROXY_URL', '')
        
        # ===== 数据库配置 =====
        self.DB_HOST = os.getenv('DB_HOST', 'localhost')
        self.DB_PORT = int(os.getenv('DB_PORT', '3306'))
        self.DB_USER = os.getenv('DB_USER', '')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD', '')
        self.DB_DATABASE = os.getenv('DB_DATABASE', 'lingxing')
        self.DB_CHARSET = os.getenv('DB_CHARSET', 'utf8mb4')
        # 新增：数据库超时配置（秒）
        self.DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', '10'))
        self.DB_READ_TIMEOUT = int(os.getenv('DB_READ_TIMEOUT', '600'))  # 10分钟
        self.DB_WRITE_TIMEOUT = int(os.getenv('DB_WRITE_TIMEOUT', '600'))  # 10分钟
        
        # ===== 飞书API配置（全局认证信息）=====
        # 注意：app_token, table_id, view_id 等表级配置应在各业务脚本中单独指定
        self.FEISHU_APP_ID = os.getenv('FEISHU_APP_ID', '')
        self.FEISHU_APP_SECRET = os.getenv('FEISHU_APP_SECRET', '')
        self.FEISHU_API_BASE = 'https://open.feishu.cn/open-apis'
        
        # ===== 数据采集配置 =====
        self.COLLECTION_START_DATE = os.getenv('COLLECTION_START_DATE', '2024-01-01')
        self.COLLECTION_PAGE_SIZE = int(os.getenv('COLLECTION_PAGE_SIZE', '200'))
        self.COLLECTION_DELAY_SECONDS = float(os.getenv('COLLECTION_DELAY_SECONDS', '1'))
        
        # 确保日志目录存在
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    @property
    def db_config(self) -> dict:
        """获取数据库配置字典"""
        return {
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'database': self.DB_DATABASE,
            'charset': self.DB_CHARSET,
            # 添加超时配置
            'connect_timeout': self.DB_CONNECT_TIMEOUT,
            'read_timeout': self.DB_READ_TIMEOUT,
            'write_timeout': self.DB_WRITE_TIMEOUT,
        }
    
    @property
    def lingxing_config(self) -> dict:
        """获取灵星API配置字典"""
        return {
            'host': self.LINGXING_HOST,
            'app_id': self.LINGXING_APP_ID,
            'app_secret': self.LINGXING_APP_SECRET,
            'proxy_url': self.LINGXING_PROXY_URL,
        }
    
    @property
    def feishu_config(self) -> dict:
        """获取飞书API配置字典（仅包含全局认证信息）"""
        return {
            'app_id': self.FEISHU_APP_ID,
            'app_secret': self.FEISHU_APP_SECRET,
            'api_base': self.FEISHU_API_BASE,
        }
    
    def validate(self) -> bool:
        """验证必要的配置是否已设置"""
        errors = []
        
        if not self.LINGXING_APP_ID:
            errors.append("LINGXING_APP_ID 未配置")
        if not self.LINGXING_APP_SECRET:
            errors.append("LINGXING_APP_SECRET 未配置")
        if not self.DB_HOST:
            errors.append("DB_HOST 未配置")
        if not self.DB_USER:
            errors.append("DB_USER 未配置")
        if not self.DB_PASSWORD:
            errors.append("DB_PASSWORD 未配置")
        
        if errors:
            print("配置错误:")
            for error in errors:
                print(f"  - {error}")
            return False
        
        return True


# 创建全局配置实例
settings = Settings()
