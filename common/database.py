#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
数据库连接管理模块
提供统一的数据库连接和连接池管理
"""
import pymysql
from typing import Optional
from contextlib import contextmanager

from .config import settings
from .logger import get_logger

logger = get_logger('database')


class DatabasePool:
    """简单的数据库连接池"""
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._pool = []
        self._in_use = set()
    
    def get_connection(self):
        """从连接池获取连接"""
        # 尝试从池中获取连接
        while self._pool:
            conn = self._pool.pop()
            try:
                # 检查连接是否有效
                conn.ping(reconnect=True)
                self._in_use.add(conn)
                return conn
            except:
                # 连接无效，关闭并继续
                try:
                    conn.close()
                except:
                    pass
        
        # 池中没有可用连接，创建新连接
        if len(self._in_use) < self.max_connections:
            conn = self._create_connection()
            self._in_use.add(conn)
            return conn
        
        raise Exception("数据库连接池已满，无法创建新连接")
    
    def return_connection(self, conn):
        """归还连接到池中"""
        if conn in self._in_use:
            self._in_use.remove(conn)
        
        try:
            # 检查连接是否有效
            conn.ping(reconnect=True)
            self._pool.append(conn)
        except:
            # 连接无效，关闭它
            try:
                conn.close()
            except:
                pass
    
    def _create_connection(self):
        """创建新的数据库连接"""
        try:
            conn = pymysql.connect(**settings.db_config)
            logger.debug(f"创建新的数据库连接: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_DATABASE}")
            return conn
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")
            raise
    
    def close_all(self):
        """关闭所有连接"""
        # 关闭池中的连接
        while self._pool:
            conn = self._pool.pop()
            try:
                conn.close()
            except:
                pass
        
        # 关闭使用中的连接
        for conn in list(self._in_use):
            try:
                conn.close()
            except:
                pass
        self._in_use.clear()
        
        logger.info("所有数据库连接已关闭")


# 全局连接池实例
_db_pool: Optional[DatabasePool] = None


def get_db_pool(max_connections: int = 10) -> DatabasePool:
    """
    获取数据库连接池实例（单例）
    
    Args:
        max_connections: 最大连接数
        
    Returns:
        DatabasePool: 连接池实例
    """
    global _db_pool
    if _db_pool is None:
        _db_pool = DatabasePool(max_connections)
        logger.info(f"数据库连接池已初始化，最大连接数: {max_connections}")
    return _db_pool


def get_db_connection():
    """
    获取数据库连接（简单模式，每次创建新连接）
    
    Returns:
        pymysql.Connection: 数据库连接
    """
    try:
        conn = pymysql.connect(**settings.db_config)
        logger.debug(f"连接到数据库: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_DATABASE}")
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        raise


@contextmanager
def db_connection():
    """
    数据库连接上下文管理器
    自动管理连接的获取和释放
    
    Usage:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"数据库操作失败: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("数据库连接已关闭")


@contextmanager
def db_cursor(dictionary=True):
    """
    数据库游标上下文管理器
    自动管理连接和游标的获取、提交和释放
    
    Args:
        dictionary: 是否返回字典格式的结果（默认True）
        
    Usage:
        with db_cursor() as cursor:
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if dictionary:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
        else:
            cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"数据库操作失败: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.debug("数据库连接已关闭")

