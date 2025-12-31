#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
领星 AccessToken 管理（缓存 + 过期自动刷新）。

背景：长任务运行过程中可能出现 `access token is missing or expire`（常见错误码 2001003）。
做法：所有请求前都通过本模块获取 token；命中缓存不额外请求，失效则刷新并重试。
"""

import asyncio
import time
from typing import Optional, Protocol

from .openapi import OpenApiBase


class _LoggerLike(Protocol):
    def info(self, msg: str, *args, **kwargs): ...
    def warning(self, msg: str, *args, **kwargs): ...


class LingxingTokenProvider:
    def __init__(
        self,
        op_api: OpenApiBase,
        refresh_margin_seconds: int = 60,
        logger: Optional[_LoggerLike] = None,
    ):
        self._op_api = op_api
        self._refresh_margin_seconds = refresh_margin_seconds
        self._logger = logger

        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expire_at: float = 0.0
        self._lock = asyncio.Lock()

    def _log_info(self, msg: str) -> None:
        if self._logger:
            self._logger.info(msg)

    def _log_warning(self, msg: str) -> None:
        if self._logger:
            self._logger.warning(msg)

    def _is_expired(self) -> bool:
        return (not self._access_token) or (time.time() >= (self._expire_at - self._refresh_margin_seconds))

    async def get_token(self, force_refresh: bool = False) -> str:
        async with self._lock:
            if not force_refresh and not self._is_expired():
                return self._access_token  # type: ignore[return-value]

            # refresh_token 只能使用一次；优先 refresh，失败则重新获取
            try:
                if self._refresh_token:
                    self._log_info("🔄 刷新 AccessToken...")
                    dto = await self._op_api.refresh_token(self._refresh_token)
                else:
                    self._log_info("🔑 获取 AccessToken...")
                    dto = await self._op_api.generate_access_token()
            except Exception as e:
                self._log_warning(f"⚠️  刷新 token 失败，改为重新获取: {e}")
                dto = await self._op_api.generate_access_token()

            self._access_token = dto.access_token
            self._refresh_token = dto.refresh_token
            self._expire_at = time.time() + int(dto.expires_in)
            self._log_info(f"✅ Token 就绪，有效期: {dto.expires_in} 秒")
            return self._access_token



