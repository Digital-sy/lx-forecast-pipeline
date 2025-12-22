#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""封装 Openapi的 http请求"""
import aiohttp
import orjson
from typing import Optional
from .resp_schema import ResponseResult


class HttpBase(object):

    def __init__(self, default_timeout=30, proxy_url: Optional[str] = None):
        self.default_timeout = default_timeout
        self.proxy_url = proxy_url

    async def request(self, method: str, req_url: str,
                      params: Optional[dict] = None,
                      json: Optional[dict] = None,
                      data: Optional[dict] = None,
                      headers: Optional[dict] = None,
                      **kwargs) -> ResponseResult:
        timeout = kwargs.pop('timeout', self.default_timeout)
        
        # 处理请求数据
        request_data = None
        if json:
            # 如果是JSON数据，转换为JSON字符串
            request_data = orjson.dumps(json, option=orjson.OPT_SORT_KEYS)
        elif data:
            # 如果是表单数据，直接使用
            request_data = data
        
        # 配置代理
        request_kwargs = {}
        if self.proxy_url:
            request_kwargs['proxy'] = self.proxy_url
        
        async with aiohttp.ClientSession() as aio_session:
            async with aio_session.request(method=method, url=req_url, params=params, data=request_data,
                                           timeout=timeout, headers=headers, **request_kwargs, **kwargs) as resp:
                if resp.status != 200:
                    raise ValueError(f"Response error, status code: {resp.status}, body: {await resp.text()}")
                resp_json = await resp.json()
                return ResponseResult(**resp_json)
