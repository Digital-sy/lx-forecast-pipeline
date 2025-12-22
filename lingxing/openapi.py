#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""封装Openapi基础操作"""
import copy
import time
from typing import Optional

from .http_util import HttpBase
from .resp_schema import AccessTokenDto, ResponseResult
from .sign import SignBase


class OpenApiBase(object):

    def __init__(self, host: str, app_id: str, app_secret: str, proxy_url: Optional[str] = None):
        self.host = host
        self.app_id = app_id
        self.app_secret = app_secret
        self.proxy_url = proxy_url
        self.http_client = HttpBase(proxy_url=proxy_url)

    async def generate_access_token(self) -> AccessTokenDto:
        """
        获取 access_token
        """
        path = '/api/auth-server/oauth/access-token'
        req_url = self.host + path
        req_data = {
            "appId": self.app_id,
            "appSecret": self.app_secret,
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        resp_result = await self.http_client.request("POST", req_url, data=req_data, headers=headers)
        if resp_result.code != 200:
            error_msg = f"generate_access_token failed, reason: {resp_result.message}"
            raise ValueError(error_msg)

        assert isinstance(resp_result.data, dict)
        return AccessTokenDto(**resp_result.data)

    async def refresh_token(self, refresh_token: str) -> AccessTokenDto:
        """续约access-token"""
        path = '/api/auth-server/oauth/refresh'
        req_url = self.host + path
        req_data = {
            "appId": self.app_id,
            "refreshToken": refresh_token,
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        resp_result = await self.http_client.request("POST", req_url, data=req_data, headers=headers)
        if resp_result.code != 200:
            error_msg = f"refresh_token failed, reason: {resp_result.message}"
            raise ValueError(error_msg)

        assert isinstance(resp_result.data, dict)
        return AccessTokenDto(**resp_result.data)

    async def request(self, access_token: str, route_name: str, method: str,
                      req_params: Optional[dict] = None,
                      req_body: Optional[dict] = None,
                      **kwargs) -> ResponseResult:
        """
        :param access_token:
        :param route_name: 请求路径
        :param method: GET/POST/PUT,etc
        :param req_params: query参数放这里, 没有则不传
        :param req_body: 请求体参数放这里, 没有则不传
        :param kwargs: timeout 等其他字段可以放这里
        :return:
        """
        req_url = self.host + route_name
        headers = kwargs.pop('headers', {})

        req_params = req_params or {}
        gen_sign_params = copy.deepcopy(req_body) if req_body else {}
        if req_params:
            gen_sign_params.update(req_params)

        sign_params = {
            "app_key": self.app_id,
            "access_token": access_token,
            "timestamp": f'{int(time.time())}',
        }
        gen_sign_params.update(sign_params)
        sign = SignBase.generate_sign(self.app_id, gen_sign_params)
        sign_params["sign"] = sign
        req_params.update(sign_params)

        # 对于带有请求体的, 需要设置默认的Content-Type
        if req_body and 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        return await self.http_client.request(method, req_url, params=req_params,
                                        headers=headers, json=req_body, **kwargs)
