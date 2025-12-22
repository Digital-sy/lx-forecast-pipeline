#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""基于 aes文件 基础加密功能 封装 openapi签名算法"""
import orjson
from typing import Union
from .aes import aes_encrypt, md5_encrypt


class SignBase(object):

    @classmethod
    def generate_sign(cls, encrypt_key: str, request_params: dict) -> str:
        """
        生成签名
        """
        canonical_querystring = cls.format_params(request_params)
        md5_str = md5_encrypt(canonical_querystring).upper()
        sign = aes_encrypt(encrypt_key, md5_str)
        return sign

    @classmethod
    def format_params(cls, request_params: Union[None, dict] = None) -> str:
        """
        格式化 params
        """
        if not request_params or not isinstance(request_params, dict):
            return ''

        canonical_strs = []
        sort_keys = sorted(request_params.keys())
        for k in sort_keys:
            v = request_params[k]
            if v == "":
                continue
            elif isinstance(v, (dict, list)):
                # 如果直接使用 json, 则必须使用separators=(',',':'), 去除序列化后的空格, 否则 json中带空格就导致签名异常
                # 使用 option=orjson.OPT_SORT_KEYS 保证dict进行有序 序列化(因为最终要转换为 str进行签名计算, 需要保证有序)
                canonical_strs.append(f"{k}={orjson.dumps(v, option=orjson.OPT_SORT_KEYS).decode()}")
            else:
                canonical_strs.append(f"{k}={v}")
        return "&".join(canonical_strs)
