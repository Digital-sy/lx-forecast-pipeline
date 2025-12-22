#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""提供 基于 AES 的基础加密功能"""
# 注意, Windows下使用AES时要安装的是pycryptodome 模块,  Linux/MacOs 下使用AES时要安装的是pycrypto模块
from Crypto.Cipher import AES
import base64
import hashlib

BLOCK_SIZE = 16  # Bytes


def do_pad(text):
    return text + (BLOCK_SIZE - len(text) % BLOCK_SIZE) * \
        chr(BLOCK_SIZE - len(text) % BLOCK_SIZE)


def aes_encrypt(key, data):
    """
    AES的ECB模式加密方法
    :param key: 密钥
    :param data:被加密字符串（明文）
    :return:密文
    """
    key = key.encode('utf-8')
    # 字符串补位
    data = do_pad(data)
    cipher = AES.new(key, AES.MODE_ECB)
    # 加密后得到的是bytes类型的数据，使用Base64进行编码,返回byte字符串
    result = cipher.encrypt(data.encode())
    encode_str = base64.b64encode(result)
    enc_text = encode_str.decode('utf-8')
    return enc_text


def md5_encrypt(text: str):
    md = hashlib.md5()
    md.update(text.encode('utf-8'))
    return md.hexdigest()
