#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
通用飞书任务通知脚本
用法: python3 notify_feishu.py --task "任务名" --status "success/failed" --detail "详情"
"""
import argparse
import requests
import json
import sys
import os

sys.path.insert(0, '/opt/apps/pythondata')
from common.config import settings

FEISHU_USER_ID = 'ou_45d24eddffa044503caf29d6c8a2e003'  # 刘宗霖

def get_token():
    resp = requests.post(
        'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
        json={'app_id': settings.FEISHU_APP_ID, 'app_secret': settings.FEISHU_APP_SECRET},
        timeout=10
    ).json()
    return resp.get('tenant_access_token')

def send_notify(task_name: str, status: str, detail: str, elapsed: str = ''):
    token = get_token()
    if not token:
        print("获取token失败")
        return

    is_success = status == 'success'
    title = f"{'✅' if is_success else '❌'} {task_name} {'完成' if is_success else '失败'}"
    
    content = detail
    if elapsed:
        content += f"\n**耗时：** {elapsed}"

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": "green" if is_success else "red"
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}}
        ]
    }

    resp = requests.post(
        'https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        json={
            'receive_id': FEISHU_USER_ID,
            'msg_type': 'interactive',
            'content': json.dumps(card, ensure_ascii=False)
        },
        timeout=15
    ).json()

    if resp.get('code') == 0:
        print(f"✅ 飞书通知已发送: {title}")
    else:
        print(f"❌ 飞书通知发送失败: {resp.get('msg')}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', required=True, help='任务名称')
    parser.add_argument('--status', required=True, choices=['success', 'failed'], help='任务状态')
    parser.add_argument('--detail', default='', help='任务详情')
    parser.add_argument('--elapsed', default='', help='耗时')
    args = parser.parse_args()
    send_notify(args.task, args.status, args.detail, args.elapsed)
