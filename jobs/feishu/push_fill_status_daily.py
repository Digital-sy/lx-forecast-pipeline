#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
每日推送「填报情况 · 数据」卡片
每天 9:00 发送给指定人员。

只发送第二张卡片，不发送：
1. 采购建议 · 生产
2. 定制面料用量 · 面料
"""

import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from jobs.feishu.push_procurement_summary import (
    make_month_labels,
    read_fill_status_rows,
    build_fill_status_card,
    send_card,
)

logger = get_logger("push_fill_status_daily")


RECEIVERS = [
    "ou_45d24eddffa044503caf29d6c8a2e003",
    "ou_f3f8c4969ddcb90d873520ffbe575b90",
]


def main():
    logger.info("=" * 60)
    logger.info("每日填报情况卡片推送")
    logger.info("=" * 60)

    current_date = datetime.now()
    month_labels = make_month_labels(current_date, 4)
    fill_rows = read_fill_status_rows(month_labels)
    fill_card = build_fill_status_card(current_date, fill_rows)

    success_count = 0
    fail_count = 0

    for receiver in RECEIVERS:
        ok = send_card(receiver, fill_card)
        if ok:
            success_count += 1
        else:
            fail_count += 1

    logger.info(f"推送完成：成功 {success_count} 人，失败 {fail_count} 人")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
