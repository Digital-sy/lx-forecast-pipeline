#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
从飞书「定制面料库存表」拉取手工台账数据，同步到 MySQL `面料库存台账` 表。

飞书多维表字段映射：
  面料编号-颜色缩写  →  匹配键（与 generate_fabric_forecast.py 对齐）
  面料名            →  面料
  颜色缩写          →  颜色缩写
  库存成品数量（条）→  库存成品数量_条（颜色维度库存）
  现有胚布数量（条）→  现有胚布数量_条（面料整体库存的胚布部分）
  备货中数量（条）  →  备货中数量_条（待到货）

generate_fabric_forecast.py 取数逻辑：
  颜色维度库存量/条  = 库存成品数量_条
  总量维度库存量/条  = 库存成品数量_条 + 现有胚布数量_条（按面料编号聚合）
  待到货量/条        = 备货中数量_条
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.feishu import FeishuClient
from common.database import db_cursor

logger = get_logger('fabric_inventory_feishu')

# ── 飞书多维表配置 ────────────────────────────────────────────────────────
FEISHU_APP_TOKEN = "XT6pbXxmdas4rdsme0XctyefnGu"
FEISHU_TABLE_ID  = "tblMxScMbTyLQbyj"
FEISHU_VIEW_ID   = None   # 不限定视图，拉全量

# ── MySQL 目标表 ──────────────────────────────────────────────────────────
TARGET_TABLE = "面料库存台账"


# ────────────────────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────────────────────

def _text(value: Any) -> str:
    """从飞书字段值中提取文本。"""
    if value is None:
        return ''
    if isinstance(value, list):
        if not value:
            return ''
        first = value[0]
        if isinstance(first, dict):
            return str(first.get('text', '') or '').strip()
        return str(first).strip()
    if isinstance(value, dict):
        return str(value.get('text', '') or '').strip()
    return str(value).strip()


def _num(value: Any) -> float:
    """从飞书字段值中提取数值，提取失败返回 0.0。"""
    if value is None:
        return 0.0
    if isinstance(value, list):
        if not value:
            return 0.0
        first = value[0]
        if isinstance(first, dict):
            raw = first.get('text', '0') or '0'
        else:
            raw = first
    elif isinstance(value, dict):
        raw = value.get('value', value.get('text', '0')) or '0'
    else:
        raw = value
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


# ────────────────────────────────────────────────────────────────────────────
# 建表
# ────────────────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    """创建 `面料库存台账` 表（如不存在）。"""
    logger.info(f"检查/创建 `{TARGET_TABLE}` 表...")
    with db_cursor(dictionary=False) as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}` (
                `id`                  INT AUTO_INCREMENT PRIMARY KEY,
                `面料编号颜色缩写`    VARCHAR(200) NOT NULL COMMENT '匹配键，格式：面料编号-颜色缩写',
                `面料`                VARCHAR(200) DEFAULT '' COMMENT '面料名',
                `颜色缩写`            VARCHAR(50)  DEFAULT '' COMMENT '颜色缩写',
                `库存成品数量_条`     DOUBLE       DEFAULT 0  COMMENT '颜色维度库存，对应飞书「库存成品数量（条）」',
                `现有胚布数量_条`     DOUBLE       DEFAULT 0  COMMENT '胚布库存，对应飞书「现有胚布数量（条）」',
                `备货中数量_条`       DOUBLE       DEFAULT 0  COMMENT '待到货，对应飞书「备货中数量（条）」',
                `飞书更新日期`        VARCHAR(50)  DEFAULT '' COMMENT '飞书台账的更新日期字段',
                `同步时间`            DATETIME     DEFAULT CURRENT_TIMESTAMP
                                                   ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_key` (`面料编号颜色缩写`(150))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='飞书定制面料库存台账（手工维护）'
        """)
    logger.info(f"✓ `{TARGET_TABLE}` 已就绪")


# ────────────────────────────────────────────────────────────────────────────
# 解析飞书记录
# ────────────────────────────────────────────────────────────────────────────

def parse_records(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    将飞书原始记录解析为入库格式。
    只保留「面料编号-颜色缩写」不为空的有效行。
    """
    result = []
    skip_count = 0

    for rec in raw_records:
        fields = rec.get('fields', {})

        match_key  = _text(fields.get('面料编号-颜色缩写', ''))
        fabric     = _text(fields.get('面料名', ''))
        color_abbr = _text(fields.get('颜色缩写', ''))
        stock_fg   = _num(fields.get('库存成品数量（条）', 0))
        stock_grey = _num(fields.get('现有胚布数量（条）', 0))
        pending    = _num(fields.get('备货中数量（条）', 0))
        upd_date   = _text(fields.get('更新日期', ''))

        if not match_key:
            skip_count += 1
            continue

        result.append({
            '面料编号颜色缩写': match_key,
            '面料':            fabric,
            '颜色缩写':        color_abbr,
            '库存成品数量_条': stock_fg,
            '现有胚布数量_条': stock_grey,
            '备货中数量_条':   pending,
            '飞书更新日期':    upd_date,
        })

    if skip_count:
        logger.warning(f"  跳过 {skip_count} 条「面料编号-颜色缩写」为空的记录")
    return result


# ────────────────────────────────────────────────────────────────────────────
# 写库
# ────────────────────────────────────────────────────────────────────────────

def save_to_mysql(records: List[Dict[str, Any]]) -> int:
    """全量覆盖写入：先清空，再批量 INSERT。"""
    if not records:
        logger.warning("没有有效记录，跳过写库")
        return 0

    with db_cursor(dictionary=False) as cur:
        cur.execute(f"TRUNCATE TABLE `{TARGET_TABLE}`")
        sql = f"""
            INSERT INTO `{TARGET_TABLE}`
                (`面料编号颜色缩写`, `面料`, `颜色缩写`,
                 `库存成品数量_条`, `现有胚布数量_条`, `备货中数量_条`,
                 `飞书更新日期`)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        batch_size = 500
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            rows = [(
                r['面料编号颜色缩写'], r['面料'], r['颜色缩写'],
                r['库存成品数量_条'], r['现有胚布数量_条'], r['备货中数量_条'],
                r['飞书更新日期'],
            ) for r in batch]
            cur.executemany(sql, rows)
            total += len(batch)
            logger.info(f"  已写入 {total}/{len(records)} 条")

    logger.info(f"✓ 成功写入 {len(records)} 条到 `{TARGET_TABLE}`")
    return len(records)


# ────────────────────────────────────────────────────────────────────────────
# 主流程
# ────────────────────────────────────────────────────────────────────────────

async def fetch_and_sync() -> None:
    logger.info("=" * 70)
    logger.info("面料库存台账同步任务（飞书 → MySQL）")
    logger.info("=" * 70)

    # 1. 建表
    ensure_table()

    # 2. 连飞书
    logger.info("初始化飞书客户端...")
    client = FeishuClient(
        app_token=FEISHU_APP_TOKEN,
        table_id=FEISHU_TABLE_ID,
        view_id=FEISHU_VIEW_ID,
    )
    await client.get_access_token()

    # 3. 读字段（用于调试首次对接）
    field_map = await client.get_table_fields()
    logger.info(f"  表字段列表: {list(field_map.values())}")

    # 4. 拉全量记录
    logger.info("读取飞书台账记录...")
    raw_records = await client.read_records(page_size=500)
    logger.info(f"  拉取到 {len(raw_records)} 条原始记录")

    if not raw_records:
        logger.warning("飞书台账为空，终止同步")
        return

    # 5. 解析
    records = parse_records(raw_records)
    logger.info(f"  解析出 {len(records)} 条有效记录")

    # 打印前3条样例便于核对
    for i, r in enumerate(records[:3]):
        logger.info(f"  样例[{i+1}]: {r}")

    # 6. 写库
    save_to_mysql(records)

    logger.info("=" * 70)
    logger.info("同步完成")
    logger.info("=" * 70)


def main() -> None:
    try:
        asyncio.run(fetch_and_sync())
    except KeyboardInterrupt:
        logger.warning("用户中断")
    except Exception as e:
        logger.error(f"同步失败: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
