#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
从飞书核价多维表（面料信息汇总）拉取 SPU 季节数据，同步到 MySQL `SPU季节表`。

飞书配置：
  app_token : PItubmXkWarkqosFanGcxKJGnvT
  table_id  : tblTYD4cZsxbuQIs
  view_id   : vewjVvzbxK
  字段      : 款号、季节-辅助

季节映射规则：
  "XX-春夏" / "历史-春夏" → 春夏
  "XX-秋冬" / "历史-秋冬" → 秋冬
  其他（空、优化等）       → 全年

被 forecast_sales_improved.py 的季节性预测逻辑读取。
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List, Any

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.feishu import FeishuClient
from common.database import db_cursor

logger = get_logger('fetch_spu_season')

FEISHU_APP_TOKEN = "PItubmXkWarkqosFanGcxKJGnvT"
FEISHU_TABLE_ID  = "tblTYD4cZsxbuQIs"
FEISHU_VIEW_ID   = "vewjVvzbxK"
TARGET_TABLE     = "SPU季节表"


# ── 建表 ─────────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    logger.info(f"检查/创建 `{TARGET_TABLE}` 表...")
    with db_cursor(dictionary=False) as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}` (
                `id`       INT AUTO_INCREMENT PRIMARY KEY,
                `SPU`      VARCHAR(200) NOT NULL COMMENT '款号',
                `季节`     VARCHAR(20)  NOT NULL DEFAULT '全年'
                           COMMENT '春夏 / 秋冬 / 全年',
                `原始季节` VARCHAR(100) DEFAULT '' COMMENT '飞书原始季节-辅助值',
                `同步时间` DATETIME DEFAULT CURRENT_TIMESTAMP
                           ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_spu` (`SPU`(100))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SPU季节标签（来自飞书核价表）'
        """)
    logger.info(f"✓ `{TARGET_TABLE}` 已就绪")


# ── 季节提取 ─────────────────────────────────────────────────────────────────

def _extract_text(value: Any) -> str:
    """从飞书字段值提取纯文本。"""
    if value is None:
        return ''
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get('text', '') or ''))
            else:
                parts.append(str(item))
        return ''.join(parts).strip()
    if isinstance(value, dict):
        return str(value.get('text', '') or '').strip()
    return str(value).strip()


def _map_season(raw: str) -> str:
    """
    把飞书「季节-辅助」映射为 春夏 / 秋冬 / 全年。
    规则：取最后一个"-"之后的部分判断。
    """
    if not raw:
        return '全年'
    suffix = raw.rsplit('-', 1)[-1].strip()
    if suffix == '春夏':
        return '春夏'
    if suffix == '秋冬':
        return '秋冬'
    return '全年'


# ── 解析记录 ──────────────────────────────────────────────────────────────────

def parse_records(raw_records: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    解析飞书记录，返回 {SPU: {季节, 原始季节}}。
    同一 SPU 若出现多次，保留最后一条（飞书里每行是一个款号）。
    """
    result: Dict[str, Dict[str, str]] = {}
    skip = 0
    for rec in raw_records:
        fields  = rec.get('fields', {})
        spu_raw = _extract_text(fields.get('款号', ''))
        raw_s   = _extract_text(fields.get('季节-辅助', ''))

        spu = spu_raw.strip()
        if not spu:
            skip += 1
            continue

        result[spu] = {
            '季节':     _map_season(raw_s),
            '原始季节': raw_s,
        }

    if skip:
        logger.warning(f"  跳过 {skip} 条款号为空的记录")
    return result


# ── 写库 ─────────────────────────────────────────────────────────────────────

def save_to_mysql(spu_map: Dict[str, Dict[str, str]]) -> int:
    if not spu_map:
        logger.warning("无有效记录，跳过写库")
        return 0

    rows = [
        (spu, info['季节'], info['原始季节'])
        for spu, info in spu_map.items()
    ]

    with db_cursor(dictionary=False) as cur:
        cur.execute(f"TRUNCATE TABLE `{TARGET_TABLE}`")
        sql = f"""
            INSERT INTO `{TARGET_TABLE}` (`SPU`, `季节`, `原始季节`)
            VALUES (%s, %s, %s)
        """
        batch_size = 500
        total = 0
        for i in range(0, len(rows), batch_size):
            cur.executemany(sql, rows[i:i + batch_size])
            total += len(rows[i:i + batch_size])
            logger.info(f"  已写入 {total}/{len(rows)} 条")

    logger.info(f"✓ 成功写入 {len(rows)} 条到 `{TARGET_TABLE}`")

    # 打印季节分布
    with db_cursor(dictionary=True) as cur:
        cur.execute(f"""
            SELECT 季节, COUNT(*) AS 数量
            FROM `{TARGET_TABLE}`
            GROUP BY 季节 ORDER BY 数量 DESC
        """)
        for r in cur.fetchall():
            logger.info(f"  {r['季节']}: {r['数量']} 个SPU")

    return len(rows)


# ── 主流程 ────────────────────────────────────────────────────────────────────

async def fetch_and_sync() -> None:
    logger.info("=" * 70)
    logger.info("SPU季节数据同步任务（飞书核价表 → MySQL）")
    logger.info("=" * 70)

    ensure_table()

    logger.info("初始化飞书客户端...")
    client = FeishuClient(
        app_token=FEISHU_APP_TOKEN,
        table_id=FEISHU_TABLE_ID,
        view_id=FEISHU_VIEW_ID,
    )
    await client.get_access_token()

    logger.info("读取飞书记录（款号 + 季节-辅助）...")
    raw_records = await client.read_records(page_size=500)
    logger.info(f"  拉取到 {len(raw_records)} 条原始记录")

    if not raw_records:
        logger.warning("飞书表为空，终止")
        return

    spu_map = parse_records(raw_records)
    logger.info(f"  解析出 {len(spu_map)} 个有效SPU")

    # 样例
    for i, (spu, info) in enumerate(list(spu_map.items())[:3]):
        logger.info(f"  样例[{i+1}]: SPU={spu} 季节={info['季节']} 原始={info['原始季节']}")

    save_to_mysql(spu_map)
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
