#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购建议报告生成增强版：在飞书「面料预估明细」增加「颜色-领星」。

实现方式：
- 复用原 jobs.feishu.generate_procurement_report 的主流程和计算逻辑；
- 覆盖 write_fabric_detail_to_feishu；
- 兼容恢复基础模块误删的 save_fabric_usage；
- 从 lxpm_product_category_snapshot 按 sku=面料颜色编号 读取 product_name；
- 解析 product_name 中形如 2#黑玛瑙 的领星颜色格式，写入飞书字段「颜色-领星」。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List

from common import get_logger
from common.database import db_cursor
from jobs.feishu import generate_procurement_report as base

logger = get_logger('procurement_report_lx_color')


def parse_lingxing_color(product_name: str) -> str:
    """从领星产品品名解析颜色，例如保留 2#黑玛瑙。"""
    text = (product_name or '').strip()
    if not text:
        return ''

    # 常见格式：2#黑玛瑙、15#浅卡其；保留 数字#颜色。
    match = re.search(r'(\d+\s*#\s*[^#，,;/|]+)', text)
    if match:
        return re.sub(r'\s+', '', match.group(1).strip())

    return ''


def read_lingxing_color_map() -> Dict[str, str]:
    """读取 sku -> 颜色-领星 映射。"""
    color_map: Dict[str, str] = {}
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS cnt
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'lxpm_product_category_snapshot'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("lxpm_product_category_snapshot 不存在，颜色-领星将为空")
                return color_map

            cursor.execute("""
                SELECT sku, product_name
                FROM `lxpm_product_category_snapshot`
                WHERE sku IS NOT NULL AND sku != ''
                  AND product_name IS NOT NULL AND product_name != ''
            """)
            for row in cursor.fetchall():
                sku = (row.get('sku') or '').strip()
                product_name = row.get('product_name') or ''
                color = parse_lingxing_color(product_name)
                if sku and color:
                    color_map[sku] = color

        logger.info(f"颜色-领星映射读取完成：{len(color_map)} 个 SKU")
    except Exception as e:
        logger.warning(f"读取颜色-领星映射失败: {e}", exc_info=True)
    return color_map


def save_fabric_usage(records: List[Dict[str, Any]]) -> None:
    """恢复基础采购模块缺失的面料预计用量汇总表写入。"""
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{base.TABLE_FABRIC_USAGE}` (
                `id`             INT AUTO_INCREMENT PRIMARY KEY,
                `面料`           VARCHAR(500) NOT NULL,
                `SPU数量`        INT          NOT NULL DEFAULT 0,
                `建议下单量合计` INT          NOT NULL DEFAULT 0,
                `单件用量(米)`   DECIMAL(8,3) NOT NULL DEFAULT 0,
                `预计用量(米)`   DECIMAL(12,2) NOT NULL DEFAULT 0,
                `更新时间`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                             ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='定制面料预计用量（产品经理用）';
        """)
        cursor.execute(f"TRUNCATE TABLE `{base.TABLE_FABRIC_USAGE}`")

        if records:
            sql = f"""
                INSERT INTO `{base.TABLE_FABRIC_USAGE}`
                    (`面料`, `SPU数量`, `建议下单量合计`, `单件用量(米)`, `预计用量(米)`)
                VALUES (%s, %s, %s, %s, %s)
            """
            rows = [
                (
                    row['面料'],
                    row['SPU数量'],
                    row['建议下单量合计'],
                    row['单件用量(米)'],
                    row['预计用量(米)'],
                )
                for row in records
            ]
            cursor.executemany(sql, rows)

    logger.info(f"✓ 写入 {len(records)} 条记录到 `{base.TABLE_FABRIC_USAGE}`")


async def write_fabric_detail_to_feishu(current_date: datetime) -> None:
    """写面料预估明细到飞书多维表，增加「颜色-领星」字段。"""
    def _month_label(delta: int) -> str:
        y, m = current_date.year, current_date.month
        m += delta
        while m > 12:
            m -= 12
            y += 1
        return f"{m}月"

    m0 = _month_label(0)
    m1 = _month_label(1)
    m2 = _month_label(2)

    field_list = [
        {'name': '统计类型',                  'type': 'text'},
        {'name': '面料',                      'type': 'text'},
        {'name': '面料编号',                  'type': 'text'},
        {'name': '颜色缩写',                  'type': 'text'},
        {'name': '颜色',                      'type': 'text'},
        {'name': '面料颜色编号',              'type': 'text'},
        {'name': '颜色-领星',                 'type': 'text'},
        {'name': '库存量/条',                 'type': 'number', 'precision': 2},
        {'name': '库存量/米',                 'type': 'number', 'precision': 2},
        {'name': '待到货量/条',               'type': 'number', 'precision': 2},
        {'name': '待到货量/米',               'type': 'number', 'precision': 2},
        {'name': f'{m0}已下单消耗/米',        'type': 'number', 'precision': 2},
        {'name': f'{m0}完整预估/米',          'type': 'number', 'precision': 2},
        {'name': f'{m0}剩余预估/米',          'type': 'number', 'precision': 2},
        {'name': f'{m1}预估/米',              'type': 'number', 'precision': 2},
        {'name': f'{m2}预估/米',              'type': 'number', 'precision': 2},
        {'name': f'运营{m0}预估/米',          'type': 'number', 'precision': 2},
        {'name': f'运营{m1}预估/米',          'type': 'number', 'precision': 2},
        {'name': f'运营{m2}预估/米',          'type': 'number', 'precision': 2},
        {'name': '用量信息缺失SPU',           'type': 'text'},
    ]
    client = await base._get_or_create_table(base.FEISHU_APP_TOKEN, '面料预估明细', field_list, remove_extra=True)
    lx_color_map = read_lingxing_color_map()

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT
                统计类型, 面料, 面料编号, 颜色缩写, 颜色, 面料颜色编号,
                `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
                `T+1月预估/米`, `T+2月预估/米`,
                `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`,
                用量信息缺失SPU
            FROM `面料预估表`
            ORDER BY 统计类型, 面料, 颜色缩写
        """)
        rows = cursor.fetchall()

    logger.info(f"面料预估明细：共 {len(rows)} 条")

    matched = 0
    feishu_records = []
    for r in rows:
        fabric_color_code = (r['面料颜色编号'] or '').strip()
        lx_color = lx_color_map.get(fabric_color_code, '')
        if lx_color:
            matched += 1
        feishu_records.append({
            '统计类型':               r['统计类型'] or '',
            '面料':                   r['面料'] or '',
            '面料编号':               r['面料编号'] or '',
            '颜色缩写':               r['颜色缩写'] or '',
            '颜色':                   r['颜色'] or '',
            '面料颜色编号':           fabric_color_code,
            '颜色-领星':              lx_color,
            '库存量/条':              float(r['库存量/条'] or 0),
            '库存量/米':              float(r['库存量/米'] or 0),
            '待到货量/条':            float(r['待到货量/条'] or 0),
            '待到货量/米':            float(r['待到货量/米'] or 0),
            f'{m0}已下单消耗/米':     float(r['当月已下单消耗/米'] or 0),
            f'{m0}完整预估/米':       float(r['当月完整预估/米'] or 0),
            f'{m0}剩余预估/米':       float(r['当月剩余预估/米'] or 0),
            f'{m1}预估/米':           float(r['T+1月预估/米'] or 0),
            f'{m2}预估/米':           float(r['T+2月预估/米'] or 0),
            f'运营{m0}预估/米':       float(r['运营当月预估/米'] or 0),
            f'运营{m1}预估/米':       float(r['运营T+1月预估/米'] or 0),
            f'运营{m2}预估/米':       float(r['运营T+2月预估/米'] or 0),
            '用量信息缺失SPU':        r['用量信息缺失SPU'] or '',
        })

    logger.info(f"颜色-领星匹配：{matched}/{len(rows)} 条")
    await client.delete_all_records()
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书面料预估明细写入 {written} 条（含颜色-领星）")


def main() -> None:
    # 基础模块在款色维度改造时误删 save_fabric_usage；在增强入口恢复该写入能力。
    base.save_fabric_usage = save_fabric_usage
    base.write_fabric_detail_to_feishu = write_fabric_detail_to_feishu
    base.main()


if __name__ == '__main__':
    main()
