#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""读取颜色体系级和旧版面料颜色归并规则。"""
from __future__ import annotations

from typing import Dict, Tuple

from common.database import db_cursor


def load_fabric_merge_maps() -> Tuple[
    Dict[Tuple[str, str, str], str],
    Dict[Tuple[str, str], str],
]:
    """体系级规则优先；只有颜色体系为空的记录才进入旧版兜底。"""
    system_map: Dict[Tuple[str, str, str], str] = {}
    legacy_map: Dict[Tuple[str, str], str] = {}

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料颜色归并对照'
        """)
        if not cursor.fetchone().get("cnt", 0):
            return system_map, legacy_map

        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料颜色归并对照'
              AND COLUMN_NAME='颜色体系'
        """)
        has_system = bool(cursor.fetchone().get("cnt", 0))

        if has_system:
            cursor.execute("""
                SELECT 面料编号, 颜色体系, 原始颜色缩写, 归并颜色缩写
                FROM `面料颜色归并对照`
                WHERE 面料编号!='' AND 颜色体系!=''
                  AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                  AND 是否启用=1
            """)
            for row in cursor.fetchall():
                key = (
                    str(row.get('面料编号') or '').strip().upper(),
                    str(row.get('颜色体系') or '').strip(),
                    str(row.get('原始颜色缩写') or '').strip().upper(),
                )
                system_map[key] = str(row.get('归并颜色缩写') or '').strip().upper()

            cursor.execute("""
                SELECT 面料编号, 原始颜色缩写, 归并颜色缩写
                FROM `面料颜色归并对照`
                WHERE 面料编号!=''
                  AND (颜色体系 IS NULL OR 颜色体系='')
                  AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                  AND 是否启用=1
            """)
        else:
            cursor.execute("""
                SELECT 面料编号, 原始颜色缩写, 归并颜色缩写
                FROM `面料颜色归并对照`
                WHERE 面料编号!=''
                  AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                  AND 是否启用=1
            """)

        for row in cursor.fetchall():
            key = (
                str(row.get('面料编号') or '').strip().upper(),
                str(row.get('原始颜色缩写') or '').strip().upper(),
            )
            legacy_map[key] = str(row.get('归并颜色缩写') or '').strip().upper()

    return system_map, legacy_map
