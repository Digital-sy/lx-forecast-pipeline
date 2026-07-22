#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""SKU 颜色体系识别与安全汇总键。

颜色代码必须和颜色体系一起使用。A2023-BK 与 B2024-BK 是两个不同的
颜色身份，除非后续存在明确的人工归并规则，否则不能自动合并。
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Tuple

COLOR_FIELD_ID = "207722905719915521"
COLOR_FIELD_NAME = "颜色体系"
SUPPORTED_SYSTEMS = ("A2023", "B2024")
UNKNOWN_SYSTEM = "待定"
STYLE_TOKENS = {"SHORT", "LONG", "TALL", "PETITE"}
PCS_RE = re.compile(r"^\d+(?:PCS|PSC)$", re.IGNORECASE)


def clean(value: Any) -> str:
    if value is None:
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def normalize_sku(value: Any) -> str:
    text = clean(value).upper().replace("_", "-")
    return re.sub(r"-+", "-", text).strip("-")


def extract_spu_from_sku(sku: str) -> str:
    normalized = normalize_sku(sku)
    return normalized.split("-", 1)[0] if normalized else ""


def extract_raw_color_code(sku: str) -> str:
    """提取 SKU 原始颜色代码；多色套装返回稳定的 MULTI[...] 伪代码。"""
    normalized = normalize_sku(sku)
    parts = [part for part in normalized.split("-") if part]
    if len(parts) < 2:
        return ""

    body = parts[1:]
    if body and body[0] in STYLE_TOKENS:
        body = body[1:]
    if not body:
        return ""

    pcs_index = next((i for i, part in enumerate(body) if PCS_RE.fullmatch(part)), None)
    if pcs_index is not None:
        colors = body[pcs_index + 1:-1] if len(body) > pcs_index + 2 else []
        return f"MULTI[{','.join(colors)}]" if colors else "MULTI"

    return clean(body[0]).upper().replace(" ", "")


def parse_custom_fields(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def extract_color_system(custom_fields: Any) -> str:
    values: list[str] = []
    for item in parse_custom_fields(custom_fields):
        field_id = clean(item.get("id"))
        field_name = clean(item.get("name"))
        if field_id != COLOR_FIELD_ID and field_name != COLOR_FIELD_NAME:
            continue
        value = ""
        for key in ("val_text", "val", "value", "field_value"):
            if item.get(key) is not None:
                value = clean(item.get(key))
                break
        if value and value not in values:
            values.append(value)
    return values[0] if len(values) == 1 else ""


@dataclass(frozen=True)
class ColorIdentity:
    sku: str
    spu: str
    color_system: str
    color_code: str
    aggregate_code: str
    source: str


class ColorSystemResolver:
    """按精确 SKU、同 SPU+颜色码、同 SPU 三层安全识别颜色体系。"""

    def __init__(self, rows: Iterable[Mapping[str, Any]] = ()) -> None:
        self._exact: Dict[str, str] = {}
        pair_systems: Dict[Tuple[str, str], set[str]] = defaultdict(set)
        spu_systems: Dict[str, set[str]] = defaultdict(set)

        for row in rows:
            sku = normalize_sku(row.get("sku"))
            if not sku:
                continue
            spu = clean(row.get("spu")).upper() or extract_spu_from_sku(sku)
            code = extract_raw_color_code(sku)
            system = extract_color_system(row.get("custom_fields_json"))
            if system not in SUPPORTED_SYSTEMS:
                continue
            self._exact[sku] = system
            if spu and code:
                pair_systems[(spu, code)].add(system)
            if spu:
                spu_systems[spu].add(system)

        self._pair_unique = {
            key: next(iter(values))
            for key, values in pair_systems.items()
            if len(values) == 1
        }
        self._spu_unique = {
            key: next(iter(values))
            for key, values in spu_systems.items()
            if len(values) == 1
        }

    @classmethod
    def from_database(cls) -> "ColorSystemResolver":
        from common.database import db_cursor

        rows: list[dict[str, Any]] = []
        try:
            with db_cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA=DATABASE()
                      AND TABLE_NAME='lxpm_product_category_snapshot'
                """)
                if not cursor.fetchone().get("cnt", 0):
                    return cls()
                cursor.execute("""
                    SELECT sku, spu, custom_fields_json
                    FROM `lxpm_product_category_snapshot`
                    WHERE sku IS NOT NULL AND sku != ''
                """)
                rows = list(cursor.fetchall())
        except Exception:
            # 颜色体系不可用时不阻断主任务，但所有颜色都会进入“待定”，不会误并到 A/B。
            return cls()
        return cls(rows)

    def resolve(self, sku: str, spu: str = "") -> ColorIdentity:
        normalized_sku = normalize_sku(sku)
        normalized_spu = clean(spu).upper() or extract_spu_from_sku(normalized_sku)
        color_code = extract_raw_color_code(normalized_sku) or "UNKNOWN"

        system = self._exact.get(normalized_sku)
        source = "SKU精确标签"
        if not system:
            system = self._pair_unique.get((normalized_spu, color_code))
            source = "同SPU同颜色码唯一体系"
        if not system:
            system = self._spu_unique.get(normalized_spu)
            source = "同SPU唯一体系"
        if not system:
            system = UNKNOWN_SYSTEM
            source = "未唯一识别"

        aggregate_code = f"{system}:{color_code}"
        return ColorIdentity(
            sku=normalized_sku,
            spu=normalized_spu,
            color_system=system,
            color_code=color_code,
            aggregate_code=aggregate_code,
            source=source,
        )


def split_aggregate_code(value: str) -> Tuple[str, str]:
    text = clean(value)
    if ":" not in text:
        return UNKNOWN_SYSTEM, text
    system, code = text.split(":", 1)
    return system or UNKNOWN_SYSTEM, code
