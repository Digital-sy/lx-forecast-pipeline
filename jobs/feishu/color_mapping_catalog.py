#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""读取 A2023/B2024 颜色编制表，并提供“颜色体系+颜色代码”映射。

颜色源数据维护在 lx-product-m 项目的
``lx_product_m.color_system_mapping_data`` 中。当前项目运行时只读该数据，
不再从领星产品名称推断颜色名称。
"""
from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

SUPPORTED_SYSTEMS = ("A2023", "B2024")
UNKNOWN_SYSTEM = "待定"


def clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def normalize_code(value: Any) -> str:
    return clean(value).upper().replace(" ", "")


@dataclass(frozen=True)
class ColorMappingEntry:
    sequence: int
    code: str
    english: str
    chinese: str
    pantone: str
    system: str
    is_primary: bool


class ColorMappingCatalog:
    """按 ``(颜色体系, 颜色代码)`` 保存主映射和历史次级映射。"""

    def __init__(self, rows: Iterable[Sequence[Any]]) -> None:
        by_key: dict[tuple[str, str], list[ColorMappingEntry]] = {}
        for raw in rows:
            if len(raw) < 6:
                continue
            sequence, code, english, chinese, pantone, system = raw[:6]
            system = clean(system)
            code = normalize_code(code)
            if system not in SUPPORTED_SYSTEMS or not code:
                continue
            key = (system, code)
            entries = by_key.setdefault(key, [])
            entries.append(
                ColorMappingEntry(
                    sequence=int(sequence or 0),
                    code=code,
                    english=clean(english),
                    chinese=clean(chinese),
                    pantone=clean(pantone),
                    system=system,
                    is_primary=not entries,
                )
            )
        self.by_key = by_key
        self.primary = {key: values[0] for key, values in by_key.items() if values}

    @classmethod
    def from_runtime(cls, strict: bool = True) -> "ColorMappingCatalog":
        rows = load_mapping_rows(strict=strict)
        return cls(rows)

    def lookup(self, system: str, code: str) -> ColorMappingEntry | None:
        return self.primary.get((clean(system), normalize_code(code)))

    def candidates(self, code: str) -> dict[str, ColorMappingEntry | None]:
        code = normalize_code(code)
        return {system: self.lookup(system, code) for system in SUPPORTED_SYSTEMS}

    def describe(self, system: str, code: str) -> dict[str, str]:
        """返回中文名称、展示名称和待定候选。

        已确定体系时，展示名称示例：``黑色｜A2023``。
        待定时不自动选一套映射，而是同时展示 A/B 候选。
        """
        system = clean(system) or UNKNOWN_SYSTEM
        code = normalize_code(code)
        candidates = self.candidates(code)
        a = candidates["A2023"]
        b = candidates["B2024"]
        a_name = a.chinese if a else ""
        b_name = b.chinese if b else ""

        if system in SUPPORTED_SYSTEMS:
            entry = candidates[system]
            if entry:
                chinese = entry.chinese or entry.english or code
                return {
                    "中文颜色名称": chinese,
                    "颜色显示名称": f"{chinese}｜{system}",
                    "颜色映射状态": "颜色编制表主映射",
                    "A2023中文候选": a_name,
                    "B2024中文候选": b_name,
                    "英文名称": entry.english,
                    "潘通色号": entry.pantone,
                }
            return {
                "中文颜色名称": "",
                "颜色显示名称": f"{code}｜{system}（未收录）",
                "颜色映射状态": "该体系颜色代码未收录",
                "A2023中文候选": a_name,
                "B2024中文候选": b_name,
                "英文名称": "",
                "潘通色号": "",
            }

        if a_name and b_name and a_name == b_name:
            display = f"{a_name}｜待定（A2023/B2024同名）"
            chinese = a_name
            status = "体系待定，A/B中文同名"
        else:
            parts = []
            if a_name:
                parts.append(f"A2023:{a_name}")
            if b_name:
                parts.append(f"B2024:{b_name}")
            display = "待定｜" + ("；".join(parts) if parts else f"代码{code}未收录")
            chinese = ""
            status = "体系待定，需人工确认" if parts else "颜色代码未收录"

        return {
            "中文颜色名称": chinese,
            "颜色显示名称": display,
            "颜色映射状态": status,
            "A2023中文候选": a_name,
            "B2024中文候选": b_name,
            "英文名称": "",
            "潘通色号": "",
        }


def _candidate_project_paths() -> list[Path]:
    configured = clean(os.getenv("LX_PRODUCT_M_HOME"))
    current_root = Path(__file__).resolve().parents[2]
    values = [
        Path(configured) if configured else None,
        Path("/opt/apps/lx-product-m"),
        current_root.parent / "lx-product-m",
    ]
    result: list[Path] = []
    for path in values:
        if path and path not in result:
            result.append(path)
    return result


def load_mapping_rows(strict: bool = True) -> Sequence[Sequence[Any]]:
    errors: list[str] = []
    try:
        module = importlib.import_module("lx_product_m.color_system_mapping_data")
        rows = getattr(module, "COLOR_MAPPING_ROWS")
        if rows:
            return rows
    except Exception as exc:  # pragma: no cover - depends on deployment path
        errors.append(f"直接导入失败: {exc}")

    for project_path in _candidate_project_paths():
        if not project_path.exists():
            continue
        path_text = str(project_path)
        if path_text not in sys.path:
            sys.path.insert(0, path_text)
        try:
            sys.modules.pop("lx_product_m.color_system_mapping_data", None)
            module = importlib.import_module("lx_product_m.color_system_mapping_data")
            rows = getattr(module, "COLOR_MAPPING_ROWS")
            if rows:
                return rows
        except Exception as exc:  # pragma: no cover - depends on deployment path
            errors.append(f"{project_path}: {exc}")

    message = (
        "无法读取颜色编制表。请确认 /opt/apps/lx-product-m 存在，或设置 "
        "LX_PRODUCT_M_HOME 指向 lx-product-m 项目根目录。"
    )
    if errors:
        message += " 错误：" + " | ".join(errors)
    if strict:
        raise RuntimeError(message)
    return []
