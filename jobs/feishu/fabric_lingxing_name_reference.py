"""Pure exact-name fallback for a separate Lingxing reference display column.

No I/O and no production imports. Fabric identities and source names must both
match exactly; the only color-name normalization is outer stripping and removal
of one final Chinese character 色. The writer controls fallback eligibility.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping


class NameIndex(dict):
    """Unique source names; retain conflict metadata and explicit fabric scope."""

    def __init__(self):
        super().__init__()
        self.blocked_keys = {}
        self.issues = []
        self.fabric_pairs = set()


def _text(value: Any) -> tuple[str, str]:
    if value is None:
        return '', 'MISSING'
    if isinstance(value, str):
        value = value.strip()
        return value, 'OK' if value else 'MISSING'
    if isinstance(value, dict):
        value = [value]
    if isinstance(value, list) and value:
        parts = []
        for part in value:
            if (not isinstance(part, dict) or
                    set(part) - {'text', 'type', 'style'} or
                    not isinstance(part.get('text'), str) or
                    part.get('type') not in (None, 'text') or
                    (len(value) > 1 and part.get('type') != 'text')):
                return '', 'INVALID'
            parts.append(part['text'])
        result = ''.join(parts).strip()
        return result, 'OK' if result else 'MISSING'
    return '', 'INVALID'


def normalize_color_name(name: str) -> str:
    """Do not remove internal spaces, internal 色, prefixes or synonyms."""
    value = name.strip()
    return value[:-1] if value.endswith('色') else value


def _source_color(product_name: str, fabric_name: str) -> str:
    prefix = fabric_name + '-'
    if not product_name.startswith(prefix):
        return ''
    remainder = product_name[len(prefix):]
    # One numbered color only. Never split compound source names or select the
    # first expression from a longer product description.
    match = re.fullmatch(r'[0-9]+#([^#\r\n,，/;；|、]+)', remainder)
    return match.group(1).strip() if match else ''


def build_name_index(source_rows: Iterable[Mapping[str, Any]],
                     fabric_pairs: Iterable[tuple[str, str]],
                     color_map: Mapping[str, str]) -> NameIndex:
    """Index source SKU/product_name by explicit complete fabric and color name.

    Repeated identical SKU/name rows are harmless. Any differing product_name
    for the same SKU blocks every corresponding name key, even if one differing
    row was unusable for matching. Missing parsed outputs still participate in
    name uniqueness, so an unavailable second SKU cannot hide a conflict.
    """
    index = NameIndex()
    for pair in fabric_pairs:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise ValueError('fabric_pairs must contain complete (code, name) pairs')
        code, cs = _text(pair[0])
        name, ns = _text(pair[1])
        if cs != 'OK' or ns != 'OK' or any(ch.isspace() for ch in code):
            raise ValueError('fabric pair is missing or invalid')
        index.fabric_pairs.add((code, name))
    versions = defaultdict(set)
    candidates = defaultdict(dict)
    for row_number, row in enumerate(source_rows):
        if not isinstance(row, Mapping):
            index.issues.append({'row_number': row_number, 'status': 'INVALID_SOURCE_ROW'})
            continue
        sku, ss = _text(row.get('sku'))
        name, ns = _text(row.get('product_name'))
        if ss != 'OK':
            index.issues.append({'row_number': row_number, 'status': 'MISSING_SOURCE_SKU'})
            continue
        versions[sku].add((ns, name))
        if ns != 'OK':
            index.issues.append({'sku': sku, 'status': 'MISSING_SOURCE_PRODUCT_NAME'})
            continue
        for fabric_code, fabric_name in sorted(index.fabric_pairs):
            if not sku.startswith(fabric_code + '-'):
                continue
            source_color = _source_color(name, fabric_name)
            normalized = normalize_color_name(source_color)
            if not normalized:
                continue
            key = (fabric_code, fabric_name, normalized)
            candidates[key][sku] = {'source_sku': sku, 'source_color_name': source_color}
    inconsistent = {sku for sku, values in versions.items() if len(values) != 1}
    for key, values in sorted(candidates.items()):
        if any(sku in inconsistent for sku in values):
            reason = 'INCONSISTENT_SOURCE_SKU'
        elif len(values) > 1:
            reason = 'MULTIPLE_SOURCE_SKUS'
        else:
            sku, value = next(iter(values.items()))
            parsed, state = _text(color_map.get(sku))
            # The existing parser owns the output format. Do not reconstruct it
            # from the chosen Chinese name or from a different source SKU.
            value = dict(value)
            value['value'] = parsed if state == 'OK' else ''
            index[key] = value
            continue
        index.blocked_keys[key] = reason
        index.issues.append({'key': list(key), 'status': 'NAME_CONFLICT',
                             'reason': reason, 'source_skus': sorted(values)})
    return index


def resolve_name_reference(record: Mapping[str, Any], index: NameIndex) -> dict[str, str]:
    """Resolve one display reference without mutating color, system or inventory.

    The primary Chinese name has priority. If absent, both candidate names must
    yield only one different normalized nonblank name. The original color system
    is returned as evidence and never changed by this name-only fallback.
    """
    fields = record.get('fields', record) if isinstance(record, Mapping) else {}
    if not isinstance(fields, Mapping):
        fields = {}
    system, _ = _text(fields.get('颜色体系'))

    def result(status, chosen='', normalized='', match=None):
        match = match or {}
        source_name = match.get('source_color_name', '')
        success = status == 'NAME_MATCHED'
        return {'value': match.get('value', '') if success else '', 'status': status,
                'source_sku': match.get('source_sku', ''), 'matched_name': chosen,
                'normalized_name': normalized, 'source_color_name': source_name,
                'match_method': ('NAME_EXACT' if chosen == source_name else 'NAME_TRAILING_SE') if success else '',
                'original_system': system}

    stat, _ = _text(fields.get('统计类型'))
    if stat == '总量':
        return result('TOTAL_ROW')
    if stat != '带颜色':
        return result('UNKNOWN_STAT_TYPE')
    code, cs = _text(fields.get('面料编号'))
    fabric_name, fs = _text(fields.get('面料'))
    if cs != 'OK' or fs != 'OK' or any(ch.isspace() for ch in code):
        return result('MISSING_OR_INVALID_FABRIC')
    if (code, fabric_name) not in index.fabric_pairs:
        return result('UNKNOWN_FABRIC')
    primary, primary_state = _text(fields.get('中文颜色名称'))
    if primary_state == 'INVALID':
        return result('INVALID_COLOR_NAME')
    if primary_state == 'OK':
        chosen = primary
        normalized = normalize_color_name(primary)
        if not normalized:
            return result('INVALID_COLOR_NAME', chosen)
    else:
        names = []
        for field in ('A2023中文候选', 'B2024中文候选'):
            value, state = _text(fields.get(field))
            if state == 'INVALID':
                return result('INVALID_COLOR_CANDIDATE')
            if state == 'OK':
                normalized_value = normalize_color_name(value)
                if not normalized_value:
                    return result('INVALID_COLOR_CANDIDATE')
                names.append((value, normalized_value))
        if not names:
            return result('MISSING_COLOR_NAME')
        if len({value for _, value in names}) != 1:
            return result('AMBIGUOUS_COLOR_CANDIDATES')
        # This is deterministic provenance only: both names normalize to exactly
        # the same identity, so no candidate is chosen based on source availability.
        chosen, normalized = names[0]
    key = (code, fabric_name, normalized)
    if key in index.blocked_keys:
        return result('NAME_CONFLICT', chosen, normalized)
    match = index.get(key)
    if not match:
        return result('NAME_NOT_FOUND', chosen, normalized)
    if not match.get('value'):
        return result('SOURCE_COLOR_UNAVAILABLE', chosen, normalized, match)
    return result('NAME_MATCHED', chosen, normalized, match)
