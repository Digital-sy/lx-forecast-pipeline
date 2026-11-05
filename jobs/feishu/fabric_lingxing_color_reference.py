"""Pure physical-fabric aliases for the independent Lingxing reference column.

No I/O, credentials, or production imports. Codes are aliases of one inventory
record, not a global old/new-code translation. Stock quantity never gates aliases.
Keep AliasIndex (including blocked_keys) until resolve_reference has completed.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable, Mapping


class AliasIndex(dict):
    """Unique aliases only; conflicting keys are deliberately unavailable."""
    def __init__(self):
        super().__init__()
        self.blocked_keys = {}


def _text(value: Any) -> tuple[str, str]:
    if value is None:
        return '', 'MISSING'
    if isinstance(value, str):
        result = value.strip()
        return result, 'OK' if result else 'MISSING'
    # A single textual fragment may omit its type. Multiple fragments must all
    # explicitly be text, so lists of unrelated values/links cannot be collapsed.
    if isinstance(value, dict):
        value = [value]
    if isinstance(value, list) and value:
        parts = []
        for fragment in value:
            if not isinstance(fragment, dict):
                return '', 'AMBIGUOUS_TEXT'
            if set(fragment) - {'text', 'type', 'style'}:
                return '', 'AMBIGUOUS_TEXT'
            if not isinstance(fragment.get('text'), str):
                return '', 'AMBIGUOUS_TEXT'
            if fragment.get('type') not in (None, 'text'):
                return '', 'AMBIGUOUS_TEXT'
            if len(value) > 1 and fragment.get('type') != 'text':
                return '', 'AMBIGUOUS_TEXT'
            parts.append(fragment['text'])
        result = ''.join(parts).strip()
        return result, 'OK' if result else 'MISSING'
    return '', 'AMBIGUOUS_TEXT'


def _code(value: Any) -> tuple[str, str]:
    text, state = _text(value)
    if state != 'OK':
        return '', state
    if not re.fullmatch(r'[A-Za-z0-9]+', text):
        return '', 'COMPOUND_OR_INVALID_CODE'
    return text.upper(), 'OK'


def _fields(record: Mapping[str, Any]) -> Mapping[str, Any]:
    fields = record.get('fields', record)
    return fields if isinstance(fields, Mapping) else {}


def build_aliases(inventory_records: Iterable[Mapping[str, Any]]) -> tuple[AliasIndex, list[dict]]:
    """Build aliases from explicit same-row A2023/B2024 evidence.

    The complete source SKU must end in '-' plus the valid new B2024 code.
    Removing only this suffix yields the complete fabric identifier. Compound
    codes are never split. More than one physical record for an alias is a
    conflict even if their new SKU strings or stock quantities happen to agree.
    """
    aliases = AliasIndex()
    issues = []
    candidates = defaultdict(set)
    record_versions = defaultdict(set)
    for row_index, record in enumerate(inventory_records):
        if not isinstance(record, Mapping):
            issues.append({'row_index': row_index, 'status': 'INVALID_RECORD'})
            continue
        record_id, id_state = _text(record.get('record_id'))
        fields = _fields(record)
        if id_state != 'OK':
            issues.append({'row_index': row_index, 'status': 'MISSING_RECORD_ID'})
            continue
        sku, sku_state = _text(fields.get('面料编号-颜色缩写'))
        new_code, new_state = _code(fields.get('领星新颜色缩写'))
        old_code, old_state = _code(fields.get('颜色缩写'))
        if sku_state != 'OK' or new_state != 'OK':
            issues.append({'record_id': record_id, 'status': 'SOURCE_SKU_OR_NEW_CODE_UNAVAILABLE',
                           'sku_state': sku_state, 'new_code_state': new_state})
            continue
        suffix = '-' + new_code
        if not sku.endswith(suffix) or not sku[:-len(suffix)].strip():
            issues.append({'record_id': record_id, 'status': 'SKU_NEW_CODE_SUFFIX_MISMATCH'})
            continue
        fabric_code = sku[:-len(suffix)].strip()
        # Whitespace inside an identifier is not silently removed or interpreted.
        if any(c.isspace() for c in fabric_code):
            issues.append({'record_id': record_id, 'status': 'INVALID_FABRIC_CODE'})
            continue
        record_versions[record_id].add((sku, new_code, old_code, old_state))
        candidates[(fabric_code, 'B2024', new_code)].add((sku, record_id))
        if old_state == 'OK':
            candidates[(fabric_code, 'A2023', old_code)].add((sku, record_id))
        elif old_state != 'MISSING':
            issues.append({'record_id': record_id, 'status': 'OLD_CODE_NOT_SINGLE', 'old_code_state': old_state})
    inconsistent = {rid for rid, versions in record_versions.items() if len(versions) > 1}
    for key, values in sorted(candidates.items()):
        if any(rid in inconsistent for _, rid in values):
            reason = 'INCONSISTENT_SOURCE_RECORD'
        elif len(values) != 1:
            reason = 'MULTIPLE_SOURCE_RECORDS'
        else:
            sku, record_id = next(iter(values))
            aliases[key] = {'sku': sku, 'record_id': record_id}
            continue
        aliases.blocked_keys[key] = reason
        issues.append({'key': list(key), 'status': 'ALIAS_CONFLICT', 'reason': reason,
                       'candidates': [{'sku': sku, 'record_id': rid} for sku, rid in sorted(values)]})
    return aliases, issues


def resolve_reference(record: Mapping[str, Any], aliases: Mapping, color_map: Mapping[str, str]) -> dict[str, str]:
    """Resolve the reference only. Never substitute standard Chinese color names.

    color_map is the already-parsed Lingxing SKU -> number#color map supplied by
    the existing reader. This function does not parse names or perform I/O.
    """
    def result(status, value='', sku='', record_id=''):
        return {'value': value, 'status': status, 'source_sku': sku, 'source_record_id': record_id}
    fields = _fields(record)
    stat_type, _ = _text(fields.get('统计类型'))
    if stat_type == '总量':
        return result('TOTAL_ROW')
    if stat_type != '带颜色':
        return result('UNKNOWN_STAT_TYPE')
    system, _ = _text(fields.get('颜色体系'))
    if system not in {'A2023', 'B2024'}:
        return result('UNKNOWN_COLOR_SYSTEM')
    fabric_code, fabric_state = _text(fields.get('面料编号'))
    if fabric_state != 'OK' or any(c.isspace() for c in fabric_code):
        return result('MISSING_OR_INVALID_FABRIC_CODE')
    code, code_state = _code(fields.get('颜色缩写'))
    if code_state != 'OK':
        return result('MISSING_OR_INVALID_COLOR_CODE')
    key = (fabric_code, system, code)
    if key in getattr(aliases, 'blocked_keys', {}):
        return result('ALIAS_CONFLICT')
    match = aliases.get(key)
    if not match:
        return result('UNMAPPED_ALIAS')
    sku = match['sku']
    record_id = match['record_id']
    value, value_state = _text(color_map.get(sku))
    if value_state != 'OK':
        return result('SOURCE_COLOR_UNAVAILABLE', sku=sku, record_id=record_id)
    return result('MATCHED', value, sku, record_id)