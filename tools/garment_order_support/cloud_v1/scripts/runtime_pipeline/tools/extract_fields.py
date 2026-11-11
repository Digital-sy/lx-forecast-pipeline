#!/usr/bin/env python3
"""Read-only garment order field extraction; no OCR, download, or workbook writing.

Public API:
  read_cells(path) -> list of {name,state,cells:[{coordinate,row,column,value,type}]}
  extract_workbook(path, metadata=None, allow_two_digit_year=False) -> dict
  parse_filename_date(name, allow_two_digit_year=False) -> dict
  parse_composition(text, provenance=None) -> dict
  merge_composition(text_result, ocr_results) -> dict

All extraction keeps raw evidence. Missing values stay None/[]; ambiguous values
are retained for review. Filename dates never fall back to sheet or file dates.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date
from itertools import combinations
import json
from pathlib import Path
import posixpath
import re
import sys
import unicodedata
import xml.etree.ElementTree as ET
from zipfile import ZipFile, is_zipfile

VERSION = "1.0.0"
TASK_DIR = Path(__file__).resolve().parent  # portable bundle; no legacy absolute dependency
MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS = {"s": MAIN, "r": REL}
STYLE_RE = re.compile(r"(?<![A-Z0-9])([A-Z]{1,8}[0-9]{2,5}(?:-(?:SHORT|LONG))?)(?![0-9])", re.I)
STYLE_LABEL = re.compile(r"(?:公司)?款\s*号|款\s*式\s*号|货\s*号|型\s*号", re.I)
ORDER_LABEL = re.compile(r"(?:^|[\r\n])\s*(?:(?:马\s*[帮邦]|[领零]\s*星)(?:系统)?(?:采购)?(?:批次号|订\s*单\s*编\s*号|订\s*单\s*号|编\s*号|单\s*号|号)?|领批次号|采购单号|采购订单号)\s*[:：]?", re.I)
STOP_LABEL = re.compile(r"(?:下单(?:时间|日期)|要求货期|跟单员|生产供应商|面料供应商|公司款号|主商标|订单日期)\s*[:：]")
FACTORY_ROOTS = {"欧洲站", "加拿大", "美国站", "新款资料", "TK站"}


def unique(seq):
    result = []
    for x in seq:
        if x not in result:
            result.append(x)
    return result


def norm_text(value):
    return unicodedata.normalize("NFKC", str(value)).strip()


def coord(rc):
    row, column = rc
    name = ""
    while column:
        column, rem = divmod(column - 1, 26)
        name = chr(65 + rem) + name
    return f"{name}{row}"


def rc(coordinate):
    m = re.match(r"([A-Z]+)([0-9]+)$", coordinate.upper())
    if not m:
        raise ValueError(f"Invalid cell coordinate: {coordinate}")
    col = 0
    for c in m[1]:
        col = col * 26 + ord(c) - 64
    return int(m[2]), col


def _xml(data):
    if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
        raise ValueError("DTD/entity declarations are unsupported")
    return ET.fromstring(data)


def _text(el):
    return "".join(t.text or "" for t in el.iter() if t.tag.rsplit("}", 1)[-1] == "t")


def _read_xlsx(path):
    sheets = []
    with ZipFile(path) as z:
        if sum(x.file_size for x in z.infolist()) > 1024 * 1024 * 1024:
            raise ValueError("OOXML expanded package exceeds 1 GiB")
        names = set(z.namelist())
        strings = []
        if "xl/sharedStrings.xml" in names:
            strings = [_text(x) for x in _xml(z.read("xl/sharedStrings.xml"))]
        workbook = _xml(z.read("xl/workbook.xml"))
        rels = {}
        for x in _xml(z.read("xl/_rels/workbook.xml.rels")):
            if x.get("TargetMode") != "External":
                target = x.get("Target", "")
                rels[x.get("Id")] = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
        for sh in workbook.find("s:sheets", NS):
            part = rels.get(sh.get(f"{{{REL}}}id"))
            if not part or part not in names:
                continue
            cells = []
            # Iterate serialized cells only: never expand formatted blank ranges.
            with z.open(part) as fh:
                for event, el in ET.iterparse(fh, events=("end",)):
                    if el.tag.rsplit("}", 1)[-1] != "c":
                        continue
                    cell_type = el.get("t", "n")
                    v = el.find("s:v", NS)
                    inline = el.find("s:is", NS)
                    formula = el.find("s:f", NS)
                    value = v.text if v is not None else None
                    if cell_type == "s" and value is not None:
                        value = strings[int(value)]
                    elif cell_type == "inlineStr" and inline is not None:
                        value = _text(inline)
                    if value is not None and str(value).strip():
                        coordinate = el.get("r")
                        row, column = rc(coordinate)
                        cell = {"coordinate": coordinate, "row": row, "column": column, "value": value, "type": cell_type}
                        if formula is not None:
                            cell["formula"] = formula.text or ""
                        cells.append(cell)
                    el.clear()
            sheets.append({"name": sh.get("name"), "state": sh.get("state", "visible"), "cells": cells})
    return sheets


def _read_xls(path):
    deps = TASK_DIR / "deps"
    if str(deps) not in sys.path:
        sys.path.insert(0, str(deps))
    import xlrd
    book = xlrd.open_workbook(str(path), on_demand=True, formatting_info=False)
    sheets = []
    try:
        for sh in book.sheets():
            cells = []
            for row in range(sh.nrows):
                for column, c in enumerate(sh.row(row)):
                    if c.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) or c.value == "":
                        continue
                    value = c.value
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    cells.append({"coordinate": coord((row + 1, column + 1)), "row": row + 1, "column": column + 1, "value": value, "type": str(c.ctype)})
            sheets.append({"name": sh.name, "state": ["visible", "hidden", "veryHidden"][min(getattr(sh, "visibility", 0), 2)], "cells": cells})
    finally:
        book.release_resources()
    return sheets


def read_cells(path):
    path = Path(path)
    with path.open("rb") as fh:
        signature = fh.read(8)
    if signature == bytes.fromhex("D0CF11E0A1B11AE1"):
        return _read_xls(path)
    if is_zipfile(path):
        return _read_xlsx(path)
    raise ValueError(f"Unsupported workbook signature: {signature.hex()}")


def read_extracted_cells(extract_dir):
    extract_dir = Path(extract_dir)
    manifest = json.loads((extract_dir / "manifest.json").read_text(encoding="utf-8-sig"))
    sheets = {s["name"]: {"name": s["name"], "state": s.get("state", "visible"), "cells": []} for s in manifest.get("sheets", [])}
    with (extract_dir / "cells.jsonl").open(encoding="utf-8-sig") as fh:
        for line in fh:
            c = json.loads(line)
            if c.get("value") is None or str(c["value"]).strip() == "":
                continue
            sheet_name = c["sheet"]
            if sheet_name not in sheets:
                sheets[sheet_name] = {"name": sheet_name, "state": "visible", "cells": []}
            coordinate = c.get("coordinate", c.get("ref"))
            row, column = rc(coordinate)
            cell = {"coordinate": coordinate, "row": row, "column": column, "value": c["value"], "type": c.get("type", c.get("data_type"))}
            if c.get("formula") is not None:
                cell["formula"] = c["formula"]
            sheets[sheet_name]["cells"].append(cell)
    return list(sheets.values())


def _valid_date(year, month, day):
    try:
        if not 2000 <= int(year) <= 2099:
            return None
        return date(int(year), int(month), int(day)).isoformat()
    except (ValueError, TypeError):
        return None


def parse_filename_date(name, allow_two_digit_year=False):
    """Enumerate explicit and zero-repair candidates; never choose by proximity.

    Up to two excess zeros may be deleted from the month/day part of 9/10 digit
    tokens. Every valid candidate is retained; only a unique date is selected.
    7 digit tokens enumerate one-digit month OR one-digit day, retaining ambiguity.
    """
    name = norm_text(name)
    tokens, used = [], []

    def add(m, candidates, method, flags=None):
        candidates = sorted(set(c for c in candidates if c))
        tokens.append({"raw": m[0], "start": m.start(), "end": m.end(), "candidates": candidates, "method": method, "flags": flags or []})
        used.append((m.start(), m.end()))

    def overlaps(m):
        return any(m.start() < e and m.end() > s for s, e in used)

    # Normal separators and explicit leading zeros in separated parts.
    for m in re.finditer(r"(?<![0-9])(20[0-9]{2})[.\-/年](0*[0-9]{1,2})[.\-/月](0*[0-9]{1,2})(?:日)?(?![0-9])", name):
        repaired = len(m[2]) > 2 or len(m[3]) > 2
        add(m, [_valid_date(m[1], m[2], m[3])], "separated", ["date_extra_leading_zero_removed"] if repaired else [])
    # Year joined to month with an explicit separator before day, e.g.2026010-27.
    for m in re.finditer(r"(?<![0-9])(20[0-9]{2})(0*[0-9]{1,2})[.\-/月](0*[0-9]{1,2})(?:日)?(?![0-9])", name):
        if not overlaps(m):
            add(m, [_valid_date(m[1], m[2], m[3])], "joined_year_month", ["date_nonstandard_format"])
    for m in re.finditer(r"(?<![0-9])(20[0-9]{5,8})(?![0-9])", name):
        if overlaps(m):
            continue
        t = m[1]
        candidates = []
        flags = []
        method = "compact"
        if len(t) == 8:
            candidates.append(_valid_date(t[:4], t[4:6], t[6:8]))
        elif len(t) == 7:
            candidates.extend([_valid_date(t[:4], t[4:5], t[5:]), _valid_date(t[:4], t[4:6], t[6:])])
            method = "unpadded_compact"
            flags.append("date_nonstandard_format")
        elif len(t) in (9, 10):
            excess = len(t) - 8
            positions = [i for i in range(4, len(t)) if t[i] == "0"]
            for remove in combinations(positions, excess):
                c = "".join(ch for i, ch in enumerate(t) if i not in remove)
                candidates.append(_valid_date(c[:4], c[4:6], c[6:]))
            method = "excess_zero_candidates"
            flags.append("date_extra_zero_repair")
        add(m, candidates, method, flags)
    for m in re.finditer(r"(?<![0-9])([2][0-9])[.\-/年]([0-9]{1,2})[.\-/月]([0-9]{1,2})(?:日)?(?![0-9])", name):
        if not overlaps(m):
            candidate = _valid_date(2000 + int(m[1]), m[2], m[3])
            add(m, [candidate] if allow_two_digit_year else [], "two_digit_year", ["date_two_digit_year_assumed_2020s"] if allow_two_digit_year else ["date_two_digit_year_unconfirmed"])
    tokens.sort(key=lambda t: t["start"])
    candidates = sorted(set(c for t in tokens for c in t["candidates"]))
    flags = unique([f for t in tokens for f in t["flags"]])
    if len(candidates) > 1:
        status = "ambiguous"
        flags.append("date_multiple_candidates")
    elif len(candidates) == 1:
        status = "repaired" if flags else "parsed"
        if len(tokens) > 1:
            flags.append("date_multiple_tokens")
    else:
        status = "unparsed" if tokens else "missing"
        flags.append("date_invalid_or_unconfirmed" if tokens else "date_missing_in_filename")
    return {"value": candidates[0] if len(candidates) == 1 else None, "raw": [t["raw"] for t in tokens], "candidates": candidates, "status": status, "tokens": tokens, "flags": flags}


FIBERS = {
    "涤纶": ["POLYESTER", "涤纶", "聚酯纤维", "聚酯", "滌綸", "涤"],
    "氨纶": ["SPANDEX", "ELASTANE", "氨纶", "氨綸", "LYCRA"],
    "锦纶": ["POLYAMIDE", "NYLON", "锦纶", "錦綸", "尼龙"],
    "粘纤": ["VISCOSE", "RAYON", "粘胶纤维", "黏胶纤维", "粘纤", "粘胶", "人造丝", "人造棉", "人棉"],
    "棉": ["COTTON", "棉"],
    "麻": ["LINEN", "亚麻"],
    "羊毛": ["WOOL", "羊毛"],
    "腈纶": ["ACRYLIC", "腈纶", "腈綸"],
    "真丝": ["SILK", "真丝", "桑蚕丝"],
    "莱赛尔": ["LYOCELL", "莱赛尔"],
    "莫代尔": ["MODAL", "莫代尔"],
    "醋酯纤维": ["ACETATE", "醋酯纤维", "醋酸纤维"],
    "再生纤维素纤维": ["再生纤维素纤维", "RECYCLED CELLULOSE FIBER", "REGENERATED CELLULOSE FIBER"],
    "人造羊毛": ["人造羊毛"],
    "羊毛蛋白": ["羊毛蛋白"],
    "竹纤维": ["竹纤维", "BAMBOO FIBER"],
    "PU": ["PU"],
    "木代尔": ["木代尔"],
    "晴纶": ["晴纶"],
    "兰精天丝": ["兰精天丝"],
}
UNVERIFIED_MATERIAL_NAMES = {"人造羊毛", "羊毛蛋白", "木代尔", "晴纶", "兰精天丝"}
FIBER_LOOKUP = {x.upper(): k for k, v in FIBERS.items() for x in v}
FIBER_PATTERN = "(?:" + "|".join(re.escape(x) for x in sorted(FIBER_LOOKUP, key=len, reverse=True)) + ")"
PAIR_RE = re.compile(r"(?:(?P<p1>[0-9]{1,3}(?:\.[0-9]+)?)\s*%\s*(?P<f1>" + FIBER_PATTERN + r")(?![A-Za-z])|(?<![A-Za-z])(?P<f2>" + FIBER_PATTERN + r")\s*[:：]?\s*(?P<p2>[0-9]{1,3}(?:\.[0-9]+)?)\s*%)", re.I)
LAYER_RE = re.compile(r"(?P<layer>SELF\s*FABRIC|OUTER\s*(?:SHELL|FABRIC)|SHELL|LINING|BODY|CONTRAST|MESH|TRIM|LACE|TOP|SKIRT|面料成分|里料成分|主布|主料|面料|里布|里料|外层|内层|网纱|拼接|蕾丝|上衣|裙身|袖子|袖部|裤身|裤腿)\s*[:：]?", re.I)
LAYER_MAP = {"SELF FABRIC": "面料", "OUTER SHELL": "面料", "OUTER FABRIC": "面料", "SHELL": "面料", "BODY": "面料", "主布": "面料", "主料": "面料", "面料成分": "面料", "LINING": "里料", "里布": "里料", "里料成分": "里料", "MESH": "网纱", "CONTRAST": "拼接", "LACE": "蕾丝", "TOP": "上衣", "SKIRT": "裙身"}


def parse_composition(text, provenance=None):
    raw = str(text)
    normalized = norm_text(raw).replace("％", "%")
    # Windows OCR may put spaces inside a fiber name. Collapse only an exact
    # recognized name; never guess a misspelled fiber or alter percentage digits.
    for alias in sorted(FIBER_LOOKUP, key=len, reverse=True):
        spaced = r"\s*".join(re.escape(ch) for ch in alias)
        normalized = re.sub(spaced, alias, normalized, flags=re.I)
    pairs = list(PAIR_RE.finditer(normalized))
    layers = list(LAYER_RE.finditer(normalized))
    groups = []
    current = None
    flags = []
    for match in pairs:
        pct = float(match["p1"] or match["p2"])
        fiber_raw = match["f1"] or match["f2"]
        prior = [x for x in layers if x.end() <= match.start()]
        layer_match = prior[-1] if prior else None
        layer_raw = layer_match["layer"] if layer_match else None
        layer = LAYER_MAP.get(re.sub(r"\s+", " ", layer_raw.upper()), layer_raw) if layer_raw else None
        layer_pos = layer_match.start() if layer_match else -1
        if current is None or current["_layer_pos"] != layer_pos:
            current = {"layer": layer, "layer_raw": layer_raw, "components": [], "_layer_pos": layer_pos, "flags": []}
            groups.append(current)
        elif abs(sum(x["percent"] for x in current["components"]) - 100) < 0.001:
            current = {"layer": layer, "layer_raw": layer_raw, "components": [], "_layer_pos": layer_pos, "flags": ["composition_separate_unlabelled_group"]}
            groups.append(current)
            flags.append("composition_multiple_groups_same_layer")
        current["components"].append({"fiber": FIBER_LOOKUP[fiber_raw.upper()], "fiber_raw": fiber_raw, "percent": int(pct) if pct.is_integer() else pct, "raw": match[0], "span": [match.start(), match.end()]})
        if FIBER_LOOKUP[fiber_raw.upper()] in UNVERIFIED_MATERIAL_NAMES:
            current["flags"].append("composition_material_name_unverified")
        if not 0 < pct <= 100:
            current["flags"].append("composition_percent_out_of_range")
    pct_matches = list(re.finditer(r"[0-9]{1,3}(?:\.[0-9]+)?\s*%", normalized))
    unmatched = [m[0] for m in pct_matches if not any(p.start() <= m.start() and p.end() >= m.end() for p in pairs)]
    if unmatched:
        flags.append("composition_unrecognized_percent_fiber")
    for group in groups:
        del group["_layer_pos"]
        total = round(sum(x["percent"] for x in group["components"]), 6)
        group["total_percent"] = total
        if abs(total - 100) > 0.01:
            group["flags"].append("composition_total_not_100")
        fibers = [x["fiber"] for x in group["components"]]
        if len(set(fibers)) < len(fibers):
            group["flags"].append("composition_repeated_fiber")
        group["display"] = (f"{group['layer']}：" if group["layer"] else "") + "、".join(f"{x['fiber']}{x['percent']:g}%" for x in group["components"])
        flags.extend(group["flags"])
    if not groups:
        flags.append("composition_not_parsed")
    # Bilingual labels can repeat precisely the same composition. Retain raw and
    # repetition count, but do not manufacture an additional fabric layer.
    deduped = []
    for group in groups:
        sig = (group["layer"], tuple(sorted((x["fiber"], x["percent"]) for x in group["components"])))
        prior = next((g for g in deduped if g["_signature"] == sig), None)
        if prior:
            prior["equivalent_group_repetitions"] = prior.get("equivalent_group_repetitions", 1) + 1
        else:
            group["_signature"] = sig
            deduped.append(group)
    if len(deduped) < len(groups):
        flags.append("composition_repeated_equivalent")
        if len(deduped) == 1:
            flags = [f for f in flags if f not in {"composition_multiple_groups_same_layer", "composition_separate_unlabelled_group"}]
    groups = deduped
    for group in groups:
        del group["_signature"]
    return {"raw": raw, "groups": groups, "display": "；".join(g["display"] for g in groups), "unmatched_percentages": unmatched, "provenance": provenance, "flags": unique(flags)}


def composition_signature(parsed, include_layer=False):
    return tuple(sorted((g.get("layer") or "" if include_layer else "", tuple(sorted((c["fiber"], c["percent"]) for c in g["components"]))) for g in parsed.get("groups", [])))


def combine_adjacent_compositions(parsed_entries):
    """Join a composition split down contiguous cells in the same column.

    Only complete 100% groups without material/layer ambiguity qualify. Original
    cell/raw pairs remain in source_parts. Separate layers are never added.
    """
    result, consumed = [], set()
    for i, entry in enumerate(parsed_entries):
        if i in consumed:
            continue
        groups = entry["groups"]
        eligible = len(groups) == 1 and groups[0]["layer"] is None and 0 < groups[0]["total_percent"] < 100 and set(entry["flags"]) <= {"composition_total_not_100"}
        if not eligible:
            result.append(entry)
            continue
        provenance = entry["provenance"]
        row, col = rc(provenance["cell"])
        parts = [(i, entry)]
        total = groups[0]["total_percent"]
        previous_row = row
        for j, other in enumerate(parsed_entries):
            if j <= i or j in consumed or other["provenance"]["sheet"] != provenance["sheet"]:
                continue
            next_row, next_col = rc(other["provenance"]["cell"])
            og = other["groups"]
            if next_col != col or next_row != previous_row + 1 or len(og) != 1 or og[0]["layer"] is not None or set(other["flags"]) - {"composition_total_not_100"}:
                continue
            if total + og[0]["total_percent"] > 100.01:
                continue
            parts.append((j, other))
            total += og[0]["total_percent"]
            previous_row = next_row
            if abs(total - 100) < 0.01:
                combined_provenance = {**provenance, "cells": [p["provenance"]["cell"] for _, p in parts], "method": "composition_adjacent_cells"}
                combined = parse_composition("\n".join(p["raw"] for _, p in parts), combined_provenance)
                combined["source_parts"] = [{"raw": p["raw"], "provenance": p["provenance"]} for _, p in parts]
                combined["flags"].append("composition_joined_adjacent_cells")
                result.append(combined)
                consumed.update(k for k, p in parts)
                break
        if i not in consumed:
            result.append(entry)
    return result


def merge_composition(text_result, ocr_results):
    """Keep every source; flag divergent parsed compositions, never overwrite.

    text_result may be the composition_text field or a list of parse results.
    OCR entries may be parse results, strings, or dicts with text/raw + provenance.
    """
    if isinstance(text_result, dict) and "parsed" in text_result:
        alternatives = [{"source_type": "cell_text", **x} for x in text_result["parsed"]]
    elif isinstance(text_result, list):
        alternatives = [{"source_type": "cell_text", **x} for x in text_result]
    elif isinstance(text_result, dict) and "groups" in text_result:
        alternatives = [{"source_type": "cell_text", **text_result}]
    else:
        alternatives = []
    for x in ocr_results or []:
        if isinstance(x, str):
            parsed = parse_composition(x)
        elif "groups" in x:
            parsed = x
        else:
            parsed = parse_composition(x.get("text", x.get("raw_text", x.get("raw", ""))), x.get("provenance", x))
        alternatives.append({"source_type": "image_ocr", **parsed})
    signatures = {composition_signature(x) for x in alternatives if x.get("groups")}
    flags = unique([f for x in alternatives for f in x.get("flags", [])])
    if len(signatures) > 1:
        flags.append("composition_multiple_or_conflicting_sources")
        types = {x["source_type"] for x in alternatives if x.get("groups")}
        if len(types) > 1:
            flags.append("composition_text_image_conflict")
    return {"values": unique([x["display"] for x in alternatives if x.get("display")]), "alternatives": alternatives, "flags": flags}


def _evidence(sheet, cell, method, value=None):
    return {"sheet": sheet["name"], "sheet_state": sheet["state"], "cell": cell["coordinate"], "raw": cell["value"], "value": value, "method": method}


def _neighbor_values(sheet, cell, max_right=5):
    right = [x for x in sheet["cells"] if x["row"] == cell["row"] and cell["column"] < x["column"] <= cell["column"] + max_right]
    below = [x for x in sheet["cells"] if cell["row"] < x["row"] <= cell["row"] + 2 and x["column"] == cell["column"]]
    return sorted(right, key=lambda c: c["column"])[:1] + sorted(below, key=lambda c: c["row"])[:1]


def _styles(value):
    return unique([m[1].upper() for m in STYLE_RE.finditer(norm_text(value))])


def _field(values, evidence, flags=None, **kwargs):
    values = unique([v for v in values if v is not None and v != ""])
    return {"value": values[0] if len(values) == 1 else None, "values": values, "evidence": evidence, "flags": unique(flags or []), **kwargs}


def extract_factory(metadata):
    parent = metadata.get("parent_path", "")
    if not parent and metadata.get("remote_path"):
        parent = metadata["remote_path"].rsplit("/", 1)[0]
    parts = [p for p in parent.replace("\\", "/").split("/") if p]
    values = []
    for i, part in enumerate(parts):
        if part in FACTORY_ROOTS and i + 1 < len(parts):
            values = [parts[i + 1]]
            break
    return _field(values, [{"raw": parent, "method": "folder_path_after_market_or_new_style", "value": values[0] if values else None}], [] if values else ["factory_path_unresolved"])


def _extract_order_value(value):
    text = norm_text(value)
    text = STOP_LABEL.split(text)[0].strip(" :：\n\r\t,，;；")
    if not text or len(text) > 250:
        return []
    if text in {"单号", "订单号", "编号", "采购单号", "订单编号"}:
        return []
    # Keep labels/statuses such as 新款 exactly; split only explicit list separators.
    # PO codes are identifiers; surrounding 备货/新加色 annotations remain in raw
    # cell evidence and do not create a second identifier for the same PO.
    identifiers = re.findall(r"(?<![A-Z0-9])PO[0-9A-Z-]{6,}(?![A-Z0-9])", text, re.I)
    if identifiers:
        return unique([x.upper() for x in identifiers])
    if "新款" in text and not re.search(r"[0-9]", text):
        return ["新款"]
    if "测试款" in text and len(text) <= 20:
        return [text]
    values = [x.strip() for x in re.split(r"[\n\r,，;；、]+", text) if x.strip()]
    return [v for v in values if re.fullmatch(r"[A-Z0-9-]{6,}", v, re.I) and re.search(r"[0-9]", v)]


def extract_workbook(path, metadata=None, allow_two_digit_year=False):
    metadata = dict(metadata or {})
    name = metadata.get("name") or Path(path).name
    result = {"version": VERSION, "source": {**metadata, "local_path": str(Path(path).resolve()), "name": name}, "status": "ok", "fields": {}, "sheets": [], "flags": []}
    result["fields"]["factory"] = extract_factory(metadata)
    result["fields"]["order_date"] = parse_filename_date(name, allow_two_digit_year)
    try:
        extract_dir = Path(metadata.get("extract_dir", TASK_DIR / "extracts" / str(metadata.get("token", "__no_token__"))))
        if (extract_dir / "manifest.json").is_file() and (extract_dir / "cells.jsonl").is_file():
            try:
                sheets = read_extracted_cells(extract_dir)
                result["extraction_method"] = "existing_cells_jsonl"
            except (ValueError, KeyError, OSError) as cached_error:
                result["cache_warning"] = f"{type(cached_error).__name__}: {cached_error}"
                sheets = read_cells(path)
                result["extraction_method"] = "workbook_direct_after_cache_error"
        else:
            sheets = read_cells(path)
            result["extraction_method"] = "workbook_direct"
    except Exception as exc:
        result["status"] = "error"
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["flags"].append("workbook_read_failed")
        return result
    header_style, sheet_style, sku_style = [], [], []
    order_evidence, orders, compositions, comp_evidence = [], [], [], []
    factory_sheet_evidence, body_dates = [], []
    for sheet in sheets:
        result["sheets"].append({"name": sheet["name"], "state": sheet["state"], "nonempty_cells": len(sheet["cells"])})
        for style in _styles(sheet["name"]):
            sheet_style.append({"sheet": sheet["name"], "sheet_state": sheet["state"], "cell": None, "raw": sheet["name"], "value": style, "method": "sheet_name"})
        for cell in sheet["cells"]:
            value = cell["value"]
            if not isinstance(value, str):
                continue
            text = norm_text(value)
            if cell.get("formula") or "DISPIMG(" in text:
                continue
            if cell["row"] <= 30 and len(text) <= 300 and STYLE_LABEL.search(text):
                hit = STYLE_LABEL.search(text)
                styles = _styles(text[hit.end():])
                for style in styles:
                    header_style.append(_evidence(sheet, cell, "style_header", style))
                if not styles and len(text) < 30:
                    for neighbor in _neighbor_values(sheet, cell):
                        for style in _styles(neighbor["value"]):
                            ev = _evidence(sheet, neighbor, "style_header_adjacent", style)
                            ev["label_cell"] = cell["coordinate"]
                            header_style.append(ev)
                            break
            if cell["row"] <= 80 and re.fullmatch(r"[A-Za-z]{1,8}[0-9]{2,5}[-_][A-Za-z0-9_-]+", text):
                for style in _styles(text):
                    sku_style.append(_evidence(sheet, cell, "sku_prefix", style))
            order_match = ORDER_LABEL.search(text)
            if order_match and cell["row"] <= 100 and len(text) <= 500:
                raw_value = text[order_match.end():]
                found = _extract_order_value(raw_value)
                for order in found:
                    orders.append(order)
                    order_evidence.append(_evidence(sheet, cell, "mabang_or_lingxing_label", order))
                if not found and len(text) <= 30:
                    for neighbor in _neighbor_values(sheet, cell):
                        nvalue = norm_text(neighbor["value"])
                        if re.search(r"[:：]|供应商|跟单员|下单|货期|商标|产品|图片|颜色|数量|SKU", nvalue, re.I):
                            continue
                        norders = _extract_order_value(nvalue)
                        if norders:
                            for order in norders:
                                orders.append(order)
                                ev = _evidence(sheet, neighbor, "order_label_adjacent", order)
                                ev["label_cell"] = cell["coordinate"]
                                order_evidence.append(ev)
                            break
            if re.search(r"生产(?:供应商|工厂)|加工厂", text) and cell["row"] <= 20:
                factory_sheet_evidence.append(_evidence(sheet, cell, "sheet_factory_for_crosscheck"))
            if re.search(r"下单(?:时间|日期)", text) and cell["row"] <= 30:
                body_dates.append(_evidence(sheet, cell, "sheet_order_date_crosscheck_only"))
            if "%" in text and (re.search(FIBER_PATTERN, text, re.I) or "成分" in text):
                ev = _evidence(sheet, cell, "cell_composition")
                parsed = parse_composition(text, ev)
                compositions.append(parsed)
                comp_evidence.append(ev)
            elif re.fullmatch(r"(?:面料|里料)?成[分份]\s*[:：]?", text):
                for neighbor in _neighbor_values(sheet, cell):
                    nt = norm_text(neighbor["value"])
                    if "%" in nt or re.search(FIBER_PATTERN, nt, re.I):
                        ev = _evidence(sheet, neighbor, "composition_label_adjacent")
                        ev["label_cell"] = cell["coordinate"]
                        if not any(x["provenance"]["sheet"] == ev["sheet"] and x["provenance"]["cell"] == ev["cell"] for x in compositions):
                            compositions.append(parse_composition(nt, ev))
                            comp_evidence.append(ev)
    filename_styles = _styles(name)
    style_evidence = header_style or sheet_style or sku_style or [{"raw": name, "value": v, "method": "filename_fallback", "sheet": None, "cell": None} for v in filename_styles]
    styles = unique([x["value"] for x in style_evidence])
    style_flags = []
    if not header_style:
        style_flags.append("style_no_explicit_header")
    if len(styles) > 1:
        style_flags.append("style_multiple_values")
    if not styles:
        style_flags.append("style_missing")
    if filename_styles and styles and set(styles) != set(filename_styles):
        style_flags.append("style_filename_header_disagreement")
    result["fields"]["style"] = _field(styles, style_evidence, style_flags, filename_candidates=filename_styles, other_evidence=sheet_style + sku_style)
    order_flags = []
    if not orders:
        order_flags.append("order_number_missing")
    if len(unique(orders)) > 1:
        order_flags.append("order_multiple_values")
    if any("测试款" in x for x in orders):
        order_flags.append("order_non_identifier_status")
    if any(x != "新款" and "测试款" not in x and not re.fullmatch(r"PO[0-9]{9}", x) for x in orders):
        order_flags.append("order_identifier_nonstandard")
    result["fields"]["order_no"] = _field(orders, order_evidence, order_flags)
    # De-duplicate duplicate scans of adjacent label/value cells without losing evidence.
    comps = []
    seen_comp = set()
    for x in compositions:
        p = x["provenance"]
        key = (p["sheet"], p["cell"], x["raw"])
        if key not in seen_comp:
            comps.append(x)
            seen_comp.add(key)
    comps = combine_adjacent_compositions(comps)
    comp_flags = unique([f for x in comps for f in x["flags"]])
    if len({composition_signature(x) for x in comps if x["groups"]}) > 1:
        comp_flags.append("composition_multiple_text_values")
    if not comps:
        comp_flags.append("composition_text_missing")
    result["fields"]["composition_text"] = _field([x["display"] for x in comps], [x["provenance"] for x in comps], comp_flags, parsed=comps, raw_values=unique([x["raw"] for x in comps]))
    result["crosschecks"] = {"factory_in_sheet": factory_sheet_evidence, "order_date_in_sheet": body_dates}
    result["flags"] = unique(result["flags"] + [f for v in result["fields"].values() for f in v.get("flags", [])])
    if len(styles) > 1 or len(unique(orders)) > 1:
        result["flags"].append("multiple_values_relationship_requires_review")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?")
    parser.add_argument("--name")
    parser.add_argument("--parent-path", default="")
    parser.add_argument("--downloads", action="append", help="Download manifest JSONL; repeat for native exports")
    parser.add_argument("--output", help="JSON/JSONL output; stdout if omitted")
    parser.add_argument("--allow-two-digit-year", action="store_true")
    args = parser.parse_args()
    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        if args.downloads:
            entries = []
            for manifest_path in args.downloads:
                for line in Path(manifest_path).read_text(encoding="utf-8-sig").splitlines():
                    if line.strip():
                        entries.append(json.loads(line))
            # Latest successful manifest entry per token wins; preserve all file tokens.
            selected = {}
            for entry in entries:
                if entry.get("status") == "ok" and entry.get("local_path"):
                    selected[entry.get("token", entry["local_path"])] = entry
            for entry in selected.values():
                result = extract_workbook(entry["local_path"], entry, args.allow_two_digit_year)
                print(json.dumps(result, ensure_ascii=False), file=out, flush=True)
        elif args.path:
            result = extract_workbook(args.path, {"name": args.name or Path(args.path).name, "parent_path": args.parent_path}, args.allow_two_digit_year)
            print(json.dumps(result, ensure_ascii=False, indent=2), file=out)
        else:
            parser.error("provide path or --downloads")
    finally:
        if args.output:
            out.close()


if __name__ == "__main__":
    main()
