#!/usr/bin/env python3
"""Read explicit workbook cells as evidence. Never save or calculate a workbook.

Plan 1.0: sources contain source_id, path and optional url, expected_sha256,
acquired_at. Records contain evidence_id, source_id, sheet, record_type, fields
and optional header_fields. Each fields/header_fields value is one A1 address
on the record's exact sheet. Relative source paths resolve beside the plan.
Output directory must not exist. Existing dependencies only; no installation.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import posixpath
import re
import sys
import zipfile
from datetime import date, datetime, time, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

READER_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0"
RECORD_TYPES = {"ORDER_LINE", "STYLE_REFERENCE", "COSTING", "OTHER"}
CELL_RE = re.compile(r"^\$?([A-Za-z]{1,3})\$?([1-9][0-9]{0,6})$")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
OLE_MAGIC = bytes.fromhex("d0cf11e0a1b11ae1")


class ReaderError(ValueError):
    """A failed precondition; no evidence should be treated as complete."""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require_keys(obj, required, optional, label):
    if not isinstance(obj, dict):
        raise ReaderError(f"{label} must be an object")
    missing, unknown = required - obj.keys(), obj.keys() - required - optional
    if missing or unknown:
        raise ReaderError(f"{label}: missing keys={sorted(missing)}, unknown keys={sorted(unknown)}")


def nonempty(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ReaderError(f"{label} must be a nonempty string")
    return value


def normalize_cell(value):
    if not isinstance(value, str) or not (match := CELL_RE.fullmatch(value)):
        raise ReaderError(f"Invalid single-cell A1 address: {value!r}")
    letters, row = match.group(1).upper(), int(match.group(2))
    column = 0
    for char in letters:
        column = column * 26 + ord(char) - ord("A") + 1
    if column > 16384 or row > 1048576:
        raise ReaderError(f"Cell exceeds XLSX limits: {value}")
    return f"{letters}{row}", row, column


def cell_map(value, label):
    if not isinstance(value, dict) or not value:
        raise ReaderError(f"{label} must be a nonempty object of field names to cell addresses")
    result = {}
    for field, address in value.items():
        nonempty(field, f"{label} field name")
        result[field] = normalize_cell(address)[0]
    return result


def unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReaderError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def load_plan(plan_path):
    plan_path = Path(plan_path).resolve(strict=True)
    raw = plan_path.read_bytes()
    try:
        plan = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=unique_json_object)
    except (ValueError, UnicodeError) as exc:
        raise ReaderError(f"Invalid UTF-8 JSON plan: {exc}") from exc
    require_keys(plan, {"sources", "records"}, {"schema_version"}, "plan")
    if plan.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise ReaderError("Unsupported plan schema_version; expected 1.0")
    if not isinstance(plan["sources"], list) or not plan["sources"]:
        raise ReaderError("sources must be a nonempty list")
    if not isinstance(plan["records"], list) or not plan["records"]:
        raise ReaderError("records must be a nonempty list")
    sources, source_ids = [], set()
    for item in plan["sources"]:
        require_keys(item, {"source_id", "path"}, {"url", "expected_sha256", "acquired_at"}, "source")
        source_id = nonempty(item["source_id"], "source_id")
        if source_id in source_ids:
            raise ReaderError(f"Duplicate source_id: {source_id}")
        source_ids.add(source_id)
        source = dict(item)
        path = Path(nonempty(item["path"], "source.path"))
        if not path.is_absolute():
            path = plan_path.parent / path
        path = path.resolve(strict=True)
        if not path.is_file():
            raise ReaderError(f"Source is not a regular file: {path}")
        source["path"] = str(path)
        for key in ("url", "acquired_at"):
            if source.get(key) is not None:
                nonempty(source[key], f"source.{key}")
        expected = source.get("expected_sha256")
        if expected is not None:
            if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected):
                raise ReaderError("expected_sha256 must be 64 hexadecimal characters")
            source["expected_sha256"] = expected.lower()
        sources.append(source)
    records, evidence_ids = [], set()
    for item in plan["records"]:
        require_keys(item, {"evidence_id", "source_id", "sheet", "record_type", "fields"}, {"header_fields"}, "record")
        evidence_id = nonempty(item["evidence_id"], "evidence_id")
        if evidence_id in evidence_ids:
            raise ReaderError(f"Duplicate evidence_id: {evidence_id}")
        evidence_ids.add(evidence_id)
        nonempty(item["source_id"], "record.source_id")
        if item["source_id"] not in source_ids:
            raise ReaderError(f"Unknown source_id: {item['source_id']}")
        nonempty(item["sheet"], "record.sheet")
        nonempty(item["record_type"], "record.record_type")
        if item["record_type"] not in RECORD_TYPES:
            raise ReaderError(f"Invalid record_type: {item['record_type']}")
        record = dict(item)
        record["fields"] = cell_map(item["fields"], "record.fields")
        record["header_fields"] = cell_map(item["header_fields"], "record.header_fields") if item.get("header_fields") else {}
        if "header_fields" in item and not isinstance(item["header_fields"], dict):
            raise ReaderError("header_fields must be an object")
        records.append(record)
    return plan_path, hashlib.sha256(raw).hexdigest(), sources, records


def detect_format(path):
    with Path(path).open("rb") as handle:
        magic = handle.read(8)
    if magic == OLE_MAGIC:
        return "XLS_BIFF"
    if magic.startswith(b"PK") and zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            names = set(archive.namelist())
            if "xl/workbook.bin" in names:
                raise ReaderError("XLSB workbooks are unsupported")
            if "[Content_Types].xml" not in names or "xl/workbook.xml" not in names:
                raise ReaderError("ZIP file is not an XLSX/XLSM workbook")
            return "XLSM_OOXML" if "xl/vbaProject.bin" in names else "XLSX_OOXML"
    raise ReaderError(f"Unsupported workbook content (verified by magic/ZIP): {path}")


def dependency(name):
    try:
        return importlib.import_module(name)
    except ImportError as exc:
        raise ReaderError(f"Missing existing dependency {name!r}. Supply --dependency-dir for an existing installation; this reader never installs packages.") from exc


def json_scalar(value):
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ReaderError(f"Unsupported cell value type: {type(value).__name__}")


def raw_sheet_cells(path, requested_sheets):
    """Read storage values separately so empty and absent formula caches differ."""
    result = {}
    with zipfile.ZipFile(path) as archive:
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        targets = {item.attrib["Id"]: item.attrib["Target"] for item in relationships if item.attrib.get("TargetMode") != "External"}
        for sheet in workbook.findall(f"{NS}sheets/{NS}sheet"):
            name = sheet.attrib["name"]
            if name not in requested_sheets:
                continue
            target = targets.get(sheet.attrib[f"{REL_NS}id"])
            if not target:
                raise ReaderError(f"Worksheet relationship is missing: {name}")
            member = target.lstrip("/") if target.startswith("/") else posixpath.normpath(posixpath.join("xl", target))
            root = ET.fromstring(archive.read(member))
            cells = {}
            for cell in root.iter(f"{NS}c"):
                val, formula = cell.find(f"{NS}v"), cell.find(f"{NS}f")
                cells[cell.attrib["r"]] = {
                    "storage_type": cell.attrib.get("t", "n"),
                    "xml_value": val.text if val is not None else None,
                    "value_element_present": val is not None,
                    "formula_element_present": formula is not None,
                    "formula_xml": formula.text if formula is not None else None,
                    "formula_attributes": dict(formula.attrib) if formula is not None else {},
                }
            result[name] = cells
    return result


class XlsxReader:
    def __init__(self, path, sheets):
        openpyxl = dependency("openpyxl")
        self.raw = raw_sheet_cells(path, sheets)
        self.formulas = self.values = None
        # Binary handles permit content-based detection despite misleading suffixes.
        # Normal mode retains merged ranges; no save method is ever called.
        try:
            with Path(path).open("rb") as handle:
                self.formulas = openpyxl.load_workbook(handle, data_only=False, read_only=False, keep_links=False)
            with Path(path).open("rb") as handle:
                self.values = openpyxl.load_workbook(handle, data_only=True, read_only=False, keep_links=False)
        except Exception:
            self.close()
            raise
        self.version = openpyxl.__version__
        self.sheet_names = self.formulas.sheetnames

    def close(self):
        for book in (self.formulas, self.values):
            if book is not None:
                book.close()

    def cell(self, sheet_name, address):
        sheet = self.formulas[sheet_name]
        anchor, merged_range = address, None
        for region in sheet.merged_cells.ranges:
            if address in region:
                anchor, merged_range = region.start_cell.coordinate, str(region)
                break
        cell = sheet[anchor]
        stored = self.raw[sheet_name].get(anchor, {
            "storage_type": None, "xml_value": None, "value_element_present": False,
            "formula_element_present": False, "formula_xml": None, "formula_attributes": {},
        })
        is_formula = cell.data_type == "f" or stored["formula_element_present"]
        cached = self.values[sheet_name][anchor].value if is_formula else None
        value = cached if is_formula else cell.value
        if is_formula:
            formula = cell.value if isinstance(cell.value, str) else getattr(cell.value, "text", None)
            if not stored["value_element_present"]:
                cache_status = "MISSING"
            elif stored["xml_value"] is None:
                cache_status = "PRESENT_EMPTY"
            else:
                cache_status = "PRESENT_VALUE_UNVERIFIED"
            formula_status = "AVAILABLE" if formula is not None else "PRESENT_TEXT_UNAVAILABLE"
        else:
            formula, cache_status, formula_status = None, "NOT_APPLICABLE", "NOT_FORMULA"
        date_value = {"raw": stored["xml_value"], "iso": value.isoformat(), "epoch": self.formulas.epoch.isoformat()} if isinstance(value, (datetime, date, time)) else None
        return {
            "requested_cell": address, "anchor_cell": anchor,
            "resolved_from_merge": address != anchor, "merged_range": merged_range,
            "value": json_scalar(value), "value_type": type(value).__name__,
            "formula": formula, "formula_text_status": formula_status,
            "cache": {"status": cache_status, "value": json_scalar(cached)},
            "raw": stored, "date": date_value,
            "number_format": cell.number_format,
        }


class XlsReader:
    def __init__(self, path, sheets):
        self.xlrd = dependency("xlrd")
        self.book = self.xlrd.open_workbook(str(path), formatting_info=True, on_demand=True)
        self.sheet_names = self.book.sheet_names()
        self.version = self.xlrd.__version__

    def close(self):
        self.book.release_resources()

    def cell(self, sheet_name, address):
        _, row, column = normalize_cell(address)
        if row > 65536 or column > 256:
            raise ReaderError(f"Cell exceeds XLS limits: {address}")
        sheet = self.book.sheet_by_name(sheet_name)
        r, c, merged_range = row - 1, column - 1, None
        for rlo, rhi, clo, chi in sheet.merged_cells:
            if rlo <= r < rhi and clo <= c < chi:
                r, c = rlo, clo
                merged_range = f"{self.xlrd.formula.colname(clo)}{rlo+1}:{self.xlrd.formula.colname(chi-1)}{rhi}"
                break
        anchor = f"{self.xlrd.formula.colname(c)}{r+1}"
        value, ctype, number_format = None, self.xlrd.XL_CELL_EMPTY, None
        if r < sheet.nrows and c < sheet.ncols:
            cell = sheet.cell(r, c)
            value, ctype = cell.value, cell.ctype
            xf = self.book.xf_list[sheet.cell_xf_index(r, c)]
            number_format = self.book.format_map[xf.format_key].format_str
        raw_value, date_value = value, None
        if ctype in (self.xlrd.XL_CELL_EMPTY, self.xlrd.XL_CELL_BLANK):
            value = None
        elif ctype == self.xlrd.XL_CELL_DATE:
            value = self.xlrd.xldate_as_datetime(value, self.book.datemode)
            date_value = {"raw": raw_value, "iso": value.isoformat(), "datemode": self.book.datemode}
        elif ctype == self.xlrd.XL_CELL_BOOLEAN:
            value = bool(value)
        elif ctype == self.xlrd.XL_CELL_ERROR:
            value = self.xlrd.error_text_from_code.get(value, f"UNKNOWN_ERROR_{value}")
        return {
            "requested_cell": address, "anchor_cell": anchor,
            "resolved_from_merge": address != anchor, "merged_range": merged_range,
            "value": json_scalar(value), "value_type": type(value).__name__,
            "formula": None, "formula_text_status": "UNAVAILABLE_XLS",
            "cache": {"status": "UNDETERMINED_XLS", "value": None},
            "raw": {"xlrd_cell_type": ctype, "value": raw_value},
            "date": date_value, "number_format": number_format,
        }


def extract(plan_path, output_dir, dependency_dir=None):
    output_dir = Path(output_dir)
    if output_dir.exists() or output_dir.is_symlink():
        raise ReaderError(f"Output directory already exists; refusing overwrite: {output_dir}")
    output_dir = output_dir.resolve()
    if dependency_dir:
        dep = Path(dependency_dir).resolve(strict=True)
        if not dep.is_dir():
            raise ReaderError("dependency-dir must be an existing directory")
        sys.path.insert(0, str(dep))
    plan_path, plan_hash, sources, records = load_plan(plan_path)
    output_records, source_metadata = [], []
    for source in sources:
        path = Path(source["path"])
        initial_hash = sha256_file(path)
        if source.get("expected_sha256") and source["expected_sha256"] != initial_hash:
            raise ReaderError(f"expected_sha256 mismatch for source_id={source['source_id']}")
        actual_format = detect_format(path)
        file_stat = path.stat()
        metadata = {
            **source, "detected_format": actual_format,
            "sha256_before": initial_hash, "sha256_after": None,
            "file_size_bytes": file_stat.st_size,
            "filesystem_modified_at": datetime.fromtimestamp(file_stat.st_mtime, timezone.utc).isoformat(),
            "read_started_at": utc_now(), "read_finished_at": None,
            "source_business_time": "NOT_INFERRED", "images": {"status": "NOT_REVIEWED"},
            "formula_capability": "TEXT_UNAVAILABLE_FORMULA_PRESENCE_UNKNOWN" if actual_format == "XLS_BIFF" else "FORMULA_TEXT_AND_STORED_CACHE_NO_CALCULATION",
            "macros_executed": False,
        }
        selected = [record for record in records if record["source_id"] == source["source_id"]]
        sheets = {record["sheet"] for record in selected}
        reader = None
        try:
            reader = XlsReader(path, sheets) if actual_format == "XLS_BIFF" else XlsxReader(path, sheets)
            missing = sheets - set(reader.sheet_names)
            if missing:
                raise ReaderError(f"Missing sheet(s) in source_id={source['source_id']}: {sorted(missing)}")
            metadata["library"] = {"name": "xlrd" if actual_format == "XLS_BIFF" else "openpyxl", "version": reader.version}
            for record in selected:
                result = {key: record[key] for key in ("evidence_id", "source_id", "sheet", "record_type")}
                result["fields"] = {field: reader.cell(record["sheet"], address) for field, address in record["fields"].items()}
                result["header_fields"] = {field: reader.cell(record["sheet"], address) for field, address in record["header_fields"].items()}
                result["images"] = {"status": "NOT_REVIEWED"}
                output_records.append(result)
        except ReaderError:
            raise
        except Exception as exc:
            raise ReaderError(f"Cannot read source_id={source['source_id']}: {type(exc).__name__}: {exc}") from exc
        finally:
            if reader is not None:
                reader.close()
        final_hash = sha256_file(path)
        if final_hash != initial_hash:
            raise ReaderError(f"Input changed during extraction: {path}")
        metadata["sha256_after"] = final_hash
        metadata["read_finished_at"] = utc_now()
        source_metadata.append(metadata)
    # Recheck every input after all workbooks are closed, before creating output.
    for metadata in source_metadata:
        if sha256_file(metadata["path"]) != metadata["sha256_before"]:
            raise ReaderError(f"Input changed before publication: {metadata['path']}")
    if sha256_file(plan_path) != plan_hash:
        raise ReaderError("Plan changed during extraction")
    by_id = {record["evidence_id"]: record for record in output_records}
    output_records = [by_id[record["evidence_id"]] for record in records]
    common = {"schema_version": SCHEMA_VERSION, "reader_version": READER_VERSION, "created_at": utc_now(), "plan": {"path": str(plan_path), "sha256": plan_hash}}
    evidence = {**common, "sources": source_metadata, "records": output_records,
        "limitations": ["Only explicitly requested cells are extracted; no semantic matching or order-status inference.", "Images are NOT_REVIEWED.", "Formulas are never calculated; stored cache values can be stale.", "XLS formula text and formula presence are unavailable through xlrd.", "Filesystem timestamps are not source business dates."]}
    evidence_bytes = (json.dumps(evidence, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    manifest = {**common, "status": "COMPLETE", "source_count": len(sources), "record_count": len(records),
        "input_hashes_verified_unchanged": True, "images": {"status": "NOT_REVIEWED"},
        "sources": source_metadata,
        "outputs": [{"path": "evidence.json", "sha256": hashlib.sha256(evidence_bytes).hexdigest(), "size_bytes": len(evidence_bytes)}]}
    manifest_bytes = (json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    output_dir.mkdir(parents=True, exist_ok=False)
    with (output_dir / "evidence.json").open("xb") as handle:
        handle.write(evidence_bytes)
    with (output_dir / "manifest.json").open("xb") as handle:
        handle.write(manifest_bytes)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--dependency-dir", type=Path, help="An existing dependency directory; never installs packages")
    args = parser.parse_args(argv)
    try:
        manifest = extract(args.plan, args.output_dir, args.dependency_dir)
    except (ReaderError, OSError, ValueError) as exc:
        print(json.dumps({"status": "ERROR", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps({"status": "COMPLETE", "output_dir": str(args.output_dir.resolve()), "source_count": manifest["source_count"], "record_count": manifest["record_count"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())