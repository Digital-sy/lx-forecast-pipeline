#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Safe runner for audit_sales_forecast_horizons.

Fixes heterogeneous-row export in the original audit script without changing
any forecast/replay/metric calculation logic. Some TOP-error rows receive
additional diagnostic keys (V4误差/V4绝对误差), so CSV/XLSX field lists must be
the ordered union of keys across all rows rather than rows[0].keys().
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import audit_sales_forecast_horizons as base


def _ordered_fields(rows: Sequence[Dict[str, Any]]) -> List[str]:
    fields: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fields.append(key)
    return fields


def write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = _ordered_fields(rows)
    with path.open('w', encoding='utf-8-sig', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def write_xlsx(path: Path, sheets: Sequence[Tuple[str, Sequence[Dict[str, Any]]]]) -> None:
    if base.Workbook is None:
        return
    wb = base.Workbook()
    wb.remove(wb.active)
    for title, rows in sheets:
        ws = wb.create_sheet(title[:31])
        if not rows:
            ws.append(['无数据'])
            continue
        fields = _ordered_fields(rows)
        ws.append(fields)
        if base.Font:
            for c in ws[1]:
                c.font = base.Font(bold=True)
        for row in rows:
            ws.append([row.get(k) for k in fields])
        ws.freeze_panes = 'A2'
        for col in ws.columns:
            width = min(max(len(str(c.value or '')) for c in col) + 2, 55)
            ws.column_dimensions[col[0].column_letter].width = max(width, 10)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


# Patch output functions only. Forecast logic and metrics remain exactly the
# implementation in audit_sales_forecast_horizons.py.
base.write_csv = write_csv
base.write_xlsx = write_xlsx


if __name__ == '__main__':
    raise SystemExit(base.main())
