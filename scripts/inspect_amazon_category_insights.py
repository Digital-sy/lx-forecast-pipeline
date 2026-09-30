#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only schema inspector for amazon_category_insights.

Prints only the minimum information needed to adapt Category Insights to the
forecast pipeline: columns/types, a few truncated sample rows, and distinct values
for likely metric/period/type columns. No writes/DDL.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor

SCHEMA = "amazon_category_insights"
TARGET_TABLES = [
    "performance_series",
    "performance_scalar",
    "product_type_keyword",
    "raw_response",
]


def fetch_all(sql: str, params=()) -> List[Dict[str, Any]]:
    with db_cursor(dictionary=True) as cur:
        cur.execute(sql, tuple(params))
        return list(cur.fetchall())


def qident(s: str) -> str:
    return "`" + str(s).replace("`", "``") + "`"


def short(v: Any, limit: int = 320) -> Any:
    if v is None or isinstance(v, (int, float, bool)):
        return v
    if isinstance(v, (bytes, bytearray)):
        return f"<bytes:{len(v)}>"
    s = str(v).replace("\n", " ").replace("\r", " ")
    return s if len(s) <= limit else s[:limit] + f"...<len={len(s)}>"


def main() -> int:
    exists = fetch_all(
        "SELECT COUNT(*) cnt FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s",
        (SCHEMA,),
    )
    if not exists or not int(exists[0]["cnt"] or 0):
        raise RuntimeError(f"schema `{SCHEMA}` 不可见")

    print(f"=== {SCHEMA} 精确结构诊断 ===")
    for table in TARGET_TABLES:
        present = fetch_all(
            "SELECT COUNT(*) cnt FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (SCHEMA, table),
        )
        if not present or not int(present[0]["cnt"] or 0):
            continue

        print(f"\n--- TABLE: {table} ---")
        cols = fetch_all(
            """
            SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            ORDER BY ORDINAL_POSITION
            """,
            (SCHEMA, table),
        )
        for c in cols:
            print({
                "column": c["COLUMN_NAME"],
                "type": c["COLUMN_TYPE"],
                "nullable": c["IS_NULLABLE"],
                "key": c["COLUMN_KEY"],
            })

        table_q = f"{qident(SCHEMA)}.{qident(table)}"
        samples = fetch_all(f"SELECT * FROM {table_q} LIMIT 3")
        print("SAMPLE_ROWS:")
        for r in samples:
            print({k: short(v) for k, v in r.items()})

        # Likely dimension columns whose distinct values help reveal the schema.
        for c in cols:
            name = str(c["COLUMN_NAME"])
            dtype = str(c["DATA_TYPE"] or "").lower()
            lname = name.lower()
            interesting = any(x in lname for x in (
                "metric", "key", "name", "type", "period", "gran", "interval",
                "range", "series", "unit", "scope", "marketplace"
            ))
            if not interesting or dtype in {"json", "blob", "longblob", "mediumblob"}:
                continue
            try:
                vals = fetch_all(
                    f"SELECT DISTINCT {qident(name)} v FROM {table_q} "
                    f"WHERE {qident(name)} IS NOT NULL LIMIT 40"
                )
                print(f"DISTINCT {name}:", [short(x.get("v"), 160) for x in vals])
            except Exception as e:
                print(f"DISTINCT {name}: <skip {type(e).__name__}: {e}>")

    print("\n判定：把 performance_series / performance_scalar 的列名、SAMPLE_ROWS、DISTINCT metric/key/type/period 贴回即可。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
