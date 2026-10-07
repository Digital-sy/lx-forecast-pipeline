#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Audit NEW_VISIBLE production overlay after the procurement recommendation table is built.

Hard gate in primary mode:
- sum(建议下单量表.建议下单量) by SPU+shop must exactly equal the approved
  NEW_VISIBLE recommended_qty_q50;
- custom/blocked/no-order NEW_VISIBLE rows must sum to zero;
- automatic PO must remain disabled.

Read-only. Exit 1 on mismatch so downstream exports do not continue.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Dict, List, Sequence, Tuple

from common.database import db_cursor
from jobs.feishu import new_visible_procurement_bridge as bridge

ORDER_TABLE = "建议下单量表"


def q(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def main() -> int:
    if bridge.mode() == "off":
        print("NV_PROD_OVERLAY_AUDIT=" + json.dumps({
            "mode": "off",
            "status": "SKIPPED_LEGACY_ROLLBACK",
        }, ensure_ascii=False))
        return 0

    recs = bridge.load_recommendations()
    actual_rows = q(f"""
        SELECT SPU, 店铺, SUM(建议下单量) AS qty
        FROM {ORDER_TABLE}
        GROUP BY SPU, 店铺
    """)
    actual = {
        (
            str(r.get("SPU") or "").strip(),
            str(r.get("店铺") or "").strip(),
        ): int(r.get("qty") or 0)
        for r in actual_rows
    }

    mismatches: List[Dict[str, Any]] = []
    expected_sum = 0
    actual_sum = 0
    custom_or_block_positive = 0

    for key, rec in recs.items():
        expected = max(0, int(rec.get("recommended_qty_q50") or 0))
        got = int(actual.get(key, 0))
        expected_sum += expected
        actual_sum += got

        fabric = str(rec.get("fabric_type") or "")
        status = str(rec.get("recommendation_status") or "")
        if fabric != "现货面料" and got > 0:
            custom_or_block_positive += 1

        if got != expected:
            mismatches.append({
                "store_name": key[1],
                "spu": key[0],
                "fabric_type": fabric,
                "recommendation_status": status,
                "expected_q50": expected,
                "actual_color_sum": got,
                "delta": got - expected,
            })

    auto_po_rows = q(
        f"""
        SELECT COUNT(*) AS n
        FROM {bridge.TABLE}
        WHERE snapshot_date=(SELECT MAX(snapshot_date) FROM {bridge.TABLE})
          AND (automatic_po_enabled<>0 OR production_po_written<>0)
        """
    )
    auto_po_bad = int(auto_po_rows[0].get("n", 0) or 0) if auto_po_rows else 0

    scope = {
        "mode": "primary",
        "override_groups": len(recs),
        "expected_q50_sum": expected_sum,
        "actual_color_sum": actual_sum,
        "mismatch_groups": len(mismatches),
        "custom_or_block_positive_groups": custom_or_block_positive,
        "automatic_po_violation_rows": auto_po_bad,
        "status": (
            "PASS"
            if not mismatches and custom_or_block_positive == 0 and auto_po_bad == 0
            else "FAIL"
        ),
    }
    print("NV_PROD_OVERLAY_AUDIT=" + json.dumps(scope, ensure_ascii=False))

    for row in mismatches[:50]:
        print("NV_PROD_OVERLAY_MISMATCH=" + json.dumps(row, ensure_ascii=False))

    if scope["status"] != "PASS":
        raise RuntimeError(
            "NEW_VISIBLE production overlay audit failed; stop downstream exports"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
