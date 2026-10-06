#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Read-only audit of historical feature *signal* coverage.

The research daily table stores zero for many summed optional metrics. Non-null coverage
therefore only proves schema presence, not informative signal. This audit reports nonzero
coverage overall, on active-sales rows, and by month.

No table is created or modified.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.database import db_cursor
from jobs.forecast_research.build_spu_daily_history import DEST_TABLE

FIELDS = [
    "clicks",
    "impressions",
    "ad_spend",
    "ad_orders",
    "ad_sales",
    "promotion_units",
    "avg_price",
]


def q(sql: str, params=()):
    with db_cursor() as c:
        c.execute(sql, params)
        return list(c.fetchall())


def rate(n, d):
    return round(float(n) / float(d), 4) if d else None


def main() -> int:
    print("=" * 100)
    print("历史研究特征信号覆盖审计（只读）")
    print("=" * 100)

    overall_expr = ["COUNT(*) AS rows_n", "SUM(sales_units>0) AS active_sales_rows"]
    for f in FIELDS:
        overall_expr.append(f"SUM(COALESCE({f},0)<>0) AS {f}_nonzero")
        overall_expr.append(
            f"SUM(sales_units>0 AND COALESCE({f},0)<>0) AS {f}_active_nonzero"
        )
    rows = q(f"SELECT {','.join(overall_expr)} FROM `{DEST_TABLE}`")
    r = rows[0]
    total = int(r.get("rows_n") or 0)
    active = int(r.get("active_sales_rows") or 0)
    out = {"rows": total, "active_sales_rows": active, "features": {}}
    for f in FIELDS:
        nz = int(r.get(f + "_nonzero") or 0)
        anz = int(r.get(f + "_active_nonzero") or 0)
        out["features"][f] = {
            "nonzero_rows": nz,
            "nonzero_rate_all_rows": rate(nz, total),
            "nonzero_rows_on_active_sales": anz,
            "nonzero_rate_on_active_sales_rows": rate(anz, active),
        }
    print("OVERALL=" + json.dumps(out, ensure_ascii=False))

    print("\n=== MONTHLY_SIGNAL_COVERAGE ===")
    # PyMySQL uses %-formatting internally; escape DATE_FORMAT percent signs as %%.
    month_expr = [
        "DATE_FORMAT(dt,'%%Y-%%m') AS ym",
        "COUNT(*) AS rows_n",
        "SUM(sales_units>0) AS active_sales_rows",
    ]
    for f in FIELDS:
        month_expr.append(f"SUM(COALESCE({f},0)<>0) AS {f}_nonzero")
        month_expr.append(
            f"SUM(sales_units>0 AND COALESCE({f},0)<>0) AS {f}_active_nonzero"
        )
    monthly = q(
        f"""
        SELECT {','.join(month_expr)}
        FROM `{DEST_TABLE}`
        GROUP BY DATE_FORMAT(dt,'%%Y-%%m')
        ORDER BY ym
        """
    )
    for r in monthly:
        total = int(r.get("rows_n") or 0)
        active = int(r.get("active_sales_rows") or 0)
        x = {
            "month": r.get("ym"),
            "rows": total,
            "active_sales_rows": active,
        }
        for f in FIELDS:
            nz = int(r.get(f + "_nonzero") or 0)
            anz = int(r.get(f + "_active_nonzero") or 0)
            x[f + "_all_rate"] = rate(nz, total)
            x[f + "_active_rate"] = rate(anz, active)
        print(json.dumps(x, ensure_ascii=False))

    print("\n判读：")
    print("1. nonzero_rate_all_rows 衡量该特征在全部SPU日记录里是否真正产生信号。")
    print("2. nonzero_rate_on_active_sales_rows 更适合判断建模价值：商品发生销售时，这个特征有多常可用。")
    print("3. avg_price 非零率低并不自动意味着不可用，应结合活跃销售行覆盖率。")
    print("4. Ads/Promotion 若长期接近0，应作为可选特征而非核心依赖。")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
