#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""290涤双磨 完整溯源导出脚本 - 输出 /tmp/fabric_290_trace.xlsx"""

import sys
from pathlib import Path
from datetime import datetime, date
from calendar import monthrange

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from common.database import db_cursor

# Python端预算日期，不在SQL里用DATE_FORMAT
today        = datetime.now().date()
cur_m_start  = today.replace(day=1).strftime('%Y-%m-%d')          # 2026-06-01
prev_m_start = (today.replace(day=1).replace(month=today.month-1) if today.month > 1
                else today.replace(day=1, year=today.year-1, month=12)).strftime('%Y-%m-%d')
cur_m_prefix = today.strftime('%Y-%m')                             # 2026-06
days_in_prev = monthrange(today.year, today.month-1 if today.month > 1 else 12)[1]
days_elapsed = today.day - 1  # 当月已过天数

print("读取数据中...")

# ── 第一层：面料预估汇总 ─────────────────────────────────────────────────
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT 统计类型, 颜色缩写, `库存量/条`, `库存量/米`,
               `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
               `T+1月预估/米`, `T+2月预估/米`,
               `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`
        FROM `面料预估表`
        WHERE 面料 = '290涤双磨'
        ORDER BY 统计类型, `当月完整预估/米` DESC
    """)
    df1 = pd.DataFrame(cur.fetchall())

# ── 第二层：系统预测SKU溯源（跨店铺聚合，每SKU每月一行）────────────────
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT
            p.SKU,
            SUBSTRING_INDEX(p.SKU, '-', 1)                              AS SPU,
            p.月份,
            SUM(p.系统预测销量)                                          AS 系统预测件数,
            fk.单件用量, fk.单件损耗,
            ROUND(SUM(p.系统预测销量) * fk.单件用量 * fk.单件损耗, 2)   AS 贡献面料米数,
            MAX(COALESCE(s_prev.月销量, 0))                              AS T减1月销量,
            ROUND(MAX(COALESCE(s_prev.月销量, 0)) / %s, 2)             AS T减1月日均,
            MAX(COALESCE(s_curr.月销量, 0))                              AS T月已销量,
            ROUND(MAX(COALESCE(s_curr.月销量, 0)) / NULLIF(%s, 0), 2)  AS T月日均,
            MAX(COALESCE(li.本地可用量,  0))                             AS 成衣本地库存,
            MAX(COALESCE(li.本地待到货,  0))                             AS 成衣本地待到货,
            MAX(COALESCE(fi.fba可售,    0))                             AS 成衣FBA库存,
            MAX(COALESCE(fi.fba在途,    0))                             AS 成衣FBA在途,
            MAX(COALESCE(li.本地可用量,  0)) + MAX(COALESCE(li.本地待到货, 0))
            + MAX(COALESCE(fi.fba可售, 0)) + MAX(COALESCE(fi.fba在途, 0))
                                                                         AS 全部有效库存
        FROM `预测对比表_SKU` p
        INNER JOIN `面料核价表` fk
            ON CONVERT(fk.SPU   USING utf8mb4)
             = SUBSTRING_INDEX(CONVERT(p.SKU USING utf8mb4), '-', 1)
            AND CONVERT(fk.面料 USING utf8mb4) = '290涤双磨'
        LEFT JOIN (
            SELECT SKU, SUM(销量) AS 月销量 FROM `销量统计_msku月度`
            WHERE 统计日期 = %s GROUP BY SKU
        ) s_prev ON CONVERT(s_prev.SKU USING utf8mb4) = CONVERT(p.SKU USING utf8mb4)
        LEFT JOIN (
            SELECT SKU, SUM(销量) AS 月销量 FROM `销量统计_msku月度`
            WHERE 统计日期 = %s GROUP BY SKU
        ) s_curr ON CONVERT(s_curr.SKU USING utf8mb4) = CONVERT(p.SKU USING utf8mb4)
        LEFT JOIN (
            SELECT SKU, SUM(可用量) AS 本地可用量, SUM(待到货量) AS 本地待到货
            FROM `仓库库存明细` GROUP BY SKU
        ) li ON CONVERT(li.SKU USING utf8mb4) = CONVERT(p.SKU USING utf8mb4)
        LEFT JOIN (
            SELECT SKU, SUM(FBA可售) AS fba可售, SUM(在途) AS fba在途
            FROM `FBA库存明细` GROUP BY SKU
        ) fi ON CONVERT(fi.SKU USING utf8mb4) = CONVERT(p.SKU USING utf8mb4)
        WHERE p.统计日期 >= %s AND p.系统预测销量 > 0
        GROUP BY p.SKU, p.月份, p.统计日期, fk.单件用量, fk.单件损耗
        ORDER BY 贡献面料米数 DESC
    """, (days_in_prev, days_elapsed, prev_m_start, cur_m_start, cur_m_start))
    df2 = pd.DataFrame(cur.fetchall())

# ── 第三层：本月采购单实际消耗 ───────────────────────────────────────────
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT a.SKU,
               SUBSTRING_INDEX(CONVERT(a.SKU USING utf8mb4), '-', 1) AS SPU,
               a.实际数量, fk.单件用量, fk.单件损耗,
               ROUND(a.实际数量 * fk.单件用量 * fk.单件损耗, 2) AS 实际消耗米数,
               a.状态, a.创建时间
        FROM `采购单` a
        INNER JOIN `面料核价表` fk
            ON CONVERT(fk.SPU   USING utf8mb4)
             = SUBSTRING_INDEX(CONVERT(a.SKU USING utf8mb4), '-', 1)
            AND CONVERT(fk.面料 USING utf8mb4) = '290涤双磨'
        WHERE LEFT(a.创建时间, 7) = %s
          AND CONVERT(a.状态 USING utf8mb4) IN ('待到货', '已完成')
        ORDER BY 实际消耗米数 DESC
    """, (cur_m_prefix,))
    df3 = pd.DataFrame(cur.fetchall())

# ── 第四层：核价参数 ──────────────────────────────────────────────────────
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT SPU, 面料, 单件用量, 单件损耗,
               ROUND(单件用量 * 单件损耗, 3) AS 实际用量系数,
               核价类型, 适用部位
        FROM `面料核价表`
        WHERE CONVERT(面料 USING utf8mb4) = '290涤双磨'
        ORDER BY 单件用量 DESC
    """)
    df4 = pd.DataFrame(cur.fetchall())

# ── 写入 Excel ────────────────────────────────────────────────────────────
out = '/tmp/fabric_290_trace.xlsx'
with pd.ExcelWriter(out, engine='openpyxl') as writer:
    df1.to_excel(writer, sheet_name='面料预估汇总',    index=False)
    df2.to_excel(writer, sheet_name='系统预测SKU溯源', index=False)
    df3.to_excel(writer, sheet_name='本月采购单消耗',  index=False)
    df4.to_excel(writer, sheet_name='核价参数',        index=False)

print(f"完成：{out}")
print(f"  面料预估汇总：    {len(df1)} 行")
print(f"  系统预测SKU溯源： {len(df2)} 行（已聚合店铺）")
print(f"  本月采购单消耗：  {len(df3)} 行")
print(f"  核价参数：        {len(df4)} 行")
