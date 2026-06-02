#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
290涤双磨 完整溯源导出脚本 v4
预估面料用量按颜色内SKU比例拆分，三层数字完全一致：
  SKU预估米加总 = 颜色维度预估米 = 面料预估汇总
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from common.database import db_cursor

today        = datetime.now().date()
cur_m_start  = today.replace(day=1).strftime('%Y-%m-%d')
prev_m_dt    = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
prev_m_start = prev_m_dt.strftime('%Y-%m-%d')
cur_m_prefix = today.strftime('%Y-%m')
days_in_prev = monthrange(prev_m_dt.year, prev_m_dt.month)[1]
days_elapsed = max(today.day - 1, 1)

def _label(y, m):
    return f"{str(y)[2:]}年{m}月"

cur_labels = []
y, m = today.year, today.month
for d in range(4):
    mm, yy = m + d, y
    while mm > 12: mm -= 12; yy += 1
    cur_labels.append(_label(yy, mm))

def _color_abbr(sku):
    parts = sku.split('-')
    return parts[1] if len(parts) >= 2 else ''

print("读取数据中...")

# ════════════════════════════════════════════════════════════════════════════
# 【第一层】面料预估汇总
# ════════════════════════════════════════════════════════════════════════════
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT 统计类型, 颜色缩写, `库存量/条`, `库存量/米`,
               `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
               `T+1月预估/米`, `T+2月预估/米`,
               `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`,
               当月月份, `T+1月份`, `T+2月份`
        FROM `面料预估表`
        WHERE 面料 = '290涤双磨'
        ORDER BY 统计类型, `当月完整预估/米` DESC
    """)
    df1 = pd.DataFrame(cur.fetchall())

# ════════════════════════════════════════════════════════════════════════════
# 【辅助A】面料预估表颜色维度：{颜色缩写: {月份标签: 预估米数}}
# ════════════════════════════════════════════════════════════════════════════
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT 颜色缩写, 当月月份,
               `当月完整预估/米` AS t0,
               `T+1月份`,        `T+1月预估/米` AS t1,
               `T+2月份`,        `T+2月预估/米` AS t2
        FROM `面料预估表`
        WHERE 面料 = '290涤双磨' AND 统计类型 = '带颜色'
    """)
    color_est_map = {}
    for row in cur.fetchall():
        c = (row.get('颜色缩写') or '').strip()
        if not c:
            continue
        color_est_map[c] = {
            row.get('当月月份', cur_labels[0]): float(row.get('t0') or 0),
            row.get('T+1月份',  cur_labels[1]): float(row.get('t1') or 0),
            row.get('T+2月份',  cur_labels[2]): float(row.get('t2') or 0),
        }

# ════════════════════════════════════════════════════════════════════════════
# 【第二层】SKU溯源
# ════════════════════════════════════════════════════════════════════════════

# Step A：系统预测（各月，跨店铺聚合）
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT
            p.SKU,
            SUBSTRING_INDEX(p.SKU, '-', 1) AS SPU,
            p.月份, p.统计日期,
            SUM(p.系统预测销量)             AS 系统预测件数,
            fk.单件用量, fk.单件损耗
        FROM `预测对比表_SKU` p
        INNER JOIN `面料核价表` fk
            ON CONVERT(fk.SPU USING utf8mb4)
             = SUBSTRING_INDEX(CONVERT(p.SKU USING utf8mb4), '-', 1)
            AND CONVERT(fk.面料 USING utf8mb4) = '290涤双磨'
        WHERE p.统计日期 >= %s AND p.系统预测销量 > 0
        GROUP BY p.SKU, p.月份, p.统计日期, fk.单件用量, fk.单件损耗
    """, (cur_m_start,))
    sys_rows = cur.fetchall()

# Step B：计算颜色内各SKU的系统预测总量（用于拆分比例）
# {(颜色缩写, 月份): 该颜色所有SKU的系统预测总量}
color_sys_total = defaultdict(int)
for row in sys_rows:
    color = _color_abbr(row['SKU'])
    color_sys_total[(color, row['月份'])] += int(row['系统预测件数'] or 0)

# Step C：建议下单量（SPU维度，供参考）
suggest_map = {}
with db_cursor(dictionary=True) as cur:
    try:
        col_parts = ", ".join(
            f"SUM(`{lbl}建议下单`) as `{lbl}建议下单`"
            for lbl in cur_labels
        )
        cur.execute(f"""
            SELECT SPU, {col_parts}
            FROM `建议下单量表`
            WHERE SPU IS NOT NULL AND SPU != ''
            GROUP BY SPU
        """)
        for row in cur.fetchall():
            spu = (row.get('SPU') or '').strip()
            for lbl in cur_labels:
                qty = int(row.get(f'{lbl}建议下单') or 0)
                if spu and qty > 0:
                    suggest_map[(spu, lbl)] = suggest_map.get((spu, lbl), 0) + qty
    except Exception as e:
        print(f"  警告：读取建议下单量表失败: {e}")

# Step D：销量+库存
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT p.SKU,
               MAX(COALESCE(s_prev.月销量, 0))                              AS T减1月销量,
               ROUND(MAX(COALESCE(s_prev.月销量, 0)) / %s, 2)             AS T减1月日均,
               MAX(COALESCE(s_curr.月销量, 0))                              AS T月已销量,
               ROUND(MAX(COALESCE(s_curr.月销量, 0)) / NULLIF(%s, 0), 2)  AS T月日均,
               MAX(COALESCE(li.本地可用量,  0))                             AS 成衣本地库存,
               MAX(COALESCE(li.本地待到货,  0))                             AS 成衣本地待到货,
               MAX(COALESCE(fi.fba可售,    0))                             AS 成衣FBA库存,
               MAX(COALESCE(fi.fba在途,    0))                             AS 成衣FBA在途,
               MAX(COALESCE(li.本地可用量,0))+MAX(COALESCE(li.本地待到货,0))
               +MAX(COALESCE(fi.fba可售,0))+MAX(COALESCE(fi.fba在途,0))   AS 全部有效库存
        FROM `预测对比表_SKU` p
        LEFT JOIN (SELECT SKU, SUM(销量) AS 月销量 FROM `销量统计_msku月度`
                   WHERE 统计日期=%s GROUP BY SKU
        ) s_prev ON CONVERT(s_prev.SKU USING utf8mb4)=CONVERT(p.SKU USING utf8mb4)
        LEFT JOIN (SELECT SKU, SUM(销量) AS 月销量 FROM `销量统计_msku月度`
                   WHERE 统计日期=%s GROUP BY SKU
        ) s_curr ON CONVERT(s_curr.SKU USING utf8mb4)=CONVERT(p.SKU USING utf8mb4)
        LEFT JOIN (SELECT SKU, SUM(可用量) AS 本地可用量, SUM(待到货量) AS 本地待到货
                   FROM `仓库库存明细` GROUP BY SKU
        ) li ON CONVERT(li.SKU USING utf8mb4)=CONVERT(p.SKU USING utf8mb4)
        LEFT JOIN (SELECT SKU, SUM(FBA可售) AS fba可售, SUM(在途) AS fba在途
                   FROM `FBA库存明细` GROUP BY SKU
        ) fi ON CONVERT(fi.SKU USING utf8mb4)=CONVERT(p.SKU USING utf8mb4)
        WHERE p.统计日期 >= %s AND p.系统预测销量 > 0
        GROUP BY p.SKU
    """, (days_in_prev, days_elapsed, prev_m_start, cur_m_start, cur_m_start))
    sales_inv_map = {row['SKU']: row for row in cur.fetchall()}

# Step E：组装记录
records = []
for row in sys_rows:
    sku     = row['SKU']
    spu     = row['SPU']
    month   = row['月份']
    sys_qty = int(row['系统预测件数'] or 0)
    unit_u  = float(row['单件用量'] or 0)
    unit_l  = float(row['单件损耗'] or 1.0)
    color   = _color_abbr(sku)

    sys_meters = round(sys_qty * unit_u * unit_l, 2)

    # ── 颜色内比例（跨SPU的同颜色SKU之间的比例）──────────────────────────
    color_total = color_sys_total.get((color, month), 0)
    color_ratio = round(sys_qty / color_total, 6) if color_total > 0 else 0.0

    # ── 面料预估表颜色总量 × 颜色内比例 = 该SKU的分摊预估米（与汇总一致）──
    color_total_meters = color_est_map.get(color, {}).get(month, 0)
    sku_est_meters     = round(color_total_meters * color_ratio, 2)

    # ── 建议下单量（供参考，仅SPU维度）───────────────────────────────────
    suggest_spu = suggest_map.get((spu, month), 0)

    si = sales_inv_map.get(sku, {})
    records.append({
        'SKU':                  sku,
        'SPU':                  spu,
        '颜色缩写':             color,
        '月份':                 month,
        # 系统预测（解释颜色比例来源）
        '系统预测件数':          sys_qty,
        '单件用量':              unit_u,
        '单件损耗':              unit_l,
        '系统预测面料米':        sys_meters,
        # 颜色内占比（核心：拆分面料预估到SKU的比例）
        '颜色内SKU占比':         color_ratio,
        '颜色预估总米数':        color_total_meters,  # 来自面料预估表
        '预估面料用量/米':       sku_est_meters,       # = 颜色总米 × 颜色内占比，与汇总一致
        # 建议下单（SPU维度参考）
        'SPU建议下单量':         suggest_spu,
        # 销量与库存
        'T减1月销量':            int(si.get('T减1月销量') or 0),
        'T减1月日均':            float(si.get('T减1月日均') or 0),
        'T月已销量':             int(si.get('T月已销量') or 0),
        'T月日均':               float(si.get('T月日均') or 0),
        '成衣本地库存':          int(si.get('成衣本地库存') or 0),
        '成衣本地待到货':        int(si.get('成衣本地待到货') or 0),
        '成衣FBA库存':           int(si.get('成衣FBA库存') or 0),
        '成衣FBA在途':           int(si.get('成衣FBA在途') or 0),
        '全部有效库存':          int(si.get('全部有效库存') or 0),
    })

df2 = pd.DataFrame(records).sort_values(
    ['月份', '颜色缩写', '系统预测件数'],
    ascending=[True, True, False]
)

# ════════════════════════════════════════════════════════════════════════════
# 【第三层】本月采购单
# ════════════════════════════════════════════════════════════════════════════
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT a.SKU,
               SUBSTRING_INDEX(CONVERT(a.SKU USING utf8mb4),'-',1) AS SPU,
               a.实际数量, fk.单件用量, fk.单件损耗,
               ROUND(a.实际数量*fk.单件用量*fk.单件损耗,2) AS 实际消耗米数,
               a.状态, a.创建时间
        FROM `采购单` a
        INNER JOIN `面料核价表` fk
            ON CONVERT(fk.SPU USING utf8mb4)
             = SUBSTRING_INDEX(CONVERT(a.SKU USING utf8mb4),'-',1)
            AND CONVERT(fk.面料 USING utf8mb4) = '290涤双磨'
        WHERE LEFT(a.创建时间,7)=%s
          AND CONVERT(a.状态 USING utf8mb4) IN ('待到货','已完成')
        ORDER BY 实际消耗米数 DESC
    """, (cur_m_prefix,))
    df3 = pd.DataFrame(cur.fetchall())

# ════════════════════════════════════════════════════════════════════════════
# 【第四层】核价参数
# ════════════════════════════════════════════════════════════════════════════
with db_cursor(dictionary=True) as cur:
    cur.execute("""
        SELECT SPU, 面料, 单件用量, 单件损耗,
               ROUND(单件用量*单件损耗,3) AS 实际用量系数,
               核价类型, 适用部位
        FROM `面料核价表`
        WHERE CONVERT(面料 USING utf8mb4) = '290涤双磨'
        ORDER BY 单件用量 DESC
    """)
    df4 = pd.DataFrame(cur.fetchall())

# ════════════════════════════════════════════════════════════════════════════
# 写入 Excel
# ════════════════════════════════════════════════════════════════════════════
out = '/tmp/fabric_290_trace.xlsx'
with pd.ExcelWriter(out, engine='openpyxl') as writer:
    df1.to_excel(writer, sheet_name='面料预估汇总',   index=False)
    df2.to_excel(writer, sheet_name='SKU溯源',        index=False)
    df3.to_excel(writer, sheet_name='本月采购单消耗', index=False)
    df4.to_excel(writer, sheet_name='核价参数',       index=False)

print(f"完成：{out}")
print(f"  面料预估汇总：{len(df1)} 行")
print(f"  SKU溯源：    {len(df2)} 行")
print(f"  本月采购单：  {len(df3)} 行")
print(f"  核价参数：    {len(df4)} 行")
