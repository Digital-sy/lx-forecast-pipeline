#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
面料溯源导出脚本（通用版）
支持多个面料，每个面料一组Sheet

用法：
  python -m jobs.feishu.export_fabric_trace [面料名1] [面料名2] ...
  python -m jobs.feishu.export_fabric_trace            # 默认：290涤双磨
  python -m jobs.feishu.export_fabric_trace 037超绒面料 "037超绒面料-优化"
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from common.database import db_cursor

# ── 参数处理 ──────────────────────────────────────────────────────────────
FABRICS = sys.argv[1:] if len(sys.argv) > 1 else ['290涤双磨']
print(f"目标面料：{FABRICS}")

# ── 日期计算 ───────────────────────────────────────────────────────────────
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

# ════════════════════════════════════════════════════════════════════════════
# 核心：按面料生成所有数据
# ════════════════════════════════════════════════════════════════════════════

def build_fabric_data(fabric_name: str) -> dict:
    """为单个面料生成溯源数据，返回各Sheet的DataFrame"""

    print(f"  处理：{fabric_name}...")

    # ── 第一层：面料预估汇总 ─────────────────────────────────────────────
    with db_cursor(dictionary=True) as cur:
        cur.execute("""
            SELECT 统计类型, 颜色缩写, `库存量/条`, `库存量/米`,
                   `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
                   `T+1月预估/米`, `T+2月预估/米`,
                   `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`,
                   当月月份, `T+1月份`, `T+2月份`
            FROM `面料预估表`
            WHERE 面料 = %s
            ORDER BY 统计类型, `当月完整预估/米` DESC
        """, (fabric_name,))
        df_summary = pd.DataFrame(cur.fetchall())

    # ── 辅助：面料预估表颜色维度 → {颜色缩写: {月份: 米数}} ──────────────
    with db_cursor(dictionary=True) as cur:
        cur.execute("""
            SELECT 颜色缩写, 当月月份,
                   `当月完整预估/米` AS t0,
                   `T+1月份`, `T+1月预估/米` AS t1,
                   `T+2月份`, `T+2月预估/米` AS t2
            FROM `面料预估表`
            WHERE 面料 = %s AND 统计类型 = '带颜色'
        """, (fabric_name,))
        color_est_map = {}
        for row in cur.fetchall():
            c = (row.get('颜色缩写') or '').strip()
            if c:
                color_est_map[c] = {
                    row.get('当月月份', cur_labels[0]): float(row.get('t0') or 0),
                    row.get('T+1月份',  cur_labels[1]): float(row.get('t1') or 0),
                    row.get('T+2月份',  cur_labels[2]): float(row.get('t2') or 0),
                }

    # ── 第二层：系统预测SKU溯源 ─────────────────────────────────────────
    with db_cursor(dictionary=True) as cur:
        cur.execute("""
            SELECT p.SKU,
                   SUBSTRING_INDEX(p.SKU, '-', 1) AS SPU,
                   p.月份, p.统计日期,
                   SUM(p.系统预测销量) AS 系统预测件数,
                   fk.单件用量, fk.单件损耗
            FROM `预测对比表_SKU` p
            INNER JOIN `面料核价表` fk
                ON CONVERT(fk.SPU USING utf8mb4)
                 = SUBSTRING_INDEX(CONVERT(p.SKU USING utf8mb4), '-', 1)
                AND CONVERT(fk.面料 USING utf8mb4) = %s
            WHERE p.统计日期 >= %s AND p.系统预测销量 > 0
            GROUP BY p.SKU, p.月份, p.统计日期, fk.单件用量, fk.单件损耗
        """, (fabric_name, cur_m_start))
        sys_rows = cur.fetchall()

    # 颜色内各SKU系统预测总量
    color_sys_total = defaultdict(int)
    for row in sys_rows:
        color_sys_total[(_color_abbr(row['SKU']), row['月份'])] += int(row['系统预测件数'] or 0)

    # ── 建议下单量（SPU维度） ────────────────────────────────────────────
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
            print(f"    警告：建议下单量读取失败: {e}")

    # ── 销量+库存（SKU维度） ─────────────────────────────────────────────
    with db_cursor(dictionary=True) as cur:
        cur.execute("""
            SELECT p.SKU,
                   MAX(COALESCE(s_prev.月销量,0))                              AS T减1月销量,
                   ROUND(MAX(COALESCE(s_prev.月销量,0))/%s,2)                AS T减1月日均,
                   MAX(COALESCE(s_curr.月销量,0))                              AS T月已销量,
                   ROUND(MAX(COALESCE(s_curr.月销量,0))/NULLIF(%s,0),2)      AS T月日均,
                   MAX(COALESCE(li.本地可用量,0))                              AS 成衣本地库存,
                   MAX(COALESCE(li.本地待到货,0))                              AS 成衣本地待到货,
                   MAX(COALESCE(fi.fba可售,0))                                AS 成衣FBA库存,
                   MAX(COALESCE(fi.fba在途,0))                                AS 成衣FBA在途,
                   MAX(COALESCE(li.本地可用量,0))+MAX(COALESCE(li.本地待到货,0))
                   +MAX(COALESCE(fi.fba可售,0))+MAX(COALESCE(fi.fba在途,0))  AS 全部有效库存
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
            WHERE p.统计日期>=%s AND p.系统预测销量>0
            GROUP BY p.SKU
        """, (days_in_prev, days_elapsed, prev_m_start, cur_m_start, cur_m_start))
        sales_inv_map = {row['SKU']: row for row in cur.fetchall()}

    # ── 组装SKU溯源 ──────────────────────────────────────────────────────
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

        color_total    = color_sys_total.get((color, month), 0)
        color_ratio    = round(sys_qty / color_total, 6) if color_total > 0 else 0.0
        color_total_m  = color_est_map.get(color, {}).get(month, 0)
        sku_est_meters = round(color_total_m * color_ratio, 2)

        suggest_spu = suggest_map.get((spu, month), 0)
        si = sales_inv_map.get(sku, {})

        records.append({
            'SKU':             sku, 'SPU': spu,
            '颜色缩写':        color, '月份': month,
            '系统预测件数':     sys_qty,
            '单件用量':         unit_u, '单件损耗': unit_l,
            '系统预测面料米':   sys_meters,
            '颜色内SKU占比':    color_ratio,
            '颜色预估总米数':   color_total_m,
            '预估面料用量/米':  sku_est_meters,
            'SPU建议下单量':    suggest_spu,
            'T减1月销量':       int(si.get('T减1月销量') or 0),
            'T减1月日均':       float(si.get('T减1月日均') or 0),
            'T月已销量':        int(si.get('T月已销量') or 0),
            'T月日均':          float(si.get('T月日均') or 0),
            '成衣本地库存':     int(si.get('成衣本地库存') or 0),
            '成衣本地待到货':   int(si.get('成衣本地待到货') or 0),
            '成衣FBA库存':      int(si.get('成衣FBA库存') or 0),
            '成衣FBA在途':      int(si.get('成衣FBA在途') or 0),
            '全部有效库存':     int(si.get('全部有效库存') or 0),
        })

    df_trace = pd.DataFrame(records).sort_values(
        ['月份', '颜色缩写', '系统预测件数'], ascending=[True, True, False]
    ) if records else pd.DataFrame()

    # ── 本月采购单 ───────────────────────────────────────────────────────
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
                AND CONVERT(fk.面料 USING utf8mb4) = %s
            WHERE LEFT(a.创建时间,7)=%s
              AND CONVERT(a.状态 USING utf8mb4) IN ('待到货','已完成')
            ORDER BY 实际消耗米数 DESC
        """, (fabric_name, cur_m_prefix))
        df_po = pd.DataFrame(cur.fetchall())

    # ── 核价参数 ─────────────────────────────────────────────────────────
    with db_cursor(dictionary=True) as cur:
        cur.execute("""
            SELECT SPU, 面料, 单件用量, 单件损耗,
                   ROUND(单件用量*单件损耗,3) AS 实际用量系数,
                   核价类型, 适用部位
            FROM `面料核价表`
            WHERE CONVERT(面料 USING utf8mb4) = %s
            ORDER BY 单件用量 DESC
        """, (fabric_name,))
        df_price = pd.DataFrame(cur.fetchall())

    print(f"    汇总{len(df_summary)}行 | 溯源{len(df_trace)}行 | 采购单{len(df_po)}行 | 核价{len(df_price)}行")

    return {
        'summary': df_summary,
        'trace':   df_trace,
        'po':      df_po,
        'price':   df_price,
    }

# ════════════════════════════════════════════════════════════════════════════
# 主流程：生成 Excel
# ════════════════════════════════════════════════════════════════════════════

# 文件名：多个面料时用第一个面料名+数量
if len(FABRICS) == 1:
    safe_name = FABRICS[0].replace('/', '_').replace(' ', '_')
else:
    safe_name = f"{FABRICS[0][:6]}等{len(FABRICS)}种面料"
out = f'/tmp/fabric_{safe_name}_trace.xlsx'

with pd.ExcelWriter(out, engine='openpyxl') as writer:
    for fabric in FABRICS:
        data = build_fabric_data(fabric)
        # Sheet名最多31字符（Excel限制）
        short = fabric[:12]
        data['summary'].to_excel(writer, sheet_name=f'{short}-汇总',   index=False)
        data['trace'].to_excel(  writer, sheet_name=f'{short}-SKU溯源', index=False)
        data['po'].to_excel(     writer, sheet_name=f'{short}-采购单',   index=False)
        data['price'].to_excel(  writer, sheet_name=f'{short}-核价',     index=False)

print(f"\n完成：{out}")
print(f"共 {len(FABRICS) * 4} 个Sheet")
