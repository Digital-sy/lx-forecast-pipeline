#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购建议报告
生成两张报表写入数据库：

【报表1】建议下单量表（给生产经理）
  维度：SPU + 店铺
  逻辑：建议下单量 = MAX(0, N月预测合计 - FBA可售 - FBA在途 - 本地库存 - 待到货)
  覆盖月数：定制面料=3个月，现货面料=2个月

【报表2】面料预计用量表（给产品经理，仅定制面料）
  维度：面料
  逻辑：预计用量 = Σ(建议下单量 × 单件用量)
"""

import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger('procurement_report')

# ── 覆盖周期配置 ──────────────────────────────────────────────────────────
COVERAGE_MONTHS_CUSTOM = 3   # 定制面料：3个月
COVERAGE_MONTHS_STOCK  = 2   # 现货面料：2个月

# ── 输出表名 ──────────────────────────────────────────────────────────────
TABLE_ORDER_SUGGEST = '建议下单量表'
TABLE_FABRIC_USAGE  = '面料预计用量表'


# ────────────────────────────────────────────────────────────────────────────
# 工具
# ────────────────────────────────────────────────────────────────────────────

def remove_psc_pattern(sku: str) -> str:
    if not sku:
        return sku
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    sku = re.sub(r'-+', '-', sku)
    return sku.strip('-')


def extract_spu(sku: str) -> str:
    if not sku:
        return ''
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    return sku[:idx] if idx > 0 else sku


# ────────────────────────────────────────────────────────────────────────────
# Step1：从预测对比表读取系统预测（已按SPU+店铺+月汇总）
# ────────────────────────────────────────────────────────────────────────────

def read_system_forecast() -> Tuple[
    Dict[Tuple[str, str], Dict[str, int]],   # {(SPU,shop): {月份: 预测量}}
    List[str]                                 # 月份顺序列表
]:
    """
    从预测对比表读取系统预测销量。
    返回：
      forecast_map: {(SPU, 店铺): {月份label: 系统预测销量}}
      month_order:  月份标签列表（按时间升序）
    """
    logger.info("读取系统预测数据（来自预测对比表）...")

    forecast_map: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    month_set = set()

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT SPU, 店铺, 月份, 统计日期, 系统预测销量
            FROM `预测对比表`
            ORDER BY 统计日期
        """)
        for row in cursor.fetchall():
            key = (row['SPU'].strip(), row['店铺'].strip())
            month = row['月份'].strip()
            forecast_map[key][month] = int(row['系统预测销量'] or 0)
            month_set.add((str(row['统计日期']), month))

    # 按统计日期排序，得到有序月份列表
    month_order = [m for _, m in sorted(month_set)]

    logger.info(f"读取完成：{len(forecast_map)} 个SPU+店铺，{len(month_order)} 个月份：{month_order}")
    return dict(forecast_map), month_order


# ────────────────────────────────────────────────────────────────────────────
# Step2：读取库存（FBA可售/FBA在途/本地库存/待到货），聚合到SPU+店铺
# ────────────────────────────────────────────────────────────────────────────

def read_inventory() -> Dict[Tuple[str, str], Dict[str, int]]:
    """
    读取四类库存，全部聚合到 SPU+店铺 维度。
    返回：{(SPU, 店铺): {
        'fba_sellable': FBA可售,
        'fba_transit':  FBA在途,
        'local':        本地可用量,
        'pending':      待到货(本地)
    }}
    """
    logger.info("读取库存数据...")

    inventory: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
        lambda: {'fba_sellable': 0, 'fba_transit': 0, 'local': 0, 'pending': 0}
    )

    # ── FBA可售 + FBA在途（来自FBA库存明细） ─────────────────────────────
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT SKU, 店铺,
                       SUM(COALESCE(`总可用库存`, 0)) AS fba_sellable,
                       SUM(COALESCE(`在途`, 0))       AS fba_transit
                FROM `FBA库存明细`
                WHERE SKU IS NOT NULL AND SKU != '' AND SKU != '无'
                  AND 店铺 IS NOT NULL AND 店铺 != '' AND 店铺 != '无'
                GROUP BY SKU, 店铺
            """)
            for row in cursor.fetchall():
                spu = extract_spu((row['SKU'] or '').strip())
                shop = (row['店铺'] or '').strip()
                if spu and shop:
                    key = (spu, shop)
                    inventory[key]['fba_sellable'] += int(row['fba_sellable'] or 0)
                    inventory[key]['fba_transit']  += int(row['fba_transit']  or 0)
        logger.info("FBA库存明细读取完成")
    except Exception as e:
        logger.warning(f"读取FBA库存明细失败: {e}")

    # ── 本地可用量 + 本地待到货（来自库存预估表，按库存状态区分） ─────────
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT sku, 店铺, 库存状态,
                       SUM(COALESCE(数量, 0)) AS 数量合计
                FROM `库存预估表`
                WHERE sku IS NOT NULL AND sku != '' AND sku != '无'
                  AND 库存状态 IN ('本地可用量', '本地待到货')
                GROUP BY sku, 店铺, 库存状态
            """)
            for row in cursor.fetchall():
                spu = extract_spu((row['sku'] or '').strip())
                shop = (row['店铺'] or '').strip()
                status = (row['库存状态'] or '').strip()
                qty = int(row['数量合计'] or 0)
                if spu:
                    # 本地库存店铺为"无"时，分摊到该SPU所有店铺（通用库存）
                    key = (spu, shop if shop and shop != '无' else '__ALL__')
                    if status == '本地可用量':
                        inventory[key]['local']   += qty
                    elif status == '本地待到货':
                        inventory[key]['pending'] += qty
        logger.info("库存预估表读取完成")
    except Exception as e:
        logger.warning(f"读取库存预估表失败: {e}")

    return dict(inventory)


def get_inventory(
    inventory_map: Dict[Tuple[str, str], Dict[str, int]],
    spu: str,
    shop: str,
) -> Dict[str, int]:
    """
    获取某SPU+店铺的库存，__ALL__通用库存会叠加进来。
    """
    result = {'fba_sellable': 0, 'fba_transit': 0, 'local': 0, 'pending': 0}
    for key in [(spu, shop), (spu, '__ALL__')]:
        if key in inventory_map:
            for k in result:
                result[k] += inventory_map[key].get(k, 0)
    return result


# ────────────────────────────────────────────────────────────────────────────
# Step3：面料类型 + 单件用量（SPU维度）
# ────────────────────────────────────────────────────────────────────────────

def read_fabric_info() -> Dict[str, Dict[str, Any]]:
    """
    读取 面料核价表 + 定制面料参数，
    返回：{SPU: {
        'fabric_type': '定制面料'|'现货面料',
        'fabrics': [(面料, 单件用量), ...]   # 按用量降序
    }}
    """
    logger.info("读取面料信息...")

    fabric_info: Dict[str, Dict[str, Any]] = {}

    try:
        with db_cursor() as cursor:
            # 定制面料名称集合
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '定制面料参数'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("定制面料参数表不存在")
                return fabric_info

            cursor.execute("SELECT DISTINCT 面料 FROM `定制面料参数` WHERE 面料 IS NOT NULL AND 面料 != ''")
            custom_fabrics = {row['面料'].strip() for row in cursor.fetchall()}

            # 面料核价表
            cursor.execute("""
                SELECT SPU, 面料, COALESCE(单件用量, 0) AS 单件用量
                FROM `面料核价表`
                WHERE SPU IS NOT NULL AND SPU != ''
                  AND 面料 IS NOT NULL AND 面料 != ''
            """)
            spu_fabrics: Dict[str, list] = defaultdict(list)
            for row in cursor.fetchall():
                spu = (row['SPU'] or '').strip()
                fabric = (row['面料'] or '').strip()
                usage = float(row['单件用量'] or 0)
                if spu and fabric:
                    spu_fabrics[spu].append((fabric, usage))

        for spu, fab_list in spu_fabrics.items():
            fab_list.sort(key=lambda x: x[1], reverse=True)
            dominant = fab_list[0][0]
            fabric_info[spu] = {
                'fabric_type': '定制面料' if dominant in custom_fabrics else '现货面料',
                'fabrics': fab_list,
            }

        custom_cnt = sum(1 for v in fabric_info.values() if v['fabric_type'] == '定制面料')
        logger.info(f"面料信息读取完成：{custom_cnt} 个定制SPU，{len(fabric_info)-custom_cnt} 个现货SPU")

    except Exception as e:
        logger.warning(f"读取面料信息失败: {e}")

    return fabric_info


# ────────────────────────────────────────────────────────────────────────────
# Step4：计算建议下单量，生成两张报表数据
# ────────────────────────────────────────────────────────────────────────────

def build_reports(
    forecast_map: Dict[Tuple[str, str], Dict[str, int]],
    month_order: List[str],
    inventory_map: Dict[Tuple[str, str], Dict[str, int]],
    fabric_info: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    计算每个SPU+店铺的建议下单量，并聚合面料用量。

    返回：
      order_records:  建议下单量表记录列表
      fabric_records: 面料预计用量表记录列表
    """
    logger.info("计算建议下单量...")

    order_records: List[Dict[str, Any]] = []
    # 面料用量聚合：{面料: {'建议下单量': x, '单件用量': y, SPU集合}}
    fabric_usage: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {'建议下单量合计': 0, '单件用量': 0.0, 'spu_set': set()}
    )

    for (spu, shop), monthly_forecast in forecast_map.items():
        # 面料类型 → 覆盖月数
        info = fabric_info.get(spu, {})
        fabric_type = info.get('fabric_type', '现货面料')
        n_months = COVERAGE_MONTHS_CUSTOM if fabric_type == '定制面料' else COVERAGE_MONTHS_STOCK

        # 取前N个月的预测合计
        selected_months = month_order[:n_months]
        forecast_total = sum(monthly_forecast.get(m, 0) for m in selected_months)

        # 各月预测（用于展示）
        month_detail = {m: monthly_forecast.get(m, 0) for m in month_order}

        # 库存
        inv = get_inventory(inventory_map, spu, shop)
        total_available = (
            inv['fba_sellable'] + inv['fba_transit'] +
            inv['local'] + inv['pending']
        )

        # 建议下单量
        suggested = max(0, forecast_total - total_available)

        record = {
            'SPU':          spu,
            '店铺':         shop,
            '面料类型':     fabric_type,
            '覆盖月数':     n_months,
            '预测合计':     forecast_total,
            'FBA可售':      inv['fba_sellable'],
            'FBA在途':      inv['fba_transit'],
            '本地库存':     inv['local'],
            '待到货':       inv['pending'],
            '库存合计':     total_available,
            '建议下单量':   suggested,
        }
        # 添加各月预测明细
        for m in month_order:
            record[f'{m}预测'] = month_detail.get(m, 0)

        order_records.append(record)

        # 面料用量（仅定制面料，且建议下单量>0）
        if fabric_type == '定制面料' and suggested > 0:
            for fabric, usage_per_unit in info.get('fabrics', []):
                fabric_usage[fabric]['建议下单量合计'] += suggested
                fabric_usage[fabric]['单件用量'] = usage_per_unit  # 取最新写入，同面料用量相同
                fabric_usage[fabric]['spu_set'].add(spu)

    # 整理面料用量表
    fabric_records: List[Dict[str, Any]] = []
    for fabric, data in sorted(fabric_usage.items(), key=lambda x: -x[1]['建议下单量合计']):
        total_order = data['建议下单量合计']
        usage = data['单件用量']
        fabric_records.append({
            '面料':         fabric,
            'SPU数量':      len(data['spu_set']),
            '建议下单量合计': total_order,
            '单件用量(米)': usage,
            '预计用量(米)': round(total_order * usage, 2),
        })

    logger.info(f"计算完成：{len(order_records)} 个SPU+店铺，{len(fabric_records)} 种定制面料")
    return order_records, fabric_records


# ────────────────────────────────────────────────────────────────────────────
# Step5：写入数据库
# ────────────────────────────────────────────────────────────────────────────

def save_order_suggest(records: List[Dict[str, Any]], month_order: List[str]) -> None:
    """建议下单量表：全量覆盖写入。"""
    with db_cursor() as cursor:
        # 动态建表（含各月预测列）
        month_cols = '\n'.join(
            f"    `{m}预测` INT NOT NULL DEFAULT 0,"
            for m in month_order
        )
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_ORDER_SUGGEST}` (
                `id`         INT AUTO_INCREMENT PRIMARY KEY,
                `SPU`        VARCHAR(200) NOT NULL,
                `店铺`       VARCHAR(200) NOT NULL,
                `面料类型`   VARCHAR(20)  NOT NULL,
                `覆盖月数`   TINYINT      NOT NULL,
                `预测合计`   INT          NOT NULL DEFAULT 0,
                `FBA可售`    INT          NOT NULL DEFAULT 0,
                `FBA在途`    INT          NOT NULL DEFAULT 0,
                `本地库存`   INT          NOT NULL DEFAULT 0,
                `待到货`     INT          NOT NULL DEFAULT 0,
                `库存合计`   INT          NOT NULL DEFAULT 0,
                `建议下单量` INT          NOT NULL DEFAULT 0,
                {month_cols}
                `更新时间`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_spu_shop (`SPU`, `店铺`),
                INDEX idx_fabric_type (`面料类型`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='采购建议下单量（生产经理用）';
        """)

        # 全量覆盖
        cursor.execute(f"TRUNCATE TABLE `{TABLE_ORDER_SUGGEST}`")

        if not records:
            return

        month_col_names = ', '.join(f'`{m}预测`' for m in month_order)
        month_placeholders = ', '.join(['%s'] * len(month_order))
        sql = f"""
            INSERT INTO `{TABLE_ORDER_SUGGEST}`
                (`SPU`, `店铺`, `面料类型`, `覆盖月数`,
                 `预测合计`, `FBA可售`, `FBA在途`, `本地库存`, `待到货`, `库存合计`, `建议下单量`,
                 {month_col_names})
            VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s, {month_placeholders})
        """
        rows = [
            (
                r['SPU'], r['店铺'], r['面料类型'], r['覆盖月数'],
                r['预测合计'], r['FBA可售'], r['FBA在途'], r['本地库存'],
                r['待到货'], r['库存合计'], r['建议下单量'],
                *[r.get(f'{m}预测', 0) for m in month_order],
            )
            for r in records
        ]
        BATCH = 500
        for i in range(0, len(rows), BATCH):
            cursor.executemany(sql, rows[i:i+BATCH])

    logger.info(f"✓ 写入 {len(records)} 条记录到 `{TABLE_ORDER_SUGGEST}`")


def save_fabric_usage(records: List[Dict[str, Any]]) -> None:
    """面料预计用量表：全量覆盖写入。"""
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_FABRIC_USAGE}` (
                `id`            INT AUTO_INCREMENT PRIMARY KEY,
                `面料`          VARCHAR(200) NOT NULL UNIQUE,
                `SPU数量`       INT          NOT NULL DEFAULT 0,
                `建议下单量合计` INT         NOT NULL DEFAULT 0,
                `单件用量(米)`  DECIMAL(8,3) NOT NULL DEFAULT 0,
                `预计用量(米)`  DECIMAL(12,2) NOT NULL DEFAULT 0,
                `更新时间`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                             ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='定制面料预计用量（产品经理用）';
        """)
        cursor.execute(f"TRUNCATE TABLE `{TABLE_FABRIC_USAGE}`")

        if not records:
            return

        sql = f"""
            INSERT INTO `{TABLE_FABRIC_USAGE}`
                (`面料`, `SPU数量`, `建议下单量合计`, `单件用量(米)`, `预计用量(米)`)
            VALUES (%s, %s, %s, %s, %s)
        """
        rows = [(r['面料'], r['SPU数量'], r['建议下单量合计'],
                 r['单件用量(米)'], r['预计用量(米)']) for r in records]
        cursor.executemany(sql, rows)

    logger.info(f"✓ 写入 {len(records)} 条记录到 `{TABLE_FABRIC_USAGE}`")


# ────────────────────────────────────────────────────────────────────────────
# 主函数
# ────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("采购建议报告生成")
    logger.info(f"  定制面料覆盖 {COVERAGE_MONTHS_CUSTOM} 个月 | 现货面料覆盖 {COVERAGE_MONTHS_STOCK} 个月")
    logger.info("=" * 70)

    # 1. 系统预测（来自预测对比表，已按SPU+店铺+月聚合）
    forecast_map, month_order = read_system_forecast()
    if not forecast_map:
        logger.error("预测对比表无数据，请先运行 generate_forecast_comparison.py")
        return

    # 2. 库存
    inventory_map = read_inventory()

    # 3. 面料类型 + 用量
    fabric_info = read_fabric_info()

    # 4. 计算
    order_records, fabric_records = build_reports(
        forecast_map, month_order, inventory_map, fabric_info
    )

    # 5. 写库
    save_order_suggest(order_records, month_order)
    save_fabric_usage(fabric_records)

    # 摘要
    custom_rows = [r for r in order_records if r['面料类型'] == '定制面料']
    stock_rows  = [r for r in order_records if r['面料类型'] == '现货面料']
    need_order  = [r for r in order_records if r['建议下单量'] > 0]

    logger.info("\n" + "=" * 70)
    logger.info("完成！")
    logger.info(f"  定制面料 SPU+店铺：{len(custom_rows)} 个")
    logger.info(f"  现货面料 SPU+店铺：{len(stock_rows)} 个")
    logger.info(f"  需要补单（建议下单量>0）：{len(need_order)} 个")
    logger.info(f"  定制面料种类（有用量需求）：{len(fabric_records)} 种")
    if fabric_records:
        top3 = fabric_records[:3]
        logger.info("  用量TOP3面料：")
        for r in top3:
            logger.info(f"    {r['面料']}: 预计用量 {r['预计用量(米)']:.1f} 米")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
