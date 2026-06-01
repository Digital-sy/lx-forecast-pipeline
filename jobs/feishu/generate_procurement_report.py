#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
采购建议报告生成

【报表1】建议下单量表（给生产经理）
  维度：SPU + 店铺 + 月份
  逻辑：建议下单量 = MAX(0, N月预测合计 - 库存 - 待到货)

【报表2】面料预计用量汇总（给产品经理）
  维度：面料
  逻辑：预计用量 = Σ(建议下单量 × 单件用量)

【报表3】面料预估明细（给面料跟单）
  来源：面料预估表（由 generate_fabric_forecast.py 生成）
  写入飞书多维表「面料预估明细」
"""

import asyncio
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

# ── 常量 ─────────────────────────────────────────────────────────────────
# 含义：第1个月权重最高，越往后越低，让建议下单量前置
COVERAGE_MONTHS_CUSTOM = 3   # 定制面料：覆盖未来3个月
COVERAGE_MONTHS_STOCK  = 2   # 现货面料：覆盖未来2个月

TABLE_ORDER_SUGGEST = '建议下单量表'
TABLE_FABRIC_USAGE  = '面料预计用量表'

# ── 飞书多维表配置 ────────────────────────────────────────────────────────
FEISHU_APP_TOKEN = "JvmNbfUp8atSpTsUH6Icyqk5nqd"


# ────────────────────────────────────────────────────────────────────────────
# Step1：读取系统预测数据（来自预测对比表）
# ────────────────────────────────────────────────────────────────────────────

def read_system_forecast() -> Tuple[Dict[Tuple[str, str], Dict[str, int]], List[str]]:
    """
    从预测对比表读取系统预测销量。
    返回：
      forecast_map: {(SPU, 店铺): {月份标签: 系统预测销量}}
      month_order: 月份标签列表（按时间顺序）
    """
    logger.info("读取系统预测数据（来自预测对比表）...")

    forecast_map: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    month_set = set()

    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT SPU, 店铺, 月份, 系统预测销量
                FROM `预测对比表`
                WHERE SPU IS NOT NULL AND SPU != ''
                  AND 店铺 IS NOT NULL AND 店铺 != ''
                  AND 系统预测销量 > 0
                ORDER BY 统计日期
            """)
            for row in cursor.fetchall():
                spu   = (row['SPU'] or '').strip()
                shop  = (row['店铺'] or '').strip()
                month = (row['月份'] or '').strip()
                qty   = int(row['系统预测销量'] or 0)
                if spu and shop and month:
                    forecast_map[(spu, shop)][month] = qty
                    month_set.add(month)
    except Exception as e:
        logger.error(f"读取预测对比表失败: {e}", exc_info=True)
        return {}, []

    # 按月份标签排序（格式 "26年4月"）
    def month_sort_key(label: str):
        try:
            import re
            m = re.match(r'(\d+)年(\d+)月', label)
            if m:
                return int(m.group(1)) * 100 + int(m.group(2))
        except Exception:
            pass
        return 0

    month_order = sorted(month_set, key=month_sort_key)
    logger.info(f"读取完成：{len(forecast_map)} 个SPU+店铺，{len(month_order)} 个月份：{month_order}")
    return dict(forecast_map), month_order


# ────────────────────────────────────────────────────────────────────────────
# Step2：读取库存数据
# ────────────────────────────────────────────────────────────────────────────

def read_inventory() -> Dict[Tuple[str, str], Dict[str, int]]:
    """读取库存数据（FBA + 本地），返回 {(SPU, 店铺): {库存, 待到货}}"""
    logger.info("读取库存数据...")

    inventory_map: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
        lambda: {'库存': 0, '待到货': 0}
    )

    try:
        with db_cursor() as cursor:
            # FBA库存
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='FBA库存明细'
            """)
            if cursor.fetchone().get('cnt', 0):
                cursor.execute("""
                    SELECT msku AS SKU, 店铺,
                           SUM(可售数量) AS 可售,
                           SUM(在途数量) AS 在途
                    FROM `FBA库存明细`
                    WHERE msku IS NOT NULL AND 店铺 IS NOT NULL
                    GROUP BY msku, 店铺
                """)
                for row in cursor.fetchall():
                    sku  = (row['SKU'] or '').strip()
                    shop = (row['店铺'] or '').strip()
                    spu  = _extract_spu(sku)
                    if spu and shop:
                        inventory_map[(spu, shop)]['库存']   += int(row['可售'] or 0)
                        inventory_map[(spu, shop)]['待到货'] += int(row['在途'] or 0)
            logger.info("FBA库存明细读取完成")

            # 库存预估表（本地仓库）
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='库存预估表'
            """)
            if cursor.fetchone().get('cnt', 0):
                cursor.execute("""
                    SELECT SKU, 店铺,
                           SUM(可用量) AS 可用,
                           SUM(待入库量) AS 待入库
                    FROM `库存预估表`
                    WHERE SKU IS NOT NULL AND 店铺 IS NOT NULL
                    GROUP BY SKU, 店铺
                """)
                for row in cursor.fetchall():
                    sku  = (row['SKU'] or '').strip()
                    shop = (row['店铺'] or '').strip()
                    spu  = _extract_spu(sku)
                    if spu and shop:
                        inventory_map[(spu, shop)]['库存']   += int(row['可用'] or 0)
                        inventory_map[(spu, shop)]['待到货'] += int(row['待入库'] or 0)
            logger.info("库存预估表读取完成")

    except Exception as e:
        logger.error(f"读取库存数据失败: {e}", exc_info=True)

    return dict(inventory_map)


def get_inventory(
    inventory_map: Dict[Tuple[str, str], Dict[str, int]],
    spu: str, shop: str
) -> Dict[str, int]:
    """获取某 SPU+店铺 的库存，若不存在返回 {库存:0, 待到货:0}"""
    # 先精确匹配
    if (spu, shop) in inventory_map:
        return inventory_map[(spu, shop)]
    # 跨店铺合并（同 SPU 所有店铺加总）
    total = {'库存': 0, '待到货': 0}
    for (s, sh), inv in inventory_map.items():
        if s == spu:
            total['库存']   += inv['库存']
            total['待到货'] += inv['待到货']
    return total


# ────────────────────────────────────────────────────────────────────────────
# Step3：其他辅助数据
# ────────────────────────────────────────────────────────────────────────────

def read_last_factory() -> Dict[Tuple[str, str], str]:
    """上次下单工厂（采购单里最新的供应商）"""
    logger.info("读取上次下单工厂...")
    factory_map: Dict[Tuple[str, str], str] = {}
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='采购单'
            """)
            if not cursor.fetchone().get('cnt', 0):
                return factory_map
            cursor.execute("""
                SELECT a.msku AS SKU, a.店铺, a.供应商
                FROM `采购单` a
                INNER JOIN (
                    SELECT msku, 店铺, MAX(创建时间) AS max_time
                    FROM `采购单`
                    WHERE msku IS NOT NULL AND 店铺 IS NOT NULL
                    GROUP BY msku, 店铺
                ) b ON a.msku = b.msku AND a.店铺 = b.店铺 AND a.创建时间 = b.max_time
            """)
            for row in cursor.fetchall():
                sku  = (row['SKU'] or '').strip()
                shop = (row['店铺'] or '').strip()
                spu  = _extract_spu(sku)
                if spu and shop:
                    factory_map[(spu, shop)] = (row['供应商'] or '').strip()
        logger.info(f"工厂信息读取完成：{len(factory_map)} 个SPU+店铺")
    except Exception as e:
        logger.warning(f"读取工厂数据失败: {e}")
    return factory_map


def read_op_forecast_by_month() -> Dict[Tuple[str, str], Dict[str, int]]:
    """运营预计下单量（按SPU+店铺+月汇总）"""
    logger.info("读取运营预计下单量...")
    result: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    try:
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='预测对比表'
            """)
            if not cursor.fetchone().get('cnt', 0):
                return {}
            cursor.execute("""
                SELECT SPU, 店铺, 月份, 运营预计下单量
                FROM `预测对比表`
                WHERE SPU IS NOT NULL AND 店铺 IS NOT NULL
                  AND 运营预计下单量 > 0
            """)
            for row in cursor.fetchall():
                spu   = (row['SPU'] or '').strip()
                shop  = (row['店铺'] or '').strip()
                month = (row['月份'] or '').strip()
                qty   = int(row['运营预计下单量'] or 0)
                if spu and shop and month:
                    result[(spu, shop)][month] = qty
        logger.info(f"运营预计下单量读取完成：{len(result)} 个SPU+店铺")
    except Exception as e:
        logger.warning(f"读取运营预计下单量失败: {e}")
    return dict(result)


def read_fabric_info() -> Dict[str, Dict[str, Any]]:
    """面料类型 + 用量信息"""
    logger.info("读取面料信息...")
    fabric_info: Dict[str, Dict[str, Any]] = {}

    try:
        with db_cursor() as cursor:
            # 定制面料列表
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='定制面料参数'
            """)
            custom_fabrics = set()
            if cursor.fetchone().get('cnt', 0):
                cursor.execute("SELECT 面料 FROM `定制面料参数` WHERE 面料 IS NOT NULL AND 面料!=''")
                custom_fabrics = {r['面料'].strip() for r in cursor.fetchall()}

            # 面料核价表
            cursor.execute("""
                SELECT SPU, 面料, COALESCE(单件用量, 0) AS 单件用量
                FROM `面料核价表`
                WHERE SPU IS NOT NULL AND SPU != ''
                  AND 面料 IS NOT NULL AND 面料 != ''
            """)
            spu_fabrics: Dict[str, list] = defaultdict(list)
            for row in cursor.fetchall():
                spu    = (row['SPU'] or '').strip()
                fabric = (row['面料'] or '').strip()
                usage  = float(row['单件用量'] or 0)
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
    factory_map: Dict[Tuple[str, str], str],
    op_forecast_map: Dict[Tuple[str, str], Dict[str, int]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    计算每个SPU+店铺的建议下单量，并聚合面料用量。
    建议下单量 = MAX(0, N月系统预测合计 - 库存 - 待到货)
    """
    logger.info("计算建议下单量...")

    order_records: List[Dict[str, Any]] = []
    fabric_usage: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {'建议下单量合计': 0, '单件用量': 0.0, 'spu_set': set()}
    )

    for (spu, shop), monthly_forecast in forecast_map.items():
        info = fabric_info.get(spu, {})
        fabric_type = info.get('fabric_type', '现货面料')
        n_months = COVERAGE_MONTHS_CUSTOM if fabric_type == '定制面料' else COVERAGE_MONTHS_STOCK

        selected_months = month_order[:n_months]
        forecast_total = sum(monthly_forecast.get(m, 0) for m in selected_months)

        inv     = get_inventory(inventory_map, spu, shop)
        stock   = inv['库存']
        pending = inv['待到货']

        total_4m_forecast = sum(monthly_forecast.get(m, 0) for m in month_order)
        suggested = max(0, forecast_total - stock - pending)

        # 各月建议下单：按预测比例分摊
        monthly_suggest: Dict[str, int] = {}
        if total_4m_forecast > 0 and suggested > 0:
            for m in month_order:
                ratio = monthly_forecast.get(m, 0) / total_4m_forecast
                monthly_suggest[m] = int(suggested * ratio)
        else:
            for m in month_order:
                monthly_suggest[m] = 0

        # 运营预计下单量（各月）
        op_monthly = op_forecast_map.get((spu, shop), {})
        op_total = sum(op_monthly.get(m, 0) for m in month_order)

        factory = factory_map.get((spu, shop), '')

        record: Dict[str, Any] = {
            'SPU':          spu,
            '店铺':         shop,
            '工厂':         factory,
            '面料类型':     fabric_type,
            '覆盖月数':     n_months,
            '建议下单合计': suggested,
            '运营预计合计': op_total,
            '库存':         stock,
            '待到货':       pending,
            '建议下单量':   suggested,
        }
        for m in month_order:
            record[f'{m}建议下单'] = monthly_suggest.get(m, 0)
            record[f'{m}运营预计'] = op_monthly.get(m, 0)

        order_records.append(record)

        # 聚合面料用量（只统计定制面料）
        if fabric_type == '定制面料' and suggested > 0:
            for fabric, unit_usage in info.get('fabrics', []):
                if unit_usage > 0:
                    fabric_usage[fabric]['建议下单量合计'] += suggested
                    fabric_usage[fabric]['单件用量'] = unit_usage
                    fabric_usage[fabric]['spu_set'].add(spu)

    logger.info(f"计算完成：{len(order_records)} 个SPU+店铺，{len(fabric_usage)} 种定制面料")

    fabric_records = []
    for fabric, data in sorted(fabric_usage.items(), key=lambda x: -x[1]['建议下单量合计']):
        total_order = data['建议下单量合计']
        unit_usage  = data['单件用量']
        fabric_records.append({
            '面料':           fabric,
            'SPU数量':        len(data['spu_set']),
            '建议下单量合计': total_order,
            '单件用量(米)':   unit_usage,
            '预计用量(米)':   round(total_order * unit_usage, 2),
        })

    return order_records, fabric_records


# ────────────────────────────────────────────────────────────────────────────
# Step5：写数据库
# ────────────────────────────────────────────────────────────────────────────

def save_order_suggest(records: List[Dict[str, Any]], month_order: List[str]) -> None:
    """建议下单量表：全量覆盖写入。"""
    with db_cursor() as cursor:
        month_cols = '\n'.join(
            f"    `{m}运营预计` INT NOT NULL DEFAULT 0,\n"
            f"    `{m}建议下单` INT NOT NULL DEFAULT 0,"
            for m in month_order
        )
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_ORDER_SUGGEST}` (
                `id`             INT AUTO_INCREMENT PRIMARY KEY,
                `SPU`            VARCHAR(200) NOT NULL,
                `店铺`           VARCHAR(200) NOT NULL,
                `工厂`           VARCHAR(200) NOT NULL DEFAULT '',
                `面料类型`       VARCHAR(20)  NOT NULL,
                `覆盖月数`       TINYINT      NOT NULL,
                `建议下单合计`   INT          NOT NULL DEFAULT 0,
                `运营预计合计`   INT          NOT NULL DEFAULT 0,
                `库存`           INT          NOT NULL DEFAULT 0,
                `待到货`         INT          NOT NULL DEFAULT 0,
                `建议下单量`     INT          NOT NULL DEFAULT 0,
                {month_cols}
                `更新时间`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                             ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_spu_shop (`SPU`, `店铺`),
                INDEX idx_fabric_type (`面料类型`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
              COMMENT='采购建议下单量（生产经理用）';
        """)

        cursor.execute(f"TRUNCATE TABLE `{TABLE_ORDER_SUGGEST}`")

        # 兜底：确保固定字段存在
        safe_alters = [
            f"ALTER TABLE `{TABLE_ORDER_SUGGEST}` ADD COLUMN IF NOT EXISTS `工厂` VARCHAR(200) NOT NULL DEFAULT '' AFTER `店铺`",
            f"ALTER TABLE `{TABLE_ORDER_SUGGEST}` ADD COLUMN IF NOT EXISTS `建议下单合计` INT NOT NULL DEFAULT 0 AFTER `覆盖月数`",
            f"ALTER TABLE `{TABLE_ORDER_SUGGEST}` ADD COLUMN IF NOT EXISTS `运营预计合计` INT NOT NULL DEFAULT 0 AFTER `建议下单合计`",
        ]
        for sql_alter in safe_alters:
            try:
                cursor.execute(sql_alter)
            except Exception:
                pass

        # 动态补月份列（表已存在时 CREATE IF NOT EXISTS 不会新增列）
        for m in month_order:
            for col, col_type in [
                (f'{m}运营预计', 'INT NOT NULL DEFAULT 0'),
                (f'{m}建议下单', 'INT NOT NULL DEFAULT 0'),
            ]:
                try:
                    cursor.execute(
                        f"ALTER TABLE `{TABLE_ORDER_SUGGEST}` ADD COLUMN `{col}` {col_type}"
                    )
                    logger.info(f"  新增列: {col}")
                except Exception as e:
                    if 'Duplicate column' not in str(e) and '1060' not in str(e):
                        pass  # 列已存在，忽略

        if not records:
            return

        month_col_names = ', '.join(
            f'`{m}运营预计`, `{m}建议下单`' for m in month_order
        )
        month_placeholders = ', '.join(['%s, %s'] * len(month_order))
        sql = f"""
            INSERT INTO `{TABLE_ORDER_SUGGEST}`
                (`SPU`, `店铺`, `工厂`, `面料类型`, `覆盖月数`,
                 `建议下单合计`, `运营预计合计`, `库存`, `待到货`, `建议下单量`,
                 {month_col_names})
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, {month_placeholders})
        """
        rows = []
        for r in records:
            month_vals = []
            for m in month_order:
                month_vals += [r.get(f'{m}运营预计', 0), r.get(f'{m}建议下单', 0)]
            rows.append((
                r['SPU'], r['店铺'], r['工厂'], r['面料类型'], r['覆盖月数'],
                r['建议下单合计'], r['运营预计合计'], r['库存'], r['待到货'], r['建议下单量'],
                *month_vals,
            ))

        BATCH = 500
        for i in range(0, len(rows), BATCH):
            cursor.executemany(sql, rows[i:i+BATCH])

    logger.info(f"✓ 写入 {len(records)} 条记录到 `{TABLE_ORDER_SUGGEST}`")


def save_fabric_usage(records: List[Dict[str, Any]]) -> None:
    """面料预计用量汇总表：全量覆盖写入。"""
    with db_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_FABRIC_USAGE}` (
                `id`             INT AUTO_INCREMENT PRIMARY KEY,
                `面料`           VARCHAR(500) NOT NULL,
                `SPU数量`        INT          NOT NULL DEFAULT 0,
                `建议下单量合计` INT          NOT NULL DEFAULT 0,
                `单件用量(米)`   DECIMAL(8,3) NOT NULL DEFAULT 0,
                `预计用量(米)`   DECIMAL(12,2) NOT NULL DEFAULT 0,
                `更新时间`       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
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
# Step6：写飞书多维表
# ────────────────────────────────────────────────────────────────────────────

async def _get_or_create_table(app_token: str, table_name: str, field_list: list, remove_extra: bool = False):
    """获取或创建飞书多维表，返回已设置好 table_id 的 FeishuClient。"""
    from common.feishu import FeishuClient
    client = FeishuClient(app_token=app_token, table_id="")
    table_id = await client.ensure_table_and_fields(
        table_name, field_list, remove_extra_fields=remove_extra
    )
    if not table_id:
        logger.warning(f"返回空 table_id，尝试按表名反查: {table_name}")
        tables = await client.get_tables()
        table_id = tables.get(table_name, '')
        if not table_id:
            raise RuntimeError(f"无法获取飞书表 '{table_name}' 的 table_id")
        logger.info(f"反查成功: {table_name} → {table_id}")
    client.table_id = table_id
    return client


async def write_order_to_feishu(records: List[Dict[str, Any]], month_order: List[str], current_date: datetime) -> None:
    """写建议下单量到飞书多维表，字段固定为 T/T+1/T+2/T+3"""
    stat_month = current_date.strftime('%Y-%m')
    field_list = [
        {'name': 'SPU',           'type': 'text'},
        {'name': '店铺',          'type': 'text'},
        {'name': '工厂',          'type': 'text'},
        {'name': '面料类型',      'type': 'text'},
        {'name': '统计月份',      'type': 'text'},
        {'name': 'T月建议下单',   'type': 'number'},
        {'name': 'T月运营预计',   'type': 'number'},
        {'name': 'T+1月建议下单', 'type': 'number'},
        {'name': 'T+1月运营预计', 'type': 'number'},
        {'name': 'T+2月建议下单', 'type': 'number'},
        {'name': 'T+2月运营预计', 'type': 'number'},
        {'name': 'T+3月建议下单', 'type': 'number'},
        {'name': 'T+3月运营预计', 'type': 'number'},
        {'name': '建议下单合计',  'type': 'number'},
        {'name': '运营预计合计',  'type': 'number'},
        {'name': '库存',          'type': 'number'},
        {'name': '待到货',        'type': 'number'},
    ]
    client = await _get_or_create_table(FEISHU_APP_TOKEN, '建议下单量', field_list, remove_extra=True)

    t_keys = ['T月', 'T+1月', 'T+2月', 'T+3月']
    feishu_records = []
    for r in records:
        row = {
            'SPU':          r['SPU'],
            '店铺':         r['店铺'],
            '工厂':         r['工厂'],
            '面料类型':     r['面料类型'],
            '统计月份':     stat_month,
            '建议下单合计': r['建议下单合计'],
            '运营预计合计': r['运营预计合计'],
            '库存':         r['库存'],
            '待到货':       r['待到货'],
        }
        for i, m in enumerate(month_order):
            tk = t_keys[i] if i < len(t_keys) else f'T+{i}月'
            row[f'{tk}建议下单'] = r.get(f'{m}建议下单', 0)
            row[f'{tk}运营预计'] = r.get(f'{m}运营预计', 0)
        feishu_records.append(row)

    await client.delete_all_records()
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书建议下单量表写入 {written} 条")


async def write_fabric_to_feishu(records: List[Dict[str, Any]], current_date: datetime) -> None:
    """写面料预计用量汇总到飞书多维表"""
    stat_month = current_date.strftime('%Y-%m')
    field_list = [
        {'name': '统计月份',       'type': 'text'},
        {'name': '面料',           'type': 'text'},
        {'name': 'SPU数量',        'type': 'number'},
        {'name': '建议下单量合计', 'type': 'number'},
        {'name': '单件用量(米)',    'type': 'number', 'precision': 2},
        {'name': '预计用量(米)',    'type': 'number', 'precision': 1},
    ]
    client = await _get_or_create_table(FEISHU_APP_TOKEN, '面料预计用量', field_list, remove_extra=True)

    await client.delete_all_records()
    feishu_records = [
        {
            '统计月份':       stat_month,
            '面料':           r['面料'],
            'SPU数量':        r['SPU数量'],
            '建议下单量合计': r['建议下单量合计'],
            '单件用量(米)':   r['单件用量(米)'],
            '预计用量(米)':   r['预计用量(米)'],
        }
        for r in records
    ]
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书面料预计用量表写入 {written} 条")


async def write_fabric_detail_to_feishu(current_date: datetime) -> None:
    """写面料预估明细到飞书多维表（v3 - 动态月份字段名，只显示T+0~T+2共3个月）"""
    from datetime import datetime as _dt

    # 动态生成月份标签，如 "6月"、"7月"、"8月"
    def _month_label(delta: int) -> str:
        y, m = current_date.year, current_date.month
        m += delta
        while m > 12:
            m -= 12
            y += 1
        return f"{m}月"

    m0 = _month_label(0)   # 当月，如 "6月"
    m1 = _month_label(1)   # T+1，如 "7月"
    m2 = _month_label(2)   # T+2，如 "8月"

    field_list = [
        {'name': '统计类型',                  'type': 'text'},
        {'name': '面料',                      'type': 'text'},
        {'name': '面料编号',                  'type': 'text'},
        {'name': '颜色缩写',                  'type': 'text'},
        {'name': '颜色',                      'type': 'text'},
        {'name': '面料颜色编号',              'type': 'text'},
        # 库存侧
        {'name': '库存量/条',                 'type': 'number', 'precision': 2},
        {'name': '库存量/米',                 'type': 'number', 'precision': 2},
        {'name': '待到货量/条',               'type': 'number', 'precision': 2},
        {'name': '待到货量/米',               'type': 'number', 'precision': 2},
        # 系统预测消耗侧（当月）
        {'name': f'{m0}已下单消耗/米',        'type': 'number', 'precision': 2},
        {'name': f'{m0}完整预估/米',          'type': 'number', 'precision': 2},
        {'name': f'{m0}剩余预估/米',          'type': 'number', 'precision': 2},
        # 系统预测消耗侧（T+1、T+2）
        {'name': f'{m1}预估/米',              'type': 'number', 'precision': 2},
        {'name': f'{m2}预估/米',              'type': 'number', 'precision': 2},
        # 运营预计消耗侧
        {'name': f'运营{m0}预估/米',          'type': 'number', 'precision': 2},
        {'name': f'运营{m1}预估/米',          'type': 'number', 'precision': 2},
        {'name': f'运营{m2}预估/米',          'type': 'number', 'precision': 2},
        # 辅助
        {'name': '用量信息缺失SPU',           'type': 'text'},
    ]
    client = await _get_or_create_table(FEISHU_APP_TOKEN, '面料预估明细', field_list, remove_extra=True)

    with db_cursor() as cursor:
        cursor.execute("""
            SELECT
                统计类型, 面料, 面料编号, 颜色缩写, 颜色, 面料颜色编号,
                `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`,
                `T+1月预估/米`, `T+2月预估/米`,
                `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`,
                用量信息缺失SPU
            FROM `面料预估表`
            ORDER BY 统计类型, 面料, 颜色缩写
        """)
        rows = cursor.fetchall()
    logger.info(f"面料预估明细：共 {len(rows)} 条")

    feishu_records = []
    for r in rows:
        feishu_records.append({
            '统计类型':               r['统计类型'] or '',
            '面料':                   r['面料'] or '',
            '面料编号':               r['面料编号'] or '',
            '颜色缩写':               r['颜色缩写'] or '',
            '颜色':                   r['颜色'] or '',
            '面料颜色编号':           r['面料颜色编号'] or '',
            '库存量/条':              float(r['库存量/条'] or 0),
            '库存量/米':              float(r['库存量/米'] or 0),
            '待到货量/条':            float(r['待到货量/条'] or 0),
            '待到货量/米':            float(r['待到货量/米'] or 0),
            f'{m0}已下单消耗/米':     float(r['当月已下单消耗/米'] or 0),
            f'{m0}完整预估/米':       float(r['当月完整预估/米'] or 0),
            f'{m0}剩余预估/米':       float(r['当月剩余预估/米'] or 0),
            f'{m1}预估/米':           float(r['T+1月预估/米'] or 0),
            f'{m2}预估/米':           float(r['T+2月预估/米'] or 0),
            f'运营{m0}预估/米':       float(r['运营当月预估/米'] or 0),
            f'运营{m1}预估/米':       float(r['运营T+1月预估/米'] or 0),
            f'运营{m2}预估/米':       float(r['运营T+2月预估/米'] or 0),
            '用量信息缺失SPU':        r['用量信息缺失SPU'] or '',
        })

    await client.delete_all_records()
    written = await client.write_records(feishu_records, batch_size=500)
    logger.info(f"✓ 飞书面料预估明细写入 {written} 条")

# ────────────────────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────────────────────

def _extract_spu(sku: str) -> str:
    import re
    if not sku:
        return ''
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    sku = re.sub(r'-+', '-', sku).strip('-')
    idx = sku.find('-')
    return sku[:idx] if idx > 0 else sku


# ────────────────────────────────────────────────────────────────────────────
# 主函数
# ────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 70)
    logger.info("采购建议报告生成")
    logger.info(f"  定制面料覆盖 {COVERAGE_MONTHS_CUSTOM} 个月 | 现货面料覆盖 {COVERAGE_MONTHS_STOCK} 个月")
    logger.info("=" * 70)

    current_date = datetime.now()

    # 1. 系统预测
    forecast_map, month_order = read_system_forecast()
    if not forecast_map:
        logger.error("预测对比表无数据，请先运行 generate_forecast_comparison.py")
        return

    # 2. 库存
    inventory_map = read_inventory()

    # 3. 工厂
    factory_map = read_last_factory()

    # 4. 运营预计下单量
    op_forecast_map = read_op_forecast_by_month()

    # 5. 面料类型 + 用量
    fabric_info = read_fabric_info()

    # 6. 计算
    order_records, fabric_records = build_reports(
        forecast_map, month_order, inventory_map,
        fabric_info, factory_map, op_forecast_map
    )

    # 7. 写数据库
    save_order_suggest(order_records, month_order)
    save_fabric_usage(fabric_records)

    # 8. 写飞书多维表
    logger.info("正在写入飞书多维表...")
    asyncio.run(write_order_to_feishu(order_records, month_order, current_date))
    asyncio.run(write_fabric_to_feishu(fabric_records, current_date))
    asyncio.run(write_fabric_detail_to_feishu(current_date))

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
