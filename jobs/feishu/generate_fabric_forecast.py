#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成面料预估表 v3（含运营预估）

【展示结构 - 横向滚动（每个面料/颜色一行）】
  库存侧：库存量/条、库存量/米、待到货量/条、待到货量/米
  消耗侧（系统预测）：
    当月已下单消耗/米  = 本月采购单(待到货+已完成) × 用量率
    当月完整预估/米    = 系统预测T月 × 用量率（A方案）
    当月剩余预估/米    = 完整预估 - 已下单消耗（B方案）
    T+1月预估/米 / T+2月预估/米 / T+3月预估/米
  消耗侧（运营预计）：
    运营当月预估/米 / 运营T+1月预估/米 / 运营T+2月预估/米 / 运营T+3月预估/米
"""

import sys
import re
import calendar
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger('fabric_forecast')

PURCHASE_VALID_STATUSES = ('待到货', '已完成')


# ────────────────────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────────────────────

def normalize_str(s: Any) -> str:
    if s is None:
        return ''
    return str(s).strip()


def remove_psc_pattern(sku: str) -> str:
    if not sku:
        return sku
    sku = re.sub(r'\d+(?:PSC|PCS)', '', sku, flags=re.IGNORECASE)
    sku = re.sub(r'-+', '-', sku)
    return sku.strip('-')


def extract_spu_from_sku(sku: str) -> str:
    if not sku:
        return ''
    sku = remove_psc_pattern(sku)
    idx = sku.find('-')
    return sku[:idx] if idx > 0 else sku


def extract_color_abbr_from_sku(sku: str) -> str:
    if not sku:
        return ''
    sku = remove_psc_pattern(sku)
    first = sku.find('-')
    if first < 0:
        return ''
    second = sku.find('-', first + 1)
    return sku[first + 1:second] if second > 0 else sku[first + 1:]


def _month_str(stat_date: Any) -> str:
    try:
        if isinstance(stat_date, str):
            dt = datetime.strptime(stat_date[:10], '%Y-%m-%d')
        else:
            dt = stat_date
        return f"{dt.strftime('%y')}年{dt.month}月"
    except Exception:
        return str(stat_date)[:7]


def get_month_date(year: int, month: int) -> str:
    return f"{year}-{month:02d}-01"


def add_months(year: int, month: int, delta: int) -> Tuple[int, int]:
    month += delta
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return year, month


# ────────────────────────────────────────────────────────────────────────────
# 数据读取
# ────────────────────────────────────────────────────────────────────────────

def get_fabric_params() -> Dict[str, Dict[str, Any]]:
    logger.info("读取定制面料参数...")
    result = {}
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT 面料, 面料编号, 米数每条
                FROM `定制面料参数`
                WHERE 面料 IS NOT NULL AND 面料 != ''
            """)
            for row in cur.fetchall():
                fabric = normalize_str(row.get('面料'))
                if fabric:
                    result[fabric] = {
                        '面料编号': normalize_str(row.get('面料编号')),
                        '米数每条': float(row.get('米数每条') or 0),
                    }
        logger.info(f"  读取到 {len(result)} 种定制面料")
    except Exception as e:
        logger.error(f"读取定制面料参数失败: {e}", exc_info=True)
    return result


def get_fabric_price_data() -> Dict[Tuple[str, str], Dict[str, Any]]:
    logger.info("读取面料核价表...")
    result = {}
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT SPU, 面料, 单件用量, 单件损耗
                FROM `面料核价表`
                WHERE SPU IS NOT NULL AND 面料 IS NOT NULL
            """)
            for row in cur.fetchall():
                spu    = normalize_str(row.get('SPU'))
                fabric = normalize_str(row.get('面料'))
                if spu and fabric:
                    result[(spu, fabric)] = {
                        '单件用量': float(row.get('单件用量') or 0),
                        '单件损耗': float(row.get('单件损耗') or 1.0),
                    }
        logger.info(f"  读取到 {len(result)} 个 SPU-面料组合")
    except Exception as e:
        logger.error(f"读取面料核价表失败: {e}", exc_info=True)
    return result


def get_purchase_order_data() -> Dict[str, int]:
    """本月采购单（待到货+已完成） → {SKU: 下单量}"""
    logger.info("读取本月采购单（待到货+已完成）...")
    result: Dict[str, int] = defaultdict(int)
    today = datetime.now()
    current_month_prefix = today.strftime('%Y-%m')
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT CONVERT(SKU USING utf8mb4) as SKU, SUM(实际数量) as qty
                FROM `采购单`
                WHERE LEFT(创建时间, 7) = %s
                  AND CONVERT(状态 USING utf8mb4) IN ('待到货', '已完成')
                  AND SKU IS NOT NULL AND SKU != ''
                  AND 实际数量 > 0
                GROUP BY SKU
            """, (current_month_prefix,))
            for row in cur.fetchall():
                sku = normalize_str(row.get('SKU'))
                qty = int(row.get('qty') or 0)
                if sku and qty > 0:
                    result[sku] += qty
        logger.info(f"  本月采购单：{len(result)} 个SKU，合计 {sum(result.values())} 件")
    except Exception as e:
        logger.warning(f"读取采购单失败: {e}，当月已下单消耗将为0")
    return dict(result)


def get_system_forecast_data() -> Dict[Tuple[str, str], int]:
    logger.info("读取预测对比表_SKU（系统预估）...")
    result: Dict[Tuple[str, str], int] = defaultdict(int)
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='预测对比表_SKU'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("预测对比表_SKU 不存在")
                return dict(result)
            cur.execute("""
                SELECT SKU, 统计日期, SUM(系统预测销量) as 总量
                FROM `预测对比表_SKU`
                WHERE SKU IS NOT NULL AND SKU != ''
                  AND 统计日期 IS NOT NULL AND 系统预测销量 > 0
                GROUP BY SKU, 统计日期
            """)
            for row in cur.fetchall():
                sku = normalize_str(row.get('SKU'))
                qty = int(row.get('总量') or 0)
                stat_date = row.get('统计日期')
                if isinstance(stat_date, str):
                    stat_date = stat_date[:10]
                elif hasattr(stat_date, 'strftime'):
                    stat_date = stat_date.strftime('%Y-%m-%d')
                else:
                    stat_date = str(stat_date)[:10]
                if sku and qty > 0:
                    result[(sku, stat_date)] += qty
        logger.info(f"  共 {len(result)} 个 SKU+日期组合有系统预测")
    except Exception as e:
        logger.error(f"读取预测对比表_SKU 失败: {e}", exc_info=True)
    return dict(result)


def get_suggest_order_data() -> Dict[Tuple[str, str], int]:
    """
    从建议下单量表读取 T+1~T+3 月的建议下单量（已扣除库存和在途）。
    合并所有店铺，返回：{(SPU, 月份标签 如'26年7月'): 建议下单量合计}
    """
    logger.info("读取建议下单量表（T~T+3月，含当月）...")
    result: Dict[Tuple[str, str], int] = defaultdict(int)
    today = datetime.now()

    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='建议下单量表'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("建议下单量表不存在，将继续用系统预测")
                return {}

            for delta in range(0, 4):   # T~T+3，包含当月
                y, m = add_months(today.year, today.month, delta)
                label = f"{str(y)[2:]}年{m}月"
                col   = f"`{label}建议下单`"
                try:
                    cur.execute(f"""
                        SELECT SPU, SUM({col}) as qty
                        FROM `建议下单量表`
                        WHERE SPU IS NOT NULL AND SPU != ''
                        GROUP BY SPU
                    """)
                    for row in cur.fetchall():
                        spu = normalize_str(row.get('SPU'))
                        qty = int(row.get('qty') or 0)
                        if spu and qty > 0:
                            result[(spu, label)] += qty
                except Exception as e:
                    logger.warning(f"  读取 {label} 建议下单失败（列可能不存在）: {e}")

        logger.info(f"  建议下单量：{len(result)} 个 SPU+月份组合")
    except Exception as e:
        logger.warning(f"读取建议下单量表失败: {e}")

    return dict(result)


def get_forecast_order_data() -> Dict[Tuple[str, str], int]:
    """运营预计下单表 → {(SKU, 统计日期): 运营预计下单量}"""
    logger.info("读取运营预计下单表...")
    result: Dict[Tuple[str, str], int] = defaultdict(int)
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT SKU, 统计日期, SUM(预计下单量) as qty
                FROM `运营预计下单表`
                WHERE SKU IS NOT NULL AND 统计日期 IS NOT NULL
                GROUP BY SKU, 统计日期
            """)
            for row in cur.fetchall():
                sku = normalize_str(row.get('SKU'))
                qty = int(row.get('qty') or 0)
                stat_date = row.get('统计日期')
                if isinstance(stat_date, str):
                    stat_date = stat_date[:10]
                elif hasattr(stat_date, 'strftime'):
                    stat_date = stat_date.strftime('%Y-%m-%d')
                else:
                    stat_date = str(stat_date)[:10]
                if sku and qty > 0:
                    result[(sku, stat_date)] = qty
        logger.info(f"  读取到 {len(result)} 个 SKU+日期")
    except Exception as e:
        logger.warning(f"读取运营预计下单表失败: {e}")
    return dict(result)


def get_fabric_color_merge_mapping() -> Dict[Tuple[str, str], str]:
    merge_map: Dict[Tuple[str, str], str] = {}
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料颜色归并对照'
            """)
            if not cur.fetchone().get('cnt', 0):
                return merge_map
            cur.execute("""
                SELECT 面料编号, 原始颜色缩写, 归并颜色缩写
                FROM `面料颜色归并对照`
                WHERE 面料编号!='' AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                  AND 是否启用=1
            """)
            for row in cur.fetchall():
                fc = normalize_str(row.get('面料编号'))
                rc = normalize_str(row.get('原始颜色缩写'))
                mc = normalize_str(row.get('归并颜色缩写'))
                if fc and rc and mc:
                    merge_map[(fc, rc)] = mc
        logger.info(f"  读取到 {len(merge_map)} 条颜色归并规则")
    except Exception as e:
        logger.warning(f"读取面料颜色归并对照失败: {e}")
    return merge_map


def get_merged_color_abbr(fabric_code: str, raw_color: str,
                          merge_map: Dict[Tuple[str, str], str]) -> str:
    return merge_map.get((normalize_str(fabric_code), normalize_str(raw_color)),
                         normalize_str(raw_color))


def get_color_map() -> Dict[str, str]:
    color_map: Dict[str, str] = {}
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='颜色对照'
            """)
            if not cur.fetchone().get('cnt', 0):
                return color_map
            for condition in ["新旧='新'", "新旧='旧'"]:
                cur.execute(f"""
                    SELECT 颜色缩写, 颜色中文 FROM `颜色对照`
                    WHERE 颜色缩写 IS NOT NULL AND 颜色缩写!=''
                      AND 颜色中文 IS NOT NULL AND 颜色中文!=''
                      AND {condition}
                """)
                for row in cur.fetchall():
                    abbr  = normalize_str(row.get('颜色缩写'))
                    cname = normalize_str(row.get('颜色中文'))
                    if abbr and abbr not in color_map:
                        color_map[abbr] = cname
    except Exception as e:
        logger.warning(f"读取颜色对照失败: {e}")
    return color_map


def get_inventory_data(
    merge_map: Dict[Tuple[str, str], str]
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    从「面料库存台账」读取颜色维度库存和待到货量。
 
    匹配键：面料编号颜色缩写（如 FAB-KNIT-JER-0017-BK）
    经 merge_map 颜色归并处理后存入 inventory / pending。
 
    Returns:
        inventory: {面料编号-颜色缩写: 库存成品数量_条}
        pending:   {面料编号-颜色缩写: 备货中数量_条}
    """
    logger.info("读取面料库存台账（飞书手工台账）...")
    inventory: Dict[str, int] = defaultdict(int)
    pending:   Dict[str, int] = defaultdict(int)
 
    # 颜色归并：原始 key → 归并后 key
    raw_to_merged = {
        f"{fc}-{rc}": f"{fc}-{mc}"
        for (fc, rc), mc in merge_map.items()
    }
 
    try:
        with db_cursor(dictionary=True) as cur:
            # 检查表是否存在
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '面料库存台账'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("面料库存台账表不存在，请先运行 fetch_fabric_inventory_from_feishu.py")
                return dict(inventory), dict(pending)
 
            cur.execute("""
                SELECT
                    面料编号颜色缩写,
                    库存成品数量_条,
                    备货中数量_条
                FROM `面料库存台账`
                WHERE 面料编号颜色缩写 IS NOT NULL
                  AND 面料编号颜色缩写 != ''
            """)
            rows = cur.fetchall()
            logger.info(f"  台账读取到 {len(rows)} 条颜色维度记录")
 
            for row in rows:
                key    = normalize_str(row.get('面料编号颜色缩写'))
                avail  = int(row.get('库存成品数量_条') or 0)
                pend   = int(row.get('备货中数量_条') or 0)
                if not key:
                    continue
                # 颜色归并
                merged = raw_to_merged.get(key, key)
                if avail > 0:
                    inventory[merged] += avail
                if pend > 0:
                    pending[merged]   += pend
 
        logger.info(f"  颜色维度库存 {len(inventory)} 条，待到货 {len(pending)} 条")
    except Exception as e:
        logger.error(f"读取面料库存台账失败: {e}", exc_info=True)
 
    return dict(inventory), dict(pending)


def get_inventory_by_fabric(
    inventory_data: Dict[str, int],
    pending_data:   Dict[str, int],
    fabric_params:  Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    将颜色维度库存聚合为面料总量维度。
 
    总量维度库存 = 库存成品数量_条 + 现有胚布数量_条（从台账重新读，不从颜色维度推算）
    总量维度待到货 = 各颜色 备货中数量_条 之和
 
    Returns:
        inv_by_fabric:  {面料名: 总库存条数（成品+胚布）}
        pend_by_fabric: {面料名: 总待到货条数}
    """
    inv_by_fabric:  Dict[str, int] = defaultdict(int)
    pend_by_fabric: Dict[str, int] = defaultdict(int)
 
    # 面料编号 → 面料名（用于聚合）
    code_to_name: Dict[str, str] = {}
    for fabric_name, info in fabric_params.items():
        code = info.get('面料编号', '').strip().upper()
        if code:
            code_to_name[code] = fabric_name
 
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '面料库存台账'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("面料库存台账表不存在，总量库存将为 0")
                return dict(inv_by_fabric), dict(pend_by_fabric)
 
            # 按面料编号（取 面料编号颜色缩写 的前半段）聚合
            # 面料编号颜色缩写 格式：{面料编号}-{颜色缩写}
            # 面料编号本身可能含"-"，但约定颜色缩写是最后一段（无"-"）
            # 所以用 SUBSTRING_INDEX(面料编号颜色缩写, '-', -1) 取颜色，
            # 用去掉末尾颜色段的剩余部分作为面料编号
            cur.execute("""
                SELECT
                    面料编号颜色缩写,
                    库存成品数量_条,
                    现有胚布数量_条,
                    备货中数量_条
                FROM `面料库存台账`
                WHERE 面料编号颜色缩写 IS NOT NULL
                  AND 面料编号颜色缩写 != ''
            """)
            rows = cur.fetchall()
 
            for row in rows:
                full_key   = normalize_str(row.get('面料编号颜色缩写'))
                stock_fg   = int(row.get('库存成品数量_条') or 0)
                stock_grey = int(row.get('现有胚布数量_条') or 0)
                pend       = int(row.get('备货中数量_条') or 0)
 
                if not full_key or '-' not in full_key:
                    continue
 
                # 面料编号 = 去掉最后一个"-颜色缩写"段
                fabric_code = full_key.rsplit('-', 1)[0].upper()
                fabric_name = code_to_name.get(fabric_code)
                if not fabric_name:
                    continue
 
                inv_by_fabric[fabric_name]  += (stock_fg + stock_grey)
                pend_by_fabric[fabric_name] += pend
 
        logger.info(f"  总量维度库存 {len(inv_by_fabric)} 种面料")
    except Exception as e:
        logger.error(f"聚合总量维度库存失败: {e}", exc_info=True)
 
    return dict(inv_by_fabric), dict(pend_by_fabric)


def get_primary_fabric_by_spu(
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]]
) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for (spu, fabric), data in fabric_usage.items():
        usage = data.get('单件用量', 0) or 0
        if spu not in result:
            result[spu] = fabric
        else:
            cur_usage = fabric_usage.get((spu, result[spu]), {}).get('单件用量', 0) or 0
            if usage > cur_usage:
                result[spu] = fabric
    return result


def calculate_average_usage_for_fabric(
    fabric_name: str,
    current_spu: str,
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]],
) -> Tuple[Optional[float], Optional[float]]:
    usage_list, loss_list = [], []
    for (spu, fabric), data in fabric_usage.items():
        if fabric == fabric_name and spu != current_spu:
            u = data.get('单件用量')
            l = data.get('单件损耗')
            if u and u > 0:
                usage_list.append(u)
                loss_list.append(l if l else 1.0)
    if not usage_list:
        return None, None
    return sum(usage_list) / len(usage_list), sum(loss_list) / len(loss_list)


def _calc_usage_meters(
    qty: int,
    unit_usage: Optional[float],
    unit_loss: Optional[float],
    fabric_name: str,
    spu: str,
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]],
) -> Tuple[float, bool]:
    missing = False
    if not unit_usage:
        missing = True
        avg_u, avg_l = calculate_average_usage_for_fabric(fabric_name, spu, fabric_usage)
        if avg_u and avg_u > 0:
            unit_usage = avg_u
            unit_loss  = avg_l if avg_l else 1.0
    if not unit_usage:
        return 0.0, missing
    return float(qty) * unit_usage * (unit_loss if unit_loss else 1.0), missing


# ────────────────────────────────────────────────────────────────────────────
# 核心生成逻辑
# ────────────────────────────────────────────────────────────────────────────

def generate_fabric_forecast(
    fabric_params:        Dict[str, Dict[str, Any]],
    fabric_usage:         Dict[Tuple[str, str], Dict[str, Any]],
    purchase_order_data:  Dict[str, int],
    system_forecast_data: Dict[Tuple[str, str], int],
    suggest_order_data:   Dict[Tuple[str, str], int],     # 建议下单量 {(SPU,月份标签): qty}
    forecast_data:        Dict[Tuple[str, str], int],
    inventory_data:       Dict[str, int],
    pending_data:         Dict[str, int],
    inv_by_fabric:        Dict[str, int],
    pend_by_fabric:       Dict[str, int],
    color_map:            Dict[str, str],
    merge_map:            Dict[Tuple[str, str], str],
) -> List[Dict[str, Any]]:
    """
    面料预估核心逻辑（v4）：
      当月(T)   → 采购单实际下单量（已实现，确定值）
      T+1~T+3  → 建议下单量（已扣除库存和在途，更准确）
                  按系统预测的 SKU 颜色比例拆分到颜色维度
                  若无建议下单量则兜底用系统预测
    """
    logger.info("开始生成面料预估数据（v4 建议下单量驱动T+1~T+3，采购单驱动当月）...")

    today        = datetime.now()
    cur_year     = today.year
    cur_month    = today.month
    current_time = today

    primary_fabric_by_spu = get_primary_fabric_by_spu(fabric_usage)

    # ── 预计算：每个 SPU+月份 的系统预测总量（用于颜色比例计算）────────────
    # {(SPU, delta): 系统预测总量}
    spu_sys_total: Dict[Tuple[str, int], int] = defaultdict(int)
    # {(SKU, delta): 系统预测量}
    sku_sys_qty:   Dict[Tuple[str, int], int] = {}

    for (sku, stat_date), qty in system_forecast_data.items():
        if qty <= 0:
            continue
        try:
            sd_dt = datetime.strptime(stat_date[:10], '%Y-%m-%d') \
                if isinstance(stat_date, str) else stat_date
            sd_year, sd_month = sd_dt.year, sd_dt.month
        except Exception:
            continue
        for d in range(4):
            y, m = add_months(cur_year, cur_month, d)
            if sd_year == y and sd_month == m:
                spu = extract_spu_from_sku(sku)
                if spu:
                    spu_sys_total[(spu, d)] += qty
                    sku_sys_qty[(sku, d)]    = qty
                break

    # ── 有效数量计算：T+1~T+3月用建议下单量，T月用系统预测（当月采购单单独处理）
    def _effective_qty(sku: str, delta: int) -> int:
        """
        所有月份（T~T+3）统一用建议下单量 × 颜色比例。
        无建议下单量时兜底用系统预测。
        """
        spu     = extract_spu_from_sku(sku)
        sys_qty = sku_sys_qty.get((sku, delta), 0)

        if not spu:
            return sys_qty

        y, m = add_months(cur_year, cur_month, delta)
        month_label = f"{str(y)[2:]}年{m}月"
        suggest_spu = suggest_order_data.get((spu, month_label), 0)

        if suggest_spu <= 0:
            return sys_qty   # 兜底：无建议下单量用系统预测

        spu_total = spu_sys_total.get((spu, delta), 0)
        if spu_total <= 0:
            return sys_qty   # 兜底：系统预测无数据则直接用建议下单量均分逻辑暂不实现

        ratio = sys_qty / spu_total
        return int(suggest_spu * ratio)

    def _empty_bucket():
        return {
            'purchase_m':  0.0,
            'sys_month_m': [0.0, 0.0, 0.0, 0.0],
            'op_month_m':  [0.0, 0.0, 0.0, 0.0],
            '缺失SPU':     set(),
        }

    total_agg: Dict[str, Dict]            = defaultdict(_empty_bucket)
    color_agg: Dict[Tuple[str,str], Dict] = defaultdict(_empty_bucket)

    # ── 步骤1：本月采购单 → 当月已下单消耗 ──────────────────────────────
    for sku, po_qty in purchase_order_data.items():
        spu = extract_spu_from_sku(sku)
        if not spu:
            continue
        for _, fabric_name, usage_data in [(s, f, d) for (s, f), d in fabric_usage.items() if s == spu]:
            if fabric_name not in fabric_params:
                continue
            meters, missing = _calc_usage_meters(
                po_qty, usage_data.get('单件用量'), usage_data.get('单件损耗'),
                fabric_name, spu, fabric_usage)
            if meters <= 0:
                continue
            total_agg[fabric_name]['purchase_m'] += meters
            if missing:
                total_agg[fabric_name]['缺失SPU'].add(spu)
            if primary_fabric_by_spu.get(spu) == fabric_name:
                color_abbr   = extract_color_abbr_from_sku(sku)
                fabric_code  = fabric_params[fabric_name].get('面料编号', '')
                merged_color = get_merged_color_abbr(fabric_code, color_abbr, merge_map)
                if merged_color:
                    color_agg[(fabric_name, merged_color)]['purchase_m'] += meters
                    if missing:
                        color_agg[(fabric_name, merged_color)]['缺失SPU'].add(spu)

    # ── 步骤2：系统预测（T月全量）+ 建议下单量（T+1~T+3）→ sys_month_m ──
    # 遍历所有有系统预测的 SKU，对每个 delta 取有效数量
    all_sku_deltas = set((sku, d) for (sku, d) in sku_sys_qty.keys())
    for (sku, delta) in all_sku_deltas:
        eff_qty = _effective_qty(sku, delta)
        if eff_qty <= 0:
            continue
        spu = extract_spu_from_sku(sku)
        if not spu:
            continue
        for _, fabric_name, usage_data in [(s, f, dd) for (s, f), dd in fabric_usage.items() if s == spu]:
            if fabric_name not in fabric_params:
                continue
            meters, missing = _calc_usage_meters(
                eff_qty, usage_data.get('单件用量'), usage_data.get('单件损耗'),
                fabric_name, spu, fabric_usage)
            if meters <= 0:
                continue
            total_agg[fabric_name]['sys_month_m'][delta] += meters
            if missing:
                total_agg[fabric_name]['缺失SPU'].add(spu)
            if primary_fabric_by_spu.get(spu) == fabric_name:
                color_abbr   = extract_color_abbr_from_sku(sku)
                fabric_code  = fabric_params[fabric_name].get('面料编号', '')
                merged_color = get_merged_color_abbr(fabric_code, color_abbr, merge_map)
                if merged_color:
                    color_agg[(fabric_name, merged_color)]['sys_month_m'][delta] += meters
                    if missing:
                        color_agg[(fabric_name, merged_color)]['缺失SPU'].add(spu)

    # ── 步骤3：运营预计 → op_month_m（逻辑不变，仅供参考）─────────────────
    def _process_op_forecast(source_data: Dict[Tuple[str, str], int]):
        for (sku, stat_date), qty in source_data.items():
            if qty <= 0:
                continue
            try:
                sd_dt = datetime.strptime(stat_date[:10], '%Y-%m-%d') \
                    if isinstance(stat_date, str) else stat_date
                sd_year, sd_month = sd_dt.year, sd_dt.month
            except Exception:
                continue
            delta = None
            for d in range(4):
                y, m = add_months(cur_year, cur_month, d)
                if sd_year == y and sd_month == m:
                    delta = d
                    break
            if delta is None:
                continue
            spu = extract_spu_from_sku(sku)
            if not spu:
                continue
            for _, fabric_name, usage_data in [(s, f, dd) for (s, f), dd in fabric_usage.items() if s == spu]:
                if fabric_name not in fabric_params:
                    continue
                meters, missing = _calc_usage_meters(
                    qty, usage_data.get('单件用量'), usage_data.get('单件损耗'),
                    fabric_name, spu, fabric_usage)
                if meters <= 0:
                    continue
                total_agg[fabric_name]['op_month_m'][delta] += meters
                if missing:
                    total_agg[fabric_name]['缺失SPU'].add(spu)
                if primary_fabric_by_spu.get(spu) == fabric_name:
                    color_abbr   = extract_color_abbr_from_sku(sku)
                    fabric_code  = fabric_params[fabric_name].get('面料编号', '')
                    merged_color = get_merged_color_abbr(fabric_code, color_abbr, merge_map)
                    if merged_color:
                        color_agg[(fabric_name, merged_color)]['op_month_m'][delta] += meters

    _process_op_forecast(forecast_data)

    # ── 步骤3：生成记录 ──────────────────────────────────────────────────
    result = []
    today_date = today.date()

    month_labels = []
    for d in range(4):
        y, m = add_months(cur_year, cur_month, d)
        month_labels.append(f"{str(y)[2:]}年{m}月")

    def _build_record(fabric_name, color_abbr, fcc, bucket, stat_type) -> Dict[str, Any]:
        fp  = fabric_params.get(fabric_name, {})
        mpr = fp.get('米数每条', 0.0)

        if stat_type == '总量':
            inv_rolls  = inv_by_fabric.get(fabric_name, 0)
            pend_rolls = pend_by_fabric.get(fabric_name, 0)
        else:
            inv_rolls  = inventory_data.get(fcc, 0)
            pend_rolls = pending_data.get(fcc, 0)

        purchase_m = round(bucket['purchase_m'], 2)
        sys_t0     = round(bucket['sys_month_m'][0], 2)
        sys_t1     = round(bucket['sys_month_m'][1], 2)
        sys_t2     = round(bucket['sys_month_m'][2], 2)
        sys_t3     = round(bucket['sys_month_m'][3], 2)
        op_t0      = round(bucket['op_month_m'][0],  2)
        op_t1      = round(bucket['op_month_m'][1],  2)
        op_t2      = round(bucket['op_month_m'][2],  2)
        op_t3      = round(bucket['op_month_m'][3],  2)

        sys_remain = round(max(0.0, sys_t0 - purchase_m), 2)

        return {
            '统计类型':          stat_type,
            'SKU':               '',
            'SPU':               '',
            '面料':              fabric_name,
            '面料编号':          fp.get('面料编号', ''),
            '颜色缩写':          color_abbr,
            '颜色':              color_map.get(color_abbr, ''),
            '面料颜色编号':      fcc,
            '统计日期':          today_date,
            '月份':              month_labels[0],
            # 库存侧
            '库存量/条':         inv_rolls,
            '库存量/米':         round(inv_rolls  * mpr, 2),
            '待到货量/条':       pend_rolls,
            '待到货量/米':       round(pend_rolls * mpr, 2),
            # 系统消耗侧
            '当月已下单消耗/米': purchase_m,
            '当月完整预估/米':   sys_t0,
            '当月剩余预估/米':   sys_remain,
            '当月月份':          month_labels[0],
            'T+1月预估/米':      sys_t1,
            'T+1月份':           month_labels[1],
            'T+2月预估/米':      sys_t2,
            'T+2月份':           month_labels[2],
            'T+3月预估/米':      sys_t3,
            'T+3月份':           month_labels[3],
            # 运营消耗侧
            '运营当月预估/米':   op_t0,
            '运营T+1月预估/米':  op_t1,
            '运营T+2月预估/米':  op_t2,
            '运营T+3月预估/米':  op_t3,
            # 辅助
            '用量信息缺失SPU':   ','.join(sorted(bucket['缺失SPU'])),
            '创建时间':          current_time,
            '更新时间':          current_time,
        }

    for fabric_name, bucket in total_agg.items():
        result.append(_build_record(fabric_name, '', '', bucket, '总量'))

    for (fabric_name, merged_color), bucket in color_agg.items():
        fabric_code = fabric_params.get(fabric_name, {}).get('面料编号', '')
        fcc = f"{fabric_code}-{merged_color}" if fabric_code else ''
        result.append(_build_record(fabric_name, merged_color, fcc, bucket, '带颜色'))

    logger.info(f"生成 {len(result)} 条记录（总量: {len(total_agg)}, 带颜色: {len(color_agg)}）")
    return result


# ────────────────────────────────────────────────────────────────────────────
# 表结构管理
# ────────────────────────────────────────────────────────────────────────────

def create_or_migrate_table() -> None:
    logger.info("检查/创建面料预估表（v3）...")
    try:
        with db_cursor(dictionary=False) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS `面料预估表` (
                    `id`                  INT AUTO_INCREMENT PRIMARY KEY,
                    `统计类型`            VARCHAR(20) NOT NULL DEFAULT '',
                    `SKU`                 VARCHAR(200) DEFAULT '',
                    `SPU`                 VARCHAR(100) DEFAULT '',
                    `面料`                VARCHAR(500) NOT NULL,
                    `面料编号`            VARCHAR(500) DEFAULT '',
                    `颜色缩写`            VARCHAR(100) DEFAULT '',
                    `颜色`                VARCHAR(100) DEFAULT '',
                    `面料颜色编号`        VARCHAR(500) DEFAULT '',
                    `统计日期`            DATE NOT NULL,
                    `月份`                VARCHAR(20) DEFAULT '',
                    `库存量/条`           DOUBLE DEFAULT 0,
                    `库存量/米`           DOUBLE DEFAULT 0,
                    `待到货量/条`         DOUBLE DEFAULT 0,
                    `待到货量/米`         DOUBLE DEFAULT 0,
                    `当月已下单消耗/米`   DOUBLE DEFAULT 0,
                    `当月完整预估/米`     DOUBLE DEFAULT 0,
                    `当月剩余预估/米`     DOUBLE DEFAULT 0,
                    `当月月份`            VARCHAR(20) DEFAULT '',
                    `T+1月预估/米`        DOUBLE DEFAULT 0,
                    `T+1月份`             VARCHAR(20) DEFAULT '',
                    `T+2月预估/米`        DOUBLE DEFAULT 0,
                    `T+2月份`             VARCHAR(20) DEFAULT '',
                    `T+3月预估/米`        DOUBLE DEFAULT 0,
                    `T+3月份`             VARCHAR(20) DEFAULT '',
                    `运营当月预估/米`     DOUBLE DEFAULT 0,
                    `运营T+1月预估/米`    DOUBLE DEFAULT 0,
                    `运营T+2月预估/米`    DOUBLE DEFAULT 0,
                    `运营T+3月预估/米`    DOUBLE DEFAULT 0,
                    `用量信息缺失SPU`     TEXT,
                    `创建时间`            DATETIME DEFAULT CURRENT_TIMESTAMP,
                    `更新时间`            DATETIME DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uk_type_fabric_color`
                        (`统计类型`(20), `面料`(100), `颜色缩写`(50)),
                    KEY `idx_fabric` (`面料`(100)),
                    KEY `idx_date`   (`统计日期`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='面料预估表 v3'
            """)
        logger.info("✓ 面料预估表（v3）已就绪")
    except Exception as e:
        logger.warning(f"建表失败: {e}，尝试继续")


# ────────────────────────────────────────────────────────────────────────────
# 写库
# ────────────────────────────────────────────────────────────────────────────

def save_fabric_forecast(records: List[Dict[str, Any]]) -> None:
    logger.info(f"保存 {len(records)} 条面料预估数据...")
    if not records:
        logger.warning("无数据需保存")
        return
    try:
        with db_cursor(dictionary=False) as cur:
            sql = """
                REPLACE INTO `面料预估表` (
                    `统计类型`, `SKU`, `SPU`, `面料`, `面料编号`,
                    `颜色缩写`, `颜色`, `面料颜色编号`, `统计日期`, `月份`,
                    `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                    `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`, `当月月份`,
                    `T+1月预估/米`, `T+1月份`,
                    `T+2月预估/米`, `T+2月份`,
                    `T+3月预估/米`, `T+3月份`,
                    `运营当月预估/米`, `运营T+1月预估/米`, `运营T+2月预估/米`, `运营T+3月预估/米`,
                    `用量信息缺失SPU`, `创建时间`, `更新时间`
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            batch_size = 200
            total = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                rows = [(
                    r['统计类型'], r['SKU'], r['SPU'], r['面料'], r['面料编号'],
                    r['颜色缩写'], r['颜色'], r['面料颜色编号'], r['统计日期'], r['月份'],
                    r['库存量/条'], r['库存量/米'], r['待到货量/条'], r['待到货量/米'],
                    r['当月已下单消耗/米'], r['当月完整预估/米'], r['当月剩余预估/米'], r['当月月份'],
                    r['T+1月预估/米'], r['T+1月份'],
                    r['T+2月预估/米'], r['T+2月份'],
                    r['T+3月预估/米'], r['T+3月份'],
                    r['运营当月预估/米'], r['运营T+1月预估/米'], r['运营T+2月预估/米'], r['运营T+3月预估/米'],
                    r['用量信息缺失SPU'], r['创建时间'], r['更新时间'],
                ) for r in batch]
                cur.executemany(sql, rows)
                total += len(batch)
                logger.info(f"  已写入 {total}/{len(records)} 条")
        logger.info(f"✓ 成功写入 {len(records)} 条数据")
    except Exception as e:
        logger.error(f"写入失败: {e}", exc_info=True)
        raise


# ────────────────────────────────────────────────────────────────────────────
# 主入口
# ────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 80)
    logger.info("面料预估表生成任务（v3 - 采购单驱动 + 系统/运营双口径横向滚动）")
    logger.info("=" * 80)

    create_or_migrate_table()

    fabric_params         = get_fabric_params()
    if not fabric_params:
        logger.warning("定制面料参数为空，终止")
        return

    fabric_usage          = get_fabric_price_data()
    purchase_order_data   = get_purchase_order_data()
    system_forecast_data  = get_system_forecast_data()
    suggest_order_data    = get_suggest_order_data()       # 新增：建议下单量
    forecast_data         = get_forecast_order_data()
    merge_map             = get_fabric_color_merge_mapping()
    color_map             = get_color_map()

    if not system_forecast_data and not purchase_order_data and not forecast_data:
        logger.warning("所有数据源均为空，终止")
        return

    inventory_data, pending_data = get_inventory_data(merge_map)
    inv_by_fabric, pend_by_fabric = get_inventory_by_fabric(
        inventory_data, pending_data, fabric_params
    )

    records = generate_fabric_forecast(
        fabric_params        = fabric_params,
        fabric_usage         = fabric_usage,
        purchase_order_data  = purchase_order_data,
        system_forecast_data = system_forecast_data,
        suggest_order_data   = suggest_order_data,         # 新增
        forecast_data        = forecast_data,
        inventory_data       = inventory_data,
        pending_data         = pending_data,
        inv_by_fabric        = inv_by_fabric,
        pend_by_fabric       = pend_by_fabric,
        color_map            = color_map,
        merge_map            = merge_map,
    )

    save_fabric_forecast(records)

    logger.info("=" * 80)
    logger.info("任务完成")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
