#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成面料预估表 v3

【核心逻辑改变】
  面料消耗来源 = 成衣采购单（状态=待到货/已完成）× 单件用量
  不是销量，因为下采购单的时刻就是面料消耗的时刻

【展示结构 - 横向滚动（每个面料/颜色一行）】
  库存侧：库存量/条、库存量/米、待到货量/条、待到货量/米
  消耗侧：
    当月已下单消耗/米  = 本月采购单(待到货+已完成) × 用量率  ← 确定值
    当月完整预估/米    = 系统预测T月 × 用量率               ← A方案（全月视角）
    当月剩余预估/米    = 完整预估 - 已下单消耗               ← B方案（剩余需求视角）
    T+1月预估/米       = 系统预测T+1月 × 用量率
    T+2月预估/米       = 系统预测T+2月 × 用量率
    T+3月预估/米       = 系统预测T+3月 × 用量率

【采购单状态过滤】
  纳入计算：待到货、已完成
  不计算：待下单、审批中
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
    """返回某月的1号日期字符串"""
    return f"{year}-{month:02d}-01"


def add_months(year: int, month: int, delta: int) -> Tuple[int, int]:
    """月份加减"""
    month += delta
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    return year, month


# ────────────────────────────────────────────────────────────────────────────
# 数据读取函数
# ────────────────────────────────────────────────────────────────────────────

def get_fabric_params() -> Dict[str, Dict[str, Any]]:
    """定制面料参数 → {面料名: {面料编号, 米数每条}}"""
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
    """面料核价表 → {(SPU, 面料): {单件用量, 单件损耗}}"""
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
    """
    【新增】从采购单表读取本月下单量（状态=待到货/已完成）
    返回：{SKU: 本月下单总量}
    """
    logger.info("读取本月采购单（待到货+已完成）...")
    result: Dict[str, int] = defaultdict(int)
    today = datetime.now()
    current_month_prefix = today.strftime('%Y-%m')

    try:
        with db_cursor(dictionary=True) as cur:
            # 使用 CONVERT 避免字符集冲突
            cur.execute("""
                SELECT
                    CONVERT(SKU USING utf8mb4) as SKU,
                    SUM(实际数量) as qty
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

        logger.info(f"  本月采购单：{len(result)} 个SKU，"
                    f"合计 {sum(result.values())} 件")
    except Exception as e:
        logger.warning(f"读取采购单失败: {e}，当月已下单消耗将为0")
    return dict(result)


def get_system_forecast_data() -> Dict[Tuple[str, str], int]:
    """预测对比表_SKU → {(SKU, 统计日期): 系统预测销量}"""
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
                  AND 统计日期 IS NOT NULL
                  AND 系统预测销量 > 0
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
    """面料颜色归并对照 → {(面料编号, 原始颜色缩写): 归并颜色缩写}"""
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
    fc = normalize_str(fabric_code)
    rc = normalize_str(raw_color)
    return merge_map.get((fc, rc), rc)


def get_color_map() -> Dict[str, str]:
    """颜色对照 → {颜色缩写: 颜色中文}"""
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
    """仓库库存明细 → 按归并后面料颜色编号聚合库存"""
    logger.info("读取仓库库存明细...")
    inventory: Dict[str, int] = defaultdict(int)
    pending:   Dict[str, int] = defaultdict(int)

    raw_to_merged = {
        f"{fc}-{rc}": f"{fc}-{mc}"
        for (fc, rc), mc in merge_map.items()
    }

    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='仓库库存明细'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("仓库库存明细表不存在")
                return dict(inventory), dict(pending)
            cur.execute("""
                SELECT SKU, SUM(可用量) as 可用, SUM(待到货量) as 待到货
                FROM `仓库库存明细`
                WHERE SKU IS NOT NULL AND SKU != ''
                GROUP BY SKU
            """)
            for row in cur.fetchall():
                sku   = normalize_str(row.get('SKU'))
                avail = int(row.get('可用') or 0)
                pend  = int(row.get('待到货') or 0)
                if sku:
                    merged = raw_to_merged.get(sku, sku)
                    if avail > 0:
                        inventory[merged] += avail
                    if pend > 0:
                        pending[merged]   += pend
        logger.info(f"  面料颜色编号库存 {len(inventory)} 条")
    except Exception as e:
        logger.error(f"读取库存明细失败: {e}", exc_info=True)
    return dict(inventory), dict(pending)


def get_inventory_by_fabric(
    inventory_data: Dict[str, int],
    pending_data:   Dict[str, int],
    fabric_params:  Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """将面料颜色编号维度的库存聚合到面料维度（去掉颜色）"""
    code_to_name: Dict[str, str] = {}
    for fabric_name, info in fabric_params.items():
        code = info.get('面料编号', '').strip().upper()
        if code:
            code_to_name[code] = fabric_name

    inv_by_fabric:  Dict[str, int] = defaultdict(int)
    pend_by_fabric: Dict[str, int] = defaultdict(int)

    def _match(sku: str) -> Optional[str]:
        sku_upper = sku.upper()
        for code, name in code_to_name.items():
            if sku_upper.startswith(code + '-'):
                return name
        return None

    for sku, qty in inventory_data.items():
        name = _match(sku)
        if name:
            inv_by_fabric[name] += qty
    for sku, qty in pending_data.items():
        name = _match(sku)
        if name:
            pend_by_fabric[name] += qty

    logger.info(f"  聚合到面料维度：库存 {len(inv_by_fabric)} 种面料")
    return dict(inv_by_fabric), dict(pend_by_fabric)


def get_primary_fabric_by_spu(
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]]
) -> Dict[str, str]:
    """获取每个SPU的主面料（单件用量最大的那块）"""
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
    """对用量缺失的SPU，用同款面料其他SPU的平均用量兜底"""
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
    """计算预计用量（米）。返回 (用量米数, 是否用量缺失)"""
    missing = False
    if not unit_usage:
        missing = True
        avg_u, avg_l = calculate_average_usage_for_fabric(fabric_name, spu, fabric_usage)
        if avg_u and avg_u > 0:
            unit_usage = avg_u
            unit_loss  = avg_l if avg_l else 1.0
    if not unit_usage:
        return 0.0, missing
    loss_factor = unit_loss if unit_loss else 1.0
    return float(qty) * unit_usage * loss_factor, missing


# ────────────────────────────────────────────────────────────────────────────
# 核心：生成面料预估数据（v3 横向滚动结构）
# ────────────────────────────────────────────────────────────────────────────

def generate_fabric_forecast(
    fabric_params:        Dict[str, Dict[str, Any]],
    fabric_usage:         Dict[Tuple[str, str], Dict[str, Any]],
    purchase_order_data:  Dict[str, int],           # 本月采购单下单量
    system_forecast_data: Dict[Tuple[str, str], int],  # 系统预测
    forecast_data:        Dict[Tuple[str, str], int],  # 运营预计
    inventory_data:       Dict[str, int],
    pending_data:         Dict[str, int],
    inv_by_fabric:        Dict[str, int],
    pend_by_fabric:       Dict[str, int],
    color_map:            Dict[str, str],
    merge_map:            Dict[Tuple[str, str], str],
) -> List[Dict[str, Any]]:
    """
    生成面料预估数据，每个面料/颜色组合一行，包含T到T+3月的滚动消耗预估。
    """
    logger.info("开始生成面料预估数据（v3 横向滚动）...")

    today       = datetime.now()
    cur_year    = today.year
    cur_month   = today.month
    current_time = today

    # T ~ T+3 月的日期字符串
    month_dates = []
    for delta in range(4):
        y, m = add_months(cur_year, cur_month, delta)
        month_dates.append(get_month_date(y, m))  # ['2026-06-01', '2026-07-01', ...]

    primary_fabric_by_spu = get_primary_fabric_by_spu(fabric_usage)

    # 聚合桶
    # 视角A（总量）：key = (面料,)
    # 视角B（带颜色）：key = (面料, 归并颜色缩写)
    # 每个桶包含：
    #   purchase_m    = 本月采购单消耗/米
    #   month_m[0..3] = T到T+3月系统预测消耗/米
    #   缺失SPU

    def _empty_bucket():
        return {
            'purchase_m': 0.0,          # 当月采购单消耗
            'month_m': [0.0, 0.0, 0.0, 0.0],  # T, T+1, T+2, T+3
            '缺失SPU': set(),
        }

    total_agg: Dict[str, Dict] = defaultdict(_empty_bucket)
    color_agg: Dict[Tuple[str, str], Dict] = defaultdict(_empty_bucket)

    # ── 步骤1：处理本月采购单 → 当月已下单消耗 ──────────────────────────
    for sku, po_qty in purchase_order_data.items():
        spu = extract_spu_from_sku(sku)
        if not spu:
            continue
        spu_fabrics = [(s, f, d) for (s, f), d in fabric_usage.items() if s == spu]
        if not spu_fabrics:
            continue

        for _, fabric_name, usage_data in spu_fabrics:
            if fabric_name not in fabric_params:
                continue
            unit_usage = usage_data.get('单件用量', 0)
            unit_loss  = usage_data.get('单件损耗', 1.0)
            meters, missing = _calc_usage_meters(
                po_qty, unit_usage, unit_loss, fabric_name, spu, fabric_usage)
            if meters <= 0:
                continue

            # 视角A
            total_agg[fabric_name]['purchase_m'] += meters
            if missing:
                total_agg[fabric_name]['缺失SPU'].add(spu)

            # 视角B：只统计主面料
            if primary_fabric_by_spu.get(spu) == fabric_name:
                color_abbr = extract_color_abbr_from_sku(sku)
                fabric_code = fabric_params[fabric_name].get('面料编号', '')
                merged_color = get_merged_color_abbr(fabric_code, color_abbr, merge_map)
                if merged_color:
                    color_agg[(fabric_name, merged_color)]['purchase_m'] += meters
                    if missing:
                        color_agg[(fabric_name, merged_color)]['缺失SPU'].add(spu)

    # ── 步骤2：处理系统预测 → T到T+3月消耗预估 ─────────────────────────
    for (sku, stat_date), sys_qty in system_forecast_data.items():
        if sys_qty <= 0:
            continue

        # 判断是T月还是T+1/T+2/T+3月
        try:
            if isinstance(stat_date, str):
                sd_dt = datetime.strptime(stat_date[:10], '%Y-%m-%d')
            else:
                sd_dt = stat_date
            sd_year, sd_month = sd_dt.year, sd_dt.month
        except Exception:
            continue

        # 找到对应的delta（0=当月, 1=T+1, 2=T+2, 3=T+3）
        delta = None
        for d in range(4):
            y, m = add_months(cur_year, cur_month, d)
            if sd_year == y and sd_month == m:
                delta = d
                break
        if delta is None:
            continue  # 不在T~T+3范围内，跳过

        spu = extract_spu_from_sku(sku)
        if not spu:
            continue
        spu_fabrics = [(s, f, dd) for (s, f), dd in fabric_usage.items() if s == spu]
        if not spu_fabrics:
            continue

        for _, fabric_name, usage_data in spu_fabrics:
            if fabric_name not in fabric_params:
                continue
            unit_usage = usage_data.get('单件用量', 0)
            unit_loss  = usage_data.get('单件损耗', 1.0)
            meters, missing = _calc_usage_meters(
                sys_qty, unit_usage, unit_loss, fabric_name, spu, fabric_usage)
            if meters <= 0:
                continue

            # 视角A
            total_agg[fabric_name]['month_m'][delta] += meters
            if missing:
                total_agg[fabric_name]['缺失SPU'].add(spu)

            # 视角B
            if primary_fabric_by_spu.get(spu) == fabric_name:
                color_abbr = extract_color_abbr_from_sku(sku)
                fabric_code = fabric_params[fabric_name].get('面料编号', '')
                merged_color = get_merged_color_abbr(fabric_code, color_abbr, merge_map)
                if merged_color:
                    color_agg[(fabric_name, merged_color)]['month_m'][delta] += meters
                    if missing:
                        color_agg[(fabric_name, merged_color)]['缺失SPU'].add(spu)

    # ── 步骤3：生成记录 ──────────────────────────────────────────────────
    result = []
    today_date = today.date()

    def _build_record(
        fabric_name: str,
        color_abbr:  str,
        merged_color_code: str,
        bucket: Dict,
        stat_type: str,
    ) -> Dict[str, Any]:
        fp   = fabric_params.get(fabric_name, {})
        mpr  = fp.get('米数每条', 0.0)
        code = fp.get('面料编号', '')

        # 库存（面料维度 或 面料颜色维度）
        if stat_type == '总量':
            inv_rolls  = inv_by_fabric.get(fabric_name, 0)
            pend_rolls = pend_by_fabric.get(fabric_name, 0)
        else:
            fcc = merged_color_code
            inv_rolls  = inventory_data.get(fcc, 0)
            pend_rolls = pending_data.get(fcc, 0)

        purchase_m = round(bucket['purchase_m'], 2)
        t0_m       = round(bucket['month_m'][0], 2)
        t1_m       = round(bucket['month_m'][1], 2)
        t2_m       = round(bucket['month_m'][2], 2)
        t3_m       = round(bucket['month_m'][3], 2)

        # 当月剩余预估 = 完整预估 - 已下单消耗（不低于0）
        t0_remain  = round(max(0.0, t0_m - purchase_m), 2)

        # 月份标签
        month_labels = []
        for d in range(4):
            y, m = add_months(cur_year, cur_month, d)
            month_labels.append(f"{str(y)[2:]}年{m}月")

        return {
            '统计类型':          stat_type,
            'SKU':               '',
            'SPU':               '',
            '面料':              fabric_name,
            '面料编号':          code,
            '颜色缩写':          color_abbr,
            '颜色':              color_map.get(color_abbr, ''),
            '面料颜色编号':      merged_color_code,
            '统计日期':          today_date,
            '月份':              month_labels[0],
            # 库存侧
            '库存量/条':         inv_rolls,
            '库存量/米':         round(inv_rolls  * mpr, 2),
            '待到货量/条':       pend_rolls,
            '待到货量/米':       round(pend_rolls * mpr, 2),
            # 消耗侧 - 当月
            '当月已下单消耗/米': purchase_m,
            '当月完整预估/米':   t0_m,       # A方案
            '当月剩余预估/米':   t0_remain,  # B方案
            '当月月份':          month_labels[0],
            # 消耗侧 - T+1 ~ T+3
            'T+1月预估/米':      t1_m,
            'T+1月份':           month_labels[1],
            'T+2月预估/米':      t2_m,
            'T+2月份':           month_labels[2],
            'T+3月预估/米':      t3_m,
            'T+3月份':           month_labels[3],
            # 兜底信息
            '用量信息缺失SPU':   ','.join(sorted(bucket['缺失SPU'])),
            '创建时间':          current_time,
            '更新时间':          current_time,
        }

    # 视角A：总量
    for fabric_name, bucket in total_agg.items():
        result.append(_build_record(fabric_name, '', '', bucket, '总量'))

    # 视角B：带颜色
    for (fabric_name, merged_color), bucket in color_agg.items():
        fabric_code = fabric_params.get(fabric_name, {}).get('面料编号', '')
        fcc = f"{fabric_code}-{merged_color}" if fabric_code else ''
        result.append(_build_record(fabric_name, merged_color, fcc, bucket, '带颜色'))

    logger.info(f"生成 {len(result)} 条记录"
                f"（总量: {len(total_agg)}, 带颜色: {len(color_agg)}）")
    return result


# ────────────────────────────────────────────────────────────────────────────
# 表结构管理
# ────────────────────────────────────────────────────────────────────────────

def create_or_migrate_table() -> None:
    """检查/创建面料预估表（v3 横向结构）"""
    logger.info("检查/创建面料预估表（v3）...")
    try:
        with db_cursor(dictionary=False) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS `面料预估表` (
                    `id`                INT AUTO_INCREMENT PRIMARY KEY,
                    `统计类型`          VARCHAR(20) NOT NULL DEFAULT '' COMMENT '总量/带颜色',
                    `SKU`               VARCHAR(200) DEFAULT '',
                    `SPU`               VARCHAR(100) DEFAULT '',
                    `面料`              VARCHAR(500) NOT NULL,
                    `面料编号`          VARCHAR(500) DEFAULT '',
                    `颜色缩写`          VARCHAR(100) DEFAULT '',
                    `颜色`              VARCHAR(100) DEFAULT '',
                    `面料颜色编号`      VARCHAR(500) DEFAULT '',
                    `统计日期`          DATE NOT NULL COMMENT '生成日期',
                    `月份`              VARCHAR(20) DEFAULT '',
                    -- 库存侧
                    `库存量/条`         DOUBLE DEFAULT 0,
                    `库存量/米`         DOUBLE DEFAULT 0,
                    `待到货量/条`       DOUBLE DEFAULT 0,
                    `待到货量/米`       DOUBLE DEFAULT 0,
                    -- 消耗侧（当月）
                    `当月已下单消耗/米` DOUBLE DEFAULT 0 COMMENT '本月采购单(待到货+已完成)折算的面料消耗',
                    `当月完整预估/米`   DOUBLE DEFAULT 0 COMMENT '系统预测T月销量折算的全月面料消耗（A方案）',
                    `当月剩余预估/米`   DOUBLE DEFAULT 0 COMMENT '完整预估-已下单消耗，剩余需采购面料（B方案）',
                    `当月月份`          VARCHAR(20) DEFAULT '',
                    -- 消耗侧（T+1 ~ T+3）
                    `T+1月预估/米`      DOUBLE DEFAULT 0,
                    `T+1月份`           VARCHAR(20) DEFAULT '',
                    `T+2月预估/米`      DOUBLE DEFAULT 0,
                    `T+2月份`           VARCHAR(20) DEFAULT '',
                    `T+3月预估/米`      DOUBLE DEFAULT 0,
                    `T+3月份`           VARCHAR(20) DEFAULT '',
                    -- 辅助
                    `用量信息缺失SPU`   TEXT,
                    `创建时间`          DATETIME DEFAULT CURRENT_TIMESTAMP,
                    `更新时间`          DATETIME DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uk_type_fabric_color`
                        (`统计类型`(20), `面料`(100), `颜色缩写`(50)),
                    KEY `idx_fabric` (`面料`(100)),
                    KEY `idx_date`   (`统计日期`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='面料预估表 v3'
            """)
        logger.info("✓ 面料预估表（v3）已就绪")
    except Exception as e:
        logger.warning(f"建表失败: {e}，尝试继续（表可能已存在）")


# ────────────────────────────────────────────────────────────────────────────
# 写库
# ────────────────────────────────────────────────────────────────────────────

def save_fabric_forecast(records: List[Dict[str, Any]]) -> None:
    """保存面料预估数据（全量替换）"""
    logger.info(f"保存 {len(records)} 条面料预估数据...")
    if not records:
        logger.warning("无数据需保存")
        return

    try:
        with db_cursor(dictionary=False) as cur:
            # 全量替换（REPLACE INTO 处理唯一键冲突）
            sql = """
                REPLACE INTO `面料预估表` (
                    `统计类型`, `SKU`, `SPU`, `面料`, `面料编号`,
                    `颜色缩写`, `颜色`, `面料颜色编号`, `统计日期`, `月份`,
                    `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                    `当月已下单消耗/米`, `当月完整预估/米`, `当月剩余预估/米`, `当月月份`,
                    `T+1月预估/米`, `T+1月份`,
                    `T+2月预估/米`, `T+2月份`,
                    `T+3月预估/米`, `T+3月份`,
                    `用量信息缺失SPU`, `创建时间`, `更新时间`
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
            """

            batch_size = 200
            total = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                rows = []
                for r in batch:
                    rows.append((
                        r['统计类型'], r['SKU'], r['SPU'], r['面料'], r['面料编号'],
                        r['颜色缩写'], r['颜色'], r['面料颜色编号'], r['统计日期'], r['月份'],
                        r['库存量/条'], r['库存量/米'], r['待到货量/条'], r['待到货量/米'],
                        r['当月已下单消耗/米'], r['当月完整预估/米'], r['当月剩余预估/米'], r['当月月份'],
                        r['T+1月预估/米'], r['T+1月份'],
                        r['T+2月预估/米'], r['T+2月份'],
                        r['T+3月预估/米'], r['T+3月份'],
                        r['用量信息缺失SPU'], r['创建时间'], r['更新时间'],
                    ))
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
    logger.info("面料预估表生成任务（v3 - 采购单驱动 + 横向滚动预估）")
    logger.info("=" * 80)

    create_or_migrate_table()

    fabric_params  = get_fabric_params()
    if not fabric_params:
        logger.warning("定制面料参数为空，终止")
        return

    fabric_usage          = get_fabric_price_data()
    purchase_order_data   = get_purchase_order_data()   # 【v3新增】本月采购单
    system_forecast_data  = get_system_forecast_data()
    forecast_data         = get_forecast_order_data()
    merge_map             = get_fabric_color_merge_mapping()
    color_map             = get_color_map()

    if not system_forecast_data and not purchase_order_data:
        logger.warning("系统预估和采购单均为空，终止")
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
