#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
生成面料预估表（新版）

两种统计类型写入同一张表，用 `统计类型` 字段区分：

  统计类型='总量'
    - 范围：所有面料（定制 + 现货）
    - 维度：面料 + 月份（无颜色）
    - 库存：按面料编号前缀聚合仓库库存明细中所有颜色的库存
    - 用途：让面料跟单同事看下个月各面料总用量

  统计类型='带颜色'
    - 范围：每个 SPU 的主面料（单件用量最大），且该主面料是定制面料
    - 维度：面料 + 归并颜色缩写 + 月份
    - 库存：按面料颜色编号精确匹配（保留原有逻辑）
    - 用途：定制面料按颜色跟单

两种类型均新增：系统预估下单量、系统预估用量/米、系统预估用量/条
  来源：预测对比表_SKU（SKU + 月份维度的系统预测销量）
"""

import sys
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from common import get_logger
from common.database import db_cursor

logger = get_logger('fabric_forecast')


# ────────────────────────────────────────────────────────────────────────────
# SKU 工具函数
# ────────────────────────────────────────────────────────────────────────────

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
    parts = sku.split('-')
    if len(parts) < 2:
        return ''
    if len(parts) >= 3 and parts[1].upper() in ['LONG', 'SHORT']:
        return parts[2] if len(parts) >= 3 else ''
    return parts[1]


def normalize_str(val) -> str:
    if val is None:
        return ''
    return str(val).strip().upper()


# ────────────────────────────────────────────────────────────────────────────
# 数据读取
# ────────────────────────────────────────────────────────────────────────────

def get_fabric_params() -> Dict[str, Dict[str, Any]]:
    """定制面料参数表 → {面料名: {面料编号, 米数每条, ...}}"""
    logger.info("读取定制面料参数表...")
    result = {}
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='定制面料参数'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("定制面料参数表不存在")
                return result
            cursor.execute("""
                SELECT 面料, 面料编号, 米数每条, 公斤数每条
                FROM `定制面料参数`
                WHERE 面料 IS NOT NULL AND 面料 != ''
            """)
            for row in cursor.fetchall():
                name = (row['面料'] or '').strip()
                if name:
                    result[name] = {
                        '面料编号':   (row['面料编号'] or '').strip(),
                        '米数每条':   float(row['米数每条'] or 0),
                        '公斤数每条': float(row['公斤数每条'] or 0),
                    }
        logger.info(f"  共 {len(result)} 种定制面料")
    except Exception as e:
        logger.error(f"读取定制面料参数失败: {e}", exc_info=True)
    return result


def get_fabric_price_data() -> Dict[Tuple[str, str], Dict[str, Any]]:
    """面料核价表 → {(SPU, 面料): {单件用量, 单件损耗}}"""
    logger.info("读取面料核价表...")
    result = {}
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料核价表'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("面料核价表不存在")
                return result
            cursor.execute("""
                SELECT SPU, 面料, 单件用量, 单件损耗
                FROM `面料核价表`
                WHERE SPU IS NOT NULL AND SPU!=''
                  AND 面料 IS NOT NULL AND 面料!=''
            """)
            for row in cursor.fetchall():
                spu    = (row['SPU'] or '').strip()
                fabric = (row['面料'] or '').strip()
                if spu and fabric:
                    result[(spu, fabric)] = {
                        '单件用量': float(row['单件用量']) if row['单件用量'] else None,
                        '单件损耗': float(row['单件损耗']) if row['单件损耗'] else None,
                    }
        logger.info(f"  共 {len(result)} 个 SPU-面料组合")
    except Exception as e:
        logger.error(f"读取面料核价表失败: {e}", exc_info=True)
    return result


def get_primary_fabric_by_spu(
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]]
) -> Dict[str, str]:
    """每个 SPU 取单件用量最大的面料作为主面料。返回 {SPU: 面料名}"""
    spu_fabrics: Dict[str, list] = defaultdict(list)
    for (spu, fabric), data in fabric_usage.items():
        usage = data.get('单件用量') or 0
        spu_fabrics[spu].append((usage, fabric))
    primary = {}
    for spu, lst in spu_fabrics.items():
        lst.sort(key=lambda x: x[0], reverse=True)
        primary[spu] = lst[0][1]
    logger.info(f"  确定了 {len(primary)} 个 SPU 的主面料")
    return primary


def get_forecast_order_data() -> Dict[Tuple[str, str], int]:
    """运营预计下单表 → {(SKU, 统计日期): 下单量}"""
    logger.info("读取运营预计下单表...")
    result: Dict[Tuple[str, str], int] = defaultdict(int)
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='运营预计下单表'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("运营预计下单表不存在")
                return dict(result)
            cursor.execute("""
                SELECT SKU, 统计日期, SUM(预计下单量) as 总量
                FROM `运营预计下单表`
                WHERE SKU IS NOT NULL AND SKU!=''
                  AND 统计日期 IS NOT NULL
                  AND 预计下单量 > 0
                GROUP BY SKU, 统计日期
            """)
            for row in cursor.fetchall():
                sku = (row['SKU'] or '').strip()
                qty = int(row['总量'] or 0)
                stat_date = row['统计日期']
                if isinstance(stat_date, str):
                    stat_date = stat_date[:10]
                elif hasattr(stat_date, 'strftime'):
                    stat_date = stat_date.strftime('%Y-%m-%d')
                else:
                    stat_date = str(stat_date)[:10]
                if sku and qty > 0:
                    result[(sku, stat_date)] += qty
        logger.info(f"  共 {len(result)} 个 SKU+日期组合")
    except Exception as e:
        logger.error(f"读取运营预计下单表失败: {e}", exc_info=True)
    return dict(result)


def get_system_forecast_data() -> Dict[Tuple[str, str], int]:
    """
    预测对比表_SKU → {(SKU, 统计日期): 系统预测销量}
    跨店铺合并（同一 SKU+月份 多个店铺的预测加总）
    """
    logger.info("读取预测对比表_SKU（系统预估）...")
    result: Dict[Tuple[str, str], int] = defaultdict(int)
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='预测对比表_SKU'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("预测对比表_SKU 不存在，系统预估用量将为0")
                return dict(result)
            cursor.execute("""
                SELECT SKU, 统计日期, SUM(系统预测销量) as 总量
                FROM `预测对比表_SKU`
                WHERE SKU IS NOT NULL AND SKU!=''
                  AND 统计日期 IS NOT NULL
                  AND 系统预测销量 > 0
                GROUP BY SKU, 统计日期
            """)
            for row in cursor.fetchall():
                sku = (row['SKU'] or '').strip()
                qty = int(row['总量'] or 0)
                stat_date = row['统计日期']
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


def get_fabric_color_merge_mapping() -> Dict[Tuple[str, str], str]:
    """面料颜色归并对照 → {(面料编号, 原始颜色缩写): 归并颜色缩写}"""
    merge_map: Dict[Tuple[str, str], str] = {}
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='面料颜色归并对照'
            """)
            if not cursor.fetchone().get('cnt', 0):
                return merge_map
            cursor.execute("""
                SELECT 面料编号, 原始颜色缩写, 归并颜色缩写
                FROM `面料颜色归并对照`
                WHERE 面料编号!='' AND 原始颜色缩写!='' AND 归并颜色缩写!=''
                  AND 是否启用=1
            """)
            for row in cursor.fetchall():
                fc = normalize_str(row.get('面料编号'))
                rc = normalize_str(row.get('原始颜色缩写'))
                mc = normalize_str(row.get('归并颜色缩写'))
                if fc and rc and mc:
                    merge_map[(fc, rc)] = mc
    except Exception as e:
        logger.error(f"读取颜色归并对照失败: {e}", exc_info=True)
    return merge_map


def get_merged_color_abbr(
    fabric_code: str,
    raw_color: str,
    merge_map: Dict[Tuple[str, str], str]
) -> str:
    fc = normalize_str(fabric_code)
    rc = normalize_str(raw_color)
    return merge_map.get((fc, rc), rc)


def get_inventory_data(
    merge_map: Dict[Tuple[str, str], str]
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """仓库库存明细 → 按归并后面料颜色编号聚合"""
    logger.info("读取仓库库存明细（按面料颜色编号）...")
    inventory: Dict[str, int] = defaultdict(int)
    pending:   Dict[str, int] = defaultdict(int)
    raw_to_merged = {
        f"{fc}-{rc}": f"{fc}-{mc}"
        for (fc, rc), mc in merge_map.items()
    }
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='仓库库存明细'
            """)
            if not cursor.fetchone().get('cnt', 0):
                logger.warning("仓库库存明细表不存在")
                return dict(inventory), dict(pending)
            cursor.execute("""
                SELECT SKU, SUM(可用量) as 可用, SUM(待到货量) as 待到货
                FROM `仓库库存明细`
                WHERE SKU IS NOT NULL AND SKU!=''
                GROUP BY SKU
            """)
            for row in cursor.fetchall():
                sku   = (row['SKU'] or '').strip()
                avail = int(row['可用'] or 0)
                pend  = int(row['待到货'] or 0)
                if sku:
                    merged = raw_to_merged.get(sku, sku)
                    if avail > 0:
                        inventory[merged] += avail
                    if pend > 0:
                        pending[merged] += pend
        logger.info(f"  面料颜色编号库存 {len(inventory)} 条")
    except Exception as e:
        logger.error(f"读取库存明细失败: {e}", exc_info=True)
    return dict(inventory), dict(pending)


def get_inventory_by_fabric(
    inventory_data: Dict[str, int],
    pending_data:   Dict[str, int],
    fabric_params:  Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """将按面料颜色编号的库存聚合到面料维度（去掉颜色）"""
    code_to_name: Dict[str, str] = {}
    for fabric_name, info in fabric_params.items():
        code = info.get('面料编号', '').strip()
        if code:
            code_to_name[code.upper()] = fabric_name

    inv_by_fabric:  Dict[str, int] = defaultdict(int)
    pend_by_fabric: Dict[str, int] = defaultdict(int)

    def _match_fabric(sku: str) -> Optional[str]:
        sku_upper = sku.upper()
        for code, name in code_to_name.items():
            if sku_upper.startswith(code + '-'):
                return name
        return None

    for sku, qty in inventory_data.items():
        name = _match_fabric(sku)
        if name:
            inv_by_fabric[name] += qty
    for sku, qty in pending_data.items():
        name = _match_fabric(sku)
        if name:
            pend_by_fabric[name] += qty

    logger.info(f"  聚合到面料维度：库存 {len(inv_by_fabric)} 种面料")
    return dict(inv_by_fabric), dict(pend_by_fabric)


def get_color_map() -> Dict[str, str]:
    """颜色对照 → {颜色缩写: 颜色中文}"""
    color_map: Dict[str, str] = {}
    try:
        with db_cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='颜色对照'
            """)
            if not cursor.fetchone().get('cnt', 0):
                return color_map
            for condition in ["新旧='新'", "新旧='旧'"]:
                cursor.execute(f"""
                    SELECT 颜色缩写, 颜色中文 FROM `颜色对照`
                    WHERE 颜色缩写 IS NOT NULL AND 颜色缩写!=''
                      AND 颜色中文 IS NOT NULL AND 颜色中文!=''
                      AND {condition}
                """)
                for row in cursor.fetchall():
                    abbr  = (row['颜色缩写'] or '').strip()
                    cname = (row['颜色中文'] or '').strip()
                    if abbr and abbr not in color_map:
                        color_map[abbr] = cname
    except Exception as e:
        logger.error(f"读取颜色对照失败: {e}", exc_info=True)
    return color_map


def calculate_average_usage_for_fabric(
    fabric_name: str,
    current_spu: str,
    fabric_usage: Dict[Tuple[str, str], Dict[str, Any]]
) -> Tuple[Optional[float], Optional[float]]:
    """对用量缺失的 SPU，用同款面料其他 SPU 的平均用量兜底"""
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
    forecast_qty: int,
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
    return float(forecast_qty) * unit_usage * loss_factor, missing


# ────────────────────────────────────────────────────────────────────────────
# 核心：生成两类记录（含系统预估）
# ────────────────────────────────────────────────────────────────────────────

def generate_fabric_forecast(
    fabric_params:       Dict[str, Dict[str, Any]],
    fabric_usage:        Dict[Tuple[str, str], Dict[str, Any]],
    forecast_data:       Dict[Tuple[str, str], int],   # 运营预计
    system_forecast_data: Dict[Tuple[str, str], int],  # 系统预估（新增）
    inventory_data:      Dict[str, int],
    pending_data:        Dict[str, int],
    inv_by_fabric:       Dict[str, int],
    pend_by_fabric:      Dict[str, int],
    color_map:           Dict[str, str],
    merge_map:           Dict[Tuple[str, str], str],
) -> List[Dict[str, Any]]:

    logger.info("开始生成面料预估数据（总量 + 带颜色）...")

    primary_fabric_by_spu = get_primary_fabric_by_spu(fabric_usage)

    # 聚合桶结构：运营 + 系统 并行
    # 视角A：{(面料, 统计日期): {运营用量米, 系统用量米, 运营下单量, 系统下单量, 缺失SPU}}
    total_agg: Dict[Tuple[str, str], Dict] = defaultdict(
        lambda: {'运营用量米': 0.0, '系统用量米': 0.0,
                 '运营下单量': 0,   '系统下单量': 0,   '缺失SPU': set()}
    )
    # 视角B：{(面料, 归并颜色缩写, 统计日期): 同上}
    color_agg: Dict[Tuple[str, str, str], Dict] = defaultdict(
        lambda: {'运营用量米': 0.0, '系统用量米': 0.0,
                 '运营下单量': 0,   '系统下单量': 0,   '缺失SPU': set()}
    )

    # 收集所有 (SKU, stat_date) 的并集（运营 + 系统都要处理）
    all_keys = set(forecast_data.keys()) | set(system_forecast_data.keys())

    skip_no_spu    = 0
    skip_no_fabric = 0

    for (sku, stat_date) in all_keys:
        op_qty  = forecast_data.get((sku, stat_date), 0)
        sys_qty = system_forecast_data.get((sku, stat_date), 0)

        if op_qty <= 0 and sys_qty <= 0:
            continue

        spu = extract_spu_from_sku(sku)
        if not spu:
            skip_no_spu += 1
            continue

        spu_fabrics = [(f, d) for (s, f), d in fabric_usage.items() if s == spu]
        if not spu_fabrics:
            skip_no_fabric += 1
            continue

        primary_fabric = primary_fabric_by_spu.get(spu)
        color_abbr     = extract_color_abbr_from_sku(sku)

        for fabric_name, usage_data in spu_fabrics:
            unit_usage = usage_data.get('单件用量')
            unit_loss  = usage_data.get('单件损耗')

            # 运营用量
            op_meters, missing = _calc_usage_meters(
                op_qty, unit_usage, unit_loss, fabric_name, spu, fabric_usage
            )
            # 系统用量（用同一用量系数）
            sys_meters, _ = _calc_usage_meters(
                sys_qty, unit_usage, unit_loss, fabric_name, spu, fabric_usage
            )

            # ── 视角A：所有面料，不拆颜色 ──────────────────────────────
            b = total_agg[(fabric_name, stat_date)]
            b['运营用量米'] += op_meters
            b['系统用量米'] += sys_meters
            b['运营下单量'] += op_qty
            b['系统下单量'] += sys_qty
            if missing:
                b['缺失SPU'].add(spu)

            # ── 视角B：仅主面料 且 是定制面料，拆颜色 ─────────────────
            if fabric_name != primary_fabric:
                continue
            if fabric_name not in fabric_params:
                continue
            if not color_abbr:
                continue
            fabric_code = fabric_params[fabric_name].get('面料编号', '')
            if not fabric_code:
                continue

            merged_color = get_merged_color_abbr(fabric_code, color_abbr, merge_map)
            b2 = color_agg[(fabric_name, merged_color, stat_date)]
            b2['运营用量米'] += op_meters
            b2['系统用量米'] += sys_meters
            b2['运营下单量'] += op_qty
            b2['系统下单量'] += sys_qty
            if missing:
                b2['缺失SPU'].add(spu)

    logger.info(f"  视角A聚合桶: {len(total_agg)} | 视角B聚合桶: {len(color_agg)}")
    logger.info(f"  跳过（无SPU）: {skip_no_spu} | 跳过（无面料）: {skip_no_fabric}")

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result: List[Dict[str, Any]] = []

    def _month_str(stat_date) -> str:
        try:
            if isinstance(stat_date, str):
                d = datetime.strptime(stat_date[:10], '%Y-%m-%d')
            else:
                d = stat_date
            return d.strftime('%y-%m')
        except Exception:
            return ''

    # ── 视角A ──────────────────────────────────────────────────────────────
    for (fabric_name, stat_date), b in total_agg.items():
        mpr = fabric_params.get(fabric_name, {}).get('米数每条', 0.0)

        op_m   = round(b['运营用量米'], 2)
        sys_m  = round(b['系统用量米'], 2)
        op_r   = round(op_m  / mpr, 2) if mpr else 0.0
        sys_r  = round(sys_m / mpr, 2) if mpr else 0.0

        inv_rolls  = inv_by_fabric.get(fabric_name, 0)
        pend_rolls = pend_by_fabric.get(fabric_name, 0)

        result.append({
            '统计类型':        '总量',
            'SKU':             '',
            'SPU':             '',
            '面料':            fabric_name,
            '面料编号':        fabric_params.get(fabric_name, {}).get('面料编号', ''),
            '颜色缩写':        '',
            '颜色':            '',
            '面料颜色编号':    '',
            '统计日期':        stat_date,
            '月份':            _month_str(stat_date),
            '运营预计下单量':  b['运营下单量'],
            '系统预估下单量':  b['系统下单量'],
            '预计用量/米':     op_m,
            '系统预估用量/米': sys_m,
            '米数每条':        mpr,
            '预计用量/条':     op_r,
            '系统预估用量/条': sys_r,
            '库存量/条':       inv_rolls,
            '库存量/米':       round(inv_rolls  * mpr, 2),
            '待到货量/条':     pend_rolls,
            '待到货量/米':     round(pend_rolls * mpr, 2),
            '预计总量/条':     inv_rolls + pend_rolls,
            '预计总量/米':     round((inv_rolls + pend_rolls) * mpr, 2),
            '用量信息缺失SPU': ','.join(sorted(b['缺失SPU'])),
            '创建时间':        current_time,
            '更新时间':        current_time,
        })

    # ── 视角B ──────────────────────────────────────────────────────────────
    for (fabric_name, merged_color, stat_date), b in color_agg.items():
        fabric_code = fabric_params.get(fabric_name, {}).get('面料编号', '')
        mpr = fabric_params.get(fabric_name, {}).get('米数每条', 0.0)
        fcc = f"{fabric_code}-{merged_color}" if fabric_code else ''

        op_m   = round(b['运营用量米'], 2)
        sys_m  = round(b['系统用量米'], 2)
        op_r   = round(op_m  / mpr, 2) if mpr else 0.0
        sys_r  = round(sys_m / mpr, 2) if mpr else 0.0

        inv_rolls  = inventory_data.get(fcc, 0)
        pend_rolls = pending_data.get(fcc, 0)

        result.append({
            '统计类型':        '带颜色',
            'SKU':             '',
            'SPU':             '',
            '面料':            fabric_name,
            '面料编号':        fabric_code,
            '颜色缩写':        merged_color,
            '颜色':            color_map.get(merged_color, ''),
            '面料颜色编号':    fcc,
            '统计日期':        stat_date,
            '月份':            _month_str(stat_date),
            '运营预计下单量':  b['运营下单量'],
            '系统预估下单量':  b['系统下单量'],
            '预计用量/米':     op_m,
            '系统预估用量/米': sys_m,
            '米数每条':        mpr,
            '预计用量/条':     op_r,
            '系统预估用量/条': sys_r,
            '库存量/条':       inv_rolls,
            '库存量/米':       round(inv_rolls  * mpr, 2),
            '待到货量/条':     pend_rolls,
            '待到货量/米':     round(pend_rolls * mpr, 2),
            '预计总量/条':     inv_rolls + pend_rolls,
            '预计总量/米':     round((inv_rolls + pend_rolls) * mpr, 2),
            '用量信息缺失SPU': ','.join(sorted(b['缺失SPU'])),
            '创建时间':        current_time,
            '更新时间':        current_time,
        })

    logger.info(f"共生成 {len(result)} 条记录（视角A: {len(total_agg)}, 视角B: {len(color_agg)}）")
    return result


# ────────────────────────────────────────────────────────────────────────────
# 表结构 & 写库
# ────────────────────────────────────────────────────────────────────────────

def create_or_migrate_table() -> None:
    logger.info("检查/创建面料预估表...")
    try:
        with db_cursor(dictionary=False) as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `面料预估表` (
                    `id`              INT AUTO_INCREMENT PRIMARY KEY,
                    `统计类型`        VARCHAR(20)   NOT NULL DEFAULT '' COMMENT '总量 或 带颜色',
                    `SKU`             VARCHAR(200)  DEFAULT '',
                    `SPU`             VARCHAR(100)  DEFAULT '',
                    `面料`            VARCHAR(500)  NOT NULL DEFAULT '',
                    `面料编号`        VARCHAR(500)  DEFAULT '',
                    `颜色缩写`        VARCHAR(100)  DEFAULT '',
                    `颜色`            VARCHAR(100)  DEFAULT '',
                    `面料颜色编号`    VARCHAR(500)  DEFAULT '',
                    `统计日期`        DATE          NOT NULL,
                    `月份`            VARCHAR(20)   DEFAULT '',
                    `运营预计下单量`  INT           DEFAULT 0 COMMENT '运营填写的预计下单量合计',
                    `系统预估下单量`  INT           DEFAULT 0 COMMENT '算法预测的下单量合计',
                    `预计用量/米`     DOUBLE        DEFAULT 0 COMMENT '运营预计用量',
                    `系统预估用量/米` DOUBLE        DEFAULT 0 COMMENT '系统预估用量',
                    `米数每条`        DOUBLE        DEFAULT 0,
                    `预计用量/条`     DOUBLE        DEFAULT 0,
                    `系统预估用量/条` DOUBLE        DEFAULT 0,
                    `库存量/条`       DOUBLE        DEFAULT 0,
                    `库存量/米`       DOUBLE        DEFAULT 0,
                    `待到货量/条`     DOUBLE        DEFAULT 0,
                    `待到货量/米`     DOUBLE        DEFAULT 0,
                    `预计总量/条`     DOUBLE        DEFAULT 0,
                    `预计总量/米`     DOUBLE        DEFAULT 0,
                    `用量信息缺失SPU` TEXT,
                    `创建时间`        DATETIME,
                    `更新时间`        DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_fabric    (`面料`(100)),
                    INDEX idx_stat_date (`统计日期`),
                    INDEX idx_month     (`月份`),
                    INDEX idx_type      (`统计类型`),
                    UNIQUE KEY uk_type_fabric_color_date (`统计类型`, `面料`(100), `颜色缩写`, `统计日期`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='面料预估表'
            """)
            # 兼容旧表：追加新字段
            for col, definition in [
                ('运营预计下单量',  "INT DEFAULT 0 COMMENT '运营填写的预计下单量合计'"),
                ('系统预估下单量',  "INT DEFAULT 0 COMMENT '算法预测的下单量合计'"),
                ('系统预估用量/米', "DOUBLE DEFAULT 0 COMMENT '系统预估用量'"),
                ('系统预估用量/条', "DOUBLE DEFAULT 0"),
            ]:
                try:
                    cursor.execute(f"ALTER TABLE `面料预估表` ADD COLUMN `{col}` {definition}")
                except Exception as e:
                    if 'Duplicate column' not in str(e) and '1060' not in str(e):
                        raise
            logger.info("  表结构就绪")
    except Exception as e:
        logger.error(f"建表失败: {e}", exc_info=True)
        raise


def save_fabric_forecast(data_list: List[Dict[str, Any]]) -> None:
    """全量覆盖写入面料预估表。"""
    if not data_list:
        logger.warning("没有数据可写入")
        return
    logger.info(f"写入 {len(data_list)} 条数据到面料预估表...")
    try:
        with db_cursor(dictionary=False) as cursor:
            cursor.execute("DELETE FROM `面料预估表`")
            logger.info("  已清空旧数据")

            sql = """
            INSERT IGNORE INTO `面料预估表`
                (`统计类型`, `SKU`, `SPU`, `面料`, `面料编号`, `颜色缩写`, `颜色`,
                 `面料颜色编号`, `统计日期`, `月份`,
                 `运营预计下单量`, `系统预估下单量`,
                 `预计用量/米`, `系统预估用量/米`, `米数每条`,
                 `预计用量/条`, `系统预估用量/条`,
                 `库存量/条`, `库存量/米`, `待到货量/条`, `待到货量/米`,
                 `预计总量/条`, `预计总量/米`,
                 `用量信息缺失SPU`, `创建时间`, `更新时间`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            BATCH = 200
            total = 0
            for i in range(0, len(data_list), BATCH):
                batch = data_list[i:i+BATCH]
                rows = [
                    (
                        r['统计类型'], r['SKU'], r['SPU'], r['面料'], r['面料编号'],
                        r['颜色缩写'], r['颜色'], r['面料颜色编号'],
                        r['统计日期'], r['月份'],
                        r['运营预计下单量'], r['系统预估下单量'],
                        r['预计用量/米'], r['系统预估用量/米'], r['米数每条'],
                        r['预计用量/条'], r['系统预估用量/条'],
                        r['库存量/条'], r['库存量/米'], r['待到货量/条'], r['待到货量/米'],
                        r['预计总量/条'], r['预计总量/米'],
                        r['用量信息缺失SPU'], r['创建时间'], r['更新时间'],
                    )
                    for r in batch
                ]
                cursor.executemany(sql, rows)
                total += len(batch)
                logger.info(f"  已写入 {total}/{len(data_list)} 条")
        logger.info(f"✓ 成功写入 {len(data_list)} 条数据")
    except Exception as e:
        logger.error(f"写入失败: {e}", exc_info=True)
        raise


# ────────────────────────────────────────────────────────────────────────────
# 主入口
# ────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 80)
    logger.info("面料预估表生成任务")
    logger.info("=" * 80)

    create_or_migrate_table()

    fabric_params  = get_fabric_params()
    if not fabric_params:
        logger.warning("定制面料参数为空，终止")
        return

    fabric_usage         = get_fabric_price_data()
    forecast_data        = get_forecast_order_data()
    system_forecast_data = get_system_forecast_data()
    merge_map            = get_fabric_color_merge_mapping()
    color_map            = get_color_map()

    if not forecast_data and not system_forecast_data:
        logger.warning("运营预计下单和系统预估均为空，终止")
        return

    inventory_data, pending_data = get_inventory_data(merge_map)
    inv_by_fabric, pend_by_fabric = get_inventory_by_fabric(
        inventory_data, pending_data, fabric_params
    )

    records = generate_fabric_forecast(
        fabric_params        = fabric_params,
        fabric_usage         = fabric_usage,
        forecast_data        = forecast_data,
        system_forecast_data = system_forecast_data,
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
