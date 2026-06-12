#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
改进版预计销量算法 v4

【v4 改进：季节性感知重构】
  v3 问题：淡季月份用 recent_avg × seasonal_factor 参与方案C混合，
           导致三个预测月使用同一个 recent_avg，压制后结果完全一样（无月份差异）。

  v4 方案：
    淡季月份（seasonal_factor < OFFSEASON_THRESHOLD=0.6）：
      直接用 yoy_sales × growth_factor
      growth_factor = min(trend_factor, OFFSEASON_GROWTH_CAP=1.5)
      → 以去年同月为基准，体现月份差异；限制放大倍数防止虚高
    旺季/过渡月份（seasonal_factor >= 0.6）：
      保持方案C逻辑（yoy_pred × α + recent_avg × (1-α)）
    爆发检测：优先级最高，两种路径都保留

【决策树 v4】
  有去年同期数据 & trend_factor有效？
    是 → 爆发检测（trend超钳位 AND 近期环比>30%）→ L3阻尼增长
         否 → 计算 seasonal_factor
              淡季（<0.6）→ L1_淡季同比：yoy_sales × growth_factor
              旺季/过渡   → L1_方案C：yoy_pred×α + recent_avg×(1-α) → 方案A兜底
    否 → L2 去年同期 / L3 新品阻尼 / L4 SPU兜底 / L5 无数据
"""

import re as _re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from common import get_logger
from common.database import db_cursor

logger = get_logger('forecast_sales')


# ────────────────────────────────────────────────────────────────────────────
# 常量配置
# ────────────────────────────────────────────────────────────────────────────

TREND_WEIGHTS = {
    "last_1": 0.6,
    "last_2": 0.3,
    "last_3": 0.1,
}

TREND_FACTOR_MIN = 0.3
TREND_FACTOR_MAX = 3.0

MIN_RECENT_SALES_FOR_L3     = 5
MAX_NEW_PRODUCT_GROWTH      = 0.5
NEW_PRODUCT_DAMPING         = 0.6
NEW_PRODUCT_GROWTH_THRESHOLD = 0.05

# v2：爆发检测
EXPLOSIVE_GROWTH_THRESHOLD = 0.30

# v2：方案C动态α
ALPHA_HIGH_THRESHOLD = 2.0
ALPHA_MID_THRESHOLD  = 1.5
ALPHA_HIGH   = 0.3
ALPHA_MID    = 0.5
ALPHA_NORMAL = 0.7

# v2：方案A环比上限
MOM_CAP_RATIO = 1.5

# v4：淡季判定阈值（目标月/旺季均值 < 此值 → 走淡季逻辑）
OFFSEASON_THRESHOLD  = 0.6
# v4：淡季增长上限（trend_factor 最多放大到此倍数）
OFFSEASON_GROWTH_CAP = 1.5

# v4：动态旺季识别 Top N 个月
PEAK_TOP_N = 3
# v4：静态兜底旺季月份（去掉启动月和收尾月）
PEAK_MONTHS_FALLBACK = {
    '春夏': [4, 5, 6, 7],
    '秋冬': [10, 11, 12, 1],
}


# ────────────────────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────────────────────

def _get_month_label(year: int, month: int) -> str:
    return f"{str(year)[-2:]}年{month}月销量"


def _offset_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    month += delta
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return year, month


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# ────────────────────────────────────────────────────────────────────────────
# 近3月销量
# ────────────────────────────────────────────────────────────────────────────

def _get_recent_3months(
    sku_data: Dict[str, Any],
    current_year: int,
    current_month: int,
) -> Tuple[int, int, int]:
    m1_y, m1_m = _offset_month(current_year, current_month, -1)
    m2_y, m2_m = _offset_month(current_year, current_month, -2)
    m3_y, m3_m = _offset_month(current_year, current_month, -3)
    m1 = sku_data.get(_get_month_label(m1_y, m1_m), 0) or 0
    m2 = sku_data.get(_get_month_label(m2_y, m2_m), 0) or 0
    m3 = sku_data.get(_get_month_label(m3_y, m3_m), 0) or 0
    return m1, m2, m3


def _calc_recent_growth(m1: int, m2: int, m3: int) -> float:
    g_old = ((m2 - m3) / m3) if m3 > 0 else 0.0
    g_new = ((m1 - m2) / m2) if m2 > 0 else 0.0
    return 0.4 * g_old + 0.6 * g_new


# ────────────────────────────────────────────────────────────────────────────
# L3：新品/爆发阻尼增长预测
# ────────────────────────────────────────────────────────────────────────────

def _calc_new_product_forecast(
    sku_data: Dict[str, Any],
    current_year: int,
    current_month: int,
    forecast_step: int,
    season: str = '全年',
    forecast_year: int = None,
    forecast_month: int = None,
    spu_trend_factor: Optional[float] = None,
) -> Tuple[int, str]:
    """
    L3 路径：无 trend_factor 时的预测（新品/去年同期无数据）。

    v4 季节款优先策略：
      季节款 + 去年同期有数据 → 直接用 yoy_sales × growth_factor
        growth_factor = min(spu_trend_factor or 1.0, OFFSEASON_GROWTH_CAP)
        → 和 L1 淡季路径逻辑统一，避免用淡季低点 recent_avg 作基准

      季节款 + 去年同期无数据（真正新品）→ recent_avg × adj_factor
        adj_factor = target_seasonal / recent_seasonal
        recent_seasonal 优先用今年近3月/旺季均值（避免 fallback 0.1 失真）

      全年款 → 原有阻尼增长/近3月均值逻辑
    """
    m1, m2, m3   = _get_recent_3months(sku_data, current_year, current_month)
    recent_total = m1 + m2 + m3

    if recent_total < MIN_RECENT_SALES_FOR_L3:
        return 0, "L3_数据不足"

    g_weighted = _calc_recent_growth(m1, m2, m3)
    recent_avg = recent_total / 3

    # ── v4：季节款优先路径 ────────────────────────────────────────────────
    if (season != '全年'
            and forecast_year is not None
            and forecast_month is not None):

        last_year = forecast_year - 1
        # 秋冬款 1/2 月跨年
        yoy_year  = last_year + 1 if (season == '秋冬' and forecast_month in [1, 2]) else last_year
        yoy_label = _get_month_label(yoy_year, forecast_month)
        yoy_sales = sku_data.get(yoy_label, 0) or 0

        if yoy_sales >= 10:
            # ── 去年同期有足够数据：直接用 yoy × growth_factor ──────────
            gf, gf_note = _calc_yoy_growth_factor_standalone(
                sku_data, current_year, current_month, forecast_year, season
            )
            growth_factor = gf
            final = int(yoy_sales * growth_factor)
            return final, (
                f"L3_季节同比({season},"
                f"去年同月{yoy_sales}件×{growth_factor:.2f}[{gf_note}]={final}件)"
            )

        else:
            # ── 去年同期无数据（真正新品）：recent_avg × adj_factor ───────
            peak_avg = _get_peak_season_avg(sku_data, season, last_year)
            if peak_avg > 0:
                # 目标月季节系数（无去年同期，用0跳过，adj_factor设为1）
                # 改用今年近3月 / 旺季均值 作为 recent_seasonal（比 fallback 0.1 更准）
                recent_seasonal = recent_avg / peak_avg if recent_avg > 0 else 0.1
                recent_seasonal = max(recent_seasonal, 0.05)

                # 目标月静态季节位置（用静态旺季月份判断）
                static_peak = set(PEAK_MONTHS_FALLBACK.get(season, []))
                if forecast_month in static_peak:
                    # 旺季月：adj_factor = 1/recent_seasonal（拉回旺季水平）
                    adj_factor = _clamp(1.0 / recent_seasonal, 0.5, 5.0)
                else:
                    adj_factor = 1.0  # 淡季新品维持近3月均值

                val = int(recent_avg * adj_factor)
                return val, (
                    f"L3_新品季节adj({season},"
                    f"近3月均值{int(recent_avg)}件,"
                    f"旺季均值{int(peak_avg)}件,"
                    f"近期系数{recent_seasonal:.2f},"
                    f"adj×{adj_factor:.2f}={val}件)"
                )

    # ── 全年款 / 无法计算季节 → 原有逻辑（加近期下跌检测）────────────────

    # 近期下跌检测：如果 m1 < m2（近2个月连续下跌），用 m1 替代近3月均值
    # 防止历史爆发期数据（m2/m3高峰）污染均值，导致预测虚高
    is_declining = (m1 > 0 and m2 > 0 and m1 < m2)
    base_val     = m1 if is_declining else int(recent_avg)
    decline_note = f"[近期下跌,用m1={m1}件替代均值{int(recent_avg)}件]" if is_declining else ""

    if g_weighted > NEW_PRODUCT_GROWTH_THRESHOLD and not is_declining:
        # 有增长趋势且未下跌：阻尼增长
        g = min(g_weighted, MAX_NEW_PRODUCT_GROWTH)
        value = float(m1)
        for i in range(forecast_step + 1):
            value *= (1.0 + g * (NEW_PRODUCT_DAMPING ** i))
        cap_note = (
            f"(原始增速{g_weighted:.0%}→限速{g:.0%})"
            if g < g_weighted else f"(增速{g:.0%})"
        )
        return int(value), f"L3_阻尼增长{cap_note}·step{forecast_step}"
    else:
        # 无明显趋势或近期下跌：用 base_val（下跌时=m1，否则=近3月均值）
        # 下跌时额外施加 0.9^step 的衰减，反映持续回落趋势
        if is_declining:
            decay     = 0.9 ** forecast_step
            final_val = max(1, int(base_val * decay))
            return final_val, f"L3_近期下跌衰减(×{decay:.2f})·step{forecast_step}{decline_note}"
        else:
            return int(recent_avg), "L3_近3月均值(无明显趋势)"


# ────────────────────────────────────────────────────────────────────────────
# 加权趋势因子
# ────────────────────────────────────────────────────────────────────────────

def _calc_weighted_trend_factor(
    sku_data: Dict[str, Any],
    base_year: int,
    base_month: int,
) -> Tuple[Optional[float], float, str]:
    offsets    = [0, -1, -2]
    weight_keys = ["last_1", "last_2", "last_3"]

    valid_weighted_sum = 0.0
    valid_weight_total = 0.0
    details = []

    for offset, wkey in zip(offsets, weight_keys):
        y, m       = _offset_month(base_year, base_month, offset)
        yoy_y, yoy_m = y - 1, m

        this_sales = sku_data.get(_get_month_label(y, m), 0) or 0
        yoy_sales  = sku_data.get(_get_month_label(yoy_y, yoy_m), 0) or 0
        w = TREND_WEIGHTS[wkey]

        if yoy_sales > 0:
            ratio = this_sales / yoy_sales
            valid_weighted_sum += w * ratio
            valid_weight_total += w
            details.append(
                f"{_get_month_label(y, m).replace('销量', '')} "
                f"{this_sales}÷{yoy_sales}={ratio:.2f}(w={w})"
            )
        else:
            details.append(
                f"{_get_month_label(y, m).replace('销量', '')} 去年同期=0，跳过"
            )

    if valid_weight_total == 0:
        return None, 0.0, "3个月去年同期均为0，无法计算趋势因子"

    raw_factor = valid_weighted_sum / valid_weight_total
    clamped    = _clamp(raw_factor, TREND_FACTOR_MIN, TREND_FACTOR_MAX)
    clamped_note = f" → 钳位至{clamped:.2f}" if clamped != raw_factor else ""

    detail_log = (
        f"加权趋势因子={raw_factor:.3f}{clamped_note}；"
        f"有效月份权重和={valid_weight_total:.1f}；"
        f"明细：{'，'.join(details)}"
    )
    return round(clamped, 2), round(raw_factor, 3), detail_log


# ────────────────────────────────────────────────────────────────────────────
# v4：动态旺季月均销量
# ────────────────────────────────────────────────────────────────────────────

def _get_valid_month_count(
    sku_data: Dict[str, Any],
    last_year: int,
    season: str,
    min_sales: int = 10,
) -> int:
    """计算去年有多少个月有有效销量（>= min_sales），用于判断产品成熟度。"""
    count = 0
    for m in range(1, 13):
        y = last_year + 1 if (season == '秋冬' and m in [1, 2]) else last_year
        v = sku_data.get(_get_month_label(y, m), 0) or 0
        if v >= min_sales:
            count += 1
    return count


def _dynamic_growth_cap(valid_months: int) -> float:
    """
    根据去年有效月份数动态确定增长系数上限：
      <= 3个月：新品/成长期 → cap 3.0
      <= 6个月：成长期     → cap 2.0
      > 6个月：成熟产品    → cap 1.5
    """
    if valid_months <= 3:
        return 3.0
    elif valid_months <= 6:
        return 2.0
    else:
        return 1.5


def _calc_yoy_growth_factor_standalone(
    sku_data: Dict[str, Any],
    current_year: int,
    current_month: int,
    forecast_year: int,
    season: str,
) -> Tuple[float, str]:
    """
    计算今年整体增长系数，上限动态确定（基于产品成熟度）。

    逻辑：
      1. 优先：近3月今年 vs 去年同期（同月对比）
      2. 次选：近3月今年均值 / 去年销量最接近今年均值的月份
      3. Fallback：1.0
    上限：_dynamic_growth_cap（基于去年有效月份数）
    """
    m1_y, m1_m = _offset_month(current_year, current_month, -1)
    m2_y, m2_m = _offset_month(current_year, current_month, -2)
    m3_y, m3_m = _offset_month(current_year, current_month, -3)
    m1 = sku_data.get(_get_month_label(m1_y, m1_m), 0) or 0
    m2 = sku_data.get(_get_month_label(m2_y, m2_m), 0) or 0
    m3 = sku_data.get(_get_month_label(m3_y, m3_m), 0) or 0
    recent_avg = (m1 + m2 + m3) / 3 if (m1 + m2 + m3) > 0 else 0

    last_year   = forecast_year - 1
    valid_months = _get_valid_month_count(sku_data, last_year, season)
    cap          = _dynamic_growth_cap(valid_months)

    # 优先：近3月今年 vs 去年同期（同月对比）
    yoy_pairs = []
    for (ry, rm), this_val in [((m1_y, m1_m), m1), ((m2_y, m2_m), m2), ((m3_y, m3_m), m3)]:
        yoy_val = sku_data.get(_get_month_label(ry - 1, rm), 0) or 0
        if this_val > 0 and yoy_val >= 10:
            yoy_pairs.append(this_val / yoy_val)
    if yoy_pairs:
        gf = sum(yoy_pairs) / len(yoy_pairs)
        return min(gf, cap), f"近期同比×{gf:.2f}(cap{cap:.1f},去年有效{valid_months}月)"

    # 次选：找去年所有有效月份中销量最接近今年近3月均值的月份
    if recent_avg > 0:
        candidates = []
        for m in range(1, 13):
            y = last_year + 1 if (season == '秋冬' and m in [1, 2]) else last_year
            v = sku_data.get(_get_month_label(y, m), 0) or 0
            if v >= 10:
                candidates.append((m, v))
        if candidates:
            closest = min(candidates, key=lambda x: abs(x[1] - recent_avg))
            closest_m, closest_v = closest
            gf = recent_avg / closest_v
            return min(gf, cap), (
                f"近3月均值/去年最近水平(去年{closest_m}月{int(closest_v)}件)"
                f"×{gf:.2f}(cap{cap:.1f},去年有效{valid_months}月)"
            )

    return 1.0, f"无同比数据默认1.0(去年有效{valid_months}月)"


def _get_peak_season_avg(
    sku_data: Dict[str, Any],
    season: str,
    last_year: int,
) -> float:
    """
    动态识别去年销量最高的 PEAK_TOP_N 个月作为旺季，计算均值。
    验证：Top N 中至少 2 个属于静态旺季定义，否则 fallback。
    完全无数据时返回 0（不做季节压制）。
    """
    if season == '全年':
        return 0.0

    # Step1：取去年全年12个月销量
    monthly_sales = []
    for m in range(1, 13):
        y = last_year + 1 if (season == '秋冬' and m in [1, 2]) else last_year
        label = f"{str(y)[-2:]}年{m}月销量"
        val = sku_data.get(label, 0) or 0
        monthly_sales.append((m, val))

    # Step2：动态找最高 N 个月
    has_sales = [(m, v) for m, v in monthly_sales if v > 0]
    if len(has_sales) >= PEAK_TOP_N:
        top_months     = sorted(has_sales, key=lambda x: x[1], reverse=True)[:PEAK_TOP_N]
        top_months_ids = {m for m, _ in top_months}
        static_peak    = set(PEAK_MONTHS_FALLBACK.get(season, []))
        overlap        = len(top_months_ids & static_peak)
        if overlap >= 2:
            return sum(v for _, v in top_months) / PEAK_TOP_N

    # Step3：fallback 到静态旺季定义
    fallback_months = PEAK_MONTHS_FALLBACK.get(season, [])
    fallback_sales  = []
    for m in fallback_months:
        y = last_year + 1 if (season == '秋冬' and m in [1, 2]) else last_year
        label = f"{str(y)[-2:]}年{m}月销量"
        val = sku_data.get(label, 0) or 0
        if val > 0:
            fallback_sales.append(val)

    if fallback_sales:
        return sum(fallback_sales) / len(fallback_sales)

    return 0.0


# ────────────────────────────────────────────────────────────────────────────
# 核心：预测单月 v4
# ────────────────────────────────────────────────────────────────────────────

def _forecast_single_month(
    sku_data: Dict[str, Any],
    forecast_year: int,
    forecast_month: int,
    current_year: int,
    current_month: int,
    trend_factor: Optional[float],
    raw_trend_factor: float,
    spu_trend_factor: Optional[float],
    forecast_step: int = 0,
    season: str = '全年',
    prev_forecast: Optional[int] = None,   # v4：上月预测结果
) -> Tuple[int, str]:
    """
    预测决策树 v4：
      L1_淡季同比  → 季节款淡季月，直接用 yoy_sales × growth_factor
      L1_方案C     → 旺季/过渡月，yoy_pred×α + recent_avg×(1-α) → 方案A兜底
      L3_爆发      → 爆发检测优先
      L2/L3/L4/L5  → 无同期数据兜底

    v4 floor 机制：
      季节款 + prev_forecast 存在时，预测结果不能低于
      max(recent_avg, prev_forecast × 0.8)
      防止旺季启动期出现月度断崖下跌
    """
    yoy_label = _get_month_label(forecast_year - 1, forecast_month)
    yoy_sales = sku_data.get(yoy_label, 0) or 0

    m1, m2, m3   = _get_recent_3months(sku_data, current_year, current_month)
    recent_total = m1 + m2 + m3
    recent_avg   = recent_total / 3 if recent_total > 0 else 0

    # ── 计算今年整体增长系数（用于 floor 修正）─────────────────────────
    def _calc_yoy_growth_factor() -> Tuple[float, str]:
        last_year_   = forecast_year - 1
        valid_months_ = _get_valid_month_count(sku_data, last_year_, season)
        cap_          = _dynamic_growth_cap(valid_months_)

        yoy_pairs = []
        for delta in [-1, -2, -3]:
            ry, rm   = _offset_month(current_year, current_month, delta)
            this_val = sku_data.get(_get_month_label(ry, rm), 0) or 0
            yoy_val  = sku_data.get(_get_month_label(ry - 1, rm), 0) or 0
            if this_val > 0 and yoy_val >= 10:
                yoy_pairs.append(this_val / yoy_val)
        if yoy_pairs:
            gf = sum(yoy_pairs) / len(yoy_pairs)
            return min(gf, cap_), f"近期同比×{gf:.2f}"

        if recent_avg > 0:
            candidates = []
            for m in range(1, 13):
                y = last_year_ + 1 if (season == '秋冬' and m in [1, 2]) else last_year_
                v = sku_data.get(_get_month_label(y, m), 0) or 0
                if v >= 10:
                    candidates.append((m, v))
            if candidates:
                closest = min(candidates, key=lambda x: abs(x[1] - recent_avg))
                closest_m, closest_v = closest
                gf = recent_avg / closest_v
                return min(gf, cap_), f"近3月/去年最近水平(去年{closest_m}月)×{gf:.2f}"

        return 1.0, "无同比数据默认1.0"

    # ── 内部函数：应用 floor 后返回 ──────────────────────────────────────
    def _apply_floor(val: int, method: str) -> Tuple[int, str]:
        """
        floor 机制：季节款上行期，预测值至少是上月的1.1倍。
        判断是否处于上行季节：去年预测月销量 >= 去年上月销量。
        非上行期（淡季回落）不施加 floor。
        """
        if season == '全年' or prev_forecast is None or prev_forecast <= 0:
            return val, method

        # 判断是否上行季节（去年本月 >= 去年上月）
        last_year = forecast_year - 1
        yoy_y     = last_year + 1 if (season == '秋冬' and forecast_month in [1, 2]) else last_year
        prev_fy, prev_fm = _offset_month(forecast_year, forecast_month, -1)
        prev_yoy_y = last_year + 1 if (season == '秋冬' and prev_fm in [1, 2]) else last_year
        yoy_this   = sku_data.get(_get_month_label(yoy_y, forecast_month), 0) or 0
        yoy_prev   = sku_data.get(_get_month_label(prev_yoy_y, prev_fm), 0) or 0

        # 上行期判断：去年本月 > 去年上月，或去年数据不足（新品默认上行）
        is_uptrend = (yoy_this >= yoy_prev) if (yoy_this >= 10 and yoy_prev >= 10) else True

        if is_uptrend:
            floor = int(prev_forecast * 1.1)
            if val < floor:
                return floor, method + f"[↑floor={floor}件,上行×1.1]"
        return val, method

    # ── L1 路径：去年同期存在 & 趋势因子有效 ────────────────────────────
    if yoy_sales > 0 and trend_factor is not None and trend_factor > 0:
        yoy_pred = int(yoy_sales * trend_factor)

        # 爆发检测（优先级最高）
        is_clamped    = raw_trend_factor > TREND_FACTOR_MAX
        recent_growth = _calc_recent_growth(m1, m2, m3)
        is_explosive  = is_clamped and recent_growth > EXPLOSIVE_GROWTH_THRESHOLD
        if is_explosive:
            val, method = _calc_new_product_forecast(
                sku_data, current_year, current_month, forecast_step,
                season=season, forecast_year=forecast_year, forecast_month=forecast_month,
                spu_trend_factor=spu_trend_factor,
            )
            if val > 0:
                return _apply_floor(val, (
                    f"L3_爆发检测(同比{raw_trend_factor:.1f}倍"
                    f"+环比{recent_growth:.0%})→阻尼增长"
                ))

        # v4：季节性判断
        if season != '全年':
            last_year    = forecast_year - 1
            peak_avg     = _get_peak_season_avg(sku_data, season, last_year)
            if peak_avg > 0:
                seasonal_factor = yoy_sales / peak_avg
                seasonal_factor = max(seasonal_factor, 0.05)  # 最低5%

                if seasonal_factor < OFFSEASON_THRESHOLD:
                    growth_factor = min(
                        trend_factor if trend_factor else 1.0,
                        OFFSEASON_GROWTH_CAP
                    )
                    final = int(yoy_sales * growth_factor)
                    return _apply_floor(final, (
                        f"L1_淡季同比({season},旺季均{int(peak_avg)}件,"
                        f"系数{seasonal_factor:.2f}<{OFFSEASON_THRESHOLD},"
                        f"去年同月{yoy_sales}件×{growth_factor:.2f}={final}件)"
                    ))
                # seasonal_factor >= 0.6：旺季/过渡月，走方案C（不压制）

        # ── 旺季/全年/过渡月：方案C动态α混合 ───────────────────────────
        if trend_factor >= ALPHA_HIGH_THRESHOLD:
            alpha = ALPHA_HIGH
        elif trend_factor >= ALPHA_MID_THRESHOLD:
            alpha = ALPHA_MID
        else:
            alpha = ALPHA_NORMAL

        if recent_avg > 0:
            blended = int(yoy_pred * alpha + recent_avg * (1 - alpha))
            mom_cap = int(recent_avg * MOM_CAP_RATIO)
            final   = min(blended, mom_cap)
            cap_applied = "→上限截断" if final < blended else ""
            return _apply_floor(final, (
                f"L1_同比趋势+方案C(α={alpha},"
                f"同比{yoy_pred}件×{alpha:.0%}"
                f"+近3月均值{int(recent_avg)}件×{1-alpha:.0%}={blended}件"
                f"{cap_applied})"
            ))
        else:
            return _apply_floor(yoy_pred, "L1_同比趋势(近期无销量)")

    # ── L2：去年同期存在但趋势因子为0 ────────────────────────────────────
    if yoy_sales > 0 and trend_factor == 0:
        return _apply_floor(int(yoy_sales), "L2_去年同期")

    # ── L3：新品/无同期数据 ───────────────────────────────────────────────
    val, method = _calc_new_product_forecast(
        sku_data, current_year, current_month, forecast_step,
        season=season, forecast_year=forecast_year, forecast_month=forecast_month,
        spu_trend_factor=spu_trend_factor,
    )
    if val > 0:
        return _apply_floor(val, method)

    # ── L4：SPU趋势兜底 ───────────────────────────────────────────────────
    if yoy_sales > 0 and spu_trend_factor is not None and spu_trend_factor > 0:
        return _apply_floor(int(yoy_sales * spu_trend_factor), "L4_SPU趋势兜底")

    return 0, "L5_无数据"


# ────────────────────────────────────────────────────────────────────────────
# 对外接口
# ────────────────────────────────────────────────────────────────────────────

def compute_forecast_for_shop(
    shop_data: Dict[str, Dict[str, Any]],
    forecast_sales_labels: List[str],
    current_date: datetime = None,
    spu_season_map: Dict[str, str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    对单个店铺的所有 SKU 计算多月预测。

    Args:
        shop_data         : {SKU: {月份label: 销量, 'SPU': ...}}
        forecast_sales_labels : 预测月份标签列表
        current_date      : 当前日期（默认今日）
        spu_season_map    : {SPU: '春夏'|'秋冬'|'全年'}，None 时退化为全年
    """
    if spu_season_map is None:
        spu_season_map = {}
    if current_date is None:
        current_date = datetime.now()

    current_year  = current_date.year
    current_month = current_date.month
    last_month_year, last_month = _offset_month(current_year, current_month, -1)

    # Step1：SKU 趋势因子
    sku_trend:     Dict[str, Optional[float]] = {}
    sku_trend_raw: Dict[str, float]           = {}
    for sku, sku_data in shop_data.items():
        tf, raw_tf, _ = _calc_weighted_trend_factor(sku_data, last_month_year, last_month)
        sku_trend[sku]     = tf
        sku_trend_raw[sku] = raw_tf

    # Step2：SPU 趋势因子均值
    spu_trend:   Dict[str, float]     = {}
    spu_sku_map: Dict[str, List[str]] = {}
    for sku, sku_data in shop_data.items():
        spu = (sku_data.get("SPU") or "").strip()
        if spu:
            spu_sku_map.setdefault(spu, []).append(sku)
    for spu, skus in spu_sku_map.items():
        valid = [sku_trend[s] for s in skus if sku_trend.get(s) is not None]
        if valid:
            spu_trend[spu] = round(sum(valid) / len(valid), 2)

    # Step3：解析预测月份标签
    forecast_months: List[Tuple[int, int, str]] = []
    for label in forecast_sales_labels:
        mm = _re.match(r"(\d{2})年(\d{1,2})月预计销量", label)
        if mm:
            yr = 2000 + int(mm.group(1)) if int(mm.group(1)) < 50 else 1900 + int(mm.group(1))
            mo = int(mm.group(2))
            forecast_months.append((yr, mo, label))

    # Step4：逐 SKU 生成预测
    result: Dict[str, Dict[str, Any]] = {}
    for sku, sku_data in shop_data.items():
        tf     = sku_trend[sku]
        raw_tf = sku_trend_raw[sku]
        spu    = (sku_data.get("SPU") or "").strip()
        spu_tf = spu_trend.get(spu)
        season = spu_season_map.get(spu, '全年')

        sku_result: Dict[str, Any] = {"趋势因子": tf if tf is not None else 0.0}
        method_labels = []
        prev_forecast: Optional[int] = None   # v4：上月预测结果，用于季节款 floor 兜底
        for idx, (fy, fm, flabel) in enumerate(forecast_months):
            val, method = _forecast_single_month(
                sku_data,
                fy, fm,
                current_year, current_month,
                tf, raw_tf, spu_tf,
                forecast_step=idx,
                season=season,
                prev_forecast=prev_forecast,
            )
            prev_forecast = val
            sku_result[flabel] = val
            method_labels.append(f"{fm}月:{method}")

        sku_result["预测方法"] = "；".join(method_labels)
        result[sku] = sku_result

    return result


# ────────────────────────────────────────────────────────────────────────────
# SPU 季节映射（从 MySQL 读取）
# ────────────────────────────────────────────────────────────────────────────

def load_spu_season_map() -> Dict[str, str]:
    """
    从 MySQL `SPU季节表` 读取 {SPU: 季节} 映射。
    表不存在或读取失败时返回空字典（退化为全年，不影响流水线）。
    """
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'SPU季节表'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("SPU季节表不存在，季节性感知跳过")
                return {}
            cur.execute("SELECT SPU, 季节 FROM `SPU季节表`")
            result = {row['SPU']: row['季节'] for row in cur.fetchall()}
            logger.info(f"读取 SPU季节表：{len(result)} 条")
            return result
    except Exception as e:
        logger.warning(f"读取 SPU季节表失败（降级为全年）: {e}")
        return {}


# ────────────────────────────────────────────────────────────────────────────
# 单测
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    current_date = datetime(2026, 6, 8)
    labels_789  = ["26年7月预计销量", "26年8月预计销量", "26年9月预计销量"]
    labels_10   = ["26年10月预计销量"]

    print("=" * 70)
    print("场景1：LTY351-BO JQ-US（秋冬款，无去年3-5月同期，走L3）")
    print("=" * 70)
    lty351_data = {
        "26年5月销量": 185, "26年4月销量": 348, "26年3月销量": 644,
        "25年7月销量": 277, "25年8月销量": 867, "25年9月销量": 932,
        "25年10月销量": 4065, "25年11月销量": 1959, "25年12月销量": 2792,
        "26年1月销量": 1874, "26年2月销量": 1290,
        "SPU": "LTY351",
    }
    r1_789 = compute_forecast_for_shop(
        {"LTY351-BO-ALL": lty351_data}, labels_789, current_date,
        spu_season_map={"LTY351": "秋冬"}
    )
    r1_10 = compute_forecast_for_shop(
        {"LTY351-BO-ALL": lty351_data}, labels_10, current_date,
        spu_season_map={"LTY351": "秋冬"}
    )
    for lbl in labels_789 + labels_10:
        r = r1_789 if lbl in labels_789 else r1_10
        print(f"  {lbl}：{r['LTY351-BO-ALL'][lbl]} 件")
    print(f"  去年实际：7月277 / 8月867 / 9月932 / 10月4065")
    print(f"  7-9月预测方法：{r1_789['LTY351-BO-ALL']['预测方法']}")
    print(f"  10月预测方法：{r1_10['LTY351-BO-ALL']['预测方法']}")

    print()
    print("=" * 70)
    print("场景2：ZQZ369-BO（秋冬款，无去年3-5月同期，走L3）")
    print("=" * 70)
    zqz369_data = {
        "26年5月销量": 1378, "26年4月销量": 1903, "26年3月销量": 2565,
        "25年8月销量": 536,  "25年9月销量": 1197,
        "25年10月销量": 3504, "25年11月销量": 4897, "25年12月销量": 4466,
        "26年1月销量": 4513, "26年2月销量": 3816,
        "SPU": "ZQZ369",
    }
    r2_789 = compute_forecast_for_shop(
        {"ZQZ369-BO-ALL": zqz369_data}, labels_789, current_date,
        spu_season_map={"ZQZ369": "秋冬"}
    )
    r2_10 = compute_forecast_for_shop(
        {"ZQZ369-BO-ALL": zqz369_data}, labels_10, current_date,
        spu_season_map={"ZQZ369": "秋冬"}
    )
    for lbl in labels_789 + labels_10:
        r = r2_789 if lbl in labels_789 else r2_10
        print(f"  {lbl}：{r['ZQZ369-BO-ALL'][lbl]} 件")
    print(f"  去年实际：7月1 / 8月536 / 9月1197 / 10月3504")
    print(f"  7-9月预测方法：{r2_789['ZQZ369-BO-ALL']['预测方法']}")
    print(f"  10月预测方法：{r2_10['ZQZ369-BO-ALL']['预测方法']}")

    print()
    print("✅ 验证完毕")
