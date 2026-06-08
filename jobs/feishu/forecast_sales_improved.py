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
) -> Tuple[int, str]:
    m1, m2, m3 = _get_recent_3months(sku_data, current_year, current_month)
    recent_total = m1 + m2 + m3

    if recent_total < MIN_RECENT_SALES_FOR_L3:
        return 0, "L3_数据不足"

    g_weighted = _calc_recent_growth(m1, m2, m3)

    if g_weighted > NEW_PRODUCT_GROWTH_THRESHOLD:
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
        return int(recent_total / 3), "L3_近3月均值(无明显趋势)"


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
) -> Tuple[int, str]:
    """
    预测决策树 v4：
      L1_淡季同比  → 季节款淡季月，直接用 yoy_sales × growth_factor（体现月份差异）
      L1_方案C     → 旺季/过渡月，yoy_pred×α + recent_avg×(1-α) → 方案A兜底
      L3_爆发      → 爆发检测优先（两种路径均适用）
      L2/L3/L4/L5  → 无同期数据兜底
    """
    yoy_label = _get_month_label(forecast_year - 1, forecast_month)
    yoy_sales = sku_data.get(yoy_label, 0) or 0

    m1, m2, m3   = _get_recent_3months(sku_data, current_year, current_month)
    recent_total = m1 + m2 + m3
    recent_avg   = recent_total / 3 if recent_total > 0 else 0

    # ── L1 路径：去年同期存在 & 趋势因子有效 ────────────────────────────
    if yoy_sales > 0 and trend_factor is not None and trend_factor > 0:
        yoy_pred = int(yoy_sales * trend_factor)

        # 爆发检测（优先级最高）
        is_clamped    = raw_trend_factor > TREND_FACTOR_MAX
        recent_growth = _calc_recent_growth(m1, m2, m3)
        is_explosive  = is_clamped and recent_growth > EXPLOSIVE_GROWTH_THRESHOLD
        if is_explosive:
            val, method = _calc_new_product_forecast(
                sku_data, current_year, current_month, forecast_step
            )
            if val > 0:
                return val, (
                    f"L3_爆发检测(同比{raw_trend_factor:.1f}倍"
                    f"+环比{recent_growth:.0%})→阻尼增长"
                )

        # v4：季节性判断
        if season != '全年':
            last_year    = forecast_year - 1
            peak_avg     = _get_peak_season_avg(sku_data, season, last_year)
            if peak_avg > 0:
                seasonal_factor = yoy_sales / peak_avg
                seasonal_factor = max(seasonal_factor, 0.05)  # 最低5%

                if seasonal_factor < OFFSEASON_THRESHOLD:
                    # ── 淡季路径：直接用去年同月 × 增长系数 ────────────
                    growth_factor = min(
                        trend_factor if trend_factor else 1.0,
                        OFFSEASON_GROWTH_CAP
                    )
                    final = int(yoy_sales * growth_factor)
                    return final, (
                        f"L1_淡季同比({season},旺季均{int(peak_avg)}件,"
                        f"系数{seasonal_factor:.2f}<{OFFSEASON_THRESHOLD},"
                        f"去年同月{yoy_sales}件×{growth_factor:.2f}={final}件)"
                    )
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
            return final, (
                f"L1_同比趋势+方案C(α={alpha},"
                f"同比{yoy_pred}件×{alpha:.0%}"
                f"+近3月均值{int(recent_avg)}件×{1-alpha:.0%}={blended}件"
                f"{cap_applied})"
            )
        else:
            return yoy_pred, "L1_同比趋势(近期无销量)"

    # ── L2：去年同期存在但趋势因子为0 ────────────────────────────────────
    if yoy_sales > 0 and trend_factor == 0:
        return int(yoy_sales), "L2_去年同期"

    # ── L3：新品/无同期数据 ───────────────────────────────────────────────
    val, method = _calc_new_product_forecast(
        sku_data, current_year, current_month, forecast_step
    )
    if val > 0:
        return val, method

    # ── L4：SPU趋势兜底 ───────────────────────────────────────────────────
    if yoy_sales > 0 and spu_trend_factor is not None and spu_trend_factor > 0:
        return int(yoy_sales * spu_trend_factor), "L4_SPU趋势兜底"

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
        for idx, (fy, fm, flabel) in enumerate(forecast_months):
            val, method = _forecast_single_month(
                sku_data,
                fy, fm,
                current_year, current_month,
                tf, raw_tf, spu_tf,
                forecast_step=idx,
                season=season,
            )
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

    print("=" * 70)
    print("场景1：LTY351-BO-M（秋冬款，6月预测7/8/9月）")
    print("=" * 70)
    lty351_data = {
        "26年5月销量": 224, "26年4月销量": 429, "26年3月销量": 371,
        "25年7月销量": 59,  "25年8月销量": 160, "25年9月销量": 224,
        "25年10月销量": 858,"25年11月销量": 550,"25年12月销量": 760,
        "25年6月销量": 180, "25年5月销量": 244, "25年4月销量": 429,
        "SPU": "LTY351",
    }
    labels = ["26年7月预计销量", "26年8月预计销量", "26年9月预计销量"]
    r1 = compute_forecast_for_shop(
        {"LTY351-BO-M": lty351_data}, labels, current_date,
        spu_season_map={"LTY351": "秋冬"}
    )
    for lbl in labels:
        print(f"  {lbl}：{r1['LTY351-BO-M'][lbl]} 件")
    print(f"  预测方法：{r1['LTY351-BO-M']['预测方法']}")

    print()
    print("=" * 70)
    print("场景2：爆火产品（应走L3，不被季节压制）")
    print("=" * 70)
    explosive_data = {
        "26年5月销量": 2000, "26年4月销量": 800, "26年3月销量": 200,
        "25年7月销量": 35,   "25年8月销量": 40,  "25年9月销量": 50,
        "SPU": "NEW001",
    }
    r2 = compute_forecast_for_shop(
        {"NEW001-BK-S": explosive_data}, labels, current_date,
        spu_season_map={"NEW001": "秋冬"}
    )
    for lbl in labels:
        print(f"  {lbl}：{r2['NEW001-BK-S'][lbl]} 件")
    print(f"  预测方法：{r2['NEW001-BK-S']['预测方法']}")

    print()
    print("=" * 70)
    print("场景3：全年款（不做季节压制）")
    print("=" * 70)
    normal_data = {
        "26年5月销量": 260, "26年4月销量": 230, "26年3月销量": 210,
        "25年7月销量": 200, "25年8月销量": 190, "25年9月销量": 210,
        "SPU": "NORMAL",
    }
    r3 = compute_forecast_for_shop(
        {"NORMAL-BK-S": normal_data}, labels, current_date,
        spu_season_map={"NORMAL": "全年"}
    )
    for lbl in labels:
        print(f"  {lbl}：{r3['NORMAL-BK-S'][lbl]} 件")
    print(f"  预测方法：{r3['NORMAL-BK-S']['预测方法']}")

    print()
    print("✅ 验证完毕")
