#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
改进版预计销量算法 v2

【v2 新增改进】
5. 爆发检测：近期环比持续快速增长 + 同比因子超钳位上限 → 走L3阻尼增长（不压制爆火产品）
6. 方案C：同比因子越高，越依赖近3月均值（动态α混合，避免高基数效应虚高预测）
7. 方案A兜底：混合结果再用环比上限兜底（防止极端情况）

【新决策树】
  有去年同期数据？
    是 → 计算trend_factor，判断是否爆发
          爆发（trend超钳位 AND 近期环比>30%）→ L3阻尼增长（真实爆火不压制）
          非爆发 → 方案C混合（trend>2.0: α=0.3; >1.5: α=0.5; 否则: α=0.7）
                   → 方案A兜底（min 近3月均值×1.5）→ L1结果
    否 → L2（去年同期=0但trend=0）/ L3（无去年同期）/ L4（SPU兜底）
"""

from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional


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

MIN_RECENT_SALES_FOR_L3 = 5
MAX_NEW_PRODUCT_GROWTH  = 0.5
NEW_PRODUCT_DAMPING     = 0.6
NEW_PRODUCT_GROWTH_THRESHOLD = 0.05

# ── v2 新增常量 ───────────────────────────────────────────────────────────
# 爆发检测：近期加权环比增速超过此值 AND 同比因子需要钳位 → 走L3
EXPLOSIVE_GROWTH_THRESHOLD = 0.30   # 30%/月

# 方案C：动态α分界线
ALPHA_HIGH_THRESHOLD = 2.0    # trend > 2.0 → α=0.3
ALPHA_MID_THRESHOLD  = 1.5    # trend > 1.5 → α=0.5
ALPHA_HIGH   = 0.3
ALPHA_MID    = 0.5
ALPHA_NORMAL = 0.7

# 方案A：近3月均值的环比上限系数
MOM_CAP_RATIO = 1.5            # 允许近3月均值增长50%


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
# 获取近3月销量（m1=上月，m2=上上月，m3=上上上月）
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
    """计算近期加权环比增速（与L3相同的公式）"""
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
        cap_note = f"(原始增速{g_weighted:.0%}→限速{g:.0%})" if g < g_weighted else f"(增速{g:.0%})"
        return int(value), f"L3_阻尼增长{cap_note}·step{forecast_step}"
    else:
        return int(recent_total / 3), "L3_近3月均值(无明显趋势)"


# ────────────────────────────────────────────────────────────────────────────
# 加权趋势因子（v2：同时返回未钳位的原始值，供爆发检测使用）
# ────────────────────────────────────────────────────────────────────────────

def _calc_weighted_trend_factor(
    sku_data: Dict[str, Any],
    base_year: int,
    base_month: int,
) -> Tuple[Optional[float], float, str]:
    """
    返回 (clamped_factor, raw_factor, detail_log)
    raw_factor：未经钳位的原始趋势因子（用于爆发检测）
    clamped_factor：钳位后的趋势因子（None表示无法计算）
    """
    offsets   = [0, -1, -2]
    weight_keys = ["last_1", "last_2", "last_3"]

    valid_weighted_sum = 0.0
    valid_weight_total = 0.0
    details = []

    for offset, wkey in zip(offsets, weight_keys):
        y, m = _offset_month(base_year, base_month, offset)
        yoy_y, yoy_m = y - 1, m

        this_sales = sku_data.get(_get_month_label(y, m), 0) or 0
        yoy_sales  = sku_data.get(_get_month_label(yoy_y, yoy_m), 0) or 0
        w = TREND_WEIGHTS[wkey]

        if yoy_sales > 0:
            ratio = this_sales / yoy_sales
            valid_weighted_sum += w * ratio
            valid_weight_total += w
            details.append(
                f"{_get_month_label(y,m).replace('销量','')} "
                f"{this_sales}÷{yoy_sales}={ratio:.2f}(w={w})"
            )
        else:
            details.append(
                f"{_get_month_label(y,m).replace('销量','')} 去年同期=0，跳过"
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
# 核心：预测单月（v2：爆发检测 + 方案C动态α + 方案A兜底）
# ────────────────────────────────────────────────────────────────────────────

def _forecast_single_month(
    sku_data: Dict[str, Any],
    forecast_year: int,
    forecast_month: int,
    current_year: int,
    current_month: int,
    trend_factor: Optional[float],
    raw_trend_factor: float,          # v2新增：未钳位原始值
    spu_trend_factor: Optional[float],
    forecast_step: int = 0,
) -> Tuple[int, str]:
    """
    预测决策树（v2）：

    有去年同期 & trend_factor有效？
      ├─ 爆发检测（raw_factor超钳位 AND 近期环比>30%）→ L3阻尼增长
      └─ 方案C混合（动态α） → 方案A兜底（min 近3月均值×1.5）→ 返回
    无去年同期 or trend_factor=None？
      ├─ L2：去年同期但trend=0
      ├─ L3：无去年同期（新品）
      └─ L4：SPU趋势兜底
    """
    yoy_label = _get_month_label(forecast_year - 1, forecast_month)
    yoy_sales = sku_data.get(yoy_label, 0) or 0

    # ── 近3月数据（供方案C、方案A和爆发检测共用）──────────────────────────
    m1, m2, m3 = _get_recent_3months(sku_data, current_year, current_month)
    recent_total = m1 + m2 + m3
    recent_avg   = recent_total / 3 if recent_total > 0 else 0

    # ── L1 路径：去年同期存在 & 趋势因子有效 ────────────────────────────
    if yoy_sales > 0 and trend_factor is not None and trend_factor > 0:

        yoy_pred = int(yoy_sales * trend_factor)

        # ── 爆发检测：同比因子需要钳位 AND 近期环比也在快速增长 ───────────
        is_clamped  = raw_trend_factor > TREND_FACTOR_MAX
        recent_growth = _calc_recent_growth(m1, m2, m3)
        is_explosive = is_clamped and recent_growth > EXPLOSIVE_GROWTH_THRESHOLD

        if is_explosive:
            # 真正爆火：信任L3近期趋势，保留季节性
            val, method = _calc_new_product_forecast(
                sku_data, current_year, current_month, forecast_step
            )
            if val > 0:
                return val, f"L3_爆发检测(同比{raw_trend_factor:.1f}倍+环比{recent_growth:.0%})→阻尼增长"

        # ── 方案C：动态α混合（高基数效应压制）──────────────────────────
        if trend_factor >= ALPHA_HIGH_THRESHOLD:
            alpha = ALPHA_HIGH      # 0.3：高度依赖近期均值
        elif trend_factor >= ALPHA_MID_THRESHOLD:
            alpha = ALPHA_MID       # 0.5：均衡
        else:
            alpha = ALPHA_NORMAL    # 0.7：主要依赖同比（原逻辑接近）

        if recent_avg > 0:
            blended = int(yoy_pred * alpha + recent_avg * (1 - alpha))
            # ── 方案A：环比上限兜底 ───────────────────────────────────────
            mom_cap = int(recent_avg * MOM_CAP_RATIO)
            final   = min(blended, mom_cap)

            alpha_note  = f"α={alpha}"
            cap_applied = "→上限截断" if final < blended else ""
            return final, (
                f"L1_同比趋势+方案C({alpha_note},同比{yoy_pred}件×{alpha:.0%}"
                f"+近3月均值{int(recent_avg)}件×{1-alpha:.0%}={blended}件"
                f"{cap_applied})"
            )
        else:
            # 近3月无销量，直接用L1
            return yoy_pred, "L1_同比趋势(近期无销量)"

    # ── L2：去年同期存在但趋势因子为0 ───────────────────────────────────
    if yoy_sales > 0 and trend_factor == 0:
        return int(yoy_sales), "L2_去年同期"

    # ── L3：新品/无同期数据 ──────────────────────────────────────────────
    val, method = _calc_new_product_forecast(
        sku_data, current_year, current_month, forecast_step
    )
    if val > 0:
        return val, method

    # ── L4：SPU趋势兜底 ──────────────────────────────────────────────────
    if yoy_sales > 0 and spu_trend_factor is not None and spu_trend_factor > 0:
        return int(yoy_sales * spu_trend_factor), "L4_SPU趋势兜底"

    return 0, "L5_无数据"


# ────────────────────────────────────────────────────────────────────────────
# 对外接口（v2：传入 raw_trend_factor）
# ────────────────────────────────────────────────────────────────────────────

def compute_forecast_for_shop(
    shop_data: Dict[str, Dict[str, Any]],
    forecast_sales_labels: List[str],
    current_date: datetime = None,
) -> Dict[str, Dict[str, Any]]:
    import re as _re

    if current_date is None:
        current_date = datetime.now()

    current_year  = current_date.year
    current_month = current_date.month
    last_month_year, last_month = _offset_month(current_year, current_month, -1)

    # Step1：计算每个SKU的趋势因子（clamped + raw）
    sku_trend:     Dict[str, Optional[float]] = {}
    sku_trend_raw: Dict[str, float]           = {}
    for sku, sku_data in shop_data.items():
        tf, raw_tf, _ = _calc_weighted_trend_factor(sku_data, last_month_year, last_month)
        sku_trend[sku]     = tf
        sku_trend_raw[sku] = raw_tf

    # Step2：SPU级趋势因子均值
    spu_trend:   Dict[str, float]       = {}
    spu_sku_map: Dict[str, List[str]]   = {}
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

    # Step4：逐SKU生成预测
    result: Dict[str, Dict[str, Any]] = {}
    for sku, sku_data in shop_data.items():
        tf     = sku_trend[sku]
        raw_tf = sku_trend_raw[sku]
        spu    = (sku_data.get("SPU") or "").strip()
        spu_tf = spu_trend.get(spu)

        sku_result: Dict[str, Any] = {
            "趋势因子": tf if tf is not None else 0.0,
        }
        method_labels = []
        for idx, (fy, fm, flabel) in enumerate(forecast_months):
            val, method = _forecast_single_month(
                sku_data,
                fy, fm,
                current_year, current_month,
                tf,
                raw_tf,      # v2新增
                spu_tf,
                forecast_step=idx,
            )
            sku_result[flabel] = val
            method_labels.append(f"{fm}月:{method}")

        sku_result["预测方法"] = "；".join(method_labels)
        result[sku] = sku_result

    return result


# ────────────────────────────────────────────────────────────────────────────
# 单测：验证3个场景
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    current_date = datetime(2026, 5, 24)

    print("=" * 70)
    print("场景1：BX389-BW-S（4月高点后下滑，方案C应压制）")
    print("=" * 70)
    bx389_data = {
        "26年4月销量": 2422, "26年3月销量": 1720, "26年2月销量": 924,
        "25年4月销量": 973,  "25年3月销量": 0,    "25年2月销量": 0,
        "25年6月销量": 1737, "25年7月销量": 1410, "25年8月销量": 842,
        "SPU": "BX389",
    }
    labels = ["26年6月预计销量", "26年7月预计销量", "26年8月预计销量"]
    r1 = compute_forecast_for_shop({"BX389-BW-S": bx389_data}, labels, current_date)
    print(f"  近3月：4月2422 → 3月1720 → 2月924，均值1689")
    print(f"  5月实际销量：1731件（已下滑）")
    for lbl in labels:
        print(f"  {lbl}：{r1['BX389-BW-S'][lbl]} 件")
    print(f"  预测方法：{r1['BX389-BW-S']['预测方法']}")

    print()
    print("=" * 70)
    print("场景2：爆火产品（持续快速上涨，方案C应放行走L3）")
    print("=" * 70)
    explosive_data = {
        "26年4月销量": 2000, "26年3月销量": 800, "26年2月销量": 200,
        "25年4月销量": 35,   "25年3月销量": 0,   "25年2月销量": 0,
        "25年6月销量": 200,  "25年7月销量": 350, "25年8月销量": 500,
        "SPU": "NEW001",
    }
    r2 = compute_forecast_for_shop({"NEW001-BK-S": explosive_data}, labels, current_date)
    print(f"  近3月：4月2000 → 3月800 → 2月200，环比 +150%/+75%")
    print(f"  同比因子：2000/35=57倍 → 钳位3.0，触发爆发检测")
    for lbl in labels:
        print(f"  {lbl}：{r2['NEW001-BK-S'][lbl]} 件")
    print(f"  预测方法：{r2['NEW001-BK-S']['预测方法']}")

    print()
    print("=" * 70)
    print("场景3：正常稳定增长产品（不受方案C误伤）")
    print("=" * 70)
    normal_data = {
        "26年4月销量": 260, "26年3月销量": 230, "26年2月销量": 210,
        "25年4月销量": 200, "25年3月销量": 190, "25年2月销量": 170,
        "25年6月销量": 300, "25年7月销量": 350, "25年8月销量": 280,
        "SPU": "NORMAL",
    }
    r3 = compute_forecast_for_shop({"NORMAL-BK-S": normal_data}, labels, current_date)
    print(f"  近3月：4月260 → 3月230 → 2月210，均值233")
    print(f"  同比因子：260/200=1.30（未超过1.5，α=0.7）")
    for lbl in labels:
        print(f"  {lbl}：{r3['NORMAL-BK-S'][lbl]} 件")
    print(f"  预测方法：{r3['NORMAL-BK-S']['预测方法']}")

    print()
    print("✅ 验证完毕")
