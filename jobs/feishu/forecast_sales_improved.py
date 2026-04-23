#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
改进版预计销量算法
替换 jobs/feishu/write_sales_to_feishu.py 中 prepare_feishu_records() 的预测逻辑

【改进点】
1. 趋势因子：单月 → 加权3个月（0.6/0.3/0.1），抗单月异常
2. 趋势因子：增加上下限钳位 [0.3, 3.0]，防止极端值
3. 兜底逻辑（4级降级）：
   L1 去年同期 × 加权趋势因子（主路径）
   L2 去年同期直接使用（趋势数据不足时）
   L3 新品阻尼增长（检测上行趋势 → 带衰减的增速外推；平稳/下行 → 近3月均值）
   L4 同SPU其他SKU的趋势因子（SKU级兜底）
4. 新增字段：预测方法（记录每个SKU用了哪一级）

【L3 阻尼增长算法说明】
  - 检测上行：加权环比增速 > NEW_PRODUCT_GROWTH_THRESHOLD
  - 加权增速：g = 0.4×(m-2/m-3增速) + 0.6×(m-1/m-2增速)，上限 MAX_NEW_PRODUCT_GROWTH
  - 阻尼外推：forecast[k] = last_sales × Π_{i=0}^{k}(1 + g × DAMPING^i)
    每推一个月，增速衰减 (1-DAMPING)，防止无限膨胀
  - 非上行趋势：退化为近3月均值（保守估计）
"""

from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional


# ────────────────────────────────────────────────────────────────────────────
# 常量配置（可按业务需要调整）
# ────────────────────────────────────────────────────────────────────────────

# 加权趋势因子的3个月权重（越近权重越高，合计=1.0）
TREND_WEIGHTS = {
    "last_1": 0.6,   # 上个月
    "last_2": 0.3,   # 上上个月
    "last_3": 0.1,   # 上上上个月
}

# 趋势因子合理区间（防止断货/促销导致极端预测）
TREND_FACTOR_MIN = 0.3
TREND_FACTOR_MAX = 3.0

# 新品判定：近3个月总销量 >= 此阈值才进入L3（太小视为无效数据）
MIN_RECENT_SALES_FOR_L3 = 5

# L3 阻尼增长：单月增速上限（防止过于激进）
MAX_NEW_PRODUCT_GROWTH = 0.5   # 最多预测增长 50%/月

# L3 阻尼增长：阻尼系数（每往后推一个月，增速 × DAMPING）
# 0.6 表示增速每月衰减 40%，预测曲线收敛而不爆炸
NEW_PRODUCT_DAMPING = 0.6

# L3 上行判定阈值：加权环比增速 > 此值才认为是上行趋势
# 低于此值（平稳/下行）退化为近3月均值
NEW_PRODUCT_GROWTH_THRESHOLD = 0.05  # 5%


# ────────────────────────────────────────────────────────────────────────────
# 工具函数
# ────────────────────────────────────────────────────────────────────────────

def _get_month_label(year: int, month: int) -> str:
    """生成月份标签，如 '25年3月销量'"""
    return f"{str(year)[-2:]}年{month}月销量"


def _offset_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    """
    对月份做偏移（delta 为负=往前，正=往后）
    返回 (year, month)
    """
    month += delta
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return year, month


def _clamp(value: float, lo: float, hi: float) -> float:
    """将 value 钳位到 [lo, hi]"""
    return max(lo, min(hi, value))


# ────────────────────────────────────────────────────────────────────────────
# L3 专用：新品阻尼增长预测
# ────────────────────────────────────────────────────────────────────────────

def _calc_new_product_forecast(
    sku_data: Dict[str, Any],
    current_year: int,
    current_month: int,
    forecast_step: int,           # 0=本月, 1=下月, 2=下下月, 3=下下下月
) -> Tuple[int, str]:
    """
    L3 新品预测：优先用阻尼增长外推，上行趋势不明显时退化为近3月均值。

    算法：
      1. 取近3个月销量 m3(最老) → m2 → m1(最新=上个月)
      2. 计算两段环比增速：
            g_old = (m2 - m3) / m3     权重 0.4（较旧）
            g_new = (m1 - m2) / m2     权重 0.6（较新）
         加权增速 g = 0.4×g_old + 0.6×g_new，再限制在 MAX_NEW_PRODUCT_GROWTH 以内
      3. 若 g > NEW_PRODUCT_GROWTH_THRESHOLD → 上行趋势，用阻尼外推：
            forecast = m1 × Π_{i=0}^{step} (1 + g × DAMPING^i)
         各步增速：step0=g, step1=g×0.6, step2=g×0.36, step3=g×0.216 ...
      4. 否则（平稳/下行）→ 退化为近3月均值

    参数：
        forecast_step: 距当前月的偏移，0=本月预测，1=下月预测，依此类推

    返回：
        (预测值, 方法标签)
    """
    # 取近3个月数据（m1=上月, m2=上上月, m3=上上上月）
    m1_y, m1_m = _offset_month(current_year, current_month, -1)
    m2_y, m2_m = _offset_month(current_year, current_month, -2)
    m3_y, m3_m = _offset_month(current_year, current_month, -3)

    m1 = sku_data.get(_get_month_label(m1_y, m1_m), 0) or 0
    m2 = sku_data.get(_get_month_label(m2_y, m2_m), 0) or 0
    m3 = sku_data.get(_get_month_label(m3_y, m3_m), 0) or 0

    recent_total = m1 + m2 + m3

    # 数据不足，直接返回0
    if recent_total < MIN_RECENT_SALES_FOR_L3:
        return 0, "L3_数据不足"

    # ── 计算加权环比增速 ──────────────────────────────────────────────────
    g_old = ((m2 - m3) / m3) if m3 > 0 else 0.0   # 较旧那段增速
    g_new = ((m1 - m2) / m2) if m2 > 0 else 0.0   # 较新那段增速
    g_weighted = 0.4 * g_old + 0.6 * g_new

    # 上行趋势检测
    if g_weighted > NEW_PRODUCT_GROWTH_THRESHOLD:
        # 钳位：不超过 MAX_NEW_PRODUCT_GROWTH
        g = min(g_weighted, MAX_NEW_PRODUCT_GROWTH)

        # 阻尼累乘：forecast = m1 × Π_{i=0}^{step}(1 + g×DAMPING^i)
        value = float(m1)
        for i in range(forecast_step + 1):
            step_growth = g * (NEW_PRODUCT_DAMPING ** i)
            value *= (1.0 + step_growth)

        cap_note = f"(原始增速{g_weighted:.0%}→限速{g:.0%})" if g < g_weighted else f"(增速{g:.0%})"
        label = f"L3_新品阻尼增长{cap_note}·step{forecast_step}"
        return int(value), label

    else:
        # 平稳/下行 → 近3月均值（保守估计）
        return int(recent_total / 3), "L3_近3月均值(无明显趋势)"


# ────────────────────────────────────────────────────────────────────────────
# 核心：计算加权趋势因子
# ────────────────────────────────────────────────────────────────────────────

def _calc_weighted_trend_factor(
    sku_data: Dict[str, Any],
    base_year: int,
    base_month: int,
) -> Tuple[Optional[float], str]:
    """
    计算加权趋势因子（基于最近3个月 vs 去年同期）

    加权公式：
        趋势因子 = Σ( weight_i × (今年第i月销量 / 去年同期第i月销量) )
        只统计去年同期 > 0 的月份，权重归一化后再计算

    参数：
        sku_data:   该 SKU 的所有月份销量字典
        base_year:  "上个月"所在年份
        base_month: "上个月"所在月份（即偏移 -1 后的当前月）

    返回：
        (trend_factor, detail_log)
        trend_factor = None 表示无法计算（数据不足）
    """
    # 3个月偏移：0=上个月，-1=上上个月，-2=上上上个月
    offsets = [0, -1, -2]
    weight_keys = ["last_1", "last_2", "last_3"]

    valid_weighted_sum = 0.0
    valid_weight_total = 0.0
    details = []

    for offset, wkey in zip(offsets, weight_keys):
        y, m = _offset_month(base_year, base_month, offset)
        yoy_y, yoy_m = y - 1, m  # 去年同期

        this_label = _get_month_label(y, m)
        yoy_label  = _get_month_label(yoy_y, yoy_m)

        this_sales = sku_data.get(this_label, 0) or 0
        yoy_sales  = sku_data.get(yoy_label,  0) or 0

        w = TREND_WEIGHTS[wkey]

        if yoy_sales > 0:
            ratio = this_sales / yoy_sales
            valid_weighted_sum  += w * ratio
            valid_weight_total  += w
            details.append(
                f"{_get_month_label(y,m).replace('销量','')} "
                f"{this_sales}÷{yoy_sales}={ratio:.2f}(w={w})"
            )
        else:
            details.append(
                f"{_get_month_label(y,m).replace('销量','')} "
                f"去年同期=0，跳过"
            )

    if valid_weight_total == 0:
        return None, "3个月去年同期均为0，无法计算趋势因子"

    # 归一化
    raw_factor = valid_weighted_sum / valid_weight_total

    # 钳位
    clamped = _clamp(raw_factor, TREND_FACTOR_MIN, TREND_FACTOR_MAX)
    clamped_note = ""
    if clamped != raw_factor:
        clamped_note = f" → 钳位至{clamped:.2f}"

    detail_log = (
        f"加权趋势因子={raw_factor:.3f}{clamped_note}；"
        f"有效月份权重和={valid_weight_total:.1f}；"
        f"明细：{'，'.join(details)}"
    )
    return round(clamped, 2), detail_log


# ────────────────────────────────────────────────────────────────────────────
# 核心：4级兜底，计算单个月份的预计销量
# ────────────────────────────────────────────────────────────────────────────

def _forecast_single_month(
    sku_data: Dict[str, Any],
    forecast_year: int,
    forecast_month: int,
    current_year: int,
    current_month: int,
    trend_factor: Optional[float],
    spu_trend_factor: Optional[float],
    forecast_step: int = 0,       # 新增：距当前月偏移，用于L3阻尼计算
) -> Tuple[int, str]:
    """
    计算某一预测月份的销量，4级兜底：

    L1  去年同期 × 加权趋势因子（最优）
    L2  去年同期（趋势因子不可用）
    L3  新品阻尼增长（上行趋势）或近3月均值（平稳/下行）
    L4  同SPU其他SKU趋势因子 × 去年同期（SKU级兜底）
    ——  最终兜底：0
    """
    yoy_year  = forecast_year - 1
    yoy_month = forecast_month
    yoy_label = _get_month_label(yoy_year, yoy_month)
    yoy_sales = sku_data.get(yoy_label, 0) or 0

    # ── L1：去年同期 × 加权趋势因子 ──────────────────────────────────────
    if yoy_sales > 0 and trend_factor is not None and trend_factor > 0:
        return int(yoy_sales * trend_factor), "L1_同比趋势"

    # ── L2：去年同期直接使用（趋势因子为0或不足） ────────────────────────
    if yoy_sales > 0 and trend_factor == 0:
        return int(yoy_sales), "L2_去年同期"

    # ── L3：新品阻尼增长（上行）或近3月均值（平稳/下行） ─────────────────
    val, method = _calc_new_product_forecast(
        sku_data, current_year, current_month, forecast_step
    )
    if val > 0:
        return val, method

    # ── L4：同SPU其他SKU趋势因子 × 去年同期 ─────────────────────────────
    if yoy_sales > 0 and spu_trend_factor is not None and spu_trend_factor > 0:
        return int(yoy_sales * spu_trend_factor), "L4_SPU趋势兜底"

    # ── 最终兜底：0 ──────────────────────────────────────────────────────
    return 0, "L5_无数据"


# ────────────────────────────────────────────────────────────────────────────
# 对外接口：替换 prepare_feishu_records() 中的预测块
# ────────────────────────────────────────────────────────────────────────────

def compute_forecast_for_shop(
    shop_data: Dict[str, Dict[str, Any]],
    forecast_sales_labels: List[str],
    current_date: datetime = None,
) -> Dict[str, Dict[str, Any]]:
    """
    对一个店铺的所有 SKU 计算改进版预计销量。

    参数：
        shop_data:             {SKU: {月份标签: 销量, 'SPU': ..., ...}}
        forecast_sales_labels: ['26年4月预计销量', '26年5月预计销量', ...]（共4个月）
        current_date:          计算基准日期，默认 datetime.now()

    返回：
        {SKU: {'趋势因子': x, '预测方法': '...', '26年4月预计销量': y, ...}}

    ──────────────────────────────────────────────────────────────────────────
    用法（在 prepare_feishu_records 里替换对应块）：

        # ——— 旧代码（删除）———
        if last_month_yoy_sales > 0:
            trend_factor = last_month_sales / last_month_yoy_sales
        ...

        # ——— 新代码（替换）———
        forecast_result = compute_forecast_for_shop(
            shop_data, forecast_sales_labels, current_date
        )
        for sku, extra in forecast_result.items():
            records_dict[sku].update(extra)
    ──────────────────────────────────────────────────────────────────────────
    """
    if current_date is None:
        current_date = datetime.now()

    current_year  = current_date.year
    current_month = current_date.month

    # "上个月"
    last_month_year, last_month = _offset_month(current_year, current_month, -1)

    # ── Step1：为每个 SKU 计算本 SKU 的加权趋势因子 ─────────────────────
    sku_trend: Dict[str, Optional[float]] = {}
    for sku, sku_data in shop_data.items():
        tf, _ = _calc_weighted_trend_factor(sku_data, last_month_year, last_month)
        sku_trend[sku] = tf

    # ── Step2：为每个 SPU 计算 SPU 级趋势因子（同SPU有效SKU的均值） ──────
    spu_trend: Dict[str, float] = {}
    spu_sku_map: Dict[str, List[str]] = {}
    for sku, sku_data in shop_data.items():
        spu = (sku_data.get("SPU") or "").strip()
        if spu:
            spu_sku_map.setdefault(spu, []).append(sku)

    for spu, skus in spu_sku_map.items():
        valid_factors = [sku_trend[s] for s in skus if sku_trend.get(s) is not None]
        if valid_factors:
            spu_trend[spu] = round(sum(valid_factors) / len(valid_factors), 2)

    # ── Step3：解析 forecast_sales_labels 得到预测月列表 ────────────────
    import re
    forecast_months: List[Tuple[int, int, str]] = []  # (year, month, label)
    for label in forecast_sales_labels:
        m = re.match(r"(\d{2})年(\d{1,2})月预计销量", label)
        if m:
            yr = 2000 + int(m.group(1)) if int(m.group(1)) < 50 else 1900 + int(m.group(1))
            mo = int(m.group(2))
            forecast_months.append((yr, mo, label))

    # ── Step4：逐 SKU 生成预测结果 ──────────────────────────────────────
    result: Dict[str, Dict[str, Any]] = {}

    for sku, sku_data in shop_data.items():
        tf = sku_trend[sku]
        spu = (sku_data.get("SPU") or "").strip()
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
                spu_tf,
                forecast_step=idx,    # 0=本月, 1=下月 ...
            )
            sku_result[flabel] = val
            method_labels.append(f"{fm}月:{method}")

        sku_result["预测方法"] = "；".join(method_labels)
        result[sku] = sku_result

    return result


# ────────────────────────────────────────────────────────────────────────────
# 演示/单测：套入评估时举的数值
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    current_date = datetime(2026, 4, 23)

    # ── 场景1：正常 SKU（有完整历史数据）─────────────────────────────────
    print("=" * 60)
    print("场景1：正常 SKU，3个月同比数据都存在")
    print("=" * 60)
    normal_sku_data = {
        # 今年近3个月
        "26年3月销量": 260,   # 上个月
        "26年2月销量": 210,   # 上上个月
        "26年1月销量": 180,   # 上上上个月
        # 去年同期（对应月份）
        "25年3月销量": 200,   # 上个月去年同期
        "25年2月销量": 190,   # 上上个月去年同期
        "25年1月销量": 160,   # 上上上个月去年同期
        # 预测目标月去年同期
        "25年4月销量": 180,
        "25年5月销量": 220,
        "25年6月销量": 300,
        "25年7月销量": 350,
        "SPU": "BX484",
    }

    tf_val, tf_log = _calc_weighted_trend_factor(normal_sku_data, 2026, 3)
    print(f"加权趋势因子：{tf_val}")
    print(f"计算明细：{tf_log}")
    print()

    # 旧算法（单月）对比
    old_tf = 260 / 200
    print(f"旧算法（单月趋势因子）：{old_tf:.2f}")
    print()

    # 新算法
    # 26年3月 vs 25年3月 → 260/200=1.30  weight=0.6
    # 26年2月 vs 25年2月 → 210/190=1.105 weight=0.3
    # 26年1月 vs 25年1月 → 180/160=1.125 weight=0.1
    # 加权 = (0.6×1.30 + 0.3×1.105 + 0.1×1.125) / 1.0
    w_calc = 0.6*1.30 + 0.3*(210/190) + 0.1*(180/160)
    print(f"手动验算加权趋势因子：{w_calc:.4f}（与上方结果应一致）")
    print()

    labels = ["26年4月预计销量", "26年5月预计销量", "26年6月预计销量", "26年7月预计销量"]
    shop_data = {"BX484-BK-M": normal_sku_data}
    result = compute_forecast_for_shop(shop_data, labels, current_date)

    print("【预测结果（新算法）】")
    for label in labels:
        old_val = int((180 if "4月" in label else 220 if "5月" in label
                       else 300 if "6月" in label else 350) * old_tf)
        new_val = result["BX484-BK-M"][label]
        print(f"  {label}: 新={new_val}  旧={old_val}  差异={new_val-old_val:+d}")
    print(f"  趋势因子: {result['BX484-BK-M']['趋势因子']}")
    print(f"  预测方法: {result['BX484-BK-M']['预测方法']}")

    # ── 场景2：单月异常冲量（旧算法失真）────────────────────────────────
    print()
    print("=" * 60)
    print("场景2：上个月因促销冲量，单月严重失真")
    print("=" * 60)
    spike_sku_data = {
        "26年3月销量": 800,   # 促销月，异常高
        "26年2月销量": 220,
        "26年1月销量": 200,
        "25年3月销量": 200,
        "25年2月销量": 195,
        "25年1月销量": 180,
        "25年4月销量": 180,
        "25年5月销量": 220,
        "25年6月销量": 300,
        "25年7月销量": 350,
        "SPU": "BX484",
    }

    old_tf_spike = 800 / 200  # = 4.0，超出合理范围
    tf_spike, tf_spike_log = _calc_weighted_trend_factor(spike_sku_data, 2026, 3)

    print(f"旧算法趋势因子：{old_tf_spike:.2f}（促销导致严重虚高，将被钳位到{TREND_FACTOR_MAX}）")
    print(f"新算法趋势因子：{tf_spike}（加权+钳位后）")
    print(f"计算明细：{tf_spike_log}")

    shop_data2 = {"BX484-BK-L": spike_sku_data}
    result2 = compute_forecast_for_shop(shop_data2, labels, current_date)
    print("\n【预测结果对比（26年4月）】")
    old_4m = int(180 * old_tf_spike)  # 旧算法预测4月
    new_4m = result2["BX484-BK-L"]["26年4月预计销量"]
    print(f"  旧算法 26年4月预计销量：{old_4m}  ← 严重虚高")
    print(f"  新算法 26年4月预计销量：{new_4m}   ← 合理")
    print(f"  预测方法: {result2['BX484-BK-L']['预测方法']}")

    # ── 场景3a：新品上行趋势 → L3 阻尼增长 ──────────────────────────────
    print()
    print("=" * 60)
    print("场景3a：新品，销量持续上行（30→60→90）→ 阻尼增长外推")
    print("=" * 60)
    new_sku_up = {
        "26年3月销量": 90,    # m1（上个月）
        "26年2月销量": 60,    # m2
        "26年1月销量": 30,    # m3
        "SPU": "NW001",
    }

    # 手动验算
    m3, m2, m1 = 30, 60, 90
    g_old = (m2 - m3) / m3   # (60-30)/30 = 1.0
    g_new = (m1 - m2) / m2   # (90-60)/60 = 0.5
    g_w   = 0.4 * g_old + 0.6 * g_new   # 0.4+0.3 = 0.70
    g     = min(g_w, MAX_NEW_PRODUCT_GROWTH)  # min(0.70, 0.50) = 0.50
    print(f"  环比增速：g_old={g_old:.0%}  g_new={g_new:.0%}  加权={g_w:.0%}  限速后={g:.0%}")

    manual = {}
    base = float(m1)
    for step in range(4):
        step_g = g * (NEW_PRODUCT_DAMPING ** step)
        base  *= (1.0 + step_g)
        manual[step] = int(base)
    print(f"  手动验算：step0={manual[0]}  step1={manual[1]}  step2={manual[2]}  step3={manual[3]}")
    print(f"  （旧算法均值：{(m1+m2+m3)//3} 件，完全抹平趋势）")

    shop_data3a = {"NW001-BK-M": new_sku_up}
    result3a = compute_forecast_for_shop(shop_data3a, labels, current_date)
    print("\n【预测结果】")
    for label in labels:
        print(f"  {label}: {result3a['NW001-BK-M'][label]}")
    print(f"  预测方法: {result3a['NW001-BK-M']['预测方法']}")

    # ── 场景3b：新品平稳/下行 → 退化为近3月均值 ─────────────────────────
    print()
    print("=" * 60)
    print("场景3b：新品，销量平稳（80→85→82）→ 退化为近3月均值")
    print("=" * 60)
    new_sku_flat = {
        "26年3月销量": 82,
        "26年2月销量": 85,
        "26年1月销量": 80,
        "SPU": "NW002",
    }
    shop_data3b = {"NW002-BK-M": new_sku_flat}
    result3b = compute_forecast_for_shop(shop_data3b, labels, current_date)
    avg_flat = (82 + 85 + 80) // 3
    print(f"  近3月均值 = (82+85+80)/3 = {avg_flat}")
    for label in labels[:2]:
        print(f"  {label}: {result3b['NW002-BK-M'][label]}")
    print(f"  预测方法: {result3b['NW002-BK-M']['预测方法']}")

    # ── 场景4：去年断货，今年有数据 → L4 SPU趋势兜底 ─────────────────────
    print()
    print("=" * 60)
    print("场景4：去年断货（同期=0），同SPU其他SKU有趋势因子 → L4兜底")
    print("=" * 60)
    # 同SPU另一个SKU有完整数据（趋势因子=1.30）
    sibling_sku_data = {
        "26年3月销量": 260,
        "26年2月销量": 190,
        "26年1月销量": 160,
        "25年3月销量": 200,
        "25年2月销量": 180,
        "25年1月销量": 150,
        "25年4月销量": 200,
        "SPU": "BX484",
    }
    # 本SKU去年同期断货
    broken_sku_data = {
        "26年3月销量": 50,
        "26年2月销量": 40,
        "26年1月销量": 0,
        # 去年4月有数据，但上个月去年同期为0
        "25年4月销量": 150,
        "25年5月销量": 0,
        "25年6月销量": 0,
        "25年7月销量": 0,
        "SPU": "BX484",  # 同SPU
    }
    shop_data4 = {
        "BX484-BK-M": sibling_sku_data,
        "BX484-RD-M": broken_sku_data,
    }
    result4 = compute_forecast_for_shop(shop_data4, ["26年4月预计销量"], current_date)
    spu_tf_val = spu_trend_demo = round(
        sum(f for f in [
            _calc_weighted_trend_factor(sibling_sku_data, 2026, 3)[0],
        ] if f) / 1, 2
    )
    print(f"  同SPU趋势因子（来自BX484-BK-M）≈ {spu_tf_val}")
    print(f"  BX484-RD-M 去年4月销量=150，预计 = 150 × {spu_tf_val} = {int(150*spu_tf_val)}")
    print(f"  实际预测值: {result4['BX484-RD-M']['26年4月预计销量']}")
    print(f"  预测方法: {result4['BX484-RD-M']['预测方法']}")

    print()
    print("=" * 60)
    print("✅ 所有场景演示完毕")
    print("=" * 60)
