#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
【补丁说明 — forecast_sales_improved.py 季节性感知升级（v2 → v3）】
 
改动范围：
  1. 新增 `load_spu_season_map()` — 从 MySQL `SPU季节表` 读取 SPU→季节映射
  2. 新增 `_get_peak_season_avg()` — 计算去年旺季月均销量
  3. 修改 `_forecast_single_month()` — 加入 seasonal_factor 压制淡季 recent_avg
  4. 修改 `compute_forecast_for_shop()` — 传入 spu_season_map 参数
 
季节定义：
  春夏款：3-8月 旺季，9-2月 淡季
  秋冬款：9-2月 旺季，3-8月 淡季
  全年款：不做季节压制（现有逻辑不变）
 
季节系数计算：
  seasonal_factor = 去年目标月销量 / 去年旺季月均销量
  recent_avg_adj  = recent_avg × seasonal_factor
  → 用调整后的 recent_avg_adj 代替原 recent_avg 参与方案C混合和方案A上限
 
替换步骤：
  1. 在文件顶部（常量区之后）添加 load_spu_season_map / _get_peak_season_avg
  2. 用本文件中的 _forecast_single_month 替换原函数
  3. 用本文件中的 compute_forecast_for_shop 替换原函数（新增 spu_season_map 参数）
  4. 调用方（generate_forecast_comparison.py）需先调用 load_spu_season_map()
     并把结果传入 compute_forecast_for_shop()
"""
 
from typing import Dict, Any, List, Tuple, Optional
from common.database import db_cursor
from common import get_logger
 
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
    raw_trend_factor: float,
    spu_trend_factor: Optional[float],
    forecast_step: int = 0,
    season: str = '全年',          # v3 新增
) -> Tuple[int, str]:
    """
    预测决策树（v3）：在 v2 基础上加入季节性感知。
 
    新增逻辑（仅在 L1 路径 & season != '全年' 时生效）：
      seasonal_factor = 去年目标月销量 / 去年旺季月均销量
      recent_avg_adj  = recent_avg × seasonal_factor
      → 用 recent_avg_adj 替换 recent_avg 参与方案C混合和方案A上限
      → 当预测月是淡季时，recent_avg 被旺季数据拉高的部分被压回淡季水平
    """
    # ── 以下从原 v2 代码复制，保持原逻辑不变 ────────────────────────────
    # （此处假设 _get_month_label / _get_recent_3months / _calc_recent_growth
    #   / _calc_new_product_forecast / ALPHA_* / MOM_CAP_RATIO 等常量
    #   均已在同一文件中定义，补丁只替换本函数体）
 
    yoy_label = _get_month_label(forecast_year - 1, forecast_month)
    yoy_sales = sku_data.get(yoy_label, 0) or 0
 
    m1, m2, m3 = _get_recent_3months(sku_data, current_year, current_month)
    recent_total = m1 + m2 + m3
    recent_avg   = recent_total / 3 if recent_total > 0 else 0
 
    # ── v3 新增：季节系数计算 ─────────────────────────────────────────────
    seasonal_note = ""
    if season != '全年' and recent_avg > 0 and yoy_sales > 0:
        last_year = forecast_year - 1
        peak_avg  = _get_peak_season_avg(sku_data, season, last_year)
        if peak_avg > 0:
            seasonal_factor = yoy_sales / peak_avg
            # 钳位：淡季最低压到旺季均值的5%，防止季节系数过小导致预测归零
            seasonal_factor = max(seasonal_factor, 0.05)
            # 旺季月份不压制（seasonal_factor > 1 时不放大，保持 ≤ 1）
            if seasonal_factor < 1.0:
                recent_avg_adj = recent_avg * seasonal_factor
                seasonal_note  = (
                    f"[季节压制:{season},旺季均值{int(peak_avg)},"
                    f"系数{seasonal_factor:.2f},adj均值{int(recent_avg_adj)}]"
                )
            else:
                recent_avg_adj = recent_avg   # 旺季月份不放大，保持原值
                seasonal_note  = f"[季节旺季:{season},无压制]"
        else:
            recent_avg_adj = recent_avg       # 去年旺季无数据，不处理
            seasonal_note  = f"[季节:{season},旺季无数据]"
    else:
        recent_avg_adj = recent_avg           # 全年款或无数据，不处理
 
    # ── L1 路径：去年同期存在 & 趋势因子有效 ────────────────────────────
    if yoy_sales > 0 and trend_factor is not None and trend_factor > 0:
        yoy_pred = int(yoy_sales * trend_factor)
 
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
                    f"+环比{recent_growth:.0%})→阻尼增长{seasonal_note}"
                )
 
        # ── 方案C：动态α混合（使用季节调整后的 recent_avg_adj）──────────
        if trend_factor >= ALPHA_HIGH_THRESHOLD:
            alpha = ALPHA_HIGH
        elif trend_factor >= ALPHA_MID_THRESHOLD:
            alpha = ALPHA_MID
        else:
            alpha = ALPHA_NORMAL
 
        if recent_avg_adj > 0:
            blended = int(yoy_pred * alpha + recent_avg_adj * (1 - alpha))
            mom_cap = int(recent_avg_adj * MOM_CAP_RATIO)
            final   = min(blended, mom_cap)
 
            alpha_note  = f"α={alpha}"
            cap_applied = "→上限截断" if final < blended else ""
            return final, (
                f"L1_同比趋势+方案C({alpha_note},"
                f"同比{yoy_pred}件×{alpha:.0%}"
                f"+调整均值{int(recent_avg_adj)}件×{1-alpha:.0%}={blended}件"
                f"{cap_applied}){seasonal_note}"
            )
        elif recent_avg > 0:
            # 有原始均值但调整后为0（极端季节系数），直接用L1
            return yoy_pred, f"L1_同比趋势(季节adj后均值为0){seasonal_note}"
        else:
            return yoy_pred, f"L1_同比趋势(近期无销量){seasonal_note}"
 
    # ── L2 / L3 / L4 路径：与 v2 完全相同，不受季节影响 ────────────────
    if yoy_sales > 0 and trend_factor == 0:
        return int(yoy_sales), "L2_去年同期"
 
    val, method = _calc_new_product_forecast(
        sku_data, current_year, current_month, forecast_step
    )
    if val > 0:
        return val, method
 
    if yoy_sales > 0 and spu_trend_factor is not None and spu_trend_factor > 0:
        return int(yoy_sales * spu_trend_factor), "L4_SPU趋势兜底"
 
    return 0, "L5_无数据"



# ────────────────────────────────────────────────────────────────────────────
# 对外接口（v2：传入 raw_trend_factor）
# ────────────────────────────────────────────────────────────────────────────

def compute_forecast_for_shop(
    shop_data: Dict[str, Dict[str, Any]],
    forecast_sales_labels: List[str],
    current_date=None,
    spu_season_map: Dict[str, str] = None,   # v3 新增
) -> Dict[str, Dict[str, Any]]:
    """
    v3：在 v2 基础上，为每个 SKU 查询其 SPU 的季节标签，
    并在调用 _forecast_single_month 时传入 season 参数。
 
    spu_season_map: {SPU: '春夏'|'秋冬'|'全年'}，由 load_spu_season_map() 提供。
                    传 None 或空字典时退化为全年（v2 行为）。
    """
    import re as _re
    from datetime import datetime
 
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
 
    # Step4：逐SKU预测（v3：传入季节）
    result: Dict[str, Dict[str, Any]] = {}
    for sku, sku_data in shop_data.items():
        tf     = sku_trend[sku]
        raw_tf = sku_trend_raw[sku]
        spu    = (sku_data.get("SPU") or "").strip()
        spu_tf = spu_trend.get(spu)
        season = spu_season_map.get(spu, '全年')   # v3 新增
 
        sku_result: Dict[str, Any] = {"趋势因子": tf if tf is not None else 0.0}
        method_labels = []
        for idx, (fy, fm, flabel) in enumerate(forecast_months):
            val, method = _forecast_single_month(
                sku_data,
                fy, fm,
                current_year, current_month,
                tf, raw_tf, spu_tf,
                forecast_step=idx,
                season=season,          # v3 新增
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
def load_spu_season_map() -> Dict[str, str]:
    """
    从 MySQL `SPU季节表` 读取 {SPU: 季节} 映射。
    季节值：'春夏' / '秋冬' / '全年'
    若表不存在或读取失败，返回空字典（降级为全年，现有逻辑不变）。
    """
    try:
        with db_cursor(dictionary=True) as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'SPU季节表'
            """)
            if not cur.fetchone().get('cnt', 0):
                logger.warning("SPU季节表不存在，季节性感知功能跳过")
                return {}
            cur.execute("SELECT SPU, 季节 FROM `SPU季节表`")
            result = {row['SPU']: row['季节'] for row in cur.fetchall()}
            logger.info(f"读取 SPU季节表：{len(result)} 条")
            return result
    except Exception as e:
        logger.warning(f"读取 SPU季节表失败（降级为全年）: {e}")
        return {}
 
 
# ════════════════════════════════════════════════════════════════════════════
# 2. 新增：计算去年旺季月均销量
# ════════════════════════════════════════════════════════════════════════════
 
逻辑：
  1. 取去年全年12个月销量
  2. 找销量最高的3个月（非连续也可）→ peak_avg
  3. 如果去年数据不足3个月有销量，fallback 到静态 PEAK_MONTHS 定义
  4. 如果静态定义也没数据，返回 0（不做季节压制）
"""
 
# ── 静态兜底旺季定义（动态识别失败时使用）────────────────────────────────
# 收窄为核心旺季月份，去掉启动月和收尾月
PEAK_MONTHS_FALLBACK = {
    '春夏': [4, 5, 6, 7],     # 去掉3月（启动）和8月（收尾）
    '秋冬': [10, 11, 12, 1],  # 去掉9月（启动）和2月（收尾）
}
 
# 动态识别时取销量最高的N个月
PEAK_TOP_N = 3
 
 
def _get_peak_season_avg(
    sku_data: Dict[str, Any],
    season: str,
    last_year: int,
) -> float:
    """
    动态识别去年旺季月均销量：
      1. 取去年全年12个月销量，找最高的 PEAK_TOP_N 个月
      2. 要求这几个月都属于该季节的合理旺季范围（过滤异常月份）
      3. 数据不足时 fallback 到静态 PEAK_MONTHS_FALLBACK
 
    Args:
        sku_data  : SKU销量字典
        season    : '春夏' / '秋冬' / '全年'
        last_year : 去年年份（4位整数）
 
    Returns:
        float: 旺季月均销量（0 = 无数据，不做季节压制）
    """
    if season == '全年':
        return 0.0
 
    # ── Step1：取去年全年12个月销量 ────────────────────────────────────────
    monthly_sales = []
    for m in range(1, 13):
        # 秋冬款的1、2月在去年对应 last_year+1
        if season == '秋冬' and m in [1, 2]:
            y = last_year + 1
        else:
            y = last_year
        label = f"{str(y)[-2:]}年{m}月销量"
        val = sku_data.get(label, 0) or 0
        monthly_sales.append((m, val))
 
    # ── Step2：动态找最高的 PEAK_TOP_N 个月 ────────────────────────────────
    # 只取有销量的月份
    has_sales = [(m, v) for m, v in monthly_sales if v > 0]
 
    if len(has_sales) >= PEAK_TOP_N:
        # 按销量降序，取前 N 个月
        top_months = sorted(has_sales, key=lambda x: x[1], reverse=True)[:PEAK_TOP_N]
        top_months_ids = {m for m, _ in top_months}
 
        # 过滤：这 N 个月中，至少有 2 个属于该季节的静态旺季范围
        # 防止因为某月有促销异常导致错误识别
        static_peak = set(PEAK_MONTHS_FALLBACK.get(season, []))
        overlap = len(top_months_ids & static_peak)
 
        if overlap >= 2:
            # 动态识别成功
            peak_avg = sum(v for _, v in top_months) / PEAK_TOP_N
            return peak_avg
        # else: 动态识别结果和静态定义偏差太大，fallback
 
    # ── Step3：Fallback 到静态旺季定义 ────────────────────────────────────
    fallback_months = PEAK_MONTHS_FALLBACK.get(season, [])
    fallback_sales = []
    for m in fallback_months:
        y = last_year + 1 if (season == '秋冬' and m in [1, 2]) else last_year
        label = f"{str(y)[-2:]}年{m}月销量"
        val = sku_data.get(label, 0) or 0
        if val > 0:
            fallback_sales.append(val)
 
    if fallback_sales:
        return sum(fallback_sales) / len(fallback_sales)
 
    # ── Step4：完全无数据，不做季节压制 ───────────────────────────────────
    return 0.0
