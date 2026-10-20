"""Materialize a local numerical report from verified results, no training."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd
try:
    from scripts import direct_context90_experiment as exp
except ModuleNotFoundError:
    import direct_context90_experiment as exp

BASE, NEW = "DIRECT30x2", "DIRECT_CONTEXT90"
KEY = ["issue_date", "shop", "spu"]


def number(value, digits=2):
    return "—" if pd.isna(value) else f"{value:,.{digits}f}"


def table(frame, labels, percent=(), integer=()):
    columns = list(labels)
    lines = ["| " + " | ".join(labels.values()) + " |", "| " + " | ".join(["---"]*len(columns)) + " |"]
    for _, row in frame[columns].iterrows():
        values = []
        for c in columns:
            v = row[c]
            if pd.isna(v):
                value = "—"
            elif c in percent:
                value = f"{v*100:.3f}%"
            elif c in integer:
                value = number(v, 0)
            elif isinstance(v, (int, float, np.integer, np.floating)):
                value = number(v, 2)
            else:
                value = str(v).replace("|", "/")
            values.append(value)
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def run(out, destination):
    root, dest = Path(out).resolve(), Path(destination).resolve()
    if dest.exists():
        raise ValueError("NEW_REPORT_DIRECTORY_REQUIRED")
    validation = exp.read_json(root/"validation/validation.json")
    assert validation["status"] == "PASS"
    sm = exp.read_json(root/"scoring/score_manifest.json")
    assert exp.sha(root/"scoring/score_manifest.json") == validation["score_manifest_sha256"]
    for rel, h in sm["files"].items():
        assert exp.sha(root/"scoring"/rel) == h
    prepared = exp.read_json(root/"prepare_manifest.json")
    exp.verify_hashes(prepared["sources"])
    metrics = pd.read_parquet(root/"scoring/metrics.parquet")
    joint = pd.read_parquet(root/"scoring/joint_summary.parquet")
    detail = pd.read_parquet(root/"scoring/paired_details.parquet")
    objects = pd.read_parquet(root/"scoring/objects.parquet")
    scores = pd.read_parquet(root/"scoring/scores.parquet")
    summary = exp.read_json(root/"scoring/summary.json")
    mature = scores[scores.truth_status.ne("NOT_MATURED")]
    # Independent summary and all five gate arithmetic; no calls to scoring functions.
    for r in joint[joint.slice.eq("overall")].itertuples():
        subset = objects[objects.model.eq(r.model) & objects.truth_complete]
        if r.scope == "ORIGINAL_DIRECT_OUTPUT":
            subset = subset[subset.original_direct_output]
        n = sum(bool(x) for x in subset.joint_hit)
        units = sum(float(a) for a, h in zip(subset.actual_d01_60, subset.joint_hit) if h)
        total = sum(float(a) for a in subset.actual_d01_60)
        assert n == r.joint_hit_n and len(subset) == r.fixed_n
        assert units == r.joint_hit_actual and total == r.known_actual
        assert units/total == r.joint_hit_actual_share
    pair = detail[detail.original_direct_output].copy()
    actual = pair.actual_d01_60
    base = pair.prediction_d01_60_base
    new = pair.prediction_d01_60_candidate
    decline = pair.decline_group.eq("PRE_ISSUE_DECLINING")
    recomputed = {
        "paired_w60_wape_lower": bool((new-actual).abs().sum()/actual.sum() < (base-actual).abs().sum()/actual.sum()),
        "joint_hit_count_increased": bool(pair.joint_hit_candidate.sum() > pair.joint_hit_base.sum()),
        "joint_hit_sales_share_not_lower": bool(actual[pair.joint_hit_candidate].sum()/actual.sum() >= actual[pair.joint_hit_base].sum()/actual.sum()),
        "declining_w60_over_rate_not_increased": bool((new[decline]-actual[decline]).clip(lower=0).sum()/actual[decline].sum() <=
                                                     (base[decline]-actual[decline]).clip(lower=0).sum()/actual[decline].sum()),
        "output_and_core_coverage_not_reduced": bool(detail.output_complete_base.eq(detail.output_complete_candidate).all() and len(detail) == summary["full_core_n"]),
    }
    assert recomputed == summary["gates"]
    original = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("overall")]
    w60 = original[original.window.eq("d01_60")].set_index("model")
    j = joint[joint.scope.eq("ORIGINAL_DIRECT_OUTPUT") & joint.slice.eq("overall")].set_index("model")
    prot = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("decline_group") &
                   metrics.decline_group.eq("PRE_ISSUE_DECLINING")]
    p60 = prot[prot.window.eq("d01_60")].set_index("model")
    columns = ["fixed_n", "wape", "ae", "over_units", "under_units", "actual_units", "hit_n"]
    def compare_slice(slice_name, keys):
        f = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq(slice_name) &
                    metrics.window.eq("d01_60")]
        a, b = f[f.model.eq(BASE)], f[f.model.eq(NEW)]
        z = a[keys+columns].merge(b[keys+columns], on=keys, suffixes=("_base", "_new"), validate="one_to_one")
        z["wape_delta"] = z.wape_new-z.wape_base
        z["ae_reduction"] = z.ae_base-z.ae_new
        z["extra_over"] = z.over_units_new-z.over_units_base
        z["under_reduction"] = z.under_units_base-z.under_units_new
        return z
    periods = compare_slice("issue", ["issue_date"])
    shops = compare_slice("shop", ["shop"])
    categories = compare_slice("shop_category", ["shop", "current_category"])
    maturity = compare_slice("maturity", ["maturity"])
    season = compare_slice("season", ["current_season"])
    # Keep group-level per-period deterioration visible even if aggregate improves.
    protection_periods = []
    for issue, g in pair[decline].groupby("issue_date"):
        for window in ["d01_30", "d31_60", "d01_60"]:
            a = g["actual_"+window]
            eb, en = g["prediction_"+window+"_base"]-a, g["prediction_"+window+"_candidate"]-a
            protection_periods.append({"issue_date": issue, "window": window, "n": len(g),
                "actual_units": float(a.sum()), "over_base": float(eb.clip(lower=0).sum()),
                "over_new": float(en.clip(lower=0).sum()),
                "over_rate_delta": float((en.clip(lower=0).sum()-eb.clip(lower=0).sum())/a.sum())})
    protection_periods = pd.DataFrame(protection_periods)
    prod = pd.read_parquet(root/"scoring/product_changes.parquet")
    migrations = pd.read_parquet(root/"scoring/hit_and_error_changes.parquet")
    broad = {
        "periods_improved": int(periods.ae_reduction.gt(0).sum()),
        "periods_degraded": int(periods.ae_reduction.lt(0).sum()),
        "records_improved": int(pair.ae_reduction_d01_60.gt(0).sum()),
        "records_degraded": int(pair.ae_reduction_d01_60.lt(0).sum()),
        "unique_products_improved": int(prod.status.eq("IMPROVED").sum()),
        "unique_products_degraded": int(prod.status.eq("DEGRADED").sum()),
        "gross_ae_reduction": float(pair.ae_reduction_d01_60.clip(lower=0).sum()),
        "gross_ae_increase": float((-pair.ae_reduction_d01_60).clip(lower=0).sum()),
        "top5_benefit_share": float(pair.ae_reduction_d01_60.nlargest(5).sum()/pair.ae_reduction_d01_60.clip(lower=0).sum()),
        "top5_harm_share": float((-pair.ae_reduction_d01_60).nlargest(5).sum()/(-pair.ae_reduction_d01_60).clip(lower=0).sum()),
        "decline_periods_over_rate_increased": int(protection_periods.query("window=='d01_60'").over_rate_delta.gt(0).sum()),
    }
    observation = pd.read_parquet(root/"observation_groups.parquet")
    ledger = exp.read_json(root/"formal_fit_ledger.json")
    manifest = exp.read_json(root/"effects_manifest.json")
    train = pd.read_csv(root/"training_reconciliation.csv")
    exec_nb = exp.read_json(root/"notebook_verified/execution.json")
    git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=exp.ROOT, text=True).strip()
    delta_ae = w60.loc[NEW, "ae"]-w60.loc[BASE, "ae"]
    hit_delta = int(j.loc[NEW, "joint_hit_n"]-j.loc[BASE, "joint_hit_n"])
    sales_delta = j.loc[NEW, "joint_hit_actual"]-j.loc[BASE, "joint_hit_actual"]
    full_j = joint[joint.scope.eq("FULL_CORE") & joint.slice.eq("overall")]
    full_m = metrics[metrics.scope.eq("FULL_CORE") & metrics.slice.eq("overall")]
    unknown_n = int(observation.decline_group.eq("UNKNOWN").sum())
    gate_labels = {
        "paired_w60_wape_lower": "配对60天WAPE下降",
        "joint_hit_count_increased": "三窗口联合命中数增加",
        "joint_hit_sales_share_not_lower": "联合命中销量占比不下降",
        "declining_w60_over_rate_not_increased": "下降观察组60天高估率不增加",
        "output_and_core_coverage_not_reduced": "输出与核心覆盖不减少",
    }
    gate_frame = pd.DataFrame([{"item": gate_labels[k], "result": "PASS" if v else "FAIL"} for k, v in recomputed.items()])
    focus = categories[categories.shop.eq("JQ-US") & categories.current_category.isin(["连体衣", "背心/吊带", "T恤"])]
    topcols = {"issue_date":"签发日", "shop":"店铺", "spu":"SPU", "actual_d01_60":"实际60天",
               "prediction_d01_60_base":"DIRECT", "prediction_d01_60_candidate":"CONTEXT90",
               "ae_reduction_d01_60":"绝对误差减少", "extra_over_d01_60":"额外高估"}
    gap = int(summary["full_core_n"]-j.loc[NEW, "joint_hit_n"])
    missing_rows = detail[~detail.output_complete_base]
    missing_actual = missing_rows.actual_d01_60.sum()
    full_candidate = full_j[full_j.model.eq(NEW)].iloc[0]
    improved_shops = "、".join(shops.loc[shops.ae_reduction.gt(0), "shop"])
    degraded_shops = "、".join(shops.loc[shops.ae_reduction.lt(0), "shop"])
    full_decline = int(observation.decline_group.eq("PRE_ISSUE_DECLINING").sum())
    pair_decline = int(p60.loc[NEW, "fixed_n"])
    pass_count = sum(v is True for v in recomputed.values())
    screen = summary["numerical_screen"]
    headline_error = "没有减少总量误差" if delta_ae >= 0 else "总量误差减少"
    error_direction = "增加" if delta_ae >= 0 else "减少"
    not_release = "不放宽、不追加候选" if screen != "PASS" else "通过中间筛选不等于生产授权"
    window_notes = []
    for w in ["d01_30", "d31_60"]:
        g = original[original.window.eq(w)].set_index("model")
        window_notes.append(f"{w} WAPE变化{(g.loc[NEW,'wape']-g.loc[BASE,'wape'])*100:+.3f}个百分点，"
                            f"命中数变化{int(g.loc[NEW,'hit_n']-g.loc[BASE,'hit_n']):+d}条")
    report = f"""# DIRECT_CONTEXT90中期背景增量实验实际效果报告

## 首页结论

1. **{headline_error}。** 在相同 {len(pair):,} 条配对记录上，60天绝对误差{error_direction} **{number(abs(delta_ae))} 件**；
   WAPE从 **{w60.loc[BASE,'wape']:.3%}** 升至 **{w60.loc[NEW,'wape']:.3%}**，增加
   **{(w60.loc[NEW,'wape']-w60.loc[BASE,'wape'])*100:.3f} 个百分点**。
2. **联合命中净增 {hit_delta} 条，实销承载增加 {number(sales_delta,0)} 件。**
   {int(j.loc[BASE,'joint_hit_n'])}→{int(j.loc[NEW,'joint_hit_n'])} 条，
   {number(j.loc[BASE,'joint_hit_actual'],0)}→{number(j.loc[NEW,'joint_hit_actual'],0)} 件；
   同一配对实销分母 {number(j.loc[NEW,'known_actual'],0)} 件，占比
   {j.loc[BASE,'joint_hit_actual_share']:.3%}→{j.loc[NEW,'joint_hit_actual_share']:.3%}。
3. **下降观察组整体没有增加高估，但有分期退化。** 配对组 {int(p60.loc[NEW,'fixed_n'])} 条、
   {int(p60.loc[NEW,'products'])} 个店铺×SPU、20期；60天高估率
   {p60.loc[BASE,'over_rate']:.3%}→{p60.loc[NEW,'over_rate']:.3%}，
   高估减少 {number(p60.loc[BASE,'over_units']-p60.loc[NEW,'over_units'])} 件；
   20期中 {broad['decline_periods_over_rate_increased']} 期组内高估率仍增加。
4. **改善与退化均分布在多期、多款。** {broad['periods_improved']}/20期改善，
   {broad['periods_degraded']}/20期退化；配对记录 {broad['records_improved']:,} 条改善、
   {broad['records_degraded']:,} 条退化。按店铺×SPU跨期汇总，{broad['unique_products_improved']} 款改善、
   {broad['unique_products_degraded']} 款退化。{degraded_shops}总体退化，{improved_shops}改善。
5. **技术验收PASS；本轮研究筛选{screen}；业务证据仍为重建历史验证。**
   五项门槛通过{pass_count}项，逐项结果见下表，{not_release}。
   完整{summary['full_core_n']:,}条核心记录中联合命中{int(full_candidate.joint_hit_n)}条
   （{full_candidate.joint_hit_fixed_share:.2%}），还有 {gap:,} 条未满足三窗口同时±20%；
   其中{len(missing_rows)}条原缺输出继续保留。距离“全体核心款逐款达标”仍很远，不能上线替代。

## 1. 范围、版本及时间

- 唯一候选：DIRECT_CONTEXT90。旧DIRECT30x2与F28预测复用原归档，没有重训。
- 20个成熟签发期：2025-01至2026-08，每月2日签发；2026-09/10共
  {summary['immature_core_n']} 条核心记录保持NOT_MATURED，不缩短目标期。
- S为签发日，C=S−2；M1=S+1..S+30，M2=S+31..S+60；60天是两段预测相加。
  两段独立拟合，M2不读取M1预测或未来实销。
- 评价单位为“店铺×SPU×签发期”，不是2,820个不同商品。连续签发的60天窗口有重叠，
  跨期合计实销是评分权重，不是去重后的经营销售总量。
- 完整核心名单与选款资格不变；“选款前近90天累计至少90%”不等于未来销量必然覆盖90%。
- 本轮是已反复研究的重建历史开发验证，不是独立OOS。来源可获取时间、历史点时映射
  仍有既有局限；当前品类/季节只作描述性标签，不用于训练或预测路由。
- 基线提交：8908e71bc45bc0891b84fca5c4103058fc5ad984；
  回放参考：34bdc3b703c45326f3c638814ad18595c8124e83。
- 事前协议：009144ff4eea1f0d14ed20b5c92704d77eefe0be；
  特征与缺失机制冻结：6e30994c166c3bdc58bad89578ff1222c0a4dd73；
  效果计算前评分/复核代码：202b4c25d87f3f5ac1804e4cb6438ce942794653。
- 本报告工具代码提交：{git_commit}。正式拟合起止：
  {manifest['started_at']} 至 {manifest['completed_at']}（UTC，原始回执带时区）。

## 2. 五项事前筛选

{table(gate_frame, {'item':'门槛','result':'结果'})}

所有判断使用未四舍五入值。保护组主门槛事前明确为60天高估率，
两个30天窗口也全部披露。高估率=ΣMAX(预测−实际,0)/同组实际合计，不是Bias。
下降组是签发前连续下降代理，不等于未来真实衰退；其广度达到事前描述性屏幕
（≥30记录、≥10商品、≥3期），但不是统计独立性或因果保障。

## 3. 三窗口严格同样本对照

以下每行均为原DIRECT有输出的{len(pair):,}条对象×签发期；没有用多模型交集缩小核心名单。
WAPE=Σ绝对误差/Σ实际；Bias=Σ(预测−实际)/Σ实际。
±20%命中只对正实销判断；真实零销量单列绝对误差，不制造百分比。
60天绝对误差不能等同于两个30天绝对误差之和，因为两段误差可能抵消。

{table(original, {'model':'模型','window':'目标窗口','wape':'WAPE','bias':'Bias',
                  'ae':'绝对误差件数','over_units':'高估件数','under_units':'低估件数',
                  'hit_n':'±20%命中数','hit_fixed_share':'固定范围命中占比'},
       percent=('wape','bias','hit_fixed_share'),integer=('hit_n',))}

60天高估净减少 {number(w60.loc[BASE,'over_units']-w60.loc[NEW,'over_units'])} 件，
但低估净增加 {number(w60.loc[NEW,'under_units']-w60.loc[BASE,'under_units'])} 件，
两者相抵后的绝对误差变化为 {number(delta_ae)} 件。
{'；'.join(window_notes)}。不能只用其中一个窗口概括整体结果。

### 联合命中必须同时看数量与销量

{table(j.reset_index(), {'model':'模型','fixed_n':'配对记录','joint_hit_n':'联合命中',
       'joint_hit_actual':'命中实销60天','joint_hit_actual_share':'同分母销量占比'},
       percent=('joint_hit_actual_share',),integer=('fixed_n','joint_hit_n','joint_hit_actual'))}

{table(migrations[(migrations.scope=='ORIGINAL_DIRECT_OUTPUT') & migrations.kind.eq('hit_migration')],
       {'state':'命中迁移','n':'记录数','actual_units':'承载实销量','actual_share':'配对实销占比'},
       percent=('actual_share',),integer=('n','actual_units'))}

GAINED为新增命中、LOST为退出命中、RETAINED为保留命中；联合命中每个对象签发期只计一次。
F28的联合命中数和承载实销量均须与新候选同时比较，不能只按款数宣布全面胜出。

## 4. 完整核心范围、覆盖和缺失

{table(full_j, {'model':'模型','fixed_n':'完整核心记录','output_complete_n':'完整输出',
       'joint_hit_n':'联合命中','joint_hit_fixed_share':'核心联合命中占比',
       'joint_hit_actual':'命中实销','joint_hit_actual_share':'核心实销承载占比'},
       percent=('joint_hit_fixed_share','joint_hit_actual_share'),
       integer=('fixed_n','output_complete_n','joint_hit_n','joint_hit_actual'))}

{table(full_m[full_m.window.eq('d01_60')], {'model':'模型','output_n':'输出数','truth_n':'真值数',
       'missing_output_n':'缺输出','unknown_truth_n':'未知实销','zero_truth_n':'明确零实销',
       'zero_truth_ae':'零实销绝对误差','actual_units':'可评分实销','known_fixed_actual_units':'完整核心已知实销'},
       integer=('output_n','truth_n','missing_output_n','unknown_truth_n','zero_truth_n','actual_units','known_fixed_actual_units'))}

新旧DIRECT输出对象完全一致，没有新增丢失、没有资格放宽、没有删掉原{len(missing_rows)}条缺输出。
这些对象对应60天实际销量{number(missing_actual,0)}件，不能把它们补0或当作已达标。
完整范围的DIRECT误差指标仍只能在有预测对象上计算，不能当作覆盖全部核心款的误差。
F28完整范围与配对范围不同，表间不得混用分母。所有状态见missing_output及not_matured明细。

## 5. 签发前下降观察组

定义：90天完整，早34天、居中28天、最近28天均值均正且严格递减。
完整核心中下降组 {full_decline} 条；
原DIRECT配对{pair_decline}条，另{full_decline-pair_decline}条原资格未满足保留缺输出。
本批90天不完整UNKNOWN为 {unknown_n} 条，但不能推广为所有未来批次都无缺日。

{table(prot, {'model':'模型','window':'窗口','fixed_n':'配对组记录','wape':'WAPE',
       'over_rate':'高估率','over_units':'高估件数','under_units':'低估件数',
       'hit_n':'±20%命中数'},percent=('wape','over_rate'),integer=('fixed_n','hit_n'))}

该组60天WAPE {p60.loc[BASE,'wape']:.3%}→{p60.loc[NEW,'wape']:.3%}，高估减少但
低估增加 {number(p60.loc[NEW,'under_units']-p60.loc[BASE,'under_units'])} 件；
不能把“没有重新推高总体预测”描述为完全无退化。逐期变化见decline_period_changes.csv。

## 6. 改善的广度、店铺与重点分组

{table(shops, {'shop':'店铺','fixed_n_base':'配对记录','wape_base':'旧WAPE','wape_new':'新WAPE',
       'ae_reduction':'绝对误差减少','extra_over':'额外高估','under_reduction':'低估减少'},
       percent=('wape_base','wape_new'),integer=('fixed_n_base',))}

逐记录改善累计 {number(broad['gross_ae_reduction'])} 件绝对误差，但退化累计增加
{number(broad['gross_ae_increase'])} 件。最大5条改善占全部改善
{broad['top5_benefit_share']:.2%}，最大5条退化占全部退化 {broad['top5_harm_share']:.2%}。
改善不是只来自4款，但也没有形成跨期、跨店一致收益。

### 20个签发期

{table(periods, {'issue_date':'签发日','fixed_n_base':'配对数','wape_base':'旧60天WAPE',
       'wape_new':'新60天WAPE','ae_reduction':'绝对误差减少'},
       percent=('wape_base','wape_new'),integer=('fixed_n_base',))}

### JQ-US预定重点组

{table(focus, {'current_category':'当前描述性品类','fixed_n_base':'配对数',
       'wape_base':'旧WAPE','wape_new':'新WAPE','ae_reduction':'绝对误差减少',
       'extra_over':'额外高估'},percent=('wape_base','wape_new'),integer=('fixed_n_base',))}

### 原货龄及当前描述性季节

{table(maturity, {'maturity':'原货龄组','fixed_n_base':'配对数','wape_base':'旧WAPE',
       'wape_new':'新WAPE','ae_reduction':'绝对误差减少'},
       percent=('wape_base','wape_new'),integer=('fixed_n_base',))}

{table(season, {'current_season':'当前季节标签','fixed_n_base':'配对数','wape_base':'旧WAPE',
       'wape_new':'新WAPE','ae_reduction':'绝对误差减少','extra_over':'额外高估'},
       percent=('wape_base','wape_new'),integer=('fixed_n_base',))}

这些是同一已冻结模型的事后描述，不用于按品类路由、不作为进一步改参依据。
历史品类生效日未知；不能据此证明改进或退化由“季节机制”引起。

### 最大改善10条与最大退化10条

正的“绝对误差减少”表示改善；负值表示退化。数量显示两位小数仅为阅读，
评分与门槛使用未舍入预测。

{table(pair.nlargest(10,'ae_reduction_d01_60'), topcols, integer=('actual_d01_60',))}

{table(pair.nsmallest(10,'ae_reduction_d01_60'), topcols, integer=('actual_d01_60',))}

## 7. 技术校验与计算预算

- 正式拟合40/40，20期×M1/M2各一次；旧模型重训0、参数追加0、数据库查询0。
  模型拟合内部耗时合计 {sum(s['result']['fit_seconds'] for s in ledger['slots']):.3f} 秒；
  整体墙钟时间包含进程启动、文件校验与预测，不与纯拟合耗时混称。
- 所有40训练单元对账PASS，累计 {int(train.train_n.sum()):,} 次训练行参与
  （跨单元重复，不是独立样本数）；键、标签、全部旧特征和隐式等权未变化。
- 新特征仅m56/m90/contrast56/contrast90及两个完整性标记；原特征逐字段保留。
  所有实际训练行的56/90天完整，新增缺失行删除0；新窗口缺失机制用合成数据单独验证。
- LightGBM4.6.0原生NaN处理保留，未将0视为缺失；没有任意epsilon，
  没有对原rolling字段重新定义。[官方缺失值说明](https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html#missing-value-handle)。
- 9项特征/缺失/截止合成测试、2项评分测试通过。
- 独立复核：40个保存模型的预测逐值一致；旧训练字段全量哈希一致；
  {validation['calendar_window_checks']:,} 个日期窗口直接切片核验，
  {validation['independent_daily_truth_windows']:,} 个成熟日真值窗口重新逐日求和，
  {validation['future_perturbation_prediction_checks']} 个实际数据未来扰动预测检查通过。
- 新特征训练窗口复核每单元抽64行，推断窗口全量核验；旧字段/标签则全量校验。
  实际未来扰动选首末签发期各8对象×两段，未声称对每个训练行都做扰动测试。
- 已从逐条对象独立复核全部总体联合命中汇总与五项门槛，分母一致。
- 原输入、原预测、名单、映射及源码哈希未变化，原档案未覆盖。

## 8. 交付、异常及停止

- 本报告与本地scoring目录保留完整逐款预测、窗口评分、联合命中迁移、排除及未成熟状态。
  prepared保留40份训练对账、特征矩阵；effects保留40模型与检查点。
  observation_groups保留完整性标记和下降组依据。业务数据、模型与结果不上传GitHub。
- 已执行复核笔记：../notebook_verified/DIRECT_CONTEXT90数值复核.ipynb，
  {exec_nb['code_cells']}个代码单元从头执行，错误输出0；没有再次拟合或查询。
- 首次笔记启动受用户目录访问限制而失败；改为仅在新笔记目录保存IPython运行配置后成功。
  初始失败目录保留，没有覆盖原归档。另有既有pandas FutureWarning，不影响逐值对账；
  未为消除警告而修改冻结源码。
- 当前工具未提供可用的工作区报告运行时发现和浏览器检查入口；本轮交付可核对的数值
  Markdown及已执行笔记，未宣称交互网页或浏览器视觉验收完成。
- **结论：本轮固定候选研究筛选为{screen}，不将单次结果推广为中期背景信息的普遍有效或无效。**
  不新增窗口、权重、种子、SPU特例或第二候选；不切换生产模型。
  生产V4、冻结A16、NEW_VISIBLE、采购、数据库、BI及调度均未改变。到此停止。
"""
    dest.mkdir(exist_ok=False)
    title = "DIRECT_CONTEXT90中期背景增量实验实际效果报告.md"
    (dest/title).write_text(report, encoding="utf-8")
    for name, frame in [("period_changes", periods), ("shop_changes", shops), ("category_changes", categories),
                        ("maturity_changes", maturity), ("season_changes", season), ("decline_period_changes", protection_periods),
                        ("top_improvements", pair.nlargest(30, "ae_reduction_d01_60")),
                        ("top_degradations", pair.nsmallest(30, "ae_reduction_d01_60")),
                        ("extra_over_risks", pair.nlargest(30, "extra_over_d01_60"))]:
        frame.to_csv(dest/(name+".csv"), index=False, encoding="utf-8-sig")
    exp.dump(dest/"supplemental_validation.json", {
        "status": "PASS", "checked_at": exp.now(), "independent_joint_summaries": True,
        "independent_five_gates": recomputed, "breadth": broad, "model_refits": 0,
        "report_commit": git_commit, "source_sha256": exp.sha(__file__),
        "score_manifest_sha256": exp.sha(root/"scoring/score_manifest.json")})
    exp.verify_hashes(prepared["sources"])
    exp.dump(dest/"report_manifest.json", {
        "created_at": exp.now(), "git_commit": git_commit, "source_sha256": exp.sha(__file__),
        "input_hash": exp.sha(Path(exp.read_json(root/"configuration.json")["archive"])/"inputs/daily.parquet"),
        "effect_manifest_sha256": exp.sha(root/"effects_manifest.json"),
        "validation_sha256": exp.sha(root/"validation/validation.json"),
        "files": {p.name: exp.sha(p) for p in dest.iterdir() if p.is_file()}})
    print(json.dumps({"status": "REPORT_COMPLETE", "numerical_screen": summary["numerical_screen"],
                      "ae_increase": delta_ae, "joint_hit_change": hit_delta, "joint_sales_change": sales_delta,
                      "report": str(dest/title)}, ensure_ascii=False))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True); p.add_argument("--destination", required=True)
    run(**vars(p.parse_args()))
