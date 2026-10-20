"""Local Markdown/CSV report from verified results; no training or remote writes."""
from __future__ import annotations
import argparse
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd
try:
    from scripts import direct_yearago_experiment as e
except ModuleNotFoundError:
    import direct_yearago_experiment as e

BASE, CAND = "DIRECT30x2", "DIRECT_YEARAGO"


def num(value, decimals=2):
    if pd.isna(value):
        return "—"
    return f"{value:,.{decimals}f}"


def pct(value):
    return "—" if pd.isna(value) else f"{value*100:.3f}%"


def table(frame, columns):
    labels = list(columns.values())
    rows = ["| "+" | ".join(labels)+" |", "| "+" | ".join("---" for _ in labels)+" |"]
    for _, row in frame.iterrows():
        cells = []
        for col in columns:
            x = row[col]
            if pd.isna(x):
                value = "—"
            elif col.endswith("_rate") or col.endswith("_share") or col in ["wape", "bias", "over_rate"]:
                value = pct(x)
            elif isinstance(x, (float, np.floating)):
                value = num(x)
            else:
                value = str(x)
            cells.append(value.replace("|", "/").replace("\n", " "))
        rows.append("| "+" | ".join(cells)+" |")
    return "\n".join(rows)


def report(out, destination, reviewed=None, validation_dir=None):
    out, dest = Path(out).resolve(), Path(destination).resolve()
    if dest.exists():
        raise ValueError("REPORT_EXISTS")
    scored = Path(reviewed).resolve() if reviewed else out/"scoring"
    validated = Path(validation_dir).resolve() if validation_dir else out/"validation"
    validation = e.read_json(validated/"validation.json")
    pre = e.read_json(out/"prevalidation/validation.json")
    assert validation["status"] == pre["status"] == "PASS"
    sm = e.read_json(scored/"score_manifest.json")
    e.verify_hashes({str(scored/k): v for k, v in sm["files"].items()})
    summary = e.read_json(scored/"summary.json")
    metrics = pd.read_parquet(scored/"metrics.parquet")
    joints = pd.read_parquet(scored/"joint_summary.parquet")
    detail = pd.read_parquet(scored/"paired_details.parquet")
    product = pd.read_parquet(scored/"product_changes.parquet")
    changes = pd.read_parquet(scored/"hit_changes.parquet")
    support = pd.read_csv(out/"annual_support.csv")
    coverage = pd.read_parquet(scored/"inference_coverage.parquet")
    p = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("overall")]
    p60 = p[p.window.eq("d01_60")].set_index("model")
    j = joints[joints.scope.eq("ORIGINAL_DIRECT_OUTPUT") & joints.slice.eq("overall")].set_index("model")
    decline = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("decline_group") &
                      metrics.decline_group.eq("PRE_ISSUE_DECLINING") & metrics.window.eq("d01_60")].set_index("model")
    ae_reduction = p60.loc[BASE, "ae"]-p60.loc[CAND, "ae"]
    hit_net = int(j.loc[CAND, "joint_hit_n"]-j.loc[BASE, "joint_hit_n"])
    hit_actual_net = j.loc[CAND, "joint_hit_actual"]-j.loc[BASE, "joint_hit_actual"]
    extra_over = decline.loc[CAND, "over_units"]-decline.loc[BASE, "over_units"]
    labels = {
        "paired_w60_wape_lower": "60天WAPE严格下降",
        "joint_hit_count_increased": "三窗口联合命中数增加",
        "joint_hit_sales_share_not_lower": "联合命中实销占比不下降",
        "declining_w60_over_rate_not_increased": "下降观察组高估率不增加",
        "output_and_core_coverage_not_reduced": "输出及核心覆盖不减少",
    }
    gate_rows = pd.DataFrame([{"gate": labels[k], "result": "PASS" if v is True else "FAIL" if v is False else "PARTIAL"}
                              for k, v in summary["gates"].items()])
    allowed = support[support.fit_allowed]
    skipped = support[~support.fit_allowed]
    comparison_cols = {"model": "模型", "window": "窗口", "fixed_n": "固定对象期数", "scored_n": "可评分",
                       "wape": "WAPE", "bias": "Bias", "over_units": "高估件", "under_units": "低估件",
                       "hit_n": "±20%命中", "hit_actual_share": "命中实销占比"}
    joint_cols = {"model": "模型", "fixed_n": "固定对象期数", "output_complete_n": "完整输出",
                  "joint_hit_n": "联合命中", "joint_hit_fixed_share": "固定范围命中率",
                  "joint_hit_actual": "联合命中承载实销", "joint_hit_actual_share": "承载实销占比"}
    periods = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("issue") &
                      metrics.window.eq("d01_60") & metrics.model.isin([BASE, CAND])]
    period_pivot = periods.pivot(index="issue_date", columns="model", values=["ae", "wape", "over_units", "fixed_n"])
    pt = pd.DataFrame(index=period_pivot.index)
    pt["n"] = period_pivot["fixed_n"][BASE]
    pt["base_wape"] = period_pivot["wape"][BASE].map(pct)
    pt["candidate_wape"] = period_pivot["wape"][CAND].map(pct)
    pt["ae_reduction"] = period_pivot["ae"][BASE]-period_pivot["ae"][CAND]
    pt["extra_over"] = period_pivot["over_units"][CAND]-period_pivot["over_units"][BASE]
    pt = pt.reset_index()
    state_metric = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq("annual_state") &
                           metrics.window.eq("d01_60") & metrics.model.isin([BASE, CAND])]
    state_answers = []
    for state, group in state_metric.groupby("annual_state"):
        pair = group.set_index("model")
        state_answers.append(f"{state}（{int(pair.loc[BASE,'fixed_n'])}条）"
                             f"WAPE {pct(pair.loc[BASE,'wape'])}→{pct(pair.loc[CAND,'wape'])}")
    changed_products = product.groupby("state", as_index=False).agg(products=("spu", "size"), actual=("actual", "sum"))
    share_total = product.actual.sum()
    changed_products["actual_share"] = changed_products.actual/share_total
    gains = product[product.ae_reduction.gt(0)].sort_values("ae_reduction", ascending=False)
    gross_gain = gains.ae_reduction.sum()
    max_share = float(gains.iloc[0].ae_reduction/gross_gain) if len(gains) else np.nan
    top5_share = gains.head(5).ae_reduction.sum()/gross_gain if gross_gain else np.nan
    extra_risk = product.sort_values("extra_over", ascending=False).head(15)
    full_joint = joints[joints.scope.eq("FULL_CORE") & joints.slice.eq("overall") & joints.model.eq(CAND)].iloc[0]
    both = metrics[metrics.scope.eq("BOTH_ANNUAL_OUTPUTS") & metrics.slice.eq("overall")]
    both_j = joints[joints.scope.eq("BOTH_ANNUAL_OUTPUTS") & joints.slice.eq("overall")]
    ledger = e.read_json(out/"formal_fit_ledger.json")
    fit_seconds = sum(x["result"]["fit_seconds"] for x in ledger["slots"])
    old_config = e.read_json(out/"configuration.json")
    pm = e.read_json(out/"prepare_manifest.json")
    git = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=e.ROOT, text=True).strip()
    unknown0 = int(detail.actual_d01_60.eq(0).sum())
    text = f"""# DIRECT_YEARAGO年度参考增量实验实际效果报告

## 首页：六个问题

1. **年度支持**：40个模型单元中实际拟合{summary['formal_fits']}个，跳过{len(skipped)}个。
   2025年1—3月M1/M2与4月M2引用原DIRECT；4月M1开始满足门槛，5月起两段均满足。
   这是训练支持判断，不是看到目标实销后选模型；{summary['baseline_reused_segments']}条有效分段预测引用旧结果，
   {summary['annual_generated_segments']}条分段预测由年度模型生成。两段都由年度模型输出的对象期数为{summary['both_annual_outputs_n']}。
2. **总误差**：同一{summary['original_direct_output_n']}条配对范围，60天绝对误差减少 **{num(ae_reduction)}件**，
   WAPE从 **{pct(p60.loc[BASE,'wape'])}** 到 **{pct(p60.loc[CAND,'wape'])}**。
   “减少”为带符号量，负数表示增加。
3. **联合命中**：从{int(j.loc[BASE,'joint_hit_n'])}到{int(j.loc[CAND,'joint_hit_n'])}，净增 **{hit_net}条**；
   命中承载实销从{num(j.loc[BASE,'joint_hit_actual'],0)}到{num(j.loc[CAND,'joint_hit_actual'],0)}件，
   净增 **{num(hit_actual_net,0)}件**。款数与销量权重同时验收，没有以一项代替另一项。
4. **年度历史差异**：{'；'.join(state_answers)}。
   窗口完整不证明历史经营可比，所有经营可比性仍为UNKNOWN。
5. **下降风险**：原下降观察组60天高估率由 **{pct(decline.loc[BASE,'over_rate'])}** 到
   **{pct(decline.loc[CAND,'over_rate'])}**，高估件数变化 **{num(extra_over)}件**（正值为增加）。
6. **五项筛选**：数值结论 **{summary['numerical_screen']}**；业务证据 **PARTIAL**。
   本轮是反复研究过的重建历史开发验证，不是独立OOS。
   完整核心{summary['full_core_n']}条仅{int(full_joint.joint_hit_n)}条三窗口同时命中
   （{pct(full_joint.joint_hit_fixed_share)}），不能宣布全店核心产品逐款±20%目标达标。

## 1. 支持矩阵与真实产出

门槛为年度两窗口完整且在年度输入开始日及以前观察到正销量，
至少30个不同模拟签发日、10个店铺×SPU，四个年度数值特征不是全部常量。
通过后仍使用原完整训练矩阵，包括年度字段为NaN的行；没有只训练年度支持子集。
“此前正销量”采用用户精确定义的包含开始日边界，0仅表示本归档未观察到。

{table(support, {'issue_date':'签发日','segment':'段','train_n':'原训练行','support_rows':'支持行','support_products':'支持商品','support_dates':'支持日期','fit_allowed':'拟合'})}

训练支持与预测端覆盖分开：`annual_support.csv`保存两侧数量，
`feature_completeness.csv`保存各期字段缺失率、范围和不同值数；
`scoring/inference_coverage.csv`按年度状态、预测来源和分段保留实际销量权重。
实际销量权重只用于事后评价，不参与支持筛查或训练。
年度窗口日历高度重叠，支持日期数不能当成独立季节样本数。

## 2. 完整流程同样本评分

主配对范围固定为原DIRECT完整输出对象；{summary['original_missing_output_n']}条原缺输出留在完整核心范围，
不补0、不补造年度输出。9/10月另有{summary['immature_n']}条NOT_MATURED，不缩短目标窗口评分。
完整成熟核心的明确零实销对象期为{unknown0}条；零实销不产生百分比命中，绝对误差另列。

{table(p, comparison_cols)}

{table(j.reset_index(), joint_cols)}

60天高估件数变化为{num(p60.loc[CAND,'over_units']-p60.loc[BASE,'over_units'])}件，
低估件数变化为{num(p60.loc[CAND,'under_units']-p60.loc[BASE,'under_units'])}件（正值均为增加）。
总误差改善不能掩盖低估风险变化。

以上四模型使用同一对象、窗口与日真值。CONTEXT90仅复用既有结果，
它的新增字段没有进入YEARAGO。全量和所有分组窗口结果见`scoring/metrics.csv`。

## 3. 真正两段均为年度模型的子集

该子集为{summary['both_annual_outputs_n']}条，不是全部20期或全部核心商品。
DIRECT、F28与CONTEXT90在这个相同子集重新评分，不引用全局旧成绩当子集基线。

{table(both, comparison_cols)}

{table(both_j, joint_cols)}

## 4. 五项事前筛选

{table(gate_rows, {'gate':'冻结门槛','result':'未舍入值判定'})}

高估率=同一配对范围Σmax(预测−实销,0)/Σ实销，不用Bias替代。
不改变名单、分母或门槛，不以结果改善为理由增加其他年度对齐方法或权重。

## 5. 年度覆盖状态的收益与风险

{table(state_metric, {'annual_state':'年度状态','model':'模型','fixed_n':'对象期数','wape':'60天WAPE','over_units':'高估件','under_units':'低估件','hit_n':'60天命中'})}

BOTH_COMPLETE_PRIOR_POSITIVE=两窗口完整且已有更早正销量；
BOTH_COMPLETE_NO_PRIOR_POSITIVE=两窗口完整但更早正销量未观察到；
ONE_WINDOW_COMPLETE=仅一窗完整；ANNUAL_WINDOWS_MISSING=年度窗缺失；
UNSAFE_MAPPING_OR_SOURCE=映射/来源无效；MIXED_SEGMENT_STATES=两段状态不同。
未在表中出现的类别在本次样本中没有记录，不能由此证明源系统不存在这种风险。
上述完整流程含支持不足时的原预测引用；不能将其全部效果归于年度模型。

## 6. 原下降观察组及重点商品

观察组原样复用CONTEXT90归档名单，以签发前90天连续下降定义，
并非已证实未来需求下降。配对组对象期数{int(decline.loc[CAND,'fixed_n'])}、
不同商品{int(decline.loc[CAND,'products'])}、签发期{int(decline.loc[CAND,'periods'])}。

{table(decline.reset_index(), {'model':'模型','fixed_n':'对象期数','wape':'WAPE','over_units':'高估件','over_rate':'高估率','under_units':'低估件','hit_n':'60天命中'})}

商品按所有已评分签发期汇总的改善/退化：

{table(changed_products, {'state':'状态','products':'店铺×SPU数','actual':'承载实销','actual_share':'实销权重'})}

新增高估最多的商品（带符号误差变化，不解释为已证明的经营原因）：

{table(extra_risk, {'shop':'店铺','spu':'SPU','periods':'期数','actual':'实销','ae_reduction':'绝对误差减少','extra_over':'额外高估'})}

同一个SPU跨签发期可改善也可退化；上表是商品汇总，逐期明细保存在`paired_details.csv`。
当前品类/季节只用于事后切片，无历史生效证据，不用于模型或分流。
JQ-US连体衣、背心/吊带、T恤、秋冬以及其他店铺的完整切片见下表及CSV。

"""
    focus = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.isin(["shop_category", "shop_season"]) &
                    metrics.window.eq("d01_60") & metrics.model.isin([BASE, CAND])]
    focus = focus[(focus.shop.eq("JQ-US") & focus.current_category.isin(["连体衣", "背心/吊带", "T恤"])) |
                  focus.current_season.fillna("").str.contains("秋|冬", regex=True)]
    text += table(focus, {"shop":"店铺","current_category":"当前品类","current_season":"当前季节","model":"模型","fixed_n":"对象期数","wape":"WAPE","over_units":"高估件","under_units":"低估件"})
    text += f"""

## 7. 分期稳定性与集中程度

{table(pt, {'issue_date':'签发日','n':'配对数','base_wape':'DIRECT WAPE','candidate_wape':'YEARAGO WAPE','ae_reduction':'绝对误差减少','extra_over':'额外高估'})}

20期中绝对误差改善{int(pt.ae_reduction.gt(0).sum())}期、退化{int(pt.ae_reduction.lt(0).sum())}期、
相同{int(pt.ae_reduction.eq(0).sum())}期。相同期包含整期引用旧预测，不能算新模型成功。
商品总改善量中，最大单款占{pct(max_share)}、前5款合计占{pct(top5_share)}；
这里分母是正改善商品的总减少误差，不是净改善量。

{table(pd.read_parquet(scored/'stability.parquet'), {'scope':'描述性范围','n':'对象期数','ae_reduction':'误差减少','extra_over':'额外高估','joint_net':'联合命中净增','joint_actual_net':'命中实销净增'})}

排除2025年11月仅用于检查集中程度，不替换预登记主评价。
按2025/2026、店铺、原货龄的60天表现：

"""
    for sl in ["year", "shop", "maturity"]:
        q = metrics[metrics.scope.eq("ORIGINAL_DIRECT_OUTPUT") & metrics.slice.eq(sl) &
                    metrics.window.eq("d01_60") & metrics.model.isin([BASE, CAND])]
        text += "\n"+table(q, {sl:sl,"model":"模型","fixed_n":"对象期数","wape":"WAPE","bias":"Bias","over_units":"高估件","under_units":"低估件"})+"\n"
    text += f"""

## 8. 命中迁移与未覆盖

完整核心范围（WAPE仅对有输出且真值已知的行计算，不把缺预测补成0；固定范围命中率以全部核心对象期为分母）：

{table(metrics[metrics.scope.eq('FULL_CORE') & metrics.slice.eq('overall')], {'model':'模型','window':'窗口','fixed_n':'核心对象期数','output_n':'有输出','missing_output_n':'缺输出','truth_n':'实销已知','zero_truth_n':'明确零实销','zero_truth_ae':'零实销绝对误差','hit_n':'命中','hit_fixed_share':'固定核心命中率'})}

{table(joints[joints.scope.eq('FULL_CORE') & joints.slice.eq('overall')], joint_cols)}

{table(changes[(changes.scope.eq('ORIGINAL_DIRECT_OUTPUT')) & changes.kind.eq('hit_migration')], {'state':'联合命中迁移','n':'对象期数','products':'不同商品','actual':'承载实销','actual_share':'配对实销占比'})}

RETAINED为保留命中，GAINED为新增命中，LOST为退出命中。联合命中按对象×签发期只计一次，
不把三段承载实销相加。完整核心、配对、真年度子集全部保留；未输出与未成熟可追溯。
本轮只能检验一个年度参考增量，不能证明所有商品的活动、经营阶段和供给变化可预测。

## 9. 技术验收、时间和来源

- 技术验收：PASS。预拟合合成测试10项通过；初次8项试运行也通过（两次测试运行各1次小型合成拟合，均不计业务拟合）。
  另通过CSV浮点精确往返读取回归测试，未增加业务拟合。
- 预拟合独立复核：{pre['train_rows']:,}训练行旧键/标签/旧特征全量对账；
  {pre['calendar_rows']:,}行（全部推断行+每单元64条训练抽样）从原始日历重算年度特征；
  {pre['future_perturbations']}次真实序列截止日后扰动检查。
- 独立效果复核：{validation['raw_truth_windows']:,}真值窗口重新求和，
  {validation['joint_objects']:,}模型×对象期联合命中核对，
  {validation['metric_rows']:,}分组指标复算；
  {validation['fallback_values_exact']}条引用原结果逐值相同，
  {validation['annual_values_exact']}条年度预测由保存模型重算一致。
- 保存模型的实际未来扰动预测检查{validation['actual_future_prediction_checks']}次；
  所有训练行引用范围和标签成熟性已全量检查。未为了扰动验证额外重训业务模型。
- 正式拟合{summary['formal_fits']}次，纯拟合累计{num(fit_seconds)}秒；{len(skipped)}个支持不足单元没有模型或fit_started文件。
- 数据库查询0次、旧模型重训0次、生产变更0项；旧来源/名单/预测/参数哈希重新检查一致。
- 交付前发现旧评分CSV默认解析导致机器精度级差异，会将少量不变对象误分类为改善/退化。
  已改为round-trip解析，并对全部原DIRECT三窗口逐值核对源预测；只重做评分/复核，不重训、不改模型。
  初版评分及报告保留为非最终产物，最终依据为本报告所绑定的reviewed目录。
- 执行时间使用真实带时区时间；输入既有获取时间不能倒填为历史签发前已获取。
- LightGBM {old_config['runtime']['lightgbm']} 原生NaN机制经合成测试；
  [官方缺失值说明](https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html#missing-value-handle)
  确认默认处理NaN，默认不把明确0视为缺失。没有升级依赖或修改生产环境。
- 预登记提交：`{e.PREREG}`。
- 实验源码初始冻结提交：`48b664ce36484c5440c43782443273a2fa5573e4`。
- 报告生成时源码HEAD：`{git}`；最终交付commit另记本地执行记录。
- 生成时间：`{e.now()}`。

核心来源哈希：

"""
    primary_files = [p for p in pm["sources"] if any(p.endswith(v) for v in [
        "inputs\\daily.parquet", "inputs\\monthly_reference.parquet", "prepared\\core.parquet",
        "prepared\\frozen_config.json", "scoring\\scores.parquet", "observation_groups.parquet"])]
    hashes = pd.DataFrame([{"file": p, "sha256": pm["sources"][p]} for p in primary_files])
    text += table(hashes, {"file":"本地只读来源","sha256":"SHA256"})
    text += """

## 10. 交付索引及停止

- `annual_support.csv`：40单元支持矩阵；`feature_completeness.csv`：缺失比例/特征范围。
- `prepared/*/training_windows.parquet`：全部训练年度窗口及状态；`inference_windows.parquet`：推断窗口。
- `effects/*/model.txt`及`checkpoint.json`：实际新模型；支持不足单元没有模型文件。
- `scoring/segment_predictions.csv`：年度原输出、比较流、来源、原因和旧预测哈希。
- `scoring/metrics.csv`、`joint_summary.csv`：全范围/配对/真年度子集的三窗口评分及切片。
- `scoring/paired_details.csv`、`product_changes.csv`、`hit_changes.csv`：逐款变化与销量权重。
- `scoring/missing_outputs.csv`、`not_matured.csv`：未输出与未成熟，未删除或补零。
- `prevalidation/validation.json`、`validation/validation.json`：数值复核范围及结果。
- `configuration.json`、`prepare_manifest.json`、`effects_manifest.json`、`formal_fit_ledger.json`：
  参数、源码/输入/输出哈希、真实拟合账本。

本轮到此停止。数值筛选不等于上线批准；不改V4/A16/NEW_VISIBLE，不改采购、数据库、BI或调度，
不追加第二候选、不合并main、不部署服务器。业务数据、逐款结果和模型仅留本地，GitHub只含脱敏代码与规范。
"""
    dest.mkdir(exist_ok=False)
    report_path = dest/"DIRECT_YEARAGO年度参考增量实验实际效果报告.md"
    if scored != out/"scoring":
        text = text.replace("scoring/", scored.name+"/")
    if validated != out/"validation":
        text = text.replace("`validation/validation.json`", f"`{validated.name}/validation.json`")
    report_path.write_text(text, encoding="utf-8")
    for name, f in [("headline_windows", p), ("headline_joint", j.reset_index()), ("period_comparison", pt),
                    ("five_gates", gate_rows), ("priority_slices", focus), ("top_extra_over", extra_risk),
                    ("product_improvement_counts", changed_products)]:
        f.to_csv(dest/(name+".csv"), index=False, encoding="utf-8-sig")
    e.dump(dest/"report_manifest.json", {
        "at": e.now(), "source_commit": git, "source_script_sha256": e.sha(__file__),
        "score_manifest_sha256": e.sha(scored/"score_manifest.json"),
        "validation_sha256": e.sha(validated/"validation.json"),
        "reviewed_directory": str(scored), "validation_directory": str(validated),
        "files": {p.name: e.sha(p) for p in dest.iterdir() if p.is_file()}})
    print(str(report_path), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True); parser.add_argument("--destination", required=True)
    parser.add_argument("--reviewed"); parser.add_argument("--validation-dir")
    report(**vars(parser.parse_args()))
