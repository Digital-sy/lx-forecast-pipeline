"""Render the single local diagnostic report from verified summaries."""
import argparse, json
from pathlib import Path
import pandas as pd

def pct(x):return f'{100*x:.2f}%'
def n(x):return f'{x:,.0f}'
def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |',*['| '+' | '.join(map(str,r))+' |' for r in rows]])

def run(root,out):
    root,out=Path(root),Path(out);report=out/'DIRECT训练样本与业务目标对齐诊断.md'
    if report.exists():raise ValueError('REFUSE_REPORT_OVERWRITE')
    u=pd.read_csv(out/'training_units.csv');g=pd.read_csv(out/'training_groups.csv');m=pd.read_csv(out/'model_metrics.csv');j=pd.read_csv(out/'model_joint.csv')
    tr=pd.read_csv(out/'unseen_418_segment_causes.csv');v=json.loads((out/'independent_validation.json').read_text(encoding='utf-8'))
    assert v['status']=='PASS'
    exposures=u.rows.sum();groups=g[g.dimension.eq('core_state')].groupby('value')[['rows','label_units','zero_rows']].sum()
    ages=g[g.dimension.eq('historical_maturity')].groupby('value')[['rows','label_units','zero_rows']].sum()
    shops=g[g.dimension.eq('shop')].groupby('value')[['rows','label_units','zero_rows']].sum()
    c=groups.loc['CORE'];non=groups.loc['NON_CORE'];zero=u.zero_rows.sum();p=m[(m.scope=='PAIRED')&(m.window=='d01_60')].set_index('model')
    joint=j[j.scope=='PAIRED'].set_index('model');last=u[u.issue_date.eq(u.issue_date.max())];first=u[u.issue_date.eq(u.issue_date.min())]
    one=tr[tr.segment=='M1'];counts=one.positive_completed_months.value_counts()
    frozen=pd.read_parquet(root/'core_model_complementarity_20261028_v1/objects.parquet');paired=frozen[frozen.paired]
    evshop=paired.groupby('shop').agg(rows=('spu','size'),actual=('actual_d01_60','sum'))
    evage=paired.groupby('maturity').agg(rows=('spu','size'),actual=('actual_d01_60','sum'))
    exposure_rows=[]
    for value in ['CORE','NON_CORE','UNKNOWN']:
        a=groups.loc[value] if value in groups.index else pd.Series({'rows':0,'label_units':0,'zero_rows':0})
        exposure_rows.append([value,n(a.rows),pct(a.rows/exposures),pct(a.label_units/groups.label_units.sum()),
            pct(a.zero_rows/a.rows) if a.rows else '—'])
    gsum=groups.reset_index();gsum['row_share']=gsum.rows/exposures;gsum['label_share']=gsum.label_units/gsum.label_units.sum()
    gsum['weight_share']=gsum.row_share;gsum.to_csv(out/'training_exposure_summary.csv',index=False,encoding='utf-8-sig')
    models=['DIRECT','BLEND50','F28','CONTEXT90','YEARAGO','GATE','DIRECT_TRAFFIC_STATE_V1','DIRECT_ITEM_ID_V1']
    names={'DIRECT_TRAFFIC_STATE_V1':'TRAFFIC_STATE','DIRECT_ITEM_ID_V1':'ITEM_ID'}
    rows=[]
    for model in models:
        a=p.loc[model];b=joint.loc[model]
        rows.append([names.get(model,model),f'{a.poisson_deviance_mean:.2f}',pct(a.wape),pct(a.bias),n(a.over_units),n(a.under_units),int(b.joint_n),n(b.joint_actual_units),pct(b.joint_sales_share)])
    seen=pd.read_csv(out/'seen_both_metrics.csv').query("window=='d01_60'").set_index('model')
    sj=pd.read_csv(out/'seen_both_joint.csv').set_index('model')
    trace_examples=[]
    for months in sorted(counts.index):
        example=one[one.positive_completed_months.eq(months)].iloc[0]
        both=tr[(tr.issue_date==example.issue_date)&(tr.shop==example.shop)&(tr.spu==example.spu)].set_index('segment')
        trace_examples.append([f'{example.shop}/{example.spu}/{example.issue_date}',int(months),example.actual_cutoff,
            example.first_eligible_simulated_issue,both.loc['M1','earliest_eligible_label_end'],both.loc['M2','earliest_eligible_label_end']])
    full=j[j.scope=='FULL'].set_index('model')
    text=f'''# DIRECT训练样本与业务目标对齐诊断

1. **存在训练行结构偏离，但没有“核心销量未被代表”的证据。** 40个单元的重复训练暴露中，历史核心占{pct(c.rows/exposures)}行、承载{pct(c.label_units/groups.label_units.sum())}标签销量；各单元核心标签销量占比均在{pct(g[(g.dimension=='core_state')&(g.value=='CORE')].label_share.min())}—{pct(g[(g.dimension=='core_state')&(g.value=='CORE')].label_share.max())}。行数、标签销量均不能代替损失/梯度影响。
2. **最明显的结构是长期累积的非核心与零标签窗口。** 零标签暴露占{pct(zero/exposures)}；最后一期M1/M2已达{pct(last.iloc[0].zero_share)}/{pct(last.iloc[1].zero_share)}。非核心中{pct(non.zero_rows/non.rows)}为零标签。值得受控检验其权重，不支持直接删除零销量或认定它们造成误差。
3. **{len(one)}条未见记录都有可用销量历史，主要是资格与标签成熟时间差。** {int(counts.get(4,0))}条仅在本期首次具备4个正销量月；其余{int(counts.get(5,0))}条虽已产生合格模拟截点，30/60天标签仍未在本期截止前结束。不是“没有该款历史”。
4. **下一项只优先建议：按每条训练样本自身截点的历史核心身份，做一次预先固定的相对样本权重对照。** 保留全部训练行，非核心权重不得为0；Poisson、特征、资格、训练窗均不变。具体比例应在下一协议中事前固定，不搜索。它可能损害真实衰退、退出核心和低销量商品，因此必须保留原842条下降观察组及非核心/零销量诊断。现有证据支持“值得试”，不支持直接更改训练方式；本轮未执行。

## 范围与状态

技术统计与独立复核PASS；因果归因与下一方案效果仍未验证。ITEM_ID研究筛选失败结案，TRAFFIC_STATE原结论不变。
2025-01—2026-08共20期；完整核心2,820条对象×签发期记录，配对2,579条，241条缺输出保留；配对下降组842条。不是2,820个不同商品。
本轮训练、推理、数据库/API查询、生产变更均为0。首次影子批次只核对112个文件哈希，没有读入其未来实销或重新预测。

## 真实训练结构

读取ITEM_ID保存且与原DIRECT逐列核验一致的40份训练矩阵，去掉新增item_key后作描述统计；不是重新生成训练或拟合。40份原DIRECT模型文本均为objective/metric=poisson、poisson_max_delta_step=0.7。原fit调用未提供sample_weight，故隐式等权；没有另外保存或假设目标销量权重。

下表为**重复训练暴露汇总**。{n(exposures)}行包含同一历史样本在不同单元反复使用，标签件数还包含相邻30天窗口重叠，不能视为独立样本或去重业务销量。权重占比等于行占比。

{table(['自身历史核心状态','暴露行数','行/有效权重占比','标签销量占比','组内零标签占比'],exposure_rows)}

历史核心先用每店截止前90天全店分母和边界同销量全纳入规则确定，再核查训练老品资格。训练行UNKNOWN为0；2,820条归档核心重建一致，另对240个抽核键用独立90天切片求和复核。这里只说明现存归档可计算，不证明那些数据在历史当时已获取。没有用2026年名单回标早期样本。

{table(['单元','行数','店铺×SPU数','模拟签发日数','零标签占比','核心行占比','核心标签销量占比','每款最大互不重叠窗数之和'],[[r.issue_date+'/'+r.segment,n(r.rows),n(r.objects),n(r.simulated_dates),pct(r.zero_share),pct(r.core_share),pct(r.core_label_share),n(r.sum_item_disjoint_windows)] for r in pd.concat([first,last]).itertuples()])}

完整40单元及每款训练行数的最小值/P25/中位数/P75/P90/最大值在training_units.csv。最后一期M1每款中位数{n(last.iloc[0].median_rows_per_item)}行、最大{n(last.iloc[0].max_rows_per_item)}行。按商品分别取贪心最大不重叠30天子集后，各单元仅保留原行数的{pct((u.sum_item_disjoint_windows/u.rows).min())}—{pct((u.sum_item_disjoint_windows/u.rows).max())}；其余是相对该子集的重叠暴露，不能称独立季节样本，互不重叠也不自动保证统计独立。

原资格以“曾有≥4个正销量完整月”累计判断，28天历史与标签日期完整即可入训，没有因近期持续零销量而退出的规则。这解释了零窗口为何持续积累，但零值本身是有效训练事实，不是应删除的坏记录。

{table(['历史训练货龄','训练行占比','训练标签销量占比','组内零标签比例','配对评价记录占比'],[[k,pct(a.rows/exposures),pct(a.label_units/groups.label_units.sum()),pct(a.zero_rows/a.rows),pct(evage.loc[k,'rows']/len(paired))] for k,a in ages.iterrows()])}

{table(['店铺','训练行占比','训练标签销量占比','配对评价记录占比','配对目标实销占比'],[[k,pct(a.rows/exposures),pct(a.label_units/groups.label_units.sum()),pct(evshop.loc[k,'rows']/len(paired)),pct(evshop.loc[k,'actual']/paired.actual_d01_60.sum())] for k,a in shops.iterrows()])}

JQ-US行数份额较高，但训练标签销量与目标实销份额接近，不能凭行数认定店铺支配损失。训练货龄取每条样本自身截止、评价货龄取签发截止，两者不同时点；这些比较是构成描述，不是已证明的偏差原因。

## Poisson与业务目标的可观察取舍

标签为各自30天累计销量原尺度，没有标准化；M1/M2分别训练，累计60天相加，原后处理仅下限截到0，没有为±20%命中做裁剪。Poisson按对数均值优化计数似然；DIRECT代码没有优化逐款±20%或三窗口联合命中。本轮未执行模型，因此也没有计算实际训练梯度贡献。[LightGBM 4.6.0参数](https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html)、[对应目标实现](https://github.com/microsoft/LightGBM/blob/v4.6.0/src/objective/regression_objective.hpp#L375)。

开发筛选仍为WAPE、联合命中数、命中实销份额、下降组高估率与输出覆盖五项；最终业务目标仍是覆盖至少90%全店销量的核心商品，两个分段和累计逐款误差±20%。本诊断没有替换任何门槛。

下表全部使用相同2,579条、60天实销分母{n(paired.actual_d01_60.sum())}件。Poisson偏差是2×[y·log(y/预测)−y+预测]的均值，越小越好；并非WAPE或准确率。

{table(['模型','平均Poisson偏差','60天WAPE','Bias','高估件数','低估件数','三窗联合命中','命中实销承载','承载占比'],rows)}

F28有4条预测及实际均为0，本表按数学极限偏差0保留，未加epsilon；sklearn严格要求预测>0，因此独立库核对在其合法域逐模型完成，合法域计数见CSV，绝未据此删除总体评分对象。[scikit-learn 1.6.1定义](https://scikit-learn.org/1.6/modules/generated/sklearn.metrics.mean_poisson_deviance.html)。

可观察的取舍：GATE的Poisson偏差低于BLEND50，但BLEND50的WAPE与命中实销承载更好；F28的联合命中更少，承载的实销反而更高。TRAFFIC_STATE虽然命中数略高于DIRECT，Poisson偏差与WAPE却更差。**ITEM_ID的Poisson偏差、WAPE、联合命中、命中实销承载同时退化**，不能把失败全部归因于评分口径。

{table(['模型','前30天WAPE/命中数','后30天WAPE/命中数','累计60天WAPE/命中数'],[[names.get(model,model),*[f"{pct(m[(m.scope=='PAIRED')&(m.model==model)&(m.window==w)].iloc[0].wape)} / {int(m[(m.scope=='PAIRED')&(m.model==model)&(m.window==w)].iloc[0].hit_n)}" for w in ['d01_30','d31_60','d01_60']]] for model in ['DIRECT','BLEND50','DIRECT_TRAFFIC_STATE_V1','DIRECT_ITEM_ID_V1']])}

同对象固定实销下，WAPE与绝对误差排序相同；Poisson与WAPE不同，不证明换损失必然改善，减少绝对误差也不保证更多商品进入±20%。完整2,820条中DIRECT联合命中{int(full.loc['DIRECT','joint_n'])}（{pct(full.loc['DIRECT','joint_fixed_share'])}），BLEND50 {int(full.loc['BLEND50','joint_n'])}（{pct(full.loc['BLEND50','joint_fixed_share'])}），ITEM_ID {int(full.loc['DIRECT_ITEM_ID_V1','joint_n'])}（{pct(full.loc['DIRECT_ITEM_ID_V1','joint_fixed_share'])}）；241条缺输出没有剔除。F28可输出完整范围，其他模型仅2,579条，因此完整范围CSV明确列scored_n，不用不同分母拼排名。配对60天有{int(p.loc['DIRECT','zero_actual_n'])}条明确零实销，保留绝对误差而不计算百分比命中。

## 418条未见对象的机制

418条对象×期次涉及{one[['shop','spu']].drop_duplicates().shape[0]}个不同店铺×SPU，全部在4—5个月货龄组。每条均有正销量记录、至少{int(one.observed_positive_days.min())}个正销量日，最近28天均已知；不能称“没有历史”或因身份无法映射而无样本。

{table(['类型','记录数','历史训练截点状态','为何当前仍有预测'],[['本期4个正销量完整月',int(counts.get(4,0)),'最早合格模拟签发日就是本期；此前均不足4月','当前特征合格，可使用跨商品共享模型'],['本期5个正销量完整月',int(counts.get(5,0)),'已有27—30个合格模拟签发日，但M1/M2标签均未成熟','推断不要求该款已有成熟训练标签']])}

{table(['可追溯对象/签发日','正销量月数','本期截止C','最早合格模拟签发r','最早M1标签结束','最早M2标签结束'],trace_examples)}

原规则每条历史特征截止r−2，标签为r+1..30、r+31..60；必须在本期C前结束。5个月例中M1最早标签结束恰好晚于C，因此即便仅差一天也不能放入训练。逐条836个分段记录见unseen_418_segment_causes.csv，已与实际训练行数0核对。无成熟资格样本已足以解释排除；不读取尚未成熟目标去补标签，也不放宽资格。本组未发现需要用“缺历史/缺日/映射异常”解释的记录；月源历史真实性限制仍继承原归档。

**不能只归咎未见商品。** 对两段均见过的2,010条，DIRECT→ITEM_ID的60天WAPE仍由{pct(seen.loc['DIRECT','wape'])}升到{pct(seen.loc['DIRECT_ITEM_ID_V1','wape'])}，Poisson偏差{seen.loc['DIRECT','poisson_deviance_mean']:.2f}→{seen.loc['DIRECT_ITEM_ID_V1','poisson_deviance_mean']:.2f}，联合命中{int(sj.loc['DIRECT','joint_n'])}→{int(sj.loc['DIRECT_ITEM_ID_V1','joint_n'])}。该组命中实销承载{n(sj.loc['DIRECT','joint_actual_units'])}→{n(sj.loc['DIRECT_ITEM_ID_V1','joint_actual_units'])}略升，仍应同时披露，不能声称每项都退化。

## 证据、复核与下一步边界

冻结DIRECT源码提交34bdc3b703c45326f3c638814ad18595c8124e83的4个文件Git内容哈希与原归档一致；训练矩阵从ITEM_ID归档读取，但原键、标签、10特征逐列哈希及dtype均与原DIRECT回执一致。旧代码仅静态审阅，没有调用训练入口。来源/版本见manifest.json、frozen_source_evidence.json、model_objective_evidence.csv。

独立复核：{v['checks']['training_unit_reconciliation']}个训练单元、{n(v['checks']['archived_nonoverlap_product_rows'])}个商品支持汇总、{v['checks']['independent_core_sample_rows']}个自身截止核心抽核键、8模型×3范围、{v['checks']['unseen_trace_checked']}条未见分段轨迹；Poisson公式另对照sklearn 1.6.1。CONTEXT90旧窗口文件的paired标记为空，本轮从固定对象表恢复统一配对范围，不改变任何预测。首次影子112文件与本轮读取的原档哈希保持不变。13项合成测试通过，涵盖核心边界并列、缺日UNKNOWN、标签成熟边界、零/未知偏差及无模型执行。

建议下一项仅改变历史核心身份的相对样本权重，是因为目前能直接证实的是行与标签量的结构差异和重复零窗口累积；**未证明Poisson错误，也未证明需要继续加商品身份或流量字段**。保留非核心、明确零值、退出核心商品以及下降组，可以检验是否以高估衰退款为代价。若没有同时改善五项门槛，仍应否决；当前不指定新模型、不更改资格、不执行该实验。

复现（已固定依赖，必须使用新的输出目录）：
```powershell
& '<workspace>/bc60_experiment_20261019/.venv/Scripts/python.exe' '<repo>/scripts/direct_training_alignment.py' --root '<workspace>' --out '<workspace>/new_alignment_output'
& '<workspace>/bc60_experiment_20261019/.venv/Scripts/python.exe' '<repo>/scripts/validate_direct_training_alignment.py' --root '<workspace>' --out '<workspace>/new_alignment_output'
& '<workspace>/bc60_experiment_20261019/.venv/Scripts/python.exe' '<repo>/scripts/report_direct_training_alignment.py' --root '<workspace>' --out '<workspace>/new_alignment_output'
```

CSV保留逐单元、货龄/店铺/历史核心构成、商品支持、三窗口全范围/配对/下降组评分和418条原因；没有复制第二套数百万行训练明细。早期调试输出目录保留但不作为最终交付；本目录为本轮最终数值版本。Git提交及本地交付哈希见delivery_receipt.json。业务汇总与明细不上传Git。研究属于反复使用的重建历史诊断，不是独立OOS，更不是预测准确率已经改善。
'''
    report.write_text(text,encoding='utf-8');print(report)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);a=p.parse_args();run(a.root,a.out)
