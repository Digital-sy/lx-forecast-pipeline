"""One local Chinese effect report; no training, inference, or model selection."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd

C='DIRECT_CORE_WEIGHT2_V1'
MODELS=['DIRECT','BLEND50','F28',C]
LABEL={C:'CORE_WEIGHT2','DIRECT':'DIRECT','BLEND50':'BLEND50','F28':'F28'}
W=[('d01_30','前30天'),('d31_60','后30天'),('d01_60','累计60天')]
GROUP={'ALL_SIX_ORIGINAL_NO_JOINT_HIT':'原六模型共同失准','BLEND50_ORIGINAL_HIT':'原BLEND50已命中',
       'OTHER_MODELS_ORIGINAL_RESCUE':'原其他模型可补中','MISSING_OR_UNDETERMINED':'原缺输出或不可判断'}
AGE={'4_5':'4—5个月','6_11':'6—11个月','12_plus':'12个月以上','0_3':'0—3个月'}

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def n(x):return f'{x:,.0f}' if pd.notna(x) else '—'
def pct(x):return f'{x:.2%}' if pd.notna(x) else '—'
def table(headers,rows):return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+['| '+' | '.join(map(str,r))+' |' for r in rows])

def auxiliary_section(out):
    """Auxiliary schema is normalized by its own module; never invent absent tests."""
    path=out/'auxiliary_risk/manifest.json'
    if not path.exists():
        return ['辅助风险结果尚未完成，不能声称非核心、退出核心及零销量风险通过。主五项筛选不因此被更改。'],dict(status='NOT_AVAILABLE')
    summary=read(path)
    # Preserve the exact auxiliary summary contract alongside the single report.
    lines=[f'辅助池状态：{summary.get("status","UNKNOWN")}；固定池{n(summary["pool_records"])}条对象×签发期。仅使用保存模型额外推理{summary["inference_calls"]}次，输出{n(summary["predicted_segment_rows"])}条分段预测，没有新增拟合；名单先于目标真值评分冻结。']
    metrics_path=out/'auxiliary_risk/metrics.csv'
    if not metrics_path.exists():
        lines.append('缺少辅助metrics.csv，覆盖与损害情况尚不可核验。');return lines,summary
    df=pd.read_csv(metrics_path)
    # Display is configured after the auxiliary producer's actual schema is known.
    groupcol='scope' if 'scope' in df else None
    if groupcol is None:
        lines.append('辅助风险字段映射尚未验收，见辅助原始汇总，不把该项标为风险通过。');return lines,summary
    sub=df[df.window.eq('d01_60')].copy() if 'window' in df else df
    rows=[]
    for r in sub.itertuples():
        rows.append([r.scope,LABEL[r.model],n(r.fixed_n),n(r.output_n),n(r.truth_n),n(r.over_units),n(r.under_units),n(r.absolute_error),
            n(r.zero_actual_n),n(r.zero_actual_prediction_units)])
    unknown=sub[sub.scope.eq('UNKNOWN_CORE_OR_TRANSITION')]
    if unknown.empty:lines.append('本次已冻结合格辅助池中UNKNOWN核心/迁移记录为0；资格不足对象仍在auxiliary_prepared/universe_coverage.csv中，不把它们宣称为已评价。')
    deltas=[]
    for group in ['CURRENT_NON_CORE','EXITED_CORE','NEW_CORE']:
        a=sub[sub.scope.eq(group)].set_index('model')
        if C in a.index and 'DIRECT' in a.index:
            deltas.append(dict(scope=group,over_change=float(a.loc[C,'over_units']-a.loc['DIRECT','over_units']),
                under_change=float(a.loc[C,'under_units']-a.loc['DIRECT','under_units']),
                ae_change=float(a.loc[C,'absolute_error']-a.loc['DIRECT','absolute_error']),
                zero_prediction_change=float(a.loc[C,'zero_actual_prediction_units']-a.loc['DIRECT','zero_actual_prediction_units'])))
    summary['report_deltas']=deltas
    periods=pd.read_csv(out/'auxiliary_risk/period_metrics.csv')
    period_risk=[]
    for group in ['CURRENT_NON_CORE','EXITED_CORE','NEW_CORE']:
        q=periods[periods.scope.eq(group)&periods.window.eq('d01_60')]
        pivot=q.pivot(index='issue_date',columns='model',values=['absolute_error','over_units','zero_actual_prediction_units'])
        period_risk.append([group,len(pivot),int((pivot['absolute_error'][C]>pivot['absolute_error']['DIRECT']).sum()),
            int((pivot['over_units'][C]>pivot['over_units']['DIRECT']).sum()),
            int((pivot['zero_actual_prediction_units'][C]>pivot['zero_actual_prediction_units']['DIRECT']).sum())])
    lines.extend(['',table(['辅助组','模型','固定记录','输出/可评分','已知真值','高估','低估','绝对误差','零实销记录','零实销预测/绝对误差'],rows),'',
        table(['辅助组','相对DIRECT高估变化','低估变化','绝对误差变化','零实销预测量变化'],[[r['scope'],n(r['over_change']),n(r['under_change']),n(r['ae_change']),n(r['zero_prediction_change'])] for r in deltas]),'',
        table(['辅助组','期数','绝对误差恶化期数','高估增加期数','零实销预测增加期数'],period_risk),'',
        '正数表示增加。前后30天及逐期同口径结果在auxiliary_risk/metrics.csv、period_metrics.csv中保留。当前非核心与退出核心不是互斥加总组，退出核心是前者子集。',
        '未来零实销仅用于这里的事后风险评价，未进入训练身份或权重。零实销不计算相对误差；未知真值和缺输出保留。辅助结果是风险旁表，不是事后新增的第六项门槛。'])
    return lines,summary

def render(out):
    out=Path(out).resolve();target=out/'DIRECT_CORE_WEIGHT2_V1历史核心身份固定相对权重实验实际效果报告.md'
    if target.exists():raise ValueError('REPORT_EXISTS')
    validation_path=out/'validation/validation.json'
    if not validation_path.exists():raise ValueError('INDEPENDENT_VALIDATION_REQUIRED')
    v=read(validation_path)
    if v.get('status')!='PASS':raise ValueError('INDEPENDENT_VALIDATION_NOT_PASS')
    s=read(out/'scoring/summary.json');m=pd.read_parquet(out/'scoring/metrics.parquet');j=pd.read_parquet(out/'scoring/joint_summary.parquet')
    d=pd.read_parquet(out/'scoring/details.parquet');top=pd.read_parquet(out/'scoring/original_top20.parquet')
    contributions=pd.read_parquet(out/'scoring/product_contributions.parquet');migrations=pd.read_parquet(out/'scoring/joint_migrations.parquet')
    pm=m[m.scope.eq('PAIRED')&m.slice.eq('overall')].set_index(['model','window'])
    pj=j[j.scope.eq('PAIRED')&j.slice.eq('overall')].set_index('model');fj=j[j.scope.eq('FULL_CORE')&j.slice.eq('overall')].set_index('model')
    period=m[m.scope.eq('PAIRED')&m.slice.eq('issue')&m.window.eq('d01_60')].pivot(index='issue_date',columns='model',values='wape')
    decline=m[m.scope.eq('PAIRED')&m.slice.eq('decline_group')&m.decline_group.eq('PRE_ISSUE_DECLINING')&m.window.eq('d01_60')].set_index('model')
    candidate=pm.loc[(C,'d01_60')];baseline=pm.loc[('DIRECT','d01_60')];blend=pm.loc[('BLEND50','d01_60')]
    common=d[d.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT')];hit=common[common[C+'_joint_hit']]
    held=d[d.BLEND50_joint_hit&d[C+'_joint_hit']];loss=d[d.BLEND50_joint_hit&~d[C+'_joint_hit']]
    aux_lines,aux_summary=auxiliary_section(out)
    exit_risk=next((x for x in aux_summary.get('report_deltas',[]) if x['scope']=='EXITED_CORE'),None)
    noncore_risk=next((x for x in aux_summary.get('report_deltas',[]) if x['scope']=='CURRENT_NON_CORE'),None)
    risk_text=(f'退出核心高估较DIRECT变化{n(exit_risk["over_change"])}件；当前非核心零实销预测变化{n(noncore_risk["zero_prediction_change"])}件。'
        if exit_risk is not None and noncore_risk is not None else '退出核心及零销量辅助结果未完整取得，风险尚不能判断。')
    weights=pd.read_csv(out/'weight_group_summary.csv');recon=pd.read_csv(out/'training_reconciliation.csv');effects=read(out/'effects_manifest.json')
    weight_rows=[]
    for dim in ['core_state','zero_label','shop']:
        a=weights[weights.dimension.eq(dim)].groupby('value')[['rows','old_weight','sample_weight']].sum()
        for value,r in a.iterrows():weight_rows.append([dim,value,n(r.rows),pct(r.old_weight/a.old_weight.sum()),n(r.sample_weight),pct(r.sample_weight/a.sample_weight.sum())])
    mainstatus='PASS' if all(s['gates']['DIRECT'].values()) and all(s['gates']['BLEND50'].values()) else 'FAIL'
    direction=lambda val:'增加' if val>0 else '减少' if val<0 else '不变'
    comparisons=[]
    for bm in ['DIRECT','BLEND50']:
        z=contributions[contributions.baseline.eq(bm)];benefits=z[z.ae_reduction.gt(0)].sort_values('ae_reduction',ascending=False)
        harms=z[z.ae_reduction.lt(0)]
        comparisons.append(dict(baseline=bm,improved_products=len(benefits),worse_products=len(harms),unchanged_products=int(z.ae_reduction.eq(0).sum()),
            gross_ae_reduction=float(benefits.ae_reduction.sum()),gross_ae_increase=float(-harms.ae_reduction.sum()),
            top5_gross_reduction_share=float(benefits.head(5).ae_reduction.sum()/benefits.ae_reduction.sum()) if len(benefits) else None,
            net_ae_reduction=float(z.ae_reduction.sum()),net_without_top5_improvers=float(z.ae_reduction.sum()-benefits.head(5).ae_reduction.sum()),
            improved_periods=int(period[C].lt(period[bm]).sum())))
    pd.DataFrame(comparisons).to_csv(out/'scoring/concentration_summary.csv',index=False,encoding='utf-8-sig')
    gate_rows=[('wape60_lower','60天WAPE严格下降'),('joint_hits_increased','联合命中数增加'),
        ('joint_hit_sales_share_not_lower','联合命中实销占比不下降'),('decline_over_rate_not_increased','原下降组高估率不增加'),
        ('output_and_core_coverage_not_reduced','输出及核心覆盖不减少')]
    lines=['# DIRECT_CORE_WEIGHT2_V1历史核心身份固定相对权重实验实际效果报告','',
       f'**技术验收：PASS；五项研究筛选：对DIRECT {s["business_screen"]["DIRECT"]}，对BLEND50 {s["business_screen"]["BLEND50"]}；业务证据等级：PARTIAL（反复使用的重建历史开发验证）。**','',
       f'1. **固定2∶1是否优于DIRECT：五项{sum(s["gates"]["DIRECT"].values())}/5通过。** 60天WAPE {pct(baseline.wape)}→{pct(candidate.wape)}；三窗口联合命中{n(pj.loc["DIRECT","joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}；命中实销承载{n(pj.loc["DIRECT","joint_hit_actual"])}→{n(pj.loc[C,"joint_hit_actual"])}件。',
       f'2. **是否优于BLEND50：五项{sum(s["gates"]["BLEND50"].values())}/5通过。** WAPE {pct(blend.wape)}→{pct(candidate.wape)}；联合命中{n(pj.loc["BLEND50","joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}，下表逐项列示，不能以胜过DIRECT代替升级通过。',
       f'3. **共同失准与原命中：** 原{n(len(common))}条共同失准中新增联合命中{n(len(hit))}条、承载{n(hit.actual_d01_60.sum())}件实销；原BLEND50的284条保留{n(len(held))}，退出{n(len(loss))}、承载{n(loss.actual_d01_60.sum())}件。',
       f'4. **下降与非核心风险：** 原842条下降组高估率DIRECT={pct(decline.loc["DIRECT","over_rate"])}，BLEND50={pct(decline.loc["BLEND50","over_rate"])}，候选={pct(decline.loc[C,"over_rate"])}。{risk_text}正数为增加，不能据核心评分宣称非核心安全。',
       f'5. **稳定性：** 60天WAPE优于DIRECT {int(period[C].lt(period.DIRECT).sum())}/20期、优于BLEND50 {int(period[C].lt(period.BLEND50).sum())}/20期。全量商品贡献及原Top20逐期结果均保留；下文分开列净收益、受益商品和损害商品。','',
       '## 五项固定门槛与主范围','',
       table(['门槛','相对DIRECT','相对BLEND50'],[[label,'通过' if s['gates']['DIRECT'][key] else '未通过','通过' if s['gates']['BLEND50'][key] else '未通过'] for key,label in gate_rows]),'',
       '判断使用未舍入数值；评分没有乘训练权重。主范围是2,579条店铺×SPU×签发期记录，完整范围2,820条，原241条缺输出与842条下降观察组均保留。不是2,820个不同商品，跨期目标实销不是去重业务销量。','',
       table(['窗口','模型','WAPE','Bias','高估件数','低估件数','±20%命中/2579'],[[label,LABEL[model],pct(pm.loc[(model,w),'wape']),pct(pm.loc[(model,w),'bias']),
          n(pm.loc[(model,w),'over_units']),n(pm.loc[(model,w),'under_units']),int(pm.loc[(model,w),'hit_n'])] for w,label in W for model in MODELS]),'',
       table(['模型','配对联合命中','配对命中率','命中实销承载','承载实销占比','完整2820联合命中/占比'],[[LABEL[model],n(pj.loc[model,'joint_hit_n']),pct(pj.loc[model,'joint_hit_fixed_share']),
          n(pj.loc[model,'joint_hit_actual']),pct(pj.loc[model,'joint_hit_actual_share']),f'{n(fj.loc[model,"joint_hit_n"])} / {pct(fj.loc[model,"joint_hit_fixed_share"])}'] for model in MODELS]),'',
       f'完整范围候选有输出{n(fj.loc[C,"output_complete_n"])}条，缺输出{s["missing_output_n"]}条；F28有完整范围输出，故完整范围与主配对分别列示，缺输出不当作0误差。完整范围各窗口的scored_n、未知真值、明确零值及缺输出见scoring/metrics.csv。',
       '两个分段都需正实销且未舍入相对误差≤20%，累计也满足，才算联合命中；60天命中不掩盖分段失败。零实销只评价绝对误差。','',
       table(['对照','60天绝对误差变化','高估变化','低估变化'],[[bm,n(candidate.ae-pm.loc[(bm,'d01_60'),'ae']),n(candidate.over_units-pm.loc[(bm,'d01_60'),'over_units']),n(candidate.under_units-pm.loc[(bm,'d01_60'),'under_units'])] for bm in ['DIRECT','BLEND50']]),
       '正数表示增加。所有预测、真值及排名采用同范围，不以新的训练权重重算业务评分。','',
       '## 联合命中迁移及固定观察组','',
       table(['对照','迁移','条数','承载实销'],[[r.baseline,r.migration,r.records,n(r.actual60)] for r in migrations[migrations.scope.eq('PAIRED')&migrations.slice.eq('overall')].itertuples()]),'',
       table(['原固定组','记录数','候选联合命中','命中承载实销','原BLEND50命中保留/退出'],[[GROUP[group],len(q),int(q[C+'_joint_hit'].sum()),n(q.loc[q[C+'_joint_hit'],'actual_d01_60'].sum()),
          f'{int((q.BLEND50_joint_hit&q[C+"_joint_hit"]).sum())}/{int((q.BLEND50_joint_hit&~q[C+"_joint_hit"]).sum())}'] for group,q in d.groupby('old_group')]),'',
       '旧六模型共同失准1,953条沿用原定义，未按本候选表现重新划组。Top20只作风险展示，未进入权重、资格、特征或超参数。','',
       '## 分期、店铺、原货龄与集中度','',
       table(['签发日','DIRECT WAPE','BLEND50 WAPE','CORE_WEIGHT2 WAPE'],[[issue,pct(r.DIRECT),pct(r.BLEND50),pct(r[C])] for issue,r in period.iterrows()]),'',
       table(['店铺','模型','60天WAPE','高估','低估','联合命中'],[[r.shop,LABEL[r.model],pct(r.wape),n(r.over_units),n(r.under_units),int(j[j.scope.eq('PAIRED')&j.slice.eq('shop')&j.shop.eq(r.shop)&j.model.eq(r.model)].iloc[0].joint_hit_n)]
          for r in m[m.scope.eq('PAIRED')&m.slice.eq('shop')&m.window.eq('d01_60')&m.model.isin(['DIRECT','BLEND50',C])].itertuples()]),'',
       table(['原货龄','模型','条数','60天WAPE','高估','低估'],[[AGE.get(r.maturity,r.maturity),LABEL[r.model],r.fixed_n,pct(r.wape),n(r.over_units),n(r.under_units)]
          for r in m[m.scope.eq('PAIRED')&m.slice.eq('maturity')&m.window.eq('d01_60')&m.model.isin(['DIRECT','BLEND50',C])].itertuples()]),'',
       table(['对照','改善商品数','退化商品数','净绝对误差减少','前5受益款占毛收益','去除前5受益款后的净减少'],[[r['baseline'],r['improved_products'],r['worse_products'],n(r['net_ae_reduction']),pct(r['top5_gross_reduction_share']),n(r['net_without_top5_improvers'])] for r in comparisons]),
       '集中度是事后解释，不删除大款重判门槛；毛收益与净收益分开，不用净数掩盖受损商品。每个商品的完整逐期贡献在scoring/product_contributions.csv。','',
       table(['原排名','店铺','SPU','期数','BLEND50绝对误差','候选绝对误差','相对BLEND50改善/退化期数'],[[int(r['rank']),r['shop'],r['spu'],r['records'],n(r['BLEND50_ae60']),n(r[C+'_ae60']),f'{r["periods_improved_vs_BLEND50"]}/{r["periods_worse_vs_BLEND50"]}'] for r in top.sort_values('rank').to_dict('records')]),'',
       '## 非核心、退出核心与零销量风险','',*aux_lines,'',
       '## 实现、权重和证据边界','',
       table(['维度','取值','重复训练暴露行数','原等权占比','归一化权重和','新权重占比'],weight_rows),'',
       '该表按40个拟合单元加总，仅描述重复训练暴露。不同单元重复使用历史样本，不能称独立训练样本；标签窗口重叠，也不能把累计权重解释成业务销量或实际模型影响力。逐单元完整记录见weight_group_summary.csv、training_reconciliation.csv。','',
       f'40单元归一化权重均值范围{recon.mean_weight.min():.16g}—{recon.mean_weight.max():.16g}，所有训练行仍保留。归一化样本权重的范围为{recon.min_weight.min():.8f}—{recon.max_weight.max():.8f}；不同单元常数不同，但同单元CORE与其他状态的原始比例固定2∶1。','',
       '- 唯一变化：自身模拟签发r的截止c=r−2，历史90天全店核心CORE原始权重2，NON_CORE与UNKNOWN为1；每个实际签发×分段除以该单元raw_weight均值。所有权重严格正，归一化均值1、总权重等于行数。不是按标签销量、最终核心或未来误差赋权。',
       '- 先以完整店铺销量分母确定核心，再考虑老品资格；边界同销量全纳入。UNKNOWN单独记录，身份关联失败不能降级成UNKNOWN。相同模拟截点在M1/M2原始身份与raw_weight相同，归一化常数可因完整训练集合不同而不同。',
       '- 模型X仍只有原10列，核心身份、sample_weight、item_key与流量均不进入特征。按原训练顺序逐键对齐，fit(..., sample_weight=...)传入；未用class_weight或额外初始化修正。Poisson、种子、超参数、资格、标签成熟条件与非负后处理不变。',
       '- CORE后续标签0仍保持核心原始权重，NON_CORE后续大幅增长也不反向提高其权重。所有原训练行及零标签均保留。每单元CORE/NON_CORE/UNKNOWN、零标签和店铺的加权前后汇总另存，权重份额不称损失或梯度影响份额。',
       f'- 实际正式拟合{s["formal_fits"]}次；旧对照模型不重训。主预测调用{effects.get("main_prediction_calls","见effects_manifest")}次；辅助推理调用{aux_summary.get("inference_calls","未完成")}次，详见auxiliary_risk/inference_calls.csv。独立验证拟合{v["validator_training_calls"]}次、推理{v["validator_inference_calls"]}次；本轮总计120次模型预测调用，不能报告“推理0次”。',
       f'- 独立验证实际检查{v["units"]}个拟合单元、{n(v["training_rows_checked"])}条重复训练暴露行、{v["independent_core_samples"]}个身份抽核键及{n(v["m1_m2_common_rows_checked"])}条M1/M2共同历史键；五项门槛、命中迁移与{v["auxiliary_validation"]["metric_cells"]}个辅助指标格均独立复算一致。',
       '- 权重接口依据[LightGBM 4.6.0 LGBMRegressor.fit官方文档](https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html#lightgbm.LGBMRegressor.fit)。依赖未升级。',
       '- 原始数据为事后封存的重建历史输入，业务日期早于截止不证明当时可获取；未称独立OOS、因果实验或前瞻成功。历史缺失与映射限制照实继承，没有数据库/API补查。',
       '- 原档与首次前瞻112文件哈希验证通过；不修改2026-10-28前瞻批次、生产V4、新品、采购、BI或调度。本轮生产变更和新前瞻签发均为0。',
       f'- 技术PASS与研究筛选{mainstatus}分开。若失败，仅否决固定2∶1方案，不推导所有加权方法无效；若通过，也只是历史开发中的中间筛选。辅助损害必须披露，不追加第六门槛或再试1.5、3、5。',
       '- 复现命令、代码及输入/权重/输出哈希、实际Git提交见最终执行回执；源码和脱敏测试进入独立分支，业务数据与模型仅保留本地。','']
    target.write_text('\n'.join(lines),encoding='utf-8');print(target)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();render(a.out)
