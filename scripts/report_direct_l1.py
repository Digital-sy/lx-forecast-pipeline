"""Render one local Chinese L1 effect report, after independent validation."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd

C='DIRECT_L1_V1'
SIDE='DIRECT_CORE_WEIGHT2_V1'
LABEL={C:'DIRECT_L1',SIDE:'CORE_WEIGHT2','DIRECT':'DIRECT','BLEND50':'BLEND50','F28':'F28'}
MAIN=['DIRECT','BLEND50',C]
ALL=['DIRECT','BLEND50',C,'F28',SIDE]
WINDOWS=[('d01_30','前30天'),('d31_60','后30天'),('d01_60','累计60天')]
GROUP={'ALL_SIX_ORIGINAL_NO_JOINT_HIT':'原六模型共同失准','BLEND50_ORIGINAL_HIT':'原BLEND50已命中',
 'OTHER_MODELS_ORIGINAL_RESCUE':'原其他模型可补中','MISSING_OR_UNDETERMINED':'原缺输出或不可判断'}
DIR={'ALL_OVER':'原共同高估','ALL_UNDER':'原共同低估','OPPOSITE':'原方向相反','ZERO_ACTUAL':'原明确零实销'}
AGE={'4_5':'4—5个月','6_11':'6—11个月','12_plus':'12个月以上','0_3':'0—3个月'}

def read(p):return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def n(x):return f'{x:,.0f}' if pd.notna(x) else '—'
def pct(x):return f'{x:.2%}' if pd.notna(x) else '—'
def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+['| '+' | '.join(map(str,r))+' |' for r in rows])

def render(out,replace_report=False):
    out=Path(out).resolve();target=out/'DIRECT_L1_V1训练目标单一增量实验实际效果报告.md'
    if target.exists() and not replace_report:raise ValueError('REPORT_EXISTS')
    v=read(out/'validation/validation.json')
    if v.get('status')!='PASS':raise ValueError('INDEPENDENT_VALIDATION_REQUIRED')
    s=read(out/'scoring/summary.json');e=read(out/'effects_manifest.json')
    m=pd.read_parquet(out/'scoring/metrics.parquet');j=pd.read_parquet(out/'scoring/joint_summary.parquet')
    d=pd.read_parquet(out/'scoring/details.parquet');migration=pd.read_parquet(out/'scoring/joint_migrations.parquet')
    directions=pd.read_parquet(out/'scoring/old_direction_metrics.parquet');dc=pd.read_parquet(out/'scoring/old_direction_changes.parquet')
    top=pd.read_parquet(out/'scoring/original_top20.parquet');pc=pd.read_parquet(out/'scoring/product_contributions.parquet')
    top_support=pc[pc.baseline.eq('DIRECT')].set_index(['shop','spu'])
    top_selected=top[['shop','spu']].merge(pc[pc.baseline.eq('DIRECT')],on=['shop','spu'],validate='one_to_one')
    reproduction=(Path(__file__).resolve().parents[1]/'docs/direct_l1_v1_reproduction.md').as_posix()
    clipping_receipts=[read(out/p) for p in e['files'] if p.endswith('/result.json')]
    if len(clipping_receipts)!=e['formal_fits']:raise ValueError('CLIPPING_RECEIPT_SCOPE')
    main_negative_n=sum(x['negative_raw_n'] for x in clipping_receipts)
    main_clipped_amount=sum(x['negative_raw_units'] for x in clipping_receipts)
    aux_clip=pd.read_csv(out/'auxiliary_risk/clipping_summary.csv',float_precision='round_trip')
    pm=m[m.scope.eq('PAIRED')&m.slice.eq('overall')].set_index(['model','window'])
    pj=j[j.scope.eq('PAIRED')&j.slice.eq('overall')].set_index('model');fj=j[j.scope.eq('FULL_CORE')&j.slice.eq('overall')].set_index('model')
    period=m[m.scope.eq('PAIRED')&m.slice.eq('issue')&m.window.eq('d01_60')].pivot(index='issue_date',columns='model',values='wape')
    decline=m[m.scope.eq('PAIRED')&m.slice.eq('decline_group')&m.decline_group.eq('PRE_ISSUE_DECLINING')&m.window.eq('d01_60')].set_index('model')
    cand=pm.loc[(C,'d01_60')];direct=pm.loc[('DIRECT','d01_60')];blend=pm.loc[('BLEND50','d01_60')]
    common=d[d.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT')];new=common[common[C+'_joint_hit']]
    retained=d[d.BLEND50_joint_hit&d[C+'_joint_hit']];lost=d[d.BLEND50_joint_hit&~d[C+'_joint_hit']]
    dir60=directions[directions.group_kind.eq('direction')&directions.old_direction.eq('ALL_UNDER')&directions.window.eq('d01_60')].set_index('model')
    am=read(out/'auxiliary_risk/manifest.json');aux=pd.read_csv(out/'auxiliary_risk/metrics.csv')
    ap=pd.read_csv(out/'auxiliary_risk/period_metrics.csv');a60=aux[aux.window.eq('d01_60')]
    ar=[]
    for name in ['ALL_ELIGIBLE','CURRENT_NON_CORE','EXITED_CORE','NEW_CORE','UNKNOWN_CORE_OR_TRANSITION']:
        z=a60[a60.scope.eq(name)].set_index('model')
        if C not in z.index or 'DIRECT' not in z.index:continue
        q=ap[ap.scope.eq(name)&ap.window.eq('d01_60')]
        p=q.pivot(index='issue_date',columns='model',values=['absolute_error','over_units','under_units'])
        ar.append(dict(scope=name,records=int(z.loc[C,'fixed_n']),ae_change=float(z.loc[C,'absolute_error']-z.loc['DIRECT','absolute_error']),
            over_change=float(z.loc[C,'over_units']-z.loc['DIRECT','over_units']),under_change=float(z.loc[C,'under_units']-z.loc['DIRECT','under_units']),
            zero_actual_prediction_change=float(z.loc[C,'zero_actual_prediction_units']-z.loc['DIRECT','zero_actual_prediction_units']),
            positive_actual_zero_prediction_n=int(z.loc[C,'positive_actual_zero_prediction_n']),
            positive_actual_zero_prediction_actual=float(z.loc[C,'positive_actual_zero_prediction_actual_units']),
            ae_worse_periods=int((p.absolute_error[C]>p.absolute_error.DIRECT).sum()) if len(p) else 0,
            over_worse_periods=int((p.over_units[C]>p.over_units.DIRECT).sum()) if len(p) else 0,
            under_worse_periods=int((p.under_units[C]>p.under_units.DIRECT).sum()) if len(p) else 0))
    conc=[]
    for base in ['DIRECT','BLEND50']:
        z=pc[pc.baseline.eq(base)];benefit=z[z.ae_reduction.gt(0)].sort_values('ae_reduction',ascending=False)
        conc.append(dict(baseline=base,improved_products=int(z.ae_reduction.gt(0).sum()),worse_products=int(z.ae_reduction.lt(0).sum()),
            net_ae_reduction=float(z.ae_reduction.sum()),top5_gross_benefit_share=float(benefit.head(5).ae_reduction.sum()/benefit.ae_reduction.sum()) if len(benefit) else None,
            net_without_top5=float(z.ae_reduction.sum()-benefit.head(5).ae_reduction.sum()),lower_prediction_products=int(z.prediction_change.lt(0).sum()),
            higher_prediction_products=int(z.prediction_change.gt(0).sum())))
    if not (out/'scoring/concentration_summary.csv').exists():
        pd.DataFrame(conc).to_csv(out/'scoring/concentration_summary.csv',index=False,encoding='utf-8-sig')
    noncore=next(x for x in ar if x['scope']=='CURRENT_NON_CORE')
    calls=int(e.get('main_prediction_calls',0))+int(am.get('inference_calls',0))+int(v.get('validator_inference_calls',0))
    gates=[('wape60_lower','60天WAPE严格下降'),('joint_hits_increased','联合命中数增加'),
       ('joint_hit_sales_share_not_lower','联合命中实销占比不下降'),('decline_over_rate_not_increased','原下降组高估率不增加'),
       ('output_and_core_coverage_not_reduced','输出及核心覆盖不减少')]
    lines=['# DIRECT_L1_V1训练目标单一增量实验实际效果报告','',
      f'**技术PASS；研究筛选：相对DIRECT {s["business_screen"]["DIRECT"]}，相对BLEND50 {s["business_screen"]["BLEND50"]}；业务证据PARTIAL（重建历史开发验证，不是独立OOS）。**','',
      f'实际计算开始：{e["started_at"]}；候选封存：{e["completed_at"]}。时间沿用真实带时区执行回执，目录中的20261028仅为任务命名，不代表执行日期。','',
      f'1. **是否优于DIRECT：五项{sum(s["gates"]["DIRECT"].values())}/5通过。** 60天WAPE {pct(direct.wape)}→{pct(cand.wape)}，绝对误差变化{n(cand.ae-direct.ae)}件；联合命中{n(pj.loc["DIRECT","joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}，命中实销{n(pj.loc["DIRECT","joint_hit_actual"])}→{n(pj.loc[C,"joint_hit_actual"])}件。',
      f'2. **是否优于BLEND50：五项{sum(s["gates"]["BLEND50"].values())}/5通过。** WAPE {pct(blend.wape)}→{pct(cand.wape)}，差{100*(cand.wape-blend.wape):+.8f}个百分点；联合命中{n(pj.loc["BLEND50","joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}。严格门槛不因差值小而放宽。',
      f'3. **是否只是降低预测总量：** 同配对60天预测总量DIRECT {n(direct.prediction_units)}→候选{n(cand.prediction_units)}件；高估变化{n(cand.over_units-direct.over_units)}，低估变化{n(cand.under_units-direct.under_units)}。原共同低估组低估量变化{n(dir60.loc[C,"under_units"]-dir60.loc["DIRECT","under_units"])}件，未用总体WAPE掩盖方向风险。',
      f'4. **共同失准与零预测：** 原1,953条共同失准新增联合命中{n(len(new))}条、承载{n(new.actual_d01_60.sum())}件；原BLEND50的284条保留{n(len(retained))}、退出{n(len(lost))}。主配对60天正实销却预测0为{n(cand.positive_actual_zero_prediction_n)}条、实销{n(cand.positive_actual_zero_prediction_actual)}件。',
      f'5. **稳定性及其他商品：** 60天WAPE优于DIRECT {int(period[C].lt(period.DIRECT).sum())}/20期、优于BLEND50 {int(period[C].lt(period.BLEND50).sum())}/20期；非核心绝对误差变化{n(noncore["ae_change"])}件，高估变化{n(noncore["over_change"])}、低估变化{n(noncore["under_change"])}，绝对误差恶化{noncore["ae_worse_periods"]}/20期。','',
      '正的变化量表示增加，负值表示减少。以上是同对象同真值的实际结果，不能视为损失函数的因果结论，也不改变已经封存的前瞻预测。','',
      '## 五项筛选与三个窗口','',
      table(['门槛','相对DIRECT','相对BLEND50'],[[label,'通过' if s['gates']['DIRECT'][key] else '未通过','通过' if s['gates']['BLEND50'][key] else '未通过'] for key,label in gates]),'',
      '主配对2,579条、完整2,820条店铺×SPU×签发期记录；241条原缺输出、842条原下降观察组均保留。评分完全等权，未使用上一轮训练权重，不把不同期同款重复记录称为独立商品。','',
      table(['窗口','模型','WAPE','Bias','预测总量','高估','低估','±20%命中/2579','命中实销','命中实销占比'],[[label,LABEL[model],pct(pm.loc[(model,w),'wape']),pct(pm.loc[(model,w),'bias']),n(pm.loc[(model,w),'prediction_units']),n(pm.loc[(model,w),'over_units']),n(pm.loc[(model,w),'under_units']),n(pm.loc[(model,w),'hit_n']),n(pm.loc[(model,w),'hit_actual']),pct(pm.loc[(model,w),'hit_actual_share'])] for w,label in WINDOWS for model in MAIN]),'',
      table(['模型','配对联合命中','配对命中率','承载实销','实销占比','完整2820联合命中/占比'],[[LABEL[model],n(pj.loc[model,'joint_hit_n']),pct(pj.loc[model,'joint_hit_fixed_share']),n(pj.loc[model,'joint_hit_actual']),pct(pj.loc[model,'joint_hit_actual_share']),f'{n(fj.loc[model,"joint_hit_n"])} / {pct(fj.loc[model,"joint_hit_fixed_share"])}'] for model in ALL]),'',
      '两个30天和累计60天均以正实销为分母，未舍入相对误差≤20%才联合命中；真实0销量单列绝对误差，缺预测不是0。候选输出范围与DIRECT逐对象一致，完整范围各窗口缺失/真值/评分数量保存在metrics.csv。','',
      '### 旧结果旁表：不重训F28或CORE_WEIGHT2','',
      table(['窗口','模型','WAPE','Bias','±20%命中','命中实销承载'],[[label,LABEL[model],pct(pm.loc[(model,w),'wape']),pct(pm.loc[(model,w),'bias']),n(pm.loc[(model,w),'hit_n']),n(pm.loc[(model,w),'hit_actual'])] for w,label in WINDOWS for model in ['F28',SIDE]]),'',
      '## 命中迁移、共同误差方向与下降组','',
      table(['对照','迁移','记录数','实销承载'],[[r.baseline,r.migration,r.records,n(r.actual60)] for r in migration[migration.scope.eq('PAIRED')&migration.slice.eq('overall')].itertuples()]),'',
      table(['原固定分组','完整范围记录数','候选联合命中','候选命中实销'],[[GROUP[name],len(g),int(g[C+'_joint_hit'].sum()),n(g.loc[g[C+'_joint_hit'],'actual_d01_60'].sum())] for name,g in d.groupby('old_group')]),'',
      '原分组不因本轮结果重选。分组按完整2820条展示，原DIRECT缺输出但其他模型可命中的对象仍保留在其他模型可补中组；缺输出总数始终单独核验。','',
      table(['窗口','原方向','模型','固定记录','WAPE','高估','低估','预测量','命中实销'],[[dict(WINDOWS)[r.window],DIR.get(r.old_direction,r.old_direction),LABEL[r.model],r.fixed_n,pct(r.wape),n(r.over_units),n(r.under_units),n(r.prediction_units),n(r.hit_actual)] for r in directions[directions.group_kind.eq('direction')&directions.model.isin(MAIN)].itertuples()]),'',
      table(['窗口','原共同低估组对照','记录数','预测量变化','低估变化','高估变化','绝对误差变化','窗口命中新增/退出'],[[dict(WINDOWS)[r.window],r.baseline,r.records,n(r.prediction_change),n(r.under_change),n(r.over_change),n(r.ae_change),f'{r.hit_gained}/{r.hit_lost}'] for r in dc[dc.old_direction.eq('ALL_UNDER')].itertuples()]),'',
      table(['模型','下降组记录','60天高估率','高估','低估','WAPE'],[[LABEL[model],n(decline.loc[model,'fixed_n']),pct(decline.loc[model,'over_rate']),n(decline.loc[model,'over_units']),n(decline.loc[model,'under_units']),pct(decline.loc[model,'wape'])] for model in MAIN]),'',
      '## 分期、店铺、原货龄与大款','',
      table(['签发日','DIRECT WAPE','BLEND50 WAPE','L1 WAPE'],[[issue,pct(r.DIRECT),pct(r.BLEND50),pct(r[C])] for issue,r in period.iterrows()]),'',
      table(['店铺','模型','60天WAPE','预测总量','高估','低估'],[[r.shop,LABEL[r.model],pct(r.wape),n(r.prediction_units),n(r.over_units),n(r.under_units)] for r in m[m.scope.eq('PAIRED')&m.slice.eq('shop')&m.window.eq('d01_60')&m.model.isin(MAIN)].itertuples()]),'',
      table(['原货龄','模型','记录','60天WAPE','高估','低估'],[[AGE.get(r.maturity,r.maturity),LABEL[r.model],r.fixed_n,pct(r.wape),n(r.over_units),n(r.under_units)] for r in m[m.scope.eq('PAIRED')&m.slice.eq('maturity')&m.window.eq('d01_60')&m.model.isin(MAIN)].itertuples()]),'',
      table(['对照','改善/退化商品数','净AE减少','前5受益款占毛收益','去除前5后净减少','总预测降低/增加商品数'],[[r['baseline'],f'{r["improved_products"]}/{r["worse_products"]}',n(r['net_ae_reduction']),pct(r['top5_gross_benefit_share']),n(r['net_without_top5']),f'{r["lower_prediction_products"]}/{r["higher_prediction_products"]}'] for r in conc]),'',
      '集中度及大款只作事后诊断，不删大款重判门槛。当前类别标签只在CSV中用于描述，未进入模型。原Top20名单不重新排序挑选。','',
      f'原Top20中，{int(top_selected.prediction_change.lt(0).sum())}款累计预测量低于DIRECT，{int((top_selected.prediction_lower_periods>top_selected.records/2).sum())}款在多数可评期次中更低；但在所有可评期次都更低的商品为{int(top_selected.prediction_lower_periods.eq(top_selected.records).sum())}款。可观察到跨期反复降低预测，并非每款每期一致压低。下表的“预测降低期数”直接比较预测值，不由绝对误差变化推断。','',
      table(['原排名','店铺/SPU','实销','DIRECT预测','L1预测','DIRECT低估','L1低估','DIRECT AE','L1 AE','预测降低期数/期数','AE改善/退化期数'],[[int(r['rank']),r['shop']+'/'+r['spu'],n(r['actual60']),n(r['DIRECT_prediction60']),n(r[C+'_prediction60']),n(r['DIRECT_under60']),n(r[C+'_under60']),n(r['DIRECT_ae60']),n(r[C+'_ae60']),f'{int(top_support.loc[(r["shop"],r["spu"]),"prediction_lower_periods"])}/{r["records"]}',f'{r["periods_improved_vs_DIRECT"]}/{r["periods_worse_vs_DIRECT"]}'] for r in top.sort_values('rank').to_dict('records')]),'',
      '## 零预测、负原始输出与辅助风险','',
      table(['窗口','模型','正实销但预测0记录','其实际销量','真实0销量记录','真实0绝对误差'],[[label,LABEL[model],n(pm.loc[(model,w),'positive_actual_zero_prediction_n']),n(pm.loc[(model,w),'positive_actual_zero_prediction_actual']),n(pm.loc[(model,w),'zero_truth_n']),n(pm.loc[(model,w),'zero_truth_ae'])] for w,label in WINDOWS for model in MAIN]),'',
      table(['窗口','候选负raw窗口数','负raw分段个数'],[[label,n(pm.loc[(C,w),'raw_negative_n']),n(pm.loc[(C,w),'raw_negative_segment_count'])] for w,label in WINDOWS]),
      '负raw仅是后处理前模型输出，最终预测保持非负。原后处理对每段clip至0后相加；60天raw和与两段clip后和不同，负raw分段个数另列。所有raw与最终prediction均保留。未知预测、明确0及缺输出没有合并。','',
      f'实际截断回执：主评价{main_negative_n}条负原始分段预测，截断增量合计{main_clipped_amount:.13f}件；辅助全集{int(aux_clip.negative_raw_n.sum())}条，截断增量合计{float(aux_clip.clipped_amount.sum()):.13f}件。主评价数字来自40个effects/result.json，辅助数字来自auxiliary_risk/clipping_summary.csv；辅助全集包含主评价对象，两个数量不可相加。截断只是原非负后处理，不是新增纠偏。','',
      f'辅助池{n(am["pool_records"])}条，新增候选模型推理{am["inference_calls"]}次；DIRECT复用原逐款推理，不重新训练。该辅助池不替代2820/2579主评价。','',
      table(['辅助组','模型','记录/可评分','高估','低估','AE','零实销预测','正实销预测0/对应销量'],[[r.scope,LABEL[r.model],f'{r.fixed_n}/{r.scored_n}',n(r.over_units),n(r.under_units),n(r.absolute_error),n(r.zero_actual_prediction_units),f'{r.positive_actual_zero_prediction_n}/{n(r.positive_actual_zero_prediction_actual_units)}'] for r in a60.itertuples()]),'',
      table(['辅助组','AE变化','高估变化','低估变化','零实销预测变化','AE/高估/低估恶化期数'],[[r['scope'],n(r['ae_change']),n(r['over_change']),n(r['under_change']),n(r['zero_actual_prediction_change']),f'{r["ae_worse_periods"]}/{r["over_worse_periods"]}/{r["under_worse_periods"]}'] for r in ar]),'',
      '退出核心是当前非核心子集，新进入核心是当前核心子集，不能把重叠组加总。UNKNOWN为0不表示资格不足对象已评价。未来实销0只用于事后风险评价；辅助损害单独披露，不新增第六项门槛。','',
      '## 实施与独立验证','',
      f'- 正式拟合{e["formal_fits"]}次。主预测调用{e.get("main_prediction_calls",0)}次（实时{e.get("live_prediction_calls",0)}、模型保存回读{e.get("reload_prediction_calls",0)}）；辅助{am["inference_calls"]}次，独立验证{v.get("validator_inference_calls",0)}次，共{calls}次。没有新增数据库/API查询、生产变更或前瞻签发。',
      '- 唯一训练改动是objective由Poisson改regression_l1；对应训练metric采用l1，不拿测试目标期early stopping选模型。原10列、训练键、标签、资格、范围、种子和其他显式超参数不变；未传样本权重、未加入商品身份/流量/年度信息。',
      '- L1的中位数初始化及残差中位数叶值是LightGBM原生行为，不是额外手工调低预测。原Poisson的指数输出及poisson_max_delta_step不适用于L1，原配置未显式另设该参数。依据[LightGBM 4.6.0官方参数](https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html#objective)与[锁定版本损失实现](https://github.com/microsoft/LightGBM/blob/v4.6.0/src/objective/regression_objective.hpp)。',
      '- 每条训练特征按自己的截止r−2构造，训练标签在实际签发截止前完整成熟。复用并逐列核验原训练矩阵，保存输入引用及哈希，没有复制新矩阵；另保存推断raw、模型及字段顺序。模型保存回读与预测一致，没有因新候选表现删资格、补零或新增例外。',
      '- 五项筛选使用未舍入值，独立验证从封存预测重算，而非重复调用主汇总函数。原归档及首次前瞻112文件哈希保持不变；2026-10-28前瞻批次没有被纳入成熟评分。',
      '- 这是反复研究过的重建历史开发验证，历史业务日期早于截止不等于当时已实际可取得。仍不代表独立OOS、上线准确率或未来因果改善。',
      '- 失败只否决这个固定L1方案，不代表所有目标函数实验无效；本轮不追加损失、权重、特征或新模型。源码及脱敏测试进入独立分支，业务数据、模型和报告留本地。',
      f'- [真实复现命令与验证顺序](<{reproduction}>)；版本、测试记录及哈希见本目录执行回执。核心数值在scoring CSV/parquet保留全精度。','']
    target.write_text('\n'.join(lines),encoding='utf-8');print(target)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--replace-report',action='store_true')
    a=p.parse_args();render(a.out,replace_report=a.replace_report)
