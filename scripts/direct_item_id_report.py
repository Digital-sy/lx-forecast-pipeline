"""Render the verified single-candidate result; no fitting or model selection."""
import argparse
import json
from pathlib import Path
import pandas as pd

C='DIRECT_ITEM_ID_V1'
MODELS=['F28','DIRECT','BLEND50','DIRECT_TRAFFIC_STATE_V1',C]
LABEL={C:'ITEM_ID','DIRECT_TRAFFIC_STATE_V1':'TRAFFIC_STATE','DIRECT':'DIRECT','BLEND50':'BLEND50','F28':'F28'}
W=[('d01_30','前30天'),('d31_60','后30天'),('d01_60','累计60天')]
GROUP={'ALL_SIX_ORIGINAL_NO_JOINT_HIT':'原六模型共同失准','BLEND50_ORIGINAL_HIT':'原BLEND50已命中',
       'OTHER_MODELS_ORIGINAL_RESCUE':'原其他模型可补中','MISSING_OR_UNDETERMINED':'原缺输出或不可判断'}
SEEN={'SEEN_BOTH':'两段均见过','SEEN_ONE_SEGMENT':'仅一段见过','UNSEEN_BOTH':'两段均未见过'}
AGE={'4_5':'4—5个月','6_11':'6—11个月','12_plus':'12个月以上','0_3':'0—3个月'}
def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def n(x):return f'{x:,.0f}' if pd.notna(x) else '—'
def p(x):return f'{x:.2%}' if pd.notna(x) else '—'
def table(headers,rows):return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+['| '+' | '.join(map(str,r))+' |' for r in rows])


def render(out):
    out=Path(out);v=read(out/'validation/validation.json')
    if v['status']!='PASS':raise ValueError('VALIDATION_REQUIRED')
    target=out/'DIRECT_ITEM_ID_V1显式商品身份增量实验实际效果报告.md'
    if target.exists():raise ValueError('REPORT_EXISTS')
    s=read(out/'scoring/summary.json');m=pd.read_parquet(out/'scoring/metrics.parquet');j=pd.read_parquet(out/'scoring/joint_summary.parquet')
    d=pd.read_parquet(out/'scoring/details.parquet');support=pd.read_parquet(out/'identity_support.parquet')
    recon=pd.read_csv(out/'training_reconciliation.csv');top=pd.read_parquet(out/'scoring/original_top20.parquet')
    directions=pd.read_parquet(out/'scoring/old_direction_metrics.parquet')
    pm=m[m.scope.eq('PAIRED')&m.slice.eq('overall')].set_index(['model','window'])
    pj=j[j.scope.eq('PAIRED')&j.slice.eq('overall')].set_index('model');fj=j[j.scope.eq('FULL_CORE')&j.slice.eq('overall')].set_index('model')
    period=m[m.scope.eq('PAIRED')&m.slice.eq('issue')&m.window.eq('d01_60')].pivot(index='issue_date',columns='model',values='wape')
    decline=m[m.scope.eq('PAIRED')&m.slice.eq('decline_group')&m.decline_group.eq('PRE_ISSUE_DECLINING')&m.window.eq('d01_60')].set_index('model')
    seen=m[m.scope.eq('PAIRED')&m.slice.eq('seen')&m.window.eq('d01_60')].set_index(['model','seen_group'])
    age=m[m.scope.eq('PAIRED')&m.slice.eq('maturity')&m.window.eq('d01_60')].set_index(['model','maturity'])
    band=m[m.scope.eq('PAIRED')&m.slice.eq('support')&m.window.eq('d01_60')].set_index(['model','support_band'])
    candidate=pm.loc[(C,'d01_60')];baseline=pm.loc[('DIRECT','d01_60')];blend=pm.loc[('BLEND50','d01_60')]
    gains=d[d.paired&~d.BLEND50_joint_hit&d[C+'_joint_hit']];loss=d[d.paired&d.BLEND50_joint_hit&~d[C+'_joint_hit']]
    common=d[d.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT')];hit=common[common[C+'_joint_hit']]
    held=d[d.BLEND50_joint_hit&d[C+'_joint_hit']]
    catmeta=[read(q) for q in sorted((out/'effects').glob('*/categorical_metadata.json'))]
    catsplits=sum(z['item_categorical_splits'] for z in catmeta);allsplits=sum(sum(x['count'] for x in z['split_counts']) for z in catmeta)
    groups=[]
    for group,g in d.groupby('old_group'):
        for bm in ['DIRECT','BLEND50']:
            a=g[bm+'_joint_hit'];b=g[C+'_joint_hit']
            groups.append(dict(group=group,baseline=bm,records=len(g),candidate_hits=int(b.sum()),hit_actual60=float(g.loc[b,'actual_d01_60'].sum()),
                gained=int((~a&b).sum()),gained_actual60=float(g.loc[~a&b,'actual_d01_60'].sum()),
                lost=int((a&~b).sum()),lost_actual60=float(g.loc[a&~b,'actual_d01_60'].sum()),retained=int((a&b).sum())))
    groups=pd.DataFrame(groups);groups.to_csv(out/'原固定分组联合命中变化.csv',index=False,encoding='utf-8-sig')
    common.to_csv(out/'原1953条共同失准对象明细.csv',index=False,encoding='utf-8-sig')
    d[d.paired].to_csv(out/'主配对逐款预测与迁移.csv',index=False,encoding='utf-8-sig')
    top.to_csv(out/'原Top20商品及跨期稳定性.csv',index=False,encoding='utf-8-sig')
    rows=[]
    for r in support.itertuples():
        rows.append(dict(issue_date=r.issue_date,segment=r.segment,shop=r.shop,spu=r.spu,seen=r.seen_in_training,
            item_key=r.item_key,input_status=r.input_status,training_dates=r.training_dates,first=r.training_first,last=r.training_last,
            span_days=r.training_span_days,nonoverlap_windows=r.nonoverlap_windows,original_positive_months=r.positive_completed_months))
    pd.DataFrame(rows).to_csv(out/'逐期逐款类别身份与训练支持.csv',index=False,encoding='utf-8-sig')
    longworse=sum(band.loc[(C,b),'wape']>band.loc[('DIRECT',b),'wape'] for b in ['0','1-3','4-11','12+'])
    lines=['# DIRECT_ITEM_ID_V1显式商品身份增量实验实际效果报告','',
        '**技术验收PASS；对DIRECT与BLEND50的五项研究筛选均未通过。属于重建历史开发验证，不是独立OOS或前瞻成功。**','',
        f'1. **是否优于DIRECT：否。** 60天WAPE {p(baseline.wape)}→{p(candidate.wape)}，联合命中{n(pj.loc["DIRECT","joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}；五项仅{sum(s["gates"]["DIRECT"].values())}/5通过。',
        f'2. **是否优于BLEND50：否。** 60天WAPE {p(blend.wape)}→{p(candidate.wape)}，联合命中{n(pj.loc["BLEND50","joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}；五项仅{sum(s["gates"]["BLEND50"].values())}/5通过。',
        f'3. **共同失准是否改善：局部有，整体退化。** 原{n(len(common))}条中新增联合命中{n(len(hit))}条、承载{n(hit.actual_d01_60.sum())}件实销；原BLEND50的284条仅保留{n(len(held))}条，退出{n(len(loss))}条、承载{n(loss.actual_d01_60.sum())}件。',
        f'4. **收益是否只在历史很长老款：未形成这类总体收益。** 预登记四个训练支持层中有{longworse}/4层60天WAPE高于DIRECT；至少12个不重叠目标窗口组为{p(band.loc[("DIRECT","12+"),"wape"])}→{p(band.loc[(C,"12+"),"wape"])}。',
        f'5. **记忆与泛化风险：有显著未见商品低估和分期退化。** 两段均未见过的{n(seen.loc[(C,"UNSEEN_BOTH"),"fixed_n"])}条WAPE {p(seen.loc[("DIRECT","UNSEEN_BOTH"),"wape"])}→{p(seen.loc[(C,"UNSEEN_BOTH"),"wape"])}，Bias={p(seen.loc[(C,"UNSEEN_BOTH"),"bias"])}；仅{int(period[C].lt(period.DIRECT).sum())}/20期优于DIRECT。类别分裂证明模型使用身份，不能单凭它断言过拟合或已找到根因。','',
        '## 五项固定筛选','',
        table(['指标','相对DIRECT','相对BLEND50'],[[label,'通过' if s['gates']['DIRECT'][key] else '未通过','通过' if s['gates']['BLEND50'][key] else '未通过'] for key,label in [
            ('wape60_lower','60天WAPE下降'),('joint_hits_increased','联合命中记录数增加'),('joint_hit_sales_share_not_lower','联合命中实销占比不下降'),
            ('decline_over_rate_not_increased','原下降组高估率不增加'),('output_and_core_coverage_not_reduced','输出与核心覆盖不减少')]]),'',
        f'原下降观察组842条，实际销量分母不变：DIRECT高估率{p(decline.loc["DIRECT","over_rate"])}，BLEND50 {p(decline.loc["BLEND50","over_rate"])}，ITEM_ID {p(decline.loc[C,"over_rate"])}。采用未舍入值判断。','',
        '## 三窗口与完整范围','',
        '主表固定2,579条店铺×SPU×签发期记录，20个成熟签发期。它们不是2,579个不同商品。±20%对正实销逐条判断，命中比例以固定范围为分母。','',
        table(['窗口','模型','WAPE','Bias','高估','低估','±20%命中/2579'],[[label,LABEL[model],p(pm.loc[(model,w),'wape']),p(pm.loc[(model,w),'bias']),
            n(pm.loc[(model,w),'over_units']),n(pm.loc[(model,w),'under_units']),int(pm.loc[(model,w),'hit_n'])] for w,label in W for model in MODELS]),'',
        table(['模型','配对联合命中','配对命中率','命中实销承载','配对实销占比','完整2820联合命中/占比'],[[LABEL[model],n(pj.loc[model,'joint_hit_n']),
            p(pj.loc[model,'joint_hit_fixed_share']),n(pj.loc[model,'joint_hit_actual']),p(pj.loc[model,'joint_hit_actual_share']),
            f'{n(fj.loc[model,"joint_hit_n"])} / {p(fj.loc[model,"joint_hit_fixed_share"])}'] for model in MODELS]),'',
        f'完整2,820条中，新候选输出{int(fj.loc[C,"output_complete_n"])}条，原241条缺输出保留；F28完整范围有输出2,820条，故其完整范围与主配对成绩分开。'
        'WAPE只对有预测且真值明确的记录计算，未输出不是零误差。完整范围逐窗口结果见scoring/metrics.csv。',
        '累计60天预测逐条等于前后两段之和，不把M1输出喂给M2。跨期实销是评估权重，重叠窗口不是去重销量或采购损失。','',
        table(['窗口','配对明确零实销','零实销绝对误差','完整范围未知实销','缺输出'],[[label,int(pm.loc[(C,w),'zero_truth_n']),n(pm.loc[(C,w),'zero_truth_ae']),
            int(m[m.scope.eq('FULL_CORE')&m.slice.eq('overall')&m.model.eq(C)&m.window.eq(w)].iloc[0].unknown_truth_n),241] for w,label in W]),'',
        f'相对DIRECT，60天绝对误差变化{n(candidate.ae-baseline.ae)}件，高估变化{n(candidate.over_units-baseline.over_units)}件，低估变化{n(candidate.under_units-baseline.under_units)}件。'
        f'相对BLEND50，分别变化{n(candidate.ae-blend.ae)}、{n(candidate.over_units-blend.over_units)}、{n(candidate.under_units-blend.under_units)}件。正数表示增加。','',
        '## 原命中迁移与共同失准','',
        f'相对BLEND50，新进入{len(gains)}条/承载{n(gains.actual_d01_60.sum())}件，退出{len(loss)}条/承载{n(loss.actual_d01_60.sum())}件；净变化{len(gains)-len(loss)}条、{n(gains.actual_d01_60.sum()-loss.actual_d01_60.sum())}件。','',
        table(['原固定组','记录数','ITEM_ID联合命中','新增/承载实销','退出/承载实销'],[[GROUP[r.group],r.records,r.candidate_hits,f'{r.gained} / {n(r.gained_actual60)}',
            f'{r.lost} / {n(r.lost_actual60)}'] for r in groups[groups.baseline.eq('BLEND50')].itertuples()]),'',
        '上述分组由原六模型定义，未加入本候选重新划组。共同失准1,953条包括至少一个窗口实销为0的46条。其他模型可补中355条中含13条原DIRECT缺输出记录；完整名单均保留。','',
        table(['原60天方向','模型','WAPE','高估','低估'],[[r.old_direction,LABEL[r.model],p(r.wape),n(r.over_units),n(r.under_units)]
            for r in directions[directions.group_kind.eq('direction')&directions.window.eq('d01_60')&directions.model.isin(['DIRECT','BLEND50',C])].itertuples()]),'',
        '前后两个30天方向组在scoring/old_direction_metrics.csv保留。身份信息改变了不同商品的预测，但局部新增命中不足以抵销退出和低估增加；不据此设计商品例外。','',
        '## 已见/未见与训练历史支持','',
        table(['身份支持','配对条数','DIRECT WAPE','BLEND50 WAPE','ITEM_ID WAPE','ITEM_ID Bias','ITEM_ID低估'],[[SEEN[g],int(seen.loc[(C,g),'fixed_n']),
            p(seen.loc[('DIRECT',g),'wape']),p(seen.loc[('BLEND50',g),'wape']),p(seen.loc[(C,g),'wape']),p(seen.loc[(C,g),'bias']),n(seen.loc[(C,g),'under_units'])]
            for g in ['SEEN_BOTH','SEEN_ONE_SEGMENT','UNSEEN_BOTH']]),'',
        '未见商品未被删除、未借用其他款编码。负类别编码按原库缺失类别机制处理；这个机制能够执行，不等于预测质量可靠。',
        '训练支持按M1/M2中较少的不重叠目标窗口数分层，分层在效果计算前登记，未参与预测路由：','',
        table(['不重叠目标窗口数','条数','DIRECT WAPE','BLEND50 WAPE','ITEM_ID WAPE'],[[g,int(band.loc[(C,g),'fixed_n']),p(band.loc[('DIRECT',g),'wape']),
            p(band.loc[('BLEND50',g),'wape']),p(band.loc[(C,g),'wape'])] for g in ['0','1-3','4-11','12+']]),'',
        table(['原货龄','条数','DIRECT WAPE','BLEND50 WAPE','ITEM_ID WAPE'],[[AGE[g],int(age.loc[(C,g),'fixed_n']),p(age.loc[('DIRECT',g),'wape']),
            p(age.loc[('BLEND50',g),'wape']),p(age.loc[(C,g),'wape'])] for g in ['4_5','6_11','12_plus']]),'',
        '同款训练日期数、首次/末次日期、跨度、不重叠窗口数及原货龄逐条保留在“逐期逐款类别身份与训练支持.csv”。重叠日标签数量不等于独立季节样本数。',
        f'40个模型合计使用item_key类别分裂{n(catsplits)}次，占全部分裂的{p(catsplits/allsplits)}。这是模型结构描述，不能作为因果归因或独立泛化证明。','',
        '## 分期、店铺与原Top20','',
        f'新候选60天WAPE低于DIRECT的签发期{int(period[C].lt(period.DIRECT).sum())}/20，低于BLEND50 {int(period[C].lt(period.BLEND50).sum())}/20。','',
        table(['签发日','DIRECT WAPE','BLEND50 WAPE','ITEM_ID WAPE','M1/M2合格未见数'],[[issue,p(r.DIRECT),p(r.BLEND50),p(r[C]),
            '/'.join(str(int(recon[recon.issue_date.eq(issue)&recon.segment.eq(seg)].iloc[0].eligible_unseen_n)) for seg in ['M1','M2'])] for issue,r in period.iterrows()]),'',
        table(['年度','模型','WAPE','Bias','高估','低估'],[[r.year,LABEL[r.model],p(r.wape),p(r.bias),n(r.over_units),n(r.under_units)]
            for r in m[m.scope.eq('PAIRED')&m.slice.eq('year')&m.window.eq('d01_60')&m.model.isin(['DIRECT','BLEND50',C])].itertuples()]),'',
        table(['店铺','模型','60天WAPE','联合命中'],[[r.shop,LABEL[r.model],p(r.wape),int(j[j.scope.eq('PAIRED')&j.slice.eq('shop')&j.shop.eq(r.shop)&j.model.eq(r.model)].iloc[0].joint_hit_n)]
            for r in m[m.scope.eq('PAIRED')&m.slice.eq('shop')&m.window.eq('d01_60')&m.model.isin(['DIRECT','BLEND50',C])].itertuples()]),'',
        'Top20名单沿用原报告，不按本轮成绩重排。下表“改善/退化期数”依据逐期绝对误差与BLEND50比较：','',
        table(['原排名','店铺','SPU','期数','BLEND50绝对误差','ITEM_ID绝对误差','改善/退化期数','最大退化期'],[[r['rank'],r['shop'],r['spu'],r['records'],
            n(r['BLEND50_ae60']),n(r[C+'_ae60']),f'{r["periods_improved_vs_BLEND50"]}/{r["periods_worse_vs_BLEND50"]}',r['largest_ae_increase_issue_vs_BLEND50']]
            for r in top.sort_values('rank').to_dict('records')]),'',
        '逐期预测和迁移见scoring/original_top20_issue_detail.csv；未设置商品系数或只训练这些款。','',
        '## 方法、限制及实际验收','',
        '- 冻结DIRECT原10列仅为店铺编码、销量滞后/均值和日历；shop/SPU虽在训练框架中，但原fit明确只投影10列。本轮只增加item_key，不使用流量、广告、价格、库存、品类或季节。',
        '- 每个签发期每个分段的字典仅来自实际训练集合，按shop、SPU字符串元组排序后赋连续int32；不按销量或误差排序，不从未来对象全集构造。未知编码−1，零是有效已知类别。',
        '- 模型显式categorical_feature=[item_key]；其余10列保持数值解释。实际模型参数：min_data_per_group=100、max_cat_threshold=32、cat_l2=10、cat_smooth=10、max_cat_to_onehot=4，均沿用4.6.0默认值，未搜索。',
        '- 原资格、训练键、标签、隐式等权、Poisson损失、种子、超参数和非负后处理均未改变。前后段独立拟合，训练标签按各签发截止成熟，历史特征按各训练行自己的截止构造。',
        '- 原始归档于2026年补取，不证明历史时点已取得；历史映射与经营属性有效性限制继承。全量核心中缺预测、未知和明确零值分别保留，2026年9/10月未成熟状态未修改。',
        f'- 实际40次正式拟合，0次旧模型重训，0次合成模型拟合。12项合成测试通过；独立复核{n(v["old_training_rows_reconciled"])}条旧训练行及{n(v["dictionary_encoding_rows"])}条身份编码，{n(v["saved_model_output_checks"])}条保存模型回读预测逐值一致。',
        f'- 在保存模型上完成{n(v["native_unknown_category_checks"])}组负编码与NaN原生未知类别等价验证，未增加拟合。独立重算{n(v["metric_rows_checked"])}行指标、{n(v["joint_metric_rows_checked"])}行联合指标及迁移，五项结论一致。',
        '- 首次前瞻112个文件与登记旧来源哈希未变。本轮业务数据库/API查询0次、生产变更0项；未修改首次影子预测、NEW_VISIBLE、采购、BI或调度。',
        '- 类别机制依据：[LightGBM 4.6.0类别与未知值文档](https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html)、[参数](https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html)、[fit接口](https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html)。',
        '- 技术PASS表示实现满足本次约束；研究筛选FAIL表示这个固定方案未取得要求的收益，不推导所有身份建模方式都无效。完成后停止，未追加编码、参数或混合权重。',
        '- 源码、依赖、输入及模型哈希、真实Git commit与复现命令见执行回执。业务明细和模型只留本地。','']
    target.write_text('\n'.join(lines),encoding='utf-8');print(str(target))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--out',required=True);a=parser.parse_args();render(a.out)
