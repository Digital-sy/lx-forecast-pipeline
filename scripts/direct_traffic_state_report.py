"""Render verified numerical results; no model execution or new scoring rules."""
import argparse
import json
from pathlib import Path
import pandas as pd

C='DIRECT_TRAFFIC_STATE_V1'
LABELS={C:'TRAFFIC_STATE','DIRECT':'DIRECT','BLEND50':'BLEND50','F28':'F28'}
W=[('d01_30','前30天'),('d31_60','后30天'),('d01_60','累计60天')]
def num(x):return f'{x:,.0f}'
def pct(x):return f'{x:.2%}'
def table(headers,rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+
        ['| '+' | '.join(map(str,r))+' |' for r in rows])


def render(out):
    out=Path(out);v=json.loads((out/'validation/validation.json').read_text(encoding='utf-8'))
    if v['status']!='PASS':raise ValueError('VALIDATION_REQUIRED')
    target=out/'DIRECT_TRAFFIC_STATE_V1签发前流量状态增量实验实际效果报告.md'
    if target.exists():raise ValueError('REPORT_EXISTS')
    s=json.loads((out/'scoring/summary.json').read_text(encoding='utf-8'))
    m=pd.read_parquet(out/'scoring/metrics.parquet');j=pd.read_parquet(out/'scoring/joint_summary.parquet')
    d=pd.read_parquet(out/'scoring/details.parquet');directions=pd.read_parquet(out/'scoring/old_direction_metrics.parquet')
    ledger=json.loads((out/'formal_fit_ledger.json').read_text(encoding='utf-8'))
    support=pd.read_csv(out/'training_reconciliation.csv');f=pd.read_csv(out/'feature_coverage.csv')
    p=m[m.scope.eq('PAIRED')&m.slice.eq('overall')].set_index(['model','window'])
    pj=j[j.scope.eq('PAIRED')&j.slice.eq('overall')].set_index('model')
    fj=j[j.scope.eq('FULL_CORE')&j.slice.eq('overall')].set_index('model')
    common=d[d.old_group.eq('ALL_SIX_ORIGINAL_NO_JOINT_HIT')]
    recovered=common[common[C+'_joint_hit']]
    den=d[d.paired].actual_d01_60.sum();oldloss=d[(d.paired)&d.BLEND50_joint_hit&~d[C+'_joint_hit']]
    newgain=d[(d.paired)&~d.BLEND50_joint_hit&d[C+'_joint_hit']]
    gd=m[m.scope.eq('PAIRED')&m.slice.eq('decline_group')&m.window.eq('d01_60')&m.decline_group.eq('PRE_ISSUE_DECLINING')].set_index('model')
    field=f[f.scope.eq('ELIGIBLE_INFERENCE')&f.segment.eq('M1')].groupby('feature').agg(rows=('rows','sum'),finite=('finite_n','sum'),unknown=('unknown_n','sum'),zero=('zero_n','sum'))
    periods=m[m.scope.eq('PAIRED')&m.slice.eq('issue')&m.window.eq('d01_60')].pivot(index='issue_date',columns='model',values='wape')
    years=m[m.scope.eq('PAIRED')&m.slice.eq('year')&m.window.eq('d01_60')]
    group_rows=[]
    for group,g in d.groupby('old_group'):
        for base in ['DIRECT','BLEND50']:
            gained=g[~g[base+'_joint_hit']&g[C+'_joint_hit']];lost=g[g[base+'_joint_hit']&~g[C+'_joint_hit']]
            retained=g[g[base+'_joint_hit']&g[C+'_joint_hit']]
            group_rows.append(dict(group=group,baseline=base,records=len(g),candidate_hits=int(g[C+'_joint_hit'].sum()),
                candidate_hit_actual60=float(g.loc[g[C+'_joint_hit'],'actual_d01_60'].sum()),
                gained=len(gained),gained_actual60=float(gained.actual_d01_60.sum()),lost=len(lost),lost_actual60=float(lost.actual_d01_60.sum()),
                retained=len(retained),retained_actual60=float(retained.actual_d01_60.sum())))
    groups=pd.DataFrame(group_rows);groups.to_csv(out/'原固定分组联合命中变化.csv',index=False,encoding='utf-8-sig')
    common.to_csv(out/'原1953条共同失准对象明细.csv',index=False,encoding='utf-8-sig')
    pd.read_parquet(out/'scoring/original_top20.parquet').to_csv(out/'原Top20流量增量效果.csv',index=False,encoding='utf-8-sig')
    sources=json.loads((out/'source_applicability.json').read_text(encoding='utf-8'))
    a=p.loc[(C,'d01_60')]
    lines=['# DIRECT_TRAFFIC_STATE_V1签发前流量状态增量实验实际效果报告','',
        '**本轮是重建历史开发验证；技术验收与业务筛选分开。没有改变首次前瞻批次或生产模型。**','',
        f'1. **相对DIRECT：五项筛选{sum(s["gates"]["DIRECT"].values())}/5通过，整体{s["business_screen"]["DIRECT"]}。** '
        f'60天WAPE由{pct(p.loc[("DIRECT","d01_60"),"wape"])}变为{pct(a.wape)}；联合命中由{num(pj.loc["DIRECT","joint_hit_n"])}变为{num(pj.loc[C,"joint_hit_n"])}。',
        f'2. **相对BLEND50：五项筛选{sum(s["gates"]["BLEND50"].values())}/5通过，整体{s["business_screen"]["BLEND50"]}。** 各项逐一见下表，不能以超过DIRECT替代这一对照。',
        f'3. **原{num(len(common))}条共同失准：新增联合命中{num(len(recovered))}条，承载实销{num(recovered.actual_d01_60.sum())}件。** '
        f'同时原BLEND50已命中对象退出{num(len(oldloss))}条、承载{num(oldloss.actual_d01_60.sum())}件；不能只报新增。',
        f'4. **60天高低估：** 相对DIRECT，高估变化{num(a.over_units-p.loc[("DIRECT","d01_60"),"over_units"])}件、低估变化{num(a.under_units-p.loc[("DIRECT","d01_60"),"under_units"])}件；'
        f'相对BLEND50，分别变化{num(a.over_units-p.loc[("BLEND50","d01_60"),"over_units"])}、{num(a.under_units-p.loc[("BLEND50","d01_60"),"under_units"])}件。正数为恶化，负数为减少。',
        f'5. **信息支持：** {int(support.variable_numeric_n.gt(0).sum())}/{len(support)}个模型单元有变化的新增数值信息；'
        f'实际正式拟合{s["formal_fits"]}次。来源标记：{s["candidate_origin_counts"]}。未知特征保留NaN，原缺输出未补造。','',
        '## 五项固定筛选（未舍入判断）','',
        table(['筛选项','相对DIRECT','相对BLEND50'],[[label,'通过' if s['gates']['DIRECT'][key] else '未通过','通过' if s['gates']['BLEND50'][key] else '未通过'] for key,label in [
            ('wape60_lower','60天WAPE严格下降'),('joint_hits_increased','三窗口联合命中数增加'),
            ('joint_hit_sales_share_not_lower','联合命中实销占比不下降'),('decline_over_rate_not_increased','原下降组高估率不增加'),
            ('output_and_core_coverage_not_reduced','输出及核心覆盖不减少')]]),'',
        f'原下降观察组固定842条。高估率DIRECT={pct(gd.loc["DIRECT","over_rate"])}、BLEND50={pct(gd.loc["BLEND50","over_rate"])}、'
        f'TRAFFIC_STATE={pct(gd.loc[C,"over_rate"])}；分母为原组相同实际销量合计，未重定义保护组。','',
        '## 三个窗口实际成绩','',
        '以下为原2,579条配对范围；±20%以正实销为分母逐条判断，命中率以固定记录数为分母。零实销保留绝对误差，不造百分比命中。','',
        table(['窗口','模型','WAPE','Bias','高估件数','低估件数','±20%命中条数/固定范围'],[
            [label,LABELS[model],pct(p.loc[(model,w),'wape']),pct(p.loc[(model,w),'bias']),num(p.loc[(model,w),'over_units']),num(p.loc[(model,w),'under_units']),
             f'{num(p.loc[(model,w),"hit_n"])} / {int(p.loc[(model,w),"fixed_n"])}'] for w,label in W for model in ['F28','DIRECT','BLEND50',C]]),'',
        table(['模型','联合命中条数','配对联合命中率','命中实销承载','配对命中实销占比','完整2,820条联合命中率'],[
            [LABELS[model],num(pj.loc[model,'joint_hit_n']),pct(pj.loc[model,'joint_hit_fixed_share']),num(pj.loc[model,'joint_hit_actual']),
             pct(pj.loc[model,'joint_hit_actual_share']),pct(fj.loc[model,'joint_hit_fixed_share'])] for model in ['F28','DIRECT','BLEND50',C]]),'',
        f'新候选完整输出{int(fj.loc[C,"output_complete_n"])}/2,820；原241条缺输出继续留在完整名单。'
        'F28可覆盖原缺输出对象；其完整范围成绩另列scoring/metrics.csv，主配对比较没有改变对象集合。'
        '跨期实销是评价权重，窗口可能重叠，不是去重销量或采购损失。','',
        table(['新候选窗口','完整核心','有输出/可评分','缺输出','明确零实销（配对）','零实销绝对误差'],[
            [label,2820,int(p.loc[(C,w),'output_n']),2820-int(p.loc[(C,w),'output_n']),
             int(p.loc[(C,w),'zero_truth_n']),num(p.loc[(C,w),'zero_truth_ae'])] for w,label in W]),'',
        '完整范围的WAPE仍仅对有输出且真值明确的记录计算，241条无输出不是零误差。'
        '未知实销为0条；原2026年9/10月未成熟记录不进入本轮20期评价，原状态未修改。','',
        '## 是否触及原共同弱点','',
        table(['原分组','记录数','新候选联合命中','新增命中/实销','退出命中/实销'],[
            [r.group,r.records,r.candidate_hits,f'{r.gained} / {num(r.gained_actual60)}',f'{r.lost} / {num(r.lost_actual60)}']
            for r in groups[groups.baseline.eq('BLEND50')].itertuples()]),'',
        '分组完全沿用旧六模型结果；“其他模型原可补中”含13条原DIRECT无输出、F28命中的记录。共同失准1,953条含至少一个窗口零实销的46条，未偷偷排除。','',
        table(['原60天方向','模型','WAPE','高估','低估','该窗口命中数'],[
            [r.old_direction,LABELS[r.model],pct(r.wape) if pd.notna(r.wape) else '不适用',num(r.over_units),num(r.under_units),num(r.hit_n)]
            for r in directions[directions.group_kind.eq('direction')&directions.window.eq('d01_60')&directions.model.isin(['DIRECT','BLEND50',C])].itertuples()]),'',
        '前30天和后30天方向拆解、原Top20所有商品及逐期明细一并保留。旧分组只用于事后诊断，未进入训练；不能从相关性断言广告、库存或季节因果。','',
        '## 分期与新增信息覆盖','',
        f'20个签发期中，新候选60天WAPE低于DIRECT的期数为{int(periods[C].lt(periods.DIRECT).sum())}，低于BLEND50为{int(periods[C].lt(periods.BLEND50).sum())}；不据此挑好月份重算总体。','',
        table(['签发日','DIRECT 60天WAPE','BLEND50 60天WAPE','TRAFFIC_STATE 60天WAPE','新增信息支持单元'],[
            [issue,pct(r.DIRECT),pct(r.BLEND50),pct(r[C]),
             int(support[support.issue_date.eq(issue)].variable_numeric_n.gt(0).sum())]
            for issue,r in periods.iterrows()]),'',
        table(['期间','模型','WAPE','高估','低估'],[[r.year,LABELS[r.model],pct(r.wape),num(r.over_units),num(r.under_units)] for r in years.itertuples()]),'',
        table(['新增字段','合格对象期数（M1去重）','有限值','未知','明确0'],[[name,int(r.rows),int(r.finite),int(r.unknown),int(r.zero)] for name,r in field.iterrows()]),'',
        'M2使用同一签发截止、同一组历史状态，但独立训练其标签；不读取M1输出。各期每个字段的训练/推断覆盖及来源标记见feature_coverage.csv和formal_fit_ledger.json。','',
        '## 来源、特征与限制','',
        '- DIRECT原有10个特征仅为店铺编码、销量滞后/均值和目标起始日历；原值、顺序、训练键、标签、权重及配置未变。本轮只追加6个数值字段及各自有效性标记。',
        '- T7/T28为最近7/28个连续日历日Sessions合计除以天数；R7/R28为同对象同期销量件数/Sessions。两项变化值分别使用(T7−T28)/(T7+T28)、(R7−R28)/(R7+R28)。',
        '- 原B2的session_mean28与T28概念相关，但order_session_ratio28使用订单量，不是本轮销量件数口径；B2为原日级递推流程，本轮没有重跑它。',
        '- Sessions沿用ODS按日期×店铺×SPU的既有求和，不是去重访客；没有ASIN/MSKU展开或多对多JOIN。R是单位会话销量代理，不是购买人数转化率，合法值不限制≤1。',
        '- 缺日、映射无效、NULL/负数或必要标记异常时保持未知；Sessions=0时R未定义，变化分母为0也未定义；未补0或epsilon，未删除原训练行。',
        f'- 日归档{num(sources["rows"])}行，{sources["first_day"]}—{sources["last_day"]}；本轮只读。Sessions NULL分量计数合计{sources["null_components"]}，未知计数字段{sources["unknown_null_count"]}行，聚合负值{sources["sessions_negative_aggregate"]}行。',
        '- 聚合归档没有Sessions原始负分量计数，不能从非负总数证明每个分量非负；历史SPU映射生效日期亦未恢复。这些限制保留，不扩大数据库核查。',
        f'- 2026段原获取时间{sources["reused_v1_acquisition_min"]}至{sources["reused_v1_acquisition_max"]}；2024–2025段于2026-10-20补取。源同步成功时间未知，业务日期早于截点不证明当时已经取得。',
        '- 仅为历史开发回放，不是严格历史点时、独立OOS或已上线准确率；本轮结果不得直接替换BLEND50或生产V4。','',
        '## 技术验收与交付边界','',
        f'- 技术验收PASS：16项特征合成测试；{num(v["old_training_rows_reconciled"])}条原训练行逐字段哈希一致；'
        f'{num(v["independent_calendar_rows"])}行独立日历核验；保存模型回读预测核验{num(v["saved_model_output_checks"])}条；独立指标{num(v["metric_rows_checked"])}行。',
        '- 未改变2,820完整名单、2,579主配对、842下降组、1,953共同失准组。本轮使用的原历史来源及首次前瞻112个文件哈希未变。',
        '- 最多40次正式拟合，实际次数和跳过/引用均见账本；未新增数据库/API查询或生产变更，未签发新前瞻预测。',
        '- 使用原LightGBM 4.6.0缺失机制，未升级依赖。官方依据：[缺失值处理](https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html)、[LGBMRegressor接口](https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html)。',
        '- 代码提交、文件哈希及复现命令见执行回执。业务数据和模型留本地；完成唯一候选后停止，不追加字段、种子或权重。','']
    target.write_text('\n'.join(lines),encoding='utf-8');print(str(target))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();render(a.out)
