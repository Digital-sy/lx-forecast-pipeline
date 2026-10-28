"""Render a concise Chinese report from verified diagnostic tables only."""
import argparse
import json
from pathlib import Path
import pandas as pd


def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+
                     ['| '+' | '.join(map(str,row))+' |' for row in rows])


def num(x): return f'{x:,.0f}'
def pct(x): return f'{x:.2%}'


def render(out):
    target=out/'核心款模型互补性与剩余误差分析.md'
    if target.exists():raise ValueError('REPORT_EXISTS')
    def csv(name):return pd.read_csv(out/(name+'.csv'),float_precision='round_trip')
    v=json.loads((out/'independent_validation.json').read_text(encoding='utf-8'))
    if v['status']!='PASS':raise ValueError('INDEPENDENT_VALIDATION_REQUIRED')
    ss=csv('scope_summary').set_index('scope');p=ss.loc['PAIRED'];f=ss.loc['FULL_CORE']
    mc=csv('model_coverage');pc=mc[mc.scope.eq('PAIRED')&mc.window.eq('d01_60')].set_index('model')
    directions=csv('all_miss_direction_summary');directions=directions[directions.scope.eq('PAIRED')]
    bands=csv('all_miss_error_band_summary');bands=bands[bands.scope.eq('PAIRED')]
    rescue=csv('rescue_by_model');rescue=rescue[rescue.scope.eq('PAIRED')]
    top=csv('top20_products');st=csv('top20_model_stability');objects=pd.read_parquet(out/'objects.parquet')
    best=[]
    for row in top.itertuples():
        alternatives=st[st.shop.eq(row.shop)&st.spu.eq(row.spu)&st.period.eq('ALL')&st.model.ne('BLEND50')]
        b=alternatives.sort_values(['absolute_error60','model']).iloc[0]
        years=st[st.shop.eq(row.shop)&st.spu.eq(row.spu)&st.model.eq(b.model)].set_index('period')
        best.append(dict(rank=row.rank,shop=row.shop,spu=row.spu,hindsight_fixed_alternative=b.model,
            delta_absolute_error60=b.delta_absolute_error60,lower_ae_issues=int(b.lower_ae_issues),
            higher_ae_issues=int(b.higher_ae_issues),tied_ae_issues=int(b.tied_ae_issues),
            delta_2025=years.loc['2025','delta_absolute_error60'],delta_2026=years.loc['2026','delta_absolute_error60'],
            alternative_joint_hit_n=int(b.joint_hit_n),rescued_joint_hits=int(b.rescue_issues),lost_joint_hits=int(b.lost_joint_hits)))
    best=pd.DataFrame(best)
    best.to_csv(out/'top20_fixed_alternative_summary.csv',index=False,encoding='utf-8-sig')
    compact=top.merge(best,on=['rank','shop','spu'],validate='one_to_one')
    zh=compact[['rank','shop','spu','absolute_error60','issues','joint_miss_n','longest_joint_miss_month_run',
                'largest_issue_error_share','top3_issue_error_share','rescue_issues','hindsight_fixed_alternative',
                'delta_absolute_error60','lower_ae_issues','higher_ae_issues','tied_ae_issues','delta_2025','delta_2026']].rename(columns={
        'rank':'排名','shop':'店铺','spu':'SPU','absolute_error60':'BLEND50累计60天绝对误差',
        'issues':'观察签发期数','joint_miss_n':'未联合命中期数','longest_joint_miss_month_run':'最长连续未命中月份',
        'largest_issue_error_share':'最大单期误差占比','top3_issue_error_share':'前三期误差占比',
        'rescue_issues':'其他模型可完整命中期数','hindsight_fixed_alternative':'事后累计误差最小的固定备选模型',
        'delta_absolute_error60':'备选减BLEND50绝对误差','lower_ae_issues':'备选误差更小期数',
        'higher_ae_issues':'备选误差更大期数','tied_ae_issues':'备选误差相同期数',
        'delta_2025':'2025年备选减BLEND50误差','delta_2026':'2026年备选减BLEND50误差'})
    zh.to_csv(out/'BLEND50剩余误差Top20.csv',index=False,encoding='utf-8-sig')
    objects[objects.blend_rescued].to_csv(out/'其他模型可完整命中的去重明细.csv',index=False,encoding='utf-8-sig')
    obs60=directions[directions.window.eq('d01_60')].set_index('direction')
    same=obs60.loc[['ALL_OVER','ALL_UNDER'],'records'].sum()
    positive=obs60.loc[obs60.index!='ZERO_ACTUAL','records'].sum()
    both_years=int((best.delta_2025.lt(0)&best.delta_2026.lt(0)).sum())
    reversals=int(((best.delta_2025.lt(0)&best.delta_2026.gt(0)) |
                   (best.delta_2025.gt(0)&best.delta_2026.lt(0))).sum())
    concentrated=top[top.top3_issue_error_share.gt(.5)]
    no_degrade=int(best.higher_ae_issues.eq(0).sum())
    ae_total=pc.loc['BLEND50','ae']
    lines=[
        '# 核心款模型互补性与剩余误差分析',
        '',
        '**结论：下一项优先研究“补充现有模型共同缺少的信息”，模型选择作为次级研究。** '
        f'现有模型确有互补：BLEND50未命中的{num(p.blend_non_hit_including_missing_n)}条中，另有模型完整联合命中'
        f'{num(p.rescue_union_n)}条（{pct(p.rescue_union_n/p.blend_non_hit_including_missing_n)}），承载实销{num(p.rescue_union_actual60)}件。'
        f'但事后选择的上限仍只有{num(p.any_model_hit_n)}/{num(p.records)}（{pct(p.any_model_hit_n/p.records)}），'
        f'命中实销占比{pct(p.any_model_hit_actual60/p.actual60)}；仍有{num(p.all_six_output_no_joint_hit_n)}条没有任何模型联合命中。',
        '',
        '这里的“上限”仅针对现有六份整套预测及本历史样本，不能部署，也不代表未来准确率。'
        '共同偏差提示现有信息或建模表达的共同不足，不能仅据本分析确定库存、活动、趋势转折等具体因果；本轮未新增这些来源。',
        '',
        '## 范围与可比性',
        '',
        f'- 2025-01至2026-08，共20个签发期。完整范围{num(f.records)}条对象×签发期、{num(f.products)}个不同店铺×SPU；'
        f'主配对{num(p.records)}条、{num(p.products)}个不同店铺×SPU。',
        '- 模型：F28、DIRECT、CONTEXT90、YEARAGO、GATE、BLEND50。联合命中要求同一模型前30天、后30天、累计60天全部在±20%以内；不跨模型拼段。',
        '- 实销承载取每条对象×签发期的60天实际销量一次；跨期窗口可能重叠，不是去重销量或采购损失。',
        '- 原241条缺输出保留；正实销才计算相对误差。明确零销量单列绝对误差，不当作百分比命中；当前成熟范围未知真值为0。',
        '- 历史来源和点时限制沿用原验收；本轮是重建历史开发诊断，不是独立OOS。未应用HTML低于10件隐藏规则。',
        '',
        '## 1. 去重后的互补空间',
        '',
        table(['范围','BLEND50联合命中','其他模型额外联合命中','额外命中实销','任一模型联合命中','联合命中实销占比'],[
            ['2,579条主配对',num(p.blend_hit_n),num(p.rescue_union_n),num(p.rescue_union_actual60),f'{num(p.any_model_hit_n)} / {pct(p.any_model_hit_n/p.records)}',pct(p.any_model_hit_actual60/p.actual60)],
            ['2,820条完整范围',num(f.blend_hit_n),num(f.rescue_union_n),num(f.rescue_union_actual60),f'{num(f.any_model_hit_n)} / {pct(f.any_model_hit_n/f.records)}',pct(f.any_model_hit_actual60/f.actual60)]]),
        '',
        f'主配对额外命中的实销占BLEND50原未命中实销{pct(p.rescue_union_actual60/p.blend_non_hit_including_missing_actual60)}。'
        f'完整范围多出的{num(f.rescue_blend_missing_n)}条、{num(f.rescue_blend_missing_actual60)}件来自BLEND50缺输出而F28命中的对象。'
        f'另{num(f.incomplete_model_output_n)}条、{num(f.incomplete_model_output_actual60)}件属于缺模型输出且F28未命中，不能称六模型都已验证失败。',
        '',
        table(['备选模型','能补中条数（有重叠）','承载实销（有重叠）','仅该模型能补中条数'],[
            [r.model,num(r.rescue_n),num(r.rescue_actual60),num(r.exclusive_rescue_n)] for r in rescue.itertuples()]),
        '',
        '以上分模型行不能相加；联合总数按对象×签发期去重。具体交集见rescue_overlap_combinations.csv。',
        '',
        '## 2. 所有模型未联合命中时，偏差是什么方向',
        '',
        f'以下固定分析{num(p.all_six_output_no_joint_hit_n)}条六模型都有输出、但无模型联合命中的记录。'
        f'其中{num(p.all_six_evaluated_joint_miss_n)}条三个窗口均为正实销；另{num(p.zero_actual_undefined_relative_error_n)}条至少一个窗口实销为0，不适用三窗口相对误差命中。',
        '',
        table(['窗口','共同高估','共同低估','模型方向相反','该窗口实销为0'],[
            [label]+[num(directions.set_index(['window','direction']).loc[(w,d),'records']) for d in ['ALL_OVER','ALL_UNDER','OPPOSITE','ZERO_ACTUAL']]
            for w,label in [('d01_30','前30天'),('d31_60','后30天'),('d01_60','累计60天')]]),
        '',
        f'累计60天正实销记录中，{num(same)}/{num(positive)}（{pct(same/positive)}）是六模型同向偏差。'
        f'共同高估记录更多，但共同低估承载{num(obs60.loc["ALL_UNDER","actual60"])}件实销，'
        f'高于共同高估的{num(obs60.loc["ALL_OVER","actual60"])}件。不能因此统一抬高或压低预测。',
        '',
        table(['窗口','所有模型高估超过20%','所有模型低估超过20%','存在两侧均越20%的模型','至少一个模型在±20%且非前述类别'],[
            [label]+[num(bands.set_index(['window','error_band']).loc[(w,d),'records']) for d in ['ALL_ABOVE_20','ALL_BELOW_MINUS20','OPPOSITE_OUTSIDE_20','AT_LEAST_ONE_WITHIN_20']]
            for w,label in [('d01_30','前30天'),('d31_60','后30天'),('d01_60','累计60天')]]),
        '',
        '某单窗口有模型命中，不代表同一模型同时命中其他窗口。原值方向表与±20%严重偏差表是两个口径，均保留零实销类别。',
        '',
        '## 3. BLEND50剩余绝对误差Top20',
        '',
        f'按店铺×SPU汇总60天绝对误差排序，包含联合命中记录的小误差，不把30/30/60三列相加。'
        f'Top20合计绝对误差{num(top.absolute_error60.sum())}件评价权重，占BLEND50全部误差{pct(top.absolute_error60.sum()/ae_total)}。'
        f'共{num(top.issues.sum())}条可评分记录、{num(top.joint_miss_n.sum())}条未联合命中；其中其他模型可完整补中{num(top.rescue_issues.sum())}条。',
        '',
        table(['序','店铺','SPU','60天绝对误差','未联合命中/观察期','最大单期误差占比','前三期误差占比','其他模型能补中期数'],[
            [r.rank,r.shop,r.spu,num(r.absolute_error60),f'{r.joint_miss_n}/{r.issues}',pct(r.largest_issue_error_share),pct(r.top3_issue_error_share),r.rescue_issues]
            for r in top.itertuples()]),
        '',
        f'这些商品不是只在一次签发中出错：每款未联合命中{int(top.joint_miss_n.min())}—{int(top.joint_miss_n.max())}期。'
        f'最大单期仅占各款误差的{pct(top.largest_issue_error_share.min())}—{pct(top.largest_issue_error_share.max())}。'
        f'{"、".join(concentrated.shop+"/"+concentrated.spu)}的前三期占比过半，存在集中放大，但其余期也持续失准。'
        '逐期日期、方向、六模型预测及实际值见top20_issue_detail.csv。',
        '',
        '### 其他模型能否稳定提供不同信息',
        '',
        f'对每款事后选取累计误差最小的单一备选模型，只作诊断：{both_years}/20款在2025及2026两个期间都减少绝对误差，'
        f'但{no_degrade}/20款能做到每个签发期都不退化；{int(best.delta_absolute_error60.gt(0).sum())}/20款连该最佳固定备选的总误差也大于BLEND50。',
        '',
        table(['商品','备选（事后）','误差更小/更大/相同期数','累计误差变化','2025变化','2026变化'],[
            [r.shop+' / '+r.spu,r.hindsight_fixed_alternative,f'{r.lower_ae_issues}/{r.higher_ae_issues}/{r.tied_ae_issues}',num(r.delta_absolute_error60),num(r.delta_2025),num(r.delta_2026)]
            for r in best.itertuples() if r.rank <= 5]),
        '',
        f'负数表示备选误差更小。以上按误差排名展示前5款，不挑选有利案例。完整20款中，{reversals}款的最佳固定备选在2025与2026的误差增减方向反转。'
        '减少累计误差也不等于获得三窗口联合命中，CSV同时保留两项指标。'
        '因此存在可研究的差异信息，但当前证据不支持按商品固定选择历史赢家。完整20款及全部备选见CSV。',
        '',
        '## 下一项研究与执行回执',
        '',
        '优先研究导致共同偏差的信息缺口或共同建模限制，先明确哪些签发前资料确实可取得，再另行定义候选；本报告不认定具体经营原因。'
        '“签发前模型选择”可作为次级方向：F28及CONTEXT90提供部分独有命中，但须用签发前证据、时间隔离验证，不能把本次事后最佳选择当训练标签后报告可部署成绩。',
        '',
        f'- 数值验收PASS：独立路径复核{num(v["source_forecast_windows_checked"])}条模型窗口、{num(v["independent_truth_windows"])}个已独立日真值窗口、全部对象联合命中、方向及Top20。',
        f'- 首次前瞻批次仅作文件哈希保护，{v["protected_shadow_files_unchanged"]}个文件前后未变；未读入当前分析，未使用未来实销。',
        '- 发现CONTEXT90旧报告中的DIRECT/F28副本使用默认CSV浮点解析；现已逐值证明其来源，且命中无变化。本轮沿用往返精度的原DIRECT/F28，仅取CONTEXT90自身输出。详见float_copy_audit.json。',
        '- 新训练0次、新推理0次、新预测0条、数据库/API查询0次、生产变更0项；未修改历史输入、模型、HTML或前瞻封存。',
        '- 本地业务CSV不进入Git；代码提交与复现命令见执行回执。未自动开始后续研究。',
        ''
    ]
    target.write_text('\n'.join(lines),encoding='utf-8')
    print(str(target))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();render(Path(a.output))
