"""One Chinese report from sealed fixed-mean scores and independent validation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

C = 'BLEND50_L1_MEAN_V1'
B = 'BLEND50'
L = 'DIRECT_L1_V1'
MAIN = [B, L, C]
MODELS = ['DIRECT', B, L, C, 'F28']
LABEL = {C: '固定均值组合', B: 'BLEND50', L: 'L1', 'DIRECT': 'DIRECT', 'F28': 'F28'}
WINDOWS = [('d01_30', '前30天'), ('d31_60', '后30天'), ('d01_60', '累计60天')]
GROUP = {'BOTH_HIT': '两端均命中', 'BLEND_ONLY': '仅BLEND50命中',
         'L1_ONLY': '仅L1命中', 'NEITHER': '两端均未命中', 'MISSING': '缺输出'}
AGE = {'4_5': '4—5个月', '6_11': '6—11个月', '12_plus': '12个月以上', '0_3': '0—3个月'}
GATES = [('wape60_lower', '60天WAPE严格下降'),
         ('joint_hits_increased', '联合命中记录数增加'),
         ('joint_hit_sales_share_not_lower', '联合命中实销占比不下降'),
         ('decline_over_rate_not_increased', '原下降组高估率不增加'),
         ('output_and_core_coverage_not_reduced', '输出及核心覆盖不减少')]


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def n(value):
    return f'{value:,.0f}' if pd.notna(value) else '—'


def pct(value):
    return f'{value:.2%}' if pd.notna(value) else '—'


def change(value):
    return f'{value:+,.2f}' if pd.notna(value) else '—'


def table(headers, rows):
    def cell(v):
        return str(v).replace('|', '／').replace('\n', ' ')
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join(['---'] * len(headers)) + ' |'] +
                     ['| ' + ' | '.join(cell(v) for v in row) + ' |' for row in rows])


def artifact(path, label=None):
    path = Path(path).resolve()
    return f'[{label or path.name}](<{path.as_posix()}>)'


def render(out, replace_report=False):
    out = Path(out).resolve()
    target = out / 'BLEND50_L1_MEAN_V1固定等权组合实际效果报告.md'
    if target.exists() and not replace_report:
        raise ValueError('REPORT_EXISTS')
    validation = read(out / 'validation/validation.json')
    if validation.get('status') != 'PASS':
        raise ValueError('INDEPENDENT_VALIDATION_REQUIRED')
    test_receipt = read(out / 'test_receipt.json')
    if test_receipt.get('status') != 'PASS':
        raise ValueError('TEST_RECEIPT_REQUIRED')
    seal = read(out / 'candidate_manifest.json')
    summary = read(out / 'scoring/summary.json')
    if summary['candidate'] != C:
        raise ValueError('WRONG_CANDIDATE')
    for key in ['formal_fits', 'inference_calls', 'database_queries', 'api_calls', 'production_changes']:
        if summary.get(key) != 0:
            raise ValueError('FIXED_MEAN_EXECUTION_BUDGET_CHANGED:' + key)
    score = out / 'scoring'
    metrics = pd.read_parquet(score / 'metrics.parquet')
    joints = pd.read_parquet(score / 'joint_summary.parquet')
    details = pd.read_parquet(score / 'details.parquet')
    endpoints = pd.read_parquet(score / 'endpoint_groups.parquet')
    migrations = pd.read_parquet(score / 'joint_migrations.parquet')
    top = pd.read_parquet(score / 'original_top20.parquet').sort_values('rank')
    products = pd.read_parquet(score / 'product_contributions.parquet')
    directions = pd.read_parquet(score / 'old_direction_metrics.parquet')
    cancellation = pd.read_parquet(score / 'cancellation_summary.parquet')
    cancel_changes = pd.read_parquet(score / 'cancellation_changes.parquet')

    pm = metrics[metrics.scope.eq('PAIRED') & metrics.slice.eq('overall')].set_index(['model', 'window'])
    full_candidate = metrics[metrics.scope.eq('FULL_CORE') & metrics.slice.eq('overall') & metrics.model.eq(C)].set_index('window')
    pj = joints[joints.scope.eq('PAIRED') & joints.slice.eq('overall')].set_index('model')
    fj = joints[joints.scope.eq('FULL_CORE') & joints.slice.eq('overall')].set_index('model')
    periods = metrics[metrics.scope.eq('PAIRED') & metrics.slice.eq('issue') & metrics.window.eq('d01_60')]
    period_wape = periods.pivot(index='issue_date', columns='model', values='wape')
    period_joint = joints[joints.scope.eq('PAIRED') & joints.slice.eq('issue')].pivot(index='issue_date', columns='model', values='joint_hit_n')
    decline = metrics[metrics.scope.eq('PAIRED') & metrics.slice.eq('decline_group') &
                      metrics.decline_group.eq('PRE_ISSUE_DECLINING') & metrics.window.eq('d01_60')].set_index('model')
    endpoint_pair = endpoints[endpoints.scope.eq('PAIRED')].set_index('endpoint_group')
    full = int(summary['full_core_n'])
    paired = int(summary['paired_n'])
    missing = int(summary['missing_output_n'])
    base = pm.loc[(B, 'd01_60')]
    new = pm.loc[(C, 'd01_60')]
    group_actual = details[details.paired].groupby('endpoint_group').apply(
        lambda g: float(g.loc[~g[C + '_joint_hit'], 'actual_d01_60'].sum()), include_groups=False)
    top_under_change = float((top[C + '_under60'] - top[B + '_under60']).sum())
    top_under_worse = int(top[C + '_under60'].gt(top[B + '_under60']).sum())
    top_pred_lower = int(top[C + '_prediction60'].lt(top[B + '_prediction60']).sum())
    concentration = []
    for baseline in [B, L]:
        p = products[products.baseline.eq(baseline)]
        improved = p[p.ae_reduction.gt(0)].sort_values('ae_reduction', ascending=False)
        gross = float(improved.ae_reduction.sum())
        concentration.append([LABEL[baseline], int(p.ae_reduction.gt(0).sum()),
                              int(p.ae_reduction.lt(0).sum()), int(p.ae_reduction.eq(0).sum()),
                              n(p.ae_reduction.sum()),
                              pct(float(improved.head(5).ae_reduction.sum()) / gross) if gross > 0 else '—',
                              n(p.ae_reduction.sum() - improved.head(5).ae_reduction.sum())])
    common_low = directions[directions.group_kind.eq('direction') & directions.old_direction.eq('ALL_UNDER')]
    common_low_index = common_low.set_index(['model', 'window'])
    common_low60 = common_low[common_low.window.eq('d01_60')].set_index('model')
    cs = cancellation[cancellation.scope.eq('PAIRED')].set_index('model')
    cc = cancel_changes[cancel_changes.paired].groupby('baseline')[
        ['segment_ae_improvement', 'cancellation_gain', 'cumulative_ae_improvement']].sum()
    gate_count = sum(bool(v) for v in summary['gates'][B].values())
    gate_result = '通过' if summary['business_screen'][B] == 'PASS' else '未通过'
    wins = int(period_wape[C].lt(period_wape[B]).sum())
    losses = int(period_wape[C].gt(period_wape[B]).sum())
    equal = int(period_wape[C].eq(period_wape[B]).sum())
    ep_text = []
    for group in ['BOTH_HIT', 'BLEND_ONLY', 'L1_ONLY', 'NEITHER']:
        r = endpoint_pair.loc[group]
        verb = '保留' if group != 'NEITHER' else '新增'
        ep_text.append(f'{GROUP[group]}{n(r.records)}条中{verb}{n(r.candidate_hit_n)}条，承载{n(r.candidate_hit_actual)}件')
    created = seal.get('sealed_at', seal.get('completed_at', seal.get('at', '未提供')))
    lines = [
        '# BLEND50_L1_MEAN_V1固定等权组合实际效果报告', '',
        f'**技术PASS；主研究筛选{summary["business_screen"][B]}（相对BLEND50，{gate_count}/5）；业务证据PARTIAL。** 本轮是观察过端点结果后的历史开发验证，不是独立OOS或前瞻成功。', '',
        f'1. **相对BLEND50的五项门槛：{gate_result}。** 60天WAPE由{pct(base.wape)}变为{pct(new.wape)}；联合命中{n(pj.loc[B,"joint_hit_n"])}→{n(pj.loc[C,"joint_hit_n"])}条，承载实销{n(pj.loc[B,"joint_hit_actual"])}→{n(pj.loc[C,"joint_hit_actual"])}件。各项按未舍入值独立判断。',
        '2. **分段和累计误差：** ' + '；'.join(f'{label}绝对误差变化{change(pm.loc[(C,w),"ae"]-pm.loc[(B,w),"ae"])}件、WAPE {pct(pm.loc[(B,w),"wape"])}→{pct(pm.loc[(C,w),"wape"])}' for w, label in WINDOWS) + '。三个窗口不可相加为总损失。',
        '3. **两端四组的保留、新增与实销：** ' + '；'.join(ep_text) + '。退出及未命中实销见下表，不以净增掩盖替换。',
        f'4. **大款低估风险：** 原固定Top20中，{top_under_worse}/{len(top)}款低估件数增加、{top_pred_lower}/{len(top)}款预测总量降低；Top20低估合计变化{change(top_under_change)}件。全配对低估变化{change(new.under_units-base.under_units)}件；原共同低估组低估变化{change(common_low60.loc[C,"under_units"]-common_low60.loc[B,"under_units"])}件。风险另列，不事后增设第六项门槛。',
        f'5. **时期分布与证据：** 20期60天WAPE相对BLEND50改善{wins}期、退化{losses}期、不变{equal}期；联合命中增加{int(period_joint[C].gt(period_joint[B]).sum())}期、减少{int(period_joint[C].lt(period_joint[B]).sum())}期。仅能支持本次重建历史开发结论，原首次影子批次保持封存。', '',
        f'本轮候选实际封存时间：{created}。新拟合0次、新模型推理0次、数据库/API查询0次、生产变更0项；只对已有未舍入的两段最终预测作固定0.5/0.5算术组合。', '',
        '## 固定范围与三个窗口', '',
        f'20个成熟签发期为2025-01至2026-08；完整{full:,}条店铺×SPU×签发期记录，原同对象配对{paired:,}条，缺输出{missing:,}条。它们不是同样数量的不同商品。原日期、真值、资格和842条下降组不变；2026-09/10未成熟，不缩短窗口。', '',
        table(['完整核心窗口', '固定范围', '有输出', '有真值', '可评分', '缺输出', '未知真值', '明确零实销'],
              [[label] + [n(full_candidate.loc[w, col]) for col in
                          ['fixed_n','output_n','truth_n','scored_n','missing_output_n','unknown_truth_n','zero_truth_n']]
               for w,label in WINDOWS]), '',
        f'另有{n(validation["not_matured_n"])}条2026-09/10未成熟对象期，继续保留在未成熟归档；它们与上述{missing}条成熟范围缺输出不是同一类。', '',
        table(['门槛', '相对BLEND50（主）', '相对DIRECT（参考）', '相对L1（参考）'],
              [[label] + ['通过' if summary['gates'][model][key] else '未通过' for model in [B, 'DIRECT', L]] for key, label in GATES]), '',
        table(['窗口', '模型', 'WAPE', 'Bias', '高估件数', '低估件数', '±20%命中', '命中实销', '命中实销占比'],
              [[label, LABEL[model], pct(pm.loc[(model,w),'wape']), pct(pm.loc[(model,w),'bias']),
                n(pm.loc[(model,w),'over_units']), n(pm.loc[(model,w),'under_units']),
                n(pm.loc[(model,w),'hit_n']), n(pm.loc[(model,w),'hit_actual']), pct(pm.loc[(model,w),'hit_actual_share'])]
               for w,label in WINDOWS for model in MAIN]), '',
        table(['模型', '配对联合命中', '配对联合命中率', '命中实销', '实销占比', f'完整{full}联合命中率'],
              [[LABEL[model], n(pj.loc[model,'joint_hit_n']), pct(pj.loc[model,'joint_hit_fixed_share']),
                n(pj.loc[model,'joint_hit_actual']), pct(pj.loc[model,'joint_hit_actual_share']),
                pct(fj.loc[model,'joint_hit_fixed_share'])] for model in MODELS]), '',
        table(['参考模型', '窗口', 'WAPE', 'Bias', '±20%命中', '命中实销'],
              [[LABEL[model], label, pct(pm.loc[(model,w),'wape']), pct(pm.loc[(model,w),'bias']),
                n(pm.loc[(model,w),'hit_n']), n(pm.loc[(model,w),'hit_actual'])]
               for model in ['DIRECT','F28'] for w,label in WINDOWS]), '',
        '组合相对BLEND50各窗口的命中实销变化：' + '；'.join(
            f'{label}{change(pm.loc[(C,w),"hit_actual"]-pm.loc[(B,w),"hit_actual"])}件'
            for w,label in WINDOWS) + '。即使联合命中增加，也逐窗披露承载实销的取舍。', '',
        f'工作汇报口径：在{paired:,}条有输出的历史预测记录中，{pct(pm.loc[(C,"d01_30"),"hit_n"]/paired)}的未来30天累计销量误差在±20%内，{pct(pm.loc[(C,"d01_60"),"hit_n"]/paired)}的未来60天累计误差在±20%内，{pct(pj.loc[C,"joint_hit_n"]/paired)}同时满足两个分段及累计要求；完整{full:,}条范围联合命中率为{pct(fj.loc[C,"joint_hit_fixed_share"])}，仍有{missing:,}条缺输出。', '',
        '命中按正实销、未舍入相对误差≤20%判定，联合命中每对象期只计一次。真实0实销只评价绝对误差；未知和缺预测没有补0。跨期实销仅为评价承载权重，不是去重销量或采购损失。', '',
        '## 两端命中迁移与分段抵消', '',
        table(['原端点分组', '记录数', '组合命中', '未命中/退出', '组合命中实销', '未命中实销'],
              [[GROUP[group], n(endpoint_pair.loc[group,'records']), n(endpoint_pair.loc[group,'candidate_hit_n']),
                n(endpoint_pair.loc[group,'records']-endpoint_pair.loc[group,'candidate_hit_n']),
                n(endpoint_pair.loc[group,'candidate_hit_actual']), n(group_actual.get(group,0))]
               for group in ['BOTH_HIT','BLEND_ONLY','L1_ONLY','NEITHER']]), '',
        '两端共同命中必须全部保留；仅一端命中的组展示保住与失去，双方均未命中也可能因均值进入±20%。端点命中集合的并集不是组合命中的上界。完整范围缺输出组另列在endpoint_groups.csv，未从固定名单剔除。', '',
        table(['对照', '迁移', '条数', '实销承载'],
              [[LABEL[r.baseline], r.migration, n(r.records), n(r.actual60)]
               for r in migrations[migrations.scope.eq('PAIRED') & migrations.slice.eq('overall') & migrations.baseline.isin([B,L])].itertuples()]), '',
        '`e1=P1−Y1，e2=P2−Y2；分段绝对误差=|e1|+|e2|；累计绝对误差=|e1+e2|；抵消量=分段绝对误差−累计绝对误差。`', '',
        table(['模型', 'e1合计', 'e2合计', '分段AE', '累计AE', '抵消量', '两段反向条数', '累计命中但分段未全中'],
              [[LABEL[model], n(cs.loc[model,'e1']), n(cs.loc[model,'e2']), n(cs.loc[model,'segment_ae']),
                n(cs.loc[model,'cumulative_ae']), n(cs.loc[model,'cancellation']), n(cs.loc[model,'opposite_segment_signs_n']),
                n(cs.loc[model,'cumulative_hit_but_segment_miss_n'])] for model in MAIN]), '',
        table(['对照', '分段AE改善', '抵消增量', '累计AE改善'],
              [[LABEL[model], change(cc.loc[model,'segment_ae_improvement']), change(cc.loc[model,'cancellation_gain']),
                change(cc.loc[model,'cumulative_ae_improvement'])] for model in [B,L]]), '',
        ('相对BLEND50，两个分段合计AE减少，同时抵消量减少；因此累计AE改善并非依赖更多的两段相互抵消。'
         if cc.loc[B,'segment_ae_improvement'] > 0 and cc.loc[B,'cancellation_gain'] < 0
         else '相对BLEND50的累计AE变化应按上表拆分，不能只凭累计指标判断两个分段都改善。') +
        f' 相对L1，抵消量变化{change(cc.loc[L,"cancellation_gain"])}件；两个比较基准不能混为一个原因。', '',
        '上表满足“累计AE改善=分段AE改善+抵消增量”。抵消是分段误差的数学性质；不能把累计改善全部说成两个分段均改善，也不能把抵消本身称为作弊。分段AE和累计AE不可重复相加。固定均值的凸性校验只是实现约束，不是新的独立业务证据。', '',
        '## 低估、下降组与原Top20', '',
        table(['窗口', '原共同低估组模型', '记录', '低估', '低估较BLEND变化', 'WAPE', '±20%命中', '命中数变化', '命中实销', '命中实销变化'],
              [[label, LABEL[model], n(common_low_index.loc[(model,w),'fixed_n']),
                n(common_low_index.loc[(model,w),'under_units']),
                change(common_low_index.loc[(model,w),'under_units']-common_low_index.loc[(B,w),'under_units']),
                pct(common_low_index.loc[(model,w),'wape']), n(common_low_index.loc[(model,w),'hit_n']),
                change(common_low_index.loc[(model,w),'hit_n']-common_low_index.loc[(B,w),'hit_n']),
                n(common_low_index.loc[(model,w),'hit_actual']),
                change(common_low_index.loc[(model,w),'hit_actual']-common_low_index.loc[(B,w),'hit_actual'])]
               for w,label in WINDOWS for model in MAIN]), '',
        table(['模型', '下降组记录', '60天高估率', '高估', '低估', 'WAPE'],
              [[LABEL[model], n(decline.loc[model,'fixed_n']), pct(decline.loc[model,'over_rate']),
                n(decline.loc[model,'over_units']), n(decline.loc[model,'under_units']), pct(decline.loc[model,'wape'])] for model in MAIN]), '',
        table(['原排名', '店铺/SPU', '期数', 'BLEND50预测', '组合预测', 'BLEND50低估', '组合低估', 'BLEND50 AE', '组合AE', '预测降低期数', 'AE改善/退化期数'],
              [[int(r['rank']), r['shop']+'/'+r['spu'], n(r['records']), n(r[B+'_prediction60']), n(r[C+'_prediction60']),
                n(r[B+'_under60']), n(r[C+'_under60']), n(r[B+'_ae60']), n(r[C+'_ae60']),
                n(r['prediction_lower_periods_vs_'+B]), f'{n(r["periods_improved_vs_"+B])}/{n(r["periods_worse_vs_"+B])}']
               for r in top.to_dict('records')]), '',
        'Top20沿用原名单与排序，未删除大款或重排挑选有利案例。各款L1端点、两个分段以及逐期明细保存在original_top20.csv和original_top20_issue_detail.csv。原共同误差方向由旧六模型定义，只作事后诊断，不进入组合规则。', '',
        '## 20期、店铺和货龄', '',
        table(['签发日', 'BLEND50 WAPE', 'L1 WAPE', '组合WAPE', 'BLEND50联合命中', '组合联合命中'],
              [[issue, pct(row[B]), pct(row[L]), pct(row[C]), n(period_joint.loc[issue,B]), n(period_joint.loc[issue,C])]
               for issue,row in period_wape.iterrows()]), '',
        table(['店铺', '模型', '60天WAPE', '高估', '低估', '预测总量'],
              [[r.shop, LABEL[r.model], pct(r.wape), n(r.over_units), n(r.under_units), n(r.prediction_units)]
               for r in metrics[metrics.scope.eq('PAIRED') & metrics.slice.eq('shop') & metrics.window.eq('d01_60') & metrics.model.isin(MAIN)].itertuples()]), '',
        table(['原货龄', '模型', '记录', '60天WAPE', '高估', '低估'],
              [[AGE.get(r.maturity,r.maturity), LABEL[r.model], n(r.fixed_n), pct(r.wape), n(r.over_units), n(r.under_units)]
               for r in metrics[metrics.scope.eq('PAIRED') & metrics.slice.eq('maturity') & metrics.window.eq('d01_60') & metrics.model.isin(MAIN)].itertuples()]), '',
        table(['对照', '改善商品', '退化商品', '不变商品', 'AE净减少', '前5受益款占毛改善', '扣除前5受益款后AE净减少'], concentration), '',
        '商品改善/退化按同店铺×SPU跨期累计AE判断，未据此改变模型或名单；毛改善是只累计受益商品的AE减少，不等于全量净改善。排除前5的数字只显示收益集中度，不用来重新宣布门槛通过。', '',
        '分期改善不构成独立样本证明；这些历史窗口曾用于研究，60天目标也存在重叠。当前描述性品类仅用于分层CSV，未加入规则。', '',
        '## 辅助池、实现与验收边界', '',
        '**非核心/退出核心全池组合风险：NOT_EVALUATED。** 在限定四个归档目录的manifest、文件名及Parquet元数据中，BLEND50 candidate_predictions.parquet只有原2,820条核心记录；14,283条辅助池只找到DIRECT和L1预测，没有同池BLEND50封存预测。本轮未新增推理，未用DIRECT冒充BLEND50，因此没有生成组合的非核心风险成绩。主范围的下降组风险已单独评价，不能由此宣布非核心安全。', '',
        '- 本轮只组合已经保存、已经非负后处理的两个端点；不重新拟合、不新增推理，不再次截断或舍入，不重算年度GATE、不按商品或分段改权重。缺输出状态原样保留。',
        f'- 实际合成与独立校验测试{test_receipt["tests_run"]}项PASS，失败{test_receipt["failures"]}、错误{test_receipt["errors"]}、跳过{test_receipt["skipped"]}；执行时间{test_receipt["started_at"]}至{test_receipt["completed_at"]}，真实测试日志及测试源码哈希见'+artifact(out/'test_receipt.json')+'。不是沿用旧模型测试数量。',
        '- 新候选预测及哈希先封存，再读取评分真值；该执行顺序不把已看过的研究结果变成独立实验。输入、原预测、原模型和首次前瞻112个文件保持只读；独立验证回执核验其哈希。',
        '- 技术PASS仅表示公式、键、日期、同范围真值、两段相加、凸性、共同命中保留及独立指标复算通过。主研究筛选只比较相对BLEND50的五项未舍入门槛，低估风险没有被新增为事后第六项。',
        '- 业务证据始终为PARTIAL：重建历史开发验证，尚非独立OOS；即使五项通过，也不能宣称全店核心款未来两个月逐款±20%达标。L1、TRAFFIC_STATE、ITEM_ID等旧实验结论未因本次组合被改写。',
        '- 首次BLEND50前瞻批次未重签；生产V4、NEW_VISIBLE、采购、数据库、BI与调度不变。完成固定0.5/0.5这一档后停止，不追加其他权重、路由或候选。', '',
        '主要证据：' + '；'.join(artifact(score/name) for name in [
            'metrics.csv','joint_summary.csv','endpoint_groups.csv','joint_migrations.csv',
            'cancellation_summary.csv','cancellation_changes.csv','old_direction_metrics.csv',
            'original_top20.csv','original_top20_issue_detail.csv','product_contributions.csv']) + '。', '',
        '执行与复现：' + artifact(out/'candidate_manifest.json') + '；' + artifact(out/'validation/validation.json') +
        '；' + artifact(Path(__file__).resolve().parents[1]/'docs/blend50_l1_mean_v1_protocol.md') + '。版本、真实提交和测试记录由本轮统一执行回执记录，业务明细只留本地。', ''
    ]
    target.write_text('\n'.join(lines), encoding='utf-8')
    print(target)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', required=True)
    parser.add_argument('--replace-report', action='store_true')
    args = parser.parse_args()
    render(args.out, replace_report=args.replace_report)
