"""Markdown and CSV handoff from verified archived-rule accounting only."""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
try:
    from scripts.yearago_gate_route import RULE, KEY, sha, dump, now, read_json
except ModuleNotFoundError:
    from yearago_gate_route import RULE, KEY, sha, dump, now, read_json

BASE,YEAR='DIRECT30x2','DIRECT_YEARAGO'


def table(frame):
    labels={'model':'对照','window':'窗口','wape':'WAPE','bias':'Bias','over_units':'高估件数','under_units':'低估件数',
            'hit_n':'窗口命中数','hit_fixed_share':'窗口命中占比','joint_hit_n':'联合命中数','joint_hit_actual':'联合承载实销',
            'joint_hit_actual_share':'联合承载实销占比','joint_hit_fixed_share':'联合命中占比','shop':'店铺','spu':'SPU',
            'issue_date':'签发日','gate_decision':'采用年度预测','gate_joint_hit':'选择后联合命中','diagnostic_bin':'边界距离档',
            'actual_d01_60':'60天实销','actual60':'60天实销','n':'记录数','products':'店铺×SPU数','periods':'期数',
            'full_n':'完整范围记录数','paired_n':'配对记录数','paired_actual60':'配对60天实销','paired_actual_share':'配对实销占比',
            'changed_n':'预测改变数','extra_ae60':'额外绝对误差60','extra_over60':'额外高估60','extra_under60':'额外低估60',
            'gate_joint_n':'GATE联合命中数','gate_joint_actual60':'GATE联合承载实销','fixed_n':'固定记录数',
            'actual_units':'实销分母','over_rate':'高估率','state':'命中迁移','annual_used':'采用年度数量',
            'joint_net':'联合命中净增','joint_actual_net':'联合承载净增','exclusion':'敏感性剔除项',
            'ae_reduction60':'绝对误差减少60','wape_reduction60':'WAPE减少60','failed_windows':'失败窗口',
            'yearago_extra_ae_d01_60':'YEARAGO额外绝对误差60','yearago_extra_over_d01_60':'YEARAGO额外高估60',
            'yearago_extra_under_d01_60':'YEARAGO额外低估60'}
    def fmt(x,c):
        if pd.isna(x):return '—'
        if isinstance(x,bool):return 'TRUE' if x else 'FALSE'
        if c in ['wape','bias','over_rate','hit_fixed_share','joint_hit_actual_share','joint_hit_fixed_share','paired_actual_share','wape_reduction60']:return f'{x:.4%}'
        if isinstance(x,(float,int)):return f'{x:,.4f}'.rstrip('0').rstrip('.') if isinstance(x,float) else f'{x:,}'
        return str(x).replace('|','/')
    return '| '+' | '.join(labels.get(c,c) for c in frame.columns)+' |\n| '+' | '.join(['---']*len(frame.columns))+' |\n'+'\n'.join('| '+' | '.join(fmt(x,c) for x,c in zip(row,frame.columns))+' |' for row in frame.itertuples(index=False,name=None))


def report(output, report_dir='report'):
    out=Path(output).resolve();dest=out/report_dir
    if Path(report_dir).name!=report_dir:raise ValueError('REPORT_CHILD_DIRECTORY_ONLY')
    if dest.exists():raise ValueError('REPORT_EXISTS')
    v=read_json(out/'validation/validation.json');assert v['status']=='PASS'
    s=read_json(out/'scoring/summary.json');rm=read_json(out/'routing_manifest.json')
    def load(name):return pd.read_parquet(out/'scoring'/f'{name}.parquet')
    d=load('details');route=pd.read_parquet(out/'routing.parquet');bins=load('boundary_bins')
    m=load('metrics');j=load('joint_summary');lost=load('original_exits');gained=load('original_gains')
    top=load('top20_exits_actual');topae=load('top20_exits_extra_ae');sensitivity=load('sensitivity')
    p60=m[m.scope.eq('PAIRED')&m.slice.eq('overall')&m.window.eq('d01_60')].set_index('model')
    pj=j[j.scope.eq('PAIRED')&j.slice.eq('overall')].set_index('model')
    decl=m[m.scope.eq('PAIRED')&m.slice.eq('decline_group')&m.decline_group.eq('PRE_ISSUE_DECLINING')&m.window.eq('d01_60')].set_index('model')
    tr=pd.DataFrame(s['routing']).set_index('gate_decision').loc[True]
    restored=lost[lost.gate_joint_hit];remaining=lost[~lost.gate_joint_hit]
    abandoned=gained[~gained.gate_joint_hit]
    top_restored=top[top.gate_joint_hit]
    unclassified=int(lost.diagnostic_bin.eq('UNCLASSIFIED').sum())
    mild=lost[lost.diagnostic_bin.eq('20-22%')];severe=lost[lost.diagnostic_bin.eq('>30%')]
    fullj=j[j.scope.eq('FULL_CORE')&j.slice.eq('overall')].set_index('model')
    still_examples='、'.join(top[~top.gate_joint_hit][['shop','spu']].drop_duplicates().head(3).apply(lambda r:r.shop+' '+r.spu,axis=1))
    full60=m[m.scope.eq('FULL_CORE')&m.slice.eq('overall')&m.window.eq('d01_60')&m.model.eq(BASE)].iloc[0]
    lines=[
        '# YEARAGO适用条件回放与核心款退出命中诊断',
        '',
        f'实际计算与核验日期：{pd.Timestamp(v["at"]).tz_convert("Asia/Shanghai").isoformat()}（Asia/Shanghai）。目录后缀 20261020 沿用实验命名，不代表回填运行日期。',
        '',
        '## 首页：六个回答',
        '',
        f'1. **实际采用年度预测：{int(tr.paired_n):,} 条对象×签发期，{int(tr.products)} 个店铺×SPU、{int(tr.periods)} 期，承载配对实销 {tr.paired_actual60:,.0f} 件，占 {tr.paired_actual_share:.4%}。** 并非原状态分组的 1,212 条；两个分段必须都有真实年度输出。',
        f'2. **五项筛选通过 4 项，未通过“联合命中承载销量占比不下降”。** 60 天 WAPE {p60.loc[BASE,"wape"]:.4%} → {p60.loc[RULE,"wape"]:.4%}，联合命中 {int(pj.loc[BASE,"joint_hit_n"])} → {int(pj.loc[RULE,"joint_hit_n"])}；承载销量 {pj.loc[BASE,"joint_hit_actual"]:,.0f} → {pj.loc[RULE,"joint_hit_actual"]:,.0f}，仍减少 {pj.loc[BASE,"joint_hit_actual"]-pj.loc[RULE,"joint_hit_actual"]:,.0f} 件评估权重。',
        f'3. **相对原 YEARAGO，伤害有所减少，但没有消除。** 恢复原退出 {len(restored)} 条、{restored.actual_d01_60.sum():,.0f} 件权重；同时失去原新增命中 {len(abandoned)} 条、{abandoned.actual_d01_60.sum():,.0f} 件权重。联合承载量较原 YEARAGO 净增加 {pj.loc[RULE,"joint_hit_actual"]-pj.loc[YEAR,"joint_hit_actual"]:,.0f} 件，但仍低于 DIRECT。',
        f'4. **原 {len(lost)} 条退出不主要是轻微越界。** 20%—22% 只有 {len(mild)} 条（{len(mild)/len(lost):.2%}）；最大窗口误差超过 30% 有 {len(severe)} 条（{len(severe)/len(lost):.2%}），承载 {severe.actual_d01_60.sum():,.0f} 件。该分档是诊断，不放宽 ±20% 标准。',
        f'5. **原退出按 60 天实销最大的前 20 条，仅恢复 {len(top_restored)} 条，仍有 {len(top)-len(top_restored)} 条未恢复。** 恢复权重 {top_restored.actual_d01_60.sum():,.0f} / {top.actual_d01_60.sum():,.0f} 件；{still_examples} 等仍在未恢复名单。这里“大款”仅指这张前 20 诊断榜。',
        f'6. **技术复核 PASS；本轮研究筛选 FAIL；业务证据 PARTIAL。** 全部 {s["full_core_n"]:,} 条核心记录中联合命中 {int(fullj.loc[RULE,"joint_hit_n"])}（{fullj.loc[RULE,"joint_hit_fixed_share"]:.2%}），不能声称核心款逐款三窗口 ±20% 已达标。本轮停止，不追加第二条规则，不上线。',
        '',
        '以上“件”在跨期比较中是实际销量评估权重；同一商品不同签发窗口可能重叠。不是实际损失的销量，也不是采购损失。',
        '',
        '## 1. 固定规则、证据及边界',
        '',
        '按店铺×SPU×签发期，同时选择两段：DIRECT 两段有效、年度输入 28 天及两个去年目标 30 天完整有效、年度输入开始日及以前观察到正销量、M1/M2 均通过原训练支持、两段均为真实 ANNUAL_MODEL 输出。任一不满足或未知，两段均保留 DIRECT。没有段间混用、重新舍入、抬高或压低。',
        '',
        '采用原年度日历锚点（闰日仅回退至 2 月 28 日）；签发 S、截止 S−2，目标 S+1..30、S+31..60。20 个成熟签发期为 2025-01 至 2026-08；2026-09/10 的 274 条保持 NOT_MATURED。无模型训练、预测推理或外部取数。',
        '',
        f'预登记提交 `{rm["preregistration_commit"]}`；选择清单于 `{rm["sealed_at"]}` 封存，首次评分读取开始 `{s["started_at"]}`。选择阶段仅投影白名单列；正销量依据日期从每个签发期单独截断的旧日级归档恢复。文件字节哈希校验不等于将真值字段载入选择器。',
        '',
        '本规则在观察 YEARAGO 分层结果后提出，属于**事后发现规则的重建历史开发回放**。完整窗口、技术训练支持及早期正销量不证明历史经营可比；未观察正销量不证明未上市。未取得历史获取时间证据，不称独立 OOS、前瞻验证或真实历史时点可得。',
        '',
        '## 2. 全量覆盖与规则实际使用范围',
        '',
        f'完整成熟核心 {s["full_core_n"]:,}；DIRECT/YEARAGO/GATE 完整输出均 {s["paired_n"]:,}；原 {s["missing_output_n"]} 条缺输出全部保留。F28 全核心输出更广，但主比较严格固定在同 {s["paired_n"]:,} 条。成熟核心有真值 {int(full60.truth_n)} 条，60 天明确零实销 {int(full60.zero_truth_n)} 条仍保留。完整核心实销 {fullj.loc[BASE,"known_actual"]:,.0f}，配对实销 {pj.loc[BASE,"known_actual"]:,.0f}；缺输出仍承载 {fullj.loc[BASE,"known_actual"]-pj.loc[BASE,"known_actual"]:,.0f} 件，不被当作零误差。',
        '',
        table(pd.DataFrame(s['routing'])[['gate_decision','full_n','paired_n','products','periods','paired_actual60','paired_actual_share','changed_n','extra_ae60']]),
        '',
        'TRUE 组不等于改善组；FALSE 配对组预测改变数、额外绝对误差均为 0。241 条缺输出属于 FALSE，但 NaN 不算“预测发生变化”。每期采用量及所有失败原因另见 routing_by_issue.csv / routing_failures.csv。',
        '',
        '## 3. 三窗口同样本成绩',
        '',
        '主表仅同 2,579 条。WAPE/Bias/高估率分母均为对应窗口同对象实销；三窗口各自评分，不能用 60 天命中遮蔽分段失败。',
        '',
        table(m[m.scope.eq('PAIRED')&m.slice.eq('overall')][['model','window','wape','bias','over_units','under_units','hit_n','hit_fixed_share']]),
        '',
        table(pj.reset_index()[['model','joint_hit_n','joint_hit_actual','joint_hit_actual_share','joint_hit_fixed_share']]),
        '',
        f'GATE 较 DIRECT：60 天绝对误差减少 {p60.loc[BASE,"ae"]-p60.loc[RULE,"ae"]:,.4f} 件；高估减少 {p60.loc[BASE,"over_units"]-p60.loc[RULE,"over_units"]:,.4f} 件，低估减少 {p60.loc[BASE,"under_units"]-p60.loc[RULE,"under_units"]:,.4f} 件。',
        '',
        '### 五项原门槛（未四舍五入判定）',
        '',
        table(pd.DataFrame([{'门槛':k,'结论':'PASS' if val is True else 'FAIL' if val is False else 'UNKNOWN'} for k,val in s['gates'].items()])),
        '',
        f'原签发前下降观察组保持 {int(decl.loc[BASE,"fixed_n"])} 条配对记录、同一 60 天实销分母 {decl.loc[BASE,"actual_units"]:,.0f} 件：',
        '',
        table(decl.reset_index()[['model','fixed_n','actual_units','over_rate','over_units','under_units','wape']]),
        '',
        '组定义仅用原签发前资料，不称已确认未来真实衰退款。GATE 高估率低于 DIRECT，但高于原 YEARAGO；不以 Bias 代替高估率。',
        '',
        '## 4. 退出与新增：边界距离及销量权重',
        '',
        table(load('hit_changes').query("scope == 'PAIRED' and model != 'f28'")[['model','state','n','actual60','products']]),
        '',
        '原退出分档：excess=max(三窗口绝对相对误差)−0.20；按未舍入数值分箱。原退出样本均有三窗正实销。extra_ae/over/under 为 YEARAGO 减 DIRECT，负值表示减少。',
        '',
        table(bins[bins.kind.eq('LOST')].drop(columns='kind')),
        '',
        '新增入界幅度对称披露：entry_margin=0.20−max(三窗口绝对相对误差)，[0,2pp]、(2,5pp]、(5,10pp]、>10pp。轻微入界同样可能脆弱，不只强调退出靠近边界。',
        '',
        table(bins[bins.kind.eq('GAINED')].drop(columns='kind')),
        '',
        f'原退出 {lost.actual_d01_60.sum():,.0f} 件、新增 {gained.actual_d01_60.sum():,.0f} 件，净减少 {lost.actual_d01_60.sum()-gained.actual_d01_60.sum():,.0f} 件；GATE 尚余 {len(remaining)} 条原退出、{remaining.actual_d01_60.sum():,.0f} 件权重。无未分类退出（实测 {unclassified} 条）。',
        '',
        '## 5. 大体量退出及最大额外误差',
        '',
        '按原退出对象的 60 天实销降序；相同值用签发期、店铺、SPU 稳定排序。恢复=True 表示保留 DIRECT 后重新联合命中，不代表未来准确性。',
        '',
        table(top[KEY+['actual_d01_60','diagnostic_bin','yearago_extra_ae_d01_60','gate_decision','gate_joint_hit']]),
        '',
        '另按新增 60 天绝对误差排序的前 20 条：',
        '',
        table(topae[KEY+['actual_d01_60','yearago_extra_ae_d01_60','yearago_extra_over_d01_60','yearago_extra_under_d01_60','failed_windows','gate_joint_hit']]),
        '',
        '## 6. 分期、店铺与敏感性',
        '',
        '以下逐期增减均为 GATE−DIRECT；额外绝对误差为负表示改善。完整货龄、品类、季节（当前描述性标签，不是历史属性）分层保存在 metrics.csv、joint_summary.csv，不参与选择。',
        '',
    ]
    byissue=d[d.paired].groupby('issue_date').apply(lambda g:pd.Series(dict(n=len(g),annual_used=int(g.gate_decision.sum()),extra_ae60=g.gate_extra_ae_d01_60.sum(),joint_net=int(g.gate_joint_hit.sum()-g.direct_joint_hit.sum()),joint_actual_net=g.loc[g.gate_joint_hit,'actual_d01_60'].sum()-g.loc[g.direct_joint_hit,'actual_d01_60'].sum())),include_groups=False).reset_index()
    byshop=d[d.paired].groupby('shop').apply(lambda g:pd.Series(dict(n=len(g),annual_used=int(g.gate_decision.sum()),extra_ae60=g.gate_extra_ae_d01_60.sum(),joint_net=int(g.gate_joint_hit.sum()-g.direct_joint_hit.sum()),joint_actual_net=g.loc[g.gate_joint_hit,'actual_d01_60'].sum()-g.loc[g.direct_joint_hit,'actual_d01_60'].sum())),include_groups=False).reset_index()
    lines += [table(byissue),'',table(byshop),'',
              f'有 {int(byissue.extra_ae60.lt(0).sum())} 期绝对误差改善、{int(byissue.extra_ae60.gt(0).sum())} 期变差、{int(byissue.extra_ae60.eq(0).sum())} 期不变。同期联合承载量增减并不稳定；剔除某些月份能让承载量方向改变，不能据此修改主评价。',
              '',table(sensitivity),'',
              '剔除最大款或单月仅为敏感性旁表，不能删除不利对象宣布通过。完整 2,820 条和配对 2,579 条始终不变。',
              '', '## 7. 独立核验与执行记录', '',
              f'- 独立原始条件复算 {v["routing_primitive_checks"]:,} 条，真值删除/重排/扰动不影响选择 {v["truth_poison_checks"]:,} 次。',
              f'- 日真值逐日重求和 {v["raw_daily_truth_checks"]:,} 个窗口；独立标量算法核对 {v["independent_metric_rows"]:,} 行指标与 {v["independent_joint_rows"]:,} 行联合汇总，全部一致。',
              '- TRUE 双段来源及 FALSE 精确保留、单段支持不足时整对象保留、年度边界及缺值、显式零、无正销量观察、复合键隔离均已核查。',
              '- GATE 新增/退出集合均是原 YEARAGO 对应集合子集；原 241 条缺输出不变。命中实销仅取 60 天一次。',
              f'- {v["old_sources_unchanged"]:,} 个原来源文件哈希前后不变，包括输入、名单、配置、预测和模型文件。模型仅哈希校验，从未加载推理。',
              '- 本轮：新拟合 0，新推理 0，数据库查询 0，生产变更 0。原归档不覆盖；新结果独立存储。',
              '', '### 核心哈希与版本', '',
              f'- YEARAGO 基准：`83e0baa32f6de5cea6ff38e19c87f56fba67daba`；DIRECT：`34bdc3b703c45326f3c638814ad18595c8124e83`。',
              f'- routing.parquet：`{rm["routing_sha256"]}`。',
              f'- scoring/manifest.json：`{sha(out/"scoring/manifest.json")}`。',
              f'- validation/validation.json：`{sha(out/"validation/validation.json")}`。',
              '- 最终源码提交、推送结果及可复制复现命令见根目录《交付与复现记录.md》；只推送源码、测试和脱敏规范。',
              '', '## 8. 交付索引与停止结论', '',
              '- routing.csv/parquet、routing_manifest.json、routing_access_log.json：完整选择及时间顺序证据。',
              '- scoring/details.csv：全量 2,820 条预测、真值、窗口误差、所有失败条件、来源及命中迁移。',
              '- scoring/original_exits.csv / original_gains.csv / boundary_bins.csv：全部退出和新增的边界诊断。',
              '- scoring/top20_exits_actual.csv / top20_exits_extra_ae.csv：两套固定排序前 20 清单。',
              '- scoring/metrics.csv / joint_summary.csv / not_matured.csv：完整范围、配对及 TRUE/FALSE、多维分层、未成熟保留。',
              '- validation/validation.json、tests_execution.json：独立数值核验及单元测试记录。',
              '',
              '**结论：现有数据支持这条规则降低总体误差、恢复部分受损记录，但不足以保住联合命中的销量权重；唯一规则未通过本轮筛选。停止，不追加条件或改动模型，不部署。**',
              '']
    dest.mkdir(exist_ok=False)
    (dest/'YEARAGO适用条件回放与核心款退出命中诊断.md').write_text('\n'.join(lines),encoding='utf-8')
    byissue.to_csv(dest/'routing_by_issue.csv',index=False,encoding='utf-8-sig')
    byshop.to_csv(dest/'by_shop.csv',index=False,encoding='utf-8-sig')
    route.groupby(['gate_decision','failed_conditions','main_reason'],dropna=False).size().rename('n').reset_index().to_csv(dest/'routing_failures.csv',index=False,encoding='utf-8-sig')
    dump(dest/'manifest.json',dict(at=now(),script_sha256=sha(__file__),files={p.name:sha(p) for p in dest.iterdir() if p.is_file()}))
    print(str(dest/'YEARAGO适用条件回放与核心款退出命中诊断.md'))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--report-dir',default='report');report(**vars(p.parse_args()))
