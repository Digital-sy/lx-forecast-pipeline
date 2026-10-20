"""Assemble actual monthly replay tables and a Chinese evidence report; no fitting."""
from pathlib import Path
import argparse
import json
import numpy as np
import pandas as pd
try:
    from scripts.bc60_monthly_score import metric, sha, dump, csv, MODELS, WINDOWS
except ModuleNotFoundError:
    from bc60_monthly_score import metric, sha, dump, csv, MODELS, WINDOWS


def text_table(frame,cols,percent=()):
    lines=['| '+' | '.join(cols)+' |','| '+' | '.join(['---']*len(cols))+' |']
    for _,row in frame.iterrows():
        vals=[]
        for c in cols:
            x=row[c]
            if pd.isna(x):v='N/A'
            elif c in percent:v=f'{float(x)*100:.2f}%'
            elif isinstance(x,(float,np.floating)):v=f'{x:,.2f}' if x!=int(x) else f'{int(x):,}'
            else:v=str(x)
            vals.append(v.replace('|','/'))
        lines.append('| '+' | '.join(vals)+' |')
    return '\n'.join(lines)


def run(root,out,validation):
    root,out,validation=Path(root),Path(out),Path(validation)
    checks=json.loads(validation.read_text(encoding='utf-8'))
    if checks.get('status')!='PASS_INDEPENDENT_FULL_RECONCILIATION':
        raise ValueError('FINAL_NUMERIC_VALIDATION_NOT_PASS')
    for section,field,path in [('prepared','prepare_manifest_sha256','prepared/prepare_manifest.json'),
            ('predictions','effect_manifest_sha256','effects/run_manifest.json'),
            ('scores','score_manifest_sha256','scoring/score_manifest.json')]:
        if checks.get(section,{}).get(field)!=sha(root/path):
            raise ValueError('FINAL_VALIDATION_BINDING_MISMATCH:'+section)
    out.mkdir(exist_ok=False)
    s=pd.read_parquet(root/'scoring/scores.parquet');s=s[s.cohort_id.eq('monthly_core')]
    metrics=pd.read_csv(root/'scoring/metrics.csv');quality=pd.read_csv(root/'prepared/selection_quality.csv')
    dates=pd.read_csv(root/'prepared/issue_grid.csv');fits=pd.read_csv(root/'effects/fit_status.csv')
    cover=pd.read_csv(root/'scoring/future_coverage.csv');cover=cover[cover.cohort_id.eq('monthly_core')]
    pairs=pd.read_csv(root/'scoring/paired_f28.csv')
    joints=pd.read_csv(root/'scoring/joint_hits.csv');joints=joints[joints.cohort_id.eq('monthly_core')]
    current=json.loads((root/'effects/run_manifest.json').read_text(encoding='utf-8'))
    q=json.loads((root/'inputs/quality.json').read_text(encoding='utf-8'))
    rows=[]
    for date in dates.itertuples():
        selection=quality[quality.issue_date.eq(date.issue_date)]
        known_selection=selection.core_n.notna().all();N=int(selection.core_n.sum()) if known_selection else np.nan
        for model in MODELS:
            for window,(a,b) in WINDOWS.items():
                g=s[s.issue_date.eq(date.issue_date)&s.model.eq(model)&s.window.eq(window)]
                rec={'issue_month':date.issue_month,'issue_date':date.issue_date,'input_cutoff':date.input_cutoff,
                     'target_start':str((pd.Timestamp(date.issue_date)+pd.Timedelta(days=a)).date()),
                     'target_end':str((pd.Timestamp(date.issue_date)+pd.Timedelta(days=b)).date()),'model':model,'window':window}
                rec.update(metric(g));rec['core_n']=N
                if not rec['scored_n']:
                    for field in ['actual_units','forecast_units','over_units','under_units','zero_absolute_error']:
                        rec[field]=np.nan
                if not rec['truth_n']:
                    rec['known_core_actual_units']=np.nan
                rec['selection_status']='|'.join(sorted(selection.selection_status.unique()))
                rec['partial_selection']=not known_selection
                j=joints[joints.issue_date.eq(date.issue_date)&joints.model.eq(model)]
                rec['joint_hit_n']=int(j.decision.eq('HIT').sum());rec['joint_unjudged_n']=int(j.decision.eq('UNJUDGED').sum())
                pg=pairs[pairs.cohort_id.eq('monthly_core')&pairs.slice.eq('issue')&pairs.issue_month.eq(date.issue_month)&pairs.model.eq(model)&pairs.window.eq(window)]
                rec['paired_f28_n']=int(pg.paired_n.iloc[0]) if len(pg) else int((g.actual.notna()&g.prediction.notna()).sum()) if model=='F28' else np.nan
                cg=cover[cover.issue_date.eq(date.issue_date)&cover.window.eq(window)]
                den=cg.observed_shop_units.sum();num=cg.complete_window_core_units.sum()
                rec['target_all_shop_units']=den if len(cg)==4 and cg.status.eq('OBSERVED_SOURCE_COVERAGE').all() else np.nan
                rec['target_core_sales_coverage']=num/den if pd.notna(rec['target_all_shop_units']) and den>0 else np.nan
                kinds=['M1'] if model=='DIRECT30x2' and window=='d01_30' else ['M2'] if model=='DIRECT30x2' and window=='d31_60' else ['M1','M2'] if model=='DIRECT30x2' else [model]
                fs=fits[fits.issue_date.eq(date.issue_date)&fits.kind.isin(kinds)]
                for field in ['training_stats_basis','available_calendar_days','train_rows','train_objects','train_dates','train_start','train_end','train_span_days','first_label','last_label','rows_after_lag_drop','effective_label_dates','max_global_nonoverlap']:
                    rec[field]='; '.join(f'{r.kind}:{getattr(r,field)}' for r in fs.itertuples() if hasattr(r,field))
                rec['status']='NOT_MATURED' if not date.calendar_mature else (
                    'PARTIAL_SELECTION_SCORED' if rec['partial_selection'] else 'SCORED') if rec['scored_n'] else 'NO_PAIRED_OUTPUT_OR_TRUTH'
                rows.append(rec)
    main=pd.DataFrame(rows);csv(out/'monthly_main_table.csv',main)
    heat=main.pivot(index='issue_month',columns=['model','window'],values='wape');heat.to_csv(out/'monthly_wape_heatmap.csv',encoding='utf-8-sig')
    main.pivot(index='issue_month',columns=['model','window'],values='status').to_csv(out/'monthly_heatmap_status.csv',encoding='utf-8-sig')
    mature=s[s.issue_date.lt('2026-09-01')];overall=[]
    for (model,window),g in mature.groupby(['model','window']):overall.append(dict(model=model,window=window,**metric(g)))
    overall=pd.DataFrame(overall);csv(out/'mature_overall.csv',overall)
    joint=[]
    for model,g in joints[joints.issue_date.lt('2026-09-01')].groupby('model'):
        joint.append(dict(model=model,core_issue_records=len(g),joint_hits=int(g.decision.eq('HIT').sum()),unjudged=int(g.decision.eq('UNJUDGED').sum()),joint_hit_fixed_share=float(g.decision.eq('HIT').mean())))
    joint=pd.DataFrame(joint);csv(out/'mature_joint_summary.csv',joint)
    shape=pd.read_csv(root/'scoring/f28_shape_summary.csv');shape=shape[shape.cohort_id.eq('monthly_core')&shape.issue_date.lt('2026-09-01')]
    extremes=pd.read_csv(root/'diagnostics/best_worst_months.csv')
    native_path=root/'native_v4_extension/execution_manifest.json'
    native=json.loads(native_path.read_text(encoding='utf-8')) if native_path.exists() else {}
    native_note=(f'V4另用已核验原始SKU月度归档补做{len(native.get("target_months",[]))}个缺失目标月，'
                 f'{native.get("native_calculation_count",0)}次原生确定性调用、0次学习型拟合；新增明细独立保存在native_v4_extension。'
                 if native.get('status')=='COMPLETE' else 'V4原生缺失月扩展未验收，保留N/A。')
    w60=overall[overall.window.eq('d01_60')].sort_values('wape')
    best_joint=joint.sort_values(['joint_hits','model'],ascending=[False,True]).iloc[0]
    paired_file=root/'findings/paired_f28_summary.csv'
    headline_pairs=pd.read_csv(paired_file) if paired_file.exists() else pd.DataFrame()
    if not headline_pairs.empty:
        headline_pairs=headline_pairs[headline_pairs.window.eq('d01_60')]
    paired_text=text_table(headline_pairs,['model','paired_n','f28_wape','candidate_wape','wape_change'],
        ['f28_wape','candidate_wape','wape_change']) if not headline_pairs.empty else '配对实数补充未生成，见scoring/paired_f28.csv。'
    lines=['# 2025年起逐月预测效果与季节经营差异报告','',
        '## 结论摘要','',
        f'已完成 {current["formal_fit_count"]} 次有限正式拟合；评价网格保留22期，其中20期60天目标已成熟。全部结果为扩展历史重训／重建回放，不是独立前瞻OOS，也不用于自动选择生产模型。',
        '',f'逐款目标仍未达到。按完整核心分母，三个窗口同时命中最多的 {best_joint.model} 为 '
            f'{int(best_joint.joint_hits):,}/{int(best_joint.core_issue_records):,}（{best_joint.joint_hit_fixed_share:.2%}）；'
            '缺预测、实销零及不可判断均未冒充命中。60天总量WAPE改善不等于逐款、分段均进入±20%。',
        '', '### 60天同对象配对结果','',paired_text,
        '', 'wape_change是候选减F28，负值表示误差下降；百分数差读作百分点。各行只在该候选与F28共同可评分对象上比较，不能混用不同范围的成绩。',
        '', '### 完整核心范围覆盖及结果','',
        '',f'共 {int(joint.core_issue_records.max()):,} 条成熟期核心对象×签发期记录；同商品可能跨期重复，不能解释成唯一商品数或唯一采购损失。各模型输出范围不同，以下原范围汇总不能当作严格同样本排名。',
        '',text_table(w60,['model','core_n','output_n','truth_n','scored_n','wape','bias','hit_n','hit_rate','hit_sales_share'],['wape','bias','hit_rate','hit_sales_share']),
        '',text_table(joint,['model','core_issue_records','joint_hits','unjudged','joint_hit_fixed_share'],['joint_hit_fixed_share']),
        '',f'F28形状限制：两个30天实销均正的 {int(shape.positive_both_n.sum()):,} 条中，{int(shape.incompatible_shape_n.sum()):,} 条的两段实销比超过1.5。对这些记录，任何两段相等的预测都不可能同时进入±20%；这不是已确认季节因果，也不能用未来形状挑模型。',
        '', '## 时间、核心名单与数据真实性','',
        '每月2日签发 S，输入截止 S−2；W1=S+1..30、W2=S+31..60、W60=S+1..60。日期、逐期N、预测/真值/配对覆盖和训练信息见 monthly_main_table.csv。2026-09、10不缩短窗口或填0。',
        '',f'来源支持 {q["first_day"]} 至 {q["last_day"]}，{q["daily_aggregate_rows"]:,} 条聚合记录。四店缺日期数 {len(q["missing_shop_dates"])}。各期独立冻结全店90天≥90%名单，88个店铺×月选款状态：{quality.selection_status.value_counts().to_dict()}。',
        '', '原2026归档原值保留。额外3天重叠复核见 overlap_revision_audit.json；来源当前创建/修改字段不证明历史时点可获取，MAX(modify_time)不代表同步任务成功。明确零值仅代表源明确记录0，不证明当时在售或无断货。月源与日源不一致单列，未改动本轮日源真值。',
        '', '## 各月最好与最差表现','',
        text_table(extremes[extremes.window.eq('d01_60')],['model','criterion','kind','issue_month','value','core_n','scored_n'],['value']),
        '', '这里只描述观察到的极值；禁止把事后每月最佳模型拼成策略。WAPE极值和逐款命中极值分别保留，完整三窗口热力矩阵见同目录CSV和交互报告。',
        '', '## 严格配对与范围差异','',
        'paired_f28.csv 为每候选与F28同对象同窗口同真值配对；all_model_common.csv仅作辅助。旧385条固定名单单列 original_fixed_385，不倒推新历史名单。新品／无资格／缺输出继续留在核心分母，不能靠模型交集缩小目标。',
        '', 'WAPE=配对对象绝对误差件数之和÷同范围实销之和；Bias=有符号误差之和÷同范围实销之和。二者按件数加权，不平均逐款百分比；WAPE可超过100%，1−WAPE不作为逐款准确率。',
        '', 'hit_rate的分母是该完整范围中实销为正的对象数，缺预测不计命中；hit_sales_share是命中对象实销÷该范围全部已知实销。配对表另在共同可评分范围计算，分母不同。±20%按未四舍五入值判断，实销为0的绝对误差另列。',
        '', '## 季节、经营与训练长度诊断','',
        '诊断文件包括 current_category/current_season 当前描述性分层、货龄/资格、365日源记录完整子表、相邻签发与跨年同月幸存者对照。365日记录完整含明确零值，不证明365天持续在售或完整可比的经营季节周期。当前元数据无历史生效证明，不回填为历史输入；幸存者子表不能代替各期完整名单。',
        '', '365日完整组是否形成有效对照，须看实际分组人数；如果全部对象均满足该标志，不能把同一批结果再次列出就当成已排除历史长度影响。',
        '', '训练覆盖保存实际起止、有效标签日期、商品数、零标签、lag丢弃及M1/M2成熟签发数。available_calendar_days是源覆盖跨度，不是实际有效训练跨度；递推模型train_rows/train_dates为lag丢弃前，rows_after_lag_drop/effective_label_dates才是有效训练量。月份之间同时改变训练跨度和商品构成，单凭月度好坏不能断言季节原因。未追加固定训练长度实验。',
        '', '主表joint_hit_n是三个窗口同时命中数，在每个窗口行重复展示，不可再跨窗口相加；实际为零的绝对误差另列，不计算虚假的相对误差命中率。',
        '', '经营诊断仅使用同批已归档指标：Sessions是Listing行汇总而非去重人数；转化/广告比率按汇总分子分母；支出不是预算；价格加权与收入/件只作指标线索；库存0行不等于整月断货。历史有效运营台账、预算和供给中断区间仍UNKNOWN。目标期信息只用于事后解释，不进入模型。',
        '', '## F28公式与缺值','',
        '严格取截止日往前28个日历日：P1=P2=30×mean28，P60=P1+P2。缺一天或无效映射/销量则保持未知，不用更早行替补。A1或A2为0时不计算无限shape_ratio；实际为0单列绝对误差，不伪造百分比命中。',
        '', '## 原生月度与滚动旁表','',
        'V4/A16原旁表复用9个目标月；A16其他12个月仍N/A，缺少对应截点冻结校准记录，未执行A7重选参。'+native_note+'月度缺实销仍UNKNOWN，原生资格/真值范围不同于90%日级核心名单，不与60天混排。R28-D/W保留原V2 T1固定终点与T2更新窗口。',
        '', '## 验收与限制','',
        f'独立数值验证：{checks.get("status","UNKNOWN")}，详情见 {validation.name}。本轮未修改V4/A16、新品模型、采购、BI、数据库或调度。未声称浏览器点击验收；报告程序构建与真实浏览器检查分别记录。',
        '', '原生来源EXPLAIN、查询预算、拒绝记录和获取时间见 source_audit。拟合账本、模型检查点和输入/输出哈希见 effects 与 prepared。源代码在独立分支，业务输入、模型和逐款结果仅存本地。',
        '', '## 文件索引','',
        '- monthly_main_table.csv：完整22期主表；monthly_wape_heatmap.csv 与 monthly_heatmap_status.csv：数值与N/A原因。',
        '- ../scoring/scores.csv：逐款预测、实销、误差、缺失状态；paired_f28.csv / joint_migrations.csv：配对与三窗口命中迁移。',
        '- ../diagnostics/：最好最差、当前品类/季节、完整历史、同款相邻/跨年、日月核对和经营线索。',
        '- ../effects/：100次上限的真实账本、各槽日志、训练审计与模型检查点；不重复拟合。',
        '- ../side_panels/：原生月度V4/A16和V2滚动更新独立旁表。','']
    if (root/'findings/实数解释补充.md').exists():
        lines.extend(['','---','', (root/'findings/实数解释补充.md').read_text(encoding='utf-8')])
    if (root/'findings/结论速读.md').exists():
        lines[2:2]=[(root/'findings/结论速读.md').read_text(encoding='utf-8'),'']
    (out/'2025年起逐月预测效果与季节经营差异报告.md').write_text('\n'.join(lines),encoding='utf-8')
    evidence=['prepared/prepare_manifest.json','effects/run_manifest.json','scoring/score_manifest.json',
        'diagnostics/diagnostic_manifest.json','findings/findings_manifest.json','side_panels/side_panels_manifest.json',
        'native_v4_extension/execution_manifest.json','validation_native_v4/validation.json',
        'source_audit/source_audit.json','overlap_revision_audit.json']
    evidence.append('findings/结论速读.md')
    dump(out/'report_manifest.json',{'source_sha256':sha(__file__),'validation_sha256':sha(validation),'fits':0,
        'evidence_sha256':{p:sha(root/p) for p in evidence if (root/p).exists()},
        'files':{p.name:sha(p) for p in out.iterdir() if p.is_file()}})
    print(json.dumps({'main_rows':len(main),'formal_fits':current['formal_fit_count'],'validation':checks.get('status')}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--out',required=True);p.add_argument('--validation',required=True);run(**vars(p.parse_args()))
