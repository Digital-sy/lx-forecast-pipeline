"""Computed descriptive findings from reviewed monthly outputs; no fit or SQL.

All business values are read at runtime. Cross-issue rows overlap and are not
independent products, causal effects, or unique procurement losses.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

KEY=['cohort_id','issue_date','shop','spu']
TRUTH_END=pd.Timestamp('2026-10-01')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_csv(path):
    # Numeric-looking identifiers and leading zeros are still identifiers.
    return pd.read_csv(path,dtype={k:str for k in KEY})


def metric(group):
    good=group.actual.notna()&group.prediction.notna()
    part=group[good];error=part.prediction-part.actual
    total=float(part.actual.sum());positive=group.actual.gt(0)
    hit=good&positive&(group.prediction-group.actual).abs().le(.2*group.actual)
    return {'core_issue_n':len(group),'output_n':int(group.prediction.notna().sum()),
        'truth_n':int(group.actual.notna().sum()),'scored_n':len(part),'positive_truth_n':int(positive.sum()),
        'hit_n':int(hit.sum()),'hit_rate':float(hit.sum()/positive.sum()) if positive.any() else np.nan,
        'actual_units':total,'forecast_units':float(part.prediction.sum()),'absolute_error_units':float(error.abs().sum()),
        'wape':float(error.abs().sum()/total) if total>0 else np.nan,
        'bias':float(error.sum()/total) if total>0 else np.nan,
        'zero_truth_n':int(group.actual.eq(0).sum()),'unknown_truth_n':int(group.actual.isna().sum())}


def mature(frame):
    return frame[frame.cohort_id.eq('monthly_core')&
        (pd.to_datetime(frame.issue_date)+pd.Timedelta(days=60)).le(TRUTH_END)].copy()


def summarize(frame,keys):
    rows=[]
    for values,group in frame.groupby(keys,dropna=False,sort=True):
        if not isinstance(values,tuple):values=(values,)
        rows.append({**dict(zip(keys,values)),**metric(group)})
    return pd.DataFrame(rows)


def paired_results(scores):
    keys=KEY+['window']
    base=scores[scores.model.eq('F28')][keys+['actual','prediction']].rename(columns={
        'actual':'baseline_actual','prediction':'baseline_prediction'})
    pairs=scores[~scores.model.eq('F28')].merge(base,on=keys,validate='many_to_one')
    both=pairs.actual.notna()&pairs.baseline_actual.notna()
    if not pairs.loc[both,'actual'].eq(pairs.loc[both,'baseline_actual']).all():raise ValueError('PAIR_TRUTH_CHANGED')
    pairs['paired']=both&pairs.prediction.notna()&pairs.baseline_prediction.notna()
    results=[]
    for (model,window),group in pairs.groupby(['model','window']):
        good=group[group.paired];baseline=good.copy();baseline['prediction']=baseline.baseline_prediction
        a,b=metric(good),metric(baseline)
        results.append({'model':model,'window':window,'full_core_issue_n':len(group),'paired_n':len(good),
            **{'candidate_'+k:v for k,v in a.items()},**{'f28_'+k:v for k,v in b.items()},
            'wape_change':a['wape']-b['wape'],'bias_change':a['bias']-b['bias'],
            'scope':'same_objects_windows_truth; overlapping_issue_records'})
    return pd.DataFrame(results)


def joint_migrations(joints):
    base=joints[joints.model.eq('F28')][KEY+['decision']].rename(columns={'decision':'baseline_decision'})
    pairs=joints[~joints.model.eq('F28')].merge(base,on=KEY,validate='many_to_one')
    rows=[]
    for model,g in pairs.groupby('model'):
        known=g.decision.ne('UNJUDGED')&g.baseline_decision.ne('UNJUDGED')
        rows.append({'model':model,'full_core_issue_n':len(g),'joint_judged_pair_n':int(known.sum()),
            'retained_hits':int((known&g.decision.eq('HIT')&g.baseline_decision.eq('HIT')).sum()),
            'new_hits':int((known&g.decision.eq('HIT')&g.baseline_decision.ne('HIT')).sum()),
            'exited_hits':int((known&g.decision.ne('HIT')&g.baseline_decision.eq('HIT')).sum()),
            'both_miss':int((known&g.decision.ne('HIT')&g.baseline_decision.ne('HIT')).sum()),
            'unjudged_pairs':int((~known).sum())})
    return pd.DataFrame(rows)


def extreme_context(scores,extremes,fits):
    rows=[]
    for e in extremes.itertuples():
        group=scores[scores.model.eq(e.model)&scores.window.eq(e.window)&scores.issue_month.eq(e.issue_month)]
        if group.empty:continue
        shops=summarize(group,['shop']).sort_values(['absolute_error_units','shop'],ascending=[False,True])
        worst=shops.iloc[0]
        kinds=['M1'] if e.model=='DIRECT30x2' and e.window=='d01_30' else ['M2'] if e.model=='DIRECT30x2' and e.window=='d31_60' else ['M1','M2'] if e.model=='DIRECT30x2' else [e.model]
        training=fits[fits.issue_date.eq(group.issue_date.iloc[0])&fits.kind.isin(kinds)]
        fields=['kind','status','train_start','train_end','available_calendar_days','train_rows','train_dates',
            'train_objects','positive_labels','zero_labels','rows_after_lag_drop','effective_label_dates',
            'effective_positive_labels','effective_zero_labels','first_label','last_label','max_global_nonoverlap']
        evidence=training[[c for c in fields if c in training]].replace({np.nan:None}).to_dict('records')
        row={**e._asdict(),**metric(group),'largest_error_shop':worst.shop,
            'largest_error_shop_n':int(worst.core_issue_n),'largest_error_shop_wape':worst.wape,
            'largest_error_shop_absolute_error':worst.absolute_error_units,
            'training_evidence':json.dumps(evidence,ensure_ascii=False,default=str),
            'training_basis':'fixed_28_calendar_days' if e.model in ['F28','WEEKDAY4'] else 'actual_issue_frozen_training',
            'interpretation':'DESCRIPTIVE_EXTREMES_WITH_CHANGING_POPULATION_AND_TRAINING'}
        row['training_calendar_days']='28' if e.model in ['F28','WEEKDAY4'] else '; '.join(
            f"{r['kind']}:{r.get('available_calendar_days','UNKNOWN')}" for r in evidence)
        row['training_effective_label_dates']='NO_FIT' if e.model in ['F28','WEEKDAY4'] else '; '.join(
            f"{r['kind']}:{r.get('effective_label_dates') if r['kind'].startswith('B') else r.get('train_dates','UNKNOWN')}" for r in evidence)
        row['training_object_counts']='NO_FIT' if e.model in ['F28','WEEKDAY4'] else '; '.join(
            f"{r['kind']}:{r.get('train_objects','UNKNOWN')}" for r in evidence)
        for field in ['lifecycle','maturity']:
            row[field+'_composition']=json.dumps(group[field].fillna('UNKNOWN').value_counts().to_dict(),ensure_ascii=False) if field in group else '{}'
        rows.append(row)
    return pd.DataFrame(rows)


def history_results(scores,history):
    if history.duplicated(KEY).any():raise ValueError('DUPLICATE_HISTORY_COVERAGE')
    out=scores.merge(history[KEY+['complete_365_days','known_history_365_days']],on=KEY,how='left',validate='many_to_one')
    state=out.complete_365_days.astype(str).str.lower()
    out['history_group']=np.select([state.eq('true'),state.eq('false')],
        ['COMPLETE_365_CALENDAR_DAYS','LESS_THAN_365_COMPLETE_DAYS'],default='UNKNOWN_COVERAGE')
    result=summarize(out,['model','window','history_group'])
    result['scope']='DESCRIPTIVE_SELECTED_SUBGROUP_NOT_SAME_DENOMINATOR'
    return result


def metadata_results(scores,metadata):
    if metadata.duplicated(['shop','spu']).any():raise ValueError('DUPLICATE_CURRENT_METADATA')
    frame=scores.merge(metadata[['shop','spu','current_category','current_season']],on=['shop','spu'],how='left',validate='many_to_one')
    metrics=[];coverage=[]
    population=frame[frame.model.eq('F28')&frame.window.eq('d01_60')]
    for field in ['current_category','current_season']:
        frame[field]=frame[field].fillna('UNKNOWN').replace('','UNKNOWN')
        known=population[field].notna()&~population[field].astype(str).isin(['','UNKNOWN'])
        coverage.append({'dimension':field,'core_issue_n':len(population),'current_label_known_n':int(known.sum()),
            'current_label_unknown_n':int((~known).sum()),'known_share':float(known.mean()) if len(population) else np.nan,
            'historical_effective_date':'UNKNOWN'})
        part=summarize(frame,['model','window',field]).rename(columns={field:'group_value'})
        part['dimension']=field;part['scope']='CURRENT_METADATA_DESCRIPTIVE_NOT_HISTORICAL_INPUT'
        metrics.append(part)
    return pd.concat(metrics,ignore_index=True),pd.DataFrame(coverage)


OPERATING=[('listing_sessions_sum','listing_sessions_sum_input_daily_mean','listing_sessions_sum_target_daily_mean'),
    ('ad_spend','ad_spend_input_daily_mean','ad_spend_target_daily_mean'),
    ('ad_clicks','ad_clicks_input_daily_mean','ad_clicks_target_daily_mean'),
    ('ad_orders','ad_orders_input_daily_mean','ad_orders_target_daily_mean'),
    ('variant_count','sku_count_input_daily_mean','sku_count_target_daily_mean'),
    ('promotion_units','promotion_units_input_daily_mean','promotion_units_target_daily_mean'),
    ('order_session_ratio','input_order_session_ratio','target_order_session_ratio'),
    ('ad_cost_sales_ratio','input_ad_cost_sales_ratio','target_ad_cost_sales_ratio'),
    ('volume_weighted_price','input_volume_weighted_price','target_volume_weighted_price'),
    ('revenue_per_unit_proxy','input_revenue_per_sold_unit_proxy','target_revenue_per_sold_unit_proxy'),
    ('zero_inventory_row_daily_observations','input_zero_inventory_row_observations','target_zero_inventory_row_observations')]


def operating_results(scores,operating):
    if operating.duplicated(KEY).any():raise ValueError('DUPLICATE_OPERATING_KEY')
    base=scores[scores.model.eq('F28')&scores.window.eq('d01_60')]
    wide=base.merge(operating,on=KEY,how='left',validate='one_to_one')
    coverage=[];associations=[]
    for field,pre,post in OPERATING:
        before=pd.to_numeric(wide[pre],errors='coerce') if pre in wide else pd.Series(np.nan,index=wide.index)
        after=pd.to_numeric(wide[post],errors='coerce') if post in wide else pd.Series(np.nan,index=wide.index)
        if field=='zero_inventory_row_daily_observations':before,after=before/28,after/60
        known=np.isfinite(before)&np.isfinite(after)
        ratio=(after/before).where(known&before.gt(0));wide[field+'_change_ratio']=ratio
        cover={'indicator':field,'full_core_issue_n':len(wide),'known_before_after_n':int(known.sum()),
            'positive_baseline_ratio_n':int(ratio.notna().sum()),'unknown_n':int((~known).sum()),
            'zero_baseline_n':int((known&before.eq(0)).sum()),
            'median_change_ratio':float(ratio.median()) if ratio.notna().any() else np.nan,
            'evidence_grade':'INDICATOR_ONLY_NOT_CAUSAL'}
        coverage.append(cover)
        directions=np.select([~known,before.eq(0),ratio.lt(1),ratio.gt(1)],
            ['UNKNOWN','ZERO_BASELINE_NO_RATIO','DECREASE','INCREASE'],default='UNCHANGED')
        temp=wide.assign(direction=directions)
        result=summarize(temp,['direction']);result['indicator']=field
        result['evidence_grade']='INDICATOR_ONLY_NOT_CAUSAL';associations.append(result)
    wide['signed_error']=wide.prediction-wide.actual
    wide['absolute_error_units']=wide.signed_error.abs()
    examples=wide[wide.absolute_error_units.notna()].sort_values('absolute_error_units',ascending=False).head(8).copy()
    examples['evidence_grade']='INDICATOR_ONLY_NOT_CAUSAL'
    return pd.DataFrame(coverage),pd.concat(associations,ignore_index=True),examples


def shape_results(shape):
    frame=shape.copy()
    known=frame.a1.notna()&frame.a2.notna();positive=known&frame.a1.gt(0)&frame.a2.gt(0)
    impossible=positive&(2*frame[['a1','a2']].max(axis=1)>3*frame[['a1','a2']].min(axis=1))
    lower=.8*frame[['a1','a2']].max(axis=1);upper=1.2*frame[['a1','a2']].min(axis=1)
    frame['error_structure']=np.select([~known,~positive,impossible,frame.p1.isna(),frame.p1.lt(lower),frame.p1.gt(upper)],
        ['UNKNOWN_ACTUAL','ZERO_ACTUAL_SEPARATE','INCOMPATIBLE_EQUAL_SEGMENT_SHAPE','MISSING_INPUT',
         'COMPATIBLE_SHAPE_LEVEL_TOO_LOW','COMPATIBLE_SHAPE_LEVEL_TOO_HIGH'],default='BOTH_SEGMENTS_HIT')
    frame['actual60']=frame.a1+frame.a2;frame['absolute_error60']=(frame.p60-frame.actual60).abs()
    monthly=[]
    for issue,group in frame.groupby('issue_date'):
        eligible=group.a1.gt(0)&group.a2.gt(0)
        flag=group.error_structure.eq('INCOMPATIBLE_EQUAL_SEGMENT_SHAPE')
        actual=float(group.actual60.sum());positive_actual=float(group.loc[eligible,'actual60'].sum())
        selected=float(group.loc[flag,'actual60'].sum())
        monthly.append({'issue_date':issue,'core_issue_n':len(group),'positive_both_n':int(eligible.sum()),
            'incompatible_n':int(flag.sum()),'object_share_positive_both':float(flag.sum()/eligible.sum()) if eligible.any() else np.nan,
            'known_core_actual_units':actual,'positive_both_actual_units':positive_actual,'incompatible_actual_units':selected,
            'sales_share_known_core':selected/actual if actual>0 else np.nan,
            'sales_share_positive_both':selected/positive_actual if positive_actual>0 else np.nan})
    classes=frame.groupby('error_structure',dropna=False).agg(core_issue_n=('shop','size'),
        known_actual_n=('actual60','count'),scored_n=('absolute_error60','count'),
        actual_units=('actual60',lambda x:x.sum(min_count=1)),
        absolute_error_units=('absolute_error60',lambda x:x.sum(min_count=1))).reset_index()
    examples=frame.sort_values(['absolute_error60','issue_date'],ascending=[False,True]).groupby('error_structure',sort=True).head(1)
    delta=pd.concat([(frame.p1-frame.mean28*30).abs(),(frame.p2-frame.mean28*30).abs(),
                     (frame.p60-frame.p1-frame.p2).abs()])
    audit={'max_formula_absolute_difference':float(delta.max()) if delta.notna().any() else None,
           'formula_known_rows':int(frame[['p1','p2','p60','mean28']].notna().all(axis=1).sum())}
    return pd.DataFrame(monthly),classes,examples,audit


def survivor_results(frame):
    rows=[]
    for (comparison,model,window),g in frame.groupby(['comparison','model','window']):
        paired=g[['actual','prediction','previous_actual','previous_prediction']].notna().all(axis=1)
        after=g[paired];before=after.copy();before['actual']=before.previous_actual;before['prediction']=before.previous_prediction
        a,b=metric(after),metric(before)
        rows.append({'comparison':comparison,'model':model,'window':window,'surviving_core_issue_n':len(g),
            'paired_n':int(paired.sum()),'previous_wape':b['wape'],'later_wape':a['wape'],
            'wape_change':a['wape']-b['wape'],'previous_bias':b['bias'],'later_bias':a['bias'],
            'scope':'DESCRIPTIVE_SURVIVOR_SELECTED_NOT_CAUSAL'})
    return pd.DataFrame(rows)


def table(frame,columns,percent=()):
    result=['| '+' | '.join(columns)+' |','| '+' | '.join(['---']*len(columns))+' |']
    for _,row in frame.iterrows():
        values=[]
        for col in columns:
            value=row[col]
            if pd.isna(value):text='N/A'
            elif col in percent:text=f'{value:.2%}'
            elif isinstance(value,(float,np.floating)):text=f'{value:,.3f}'
            else:text=str(value)
            values.append(text.replace('|','/'))
        result.append('| '+' | '.join(values)+' |')
    return '\n'.join(result)


def native_note(native,extension=None):
    prefix=f"原复用旁表覆盖{len(native['monthly_available_targets'])}个目标月，另{len(native['monthly_unavailable_targets'])}个月没有旧归档；这不等于本轮未取得日历史。"
    if extension is None or extension.get('status')!='COMPLETE' or extension.get('source_unchanged') is False:
        v4='V4已另找到原冻结SKU月度输入，可作独立纯离线扩展；本摘要尚未读到已完成的新增旁表清单，完成范围保持待定，不据旧旁表N/A断言V4不能计算。'
    else:
        v4=f"V4新增原生月度扩展已完成，清单覆盖{len(extension.get('target_months',[]))}个新增目标月、{extension.get('native_calculation_count','UNKNOWN')}次冻结原生计算、拟合{extension.get('fits','UNKNOWN')}次；另见native_v4_extension的实际范围和版本清单。不得继续把旧复用旁表的缺月数当成V4新增结果的不可用月数。"
    return prefix+v4+'A16仍需各截点已冻结的校准choices；原入口会执行A7参数选择，早期截点也可能无合法校准标签，本轮未盲跑入口或借未来参数。所有月度结果保留原生截点、真值和范围，不均摊为日值、不混入首次签发60天榜。'


def run(root,out):
    root,out=Path(root),Path(out)
    if out.exists():raise ValueError('OUTPUT_EXISTS')
    paths={'scores':root/'scoring/scores.parquet','joints':root/'scoring/joint_hits.csv',
        'shapes':root/'scoring/f28_shape_details.csv','operating':root/'scoring/operating_indicators_diagnostic.csv',
        'extremes':root/'diagnostics/best_worst_months.csv','history':root/'diagnostics/season_history_coverage.csv',
        'survivors':root/'diagnostics/survivor_adjacent_yoy.parquet',
        'metadata':root/'side_panels/current_metadata_mapping.csv','fits':root/'effects/fit_status.csv',
        'native_manifest':root/'side_panels/side_panels_manifest.json'}
    source_hashes={str(path.resolve()):sha(path) for path in paths.values()}
    native=json.loads(paths['native_manifest'].read_text(encoding='utf-8'))
    extension_path=root/'native_v4_extension/execution_manifest.json'
    extension=json.loads(extension_path.read_text(encoding='utf-8')) if extension_path.exists() else None
    if extension is not None:source_hashes[str(extension_path.resolve())]=sha(extension_path)
    scores=mature(pd.read_parquet(paths['scores']));joints=mature(read_csv(paths['joints']))
    fits=read_csv(paths['fits']);extremes=read_csv(paths['extremes'])
    if scores.duplicated(KEY+['model','window']).any():raise ValueError('DUPLICATE_SCORE_KEY')
    paired=paired_results(scores);joint=joint_migrations(joints)
    contexts=extreme_context(scores,extremes,fits)
    history=history_results(scores,read_csv(paths['history']))
    metadata,meta_coverage=metadata_results(scores,read_csv(paths['metadata']))
    operating,associations,oper_examples=operating_results(scores,read_csv(paths['operating']))
    shapes,shape_classes,shape_examples,formula=shape_results(mature(read_csv(paths['shapes'])))
    survivors=survivor_results(mature(pd.read_parquet(paths['survivors'])))
    output={'paired_f28_summary':paired,'joint_migration_summary':joint,'extreme_context':contexts,
        'training_by_issue':fits,'complete_season_history_summary':history,'current_metadata_summary':metadata,
        'current_metadata_coverage':meta_coverage,'operating_coverage':operating,'operating_error_associations':associations,
        'operating_examples':oper_examples,'f28_shape_by_month':shapes,'f28_error_structure':shape_classes,
        'f28_formula_examples':shape_examples,'survivor_summary':survivors}
    out.mkdir(exist_ok=False)
    for name,frame in output.items():frame.to_csv(out/(name+'.csv'),index=False,encoding='utf-8-sig')
    p60=paired[paired.window.eq('d01_60')]
    ext=contexts[contexts.window.eq('d01_60')]
    hist=history[history.window.eq('d01_60')]
    groups=metadata[metadata.model.eq('F28')&metadata.window.eq('d01_60')&metadata.group_value.ne('UNKNOWN')]
    groups=groups.sort_values('absolute_error_units',ascending=False).groupby('dimension',sort=True).head(3)
    op=operating.sort_values(['known_before_after_n','indicator'],ascending=[False,True])
    known_sales=float(shapes.known_core_actual_units.sum());shape_sales=float(shapes.incompatible_actual_units.sum())
    n=int(shapes.positive_both_n.sum());bad=int(shapes.incompatible_n.sum())
    text=['# 逐月结果的实数解释补充','',
        '以下数值均由本轮已落盘结果计算，不追加拟合。跨期记录是“核心对象×签发期”，60天窗口相互重叠；不是独立样本、唯一商品数或采购损失。所有分层和经营对应关系均为描述性证据，不证明因果，也不用于事后选模型。','',
        '## 同对象配对：候选相对 F28','',
        table(p60,['model','full_core_issue_n','paired_n','candidate_wape','f28_wape','wape_change','candidate_bias','f28_bias'],
              ['candidate_wape','f28_wape','wape_change','candidate_bias','f28_bias']),
        '', '上述每行候选与F28共用对象、目标窗和真值；不同行的paired_n仍可能不同，不能再次当作共同分母总榜。WAPE差是百分点差。','',
        table(joint,['model','full_core_issue_n','joint_judged_pair_n','retained_hits','new_hits','exited_hits','unjudged_pairs']),
        '', '三窗口新增/退出命中仅在两边三窗均可判断的对子中统计；未知对子单列，不冒充退出或成功。','',
        '## 最好、最差月份及构成','',
        table(ext,['model','criterion','kind','issue_month','value','core_issue_n','scored_n','largest_error_shop','largest_error_shop_wape'],['largest_error_shop_wape']),
        '', 'value 为比例值：wape越低越好，hit_rate越高越好；店铺列是该期绝对误差贡献最大的店铺，不是自动判定的原因。','',
        table(ext[ext.criterion.eq('wape')],['model','kind','issue_month','training_calendar_days',
            'training_effective_label_dates','training_object_counts','lifecycle_composition','maturity_composition']),
        '', '训练有效日期为B类lag丢弃后的标签日期、DIRECT各段成熟签发日期；不是预测窗口长度。完整训练起止、零/正标签见 extreme_context.csv 和 training_by_issue.csv。跨月训练长度与商品构成同时变化，不能排除这些混淆。','',
        '## 365日源记录完整的描述性子集','',
        table(hist,['model','history_group','core_issue_n','output_n','scored_n','wape','bias','hit_rate'],['wape','bias','hit_rate']),
        '', '“365日源记录完整”仅指截点前365个日历日的销量记录均有效且映射有效，包含来源明确记录的0；不是只看首末跨度，也不证明365天持续在售、完整经营季节周期或历史供给可比。货龄和原资格另列于描述性分层与极值构成，不由此标志替代。子集与非完整记录组分母不同；这不是固定训练长度实验，也没有消除商品存活选择。','',
        '## 当前品类、季节标签：覆盖及高误差组','',
        table(meta_coverage,['dimension','core_issue_n','current_label_known_n','current_label_unknown_n','known_share'],['known_share']),
        '',table(groups,['dimension','group_value','core_issue_n','scored_n','absolute_error_units','wape','bias'],['wape','bias']),
        '', '此处采用F28作为统一描述对象，列各标签维度绝对误差贡献最大的至多3组；UNKNOWN没有被删出完整名单。当前标签缺少历史生效证明，不能声称是历史季节类别，更不能回填模型输入。','',
        '## 经营指标：先看证据覆盖，再看对应变化','',
        table(op,['indicator','full_core_issue_n','known_before_after_n','positive_baseline_ratio_n','unknown_n','median_change_ratio']),
        '', '变化比为每对象目标60天与输入28天可比日均或原生比率之比，中位数不是销量加权效应。覆盖要求前后来源完整；零基数不生成无穷比。库存只统计每天的零值行观察数，不是断货天数；花费不是预算。','',
        table(associations[associations.indicator.isin(['listing_sessions_sum','ad_spend','volume_weighted_price'])],
            ['indicator','direction','core_issue_n','scored_n','wape','bias'],['wape','bias']),
        '', '上表是F28误差与指标升降的同时出现，不证明投放、季节或供给导致误差；指标完整性和样本构成可能不同。以下只展示误差最大的至多5条，非典型性抽样；更完整本地明细见 operating_examples.csv。','',
        table(oper_examples.head(5),['issue_date','shop','spu','actual','prediction','signed_error',
            'listing_sessions_sum_change_ratio','ad_spend_change_ratio','volume_weighted_price_change_ratio']),
        '', '历史运营变更台账、实际预算和已确认整段断货区间均未恢复；这些示例仍只是指标线索。','',
        '## F28：形状不可能与水平估计偏差','',
        f'两个30天实销均正的 {n:,} 条记录中，{bad:,} 条形状比超过1.5（{bad/n:.2%}）' if n else '没有两个30天实销均正的可判断记录。',
        f'；这些记录占已知核心实销的 {shape_sales/known_sales:.2%}。' if known_sales else '；已知核心实销分母不可用。',
        '',table(shapes,['issue_date','core_issue_n','positive_both_n','incompatible_n','object_share_positive_both','sales_share_known_core'],
            ['object_share_positive_both','sales_share_known_core']),
        '',table(shape_classes,['error_structure','core_issue_n','known_actual_n','scored_n','actual_units','absolute_error_units']),
        '', '形状兼容时，相等预测同时命中的可行区间是[0.8×max(A1,A2), 1.2×min(A1,A2)]；预测落在区间下方/上方分别记近期水平偏低/偏高。这是数值分类而非业务成因。零实销、缺实销和缺输入单列。',
        '',f"F28公式核对最大绝对差：{formula['max_formula_absolute_difference']}；有效公式行 {formula['formula_known_rows']:,}。",'',
        table(shape_examples,['issue_date','shop','spu','mean28','p1','p2','p60','a1','a2','shape_ratio','first_level_ratio','second_level_ratio','error_structure']),
        '', '公式示例按各错误结构选择绝对误差较大的一条，不代表典型商品；实际公式及输入/目标日期见逐款文件。','',
        '## 同商品跨期辅助对照','',
        table(survivors[survivors.window.eq('d01_60')],['comparison','model','surviving_core_issue_n','paired_n','previous_wape','later_wape','wape_change'],
            ['previous_wape','later_wape','wape_change']),
        '', '只保留前后均在核心名单的存活对象；数值不能替代每期完整名单，也不能被解释为同一独立样本的因果变化。','',
        '## 原生月度旁表的 N/A 边界','',
        native_note(native,extension),'']
    (out/'实数解释补充.md').write_text('\n'.join(text),encoding='utf-8')
    unchanged=all(sha(path)==digest for path,digest in source_hashes.items())
    manifest={'status':'COMPLETE' if unchanged else 'SOURCE_CHANGED','completed_at':dt.datetime.now(dt.timezone.utc).isoformat(),
        'script_sha256':sha(__file__),'sources':source_hashes,'source_unchanged':unchanged,'fits':0,'queries':0,
        'native_v4_extension_status':extension.get('status') if extension is not None else 'NOT_YET_AVAILABLE',
        'classification':'DESCRIPTIVE_NOT_CAUSAL','formula_audit':formula,
        'files':{p.name:sha(p) for p in out.iterdir() if p.is_file()}}
    (out/'findings_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    if not unchanged:raise ValueError('SOURCE_CHANGED_DURING_FINDINGS')
    print(json.dumps({'status':manifest['status'],'core_issue_records':len(scores[scores.model.eq('F28')&scores.window.eq('d01_60')]),'tables':len(output)}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True);parser.add_argument('--out',required=True)
    run(**vars(parser.parse_args()))
