"""Produce Markdown and small evidence tables from verified cross-diagnostics."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
from pathlib import Path
from datetime import datetime,timezone
import numpy as np
import pandas as pd


def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def table(frame, columns, pct=()):
    def fmt(k,x):
        if pd.isna(x):return "—"
        if k in pct:return f"{float(x)*100:.2f}%"
        if isinstance(x,(int,np.integer)):return f"{int(x):,}"
        if isinstance(x,(float,np.floating)):return f"{x:,.2f}" if x%1 else f"{int(x):,}"
        return str(x).replace("|","／").replace("\n"," ")
    lines=["| "+" | ".join(columns)+" |","| "+" | ".join("---" for _ in columns)+" |"]
    lines += ["| "+" | ".join(fmt(k,row[k]) for k in columns)+" |" for _,row in frame.iterrows()]
    return "\n".join(lines)


def run(data,validation,out):
    data,validation,out=Path(data).resolve(),Path(validation).resolve(),Path(out).resolve()
    v=json.loads(validation.read_text(encoding="utf-8"))
    if v["status"]!="PASS_INDEPENDENT_CALCULATION_PATH" or v["analysis_manifest_sha256"]!=sha(data/"run_manifest.json"):
        raise ValueError("final validation required")
    if out.exists():raise ValueError("new report directory required")
    manifest=json.loads((data/"run_manifest.json").read_text(encoding="utf-8"))
    for f,h in manifest["files"].items():
        if sha(data/f)!=h:raise ValueError(f)
    b=pd.read_parquet(data/"base_windows.parquet")
    o=pd.read_parquet(data/"base_objects.parquet");o=o[o.calendar_mature.eq(True)].copy()
    p=pd.read_csv(data/"layer_pairs.csv");p60=p[p.window.eq("d01_60")]
    m=pd.read_csv(data/"layer_metrics.csv")
    cls=pd.read_csv(data/"error_classes.csv")
    cond=pd.read_csv(data/"conditional_comparisons.csv")
    stable=pd.read_csv(data/"combination_stability.csv")
    rank1=pd.read_csv(data/"ranking_1_supported_error_rates.csv")
    if "pair_n_x" in rank1:
        if not rank1.pair_n_x.eq(rank1.pair_n_y).all():
            raise ValueError("ranking support and metric pairing counts disagree")
        rank1=rank1.rename(columns={"pair_n_x":"pair_n"}).drop(columns="pair_n_y")
    rank2=pd.read_csv(data/"ranking_2_disjoint_error_units.csv")
    overlap=pd.read_csv(data/"dimension_overlap.csv")
    repeat=pd.read_csv(data/"calendar_repeatability.csv")
    sourcefile=Path(__file__).parent/"bc60_v3_direct.py"
    tree=ast.parse(sourcefile.read_text(encoding="utf-8"))
    features=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and
                  any(isinstance(t,ast.Name) and t.id=="FEATURES" for t in n.targets))
    pair=o[o.paired_object]
    all_n=len(o)
    globalp=p60[p60.level.eq("overall")].iloc[0]
    directcls=cls[cls.model.eq("DIRECT30x2")]
    unpred=directcls[directcls.error_class.eq("MISSING_PREDICTION")].iloc[0]
    allocation=directcls[directcls.error_class.eq("CUMULATIVE_HIT_SEGMENTS_MISS")].iloc[0]
    totalmiss=directcls[directcls.error_class.isin(["BOTH_OVER_20","BOTH_UNDER_20","MIXED_TOTAL_MISS"])]
    total_ae=directcls.ae60.sum()
    top=rank2[rank2.model.eq("DIRECT30x2")].head(3)
    pooled=p60[p60.level.eq("shop_category")].sort_values("direct_ae",ascending=False)
    out.mkdir(parents=True,exist_ok=False)
    tables={}
    commonclasses=[]
    for model in ["f28","direct"]:
        for name,g in pair.groupby(model+"_class"):
            commonclasses.append(dict(model=model,error_class=name,n=len(g),actual_units=g.a60.sum(),
                ae60=g[model+"_ae60"].sum(min_count=1),ae_segments=g[model+"_ae_segments"].sum(min_count=1)))
    tables["same_pair_error_classes.csv"]=pd.DataFrame(commonclasses)
    z=pair.copy()
    z["ae_improvement"]=z.f28_ae60-z.direct_ae60
    product=z.groupby(["shop","spu"]).agg(records=("spu","size"),actual_units=("a60","sum"),
        ae_improvement=("ae_improvement","sum")).reset_index()
    product["change"]=np.where(product.ae_improvement>0,"IMPROVED",np.where(product.ae_improvement<0,"DEGRADED","UNCHANGED"))
    tables["paired_product_change.csv"]=product
    comb=[]
    for keys,g in o.groupby(["shop","current_category","issue_month","direct_class"]):
        comb.append(dict(zip(["shop","current_category","issue_month","direct_class"],keys),
            n=len(g),actual_units=g.a60.sum(),ae60=g.direct_ae60.sum(min_count=1),
            ae_segments=g.direct_ae_segments.sum(min_count=1)))
    tables["combination_error_classes.csv"]=pd.DataFrame(comb)
    monthly_migrations=[]
    for issue,g in pair.groupby("issue_month"):
        monthly_migrations.append(dict(issue_month=issue,n=len(g),
            retained_hit=int((g.f28_joint_hit&g.direct_joint_hit).sum()),
            gained_hit=int((~g.f28_joint_hit&g.direct_joint_hit).sum()),
            lost_hit=int((g.f28_joint_hit&~g.direct_joint_hit).sum())))
    tables["paired_joint_migrations.csv"]=pd.DataFrame(monthly_migrations)
    # Exact high-error rows make claims traceable, not aggregate-to-aggregate joins.
    examples=[]
    for r in top.itertuples():
        z=o[o.shop.eq(r.shop)&o.current_category.eq(r.current_category)&o.issue_month.eq(r.issue_month)].copy()
        z["combination_label"]=f"{r.shop} / {r.current_category} / {r.issue_month}"
        examples.append(z.sort_values("direct_ae60",ascending=False).head(5))
    examples=pd.concat(examples)
    excols=["combination_label","shop","spu","issue_date","input_cutoff","target_start_1","target_end_60",
            "maturity","a1","a2","a60","direct_p1","direct_p2","direct_p60","direct_class","direct_ae60",
            "listing_sessions_sum_ratio","ad_spend_ratio","confirmed_stockout_interval"]
    tables["top_combination_examples.csv"]=examples[excols]
    # Explicit support coverage for error-rate ranking, without removing low-support rows.
    coverage=[]
    for model in ["F28","DIRECT30x2"]:
        q=rank1[rank1.model.eq(model)]
        coverage.append(dict(model=model,supported_groups=len(q),core_records=q.n.sum(),output_records=q.output_n.sum(),
            actual_units=q.actual_units.sum(),absolute_error=q.absolute_error.sum(),
            full_core_records=all_n,full_actual_units=o.a60.sum()))
    tables["ranking_support_coverage.csv"]=pd.DataFrame(coverage)
    # Report label screening separately from loss of common-stratum overlap.
    conditional_scope=[]
    dimensions={"category":"current_category","season":"current_season",
                "issue":"issue_month","calendar":"calendar_month"}
    rawpair=b[b.calendar_mature & b.paired & b.window.eq("d01_60")]
    for r in cond.itertuples():
        for side in ["a","b"]:
            value=getattr(r,"group_"+side)
            g=rawpair[rawpair[dimensions[r.comparison]].astype(str).eq(str(value))]
            raw_n=len(g);raw_units=g.actual.sum()
            eligible_n=getattr(r,"full_"+side+"_n");eligible_units=getattr(r,"full_"+side+"_units")
            kept_n=getattr(r,"kept_"+side+"_n");kept_units=getattr(r,"kept_"+side+"_units")
            conditional_scope.append(dict(comparison=r.comparison,group_a=r.group_a,group_b=r.group_b,
                side=side,group_value=value,raw_pair_n=raw_n,raw_pair_units=raw_units,
                label_eligible_n=eligible_n,label_eligible_units=eligible_units,
                label_excluded_n=raw_n-eligible_n,label_excluded_units=raw_units-eligible_units,
                overlap_kept_n=kept_n,overlap_kept_units=kept_units,
                overlap_excluded_n=eligible_n-kept_n,overlap_excluded_units=eligible_units-kept_units,
                coverage_of_raw_pair=kept_units/raw_units if raw_units>0 else np.nan,
                coverage_of_label_eligible=kept_units/eligible_units if eligible_units>0 else np.nan))
    tables["conditional_scope_coverage.csv"]=pd.DataFrame(conditional_scope)
    # Same-product survivors: all pairs retained, not claimed to remove all confounding.
    surv=pd.read_parquet(data/"survivor_details.parquet")
    sr=[]
    for (lag,old,new),g in surv.groupby(["lag_months","previous_issue_month","issue_month"]):
        row=dict(lag_months=lag,previous_issue_month=old,issue_month=new,n=len(g),
                 products=g[["shop","spu"]].drop_duplicates().shape[0],
                 previous_units=g.previous_actual.sum(),current_units=g.actual.sum())
        for name in ["f28","direct"]:
            row[name+"_previous_wape"]=(g["previous_"+name+"_prediction"]-g.previous_actual).abs().sum()/g.previous_actual.sum() if g.previous_actual.sum()>0 else np.nan
            row[name+"_current_wape"]=(g[name+"_prediction"]-g.actual).abs().sum()/g.actual.sum() if g.actual.sum()>0 else np.nan
        sr.append(row)
    tables["survivor_paired_metrics.csv"]=pd.DataFrame(sr)
    for name,frame in tables.items():frame.to_csv(out/name,index=False,encoding="utf-8-sig")
    known_cond=cond[cond.comparison.eq("category")&cond.group_a.isin(["T恤","背心/吊带","连体衣"])&
                    cond.group_b.isin(["T恤","背心/吊带","连体衣"])]
    seasons=cond[cond.comparison.eq("season")]
    season_main=seasons[seasons.group_a.eq("春夏")&seasons.group_b.eq("秋冬")].iloc[0]
    season_scope=tables["conditional_scope_coverage.csv"]
    season_scope=season_scope[season_scope.comparison.eq("season")&season_scope.group_a.eq("春夏")&season_scope.group_b.eq("秋冬")]
    raw_season_coverage=season_scope.set_index("side").coverage_of_raw_pair
    categoryraw=p60[p60.level.eq("category")].sort_values("direct_ae",ascending=False).head(8)
    seasonraw=p60[p60.level.eq("season")]
    rawmonth=p60[p60.level.eq("issue")]
    priority_stable=stable[stable.support.eq("SUPPORT_SCREEN_PASS")]
    r1=rank1[rank1.model.eq("DIRECT30x2")].head(8)
    r2=rank2[rank2.model.eq("DIRECT30x2")].head(10)
    repeat_summary=repeat.groupby(["shop","current_category"]).agg(
        compared_months=("calendar_month","size"),same_direction=("same_direction","sum"),
        both_improve=("both_improve","sum"),both_degrade=("both_degrade","sum")).reset_index()
    repeat_summary=repeat_summary.merge(pooled[["shop","current_category","support"]],on=["shop","current_category"],validate="one_to_one")
    tables["repeatability_summary.csv"]=repeat_summary
    repeat_summary.to_csv(out/"repeatability_summary.csv",index=False,encoding="utf-8-sig")
    conditional_month=cond[cond.comparison.eq("issue")&(
        (cond.group_a.eq("2025-05")&cond.group_b.eq("2025-11"))|
        (cond.group_a.eq("2025-04")&cond.group_b.eq("2026-04")))]
    known_m1=o[["issue_date","m1_train_dates","m1_train_span_days","m2_train_dates","m2_train_span_days"]].drop_duplicates()
    gained=int((~pair.f28_joint_hit&pair.direct_joint_hit).sum())
    lost=int((pair.f28_joint_hit&~pair.direct_joint_hit).sum())
    pchange=product.groupby("change").agg(products=("spu","size"),actual_units=("actual_units","sum")).reset_index()
    top_description="；".join(f"{r.shop}×{r.current_category}×{r.issue_month}：{r.absolute_error:,.0f}件" for r in top.itertuples())
    pooled_description="、".join(f"{r.shop}×{r.current_category}" for r in pooled.head(3).itertuples())
    result=[
        "# DIRECT与F28：季节、品类交叉诊断及优化优先级",
        "## 首页：五个问题的回答",
        f"1. **品类与签发月份有明确的描述性差异，不能把它们排成因果影响力。** 同对象配对下，DIRECT的主要品类WAPE见下表；控制店铺、签发年月和货龄后的具体差异见共同层结果。季节与品类的重合程度见第4节实数表，不能把两个标签当成独立因素。所有标签均为CURRENT_NOT_POINT_IN_TIME。",
        f"2. **可以识别同条件下的差异，不能独立分出季节、品类、经营的主次。** 春夏/秋冬在同店铺、年月、货龄及品类的共同层内，DIRECT加权WAPE分别为{season_main.direct_wape_a:.2%}和{season_main.direct_wape_b:.2%}；但只保留两组全部原配对实销的{raw_season_coverage['a']:.2%}/{raw_season_coverage['b']:.2%}。存在单条记录的小层，权重调整不是因果消混；年份、训练历史和商品构成仍共同变化。",
        f"3. **按互不重叠的店铺×品类×签发年月，DIRECT最大剩余60天绝对误差三组为：** {top_description}。这些是误差件数，不是独立采购损失。合并期间的{pooled_description}另构成主要研究范围，不能与月份单元重复累计。",
        f"4. **当前首要仍是总量偏差，分段分配也不能忽略。** DIRECT累计未命中的三类共{int(totalmiss.n.sum()):,}条，占其已知60天绝对误差{totalmiss.ae60.sum()/total_ae:.2%}；累计命中但分段不同时命中有{int(allocation.n):,}条，承载完整核心实销{allocation.actual_share:.2%}，分段绝对误差合计{allocation.ae_segments:,.0f}件。缺输出另有{int(unpred.n)}条、{unpred.actual_units:,.0f}件实销，不能当0误差。",
        "5. **下一次只优先测试：在DIRECT中受控加入签发前56/90天销量水平及其与近28天的变化信息，检验换季前后总量转折识别。** 不是给特定SPU抬高预测，也不是按当前季节标签加系数。冻结源码当前只显式使用28天内销量滞后、7/28天均值和日历；需要预登记单一输入增量、保留原DIRECT/F28及全核心范围，尤其保护真实衰退款。该方向是待验证假设，不是本轮已证明收益。",
        "## 1. 范围、原值与数量守恒",
        f"成熟范围为20期、{all_n:,}条对象×签发记录、{o[['shop','spu']].drop_duplicates().shape[0]}个不同店铺×SPU；共同可评分60天范围{len(pair):,}条。保留2026年9/10月NOT_MATURED，单列于not_matured.csv。原核心名单、预测和实际销量没有重选或改写。",
        f"共读取{len(b):,}条双模型唯一窗口底表；加入标签、经营证据、训练覆盖后不增行，真值和预测逐字段对账。当前品类原值‘未映射’{int(o.current_category.eq('未映射').sum())}条保留原文但视为语义未知；季节UNKNOWN {int(o.current_season.eq('UNKNOWN').sum())}条。冲突规则为CONFLICT，未任取末行；实际本批未发现冲突。背心与背心/吊带等原值未合并。",
        "窗口为S+1..30、S+31..60、S+1..60，输入截止S−2。base_windows.csv保留逐条target_start/target_end；签发年月不能称自然目标月。",
        table(p[p.level.eq("overall")],["window","full_n","pair_n","pair_products","pair_issues","pair_actual_units","f28_wape","direct_wape","delta_wape"],["f28_wape","direct_wape","delta_wape"]),
        "上表WAPE均在同一行的共同对象和同一实际销量分母计算；delta为DIRECT−F28，百分数差读作百分点。完整范围与共同集分别在layer_metrics.csv的FULL_CORE、MODEL_COMMON中保留。缺预测仍保留在完整核心命中分母，实销0不计算相对误差。",
        "## 2. DIRECT剩余误差：总量、分段与覆盖分开",
        table(cls,["model","error_class","n","products","actual_units","actual_share","ae60","ae60_share","ae_segments"],["actual_share","ae60_share"]),
        "类别互斥；MISSING_PREDICTION优先于ZERO_ACTUAL，同时另存any_zero_actual标记，所以DIRECT缺输出中可能包含零实销格。缺预测类别AE是未知，不是0。ae60与ae_segments是两个评价尺度，不能相加。",
        f"在同一{len(pair):,}条配对记录上，DIRECT高估件数比F28减少{globalp.f28_over-globalp.direct_over:,.0f}件，但低估增加{globalp.direct_under-globalp.f28_under:,.0f}件，净绝对误差减少{globalp.net_ae_improvement:,.0f}件。WAPE改善主要体现为减少过高预测，不代表多数款已进入容差。",
        table(tables["same_pair_error_classes.csv"],["model","error_class","n","actual_units","ae60","ae_segments"]),
        f"同配对范围三窗口联合命中：F28 {int(pair.f28_joint_hit.sum())}→DIRECT {int(pair.direct_joint_hit.sum())}；新增命中{gained}条，同时失去原命中{lost}条。完整范围仍是F28 {int(o.f28_joint_hit.sum())}、DIRECT {int(o.direct_joint_hit.sum())}，分母{all_n}不变。迁移见class_migrations.csv及paired_joint_migrations.csv，不把多窗口行重复计数。",
        table(pchange,["change","products","actual_units"]),
        "上表是将同一商品跨期的绝对误差差额汇总后，得到互斥的不同商品改善/退化数；并非每期都改善。layer_pairs.csv中的improved_n/degraded_n是对象×签发记录；其中不同商品计数允许跨期出现在两类，不可直接相加。",
        "## 3. 原始分层：标签、品类和月份",
        table(seasonraw,["current_season","full_n","pair_n","pair_products","pair_issues","pair_actual_units","f28_wape","direct_wape","delta_wape","support"],["f28_wape","direct_wape","delta_wape"]),
        table(categoryraw,["current_category","full_n","pair_n","pair_products","pair_actual_units","f28_wape","direct_wape","delta_wape","support"],["f28_wape","direct_wape","delta_wape"]),
        table(rawmonth,["issue_month","full_n","pair_n","pair_products","pair_actual_units","f28_wape","direct_wape","delta_wape"],["f28_wape","direct_wape","delta_wape"]),
        "完整层级含季节、品类、签发年月、品类×签发年月、店铺×品类×签发年月及货龄分层。每格的原范围、配对范围、不同商品、实销、误差、Bias、命中和覆盖在layer_metrics/layer_pairs中；三窗口联合命中单独置于layer_joint_once.csv，不跨窗口重复求和。小样本全部保留。",
        "## 4. 同条件比较：不是控制了标签就证明因果",
        "比较品类时保持店铺×签发年月×货龄相同；比较季节时再保持品类相同。只使用两组实际存在、实际销量分母为正的共同层。每层权重为两组该层实销之和÷全部共同层合计，两组及两模型用相同权重；没有补空单元，也没有按好坏选择层。",
        table(known_cond,["group_a","group_b","strata_n","kept_a_n","kept_b_n","a_unit_coverage","b_unit_coverage","direct_wape_a","direct_wape_b","f28_wape_a","f28_wape_b"],["a_unit_coverage","b_unit_coverage","direct_wape_a","direct_wape_b","f28_wape_a","f28_wape_b"]),
        table(seasons,["group_a","group_b","status","strata_n","kept_a_n","kept_b_n","a_products","b_products","a_unit_coverage","b_unit_coverage","direct_wape_a","direct_wape_b","support"],["a_unit_coverage","b_unit_coverage","direct_wape_a","direct_wape_b"]),
        "上表a_unit_coverage/b_unit_coverage以标签足够完成分层的配对实销为分母，不等于排除标签前的全部配对实销。以下逐步对账；完整的各条件比较见conditional_scope_coverage.csv。未知标签对象仍在原始分层与完整核心中，没有被删除。",
        table(season_scope,["group_value","raw_pair_n","raw_pair_units","label_excluded_n","label_excluded_units","label_eligible_n","overlap_kept_n","overlap_kept_units","coverage_of_raw_pair","coverage_of_label_eligible"],["coverage_of_raw_pair","coverage_of_label_eligible"]),
        "全年标签样本很少，与秋冬没有正分母共同层，不外推。春夏与秋冬虽达到展示支持筛查，但仅少量原样本实销进入共同层，最小层只有1条；不能将局部标准化差异推广到全部商品。",
        table(overlap[overlap.overlap.eq("category_season")&overlap.current_category.isin(["T恤","背心/吊带","连体衣"])],
              ["current_category","current_season","n","products","object_share","actual_share"],["object_share","actual_share"]),
        "dimension_overlap.csv另保存品类×店铺与季节×店铺。品类与季节存在结构重合，全年样本有限；即使同条件差异仍在，也不能按最大最小WAPE差排名因果影响力。",
        "月份比较保持店铺×品类×货龄相同；以下两组是对既有弱期/跨年变化的诊断例子，不是依据实际效果选择预测路线。",
        table(conditional_month,["group_a","group_b","kept_a_n","kept_b_n","a_unit_coverage","b_unit_coverage","direct_wape_a","direct_wape_b","f28_wape_a","f28_wape_b"],["a_unit_coverage","b_unit_coverage","direct_wape_a","direct_wape_b","f28_wape_a","f28_wape_b"]),
        "全部月份对照与精确共同层权重见conditional_comparisons.csv、conditional_weights.csv。相同商品的相邻/跨年辅助分析见survivor_paired_metrics.csv及survivor_details.parquet，仍有幸存者选择、训练增长、经营变化和重叠窗口，不独立识别季节效应。2025年9—12月没有第二个成熟评价年度。",
        "## 5. 两张榜及稳定性",
        "榜1：合并期间达到至少30条配对记录、10个不同商品、3个签发期的店铺×品类，按DIRECT误差率排序。SUPPORT_SCREEN_PASS不是显著性或稳定性证明。",
        table(r1,["shop","current_category","full_n","output_n","pair_n","pair_products","pair_issues","actual_units","scored_actual_units","absolute_error","wape","bias","missed_positive_units"],["wape","bias"]),
        "榜2：互不重叠的店铺×原品类×签发年月，按DIRECT绝对误差排序。只有此层用于正式累计贡献；单月不要求三期，但不能称跨期规律。",
        table(r2,["shop","current_category","issue_month","full_n","output_n","pair_n","products","actual_units","scored_actual_units","absolute_error","error_share","wape"],["error_share","wape"]),
        "两榜actual_units是完整核心实销；scored_actual_units才是该模型WAPE/Bias的实际销量分母，即同时有预测和真值的范围。本批DIRECT有输出的成熟对象均可与F28配对。缺输出对象保留在full_n、actual_units和完整范围未命中销量中，但没有人为补出AE。error_share仅分摊已知AE，不代表完整核心的潜在全部误差。",
        "低支持单元并未删除，见ranking_1_low_support_retained.csv。支持榜覆盖及排除范围见ranking_support_coverage.csv。完整榜包含全部已知误差，不把未预测对象的未知误差填0。源榜1的pair_n_x/pair_n_y分别来自指标层和支持层；已逐行检查完全一致，报告仅显示一列pair_n。",
        table(priority_stable[priority_stable.sensitivity.eq("ORIGINAL")],["shop","current_category","pair_n","products","periods","improve_periods","degrade_periods","top_error_spu","top_error_share","november_2025_error_share","delta_wape"],["top_error_share","november_2025_error_share","delta_wape"]),
        table(priority_stable[priority_stable.sensitivity.ne("ORIGINAL")],["shop","current_category","sensitivity","retained_n","retained_units","f28_wape","direct_wape","delta_wape"],["f28_wape","direct_wape","delta_wape"]),
        "剔除2025年11月或最大误差商品仅是敏感性旁表，不替代原成绩。最大商品误差占比是算术集中度；正改善贡献占比以各商品跨期净改善的正部分为分母，不冒充因果贡献。逐期改善/退化月份原文见combination_stability.csv。",
        table(repeat_summary[repeat_summary.support.eq("SUPPORT_SCREEN_PASS")],["shop","current_category","compared_months","same_direction","both_improve","both_degrade"]),
        "这里只检查2025/2026同日历月已存在的两期，核心商品与训练历史不相同。方向相同不代表重复独立验证。",
        "## 6. 重点经营线索和训练覆盖",
        table(examples[excols].head(15),["combination_label","spu","issue_date","maturity","a1","a2","direct_p1","direct_p2","direct_class","direct_ae60","listing_sessions_sum_ratio","ad_spend_ratio"]),
        "上述target/input比值只说明指标变化。Sessions是既有Listing行汇总，不是可任意跨子体去重人数；广告花费不是预算；库存零行不是断货持续时间。签发后实际指标没有回填模型。价格与转化按旧定义保留，不平均原行比率；经营指标缺失为UNKNOWN。",
        "既有经营档案没有可用的历史生效运营变更、真实预算或确证供给中断区间。本轮不另查数据库，不把这些缺口补齐成解释。operating_summary.csv给出各组各指标可用N和中位数，operating_details.csv保留逐款具体记录。",
        table(known_m1,["issue_date","m1_train_dates","m1_train_span_days","m2_train_dates","m2_train_span_days"]),
        "训练跨度、样本商品和年份同步变化；同商品跨年或同货龄比较不能完全分离这些因素。未做新的解释模型、SHAP、显著性筛选或固定训练长度网格。",
        "## 7. 后续实验方向（本轮不执行）",
        "**第一优先：签发前更长销量背景的单一输入增量。** 优先关注上述高贡献单元，同时保持全核心评价，不裁成这些成功/失败样本。第6节逐款记录提供总量偏差与经营变化的线索，不证明因果。原DIRECT每次签发所用的销量滞后与均值特征只覆盖最近28天，训练样本另有较长历史；拟在不改变资格、训练标签和损失的条件下加入56/90天水平及相对28天的变化，事前固定定义。不把未来流量增长当输入，不先规定抬高或压低系数。",
        "签发前销量日期可由归档构造，但本轮档案没有当时实际获取证明，只能做重建验证；若进入前瞻实验，须冻结真实获取时间与修订版本。可能伤害近期持续衰退、历史高位刚回落的商品，尤其已经减少高估的T恤/秋冬样本。必须同时检查高估增加、低估减少、分段和累计±20%、零值及缺输出，不以只修好重点款为成功。当前数据不能证明56/90天特征一定有效，也不能证明它能预测尚未计划的活动或供给变化。",
        f"**第二优先：总量与两个分段的联合约束诊断。** 面向累计命中但分段未同时命中的对象，下一轮可事前定义总量与分段比例的联合训练/评价方案；必须保持原两段真实日期、禁止用目标期shape_ratio进行路由。此问题承载较多销量，但60天绝对误差占比低于总量未命中，因此排在第二。可能损害现已同时命中的{int(o.direct_joint_hit.sum())}条或真实突变商品，不能把F28平坦分段当答案。具体机制和预算需另行预登记，本轮不拟合。",
        f"{int(unpred.n)}条原资格不足对象属于全店目标的覆盖缺口，需与既有新品范围单列后续验收，不擅自改DIRECT资格或更改NEW_VISIBLE。它们并未从名单消失。",
        "## 8. 验证、版本和复现",
        f"独立计算路径验收：{v['status']}。这是同一执行者用不同实现直接重读原始评分的复核，不冒称第二人审核；没有导入分析指标/分类帮助函数。已核验原始窗口记录、标签唯一性、关联守恒、逐对象分类、全分组、配对分母、共同层权重、联合命中一次计数及原来源哈希不变。",
        f"验证回执：../{validation.name}；分析清单：../run_manifest.json。新增拟合0、数据库查询0、生产变更0。参考提交34bdc3b703c45326f3c638814ad18595c8124e83。当前冻结DIRECT源码SHA256：{sha(sourcefile)}；FEATURES={json.dumps(features,ensure_ascii=False)}。只静态读取源码，未运行生产入口。来源哈希未变结论覆盖本轮实际消费的21个来源文件及清单锚点，不声称另行遍历了全部未消费旧模型归档。",
        "主报告和CSV是本轮正式交付，不新建应用、不要求浏览器验收。原归档完全只读；本轮初稿保留在父目录，reviewed修正了‘未映射’标签语义及空分组中位数提示，没有修改预测、真值或核心名单。具体文件、真实新提交、测试与复现命令见交付记录。",
    ]
    path=out/"DIRECT与F28：季节、品类交叉诊断及优化优先级.md"
    path.write_text("\n\n".join(result)+"\n",encoding="utf-8")
    receipt=dict(completed_at=datetime.now(timezone.utc).isoformat(),analysis_manifest_sha256=sha(data/"run_manifest.json"),
        validation_sha256=sha(validation),script_sha256=sha(__file__),frozen_direct_source_sha256=sha(sourcefile),
        fits=0,database_queries=0,production_changes=0,
        files={p.name:sha(p) for p in out.iterdir() if p.is_file()})
    (out/"report_manifest.json").write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({"report":str(path),"supplementary_tables":len(tables),"fits":0,"queries":0},ensure_ascii=False))


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--data",required=True);p.add_argument("--validation",required=True);p.add_argument("--out",required=True)
    run(**vars(p.parse_args()))
