"""Prepare a source-backed monthly replay report snapshot and authored content.

Requires completed, independently validated result files. No fitting, database
access, remote publication, package installation, or production changes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

try:
    from scripts.bc60_monthly_notebook import load_review, display_tables, sha, MODELS
except ModuleNotFoundError:
    from bc60_monthly_notebook import load_review, display_tables, sha, MODELS


CONTENT = r'''import React from "react";
import { ReportSection, RichNarrative, DataComponent, DataTable, EvidenceChart, useDataApp } from "../../data-app-public.jsx";

const percent=value=>value==null?"—":`${(Number(value)*100).toFixed(2)}%`;
const number=value=>value==null?"—":Number(value).toLocaleString("zh-CN",{maximumFractionDigits:0});
const words={d01_30:"1–30天",d31_60:"31–60天",d01_60:"1–60天",BEST:"最好",WORST:"最差",HIT:"命中",MISS:"未命中",UNJUDGED:"不可判断"};
const cols=(...items)=>items.map(([key,label,kind])=>({key,label,renderCell:value=>kind==="pct"?percent(value):kind==="num"?number(value):value==null?"—":words[value]||value}));
const colors={F28:"#53677d",WEEKDAY4:"#a3aeb8",B1:"#1b70a9",B2:"#c38a28",B3:"#8d6aa4",DIRECT30x2:"#8b9145"};

export function ReportContent(){
 const {snapshot,visible,appTitle,canEdit,mode,setAppTitle}=useDataApp();
 const rows=id=>snapshot.queries[id]?.rows||[];
 const meta=rows("meta")[0];
 const scoped=ids=>Object.fromEntries(ids.map(id=>[id,rows(id)]));
 const section=(id,title,ids,children)=>visible(id)&&<ReportSection id={id} title={title} queryId={ids[0]} queryIds={ids} sourceRowsByQuery={scoped(ids)}>{children}</ReportSection>;
 const table=(id,title,query,columns,data=rows(query),searchable=true)=><DataComponent id={id} title={title} queryId={query} kind="table" sourceRows={data} displayRows={data}><DataTable rows={data} columns={columns} searchable={searchable} caption={title}/></DataComponent>;
 return <article className="report-content monthly-report" aria-label="逐月预测效果与季节经营差异报告">
  <header className="report-hero"><h1 data-data-app-title contentEditable={canEdit&&mode==="edit"} suppressContentEditableWarning onBlur={canEdit&&mode==="edit"?event=>setAppTitle(event.currentTarget.textContent.trim()||appTitle):undefined}>{appTitle}</h1></header>
  {section("monthly-summary","结论先读",["meta","overall","joint","extremes"],<RichNarrative id="monthly-summary:body" value={meta.executive}/>)}
  {section("monthly-map","各签发月的误差地图",["monthly","cell_status"],<>
   <RichNarrative id="monthly-map:note" value="以下为完整核心范围中有完整真值且有预测的记录；缺预测和未成熟不补零。不同模型可评分对象可能不同，公平比较另看与F28的同对象配对。签发月不是自然目标月。"/>
   {["d01_30","d31_60","d01_60"].map(window=>{const selected=rows("monthly").filter(row=>row.window===window);return <EvidenceChart key={window} id={`monthly-wape-${window}`} queryId="monthly" title={`${words[window]} WAPE｜逐期可评分范围`} rows={selected} sourceRows={selected} spec={{type:"line",x:"issue_date",y:"wape (%)",series:"model",stackable:false,colors,valueDecimals:2}} height={330}/>;})}
   {table("monthly-grid-table","签发日、目标窗口与未计算原因","cell_status",cols(["issue_month","签发月"],["input_cutoff","输入截止"],["target_start","目标开始"],["target_end","目标结束"],["shop","店铺"],["model","模型"],["window","窗口"],["core_n","核心N","num"],["output_n","输出N","num"],["truth_n","真值N","num"],["cell_status","状态"]))}
  </>)}
  {section("monthly-population","完整名单、命中和缺失一起看",["overall","joint","selection"],<>
   <RichNarrative id="monthly-population:note" value="每期按截点前90天全店销量冻结名单。合并行是店铺×SPU×签发期记录，跨期重复不等于独立商品；相邻60天标签重叠。WAPE由高估与低估件数除以相应实际件数，不平均每个月的百分比。"/>
   {table("monthly-overall-table","成熟签发期合并指标","overall",cols(["model","模型"],["window","窗口"],["core_n","核心N","num"],["output_n","输出N","num"],["truth_n","真值N","num"],["scored_n","可评分N","num"],["wape","WAPE","pct"],["bias","Bias","pct"],["hit_n","命中N","num"],["hit_rate","正实销命中率","pct"],["unjudged_n","不可判断N","num"]))}
   {table("monthly-joint-table","三窗口同时命中","joint",cols(["issue_month","签发月"],["model","模型"],["core_n","核心N","num"],["HIT","同时命中","num"],["MISS","未同时命中","num"],["UNJUDGED","不可判断","num"],["joint_fixed_share","完整名单占比","pct"]))}
  </>)}
  {section("monthly-paired","与F28逐期同对象比较",["paired","common","extremes"],<>
   <RichNarrative id="monthly-paired:note" value="每个候选分别与F28在相同店铺、SPU、签发日、窗口和真值上配对。全模型共同集仅作辅助，不能替代完整名单或把缺预测对象删除。"/>
   {table("monthly-paired-table","候选相对F28的配对差异","paired",cols(["issue_month","签发月"],["model","候选"],["window","窗口"],["full_core_n","完整N","num"],["paired_n","配对N","num"],["candidate_wape","候选WAPE","pct"],["f28_wape","F28 WAPE","pct"],["wape_change","WAPE差","pct"],["improved_n","改善N","num"],["degraded_n","退化N","num"]))}
   {table("monthly-extremes-table","每个模型的最好与最差月份","extremes",cols(["model","模型"],["window","窗口"],["metric","衡量指标"],["extreme","类型"],["issue_month","签发月"],["value","指标值","pct"],["core_n","核心N","num"],["scored_n","可评分N","num"]))}
  </>)}
  {section("monthly-training","训练时间支持与拟合状态",["training"],<>
   <RichNarrative id="monthly-training:note" value="训练行数不能代替时间跨度；直接模型M1与M2分别要求标签完整成熟。重叠模拟日期不是独立样本，不能只凭商品行多宣称充分验证。complete_365_days仅表示365日源记录完整，包含明确0；不证明持续在售、供给正常或完整可比经营季节。"/>
   {table("monthly-training-table","各期模型训练覆盖","training",cols(["issue_date","签发日"],["kind","模型/分段"],["status","状态"],["train_rows","训练行","num"],["train_objects","训练对象","num"],["train_dates","训练日期数","num"],["train_start","训练起点"],["train_end","训练终点"],["train_span_days","跨度日数","num"],["max_global_nonoverlap","非重叠窗口","num"]))}
  </>)}
  {section("monthly-shape","F28的等量两段为什么可能同时失准",["shape"],<>
   <RichNarrative id="monthly-shape:note" value="F28两段预测都等于30×此前28天日均，60天等于两段之和。只有两段实销都为正才计算形状比；shape_ratio>1.5时，任何两段等量预测都无法同时进入±20%。这只是事后形状诊断，不是季节原因已确认，也不能回填为模型选择规则。"/>
   <EvidenceChart id="monthly-f28-shape" queryId="shape" title="两段实销比超过1.5的对象与销量占比" rows={rows("shape")} sourceRows={rows("shape")} spec={{type:"line",x:"issue_date",y:"objectShare",fields:["objectShare","unitShare"],stackable:false,colors:{objectShare:"#1b70a9",unitShare:"#c38a28"},legend:{labels:{objectShare:"正实销双段对象占比",unitShare:"正实销双段销量占比"}},valueDecimals:2}} height={340}/>
   {table("monthly-shape-table","F28形状限制与未知状态","shape",cols(["issue_month","签发月"],["core_n","核心N","num"],["positive_both_n","双段正实销N","num"],["incompatible_shape_n","形状不兼容N","num"],["object_share","对象占比","pct"],["sales_share_positive_both","销量占比","pct"],["zero_or_unknown_n","零或未知N","num"]))}
  </>)}
  {section("monthly-coverage","选款覆盖与目标期覆盖不可混用",["selection","future"],<>
   {table("monthly-selection-table","选款输入完整性","selection",cols(["issue_month","签发月"],["shop","店铺"],["selection_status","状态"],["core_n","核心N","num"],["full_shop_units","全店选款期件数","num"],["selected_units","已选件数","num"],["unmapped_units","映射不明件数","num"],["selection_coverage","选款覆盖","pct"]))}
   {table("monthly-future-table","冻结名单的目标期实销覆盖","future",cols(["issue_date","签发日"],["shop","店铺"],["window","窗口"],["status","来源状态"],["core_n","核心N","num"],["complete_window_core_n","完整真值N","num"],["complete_window_core_units","完整核心实销","num"],["observed_shop_units","已观察全店实销","num"],["coverage","完整窗口覆盖","pct"]))}
  </>)}
  {section("monthly-limits","经营诊断与真实性边界",["meta"],<RichNarrative id="monthly-limits:body" value={meta.limits}/>)}
  {rows("native_extension").length>0&&section("monthly-native-v4","V4原生自然月扩展｜独立旁表",["native_extension"],<>
   <RichNarrative id="monthly-native-v4:note" value="仅补充V4缺失的原生目标自然月，成熟可观察4月以上范围，不是日级90%核心名单。A16未重新校准。当前归档季节及映射不证明历史截点可获取；不能将此表与日级60天或不同范围的A16拼成排名。独立旁表核验已通过：真值、资格、日期、覆盖、判定、汇总和哈希；核验没有独立重新计算V4预测，不等于预测公式第二次复现。"/>
   {table("monthly-native-v4-table","新增V4自然月与覆盖","native_extension",cols(["target_month","目标自然月"],["horizon","原生H"],["eligible_n","成熟N","num"],["output_n","输出N","num"],["truth_n","实销N","num"],["scored_n","可评分N","num"],["wape","WAPE","pct"],["bias","Bias","pct"],["hit20_n","命中N","num"],["unknown_actual_n","未知实销N","num"]))}
  </>)}
 </article>;
}
'''

CSS = '''
.report-page { --data-app-layout-intent: authored-report; --data-app-content-width: 1160px; }
.monthly-report { display: grid; gap: 34px; }
.monthly-report .report-hero h1 { line-height: 1.18; }
.monthly-report .report-section { min-width: 0; }
'''


def records(frame):
    # pandas' JSON serializer retains unknown numeric fields as null, not zero.
    return json.loads(frame.to_json(orient='records', force_ascii=False, date_format='iso'))


def generate(root, validation, out):
    review = load_review(root, validation)
    out = Path(out).resolve()
    if out.exists():
        raise ValueError('OUTPUT_ALREADY_EXISTS')
    out.mkdir(parents=True)
    frames, meta = review['frames'], review['meta']
    tables = display_tables(review)
    grid = frames['grid']
    mature = grid.calendar_mature.astype(str).str.lower().eq('true')
    monthly = tables['primary'].copy()
    monthly['issue_date'] = monthly.issue_month + '-02'
    monthly['wape (%)'] = monthly.wape * 100
    shape = tables['shape_display'].copy()
    shape['objectShare'] = shape.object_share
    shape['unitShare'] = shape.sales_share_positive_both
    primary60 = tables['summary'][tables['summary'].window.eq('d01_60')]
    lines = []
    for row in primary60.itertuples():
        wape = f'{row.wape:.2%}' if pd.notna(row.wape) else 'N/A'
        lines.append(f'{row.model}：可评分 {int(row.scored_n):,}/{int(row.core_n):,}，WAPE {wape}')
    executive = (f"- 本轮保留{len(grid)}个签发月，完整60天已到期{int(mature.sum())}期；正式学习型拟合{meta['effects']['formal_fit_count']}次。\n\n"
                 "- 成熟签发期60天合并结果（各模型可评分范围不同，不据此作公平总排名）：" + '；'.join(lines) + '。\n\n'
                 "- 每月核心名单不同，三窗口同时命中、缺预测和真值覆盖均保留。相邻60天窗口重叠，累计件数不是独立销量或采购损失。\n\n"
                 "- 这是扩展历史重训回放，不是独立前瞻OOS，不自动选择或部署任何生产模型。")
    limits = (f"- 数值核验状态：{meta['validation']['status']}。日源业务范围为{frames['input_quality']['first_day']}至{frames['input_quality']['last_day']}，实际获取时间与业务日期分别归档。\n\n"
              "- 2024/2025数据为2026年取得的历史重建；源行创建/修改时间不能证明历史截点已可获取。未知不补0，当前品类不能回填为历史模型输入。\n\n"
              "- 流量、花费、促销和库存观测仅是经营线索，不证明预算决策、在售状态或整段断货；详细经营诊断见随附本地证据。\n\n"
              "- V4/A16自然月旁表、原385条复核及R28滚动更新保持原范围，不混入首次签发60天排名。\n\n"
              "- 本地报告读取冻结文件，不访问数据库、不训练、不刷新BI、不发布到外网。浏览器点击验收状态另行记录，不能由构建成功替代。")
    datasets = {
        'meta': pd.DataFrame([{'executive': executive, 'limits': limits}]),
        'monthly': monthly, 'overall': tables['summary'], 'joint': tables['joint_display'],
        'paired': tables['pair_display'], 'common': frames['common'], 'extremes': tables['extremes'],
        'training': frames['training'], 'shape': shape, 'cell_status': frames['cell_status'],
        'selection': frames['selection'], 'future': frames['future'][frames['future'].cohort_id.eq('monthly_core')],
    }
    if 'v4_extension' in frames:
        extension = frames['v4_extension']
        datasets['native_extension'] = extension[extension.group_by.eq('target_month+horizon')]
    origins = {
        'meta': ['scoring/metrics.csv','scoring/joint_summary.csv','inputs/quality.json','effects/run_manifest.json'],
        'monthly': ['scoring/metrics.csv'], 'overall': ['scoring/metrics.csv'],
        'joint': ['scoring/joint_summary.csv'], 'paired': ['scoring/paired_f28.csv'],
        'common': ['scoring/all_model_common.csv'], 'extremes': ['scoring/metrics.csv'],
        'training': ['effects/training_coverage.csv'], 'shape': ['scoring/f28_shape_summary.csv'],
        'cell_status': ['scoring/full_issue_status_grid.csv'], 'selection': ['prepared/selection_quality.csv'],
        'future': ['scoring/future_coverage.csv'],
        'native_extension': ['native_v4_extension/native_v4_summary.csv','native_v4_extension/execution_manifest.json',
                             'validation_native_v4/validation.json'],
    }
    queries = {}
    for name, frame in datasets.items():
        source = {'type': 'reviewed-local-archive', 'label': '扩展历史重训回放，已完成独立数值核验',
            'files': origins[name], 'executedAt': meta['scoring'].get('finished_at'),
            'asOf': frames['input_quality']['last_day'], 'timezone': 'Asia/Shanghai',
            'evidenceFlow': [{'title': '版本与来源', 'detail': '只读复用已冻结日源、月参考及各期名单；新历史与旧段不覆盖合并。'},
                             {'title': '数值核验', 'detail': str(meta['validation']['status'])},
                             {'title': '展示变换', 'detail': '使用预计算评分；合并WAPE从高估+低估件数及实际件数重算；缺值保留null。'}],
            'limitations': ['重建历史回放，不证明点时可获取性', '每期完整核心范围不同；窗口重叠不是独立样本',
                            '跨模型误差应采用与F28的同对象配对，不用全模型交集取代完整核心']}
        definitions = [
            {'label': 'WAPE', 'definition': '在有预测且完整真值的同范围记录上，绝对误差件数合计除以实际件数合计；不平均逐款或各月百分比。'},
            {'label': '正实销±20%命中率', 'definition': '误差不超过实际值20%的正实销记录数除以固定核心中正实销记录数；缺预测不从固定分母删除。'},
            {'label': '三窗口同时命中', 'definition': '同一签发的1–30、31–60、1–60天均命中；缺输出或缺真值保持不可判断。'}]
        source['metricDefinitions'] = (definitions[:2] if name in ('meta','monthly','overall','paired','common','extremes','cell_status')
                                       else definitions[2:] if name == 'joint' else [])
        if name == 'native_extension':
            source['label'] = '冻结V4原生自然月重建回放；独立范围旁表'
            source['executedAt'] = frames['v4_extension_meta']['finished_at']
            source['asOf'] = frames['v4_extension_meta']['source_quality']['last_month']
            source['evidenceFlow'][1]['detail'] = (frames['v4_extension_validation']['status'] +
                '；独立核对真值、资格、日期、覆盖、判定、汇总和哈希，没有独立重新生成V4预测。')
            source['limitations'] = ['成熟可观察4月以上，不是日级90%核心名单',
                '当前季节/映射及事后获取输入，不证明历史点时可获取',
                'A16未扩展；不与不同范围的旧成绩或日级60天排名混合']
        queries[name] = {'rows': records(frame), 'source': source}
    snapshot = {'surface': 'report', 'title': '2025年起逐月预测效果与季节经营差异',
        'generatedAt': dt.datetime.now(dt.timezone.utc).isoformat(), 'buildStatus': 'creating',
        'status': 'reviewed', 'report': {'asOf': frames['input_quality']['last_day'], 'audience': '离线预测实验评审'},
        'filters': [], 'queries': queries}
    (out / 'reviewed_snapshot.json').write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    (out / 'ReportContent.jsx').write_text(CONTENT, encoding='utf-8')
    (out / 'report_additions.css').write_text(CSS, encoding='utf-8')
    manifest = {'status': 'REVIEWED_SNAPSHOT_AND_AUTHORED_CONTENT_PREPARED',
        'created_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'generator_sha256': sha(__file__),
        'source_hashes': review['hashes'], 'new_fits': 0, 'new_queries': 0, 'published': False,
        'files': {p.name: sha(p) for p in out.iterdir() if p.is_file()},
        'next_step': 'Official prepare-data-app --surface report --snapshot reviewed_snapshot.json, read copied AGENTS, then author and build.'}
    (out / 'report_preparation.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    return {key: value for key, value in manifest.items() if key != 'source_hashes'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    print(json.dumps(generate(**vars(parser.parse_args())), ensure_ascii=False, indent=2))
