"""Package independently verified V3 results for the official local report app.

No model fitting, source queries, or business-result constants. Run evidence
first, prepare the official starter, then use --author to install content only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def records(frame):
    return json.loads(frame.to_json(orient="records", date_format="iso", double_precision=15))


def pct(value):
    return f"{value * 100:.2f}%"


def pp(value):
    return f"{value * 100:+.2f}个百分点"


def package(root, archive, validation, refresh=False):
    root, archive, validation = map(Path, (root, archive, validation))
    out = root / "report_evidence"
    if out.exists() and not refresh:
        raise ValueError("new report evidence directory required")
    vm = read_json(validation)
    if vm["status"] != "PASS_INDEPENDENT_V3_RECONCILIATION":
        raise ValueError("final independent validation must pass")
    for name, digest in vm["effects"]["effect_hashes"].items():
        if sha(root / "effects" / name) != digest:
            raise ValueError(f"verified effects changed: {name}")
    coverage_path = archive / "effects_v1" / "coverage.csv"
    if sha(coverage_path) != vm["v1_hashes"]["effects_v1\\coverage.csv"]:
        raise ValueError("original coverage changed")
    training = pd.read_csv(root / "prepared" / "coverage.csv")
    summary = pd.read_csv(root / "effects" / "summary.csv")
    scores = pd.read_csv(root / "effects" / "scores.csv")
    paired = pd.read_csv(root / "effects" / "paired_summary.csv")
    migrations = pd.read_csv(root / "effects" / "joint_migrations.csv")
    joint = pd.read_csv(root / "effects" / "joint_hits.csv")
    details = pd.read_csv(root / "effects" / "paired_details.csv")
    base_cov = pd.read_csv(coverage_path)
    cov = base_cov[base_cov.window.eq("d01_60")].groupby("issue_date").agg(
        fixed_n=("fixed_core_n", "sum"), selection_core=("selection_core_units", "sum"),
        selection_total=("selection_total_units", "sum"), future_core=("target_observed_core_units", "sum"),
        future_shop=("target_observed_shop_units", "sum")).reset_index()
    cov["selection_coverage"] = cov.selection_core / cov.selection_total
    cov["future_coverage"] = cov.future_core / cov.future_shop
    migration = migrations.groupby(["scope", "migration"]).agg(
        object_n=("spu", "size"), actual_units=("actual60_f28", "sum")).reset_index()
    joint_summary = joint.groupby("model").agg(
        fixed_n=("spu", "size"), hit_n=("joint_decision", lambda s: s.eq("HIT").sum()),
        miss_n=("joint_decision", lambda s: s.eq("MISS").sum()),
        unjudged_n=("joint_decision", lambda s: s.eq("UNJUDGED").sum()),
        actual_units=("actual60", "sum")).reset_index()
    joint_summary["hit_rate"] = joint_summary.hit_n / joint_summary.fixed_n
    fair = summary[summary.scope.eq("original_b1_369") & summary.slice.eq("overall")]
    full = summary[summary.scope.eq("full_core") & summary.slice.eq("overall")]
    period = summary[summary.scope.eq("original_b1_369") & summary.slice.eq("issue")]
    pairs = paired[paired.scope.eq("original_b1_369") & paired.slice.eq("overall")]
    period_pairs = paired[paired.scope.eq("original_b1_369") & paired.slice.eq("issue")]
    risk = details[details.scope.eq("full_core") & details.window.eq("d31_60")].copy()
    risk = risk[risk.change.eq("DEGRADED")].sort_values("absolute_error_change", ascending=False).head(10)
    fair_n = int(vm["effects"]["original_b1_eligible"])
    full_n = int(vm["fixed_core_records"])
    gap_n = int(vm["effects"]["original_gap"])
    def metric(model, window):
        selected = fair[fair.model.eq(model) & fair.window.eq(window)]
        if len(selected) != 1:
            raise ValueError("unexpected fair summary grain")
        return selected.iloc[0]
    f60, d60 = metric("F28", "d01_60"), metric("DIRECT30x2", "d01_60")
    f31, d31 = metric("F28", "d31_60"), metric("DIRECT30x2", "d31_60")
    p31 = pairs[pairs.window.eq("d31_60")].iloc[0]
    mig = migration[migration.scope.eq("full_core")].set_index("migration").object_n.to_dict()
    new, exited = int(mig.get("NEW_HIT", 0)), int(mig.get("EXITED_HIT", 0))
    jdict = joint_summary.set_index("model").hit_n.to_dict()
    observed_delta = int(jdict["DIRECT30x2"] - jdict["F28"])
    lost_unknown = observed_delta - (new - exited)
    time_parts = []
    for issue, g in training.groupby("issue_date"):
        g = g.set_index("segment")
        time_parts.append(f"{issue[5:7]}月 M1/M2 各有{int(g.loc['M1', 'train_issue_dates'])}/{int(g.loc['M2', 'train_issue_dates'])}个模拟签发日；最多{int(g.loc['M1', 'max_global_nonoverlap'])}/{int(g.loc['M2', 'max_global_nonoverlap'])}个互不重叠30天标签窗口")
    limited = training[training.support_status.eq("LIMITED_TEMPORAL_SUPPORT")]
    limited_note = "；".join(f"{r.issue_date[5:7]}月{r.segment}仅{int(r.max_global_nonoverlap)}个互不重叠标签窗口，时间支持有限" for r in limited.itertuples())
    limited_note = limited_note or "各组时间支持均已单列，不能据横截面记录多而宣称独立时间验证"
    period_notes = []
    for issue, g in period_pairs[period_pairs.window.eq("d01_60")].groupby("issue_date"):
        delta = float(g.iloc[0].wape_change)
        period_notes.append(f"{issue[5:7]}月60天WAPE差{pp(delta)}")
    improvements = int(period_pairs[period_pairs.window.eq("d01_60")].wape_change.lt(0).sum())
    periods = int(period_pairs[period_pairs.window.eq("d01_60")].issue_date.nunique())
    gain = d60.wape < f60.wape and observed_delta > 0
    b1_gain = d60.wape < metric("B1", "d01_60").wape
    partial = b1_gain or abs(d31.bias) < abs(f31.bias)
    conclusion = ("本次回放支持继续验证，但不支持上线" if gain else
                  "仅部分支持该结构；本轮没有证据支持替代F28" if partial else
                  "本轮未见所评估方向的总体收益；没有证据支持替代F28")
    answers = [
        {"question": "1. 真正可用的训练时间窗口有多少？", "answer": "；".join(time_parts) + "。重叠日期不是独立样本；" + limited_note + "。"},
        {"question": "2. DIRECT30x2胜过同口径F28吗？", "answer": f"在相同{fair_n}条原B1可用记录上，60天WAPE：F28 {pct(f60.wape)}，DIRECT {pct(d60.wape)}，差{pp(d60.wape-f60.wape)}；" + ("整体误差降低。" if d60.wape < f60.wape else "整体误差没有降低。")},
        {"question": "3. 后30天高估与逐款误差改善吗？", "answer": f"同口径Bias从{pct(f31.bias)}变为{pct(d31.bias)}，但WAPE从{pct(f31.wape)}变为{pct(d31.wape)}；{int(p31.improved_n)}条误差改善、{int(p31.degraded_n)}条退化，分别承载{pct(p31.improved_sales_share)}/{pct(p31.degraded_sales_share)}的实销。总量偏差与逐款准确度必须分开判断。"},
        {"question": "4. 三窗口同时命中净增多少、退出多少？", "answer": f"完整{full_n}条名单：F28 {int(jdict['F28'])}条同时命中，DIRECT {int(jdict['DIRECT30x2'])}条，已知命中数差{observed_delta:+d}条；可判断迁移中新命中{new}条、退出{exited}条，净{new-exited:+d}条。另有{int(mig.get('UNJUDGED',0))}条迁移不可判断，不把它们归为退出；完整名单命中差中另有{lost_unknown:+d}条来自这组未判断记录。"},
        {"question": "5. 三个签发期结论一致吗？", "answer": "；".join(period_notes) + f"。{periods}期中{improvements}期60天WAPE低于F28。" + limited_note + "；不能据横截面记录多而宣称充分时间验证。"},
        {"question": "6. 这个方向值得继续吗？", "answer": conclusion + f"。相对原递归B1的60天WAPE差为{pp(d60.wape-metric('B1', 'd01_60').wape)}，仍须单独判断是否胜过F28；三期均为已研究的历史重建回放，不是独立OOS，不能证明全店核心产品两个月±20%的前瞻目标已经实现。"},
    ]
    executive = "\n\n".join([
        f"- {conclusion}。同口径{fair_n}条记录的60天WAPE变化为{pp(d60.wape-f60.wape)}。",
        f"- 完整名单的三窗口同时命中由{int(jdict['F28'])}条变为{int(jdict['DIRECT30x2'])}条；候选仍有{gap_n}条未预测，不等同于已命中或数值零。",
        f"- 训练时间支持见分期覆盖表；末期核心名单覆盖未来全店销量的{pct(cov.iloc[-1].future_coverage)}。结果仅限历史回放。",
    ])
    frames = {
        "answers": pd.DataFrame(answers), "fair": fair, "full": full,
        "training": training, "period": period, "paired": pairs,
        "migration": migration, "joint": joint_summary, "coverage": cov,
        "strata": summary[summary.scope.eq("original_b1_369") & summary.slice.isin(["shop", "maturity", "lifecycle"])],
        "risks": risk,
        "meta": pd.DataFrame([{"fixed_n": full_n, "fair_n": fair_n, "gap_n": gap_n,
            "common_n": int(vm["effects"]["common_complete_records"]), "executive": executive,
            "conclusion": conclusion, "validation_status": vm["status"],
            "formal_fits": vm["effects"]["formal_fit_count"],
            "max_truth_difference": vm["effects"]["max_truth_difference"],
            "max_prediction_difference": vm["effects"]["max_prediction_difference"]}]),
    }
    out.mkdir(exist_ok=refresh)
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    queries = {}
    for name, frame in frames.items():
        frame.to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")
        queries[name] = {"rows": records(frame), "source": {
            "type": "reviewed-local-archive", "label": "BC60 V3 独立核验历史回放",
            "files": [f"report_evidence/{name}.csv"], "executedAt": vm["finished_at"],
            "asOf": "2026-10-01", "timezone": "Asia/Shanghai",
            "evidenceFlow": [
                {"title": "冻结归档", "detail": "只读复用V1日真值、核心名单及覆盖记录；辅助对照为原B1与V2 D1归档。"},
                {"title": "同对象同窗口", "detail": "全名单保留未预测；四模型误差对比使用原B1可用共同记录，基线在此子集重算。"},
                {"title": "独立核验", "detail": vm["status"] + "；报告读取结果前再次校验已验收输出哈希。"},
            ],
            "metricDefinitions": [
                {"label": "WAPE / Bias", "definition": "同窗口可评分对象的绝对误差合计/实销合计；Bias为预测减实销的有符号合计/实销合计。不是逐款命中率。"},
                {"label": "±20%命中", "definition": "实际为正且预测已知，abs(预测−实际)/实际≤20%；零实销相对误差未定义，缺输出不可判断。"},
                {"label": "三窗口同时命中", "definition": "1–30、31–60与1–60三个目标均命中；任一输出未知不能判通过。"},
                {"label": "历史时间支持", "definition": "模拟签发日通常重叠；最多互不重叠30天标签窗口是描述性时间覆盖，不是独立样本量。"},
            ],
        }, "methods": [{"language": "text", "code": "Read independently reconciled effects CSVs. Select recorded scope/slice; preserve unrounded values. Group additive migration counts and sum coverage numerators/denominators. Compute narrative directly from those reviewed cells; no fitting or new queries."}]}
    snapshot = {"surface": "report", "title": "BC60 V3直接分段预测实际效果报告",
        "generatedAt": now, "buildStatus": "creating", "status": "reviewed",
        "report": {"asOf": "2026-10-01", "audience": "预测实验评审"},
        "filters": [], "queries": queries}
    (out / "snapshot.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    (out / "findings.json").write_text(json.dumps({"executive": executive, "answers": answers}, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "manifest.json").write_text(json.dumps({"created_at": now,
        "script_sha256": sha(__file__), "validation_sha256": sha(validation),
        "run_manifest_sha256": sha(root / "effects" / "run_manifest.json"),
        "files": {p.name: sha(p) for p in out.iterdir() if p.is_file() and p.name != "manifest.json"}}, indent=2), encoding="utf-8")
    print(json.dumps({"status": "PREPARED", "snapshot": str(out / "snapshot.json")}, ensure_ascii=False))


CONTENT = r'''import React from "react";
import { ReportSection, RichNarrative, DataComponent, DataTable, SortableRegion, SortableItem, useDataApp } from "../../data-app-public.jsx";

const percent = x => x == null ? "—" : `${(Number(x)*100).toFixed(2)}%`;
const number = x => x == null ? "—" : Number(x).toLocaleString("zh-CN", {maximumFractionDigits: 0});
const labels = {d01_30:"1–30天",d31_60:"31–60天",d01_60:"1–60天",full_core:"完整名单",original_b1_369:"原B1可用同口径",common_complete:"完整共同子集",RETAINED_HIT:"保留命中",NEW_HIT:"新命中",EXITED_HIT:"退出命中",BOTH_MISS:"均未命中",UNJUDGED:"不可判断"};
const field = (key,label,kind) => ({key,label,renderCell:value=>kind === "pct" ? percent(value) : kind === "num" ? number(value) : labels[value] || value || "—"});
const cols = (...items) => items.map(x=>field(...x));

export function ReportContent() {
  const {snapshot,visible,appTitle,canEdit,mode,setAppTitle}=useDataApp();
  const rows = id => snapshot.queries[id]?.rows || [];
  const meta=rows("meta")[0];
  const table=(id,title,query,columns,selected=rows(query),searchable=false)=><DataComponent id={id} title={title} queryId={query} kind="table" sourceRows={selected} displayRows={selected}>
    <DataTable rows={selected} columns={columns} searchable={searchable} caption={title}/>
  </DataComponent>;
  const sections=["v3-training","v3-fair","v3-full","v3-period","v3-paired","v3-joint","v3-strata","v3-coverage","v3-risks","v3-limits"];
  const scoped=ids=>Object.fromEntries(ids.map(id=>[id,rows(id)]));
  const wrap=(id,title,query,children)=>{const ids=id==="v3-joint"?["joint","migration"]:[query];return <SortableItem id={id} label={title} kind="narrative"><ReportSection id={id} title={title} queryId={query} queryIds={ids} sourceRowsByQuery={scoped(ids)}>{children}</ReportSection></SortableItem>;};
  return <article className="report-content v3-report" aria-label="BC60 V3历史回放报告">
    <header className="report-hero">
      <h1 data-data-app-title contentEditable={canEdit&&mode==="edit"} suppressContentEditableWarning onBlur={canEdit&&mode==="edit"?e=>setAppTitle(e.currentTarget.textContent.trim()||appTitle):undefined}>{appTitle}</h1>
      <RichNarrative id="v3:deck" value="两个独立30天累计模型；只读归档历史回放。真值截止2026-10-01，非独立OOS，不是生产选型或部署建议。" className="report-deck"/>
    </header>
    {visible("v3-executive")&&<ReportSection id="v3-executive" title="结论先读" queryId="meta" queryIds={["meta","fair","joint","coverage"]} sourceRowsByQuery={scoped(["meta","fair","joint","coverage"])}><RichNarrative id="v3-executive:body" value={meta.executive}/></ReportSection>}
    {visible("v3-answers")&&<ReportSection id="v3-answers" title="首页六问" queryId="answers" queryIds={["answers","training","fair","paired","migration","joint","period"]} sourceRowsByQuery={scoped(["answers","training","fair","paired","migration","joint","period"])}>
      {rows("answers").map((r,i)=><RichNarrative key={i} id={`v3-answer:${i+1}`} value={`### ${r.question}\n\n${r.answer}`.replaceAll("\\n","\n")}/>)}</ReportSection>}
    <SortableRegion id="v3:sections" label="报告证据章节" variant="stack" authoredOrder={sections}>
      {visible("v3-training")&&wrap("v3-training","训练行数不能替代时间覆盖","training",<>
        <RichNarrative id="v3-training:note" value="训练使用四店合格上下文，不限核心名单。M1/M2分别检查标签成熟；资格只看当时已结束月份，销量特征只到模拟签发日减2天。下表的模拟日期高度重叠，非独立样本。"/>
        {table("v3-training-table","各期训练覆盖","training",cols(["issue_date","签发日"],["segment","分段"],["train_rows","训练行","num"],["train_objects","对象","num"],["train_issue_dates","模拟日期","num"],["first_simulated_issue","首模拟日"],["last_simulated_issue","末模拟日"],["max_global_nonoverlap","非重叠窗口","num"],["support_status","时间支持"]))}
      </>)}
      {visible("v3-fair")&&wrap("v3-fair",`四模型公平对比：同一${meta.fair_n}条记录`,"fair",<>
        <RichNarrative id="v3-fair:note" value={`F28、原B1、诊断性D1与DIRECT30x2均在原B1可用${meta.fair_n}条记录上比较。当前完整共同子集也是${meta.common_n}条；不能与完整名单F28的WAPE直接混比。D1不是生产候选。`}/>
        {table("v3-fair-table","同口径误差与命中","fair",cols(["window","窗口"],["model","模型"],["scored_n","评分数","num"],["wape","WAPE","pct"],["bias","Bias","pct"],["over_units","高估件数","num"],["under_units","低估件数","num"],["hit_n","命中数","num"],["hit_rate","命中率","pct"],["hit_sales_share","命中销量占比","pct"]))}
      </>)}
      {visible("v3-full")&&wrap("v3-full",`完整${meta.fixed_n}条名单：缺口不删除`,"full",<>
        <RichNarrative id="v3-full:note" value={`F28保留完整名单；原B1、D1与DIRECT的${meta.gap_n}条未预测仍计入固定分母。未知不是零，也不属于已评分未命中。此处只读状态与命中分母；跨模型WAPE公平比较见上一节。`}/>
        {table("v3-full-table","完整名单状态","full",cols(["window","窗口"],["model","模型"],["fixed_n","固定数","num"],["output_n","输出数","num"],["hit_n","命中","num"],["miss_n","未命中","num"],["unjudged_n","不可判断","num"],["hit_rate","完整分母命中率","pct"],["zero_actual_n","零实销","num"]))}
      </>)}
      {visible("v3-period")&&wrap("v3-period","逐期对比：不以合并值掩盖差异","period",table("v3-period-table","分签发期误差","period",cols(["issue_date","签发日"],["window","窗口"],["model","模型"],["scored_n","评分数","num"],["wape","WAPE","pct"],["bias","Bias","pct"],["hit_n","命中","num"]),rows("period"),true))}
      {visible("v3-paired")&&wrap("v3-paired","逐款改善与退化：DIRECT相对F28","paired",table("v3-paired-table","配对对象误差变化","paired",cols(["window","窗口"],["paired_n","配对数","num"],["improved_n","改善数","num"],["degraded_n","退化数","num"],["unchanged_n","不变数","num"],["improved_sales_share","改善销量权重","pct"],["degraded_sales_share","退化销量权重","pct"],["wape_change","WAPE差","pct"])))}
      {visible("v3-joint")&&wrap("v3-joint","三窗口同时命中及迁移","joint",<>
        <RichNarrative id="v3-joint:note" value="完整名单的已知命中差与可判断配对的净迁移不是同一分母。不可判断记录保留独立状态，不能计为退出命中或均未命中。"/>
        {table("v3-joint-table","完整名单同时命中","joint",cols(["model","模型"],["fixed_n","固定数","num"],["hit_n","同时命中","num"],["miss_n","未同时命中","num"],["unjudged_n","不可判断","num"],["hit_rate","完整名单占比","pct"]))}
        {table("v3-migration-table","F28到DIRECT的命中迁移","migration",cols(["scope","范围"],["migration","迁移"],["object_n","记录数","num"],["actual_units","承载实销","num"]))}
      </>)}
      {visible("v3-strata")&&wrap("v3-strata","店铺、原货龄与原资格分层","strata",table("v3-strata-table","同口径分层指标","strata",cols(["slice","分层"],["shop","店铺"],["maturity","原货龄"],["lifecycle","原资格"],["window","窗口"],["model","模型"],["scored_n","评分数","num"],["wape","WAPE","pct"],["bias","Bias","pct"],["hit_n","命中","num"]),rows("strata"),true))}
      {visible("v3-coverage")&&wrap("v3-coverage","核心名单不能代表全店未来需求","coverage",table("v3-coverage-table","固定名单销量覆盖","coverage",cols(["issue_date","签发日"],["fixed_n","核心记录","num"],["selection_coverage","选款期覆盖","pct"],["future_core","核心未来实销","num"],["future_shop","全店未来实销","num"],["future_coverage","未来销量覆盖","pct"])))}
      {visible("v3-risks")&&wrap("v3-risks","后30天绝对误差增幅最大的风险对象（最多10条）","risks",<>
        <RichNarrative id="v3-risks:note" value="仅按DIRECT相对F28的后30天绝对误差增幅排序。这是描述性风险定位，不解释业务原因，不据此删除对象或追加模型配置。"/>
        {table("v3-risks-table","后30天退化对象","risks",cols(["issue_date","签发日"],["shop","店铺"],["spu","SPU"],["actual_f28","实销","num"],["prediction_f28","F28预测","num"],["prediction_direct","DIRECT预测","num"],["absolute_error_change","误差增加","num"]),rows("risks"),true)}
      </>)}
      {visible("v3-limits")&&wrap("v3-limits","方法边界与交付状态","meta",<RichNarrative id="v3-limits:body" value={`- 两段分别预测未来1–30与31–60天，60天值只由两段预测相加；无递归、无M1注入M2、无未来实销替换。\n\n- 直接模型要求28个连续已知历史日；不附加原递归56天分段门槛。这是结构差异，不是效果后调参。\n\n- 原参数固定，正式拟合共${meta.formal_fits}次，无搜索、无早停、无自动追加配置；独立核验状态：${meta.validation_status}。\n\n- 这三期已被研究过，只能称RECONSTRUCTED_HISTORICAL_REPLAY。归档不能恢复当时真实取数可得性，不能外推为前瞻OOS、全店达标或经营因果。\n\n- 当前报告为本地冻结证据，不连接数据库，不自动刷新，不发布到Sites。已做构建与本地HTTP交付检查；未完成浏览器点击和视觉验收。`.replaceAll("\\n","\n")}/>)}
    </SortableRegion>
  </article>;
}
'''

CSS = '''.report-page { --data-app-layout-intent: authored-report; --data-app-content-width: 1100px; }
.v3-report .report-hero { margin-bottom: 24px; }
.v3-report .report-deck { max-width: 800px; }
.v3-report .report-section { margin-bottom: 28px; }
.v3-report h1 { line-height: 1.18; }
'''


def author(app, plugin_root):
    app = Path(app)
    data = read_json(app / "src" / "data.json")
    if not data.get("id") or data.get("surface") != "report":
        raise ValueError("prepare the official report starter first")
    reviewed = read_json(app.parent / "report_evidence" / "snapshot.json")
    data.update({key: value for key, value in reviewed.items() if key != "id"})
    content = app / "src" / "content" / "report"
    (content / "ReportContent.jsx").write_text(CONTENT, encoding="utf-8")
    starter_css = Path(plugin_root) / "templates" / "data-app" / "base" / "src" / "content" / "report" / "report.css"
    (content / "report.css").write_text(starter_css.read_text(encoding="utf-8") + "\n" + CSS, encoding="utf-8")
    data["buildStatus"] = "complete"
    (app / "src" / "data.json").write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps({"status": "AUTHORED", "artifact_id": data["id"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root")
    parser.add_argument("--archive")
    parser.add_argument("--validation")
    parser.add_argument("--author")
    parser.add_argument("--plugin-root", help="installed Data plugin root; required with --author")
    parser.add_argument("--refresh-evidence", action="store_true", help="refresh only this report's derived evidence after source-hash checks")
    args = parser.parse_args()
    if args.author:
        if not args.plugin_root:
            parser.error("--author requires --plugin-root to preserve official report styles")
        author(args.author, args.plugin_root)
    else:
        package(args.root, args.archive, args.validation, args.refresh_evidence)
