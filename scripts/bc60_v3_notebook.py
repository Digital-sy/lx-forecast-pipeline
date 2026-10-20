"""Execute a result-review notebook for completed DIRECT30x2 V3 artifacts.

The notebook reads archived coverage and scores only. It cannot fit a model,
query a database, change prior archives, or choose a production candidate.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
from pathlib import Path
import sys
import textwrap

import nbformat
from nbclient import NotebookClient
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def preview(notebook, path: Path):
    blocks = []
    for cell in notebook.cells:
        if cell.cell_type == "markdown":
            blocks.append('<section><pre class="markdown">' + html.escape(cell.source) + '</pre></section>')
            continue
        blocks.append('<details><summary>Editable cell source</summary><pre>' + html.escape(cell.source) + '</pre></details>')
        for output in cell.get("outputs", []):
            data = output.get("data", {})
            if "image/png" in data:
                blocks.append('<figure><img alt="Executed V3 notebook figure" src="data:image/png;base64,' + data["image/png"] + '"></figure>')
            elif "text/html" in data:
                blocks.append('<section class="output">' + data["text/html"] + '</section>')
            elif "text/plain" in data:
                blocks.append('<pre class="output">' + html.escape(data["text/plain"]) + '</pre>')
            elif output.get("output_type") == "stream":
                blocks.append('<pre class="output">' + html.escape(output["text"]) + '</pre>')
    path.write_text('<!doctype html><html lang="zh"><meta charset="utf-8"><title>BC60 V3 notebook review</title>'
                    '<style>body{font:16px system-ui,sans-serif;color:#263344;max-width:1150px;margin:30px auto;padding:20px;background:#fff}'
                    'pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:13px;line-height:1.6}.markdown{font:16px system-ui,sans-serif;line-height:1.8}'
                    'section,details{margin:22px 0}summary{color:#53677d;cursor:pointer}img{max-width:100%;height:auto}'
                    'table{border-collapse:collapse;font-size:13px}th,td{padding:7px 10px;border-bottom:1px solid #ddd;text-align:right}'
                    '.output{overflow-x:auto}figure{margin:20px 0}</style><body>' + ''.join(blocks) + '</body></html>', encoding="utf-8")


def run(prepared: Path, effects: Path, validation: Path, prepared_validation: Path, out: Path):
    prepared, effects, validation, prepared_validation, out = [p.resolve() for p in (prepared, effects, validation, prepared_validation, out)]
    if out.exists():
        raise ValueError("OUTPUT_EXISTS: use a new notebook output directory")
    manifests_paths = {"prepared_manifest": prepared / "prepare_manifest.json",
                       "effects_manifest": effects / "run_manifest.json", "validation": validation,
                       "prepared_validation": prepared_validation}
    manifests = {k: json.loads(p.read_text(encoding="utf-8")) for k, p in manifests_paths.items()}
    if not manifests["validation"].get("status", "").startswith("PASS"):
        raise ValueError("INDEPENDENT_VALIDATION_NOT_PASS")
    if not manifests["prepared_validation"].get("status", "").startswith("PASS"):
        raise ValueError("INDEPENDENT_PREPARED_VALIDATION_NOT_PASS")
    if not manifests["effects_manifest"].get("originals_unchanged"):
        raise ValueError("ORIGINAL_ARCHIVE_PRESERVATION_NOT_VERIFIED")
    if manifests["effects_manifest"]["formal_fit_count"] > 6:
        raise ValueError("FIT_BUDGET_EXCEEDED")
    if sha(manifests_paths["prepared_manifest"]) != manifests["effects_manifest"]["prepared_manifest_sha256"]:
        raise ValueError("PREPARED_MANIFEST_HASH_MISMATCH")
    source = {
        "coverage": prepared / "coverage.csv", "exclusions": prepared / "exclusions.csv",
        "per_object_windows": prepared / "per_object_windows.csv", "core": prepared / "fixed_core.csv",
        "summary": effects / "summary.csv", "joint": effects / "joint_hits.csv",
        "migrations": effects / "joint_migrations.csv", "paired": effects / "paired_summary.csv",
        "predictions": effects / "predictions.csv", "scores": effects / "scores.csv", "fits": effects / "fits.json",
    }
    for key, path in source.items():
        owner = manifests["prepared_manifest"] if path.parent == prepared else manifests["effects_manifest"]
        if sha(path) != owner["files"][path.name]:
            raise ValueError("SOURCE_HASH_MISMATCH: " + key)
    fingerprints = {str(p): sha(p) for p in [*source.values(), *manifests_paths.values()]}
    out.mkdir(parents=True, exist_ok=False)
    runtime = out / "runtime"
    spec = runtime / "kernels" / "bc60-v3-review"
    spec.mkdir(parents=True)
    (spec / "kernel.json").write_text(json.dumps({
        "argv": [sys.executable, "-B", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
        "display_name": "BC60 V3 result review", "language": "python",
        "env": {"IPYTHONDIR": str(runtime / "ipython"), "MPLCONFIGDIR": str(runtime / "matplotlib"),
                "JUPYTER_RUNTIME_DIR": str(runtime), "JUPYTER_CONFIG_DIR": str(runtime / "jupyter_config"),
                "PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8"}}, indent=2), encoding="utf-8")
    parameters = {"files": {k: str(v) for k, v in source.items()}, "manifests": {k: str(v) for k, v in manifests_paths.items()},
                  "hashes": fingerprints, "output": str(out)}
    cells = []

    def md(text):
        cells.append(nbformat.v4.new_markdown_cell(textwrap.dedent(text).strip()))

    def code(text):
        cells.append(nbformat.v4.new_code_cell(textwrap.dedent(text).strip()))

    md("""
    # BC60 V3：直接分段预测结果复核
    ## tl;dr
    本Notebook仅复核已经计算且独立验证的V3结果，不训练模型、不取新数据、不替换旧预测。
    核心目标仍为全店核心商品两段及累计均在±20%，不以WAPE下降替代。
    """)
    code("PARAMETERS = " + repr(parameters))
    code("""
    from pathlib import Path
    import hashlib, json
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
    from IPython.display import display, Markdown

    for file, digest in PARAMETERS['hashes'].items():
        assert hashlib.sha256(Path(file).read_bytes()).hexdigest() == digest, file
    manifests = {key: json.loads(Path(file).read_text(encoding='utf-8')) for key, file in PARAMETERS['manifests'].items()}
    frames = {key: pd.read_csv(file) for key, file in PARAMETERS['files'].items() if key != 'fits'}
    fits = pd.DataFrame(json.loads(Path(PARAMETERS['files']['fits']).read_text(encoding='utf-8')))
    COLORS = {'F28': '#65758a', 'DIRECT30x2': '#176ea5', 'B1': '#d18b21', 'D1': '#9270ad',
              'M1': '#176ea5', 'M2': '#d18b21'}
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11, 'axes.spines.top': False,
                         'axes.spines.right': False, 'axes.titlepad': 13, 'figure.dpi': 125, 'savefig.dpi': 160})
    pd.set_option('display.max_rows', 32)
    pd.set_option('display.max_columns', 15)
    pd.set_option('display.float_format', lambda value: f'{value:,.4f}')
    FIGURES = Path(PARAMETERS['output']) / 'figures'
    FIGURES.mkdir(exist_ok=True)

    def select(scope='full_core', dimension='overall', **filters):
        frame = frames['summary']
        mask = frame.scope.eq(scope) & frame['slice'].eq(dimension)
        for key, value in filters.items():
            mask &= frame[key].eq(value)
        return frame.loc[mask].copy()

    def one(**filters):
        rows = select(**filters)
        assert len(rows) == 1, filters
        return rows.iloc[0]

    common_f28 = one(scope='common_complete', model='F28', window='d01_60')
    common_direct = one(scope='common_complete', model='DIRECT30x2', window='d01_60')
    joint = frames['joint']
    joint_counts = joint.groupby(['model', 'joint_decision']).size().unstack(fill_value=0)
    migration = frames['migrations'][frames['migrations'].scope.eq('full_core')]
    migration_counts = migration.migration.value_counts()
    display(Markdown(
        f"- 同一完整共同集合 N={int(common_direct.fixed_n)}，60天WAPE：F28 {common_f28.wape:.2%}，"
        f"DIRECT30x2 {common_direct.wape:.2%}，变化 {(common_direct.wape-common_f28.wape)*100:+.2f}个百分点。\\n"
        f"- 固定385主名单三窗口同时命中：F28 {int(joint_counts.loc['F28'].get('HIT',0))}，"
        f"DIRECT30x2 {int(joint_counts.loc['DIRECT30x2'].get('HIT',0))}；新增命中 "
        f"{int(migration_counts.get('NEW_HIT',0))}、退出命中 {int(migration_counts.get('EXITED_HIT',0))}，"
        f"迁移不可判断 {int(migration_counts.get('UNJUDGED',0))}。\\n"
        f"- 独立核验：{manifests['validation']['status']}。训练支持、分期表现和缺输出见下文，不能只按总体排序选模型。"))
    """)
    md("""
    ## Context & Methods
    ### Key Assumptions
    固定385个店铺×SPU×签发期记录，174个不同店铺×SPU；原B1可输出369条与缺资格16条均保留。
    三次签发日为2026-04-02、06-02、08-02；假定C=S−2。M1直接预测S+1至S+30，M2直接预测S+31至S+60。
    M2不读取M1预测或第一段未来实销；60天输出必须是两段预测相加，缺一段即不能判累计通过。
    M1训练标签结束须≤C，M2分别满足其标签结束≤C。重叠标签行数不是独立时间样本。
    F28与B1是原首次签发对照；D1只是V2反馈干预诊断，不是生产候选。本Notebook不混入V2滚动T1/T2成绩。
    三个期间均已研究，分类是RECONSTRUCTED_HISTORICAL_REPLAY，不是独立OOS；回填历史不能证明历史时点实际可用。
    """)
    md("## Data\n### 归档指纹与固定名单")
    code("""
    core = frames['core']
    assert len(core) == 385
    assert len(core[['shop','spu']].drop_duplicates()) == 174
    assert int(core.b1_original_eligible.sum()) == 369
    display(core.groupby('issue_date').agg(fixed_core_n=('spu','size'), b1_original_eligible=('b1_original_eligible','sum')))
    display(pd.DataFrame([{'artifact': Path(file).name, 'sha256': digest}
                          for file, digest in PARAMETERS['hashes'].items()]))
    """)
    md("""
    ## Results
    ### 1. 训练时间支持：行数、日期数与不重叠窗口
    下表及图来自实际构造记录，而非理论日期上界。技术上拟合成功不代表时间覆盖充分。
    有限时间支持单列；不能把数万重叠样本行当作数万个独立历史时段。
    """)
    code("""
    coverage = frames['coverage'].sort_values(['issue_date','segment'])
    display(coverage[['issue_date','segment','input_cutoff','train_rows','train_objects','train_issue_dates',
                      'positive_labels','zero_labels','max_global_nonoverlap','support_status','inference_ok']])
    display(coverage[['issue_date','segment','first_simulated_issue','last_simulated_issue','first_label','last_label']])
    windows = frames['per_object_windows'].groupby(['issue_date','segment']).agg(
        objects=('spu','size'), min_nonoverlap=('nonoverlap_windows','min'),
        median_nonoverlap=('nonoverlap_windows','median'), max_nonoverlap=('nonoverlap_windows','max'))
    display(windows)
    labels = sorted(coverage.issue_date.unique())
    x = np.arange(len(labels))
    fig, axes = plt.subplots(1,2,figsize=(12.6,4.5),layout='constrained')
    for axis, metric, title in zip(axes, ['train_issue_dates','max_global_nonoverlap'],
                                  ['Distinct historical issue dates','Maximum nonoverlapping target windows']):
        for index, segment in enumerate(['M1','M2']):
            z = coverage[coverage.segment.eq(segment)].set_index('issue_date').reindex(labels)
            bars = axis.bar(x + (index-.5)*.34, z[metric], .34, color=COLORS[segment],
                            label=segment, hatch='//' if segment=='M2' else None)
            axis.bar_label(bars, padding=3, fmt='%.0f', fontsize=10)
        axis.set_xticks(x,labels)
        axis.set_ylim(bottom=0)
        axis.set_xlabel('Original forecast issue date')
        axis.set_title(title)
        axis.grid(axis='y',alpha=.18)
        axis.set_axisbelow(True)
        axis.legend(frameon=False)
    fig.suptitle('Training temporal support · overlapping rows are not independent windows',fontsize=14)
    fig.savefig(FIGURES/'training_temporal_support.png',bbox_inches='tight')
    plt.show()
    display(frames['exclusions'].groupby(['issue_date','segment','reason'],as_index=False).rows.sum())
    display(fits[['issue_date','segment','status','fit_attempted','training_sha256']])
    """)
    md("""
    ### 2. 固定385主名单与369公平范围
    正实销命中率的分母保留固定范围内全部正实销对象，缺输出不从名单中消失。
    WAPE/Bias使用有预测且真值已知对象的实际销量分母，因此主名单表必须同时看scored_n。
    完整共同子集重新计算所有模型，不能引用其他样本的旧成绩。
    """)
    code("""
    fields = ['model','window','fixed_n','output_n','scored_n','positive_truth_n',
              'hit_n','miss_n','unjudged_n','hit_rate','hit_sales_share','wape','bias','zero_actual_n','zero_actual_absolute_error']
    display(select().loc[:,fields].sort_values(['model','window']))
    display(select(scope='original_b1_369').loc[:,fields].sort_values(['model','window']))
    display(select(scope='common_complete').loc[:,fields].sort_values(['model','window']))
    display(frames['predictions'].groupby(['issue_date','segment','prediction_status']).size().rename('records').reset_index())
    zero_missing = frames['scores'].groupby(['model','window']).agg(
        missing_actual=('actual',lambda values:int(values.isna().sum())),
        missing_output=('prediction',lambda values:int(values.isna().sum())),
        zero_actual=('actual',lambda values:int(values.eq(0).sum())))
    display(zero_missing)
    """)
    md("""
    ### 3. 同样本效果与分期一致性
    左图使用实际完整共同集合，N由已验证结果确定；原369范围另表保留，不把不同分母放到同一排名。
    右图显示每期同一完整共同集合的60天WAPE；三个期间均保留，包括4月有限时间支持。
    """)
    code("""
    order = ['d01_30','d31_60','d01_60']
    comparison = select(scope='common_complete')
    fig, axes = plt.subplots(1,2,figsize=(12.6,4.7),layout='constrained')
    for index, model in enumerate(['F28','DIRECT30x2']):
        z = comparison[comparison.model.eq(model)].set_index('window').reindex(order)
        bars = axes[0].bar(np.arange(3)+(index-.5)*.34,z.wape,.34,color=COLORS[model],label=model,
                           hatch='//' if model=='DIRECT30x2' else None)
        axes[0].bar_label(bars,labels=[f'{v:.1%}' for v in z.wape],padding=3,fontsize=10)
        z = select(scope='common_complete',dimension='issue',model=model,window='d01_60').set_index('issue_date').reindex(labels)
        bars = axes[1].bar(x+(index-.5)*.34,z.wape,.34,color=COLORS[model],label=model,
                           hatch='//' if model=='DIRECT30x2' else None)
        axes[1].bar_label(bars,labels=[f'{v:.1%}' for v in z.wape],padding=3,fontsize=10)
    axes[0].set_xticks(np.arange(3),['Days 1–30','Days 31–60','Days 1–60'])
    axes[0].set_title('Same complete common cohort · three windows')
    axes[1].set_xticks(x,labels)
    axes[1].set_title('Same-period common cohort · full 60 days')
    for axis in axes:
        axis.set_ylim(bottom=0)
        axis.yaxis.set_major_formatter(PercentFormatter(1))
        axis.set_ylabel('WAPE')
        axis.grid(axis='y',alpha=.18)
        axis.set_axisbelow(True)
        axis.legend(frameon=False)
    fig.suptitle('DIRECT30x2 versus frozen F28 · no rolling revisions',fontsize=14)
    fig.savefig(FIGURES/'same_sample_wape.png',bbox_inches='tight')
    plt.show()
    display(select(scope='common_complete',dimension='issue',window='d01_60')[
        ['issue_date','model','fixed_n','scored_n','wape','bias','over_units','under_units','hit_n','hit_rate']]
        .sort_values(['issue_date','model']))
    display(select(scope='common_complete',window='d31_60')[
        ['model','fixed_n','scored_n','wape','bias','over_units','under_units','hit_n','hit_rate']])
    """)
    md("""
    ### 4. 三窗口同时命中迁移
    迁移是同一冻结名单上的联合结果，不是把三个窗口命中数相加。
    候选任一分段缺输出时，联合结果不可判断；这种情况不能静默算作两者都未命中。
    """)
    code("""
    display(joint_counts)
    category_order = ['RETAINED_HIT','NEW_HIT','EXITED_HIT','BOTH_MISS','UNJUDGED']
    counts = migration_counts.reindex(category_order,fill_value=0)
    assert counts.sum() == 385
    fig, axis = plt.subplots(figsize=(11.8,4.5),layout='constrained')
    bars = axis.barh(np.arange(len(counts)),counts.to_numpy(),color=['#65758a','#176ea5','#d18b21','#adb7c4','#9270ad'])
    axis.bar_label(bars,padding=5,fmt='%.0f')
    axis.set_yticks(np.arange(len(counts)),['Retained joint hit','New joint hit','Exited joint hit','Both miss','Unjudged'])
    axis.invert_yaxis()
    axis.set_xlim(left=0,right=max(counts.max()*1.14,1))
    axis.set_xlabel('Frozen shop × SPU × original issue records (total 385)')
    axis.set_title('F28 → DIRECT30x2 · all three windows must hit')
    axis.grid(axis='x',alpha=.18)
    axis.set_axisbelow(True)
    fig.savefig(FIGURES/'joint_hit_migrations.png',bbox_inches='tight')
    plt.show()
    display(migration.groupby(['issue_date','migration']).size().unstack(fill_value=0).reindex(columns=category_order,fill_value=0))
    pairs = frames['paired']
    display(pairs[pairs.scope.eq('common_complete') & pairs['slice'].eq('overall')][
        ['window','fixed_n','paired_n','unjudged_n','improved_n','degraded_n','unchanged_n',
         'improved_sales_share','degraded_sales_share','wape_change']])
    display(pairs[pairs.scope.eq('common_complete') & pairs['slice'].eq('shop') & pairs.window.eq('d01_60')][
        ['shop','paired_n','improved_n','degraded_n','wape_change','degraded_sales_share']].sort_values('wape_change',ascending=False))
    """)
    md("""
    ## Takeaways
    本文件只复核单一配置的真实离线结果，不追加搜索、不重新选择名单。
    目标单位从日销量改为30天累计量，训练样本单位也改变；结果不是只翻转一个递推开关的纯因果结论。
    各期训练时间支持与核心名单覆盖限制见归档证据；历史点时可获取性不能由本次回放证明。
    联合命中、误差、退出命中与缺输出必须一起判断；即使某项改善，也不能宣称全店核心品前瞻两个月±20%已经达标。
    后续方向由主报告给出有限结论，本Notebook不选择生产模型，不修改V4/A16/NEW_VISIBLE、采购、BI或调度。
    """)
    code("""
    assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest for path,digest in PARAMETERS['hashes'].items())
    assert manifests['validation']['status'].startswith('PASS')
    assert manifests['prepared_validation']['status'].startswith('PASS')
    assert manifests['effects_manifest']['originals_unchanged']
    display(Markdown('**复核执行完成；读取的训练覆盖、效果与独立验收文件哈希未变。**'))
    """)
    notebook = nbformat.v4.new_notebook(cells=cells,metadata={
        "kernelspec":{"name":"bc60-v3-review","display_name":"BC60 V3 result review","language":"python"},
        "language_info":{"name":"python","version":sys.version.split()[0]},
        "scope":"READ_COMPLETED_RESULTS_ONLY_NO_FITS_NO_QUERIES"})
    nbformat.validate(notebook)
    path = out / "BC60_V3_直接分段结果复核.ipynb"
    nbformat.write(notebook,path)
    manager = KernelManager(kernel_name="bc60-v3-review",connection_file=str(runtime/'kernel_connection.json'),
                            kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(runtime/'kernels')]))
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        NotebookClient(notebook,km=manager,timeout=120,resources={"metadata":{"path":str(out)}},allow_errors=False).execute()
    finally:
        nbformat.write(notebook,path)
    nbformat.validate(notebook)
    errors = [o for c in notebook.cells for o in c.get("outputs",[]) if o.output_type=="error"]
    if errors:
        raise ValueError("NOTEBOOK_HAS_ERROR_OUTPUT")
    preview(notebook,out/"BC60_V3_notebook_preview.html")
    result = {"status":"EXECUTED_TOP_TO_BOTTOM","started_at":started,"completed_at":dt.datetime.now(dt.timezone.utc).isoformat(),
              "generator_sha256":sha(Path(__file__)),"notebook_sha256":sha(path),"source_hashes":fingerprints,
              "code_cells":sum(c.cell_type=="code" for c in notebook.cells),"error_outputs":len(errors),
              "new_fits":0,"source_queries":0,"figures":{p.name:sha(p) for p in sorted((out/'figures').glob('*.png'))},
              "visual_inspection":"PENDING_EXPLICIT_IMAGE_INSPECTION","browser_interaction":"NOT_VERIFIED"}
    (out/'notebook_execution.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepared',type=Path,required=True)
    parser.add_argument('--effects',type=Path,required=True)
    parser.add_argument('--validation',type=Path,required=True)
    parser.add_argument('--prepared-validation',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    print(json.dumps(run(**vars(parser.parse_args())),ensure_ascii=False,indent=2))
