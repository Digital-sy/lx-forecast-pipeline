"""Build and execute a read-only review notebook from completed BC60 V2 artifacts.

This companion consumes already-calculated results. It never fits estimators,
queries a source system, or replaces an archived experiment directory.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import os
from pathlib import Path
import sys
import textwrap

import nbformat
from nbclient import NotebookClient
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def render_notebook_preview(notebook, output: Path) -> None:
    """Offline preview of actual cell outputs; no new analytical rendering."""
    blocks = []
    for cell in notebook.cells:
        if cell.cell_type == "markdown":
            blocks.append('<section><pre class="markdown">' + html.escape(cell.source) + '</pre></section>')
            continue
        blocks.append('<details><summary>Editable cell source</summary><pre>' + html.escape(cell.source) + '</pre></details>')
        for result in cell.get("outputs", []):
            data = result.get("data", {})
            if "image/png" in data:
                blocks.append('<figure><img alt="Executed notebook figure" src="data:image/png;base64,' + data["image/png"] + '"></figure>')
            elif "text/html" in data:
                blocks.append('<section class="output">' + data["text/html"] + '</section>')
            elif "text/plain" in data:
                blocks.append('<pre class="output">' + html.escape(data["text/plain"]) + '</pre>')
            elif result.get("output_type") == "stream":
                blocks.append('<pre class="output">' + html.escape(result["text"]) + '</pre>')
    output.write_text('<!doctype html><html lang="zh"><meta charset="utf-8"><title>BC60 V2 notebook preview</title>'
                     '<style>body{font:16px system-ui,sans-serif;color:#263344;max-width:1150px;margin:30px auto;padding:20px;background:#fff}'
                     'pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:13px;line-height:1.6}.markdown{font:16px system-ui,sans-serif;line-height:1.8}'
                     'section,details{margin:22px 0}summary{color:#53677d;cursor:pointer}img{max-width:100%;height:auto}'
                     'table{border-collapse:collapse;font-size:13px}th,td{padding:7px 10px;border-bottom:1px solid #ddd;text-align:right}'
                     '.output{overflow-x:auto}figure{margin:20px 0}</style><body>' + ''.join(blocks) + '</body></html>', encoding="utf-8")


def build(rolling: Path, diagnostic: Path, validation: Path, diagnostic_validation: Path, out: Path) -> dict:
    rolling, diagnostic, validation, diagnostic_validation, out = [p.resolve() for p in (rolling, diagnostic, validation, diagnostic_validation, out)]
    if out.exists():
        raise ValueError("OUTPUT_ALREADY_EXISTS: choose a new review directory")
    source_specs = {
        "rolling_manifest": rolling / "run_manifest.json",
        "diagnostic_manifest": diagnostic / "diagnostic_manifest.json",
        "validation": validation,
        "diagnostic_validation": diagnostic_validation,
    }
    manifests = {k: json.loads(p.read_text(encoding="utf-8")) for k, p in source_specs.items()}
    if manifests["rolling_manifest"].get("status") != "COMPLETE":
        raise ValueError("ROLLING_RESULTS_NOT_COMPLETE")
    if manifests["diagnostic_manifest"].get("status") != "COMPLETE":
        raise ValueError("B1_DIAGNOSTIC_NOT_COMPLETE")
    if not manifests["validation"].get("status", "").startswith("PASS"):
        raise ValueError("INDEPENDENT_VALIDATION_NOT_PASS")
    if not manifests["diagnostic_validation"].get("status", "").startswith("PASS"):
        raise ValueError("INDEPENDENT_B1_VALIDATION_NOT_PASS")
    files = {"t0": rolling / "t0_summary.csv", "t1": rolling / "t1_summary.csv",
             "t2": rolling / "t2_summary.csv", "paired": rolling / "paired_summary.csv",
             "joint": rolling / "joint_hits.csv", "revision": rolling / "revision_summary.csv",
             "core": rolling / "fixed_core.csv", "stages": diagnostic / "stage_summary.csv"}
    for key, path in files.items():
        owner = manifests["diagnostic_manifest"] if key == "stages" else manifests["rolling_manifest"]
        expected = owner["files"][path.name]
        if sha256(path) != expected:
            raise ValueError(f"SUMMARY_HASH_MISMATCH: {path.name}")
    evidence = {str(p): sha256(p) for p in [*source_specs.values(), *files.values()]}
    out.mkdir(parents=True, exist_ok=False)
    runtime = out / "runtime"
    kernels = runtime / "kernels"
    spec = kernels / "bc60-v2-review"
    spec.mkdir(parents=True)
    (spec / "kernel.json").write_text(json.dumps({
        "argv": [sys.executable, "-B", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
        "display_name": "BC60 V2 review (isolated existing environment)", "language": "python",
        "env": {"IPYTHONDIR": str(runtime / "ipython"), "PYTHONDONTWRITEBYTECODE": "1",
                "MPLCONFIGDIR": str(runtime / "matplotlib"), "JUPYTER_RUNTIME_DIR": str(runtime),
                "JUPYTER_CONFIG_DIR": str(runtime / "jupyter_config"), "PYTHONIOENCODING": "utf-8"}}, indent=2), encoding="utf-8")
    parameters = {k: str(v) for k, v in source_specs.items()} | {"files": {k: str(v) for k, v in files.items()},
                 "expected_hashes": evidence, "output": str(out)}
    cells = []

    def md(source):
        cells.append(nbformat.v4.new_markdown_cell(textwrap.dedent(source).strip()))

    def code(source):
        cells.append(nbformat.v4.new_code_cell(textwrap.dedent(source).strip()))

    md("""
    # B1远期高估与28日均销滚动更新：V2复核Notebook
    ## tl;dr
    本文件只读取已完成并独立核验的本地结果，不取数、不重训、不重算模型效果。
    下列摘要由结果文件读取生成。T0、T1、T2分开，不能把获得新实销后的更新能力冒充首次60天能力。
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

    for file, expected in PARAMETERS['expected_hashes'].items():
        assert hashlib.sha256(Path(file).read_bytes()).hexdigest() == expected, file
    manifests = {key: json.loads(Path(PARAMETERS[key]).read_text(encoding='utf-8'))
                 for key in ['rolling_manifest', 'diagnostic_manifest', 'validation', 'diagnostic_validation']}
    frames = {key: pd.read_csv(file) for key, file in PARAMETERS['files'].items()}
    CHECKPOINTS = [0, 7, 14, 21, 30, 45]
    COLORS = {'F28': '#65758a', 'R28-D': '#176ea5', 'R28-W': '#d18b21',
              'D0': '#65758a', 'D1': '#176ea5', 'B1': '#d18b21', 'WEEKDAY4': '#9270ad'}
    MARKERS = {'F28': 's', 'R28-D': 'o', 'R28-W': '^', 'D0': 's', 'D1': 'o'}
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11,
                         'axes.spines.top': False, 'axes.spines.right': False,
                         'axes.titlepad': 14, 'figure.dpi': 125, 'savefig.dpi': 160})
    FIGURES = Path(PARAMETERS['output']) / 'figures'
    FIGURES.mkdir(exist_ok=True)
    pd.set_option('display.max_rows', 30)
    pd.set_option('display.max_columns', 14)
    pd.set_option('display.float_format', lambda value: f'{value:,.4f}')

    def select(name, scope='full_core', **filters):
        frame = frames[name]
        mask = frame.scope.eq(scope) & frame['slice'].eq('overall')
        for field, value in filters.items():
            mask &= frame[field].eq(value)
        return frame.loc[mask].copy()

    def one(name, **filters):
        frame = select(name, **filters)
        assert len(frame) == 1, (name, filters, len(frame))
        return frame.iloc[0]

    t1_initial = one('t1', update_offset=0, model='F28')
    t1_fixed = one('t1', update_offset=30, model='F28')
    t1_daily = one('t1', update_offset=30, model='R28-D')
    t1_weekly = one('t1', update_offset=30, model='R28-W')
    t2_daily = one('t2', scope='t2_balanced_279', update_offset=30, window='d01_60', model='R28-D')
    t2_weekly = one('t2', scope='t2_balanced_279', update_offset=30, window='d01_60', model='R28-W')
    display(Markdown(
        f"- T1更新第30天（只预测原终点前剩余30天）：F28 / 日更 / 周更 WAPE = "
        f"{t1_fixed.wape:.2%} / {t1_daily.wape:.2%} / {t1_weekly.wape:.2%}；"
        f"日更命中 {int(t1_daily.hit_n)}/{int(t1_daily.fixed_core_n)}。\\n"
        f"- T2第30天另发完整未来60天、固定前两期279记录：日更 / 周更 WAPE = "
        f"{t2_daily.wape:.2%} / {t2_weekly.wape:.2%}。此为同一R、同一目标日期对比。\\n"
        f"- T0原始输出未被更新覆盖。独立数值核验：{manifests['validation']['status']}。\\n"
        '- 本轮为 RECONSTRUCTED_HISTORICAL_REPLAY；重叠目标不是独立样本，修订额不是采购损失。'))
    """)
    md("""
    ## Context & Methods
    ### Key Assumptions
    固定385个店铺×SPU×签发期记录、174个不同店铺×SPU；B1原可输出369记录另报，不填16个资格缺口。
    来源为V1归档，按两天延迟模拟可用性，不代表历史点时实际可获取。
    F28冻结不变；日更用R−29至R−2的28个完整日历日，周更每7天更新，其余日维持原值。
    T1只评分[R+1,S+60]；T2另发[R+1,R+60]，超出2026-10-01的真值按各窗口标NOT_MATURED。
    D1仅替换未来递推状态的代理销量，保留每步B1实际输出；不是上线候选。
    """)
    md("## Data\n### 本地来源与证据指纹")
    code("""
    evidence_table = pd.DataFrame([{'artifact': Path(file).name, 'sha256': digest, 'path': file}
                                   for file, digest in PARAMETERS['expected_hashes'].items()])
    display(evidence_table[['artifact', 'sha256']])
    core = frames['core']
    assert len(core) == 385
    assert len(core[['shop', 'spu']].drop_duplicates()) == 174
    assert int(core.b1_original_eligible.sum()) == 369
    assert manifests['rolling_manifest']['original_archive_hashes_unchanged']
    display(core.groupby('issue_date' if 'issue_date' in core else 'original_issue_date')
            .agg(core_records=('spu', 'size'), b1_original_eligible=('b1_original_eligible', 'sum')))
    """)
    md("""
    ## Results
    ### T0：原始首次签发结果保留
    下表是385主名单；B1 WAPE的分母来自有输出的369记录，不能和385范围的其他模型直接冒充同样本排名。
    第二表因此单列原369公平比较。±20%命中率与WAPE不是同一业务目标。
    """)
    code("""
    columns = ['model', 'window', 'fixed_core_n', 'output_n', 'scored_n',
               'hit_n', 'hit_rate', 'wape', 'bias', 'unjudged_n']
    display(select('t0').loc[:, columns].sort_values(['model', 'window']))
    display(select('t0', scope='original_b1_369').loc[:, columns].sort_values(['model', 'window']))
    joint = frames['joint']
    original_joint = joint[joint.track.eq('T0')].groupby('model').agg(
        fixed_core_n=('all_three_hit', 'size'), all_three_hit=('all_three_hit', 'sum'))
    display(original_joint)
    """)
    md("""
    ### A：逐段偏差与反馈干预
    图中每个阶段长度不同，故比较WAPE与Bias而非直接比较阶段总件数。
    两个过桥日另表保留，不计入原1—60天；D0/D1只比较相同原369记录。
    """)
    code("""
    stages = frames['stages']
    audit = stages[stages.scope.eq('SECONDARY_ORIGINAL_369_B1_OUTPUTS') & stages['slice'].eq('overall')]
    order = ['d01_07', 'd08_14', 'd15_30', 'd31_45', 'd46_60']
    fig, axes = plt.subplots(1, 2, figsize=(12.6, 4.5), layout='constrained')
    for metric, axis in zip(['wape', 'bias'], axes):
        for model in ['D0', 'D1']:
            rows = audit[audit.model.eq(model)].set_index('stage').reindex(order)
            axis.plot(range(len(order)), rows[metric], marker=MARKERS[model], color=COLORS[model],
                      label=model, linestyle='-' if model == 'D0' else '--')
        axis.set_xticks(range(len(order)), ['1–7', '8–14', '15–30', '31–45', '46–60'])
        axis.set_xlabel('Original target-day stage (bridge excluded)')
        axis.set_title(metric.upper() if metric == 'wape' else 'Bias')
        axis.yaxis.set_major_formatter(PercentFormatter(1))
        if metric == 'wape':
            axis.set_ylim(bottom=0)
        else:
            axis.axhline(0, color='#53677d', linewidth=.7)
        axis.grid(axis='y', alpha=.2)
        axis.legend(frameon=False)
    fig.suptitle('B1 feedback diagnostic · same original 369 records', fontsize=14)
    fig.savefig(FIGURES / 'a_feedback_stages.png', bbox_inches='tight')
    plt.show()
    display(audit[audit.stage.isin(['bridge', 'd01_07', 'd01_30', 'd31_60', 'd01_60'])][
        ['model', 'stage', 'fixed_core_n', 'scored_n', 'hit_n', 'forecast_units', 'actual_units',
         'wape', 'bias', 'over_units', 'under_units']].sort_values(['stage', 'model']))
    """)
    code("""
    d0 = audit[audit.model.eq('D0') & audit.stage.eq('d31_60')].iloc[0]
    d1 = audit[audit.model.eq('D1') & audit.stage.eq('d31_60')].iloc[0]
    first0 = audit[audit.model.eq('D0') & audit.stage.eq('d01_07')].iloc[0]
    first1 = audit[audit.model.eq('D1') & audit.stage.eq('d01_07')].iloc[0]
    display(Markdown(f"D1相对D0，31—60天 WAPE 改变 {(d1.wape-d0.wape)*100:+.2f}个百分点，"
                     f"高估件数改变 {d1.over_units-d0.over_units:+,.0f}件。"
                     f"1—7天命中数则由 {int(first0.hit_n)} 变为 {int(first1.hit_n)}，整体误差与逐款命中不能混用。"
                     '这是受控反馈路径诊断，不能据此解释全部误差或实际经营因果。'))
    """)
    md("""
    ### B/T1：原终点下的剩余需求
    每个横轴点单独比较完全相同的未来日期，依次剩余60/53/46/39/30/15天。
    跨点预测长度缩短，不能据该曲线声称首次提前60天已更准确。
    """)
    code("""
    t1 = select('t1')
    t1 = t1[t1.update_offset.isin(CHECKPOINTS)]
    fig, axes = plt.subplots(1, 2, figsize=(12.6, 4.5), layout='constrained')
    for metric, axis in zip(['wape', 'hit_rate'], axes):
        for model in ['F28', 'R28-D', 'R28-W']:
            rows = t1[t1.model.eq(model)].sort_values('update_offset')
            axis.plot(rows.update_offset, rows[metric], marker=MARKERS[model], color=COLORS[model],
                      label=model, linestyle='--' if model == 'R28-W' else '-')
        axis.set_xticks(CHECKPOINTS)
        axis.set_xlabel('Update day R − S (remaining length decreases)')
        axis.set_title('WAPE' if metric == 'wape' else 'Within ±20% · positive actual records')
        axis.yaxis.set_major_formatter(PercentFormatter(1))
        axis.set_ylim(0, 1 if metric == 'hit_rate' else None)
        axis.grid(axis='y', alpha=.2)
        axis.legend(frameon=False)
    fig.suptitle('T1 · fixed original endpoint · 385 frozen core records', fontsize=14)
    fig.savefig(FIGURES / 't1_remaining_checkpoints.png', bbox_inches='tight')
    plt.show()
    display(t1[['update_offset', 'model', 'scored_n', 'hit_n', 'miss_n', 'unjudged_n',
                'wape', 'bias', 'hit_sales_share']].sort_values(['update_offset', 'model']))
    """)
    md("""
    ### B/T2：相同R的新完整60天
    固定前两次签发279记录用作纵向对照，避免8月批次逐步不成熟改变组成。
    385主名单与全部三窗口仍保留，不能用279替代整体覆盖；该图也不将跨R高度重叠窗口当独立实验。
    """)
    code("""
    t2 = select('t2', scope='t2_balanced_279', window='d01_60')
    fig, axes = plt.subplots(1, 2, figsize=(12.6, 4.5), layout='constrained')
    for metric, axis in zip(['wape', 'hit_rate'], axes):
        for model in ['R28-D', 'R28-W']:
            rows = t2[t2.model.eq(model)].sort_values('update_offset')
            axis.plot(rows.update_offset, rows[metric], color=COLORS[model], label=model,
                      linestyle='-' if model == 'R28-D' else '--')
        axis.set_xticks([0, 7, 14, 21, 30, 45, 59])
        axis.set_xlabel('Update day R − S · every target remains 60 days')
        axis.set_title('WAPE' if metric == 'wape' else 'Within ±20% · positive actual records')
        axis.yaxis.set_major_formatter(PercentFormatter(1))
        axis.set_ylim(0, 1 if metric == 'hit_rate' else None)
        axis.grid(axis='y', alpha=.2)
        axis.legend(frameon=False)
    fig.suptitle('T2 · complete new 60-day windows · fixed April/June panel (279)', fontsize=14)
    fig.savefig(FIGURES / 't2_full60_balanced.png', bbox_inches='tight')
    plt.show()
    full_maturity = select('t2', model='R28-D')
    display(full_maturity[full_maturity.update_offset.isin(CHECKPOINTS)][
        ['update_offset', 'window', 'fixed_core_n', 'output_n', 'truth_n', 'scored_n',
         'not_matured_n', 'unknown_truth_n', 'hit_n', 'unjudged_n']].sort_values(['update_offset', 'window']))
    """)
    md("""
    ### 退化与操作波动
    T1检查点30按店铺查看日更相对冻结F28的配对变化；正的delta_wape表示变差。
    修订量统一换为同等60天长度，避免剩余天数减少制造修订。它是重叠预测的修订强度，不是实际下单或撤单成本。
    """)
    code("""
    pairs = frames['paired']
    shops = pairs[pairs.track.eq('T1') & pairs.scope.eq('full_core') & pairs['slice'].eq('shop') &
                  pairs.update_offset.eq(30) & pairs.comparison.eq('F28->R28-D')]
    display(shops[['shop', 'paired_n', 'delta_wape', 'delta_over_units', 'delta_under_units',
                   'improved_n', 'degraded_n', 'unchanged_n', 'degraded_sales_weight']].sort_values('delta_wape', ascending=False))
    revision = frames['revision']
    revision_table = revision.groupby('model').agg(
        fixed_core_records=('spu', 'size'),
        median_nonzero_revisions=('nonzero_revisions', 'median'),
        median_direction_reversals=('direction_reversals', 'median'),
        median_absolute_60day_revision=('standardized60_absolute_revision', 'median'),
        max_single_60day_revision=('max_standardized60_revision', 'max'))
    display(revision_table)
    """)
    md("""
    ## Takeaways
    该Notebook是主报告的可运行结果复核附件，不替代逐款CSV/Parquet和完整独立核验。
    是否改善按同轨道、同R、同目标日期、同样本判断；缺失、零销量和未成熟真值保持原状态。
    首次签发仍以T0为准；T1的短剩余期收益不能包装成初始60天能力。T2固定长度有助于识别该差别。
    既有核心名单未来销量覆盖缺口仍存在。即使某些滚动检查点指标改善，也不等于全店核心产品前瞻逐款两个月±20%已达标。
    本轮不选择生产模型、不改V4/A16/NEW_VISIBLE、不触碰采购、数据库、BI及调度。
    """)
    code("""
    final_hashes = {file: hashlib.sha256(Path(file).read_bytes()).hexdigest()
                    for file in PARAMETERS['expected_hashes']}
    assert final_hashes == PARAMETERS['expected_hashes']
    assert manifests['rolling_manifest']['status'] == 'COMPLETE'
    assert manifests['diagnostic_manifest']['status'] == 'COMPLETE'
    assert manifests['validation']['status'].startswith('PASS')
    assert manifests['diagnostic_validation']['status'].startswith('PASS')
    display(Markdown('**复核附件执行完成；所读取来源文件哈希未变。**'))
    """)
    notebook = nbformat.v4.new_notebook(cells=cells, metadata={
        "kernelspec": {"name": "bc60-v2-review", "display_name": "BC60 V2 review", "language": "python"},
        "language_info": {"name": "python", "version": sys.version.split()[0]},
        "scope": "READ_EXISTING_SUMMARIES_ONLY_NO_FITS_NO_SOURCE_QUERIES"})
    nbformat.validate(notebook)
    target = out / "BC60_V2_结果复核.ipynb"
    nbformat.write(notebook, target)
    manager = KernelManager(kernel_name="bc60-v2-review", connection_file=str(runtime / 'kernel_connection.json'),
                            kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(kernels)]))
    started_at = dt.datetime.now(dt.timezone.utc).isoformat()
    client = NotebookClient(notebook, km=manager, timeout=120, resources={"metadata": {"path": str(out)}}, allow_errors=False)
    try:
        client.execute()
    finally:
        nbformat.write(notebook, target)
    nbformat.validate(notebook)
    errors = [output for cell in notebook.cells for output in cell.get("outputs", []) if output.output_type == "error"]
    if errors:
        raise AssertionError("NOTEBOOK_ERROR_OUTPUT")
    render_notebook_preview(notebook, out / "BC60_V2_notebook_preview.html")
    manifest = {"status": "EXECUTED_TOP_TO_BOTTOM", "started_at": started_at,
                "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "generator_sha256": sha256(Path(__file__)), "source_hashes": evidence,
                "executed_code_cells": sum(c.cell_type == "code" for c in notebook.cells),
                "error_outputs": len(errors), "new_fits": 0, "source_queries": 0,
                "notebook": str(target), "notebook_sha256": sha256(target),
                "figure_files": {p.name: sha256(p) for p in sorted((out / 'figures').glob('*.png'))},
                "visual_inspection": "PENDING_EXPLICIT_IMAGE_INSPECTION",
                "html_browser_interaction": "NOT_VERIFIED"}
    (out / "notebook_execution.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rolling", type=Path, required=True)
    parser.add_argument("--diagnostic", type=Path, required=True)
    parser.add_argument("--validation", type=Path, required=True)
    parser.add_argument("--diagnostic-validation", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(**vars(args)), ensure_ascii=False, indent=2))
