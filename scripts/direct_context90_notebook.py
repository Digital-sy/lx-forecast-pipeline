"""Read-only numerical companion: saved results only, no fitting."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
import textwrap

import nbformat
from nbclient import NotebookClient
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager

try:
    from scripts import direct_context90_experiment as exp
    from scripts.bc60_monthly_notebook import preview
except ModuleNotFoundError:
    import direct_context90_experiment as exp
    from bc60_monthly_notebook import preview


def run(out, destination):
    root, dest = Path(out).resolve(), Path(destination).resolve()
    if dest.exists():
        raise ValueError("NEW_NOTEBOOK_DIRECTORY_REQUIRED")
    verification = exp.read_json(root/"validation/validation.json")
    if verification["status"] != "PASS":
        raise ValueError("VALIDATION_REQUIRED")
    paths = [root/"scoring"/name for name in [
        "summary.json", "metrics.csv", "joint_summary.csv", "hit_and_error_changes.csv",
        "paired_details.parquet", "score_manifest.json"]]
    paths.append(root/"validation/validation.json")
    hashes = {str(p): exp.sha(p) for p in paths}
    dest.mkdir(exist_ok=False)
    runtime = dest/"runtime"
    kernel = runtime/"kernels"/"context90-review"
    kernel.mkdir(parents=True)
    for name in ["ipython", "jupyter_config", "jupyter_data"]:
        (runtime/name).mkdir()
    exp.dump(kernel/"kernel.json", {
        "argv": [sys.executable, "-B", "-m", "ipykernel_launcher", "-f", "{connection_file}"],
        "display_name": "CONTEXT90 saved numerical review", "language": "python",
        "env": {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONIOENCODING": "utf-8",
                "IPYTHONDIR": str(runtime/"ipython"), "JUPYTER_CONFIG_DIR": str(runtime/"jupyter_config"),
                "JUPYTER_DATA_DIR": str(runtime/"jupyter_data"), "JUPYTER_RUNTIME_DIR": str(runtime)}})
    cells = []
    def md(s): cells.append(nbformat.v4.new_markdown_cell(textwrap.dedent(s).strip()))
    def code(s): cells.append(nbformat.v4.new_code_cell(textwrap.dedent(s).strip()))
    md("""# DIRECT_CONTEXT90 数值复核

    ## tl;dr
    本笔记仅复核已完成的单一增量实验，不拟合模型、不读取数据库。
    结论由下列实际评分表读取；技术校验不等于业务达标。
    """)
    code(f"""
    from pathlib import Path
    import hashlib, json
    import pandas as pd
    from IPython.display import display, Markdown
    ROOT = Path({str(root)!r})
    HASHES = {hashes!r}
    def verify():
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == h for p,h in HASHES.items())
    verify()
    summary = json.loads((ROOT/'scoring/summary.json').read_text(encoding='utf-8'))
    display(pd.DataFrame([{{'研究数值筛选': summary['numerical_screen'],
                           '完整核心记录':summary['full_core_n'],
                           '原DIRECT配对记录':summary['original_direct_output_n'],
                           '成熟签发期':summary['mature_issues']}}]))
    display(pd.DataFrame([{{'门槛':k,'通过':v}} for k,v in summary['gates'].items()]))
    """)
    md("""## Context & Methods

    2025-01 至 2026-08 的20个成熟签发期。评价单位为店铺×SPU×签发期；
    同款跨期重复，60天目标可重叠。本轮是重建历史开发验证，不是独立OOS。
    ### Key Assumptions
    S为签发日，C=S−2；两段分别为S+1..30和S+31..60。
    原资格、训练标签、旧特征、参数不变。只增加56/90日水平、近期差异和完整性标记。
    缺记录不补零；当前品类、季节仅为描述性分组，不进入训练。
    """)
    md("## Data\n\n精确输入、代码及结果哈希见同目录清单；以下检查报告来自独立原始日真值求和与模型检查点复核。")
    code("""
    validation = json.loads((ROOT/'validation/validation.json').read_text(encoding='utf-8'))
    display(pd.DataFrame([{k:validation[k] for k in [
        'status','formal_models','business_refits','old_training_rows_compared',
        'independent_daily_truth_windows','future_perturbation_prediction_checks']}]))
    """)
    md("## Results\n\n### 三窗口及联合命中：同时展示数量和实销权重")
    code("""
    metrics = pd.read_csv(ROOT/'scoring/metrics.csv')
    joint = pd.read_csv(ROOT/'scoring/joint_summary.csv')
    paired = metrics[metrics.scope.eq('ORIGINAL_DIRECT_OUTPUT') & metrics.slice.eq('overall')]
    display(paired[['model','window','fixed_n','output_n','wape','bias','over_units',
                    'under_units','hit_n','hit_fixed_share','hit_actual_share']])
    display(joint[joint.scope.eq('ORIGINAL_DIRECT_OUTPUT') & joint.slice.eq('overall')][
        ['model','fixed_n','joint_hit_n','joint_hit_actual','joint_hit_actual_share']])
    """)
    md("### 下降观察组\n\n按签发前资料定义；不等于已确认未来真实衰退。")
    code("""
    display(metrics[metrics.scope.eq('ORIGINAL_DIRECT_OUTPUT') & metrics.slice.eq('decline_group') &
                    metrics.decline_group.eq('PRE_ISSUE_DECLINING')][
        ['model','window','fixed_n','products','periods','wape','over_rate','over_units','under_units','hit_n']])
    """)
    md("### 分期稳定性和命中迁移\n\n只读取已评分表；本次按用户要求以数值为先，不为图形重新训练。")
    code("""
    periods = metrics[metrics.scope.eq('ORIGINAL_DIRECT_OUTPUT') & metrics.slice.eq('issue') &
                      metrics.window.eq('d01_60')]
    display(periods.pivot(index='issue_date',columns='model',values='wape'))
    display(pd.read_csv(ROOT/'scoring/hit_and_error_changes.csv').query("scope == 'ORIGINAL_DIRECT_OUTPUT'"))
    """)
    md("""## Takeaways

    五项门槛按未四舍五入数值判断。任何一项冲突均保留，不追加搜索。
    本笔记不能将整体改善转换为生产授权；生产V4、冻结A16、采购、BI及调度不变。
    """)
    code("""
    verify()
    display(Markdown('已完成：输入结果哈希未变化；本笔记新增业务拟合0次、数据库查询0次。'))
    """)
    nb = nbformat.v4.new_notebook(cells=cells, metadata={
        "kernelspec": {"name": "context90-review", "display_name": "CONTEXT90 saved numerical review", "language": "python"},
        "language_info": {"name": "python", "version": sys.version.split()[0]}})
    nbformat.validate(nb)
    target = dest/"DIRECT_CONTEXT90数值复核.ipynb"
    nbformat.write(nb, target)
    manager = KernelManager(kernel_name="context90-review", connection_file=str(runtime/"connection.json"),
                            kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(runtime/"kernels")]))
    started = exp.now()
    try:
        NotebookClient(nb, km=manager, timeout=120, resources={"metadata": {"path": str(dest)}}, allow_errors=False).execute()
    finally:
        nbformat.write(nb, target)
    nbformat.validate(nb)
    errors = [o for c in nb.cells for o in c.get("outputs", []) if o.output_type == "error"]
    assert not errors
    preview(nb, dest/"numerical_review.html")
    exp.verify_hashes(hashes)
    exp.dump(dest/"execution.json", {
        "status": "EXECUTED_TOP_TO_BOTTOM", "started_at": started, "completed_at": exp.now(),
        "error_outputs": 0, "code_cells": sum(c.cell_type == "code" for c in nb.cells),
        "new_fits": 0, "new_queries": 0, "sources": hashes, "notebook_sha256": exp.sha(target),
        "browser_visual_check": "NOT_PERFORMED", "source_sha256": exp.sha(__file__)})
    print(json.dumps({"status": "EXECUTED_TOP_TO_BOTTOM", "new_fits": 0, "error_outputs": 0}))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True); p.add_argument("--destination", required=True)
    run(**vars(p.parse_args()))
