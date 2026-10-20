"""Review completed monthly replay artifacts; no fitting, SQL, or live inputs.

Both the editable notebook and the companion app consume these verified local
summary files. Unknown cells remain unknown, and the original-cohort panel never
silently replaces the monthly frozen population.
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
import numpy as np
import pandas as pd

MODELS = ['F28', 'WEEKDAY4', 'B1', 'B2', 'B3', 'DIRECT30x2']
WINDOWS = ['d01_30', 'd31_60', 'd01_60']


def sha(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def require_final_validation(validation, manifests):
    if validation.get('status') != 'PASS_INDEPENDENT_FULL_RECONCILIATION':
        raise ValueError('INDEPENDENT_FULL_VALIDATION_NOT_PASS')
    bindings = [('prepared','prepare_manifest_sha256','prepared'),
                ('predictions','effect_manifest_sha256','effects'),
                ('scores','score_manifest_sha256','scoring')]
    for section, field, owner in bindings:
        if validation.get(section,{}).get(field) != sha(manifests[owner]):
            raise ValueError('VALIDATION_MANIFEST_BINDING_MISMATCH:' + owner)


def require_native_validation(validation, manifest):
    expected = 'PASS_INDEPENDENT_NATIVE_TRUTH_SUMMARY_HASHES_NOT_PREDICTION_REPLAY'
    if validation.get('status') != expected or validation.get('native_predictions_regenerated') is not False:
        raise ValueError('NATIVE_VALIDATION_SCOPE_MISMATCH')
    if validation.get('execution_manifest_sha256') != sha(manifest):
        raise ValueError('NATIVE_VALIDATION_MANIFEST_MISMATCH')


def load_review(root, validation):
    root, validation = Path(root).resolve(), Path(validation).resolve()
    manifests = {
        'inputs': root / 'inputs/input_manifest.json',
        'prepared': root / 'prepared/prepare_manifest.json',
        'effects': root / 'effects/run_manifest.json',
        'scoring': root / 'scoring/score_manifest.json',
        'validation': validation,
    }
    meta = {key: read_json(path) for key, path in manifests.items()}
    require_final_validation(meta['validation'],manifests)
    if meta['inputs'].get('status') != 'COMPLETE':
        raise ValueError('INPUT_PREPARATION_NOT_COMPLETE')
    if not meta['effects'].get('source_unchanged'):
        raise ValueError('SOURCE_PRESERVATION_NOT_VERIFIED')
    if meta['effects'].get('formal_fit_count', 101) > 100:
        raise ValueError('FORMAL_FIT_BUDGET_EXCEEDED')
    if sha(manifests['prepared']) != meta['effects']['prepared_manifest_sha256']:
        raise ValueError('PREPARED_BINDING_MISMATCH')
    paths = {
        'grid': root / 'prepared/issue_grid.csv',
        'selection': root / 'prepared/selection_quality.csv',
        'original': root / 'prepared/original_core_panel.csv',
        'training': root / 'effects/training_coverage.csv',
        'metrics': root / 'scoring/metrics.csv',
        'cell_status': root / 'scoring/full_issue_status_grid.csv',
        'joint': root / 'scoring/joint_summary.csv',
        'pairs': root / 'scoring/paired_f28.csv',
        'common': root / 'scoring/all_model_common.csv',
        'shape': root / 'scoring/f28_shape_summary.csv',
        'future': root / 'scoring/future_coverage.csv',
    }
    for key, path in paths.items():
        owner = meta[path.parent.name]
        if sha(path) != owner['files'][path.name]:
            raise ValueError('REVIEW_SOURCE_HASH_MISMATCH:' + key)
    hashes = {str(path): sha(path) for path in [*paths.values(), *manifests.values()]}
    frames = {key: pd.read_csv(path) for key, path in paths.items()}
    if sha(root / 'inputs/quality.json') != meta['inputs']['files']['quality.json']:
        raise ValueError('INPUT_QUALITY_HASH_MISMATCH')
    frames['input_quality'] = read_json(root / 'inputs/quality.json')
    hashes[str(root / 'inputs/quality.json')] = sha(root / 'inputs/quality.json')
    side_path = root / 'scoring/side_panel_provenance.json'
    if sha(side_path) != meta['scoring']['files'][side_path.name]:
        raise ValueError('SIDE_PANEL_PROVENANCE_HASH_MISMATCH')
    frames['side_provenance'] = read_json(side_path)
    hashes[str(side_path)] = sha(side_path)
    native = root / 'scoring/native_monthly_side_panel.csv'
    if native.exists():
        if sha(native) != meta['scoring']['files'][native.name]:
            raise ValueError('NATIVE_SIDE_PANEL_HASH_MISMATCH')
        frames['native_monthly'] = pd.read_csv(native)
        paths['native_monthly'] = native
        hashes[str(native)] = sha(native)
    extension_manifest = root / 'native_v4_extension/execution_manifest.json'
    if extension_manifest.exists():
        extension_meta = read_json(extension_manifest)
        if extension_meta.get('status') != 'COMPLETE' or not extension_meta.get('source_unchanged'):
            raise ValueError('V4_NATIVE_EXTENSION_INCOMPLETE')
        extension = extension_manifest.parent / 'native_v4_summary.csv'
        if sha(extension) != extension_meta['files'][extension.name]:
            raise ValueError('V4_NATIVE_EXTENSION_HASH_MISMATCH')
        frames['v4_extension'] = pd.read_csv(extension)
        frames['v4_extension_meta'] = extension_meta
        native_validation_path = root / 'validation_native_v4/validation.json'
        frames['v4_extension_validation'] = read_json(native_validation_path)
        require_native_validation(frames['v4_extension_validation'],extension_manifest)
        hashes[str(native_validation_path)] = sha(native_validation_path)
        paths['v4_extension'] = extension
        hashes[str(extension)] = sha(extension)
        hashes[str(extension_manifest)] = sha(extension_manifest)
    return {'root': root, 'paths': paths, 'manifests': manifests,
            'meta': meta, 'frames': frames, 'hashes': hashes}


def display_tables(review):
    """Only reshape already-scored metrics; never rerun predictions or scoring."""
    frames = review['frames']
    grid = frames['grid']
    mature = set(grid.loc[grid.calendar_mature.astype(str).str.lower().eq('true'), 'issue_month'])
    metrics = frames['metrics']
    primary = metrics[metrics.cohort_id.eq('monthly_core') & metrics['slice'].eq('issue')].copy()
    primary['calendar_mature'] = primary.issue_month.isin(mature)
    completed = primary[primary.calendar_mature].copy()
    summary = []
    additive = ['core_n', 'output_n', 'truth_n', 'scored_n', 'positive_truth_n', 'hit_n',
                'miss_n', 'unjudged_n', 'actual_units', 'known_core_actual_units', 'forecast_units',
                'over_units', 'under_units', 'zero_actual_n', 'zero_absolute_error']
    for (model, window), group in completed.groupby(['model', 'window']):
        record = {'model': model, 'window': window, 'issue_count': group.issue_month.nunique()}
        record.update({name: group[name].sum(min_count=1) for name in additive})
        denominator = record['actual_units']
        record['wape'] = (record['over_units'] + record['under_units']) / denominator if denominator > 0 else np.nan
        record['bias'] = (record['over_units'] - record['under_units']) / denominator if denominator > 0 else np.nan
        record['hit_rate'] = record['hit_n'] / record['positive_truth_n'] if record['positive_truth_n'] > 0 else np.nan
        record['hit_fixed_share'] = record['hit_n'] / record['core_n'] if record['core_n'] > 0 else np.nan
        known = record['known_core_actual_units']
        record['hit_sales_share'] = (group.hit_sales_share * group.known_core_actual_units).sum(min_count=1) / known if known > 0 else np.nan
        summary.append(record)
    summary = pd.DataFrame(summary)
    official = metrics[metrics.cohort_id.eq('monthly_core') &
                       metrics['slice'].eq('overall_mature_overlapping_issue_records')].copy()
    if not official.empty:
        if official.duplicated(['model', 'window']).any():
            raise ValueError('DUPLICATE_MATURE_SCORING_SUMMARY')
        check = summary.merge(official, on=['model', 'window'], suffixes=('_display', '_scored'),
                              how='outer', validate='one_to_one', indicator=True)
        if not check['_merge'].eq('both').all():
            raise ValueError('MATURE_SUMMARY_SCOPE_MISMATCH')
        for name in additive + ['wape', 'bias', 'hit_rate', 'hit_fixed_share', 'hit_sales_share']:
            if not np.isclose(check[name+'_display'], check[name+'_scored'], rtol=1e-12,
                              atol=1e-9, equal_nan=True).all():
                raise ValueError('MATURE_SUMMARY_RECONCILIATION_FAILED:' + name)
        summary = official.merge(summary[['model','window','issue_count']], on=['model','window'],
                                 validate='one_to_one')
    extremes = []
    for (model, window), group in completed.groupby(['model', 'window']):
        for metric, direction in [('wape', True), ('hit_rate', False)]:
            eligible = group[group[metric].notna()]
            if eligible.empty:
                continue
            for label, ascending in [('BEST', direction), ('WORST', not direction)]:
                value = eligible.sort_values([metric, 'issue_month'], ascending=[ascending, True]).iloc[0]
                extremes.append({'model': model, 'window': window, 'metric': metric, 'extreme': label,
                                 'issue_month': value.issue_month, 'value': value[metric],
                                 'core_n': value.core_n, 'scored_n': value.scored_n})
    joint = frames['joint'][frames['joint'].cohort_id.eq('monthly_core')].copy()
    joint['issue_month'] = joint.issue_date.astype(str).str[:7]
    joint = joint.pivot_table(index=['issue_date', 'issue_month', 'model'], columns='decision', values='records',
                              aggfunc='sum', fill_value=0).reset_index()
    for state in ['HIT', 'MISS', 'UNJUDGED']:
        if state not in joint:
            joint[state] = 0  # Absent count category, not missing sales or an uncomputed metric.
    joint['core_n'] = joint[['HIT', 'MISS', 'UNJUDGED']].sum(axis=1)
    joint['joint_fixed_share'] = joint.HIT / joint.core_n.where(joint.core_n.gt(0))
    joint['calendar_mature'] = joint.issue_month.isin(mature)
    shape = frames['shape'][frames['shape'].cohort_id.eq('monthly_core')].copy()
    shape['issue_month'] = shape.issue_date.astype(str).str[:7]
    pairs = frames['pairs'][frames['pairs'].cohort_id.eq('monthly_core') & frames['pairs']['slice'].eq('issue')].copy()
    return {'primary': primary, 'summary': summary, 'extremes': pd.DataFrame(extremes),
            'joint_display': joint, 'shape_display': shape, 'pair_display': pairs}


def preview(notebook, path):
    blocks = []
    for cell in notebook.cells:
        if cell.cell_type == 'markdown':
            blocks.append('<section><pre class="markdown">' + html.escape(cell.source) + '</pre></section>')
            continue
        blocks.append('<details><summary>可编辑计算单元</summary><pre>' + html.escape(cell.source) + '</pre></details>')
        for output in cell.get('outputs', []):
            data = output.get('data', {})
            if 'image/png' in data:
                blocks.append('<figure><img alt="已执行月度回放证据图" src="data:image/png;base64,' + data['image/png'] + '"></figure>')
            elif 'text/html' in data:
                blocks.append('<section class="output">' + data['text/html'] + '</section>')
            elif 'text/plain' in data:
                blocks.append('<pre class="output">' + html.escape(data['text/plain']) + '</pre>')
            elif output.get('output_type') == 'stream':
                blocks.append('<pre>' + html.escape(output['text']) + '</pre>')
    path.write_text('<!doctype html><html lang="zh"><meta charset="utf-8"><title>逐月回放结果复核</title>'
        '<style>body{font:16px system-ui,sans-serif;color:#263344;max-width:1180px;margin:28px auto;padding:20px;background:#fff}'
        'pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:13px;line-height:1.6}.markdown{font:16px system-ui;line-height:1.8}'
        'section,details{margin:22px 0}summary{color:#53677d;cursor:pointer}img{max-width:100%;height:auto}'
        'table{border-collapse:collapse;font-size:13px}th,td{padding:7px;border-bottom:1px solid #ddd;text-align:right}'
        '.output{overflow:auto}figure{margin:20px 0}</style><body>' + ''.join(blocks) + '</body></html>', encoding='utf-8')


def run(root, validation, out):
    review = load_review(root, validation)
    out = Path(out).resolve()
    if out.exists():
        raise ValueError('OUTPUT_ALREADY_EXISTS')
    out.mkdir(parents=True)
    runtime = out / 'runtime'
    spec = runtime / 'kernels/monthly-replay-review'
    spec.mkdir(parents=True)
    (spec / 'kernel.json').write_text(json.dumps({
        'argv': [sys.executable, '-B', '-m', 'ipykernel_launcher', '-f', '{connection_file}'],
        'display_name': 'Monthly replay result review', 'language': 'python',
        'env': {'IPYTHONDIR': str(runtime / 'ipython'), 'MPLCONFIGDIR': str(runtime / 'matplotlib'),
                'JUPYTER_RUNTIME_DIR': str(runtime), 'JUPYTER_CONFIG_DIR': str(runtime / 'jupyter_config'),
                'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONIOENCODING': 'utf-8'}}, indent=2), encoding='utf-8')
    parameters = {'root': str(review['root']), 'validation': str(Path(validation).resolve()),
                  'source': str(Path(__file__).resolve()), 'output': str(out), 'hashes': review['hashes']}
    parameters['hashes'][parameters['source']] = sha(__file__)
    cells = []
    def md(value): cells.append(nbformat.v4.new_markdown_cell(textwrap.dedent(value).strip()))
    def code(value): cells.append(nbformat.v4.new_code_cell(textwrap.dedent(value).strip()))
    md('''
    # 2025年起逐月预测效果与季节经营差异：结果复核
    ## tl;dr
    本文件只读取已完成且通过独立核验的结果，不重训、不取数、不选择生产模型。
    核心名单逐期冻结；下列数量为店铺×SPU×签发期记录，不能当作跨期去重商品或销量。
    ''')
    code('PARAMETERS = ' + repr(parameters))
    code('''
    from pathlib import Path
    import hashlib, importlib.util, json
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter
    from matplotlib.dates import DateFormatter
    from IPython.display import display, Markdown
    for source, digest in PARAMETERS['hashes'].items():
        assert hashlib.sha256(Path(source).read_bytes()).hexdigest() == digest, source
    spec = importlib.util.spec_from_file_location('monthly_review', PARAMETERS['source'])
    review_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(review_module)
    review = review_module.load_review(PARAMETERS['root'], PARAMETERS['validation'])
    frames, meta = review['frames'], review['meta']
    tables = review_module.display_tables(review)
    MODELS = review_module.MODELS
    COLORS = {'F28':'#53677d','WEEKDAY4':'#a3aeb8','B1':'#1b70a9','B2':'#c38a28','B3':'#8d6aa4','DIRECT30x2':'#8b9145'}
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,
                         'axes.spines.right':False,'axes.titlepad':12,'figure.dpi':120,'savefig.dpi':160})
    pd.set_option('display.max_rows',26)
    pd.set_option('display.max_columns',16)
    FIGURES = Path(PARAMETERS['output']) / 'figures'
    FIGURES.mkdir(exist_ok=True)
    def month_ticks(axis, dates):
        unique = sorted(pd.to_datetime(pd.Series(dates).dropna()).unique())
        if unique:
            positions = np.unique(np.linspace(0, len(unique)-1, min(5,len(unique))).round().astype(int))
            axis.set_xticks([unique[position] for position in positions])
            axis.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    quality = frames['input_quality']
    grid = frames['grid']
    matured = grid.calendar_mature.astype(str).str.lower().eq('true')
    display(Markdown(f"- 已核验日源范围：{quality['first_day']} 至 {quality['last_day']}。"
        f"签发网格 {len(grid)} 期，其中完整60天已到期 {int(matured.sum())} 期。\\n"
        f"- 正式学习型拟合 {meta['effects']['formal_fit_count']} 次；独立核验：{meta['validation']['status']}。\\n"
        "- 合并误差按件数分母重算，不平均月度WAPE；相邻60天窗口重叠，不是独立销量。\\n"
        "- 结果属于扩展历史重训回放，不证明2024/2025记录在当时已经获取，也不构成独立OOS。"))
    display(tables['summary'][['model','window','core_n','output_n','truth_n','scored_n','wape','bias','hit_n','hit_rate']])
    ''')
    md('''
    ## Context & Methods
    ### Key Assumptions
    每月2日模拟签发S；输入截止C=S−2。W1=S+1至S+30，W2=S+31至S+60，W60=S+1至S+60。
    每期、每店按此前90天销量累计≥90%并纳入边界同量商品选款，不用目标期销量选款。
    WAPE只在同一模型有输出且真值完整的记录计算；跨模型公平比较另看与F28配对表。
    缺输出、无资格、缺真值和未成熟保留状态；明确零实销只报告绝对误差，不制造百分比。
    当前属性仅可作描述，不能回写过去输入；经营变化线索不等于经营因果。
    ## Data
    ''')
    code('''
    display(frames['grid'])
    display(frames['selection'].groupby(['issue_month','selection_status'],dropna=False)
        .agg(shops=('shop','size'),core_n=('core_n',lambda x:x.sum(min_count=1)),
             full_shop_units=('full_shop_units',lambda x:x.sum(min_count=1)),
             selected_units=('selected_units',lambda x:x.sum(min_count=1))))
    display(pd.DataFrame([{'source':key,'sha256':review_module.sha(path)} for key,path in review['manifests'].items()]))
    display(pd.DataFrame([{'status':key,'records':value} for key,value in quality['source_state_counts'].items()]))
    ''')
    md('''
    ## Results
    ### 1. 每个月都保留，不能只看合并成绩
    色块来自当期完整核心范围中的可评分记录；旁表保留核心N、输出N、真值N与未计算原因。
    空白格不是零误差。每个模型都显示最差和最好月份；不据此拼接事后选模策略。
    ''')
    code('''
    primary=tables['primary']
    months=frames['grid'].issue_month.tolist()
    matrices=[primary[primary.window.eq(w)].pivot(index='issue_month',columns='model',values='wape')
              .reindex(index=months,columns=MODELS) for w in review_module.WINDOWS]
    finite=np.concatenate([m.to_numpy()[np.isfinite(m.to_numpy())] for m in matrices])
    vmax=float(finite.max()) if len(finite) else 1.
    fig,axes=plt.subplots(1,3,figsize=(15,10),sharey=True,layout='constrained')
    palette=plt.colormaps['Blues'].copy();palette.set_bad('#eeeeee')
    for axis,matrix,label in zip(axes,matrices,['Days 1–30','Days 31–60','Days 1–60']):
        image=axis.imshow(np.ma.masked_invalid(matrix.to_numpy()),cmap=palette,vmin=0,vmax=vmax,aspect='auto')
        axis.set_xticks(range(len(MODELS)),['F28','WD4','B1','B2','B3','DIR'])
        axis.set_yticks(range(len(months)),months)
        axis.set_title(label)
        for row in range(len(months)):
            for col in range(len(MODELS)):
                value=matrix.iloc[row,col]
                text='—' if pd.isna(value) else f'{value:.0%}'
                axis.text(col,row,text,ha='center',va='center',fontsize=8,
                          color='white' if pd.notna(value) and value>.60*vmax else '#263344')
    axes[0].set_ylabel('Issue month; target dates are not calendar months')
    fig.colorbar(image,ax=axes,label='WAPE on observed paired units',format=PercentFormatter(1),shrink=.8)
    fig.suptitle('Monthly frozen-core error map · unknown cells are not zero',fontsize=15)
    fig.savefig(FIGURES/'monthly_error_map.png',bbox_inches='tight');plt.show()
    display(tables['extremes'])
    display(frames['cell_status'].groupby(['issue_month','cell_status']).size().unstack(fill_value=0))
    ''')
    md('''
    ### 2. 同时命中、覆盖与公平对照
    三窗口同时命中要求两个30天分段及60天合计均在±20%。分母为每期完整核心记录，含缺预测。
    对模型误差差异的公平判断使用各候选与F28的同对象配对；六模型交集只作辅助。
    ''')
    code('''
    joint=tables['joint_display']
    display(joint[['issue_month','model','core_n','HIT','MISS','UNJUDGED','joint_fixed_share']])
    display(tables['pair_display'][['issue_month','model','window','full_core_n','paired_n',
        'candidate_wape','f28_wape','wape_change','improved_n','degraded_n']])
    display(frames['future'].groupby(['issue_date','window','status']).agg(
        shop_rows=('shop','size'),observed_core_units=('observed_core_units','sum'),
        complete_core_n=('complete_window_core_n','sum')))
    auxiliary=frames['metrics'][frames['metrics'].cohort_id.ne('monthly_core') & frames['metrics']['slice'].eq('overall_overlapping_issue_records')]
    display(auxiliary[['cohort_id','model','window','core_n','scored_n','wape','bias','hit_n','unjudged_n']])
    ''')
    md('''
    ### 3. 训练跨度不能用训练行数替代
    递归模型与直接模型的训练标签单位不同，训练行数不能直接比较。
    DIRECT的日期窗口可能高度重叠；非重叠窗口只是时间广度描述，不是证明独立样本量。
    `complete_365_days`仅表示365日源记录完整，包含明确0；不证明持续在售、供给正常或完整可比经营季节。
    ''')
    code('''
    training=frames['training']
    columns=['issue_date','kind','status','available_calendar_days','train_rows','train_objects',
             'train_dates','train_start','train_end','train_span_days','max_global_nonoverlap',
             'positive_labels','zero_labels','rows_after_lag_drop','effective_label_first','effective_label_last']
    display(training[[c for c in columns if c in training]])
    fig,axes=plt.subplots(1,2,figsize=(13,4.5),layout='constrained')
    for kind,color in [('B1','#1b70a9'),('M1','#c38a28'),('M2','#8b9145')]:
        selected=training[training.kind.eq(kind)].sort_values('issue_date')
        if 'train_span_days' in selected:
            axes[0].plot(pd.to_datetime(selected.issue_date),selected.train_span_days,marker='o',markersize=3,label=kind,color=color)
        if kind in ['M1','M2'] and 'max_global_nonoverlap' in selected:
            axes[1].plot(pd.to_datetime(selected.issue_date),selected.max_global_nonoverlap,marker='o',markersize=3,label=kind,color=color)
    axes[0].set_title('Calendar training support');axes[0].set_ylabel('Days between first and last training date')
    axes[1].set_title('DIRECT label-window breadth');axes[1].set_ylabel('Maximum non-overlapping label windows')
    for axis in axes:
        axis.set_ylim(bottom=0);axis.grid(alpha=.18);axis.legend();axis.set_xlabel('Issue date')
        month_ticks(axis, training.loc[training.train_span_days.notna(),'issue_date'])
    fig.savefig(FIGURES/'training_time_support.png',bbox_inches='tight');plt.show()
    ''')
    md('''
    ### 4. F28的水平与形状限制
    F28两段都等于30×此前28天日均，60天为两段之和。
    只有两段实销均为正时计算shape_ratio；大于1.5意味着两段等量预测不可能同时命中±20%。
    这是事后形状限制，不是季节已确认，更不能回填成预测时的模型选择条件。
    ''')
    code('''
    shape=tables['shape_display'].sort_values('issue_date')
    fig,axis=plt.subplots(figsize=(13,4.5),layout='constrained')
    dates=pd.to_datetime(shape.issue_date)
    axis.plot(dates,shape.object_share,marker='o',label='Object share among positive actual pairs',color='#1b70a9')
    axis.plot(dates,shape.sales_share_positive_both,marker='s',linestyle='--',label='Actual-unit share among positive pairs',color='#c38a28')
    axis.set_ylim(0,1);axis.yaxis.set_major_formatter(PercentFormatter(1))
    axis.set_xlabel('Issue date');axis.set_ylabel('Share within stated positive-pair denominator')
    axis.set_title('F28 equal-segment incompatibility · actual segment ratio > 1.5')
    axis.grid(alpha=.18);axis.legend(loc='upper left',bbox_to_anchor=(0,1.02),frameon=False)
    month_ticks(axis, shape.loc[shape.object_share.notna() | shape.sales_share_positive_both.notna(),'issue_date'])
    fig.savefig(FIGURES/'f28_shape_limits.png',bbox_inches='tight');plt.show()
    display(shape[['issue_month','core_n','positive_both_n','incompatible_shape_n','object_share',
                   'sales_share_positive_both','zero_or_unknown_n']])
    ''')
    md('''
    ## Takeaways
    逐期误差、命中和覆盖必须同时解释；最佳WAPE不一定对应最多逐款命中。
    训练历史、商品构成和经营指标可能共同变化，月份标签不能单独证明季节原因。
    月度原生V4/A16与滚动更新只作独立旁表，不混入首次签发60天排名。
    本Notebook不拟合、查询或更改任何原归档；不切换生产模型，不追加配置搜索。
    ''')
    code('''
    display(pd.DataFrame([{'panel':key,'evidence':json.dumps(value,ensure_ascii=False)}
                         for key,value in frames['side_provenance'].items()]))
    if 'native_monthly' in frames:
        display(Markdown('### 原归档原生自然月旁表（旧范围，不混入日级排名）'))
        display(frames['native_monthly'])
    if 'v4_extension' in frames:
        display(Markdown('### 新补充V4原生自然月旁表（成熟可观察4月以上）\\n'
            '本表与日级90%核心名单不同。仅V4扩展，A16未重新校准；使用当前归档季节，'
            '不证明历史点时可获取。独立旁表核验已通过：真值、资格、日期、覆盖、判定、汇总与哈希；'
            '该核验没有独立重新计算V4预测，不等于预测公式第二次复现。'))
        display(pd.DataFrame([{'status':frames['v4_extension_validation']['status'],
            'native_predictions_regenerated':frames['v4_extension_validation']['native_predictions_regenerated'],
            'completed_at':frames['v4_extension_validation']['completed_at']}]))
        native=frames['v4_extension']
        display(native[native.group_by.eq('target_month+horizon')][['target_month','horizon','eligible_n',
            'output_n','truth_n','scored_n','wape','bias','hit20_n','hit20_rate','unknown_actual_n']])
    assert all(review_module.sha(path)==digest for path,digest in PARAMETERS['hashes'].items())
    display(Markdown('**执行完成：读取文件哈希未变，新增拟合和数据查询均为0。**'))
    ''')
    notebook = nbformat.v4.new_notebook(cells=cells, metadata={
        'kernelspec': {'name': 'monthly-replay-review', 'display_name': 'Monthly replay result review', 'language': 'python'},
        'language_info': {'name': 'python', 'version': sys.version.split()[0]},
        'scope': 'READ_VERIFIED_SUMMARIES_ONLY_NO_FITS_NO_QUERIES'})
    nbformat.validate(notebook)
    path = out / '2025年起逐月回放结果复核.ipynb'
    nbformat.write(notebook, path)
    manager = KernelManager(kernel_name='monthly-replay-review', connection_file=str(runtime / 'kernel_connection.json'),
        kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(runtime / 'kernels')]))
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        NotebookClient(notebook, km=manager, timeout=180, resources={'metadata': {'path': str(out)}}, allow_errors=False).execute()
    finally:
        nbformat.write(notebook, path)
    errors = [output for cell in notebook.cells for output in cell.get('outputs', []) if output.output_type == 'error']
    if errors:
        raise ValueError('NOTEBOOK_ERROR_OUTPUT')
    nbformat.validate(notebook)
    preview(notebook, out / 'monthly_notebook_preview.html')
    result = {'status': 'EXECUTED_TOP_TO_BOTTOM', 'started_at': started,
        'completed_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'generator_sha256': sha(__file__),
        'notebook_sha256': sha(path), 'sources': review['hashes'], 'error_outputs': len(errors),
        'code_cells': sum(cell.cell_type == 'code' for cell in notebook.cells), 'new_fits': 0, 'source_queries': 0,
        'figures': {p.name: sha(p) for p in (out / 'figures').glob('*.png')},
        'visual_inspection': 'PENDING_EXPLICIT_IMAGE_INSPECTION', 'browser_interaction': 'NOT_VERIFIED'}
    (out / 'notebook_execution.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--validation', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    print(json.dumps(run(**vars(parser.parse_args())), ensure_ascii=False, indent=2))
