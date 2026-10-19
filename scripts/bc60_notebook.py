"""Make and execute a local inspection notebook from existing BC60 outputs only."""
import argparse,pathlib,os
import nbformat as nbf
from nbclient import NotebookClient


def make(root):
    root=pathlib.Path(root).resolve();dest=root/'BC60_结果复核.ipynb'
    if dest.exists():raise FileExistsError(dest)
    os.environ.setdefault('IPYTHONDIR',str(root/'.ipython'))
    os.environ.setdefault('JUPYTER_CONFIG_DIR',str(root/'.jupyter'))
    os.environ.setdefault('JUPYTER_RUNTIME_DIR',str(root/'.jupyter_runtime'))
    nb=nbf.v4.new_notebook()
    nb.metadata['kernelspec']={'display_name':'Python 3','language':'python','name':'python3'}
    nb.cells=[
      nbf.v4.new_markdown_cell('# B+C核心款60天预测实验 V1\n\n## tl;dr\n\n首轮实际效果计算已完成，业务目标未达到。这里直接复核已冻结输出，不重新训练或查询数据库。所有时期均为重建历史回放，不是独立OOS。'),
      nbf.v4.new_markdown_cell('## Context & Methods\n\n四个美国店铺，店铺×SPU。先用全店截点前90天销量选累计≥90%的固定名单，再分成熟资格；两个分段及60天累计分别验收。缺预测保留在名单分母。训练只使用可用截止日前的一天标签；没有未完成60天标签参与训练或调参。\n\n### Key Assumptions\n\n签发日2026-04-02、06-02、08-02，可用延迟假设2天。流量、广告未来输入只延续截点前均值。历史实际获取时间不可恢复，品类等当前属性不用于训练。'),
      nbf.v4.new_code_cell("from pathlib import Path\nimport json, pandas as pd, numpy as np\nimport matplotlib.pyplot as plt\nfrom IPython.display import display\nP=Path.cwd()\nmetrics=pd.read_csv(P/'effects_v1/metrics.csv')\ncommon=pd.read_csv(P/'effects_v1/secondary_common_metrics.csv')\ncoverage=pd.read_csv(P/'validation_v1_checked/coverage_with_rates.csv')\nchecks=json.loads((P/'validation_v1_checked/validation.json').read_text(encoding='utf-8'))\nplt.rcParams.update({'figure.figsize':(10,4.5),'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':130})\nassert checks['truth_max_difference']==0 and checks['forecast_window_max_difference']==0\nprint('Independent numeric reconciliation:', checks['status'])"),
      nbf.v4.new_markdown_cell('## Data\n\n来源SQL、原生执行计划、查询时间和原始日数据分别位于 evidence 与 daily_raw。标准化数据在 inputs_v1_final；名单及预测在 effects_v1。下表为固定名单覆盖，而非按结果重新选出的名单。'),
      nbf.v4.new_code_cell("c=coverage[coverage.window.eq('d01_60')].copy()\ndisplay(c[['issue_date','shop','fixed_core_n','selection_coverage','target_coverage']])\nassert c.selection_coverage.ge(.9).all()\nprint('Distinct core shop-SPU:',checks['unique_core_shop_spu'])\nprint('Core issue objects:',checks['core_issue_objects'])"),
      nbf.v4.new_markdown_cell('## Results\n\n### 固定名单命中率与同样本误差\n\n左图保留全部固定核心对象作为命中率分母。右图为369个共同可评分对象；只作公平模型比较，不能代替完整名单验收。'),
      nbf.v4.new_code_cell("order=['rolling28','weekday4','B1','B2','B3']\nwins=['d01_30','d31_60','d01_60']\ncolors=['#334155','#8a99aa','#2d75bb','#b88b23','#a65a53']\na=metrics[metrics.slice.eq('overall')].pivot(index='window',columns='model',values='hit_rate').reindex(wins)[order]*100\nb=common.pivot(index='window',columns='model',values='wape').reindex(wins)[order]*100\nfig,axes=plt.subplots(1,2,figsize=(13,4.5))\na.plot.bar(ax=axes[0],color=colors,rot=0);b.plot.bar(ax=axes[1],color=colors,rot=0)\naxes[0].set(title='Fixed-core item hit rate',ylabel='Percent within +/-20%',xlabel='Forecast window')\naxes[1].set(title='Same-scored-sample WAPE',ylabel='WAPE (%)',xlabel='Forecast window')\nfor ax in axes:ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True);ax.legend(fontsize=8)\nfig.tight_layout();fig.savefig(P/'notebook_model_comparison.png');plt.show()\ndisplay(common[['model','window','scored_n','wape','bias','over_units','under_units']])"),
      nbf.v4.new_markdown_cell('### 每步增量与三个窗口同时命中\n\n不把累计命中当作两个分段通过。正的ΔWAPE是退化，增加的高估数量不能被较少低估掩盖。'),
      nbf.v4.new_code_cell("display(pd.read_csv(P/'validation_v1_checked/increments.csv'))\ndisplay(pd.read_csv(P/'validation_v1_checked/all_three_windows_summary.csv'))\nperiod=metrics[metrics.slice.eq('issue')&metrics.window.eq('d01_60')]\ndisplay(period[['issue_date','model','fixed_core_n','scored_n','hit_rate','wape','bias']])"),
      nbf.v4.new_markdown_cell('## Takeaways\n\n本轮日级基线优于固定B1；B2、B3未带来总体改善。不能据此认定所有学习型方案均无效，也不能宣布基线达到业务目标。过去90天核心覆盖不保证未来60天覆盖。V4/A16未在无事前公平适配器的情况下混入日窗口排名。生产模型、采购、BI及调度保持不变，本轮停止。')]
    client=NotebookClient(nb,timeout=180,kernel_name='python3',resources={'metadata':{'path':str(root)}})
    client.execute()
    nbf.validate(nb);nbf.write(nb,dest)
    print(dest)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('root');make(p.parse_args().root)
