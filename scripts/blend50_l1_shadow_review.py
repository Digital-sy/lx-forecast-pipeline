"""Private, network-free manual review artifacts from a successfully sealed run."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

KEY = ['issue_date', 'shop', 'spu']
MODELS = ['F28', 'DIRECT30x2', 'YEARAGO_COMPONENT', 'DIRECT_YEARAGO_BLEND50',
          'DIRECT_L1_V1', 'BLEND50_L1_MEAN_V1']
CANDIDATE = MODELS[-1]
BLEND = 'DIRECT_YEARAGO_BLEND50'
WINDOWS = {'d01_30': (1, 30), 'd31_60': (31, 60), 'd01_60': (1, 60)}
MANUAL_COLUMNS = ['run_id', 'shop', 'spu', 'window', 'original_candidate',
                  'manual_reference_demand', 'adjustment_reason', 'owner', 'confirmed_at']


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(4194304), b''):
            h.update(b)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def dump(path, data):
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2,
                                    default=str, allow_nan=False), encoding='utf-8')


def clock():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def records(frame):
    data = frame.astype(object).where(pd.notna(frame), None).to_dict('records')
    return json.loads(json.dumps(data, ensure_ascii=False, default=str, allow_nan=False))


def unique(frame, keys, label):
    if frame[keys].isna().any().any() or frame.duplicated(keys).any():
        raise ValueError('INVALID_KEYS:' + label)


def projected(path, required, optional=()):
    names = pq.ParquetFile(path).schema_arrow.names
    if not set(required).issubset(names):
        raise ValueError('MISSING_FIELDS:' + str(path) + ':' + repr(sorted(set(required) - set(names))))
    return pd.read_parquet(path, columns=required + [c for c in optional if c in names])


def normalize_date_keys(frame):
    out = frame.copy()
    if 'issue_date' in out:
        out['issue_date'] = pd.to_datetime(out.issue_date, errors='raise').dt.strftime('%Y-%m-%d')
    for col in ['shop', 'spu']:
        if out[col].isna().any():
            raise ValueError('NULL_OBJECT_KEY')
        out[col] = out[col].astype(str)
    return out


def assemble_rows(core, predictions, routing, issue_date, run_id):
    """Pure formatting and range validation; no model or future-truth access."""
    core, predictions, routing = [normalize_date_keys(x) for x in [core, predictions, routing]]
    if 'issue_date' not in core:
        core['issue_date'] = issue_date
    if 'issue_date' not in routing:
        routing['issue_date'] = issue_date
    for name, f in [('core', core), ('routing', routing)]:
        unique(f, KEY, name)
        if not f.issue_date.eq(issue_date).all():
            raise ValueError('ISSUE_DATE_MISMATCH:' + name)
    unique(predictions, KEY + ['model', 'window'], 'predictions')
    if not predictions.issue_date.eq(issue_date).all():
        raise ValueError('ISSUE_DATE_MISMATCH:predictions')
    if set(predictions.model) != set(MODELS) or set(predictions.window) != set(WINDOWS):
        raise ValueError('MODEL_OR_WINDOW_SCOPE')
    if len(predictions) != len(core) * len(MODELS) * len(WINDOWS):
        raise ValueError('INCOMPLETE_MODEL_OBJECT_STATES')
    if np.isinf(predictions.prediction.to_numpy(float)).any() or predictions.prediction.lt(0).any():
        raise ValueError('INVALID_PREDICTION_VALUE')
    pred_objects = predictions[KEY].drop_duplicates()
    check = core[KEY].merge(pred_objects, on=KEY, how='outer', indicator=True, validate='one_to_one')
    if not check._merge.eq('both').all():
        raise ValueError('CORE_OBJECT_SCOPE_CHANGED')
    for window, (a, b) in WINDOWS.items():
        f = predictions[predictions.window.eq(window)]
        if not pd.to_datetime(f.target_start).eq(pd.Timestamp(issue_date) + pd.Timedelta(days=a)).all():
            raise ValueError('WINDOW_START_MISMATCH')
        if not pd.to_datetime(f.target_end).eq(pd.Timestamp(issue_date) + pd.Timedelta(days=b)).all():
            raise ValueError('WINDOW_END_MISMATCH')
    if not pd.to_datetime(predictions.input_cutoff).eq(pd.Timestamp(issue_date) - pd.Timedelta(days=2)).all():
        raise ValueError('INPUT_CUTOFF_MISMATCH')
    pivot = predictions.pivot(index=KEY + ['window'], columns='model', values='prediction').reset_index()
    status = predictions.pivot(index=KEY + ['window'], columns='model', values='prediction_status').reset_index()
    status = status.rename(columns={m: m + '_status' for m in MODELS})
    row = pivot.merge(status, on=KEY + ['window'], validate='one_to_one')
    dates = predictions[KEY + ['window', 'input_cutoff', 'target_start', 'target_end']].drop_duplicates()
    unique(dates, KEY + ['window'], 'window_dates')
    row = row.merge(dates, on=KEY + ['window'], validate='one_to_one')
    row = row.merge(core, on=KEY, validate='many_to_one')
    allowed = ['input_status_M1', 'input_status_M2', 'gate_decision', 'main_reason']
    if not set(allowed).issubset(routing):
        raise ValueError('MISSING_ROUTING_STATUS')
    row = row.merge(routing[KEY + allowed], on=KEY, how='left', validate='many_to_one')
    if row[allowed].isna().any().any():
        raise ValueError('ROUTING_JOIN_MISSING')
    row['run_id'] = run_id
    row['qualification'] = np.where(row.input_status_M1.eq('OK') & row.input_status_M2.eq('OK'),
                                    '满足原老品资格', '候选不覆盖')
    row['candidate_status'] = row[CANDIDATE + '_status']
    row['candidate_output'] = row[CANDIDATE].notna()
    row['model_disagreement_units'] = row[CANDIDATE] - row[BLEND]
    row['model_disagreement_ratio'] = np.where(row[BLEND].gt(0), row.model_disagreement_units / row[BLEND], np.nan)
    row['model_disagreement_status'] = np.select(
        [row[CANDIDATE].isna() | row[BLEND].isna(), row[BLEND].eq(0)],
        ['MISSING_ENDPOINT', 'ZERO_DENOMINATOR'], default='DEFINED')
    row['actual_status'] = '待成熟'
    row['error_status'] = '待成熟'
    row['hit_status'] = '待成熟'
    if len(row) != len(core) * 3:
        raise ValueError('REVIEW_JOIN_EXPANSION')
    return row.sort_values(['shop', 'spu', 'window']).reset_index(drop=True)


def production_reference(document, review_rows):
    """A separate source panel; only exact target-window quantity definitions compare."""
    if document is None:
        return {'status': 'NOT_AVAILABLE', 'reason': '本批未取得合法现役预测快照；不影响影子独立签发。', 'records': []}
    if document.get('status') not in ['NOT_AVAILABLE', 'NOT_COMPARABLE', 'AVAILABLE']:
        raise ValueError('INVALID_PRODUCTION_REFERENCE_STATUS')
    result = {'status': document['status'], 'reason': document.get('reason', ''), 'records': []}
    seen = set()
    for original in document.get('records', []):
        allowed = ['shop', 'spu', 'target_start', 'target_end', 'target_month', 'forecast_qty',
                   'model_name', 'model_version', 'issue_date', 'published_at', 'input_cutoff',
                   'unit', 'quantity_definition', 'source_version']
        r = {k: original.get(k) for k in allowed}
        r.update(comparability='NOT_COMPARABLE', candidate_prediction=None,
                 model_disagreement_units=None, model_disagreement_ratio=None,
                 difference_status='NOT_COMPARABLE')
        matching = review_rows[(review_rows.shop == r['shop']) & (review_rows.spu == r['spu']) &
                               (review_rows.target_start.astype(str) == str(r['target_start'])) &
                               (review_rows.target_end.astype(str) == str(r['target_end']))]
        known_quantity = isinstance(r['forecast_qty'], (int, float)) and np.isfinite(r['forecast_qty']) and r['forecast_qty'] >= 0
        comparable = (document['status'] == 'AVAILABLE' and len(matching) == 1 and
                      r['unit'] == '件' and r['quantity_definition'] == '销量预测件数' and known_quantity)
        if comparable:
            key = (r['shop'], r['spu'], r['target_start'], r['target_end'])
            if key in seen:
                raise ValueError('AMBIGUOUS_PRODUCTION_REFERENCE_BATCH')
            seen.add(key)
            value = matching.iloc[0][CANDIDATE]
            r['comparability'] = 'SAME_WINDOW_DIFFERENT_ISSUE_OR_INPUT'
            if pd.notna(value):
                r['candidate_prediction'] = float(value)
                r['model_disagreement_units'] = float(value - r['forecast_qty'])
                r['difference_status'] = 'ZERO_DENOMINATOR' if r['forecast_qty'] == 0 else 'DEFINED'
                if r['forecast_qty'] > 0:
                    r['model_disagreement_ratio'] = float((value - r['forecast_qty']) / r['forecast_qty'])
            else:
                r['difference_status'] = 'MISSING_CANDIDATE'
        result['records'].append(r)
    return result


def manual_template(rows):
    out = rows[['run_id', 'shop', 'spu', 'window', CANDIDATE]].rename(columns={CANDIDATE: 'original_candidate'}).copy()
    for col in MANUAL_COLUMNS[5:]:
        out[col] = ''
    return out[MANUAL_COLUMNS]


STYLE = r'''
:root{font-family:"Microsoft YaHei",system-ui,sans-serif;color:#252b34;background:#f4f5f7;font-size:14px}*{box-sizing:border-box}body{margin:0}header,main,footer{max-width:1800px;margin:auto;padding:20px 26px}h1{font-size:25px;margin:0 0 10px}h2{font-size:18px;margin:0 0 12px}p{line-height:1.65}.muted,small{color:#626b78}.notice{border-left:3px solid #8995a1;background:#edf0f3;padding:10px 14px}section,details{background:#fff;border:1px solid #dfe4e9;border-radius:8px;padding:16px;margin-bottom:16px}.controls{display:grid;grid-template-columns:repeat(5,minmax(130px,1fr)) auto;gap:12px;align-items:end}label{display:flex;flex-direction:column;gap:7px;font-size:13px}input,select,button{font:inherit;padding:9px;border:1px solid #c6cdd5;border-radius:4px;background:#fff}button{cursor:pointer}.kpis{display:flex;gap:12px;flex-wrap:wrap;margin:12px 0}.kpi{border:1px solid #e3e7eb;padding:12px;min-width:180px;flex:1}.kpi span{display:block;color:#626b78}.kpi strong{display:block;font-size:23px;margin-top:6px}.table-scroll{overflow:auto;max-height:620px}table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:9px 10px;border-bottom:1px solid #e5e9ed;text-align:right;white-space:nowrap}th{background:#f2f4f7;position:sticky;top:0;z-index:1}th[data-sort]{cursor:pointer}td.left,th.left{text-align:left}tbody tr:hover{background:#f7f9fb}tr.missing{color:#707b88}.negative{color:#935426}.status{font-weight:600}.meta{overflow-wrap:anywhere;font-size:12px}.toolbar{display:flex;gap:12px;justify-content:space-between;align-items:center;flex-wrap:wrap}.model-note{font-size:12px}summary{cursor:pointer;font-weight:600}footer{font-size:12px;color:#66717e}#error{color:#9b2c2c}select:focus,input:focus,button:focus{outline:2px solid #738ca2;outline-offset:2px}@media(max-width:1000px){.controls{grid-template-columns:repeat(3,1fr)}}@media(max-width:620px){header,main,footer{padding:12px}.controls{grid-template-columns:repeat(2,1fr)}h1{font-size:20px}}
'''

HTML = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; connect-src 'none'; object-src 'none'; base-uri 'none'">
<title>核心款滚动销量预测｜影子人工参考</title><style>__STYLE__</style></head><body>
<header><h1>核心款滚动销量预测｜人工参考</h1><p class="notice"><strong>影子试运行／人工参考，未替代生产，不自动下单。</strong> 人工参考需求不是采购数量。尚无完整未来实销，误差与命中统一显示“待成熟”。</p><div id="metadata" class="meta"></div></header>
<main><section><h2>本批覆盖</h2><div id="coverage" class="kpis"></div><div id="shopCoverage" class="table-scroll"></div><p class="model-note">核心名单历史销量覆盖、满足资格的历史销量覆盖、对象输出覆盖分别列示，不相互替代；未知不补0。全部核心对象均可查看，没有低销量隐藏规则。</p></section>
<section><div class="controls"><label>店铺<select id="shop"></select></label><label>SPU搜索<input id="spu" type="search" placeholder="输入款号"></label><label>资格/输出<select id="qualification"><option value="ALL">全部核心</option><option value="OUTPUT">候选有输出</option><option value="MISSING">候选不覆盖/缺输出</option></select></label><label>目标窗口<select id="window"></select></label><label>人工关注排序<select id="attention"><option value="history">近90天销量从高到低</option><option value="down">相对BLEND50下调件数</option><option value="disagreement">模型分歧绝对值</option><option value="missing">候选缺输出优先</option></select></label><button id="reset">清空筛选</button></div><p id="scope" role="status"></p><div class="toolbar"><small>点击表头可排序；数量展示2位小数，CSV保留未舍入值。差额是模型分歧，不是实际预测误差。</small><button id="export">导出当前参考CSV</button></div><div class="table-scroll"><table id="forecastTable"><thead></thead><tbody></tbody></table></div><p id="error" role="alert"></p></section>
<section><h2>现役预测参考｜独立口径</h2><p id="productionStatus"></p><p class="muted">自然月与滚动30天不能均摊拼接比较。即使目标窗口一致，签发时间/输入截止不同也不代表同条件效果比较；这里只展示合法同窗口的模型分歧，不计算准确率。</p><div class="table-scroll"><table id="productionTable"><thead></thead><tbody></tbody></table></div></section>
<details><summary>字段、缺输出与人工复核</summary><p>F28是近期销量基线；DIRECT和L1是本批独立两段模型；YEARAGO是年度组件，支持不足可无输出；BLEND50沿用冻结年度条件；最终组合每段等于0.5×BLEND50＋0.5×L1。GATE为假时仍为0.5×DIRECT＋0.5×L1，不等于只用DIRECT。</p><p>完整CSV附资格、GATE和缺输出原因。人工复核模板的人工参考值、原因、负责人和确认时间默认空，不代表已批准。模板独立保存，不覆盖原预测、不接采购/面料计算。</p><p id="maturityDates"></p><pre id="provenance" class="meta"></pre></details></main><footer>私有离线文件，内含业务数据，请勿公开上传。HTML无需外网；页面交互不会查询数据库、调用模型、改变预测或自动下单。</footer>
<script id="review-data" type="application/json">__DATA__</script><script>__SCRIPT__</script></body></html>'''

SCRIPT = r'''
'use strict';
const D=JSON.parse(document.getElementById('review-data').textContent);
const C='BLEND50_L1_MEAN_V1',B='DIRECT_YEARAGO_BLEND50';
const names={F28:'F28',DIRECT30x2:'DIRECT',YEARAGO_COMPONENT:'YEARAGO组件',DIRECT_YEARAGO_BLEND50:'BLEND50',DIRECT_L1_V1:'L1',BLEND50_L1_MEAN_V1:'组合版'};
const windowNames={d01_30:'前30天',d31_60:'后30天',d01_60:'累计60天'};
const $=id=>document.getElementById(id),finite=v=>typeof v==='number'&&Number.isFinite(v);
const num=v=>finite(v)?v.toLocaleString('zh-CN',{maximumFractionDigits:2}):'—';
const percent=v=>finite(v)?(100*v).toFixed(2)+'%':'—';
let sorting={key:'selection_qty',descending:true};let visible=[];
function text(parent,tag,value,cls){const el=document.createElement(tag);el.textContent=value??'—';if(cls)el.className=cls;parent.appendChild(el);return el;}
function selectOptions(el,values,all){if(all){const o=new Option('全部店铺','ALL');el.appendChild(o);}for(const v of values)el.appendChild(new Option(v,v));}
function compare(a,b,key,desc){let x=a[key],y=b[key];if(key==='absolute_disagreement'){x=finite(a.model_disagreement_units)?Math.abs(a.model_disagreement_units):null;y=finite(b.model_disagreement_units)?Math.abs(b.model_disagreement_units):null;}if(x===null||x===undefined)return y===null||y===undefined?0:1;if(y===null||y===undefined)return -1;const d=finite(x)&&finite(y)?x-y:String(x).localeCompare(String(y),'zh-CN');return desc?-d:d;}
function makeHeader(table,cols,sortable){const tr=document.createElement('tr');table.tHead.replaceChildren(tr);for(const [key,label] of cols){const th=text(tr,'th',label);if(sortable){th.dataset.sort=key;th.addEventListener('click',()=>{sorting={key,descending:sorting.key===key?!sorting.descending:true};render();});}}}
const cols=[['shop','店铺'],['spu','SPU'],['qualification','资格'],['selection_qty','近90天销量'],...D.models.map(m=>[m,names[m]]),['model_disagreement_units','组合−BLEND50／件'],['model_disagreement_ratio','模型分歧比例'],['candidate_status','缺输出/状态'],['actual_status','未来实销'],['error_status','误差'],['hit_status','命中']];
function render(){const shop=$('shop').value,spu=$('spu').value.trim().toUpperCase(),q=$('qualification').value,w=$('window').value;visible=D.rows.filter(r=>(shop==='ALL'||r.shop===shop)&&(!spu||r.spu.toUpperCase().includes(spu))&&(q==='ALL'||(q==='OUTPUT'?r.candidate_output:!r.candidate_output))&&r.window===w);visible.sort((a,b)=>compare(a,b,sorting.key,sorting.descending)||a.shop.localeCompare(b.shop)||a.spu.localeCompare(b.spu));const body=$('forecastTable').tBodies[0];body.replaceChildren();for(const r of visible){const tr=document.createElement('tr');if(!r.candidate_output)tr.className='missing';tr.dataset.shop=r.shop;tr.dataset.spu=r.spu;for(const [key] of cols){let value=r[key];if(key==='model_disagreement_ratio')value=r.model_disagreement_status==='ZERO_DENOMINATOR'?'分母为0':percent(value);else if(D.models.includes(key)||key==='selection_qty'||key==='model_disagreement_units')value=num(value);const td=text(tr,'td',value);if(['shop','spu','qualification','candidate_status'].includes(key))td.className='left';if(finite(r[key]))td.title=String(r[key]);}body.appendChild(tr);}const win=D.windows[w];$('scope').textContent=`${windowNames[w]} ${win.target_start}—${win.target_end}｜当前 ${visible.length} 条；候选有输出 ${visible.filter(r=>r.candidate_output).length} 条。完整核心 ${D.metadata.core_n} 个对象。排序：${sorting.key} ${sorting.descending?'降序':'升序'}`;}
function csvCell(v){const s=v===null||v===undefined?'':String(v);return '"'+s.replaceAll('"','""')+'"';}
function exportVisible(){const keys=Object.keys(D.rows[0]||{});const value='\ufeff'+[keys.map(csvCell).join(','),...visible.map(r=>keys.map(k=>csvCell(r[k])).join(','))].join('\r\n');const blob=new Blob([value],{type:'text/csv;charset=utf-8'});const url=URL.createObjectURL(blob);const a=document.createElement('a');a.href=url;a.download=D.metadata.run_id+'_人工参考.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
$('metadata').textContent=`批次：${D.metadata.run_id}｜版本：${D.metadata.code_commit}｜签发日：${D.metadata.issue_date}｜输入截止：${D.metadata.input_cutoff}｜封存：${D.metadata.sealed_at}`;
for(const [label,value] of [['完整核心对象',D.metadata.core_n],['满足原老品资格',D.metadata.qualified_n],['组合完整输出对象',D.metadata.output_n]]){const x=text($('coverage'),'div','', 'kpi');text(x,'span',label);text(x,'strong',num(value));}
const st=document.createElement('table');const sh=st.createTHead();sh.appendChild(document.createElement('tr'));const sb=st.createTBody();for(const t of ['店铺','全店近90天销量','核心承载销量','核心销量覆盖','资格销量覆盖','对象输出覆盖'])text(sh.rows[0],'th',t);for(const r of D.coverage){const tr=sb.insertRow();for(const v of [r.shop,num(r.full_shop_units),num(r.selected_units),percent(r.selection_coverage),percent(r.qualified_sales_coverage),percent(r.output_object_coverage)])text(tr,'td',v);} $('shopCoverage').appendChild(st);
selectOptions($('shop'),[...new Set(D.rows.map(r=>r.shop))].sort(),true);for(const w of ['d01_30','d31_60','d01_60'])$('window').appendChild(new Option(windowNames[w],w));$('window').value='d01_60';makeHeader($('forecastTable'),cols,true);
for(const id of ['shop','qualification','window'])$(id).addEventListener('change',render);$('spu').addEventListener('input',render);$('attention').addEventListener('change',()=>{sorting={key:({history:'selection_qty',down:'model_disagreement_units',disagreement:'absolute_disagreement',missing:'candidate_output'})[$('attention').value],descending:['history','disagreement'].includes($('attention').value)};render();});$('reset').addEventListener('click',()=>{$('shop').value='ALL';$('spu').value='';$('qualification').value='ALL';$('window').value='d01_60';$('attention').value='history';sorting={key:'selection_qty',descending:true};render();});$('export').addEventListener('click',exportVisible);
$('productionStatus').textContent=D.production_reference.status+'｜'+D.production_reference.reason;
const pcols=[['shop','店铺'],['spu','SPU'],['target_month','自然月'],['target_start','目标开始'],['target_end','目标结束'],['forecast_qty','现役预测'],['model_name','现役模型'],['model_version','版本'],['issue_date','签发日'],['published_at','发布时间'],['input_cutoff','输入截止'],['comparability','可比性'],['candidate_prediction','同窗候选'],['model_disagreement_units','合法同窗分歧']];makeHeader($('productionTable'),pcols,false);for(const r of D.production_reference.records){const tr=$('productionTable').tBodies[0].insertRow();for(const [k] of pcols)text(tr,'td',['forecast_qty','candidate_prediction','model_disagreement_units'].includes(k)?num(r[k]):r[k]);}
$('maturityDates').textContent=`前30天目标结束：${D.windows.d01_30.target_end}；完整60天目标结束：${D.windows.d01_60.target_end}。结束后仍需按既定数据延迟核验实销成熟；本页不承诺自动评分。`;
$('provenance').textContent=JSON.stringify(D.provenance,null,2);render();window.__reviewReady=true;
'''


def render_html(payload):
    data = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    data = data.replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    return HTML.replace('__STYLE__', STYLE).replace('__DATA__', data).replace('__SCRIPT__', SCRIPT)


def build(run, out, production=None):
    run, out = Path(run).resolve(), Path(out).resolve()
    if out.exists() or out == run or run in out.parents:
        raise ValueError('OUTPUT_MUST_BE_NEW_AND_OUTSIDE_SEALED_RUN')
    seal_path = run / 'issued/issue_seal.json'
    seal = read(seal_path)
    if seal.get('status') != 'ISSUED_SHADOW_AWAITING_ACTUALS':
        raise ValueError('SUCCESSFUL_ISSUE_SEAL_REQUIRED')
    files = {k.replace('\\', '/'): v for k, v in seal['files'].items()}
    sources = {str(seal_path): sha(seal_path)}

    def source(rel):
        path = run / rel
        if rel not in files or sha(path) != files[rel]:
            raise ValueError('SEALED_SOURCE_CHANGED:' + rel)
        sources[str(path)] = files[rel]
        return path

    core = projected(source('prepared/core.parquet'), ['shop', 'spu', 'selection_qty'],
                     ['issue_date', 'lifecycle', 'maturity', 'shop_selection_total'])
    pred = projected(source('issued/predictions.parquet'), KEY + ['model', 'window', 'prediction',
                    'prediction_status', 'input_cutoff', 'target_start', 'target_end'])
    routing = projected(source('issued/routing_predictions.parquet'), ['shop', 'spu',
                        'input_status_M1', 'input_status_M2', 'gate_decision', 'main_reason'], ['issue_date'])
    quality = pd.read_csv(source('prepared/selection_quality.csv'), float_precision='round_trip')
    if not set(['shop', 'full_shop_units', 'selected_units', 'selection_coverage']).issubset(quality):
        raise ValueError('MISSING_SELECTION_COVERAGE_FIELDS')
    unique(quality, ['shop'], 'selection_quality')
    rows = assemble_rows(core, pred, routing, seal['issue_date'], run.name)
    representatives = rows[rows.window.eq('d01_60')]
    coverage = []
    for r in quality.itertuples():
        group = representatives[representatives.shop.eq(r.shop)]
        qualified_units = float(group.loc[group.qualification.eq('满足原老品资格'), 'selection_qty'].sum())
        coverage.append(dict(shop=r.shop, full_shop_units=r.full_shop_units, selected_units=r.selected_units,
                             selection_coverage=r.selection_coverage, core_n=len(group),
                             qualified_n=int(group.qualification.eq('满足原老品资格').sum()),
                             output_n=int(group.candidate_output.sum()),
                             qualified_sales_coverage=qualified_units/r.full_shop_units if r.full_shop_units>0 else None,
                             output_object_coverage=float(group.candidate_output.mean()) if len(group)>0 else None))
    prod_doc = None
    if production:
        path = Path(production).resolve()
        sources[str(path)] = sha(path)
        prod_doc = read(path)
    prod = production_reference(prod_doc, rows)
    windows = {w: {'target_start': str(rows[rows.window.eq(w)].iloc[0].target_start),
                   'target_end': str(rows[rows.window.eq(w)].iloc[0].target_end)} for w in WINDOWS}
    when = seal['sealed_at']
    if isinstance(when, dict):
        when = when.get('utc', json.dumps(when, ensure_ascii=False))
    payload = dict(metadata=dict(run_id=run.name, code_commit=seal['code_commit'], issue_date=seal['issue_date'],
                                input_cutoff=seal['input_cutoff'], sealed_at=when, core_n=len(core),
                                qualified_n=int(representatives.qualification.eq('满足原老品资格').sum()),
                                output_n=int(representatives.candidate_output.sum()), actual_status='待成熟'),
                   models=MODELS, windows=windows, coverage=coverage, rows=records(rows),
                   production_reference=prod, provenance=dict(sources=sources,
                       quantity='销量预测件数；人工参考不是采购数量', current_accuracy='NOT_MATURED',
                       source_limitations='继承本批输入manifest；没有用补录数据证明历史点时可获取性',
                       manual_confirmation='默认留空；不代表批准', offline=True))
    out.mkdir(parents=True, exist_ok=False)
    rows.to_csv(out/'逐款预测_人工参考.csv', index=False, encoding='utf-8-sig', float_format='%.17g')
    manual_template(rows).to_csv(out/'人工复核记录模板.csv', index=False, encoding='utf-8-sig', float_format='%.17g')
    dump(out/'review_data.json', payload)
    (out/'核心款滚动销量预测_人工参考.html').write_text(render_html(payload), encoding='utf-8')
    for path, expected in sources.items():
        if sha(path) != expected:
            raise ValueError('SOURCE_CHANGED_DURING_REVIEW:' + path)
    dump(out/'review_manifest.json', dict(status='REVIEW_FILES_BUILT_BROWSER_NOT_YET_VERIFIED',
         run_id=run.name, created_at=clock(), issue_seal_sha256=sha(seal_path), sources=sources,
         source_code_sha256=sha(__file__), core_n=len(core), rows=len(rows),
         model_count=len(MODELS), future_truth_read=False, model_inference_calls=0,
         production_reference_status=prod['status'], new_fits=0,
         files={f.name: sha(f) for f in out.iterdir() if f.is_file()}))
    print(json.dumps({'status': 'REVIEW_FILES_BUILT_BROWSER_NOT_YET_VERIFIED', 'out': str(out),
                      'core_n': len(core), 'rows': len(rows)}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--production-reference', dest='production')
    build(**vars(parser.parse_args()))
