'use strict';
(() => {
const D=JSON.parse(document.getElementById('reviewed-data').textContent);
const $=id=>document.getElementById(id), NS='http://www.w3.org/2000/svg';
const N=new Intl.NumberFormat('zh-CN',{maximumFractionDigits:2}), I=new Intl.NumberFormat('zh-CN',{maximumFractionDigits:0});
const num=v=>v===null||!Number.isFinite(v)?'—':N.format(v), int=v=>v===null?'—':I.format(v), pct=v=>v===null||!Number.isFinite(v)?'—':(v*100).toFixed(2)+'%';
const labels=Object.fromEntries(D.models.map(m=>[m.id,m.label]));
const maturityLabel=v=>({'4_5':'4—5个月','6_11':'6—11个月','12_plus':'12个月以上','UNKNOWN':'未知（UNKNOWN）'}[v]||v);
const defaults=['F28','DIRECT30x2','DIRECT_YEARAGO','DIRECT_YEARAGO_GATE_V1'];
const months=[...new Set(D.rows.map(r=>r.month))].sort();
const state={window:'d01_30',start:months[0],end:months.at(-1),shop:'',category:'',maturity:'',models:defaults.filter(m=>labels[m]),search:'',zoom:1};
const byId=new Map(D.rows.map(r=>[r.id,r]));
let observer, fixedId=null, unavailableLimit=50, svgSerial=0, highlighted=null;
const anchors=['#1A9850','#91CF60','#FEE08B','#FDAE61','#F46D43','#D73027'];
function color(ape){
 if(ape===null||!Number.isFinite(ape)||ape<0)return '#b7bdc5';
 if(ape>1)return '#000000'; if(ape===1)return anchors[5];
 const v=ape*5,lo=Math.floor(v),t=v-lo;
 const a=anchors[lo].match(/[0-9a-f]{2}/gi).map(x=>parseInt(x,16)),b=anchors[lo+1].match(/[0-9a-f]{2}/gi).map(x=>parseInt(x,16));
 return '#'+a.map((x,i)=>Math.round(x+(b[i]-x)*t).toString(16).padStart(2,'0')).join('').toUpperCase();
}
function valid(r,m){const z=r.values[m];return !!z&&z.state==='OK'&&typeof z.p==='number'&&Number.isFinite(z.p)&&z.p>=0;}
function threshold(win){return win==='d01_60'?20:10;}
function known(r){return r.truth==='OBSERVED'&&r.actual!==null&&Number.isFinite(r.actual)&&r.actual>=0;}
function matches(r){return r.window===state.window&&r.month>=state.start&&r.month<=state.end&&(!state.shop||r.shop===state.shop)&&(!state.category||r.category===state.category)&&(!state.maturity||r.maturity===state.maturity);}
function visible(r){return known(r)&&r.actual>=threshold(state.window);}
function filtered(){return D.rows.filter(matches);}
function shown(){return filtered().filter(visible);}
function searchMatch(r){const text=(r.shop+' '+r.spu).toUpperCase();return state.search.toUpperCase().trim().split(/\s+/).filter(Boolean).every(t=>text.includes(t));}
function result(r,m){
 const p=r.values[m]?.p;if(!known(r))return {p,ape:null,signed:null,ae:null,hit:false,dir:r.truth==='NOT_MATURED'?'目标期未成熟':'实销未知'};
 if(!valid(r,m))return {p,ape:null,signed:null,ae:null,hit:false,dir:r.values[m]?.state==='NO_PREDICTION'?'无预测':'预测异常'};
 const e=p-r.actual;return {p,ape:r.actual>0?Math.abs(e)/r.actual:null,signed:r.actual>0?e/r.actual:null,ae:Math.abs(e),hit:r.actual>0&&Math.abs(e)/r.actual<=.2,dir:e>0?'↑ 高估':e<0?'↓ 低估':'持平'};
}
function aggregate(rows,m){
 const k=rows.filter(known),s=k.filter(r=>valid(r,m));const actual=k.reduce((a,r)=>a+r.actual,0),sa=s.reduce((a,r)=>a+r.actual,0),ae=s.reduce((a,r)=>a+Math.abs(r.values[m].p-r.actual),0);
 const hits=s.filter(r=>result(r,m).hit),black=s.filter(r=>result(r,m).ape>1);
 return {n:k.length,products:new Set(k.map(r=>r.shop+'\0'+r.spu)).size,output:s.length,missing:k.filter(r=>r.values[m]?.state==='NO_PREDICTION').length,abnormal:k.filter(r=>!valid(r,m)&&r.values[m]?.state!=='NO_PREDICTION').length,actual,scoredActual:sa,ae,wape:sa?ae/sa:null,hit:hits.length,hitRate:k.length?hits.length/k.length:null,black:black.length,blackActual:black.reduce((a,r)=>a+r.actual,0)};
}
function el(tag,text,attrs={}){const n=document.createElement(tag);if(text!==undefined&&text!==null)n.textContent=text;for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);return n;}
function svgEl(tag,attrs={},text){const n=document.createElementNS(NS,tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;return n;}
function table(headers,rows){const t=el('table'),h=el('thead'),tr=el('tr');headers.forEach(x=>tr.append(el('th',x)));h.append(tr);t.append(h);const b=el('tbody');rows.forEach(row=>{const tr=el('tr');row.forEach(v=>tr.append(el('td',v)));b.append(tr);});t.append(b);return t;}
function options(id,items,first){const s=$(id);s.replaceChildren();if(first!==undefined)s.append(el('option',first,{value:''}));items.forEach(x=>{const v=typeof x==='string'?x:x.value,txt=typeof x==='string'?x:x.label;s.append(el('option',txt,{value:v}));});}
function bindSelect(id,field){$(id).addEventListener('change',()=>{state[field]=$(id).value;if(state.start>state.end){if(field==='start'){state.end=state.start;$('monthEnd').value=state.end;}else{state.start=state.end;$('monthStart').value=state.start;}}render();});}
function metricCells(m){return [labels[m]+' · '+m];}
function summaryTable(models,rows){return table(['模型','应展示','已输出 / 缺预测','±20%命中 / 完整展示占比','>100%数 / 实销权重','WAPE','评分对象 / 实销分母'],models.map(m=>{const s=aggregate(rows,m);return [...metricCells(m),int(s.n),int(s.output)+' / '+int(s.missing),int(s.hit)+' / '+pct(s.hitRate),int(s.black)+' / '+int(s.blackActual),pct(s.wape),int(s.output)+' / '+int(s.scoredActual)];}));}
function selectedMonths(){return months.filter(m=>m>=state.start&&m<=state.end);}
function areaLegend(parent,scale=1){
 const k=D.layouts[state.window].k,s=svgEl('svg',{viewBox:'0 0 430 120',width:'100%',height:120,'aria-label':'同窗口统一面积图例'});
 const quantities=[100,1000,10000];let x=4;
 quantities.forEach(a=>{const side=Math.sqrt(k*a)*scale;s.append(svgEl('rect',{x,y:15+70-side,width:side,height:side,fill:'#9ba3ae'}));s.append(svgEl('text',{x,y:106,'font-size':12,fill:'#4d5662'},int(a)+'件'));x+=Math.max(side+30,105);});
 parent.replaceChildren(el('strong','面积图例 · 原始画布尺度'),s,el('small','面积=k×实销；窗口切换可能改变k。月份放大按同倍率缩放，当前'+(state.zoom*100)+'%。'));
}
function legend(){
 const p=$('colorLegend');p.replaceChildren(el('strong','绝对误差率 · 固定色标'));
 p.append(el('div',null,{class:'colorbar'}));const tick=el('div',null,{class:'legend-ticks'});['0%','20% 目标','40%','60%','80%','100%'].forEach(t=>tick.append(el('span',t)));p.append(tick);
 const statuses=el('div',null,{class:'legend-states'});[['#000000','严格>100%'],['hatch','灰斜纹＝无预测']].forEach(([c,t])=>{const sp=el('span');const b=el('i',null,{class:'chip'+(c==='hatch'?' hatch':'')});if(c!=='hatch')b.style.background=c;sp.append(b,document.createTextNode(t));statuses.append(sp);});statuses.append(el('span','↑高估 / ↓低估；颜色不表达方向'));p.append(statuses);areaLegend($('areaLegend'));
}
function initSVG(model,month,rows,interactive=true){
 const serial='pattern-'+(++svgSerial),s=svgEl('svg',{viewBox:`0 0 ${D.width} ${D.height}`,class:'tile-svg',role:'img','aria-label':labels[model]+' '+month+' '+D.windows[state.window],'data-model':model,'data-month':month});
 const defs=svgEl('defs'),pattern=svgEl('pattern',{id:serial,width:6,height:6,patternUnits:'userSpaceOnUse',patternTransform:'rotate(45)'});pattern.append(svgEl('rect',{width:6,height:6,fill:'#d7dbe0'}),svgEl('line',{x1:0,y1:0,x2:0,y2:6,stroke:'#8d95a0','stroke-width':2}));defs.append(pattern);s.append(defs);
 for(const r of rows){const q=D.layouts[state.window].coordinates[r.id];if(!q)continue;const[x,y,side]=q,z=result(r,model),good=valid(r,model),fill=good?color(z.ape):'url(#'+serial+')';
  const rect=svgEl('rect',{x,y,width:side,height:side,fill,class:'tile','data-rid':r.id,'data-model':model,'data-area':r.actual,'tabindex':interactive?0:-1,'aria-label':r.shop+' '+r.spu+' 实销'+r.actual+' '+(good?'误差'+pct(z.ape):z.dir)});
  if(interactive&&state.search&&!searchMatch(r))rect.classList.add('faded');if(interactive&&state.search&&searchMatch(r))rect.classList.add('search-hit');s.append(rect);
  if(side>=45){const text=svgEl('text',{x:x+side/2,y:y+side/2-2,'text-anchor':'middle',class:'tile-label'+(good&&(z.ape>.78||z.ape<.08)?' light':'')},r.spu);s.append(text);s.append(svgEl('text',{x:x+side/2,y:y+side/2+12,'text-anchor':'middle',class:text.getAttribute('class')},good?(z.dir.startsWith('↑')?'↑':z.dir.startsWith('↓')?'↓':'')+pct(z.ape):'无预测'));}
  if(interactive){rect.addEventListener('pointerenter',ev=>{highlight(r.id);tooltip(r,model,ev);});rect.addEventListener('pointermove',moveTip);rect.addEventListener('pointerleave',()=>{$('tooltip').hidden=true;highlight(null);});rect.addEventListener('focus',()=>{highlight(r.id);tooltip(r,model,{clientX:20,clientY:120});});rect.addEventListener('blur',()=>{$('tooltip').hidden=true;highlight(null);});rect.addEventListener('click',()=>openDetail(r));rect.addEventListener('keydown',ev=>{if(ev.key==='Enter')openDetail(r);});}
 }
 return s;
}
function highlight(id){highlighted=id;document.querySelectorAll('.tile.linked').forEach(n=>n.classList.remove('linked'));if(id!==null)document.querySelectorAll('.tile[data-rid="'+id+'"]').forEach(n=>n.classList.add('linked'));}
function moveTip(ev){const t=$('tooltip');if(t.hidden)return;t.style.left=Math.max(8,Math.min(ev.clientX+16,innerWidth-t.offsetWidth-10))+'px';t.style.top=Math.max(8,Math.min(ev.clientY+14,innerHeight-t.offsetHeight-8))+'px';}
function tooltip(r,m,ev){
 const z=result(r,m),t=$('tooltip');t.replaceChildren(el('strong',labels[m]+' · '+m),el('div',r.shop+' / '+r.spu),el('div',r.month+'签发 · 签发日 '+r.issue),el('div',D.windows[r.window]+'：'+r.start+' — '+r.end),el('div','实际 '+int(r.actual)+' 件｜预测 '+num(z.p)+' 件'),el('div','绝对误差 '+num(z.ae)+' 件｜误差率 '+pct(z.ape)),el('div',z.dir+'｜有符号误差 '+(z.signed>0?'+':'')+pct(z.signed)),el('div','±20%：'+(z.ape===null?'不可评价':z.hit?'命中':'未命中')),el('div','来源 / 回退：'+r.values[m].origin,{class:'muted'}),el('div','当前品类 '+r.category+'｜原货龄 '+r.maturity+'｜当前季节 '+r.season,{class:'muted'}));t.hidden=false;moveTip(ev);
}
function openDetail(r){
 fixedId=r.id;const p=$('detailContent');p.replaceChildren(el('h2',r.shop+' · '+r.spu),el('p',r.issue+'签发｜'+D.windows[r.window]),el('p','目标 '+r.start+' — '+r.end),el('p','实际销量 '+int(r.actual)+' 件；当前品类 '+r.category+'，原货龄 '+r.maturity));
 const wrap=el('div',null,{class:'table-scroll'});wrap.append(table(['模型','预测','误差率','方向','±20%'],D.models.map(m=>{const z=result(r,m.id);return[m.label,num(z.p),pct(z.ape),z.dir,z.ape===null?'不可评价':z.hit?'命中':'未命中'];})));p.append(wrap);
 p.append(el('h3','该店铺×SPU跨期明细 · 当前窗口'));
 const cross=D.rows.filter(x=>x.shop===r.shop&&x.spu===r.spu&&x.window===state.window),c=el('div',null,{class:'table-scroll'});c.append(table(['签发日','目标期','实际',...state.models.map(m=>labels[m])],cross.map(x=>[x.issue,x.start+'—'+x.end,int(x.actual),...state.models.map(m=>num(x.values[m].p))])));p.append(c,el('h3','本期来源 / 回退'));
 D.models.forEach(m=>p.append(el('p',m.label+'：'+r.values[m.id].origin,{class:'muted'})));$('detail').hidden=false;highlight(r.id);
}
function monthCard(model,month,rows,lazy=true,withTitle=false){
 const card=el('div',null,{class:'month-card','data-model':model,'data-month':month});if(withTitle)card.append(el('h3',labels[model]+' · '+model));
 const top=el('div',null,{class:'month-top'});top.append(el('strong',month+'签发'));const zoom=el('button','放大',{type:'button','data-action':'enlarge'});zoom.onclick=()=>openMonth(month);top.append(zoom);card.append(top);
 const reference=D.rows.find(r=>r.month===month&&r.window===state.window);card.append(el('div',D.windows[state.window]+' · '+reference.start+' — '+reference.end,{class:'month-period'}));
 const host=el('div',null,{class:'svg-scroll'});card.append(host);
 const fill=()=>{if(host.dataset.ready)return;host.dataset.ready='1';if(reference.truth==='NOT_MATURED'){host.append(el('div','目标期未成熟 · NOT_MATURED',{class:'month-empty'}));return;}if(!rows.length){host.append(el('div','当前条件下无满足门槛记录',{class:'month-empty'}));return;}host.append(initSVG(model,month,rows));if(highlighted!==null)highlight(highlighted);};
 if(lazy){host.style.aspectRatio=D.width+'/'+D.height;card._fill=fill;observer.observe(card);}else fill();
 card.append(el('div',int(rows.length)+' 条对象期｜实销权重 '+int(rows.reduce((a,r)=>a+r.actual,0))+' 件',{class:'month-foot'}));return card;
}
function modelSection(model,rows){
 const section=el('details',null,{class:'model-section',open:''}),summary=el('summary',labels[model]+' ');summary.append(el('span',model,{class:'model-id'}));section.append(summary);
 const actions=el('div',null,{class:'model-actions'}),solo=el('button','只看此模型'),ex=el('button','导出此模型 SVG');solo.onclick=()=>{state.models=[model];syncChecks();render();};ex.onclick=()=>exportSVG([model],selectedMonths());actions.append(solo,ex);section.append(actions);
 const s=aggregate(rows,model),metrics=el('div',null,{class:'model-stats'});
 const vals=[['应展示对象期 / 不同店铺×SPU',int(s.n)+' / '+int(s.products),'当前筛选范围'],['已输出 / 缺预测',int(s.output)+' / '+int(s.missing),'异常 '+int(s.abnormal)],['±20% 命中',int(s.hit)+' / '+pct(s.hitRate),'完整展示分母 '+int(s.n)],['实际销量权重',int(s.actual)+' 件','可评分销量覆盖 '+pct(s.actual?s.scoredActual/s.actual:null)],['>100%误差对象',int(s.black),'承载 '+int(s.blackActual)+' 件'],['WAPE',pct(s.wape),'分母 '+int(s.output)+' 条 / '+int(s.scoredActual)+' 件']];
 vals.forEach(([a,b,c])=>{const div=el('div',null,{class:'stat'});div.append(el('span',a),el('strong',b),el('small',c));metrics.append(div);});section.append(metrics);
 for(const year of [...new Set(selectedMonths().map(x=>x.slice(0,4)))]){section.append(el('h3',year+'年签发',{class:'year-title'}));const grid=el('div',null,{class:'month-grid'});selectedMonths().filter(x=>x.startsWith(year)).forEach(month=>grid.append(monthCard(model,month,rows.filter(r=>r.month===month))));section.append(grid);}
 return section;
}
function renderComparison(){
 const left=$('compareLeft').value,right=$('compareRight').value,month=$('compareMonth').value,rows=shown().filter(r=>r.month===month);
 $('comparison').replaceChildren(monthCard(left,month,rows,false,true),monthCard(right,month,rows,false,true));
}
function openMonth(month){
 $('dialogTitle').textContent=month+'签发 · '+D.windows[state.window]+' · 同色标同面积尺度';const c=$('dialogContent');c.replaceChildren();const grid=el('div',null,{class:'dialog-models'}),rows=shown().filter(r=>r.month===month);state.models.forEach(m=>grid.append(monthCard(m,month,rows,false,true)));c.append(grid);if(!$('monthDialog').open)$('monthDialog').showModal();
}
function updateExceptions(rows){
 const exceptions=[];for(const r of rows){if(!known(r)){exceptions.push([r.issue,r.shop,r.spu,r.truth==='NOT_MATURED'?'目标期未成熟':r.truth,'—','全部模型']);continue;}for(const m of state.models){if(r.values[m].state!=='OK'&&(visible(r)||r.values[m].state!=='NO_PREDICTION'))exceptions.push([r.issue,r.shop,r.spu,r.values[m].state,int(r.actual),labels[m]]);}}
 $('unavailableTable').replaceChildren(table(['签发日','店铺','SPU','状态','实销','模型'],exceptions.slice(0,unavailableLimit)));
 $('unavailable').querySelector('summary').textContent='不可评价、未成熟与异常记录：'+int(exceptions.length)+' 条（展开查看）';$('unavailableMore').hidden=exceptions.length<=unavailableLimit;return exceptions;
}
function render(){
 const started=performance.now();$('tooltip').hidden=true;if(observer)observer.disconnect();observer=new IntersectionObserver(entries=>entries.forEach(e=>{if(e.isIntersecting){e.target._fill();observer.unobserve(e.target);}}),{rootMargin:'250px'});
 document.documentElement.style.setProperty('--zoom',String(state.zoom));const all=filtered(),rows=all.filter(visible),low=all.filter(r=>known(r)&&!visible(r)),knownRows=all.filter(known),unmatured=all.filter(r=>r.truth==='NOT_MATURED');
 legend();$('scopeNote').textContent=(state.window==='d01_60'?'60天月均≥10件，即累计≥20件':'30天实销≥10件')+'｜当前 '+state.start+' 至 '+state.end+'签发｜筛选只隐藏、不放大或重排。';
 const audit=$('filterAudit');audit.replaceChildren();const total=knownRows.reduce((a,r)=>a+r.actual,0),keep=rows.reduce((a,r)=>a+r.actual,0);['筛选前 '+int(all.length)+' 条对象期','已知实销 '+int(knownRows.length)+' 条','隐藏低销量 '+int(low.length)+' 条 / '+int(low.reduce((a,r)=>a+r.actual,0))+' 件','保留 '+int(rows.length)+' 条 / '+pct(total?keep/total:null)+' 实销权重','未成熟 '+int(unmatured.length)+' 条'].forEach(x=>audit.append(el('span',x)));
 const common=rows.filter(r=>state.models.length>0&&state.models.every(m=>valid(r,m))),excluded=rows.filter(r=>!common.includes(r));
 $('commonNote').textContent='选中模型共同可评分 '+int(common.length)+' 条，实销分母 '+int(common.reduce((a,r)=>a+r.actual,0))+' 件；从完整展示排除 '+int(excluded.length)+' 条、'+int(excluded.reduce((a,r)=>a+r.actual,0))+' 件。不同分母WAPE不直接排名。';
 $('commonTable').replaceChildren(summaryTable(state.models,common));$('charts').replaceChildren(...state.models.map(m=>modelSection(m,rows)));if(!state.models.length)$('charts').append(el('p','请至少勾选一个模型。',{class:'error-state'}));
 updateExceptions(all);renderComparison();searchUpdate(false);if(fixedId!==null)openDetail(byId.get(fixedId));window.__lastRenderMs=performance.now()-started;
}
function searchUpdate(scroll){
 const hit=shown().filter(searchMatch);$('searchInfo').textContent=state.search?'搜索匹配 '+int(hit.length)+' 条，其他商品仍保留；点击方块可固定详情。':'';
 document.querySelectorAll('.tile').forEach(n=>{const r=byId.get(Number(n.dataset.rid)),match=searchMatch(r);n.classList.toggle('faded',!!state.search&&!match);n.classList.toggle('search-hit',!!state.search&&match);});
 if(scroll&&state.search&&hit.length){const card=document.querySelector('.model-section .month-card[data-month="'+hit[0].month+'"]');if(card){card._fill?.();card.scrollIntoView({behavior:'smooth',block:'center'});}}
}
function syncChecks(){document.querySelectorAll('#models input').forEach(n=>n.checked=state.models.includes(n.value));}
function csvCell(v){if(v===null||v===undefined)return '""';let s=String(v);if(typeof v==='string'&&/^[\s\u0000-\u001f]*[=+\-@]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';}
function download(text,type,name){const b=new Blob([text],{type}),url=URL.createObjectURL(b),a=el('a',null,{href:url,download:name});document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),5000);window.__lastDownload={name,type,size:b.size};}
function exportCSV(){
 const records=filtered().filter(r=>visible(r)||!known(r)),headers=['模型','店铺','SPU','签发月份','签发日','窗口','目标开始','目标结束','实际销量','预测销量','绝对误差件数','绝对误差率','有符号误差率','命中20%','方向','实销状态','预测状态','来源回退','当前描述性品类','原货龄'];
 const lines=[headers.map(csvCell).join(',')];for(const r of records)for(const m of state.models){const z=result(r,m);lines.push([m,r.shop,r.spu,r.month,r.issue,D.windows[r.window],r.start,r.end,r.actual,z.p,z.ae,z.ape,z.signed,z.ape===null?'不可评价':z.hit?'命中':'未命中',z.dir,r.truth,r.values[m].state,r.values[m].origin,r.category,r.maturity].map(csvCell).join(','));}
 download('\uFEFF'+lines.join('\r\n'),'text/csv;charset=utf-8','预测误差_当前筛选_'+state.window+'.csv');
}
function exportSVG(models,requestedMonths){
 const list=requestedMonths.filter(m=>m>=state.start&&m<=state.end),cols=models.length===2?2:3,panelW=520,panelH=390,top=205,panels=models.length===2?list.flatMap(month=>models.map(model=>({month,model}))):models.flatMap(model=>list.map(month=>({month,model}))),height=top+Math.ceil(panels.length/cols)*panelH+50;
 const s=svgEl('svg',{xmlns:NS,width:cols*panelW,height,viewBox:`0 0 ${cols*panelW} ${height}`});s.append(svgEl('rect',{width:'100%',height:'100%',fill:'#fff'}));
 s.append(svgEl('text',{x:20,y:30,'font-size':21,'font-family':'Microsoft YaHei, sans-serif'},'销量预测误差方块图｜'+models.map(m=>labels[m]).join(' / ')));
 const notes=[D.windows[state.window]+'｜按签发月 '+state.start+' — '+state.end+'｜'+(state.window==='d01_60'?'60天累计≥20件':'30天实销≥10件'),
  '面积=k×实销；所有面板同k，非逐月归一；历史重建回放，非独立前瞻。',
  '筛选：店铺 '+(state.shop||'全部')+'；当前品类 '+(state.category||'全部')+'；原货龄 '+(state.maturity||'全部')+'。↑高估 / ↓低估；灰斜纹=无预测。'];
 notes.forEach((t,i)=>s.append(svgEl('text',{x:20,y:55+i*22,'font-size':12,'font-family':'Microsoft YaHei, sans-serif'},t)));
 const defs=svgEl('defs'),lg=svgEl('linearGradient',{id:'export-gradient'});anchors.forEach((c,i)=>lg.append(svgEl('stop',{offset:(i*20)+'%','stop-color':c})));defs.append(lg);s.append(defs,svgEl('rect',{x:20,y:112,width:360,height:12,fill:'url(#export-gradient)'}));
 ['0%','20%目标','40%','60%','80%','100%'].forEach((t,i)=>s.append(svgEl('text',{x:20+i*72,y:141,'font-size':11},t)));s.append(svgEl('rect',{x:415,y:112,width:14,height:12,fill:'#000'}),svgEl('text',{x:435,y:123,'font-size':12},'>100%'));
 let ax=580;[100,1000,10000].forEach(a=>{const size=Math.sqrt(D.layouts[state.window].k*a);s.append(svgEl('rect',{x:ax,y:160-size,width:size,height:size,fill:'#9ba3ae'}),svgEl('text',{x:ax,y:178,'font-size':11},int(a)+'件'));ax+=Math.max(size+35,110);});
 const rows=shown();panels.forEach(({model,month},i)=>{const x=(i%cols)*panelW+20,y=top+Math.floor(i/cols)*panelH;const rr=D.rows.find(r=>r.month===month&&r.window===state.window),g=svgEl('g',{transform:`translate(${x},${y})`});g.append(svgEl('text',{x:0,y:15,'font-size':15},labels[model]+' · '+month+'签发'),svgEl('text',{x:0,y:34,'font-size':11},model+'｜'+rr.start+' — '+rr.end));if(rr.truth==='NOT_MATURED')g.append(svgEl('text',{x:100,y:150,'font-size':16},'目标期未成熟'));else{const chart=initSVG(model,month,rows.filter(r=>r.month===month),false);chart.setAttribute('x',0);chart.setAttribute('y',45);chart.setAttribute('width',D.width);chart.setAttribute('height',D.height);g.append(chart);}s.append(g);});
 const style=svgEl('style',{},'.tile{stroke:white;stroke-width:.3}.tile-label{font:600 10px Microsoft YaHei,sans-serif;fill:#18221e}.tile-label.light{fill:white}');s.insertBefore(style,s.firstChild);
 const text=new XMLSerializer().serializeToString(s);download('<?xml version="1.0" encoding="UTF-8"?>\n'+text,'image/svg+xml;charset=utf-8',models.length===2?'预测误差_并排比较.svg':'预测误差_'+models[0]+'.svg');
}
function init(){
 $('version').textContent='数据版本 '+D.version+' · 实际生成 '+D.generatedAt+' · 已核验 '+D.models.length+' 个模型';
 options('window',Object.entries(D.windows).map(([value,label])=>({value,label})));options('monthStart',months);options('monthEnd',months);$('monthEnd').value=state.end;
 for(const [id,key]of [['shop','shop'],['category','category'],['maturity','maturity']])options(id,[...new Set(D.rows.map(r=>r[key]))].sort().map(value=>({value,label:key==='maturity'?maturityLabel(value):value})),'全部');
 for(const [id,key]of [['window','window'],['monthStart','start'],['monthEnd','end'],['shop','shop'],['category','category'],['maturity','maturity']])bindSelect(id,key);
 D.models.forEach(m=>{const l=el('label'),i=el('input',null,{type:'checkbox',value:m.id});i.checked=state.models.includes(m.id);i.onchange=()=>{state.models=D.models.filter(x=>document.querySelector('#models input[value="'+x.id+'"]').checked).map(x=>x.id);render();};l.append(i,document.createTextNode(m.label));$('models').append(l);});
 ['compareLeft','compareRight'].forEach(id=>options(id,D.models.map(m=>({value:m.id,label:m.label}))));$('compareLeft').value='DIRECT30x2';$('compareRight').value=labels.DIRECT_YEARAGO_BLEND50?'DIRECT_YEARAGO_BLEND50':'DIRECT_YEARAGO_GATE_V1';options('compareMonth',months);$('compareMonth').value='2026-08';
 $('compareShow').onclick=renderComparison;$('compareExport').onclick=()=>exportSVG([$('compareLeft').value,$('compareRight').value],[$('compareMonth').value]);
 $('search').oninput=()=>{state.search=$('search').value;searchUpdate(false);};$('search').onkeydown=e=>{if(e.key==='Enter')searchUpdate(true);};$('zoom').onchange=()=>{state.zoom=Number($('zoom').value);document.documentElement.style.setProperty('--zoom',state.zoom);areaLegend($('areaLegend'));};
 $('reset').onclick=()=>{Object.assign(state,{window:'d01_30',start:months[0],end:months.at(-1),shop:'',category:'',maturity:'',search:'',zoom:1,models:defaults.filter(m=>labels[m])});['window','shop','category','maturity','search','zoom'].forEach(id=>$(id).value=state[id]);$('monthStart').value=state.start;$('monthEnd').value=state.end;syncChecks();$('detail').hidden=true;fixedId=null;render();};
 $('exportCSV').onclick=exportCSV;$('closeDetail').onclick=()=>{$('detail').hidden=true;fixedId=null;highlight(null);};$('closeDialog').onclick=()=>$('monthDialog').close();$('unavailableMore').onclick=()=>{unavailableLimit+=100;updateExceptions(filtered());};
 $('auditTable').append(table(['模型','窗口','完整核心','输出 / 缺预测','实销权重 / 评分分母','命中数 / 完整占比','WAPE'],D.unfiltered.map(x=>[labels[x.model],D.windows[x.window],int(x.n),int(x.output)+' / '+int(x.missing),int(x.actual)+' / '+int(x.scoredActual),int(x.hit)+' / '+pct(x.hitRate),pct(x.wape)])));
 D.models.forEach(m=>$('sources').append(el('div',m.label+'（'+m.id+'）｜成熟'+m.mature_periods+'期 / 含未成熟'+m.periods+'期｜'+m.status+'｜manifest '+m.manifest+(m.reference_commit?'｜参考提交 '+m.reference_commit:''),{class:'source-line'})));
 D.duplicateAudit.filter(x=>x.kind==='DUPLICATE_DIRECT').forEach(x=>$('sources').append(el('p','DIRECT副本核验 '+x.source+'：逐值相同 '+int(x.identical_n)+'，二进制差异 '+int(x.binary_difference_n)+'，最大 '+x.max_absolute_difference+'件。始终使用原长历史DIRECT，不取副本最后一条。',{class:'muted'})));
 render();document.body.dataset.ready='true';
}
// Local test hooks expose only already embedded reviewed rows; no network or model calls.
window.TILES={data:D,state,color,visible,known,valid,result,aggregate,filtered,shown,render,initSVG,csvCell,exportSVG,exportCSV,openMonth,openDetail};
init();
})();
