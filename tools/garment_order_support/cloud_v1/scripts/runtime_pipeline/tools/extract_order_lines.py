#!/usr/bin/env python3
"""Read garment order rows without modifying source files. See --help."""
from __future__ import annotations
import argparse, collections, hashlib, importlib.util, json, os, posixpath, re, sys, unicodedata
from pathlib import Path
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from zipfile import ZipFile, is_zipfile
import xml.etree.ElementTree as ET

VERSION="1.1.0-body-yyyymmdd"
BUNDLE_ROOT=Path(__file__).resolve().parent
HELPER_FILENAME="extract_fields.py"
_spec=importlib.util.spec_from_file_location("_readonly_old_helpers",BUNDLE_ROOT/HELPER_FILENAME)
H=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(H)
NS={"s":"http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
COLOR=re.compile(r"^(?:新|旧|贴纸|采购面料|调面料)?(?:颜色|色别)(?:名称)?$|^色名$")
SKU=re.compile(r"^(?:SKU(?:系列|编码)?|款号SKU|型号SKU)$",re.I)
SIZE=re.compile(r"^(?:XXXS|XXS|[2-3]XS|XS|S|M|L|XL|XXL|XXXL|[2-6]XL|[2-6]X|[2-6]XK|ONESIZE|均码|F)$",re.I)
TOTAL=re.compile(r"^(?:合计(?:数量)?|总计(?:数量)?|总数量|总件数|件数|数量|生产数量|下单数量|订单数量|采购数量|总数)$")
TOTAL_ROW=re.compile(r"^(?:合计|总计|小计|总数|总数量|总件数|订单合计|总金额)(?:[:：\s]|$)")
IMG=re.compile(r"(?:DISPIMG|IMAGE)\s*\(",re.I)
FABRIC=re.compile(r"面料|布料|布种|网纱|网布|里布|主布|配布|蕾丝|羊毛单面|天丝|超绒|仿棉|提花|烧花|针织|梭织|四面弹|TC袋布|印花",re.I)
NOTE=re.compile(r"备注|裁剪|剪裁|面料|布种|主布|里布|网纱|网布|配布|面层|里层|下节|印花|烧花|蕾丝",re.I)
def tx(v):return "" if v is None else unicodedata.normalize("NFKC",str(v)).strip()
def co(v):return re.sub(r"\s+","",tx(v))
def uq(v):return list(dict.fromkeys(v))
def number(v):
    if v is None or isinstance(v,bool):return None
    s=tx(v).replace(",","")
    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:\s*(?:件|套|条))?",s):return None
    try:
        n=Decimal(re.sub(r"(?:件|套|条)$","",s).strip())
        return int(n) if n==n.to_integral_value() else float(n)
    except InvalidOperation:return None
def hashfile(path):
    h=hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda:f.read(1048576),b""):h.update(b)
    return h.hexdigest()
def evidence(sheet,c,role):
    return {"sheet":sheet,"cell":c["coordinate"],"raw":c.get("value"),"role":role,**({"formula":c["formula"]} if c.get("formula") is not None else {})}
def merges(values):
    out=[]
    for x in values:
        if isinstance(x,str):
            a,_,b=x.partition(":");r1,c1=H.rc(a);r2,c2=H.rc(b or a)
        else:r1,r2,c1,c2=x;r1+=1;c1+=1
        out.append((r1,r2,c1,c2))
    return out
def read_snapshot(token,root):
    shs=H.read_extracted_cells(root/"extracts"/token)
    m=json.loads((root/"extracts"/token/"manifest.json").read_text(encoding="utf-8-sig"))
    ds={s["name"]:s for s in m.get("sheets",[])}
    for sh in shs:
        d=ds.get(sh["name"],{});sh["merges"]=merges(d.get("merged_ranges",[]))
        sh["state"]=d.get("state","visible")
    return shs
def read_workbook(path):
    shs=H.read_cells(path);mapping={s["name"]:s for s in shs}
    with Path(path).open("rb") as f:magic=f.read(8)
    if magic==bytes.fromhex("D0CF11E0A1B11AE1"):
        sys.path.insert(0,str(BUNDLE_ROOT/"deps"))
        import xlrd
        bk=xlrd.open_workbook(str(path),on_demand=True,formatting_info=True)
        try:
            for s in bk.sheets():mapping[s.name]["merges"]=merges(s.merged_cells)
        finally:bk.release_resources()
    elif is_zipfile(path):
        with ZipFile(path) as z:
            rs={}
            for x in ET.fromstring(z.read("xl/_rels/workbook.xml.rels")):
                t=x.get("Target","")
                if x.get("TargetMode")!="External":rs[x.get("Id")]=t.lstrip("/") if t.startswith("/") else posixpath.normpath("xl/"+t)
            for sh in ET.fromstring(z.read("xl/workbook.xml")).find("s:sheets",NS):
                p=rs.get(sh.get("{"+REL+"}id"))
                if not p or sh.get("name") not in mapping:continue
                ms=[]
                with z.open(p) as f:
                    for _,e in ET.iterparse(f,events=("end",)):
                        tag=e.tag.rsplit("}",1)[-1]
                        if tag=="mergeCell":ms.append(e.get("ref"))
                        if tag in ("c","row","mergeCell"):e.clear()
                mapping[sh.get("name")]["merges"]=merges(ms)
    for sh in shs:sh.setdefault("merges",[])
    return shs
def index(sh):
    rows=collections.defaultdict(list);cells={}
    for c in sh["cells"]:
        cells[(c["row"],c["column"])]=c;rows[c["row"]].append(c)
    for r in rows.values():r.sort(key=lambda c:c["column"])
    return rows,cells
def cell_at(sh,cells,row,col):
    c=cells.get((row,col))
    if c and tx(c.get("value")):
        for r1,r2,c1,c2 in sh.get("merges",[]):
            if r1<=row<=r2 and c1<=col<=c2:return c,(r1,r2,c1,c2)
        return c,None
    for r1,r2,c1,c2 in sh.get("merges",[]):
        if r1<=row<=r2 and c1<=col<=c2:
            c=cells.get((r1,c1))
            if c and tx(c.get("value")):return c,(r1,r2,c1,c2)
    return None,None
def labeled(sh,pat,maxrow=35):
    rows,cells=index(sh);out=[]
    for c in sh["cells"]:
        s=tx(c.get("value"))
        if c["row"]>maxrow or len(s)>300 or c.get("formula"):continue
        m=pat.match(s)
        if not m:continue
        tail=s[m.end():].strip(" :：")
        if tail:out.append((tail,c,"inline_label"));continue
        below=cells.get((c["row"]+1,c["column"]))
        right=[v for v in rows[c["row"]] if c["column"]<v["column"]<=c["column"]+5][:1]
        # Header grids have the field value directly below; inline labels often use right.
        for v in ([below] if below else [])+right:
            t=tx(v["value"])
            if t and len(t)<160 and not re.search(r"[:：]|供应商|时间|日期|跟单|款号|单号|主商标|采购金额|图片|款式名称|面料布种号",t):
                out.append((t,v,"adjacent_label"));break
    return out
def sheet_meta(sh):
    o={}
    o["fabric"]=labeled(sh,re.compile(r"^面料布种号\s*[:：]?|^面料\s*[:：]?$"))
    o["supplier"]=labeled(sh,re.compile(r"^面料供应商\s*[:：]?"))
    o["orders"]=[(v,c,k) for raw,c,k in labeled(sh,H.ORDER_LABEL,100) for v in H._extract_order_value(raw)]
    o["styles"]=[(v,c,k) for raw,c,k in labeled(sh,re.compile(r"^(?:公司)?(?:款号|型号|货号|款式号)\s*[:：]?")) for v in H._styles(raw)]
    o["dates"]=labeled(sh,re.compile(r"^下单(?:时间|日期)\s*[:：]?"))
    return o
def size_name(v):
    s=co(v).upper()
    if not SIZE.fullmatch(s):return None
    return {"2XS":"XXS","3XS":"XXXS","XXL":"2XL","XXXL":"3XL","2X":"2XL","3X":"3XL","4X":"4XL"}.get(s,s)
def blocks(sh):
    rows,cells=index(sh);out=[]
    candidates=sorted({c["row"] for c in sh["cells"] if COLOR.fullmatch(co(c["value"])) or SKU.fullmatch(co(c["value"]))})
    for rr in candidates:
        hs=rows[rr];colors=[c for c in hs if COLOR.fullmatch(co(c["value"]))];skus=[c for c in hs if SKU.fullmatch(co(c["value"]))]
        color=sorted(colors,key=lambda c:(0 if co(c["value"]).startswith("新") else 1 if co(c["value"]) in ("颜色","颜色名称","色别","色名") else 2,c["column"]))
        ss0=[c for c in hs if size_name(c["value"])];ss1=[c for c in rows.get(rr+1,[]) if size_name(c["value"])]
        ss=ss0 if len(ss0)>=2 else ss1 if len(ss1)>=2 else []
        end=rr+1 if ss and ss==ss1 else rr
        if not ss and any(co(c["value"]) in ("单价","金额","比例","条数") for c in rows.get(rr+1,[])):end=rr+1
        hc=[c for r in range(rr,end+1) for c in rows.get(r,[])]
        qs=sorted([c for c in hc if TOTAL.fullmatch(co(c["value"]))],key=lambda c:(0 if re.search("合计|总计|总数量|件数",co(c["value"])) else 1,-c["row"],c["column"]))
        ctx=bool(re.search("下单|订单",sh["name"])) or any(re.search(r"下单(?:表|明细|时间|日期)|供应链下单",tx(c["value"])) for c in sh["cells"] if c["row"]<rr)
        out.append({"header":rr,"header_end":end,"colors":colors,"color":color[0] if color else None,"sku":skus[0] if skus else None,"sizes":ss,"quantity":qs[0] if qs else None,"quantity_headers":qs,"notes":[c for c in hc if NOTE.search(tx(c["value"])) and c not in colors],"headers":hc,"order_context":ctx})
    for i,b in enumerate(out):b["end"]=out[i+1]["header"]-1 if i+1<len(out) else max(rows,default=0)
    return out
def inline_sizes(v,ref):
    s=tx(v).upper()
    if "=" in s or "比例" in s or re.search(r"\d+\s*[:：]\s*\d+",s):return [],bool(re.search("XS|S|M|L|XL",s))
    ms=re.finditer(r"(?<![A-Z0-9])([2-6]XL|XXXL|XXL|XXS|XS|XL|S|M|L)\s*[/：:]?\s*(\d+(?:\.\d+)?)",s)
    return [{"size":size_name(m[1]),"quantity":number(m[2]),"raw":m[0],"cell":ref,"source":"inline_explicit_quantity"} for m in ms],False
def body_dates(values):
    out=[]
    for raw,c,k in values:
        v=tx(raw)
        try:
            if re.fullmatch(r"\d{5}(?:\.0+)?",v):d=(datetime(1899,12,30)+timedelta(days=float(v))).date().isoformat()
            elif re.fullmatch(r"20\d{6}",v):
                # Values reach this function only from explicitly labelled
                # order-date cells. Never parse PO numbers or arbitrary text.
                d=datetime.strptime(v,"%Y%m%d").date().isoformat()
            else:
                m=re.search(r"(20\d{2})[-./年](\d{1,2})[-./月](\d{1,2})",v)
                d=datetime(int(m[1]),int(m[2]),int(m[3])).date().isoformat() if m else None
        except ValueError:d=None
        out.append({"value":d,"raw":raw,"cell":c["coordinate"],"source":k})
    return out


def extract_file(path,metadata,*,cells=None,source_sha256=None,development=False):
    token=metadata.get("token") or metadata.get("source_token")
    name=metadata.get("name") or metadata.get("file_name") or Path(path).name
    url=metadata.get("source_url") or metadata.get("url") or "https://yxqje9jjxtk.feishu.cn/file/"+str(token)
    parent=metadata.get("parent_path") or metadata.get("folder_path") or metadata.get("path","").rsplit("/",1)[0]
    factory=H.extract_factory({"parent_path":parent})["value"] or metadata.get("factory")
    market=next(iter(p for p in parent.split("/") if p),"")
    date=H.parse_filename_date(name)
    filename_date=date.get("value") if date.get("status")=="parsed" else None
    # Corrected/ambiguous filename dates must not be used to choose price versions.
    eligible_filename_date=filename_date if date.get("status")=="parsed" else None
    audit={"token":token,"file_name":name,"source_url":url,"local_path":str(path),"source_sha256":source_sha256,"factory":factory,"market":market,"status":"ok","extraction_version":VERSION,"development_snapshot":development,"filename_date":filename_date,"date_parse":date,"issues":[],"sheet_audits":[],"line_count":0}
    if re.search("返单|补单|追单|加单",name):audit["issues"].append("repeat_or_additional_order_filename")
    if re.search("改单|改数|修改|更新|取消|作废|最终|优化",name):audit["issues"].append("revision_or_cancellation_filename_review")
    if date.get("status")!="parsed":audit["issues"].append("filename_date_not_unambiguously_standard")
    try:shs=cells if cells is not None else read_workbook(path)
    except Exception as exc:
        audit.update(status="read_error",error=f"{type(exc).__name__}: {exc}")
        return {"lines":[],"audit":audit}
    if not source_sha256 and not development:audit["source_sha256"]=hashfile(path)
    metas={s["name"]:sheet_meta(s) for s in shs}
    fileorders=uq(v for sm in metas.values() for v,c,k in sm["orders"])
    filestyles=uq(v for sm in metas.values() for v,c,k in sm["styles"])
    all_lines=[]
    for si,sh in enumerate(shs,1):
        rows,cmap=index(sh);bs=blocks(sh);sm=metas[sh["name"]]
        sa={"sheet":sh["name"],"sheet_index":si,"state":sh.get("state","visible"),"nonempty_cells":len(sh["cells"]),"block_audits":[],"line_count":0,"issues":[]}
        ordervals=uq(v for v,c,k in sm["orders"]);inherited=False
        if not ordervals and len(fileorders)==1:ordervals=fileorders[:];inherited=True
        styles=uq(v for v,c,k in sm["styles"]) or H._styles(sh["name"]) or filestyles or H._styles(name)
        fabric=" | ".join(uq(v for v,c,k in sm["fabric"]))
        supplier=" | ".join(uq(v for v,c,k in sm["supplier"]))
        he=[evidence(sh["name"],c,role) for role,key in (("fabric_header","fabric"),("supplier","supplier"),("order_id","orders")) for v,c,k in sm[key]]
        bdates=body_dates(sm["dates"]);ds=uq(x["value"] for x in bdates if x["value"])
        date_conflict=bool(filename_date and ds and any(d!=filename_date for d in ds))
        orderdate=eligible_filename_date if not date_conflict else None
        if date_conflict:sa["issues"].append("filename_body_date_conflict")
        for b in bs:
            ba={"header_row":b["header"],"header_end":b["header_end"],"end_row":b["end"],"headers":[evidence(sh["name"],c,"header") for c in b["headers"]],"template":"size_columns" if b["sizes"] else "color_quantity" if b["quantity"] else "color_without_quantity","status":"parsed","line_count":0,"excluded_rows":[],"total_checks":[],"issues":[]}
            if len(b["quantity_headers"])>1:
                qs=b["quantity_headers"]
                def qscore(h):
                    vals=[cmap.get((r,h["column"]),{}).get("value") for r in range(b["header_end"]+1,b["end"]+1)]
                    return (sum(number(v) is not None for v in vals)-sum("条布" in tx(v) for v in vals),-qs.index(h))
                chosen=max(qs,key=qscore)
                if chosen!=b["quantity"]:ba["issues"].append("quantity_column_selected_using_explicit_numeric_values");b["quantity"]=chosen
            qheaders=b["sizes"]+([b["quantity"]] if b["quantity"] else [])
            actual=any(number(cmap.get((r,h["column"]),{}).get("value")) is not None for r in range(b["header_end"]+1,b["end"]+1) for h in qheaders)
            if not b["order_context"] and not actual:
                ba["status"]="reference_color_table";ba["issues"].append("color_catalogue_without_order_quantity")
                sa["block_audits"].append(ba);continue
            if not qheaders:ba["issues"].append("quantity_header_unrecognized")
            lines=[];totals=[]
            for rr in range(b["header_end"]+1,b["end"]+1):
                rcs=rows.get(rr,[])
                if not rcs:continue
                cc,cm=cell_at(sh,cmap,rr,b["color"]["column"]) if b["color"] else (None,None)
                sc,ssm=cell_at(sh,cmap,rr,b["sku"]["column"]) if b["sku"] else (None,None)
                color=tx(cc.get("value")) if cc else "";sku=tx(sc.get("value")) if sc else ""
                garment_color_raw=color or None;color_source_role="garment_color"
                if not color:
                    for fh in b["notes"]:
                        if re.search(r"主布色号|布种色号|面料.*色号",tx(fh["value"])):
                            fc,fm=cell_at(sh,cmap,rr,fh["column"])
                            if fc and re.search(r"[\u4e00-\u9fff]",tx(fc["value"])) and not IMG.search(tx(fc["value"])) and not re.search(r"入库|剩下|数量|按此|订单|PO\d",tx(fc["value"]),re.I):
                                color=tx(fc["value"]);cc=fc;cm=fm;color_source_role="fabric_color_column";break
                skuok=bool(re.fullmatch(r"(?:TK[-_])?[A-Z]{1,8}\d{2,5}[-_][A-Z0-9_-]+",sku,re.I))
                qc=cmap.get((rr,b["quantity"]["column"])) if b["quantity"] else None
                sq=[]
                for h in b["sizes"]:
                    c=cmap.get((rr,h["column"]))
                    sq.append({"size":size_name(h["value"]),"quantity":number(c.get("value")) if c else None,"raw":c.get("value") if c else None,"cell":H.coord((rr,h["column"])),"source":"size_column","header_raw":h["value"],"header_cell":h["coordinate"],"formula":c.get("formula") if c else None})
                tl=[c for c in rcs if TOTAL_ROW.match(co(c["value"]))]
                if tl and not skuok:
                    totals.append({"row":rr,"label_cells":[evidence(sh["name"],c,"total_label") for c in tl],"quantity":number(qc.get("value")) if qc else None,"quantity_cell":qc["coordinate"] if qc else None,"size_quantities":sq,"evidence_cells":[evidence(sh["name"],c,"total_row") for c in rcs]})
                    ba["excluded_rows"].append({"row":rr,"reason":"total_or_subtotal_row"});continue
                ins=[];ratio=False
                for h in b["notes"]:
                    c,_=cell_at(sh,cmap,rr,h["column"])
                    if c:
                        z,rat=inline_sizes(c["value"],c["coordinate"]);ins+=z;ratio|=rat
                if not b["sizes"]:sq=ins
                q=number(qc.get("value")) if qc else None
                # Visual continuation rows have inherited identity but no new quantity or direct identity.
                if (cm or ssm) and not any(cmap.get((rr,h["column"])) for h in qheaders) and not (b["sku"] and cmap.get((rr,b["sku"]["column"]))) and not (b["color"] and cmap.get((rr,b["color"]["column"]))):
                    ba["excluded_rows"].append({"row":rr,"reason":"merged_visual_continuation_not_order_line"});continue
                # Ratios/accessory-size legends are not garment orders.
                if not skuok and (any(co(c["value"]) in ("比例","尺码比例","裁剪比例","棉杯","胸垫","杯垫对码","色号") for c in rcs) or sum(bool(size_name(c["value"])) for c in rcs)>=2):
                    ba["excluded_rows"].append({"row":rr,"reason":"ratio_or_accessory_size_legend"});continue
                prevsum=sum(x["quantity"] for x in lines if x["quantity"] is not None and x.get("line_type")=="order_color")
                unlabeled_total=(not sku and not color and bool(lines) and ((qc and re.search(r"SUM|SUBTOTAL",str(qc.get("formula") or ""),re.I) and any(int(x)<rr for x in re.findall(r"[A-Z]+(\d+)",str(qc.get("formula") or "")))) or (q is not None and q==prevsum and len(lines)>=1)))
                if unlabeled_total:
                    totals.append({"row":rr,"label_cells":[],"quantity":q,"quantity_cell":qc["coordinate"] if qc else None,"size_quantities":sq,"evidence_cells":[evidence(sh["name"],c,"unlabeled_total_row") for c in rcs],"recognition":"SUM_formula_or_equal_preceding_rows_without_identity"})
                    ba["excluded_rows"].append({"row":rr,"reason":"unlabeled_total_with_formula_or_sum_evidence"});continue
                sn=[x["quantity"] for x in sq if x["quantity"] is not None]
                ssize=sum(sn) if sn else None
                qpresent=q is not None or ssize is not None
                validcolor=bool(color and len(color)<=100 and not IMG.search(color) and not re.search("注意事项|公司审批|签字|支付|合计|总计|款式名称|SKU|尺码|生产工艺|面料供应商",color,re.I))
                directcolor=bool(b["color"] and cmap.get((rr,b["color"]["column"])))
                if not skuok and not(validcolor and qpresent) and not(validcolor and b["order_context"] and directcolor):
                    if qpresent and any(n!=0 for n in sn+([q] if q is not None else [])) and b["order_context"] and not any(len(tx(c["value"]))>120 for c in rcs):
                        validcolor=False
                    else:
                        if qpresent:ba["excluded_rows"].append({"row":rr,"reason":"blank_template_or_non_order_numeric_row"})
                        continue
                stock_release=bool(qc and re.search("库存|余货|旧货",tx(qc.get("value"))) and number(qc.get("value")) is None)
                allocation=not skuok and not garment_color_raw and any(re.search(r"入库|剩下的数量|按此数|数量入",tx(c["value"])) for c in rcs)
                issues=[]
                if allocation:issues.append("allocation_breakdown_not_additional_order")
                if stock_release:issues.append("existing_stock_release_not_new_order")
                if color_source_role=="fabric_color_column":issues.append("garment_color_missing_using_fabric_color_text")
                if not validcolor:issues.append("color_missing_or_unrecognized")
                if not skuok and not validcolor:issues.append("row_identity_unresolved")
                if cm:issues.append("color_from_confirmed_merged_cell")
                if ssm:issues.append("sku_from_confirmed_merged_cell")
                if ratio and not b["sizes"]:issues.append("size_ratio_retained_not_expanded")
                if len({x["size"] for x in sq})<len(sq):issues.append("duplicate_size_columns")
                if any(re.fullmatch(r"[2-6]XK",x["size"] or "") for x in sq):issues.append("size_label_unrecognized_preserved")
                if any(x["raw"] not in (None,"") and x["quantity"] is None for x in sq):issues.append("unparsed_size_quantity")
                quantity=q if q is not None else ssize
                qsource="row_total" if q is not None else "size_sum" if ssize is not None else "missing"
                if quantity is None:issues.append("quantity_missing")
                if qc and tx(qc.get("value")) and q is None:issues.append("unparsed_row_quantity")
                if q is not None and ssize is not None and abs(q-ssize)>1e-8:issues.append("row_total_size_sum_mismatch")
                if quantity is not None and (quantity<0 or int(quantity)!=quantity):issues.append("negative_or_fractional_quantity")
                if qsource=="size_sum":issues.append("quantity_derived_from_explicit_sizes")
                if b["sizes"] and any(x["raw"] in (None,"") for x in sq):issues.append("blank_size_cells_preserved")
                if any(IMG.search(tx(c.get("value"))) for c in rcs):issues.append("row_contains_image_reference")
                assignments=[]
                for h in b["notes"]:
                    c,m=cell_at(sh,cmap,rr,h["column"])
                    if not c or c["row"]<=b["header_end"] or not tx(c["value"]):continue
                    label=tx(h["value"]);value=tx(c["value"])
                    explicit=bool(re.search(r"面料(?:旧名称|新名称)?$|布种|网纱|网布|里布|主布|配布|面层|里层|下节|印花|烧花|蕾丝",label))
                    if explicit or FABRIC.search(value) or re.search("备注|裁剪|剪裁",label):
                        assignments.append({"text":value,"sheet":sh["name"],"cell":c["coordinate"],"header":label,"source_kind":"fabric_column" if explicit else "row_note","scope":"merged_range" if m else "row","confirmed":explicit,"applies_to_rows":list(range(m[0],m[1]+1)) if m else [rr]})
                        if m and not explicit:issues.append("merged_note_fabric_applicability_requires_review")
                rs=H._styles(sku) if skuok else [];spus=rs or styles
                spu=spus[0] if len(spus)==1 else None
                if len(spus)>1:issues.append("spu_multiple_candidates")
                if rs and styles and not set(rs).issubset(set(styles)):issues.append("sku_header_spu_disagreement")
                if not spu:issues.append("spu_missing_or_ambiguous")
                if len(ordervals)>1:issues.append("order_id_multiple_candidates")
                if not ordervals:issues.append("order_id_missing")
                if ordervals==["新款"]:issues.append("new_style_status_not_global_order_key")
                if inherited:issues.append("order_id_from_other_sheet_unique")
                if sh.get("state")!="visible":issues.append("hidden_sheet_order_rows")
                if any(re.search("取消订单|取消下单|此色取消|作废",tx(c["value"])) for c in rcs):issues.append("row_order_cancelled")
                if date_conflict:issues.append("filename_body_date_conflict")
                if not orderdate:issues.append("order_date_not_valid_for_price_version")
                colorcode=None
                if skuok:
                    colorcode=re.split("[-_]",sku,maxsplit=1)[1]
                    if sku.upper().startswith("TK-"):colorcode=sku.split("-",2)[-1]
                cname=re.sub(r"^\s*\d+(?:\.\d+)?\s*#\s*|\s*#\s*\d+\s*$","",color).strip() or None
                reliable=quantity is not None and not any(x in issues for x in ("unparsed_size_quantity","unparsed_row_quantity","row_total_size_sum_mismatch","negative_or_fractional_quantity","row_identity_unresolved"))
                line={"line_id":f"{token}:{si}:{b['header']}:{rr}","line_type":"allocation_breakdown" if allocation else "stock_release" if stock_release else "order_color","token":token,"factory":factory,"market":market,"spu":spu,"spu_candidates":spus,"sku_raw":sku or None,"order_id":ordervals[0] if len(ordervals)==1 else None,"order_id_candidates":ordervals,"order_date":orderdate,"filename_date":filename_date,"filename_date_raw":date.get("raw"),"filename_date_status":date.get("status"),"body_order_dates":bdates,"order_date_conflict":date_conflict,"order_date_source":"filename_standard_no_body_conflict" if orderdate else "unresolved","file_name":name,"source_url":url,"local_path":str(path),"source_sha256":audit["source_sha256"],"sheet":sh["name"],"sheet_index":si,"table_header_row":b["header"],"row":rr,"color_raw":color or None,"garment_color_raw":garment_color_raw,"color_source_role":color_source_role,"color_code":colorcode,"color_name":cname,"color_source_cell":cc["coordinate"] if cc else None,"color_alternatives":[{"header":c["value"],"cell":H.coord((rr,c["column"])),"raw":cmap.get((rr,c["column"]),{}).get("value")} for c in b["colors"]],"quantity":quantity,"quantity_source":qsource,"quantity_reliable":reliable,"include_in_summary":reliable and not allocation and not stock_release and "row_order_cancelled" not in issues,"row_total_raw":qc.get("value") if qc else None,"size_sum":ssize,"size_quantities":sq,"fabric_header":fabric,"fabric_row_note":" | ".join(uq(x["text"] for x in assignments)),"supplier":supplier,"fabric_assignment_sources":assignments,"evidence_cells":he+[evidence(sh["name"],c,"row") for c in rcs],"issues":uq(issues),"duplicate_source_of":None,"duplicate_line_of":None,"development_snapshot":development}
                lines.append(line)
            for t in totals:
                before=[l for l in lines if l["row"]<t["row"] and l.get("line_type")=="order_color"]
                extracted=sum(l["quantity"] for l in before if l["quantity"] is not None)
                expected=t["quantity"]
                if expected is None:
                    n=[x["quantity"] for x in t["size_quantities"] if x["quantity"] is not None]
                    expected=sum(n) if n else None
                t.update(extracted_rows_before_total=len(before),extracted_quantity_before_total=extracted,comparison_quantity=expected,status="not_comparable" if expected is None or any(l["quantity"] is None for l in before) else "match" if abs(expected-extracted)<1e-8 else "mismatch",basis="all_emitted_rows_in_block_before_total")
                ba["total_checks"].append(t)
                if t["status"]=="mismatch":ba["issues"].append("order_total_mismatch")
            if not totals:ba["issues"].append("no_explicit_order_total")
            ba.update(line_count=len(lines),quantity_sum=sum(l["quantity"] for l in lines if l["quantity"] is not None),reliable_quantity_sum=sum(l["quantity"] for l in lines if l["quantity_reliable"]))
            if not lines:ba["status"]="no_rows_extracted";ba["issues"].append("order_table_no_rows_requires_review")
            for l in lines:l["order_total_status"]="mismatch" if any(t["status"]=="mismatch" for t in totals) else "match" if any(t["status"]=="match" for t in totals) else "not_comparable"
            all_lines+=lines;sa["line_count"]+=len(lines);sa["block_audits"].append(ba)
        if not bs and any(re.search(r"下单(?:表|明细|时间|日期)",tx(c["value"])) for c in sh["cells"]):
            sa["issues"].append("order_lead_time_reference_sheet" if any("生产周期" in tx(c["value"]) for c in sh["cells"]) else "order_context_without_recognized_color_header")
        sa["status"]="parsed" if sa["line_count"] else "reference_or_unresolved" if bs else "no_order_table_identified"
        audit["sheet_audits"].append(sa)
    if sum(bool(s["line_count"]) for s in audit["sheet_audits"])>1:audit["issues"].append("multiple_order_sheets_kept_separate")
    if len(fileorders)>1:audit["issues"].append("multiple_order_ids_in_file")
    if not all_lines:audit["status"]="no_order_lines_review";audit["issues"].append("no_order_lines_extracted")
    elif any(not l["quantity_reliable"] for l in all_lines):audit["status"]="parsed_with_review"
    for l in all_lines:l["file_issues"]=audit["issues"][:]
    audit.update(line_count=len(all_lines),quantity_sum=sum(l["quantity"] for l in all_lines if l["quantity"] is not None),reliable_quantity_sum=sum(l["quantity"] for l in all_lines if l["quantity_reliable"]),quantity_missing_rows=sum(l["quantity"] is None for l in all_lines),quantity_unreliable_rows=sum(not l["quantity_reliable"] for l in all_lines),color_missing_rows=sum(not l["color_raw"] for l in all_lines),order_ids=fileorders,spus=uq(l["spu"] for l in all_lines if l["spu"]))
    return {"lines":all_lines,"audit":audit}


def load_items(path):
    x=json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return x if isinstance(x,list) else x.get("items",x.get("files",[]))


def refine_material_assignments(line):
    """Confirmed cell geometry is separate from conditional material instructions."""
    line["issues"]=[x for x in line["issues"] if x!="merged_note_fabric_applicability_requires_review"]
    specific=re.compile(r"(?:0\d{2}(?:优化|面料)?|(?<!\d)\d{3,4}(?!\d)|[A-Z]{1,4}\d{3,6}|羊毛单面|天丝羊绒|仿棉|锦单面|涤双磨|莫代尔|椰感|四面弹|超绒|蕾丝|网纱|网布|坑条)",re.I)
    conditional=re.compile(r"其他|其它|分别|如.{0,20}没|[白黑红蓝绿粉杏棕灰紫黄].{0,12}(?:用|调货)|待(?:定|确认|确定)|待周")
    for a in line.get("fabric_assignment_sources",[]):
        value=tx(a["text"]);header=tx(a.get("header"))
        a["scope_confirmed"]=True
        if IMG.search(value):
            a.update(confirmed=False,material_relevance="image_reference",assignment_status="image_content_not_read")
            continue
        explicit=a.get("source_kind")=="fabric_column"
        material=bool(specific.search(value) and (FABRIC.search(value) or re.search(r"0\d{2}|[A-Z]{1,4}\d{3,6}",value)))
        # Pure swatch identifiers are color evidence and cannot prove fabric category.
        if explicit and re.search("色号",header) and not material:
            a.update(confirmed=True,material_relevance="fabric_color_only",assignment_status="color_code_not_material_id")
            continue
        if not explicit and not material:
            a.update(confirmed=False,material_relevance="general_instruction",assignment_status="not_specific_material_assignment")
            continue
        condition=bool(conditional.search(value) and len(a.get("applies_to_rows",[]))>1)
        a.update(confirmed=not condition,material_relevance="specific_material_instruction",assignment_status="conditional_material_instruction" if condition else "confirmed_row_or_merge_scope")
        if condition:line["issues"].append("conditional_merged_material_instruction_review")
    line["issues"]=uq(line["issues"])


def finalize_outputs(line_path,audit_path):
    """Apply symmetric document-risk flags after every current file has been read."""
    lines=[json.loads(x) for x in line_path.read_text(encoding="utf-8").splitlines()]
    audits=[json.loads(x) for x in audit_path.read_text(encoding="utf-8").splitlines()]
    bytoken=collections.defaultdict(list)
    geometry={}
    for line in lines:
        for a in line.get("fabric_assignment_sources",[]):
            if a.get("scope")=="merged_range":geometry[(line["token"],a["sheet"],a["cell"])]=a.get("applies_to_rows",[])
    for line in lines:
        for a in line.get("fabric_assignment_sources",[]):
            k=(line["token"],a["sheet"],a["cell"])
            if k in geometry:a["scope"]="merged_range";a["applies_to_rows"]=geometry[k]
    for line in lines:
        line["issues"]=[i for i in line["issues"] if i!="same_bytes_other_document_identity_requires_review"]
        refine_material_assignments(line)
        bytoken[line["token"]].append(line)
    bysha=collections.defaultdict(list)
    for a in audits:
        if a.get("source_sha256"):bysha[a["source_sha256"]].append(a)
    for sha,group in bysha.items():
        if len(group)<2:continue
        scopes=collections.defaultdict(list)
        for a in group:scopes[(a.get("factory"),a.get("market"),a.get("file_name"))].append(a)
        risk=[]
        if len({a.get("factory") for a in group})>1:risk.append("same_bytes_across_factories_review")
        if len({a.get("market") for a in group})>1:risk.append("same_bytes_across_markets_review")
        if len({a.get("file_name") for a in group})>1:risk.append("same_bytes_different_filename_or_date_review")
        for a in group:
            a["same_bytes_other_tokens"]=[v["token"] for v in group if v["token"]!=a["token"]]
            a.setdefault("issues",[]).extend(risk);a["issues"]=uq(a["issues"])
            for l in bytoken[a["token"]]:
                l["same_bytes_other_tokens"]=a["same_bytes_other_tokens"]
                l["issues"]=uq(l["issues"]+risk)
        for subset in scopes.values():
            for a in subset[1:]:
                original=subset[0]["token"];a["duplicate_source_of"]=original
                for l in bytoken[a["token"]]:
                    l["duplicate_source_of"]=original
                    l["duplicate_line_of"]=l["line_id"].replace(a["token"]+":",original+":",1)
                    l["include_in_summary"]=False
                    l["issues"]=uq(l["issues"]+["exact_duplicate_same_factory_market_name_bytes"])
    versions=collections.defaultdict(list)
    for l in lines:
        if l.get("order_id") and l["order_id"]!="新款" and re.search(r"\d",l["order_id"]):
            versions[(l.get("factory"),l.get("market"),l.get("spu"),l["order_id"])].append(l)
    for key,group in versions.items():
        hashes={l["source_sha256"] for l in group}
        if len(hashes)>1:
            toks=uq(l["token"] for l in group)
            for l in group:
                l["same_order_different_source_tokens"]=toks
                l["issues"]=uq(l["issues"]+["same_order_spu_different_source_review"])
    for a in audits:
        a["issues"]=[i for i in a.get("issues",[]) if i!="same_bytes_other_document_identity_requires_review"]
        ls=bytoken[a["token"]]
        if not ls:continue
        a["line_count"]=len(ls)
        a["quantity_sum"]=sum(x["quantity"] for x in ls if x["quantity"] is not None)
        a["reliable_quantity_sum"]=sum(x["quantity"] for x in ls if x["quantity_reliable"])
        a["summary_quantity_sum"]=sum(x["quantity"] for x in ls if x["include_in_summary"] and x["quantity"] is not None)
        a["summary_line_count"]=sum(x["include_in_summary"] for x in ls)
        a["quantity_unreliable_rows"]=sum(not x["quantity_reliable"] for x in ls)
        a["allocation_breakdown_rows"]=sum(x.get("line_type")=="allocation_breakdown" for x in ls)
        a["date_conflict_rows"]=sum(x.get("order_date_conflict",False) for x in ls)
        a["row_issue_counts"]=dict(collections.Counter(v for x in ls for v in x["issues"]))
        if a["allocation_breakdown_rows"]:a.setdefault("issues",[]).append("allocation_breakdowns_preserved_not_double_counted")
        if "same_order_spu_different_source_review" in a["row_issue_counts"]:a.setdefault("issues",[]).append("same_order_spu_different_source_review")
        if any(t["status"]=="mismatch" for s in a.get("sheet_audits",[]) for b in s["block_audits"] for t in b["total_checks"]):a.setdefault("issues",[]).append("order_total_mismatch")
        a["issues"]=uq(a.get("issues",[]))
        if a["quantity_unreliable_rows"] or a["allocation_breakdown_rows"] or "order_total_mismatch" in a["issues"]:a["status"]="parsed_with_review"
        for line in ls:line["file_issues"]=a["issues"][:]
    assert len({l["line_id"] for l in lines})==len(lines),"Duplicate line_id"
    assert len({a["token"] for a in audits})==len(audits),"Duplicate file audit"
    line_path.write_text("".join(json.dumps(x,ensure_ascii=False)+"\n" for x in lines),encoding="utf-8")
    audit_path.write_text("".join(json.dumps(x,ensure_ascii=False)+"\n" for x in audits),encoding="utf-8")


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--inventory",required=True);p.add_argument("--downloads",action="append",default=[])
    p.add_argument("--output-lines",required=True);p.add_argument("--output-audit",required=True)
    p.add_argument("--snapshot-dir");p.add_argument("--limit",type=int);p.add_argument("--tokens",nargs="*")
    args=p.parse_args()
    items=[x for x in load_items(args.inventory) if x.get("type")!="folder"]
    latest={};success={}
    for filename in args.downloads:
        with Path(filename).open(encoding="utf-8-sig") as f:
            for raw in f:
                try:r=json.loads(raw)
                except json.JSONDecodeError:continue
                token=r.get("token") or r.get("source_token");latest[token]=r
                if r.get("status") in ("ok","success","downloaded") and r.get("local_path"):success[token]=r
    if args.tokens:items=[x for x in items if (x.get("token") or x.get("source_token")) in args.tokens]
    if args.limit:items=items[:args.limit]
    op=Path(args.output_lines);ap=Path(args.output_audit)
    op.parent.mkdir(parents=True,exist_ok=True);ap.parent.mkdir(parents=True,exist_ok=True)
    ot=op.with_suffix(op.suffix+".tmp");at=ap.with_suffix(ap.suffix+".tmp")
    seen={};seen_sha=collections.defaultdict(list);stats=collections.Counter()
    with ot.open("w",encoding="utf-8") as lo,at.open("w",encoding="utf-8") as au:
        for i,item in enumerate(items,1):
            token=item.get("token") or item.get("source_token");md={**item,**success.get(token,{})}
            path=md.get("local_path","");snapshot=Path(args.snapshot_dir) if args.snapshot_dir else None
            if str(md.get("name",md.get("file_name",""))).lower().endswith(".prj"):
                result={"lines":[],"audit":{"token":token,"file_name":md.get("name") or md.get("file_name"),"status":"not_a_spreadsheet","line_count":0,"issues":["PRJ_not_order_workbook"]}}
            elif snapshot:
                try:result=extract_file(path or "snapshot/"+str(token),md,cells=read_snapshot(token,snapshot),development=True)
                except Exception as exc:result={"lines":[],"audit":{"token":token,"status":"snapshot_read_error","error":f"{type(exc).__name__}: {exc}","line_count":0}}
            elif token not in success or not Path(path).is_file():
                result={"lines":[],"audit":{"token":token,"file_name":md.get("name") or md.get("file_name"),"status":"pending_or_failed_download","download_status":latest.get(token,{}).get("status"),"line_count":0,"issues":["not_read_as_current_source"]}}
            else:
                try:
                    sha=hashfile(path);result=extract_file(path,md,source_sha256=sha)
                    scope=(result["audit"].get("factory"),result["audit"].get("market"),result["audit"].get("file_name"),sha)
                    if scope in seen:
                        orig=seen[scope];result["audit"]["duplicate_source_of"]=orig
                        result["audit"].setdefault("issues",[]).append("exact_duplicate_source_bytes")
                        for line in result["lines"]:
                            line["duplicate_source_of"]=orig;line["duplicate_line_of"]=line["line_id"].replace(str(token)+":",orig+":",1)
                            line["issues"].append("exact_duplicate_same_factory_market_name_bytes");line["include_in_summary"]=False
                        result["audit"]["summary_quantity_sum"]=0
                    else:seen[scope]=token
                    if sha in seen_sha:
                        result["audit"].setdefault("issues",[]).append("same_bytes_other_document_identity_requires_review")
                        result["audit"]["same_bytes_other_tokens"]=seen_sha[sha][:]
                        for line in result["lines"]:line["issues"].append("same_bytes_other_document_identity_requires_review")
                    seen_sha[sha].append(token)
                except Exception as exc:result={"lines":[],"audit":{"token":token,"status":"unexpected_extraction_error","error":f"{type(exc).__name__}: {exc}","line_count":0}}
            for line in result["lines"]:lo.write(json.dumps(line,ensure_ascii=False)+"\n")
            au.write(json.dumps(result["audit"],ensure_ascii=False)+"\n")
            stats[result["audit"]["status"]]+=1;stats["lines"]+=len(result["lines"])
            if i%100==0:print(json.dumps({"processed":i,"total":len(items),"stats":stats},ensure_ascii=False),flush=True)
    finalize_outputs(ot,at)
    os.replace(ot,op);os.replace(at,ap)
    print(json.dumps({"output_lines":str(op),"output_audit":str(ap),"stats":stats},ensure_ascii=False),flush=True)
if __name__=="__main__":main()

