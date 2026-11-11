"""Offline reference-page lineage audit. No remote calls or writes to source archives."""
from __future__ import annotations
import argparse, collections, csv, hashlib, json
from datetime import datetime, timezone
from pathlib import Path

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def jread(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))
def decode(v):
    return json.loads(v) if isinstance(v,str) else v
def stable_id(*v):
    return hashlib.sha256(json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()).hexdigest()
def source_key(ref):
    # No fuzzy name, SPU, color or hash-only cross-document join.
    return ref["token"], ref["source_sha256"]
def version_binding(ref, documents, versions):
    doc=documents.get(ref.get("token"))
    if not doc: return None,"SOURCE_DOCUMENT_NOT_FOUND"
    matches=versions.get((doc["document_id"],ref.get("source_sha256")),[])
    if len(matches)!=1:return None,"SOURCE_VERSION_NOT_UNIQUE_OR_ABSENT"
    return {"document_id":doc["document_id"],"version_id":matches[0]["version_id"]},"EXACT_DOCUMENT_AND_CONTENT_SHA256"
def locator_quality(ref):
    cells={c["cell"]:c["value"] for c in ref.get("cells",[])}
    identity=bool(ref.get("identity_cells")) and all(k in cells for k in ref["identity_cells"])
    nums=ref.get("explicit_number_evidence",[])
    number=bool(nums) and all(e.get("cell") in cells and str(cells[e["cell"]])==str(e["raw"]) for e in nums)
    return bool(ref.get("sheet")) and isinstance(ref.get("row"),int) and ref["row"]>0 and identity,number
def classify_case(refs):
    if not refs:return "NO_ARCHIVED_COLOR_LOCATOR"
    if all(r["lineage_status"]=="EXACT_REFERENCE_PAGE_LINEAGE_VERIFIED" for r in refs):
        return "REFERENCE_PAGE_LINEAGE_RECOVERABLE"
    return "REFERENCE_PAGE_LINEAGE_PARTIAL"
def csv_write(path, rows, columns):
    with Path(path).open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=columns,extrasaction="ignore");w.writeheader();w.writerows(rows)
def main():
    ap=argparse.ArgumentParser()
    for k in ("receipt","completion","manual-archive","review-registry","builder","pending","out"):
        ap.add_argument("--"+k,required=True)
    args=ap.parse_args(); args=argparse.Namespace(**{k:str(Path(v)) for k,v in vars(args).items()}); out=Path(args.out)
    outputs=out/"outputs"; verification=out/"verification"
    expected_files=[outputs/"reference_followup.json",outputs/"reference_followup_47引用复核.csv",
                    outputs/"reference_candidates_参考页引用候选.csv",outputs/"reference_summary.md",
                    verification/"reference_verification.json"]
    if any(p.exists() for p in expected_files):raise RuntimeError("Refuse to overwrite existing reference audit")
    input_paths=[Path(args.receipt),Path(args.completion),Path(args.manual_archive),Path(args.review_registry),Path(args.builder),Path(args.pending)]
    before={str(p):sha(p) for p in input_paths}
    completion=jread(args.completion)
    assert completion["status"]=="COMMITTED_VERIFIED"
    assert before[args.receipt]==completion["production_receipt_sha256"]
    tables=jread(args.receipt)["tables"]; archive=jread(args.manual_archive)
    registry=jread(args.review_registry); masters={r["id"]:r for r in archive["master"]}
    assert len(masters)==len(archive["master"])
    file_table=next(t for t in registry["tables"] if t["name"]=="候选文件")
    file_rows={r[0]:dict(zip(file_table["headers"],r)) for r in file_table["rows"]}
    documents={r["source_id"]:r for r in tables["dim_sm_garment_order_document"]}
    versions=collections.defaultdict(list)
    for r in tables["ods_sm_garment_order_document_version"]:
        versions[(r["document_id"],r["content_sha256"])].append(r)
    raw_locator=collections.defaultdict(list)
    for r in tables["dwd_sm_garment_order_color_evidence"]:
        raw_locator[(r["version_id"],r["sheet_name"],r["row_number"])].append(r["evidence_id"])
    current_refs=collections.defaultdict(list)
    for r in tables["dwd_sm_garment_confirmation_evidence_ref"]:current_refs[r["confirmation_id"]].append(r["reference_id"])
    pending=[r for r in tables["ods_manual_sm_garment_color_confirmation"] if r["reference_status"]=="REVIEW_REQUIRED"]
    file_checks={};source_evidence_checks=[]
    for p in archive["references"]["source_files"]:
        found=Path(p["path"]).is_file()
        source_evidence_checks.append({**p,"exists":found,"actual_sha256":sha(p["path"]) if found else None})
        assert found and source_evidence_checks[-1]["actual_sha256"]==p["sha256"]
        before[p["path"]]=p["sha256"]
    with Path(args.pending).open(encoding="utf-8-sig") as f: pending_demands=list(csv.DictReader(f))
    pending_review_ids={r["review_id"] for r in pending_demands}
    cases=[]; candidates=[]
    for manual in sorted(pending,key=lambda r:r["review_id"]):
        rid=manual["review_id"]; master=masters[rid]; raw=decode(manual["raw_json"])
        resolution=master["manual_resolution"]; manual_snapshot=raw["manual_resolution"]
        assert resolution==manual_snapshot,("Manual archive differs",rid)
        assert manual["manual_archive_sha256"]==before[args.manual_archive]
        scope=decode(manual["scope_json"])
        assert scope==resolution["scope"]
        for key in ("confirmed_material","confirmed_part","confirmed_physical_color","confirmed_lingxing_sku","confirmed_product_name"):
            assert manual[key]==resolution.get(key),(rid,key)
        old_refs=current_refs[manual["confirmation_id"]]
        refs=(master.get("reference_evidence") or {}).get("references",[])
        audited=[]
        for ref in refs:
            assert ref["review_id"]==rid
            binding,binding_status=version_binding(ref,documents,versions)
            p=ref["source_path"]
            if p not in file_checks:
                exists=Path(p).is_file()
                file_checks[p]={"path":p,"expected_sha256":ref["source_sha256"],"actual_sha256":sha(p) if exists else None,"exists":exists}
            assert file_checks[p]["expected_sha256"]==ref["source_sha256"]
            file_ok=file_checks[p]["actual_sha256"]==ref["source_sha256"]
            supporting=[]
            for fid in resolution["supporting_file_ids"]:
                rr=file_rows.get(fid)
                if rr and rr["打开原文件"].rstrip("/").split("/")[-1]==ref["token"] and rr["文件SHA256"]==ref["source_sha256"]:
                    supporting.append(fid)
            cells=ref.get("cells",[])
            cell_map={c["cell"]:c["value"] for c in cells}
            identity_ok=bool(ref.get("identity_cells")) and all(k in cell_map for k in ref["identity_cells"])
            exact_numbers=ref.get("explicit_number_evidence",[])
            numbers_ok=bool(exact_numbers) and all(e.get("cell") in cell_map and str(cell_map[e["cell"]])==str(e["raw"]) for e in exact_numbers)
            locator_ok=bool(ref.get("sheet")) and isinstance(ref.get("row"),int) and ref["row"]>0 and identity_ok and numbers_ok
            existing=raw_locator.get((binding["version_id"],ref["sheet"],ref["row"]),[]) if binding else []
            ok=bool(binding and file_ok and supporting and locator_ok)
            cid=stable_id("REFERENCE_PAGE_SUPPORT_CANDIDATE",manual["confirmation_id"],ref["token"],ref["source_sha256"],ref["sheet"],ref["row"],cells)
            item={"candidate_ref_id":cid,"confirmation_id":manual["confirmation_id"],"review_id":rid,
                  "existing_reference_ids":old_refs,"source_document_id":binding["document_id"] if binding else None,
                  "source_version_id":binding["version_id"] if binding else None,"original_order_evidence_ids_at_same_locator":existing,
                  "proposed_order_evidence_id":None,"proposed_material_evidence_id":None,
                  "reference_role_candidate":"SUPPORTING_ARCHIVED_REFERENCE_PAGE",
                  "lineage_status":"EXACT_REFERENCE_PAGE_LINEAGE_VERIFIED" if ok else "REFERENCE_PAGE_LINEAGE_NOT_COMPLETE",
                  "binding_status":binding_status,"source_file_hash_verified":file_ok,
                  "locator_fields_complete":locator_ok,"identity_locator_present":identity_ok,"explicit_physical_number_present":numbers_ok,"registry_supporting_file_ids":supporting,
                  "refid_is_diagnostic_only":True,"database_reference_created":False,
                  "source_token":ref["token"],"source_sha256":ref["source_sha256"],"source_path":p,
                  "source_url":ref.get("source_url"),"source_file_name":ref.get("file_name"),
                  "sheet":ref["sheet"],"row":ref["row"],"identity_cells":ref.get("identity_cells",[]),"cells":cells,
                  "explicit_number_evidence":exact_numbers,"same_file_material_context":ref.get("same_file_material_context",[]),
                  "source_location_kind":ref.get("source_location_kind"),"read_mode":ref.get("read_mode"),
                  "provenance":{"manual_archive_path":args.manual_archive,"manual_archive_sha256":before[args.manual_archive],
                      "json_locator":f"master[id={rid}].reference_evidence.references",
                      "original_reference_use":ref.get("reference_use_only"),
                      "original_future_default_confirmed":ref.get("future_default_confirmed"),
                      "cell_readback_this_run":"NOT_REPARSED; verified original source bytes and exact archived cell evidence"},
                  "scope_warning":"Reference page support only; not an order color line, normal purchase proof, chosen file, part approval or full garment recipe."}
            audited.append(item);candidates.append(item)
        status=classify_case(audited)
        reason="原人工事件没有订单行source_line_ids；参考页色号已归档但现有迁移器仅接订单行，不导入参考页引用。" if refs else "原人工事件没有订单行source_line_ids；现有逐色归档也没有可定位的参考页证据，不能凭同款文件或颜色名称补链。"
        cases.append({"confirmation_id":manual["confirmation_id"],"review_id":rid,"spu":master["spu"],
            "scope":scope,"source_color":master["source_color"],"reference_status_before":manual["reference_status"],
            "reference_status_after":manual["reference_status"],"existing_reference_ids":old_refs,
            "diagnostic_status":status,"why_no_reference":reason,"in_pending_demand_review_ids":rid in pending_review_ids,
            "manual_confirmation_status":manual["confirmation_status"],
            "manual_decision_verbatim":manual["decision_verbatim"],
            "confirmed_material":manual["confirmed_material"],"confirmed_part":manual["confirmed_part"],
            "confirmed_physical_color":manual["confirmed_physical_color"],"confirmed_lingxing_sku":manual["confirmed_lingxing_sku"],
            "manual_business_confirmation_preserved":True,
            "applied_to_current_source_before":manual["applied_to_current_source"],
            "applied_to_current_source_after":manual["applied_to_current_source"],
            "manual_source_uri":manual["decision_source_uri"],"manual_source_sha256":manual["decision_source_sha256"],
            "manual_source_locator":decode(manual["decision_source_locator_json"]),
            "source_line_ids":raw["source_line_ids"],"source_version_links":raw["source_version_links"],
            "unresolved_references":decode(manual["unresolved_references_json"]),
            "manual_supporting_file_ids":resolution["supporting_file_ids"],"manual_selected_file_ids":resolution["selected_file_ids"],
            "reference_candidate_ids":[r["candidate_ref_id"] for r in audited],
            "reference_candidate_count":len(audited),"verified_reference_candidate_count":sum(r["lineage_status"]=="EXACT_REFERENCE_PAGE_LINEAGE_VERIFIED" for r in audited),
            "existing_order_locator_count":sum(len(r["original_order_evidence_ids_at_same_locator"]) for r in audited),
            "remaining":decode(manual["remaining_json"]),"manual_notes":resolution["notes"],
            "missing_source_evidence":master.get("missing_source_evidence"),
            "reference_unknowns":(master.get("reference_evidence") or {}).get("unknowns",[]),
            "nov_qty":master.get("nov_qty"),"dec_qty":master.get("dec_qty"),"total_qty":master.get("total_qty"),
            "quantity_note":"原人工复核对象行关联件数，仅供定位；不是去重产品需求，不能与297待确认需求汇总相加。",
            "next_action":"审核新增参考页证据类型与引用链；保留人工决定；不可伪造订单行ID。" if refs else "需要真实逐色证据与具体SKU；保留已采用意向，不自动补链或确认SKU。",
            "writeback_performed":False})
    assert len({r["candidate_ref_id"] for r in candidates})==len(candidates)
    assert all(not r["existing_reference_ids"] for r in cases)
    assert all(not r["source_line_ids"] and not r["source_version_links"] for r in cases)
    status_counts=dict(collections.Counter(r["diagnostic_status"] for r in cases))
    manual_counts=dict(collections.Counter(r["manual_confirmation_status"] for r in cases))
    summary={"reviewed_confirmation_events":len(cases),"manual_adopted_without_unique_sku":sum(not r["confirmed_lingxing_sku"] for r in tables["ods_manual_sm_garment_color_confirmation"]),"production_reference_count":len(tables["dwd_sm_garment_confirmation_evidence_ref"]),
       "diagnostic_status_counts":status_counts,"manual_status_counts":manual_counts,
       "reference_page_events":sum(bool(r["reference_candidate_count"]) for r in cases),
       "reference_page_candidates":len(candidates),
       "exact_physical_number_reference_candidates":sum(r["lineage_status"]=="EXACT_REFERENCE_PAGE_LINEAGE_VERIFIED" for r in candidates),
       "identity_color_only_reference_candidates":sum(not r["explicit_physical_number_present"] for r in candidates),
       "pending_demand_rows":len(pending_demands),"pending_unique_review_ids":len(pending_review_ids),
       "pending_reference_review_intersection_ids":sorted(r["review_id"] for r in cases if r["in_pending_demand_review_ids"]),
       "status_breakdown":{k:{"events":sum(r["diagnostic_status"]==k for r in cases),"manual_status_counts":dict(collections.Counter(r["manual_confirmation_status"] for r in cases if r["diagnostic_status"]==k)), "pending_review_id_intersection":sorted(r["review_id"] for r in cases if r["diagnostic_status"]==k and r["in_pending_demand_review_ids"])} for k in status_counts},"distinct_original_files":len(file_checks),
       "original_files_sha256_match":sum(v["actual_sha256"]==v["expected_sha256"] for v in file_checks.values()),
       "all_candidates_document_version_exact":all(c["binding_status"]=="EXACT_DOCUMENT_AND_CONTENT_SHA256" for c in candidates),
       "all_candidate_locators_absent_from_order_index":all(not c["original_order_evidence_ids_at_same_locator"] for c in candidates),
       "reference_recoverable_unique_sku_events":sum(c["diagnostic_status"]=="REFERENCE_PAGE_LINEAGE_RECOVERABLE" and bool(c["confirmed_lingxing_sku"]) for c in cases),
       "reference_recoverable_still_business_pending_events":sum(c["diagnostic_status"]=="REFERENCE_PAGE_LINEAGE_RECOVERABLE" and not c["confirmed_lingxing_sku"] for c in cases),
       "database_writes":0,"feishu_writes":0,"formal_mapping_writes":0,"remote_calls":0,
       "time_basis":"Current local archive verification; production state refers to supplied successful readback, not a new live query."}
    # Rehash all referenced sources; retain original archive and human fields unchanged.
    for p,check in file_checks.items():before[p]=check["expected_sha256"]
    after={p:sha(p) for p in before}
    assert before==after
    result={"schema_version":"reference_followup_v1","created_at_utc":datetime.now(timezone.utc).isoformat(),
        "summary":summary,"source_manifest":[{"path":p,"sha256":h} for p,h in before.items()],
        "cases":cases,"reference_candidates":candidates,
        "limitations":["No new source parsing, images or source downloads; exact archived cell locators reviewed against unchanged original file hashes.",
          "Reference-page support is not normal-order line evidence or future full-garment scheme approval.",
          "References are candidates only; current REVIEW_REQUIRED and manual conclusions remain unchanged.",
          "No reference chain can be inferred from SPU, color name or physical number alone."]}
    outputs.mkdir(parents=True,exist_ok=True);verification.mkdir(parents=True,exist_ok=True)
    expected_files[0].write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    case_rows=[]
    for r in cases:
        case_rows.append({"人工确认ID":r["confirmation_id"],"复核ID":r["review_id"],"款号":r["spu"],"店铺":r["scope"].get("shops"),
            "原成衣颜色":r["source_color"],"原人工状态":r["manual_confirmation_status"],"确认面料":r["confirmed_material"],
            "确认实物色号":r["confirmed_physical_color"],"确认领星SKU":r["confirmed_lingxing_sku"],
            "原引用状态":r["reference_status_before"],"本轮诊断":r["diagnostic_status"],"缺引用原因":r["why_no_reference"],
            "参考页引用候选数":r["reference_candidate_count"],"逐项通过数":r["verified_reference_candidate_count"],
            "原订单行ID数":len(r["source_line_ids"]),"现有引用ID":";".join(r["existing_reference_ids"]),
            "原人工单元格":json.dumps(r["manual_source_locator"],ensure_ascii=False),
            "人工支持文件":";".join(r["manual_supporting_file_ids"]),"候选引用ID":";".join(r["reference_candidate_ids"]),
            "仍待业务确认":r["remaining"],"在需求待确认ID集合中":"是" if r["in_pending_demand_review_ids"] else "否","下一步":r["next_action"],"已写回":"否"})
    csv_write(expected_files[1],case_rows,list(case_rows[0]))
    ref_rows=[]
    for r in candidates:
        ref_rows.append({"候选引用ID":r["candidate_ref_id"],"人工确认ID":r["confirmation_id"],"复核ID":r["review_id"],
           "源文档ID":r["source_document_id"],"源版本ID":r["source_version_id"],"原始文件Token":r["source_token"],
           "文件SHA256":r["source_sha256"],"文件名":r["source_file_name"],"工作表":r["sheet"],"行号":r["row"],
           "身份单元格":";".join(r["identity_cells"]),"单元格证据":json.dumps(r["cells"],ensure_ascii=False),
           "色号证据":json.dumps(r["explicit_number_evidence"],ensure_ascii=False),"人工文件编号":";".join(r["registry_supporting_file_ids"]),
           "候选角色":r["reference_role_candidate"],"核验状态":r["lineage_status"],
           "当前订单行ID":";".join(r["original_order_evidence_ids_at_same_locator"]),
           "原件路径":r["source_path"],"原件链接":r["source_url"],"已写回":"否"})
    csv_write(expected_files[2],ref_rows,list(ref_rows[0]) if ref_rows else [])
    report=f"""# 人工确认引用待复核核查

当前归档回读中共 {len(cases)} 条人工事件仍为 REVIEW_REQUIRED；既有 {summary['production_reference_count']} 条引用保持不变。本轮分类全部覆盖{len(cases)}条：

| 引用证据状态 | 事件数 | 含义 |
| --- | ---: | --- |
| 参考页候选完整 | {status_counts.get('REFERENCE_PAGE_LINEAGE_RECOVERABLE',0)} | 每个候选均有确切文件版本、身份与色号单元格 |
| 参考页候选混合 PARTIAL | {status_counts.get('REFERENCE_PAGE_LINEAGE_PARTIAL',0)} | 涉及{"、".join(sorted({r["spu"] for r in cases if r["diagnostic_status"]=="REFERENCE_PAGE_LINEAGE_PARTIAL"}))}，既有带色号证据，也混有只见成衣色名、不见色号的参考页 |
| 现有逐色归档无参考页 | {status_counts.get('NO_ARCHIVED_COLOR_LOCATOR',0)} | 不凭同款文件或色名补链，继续待确认 |

共有 {summary['reference_page_events']} 条找到参考页、{len(candidates)} 个引用候选，涉及 {len(file_checks)} 个原文件。全部原文件 SHA256 与封存一致，全部 token+SHA 唯一对应当前文件版本。候选中 {summary['exact_physical_number_reference_candidates']} 条带明确色号；{summary['identity_color_only_reference_candidates']} 条只有身份/色名，不可冒充实物色号证据。{status_counts.get("REFERENCE_PAGE_LINEAGE_PARTIAL",0)}条混合事件分别仍有{"、".join(str(r["verified_reference_candidate_count"]) for r in cases if r["diagnostic_status"]=="REFERENCE_PAGE_LINEAGE_PARTIAL")}条带色号证据，不能解释成完全无证据。

全部{len(cases)}条原订单行source_line_ids为空；参考页位置均没有当前订单行证据ID。现有迁移器仅遍历 source_version_links 并要求订单行+材料根证据，因此没有纳入 reference_evidence 的参考页。这里恢复的是可审查追溯候选，不是推定未来方案。

原人工状态：{json.dumps(manual_counts,ensure_ascii=False)}。有参考页事件的人工状态分布为 {json.dumps(dict(collections.Counter(r["manual_confirmation_status"] for r in cases if r["reference_candidate_count"])),ensure_ascii=False)}；无逐色参考页事件的人工状态分布为 {json.dumps(dict(collections.Counter(r["manual_confirmation_status"] for r in cases if not r["reference_candidate_count"])),ensure_ascii=False)}。各事件的人工决定、SKU、范围和来源单元格均原样保留。

待确认需求文件实读 {len(pending_demands)} 个店铺×颜色需求行、{len(pending_review_ids)} 个去重review_id；与{len(cases)}条引用队列交集为 {len(summary['pending_reference_review_intersection_ids'])} 个review_id。{len(cases)}引用状态、{summary["manual_adopted_without_unique_sku"]}已采用但未落唯一SKU和{len(pending_review_ids)}需求待确认是不同分母，不能相加。

本轮没有数据库、飞书或正式映射写回。建议审核独立参考页证据类型及引用支持；不要伪造订单行ID、不要把支持文件升级为用户指定文件。引用补齐也不自动消除尚缺SKU、多方案、部位或未来完整用料方案的业务事项。

限制：复核既有单元格提取和原文件哈希，未重新解析{len(file_checks)}份文件或视觉重读图片。原未知的材料版本、部位、正常采购资格及未来适用性继续保留。生产状态来源于已保存的成功回读，不是本轮实时查询。
"""
    expected_files[3].write_text(report,encoding="utf-8")
    validation={"status":"PASS","summary":summary,"source_files_before_after_identical":before==after,
       "manual_confirmation_fields_identical":True,"production_receipt_hash_matches_completion":True,
       "source_hash_checks":list(file_checks.values()),"reference_source_manifests":source_evidence_checks,
       "output_sha256":{str(p):sha(p) for p in expected_files[:4]},
       "script_sha256":sha(__file__),"command_inputs":vars(args)}
    expected_files[4].write_text(json.dumps(validation,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False))
if __name__=="__main__":main()
