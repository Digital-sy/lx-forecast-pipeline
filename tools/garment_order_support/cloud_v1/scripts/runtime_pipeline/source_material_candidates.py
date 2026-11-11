"""Local conservative projection of pinned parser evidence; never confirms a mapping.

Only explicit fabric headers, component headers and literal color-number cells are
structured. Free prose, image references and ambiguous scopes remain review items.
"""
from __future__ import annotations
import argparse
from collections import Counter,defaultdict
import hashlib
import json
from pathlib import Path
import re

VERSION='1.0.0'
COLOR_HEADERS={'主布色号':'主布','里布色号':'里布','面料色号':None,'色号':None}
MATERIAL_HEADERS={'面料','面料名称','面料布种号','主布','里布'}
COLOR=re.compile(r'^\s*([0-9]+(?:\.[0-9]+)?[#＃]|[A-Za-z]+[0-9]+)\s*[-－:：]?\s*([^#＃\n,，;/；]*)\s*$')
NUMBER_TOKENS=re.compile(r'(?<![A-Za-z0-9])(?:[0-9]+(?:\.[0-9]+)?[#＃]|[A-Za-z]+[0-9]+)(?![A-Za-z0-9])')

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def is_image(text):return any(x in str(text).upper() for x in ('DISPIMG','IMAGE(','.PNG','.JPG','.JPEG'))

def extract(rows):
    result=[];ids=set()
    for row in rows:
        key=row.get('line_id')
        if not key or key in ids:raise ValueError('MISSING_OR_DUPLICATE_RAW_LINE_ID')
        ids.add(key)
        common={k:row.get(k) for k in ('line_id','token','source_sha256','source_url','file_name','spu','sku_raw','sheet','sheet_index','row','table_header_row','garment_color_raw','color_code','quantity','quantity_reliable')}
        common.update(material_context_header_raw=row.get('fabric_header'),garment_color_system=None,
                      human_confirmation=False,future_default_confirmed=False,lingxing_candidate=None,
                      raw_source_issues=row.get('issues',[]),source_cells=row.get('evidence_cells',[]))
        sources=row.get('fabric_assignment_sources',[])
        # Header is a document-level context, not a blanket color or part assignment.
        if row.get('fabric_header'):
            result.append({**common,'candidate_id':key+':header','candidate_kind':'MATERIAL_HEADER_CONTEXT',
              'material_name_raw':row['fabric_header'],'part_raw':None,'physical_color_number_raw':None,
              'physical_color_name_raw':None,'header_raw':None,'text_raw':row['fabric_header'],
              'cell_refs':[c for c in row.get('evidence_cells',[]) if c.get('role')=='fabric_header'],
              'scope':'HEADER_CONTEXT_ONLY','status':'REVIEW_REQUIRED','reasons':['NOT_A_CONFIRMED_COMPONENT_OR_COLOR_ASSIGNMENT']})
        components=[]
        for i,s in enumerate(sources):
            text=str(s.get('text') or '');header=s.get('header');reasons=[]
            scope_ok=s.get('scope_confirmed') is True and row.get('row') in s.get('applies_to_rows',[])
            image=is_image(text) or s.get('material_relevance')=='image_reference'
            rec={**common,'candidate_id':key+':assignment:'+str(i),'candidate_kind':'SOURCE_ASSIGNMENT',
              'material_name_raw':None,'part_raw':None,'physical_color_number_raw':None,'physical_color_name_raw':None,
              'header_raw':header,'text_raw':text,'cell_refs':[{'sheet':s.get('sheet'),'cell':s.get('cell')}],
              'scope':s.get('scope'),'applies_to_rows':s.get('applies_to_rows',[]),
              'source_assignment_status':s.get('assignment_status'),'source_kind':s.get('source_kind'),
              'status':'REVIEW_REQUIRED','reasons':reasons}
            if not scope_ok: reasons.append('ROW_SCOPE_NOT_VERIFIED')
            if image: reasons.append('IMAGE_CONTENT_NOT_READ')
            if scope_ok and not image:
                if header in COLOR_HEADERS:
                    rec['part_raw']=COLOR_HEADERS[header]
                    matches=NUMBER_TOKENS.findall(text)
                    if len(matches)>1:
                        reasons.append('MULTIPLE_COLOR_NUMBERS_CONFLICT');rec['physical_color_candidates_raw']=matches
                    else:
                        m=COLOR.fullmatch(text)
                        if m:
                            rec['physical_color_number_raw']=m.group(1);rec['physical_color_name_raw']=m.group(2).strip() or None
                            rec['status']='EXPLICIT_LITERAL_COLOR_NUMBER_UNCONFIRMED_MAPPING'
                        else: reasons.append('COLOR_NUMBER_NOT_UNAMBIGUOUS_LITERAL')
                elif header in MATERIAL_HEADERS and text.strip() and not text.lstrip().startswith('='):
                    rec['material_name_raw']=text
                    rec['part_raw']=header if header in ('主布','里布') else None
                    rec['status']='EXPLICIT_MATERIAL_CELL_UNCONFIRMED_MAPPING'
                elif s.get('material_relevance')=='general_instruction':reasons.append('GENERAL_INSTRUCTION_NOT_MATERIAL_ASSIGNMENT')
                else:reasons.append('FREE_TEXT_OR_COMPONENT_LAYOUT_REQUIRES_REVIEW')
            if rec['material_name_raw'] is None:reasons.append('EXACT_COMPONENT_MATERIAL_UNRESOLVED')
            if rec['part_raw'] is None:reasons.append('COMPONENT_UNRESOLVED')
            if rec['physical_color_number_raw'] is None:reasons.append('PHYSICAL_COLOR_NUMBER_UNRESOLVED')
            components.append(rec)
        by_part=defaultdict(set)
        for rec in components:
            if rec['part_raw'] and rec['physical_color_number_raw']:by_part[rec['part_raw']].add(rec['physical_color_number_raw'])
        for rec in components:
            if len(by_part.get(rec['part_raw'],()))>1:
                rec['reasons'].append('SAME_PART_CONFLICTING_COLOR_NUMBERS');rec['status']='CONFLICT_REQUIRES_REVIEW'
        result.extend(components)
        if not sources:
            result.append({**common,'candidate_id':key+':missing','candidate_kind':'NO_ASSIGNMENT',
              'material_name_raw':None,'part_raw':None,'physical_color_number_raw':None,'physical_color_name_raw':None,
              'status':'REVIEW_REQUIRED','reasons':['NO_ROW_MATERIAL_ASSIGNMENT']})
    return result

def build(order_lines,output):
    src=Path(order_lines);out=Path(output)
    if out.exists():raise FileExistsError('OUTPUT_EXISTS')
    before=sha(src);rows=[json.loads(l) for l in src.read_text(encoding='utf-8-sig').splitlines() if l.strip()]
    candidates=extract(rows);out.mkdir(parents=True,exist_ok=False)
    def write(name,data):
        with (out/name).open('x',encoding='utf-8') as f:
            for r in data:f.write(json.dumps(r,ensure_ascii=False)+'\n')
    write('material_candidates.jsonl',candidates)
    write('needs_review.jsonl',[r for r in candidates if r.get('reasons')])
    if sha(src)!=before:raise ValueError('SOURCE_CHANGED')
    manifest={'version':VERSION,'status':'LOCAL_EVIDENCE_CANDIDATES_NOT_CONFIRMED',
      'source':{'path':str(src.resolve()),'sha256':before},'raw_rows':len(rows),'candidate_rows':len(candidates),
      'status_counts':dict(Counter(r['status'] for r in candidates)),
      'explicit_color_number_rows':sum(r.get('physical_color_number_raw') is not None for r in candidates),
      'image_unknown_rows':sum('IMAGE_CONTENT_NOT_READ' in r.get('reasons',[]) for r in candidates),
      'outputs':{n:sha(out/n) for n in ('material_candidates.jsonl','needs_review.jsonl')},
      'source_unchanged':True,'database_operations':0,'manual_confirmations_created':0,
      'limitations':['Existing 1975 historical semantic rows are not reinterpreted.','Header context is not an exact component assignment.','Literal physical numbers are not verified Lingxing SKU matches.','Images and free prose remain explicit review items.']}
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--order-lines',required=True);p.add_argument('--output-dir',required=True);a=p.parse_args()
    print(json.dumps(build(a.order_lines,a.output_dir),ensure_ascii=False))
