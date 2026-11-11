"""Synthetic local package tests; no network, credentials or database connections."""
import copy
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import build_increment_package as m

DDL=Path(os.environ['GARMENT_SOURCE_DDL'])
SCHEMA=m.source.parse_schema(DDL.read_text(encoding='utf-8-sig'))


def metadata():
    result={}
    for table in m.TABLES:
        cols=[]
        for ordinal,(name,spec) in enumerate(SCHEMA[table]['columns'].items(),1):
            default=0 if name=='delete_flag' else ('UNKNOWN' if spec['has_default'] and not spec['nullable'] and name not in m.append.AUTO_TIME else None)
            cols.append({'TABLE_NAME':table,'COLUMN_NAME':name,'ORDINAL_POSITION':ordinal,
                'DATA_TYPE':spec['base_type'].lower(),'COLUMN_TYPE':spec['type'].lower()+(' unsigned' if spec['unsigned'] else ''),
                'IS_NULLABLE':'YES' if spec['nullable'] else 'NO','COLUMN_DEFAULT':default,'EXTRA':'',
                'CHARACTER_SET_NAME':None,'COLLATION_NAME':None})
        result[table]=cols
    return result


def fixture():
    source='synthetic-file';h='b'*64;version='a'*64;stamp='2026-11-10T00:00:00+00:00'
    doc={'source_id':source,'version_id':version,'content_sha256':h,'file_verification':'HASH_MATCH','parse_status':'ok',
       'inventory':{'token':source,'path':'synthetic','modified_time':None,'created_time':None},
       'file_audit':{'source_sha256':h,'extraction_version':'synthetic/1'},
       'same_content_source_ids':[source],'file_name':'synthetic.xlsx','source_url':'https://example.invalid/file/synthetic-file',
       'metadata_status':'ARCHIVED','inventory_as_of':stamp,'metadata_changes':[]}
    raw={'line_id':'synthetic-line','token':source,'source_sha256':h,'line_type':'order_color','quantity':0,'quantity_reliable':True,
       'spu':'TEST','sheet':'synthetic','row':1,'color_name':'未确定','fabric_row_note':'保留原文'}
    item={'source_id':source,'version_id':version,'content_sha256':h,'evidence_id':'c'*64,'raw':raw,'future_applicability':'NOT_DECIDED_BY_INDEX'}
    dl={'token':source,'status':'ok','sha256':h,'bytes':12,'local_path':'synthetic.xlsx','format':'xlsx','inspect':{'checked_at':stamp},'acquired_at':stamp}
    plan={'created_at':stamp,'scope':[{'token':source}]}
    acq={'status':'COMPLETE','failed':0,'not_run':0,'source_count':1,'downloaded':1,'finished_at':stamp,'snapshot_limitations':'Synthetic non-atomic observation'}
    parser={'status':'PARSED','returncode':0,'line_count':1,'parser_sha256':'d'*64,'helper_sha256':'e'*64,
       'inventory_sha256':'f'*64,'downloads_sha256':'1'*64,'plan_sha256':'2'*64,'command':['synthetic'],
       'output_sha256':{'order_lines.jsonl':'3'*64},'started_at':stamp,'finished_at':stamp}
    index={'summary':{'documents':1,'order_rows':1,'enriched_rows':0,'confirmations':0}}
    hashes={'download_manifest':'4'*64,'parse_manifest':'5'*64,'index_manifest':'6'*64}
    paths={'download_manifest':'synthetic/acquisition.json','order_lines':'synthetic/order_lines.jsonl'}
    return [[doc],[item],[dl],plan,acq,parser,index,hashes,paths]


def convert(args=None):
    return m.source.convert_data(*(args or fixture()))


def existing(package):
    return {t:{r[m.loader.PK[t]]:copy.deepcopy(r) for r in rows} for t,rows in package.items()}


def db_snapshot(package):
    data=copy.deepcopy(package);meta=metadata()
    for table,rows in data.items():
        for row in rows:
            row['create_time']='2026-11-01 00:00:00.000001';row['update_time']='2026-11-01 00:00:00.000002'
            for col in meta[table]:
                name=col['COLUMN_NAME']
                if name not in row:row[name]=col['COLUMN_DEFAULT']
                if col['DATA_TYPE']=='json' and row[name] is not None:
                    row[name]=json.dumps(row[name],ensure_ascii=False)
    return {'status':'SNAPSHOT_VERIFIED','metadata':meta,'tables':data}


class PackageTests(unittest.TestCase):
    def test_raw_converter_validates_original_schema(self):m.source.validate_rows(convert(),SCHEMA)
    def test_new_ids_preserve_converter_and_last_three_empty(self):
        p=convert();out,checks=m.preserve_document_versions(p,{t:{} for t in m.TABLES});self.assertEqual(p,out);self.assertEqual(checks,[]);self.assertTrue(all(not out[t] for t in m.TABLES[6:]))
    def test_existing_version_metadata_reused_exactly(self):
        p=convert();prior=existing(p);v=prior[m.TABLES[2]][p[m.TABLES[2]][0]['version_id']];v['archive_uri']='old/location.xlsx';v['origin_receipt_json']={'old':'receipt'};v['first_acquired_at_utc']='2026-10-01 00:00:00.000000'
        out,checks=m.preserve_document_versions(p,prior);self.assertEqual(out[m.TABLES[2]][0],v);self.assertEqual(len(checks),2)
    def test_existing_document_observation_time_preserved(self):
        p=convert();prior=existing(p);prior[m.TABLES[1]][p[m.TABLES[1]][0]['document_id']]['first_observed_at_utc']='2026-09-01 00:00:00.000000'
        out,_=m.preserve_document_versions(p,prior);self.assertEqual(out[m.TABLES[1]][0]['first_observed_at_utc'],'2026-09-01 00:00:00.000000')
    def test_source_receipt_stays_in_new_observation(self):
        p=convert();prior=existing(p);v=next(iter(prior[m.TABLES[2]].values()));v['archive_uri']='old.xlsx'
        out,_=m.preserve_document_versions(p,prior);self.assertEqual(out[m.TABLES[3]][0]['receipt_json']['download']['local_path'],'synthetic.xlsx')
    def test_no_mutation_of_package_or_baseline(self):
        p=convert();before=copy.deepcopy(p);prior=existing(p);out,_=m.preserve_document_versions(p,prior);out[m.TABLES[2]][0]['origin_receipt_json']['later']=1;self.assertEqual(p,before);self.assertNotIn('later',next(iter(prior[m.TABLES[2]].values()))['origin_receipt_json'])
    def test_same_version_different_hash_blocked(self):
        p=convert();prior=existing(p);next(iter(prior[m.TABLES[2]].values()))['content_sha256']='f'*64
        with self.assertRaises(ValueError):m.preserve_document_versions(p,prior)
    def test_same_bytes_different_version_id_blocked(self):
        p=convert();prior=existing(p);v=prior[m.TABLES[2]].pop(p[m.TABLES[2]][0]['version_id']);v['version_id']='f'*64;prior[m.TABLES[2]][v['version_id']]=v
        with self.assertRaises(ValueError):m.preserve_document_versions(p,prior)
    def test_same_source_different_document_id_blocked(self):
        p=convert();prior=existing(p);d=prior[m.TABLES[1]].pop(p[m.TABLES[1]][0]['document_id']);d['document_id']='f'*64;prior[m.TABLES[1]][d['document_id']]=d
        with self.assertRaises(ValueError):m.preserve_document_versions(p,prior)
    def test_byte_count_conflict_blocked(self):
        p=convert();prior=existing(p);next(iter(prior[m.TABLES[2]].values()))['byte_count']=99
        with self.assertRaises(ValueError):m.preserve_document_versions(p,prior)
    def test_format_conflict_blocked(self):
        p=convert();prior=existing(p);next(iter(prior[m.TABLES[2]].values()))['file_format']='xls'
        with self.assertRaises(ValueError):m.preserve_document_versions(p,prior)
    def test_deleted_identity_not_revived(self):
        p=convert();prior=existing(p);next(iter(prior[m.TABLES[1]].values()))['delete_flag']=1
        with self.assertRaises(ValueError):m.preserve_document_versions(p,prior)
    def test_manual_or_semantic_additions_blocked(self):
        for table in m.TABLES[6:]:
            p=convert();p[table]=[{}]
            with self.assertRaises(ValueError):m.preserve_document_versions(p,{t:{} for t in m.TABLES})
    def test_fabric_header_original_field_and_unknown_preserved(self):
        a=fixture();a[1][0]['raw']['fabric_header']='TEST原始面料';p=convert(a);self.assertEqual(p[m.TABLES[5]][0]['fabric_header_raw'],'TEST原始面料');self.assertEqual(p[m.TABLES[5]][0]['raw_json'],a[1][0]['raw'])
        a[1][0]['raw']['fabric_header']=None;p=convert(a);self.assertIsNone(p[m.TABLES[5]][0]['fabric_header_raw'])
    def test_unknown_not_zero(self):
        a=fixture();a[1][0]['raw']['quantity']=None;p=convert(a);self.assertIsNone(p[m.TABLES[5]][0]['quantity']);self.assertEqual(p[m.TABLES[5]][0]['quantity_state'],'UNKNOWN')
    def test_negative_not_removed(self):
        a=fixture();a[1][0]['raw']['quantity']=-2;p=convert(a);self.assertEqual(p[m.TABLES[5]][0]['quantity'],'-2');self.assertEqual(p[m.TABLES[5]][0]['quantity_state'],'NEGATIVE_SOURCE_VALUE')
    def test_zero_and_unconfirmed_color_preserved(self):
        p=convert();r=p[m.TABLES[5]][0];self.assertEqual(r['quantity'],'0');self.assertEqual(r['quantity_state'],'KNOWN_ZERO');self.assertIsNone(r['garment_color_system']);self.assertEqual(r['fabric_row_note_raw'],'保留原文')
    def test_incomplete_acquisition_blocked(self):
        a=fixture();a[4]['status']='PARTIAL'
        with self.assertRaises(ValueError):convert(a)
    def test_review_not_relabelled_ok(self):
        for status in ('parsed_with_review','no_order_lines_review'):
            a=fixture();a[0][0]['parse_status']=status
            with self.assertRaises(ValueError):convert(a)
    def test_out_of_scope_source_blocked(self):
        a=fixture();a[3]['scope']=[{'token':'other'}]
        with self.assertRaises(ValueError):convert(a)
    def test_snapshot_normalization_exact_json_and_times(self):
        snap=db_snapshot(convert());prior,norm=m.business_existing(snap);out,checks=m.preserve_document_versions(convert(),prior);plan=m.append.prepare_plan(out,snap['metadata'],norm);self.assertEqual(plan['insert_count'],0);self.assertEqual(sum(plan['skipped'].values()),6)
    def test_json_scalar_not_double_encoded(self):
        snap=db_snapshot(convert());snap['tables'][m.TABLES[0]][0]['limitations_json']=json.dumps('a scalar')
        prior,_=m.business_existing(snap);self.assertEqual(next(iter(prior[m.TABLES[0]].values()))['limitations_json'],'a scalar')
    def test_snapshot_status_required(self):
        snap=db_snapshot(convert());snap['status']='PARTIAL'
        with self.assertRaises(ValueError):m.business_existing(snap)
    def test_new_observation_reuses_doc_version_but_appends_raw(self):
        p=convert();snap=db_snapshot(p);prior,norm=m.business_existing(snap);a=fixture();a[7]['download_manifest']='7'*64;a[7]['parse_manifest']='8'*64;new=convert(a);out,_=m.preserve_document_versions(new,prior);plan=m.append.prepare_plan(out,snap['metadata'],norm);self.assertEqual(plan['insert_count'],4);self.assertEqual(plan['skipped'][m.TABLES[1]],1);self.assertEqual(plan['skipped'][m.TABLES[2]],1)
    def test_append_primary_key_content_conflict_not_hidden(self):
        snap=db_snapshot(convert());prior,norm=m.business_existing(snap);p=convert();p[m.TABLES[5]][0]['quantity']='9';out,_=m.preserve_document_versions(p,prior)
        with self.assertRaises(ValueError):m.append.prepare_plan(out,snap['metadata'],norm)


def artifact_fixture(root):
    def write(name,obj,lines=False):
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in obj) if lines else json.dumps(obj,ensure_ascii=False),encoding='utf-8')
        return path
    a=fixture();raw=root/'raw.xlsx';raw.write_bytes(b'synthetic source bytes');h=m.source.sha_file(raw)
    a[0][0]['content_sha256']=h;a[0][0]['file_audit']['source_sha256']=h;a[1][0]['content_sha256']=h;a[1][0]['raw']['source_sha256']=h
    a[2][0].update(sha256=h,bytes=raw.stat().st_size,local_path=str(raw))
    downloads=write('downloads.jsonl',a[2],True)
    inventory=write('inventory.json',{'synthetic':True})
    allowlist=write('allowlist.json',{'synthetic':True})
    actions=write('actions.jsonl',[{'synthetic':True}],True)
    parser_code=write('parser.py',{'not_executable':'synthetic'})
    helper_code=write('helper.py',{'not_executable':'synthetic'})
    line=write('order_lines.jsonl',[a[1][0]['raw']],True)
    audit=write('file_audit.jsonl',[a[0][0]['file_audit']],True)
    a[4].update(downloads_sha256=m.source.sha_file(downloads),inventory_sha256=m.source.sha_file(inventory),allowlist_sha256=m.source.sha_file(allowlist))
    a[5].update(parser_path=str(parser_code),parser_sha256=m.source.sha_file(parser_code),helper_path=str(helper_code),helper_sha256=m.source.sha_file(helper_code),
        inventory_sha256=m.source.sha_file(inventory),downloads_sha256=m.source.sha_file(downloads),plan_sha256=m.source.sha_file(actions),
        source_manifest={a[2][0]['token']:{'sha256':h}},output_sha256={'order_lines.jsonl':m.source.sha_file(line),'order_file_audit.jsonl':m.source.sha_file(audit)})
    acq=write('acquisition.json',a[4]);parse=write('parse_manifest.json',a[5]);plan=write('fetch_plan.json',a[3]);config_index=write('index_config.json',{'synthetic':True})
    idx=a[6];idx.update(status='LOCAL_INDEX_COMPLETE',raw_files_hash_checked=True,config={'path':str(config_index),'sha256':m.source.sha_file(config_index)},
        sources={k:{'path':str(p),'sha256':m.source.sha_file(p)} for k,p in {'downloads':downloads,'inventory':inventory,'order_lines':line,'file_audit':audit}.items()},outputs={})
    for name,rows in [('documents.jsonl',a[0]),('order_rows.jsonl',a[1]),('enriched_rows.jsonl',[]),('confirmations.jsonl',[])]:
        p=write('index/'+name,rows,True);idx['outputs'][name]={'sha256':m.source.sha_file(p),'rows':len(rows)}
    write('index/manifest.json',idx)
    cfg=write('config.json',{'schema_version':'garment_raw_increment_package_config/1','paths':{
        'index_dir':str(root/'index'),'acquisition_manifest':str(acq),'parse_manifest':str(parse),'fetch_plan':str(plan),
        'downloads':str(downloads),'fetch_allowlist':str(allowlist),'acquisition_inventory':str(inventory),'parse_inventory':str(inventory),'parse_actions':str(actions)}})
    before=write('before.json',{'status':'SNAPSHOT_VERIFIED','tables':{t:[] for t in m.TABLES},'metadata':metadata()})
    return cfg,before,raw


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=Path(os.environ['GARMENT_SOURCE_TEST_TMP']));self.root=Path(self.tmp.name);self.cfg,self.before,self.raw=artifact_fixture(self.root)
    def tearDown(self):self.tmp.cleanup()
    def build(self):return m.build_package(self.cfg,self.before,DDL,self.root/'output')
    def test_full_immutable_package_and_loader_readback(self):
        result=self.build();self.assertEqual(result['append_preview']['insert_count'],6);self.assertEqual(result['database_operations'],0);self.assertEqual(result['remote_operations'],0);self.assertEqual(result['quantity_reconciliation']['source_raw_quantity_sum'],'0')
    def test_refuse_output_overwrite(self):
        self.build()
        with self.assertRaises(ValueError):self.build()
    def test_source_tamper_prevents_output(self):
        self.raw.write_bytes(b'changed')
        with self.assertRaises(ValueError):self.build()
        self.assertFalse((self.root/'output').exists())
    def test_artifact_count_mismatch_rejected(self):
        p=self.root/'index/manifest.json';idx=json.loads(p.read_text());idx['outputs']['documents.jsonl']['rows']=2;p.write_text(json.dumps(idx))
        with self.assertRaises(ValueError):self.build()
    def test_extra_semantic_output_rejected(self):
        p=self.root/'index/enriched_rows.jsonl';p.write_text('{}\n');mp=self.root/'index/manifest.json';idx=json.loads(mp.read_text());idx['outputs']['enriched_rows.jsonl']={'sha256':m.source.sha_file(p),'rows':1};mp.write_text(json.dumps(idx))
        with self.assertRaises(ValueError):self.build()
    def test_index_inventory_may_differ_from_parser_with_both_pinned(self):
        other=self.root/'index_inventory.json';other.write_text('{"index_metadata":true}');mp=self.root/'index/manifest.json';idx=json.loads(mp.read_text());idx['sources']['inventory']={'path':str(other),'sha256':m.source.sha_file(other)};mp.write_text(json.dumps(idx));self.assertEqual(self.build()['append_preview']['insert_count'],6)
    def test_incomplete_parse_does_not_write_package(self):
        p=self.root/'parse_manifest.json';a=json.loads(p.read_text());a['status']='PARSED_WITH_REVIEW';p.write_text(json.dumps(a))
        with self.assertRaises(ValueError):self.build()
        self.assertFalse((self.root/'output').exists())

if __name__=='__main__':unittest.main()
