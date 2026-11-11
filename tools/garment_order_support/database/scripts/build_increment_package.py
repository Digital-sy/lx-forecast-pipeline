"""Build a RAW_ONLY append package from explicit frozen artifacts, without network or DB I/O."""
from __future__ import annotations
import argparse
import copy
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import build_load_package as source
import mysql_load_core as loader
import append_load_core as append

VERSION = '1.0.0-public1'
TABLES = source.TABLES
DEPENDENCY_HASHES = {
 'build_load_package.py':'9da9bc33f0186e46ec428e1ebd2df968f40abeef43128f974603417344636258',
 'mysql_load_core.py':'9c2878ad8e7c2d3e5d91d9132e824bec54c2ba23e32654a27c96149c97358418',
 'append_load_core.py':'196b7d4b8c805bd14dca7531be0c26651853c187697b61abe2e251db5e3b594a',
}
require = source.require


def business_existing(snapshot):
    """Decode DB JSON exactly once, preserving JSON scalar/null versus SQL NULL."""
    require(snapshot.get('status') == 'SNAPSHOT_VERIFIED', 'BASELINE_NOT_VERIFIED_SNAPSHOT')
    require(set(snapshot['tables']) == set(TABLES), 'BASELINE_TABLE_SCOPE')
    meta = snapshot['metadata']
    normalized = append.normalize_snapshot(snapshot['tables'], meta)
    result = {t:{} for t in TABLES}
    for table in TABLES:
        columns = {c['COLUMN_NAME']:c for c in meta[table]}
        for row in snapshot['tables'][table]:
            out = {}
            for name, value in row.items():
                if name in append.AUTO_TIME:
                    continue
                dtype = columns[name]['DATA_TYPE']
                if value is not None and dtype == 'json':
                    require(isinstance(value,str), 'BASELINE_JSON_MUST_BE_DATABASE_ENCODING')
                    value = json.loads(value)
                elif value is not None and dtype == 'decimal':
                    value = format(Decimal(str(value)), 'f')
                elif value is not None and dtype == 'datetime':
                    value = datetime.fromisoformat(str(value)).strftime('%Y-%m-%d %H:%M:%S.%f')
                out[name] = value
            result[table][out[loader.PK[table]]] = out
    return result, normalized


def preserve_document_versions(package, existing):
    """Preserve pre-existing immutable identities and version metadata; never update them."""
    result = copy.deepcopy(package)
    require(all(not result[t] for t in TABLES[6:]), 'SEMANTIC_OR_MANUAL_ROWS_OUT_OF_SCOPE')
    checks = []
    doc_natural = {}
    for row in existing[TABLES[1]].values():
        key = tuple(row[n] for n in ('source_system','resource_type','source_id'))
        require(key not in doc_natural, 'BASELINE_DUPLICATE_DOCUMENT_NATURAL_KEY')
        doc_natural[key] = row['document_id']
    ver_natural = {}
    for row in existing[TABLES[2]].values():
        key = (row['document_id'],row['content_sha256'])
        require(key not in ver_natural, 'BASELINE_DUPLICATE_VERSION_NATURAL_KEY')
        ver_natural[key] = row['version_id']
    for table,pk,identity,natural in (
      (TABLES[1],'document_id',('source_system','resource_type','source_id'),doc_natural),
      (TABLES[2],'version_id',('document_id','content_sha256'),ver_natural)):
        for i, row in enumerate(result[table]):
            key = tuple(row[n] for n in identity)
            if key in natural:
                require(natural[key] == row[pk], 'EXISTING_NATURAL_ID_CONFLICT:'+table)
            previous = existing[table].get(row[pk])
            if previous is not None:
                require(tuple(previous[n] for n in identity) == key, 'EXISTING_ID_CONTENT_CONFLICT:'+table)
                require(previous['delete_flag'] == 0, 'EXISTING_ID_IS_DELETED:'+table)
                if table == TABLES[2]:
                    require(previous['byte_count'] == row['byte_count'], 'EXISTING_VERSION_BYTE_COUNT_CONFLICT')
                    require(previous['file_format'] == row['file_format'], 'EXISTING_VERSION_FORMAT_CONFLICT')
                result[table][i] = copy.deepcopy(previous)
                checks.append({'table':table,'primary_key':row[pk], 'action':'REUSE_EXACT_EXISTING_BUSINESS_ROW'})
    return result, checks


def build_package(config_path, before_path, ddl_path, output):
    config_path,before_path,ddl_path,output = map(Path,(config_path,before_path,ddl_path,output))
    require(not output.exists(), 'OUTPUT_EXISTS_REFUSE_OVERWRITE')
    tracked = {}
    def verify(path, expected=None):
        path = Path(path).resolve()
        require(path.is_file(), 'MISSING_INPUT:'+path.name)
        observed = source.sha_file(path)
        require(expected is None or observed == expected, 'HASH_MISMATCH:'+path.name)
        if str(path) in tracked:
            require(tracked[str(path)] == observed, 'INPUT_CHANGED_DURING_READ:'+path.name)
        tracked[str(path)] = observed
        return observed
    for name,expected in DEPENDENCY_HASHES.items():
        verify(Path(__file__).parent/name,expected)
    verify(__file__)
    verify(config_path)
    config = source.load_json(config_path)
    require(config.get('schema_version') == 'garment_raw_increment_package_config/1','CONFIG_SCHEMA_VERSION')
    required = {'index_dir','acquisition_manifest','fetch_plan','parse_manifest','downloads',
                'fetch_allowlist','acquisition_inventory','parse_inventory','parse_actions'}
    require(set(config.get('paths',{})) == required, 'EXPLICIT_PATH_SET_MISMATCH')
    paths = {k:Path(v).resolve() for k,v in config['paths'].items()}
    index_dir = paths.pop('index_dir')
    paths['index_manifest'] = index_dir/'manifest.json'
    paths['download_manifest'] = paths.pop('acquisition_manifest')
    verify(ddl_path,source.DDL_SHA256)
    schema = source.parse_schema(ddl_path.read_text(encoding='utf-8-sig'))
    verify(before_path)
    snapshot = source.load_json(before_path)
    existing,before = business_existing(snapshot)
    for key in ('index_manifest','download_manifest','fetch_plan','parse_manifest'):
        verify(paths[key])
    index,acquisition,plan,parser = (source.load_json(paths[k]) for k in
                                  ('index_manifest','download_manifest','fetch_plan','parse_manifest'))
    require(index.get('status') == 'LOCAL_INDEX_COMPLETE', 'INDEX_NOT_COMPLETE')
    require(index.get('raw_files_hash_checked') is True, 'INDEX_RAW_HASH_UNVERIFIED')
    require(set(index['outputs']) == {'documents.jsonl','order_rows.jsonl','enriched_rows.jsonl','confirmations.jsonl'},'INDEX_OUTPUT_SCOPE')
    verify(index['config']['path'], index['config']['sha256'])
    for spec in index['sources'].values():
        verify(spec['path'],spec['sha256'])
    for name,spec in index['outputs'].items():
        verify(index_dir/name,spec['sha256'])
        require(len(source.load_jsonl(index_dir/name)) == spec['rows'],'INDEX_OUTPUT_ROW_COUNT:'+name)
    require(not source.load_jsonl(index_dir/'enriched_rows.jsonl') and not source.load_jsonl(index_dir/'confirmations.jsonl'), 'RAW_ONLY_INDEX_REQUIRED')
    require(Path(index['sources']['downloads']['path']).resolve() == paths['downloads'], 'INDEX_DOWNLOAD_SOURCE_MISMATCH')
    paths['order_lines'] = Path(index['sources']['order_lines']['path']).resolve()
    verify(paths['downloads'],acquisition['downloads_sha256'])
    verify(paths['downloads'],parser['downloads_sha256'])
    verify(paths['fetch_allowlist'],acquisition['allowlist_sha256'])
    verify(paths['acquisition_inventory'],acquisition['inventory_sha256'])
    verify(paths['parse_actions'],parser['plan_sha256'])
    verify(paths['parse_inventory'],parser['inventory_sha256'])
    verify(paths['order_lines'],parser['output_sha256']['order_lines.jsonl'])
    verify(index['sources']['file_audit']['path'],parser['output_sha256']['order_file_audit.jsonl'])
    verify(parser['parser_path'],parser['parser_sha256'])
    verify(parser['helper_path'],parser['helper_sha256'])
    downloads = source.load_jsonl(paths['downloads'])
    require(set(parser['source_manifest']) == {r['token'] for r in downloads}, 'PARSER_SOURCE_SCOPE_MISMATCH')
    for dl in downloads:
        verify(dl['local_path'],dl['sha256'])
        require(Path(dl['local_path']).stat().st_size == dl['bytes'], 'RAW_BYTE_COUNT_MISMATCH')
        require(parser['source_manifest'][dl['token']]['sha256'] == dl['sha256'], 'PARSER_SOURCE_HASH_MISMATCH')
    documents = source.load_jsonl(index_dir/'documents.jsonl')
    rows = source.load_jsonl(index_dir/'order_rows.jsonl')
    hashes = {key:tracked[str(path)] for key,path in paths.items() if str(path) in tracked}
    converted = source.convert_data(documents,rows,downloads,plan,acquisition,parser,index,hashes,{k:str(v) for k,v in paths.items()})
    package,reused = preserve_document_versions(converted,existing)
    source.validate_rows(package,schema)
    prepared = append.prepare_plan(package,snapshot['metadata'],before)
    require(all(not prepared['inserts'][t] for t in TABLES[6:]), 'MANUAL_OR_SEMANTIC_INSERT_UNEXPECTED')
    source_sum = sum((Decimal(str(r['raw']['quantity'])) for r in rows if r['raw'].get('quantity') is not None),Decimal(0))
    package_sum = sum((Decimal(r['quantity']) for r in package[TABLES[5]] if r['quantity'] is not None),Decimal(0))
    require(source_sum == package_sum,'RAW_QUANTITY_NOT_CONSERVED')
    for path,expected in tracked.items():
        require(source.sha_file(path) == expected,'INPUT_CHANGED_BEFORE_WRITE:'+Path(path).name)
    output.mkdir(parents=True,exist_ok=False)
    table_files = {}
    for table,data in package.items():
        path = output/(table+'.jsonl')
        path.write_text(''.join(source.canonical(r)+'\n' for r in data),encoding='utf-8',newline='\n')
        table_files[table] = {'file':path.name,'row_count':len(data),'sha256':source.sha_file(path),
            'primary_key':schema[table]['primary_key'], 'columns':schema[table]['columns'],
            'json_columns':[k for k,v in schema[table]['columns'].items() if v['base_type']=='JSON']}
    manifest = {
      'schema_version':'garment_source_load_package/1','builder_version':VERSION,
      'built_at_utc':datetime.now(timezone.utc).isoformat(),'status':'LOCAL_PACKAGE_VALIDATED_NOT_IMPORTED',
      'target_schema':'lingxing','ddl_sha256':source.DDL_SHA256,'builder_sha256':source.sha_file(__file__),
      'scope':'INCREMENT_RAW_EVIDENCE_ONLY','table_order':list(TABLES),'tables':table_files,
      'total_rows':sum(len(v) for v in package.values()),'source_files':tracked,
      'production_before':{'path':str(before_path.resolve()),'sha256':tracked[str(before_path.resolve())],
        'status':snapshot['status'],'normalized_sha256':loader.sha(before)},
      'reuse_exact_existing':reused,
      'append_preview':{'insert_counts':{t:len(v) for t,v in prepared['inserts'].items()},
        'skip_counts':prepared['skipped'],'insert_count':prepared['insert_count'],
        'expected_after_counts':{t:len(v) for t,v in prepared['merged'].items()}},
      'quantity_reconciliation':{'source_raw_quantity_sum':str(source_sum),'package_raw_quantity_sum':str(package_sum),
        'meaning':'Parsed evidence only; not deduplicated business demand or approved order quantities'},
      'load_contract':{'mode':'INSERT_ONLY_COMPARE_EXISTING','on_existing_identical':'SKIP','on_existing_different':'FAIL_NO_UPDATE',
        'database_default_columns':['create_time','update_time'],'json_values':'Encode native JSON exactly once',
        'decimal_values':'Bind Decimal strings without float conversion','utc_connection_required':True,'foreign_keys_required':True,'no_disable_checks':True},
      'limitations':source.LIMITATIONS+['Previously stored documents and byte-identical versions are reused exactly; fresh observation retains the new acquisition receipt.',
        'All prior material interpretations and manual confirmation rows remain outside this package.'],
      'database_operations':0,'remote_operations':0}
    (output/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    # Independently read back package framing/hashes using the established loader, without a DB.
    loaded,loaded_manifest = loader.load_package(output)
    require(loaded == package,'PACKAGE_FILE_READBACK_MISMATCH')
    return manifest


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--production-before',required=True)
    parser.add_argument('--ddl',required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args(argv)
    result=build_package(args.config,args.production_before,args.ddl,args.output)
    print(json.dumps({k:result[k] for k in ('status','total_rows','append_preview','database_operations','remote_operations')},ensure_ascii=False))
    return 0

if __name__=='__main__':
    raise SystemExit(main())
