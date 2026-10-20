"""Read-only comparison of a later local overlap extract against frozen V1.

Writes a new audit directory only; never merges revisions, queries a database,
or fits a model. Query receipts and hashes are checked before comparison.
"""
from __future__ import annotations
import argparse
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import re

import pandas as pd

try:
    from scripts import bc60_monthly_inputs as rules
except ModuleNotFoundError:
    import bc60_monthly_inputs as rules

KEYS = ['ds','shop','spu']


def compare(old, new):
    if old.duplicated(KEYS).any() or new.duplicated(KEYS).any():
        raise ValueError('OVERLAP_GRAIN_NOT_UNIQUE')
    common = [column for column in old if column not in KEYS and column in new]
    joined = old.merge(new[KEYS+common],on=KEYS,how='outer',indicator=True,
                       suffixes=('_v1','_new'),validate='one_to_one')
    both = joined._merge.eq('both')
    differences, fields = [], []
    for field in common:
        left,right = joined[field+'_v1'],joined[field+'_new']
        equal = left.eq(right).fillna(False) | (left.isna() & right.isna())
        changed = both & ~equal
        fields.append({'field':field,'compared_rows':int(both.sum()),'changed_rows':int(changed.sum()),
                       'v1_nulls':int(left[both].isna().sum()),'new_nulls':int(right[both].isna().sum())})
        for day,shop,spu,a,b in joined.loc[changed,KEYS+[field+'_v1',field+'_new']].itertuples(index=False,name=None):
            differences.append({'ds':str(day.date()),'shop':shop,'spu':spu,'field':field,
                                'old_value':a,'new_value':b,'state':'CHANGED_EXISTING_KEY'})
    for day,shop,spu,state in joined.loc[~both,KEYS+['_merge']].itertuples(index=False,name=None):
        differences.append({'ds':str(day.date()),'shop':shop,'spu':spu,'field':'__ROW_PRESENCE__',
                            'old_value':'PRESENT' if state=='left_only' else None,
                            'new_value':'PRESENT' if state=='right_only' else None,'state':str(state)})
    def total(frame):
        value=frame.y.sum(min_count=1)
        return None if pd.isna(value) else float(value)
    summary={'old_rows':len(old),'new_rows':len(new),'common_rows':int(both.sum()),
        'only_v1_rows':int(joined._merge.eq('left_only').sum()),'only_new_rows':int(joined._merge.eq('right_only').sum()),
        'field_checks':fields,'difference_cells':len(differences),
        'y_units_v1_observed_aggregate':total(old),'y_units_new_observed_aggregate':total(new),
        'extra_new_columns_not_comparable':sorted(set(new)-set(old))}
    return summary,pd.DataFrame(differences,columns=['ds','shop','spu','field','old_value','new_value','state'])


def run(root,v1,first,last,out,v1_sha=rules.DAILY_SHA):
    root,v1,out=map(lambda p:Path(p).resolve(),(root,v1,out))
    if out.exists():
        raise ValueError('OUTPUT_ALREADY_EXISTS')
    first,last=pd.Timestamp(first),pd.Timestamp(last)
    if first>last or first!=first.normalize() or last!=last.normalize():
        raise ValueError('INVALID_BUSINESS_RANGE')
    stem=f'overlap_revision_{first.date()}_{last.date()}'
    raw=root/'raw'/f'{stem}.json.gz'
    evidence=root/'evidence'/f'{stem}.json'
    old_path=v1/'daily.parquet'
    source=Path(rules.__file__).resolve()
    originals={str(p):rules.sha(p) for p in [old_path,raw,evidence,source,Path(__file__).resolve()]}
    if originals[str(old_path)]!=v1_sha:
        raise ValueError('V1_FROZEN_HASH_MISMATCH')
    receipt=json.loads(evidence.read_text(encoding='utf-8-sig'))
    payload=gzip.decompress(raw.read_bytes())
    if hashlib.sha256(payload).hexdigest()!=receipt['rows_sha256']:
        raise ValueError('OVERLAP_RAW_HASH_MISMATCH')
    if receipt['status']!='OK' or receipt['mode']!='select':
        raise ValueError('OVERLAP_READ_NOT_SUCCESSFUL')
    acquired=receipt.get('acquired_at')
    if not acquired or dt.datetime.fromisoformat(acquired.replace('Z','+00:00')).tzinfo is None:
        raise ValueError('ACQUIRED_AT_REQUIRES_TIMEZONE')
    rows=json.loads(payload)
    if not isinstance(rows,list) or len(rows)!=receipt['row_count']:
        raise ValueError('OVERLAP_ROW_COUNT_MISMATCH')
    sql=receipt['request']['sql']
    if hashlib.sha256(sql.encode()).hexdigest()!=receipt['sql_sha256']:
        raise ValueError('OVERLAP_SQL_HASH_MISMATCH')
    clause=re.search(r'\bdt\s+IN\s*\(([^)]*)\)',sql,flags=re.I)
    expected={str(day.date()) for day in pd.date_range(first,last)}
    if clause is None or set(re.findall(r"'(\d{4}-\d{2}-\d{2})'",clause.group(1)))!=expected:
        raise ValueError('RECEIPT_DATE_RANGE_MISMATCH')
    new,normalization=rules.normalize(rows)
    if not new.ds.between(first,last).all():
        raise ValueError('OVERLAP_OUTSIDE_APPROVED_DATES')
    old=pd.read_parquet(old_path,filters=[('ds','>=',first),('ds','<=',last)])
    result,differences=compare(old,new)
    if any(rules.sha(path)!=digest for path,digest in originals.items()):
        raise ValueError('SOURCE_CHANGED_DURING_OVERLAP_AUDIT')
    out.mkdir(parents=True)
    csv=out/'overlap_revision_differences.csv'
    differences.to_csv(csv,index=False,encoding='utf-8-sig')
    result.update(status='COMPARED_NO_INPUT_REVISION',completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        business_first=str(first.date()),business_last=str(last.date()),acquired_at=acquired,source_sync_at=None,
        source_sync_note='max_sync is MAX(modify_time), not a successful sync task timestamp',
        normalization=normalization,v1_unchanged=True,merged_into_experiment_inputs=False,
        pit_limitation='Later reads reveal current-source revision only, not historical point-in-time availability.',
        sources=originals,difference_csv_sha256=rules.sha(csv),new_queries=0,new_fits=0)
    rules.dump(out/'overlap_revision_audit.json',result)
    return {key:value for key,value in result.items() if key not in ('sources','field_checks','normalization')}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('root','v1','out'):
        parser.add_argument('--'+name,type=Path,required=True)
    for name in ('first','last'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--v1-sha',default=rules.DAILY_SHA)
    print(json.dumps(run(**vars(parser.parse_args())),ensure_ascii=False,indent=2))
