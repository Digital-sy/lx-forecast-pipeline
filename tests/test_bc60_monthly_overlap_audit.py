"""Synthetic-only overlap audit tests, not new source acquisition."""
import datetime as dt
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
spec=importlib.util.spec_from_file_location('overlap',ROOT/'scripts/bc60_monthly_overlap_audit.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)


class OverlapTests(unittest.TestCase):
    def frame(self,values):
        return pd.DataFrame([{'ds':pd.Timestamp('2024-01-01'),'shop':'JQ-US','spu':spu,'y':y} for spu,y in values])

    def test_null_not_equal_zero_and_no_merge(self):
        old=self.frame([('A',None),('B',0)]);new=self.frame([('A',0),('B',0)])
        summary,diff=mod.compare(old,new)
        self.assertEqual(summary['difference_cells'],1)
        self.assertEqual(diff.iloc[0].spu,'A')
        self.assertTrue(pd.isna(old.iloc[0].y))

    def test_unknown_unknown_equal_and_sum_stays_unknown(self):
        frame=self.frame([('A',None)])
        result,diff=mod.compare(frame,frame.copy())
        self.assertTrue(diff.empty)
        self.assertIsNone(result['y_units_v1_observed_aggregate'])

    def test_outer_grain_and_additional_fields(self):
        old=self.frame([('A',1),('B',2)]);new=self.frame([('A',1),('C',3)]);new['extra']=8
        result,diff=mod.compare(old,new)
        self.assertEqual(result['only_v1_rows'],1)
        self.assertEqual(result['only_new_rows'],1)
        self.assertEqual(result['extra_new_columns_not_comparable'],['extra'])
        self.assertEqual(len(diff),2)

    def test_duplicate_grain_stops(self):
        frame=self.frame([('A',1),('A',2)])
        with self.assertRaisesRegex(ValueError,'GRAIN_NOT_UNIQUE'):
            mod.compare(frame,frame)

    def test_cli_pipeline_preserves_sources_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);(root/'raw').mkdir();(root/'evidence').mkdir();(root/'v1').mkdir()
            rows=[dict(dt='2024-01-01',store_name='JQ-US',spu='A',sales_qty=2,null_qty=0,negative_qty=0,
                date_mismatch=0,deleted_rows=0,source_rows=1,ad_spend=0,ad_clicks=0,ad_orders=0,
                listing_sessions_sum=1,order_qty=1)]
            frame,_=mod.rules.normalize(rows);old=root/'v1/daily.parquet';frame.to_parquet(old,index=False)
            payload=json.dumps(rows).encode();stem='overlap_revision_2024-01-01_2024-01-01'
            (root/'raw'/f'{stem}.json.gz').write_bytes(gzip.compress(payload))
            sql="SELECT bounded FROM source WHERE dt IN ('2024-01-01')"
            receipt=dict(status='OK',mode='select',acquired_at='2026-10-20T00:00:00+00:00',row_count=1,
                rows_sha256=hashlib.sha256(payload).hexdigest(),request={'sql':sql},sql_sha256=hashlib.sha256(sql.encode()).hexdigest())
            (root/'evidence'/f'{stem}.json').write_text(json.dumps(receipt),encoding='utf-8')
            digest=mod.rules.sha(old)
            result=mod.run(root,root/'v1','2024-01-01','2024-01-01',root/'result',digest)
            self.assertEqual(result['difference_cells'],0)
            self.assertEqual(mod.rules.sha(old),digest)
            self.assertFalse(result['merged_into_experiment_inputs'])
            with self.assertRaisesRegex(ValueError,'OUTPUT_ALREADY_EXISTS'):
                mod.run(root,root/'v1','2024-01-01','2024-01-01',root/'result',digest)


if __name__=='__main__':unittest.main()
