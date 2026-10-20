"""Synthetic adapter tests only; no business replay or database access."""
import datetime as dt
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('native_extension',ROOT/'scripts/bc60_native_v4_extension.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class NativeExtensionTests(unittest.TestCase):
    def archive(self, sales):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name)/'input.jsonl'
        rows = [{'kind':'seasons','value':[{'SPU':'S','季节':'全年','同步时间':'2026-07-03'}]}]
        rows += [{'kind':'sales','value':row} for row in sales]
        path.write_text('\n'.join(json.dumps(r) for r in rows),encoding='utf-8')
        return path

    def record(self, sku='S-A', spu='S', qty=3, month='2024-01-01'):
        return {'SKU':sku,'SPU':spu,'店铺':'JQ-US','销量':qty,'统计日期':month}

    def test_scope_disjoint_and_fixed(self):
        self.assertEqual(len(module.MONTHS),12)
        self.assertFalse(set(module.MONTHS)&set(module.EXISTING))
        self.assertEqual(module.MONTHS[-1],'2026-06')

    def test_archive_aggregates_source_sku_rows_not_spu_reconstruction(self):
        path=self.archive([self.record(qty=3),self.record(qty=4),self.record('S-B',qty=0)])
        data,first,source,seasons,quality,_=module.read_archive(path,lambda sku:'S')
        self.assertEqual(len(data['JQ-US']),2)
        self.assertEqual(data['JQ-US']['S-A','S'][dt.date(2024,1,1)],7)
        self.assertEqual(quality['explicit_zero_rows'],1)
        self.assertEqual(first['JQ-US','S'],dt.date(2024,1,1))
        self.assertEqual(seasons,{'S':'全年'})

    def test_null_sales_not_zero(self):
        with self.assertRaisesRegex(ValueError,'UNKNOWN'):
            module.read_archive(self.archive([self.record(qty=None)]),lambda sku:'S')

    def test_conflicting_source_spu_stops(self):
        with self.assertRaisesRegex(ValueError,'MAPPING_CONFLICT'):
            module.read_archive(self.archive([self.record(),self.record(spu='Q')]),lambda sku:'S')

    def test_full_sku_context_and_strict_month_cutoff(self):
        data={'JQ-US':{('S-A','S'):{dt.date(2024,1,1):2,dt.date(2024,7,1):10000},
                         ('S-B','S'):{dt.date(2024,1,1):5},
                         ('Q-A','Q'):{dt.date(2024,1,1):3}}}
        first={('JQ-US','S'):dt.date(2024,1,1),('JQ-US','Q'):dt.date(2024,1,1)}
        calls=[]
        def predict(history,labels,cutoff,spu_season_map):
            calls.append((history,labels,cutoff,spu_season_map))
            return {sku:{label:len(history) for label in labels} for sku in history}
        rows,_=module.replay_target(data,first,{'S':'全年'},dt.date(2024,9,1),2,predict)
        history,labels,cutoff,_=calls[0]
        self.assertEqual(set(history),{'S-A','S-B','Q-A'})
        self.assertNotIn('24年7月销量',history['S-A'])
        self.assertEqual(labels,['24年7月预计销量','24年8月预计销量','24年9月预计销量'])
        self.assertEqual(next(r for r in rows if r['spu']=='S')['prediction'],6)
        self.assertIsNone(rows[0]['actual'])

    def test_missing_prediction_not_zero(self):
        data={'JQ-US':{('S-A','S'):{dt.date(2024,1,1):2},('S-B','S'):{dt.date(2024,1,1):3}}}
        def predict(history,labels,cutoff,spu_season_map):
            return {'S-A':{label:3 for label in labels}}
        rows,_=module.replay_target(data,{('JQ-US','S'):dt.date(2024,1,1)}, {},dt.date(2024,9,1),2,predict)
        self.assertIsNone(rows[0]['prediction'])
        self.assertEqual(rows[0]['prediction_status'],'INCOMPLETE_SKU_OUTPUT')

    def test_actual_zero_distinct_from_unknown(self):
        data={'JQ-US':{('S-A','S'):{dt.date(2024,1,1):2,dt.date(2024,9,1):0}}}
        def predict(history,labels,cutoff,spu_season_map):
            return {'S-A':{label:3 for label in labels}}
        rows,_=module.replay_target(data,{('JQ-US','S'):dt.date(2024,1,1)}, {},dt.date(2024,9,1),2,predict)
        self.assertEqual(rows[0]['actual'],0)
        self.assertEqual(rows[0]['truth_status'],'EXPLICIT_ZERO')
        self.assertIsNone(rows[0]['hit20'])

    def test_frozen_pure_loader_has_no_database_or_logger(self):
        predict=module.pure_v4(ROOT/'jobs/feishu/forecast_sales_improved.py')
        self.assertNotIn('db_cursor',predict.__globals__)
        self.assertNotIn('logger',predict.__globals__)
        self.assertNotIn('load_spu_season_map',predict.__globals__)
        result=predict({'S-A':{'SPU':'S','24年1月销量':10,'24年2月销量':12}},
                       ['24年4月预计销量'],dt.datetime(2024,3,1),spu_season_map={'S':'全年'})
        self.assertIsInstance(result['S-A']['24年4月预计销量'],int)


if __name__ == '__main__':
    unittest.main()
