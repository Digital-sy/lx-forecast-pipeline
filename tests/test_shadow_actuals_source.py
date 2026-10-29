import copy
import datetime as dt
import gzip
import hashlib
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from scripts import shadow_actuals_source as source


class Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.run = self.root / 'run'
        (self.run / 'prepared').mkdir(parents=True)
        (self.run / 'issued').mkdir()
        self.core = pd.DataFrame([dict(shop=s, spu=p) for s in source.SHOPS for p in ['X', 'Y', 'Z']])
        self.core.to_parquet(self.run / 'prepared/core.parquet', index=False)
        self.seal = dict(status='ISSUED_SHADOW_AWAITING_ACTUALS', run_id='synthetic',
                         input_cutoff='2026-10-27', issue_date='2026-10-29',
                         windows={'W60': ['2026-10-30', '2026-12-28']},
                         files={'prepared/core.parquet': source.sha(self.run / 'prepared/core.parquet')})
        source.write_new(self.run / 'issued/issue_seal.json', self.seal)
        self.rows = []
        sids = {'JQ-US': '11548', 'MT-US': '11545', 'RKZ-US': '11550', 'SY-US': '11547'}
        for shop in source.SHOPS:
            for asin, spu, qty in [('A', 'X', 2), ('B', 'Y', 0), ('C', None, 3)]:
                self.rows.append(dict(dt='2026-10-30', store_name=shop, country='美国',
                                     asin=asin, sid=sids[shop], spu=spu, sku='NOT_USED_FOR_MAPPING',
                                     stat_date='2026-10-30', delete_flag=0, volume=qty,
                                     create_time='2026-10-31 06:00:00', modify_time='2026-10-31 06:00:00'))
        anchor = pd.DataFrame(self.rows)[source.PK + ['spu']]
        anchor['dt'] = '2026-10-27'
        self.anchor = self.root / 'anchor.parquet'
        anchor.to_parquet(self.anchor, index=False)
        self.logs = self.make_logs()
        self.calls = []
        self.result_transform = None
        self.client = self.transport

    def make_logs(self):
        events = []
        for shop in source.SHOPS:
            for marker, nums in [('开始抓取', {}), ('当前页已写入数据库', {'offset': '0', 'page_rows': '3'}),
                                 ('分片店铺抓取并写库完成', {'rows': '3'})]:
                events.append(dict(path='synthetic.log', line=len(events)+1, markers=[marker],
                                   shops=[shop], dates=['2026-10-30', '2026-10-30'],
                                   times=['2026-10-31 06:00:00'], numbers=nums))
        events.append(dict(path='synthetic.log', line=len(events)+1, markers=['全部完成'],
                           shops=[], dates=[], times=['2026-10-31 06:00:01'], numbers={}))
        return dict(files=[dict(path='synthetic.log', windows=[['2026-10-30', '2026-10-30']],
                                sha256='synthetic-log-hash', complete=True)], events=events,
                    active_known_pids=[], all_other_writers_excluded=False)

    def transport(self, request):
        self.calls.append(copy.deepcopy(request))
        result = dict(mode=request['mode'], status='OK', started_at='2026-11-01T00:00:00+00:00',
                      finished_at='2026-11-01T00:00:01+00:00', production_writes=0, api_calls=0)
        if request['mode'] == 'logs':
            result['logs'] = copy.deepcopy(self.logs)
        elif request['mode'] == 'metadata':
            result['columns'] = [dict(COLUMN_NAME=f) for f in source.FIELDS]
            result['indexes'] = []
        else:
            result.update(sql='SELECT '+','.join(source.FIELDS)+' FROM '+source.SOURCE+
                          ' FORCE INDEX(PRIMARY) WHERE dt>=%s AND dt<%s AND store_name IN (%s,%s,%s,%s) ORDER BY dt,store_name,country,asin',
                          params=[request['start'], request['end_exclusive']]+list(source.SHOPS),
                          rows=copy.deepcopy(self.rows), row_count=len(self.rows), estimated_scan=20,
                          explain={'query_block': {'table': {'access_type': 'range', 'rows_examined_per_scan': 20}}},
                          select_seconds=0.001)
        return self.result_transform(request, result) if self.result_transform else result

    def collect(self, name='out', now='2026-11-01T01:00:00+00:00', previous=None, anchor=True):
        return source.collect(self.run, self.root/name,
                              clock_provider=lambda: dt.datetime.fromisoformat(now),
                              transport=self.client, execute_read=True, previous=previous,
                              frozen_mapping=self.anchor if anchor else None,
                              frozen_mapping_sha256=source.sha(self.anchor) if anchor else None)


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.f = Fixture(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_waiting_uses_server_shanghai_date_without_transport_or_database(self):
        with patch.object(source, 'SSHProbeTransport', side_effect=AssertionError('must not initialize')):
            result = source.collect(self.f.run, self.f.root/'wait',
                                    clock_provider=lambda: dt.datetime.fromisoformat('2026-10-29T15:00:00+08:00'),
                                    execute_read=True)
        self.assertEqual(result['status'], 'WAITING_TARGET_START')
        self.assertEqual(result['database_queries'], 0)
        self.assertEqual(result['source_available_through'], '2026-10-27')
        self.assertFalse((self.f.root/'wait/source').exists())

    def test_timezone_and_two_day_chunks(self):
        _, _, contract = source.issue_contract(self.f.run)
        plan = source.plan(contract, '2026-11-04T18:00:00+00:00')
        self.assertEqual(plan['source_available_through'], '2026-11-03')
        self.assertEqual(plan['chunks'], [['2026-10-30', '2026-11-01'], ['2026-11-01', '2026-11-03'], ['2026-11-03', '2026-11-04']])
        with self.assertRaisesRegex(ValueError, 'AWARE_SERVER_CLOCK_REQUIRED'):
            source.plan(contract, '2026-11-04T18:00:00')

    def test_import_does_not_connect_or_authenticate(self):
        with patch('subprocess.run', side_effect=AssertionError('No network')):
            importlib.reload(source)

    def test_pure_adapter_exactly_replays_reviewed_original_functions(self):
        # The legacy module is imported only in this offline parity test.
        from scripts.blend50_recent_input import normalized as old_normalized, completion as old_completion
        for mutate in (False, True):
            raw = pd.DataFrame(copy.deepcopy(self.f.rows))
            if mutate:
                raw.loc[0, 'volume'] = None
                raw.loc[3, 'spu'] = 'X|Y'
                raw.loc[6, 'volume'] = -1
            pd.testing.assert_frame_equal(source.normalized(raw, 'fixture', 'fixture'),
                                          old_normalized(raw, 'fixture', 'fixture'), check_exact=True)
        for bad in (False, True):
            logs = copy.deepcopy(self.f.logs)
            if bad:
                logs['events'].pop(1)
            for shop in source.SHOPS:
                self.assertEqual(source.completion(logs, '2026-10-30', shop),
                                 old_completion(logs, '2026-10-30', shop))

    def test_default_plans_without_database(self):
        result = source.collect(self.f.run, self.f.root/'plan',
                                clock_provider=lambda: dt.datetime.fromisoformat('2026-11-01T00:00:00+00:00'),
                                transport=lambda _: self.fail('Must not request'))
        self.assertEqual(result['status'], 'READ_PLANNED_NOT_EXECUTED')

    def test_complete_source_full_shop_core_zero_and_missing_separate(self):
        result = self.f.collect()
        self.assertEqual(result['status'], 'SOURCE_READY', result)
        checked = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)
        cd, sd = checked['core_daily'], checked['shop_daily']
        self.assertEqual(sd.y.sum(), 20)
        self.assertEqual(sd.unknown_mapping_units.sum(), 12)
        self.assertEqual(cd[cd.spu.eq('Y')].sales_state.unique().tolist(), ['EXPLICIT_ZERO'])
        self.assertTrue(cd[cd.spu.eq('Y')].y.eq(0).all())
        self.assertTrue(cd[cd.spu.eq('Z')].y.isna().all())
        self.assertTrue(cd[cd.spu.eq('Z')].sales_state.eq('MISSING_RECORD_NOT_ZERO').all())
        self.assertEqual(result['core_unknown_days'], 4)

    def test_no_mapping_anchor_preserves_denominator_but_blocks_core(self):
        result = self.f.collect(anchor=False)
        self.assertEqual(result['status'], 'SOURCE_READY')
        checked = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)
        self.assertEqual(checked['shop_daily'].y.sum(), 20)
        self.assertTrue(checked['core_daily'].y.isna().all())
        self.assertTrue(checked['core_daily'].sales_state.eq('MAPPING_EVIDENCE_UNKNOWN').all())

    def test_mapping_change_flags_old_and_new_core_not_arbitrary_first(self):
        self.f.rows[0]['spu'] = 'Z'
        result = self.f.collect()
        self.assertEqual(result['status'], 'SOURCE_READY')
        d = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)['core_daily']
        both = d[d.shop.eq('JQ-US') & d.spu.isin(['X', 'Z'])]
        self.assertTrue(both.sales_state.eq('MAPPING_CHANGED_OR_NEW').all())
        self.assertTrue(both.y.isna().all())

    def test_missing_page_not_hand_pass(self):
        self.f.logs['events'].pop(1)
        result = self.f.collect()
        self.assertEqual(result['status'], 'SOURCE_INCOMPLETE')
        d = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)['core_daily']
        self.assertTrue(d[d.shop.eq('JQ-US')].y.isna().all())

    def test_before_after_log_change_blocks(self):
        def transform(req, result):
            if req['mode'] == 'logs' and len(self.f.calls) > 1:
                result['logs']['files'][0]['sha256'] = 'changed'
            return result
        self.f.result_transform = transform
        result = self.f.collect()
        self.assertEqual(result['status'], 'SOURCE_INCOMPLETE')
        d = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)['core_daily']
        self.assertTrue(d.source_state.eq('SYNC_CHANGED_DURING_READ').all())

    def test_null_and_negative_not_filled(self):
        for i, v in [(0, None), (3, -1)]:
            self.f.rows[i]['volume'] = v
        result = self.f.collect()
        self.assertEqual(result['status'], 'SOURCE_INCOMPLETE')
        d = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)['core_daily']
        self.assertTrue(d[d.shop.isin(['JQ-US', 'MT-US'])].y.isna().all())

    def test_duplicate_asin_rejected_not_drop_duplicates(self):
        self.f.rows.append(copy.deepcopy(self.f.rows[0]))
        result = self.f.collect()
        self.assertEqual(result['guard'], 'INVALID_OR_DUPLICATE_ASIN_KEY')

    def test_scope_mismatch_rejected(self):
        self.f.rows[0]['dt'] = '2026-10-29'
        result = self.f.collect()
        self.assertEqual(result['guard'], 'ROW_OUT_OF_SCOPE')

    def test_sid_not_reused_for_other_shop(self):
        self.f.rows[0]['sid'] = '11545'
        self.assertEqual(self.f.collect()['guard'], 'SHOP_SID_COUNTRY_CHANGED')

    def test_payload_tamper_rejected(self):
        self.f.collect()
        p = self.f.root/'out/source/daily_00.json.gz'
        p.write_bytes(p.read_bytes()+b'changed')
        with self.assertRaisesRegex(ValueError, 'SOURCE_PACKAGE_CHANGED'):
            source.verify_package(self.f.root/'out', 'synthetic', self.f.core)

    def test_rehashed_false_success_still_fails_evidence_derivation(self):
        self.f.logs['events'].pop(1)
        self.f.collect()
        p = self.f.root/'out'
        m = source.read(p/'manifest.json')
        m['status'] = 'SOURCE_READY'
        (p/'manifest.json').write_text(json.dumps(m), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'HAND_FILLED_SOURCE_PASS'):
            source.verify_package(p, 'synthetic', self.f.core)

    def test_rehashed_changed_truth_cannot_pass(self):
        self.f.collect()
        p = self.f.root/'out'
        d = pd.read_csv(p/'core_daily.csv')
        d.loc[0, 'y'] = 999
        d.to_csv(p/'core_daily.csv', index=False)
        m = source.read(p/'manifest.json')
        m['files']['core_daily.csv'] = source.sha(p/'core_daily.csv')
        (p/'manifest.json').write_text(json.dumps(m), encoding='utf-8')
        with self.assertRaises(AssertionError):
            source.verify_package(p, 'synthetic', self.f.core)

    def test_revision_is_replacement_and_old_version_unchanged(self):
        self.f.collect()
        old = {str(p): source.sha(p) for p in (self.f.root/'out').rglob('*') if p.is_file()}
        self.f.rows[0]['volume'] = 4
        result = self.f.collect('revision', previous=self.f.root/'out')
        self.assertEqual(result['status'], 'SOURCE_READY', result)
        self.assertEqual(result['revisions'], 1)
        d = source.verify_package(self.f.root/'revision', 'synthetic', self.f.core)['shop_daily']
        self.assertEqual(d.y.sum(), 22)
        self.assertTrue(all(source.sha(p) == h for p, h in old.items()))

    def test_refuse_overwrite_and_immutable_run_output(self):
        self.f.collect()
        with self.assertRaises(FileExistsError):
            self.f.collect()
        with self.assertRaisesRegex(ValueError, 'OUTPUT_INSIDE_IMMUTABLE_RUN'):
            source.collect(self.f.run, self.f.run/'new',
                           clock_provider=lambda: dt.datetime.fromisoformat('2026-10-29T00:00:00+00:00'))

    def test_missing_receipt_not_replaced_by_pass(self):
        self.f.collect()
        p = self.f.root/'out'
        m = source.read(p/'manifest.json')
        del m['files']['source/logs_before.receipt.json']
        (p/'manifest.json').write_text(json.dumps(m), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'SOURCE_EVIDENCE_MISSING'):
            source.verify_package(p, 'synthetic', self.f.core)

    def test_new_asin_and_mapping_multivalue_not_expanded(self):
        self.f.rows[0]['asin'] = 'NEW'
        self.f.rows[0]['spu'] = 'X|Y'
        result = self.f.collect()
        self.assertEqual(result['raw_rows'], 12)
        d = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)['shop_daily']
        self.assertEqual(d.y.sum(), 20)
        self.assertEqual(d.unknown_mapping_units.sum(), 14)

    def test_sql_parameter_mutation_blocks(self):
        def transform(req, response):
            if req['mode'] == 'data':
                response['params'][2] = 'OTHER'
            return response
        self.f.result_transform = transform
        self.assertEqual(self.f.collect()['guard'], 'QUERY_PARAMETER_MISMATCH')

    def test_all_scan_even_with_small_row_estimate_is_rejected(self):
        def transform(req, response):
            if req['mode'] == 'data':
                response['explain']['query_block']['table']['access_type'] = 'ALL'
            return response
        self.f.result_transform = transform
        self.assertEqual(self.f.collect()['guard'], 'UNBOUNDED_EXECUTION_PLAN')

    def test_full_zero_shop_snapshot_does_not_zero_absent_core(self):
        self.f.rows = [row for row in self.f.rows if row['store_name'] != 'JQ-US']
        for e in self.f.logs['events']:
            if e['shops'] == ['JQ-US']:
                if 'page_rows' in e['numbers']:
                    e['numbers']['page_rows'] = '0'
                if 'rows' in e['numbers']:
                    e['numbers']['rows'] = '0'
        self.assertEqual(self.f.collect()['status'], 'SOURCE_READY')
        result = source.verify_package(self.f.root/'out', 'synthetic', self.f.core)
        store = result['shop_daily'][result['shop_daily'].shop.eq('JQ-US')].iloc[0]
        self.assertEqual(store.y, 0)
        self.assertEqual(store.sales_state, 'EXPLICIT_ZERO')
        self.assertTrue(result['core_daily'].loc[result['core_daily'].shop.eq('JQ-US'), 'y'].isna().all())

    def test_binding_to_same_named_changed_issue_rejected(self):
        self.f.collect()
        _, _, contract = source.issue_contract(self.f.run)
        contract['core_sha256'] = 'changed'
        with self.assertRaisesRegex(ValueError, 'SOURCE_ISSUE_BINDING_MISMATCH'):
            source.verify_package(self.f.root/'out', 'synthetic', self.f.core, contract)

    def test_future_anchor_rejected(self):
        anchor = pd.read_parquet(self.f.anchor)
        anchor['dt'] = '2026-10-28'
        anchor.to_parquet(self.f.anchor, index=False)
        self.assertEqual(self.f.collect()['guard'], 'MAPPING_ANCHOR_AFTER_FORECAST_CUTOFF')

    def test_false_zero_and_infinite_actual_rejected_even_if_rehashed(self):
        for i, value in enumerate([float('inf'), 0]):
            name = 'out'+str(i)
            self.f.collect(name)
            p = self.f.root/name
            d = pd.read_csv(p/'core_daily.csv')
            d.loc[0, 'y'] = value
            d.to_csv(p/'core_daily.csv', index=False)
            m = source.read(p/'manifest.json')
            m['files']['core_daily.csv'] = source.sha(p/'core_daily.csv')
            (p/'manifest.json').write_text(json.dumps(m), encoding='utf-8')
            with self.assertRaises(ValueError):
                source.verify_package(p, 'synthetic', self.f.core)

    def test_probe_failure_not_retried_or_published(self):
        def transform(req, response):
            if req['mode'] == 'data':
                response['status'] = 'ERROR'
            return response
        self.f.result_transform = transform
        result = self.f.collect()
        self.assertEqual(result['guard'], 'SOURCE_PROBE_FAILED')
        self.assertEqual(sum(r['mode'] == 'data' for r in self.f.calls), 1)
        self.assertFalse((self.f.root/'out/core_daily.csv').exists())


if __name__ == '__main__':
    unittest.main()
