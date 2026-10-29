"""Independent synthetic tests: no real actuals, connection, model, or forecast writes."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from scripts import shadow_actuals_monitor as monitor


SHOPS = ['JQ-US', 'MT-US', 'RKZ-US', 'SY-US']
START = pd.Timestamp('2026-10-30')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save_json(path, data):
    Path(path).write_text(json.dumps(data, allow_nan=False), encoding='utf-8')


def fake_run(root):
    root.mkdir()
    (root / 'prepared').mkdir()
    (root / 'issued').mkdir()
    core = pd.DataFrame({'shop': SHOPS, 'spu': ['A', 'B', 'C', 'D']})
    decline = core.assign(decline_group=['DECLINING', 'OTHER', 'UNKNOWN', 'OTHER'])
    windows = {'W1': ['2026-10-30', '2026-11-28'],
               'W2': ['2026-11-29', '2026-12-28'],
               'W60': ['2026-10-30', '2026-12-28']}
    rows = []
    for obj in core.itertuples(index=False):
        for model in [monitor.CANDIDATE, 'F28']:
            missing = obj.spu == 'D' and model == monitor.CANDIDATE
            for window, name, value in [('d01_30', 'W1', 30.0), ('d31_60', 'W2', 60.0), ('d01_60', 'W60', 90.0)]:
                rows.append(dict(run_id='SYNTHETIC_RUN', issue_date='2026-10-29',
                                 input_cutoff='2026-10-27', shop=obj.shop, spu=obj.spu,
                                 model=model, window=window,
                                 target_start=windows[name][0], target_end=windows[name][1],
                                 prediction=np.nan if missing else value,
                                 prediction_status='MISSING_QUALIFICATION' if missing else 'OK'))
    pred = pd.DataFrame(rows)
    core.to_parquet(root / 'prepared/core.parquet', index=False)
    decline.to_parquet(root / 'prepared/decline.parquet', index=False)
    pred.to_parquet(root / 'issued/predictions.parquet', index=False)
    seal = dict(status='ISSUED_SHADOW_AWAITING_ACTUALS', run_id='SYNTHETIC_RUN',
                issue_date='2026-10-29', input_cutoff='2026-10-27', windows=windows,
                files={name: digest(root / name) for name in ['prepared/core.parquet',
                       'prepared/decline.parquet', 'issued/predictions.parquet']})
    save_json(root / 'issued/issue_seal.json', seal)
    return seal, core, pred, decline


def synthetic_daily(core, days=60, value=2.0):
    rows, stores = [], []
    for day in pd.date_range(START, periods=days):
        for shop, spu in core.itertuples(index=False, name=None):
            rows.append(dict(ds=str(day.date()), shop=shop, spu=spu, y=value,
                             sales_state='EXPLICIT_ZERO' if value == 0 else 'OBSERVED',
                             source_state='SOURCE_READY'))
            stores.append(dict(ds=str(day.date()), shop=shop, y=value + 3,
                               source_state='SOURCE_READY'))
    return pd.DataFrame(rows), pd.DataFrame(stores)


class IndependentMonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='shadow_monitor_test_')
        self.base = Path(self.tmp.name)
        self.run = self.base / 'run'
        self.seal, self.core, self.pred, self.decline = fake_run(self.run)

    def tearDown(self):
        self.tmp.cleanup()

    def resign_synthetic_prediction(self, pred):
        pred.to_parquet(self.run / 'issued/predictions.parquet', index=False)
        self.seal['files']['issued/predictions.parquet'] = digest(self.run / 'issued/predictions.parquet')
        save_json(self.run / 'issued/issue_seal.json', self.seal)

    def test_load_retains_all_core_and_missing_output(self):
        seal, core, pred, decline = monitor.load_run(self.run)
        self.assertEqual(len(core), 4)
        self.assertEqual(len(pred), 24)
        self.assertEqual(len(monitor.fixed_paired(pred)), 3)
        self.assertEqual(set(decline.spu), set(core.spu))

    def test_changed_sealed_prediction_is_rejected(self):
        with (self.run / 'issued/predictions.parquet').open('ab') as stream:
            stream.write(b'changed')
        with self.assertRaisesRegex(ValueError, 'SEALED_FILE_CHANGED'):
            monitor.load_run(self.run)

    def test_whole_missing_model_object_not_removed_from_core(self):
        self.resign_synthetic_prediction(self.pred[~self.pred.spu.eq('A')])
        with self.assertRaisesRegex(ValueError, 'PREDICTION_SCOPE'):
            monitor.load_run(self.run)

    def test_window_or_cumulative_changes_rejected(self):
        p = self.pred.copy()
        p.loc[p.window.eq('d01_60') & p.prediction.notna(), 'prediction'] += 1
        self.resign_synthetic_prediction(p)
        with self.assertRaisesRegex(ValueError, 'CUMULATIVE_CHANGED'):
            monitor.load_run(self.run)

    def test_waiting_skips_source_scoring_and_preserves_run(self):
        before = {str(p.relative_to(self.run)): digest(p) for p in self.run.rglob('*') if p.is_file()}
        source = types.ModuleType('scripts.shadow_actuals_source')
        scorer = types.ModuleType('scripts.shadow_actuals_scoring')
        source.verify_package = lambda *a, **kw: self.fail('waiting invoked source')
        scorer.score_windows = lambda *a, **kw: self.fail('waiting invoked scorer')
        out = self.base / 'monitor'
        with patch.dict('sys.modules', {'scripts.shadow_actuals_source': source,
                                       'scripts.shadow_actuals_scoring': scorer}):
            result = monitor.run_monitor(self.run, out, source_package=self.base / 'DOES_NOT_EXIST',
                                         now='2026-10-29T08:00:00+00:00')
        self.assertEqual(result['status'], 'WAITING_TARGET_START')
        self.assertFalse(result['actual_scores_generated'])
        self.assertEqual(result['fixed_missing_candidate_objects'], 1)
        self.assertEqual(result['database_queries'], 0)
        self.assertEqual(result['model_inference_calls'], 0)
        truth = pd.read_csv(out / 'window_truth_status.csv')
        self.assertTrue(truth.actual.isna().all())
        self.assertEqual(set(truth.actual_status), {'NOT_STARTED'})
        after = {str(p.relative_to(self.run)): digest(p) for p in self.run.rglob('*') if p.is_file()}
        self.assertEqual(before, after)

    def test_maturity_uses_shanghai_day_minus_two(self):
        a, sa, _ = monitor.window_truth(self.core, self.seal, '2026-11-28T15:59:59+00:00')
        b, sb, _ = monitor.window_truth(self.core, self.seal, '2026-11-28T16:00:00+00:00')
        self.assertEqual(int(sa.loc[sa.window.eq('d01_30'), 'eligible_days'].iloc[0]), 28)
        self.assertEqual(int(sb.loc[sb.window.eq('d01_30'), 'eligible_days'].iloc[0]), 29)
        self.assertTrue(a.actual.isna().all() and b.actual.isna().all())

    def test_partial_units_not_formal_actual(self):
        daily, stores = synthetic_daily(self.core, days=10)
        truth, status, coverage = monitor.window_truth(self.core, self.seal,
            '2026-11-10T01:00:00+00:00', daily, stores)
        first = truth[truth.window.eq('d01_30')]
        self.assertTrue(first.actual.isna().all())
        self.assertTrue(first.observed_units.eq(20).all())
        self.assertEqual(set(first.actual_status), {'NOT_MATURED'})
        self.assertFalse(status.official_score_allowed.any())
        self.assertTrue(coverage.full_store_actual.isna().all())

    def test_complete_calendar_reconciled_contract_scores(self):
        daily, stores = synthetic_daily(self.core)
        truth, status, coverage = monitor.window_truth(self.core, self.seal,
            '2026-12-30T01:00:00+00:00', daily, stores)
        self.assertTrue(status.official_score_allowed.all())
        for w, expected in [('d01_30', 60), ('d31_60', 60), ('d01_60', 120)]:
            self.assertTrue(truth.loc[truth.window.eq(w), 'actual'].eq(expected).all())
        self.assertEqual(float(coverage.loc[coverage.window.eq('d01_60'), 'full_store_actual'].iloc[0]), 1200)

    def test_one_missing_day_unknown_not_zero(self):
        daily, stores = synthetic_daily(self.core)
        daily = daily.drop(daily.index[0])
        truth, status, _ = monitor.window_truth(self.core, self.seal,
            '2026-12-30T01:00:00+00:00', daily, stores)
        obj = truth[truth.spu.eq('A') & truth.window.eq('d01_30')].iloc[0]
        self.assertTrue(np.isnan(obj.actual))
        self.assertEqual(obj.actual_status, 'SOURCE_INCOMPLETE')
        self.assertEqual(obj.observed_units, 58)
        self.assertFalse(bool(status.loc[status.window.eq('d01_30'), 'official_score_allowed'].iloc[0]))
        self.assertTrue(bool(status.loc[status.window.eq('d31_60'), 'official_score_allowed'].iloc[0]))

    def test_unknown_source_blocks_whole_window(self):
        daily, stores = synthetic_daily(self.core)
        stores.loc[0, 'source_state'] = 'SYNC_COMPLETION_UNKNOWN'
        truth, status, _ = monitor.window_truth(self.core, self.seal,
            '2026-12-30T01:00:00+00:00', daily, stores)
        self.assertTrue(truth.loc[truth.window.eq('d01_30'), 'actual'].isna().all())
        self.assertFalse(bool(status.loc[status.window.eq('d01_30'), 'official_score_allowed'].iloc[0]))

    def test_explicit_zero_remains_distinct(self):
        daily, stores = synthetic_daily(self.core, value=0)
        truth, status, _ = monitor.window_truth(self.core, self.seal,
            '2026-12-30T01:00:00+00:00', daily, stores)
        self.assertTrue(truth.actual.eq(0).all())
        self.assertTrue(status.official_score_allowed.all())

    def test_invalid_negative_infinite_or_duplicate_daily_rejected(self):
        daily, stores = synthetic_daily(self.core)
        for value in [-1.0, np.inf]:
            q = daily.copy(); q.loc[0, 'y'] = value
            with self.assertRaisesRegex(ValueError, 'INVALID_ACTUAL'):
                monitor.window_truth(self.core, self.seal, '2026-12-30T00:00:00+00:00', q, stores)
        with self.assertRaisesRegex(ValueError, 'DUPLICATE_OR_UNKNOWN_CORE'):
            monitor.window_truth(self.core, self.seal, '2026-12-30T00:00:00+00:00',
                                 pd.concat([daily, daily.iloc[[0]]]), stores)

    def test_future_daily_refused_not_trimmed(self):
        daily, stores = synthetic_daily(self.core, days=2)
        with self.assertRaisesRegex(ValueError, 'FUTURE_OR_OUTSIDE_TARGET_ACTUAL'):
            monitor.window_truth(self.core, self.seal, '2026-11-01T00:00:00+00:00', daily, stores)

    def test_other_batch_source_rejected_after_target_starts(self):
        source = types.ModuleType('scripts.shadow_actuals_source')
        source.verify_package = lambda *a, **kw: dict(core_daily=None, shop_daily=None,
            manifest={'run_id': 'SYNTHETIC_RUN', 'issue_seal_sha256': 'WRONG',
                      'core_sha256': 'WRONG', 'target_start': '2026-10-30', 'target_end': '2026-12-28'})
        package = self.base / 'source'; package.mkdir()
        save_json(package / 'manifest.json', {})
        with patch.dict('sys.modules', {'scripts.shadow_actuals_source': source}):
            with self.assertRaises(ValueError):
                monitor.run_monitor(self.run, self.base / 'monitor_bad_source', source_package=package,
                                    now='2026-11-02T00:00:00+00:00')

    def test_mature_monitor_source_to_real_scorer_end_to_end_synthetic(self):
        """Synthetic source adapter only; real monitor and pure scoring run together."""
        daily, stores = synthetic_daily(self.core)
        source = types.ModuleType('scripts.shadow_actuals_source')
        source_manifest = dict(run_id='SYNTHETIC_RUN',
            issue_seal_sha256=digest(self.run / 'issued/issue_seal.json'),
            core_sha256=digest(self.run / 'prepared/core.parquet'),
            target_start='2026-10-30', target_end='2026-12-28', status='SOURCE_READY')
        calls = []
        def verify(path, run_id, core, expected_contract=None):
            calls.append((run_id, len(core), expected_contract))
            self.assertEqual(expected_contract, {k: source_manifest[k] for k in expected_contract})
            return dict(core_daily=daily, shop_daily=stores, manifest=source_manifest)
        source.verify_package = verify
        package = self.base / 'source_complete'; package.mkdir()
        save_json(package / 'manifest.json', source_manifest)
        out = self.base / 'mature_monitor'
        with patch.dict('sys.modules', {'scripts.shadow_actuals_source': source}):
            result = monitor.run_monitor(self.run, out, source_package=package,
                                        now='2026-12-30T01:00:00+00:00')
        self.assertEqual(len(calls), 1)
        self.assertEqual(result['status'], 'ALL_WINDOWS_SCORED')
        self.assertEqual(set(result['official_scored_windows']), set(monitor.WINDOWS))
        self.assertEqual(result['fixed_core_objects'], 4)
        self.assertEqual(result['fixed_paired_objects'], 3)
        metrics = pd.read_csv(out / 'scores_metrics.csv')
        row = metrics[metrics.scope.eq('FULL_CORE') & metrics.model.eq(monitor.CANDIDATE)
                      & metrics.window.eq('d01_60')].iloc[0]
        self.assertEqual(row.fixed_n, 4)
        self.assertEqual(row.output_n, 3)
        self.assertEqual(row.missing_output_n, 1)
        self.assertEqual(row.ae, 90)
        self.assertEqual(row.wape, 0.25)
        coverage = pd.read_csv(out / 'future_coverage.csv')
        row = coverage[coverage.window.eq('d01_60')].iloc[0]
        self.assertAlmostEqual(row.core_future_sales_coverage, 0.4)
        self.assertAlmostEqual(row.output_future_sales_coverage, 0.3)
        self.assertEqual(digest(self.run / 'issued/predictions.parquet'),
                         self.seal['files']['issued/predictions.parquet'])

    def manual_record(self, **updates):
        value = dict(run_id='SYNTHETIC_RUN', shop='JQ-US', spu='A', window='d01_30',
                     original_candidate=30.0, manual_reference_demand=40.0,
                     adjustment_reason='SYNTHETIC_REASON', evidence='SYNTHETIC_EVIDENCE', owner='TEST_OWNER')
        value.update(updates)
        return value

    def append(self, record, journal=None, now='2026-10-29T08:00:00+00:00'):
        record_path = self.base / ('record_' + str(len(list(self.base.glob('record_*.json')))) + '.json')
        save_json(record_path, record)
        return monitor.append_review(self.run, journal or self.base / 'journal', record_path, now=now)

    def test_manual_append_no_automatic_confirmation_or_forecast_rewrite(self):
        before = digest(self.run / 'issued/predictions.parquet')
        event = self.append(self.manual_record())
        self.assertEqual(event['ingested_at'], '2026-10-29T08:00:00+00:00')
        self.assertFalse(event['approval_by_codex'])
        self.assertIsNone(event['record'].get('confirmed_at'))
        self.assertIsNone(event['record'].get('actual_occurred_at'))
        state = monitor.review_status(self.pred, 'SYNTHETIC_RUN', self.base / 'journal')
        self.assertEqual(int(state.review_state.ne('PENDING').sum()), 1)
        self.assertEqual(digest(self.run / 'issued/predictions.parquet'), before)

    def test_manual_revision_requires_explicit_prior_event_and_preserves_old(self):
        first = self.append(self.manual_record())
        with self.assertRaisesRegex(ValueError, 'EXPLICIT_REVIEW_REVISION_REQUIRED'):
            self.append(self.manual_record(manual_reference_demand=42.0), now='2026-10-29T09:00:00+00:00')
        files = list((self.base / 'journal').glob('*.json')); prior_hash = digest(files[0])
        second = self.append(self.manual_record(manual_reference_demand=42.0,
            supersedes_event_id=first['event_id']), now='2026-10-29T09:00:00+00:00')
        self.assertEqual(second['supersedes_event_id'], first['event_id'])
        self.assertEqual(digest(files[0]), prior_hash)
        self.assertEqual(len(monitor.journal_events(self.base / 'journal')), 2)

    def test_manual_original_and_future_confirmation_refused(self):
        with self.assertRaisesRegex(ValueError, 'ORIGINAL_PREDICTION_CHANGED'):
            self.append(self.manual_record(original_candidate=31.0))
        with self.assertRaisesRegex(ValueError, 'FUTURE_MANUAL_EVENT_TIME'):
            self.append(self.manual_record(confirmed_at='2026-10-30T00:00:00+00:00'))

    def test_journal_inside_seal_rejected_before_any_mutation(self):
        journal = self.run / 'SHOULD_NOT_EXIST'
        with self.assertRaisesRegex(ValueError, 'JOURNAL_MUST_BE_SEPARATE'):
            self.append(self.manual_record(), journal=journal)
        self.assertFalse(journal.exists())

    def test_declared_historical_record_time_does_not_backdate_audit(self):
        event = self.append(self.manual_record(recorded_at='2026-10-28T01:00:00+00:00'))
        self.assertEqual(event['record']['declared_recorded_at'], '2026-10-28T01:00:00+00:00')
        self.assertEqual(event['record']['recorded_at'], event['ingested_at'])
        self.assertEqual(event['ingested_at'], '2026-10-29T08:00:00+00:00')

    def test_event_audit_time_tampering_detected(self):
        self.append(self.manual_record())
        path = next((self.base / 'journal').glob('*.json'))
        event = json.loads(path.read_text(encoding='utf-8'))
        event['ingested_at'] = '2026-10-28T00:00:00+00:00'
        save_json(path, event)
        with self.assertRaisesRegex(ValueError, 'REVIEW_METADATA_TAMPERED'):
            monitor.journal_events(self.base / 'journal')

    def test_existing_writer_lock_blocks_without_removal(self):
        journal = self.base / 'journal'; journal.mkdir()
        lock = journal / '.writer.lock'; lock.write_text('OTHER_WRITER', encoding='utf-8')
        with self.assertRaises(FileExistsError):
            self.append(self.manual_record(), journal=journal)
        self.assertEqual(lock.read_text(encoding='utf-8'), 'OTHER_WRITER')
        self.assertEqual(list(journal.glob('*.json')), [])


if __name__ == '__main__':
    unittest.main()
