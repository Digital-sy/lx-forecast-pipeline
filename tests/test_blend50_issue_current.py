import unittest
from unittest.mock import patch
import datetime
import pandas as pd
from scripts.blend50_issue_current import label_check,same_business_day,SLOTS
from scripts.blend50_prospective import time_contract,verify_seal_time

class IssueTests(unittest.TestCase):
    def training(self):
        return pd.DataFrame([dict(simulated_issue_date=pd.Timestamp('2026-01-02'),input_cutoff=pd.Timestamp('2025-12-31'),
            history_end=pd.Timestamp('2025-12-31'),label_end=pd.Timestamp('2026-03-03'))])
    def test_labels_must_finish(self):
        with self.assertRaisesRegex(ValueError,'IMMATURE'):label_check(self.training(),pd.Timestamp('2026-03-02'))
    def test_own_cutoff(self):
        x=self.training();x['input_cutoff']=pd.Timestamp('2026-01-01')
        with self.assertRaisesRegex(ValueError,'ROW_CUTOFF'):label_check(x,pd.Timestamp('2026-03-03'))
    def test_history_cannot_extend(self):
        x=self.training();x['history_end']=pd.Timestamp('2026-01-01')
        with self.assertRaisesRegex(ValueError,'FUTURE'):label_check(x,pd.Timestamp('2026-03-03'))
    def test_correct_mature_labels(self):label_check(self.training(),pd.Timestamp('2026-03-03'))
    def test_arbitrary_issue_windows(self):
        x=time_contract('2026-10-28');self.assertEqual(x['W1'],['2026-10-29','2026-11-27']);self.assertEqual(x['W2'],['2026-11-28','2026-12-27'])
    def test_expired_date_refused(self):
        with self.assertRaisesRegex(ValueError,'NO_BACKDATE'):same_business_day('2001-01-01')
    def test_seal_after_target_rejected(self):
        with self.assertRaisesRegex(ValueError,'TARGET_ALREADY'):verify_seal_time('2026-10-28','2026-10-29T00:00:00+08:00')
    def test_fit_budget_four_fixed_slots(self):
        self.assertEqual(set(SLOTS),{'direct_M1','direct_M2','annual_M1','annual_M2'})

if __name__=='__main__':unittest.main()
