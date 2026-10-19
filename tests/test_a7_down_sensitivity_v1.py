"""Synthetic tests only; no business rows, network, or production execution."""
import datetime as dt
import unittest
from unittest.mock import patch

from scripts import a7_down_sensitivity_v1 as x


class TrustAndInputTests(unittest.TestCase):
    def test_no_evidence_unknown(self):
        self.assertEqual(x.trust_state()[0],'UNKNOWN')

    def test_warnings_not_proof(self):
        self.assertEqual(x.trust_state({'descriptive_trough_flag':True,'conclusion':'NON_COMPARABLE'})[0],'UNKNOWN')

    def test_verified_comparability(self):
        for label in ('COMPARABLE','NON_COMPARABLE'):
            evidence=dict(scope_verified=True,effective_dates_verified=True,operating_evidence_verified=True,conclusion=label)
            self.assertEqual(x.trust_state(evidence)[0],label)

    def test_zero_distinct_from_missing(self):
        self.assertEqual([x.value_state(v)for v in(None,0,8)],['MISSING_RECORD','EXPLICIT_ZERO','OBSERVED_VALUE'])

    def test_protection_uses_precutoff_sequence(self):
        self.assertEqual(x.protection('稳定',[30,20,10]),(False,True,True))
        self.assertEqual(x.protection('衰退',[10,20,30]),(True,False,True))

    def test_missing_not_decline_proof(self):
        for seq in ([30,None,10],[30,20,0],[30,30,10]):
            self.assertFalse(x.protection('稳定',seq)[2])

    def test_protection_exact_window(self):
        with self.assertRaises(ValueError):x.protection('稳定',[3,2])

    def daily(self):
        return {'store_name':'SHOP','spu':'ITEM','target_month':'2024-02','n':58,'days':29,'asin_country_n':2,
          'volume':'0','null_volume':'0','negative_volume':'0','deleted_rows':'0','date_mismatch':'0',
          'first_dt':'2024-02-01','last_dt':'2024-02-29'}

    def test_complete_zero_leap_month(self):
        x.validate_daily_zero({'shop':'SHOP','spu':'ITEM','target_month':'2024-02'},self.daily())

    def test_incomplete_zero_rejected(self):
        for field,value in [('n',57),('days',28),('null_volume','1'),('volume','2'),('deleted_rows','1'),('first_dt','2024-02-02'),('spu','OTHER')]:
            d=self.daily();d[field]=value
            with self.assertRaises(ValueError):x.validate_daily_zero({'shop':'SHOP','spu':'ITEM','target_month':'2024-02'},d)


class SensitivityTests(unittest.TestCase):
    def test_frozen_weight_preserved_even_missing(self):
        self.assertEqual(x.sensitive_value(100,20,20,20,'DOWN',1,1,1),(20,'UNCHANGED'))

    def test_only_down_changes(self):
        for rule in ('UP_RISING','UP_BLOCKED','NEUTRAL','A3_ONLY'):
            self.assertEqual(x.sensitive_value(100,130,120,110,rule,1,0,0),(110,'UNCHANGED'))

    def test_original_gamma_zero_unchanged(self):
        self.assertEqual(x.sensitive_value(100,20,100,100,'DOWN',0,.5,1),(100,'UNCHANGED'))

    def test_required_missing_explicit_null(self):
        self.assertEqual(x.sensitive_value(100,20,20,20,'DOWN',1,0,1),(None,'MISSING_REQUIRED_INPUT'))

    def test_frozen_rounding_matches_exact_function(self):
        c=x.Candidate({}, {}, x.ROOT)
        for a3,cand in ((101,20),(103,12.3),(7,0),(1000,899)):
            row={x.A3:a3,'季节候选':cand,'历史季节阶段':'FALLING'}
            for gamma in (0,.1,.3,.6,1):
                original=c.r5.add_a7([row],gamma,.3)[0]
                for weight in (1,.5,0):
                    expected=c.r5.add_a7([row],gamma*weight,.3)[0][x.A7]
                    actual=x.sensitive_value(a3,cand,original[x.A7],original[x.A7],'DOWN',gamma,weight,0)[0]
                    self.assertEqual(actual,expected)

    def test_unregistered_grid_rejected(self):
        with self.assertRaises(ValueError):x.sensitive_value(100,20,20,20,'DOWN',1,.2,0)

    def test_changed_a16_down_rejected(self):
        with self.assertRaises(ValueError):x.sensitive_value(100,20,20,30,'DOWN',1,.5,0)


class MetricTests(unittest.TestCase):
    def test_over_under_denominator_and_positive_hits(self):
        s=x.score([{'truth':100,'a16':130},{'truth':100,'a16':50},{'truth':0,'a16':10},{'truth':None,'a16':999}],'a16')
        self.assertEqual((s['n'],s['actual_sum'],s['over_units'],s['under_units']),(3,200,40,50))
        self.assertAlmostEqual(s['wape'],.45);self.assertAlmostEqual(s['bias'],-.05)
        self.assertEqual((s['positive_n'],s['hit30'],s['zero_absolute_error']),(2,.5,10))

    def test_all_zero_rate_undefined(self):
        s=x.score([{'truth':0,'a16':10}],'a16')
        self.assertIsNone(s['over_rate']);self.assertIsNone(s['wape']);self.assertEqual(s['zero_mae'],10)

    def test_common_population_required(self):
        rows=[{'common_score':False,'truth':100,'a16':100}]
        m=x.grouped_metrics(rows)
        self.assertTrue(all(z['n']==0 for z in m))

    def test_bootstrap_reproducible(self):
        rows=[{'common_score':True,'protected':True,'shop':'SHOP','spu':str(i),'truth':100,'a16':80,'w_half':90}for i in range(3)]
        b=x.bootstrap(rows,'w_half')
        self.assertEqual(b,x.bootstrap(rows,'w_half'));self.assertAlmostEqual(b['lower'],-.1)


class GateTests(unittest.TestCase):
    def test_receipt_requires_real_confirmation(self):
        for receipt in ({},{'gate_confirmed':True,'prereg_commit':x.PREREG}):
            with self.assertRaises(ValueError):x.check_approval(receipt,'2026-10-19T03:00:00+00:00')

    def test_future_confirmation_rejected(self):
        r={'gate_confirmed':True,'prereg_commit':x.PREREG,'user_confirmation':'synthetic test only','confirmed_at':'2099-01-01T00:00:00+00:00'}
        with self.assertRaises(ValueError):x.check_approval(r,'2026-10-19T03:00:00+00:00')

    def test_synthetic_approved_receipt(self):
        r={'gate_confirmed':True,'prereg_commit':x.PREREG,'user_confirmation':'synthetic test only','confirmed_at':'2026-10-19T02:00:00+00:00'}
        x.check_approval(r,'2026-10-19T03:00:00+00:00')

    def test_future_feature_access_rejected(self):
        c=x.CutoffHistory({('S|X',x.shift('2026-01',0)):1},x.shift('2026-02',0))
        with self.assertRaises(ValueError):c.get(('S|X',x.shift('2026-02',0)))


if __name__=='__main__':unittest.main()
