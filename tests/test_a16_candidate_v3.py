import datetime as dt
import importlib.util
import pathlib
import sys
import unittest

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from a16_candidate_v3 import Candidate,CutoffHistory,shadow_envelope,shift


class CandidateSafetyTests(unittest.TestCase):
    def test_cutoff_excludes_current_and_future(self):
        values={('S|P',shift('2026-06',0)):1,('S|P',shift('2026-07',0)):99}
        h=CutoffHistory(values,shift('2026-07',0))
        self.assertEqual(len(h),1)
        self.assertEqual(h.get(('S|P',shift('2026-06',0))),1)
        with self.assertRaises(ValueError):h.get(('S|P',shift('2026-07',0)),0)

    def test_missing_actual_not_zero(self):
        h=CutoffHistory({},shift('2026-07',0))
        self.assertIsNone(h.get(('S|P',shift('2026-06',0))))
        self.assertEqual(h.missing,1)

    def test_year_rollover(self):
        self.assertEqual(shift('2026-01',-1),dt.date(2025,12,1))
        self.assertEqual(shift('2025-12',2),dt.date(2026,2,1))

    def test_default_switch_does_not_call_candidate(self):
        result=shadow_envelope([{'qty':9}],lambda _:self.fail('must not execute'))
        self.assertEqual(result['status'],'DISABLED')

    def test_failure_cannot_mutate_production_or_leak_error_text(self):
        original=[{'qty':9}]
        def fail(rows):
            rows[0]['qty']=999
            raise RuntimeError('secret-like text must not be returned')
        result=shadow_envelope(original,fail,enabled=True)
        self.assertEqual(original,[{'qty':9}])
        self.assertEqual(result['production'],original)
        self.assertEqual(result['status'],'FAILED_ISOLATED')
        self.assertNotIn('secret',str(result))

    def test_future_sales_labels_do_not_change_prediction(self):
        observations={('JQ-US|SYNTHETIC',shift('2024-01',i)):100+i%7 for i in range(30)}
        first=Candidate(observations,{'SYNTHETIC':'synthetic'},ROOT)
        p,features,m=first.predict('2025-10',3)
        mutated={k:(1000000 if k[1]>=shift('2025-07',0) else v) for k,v in observations.items()}
        second=Candidate(mutated,{'SYNTHETIC':'synthetic'},ROOT)
        p2,_,m2=second.predict('2025-10',3)
        self.assertEqual(p['JQ-US|SYNTHETIC'][first.r12.A16],p2['JQ-US|SYNTHETIC'][second.r12.A16])
        self.assertEqual((m['gamma_down'],m['gamma_up']),(m2['gamma_down'],m2['gamma_up']))
        self.assertLess(max(m['training_months']),m['cutoff'])

    def test_shop_grain_not_merged(self):
        obs={(s+'|SYNTHETIC',shift('2024-01',i)):q for s,q in [('JQ-US',100),('MT-US',500)] for i in range(24)}
        c=Candidate(obs,{},ROOT);p,_,_=c.predict('2025-10',3)
        self.assertEqual(set(p),{'JQ-US|SYNTHETIC','MT-US|SYNTHETIC'})
        self.assertNotEqual(p['JQ-US|SYNTHETIC'][c.r12.A16],p['MT-US|SYNTHETIC'][c.r12.A16])

    def test_dormant_is_not_missing_output(self):
        obs={('JQ-US|SYNTHETIC',shift('2024-01',0)):10}
        c=Candidate(obs,{},ROOT);rows,_=c.features('2025-10',1)
        self.assertEqual(rows[0]['可预测性'],'DORMANT_RETURN')


if __name__=='__main__':unittest.main()
