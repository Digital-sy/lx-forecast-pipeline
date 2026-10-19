import unittest
from scripts import assess_a7_down_sensitivity_v1 as a


class AssessmentTests(unittest.TestCase):
    def test_strict_unrounded_wape_gate(self):
        b=dict(actual=1000000000,absolute_error=400000000,over=200000000)
        c=dict(b,absolute_error=399999999)
        self.assertEqual(a.numerical_gate(b,c,b,b),'通过')
        self.assertEqual(a.numerical_gate(b,b,b,b),'不通过')

    def test_tiny_protection_increase_fails(self):
        b=dict(actual=1000000000,absolute_error=400000000,over=200000000)
        self.assertEqual(a.numerical_gate(b,dict(b,absolute_error=399999999),b,dict(b,over=200000001)),'不通过')

    def test_zero_denominator_unknown(self):
        b=dict(actual=0,absolute_error=0,over=0)
        self.assertEqual(a.numerical_gate(b,b,b,b),'不可判断')

    def test_changed_denominator_rejected(self):
        b=dict(actual=10,absolute_error=5,over=4)
        with self.assertRaises(ValueError):a.numerical_gate(b,dict(b,actual=11),b,b)

    def test_independent_metrics_and_zero(self):
        rows=[dict(truth=100,a16=130),dict(truth=100,a16=50),dict(truth=0,a16=10),dict(truth=None,a16=999)]
        m=a.measure(rows,'a16')
        self.assertEqual((m['actual'],m['over'],m['under'],m['zero_absolute_error']),(200,40,50,10))
        self.assertEqual(m['hit30'],.5)

    def test_other_shop_focus_preserved(self):
        rows=[dict(shop='RKZ-US',spu='KZ230'),dict(shop='SY-US',spu='BX316'),dict(shop='JQ-US',spu='NOT_FOCUS')]
        pairs=a.focus_pairs(rows)
        self.assertIn(('RKZ-US','KZ230'),pairs);self.assertIn(('SY-US','BX316'),pairs)
        self.assertNotIn(('JQ-US','KZ230'),pairs);self.assertNotIn(('JQ-US','NOT_FOCUS'),pairs)

    def test_shared_population_rejects_partial_candidate(self):
        row=dict(truth=10,**{m:1 for m in a.MODELS});row['w_half']=None
        with self.assertRaises(ValueError):a.compare([row],'synthetic')

    def test_missing_truth_not_zero(self):
        m=a.measure([dict(truth=None,a16=7)],'a16')
        self.assertEqual(m['n'],0);self.assertEqual(m['zero_n'],0);self.assertIsNone(m['wape'])


if __name__=='__main__':unittest.main()
