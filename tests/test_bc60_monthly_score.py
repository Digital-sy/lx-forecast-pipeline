import unittest
import numpy as np
import pandas as pd
from scripts.bc60_monthly_score import metric, decision, window_truth


class ScoreTests(unittest.TestCase):
    def test_full_core_unknown_and_zero_denominators(self):
        f=pd.DataFrame({'actual':[100.,0.,200.,np.nan],'prediction':[110.,3.,np.nan,8.]})
        f['decision']=[decision(a,p) for a,p in zip(f.actual,f.prediction)]
        m=metric(f)
        self.assertEqual(m['core_n'],4);self.assertEqual(m['scored_n'],2)
        self.assertEqual(m['hit_rate'],.5);self.assertEqual(m['hit_sales_share'],1/3)
        self.assertEqual(m['zero_actual_n'],1);self.assertEqual(m['zero_absolute_error'],3)
        self.assertEqual(m['wape'],.13)

    def test_missing_calendar_never_zero_and_not_matured_first(self):
        g=pd.DataFrame({'y':[10,20],'sales_state':['OBSERVED']*2,'mapped':[True]*2},index=pd.to_datetime(['2026-01-01','2026-01-03']))
        v,s=window_truth(g,pd.Timestamp('2026-01-01'),pd.Timestamp('2026-01-03'),pd.Timestamp('2026-10-01'))
        self.assertTrue(np.isnan(v));self.assertEqual(s,'MISSING_OR_INVALID_DAYS')
        v,s=window_truth(g,pd.Timestamp('2026-01-01'),pd.Timestamp('2027-01-03'),pd.Timestamp('2026-10-01'))
        self.assertEqual(s,'NOT_MATURED')

    def test_inf_not_valid_truth_or_percentage(self):
        self.assertEqual(decision(10,float('inf')),'UNJUDGED')
        g=pd.DataFrame({'y':[np.inf],'sales_state':['OBSERVED'],'mapped':[True]},index=pd.to_datetime(['2026-01-01']))
        self.assertTrue(np.isnan(window_truth(g,g.index[0],g.index[0],pd.Timestamp('2026-10-01'))[0]))

    def test_exact_twenty_percent(self):
        self.assertEqual(decision(100,120),'HIT');self.assertEqual(decision(100,120.0001),'MISS')
        self.assertEqual(decision(0,0),'UNJUDGED')


if __name__=='__main__':unittest.main()
