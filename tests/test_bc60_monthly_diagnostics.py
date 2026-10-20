import tempfile
from pathlib import Path
import unittest
import pandas as pd
from scripts.bc60_monthly_diagnostics import run


class DiagnosticTests(unittest.TestCase):
    def test_descriptive_metadata_and_monthly_absence_are_not_zero(self):
        with tempfile.TemporaryDirectory(prefix='bc60_diag_synthetic_') as tmp:
            root=Path(tmp);(root/'inputs').mkdir();(root/'scoring').mkdir()
            d=pd.DataFrame({'shop':'JQ-US','spu':'synthetic','ds':pd.date_range('2024-01-01','2025-04-01'),'y':1,'mapped':True,'sales_state':'OBSERVED'})
            d.to_parquet(root/'inputs/daily.parquet',index=False)
            pd.DataFrame([{'shop':'JQ-US','spu':'synthetic','month':'2024-01','qty':31}]).to_parquet(root/'inputs/monthly_reference.parquet',index=False)
            row={'cohort_id':'monthly_core','issue_date':'2025-01-02','issue_month':'2025-01','shop':'JQ-US','spu':'synthetic','model':'F28','window':'d01_60','prediction':60.,'actual':60.,'absolute_error':0.,'decision':'HIT','maturity':'12+','lifecycle':'ESTABLISHED','truth_status':'OBSERVED'}
            pd.DataFrame([row]).to_parquet(root/'scoring/scores.parquet',index=False)
            pd.DataFrame([dict(row,slice='issue',wape=0.,hit_rate=1.,core_n=1,scored_n=1,hit_n=1)]).to_csv(root/'scoring/metrics.csv',index=False)
            pd.DataFrame([{k:row[k] for k in ['cohort_id','issue_date','shop','spu']}]).to_csv(root/'scoring/operating_indicators_diagnostic.csv',index=False)
            run(root,root/'out')
            rec=pd.read_csv(root/'out/daily_monthly_reconciliation.csv')
            self.assertEqual(rec.loc[rec.month.eq('2024-01'),'status'].iloc[0],'EXACT')
            missing=rec[rec.month.eq('2024-02')].iloc[0]
            self.assertEqual(missing.status,'MONTHLY_REFERENCE_ABSENT');self.assertTrue(pd.isna(missing.qty))
            aggregate=pd.read_csv(root/'out/daily_monthly_reconciliation_summary.csv')
            missing_aggregate=aggregate[aggregate.month.eq('2024-02')].iloc[0]
            self.assertTrue(pd.isna(missing_aggregate.monthly_units))
            self.assertTrue(pd.isna(missing_aggregate.difference))
            self.assertEqual(missing_aggregate.comparable_n,0)
            self.assertEqual(missing_aggregate.known_monthly_n,0)
            self.assertEqual(missing_aggregate.known_daily_n,1)
            self.assertTrue(pd.read_csv(root/'out/season_history_coverage.csv').complete_365_days.all())
            self.assertTrue(pd.read_csv(root/'out/descriptive_slices.csv').scope.eq('DESCRIPTIVE_NOT_CAUSAL').all())
            with self.assertRaises(FileExistsError):run(root,root/'out')


if __name__=='__main__':unittest.main()
