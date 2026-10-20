"""Synthetic archive-adapter tests. No models and no business effects."""
import unittest
import numpy as np
import pandas as pd
from scripts import bc60_monthly_side_panels as panels


class SidePanelTests(unittest.TestCase):
    def source(self):
        return pd.DataFrame([{'target_month':'2025-10','cutoff_month':'2025-09','horizon':'H1',
            'shop':'synthetic_shop','spu':str(i),'actual':a,'v4':v,'a16':b,
            'forecastability':'ESTABLISHED','a16_status':'OUTPUT','current_category':'category',
            'current_season':'season'} for i,(a,v,b) in enumerate([(100,120,80),(0,5,0),(np.nan,20,20),(100,100,np.nan)])])

    def test_native_dates_missing_and_closed_tolerance(self):
        frame,scores=panels.prepare_monthly(self.source())
        self.assertTrue(frame.input_cutoff.eq('2025-08-31').all())
        self.assertTrue(frame.target_end.eq('2025-10-31').all())
        a=scores[scores.model.eq('A16')]
        self.assertEqual(a.decision.tolist(),['HIT','UNJUDGED','UNJUDGED','UNJUDGED'])
        m=panels.monthly_metrics(a)
        self.assertEqual(m['hit_rate'],.5)
        self.assertEqual(m['scored_n'],2)
        self.assertEqual(m['zero_truth_n'],1)
        self.assertEqual(m['missing_output_n'],1)

    def test_pair_denominator_matches_and_missing_month_not_zero(self):
        _,scores=panels.prepare_monthly(self.source())
        summary=panels.monthly_summary(scores)
        pair=summary[(summary.target_month=='2025-10')&(summary.horizon=='H1')&
                     (summary.scope=='pairwise_v4_a16')&(summary.shop=='ALL_SHOPS')]
        self.assertEqual(pair.cohort_n.tolist(),[2,2])
        missing=summary[summary.target_month.eq('2025-01')]
        self.assertTrue(missing.status.eq('NOT_AVAILABLE_ARCHIVE').all())
        self.assertTrue(missing.cohort_n.isna().all())

    def test_current_metadata_conflict_not_arbitrarily_resolved(self):
        source=self.source().iloc[[0,0]].copy()
        source.iloc[1,source.columns.get_loc('current_category')]='different'
        mapping,conflicts=panels.metadata_tables(source)
        self.assertEqual(mapping.iloc[0].current_category_state,'CONFLICT')
        self.assertIsNone(mapping.iloc[0].current_category)
        self.assertEqual(len(conflicts),2)
        self.assertTrue(conflicts.resolution.eq('UNRESOLVED_NO_BACKFILL').all())

    def test_bad_cutoff_rejected(self):
        source=self.source();source.loc[0,'cutoff_month']='2025-08'
        with self.assertRaisesRegex(ValueError,'CUTOFF_HORIZON'):
            panels.prepare_monthly(source)

    def test_rolling_fallback_keeps_update_and_target_boundaries(self):
        rows=[]
        for offset in [0,7]:
            rows.append({'track':'T2','original_issue_date':'2025-01-02',
                'update_date':str((pd.Timestamp('2025-01-02')+pd.Timedelta(days=offset)).date()),
                'update_offset':offset,'input_cutoff':'2024-12-31','window':'d01_30',
                'target_start':str((pd.Timestamp('2025-01-03')+pd.Timedelta(days=offset)).date()),
                'target_end':str((pd.Timestamp('2025-02-01')+pd.Timedelta(days=offset)).date()),
                'model':'R28-D','shop':'synthetic_shop','actual':100.,'prediction':110.,
                'decision':'HIT','truth_status':'OBSERVED'})
        result=panels.recompute_rolling_summary(pd.DataFrame(rows))
        self.assertEqual(len(result),2)
        self.assertEqual(result.target_start.tolist(),['2025-01-03','2025-01-10'])
        np.testing.assert_allclose(result.wape,[.1,.1])


if __name__=='__main__':unittest.main()
