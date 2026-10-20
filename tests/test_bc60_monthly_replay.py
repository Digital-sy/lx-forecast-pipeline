"""Synthetic, zero-fit tests for the finite extension executor."""
import unittest

import numpy as np
import pandas as pd

from scripts import bc60_monthly_replay as replay
from scripts import bc60_v3_direct as direct


class MonthlyReplayTests(unittest.TestCase):
    def daily(self,start='2024-01-01',end='2024-05-31'):
        dates=pd.date_range(start,end)
        frames=[]
        for shop,spu,offset in [('JQ-US','synthetic_a',1),('MT-US','synthetic_a',101),('JQ-US','synthetic_b',11)]:
            frames.append(pd.DataFrame({'shop':shop,'spu':spu,'ds':dates,'y':np.arange(len(dates),dtype=np.int64)+offset,
                'mapped':True,'sales_state':'OBSERVED',**{c:1 for c in replay.RAW_EXOG}}))
        return pd.concat(frames,ignore_index=True)

    def monthly(self):
        return pd.DataFrame([{'shop':s,'spu':p,'month':m,'qty':1} for s,p in [('JQ-US','synthetic_a'),('MT-US','synthetic_a'),('JQ-US','synthetic_b')]
            for m in ['2023-09','2023-10','2023-11','2023-12']])

    def test_grid_exact_boundaries(self):
        grid=replay.issue_grid()
        self.assertEqual(len(grid),22)
        self.assertEqual(int(grid.calendar_mature.sum()),20)
        self.assertEqual(grid.iloc[0].input_cutoff,'2024-12-31')
        self.assertEqual(grid.iloc[0].target_start,'2025-01-03')
        self.assertEqual(grid.iloc[0].target_end,'2025-03-03')
        self.assertFalse(grid.iloc[-1].calendar_mature)

    def test_unknown_calendar_does_not_create_empty_success(self):
        d=self.daily('2024-12-04','2024-12-31');m=self.monthly();m.month=pd.PeriodIndex(m.month,freq='M')
        u,q=replay.freeze_issue(d,m,'2025-01-02')
        self.assertTrue(u.empty)
        self.assertTrue(q.core_n.isna().all())
        self.assertTrue(q.selection_status.eq('INSUFFICIENT_HISTORY').all())

    def test_complete_calendar_ties_and_full_denominator(self):
        dates=pd.date_range('2024-10-03','2024-12-31')
        d=pd.concat([pd.DataFrame({'shop':'JQ-US','spu':p,'ds':dates,'y':q,'mapped':mapped,'sales_state':'OBSERVED'})
                     for p,q,mapped in [('a',80,True),('b',5,True),('c',5,True),('unmapped',10,False)]],ignore_index=True)
        m=self.monthly();m.month=pd.PeriodIndex(m.month,freq='M')
        u,q=replay.freeze_issue(d,m,'2025-01-02');good=q[q.shop.eq('JQ-US')].iloc[0]
        self.assertEqual(good.full_shop_units,9000)
        self.assertEqual(good.unmapped_units,900)
        self.assertEqual(good.core_n,3)
        self.assertEqual(good.selection_coverage,.9)
        self.assertEqual(set(u.loc[u.is_core,'spu']),{'a','b','c'})

    def test_invalid_selection_day_is_not_silently_summed(self):
        d=self.daily('2024-10-03','2024-12-31');d.loc[0,'sales_state']='UNKNOWN'
        m=self.monthly();m.month=pd.PeriodIndex(m.month,freq='M')
        _,q=replay.freeze_issue(d,m,'2025-01-02')
        self.assertEqual(q[q.shop.eq('JQ-US')].iloc[0].selection_status,'INVALID_SELECTION_INPUT')

    def test_zero_sales_complete_calendar_is_known_zero_core(self):
        d=self.daily('2024-10-03','2024-12-31');d['y']=0;d['sales_state']='EXPLICIT_ZERO'
        m=self.monthly();m.month=pd.PeriodIndex(m.month,freq='M')
        _,q=replay.freeze_issue(d,m,'2025-01-02')
        row=q[q.shop.eq('JQ-US')].iloc[0]
        self.assertEqual(row.core_n,0)
        self.assertEqual(row.selection_status,'NO_POSITIVE_SELECTION_SALES')

    def test_vectorized_matches_frozen_v3_exactly(self):
        d,m=replay.normalize(self.daily(),self.monthly())
        # Unknown calendar day, explicit zero, and delayed historical eligibility.
        d.loc[10,['y','sales_state']]=[0,'EXPLICIT_ZERO']
        d=d[~((d.shop=='JQ-US')&(d.spu=='synthetic_b')&(d.ds==pd.Timestamp('2024-02-01')))]
        m.loc[(m.shop=='MT-US')&(m.month==pd.Period('2023-12')),'month']=pd.Period('2024-02')
        old=direct.Context(d,m,replay.SHOPS,'2024-05-31');new=replay.VectorContext(d,m,replay.SHOPS,'2024-05-31')
        for segment in ['M1','M2']:
            a,ae=old.training('2024-06-02',segment);b,be=new.training('2024-06-02',segment)
            pd.testing.assert_frame_equal(a,b,check_dtype=False,check_exact=True)
            columns=['simulated_issue_date','reason','rows','issue_date','segment']
            pd.testing.assert_frame_equal(ae[columns].sort_values(columns[:2]).reset_index(drop=True),
                be[columns].sort_values(columns[:2]).reset_index(drop=True),check_dtype=False)

    def test_legacy_integer_target_survives_nullable_extension(self):
        d,m=replay.normalize(self.daily(),self.monthly())
        d['y']=d.y.astype(float);d.loc[0,'y']=np.nan;d.loc[0,'sales_state']='UNKNOWN'
        train,terminal,_,_,_=replay.legacy_training(d,m,'2024-06-02')
        self.assertEqual(str(train.y.dtype),'int64')
        self.assertTrue(terminal)
        self.assertTrue(all(g.ds.max()==pd.Timestamp('2024-05-31') for g in terminal.values()))

    def test_no_stale_terminal_prediction(self):
        d,m=replay.normalize(self.daily(end='2024-05-30'),self.monthly())
        _,terminal,_,_,_=replay.legacy_training(d,m,'2024-06-02')
        self.assertFalse(terminal)

    def test_positive_months_never_backfilled_from_future(self):
        d,m=replay.normalize(self.daily(),self.monthly())
        m['month']=pd.PeriodIndex(['2026-01']*len(m),freq='M')
        # Avoid duplicate keys intentionally introduced by replacing months.
        m=m.drop_duplicates(['shop','spu','month'])
        train,_,_,_,qualified=replay.legacy_training(d,m,'2024-06-02')
        self.assertFalse(qualified);self.assertTrue(train.empty)
        ctx=replay.VectorContext(d,m,replay.SHOPS,'2024-05-31')
        frame,_=ctx.training('2024-06-02','M1')
        self.assertTrue(frame.empty)

    def test_baselines_calendar_and_missing_not_zero(self):
        d=self.daily('2024-12-04','2024-12-31');objects=d[['shop','spu']].drop_duplicates()
        outputs=pd.DataFrame(replay.baseline_values(d,'2025-01-02',objects))
        f=outputs[outputs.model.eq('F28')].pivot(index=['shop','spu'],columns='window',values='prediction')
        np.testing.assert_array_equal(f.d01_30,f.d31_60)
        np.testing.assert_array_equal(f.d01_60,2*f.d01_30)
        broken=pd.DataFrame(replay.baseline_values(d.iloc[1:],'2025-01-02',objects))
        self.assertTrue(broken[(broken.shop=='JQ-US')&(broken.spu=='synthetic_a')].prediction.isna().all())

    def test_fractional_known_sales_rejected(self):
        d=self.daily();d['y']=d.y.astype(float);d.loc[0,'y']=1.5
        with self.assertRaises(ValueError):replay.normalize(d,self.monthly())

    def test_fast_nonoverlap_matches_original_greedy(self):
        for starts in [pd.date_range('2024-01-01',periods=91),pd.to_datetime(['2024-01-01','2024-01-02','2024-02-01','2024-04-01']),pd.DatetimeIndex([])]:
            frame=pd.DataFrame({'label_start':starts,'label_end':starts+pd.Timedelta(days=29)})
            self.assertEqual(replay.nonoverlap_count(starts),direct.disjoint_windows(frame))

    def test_after_lag_drop_support_is_not_technical_failure(self):
        self.assertEqual(replay.after_lag_drop_status(pd.DataFrame({'y':[]})),'NOT_TRAINABLE_AFTER_LAG_DROP')
        self.assertEqual(replay.after_lag_drop_status(pd.DataFrame({'y':[0,0]})),'NOT_TRAINABLE_ZERO_LABEL_SUPPORT')
        self.assertEqual(replay.after_lag_drop_status(pd.DataFrame({'y':[0,1]})),'OK')

    def test_training_fingerprints_are_order_invariant_but_value_sensitive(self):
        frame=pd.DataFrame({'key':['b','a'],'y':np.array([2,1],dtype=np.float32)})
        left=replay.training_audit(frame,['key'],['key','y'])
        self.assertEqual(left,replay.training_audit(frame.iloc[::-1],['key'],['key','y']))
        frame.loc[0,'y']=3
        right=replay.training_audit(frame,['key'],['key','y'])
        self.assertEqual(left['column_sha256']['key'],right['column_sha256']['key'])
        self.assertNotEqual(left['column_sha256']['y'],right['column_sha256']['y'])

    def test_direct_earliest_support_does_not_require56days(self):
        d,m=replay.normalize(self.daily(end='2024-02-29'),self.monthly())
        ctx=replay.VectorContext(d,m,replay.SHOPS,'2024-02-29')
        frame,_=ctx.training('2024-03-02','M1')
        self.assertFalse(frame.empty)
        self.assertEqual(frame.simulated_issue_date.min(),pd.Timestamp('2024-01-30'))


if __name__=='__main__':unittest.main()
