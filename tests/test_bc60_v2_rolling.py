import unittest
import pandas as pd
from scripts.bc60_v2_rolling import CalendarStore, information_dates, target_dates, score_row, aggregate_metrics


def daily_fixture():
    dates=pd.date_range('2026-12-01','2027-04-01')
    return pd.DataFrame([{'shop':s,'spu':p,'ds':d,'y':float(i+1)+offset,'sales_state':'OBSERVED'}
                         for (s,p,offset) in [('A','X',0),('B','X',1000),('A','Y',2000)]
                         for i,d in enumerate(dates)])


class RollingV2Tests(unittest.TestCase):
    def test_delay_weekly_cross_year(self):
        self.assertEqual(str(information_dates('2026-12-31',1,'R28-D')[1].date()),'2026-12-30')
        self.assertEqual(str(information_dates('2026-12-31',6,'R28-W')[0].date()),'2026-12-31')
        self.assertEqual(str(information_dates('2026-12-31',7,'R28-W')[0].date()),'2027-01-07')
        self.assertEqual(str(information_dates('2026-12-31',45,'F28')[1].date()),'2026-12-29')

    def test_boundaries_and_unrealized_only(self):
        s=pd.Timestamp('2026-12-31')
        for r,length in [(0,60),(7,53),(14,46),(21,39),(30,30),(45,15),(59,1)]:
            a,b=target_dates(s,r,'T1');self.assertEqual((b-a).days+1,length)
            self.assertGreater(a,s+pd.Timedelta(days=r));self.assertEqual(b,s+pd.Timedelta(days=60))
        a,b=target_dates(s,0,'T2','d31_60');self.assertEqual(a,s+pd.Timedelta(days=31));self.assertEqual((b-a).days,29)

    def test_missing_day_no_compression_and_zero_valid(self):
        d=daily_fixture();c=CalendarStore(d,'fixture');r,st,_=c.mean28(('A','X'),'2026-12-31')
        self.assertEqual(r,sum(range(4,32))/28);self.assertEqual(st,'OK')
        mask=d.shop.eq('A')&d.spu.eq('X')&d.ds.eq(pd.Timestamp('2026-12-10'))
        removed=CalendarStore(d[~mask],'fixture');self.assertIsNone(removed.mean28(('A','X'),'2026-12-31')[0])
        d.loc[mask,['y','sales_state']]=[0.,'EXPLICIT_ZERO']
        self.assertEqual(CalendarStore(d,'fixture').mean28(('A','X'),'2026-12-31')[1],'OK')
        d.loc[mask,'y']=float('nan');self.assertIsNone(CalendarStore(d,'fixture').mean28(('A','X'),'2026-12-31')[0])

    def test_shop_and_spu_isolation(self):
        c=CalendarStore(daily_fixture(),'fixture');a=c.mean28(('A','X'),'2026-12-31')[0]
        self.assertEqual(c.mean28(('B','X'),'2026-12-31')[0]-a,1000)
        self.assertEqual(c.mean28(('A','Y'),'2026-12-31')[0]-a,2000)

    def test_future_perturbation_and_initial_equivalence(self):
        d=daily_fixture();c=CalendarStore(d,'fixture');cutoff=pd.Timestamp('2026-12-31')
        before=c.mean28(('A','X'),cutoff)
        d.loc[d.ds>cutoff,'y']*=1000
        self.assertEqual(before,CalendarStore(d,'fixture').mean28(('A','X'),cutoff))
        self.assertEqual(information_dates('2027-01-02',0,'F28'),information_dates('2027-01-02',0,'R28-D'))
        self.assertEqual(information_dates('2027-01-02',0,'F28'),information_dates('2027-01-02',0,'R28-W'))

    def test_maturity_window_specific(self):
        c=CalendarStore(daily_fixture(),'fixture')
        self.assertEqual(c.total(('A','X'),'2027-03-02','2027-04-01')[1],'OBSERVED')
        self.assertEqual(c.total(('A','X'),'2027-03-02','2027-04-02')[1],'NOT_MATURED')

    def test_unknowns_and_zero_do_not_pass(self):
        data=[score_row(100.,110.,'OBSERVED','OK'),score_row(100.,None,'OBSERVED','MISSING_INPUT'),
              score_row(None,20.,'NOT_MATURED','OK'),score_row(0.,20.,'OBSERVED','OK')]
        m=aggregate_metrics(pd.DataFrame(data))
        self.assertEqual((m['hit_n'],m['miss_n'],m['unjudged_n']),(1,0,3))
        self.assertEqual(m['hit_rate'],.5);self.assertEqual(m['full_core_hit_share'],.25)
        self.assertEqual(m['zero_actual_absolute_error'],20);self.assertEqual(m['not_matured_n'],1)


if __name__=='__main__':unittest.main()
