"""No-fit synthetic guards for the frozen direct cumulative experiment."""
import unittest
import tempfile
import ast
import inspect
from pathlib import Path
import numpy as np
import pandas as pd
from scripts.bc60_v3_direct import Context,bounds,completed_month,features_from_history,disjoint_windows,support_status,score,metrics,FEATURES,reserve_fit_run


def fixture():
    dates=pd.date_range('2026-01-01','2026-05-31')
    d=pd.DataFrame([{'shop':shop,'spu':'X','ds':date,'y':i+1+shift,'mapped':True,'sales_state':'OBSERVED'}
        for shop,shift in [('JQ-US',0),('MT-US',1000)] for i,date in enumerate(dates)])
    m=pd.DataFrame([{'shop':shop,'spu':'X','month':month,'qty':10}
        for shop in ['JQ-US','MT-US'] for month in ['2025-09','2025-10','2025-11','2025-12','2026-01','2026-02']])
    return d,m


class DirectTests(unittest.TestCase):
    def test_json_reads_are_explicit_utf8(self):
        import scripts.bc60_v3_direct as module
        tree=ast.parse(inspect.getsource(module))
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='read_text']
        self.assertGreaterEqual(len(calls),6)
        for call in calls:self.assertTrue(any(k.arg=='encoding' and isinstance(k.value,ast.Constant) and k.value.value=='utf-8' for k in call.keywords))

    def test_no_repeat_formal_fit_reservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger=Path(tmp)/'ledger.json'
            reserve_fit_run(ledger,Path(tmp)/'first')
            with self.assertRaises(FileExistsError):reserve_fit_run(ledger,Path(tmp)/'second')

    def test_windows_bridge_and_year(self):
        c,a,b=bounds('2027-01-01','M1');_,a2,b2=bounds('2027-01-01','M2')
        self.assertEqual(c,pd.Timestamp('2026-12-30'))
        self.assertEqual(a,pd.Timestamp('2027-01-02'));self.assertEqual((b-a).days+1,30)
        self.assertEqual(a2,b+pd.Timedelta(days=1));self.assertEqual((b2-a).days+1,60)

    def test_completed_month_leap(self):
        self.assertEqual(str(completed_month('2024-02-28')),'2024-01')
        self.assertEqual(str(completed_month('2024-02-29')),'2024-02')
        self.assertEqual(str(completed_month('2026-12-31')),'2026-12')

    def test_features_exact(self):
        f=features_from_history(np.arange(1,29),0,'2027-01-02')
        self.assertEqual(set(f),set(FEATURES));self.assertEqual([f['lag1'],f['lag7'],f['lag14'],f['lag28']],[28,22,15,1])
        self.assertEqual(f['rolling_mean_lag1_window_size7'],25)
        self.assertEqual(f['rolling_mean_lag1_window_size28'],14.5)
        self.assertEqual(f['dayofweek'],5);self.assertEqual(f['month'],1);self.assertEqual(f['dayofyear'],2)

    def test_actual_and_historical_maturity(self):
        d,m=fixture();ctx=Context(d,m,['JQ-US','MT-US'],'2026-03-31')
        a,_=ctx.training('2026-04-02','M1');b,_=ctx.training('2026-04-02','M2')
        self.assertEqual(a.simulated_issue_date.nunique(),31);self.assertEqual(b.simulated_issue_date.nunique(),1)
        self.assertEqual(a.simulated_issue_date.max(),pd.Timestamp('2026-03-01'))
        self.assertEqual(b.simulated_issue_date.max(),pd.Timestamp('2026-01-30'))
        self.assertTrue(a.label_end.le(ctx.cutoff).all());self.assertTrue(b.label_end.le(ctx.cutoff).all())
        self.assertEqual(support_status(b),'LIMITED_TEMPORAL_SUPPORT')
        self.assertEqual(disjoint_windows(a[a.shop.eq('JQ-US')]),2)

    def test_future_perturbation_and_isolation(self):
        d,m=fixture();ctx=Context(d,m,['JQ-US','MT-US'],'2026-03-31');a,_=ctx.training('2026-04-02','M1')
        changed=d.copy();changed.loc[changed.ds.gt('2026-03-31'),'y']=999999
        m2=m.copy();m2.loc[len(m2)]=['JQ-US','X','2026-04',999]
        other=Context(changed,m2,['JQ-US','MT-US'],'2026-03-31');b,_=other.training('2026-04-02','M1')
        pd.testing.assert_frame_equal(a,b)
        r=ctx.row(('JQ-US','X'),'2026-04-02','M1','2026-04-02')
        s=ctx.row(('MT-US','X'),'2026-04-02','M1','2026-04-02')
        self.assertEqual(s['lag1']-r['lag1'],1000)

    def test_missing_zero_and_mapping(self):
        d,m=fixture();idx=d.index[(d.shop=='JQ-US')&(d.ds==pd.Timestamp('2026-03-20'))][0]
        for kind in ['missing','unknown','unmapped','zero']:
            changed=d.copy()
            if kind=='missing':changed=changed.drop(idx)
            elif kind=='unknown':changed.loc[idx,'sales_state']='UNKNOWN'
            elif kind=='unmapped':changed.loc[idx,'mapped']=False
            else:changed.loc[idx,['sales_state','y']]=['EXPLICIT_ZERO',0]
            ctx=Context(changed,m,['JQ-US','MT-US'],'2026-03-31')
            r=ctx.row(('JQ-US','X'),'2026-04-02','M1','2026-04-02')
            self.assertEqual(r['input_status'],'OK' if kind=='zero' else 'MISSING_HISTORY')

    def test_eligibility_not_backfilled(self):
        d,m=fixture();m=m[m.month.isin(['2025-11','2025-12','2026-01','2026-02'])]
        ctx=Context(d,m,['JQ-US','MT-US'],'2026-03-31')
        self.assertEqual(ctx.qualification(('JQ-US','X'),'2026-02-27'),3)
        self.assertEqual(ctx.qualification(('JQ-US','X'),'2026-02-28'),4)
        a,_=ctx.training('2026-04-02','M2');self.assertTrue(a.empty)
        self.assertEqual(support_status(a),'NOT_TRAINABLE')

    def test_segment_features_share_only_history(self):
        d,m=fixture();ctx=Context(d,m,['JQ-US','MT-US'],'2026-03-31')
        r=ctx.row(('JQ-US','X'),'2026-04-02','M1','2026-04-02')
        s=ctx.row(('JQ-US','X'),'2026-04-02','M2','2026-04-02')
        for f in FEATURES[:-3]:self.assertEqual(r[f],s[f])
        self.assertNotEqual(r['month'],s['month'])
        self.assertNotIn('y_label',r)

    def test_unknown_scoring_and_zero(self):
        self.assertEqual(score(0,0,'OBSERVED','OK')['decision'],'UNJUDGED')
        self.assertEqual(score(10,np.nan,'OBSERVED','MISSING')['decision'],'UNJUDGED')
        self.assertEqual(score(10,12,'OBSERVED','OK')['decision'],'HIT')
        self.assertEqual(score(10,12.00001,'OBSERVED','OK')['decision'],'MISS')
        g=pd.DataFrame([score(0,4,'OBSERVED','OK'),score(10,10,'OBSERVED','OK'),score(20,np.nan,'OBSERVED','MISSING')])
        x=metrics(g);self.assertEqual(x['fixed_n'],3);self.assertEqual(x['zero_actual_absolute_error'],4)
        self.assertEqual(x['hit_sales_share'],1/3)


if __name__=='__main__':unittest.main()
