import unittest
import numpy as np
import pandas as pd
from scripts.direct_traffic_state_v1 import TrafficIndex, augment, supported, NUMERIC, EXTRA, FEATURES, direct


def daily(n=80):
    return pd.DataFrame(dict(ds=pd.date_range('2020-01-01',periods=n),shop='TEST',spu='SYNTHETIC',
        y=np.arange(n,dtype=float)+1,mapped=True,sales_state='OBSERVED',listing_sessions_sum=10.,
        null_sessions=0,null_qty=0,negative_qty=0,date_mismatch=0,deleted_rows=0,source_rows=1))


def values(d,c):return TrafficIndex(d).at(('TEST','SYNTHETIC'),[pd.Timestamp(c)])[0].iloc[0]


class TrafficTests(unittest.TestCase):
    def test_exact_windows(self):
        d=daily();x=values(d,d.ds.iloc[49])
        self.assertEqual(x.traffic_t7,10);self.assertEqual(x.traffic_t28,10)
        self.assertEqual(x.yield_r7,d.y.iloc[43:50].sum()/70)
        self.assertEqual(x.yield_r28,d.y.iloc[22:50].sum()/280)
        self.assertEqual(x.traffic_change,0)

    def test_yield_greater_than_one_allowed(self):
        self.assertGreater(values(daily(), '2020-02-19').yield_r7,1)

    def test_zero_sessions_undefined_yield(self):
        d=daily();d['listing_sessions_sum']=0.;x=values(d,d.ds.iloc[-1])
        self.assertEqual(x.traffic_t28,0);self.assertEqual(x.traffic_t28_valid,1)
        self.assertTrue(np.isnan(x.yield_r28));self.assertTrue(np.isnan(x.traffic_change))
        self.assertEqual(x.traffic_change_valid,0)

    def test_zero_sales_yield_known_change_undefined(self):
        d=daily();d['y']=0;d['sales_state']='EXPLICIT_ZERO';x=values(d,d.ds.iloc[-1])
        self.assertEqual(x.yield_r7,0);self.assertEqual(x.yield_r7_valid,1)
        self.assertTrue(np.isnan(x.yield_change))

    def test_missing_day_not_replaced_by_older_day(self):
        d=daily().drop(index=70);x=values(d,'2020-03-20')
        self.assertEqual(x.traffic_t7,10);self.assertTrue(np.isnan(x.traffic_t28))
        self.assertTrue(np.isnan(x.yield_r28));self.assertTrue(np.isnan(x.traffic_change))

    def test_null_component_blocks_nonnull_sum(self):
        d=daily();d.loc[79,'null_sessions']=1;x=values(d,d.ds.iloc[-1])
        self.assertTrue(np.isnan(x.traffic_t7));self.assertEqual(x.traffic_t7_valid,0)

    def test_unknown_null_count(self):
        d=daily();d.loc[79,'null_sessions']=np.nan
        self.assertTrue(np.isnan(values(d,d.ds.iloc[-1]).traffic_t7))

    def test_invalid_session_values(self):
        for val in [np.nan,-1,np.inf]:
            d=daily();d.loc[79,'listing_sessions_sum']=val
            self.assertTrue(np.isnan(values(d,d.ds.iloc[-1]).traffic_t7))

    def test_mapping_and_range_flags(self):
        for col,value in [('mapped',False),('date_mismatch',1),('deleted_rows',1),('source_rows',0)]:
            d=daily();d.loc[79,col]=value
            self.assertTrue(np.isnan(values(d,d.ds.iloc[-1]).traffic_t7))

    def test_invalid_sales_do_not_invent_ratio(self):
        for col,value in [('y',-1),('y',np.nan),('null_qty',1),('negative_qty',1),('sales_state','UNKNOWN')]:
            d=daily();d.loc[79,col]=value;x=values(d,d.ds.iloc[-1])
            self.assertTrue(np.isnan(x.yield_r7));self.assertEqual(x.traffic_t7,10)

    def test_future_perturbation(self):
        d=daily();cut=d.ds.iloc[49];a=values(d,cut)
        d.loc[d.ds.gt(cut),'listing_sessions_sum']=999999.;d.loc[d.ds.gt(cut),'y']=999999.
        pd.testing.assert_series_equal(a,values(d,cut),check_exact=True)

    def test_each_training_row_uses_own_cutoff(self):
        d=daily();index=TrafficIndex(d)
        f=pd.DataFrame(dict(shop=['TEST']*2,spu=['SYNTHETIC']*2,input_cutoff=[d.ds.iloc[29],d.ds.iloc[59]],old_float=np.array([1.2,3.4],dtype=np.float32),y_label=[100,200]))
        full,_=augment(index,f)
        pd.testing.assert_frame_equal(full[f.columns],f,check_exact=True)
        self.assertNotEqual(full.yield_r28.iloc[0],full.yield_r28.iloc[1])
        self.assertEqual(full.yield_r28.iloc[0],values(d,d.ds.iloc[29]).yield_r28)

    def test_duplicate_grain_rejected(self):
        d=daily()
        with self.assertRaises(ValueError):TrafficIndex(pd.concat([d,d.iloc[:1]]))

    def test_unknown_object_and_left_boundary(self):
        index=TrafficIndex(daily());f,_=index.at(('TEST','ABSENT'),['2020-02-01'])
        self.assertTrue(f[NUMERIC].isna().all().all())
        x=values(daily(),'2020-01-10');self.assertTrue(np.isnan(x.traffic_t28));self.assertEqual(x.traffic_t7,10)

    def test_support_requires_varying_numeric_information(self):
        f=pd.DataFrame({c:[np.nan,np.nan] for c in NUMERIC});self.assertEqual(supported(f),[])
        f['traffic_t7']=[10,10];self.assertEqual(supported(f),[])
        f['traffic_t7']=[10,11];self.assertEqual(supported(f),['traffic_t7'])

    def test_original_feature_order_and_only_approved_additions(self):
        self.assertEqual(FEATURES[:len(direct.FEATURES)],direct.FEATURES)
        self.assertEqual(len(NUMERIC),6);self.assertEqual(len(EXTRA),12)
        self.assertEqual(len(set(FEATURES)),len(FEATURES))


if __name__=='__main__':unittest.main()
