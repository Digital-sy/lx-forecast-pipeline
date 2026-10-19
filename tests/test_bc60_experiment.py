import unittest
import numpy as np
import pandas as pd
from scripts.bc60_experiment import select_core,metric,make_segments,lifecycle


class CoreAndMissingTests(unittest.TestCase):
    def test_boundary_ties(self):
        d=pd.DataFrame({'spu':['A','B','C','D'],'selection_qty':[60,20,10,10]})
        self.assertEqual(select_core(d,100).is_core.sum(),4)

    def test_whole_shop_unmapped_denominator(self):
        d=pd.DataFrame({'spu':['A','B'],'selection_qty':[81,9]})
        self.assertTrue(select_core(d,100).is_core.all())

    def test_no_prediction_not_removed_from_hit_denominator(self):
        g=pd.DataFrame({'actual':[100,100,0,np.nan],'prediction':[100,np.nan,5,10]})
        m=metric(g)
        self.assertEqual(m['hit_rate'],.5)
        self.assertEqual(m['hit_sales_share'],.5)
        self.assertEqual(m['fixed_core_n'],4)
        self.assertEqual(m['zero_actual_absolute_error'],5)

    def test_unknown_history_not_imputed(self):
        dates=pd.date_range('2026-01-01',periods=120)
        d=pd.DataFrame({'ds':dates,'shop':'JQ-US','spu':'A','y':1.,'sales_state':'OBSERVED','mapped':True,
            'listing_sessions_sum':10.,'null_sessions':0,'order_qty':1.,'ad_spend':2.,'ad_clicks':3.,'ad_orders':1.})
        d.loc[60,'sales_state']='UNKNOWN';d.loc[60,'y']=np.nan
        train,terminal,ids=make_segments(d,{('JQ-US','A')})
        self.assertEqual(train.unique_id.nunique(),2)
        self.assertEqual(len(terminal),1)
        self.assertFalse(train.y.isna().any())
        self.assertFalse((train.ds==dates[60]).any())
        self.assertTrue(train.groupby('unique_id').ds.diff().dropna().eq(pd.Timedelta(days=1)).all())

    def test_lifecycle_ignores_future(self):
        d=pd.DataFrame({'shop':'JQ-US','spu':'A','month':pd.period_range('2026-01',periods=9,freq='M'),'qty':10})
        x=lifecycle(d,'2026-04-02').iloc[0]
        self.assertEqual(x.positive_months,3)
        self.assertNotEqual(x.lifecycle,'ESTABLISHED')

    def test_synthetic_mlforecast_interface(self):
        from lightgbm import LGBMRegressor
        from mlforecast import MLForecast
        from mlforecast.lag_transforms import RollingMean
        d=pd.DataFrame([{'unique_id':str(i),'ds':day,'y':float(5+i+j%7),'shop_code':i,'x':float(j%5)}
            for i in range(2) for j,day in enumerate(pd.date_range('2026-01-01',periods=100))])
        m=MLForecast(models={'B1':LGBMRegressor(n_estimators=5,n_jobs=2,verbosity=-1,random_state=1)},freq='D',
            lags=[1,7,14,28],lag_transforms={1:[RollingMean(7),RollingMean(28)]},date_features=['dayofweek','month','dayofyear'])
        m.fit(d,static_features=['shop_code'])
        x=pd.DataFrame([{'unique_id':'0','ds':day,'x':1.} for day in pd.date_range(d.ds.max()+pd.Timedelta(days=1),periods=62)])
        f=m.predict(62,ids=['0'],X_df=x,after_predict_callback=lambda a:np.maximum(a,0))
        self.assertEqual(len(f),62)
        self.assertTrue(f.B1.notna().all())


if __name__=='__main__':unittest.main()
