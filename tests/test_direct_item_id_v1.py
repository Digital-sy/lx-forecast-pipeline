import json
from pathlib import Path
import tempfile
import unittest
import numpy as np
import pandas as pd
from scripts.direct_item_id_v1 import dictionary,encode,support,model_metadata,FEATURES,CAT_DEFAULTS,direct


def frame():
    return pd.DataFrame({'shop':['B','A','A','B'],'spu':['SAME','OTHER','SAME','SAME'],
        'y_label':[9.,2.,7.,1.],'old':np.array([1,2,3,4],dtype=np.float32)})


class IdentityTests(unittest.TestCase):
    def test_dictionary_from_training_only(self):
        t=frame();cat=dictionary(t);i=pd.DataFrame({'shop':['A','NEW'],'spu':['SAME','FUTURE']})
        self.assertEqual(set(map(tuple,cat[['shop','spu']].to_numpy())),set(map(tuple,t[['shop','spu']].to_numpy())))
        self.assertEqual(encode(i,cat).item_key.iloc[1],-1)
        self.assertNotIn(('NEW','FUTURE'),set(map(tuple,cat[['shop','spu']].to_numpy())))

    def test_labels_and_row_order_do_not_change_dictionary(self):
        a=frame();b=a.iloc[::-1].copy();b.y_label=[999,0,500,1]
        pd.testing.assert_frame_equal(dictionary(a),dictionary(b),check_exact=True)

    def test_same_spu_different_shops(self):
        z=encode(frame(),dictionary(frame()))
        self.assertNotEqual(z.item_key.iloc[0],z.item_key.iloc[2])
        self.assertEqual(z.item_key.iloc[0],z.item_key.iloc[3])

    def test_no_separator_collision(self):
        a=pd.DataFrame({'shop':['A|B','A'],'spu':['C','B|C']})
        self.assertEqual(len(dictionary(a)),2)

    def test_unknown_does_not_borrow_known_code_zero(self):
        c=dictionary(frame());z=encode(pd.DataFrame({'shop':['MISSING','A'],'spu':['SAME','OTHER']}),c)
        self.assertEqual(z.item_key.tolist(),[-1,0]);self.assertEqual(str(z.item_key.dtype),'int32')

    def test_old_columns_and_order_unchanged(self):
        a=frame();b=encode(a,dictionary(a));pd.testing.assert_frame_equal(a,b[a.columns],check_exact=True)
        self.assertEqual(FEATURES,direct.FEATURES+['item_key']);self.assertEqual(len(FEATURES),11)

    def test_no_overwrite(self):
        a=frame();a['item_key']=0
        with self.assertRaises(ValueError):encode(a,dictionary(a))

    def test_invalid_identity_rejected(self):
        for val in [None,'',42]:
            a=frame();a.loc[0,'spu']=val
            with self.assertRaises(ValueError):dictionary(a)

    def test_bad_dictionary_rejected(self):
        a=frame();c=dictionary(a);c.loc[1,'item_key']=0
        with self.assertRaises(ValueError):encode(a,c)

    def test_support_counts_overlapping_windows_once(self):
        t=pd.DataFrame({'shop':['A']*61,'spu':['P']*61,'simulated_issue_date':pd.date_range('2020-01-01',periods=61)})
        t['label_start']=t.simulated_issue_date+pd.Timedelta(days=1);t['label_end']=t.simulated_issue_date+pd.Timedelta(days=30)
        inf=pd.DataFrame({'shop':['A','B'],'spu':['P','Q'],'issue_date':['2020-05-01']*2,'segment':['M1']*2,
            'item_key':[0,-1],'input_status':['OK','OK'],'positive_completed_months':[8,8]})
        s=support(t,inf)
        self.assertEqual(s.training_dates.tolist(),[61,0]);self.assertEqual(s.nonoverlap_windows.tolist(),[3,0])
        self.assertEqual(s.training_span_days.tolist(),[61,0]);self.assertIsNone(s.training_first.iloc[1])

    def test_post_cutoff_perturbation(self):
        dates=pd.date_range('2020-01-01',periods=220)
        d=pd.DataFrame({'ds':dates,'shop':'A','spu':'P','y':np.arange(220)+1.,'mapped':True,'sales_state':'OBSERVED'})
        m=pd.DataFrame({'shop':['A']*7,'spu':['P']*7,'month':pd.date_range('2020-01-01',periods=7,freq='MS'),'qty':[10]*7})
        issue=pd.Timestamp('2020-07-01');c=issue-pd.Timedelta(days=2)
        before=direct.Context(d,m,['A'],c).row(('A','P'),issue,'M1',issue)
        d.loc[d.ds.gt(c),'y']=9999999
        after=direct.Context(d,m,['A'],c).row(('A','P'),issue,'M1',issue)
        self.assertEqual(before,after)

    def test_metadata_requires_only_identity_categorical(self):
        class Model:
            def feature_name(self):return FEATURES
            def dump_model(self):return {'feature_infos':{'item_key':{'values':[-1,0,1]}},'tree_info':[
                {'tree_structure':{'split_index':0,'split_feature':10,'decision_type':'==','left_child':{},'right_child':{}}}]}
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'model.txt';p.write_text('[categorical_feature: 10]\n'+''.join(f'[{k}: {v}]\n' for k,v in CAT_DEFAULTS.items()))
            self.assertEqual(model_metadata(Model(),p)['item_categorical_splits'],1)
            p.write_text(p.read_text().replace('categorical_feature: 10','categorical_feature: 0 10'))
            with self.assertRaises(ValueError):model_metadata(Model(),p)


if __name__=='__main__':unittest.main()
