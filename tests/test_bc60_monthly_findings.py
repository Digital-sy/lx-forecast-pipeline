"""Synthetic calculations only; no archive reads, effects, DB or model fits."""
import unittest
import json
from pathlib import Path
import tempfile
import numpy as np
import pandas as pd
from scripts import bc60_monthly_findings as f


class FindingsTests(unittest.TestCase):
    def scores(self):
        return pd.DataFrame([{'cohort_id':'monthly_core','issue_date':'2025-01-02','issue_month':'2025-01',
            'shop':'synthetic','spu':str(i),'window':'d01_60','model':m,'actual':100.,'prediction':p,
            'lifecycle':'ESTABLISHED','maturity':'MATURE'}
            for m,pred in [('F28',[100.,100.]),('B1',[110.,np.nan])] for i,p in enumerate(pred)])

    def test_pair_retains_full_population_and_uses_equal_scored_denominator(self):
        r=f.paired_results(self.scores()).iloc[0]
        self.assertEqual(r.full_core_issue_n,2);self.assertEqual(r.paired_n,1)
        self.assertEqual(r.candidate_actual_units,r.f28_actual_units)
        self.assertAlmostEqual(r.wape_change,.1)

    def test_missing_prediction_counts_against_full_positive_hit_denominator(self):
        r=f.metric(self.scores().query("model=='B1'"))
        self.assertEqual(r['core_issue_n'],2);self.assertEqual(r['scored_n'],1)
        self.assertEqual(r['hit_rate'],.5)

    def test_shape_boundary_level_and_zero_are_distinct(self):
        rows=[]
        for i,(a,b,p) in enumerate([(100,150,120),(100,151,120),(100,100,70),(100,100,130),(0,100,10)]):
            rows.append({'issue_date':'2025-01-02','shop':'synthetic','spu':str(i),'a1':a,'a2':b,
                'p1':p,'p2':p,'p60':2*p,'mean28':p/30})
        monthly,classes,examples,audit=f.shape_results(pd.DataFrame(rows))
        self.assertEqual(monthly.iloc[0].incompatible_n,1)
        self.assertEqual(set(classes.error_structure),{'BOTH_SEGMENTS_HIT','INCOMPATIBLE_EQUAL_SEGMENT_SHAPE',
            'COMPATIBLE_SHAPE_LEVEL_TOO_LOW','COMPATIBLE_SHAPE_LEVEL_TOO_HIGH','ZERO_ACTUAL_SEPARATE'})
        self.assertAlmostEqual(audit['max_formula_absolute_difference'],0.)

    def test_operating_zero_baseline_is_not_infinite_ratio(self):
        s=self.scores();op=s[s.model.eq('F28')][f.KEY].copy()
        op['ad_spend_input_daily_mean']=[0.,2.];op['ad_spend_target_daily_mean']=[10.,4.]
        cover,groups,_=f.operating_results(s,op)
        r=cover[cover.indicator.eq('ad_spend')].iloc[0]
        self.assertEqual(r.known_before_after_n,2);self.assertEqual(r.positive_baseline_ratio_n,1)
        self.assertEqual(r.median_change_ratio,2.)
        self.assertIn('ZERO_BASELINE_NO_RATIO',set(groups[groups.indicator.eq('ad_spend')].direction))

    def test_missing_current_labels_remain_unknown_not_deleted(self):
        s=self.scores();meta=pd.DataFrame({'shop':['synthetic'],'spu':['0'],
            'current_category':['example'],'current_season':[None]})
        summary,coverage=f.metadata_results(s,meta)
        self.assertEqual(coverage[coverage.dimension.eq('current_category')].iloc[0].current_label_known_n,1)
        self.assertEqual(coverage[coverage.dimension.eq('current_season')].iloc[0].current_label_unknown_n,2)
        self.assertIn('UNKNOWN',set(summary.group_value))

    def test_maturity_requires_whole60day_issue(self):
        s=self.scores();s.loc[0,'issue_date']='2026-09-02'
        self.assertEqual(len(f.mature(s)),len(s)-1)

    def test_unknown_shape_actual_not_aggregated_into_zero(self):
        source=pd.DataFrame([{'issue_date':'2025-01-02','shop':'synthetic','spu':'x','a1':np.nan,'a2':10.,
            'p1':20.,'p2':20.,'p60':40.,'mean28':20/30}])
        _,classes,_,_=f.shape_results(source)
        self.assertEqual(classes.iloc[0].error_structure,'UNKNOWN_ACTUAL')
        self.assertEqual(classes.iloc[0].known_actual_n,0)
        self.assertTrue(np.isnan(classes.iloc[0].actual_units))

    def test_native_old_missing_months_are_not_new_v4_unavailability(self):
        native={'monthly_available_targets':['2025-10'],'monthly_unavailable_targets':['2025-01']}
        self.assertIn('保持待定',f.native_note(native))
        done=f.native_note(native,{'status':'COMPLETE'})
        self.assertIn('扩展已完成',done)
        self.assertIn('不得继续把旧',done)

    def test_full_offline_findings_build_with_synthetic_inputs(self):
        with tempfile.TemporaryDirectory(prefix='bc60_findings_synthetic_') as directory:
            root=Path(directory)
            for name in ['scoring','diagnostics','effects','side_panels']:(root/name).mkdir()
            def save(name,frame):
                if name.endswith('.parquet'):frame.to_parquet(root/name,index=False)
                else:frame.to_csv(root/name,index=False)
            scores=self.scores();base=scores[scores.model.eq('F28')][f.KEY].copy()
            save('scoring/scores.parquet',scores)
            joints=scores.drop(columns=['actual','prediction','window']).assign(decision='HIT')
            save('scoring/joint_hits.csv',joints)
            shapes=base.assign(mean28=50/30,p1=50.,p2=50.,p60=100.,a1=50.,a2=50.,shape_ratio=1.,first_level_ratio=1.,second_level_ratio=1.)
            save('scoring/f28_shape_details.csv',shapes)
            save('scoring/operating_indicators_diagnostic.csv',base.assign(ad_spend_input_daily_mean=1.,ad_spend_target_daily_mean=2.))
            extreme=pd.DataFrame([{'model':'B1','window':'d01_60','criterion':'wape','kind':'WORST',
                'issue_month':'2025-01','value':.1}])
            save('diagnostics/best_worst_months.csv',extreme)
            save('diagnostics/season_history_coverage.csv',base.assign(complete_365_days=[True,False],known_history_365_days=[365,100]))
            survivor=scores.assign(comparison='ADJACENT_ISSUES',previous_actual=100.,previous_prediction=100.)
            save('diagnostics/survivor_adjacent_yoy.parquet',survivor)
            save('side_panels/current_metadata_mapping.csv',base[['shop','spu']].assign(current_category='synthetic',current_season='UNKNOWN'))
            save('effects/fit_status.csv',pd.DataFrame([{'issue_date':'2025-01-02','kind':'B1','status':'SYNTHETIC_NO_FIT',
                'available_calendar_days':366,'effective_label_dates':338,'train_objects':2}]))
            (root/'side_panels/side_panels_manifest.json').write_text(json.dumps({
                'monthly_available_targets':['2025-10'],'monthly_unavailable_targets':['2025-01']}),encoding='utf-8')
            f.run(root,root/'findings')
            manifest=json.loads((root/'findings/findings_manifest.json').read_text('utf-8'))
            self.assertEqual(manifest['status'],'COMPLETE');self.assertEqual(manifest['fits'],0)
            self.assertTrue(manifest['source_unchanged'])
            self.assertEqual(len(list((root/'findings').glob('*.csv'))),14)
            text=(root/'findings/实数解释补充.md').read_text('utf-8')
            self.assertIn('不同行的paired_n仍可能不同',text)
            self.assertIn('365日源记录完整的描述性子集',text)
            self.assertIn('包含来源明确记录的0',text)
            self.assertIn('不证明365天持续在售、完整经营季节周期或历史供给可比',text)
            self.assertIn('货龄和原资格另列',text)


if __name__=='__main__':unittest.main()
