import unittest
import numpy as np
import pandas as pd
from scripts.direct_f28_cross_diagnostic import classify, normalize_metadata, metric, pair_stats, object_table, conditional
from scripts.direct_f28_cross_report import table
from scripts.direct_f28_cross_report_check import equal


def row(a1=100,a2=100,p1=100,p2=100):
    return dict(a1=a1,a2=a2,a60=a1+a2,p1=p1,p2=p2,p60=p1+p2)


class CrossDiagnosticTests(unittest.TestCase):
    def test_both_hit(self):
        self.assertEqual(classify(row(p1=81,p2=119)),"BOTH_SEGMENTS_HIT")

    def test_total_hit_can_mask_segments(self):
        self.assertEqual(classify(row(p1=50,p2=150)),"CUMULATIVE_HIT_SEGMENTS_MISS")

    def test_high_low_mixed(self):
        self.assertEqual(classify(row(p1=150,p2=150)),"BOTH_OVER_20")
        self.assertEqual(classify(row(p1=50,p2=50)),"BOTH_UNDER_20")
        self.assertEqual(classify(row(p1=100,p2=200)),"MIXED_TOTAL_MISS")

    def test_missing_and_zero(self):
        self.assertEqual(classify(row(a1=0)),"ZERO_ACTUAL")
        self.assertEqual(classify(row(p1=np.nan)),"MISSING_PREDICTION")
        self.assertEqual(classify(row(a1=np.nan,p1=np.nan)),"MISSING_TRUTH")
        self.assertEqual(classify(row(),False),"NOT_MATURED")

    def test_no_rounding(self):
        self.assertEqual(classify(row(p1=120,p2=120)),"BOTH_SEGMENTS_HIT")
        self.assertEqual(classify(row(p1=120.000001,p2=120.000001)),"BOTH_OVER_20")

    def test_label_conflicts_not_last_wins(self):
        d=pd.DataFrame([dict(shop="S",spu="P",current_category=x,current_season="spring")
                        for x in ["top","top","vest"]])
        x=normalize_metadata(d).iloc[0]
        self.assertEqual(x.current_category,"CONFLICT")
        self.assertEqual(x.current_season,"spring")
        self.assertIn("vest",x.current_category_raw_values)

    def test_raw_category_no_alias_merge(self):
        d=pd.DataFrame([dict(shop="S",spu="P",current_category=x,current_season="")
                        for x in ["vest","vest/top"]])
        x=normalize_metadata(d).iloc[0]
        self.assertEqual(x.current_category,"CONFLICT")
        self.assertEqual(x.current_season,"UNKNOWN")

    @staticmethod
    def frame():
        return pd.DataFrame(dict(shop=["S"]*3,spu=["a","b","c"],issue_date=["2025-01-02"]*3,
            issue_month=["2025-01"]*3,calendar_month=["01"]*3,maturity=["4_5"]*3,
            current_category=["top"]*3,current_season=["spring"]*3,window=["d01_60"]*3,
            actual=[100.,100.,0.],f28_prediction=[200.,100.,10.],
            direct_prediction=[150.,np.nan,0.],paired=[True,False,True]))

    def test_missing_output_kept_in_hit_denominator(self):
        d=self.frame()
        m=metric(d,"direct")
        self.assertEqual(m["positive_truth_n"],2)
        self.assertEqual(m["output_n"],2)
        self.assertEqual(m["n"],3)
        self.assertEqual(m["zero_actual_absolute_error"],0)
        self.assertEqual(m["wape"],.5)

    def test_unknown_not_zero(self):
        d=self.frame()
        d["actual"]=np.nan
        m=metric(d,"direct")
        self.assertTrue(np.isnan(m["actual_units"]))
        self.assertTrue(np.isnan(m["wape"]))

    def test_pair_denominator(self):
        d=self.frame()
        s=pair_stats(d)
        self.assertEqual(s["pair_n"],2)
        self.assertEqual(s["excluded_pair_n"],1)
        self.assertEqual(s["pair_actual_units"],100)
        self.assertEqual(s["improved_n"],2)

    def test_unmapped_raw_label_is_retained_but_not_known_category(self):
        d=self.frame()
        d["current_category"]="未映射"
        m=metric(d,"f28")
        self.assertEqual(m["category_known_n"],0)
        self.assertEqual(m["category_unknown_n"],3)
        meta=normalize_metadata(d[["shop","spu","current_category","current_season"]])
        self.assertTrue(meta.current_category.eq("未映射").all())

    def test_standardization_same_weights(self):
        # Two categories co-exist in one true stratum; no fictitious second stratum.
        d=self.frame().iloc[[0]].copy()
        c=d.copy()
        c["spu"]="d";c["current_category"]="vest";c["actual"]=300.
        c["f28_prediction"]=600.;c["direct_prediction"]=300.
        q=pd.concat([d,c],ignore_index=True)
        summaries,cells=conditional(q)
        s=summaries[summaries.comparison.eq("category")].iloc[0]
        self.assertEqual(s.strata_n,1)
        self.assertEqual(s.direct_wape_a,.5)
        self.assertEqual(s.direct_wape_b,0)
        self.assertEqual(cells[cells.comparison.eq("category")].weight.sum(),1)

    def test_no_common_strata_no_fabrication(self):
        d=self.frame().iloc[[0]].copy()
        c=d.copy();c["spu"]="d";c["shop"]="T";c["current_category"]="vest"
        s,_=conditional(pd.concat([d,c],ignore_index=True))
        r=s[s.comparison.eq("category")].iloc[0]
        self.assertEqual(r.status,"NO_COMMON_POSITIVE_STRATA")
        self.assertEqual(r.kept_a_n,0)

    def test_report_unknown_is_dash_not_zero(self):
        text=table(pd.DataFrame({"unknown":[np.nan],"known_zero":[0.]}),["unknown","known_zero"])
        self.assertIn("| — | 0 |",text)

    def test_report_checker_rejects_zero_for_unknown(self):
        equal(np.nan,np.nan)
        with self.assertRaises(AssertionError):
            equal(0.,np.nan)


if __name__=="__main__":
    unittest.main()
