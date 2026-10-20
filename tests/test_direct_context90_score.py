"""Denominator and joint-hit unit tests, without business data."""
from pathlib import Path
import sys
import unittest
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.direct_context90_score import metric, paired_objects, joint_summary, WINDOWS


class ScoringTests(unittest.TestCase):
    def test_missing_and_zero_are_not_hits(self):
        d = pd.DataFrame({"shop": ["S"]*4, "spu": list("abcd"), "issue_date": ["2025-01-02"]*4,
                          "actual": [100., 100., 0., np.nan], "prediction": [120., np.nan, 10., 0.]})
        m = metric(d)
        self.assertEqual(m["fixed_n"], 4)
        self.assertEqual(m["hit_n"], 1)
        self.assertEqual(m["hit_fixed_share"], .25)
        self.assertEqual(m["hit_actual_share"], .5)
        self.assertEqual(m["zero_truth_ae"], 10)
        self.assertEqual(m["over_rate"], .3)

    def test_joint_count_once_and_compensation_not_joint_hit(self):
        rows = []
        for product, forecast in [("A", [100, 100, 200]), ("B", [70, 130, 200])]:
            for w, p, a in zip(WINDOWS, forecast, [100, 100, 200]):
                rows.append(dict(issue_date="2025-01-02", shop="S", spu=product, model="TEST",
                                 window=w, prediction=p, actual=a))
        obj = paired_objects(pd.DataFrame(rows))
        j = joint_summary(obj)
        self.assertEqual(j["joint_hit_n"], 1)
        self.assertEqual(j["joint_hit_actual"], 200)
        self.assertEqual(j["joint_hit_actual_share"], .5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
