"""Synthetic no-I/O risk tests for JQ-US A16 manual shadow outputs."""
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"scripts"))
sys.path.insert(0, str(ROOT/"tests"))
from audit_a16_shadow_zero_risk import diagnose
from test_a16_shadow_result import fixtures

class RiskTests(unittest.TestCase):
    def test_default_positive_to_positive(self):
        rows, meta = fixtures()
        result = diagnose(rows, meta, "a"*64)
        self.assertEqual(result["status"], "PASS_OFFLINE_RISK_DIAGNOSTIC_ONLY")
        self.assertEqual(result["horizons"]["H2"]["zero_transition"]["both_positive"], 1)
        self.assertEqual(result["cross_horizon"]["common_eligible"], 1)
        self.assertEqual(result["cross_horizon"]["a16_same_positive"], 1)

    def test_true_zero_transitions_not_net_count(self):
        rows, meta = fixtures()
        rows[0]["a16_shadow"] = "0"
        rows[0]["delta"] = "-200"
        rows[2]["v4_replay"] = "0"
        rows[2]["a16_shadow"] = "100"
        rows[2]["delta"] = "100"
        result = diagnose(rows, meta, "a"*64)
        h2 = result["horizons"]["H2"]
        h3 = result["horizons"]["H3"]
        self.assertEqual(h2["zero_transition"]["positive_to_zero"], 1)
        self.assertEqual(h2["v4_units_lost_when_a16_zero"], 200)
        self.assertEqual(h3["zero_transition"]["zero_to_positive"], 1)
        self.assertEqual(h3["new_a16_units_when_v4_zero"], 100)
        self.assertEqual(h2["positive_to_zero_v4_ge100"], 1)

    def test_reject_tampered_record(self):
        rows, meta = fixtures()
        rows[0]["delta"] = "999"
        with self.assertRaisesRegex(ValueError, "arithmetic"):
            diagnose(rows, meta, "a"*64)

    def test_different_horizon_eligibility_is_partial(self):
        rows, meta = fixtures()
        rows[2]["spu"] = "BX515"
        result = diagnose(rows, meta, "a"*64)
        self.assertEqual(result["status"], "PARTIAL_ELIGIBLE_SET_CHANGED")
        self.assertEqual(result["cross_horizon"]["common_eligible"], 0)

if __name__ == "__main__":
    unittest.main()
