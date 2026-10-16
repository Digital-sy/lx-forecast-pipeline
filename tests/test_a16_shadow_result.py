"""Synthetic offline tests for A16 one-shot shadow post-run integrity audit."""
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from audit_a16_shadow_result import audit

MODEL = "a16_shop_cutoff_v3_20261016"
DIGEST = "d" * 64

def fixtures():
    models = []
    rows = []
    for h, target in (("H2", "2026-12"), ("H3", "2027-01")):
        models.append({"horizon": h, "target_month": target, "candidate_scope": 2,
                       "eligible_count": 1, "a16_output_count": 1,
                       "v4_output_count": 2, "training_max_target": "2026-09"})
        for spu, eligibility, v4, a16, delta in (
                ("BX517", "ESTABLISHED", "200", "120", "-80"),
                ("BX389", "DORMANT_RETURN", "0", "", "")):
            rows.append({"issue_month": "2026-10", "target_month": target,
                         "horizon": h, "shop": "JQ-US", "spu": spu,
                         "qualification": eligibility,
                         "status": "OUTPUT" if a16 else "INELIGIBLE",
                         "v4_replay": v4, "a16_shadow": a16, "delta": delta,
                         "category": "Dress", "source_hash": DIGEST,
                         "candidate_version": MODEL})
    meta = {"status": "COMPLETE_OBSERVATION_ONLY", "production_modified": False,
            "shop": "JQ-US", "horizons": ["H2", "H3"],
            "published_v4_compared": False, "issue_month": "2026-10",
            "input_hash": DIGEST, "candidate": MODEL, "models": models,
            "output_rows": len(rows)}
    return rows, meta

class ShadowAuditTests(unittest.TestCase):
    def test_valid_output(self):
        rows, meta = fixtures()
        result = audit(rows, meta, "a" * 64)
        for h in ("H2", "H3"):
            self.assertEqual(result["horizons"][h]["net_unit_delta"], -80)
            self.assertEqual(result["horizons"][h]["qualified_spu_count"], 1)
            self.assertEqual(result["horizons"][h]["noneligible_count"], 1)

    def test_bad_delta_fails(self):
        rows, meta = fixtures()
        rows[0]["delta"] = "-79"
        with self.assertRaisesRegex(ValueError, "arithmetic"):
            audit(rows, meta, "a" * 64)

    def test_duplicate_spu_fails(self):
        rows, meta = fixtures()
        rows[1]["spu"] = "BX517"
        with self.assertRaisesRegex(ValueError, "duplicate"):
            audit(rows, meta, "a" * 64)

    def test_wrong_future_target_fails(self):
        rows, meta = fixtures()
        rows[0]["target_month"] = "2026-11"
        with self.assertRaisesRegex(ValueError, "target month"):
            audit(rows, meta, "a" * 64)

    def test_unqualified_output_fails(self):
        rows, meta = fixtures()
        rows[1]["a16_shadow"] = "100"
        with self.assertRaisesRegex(ValueError, "qualification"):
            audit(rows, meta, "a" * 64)

    def test_output_count_and_future_training_fails(self):
        rows, meta = fixtures()
        meta["models"][0]["training_max_target"] = "2026-10"
        with self.assertRaisesRegex(ValueError, "training"):
            audit(rows, meta, "a" * 64)

if __name__ == "__main__":
    unittest.main()
