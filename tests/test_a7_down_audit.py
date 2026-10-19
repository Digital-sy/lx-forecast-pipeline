"""Synthetic tests only; no customer inputs, network, or database."""
import ast
from pathlib import Path
import unittest
from scripts.audit_a7_down_batch import aggregate,diagnostic_flags,shift,run


class DownAuditTests(unittest.TestCase):
    def test_denominators_and_serial_stages(self):
        rows=[dict(a3=100,seasonal_candidate=20,a7=20,a16=20,a7_rule='DOWN',gamma_down=1),
              dict(a3=100,seasonal_candidate=150,a7=120,a16=110,a7_rule='UP_RISING',gamma_down=1)]
        a=aggregate(rows)
        self.assertEqual(a['gross_a7_reduction'],80)
        self.assertEqual(a['a7_net_change'],-60)
        self.assertEqual(a['a16_net_change'],-10)
        self.assertEqual(a['down_reduction_contribution'],1)
        self.assertEqual(a['down_a3_quantity_share'],.5)
        self.assertAlmostEqual(a['down_final_quantity_share'],20/130)
        self.assertEqual(a['a7_net_change']+a['a16_net_change'],a['a16']-a['a3'])

    def test_missing_candidate_not_zero(self):
        a=aggregate([dict(a3=1,seasonal_candidate=None,a7=1,a16=1,a7_rule='A3_ONLY',gamma_down=1)])
        self.assertEqual(a['seasonal_known_objects'],0)
        self.assertIsNone(a['down_reduction_contribution'])

    def test_missing_reference_not_low_or_zero(self):
        a=diagnostic_flags({},'JQ-US|S',shift('2026-10',0),shift('2026-12',0),{})
        self.assertTrue(a['ly_target_missing'])
        self.assertFalse(a['ly_target_zero'])
        self.assertFalse(a['ly_target_low_base'])
        self.assertIsNone(a['descriptive_trough_flag'])
        self.assertIsNone(a['sku_composition_change_flag'])

    def test_no_retraining_or_database_calls(self):
        tree=ast.parse(Path('scripts/audit_a7_down_batch.py').read_text(encoding='utf-8'))
        attrs={n.func.attr for n in ast.walk(tree)if isinstance(n,ast.Call)and isinstance(n.func,ast.Attribute)}
        self.assertTrue({'predict','choose_params','execute','compute_shadow'}.isdisjoint(attrs))

    def test_original_directory_is_refused(self):
        with self.assertRaises(ValueError):run('absent','.','.')


if __name__=='__main__':unittest.main()
