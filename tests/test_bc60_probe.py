import unittest
from scripts.bc60_readonly_probe import validate, scan_rows


class ProbeSafety(unittest.TestCase):
    def test_select(self):
        validate("SELECT dt FROM dwd_db.dwd_sale_product_performance_daily WHERE dt='2026-01-01' LIMIT 5")

    def test_unsafe(self):
        for sql in ['DELETE FROM t', 'SELECT * FROM lingxing.secret WHERE 1=1',
                    'SELECT * FROM dwd_db.dwd_sale_product_performance_daily',
                    "SELECT SLEEP(1) FROM dwd_db.dwd_sale_product_performance_daily WHERE 1=1"]:
            with self.assertRaises(ValueError):
                validate(sql)

    def test_scan(self):
        self.assertEqual(scan_rows({'a':[{'rows_examined_per_scan':12}, {'rows_examined_per_scan':3}]}), 15)


if __name__ == '__main__':
    unittest.main()
