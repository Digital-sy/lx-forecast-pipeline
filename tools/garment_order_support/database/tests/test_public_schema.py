"""Public-copy schema pin and scope checks; no MySQL connection or writes."""
import hashlib
from pathlib import Path
import sys
import unittest

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE / 'scripts'))
import build_load_package as builder
import mysql_load_core as loader
import schema_ops

DDL = BASE / 'sql/lingxing_garment_order_schema_v1_1.sql'

class PublishedSchemaTests(unittest.TestCase):
    def test_all_components_pin_public_bytes(self):
        actual = hashlib.sha256(DDL.read_bytes()).hexdigest()
        self.assertEqual(actual, builder.DDL_SHA256)
        self.assertEqual(actual, loader.DDL_SHA256)
        self.assertEqual(actual, schema_ops.DDL_HASH)

    def test_complete_contract_and_safe_test_schema(self):
        raw = DDL.read_bytes()
        schema = builder.parse_schema(raw.decode('utf-8-sig'))
        self.assertEqual(tuple(schema), builder.TABLES)
        self.assertEqual(builder.TABLES, loader.TABLES)
        self.assertEqual(builder.TABLES, schema_ops.TABLES)
        self.assertEqual(sum(len(v['columns']) for v in schema.values()), 195)
        statements = schema_ops.statements(raw, 'go_test_public_copy')
        self.assertEqual(len(statements), 9)
        self.assertTrue(all('CREATE TABLE `go_test_public_copy`.' in sql for _,sql in statements))
        self.assertTrue(all('`lingxing`.' not in sql for _,sql in statements))

    def test_unreviewed_ddl_bytes_fail_before_execution(self):
        with self.assertRaisesRegex(ValueError, 'DDL_HASH_CHANGED'):
            schema_ops.statements(DDL.read_bytes() + b'\n', 'go_test_public_copy')

    def test_unapproved_schema_fails(self):
        for target in ('mysql', 'information_schema', 'go_test_bad-name', 'other_production'):
            with self.subTest(target=target), self.assertRaisesRegex(ValueError, 'SCHEMA_NOT_ALLOWED'):
                schema_ops.statements(DDL.read_bytes(), target)

if __name__ == '__main__':
    unittest.main()
