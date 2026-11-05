# Restore the Lingxing color reference

The named-color fabric-detail writer retains the separate `颜色-领星` text field.
Its value uses the existing parser on `lxpm_product_category_snapshot.product_name`:
the first numeric `#` color segment, with whitespace removed. Lookup priority is
the full fabric-color key followed by the legacy fabric-code/color-code key.
Unknowns and total rows remain blank. This is a source-name reference, not a
replacement for the A2023/B2024 mapping catalog or inventory ownership rules.
The legacy fallback does not prove cross-system color equivalence.

The optional SKU list limits source reads using parameterized primary-key
lookups in batches of 500. Existing no-argument reader callers keep their
previous behavior. No database schema or scheduling changes are required.
Existing production coverage log wording is preserved.

Offline verification (standard library only, no production imports):

```sh
python tests/test_restore_lingxing_color.py -v
```

The function-only fixtures provide the prior schema and record behavior.
Tests compare all previous fields and numerical values, priority, missing/total
rows, actual writer dispatch and bounded reader behavior. Network/database
clients are explicit test doubles. This command does not publish Feishu data.

Existing-table restoration must back up the schema and records, create only the
missing text field, and update that field by existing record ID. Do not run the
full procurement pipeline for a column backfill. Read back every row and compare
all original fields and record IDs. Deploy only reviewed files after checking
their current hashes and preserving dirty production changes and backups.
