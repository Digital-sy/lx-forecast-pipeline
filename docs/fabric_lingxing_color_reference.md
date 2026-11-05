# System-aware Lingxing fabric color reference

This supersedes the equal-abbreviation fallback in `restore_feishu_lingxing_color.md`.
That fallback loses the distinction between A2023 and B2024: an old code may be
missing from current Lingxing SKUs or may identify an entirely different color.

Use the existing manually maintained fabric inventory source, including rows
whose stock is zero. Each physical source record contains an A2023 old code
(`颜色缩写`) and a B2024 current code (`领星新颜色缩写`). Its
`面料编号-颜色缩写` formula uses that new code. Verify the complete suffix and
remove only that suffix to retain the complete fabric identifier.

Resolve `(fabric identifier, color system, code)` only when exactly one source
record supports it, then look up that record's exact current Lingxing SKU in
the existing product snapshot and use the existing number#color parser.
Do not use current Chinese display names, same-code cross-system fallback,
compound-code splitting, or stock-positive filtering. Different fabrics and
their optimized variants remain separate.

Unknown system, missing fabric ID, missing source color, absent alias or alias
conflict stays blank. The pure resolver returns an explicit reason and source
record/SKU for local audit. These are independent of the standard catalog's
color mapping status. No inventory, quantity calculation, forecast model,
database schema, BI dataset or schedule changes are included.

The named-color writer reads the existing approved Feishu inventory source
through the current client, then performs bounded SKU queries. Empty inventory,
no usable aliases or a completely unavailable color source aborts before record
deletion. No production pipeline needs to be rerun for a one-column repair.

Offline tests (no production imports or connections):

```sh
python -m unittest discover -s tests -p 'test_*color*reference*.py' -v
python tests/test_restore_lingxing_color.py -v
```

The existing equal-abbreviation writer assertions are intentionally superseded;
the unchanged bounded-reader tests remain. Production record repair updates
only `颜色-领星` by existing record ID, preserves other fields and view layouts,
and requires full readback plus source and previous-value backups.
