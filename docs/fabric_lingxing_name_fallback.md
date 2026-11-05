# Fabric color name fallback

This extends `fabric_lingxing_color_reference.md` for the independent
`颜色-领星` reference column. It does not change inventory allocation, quantity
calculation, the color catalog or the stored color-system classification.

1. Keep unique physical-record code mappings first. Never replace a successful
   code mapping, bypass its alias conflict, or switch away from its known SKU
   merely because the source color is missing.
2. For an unmapped alias or an unknown color system, a fallback may use the
   primary Chinese color name. Only when that is absent may the A2023/B2024
   candidate names be used, and they must represent one normalized name.
3. Require both the complete fabric identifier and complete fabric name to
   match. Optimized variants remain distinct. Read the complete bounded SKU
   prefixes to test name uniqueness, rather than an inventory-selected SKU
   subset.
4. Compare whole Chinese color names after stripping outer whitespace and
   optionally removing one final `色`. Do not remove internal `色`, internal
   spaces, prefixes, synonyms or arbitrary punctuation. Do not use substring
   or similarity matching. The supplier's numeric `#` designation is parsed
   from the source product name separately.
5. Multiple normalized candidate names or multiple source SKUs remain blank.
   An unavailable output does not eliminate a source SKU from the uniqueness
   check. Conflicting repeated source SKUs are also blocked.
6. Query only the existing product snapshot with bound prefix parameters, at
   most 20 prefixes per query and 5000 total rows. Empty or failed source reads
   stop the writer before record deletion. No DDL or database writes are added.

Only the reference value is filled. An original `A2023` or pending system may
remain as an explicit upstream classification issue even if the name reference
has been resolved. This is not evidence that the original inventory mapping was
reclassified or that all upstream color-system issues have been repaired.

For a current-table repair, back up all rows and view configurations, update
only `颜色-领星` by record ID, and read back every field. Do not rerun the full
procurement pipeline to repair one reference column.

Offline tests, with no production imports or network connections:

```sh
python -m unittest discover -s tests -p 'test_fabric_lingxing_name_reference.py' -v
python -m unittest discover -s tests -p 'test_named_color_name_fallback_writer.py' -v
python -m unittest discover -s tests -p 'test_named_color_reference_writer.py' -v
```
