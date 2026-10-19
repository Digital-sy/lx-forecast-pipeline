# A7 DOWN V1 closeout boundary

Status: closed on the basis of independent review commit
`d2d0ec237ac04b46ee6a073d683888cf7fc147b3`.

- `w=0.5`: numerical gate FAIL; business evidence PARTIAL.
- `w=0`: numerical gate FAIL; business evidence PARTIAL.
- The decision rejects only these two uniform DOWN-weakening candidates.
- Frozen A16 and production V4 remain unchanged.
- No new weight grid, SKU-specific exceptions, model selection, history rerun,
  database query, shadow batch, procurement/BI change or schedule is authorized.
- The outcome does not establish global A16 optimality or validated
  comparability-based routing.

The controlled local closeout package references the existing assessment,
comparisons, coverage and full per-row outputs by SHA256. The six controls
(V4 replay, frozen A16, last-complete-month continuation, A3, half weight,
zero weight) must all reference the same registered scoring keys and actual
denominator. Do not combine own-coverage or older scores into a final ranking.
Business inputs, numeric result tables and original shadow artifacts remain
outside Git. Later reconstructed evidence must retain its actual acquisition
time and cannot be retroactively attached as original capture.

See [next prospective evidence plan](a16_next_prospective_shadow_evidence_plan.md).
It reuses the current archive schema, hash contract and synthetic tests.
This commit is documentation only: the new archival integration and real
prospective run have not been implemented, deployed or executed.

Existing safe checks:
```text
python -m unittest tests.test_shadow_archive_contract tests.test_a16_shadow_once -v
```
These tests verify synthetic contracts and source fingerprints, not successful
future collection or an actual shadow run. A new batch needs separate approval.

