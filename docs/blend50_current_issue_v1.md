# One current-date BLEND50 shadow issuance

Authorization: bounded read-only recent ODS acquisition followed by one fixed local
issue. Production/API writes, API acquisition, deployment and scheduling are absent.
The previous repaired cache is the base. Preserve old failed and successful archives.

Freeze real Asia/Shanghai S at initialization, C=S-2. W1 is S+1..30 and W2 S+31..60.
If the business date changes, stop before fitting/sealing; never backdate. Save UTC,
business day, acquisition, fit and seal times separately. User-facing reports also
show the client's time zone.

Read only absent recent days and seven prior boundary days, serially. Use inspected
ODS fields and primary index, native EXPLAIN, 10s/query, max 30 SELECTs, 1M estimated
rows/query, 10M cumulative; cap returned rows. Estimates are not actual rows. Before
and after, inspect the explicitly identified normal task logs and known process.
Require completed latest covering task, contiguous page-write offsets, completion
counts equal extracted unique keys, and no changed task evidence during extraction.
This is operational sync evidence, not independent upstream business truth. Keep
accepted prior platform differences and inherited historical limitations explicit.
No new per-slice platform export requirement. No production recovery in this task.

Preserve missing/invalid quantities and mapping flags. Compare overlap keys before
replacement and retain all revisions; no arbitrary deduplication. New ASIN records
aggregate exactly once to source SPU, with non-unique/unmapped SPU quantities retained
in the shop denominator. Repaired slices before overlap must remain field-identical.

Reuse pinned methods and original runtime via `blend50_prospective.verify_code`.
Reuse the accepted historical reproduction if hashes/runtime match; do not refit or
repeat the prior reproduction. Monthly qualification uses the existing completed
monthly archive, never current-month partial quantities. Freeze all-shop 90-day core
with original ties and >=90% denominator before model fitting. Retain new/ineligible
core objects and missing outputs; no new-product invocation or HTML volume filter.

`blend50_issue_current.py prepare` archives complete-context training/inference
matrices, keys, labels, old features and annual features, support, core, decline
diagnostics, F28 and configuration. No fits occur in preparation. Exactly four
exclusive slots maximum: DIRECT M1/M2 and YEARAGO M1/M2. Use unchanged original
LightGBM configuration, qualifications, per-row cutoffs, label maturity, annual
support, GATE conditions and clip-to-zero postprocessing. Each slot writes a start
reservation before `fit`, checkpoints before inference, and refuses overwrite.
A technical failure is not automatically retrained. Unsupported annual slots do
not fit; GATE FALSE preserves DIRECT for both segments. No tuning or new weights.

Assembly uses the existing frozen serial assembly and BLEND function. A separate
checker reloads saved models, independently sums all training labels from daily
inputs, checks F28, routing, exact FALSE copies, two-segment totals, fixed core keys
and fit budget. No future truth or present accuracy. Seal only after this passes,
before target start, with full input/source/code/config/model/output hashes.

Local commands (paths supplied by caller; use the original isolated interpreter):

```text
python scripts/blend50_recent_acquire.py --root NEW --workspace WORKSPACE --mode initialize
python scripts/blend50_recent_acquire.py --root NEW --mode metadata --name metadata
python scripts/blend50_recent_acquire.py --root NEW --mode logs --name logs_before
# reviewed 1-2 day queries inside run_lock scope, each with an exclusive name
python scripts/blend50_recent_acquire.py --root NEW --mode data --name UNIQUE --start DATE --end EXCLUSIVE
python scripts/blend50_recent_acquire.py --root NEW --mode logs --name logs_after
python scripts/blend50_recent_input.py --root NEW --workspace WORKSPACE
python scripts/blend50_issue_validate.py input --root NEW
python scripts/blend50_issue_current.py prepare --root NEW --workspace WORKSPACE
python scripts/blend50_issue_current.py fit --root NEW --slot SLOT
python scripts/blend50_issue_current.py assemble --root NEW
python scripts/blend50_issue_validate.py prediction --root NEW
python scripts/blend50_issue_current.py seal --root NEW
```

Commit only sanitized source/tests/specification. Local archives, models, input,
predictions, receipts with business content and credentials never enter Git.
The final states distinguish INPUT_INCOMPLETE, INPUT_READY_NOT_ISSUED and
ISSUED_SHADOW_AWAITING_ACTUALS. Stop after this one issue.
