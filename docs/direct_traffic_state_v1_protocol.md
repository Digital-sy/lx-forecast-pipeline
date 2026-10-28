# DIRECT_TRAFFIC_STATE_V1 fixed increment

One candidate on immutable DIRECT30x2. Historical developmental reconstruction,
not independent OOS or historical point-in-time proof. No routing or weight search.

## Scope and unchanged behavior

Use existing daily archive, January 2025–August 2026 issue dates, full core and
original DIRECT-output pairing. Preserve the original 241 missing-output records.
Do not derive eligibility from a visualization or diagnostic Top 20.
Reuse frozen VectorContext to regenerate original training rows and require exact
frame and per-column hashes against the original DIRECT checkpoints. Sample
weights remain implicit uniform; Poisson loss, parameters, seed, qualification,
label maturity, old features and nonnegative output clipping stay unchanged.
Each M1/M2 slot may fit at most once; at most 40 formal fits. No baseline refits.

## Fixed appended fields (original features remain in the same order)

For each row's own input cutoff c, inclusive continuous calendar days:

| Numeric field | Definition |
|---|---|
| traffic_t7 | sum of valid Sessions[c-6:c] / 7 |
| traffic_t28 | sum of valid Sessions[c-27:c] / 28 |
| yield_r7 | sum of valid sales units[c-6:c] / same-window Sessions |
| yield_r28 | sum of valid sales units[c-27:c] / same-window Sessions |
| traffic_change | (traffic_t7-traffic_t28)/(traffic_t7+traffic_t28) |
| yield_change | (yield_r7-yield_r28)/(yield_r7+yield_r28) |

Append one finite/defined validity indicator for each numeric field. Window sums,
reason codes and date bounds are retained for audit only, not appended features.
No other window, threshold, category, season, price, advertising or inventory field.

Sessions = archived listing_sessions_sum, derived by summing sessions_total at
the existing shop × SPU × business-day grain. Not deduplicated visitors. Sales =
archived y, derived from volume, not order_items. R is units/session, may exceed 1.
No source join or expansion of ASIN to multiple mapped products is performed.
Retain source grain/mapping and its existing limitations; no string SPU inference.

Sessions are valid only with a mapped object, finite nonnegative aggregate,
known zero null_sessions count, positive source_rows, and zero date_mismatch and
deleted_rows flags. Sales additionally require original known sales_state,
finite nonnegative y, null_qty=negative_qty=0. Missing days remain unknown.
T requires the Sessions window; R requires BOTH sales and Sessions windows.
Zero Sessions or a zero change denominator gives NaN with explicit status;
no epsilon, clipping, window backfill or conversion-to-zero. Do not drop rows.

Use LightGBM 4.6.0's unchanged native NaN handling (use_missing defaults true;
zero_as_missing defaults false), as documented at
https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html .
API reference: https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html .
Use the existing isolated runtime, no dependency upgrade or production venv edits.

## Original B2 comparison (static only)

DIRECT has none of these fields. B2's session_mean28 is related to T28, while
order_session_ratio28 uses order_items rather than sales units and is NOT R28.
B2 uses daily recursive modeling; this experiment keeps direct disjoint segment
prediction. Do not rerun B2/B1/B3 or reuse their performance as this experiment.

## Support and recovery

Before all fitting, seal every training input, feature order, config, input hashes,
source limitations and the 40-slot ledger. A slot with no finite, varying added
numeric training field is SKIPPED_NO_NEW_INFORMATION; reference its original DIRECT
predictions exactly and mark origin REFERENCE_DIRECT. Never count references as
new model output. Individual unknown features stay NaN; qualification is unchanged.
Checkpoint recovery only loads an already saved model. No fit retries or seed search.
No future period outcomes are used to choose support, features or configurations.

## Evaluation, frozen before fitting

Compare with DIRECT (controlled increment), BLEND50 (candidate comparator) and F28.
Report full core and original pair separately; retain missing, zero and immature.
Three windows: WAPE, Bias, over/under units, positive-actual +/-20% hits and joint
hits, actual-sales weights. Denominators are unchanged; 60-day actual counted once
per object-period, not unique calendar sales.
Against DIRECT and BLEND50 independently: lower 60-day WAPE, more joint hits,
nondecreasing joint-hit actual share, nonincreasing original declining-group
over-rate, and nondecreasing output/core coverage. Use unrounded comparisons.
Keep original 842 paired declining records. No result-driven group redefinition.

Reuse original complementarity groups: BLEND hit; other-model rescue; all-six
output/no joint hit (including explicit-zero cases); missing/undetermined.
Reuse original window direction groups and Top 20. New/retained/lost joint hits
and actual weights are descriptive diagnostics only, never training fields.

## Evidence and limits

Archive retrieval time is not historical availability. Null component evidence
is taken only from existing null-count fields and saved acquisition SQL/receipts.
The aggregate source lacks a Sessions negative-component counter; a nonnegative
aggregate cannot establish every original component was nonnegative. No new query
is authorized to repair that evidence gap. Do not confuse sum Sessions with users.
The old sources, model checkpoints, reports and the 112-file prospective batch
remain read-only and are fingerprinted before/after. Prospective predictions or
actuals are never parsed as model or evaluation inputs.

Deliver local numerical report, details, support ledger, training reconciliation,
independent checks and reproduction commands. Only sanitized code/tests/spec enter
a new Git branch. No DB/API queries, production changes, deployment or new shadow.
Stop after this candidate regardless of outcome.
