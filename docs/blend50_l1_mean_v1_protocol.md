# BLEND50_L1_MEAN_V1 fixed archived-prediction combination

This protocol is recorded after observing the two endpoint experiments. It is
reconstructed historical development, not independent OOS or prospective success.
It preserves the prior L1 decision and the original BLEND50 shadow issue.

## Single fixed rule

For each issue_date, shop, spu and each original 30-day segment:

    prediction = 0.5 * blend50_prediction + 0.5 * l1_prediction
    prediction_60 = prediction_M1 + prediction_M2

Use sealed, final nonnegative, unrounded endpoint predictions. Preserve their
missing outputs and original eligibility. No rounding, clipping, calibration,
fallback, gate recomputation, routing, or additional mixture is introduced.
Endpoint output masks must match; a mismatch blocks calculation rather than
shrinking the comparison population. No fit, model inference, source query,
production change or new shadow issue is allowed.

The generation process projects only business keys, date boundaries, endpoint
predictions and output states. It writes and hashes the candidate before opening
scoring truths. Existing source files may be hashed without parsing their truths.
Generation is not made independent of past research merely by this separation.

## Frozen scope and sources

- Mature issue periods: January 2025 through August 2026, twenty periods.
- Complete core: 2,820 object-issue records; paired output: 2,579; missing: 241.
- Original decline observation group: 842 paired records.
- Original DIRECT issue/cutoff and three target windows remain unchanged.
- Sources: the sealed L1 archive, BLEND50 archive, monthly replay and original
  model-complementarity archive. Locate and verify actual manifests and hashes.
- Only the first shadow's original file hashes are checked; no new actuals or
  predictions for that issue are read or created.
- Inputs and old artifacts remain read-only. The new output directory must not
  exist. Record actual timestamps with timezone, source hashes and code version.

## Evaluation fixed before candidate scoring

Primary upgrade comparator: BLEND50. Reuse DIRECT, L1 and F28 as side references.
Keep both complete and paired denominators. Missing is not zero. A true zero
actual has absolute error but no invented relative-error hit.

The five gates use unrounded values against BLEND50:

1. Strictly lower paired 60-day WAPE.
2. More joint hits across both 30-day segments and their 60-day sum.
3. Joint-hit actual-sales share does not decrease.
4. Original decline group's 60-day overestimation rate does not increase.
5. Output and full-core coverage do not decrease.

Report WAPE, Bias, over/under units, individual-window 20-percent hits and carried
actual sales. Joint hits are counted once per object-issue. Report shops, maturity,
all twenty periods, original Top20 and original common-underestimation group.
Check whether higher hit counts conceal lower single-window hit sales.
Underestimation risks are disclosed separately, not turned into a sixth gate.

Recompute endpoint joint groups from original predictions: BOTH_HIT, BLEND_ONLY,
L1_ONLY, NEITHER; keep missing/unscorable records separate. Report new-candidate
hits, losses and carried actual sales in each group. The endpoint hit union is
not an upper bound on mean-prediction hits.

For each paired object and each of BLEND50, L1 and the mean:

    e1 = prediction_M1 - actual_M1
    e2 = prediction_M2 - actual_M2
    segment_AE = abs(e1) + abs(e2)
    cumulative_AE = abs(e1 + e2)
    cancellation = segment_AE - cumulative_AE

Do not add segment_AE to cumulative_AE as a total loss. Cancellation is a
mathematical property, not fraud or independent evidence of predictive skill.

Auxiliary noncore analysis is allowed only if both matching sealed endpoint
predictions already exist. Otherwise mark NOT_EVALUATED; never use DIRECT as a
surrogate BLEND50 or run models to fill the gap.

## Verification and stop

Synthetic tests and an independent arithmetic implementation check keys, dates,
truth versions, no row multiplication, formula, output masks, additivity, endpoint
bounds, convex absolute/over/under error bounds, mean Bias and common-hit retention.
Machine precision tolerance is only for algebraic identities, never research gates
or the unrounded 20-percent hit test. Independently recompute gates and migrations.
Verify source hashes and all original 112 shadow files after completion.

Deliver one actual-effect report, necessary CSVs and validation/command receipts.
Only sanitized source, rules and tests enter an independent GitHub branch; local
business data and effects stay local. No weight search, additional candidate,
training, deployment, source query, or follow-on experiment after this one run.
