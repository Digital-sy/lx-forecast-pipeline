# BC60 V3 preregistration: DIRECT30x2

This protocol is frozen before candidate effects. Classification:
`RECONSTRUCTED_HISTORICAL_REPLAY`. Three original issue periods were previously
explored; none is independent OOS. Source acquisition history is not restored.

## Scope and decision

Only one candidate: two independent LightGBM cumulative-unit regressions per
original issue, M1 days1–30 and M2 days31–60; sum gives days1–60. No recursion,
no M1 prediction/realization enters M2. At most six formal fits, no search,
early stopping, weights, target scaling, log transform, exogenous inputs, or
production changes. All predictions remain floating point with original
nonnegative clipping. No database/server queries or new business inputs.

Fixed385 issue-object records/174 distinct shop-SPUs remain the primary grid.
Original369 B1-available subset is secondary;16 missing qualifications remain.
Preserve future sales coverage limitations. F28 is primary comparator; B1 and
V2 D1 are archived secondary controls, D1 remains diagnostic, not production.

## Provenance

Read only V1 `inputs_v1_final`, `effects_v1`, existing verification; V2
`a_diagnostic_final`. Original daily SHA256:
`ff69063d3c4e506b2b2b8c701db6afa95b4f8af7ce7af3b7c17a898e70bfc7da`;
core SHA256:
`f634f9e1ee857be71db2e4f31e7136a4d7f6ff08761fb227edea972cc71f32cf`.
V1 effects commit58674f0a8ebe14f176bfd0a6baaccb5cfc1c71e7;
V1 helper d29431dfd06db22588e8637b0673c37166f8ad09;
V2 final32a66a40e66f9a83243a518ffbd100665eae8f9b.
Use existing locked environment; no installs/upgrades. Read original lock at
runtime, verify its SHA and preserve its LightGBM dictionary unchanged.

## Date, qualification, features

Actual issues2026-04-02,06-02,08-02; cutoffC=S−2. Simulated r cutoffc=r−2.
M1 label[r+1,r+30], M2[r+31,r+60]. M1 labelend<=C and M2labelend<=C checked
separately. No need to predict bridges r−1/r, and no shift of scored windows.

Training uses all mapped series in four original shops, never core-only. For
each simulated r, eligibility requires at least4 positive **completed** monthly
records whose monthend<=c. At inside-month c, full current month is excluded.
The original monthly helper cannot be reused blindly on arbitrary r because it
was designed for month-end cutoffs. Current/future eligibility cannot backfill.
Monthly source omissions remain unknown, not proof of true business ineligibility.

DIRECT requires28 consecutive calendar days with mapped OBSERVED/EXPLICIT_ZERO
nonnegative finite sales ending c. The original recursive segment56day guard is
not an additional direct feature/guard; this deliberate structural change is
disclosed. Earliest possible r=sourcefirst+29days. Missing/invalid days remain
missing, never zero or compressed observations. Labels independently require
all30 valid calendar days; explicit zero labels are retained.

Sales features mirror original first recursive step t=c+1: lag1=y[c],lag7=y[c−6],
lag14=y[c−13],lag28=y[c−27],rm7=mean[c−6,c],rm28=mean[c−27,c]. Preserve float32
native integer-history representation for lag/rolling arithmetic, without
normalization. Fixed numeric shop codes follow original shop order. No SPU ID
predictor. Calendar for each segment's target start: weekday0–6,month1–12,
dayofyear1–366. Same definitions train/predict. No learned whole-data encoding.

## Training support and safeguards

All eligible simulated calendar dates, no random split. Save exclusion counts
in an explicit ordered funnel: qualification, maturity, history, label validity.
Record rows, objects, dates, date/label spans, positive/zero labels and greedy
maximum nonoverlapping30day windows perobject. Overlap never counts as independent
time evidence. If global maximum nonoverlapping windows<2, flag
LIMITED_TEMPORAL_SUPPORT (descriptive, not effect-selected); no rows=>NOT_TRAINABLE.
All-zero Poisson labels=>NOT_TRAINABLE_ZERO_LABEL_SUPPORT, no fabricated prediction.
Each fit <=original10minute budget,2threads,200trees. Preserve failed attempt;
checkpoint before inference; recovery can only reuse verified saved models,
never secretly refit a failed period. Failure in one segment does not stop others.

## Scoring and stopping

Primary full385, secondary original369, plus explicit complete common subset
when required. Recompute comparator scores in every subset. WAPE/Bias/over/under,
positive-truth20%hit, unknown/missing/zero states, hit-sales fraction, joint3window
hits and F28 hit migration; issue/shop/maturity/lifecycle stratification. Zero
truth relative error undefined. Either missing segment makes Y60 unavailable.
No new acceptance threshold; judge user objective, not WAPE alone.

Independently recompute truth directly from original daily, training label/feature
boundaries and historical qualification, sum identity, source hashes, fixedgrid,
metrics/migrations. Synthetic boundary/leakage tests before fits. Reconcile raw
model output, clipped output, model hashes and actual fit count. Archive original
inputs, trainingsets, models, outputs, scores, failures, timing, config and hashes
locally. Only sanitized source/tests/specification enter GitHub independentbranch.
Finish actual effects/report then stop; no model selection/deployment/BI updates.

Public API reference checked: LightGBM4.6.0 LGBMRegressor fit/predict/booster save,
https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html
