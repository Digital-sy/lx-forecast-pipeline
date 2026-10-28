# DIRECT_CORE_WEIGHT2_V1 preregistration

Only change: sample weights in the archived, frozen DIRECT30x2 fit.
Historical CORE=2, NON_CORE=1, UNKNOWN=1; divide by this entire fit unit's raw
weight mean. No other balancing, feature, loss, seed, eligibility, initial score,
postprocessing or training-date change. All rows, including zero targets, remain.
No weight search and no successor experiment in this authorization.

Use the verified diagnostic algorithm from c738374ac28fd0c59bdf31bfa81ed5da716d3ac6.
At each training simulated issue r, cutoff c=r-2. Select core from the entire shop's
preceding 90 calendar days, cumulative >=90%, include boundary ties, before mature
qualification. UNKNOWN is an explicit valid state; missing joins are errors.
Raw states agree on common M1/M2 training keys. No target labels or errors enter
states or weights. Preserve training order using row indices and exact keys.

Source model: original monthly replay archive, original source commit
34bdc3b703c45326f3c638814ad18595c8124e83, its actual configuration and hashes.
Matrices may be reused from ITEM_ID only after every original key/label/10-feature
column and dtype matches DIRECT's recorded audit. Project original 10 features
only; no identity, traffic, core-state or weight column enters X.
LightGBM 4.6.0 fit(sample_weight=...) with original Poisson config, including
default weighted initialization. No manual initial-level correction.
Official API checked: https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html

20 mature issues 2025-01 through 2026-08, two independent 30-day fits each, max40.
Each historical training feature uses its own cutoff; target must fully end by
actual issue cutoff. Exclusive fit-start ledger; checkpoint recovery never refits
an already attempted slot without a valid saved model. Freeze weights/source hashes
before fits; seal predictions before scoring. Original predictions and sources
are immutable. No business database/API calls or production changes.

Main scores remain unweighted on full2820 and original paired2579 object-issues,
with241 missing outputs and842 paired pre-issue declining records retained.
Compare candidate separately with DIRECT and BLEND50; F28 retained unchanged.
Five unchanged strict/unrounded gates: paired60 WAPE lower; joint hit count higher;
joint-hit actual-sales share not lower; original decline-group60 overprediction
rate not higher; output/core coverage not lower. All three original windows and
positive-actual <=20% joint-hit rules unchanged. No zero-actual percentages.
Preserve original1953 six-model joint misses,284 BLEND50 hits,Top20,age,shop,issues.

Auxiliary risk pool: freeze all observed mapped objects at each original issue,
qualify using original DIRECT input/eligibility rules, before reading target truth.
Include contemporaneous NON_CORE; CORE->NON_CORE since c-30 is EXITED_CORE;
NON_CORE->CORE is NEW_CORE. Unknown states remain explicit. Same local daily source,
no new queries. Infer saved new and original DIRECT models, never refit controls;
count all actual prediction calls. Report AE/over/under and truth/output coverage,
zero-actual predicted quantities separately. These risks do not become an added
post-hoc sixth gate, but any harm must be disclosed. Missing auxiliary coverage
cannot be represented as safety approval and cannot block valid main computation.

Validation: original matrices exact, positive mean-one weights and2:1 raw ratio,
row correspondence, own-cutoff/future perturbation, label-independent weight,
actual fit interface receiving registered weights, unchanged output scope, segment
sum, independent metrics/migrations, immutable sources and112 sealed shadow files.
Synthetic tests use fake estimator forwarding, not additional real-model fits.

This is repeated reconstructed historical development, not independent OOS or
prospective validation. Technical PASS, five-gate result and business evidence
grade must be distinct. Results/weights/models stay local. Only sanitized source,
tests and this specification go to a separate GitHub branch. Stop after this one
fixed candidate; do not modify the already issued first shadow or production.
