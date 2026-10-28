# DIRECT_ITEM_ID_V1 fixed identity increment

One candidate. Historical developmental reconstruction only, not independent OOS
or proof that omitted product identity caused the original errors. No routing,
parameter search, model blending, source-system queries or production changes.

## Frozen baseline and scope

Original DIRECT has ten features: shop_code, lag1/7/14/28, two existing rolling
means, dayofweek, month, dayofyear. The original fit projects exactly those ten
columns; shop/SPU identifiers in the training frame do not enter the model.
Use the original archive's configuration, per-column hashes and full-frame hash,
not changed defaults from a later research branch. Preserve training keys, labels,
implicit uniform weights, eligibility, float dtypes, old feature order and values,
Poisson loss, random seed, training range and nonnegative prediction clipping.

Twenty mature issues, January 2025–August 2026, full core 2,820 object-periods,
original paired 2,579, original missing-output 241. Immature issues remain excluded
without shortening windows. S-2 cutoff; M1 S+1..30, M2 S+31..60; labels must finish
by the actual issue cutoff. Each training row uses its own historical cutoff.

## Only appended feature

`item_key`: nominal shop × SPU category, not a numeric trend. For each issue and
segment independently, collect unique nonempty string tuples from that unit's
actual training frame, sort lexicographically by the two strings, assign int32
codes 0..K-1. The ordering does not use labels, volume, errors or evaluation data.
Do not build a dictionary from all periods or the inference universe. No string
prefix or numeric SKU extraction. Tuple keys avoid separator collisions.

Encode absent inference tuples as -1 and report UNSEEN. Negative categorical codes
are LightGBM missing values; never map them to an existing product or remove them.
Explicit `fit(..., categorical_feature=['item_key'], feature_name=FEATURES)`;
the other ten columns retain their original numeric interpretation. No target
encoding, per-product models, metadata fields, extra histories or validity feature.

LightGBM 4.6.0, unchanged original constructor parameters. Native category defaults
are retained without tuning: min_data_per_group=100, max_cat_threshold=32,
cat_l2=10, cat_smooth=10, max_cat_to_onehot=4. Record actual serialized model
parameters and categorical feature index, and inspect actual categorical splits.
Unknown categories use the documented negative-code mechanism; zero is a valid
known category, not missing. No dependency upgrades.

References:
- https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html
- https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html
- https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html

## Support and budget

Save each dictionary and hash, original/audited training rows and inference frame.
For every inference object, retain seen status, training row and distinct issue-day
counts, first/last training issue, inclusive calendar span, label range and maximum
non-overlapping 30-day target intervals (greedy ascending end). The last count
describes temporal coverage, not statistically independent seasonal examples.

Two shared models per issue, at most 40 formal fits. Reserve each fit in the ledger;
resume saved checkpoints without refitting. No early stopping on evaluation data.
No result-driven retries or changed seeds. Qualification remains unchanged even
for unknown identity or limited support. Synthetic encoding/metadata tests are
separately recorded, never represented as historical effects.

Before effects, freeze diagnostic support bands based on the MINIMUM non-overlap
count across M1/M2: 0, 1–3, 4–11, 12+. These labels never affect training or routing.
Seen groups: SEEN_BOTH, SEEN_ONE_SEGMENT, UNSEEN_BOTH. Keep original 4–5, 6–11,
12+ maturity and original 842 paired declining records.

## Scoring protocol

Controlled comparison DIRECT; candidate comparison BLEND50. Reuse F28 and the
completed TRAFFIC_STATE predictions as side references, no retraining. Full core
and original paired denominators separately. Three windows: WAPE, Bias, over/under
units, positive-actual ±20% hits, joint hits counted once and actual-sales weights.
Explicit zero actual retains absolute error, missing output is not zero error.

Against each DIRECT/BLEND50 independently, unrounded five gates: lower 60-day WAPE,
more joint hits, nondecreasing joint-hit actual share, nonincreasing original
declining-group over-rate, and no loss of output/core coverage. No changed threshold.

Reuse original six-model groups, not groups recomputed including the new candidate:
original BLEND50 284 joint hits; other-model rescue; 1,953 all-six-output/no joint
hit records; missing/undetermined. Reuse original Top20 and direction labels.
Report hit gains/losses and actual weight, per-issue/year/shop/maturity/seen/support
performance, Top20 period stability. No SPU exceptions or post-hoc routing.

## Validation and immutable evidence

Verify complete original training frames and column hashes, dictionary training-
only membership, cross-shop separation, unknown handling, own-row cutoff, label
maturity, feature projection, categorical metadata and saved-model equality.
Independent scalar scoring must confirm joint migrations and all five gates.
Truth chain reuses previously independently reconciled daily-to-window truth.
No new database/API reads. Hash all registered old sources and all 112 protected
first-shadow files before/after; never parse that batch's future actuals or rerun it.

Save local actual report, necessary CSVs, models, dictionaries, ledgers and hashes.
Only sanitized source, synthetic tests and this specification go to an independent
GitHub branch. Stop after the one fixed candidate regardless of effect.
