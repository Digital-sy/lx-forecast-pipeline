# DIRECT_L1_V1 preregistration

Single fixed objective contrast. Start from frozen DIRECT30x2 configuration and
all original training rows, keys, order, raw 30-day labels and ten feature dtypes.
Set objective=regression_l1 and metric=l1. Implicit equal weights: call fit(X,y)
without sample_weight, class_weight, init_score, callbacks, evaluation-set early
stopping or label transforms. Do not import the CORE_WEIGHT2 training config.
No new features, routing, weights, seeds, target windows or qualifications.

Original source: 34bdc3b703c45326f3c638814ad18595c8124e83 and the monthly replay's
frozen_config.json. Reused ITEM_ID matrices must pass original-column/key/label
hash and dtype checks after dropping item_key. CORE_WEIGHT2 provides validated
matrix provenance and a frozen auxiliary risk pool, never training weights.
Use the locked LightGBM 4.6.0 environment and unchanged common hyperparameters.
Reject conflicting objective/metric aliases or hidden weight/initial-score config.
Record every parameter difference. poisson_max_delta_step is Poisson-only;
remove it if explicitly present, otherwise record its inactive library default.
L1 native initialization/link/leaf updating apply; no manual compensation.

Official version references verified before fitting:
- https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html
- https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html

20 mature issues 2025-01 through 2026-08, independent M1/M2, max40 formal fits.
Reserve each attempt once, keep checkpoints, no seed or failed-result refit search.
Features use each simulated sample's cutoff; full label ends by actual issue C.
Never feed M1 output to M2. Save native raw outputs and max(raw,0), without rounding.
Record negative counts/units. Compare live and saved/reloaded model predictions;
count these prediction calls separately. Seal outputs before scoring.

Reuse fixed full2820 and paired2579 records,241 missing outputs,842 declining
records,1953 common misses,284 BLEND50 hits,original Top20 and all issue/shop/age
groups. Main baselines DIRECT and BLEND50; F28 and CORE_WEIGHT2 are side tables.
No control refits. All business scores remain unweighted with original truths.
Five gates against each main baseline separately: lower paired60 WAPE; more
all-three positive-actual <=20% hits; no decline in hit actual-sales share;
no higher original decline-group60 overprediction rate; no coverage reduction.
Report each window's WAPE,Bias,over/under,hit counts and hit sales, plus joint
retained/gained/lost. Do not equate lower training MAE with gate success.

Reuse the exact previously frozen auxiliary14,283 object-issue pool and truth.
Do not reselect it. Infer L1 saved models only; reuse original DIRECT auxiliary
forecasts per key. Missing controls cannot silently change the pool. Preserve
CURRENT_NON_CORE,EXITED_CORE,NEW_CORE,unknown,zero and missing states. Report
AE/over/under,Bias,forecast totals,zero-actual predicted units,and zero-prediction
positive-actual counts and actual units. Auxiliary harms inform interpretation,
not a retrospective sixth gate. Focus also on persistent underprediction in
original high-volume/Top20 objects and original jointly-underpredicted groups.

Independent validation checks training equality,no passed or hidden weights,
saved objective/metric,own-cutoff/label maturity,save-load equality,segment sums,
metrics and migrations using separate arithmetic,auxiliary risks,and immutable
sources including112 first-shadow files. Synthetic tests do not add real fits.

This is repeatedly studied reconstructed historical development, not independent
OOS or prospective success. Technical validation, research gates and business
evidence grade are distinct. No business database/API reads,production edits or
new shadow issuance. Save results/models locally; publish only sanitized source,
tests and specifications to an independent GitHub branch. Stop after this fixed
candidate; do not add Huber,quantile,other losses,mixtures or tuning.
