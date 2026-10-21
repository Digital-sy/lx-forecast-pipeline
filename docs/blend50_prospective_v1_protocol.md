# BLEND50 first prospective shadow V1 — execution lock

Scope: user's attached first prospective issuance request. No production writes,
deployment, schedules, procurement, Feishu or BI publication. Historical HTML and
all prior archives remain unchanged. A new local directory must not exist.

Frozen sources: DIRECT 34bdc3b703c45326f3c638814ad18595c8124e83;
YEARAGO 83e0baa32f6de5cea6ff38e19c87f56fba67daba;
GATE 601e3d2c1f592165141d526eb9b6571dd557b0aa;
BLEND50 4ea35ad10b1c6d059e0b7f8249136b11b49f1fbb.
Reuse existing helpers without modifying their formulas, features, qualification,
training windows, support screen, seed, weights or postprocessing. No CONTEXT90
model fields. Its decline definition is evaluation-only.

## Hard order

1. Verify repository, runtime, frozen code and archived input/model hashes.
2. Reconstruct August 2026 inference features from daily dates <= July 31 and
   completed monthly qualification only, retain full context. Load four saved
   boosters, never fit. Recompute annual support from archived historical training
   rows whose labels already mature at cutoff. Generate GATE and half adjustment.
3. Seal generated predictions before reading any original prediction columns.
   Compare DIRECT/YEARAGO/GATE/BLEND and missing states by complete business key.
   Model numerics use predeclared rtol=1e-12, atol=1e-9 (float64 saved booster
   inference; not relaxed after results). Old feature values/dtypes, flags and
   states must match exactly. FALSE copy is exact; no hit thresholds are relaxed.
4. Failure blocks fresh extraction and current formal fits. No historical refits.
5. Only after PASS, bounded read-only acquisition, source/coverage gate, freeze
   complete current core, then at most four current model fits with exclusive
   per-slot reservations. Failure without checkpoint is not silently retried.

## Dates and data budget

Use real UTC and Asia/Shanghai clocks. S is actual issue business day; C=S-2.
W1=S+1..30, W2=S+31..60, W60=sum. Seal strictly before W1 begins; never backdate.
At least seven archived tail days overlap fresh extraction. Keep revisions and
old input separately. Metadata and native EXPLAIN logged apart from business
SELECTs. At most 30 business SELECTs, 10 seconds each, <=1,000,000 estimated scan
per query and <=10,000,000 cumulatively, serial, no joins or production entrypoints.
Credentials remain in existing secure connection configuration, never serialized.

Only frozen method's sales/qualification/mapping/source completeness fields.
MAX(dt) or modify_time alone does not prove a successful complete source sync.
If coverage through C cannot be established, stop before formal issuance and
report missing dates/shops/evidence; do not silently move cutoff backwards.

## Qualification and artifacts

Per shop select >=90% of last 90 calendar-day actual units with original boundary
ties. Preserve unknown mapping and whole-shop denominator; new/ineligible members
remain in the core ledger without automatic NEW_VISIBLE invocation. No future
truth-based low-volume filter. Freeze core hash before predictions.

Formal fit uses full original eligible historical context, labels r+30 or r+60
<=C, each row's own cutoff r-2. Annual fitting uses existing 30 dates / 10 products
/ varying features support check; unsupported units retain original skip/fallback.
GATE six original conditions select both annual segments or both DIRECT values.
TRUE blend D+0.5*(A-D); FALSE returns D without arithmetic; total adds two segments.

Archive normalized full input, raw receipts, mapping/eligibility/core, feature
matrices/order/dtypes, trained model files/config/runtime, source acquired/business
and sync status, fit ledger, raw unrounded predictions, reasons and hashes. Unknown
future truth remains NULL, not zero. Full hash seal uses exclusive creation.

Future scoring waits until S+32/S+62 at earliest and independent source completeness.
Scores are new artifacts and never modify original predictions. Same DIRECT/F28
tracks, original five comparative gates and full core/coverage retained. Current
decline group is recomputed pre-issue (early34>middle28>recent28>0, complete90), not
copied historical membership. No current accuracy exists before actuals mature.

## Classification and stop

READY_OFFLINE_ONLY means reproduction passed, no future issuance claimed.
BLOCKED_SOURCE_OR_REPRODUCTION means a required gate failed or lacks evidence.
ISSUED_SHADOW_AWAITING_ACTUALS requires real future predictions and complete seal.
User authorized exactly one current issuance; no weights search or daily replay.
Only sanitized source/tests/protocol go to a separate GitHub branch. Business
reports, inputs, models, predictions and notebooks with business output stay local.

API references: LightGBM 4.6.0 official Booster(model_file/model_str), predict and
feature_name; LGBMRegressor fit/predict/save via booster. Runtime pinned to original
configuration and inspected installed signatures. Reproduction API documentation:
https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.Booster.html
https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html
