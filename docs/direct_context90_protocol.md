# DIRECT_CONTEXT90 V1 preregistration

Recorded before candidate effects: 2026-10-20T07:55:39+00:00.
The current user request authorizes this single finite local experiment and its
five screening conditions. No further authorization or synthetic approval is
substituted for that request.

Base diagnostic commit: `8908e71bc45bc0891b84fca5c4103058fc5ad984`.
Replay reference: `34bdc3b703c45326f3c638814ad18595c8124e83`.
Only the authorized existing daily input, monthly qualification history, core
lists, frozen configuration, checkpoints, training audits and scores are read.
No database, BI, procurement, scheduler, production model or source changes.
Current category/season labels are post-hoc descriptive fields only.

## One candidate, unchanged parent

Candidate: DIRECT_CONTEXT90. No window/seed/parameter search or fallback routing.
Retain frozen DIRECT30x2's ten features, original ordering and native dtypes.
Append exactly:

1. `m56`
2. `m90`
3. `contrast56`
4. `contrast90`
5. `history56_complete`
6. `history90_complete`

For each row, c is that row's own issue minus two calendar days, not the final
training cut. Means include both endpoints: m28=c-27..c, m56=c-55..c,
m90=c-89..c, prev28=c-55..c-28, prev62=c-89..c-28.
Contrasts are (m28-prev)/(m28+prev). Complete explicit double zero yields 0.
Any required missing, unknown, negative, nonfinite or unmapped date yields NaN
for the affected numerical feature and 0 for its completeness flag. No epsilon,
forward fill, dropping early rows or use of target sales. Dense numeric input,
float64 new numeric features, integer 0/1 completeness flags.
Intermediate means are not additional model features.

The original Poisson objective, 200 trees, learning rate 0.05, 31 leaves,
min_child_samples 40, reg_lambda 1, random_state 20261019, two threads,
deterministic/force_col_wise and nonnegative clipping remain unchanged.
No sample-weight argument is added: uniform implicit weights remain.
The frozen parameter dictionary, runtime versions and source hashes must agree.

LightGBM 4.6.0 documents default NaN handling with use_missing enabled and
zero_as_missing disabled. Do not change these defaults.
Source: https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html#missing-value-handle
Verify with a separately recorded synthetic fit before business fits. Synthetic
tests are not candidate results and do not consume the 40 business-fit slots.

## Frozen population and temporal design

- Twenty original mature monthly issues, 2025-01 through 2026-08.
- Preserve original full monthly_core and first-issue F28/DIRECT predictions.
- Preserve original September/October NOT_MATURED records; no candidate fits.
- S is the archived issue, C=S-2. M1=S+1..S+30, M2=S+31..S+60.
- Training simulated issue r uses c=r-2, labels ending r+30/r+60 <= C(S).
- Reconstruct original complete training context, not just core products.
- Before any candidate effect, compare all forty train-key/label/old-feature
  column hashes and native dtypes with archived original training_audit.json;
  compare full training frame hash with the original checkpoint as well.
- Retain all original included rows, even if a new long feature is NaN.
- Check inference keys, input statuses and old features against original saved
  segment predictions. No qualification widening or new output for old failures.
- Keep a pre-effect manifest of training keys, labels, features, inputs,
  eligibility, evaluation rules, source versions and empty fit ledger.

This is repeatedly explored reconstructed historical validation, NOT independent
OOS or proven point-in-time availability. No source acquisition time is backfilled.
Train-label maturity is enforced, but historical actual availability remains a
separate limitation inherited from the archive.

## Declining-input observation group

Evaluation only, not model input or routing:
early34=c-89..c-56, middle28=c-55..c-28, recent28=c-27..c.
Complete 90-day history, all three means >0, and early34>middle28>recent28.
Otherwise complete rows are NOT_DECLINING; incomplete rows UNKNOWN.
This is a pre-issue decline proxy, not proof future demand continues declining.
The same group's old DIRECT and F28 are recomputed on identical scored members.
Report group size, distinct products, periods, actual-unit coverage, high/low
errors, WAPE, hits and zero/unknown status for all three windows.

For descriptive breadth only, reuse the diagnostic display screen of >=30
paired records, >=10 products and >=3 periods. It is not a new performance
gate or a significance claim. Insufficient breadth leaves protection evidence
PARTIAL even when numerical comparison is available. Always disclose per-period
support, repeated products and overlapping targets.

## Scoring and the five user-authorized screens

Retain FULL_CORE and ORIGINAL_DIRECT_OUTPUT scopes. Do not redefine the core
through a multi-model intersection. If candidate output is missing, preserve it,
export loss members and units, and calculate a clearly labeled common-available
side table; output-coverage screening cannot pass by dropping failures.
Use independently summed original daily truth, and reconcile original scores.
No truth/forecast rounding before classification or gate comparisons.

Metrics by 1-30,31-60,1-60: WAPE, Bias, AE, high and low error, hit count,
hit/fixed-core count, positive-truth hit rate, hit sales, zero-truth AE, output/
truth coverage. Joint hit means all three positive-truth windows are within
inclusive +/-20%; count once per object/issue. Show joint-hit actual60 /
same-scope known actual60, and gained/lost/retained hit members and units.

The five screens versus frozen DIRECT, with unrounded values:
1. Paired W60 WAPE strictly lower.
2. Joint-hit count strictly higher.
3. Joint-hit actual sales share not lower, identical denominator.
4. Decline-proxy high-error rate not higher; denominator is same-group actual
   units, not bias and not a selected positive-error subset.
5. Output coverage and full-core retention not reduced.

All conflicts remain visible. No threshold relaxation after results. An unknown
denominator or insufficient evidence is PARTIAL, never silently PASS.
Numerical screen, technical validation, and business candidacy are separate.
Passing the screen is not meeting the ultimate per-core-item +/-20% goal.

Full and paired breakdowns: shop, issue, original maturity, current descriptive
category and season. Existing diagnostic focus groups remain descriptive only.
Report improvement/decline breadth, monthly stability, error concentration,
extra overprediction and hit-sales tradeoffs, not only aggregate WAPE.

## Budget, checkpoints and stopping

At most 40 business fits, one M1/M2 for each mature issue. Old models and F28
are never refitted. Two threads per fit; sequential slots; 600-second budget per
slot. A reserved/started slot is never fitted twice. Persist fitted checkpoints
before prediction. Recovery may load a saved checkpoint to finish inference;
it must never refit a started slot. Missing checkpoint after technical failure
is a blocker, not permission to retry. No performance-triggered retries.

New root and child artifact creation reject pre-existing destinations; explicit
resume only validates existing receipts and skips completed work. Original
archives are never modified. Snapshot source hashes before and after.

Independent checks: exact calendar boundaries; row-local cutoff; missing vs
zero; old train/labels/features/weights unchanged; mature labels; future-truth
perturbation invariance; M1/M2 independence and sum identity; daily truth sums;
single-count joint hits and fixed sales denominators; original hashes unchanged.

Save models, train/inference matrices, feature coverage, score rows, migrations,
ledger, tests and report locally. Commit only new desensitized code/tests/docs
on an independent branch. Stop after the numerical report and independent
validation. No next experiment, model selection, merge or deployment.
