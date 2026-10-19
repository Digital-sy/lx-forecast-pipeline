# BC60 V2: preregistered B1 feedback diagnosis and rolling-28 experiment

Scope: local archived evidence only. No database query, server deployment, production
entry point, B2/B3 refit, new model family, parameter search, BI or schedule change.
This is RECONSTRUCTED_HISTORICAL_REPLAY, not independent OOS or an online issue.

## Frozen reference

Use V1 final input and fixed core files only: `inputs_v1_final`, `effects_v1`, and
`validation_v1_checked`. Never substitute intermediate inputs. Original execution
commit is `58674f0a8ebe14f176bfd0a6baaccb5cfc1c71e7`, delivery auxiliary commit
`d29431dfd06db22588e8637b0673c37166f8ad09`. Verify file hashes and pinned runtime
before effects; store this run in a NEW local directory and preserve the old one.

Keep all 385 core issue-object records / 174 distinct shop-SPUs. The 369 original
B1-output records are a secondary comparison scope, not a new full core denominator.
The 16 insufficient original B1 qualifications must not be backfilled. Whole-shop
core coverage is the original fixed result and is not optimized in this task.

## Dates, controls and information boundary

Original issue dates S: 2026-04-02, 2026-06-02, 2026-08-02. Original cutoff S-2.
Update offsets r = 0..59, R=S+r. Known-through C(R)=R-2 (experimental two-day lag,
not proof of historical availability). Business timezone Asia/Shanghai.

- F28: original archived daily forecast; no rewriting of issued values.
- R28-D: mean of 28 consecutive calendar days [R-29,R-2].
- R28-W: latest scheduled update U=S+7*floor(r/7), mean of [U-29,U-2].
- Explicit zero is valid; any missing/NULL/invalid day yields missing prediction,
  no imputation, no alternative window and no fallback. Use source `volume` truth.
- At R=S reconcile both update methods with original F28. A discrepant object is
  isolated, never forced to match or advertised as same-rule gain.

## Separate evaluation tracks

T0: preserve original F28, weekday control and B1 scores for [S+1,S+30],
[S+31,S+60], [S+1,S+60], plus joint-hit count. Never replace T0 with an update.

T1: every R predicts ONLY [R+1,S+60]. F28 uses original predictions on those dates;
R28-D/W use their rate times the identical remaining length. Main checkpoints r=
0,7,14,21,30,45 have 60,53,46,39,30,15 days remaining. Do not combine them into one
accuracy score, and never add realized sales to predicted remaining demand.

T2: every R issues fresh [R+1,R+30], [R+31,R+60], [R+1,R+60] for D/W only. No F28
extension beyond its original archived horizon. Truth maturity is WINDOW-SPECIFIC:
end after 2026-10-01 => NOT_MATURED, never truncate, fill zero or fetch data. Earlier
windows may be mature even when the same R's 60-day window is not; disclose separately.
Missing internal source days are UNKNOWN_TRUTH, not NOT_MATURED and not zero.

T2 full-core reporting retains 385 rows at each r. For longitudinal complete-60-day
comparisons, additionally show a predeclared first-two-issue panel (279 fixed rows)
because the August issue's 60-day targets are immature at r>0. Disclose missing truth
even within that panel; do not let changing mature denominators masquerade as gains.
Same-R D/W comparisons always share identical target dates and truth. Do not sum
overlapping-vintage errors into actual procurement loss or independent sample size.

## B1 diagnosis (not a new production candidate)

Read original implementation and library callbacks before coding. Preserve Poisson,
training context, feature/calendar rules, qualification, all parameters and clipping.
Reuse saved original fitted models if present; otherwise at most one original B1
rebuild for each of three issues. Save each rebuilt model locally before predictions.
Never refit to improve a failed score or to redraw a chart.

D0: original recursion, 62 steps including S-1 and S bridge days. Reconcile exact
archived original B1 core keys and values (max absolute tolerance 1e-8; report actual
deviation). If reproduction or interface checks fail, stop only affected D1 diagnosis.

D1: capture each B1 output, but feed the cutoff-known 28-day constant into future
sales state instead of B1's output, including bridge days. Do not replace observed
history, calendar features, parameters or source population. The captured model
output is the diagnostic forecast; a callback's returned feedback value is NOT it.
First recursive step equals D0. Persist features, feedback origin, proxy, prediction
and identifier alignment per step. Label FEEDBACK_INTERVENTION_DIAGNOSTIC.

Report bridge days separately, then 1-7,8-14,15-30,31-45,46-60 and original30/60
windows. Cover all original369 before showing error contributors. Decompose signed
early/late error change as change in predicted units minus change in actual units;
do not equate arithmetic contributions with operational causality. D0-D1 isolates
this specific feedback intervention, not every cause of forecast error.

## Metrics and statuses fixed before effects

Every output binds original issue, update date, actual information cutoff, shop, SPU,
model, target boundaries, length, prediction/actual, status and source/input hashes.
Keep current date out of historical acquired-at claims.

- HIT: positive known actual, available prediction, abs(error)/actual<=0.20.
- MISS: positive known actual, available prediction, outside tolerance.
- UNJUDGED: missing output, missing/immature truth or explicit-zero actual.
- Report HIT+MISS+UNJUDGED=full fixed core; separately report missing output and
  zero-actual absolute error. Positive-truth hit rate keeps missing outputs in its
  positive-actual denominator; full-core hit share is also retained.
- WAPE/Bias and over/under units use valid prediction/truth pairs; expose their
  counts and actual denominator. Known fixed-core truth is the hit-sales-share
  denominator, including known-actual objects with missing forecasts.
- Pairwise improvements use identical keys/dates/truth, classify absolute-error
  change with 1e-8 floating tolerance (not a business threshold). Report unscored
  pairs rather than silently dropping them from the main population.
- Stratify by issue, shop, original lifecycle and observed maturity. No retrospective
  current category is invented. Joint hits remain a separate 3-window outcome.
- Revision: compare per-day rate and constant-length standardized60-day amount
  between successive R; separately describe T1 horizon shrinkage. Reversal counts
  use changes greater than1e-8 and ignore unchanged days, not an economic order rule.

No acceptance threshold or parameter tuning is introduced. Report whether updating
improves remaining-demand decisions and/or fixed-length targets; never infer the
original60-day forecast or irrevocable purchase commitments have become correct.

## Budget, safety, tests and stop

Three actual B1 rebuilds max, each two threads/200 original trees and <=10 minutes;
two predictive paths per fitted model, no B2/B3. Rolling arithmetic:385*60 updates,
3 T1 methods and2 T2 methods/3 lengths. Local input <= original archive; no queries.
Archive daily issuance rates plus explicit daily future rows for D/W (60 days) and
F28 only within original end. New files refuse overwrite. Aggregate tables are not
the only retained evidence; per-object outputs and failures remain local.

Test delay, all boundaries, cross-month/year/week, missing days, IDs, future-data
perturbation, R=S equivalence, D1 first step/history preservation, no realized T1
dates, NOT_MATURED, immutable old hashes, independent scoring and same denominator.
After checks pass, run actual effects without another approval ceremony. A blocked
A subtask does not block independent B. Finish results and independent verification
before charts; then report and stop, no auto model selection/deployment.
