# DIRECT_YEARAGO_BLEND50 — fixed half-adjustment preregistration

User authorization: one fixed offline arithmetic candidate, no further approval
needed within this scope. Zero model fits, inference calls, database queries or
production changes. No alternative weights, selection changes or parameter search.

## Immutable sources and formula

- GATE source commit: 601e3d2c1f592165141d526eb9b6571dd557b0aa.
- YEARAGO source commit: 83e0baa32f6de5cea6ff38e19c87f56fba67daba.
- DIRECT replay reference: 34bdc3b703c45326f3c638814ad18595c8124e83.
- Read final sealed GATE routing and final reviewed source predictions only.
- Key: issue_date/shop/spu; retain original GATE decision unchanged.
- For each M1/M2 segment with TRUE decision:
  `blend = direct + 0.5 * (gate - direct)`.
- FALSE: copy original DIRECT exactly, without arithmetic. Missing original
  outputs remain missing. Unexpected absent or contradictory GATE output is an
  integrity error, never a fallback. No rounding, clipping or imputation.
- Total60 is the sum of the two blended segments. Same weight for both segments,
  every issue, shop and product. Neither F28 nor CONTEXT90 enters the formula.

Whitelist-only candidate generation reads keys, cutoff, original decision,
unrounded DIRECT/GATE/annual segment forecasts and origin hashes. It never reads
target truth, errors, hits or score-driven metadata. Seal output, source hashes,
formula/code hashes and actual timestamp before scoring. Integrity byte hashing
does not parse score fields. All old source/model/report files remain unchanged.

## Fixed scope and comparisons

20 mature issues Jan2025-Aug2026; full 2,820 object-issues, original complete paired
2,579; original 241 missing outputs remain. Sep/Oct 274 records stay NOT_MATURED.
S cutoff S-2; targets S+1..30, S+31..60 and sum. Same truth/version/qualification.
Score DIRECT, GATE, YEARAGO and F28 on the same objects as the new candidate.
Report full, paired and original GATE TRUE/FALSE, shop, issue, original maturity,
original decline group and descriptive category/season. No changed main denominator.

Five unrounded screens versus DIRECT: paired60 WAPE strictly lower; joint hit count
higher; joint hit actual60 share not lower; original decline-group60 overprediction
rate not higher; output/core coverage not reduced. Unknown denominator => unknown.
Compare costs and gains versus GATE too. Known-zero truth has no relative error;
unknown truth/output is not a zero error or successful hit. Joint hit requires
positive actuals and all three absolute relative errors <= .20, counted once.
Joint sales weight uses actual60 once; overlapping issue weights are not real
lost sales or deduplicated sales volumes.

## Four fixed migration groups and diagnostic lists

Independently recompute DIRECT-vs-GATE joint categories: both hit, GATE gained,
GATE lost, neither hit. User reference counts are reconciliation expectations,
not computed values. Report each group's BLEND hit/remaining failures, actual60
weight and differences. Both-endpoint joint hits must remain hits. New joint hits
can arise from neither-hit group; do NOT reuse old subset-of-YEARAGO-hit assertions.

Retain the exact previous `scoring/top20_exits_actual` keys and order (original
YEARAGO exits diagnosed by GATE); do not re-sort for favorable BLEND results.
Disclose this lineage and additionally identify GATE-lost membership within that
list. Report each window's signed relative/absolute error, maximum window error,
over/under direction, and joint status. Never use the list in selection.

## Independent validation and arithmetic bounds

Validate key uniqueness/joins; exact FALSE copying; TRUE formula; missing/zero;
date boundaries; segment sum; truth deletion/reordering/perturbation invariance;
same source/config/model hashes; original qualification and immature statuses.
Use independent scalar accounting versus vectorized primary scoring, with the
already independently verified per-object truth and explicit hash/validation chain.
Do not rerun old models or old validation entrypoints that call prediction.

For each scored key/window check absolute error, positive overage and positive
underage <= mean of corresponding DIRECT/GATE values. Arithmetic comparison may
allow only IEEE-754 roundoff (`32 * eps * max(1,abs(D),abs(G),abs(B),abs(y))`);
this numerical tolerance NEVER relaxes the strict .20 hit rule or five gates.
Recalculate exact same-sample aggregate WAPE/decline-over bounds from raw values.
These convexity properties are implementation checks, not new independent evidence
of predictive power or guarantees about future endpoint performance.

## Delivery and stop

New exclusive local directory, separate GitHub branch. Only sanitized code/tests/
specification are pushed; all business inputs/results/models remain local.
Deliver Markdown report, predictions/origins, four-group transitions, slices,
independent checks, hashes and executable reproduction instructions. No visual app
is needed. Full source-backed arithmetic trail serves as the runnable companion.

Classification: reconstructed historical development validation, rule inspired by
already-observed GATE results; NOT independent OOS or prospective success. Even all
five passing would not establish the final core-product accuracy goal. Stop after
this single weight; no deployment, merge, new shadow run or subsequent experiment.
