# DIRECT_YEARAGO_GATE_V1: fixed replay and exit diagnosis

Authorization: user's YEARAGO applicability replay task. This specification is
frozen before computing this rule's outcomes. No fits, inference, database access,
production writes, alternate rules or threshold search are authorized.

Sources: YEARAGO reviewed artifacts at commit
83e0baa32f6de5cea6ff38e19c87f56fba67daba; DIRECT replay reference
34bdc3b703c45326f3c638814ad18595c8124e83. Context90 supplies only the
original decline membership. Original artifacts are immutable and hash checked.

## Selection, before truth

Key = issue_date/shop/spu. Select both genuine annual predictions only if:
both original DIRECT outputs are finite; annual input28 and both annual target30
windows are complete with valid source/mapping; a valid positive sale is observed
on or before annual input start; both archived training units pass the unchanged
30 distinct dates / 10 products / varying annual features screen; both predictions
have ANNUAL_MODEL origin and are finite. Otherwise select both DIRECT outputs.
Missing evidence is a failed condition, never proof of a zero or not listed.
Hash mismatch, duplicates and contradictory evidence stop execution.

Annual date shift retains the same calendar date (Feb29 -> Feb28 only), cutoff
issue-2, annual input cutoff-27..cutoff, annual targets anchor+1..30/+31..60.
All selection evidence is no later than its issue cutoff. Exact archived floating
values are preserved, with no mixing, clipping, rescaling or rounding.

Routing code is isolated from scoring code. Its readers project only keys, windows,
completeness/support/origin and predictions. Existing daily data is read separately
per issue only through that issue's annual input start to recover the earliest
valid positive observation date. No target truth/error/hit fields are loaded.
Seal routing rows, access log, source hashes, code hashes and real timestamp before
any score read. Hashing file bytes for integrity is not parsing its truth fields.

## Fixed evaluation

20 mature issues Jan2025-Aug2026. Report full core, original DIRECT complete
scope, and TRUE/FALSE strata without dropping failures. Sep/Oct remain immature.
Compare DIRECT, original YEARAGO comparison stream, GATE, F28; total = two segments.
No context90 predictions. Use exact original decline group and denominator.
The five unchanged unrounded screens are: lower paired60 WAPE, more joint hits,
nondecreasing joint hit actual60 share, nonincreasing decline60 overprediction rate,
and no output/core coverage reduction. Unknown denominator => unknown screen.

Joint means all three relative absolute errors <= .20 with positive actuals,
counted once per key. Unknown actual/output is not zero; zero actual has separate
absolute error. Report all window metrics, metadata slices and migrations.

## Fixed diagnostic bins (never selection conditions)

Original YEARAGO exits: excess=max(abs relative error across3)-.20.
Bins: (0,.02], (.02,.05], (.05,.10], (.10,infinity).
Original gains: entry_margin=.20-max(abs relative error across3).
Bins: [0,.02], (.02,.05], (.05,.10], (.10,.20]. These are descriptive
distances inside the boundary, not relaxed success thresholds. Floating comparisons
are unrounded. All3 actuals must be positive; otherwise retain unclassified status.
Top20 exits ranked independently by actual60 and extra absolute error60,
descending; ties sorted issue/shop/spu. 'Large exits' in headline refers precisely
to this top20 actual60 list, not a new model eligibility group. Per-window error
changes also retained. Sales weights across issues are not realized sales losses.
Sensitivity excludes largest product by paired actual60 and, separately, each issue;
these are side analyses and never change the main scope.

## Verification / stopping

Independent row-wise score path, raw daily truth sums, exact fallback and sources,
all-window sum, original missing outputs, joins, truth perturbation/deletion/shuffle
invariance, genuine origins, single unsupported segment, missing/zero separation,
and unchanged source/core/prediction/model/config hashes. GATE gains/losses must
be subsets of original YEARAGO gains/losses. No model loading or prediction calls.

Classification: post-hoc discovered rule, reconstructed historical development
replay; never independent OOS, prospective or proven historical availability.
Complete windows do not establish operating comparability. Numerical success can
only support later prospective consideration. On failure stop without a second rule.
Only sanitized source/tests/specification go to the independent GitHub branch;
all business results remain local. Markdown/CSV deliverables take priority.
