# DIRECT training / business alignment diagnostic

Scope: read archived matrices and predictions only. No fitting, inference, database,
API, production changes, new model or changed eligibility. ITEM_ID and TRAFFIC_STATE
remain rejected research candidates. Historical dates and the sealed first shadow
remain unchanged. Output must be a new directory and must not be inside an input.

Per fit unit (20 issues x two segments), project ITEM_ID's verified training matrix
back to the original DIRECT columns. Independently verify every audited original
column hash and dtype against the original DIRECT receipt. Check model text, not
load or evaluate model objects. Implicit sample weights are one only after confirming
the actual archived fit call and configuration supplied no sample_weight.

Historical core status is descriptive, never a training filter or weight. At each
row's own cutoff, use the preceding 90 calendar dates, full shop sales denominator,
mapped products ranked by units and all boundary ties. Establish core before mature
eligibility. Insufficient shop calendar, invalid values, insufficient object days
or ambiguous mapping => UNKNOWN, not noncore. Compare reconstruction against the
archived issue core lists and explicitly report any stricter completeness differences.
Current/target sales never define historical core status.

Report each unit separately. Row-weighted summaries across units are repeated
training exposures, not independent samples. Greedy disjoint 30-day intervals per
item are a descriptive maximum disjoint subset, not statistically independent data.
Row, explicit/implicit weight and label-volume shares are not gradient/loss shares.

Reuse eight archived models on the fixed 2820 full / 2579 paired universe, preserving
241 missing outputs and the 842 paired declining records. Score mean Poisson
deviance D=2*(y*log(y/mu)-y+mu), with y=0 giving 2*mu. No epsilon or prediction
alteration. For mu=0,y>0 the extended diagnostic is infinite; mu=y=0 has limit zero.
Report strict library domain counts (mu>0) separately. Compare finite-domain formula
against sklearn 1.6.1. Missing/invalid output remains unscored, not zero.
Joint hits require positive actuals and unrounded <=20% in all three windows.

Trace both unseen segments from preserved monthly qualification records and daily
known masks, following the frozen exclusion order. Record when four positive
completed months first exist, each earliest target completion, and actual cutoff.
No change to qualification. Preserve seen-both regressions and unknown evidence.

Sources are reconstructed historical archives, not point-in-time or independent
OOS evidence. Propose at most one next experiment; do not execute it. Publish only
sanitized source/tests/this protocol to an independent GitHub branch. All numerical
summaries and business details remain local.

Official references checked: LightGBM 4.6.0 Parameters and regression_objective.hpp;
scikit-learn 1.6.1 mean_poisson_deviance (requires y>=0 and mu>0).
