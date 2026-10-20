# DIRECT / F28 archived cross-diagnostic protocol

This is descriptive post-hoc analysis, not a new prediction experiment.
Reference commit: `34bdc3b703c45326f3c638814ad18595c8124e83`.
Only existing authorized local archives may be read. No fits, database queries,
model changes, core reselection, truth revisions, production writes or routing.
Business inputs and outputs must remain outside the source repository.

## Population and joins

- Retain the original monthly_core universe and all three windows. Mature issues
  are the archived mature grid, January 2025 through August 2026. September and
  October remain NOT_MATURED in the separate status export.
- Join scores by cohort_id, issue_date, shop, spu, window. Each model must be
  unique. Actuals, dates and original attributes must agree before the merge.
- Current metadata join is shop × spu. Exact duplicate labels may collapse;
  conflicting nonmissing values become CONFLICT, absent values UNKNOWN.
  Preserve raw source values and source states. Never normalize category names.
  All current labels are CURRENT_NOT_POINT_IN_TIME, not historical features.
  Source literal `未映射` is retained as its raw display value but counted as
  semantically unknown and excluded from informative conditional comparisons;
  it is not treated as a genuine product category. This is label-quality handling,
  not a model/core selection change.
- Operating evidence joins once per object × issue. Training support joins once
  per issue, with M1/M2 separate. Validate uniqueness and quantity conservation.

## Metrics and error categories

Keep full core and common scorable populations side by side. WAPE/Bias use the
actual units of records with both known output and known truth; full-scope hit
rate uses all positive-truth records, including missing predictions as nonhits.
Report denominator membership, output coverage, known truth, label coverage,
distinct shop × spu and distinct issue counts.

Object-level classification precedence: NOT_MATURED, MISSING_TRUTH,
MISSING_PREDICTION, ZERO_ACTUAL, then BOTH_SEGMENTS_HIT,
CUMULATIVE_HIT_SEGMENTS_MISS, BOTH_OVER_20, BOTH_UNDER_20, MIXED_TOTAL_MISS.
Preserve independent flags so a missing prediction does not erase zero truth.
Classification uses original unrounded values and ±20%. Check A60=A1+A2 and
P60=P1+P2 with machine-precision tolerance, never changing stored values.
Report cumulative and sum-of-segments absolute error separately, never adding
the two as independent losses. Joint success is counted once per object.

## Comparisons and weights (fixed before computing cross-results)

Raw hierarchical tables cover season, category, issue, calendar month, shop,
age, and their requested combinations. Full results are not replaced by pairs.
No new operating bins or materiality thresholds are introduced.

Conditional descriptive comparisons use W60 model-common rows:

1. Category: same shop × issue month × original maturity.
2. Season: additionally same raw current category.
3. Issue month / calendar month: same shop × raw category × maturity.

Compare every observed pair of known dimension values, not selected winners.
Only strata with positive actual-unit denominators in both groups enter.
For each pair and stratum use one common weight
`(actual_a + actual_b) / sum_over_common_strata(actual_a + actual_b)`.
Apply this identical weight to both groups and both models' stratum WAPE/Bias.
This is standardization on observed overlap, NOT causal adjustment. Export
stratum weights, retained/excluded records and unit coverage. Never fill absent
strata. Small strata remain visible. Report survivor-selected same-product
adjacent and year-over-year comparisons separately.

## Support and rankings

Pooled comparison combinations meeting at least 30 pairs, 10 distinct products
and 3 issue periods are SUPPORT_SCREEN_PASS. Others remain LOW_SUPPORT.
Single-period cells may have adequate counts but are never cross-period proof.
These are display screens, not statistical significance or stable effects.

Rank 1: pooled shop × raw category error rates with support, plus other strata
as clearly separate views. Rank 2: absolute error on disjoint
shop × raw category × issue-month cells. Do not add overlapping tables.
Stability: monthly improvement/decline, largest-product error share and
positive-improvement share, November 2025 share, and leave-November / leave
largest-error-product sensitivity as side tables only. Same-calendar-year
repeatability is possible only January–August; other months have one year.

## Validation and delivery

Separate validator expressions must re-read original scores rather than import
analysis metric/classification helpers. Check row/quantity conservation,
truth identity, classifications, migrations, group totals, common denominators,
unknown preservation, source hashes before/after, and conditional weights.
New tests use synthetic records only. Markdown and CSV are primary outputs;
no new application, training, model selection or follow-on execution.

## Reproduction (authorized local archive only)

Use the existing isolated analysis environment with pandas, numpy and parquet
support. Do not install into or execute a production environment. Run from the
repository root; replace the placeholder paths with authorized local paths.
The output directory and each verification receipt must be new. No database or
network credentials are needed by any of these commands.

```text
python -B -m unittest discover -s tests -p test_direct_f28_cross_diagnostic.py -v
python -B scripts/direct_f28_cross_diagnostic.py --root <ARCHIVED_REPLAY> --out <NEW_DIAGNOSTIC>
python -B scripts/direct_f28_cross_validate.py --root <ARCHIVED_REPLAY> --out <NEW_DIAGNOSTIC> --receipt <NEW_DIAGNOSTIC>/validation.json
python -B scripts/direct_f28_cross_report.py --data <NEW_DIAGNOSTIC> --validation <NEW_DIAGNOSTIC>/validation.json --out <NEW_DIAGNOSTIC>/report
python -B scripts/direct_f28_cross_report_check.py --data <NEW_DIAGNOSTIC> --report <NEW_DIAGNOSTIC>/report --receipt <NEW_DIAGNOSTIC>/report_check.json
```

The report generator statically parses the frozen DIRECT feature declaration;
it never imports or runs the forecasting entry point. The independent validator
reads the original score records and checks the derived base and all metric
cells through separate arithmetic. The supplementary checker verifies the
nine report tables and source hashes again. This is a separate computation
path by the same executor, not a claim of second-person peer review.

### File dictionary

- `base_windows`: unique object/issue/window with unchanged predictions/truth.
- `base_objects`: one object/issue; mutually exclusive error classes and flags.
- `field_lineage.json`: real source paths and join grain.
- `layer_metrics`: FULL_CORE and MODEL_COMMON; `actual_units` is full known
  truth, whereas `scored_actual_units` is the WAPE/Bias denominator.
- `layer_pairs`: same-model-common population for each cell; deltas are
  DIRECT minus F28. Product counts across improvement/decline can overlap.
- `layer_joint_once`: three-window joint hit once per object, not per window.
- `conditional_comparisons` / `conditional_weights`: observed overlap only,
  explicit retained/excluded coverage and identical weights.
- `report/conditional_scope_coverage`: separates the raw model-common population,
  label-eligible population and retained common-stratum population. The original
  conditional table's `full_*` denotes label-eligible rows, not all original
  paired rows. Both unit-coverage denominators are exported explicitly.
- `ranking_1_supported_error_rates`: supported pooled shop/category; duplicated
  `pair_n_x`/`pair_n_y` refer to metric/support joins and must match exactly.
- `ranking_1_low_support_retained`: no small group is silently removed.
- `ranking_2_disjoint_error_units`: disjoint shop/category/issue error shares;
  missing predictions have unknown AE, not zero AE.
- `combination_stability`, `calendar_repeatability`, `survivor_details`:
  original results and clearly separated sensitivities/survivor comparisons.
- `operating_details` / `operating_summary`: post-hoc indicators, not causal
  labels or historical input authorization.
- `validation.json`, `report_check.json`: executed verification receipts.
- `run_manifest.json`, `report_manifest.json`: input/output/source hashes.

Business records and generated results remain local and outside Git tracking.
Only the analysis, checks, synthetic tests and this protocol are committed.
