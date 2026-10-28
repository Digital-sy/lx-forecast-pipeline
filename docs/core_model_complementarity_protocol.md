# Historical core-model complementarity diagnostic

This is a descriptive reanalysis of frozen, repeatedly studied historical forecasts,
not a new model experiment, an independent OOS test, or a deployable router.

## Frozen scope

- Mature issue months: January 2025 through August 2026 inclusive.
- Full core: all original shop × SPU × issue keys, including missing outputs.
- Primary paired scope: the original DIRECT complete-output keys, never the
  intersection selected by the performance of six models.
- Models: F28, DIRECT30x2, DIRECT_CONTEXT90, DIRECT_YEARAGO,
  DIRECT_YEARAGO_GATE_V1, DIRECT_YEARAGO_BLEND50.
- Use unrounded parquet forecasts and the previously independently daily-summed
  truth. Check keys, exact forecast copies, truth, target dates, and archive hashes.
- Keep original 1–30, 31–60 and 1–60 day windows. Positive actuals only can pass
  the original absolute relative error <= 0.20 test. Zero actuals retain absolute
  error and an undefined percentage state. Missing forecasts remain missing.
- No training, inference, new predictions, source queries, threshold search,
  segment stitching, new-product reclassification, or prospective-batch scoring.

## Definitions fixed before this analysis

1. Joint hit requires all three windows from ONE existing model to hit.
   A BLEND50 miss is rescued only if another complete model jointly hits.
   The union is deduplicated by shop × SPU × issue. Report missing BLEND50
   outputs separately from its evaluable misses.
2. Actual-sales weight is the 60-day actual once per object × issue, never a
   sum of windows or successful models. Across issues this is an evaluation
   weight, potentially overlapping sales, not unique sales or procurement loss.
3. Distinguish all six models evaluated and missing-output/unknown-truth
   records. Never call an unobserved forecast an evaluated failure.
4. Within all-six-output joint misses, classify each window by raw signed errors:
   all above actual, all below actual, opposite signs, exact/tie mixtures.
   Also report existing +/-20% bands (all above, all below, opposite outside
   bands, at least one inside). Zero truth and unknown remain separate.
5. Primary Top 20 ranks shop × SPU by the sum of BLEND50 60-day absolute error
   across the fixed paired scope, including small errors in joint-hit records.
   Also retain non-joint-only error and the sum of the TWO disjoint segment
   absolute errors. Do not add 30/30/60 errors and double-count time.
6. Describe persistence with observed/scored issue counts, joint misses,
   consecutive calendar-month miss runs, largest one/three-issue error shares,
   and issue-level errors. Do not invent a persistence threshold.
7. For every Top 20 product, show each fixed alternative across all the same
   issues: improved/degraded/tied absolute errors, total error, joint hits,
   rescue/loss counts and year-specific results. This is retrospective evidence
   of complementarity, not a future-truth-trained selection rule.

## Evidence and delivery

The new output directory must not exist. Frozen sources and all files of the
issued prospective batch are hashed before/after without parsing prospective
predictions. The original shadow batch is never an analysis input.

Deliver local CSVs and a concise Chinese report. Independent scalar validation
must recompute unions, truth alignment, signs and Top 20 aggregates without
calling the analysis aggregation functions. Synthetic tests cover overlap,
missing/zero, boundaries, segment stitching and duplicate keys.

Only source, tests and this sanitized protocol enter an independent Git branch.
Business inputs, results, models, source paths containing credentials, and
prospective artifacts do not enter Git. Stop after this diagnostic.

## Archive serialization finding

The CONTEXT90 scoring code read old DIRECT/F28 CSV copies using pandas' default
float parser. The later reviewed YEARAGO/GATE/BLEND50 archives retained the
round-trip values. This diagnostic verifies both copies against their EXACT
respective parses of the same hash-verified CSV, and verifies no hit changed.
It selects the canonical round-trip baselines and only CONTEXT90's own candidate
from that archive. No predictions are rounded, repaired, or tolerance-scored.

## Reproduce in the existing isolated analytics environment

Use the previously validated environment (pandas 2.2.3, numpy 2.2.6 and its
existing pyarrow version); do not install into production. Run from this checkout:

```powershell
python -B -m unittest tests.test_core_model_complementarity -v
python -B scripts/core_model_complementarity.py --archive-root '<local-archive-root>' --output '<new-exclusive-output>' --protected-shadow '<immutable-issued-batch>'
python -B scripts/core_model_complementarity_validate.py --output '<new-exclusive-output>'
python -B scripts/core_model_complementarity_report.py --output '<new-exclusive-output>'
```

The main computation refuses an existing output directory. Validation and report
also refuse overwriting their respective existing artifacts. Reproduction creates
no model forecasts and does not call a database, API, training or inference entry.
The analyst must retain the frozen source tree. Output tables contain business
data and must remain local, outside the source checkout.
