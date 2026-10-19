# B+C core 60-day experiment V1 — pre-effect protocol

Status: source discovery; outcome computation is not authorized by this document alone.
The user has authorized readonly acquisition and local offline computation, not production changes.

## Decision and new V1 definitions

- Unit: shop × SPU, units sold; JQ-US, MT-US, RKZ-US, SY-US.
- At every issue date select the smallest descending-sales prefix reaching >=90% of
  observed full-shop trailing 90-day units; include all ties at the boundary.
  Select across the whole shop before lifecycle splits. Preserve unmapped units and
  source gaps; do not mislabel observed-source coverage as complete commercial coverage.
- These 90/60-day definitions are this round's proposed V1 operationalization, not
  a previously agreed historical policy.
- Keep days 1–30, 31–60 and cumulative 1–60 separately. A cumulative hit cannot
  override segment misses. Per-product tolerance is abs(prediction-actual)/actual<=0.2.
- Preserve the fixed core list across models; missing outputs fail coverage, unknown
  truth is unaccepted, explicit-zero truth uses absolute error, not a percentage.
- Report product hit rate AND the actual units carried by hit products. Neither WAPE
  <=20% nor 90% of core products hitting is equivalent to the business objective.

## Scope and governance

A7 weight search is closed. Frozen A16, V4, NEW_VISIBLE, procurement, BI and schedules
remain unchanged. Only sanitized source/tests/specifications enter this branch.
Business inputs/results stay in a new sibling local archive, never the repository.
No production venv changes; an isolated Windows venv will pin dependency versions.
V4/A16 monthly results are side references unless a prospective, frozen allocator
can genuinely align them; never rank incompatible windows together.

## Initial source query budget (frozen before source SELECT)

- Inspect only the three named daily candidates, ODS product performance and monthly
  sales metadata. Native EXPLAIN FORMAT=JSON precedes source SELECT.
- One connection, readonly transaction, MAX_EXECUTION_TIME=10000 ms per source query.
- No EXPLAIN ANALYZE, no writes, no production job imports.
- Each extraction request: date-bounded, four shops, projected fields, <=31-day chunk,
  no source-side multi-table joins; <=2,000,000 estimated scanned rows per query,
  <=50,000,000 cumulatively estimated scanned rows in this experiment, <=500,000
  returned rows per query, <=120 source SELECTs. These are ceilings, not targets.
- Discovery can use bounded/indexed MIN/MAX probes after EXPLAIN, metadata and <=20
  samples. A rejected query requires a narrower plan, not automatic budget increases.
- Stop on timeout or unclear grain; no blind retries. Preserve plans and timings.

## Before effects

Append exact issue dates, available-lag assumption, training/validation/report periods,
source/query/input/metadata hashes, library lock and fixed model configurations after
schema and coverage inspection, but BEFORE any candidate effects are calculated.
Historical availability not provable => RECONSTRUCTED_HISTORICAL_REPLAY; periods
already studied stay development/replay, never independent OOS.

Every training label's complete window must end and be available before its cutoff.
No random splitting; preserve issue-date results and overlapping-window dependence.
Core selection never sees forecast-period truth. No parameter search based on the
four exploratory products or any prior risk list. Missing history is not zero.

## Intended finite models

- Rolling daily and day-of-week controls.
- B1: fixed daily LightGBM + MLForecast with sales/calendar/history only.
- B2: same configuration plus strictly lagged/forecasted traffic/conversion if source
  semantics and temporal alignment pass. Never use realized future covariates.
- B3: advertising only when numerator/denominator and availability gates pass.
- Chronos-2 remains a finite alternative, not part of a broad search.

Sessions are not deduplicated visitors summed across variants. Ratios must be rebuilt
from validated numerators/denominators. Missing budget, supply or activity stays UNKNOWN
and does not by itself block B1. New products remain in the list but are not modified
by an established-product candidate; unresolved new-product windows block overall PASS.

## Locked implementation notes (before effects)

The exact lock is `bc60_design_lock_v1.json`. Issue dates are Apr 2, Jun 2 and Aug 2,
2026; available-through dates are Mar 31, May 31 and Jul 31 (assumed two-day lag).
Day 1 is the day AFTER issue. Prediction recursively covers two bridge days plus 60
scored days. The three scored windows do not overlap. No hyperparameter selection is
performed on these results. Their history has already been researched: all remain replay.

Actual schema inspection found DWD/DWS only begin June 2026; ODS supplies the bounded
Jan 1–Oct 1 historical daily facts. ODS January records were acquired/backfilled later;
no claim of point-in-time access. A previous authorized monthly archive supplies
observed positive-month eligibility, not unverified negative eligibility. Only >=4
observed positive months qualifies the established candidate; absence is UNKNOWN or
insufficient evidence, not a claim about production NEW_VISIBLE qualification.

MLForecast 1.0.2 rejects null target inputs. Therefore unknown days stay null in the
archive, while training is split into contiguous observed segments of >=56 days.
Lags never bridge missing calendar dates. Prediction requires such a segment ending
on the available cutoff. This is an implementation of the locked missing-history
rule, not zero imputation. All eligible context objects are considered, not only core.

Daily recursive training has ONE-day labels fully observed by the available cutoff.
No incomplete 60-day labels enter training, tuning or model selection. No random split.
Traffic uses lagged 28-day sums/means and order-count/session-event ratios; session
events are not deduplicated visitors. Future covariates use fixed cutoff-available
means, never actual future traffic/advertising. Budget/stock/activity remain UNKNOWN.

The primary positive-actual hit denominator includes missing-model-output products.
WAPE/Bias necessarily use scored pairs and disclose output/truth coverage. A secondary
common-output comparison is explicit and cannot establish full-core business acceptance.

Source quality handling before effects: invalid negative advertising counts are kept
in raw evidence but become missing covariates, never zeros. No candidate may use them
as physical negative clicks/orders. ODS is the single daily truth/feature definition;
other layers and monthly truth are reconciliation references, not silently mixed
replacements. Found mapping conflicts remain explicitly flagged; no string-based SKU
repair, source JOIN or demand multiplication is performed. Reconstructed feature
experiments do not establish historical advertising attribution finality.
