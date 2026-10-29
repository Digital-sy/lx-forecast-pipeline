# Immutable shadow actuals and mature scoring

This is a manual monitoring adapter, not a model or scheduler. It never loads a
model, calls an API, writes a business database, or replaces a sealed prediction.
`manual_policy.json` is a disabled policy description, not an installed task.

## Fixed population and timing

The issue seal, original core, predictions and decline labels are hash-checked.
Every original object remains in every model/window skeleton, including missing
predictions. The paired population is derived once from the original candidate's
complete two-segment output. F28 is reported on both the full and paired ranges;
it never fills candidate gaps. Another issue's cohort or windows are rejected.

Real manual runs use the server's timezone-aware clock. Eligible actual dates
end at the Shanghai business date minus two days, capped at the sealed target
end. If that cutoff precedes the target start, return `WAITING_TARGET_START`
before initializing a database transport. The CLI has no historical clock flag.
Test functions accept explicit synthetic clocks; those results are TEST_ONLY.

An elapsed calendar window is insufficient. The original four-shop daily
denominator, all fixed-core daily truth, source completion and mapping must be
known. An unfinished or incomplete window cannot publish official WAPE, Bias,
hits or joint hits. Partial observed quantities are separate from full-window
actuals. No daily forecast is manufactured from a 30-day prediction.

## Read-only source acquisition

`shadow_actuals_source.py` reuses the reviewed ODS probe, fields, primary key,
normal-sync log completion, and quantity normalization. `ADAPTER_BASELINES`
records the original pure-function sources. No production module is imported
on local import. The existing database factory is loaded only by the bounded
server-side probe when an eligible read is explicitly requested.

The source transport is the existing SSH route. It preserves StrictHostKeyChecking,
uses native EXPLAIN, the original read-only transaction and timeout, serial chunks
of at most two dates, and finite query/scan budgets. No credentials enter files,
arguments, outputs or Git. There is no automatic retry loop.

Run from the authorized local environment with explicit existing paths:

```text
python scripts/shadow_actuals_source.py --run SEALED_RUN_MIRROR --out NEW_SOURCE_VERSION --previous PREVIOUS_SOURCE_VERSION --frozen-mapping ORIGINAL_INPUT_RAW_PARQUET --frozen-mapping-sha256 VERIFIED_ORIGINAL_SHA --execute-read
```

Omit `--previous` on the first actuals version. Without `--execute-read`, a mature
request only produces a read plan; a premature target still returns waiting.
The frozen ASIN/SPU mapping anchor must come from the sealed input version at or
before its cutoff, not a replacement current mapping. Missing/changed/new mappings
are explicit evidence states. Affected object truth remains unknown; raw quantities
still remain in the full-shop denominator.

Each new package retains full raw slice records, receipts, source and acquisition
times, normal-sync completion/page evidence, aggregates, mapping differences and
hashes. A new version re-reads only the needed tail plus one boundary day, compares
that day by key, and keeps the earlier version. It never silently deduplicates.
`verify_package` derives completion and aggregates again from the evidence and
binds the package to the exact issue seal, core and target range. A hand-written
PASS is not sufficient. These operational receipts are not independent upstream
business truth; existing accepted source limitations continue.

The existing log reader covers only recent normal logs. Preserve versions with
manual checks while that evidence is available. Missing historic completion
proof remains a blocker; do not reconstruct it from MAX(date) or a nonzero count.

## Monitoring and mature scores

```text
python scripts/shadow_actuals_monitor.py run --run-root SEALED_RUN --out NEW_MONITOR_RUN --source-package VERIFIED_SOURCE_VERSION --review-template ORIGINAL_REVIEW_TEMPLATE --journal SEPARATE_REVIEW_JOURNAL
```

The source package is optional before the target starts. A missing package after
eligible dates exist returns `SOURCE_INCOMPLETE`, not invented zero actuals.
Output directories must be new and outside the sealed run. Each manual evaluation
binds a specific actuals version, so later revisions produce new results rather
than overwriting earlier scores. Failed or incomplete directories are retained.

`monitoring_status.csv`, `window_status.csv`, `future_coverage.csv` and
`manual_review_status.csv` show progress independently from the original reference
page. Formal score tables are written only after at least one full window passes.
The full population gate applies to the paired comparison too; shrinking to an
available subset cannot create a complete result.

The pure scorer keeps the existing definitions: WAPE=sum(abs(p-y))/sum(y),
Bias=sum(p-y)/sum(y), separate over/under quantities, unrounded ±20% on positive
actuals, and one three-window joint hit per original object. Zero actuals have
absolute error, not fabricated relative percentages. Missing predictions count
toward coverage failure, not zero error. Known zero total denominators remain
undefined. Full-store totals separately measure the fixed core's and output
objects' future-sales coverage. Scoring never uses training weights.

## Human review

Reuse the existing review template. The monitor references and validates it; it
does not duplicate blank templates or fill owners, judgments or approvals.
Human-submitted records can be appended to a separate private journal:

```text
python scripts/shadow_actuals_monitor.py append-review --run-root SEALED_RUN --journal SEPARATE_REVIEW_JOURNAL --record HUMAN_RECORD_JSON
```

The record identifies run/shop/SPU/window and the exact original candidate, plus
manual reference quantity, reason, evidence and owner. Optional confirmation,
effective and occurrence times stay unknown if absent. The audit `recorded_at`
and `ingested_at` use the actual clock. A supplied older recorded time is retained
only as `declared_recorded_at`; it is not treated as proven prior availability.
Revisions explicitly cite the prior event, retain it, and create a new exclusive
event file under a writer lock. An unresolved lock blocks instead of being removed.
Whole-event hashes protect timing and history links as well as record contents.
These controls do not independently authenticate the human declaration.

All manual values are post-issue references, not procurement quantities, original
forecasts, or evidence that a later human judgment fairly beat an earlier model.

## Verification and operation

```text
python -m unittest discover -s tests -p "test_shadow_actuals_*.py" -v
python scripts/validate_shadow_actuals_historical.py --source EXISTING_ACCEPTED_HISTORICAL_ARCHIVE --out NEW_TEST_ONLY_RESULT
```

Historical validation checks the same scoring function against accepted sealed
predictions/truth and independent prior metrics; no historical training or
inference is invoked. Synthetic source/clock/failure tests are not current real
actuals. Future live source and completeness conditions still need verification
when those dates become eligible.

Suggested manual cadence is daily after normal source synchronization completes.
Do not install cron, workflow tasks, monitors or services from this document.
Stop use by making no further manual call; keep original issues and evaluations.
Maturity dates, evidence and actual acquisition state determine when scoring can
occur; no future unattended execution is promised.
