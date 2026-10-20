# Monthly historical replay: frozen extension protocol

This is a finite, user-authorized extension of BC60 V1/V3, not a new model or a
production selection policy. Base commit: 7fe6103255faf19a9b415255cc508b8b29b964da.
All results are RECONSTRUCTED_HISTORICAL_REPLAY; extending training history is
EXTENDED_HISTORY_RETRAIN_REPLAY, not exact reproduction of the short archive.

## Question, periods and comparison

Explain where forecast errors vary by issue month, shop and observed maturity;
separate descriptive seasonality/operating signals from demonstrated causes.
Freeze the issue grid 2025-01 through 2026-10, day 2 of each month, cutoff S-2.
Freeze target truth through 2026-10-01; complete 60-day issues end at 2026-08.
September and October remain NOT_MATURED, not shortened windows. Historical
support sought: 2024-01-01 through 2025-12-31, plus a small overlap revision audit.
Retain the existing 2026-01-01 through 2026-10-01 archive unchanged.

Each issue independently freezes each shop's trailing 90 calendar days, the
full-shop denominator, safe SPU mapping, cumulative >=90%, and boundary ties.
Do not derive 2025 cores from the 2026 list. Incomplete shop-date coverage makes
core selection INSUFFICIENT_HISTORY; do not claim a 90% list from a partial base.
Retain original 385 records only as a separately labeled historical panel.
No-qualification and unknown-qualification objects remain in complete cores.

W1=[S+1,S+30], W2=[S+31,S+60], W60=[S+1,S+60]. M1 training r+30<=C,
M2 r+60<=C; historical direct eligibility uses completed months at r-2. The
recursive candidates retain V1's actual qualification/segment implementation;
document that its eligible population is chosen at the actual issue cutoff.
Never use target-period sales for features, early stopping or model selection.

## Algorithms and execution budget

F28, WEEKDAY4, B1/B2/B3 use the verified V1 lock and source implementation.
DIRECT30x2 uses the verified V3 feature, label, eligibility and clipping rules.
No changed parameters, losses, weights, encodings, features, filters or tuning.
Exogenous inputs use the original pre-cutoff means and not future realized data.
Unavailable exogenous source fields are explicitly NOT_AVAILABLE; never silently
substitute B1 under the B2/B3 label. Incomplete individual covariates retain the
original locked handling and are reported, not filled with zero.

At most five formal fits per mature issue: B1/B2/B3, DIRECT M1/M2; total <=100.
Missing history/labels produce status without fitting. Two threads, <=10 minutes
per fit, no retries/refits for reporting. Save models, predicted trajectories,
fit attempts, errors and hashes per checkpoint; preserve failures. Recovery
loads existing checkpoints only. No database or production writes.

## Source query budget (frozen before new reads)

Only existing authorized MySQL access; no plaintext credentials, new ports,
accounts or paid APIs. Ephemeral read-only execution may reuse the established
SSH client path; no remote file writes or production task entrypoints.

- At most 400 data SELECT attempts, 450 native EXPLAIN FORMAT=JSON statements,
  and 30 metadata SELECT statements.
- Each query MAX_EXECUTION_TIME=10000 ms, transport timeout 90 seconds.
- Each data query estimated rows_examined_per_scan <=2,000,000; cumulative
  optimizer scan estimate <=240,000,000, counted before execution, failures too.
- At most 500,000 returned rows per query, <=10 GiB local new raw/normalized data.
- Source allowlist: ods_db.ods_lx_product_performance and the already verified
  monthly sales table for needed reconciliation. No source JOIN, ANALYZE or DDL.
- Read indexed boundaries first; fetch only missing intervals, serial bounded
  date chunks. No automatic budget increase. Checkpoint receipts and stop on
  query, authentication, timeout or budget failure; continue offline work.

Preserve raw records/SQL/plan, acquire time, source business and sync fields,
and hash. Unknown historical availability stays unknown. New overlap differences
are revision evidence, never overwrite old archived input or silently choose
the convenient truth. Do not generate daily history from monthly allocations.

## Scores and diagnostic boundaries

Show complete core coverage, each candidate paired with F28, and all-model
common set only as auxiliary. Compute WAPE/Bias from summed units, not averages
of row percentages. Report zero truth separately, unknown/absent output as
UNJUDGED, joint three-window hits and migrations. No output is not a hit.
Preserve monthly N, exact dates, all-shop future coverage and source gaps.
Overlapping issue targets are not independent or unique business units.

F28 structural audit: P1=P2=30*mean28, P60=P1+P2. With positive A1/A2,
max(A1,A2)/min(A1,A2)>1.5 precludes both equal predictions being within 20%.
This target-derived shape is retrospective diagnosis only, never a router.
Current categories have no historical-effective proof and remain descriptive.
Ad spending is not budget, indicators are not proof of an operating decision,
and isolated inventory zero is not proof of a whole-period stockout.

Native V4/A16 and existing rolling V2 results are separate side panels with
their original cutoffs, truth and denominators; no daily proration or joint rank.
Do not search A7 weights. Explain unavailable months and truth conflicts.

Independent checks cover calendar/bridges, mature labels, core ties/full denominator,
missing-vs-zero, source/mapping joins, historical qualification, fixed hashes,
prediction=sum segments, paired denominators, F28 shape and best/worst months.
Source/tests/specifications only to the independent GitHub branch. Business data,
models and results local only. Stop after effects, independent checks and report.
