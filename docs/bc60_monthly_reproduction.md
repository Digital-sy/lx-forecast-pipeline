# Bounded monthly replay reproduction

Use the existing locked BC60 virtual environment; do not install into production.
The protocol commit precedes data reads and effects. The engine/model source
hashes are enforced by prepare/run. Reports may receive formatting or diagnostic
clarifications without changing any fitted prediction.

## Local archives

Define `ROOT` as the new monthly archive, `V1` as the read-only original BC60
archive, and `PYTHON` as its existing `.venv/Scripts/python.exe`. Do not put data,
models, receipts, credentials or private results in this repository. Each output
directory must be new. Keep the existing V1/V2/V3 archives unchanged.

1. `bc60_monthly_source.py` performs metadata/native-EXPLAIN-gated read-only
   extraction through the existing authorized SSH path. `bc60_monthly_acquire.py`
   serializes missing-history chunks. Do not rerun completed queries. The frozen
   budget and stop conditions are in `bc60_monthly_protocol.md`.
2. Normalize only the missing historical partitions with
   `bc60_monthly_inputs.py --root ROOT --v1-inputs V1/inputs_v1_final --out ROOT/inputs`.
   The separately acquired overlap audit never overwrites the old input segment.
3. Freeze lists before effects:
   `bc60_monthly_replay.py prepare --inputs ROOT/inputs --archive V1 --out ROOT/prepared`.
4. Execute the exclusive fit ledger once:
   `bc60_monthly_replay.py run --prepared ROOT/prepared --out ROOT/effects`.
   This is a finite local computation, not a production/online shadow entrypoint.
   At most five fits per mature issue and 100 in total. Failure is retained.
5. Score saved outputs without fitting:
   `bc60_monthly_score.py --inputs ROOT/inputs --prepared ROOT/prepared --effects ROOT/effects --out ROOT/scoring`.
6. Generate descriptive tables with `bc60_monthly_diagnostics.py --root ROOT
   --out ROOT/diagnostics --current-metadata ROOT/side_panels/current_metadata_mapping.csv`.
7. Run `bc60_monthly_validate.py --root ROOT --archive V1 --output ROOT/validation`
   for independent source/calendar, cohort, training and saved-model checks.
   Validation never refits. An optional completed-slot review uses
   `--checkpoints-only`; that partial result is not the final numeric gate.
8. Generate `bc60_monthly_findings.py --root ROOT --out ROOT/findings` from the
   completed scores and diagnostics. It calculates paired comparisons and
   descriptive explanations only; there are no new queries or fits.
9. Generate the actual report only after numeric validation:
   `bc60_monthly_report.py --root ROOT --out ROOT/report --validation ROOT/validation/validation.json`.
   Notebook and official report-app generators consume these reviewed local
   outputs; they do not access a database or fit a model.

All commands above are script arguments following `PYTHON -B scripts/…` from
the repository root. Use the source hash recorded in each local manifest. Raw
filesystem SHA256 and Git blobs may differ in line endings; both identities
are recorded rather than treating either as a historical acquisition timestamp.

## Interpretation and recovery

Twenty mature issue months and two immature months are retained. Daily target
windows are exact days, not natural months. Monthly V4/A16 and V2 rolling
comparisons remain separate native-scope side panels. No missing cell is zero.

Full-core, candidate/F28-paired, and all-model-common populations are distinct.
Overlapping targets are not independent trials or unique business loss totals.
Current category/season labels and future operating indicators are descriptive,
not historical inputs or proof of an operating cause.

The engine's `recover` command can load existing checkpoints into a *new*
output directory; it cannot fit missing models or replace the original ledger.
Recovery is appropriate only for a confirmed output-generation problem, not
to improve results. Do not delete a ledger to force a rerun.

Tests use `python -B -m unittest discover -s tests -p 'test_bc60_monthly*.py'`
and `python -B -m unittest tests.test_bc60_native_v4_extension`.
No pytest installation is required. Warnings from the locked libraries are
recorded; they do not authorize dependency upgrades.

## Native monthly archive extension

`bc60_native_v4_extension.py` and its separately committed protocol reuse the
verified SKU-month archive for missing native V4 target months. This is a
deterministic, AST-isolated replay of the frozen implementation, not an
additional learning fit. Existing months are not rerun. Absent monthly truth
remains unknown; A16 missing calibration records remain N/A. These results
cannot be ranked with daily 60-day models or an unequal A16 sample.

The generic `bc60_monthly_overlap_audit.py` can review already acquired overlap
receipts without any database access. It refuses an existing output directory.
It is not permission to reacquire data, merge revisions into the original
segment, or replace the original historical audit receipt.

This branch is analysis-only: no production model, scheduler, purchase,
database, warehouse or BI resource change is authorized by these commands.
