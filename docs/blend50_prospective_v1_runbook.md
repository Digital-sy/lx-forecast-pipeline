# Isolated BLEND50 prospective preflight

This entrypoint implements the prerequisite historical inference chain and
bounded source acquisition/audit. A source failure stops current fitting and
issuance. There is no production deployment, scheduler, model-selection search,
automatic procurement or BI write path.

## Verified interfaces

Use the existing isolated research interpreter (not the production virtualenv).
Dependencies must match `bc60_monthly_replay_20261020/prepared/frozen_config.json`.
This protocol uses LightGBM 4.6.0's documented `Booster(model_str=...)`,
`feature_name()` and `predict(validate_features=True)`. The six frozen method
files are compared with their exact Git commits and normalized hashes before
historical inference. No fit method is invoked by the reproduction command.

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTHONIOENCODING='utf-8'
& $researchPython -B -m unittest tests.test_blend50_prospective -v
& $researchPython -B scripts/blend50_prospective.py reproduce --workspace $workspace --output $newReproductionDirectory
```

The output directory must not already exist. The original inference period is
2026-08-02. This is an old-period replay, **not a new prospective issuance**.
Only inputs through the cutoff enter feature construction. Generated predictions
are sealed before old prediction columns are opened. Target actuals are not
loaded. A failure is retained and blocks current formal fitting; do not refit
historical models or relax the predeclared tolerance to pass.

## Live-source boundary

Only after reproduction PASS, `blend50_source_acquire.py` streams a restricted
read-only probe over the existing SSH connection. It is never installed on the
server. Connection credentials remain in existing server configuration; neither
configuration files nor credentials are output. No password prompts/retries.
The frozen maximums are 30 business SELECTs, 10 seconds per statement, 1 million
estimated scan per query and 10 million cumulative. Metadata and native EXPLAIN
are separately counted. Each request is reserved before transport; failures are
retained. Execute serially, never launch concurrent acquisition processes.

```powershell
& $researchPython -B scripts/blend50_source_acquire.py --root $newBatch --name metadata
& $researchPython -B scripts/blend50_source_acquire.py --root $newBatch --name $uniqueName --sql-file $reviewedBoundedSelect
& $researchPython -B scripts/blend50_source_audit.py --root $newBatch --archive $oldReplay --issue $actualShanghaiDate --out $newAuditDirectory
```

The audit preserves explicit zeros and absent records separately, checks seven
overlap days and records source business dates versus actual acquisition times.
`MAX(modify_time)` is not sync-success evidence. Day counts are not sufficient
when some objects are missing. The combined ledger is diagnostic only and cannot
be consumed as an approved current-model input. No core is frozen on incomplete
data. Unmapped source quantities remain in diagnostics and are not allocated.

If source completeness fails, no further model fitting or prediction sealing is
attempted. Source repair or backfill is not authorized by these commands. A new
attempt must use the then-current Shanghai issue day and a new exclusive directory.

## Future scoring boundary

`blend50_maturity_gate.py` is a preflight, **not a complete issued-batch scorer**.
It refuses non-issued batches, checks real clock/timezones, and requires explicit
source reconciliation evidence after S+32/S+62. Passing a date alone is not truth
maturity. It never changes predictions or computes an invented accuracy.

The numerical reference for the later adapter is the already independently
validated daily-window scorer `scripts/bc60_monthly_validate.py`; archived result
reuse is not permitted as future truth. A real issued manifest, preserved core,
decline membership, immutable unrounded predictions and a separately hashed
future daily-truth version are required. An adapter for an actual prospective
issuance remains unvalidated until a source-complete issuance exists.

Operating-change templates remain local. They are explanatory evidence only,
never new input features or a requirement to invent missing operating history.
Post-event notes must not masquerade as pre-issue known plans.

## Repository boundary

Commit only these sanitized source/tests/specification files on an independent
`codex/` branch. Never commit inputs, query results, model checkpoints, per-product
forecasts, notebooks with business output, credentials or server backups. Do not
merge main, deploy, touch the server worktree, or start a scheduler.
