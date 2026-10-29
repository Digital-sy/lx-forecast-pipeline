# Fixed archived-forecast mean: local reproduction

This closed research run uses no learned model loading, fitting, prediction API,
source-system query, production access or new prospective issue. The operation is
arithmetic on sealed unrounded nonnegative historical predictions. This is not an
authorization for repeated candidate searches or changes to frozen archives.

The protocol was committed locally before candidate generation. The two endpoint
results had already been observed, so the experiment is historical development.
The candidate is sealed before actual-sales columns are loaded by scoring.

Use the existing pinned research interpreter and independent worktree. Pick an
unused output directory; all generation and scoring stages reject overwrites.
Do not copy business inputs, results or models into a Git repository.

```powershell
$ArchiveRoot = '<approved local archive root>'
$Python = Join-Path $ArchiveRoot 'bc60_experiment_20261019/.venv/Scripts/python.exe'
$Output = Join-Path $ArchiveRoot '<new exclusive output directory>'
$ProtocolCommit = '2233997b2c8927eaad0607f52e217cd411d00c18'

& $Python -B tests/test_blend50_l1_mean.py
& $Python -B tests/test_score_blend50_l1_mean.py
& $Python -B tests/test_blend50_l1_mean_independent.py
& $Python -B scripts/blend50_l1_mean.py --root $ArchiveRoot --out $Output --preregistration-commit $ProtocolCommit
& $Python -B scripts/score_blend50_l1_mean.py --out $Output
& $Python -B scripts/validate_blend50_l1_mean.py --root $ArchiveRoot --out $Output
& $Python -B scripts/report_blend50_l1_mean.py --out $Output
```

Check every exit status before the next step. Read actual manifests, not report
rounded numbers. The original monthly core file also contains two not-matured
periods; the generator applies the predeclared twenty mature periods, separately
records excluded periods, and retains missing model outputs inside mature scope.
This is a date-boundary choice, not outcome-based selection.

For M1 and M2 the exact expression is `0.5 * B + 0.5 * L`. The 60-day result is
the sum of those two new segments. No extra rounding, floor, cap or fallback is
introduced. Both endpoint masks and statuses must agree. Fixed common-hit groups
and algebraic identities are checked independently, while research gates use
unrounded values without tolerance. Small floating tolerance is reserved for
algebraic verification; it never relaxes the business hit threshold.

Original endpoints, source archive hashes and all protected first-shadow files
are verified before and after the operation. No new target actuals are fetched.
Supplemental noncore risk remains unevaluated when matching sealed BLEND50 outputs
do not exist; no DIRECT substitution or extra model inference is allowed.

Keep candidate_manifest.json, prediction_access_log.json, scoring/manifest.json,
validation/validation.json, test_receipt.json and delivery_receipt.json locally.
The final delivery receipt records the actual local and remote commits, code,
sources, output hashes, counts and execution times. Only sanitized source, tests
and protocol files are published to the independent GitHub branch.
