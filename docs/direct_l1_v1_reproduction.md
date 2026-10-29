# DIRECT_L1_V1 local reproduction

This note documents the closed fixed-objective experiment. It does not authorize
another candidate or another run. Existing result directories reject overwrites.
Use only the recorded local archives and pinned interpreter. No database, API,
production client, new shadow batch or source refresh is needed.

Original model configuration is read from the monthly replay's frozen_config.json,
not from the weighting experiment. Only objective and metric change to
regression_l1 and l1. The original configuration did not explicitly set
poisson_max_delta_step; its library default may remain in model metadata but is
inactive for L1. Native L1 median initialization and residual-median leaf updates
are used without external initialization. Raw original-scale labels are passed
unchanged and fit receives no weight array or evaluation set.

Version evidence:
- LightGBM4.6.0,MLForecast1.0.2,pandas2.2.3,NumPy2.2.6,scikit-learn1.6.1.
- Original DIRECT source:34bdc3b703c45326f3c638814ad18595c8124e83.
- Preregistration local commit:f44166be56416bb4a910ffac2b73600f58ba7f4f.
- Training implementation local commit:5de19f1ff80c177db8a4bc9a2bfc9d1b5fe6bb6f.
- The final publication commit and exact executed hashes are in the local delivery receipt.
- https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html#objective
- https://github.com/microsoft/LightGBM/blob/v4.6.0/src/objective/regression_objective.hpp

Run from the independent research worktree. On Windows preserve original Git-blob
LF bytes when comparing frozen archive hashes. Do not rewrite another worktree
or change hashes to work around provenance checks. For an explicitly authorized
reproduction, choose an unused output path and retain the original manifests.

```powershell
$ArchiveRoot = '<approved local archive root>'
$Python = Join-Path $ArchiveRoot 'bc60_experiment_20261019/.venv/Scripts/python.exe'
$AnalysisOutput = Join-Path $ArchiveRoot '<new exclusive experiment directory>'
$PreflightReceipt = '<actual successful test receipt>'

& $Python -B tests/test_direct_l1_v1.py
& $Python -B tests/test_direct_l1_auxiliary.py
& $Python -B tests/test_score_direct_l1.py
& $Python -B tests/test_direct_l1_independent.py

& $Python -B scripts/direct_l1_v1.py prepare --root $ArchiveRoot --out $AnalysisOutput --tests $PreflightReceipt
& $Python -B scripts/direct_l1_auxiliary.py prepare --root $ArchiveRoot --out $AnalysisOutput
& $Python -B scripts/direct_l1_v1.py fit --out $AnalysisOutput
& $Python -B scripts/score_direct_l1.py --out $AnalysisOutput
& $Python -B scripts/direct_l1_auxiliary.py evaluate --root $ArchiveRoot --out $AnalysisOutput
& $Python -B scripts/validate_direct_l1.py --root $ArchiveRoot --out $AnalysisOutput
& $Python -B scripts/report_direct_l1.py --out $AnalysisOutput
```

Check each exit status. The fixed fit ledger allows only one attempt per unit;
checkpoint recovery never refits a failed slot. Each successful original fit
records one native prediction and one saved-model readback prediction. Auxiliary
evaluation records its own saved-model calls and reuses DIRECT's archived outputs.
Report actual counts, including failed attempts, not a prefilled expected total.

Main evaluation and independent verification use the same fixed historical truths,
but separate arithmetic for metrics and migrations. No new training weights enter
business scoring. Auxiliary membership and truths are immutable references to the
previously sealed pool; they are not reselected after seeing candidate effects.
All negative raw outputs are retained and each segment is clipped at zero before
summing the60-day forecast. Missing outputs are never substituted with zero.

Keep the local configuration, fit receipts, model metadata, prediction seal,
scoring, auxiliary-risk and independent-validation receipts. Repeated historical
development is not independent OOS. Numeric outputs, models, business records and
credentials are excluded from the Git publication.
