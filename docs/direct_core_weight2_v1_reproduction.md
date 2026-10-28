# Fixed historical-core weight experiment: reproduction

This is a local, archived-data experiment. It has no database or API input and
does not issue a new prospective forecast. The protocol was committed before
effects were computed; this execution note is post-run documentation, not a new
preregistration. Results, model files, weight rows and business data are not in Git.

Use the archived Python 3.12 environment with LightGBM 4.6.0, MLForecast 1.0.2,
pandas 2.2.3, NumPy 2.2.6 and scikit-learn 1.6.1. Do not upgrade it. Run commands
from the independent research worktree. Keep the original archives at the paths
recorded in their manifests. A relocation requires an explicit provenance review,
not silent path substitution or replacement of source hashes.

The original DIRECT source is commit
`34bdc3b703c45326f3c638814ad18595c8124e83`; the historical-core helper comes from
`c738374ac28fd0c59bdf31bfa81ed5da716d3ac6`. The archived matrices are accepted only
after exact original-feature, label, key and dtype verification. Preserve the
Git-blob LF bytes of frozen source files on Windows when checking archive hashes.
Do not overwrite another worktree to fix line-ending differences.

The following commands describe the executed workflow. They are not an instruction
to repeat this closed experiment. A fresh reproduction needs an unused output
directory and its own fit budget; never delete a completed directory to rerun it.

```powershell
# Set these to the approved local archive root, existing interpreter and new path.
$ArchiveRoot = '<approved archive root>'
$Python = Join-Path $ArchiveRoot 'bc60_experiment_20261019/.venv/Scripts/python.exe'
$AnalysisOutput = Join-Path $ArchiveRoot '<new exclusive experiment directory>'
$PreflightReceipt = '<actual successful local test receipt path>'

& $Python -B tests/test_direct_core_weight2_v1.py
& $Python -B tests/test_core_weight2_auxiliary.py
& $Python -B tests/test_score_core_weight2.py
& $Python -B tests/test_core_weight2_independent.py

& $Python -B scripts/direct_core_weight2_v1.py prepare --root $ArchiveRoot --out $AnalysisOutput --tests $PreflightReceipt
& $Python -B scripts/core_weight2_auxiliary.py prepare --root $ArchiveRoot --out $AnalysisOutput
& $Python -B scripts/direct_core_weight2_v1.py fit --out $AnalysisOutput
& $Python -B scripts/score_core_weight2.py --out $AnalysisOutput
& $Python -B scripts/core_weight2_auxiliary.py evaluate --root $ArchiveRoot --out $AnalysisOutput
& $Python -B scripts/validate_core_weight2.py --root $ArchiveRoot --out $AnalysisOutput
& $Python -B scripts/report_core_weight2.py --out $AnalysisOutput
```

Check every command's exit status before continuing. The fit ledger reserves one
formal attempt per issue/segment and rejects refitting a failed attempted slot
without a valid saved checkpoint. A persisted model can be used to recover its
prediction output; this is not a new fit. Auxiliary evaluation uses saved original
and candidate models only, and counts its extra inference calls.

The scoring and validation commands read sealed forecasts; they do not train or
infer. Core identity uses each sample's own cutoff. Scoring is unweighted. The
independent validator recomputes scores and migrations from saved per-object truth
and predictions, checks original-feature and weight receipts, and verifies the
protected prospective files. Synthetic tests use estimator substitutes, never
additional real fits. Report generation does not choose a model or alter metrics.

Keep the preflight, prepare, effects, auxiliary, independent-validation and final
test receipts alongside the local result. Final delivery records the actual
executed code hashes and commits separately from later reporting-only changes.
An interface or code check passing is not evidence that the business gates passed.
