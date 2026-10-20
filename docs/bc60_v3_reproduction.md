# BC60 V3 reproducibility and stop boundary

The V3 protocol/design lock were committed before effects. The original V1/V2
archives remain read-only. This branch contains only sanitized code/tests/specs;
the six fitted models, input tables, predictions, scores and reports stay in the
authorized local V3 archive. This is reconstructed historical replay, not OOS.

## Existing locked runtime

Run from this repository with the original isolated environment:

```powershell
$bcPython = '../bc60_experiment_20261019/.venv/Scripts/python.exe'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONIOENCODING = 'utf-8'
& $bcPython -B -m unittest tests.test_bc60_v3_direct tests.test_bc60_v3_validation
```

No installs, upgrades, production imports or credentials are required. Runtime
versions and original configuration must match the verified V1 lock. Unit tests
use synthetic arrays and do not fit additional estimators.

## Verify existing artifacts, with zero new fits

The output directory must not exist:

```powershell
& $bcPython -B scripts/bc60_v3_validate.py `
  --archive ../bc60_experiment_20261019 `
  --v2-root ../bc60_rolling_v2_20261019 `
  --root ../bc60_direct_v3_20261019 `
  --output ../bc60_direct_v3_20261019/verification_reproduced `
  --prepared-evidence ../bc60_direct_v3_20261019/validation_prepared/validation.json
```

Omit `--prepared-evidence` to independently rebuild the complete training oracle;
that still performs no fits. Evidence reuse requires exact-byte prepared hashes.
The verifier independently sums original daily truth and reloads saved boosters
for prediction. It does not import the training engine. Model loading uses the
public API and an ASCII relative path because the installed LightGBM Windows
binary does not accept the Chinese-containing absolute archive path.

## Rebuild the review notebook, without retraining

```powershell
& $bcPython -B scripts/bc60_v3_notebook.py `
  --prepared ../bc60_direct_v3_20261019/prepared `
  --effects ../bc60_direct_v3_20261019/effects `
  --validation ../bc60_direct_v3_20261019/validation_final/validation.json `
  --prepared-validation ../bc60_direct_v3_20261019/validation_prepared/validation.json `
  --out ../bc60_direct_v3_20261019/notebook_reproduced
```

## Original effect execution sequence (already completed)

These commands document the completed experiment, not authorization to repeat
the six fits. A repeated independent experiment requires a new approved budget.

```powershell
& $bcPython -B scripts/bc60_v3_direct.py prepare `
  --v1 ../bc60_experiment_20261019 `
  --v2 ../bc60_rolling_v2_20261019 `
  --config docs/bc60_v3_design_lock.json `
  --out ../bc60_direct_v3_20261019/prepared

# Run only after synthetic and independent prepared gates have passed.
& $bcPython -B scripts/bc60_v3_direct.py run `
  --prepared ../bc60_direct_v3_20261019/prepared `
  --out ../bc60_direct_v3_20261019/effects
```

Existing destinations are refused. `formal_fit_ledger.json` is exclusively
reserved before the first fit and prevents a second formal run under the same
archive root even with another effects directory. Never delete or reset it.

If a technical interruption occurred after checkpoints were written, use only
the checkpoint-only route into a new directory:

```powershell
& $bcPython -B scripts/bc60_v3_direct.py recover `
  --prepared ../bc60_direct_v3_20261019/prepared `
  --models-from ../bc60_direct_v3_20261019/effects `
  --out ../bc60_direct_v3_20261019/recovered_effects
```

No missing checkpoint is refitted; failed/unknown states remain. This recovery
was not needed for the completed effect run. It must not overwrite the original.

## Evidence and failure records

- `prepared/prepare_manifest.json`: source/config/script and data-file hashes.
- `prepared/coverage.csv`, `exclusions.csv`, `per_object_windows.csv`: actual
  temporal support and full-context qualification/label exclusions.
- `effects/fits.json`, `run_manifest.json`, `models/`: six attempts, parameters,
  model hashes, raw/clipped output and elapsed time.
- `effects/scores.csv`, `summary.csv`, `joint_hits.csv`, `joint_migrations.csv`,
  `paired_details.csv`, `paired_summary.csv`: fixed-grid and same-sample scoring.
- `validation_prepared/validation.json`, `validation_final/validation.json`:
  authoritative independent checks; prior failed verifier directories retained.
- `pre_fit_failure.json`: preflight UTF-8 path failure before any model fit.
- `test_execution.json`: actual final synthetic test execution.

The original preparation source is retained locally as `prepare_source.py` to
bind its exact hash; later source hardening only added scoring validity checks,
fit reservation/recovery and explicit UTF-8 JSON reading. It did not change
prepared features/labels, estimator parameters or historical sample selection.

Stop after the finite result/report. No production deployment, main merge,
database query, shadow execution, scheduler change or model selection follows.
