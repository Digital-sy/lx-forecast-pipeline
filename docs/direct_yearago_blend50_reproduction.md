# DIRECT_YEARAGO_BLEND50 reproducibility and isolation

This is one fixed arithmetic experiment on previously sealed local forecasts.
It does not fit or invoke any model, query a database, create a shadow batch,
change a production resource, or search a weight. Business archives are required
locally but are deliberately not distributed with this repository.

## Versions and inputs

- Preregistration: `d4bcdff72f5014913d0638868a32ca478ce4ff93`.
- Parent GATE implementation: `601e3d2c1f592165141d526eb9b6571dd557b0aa`.
- YEARAGO source reference: `83e0baa32f6de5cea6ff38e19c87f56fba67daba`.
- DIRECT replay reference: `34bdc3b703c45326f3c638814ad18595c8124e83`.
- Tested independent runtime: Python 3.12.14, pandas 2.2.3, numpy 2.2.6,
  pyarrow 19.0.1. No new package or production environment is required.
- The input GATE archive must contain the original routing, scoring, validation,
  final review and hash-addressed dependencies. The runner verifies pinned
  manifests and all inherited source hashes before generation and after scoring.

Use a checkout of this exact independent branch/commit, outside production.
Run the following commands from its repository root, substituting existing
local paths. The output path must not exist. These examples contain no credentials.

```powershell
$blendPython = 'D:\controlled\existing-experiment-env\Scripts\python.exe'
$blendSource = 'D:\controlled\yearago_gate_v1_20261020'
$blendOutput = 'D:\controlled\direct_yearago_blend50_new_unique_run'
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONDONTWRITEBYTECODE = '1'

& $blendPython -B -m unittest tests.test_direct_yearago_blend50 -v
if ($LASTEXITCODE -ne 0) { throw 'Arithmetic tests failed' }
& $blendPython -B scripts/direct_yearago_blend50.py prepare --gate-archive $blendSource --output $blendOutput
if ($LASTEXITCODE -ne 0) { throw 'Candidate generation failed' }
& $blendPython -B scripts/direct_yearago_blend50.py score --output $blendOutput
if ($LASTEXITCODE -ne 0) { throw 'Scoring failed' }
& $blendPython -B scripts/direct_yearago_blend50_validate.py --output $blendOutput
if ($LASTEXITCODE -ne 0) { throw 'Independent validation failed' }
& $blendPython -B scripts/direct_yearago_blend50_report.py --output $blendOutput
if ($LASTEXITCODE -ne 0) { throw 'Report generation failed' }
```

Do not run an old training or prediction validation entrypoint. A filename
containing `validate` is not by itself evidence that it avoids model inference.
The new validator imports only existing scalar scoring helpers and executes
the new arithmetic-only unit tests. Stage directories refuse overwriting.

## Evidence and verification chain

1. `prepare` parses only an explicit forecast-column projection. It creates
   the candidate and seals the input/parameter/code/output hashes before truth
   is read by `score`. Byte hashing a file does not parse its truth columns.
2. Original FALSE decisions copy DIRECT exactly. Original missing output stays
   missing. Missing or contradictory expected endpoints are errors, not fallback.
3. The primary scorer reuses immutable truth. The independent validator follows
   the previous daily-truth verification receipt and confirms every copied truth
   row, calendar window and original model forecast, then uses scalar arithmetic
   independently of the primary vectorized metric summaries.
4. All original four-group migration memberships are recomputed. The old fixed
   exit list retains both its keys and order; it is not re-selected for this result.
5. Convexity tolerances cover floating-point roundoff only. The ±20% hit boundary
   and research gates have no additional tolerance and use unrounded values.
6. Immature truth remains unknown and is excluded from mature scoring. Existing
   side-baseline predictions for immature issues are preserved, not deleted just
   because their labels have not matured.

The Chinese report is in `report/`; full and paired metrics, individual forecasts,
truth states, migrations, zero-actual absolute errors, decline-group and temporal
slices are in `scoring/`. The validator receipt, tests and per-window convexity
checks are in `validation/`. These are local business results, not Git inputs.

## Interpretation and stop rule

The scheme was proposed after GATE results were inspected. It is reconstructed
historical development validation, not independent OOS or prospective success.
Passing the intermediate screens does not establish the whole-core business
goal or authorize deployment. Convexity is an arithmetic property, not proof of
new predictive information. Complete this one fixed weight and stop.
