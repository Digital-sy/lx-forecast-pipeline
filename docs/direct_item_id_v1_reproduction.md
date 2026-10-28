# Reproduce DIRECT_ITEM_ID_V1

Local offline research only. This branch adds one categorical identity feature to
the original frozen DIRECT, not to TRAFFIC_STATE. Never invoke source acquisition,
production forecasting, prospective issuance or a database client.

## Required immutable sources

Use the original monthly replay, fixed BLEND50, complementarity and completed
TRAFFIC_STATE archives named in the protocol and their referenced manifests.
TRAFFIC_STATE supplies comparison outputs and reusable validation patterns only;
its prepared training frames and traffic features are not training inputs here.
The daily sales input and original training generator are taken from the frozen
monthly replay. Regenerated old training frames must match its exact hashes.

Runtime used: Python 3.12.14, LightGBM 4.6.0, MLForecast 1.0.2, pandas 2.2.3,
NumPy 2.2.6 and scikit-learn 1.6.1. Do not upgrade or edit the production venv.
Frozen source files must have the recorded byte hashes; Windows CRLF conversion
must not be mistaken for a model change or waived silently. Restore exact verified
Git blob bytes only in a new isolated research worktree, recording that operation.

## Commands

Run from the isolated repository with the existing isolated interpreter PY.
ROOT is the authorized archive root. OUT must be a nonexistent child of ROOT.
TEST_RECEIPT is a new local JSON recording an actual successful test run, timestamps,
stdout/stderr, and `files_sha256` for the new implementation and synthetic test.

```text
PY -B -m unittest tests.test_direct_item_id_v1 -v
PY -B scripts/direct_item_id_v1.py prepare --root ROOT --out OUT --tests TEST_RECEIPT
PY -B scripts/direct_item_id_v1.py fit --out OUT
PY -B scripts/direct_item_id_score.py --out OUT
PY -B scripts/direct_item_id_validate.py --out OUT
PY -B scripts/direct_item_id_report.py --out OUT
```

Prepare seals all 40 training units, dictionaries, support records, configuration,
and input hashes before the first effect fit. It checks the original ten columns,
labels, keys, full-frame hash, eligibility and runtime against frozen evidence.
It constructs dictionaries from the actual training tuples for each unit only.
No inference universe, truth-derived encoding, missing-output deletion or routing.

The worker projects exactly the old ten columns plus int32 `item_key` and explicitly
declares only `item_key` categorical. Constructor parameters are unchanged. Native
category defaults are read back from each saved model, not assumed from a report.
Unknown tuples are -1; zero remains a real known category.

Each slot can fit once. Reserve before starting, preserve the fit ledger and load
saved model checkpoints after technical interruption; do not remove the ledger
to repeat fitting. Maximum 40 formal historical fits, no baseline refits. The
synthetic suite does not fit a model. Scoring/validation/reporting never fit models.
Validation inference from saved models is not a new prospective forecast batch.

## Interpretation and independent checks

The validator uses separate scalar metrics and joint-hit logic; it checks frozen
truth/calendar equality, all dictionaries and identity encodings, original column
hashes, label maturity, support interval counts, categorical split metadata,
saved-model prediction equality and negative-code/NaN equivalence. It independently
recomputes the fixed gates and joint migrations, retaining full/paired scopes.
The daily-to-window truth chain was independently verified in prior archives;
this run reuses that fixed truth version and does not re-query source data.

All source and first-shadow protection hashes are checked again. The first-shadow
files are only hashed, not parsed for future actuals or re-predicted. Business rows,
models, dictionaries and effect reports stay local. Only sanitized code, synthetic
tests and specifications enter GitHub. Use the final execution receipt for exact
commits, byte hashes and real timestamps. No deployment or automatic next candidate.
