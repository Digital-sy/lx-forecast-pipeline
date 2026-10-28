# Reproducing the fixed traffic-state increment

This branch contains offline code, synthetic tests and specifications only.
Daily inputs, trained models, per-product predictions and effect reports remain
in the authorized local research archive. No database/API client is used here.
No production or prospective-shadow entry point is invoked.

## Runtime and inputs

Use the existing isolated BC60 runtime recorded in configuration.json. In this
run: Python 3.12.14, LightGBM 4.6.0, MLForecast 1.0.2, pandas 2.2.3, NumPy 2.2.6 and
scikit-learn 1.6.1. Do not silently install or upgrade dependencies.

Required existing sibling archives:

- bc60_monthly_replay_20261020 (frozen training checkpoints and daily inputs)
- bc60_experiment_20261019 (original acquisition and quality evidence)
- core_model_complementarity_20261028_v1 (historical outputs and protected hashes)
- All local sources referenced by those immutable manifests, including the
  fixed BLEND50 and decline-observation-group evidence.

The prospective batch is only hashed using its protection list. Its forecasts,
inputs and future actuals are not parsed as experiment data.

Keep source bytes identical to the frozen manifest. A Windows checkout can change
LF to CRLF; verify Git blob bytes before any research run. Do not edit old archives
or waive a hash mismatch. Normalization of a fresh isolated source checkout must
be recorded and must not change code semantics or the production working tree.

## Commands

Run from this isolated repository. Set ROOT to the authorized archive root, OUT
to a **new, nonexistent** child directory, PY to the existing isolated Python,
and TEST_RECEIPT to a new local JSON result. These are local paths, not secrets.

1. Run `PY -B -m unittest tests.test_direct_traffic_state_v1 -v` and record its
   actual exit code, stdout/stderr, UTC time and test/code SHA256 in TEST_RECEIPT.
   Set `status` to `PASS` only after a successful real run. Existing unchanged
   test evidence may be reused after comparing code and test hashes.
2. `PY -B scripts/direct_traffic_state_v1.py prepare --root ROOT --out OUT --tests TEST_RECEIPT`
3. Inspect the sealed training reconciliation and pre-fit support ledger.
4. `PY -B scripts/direct_traffic_state_v1.py fit --out OUT`
5. `PY -B scripts/direct_traffic_state_score.py --out OUT`
6. `PY -B scripts/direct_traffic_state_validate.py --out OUT`
7. Only after validation PASS:
   `PY -B scripts/direct_traffic_state_report.py --out OUT`

No extra fit is needed to render, score or validate a completed run. The fit
command reserves each slot before starting, loads completed checkpoints on
recovery and rejects refitting a started slot without a checkpoint. It never
resets the 40-fit budget. A skipped slot is explicitly marked as referenced
DIRECT, never a newly trained output. Do not delete ledgers to retry fitting.

## Validation scope

The synthetic suite covers exact calendars, each row's own cutoff, future
perturbation, missing days, mapping validity, component NULLs, negative/nonfinite
values, zero Sessions and denominator semantics, and preservation of old columns.

Independent validation checks all old training-column hashes and labels, all
inference feature calendars plus a fixed systematic training sample, saved-model
prediction equality, unchanged truth/calendar alignment, scalar joint hits and
metrics, original fixed groups, gate results and all registered source hashes.
Saved-model reads are validation inference, not additional model fits or a new
prospective batch. The validator does not call the scorer's aggregation helpers.

Actual truth is reused through the previously independently checked daily-to-
window truth chain in the complementarity archive. It is not re-queried or revised.
Zero-actual records retain absolute error; missing predictions retain their status.
The paired scope and full-core scope remain separate, especially for F28.

The final local receipt records exact commits, code hashes, fit count, validation
results and artifact hashes. Research selection may FAIL despite technical PASS.
Do not tune, deploy, or open another experiment based on this run automatically.
