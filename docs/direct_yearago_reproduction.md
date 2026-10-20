# DIRECT_YEARAGO reproduction and review

Use the existing isolated environment recorded in the local archive. Do not
install or upgrade packages or run any production entrypoint. Keep all business
files outside the repository. The preregistration is commit
`78773ceaddff789aeb01afe13f3ad04e995297ff`; the initial executor/scorer/validator
freeze is `48b664ce36484c5440c43782443273a2fa5573e4`.

## Inputs

Required read-only archives:

- expanded monthly replay with its original DIRECT slots, daily input, core
  lists and frozen configuration;
- existing CONTEXT90 archive with verified full training matrices and original
  declining-group membership;
- reviewed cross-diagnostic current category/season metadata.

The executor verifies the original DIRECT training frame hashes after removing
the six CONTEXT90-only fields. Only the seven preregistered annual fields are
appended. Metadata is used solely after prediction for descriptive slices.

## Commands

Run from the repository root. Replace `PYTHON`, `ARCHIVE`, `CONTEXT`, `DIAGNOSTIC`,
`OUT` and `TEST_RECEIPT` with the already authorized local paths. OUT must be a
new exclusive directory. These placeholders contain no business data or secrets.

```text
PYTHON -B -m tests.test_direct_yearago --receipt TEST_RECEIPT
PYTHON -B scripts/direct_yearago_experiment.py prepare --archive ARCHIVE --context CONTEXT --out OUT --tests TEST_RECEIPT
PYTHON -B scripts/direct_yearago_validate.py prepared --out OUT --destination OUT/prevalidation
PYTHON -B scripts/direct_yearago_experiment.py fit --out OUT
PYTHON -B scripts/direct_yearago_score.py --out OUT --diagnostic DIAGNOSTIC --destination OUT/scoring
PYTHON -B scripts/direct_yearago_validate.py effects --out OUT --reviewed OUT/scoring --destination OUT/validation
PYTHON -B scripts/direct_yearago_report.py --out OUT --destination OUT/report
```

Do not call `fit` unless prevalidation is PASS and its prepare-manifest hash
matches. This intentionally separates support checks from any effect calculation.
There is at most one formal fit per supported unit. Skipped units never fit.
An existing checkpoint can recover predictions; a fit_started marker without a
checkpoint stops automatic retry. Source/config changes stop execution.

For a no-refit review of completed results, use only `effects` validation with
a **new** destination, or inspect existing validation records. Scoring/report
also require new output destinations. Do not overwrite previous reviewed output.
User authorization is required before another business experiment; these commands
are reproducibility documentation, not an instruction to rerun automatically.

## Review boundaries

- Every segment retains annual_model_prediction, comparison_prediction, origin,
  missing/fallback reason and exact original prediction file hash.
- The primary paired comparison includes disclosed original-DIRECT fallbacks.
  Both-annual-output results are a separate same-object subset.
- Annual operating comparability remains UNKNOWN. Observed zeros do not prove
  normal historical trading. The positive-observation flag includes the annual
  input start date, following the user's precise definition.
- Native missing handling is unchanged. LightGBM 4.6.0 defaults are documented
  at https://lightgbm.readthedocs.io/en/v4.6.0/Advanced-Topics.html#missing-value-handle
  and tested with synthetic NaNs; no model field is filled with an invented zero.
- Independent verification recomputes annual features from raw date reindexing,
  target truth from direct calendar selection, all grouped metrics, joint hits
  and the five unrounded research screens. It reloads models but does not refit.
- The synthetic test was first run as eight tests and then as a ten-test receipt
  including the two existing scoring tests. Each run has one small synthetic
  fit; neither is part of the business fit ledger.
- The final scorer uses round-trip CSV floating parsing, checked against every
  original DIRECT segment and cumulative prediction. This removes machine-scale
  fake improvement/decline classifications on unchanged fallback predictions.
  Initial scoring was retained; a separate reviewed scoring/validation directory
  is the final evidence. No model was refitted for this arithmetic repair.
- To report an alternate reviewed directory, pass `--reviewed OUT/scoring_reviewed`
  and `--validation-dir OUT/validation_reviewed` to the report command and use
  `--destination OUT/report_reviewed`. The report manifest identifies exact inputs.
- Formal results and statistical limitations belong in the controlled local
  report, never in GitHub source or test expectations. No business performance
  number is asserted by the synthetic test suite.

No database query, new source extraction, online shadow, model deployment,
parameter search, main merge, production update or BI change is performed.
