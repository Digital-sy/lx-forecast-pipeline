# BC60 V1 reproduction and scope

This independent branch contains sanitized source, tests and specifications only.
Never commit the input archives, scores, report snapshot, notebook outputs or credentials.
Do not invoke any production forecast entry point to reproduce this experiment.

## Environment and official interfaces

Python 3.12 in a NEW local virtual environment; install `docs/bc60_requirements_lock.txt`.
MLForecast 1.0.2 and LightGBM 4.6.0 were tested with a synthetic 62-step forecast and
explicit future covariates. No Optuna search is performed despite its transitive dependency.

- https://nixtlaverse.nixtla.io/mlforecast/docs/how-to-guides/cross_validation.html
- https://nixtlaverse.nixtla.io/mlforecast/docs/how-to-guides/exogenous_features.html
- https://lightgbm.readthedocs.io/en/v4.6.0/pythonapi/lightgbm.LGBMRegressor.html
- https://lightgbm.readthedocs.io/en/v4.6.0/Parameters.html

## Local archive contract

An approved archive root contains `evidence/ods_jan01_07.json`, subsequent three-day
`daily_raw/*.json.gz` and matching evidence receipts. Receipts bind native EXPLAIN,
the exact SELECT, query duration, returned row count, acquisition time and raw-row SHA256.
The standardized daily input and historical monthly reference are immutable Parquet.
The monthly reference is a previously authorized archive, not a fresh reconstruction of
historical point-in-time access. Missing source states, stock availability, listing history,
budget/activity and historical metadata validity must never be inferred from zero sales.

The readonly probe can be delivered as exact committed source to stdin over an existing
authorized SSH session. It does not save or deploy source remotely. It imports only the
existing database connection helper; credentials remain in that existing secure mechanism.
Use an explicit reviewed request with `metadata`, `plan` or `select`; keep query evidence
locally. Never print the database configuration or raw connection exceptions.

## Commands (replace placeholders with authorized LOCAL absolute paths)

```text
<isolated-python> -m unittest tests.test_bc60_probe tests.test_bc60_experiment -v
<isolated-python> scripts/bc60_experiment.py prepare --archive <archive-root> --monthly-archive <existing-monthly-jsonl> --out <new-input-directory>
<isolated-python> scripts/bc60_experiment.py run --inputs <frozen-input-directory> --config docs/bc60_design_lock_v1.json --out <NEW-effect-directory> --allow-exogenous
<isolated-python> scripts/bc60_validate_results.py <archive-root>
<isolated-python> scripts/bc60_notebook.py <archive-root>
```

The effect runner refuses an existing output directory. Freeze all input/config/source
hashes and every issue's core list before fitting. The validator uses the delivered V1
directory contract (`inputs_v1_final`, `effects_v1`) and does not retrain models. Its
output `validation_v1_checked` also refuses overwrite. Notebook execution reads only
existing scores and writes runtime files under the local archive, not the user's home.

## Interpretation boundaries

- All historical windows are reconstructed replay. No independent-OOS or historical
  acquired-at claim is permitted.
- The experimental established qualification is >=4 observed positive monthly records;
  it is not an assertion that production NEW_VISIBLE eligibility is identical.
- Fixed core membership is selected from observed full-shop past sales, including
  unmapped sales in the coverage denominator; ambiguous SPU strings are never split.
- Current category can be added for descriptive slicing, clearly marked non-PIT, never
  retroactively inserted into model inputs or core selection.
- Daily observed units (`volume`) are the primary truth. Do not add B2B, use order counts
  as units, or subtract refunds without a separate approved definition.
- The traffic variable is a sum of listing-level session events, not unique visitors.
  Order/session is a conversion proxy, not a claim of deduplicated visitor conversion.
- V4/A16 are preserved side references. Without a preregistered fair daily adapter,
  their monthly results cannot enter an exact 60-day ranking. Chronos was not executed.
- Recursive daily labels have fully elapsed by the cutoff. No incomplete 60-day label
  trains or calibrates the model. No future actual exogenous data enters prediction.
- Partial output coverage does not change the fixed core denominator. Secondary common
  sample WAPE does not establish the full-core business target.

After first-round reporting, stop. Do not choose a production model, extend parameter
searches, start an online shadow run, deploy, schedule, or write procurement/BI data.
