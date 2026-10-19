# BC60 V2 finite offline reproduction

This branch contains code, synthetic tests and specifications only. Business
archives, trained estimators, predictions, scores and local report applications
must stay outside the Git repository. This is reconstructed historical replay,
not independent OOS, a production model change or an online shadow issue.

## Immutable references

- V1 effect source: `58674f0a8ebe14f176bfd0a6baaccb5cfc1c71e7`.
- V1 delivery: `d29431dfd06db22588e8637b0673c37166f8ad09`.
- V2 protocol committed before effects: `7e3faa5d93a5a16751a47170d6043b90fb244bba`.
- Original daily SHA256 and frozen core SHA256 are enforced in the versioned lock.
- Only `inputs_v1_final` is the effect input; intermediate directories are not substitutes.
- Preserve all fixed core issue-object rows, including original B1 qualification gaps.

## Runtime and controlled commands

Use the existing isolated V1 environment: Python 3.12.14, MLForecast 1.0.2,
LightGBM 4.6.0, pandas 2.2.3, numpy 2.2.6, scikit-learn 1.6.1.
The native recursive-state audit additionally binds coreforecast 0.0.18.
Do not install into production or silently upgrade libraries.

In the commands below, `<python>` is that isolated interpreter, `<archive>` is
the existing V1 archive, and `<new-root>` is a different newly created local
output root. All result destinations refuse overwrite. These commands are
documentation, not authorization to run a second experiment.

```text
<python> -B -m unittest tests.test_bc60_v2_rolling tests.test_bc60_v2_b1 tests.test_bc60_v2_validation

<python> -B scripts/bc60_v2_rolling.py --archive <archive> --config docs/bc60_v2_design_lock.json --out <new-root>/b_rolling
```

For Task A, prefer the already saved V2 local checkpoints. Recovery must never
fit another model. Supply the directory containing one checkpoint per original
issue, not a directory of result CSVs:

```text
<python> -B scripts/bc60_v2_b1_diagnostic.py --archive <archive> --out <new-root>/a_diagnostic_final --models-from <existing-checkpoint-root>/models
```

Only if no fitted models have ever been saved and a new execution is separately
authorized may the initial `--models-from`-free path be used; it is limited to
one unchanged fit per original issue, three fits total. Failed effect checks do
not authorize refitting. Original D0 predictions must reproduce before D1 runs.

```text
<python> -B scripts/bc60_v2_validate.py --archive <archive> --root <new-root> --skip-diagnostic
<python> -B scripts/bc60_v2_validate.py --archive <archive> --root <new-root> --only-diagnostic --diagnostic <new-root>/a_diagnostic_final --output <new-root>/validation_b1
<python> -B scripts/bc60_v2_prepare_report.py --archive <archive> --root <new-root> --diagnostic <new-root>/a_diagnostic_final --validation <new-root>/validation
```

Inspect the generated validation manifests before producing final narratives.
Notebook generation help is available through `scripts/bc60_v2_notebook.py --help`.
It reads completed results and executes review cells, not model training.

## Interpretation and reproducibility limits

T0 preserves the first issue. T1 scores only still-future dates before the fixed
original end. T2 issues new complete windows and retains NOT_MATURED truth.
Weekly refresh dates coincide with daily refresh dates every seven days; equal
results on those dates are expected. The fixed first-two-issue T2 panel is a
separately declared maturity comparison, not a replacement for the full core.

All overlapping update vintages are correlated. Do not sum their errors into
actual procurement loss, manufacture an independent sample size, or replace the
original issued forecast with later observations. Missing/zero/immature truth and
missing output are distinct. Report hit coverage separately from WAPE and Bias.

## Audit-only recovery notes

Two interface checks are important for MLForecast 1.0.2:

1. Integer training targets are represented as native float32 recursive state.
   The independent feature oracle must reproduce sequential native rolling
   accumulation rather than compare a float64 mean with float32 features.
   This affects the checker, not the original model, proxy or predicted values.
2. Re-saving a publicly loaded no-interval model may reference an unset
   `_cs_df` in this pinned version. Recovery preserves/copies the existing
   hash-verified pre-prediction checkpoint; it does not patch private state.

Keep original failed attempts and precise error evidence in the local archive.
Never relabel a recovery as a successful original attempt. D0 output tolerance,
qualification, training context and parameter settings remain unchanged.
