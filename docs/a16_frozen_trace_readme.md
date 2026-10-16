# Frozen A16 calculation trace (offline diagnostic only)

This tool reads an existing shadow result and a separately obtained input snapshot.
It does not query a database, invoke a production entrypoint, select model parameters,
start a new shadow batch, or alter the frozen model. Keep all business artifacts
outside Git. No data or credentials are supplied by this branch.

## Inputs and safety

- `--input-json`: UTF-8 object with `kind=RECONSTRUCTED_INPUT_DIAGNOSTIC`, `monthly`,
  `catalog`, and `season`; row fields are those consumed by the existing runner's
  `prepare` function. Preserve the full original shop history and metadata, not just
  the objects being investigated. Obtain records separately through approved readonly
  queries after native EXPLAIN, timeouts and row caps.
- `--original-dir`: unchanged `a16_v4_shadow_rows.csv`, `shadow_run_meta.json`, and
  `shadow_acceptance_summary.json`. CSV SHA256 is checked against the retained receipt.
- `--spus`: final output selection only; full shop/category support is calculated first.
- `--output-dir`: NEW folder outside the original run. Existing folders are refused.

The source manifest is verified, original gammas are read from original metadata,
and parameter selection / full prediction calls are disabled in memory. Calibration
population is counted using the original eligibility predicate; no historical forecasts
or parameter grids are rerun. A3 and seasonal helper locals are recorded at function
return without modifying the helper source. A7 arithmetic is checked independently.
No forecast baseline values are embedded as computation outputs.

```text
python scripts/diagnose_a16_frozen_trace.py --input-json /local/audit/input.json --original-dir /local/audit/original --spus PRODUCT_A,PRODUCT_B --output-dir /local/audit/trace_new
python -m unittest discover -s tests -p test_a16_frozen_trace.py -v
```

Outputs: `trace.json`, `trace.csv` (nested evidence in JSON-valued columns), `audit.json`.
They contain business data and must NOT be committed or uploaded to GitHub.

## Interpretation

Canonical input hash equality establishes equality of the normalized model input,
not preservation of the original raw archive nor historical availability/operating state.
The collection remains labelled reconstructed. Numerical equality is not forecast
accuracy. Explicit zeros, missing observations and model-internal zero defaults are
distinct. Historical metadata and stockout/promotion evidence require independent proof.
V4 is a separate path; do not treat V4-to-A16 differences as A3-to-A7 contribution.

This diagnostic targets selected nonzero eligible seasonal forecasts. It fails closed
when required evidence is absent; do not use it as a general production predictor.
