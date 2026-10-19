# B1 source audit and callback contract

This is a sanitized implementation audit. Business inputs, object identifiers,
model checkpoints and experiment outcomes belong in the separate local V2 archive.
The original archive and implementation are read-only. No database query, network
business access or estimator training was performed for the interface audit.

## Evidence and version boundary

- Original source: `scripts/bc60_experiment.py`, SHA256
  `5e433c029ebd8fa2c60e19a389a9d2ce5cd3c344238645160cec69f7c0e4f745`.
- Original lock: `docs/bc60_design_lock_v1.json`, SHA256
  `3527e51cf6336e25706fe9cf3ea7f4c0a486341e4f6011f9e2760bd4409ebf28`.
- Both hashes match the original `effects_v1/run_manifest.json`.
- Installed MLForecast is exactly 1.0.2. Its local source and signatures, rather
  than an assumption about current documentation, determine execution behavior.
- [Nixtla's public callback guide](https://nixtlaverse.nixtla.io/mlforecast/docs/how-to-guides/predict_callbacks.html)
  documents before-prediction feature inspection and after-prediction adjustment
  before recursive target updates. The experiment uses those public callbacks.
- The authoritative V2 protocol preserves 385 core issue-object rows: 369 original
  B1 outputs and 16 missing B1 qualifications. The 369 subset is secondary.

## Original B1 audit

| Concern | Source evidence and finding |
| --- | --- |
| Target and objective | Original lines 198–203 pass the original daily `y` to LightGBM with locked `objective="poisson"`. No `log1p`, normalization, differencing or target transform is supplied. MLForecast's constructor defaults `target_transforms=None`; installed `core.py:283–298` only transforms when explicitly configured. Poisson's internal response link is not a preprocessing transformation of the stored target. |
| Weights | Original fit supplies no `weight_col`. Installed `forecast.py:285–294` passes `sample_weight` only when a weight column exists. Thus there is no explicit row, object, shop or recency weighting; longer eligible series contribute more training rows. |
| Identifier isolation | Original lines 129 and 198–203 create a shop–SPU–contiguous-run identifier, then explicitly set only `shop_code` as a static feature. Installed `core.py:299–330` excludes `unique_id`, `ds`, and `y` from model feature columns in this case. The identifier controls lag grouping, not a learned categorical predictor. Shop code is a numeric model feature. |
| Calendar and lag rules | Locked lags are 1, 7, 14, 28; rolling means are widths 7 and 28 on lag 1; calendar fields are day-of-week, month, day-of-year. At day t, the rolling-28 input covers t−28 through t−1. Synthetic preprocessing independently matches shift-then-roll calculations. |
| Unknown dates | Original lines 119–140 select only mapped OBSERVED/EXPLICIT_ZERO targets and split at every nonconsecutive day. Segments shorter than 56 days are excluded. A prediction terminal must end at the historical cutoff. No gap is imputed or bridged by a lag. MLForecast drops rows with incomplete generated lag features. |
| Label cutoff | Original lines 179–185 select history through issue−2 before segmentation and fit. One-step labels, including the latest cutoff day, are passed directly. `max_horizon=None` is the recursive one-label default. No 60-day target is used for fitting or tuning. Original `fits.json` records the corresponding cutoff as latest label for each issue. |
| Prediction chronology | Original lines 186 and 215 generate 62 daily steps from cutoff+1 through issue+60. Relative days −1 and 0 are bridge steps; the scored original window starts at issue+1. Both bridges update the subsequent lag state. |
| Clipping | Original line 215 returns `maximum(prediction,0)` through the after callback. In version 1.0.2 that clipped value is both recorded and fed to later steps; the original path does not merely clip final exported numbers. Floating values are retained. |

The audited context is all eligible established segments, not only fixed core
objects. Unqualified core objects retain missing B1 outputs. These findings do not
prove point-in-time historical source availability; the run remains reconstructed
historical replay.

## D1 public-interface implementation

Installed `mlforecast/core.py:666–675` executes, in order: compute next features;
before callback; estimator predict; after callback; append callback result to
state; build the returned forecasts. At `core.py:549–558`, `_update_y` appends
the same array to both the stored predictions and recursive state.

Consequently, returning the known cutoff mean from the after callback also makes
the ordinary `predict()` result contain that mean. That result must not be scored
as the D1 model output. The supported approach is:

1. Use public `make_future_dataframe(62)` to obtain model series/time order,
   then filter by terminal identifier membership without reordering it.
2. Capture each before-callback feature dataframe, validate shop and lag values
   against an independent history buffer, and return the dataframe unchanged.
3. In the after callback, copy raw model predictions, apply the original
   nonnegative clipping, and retain the clipped copy as the scored output.
4. Return each series' known 28-day mean ending at the original cutoff as D1
   feedback, including both bridge steps. The observed initial history remains
   untouched. This proxy is constant throughout the 62-step call.
5. Validate returned keys/order against the public future template. Construct
   scored output from the captured step-by-series arrays in that verified order.
   Preserve returned feedback separately in the trace.

The D1 predictions use the same fitted B1 estimator under changed future feedback.
They equal D0 at the first recursive step; later outputs need not equal the
archived original path. This intervention cannot, by itself, establish all causes
of error or any operating cause.

The 1.0.2 `forecast.py:663–670` docstring says callback predictions are an indexed
Series, but the actual LightGBM path yields an ndarray, and input features carry a
RangeIndex with no identifier. Never align by that index, by the caller's `ids`
list order, or by shop code alone. The `ids` filter preserves model series order
(`core.py:767–773`); public future keys use that same order (`forecast.py:604–625`).

Private `_update_y`, `_predict_recursive`, `_get_features_for_next_step`, grouped
arrays and saved lag internals were inspected as evidence; the actual experiment
does not call, subclass, patch or assign them. Some examples in the current guide
use private methods as explanatory checks; that is not the implementation used
here. Synthetic tests inject a fixed predictor solely to exercise callbacks with
zero fits; actual runs use the original standard fit followed by public predict.

## Artifact reuse and recovery

The original archive contains only one `effects*` directory, `effects_v1`, whose
file inventory and manifest contain forecasts, scores and evidence, but no saved
estimator. The original source also contains no save call. Thus there is no model
artifact to reuse within the audited original effects scope; no broader filesystem
or credential search was performed.

V2 permits one unchanged B1 rebuild per original issue, at most three total. The
new script saves each model with public `MLForecast.save` before either prediction
path, including hashes and a fingerprint. `--models-from` loads verified local
checkpoints with public `MLForecast.load`, produces a new output directory, and
does not refit missing or invalid checkpoints. D0 must match every archived core
B1 key and value within maximum absolute difference 1e−8 before that issue's D1
can run. Failure affects that issue's diagnostic; it does not authorize refitting.

## Verification performed before actual effects

`tests/test_bc60_v2_b1.py` contains six synthetic tests; all passed with zero model
fits. They check raw targets and shifted features; identifier exclusion; reversed
requested-ID order; 62 callback steps; first-step D0/D1 equality; distinct output
and feedback; lag/rolling provenance transitions; unchanged history and restored
native prediction state; missing-proxy rejection; alignment failure; clipping;
reproduction keys/tolerance; fixed-core missing output preservation; bridge versus
scored boundaries; and signed-error arithmetic identities.

The saved feature trace includes all terminal context series at every step,
per-feature observed and future-feedback source counts, cutoff proxy dates and
value, raw output, clipped scored output and actual recursive feedback. Signed
error growth is explicitly an arithmetic identity, not causal attribution.
