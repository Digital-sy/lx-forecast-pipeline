# BC60 V3 source audit: direct cumulative targets

Scope: read-only inspection of the original implementation, frozen inputs and
installed runtime; no fit, database query, new business data or Git operation.
The V3 user instruction was read in full. This audit proposes explicit rules for
preregistration; calendar upper bounds below are not measured training coverage.

## Verified source identity

| Resource | SHA256 |
| --- | --- |
| `scripts/bc60_experiment.py` | `5e433c029ebd8fa2c60e19a389a9d2ce5cd3c344238645160cec69f7c0e4f745` |
| `docs/bc60_design_lock_v1.json` | `3527e51cf6336e25706fe9cf3ea7f4c0a486341e4f6011f9e2760bd4409ebf28` |
| V1 `inputs_v1_final/daily.parquet` | `ff69063d3c4e506b2b2b8c701db6afa95b4f8af7ce7af3b7c17a898e70bfc7da` |
| V1 `inputs_v1_final/monthly_reference.parquet` | `423ce1086fc885d7d41e087e95b048638600710c1d174d3d18a9a3f1a98228af` |
| V1 `effects_v1/core_lists.csv` | `f634f9e1ee857be71db2e4f31e7136a4d7f6ff08761fb227edea972cc71f32cf` |

All listed hashes were computed from the existing files and match their
authoritative references. No intermediate input directory is valid.

The unchanged runtime inspected is MLForecast 1.0.2, LightGBM 4.6.0, pandas 2.2.3,
NumPy 2.2.6 and scikit-learn 1.6.1. Auxiliary packages remain coreforecast 0.0.18
and utilsforecast 0.2.17. No environment installation or upgrade was performed.

## Actual B1 parameters and model columns

Read the following values directly from the original lock, rather than creating
a fresh default configuration:

```json
{
  "objective": "poisson",
  "n_estimators": 200,
  "learning_rate": 0.05,
  "num_leaves": 31,
  "min_child_samples": 40,
  "reg_lambda": 1.0,
  "random_state": 20261019,
  "n_jobs": 2,
  "deterministic": true,
  "force_col_wise": true,
  "verbosity": -1
}
```

Original source lines 198–203 pass raw daily counts, no `weight_col`, no target
transform, and `static_features=['shop_code']`. Installed MLForecast
`forecast.py:285–294` adds `sample_weight` only for an explicit weight column.
The original identifier controls grouping but is not a predictor
(`core.py:299–330`). There is no explicit recency, shop or object weighting.

Freeze the same ten model columns: `shop_code`, `lag1`, `lag7`, `lag14`, `lag28`,
`rolling_mean_lag1_window_size7`, `rolling_mean_lag1_window_size28`, `dayofweek`,
`month`, `dayofyear`. Original numeric shop encoding at source line 130 is the
position in the original locked four-shop order. Do not learn an encoding on the
whole future dataset. Identifiers, sample dates, labels, eligibility, monthly
quantities, maturity and segment metadata must not enter the predictor matrix.

V3 changes the target to each segment's raw 30-day sum and changes the unit of a
training row to shop × SPU × historical issue × segment. Even with the same
Poisson loss and parameters, this is not a pure one-switch causal experiment.
Keep raw prediction and `max(raw, 0)` separately, record the count clipped, and
never round before scoring. Fit no more than two models per original issue and
six total; no early stopping on any original target period.

## Recommended historical eligibility rule

For every historical simulated issue `r`, define `c = r - 2 calendar days` using
the existing Asia/Shanghai business-date convention. Determine the latest fully
completed monthly period at that cutoff:

```python
last_complete_month = (c.normalize() + pd.Timedelta(days=1)).to_period('M') - 1
```

Use only monthly rows with `month <= last_complete_month` and positive `qty`.
The existing established criterion is at least four distinct observed positive
months, not four consecutive months and not merely an age of four months.
Preserve absent history as unknown/insufficient evidence. If reporting historical
observed maturity, calculate it from the earliest positive month available under
this same bound; it remains metadata, not a feature.

This restriction is necessary: original `lifecycle` at lines 31–38 uses
`(issue-2).to_period('M')` and includes that entire month. Its line 33 explicitly
assumes issue dates are day 2, making the cutoff a complete month end. Calling it
unchanged for arbitrary daily `r` would use a future portion of the current
month. Likewise, original lines 179–185 compute qualification at the final issue
and apply it to the whole daily training history; that cannot be reused as the
historical V3 qualification mask.

The period-arithmetic rule includes the cutoff month exactly when `c` is its last
calendar date. It handles leap days and year boundaries without comparing a
23:59:59 month-end timestamp against a midnight daily cutoff. A January 28 cutoff
can use months through December, while a January 31 cutoff can use January.

For prediction at each original `S`, apply the same rule at `C(S)=S-2`. Preserve
the fixed 385 core issue-object records and the original 16 insufficient B1
qualifications; never fill their output with an invented established-product
prediction. The original 369 B1-output records remain a secondary comparison
scope. Training context includes all safely mapped eligible objects in the four
shops, not just the fixed core.

## Recommended history and exact feature indexing

Require 28 consecutive calendar dates `[c-27,c]`, each safely mapped with state
OBSERVED or EXPLICIT_ZERO and a finite, nonnegative count. This does not mean the
last 28 stored observations. Explicit zeros are valid; absent, unknown, invalid
or ambiguous-mapping dates reject the sample without filling.

Do not carry the original `make_segments` 56-day guard into DIRECT. Original
lines 126–128 use it as a recursive-series implementation rule. The new user
instruction explicitly permits a 28-day input, identifies January 30 as the
earliest possible historical issue, and excludes additional 56/90-day features.
The existing positive-month qualification remains separate from this new sample
history requirement. This intentional rule difference should be disclosed.

Define sales-history features as they would appear at the original B1's first
next-day step `t=c+1`; no synthetic bridge predictions are required:

| Feature | Exact source dates |
| --- | --- |
| `lag1` | `y[c]` |
| `lag7` | `y[c-6]` |
| `lag14` | `y[c-13]` |
| `lag28` | `y[c-27]` |
| rolling 7 on lag 1 | mean of `y[c-6:c]`, inclusive |
| rolling 28 on lag 1 | mean of `y[c-27:c]`, inclusive |

Both models use that identical historical feature vector. M1 calendar fields
describe `r+1=c+3`; M2 calendar fields describe `r+31=c+33`. Freeze weekday as
Monday=0 through Sunday=6, month as 1–12, and day-of-year as 1–365/366. Use the
same definitions in training and inference. Never derive the lag offsets from
the future segment-start date: doing so would require unavailable future history.

For exact original numeric representation, integer input targets are converted
to float32 by installed `utilsforecast/processing.py:702–713`. Original B1 lag
features therefore use native float32 values; rolling updates use sequential
native-type accumulation and division, as established in the V2 source audit.
An implementation can reproduce those arithmetic operations from the observed
window without calling private MLForecast methods. If the preregistration instead
chooses float64 direct features, disclose the representation difference and do
not claim bit-identical B1 features. Neither choice permits target normalization,
new clipping or a new sample-weight scheme.

## Label boundaries and calendar support upper bounds

For a historical issue `r`, M1 uses `[r+1,r+30]` and M2 uses `[r+31,r+60]`.
Relative to the feature cutoff these are `[c+3,c+32]` and `[c+33,c+62]`. The two
30-day intervals do not overlap and jointly cover the original 60-day target;
the two bridge dates `c+1,c+2` are excluded from both labels.

At actual replay issue `S`, independently require M1 label end `r+30<=C(S)` and
M2 label end `r+60<=C(S)`. M1 must not be forced to wait for 60-day maturity.
Every label also needs all 30 valid calendar observations under the original
source-state and mapping rules. A zero label is valid only if all required days
are present and explicit/valid, never because missing rows summed to zero.

With first daily date January 1 and a 28-day input, earliest possible `r` is
January 30. Calendar arithmetic, before qualification or source exclusions, gives:

| Actual issue | Segment | Latest possible r | Possible distinct r upper bound | Per-object disjoint 30-day target upper bound |
| --- | --- | --- | ---: | ---: |
| April 2 | M1 | March 1 | 31 | 2 |
| April 2 | M2 | January 30 | 1 | 1 |
| June 2 | M1 | May 1 | 92 | 4 |
| June 2 | M2 | April 1 | 62 | 3 |
| August 2 | M1 | July 1 | 153 | 6 |
| August 2 | M2 | June 1 | 123 | 5 |

These are upper-bound checks, not results. Determine actual per-object disjoint
support by sorting eligible target intervals by end date and greedily retaining
the next interval whose start is later than the previous selected end. Disclose
overlap even when there are many cross-sectional rows. A technically trainable
one-date M2 fit requires LIMITED_TEMPORAL_SUPPORT. No valid training labels means
NOT_TRAINABLE; it must not block an independently trainable segment or issue.

## Safe schema inspection and remaining leakage risks

The actual authoritative daily schema has `ds: datetime64[ns]`, `shop/spu: object`,
`y: int64`, `mapped: bool`, and `sales_state: object`. Dates span January 1 through
October 1, 2026. The inspected stored rows contain OBSERVED and EXPLICIT_ZERO
states, no duplicate daily grain, no null y, and no negative valid-state y.
Those facts do not prove every object's calendar is complete; the explicit daily
grid gate remains necessary. Do not let the presence of auxiliary traffic,
advertising, amount or metadata columns expand the predictor set.

Monthly reference has only `shop`, `spu`, `month`, `qty`; `month` is a YYYY-MM
string and `qty` is int64. Its range extends from January 2024 through September
2026, allowing qualification evidence before the daily source begins. No duplicate
monthly grain, null quantity or negative quantity was found. It has no historical
acquired-at field; completed-month filtering prevents future-period leakage but
does not establish real historical availability. Classification remains
RECONSTRUCTED_HISTORICAL_REPLAY.

Priority checks for the independent verifier:

- Perturb values after a sample's information cutoff: its features and eligibility
  must not change. A future monthly positive record must not grant earlier status.
- Perturb replay target sales: fixed fitted-model predictions must not change.
  Rebuilding labels for later legitimately mature training samples is a separate
  operation, so do not confuse that with prediction leakage.
- Verify raw-source sums independently for both training maturity and scored
  target windows; do not import the label-builder into the scoring oracle.
- Assert the ten-column predictor allowlist, per-shop/SPU history isolation,
  unchanged fixed core keys, and the original missing-output rows.
- Require `Y60_hat=M1_hat+M2_hat` only when both predictions exist. M2 cannot read
  M1 output or realized first-segment sales. Any missing segment prevents a
  cumulative pass and a three-window joint hit.
- Keep primary denominators at all 385 fixed records; recompute every comparator
  on any disclosed new common subset. Use unrounded quantities for all metrics.
- Save data/model/output hashes and failed states; reports must reuse the saved
  models instead of consuming another fit. No post-result parameter change.
