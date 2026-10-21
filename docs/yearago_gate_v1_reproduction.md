# YEARAGO applicability gate: reproduction and scope

Use the existing isolated Python runtime with pandas 2.2.3, numpy 2.2.6 and
pyarrow 19.0.1. No model library is imported by the new gate modules. No database
or service connection is needed. Do not run the old experiment, fit, or validator
entrypoints: some old validators invoke model prediction.

Required original source commits:
- YEARAGO final reviewed: 83e0baa32f6de5cea6ff38e19c87f56fba67daba.
- DIRECT replay: 34bdc3b703c45326f3c638814ad18595c8124e83.
- This gate's preregistration: 8d898a0 (full hash in Git history).

The authorized archive's configuration points to the original replay and the
original decline-group archive. Preserve those exact source files/paths. The gate
checks all original manifest hashes, including model files, without loading models.
The reviewed score-manifest hash is explicitly pinned. It never reads the draft
score/report directories affected by historical CSV float parsing.

From this source checkout, with the existing isolated interpreter:

```text
python -B -m unittest tests.test_yearago_gate -v
python -B scripts/yearago_gate_route.py --yearago <authorized-yearago-archive> --destination <new-exclusive-output>
python -B scripts/yearago_gate_score.py --output <new-exclusive-output>
python -B scripts/yearago_gate_validate.py --output <new-exclusive-output>
python -B scripts/yearago_gate_review.py --output <new-exclusive-output>
python -B scripts/yearago_gate_report.py --output <new-exclusive-output> --report-dir report_reviewed
```

Existing output directories are rejected. If a display-only report is revised,
write to a new child name, without rerunning routing/scoring. Review artifacts
must accompany the report; numerical gate failure is a valid experiment result,
not a technical error to fix with extra rules.

Stages:
1. Router loads projected input/support/origin/prediction fields, never target
   actual/error/hit columns. Per-issue raw history stops at annual input start.
   It seals the routing artifact and access ledger before scoring begins.
2. Scorer loads final reviewed truth and archived predictions, selects no models,
   and evaluates full, paired, TRUE and FALSE scopes. CSV round-trip precision
   is retained where CSV is used; canonical predictions are read from parquet.
3. Independent validator recalculates primitive routing conditions, scalar metrics,
   daily sums, joint hits, bins and set inclusions, without importing score helpers.
4. Final review checks every annual segment against its canonical original file,
   rederives the five unrounded screening decisions, runs pure unit tests, and
   rechecks old hashes. This is not new model inference.

Output CSV/Parquet/JSON includes business records. Keep all outputs, models, truth,
input snapshots, command receipts and credentials off GitHub. Only these new
sanitized scripts, tests and specifications belong on the independent branch.

Evidence classification: post-hoc discovered rule, reconstructed development
replay, not independent OOS or historically proven point-in-time availability.
Complete annual windows are not proof of operating comparability. A numerical
failure must remain a failure; success would only justify prospective consideration.
No deployment, merge, scheduling, production changes or second-rule search.
