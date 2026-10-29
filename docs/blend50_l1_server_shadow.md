# BLEND50_L1_MEAN_V1 isolated server shadow

This adapter runs one prospective issue in an isolated release and venv. It has no
database/API client, scheduler, production writer, or purchasing integration.
Source acquisition remains the separately authorized read-only process.

## Frozen chain

DIRECT and YEARAGO use the frozen ten/seventeen feature definitions and Poisson
configuration. L1 changes only objective to `regression_l1` and metric to `l1`;
sample weights and initial scores are not passed. Fixed GATE gives
`B=D+0.5*(Y-D)` when true, otherwise exact D. Final output is `0.5*B+0.5*L`
for each segment, with the two segments summed for sixty days.

## Deployment contract

Copy only the reviewed scripts, tests and this configuration directory into a
new release. `release_manifest.json` records `code_commit` and a relative-path
SHA256 `files` dictionary. Never include credentials, input data or trained
models in Git. Run with an explicit release working directory/PYTHONPATH and
`PYTHONDONTWRITEBYTECODE=1`. Use an existing authorized low-privilege identity,
serial processes and the frozen two model threads. No service or cron is added.

The requirements lock is the dependency closure of the audited local research
environment, not the production environment. A private CPython 3.12.14 runtime
may be installed entirely within the authorized shadow root; the OS Python and
production venv are untouched. Verify package hashes from the official source
and record installed versions. Official references:
https://lightgbm.readthedocs.io/en/v4.6.0/Python-API.html
https://github.com/astral-sh/python-build-standalone/releases/tag/20260924

## Command sequence

Use explicit paths; the following are command templates, not evidence of execution.

```sh
python scripts/blend50_l1_server_parity.py verify --fixture FIXTURE --out NEW_PARITY --release-manifest RELEASE/release_manifest.json
python scripts/blend50_l1_server_issue.py prepare --root NEW_RUN --input-root VERIFIED_INPUT --monthly-input MONTHLY --frozen-config RELEASE/config/blend50_l1_shadow/frozen_config.json --release-manifest RELEASE/release_manifest.json --parity-receipt NEW_PARITY/parity_result.json --issue-date YYYY-MM-DD
python scripts/blend50_l1_server_issue.py fit --root NEW_RUN --slot direct_M1
# Then direct_M2, annual_M1, annual_M2, l1_M1, l1_M2, serially.
python scripts/blend50_l1_server_issue.py assemble --root NEW_RUN
python scripts/blend50_l1_server_validate.py --root NEW_RUN
python scripts/blend50_l1_server_issue.py seal --root NEW_RUN
python scripts/blend50_l1_server_issue.py verify --root NEW_RUN
python scripts/blend50_l1_shadow_review.py --run NEW_RUN --out NEW_PRIVATE_REPORT --production-reference PRODUCTION_REFERENCE_JSON
```

S is the actual Asia/Shanghai date; C=S-2. Training labels end by C. The next
day begins the first target window; seal must precede it. An incomplete fit slot
cannot be automatically refit. Six starts is the hard budget, including failed
starts. A sealed run is verified/reused, never overwritten. A future issue needs
a new explicit authorization and independent directory.

Historical parity uses six saved models, zero fits, and truth-free inference
fixtures; tolerance was fixed before execution to `1e-6 + 1e-8*abs(reference)`.
Missing/qualification/zero boundaries have synthetic tests, separately reported
from real archived-model parity. Current validation independently recomputes
core selection, mature training labels, gate, formulas, coverage and hashes.

The offline HTML contains private business predictions and no future accuracy.
Natural-month production reference stays separate from rolling windows.
Disabling means making no further manual invocation and withdrawing only the
private review link if needed; do not delete sealed batches or stop production
tasks. There is no enabled shadow schedule or success pointer to disable.
