# A16 / V4 isolated shadow design (not deployed)

## Boundary

Candidate `a16_shop_cutoff_v3_20261016` is a research adapter. Production V4 remains authoritative. NEW_VISIBLE H48/H60 and procurement recommendations have no dependency on this adapter. No runtime schedule, database table, migration, credential, or BI resource is included.

## Input contract

One immutable in-memory input envelope per issue cutoff contains:

- `issue_at`, `business_timezone=Asia/Shanghai`, `history_cutoff_month`;
- canonical `shop`, `spu`, validated SKU→SPU rows and mapping fingerprint;
- observed monthly history, explicit presence indicators, source business cutoff and source synchronization time;
- category/season metadata snapshot and fingerprint, provenance/effective-date limitations;
- source query and input hashes; selected model source commit and candidate manifest.

For target month T, H1/H2/H3 mean T minus issue-business-month = 1/2/3. Both models see only complete sales months strictly before issue-business-month. Do not silently use a partial current month or shift H semantics. Training labels must have target month before issue cutoff and known arrival time before issue_at for a prospective run. Historical replay cannot reconstruct the latter guarantee.

## Qualification and output states

At the same shop×SPU universe record `forecastability`, lifecycle rule version, observed age, visible-month count and mapping quality. A16 receives only ESTABLISHED; other objects retain INELIGIBLE, not zero. Output states: OUTPUT_POSITIVE, OUTPUT_ZERO, INELIGIBLE, MISSING_INPUT, MISSING_OUTPUT, FAILED. Truth states: MONTHLY_OBSERVED, DAILY_CONFIRMED_ZERO, SOURCE_CONFLICT, UNKNOWN. Never collapse these states.

The research formula's internal missing-history zero defaults must be disclosed in `missing_history_reads`. Missing recent inputs may make candidate unsuitable; no silent future routing or imputation is added in V3. Input-quality policy must be approved before execution.

## Execution isolation

1. Existing production pipeline finishes under its existing control; no hook is added this round.
2. After separate authorization, a standalone operator-triggered runner could consume the same approved input envelope. Default `enabled=false`; enabling is explicit and independent of production switches.
3. Run V4 replay and A16 in an isolated process with bounded CPU/memory/time; do not invoke production modules with module-level side effects. This repository currently supplies only the pure offline adapter and in-memory envelope tests, not an installed runner.
4. Candidate results cannot replace/append production comparison, prediction snapshot, recommendations, purchase orders, BI facts or workflow inputs. Proposed sink is a separate approved shadow artifact path with owner, retention, permissions and idempotency, not a production table. No sink exists this round.
5. Failure, timeout, incomplete input, duplicate keys or manifest mismatch marks the shadow batch failed. V4 and published values remain byte-for-byte unchanged. No fallback can relabel V4 as A16; disable the independent switch to stop observation.

## Proposed trace schema

Key: issue_at × shop × spu × target_month × horizon × model_name × model_version × input_hash.

Fields: candidate commit/hash, V4 source hash, metadata hash, input availability, source timestamps, training-label maximum month/arrival time, training row count, gamma_down/up, qualification, output state, forecast quantity, A3/A7 quantities and branch codes, V4 branch summary, absolute/relative model difference, warning flags, run start/end/status/error class. Relative difference is NULL when V4 is zero; preserve absolute difference and the zero flag.

## Quality and acceptance

- Manifest matches; same input hash and key set; shop grain preserved; cutoff and year rollover safe.
- A16 output covers 100% of eligible accepted inputs or the batch is explicitly partial/failed. Zero and missing are tested separately.
- Record coverage on full eligible universe as well as strict shared-model scoring; no outcome-based exclusion.
- Complete natural-month truth only; day/month conflicts evaluated separately until reconciled.
- Report WAPE, Bias and ±30% hit rate (positive-actual denominator), n, zero-actual count, and per-shop/age/horizon degradation.
- Pre-register prospective targets after authorization/freeze. Do not call the previously viewed retrospective months independent OOS. No automatic promotion thresholds are set by this design.
- Success permits observation only. Any production switch, procurement use, persistence, new schedule or BI integration requires separate authorization and acceptance.
