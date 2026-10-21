# BLEND50 source evidence gate v1

## Scope and hard boundaries

Offline evidence validation only. No model import/fit/predict, API requests, DB
writes or live repair command exists in this module. The old BLOCKED archive is
immutable. Source PASS is not a forecast, new issuance or scorer acceptance.

`scripts/blend50_source_evidence.py` is the new evidence-aware entry point. The
old diagnostic audit remains unchanged and is not relabelled PASS. Its aggregate
receipts lack raw API page/key completion proof. This is intentional.

## Evidence contract

`source_slice_evidence_v1` requires an externally reviewed, pinned manifest hash,
an exact required date/shop/SID list and a separately hashed **current** readback.
The source grain is dt / store_name / country / asin, not SKU. Never infer zeros
for absent records. Acquired time, committed sync time and business date are
separate; unknown timestamps fail rather than receive fabricated dates.

Each slice binds:

1. Original credential-free response bytes (no headers, credentials or signed URL).
2. Serial request parameters and response request ID/code/total/time; contiguous
   offsets, stable total, no duplicate request/page or primary key, all rows.
3. Canonical projection matching the preserved native response. The minimal
   supported shapes come from the inspected deployed collector, not a new API
   promise. Unsupported shapes fail closed. Real response adapter acceptance
   remains outstanding; only synthetic examples have been exercised.
4. Independent platform same-scope export with an export ID/time and exact
   primary-key/quantity/mapping reconciliation. Equal counts alone are insufficient.
5. Publication transaction/task identifiers bound to the page manifest and content.
6. Post-publication current ODS readback with the same keys/quantities/mapping.
   Any content change invalidates the old receipt, even when its file is rehashed.

The whole required range needs evidence; seven repaired slices alone cannot
prove all training/selection/history inputs complete. Hash chains detect mutation,
not a maliciously fabricated source. Approved acquisition provenance and original
platform evidence must be reviewed. A handwritten `status: PASS` is ignored.

Collector staging may preserve unresolved SKU/SPU values and flag them; v1 model
gate blocks them pending explicit mapping dispositions. It never silently removes
their sales from the full-store selection denominator. Null/negative sales,
deletion and date anomalies also block. Explicit observed zero remains distinct.

## Read-only acquisition and versions

`blend50_source_acquire.py --proof-root <prior-batch>` reuses a verified reproduction
receipt and prediction seal, records their hashes, and writes a new query ledger.
It does not rerun historical inference. Frozen probe bounds remain unchanged:
serial, 30 business SELECT, 10 seconds each, 1M estimated scan each, 10M cumulative.
Native EXPLAIN precedes each business SELECT. Failed attempts remain archived.

## Commands (placeholders, no credentials)

```
python -B -m unittest tests.test_blend50_source_evidence -v
python -B scripts/blend50_source_evidence.py --root NEW_EVIDENCE_BUNDLE --manifest-sha256 REVIEWED_HASH --scope REQUIRED_SCOPE_JSON --current-ref CURRENT_READBACK_REF_JSON --out NEW_GATE_JSON
```

Output creation is exclusive. Missing receipts, unknown values, partial pages,
wrong date/shop/SID, tampering, source revisions, conflicting exports and unsafe
paths fail closed. Test fixtures are explicitly synthetic and cannot be used as
real acquisition authorization or production evidence.

## Remaining separately approved steps

Authenticate collector-repository publication without copying hardcoded secrets;
approve exact repair slices and source transaction/concurrency integration;
validate the real adapter and same-scope platform export; backup and repair;
re-read and validate required range and mapping; select a NEW real issue date;
freeze current core and input; run the previously authorized design's actual
current fits/inference and immutable sealing only after new execution approval.
Future scoring still requires a real issued-batch adapter and matured truth.

No claim that historical acquisition existed earlier, no backdating, no main merge.
