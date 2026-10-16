# A16 shop×SPU candidate V3 — pre-test freeze

Status: research adapter, not the deployed model and not the original published Champion score.
Base: freshly fetched main 97b868d75cde24e4d408399cdeecc292395d1b4a.
Candidate: `a16_shop_cutoff_v3_20261016`. No production entrypoint or scheduler.

- Shop×SPU identity is preserved; category support is calculated separately per shop.
- Formula chain: A3 → A7 → round-12 A16, unchanged source formulas.
- H1 is an adapter extension; original A16 research scope is H2/H3.
- Target T with horizon h uses cutoff T−h at month start; only sales months strictly before cutoff.
- Calibration starts at 2025-01, same horizon, only observed labels with target strictly before cutoff.
- Gamma grid and objective are exactly round-5 choose_params; this causal recalibration rule is frozen, not one globally fixed gamma.
- Only forecastability ESTABLISHED receives A16; NEW_VISIBLE/COLD_NO_HISTORY/DORMANT_RETURN remain separately identified.
- Lifecycle uses visible positive sales history (up to 24 months in the research implementation), not SKU creation date. No historical lifecycle truth is asserted.
- Current category/season metadata are not point-in-time reconstructed. V4 uses current season; A16 uses current category. Explicit limitation applies to all replays.
- Missing history is not fabricated as actual truth. Legacy internal get(...,0) semantics remain frozen and missing reads are counted.
- A16 and V4 must use identical raw monthly history/cutoffs. Actual-month missingness is reported, never automatically zero-filled.
- Expansion predeclared: 2025-10/11/12 and 2026-01/02/03; regression reference 2026-07/08/09.
- These are retrospective temporal extensions, not certified independent OOS: prior model-development exposure cannot be ruled out and current metadata were unavailable historically.
- No tuning on this expansion; any future formula change needs a new version and genuinely held-out/prospective data.
- Shadow default disabled; in-memory validation only. No DB sink, procurement hook, credentials or external calls.

Generated freeze manifest records original-file and normalized SHA256, adapter hash and source commit before replay.
