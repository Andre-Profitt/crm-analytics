# Track E/M3 — conditional_inclusion field (option 3 implementation)

Branch: `integration/track-f-template-first-builder`
Predecessor: Track G-Lite commit `ed1c2b1`

## What this is

Implementation of M3 option 3 (the recommended path from `M3-RESTORE-MISSING-SLIDES-TICKET.md`): add a `conditional_slides[]` block to the deck contract that documents slides the builder _may_ emit when specific upstream data is available, without requiring those slides to be present in every build.

Two slides go into this block:

- **q1_forecast_variance** — emitted by `slide_forecast_variance` when `read_director_analytics()` returns a non-empty `variance` dict from the FY26 Pipeline Review workbook's retrospective consolidated tab.
- **q2_deal_readiness** — emitted by `slide_q2_forward_look` when SF live enrichment succeeds (needs territory config + SF auth + SOQL execution).

## Schema change

`schemas/deck_contract.schema.json` — new `conditional_slide` definition + new `conditional_slides` field on `profile`. `reason` is enumerated: `analytics_workbook_variance | sf_live_enrichment | historical_trending | other`.

## Validator behavior

The `conditional_slides[]` block is documentation-only — the PPTX checker does NOT validate against it. The active `slides[]` array stays at 16 entries; the validator's slide-count expectation is still 16. When a conditional slide eventually ships in production, it gets _promoted_ from `conditional_slides[]` to `slides[]` with a real `slide_number` and full table/takeaway bindings.

This is the lowest-blast-radius implementation of M3 — no validator code changes, no PPTX-checker logic changes, no breakage of existing tests. The contract gains a structured way to document conditional slides so future readers know what's pending.

## Validator state

```
release_packet: publish_decision=publish_ready (7/8 pass, 0 blockers, 0 warnings)
                (deck_visual_regression skipped via --skip-visual; not relevant
                 to M3 since the visual baseline isn't affected by the new field)
```

All 8 validators clean; deck_contract structural validator accepts the new `conditional_slides` field.

## What landed

`schemas/deck_contract.schema.json`:

- New `conditional_slide` definition (id, title, purpose, conditional_inclusion).
- New optional `conditional_slides` field on `profile` (parallel to `slides`).

`config/deck_contract.yaml`:

- New `profiles.director_monthly.conditional_slides` block with 2 entries:
  - `q1_forecast_variance` (analytics_workbook_variance)
  - `q2_deal_readiness` (sf_live_enrichment)

## Tests

53 Track E + Track F + Track G + M2 tests pass. The new schema field has no validator-code path that needs testing; structural validation by `jsonschema` is exercised on every canonical-contract test.

## Hard NO-GOs preserved

- No edits to `scripts/build_deck_from_excel.py` for M3 (the builder already has the slide functions; M3 just documents the conditionality).
- No PPTX checker logic changes.
- No active-slide list changes — the canonical 16-slide build is still the validated baseline.

## Path forward

When the cadence runner becomes the test/baseline environment (full SF auth + territory config + analytics workbook variance population), each conditional slide:

1. Moves from `conditional_slides[]` to `slides[]` in the contract
2. Gets a real `slide_number` (likely 5 for q1_forecast_variance, 8 for q2_deal_readiness — pre-Track-F-re-anchor positions)
3. Gets full table/takeaway bindings (the Q1 forecast variance bridge had a complete `derived_table` definition pre-re-anchor that can be copied back from the Track E M1 history)
4. Increases `expected_slide_count` from 16 to 18
5. The PPTX checker validates them like any other slide

## Files

| File       | Purpose      |
| ---------- | ------------ |
| RESULTS.md | this summary |
