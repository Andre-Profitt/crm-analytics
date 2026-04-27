# Track E / Milestone 3 — restore conditional slides on `build_deck_from_excel.py`

Filed during the Track E re-anchor (2026-04-27). Updated 2026-04-27 after Track F sub-milestones — the slides aren't removed, they're CONDITIONAL.

## Updated finding (2026-04-27 post-Track F)

**The slides are not removed from the builder. They are conditionally included based on data availability.**

- `slide_forecast_variance` — function exists at `build_deck_from_excel.py:2085`, called at `:3738` only when `analytics.get("variance")` is truthy. The `variance` payload is populated by `read_director_analytics()` reading the Forecast Variance bucket data from `output/sharepoint/FY26 Pipeline Review, All Territories.xlsx`. Verified 2026-04-27: even with the analytics workbook present, the variance dict stays None — the analytics workbook's retrospective consolidated tab needs to carry director-territory-keyed bucket rows the function can match.

- `slide_q2_forward_look` — function exists at `:1302`, called at `:3866` only when SF live enrichment succeeds. Requires (a) `config/sd_monthly_territories.json` with the director's `soql_where`, (b) SF auth via `sf org display`, (c) running SOQL against the live org. The standalone `build_deck_from_excel.py` call without territory config logs `[SKIP] Forward Look: territory config not found`.

So restoring these two slides is **not a builder code change** — it's a data + environment configuration:

1. For `forecast_variance`: ensure the analytics workbook's retrospective consolidated tab is populated and parses correctly for the director-territory pair.
2. For `q2_forward_look`: ensure the cadence runner (which has `sd_monthly_territories.json` + SF auth) is the test environment for visual regression baselines.

## Problem (original)

The Track E M1 contract was anchored to a 2026-04-20 production deck (`jesper-tyrer-LAND.pptx`) with 18 slides. As of 2026-04-27, `scripts/build_deck_from_excel.py` produces only **16 slides** in standalone mode — two slides are conditionally included only when the cadence pipeline supplies them.

To unblock Track F, the contract was re-anchored to the current 16-slide build that the standalone validator can reproduce.

## Slides removed

| Old slide # | Stable id              | Purpose                                                                                                                                           | Removed because                                                                |
| ----------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| 5           | `q1_forecast_variance` | Q1 forecast variance bridge — 8-row bucket bridge (Initial Q1 → Closed Won → Closed Lost → New deals → ARR up → ARR down → Final Q1 → Net change) | unknown — needs git blame on build_deck_from_excel.py                          |
| 8           | `q2_deal_readiness`    | Per-deal Q2 readiness table — days to close, last activity, AuM, readiness, next step                                                             | possibly merged into top_open_opportunities (slide 7 has 8 cols including Age) |

## Context

GPT's note when granting Track E approval explicitly called out the bridge as a strong pattern: "Q1 Forecast Variance ... is the kind of bridge that lets a director see what happened to the opening pipeline." The contract's `q1_forecast_variance_bridge` derived_table was specifically designed to govern that 8-row bucket layout.

The validator code paths for `binding_type=derived_table` are kept (synthetic injection in tests covers them), so when a builder PR restores the slide, the contract can flip the slide back to `status: active` without further validator work.

## Decision required (revised 2026-04-27)

The slides exist as functions; they need data + environment plumbing to surface in builds:

1. **Wire the analytics-workbook variance** — populate the retrospective consolidated tab in `output/sharepoint/FY26 Pipeline Review, All Territories.xlsx` so `read_director_analytics()` can return a non-empty `variance` dict for each director-territory pair. Then standalone builds emit slide_forecast_variance.

2. **Establish a cadence-environment baseline** — switch the visual-regression baseline (and the live anchor at `~/Downloads/jesper-tyrer-LAND.pptx`) to a build produced by the cadence runner with full territory config + SF auth, so q2_forward_look + forecast_variance both render. The contract grows back to 18 slides; the validator's slide_count expectation moves 16 → 18.

3. **Or accept conditional slides in the contract** — add a `conditional_inclusion: { reason: "analytics_workbook.variance" | "sf_live_enrichment" }` field to slide entries; the PPTX checker treats missing conditional slides as pass-with-info instead of fail. Slide count expectation becomes a min/max range. This is the cleanest "describe truth" path, but requires schema + checker changes.

Recommend (3) for the lowest blast radius — it's a contract refinement that doesn't depend on data plumbing or test-environment changes.

## Out of scope

- Editing dashboard builders. The standing rule still applies; only `build_deck_from_excel.py` is in scope.
- Track F builder convergence. M3 restoration is a separate slice; F1–F6 keep their order.
- Re-anchoring Track E _again_ — the 16-slide anchor is the working contract until M3 restores slides.

## Acceptance criteria (when M3 ships)

- `slide_q1_forecast_variance` (or equivalent) is in `build_deck_from_excel.py` and emits a slide with the bucket bridge table.
- `slide_q2_deal_readiness` (or successor) emits its own slide.
- Deck contract has 18 slides again with the restored stable ids.
- All 5 Track E validators pass against the new live anchor.
- The Track F validator CI (when added) gates the restoration PR.

## Pointer to the re-anchor commit

Re-anchor commit on `integration/track-f-template-first-builder` 2026-04-27 ships:

- `config/deck_contract.yaml` — 18 slides → 16 (drops `q1_forecast_variance` and `q2_deal_readiness`)
- `scripts/monthly_platform/deck_contract.py` — `DIRECTOR_MONTHLY_EXPECTED_SLIDES = 18 → 16`
- `tests/test_track_e_*.py` — counts updated; `derived_table` tests use synthetic injection
- `docs/review-artifacts/track-e-m1/*.json` — reports regenerated against fresh build
- This ticket
