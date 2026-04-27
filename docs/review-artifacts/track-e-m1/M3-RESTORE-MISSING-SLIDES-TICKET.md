# Track E / Milestone 3 — restore slides removed from `build_deck_from_excel.py`

Filed during the Track E re-anchor (2026-04-27). **Not a Track F blocker.**

## Problem

The Track E M1 contract was anchored to a 2026-04-20 production deck (`jesper-tyrer-LAND.pptx`) with 18 slides. As of 2026-04-27, `scripts/build_deck_from_excel.py` produces only **16 slides** — two slides were removed from the builder between 2026-04-20 and 2026-04-27.

To unblock Track F, the contract was re-anchored to the current 16-slide builder output. The two removed slides are tracked here as restore candidates.

## Slides removed

| Old slide # | Stable id              | Purpose                                                                                                                                           | Removed because                                                                |
| ----------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| 5           | `q1_forecast_variance` | Q1 forecast variance bridge — 8-row bucket bridge (Initial Q1 → Closed Won → Closed Lost → New deals → ARR up → ARR down → Final Q1 → Net change) | unknown — needs git blame on build_deck_from_excel.py                          |
| 8           | `q2_deal_readiness`    | Per-deal Q2 readiness table — days to close, last activity, AuM, readiness, next step                                                             | possibly merged into top_open_opportunities (slide 7 has 8 cols including Age) |

## Context

GPT's note when granting Track E approval explicitly called out the bridge as a strong pattern: "Q1 Forecast Variance ... is the kind of bridge that lets a director see what happened to the opening pipeline." The contract's `q1_forecast_variance_bridge` derived_table was specifically designed to govern that 8-row bucket layout.

The validator code paths for `binding_type=derived_table` are kept (synthetic injection in tests covers them), so when a builder PR restores the slide, the contract can flip the slide back to `status: active` without further validator work.

## Decision required

For each removed slide, decide:

1. **Restore in builder** — re-add `slide_q1_forecast_variance` and `slide_q2_deal_readiness` functions in `build_deck_from_excel.py`. Re-add the slides to the deck contract `profiles.director_monthly.slides` block. Re-add the `derived_table` (q1_forecast_variance_bridge) and `q2_deal_readiness` table bindings.

2. **Accept as deprecated** — document why each slide was removed (was it stakeholder feedback?) and leave the contract at 16 slides.

3. **Replace with successor slide** — if a different slide carries the same value (e.g., q2_deal_readiness merged into top_open), document the equivalence and update the contract takeaway/columns to capture the merge.

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
