# Track E / Milestone 2 — `monthly_director_bundle_contract.json` cleanup

Filed during Track E M1 sign-off (per GPT) so the policy gap doesn't
get lost. **Not a Track E M1 blocker.** Don't open for work until M1 is
merged and Track F is underway.

## Problem

`config/monthly_director_bundle_contract.json` declares several
director-deck-consumed datasets as `policy: optional_empty`:

- `won_lost`
- `renewals`
- `approvals`
- `commit_items`
- `activity`
- `stage_events`
- `forecast_category_events`
- `close_date_events`
- `movement_prior`
- `movement_current`

Meanwhile the new `config/deck_contract.yaml` (Track E M1) governs the
director monthly deck, and the deck consumes those datasets via
`director_workbook` sheet bindings:

| Bundle dataset             | Deck workbook sheet         | Used by slide(s)                            |
| -------------------------- | --------------------------- | ------------------------------------------- |
| `won_lost`                 | `Won Lost FY26`             | 6 (q1_loss_drivers), 14 (forecast_accuracy) |
| `renewals`                 | `Renewals FY26`             | 17 (renewals)                               |
| `approvals`                | `Commercial Approval`       | 16 (commercial_approvals)                   |
| `commit_items`             | `Commit Items`              | 15 (forecast_mix)                           |
| `activity`                 | `Activity Volume`           | 8 (q2_deal_readiness)                       |
| `stage_events`             | `Stage History`             | 3 (since_last_review), 8, 13 (q1_slippage)  |
| `forecast_category_events` | `Forecast Category History` | 3, 8                                        |
| `movement_*`               | `Q1 Movement`               | 3, 13                                       |

These should not stay casually `optional_empty` forever — the deck
contract is governing them.

## Decision required

For each dataset, classify into one of:

1. **`required_for_publish: true`, `policy: source_backed`** — deck cannot publish without it; add a source contract requirement.
2. **`legitimate_optional`** — can legitimately be empty for some directors / scopes; add a `legitimate_when:` clause documenting the conditions.
3. **`suspicious_empty`** — empty is a data-quality signal that should fail the publish gate.

## Proposed scope

- Read every `optional_empty` dataset in `monthly_director_bundle_contract.json`.
- Cross-reference with the deck contract's `data_sources.director_workbook.required_sheets` and per-slide `tables[].sheet`.
- Patch the bundle contract with explicit policies + a brief rationale per dataset.
- Add tests asserting that the bundle policy and the deck contract agree on which datasets are required.

## Out of scope

- Adding new source extractors. This ticket is policy-only.
- Editing `build_*.py`. This ticket is contract-only.
- Materializing the workbook into the warehouse. Track F+.

## Acceptance criteria

- Every dataset has an explicit policy (no more silent `optional_empty`).
- The bundle contract and the deck contract are consistent on
  required-for-publish status.
- A new test asserts agreement between the two contracts.
- A short note added to `docs/2026-04-26-track-e-deck-contract-design.md`
  pointing at this cleanup as Track E/M2.
