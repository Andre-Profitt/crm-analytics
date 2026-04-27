# Track E — Director Monthly deck contract foundation (Milestone 1 design, retargeted)

- **Integration branch:** `integration/track-e-director-monthly-contract`
- **Anchored ETL baseline:** `docs/checkpoints/v20d-etl-spine-freeze-2026-04-30/` (Milestone 0, ETL 1.0)
- **Anchored deck artifact:** `output/simcorp_director_decks/2026-04-20/land-only/jesper-tyrer-LAND.pptx` (Rebekka-aligned APAC build, the v2→v13 chain productionalized 2026-04-20)
- **Anchored upstream workbook:** `output/director_live_workbooks/2026-04-20/jesper-tyrer.xlsx` (13-sheet APAC live director workbook)
- **Builder lineage:** `scripts/build_sd_monthly_deck_v2.py`, fed by `run_sales_director_monthly_cadence.py` and the simcorp_director_decks lane.
- **Status:** retargeted from control deck to director_monthly per GPT Reading B (2026-04-26).

## What changed and why

The first scaffold of Track E (commit `b2903da`) anchored to `scripts/build_source_backed_deck.py` — a 6-slide internal "is the data trustworthy" control deck. After review, this was the wrong artifact: the 6-slide deck has near-zero stakeholder blast radius, while the actual monthly deliverable is the 9 director-facing decks under `output/simcorp_director_decks/<date>/land-only/<director>-LAND.pptx`.

Reading B confirmed: **Track E governs the director-facing Sales Director Monthly deck first.** The control deck becomes `profiles.control_deck` later; it is not the Milestone 1 anchor.

This commit replaces the prior `config/deck_contract.yaml` and design doc with the director_monthly profile. The brand block, JSON Schema plumbing, validator pattern, and mart cross-reference machinery from the prior scaffold are preserved.

## Hard NO-GO this milestone

- PowerPoint builder edits (`build_sd_monthly_deck_v2.py` and any `build_*.py`) — Track F (template-first builder) only.
- Render/visual regression gates — Track F.
- Forcing the director deck to consume warehouse marts directly — the director deck reads the Excel workbook today; warehouse materialization is a separate later track.
- Release catalog, waivers — Track K.
- OpenLineage events — Track J.
- Reusable workflows — Track L.
- Governing the 6-slide source-backed control deck (`profiles.control_deck` is declared but **`status: deferred`**).

## What this milestone delivers

### Artifacts

1. `config/deck_contract.yaml` — `profiles.director_monthly` with 18 stable slide IDs, separated `title` / `generated_takeaway` / `tables`, `period` / `population` / `movement_window` blocks, table bindings to workbook sheets/columns. `profiles.control_deck` declared but `status: deferred`.
2. `schemas/deck_contract.schema.json` — strict JSON Schema (draft 2020-12) covering profiles, slide registry, takeaway templates, table bindings, workbook + warehouse data source types.
3. `config/director_workbook_contract.yaml` — first-class workbook source contract: 13 required sheets, required columns per sheet, required date/currency parsing, snapshot/territory/director metadata.
4. `schemas/director_workbook_contract.schema.json` — strict JSON Schema for the workbook contract.
5. _(later in this milestone)_ `scripts/monthly_platform/deck_contract.py` — loader + cross-reference validator (covers profile selection, slide identity, table binding cross-checks against workbook contract).
6. _(later in this milestone)_ `scripts/validate_deck_contract.py` + `scripts/validate_director_workbook_contract.py` CLIs.
7. _(later in this milestone)_ `scripts/monthly_platform/deck_binding_resolver.py` + `scripts/validate_deck_bindings.py` — resolves slide table bindings against a real director workbook + (where applicable) warehouse run; emits `deck_binding_report.json/.md`.
8. _(later in this milestone)_ `scripts/validate_pptx_against_contract.py` — reads a generated `.pptx` and validates slide count, stable title text, table count per slide, table headers, required source notes, legal notice, Salesforce links. Does not modify the builder.
9. Tests + fixtures (positive + 10 negative controls).

This commit ships items 1-4 (the contract + schema scaffold). Items 5-9 follow on the same branch.

## The 18-slide stable registry (anchored to jesper-tyrer-LAND.pptx, verified 2026-04-26)

Every slide identity is stable. Dynamic numbers go in `generated_takeaway`, not `title`. Tables are governed by `tables[]` bindings.

| #   | Stable id                | Stable title                           | Tables | Primary workbook source                                                       |
| --- | ------------------------ | -------------------------------------- | ------ | ----------------------------------------------------------------------------- |
| 1   | `cover`                  | Sales Director Monthly Pipeline Review | 0      | (metadata-only)                                                               |
| 2   | `executive_summary`      | Executive Summary                      | 0      | Pipeline Open FY26, Won Lost FY26                                             |
| 3   | `since_last_review`      | Since Last Review                      | 1      | Q1 Movement, Stage History, Forecast Category History                         |
| 4   | `q1_promised_delivered`  | Q1 Promised vs Delivered               | 0      | Q1 Snapshot Trend, Won Lost FY26                                              |
| 5   | `q1_forecast_variance`   | Q1 Forecast Variance                   | 1      | Q1 Snapshot Trend, Q1 Movement                                                |
| 6   | `q1_loss_drivers`        | Q1 Loss Drivers                        | 2      | Won Lost FY26                                                                 |
| 7   | `q2_outlook`             | Q2 Outlook                             | 0      | Pipeline Open FY26, Q2 Snapshot Trend                                         |
| 8   | `q2_deal_readiness`      | Q2 Deal Readiness                      | 1      | Pipeline Open FY26, Activity Volume, Stage History, Forecast Category History |
| 9   | `top_open_opportunities` | Top Open Opportunities                 | 1      | Pipeline Open FY26                                                            |
| 10  | `deal_risk_triage`       | Deal Risk Triage                       | 1      | Pipeline Inspection, Pipeline Open FY26                                       |
| 11  | `owner_coaching`         | Owner Coaching Priorities              | 1      | Pipeline Open FY26, Pipeline Inspection                                       |
| 12  | `pushed_deals`           | Pushed Deals                           | 0      | Pipeline Open FY26                                                            |
| 13  | `q1_slippage`            | Q1 Slippage                            | 1      | Q1 Movement, Pipeline Open FY26                                               |
| 14  | `forecast_accuracy`      | Forecast Accuracy                      | 0      | Q1 Snapshot Trend, Won Lost FY26                                              |
| 15  | `forecast_mix`           | Forecast Mix                           | 1      | Commit Items, Pipeline Open FY26                                              |
| 16  | `commercial_approvals`   | Commercial Approvals                   | 4      | Commercial Approval                                                           |
| 17  | `renewals`               | FY26 Renewals                          | 1      | Renewals FY26                                                                 |
| 18  | `legal_notice`           | Legal Notice                           | 0      | (static text)                                                                 |

Verified against the attached APAC deck: 18 slides, table counts match. The current production builder generates dynamic, sentence-style titles (e.g. "3 owners carry 50 pushes across EUR 22.0M. Coach in this order."). The contract requires the **stable** title; the builder will be updated to populate the `generated_takeaway` slot underneath in a later milestone (Track F or a small follow-up under Track E).

## Title vs takeaway separation

For every slide:

```yaml
title: "Q2 Deal Readiness" # stable, max 48 chars, no currency, no counts
generated_takeaway: # dynamic, regenerated per run
  required: true
  max_chars: 150
  template: "{territory}: {q2_deal_count} Q2 deals, {q2_open_arr_eur} open ARR. {readiness_summary}"
  required_metrics:
    [q2_open_deal_count, q2_open_arr_unweighted, q2_readiness_summary]
```

This unblocks: stable contract IDs, dynamic narrative regeneration, validators that don't need to parse free-form English.

## Period / population / movement_window discipline

Every metric and table binding must declare its semantic scope so two true numbers cannot look contradictory (the EUR 6.6M → EUR 4.7M vs EUR 6.6M → EUR 0 issue GPT flagged on slide 5):

```yaml
period:
  role: q1
  start: 2026-01-01
  end: 2026-03-31
population:
  type: Land
  territory: ${territory}
  close_date_scope: q1_original_close_date | current_q1_close_date | q1_historical_snapshot
definition: opening_pipeline | final_open_pipeline | closed_outcome | historical_trending_delta
```

Movement slides also declare their window:

```yaml
movement_window:
  from: 2026-04-20
  to: 2026-04-22
```

Cover-level metadata is set once on the contract and inherited:

```yaml
presentation_month: 2026-04
snapshot_date: 2026-04-22
prior_snapshot_date: 2026-04-20
analysis_scope: Q1-Q3 FY26
```

## Data source types

Two first-class types in M1:

```yaml
data_sources:
  director_workbook:
    type: excel_workbook
    contract: config/director_workbook_contract.yaml
    required_sheets:
      [
        Summary,
        Pipeline Open FY26,
        Won Lost FY26,
        Commercial Approval,
        Renewals FY26,
        Pipeline Inspection,
        Activity Volume,
        Commit Items,
        Q1 Movement,
        Stage History,
        Forecast Category History,
        Q1 Snapshot Trend,
        Q2 Snapshot Trend,
      ]
  warehouse:
    type: parquet_warehouse
    required_marts:
      [
        mart_source_run_summary,
        mart_director_source_health,
        staged_source_quality_findings,
        staged_distribution_findings,
      ]
```

The director deck reads `director_workbook` today; the warehouse is referenced for cross-checks (e.g. director_source_health for source readiness on the cover) but is not the primary feed in this milestone. Materializing the workbook into the warehouse is **explicitly deferred** to a later track.

## Director presets — referenced, not inlined

The 9 territory presets remain in `config/sales_director_md1_presets.json`. The deck contract references them rather than duplicating:

```yaml
director_profiles:
  source: config/sales_director_md1_presets.json
  required_count: 9
  required_fields:
    [director, territory, region, salesforce_filters, output_slug]
```

Per-territory variations (e.g. APAC = Land only; Adam Steinhaus = US-only P&I) are operational knobs that belong in the preset file, not in the deck contract.

## Acceptance criteria (Milestone 1 done means)

1. `config/deck_contract.yaml` exists with `profiles.director_monthly` defining all 18 slides; `profiles.control_deck` declared but `status: deferred`.
2. Every slide has stable `id`, `slide_number`, `title` (≤48 chars, no currency, no count), `layout`, `generated_takeaway` block (or explicit `required: false`), `required_source_notes`, and either tables or metric bindings.
3. Every `tables[].mart` resolves to either:
   - a `data_sources.director_workbook.required_sheets[]` entry, with every column resolving to a known column on that sheet (cross-checked against `director_workbook_contract.yaml`), or
   - a `data_sources.warehouse.required_marts[]` entry with column on the registered Pandera schema.
4. Every period-bearing slide declares `period` + `population` + `definition`. Movement-bearing slides declare `movement_window`.
5. `config/director_workbook_contract.yaml` declares all 13 sheets and the columns the deck contract binds to.
6. Negative-control fixtures (10 cases) all fail validation cleanly:
   - missing slide id, duplicate slide_number, unknown sheet, unknown column, missing required generated_takeaway, invalid table binding, unknown profile, missing source note, invalid template path, missing period/population on a period-bearing slide.
7. Contract validator + workbook validator pass on the canonical inputs.
8. Binding resolver passes against the attached APAC workbook (`jesper-tyrer-2026-04-20.xlsx`).
9. PPTX validator passes against the attached APAC deck (`jesper-tyrer-LAND.pptx`): 18 slides, expected stable titles (or recognized dynamic-title legacy form during transition), expected table counts per slide, legal notice present.
10. No edits to `build_sd_monthly_deck_v2.py` or any `build_*.py` (per project CLAUDE.md). The PPTX validator is read-only.
11. Existing 96/96 contract test suite stays green.

## Implementation order

1. Rewrite design doc (this file). ✅ on commit.
2. Rewrite `config/deck_contract.yaml` with profiles + 18-slide registry + workbook bindings.
3. Rewrite `schemas/deck_contract.schema.json` to add `profiles`, `generated_takeaway`, `period`/`population`/`movement_window`, `data_sources.director_workbook`, `data_sources.warehouse`.
4. Write `config/director_workbook_contract.yaml` + `schemas/director_workbook_contract.schema.json`.
5. Pre-commit: validate the new contract structurally + cross-check workbook columns against the attached `jesper-tyrer-2026-04-20.xlsx` + slide titles/table counts against the attached `jesper-tyrer-LAND.pptx`.
6. Commit reset to `integration/track-e-director-monthly-contract`.
7. _(next commits)_ loaders + validators + binding resolver + PPTX validator + tests + fixtures.

## Open items (handed back to GPT after first reset commit)

- `monthly_director_bundle_contract.json` declares many datasets as `optional_empty` (won_lost, renewals, approvals, commit_items, etc.) — but the attached APAC workbook has those sheets populated with real data, and the deck consumes them. The deck contract should not be blocked by the bundle contract's "optional" stance, but we may want to upgrade those bundle policies to `source_backed` once requirements land. Not in M1 scope.
- The PPTX validator's "stable title" check needs a transition mode: the current production deck has dynamic titles. Two paths: (a) accept legacy dynamic titles during transition with a warning; (b) only enforce stable titles after the builder is updated. Recommend (a) for M1 so we don't block on a builder change.
- Whether to fold `presentation_month` / `snapshot_date` / `prior_snapshot_date` / `analysis_scope` under `profiles.director_monthly.run_metadata` (per-profile) or at top-level (cross-profile). Going per-profile so each deck profile owns its own period semantics.
