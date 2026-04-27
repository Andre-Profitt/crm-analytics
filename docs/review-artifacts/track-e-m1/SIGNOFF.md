# Track E Milestone 1 — sign-off evidence

This folder is the durable, repo-persisted evidence packet for Track E
Milestone 1. The originating sign-off request was PR #22; this folder
holds the final state of the validators after GPT's three M1 hardening
conditions were cleared.

The raw `output/track_e/` reports are gitignored and regenerable from
the live anchors (workbook + PPTX in `~/Downloads/`). The copies under
this folder have personal filesystem paths redacted (`/Users/<name>/`
→ `<sanitized>/`; the live anchors → `<workbook-anchor>` and
`<pptx-anchor>`) so the milestone evidence is publishable.

## Sign-off conditions

GPT approved Track E M1 with three conditions before Track F may
start. All three are cleared:

| #   | Condition                                | Status  | Evidence                                                                                                                                                                                                                                                                                                                           |
| --- | ---------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Add PPTX table-header validation         | cleared | `pptx_contract_report.json` exposes per-slide `header_status` and `header_results`. 14 `legacy_header_drift` warnings + 0 blockers against the live deck. Tests at `tests/test_track_e_pptx_checker.py` cover (a) mutated stable + dropped legacy → blocker, (b) `evidence_only` exclusion, (c) dual-table slide checked in order. |
| 2   | Tighten Salesforce link target semantics | cleared | `_link_satisfies_kind` enforces `salesforce_list_view` URL pattern (`simcorp.{lightning.force,my.salesforce}.com/(lightning/)?o/Opportunity/list`). Wrong-target hyperlink → blocker `required_link_target_mismatch`. Missing link entirely → warning (M1 transition). Tests exist for correct, wrong-target, missing.             |
| 3   | Persist M1 sign-off evidence in the repo | cleared | this folder.                                                                                                                                                                                                                                                                                                                       |

## Validator results (post-Cond 1+2 patch, against live anchors)

| Validator                           | Status | Detail                                                                                                                                        |
| ----------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `deck_contract` (E1)                | pass   | 0 blockers, 0 warnings, 18 slides                                                                                                             |
| `director_workbook_contract` (E1)   | pass   | 0 blockers, 0 warnings, 13 sheets, 9 snapshot_roles                                                                                           |
| `director_workbook_validation` (E2) | pass   | 13/13 sheets present, 9/9 snapshot_roles resolved                                                                                             |
| `deck_binding_report` (E3)          | pass   | 75/75 bindings resolve, 0 fails                                                                                                               |
| `pptx_contract` (E4)                | pass   | 0 blockers, 31 warnings (16 legacy_verbose_title + 14 legacy_header_drift + 1 missing_required_link_transition); 1 stable title; 18/18 slides |

## Resolved snapshot roles (live workbook anchor)

```
q1_opening         -> ARR 2026-01-01
q1_latest          -> ARR 2026-04-12
q1_change_to_close -> ARR Change 2026-04-12
q1_stage_opening   -> StageName_ 2026-01-01
q1_stage_latest    -> StageName_ 2026-04-12
q2_opening         -> ARR 2026-04-01
q2_latest          -> ARR 2026-04-15
movement_prior     -> runtime: prior_snapshot_date
movement_current   -> runtime: snapshot_date
```

## Q1 Forecast Variance derived bridge — resolved

```
binding_type    : derived_table
transform_id    : q1_forecast_variance_bridge
display_grain   : bucket
source_grain    : opportunity
sheet           : Q1 Snapshot Trend
snapshot_roles_resolved:
  opening_arr     -> ARR 2026-01-01
  latest_arr      -> ARR 2026-04-12
  change_at_close -> ARR Change 2026-04-12
rows[]: initial_q1_pipeline, closed_won_removed, closed_lost_removed,
        new_deals_added, arr_revised_up, arr_revised_down,
        final_q1_pipeline, net_change
status: pass
```

The opportunity-grain `tbl_q1_forecast_variance_evidence` is
`evidence_only: true`; the PPTX checker excludes it from per-slide
table-count expectations and from the per-table header check.

## Test suite

| Suite                                                     | Tests | Status   |
| --------------------------------------------------------- | ----: | -------- |
| `tests/test_track_e_*.py`                                 |    32 | all pass |
| Full repo (excluding two pre-existing unrelated failures) |   682 | all pass |

The two pre-existing failures (`test_harness_registry`, `test_salesforce_dashboard_executor_cli`) are documented in memory and are not Track E regressions.

## Hard NO-GOs preserved across the milestone

- No edits to `scripts/build_sd_monthly_deck_v2.py` or any `build_*.py`.
- No template-first builder (Track F).
- No render or visual regression gates (Track F).
- No release catalog or waivers (Track K).
- No OpenLineage events (Track J).
- No reusable workflows (Track L).
- `profiles.control_deck` declared but `status: deferred`.
- Director deck still consumes the live workbook directly; no warehouse-only rewrite.

## Forward-state items deferred (not blockers for M1)

1. **Salesforce drill-through link rendering** — current production deck has no hyperlink on slide 12 (pushed_deals). The PPTX checker emits an M1 transition warning today; once Track F starts editing the builder, this becomes a blocker.
2. **`monthly_director_bundle_contract.json` `optional_empty` policy refresh** — `won_lost`, `renewals`, `approvals`, `commit_items` are deck-consumed. Filed as Track E/M2 ticket at `docs/review-artifacts/track-e-m1/M2-CLEANUP-TICKET.md`.
3. **Builder convergence on stable headers** — every `legacy_header_sets` entry is a forward-state debt; pruning happens as the builder switches to the stable contract `columns[].header` strings. Tracked in Track F.

## Files in this folder

| File                                            | Purpose                                                                              |
| ----------------------------------------------- | ------------------------------------------------------------------------------------ |
| `SIGNOFF.md`                                    | This doc — milestone evidence summary                                                |
| `M2-CLEANUP-TICKET.md`                          | Track E/M2 ticket (`monthly_director_bundle_contract.json` `optional_empty` refresh) |
| `deck_contract_report.json`                     | E1 deck-contract validator output                                                    |
| `director_workbook_contract_report.json`        | E1 workbook-contract validator output                                                |
| `director_workbook_validation_report.{json,md}` | E2 real-workbook validation against live APAC                                        |
| `deck_binding_report.{json,md}`                 | E3 binding resolution against live APAC (75/75 pass)                                 |
| `pptx_contract_report.{json,md}`                | E4 PPTX check against live APAC LAND deck                                            |

## How to regenerate (against the same anchors)

```bash
python3 scripts/validate_deck_contract.py \
  --report-out output/track_e/deck_contract_report.json
python3 scripts/validate_track_e_workbook_contract.py \
  --report-out output/track_e/director_workbook_contract_report.json
python3 scripts/validate_track_e_workbook.py \
  --workbook <workbook-anchor> \
  --report-out output/track_e/director_workbook_validation_report.json \
  --md-out output/track_e/director_workbook_validation_report.md
python3 scripts/validate_deck_bindings.py \
  --workbook <workbook-anchor> \
  --report-out output/track_e/deck_binding_report.json \
  --md-out output/track_e/deck_binding_report.md
python3 scripts/validate_director_monthly_pptx.py \
  --pptx <pptx-anchor> \
  --report-out output/track_e/pptx_contract_report.json \
  --md-out output/track_e/pptx_contract_report.md
```
