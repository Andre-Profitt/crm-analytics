# Track F / F1 — stable titles + takeaway split (results)

Branch: `integration/track-f-template-first-builder`
Anchored against: `<pptx-anchor>` (rebuilt from `<workbook-anchor>` post F1 edits)
Predecessor: re-anchor commit `9283f60` (16-slide contract baseline)

## Acceptance gate

| GPT criterion                                   | Status   | Evidence                                                                        |
| ----------------------------------------------- | -------- | ------------------------------------------------------------------------------- |
| `pptx_contract.legacy_verbose_title_count == 0` | **pass** | `pptx_contract_report.json`                                                     |
| `pptx_contract.blocker_count == 0`              | **pass** | `pptx_contract_report.json`                                                     |
| Generated takeaway separate from stable title   | **pass** | every non-static slide emits stable text in ph 144, dynamic narrative in ph 145 |
| No table/header/template/link work in F1        | **pass** | only ph 144 + ph 145 lines touched on `slide_*` functions                       |

## Validator results (post-F1)

```
deck_contract:                pass (blockers=0 warnings=0 slides=16)
director_workbook_contract:   pass (blockers=0 warnings=0 sheets=13 roles=9)
director_workbook_validation: pass (blockers=0 warnings=0 sheets=13/13 roles_resolved=9/9)
deck_bindings:                pass (64/64 pass, warn=0 fail=0)
pptx_contract:                pass (blockers=0 warnings=14 stable=15 legacy=0 slides=16/16)
```

Pre-F1 state was `pptx_contract: pass (warnings=29 stable=0 legacy=15 slides=16/16)`.

## Builder edits

`scripts/build_deck_from_excel.py` — 14 functions changed (cover + 13 narrative slides). Two-batch transformation:

**Batch 1 (6 edits, blocker-clearing):**

- slide 2 `slide_executive_summary`: `f"Exec. Summary | {territory}"` → `"Executive Summary"`
- slide 4 `slide_q1_promised_vs_delivered`: `f"Q1 Promised vs Delivered | {territory}"` → `"Q1 Promised vs Delivered"`
- slide 12 `slide_forecast_combined`: `f"Forecast Accuracy | {territory}"` → `"Forecast Accuracy"`
- slide 10 `slide_pushed_deals_with_link`: `f"Pushed Deals: ... exposed ARR"` → `"Pushed Deals"`
- slide 11 `slide_q1_movement`: verbose narrative → `"Q1 Slippage"`; narrative moves to ph 145
- slide 9 `slide_owner_coaching`: verbose narrative → `"Owner Coaching Priorities"`; narrative moves to ph 145

**Batch 2 (12 edits across 8 slides):**

- slide 3 `slide_month_over_month` (since_last_review): `"Since last review (...): what moved"` → `"Since Last Review"`
- slide 5 `slide_win_loss_diagnostic` (q1_loss_drivers): two paths (empty + data); both → `"Q1 Loss Drivers"`
- slide 6 `slide_quarter_outlook` (q2_outlook): verbose → `f"{q_label} Outlook"` (covers Q2/Q3 fallback)
- slide 7 `slide_top_deals` (top_open_opportunities): two paths; both → `"Top Open Opportunities"`
- slide 8 `slide_deal_risk_scoring` (deal_risk_triage): two paths; both → `"Deal Risk Triage"`
- slide 13 `slide_forecast_combined` (forecast_mix): `f"Forecast Breakdown: ..."` → `"Forecast Mix"`
- slide 14 `slide_commercial_approvals`: two paths; both → `"Commercial Approvals"`
- slide 15 `slide_renewals`: two paths; both → `"FY26 Renewals"`
- slide 1 `slide_cover`: `"Monthly Pipeline Review"` → `"Sales Director Monthly Pipeline Review"`

In every dynamic-data path, the narrative that used to live in ph 144 is now in ph 145 (overwriting any prior static description text). Static descriptions were generic flavor — the dynamic narrative is the higher-value content per GPT's "dynamic takeaway moves below title" intent.

## Contract clean-up

`config/deck_contract.yaml`: `legacy_title_patterns` blocks dropped from all 15 narrative slides (cover + 14 content). Stable contract title is now the only accepted form. The validator's title-status returns `pass_stable` for every slide.

`legacy_header_sets` blocks remain in place — they're F2 territory.

## Test changes

`tests/test_track_e_pptx_checker.py`:

- `test_apac_anchor_passes_in_legacy_mode`: assertion changed from `legacy_verbose_title_count == 15` to `== 0`; added `stable_title_count == 15`.
- `test_title_neither_stable_nor_legacy_blocks`: now mutates the stable contract title (in addition to dropping legacy patterns) so the produced stable title doesn't match. Without this change the test would no-op since the produced deck now emits stable titles directly.

All 32 Track E tests pass. 682/682 unrelated tests pass.

## Files in this folder

| File                             | Purpose                                                                    |
| -------------------------------- | -------------------------------------------------------------------------- |
| `F1-RESULTS.md`                  | This summary                                                               |
| `pptx_contract_report.{json,md}` | Post-F1 PPTX validator output (status: pass; legacy_verbose_title_count=0) |
| `deck_binding_report.{json,md}`  | Post-F1 binding resolver output (64/64 bindings)                           |
| `deck_contract_report.json`      | Post-F1 deck-contract structural validator output                          |

## What stays open for F2 — F6

- F2 stable headers: 13 `legacy_header_drift` warnings outstanding. F2 will converge column headers (e.g. `ARR (mEUR)` → `ARR Unweighted (EUR)` per contract column shape) and drop the corresponding `legacy_header_sets` entries.
- F3 Salesforce drill-through link: 1 `missing_required_link_transition` outstanding on slide 10 (pushed_deals).
- F4 template-first + brand fingerprint: not started.
- F5 render / overflow gates: not started.
- F6 golden visual baseline: not started.
