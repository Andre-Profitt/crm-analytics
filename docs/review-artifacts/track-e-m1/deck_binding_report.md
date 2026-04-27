# Deck binding report — director_monthly

- deck contract: `<sanitized>/crm-analytics/config/deck_contract.yaml`
- workbook contract: `<sanitized>/crm-analytics/config/director_workbook_contract.yaml`
- workbook: `<workbook-anchor>`
- resolved_at: 2026-04-27T02:25:02.984272+00:00
- **status: pass**
- bindings: 75 (pass=75 warn=0 fail=0)

## Slide 1: `cover`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `analysis_scope` | source_note | pass |  |

## Slide 2: `executive_summary`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `executive_summary_takeaway` | generated_takeaway | pass | max_chars=220 metrics=['open_arr_unweighted', 'open_arr_weighted', 'open_deal_count'] |

## Slide 3: `since_last_review`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `movement_window` | source_note | pass |  |
| source_note | `snapshot_date` | source_note | pass |  |
| takeaway | `since_last_review_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['net_movement_eur', 'movement_event_count'] |
| table | `tbl_since_last_review` | direct_workbook_table | pass | sheet=Q1 Movement cols=9 |

## Slide 4: `q1_promised_delivered`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `q1_promised_delivered_takeaway` | generated_takeaway | pass | max_chars=200 metrics=['q1_opening_arr', 'q1_opening_deal_count', 'q1_committed_arr'] |

## Slide 5: `q1_forecast_variance`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `period` | source_note | pass |  |
| takeaway | `q1_forecast_variance_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['q1_variance_arr', 'q1_net_change', 'q1_variance_direction'] |
| table | `tbl_q1_forecast_variance_bridge` | derived_table | pass | transform=q1_forecast_variance_bridge rows=8 roles={'opening_arr': 'ARR 2026-01-01', 'latest_arr': 'ARR 2026-04-12', 'change_at_close': 'ARR Change 2026-04-12'} |
| table | `tbl_q1_forecast_variance_evidence` | direct_workbook_table | pass | sheet=Q1 Snapshot Trend cols=5 |

## Slide 6: `q1_loss_drivers`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `period` | source_note | pass |  |
| takeaway | `q1_loss_drivers_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['q1_loss_count', 'q1_loss_arr', 'q1_losses_missing_reason'] |
| table | `tbl_q1_loss_reasons` | direct_workbook_table | pass | sheet=Won Lost FY26 cols=3 |
| table | `tbl_q1_loss_stage_reached` | direct_workbook_table | pass | sheet=Won Lost FY26 cols=3 |

## Slide 7: `q2_outlook`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `period` | source_note | pass |  |
| takeaway | `q2_outlook_takeaway` | generated_takeaway | pass | max_chars=200 metrics=['q2_open_arr_unweighted', 'q2_open_arr_weighted', 'q2_open_deal_count'] |

## Slide 8: `q2_deal_readiness`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `period` | source_note | pass |  |
| takeaway | `q2_deal_readiness_takeaway` | generated_takeaway | pass | max_chars=180 metrics=['q2_open_deal_count', 'q2_open_arr_unweighted', 'q2_readiness_summary'] |
| table | `tbl_q2_deal_readiness` | direct_workbook_table | pass | sheet=Pipeline Open FY26 cols=8 |

## Slide 9: `top_open_opportunities`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `top_open_opportunities_takeaway` | generated_takeaway | pass | max_chars=180 metrics=['top_n', 'top_n_arr', 'top_account', 'top_account_arr', 'top_5_share_pct'] |
| table | `tbl_top_open_opportunities` | direct_workbook_table | pass | sheet=Pipeline Open FY26 cols=7 |

## Slide 10: `deal_risk_triage`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `deal_risk_triage_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['risk_top_n', 'risk_arr'] |
| table | `tbl_deal_risk_triage` | direct_workbook_table | pass | sheet=Pipeline Inspection cols=9 |

## Slide 11: `owner_coaching`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `owner_coaching_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['coach_owner_count', 'coach_total_pushes', 'coach_total_arr'] |
| table | `tbl_owner_coaching` | direct_workbook_table | pass | sheet=Pipeline Open FY26 cols=5 |

## Slide 12: `pushed_deals`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `salesforce_drill_link` | source_note | pass |  |
| takeaway | `pushed_deals_takeaway` | generated_takeaway | pass | max_chars=180 metrics=['pushed_deal_count', 'pushed_top_owner', 'pushed_top_owner_count'] |
| link | `salesforce_pushed_deals` | external_link | pass | kind=salesforce_list_view |

## Slide 13: `q1_slippage`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `q1_slippage_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['slipped_deal_count', 'slipped_arr', 'pushed_since_count'] |
| table | `tbl_q1_slippage` | direct_workbook_table | pass | sheet=Q1 Movement cols=7 |

## Slide 14: `forecast_accuracy`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| source_note | `period` | source_note | pass |  |
| takeaway | `forecast_accuracy_takeaway` | generated_takeaway | pass | max_chars=150 metrics=['closed_won_count', 'closed_won_unit', 'closed_won_arr'] |

## Slide 15: `forecast_mix`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `forecast_mix_takeaway` | generated_takeaway | pass | max_chars=180 metrics=['forecast_mix_total_arr', 'forecast_mix_deal_count', 'forecast_commit_arr', 'forecast_commit_pct'] |
| table | `tbl_forecast_mix` | direct_workbook_table | pass | sheet=Commit Items cols=4 |

## Slide 16: `commercial_approvals`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `commercial_approvals_takeaway` | generated_takeaway | pass | max_chars=180 metrics=['approved_2026_count', 'pending_approval_count', 'candidate_count'] |
| table | `tbl_approvals_approved_2026` | direct_workbook_table | pass | sheet=Commercial Approval cols=4 |
| table | `tbl_approvals_pending` | direct_workbook_table | pass | sheet=Commercial Approval cols=5 |
| table | `tbl_approvals_candidate` | direct_workbook_table | pass | sheet=Commercial Approval cols=4 |
| table | `tbl_approvals_other` | direct_workbook_table | pass | sheet=Commercial Approval cols=4 |

## Slide 17: `renewals`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| source_note | `snapshot_date` | source_note | pass |  |
| source_note | `territory` | source_note | pass |  |
| takeaway | `renewals_takeaway` | generated_takeaway | pass | max_chars=180 metrics=['renewal_count', 'renewal_acv', 'renewal_q2_count', 'renewal_q3_count'] |
| table | `tbl_renewals` | direct_workbook_table | pass | sheet=Renewals FY26 cols=8 |

## Slide 18: `legal_notice`

| Kind | ID | Binding type | Status | Detail |
| --- | --- | --- | --- | --- |
| static | `legal_notice_static` | legal_text | pass |  |

