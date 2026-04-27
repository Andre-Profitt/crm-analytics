# PPTX contract report

- pptx: `<pptx-anchor>`
- deck contract: `<sanitized>/crm-analytics/config/deck_contract.yaml`
- validated_at: 2026-04-27T12:48:57.237096+00:00
- **status: pass**
- slides: 16/16
- titles: stable=15 legacy_verbose=0 mismatch=0
- blockers: 0 | warnings: 14

| # | Slide | Title | Tables (decl/actual) | Headers | Link | Slide status |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `cover` | pass_stable | 0/0 | n/a | n/a | pass |
| 2 | `executive_summary` | pass_stable | 0/0 | n/a | n/a | pass |
| 3 | `since_last_review` | pass_stable | 1/1 | warning | n/a | warning |
| 4 | `q1_promised_delivered` | pass_stable | 0/0 | n/a | n/a | pass |
| 5 | `q1_loss_drivers` | pass_stable | 2/2 | warning | n/a | warning |
| 6 | `q2_outlook` | pass_stable | 0/0 | n/a | n/a | pass |
| 7 | `top_open_opportunities` | pass_stable | 1/1 | warning | n/a | warning |
| 8 | `deal_risk_triage` | pass_stable | 1/1 | warning | n/a | warning |
| 9 | `owner_coaching` | pass_stable | 1/1 | warning | n/a | warning |
| 10 | `pushed_deals` | pass_stable | 0/0 | n/a | warning | warning |
| 11 | `q1_slippage` | pass_stable | 1/1 | warning | n/a | warning |
| 12 | `forecast_accuracy` | pass_stable | 0/0 | n/a | n/a | pass |
| 13 | `forecast_mix` | pass_stable | 1/1 | warning | n/a | warning |
| 14 | `commercial_approvals` | pass_stable | 4/4 | warning | n/a | warning |
| 15 | `renewals` | pass_stable | 1/1 | warning | n/a | warning |
| 16 | `legal_notice` | pass_static | 0/0 | n/a | n/a | pass |

## Findings

- **warning** `legacy_header_drift` — slide 3 (since_last_review) table 'tbl_since_last_review' headers match a legacy_header_sets entry rather than stable headers. actual=['Metric', '2026-04-23', '2026-04-27', 'Change'] stable=['Account', 'Opportunity', 'Owner', 'Stage', 'Movement', 'Old Close', 'New Close', 'Changed', 'ARR']
- **warning** `legacy_header_drift` — slide 5 (q1_loss_drivers) table 'tbl_q1_loss_reasons' headers match a legacy_header_sets entry rather than stable headers. actual=['Loss Reason', 'Count', 'Lost ARR'] stable=['Reason', 'Count', 'ARR']
- **warning** `legacy_header_drift` — slide 5 (q1_loss_drivers) table 'tbl_q1_loss_stage_reached' headers match a legacy_header_sets entry rather than stable headers. actual=['Stage Reached Before Loss', 'Count', 'Lost ARR'] stable=['Stage Reached', 'Count', 'ARR']
- **warning** `legacy_header_drift` — slide 7 (top_open_opportunities) table 'tbl_top_open_opportunities' headers match a legacy_header_sets entry rather than stable headers. actual=['Account', 'Opportunity', 'Owner', 'Stage', 'Close Date', 'Age', 'ARR (mEUR)', 'ARR Wtd (mEUR)'] stable=['Account', 'Opportunity', 'Owner', 'Stage', 'Close', 'ARR', 'Prob %']
- **warning** `legacy_header_drift` — slide 8 (deal_risk_triage) table 'tbl_deal_risk_triage' headers match a legacy_header_sets entry rather than stable headers. actual=['#', 'Score', 'Account', 'Opportunity', 'Stage', 'Close', 'ARR (mEUR)', 'Reasons'] stable=['Opportunity', 'Owner', 'Stage', 'Forecast', 'Wtd ARR', 'Close', 'Pushes', 'Score', 'Priority']
- **warning** `legacy_header_drift` — slide 9 (owner_coaching) table 'tbl_owner_coaching' headers match a legacy_header_sets entry rather than stable headers. actual=['Owner', 'Deals', 'Open ARR', 'Pushes', 'Top Risk Signals', 'Coaching Focus'] stable=['Owner', 'Deals', 'ARR', 'Pushes', 'Avg Push']
- **warning** `missing_required_link_transition` — slide 10 (pushed_deals) missing required hyperlink (1 declared). Update builder to emit the Salesforce drill-through link.
- **warning** `legacy_header_drift` — slide 11 (q1_slippage) table 'tbl_q1_slippage' headers match a legacy_header_sets entry rather than stable headers. actual=['Account', 'Opportunity', 'Movement', 'Old Close', 'New Close', 'Changed', 'ARR (mEUR)'] stable=['Account', 'Opportunity', 'Owner', 'Stage', 'Old Close', 'New Close', 'ARR']
- **warning** `legacy_header_drift` — slide 13 (forecast_mix) table 'tbl_forecast_mix' headers match a legacy_header_sets entry rather than stable headers. actual=['Category', 'Deals', 'ARR (mEUR)'] stable=['Category', 'Deals', 'Wtd ARR', 'Unwtd ARR']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_approved_2026' headers match a legacy_header_sets entry rather than stable headers. actual=['Region', '# Approvals', 'Avg Deal Size (mEUR ARR)*', 'Actual Deal ARR Coverage (mEUR)'] stable=['Account', 'Opportunity', 'ARR', 'Approved']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_pending' headers match a legacy_header_sets entry rather than stable headers. actual=['Region', '# Approvals', 'Avg Deal Size', 'Actual Deal ARR Coverage (mEUR)'] stable=['Account', 'Opportunity', 'Stage', 'ARR', 'Next Step']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_candidate' headers match a legacy_header_sets entry rather than stable headers. actual=['Opportunity Name', 'Deal size (mEUR)', 'Approved subject to'] stable=['Account', 'Opportunity', 'Stage', 'ARR']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_other' headers match a legacy_header_sets entry rather than stable headers. actual=['Opportunity Name', 'ARR (mEUR)'] stable=['Account', 'Opportunity', 'Status', 'ARR']
- **warning** `legacy_header_drift` — slide 15 (renewals) table 'tbl_renewals' headers match a legacy_header_sets entry rather than stable headers. actual=['Close Date', 'Account', 'Opportunity', 'Owner', 'Stage', 'ACV (EUR)', 'Probability', 'Commentary'] stable=['Close', 'Account', 'Opportunity', 'Owner', 'Stage', 'ACV', 'Prob %', 'Comments']

