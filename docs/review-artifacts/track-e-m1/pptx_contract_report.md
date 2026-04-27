# PPTX contract report

- pptx: `<pptx-anchor>`
- deck contract: `<sanitized>/crm-analytics/config/deck_contract.yaml`
- validated_at: 2026-04-27T12:08:02.354031+00:00
- **status: pass**
- slides: 16/16
- titles: stable=0 legacy_verbose=15 mismatch=0
- blockers: 0 | warnings: 29

| # | Slide | Title | Tables (decl/actual) | Headers | Link | Slide status |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `cover` | warning_legacy | 0/0 | n/a | n/a | warning |
| 2 | `executive_summary` | warning_legacy | 0/0 | n/a | n/a | warning |
| 3 | `since_last_review` | warning_legacy | 1/1 | warning | n/a | warning |
| 4 | `q1_promised_delivered` | warning_legacy | 0/0 | n/a | n/a | warning |
| 5 | `q1_loss_drivers` | warning_legacy | 2/2 | warning | n/a | warning |
| 6 | `q2_outlook` | warning_legacy | 0/0 | n/a | n/a | warning |
| 7 | `top_open_opportunities` | warning_legacy | 1/1 | warning | n/a | warning |
| 8 | `deal_risk_triage` | warning_legacy | 1/1 | warning | n/a | warning |
| 9 | `owner_coaching` | warning_legacy | 1/1 | warning | n/a | warning |
| 10 | `pushed_deals` | warning_legacy | 0/0 | n/a | warning | warning |
| 11 | `q1_slippage` | warning_legacy | 1/1 | warning | n/a | warning |
| 12 | `forecast_accuracy` | warning_legacy | 0/0 | n/a | n/a | warning |
| 13 | `forecast_mix` | warning_legacy | 1/1 | warning | n/a | warning |
| 14 | `commercial_approvals` | warning_legacy | 4/4 | warning | n/a | warning |
| 15 | `renewals` | warning_legacy | 1/1 | warning | n/a | warning |
| 16 | `legal_notice` | pass_static | 0/0 | n/a | n/a | pass |

## Findings

- **warning** `legacy_verbose_title` — slide 1 (cover) emits legacy verbose title 'Monthly Pipeline Review'; matches legacy_title_patterns. Update builder to emit stable title 'Sales Director Monthly Pipeline Review'.
- **warning** `legacy_verbose_title` — slide 2 (executive_summary) emits legacy verbose title 'Pipeline EUR 9.6M unweighted, EUR 3.8M weighted, across 8 deals. 38% in 3 - Engagement.'; matches legacy_title_patterns. Update builder to emit stable title 'Executive Summary'.
- **warning** `legacy_verbose_title` — slide 3 (since_last_review) emits legacy verbose title 'Since last review (2026-04-23): what moved'; matches legacy_title_patterns. Update builder to emit stable title 'Since Last Review'.
- **warning** `legacy_header_drift` — slide 3 (since_last_review) table 'tbl_since_last_review' headers match a legacy_header_sets entry rather than stable headers. actual=['Metric', '2026-04-23', '2026-04-27', 'Change'] stable=['Account', 'Opportunity', 'Owner', 'Stage', 'Movement', 'Old Close', 'New Close', 'Changed', 'ARR']
- **warning** `legacy_verbose_title` — slide 4 (q1_promised_delivered) emits legacy verbose title 'Q1 pipeline opened at EUR 17.1M across 14 deals. Committed (stage 4+): EUR 2.7M in 4 deals.'; matches legacy_title_patterns. Update builder to emit stable title 'Q1 Promised vs Delivered'.
- **warning** `legacy_verbose_title` — slide 5 (q1_loss_drivers) emits legacy verbose title '14 Q1 Land losses, EUR 5.5M. 9 missing reason code.'; matches legacy_title_patterns. Update builder to emit stable title 'Q1 Loss Drivers'.
- **warning** `legacy_header_drift` — slide 5 (q1_loss_drivers) table 'tbl_q1_loss_reasons' headers match a legacy_header_sets entry rather than stable headers. actual=['Loss Reason', 'Count', 'Lost ARR'] stable=['Reason', 'Count', 'ARR']
- **warning** `legacy_header_drift` — slide 5 (q1_loss_drivers) table 'tbl_q1_loss_stage_reached' headers match a legacy_header_sets entry rather than stable headers. actual=['Stage Reached Before Loss', 'Count', 'Lost ARR'] stable=['Stage Reached', 'Count', 'ARR']
- **warning** `legacy_verbose_title` — slide 6 (q2_outlook) emits legacy verbose title 'Q2 book EUR 5.0M unweighted, EUR 3.4M weighted. 5 deals closing Apr-Jun.'; matches legacy_title_patterns. Update builder to emit stable title 'Q2 Outlook'.
- **warning** `legacy_verbose_title` — slide 7 (top_open_opportunities) emits legacy verbose title 'Key 7 open deals = EUR 9.6M. Amova Asset Management leads at EUR 2.6M; top 5 = 89% of this book.'; matches legacy_title_patterns. Update builder to emit stable title 'Top Open Opportunities'.
- **warning** `legacy_header_drift` — slide 7 (top_open_opportunities) table 'tbl_top_open_opportunities' headers match a legacy_header_sets entry rather than stable headers. actual=['Account', 'Opportunity', 'Owner', 'Stage', 'Close Date', 'Age', 'ARR (mEUR)', 'ARR Wtd (mEUR)'] stable=['Account', 'Opportunity', 'Owner', 'Stage', 'Close', 'ARR', 'Prob %']
- **warning** `legacy_verbose_title` — slide 8 (deal_risk_triage) emits legacy verbose title 'Key 4 Q2-Q3 Deals at Risk, EUR 4.9M exposed'; matches legacy_title_patterns. Update builder to emit stable title 'Deal Risk Triage'.
- **warning** `legacy_header_drift` — slide 8 (deal_risk_triage) table 'tbl_deal_risk_triage' headers match a legacy_header_sets entry rather than stable headers. actual=['#', 'Score', 'Account', 'Opportunity', 'Stage', 'Close', 'ARR (mEUR)', 'Reasons'] stable=['Opportunity', 'Owner', 'Stage', 'Forecast', 'Wtd ARR', 'Close', 'Pushes', 'Score', 'Priority']
- **warning** `legacy_verbose_title` — slide 9 (owner_coaching) emits legacy verbose title '3 owners carry 50 pushes across EUR 21.8M. Coach in this order.'; matches legacy_title_patterns. Update builder to emit stable title 'Owner Coaching Priorities'.
- **warning** `legacy_header_drift` — slide 9 (owner_coaching) table 'tbl_owner_coaching' headers match a legacy_header_sets entry rather than stable headers. actual=['Owner', 'Deals', 'Open ARR', 'Pushes', 'Top Risk Signals', 'Coaching Focus'] stable=['Owner', 'Deals', 'ARR', 'Pushes', 'Avg Push']
- **warning** `legacy_verbose_title` — slide 10 (pushed_deals) emits legacy verbose title '11 open deals pushed. Edwina Chow owns 5 of the most-pushed deals, pattern review recommended.'; matches legacy_title_patterns. Update builder to emit stable title 'Pushed Deals'.
- **warning** `missing_required_link_transition` — slide 10 (pushed_deals) missing required hyperlink (1 declared). Update builder to emit the Salesforce drill-through link.
- **warning** `legacy_verbose_title` — slide 11 (q1_slippage) emits legacy verbose title '8 deals slipped out of Q1 (EUR 9.4M). 5 pushed since.'; matches legacy_title_patterns. Update builder to emit stable title 'Q1 Slippage'.
- **warning** `legacy_header_drift` — slide 11 (q1_slippage) table 'tbl_q1_slippage' headers match a legacy_header_sets entry rather than stable headers. actual=['Account', 'Opportunity', 'Movement', 'Old Close', 'New Close', 'Changed', 'ARR (mEUR)'] stable=['Account', 'Opportunity', 'Owner', 'Stage', 'Old Close', 'New Close', 'ARR']
- **warning** `legacy_verbose_title` — slide 12 (forecast_accuracy) emits legacy verbose title '1 deal closed-won (EUR 1.9M).'; matches legacy_title_patterns. Update builder to emit stable title 'Forecast Accuracy'.
- **warning** `legacy_verbose_title` — slide 13 (forecast_mix) emits legacy verbose title 'Forecast Breakdown: EUR 5.6M across 12 open deals. Commit = EUR 2.7M (48%).'; matches legacy_title_patterns. Update builder to emit stable title 'Forecast Mix'.
- **warning** `legacy_header_drift` — slide 13 (forecast_mix) table 'tbl_forecast_mix' headers match a legacy_header_sets entry rather than stable headers. actual=['Category', 'Deals', 'ARR (mEUR)'] stable=['Category', 'Deals', 'Wtd ARR', 'Unwtd ARR']
- **warning** `legacy_verbose_title` — slide 14 (commercial_approvals) emits legacy verbose title 'Commercial Approvals: 2 approved 2026, 1 pending.'; matches legacy_title_patterns. Update builder to emit stable title 'Commercial Approvals'.
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_approved_2026' headers match a legacy_header_sets entry rather than stable headers. actual=['Region', '# Approvals', 'Avg Deal Size (mEUR ARR)*', 'Actual Deal ARR Coverage (mEUR)'] stable=['Account', 'Opportunity', 'ARR', 'Approved']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_pending' headers match a legacy_header_sets entry rather than stable headers. actual=['Region', '# Approvals', 'Avg Deal Size', 'Actual Deal ARR Coverage (mEUR)'] stable=['Account', 'Opportunity', 'Stage', 'ARR', 'Next Step']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_candidate' headers match a legacy_header_sets entry rather than stable headers. actual=['Opportunity Name', 'Deal size (mEUR)', 'Approved subject to'] stable=['Account', 'Opportunity', 'Stage', 'ARR']
- **warning** `legacy_header_drift` — slide 14 (commercial_approvals) table 'tbl_approvals_other' headers match a legacy_header_sets entry rather than stable headers. actual=['Opportunity Name', 'ARR (mEUR)'] stable=['Account', 'Opportunity', 'Status', 'ARR']
- **warning** `legacy_verbose_title` — slide 15 (renewals) emits legacy verbose title '3 renewals due FY26, EUR 33.5M ACV (1 in Q2, 2 in Q3).'; matches legacy_title_patterns. Update builder to emit stable title 'FY26 Renewals'.
- **warning** `legacy_header_drift` — slide 15 (renewals) table 'tbl_renewals' headers match a legacy_header_sets entry rather than stable headers. actual=['Close Date', 'Account', 'Opportunity', 'Owner', 'Stage', 'ACV (EUR)', 'Probability', 'Commentary'] stable=['Close', 'Account', 'Opportunity', 'Owner', 'Stage', 'ACV', 'Prob %', 'Comments']

