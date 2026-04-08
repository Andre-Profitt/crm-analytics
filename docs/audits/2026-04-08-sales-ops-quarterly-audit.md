# Sales Director Monthly Dashboard Audit - 2026-04-08

## Header

- **Dashboard ID:** `01ZTb00000FSP9JMAX`
- **Dashboard name:** Sales Ops Quarterly KPI Dashboard
- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP9JMAX/view
- **Dashboard lastModifiedDate:** 2026-04-08T16:09:36Z
- **Audit run date:** 2026-04-08
- **Spec graded against:** `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`)
- **Audit script:** `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted by convention)
- **Tally:** 40 entries . 23 BLOCKING . 9 WRONG-DATA . 7 ORPHAN . 1 OK

## Table 1: Executive summary

Sorted by severity then KPI bullet. Read this table first. Fix every BLOCKING item before the deck rebuild.

| Severity | Widget | KPI bullet | Issue | Recommended fix |
|---|---|---|---|---|
| BLOCKING | Missing Quote Type | (orphan) | (also orphan: widget does not map to any spec entry). Filter on APTS_Primary_Quote_Type__c is structurally obsolete (picklist is empty in the org). Switch to Type field (Land/Expand/Renewal).; Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Widget is 'Missing quote type' but the field is not in the detail columns | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| BLOCKING | (MISSING) dq_kyc_not_completed | CRM data quality (accuracy) | Spec requires this widget; dashboard does not have it | Add widget dq_kyc_not_completed with filters:  |
| BLOCKING | (MISSING) dq_missing_amount | CRM data quality (completeness) | Spec requires this widget; dashboard does not have it | Add widget dq_missing_amount with filters:  |
| BLOCKING | (MISSING) dq_missing_decision_reason | CRM data quality (completeness) | Spec requires this widget; dashboard does not have it | Add widget dq_missing_decision_reason with filters:  |
| BLOCKING | (MISSING) dq_missing_quote_type | CRM data quality (completeness) | Spec requires this widget; dashboard does not have it | Add widget dq_missing_quote_type with filters:  |
| BLOCKING | (MISSING) dq_missing_won_loss_cfq | CRM data quality (completeness) | Spec requires this widget; dashboard does not have it | Add widget dq_missing_won_loss_cfq with filters:  |
| BLOCKING | (MISSING) fa_forecast_change_volatility | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_forecast_change_volatility with filters:  |
| BLOCKING | (MISSING) fa_quarterly_realized_vs_bestcase | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_quarterly_realized_vs_bestcase with filters:  |
| BLOCKING | (MISSING) fa_quarterly_realized_vs_commit | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_quarterly_realized_vs_commit with filters:  |
| BLOCKING | (MISSING) fa_slipped_count_quarterly | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_slipped_count_quarterly with filters:  |
| BLOCKING | (MISSING) Widget ID | KPI bullet | Spec requires this widget; dashboard does not have it | Add widget Widget ID with filters:  |
| BLOCKING | (MISSING) ph_low_probability_in_quarter | Pipeline hygiene | Spec requires this widget; dashboard does not have it | Add widget ph_low_probability_in_quarter with filters:  |
| BLOCKING | (MISSING) ph_no_activity_30_plus | Pipeline hygiene | Spec requires this widget; dashboard does not have it | Add widget ph_no_activity_30_plus with filters:  |
| BLOCKING | (MISSING) ph_overdue_close_date_list | Pipeline hygiene | Spec requires this widget; dashboard does not have it | Add widget ph_overdue_close_date_list with filters:  |
| BLOCKING | (MISSING) ph_overdue_opportunities | Pipeline hygiene | Spec requires this widget; dashboard does not have it | Add widget ph_overdue_opportunities with filters:  |
| BLOCKING | (MISSING) ph_probability_mismatch_by_stage | Pipeline hygiene | Spec requires this widget; dashboard does not have it | Add widget ph_probability_mismatch_by_stage with filters:  |
| BLOCKING | (MISSING) ph_high_value_stale_deals | Pipeline hygiene (aging) | Spec requires this widget; dashboard does not have it | Add widget ph_high_value_stale_deals with filters:  |
| BLOCKING | (MISSING) ph_stale_opportunities | Pipeline hygiene (aging) | Spec requires this widget; dashboard does not have it | Add widget ph_stale_opportunities with filters:  |
| BLOCKING | (MISSING) pc_land_commercial_approval_flow | Process compliance rates | Spec requires this widget; dashboard does not have it | Add widget pc_land_commercial_approval_flow with filters:  |
| BLOCKING | (MISSING) pc_next_step_documented | Process compliance rates | Spec requires this widget; dashboard does not have it | Add widget pc_next_step_documented with filters:  |
| BLOCKING | (MISSING) pc_recent_activity_logged | Process compliance rates | Spec requires this widget; dashboard does not have it | Add widget pc_recent_activity_logged with filters:  |
| BLOCKING | (MISSING) pc_stage_age_within_threshold | Process compliance rates | Spec requires this widget; dashboard does not have it | Add widget pc_stage_age_within_threshold with filters:  |
| BLOCKING | (MISSING) pc_won_loss_reason_documented | Process compliance rates | Spec requires this widget; dashboard does not have it | Add widget pc_won_loss_reason_documented with filters:  |
| WRONG-DATA | Active Opps With No Activity Logged | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Land Deals Lacking Commercial Approval Flow | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Low Probability In Quarter | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Mid-Stage Opps Lacking NextStep | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Missing Amount on Open Opps | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Missing Decision Reason | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Widget is 'Missing decision reason' but the field is not in the detail columns | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Stale Opportunities | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Under Construction: Probability Mismatch by Stage | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Won Loss Info Missing CFQ | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| ORPHAN | Closed Opps This Quarter Lacking Reason | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| ORPHAN | High Value Stale Deals | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| ORPHAN | KYC Not Completed | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| ORPHAN | Mid-Stage Opps Exceeding 60-Day Age | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| ORPHAN | No Activity 30 Plus Days | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| ORPHAN | Overdue Close Date Open Opps | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| ORPHAN | Overdue Opportunities | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| OK | Aging Pipeline 365 Plus Days | Pipeline hygiene (aging) | Matches spec and passes static rules | n/a |

### Severity meaning

- **BLOCKING** - must be fixed before deck rebuild. Wrong field, stale picklist, no data, or required-by-spec widget is missing entirely.
- **WRONG-DATA** - must be triaged before this is shown to Sales Directors. Filters partially right but value is suspect.
- **ORPHAN** - widget exists on the dashboard but maps to no spec entry. Decision needed: keep, drop, or fold into spec.
- **COSMETIC** - can ship as a follow-up. Label or column-order issue, em-dash, etc.
- **OK** - matches spec and passes all static rules.

## Table 2: Full appendix

Every entry, all metadata columns. Greppable for any specific widget.

| # | Widget | Type | Component | Report ID | Report name | Format | Date filter | Current value | Matched spec ID | KPI bullet | Severity | Issue |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Missing Quote Type | Report | 01aTb00000CmjwrIAB | 00OTb000008ekynMAA | Missing Quote Type | SUMMARY | THIS_FISCAL_YEAR | 0 |  | (orphan) | BLOCKING | (also orphan: widget does not map to any spec entry). Filter on APTS_Primary_Quote_Type__c is structurally obsolete (picklist is empty in the org). Switch to Type field (Land/Expand/Renewal).; Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Widget is 'Missing quote type' but the field is not in the detail columns |
| 2 | (MISSING) dq_kyc_not_completed | summary |  |  |  |  |  |  | dq_kyc_not_completed | CRM data quality (accuracy) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 3 | (MISSING) dq_missing_amount | tabular |  |  |  |  |  |  | dq_missing_amount | CRM data quality (completeness) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 4 | (MISSING) dq_missing_decision_reason | summary |  |  |  |  |  |  | dq_missing_decision_reason | CRM data quality (completeness) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 5 | (MISSING) dq_missing_quote_type | summary |  |  |  |  |  |  | dq_missing_quote_type | CRM data quality (completeness) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 6 | (MISSING) dq_missing_won_loss_cfq | summary |  |  |  |  |  |  | dq_missing_won_loss_cfq | CRM data quality (completeness) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 7 | (MISSING) fa_forecast_change_volatility | chart |  |  |  |  |  |  | fa_forecast_change_volatility | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 8 | (MISSING) fa_quarterly_realized_vs_bestcase | metric |  |  |  |  |  |  | fa_quarterly_realized_vs_bestcase | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 9 | (MISSING) fa_quarterly_realized_vs_commit | metric |  |  |  |  |  |  | fa_quarterly_realized_vs_commit | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 10 | (MISSING) fa_slipped_count_quarterly | chart |  |  |  |  |  |  | fa_slipped_count_quarterly | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 11 | (MISSING) Widget ID | Type |  |  |  |  |  |  | Widget ID | KPI bullet | BLOCKING | Spec requires this widget; dashboard does not have it |
| 12 | (MISSING) ph_low_probability_in_quarter | summary |  |  |  |  |  |  | ph_low_probability_in_quarter | Pipeline hygiene | BLOCKING | Spec requires this widget; dashboard does not have it |
| 13 | (MISSING) ph_no_activity_30_plus | summary |  |  |  |  |  |  | ph_no_activity_30_plus | Pipeline hygiene | BLOCKING | Spec requires this widget; dashboard does not have it |
| 14 | (MISSING) ph_overdue_close_date_list | tabular |  |  |  |  |  |  | ph_overdue_close_date_list | Pipeline hygiene | BLOCKING | Spec requires this widget; dashboard does not have it |
| 15 | (MISSING) ph_overdue_opportunities | summary |  |  |  |  |  |  | ph_overdue_opportunities | Pipeline hygiene | BLOCKING | Spec requires this widget; dashboard does not have it |
| 16 | (MISSING) ph_probability_mismatch_by_stage | summary |  |  |  |  |  |  | ph_probability_mismatch_by_stage | Pipeline hygiene | BLOCKING | Spec requires this widget; dashboard does not have it |
| 17 | (MISSING) ph_high_value_stale_deals | tabular |  |  |  |  |  |  | ph_high_value_stale_deals | Pipeline hygiene (aging) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 18 | (MISSING) ph_stale_opportunities | tabular |  |  |  |  |  |  | ph_stale_opportunities | Pipeline hygiene (aging) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 19 | (MISSING) pc_land_commercial_approval_flow | summary |  |  |  |  |  |  | pc_land_commercial_approval_flow | Process compliance rates | BLOCKING | Spec requires this widget; dashboard does not have it |
| 20 | (MISSING) pc_next_step_documented | summary |  |  |  |  |  |  | pc_next_step_documented | Process compliance rates | BLOCKING | Spec requires this widget; dashboard does not have it |
| 21 | (MISSING) pc_recent_activity_logged | summary |  |  |  |  |  |  | pc_recent_activity_logged | Process compliance rates | BLOCKING | Spec requires this widget; dashboard does not have it |
| 22 | (MISSING) pc_stage_age_within_threshold | summary |  |  |  |  |  |  | pc_stage_age_within_threshold | Process compliance rates | BLOCKING | Spec requires this widget; dashboard does not have it |
| 23 | (MISSING) pc_won_loss_reason_documented | summary |  |  |  |  |  |  | pc_won_loss_reason_documented | Process compliance rates | BLOCKING | Spec requires this widget; dashboard does not have it |
| 24 | Active Opps With No Activity Logged | Report | 01aTb00000Cn9PrIAJ | 00OTb000008fAmnMAE | P2.6 Active Opps: No Activity Ever | SUMMARY | THIS_FISCAL_QUARTER | 276 |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 25 | Land Deals Lacking Commercial Approval Flow | Report | 01aTb00000Cn9PqIAJ | 00OTb000008fAlBMAU | P2.6 Land: No Approval Flow | SUMMARY | THIS_FISCAL_QUARTER | 5 |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 26 | Low Probability In Quarter | Report | 01aTb00000CmjwzIAB | 00OTb000008RfKDMA0 | Low Probability In Quarter | SUMMARY | THIS_FISCAL_QUARTER | 9.17M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 27 | Mid-Stage Opps Lacking NextStep | Report | 01aTb00000Cn9PpIAJ | 00OTb000008fAjZMAU | P2.6 Mid-Stage: No NextStep | SUMMARY | THIS_FISCAL_QUARTER | 191 |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 28 | Missing Amount on Open Opps | Report | 01aTb00000CmjwwIAB | 00OTb000008TZqcMAG | Missing Amount | TABULAR | THIS_FISCAL_YEAR | 770 |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 29 | Missing Decision Reason | Report | 01aTb00000CmjwnIAB | 00OTb000008el0PMAQ | Missing Decision Reason | SUMMARY | THIS_FISCAL_YEAR | 28.79M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Widget is 'Missing decision reason' but the field is not in the detail columns |
| 30 | Stale Opportunities | Report | 01aTb00000CmjwvIAB | 00OTb000008TZgvMAG | Stale Opportunities | TABULAR | THIS_FISCAL_YEAR | 1.3K |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 31 | Under Construction: Probability Mismatch by Stage | Report | 01aTb00000CmjwpIAB | 00OTb000008TaJdMAK | Probability Mismatch by Stage | SUMMARY | THIS_FISCAL_YEAR | 1039.43M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 32 | Won Loss Info Missing CFQ | Report | 01aTb00000CmjwsIAB | 00OTb000008SqblMAC | Won/Loss Info Missing CFQ | SUMMARY | THIS_FISCAL_QUARTER | 3.45M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 33 | Closed Opps This Quarter Lacking Reason | Report | 01aTb00000Cn9PsIAJ | 00OTb000008fAoPMAU | P2.6 Closed This Qtr: No W/L Reason | SUMMARY | THIS_QUARTER | 9 |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 34 | High Value Stale Deals | Report | 01aTb00000Cmjx0IAB | 00OTb000008Ti97MAC | High Value Stale Deals | TABULAR | CUSTOM | 22.03M |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 35 | KYC Not Completed | Report | 01aTb00000CmjwxIAB | 00OTb000007BvlJMAS | KYC Not Completed | SUMMARY | CUSTOM | 0 |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 36 | Mid-Stage Opps Exceeding 60-Day Age | Report | 01aTb00000Cn9PtIAJ | 00OTb000008fArdMAE | P2.6 Mid-Stage: Age Exceeded 60d | SUMMARY | CUSTOM | 17 |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 37 | No Activity 30 Plus Days | Report | 01aTb00000CmjwyIAB | 00OTb000008TaEnMAK | No Activity 30+ Days — Open Opps | SUMMARY | THIS_YEAR | 146.45M |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 38 | Overdue Close Date Open Opps | Report | 01aTb00000CmjwtIAB | 00OTb000008TaBZMA0 | Overdue Close Date | TABULAR | CUSTOM | 107 |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 39 | Overdue Opportunities | Report | 01aTb00000CmjwoIAB | 00OTb000008SrmLMAS | Overdue Opportunities | SUMMARY | CUSTOM | 1.0K |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 40 | Aging Pipeline 365 Plus Days | Report | 01aTb00000CmjwqIAB | 00OTb000008Ti7VMAS | Aging Pipeline 365 Plus Days | SUMMARY | CUSTOM | 267.01M | ph_aging_pipeline_365_plus | Pipeline hygiene (aging) | OK | Matches spec and passes static rules |

## Spec gaps surfaced during audit

Any entries tagged ORPHAN (dashboard widgets with no spec entry) need a keep/drop decision. Any entries with `(MISSING) widget_id` in the widget column are spec entries that the dashboard does not implement.

## Phase 2 / 3 / 4 implications

- **Phase 2:** run this audit against `01ZTb00000FSP9JMAX` (Sales Ops Quarterly KPI Dashboard) by updating the DASHBOARD_ID and SPEC_PATH constants at the top of the script. Requires a Report 2 spec to exist first.
- **Phase 3:** for every OK or WRONG-DATA entry in this audit, the current_value should match the corresponding CRMA dashboard step value. Build a cross-check script that runs both and diffs them.
- **Phase 4:** deck rebuild uses Option D. Every slide chart should pull from the CRMA step that matches a NON-BLOCKING widget in this audit. Do not embed values from widgets this audit flagged as BLOCKING.

## Reproducibility

```bash
cd ~/crm-analytics
python3 scripts/audit_sales_director_monthly_dashboard.py
```

Spec commit graded against: `25cc03d`

