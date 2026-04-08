# Sales Director Monthly Dashboard Audit - 2026-04-08

## Header

- **Dashboard ID:** `01ZTb00000FSP9JMAX`
- **Dashboard name:** Sales Ops Quarterly KPI Dashboard
- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP9JMAX/view
- **Dashboard lastModifiedDate:** 2026-04-08T21:04:41Z
- **Audit run date:** 2026-04-08
- **Spec graded against:** `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`)
- **Audit script:** `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted by convention)
- **Tally:** 24 entries . 7 BLOCKING . 1 WRONG-DATA . 16 OK

## Table 1: Executive summary

Sorted by severity then KPI bullet. Read this table first. Fix every BLOCKING item before the deck rebuild.

| Severity | Widget | KPI bullet | Issue | Recommended fix |
|---|---|---|---|---|
| BLOCKING | Missing Quote Type | CRM data quality (completeness) | Filter on APTS_Primary_Quote_Type__c is structurally obsolete (picklist is empty in the org). Switch to Type field (Land/Expand/Renewal).; Widget is 'Missing quote type' but the field is not in the detail columns | See static rule detail |
| BLOCKING | (MISSING) fa_forecast_change_volatility | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_forecast_change_volatility with filters:  |
| BLOCKING | (MISSING) fa_quarterly_realized_vs_bestcase | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_quarterly_realized_vs_bestcase with filters:  |
| BLOCKING | (MISSING) fa_quarterly_realized_vs_commit | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_quarterly_realized_vs_commit with filters:  |
| BLOCKING | (MISSING) fa_slipped_count_quarterly | Forecast accuracy | Spec requires this widget; dashboard does not have it | Add widget fa_slipped_count_quarterly with filters:  |
| BLOCKING | (MISSING) Widget ID | KPI bullet | Spec requires this widget; dashboard does not have it | Add widget Widget ID with filters:  |
| BLOCKING | (MISSING) ph_stale_opportunities | Pipeline hygiene (aging) | Spec requires this widget; dashboard does not have it | Add widget ph_stale_opportunities with filters:  |
| WRONG-DATA | Missing Won/Loss Reason | CRM data quality (completeness) | Widget is 'Missing won/loss reason' but the field is not in the detail columns | See static rule detail |
| OK | KYC Not Completed | CRM data quality (accuracy) | Matches spec and passes static rules | n/a |
| OK | Missing Amount on Open Opps | CRM data quality (completeness) | Matches spec and passes static rules | n/a |
| OK | Won Loss Info Missing CFQ | CRM data quality (completeness) | Matches spec and passes static rules | n/a |
| OK | Low Probability In Quarter | Pipeline hygiene | Matches spec and passes static rules | n/a |
| OK | No Activity 30 Plus Days | Pipeline hygiene | Matches spec and passes static rules | n/a |
| OK | Overdue Close Date Open Opps | Pipeline hygiene | Matches spec and passes static rules | n/a |
| OK | Overdue Opportunities | Pipeline hygiene | Matches spec and passes static rules | n/a |
| OK | Under Construction: Probability Mismatch by Stage | Pipeline hygiene | Matches spec and passes static rules | n/a |
| OK | Aging Pipeline 365 Plus Days | Pipeline hygiene (aging) | Matches spec and passes static rules | n/a |
| OK | High Value Stale Deals | Pipeline hygiene (aging) | Matches spec and passes static rules | n/a |
| OK | Active Opps With No Activity Logged | Process compliance rates | Matches spec and passes static rules | n/a |
| OK | Closed Opps This Quarter Lacking Reason | Process compliance rates | Matches spec and passes static rules | n/a |
| OK | Land Deals Lacking Commercial Approval Flow | Process compliance rates | Matches spec and passes static rules | n/a |
| OK | Mid-Stage Opps Exceeding 60-Day Age | Process compliance rates | Matches spec and passes static rules | n/a |
| OK | Mid-Stage Opps Lacking NextStep | Process compliance rates | Matches spec and passes static rules | n/a |
| OK | Stale Opportunities | Process compliance rates | Matches spec and passes static rules | n/a |

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
| 1 | Missing Quote Type | Report | 01aTb00000CmjwrIAB | 00OTb000008ekynMAA | Missing Quote Type | SUMMARY | CUSTOM | 0 | dq_missing_quote_type | CRM data quality (completeness) | BLOCKING | Filter on APTS_Primary_Quote_Type__c is structurally obsolete (picklist is empty in the org). Switch to Type field (Land/Expand/Renewal).; Widget is 'Missing quote type' but the field is not in the detail columns |
| 2 | (MISSING) fa_forecast_change_volatility | chart |  |  |  |  |  |  | fa_forecast_change_volatility | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 3 | (MISSING) fa_quarterly_realized_vs_bestcase | metric |  |  |  |  |  |  | fa_quarterly_realized_vs_bestcase | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 4 | (MISSING) fa_quarterly_realized_vs_commit | metric |  |  |  |  |  |  | fa_quarterly_realized_vs_commit | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 5 | (MISSING) fa_slipped_count_quarterly | chart |  |  |  |  |  |  | fa_slipped_count_quarterly | Forecast accuracy | BLOCKING | Spec requires this widget; dashboard does not have it |
| 6 | (MISSING) Widget ID | Type |  |  |  |  |  |  | Widget ID | KPI bullet | BLOCKING | Spec requires this widget; dashboard does not have it |
| 7 | (MISSING) ph_stale_opportunities | tabular |  |  |  |  |  |  | ph_stale_opportunities | Pipeline hygiene (aging) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 8 | Missing Won/Loss Reason | Report | 01aTb00000CmjwnIAB | 00OTb000008el0PMAQ | Missing Won/Loss Reason | SUMMARY | CUSTOM | 386.78M | dq_missing_decision_reason | CRM data quality (completeness) | WRONG-DATA | Widget is 'Missing won/loss reason' but the field is not in the detail columns |
| 9 | KYC Not Completed | Report | 01aTb00000CmjwxIAB | 00OTb000007BvlJMAS | KYC Not Completed | SUMMARY | CUSTOM | 0 | dq_kyc_not_completed | CRM data quality (accuracy) | OK | Matches spec and passes static rules |
| 10 | Missing Amount on Open Opps | Report | 01aTb00000CmjwwIAB | 00OTb000008TZqcMAG | Missing Amount | TABULAR | CUSTOM | 1.1K | dq_missing_amount | CRM data quality (completeness) | OK | Matches spec and passes static rules |
| 11 | Won Loss Info Missing CFQ | Report | 01aTb00000CmjwsIAB | 00OTb000008SqblMAC | Won/Loss Info Missing CFQ | SUMMARY | THIS_QUARTER | 0 | dq_missing_won_loss_cfq | CRM data quality (completeness) | OK | Matches spec and passes static rules |
| 12 | Low Probability In Quarter | Report | 01aTb00000CmjwzIAB | 00OTb000008RfKDMA0 | Low Probability In Quarter | SUMMARY | THIS_QUARTER | 9.20M | ph_low_probability_in_quarter | Pipeline hygiene | OK | Matches spec and passes static rules |
| 13 | No Activity 30 Plus Days | Report | 01aTb00000CmjwyIAB | 00OTb000008TaEnMAK | No Activity 30+ Days - Open Opps | SUMMARY | CUSTOM | 203.21M | ph_no_activity_30_plus | Pipeline hygiene | OK | Matches spec and passes static rules |
| 14 | Overdue Close Date Open Opps | Report | 01aTb00000CmjwtIAB | 00OTb000008TaBZMA0 | Overdue Close Date | SUMMARY | CUSTOM | 106 | ph_overdue_close_date_list | Pipeline hygiene | OK | Matches spec and passes static rules |
| 15 | Overdue Opportunities | Report | 01aTb00000CmjwoIAB | 00OTb000008SrmLMAS | Overdue Opportunities | SUMMARY | CUSTOM | 998 | ph_overdue_opportunities | Pipeline hygiene | OK | Matches spec and passes static rules |
| 16 | Under Construction: Probability Mismatch by Stage | Report | 01aTb00000CmjwpIAB | 00OTb000008TaJdMAK | Probability Mismatch by Stage | SUMMARY | CUSTOM | 1638.51M | ph_probability_mismatch_by_stage | Pipeline hygiene | OK | Matches spec and passes static rules |
| 17 | Aging Pipeline 365 Plus Days | Report | 01aTb00000CmjwqIAB | 00OTb000008Ti7VMAS | Aging Pipeline 365 Plus Days | SUMMARY | CUSTOM | 267.21M | ph_aging_pipeline_365_plus | Pipeline hygiene (aging) | OK | Matches spec and passes static rules |
| 18 | High Value Stale Deals | Report | 01aTb00000Cmjx0IAB | 00OTb000008Ti97MAC | High Value Stale Deals | TABULAR | CUSTOM | 22.03M | ph_high_value_stale_deals | Pipeline hygiene (aging) | OK | Matches spec and passes static rules |
| 19 | Active Opps With No Activity Logged | Report | 01aTb00000Cn9PrIAJ | 00OTb000008fAmnMAE | P2.6 Active Opps: No Activity Ever | SUMMARY | CUSTOM | 1.1K | pc_recent_activity_logged | Process compliance rates | OK | Matches spec and passes static rules |
| 20 | Closed Opps This Quarter Lacking Reason | Report | 01aTb00000Cn9PsIAJ | 00OTb000008fAoPMAU | P2.6 Closed This Qtr: No W/L Reason | SUMMARY | THIS_QUARTER | 9 | pc_won_loss_reason_documented | Process compliance rates | OK | Matches spec and passes static rules |
| 21 | Land Deals Lacking Commercial Approval Flow | Report | 01aTb00000Cn9PqIAJ | 00OTb000008fAlBMAU | P2.6 Land: No Approval Flow | SUMMARY | CUSTOM | 20 | pc_land_commercial_approval_flow | Process compliance rates | OK | Matches spec and passes static rules |
| 22 | Mid-Stage Opps Exceeding 60-Day Age | Report | 01aTb00000Cn9PtIAJ | 00OTb000008fArdMAE | P2.6 Mid-Stage: Age Exceeded 60d | SUMMARY | CUSTOM | 17 | pc_stage_age_within_threshold | Process compliance rates | OK | Matches spec and passes static rules |
| 23 | Mid-Stage Opps Lacking NextStep | Report | 01aTb00000Cn9PpIAJ | 00OTb000008fAjZMAU | P2.6 Mid-Stage: No NextStep | SUMMARY | CUSTOM | 620 | pc_next_step_documented | Process compliance rates | OK | Matches spec and passes static rules |
| 24 | Stale Opportunities | Report | 01aTb00000CmjwvIAB | 00OTb000008TZgvMAG | Stale Opportunities | TABULAR | CUSTOM | 1.8K | pc_stage_age_within_threshold | Process compliance rates | OK | Matches spec and passes static rules |

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

