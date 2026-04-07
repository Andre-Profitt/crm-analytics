# Sales Director Monthly Dashboard Audit - 2026-04-07

## Header

- **Dashboard ID:** `01ZTb00000FSP7hMAH`
- **Dashboard name:** Sales Directors Monthly Pipeline and Insights
- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view
- **Dashboard lastModifiedDate:** 2026-04-06T19:41:03Z
- **Audit run date:** 2026-04-07
- **Spec graded against:** `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`)
- **Audit script:** `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted by convention)
- **Tally:** 26 entries . 13 BLOCKING . 10 WRONG-DATA . 1 ORPHAN . 2 OK

## Table 1: Executive summary

Sorted by severity then KPI bullet. Read this table first. Fix every BLOCKING item before the deck rebuild.

| Severity | Widget | KPI bullet | Issue | Recommended fix |
|---|---|---|---|---|
| BLOCKING | (MISSING) land_stage3_no_approval_apac | Commercial Approval - Land Stage 3 missing approval | Spec requires this widget; dashboard does not have it | Add widget land_stage3_no_approval_apac with filters: `Type='Land'` AND `StageName='3 - Engagement'` AND `Stage_20_Approval__c=false` AND `Sales_Region__c IN ('APAC','Middle East & Africa')` |
| BLOCKING | (MISSING) land_stage3_no_approval_nam | Commercial Approval - Land Stage 3 missing approval | Spec requires this widget; dashboard does not have it | Add widget land_stage3_no_approval_nam with filters: `Type='Land'` AND `StageName='3 - Engagement'` AND `Stage_20_Approval__c=false` AND `Sales_Region__c='North America'` |
| BLOCKING | (MISSING) commercial_approval_approved_ytd | Commercial Approval overview (global, YTD approved) | Spec requires this widget; dashboard does not have it | Add widget commercial_approval_approved_ytd with filters: `Type='Land'` AND `Stage_20_Approval__c=true` AND `Stage_20_Approval_Date__c >= start of THIS_CALENDAR_YEAR` |
| BLOCKING | (MISSING) commercial_approval_global | Commercial Approval overview (global, current state) | Spec requires this widget; dashboard does not have it | Add widget commercial_approval_global with filters: `IsClosed=false` AND `StageName!='0 - Lost'` |
| BLOCKING | (MISSING) pipeline_overview_apac | Pipeline overview with quarterly focus | Spec requires this widget; dashboard does not have it | Add widget pipeline_overview_apac with filters: `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `Sales_Region__c IN ('APAC','Middle East & Africa')` |
| BLOCKING | (MISSING) pipeline_overview_emea | Pipeline overview with quarterly focus | Spec requires this widget; dashboard does not have it | Add widget pipeline_overview_emea with filters: `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `Sales_Region__c IN ('United Kingdom & Ireland','Central Europe','Northern Europe','Southwestern Europe')` |
| BLOCKING | (MISSING) pipeline_overview_nam | Pipeline overview with quarterly focus | Spec requires this widget; dashboard does not have it | Add widget pipeline_overview_nam with filters: `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `Sales_Region__c='North America'` |
| BLOCKING | Renewal Pipeline This Quarter | Pipeline overview with quarterly focus | Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Renewal widget aggregates on standard AMOUNT (`AMOUNT`). Brief requires APTS_Renewal_ACV__c for renewal ACV. | See static rule detail |
| BLOCKING | (MISSING) renewal_likelihood | Renewals tracking | Spec requires this widget; dashboard does not have it | Add widget renewal_likelihood with filters: `Type='Renewal'` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `IsClosed=false` |
| BLOCKING | (MISSING) renewal_upcoming_list | Renewals tracking | Spec requires this widget; dashboard does not have it | Add widget renewal_upcoming_list with filters: `Type='Renewal'` AND `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` |
| BLOCKING | Renewal ACV by Quarter | Renewals tracking | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Renewal widget aggregates on standard AMOUNT (`AMOUNT`). Brief requires APTS_Renewal_ACV__c for renewal ACV. | See static rule detail |
| BLOCKING | Renewals by Fiscal Quarter | Renewals tracking | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Renewal widget aggregates on standard AMOUNT (`AMOUNT`). Brief requires APTS_Renewal_ACV__c for renewal ACV.; Widget title contains the word 'fiscal': 'Renewals by Fiscal Quarter'. The brief requires calendar-year framing. | See static rule detail |
| BLOCKING | (MISSING) slipped_deals_trend | Slipped deals analysis | Spec requires this widget; dashboard does not have it | Add widget slipped_deals_trend with filters: Source: **Pipeline Inspection native** (canonical). SF report fallback: `LastCloseDateChangedHistoryId != null` |
| WRONG-DATA | New Customers Won CFY | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Stage Duration CFY | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Top Accounts by ARR CFY | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Win Rate by Quarter | (orphan) | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | Fix the static rule issue AND decide keep/drop/fold. See static rule detail. |
| WRONG-DATA | Business At Risk | Churn Risk and trends | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | See static rule detail |
| WRONG-DATA | New Customers (Land) by Region | Commercial Approval - Land Stage 3 missing approval | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | See static rule detail |
| WRONG-DATA | Forecast Accuracy | Forecast accuracy (handbook-derived) | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | See static rule detail |
| WRONG-DATA | Forecast and Closed Won | Forecast accuracy (handbook-derived) | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | See static rule detail |
| WRONG-DATA | Pipeline Coverage by Stage | Pipeline overview with quarterly focus | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | See static rule detail |
| WRONG-DATA | Close Date Slipped by Stage | Slipped deals analysis | Report standard date filter uses LAST_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. | See static rule detail |
| ORPHAN | Win Rate Rolling 90d | (orphan) | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). | Decision needed: keep (add to spec), drop, or fold into an existing spec row. |
| OK | Commercial Approval Candidates by Stage | Commercial Approval - Land Stage 3 missing approval | Matches spec and passes static rules | n/a |
| OK | Land Stage 3 Missing Approval by Region | Commercial Approval - Land Stage 3 missing approval | Matches spec and passes static rules | n/a |

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
| 1 | (MISSING) land_stage3_no_approval_apac | table |  |  |  |  |  |  | land_stage3_no_approval_apac | Commercial Approval - Land Stage 3 missing approval | BLOCKING | Spec requires this widget; dashboard does not have it |
| 2 | (MISSING) land_stage3_no_approval_nam | table |  |  |  |  |  |  | land_stage3_no_approval_nam | Commercial Approval - Land Stage 3 missing approval | BLOCKING | Spec requires this widget; dashboard does not have it |
| 3 | (MISSING) commercial_approval_approved_ytd | metric+list |  |  |  |  |  |  | commercial_approval_approved_ytd | Commercial Approval overview (global, YTD approved) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 4 | (MISSING) commercial_approval_global | metric |  |  |  |  |  |  | commercial_approval_global | Commercial Approval overview (global, current state) | BLOCKING | Spec requires this widget; dashboard does not have it |
| 5 | (MISSING) pipeline_overview_apac | chart |  |  |  |  |  |  | pipeline_overview_apac | Pipeline overview with quarterly focus | BLOCKING | Spec requires this widget; dashboard does not have it |
| 6 | (MISSING) pipeline_overview_emea | chart |  |  |  |  |  |  | pipeline_overview_emea | Pipeline overview with quarterly focus | BLOCKING | Spec requires this widget; dashboard does not have it |
| 7 | (MISSING) pipeline_overview_nam | chart |  |  |  |  |  |  | pipeline_overview_nam | Pipeline overview with quarterly focus | BLOCKING | Spec requires this widget; dashboard does not have it |
| 8 | Renewal Pipeline This Quarter | Report | 01aTb00000CmjvDIAR | 00OTb000008ektxMAA | Renewal Pipeline This Quarter | SUMMARY | THIS_FISCAL_QUARTER | 10.24M | pipeline_overview_global | Pipeline overview with quarterly focus | BLOCKING | Report standard date filter uses THIS_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Renewal widget aggregates on standard AMOUNT (`AMOUNT`). Brief requires APTS_Renewal_ACV__c for renewal ACV. |
| 9 | (MISSING) renewal_likelihood | chart |  |  |  |  |  |  | renewal_likelihood | Renewals tracking | BLOCKING | Spec requires this widget; dashboard does not have it |
| 10 | (MISSING) renewal_upcoming_list | table |  |  |  |  |  |  | renewal_upcoming_list | Renewals tracking | BLOCKING | Spec requires this widget; dashboard does not have it |
| 11 | Renewal ACV by Quarter | Report | 01aTb00000CmjvJIAR | 00OTb000008ekxBMAQ | Renewal ACV by Quarter | SUMMARY | THIS_FISCAL_YEAR | 63.12M | renewal_acv_this_quarter | Renewals tracking | BLOCKING | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Renewal widget aggregates on standard AMOUNT (`AMOUNT`). Brief requires APTS_Renewal_ACV__c for renewal ACV. |
| 12 | Renewals by Fiscal Quarter | Report | 01aTb00000CmjvOIAR | 00OTb000008eksLMAQ | Renewals by Fiscal Quarter | SUMMARY | THIS_FISCAL_YEAR | 84.78M | renewal_acv_this_quarter | Renewals tracking | BLOCKING | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.; Renewal widget aggregates on standard AMOUNT (`AMOUNT`). Brief requires APTS_Renewal_ACV__c for renewal ACV.; Widget title contains the word 'fiscal': 'Renewals by Fiscal Quarter'. The brief requires calendar-year framing. |
| 13 | (MISSING) slipped_deals_trend | chart |  |  |  |  |  |  | slipped_deals_trend | Slipped deals analysis | BLOCKING | Spec requires this widget; dashboard does not have it |
| 14 | New Customers Won CFY | Report | 01aTb00000CmjvBIAR | 00OTb000008RfFNMA0 | New Customers Won CFY | SUMMARY | THIS_FISCAL_YEAR | 1.92M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 15 | Stage Duration CFY | Report | 01aTb00000CmjvLIAR | 00OTb000006ScDNMA0 | All open+won opps. FY26 | SUMMARY | THIS_FISCAL_YEAR | 87.21M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 16 | Top Accounts by ARR CFY | Report | 01aTb00000CmjvEIAR | 00OTb000008el21MAA | Top Accounts by ARR CFY | SUMMARY | THIS_FISCAL_YEAR | 326.70M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 17 | Win Rate by Quarter | Report | 01aTb00000CmjvCIAR | 00OTb000008TZdhMAG | Win Rate by Quarter | MATRIX | THIS_FISCAL_YEAR | 100.70M |  | (orphan) | WRONG-DATA | (also orphan: widget does not map to any spec entry). Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 18 | Business At Risk | Report | 01aTb00000CmjvPIAR | 00OTb000008Ta9xMAC | Business At Risk | SUMMARY | THIS_FISCAL_YEAR | 486.51M | churn_risk_placeholder | Churn Risk and trends | WRONG-DATA | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 19 | New Customers (Land) by Region | Report | 01aTb00000CmjvMIAR | 00OTb000008ekqjMAA | New Customers (Land) by Region | SUMMARY | THIS_FISCAL_YEAR | 1.92M | land_stage3_no_approval_emea | Commercial Approval - Land Stage 3 missing approval | WRONG-DATA | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 20 | Forecast Accuracy | Report | 01aTb00000CmjvNIAR | 00OTb000008TZsDMAW | Forecast Accuracy | MATRIX | THIS_FISCAL_YEAR | 23.65M | forecast_accuracy_snapshot | Forecast accuracy (handbook-derived) | WRONG-DATA | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 21 | Forecast and Closed Won | Report | 01aTb00000CmjvHIAR | 00OTb000008TZaTMAW | Forecast & Closed Won | SUMMARY | THIS_FISCAL_YEAR | 115.29M | forecast_accuracy_snapshot | Forecast accuracy (handbook-derived) | WRONG-DATA | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 22 | Pipeline Coverage by Stage | Report | 01aTb00000CmjvGIAR | 00OTb000008TZc5MAG | Pipeline Coverage by Stage | SUMMARY | THIS_FISCAL_YEAR | 137.63M | pipeline_overview_global | Pipeline overview with quarterly focus | WRONG-DATA | Report standard date filter uses THIS_FISCAL_YEAR but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 23 | Close Date Slipped by Stage | Report | 01aTb00000CmjvFIAR | 00OTb000008eknVMAQ | Close Date Slipped CFQ Aging | SUMMARY | LAST_FISCAL_QUARTER | 47.99M | slipped_deals_root_cause | Slipped deals analysis | WRONG-DATA | Report standard date filter uses LAST_FISCAL_QUARTER but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year. |
| 24 | Win Rate Rolling 90d | Report | 01aTb00000CmjvIIAR | 00OTb000008RdyMMAS | Win Rate Rolling 90d | SUMMARY | LAST_N_DAYS:90 | 474 |  | (orphan) | ORPHAN | Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for). |
| 25 | Commercial Approval Candidates by Stage | Report | 01aTb00000CmjvQIAR | 00OTb000008ekp7MAA | Commercial Approval Candidates | SUMMARY | CUSTOM | 315.21M | land_stage3_no_approval_emea | Commercial Approval - Land Stage 3 missing approval | OK | Matches spec and passes static rules |
| 26 | Land Stage 3 Missing Approval by Region | Report | 01aTb00000CmjvKIAR | 00OTb000008ekltMAA | Land Stage 3 Missing Approval | SUMMARY | CUSTOM | 314.83M | land_stage3_no_approval_emea | Commercial Approval - Land Stage 3 missing approval | OK | Matches spec and passes static rules |

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

Spec commit graded against: `8c81d2d`

