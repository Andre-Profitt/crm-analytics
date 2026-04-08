# Sales Director Monthly Dashboard Audit - 2026-04-08

## Header

- **Dashboard ID:** `01ZTb00000FSP7hMAH`
- **Dashboard name:** Sales Directors Monthly Pipeline and Insights
- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view
- **Dashboard lastModifiedDate:** 2026-04-08T20:30:45Z
- **Audit run date:** 2026-04-08
- **Spec graded against:** `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`)
- **Audit script:** `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted by convention)
- **Tally:** 23 entries . 8 BLOCKING . 1 COSMETIC . 14 OK

## Table 1: Executive summary

Sorted by severity then KPI bullet. Read this table first. Fix every BLOCKING item before the deck rebuild.

| Severity | Widget | KPI bullet | Issue | Recommended fix |
|---|---|---|---|---|
| BLOCKING | (MISSING) land_stage3_no_approval_apac | Commercial Approval - Land Stage 3 missing approval | Spec requires this widget; dashboard does not have it | Add widget land_stage3_no_approval_apac with filters: `Type='Land'` AND `StageName='3 - Engagement'` AND `Stage_20_Approval__c=false` AND `Sales_Region__c IN ('APAC','Middle East & Africa')` |
| BLOCKING | (MISSING) land_stage3_no_approval_nam | Commercial Approval - Land Stage 3 missing approval | Spec requires this widget; dashboard does not have it | Add widget land_stage3_no_approval_nam with filters: `Type='Land'` AND `StageName='3 - Engagement'` AND `Stage_20_Approval__c=false` AND `Sales_Region__c='North America'` |
| BLOCKING | (MISSING) pipeline_overview_apac | Pipeline overview with quarterly focus | Spec requires this widget; dashboard does not have it | Add widget pipeline_overview_apac with filters: `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `Sales_Region__c IN ('APAC','Middle East & Africa')` |
| BLOCKING | (MISSING) pipeline_overview_emea | Pipeline overview with quarterly focus | Spec requires this widget; dashboard does not have it | Add widget pipeline_overview_emea with filters: `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `Sales_Region__c IN ('United Kingdom & Ireland','Central Europe','Northern Europe','Southwestern Europe')` |
| BLOCKING | (MISSING) pipeline_overview_nam | Pipeline overview with quarterly focus | Spec requires this widget; dashboard does not have it | Add widget pipeline_overview_nam with filters: `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `Sales_Region__c='North America'` |
| BLOCKING | (MISSING) renewal_likelihood | Renewals tracking | Spec requires this widget; dashboard does not have it | Add widget renewal_likelihood with filters: `Type='Renewal'` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `IsClosed=false` |
| BLOCKING | (MISSING) renewal_upcoming_list | Renewals tracking | Spec requires this widget; dashboard does not have it | Add widget renewal_upcoming_list with filters: `Type='Renewal'` AND `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` |
| BLOCKING | (MISSING) slipped_deals_trend | Slipped deals analysis | Spec requires this widget; dashboard does not have it | Add widget slipped_deals_trend with filters: Source: **Pipeline Inspection native** (canonical). SF report fallback: `LastCloseDateChangedHistoryId != null` |
| COSMETIC | Renewals by Fiscal Quarter | Renewals tracking | Widget title contains the word 'fiscal': 'Renewals by Fiscal Quarter'. The brief requires calendar-year framing. | See static rule detail |
| OK | Business At Risk | Churn Risk and trends | Matches spec and passes static rules | n/a |
| OK | Commercial Approval Candidates by Stage | Commercial Approval - Land Stage 3 missing approval | Matches spec and passes static rules | n/a |
| OK | Land Stage 3 Missing Approval by Region | Commercial Approval - Land Stage 3 missing approval | Matches spec and passes static rules | n/a |
| OK | New Customers (Land) by Region | Commercial Approval - Land Stage 3 missing approval | Matches spec and passes static rules | n/a |
| OK | Commercial Approval Approved YTD (Land) | Commercial Approval overview (global, YTD approved) | Matches spec and passes static rules | n/a |
| OK | Commercial Approval Global Current State | Commercial Approval overview (global, current state) | Matches spec and passes static rules | n/a |
| OK | Forecast Accuracy | Forecast accuracy (handbook-derived) | Matches spec and passes static rules | n/a |
| OK | Forecast and Closed Won | Forecast accuracy (handbook-derived) | Matches spec and passes static rules | n/a |
| OK | Pipeline Coverage by Stage | Pipeline overview with quarterly focus | Matches spec and passes static rules | n/a |
| OK | Pipeline Overview Global by Stage (This Quarter) | Pipeline overview with quarterly focus | Matches spec and passes static rules | n/a |
| OK | Renewal Pipeline This Quarter | Pipeline overview with quarterly focus | Matches spec and passes static rules | n/a |
| OK | Renewal ACV by Quarter | Renewals tracking | Matches spec and passes static rules | n/a |
| OK | Renewal Likelihood by Probability (This Quarter) | Renewals tracking | Matches spec and passes static rules | n/a |
| OK | Close Date Slipped by Stage | Slipped deals analysis | Matches spec and passes static rules | n/a |

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
| 3 | (MISSING) pipeline_overview_apac | chart |  |  |  |  |  |  | pipeline_overview_apac | Pipeline overview with quarterly focus | BLOCKING | Spec requires this widget; dashboard does not have it |
| 4 | (MISSING) pipeline_overview_emea | chart |  |  |  |  |  |  | pipeline_overview_emea | Pipeline overview with quarterly focus | BLOCKING | Spec requires this widget; dashboard does not have it |
| 5 | (MISSING) pipeline_overview_nam | chart |  |  |  |  |  |  | pipeline_overview_nam | Pipeline overview with quarterly focus | BLOCKING | Spec requires this widget; dashboard does not have it |
| 6 | (MISSING) renewal_likelihood | chart |  |  |  |  |  |  | renewal_likelihood | Renewals tracking | BLOCKING | Spec requires this widget; dashboard does not have it |
| 7 | (MISSING) renewal_upcoming_list | table |  |  |  |  |  |  | renewal_upcoming_list | Renewals tracking | BLOCKING | Spec requires this widget; dashboard does not have it |
| 8 | (MISSING) slipped_deals_trend | chart |  |  |  |  |  |  | slipped_deals_trend | Slipped deals analysis | BLOCKING | Spec requires this widget; dashboard does not have it |
| 9 | Renewals by Fiscal Quarter | Report | 01aTb00000Cn85oIAB | 00OTb000008eksLMAQ | Renewals by Fiscal Quarter | SUMMARY | THIS_YEAR | 15.09M | renewal_acv_this_quarter | Renewals tracking | COSMETIC | Widget title contains the word 'fiscal': 'Renewals by Fiscal Quarter'. The brief requires calendar-year framing. |
| 10 | Business At Risk | Report | 01aTb00000Cn85dIAB | 00OTb000008Ta9xMAC | Business At Risk | SUMMARY | THIS_YEAR | 483.21M | churn_risk_placeholder | Churn Risk and trends | OK | Matches spec and passes static rules |
| 11 | Commercial Approval Candidates by Stage | Report | 01aTb00000Cn85jIAB | 00OTb000008ekp7MAA | Commercial Approval Candidates | SUMMARY | CUSTOM | 303.43M | land_stage3_no_approval_emea | Commercial Approval - Land Stage 3 missing approval | OK | Matches spec and passes static rules |
| 12 | Land Stage 3 Missing Approval by Region | Report | 01aTb00000Cn85hIAB | 00OTb000008ekltMAA | Land Stage 3 Missing Approval | SUMMARY | CUSTOM | 303.04M | land_stage3_no_approval_emea | Commercial Approval - Land Stage 3 missing approval | OK | Matches spec and passes static rules |
| 13 | New Customers (Land) by Region | Report | 01aTb00000Cn85cIAB | 00OTb000008ekqjMAA | New Customers (Land) by Region | SUMMARY | THIS_YEAR | 1.92M | land_stage3_no_approval_emea | Commercial Approval - Land Stage 3 missing approval | OK | Matches spec and passes static rules |
| 14 | Commercial Approval Approved YTD (Land) | Report | 01aTb00000Cn8FFIAZ | 00OTb000008aTtJMAU | Commercial Approval approved 2026 | SUMMARY | CUSTOM | 18.94M | commercial_approval_approved_ytd | Commercial Approval overview (global, YTD approved) | OK | Matches spec and passes static rules |
| 15 | Commercial Approval Global Current State | Report | 01aTb00000Cn9mQIAR | 00OTb000008fBEDMA2 | P2.7 Commercial Approval Global | SUMMARY | CUSTOM | 1.8K | commercial_approval_global | Commercial Approval overview (global, current state) | OK | Matches spec and passes static rules |
| 16 | Forecast Accuracy | Report | 01aTb00000Cn85iIAB | 00OTb000008TZsDMAW | Forecast Accuracy | MATRIX | THIS_YEAR | 23.37M | forecast_accuracy_snapshot | Forecast accuracy (handbook-derived) | OK | Matches spec and passes static rules |
| 17 | Forecast and Closed Won | Report | 01aTb00000Cn85gIAB | 00OTb000008TZaTMAW | Forecast & Closed Won | SUMMARY | THIS_YEAR | 115.39M | forecast_accuracy_snapshot | Forecast accuracy (handbook-derived) | OK | Matches spec and passes static rules |
| 18 | Pipeline Coverage by Stage | Report | 01aTb00000Cn85aIAB | 00OTb000008TZc5MAG | Pipeline Coverage by Stage | SUMMARY | THIS_YEAR | 137.63M | pipeline_overview_global | Pipeline overview with quarterly focus | OK | Matches spec and passes static rules |
| 19 | Pipeline Overview Global by Stage (This Quarter) | Report | 01aTb00000Cn9mPIAR | 00OTb000008fBfdMAE | P2.7 Pipeline Global This Qtr | SUMMARY | THIS_QUARTER | 51.42M | pipeline_overview_global | Pipeline overview with quarterly focus | OK | Matches spec and passes static rules |
| 20 | Renewal Pipeline This Quarter | Report | 01aTb00000Cn85ZIAR | 00OTb000008ektxMAA | Renewal Pipeline This Quarter | SUMMARY | THIS_QUARTER | 2.59M | pipeline_overview_global | Pipeline overview with quarterly focus | OK | Matches spec and passes static rules |
| 21 | Renewal ACV by Quarter | Report | 01aTb00000Cn85bIAB | 00OTb000008ekxBMAQ | Renewal ACV by Quarter | SUMMARY | THIS_YEAR | 11.04M | renewal_acv_this_quarter | Renewals tracking | OK | Matches spec and passes static rules |
| 22 | Renewal Likelihood by Probability (This Quarter) | Report | 01aTb00000Cn9mRIAR | 00OTb000008fBULMA2 | P2.7 Renewal Likelihood This Qtr | SUMMARY | THIS_QUARTER | 2.59M | renewal_acv_this_quarter | Renewals tracking | OK | Matches spec and passes static rules |
| 23 | Close Date Slipped by Stage | Report | 01aTb00000Cn85lIAB | 00OTb000008eknVMAQ | Close Date Slipped CFQ Aging | SUMMARY | LAST_QUARTER | 58.57M | slipped_deals_root_cause | Slipped deals analysis | OK | Matches spec and passes static rules |

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

