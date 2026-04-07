# Report 2 Source-of-Truth Contract - Sales Ops Quarterly Report

> Maps each of the 22 widgets in the Report 2 spec to a canonical SF report ID or Pipeline Inspection list view. Output of Phase 2 Task 6. Phase 4 deck rebuild reads this file.

## Header

- **Report:** Sales Ops Quarterly Report
- **Feeder dashboard:** `01ZTb00000FSP9JMAX` (Sales Ops Quarterly KPI Dashboard)
- **Spec graded against:** `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`, 22 widgets)
- **Audit input:** `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` (commit `d48f13c`)
- **PI probe input:** `/tmp/phase2-pi-probe.txt` and `/tmp/phase2-pi-matches.txt`
- **Generated:** 2026-04-07
- **Note on matcher miss rate:** The Phase 1 audit script's stem matcher had a high miss rate on this dashboard because Report 2 spec widget IDs use snake_case (e.g., `dq_kyc_not_completed`) while Dashboard 2 widget labels are plain English ("KYC Not Completed"). For the 13 existing dashboard widgets in Sections 1 and 4 of this contract, the source report IDs are taken from the SPEC's "Source report" column rather than the audit's `Matched spec ID` column.

## Source contract

22 rows, one per spec widget, in spec order, grouped by section.

### Section 1: CRM data quality (5 widgets)

All 5 exist on Dashboard 2. Source IDs pinned from spec "Source report" column. Audit confirmed the widgets exist but tagged them all ORPHAN due to the matcher miss rate on plain-English labels. The audit's static rule checks are still valid: 4 of these 5 widgets have fiscal date filter defects that must be corrected in Phase 2.5.

| # | Spec widget | Canonical source | Source ID | Verification status | Phase 2.5 fix needed | Notes |
|---|---|---|---|---|---|---|
| 1 | `dq_missing_decision_reason` | SF Report | `00OTb000008el0PMAQ` | exists - fiscal filter defect | yes | Audit row 26: "Missing Decision Reason", `THIS_FISCAL_YEAR` filter. Must switch to calendar year per hard rule 1. Also flagged: field not in detail columns (field name TBD - see spec open question 3). |
| 2 | `dq_missing_quote_type` | SF Report | `00OTb000008ekynMAA` | needs_decision | yes | Audit row 1: "Missing Quote Type", `THIS_FISCAL_YEAR` filter + picklist structurally obsolete. `APTS_Primary_Quote_Type__c` has zero active values in org. Fiscal filter defect. Per spec open question 2: decision needed - retire in favor of canonical `Type` field or repurpose for migration cleanup? |
| 3 | `dq_missing_won_loss_cfq` | SF Report | `00OTb000008SqblMAC` | exists - fiscal filter defect | yes | Audit row 30: "Won Loss Info Missing CFQ", `THIS_FISCAL_QUARTER` filter. Must switch to calendar quarter per hard rule 1. CFQ field name still needs verification (see spec open question 7). |
| 4 | `dq_missing_amount` | SF Report | `00OTb000008TZqcMAG` | exists - fiscal filter defect | yes | Audit row 25: "Missing Amount on Open Opps", `THIS_FISCAL_YEAR` filter. Must switch to calendar year per hard rule 1. Spec note: should also flag `APTS_Opportunity_ARR__c=null` per hard rule 2 (ARR is canonical). |
| 5 | `dq_kyc_not_completed` | SF Report | `00OTb000007BvlJMAS` | exists - OK on date filter | no | Audit row 33: "KYC Not Completed", `CUSTOM` filter (no fiscal date defect). Current value: 0. No Phase 2.5 fix required for this widget. |

### Section 2: Process compliance rates (5 widgets, all NEW)

None of these 5 widgets exist on Dashboard 2. All 5 are newly specified, grounded in Sales Handbook V4 slides 8-9 (stage exit gates) via SF-native field proxies. Phase 2.5 must create one new SF report per widget using the filter shapes from the spec row. No source IDs assigned yet.

| # | Spec widget | Canonical source | Source ID | Verification status | Phase 2.5 build needed | Notes |
|---|---|---|---|---|---|---|
| 6 | `pc_next_step_documented` | SF Report | (needs build) | needs_phase_2_5_build | yes | Build SF report: `IsClosed=false AND StageName IN ('3 - Engagement','4 - Shortlisted','5 - Preferred','6 - Contracting')`. Metric: `count(NextStep != null) / count(all)`. Group by `Sales_Region__c`. |
| 7 | `pc_land_commercial_approval_flow` | SF Report | (needs build) | needs_phase_2_5_build | yes | Build SF report: `Type='Land' AND IsClosed=false AND StageName IN ('3','4','5','6')`. Metric: `count(Stage_20_Approval__c=true OR Submit_for_Stage_20_Review__c=true) / count(all)`. Group by `Sales_Region__c`. Per Sales Handbook V4 slide 12 commercial approval gate. |
| 8 | `pc_recent_activity_logged` | SF Report | (needs build) | needs_phase_2_5_build | yes | Build SF report: `IsClosed=false AND StageName IN ('2','3','4','5','6')`. Metric: `count(LastActivityDate >= TODAY-30) / count(all)`. Group by `Sales_Region__c`. Per Sales Handbook V4 slide 21 activities monitoring. |
| 9 | `pc_won_loss_reason_documented` | SF Report | (needs build) | needs_phase_2_5_build | yes | Build SF report: `IsClosed=true AND CloseDate IN THIS_CALENDAR_QUARTER`. Metric: `count(Reason_Won_Lost__c != null) / count(all)`. Group by `Sales_Region__c`. Inverted rate version of widget 3. Note: related to report `00OTb000008SqblMAC` (widget 3) but is a distinct new report. |
| 10 | `pc_stage_age_within_threshold` | SF Report | (needs build) | needs_phase_2_5_build | yes | Build SF report: `IsClosed=false AND StageName IN ('3','4','5','6')`. Metric: `count(LastStageChangeDate within stage threshold) / count(all)`. Group by `StageName`. Stage thresholds (placeholders per spec open question 4): Stage 3=60d, Stage 4=45d, Stage 5=30d, Stage 6=15d. Thresholds need Sales Ops sign-off before build. |

### Section 3: Forecast accuracy (4 widgets, all PI native)

All 4 widgets are Pipeline Inspection native per spec hard rule 5. None exist on Dashboard 2 today. Source IDs assigned from `/tmp/phase2-pi-matches.txt` (Task 4 probe output).

| # | Spec widget | Canonical source | Source ID | Verification status | Phase 2.5 follow-up | Notes |
|---|---|---|---|---|---|---|
| 11 | `fa_quarterly_realized_vs_commit` | Pipeline Inspection | `4c2Tb0000003jobIAA` | needs_pi_integration_design | yes (Phase 4 PI integration) | PI list view: Global Book of Business ARR CFQ Forecast (`DateLiteralType=THIS_FISCAL_QUARTER`, `ChangePeriodLiteralType=START_OF_THE_PERIOD`, SummaryField=ARR). "Did we hit our commit?" - realized closed-won vs quarter-start commit baseline. Shared source with Report 1 widget 16. |
| 12 | `fa_quarterly_realized_vs_bestcase` | Pipeline Inspection | `4c2Tb0000003jobIAA` (same view) | needs_pi_integration_design | yes (commit/best-case split via downstream ForecastCategoryName filter) | Same PI list view as widget 11. No separate best-case list view exists in org. Best-case split must come from `Opportunity.ForecastCategoryName` filter in CRMA recipe downstream, not a distinct PI list view. The gap between commit and best-case accuracy is a leading indicator of forecast volatility. |
| 13 | `fa_forecast_change_volatility` | Pipeline Inspection | `needs_pi_config_change` | blocked | yes (Phase 2.5 PI Lightning UI setup) | No list view with 6-month trailing change window exists. `ChangePeriodLiteralType` options in org: `START_OF_THE_PERIOD` and `THIS_WEEK` only. Phase 2.5 must create a new PI list view in SF Lightning UI with a 26-week (6-month) trailing window. Metric: std dev of weekly forecast change rate, grouped by Month. |
| 14 | `fa_slipped_count_quarterly` | Pipeline Inspection | `needs_pi_config_change` | blocked | yes (Phase 2.5 PI Lightning UI setup) | No list view with slip-type filter exists. `PipelineInspectionListView` has no `SlipFilter` field; PI "Pipeline Changes" view with Slipped filter is a Lightning UI-only construct not queryable via SOQL. Phase 2.5 must create a Pipeline Changes view with "Slipped" change type filter via SF Lightning UI. Uses `OpportunityFieldHistory` for true forward-only slip detection (not the `LastCloseDateChangedHistoryId` proxy). |

### Section 4: Pipeline hygiene (8 widgets)

All 8 exist on Dashboard 2. Source IDs pinned from spec "Source report" column. Audit tagged them all ORPHAN due to matcher miss rate on plain-English labels. Two known data defects called out in spec; one widget is WIP/Under Construction. Audit also surfaced fiscal date filter defects on several widgets in this section.

| # | Spec widget | Canonical source | Source ID | Verification status | Phase 2.5 fix needed | Notes |
|---|---|---|---|---|---|---|
| 15 | `ph_probability_mismatch_by_stage` | SF Report | `00OTb000008TaJdMAK` | WIP/DEFERRED (Under Construction) | yes (complete the WIP) | Audit row 29: "Under Construction: Probability Mismatch by Stage", `THIS_FISCAL_YEAR` filter. Marked Under Construction by Andre. Filter shape unvalidated - requires comparing actual `Probability` to per-stage defaults from Sales Handbook V4 slide 17. Two Phase 2.5 actions: (1) complete widget design + fiscal-to-calendar fix; (2) define probability mismatch threshold per spec open question 8. |
| 16 | `ph_low_probability_in_quarter` | SF Report | `00OTb000008RfKDMA0` | exists - fiscal filter defect | yes | Audit row 24: "Low Probability In Quarter", `THIS_FISCAL_QUARTER` filter. Must switch to calendar quarter per hard rule 1. Current value: 11.48M (using Amount, not ARR - verify aggregation field). Probability threshold implicit - verify in Phase 2.5. |
| 17 | `ph_aging_pipeline_365_plus` | SF Report | `00OTb000008Ti7VMAS` | exists - ARR aggregation defect | yes | Audit row 31: "Aging Pipeline 365 Plus Days", `CUSTOM` filter (no fiscal date defect). WRONG-DATA: aggregates on standard `AMOUNT`, must switch to `APTS_Opportunity_ARR__c` per hard rule 2. Current value: 2164.51M (Amount). |
| 18 | `ph_high_value_stale_deals` | SF Report | `00OTb000008Ti97MAC` | exists - OK on date filter | no (pending threshold verification) | Audit row 32: "High Value Stale Deals", `CUSTOM` filter (no fiscal date defect). Current value: 22.03M. High-value ARR threshold implicit - needs explicit value (e.g. EUR 1M) per spec open question 5. No Phase 2.5 fix required unless threshold confirmed wrong. |
| 19 | `ph_stale_opportunities` | SF Report | `00OTb000008TZgvMAG` | exists - fiscal filter defect | yes | Audit row 28: "Stale Opportunities", `THIS_FISCAL_YEAR` filter. Must switch to calendar year per hard rule 1. List view sibling of widget 10 (`pc_stage_age_within_threshold`). Current value: 1.3K rows. |
| 20 | `ph_no_activity_30_plus` | SF Report | `00OTb000008TaEnMAK` | exists - fiscal filter defect + ARR aggregation defect | yes | Audit row 27: "No Activity 30+ Days - Open Opps", `THIS_FISCAL_YEAR` filter. TWO defects: (1) fiscal-to-calendar fix required per hard rule 1; (2) aggregation uses `s!AMOUNT` (standard Amount) instead of `APTS_Opportunity_ARR__c` per hard rule 2. Current value: 1214.99M (Amount). Both defects must be fixed before deck rebuild. |
| 21 | `ph_overdue_opportunities` | SF Report | `00OTb000008SrmLMAS` | exists - fiscal grouping defect | yes | Audit row 35: "Overdue Opportunities", `CUSTOM` date filter (OK on filter). WRONG grouping: uses `FISCAL_QUARTER` grouping dimension instead of calendar quarter per hard rule 1. Must change grouping from `FISCAL_QUARTER` to calendar quarter. Current value: 2.4K. |
| 22 | `ph_overdue_close_date_list` | SF Report | `00OTb000008TaBZMA0` | exists - OK | no | Audit row 34: "Overdue Close Date Open Opps", `CUSTOM` filter (no fiscal date defect). List view of widget 21's count. Current value: 122 open opps with overdue close date. No Phase 2.5 fix required for this widget itself. |

## Summary by source category

| Source category | Count | Notes |
|---|---|---|
| SF Report (existing, OK) | 2 | Widgets 5, 22 - no fix needed |
| SF Report (existing, needs fix - fiscal date filter) | 7 | Widgets 1, 2, 3, 4, 16, 19, 20 - `THIS_FISCAL_YEAR` or `THIS_FISCAL_QUARTER` must switch to calendar equivalents (hard rule 1). Widget 2 also has the open-question decision on field retirement. |
| SF Report (existing, needs fix - ARR aggregation) | 2 | Widgets 17, 20 - aggregate on `AMOUNT` instead of `APTS_Opportunity_ARR__c` (hard rule 2). Widget 20 appears in both fiscal-filter and ARR-aggregation categories (two distinct defects). |
| SF Report (existing, needs fix - fiscal grouping) | 1 | Widget 21 - `ph_overdue_opportunities` uses `FISCAL_QUARTER` grouping, not calendar. |
| SF Report (existing, WIP/DEFERRED) | 1 | Widget 15 - `ph_probability_mismatch_by_stage` Under Construction on live dashboard. |
| SF Report (existing, needs_decision) | 1 | Widget 2 - `dq_missing_quote_type` field is migrated-empty; retire vs repurpose decision needed before any fix can be applied. |
| SF Report (missing, needs build) | 5 | Widgets 6-10 - Section 2 process compliance. Phase 2.5 creates 5 new SF reports per spec filter shapes. |
| Pipeline Inspection (assigned, needs integration design) | 2 | Widgets 11, 12 - pinned to `4c2Tb0000003jobIAA`. Phase 4 PI-to-CRMA integration design needed. Widget 12 also needs `ForecastCategoryName` filter downstream for commit/best-case split. |
| Pipeline Inspection (needs config change) | 2 | Widgets 13, 14 - no matching PI list view exists in org. Phase 2.5 must create new PI list views via SF Lightning UI. |

**Total: 22 widgets.** (5 + 5 + 4 + 8)

**Distinct widgets needing any Phase 2.5 action: 17 of 22.** Widgets with no action required: 5 (`dq_kyc_not_completed`), 18 (`ph_high_value_stale_deals` - threshold verification only), 22 (`ph_overdue_close_date_list`).

## Phase 2.5 follow-ups

### Build queue (5 new SF reports for process compliance)

Widgets 6-10: `pc_next_step_documented`, `pc_land_commercial_approval_flow`, `pc_recent_activity_logged`, `pc_won_loss_reason_documented`, `pc_stage_age_within_threshold`. None exist on Dashboard 2 or as SF reports today. Build each using the filter shape from the spec row in Section 2.

- **Prerequisite for widget 10:** Sales Ops sign-off on stage-specific aging thresholds. Spec placeholders: Stage 3=60d, Stage 4=45d, Stage 5=30d, Stage 6=15d. Per spec open question 4.

### Defect fix queue

1. **Widget 20** (`ph_no_activity_30_plus`): TWO defects - (a) switch aggregation from `s!AMOUNT` to `APTS_Opportunity_ARR__c` (hard rule 2); (b) switch `THIS_FISCAL_YEAR` to calendar year (hard rule 1).
2. **Widget 21** (`ph_overdue_opportunities`): switch `FISCAL_QUARTER` grouping to calendar quarter (hard rule 1).
3. **Widget 17** (`ph_aging_pipeline_365_plus`): switch aggregation from `AMOUNT` to `APTS_Opportunity_ARR__c` (hard rule 2).
4. **Fiscal date filter sweep (7 widgets):** Widgets 1, 2, 3, 4, 16, 19, 20 - switch `THIS_FISCAL_YEAR` / `THIS_FISCAL_QUARTER` to custom calendar date filters per hard rule 1.

### WIP completion

- **Widget 15** (`ph_probability_mismatch_by_stage`): Phase 2.5 actions: (1) define probability mismatch threshold per Sales Handbook V4 slide 17 per-stage defaults (see spec open question 8); (2) validate filter logic; (3) fix `THIS_FISCAL_YEAR` to calendar year; (4) unpublish the "Under Construction" label.

### PI configuration (SF Lightning UI manual setup)

- **Widget 13** (`fa_forecast_change_volatility`): create new PI list view in SF Lightning UI with 26-week (6-month) trailing change window. No `ChangePeriodLiteralType` option for 6-month window currently exists in the org's PI configuration.
- **Widget 14** (`fa_slipped_count_quarterly`): create Pipeline Changes view in SF Lightning UI with "Slipped" change type filter. `PipelineInspectionListView` has no `SlipFilter` field - this is a UI-only configuration.

### Open question: widget 2

`dq_missing_quote_type` - retire or repurpose? `APTS_Primary_Quote_Type__c` is migrated-empty (zero active picklist values in org). Options: (a) retire the widget; (b) replace with a widget grading the canonical `Type` field (Land/Expand/Renewal). Per spec open question 2. Decision must precede any Phase 2.5 fix on this widget.
