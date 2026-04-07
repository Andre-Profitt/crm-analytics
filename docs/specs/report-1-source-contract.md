# Report 1 Source-of-Truth Contract - Pipeline Reporting & Insights (Sales Director Monthly)

> Maps each of the 16 widgets in the Report 1 spec to a canonical SF report ID or Pipeline Inspection list view. Output of Phase 2 Task 5. Phase 4 deck rebuild reads this file to decide which data source feeds each slide.

## Header

- **Report:** Pipeline Reporting & Insights (monthly, Sales Directors)
- **Feeder dashboard:** `01ZTb00000FSP7hMAH` (Sales Directors Monthly - Pipeline and Insights)
- **Spec graded against:** `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`, 16 widgets)
- **Audit input:** `docs/audits/2026-04-07-sales-director-monthly-audit.md` (commit `6cbe8fe`)
- **PI probe input:** `/tmp/phase2-pi-probe.txt` and `/tmp/phase2-pi-matches.txt`
- **Generated:** 2026-04-07

## Source contract

| # | Spec widget | Canonical source | Source ID | Verification status | Phase 1.5 fix needed | Notes |
|---|---|---|---|---|---|---|
| 1 | pipeline_overview_global | SF Report | `00OTb000008ektxMAA` (Renewal Pipeline This Quarter) | BLOCKING | yes | Two audit matches for this spec ID: `00OTb000008ektxMAA` (Renewal Pipeline This Quarter, THIS_FISCAL_QUARTER, BLOCKING) and `00OTb000008TZc5MAG` (Pipeline Coverage by Stage, THIS_FISCAL_YEAR, WRONG-DATA). Worst severity = BLOCKING. Both existing reports use fiscal date filters; spec requires `CloseDate IN THIS_CALENDAR_QUARTER`. Existing widget also aggregates on `AMOUNT` instead of `APTS_Opportunity_ARR__c`. Dashboard widget needs to be rebuilt with correct field and calendar-quarter filter. |
| 2 | pipeline_overview_emea | SF Report | needs_phase_1_5_build | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec requires stacked bar by stage filtered to `IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER AND Sales_Region__c IN ('United Kingdom & Ireland','Central Europe','Northern Europe','Southwestern Europe')` with `sum(APTS_Opportunity_ARR__c)` grouped by `StageName`. Must be created. |
| 3 | pipeline_overview_nam | SF Report | needs_phase_1_5_build | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec requires stacked bar by stage filtered to `IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER AND Sales_Region__c='North America'` with `sum(APTS_Opportunity_ARR__c)` grouped by `StageName`. Must be created. |
| 4 | pipeline_overview_apac | SF Report | needs_phase_1_5_build | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec requires stacked bar by stage filtered to `IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER AND Sales_Region__c IN ('APAC','Middle East & Africa')` with `sum(APTS_Opportunity_ARR__c)` grouped by `StageName`. MEA grouped with APAC per existing CRMA convention. Must be created. |
| 5 | commercial_approval_global | SF Report | needs_phase_1_5_build | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec requires count of `Stage_20_Approval__c` (approved vs not-approved) on open opps: `IsClosed=false AND StageName!='0 - Lost'`. Current-state forward-looking snapshot. Must be created. |
| 6 | commercial_approval_approved_ytd | SF Report | `00OTb000008aTtJMAU` (Commercial_Approval_approved_2026_qbd) | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit (MISSING in live dashboard). Spec-pinned source-of-truth report is `00OTb000008aTtJMAU` (Sales Operations folder), which filters `Type='Land' AND Stage_20_Approval__c=true AND Stage_20_Approval_Date__c > '2026-01-01'` and aggregates `count + sum(APTS_Opportunity_ARR__c) + sum(APTS_Forecast_ARR__c)`. Dashboard widget must be created pointing at this canonical report. Source ID pre-pinned from spec source-of-truth references. |
| 7 | land_stage3_no_approval_emea | SF Report | `00OTb000008ekp7MAA` (Commercial Approval Candidates) | WRONG-DATA | yes | Mixed audit result: rows 25/26 are OK (`00OTb000008ekp7MAA` and `00OTb000008ekltMAA`, CUSTOM date filter, pass static rules), but row 19 (New Customers (Land) by Region, `00OTb000008ekqjMAA`, THIS_FISCAL_YEAR) is WRONG-DATA. Worst severity applied = WRONG-DATA. Canonical source pinned to `00OTb000008ekp7MAA` (CUSTOM filter, OK row). Phase 1.5 fix: retire or correct `00OTb000008ekqjMAA`. Note: spec's named source-of-truth is `00OTb000008d6ovMAA` (Commercial_Approval_candidates_cdi) which should feed all 3 per-region widgets via region grouping. |
| 8 | land_stage3_no_approval_nam | SF Report | `00OTb000008d6ovMAA` (Commercial_Approval_candidates_cdi) | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec-named canonical source is `00OTb000008d6ovMAA` (Sales Operations folder), same underlying report as widget 7 filtered to `Sales_Region__c='North America'`. Must be created as a new dashboard widget with the NAM region filter applied. |
| 9 | land_stage3_no_approval_apac | SF Report | `00OTb000008d6ovMAA` (Commercial_Approval_candidates_cdi) | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec-named canonical source is `00OTb000008d6ovMAA`, same underlying report as widgets 7 and 8 filtered to `Sales_Region__c IN ('APAC','Middle East & Africa')`. Must be created. |
| 10 | renewal_acv_this_quarter | SF Report | `00OTb000008ekxBMAQ` (Renewal ACV by Quarter) | BLOCKING | yes | Two audit matches: `00OTb000008ekxBMAQ` (Renewal ACV by Quarter, THIS_FISCAL_YEAR, BLOCKING) and `00OTb000008eksLMAQ` (Renewals by Fiscal Quarter, THIS_FISCAL_YEAR + "fiscal" in title, BLOCKING). Both BLOCKING. Primary source pinned to `00OTb000008ekxBMAQ`. Two defects: (1) fiscal year filter must be replaced with `CloseDate IN THIS_CALENDAR_QUARTER`; (2) aggregation uses `AMOUNT` instead of `APTS_Renewal_ACV__c` per spec hard rule 2 (Sales Handbook V4 slides 24-25). `00OTb000008eksLMAQ` additionally uses "Fiscal" in title (violates hard rule 1). Both reports need remediation. |
| 11 | renewal_likelihood | SF Report | needs_phase_1_5_build | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec requires `sum(APTS_Renewal_ACV__c)` grouped by `Probability` bucket, filtered to `Type='Renewal' AND CloseDate IN THIS_CALENDAR_QUARTER AND IsClosed=false`. Likelihood = standard SF `Probability` field (confirmed via Sales Handbook V4 slide 17). Must be created. |
| 12 | renewal_upcoming_list | SF Report | needs_phase_1_5_build | BLOCKING (MISSING) | yes | No matching dashboard widget found in audit. Spec requires a table filtered to `Type='Renewal' AND IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER` with columns: Account, Opp Name, `APTS_Renewal_ACV__c`, CloseDate, Probability, Owner. Must be created. |
| 13 | churn_risk_placeholder | Finance feed (pending Alex P) | | blocked_on_finance_feed | no (separate workstream) | Audit matched "Business At Risk" (`00OTb000008Ta9xMAC`, WRONG-DATA, THIS_FISCAL_YEAR). However the spec designates this widget as a placeholder pending Finance feed from Alex P -- not a Phase 1.5 fix but a separate workstream. The existing "Business At Risk" report is a weak proxy; spec states "finance_feed_status: pending" and cites the Sales Handbook V4 LAER model (slides 28-30) but notes no specific churn metric is defined. Source ID intentionally blank until Finance confirms the feed. Phase 4 deck slide should display "Awaiting Finance feed (Alex P)" label. |
| 14 | slipped_deals_root_cause | Pipeline Inspection (native, future) | needs_pi_config_change | needs_pi_integration_design | yes (PI integration is new) | Spec declares Pipeline Inspection native as canonical; SF report fallback is a weak proxy. Audit matched "Close Date Slipped by Stage" (`00OTb000008eknVMAQ`, WRONG-DATA, LAST_FISCAL_QUARTER). SF fallback uses `LastCloseDateChangedHistoryId != null` which cannot distinguish forward slips from backward pull-ins. PI native computes period-over-period slip detection from `OpportunityFieldHistory` natively. `/tmp/phase2-pi-matches.txt` shows no PI list view assignment for slipped deals -- the PI "Pipeline Changes - Slipped" construct is Lightning UI-only, not queryable via SOQL. Phase 2.5 will configure this in SF Lightning UI. SF report `00OTb000008eknVMAQ` is the interim fallback only; must not be presented as authoritative. |
| 15 | slipped_deals_trend | Pipeline Inspection (native, future) | needs_pi_config_change | needs_pi_integration_design | yes (PI integration is new) | Same canonical source rationale as widget 14. No matching dashboard widget in audit (MISSING, BLOCKING). PI probe found no list view with a 6-month trailing change window; `ChangePeriodLiteralType` options observed are `START_OF_THE_PERIOD` and `THIS_WEEK` only -- no 26-week window available. A new PI list view must be created in the SF Lightning UI (Phase 2.5). Spec requires a line chart of trailing 6 calendar months grouped by `Month(LastStageChangeDate)`. No SF report fallback exists for this widget. |
| 16 | forecast_accuracy_snapshot | Pipeline Inspection | `4c2Tb0000003jobIAA` (Global Book of Business ARR CFQ Forecast) | needs_pi_integration_design | yes (PI integration is new) | Spec hard rule 6 mandates PI native as canonical source. Existing dashboard widgets `00OTb000008TZsDMAW` ("Forecast Accuracy", WRONG-DATA, THIS_FISCAL_YEAR) and `00OTb000008TZaTMAW` ("Forecast and Closed Won", WRONG-DATA, THIS_FISCAL_YEAR) are SF-report fallbacks for the same intent but use fiscal date filters -- flagged WRONG-DATA in the audit. PI list view `4c2Tb0000003jobIAA` (Global Book of Business ARR CFQ Forecast, `DateLiteralType=THIS_FISCAL_QUARTER`, `ChangePeriodLiteralType=START_OF_THE_PERIOD`, ARR summary field) is the canonical source per the PI probe. Note: no `ForecastCategory` roll-up columns exist in PI schema; a CRMA-side category split from `Opportunity.ForecastCategoryName` will be needed for Phase 4 deck rendering. |

## Source mapping rules applied

- **For widgets where the audit found a matching dashboard widget with severity OK:** pin the `report_id` as the canonical source. Verification status = OK. Fix needed = no.
- **For widgets where the audit found a matching dashboard widget with severity WRONG-DATA or BLOCKING:** pin the same `report_id` as the canonical source but mark Phase 1.5 fix needed = yes, with the specific defect noted.
- **For widgets where the audit found NO match (`(MISSING)`):** leave the source ID as `needs_phase_1_5_build` (or pin the spec-named source-of-truth if one is explicitly referenced) and mark the verification status accordingly.
- **For widgets 14 and 15 (slipped deals):** declare `Pipeline Inspection (native, future)` as the canonical source per spec, but note that no PI list view is configured yet (`/tmp/phase2-pi-matches.txt` shows `<none>`). Source ID = `needs_pi_config_change`. The SF report fallback (matched in the audit for widget 14) is a weak proxy.
- **For widget 16 (forecast accuracy snapshot):** declare `Pipeline Inspection` as canonical source. Source ID = `4c2Tb0000003jobIAA` (from the PI matches file). Existing SF dashboard widgets `00OTb000008TZsDMAW` and `00OTb000008TZaTMAW` are fallbacks but the spec mandates PI native per hard rule 6.
- **For widget 13 (churn risk placeholder):** source = "Finance feed (pending Alex P)". Source ID = blank. Status = blocked on Finance feed identification. Not a Phase 1.5 fix -- it is a separate workstream.
- **Mixed severity rule:** where multiple audit rows match the same spec widget ID at different severities, apply the worst severity and note all matches. Pin the cleanest available report ID as the canonical source.

## Summary by source category

| Source category | Count | Notes |
|---|---|---|
| SF Report (existing, OK) | 0 | No spec widget has a purely OK audit match; widget 7's two OK rows are outweighed by a WRONG-DATA co-match |
| SF Report (existing, needs fix) | 3 | Widgets 1 (BLOCKING), 7 (WRONG-DATA), 10 (BLOCKING) -- Phase 1.5 fix queue |
| SF Report (missing, needs build) | 9 | Widgets 2, 3, 4, 5, 6, 8, 9, 11, 12 -- Phase 1.5 build queue |
| Pipeline Inspection (assigned) | 1 | Widget 16 -- Source ID `4c2Tb0000003jobIAA` pinned; integration design needed for Phase 4 |
| Pipeline Inspection (needs config) | 2 | Widgets 14, 15 -- Phase 2.5 PI Lightning UI setup required before source ID can be pinned |
| Finance feed (pending Alex P) | 1 | Widget 13 -- Blocked on external; separate workstream from Phase 1.5 |

**Total: 16 widgets mapped.**

## Phase 1.5 / Phase 2.5 / Phase 4 follow-ups

### Phase 1.5 fixes (Dashboard 1 hotfix)

Every row with `Phase 1.5 fix needed = yes`:

1. **Widget 1 - pipeline_overview_global:** Replace or fix `00OTb000008ektxMAA` (fiscal quarter filter, AMOUNT aggregation): use `CloseDate IN THIS_CALENDAR_QUARTER` and `sum(APTS_Opportunity_ARR__c)`. Also retire or fix `00OTb000008TZc5MAG` (Pipeline Coverage by Stage, THIS_FISCAL_YEAR) which shares the same spec ID.
2. **Widget 2 - pipeline_overview_emea:** Create new SF report and dashboard widget: `IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER AND Sales_Region__c IN ('United Kingdom & Ireland','Central Europe','Northern Europe','Southwestern Europe')`, stacked bar by `StageName`, `sum(APTS_Opportunity_ARR__c)`.
3. **Widget 3 - pipeline_overview_nam:** Create new SF report and dashboard widget: `IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER AND Sales_Region__c='North America'`, stacked bar by `StageName`.
4. **Widget 4 - pipeline_overview_apac:** Create new SF report and dashboard widget: `IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER AND Sales_Region__c IN ('APAC','Middle East & Africa')`, stacked bar by `StageName`.
5. **Widget 5 - commercial_approval_global:** Create new SF report and dashboard widget counting `Stage_20_Approval__c` (true vs false) on `IsClosed=false AND StageName!='0 - Lost'`.
6. **Widget 6 - commercial_approval_approved_ytd:** Create new dashboard widget pointing to `00OTb000008aTtJMAU` (report already exists in Sales Operations folder). Widget shape: metric+list showing count and `sum(APTS_Opportunity_ARR__c)` YTD approved Land deals.
7. **Widget 7 - land_stage3_no_approval_emea:** Retire or fix `00OTb000008ekqjMAA` (New Customers (Land) by Region, THIS_FISCAL_YEAR). Confirm `00OTb000008ekp7MAA` (or canonical `00OTb000008d6ovMAA`) as the sole EMEA source.
8. **Widget 8 - land_stage3_no_approval_nam:** Create new dashboard widget from `00OTb000008d6ovMAA` filtered to `Sales_Region__c='North America'`.
9. **Widget 9 - land_stage3_no_approval_apac:** Create new dashboard widget from `00OTb000008d6ovMAA` filtered to `Sales_Region__c IN ('APAC','Middle East & Africa')`.
10. **Widget 10 - renewal_acv_this_quarter:** Fix `00OTb000008ekxBMAQ`: replace THIS_FISCAL_YEAR with `CloseDate IN THIS_CALENDAR_QUARTER` and replace `AMOUNT` with `APTS_Renewal_ACV__c`. Remediate or retire `00OTb000008eksLMAQ` (same two defects plus "Fiscal" in title).
11. **Widget 11 - renewal_likelihood:** Create new SF report and dashboard widget: `Type='Renewal' AND CloseDate IN THIS_CALENDAR_QUARTER AND IsClosed=false`, bucketed by `Probability`, `sum(APTS_Renewal_ACV__c)`.
12. **Widget 12 - renewal_upcoming_list:** Create new SF report and dashboard widget: `Type='Renewal' AND IsClosed=false AND CloseDate IN THIS_CALENDAR_QUARTER`, columns: Account, Opp Name, `APTS_Renewal_ACV__c`, CloseDate, Probability, Owner.
13. **Widget 14 - slipped_deals_root_cause (interim):** Fix the fiscal filter on existing SF fallback `00OTb000008eknVMAQ` (LAST_FISCAL_QUARTER -> calendar equivalent). Label widget explicitly as "proxy -- see PI config Phase 2.5". Full PI native replacement is Phase 2.5.
14. **Widget 15 - slipped_deals_trend (interim):** No existing widget (MISSING). A placeholder dashboard widget may be added with label "Pending PI configuration (Phase 2.5)".
15. **Widget 16 - forecast_accuracy_snapshot (interim):** Label existing widgets `00OTb000008TZsDMAW` and `00OTb000008TZaTMAW` as fiscal-filter defects; deprioritize for deck use. Full PI native integration (`4c2Tb0000003jobIAA`) is Phase 4.

### Phase 2.5 follow-ups

Rows where Source ID is `needs_pi_config_change`:

- **Widget 14 - slipped_deals_root_cause:** Create a new `PipelineInspectionListView` in the SF Lightning UI with a slip-type filter. The PI schema does not expose a `SlipFilter` field via SOQL; the "Pipeline Changes - Slipped" construct is Lightning UI-only. Once configured, query the new list view ID and update this contract.
- **Widget 15 - slipped_deals_trend:** Create a new `PipelineInspectionListView` with a 26-week (6-month trailing) change period. Current `ChangePeriodLiteralType` options (`START_OF_THE_PERIOD`, `THIS_WEEK`) do not cover a 6-month window. Requires manual configuration in the SF Lightning UI.

### Phase 4 deck rebuild inputs

Every spec widget with a non-blank Source ID is a candidate for Phase 4 deck rendering. Status after Phase 1.5 and Phase 2.5 work is complete:

| Widget | Source ID | Ready for Phase 4? |
|---|---|---|
| 1 | `00OTb000008ektxMAA` | No - fix fiscal filter + wrong aggregation field first |
| 2 | needs_phase_1_5_build | No - must be created |
| 3 | needs_phase_1_5_build | No - must be created |
| 4 | needs_phase_1_5_build | No - must be created |
| 5 | needs_phase_1_5_build | No - must be created |
| 6 | `00OTb000008aTtJMAU` | Partial - report exists, dashboard widget must be created |
| 7 | `00OTb000008ekp7MAA` | No - fix co-matched WRONG-DATA widget first |
| 8 | `00OTb000008d6ovMAA` | Partial - report exists, dashboard widget must be created |
| 9 | `00OTb000008d6ovMAA` | Partial - report exists, dashboard widget must be created |
| 10 | `00OTb000008ekxBMAQ` | No - fix fiscal filter + wrong aggregation field first |
| 11 | needs_phase_1_5_build | No - must be created |
| 12 | needs_phase_1_5_build | No - must be created |
| 13 | (blank - finance feed pending) | No - separate workstream, not Phase 1.5 |
| 14 | needs_pi_config_change | No - Phase 2.5 PI config required |
| 15 | needs_pi_config_change | No - Phase 2.5 PI config required |
| 16 | `4c2Tb0000003jobIAA` | Partial - PI list view ID pinned; integration design + ForecastCategory split needed |

Confirm that all 16 spec widgets have a row in this contract before Phase 4 begins.
