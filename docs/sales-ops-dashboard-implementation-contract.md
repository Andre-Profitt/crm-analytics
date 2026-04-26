# Sales Ops Dashboard Implementation Contract

This document turns Report 2 into a repo-native dashboard build contract.

It is intentionally tighter than the high-level reporting spec in
[sales-director-and-sales-ops-reporting-spec.md](sales-director-and-sales-ops-reporting-spec.md).

Detailed Page 2/Page 3 step reuse map lives in
[sales-ops-page23-step-reuse-contract.md](sales-ops-page23-step-reuse-contract.md).

Detailed Page 3 process-compliance mutation contract lives in
[sales-ops-page3-process-compliance-contract.md](sales-ops-page3-process-compliance-contract.md).

Detailed Page 4 forecast-accuracy mutation contract lives in
[sales-ops-page4-forecast-accuracy-contract.md](sales-ops-page4-forecast-accuracy-contract.md).

Detailed Page 5 pipeline-hygiene map lives in
[sales-ops-page5-pipeline-hygiene-contract.md](sales-ops-page5-pipeline-hygiene-contract.md).

Detailed Page 6 action-queue contract lives in
[sales-ops-page6-action-queue-contract.md](sales-ops-page6-action-queue-contract.md).

The goal is a quarterly Sales Ops CRM Analytics dashboard that can later feed a shorter PowerPoint readout.

## Build Posture

- Primary build target: CRMA dashboard.
- Secondary output: quarterly PowerPoint built from the dashboard.
- Build path: direct Wave API calls and `sf` CLI only.
- Legacy Python builder files are not part of this contract.
- Current source surfaces stay the system of record:
  - `Forecast & Revenue Motions`
  - `Executive Revenue Source Truth`
  - `Account Intelligence KPIs`
  - `Commercial Rhythm Control Tower`
- Historical deleted dashboards are pattern references only, not build targets.

## Source Evidence

- [sales-director-and-sales-ops-reporting-spec.md](sales-director-and-sales-ops-reporting-spec.md)
- [DASHBOARD_PAGE_PLAN.md](DASHBOARD_PAGE_PLAN.md)
- [audit_forecast_revenue_motions.py](../scripts/audit_forecast_revenue_motions.py)
- [audit_commercial_rhythm_control_tower.py](../scripts/audit_commercial_rhythm_control_tower.py)
- [forecast_revenue_motions audit](../output/autopilot/runs/quality_refresh_2026-03-11T07-29-25/forecast_revenue_motions/audit/audit.md)
- [executive_revenue_source_truth audit](../output/autopilot/runs/quality_refresh_2026-03-11T07-29-25/executive_revenue_source_truth/audit/audit.md)
- [account_intelligence audit](../output/autopilot/runs/continuous_full_2026-03-11T05-51-40/account_intelligence/audit/audit.md)
- [sales_process_compliance_kpis deleted backup](generated/deleted_dashboard_backups/andre_profitt_reset_2026-03-10T20-37-32Z/sales_process_compliance_kpis_0fktb0000000ietoau/dashboard.json)

## Live Validation Snapshot

Validated read-only against `apro@simcorp.com` on March 31, 2026.

### Opportunity Field Contract

- Confirmed live fields for process/compliance work:
  - `Approval_Status__c`
  - `NextStep`
  - `Reason_Won_Lost__c`
  - `Risk_Assessment_Level__c`
  - `Risk_Assessment_Comment__c`
  - `Sales_Cycle_Duration__c`
  - `Stage_20_Approval__c`
  - `Stage_20_Approval_Date__c`
  - `Submit_for_Stage_20_Review__c`
  - `Submit_for_Stage_20_Review_Date__c`
- Confirmed forecast/pipeline support fields:
  - `CloseDate`
  - `CreatedDate`
  - `ForecastCategory`
  - `StageName`
  - `HasOverdueTask`
- Important limitation:
  - `HasOverdueTask` exists, but Salesforce does not allow filtering on it in a direct SOQL query call.
  - Overdue-task compliance therefore needs to come from the analytics dataset layer, not raw object filtering.

### Account Field Contract

- Confirmed live fields for data quality and account controls:
  - `DUNS_No__c`
  - `Unit_Group__c`
  - `KYC_Approval_Status__c`
  - `KYC_Approval_Date__c`
  - `KYC_Approval_Expiry_Date__c`
  - `Risk_of_Potential_Termination__c`
  - `Termination_Risk_Last_Updated__c`

### Current Baseline Counts

- Open opportunities: `1,760`
- Open opportunities missing `NextStep`: `1,221` (`69.4%`)
- Open opportunities with `Risk_Assessment_Level__c`: `200` (`11.4%`)
- Current open stage mix:
  - `2 - Discovery`: `747`
  - `3 - Engagement`: `425`
  - `1 - Prospecting`: `371`
  - `4 - Shortlisted`: `113`
  - `5 - Preferred`: `49`
  - `6 - Contracting`: `43`
  - `Quota`: `12`
- Current open opportunity approval status:
  - `Approval_Status__c = No Approval Necessary`: `1,760`
- Open opportunities already marked with commercial approval:
  - `3 - Engagement`: `33`
  - `4 - Shortlisted`: `21`
  - `2 - Discovery`: `8`
  - `5 - Preferred`: `4`
  - `6 - Contracting`: `2`
- Open opportunities already marked as submitted for approval:
  - `3 - Engagement`: `38`
  - `4 - Shortlisted`: `20`
  - `2 - Discovery`: `9`
  - `5 - Preferred`: `3`
  - `6 - Contracting`: `2`
- Current late-stage compliance baseline using the live stage model `3 - Engagement` through `6 - Contracting`:
  - open late-stage opportunities: `629`
  - late-stage opportunities with commercial approval: `60` (`9.5%`)
  - late-stage opportunities submitted for approval: `63` (`10.0%`)
  - late-stage opportunities missing `NextStep`: `400` (`63.6%`)
- Open opportunities missing `NextStep` by stage:
  - `2 - Discovery`: `533`
  - `1 - Prospecting`: `277`
  - `3 - Engagement`: `259`
  - `4 - Shortlisted`: `70`
  - `5 - Preferred`: `36`
  - `6 - Contracting`: `35`
  - `Quota`: `12`
- Accounts with `DUNS_No__c`: `10,282 / 13,543` (`75.9%`)
- Accounts with `Unit_Group__c`: `13,458 / 13,543` (`99.4%`)
- Accounts with `KYC_Approval_Status__c`: `13,492 / 13,543` (`99.6%`)
- Closed opportunities with `Reason_Won_Lost__c`: `16,063 / 46,079` (`34.9%`)

### What The Snapshot Means

- Data-quality completeness is strong enough to build now.
- `NextStep` hygiene is weak enough to justify a first-class KPI and queue immediately.
- Commercial approval fields are live, but `Approval_Status__c` is not currently useful as the primary open-pipeline compliance signal.
- The live org no longer uses the old stage labels from the deleted process dashboard.
- Any approval-eligibility rule must be written against current stages like `3 - Engagement`, `4 - Shortlisted`, `5 - Preferred`, and `6 - Contracting`.

## Analytics Dataset Validation

This section separates fields that are confirmed in current analytics exports from fields that are still only audit expectations or historical patterns.

### Account Intelligence

- Validated current dataset metadata:
  - `OwnerName`
  - `UnitGroup`
  - `HasDUNS`
  - `HasUnitGroup`
  - `KYCStatus`
  - `DataQualityBand`
  - `RiskLevel`
  - `DataQualityScore`
- Validated current dashboard steps using those fields:
  - `s_duns_rate`
  - `s_unitgroup_rate`
  - `s_bullet_dq`
  - `s_dq_poor_list`
  - `s_kyc_funnel`
  - `s_kyc_detail`
- Implication:
  - Page 2 can be built directly from the current `Account_Intelligence` dataset and dashboard patterns.

### Forecast & Revenue Motions

- Validated current dataset metadata:
  - `OwnerName`
  - `StageName`
  - `RiskLevel`
  - `RiskBand`
  - `QuotaAmount`
  - `AgeInDays`
  - `RiskScore`
  - `RenewalRiskARR`
  - `RenewalRiskCount`
  - `RiskyCommitARR`
- Validated current dashboard steps using those fields:
  - `s_summary`
  - `s_owner_confidence`
  - `s_owner_gap`
  - `s_top_forecast_risk`
  - `s_top_renewals`
- Not found in the current live forecast export:
  - `ManagerName`
  - `DaysInStage`
  - `PushCount`
  - `OverdueTaskFlag`
  - `CommercialApprovalAgeDays`
  - `PendingApprovalCount`
  - `StaleApprovalCount`
  - `PendingApprovalARR`
  - `ProcessPressureScore`
- Implication:
  - Page 4 is buildable now from the existing forecast surface.
  - Page 3 cannot assume the stricter process-compliance signals already exist in the current forecast dataset just because the audit script expects them.

### Commercial Rhythm Control Tower

- Validated current dataset metadata:
  - `OppOwnerName`
  - `OppManagerName`
  - `OppOwnerPersona`
  - `OppOwnershipAlignment`
  - `HandoffState`
  - `ReviewPulse`
  - `LeadershipAsk`
  - `NoNextStepCount`
  - `OwnershipReviewCount`
  - `ReviewCandidateCount`
  - `OpenValue`
  - `CoveredRenewalOppCount`
  - `RenewalOppCount`
  - `AtRiskRenewalValue`
  - `ZeroValueRenewalCount`
- Validated current dashboard step patterns:
  - owner/manager renewal semantic-confidence table
  - open review queue with `HandoffState`, `ReviewPulse`, and `LeadershipAsk`
  - ownership review and forecast-hygiene ranking
- Live Page 3 query validation on March 31, 2026 confirmed:
  - open rhythm denominator: `1,657`
  - `Renewal Ownership Alignment % = 76.92`
  - `Next Step Coverage % = 32.65`
  - `Forecast Hygiene Candidate Count = 1,380`
  - `Ownership Review Count = 66`
  - top manager hygiene pressure:
    - `Christian Ebbesen = 194`
    - `Alexander Schnitzler = 137`
    - `Niklas Salminen = 119`
- Implication:
  - Page 3 already has strong owner/manager, handoff, and next-step coverage signals in the rhythm dataset.
  - Renewal semantic confidence should come from this dataset, not be rebuilt from scratch.
  - Page 3 now has a mutation-prep package and a clean dry preview path, not just a reuse contract.

### Pipeline Hygiene Dataset Status

- Fresh live shell export captured on March 31, 2026 from:
  - `Sales Ops Data Quality & Forecast Accuracy`
  - dashboard id `0FKTb0000000K5BOAU`
  - export path `output/live_target_exports/sales_ops_dashboard_shell_2026-03-31/sales_ops_data_quality_forecast_accuracy`
- Confirmed current `Pipeline_Opportunity_Operations` XMD fields:
  - `DaysInStage`
  - `PushCount`
  - `PushDays`
  - `PastDueCount`
  - `StaleCount`
  - `BackwardMoveCount`
  - `MissingApprovalCount`
  - `OwnerName`
  - `StageName`
  - `StageOrder`
  - `StageSlaDays`
  - `SlipRiskScore`
  - `TotalRiskScore`
  - `WeightedOpenARR`
  - `AtRiskARR`
  - `CloseQuarter`
  - `FYLabel`
  - `ForecastCategory`
  - `SalesRegion`
  - `UnitGroup`
- Still not proven in the fresh shell export:
  - `ManagerName`
  - `OverdueTaskFlag`
  - `CommercialApprovalAgeDays`
  - `PendingApprovalARR`
- Live Page 5 query validation on March 31, 2026 confirmed:
  - `Stale Pipeline % = 21.12`
  - `Stuck ARR = 138,276,413.84`
  - `Past-Due Open Opportunity Count = 62`
  - `Push Count Pressure = 1.50`
  - `StageOrder` is not currently discriminating for stage-aging ranking, so Page 5 should order that chart by `avg_days_in_stage desc` until a better sort key is proven
- Implication:
  - Page 5 is no longer blocked on missing live-export evidence for the core pipeline-hygiene dataset.
  - Page 5 now has a mutation-prep package and dry preview path, not just a conceptual contract.
  - Page 3 approval-aging and overdue-task metrics still need additional dataset proof.

## Build Principles

- Use current live stage labels, not deleted-dashboard labels.
- Derive compliance from proven fields and analytics signals, not from missing helper fields like `Deal_Cycle_Compliant__c`.
- Keep record-level owner/action queues on every page.
- Reuse live dashboards where they already solve the problem.
- Only create new steps where the current source surfaces do not already expose the KPI cleanly.

## Page Contract

### Page 1: Quarterly Sales Ops Summary

- Purpose:
  - one-page operating scoreboard for the quarter
  - quick read on data quality, process compliance, forecast accuracy, and hygiene
- Source surfaces:
  - `Account Intelligence KPIs`
  - `Forecast & Revenue Motions`
  - `Executive Revenue Source Truth`
  - `Commercial Rhythm Control Tower`
- V1 headline KPIs:
  - `Data Completeness Score`
  - `Process Compliance Rate`
  - `Forecast Accuracy`
  - `Pipeline Hygiene Rate`
  - `Top Exception Queue Count`
- V1 contract:
  - `Data Completeness Score` is a simple scorecard rollup from Page 2 metrics.
  - `Process Compliance Rate` is a simple scorecard rollup from Page 3 metrics.
  - `Forecast Accuracy` should reuse the existing forecast-accuracy logic from the forecast source layer.
  - `Pipeline Hygiene Rate` is a simple scorecard rollup from Page 5 metrics.
- Required action surface:
  - one compact manager queue showing the highest-priority issues across pages 2-5.
- Status:
  - buildable now
  - exact Page 1 mutation prep is now tracked in [sales-ops-page1-quarterly-summary-contract.md](sales-ops-page1-quarterly-summary-contract.md) and [generated/sales_ops_page1_mutation_prep/README.md](generated/sales_ops_page1_mutation_prep/README.md)

### Page 2: CRM Data Quality

- Purpose:
  - measure completeness by field and by owner
  - expose fix queues instead of only completeness percentages
- Primary source surface:
  - `Account Intelligence KPIs`
- Confirmed live fields:
  - `DUNS_No__c`
  - `Unit_Group__c`
  - `KYC_Approval_Status__c`
  - `Reason_Won_Lost__c`
- V1 KPIs:
  - `DUNS Coverage %`
  - `Unit Group Coverage %`
  - `KYC Status Coverage %`
  - `Closed Won/Lost Reason Coverage %`
- V1 queues:
  - accounts missing DUNS
  - accounts missing Unit Group
  - accounts missing KYC status
  - closed opportunities missing won/lost reason
- V1 note:
  - this page is completeness-first
  - "accuracy" should be treated as a later contract unless a trusted comparison source is defined
- Status:
  - buildable now
  - exact `bundle -> materialize -> deploy preview` against the Sales Ops shell is now clean with `contract_violation_count: 0`
  - Page 2 is intentionally narrowed to the current shell selector plane:
    - `f_unit` for the Account Intelligence seam
    - `f_fy`, `f_region`, `f_unit` for the closed won/lost-reason seam

### Page 3: Process Compliance

- Purpose:
  - show whether the commercial process is being followed
  - expose approval, next-step, and task hygiene gaps
- Primary source surfaces:
  - `Commercial Rhythm Control Tower`
  - `Forecast & Revenue Motions`
- Confirmed live object fields:
  - `Stage_20_Approval__c`
  - `Stage_20_Approval_Date__c`
  - `Submit_for_Stage_20_Review__c`
  - `Submit_for_Stage_20_Review_Date__c`
  - `NextStep`
  - `HasOverdueTask`
  - `Sales_Cycle_Duration__c`
- Required dataset-level derived signals:
  - `CommercialApprovalAgeDays`
  - `OverdueTaskFlag`
  - `ProcessPressureScore`
  - approval-eligibility flag based on current stage model
- Historical step patterns to reuse:
  - approval-rate pattern from `s_bullet_approval`
  - stage-aging and stuck-deal patterns from `s_stg_aging`, `s_stuck_cnt`, `s_stuck_arr`, and `s_stuck_detail`
  - process-table expectations from [audit_forecast_revenue_motions.py](../scripts/audit_forecast_revenue_motions.py)
- V1 KPIs:
  - `Approval Submitted Coverage %`
  - `Approval Completed Coverage %`
  - `Next Step Coverage %`
  - `Open Deals With Overdue Tasks`
  - `Median Approval Age Days`
- V1 queues:
  - approval-required candidates missing submission
  - submitted approvals aging beyond threshold
  - open opportunities missing next step
  - open opportunities with overdue tasks
- Open contract questions:
  - exact approval-eligibility business rule
  - whether stage `2 - Discovery` belongs in the approval denominator
  - aging thresholds for "stale approval"
- Status:
  - buildable now as a derived-signal page
  - exact `bundle -> materialize -> deploy preview` against the Sales Ops shell is now clean with `contract_violation_count: 0`
  - Page 3 is intentionally narrowed to the current shell selector plane:
    - `f_fy`
    - `f_region`
  - not safe to define as a raw-field-only page
  - should reuse current `Commercial_Rhythm_Control_Tower` owner/manager semantics and only add forecast-side signals after they are actually present in a live dataset export

### Page 4: Forecast Accuracy

- Purpose:
  - show whether the forecast is trustworthy
  - separate quarter-close accuracy from current-quarter promotion risk
- Primary source surfaces:
  - `Forecast & Revenue Motions`
  - `Executive Revenue Source Truth`
- Required dataset layer:
  - weekly forecast snapshots, not raw object point-in-time fields
- V1 KPIs:
  - `Prior Quarter Forecast Accuracy %`
  - `Commit Change WoW`
  - `Best Case Gap ARR`
  - `Low Confidence Pipeline ARR`
  - `Promotion Need %`
- V1 queues:
  - low-confidence pipeline needing promotion
  - top forecast risk deals
  - commit protection list
- Rule:
  - do not rebuild forecast accuracy from raw Opportunity fields if the existing weekly forecast layer already provides it
- Status:
  - buildable now by reusing current live forecast source surfaces
  - mutation-prep artifacts now live in [generated/sales_ops_page4_mutation_prep/README.md](generated/sales_ops_page4_mutation_prep/README.md)
  - dry preview against the Sales Ops shell is clean with `contract_violation_count: 0`
  - exact `bundle -> materialize -> deploy preview` against the Sales Ops shell is clean with `contract_violation_count: 0`
  - live March 31, 2026 snapshot currently shows:
    - `Forecast Confidence % = 65.46`
    - `Commit Change WoW = -89,981.21`
    - `Needed Promotion ARR = 0`
    - `Promotion Need % = 0`
    - `Low Confidence Forecast ARR = 17,162,125.52`
  - the rep pressure queue now sorts by a composite `PromotionPressureScore` because the global quarter snapshot is already over-covered on promotion need
  - region-compatible historical calibration remains a later seam, not part of this first mutation slice

### Page 5: Pipeline Hygiene

- Purpose:
  - measure aging, stuck pipeline, past-due close dates, and stage progression quality
- Primary source surfaces:
  - `Forecast & Revenue Motions`
  - `Pipeline_Analytics`
  - `Pipeline_Opportunity_Operations`
- Required dataset signals:
  - `DaysInStage` or `DaysInCurrentStage`
  - `PushCount`
  - `CloseDate`
  - `StageName`
  - `ARR`
- Historical patterns to reuse:
  - stage aging bands from `s_stg_aging`
  - owner-level stuck queue from `s_stuck_by_owner`
  - stuck ARR and stuck count scorecards from `s_stuck_arr` and `s_stuck_cnt`
- V1 KPIs:
  - `Stale Pipeline %`
  - `Stuck ARR`
  - `Past-Due Open Opportunity Count`
  - `Avg Days In Stage`
  - `Push Count Pressure`
- V1 working thresholds:
  - stale pipeline should start with the deleted-dashboard heuristic: `DaysInStage > 30`
  - prioritize stages `3 - Engagement` and `4 - Shortlisted` first
  - graduate to stage-specific thresholds later if the data supports it
- V1 queues:
  - stuck opportunities
  - past-due close-date opportunities
  - repeated-push opportunities
- Status:
  - core hygiene slice now buildable from the fresh `Pipeline_Opportunity_Operations` export
  - mutation-prep artifacts now live in [generated/sales_ops_page5_mutation_prep/README.md](generated/sales_ops_page5_mutation_prep/README.md)
  - exact `bundle -> materialize -> deploy preview` against the Sales Ops shell is clean with `contract_violation_count: 0`
  - manager-rollup, overdue-task, and approval-aging refinements still need additional dataset proof

### Page 6: Action Queue And Root Causes

- Purpose:
  - terminate the dashboard in accountable action
  - avoid a dashboard that is only rollups and color
- Source surfaces:
  - Page 2 queues
  - Page 3 queues
  - Page 4 queues
  - Page 5 queues
- Required queue columns:
  - `AccountName`
  - `OpportunityName`
  - `OwnerName`
  - `ManagerName`
  - `ARR`
  - `StageName`
  - `CloseDate`
  - `DaysInStage`
  - `RootCause`
  - `RecommendedAction`
  - `PriorityScore`
- Sorting rule:
  - every primary queue should resolve to one deterministic priority score
  - avoid brittle multi-column sort logic
- V1 queue groups:
  - missing-data remediation
  - approval/commercial-process remediation
  - forecast-risk remediation
  - stale-pipeline remediation
- Status:
  - mutation-prep package now defined
  - dry preview clean against the Sales Ops shell
  - exact `bundle -> materialize -> deploy preview` against the Sales Ops shell is clean with `contract_violation_count: 0`
  - live SAQL validation complete on all eight Page 6 queries

## Combined State Readiness

Validated read-only on March 31, 2026 by merging the exact materialized Page 1-6 modules into one candidate dashboard state.

- Merge artifact:
  - `output/sales_ops_pages_1_6_combined_2026-03-31/merge/dashboard_state.merged.json`
- Merge summary:
  - `output/sales_ops_pages_1_6_combined_2026-03-31/merge/merge_summary.json`
- Combined dry preview artifact:
  - `output/sales_ops_pages_1_6_combined_2026-03-31/deploy_preview/wave_patch_request.json`
- Combined dry preview result:
  - `page_count: 6`
  - `widget_count: 49`
  - `step_count: 54`
  - `contract_violation_count: 0`

What this means:

- Pages 1-6 now exist as one merged, contract-clean dashboard candidate state.
- The repo no longer depends on isolated per-page previews only.
- No live PATCH has been run yet.

## Explicit Non-Goals For V1

- No attempt to rebuild a full process page from missing helper fields.
- No assumption that `Approval_Status__c` alone defines approval compliance.
- No attempt to define CRM "accuracy" without a trusted comparison source.
- No direct reuse of deleted dashboard JSON as production JSON.

## Immediate Build Sequence

1. Decide whether the first live apply should ship the merged Pages 1-6 state as-is or preserve more shell chrome first.
2. Pin the approval-eligibility rule against the live stage model before widening the process-compliance seam.
3. Decide what comparison source should define CRM "accuracy" beyond completeness.
4. Keep approval-aging, overdue-task, and manager-rollup refinements behind fresh dataset proof.

## Next Open Questions

- Which exact open stages should count as commercial-approval eligible?
- Is `2 - Discovery` intentionally part of the approval workflow, or just noisy data?
- What threshold should define a stale pending approval?
- What comparison source should define CRM "accuracy" beyond completeness?
- Where is the current live export for `Pipeline_Opportunity_Operations` or `Pipeline_Analytics`, and does it still expose `DaysInStage` and `PushCount` under those exact names?
