# Sales Ops Page 6 Action Queue Contract

This document turns Page 6 into a mutation-ready contract for the `Sales Ops Quarterly Dashboard`.

Page 6 is intentionally the operator page. It should end the dashboard in accountable action instead of another layer of summary charts.

## Purpose

- terminate the dashboard in accountable owner and manager action
- normalize the four queue families from Pages 2 through 5 into one page
- keep the page rooted in live record seams, not narrative-only rollups

## Source Seams

- `Account_Intelligence`
- `Commercial_Rhythm_Control_Tower`
- `Forecast_Revenue_Motions`
- `Pipeline_Opportunity_Operations`

## V1 Normalized Queue Contract

The old Page 6 note required `NextStep` on every queue row. The live datasets do not support that consistently across all four queue families.

V1 therefore normalizes on:

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
- `Id`

V1 normalization rules:

- `RootCause` is rule-based from live dataset signals, not free-text owner commentary.
- `RecommendedAction` is deterministic and action-oriented so the page still terminates in accountable follow-through.
- `ManagerName` is left blank where the live dataset does not prove a manager field.
- `CloseDate` is a contextual close field. For pipeline-operations rows it is populated from `CloseQuarter`, because the fresh live export does not prove a row-level `CloseDate` field.

## Queue Groups

### 1. Missing Data Remediation

- Source surfaces:
  - low-quality account seam from `Account_Intelligence`
  - closed opportunities missing won/lost reason from `Forecast_Revenue_Motions`
- Intent:
  - join the account-completeness queue and the closed-opportunity reason-code cleanup queue into one remediation surface
- Live proof:
  - `Account_Intelligence` exposes `Name`, `OwnerName`, `DataQualityBand`, `DataQualityScore`, `HasDUNS`, `HasUnitGroup`, `HasAxiomaId`, `KYCStatus`, and `ExpectedTerminationDate`
  - `Forecast_Revenue_Motions` exposes `OpportunityName`, `AccountName`, `OwnerName`, `ManagerName`, `ARR`, `StageName`, `CloseDate`, `AgeInDays`, and `WonLostReason`
- Priority rule:
  - account rows rank by missing core fields plus low data-quality score
  - opportunity rows rank by ARR, closed-stage severity, and aging

### 2. Commercial Process Remediation

- Source surface:
  - `Commercial_Rhythm_Control_Tower`
- Intent:
  - surface the review, ownership, and no-next-step backlog as one commercial-process queue
- Live proof:
  - current export proves `OpportunityName`, `AccountName`, `OppOwnerName`, `OppManagerName`, `StageProgression`, `CloseDate`, `OpenValue`, `HandoffState`, `ReviewPulse`, `LeadershipAsk`, `ReviewCandidateCount`, `OwnershipReviewCount`, and `NoNextStepCount`
- Priority rule:
  - rank rows by ownership-review need, leadership-review need, next-step gap, and open value
  - deduplicate by opportunity id before queue scoring so the page does not overcount month-level rhythm history

### 3. Forecast Risk Remediation

- Source surface:
  - `Forecast_Revenue_Motions`
- Intent:
  - collapse the Page 4 promotion and commit-protection logic into one operator queue
- Live proof:
  - current export and Page 4 validation already prove `ForecastCategory`, `StageProgression`, `CloseDate`, `ARR`, `AgeInDays`, `ReviewCandidateCount`, `NoNextStepFlag`, `CommercialApprovalFlag`, `StaleCommercialApprovalFlag`, and `OverdueTaskFlag`
- Priority rule:
  - rank rows by stalled approval, pending approval, overdue-task pressure, forecast category, missing next step, review-candidate status, ARR, and aging

### 4. Stale Pipeline Remediation

- Source surface:
  - `Pipeline_Opportunity_Operations`
- Intent:
  - collapse the Page 5 process-exception and repeat-push queues into one pipeline-action surface
- Live proof:
  - fresh shell export proves `OpportunityName`, `AccountName`, `OwnerName`, `StageName`, `CloseQuarter`, `WeightedOpenARR`, `DaysInStage`, `PastDueCount`, `StaleCount`, `BackwardMoveCount`, and `PushCount`
- Priority rule:
  - rank rows by past-due pressure, stale coverage, backward movement, repeat-push intensity, ARR, and days in stage

## Generated Mutation Package

Page 6 mutation prep lives in:

- [generated/sales_ops_page6_mutation_prep/README.md](generated/sales_ops_page6_mutation_prep/README.md)
- [generated/sales_ops_page6_mutation_prep/wave_patch_payload.json](generated/sales_ops_page6_mutation_prep/wave_patch_payload.json)
- [generated/sales_ops_page6_mutation_prep/step_contract.json](generated/sales_ops_page6_mutation_prep/step_contract.json)

## Live Target Validation

Validated read-only against `apro@simcorp.com` on March 31, 2026.

- Live shell label: `Sales Ops Data Quality & Forecast Accuracy`
- Live shell id: `0FKTb0000000K5BOAU`
- Live shell export:
  - `output/live_target_exports/sales_ops_dashboard_shell_2026-03-31/sales_ops_data_quality_forecast_accuracy`
- Materialization artifact:
  - `output/sales_ops_page6_mutation_prep_2026-03-31/materialized_from_contract/materialization_summary.json`
- Dry preview artifact:
  - `output/sales_ops_page6_mutation_prep_2026-03-31/deploy_preview_against_sales_ops_shell/wave_patch_request.json`
- Dry preview result:
  - `contract_violation_count: 0`
  - `page_count: 1`
  - `widget_count: 8`
  - `step_count: 8`
- Live SAQL validation artifact:
  - `output/sales_ops_page6_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json`

Unfiltered March 31, 2026 live snapshot from the exact Page 6 queries:

- `Missing Data Queue Count = 25`
- `Commercial Process Queue Count = 5,753`
- `Forecast Risk Queue Count = 86`
- `Stale Pipeline Queue Count = 701`
- missing-data queue top rows are low-quality accounts missing all core keys:
  - `NPL Markets`
  - `Client Reporting PreSales`
- commercial-process queue top rows currently resolve to real late-stage renewal follow-up:
  - `DDBO - Renewal 2026`
  - `GIB - Renewal 2026`
- forecast-risk queue top rows currently surface real intervention targets:
  - `Sony Life - F2B`
  - `QNB - F2B`
- stale-pipeline queue top rows currently surface real repeat-push / past-due pressure:
  - `DB - Enterprise eLearning`
  - `EPA - License Renewal`

## Guardrails

- Keep all queue ordering on a single deterministic `PriorityScore desc` sort.
- Do not add `columnMap` to comparison tables.
- Keep number widgets on the minimal `step` plus `measureField` contract only.
- Do not widen Page 6 into free-text owner commentary until there is a separate collection workflow for slipped-deal and churn commentary.
