# Sales Ops Page 2 And Page 3 Step Reuse Contract

This document turns the validated Sales Ops dashboard seams into a build-ready reuse map for:

- Page 2: `CRM Data Quality`
- Page 3: `Process Compliance`

It is narrower than [sales-ops-dashboard-implementation-contract.md](sales-ops-dashboard-implementation-contract.md).

The goal here is simple:

- reuse existing dashboard steps where they already solve the KPI cleanly
- define only the missing steps we still need
- separate "ready now" from "blocked on missing dataset export"

## Current Reuse Strategy

- Page 2 should primarily reuse `Account_Intelligence`.
- Page 2 should add one small opportunity-quality slice from `Forecast_Revenue_Motions` for won/lost reason coverage.
- Page 3 should primarily reuse `Commercial_Rhythm_Control_Tower`.
- Page 3 should not pretend the stricter approval and overdue-task signals already exist in current analytics exports.

## Page 2: CRM Data Quality

### Reuse Now

These steps already exist in the live exported `Account Intelligence KPIs` dashboard and are directly reusable.

| Existing Step | Current Widget | Current Page | Current Viz | Sales Ops Reuse |
| --- | --- | --- | --- | --- |
| `s_duns_rate` | `p1_g_duns` | `Data Quality` | `gauge` | `DUNS Coverage %` KPI |
| `s_unitgroup_rate` | `p1_g_unit` | `Data Quality` | `gauge` | `Unit Group Coverage %` KPI |
| `s_bullet_dq` | `p7_bullet_dq` | `Statistical Analysis` | `bullet` | `Data Completeness Score` or `Avg Data Quality Score` KPI |
| `s_dq_poor_list` | `p1_tbl_poor` | `Data Quality` | `comparisontable` | low-quality account remediation queue |
| `s_kyc_funnel` | `p2_ch_funnel` | `KYC Pipeline` | `funnel` | `KYC Status Coverage` distribution |
| `s_kyc_detail` | `p2_tbl_kyc` | `KYC Pipeline` | `comparisontable` | KYC remediation queue |

### Page 2 Fields Already Proven In The Analytics Layer

- `OwnerName`
- `UnitGroup`
- `HasDUNS`
- `HasUnitGroup`
- `KYCStatus`
- `DataQualityBand`
- `RiskLevel`
- `DataQualityScore`

### Add New

The current account dashboard does not cover closed-opportunity won/lost reason completeness. That should come from `Forecast_Revenue_Motions`, which already exposes:

- `WonLostReason`
- `IsClosed`
- `IsWon`
- `OwnerName`
- `StageName`
- `CloseDate`
- `FYLabel`

#### Proposed Step: `sales_ops_closed_reason_rate`

Purpose:

- `Closed Won/Lost Reason Coverage %`

Proposed SAQL:

```saql
q = load "Forecast_Revenue_Motions";
q = filter q by RecordType == "detail";
q = filter q by IsClosed == "true";
q = filter q by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q = filter q by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q = foreach q generate (case when WonLostReason != "" then 1 else 0 end) as has_reason;
q = group q by all;
q = foreach q generate (sum(has_reason) / count()) * 100 as fill_rate;
```

#### Proposed Step: `sales_ops_closed_reason_missing_list`

Purpose:

- queue of closed opportunities missing `WonLostReason`

Proposed SAQL:

```saql
q = load "Forecast_Revenue_Motions";
q = filter q by RecordType == "detail";
q = filter q by IsClosed == "true";
q = filter q by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q = filter q by {{coalesce(column(f_fy.selection, ["FYLabel"]), column(f_fy.result, ["FYLabel"])).asEquality('FYLabel')}};
q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q = filter q by WonLostReason == "";
q = group q by (OpportunityName, AccountName, OwnerName, StageName, CloseDate, Id);
q = foreach q generate OpportunityName, AccountName, OwnerName, StageName, CloseDate, Id;
q = order q by CloseDate desc;
q = limit q 25;
```

### Page 2 Ready/Blocked Split

- Ready now:
  - DUNS coverage
  - Unit Group coverage
  - KYC status coverage/distribution
  - low-quality account queue
  - KYC remediation queue
- Needs only small new step work:
  - won/lost reason coverage
  - closed-opportunity missing-reason queue

## Page 3: Process Compliance

### Reuse Now

These steps already exist in the current `Commercial_Rhythm_Control_Tower` export and are directly reusable.

| Existing Step | Current Widget | Current Page | Current Viz | Sales Ops Reuse |
| --- | --- | --- | --- | --- |
| `summary_actual_ownership_alignment_1` | `summary_headline_story_1` | `summary` | `number` | renewal ownership / semantic alignment headline KPI |
| `summary_variance_driver_forecast_hygiene_2` | `summary_headline_story_2` | `summary` | `hbar` | forecast hygiene pressure by manager |
| `ownership_handoffs_variance_driver_forecast_hygiene_1` | `ownership_handoffs_diagnostic_breakdown_1` | `ownership_handoffs` | `hbar` | forecast hygiene pressure by owner/persona |
| `ownership_handoffs_risk_renewal_semantic_confidence_2` | `ownership_handoffs_diagnostic_breakdown_2` | `ownership_handoffs` | `comparisontable` | renewal semantic-confidence table |
| `process_quality_action_queue_handoff_quality_1` | `process_quality_action_layer_1` | `process_quality` | `comparisontable` | owner/accountable action queue |

### Page 3 Fields Already Proven In The Analytics Layer

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

### What These Current Steps Already Give Us

- owner-manager handoff quality
- renewal semantic confidence
- next-step pressure through `NoNextStepCount`
- manager and owner-level hygiene rankings
- record-context action queue with:
  - `OpportunityName`
  - `AccountName`
  - `OwnerName`
  - `Persona`
  - `HandoffState`
  - `ReviewPulse`
  - `LeadershipAsk`

### Current Live Org Baseline For The Missing Compliance Slice

Using the live open stage model `3 - Engagement` through `6 - Contracting`:

- open late-stage opportunities: `629`
- late-stage opportunities with commercial approval: `60` (`9.5%`)
- late-stage opportunities submitted for approval: `63` (`10.0%`)
- late-stage opportunities missing `NextStep`: `400` (`63.6%`)

This means the first process page can already tell a strong story even before the missing approval-aging signals land:

- ownership/handoff quality is weak
- next-step hygiene is weak
- approval coverage is currently low on the late-stage denominator

### Still Missing In Current Analytics Exports

These fields are not present in the current validated forecast or rhythm dataset exports:

- `CommercialApprovalAgeDays`
- `OverdueTaskFlag`
- `ProcessPressureScore`
- `PendingApprovalCount`
- `StaleApprovalCount`
- `PendingApprovalARR`
- `DaysInStage`
- `PushCount`

### Page 3 Build Rule

Do not fabricate these fields in the dashboard layer.

For Page 3, use a split approach:

1. Ship the current rhythm-backed slice first:
   - ownership alignment
   - forecast hygiene
   - renewal semantic confidence
   - handoff/action queue
   - next-step pressure
2. Add the approval and overdue-task slice only after a fresh analytics export proves where those fields now live.

### Likely New Step Names Once The Missing Signals Exist

- `sales_ops_approval_submitted_rate`
- `sales_ops_approval_completed_rate`
- `sales_ops_pending_approval_age`
- `sales_ops_overdue_task_rate`
- `sales_ops_process_pressure_queue`

These should not be implemented until the source dataset contract is real.

## Recommended Immediate Build Order

1. Build Page 2 first.
2. Build the rhythm-backed portion of Page 3 second.
3. Treat approval-aging and overdue-task compliance as a separate follow-on slice.
4. Do not start Page 5 until a fresh pipeline-operations export is captured.

## Bottom Line

- Page 2 is effectively build-ready now.
- Page 3 is partially build-ready now.
- The usable "continue" path is:
  - reuse existing Page 2 and Page 3 rhythm steps
  - add the two won/lost-reason steps
  - defer approval-aging and overdue-task metrics until the missing dataset export is refreshed
