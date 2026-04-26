# Sales Ops Page 5 Pipeline Hygiene Contract

This document turns the validated `Pipeline_Opportunity_Operations` seam into a build-ready Page 5 module for the quarterly Sales Ops dashboard.

It is narrower than [sales-ops-dashboard-implementation-contract.md](sales-ops-dashboard-implementation-contract.md) and should be read alongside the generated mutation package in [generated/sales_ops_page5_mutation_prep/README.md](generated/sales_ops_page5_mutation_prep/README.md).

## Current Reuse Strategy

- Reuse the live Sales Ops shell selector contract:
  - `f_fy`
  - `f_quarter`
  - `f_unit`
  - `f_region`
- Reuse the live shell chart and table shells where they already fit:
  - `p11_pastdue_owner`
  - `p11_pastdue_table`
- Keep the live shell hygiene page layout pattern, but remove the non-Page-5 content:
  - retire `Win/Loss Reason Fill Rate`
  - retire `Forecast Category Not Omitted (%)`
  - retire the mixed opportunity-data-quality framing

## Fields Proven In The Current Analytics Layer

- `OpportunityName`
- `AccountName`
- `OwnerName`
- `StageName`
- `ForecastCategory`
- `RecordType`
- `IsClosed`
- `FYLabel`
- `CloseQuarter`
- `UnitGroup`
- `SalesRegion`
- `WeightedOpenARR`
- `AtRiskARR`
- `DaysInStage`
- `PushCount`
- `PushDays`
- `PastDueCount`
- `StaleCount`
- `BackwardMoveCount`
- `MissingApprovalCount`
- `StageOrder`

## Keep From The Live Shell

These current shell patterns are still directly useful for Page 5:

| Existing Step | Existing Widget | Sales Ops Page 5 Reuse |
| --- | --- | --- |
| `s_p11_stale_pct` | `p11_kpi_stale` | `Stale Pipeline %` KPI logic |
| `s_p11_pastdue_cnt` | `p11_kpi_pastdue` | open past-due count logic |
| `s_p11_pastdue_owner` | `p11_pastdue_owner` | horizontal-bar shell for ranked hygiene pressure |
| `s_p11_pastdue_table` | `p11_pastdue_table` | record-level queue shell with record actions |

## Add Or Tighten

The live shell does not yet give Page 5 the right metric contract. These additions tighten it into a true pipeline-hygiene page.

### New KPI: `Stuck ARR`

Definition:

- sum `WeightedOpenARR` for open opportunities with any hygiene pressure flag:
  - `PastDueCount > 0`
  - `StaleCount > 0`
  - `BackwardMoveCount > 0`
  - `PushCount >= 2`

### Tightened KPI: `Past-Due Open Opportunity Count`

Build rule:

- keep the current `sum(PastDueCount)` pattern
- add an explicit `IsClosed == "false"` filter in the Page 5 contract instead of assuming closed rows contribute zero

### New KPI: `Push Count Pressure`

Definition:

- average `PushCount` across open opportunities

Why:

- the repo already uses slipped-deal metrics off `PushCount`
- average push count is safer and easier to explain than a synthetic score

### Diagnostic Chart: `Avg Days In Stage by Stage`

Definition:

- group by `StageName`
- show `avg(DaysInStage)`
- order by `avg_days_in_stage desc`

Why:

- this keeps the stage-aging story on the page without inventing unproven SLA or manager-rollup signals
- live March 31, 2026 query validation showed `StageOrder` was not currently discriminating in the dataset output, so ranking by average dwell is the safer v1 chart contract

### Diagnostic Chart: `Hygiene Pressure by Owner`

Definition:

- record-level hygiene flag when any of these are true:
  - `PastDueCount > 0`
  - `StaleCount > 0`
  - `BackwardMoveCount > 0`
  - `PushCount >= 2`
- sum that flag by `OwnerName`

### Queue: `Process Exceptions`

Filter:

- open opportunities where:
  - `PastDueCount > 0`
  - or `StaleCount > 0`
  - or `BackwardMoveCount > 0`

### Queue: `Repeat Push Opportunities`

Filter:

- open opportunities where `PushCount >= 2`
- sort by `WeightedOpenARR desc` so the queue resolves toward the highest-value repeated pushes first

## Ready/Blocked Split

- Ready now:
  - `Stale Pipeline %`
  - `Stuck ARR`
  - `Past-Due Open Opportunity Count`
  - `Push Count Pressure`
  - `Avg Days In Stage by Stage`
  - owner hygiene pressure ranking
  - process exception queue
  - repeat push queue
- Still blocked on additional dataset proof:
  - manager-level rollups
  - overdue-task rate
  - approval-aging metrics
  - approval-ARR rollups

## Build Rule

Page 5 should stay on proven pipeline-hygiene signals. Do not widen it back into approval-aging or overdue-task compliance just because those are conceptually adjacent.
