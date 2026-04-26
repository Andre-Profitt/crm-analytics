# Sales Ops Page 4 Forecast Accuracy Contract

This document turns the fresh March 31, 2026 `Forecast & Revenue Motions` export into a build-ready Page 4 module for the quarterly Sales Ops dashboard.

It is narrower than [sales-ops-dashboard-implementation-contract.md](sales-ops-dashboard-implementation-contract.md) and should be read alongside the generated mutation package in [generated/sales_ops_page4_mutation_prep/README.md](generated/sales_ops_page4_mutation_prep/README.md).

## Current Reuse Strategy

- Keep the page on shell-supported selectors only:
  - `f_fy`
  - `f_quarter`
  - `f_region`
  - `f_unit`
- Reuse the current live `Forecast & Revenue Motions` visual shells where they already solve the page cleanly:
  - `p1_ch_bridge`
  - `p5_ch_timeline`
  - `p4_tbl_owner`
  - `p4_tbl_forecast`
  - `p4_tbl_commit`
- Rebuild the KPI tiles as minimal number widgets:
  - do not copy the live forecast number widgets because they carry `compact`, `title`, and other parameters that are blocked by the repo PATCH rules
- Keep the page focused on the proven forecast seams:
  - quarter coverage bridge
  - weekly forecast mix
  - rep promotion pressure
  - forecast-risk queue
  - commit-protection queue

## Fields Proven In The Current Analytics Layer

Validated from the fresh live export at `output/live_target_exports/forecast_revenue_motions_2026-03-31/forecast_revenue_motions`.

### `Forecast_Revenue_Motions`

- `ActualARR`
- `AgeInDays`
- `ARR`
- `CloseQuarter`
- `CommercialApprovalAgeDays`
- `CommercialApprovalFlag`
- `FYLabel`
- `ForecastCategory`
- `ManagerName`
- `NeedsReviewOwnershipARR`
- `NeedsReviewOwnershipCount`
- `NextStep`
- `NoNextStepFlag`
- `OverdueTaskFlag`
- `OwnerName`
- `QuotaAmount`
- `RecordType`
- `RiskScore`
- `RiskyCommitARR`
- `SalesRegion`
- `StageProgression`
- `StaleCommercialApprovalFlag`
- `UnitGroup`
- `WeightedOpenARR`

### `Weekly_Forecast_Summary`

- `ARRDeltaWoW`
- `CloseQuarter`
- `CurrentWeekFlag`
- `FYLabel`
- `ForecastCategory`
- `ManagerName`
- `OppCount`
- `OppCountDeltaWoW`
- `OwnerName`
- `PreviousWeekFlag`
- `SalesRegion`
- `TotalARR`
- `UnitGroup`
- `Week`
- `WeekEndDate`
- `WeekIndex`

## Keep From The Live Forecast Surface

These current live steps already give Page 4 most of the shape it needs:

| Existing Step | Existing Widget | Sales Ops Page 4 Reuse |
| --- | --- | --- |
| `s_plan_bridge` | `p1_ch_bridge` | quarter coverage bridge |
| `s_wow_timeline` | `p5_ch_timeline` | weekly forecast mix line |
| `s_owner_gap` | `p4_tbl_owner` | rep promotion and hygiene pressure queue |
| `s_top_forecast_risk` | `p4_tbl_forecast` | promotion-candidate / top forecast-risk queue |
| `s_top_commit_protection` | `p4_tbl_commit` | commit-protection queue |

## Add Or Tighten

The current live forecast dashboard already has the right charts and queues, but the headline KPI row needs to be rebuilt for the Sales Ops dashboard.

### New KPI: `Forecast Confidence %`

Definition:

- numerator: `ActualARR + WeightedOpenARR`
- denominator: `ActualARR + Commit ARR + Best Case ARR + Pipeline ARR`
- implementation:
  - reuse the `s_summary` bridge logic
  - keep the result on the current-quarter forecast mix, not a historical calibration proxy

Why:

- the live forecast surface already treats this as the cleanest top-line trust signal
- it stays compatible with the shell region selector, unlike the current shell `Forecast_Intelligence` accuracy KPI

### New KPI: `Commit Change WoW`

Definition:

- `Current commit ARR - Previous week commit ARR`
- implementation:
  - reuse the live weekly summary logic from `s_wow_commit`

Why:

- this gives the page a true weekly movement signal without rebuilding from raw opportunities

### New KPI: `Needed Promotion ARR`

Definition:

- `target - (closed won + commit + best case)`, floored at `0`

Why:

- this is the cleanest current-quarter pressure metric for the Sales Ops audience

### New KPI: `Promotion Need %`

Definition:

- `Needed Promotion ARR / target * 100`, floored at `0`

Why:

- this normalizes the promotion gap across units and regions

### New KPI: `Low Confidence Forecast ARR`

Definition:

- sum `RiskyCommitARR` across open commit and best-case opportunities

Why:

- it captures the portion of the active forecast that still needs evidence or intervention

### Queue Ordering Rule: `Rep Promotion & Hygiene Pressure`

Definition:

- keep `NeededPromotionARR` visible in the table
- rank the table by a composite `PromotionPressureScore`
  - positive `NeededPromotionARR`
  - `OwnershipReviewARR`
  - `ReviewCandidateCount`
  - `NoNextStepCount`

Why:

- the March 31 live snapshot is already globally over-covered on promotion need, so a pure `NeededPromotionARR desc` sort collapses into zero-value rows
- the page still needs to terminate in accountable rep-level pressure even when the quarter is nominally covered

## Live March 31, 2026 Baseline

Validated read-only against `apro@simcorp.com` using the live forecast datasets.

- `Forecast Confidence % = 65.46`
- `Commit Change WoW = -89,981.21`
- `Needed Promotion ARR = 0`
- `Promotion Need % = 0`
- `Low Confidence Forecast ARR = 17,162,125.52`
- top rep pressure under the new composite ranking:
  - `Christian Ebbesen / Johanna Hornwall Bergkvist = 9,150,000`
  - `Christian Ebbesen / Tanja Fannon = 8,900,000`
  - `Alexander Schnitzler / Nathalie Bonnet = 5,500,000`
- top promotion candidates:
  - `RAJA (Carillon Towers) SaaS`
  - `Fidelity - F2B`
  - `AP Pension - SimCorp One - 15 users + MOBO`
- top commit-protection candidates:
  - `GIB UK: Front-to-Back - reimplementation`
  - `UBS - Continuous Testing 2026`
  - `BOCI Prudential - M2B`

The validation artifact will live at [live_saql_validation.json](../output/sales_ops_page4_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json).

## Page 4 Layout Contract

- `headline_story`
  - `Forecast Confidence %`
  - `Commit Change WoW`
  - `Needed Promotion ARR`
  - `Promotion Need %`
  - `Low Confidence Forecast ARR`
- `diagnostic_breakdown`
  - `Quarter Coverage Bridge`
  - `Weekly Forecast Mix`
- `action_layer`
  - `Rep Promotion & Hygiene Pressure`
  - `Promotion Candidates`
  - `Commit Protection Queue`

## Ready/Blocked Split

- Ready now:
  - forecast confidence
  - weekly commit movement
  - needed promotion ARR
  - promotion need %
  - low-confidence forecast ARR
  - quarter coverage bridge
  - weekly forecast mix
  - rep promotion pressure queue
  - promotion candidates queue
  - commit protection queue
- Still blocked on a region-compatible historical seam:
  - prior-quarter forecast calibration at the same selector grain as the live forecast page
  - historical bias rollups that respect the page region selector

## Build Rule

Page 4 should stay on the proven live forecast datasets and the shell-safe selector plane. Do not widen it back into the shell `Forecast_Intelligence` calibration widgets unless a region-compatible historical seam is proven.
