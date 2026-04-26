# Sales Ops Page 1 Quarterly Summary Contract

This document turns the Sales Ops dashboard landing page into a build-ready Page 1 module for the quarterly Sales Ops dashboard.

It is narrower than [sales-ops-dashboard-implementation-contract.md](sales-ops-dashboard-implementation-contract.md) and should be read alongside the generated mutation package in [generated/sales_ops_page1_mutation_prep/README.md](generated/sales_ops_page1_mutation_prep/README.md).

## Current Reuse Strategy

- Keep the page on the current live shell selector plane:
  - `f_fy`
  - `f_quarter`
  - `f_region`
  - `f_unit`
- Reuse the live shell visual shells where they already fit:
  - `p11_pastdue_owner` for the ranked exception hbar
  - `p20_rep_table` for the compact manager queue
- Rebuild every headline KPI as a minimal number widget:
  - do not copy live number widgets because the shell versions carry blocked number-widget parameters like `compact` and `title`
- Keep the page summary-first, not dashboard-within-dashboard:
  - 5 scorecard tiles
  - 1 ranked exception-area chart
  - 1 compact cross-page manager queue

## V1 Headline KPI Contract

### `Data Completeness Score`

Definition:

- average of four proven completeness seams:
  - `DUNS Coverage %`
  - `Unit Group Coverage %`
  - `KYC Status Capture %`
  - `Closed Won/Lost Reason Coverage %`

Why:

- this keeps the summary KPI anchored to completeness, not the looser `DataQualityScore`
- it matches the repo direction that CRM "accuracy" still needs a separate comparison source

### `Process Compliance Rate`

Definition:

- average of four Page 3 signals:
  - `Renewal Ownership Alignment %`
  - `Next Step Coverage %`
  - `Forecast Hygiene Clean %`
  - `Ownership Review Clean %`

Why:

- this converts the current Page 3 mix of one rate plus pressure counts into one executive score without inventing new source fields

### `Forecast Accuracy`

Definition:

- reuse the Page 4 `Forecast Confidence %` logic as the landing-page forecast trust metric

Why:

- it is already the most robust forecast summary seam on the live forecast dataset
- it respects the current shell selector plane

### `Pipeline Hygiene Rate`

Definition:

- percent of open pipeline rows with no current hygiene pressure:
  - `PastDueCount == 0`
  - `StaleCount == 0`
  - `BackwardMoveCount == 0`
  - `PushCount < 2`

Why:

- it gives the landing page one clean inverse score instead of forcing leaders to interpret `Stale Pipeline %` as a positive KPI

### `Top Exception Queue Count`

Definition:

- maximum queue count across the four normalized Page 6 exception lanes:
  - `Missing Data`
  - `Commercial Process`
  - `Forecast Risk`
  - `Stale Pipeline`

Why:

- the landing page should answer "where is the biggest operational backlog right now?" in one tile

## Diagnostic And Action Layer

### Diagnostic Chart: `Exception Pressure by Area`

Definition:

- union the four Page 6 queue-count seams
- rank by `queue_cnt desc`

Why:

- it gives the landing page one ranked picture of where pressure is actually sitting

### Action Queue: `Top Manager Exceptions`

Definition:

- compact union of the four Page 6 remediation queues
- normalize onto one column contract:
  - `QueueArea`
  - `AccountName`
  - `OpportunityName`
  - `OwnerName`
  - `ManagerName`
  - `ARR`
  - `StageName`
  - `CloseDate`
  - `RootCause`
  - `RecommendedAction`
  - `PriorityScore`
- sort by `PriorityScore desc`

Why:

- Page 1 needs to terminate in action, not just scorecards
- a compact union queue is more useful here than copying four separate tables onto the landing page

## Live March 31, 2026 Baseline

Validated read-only against `apro@simcorp.com` using the current live account, rhythm, forecast, and pipeline datasets.

- `Data Completeness Score = 83.54`
- `Process Compliance Rate = 58.59`
- `Forecast Accuracy = 65.46`
- `Pipeline Hygiene Rate = 47.13`
- `Top Exception Queue Count = 5,753`
- ranked exception pressure:
  - `Commercial Process = 5,753`
  - `Stale Pipeline = 701`
  - `Forecast Risk = 86`
  - `Data Quality = 25`

The validation artifact is [live_saql_validation.json](../output/sales_ops_page1_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json).

## Page 1 Layout Contract

- `headline_story`
  - `Data Completeness Score`
  - `Process Compliance Rate`
  - `Forecast Accuracy`
  - `Pipeline Hygiene Rate`
  - `Top Exception Queue Count`
- `diagnostic_breakdown`
  - `Exception Pressure by Area`
- `action_layer`
  - `Top Manager Exceptions`

## Build Rule

Page 1 should stay on already-proven page seams and the current shell selector plane. Do not widen it into separate narrative charts or historical commentary until the quarterly PowerPoint layer is built above the dashboard.
