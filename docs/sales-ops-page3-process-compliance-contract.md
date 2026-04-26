# Sales Ops Page 3 Process Compliance Contract

This document turns the validated `Commercial_Rhythm_Control_Tower` seam into a build-ready Page 3 module for the quarterly Sales Ops dashboard.

It is narrower than [sales-ops-dashboard-implementation-contract.md](sales-ops-dashboard-implementation-contract.md) and should be read alongside the generated mutation package in [generated/sales_ops_page3_mutation_prep/README.md](generated/sales_ops_page3_mutation_prep/README.md).

## Current Reuse Strategy

- Reuse the live Sales Ops shell selector contract that the page can actually deploy on today:
  - `f_fy`
  - `f_region`
- Reuse the current live rhythm visual shells where they already solve the KPI cleanly:
  - `summary_headline_story_1`
  - `summary_headline_story_2`
  - `ownership_handoffs_diagnostic_breakdown_1`
  - `ownership_handoffs_diagnostic_breakdown_2`
  - `process_quality_action_layer_1`
- Keep the page scoped to proven rhythm signals:
  - renewal ownership alignment
  - next-step compliance
  - forecast hygiene review pressure
  - ownership-review pressure
  - renewal semantic-confidence and handoff-action queues

## Fields Proven In The Current Analytics Layer

- `AccountId`
- `AccountName`
- `FYLabel`
- `HandoffState`
- `Id`
- `IsClosed`
- `LeadershipAsk`
- `MotionType`
- `OppManagerName`
- `OppOwnerName`
- `OppOwnerPersona`
- `OppOwnershipAlignment`
- `OpportunityName`
- `RecordType`
- `ReviewPulse`
- `SalesRegion`
- `AtRiskRenewalValue`
- `CoveredRenewalOppCount`
- `NoNextStepCount`
- `OpenValue`
- `OwnershipReviewCount`
- `RenewalOppCount`
- `ReviewCandidateCount`
- `ZeroValueRenewalCount`

## Keep From The Live Rhythm Surface

These current rhythm steps already give Page 3 most of the shape it needs:

| Existing Step | Existing Widget | Sales Ops Page 3 Reuse |
| --- | --- | --- |
| `summary_actual_ownership_alignment_1` | `summary_headline_story_1` | `Renewal Ownership Alignment %` KPI |
| `summary_variance_driver_forecast_hygiene_2` | `summary_headline_story_2` | manager forecast hygiene ranking |
| `ownership_handoffs_variance_driver_forecast_hygiene_1` | `ownership_handoffs_diagnostic_breakdown_1` | owner/persona forecast hygiene ranking |
| `ownership_handoffs_risk_renewal_semantic_confidence_2` | `ownership_handoffs_diagnostic_breakdown_2` | renewal semantic-confidence table |
| `process_quality_action_queue_handoff_quality_1` | `process_quality_action_layer_1` | handoff-quality action queue |

## Add Or Tighten

The current rhythm dashboard is still missing three simple headline KPIs that the Sales Ops process page needs.

### New KPI: `Next Step Coverage %`

Definition:

- denominator: open detail rows in `Commercial_Rhythm_Control_Tower`
- numerator: rows where `NoNextStepCount == 0`
- implementation:
  - sum `NoNextStepCount`
  - divide by `count()`
  - invert into coverage %

Why:

- Report 2 explicitly asks for process compliance rates
- this turns the already-proven `NoNextStepCount` seam into a compliance KPI instead of only burying it in a queue

### New KPI: `Forecast Hygiene Candidate Count`

Definition:

- sum `ReviewCandidateCount` across open detail rows

Why:

- the live rhythm surface already uses this field as the main hygiene-pressure signal
- keeping it as a count avoids inventing a synthetic score

### New KPI: `Ownership Review Count`

Definition:

- sum `OwnershipReviewCount` across open detail rows

Why:

- this is the cleanest current count of records that still need ownership-review intervention
- it stays inside the proven rhythm dataset instead of reaching for unproven approval-aging logic

## Live March 31, 2026 Baseline

Validated read-only against `apro@simcorp.com` using the live `Commercial_Rhythm_Control_Tower` dataset.

- open rhythm denominator: `1,657`
- `Renewal Ownership Alignment % = 76.92`
- `Next Step Coverage % = 32.65`
- `Forecast Hygiene Candidate Count = 1,380`
- `Ownership Review Count = 66`
- top manager hygiene pressure:
  - `Christian Ebbesen = 194`
  - `Alexander Schnitzler = 137`
  - `Niklas Salminen = 119`
- top owner/persona hygiene pressure:
  - `Johanna Hornwall Bergkvist / CX = 78`
  - `Thilo Schreyer / CX = 63`
  - `Tanja Fannon / CX = 57`

The validation artifact is [live_saql_validation.json](../output/sales_ops_page3_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json).

## Page 3 Layout Contract

- `headline_story`
  - `Renewal Ownership Alignment %`
  - `Next Step Coverage %`
  - `Forecast Hygiene Candidate Count`
  - `Ownership Review Count`
- `diagnostic_breakdown`
  - `Forecast Hygiene by Manager`
  - `Forecast Hygiene by Owner / Persona`
- `action_layer`
  - `Renewal Semantic Confidence`
  - `Handoff Quality Action Queue`

## Ready/Blocked Split

- Ready now:
  - renewal ownership alignment
  - next-step coverage
  - forecast hygiene candidate count
  - ownership review count
  - manager hygiene ranking
  - owner/persona hygiene ranking
  - renewal semantic-confidence table
  - handoff-quality action queue
- Still blocked on additional dataset proof:
  - approval-submitted rate
  - approval-completed rate
  - pending approval aging
  - overdue-task rate
  - approval ARR rollups

## Build Rule

Page 3 should stay on the proven rhythm dataset and the current shell selector plane. Do not widen this page into approval-aging, overdue-task compliance, or extra persona/manager selector chrome until those seams are explicitly added to the shell contract.
