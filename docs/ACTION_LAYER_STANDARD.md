# Action Layer Standard

**Date:** March 10, 2026
**Purpose:** Define how CRM Analytics dashboards become operating surfaces instead of passive reporting surfaces.

Use this with:

- [/Users/test/crm-analytics/docs/CONSULTANT_GRADE_CRMA_PLAYBOOK.md](/Users/test/crm-analytics/docs/CONSULTANT_GRADE_CRMA_PLAYBOOK.md)
- [/Users/test/crm-analytics/docs/dashboard_review_rubric.md](/Users/test/crm-analytics/docs/dashboard_review_rubric.md)
- [/Users/test/crm-analytics/docs/generated/record_actions_api_limitation.md](/Users/test/crm-analytics/docs/generated/record_actions_api_limitation.md)
- [/Users/test/crm-analytics/docs/generated/DOMAIN_PERSONA_OPERATING_MODEL_2026-03-10.md](/Users/test/crm-analytics/docs/generated/DOMAIN_PERSONA_OPERATING_MODEL_2026-03-10.md)

## 1. Goal

An action layer means:

- the dashboard identifies the issue
- the dashboard identifies the affected record or segment
- the dashboard presents the next step clearly
- the user can move directly into Salesforce workflow

If a user must manually hunt for the record after seeing the issue, the action layer is incomplete.

## 2. Current Platform Reality

### What works now

- dataset-level XMD record links
- cross-dashboard links
- drill paths into account/opportunity/lead/contract views
- exception tables with explicit `NextBestAction` or reason columns

### What does not reliably work in the current API deploy path

- persisted `salesforceActions` interactions on dashboard widgets

That limitation is already documented in:

- [/Users/test/crm-analytics/docs/generated/record_actions_api_limitation.md](/Users/test/crm-analytics/docs/generated/record_actions_api_limitation.md)

Practical implication:

- `Open Record` via XMD is a real, working foundation
- `Create Task`, `Change Owner`, and similar row-level actions should not be treated as deployable-by-code in the current path until a metadata-native or UI-confirmed route is proven

## 3. Official Platform Rules We Build Around

- Salesforce says to use facets for simple same-dataset filtering and bindings for more complex cross-dataset or query-shape interactions:
  - https://developer.salesforce.com/docs/analytics/bi-dev-guide-bindings/guide/bi-dbjson-bindings.html
- Salesforce also documents that bindings can change measures, groupings, limits, filters, and some display properties, but not everything:
  - https://developer.salesforce.com/docs/analytics/bi-dev-guide-bindings/guide/bi-dashboard-bindings-limitations.html
- Dynamic measure switching must bind the query `measures` and avoid fixed `columnMap` assumptions:
  - https://developer.salesforce.com/docs/analytics/bi-dev-guide-bindings/guide/bi-dashboard-bindings-wave-designer-use-case-measure.html
- Tableau’s small-multiples guidance is useful for trellis thinking: use repeated panes to compare the same pattern across categories, then simplify when the view gets too dense:
  - https://help.tableau.com/current/pro/desktop/en-us/getstarted_buildmanual_ex1basic.htm

## 4. Action Design Hierarchy

Use the simplest action path that actually changes user behavior.

### Level 1: Contextual navigation

Use when:

- the user must inspect a specific record next
- the dashboard has already narrowed the problem to a short list

Patterns:

- clickable account name
- clickable opportunity name
- clickable lead name
- clickable contract name
- link to account 360
- link to opportunity risk view

This is the default and should be present on every exceptions table.

### Level 2: Guided triage

Use when:

- the user needs help understanding what to do, not just which record to open

Patterns:

- `Why Flagged`
- `PriorityBand`
- `NextBestAction`
- `RiskDriver`
- `AgingBucket`
- `OwnerAttention`

This belongs in the dataset and the compare table, not hidden in documentation.

### Level 3: Cross-dashboard operating drill

Use when:

- the first dashboard detects the issue
- a second dashboard is the right place to diagnose or act

Patterns:

- exec surface → manager queue
- manager exception table → account 360
- product whitespace table → account action center
- renewal risk queue → contract / renewal detail

### Level 4: Native row-level actions

Use when:

- the user can act immediately from the row
- the platform path is proven in the target environment

Examples:

- create task
- reassign owner
- open flow
- launch quick action

Current repo rule:

- design for this, but do not rely on API persistence for it yet

## 5. Persona Rules

### Executive

Action surfaces should be light.

Use:

- top-risk queue
- top-opportunity/risk drill
- link to manager surface

Avoid:

- dense row-level action collections
- too many direct operational tasks

### Manager

This is the primary action-layer persona.

Every manager dashboard should end in:

- ranked exception table
- clear reason columns
- direct record links
- obvious handoff to the next operating view

If a manager page has no action queue, it is incomplete.

### Individual

This should be the strongest action layer.

Use:

- queue-first views
- few charts
- many priorities
- direct record navigation
- explicit `NextBestAction`

### Analyst

Analyst pages are for explanation and QA, not task routing.

Only add action paths when they genuinely help validation or escalation.

## 6. Exception Table Standard

Every exception table should have:

- the business key: `AccountName`, `OpportunityName`, `LeadName`, or `ContractName`
- the Salesforce record id field
- impact measure: ARR, quota gap, risk amount, churn amount, pipeline value
- urgency measure: aging, slip count, renewal window, health band, SLA breach
- explanation column: why this item is here
- `NextBestAction`

Preferred columns by use case:

### Revenue risk

- `OpportunityName`
- `OwnerName`
- `ForecastCategory`
- `GapToPlan`
- `PushCount`
- `RiskScore`
- `NextBestAction`
- `OppId`

### Customer / account risk

- `AccountName`
- `OwnerName`
- `HealthScore`
- `RenewalRiskScore`
- `WhitespaceScore`
- `CoverageGap`
- `NextBestAction`
- `AccountId`

### Lead / BDR queue

- `LeadName`
- `Company`
- `OwnerName`
- `PriorityBand`
- `SLABreachCount`
- `DaysToConvert`
- `NextBestAction`
- `LeadId`

### Renewal queue

- `ContractName` or renewal opportunity
- `AccountName`
- `OwnerName`
- `DaysToExpiry`
- `RenewalRiskScore`
- `RenewalARR`
- `NextBestAction`
- `Id`

## 7. Trellis Standard

Trellis is for comparing the same shape across categories.

Use trellis when:

- the analytical task is pattern comparison
- each pane uses the same axes and the same encoding
- the number of panes is small enough to scan quickly
- the user needs to compare trend shape, not just rank

Good trellis examples:

- pacing by region
- win rate trend by motion
- renewal exposure by segment
- monthly activity rhythm by team

Avoid trellis when:

- there are too many panes
- labels become unreadable
- ranking is the real question
- exact values matter more than pattern shape
- one heatmap or ranked bar would say the same thing more clearly

Default guidance:

- prefer `2` to `6` panes
- be cautious above `9`
- if the user needs to compare many categories, switch to heatmap or ranked bar

## 8. Trellis vs Other Patterns

Use:

- `trellis` for comparing trend shape across a few categories
- `heatmap` for concentration, under-penetration, or two-dimensional pattern detection
- `ranked bar` for ordered comparison
- `scatter / bubble` for prioritization and quadrants
- `compare table` for action and triage

Quick rule:

- if the question is “who is worst or best?” use ranked bar
- if the question is “where are gaps concentrated?” use heatmap
- if the question is “which records do I work next?” use compare table
- if the question is “how do patterns differ by segment?” use trellis

## 9. Cards / Repeater Guidance

The repo research correctly points out that repeaters or card-style surfaces are useful for top-priority items.

Use card-like patterns when:

- the queue is intentionally small
- each item needs explanation
- the user should scan one issue at a time

Best use cases:

- top 5 slipped deals
- top 5 at-risk renewals
- top 5 whitespace accounts
- top 5 SLA-breached leads

Avoid card/repeater patterns when:

- the user needs sorting, scanning, or filtering across many records
- there are more than roughly 10 items
- a compare table is clearly stronger

Current practical rule:

- use compare tables as the default action container
- use card/repeater concepts sparingly for top-priority queues or future LWC surfaces

## 10. What To Build Now

With current permissions and platform behavior, the best action layer we can reliably ship now is:

1. strong exception tables
2. XMD record links
3. clear `NextBestAction` and reason columns
4. drill to shared operating views like account 360
5. cross-dashboard navigation from executive → manager → record detail

This is enough to materially beat siloed BI reporting, even before richer actions are unlocked.

## 11. What To Add Later

After the foundation is proven and permissions improve:

- true row-level Salesforce actions
- quick actions and flows
- embedded dashboards on record pages
- LWC action centers
- notifications and subscriptions
- AI-assisted summaries and triage

## 12. Build Rules

Before adding an action surface, ask:

1. Who is expected to act?
2. Can they act immediately?
3. Is the next step a record, a queue, or a drill page?
4. Does the widget expose why the item is flagged?
5. Would a table, trellis, heatmap, or ranked bar answer the question more clearly?

If those answers are fuzzy, the action design is not ready.

## 13. Hard Rejections

Reject any page that:

- ends with charts but no exception queue
- shows exception records without record links
- shows a queue without `NextBestAction`
- uses trellis when the panes are too dense to scan
- uses trellis when ranking is the real question
- claims row-level actions are available when the deploy path does not persist them

## 14. Default Pattern By Page Type

### Executive summary page

- KPI strip
- pacing / bridge / mix chart
- top risk queue with links

### Manager exceptions page

- ranked comparison
- diagnostic chart
- action table with links and reasons

### Individual queue page

- compact KPI header
- work queue
- upcoming / overdue / risk split

This is the default until a better proven pattern replaces it.
