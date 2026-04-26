# Consultant-Grade CRM Analytics Playbook

**Date:** March 10, 2026
**Purpose:** Define the operating standard for building a consultant-grade CRM Analytics and Tableau-style decision system for SimCorp.

This playbook is the working standard for:

- dashboard design
- dataset design
- widget selection
- persona alignment
- action-layer design
- QA and deployment review

Use this with:

- `/Users/test/crm-analytics/docs/dashboard_review_rubric.md`
- `/Users/test/crm-analytics/docs/data-storytelling-framework.md`
- `/Users/test/crm-analytics/docs/ACTION_LAYER_STANDARD.md`
- `/Users/test/crm-analytics/docs/WIDGET_DECISION_LIBRARY.md`
- `/Users/test/crm-analytics/docs/COMMERCIAL_OPERATING_RHYTHM.md`
- `/Users/test/crm-analytics/docs/generated/REVENUE_WIDGET_CONTRACT_2026-03-10.md`
- `/Users/test/crm-analytics/docs/generated/DOMAIN_PERSONA_OPERATING_MODEL_2026-03-10.md`

## 1. Source Of Truth

The standard is based on three inputs:

### Official platform guidance

- Salesforce CRM Analytics bindings and interactions:
  - https://developer.salesforce.com/docs/analytics/bi-dev-guide-bindings/guide/bi-dbjson-bindings.html
  - https://developer.salesforce.com/docs/analytics/bi-dev-guide-bindings/guide/bi-dashboard-bindings-limitations.html
  - https://developer.salesforce.com/docs/analytics/bi-dev-guide-bindings/guide/bi-dashboard-bindings-wave-designer-use-case-measure.html
- Salesforce dataset design guidance:
  - https://trailhead.salesforce.com/content/learn/modules/best-practices-for-building-datasets-with-tableau-crm
  - https://trailhead.salesforce.com/fr/content/learn/modules/best-practices-for-building-datasets-with-tableau-crm/understand-dataset-grain-and-when-to-use-a-lookup
- Tableau dashboard and visual guidance:
  - https://help.tableau.com/current/pro/desktop/en-us/dashboards_best_practices.htm
  - https://help.tableau.com/current/blueprint/en-us/bp_visual_best_practices.htm

### Real-world archetypes

- Tableau sales pipeline and forecast accelerator:
  - https://exchange.tableau.com/products/840
- Tableau open pipeline accelerator:
  - https://exchange.tableau.com/products/515
- Salesforce’s Tableau forecasting and pipeline example:
  - https://www.tableau.com/solutions/customer/how-salesforce-built-smarter-sales-strategy-tableau-and-ai
- Salesforce Revenue Intelligence framing:
  - https://www.salesforce.com/sales/revenue-intelligence/

### SimCorp-specific research and live org evidence

- live Salesforce org schema and CRMA assets
- live deployed dashboards and datasets
- repo-local build scripts and docs
- handoff and deep-research files in `/Users/test/crm-analytics` and `/Users/test/Downloads`
- handbook-backed role and motion model in `/Users/test/crm-analytics/config/commercial_operating_model.json`

## 2. What We Are Building

We are not building a collection of charts.

We are building a role-based commercial operating system with these layers:

- `Executive`: decide where the business is off track
- `Manager`: diagnose why and assign intervention
- `Individual`: know what to work on next
- `Analyst`: validate, explain, and model the underlying dynamics
- `Embedded`: show account/opportunity/lead/contract context in the flow of work

The correct architecture is:

- CRMA-native for interactions, bindings, record actions, dashboard state, XMD, and embedded workflow
- Python for dataset engineering, dashboard generation, QA automation, and deployment

## 3. Core Design Rules

### Rule 1: Domain first, then persona

Every request must answer:

1. Which domain owns the question?
2. Which persona is making the decision?
3. Is this a top-level surface, a drill path, or a filter slice?

The core domains are:

- `Demand`
- `Revenue`
- `Customer`
- `Retention`
- `Product / GTM`
- `Productivity / Compliance`

Do not create a new dashboard until those three questions are explicit.

### Rule 2: Lock the metric spine before chart design

The semantic layer must define:

- funnel stages: `MQL`, `SAL`, `SQL`, `SQO`
- revenue motions: `New`, `Expand`, `Renewal`, `Contraction`, `Churn`
- forecast categories: `Pipeline`, `Best Case`, `Commit`, `Closed`
- time logic: booked month, close month, renewal month, cohort month
- hierarchy: rep, team, manager, region, unit group
- account segmentation: segment, industry, product family, delivery model
- customer health and retention formulas: `Health Score`, `NRR`, `GRR`, churn, logo retention

If a KPI cannot be defined precisely, it should not yet be visualized.

### Rule 3: Design for decisions, not display

Every widget must answer:

- what happened?
- compared to what?
- why does it matter?
- what should happen next?

If the widget cannot produce an action or escalation path, it is probably decorative.

### Rule 4: Shared drill targets beat dashboard sprawl

`Account 360`, opportunity detail, lead queue, and renewal detail should be shared drill targets across domains.

Do not build separate standalone pages for every account, region, rep, and segment view unless the workflow truly changes.

## 4. CRMA Platform Rules

### Facets vs bindings

Use facets when widgets share the same dataset and the interaction is a normal filter.

Use bindings when:

- widgets come from different datasets
- the interaction changes measures
- the interaction changes grouping
- the interaction changes ordering, limits, or query shape

Do not fake cross-dataset behavior with brittle assumptions.

### Respect binding limits

Not every property is bindable. Design interactions that the platform actually supports.

If an experience depends on unsupported binding behavior, reject it and redesign.

### Dynamic measure switching

When switching measures dynamically:

- bind query-level measures
- avoid fixed `columnMap` assumptions
- use the supported measure-binding pattern from Salesforce documentation

### Dataset grain comes first

Start by defining the grain explicitly.

Examples:

- owner x quarter x product family
- opportunity x month
- account x product family
- account x product cluster x month

Do not mix incompatible grains in one dataset or one visual.

Use lookups and multi-dataset dashboards intentionally when the business question requires them.

### Multi-dataset dashboards are valid

Production already shows that the strongest manager and executive surfaces use multiple datasets.

Use multiple datasets when it improves:

- metric trust
- business meaning
- drill behavior
- actionability

Do not force one mega-dataset when the result is ambiguous logic.

### XMD and actions are part of the product

XMD is not a polish step. It controls:

- labels
- formatting
- geographic behavior
- record links
- actions
- metadata consistency

Record links and action behavior must be designed with the dashboard, not bolted on later.

### Audit before deployment

Every deployable dashboard must pass:

- dashboard audit
- SAQL/query sanity check
- binding review
- XMD/link validation
- screenshot review

## 5. Tableau / Visual Design Rules

### Start with audience and purpose

Tableau guidance is clear: a dashboard should have a clear purpose and a defined audience.

That means each page must declare:

- the audience
- the question
- the operating cadence
- the expected action

### Limit visual sprawl

Too many views reduce clarity and performance.

Do not use more charts just because they fit on the page.

Pages should have a clear narrative:

- KPI strip
- primary chart
- supporting comparison
- action queue

### Title the finding, not the axis

Good titles tell the viewer the conclusion.

Bad:

- `Revenue by Region`

Better:

- `EMEA pacing is below plan while APAC offsets the gap`

### Direct attention intentionally

Use gray as the default and reserve semantic color for what matters.

Use color to communicate:

- risk
- target miss
- positive variance
- selected state

Do not rely on rainbow palettes or color alone.

### Every number needs context

An orphan KPI is not acceptable.

Top-line numbers should show:

- current value
- versus target
- versus prior period
- directional variance
- optional sparkline where it adds value

### Use the right chart for the question

Use:

- `line / timeline` for pacing, trajectory, forecast handoff, and time-based comparison
- `waterfall` for bridge and decomposition logic
- `hbar / bar` for ranked comparisons and decomposition
- `bullet` for actual vs target vs threshold
- `heatmap` for two-dimensional concentration and gap detection
- `scatter / bubble` for prioritization and quadrant analysis
- `compare/value table` for triage and action queues

Avoid:

- gauges
- pies for precise comparison
- decorative charts with no decision value
- duplicate summaries of the same KPI in different widgets

## 6. Widget Contract Standard

Every widget should be specified before implementation with:

- business question
- audience
- grain
- time logic
- measures and series
- comparator
- widget type
- interaction behavior
- action path
- rejection criteria

This is already applied in:

- `/Users/test/crm-analytics/docs/generated/REVENUE_WIDGET_CONTRACT_2026-03-10.md`

That level of detail is the expected standard for every major domain, not just revenue.

## 7. Page Architecture Standard

Every page should follow a clear operating arc:

1. `Situation`
   - KPI strip
2. `Complication`
   - the main trend, bridge, ranking, or concentration visual
3. `Resolution`
   - exception queue, priority table, or action surface

This is the default pattern unless there is a strong reason to break it.

Executive pages should optimize for scan speed.

Manager pages should optimize for diagnosis and intervention.

Individual pages should optimize for immediate action.

Analyst pages can be denser, but they should never be mistaken for executive pages.

## 8. Product Suite Standard

The current sprawl becomes manageable when every dashboard belongs to a domain and persona layer.

The target suite should be organized as:

- `Executive`
  - revenue and forecast
  - customer risk and growth
  - product mix and GTM
  - demand and pipeline
- `Manager`
  - forecast and revenue motions
  - pipeline risk and process
  - demand / lead operations
  - BDR manager
  - customer health manager
  - renewal risk and retention
  - product / GTM deep dive
- `Individual`
  - BDR rep queue
  - rep / AM portfolio
  - renewal portfolio
  - account action center
- `Analyst`
  - revenue pipeline analyst lab
  - customer revenue analyst lab
  - advanced analytics / model QA

Use the operating model here:

- `/Users/test/crm-analytics/docs/generated/DOMAIN_PERSONA_OPERATING_MODEL_2026-03-10.md`

## 9. Build Workflow

For every major dashboard or refactor:

1. inspect live org shape, datasets, and current dashboard state
2. confirm metric contract and dataset grain
3. review official Salesforce/Tableau guidance for the relevant interaction or visual pattern
4. define widget contracts before building
5. build or refactor dataset logic
6. build dashboard JSON and XMD
7. validate totals, bindings, and actions
8. run screenshot review and design critique
9. compare against the domain/persona model before deployment

## 10. Definition Of Done

A dashboard is not done when it renders.

It is done when:

- the business question is explicit
- the audience is explicit
- the metric logic is stable and reconciled
- the grain is correct
- the chart types match the analytical task
- the controls are understandable
- the page ends in a usable action path
- links and actions work
- the dashboard audit passes
- the visual hierarchy is clear in five seconds

## 11. Research Policy

We already have enough context to begin building now.

We do **not** need to pause for a generic research phase before every task.

We **do** need targeted research when:

- a CRMA interaction pattern is complex or constrained
- a metric definition is ambiguous
- a new business domain is being introduced
- a visual pattern needs external validation
- platform behavior may have changed

The workflow is:

- use repo-local knowledge first
- verify platform-specific mechanics with official Salesforce or Tableau sources
- capture decisions back into repo docs

## 12. Non-Negotiable Rejections

Reject any dashboard, page, or widget that:

- mixes incompatible grains
- shows a number without context
- uses a chart type that obscures the actual question
- duplicates the same story in several widgets
- relies on unsupported bindings
- produces insight without an action path
- mixes executive, manager, and analyst needs on one page
- adds more navigation instead of improving the control model

## 13. What Good Looks Like

The target outcome is:

- executive surfaces that explain the business in 30 seconds
- manager surfaces that diagnose and route work in 2 minutes
- individual surfaces that tell a person what to do next
- analyst surfaces that validate the model without cluttering operational views
- embedded account and opportunity views that bring analytics into Salesforce workflow

That is the standard going forward.
