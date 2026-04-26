# Widget Decision Library

Use this when deciding which CRM Analytics widget belongs on an executive, manager, individual, analyst, or embedded surface.

Use with:

- [/Users/test/crm-analytics/docs/CONSULTANT_GRADE_CRMA_PLAYBOOK.md](/Users/test/crm-analytics/docs/CONSULTANT_GRADE_CRMA_PLAYBOOK.md)
- [/Users/test/crm-analytics/docs/dashboard_review_rubric.md](/Users/test/crm-analytics/docs/dashboard_review_rubric.md)
- [/Users/test/crm-analytics/docs/ACTION_LAYER_STANDARD.md](/Users/test/crm-analytics/docs/ACTION_LAYER_STANDARD.md)
- [/Users/test/crm-analytics/docs/AUTOPILOT_AUDIT_GATES.md](/Users/test/crm-analytics/docs/AUTOPILOT_AUDIT_GATES.md)

## Core Rule

Pick the widget from the business question, not from personal preference.

Every widget choice should answer:

1. What question is being answered?
2. What comparison matters?
3. Who is the audience?
4. Is the goal trend, rank, composition, variance, pattern, or action?
5. What should the user do next?

## Decision Shortcuts

Use:

- `number / KPI tile` for single headline values
- `bullet` for actual vs target or threshold
- `line / timeline` for trend, pacing, trajectory, forecast ladder
- `waterfall` for bridge or build-up logic
- `hbar` for ranking
- `compare table` for diagnosis and action
- `heatmap` for concentration or bottleneck patterns
- `trellis` for comparing the same shape across a small set of slices
- `treemap` for composition when there are many categories and exact ranking is secondary
- `choropleth` for geographic pattern, always paired with a rank table or bar

Do not use:

- `bullet` for forecast ladders or composition
- `trellis` when rank matters more than shape
- `map` when the user needs precise comparisons
- `treemap` when ordered rank is the real question
- `gauge` for most business pages
- `generic bars` when the page really needs a queue or bridge

## Widget Rules

### Number / KPI Tile

Use when:

- the user needs one current-state number
- the comparator is obvious from the title or neighboring tile

Good fits:

- closed won ARR
- best case call ARR
- 10% YoY target
- NRR
- churn rate
- SLA attainment

Avoid when:

- the number has no comparator
- the page turns into a tile graveyard

### Bullet

Use when:

- actual vs target matters
- actual vs threshold matters
- the user needs to know whether performance is on plan

Good fits:

- pipeline coverage vs 3x target
- win rate vs target
- BDR activity logged vs target
- SLA attainment vs target
- quote cycle time vs threshold
- NRR / GRR vs plan
- health score vs target band

Avoid when:

- the question is about trend over time
- the question is about multiple nested forecast scenarios
- the question is about composition

### Line / Timeline

Use when:

- the question is trend or pacing
- the handoff from actual to forecast matters
- multiple scenarios should be compared over time

Good fits:

- closed won + forecast call ladder
- win rate trend by month
- renewals by month
- lead-to-opportunity time trend
- activity trend vs target line

Avoid when:

- the real question is rank
- there are too many series to read

### Waterfall

Use when:

- the question is how the total is built
- the user needs a bridge from start to end

Good fits:

- closed won -> commit -> best case -> gap to target
- prior ARR -> expansion -> contraction -> churn -> ending ARR
- pipeline -> approval -> quote -> close conversion loss bridge

Avoid when:

- the question is trend
- the steps are not additive and directional

### HBar

Use when:

- the user needs ranking
- the categories are textual
- the answer is “who is best/worst” or “where is the largest gap”

Good fits:

- best-case gap by region
- churn ARR by segment
- renewal risk by unit group
- lead source conversion by channel

Avoid when:

- the user needs pattern shape over time
- the page needs an action queue instead

### Compare Table

Use when:

- the user needs diagnosis
- the page should end in action
- multiple fields together create meaning

Good fits:

- opportunity risk queue
- regional confidence table
- account health intervention table
- BDR queue
- renewal exception queue

Required fields for operating tables:

- business key
- impact measure
- urgency measure
- explanation
- next action
- record link or drill key

Avoid when:

- the page is still in high-level story mode
- a chart would answer the question faster

### Heatmap

Use when:

- the question is about concentration, density, or bottlenecks
- both axes matter

Good fits:

- stage aging by region
- product penetration by segment
- activity distribution by weekday x owner
- renewal risk by month x unit group

Avoid when:

- users need exact values
- ranking is the main task

### Trellis

Use when:

- the same chart should be repeated across a small number of slices
- the question is about pattern comparison

Good fits:

- forecast trend by region
- win-rate trend by segment
- pipeline creation rhythm by manager
- renewal curve by unit group

Avoid when:

- there are too many panels
- the panels become unreadable
- the real question is ranking or action

Default rule:

- `Executive`: rarely default to trellis
- `Manager`: useful when comparing regional or team patterns
- `Individual`: usually not worth the space

### Treemap

Use when:

- the question is composition
- there are many categories
- approximate relative size is enough

Good fits:

- ARR by product family
- ARR by unit group
- pipeline composition by owner or motion

Avoid when:

- precise rank matters
- small categories need comparison

### Choropleth / Global Map

Use when:

- the question is geographic pattern
- the audience benefits from spatial context

Good fits:

- won ARR by country
- open pipeline ARR by country
- customer ARR by country
- renewal risk by country

Requirements:

- pair the map with a ranked bar or table
- use a stable country field
- label clearly whether it is won, forecast, open pipeline, or installed ARR

Avoid when:

- the user needs exact ranking from the map alone
- country coverage is sparse or low-quality

## Domain Rules

### Revenue

Use most often:

- line / timeline
- waterfall
- hbar
- compare table
- bullet

Typical questions:

- Are we on track?
- Which forecast category is missing?
- Where is the gap by region or segment?
- Which opportunities need intervention?

Avoid:

- decorative maps without a ranking companion
- bullets for forecast ladders

### Demand / BDR / Lead Management

Use most often:

- bullet
- funnel
- line
- heatmap
- compare table

Typical questions:

- Are SLAs being met?
- Are leads converting?
- Which sources create real pipeline?
- Which reps or queues need intervention now?

### Customer / Account Health

Use most often:

- bullet
- heatmap
- compare table
- hbar
- line

Typical questions:

- Which accounts are at risk?
- Which segments are healthy or unhealthy?
- Where is expansion headroom?

### Retention / Renewals

Use most often:

- line
- waterfall
- bullet
- hbar
- compare table

Typical questions:

- Are we holding revenue?
- What is churn doing over time?
- Which renewals are at risk?
- Where is contraction concentrated?

### Product / GTM

Use most often:

- heatmap
- treemap
- hbar
- choropleth
- compare table

Typical questions:

- Which segments buy which products?
- Where is ARR concentrated geographically?
- Where is whitespace or underpenetration?

## Persona Rules

### Executive

Prefer:

- 4-6 KPI tiles
- 1-2 hero visuals
- 1 bridge
- 1 ranked diagnostic
- 1 compact diagnosis table
- 1 exception queue

Avoid:

- dense trellis
- too many tables
- exploratory visuals that need interpretation training

### Manager

Prefer:

- stronger diagnosis layer
- pattern comparison
- segment / region / rep cuts
- action-first queue

### Individual

Prefer:

- queue-first
- bullet for target attainment
- minimal summary charts

### Analyst

Prefer:

- deeper comparison
- heatmaps
- trellis
- cohort views
- QA surfaces

## Audit Questions

Every widget should survive these checks:

- Is this the right widget type for the question?
- Is a simpler widget better?
- Does the title tell the user what comparator matters?
- Is the persona right for this level of complexity?
- Is there an action path after the visual?
- Would a wrong time frame or wrong forecast-of-record make this visual misleading?
