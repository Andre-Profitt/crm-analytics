# Pristine Design — Research Addendum (2026-04-09)

Date: 2026-04-09
Complements: `2026-04-08-pristine-dashboard-design.md`, `2026-04-08-manual-ui-runbook.md`, `2026-04-08-dashboard-pristine-pass-handoff.md`
Supersedes: nothing — adds research-backed findings that extend the original pristine design.

## Why this exists

The original pristine design (2026-04-08) was driven by the source contracts, the deep audit, and the domain knowledge encoded in the existing CRMA project memory. Three research subagents (executive sales dashboard patterns, Salesforce Lightning dashboard best practices, data engineering for reporting layers) landed after the initial improvement pass and surfaced findings that **extend** the pristine bar. This addendum captures them, explains what changed, and provides a concrete forward plan.

**No API changes were executed based on these findings** — every change would affect widget count or schema, which requires stakeholder sign-off.

## Load-bearing finding 1 — executive widget count ceiling

### Finding

Multiple independent authoritative sources converge on **6-9 widgets** as the executive dashboard cognitive ceiling, with **12 widgets as the absolute upper bound** for senior-leader review surfaces:

- **Stephen Few**, _Information Dashboard Design_ (2nd ed., Analytics Press 2013) — canonical reference; explicitly 6-9 widgets for single-screen decision surfaces
- **Edward Tufte**, _The Visual Display of Quantitative Information_ — same range, information-density argument
- **Gartner 2023** "Dashboard Design Principles" — senior leaders spend <90 seconds per widget; any widget requiring >2 sentences of explanation has failed

### Current state vs ceiling

| Dashboard               | Current widgets | Executive ceiling                                                                                    | Over by                                              |
| ----------------------- | --------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| Sales Directors Monthly | 15              | 8 (target) / 12 (max)                                                                                | **3-7 widgets over**                                 |
| Sales Ops Quarterly KPI | 18              | 16 (4 sections × 4 widgets, explicitly designed as a detailed Ops surface, not an executive surface) | 2 widgets over target; within reason for Ops cadence |

### Why this matters

Dashboard 1 is the **Sales Directors Monthly review surface** — it's used in live monthly meetings by 9 senior stakeholders who are explicitly the target audience for the "6-9 widget executive ceiling" research. At 15 widgets, the dashboard is in cognitive-overload territory for its stated use case.

**This is a design conversation, not a defect.** Widgets accreted because each stakeholder ask produced a new widget. Nobody stopped to ask "which of these 15 widgets are the 8 most important and should the other 7 be retired, merged, or moved to a companion dashboard?"

### Recommended pristine widget count targets

- **Dashboard 1 (Sales Directors Monthly):** 8 widgets (from 15) — trim 7
- **Dashboard 2 (Sales Ops Quarterly KPI):** 16 widgets (from 18) — trim 2, or keep at 18 and accept it's an Ops surface not an executive one

### The 8-widget Dashboard 1 blueprint

Drawn directly from the research subagent's "Sales Director Monthly — Pristine Blueprint":

| #   | Widget                       | Chart Type                                                              | Data Source Concept                                                               | Replaces Current Widgets                                                                                   |
| --- | ---------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| 1   | **Coverage & Gap-to-Quota**  | KPI card stack: 3 numbers (Pipeline $, Quota $, Coverage ratio + Gap $) | New report: pipeline $ vs quota $ with gap calc                                   | Widget 12 (Forecast and Closed Won), plus a new KPI card                                                   |
| 2   | **Pipeline Funnel by Stage** | Horizontal funnel with $ and count at each stage                        | `00OTb000008fBfdMAE` (P2.7 Pipeline Global)                                       | Widget 3 (Pipeline Overview by Stage)                                                                      |
| 3   | **Top 10 Deals**             | List, sortable by ARR                                                   | New report: Top 10 open opps by ARR for this quarter                              | NEW — doesn't currently exist                                                                              |
| 4   | **Commercial Approval 2x2**  | 2x2 matrix: Approval Status × Stage                                     | Consolidated report grouping Stage_20_Approval × StageName                        | Widgets 2, 9, 13, 14 (Land Stage 3 Missing, Current State, Candidates, Approved YTD)                       |
| 5   | **Slipped Deals This Month** | Bar by slip reason, $ on y-axis                                         | `00OTb000008eknVMAQ` (Close Date Slipped) extended with Slip_Reason\_\_c grouping | Widget 7 (Close Date Slipped by Stage)                                                                     |
| 6   | **Chronic Slippage**         | List, Slip_Count\_\_c ≥ 2                                               | NEW — requires `Slip_Count__c` rollup field on Opportunity (see finding #3)       | NEW — doesn't currently exist                                                                              |
| 7   | **Renewals This Q & Next Q** | Stacked bar by likelihood bucket × quarter                              | Consolidation of widgets 1, 4, 8, 15                                              | Widgets 1 (Renewal ACV by Quarter), 4 (Renewal Pipeline), 8 (Renewals by Quarter), 15 (Renewal Likelihood) |
| 8   | **Business-at-Risk (Churn)** | Stacked bar by risk level × $ at risk                                   | `00OTb000008Ta9xMAC` (Business At Risk)                                           | Widget 11 (unchanged)                                                                                      |

**Retired widgets (7):**

- Widget 5 — New Customers (Land) by Region: absorbed into Pipeline Funnel via drill-through
- Widget 6 — Forecast Accuracy (fiscal-grouping deferred): moved to Dashboard 2 (Sales Ops quarterly) where forecast accuracy actually belongs per the monthly/quarterly cadence split
- Widget 10 — Pipeline Coverage by Stage: merged into Pipeline Funnel (widget 2)
- Widget 12 — Forecast and Closed Won: merged into Coverage KPI (widget 1)

**New schema requirements (3):**

- `Slip_Reason__c` picklist on Opportunity (values: Customer Budget, Procurement Delay, Competitive, Scope Change, Internal Approval, Other)
- `Slip_Commentary__c` long text on Opportunity (required when Slip_Reason\_\_c = Other)
- `Slip_Count__c` integer rollup on Opportunity (incremented via Flow on CloseDate forward move >14 days)

**New reports to build (3):**

- Quota-vs-pipeline comparison report for widget 1
- Top 10 deals report for widget 3
- 2x2 Approval matrix report for widget 4

### Action: stakeholder decision required

This is a **widget consolidation conversation** that needs 9 Sales Directors to agree to a simpler view. Options to present:

- **Option A (recommended per research):** trim to 8 widgets, follow Stephen Few / Gartner / Tufte best practices, consolidate duplicates. Gain: faster monthly reviews, higher-signal per widget. Cost: some drill-through workflow changes.
- **Option B (status quo):** keep 15 widgets. Gain: no conversation needed. Cost: cognitive overload during live reviews; widgets compete for attention.

Recommend Option A but DO NOT execute without stakeholder sign-off. This addendum is the design artifact; the executor is a follow-up phase.

## Load-bearing finding 2 — LoggedInUser mode cannot be scheduled

### Finding

From the Salesforce dashboard best practices research:

- **SpecifiedUser mode**: dashboard frozen to the specified user's data-visibility. Schedulable and emailable. Org cap: unlimited.
- **LoggedInUser (dynamic) mode**: each viewer sees their own role-hierarchy slice. **NOT schedulable, NOT emailable.** Org cap: 5 on Enterprise, 200 on Unlimited.

### Why this matters

The original pristine design doc recommended `canChangeRunningUser = true` + `LoggedInUser` mode as the target for per-Director scoping. **But if the Directors want the dashboard emailed to them on a schedule** (a common monthly-review workflow), LoggedInUser blocks that — they'd need 9 SpecifiedUser clones.

### Decision tree

```
Does each Director need the dashboard emailed to them daily/weekly?
├── NO → LoggedInUser + dashboard filter combos (ORIGINAL RECOMMENDATION, still correct)
│        • Single dashboard, 9 filter combos, each Director opens their view
│        • Security flows from role hierarchy
│        • Best maintenance story
│
└── YES → Cannot use LoggedInUser
          │
          Can SimCorp's edition support the required number of dynamic dashboards?
          │   (5 cap on Enterprise, 200 on Unlimited — verify license)
          │
          ├── YES → Dynamic dashboards (one per Director, running user = Director)
          │         • 9 dynamic dashboards, each shows their data
          │         • Schedulable ✓
          │         • Maintenance cost: 9x edits per widget change
          │
          └── NO (Enterprise + needs >5) → SpecifiedUser clones per Director
                    • 9 separate static dashboards with hardcoded "run as Director X"
                    • Schedulable ✓
                    • Maintenance cost: 9x
                    • Security risk: data leaks if wrong user is "Specified"
```

### Action

1. **Confirm SimCorp's Salesforce edition** (Enterprise or Unlimited) to know the dynamic dashboard cap
2. **Confirm the Directors' scheduling requirement**: do they need daily/weekly emails or do they open the dashboard manually?
3. Based on the answers, pick the right tree branch above
4. Update `2026-04-08-manual-ui-runbook.md` §1 with the correct running-user target

## Load-bearing finding 3 — slipped deals schema (Slip_Reason**c / Slip_Count**c)

### Finding

Jason Jordan's _Cracking the Sales Management Code_ (McGraw-Hill 2011) and consistent community practice: **root-cause commentary must be captured at the slip event, not retrospectively.** The pattern:

1. Add `Slip_Reason__c` picklist to Opportunity (Customer Budget / Procurement Delay / Competitive / Scope Change / Internal Approval / Other)
2. Add `Slip_Commentary__c` long text (required when reason = Other)
3. Add `Slip_Count__c` integer rollup (incremented by Flow when CloseDate moves forward >14 days)
4. **Flow/validation rule** triggered at `CloseDate` forward move: forces the rep to pick a reason + optional commentary before save

### Why this matters

The current `Close Date Slipped by Stage` widget shows WHICH deals slipped but not WHY. The Director's monthly review becomes an interrogation ("why did this one slip?"). With the schema in place, the widget can be grouped by `Slip_Reason__c` and the monthly review becomes "what's the biggest slip category this month?" — a category-level conversation rather than an interrogation.

### Even more valuable: the Chronic Slippage widget

A widget showing Opportunities with `Slip_Count__c >= 2` — deals that have slipped twice. Per the research: **"This is the highest-signal widget in the entire monthly review."** Repeat-slip deals are either stuck in customer procurement hell or being misforecast; either way, the Director needs to intervene.

This widget does not currently exist on Dashboard 1. Adding it requires the schema change above, then a 5-minute dashboard PATCH.

### Action

1. Metadata deploy: add the 3 fields above (1 picklist, 1 long text, 1 rollup/formula)
2. Build the Flow that triggers on CloseDate forward-move
3. Run a backfill script to set `Slip_Count__c` for opportunities that have historically slipped (from OpportunityFieldHistory)
4. Add the "Chronic Slippage" widget to Dashboard 1 via PATCH
5. Update `Close Date Slipped by Stage` widget to group by `Slip_Reason__c`

## Load-bearing finding 4 — commercial approval 2x2 matrix, not list

### Finding

From the research: approval-flow dashboards should use a **2x2 matrix (Approval Status × Stage)**, not a filtered list. The "Land Stage 3 with no approval" cohort becomes a specific quadrant (top-right danger) with color + count + $ badge, rather than being buried in a filter.

### Current state vs finding

Dashboard 1 currently has **4 separate widgets** for commercial approval:

- Widget 2: Land Stage 3 Missing Approval by Region (candidates list)
- Widget 9: Commercial Approval Current State (global status)
- Widget 13: Commercial Approval Candidates by Stage (stage breakdown)
- Widget 14: Commercial Approval Approved YTD (approved list)

This is **4 widgets carrying 1 concept**. The 2x2 matrix replaces all 4 with a single widget that shows the full landscape in one view.

### Action

Consolidation opportunity flagged in the 8-widget blueprint above. Concrete steps:

1. Build a new SUMMARY report grouped by (Stage_20_Approval × StageName), aggregating RowCount + `APTS_Opportunity_ARR__c.CONVERT`
2. Configure as a matrix or flex table widget in the dashboard builder
3. Retire the 4 current widgets
4. Save 3 widget slots (9 widgets instead of 12 for the approval concept before the further consolidation)

## Load-bearing finding 5 — widget count audit flag

### Finding

The research converges on 6-9 widgets as the executive ceiling. The existing `dashboard_state_dump.py` audit script doesn't check this.

### Recommended change

Add a `widget-count-over-exec-ceiling` warning (not active flag — this is a design choice, not a defect) when a dashboard type is classified as "executive" and exceeds the count.

```python
EXECUTIVE_DASHBOARDS: set[str] = {
    "01ZTb00000FSP7hMAH",  # Sales Directors Monthly — explicitly an executive review surface
}
EXECUTIVE_WIDGET_CEILING_TARGET = 8
EXECUTIVE_WIDGET_CEILING_MAX = 12

# Add to the widget-level flag check:
if dashboard_id in EXECUTIVE_DASHBOARDS and widget_count > EXECUTIVE_WIDGET_CEILING_TARGET:
    if widget_count > EXECUTIVE_WIDGET_CEILING_MAX:
        flags.append(f"widget-count-over-max:{widget_count}/{EXECUTIVE_WIDGET_CEILING_MAX}")
    else:
        flags_warning.append(f"widget-count-over-target:{widget_count}/{EXECUTIVE_WIDGET_CEILING_TARGET}")
```

### Action

Script update deferred — adding a WARNING tier to the script is a small improvement but doesn't change any data. Can be done in the next pass.

## Load-bearing finding 6 — metric registry is the future-state architecture

### Finding

The data engineering research provided a complete metric registry YAML schema (dbt MetricFlow / LookML / Cube.dev pattern applied to the Salesforce Reports substrate). Key elements:

- **One YAML file per metric** at `reporting-layer/metrics/<metric_name>.yml`
- **One YAML file per source report** at `reporting-layer/contracts/report_<id>.yml`
- **One YAML file per dashboard** at `reporting-layer/dashboards/<dashboard_name>.yml` (binds widgets to metrics)
- **Metric versioning** via semver (`1.0.0` → `1.1.0` additive, `2.0.0` breaking)
- **Hash-based drift detection** — CI job pulls describe, normalizes, hashes, compares
- **Lineage graph** generated on merge at `lineage/graph.json`

### Why this matters

This is the **long-term target architecture** for the reporting layer. It formalizes what's currently in `docs/specs/report-1-source-contract.md` and `docs/specs/report-2-source-contract.md` into a structured, programmatically diffable format. The existing source contracts are a proto-registry; the YAML schema is the industrial-strength version.

### Action

**Not immediately required, but documented for the next major phase.**

Proof-of-concept stub that could be built in 1 day:

1. Create `reporting-layer/` directory in the crm-analytics repo
2. Write 3 example metric YAML files: `new_business_arr.yml`, `renewal_acv.yml`, `pipeline_coverage.yml`
3. Write 3 example source contract YAML files for those metrics' reports
4. Write a Python script that reads the metric YAML + pulls the report describe + verifies the hash matches
5. Wire the verification script into `make verify` or a GitHub Action

This becomes "Phase 5" after the current remaining work (UI handoff, schema change, Phase 3/4 deck rebuild) lands.

## Load-bearing finding 7 — data quality theater avoidance

### Finding

Three rules from the research (opinion-marked but consistent across multiple sources):

1. **Route to owners, not dashboards.** A DQ flag that fires as a Chatter post on the opportunity owner's feed is actionable; the same flag on a dashboard is wallpaper.
2. **Cap flags per owner per week.** If 200 flags fire on one rep, 0 get fixed. Cap at 10 per week, prioritize by ARR × severity.
3. **Measure fix rate, not flag count.** The dashboard tile should be "% of flags resolved in 7 days" — if <60%, the algorithm is over-firing.

### Why this matters

The current Dashboard 2 (Sales Ops Quarterly) has 5 data quality widgets, all of which show COUNTS of non-compliant opportunities. None show FIX RATES. This is the theater pattern.

### Action

Dashboard 2 enhancement proposal (defer until Sales Ops has a conversation):

- Replace each "count of non-compliant" widget with a "% fixed in last 7 days" widget
- Add a chatter automation that routes each flag to the opportunity owner
- Cap flags per owner per week via a Flow

This is a multi-week improvement and is out of scope for the current audit/improvement pass. Captured here for future reference.

## Summary of changes from research

### Changes requiring stakeholder conversation

1. **Widget count consolidation on Dashboard 1** (15 → 8). High-impact conversation with 9 Directors.
2. **Running user mode decision** (LoggedInUser vs SpecifiedUser) based on scheduling requirement.
3. **Schema change for slipped deals** (`Slip_Reason__c` / `Slip_Count__c` / Flow).
4. **Dashboard 2 data quality widget rework** (counts → fix rates + Chatter routing).

### Changes requiring metadata deploy

1. **`Slip_Reason__c` picklist on Opportunity**
2. **`Slip_Commentary__c` long text on Opportunity**
3. **`Slip_Count__c` rollup/formula on Opportunity**
4. **`Calendar_Quarter__c` formula field on Opportunity** (already flagged in original design doc; still the top metadata deploy)

### Changes requiring new reports

1. Quota-vs-pipeline report for the Coverage KPI widget
2. Top 10 deals report for the Top 10 widget
3. Commercial Approval 2x2 matrix report (consolidates 4 existing widgets)

### Changes requiring dashboard PATCHes

1. Add the 3 new widgets above to Dashboard 1
2. Retire the 7 widgets being consolidated or moved
3. Add Chronic Slippage widget once Slip_Count\_\_c exists
4. Re-group Close Date Slipped widget by Slip_Reason\_\_c once it exists

### Changes requiring future-state architecture

1. Port source contracts to metric registry YAML format (Phase 5)
2. Add widget-count-over-exec-ceiling flag to `dashboard_state_dump.py`
3. Generate lineage graph at `lineage/graph.json`

## Research sources

All three research subagents provided comprehensive source lists. Most load-bearing:

- **Stephen Few**, _Information Dashboard Design_, 2nd ed., Analytics Press 2013 — executive widget count ceiling
- **Gartner 2023** "Chief Sales Officer Leadership Vision" — senior-leader attention budget per widget
- **Jason Jordan**, _Cracking the Sales Management Code_, McGraw-Hill 2011 — slipped-deal root cause capture pattern
- **Mark Roberge**, _The Sales Acceleration Formula_, Wiley 2015 — the "Top 10 deals" list pattern
- **David Skok**, _SaaS Metrics 2.0_ — ARR vs ACV canonicalization
- **Salesforce Help** — filter limits, running user modes, chart types, performance optimization (full list in subagent transcripts)
- **dbt Labs** — semantic layer + source contract patterns (metric registry YAML)
- **Cube.dev** — pre-aggregations as the execution substrate (applies to Salesforce Reports as non-warehouse materialized data)
- **Salesforce Well-Architected framework 2023** — data quality pillar

## What does NOT change from the 2026-04-08 pristine design

1. Revenue aggregate hierarchy (ARR / Forecast_ARR / Renewal_ACV with `.CONVERT`)
2. Calendar framing (never fiscal)
3. Source-contract pinning
4. One filterable dashboard (not N clones) — still the right call, just with a caveat about scheduling
5. No `AMOUNT` in revenue widgets
6. FlexTable dual-storage PATCH pattern
7. The current state: 0 active flags, 4 deferred flags (fiscal-grouping schema change still the #1 deployment)

All of this holds. The research extends the design with **widget count targets**, **schema recommendations**, and the **metric registry architecture** for Phase 5.
