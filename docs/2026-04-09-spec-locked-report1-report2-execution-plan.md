# Spec-Locked Execution Plan - Report 1 + Report 2

Date: 2026-04-09

## Purpose

Freeze the work against the stakeholder brief so future Salesforce changes do not drift into generic dashboard optimization.

This plan defines:
- the only valid target for `Report 1` and `Report 2`
- the current live baseline we are improving from
- the exact MD-1 audience map
- the canonical Salesforce source map per content module
- the guardrails for any future live report or dashboard change

## Authoritative Source Of Truth

The stakeholder brief is the governing spec. The important hard requirements are:

- `Report 1` is the monthly Sales Director pack in PowerPoint.
- `Report 1` must be forward-looking and insight-driven.
- `Report 1` must cover:
  - pipeline overview with quarterly focus
  - commercial approval overview
  - list of Land stage 3 opportunities with no commercial approval
  - renewals tracking: this quarter, value, likelihood
  - churn risk and trends
  - slipped deals analysis with root cause commentary
- `Report 1` must support one report per MD-1.
- `Report 2` is the quarterly Sales Ops pack in PowerPoint.
- `Report 2` must track:
  - CRM data quality
  - process compliance
  - forecast accuracy
  - pipeline hygiene
- `Report 2` must also have a KPI dashboard behind it.

The following stakeholder corrections are also binding:

- `0 - No Opportunity` must not count as missing win/loss reason.
- The label should be `Missing Win/Loss Reason`.
- `Overdue close date open Opps` should sort by largest record count, not by owner.
- `KYC missing` means accounts without KYC approval.
- Renewal amount should be `ACV`.
- Missing commercial approval overview and list of opportunities must be explicit.

## Current Live Baseline

As of 2026-04-09:

- `Dashboard 1` `01ZTb00000FSP7hMAH` summary:
  - `8 widgets`
  - `0 active flags`
  - `0 deferred flags`
- `Dashboard 2` `01ZTb00000FSP9JMAX` summary:
  - `18 widgets`
  - `0 active flags`
  - `1 deferred flag`

Interpretation:

- The live org is not in drift technically.
- `Dashboard 1` is still an interim operating surface, not the final product definition for `Report 1`.
- `Dashboard 2` is a usable KPI baseline, but stakeholder-fit work still remains.

Current live `Dashboard 1` component set after the first spec-alignment pass:

- `Pipeline Overview by Stage`
- `Commercial Approval Current State`
- `Renewal Likelihood by Probability`
- `Business At Risk`
- `Commercial Approval Approved YTD (Land)`
- `Commercial Approval Candidates by Stage`
- `Renewal Pipeline This Quarter`
- `Close Date Slipped by Stage`

MD-1 preset validation status:

- complete on 2026-04-09
- `72 / 72` live report executions succeeded across `9` presets x `8` current D1 sources
- see [2026-04-09-d1-md1-preset-validation.md](/Users/test/crm-analytics/docs/audits/2026-04-09-d1-md1-preset-validation.md)

## Preserve These Live Upgrades

Do not accidentally revert these live report-level improvements while rebuilding the surfaces:

- `00OTb000008ekqjMAA` `New Customers (Land) by Region`
  - enriched to expose account, opportunity, owner, close date, and ARR detail
- `00OTb000008Ta9xMAC` `Business At Risk`
  - upgraded into a risk worklist surface with account, opportunity, owner, stage, close date, ACV, and risk update date
- `00OTb000008eknVMAQ` `Close Date Slipped by Stage`
  - upgraded into a tabular worklist with account, opportunity, owner, stage, close date, age, and ARR
- `00OTb000008ekp7MAA` `Commercial Approval Candidates by Stage`
  - enriched to carry account, opportunity, owner, close date, next step, forecast ARR, and opportunity ARR
- `00OTb000008ekltMAA` `Land Stage 3 Missing Approval`
  - enriched underneath the grouped summary so it can support an actual candidate list
- `00OTb000008fBfdMAE` `Pipeline Overview by Stage`
  - enriched with account, opportunity, owner, close date, next step, and ARR underneath the overview chart

## Anti-Drift Rules

1. A change is only valid if it serves a named stakeholder bullet from the brief.
2. `Report 1` and `Report 2` are the products. Dashboards are implementation surfaces behind them.
3. No live dashboard add, remove, or replacement is allowed unless the change notes say:
   - which report it serves
   - which module it serves
   - which audience it serves
   - which existing surface it replaces or why it is required
4. No widget survives just because it exists today. Every widget must earn a place in the target slide map below.
5. CRM proxy surfaces must be labeled as proxies until the Finance feed or owner-commentary workflow exists.
6. Patrick and Adam must be treated as distinct North America outputs. If Salesforce filter UX cannot cleanly express the split, the deck generation layer must do so explicitly instead of pretending the dashboard solved it.
7. Every live PATCH must have:
   - before metadata snapshot
   - after metadata snapshot
   - readback verification
8. Any future consolidation work must be done against the module map below, not by reducing widget count for its own sake.

## Report 1 Target Product

`Report 1` is a monthly MD-1 pack in PowerPoint, not a screenshot export of `Dashboard 1`.

The fixed content structure is:

| Slide | Module | Scope | Required output |
| --- | --- | --- | --- |
| 1 | Pipeline overview with quarterly focus | MD-1 specific | One slide per MD-1 showing current-quarter pipeline by stage and the forward-looking read |
| 2 | Commercial approval overview | Global | One global approval-state slide showing approved vs not approved state clearly |
| 3 | Missing commercial approval candidates | MD-1 specific | One slide listing Land stage 3 opportunities without approval for that MD-1 book |
| 4 | Renewals tracking | MD-1 specific | One slide showing what renews this quarter, the ACV value, and likelihood of renewal |
| 5 | Churn risk and trends | MD-1 specific with Finance overlay | One slide showing CRM risk now and Finance trend input when available |
| 6 | Slipped deals analysis | MD-1 specific | One slide showing slipped deals plus owner follow-up / root-cause commentary process |

Important scoping rules:

- `Slide 2` is global and can be reused across all MD-1 packs.
- `Slides 1, 3, 4, 5, 6` must all be filterable or reproducible per MD-1.
- Any extra slide is optional only if it replaces a weak required slide or materially improves actionability without diluting the six required modules.
- `Top 10 Deals by ARR This Qtr` and `Commercial Approval 2x2 Matrix` are enhancement candidates, not mandatory deliverables by themselves.

## Report 1 Canonical Salesforce Source Map

Use this source map to decide what feeds each `Report 1` module.

| Module | End-state source | Interim live source | Notes |
| --- | --- | --- | --- |
| Pipeline overview with quarterly focus | `00OTb000008fBfdMAE` `P2.7 Pipeline Global This Qtr` | same | Canonical summary source. Director-specific delivery comes from MD-1 presets, not separate hard-coded reports. |
| Commercial approval overview | `00OTb000008fQ6nMAE` `Commercial Approval 2x2 Matrix` | `00OTb000008fBEDMA2` `P2.7 Commercial Approval Global` | End-state should consolidate approval state cleanly. Approved-deals visibility must still remain explicit. |
| Approved commercial approvals reference | `00OTb000008aTtJMAU` `Commercial Approval approved 2026` | currently not explicit on live D1 | Keep as the approved-deals source even if the dashboard chooses a stronger summary chart. |
| Missing commercial approval candidates | `00OTb000008ekltMAA` `Land Stage 3 Missing Approval` | `00OTb000008ekp7MAA` and `00OTb000008ekltMAA` | `00OTb000008ekltMAA` is the most spec-faithful candidate list. `00OTb000008ekp7MAA` remains a useful supporting grouped view. |
| Renewals tracking: this quarter and ACV | `00OTb000008ektxMAA` `Renewal Pipeline This Quarter` | same | Keep ACV as the canonical amount field. |
| Renewals tracking: likelihood | `00OTb000008fBULMA2` `P2.7 Renewal Likelihood This Qtr` | same if restored into the delivery flow | Required by the stakeholder brief even if not currently prominent on live D1. |
| Renewals tracking: quarter framing | calendar-quarter version of `00OTb000008ekxBMAQ` | current `00OTb000008ekxBMAQ` is still a fiscal-quarter compromise | Calendar-quarter framing remains a known schema dependency. |
| Churn risk and trends | Finance feed overlay + `00OTb000008Ta9xMAC` `Business At Risk` | same | CRM is interim only. Finance dependency is still open via Alex P. |
| Slipped deals analysis | Pipeline Inspection list views + owner commentary workflow | `00OTb000008eknVMAQ` `Close Date Slipped by Stage` | Current report is the interim detection surface. Root-cause commentary is still a process dependency. |

## MD-1 Output Map

This is the target audience map for `Report 1`. These are the intended book definitions, even if the current dashboard UI needs saved states or deck-side filtering to express them cleanly.

| MD-1 | Territory | Target slice definition |
| --- | --- | --- |
| Megan Miceli | Canada | `Sales Region = North America AND Legal Country = Canada AND Account Unit Group = SC North America` |
| Patrick Gaughan | NA Asset Management | `Sales Region = North America AND Legal Country = Exclude Canada AND Account Unit Group = SC North America AND Industry IN (Asset Management, Bank, Wealth Management, Asset Servicer, Other)` |
| Jesper Tyrer | APAC | `Sales Region = APAC AND Account Unit Group = SC Asia` |
| Sarah Pittroff | Central Europe | `Sales Region = Central Europe AND Account Unit Group = SC EMEA` |
| Francois Thaury | Southern Europe | `Sales Region = Southwestern Europe AND Account Unit Group = SC EMEA` |
| Dan Peppett | UK & Ireland | `Sales Region = United Kingdom & Ireland AND Account Unit Group = SC EMEA` |
| Christian Ebbesen | NL & Nordics | `Sales Region = Northern Europe AND Account Unit Group = SC EMEA` |
| Mourad Essofi | Middle East & Africa | `Sales Region = Middle East & Africa AND Account Unit Group = SC EMEA` |
| Adam Steinhaus | Pension & Insurance | `Sales Region = North America AND Legal Country = Exclude Canada AND Account Unit Group = SC North America AND Industry IN (Pension, Insurance)` |

Implementation note:

- If the dashboard filter UX cannot cleanly express Patrick and Adam as mutually exclusive saved states, the deck-building or runbook layer must own that split explicitly. Do not silently collapse them into one North America remainder view.

## Report 2 Target Product

`Report 2` is the quarterly Sales Ops pack plus the KPI dashboard behind it.

Its fixed KPI families are:

| KPI family | Required stakeholder meaning | Immediate implementation rule |
| --- | --- | --- |
| CRM data quality | completeness and accuracy of key CRM fields | Keep naming and filter logic aligned to stakeholder language, not generic audit language |
| Process compliance | are the required control steps being followed | Favor exception reporting and ranked worklists over generic bars |
| Forecast accuracy | how forecast moves against outcome | Keep this separate from pipeline hygiene and do not fake it with unrelated stage metrics |
| Pipeline hygiene | aging, stage progression, overdue management | Keep ranked operational surfaces that show where intervention is required |

## Report 2 Immediate Fix Backlog

These items are already stakeholder-defined and should stay pinned as the next D2 fit pass:

- Rename the missing-reason surface to `Missing Win/Loss Reason`.
- Exclude `0 - No Opportunity` from the missing-reason logic.
- Keep `Overdue close date open Opps` sorted by largest record count.
- Keep `KYC missing` aligned to accounts without KYC approval.
- Keep renewal value on `ACV`.
- Keep process and hygiene widgets framed as intervention surfaces, not just audit counts.

## Execution Sequence

Future work should follow this order:

1. Freeze baseline before each change set.
   - Snapshot the live report and dashboard metadata.
   - Record which `Report 1` or `Report 2` module the change is for.
2. Finish `Report 1` source alignment before more visual reshuffling.
   - Restore or surface the required approved-deals and renewal-likelihood sources in the delivery flow.
   - Keep the six required modules intact.
3. Validate the 9 MD-1 outputs.
   - Prove each required slice can actually be produced.
   - Escalate any filter-expression limitation instead of papering over it.
4. Execute the D2 stakeholder-fit backlog.
   - Fix naming, filter semantics, and sort order before any cosmetic redesign.
5. Only after the source map is stable, redesign the dashboards.
   - `Dashboard 1` should become the clean operating surface behind the monthly pack.
   - `Dashboard 2` should become the clean KPI surface behind the quarterly pack.

## Out Of Scope Until Dependencies Move

These are real requirements, but they are blocked on non-dashboard dependencies:

- Finance-backed churn trend feed through Alex P / Finance
- slipped-deals root-cause commentary structure
- any calendar-quarter regrouping that depends on schema work rather than a simple report PATCH

## Definition Of Done

This work is on target only when:

- every live dashboard and report change can be traced to a stakeholder bullet
- `Report 1` can be generated as one pack per MD-1 without manual reinterpretation of the dashboard
- `Report 2` reads as a quarterly KPI operating pack rather than a generic dashboard export
- proxy surfaces are clearly labeled until the Finance feed and commentary workflow are real
