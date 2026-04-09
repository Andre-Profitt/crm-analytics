# Pristine Dashboard Design — Sales Director Monthly + Sales Ops Quarterly

Date: 2026-04-08
Supersedes: (nothing; this is a new target-state document)
Complements: `2026-04-08-sales-director-monthly-handoff.md`, `specs/report-1-source-contract.md`, `specs/report-2-source-contract.md`, `audits/2026-04-08-deep-audit-against-stakeholder-goals.md`

## Purpose

Define the "pristine" target state for both dashboards — the bar every future improvement should converge toward. Pristine means:

1. **Every widget passes data-quality hard rules** (calendar framing, ARR/ACV canonical fields with `.CONVERT` for multi-currency rollup, no `AMOUNT` aggregates, no "Fiscal" in widget titles)
2. **Every widget maps to a stakeholder ask** or explicit Sales Ops function — no orphans, no cruft
3. **Every widget has a canonical source report** pinned in the source contract
4. **Every source report is diffable** — its aggregates, groupings, filters, and detail columns are scripted + verifiable
5. **Every per-Director view is achievable via dashboard filters** — no hard-coded regional clones
6. **The runningUser permission model is deliberate** (SpecifiedUser for fixed auditor view, LoggedInUser for per-Director scoping)

## State of the world — 2026-04-08

After this session's improvement pass:

**Dashboard 1 — Sales Directors Monthly** (`01ZTb00000FSP7hMAH`)

- 15 / 20 widgets, 0 filters, SpecifiedUser mode, canChangeRunningUser = false
- Every revenue aggregate now uses `.CONVERT` (multi-currency correct)
- Only 3 remaining defects: `FISCAL_QUARTER` grouping on 3 widgets (requires bucket field or custom formula — deferred, schema change)
- Full stakeholder goal coverage verified widget-by-widget (see §Goal coverage map below)

**Dashboard 2 — Sales Ops Quarterly KPI** (`01ZTb00000FSP9JMAX`)

- 18 / 20 widgets, 0 filters, SpecifiedUser mode
- Every revenue aggregate now uses `.CONVERT`
- `s!AMOUNT` widget binding on No Activity 30+ eliminated (replaced with `APTS_Opportunity_ARR__c.CONVERT` — required the FlexTable dual-storage fix for `visualizationProperties.tableColumns`)
- Only 1 remaining defect: `FISCAL_QUARTER` grouping on Overdue Opportunities (same schema deferral)

## Pristine design principles (applied and locked)

### P1. Revenue aggregate hierarchy

For every widget that sums, averages, or counts a revenue figure, use the canonical field for its domain:

| Domain                                                                    | Canonical field                        | Form                                                                       |
| ------------------------------------------------------------------------- | -------------------------------------- | -------------------------------------------------------------------------- |
| Pipeline value, commercial approval, stage aging, forecast accuracy (ARR) | `Opportunity.APTS_Opportunity_ARR__c`  | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`                            |
| Forecast category rollup, commit/best-case                                | `Opportunity.APTS_Forecast_ARR__c`     | `s!Opportunity.APTS_Forecast_ARR__c.CONVERT`                               |
| Renewal value, ACV-at-risk                                                | `Opportunity.APTS_Renewal_ACV__c`      | `s!Opportunity.APTS_Renewal_ACV__c.CONVERT`                                |
| Business-at-risk proxy (CRM-side)                                         | `Opportunity_Average_ACV__c` (formula) | `s!Opportunity.Opportunity_Average_ACV__c` (no CONVERT — average, not sum) |

**Do not use** the standard `AMOUNT` field on any widget. It's per-opportunity-currency and creates mixed-currency totals.

**Do not use** the non-`.CONVERT` form on any multi-sum aggregate. Any widget that rolls up across records in different currencies needs `.CONVERT` or it produces nonsense.

Every report that gets a `.CONVERT` aggregate must also have the corresponding `.CONVERT` column in its `detailColumns` array — Salesforce rejects the aggregate-only form.

### P2. Calendar framing (not fiscal)

Every widget that has a date filter or a date grouping must use calendar framing, not fiscal:

- **Date filters:** `THIS_QUARTER`, `THIS_YEAR`, `LAST_QUARTER` — **not** `THIS_FISCAL_QUARTER`, `THIS_FISCAL_YEAR`. State-check widgets (no time dimension) should use `CUSTOM` with unbounded dates.
- **Date groupings:** calendar quarter buckets — **not** `FISCAL_QUARTER`. Limitation: the Reports API will not accept `CALENDAR_QUARTER` as a grouping column name (verified empirically — rejected with "not a valid column for groupings"). The only way to achieve calendar-quarter grouping is a **bucket field or custom formula field** on the Opportunity object. 4 widgets are currently deferred on this basis:
  - D1 Widget 1 — Renewal ACV by Quarter (`00OTb000008ekxBMAQ`)
  - D1 Widget 6 — Forecast Accuracy (`00OTb000008TZsDMAW`)
  - D1 Widget 8 — Renewals by Quarter (`00OTb000008eksLMAQ`)
  - D2 Widget 15 — Overdue Opportunities (`00OTb000008SrmLMAS`)

Recommended schema change (one of):

1. **Bucket field on Opportunity**: a read-only bucket that maps `CloseDate` to calendar quarter strings (`2026-Q1`, `2026-Q2`, ...). Cheapest option. Zero apex, no deploy needed beyond the bucket definition in the report itself — but buckets are per-report, not reusable across reports.
2. **Custom formula field on Opportunity** (`Calendar_Quarter__c` or similar): a picklist or text formula derived from `CloseDate`. Reusable across all reports. Requires a metadata deploy.

Once either is in place, the 4 deferred widgets can switch their grouping column in a trivial PATCH.

### P3. Widget naming convention

- **Header text** should be human-readable and free of dev-phase prefixes (`P2.7`, `P2.8`, etc. — cosmetic only, should be removed in a pristine pass)
- **Report name** should mirror widget header when possible
- **Developer name** should be snake_case and spec-stem-match (e.g., `Pipeline_Overview_This_Quarter` for `pipeline_overview_global`)
- **Title** and **footer** fields on the dashboard component should be used sparingly — header is the primary label

### P4. Source-contract pinning

Every widget's source report ID must appear in either `report-1-source-contract.md` or `report-2-source-contract.md`. Any widget whose report is not in the contract is either:

- An orphan (candidate for deletion)
- A new widget pending contract amendment

No exceptions. The contracts are the source of truth for Phase 4 deck rebuild.

### P5. One filterable dashboard, not N clones

The Sales Director Monthly dashboard serves 9 named Directors via a single filterable dashboard + 9 preset filter combos (documented in `report-1-source-contract.md` §Phase 2.8). **Do not** clone the dashboard per Director. The filter architecture:

- Global filter 1: `Opportunity.Sales_Region__c` (7 values)
- Global filter 2: `Account.Legal Country` (Canada, United States)
- Global filter 3 (aspirational): `Account.Industry` with SimCorp-specific values — blocked pending Industry picklist investigation

Filter creation is Lightning-UI-only (Analytics REST API rejects filter CREATE on classic dashboards). Documented in the manual runbook.

### P6. `canChangeRunningUser` and running user semantics

Current state: `SpecifiedUser` mode, runningUser = Andre Profitt, `canChangeRunningUser = false`. This means ANY user opening the dashboard sees Andre's data-visibility scope — a hard pin that doesn't respect role hierarchy.

**Pristine target for Dashboard 1:** `canChangeRunningUser = true` AND runningUser = `LoggedInUser` (alternative: keep SpecifiedUser but allow each Director to override). That way each Director's data visibility flows from their own role hierarchy, not Andre's — which is the correct security model for per-person monthly reviews.

This flag is Lightning-UI-only to flip (REST API returns 200 but silently ignores the change). Documented in the manual runbook.

## Goal coverage map (Dashboard 1)

Every Dashboard 1 widget maps back to a stakeholder ask. No cruft.

| Widget                                      | Report ID            | Stakeholder ask                                     | Notes                                            |
| ------------------------------------------- | -------------------- | --------------------------------------------------- | ------------------------------------------------ |
| 3. Pipeline Overview by Stage               | `00OTb000008fBfdMAE` | Pipeline overview (global, quarterly focus)         | Phase 2.7 canonical                              |
| 10. Pipeline Coverage by Stage              | `00OTb000008TZc5MAG` | Pipeline overview (supplementary, forecast ARR)     |                                                  |
| 5. New Customers (Land) by Region           | `00OTb000008ekqjMAA` | Pipeline overview (Land deal context)               |                                                  |
| 9. Commercial Approval Current State        | `00OTb000008fBEDMA2` | Commercial Approval overview (global)               | Phase 2.7 canonical                              |
| 14. Commercial Approval Approved YTD (Land) | `00OTb000008aTtJMAU` | Commercial Approval overview (approved list)        |                                                  |
| 13. Commercial Approval Candidates by Stage | `00OTb000008ekp7MAA` | Commercial Approval (candidates list)               | ARR fix Phase 2.8                                |
| 2. Land Stage 3 Missing Approval by Region  | `00OTb000008ekltMAA` | Commercial Approval (Land stage 3 no-approval list) | ARR fix Phase 2.8                                |
| 4. Renewal Pipeline This Quarter            | `00OTb000008ektxMAA` | Renewals tracking (this quarter, value)             | ACV.CONVERT fix 2026-04-08                       |
| 1. Renewal ACV by Quarter                   | `00OTb000008ekxBMAQ` | Renewals tracking (by quarter, value)               | Fiscal grouping deferred                         |
| 8. Renewals by Quarter                      | `00OTb000008eksLMAQ` | Renewals tracking (historical context)              | Renamed off "Fiscal"; ACV.CONVERT fix 2026-04-08 |
| 15. Renewal Likelihood by Probability       | `00OTb000008fBULMA2` | Renewals tracking (likelihood dimension)            | Phase 2.7 canonical                              |
| 11. Business At Risk                        | `00OTb000008Ta9xMAC` | Churn Risk (proxy until Finance feed)               | CRM-side only; Finance feed pending Alex P       |
| 7. Close Date Slipped by Stage              | `00OTb000008eknVMAQ` | Slipped deals analysis (interim, ARR-fixed)         | PI native source deferred                        |
| 6. Forecast Accuracy                        | `00OTb000008TZsDMAW` | Forecast context (adjacent, not core Track 1)       | Fiscal grouping deferred                         |
| 12. Forecast and Closed Won                 | `00OTb000008TZaTMAW` | Forecast context (funnel view)                      |                                                  |

**Coverage: 5/5 stakeholder bullets addressed** (pipeline overview, commercial approval, renewals, churn proxy, slipped deals). The 9 Director per-person views require the Lightning UI filter handoff.

## Goal coverage map (Dashboard 2)

| Spec section                                 | Live coverage  | Remaining work                                                                    |
| -------------------------------------------- | -------------- | --------------------------------------------------------------------------------- |
| CRM Data Quality (5 widgets)                 | 4/5 OK         | `dq_missing_quote_type` blocked on retire/repurpose decision                      |
| Process Compliance (5 widgets)               | 5/5 OK         | All 5 built Phase 2.6 with documented simplifications                             |
| Forecast Accuracy (4 widgets, all PI native) | 0/4 BLOCKED    | Pipeline Inspection Lightning UI list view config required for all 4              |
| Pipeline Hygiene (8 widgets)                 | 7/8 OK + 1 WIP | `ph_probability_mismatch_by_stage` Under Construction pending Sales Ops threshold |

## Improvements applied in this session (2026-04-08 second pass)

All via Analytics REST API PATCH, all verified via inline GET:

### Dashboard 1

1. **Forecast Accuracy** (`00OTb000008TZsDMAW`) — widget binding upgraded from `APTS_Forecast_ARR__c` to `APTS_Forecast_ARR__c.CONVERT`. Also added `.CONVERT` to source report aggregates + detail columns; retired the non-CONVERT form.
2. **Pipeline Coverage by Stage** (`00OTb000008TZc5MAG`) — same upgrade pattern as above.
3. **Renewal ACV by Quarter** (`00OTb000008ekxBMAQ`) — ACV field upgraded to `.CONVERT`.
4. **Renewal Pipeline This Quarter** (`00OTb000008ektxMAA`) — ACV field upgraded to `.CONVERT`.
5. **Renewals by Quarter** (`00OTb000008eksLMAQ`) — ACV field upgraded to `.CONVERT`.

### Dashboard 2

6. **No Activity 30+ Days** (`00OTb000008TaEnMAK`) — widget binding changed from `s!AMOUNT` to `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`. **Required the FlexTable dual-storage fix**: the aggregate is stored in BOTH `properties.aggregates` and `properties.visualizationProperties.tableColumns[].column`, and both must be PATCHed together or Salesforce silently reverts the change.
7. **High Value Stale Deals** (`00OTb000008Ti97MAC`) — source report `Forecast_ARR` upgraded to `.CONVERT`; non-CONVERT form retired.
8. **Cleanup pass** — 7 source reports had stale non-CONVERT aggregates left behind from the addition; all removed.

### Locked-in finding: fiscal grouping requires schema change

Attempted `FISCAL_QUARTER` → `CALENDAR_QUARTER` grouping swap via PATCH on all 4 deferred reports. All 4 rejected by Salesforce:

```
"The column CALENDAR_QUARTER is not a valid column for groupings."
"The column CLOSE_DATE_CALENDAR_QUARTER is not a valid column for groupings."
```

This confirms the deep audit's schema-change deferral. The fix requires a **bucket field** (per-report) or a **custom formula field** (reusable across reports) on the Opportunity object. Either option needs a metadata deploy, not a REST PATCH.

## Drift detection & "pristine" audit script

The improvements above were driven by a new audit script at `/tmp/full-state-dump.mjs` (one-off — should be ported into the repo at `scripts/dashboard_state_dump.py` or similar for repeatability). It walks both dashboards and every source report, cross-references widget bindings with report aggregates, and emits flags for:

- `no-convert:<field>` — revenue aggregate missing `.CONVERT`
- `amount-not-arr-on-widget` — widget binding uses `s!AMOUNT`
- `fiscal-date-filter:<form>` — fiscal framing on date filter
- `fiscal-grouping:<col>` — fiscal framing on grouping

A pristine dashboard is one that emits zero flags. As of 2026-04-08 evening pass, both dashboards emit only the 4 deferred fiscal-grouping flags — nothing else.

**Recommendation: port this script into `scripts/` as a reusable audit tool** and wire it into CI or into a daily watcher run (the Frontier OS `overnight-review` pattern works here — the Frontier OS session ledger can track dashboard state deltas across runs).

## Manual UI runbook (not automatable)

See `2026-04-08-manual-ui-runbook.md`.

## What's next

1. **5-minute UI handoff** — add the 3 Dashboard 1 filters + flip `canChangeRunningUser` to true. Unblocks per-Director scoping.
2. **Schema change** — add `Calendar_Quarter__c` formula field on Opportunity (or per-report bucket fields) to unlock the 4 fiscal-grouping deferrals.
3. **Industry picklist investigation** — SOQL to identify the SimCorp-values industry field. Unblocks Patrick/Adam NAM sub-cuts.
4. **Pipeline Inspection Lightning UI list view creation** — 2 PI list views for the 4 forecast accuracy widgets on D2 + the slipped deals widgets on D1.
5. **Stakeholder decisions** — 3 pending: `dq_missing_quote_type` retire/repurpose, `ph_probability_mismatch_by_stage` threshold, Finance churn feed handshake.
6. **Phase 3/4 deck rebuild** — can now proceed against the 2 pinned source contracts; most data defects are cleared.
