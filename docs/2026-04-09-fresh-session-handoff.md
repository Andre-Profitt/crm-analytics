# Fresh Session Handoff — Sales Director Monthly + Sales Ops Quarterly

Date: 2026-04-09
Supersedes: `docs/2026-04-08-sales-director-monthly-handoff.md` (commit `2550fa7`) as the primary onboarding doc. The earlier handoff remains the audit trail for Phases 1-2.8; this doc integrates the 2026-04-08/09 pristine pass on top of it.

> **Start here.** If you're picking this up cold, read this doc first, then `docs/2026-04-08-pristine-dashboard-design.md`, then `docs/2026-04-09-pristine-design-research-addendum.md`. Source contracts (`docs/specs/report-{1,2}-source-contract.md`) are the authoritative per-widget pinnings. The new Phase 5 metric registry at `reporting-layer/` is the forward-facing target architecture.

## TL;DR for a cold read

Two production Salesforce dashboards serve 9 named Sales Directors (Track 1, monthly) and Sales Ops (Track 2, quarterly). The last successful full audit still showed **pristine state** (zero active drift flags, four deferred schema items, one design warning), and D1 has since been reshaped live into an **8-widget executive layout with 4 native dashboard filters**: `Industry`, `Legal Country`, `Sales Region`, and `Account Unit Group`. Most improvements were applied via the Analytics REST API with verified inline GET after PATCH; dashboard filter creation still required the Lightning editor. Final D1 continuation verification was done via direct Analytics API GETs because `scripts/dashboard_state_dump.py` hung on the closing pass. Next work is the running-user/distribution decision, the `Calendar_Quarter__c` metadata deploy, and the remaining D2 Forecast Accuracy / slipped-schema follow-through.

## 1. North Star — the original asks (verbatim from 2550fa7)

### Track 1 — Sales Directors Monthly (Report 1)

Audience: **9 named Sales Directors** (MD-1 level). Cadence: monthly. Format: one filterable dashboard feeding a per-Director deck.

**Stakeholder bullets (verbatim from the brief):**

- A pipeline overview with quarterly focus (one slide per region)
- Commercial Approval overview — which deals have been approved and a list of any Land stage 3 deals with no commercial approval (A global overview (one slide) + the list of candidates by region (one slide))
- Renewals tracking (what renewals are coming up this quarter, what is the value and likelihood of renewing)
- Churn Risk and trends (difficult for now, but let's try and build a slide of what we can get from Finance for now. Please reach out to Alex P and understand from who in Finance he gets this reporting, and please get you involved, so you also receive this going forward.)
- Slipped deals analysis (root cause commentary) — start with slipped deals — root cause commentary most likely need us to reach out to the opportunity owner.

**Stakeholder feedback bullets also captured:**

- "0 — no opportunity, no reason is OK. Maybe rename to Missing Win/Loss Reason"
- "Overdue close date open Opps: sort by largest record count instead of Opps owner"
- "KYC missing: Accounts without KYC Approval"
- "Pipeline Reporting (Sales director monthly). one report per MD-1"
- "Renewal amount -> ACV"
- "Missing commercial approval overview / list of opportunities"

### The 9 named Sales Directors (MD-1)

| #   | Name              | Territory                             | User ID              | Role                   | Unique Role? |
| --- | ----------------- | ------------------------------------- | -------------------- | ---------------------- | ------------ |
| 1   | Megan Miceli      | Canada (all segments)                 | `005Tb00000MlZXCIA3` | SC NA Sales            | shared       |
| 2   | Patrick Gaughan   | NA remainder (AM + Bank + WM + Other) | `005Tb00000XYMJIIA5` | SC NA Sales            | shared       |
| 3   | Jesper Tyrer      | APAC                                  | `005Tb00000PY6SpIAL` | SC Asia Sales Director | unique       |
| 4   | Sarah Pittroff    | Central Europe                        | `005Tb00000WVuoKIAT` | SC EMEA Sales          | shared       |
| 5   | Francois Thaury   | Southern Europe                       | `005D000000272NoIAI` | SC EMEA Sales          | shared       |
| 6   | Dan Peppett       | UK & Ireland                          | `00557000006VpU9AAK` | SC UK & ME Head of CX  | unique       |
| 7   | Christian Ebbesen | NL & Nordics                          | `0052o00000BeANWAA3` | SC NE Head of CX       | unique       |
| 8   | Mourad Essofi     | Middle East & Africa                  | `005QA000003DawpYAC` | SC EMEA Sales          | shared       |
| 9   | Adam Steinhaus    | NA Pension & Insurance                | `005QA000006WqODYA0` | SC NA Sales            | shared       |

Role-hierarchy scoping alone does **not** cleanly slice the 9 Directors — 5 of 9 share roles with peers. Per-Director slicing now comes from dashboard filters on `Sales_Region__c` + `ADDRESS1_COUNTRY_CODE` (`Legal Country`) + `INDUSTRY` + `Opportunity.Account_Unit_Group__c`.

### Track 2 — Sales Ops Quarterly (Report 2)

Audience: Sales Operations. Cadence: quarterly. Format: CRMA-style dashboard as system of record; quarterly PowerPoint readout derived from it.

**22 widgets across 4 sections:**

1. CRM Data Quality (5 widgets)
2. Process Compliance (5 widgets)
3. Forecast Accuracy (4 widgets, all Pipeline Inspection native)
4. Pipeline Hygiene (8 widgets)

## 2. Target dashboards

- **Dashboard 1:** `01ZTb00000FSP7hMAH` — "Sales Directors Monthly Pipeline and Insights"
- **Dashboard 2:** `01ZTb00000FSP9JMAX` — "Sales Ops Quarterly KPI Dashboard"

Both share folder `005QA000003DUwWYAW` (Andre's personal folder). Target org `apro@simcorp.com`, instance `simcorp.my.salesforce.com`, API v66.0.

## 3. Phase journey

| Phase                        | Date           | What it did                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phase 1                      | 2026-04-06     | Initial audit of D1 against 16-widget spec. 12 BLOCKING / 10 WRONG-DATA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Phase 1.5                    | 2026-04-07     | D1 hotfix: 3 AMOUNT→ACV aggregation swaps + 6 in-flight component fixes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Phase 2                      | 2026-04-07     | D2 audit + Report 2 source contract authored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Phase 2.5 B-core             | 2026-04-08     | D2 ARR + fiscal fixes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| Phase 2.6                    | 2026-04-08     | D2 process compliance: 5 new SF reports via POST, 5 new components, aging_pipeline ARR fix.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Phase 2.7                    | 2026-04-08     | D1 missing widgets: 8 new SF reports, 3 added as components. Initial approach hardcoded regions — later rebuilt.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Phase 2.8                    | 2026-04-08     | Recontextualization: "one filterable dashboard, not 9 clones". Deleted 5 hardcoded regional reports + 5 ORPHAN cruft. Amended contracts with 9 Director preset filter combos. D2 fiscal sweep (10 reports PATCHed). Matcher improved. Deep audit + 10 inline defect fixes. Playwright attempt at filter creation: failed on save. Visual polish via API: 5 fixes.                                                                                                                                                                             |
| **Pristine pass**            | **2026-04-08** | **Second-pass improvement. Verified 9/9 prior inline fixes intact. Applied 8 new API-executable improvements: 5 ACV/ARR `.CONVERT` upgrades on D1, 1 FlexTable `s!AMOUNT→ARR.CONVERT` swap on D2 (required FlexTable dual-storage fix), 1 Forecast_ARR `.CONVERT` upgrade on D2, 1 cleanup pass retiring 7 stale aggregates. Empirically verified CALENDAR_QUARTER grouping is API-unfixable. Ported audit logic into reusable `scripts/dashboard_state_dump.py`. Produced pristine design doc + manual UI runbook + pristine pass handoff.** |
| **Research addendum**        | **2026-04-09** | **3 research subagents (SF dashboard best practices, executive sales patterns, data engineering) landed with 7 load-bearing findings: executive widget count ceiling 6-9, LoggedInUser-can't-be-scheduled, slip schema recommendation, commercial approval 2x2 pattern, metric registry architecture, data quality theater avoidance. Documented concrete 8-widget D1 blueprint.**                                                                                                                                                            |
| **Phase 5 POC + extensions** | **2026-04-09** | **Widget-count-over-exec-ceiling warning added to audit script (D1 now emits `💡 widget-count-over-max:15/12`). 2 new SF reports built via POST as infrastructure for the 8-widget blueprint: `Top 10 Deals by ARR This Qtr` and `Commercial Approval 2x2 Matrix`. Phase 5 metric registry proof-of-concept: 4 metric YAMLs + 3 source contract YAMLs at `reporting-layer/`. Locked in 3 new POST-time constraints.**                                                                                                                         |
| **Live continuation**        | **2026-04-09** | **D1 Lightning/UI + API follow-through. Created 4 dashboard-owned filters (`Industry`, `Legal Country`, `Sales Region`, `Account Unit Group`), upgraded the weakest D1 reports/widgets into actionable worklists, and consolidated D1 from 15 widgets to 8. Final state was verified via direct Analytics API GETs and `/tmp` backups because `dashboard_state_dump.py` hung on the closing pass.**                |

## 4. Where we are now — live state

### Dashboard 1 — Sales Directors Monthly

- **Components:** 8 / 20 hard cap (after the 2026-04-09 executive consolidation)
- **Filters:** 4 (`Industry`, `Legal Country`, `Sales Region`, `Account Unit Group`)
- **Running user mode:** SpecifiedUser (Andre Profitt), `canChangeRunningUser = false`
- **Lightning editor save path:** confirmed live on 2026-04-09 for filter + layout changes; REST still cannot CREATE dashboard filters

**Verification state:**

```
Last successful full audit before the final D1 continuation:
✓ Pristine state: 0 active flags, 4 deferred flags, 1 design warning(s).
```

- **Direct GET verification after the final continuation** confirmed `components.length = 8` and `filters.length = 4` on `01ZTb00000FSP7hMAH`
- **Closing-pass caveat:** `dashboard_state_dump.py` hung after the final D1 continuation, so re-run it at the start of the next session before relying on the older deferred / warning counts

**Live composition (8 widgets):**

| #   | Widget                                  | Report ID                   | Format    | Date         | Widget Agg                        | Stakeholder Ask                            |
| --- | --------------------------------------- | --------------------------- | --------- | ------------ | --------------------------------- | ------------------------------------------ |
| 1   | Pipeline Overview by Stage              | `00OTb000008fBfdMAE` (P2.7) | SUMMARY   | THIS_QUARTER | `APTS_Opportunity_ARR__c.CONVERT` | Pipeline overview                          |
| 2   | Pipeline Coverage by Stage              | `00OTb000008TZc5MAG`        | SUMMARY   | THIS_QUARTER | `APTS_Forecast_ARR__c.CONVERT`    | Pipeline overview (supplementary)          |
| 3   | Commercial Approval Current State       | `00OTb000008fBEDMA2` (P2.7) | SUMMARY   | CUSTOM       | RowCount                          | Commercial Approval (global)               |
| 4   | Commercial Approval Candidates by Stage | `00OTb000008ekp7MAA`        | FlexTable | CUSTOM       | `APTS_Opportunity_ARR__c.CONVERT` | Commercial Approval (candidates)           |
| 5   | Renewal ACV by Quarter                  | `00OTb000008ekxBMAQ`        | SUMMARY   | THIS_YEAR    | `APTS_Renewal_ACV__c.CONVERT`     | Renewals tracking                          |
| 6   | Renewal Pipeline This Quarter           | `00OTb000008ektxMAA`        | FlexTable | THIS_QUARTER | `APTS_Renewal_ACV__c.CONVERT`     | Renewals tracking                          |
| 7   | Business At Risk                        | `00OTb000008Ta9xMAC`        | FlexTable | CUSTOM       | `Opportunity_Average_ACV__c`      | Churn Risk (CRM-side proxy)                |
| 8   | Close Date Slipped by Stage             | `00OTb000008eknVMAQ`        | FlexTable | LAST_QUARTER | `APTS_Opportunity_ARR__c.CONVERT` | Slipped deals                              |

### Dashboard 2 — Sales Ops Quarterly KPI

- **Components:** 18 / 20 hard cap
- **Filters:** 0
- **Running user mode:** SpecifiedUser (Andre Profitt)

**Audit state:**

```
0 active flags, 1 deferred (fiscal-grouping on widget 15 — Overdue Opportunities)
```

### New reports built 2026-04-09 (not yet on any dashboard)

These are **infrastructure for the 8-widget pristine blueprint**. `Top 10 Deals by ARR This Qtr` is already pinned in the Phase 5 metric registry; `Commercial Approval 2x2 Matrix` is live in Salesforce but still needs its `reporting-layer/contracts/` file. Neither is wired into a dashboard component yet (that step awaits stakeholder consolidation sign-off).

| Report ID            | Name                           | Format  | Purpose                                        |
| -------------------- | ------------------------------ | ------- | ---------------------------------------------- |
| `00OTb000008fQ3ZMAU` | Top 10 Deals by ARR This Qtr   | SUMMARY | "Top 10 Deals" widget in 8-widget D1 blueprint |
| `00OTb000008fQ6nMAE` | Commercial Approval 2x2 Matrix | MATRIX  | Consolidates 4 current approval widgets into 1 |

## 5. Source-of-truth hierarchy

Three layers, in priority order:

1. **`reporting-layer/` (Phase 5 metric registry, new 2026-04-09)** — forward-facing YAML-based target. 4 metrics + 3 source contracts so far. Pattern: `metrics/<name>.yml` + `contracts/report_<id>.yml`. Mirrors dbt MetricFlow / LookML / Cube.dev. README at `reporting-layer/README.md`.
2. **`docs/specs/report-{1,2}-source-contract.md`** — existing markdown source contracts with Phase 2.7/2.8 amendments. Still authoritative for every widget in both dashboards. Will be migrated into the metric registry as Phase 5 progresses.
3. **`scripts/dashboard_state_dump.py`** — the live-truth audit tool. Pulls both dashboards + every source report via sf CLI + curl, emits structured flags. Ground truth when contracts and live state disagree. See §Audit + tooling cheat sheet.

Diffing contracts against live state is the core of the "new way of working" — see §6.

## 6. The new way of working with Salesforce

This section is the **load-bearing handoff** — every pattern discovered/confirmed during the pristine pass and its continuation. These are the patterns a fresh session should follow when touching the dashboards or reports.

### 6.1 Authentication + request pattern

All work goes through `sf` CLI + `curl` (no MCP, no Python builders). The pattern:

```bash
# Fresh token (expires; refresh when you get INVALID_SESSION_ID)
sf org display --target-org apro@simcorp.com --json > /tmp/frontier-sf-org-display.json

# Or inline into a script:
sf org display --target-org apro@simcorp.com --json | jq -r '.result.accessToken'
sf org display --target-org apro@simcorp.com --json | jq -r '.result.instanceUrl'
```

All API calls use `https://simcorp.my.salesforce.com/services/data/v66.0/...` with `Authorization: Bearer <token>` + `Content-Type: application/json`. `scripts/dashboard_state_dump.py` defaults to `--target-org apro@simcorp.com` so it cannot silently follow the wrong default org.

### 6.2 Dashboard PATCH body shape

**The PATCH body is the `dashboardMetadata` dict directly**, NOT wrapped in another `dashboardMetadata` key. This is a load-bearing gotcha — getting it wrong returns `JSON_PARSER_ERROR`.

```javascript
// CORRECT
PATCH /services/data/v66.0/analytics/dashboards/{id}
Body: {
  developerName, name, dashboardType, components: [...],
  filters: [...], runningUser: {...}, ...
}

// WRONG — returns 400 JSON_PARSER_ERROR
PATCH /services/data/v66.0/analytics/dashboards/{id}
Body: { dashboardMetadata: { ... } }
```

**Dashboard read-only fields to strip before PATCH:**

```javascript
const DASH_READONLY = [
  "id",
  "createdDate",
  "lastModifiedDate",
  "lastAccessedDate",
  "url",
  "owner",
  "runningUser",
  "folderName",
];
```

**Component read-only fields to strip before PATCH:**

```javascript
const COMPONENT_READONLY = ["lastModifiedDate"];
```

### 6.3 Report PATCH body shape

**Wrapped** in `reportMetadata`:

```javascript
PATCH /services/data/v66.0/analytics/reports/{id}
Body: { reportMetadata: { name, developerName, aggregates: [...], ... } }
```

**Report read-only fields to strip:**

```javascript
const REPORT_READONLY = [
  "id",
  "createdDate",
  "lastModifiedDate",
  "lastRunDate",
  "lastModifiedById",
  "createdById",
  "currency",
];
```

### 6.4 Report POST body shape (NEW FROM 2026-04-09)

**3 load-bearing POST constraints discovered building `Top 10 Deals` + `Commercial Approval 2x2`:**

1. **`sort_aggregate` on a grouping cannot be set at POST time** — triggers HTTP 500 "invalid parameter value". Leave it `null` on POST, apply via subsequent PATCH if the report needs sort-by-aggregate.

2. **`description` on POST triggers HTTP 500** "invalid parameter value". Apply as a subsequent PATCH after creation. (The template's inherited description does NOT survive — explicit `description` is what breaks.)

3. **`folderId` on POST triggers HTTP 500** "invalid parameter value". Either inherit the template's folder (if it's the right one) or apply via subsequent PATCH.

**Best practice for creating new reports:** clone from a working template via `GET /analytics/reports/{template_id}/describe`, strip read-only fields + `chart`/`crossFilters`/`historicalSnapshotDates`/`reportBooleanFilter`, override only `name` + `developerName` + `reportFormat` + `groupingsDown` + `groupingsAcross` + `detailColumns` + `aggregates` + `reportFilters` + `standardDateFilter`, POST, then PATCH description/folderId if needed.

See `/tmp/build-new-reports.mjs` from the 2026-04-09 session for a working reference implementation.

### 6.5 `.CONVERT` aggregate requirement (confirmed)

For any revenue field (`APTS_Opportunity_ARR__c`, `APTS_Renewal_ACV__c`, `APTS_Forecast_ARR__c`) on a multi-currency org, you MUST use the `.CONVERT` suffix on the aggregate expression:

```
s!Opportunity.APTS_Opportunity_ARR__c.CONVERT
```

**Without `.CONVERT`**, aggregates sum raw currency values across records in different currencies (EUR + USD + JPY as plain numbers) = nonsense totals.

**The `.CONVERT` form must ALSO appear in `detailColumns`** — Salesforce rejects the aggregate with `specificErrorCode: 113` ("is not a valid aggregate because it was not selected in the detail columns") if the corresponding column is missing. Add `Opportunity.APTS_Opportunity_ARR__c.CONVERT` to `detailColumns` alongside `Opportunity.APTS_Opportunity_ARR__c`.

### 6.6 FlexTable widget dual-storage quirk (LOAD-BEARING)

**The most important finding from the pristine pass.** FlexTable widgets store the aggregate in **TWO places** and Salesforce silently reverts the PATCH if you only update one:

```javascript
component.properties.aggregates = [{ name: "s!AMOUNT" }];  // OLD location
component.properties.visualizationProperties.tableColumns = [
  { column: "FULL_NAME", type: "grouping", ... },
  { column: "s!AMOUNT", type: "aggregate", ... }  // NEW location — ALSO here
];
```

**Both must be updated in the same PATCH** or Salesforce returns 200 OK and then silently reverts `properties.aggregates` back to match `visualizationProperties.tableColumns` (which is apparently the source of truth for FlexTable render).

Cost the pristine pass ~20 minutes of debugging. Locked into project memory; any future FlexTable aggregate PATCH must touch both paths together.

### 6.7 `CALENDAR_QUARTER` grouping is not API-settable

Empirically verified 2026-04-08: the Reports API rejects both `CALENDAR_QUARTER` and `CLOSE_DATE_CALENDAR_QUARTER` as grouping column names:

```
"The column CALENDAR_QUARTER is not a valid column for groupings."
```

The only path to calendar-quarter grouping is a **bucket field** (per-report) or a **custom formula field on Opportunity** (reusable). Recommended formula:

```
Calendar_Quarter__c =
  TEXT(YEAR(CloseDate)) & "-Q" & TEXT(CEILING(MONTH(CloseDate) / 3))
```

This is a metadata deploy, not a REST PATCH. Once the field exists, the 4 deferred widgets (D1 Renewal ACV by Quarter, D1 Forecast Accuracy, D1 Renewals by Quarter, D2 Overdue Opportunities) can switch their grouping column in a trivial PATCH.

### 6.8 Dashboard filter CREATE is Lightning-UI only

The Analytics REST API **cannot create new dashboard filters** — only update existing ones. Any new filter definition must come from the Lightning UI. Known attempt results:

- `PATCH dashboardMetadata.filters[]` with new entries → HTTP 400 `"filter field is no longer available"` (because the server-assigned filter id like `0IB...` is created by the UI only)
- `PATCH canChangeRunningUser` → HTTP 200 OK but silently ignored (also UI-only)

Once the Lightning UI has created the dashboard-owned filter ids, REST can then mutate the filter labels, option lists, and widget bindings. That is how the live D1 filter set was normalized on 2026-04-09.

Document this in the manual UI runbook (`docs/2026-04-08-manual-ui-runbook.md` §1).

### 6.9 Inline verification pattern

Every PATCH is followed by a GET to verify the change landed. The `dashboard_state_dump.py` script formalizes this for dashboards + reports; for one-off fixes, the inline pattern is:

```javascript
// PATCH
const patchRes = await api("PATCH", `/analytics/reports/${id}`, body);

// Verify
const verify = await api("GET", `/analytics/reports/${id}/describe`);
assert(verify.reportMetadata.aggregates.includes(newAgg));
```

**Never trust a PATCH that returns 200 without verifying.** The FlexTable quirk (§6.6) is exactly the reason.

### 6.10 Drift detection via the audit script

`scripts/dashboard_state_dump.py` is the canonical drift-detection tool. It pulls both dashboards + every source report, cross-references widget bindings with report aggregates, and emits:

- **Active flags** (⚠️) — data defects that must be fixed (e.g., `no-convert:APTS_Opportunity_ARR__c`, `amount-not-arr-on-widget`, `fiscal-date-filter:THIS_FISCAL_YEAR`)
- **Deferred flags** (🔶) — known defects awaiting schema change (allow-listed at the top of the script: `DEFERRED_FISCAL_GROUPING_REPORTS`)
- **Design warnings** (💡) — design choices that cross best-practice thresholds (e.g., `widget-count-over-max:N/12` on executive dashboards)

See §Audit + tooling cheat sheet for usage.

## 7. Commit map

| Commit        | Date           | Scope                                                                                   |
| ------------- | -------------- | --------------------------------------------------------------------------------------- |
| `36103a8`     | 2026-04-08     | Phase 2.7 scope doc                                                                     |
| `489871a`     | 2026-04-08     | Phase 2.7 ARR form correction                                                           |
| `c9ddb70`     | 2026-04-08     | Phase 2.7 audit + contract amendment                                                    |
| `d09a870`     | 2026-04-08     | Phase 2.8: 9 Director preset filter combos                                              |
| `6434f66`     | 2026-04-08     | Phase 2.8: D1 ORPHAN cleanup (5 cruft components removed)                               |
| `62c4862`     | 2026-04-08     | Phase 2.8: D2 fiscal sweep (10 reports PATCHed)                                         |
| `c913e00`     | 2026-04-08     | Phase 2.8: em-dash fix at source                                                        |
| `0c78b5b`     | 2026-04-08     | Phase 2.8: Report 2 contract amendment                                                  |
| `2e0bf81`     | 2026-04-08     | Phase 2.8: Report 2 verification status sync                                            |
| `0eee5b7`     | 2026-04-08     | Phase 2.8: D2 audit with source contract pinning                                        |
| `1aa7c42`     | 2026-04-08     | Deep audit + 10 inline defect fixes                                                     |
| `f342187`     | 2026-04-08     | D1 visual polish                                                                        |
| `2550fa7`     | 2026-04-08     | Session handoff doc (Phases 1-2.8)                                                      |
| **`5f652c8`** | **2026-04-08** | **Pristine pass: 8 improvements + audit tool + 3 docs (976 insertions)**                |
| **`a1d4466`** | **2026-04-09** | **Research addendum: 7 load-bearing findings (328 insertions)**                         |
| **`9e4ce63`** | **2026-04-09** | **Phase 5 metric registry POC + exec-ceiling warning + 2 new reports (626 insertions)** |
| **`d995851`** | **2026-04-09** | **Fresh-session handoff integrating the new Salesforce way of working**                  |

**Out-of-band live Salesforce changes** (no git tracking; recorded in the pristine pass handoff):

- 8 API-executable improvements applied via PATCH (2026-04-08 pristine pass — see §6 for the pattern for each type)
- 7 stale non-`.CONVERT` aggregates retired from source reports
- 2 new reports created via POST (`00OTb000008fQ3ZMAU`, `00OTb000008fQ6nMAE`)
- 2026-04-09 live continuation: 4 native D1 filters created, 8 D1 report/widget surfaces enriched, and D1 consolidated from 15 widgets to 8

## 8. Stakeholder coverage scorecard (updated 2026-04-09)

### Track 1 — Sales Directors Monthly

| Goal                                                                  | Coverage                | Notes                                                                                                                                                                                                                                                                                           |
| --------------------------------------------------------------------- | ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pipeline overview with quarterly focus (one slide per region)         | OK via live filters     | `Pipeline Overview by Stage` + `Pipeline Coverage by Stage` remain on D1. Per-Director regional slices now use the 4 live dashboard filters documented in the contract.                                                                                                                        |
| Commercial Approval overview (global + Land stage 3 missing approval) | OK                      | 2 retained widgets cover the concept end-to-end on the live 8-widget D1. **Future: 1 consolidated 2x2 widget via the new `Commercial Approval 2x2 Matrix` report — awaits stakeholder sign-off.**                                                                                            |
| Renewals tracking (this quarter, value, likelihood)                   | PARTIAL                 | The live 8-widget D1 keeps `Renewal ACV by Quarter` and `Renewal Pipeline This Quarter`, both enriched with worklist-level detail. The explicit `Renewal Likelihood by Probability` view was removed in the consolidation and can be restored if stakeholders miss it.                     |
| Churn Risk and trends                                                 | PROXY only              | `Business At Risk` widget is the CRM-side fallback. Finance feed pending Alex P — not reached in any session.                                                                                                                                                                                 |
| Slipped deals analysis                                                | PARTIAL                 | `Close Date Slipped by Stage` is now an actionable worklist on D1, not a thin chart. Root cause commentary still requires schema: `Slip_Reason__c` / `Slip_Count__c` / Flow per research (see §Remaining work). PI native source deferred to PI Lightning UI phase.                     |
| 9 MD-1 Sales Directors per-person views                               | LIVE via 4-filter model | D1 now has native `Industry`, `Legal Country`, `Sales Region`, and `Account Unit Group` filters. Remaining open question is distribution mode: keep `SpecifiedUser`, flip to `LoggedInUser`, or clone for scheduled mailouts.                                                             |

### Track 2 — Sales Ops Quarterly

| Section                        | Coverage       | Notes                                                                                                                                                                                      |
| ------------------------------ | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| CRM Data Quality (5 widgets)   | 4/5 OK         | `dq_missing_quote_type` blocked on product decision. Won/Loss Info CFQ filter cleaned per stakeholder bullet.                                                                              |
| Process Compliance (5 widgets) | 5/5 OK         | All built in Phase 2.6 with documented simplifications.                                                                                                                                    |
| Forecast Accuracy (4 widgets)  | 0/4 BLOCKED    | All 4 require Pipeline Inspection Lightning UI list view configuration.                                                                                                                    |
| Pipeline Hygiene (8 widgets)   | 7/8 OK + 1 WIP | `ph_probability_mismatch_by_stage` Under Construction pending Sales Ops threshold decision. **`No Activity 30+` s!AMOUNT defect fixed in pristine pass via FlexTable dual-storage PATCH.** |

## 9. Remaining work (priority order)

### P0 — Running-user / distribution decision

D1 now already has the 4 live filters the Directors need. The remaining decision is how the dashboard should be consumed:

- **Keep `SpecifiedUser` + shared filter presets** (lowest operational risk, current live state)
- **Flip to `LoggedInUser`** if no scheduled email is needed and per-viewer scoping is worth the trade-off
- **Create director-specific clones** only if recurring scheduled delivery is mandatory

Decision tree remains in `docs/2026-04-09-pristine-design-research-addendum.md` §Finding 2.

### P1 — `Calendar_Quarter__c` schema deploy

Add custom formula field on Opportunity:

```
TEXT(YEAR(CloseDate)) & "-Q" & TEXT(CEILING(MONTH(CloseDate) / 3))
```

One-time metadata deploy. Unblocks 4 deferred fiscal-grouping widgets (D1 × 3, D2 × 1). Runbook §4.

### P2 — Saved filter states / Director enablement

The 4-filter model is live, but the 9 Directors still need an operating pattern around it: saved browser bookmarks, one-page instructions, or user-by-user enablement. The live filter values are now documented in `docs/specs/report-1-source-contract.md` and `docs/2026-04-08-manual-ui-runbook.md`.

### P3 — Pipeline Inspection list views

2 PI list views manually created in SF Lightning UI — required for 4 forecast accuracy widgets on D2 + slipped deals widgets on D1. Runbook §2.

### P4 — D1 second-pass refinement on top of the live 8-widget layout

The 15 → 8 consolidation is already live. The remaining optional refinement is whether to pull the Phase 5 blueprint the rest of the way through by introducing the new `Top 10 Deals by ARR This Qtr` and `Commercial Approval 2x2 Matrix` reports into D1.

Infrastructure ready: both reports are already built (`00OTb000008fQ3ZMAU` Top 10 Deals, `00OTb000008fQ6nMAE` Commercial Approval 2x2). `Top 10 Deals` is already pinned in `reporting-layer/contracts/`; `Commercial Approval 2x2` still needs its contract file. Binding either into dashboard components is a simple PATCH once stakeholders want the next iteration.

### P5 — Stakeholder decisions (3 items)

- `dq_missing_quote_type` — retire vs repurpose to `Type` field (`APTS_Primary_Quote_Type__c` is migrated-empty)
- `ph_probability_mismatch_by_stage` — per-stage threshold definition from Sales Ops
- `churn_risk_placeholder` — Finance feed handshake with Alex P

### P6 — Slipped deals schema (`Slip_Reason__c` / `Slip_Count__c`)

From the research addendum: add picklist + long-text + rollup fields on Opportunity, build a Flow triggered on CloseDate forward-move >14 days, backfill `Slip_Count__c` from `OpportunityFieldHistory`. Unlocks the **"Chronic Slippage" widget** (`Slip_Count__c >= 2`) which the research identifies as **"the highest-signal widget in the entire monthly review."** ~1 week of metadata + Flow work.

### P7 — Metric registry migration

Port source contracts from `docs/specs/report-{1,2}-source-contract.md` to `reporting-layer/metrics/*.yml` + `reporting-layer/contracts/*.yml`. Phase 5 POC already has 4 metrics + 3 contracts. Add 1 metric per touched widget until the markdown contracts can be retired. Also build `scripts/normalize_and_hash.py` that populates the `hash` field on each contract and a CI job that diffs against live state.

### P8 — Phase 3 or Phase 4 deck rebuild

Source contracts are now clean. Phase 3 is a safer validation pass that diffs canonical SF report values against what the deck shows. Phase 4 is the actual deck builder. The `output/sales_director_monthly_deck_2026-03-31/` infrastructure already exists and may just need to point at the new pinned sources.

### P9 — CI gating

Wire `python3 scripts/dashboard_state_dump.py --fail-if-drifted` into a pre-commit hook or GitHub Action. Future drift gets caught automatically instead of accumulating until the next audit session.

## 10. Known constraints (full list, updated 2026-04-09)

### Salesforce platform constraints

1. **Lightning dashboard filter creation/save is still UI-only and historically flaky under automation.** Earlier Phase 2.8 browser attempts failed, but the 2026-04-09 continuation did persist D1 filter + layout changes once the dashboard was already in the Lightning editor. Manual UI remains the safest fallback if automation stalls.
2. **Analytics REST API cannot CREATE dashboard filters** — only UPDATE existing. New filter definitions come from the Lightning UI only (server-assigned filter id `0IB...` prefix is created by the UI).
3. **`canChangeRunningUser` PATCH silently ignored** — returns HTTP 200 but doesn't flip the flag. Lightning UI only.
4. **SF Reports API rejects `groupingsDown` names that also appear in `detailColumns`** (specificErrorCode 113). Strip grouping names from the detail columns list before POST.
5. **SUMMARY reports without `groupingsDown` cannot source dashboard components.** Always include at least one grouping.
6. **TABULAR reports cannot source dashboard components** without `dashboardSetting` + `rowLimit` config the API doesn't expose cleanly. Convert TABULAR→SUMMARY with a grouping.
7. **20-widget hard cap** per dashboard (25 total including text/images). No workaround — split dashboards or move to CRMA.
8. **Auto-suffix on developerName collision** even after colliding report is DELETEd (sticks for some caching window). Produces `_1`, `_2`, `_3` suffixes.
9. **Long-lived tabs can trigger session cookie expiry on the API path** (INVALID_SESSION_ID). Refresh via `sf org display --target-org apro@simcorp.com --json` and re-run.

### POST-time constraints discovered 2026-04-09

10. **`sort_aggregate` on a grouping is PATCH-only, not POST-settable** — triggers HTTP 500 "invalid parameter value" on POST.
11. **`description` on report POST triggers HTTP 500** "invalid parameter value" — apply via subsequent PATCH.
12. **`folderId` on report POST triggers HTTP 500** "invalid parameter value" — inherit from template or PATCH after creation.

### Multi-currency constraints

13. **`.CONVERT` suffix required on all revenue aggregates** (`APTS_Opportunity_ARR__c.CONVERT`, `APTS_Forecast_ARR__c.CONVERT`, `APTS_Renewal_ACV__c.CONVERT`). Without it, aggregates sum raw values across currencies = garbage totals.
14. **`.CONVERT` aggregate requires the `.CONVERT` column in `detailColumns`** — Salesforce rejects the aggregate with specificErrorCode 113 otherwise.

### Widget-type constraints

15. **FlexTable widgets store aggregates in TWO places** (`properties.aggregates[]` AND `properties.visualizationProperties.tableColumns[]`). Both must be updated in the same PATCH or Salesforce silently reverts the change without failing. See §6.6.

### Calendar vs fiscal constraints

16. **`CALENDAR_QUARTER` grouping is not API-settable** — Reports API rejects "is not a valid column for groupings". Requires bucket field or custom formula field on Opportunity. 4 widgets currently deferred on this.
17. **`THIS_CALENDAR_QUARTER` date literal is rejected** by this org — use bare `THIS_QUARTER` / `THIS_YEAR` for calendar framing (not `THIS_FISCAL_QUARTER` / `THIS_FISCAL_YEAR`).

### Data convention constraints

18. **Folder ID for both dashboards' reports:** `005QA000003DUwWYAW` (Andre's personal folder).
19. **Standard POST body shape:** `{"reportMetadata": {...}}` with read-only fields stripped.
20. **Dashboard PATCH body shape:** `dashboardMetadata` dict directly (NOT wrapped in another key).

## 11. Next session recommendations

**Order of operations** assuming zero external blockers land:

1. **Start with `scripts/dashboard_state_dump.py`** — run it immediately to verify the current state matches this handoff:

   ```bash
   cd /Users/test/crm-analytics
   python3 scripts/dashboard_state_dump.py --summary-only
   ```

   Goal: no active flags. If the script hangs again, fall back to direct GETs on `/analytics/dashboards/01ZTb00000FSP7hMAH` and `/analytics/dashboards/01ZTb00000FSP9JMAX` and confirm D1 still shows 8 components / 4 filters before making more changes.

2. **Answer the running-user question** — does the dashboard need to be emailed to the 9 Directors on a schedule? (Yes → keep `SpecifiedUser` or clone; No → `LoggedInUser` remains viable.) This is now a distribution decision, not a filter-creation blocker.

3. **Lock the 9 Director operating model** (§9 P2) — decide whether the team wants saved bookmarks, enablement screenshots, or a lightweight SOP for the 4 live filters.

4. **Metadata deploys** (§9 P1 + P6) — `Calendar_Quarter__c` formula field (trivial) + `Slip_Reason__c` / `Slip_Count__c` schema (medium effort). These are the main remaining schema blockers.

5. **Pipeline Inspection list views** (§9 P3) — still required for D2 Forecast Accuracy and the future PI-native slipped-deals view.

6. **D1 second-pass refinement** (§9 P4) — if stakeholders want the next iteration, wire the 2 new reports into D1 via PATCH (`00OTb000008fQ3ZMAU`, `00OTb000008fQ6nMAE`).

7. **Phase 5 metric registry migration** (§9 P7) — incremental, per widget touched. Start with the missing contract for `00OTb000008fQ6nMAE`.

8. **Phase 3 cross-check or Phase 4 deck rebuild** (§9 P8) — once the dashboards are stable, execute the deck infrastructure.

9. **CI gating** (§9 P9) — wire `--fail-if-drifted` into a pre-commit or GitHub Action.

## 12. Audit + tooling cheat sheet

### `scripts/dashboard_state_dump.py`

The pristine-state audit tool. Run it first when starting any dashboard work.

```bash
# Fast startup check
python3 scripts/dashboard_state_dump.py --summary-only

# Full markdown report to stdout
python3 scripts/dashboard_state_dump.py

# Markdown only to file
python3 scripts/dashboard_state_dump.py --format markdown --out-md /tmp/state.md

# JSON only to stdout
python3 scripts/dashboard_state_dump.py --format json

# Write JSON to file
python3 scripts/dashboard_state_dump.py --format json --out-json /tmp/state.json

# Single dashboard
python3 scripts/dashboard_state_dump.py --dashboard 01ZTb00000FSP7hMAH

# CI gate — exit non-zero if any active flag is present
python3 scripts/dashboard_state_dump.py --fail-if-drifted
```

Defaults to `--target-org apro@simcorp.com`. Override `--target-org` only if you intentionally want a different org.

Known caveat from the 2026-04-09 live continuation: the script hung on the closing pass after the D1 filter/layout consolidation. If that recurs, use direct Analytics API GETs to confirm D1 still has 8 components and 4 filters, then investigate the script separately.

**Flag legend:**

| Marker                            | Meaning                                | Action                                                                   |
| --------------------------------- | -------------------------------------- | ------------------------------------------------------------------------ |
| ⚠️ `no-convert:<field>`           | Revenue aggregate missing `.CONVERT`   | PATCH report aggregates + detailColumns                                  |
| ⚠️ `amount-not-arr-on-widget`     | Widget binding uses `s!AMOUNT`         | Swap to ARR (remember FlexTable dual-storage)                            |
| ⚠️ `fiscal-date-filter:<form>`    | Fiscal framing on standardDateFilter   | Switch to calendar via `THIS_QUARTER` / `THIS_YEAR` / `CUSTOM` unbounded |
| 🔶 `fiscal-grouping:<col>`        | Fiscal framing on grouping             | Deferred — schema change only. Allow-listed at top of script.            |
| 💡 `widget-count-over-target:N/8` | Executive dashboard exceeds 8 widgets  | Design conversation — stakeholder sign-off                               |
| 💡 `widget-count-over-max:N/12`   | Executive dashboard exceeds 12 widgets | Design conversation — cognitive overload                                 |

### `reporting-layer/` metric registry

Phase 5 proof-of-concept. 4 metrics + 3 source contracts as of `9e4ce63`. Pattern:

```
reporting-layer/
├── README.md
├── metrics/
│   ├── new_business_arr.yml         (1.0.0 — production)
│   ├── renewal_acv.yml              (1.0.0 — production)
│   ├── top_10_deals_arr.yml         (1.0.0 — infrastructure, not yet bound)
│   └── pipeline_coverage.yml        (0.1.0 — blocked on quota source)
└── contracts/
    ├── report_00OTb000008fBfdMAE.yml  (P2.7 Pipeline Global)
    ├── report_00OTb000008fBULMA2.yml  (P2.7 Renewal Likelihood)
    └── report_00OTb000008fQ3ZMAU.yml  (Top 10 Deals, new 2026-04-09)
```

Each metric YAML binds one or more dashboard widgets to a canonical measure + source report. Each source contract YAML declares the expected shape of a Salesforce Report — the drift-detection target.

### Canonical auth pattern (inline for scripting)

```bash
# Refresh + cache to /tmp/frontier-sf-auth.json
sf org display --target-org apro@simcorp.com --json 2>/dev/null | \
  node -e '
    let s=""; process.stdin.on("data", d => s += d);
    process.stdin.on("end", () => {
      const r = JSON.parse(s);
      require("fs").writeFileSync("/tmp/frontier-sf-auth.json",
        JSON.stringify({token: r.result.accessToken, url: r.result.instanceUrl}));
    });'

# Use in a script
AUTH=$(cat /tmp/frontier-sf-auth.json)
TOKEN=$(echo "$AUTH" | node -e 'let s=""; process.stdin.on("data",d=>s+=d); process.stdin.on("end",()=>console.log(JSON.parse(s).token));')
INST=$(echo "$AUTH" | node -e 'let s=""; process.stdin.on("data",d=>s+=d); process.stdin.on("end",()=>console.log(JSON.parse(s).url));')

curl -sS -H "Authorization: Bearer $TOKEN" "$INST/services/data/v66.0/analytics/dashboards/01ZTb00000FSP7hMAH"
```

### Canonical PATCH pattern (Node, matches the scripts used in the pristine pass)

```javascript
import { readFileSync } from "node:fs";
const { token, url } = JSON.parse(
  readFileSync("/tmp/frontier-sf-auth.json", "utf8"),
);

const DASH_READONLY = [
  "id",
  "createdDate",
  "lastModifiedDate",
  "lastAccessedDate",
  "url",
  "owner",
  "runningUser",
  "folderName",
];
const COMPONENT_READONLY = ["lastModifiedDate"];

function strip(obj, keys) {
  const out = { ...obj };
  for (const k of keys) delete out[k];
  return out;
}

async function api(method, path, body) {
  const opts = { method, headers: { Authorization: `Bearer ${token}` } };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const r = await fetch(`${url}/services/data/v66.0${path}`, opts);
  if (!r.ok)
    throw new Error(`${method} ${path}: ${r.status} ${await r.text()}`);
  return r.json();
}

// Read + modify + PATCH a dashboard component
const d = await api("GET", "/analytics/dashboards/01ZTb00000FSP7hMAH");
const m = d.dashboardMetadata;
const newComponents = m.components.map((c) => {
  const cleaned = strip(c, COMPONENT_READONLY);
  // ... modify cleaned.properties as needed ...
  // For FlexTable widgets, ALSO modify cleaned.properties.visualizationProperties.tableColumns
  return cleaned;
});
await api(
  "PATCH",
  "/analytics/dashboards/01ZTb00000FSP7hMAH",
  strip({ ...m, components: newComponents }, DASH_READONLY),
);

// Verify
const verify = await api("GET", "/analytics/dashboards/01ZTb00000FSP7hMAH");
// assert the change landed
```

### Test reference implementations

Working scripts from the pristine pass (stored in `/tmp/` during the session — can be re-derived from this doc):

- `/tmp/drift-check.mjs` — diff 9 claimed inline fixes against live state
- `/tmp/full-state-dump.mjs` — precursor to `scripts/dashboard_state_dump.py`
- `/tmp/fix-convert-drift.mjs` — PATCH Forecast_ARR → `.CONVERT` on 2 D1 widgets
- `/tmp/comprehensive-fix.mjs` — batch improvement across both dashboards
- `/tmp/fix-no-activity.mjs` — FlexTable dual-storage PATCH pattern (load-bearing)
- `/tmp/cleanup-stale-aggs.mjs` — retire stale non-`.CONVERT` aggregates
- `/tmp/final-fixes.mjs` — High Value Stale Deals `.CONVERT` + CALENDAR_QUARTER verification
- `/tmp/build-new-reports.mjs` — clone-template POST pattern for new reports
- `/tmp/probe-post-2.mjs` — binary-search diagnostic for POST failures

All patterns in these scripts are codified in §6 above.

## 13. Files to read first (next session)

**Primary handoff (this doc):**

- `docs/2026-04-09-fresh-session-handoff.md` (this file)

**Original handoff audit trail:**

- `docs/2026-04-08-sales-director-monthly-handoff.md` (commit `2550fa7` — Phases 1-2.8)

**Pristine pass docs:**

- `docs/2026-04-08-pristine-dashboard-design.md` — target-state design principles (P1-P6)
- `docs/2026-04-08-manual-ui-runbook.md` — precise click-by-click manual work
- `docs/2026-04-08-dashboard-pristine-pass-handoff.md` — pristine pass session handoff
- `docs/2026-04-09-pristine-design-research-addendum.md` — 7 research findings + 8-widget blueprint

**Source contracts (still authoritative for every widget):**

- `docs/specs/report-1-source-contract.md`
- `docs/specs/report-2-source-contract.md`
- `docs/audits/2026-04-08-deep-audit-against-stakeholder-goals.md`

**New Phase 5 metric registry:**

- `reporting-layer/README.md`
- `reporting-layer/metrics/*.yml` (4 metrics)
- `reporting-layer/contracts/*.yml` (3 contracts)

**Reusable audit tool:**

- `scripts/dashboard_state_dump.py`

**Project memory:**

- `~/.claude/projects/-Users-test-crm-analytics/memory/MEMORY.md` (index)
- `~/.claude/projects/-Users-test-crm-analytics/memory/project_dashboard_pristine_pass_2026-04-08.md` (pristine pass load-bearing patterns — READ THIS for the FlexTable dual-storage + POST constraint gotchas before touching reports)
- `~/.claude/projects/-Users-test/memory/feedback_sf_classic_dashboard_lightning_save.md` (don't automate SF Classic Lightning save)

## 14. Contact + escalation

Session author (all sessions 2026-04-06 through 2026-04-09): Andre (`apro@simcorp.com`). Questions, discovered drift, or schema decisions: escalate via project memory at `~/.claude/projects/-Users-test-crm-analytics/memory/`.
