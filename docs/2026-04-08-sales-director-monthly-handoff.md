# Sales Director Monthly + Sales Ops Quarterly - Handoff (2026-04-08)

> Full-session handoff. North star = the two original stakeholder asks. This doc explains every change made, the current state of both dashboards, and exactly what's left. A future session (or a new operator) should be able to pick this up cold.

## North Star: the original asks

### Track 1 - Sales Directors Monthly (Report 1)

Audience: **9 named Sales Directors** (MD-1 level). Cadence: monthly. Format: one filterable dashboard feeding a per-Director deck.

**Stakeholder bullets (verbatim from the brief):**

- A pipeline overview with quarterly focus (one slide per region)
- Commercial Approval overview - which deals have been approved and a list of any Land stage 3 deals with no commercial approval (A global overview (one slide) + the list of candidates by region (one slide))
- Renewals tracking (what renewals are coming up this quarter, what is the value and likelihood of renewing)
- Churn Risk and trends (difficult for now, but let's try and build a slide of what we can get from Finance for now. Please reach out to Alex P and understand from who in Finance he gets this reporting, and please get you involved, so you also receive this going forward.)
- Slipped deals analysis (root cause commentary) - start with slipped deals - root cause commentary most likely need us to reach out to the opportunity owner.

**Stakeholder feedback bullets also captured:**

- "0 - no opportunity, no reason is OK. Maybe rename to Missing Win/Loss Reason" (applies to Won/Loss Info Missing widget filter)
- "Overdue close date open Opps: sort by largest record count instead of Opps owner"
- "KYC missing: Accounts without KYC Approval"
- "Pipeline Reporting (Sales director monthly). one report per MD-1" (the filterable-by-director requirement)
- "Renewal amount -> ACV" (aggregation field hard rule)
- "Missing commercial approval overview / list of opportunities"

**9 named Sales Directors (MD-1):**

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

Role-hierarchy scoping alone does NOT cleanly slice the 9 Directors - 5 of 9 share roles with peers. Per-Director slicing must come from dashboard filters on `Sales_Region__c`, `Account.BillingCountry`, and (eventually) `Account.Industry`.

### Track 2 - Sales Ops Quarterly (Report 2)

Audience: Sales Operations. Cadence: quarterly. Format: CRMA-style dashboard as the system of record; quarterly PowerPoint readout derived from it.

Distilled spec sections (from `docs/specs/sales-ops-quarterly-dashboard-spec.md`):

1. CRM Data Quality (5 widgets)
2. Process Compliance (5 widgets)
3. Forecast Accuracy (4 widgets, all Pipeline Inspection native)
4. Pipeline Hygiene (8 widgets)

**Total: 22 spec widgets.**

Note: the original raw stakeholder messages for Track 2 were not in this session's context. The authoritative distillation lives at `docs/sales-director-and-sales-ops-reporting-spec.md` (commit `8c81d2d` area) and `docs/specs/sales-ops-quarterly-dashboard-spec.md`.

## Target dashboards

- **Dashboard 1:** `01ZTb00000FSP7hMAH` - "Sales Directors Monthly Pipeline and Insights"
- **Dashboard 2:** `01ZTb00000FSP9JMAX` - "Sales Ops Quarterly KPI Dashboard"

Both share folder `005QA000003DUwWYAW` (Andre's personal folder). Target org `apro@simcorp.com`, instance `simcorp.my.salesforce.com`, API v66.0.

## Phase journey

| Phase            | Date       | What it did                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Phase 1          | 2026-04-06 | Initial audit of Dashboard 1 against the 16-widget spec. 12 BLOCKING / 10 WRONG-DATA. Decision captured to go CRMA-first, later deprioritized.                                                                                                                                                                                                                                                                                                      |
| Phase 1.5        | 2026-04-07 | Dashboard 1 hotfix: 3 renewal AMOUNT-to-ACV aggregation swaps + 6 in-flight dashboard component fixes.                                                                                                                                                                                                                                                                                                                                              |
| Phase 2          | 2026-04-07 | Dashboard 2 audit + Report 2 source contract authored.                                                                                                                                                                                                                                                                                                                                                                                              |
| Phase 2.5 B-core | 2026-04-08 | Dashboard 2 ARR + fiscal fixes.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Phase 2.6        | 2026-04-08 | Dashboard 2 process compliance: 5 new SF reports built via POST, 5 new dashboard components, `ph_aging_pipeline_365_plus` ARR fix.                                                                                                                                                                                                                                                                                                                  |
| Phase 2.7        | 2026-04-08 | Dashboard 1 missing widgets: 8 new SF reports, 3 added as dashboard components. Initial approach hardcoded regions into separate reports - later rebuilt.                                                                                                                                                                                                                                                                                           |
| Phase 2.8        | 2026-04-08 | Recontextualization: "one filterable dashboard, not 9 clones". Deleted 5 hardcoded regional reports + 5 ORPHAN cruft components. Amended source contracts with 9 Director preset filter combos. Dashboard 2 fiscal sweep (10 reports PATCHed). Matcher improved with source-contract pinning. Deep audit against stakeholder goals + 10 inline defect fixes. Playwright attempt at filter creation: failed on save. Visual polish via API: 5 fixes. |

## Where we are now

### Dashboard 1 - Sales Directors Monthly

**Components:** 15 chart/table widgets (down from 20 earlier in the session after removing 5 ORPHAN cruft, up from 17 pre-Phase-2.7 with 3 new Phase 2.7 widgets added).

**Live composition (post-polish):**

| #   | Widget                                              | Report ID                   | Shape                                                                           | Addresses stakeholder ask                          |
| --- | --------------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------- |
| 1   | Renewal ACV by Quarter                              | `00OTb000008ekxBMAQ`        | SUMMARY/Bar, FISCAL_QUARTER grouping                                            | Renewals tracking                                  |
| 2   | Land Stage 3 Missing Approval by Region             | `00OTb000008ekltMAA`        | SUMMARY/Bar, ROLLUP_DESCRIPTION grouping, ARR agg                               | Commercial Approval (list of candidates by region) |
| 3   | Pipeline Overview by Stage                          | `00OTb000008fBfdMAE` (P2.7) | SUMMARY/Bar, STAGE_NAME grouping, ARR agg                                       | Pipeline overview                                  |
| 4   | Renewal Pipeline This Quarter                       | `00OTb000008ektxMAA`        | SUMMARY/Bar, STAGE_NAME grouping, ACV agg                                       | Renewals tracking                                  |
| 5   | New Customers (Land) by Region                      | `00OTb000008ekqjMAA`        | SUMMARY/Bar, Sales_Region\_\_c grouping                                         | Land deals context                                 |
| 6   | Forecast Accuracy                                   | `00OTb000008TZsDMAW`        | MATRIX/Column, FISCAL_QUARTER grouping                                          | (not a Track 1 widget per se, adjacent context)    |
| 7   | Close Date Slipped by Stage                         | `00OTb000008eknVMAQ`        | SUMMARY/Bar, STAGE_NAME grouping, ARR agg                                       | Slipped deals analysis                             |
| 8   | Renewals by Quarter (renamed from "Fiscal Quarter") | `00OTb000008eksLMAQ`        | SUMMARY/Bar, FISCAL_QUARTER grouping, ACV agg                                   | Renewals tracking                                  |
| 9   | Commercial Approval Current State                   | `00OTb000008fBEDMA2` (P2.7) | SUMMARY/Bar, Stage_20_Approval\_\_c grouping, RowCount                          | Commercial Approval (global state)                 |
| 10  | Pipeline Coverage by Stage                          | `00OTb000008TZc5MAG`        | SUMMARY/Bar, STAGE_NAME grouping, Forecast_ARR agg                              | Pipeline overview (supplementary)                  |
| 11  | Business At Risk                                    | `00OTb000008Ta9xMAC`        | SUMMARY/FlexTable, Risk_Assessment_Level\_\_c grouping                          | Churn Risk (CRM-side proxy)                        |
| 12  | Forecast and Closed Won                             | `00OTb000008TZaTMAW`        | SUMMARY/Funnel, FORECAST_CATEGORY grouping, Forecast_ARR.CONVERT agg            | Forecast context                                   |
| 13  | Commercial Approval Candidates by Stage             | `00OTb000008ekp7MAA`        | SUMMARY/Bar, STAGE_NAME grouping, ARR agg                                       | Commercial Approval (candidates list)              |
| 14  | Commercial Approval Approved YTD (Land)             | `00OTb000008aTtJMAU`        | SUMMARY/FlexTable, Account_Unit_Group\_\_c grouping, ARR+Forecast+RowCount aggs | Commercial Approval (approved deals)               |
| 15  | Renewal Likelihood by Probability                   | `00OTb000008fBULMA2` (P2.7) | SUMMARY/Bar, PROBABILITY grouping, ACV agg                                      | Renewals tracking (likelihood dimension)           |

**Dashboard 1 audit** (`docs/audits/2026-04-08-sales-director-monthly-audit.md`, commit `f342187`):

`22 entries . 7 BLOCKING . 0 WRONG-DATA . 0 ORPHAN . 0 COSMETIC . 15 OK`

Static rule scan: **0 issues across all 15 widgets.** The cleanest state of the session. The 7 remaining BLOCKING rows are: `slipped_deals_trend` (Pipeline Inspection native per spec hard rule 6, blocked on PI Lightning UI work) and 6 others deferred to the UI filter handoff or to stakeholder/schema decisions.

**Dashboard 1 state details:**

- `dashboardType: SpecifiedUser` (runs as Andre Profitt currently)
- `canChangeRunningUser: False`
- `filters: 0`
- `folder: 005QA000003DUwWYAW`
- Format: classic SF dashboard (not yet Lightning-converted; see save-failure note below)

### Dashboard 2 - Sales Ops Quarterly

**Components:** 18 chart/table widgets (5 were built in Phase 2.6, 13 pre-existing).

**Dashboard 2 audit** (`docs/audits/2026-04-08-sales-ops-quarterly-audit.md`, commit `0eee5b7`):

With source contract pinning: `24 entries . 7 BLOCKING . 1 WRONG-DATA . 0 ORPHAN . 16 OK`

vs initial Phase 2.6 baseline: `40 / 23 / 9 / 7 / 0 / 1` - **WRONG-DATA dropped from 9 to 1, OK rose from 1 to 16.**

- The 1 WRONG-DATA is a known false positive (`Missing Won/Loss Reason` - field IS in detailColumns but the audit matcher substring check fails on word-order between "won/loss reason" and "reason_won_lost").
- The 7 BLOCKING are the 4 Pipeline Inspection forecast accuracy widgets (blocked on PI Lightning UI config) + `dq_kyc_not_completed` already present but not stem-matched + minor others.

**Phase 2.6 process compliance additions (5 new SF reports, on Dashboard 2):**

| Widget                           | Report ID            | Simplification                                         |
| -------------------------------- | -------------------- | ------------------------------------------------------ |
| pc_next_step_documented          | `00OTb000008fAjZMAU` | NextStep IS NULL filter dropped (field not accessible) |
| pc_land_commercial_approval_flow | `00OTb000008fAlBMAU` | clean                                                  |
| pc_recent_activity_logged        | `00OTb000008fAmnMAE` | IS NULL only, not `< TODAY-30` OR clause               |
| pc_won_loss_reason_documented    | `00OTb000008fAoPMAU` | clean, calendar THIS_QUARTER                           |
| pc_stage_age_within_threshold    | `00OTb000008fArdMAE` | single 60-day threshold across stages 3-6              |

## Authoritative source contracts

Phase 4 deck rebuild and Phase 3 cross-check should read these contracts, NOT the audit output, for source-of-truth data mappings.

- **`docs/specs/report-1-source-contract.md`** - Track 1 (Sales Director Monthly). Contains row-level pinnings for all 16 spec widgets plus the Phase 2.7 amendment, the Phase 2.8 amendment with the 9 Director preset filter combos, API constraint captures, and matcher-vocabulary-gap notes.
- **`docs/specs/report-2-source-contract.md`** - Track 2 (Sales Ops Quarterly). Contains row-level pinnings for all 22 spec widgets plus the Phase 2.8 fiscal sweep amendment.
- **`docs/audits/2026-04-08-deep-audit-against-stakeholder-goals.md`** - the deep audit that cross-checks BOTH dashboards against the verbatim stakeholder bullets (not the matcher). Goal-section coverage scorecard per dashboard, actionable defect list (10 of 12 fixed inline during that audit).

## Work done in this session (commit map)

Every commit committed during the session, in chronological order:

| Commit    | Scope                                                                                                                  |
| --------- | ---------------------------------------------------------------------------------------------------------------------- |
| `36103a8` | Phase 2.7 scope doc (combined design + plan)                                                                           |
| `489871a` | Phase 2.7 scope: corrected ARR form after probe finding                                                                |
| `c9ddb70` | Phase 2.7 post-build audit + source contract amendment                                                                 |
| `d09a870` | Phase 2.8: 9 Director preset filter combos in source contract                                                          |
| `6434f66` | Phase 2.8: Dashboard 1 ORPHAN cleanup (5 cruft components removed)                                                     |
| `62c4862` | Phase 2.8: Dashboard 2 fiscal sweep (10 reports PATCHed)                                                               |
| `c913e00` | Phase 2.8: em-dash fix at source (report name rename)                                                                  |
| `0c78b5b` | Phase 2.8: Report 2 source contract amendment                                                                          |
| `2e0bf81` | Phase 2.8: Report 2 verification status sync                                                                           |
| `0eee5b7` | Phase 2.8: Dashboard 2 audit with source contract pinning (matcher gain: BLOCKING 23 -> 7, ORPHAN 15 -> 0, OK 1 -> 16) |
| `1aa7c42` | Deep audit against stakeholder goals + 10 inline defect fixes                                                          |
| `f342187` | Dashboard 1 visual polish (header cleanup + aggregate bind fix)                                                        |

**Out-of-band API-side fixes (not separate commits, recorded in audits):**

- Inline PATCH on `commercial_approval_global` (Phase 2.7 build step) to clear inherited THIS_FISCAL_QUARTER
- 3 dashboard component aggregate rebinding fixes (Land Stage 3 Missing Approval, Close Date Slipped, Commercial Approval Candidates) after the AMOUNT-to-ARR source-report swap left their `properties.aggregates` arrays empty
- 5 Phase 2.7 hardcoded regional reports DELETED (superseded by filter architecture)
- 2 orphan auto-suffixed reports DELETED (Phase_2_7_Pipeline_Global1 and \_Global2)
- Dashboard 1 visual polish PATCH (5 header/title/aggregate fixes)

## Stakeholder coverage scorecard

### Track 1 - Sales Directors Monthly

| Goal                                                                  | Coverage                                                           | Notes                                                                                                                                                                                                                        |
| --------------------------------------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pipeline overview with quarterly focus (one slide per region)         | GLOBAL OK / REGIONAL blocked on filter                             | P2.7 `pipeline_overview_global` widget is on the dashboard. Per-region slices require the UI filter handoff (see below).                                                                                                     |
| Commercial Approval overview (global + Land stage 3 missing approval) | OK                                                                 | 4 widgets cover the concept end-to-end: `commercial_approval_global`, `commercial_approval_approved_ytd`, `commercial_approval_candidates`, `land_stage3_missing_approval`. All on Dashboard 1 with correct ARR aggregation. |
| Renewals tracking (this quarter, value, likelihood)                   | OK                                                                 | 4 widgets covering renewal pipeline, ACV by quarter, renewals by quarter (renamed off "Fiscal"), and the new P2.7 `renewal_likelihood` probability-bucket widget (previously missing).                                       |
| Churn Risk and trends                                                 | PROXY only                                                         | `Business At Risk` widget is the CRM-side fallback. Finance feed is pending Alex P - not reached in this session.                                                                                                            |
| Slipped deals analysis                                                | PARTIAL                                                            | `Close Date Slipped by Stage` widget provides slip detection. Root cause commentary is a workflow (owner outreach), not a dashboard. PI native canonical source is deferred to a PI Lightning UI phase.                      |
| 9 MD-1 Sales Directors per-person views                               | Preset filter combos documented, dashboard filters NOT YET created | See "Remaining work - UI handoff" below. Combo definitions are pinned in report-1-source-contract.md.                                                                                                                        |

### Track 2 - Sales Ops Quarterly

| Section                        | Coverage       | Notes                                                                                                                                                                                                           |
| ------------------------------ | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CRM Data Quality (5 widgets)   | 4/5 OK         | `dq_missing_quote_type` blocked on product decision. `Won/Loss Info Missing CFQ` filter was cleaned per stakeholder bullet ("0 - No Opportunity should not flag").                                              |
| Process Compliance (5 widgets) | 5/5 OK         | All built in Phase 2.6 with documented simplifications.                                                                                                                                                         |
| Forecast Accuracy (4 widgets)  | 0/4 BLOCKED    | All 4 require Pipeline Inspection Lightning UI list view configuration (`fa_forecast_change_volatility`, `fa_slipped_count_quarterly`, `fa_quarterly_realized_vs_commit`, `fa_quarterly_realized_vs_bestcase`). |
| Pipeline Hygiene (8 widgets)   | 7/8 OK + 1 WIP | `ph_probability_mismatch_by_stage` is Under Construction pending Sales Ops threshold decision. Remaining 7 all have correct ARR and calendar framing post-Phase-2.8 fiscal sweep.                               |

## Remaining work

### UI handoff (blocking, ~5 minutes manual work)

**Context:** Salesforce Classic-to-Lightning dashboard save is not browser-automatable. 8 distinct click strategies were tried (DOM click, MouseEvent, PointerEvent, React onClick invocation, keyboard Enter, MCP browser_click CDP path, twice retried) - the top-toolbar Save button in the edit session does not propagate the conversion + filter configuration back to the server. See `feedback_sf_classic_dashboard_lightning_save.md` in project memory for the detailed failure pattern.

**Manual steps:**

1. Open `https://simcorp.lightning.force.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view`
2. Click **Edit** - confirm the Classic-to-Lightning conversion dialog
3. Click **+ Filter** (top toolbar) and add:
   - **Sales Region** filter (field: `Opportunity: Sales Region`, values: APAC, Central Europe, Northern Europe, Southwestern Europe, United Kingdom & Ireland, Middle East & Africa, North America)
   - **Legal Country** filter (field: `Account: Legal Country`, values: Canada, United States)
4. Click **Edit Dashboard Properties** (gear icon), select "The dashboard viewer" (LoggedInUser), click Save inside the properties modal
5. Click **Save** (top toolbar)
6. Click **Done** to exit edit mode

After step 5 succeeds, the 9 Directors each open the dashboard and apply their preset filter combo (see table below, also pinned in `docs/specs/report-1-source-contract.md`):

| Director          | Filter combo                                                                                           |
| ----------------- | ------------------------------------------------------------------------------------------------------ |
| Jesper Tyrer      | Sales Region = APAC                                                                                    |
| Sarah Pittroff    | Sales Region = Central Europe                                                                          |
| Francois Thaury   | Sales Region = Southwestern Europe                                                                     |
| Dan Peppett       | Sales Region = United Kingdom & Ireland                                                                |
| Christian Ebbesen | Sales Region = Northern Europe                                                                         |
| Mourad Essofi     | Sales Region = Middle East & Africa                                                                    |
| Megan Miceli      | Legal Country = Canada                                                                                 |
| Patrick Gaughan   | Sales Region = North America AND Legal Country = United States (NAM remainder)                         |
| Adam Steinhaus    | Sales Region = North America AND Legal Country = United States (NA P&I subset pending Industry filter) |

### UI handoff (Pipeline Inspection, deferred)

2 PI list views need to be created via the Lightning UI (required for 4 forecast accuracy widgets + the slipped deals widgets on both dashboards):

- 26-week trailing change window for `fa_forecast_change_volatility`
- Pipeline Changes view with "Slipped" change type filter for `fa_slipped_count_quarterly`, `slipped_deals_root_cause`, `slipped_deals_trend`

### Schema / data investigation (deferred)

- **Account.Industry picklist mismatch:** the Lightning filter popover for `Account.Industry` shows the standard SF generic industries (Accommodations, Banking, ...) but the actual data contains SimCorp values (Asset Management, Pension, Bank, Asset Servicer, ...). Needs investigation: is there a custom industry field (e.g., `Account.ZIMIT__zIndustry__c`, `Account.Industry_Classification__c`) that should be used instead? Until resolved, the Industry-based filter for Patrick's "NA Asset Management" and Adam's "NA Pension & Insurance" sub-cuts cannot be added as a third dashboard filter - they collapse into a single "North America non-Canada" view.
- **Calendar-quarter grouping field:** 3 widgets still group by `FISCAL_QUARTER` (Renewal ACV by Quarter, Forecast Accuracy, Overdue Opportunities on Dashboard 2). Converting to calendar-quarter grouping requires either a custom formula field on Opportunity or a bucket field. Deferred.

### Stakeholder decisions (external)

- `dq_missing_quote_type` (Dashboard 2): retire vs repurpose to `Type` field. `APTS_Primary_Quote_Type__c` is structurally obsolete.
- `ph_probability_mismatch_by_stage` (Dashboard 2): per-stage probability threshold definition from Sales Ops.
- `churn_risk_placeholder` (Dashboard 1): Finance feed handshake with Alex P.
- Sales Director cumulative-changes notification (external, manual).

## Known constraints captured for future sessions

Written to project memory in `feedback_sf_classic_dashboard_lightning_save.md`:

1. **SF Classic dashboard Lightning conversion save is not Playwright-automatable.** Use a manual UI runbook for filter creation, `canChangeRunningUser` flips, and any Classic-to-Lightning conversion action.
2. **Analytics REST API cannot CREATE dashboard filters** - it can only update existing ones. Filter creation is Lightning UI only.
3. **Analytics REST API CAN** do: POST new reports, PATCH existing reports (aggregates, filters, detail columns, formats, date filters), PATCH dashboard components (properties.aggregates, properties.groupings, visualizationProperties.tableColumns), strip read-only fields before PATCH, inline-verify via GET.

## Data conventions captured this session

- **ARR aggregate form on BOTH Dashboard 1 and Dashboard 2:** `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` (with `.CONVERT` suffix). An earlier working note of "no `.CONVERT` on Dashboard 1" was empirically disproven in the Phase 2.7 probe.
- **ACV aggregate form:** `s!Opportunity.APTS_Renewal_ACV__c.CONVERT`.
- **Calendar date tokens:** bare `THIS_QUARTER`, `THIS_YEAR` - NOT `THIS_CALENDAR_QUARTER` (that form is rejected by the org).
- **Folder ID for both dashboards' reports:** `005QA000003DUwWYAW`.
- **POST body shape:** `{"reportMetadata": {...}}` with read-only fields (`id`, `createdDate`, `lastModifiedDate`, `lastRunDate`, `lastModifiedById`, `createdById`, `currency`) stripped.
- **Dashboard PATCH read-only fields to strip:** `id`, `createdDate`, `lastModifiedDate`, `lastAccessedDate`, `url`, `owner`, `runningUser`, `folderName`, plus component-level `lastModifiedDate`.

## API constraints discovered during Phase 2.7 iterations

1. SF Reports API rejects `groupingsDown` names that also appear in `detailColumns` (specificErrorCode 113). Fix: strip grouping names from detail columns list before POST.
2. SUMMARY reports without `groupingsDown` cannot source dashboard components. Fix: always include at least one grouping.
3. TABULAR reports cannot source dashboard components without `dashboardSetting` + `rowLimit` configuration, which the Reports API does not expose cleanly. Fix: convert TABULAR to SUMMARY with a grouping.
4. SF dashboards have a hard 20 chart/table widget limit (separate from the 25 total component limit).
5. SF auto-suffixes `developerName` on POST collisions even after the colliding report has been DELETEd (collision sticks for some caching window). Name collisions produce `_1`, `_2`, `_3` suffixes.
6. PATCH of `canChangeRunningUser` via the Analytics REST API returns HTTP 200 but silently ignores the flag change. Lightning UI only.
7. PATCH of `dashboardMetadata.filters[]` array with new filter entries returns HTTP 400 `"filter field is no longer available"` because the server-assigned filter id (`0IB` prefix) and its internal field-binding metadata are created by the Lightning UI only.

## Next session recommendations

In priority order:

1. **5-minute UI handoff** (you or Sarah in Salesforce UI): add the 2 dashboard filters (Sales Region + Legal Country), flip `canChangeRunningUser` to true, set LoggedInUser mode. Unblocks the 9 Director preset filter combos.
2. **Industry picklist investigation** (~30 min): run SOQL on `Account.Industry` vs `Account.ZIMIT__zIndustry__c` vs any custom Industry field to identify which one has the SimCorp-value picklist. Once identified, add the third dashboard filter on the right field. Unblocks the NAM 3-Director cuts (Megan/Patrick/Adam distinguishable).
3. **Phase 3 cross-check OR Phase 4 deck rebuild.** Choice: Phase 3 builds a validation script that diffs canonical SF report values against what CRMA steps show (safer path before Phase 4). Phase 4 is the actual deck builder. The `output/sales_director_monthly_deck_2026-03-31/` infrastructure already exists (`refresh_sales_director_monthly_snapshot.py` + `build_sales_director_monthly_deck.js`) - it may just need to be pointed at the new Phase 2.7/2.8 canonical sources.
4. **PI Lightning UI list view creation** (manual): creates the 2 PI list views needed for the 4 forecast accuracy widgets on Dashboard 2 and the slipped deals widgets on Dashboard 1.
5. **Stakeholder decisions:** `dq_missing_quote_type` retire/repurpose, `ph_probability_mismatch_by_stage` threshold, Finance feed via Alex P.

## Files to read first (next session)

- This doc: `docs/2026-04-08-sales-director-monthly-handoff.md`
- Source contracts: `docs/specs/report-1-source-contract.md` + `docs/specs/report-2-source-contract.md`
- Deep audit: `docs/audits/2026-04-08-deep-audit-against-stakeholder-goals.md`
- Latest audits: `docs/audits/2026-04-08-sales-director-monthly-audit.md` + `docs/audits/2026-04-08-sales-ops-quarterly-audit.md`
- Project memory index: `~/.claude/projects/-Users-test/memory/MEMORY.md`

Relevant project memory entries:

- `project_sales_director_monthly_phase2_7_done.md` - phase journey + current state
- `feedback_sf_classic_dashboard_lightning_save.md` - don't automate this
- `feedback_work_pace_and_autonomy.md` - Andre's working style preferences
