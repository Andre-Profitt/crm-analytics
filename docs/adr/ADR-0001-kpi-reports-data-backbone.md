# ADR-0001: KPI Reports Data Backbone — CRM Analytics Primary, Salesforce Reports as Link Layer

**Date:** 2026-04-06
**Status:** Accepted
**Deciders:** Andre P. (Senior Rev Intelligence)
**Scope:** Sales Directors monthly report (Report 1) and Sales Ops quarterly report (Report 2) for the SimCorp Salesforce org (`apro@simcorp.com`).

---

## Context

SimCorp's Revenue Operations function needs two recurring KPI deliverables:

- **Report 1 — Sales Directors monthly:** forward-looking PowerPoint covering pipeline overview by region, commercial approval coverage, renewals tracking, churn risk, and slipped-deal analysis.
- **Report 2 — Sales Ops quarterly:** PowerPoint readout backed by a CRMA dashboard, covering CRM data quality, process compliance, forecast accuracy, and pipeline hygiene.

Both reports are PowerPoint at the audience layer, but the audience layer is the _output_, not the _system of record_. The architectural question is: where do the KPIs live, and what data backbone are they computed against?

The candidate backbones in this org are:

1. **CRM Analytics (CRMA / Tableau CRM)** — Wave API, SAQL, datasets, dashboards. Pre-joined datasets exist for opportunities, accounts, users, and OLI. Composite metrics, stage history, and forecast snapshots are first-class.
2. **Standard Salesforce Reports** — Analytics REST API, SOQL-backed report definitions, summary/matrix/joined report types, scheduled subscriptions, native record actions.
3. **Hybrid** — both backbones used selectively, with one designated as primary.

### Business drivers

- Sales Directors are not the audience for raw dashboards; they consume a curated narrative monthly. The deck IS the deliverable.
- Sales Ops needs both a recurring deck AND a live dashboard system-of-record they can drill into between cycles.
- Stakeholder familiarity matters: standard Salesforce reports are universally readable across SimCorp's commercial org; CRMA literacy is concentrated.
- Data quality, process compliance, forecast accuracy, and pipeline hygiene are KPIs that are _easy in CRMA and hard in standard reports_ because they require composite scoring, stage history, and cross-object joins.
- Some Report 1 slides (commercial approval candidate lists, slipped-deal lists, renewals roster) are fundamentally **action-oriented record lists**, not analytics — they need a clickable, real-time, owner-actionable surface, not a dashboard tile.

### Technical constraints

- **No Metadata API access.** The org has not granted Metadata API permissions. This means:
  - Reports and dashboards cannot be deployed as `.report-meta.xml` / `.wdash-meta.xml` files via SFDX (`sf project deploy start`).
  - Source-control of report/dashboard definitions cannot use the standard SFDX file-based workflow.
  - Both candidate backbones are equally affected — Metadata API is **not** the same as the runtime APIs each backbone uses.
- **Runtime APIs are unaffected and fully usable on both paths:**
  - Analytics REST API (`/services/data/v66.0/analytics/reports/...`) supports full CRUD on standard report definitions via JSON payloads. Already proven end-to-end in `salesforce-api/deploy_coo_dashboard.py` (35 reports + 2 standard dashboards deployed without Metadata API).
  - Wave API (`/services/data/v66.0/wave/...`) supports full CRUD on CRMA datasets, dashboards, dataflows, and queries. Already proven end-to-end across 48 dashboards in this repo.
- **CRMA dashboard chart-widget limit: 20 per dashboard.** BOB and RTB are already at this limit.
- **CRMA dataflow latency:** datasets refresh on a dataflow schedule (typically hourly or daily), so CRMA is _near_-real-time, not strictly live. Standard reports run live SOQL at request time.
- **Auth path:** `sf` CLI (`sf org display --target-org apro@simcorp.com --json`) is the documented auth source. No `.env` files for org credentials.

### Current situation

- Report 1 has a working deck scaffold at `output/sales_director_monthly_deck_2026-03-31/` with a 9-slide pptx generated via `pptxgenjs`, snapshot pulled from CRMA datasets (`Executive_Revenue_Source_Truth`, `Forecast_Revenue_Motions`, `Revenue_Retention_Health`, `Pipeline_Opportunity_Operations`).
- Report 2 has a working deck scaffold at `output/sales_ops_quarterly_deck_2026-03-31/` with a baseline showing the four headline KPIs (data completeness 83.5, process compliance 58.6, forecast accuracy 65.5, pipeline hygiene 47.1). It is fed by per-page CRMA dashboard mutation-prep JSONs.
- A Sales Ops Quarterly CRMA dashboard exists at `0FKTb0000000K5BOAU` ("Sales Ops Data Quality & Forecast Accuracy"). Most recent Wave PATCH was a `dry_run` on 2026-03-31 — production deploy state pending verification.
- An existing spec at `docs/sales-director-and-sales-ops-reporting-spec.md` (line 7–11) already documents the recommendation as "Report 1 hybrid; Report 2 dashboard-first," but does not explicitly call out the Metadata API constraint, the upgrade triggers, or the rationale for choosing CRMA as primary over standard reports. This ADR formalizes that decision.
- Two known publish blockers exist on Report 1 (Finance churn feed, slipped-deal owner commentary) and are tracked separately in the publish_checklist contract — not part of this ADR's scope.

### Problem statement

We need a durable, defensible choice of data backbone for these two recurring KPI reports that:

1. Honors the existing investment in CRMA datasets and the working scaffolds.
2. Does not require Metadata API access.
3. Plays to each backbone's strengths rather than forcing one tool to do everything.
4. Leaves the door open to reconsider as constraints (Metadata API, Tableau Next, license cost, audience size) evolve.
5. Records _why_ the choice was made and _when_ to revisit it.

---

## Decision

**CRM Analytics (Wave API) is the primary data backbone for both Report 1 and Report 2.**

**Standard Salesforce Reports (Analytics REST API) are the secondary "actionable list" link layer**, used specifically where slides need to surface a record-level list that the reader will _act_ on — commercial approval candidate lists, slipped-deal rosters, renewals due-this-quarter rosters.

Concretely:

| Layer                                               | Backbone                                    | API                                            | Purpose                                                                                                                       |
| --------------------------------------------------- | ------------------------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Report 1 deck content (KPIs, charts, narrative)** | CRMA                                        | Wave API SAQL                                  | Composite metrics, regional cuts, forecast vs actual, churn trend, pipeline movement                                          |
| **Report 1 actionable lists (3 slides)**            | Standard SF Reports                         | Analytics REST API                             | Stage 3 land deals missing approval, slipped deals roster, renewals due this quarter — embedded as clickable URLs in the deck |
| **Report 2 dashboard (system of record)**           | CRMA                                        | Wave API                                       | Live Sales Ops dashboard with data quality, compliance, forecast accuracy, pipeline hygiene, action queues                    |
| **Report 2 deck content**                           | CRMA                                        | Wave API SAQL                                  | Quarterly readout snapshots from the dashboard                                                                                |
| **Auth (both)**                                     | `sf` CLI                                    | `sf org display --target-org apro@simcorp.com` | No `.env`, no MCP, no hardcoded credentials                                                                                   |
| **Version control of definitions (both)**           | JSON state snapshots committed in this repo | n/a (no Metadata API)                          | Wave dashboard `state` JSON for CRMA; report definition JSON for standard reports                                             |

**This decision does not change the existing implementation.** Both deck scaffolds already use CRMA via this pattern. The standard-reports link layer is partially in place (35 reports already live via `salesforce-api/deploy_coo_dashboard.py`) but is **not yet wired into the Report 1 deck slides** — that wiring is a follow-up task tracked outside this ADR.

---

## Rationale

### Capability-fit per KPI

| KPI                                              | Backbone        | Why                                                                                                                                                                         |
| ------------------------------------------------ | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pipeline overview by region                      | CRMA            | Cross-region comparison via heatmap; pre-joined dataset                                                                                                                     |
| Commercial approval overview (counts/aggregates) | CRMA            | Composite of `NeedsApproval`, `Stage_20_Approval__c`, `Approval_Status__c` across regions                                                                                   |
| **Stage 3 land deals missing approval — list**   | **SF Report**   | Record list for owner action; native row-level link → opp record                                                                                                            |
| Renewals tracking (counts, value, risk)          | CRMA            | Existing `Revenue_Retention_Health` dataset has it pre-computed                                                                                                             |
| **Renewals roster — list**                       | **SF Report**   | Owner-actionable list with native record links                                                                                                                              |
| Churn risk + trend                               | CRMA + external | CRM-side trend from CRMA; Finance feed merged externally per existing overlay contract                                                                                      |
| **Slipped deals — list**                         | **SF Report**   | Owner-actionable list; the trend stays in CRMA                                                                                                                              |
| Slipped deals trend / regional concentration     | CRMA            | Stage history requires `OpportunityHistory` joins; pre-computed in CRMA                                                                                                     |
| Data completeness / accuracy (composite score)   | CRMA            | `case when` aggregations across many fields; cannot be expressed cleanly as a SF report formula                                                                             |
| Process compliance rates                         | CRMA            | Same — requires `count(case when ...)` across multiple compliance signals                                                                                                   |
| Forecast accuracy (historical)                   | CRMA            | `Forecast_Intelligence` dataset already stores forecast snapshots; standard reports cannot reconstruct historical forecast vs actual without recording snapshots as records |
| Pipeline hygiene (aging, stage progression)      | CRMA            | Stage progression needs history joins; pre-computed in CRMA                                                                                                                 |

The split is not arbitrary: every Report 2 KPI is in CRMA's "natural strength" zone, and every Report 1 KPI is too — _except_ for the three actionable lists, which are in standard reports' natural strength zone (record actions, ownership, real-time, universally readable).

### Trade-offs considered

- **Real-time freshness:** standard reports win on freshness (live SOQL) over CRMA (dataflow latency). For these KPI reports, the freshness gap is irrelevant — a monthly deck does not need second-by-second accuracy. The actionable list slides DO benefit from freshness, which is exactly why those slides use standard reports.
- **Audience familiarity:** standard reports win on universal SF user literacy. We compensate by keeping the audience's primary surface as PowerPoint (Report 1) and a curated CRMA dashboard (Report 2). Sales Directors don't need to read SAQL — they read the deck.
- **Composite KPIs:** CRMA wins decisively. Report 2's four headline KPIs cannot be expressed as standard reports without storing intermediate computed records on the underlying objects, which is itself a custom development burden.
- **Existing investment:** CRMA wins. 48 dashboards exist; both deck scaffolds already pull from CRMA. Pivoting to standard reports as primary would mean rewriting `refresh_sales_director_monthly_snapshot.py` and re-validating every widget for zero user-visible benefit.
- **Metadata API constraint:** **does not differentiate the two backbones.** Both are accessible via runtime APIs. Both lose the SFDX-style file-based version control. Both will live as JSON snapshots in this repo until Metadata API access is granted.
- **License cost:** CRMA requires CRM Analytics licenses for users who want to drill in to dashboards directly. SimCorp already has these for the Sales Ops audience. Sales Directors are deck consumers, not dashboard users, so license footprint is small. Standard reports are included with all SF licenses.
- **Future migration:** CRMA datasets port more cleanly to Tableau Next than standard reports do. If SimCorp consolidates on Tableau Next (a possibility flagged in user memory), CRMA-primary preserves the upgrade path.
- **Dashboard widget limit (20 per page):** CRMA's hard limit is real and BOB/RTB are already at it. The Sales Ops Quarterly dashboard (`0FKTb0000000K5BOAU`) currently has 6 pages, each well under 20 widgets, so this is not a present constraint for Report 2. Future widget pressure on Report 2 will require splitting pages, which is supported.

### Why not pure standard reports (rejected)

See alternatives section below — short version: Report 2's KPIs cannot be expressed cleanly as standard reports, and Report 1 has zero user-visible benefit from migration off the existing CRMA scaffolds.

### Why not pure CRMA (rejected)

The actionable list slides in Report 1 are fundamentally a record-list problem, not an analytics problem. CRMA can render them (`navigateToRecord` action), but a SF report URL is more natural for a one-click drill into an owner-editable list, integrates with native subscription/sharing, and updates in real time without dataflow lag. Forcing those three slides into CRMA would mean either embedding compare tables (which feel like dashboards, not action lists) or building a CRMA action queue page that duplicates what a SF report does for free.

---

## Consequences

### Positive

- **No code rewrite needed.** Existing deck scaffolds continue to work unchanged.
- **Composite KPIs are expressed in their natural language (SAQL),** which is the only honest way to compute them.
- **Record-list slides become clickable** — Sales Directors can land in a live, owner-editable list in one click instead of asking RevOps for a CSV.
- **Each tool plays its strength;** no awkward "force fit" of one backbone to do everything.
- **Tableau Next migration path stays open** for the analytics layer.
- **No dependency on Metadata API access** — both backbones are reachable today.
- **Audience layer (PowerPoint) is decoupled** from data backbone choice. Future migration of the analytics layer does not require redesigning the deck format.
- **Existing operator wrappers (`scripts/run_report1_monthly_default.sh`) continue to work** with no flag changes.

### Negative

- **Two backbones to maintain.** Wave API patterns and Analytics REST API patterns are different; engineers need fluency in both. Mitigation: `salesforce-api/deploy_coo_dashboard.py` and `crm-analytics/` already encapsulate the two patterns separately, and `salesforce_client.py` is the shared low-level client.
- **No SFDX-deployable artifacts** for either path until Metadata API access is granted. JSON snapshots in repo are the substitute. Mitigation: tracked in upgrade triggers below.
- **CRMA dataflow latency** means the deck baselines a snapshot date, not a real-time view. Mitigation: documented in each `.summary.json`; snapshot date is part of the deck title.
- **CRMA dashboard 20-widget limit** caps how much we can pack onto any one page. Mitigation: split into multi-page dashboards; the Sales Ops Quarterly dashboard already plans 6 pages.
- **License footprint** for CRMA grows with audience size. Mitigation: keep Sales Directors as deck consumers (not dashboard users); only Sales Ops audience needs licenses.

### Neutral

- The decision encodes the existing reality. There is no migration cost in _adopting_ this decision; the cost is the upgrade work whenever a trigger fires.
- Adding the standard-reports link layer to Report 1 slides is a follow-up task, not part of this decision.
- The Sales Ops dashboard `0FKTb0000000K5BOAU` needs to be promoted from `dry_run` to live deploy, but that is implementation work, not an architectural change.

---

## Alternatives Considered

### Alternative 1: Pure Standard Salesforce Reports (rejected)

**Description:** All KPIs in both reports computed via standard SF reports (summary, matrix, joined types). PowerPoint generated from report results pulled via Analytics REST API.

**Pros:**

- Universal user familiarity
- Real-time data, no dataflow latency
- Native subscription/sharing/scheduling
- No CRMA license dependency
- Native record actions on rows

**Cons:**

- Report 2's four headline KPIs (composite data quality score, composite process compliance, forecast accuracy from historical snapshots, pipeline hygiene with stage progression) cannot be expressed as standard reports without significant custom development (storing intermediate computed records on the underlying objects, custom report types for stage history, etc.).
- Report 1 currently uses CRMA datasets — migration would require rewriting the snapshot refresher and re-validating every widget for **zero user-visible change**. Pure churn.
- Cross-object joins beyond parent-child require custom report types per join shape, multiplying maintenance.
- Without Metadata API, custom report types still cannot be deployed via SFDX — same constraint, less expressive backbone.
- No upgrade path to Tableau Next.

**Why rejected:** the work-to-benefit ratio is upside-down. We would do significant rework to _lose_ the expressive backbone we already have, gain freshness we don't need for monthly/quarterly cadence, and end up unable to express the Report 2 KPIs cleanly.

### Alternative 2: Pure CRM Analytics (rejected)

**Description:** All KPIs and slides — including the actionable record lists — computed and surfaced from CRMA only. No standard reports involved.

**Pros:**

- Single backbone, single mental model
- Maximum expressiveness
- Simpler operational story
- Existing scaffolds match this exactly today

**Cons:**

- Actionable list slides are awkward in CRMA: compare tables feel like dashboards, not action lists, and `navigateToRecord` is per-cell rather than per-row.
- CRMA dashboards are not natively shareable as URL → record-list views the way SF reports are.
- No native subscription/scheduling for record-list rosters.
- License footprint grows because anyone wanting to act on a record list needs CRMA access.

**Why rejected:** the three actionable list slides in Report 1 are fundamentally a record-list problem, not an analytics problem. Forcing them into CRMA loses the one-click drill-into-live-list benefit and adds friction for the audience.

### Alternative 3: Build a custom data warehouse / external store (rejected)

**Description:** Extract opportunity, account, forecast, and history data into an external store (e.g., Snowflake, Postgres, or a flat-file pipeline). Compute KPIs externally. Render the deck from the external store.

**Pros:**

- Full SQL expressiveness
- Decoupled from Salesforce API rate limits
- Could feed multiple downstream consumers

**Cons:**

- Massive scope creep — this is not what the user asked for.
- Adds infrastructure (storage, ETL, scheduler, monitoring) that does not currently exist.
- Loses the "live drill into Salesforce" benefit for action lists.
- Duplicates state, introducing freshness/consistency questions.
- No alignment with current investment in CRMA datasets.

**Why rejected:** scope is wrong. The user has working CRMA datasets and a working deck pipeline; the question is which tool fits which slide, not "should we build a new platform."

### Alternative 4: Defer the decision (rejected)

**Description:** Don't make a backbone decision; let each new KPI pick its own backbone ad-hoc.

**Pros:**

- Maximum flexibility per KPI

**Cons:**

- No durable record of _why_ we picked what we picked.
- New KPIs and engineers will re-litigate the decision repeatedly.
- The implementation already encodes a decision implicitly (everything is CRMA-first); refusing to write it down does not make it not a decision, it just makes it undocumented.

**Why rejected:** the decision is already made implicitly in code. This ADR is the formal record. Deferring just means leaving the record absent.

---

## Implementation Notes

### Existing artifacts that already follow this decision

- `crm-analytics/output/sales_director_monthly_deck_2026-03-31/` — Report 1 deck workspace, pulls from CRMA
- `crm-analytics/output/sales_ops_quarterly_deck_2026-03-31/` — Report 2 deck workspace, pulls from CRMA dashboard mutation-prep JSONs
- `crm-analytics/scripts/run_sales_director_monthly_report.py` — Report 1 one-command runner
- `crm-analytics/scripts/run_report1_monthly_default.sh` — Report 1 locked operator wrapper
- `crm-analytics/output/sales_ops_pages_1_6_combined_2026-03-31/` — assembled Sales Ops dashboard state for `0FKTb0000000K5BOAU`
- `salesforce-api/deploy_coo_dashboard.py` — proves the standard-reports backbone is reachable in this org without Metadata API (35 reports + 2 standard dashboards already deployed)

### Follow-up work created by this decision (NOT done as part of this ADR)

1. **Wire standard-reports URLs into Report 1 deck slides.** Three slides need clickable links:
   - Slide: Stage 3 land deals missing commercial approval → SF report URL
   - Slide: Slipped deals roster → SF report URL
   - Slide: Renewals due this quarter → SF report URL
     The reports themselves likely already exist in `salesforce-api/`'s 35-report inventory; verification + URL embedding is the new work.
2. **Promote `0FKTb0000000K5BOAU` from `dry_run` to live deploy** for the Sales Ops Quarterly dashboard. Currently only proven in dry-run mode in `output/sales_ops_pages_1_6_combined_2026-03-31/wave_patch_overview.md`.
3. **Document the JSON snapshot version-control pattern** as the substitute for SFDX file-based version control until Metadata API access is granted. Currently implicit; should be explicit in the repo README.
4. **Resume the paused 2026-03-31 refresh** on the existing CRMA-backed flow once this ADR is approved. Refresh design lives in conversation history; no spec was written because the user paused it before that step.

### Migration strategy

There is no migration. The decision encodes the existing implementation. The "implementation" of this ADR is its acceptance and the follow-up work above.

### Dependencies

- `sf` CLI installed and authenticated against `apro@simcorp.com`
- CRMA license on the SimCorp org (confirmed — already in use)
- Wave API access (confirmed — already in use across 48 dashboards)
- Analytics REST API access (confirmed — already in use across 35 reports)
- `pptxgenjs` (Node.js) for deck generation (already installed in deck workspaces)
- Microsoft PowerPoint installed locally for WYSIWYG validation export (confirmed at `/Applications/Microsoft PowerPoint.app`)

### Upgrade triggers (when to revisit this ADR)

| Trigger                                                 | Likely move                                                                                                                   | New ADR needed?                               |
| ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **Metadata API access granted**                         | Begin SFDX-version-controlling reports + CRMA dashboards as deployable artifacts. No backbone change.                         | No — supersedes the version-control note only |
| **Org consolidates on Tableau Next**                    | Migrate CRMA datasets to Tableau Next datasets; rewrite deck pulls against Tableau APIs. Standard reports unchanged.          | Yes — supersede this ADR                      |
| **CRMA license cost becomes contentious**               | Migrate Report 1 actionable list slides fully to standard reports; consider whether Report 2 audience can be reduced or moved | Yes — supersede this ADR                      |
| **A new KPI requires sub-minute freshness**             | Use a standard report for that specific KPI, embed via URL — does not invalidate the overall decision                         | No                                            |
| **Sales Ops audience grows beyond CRMA-licensed users** | Expand standard-reports surface for democratized access; CRMA stays for the analytics layer                                   | Maybe — depends on scale                      |
| **CRMA hits the 20-widget limit on Report 2**           | Split into more dashboard pages; not an architectural change                                                                  | No                                            |
| **Finance owns more KPIs and wants their own backbone** | Re-evaluate per-KPI ownership boundaries                                                                                      | Yes if scope shifts materially                |

---

## References

### Repo artifacts

- [Existing reporting spec](../sales-director-and-sales-ops-reporting-spec.md) — pre-existing, lines 7–11 already document "Report 1 hybrid; Report 2 dashboard-first" without the ADR formalization
- [Sales Ops dashboard implementation contract](../sales-ops-dashboard-implementation-contract.md)
- [Pipeline & Sales Ops dashboards design spec (2026-03-25)](../superpowers/specs/2026-03-25-pipeline-salesops-dashboards-design.md) — earlier spec proposing Python builders; superseded by current Wave API mutation-prep flows
- [Pipeline & Sales Ops dashboards plan (2026-03-25)](../superpowers/plans/2026-03-25-pipeline-salesops-dashboards.md) — earlier plan; checkboxes never tracked, current implementation uses different patterns
- [Sales Director monthly deck README](../../output/sales_director_monthly_deck_2026-03-31/README.md)
- [Sales Ops quarterly deck README](../../output/sales_ops_quarterly_deck_2026-03-31/README.md)
- [`salesforce-api/CLAUDE.md`](../../../code/apps/salesforce-api/CLAUDE.md) — standard reports backbone via Analytics REST API
- [`crm-analytics/CLAUDE.md`](../../CLAUDE.md) — Wave API gotchas, SAQL rules

### Org details

- Target org: `apro@simcorp.com`
- Instance: `simcorp.my.salesforce.com`
- API version: v66.0
- Sales Ops Quarterly dashboard ID: `0FKTb0000000K5BOAU`
- BOB dashboard ID: `01ZTb00000DoGYLMA3` (at 20-widget limit)
- RTB dashboard ID: `01ZTb00000E1e50MAB` (at 20-widget limit)

### Related ADRs

- None yet — this is ADR-0001 in this repo.

### External

- Salesforce Analytics REST API reference (Reports & Dashboards): used by `salesforce-api/deploy_coo_dashboard.py`
- Salesforce Wave API (CRM Analytics) reference: used throughout `crm-analytics/`
- SimCorp fiscal calendar: fiscal year starts February 1 (Q1 = Feb–Apr, Q2 = May–Jul, Q3 = Aug–Oct, Q4 = Nov–Jan)
