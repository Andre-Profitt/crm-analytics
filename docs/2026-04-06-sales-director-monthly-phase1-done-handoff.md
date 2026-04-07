# Next Session Handoff - Phase 1 Done, Phase 2 CRMA Audit Starts Next

> **For the next Claude session.** Phase 1 audit of the Sales Director Monthly standard SF dashboard is complete and committed. This file is the load-bearing context for picking up the Phase 2 CRMA audit cleanly. Read this first, then read the Phase 1 audit output.

## One sentence

Phase 1 audit of `01ZTb00000FSP7hMAH` found it is structurally misaligned with the KPI brief (only 2 of 16 widgets pass; 10 of 14 spec-required widgets are missing); decision is to treat the standard SF dashboard as legacy and go CRMA-first for the Sales Director Monthly deck rebuild, with Phase 2 = audit of the 9 B2B_MA CRMA dashboards against the same committed spec.

## What was done in Phase 1

Committed artifacts (in order):

| Commit    | File                                                            | Purpose                                                    |
| --------- | --------------------------------------------------------------- | ---------------------------------------------------------- |
| `01b5b76` | `docs/2026-04-06-deck-and-dashboard-verification-handoff.md`    | Prior-session handoff that kicked off this work            |
| `8309384` | `docs/2026-04-06-sales-director-monthly-phase1-audit-design.md` | Brainstormed design for Phase 1                            |
| `5fcb89f` | `docs/2026-04-06-sales-director-monthly-phase1-audit-plan.md`   | Bite-sized 17-task implementation plan                     |
| `bd1c609` | (fixup to plan)                                                 | Severity-max bug fix and report-run wording                |
| `f9f0559` | `docs/specs/sales-director-monthly-dashboard-spec.md`           | Expected-widgets spec (initial 14 widgets)                 |
| `06713fb` | (fixup to spec)                                                 | Real Opportunity field names resolved via sobject describe |
| `b09f423` | `docs/audits/2026-04-06-sales-director-monthly-audit.md`        | Phase 1 delta report (25 entries)                          |

Uncommitted (tool, not deliverable):

- `scripts/audit_sales_director_monthly_dashboard.py` - 10-cell notebook-style audit script. Proven end-to-end against the live org. Cells:
  - Cell 1 Auth (shells to `sf org display`)
  - Cell 2 Picklist freshness assertion (asserts APTS_Primary_Quote_Type\_\_c stale values absent)
  - Cell 3 Spec loader + parser tests
  - Cell 4 Dashboard describe (`/analytics/dashboards/{id}/describe`)
  - Cell 5 Report describes (`/analytics/reports/{id}/describe`)
  - Cell 6 Report runs (`GET /analytics/reports/{id}?includeDetails=true`)
  - Cell 7 Static rules + tests
  - Cell 8 Bidirectional comparison + tests (stopword-weighted stem matcher)
  - Cell 9 Markdown rendering + tests
  - Cell 10 Composition (writes audit to `docs/audits/`)

## Phase 1 audit headline

25 entries total: **12 BLOCKING, 10 WRONG-DATA, 1 ORPHAN, 2 OK**.

**The three structural themes that dominated the audit:**

1. **Renewal widgets use the wrong aggregation field.** 3 renewal widgets (`Renewal ACV by Quarter`, `Renewal Pipeline This Quarter`, `Renewals by Fiscal Quarter`) aggregate on standard `Opportunity.Amount` via `s!AMOUNT` instead of the canonical `APTS_Renewal_ACV__c`. The displayed values are meaningless for renewal reporting to Sales Directors. BLOCKING.

2. **13 of 16 widgets use fiscal date filters.** `THIS_FISCAL_YEAR` or `THIS_FISCAL_QUARTER`. The hard rule is calendar year. WRONG-DATA. The only widgets that do NOT have this issue are the 2 OK widgets (CUSTOM date filter) and Win Rate Rolling 90d (LAST_N_DAYS:90).

3. **10 of 14 spec-required widgets are MISSING entirely.** The dashboard lacks the 3 per-region pipeline overviews, the NAM and APAC Land-Stage-3 missing-approval tables, the global commercial approval metric, renewal_likelihood, renewal_upcoming_list, churn_risk_placeholder, and both slipped deals widgets. The dashboard was built from legacy BOB/RTB clones, not from the brief.

**Good news validated by the audit:**

- Zero widgets still filter on `APTS_Primary_Quote_Type__c`. The 10 corrected reports Andre swapped in last session DID fix the stale-picklist issue at the filter level. That work was successful.
- The `APTS_Primary_Quote_Type__c` picklist on Opportunity has zero active values in the org (verified via sobject describe). The migration is complete.
- Commercial Approval Candidates by Stage and Land Stage 3 Missing Approval by Region are OK. Both use CUSTOM date filters, match the commercial-approval spec bullet, and have no static rule hits. **These are the two widgets safe to use as-is for the deck.**

## The decision (confirmed by Andre)

**Option 3.1: treat `01ZTb00000FSP7hMAH` as legacy, go CRMA-first for the deck.**

Rationale (in order of weight):

1. The monthly deck for Sales Directors is the primary deliverable per the brief. The standard SF dashboard is a secondary click-through surface.
2. CRMA is Andre's real working surface. Every `scripts/audit_*.py` targets a CRMA dashboard. The Wave API PATCH muscle memory, the `CLAUDE.md` gotchas, and the proven Option D POC are all CRMA-centric. Standard SF dashboards are the outlier.
3. The proven Option D recipe (`scripts/simcorp_crma_chart_sample.py`) pulls chart data directly from CRMA dashboard SAQL steps via the Wave API. That is the deck's data source.
4. Fixing `01ZTb00000FSP7hMAH` structurally is high-cost yak-shaving. 26 widget-level fixes on an artifact that was never aligned with the brief.

## What to do in the next session (Phase 2 plan)

### Phase 2: audit the 9 B2B_MA CRMA dashboards against the same spec

The committed spec at `docs/specs/sales-director-monthly-dashboard-spec.md` is field-agnostic at the KPI-bullet level - it applies equally well to CRMA steps as to standard SF reports. Reuse it.

The 9 CRMA dashboards in `B2B_MA` (from the original 2026-04-06 handoff doc):

| Dashboard                                  | ID                   | Refreshed  |
| ------------------------------------------ | -------------------- | ---------- |
| Sales Ops Data Quality & Forecast Accuracy | `0FKTb0000000K5BOAU` | 2026-04-06 |
| Pipeline & Opportunity Operations          | `0FKTb0000000KwPOAU` | 2026-04-06 |
| Forecast & Revenue Motions                 | `0FKTb0000000JCLOA2` | 2026-04-06 |
| Executive Revenue Source Truth             | `0FKTb0000000IxpOAE` | 2026-04-06 |
| Revenue Retention & Health                 | `0FKTb0000000J97OAE` | 2026-04-06 |
| Forecast Intelligence                      | `0FKTb0000000Jc9OAE` | 2026-04-06 |
| Account Intelligence KPIs                  | `0FKTb0000000J7VOAU` | 2026-04-06 |
| Customer & Account Health                  | `0FKTb0000000KunOAE` | 2026-04-06 |
| Commercial Rhythm Control Tower            | `0FKTb0000000JPFOA2` | 2026-04-06 |

For each dashboard, identify the steps that map to the spec's 14 KPI bullets. The most directly relevant:

- Pipeline & Opportunity Operations (`0FKTb0000000KwPOAU`) - the `s_region_hygiene` step is already proven working in the Option D POC. Should cover pipeline*overview*\* widgets.
- Revenue Retention & Health (`0FKTb0000000J97OAE`) - should cover renewal\_\* widgets.
- Commercial Rhythm Control Tower (`0FKTb0000000JPFOA2`) - should cover commercial*approval*\* widgets.
- Customer & Account Health (`0FKTb0000000KunOAE`) - placeholder for churn_risk until Finance feed lands.
- Pipeline & Opportunity Operations may also have slipped-deals SAQL.

### How to reuse the audit script for Phase 2

Cells 3, 7, 8, 9, 10 are reusable with minor tweaks:

- **Cell 3 (spec loader):** unchanged. Same spec, same parse logic.
- **Cell 4 (dashboard describe):** replace with CRMA dashboard describe via `GET /services/data/v66.0/wave/dashboards/{id}`. The response shape is different (state.steps dict instead of components list). Adapt `extract_widgets()` to extract CRMA steps as "widgets".
- **Cell 5 (report describes):** not needed for CRMA - all step config is in the dashboard state already (SAQL query, dataset, etc.).
- **Cell 6 (run reports):** replace with running the SAQL via `POST /wave/query` after `html.unescape()` on the step's query and stripping Mustache filter bindings (same pattern as the Option D POC at `scripts/simcorp_crma_chart_sample.py`).
- **Cell 7 (static rules):** mostly unchanged. Add CRMA-specific rules: check the dataset name, check the SAQL for `load "{{datasetId}}/{{versionId}}"`, check for common anti-patterns like `count('field')` which is invalid SAQL.
- **Cell 8 (comparison):** unchanged matcher and ranking logic. Possibly need to loosen the stopword list since CRMA step names differ from SF widget titles.
- **Cell 9 (markdown render):** unchanged.
- **Cell 10 (composition):** loop over all 9 dashboards. Write one audit markdown per dashboard, or one combined audit.

### Suggested cell-by-cell next-session flow

1. Read this handoff doc end to end.
2. Read the Phase 1 audit at `docs/audits/2026-04-06-sales-director-monthly-audit.md` to understand the baseline.
3. Re-run `scripts/simcorp_crma_chart_sample.py` to confirm the Option D recipe still works against the live org.
4. Brainstorm Phase 2 scope with Andre: one combined audit across all 9 dashboards, or 9 separate audits? Probably one combined so the deck-builder can consume a single delta table. Get sign-off before writing code.
5. Copy `scripts/audit_sales_director_monthly_dashboard.py` to `scripts/audit_crma_sales_director_monthly.py`.
6. Replace cells 4-6 with CRMA adapters as described above.
7. Run against one dashboard as a spot check (Pipeline & Opportunity Operations is a safe choice - Option D POC already used it).
8. Run against all 9.
9. Commit the Phase 2 audit output at `docs/audits/<rundate>-sales-director-monthly-crma-audit.md` by exact path.
10. Hand back to Andre.

## Phase 1.5 (minimal hotfix, defer until after Phase 2 or later)

Small patch for the standard SF dashboard so Sales Directors do not click through to a visibly broken artifact in the gap before the deck rebuild:

1. Fix the 3 renewal AMOUNT-vs-ACV BLOCKING widgets via Analytics REST PATCH on each report's `aggregates` field. Swap `s!AMOUNT` for `s!APTS_Renewal_ACV__c`.
2. Rename the dashboard to "Sales Directors Monthly - Legacy View" via the developerName PATCH endpoint (or via the UI if API is too painful).
3. Leave the fiscal date filters and the missing structural widgets.

Phase 1.5 is a 30-minute fix, not a rebuild. Optional: can be deferred until after Phase 2 is complete. Phase 4 (deck rebuild via Option D) is the priority.

## Hard rules (do not violate - copy from the prior handoff)

1. No em-dashes anywhere. Use ASCII hyphens, periods, or rephrase.
2. Calendar year labels only. No `FY26`, no `FY27`, no `Q1 FY26`.
3. Renewals use ACV (`APTS_Renewal_ACV__c`), not ARR. Land and expand pipeline stays in ARR (`APTS_Opportunity_ARR__c`). SimCorp methodology quirk.
4. No MCP tools. Use `sf` CLI, `curl`, `python3 requests` directly. CLI to CLI.
5. Never use `git add .` / `-A` / `-u`. Stage by exact path only. Working tree has many user WIP files (DO NOT touch them).
6. Never push to origin unless Andre explicitly says so.
7. Do not produce basic / elementary work. Consultant-grade output.
8. Don't summarize what was just done. Andre reads diffs.
9. Stop when something feels fundamentally wrong. Plan with Andre, then execute.
10. Auth via `sf org display --target-org apro@simcorp.com --json`. No `.env` files.

## Field reference (resolved 2026-04-06)

| Concept                  | Field                           | Type                                                                                                              |
| ------------------------ | ------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Renewal/Land/Expand type | `Type`                          | picklist (`Land`, `Expand`, `Renewal`)                                                                            |
| Stage                    | `StageName`                     | picklist with numeric prefix: `1 - Prospecting` through `8 - Won`, plus `0 - Lost`, `0 - No Opportunity`, `Quota` |
| Sales region             | `Sales_Region__c`               | string, 7 values: UKI, CE, NE, SWE, NAM, APAC, MEA                                                                |
| Commercial approval      | `Stage_20_Approval__c`          | boolean                                                                                                           |
| Opportunity ARR          | `APTS_Opportunity_ARR__c`       | currency                                                                                                          |
| Renewal ACV              | `APTS_Renewal_ACV__c`           | currency                                                                                                          |
| Slip proxy               | `LastCloseDateChangedHistoryId` | reference (not null = close date changed at least once)                                                           |
| Stale (empty) picklist   | `APTS_Primary_Quote_Type__c`    | picklist with ZERO active values - migration complete                                                             |

## File paths recap

**Committed outputs (2026-04-06 session):**

- Design doc: `docs/2026-04-06-sales-director-monthly-phase1-audit-design.md`
- Plan doc: `docs/2026-04-06-sales-director-monthly-phase1-audit-plan.md`
- Spec: `docs/specs/sales-director-monthly-dashboard-spec.md`
- Phase 1 audit: `docs/audits/2026-04-06-sales-director-monthly-audit.md`
- This handoff: `docs/2026-04-06-sales-director-monthly-phase1-done-handoff.md`

**Uncommitted tools (keep local):**

- Audit script: `scripts/audit_sales_director_monthly_dashboard.py`
- Option D POC: `scripts/simcorp_crma_chart_sample.py` (reference pattern for Phase 2 cells 4-6)

**Reference artifacts from the prior session (still in the run dir):**

- Canonical deck baseline: `output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/sales_director_monthly_pipeline_insights_2026-04-01.pptx`
- Option D POC output: `output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/sample_slide_crma_chart.pptx` (re-run and confirmed alive 2026-04-06)
- Report 1 snapshot: `output/sales_director_monthly_runs/2026-04-06T20-00-11Z_2026-04-01/report1_snapshot.json`

## Suggested next-session opening message

Paste this into the first message:

> Pick up the Sales Director Monthly work per `crm-analytics/docs/2026-04-06-sales-director-monthly-phase1-done-handoff.md`. Phase 1 is done and committed. Start with Phase 2: audit the 9 B2B_MA CRMA dashboards against the spec at `docs/specs/sales-director-monthly-dashboard-spec.md`. Read the handoff first, then brainstorm Phase 2 scope before writing any code.
