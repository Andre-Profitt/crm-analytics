# Sales Director Monthly - Phase 2 SF Reports + Pipeline Inspection Design

> Design doc for the rebooted Phase 2 of the Sales Director Monthly workstream. Replaces the superseded CRMA-coverage design at `docs/2026-04-07-sales-director-monthly-phase2-audit-design.md`. This is the output of a rediscussion brainstorm on 2026-04-07 in which Andre clarified that the workstream should source from standard Salesforce Reports + Dashboards + Pipeline Inspection (turnkey, standardized) rather than from CRMA (bespoke, drift-prone).

## One sentence

Run the existing Phase 1 audit script against **both** feeder dashboards (`01ZTb00000FSP7hMAH` for Report 1, `01ZTb00000FSP9JMAX` for Report 2), grade each against its respective spec (the amended Report 1 spec at commit `8c81d2d`, the new Report 2 spec at commit `25cc03d`), commit two delta reports, then write two source-of-truth contracts that map each spec widget to a canonical SF report ID or Pipeline Inspection list view, ready for Phase 4 deck rebuild via the Analytics REST API plus PI screenshots / link-outs.

## Why this design replaces the CRMA approach

The superseded CRMA design (`dc418b9`) was abandoned within the same session for four reasons, captured in the SUPERSEDED header on that file and recapped here for context:

1. **Direction change.** Andre clarified that the canonical analytics surface for both reports is **Salesforce Dashboards + Reports + Pipeline Inspection**, not CRMA. The reasoning: "turnkey standardization, layer of protection." Pipeline Inspection native gives metric definitions that don't drift; the in-house Forecast_Intelligence CRMA dashboard is a bespoke mirror that requires hand-maintenance to stay aligned with PI semantics.
2. **Scope expansion to two reports.** The work is now scoped as TWO PowerPoint deliverables, not one. Report 1 (Pipeline Reporting & Insights, monthly, Sales Directors) AND Report 2 (Sales Ops Quarterly Report, quarterly, Sales Ops). The CRMA design only contemplated one report.
3. **Handbook anchoring.** The SimCorp Sales Handbook V4 (slide 22) explicitly lists "Reports & Dashboards" as the canonical analytics surface provided by Customer & Commercial Insights. CRMA is not mentioned. The pivot puts the workstream back on the handbook-canonical track.
4. **A real bug found in the CRMA cell 4 design** by a live-org probe. Saql-typed CRMA steps return `datasets: None` at the step level (only aggregateflex filter steps carry datasets); the dataset name lives only inside the SAQL string as `q = load "Name"`. The CRMA plan's `extract_crma_widgets` would have read `step["datasets"][0]["name"]` and gotten `None` for every saql widget. The bug never shipped because none of the 13 tasks in the CRMA plan were executed.

The rebooted Phase 2 avoids all four issues: it audits SF Reports (which Phase 1's audit script already knows how to grade), it covers both reports in one session, it anchors to the handbook directly via the spec source-of-truth references sections, and it does not need any new dashboard-state walkers because SF Dashboard describe via `/analytics/dashboards/{id}/describe` returns a flat component list without the saql/aggregateflex distinction.

## Goal

Produce **two committed source-of-truth contracts** that the Phase 4 deck rebuild reads to decide, for each spec widget, which canonical SF report (or PI list view) feeds the corresponding deck slide:

- `docs/specs/report-1-source-contract.md` - one row per Report 1 spec widget (16 widgets), each mapped to a canonical SF report ID or PI metric config plus a verification status (filter shape matches spec, value sample matches expectation, etc.).
- `docs/specs/report-2-source-contract.md` - one row per Report 2 spec widget (22 widgets), same shape.

Plus **two committed delta reports** (`docs/audits/2026-04-07-sales-director-monthly-audit.md` re-run, and `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` net-new) that grade each feeder dashboard against its respective spec. The audits surface defects (missing widgets, wrong fields, fiscal-vs-calendar violations) and gaps (spec widgets the dashboard doesn't have yet) that the source contract needs to address.

## Non-goals

- Not a CRMA dashboard rebuild. Not a CRMA audit. The 9 B2B_MA dashboards are out of scope. The Forecast_Intelligence dashboard is out of scope.
- Not a deck rebuild. Phase 4 (separate session) consumes the two source contracts to produce the actual PowerPoint files.
- Not a Pipeline Inspection configuration change. Phase 2 reads PI list view configs to identify which view to point at; it does not modify any PI metric config or add list views.
- Not a finance feed integration for churn risk. That's a separate Alex P outreach action item.
- Not a fix-it pass for the defects surfaced. Phase 2.5 (separate task list) handles fixes to bad widgets on Dashboard 1 and Dashboard 2. Phase 1.5 (the 30-min hotfix on Dashboard 1's renewal AMOUNT-vs-ACV widgets) is now blocking, not optional - see Phase 2.5 implications below.

## Constraints (non-negotiable)

From `crm-analytics/CLAUDE.md` and the Phase 1 hard rules, plus three new ones from this rediscussion:

1. CLI-first. `sf` CLI for auth, `requests` for API calls. No MCP. No `build_*.py`. No `.env`.
2. Auth via `sf org display --target-org apro@simcorp.com --json`.
3. API version `v66.0`.
4. Calendar year only. Fiscal date filters are defects.
5. Renewals use ACV (`APTS_Renewal_ACV__c`); land/expand uses ARR (`APTS_Opportunity_ARR__c`).
6. Type field is canonical for renewal/land/expand; `APTS_Primary_Quote_Type__c` is empty.
7. No em-dashes anywhere in widget labels or output.
8. Stage exact paths only. Never `git add .` / `-A` / `-u`.
9. Audit script stays uncommitted (matches `scripts/audit_*.py` convention).
10. Audit output and source contracts committed by exact path.
11. **(NEW)** Pipeline Inspection native is the canonical source for forecast accuracy and slipped deals widgets. No in-house mirrors.
12. **(NEW)** Sales Handbook V4 is the canonical source for business rules: stage names, commercial approval gates, ARR-vs-ACV, exit gate definitions, role accountability for forecast accuracy.
13. **(NEW)** Both spec files (Report 1, Report 2) include a Source-of-truth references section citing the handbook + the canonical SF reports + PI native. The audit + source contract grade against the latest committed spec hash.

## Architecture

The Phase 1 audit script at `scripts/audit_sales_director_monthly_dashboard.py` already knows how to:

- Auth via `sf org display`
- Assert picklist freshness (`APTS_Primary_Quote_Type__c` is empty)
- Load expected spec from a markdown table
- Fetch dashboard describe via `/analytics/dashboards/{id}/describe`
- Fetch report describe via `/analytics/reports/{id}/describe` (cell 5, which we KEEP this time - SF reports actually have describe endpoints that return filter + grouping + aggregate metadata, unlike CRMA steps)
- Run reports synchronously via `/analytics/reports/{id}?includeDetails=true`
- Apply 8 static rules grading filter / aggregation / format / title shape
- Compare bidirectionally against the spec via stem matcher
- Render a two-section markdown delta report (executive summary + full appendix)
- Compose end-to-end and write the audit file

For the rebooted Phase 2, the script needs **three small parameterization changes** so it can run against either feeder dashboard with either spec:

1. `DASHBOARD_ID` constant becomes `--dashboard-id` argv (or env var) input.
2. `SPEC_PATH` constant becomes `--spec-path` argv (or env var) input.
3. `AUDIT_OUTPUT_FILENAME` derives from a `--output-name` argv (or env var) input.

That is the entire code change to the Phase 1 script. Cells 1-10 stay structurally identical. The static rule scan, the matcher, the bidirectional compare, the markdown render - all reused verbatim. The 8 static rules in cell 7 already handle the defects this audit needs to surface (fiscal date filters, renewal-uses-AMOUNT, pipeline-uses-AMOUNT, em-dashes, fiscal in title, etc.). No new rules needed for Report 1; one new rule may be needed for Report 2's Section 3 (forecast accuracy widgets) to flag any widget not sourced from PI - but since PI sources don't exist as SF report IDs in the same way, this can be handled in the source contract generation instead of the audit.

After both audits run, the **source contract generation** is a separate, smaller task that does NOT live in the audit script. For each spec widget:

1. If the audit's match found a corresponding live widget on the dashboard with `severity='OK'` or `severity='COSMETIC'`, the source contract row pins the live widget's `report_id` as the canonical source.
2. If the audit's match found a corresponding live widget but flagged it `BLOCKING` or `WRONG-DATA`, the source contract row pins the same `report_id` but marks it as `needs_phase_2_5_fix` and references the audit row for the specific defect.
3. If the audit found no match (the spec widget is `(MISSING)` from the dashboard), the source contract row leaves the report ID blank and marks it as `needs_new_report` with a reference to the spec row and the canonical filter shape.
4. For widgets in Report 2's Section 3 (forecast accuracy, all PI native), the source contract row pins a `pipeline_inspection_list_view_id` instead of a SF report ID. Phase 2 probes `PipelineInspectionListView` for active configurations and pins the right one per widget.

The source contract is written as a markdown table with one row per spec widget plus a header section explaining the schema. It is committed by exact path.

## Output

Four committed files (two pairs, one pair per report):

1. `docs/audits/2026-04-07-sales-director-monthly-audit.md` (re-run of the Phase 1 audit against the **amended** Report 1 spec, expecting NEW findings for the 2 added widgets - the audit will say `MISSING: commercial_approval_approved_ytd` and `MISSING: forecast_accuracy_snapshot`, both BLOCKING).
2. `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` (net-new audit of `01ZTb00000FSP9JMAX` against the Report 2 spec, expecting 9 BLOCKING `MISSING` entries for the unbuilt process compliance + forecast accuracy widgets, plus 2 WRONG-DATA defects on the existing pipeline hygiene widgets).
3. `docs/specs/report-1-source-contract.md` - Report 1 source-of-truth contract, 16 rows (one per spec widget), each mapping to a canonical SF report ID or PI list view.
4. `docs/specs/report-2-source-contract.md` - Report 2 source-of-truth contract, 22 rows.

Plus a small uncommitted change to `scripts/audit_sales_director_monthly_dashboard.py` to parameterize its constants (3 lines edited).

## Pipeline Inspection probing strategy

For Report 1 widget 16 (`forecast_accuracy_snapshot`) and Report 2 widgets 11-14 (forecast accuracy section), the source contract pins a Pipeline Inspection list view ID. Phase 2 probes:

- `GET /services/data/v66.0/sobjects/PipelineInspectionListView` (or the equivalent tooling endpoint) - lists all active list views in the org
- For each active list view, inspect its filter / grouping / metric config to identify which one matches each spec widget's intent
- For widget 11 (`fa_quarterly_realized_vs_commit`): look for a list view with quarterly time range + commit forecast category roll-up
- For widget 12 (`fa_quarterly_realized_vs_bestcase`): same but best-case category
- For widget 13 (`fa_forecast_change_volatility`): look for a Pipeline Changes view with trailing 6-month time range
- For widget 14 (`fa_slipped_count_quarterly`): look for a Pipeline Changes view with the "Slipped" change-type filter

The probe results land in the source contract. If no PI list view matches a spec widget's intent, the source contract row marks it as `needs_pi_config_change` and Phase 2.5 handles configuring the right list view in the SF Lightning UI (manual one-time setup, not scriptable via REST).

## Acceptance criteria

The rebooted Phase 2 work is done when all of the following are true:

1. The Phase 1 audit script is parameterized to take dashboard ID, spec path, and output name via argv or env. Inline tests still pass.
2. Both audits run end-to-end against the live org without manual intervention beyond `sf` CLI auth.
3. `docs/audits/2026-04-07-sales-director-monthly-audit.md` is committed by exact path. The Re-run version supersedes the Phase 1 version (commit `b09f423`) with the amended spec applied. New BLOCKING entries for widgets 6 and 16 are present.
4. `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` is committed by exact path. 9 MISSING + 2 WRONG-DATA + WIP + open-question entries match expectations.
5. `docs/specs/report-1-source-contract.md` is committed by exact path with 16 rows.
6. `docs/specs/report-2-source-contract.md` is committed by exact path with 22 rows.
7. PI list view IDs are pinned for all 5 forecast-accuracy widgets across both reports (Report 1 widget 16 + Report 2 widgets 11-14).
8. Audit script stays uncommitted; audit outputs and source contracts are committed by exact path.
9. No em-dashes anywhere in any committed file.
10. Both commit message footers cite the spec commit hashes (Report 1: `8c81d2d`, Report 2: `25cc03d`) and the source-of-truth references in each spec.

## Phase 1.5, 2.5, and 4 implications

- **Phase 1.5 is now blocking, not optional.** The Phase 1 done-handoff treated the 30-min hotfix on Dashboard 1 (3 renewal AMOUNT-vs-ACV widgets, 13 fiscal date filters) as deferred-and-optional under the assumption that we were going CRMA-first. With the pivot back to SF native, Dashboard 1 is the canonical Report 1 feeder, so the hotfix is required before Report 1 ships. Phase 1.5 should run AFTER Phase 2's audit confirms the defect set (the audit will re-validate the same defects the Phase 1 audit found, plus the 2 NEW BLOCKING entries from the spec amendment). Phase 1.5 = patch the 3 renewal widgets to use `APTS_Renewal_ACV__c`, swap fiscal date filters for calendar, build the 2 new widgets (`commercial_approval_approved_ytd` and `forecast_accuracy_snapshot`).
- **Phase 2.5 covers Dashboard 2 fixes.** From the Report 2 audit: build the 9 missing widgets (5 process compliance + 4 forecast accuracy), fix the `s!AMOUNT` defect on `ph_no_activity_30_plus`, fix the `FISCAL_QUARTER` defect on `ph_overdue_opportunities`, and resolve open question 2 (retire `dq_missing_quote_type` or repurpose for the canonical `Type` field).
- **Phase 4 (deck rebuild) consumes both source contracts.** For each spec widget in Report 1 (or Report 2), Phase 4 reads the source contract, fetches the canonical SF report via `/analytics/reports/{id}?includeDetails=true` (same Analytics REST endpoint Phase 1 cell 6 already uses), and renders the result as a native PowerPoint chart. For PI-sourced widgets, Phase 4 either embeds a PI screenshot, generates a PI deep-link, or queries the underlying objects (`Opportunity`, `OpportunityHistory`, `OpportunityFieldHistory`) directly to recompute the PI metric in-deck. The deck-rebuild approach choice (screenshot vs link vs SOQL recompute) is itself an open question for Phase 4 brainstorming.
- **Open question follow-ups.** Each spec has unresolved open questions tagged for Phase 2 / Phase 2.5 / Phase 3. The audit + source contract work surfaces but does not resolve them; resolutions require Sales Ops sign-off (e.g. stage-specific aging thresholds, MEA-vs-APAC slide grouping, churn risk Finance feed source identification via Alex P outreach).

## File paths

Inputs (read-only, already committed):

- `docs/specs/sales-director-monthly-dashboard-spec.md` (Report 1 spec, commit `8c81d2d`, 16 widgets)
- `docs/specs/sales-ops-quarterly-dashboard-spec.md` (Report 2 spec, commit `25cc03d`, 22 widgets)
- `docs/2026-04-06-sales-director-monthly-phase1-done-handoff.md` (Phase 1 closeout context)
- `/Users/test/Downloads/Sales Handbook V4.pptx` (handbook source)
- `scripts/audit_sales_director_monthly_dashboard.py` (Phase 1 audit script, uncommitted, ~1369 lines)

Modified (uncommitted):

- `scripts/audit_sales_director_monthly_dashboard.py` - 3-line parameterization change (constants -> argv)

Net new and committed by exact path:

- `docs/audits/2026-04-07-sales-director-monthly-audit.md` (Report 1 audit re-run, supersedes Phase 1 commit `b09f423`)
- `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` (Report 2 audit, net new)
- `docs/specs/report-1-source-contract.md` (Report 1 source-of-truth contract, net new)
- `docs/specs/report-2-source-contract.md` (Report 2 source-of-truth contract, net new)
- `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-design.md` (this file)
- `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-plan.md` (writing-plans output, lands next)

Superseded (kept for provenance):

- `docs/2026-04-07-sales-director-monthly-phase2-audit-design.md` (CRMA design, commit `dc418b9`, marked superseded in commit `46fcabf`)
- `docs/2026-04-07-sales-director-monthly-phase2-audit-plan.md` (CRMA plan, commit `31c15ea`, marked superseded in commit `46fcabf`)

## Handoff to writing-plans

After this design doc lands and the user reviews it, the next step is to invoke `superpowers:writing-plans` and produce a bite-sized task plan at `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-plan.md` with cell-level tasks for: parameterizing the Phase 1 audit script, running it against both dashboards, drafting the two source contracts, probing Pipeline Inspection list views, and committing each output by exact path. The plan should slice work along the same boundaries as the Phase 1 plan (one task per parameterization step, one task per audit run, one task per source contract, one task per PI probe, one task per commit).
