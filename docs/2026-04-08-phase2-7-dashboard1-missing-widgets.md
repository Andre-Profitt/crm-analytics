# Phase 2.7 - Dashboard 1 Missing Widgets (9 New SF Reports)

> Tight scope doc for Phase 2.7. Builds the 9 missing Report 1 spec widgets on Dashboard 1 via POST /analytics/reports. Uses the POST pattern validated by Phase 2.6 (commit `d4d8b63`). Probe confirmed Dashboard 1 shares folder + `.CONVERT` ARR convention with Dashboard 2; an earlier "no `.CONVERT`" working note was wrong. Same combined design+plan pattern as Phase 2.6 (skipping the formal brainstorm cycle per Andre's autonomy directive).

## One sentence

Build 9 new SF reports covering the Report 1 spec gaps (4 pipeline overviews, 1 commercial approval global, 2 per-region Land Stage 3 candidate tables, 2 renewal widgets) via POST /analytics/reports, add them as new components on Dashboard 1, re-run the audit, and commit the strictly-better tally.

## Scope

9 new SF reports. All SUMMARY format. All grouped by Opportunity report type. Dashboard 1 uses the `.CONVERT` ARR form `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` (same as Dashboard 2, per Phase 2.7 probe finding on 2026-04-08 - empirical scan of Dashboard 1 reports found 3/4 ARR reports use `.CONVERT`, 1 bare, 1 mixed; majority + Dashboard 2 parity is the correct Phase 2.7 choice). Dashboard 1 and Dashboard 2 share folder `005QA000003DUwWYAW`, so the POST path is already validated by Phase 2.6.

| #   | Widget ID                      | Filters (simplified for v1 as needed)                                                                                                                       | Group                              | Aggregate                                                 | Shape                                                               |
| --- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------- |
| 1   | `pipeline_overview_global`     | `IsClosed=false AND CloseDate IN THIS_QUARTER`                                                                                                              | (none)                             | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`           | single metric                                                       |
| 2   | `pipeline_overview_emea`       | `IsClosed=false AND CloseDate IN THIS_QUARTER AND Sales_Region__c IN ('United Kingdom & Ireland','Central Europe','Northern Europe','Southwestern Europe')` | `STAGE_NAME`                       | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`           | stacked bar by stage                                                |
| 3   | `pipeline_overview_nam`        | `IsClosed=false AND CloseDate IN THIS_QUARTER AND Sales_Region__c='North America'`                                                                          | `STAGE_NAME`                       | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`           | stacked bar by stage                                                |
| 4   | `pipeline_overview_apac`       | `IsClosed=false AND CloseDate IN THIS_QUARTER AND Sales_Region__c IN ('APAC','Middle East & Africa')`                                                       | `STAGE_NAME`                       | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`           | stacked bar by stage                                                |
| 5   | `commercial_approval_global`   | `IsClosed=false AND STAGE_NAME not equals '0 - Lost'`                                                                                                       | `Opportunity.Stage_20_Approval__c` | `RowCount`                                                | count approved vs not approved, grouped                             |
| 6   | `land_stage3_no_approval_nam`  | `TYPE='Land' AND STAGE_NAME='3 - Engagement' AND Opportunity.Stage_20_Approval__c='False' AND Sales_Region__c='North America'`                              | (table)                            | `RowCount, s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` | tabular list                                                        |
| 7   | `land_stage3_no_approval_apac` | `TYPE='Land' AND STAGE_NAME='3 - Engagement' AND Opportunity.Stage_20_Approval__c='False' AND Sales_Region__c IN ('APAC','Middle East & Africa')`           | (table)                            | `RowCount, s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` | tabular list                                                        |
| 8   | `renewal_likelihood`           | `TYPE='Renewal' AND IsClosed=false AND CloseDate IN THIS_QUARTER`                                                                                           | `PROBABILITY`                      | `s!Opportunity.APTS_Renewal_ACV__c.CONVERT`               | chart by probability bucket                                         |
| 9   | `renewal_upcoming_list`        | `TYPE='Renewal' AND IsClosed=false AND CloseDate IN THIS_QUARTER`                                                                                           | (table)                            | n/a                                                       | tabular list with Account, Opp Name, ACV, Close, Probability, Owner |

Deferred widgets (not in Phase 2.7 scope):

- `slipped_deals_root_cause` - Pipeline Inspection native per spec hard rule 6; deferred to a phase that handles PI Lightning UI work.
- `slipped_deals_trend` - same as above.
- `forecast_accuracy_snapshot` - already matched to existing dashboard widgets (Phase 1.5 discovery); PI native canonical per spec.
- `churn_risk_placeholder` - blocked on Alex P / Finance feed.

## Goal

Report 1 (Dashboard 1) has all 16 spec widgets either present or pinned to a canonical source. The 9 new widgets add live data to the dashboard so Sales Directors clicking through see the 4 regional pipeline slides, the commercial approval current-state count, the 2 per-region Land Stage 3 candidate tables, and the 2 renewal widgets.

## Non-goals

- Not PI list view configuration (separate phase).
- Not slipped deals widgets (PI native per spec hard rule).
- Not forecast accuracy widget (already matched in Phase 1.5).
- Not churn risk (Finance feed pending).
- Not the 5 Phase 2.6 widget cosmetic cleanup (separate phase).
- Not sales director notification (external, manual).

## Architecture

Two new uncommitted scripts:

- `scripts/phase2_7_probe.py` - Dashboard 1-specific probe. Fetches an existing Dashboard 1 report (e.g., `00OTb000008ekp7MAA` Commercial Approval Candidates by Stage, confirmed OK in Phase 1) to extract folder ID, report type, filter convention, and clone template. Verifies POST works on Dashboard 1's folder. Extracts Dashboard 1's layout shape for the dashboard component PATCH step. Writes `/tmp/phase2_7_probe/confirmed.json`.

- `scripts/phase2_7_build.py` - main build script. 6 cells: auth, backup (Dashboard 1 + the template report), build (9 POSTs), dashboard (1 dashboard PATCH adding 9 components), summary. No report-patch cell (no existing Dashboard 1 defects left post-Phase-1.5 for this scope). `--dry-run` support.

## Dashboard 1 vs Dashboard 2 conventions (CRITICAL)

| Aspect                 | Dashboard 1 (Phase 1.5, 2.7)                                      | Dashboard 2 (Phase 2.5, 2.6)                                      |
| ---------------------- | ----------------------------------------------------------------- | ----------------------------------------------------------------- |
| ARR aggregate form     | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` (with `.CONVERT`) | `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` (with `.CONVERT`) |
| Detail column ARR form | `Opportunity.APTS_Opportunity_ARR__c.CONVERT` (with suffix)       | `Opportunity.APTS_Opportunity_ARR__c.CONVERT` (with suffix)       |
| Calendar date tokens   | `THIS_YEAR`, `THIS_QUARTER` (bare)                                | same (bare)                                                       |
| POST body shape        | `wrapped_full` with read-only fields stripped                     | same                                                              |
| Folder ID              | `005QA000003DUwWYAW` (same as Dashboard 2, probe-confirmed)       | `005QA000003DUwWYAW` (Andre's personal)                           |

The Phase 2.7 probe confirmed empirically that Dashboard 1 and Dashboard 2 share conventions. The earlier Phase 1.5 scope claim of a "no `.CONVERT`" rule was inverted in my working notes; the actual Phase 1.5 build script (`scripts/phase1_5_patch_dashboard1.py`) uses `.CONVERT` form (see lines 851, 952, 1596 of the Phase 1.5 plan). Phase 2.7 uses `.CONVERT` for both ARR and ACV aggregates on all applicable widgets.

## Pre-flight probe goals

1. Extract Dashboard 1 clone template from an existing OK widget. Candidates: `00OTb000008ekp7MAA` (Commercial Approval Candidates by Stage) or `00OTb000008TZsDMAW` (Forecast Accuracy after Phase 1.5 fix).
2. Extract folder ID, report type, filter convention, column naming.
3. Confirm POST works on Dashboard 1's target folder with a minimal test report (delete after).
4. Capture Dashboard 1's layout shape for the dashboard component PATCH (layout.components positional grid? Same as Dashboard 2 from Phase 2.6?).
5. Confirm ARR aggregate form empirically by scanning Dashboard 1 component reports.

## The 9 new reports - filter + metadata construction

Each new report inherits the template's baseline metadata and overrides:

- `name`: "P2.7 <short>" format (40-char limit)
- `developerName`: "Phase*2_7*<short_name>"
- `reportMetadata.reportFilters`: widget-specific, per the table above
- `reportMetadata.standardDateFilter`: widget-specific (most use `CLOSE_DATE`, `THIS_QUARTER`)
- `reportMetadata.groupingsDown`: per the Group column in the table
- `reportMetadata.aggregates`: per the Aggregate column, WITH `.CONVERT` on ARR and ACV
- `reportMetadata.detailColumns`: minimal set for the shape
- `reportMetadata.reportFormat`: `SUMMARY` for all except widgets 6, 7, 9 which may need `TABULAR` for list views

Critical: for widgets 1-4 (pipeline overviews), the aggregate is `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` AND `Opportunity.APTS_Opportunity_ARR__c.CONVERT` must appear in detailColumns per the Salesforce Reports API constraint from Phase 1.5.

For widget 8 (`renewal_likelihood`), the aggregate is `s!Opportunity.APTS_Renewal_ACV__c.CONVERT` AND `Opportunity.APTS_Renewal_ACV__c.CONVERT` must be in detailColumns.

For widgets 6, 7 (`land_stage3_no_approval_*`), the aggregate list is `[RowCount, s!Opportunity.APTS_Opportunity_ARR__c.CONVERT]` per the Phase 2 spec amendment (show both count and ARR sum).

## Dashboard component additions

For each new report, clone an existing Dashboard 1 SUMMARY-report component (same pattern as Phase 1.5 Cell 5 and Phase 2.6 Cell 5, with all in-flight fixes baked in):

- Reset `properties.aggregates` / `properties.groupings` / `properties.filterColumns` to `[]`
- Populate `properties.aggregates` with the new report's aggregate shape
- Populate `properties.groupings` with the new report's grouping shape
- Sync `visualizationProperties.tableColumns` if Dashboard 1 uses that pattern
- Extend `layout.components` positional grid only if present (probe found Dashboard 1 has empty layout grid, so this step is likely a no-op; verify empirically)
- Strip read-only dashboard fields: `id`, `createdDate`, `lastModifiedDate`, `lastAccessedDate`, `url`, `owner`, `runningUser`, `folderName`

## Acceptance criteria

1. Probe completed, confirmed template + folder ID + Dashboard 1 convention.
2. 9 new SF reports created via POST. Each verified via GET describe.
3. 9 new components added to Dashboard 1 via dashboard PATCH.
4. Re-run Phase 1 audit against Dashboard 1 with the amended Report 1 spec. Output: `docs/audits/2026-04-08-sales-director-monthly-audit.md` (overwrites Phase 1.5 audit at commit `d6476b8`).
5. Post-Phase-2.7 audit tally strictly better than `d6476b8`:
   - BLOCKING drops by 9 (the 9 spec widgets are no longer MISSING) OR the matcher fails to bridge the vocabulary gap and the tally shows unchanged BLOCKING but the source contract amendment pins the canonical mapping (same fallback pattern as Phase 2.6).
6. Audit committed by exact path. Report 1 source contract amended to pin the 9 new report IDs. All 3 scripts (audit + 2 phase2_7 scripts) stay uncommitted.
7. No em-dashes in any committed file.

## Risks

1. **Dashboard 1 layout shape** resolved by probe: Dashboard 1 has 17 components but an empty `layout.components` positional grid. Phase 2.7 skips the positional-grid sync step that Phase 2.6 needed for Dashboard 2. Still need to keep `properties.groupings` / `visualizationProperties.tableColumns` in sync per the Phase 2.6 Cell 5 fixes.
2. **Sum aggregates are new to this session.** Phase 2.6 used `RowCount` only. Phase 2.7 needs `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT` for 6 of 9 widgets. Salesforce Reports API rejects sum aggregates if the corresponding field is not a summarizable numeric field OR if the field is not in detailColumns. Both conditions should be satisfied, but the live run may surface API quirks.
3. **Probability bucket grouping** for `renewal_likelihood`. Native SF reports can group by `PROBABILITY` directly (renders as bar per value) but "bucket" grouping requires a bucket field or custom formula. v1 uses direct grouping (each distinct probability value becomes a bar).
4. **Tabular vs summary format** for list widgets (6, 7, 9). Dashboard 1's Land Stage 3 Missing Approval by Region widget from Phase 1 is OK, so there's a working template for TABULAR-format list widgets. Clone from that if possible.
5. **Matcher vocabulary gap** (same as Phase 2.6). The new component titles (e.g., "P2.7 Pipeline EMEA This Quarter") may not match the spec's KPI bullet ("Pipeline overview with quarterly focus") by stem matching. Fall back: amend the Report 1 source contract manually to pin the canonical mapping.
6. **Wall time** could be ~2-3 hours given 9 reports + iterative fix loops (Phase 2.6 took 4 iterations for 5 reports).

## File paths

Inputs (read-only):

- `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`, 16-widget Report 1 spec)
- `docs/audits/2026-04-08-sales-director-monthly-audit.md` (commit `d6476b8`, post-Phase-1.5 state)
- `docs/specs/report-1-source-contract.md` (commit `5ac6d62`)

Net new committed by exact path:

- `docs/2026-04-08-phase2-7-dashboard1-missing-widgets.md` (this file)
- `docs/audits/2026-04-08-sales-director-monthly-audit.md` (post-Phase-2.7, OVERWRITES `d6476b8`)
- `docs/specs/report-1-source-contract.md` (amendment pinning 9 new report IDs)

Net new and uncommitted (working tree only):

- `scripts/phase2_7_probe.py`
- `scripts/phase2_7_build.py`
- `/tmp/phase2_7_probe/*.json`
- `/tmp/phase2_7_backup/dashboards/01ZTb00000FSP7hMAH.json`
- `/tmp/phase2_7_backup/reports/*.json` (template + any reference reports)
- `/tmp/phase2_7_backup/rollback_dashboard.sh`

## Wall time estimate

~2-3 hours including probe + build + iterative fix loops. Phase 2.6 needed 4 POST iterations for 5 reports; Phase 2.7 has 9 reports with more variation so expect similar or slightly more.
