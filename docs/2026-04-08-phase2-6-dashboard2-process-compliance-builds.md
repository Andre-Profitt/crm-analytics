# Phase 2.6 - Dashboard 2 Process Compliance Builds + ph_aging_pipeline_365_plus Fix

> Tight scope doc for Phase 2.6 of the Sales Director Monthly workstream. Combines design + plan in one file (Andre's autonomy directive: skip the formal brainstorm + writing-plans cycle for non-load-bearing creative work). Picks up after Phase 2.5 B-core (commit `e9caec9`). The scoped subset of full Phase 2.5 that builds the 5 process compliance widgets for Dashboard 2 plus a small adjacent fix.

## One sentence

Build 5 new SF reports for Dashboard 2's process compliance KPI section via `POST /analytics/reports`, add them as new components to Dashboard 2 via dashboard PATCH, fix the `ph_aging_pipeline_365_plus` AMOUNT-vs-ARR defect via report PATCH (same pattern as Phase 1.5 / Phase 2.5 B-core Fix 1a), then re-run the audit and commit.

## Scope locked from brainstorm

- **5 new SF reports for the process compliance KPI section** of Report 2 (`pc_*` widgets in the Report 2 spec). All v1 use **count-of-non-compliant** shape (Option A from the brainstorm) - simpler than ratio metrics and matches the existing CRM data quality widget convention on Dashboard 2. Ratio variants are deferred to a future phase.
- **5 new dashboard component PATCHes** to add the new reports to Dashboard 2 - same `clone_existing_component_for_new_widget` pattern as Phase 1.5 Cell 5, with all the in-flight fixes from Phase 1.5 baked in (reset aggregates/groupings/filterColumns to [], strip owner/runningUser/folderName, populate empty aggregates from the new report's actual aggregate).
- **`ph_aging_pipeline_365_plus` AMOUNT-to-ARR fix** - small Dashboard 2 cleanup using the validated B-core Fix 1 pattern (swap aggregate, add to detailColumns, use `.CONVERT` suffix per Dashboard 2 convention).

## Out of scope (deferred)

- **9 new Dashboard 1 missing widgets** (Report 1 spec gaps) - bigger phase, deserves its own focused session after the new-report-creation pattern is validated by Phase 2.6.
- **2 new PI list views** for `fa_forecast_change_volatility` and `fa_slipped_count_quarterly` - manual SF Lightning UI work.
- **WIP completion** for `ph_probability_mismatch_by_stage` - needs Sales Ops threshold decision.
- **Retire-vs-repurpose** for `dq_missing_quote_type` - needs product decision.
- **`ph_overdue_opportunities` fiscal-quarter grouping** - needs bucket field / custom formula approach.
- **Ratio-metric upgrade** for the 5 process compliance reports - v2 enhancement requiring custom summary formulas.
- **Sales Director notification** about cumulative visible changes - external, manual.

## Goal

Report 2 has all 5 KPI sections covered:

- CRM data quality (5 widgets, all OK from prior phases)
- **Process compliance (5 NEW widgets, this phase)**
- Forecast accuracy (4 widgets - PI deferred)
- Pipeline hygiene (8 widgets, mostly clean post-B-core; `ph_aging_pipeline_365_plus` cleaned this phase)

Dashboard 2 audit shows substantial WRONG-DATA + BLOCKING reduction.

## The 5 new reports (filter shapes per Option A)

| #   | Widget ID                          | Filter                                                                                                                                                                                          | Group by                     | Aggregate | Display title                                            |
| --- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- | --------- | -------------------------------------------------------- |
| 1   | `pc_next_step_documented`          | `IsClosed=false AND StageName IN ('3 - Engagement','4 - Shortlisted','5 - Preferred','6 - Contracting') AND NextStep IS NULL`                                                                   | `Sales_Region__c`            | RowCount  | "Mid-stage opportunities lacking NextStep"               |
| 2   | `pc_land_commercial_approval_flow` | `Type='Land' AND IsClosed=false AND StageName IN ('3 - Engagement','4 - Shortlisted','5 - Preferred','6 - Contracting') AND Stage_20_Approval__c=false AND Submit_for_Stage_20_Review__c=false` | `Sales_Region__c`            | RowCount  | "Land deals lacking commercial approval flow"            |
| 3   | `pc_recent_activity_logged`        | `IsClosed=false AND StageName IN ('2 - Discovery','3 - Engagement','4 - Shortlisted','5 - Preferred','6 - Contracting') AND (LastActivityDate < TODAY-30 OR LastActivityDate IS NULL)`          | `Sales_Region__c`            | RowCount  | "Active opportunities with no activity in last 30 days"  |
| 4   | `pc_won_loss_reason_documented`    | `IsClosed=true AND CloseDate IN THIS_QUARTER AND Reason_Won_Lost__c IS NULL`                                                                                                                    | `Sales_Region__c`            | RowCount  | "Closed deals this quarter lacking won/loss reason"      |
| 5   | `pc_stage_age_within_threshold`    | `IsClosed=false AND StageName IN ('3 - Engagement','4 - Shortlisted','5 - Preferred','6 - Contracting') AND LastStageChangeDate < TODAY-60`                                                     | `Sales_Region__c, StageName` | RowCount  | "Mid-stage opportunities exceeding 60-day age threshold" |

All 5 share:

- `reportType: "Opportunity"` (or whichever API name the org uses for the standard Opportunity report type - probe verifies)
- `reportFormat: "SUMMARY"`
- `aggregates: ["RowCount"]`
- `developerName: "Phase_2_6_<short_name>"`
- Folder ID: probed from an existing Dashboard 2 report (target the same folder Sales Ops reports already live in)

## The 1 fix

`ph_aging_pipeline_365_plus` (`00OTb000008Ti7VMAS`) — same pattern as Phase 2.5 B-core Fix 1a:

- Swap `aggregates[0]` from `s!AMOUNT` to `s!Opportunity.APTS_Opportunity_ARR__c.CONVERT`
- Add `Opportunity.APTS_Opportunity_ARR__c.CONVERT` to `detailColumns`
- PATCH + inline verify
- Plus possibly a fiscal date filter swap if the audit shows that defect too (probe will check)

## Architecture

Two new uncommitted scripts:

- **`scripts/phase2_6_post_probe.py`** — pre-flight probe. Auths, fetches one existing Dashboard 2 report (e.g., `00OTb000008el0PMAQ` "Missing Decision Reason") to extract folder ID + report type API name + filter operator conventions. Then attempts a `POST /analytics/reports` with the simplest possible body cloned from that template (modifying only name + developerName) to validate the POST endpoint works. Saves the test report ID for optional cleanup. Writes confirmed metadata to `/tmp/phase2_6_probe/confirmed.json`.
- **`scripts/phase2_6_build.py`** — main build script. Notebook-style cells: auth, backup (Dashboard 2 + the 1 fix target report + 1 reference report for the clone template), build (5 POSTs for the new reports), fix (1 PATCH for ph_aging_pipeline_365_plus), dashboard (1 dashboard PATCH adding 5 components), summary. `--dry-run` support. Inline assert tests in each cell.

The build script bakes in all Phase 1.5 lessons:

- `wrapped_full` body shape (probed, but expected to match Dashboard 2 convention)
- Aggregate fields must be in detailColumns
- ARR uses `.CONVERT` suffix on Dashboard 2 (per Phase 2.5 B-core probe)
- Calendar date tokens use bare form (THIS_YEAR, etc.) - not relevant to this phase since none of the 5 new reports use date filters except #4 which uses `THIS_QUARTER`
- Dashboard PATCH must reset cloned component's aggregates/groupings/filterColumns to [], populate from new report's actual metadata, strip owner/runningUser/folderName/etc

## Pre-flight probe steps

1. Auth via `sf org display`.
2. GET `00OTb000008el0PMAQ` describe. Extract: `reportType`, `folderId`, sample filter shapes, sample column naming convention (e.g., is it `OPPORTUNITY.NEXT_STEP` or `NextStep` or `Opportunity.NextStep`?).
3. Build a minimal test report `reportMetadata` from the template, modifying only `name` and `developerName` to something obviously test-tagged ("Phase 2.6 POST Probe Test").
4. POST `/analytics/reports` with the test body. Verify 2xx response with a new Id.
5. GET the new report's describe to verify it landed. Save the test report ID to `/tmp/phase2_6_probe/test_report_id.txt`.
6. (Optional) DELETE the test report via `DELETE /analytics/reports/{id}` to keep the org clean. If DELETE fails, leave a note for manual cleanup.
7. Save all confirmed metadata to `/tmp/phase2_6_probe/confirmed.json` for the build script.

If POST fails: try progressively-refined body shapes (similar to Phase 1.5's PATCH shape probe). Up to 5 retries. STOP and escalate if all 5 fail.

## Build script execution flow

1. **Cell 1: Auth** - same pattern as prior phases.
2. **Cell 2: Backup** - GET Dashboard 2, ph_aging_pipeline_365_plus, and 00OTb000008el0PMAQ (template). Write to `/tmp/phase2_6_backup/`. Write `rollback_one.sh` helper.
3. **Cell 3: Build 5 new reports** - for each of the 5 widgets, construct the reportMetadata body from the template + spec filter shape, POST, verify the new report ID is returned, save the new ID to a list. If any POST fails, log + continue. Inline assert tests on the metadata-construction helpers.
4. **Cell 4: Fix ph_aging_pipeline_365_plus** - read the backup, swap aggregate + add to detailColumns, PATCH, inline verify. Same as B-core Fix 1.
5. **Cell 5: Dashboard component additions** - read Dashboard 2 backup, clone an existing component for each of the 5 new reports, modify reportId/header/title/id, RESET aggregates/groupings/filterColumns to [], populate aggregates from each new report's actual metadata, append to components array, PATCH the dashboard with read-only fields stripped. Same pattern as Phase 1.5 Cell 5 + its in-flight fixes.
6. **Cell 6: Summary** - print counts of POSTs, fixes, dashboard component additions, failures.

`--dry-run` mode skips all POST/PATCH operations and prints what they WOULD send.

## Acceptance criteria

1. POST shape probe completed and confirmed POST works on this org's reports endpoint.
2. Probe target test report exists in the org (or was successfully DELETEd).
3. 5 new SF reports created via POST. Each has a unique ID and is verifiable via GET describe.
4. `ph_aging_pipeline_365_plus` patched and inline-verified: aggregate is ARR, detailColumns includes the field.
5. Dashboard 2 has 5 new components, each referencing one of the 5 new reports.
6. Audit re-run produces `docs/audits/2026-04-08-sales-ops-quarterly-audit.md` (overwriting the Phase 2.5 B-core commit `e9caec9`).
7. Post-Phase-2.6 audit tally is strictly better than `e9caec9`:
   - WRONG-DATA drops by 1 (the ph_aging_pipeline_365_plus fix) or more.
   - BLOCKING drops by 5 (the 5 new spec widgets are no longer MISSING).
   - OK count increases correspondingly.
8. New audit committed by exact path. All 3 scripts (audit + 2 phase2_6 scripts) stay uncommitted.
9. No em-dashes in any committed file.

## Risks

1. **POST shape unknown.** Phase 1.5 hit 5 bugs on PATCH. POST is a different surface area. Mitigation: pre-flight probe with retries + simplest possible body first.
2. **Folder permissions.** New reports need a folder with write access. The probe extracts the folder ID from an existing Dashboard 2 report - that folder is known to be writable (since the existing report lives there).
3. **Filter shape conventions.** Spec filters are conceptual ("`NextStep IS NULL`"); the Reports API uses specific operator names and column refs. Probe extracts the convention from an existing report's filters.
4. **`pc_recent_activity_logged` filter complexity.** The OR clause (`LastActivityDate < TODAY-30 OR LastActivityDate IS NULL`) needs `reportBooleanFilter` to combine the conditions correctly. May need experimentation. Worst case: simplify to just `LastActivityDate IS NULL` for v1 (still useful, slightly weaker signal).
5. **`pc_stage_age_within_threshold` simplification.** Spec wanted per-stage thresholds (3 = 60d, 4 = 45d, 5 = 30d, 6 = 15d). v1 uses a single 60-day threshold across all 4 stages. Per-stage thresholds require either a custom field/bucket or a separate report per stage. Simplification noted in the report's display title.
6. **Dashboard component clone bugs.** Phase 1.5 Cell 5 hit 3 bugs in-flight; those fixes are pre-applied here.

## File paths

Inputs (read-only, already committed):

- `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`)
- `docs/specs/report-2-source-contract.md` (commit `aa8e69a`)
- `docs/audits/2026-04-08-sales-ops-quarterly-audit.md` (commit `e9caec9`, pre-Phase-2.6 state)

Net new committed by exact path:

- `docs/2026-04-08-phase2-6-dashboard2-process-compliance-builds.md` (this file)
- `docs/audits/2026-04-08-sales-ops-quarterly-audit.md` (post-Phase-2.6, OVERWRITES `e9caec9` since both are same-day)

Net new and uncommitted (working tree only):

- `scripts/phase2_6_post_probe.py`
- `scripts/phase2_6_build.py`
- `/tmp/phase2_6_probe/*.json`
- `/tmp/phase2_6_backup/reports/*.json`
- `/tmp/phase2_6_backup/dashboards/01ZTb00000FSP9JMAX.json`
- `/tmp/phase2_6_backup/rollback_one.sh`

## Wall time estimate

~60-90 minutes if POST works on the first try. ~2 hours if iterative fix loops are needed (Phase 1.5 took ~25 minutes for 6 fix iterations on PATCH). The probe step de-risks the POST surface by ~half before the main script runs.
