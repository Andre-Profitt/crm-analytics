# Phase 1.5 - Dashboard 1 Hotfix Design

> Design doc for Phase 1.5 of the Sales Director Monthly workstream. Phase 2 is complete (commits `46fcabf` through `aa8e69a`). Phase 1.5 is the first WRITE workflow in the workstream - it PATCHes Dashboard 1's underlying reports to fix the defects the Phase 2 audit surfaced at commit `6cbe8fe`. This doc is the output of a brainstorm session on 2026-04-07 and the input for `superpowers:writing-plans`.

## One sentence

Run a small new uncommitted script at `scripts/phase1_5_patch_dashboard1.py` that backs up every affected report + Dashboard 1 to `/tmp/phase1_5_backup/`, PATCHes 3 renewal reports to swap `Amount` for `APTS_Renewal_ACV__c`, PATCHes ~12-15 Dashboard 1 reports to swap fiscal date filters for calendar, appends a new `commercial_approval_approved_ytd` component to Dashboard 1 referencing the existing canonical report `00OTb000008aTtJMAU`, then re-runs the audit script to overwrite `docs/audits/2026-04-07-sales-director-monthly-audit.md` with a strictly-better post-patch tally.

## Context

Phase 2 committed the following inputs for Phase 1.5:

- **Report 1 spec** at `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`, 16 widgets, handbook-anchored)
- **Report 1 audit** at `docs/audits/2026-04-07-sales-director-monthly-audit.md` (commit `6cbe8fe`, 26 entries: 13 BLOCKING, 10 WRONG-DATA, 1 ORPHAN, 2 OK)
- **Report 1 source contract** at `docs/specs/report-1-source-contract.md` (commit `5ac6d62`, 16 widgets mapped to canonical sources)
- **Phase 2 SF Reports design** at `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-design.md` (commit `c8bdcb7`) which reframed Phase 1.5 from "30-minute optional hotfix" to BLOCKING, because Dashboard 1 is now the canonical Report 1 feeder (the CRMA approach was abandoned).

Phase 1.5 closes the gap between the Phase 1 audit findings and a dashboard that Sales Directors can trust when they click through from the deck. Phase 4 (deck rebuild) can technically ship without Phase 1.5 (the deck pulls from canonical SF reports directly, not from dashboard components), but a broken dashboard creates user trust problems. Phase 1.5 fixes the visible defects so the dashboard and the deck tell the same story.

## Goal

Produce a Dashboard 1 (`01ZTb00000FSP7hMAH`) that passes a post-Phase-1.5 audit with:

- Zero rows tagged with `renewal_uses_amount_not_acv` rule (was 3).
- Zero rows tagged with `fiscal_date_filter` rule on Dashboard 1 reports (was roughly 10).
- The `commercial_approval_approved_ytd` spec widget matched to a new dashboard component with OK severity (was `(MISSING) BLOCKING`).
- The 2 existing forecast accuracy widgets (`00OTb000008TZsDMAW`, `00OTb000008TZaTMAW`) still matched to `forecast_accuracy_snapshot` but now OK (fiscal filter removed).
- All other defects from `6cbe8fe` either resolved or explicitly carried forward to Phase 2.5.

## Non-goals

- **Not Phase 2.5.** Dashboard 2 work is out of scope. The 8 Report 1 spec widgets that still need new SF reports built from scratch (pipeline_overview_emea/nam/apac, land_stage3_no_approval_nam/apac, renewal_likelihood, renewal_upcoming_list, slipped_deals_root_cause, slipped_deals_trend) are out of scope - Phase 2.5 builds them.
- **Not Phase 4.** No deck rendering. No PI integration design. Phase 1.5 only touches Dashboard 1's underlying reports and one dashboard component.
- **Not a dashboard rename.** The original Phase 1 done-handoff included a "rename to Legacy View" step. That was a CRMA-first holdover. Phase 1.5 leaves the dashboard name intact because Dashboard 1 is no longer treated as legacy.
- **Not a PI integration for forecast accuracy.** The spec's hard rule 6 mandates PI native as the canonical source for the deck's forecast accuracy slide, but Phase 1.5 fixes the existing dashboard widgets in place per Option D (brainstorm Q1 answer). The deck (Phase 4) pulls from PI list view `4c2Tb0000003jobIAA`. The dashboard and the deck have different sources for forecast accuracy; drift detection is a Phase 2.6 follow-up if needed.
- **Not the churn risk widget.** Still blocked on Alex P / Finance feed. External.
- **No new SF reports built from scratch.** The only "new" thing Phase 1.5 adds is a dashboard component referencing the existing canonical report `00OTb000008aTtJMAU` (which already exists in Sales Ops's folder and has the correct filter shape).

## Constraints (non-negotiable)

From `crm-analytics/CLAUDE.md`, the Phase 1 hard rules, and this brainstorm's decisions:

1. CLI-first. `sf` CLI for auth, `requests` for API calls. No MCP. No `build_*.py`. No `.env`.
2. Auth via `sf org display --target-org apro@simcorp.com --json`.
3. API version `v66.0`.
4. Calendar year only. Fiscal date filters are defects (the whole point of Phase 1.5 cell 4).
5. Renewals use ACV (`APTS_Renewal_ACV__c`); land/expand uses ARR (`APTS_Opportunity_ARR__c`) (the whole point of Phase 1.5 cell 3).
6. Pre-PATCH backup to `/tmp/phase1_5_backup/` for every affected report and Dashboard 1 (Question 2 answer B).
7. Hybrid verification: inline verification for the 3 ARR patches, batch verification for fiscal swaps via full audit re-run (Question 3 answer C).
8. Patch script stays uncommitted. Backup files stay in /tmp (ephemeral).
9. Audit output re-committed by exact path (overwrites `6cbe8fe`).
10. No em-dashes anywhere.
11. Never push to origin.
12. Stage exact paths only. Never `git add .` / `-A` / `-u`.

## Approach (locked during brainstorm)

Four clarifying questions were answered during the brainstorm:

- **Q1 - Forecast accuracy widget approach:** Option D. Fix the existing 2 dashboard widgets in place (calendar swap). Leave them on the dashboard as Sales Directors' click-through surface. Pin PI list view `4c2Tb0000003jobIAA` as the canonical source in the Report 1 source contract (widget 16). Phase 4 deck pulls from PI.
- **Q2 - Rollback strategy:** Option B. Pre-PATCH `GET` + backup to `/tmp/phase1_5_backup/<kind>/<id>.json` for every affected entity. Keeps backups ephemeral (not committed).
- **Q3 - Verification cadence:** Option C (hybrid). Inline verification for the 3 ARR patches (highest-stakes, silent failure would mean wrong renewal numbers in the deck). Batch verification via full audit re-run for the fiscal filter swaps.
- **Q4 - Scope:** Option B (medium). Defect fixes on existing widgets + 1 dashboard component addition for `commercial_approval_approved_ytd`. The 8 other missing widgets are deferred to Phase 2.5 (where new reports get built alongside Dashboard 2's 5 new process compliance reports).

## Architecture

**New script:** `scripts/phase1_5_patch_dashboard1.py` (uncommitted by convention, same pattern as `audit_*.py`). Notebook-style cells with `# %%` markers. Auth via `sf org display`. Uses `requests` directly against the Analytics REST API. Supports `--dry-run` flag for safety.

**Separate pre-flight script:** `scripts/phase1_5_patch_shape_probe.py` (also uncommitted). Runs FIRST to empirically determine the Analytics REST API PATCH body shape for standard SF reports in this org. Details in the PATCH shape probe section below.

**API endpoints used:**

- `GET /services/data/v66.0/analytics/reports/{id}/describe` - fetch the full `reportMetadata` (already used by Phase 1 audit cell 5).
- `PATCH /services/data/v66.0/analytics/reports/{id}` - replace the full `reportMetadata`. Body shape determined empirically by the pre-flight probe.
- `GET /services/data/v66.0/analytics/dashboards/01ZTb00000FSP7hMAH/describe` - fetch Dashboard 1's metadata with components array (already used by Phase 1 audit cell 4).
- `PATCH /services/data/v66.0/analytics/dashboards/01ZTb00000FSP7hMAH` - replace the full dashboard body with the modified components array.

**PATCH semantics.** Analytics REST API PATCH replaces the full `reportMetadata` for reports (not a sparse update). The round-trip is: `GET` current metadata → modify the specific field in memory → `PATCH` the full modified metadata back. This means the backup file from Question 2 serves two purposes: rollback AND source-of-truth for the PATCH body.

**Cell-by-cell layout of `scripts/phase1_5_patch_dashboard1.py`:**

| Cell | Purpose                                                                                                                                                                                                                                                                                                                                         | Network            | Verification |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ | ------------ |
| 1    | `get_auth()` - shell to `sf org display`                                                                                                                                                                                                                                                                                                        | shell              | -            |
| 2    | `backup_all()` - GET every affected report + Dashboard 1, write JSON to `/tmp/phase1_5_backup/reports/<id>.json` and `/tmp/phase1_5_backup/dashboards/<id>.json`. Also writes `rollback_one.sh` helper alongside the backups.                                                                                                                   | N+1 GETs           | -            |
| 3    | `patch_arr_widgets()` - the 3 renewal report PATCHes with inline verification. For each: read backup, swap `aggregates[0]` from `s!AMOUNT` (or `s!Opportunity.Amount`) to `s!Opportunity.APTS_Renewal_ACV__c.CONVERT`, PATCH, GET again, confirm field changed.                                                                                 | 3 PATCHes + 3 GETs | inline       |
| 4    | `patch_fiscal_filters()` - N report PATCHes for fiscal date filter swaps. For each: read backup, swap `standardDateFilter.durationValue` (e.g., `THIS_FISCAL_YEAR` -> `THIS_CALENDAR_YEAR`), PATCH. No inline verification - trust the HTTP 200.                                                                                                | N PATCHes          | batch        |
| 5    | `add_commercial_approval_widget()` - 1 dashboard PATCH. Read Dashboard 1 backup, clone an existing SUMMARY-report component entry, change `id` (new UUID), `reportId` to `00OTb000008aTtJMAU`, `header` to `Commercial Approval Approved YTD (Land)`, `title` to the spec's description, append to components array, PATCH the whole dashboard. | 1 PATCH            | batch        |
| 6    | `summary()` - print counts of succeeded/failed operations, paths to backup files, next step instructions                                                                                                                                                                                                                                        | -                  | -            |

**Dry-run mode.** `--dry-run` flag. When set: cells 1 and 2 run (auth + backup). Cells 3, 4, 5 build the PATCH body and print the first 300 chars of what they WOULD send, then skip the actual PATCH call. Recommended to run `--dry-run` once before the live run.

**Fiscal date filter swap mapping:**

| From                  | To                      |
| --------------------- | ----------------------- |
| `THIS_FISCAL_YEAR`    | `THIS_CALENDAR_YEAR`    |
| `THIS_FISCAL_QUARTER` | `THIS_CALENDAR_QUARTER` |
| `LAST_FISCAL_YEAR`    | `LAST_CALENDAR_YEAR`    |
| `LAST_FISCAL_QUARTER` | `LAST_CALENDAR_QUARTER` |
| `NEXT_FISCAL_YEAR`    | `NEXT_CALENDAR_YEAR`    |
| `NEXT_FISCAL_QUARTER` | `NEXT_CALENDAR_QUARTER` |

Reports whose `standardDateFilter.durationValue` is `CUSTOM` or unset are untouched.

**Dashboard component clone strategy.** Standard SF dashboard `components` entry shapes vary slightly across org configurations. The safest approach for adding a new component is to copy an existing SUMMARY-report component from Dashboard 1's current components array (for example, one of the existing Commercial Approval widgets), and modify only 4 fields: `id` (new UUID), `reportId` (`00OTb000008aTtJMAU`), `header` (new title), `title` (new description). The rest of the properties (reportFormat, groupings, aggregates, column metadata) are inherited from the clone source, which means the new component renders with similar style to its sibling. Position on the dashboard: appended to the end of `components`. Layout polish is a Phase 2.5 follow-up if needed.

**Error handling per PATCH:**

```python
def patch_report_safe(inst, tok, report_id, new_metadata):
    url = f"{inst}/services/data/v66.0/analytics/reports/{report_id}"
    try:
        r = requests.patch(
            url,
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            json={"reportMetadata": new_metadata},
            timeout=30,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}", "status": None}
    if not r.ok:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:500]}", "status": r.status_code}
    return {"ok": True, "error": None, "status": r.status_code}
```

Each cell maintains a list of failures. Cell 6's summary prints failed IDs with their backup paths. The script does NOT raise on individual PATCH failures - it continues so the operator sees the full picture.

**Rollback helper.** `/tmp/phase1_5_backup/rollback_one.sh` is written by cell 2. Usage: `./rollback_one.sh <report_id>`. It re-fetches auth, reads the backup JSON for that report, and PATCHes the original `reportMetadata` back. Ephemeral. Not committed.

## PATCH shape probe (pre-flight)

The Analytics REST API PATCH body shape for standard SF reports has historical quirks: some orgs require the body wrapped in `{"reportMetadata": {...}}`, some accept a bare `{...}`, some reject certain read-only fields (`id`, `createdDate`, `lastModifiedDate`) and others silently accept them. The probe empirically determines the correct shape BEFORE the patch script runs.

**Script:** `scripts/phase1_5_patch_shape_probe.py` (uncommitted).

**Target:** one of the 2 OK widgets from the Phase 1 audit (candidates: the Commercial Approval Candidates by Stage or Land Stage 3 Missing Approval by Region reports). Both are OK (no defects), so any successful PATCH is observably neutral.

**Probe steps:**

1. Auth via `sf org display`.
2. GET the candidate report's describe. Save to `/tmp/phase1_5_probe/original.json`.
3. Extract `reportMetadata` from the GET response.
4. Construct a PATCH body: the full `reportMetadata`, with the `name` field appended with ` (probe)`. No other changes.
5. PATCH the report with `Content-Type: application/json` and the body.
6. Inspect the response status and body. If 2xx, proceed. If 4xx/5xx, log the error body, refine the body (try stripping `id` / `createdDate` / `lastModifiedDate`, or try unwrapping from `reportMetadata` envelope), retry. Up to 5 attempts before STOP + escalate.
7. On successful 2xx, GET the report describe again and confirm the `name` field actually changed.
8. PATCH back to the original name (read from `/tmp/phase1_5_probe/original.json`).
9. GET one more time, confirm the original name is restored.
10. Write the confirmed body shape as a JSON stub to `/tmp/phase1_5_probe/confirmed_shape.json` and print it to stdout.

**Cost.** ~5 API calls. ~30 seconds if the first attempt works. Up to ~2 minutes if 5 attempts are needed.

**Success criterion.** The probe confirms a body shape that produces a 2xx and correctly round-trips. The patch script codes against that confirmed shape.

## Testing strategy

Inline `assert`-based tests in each `_cellN_main()` function, same pattern as the Phase 1 audit script. Tests run on every script invocation. If any cell's tests fail, the script exits before hitting the network.

- **Cell 2 (backup):** trivial - create temp dir, write fake backup, read back, assert equality.
- **Cell 3 (ARR):** feed a synthetic `reportMetadata` dict with `aggregates: ['s!AMOUNT']`, assert the transform produces `['s!Opportunity.APTS_Renewal_ACV__c.CONVERT']`. Feed one whose aggregates[0] is already ACV, assert no-op.
- **Cell 4 (fiscal):** 6 fixtures, one per `THIS/LAST/NEXT x FISCAL_QUARTER/FISCAL_YEAR` combination. Each asserts the corresponding `CALENDAR_*` value. Plus one `CUSTOM` fixture that asserts no-op. Plus one `None` fixture.
- **Cell 5 (dashboard component):** feed a synthetic dashboard components list containing at least one SUMMARY-report entry, assert the clone-and-modify logic produces a new entry with the expected reportId, header, title, and a unique id. Assert components length += 1.

## Acceptance criteria

Phase 1.5 is done when all of the following are true:

1. `scripts/phase1_5_patch_shape_probe.py` ran successfully against a low-risk report and confirmed a working PATCH body shape. The confirmed shape is captured as an inline comment in `scripts/phase1_5_patch_dashboard1.py`.
2. `/tmp/phase1_5_backup/reports/*.json` contains one JSON file per affected report (15-17 files expected; exact count enumerated by the plan task from the `6cbe8fe` audit). `/tmp/phase1_5_backup/dashboards/01ZTb00000FSP7hMAH.json` exists. `/tmp/phase1_5_backup/rollback_one.sh` exists.
3. Patch script cell 3 produced 3 successful PATCHes against the 3 renewal reports. Inline verification confirmed each one's `reportMetadata.aggregates[0]` now references `APTS_Renewal_ACV__c` with the `.CONVERT` suffix.
4. Patch script cell 4 produced N successful PATCHes (N is the fiscal filter sweep count). Zero exceptions. Zero 4xx/5xx responses.
5. Patch script cell 5 produced 1 successful PATCH against Dashboard 1. The new component appears in the dashboard's components array referencing `00OTb000008aTtJMAU`.
6. Cell 6 summary printed 0 failures.
7. The audit script re-ran against the post-patch Dashboard 1 state. Output written to `docs/audits/2026-04-07-sales-director-monthly-audit.md` (overwriting `6cbe8fe`).
8. The post-patch audit tally is strictly better than the pre-patch tally:
   - Zero rows tagged with `renewal_uses_amount_not_acv` rule (was 3).
   - Zero rows tagged with `fiscal_date_filter` rule on Dashboard 1 reports (was roughly 10).
   - `commercial_approval_approved_ytd` no longer appears as `(MISSING) BLOCKING`.
   - The 2 forecast accuracy widgets (`00OTb000008TZsDMAW`, `00OTb000008TZaTMAW`) are now OK.
9. The new audit file is committed by exact path. Commit message includes the post-patch tally.
10. `scripts/audit_sales_director_monthly_dashboard.py` stays uncommitted.
11. `scripts/phase1_5_patch_dashboard1.py` stays uncommitted.
12. `scripts/phase1_5_patch_shape_probe.py` stays uncommitted.
13. No em-dashes in any committed file.
14. Commit footer cites the spec commit hash (`8c81d2d`).

## Risks

1. **PATCH body shape may require multiple probe iterations.** Mitigation: the probe loops with progressively refined bodies. If no 2xx after 5 attempts, STOP and escalate.
2. **The fiscal filter sweep may include a report outside Dashboard 1's scope.** Bounded - the audit grades only reports referenced by Dashboard 1's components.
3. **Sales Directors will see different numbers after the fiscal -> calendar swap.** A report previously showing fiscal Q1 (e.g., Feb-Apr) now shows calendar Q1 (Jan-Mar). This is the intended behavior per hard rule 1, but it is a visible change. Someone should probably notify the Sales Directors before they next click through. Phase 1.5 does not include a notification step.
4. **Cell 5 (dashboard component PATCH) is the least-tested operation.** Mitigation: Option A from section 3 brainstorm - accept the risk, backup is one file away, bounded blast radius.
5. **A 2xx PATCH can still produce unexpected field shapes.** Some Analytics REST fields reset to defaults if omitted from the body. Mitigation: the round-trip strategy (full `reportMetadata` in the body) minimizes this risk. The inline verification in cell 3 catches silent corruption for the ARR patches.
6. **The `forecast_accuracy_snapshot` matching may shift after the calendar swap.** The matcher may re-rank the 2 existing widgets vs the new PI-sourced canonical. Informational - not a blocker.

## Out of scope

- Dashboard 2 work (Phase 2.5).
- New SF report builds (Phase 2.5).
- PI Lightning UI list view setup (Phase 2.5).
- Deck rebuild (Phase 4).
- Alex P / Finance feed for churn risk (external).
- Sales Director notification about the fiscal -> calendar change (manual, separate).
- Dashboard layout/ordering after the cell 5 component append.
- All open-question decisions from the specs.

## File paths

**Inputs (read-only, already committed):**

- `docs/specs/sales-director-monthly-dashboard-spec.md` (Report 1 spec, commit `8c81d2d`)
- `docs/audits/2026-04-07-sales-director-monthly-audit.md` (Phase 2 audit, commit `6cbe8fe`; overwritten by Phase 1.5's re-run)
- `docs/specs/report-1-source-contract.md` (Phase 2 source contract, commit `5ac6d62`)
- `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-design.md` (Phase 2 design, commit `c8bdcb7`)
- `scripts/audit_sales_director_monthly_dashboard.py` (Phase 1 audit script, uncommitted, parameterized in Phase 2 Task 1)

**Net new and committed by exact path:**

- `docs/2026-04-07-phase1-5-dashboard1-hotfix-design.md` (this file)
- `docs/2026-04-07-phase1-5-dashboard1-hotfix-plan.md` (writing-plans output, lands next)
- `docs/audits/2026-04-07-sales-director-monthly-audit.md` (re-committed, overwrites `6cbe8fe`)

**Net new and uncommitted (working tree only):**

- `scripts/phase1_5_patch_shape_probe.py`
- `scripts/phase1_5_patch_dashboard1.py`
- `/tmp/phase1_5_probe/original.json` (ephemeral)
- `/tmp/phase1_5_probe/confirmed_shape.json` (ephemeral)
- `/tmp/phase1_5_backup/reports/*.json` (ephemeral, one per affected report)
- `/tmp/phase1_5_backup/dashboards/01ZTb00000FSP7hMAH.json` (ephemeral)
- `/tmp/phase1_5_backup/rollback_one.sh` (ephemeral)

## Handoff to writing-plans

After this design doc lands and Andre approves it, the next step is to invoke `superpowers:writing-plans` and produce a bite-sized implementation plan at `docs/2026-04-07-phase1-5-dashboard1-hotfix-plan.md`. The plan should slice work along cell boundaries: probe task, backup task, ARR patch task, fiscal patch task, dashboard component task, audit re-run task, commit task. Each task independently runnable and reviewable via the subagent-driven-development skill.
