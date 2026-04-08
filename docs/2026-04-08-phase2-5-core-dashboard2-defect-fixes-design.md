# Phase 2.5 B-core - Dashboard 2 Defect Fixes Design

> Design doc for the minimal subset of Phase 2.5: two mechanical defect fixes on Dashboard 2 (`01ZTb00000FSP9JMAX`). Scope B-core from the brainstorm - excludes new report builds, dashboard component additions, and product/design decisions. Follows Phase 1.5's validated script pattern.

## One sentence

Patch two defective reports on Dashboard 2 - swap `s!AMOUNT` to `s!Opportunity.APTS_Opportunity_ARR__c` on `ph_no_activity_30_plus` (with `detailColumns` updated), and swap the `FISCAL_QUARTER` grouping to a calendar-quarter equivalent on `ph_overdue_opportunities` - then re-run the Phase 1 audit script against Dashboard 2 and commit the new audit output.

## Context

Phase 1.5 landed on 2026-04-08 (commit `d6476b8`). The Report 1 dashboard is fully patched and Sales Directors can click through to it seeing correct renewal values (ACV), calendar date windows, and a new YTD approved tracker. Phase 1.5's post-patch audit shows strictly better state.

Phase 2 Task 3's Dashboard 2 audit at commit `d48f13c` found 35 entries: 23 BLOCKING, 8 WRONG-DATA, 4 ORPHAN. Two of those WRONG-DATA entries are the targets of this phase:

- `ph_no_activity_30_plus` (report `00OTb000008TaEnMAK`) aggregates standard `s!AMOUNT` instead of the canonical `APTS_Opportunity_ARR__c` per hard rule 2.
- `ph_overdue_opportunities` (report `00OTb000008SrmLMAS`) groups by `FISCAL_QUARTER` instead of calendar quarter per hard rule 1.

The remaining Dashboard 2 work (5 new process compliance reports, WIP completion, retire-vs-repurpose decision, 2 new PI list view configs) is explicitly deferred to future phases.

## Goal

Produce a Dashboard 2 (`01ZTb00000FSP9JMAX`) that passes a post-B-core audit with:

- Zero rows tagged with the ARR-vs-AMOUNT issue text for `ph_no_activity_30_plus`.
- Zero rows tagged with the fiscal-quarter grouping issue for `ph_overdue_opportunities` (or explicit deferral if the fiscal-grouping fix is not tractable in the Reports API).
- All other defects from `d48f13c` carried forward unchanged.
- WRONG-DATA count drops by 2 (or 1 if Fix 2 is deferred) compared to `d48f13c`.

## Non-goals (explicitly deferred)

- **5 new process compliance SF reports** (`pc_*`) - Phase 2.6.
- **9 new Dashboard 1 widget builds** (Report 1 missing spec widgets) - Phase 2.6 or later.
- **2 new PI list views** for `fa_forecast_change_volatility` and `fa_slipped_count_quarterly` - needs manual SF Lightning UI work.
- **WIP completion** for `ph_probability_mismatch_by_stage` - needs a threshold definition decision from Sales Ops.
- **Retire-vs-repurpose** for `dq_missing_quote_type` - needs a product decision.
- **Sales Director notification** about Phase 1.5 visible changes - external, non-code.
- **Phase 4 deck rebuild** - consumes the source contracts, happens after Phase 2.5.

## Constraints (non-negotiable)

Inherited from the workstream's hard rules and Phase 1.5's validated conventions:

1. CLI-first. `sf` CLI for auth, `requests` for API calls. No MCP. No `build_*.py`. No `.env`.
2. Auth via `sf org display --target-org apro@simcorp.com --json`, using the trim-to-first-brace pattern (`r.stdout[r.stdout.find("{") :]`).
3. API version `v66.0`.
4. Calendar year only. The fiscal-grouping fix is the whole point of Fix 2.
5. Use ARR (`APTS_Opportunity_ARR__c`) for pipeline/hygiene widgets per hard rule 2.
6. Pre-PATCH backup to `/tmp/phase2_5_backup/reports/<id>.json` for both affected reports before any PATCH. Dashboard 2 itself is NOT backed up because this phase does not touch dashboard components.
7. Inline verification for BOTH patches. Neither is fail-soft - if the GET-back verify doesn't land the expected change, the patch is marked as failed and the summary surfaces it.
8. Patch script stays uncommitted per `audit_*.py` family convention.
9. Audit output committed by exact path (new file, not an overwrite, since it has a new rundate).
10. No em-dashes in any committed file.
11. Never push to origin.
12. Stage exact paths only.
13. **Canonical aggregate form for ARR is `s!Opportunity.APTS_Opportunity_ARR__c` (NO `.CONVERT` suffix)** - confirmed empirically during Phase 1.5 Task 9. The `.CONVERT` form was rejected by the Reports API.
14. **PATCH body shape is `wrapped_full`** (`{"reportMetadata": {...full metadata...}}`) - confirmed by Phase 1.5 Task 3's probe.
15. **Aggregate fields must also appear in `detailColumns`** - confirmed empirically during Phase 1.5. Fix 1 must add `Opportunity.APTS_Opportunity_ARR__c` to the detail columns list.

## Approach (locked during brainstorm)

Single script `scripts/phase2_5_core_patch.py` with 4 cells plus a separate pre-flight probe `scripts/phase2_5_shape_probe.py` for Fix 2's grouping shape. Same notebook-style pattern as Phase 1.5. Same `--dry-run` support. Same backup / rollback helper pattern.

**Pre-flight probe decides whether Fix 2 is tractable.** The probe queries the org for a non-fiscal, non-custom report that groups by a CloseDate-derived calendar quarter and inspects its `groupingsDown[]` shape. If the probe finds a usable reference, Fix 2 proceeds using that shape. If not, Fix 2 is deferred (leaving just Fix 1 in scope) and the deferral is noted in the audit commit message.

## Architecture

### Pre-flight probe: `scripts/phase2_5_shape_probe.py`

- Auths via `sf org display`.
- Reads `ph_no_activity_30_plus` to confirm the canonical ARR aggregate convention on Dashboard 2 (does it use `.CONVERT` or not?). The existing widget `dq_missing_decision_reason` on Dashboard 2 already uses `Opportunity.APTS_Opportunity_ARR__c.CONVERT` per the Phase 2 audit, so this may differ from Phase 1.5's convention. The probe verifies which form works.
- Reads `ph_overdue_opportunities` to capture its current `groupingsDown[]` structure.
- Queries up to 100 reports for any that group by a CloseDate-derived calendar quarter. If found, inspects the `groupingsDown[]` shape and saves to `/tmp/phase2_5_probe/grouping_shape.json`.
- Optionally tries a round-trip PATCH against `ph_no_activity_30_plus` with a cosmetic name change to confirm the body shape still works with `wrapped_full` on Dashboard 2's reports (same pattern as Phase 1.5's probe).
- Writes results to `/tmp/phase2_5_probe/`.

### Main patch script: `scripts/phase2_5_core_patch.py`

Cells:

| Cell | Purpose                                                                                                                                                                                                                                          |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1    | `get_auth()` - same pattern as Phase 1.5                                                                                                                                                                                                         |
| 2    | `backup_all()` - GET both reports, save to `/tmp/phase2_5_backup/reports/{id}.json`, write `rollback_one.sh` helper                                                                                                                              |
| 3    | `patch_no_activity_arr()` - swap `aggregates[0]` to the confirmed ARR form + add `Opportunity.APTS_Opportunity_ARR__c` to `detailColumns` if absent + PATCH + inline verify                                                                      |
| 4    | `patch_overdue_grouping()` - swap `FISCAL_QUARTER` in `groupingsDown[]` with the calendar-quarter shape from the probe + PATCH + inline verify. If the probe found no usable shape, print a DEFERRED message and return success (no PATCH sent). |
| 5    | `summary()` - print counts                                                                                                                                                                                                                       |

Supports `--dry-run` for safety. Inline assert tests in cells 3 and 4.

### Fix 1 details

Current state (from Phase 2 audit): `aggregates: ['s!AMOUNT']`, grouping by `FULL_NAME`.

Target state:

- `aggregates[0]` = `s!Opportunity.APTS_Opportunity_ARR__c` (or `.CONVERT` variant if the probe confirms Dashboard 2 uses that form)
- `detailColumns` includes `Opportunity.APTS_Opportunity_ARR__c`

Inline verify: GET the report after PATCH, check `aggregates[0]` matches the target string exactly.

### Fix 2 details

Current state: `groupingsDown: [{name: 'Opportunity.Account_Unit_Group__c'}, {name: 'FISCAL_QUARTER'}]` or similar.

Target state (depending on probe results):

- **Option A**: replace the second grouping's `name` with `CLOSE_DATE` and set `dateGranularity: 'Quarter'` (if the Reports API supports this on standard-report types).
- **Option B**: replace with another org-specific calendar-quarter grouping name the probe discovered.
- **Option C**: if neither option is found, DEFER Fix 2 - print a message, do NOT PATCH, note the deferral in the summary.

Inline verify: GET the report after PATCH, check the grouping at index 1 no longer contains `FISCAL_QUARTER`.

## Testing

Inline assert tests in each cell (Phase 1.5 pattern):

- **Cell 3 tests**: feed a synthetic `reportMetadata` dict with `aggregates: ['s!AMOUNT']` and `detailColumns: ['FULL_NAME']`. Assert the transform produces the target ARR aggregate AND adds the ARR column to detailColumns. Feed a fixture whose aggregate is already correct - assert no-op. Feed a fixture whose detailColumns already contains the ARR column - assert the list is not duplicated.
- **Cell 4 tests**: feed a synthetic `groupingsDown` list with `FISCAL_QUARTER` as the second entry. Assert the transform produces the target calendar-quarter shape. Feed a fixture without FISCAL_QUARTER - assert no-op.

## Acceptance criteria

Phase 2.5 B-core is done when:

1. Pre-flight probe completed and confirmed (a) ARR aggregate form for Dashboard 2 (with or without `.CONVERT`), and (b) calendar-quarter grouping shape (or explicit "not found, defer Fix 2").
2. Backups of both affected reports at `/tmp/phase2_5_backup/reports/` + `rollback_one.sh` helper.
3. Fix 1 applied and inline-verified: `ph_no_activity_30_plus` aggregate is ARR, detailColumns includes it.
4. Fix 2 applied and inline-verified OR explicitly deferred: `ph_overdue_opportunities` grouping does not contain `FISCAL_QUARTER`, OR the deferral is noted in the summary with the reason.
5. Audit re-run completed: `docs/audits/2026-04-08-sales-ops-quarterly-audit.md` written.
6. Post-B-core tally strictly better than pre-patch tally from `d48f13c`:
   - Zero rows tagged with ARR-vs-Amount issue for `ph_no_activity_30_plus`.
   - Zero rows tagged with fiscal-grouping issue for `ph_overdue_opportunities` OR a known-deferred entry.
   - WRONG-DATA count dropped by 2 (or 1 if Fix 2 deferred).
7. New audit file committed by exact path. Commit footer cites Report 2 spec hash `25cc03d`.
8. All scripts (probe + patch + audit) stay uncommitted.
9. No em-dashes in any committed file.

## Risks

1. **Fix 2 shape is not determinable.** If no reference calendar-quarter grouping exists in the org, Fix 2 is deferred. This is an acceptable outcome per the B-core scope - Fix 1 still ships, and the deferral is documented.
2. **Dashboard 2 ARR convention may differ from Phase 1.5's Dashboard 1 convention.** The existing `dq_missing_decision_reason` widget uses `Opportunity.APTS_Opportunity_ARR__c.CONVERT` per the Phase 2 audit - but Phase 1.5 found the Reports API rejected `.CONVERT` on Dashboard 1's reports. This is a real inconsistency in the org's API behavior. The probe resolves it empirically before Fix 1 runs.
3. **PATCH body shape drift.** Phase 1.5 confirmed `wrapped_full` works for Dashboard 1's reports. Dashboard 2's reports are the same type (Opportunities), so the same shape should work. The probe can re-verify cheaply.
4. **The `ph_overdue_opportunities` report may have dependencies on the FISCAL_QUARTER grouping** elsewhere (e.g., dashboard component column bindings). Swapping the grouping could break rendering. Mitigation: inline verify + rollback helper.

## File paths

Inputs (read-only):

- `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`)
- `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` (commit `d48f13c`, the pre-patch state)
- `docs/specs/report-2-source-contract.md` (commit `aa8e69a`)

Net new committed by exact path:

- `docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-design.md` (this file)
- `docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-plan.md` (writing-plans output)
- `docs/audits/2026-04-08-sales-ops-quarterly-audit.md` (post-B-core audit, NEW file not an overwrite)

Net new and uncommitted (working tree only):

- `scripts/phase2_5_shape_probe.py`
- `scripts/phase2_5_core_patch.py`
- `/tmp/phase2_5_probe/*.json` (ephemeral)
- `/tmp/phase2_5_backup/reports/*.json` (ephemeral)
- `/tmp/phase2_5_backup/rollback_one.sh` (ephemeral)

## Handoff to writing-plans

After this design doc lands, invoke `superpowers:writing-plans` to produce `docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-plan.md` with bite-sized tasks: pre-flight probe, backup, Fix 1, Fix 2 (or deferral), audit re-run, commit. ~8-10 tasks, ~800-1000 lines.
