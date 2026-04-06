# KPI Reports Refresh — Snapshot 2026-04-01 — Design Spec

**Date:** 2026-04-06
**Author:** Andre P. (Senior Rev Intelligence) + Claude
**Audience:** internal — operator runbook for the 2026-04-01 snapshot refresh of Report 1 (Sales Directors monthly) and Report 2 (Sales Ops quarterly)
**Architectural decision reference:** [ADR-0001 KPI Reports Data Backbone](../../adr/ADR-0001-kpi-reports-data-backbone.md) — CRMA primary, Salesforce reports as link layer
**Status:** Draft, awaiting user review

---

## Goal

Run a clean refresh of both KPI reports against snapshot date **2026-04-01** (not 2026-03-31), so the deck content reflects the post-month-end state of past-due deals, slipped deals, and field history that became visible after the March 31 fiscal close. No new content beyond what the existing deck templates already render. No changes to KPI definitions, slide layouts, or backbone architecture (those are governed by ADR-0001 and the existing reporting spec).

## Why 2026-04-01 specifically

- The 2026-03-31 baselines were generated on 2026-04-01–04-02 against `--snapshot-date 2026-03-31`. At the close date itself, several signals are near-zero by definition: `IsPastDue` rate, slipped-this-month counts, push events crossing the month boundary.
- Querying as of 2026-04-01 gives a 1-day post-close view: every deal that was forecast to close in March but did not, every push event that landed at month-end, every status flip on Stage 4+ opportunities. Slipped-deal slides and past-due hygiene KPIs become materially populated.
- SimCorp fiscal calendar: fiscal year starts February 1, so Q1 = Feb–Apr. As of 2026-04-01 there are 24 days remaining in fiscal Q1; "this quarter" semantics still resolve to Q1, not Q2. The deck's `quarter_focus` label should still read "Q1".
- This is the **first time** the deck is run against an April-1 snapshot. The 2026-03-31 baseline and the 2026-02-28 repeatability proof (`2026-04-01T_exec_blockR_second_snapshot_proof_phase29`) are the only prior runs. Some signals may surface fresh edge cases.

## Scope

### In scope

- Re-run Report 1 deck (`output/sales_director_monthly_deck_2026-03-31/`) with `--snapshot-date 2026-04-01`
- Re-run Report 2 deck (`output/sales_ops_quarterly_deck_2026-03-31/`) against freshly pulled live SAQL JSONs for the same date
- PowerPoint-based validation (PDF export + page-image montage) for both decks; LibreOffice render path is dropped
- Preflight: kill stale PowerPoint instances, verify `sf` auth
- Land outputs in new run dirs that name both the run date and the snapshot date
- Carry the existing Report 1 publish blockers forward unchanged (Finance churn, slipped commentary)

### Out of scope

- Resolving the Report 1 publish blockers (Finance churn feed, slipped commentary)
- Promoting `0FKTb0000000K5BOAU` from `dry_run` to live deploy
- Wiring standard-reports URLs into Report 1 link-layer slides (ADR-0001 follow-up #1)
- Any changes to deck content, layout, or KPI definitions
- Any changes to CRMA datasets or dashboards
- Any changes to `build_*.py` CRMA builders

## Architecture & flow

```
Phase 0: Preflight
  ├─ pkill -9 'Microsoft PowerPoint' 2>/dev/null || true   (kill stale instances)
  └─ sf org display --target-org apro@simcorp.com --json   (assert auth, capture accessToken/instanceUrl)

Phase 1: Report 1 (Sales Director Monthly)
  └─ scripts/run_report1_monthly_default.sh default \
       --snapshot-date 2026-04-01 \
       --skip-validation \
       --output-dir output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01 \
       --json
     (internally: snapshot refresh → deck build → PowerPoint review bundle → publish_checklist → INTERNAL_REVIEW_PACKET.md)

  └─ Re-preflight: pkill -9 'Microsoft PowerPoint' (in case Report 1's export left state)

Phase 2: Report 2 (Sales Ops Quarterly)
  ├─ Re-pull live SAQL validation JSONs for the four Sales Ops dashboard pages used by the deck:
  │     output/sales_ops_page1_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json
  │     output/sales_ops_page4_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json
  │     output/sales_ops_page5_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json
  │     output/sales_ops_page6_mutation_prep_2026-03-31/live_saql_validation/live_saql_validation.json
  │   (these are dataset-grounding artifacts; refresh them via Wave API SAQL queries against
  │    `apro@simcorp.com` so the deck reflects current org state — they remain in their existing
  │    dirs since they are reusable inputs, NOT in the new run dir)
  │
  ├─ Build deck:
  │     cd output/sales_ops_quarterly_deck_2026-03-31/
  │     node build_sales_ops_quarterly_deck.js \
  │       --output ../../output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/sales_ops_quarterly_review_2026-04-01.pptx \
  │       --snapshot-date 2026-04-01 \
  │       --deck-title "Quarterly Sales Ops Review"
  │
  ├─ PowerPoint PDF export (with retry-once on timeout):
  │     python3 scripts/export_powerpoint_pdf.py \
  │       --input <deck.pptx> \
  │       --output <run_dir>/powerpoint_review/<deck>.pdf \
  │       --json
  │
  ├─ PDF → page images → montage:
  │     .venv_slides/bin/python output/sales_director_monthly_deck_2026-03-31/scripts/render_slides.py \
  │       <pdf> --output_dir <run_dir>/powerpoint_review/rendered
  │     .venv_slides/bin/python output/sales_director_monthly_deck_2026-03-31/scripts/create_montage.py \
  │       --input_dir <rendered> --output_file <run_dir>/powerpoint_review/montage.png
  │
  └─ Quick Look thumbnail:
        qlmanage -t -s 1200 -o <run_dir>/ql_thumb <deck.pptx>

Phase 3: Summary report
  └─ Print to stdout:
       - Both .pptx paths
       - Both summary.json payloads (key KPIs, snapshot date, slide count)
       - KPI delta vs prior 2026-03-31 baseline (data completeness, process compliance,
         forecast accuracy, pipeline hygiene for Report 2; biggest gap region, weakest
         confidence region, slipped-deal counts for Report 1)
       - PowerPoint review status per report (pass / warn / error)
       - Carried-forward Report 1 blockers (finance churn pending, commentary pending)
       - Total wall time
```

## Output dir convention

| Report   | Run dir                                                                                                                  |
| -------- | ------------------------------------------------------------------------------------------------------------------------ |
| Report 1 | `output/sales_director_monthly_runs/2026-04-06T_refresh_snapshot_2026-04-01/`                                            |
| Report 2 | `output/sales_ops_quarterly_runs/2026-04-06T_refresh_snapshot_2026-04-01/` (new tree, mirrors Report 1's run convention) |

Run dir prefix encodes both:

- `2026-04-06T_` — the run date (today), matches existing Report 1 run dir convention
- `_refresh_snapshot_2026-04-01` — what kind of run it is and the snapshot date being targeted

The existing 2026-03-31 baselines in `output/sales_director_monthly_deck_2026-03-31/` and `output/sales_ops_quarterly_deck_2026-03-31/` are NOT overwritten — the refresh is additive.

## Validation gates

Per ADR-0001 follow-up and prior conversation, LibreOffice is dropped on both reports; PowerPoint is the authoritative WYSIWYG validator.

| Gate                                                        | Report 1                            | Report 2                         | Authoritative?                      |
| ----------------------------------------------------------- | ----------------------------------- | -------------------------------- | ----------------------------------- |
| Snapshot pull / SAQL refresh                                | runs (in wrapper)                   | new step (wired in this run)     | yes — hard fail blocks the report   |
| pptxgenjs build success                                     | runs (in wrapper)                   | runs                             | yes — hard fail blocks the report   |
| Quick Look thumbnail                                        | runs (in wrapper)                   | runs                             | yes (cheap sanity check)            |
| **PowerPoint PDF export**                                   | runs (in wrapper)                   | new step (wired in this run)     | **yes — primary gate**              |
| **PowerPoint review montage** (PDF → page images → montage) | runs (in wrapper)                   | new step (wired in this run)     | **yes — primary gate**              |
| LibreOffice render bundle                                   | **skipped via `--skip-validation`** | not present                      | not used                            |
| publish_checklist generation                                | runs (in wrapper)                   | not present (deferred follow-up) | informational only for this refresh |

## Failure handling

| Failure                                                                                                                                                                                                                              | Behavior                                                                                                                                                                                     |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sf` auth fails (Phase 0)                                                                                                                                                                                                            | Stop entire run, surface error, do nothing else                                                                                                                                              |
| `pkill 'Microsoft PowerPoint'` returns non-zero                                                                                                                                                                                      | Ignore (advisory only)                                                                                                                                                                       |
| Report 1 snapshot refresh fails                                                                                                                                                                                                      | Stop Report 1, continue to Report 2 (they are independent), report failure in summary                                                                                                        |
| Report 1 deck build fails                                                                                                                                                                                                            | Stop Report 1, capture stderr, continue to Report 2, report in summary                                                                                                                       |
| Report 2 SAQL re-pull fails                                                                                                                                                                                                          | Stop Report 2, report in summary; Report 1 already complete by this phase                                                                                                                    |
| Report 2 deck build fails                                                                                                                                                                                                            | Stop Report 2, capture stderr, report in summary                                                                                                                                             |
| `export_powerpoint_pdf.py` times out (75s default) or returns error                                                                                                                                                                  | Retry once after a 5s pause; if still failing, surface as `warn` (not hard fail). The .pptx is still the deliverable; manual "Open in PowerPoint" review is the documented signoff fallback. |
| `render_slides.py` fails on the exported PDF                                                                                                                                                                                         | Surface as `warn`; PDF still exists in the run dir for manual review                                                                                                                         |
| Quick Look thumbnail fails                                                                                                                                                                                                           | Surface as `warn`; non-blocking                                                                                                                                                              |
| Report 1 publish_checklist still shows the two `blocked` items (Finance churn, slipped commentary) and no new blockers; the "Rendered validation bundle generated" row flips from `pass` to `skipped` because of `--skip-validation` | Expected — not a failure                                                                                                                                                                     |

## Success criteria

A run is successful if **all** of these are true:

1. Both `.pptx` files exist in their `2026-04-06T_refresh_snapshot_2026-04-01/` dirs
2. Both PowerPoint PDF exports completed (after at most 1 retry) — i.e., both `powerpoint_review/<deck>.pdf` exist
3. Both PowerPoint montages were generated — i.e., both `powerpoint_review/montage.png` exist
4. Both Quick Look thumbnails were generated
5. Report 1 publish_checklist still shows the same 2 blockers as before (Finance churn pending, slipped commentary pending) and no new blockers appeared
6. Summary report printed to stdout shows KPI deltas vs the 2026-03-31 baseline

If criterion 2 ends in `warn` after retry (PowerPoint session-sensitive failure), the run is considered "complete with caveat" — not a hard failure. The .pptx is still the deliverable.

## Code touch list

| File                                                                                 | Action                                                                                                                                                                             |
| ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/run_sales_director_monthly_report.py`                                       | none — `--skip-validation` flag already exists                                                                                                                                     |
| `scripts/run_report1_monthly_default.sh`                                             | none — passes args through to runner                                                                                                                                               |
| `output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js` | none                                                                                                                                                                               |
| `output/sales_ops_quarterly_deck_2026-03-31/build_sales_ops_quarterly_deck.js`       | none                                                                                                                                                                               |
| `scripts/export_powerpoint_pdf.py`                                                   | none — already exists, used by Report 1 wrapper, re-used here for Report 2                                                                                                         |
| **NEW** `scripts/run_report2_quarterly_default.sh`                                   | create — minimal wrapper for Report 2 that does preflight + SAQL refresh + build + PowerPoint review + summary; mirrors `run_report1_monthly_default.sh` structure (~80–120 lines) |
| **NEW** `scripts/run_report2_saql_refresh.py`                                        | create — small helper that re-runs the four sales_ops_pageN SAQL queries via Wave API and writes updated `live_saql_validation.json` files in their existing dirs (~80–120 lines)  |
| **NEW** `output/sales_ops_quarterly_runs/` directory tree                            | create — mirrors `output/sales_director_monthly_runs/`                                                                                                                             |

The two new scripts are the only net-new code. They follow existing patterns: `run_report1_monthly_default.sh` is the model for the wrapper, and the SAQL refresh helper mirrors `output/sales_director_monthly_deck_2026-03-31/refresh_sales_director_monthly_snapshot.py`'s general shape (auth via `sf org display`, query via `requests.post` against `/services/data/v66.0/wave/query`, JSON output, no MCP, no `build_*.py` involvement).

## Open questions / risks

- **Report 2 SAQL refresh queries.** The four `live_saql_validation.json` files contain SAQL query strings and their last-run results. The new `run_report2_saql_refresh.py` helper will need to re-execute those exact queries against the live org. The query strings are already in the JSON files — the helper reads them, re-runs them, writes back the new results. Risk: if the underlying datasets have schema changes since 2026-03-31, queries may fail. Mitigation: surface failed queries in the summary; do not silently fall back to stale data.
- **PowerPoint session-sensitive timeouts.** The README documents this as a known issue, and the publish_checklist from `2026-04-01T_exec_blockU_finance_request_pack_phase32` shows a 45s timeout failure. The runner already uses 75s and writes through `/private/tmp`; phase33 (`2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33`) was the last passing PowerPoint review bundle. Mitigation: preflight pkill + retry-once + warn-not-fail per failure handling table.
- **Fresh edge cases at 2026-04-01.** First run against an April-1 snapshot. May surface SAQL or layout issues that the 2026-03-31 and 2026-02-28 prior runs did not. Mitigation: read the resulting `summary.json` and validate it against the 2026-03-31 baseline before considering the run complete; flag any KPI that is null where it was non-null.
- **Quarter focus label.** The deck's `quarter_focus` should still read "Q1" since 2026-04-01 falls within fiscal Q1 (Feb–Apr). Verify in the resulting summary.json. If the runner mis-resolves to "Q2" the deck will be wrong.
- **The Report 2 SAQL refresh writes back to the existing `2026-03-31` named directories.** This is an intentional reuse pattern (those directories are the per-page mutation prep workspaces, not date-frozen snapshots), but worth flagging — the directory name no longer perfectly reflects the data inside it. A follow-up rename to `sales_ops_pageN_mutation_prep/` (no date) would clean this up but is out of scope here.

## What this design does NOT do

- Does not resolve Report 1's two open publish blockers (Finance churn feed, slipped commentary)
- Does not deploy or verify the live Sales Ops dashboard `0FKTb0000000K5BOAU`
- Does not change deck content, layout, KPI definitions, or backbone architecture
- Does not touch any `build_*.py` CRM Analytics builder
- Does not introduce a new Report 2 publish_checklist (deferred follow-up)
- Does not push to origin

## References

- [ADR-0001 KPI Reports Data Backbone](../../adr/ADR-0001-kpi-reports-data-backbone.md) — CRMA primary, SF reports as link layer
- [Existing reporting spec](../../sales-director-and-sales-ops-reporting-spec.md) — Report 1 hybrid, Report 2 dashboard-first product decision
- [Sales Director monthly deck README](../../../output/sales_director_monthly_deck_2026-03-31/README.md) — operator instructions, current best run, source seams
- [Sales Ops quarterly deck README](../../../output/sales_ops_quarterly_deck_2026-03-31/README.md) — current build/validate flow
- [Pipeline & Sales Ops dashboards spec (2026-03-25)](2026-03-25-pipeline-salesops-dashboards-design.md) — earlier dashboard design, partially implemented via Wave API mutation-prep flows rather than the proposed Python builders
- `crm-analytics/CLAUDE.md` — Wave API gotchas, SAQL rules, no MCP / no Python builders
- `crm-analytics/scripts/run_sales_director_monthly_report.py` — Report 1 runner (read-only reference)
- `crm-analytics/scripts/run_report1_monthly_default.sh` — Report 1 locked operator wrapper
- `crm-analytics/scripts/export_powerpoint_pdf.py` — PowerPoint PDF export helper used by both reports
