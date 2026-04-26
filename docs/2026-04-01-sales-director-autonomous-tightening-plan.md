# Sales Director Autonomous Tightening Plan

Date: April 1, 2026

## Purpose

This is the authoritative 90-120 minute autonomous operating plan for the Report 1 Sales Director monthly deck lane.

Use it when the goal is:
- keep improving the monthly PowerPoint without waiting for step-by-step direction
- reduce churn from start/stop sessions
- push the pack materially forward even when Finance or stakeholder responses are still pending

This plan is intentionally tighter than the broader 50-step execution plan. It is meant to remove re-analysis and force one focused work block at a time.

Friday objective:
- keep the pack `internal_review_ready`
- reduce avoidable operator friction to near zero
- leave only real external blockers, not workflow blockers
- ensure every new baseline run carries its own validation bundle and publish gate

## Current Baseline

Primary internal-review baseline:
- [phase20 commentary-coverage baseline run](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20)
- [current best `.pptx`](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [run summary](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/RUN_SUMMARY.md)
- [publish checklist](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/publish_checklist.md)
- [approval rule contract](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/approval_rule_contract.md)
- [validation montage](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/validation/montage.png)
- [owner summary](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/owner_commentary_owner_summary.md)
- [owner send list](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/owner_commentary_owner_send_list.csv)
- [owner packet index](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/owner_commentary_owner_packets.md)
- [internal review packet](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/INTERNAL_REVIEW_PACKET.md)

Proof that slipped commentary can be cleared without Finance:
- [commentary merge check run](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_commentary_merge_check)
- [phase9 commentary auto-merge proof](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_hardening_phase9_commentary_polish_v2)
- [phase20 partial commentary proof](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_partial_proof_phase20)

Current blocker state:
- `slipped commentary`: operationally solvable now through the CSV/email/merge path
- `Finance churn`: still external
- `methodology`: locked to SimCorp semantics in phase 8 with renewals/churn on ACV and land/expand pipeline in ARR

## Canonical Truth

Do not rebuild context from old runs unless something in the current baseline is broken.

Default current-truth read set:
- [workspace README](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/README.md)
- [phase20 run summary](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/RUN_SUMMARY.md)
- [phase20 publish checklist](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/publish_checklist.md)
- [phase20 validation montage](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/validation/montage.png)

Treat these items as solved unless validation or business logic proves otherwise:
- SimCorp methodology lock: renewals/churn in ACV, land/expand in ARR
- approval rule contract: open current-quarter `Land` stage 3 deals without commercial approval
- brand lockup path: current PNG wordmark is the safe default
- render validation stack: LibreOffice + `.venv_slides` + wrapper script is working
- monthly runner: generates snapshot, deck, checklist, thumbnail, validation bundle, internal review packet, owner send list, and per-owner packets

## Autonomy Contract

During a 90-120 minute block, do not stop for incremental confirmation.

Keep going unless one of these is true:
- a destructive org mutation is required
- the data contract contradicts the stated report target
- a source seam fails and there is no safe fallback
- a new external dependency appears that cannot be mocked or deferred

Safe assumptions to make without asking:
- keep March 31, 2026 as the reference snapshot unless explicitly changing the month
- keep the `.pptx` as the deliverable and Quick Look as validation only
- keep the current SimCorp wordmark asset path
- treat Finance as unavailable and continue improving everything else
- prefer creating new dated run directories over overwriting older good artifacts

Do not stop a block just to:
- restate known blockers
- compare against superseded pre-phase20 runs
- re-open solved logo/preview/tooling questions
- ask whether to keep tightening the deck when no new branch choice appeared

Stop only for:
- destructive Salesforce mutation
- contradictory business methodology
- hard source failure with no safe fallback
- a new stakeholder requirement that changes the report target itself

## Session Start Protocol

The first 10 minutes of every new 90-120 minute block should be deterministic.

1. Read only the current-truth files listed above.
2. Run the restart command bundle below.
3. Inspect the latest checklist and choose the highest-leverage tranche from the fixed queue.
4. Do not browse the repo randomly after that unless a concrete file or helper is needed.

Restart command bundle:

```bash
python3 -m py_compile \
  scripts/run_sales_director_monthly_report.py \
  scripts/merge_sales_director_overlay.py \
  output/sales_director_monthly_deck_2026-03-31/refresh_sales_director_monthly_snapshot.py
node --check output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js
python3 scripts/run_sales_director_monthly_report.py \
  --snapshot-date 2026-03-31 \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_autonomous_next \
  --json
```

That command already emits:
- `.pptx`
- `RUN_SUMMARY.md`
- `publish_checklist.md`
- `INTERNAL_REVIEW_PACKET.md`
- `owner_commentary_owner_send_list.csv`
- `owner_commentary_owner_packets.md`
- `ql_thumb/`
- `validation/`

## 90-120 Minute Sprint Shape

### 1. Baseline Refresh: 10-15 min

Read only the minimum current-truth files:
- [workspace README](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/README.md)
- [latest run summary](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/RUN_SUMMARY.md)
- [latest publish checklist](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/publish_checklist.md)

Then decide the highest-leverage tranche using this order:
1. blocker removal
2. data-contract tightening
3. slide copy and geometry polish
4. operator workflow cleanup
5. repeatability and regression protection

Use only one active tranche per block. Do not mix three partial objectives into one block.

### 2. Blocker Removal: 20-30 min

If slipped commentary is still not publishable:
- work only on the collection path, merge path, overlay ingestion, and slide behavior after commentary lands
- do not idle on Finance

If slipped commentary is already publishable:
- move immediately to slide tightening or repeatability

Highest-value moves in this phase:
- tighten commentary collection outputs
- simplify merge-to-overlay workflow
- ensure the deck cleanly flips from `sample/pending` to `provided`
- reduce blocker count in the publish checklist

Default command when commentary exists:

```bash
python3 scripts/run_sales_director_monthly_report.py \
  --snapshot-date 2026-03-31 \
  --commentary-csv output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_coverage_phase20/owner_commentary_request.csv \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_commentary_ready \
  --json
```

### 3. Data And Logic Tightening: 20-25 min

Work only on logic that materially changes trust in the deck:
- approval rule clarity
- renewal scope clarity
- slide copy that overstates certainty
- noisy records that distort leadership reads

Preferred targets:
- remove stale or misleading fallback language
- drop zero-value or test-like rows from leadership tables
- tighten any rule note that is still fuzzy even after the code is explicit

Current preferred logic targets after phase20:
1. final slide-copy density on approvals, churn, and slipped-deals pages
2. review-packet clarity and reviewer instructions
3. commentary-ready reviewer workflow once partial replies arrive
4. publish checklist clarity if blockers change

### 4. Presentation Tightening: 20-30 min

Polish the actual leadership read, not decorative styling.

Priority order:
1. headline clarity
2. management implication bullets
3. table density
4. card labels
5. repeated phrasing across slide families

When choosing between visual polish and readability:
- choose readability

Current slide order of attack:
1. regional pages
2. renewals
3. slipped deals
4. churn
5. approvals
6. cover

Do not spend a full block on:
- HTML preview cosmetics
- generic theme tweaks
- broad color changes without readability gain
- replacing the wordmark path unless the current asset fails render validation

### 5. Rerun And Validate: 15-20 min

Every autonomous block should end with:
- `py_compile`
- `node --check`
- full rerun through [run_sales_director_monthly_report.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py)
- Quick Look thumbnail generation

The monthly runner already performs the validation bundle by default. Only run the wrapper directly when validating an existing deck outside the runner.

If a new operator flow was added:
- exercise it with one sample artifact, not just a static file check

### 6. Lock The State: 10 min

Before ending the block:
- update [workspace README](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/README.md) if the operator path changed
- update any dated execution note or handoff only if the current-truth pointer changed materially
- leave one clean current best run directory with manifest, summary, checklist, thumbnail, and validation bundle

If the new run is not clearly better than the prior baseline:
- do not promote it as the new current baseline
- keep the old baseline pointer
- record the failed direction briefly only if it saves future time

## What To Work On First When Finance Is Still Blocked

This is the default priority stack until Finance arrives:

1. Make slipped commentary collection and ingestion frictionless.
2. Make the slipped-deal slide read like an action page even with commentary pending.
3. Tighten regional slide language so each page lands one executive answer fast.
4. Tighten renewal prioritization so the quarter-critical list is clean and credible.
5. Add regression coverage for dynamic copy, overlay states, and validation-summary behavior.
6. Improve the publish checklist so it reflects reality, not hope.

Do not spend a sprint on:
- hunting a broader brand kit
- generic refactoring with no artifact improvement
- dashboard work that does not improve Report 1
- HTML preview polish beyond what is needed to validate the `.pptx`

## Default Task Queue For The Next Autonomous Blocks

### Block A: Commentary Lane

Goal:
- make owner commentary easy to request, ingest, and rerun

Success criteria:
- CSV request path exists
- email/request note exists
- merge-to-overlay path exists
- rerun with commentary reduces blockers to Finance only

### Block B: Executive Copy Tightening

Goal:
- make every slide read like a leadership pack instead of a smart export

Success criteria:
- no generic labels where an executive read should exist
- no visibly repeated regional phrasing
- no card text that sounds like implementation detail

### Block C: Repeatability And Regression

Goal:
- make the lane resilient to future monthly reruns

Success criteria:
- fixture coverage for sample overlay, pending overlay, and commentary-provided overlay states
- dynamic title/bullet helpers cannot silently bloat past layout expectations
- runner docs match the real artifact flow
- validation bundle remains attached to every promoted baseline

### Block D: Publish Packet Tightening

Goal:
- make the internal-review pack operationally frictionless

Success criteria:
- every promoted run has a clean `RUN_SUMMARY.md`
- the checklist shows only real blockers
- commentary request artifacts are current
- handoff and README point to the same baseline

## Run Naming Convention

Use dated, tranche-specific run directories and do not overwrite old good runs.

Preferred pattern:
- `output/sales_director_monthly_runs/YYYY-MM-DDT_exec_<tranche_name>/`

Examples:
- `.../2026-04-01T_exec_blockA_commentary_tightening_phase12`
- `.../2026-04-01T_exec_commentary_ready`
- `.../2026-04-01T_exec_copy_tightening_blockB`

If a run is only a proof or sample-driven exercise, say so in the directory name.

## Stop Conditions

A sprint is complete when all of the following are true:
- a new dated run exists
- the `.pptx` rebuild succeeded
- the checklist reflects the new truth
- the biggest improvement is visible in the artifact, not just in code

If time runs out mid-block:
- stop only after rerunning or after leaving a clear partial state in code plus one concise doc note

## Resume Commands

Use these as the default restart path for the next 90-120 minute block:

```bash
python3 -m py_compile scripts/run_sales_director_monthly_report.py scripts/merge_sales_director_overlay.py output/sales_director_monthly_deck_2026-03-31/refresh_sales_director_monthly_snapshot.py
node --check output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js
python3 scripts/run_sales_director_monthly_report.py \
  --snapshot-date 2026-03-31 \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_autonomous_next \
  --json
```

If owner commentary arrives as CSV:

```bash
python3 scripts/run_sales_director_monthly_report.py \
  --snapshot-date 2026-03-31 \
  --commentary-csv output/sales_director_monthly_runs/2026-04-01T_exec_blockA_commentary_tightening_phase12/owner_commentary_request.csv \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_commentary_ready \
  --json
```

That one command now auto-builds the commentary overlay and reruns the pack. Use `--overlay-json` too if Finance input is already available and should be layered into the same run.

If validating an existing promoted run directly:

```bash
./scripts/run_sales_director_deck_validation.sh \
  output/sales_director_monthly_runs/2026-04-01T_exec_blockA_commentary_tightening_phase12/sales_director_monthly_pipeline_insights_2026-03-31.pptx \
  output/sales_director_monthly_runs/2026-04-01T_exec_blockA_commentary_tightening_phase12/validation
```

## Definition Of Good Autonomous Progress

A good 90-120 minute block should achieve at least one of these:
- reduce blocker count
- remove one misleading business read
- improve one slide family materially
- remove one operator pain point
- strengthen repeatability for the next month

If it does not change one of those, it was busywork.

## Immediate Next 3 Autonomous Blocks

If there is no new external input, run these in order:

### Next Block 1
- target: publish packet and commentary-ready operator path
- success: internal reviewers get a cleaner packet and commentary intake remains one-step operational

### Next Block 2
- target: commentary-backed rerun once real owner input lands
- success: the deck drops to Finance-only blocker state without extra plumbing work

### Next Block 3
- target: final internal-review copy compression pass
- success: cover, churn, and slipped-deals copy are as short and executive as possible without losing method notes
