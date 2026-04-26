# Sales Director Monthly Deck Handoff

Date: March 31, 2026

## North Star

The priority is Report 1 as a real monthly PowerPoint product for Sales Directors:
- forward-looking
- insight-driven
- sourced from live CRM Analytics seams
- packaged as an editable `.pptx`, not a dashboard screenshot set

Current design rule:
- the deliverable is the `.pptx`
- Quick Look `Preview.html`, `.qlpreview/`, and browser screenshots are validation surfaces only

## Current State

The Report 1 lane is runnable.

What is implemented:
- reusable live snapshot refresher in [refresh_sales_director_monthly_snapshot.py](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/refresh_sales_director_monthly_snapshot.py)
- reusable deck generator in [build_sales_director_monthly_deck.js](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js)
- one-command monthly runner in [run_sales_director_monthly_report.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py)
- optional external overlay contract for Finance churn and slipped-deal commentary in [sales_director_monthly_overlay.example.json](/Users/test/crm-analytics/config/sales_director_monthly_overlay.example.json)

What changed most recently:
- the deck was restyled away from the beige prototype look into a cleaner white-ground executive layout
- the official SimCorp wordmark was extracted from the `Sales Handbook V4.pptx` master and copied into the deck workspace
- the deck source now uses the PNG-backed wordmark across the cover, headers, and footer
- the slide helpers were tightened so tables and bar lists stay inside their containers instead of bleeding into adjacent panels
- the slide 1 cover readout and top-right metric stack were tightened so the executive panel clears the brand lockup cleanly
- the direct `node` build path now completes without overlap or out-of-bounds warnings
- the run manifest and deck summary now preserve overlay status and overlay path for reproducibility
- the workspace README now states explicitly that HTML preview artifacts are not the deliverable
- the latest rerun tightened the narrative framing so the `.pptx` now uses shorter answer-first executive titles instead of generic overview headings
- the latest execution tranche tightened the approval, renewals, churn, and slipped-deal slides so the body pages read more like management pages and less like raw operating tables
- the newest execution tranche also differentiated EMEA, North America, and APAC so the regional pages now carry distinct reads, panel titles, and action framing
- the current execution tranche adds shared executive-copy helpers so card labels, table titles, and slide-level implication lines are more explicit and less placeholder-like
- the latest execution tranche also turns the zero-miss approvals page into a control summary, makes the stage-based approval proxy explicit, and narrows renewals down to the few deals that actually drive concentration
- the current baseline now also separates observed churn history from forward-risk readiness and replaces the dense slipped-deal lower queue with recovery-priority cards when owner commentary is still missing
- the latest hardening tranche adds stricter boundary-aware title shortening, exports the builder helper surface for tests, and adds regression fixtures for low-risk, high-risk, and missing-overlay monthly runs
- the latest execution tranche adds commentary coverage tracking, so partial owner replies now surface as `provided_partial` in the snapshot, checklist, and slipped-deals slide instead of reading like either fully missing or fully publishable
- the current baseline also emits `INTERNAL_REVIEW_PACKET.md`, `owner_commentary_owner_send_list.csv`, `owner_commentary_owner_packets.md`, and one markdown packet per owner so review and owner follow-up can start without manual assembly
- the newest tranche ports a stronger SimCorp proof-point pattern into slide 7, replacing the lower renewals table with quarter-critical proof cards
- the monthly runner now also generates a working PowerPoint-first review export into `powerpoint_review/` by exporting through `/private/tmp` and then moving the PDF back into the run folder
- an earlier tranche made PowerPoint export failures fail fast instead of hanging the runner, and the latest export fix still leaves phase31 as the last passing automated review bundle
- the latest slide-9 cleanup tranche replaces the repeat-push table with a ranked ARR-anchor bar list and upgrades the center implication box into a structured slipped-deal readout strip
- the operator sourcing and publish rules are now written down in `2026-04-01-sales-director-operator-publish-contract.md`
- the runner now also emits `finance_churn_request.md`, `finance_churn_request.csv`, and `finance_churn_request_email.md` so the remaining Finance blocker is an explicit collection workflow
- the newest tranche also lets the runner ingest a filled Finance CSV directly, and the finance-merge proof run completed with a passing automated PowerPoint review bundle

## Most Important Files

- [sales-director-and-sales-ops-reporting-spec.md](/Users/test/crm-analytics/docs/sales-director-and-sales-ops-reporting-spec.md)
- [README.md](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/README.md)
- [refresh_sales_director_monthly_snapshot.py](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/refresh_sales_director_monthly_snapshot.py)
- [build_sales_director_monthly_deck.js](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js)
- [simcorp_wordmark.png](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/assets/simcorp_wordmark.png)
- [run_sales_director_monthly_report.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py)
- [run_report1_monthly_default.sh](/Users/test/crm-analytics/scripts/run_report1_monthly_default.sh)
- [sales_director_monthly_overlay.example.json](/Users/test/crm-analytics/config/sales_director_monthly_overlay.example.json)
- [2026-04-01-sales-director-monthly-deck-execution-plan.md](/Users/test/crm-analytics/docs/2026-04-01-sales-director-monthly-deck-execution-plan.md)
- [2026-04-01-sales-director-default-operator-flow.md](/Users/test/crm-analytics/docs/2026-04-01-sales-director-default-operator-flow.md)
- [2026-04-01-sales-director-operator-publish-contract.md](/Users/test/crm-analytics/docs/2026-04-01-sales-director-operator-publish-contract.md)
- [2026-04-01-sales-director-reference-pattern-audit.md](/Users/test/crm-analytics/docs/2026-04-01-sales-director-reference-pattern-audit.md)

## Latest Operator Run

Current operator baseline:
- [manifest.json](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/manifest.json)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/RUN_SUMMARY.md)
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [sales_director_monthly_pipeline_insights_2026-03-31.summary.json](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.summary.json)
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/ql_thumb/sales_director_monthly_pipeline_insights_2026-03-31.pptx.png)
- [montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/validation/montage.png)
- [font_report.json](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/validation/font_report.json)
- [finance_churn_request.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request.md)
- [finance_churn_request.csv](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request.csv)
- [finance_churn_request_email.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request_email.md)
- [owner_commentary_owner_summary.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/owner_commentary_owner_summary.md)
- [owner_commentary_owner_send_list.csv](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/owner_commentary_owner_send_list.csv)
- [owner_commentary_owner_packets.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/owner_commentary_owner_packets.md)
- [INTERNAL_REVIEW_PACKET.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/INTERNAL_REVIEW_PACKET.md)

Last passing automated PowerPoint review bundle:
- [powerpoint_review/montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/powerpoint_review/montage.png)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/publish_checklist.md)
- [partial proof checklist](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockI_commentary_partial_proof_phase20/publish_checklist.md)

Finance merge proof run:
- [manifest.json](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/manifest.json)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/RUN_SUMMARY.md)
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [powerpoint_review/montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/powerpoint_review/montage.png)

Repeatability proof run on a second snapshot:
- [sales_director_monthly_pipeline_insights_2026-02-28.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/sales_director_monthly_pipeline_insights_2026-02-28.pptx)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/RUN_SUMMARY.md)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/publish_checklist.md)
- [montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/validation/montage.png)

Superseded run:
- `2026-03-31T_brand_asset_refresh` built successfully but used SVG logo embeds that Quick Look rendered as placeholders
- `2026-03-31T_brand_asset_refresh_png` fixed the render-safe logo path
- `2026-03-31T_cover_polish` moved the dark-panel logo and tightened the slide 1 cover, but still carried longer readout bullets than necessary
- `2026-03-31T_cover_polish_v2` is the last pre-layout-cleanup baseline before the table and bar-list geometry fixes
- `2026-03-31T_body_layout_cleanup` is the last clean-layout baseline before the answer-first narrative/title polish
- `2026-04-01T_title_polish_v2` is the last answer-first-title baseline before the body-page management-read polish
- `2026-04-01T_exec_polish_phase1` is the last body-page management-read baseline before the regional-page differentiation tranche
- `2026-04-01T_exec_blockC_renewal_tightening_phase14` is the last baseline before the regional action-card conversion
- `2026-04-01T_exec_blockD_regional_action_cards_phase15` is the last baseline before the executive-copy helper tranche
- `2026-04-01T_exec_blockE_exec_copy_helpers_phase16` is the last baseline before the approval proxy and renewal concentration tranche
- `2026-04-01T_exec_blockF_approval_renewal_focus_phase17` is the last baseline before the churn and slipped-deal restructuring tranche
- `2026-04-01T_exec_blockH_owner_packets_phase19` is the last baseline before commentary coverage tracking and partial-reply behavior
- `2026-04-01T_exec_blockG_churn_slipped_focus_phase18` is the last baseline before the title-rule and regression-test hardening tranche

Current March 31, 2026 readout:
- biggest coverage gap: `North America` at `30,659,427.15`
- weakest confidence region: `EMEA` at `90.0612375598722`
- approval candidate count: `0`
- open renewal pipeline ACV: `28,341,727.14`
- critical renewal ACV: `28,341,727.14`
- biggest slipped region: `APAC` at `52,952,540.61`
- Finance churn overlay status: `pending`
- slipped commentary overlay status: `pending`

## Validation Status

Most recent local checks passed:

```bash
python3 -m py_compile output/sales_director_monthly_deck_2026-03-31/refresh_sales_director_monthly_snapshot.py scripts/run_sales_director_monthly_report.py scripts/merge_sales_director_overlay.py
node --check output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js
python3 -m pytest tests/test_sales_director_monthly_runner.py tests/test_sales_director_monthly_deck_builder_contract.py tests/test_export_powerpoint_pdf.py -q
```

Most recent run checks passed:

```bash
scripts/run_report1_monthly_default.sh default \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32 \
  --json
./scripts/run_sales_director_deck_validation.sh \
  output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.pptx \
  output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/validation
```

Most recent operator rerun:
- `2026-04-01T_exec_blockU_finance_request_pack_phase32` completed with the new Finance request artifacts and a clean validation bundle
- `2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33` proved the new `--finance-csv` path end to end with sample Finance data and a passing PowerPoint review bundle
- preferred publish path is now filled `finance_churn_request.csv` plus filled `owner_commentary_request.csv`, not a hand-edited overlay JSON

Visual validation used:
- Quick Look preview render
- browser screenshot review of the generated Quick Look HTML
- Quick Look thumbnail review of the cleaned cover artifact

Current validation stack:
- LibreOffice render path is installed and working
- `slides_test.py` passes on the current baseline
- font substitution report is clean on the current baseline
- the PowerPoint review export path has a known-good passing bundle in phase33, but it is still session-sensitive

## Known Gaps

Main remaining gaps are not deck mechanics:
- the official SimCorp wordmark is now wired into the deck, but there is still no broader checked-in brand kit beyond that extracted lockup
- Finance churn overlay is still an external contract, now with an automated request pack but not a live integrated feed
- slipped-deal commentary is still an external collection process, not an automated source
- commercial approval eligibility is still based on the current stage-driven approximation
- PowerPoint-first PDF export is still session-sensitive, and it does not replace manual PowerPoint review when fidelity matters

## Immediate Blocker

No active blocker in the current session.

What changed:
- `ls -la ~/Downloads | sed -n '1,120p'` now succeeds
- `Sales Handbook V4.pptx` was readable from `~/Downloads`
- the SimCorp wordmark was extracted from that deck master and added to the monthly deck workspace

Still treat this as session-sensitive:
- if a future Codex session cannot read `~/Downloads`, re-run the same access check first

Observed successful result:

```bash
ls -la ~/Downloads | sed -n '1,120p'
```

Representative source file confirmed:
- `/Users/test/Downloads/Sales Handbook V4.pptx`

## Resume Commands

If work resumes in a fresh Codex session, start here:

Read:
- [SALES_DIRECTOR_MONTHLY_DECK_HANDOFF_2026-03-31.md](/Users/test/crm-analytics/docs/SALES_DIRECTOR_MONTHLY_DECK_HANDOFF_2026-03-31.md)
- [2026-04-01-sales-director-default-operator-flow.md](/Users/test/crm-analytics/docs/2026-04-01-sales-director-default-operator-flow.md)
- [README.md](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/README.md)
- [sales-director-and-sales-ops-reporting-spec.md](/Users/test/crm-analytics/docs/sales-director-and-sales-ops-reporting-spec.md)

First verify Downloads access:

```bash
ls -la ~/Downloads | sed -n '1,120p'
```

If access works, inspect likely source files:

```bash
find ~/Downloads -maxdepth 2 -type f \( -iname '*simcorp*logo*' -o -iname '*simcorp*.svg' -o -iname '*simcorp*.png' -o -iname '*simcorp*.jpg' -o -iname '*handbook*.pptx' \) | sed -n '1,120p'
```

Rerun the branded monthly deck:

```bash
scripts/run_report1_monthly_default.sh default \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_resume_rerun \
  --json
```

When real manual inputs are back, use:

```bash
scripts/run_report1_monthly_default.sh publish \
  --finance-csv output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request.csv \
  --commentary-csv output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/owner_commentary_request.csv \
  --output-dir output/sales_director_monthly_runs/2026-04-01T_exec_publish_attempt \
  --json
```

## Recommended Next Step

Best next slice after the restart:
- keep the PNG-backed SimCorp wordmark as the current safe branded asset
- treat layout cleanup as done unless a new visual review shows a real regression
- look for a fuller official SimCorp brand kit only if the next revision needs more than the wordmark
- continue content polish and insight quality rather than spending more time on core deck mechanics
- use the autonomous tightening plan and keep the validation bundle attached to each new baseline run

## Copy-Paste Resume Prompt

```text
Read /Users/test/crm-analytics/docs/SALES_DIRECTOR_MONTHLY_DECK_HANDOFF_2026-03-31.md first.

We are continuing the Report 1 Sales Directors monthly PowerPoint lane in /Users/test/crm-analytics.

Current state:
- the deliverable is the .pptx, not the Quick Look Preview.html
- latest operator baseline is output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32
- finance merge proof run is output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33
- last passing automated PowerPoint review bundle is output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33
- overlay-backed monthly runner works
- the deck now uses the SimCorp wordmark extracted from Sales Handbook V4
- the slide helpers were tightened and the current generator now runs clean without overlap warnings
- the monthly runner now also emits a render validation bundle by default
- the full workflow has now also been proven on a second Q1 snapshot at `2026-02-28`, not just the March 31 baseline
- the locked default operator wrapper has also been verified end to end on the March 31 lane
- each run now emits a grouped `owner_commentary_owner_summary.md` artifact for fast owner follow-up
- each run now emits `owner_commentary_owner_send_list.csv` with suggested subject lines and owner packet paths
- each run now emits `owner_commentary_owner_packets.md` plus one markdown packet per owner under `owner_commentary_owner_packets/`
- each run now emits `INTERNAL_REVIEW_PACKET.md` for reviewer handoff alongside the deck, checklist, thumbnail, and validation montage
- partial owner replies now surface as `provided_partial`, preserve the block on publish, and show real `received/requested/pending` counts in both the checklist and the slipped-deals read
- the regional pages now use differentiated watchlist/action labels so each region reads more like a leadership page and less like a repeated export
- the renewals slide now centers the decision split between overdue carryover and due-this-quarter exposure and turns the lower queue into quarter-critical proof cards
- the current deck also has answer-first executive titles and stronger section labels instead of generic overview headings
- the body pages for approvals, renewals, churn, and slipped deals were tightened in the latest tranche
- the regional pages now differentiate confidence recovery, coverage gap, and promotion risk instead of repeating the same framing three times
- the current deck now also uses shared executive-copy helpers so card labels, table titles, and slide-level implication lines are more explicit and less placeholder-like
- the approvals follow-up page now defaults to a control summary when live misses are zero, while the renewals page focuses on the few deals that actually drive concentration and labels each by pressure type
- the churn page now separates observed history from forward-risk readiness, and the slipped page now uses recovery-priority cards instead of a dense lower queue while owner commentary is still missing
- the slipped page now also uses a ranked repeat-push ARR bar list plus a structured readout strip instead of generic recovery bullets in the center panel
- the deck builder now has stricter boundary-aware title shortening and regression tests/fixtures around title length, bullet length, helper behavior, and publish-state outcomes
- the slide kickers now use a cleaner section language system, the cover uses a tighter readout strip instead of wrapped bullets, the churn slide now uses both observed proof points and an early-warning readout when Finance input is missing, and the slipped-deals slide uses recovery proof points when owner commentary is missing
- the operator publish contract is in docs/2026-04-01-sales-director-operator-publish-contract.md and defines which inputs stay live versus manual
- the PowerPoint-first PDF export path is still session-sensitive; phase33 is the last passing automated review bundle, and manual PowerPoint review remains the signoff surface when fidelity matters
- the 50-step execution plan is in docs/2026-04-01-sales-director-monthly-deck-execution-plan.md
- the locked default operator flow is in docs/2026-04-01-sales-director-default-operator-flow.md
- the repeatability proof run is output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29
- ~/Downloads access worked in the last successful session, but should still be rechecked at the start of a new Codex session because that permission has been session-specific before

First action:
- verify Downloads access in this new session with: ls -la ~/Downloads | sed -n '1,120p'

If access works:
- keep using output/sales_director_monthly_deck_2026-03-31/assets/simcorp_wordmark.png unless a better official asset is found
- rerun the monthly deck if any layout changes are made
- validate the refreshed .pptx
```
