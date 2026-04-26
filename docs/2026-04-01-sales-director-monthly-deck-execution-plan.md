# Sales Director Monthly Deck Execution Plan

Date: April 1, 2026

## Goal

Finish Report 1 as a repeatable, insight-led monthly PowerPoint product for Sales Directors:
- branded to real SimCorp standards
- built from live CRM Analytics seams
- editable as a `.pptx`
- strong enough to review in PowerPoint without reading like a dashboard export

## Current Baseline

Current operator baseline:
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.pptx)

Primary source files:
- [build_sales_director_monthly_deck.js](/Users/test/crm-analytics/output/sales_director_monthly_deck_2026-03-31/build_sales_director_monthly_deck.js)
- [run_sales_director_monthly_report.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py)
- [SALES_DIRECTOR_MONTHLY_DECK_HANDOFF_2026-03-31.md](/Users/test/crm-analytics/docs/SALES_DIRECTOR_MONTHLY_DECK_HANDOFF_2026-03-31.md)

Execution status legend:
- `done`: already completed in the repo
- `in_progress`: active tranche in this session
- `queued`: still to do

## 50 Steps

1. `done` Freeze the current resume baseline in a dated run directory.
2. `done` Treat the `.pptx` as the only deliverable and demote HTML/Quick Look to validation only.
3. `done` Keep the SimCorp wordmark wired into the deck workspace as the current safe branded asset.
4. `done` Keep the one-command monthly runner as the primary operator entrypoint.
5. `done` Preserve snapshot JSON, summary JSON, manifest, and run summary for every deck run.
6. `done` Keep March 31, 2026 as the reference snapshot while the deck pattern is hardened.
7. `done` Tighten slide titles away from generic “overview” phrasing toward answer-first headlines.
8. `done` Update the workspace README and handoff so the next session resumes from the right artifact.
9. `done` Strengthen the approvals page so it reads like a control narrative, not a system dump.
10. `done` Strengthen the renewals page so it emphasizes concentration and required leadership action.
11. `done` Strengthen the churn page so the Finance overlay reads as a management input, not metadata.
12. `done` Strengthen the slipped-deals page so it emphasizes where pressure sits and what to do next.
13. `done` Replace any remaining generic metric-card labels with executive language.
14. `done` Standardize kicker language across all slides so sections feel deliberate and not auto-generated.
15. `done` Tighten the cover subtitle so it sounds like a leadership pack, not an internal prototype.
16. `done` Add a short “management implication” sentence to each slide family.
17. `done` Reduce body-slide repetition across the three regional pages.
18. `done` Introduce a shared regional takeaway template so each region page lands a distinct message.
19. `done` Separate “coverage gap” pages from “confidence recovery” pages in regional framing.
20. `done` Upgrade the regional watchlist table to surface only the highest-value movement risks.
21. `done` Add a clearer distinction between promotable risk and true coverage shortfall.
22. `done` Rebalance slide 2-4 geometry so the regional tables no longer dominate the page.
23. `done` Replace any placeholder-like card text with explicit business reads.
24. `done` Add a reusable helper for concise management bullets per slide type.
25. `done` Add a reusable helper for concise table titles per slide type.
26. `done` Add a reusable helper for slide-level “so what” framing based on live snapshot values.
27. `done` Tighten approval logic language so stage-based approximation is clearly labeled as such.
28. `done` If approval candidates remain zero, repurpose the lower approvals section into a control summary.
29. `done` If approval candidates are non-zero in a future run, switch the page into escalation mode automatically.
30. `done` Surface the two or three renewals that truly drive concentration instead of a broad list.
31. `done` Highlight whether critical renewals are timing-driven, forecast-driven, or risk-driven.
32. `done` Separate observed churn history from forward churn risk so those ideas are not blended loosely.
33. `done` Tighten Finance overlay copy to avoid sounding like ingestion metadata.
34. `done` Tighten slipped-deal root-cause bullets so they read as synthesized leadership findings.
35. `done` Reduce the slipped-deal commentary table if it becomes denser than the management read.
36. `done` Add a stricter short-text rule for dynamic headlines and panel titles.
37. `done` Add tests for dynamic title generation so future data changes do not create unreadable headlines.
38. `done` Add tests for bullet generation so panel copy remains concise enough for the layout.
39. `done` Add tests for any new helper functions introduced for executive reads.
40. `done` Add a small regression fixture set for low-risk, high-risk, and missing-overlay monthly runs.
41. `done` Validate every major revision with `node --check`, `py_compile`, and a full rerun.
42. `done` Generate Quick Look review crops for every major revision to keep visual regressions visible.
43. `done` Prefer manual PowerPoint review for signoff when Quick Look and PowerPoint diverge.
44. `done` If PowerPoint export automation is available, add a PowerPoint-first review path to the workflow.
45. `done` Audit more recent SimCorp decks from local references and SharePoint pulls for body-slide patterns.
46. `done` Port only proven body-slide patterns from the stronger SimCorp deck system into this runner.
47. `done` Decide which overlay fields must become live feeds versus staying manual operator inputs.
48. `done` Create a publish-ready checklist for monthly runs: snapshot date, overlay freshness, slide QA, owner signoff.
49. `done` Run the finished workflow on a second month or second snapshot to prove repeatability.
50. `done` Lock the monthly deck lane as the default Report 1 PowerPoint product and document the operator flow.

## Current Tranche

This session executed the Finance-merge tranche:
- extended [run_sales_director_monthly_report.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py) so each run can ingest a filled Finance churn CSV directly into the overlay path
- extended [merge_sales_director_overlay.py](/Users/test/crm-analytics/scripts/merge_sales_director_overlay.py) so Finance and commentary CSV inputs can be merged through one utility
- updated [run_report1_monthly_default.sh](/Users/test/crm-analytics/scripts/run_report1_monthly_default.sh) usage so the publish path points at the Finance CSV request flow instead of hand-edited JSON
- added focused regression coverage in [test_sales_director_monthly_runner.py](/Users/test/crm-analytics/tests/test_sales_director_monthly_runner.py)
- reran the locked operator flow into `2026-04-01T_exec_blockU_finance_request_pack_phase32`
- proved the new Finance CSV merge path end to end in `2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33`

Repeatability proof artifact:
- [sales_director_monthly_pipeline_insights_2026-02-28.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/sales_director_monthly_pipeline_insights_2026-02-28.pptx)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/RUN_SUMMARY.md)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/publish_checklist.md)
- [montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/validation/montage.png)

Current operator baseline with Finance request assets:
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/RUN_SUMMARY.md)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/publish_checklist.md)
- [finance_churn_request.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request.md)
- [finance_churn_request.csv](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request.csv)
- [finance_churn_request_email.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request_email.md)

Finance CSV merge proof run:
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/RUN_SUMMARY.md)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/publish_checklist.md)
- [powerpoint_review/montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/powerpoint_review/montage.png)

Next highest-value tranche is publish-data completion:
- use the new Finance request pack plus the existing owner-commentary pack to collect the two missing manual overlays, then rerun `publish` with the filled CSVs
- if manual inputs remain missing, keep tightening only snapshot-sensitive fallback copy instead of reopening stable layout work
