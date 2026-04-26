# Sales Director Default Operator Flow

Date: April 1, 2026

## Purpose

Lock the Report 1 monthly deck lane as the default PowerPoint product and make the operator path explicit.

## Default Product Definition

The default Report 1 product is:
- a branded `.pptx`, not a dashboard export and not a Quick Look HTML preview
- built by the one-command monthly runner
- reviewed in PowerPoint for signoff when visual fidelity matters
- blocked from publish until the manual overlay gates are satisfied

Primary polished baseline:
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.pptx)

Last passing automated PowerPoint review bundle:
- [powerpoint_review/montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/powerpoint_review/montage.png)

Repeatability proof baseline:
- [sales_director_monthly_pipeline_insights_2026-02-28.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockR_second_snapshot_proof_phase29/sales_director_monthly_pipeline_insights_2026-02-28.pptx)

## Default Entry Point

Use:
- [run_report1_monthly_default.sh](/Users/test/crm-analytics/scripts/run_report1_monthly_default.sh)

That wrapper keeps one operator command surface while still delegating the real work to:
- [run_sales_director_monthly_report.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_report.py)

## Modes

### `default`

Use for the main monthly internal-review run.

Default snapshot:
- `2026-03-31`

Command:

```bash
scripts/run_report1_monthly_default.sh default --json
```

### `proof`

Use for the second-snapshot repeatability check.

Default snapshot:
- `2026-02-28`

Command:

```bash
scripts/run_report1_monthly_default.sh proof --json
```

### `publish`

Use when manual overlays are attached and the operator wants a publish-attempt run.

Default snapshot:
- `2026-03-31`

Command:

```bash
scripts/run_report1_monthly_default.sh publish \
  --finance-csv output/sales_director_monthly_runs/<run>/finance_churn_request.csv \
  --commentary-csv output/sales_director_monthly_runs/<run>/owner_commentary_request.csv \
  --json
```

`publish` does not bypass the gate. It just standardizes the starting mode for the publish attempt.

## Required Review Artifacts

For every run, inspect:
- the generated `.pptx`
- `publish_checklist.md`
- `validation/montage.png`
- `INTERNAL_REVIEW_PACKET.md`
- `powerpoint_review/montage.png` when the review bundle is present

Treat as support only:
- Quick Look thumbnail
- Quick Look HTML preview
- PowerPoint review PDF if a live PowerPoint open/close cycle behaved unexpectedly

## Publish Gate

The deck is not publishable until both manual overlays are present and publishable:
- Finance churn overlay
- slipped-deal owner commentary

Current recurring rule:
- the runner can produce a PowerPoint-first review bundle on this machine, but it is still session-sensitive and manual PowerPoint review remains the signoff surface when visual fidelity matters

## Operator Rules

1. Use the March 31 polished baseline as the main visual reference.
2. Use the February 28 proof run only to confirm repeatability, not to replace the main baseline.
3. Do not reopen stable layout work unless a new snapshot exposes a real regression.
4. If overlays are still missing, prefer tightening snapshot-sensitive copy over inventing fake publish content.
5. Keep every run reproducible with its snapshot JSON, summary JSON, manifest, and checklist.

## Current Default Outcome

As of this lock:
- the lane is runnable from one explicit wrapper command
- the March 31 baseline is the main deck reference and now includes both slipped-commentary and Finance request artifacts
- the February 28 run proves the workflow is not single-snapshot fragile
- the Finance CSV merge path is now proven end to end with a sample overlay file
- publish is still honestly blocked by the two manual overlay inputs

Wrapper verification run:
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/RUN_SUMMARY.md)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/publish_checklist.md)
- [finance_churn_request.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockU_finance_request_pack_phase32/finance_churn_request.md)

Finance merge proof run:
- [sales_director_monthly_pipeline_insights_2026-03-31.pptx](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/sales_director_monthly_pipeline_insights_2026-03-31.pptx)
- [RUN_SUMMARY.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/RUN_SUMMARY.md)
- [publish_checklist.md](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/publish_checklist.md)
- [powerpoint_review/montage.png](/Users/test/crm-analytics/output/sales_director_monthly_runs/2026-04-01T_exec_blockV_finance_csv_merge_proof_phase33/powerpoint_review/montage.png)
