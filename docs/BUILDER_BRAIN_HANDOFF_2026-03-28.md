# Builder Brain Handoff

Date: March 28, 2026

## North Star

The priority is the builder brain, not legacy Python dashboard builders.

Current target:
- strong cross-surface builder planning in `scripts/builder_brain.py`
- real executable lanes for:
  - CRMA / Wave
  - native Salesforce reports
  - native Salesforce dashboards
- safe top-level live regression flows for throwaway create -> verify -> cleanup

Hard repo rule:
- do not modify or rely on `build_*.py`

## Current State

The builder system is materially upgraded.

What is live-proven:
- CRMA builder lane
  - package -> bundle -> deploy preview -> live PATCH was proven earlier
- native Salesforce report lane
  - preview/apply/verify/delete are live-proven in `scripts/salesforce_report_executor.py`
- native Salesforce dashboard lane
  - preview/apply/verify/delete are live-proven in `scripts/salesforce_dashboard_executor.py`
  - manual native filter authoring is live-proven through browser automation in `scripts/salesforce_dashboard_filter_automation.py`
- top-level builder probe
  - `scripts/builder_brain.py probe` is live-proven for report and dashboard package-driven probes
- top-level batch builder probe
  - `scripts/builder_brain.py probe-matrix` is live-proven for:
    - the March 28, 2026 mixed batch proof in `output/builder_brain/probe_matrix_live_20260328_v2`
    - the checked-in report-only live smoke suite in `config/builder_brain_live_smoke/probe_matrix_report_live_smoke.json`
    - the checked-in mixed live smoke suite in `config/builder_brain_live_smoke/probe_matrix_mixed_live_smoke.json`
- recurring live smoke entry points
  - `make builder-brain-live-smoke-report`
  - `make builder-brain-live-smoke`

## Most Important Files

- [builder_brain.py](/Users/test/crm-analytics/scripts/builder_brain.py)
- [builder_brain_cli.md](/Users/test/crm-analytics/docs/builder_brain_cli.md)
- [salesforce_report_executor.py](/Users/test/crm-analytics/scripts/salesforce_report_executor.py)
- [salesforce_dashboard_executor.py](/Users/test/crm-analytics/scripts/salesforce_dashboard_executor.py)
- [salesforce_dashboard_filter_automation.py](/Users/test/crm-analytics/scripts/salesforce_dashboard_filter_automation.py)
- [test_builder_brain.py](/Users/test/crm-analytics/tests/test_builder_brain.py)
- [probe_matrix_report_live_smoke.json](/Users/test/crm-analytics/config/builder_brain_live_smoke/probe_matrix_report_live_smoke.json)
- [probe_matrix_mixed_live_smoke.json](/Users/test/crm-analytics/config/builder_brain_live_smoke/probe_matrix_mixed_live_smoke.json)
- [Makefile](/Users/test/crm-analytics/Makefile)

## Latest Live Proof

Best current end-to-end proof:
- [probe_matrix_mixed_live_smoke.json](/Users/test/crm-analytics/config/builder_brain_live_smoke/probe_matrix_mixed_live_smoke.json)
- [live_smoke/mixed](/Users/test/crm-analytics/output/builder_brain/live_smoke/mixed)

Key result:
- batch command completed `2/2`
- report probe:
  - created `00OTb000008dUcjMAE`
  - verified clean
  - deleted clean
- dashboard probe:
  - created `01ZTb00000FFdJlMAL`
  - authored and verified 3 manual filters
  - deleted clean

Primary summary artifact:
- [probe_matrix_summary.json](/Users/test/crm-analytics/output/builder_brain/live_smoke/mixed/probe_matrix_summary.json)

Per-entry results:
- [probe_result.json](/Users/test/crm-analytics/output/builder_brain/live_smoke/mixed/01_manager_report_probe/probe_result.json)
- [probe_result.json](/Users/test/crm-analytics/output/builder_brain/live_smoke/mixed/02_manager_dashboard_probe/probe_result.json)

Report-only smoke proof:
- [probe_matrix_report_live_smoke.json](/Users/test/crm-analytics/config/builder_brain_live_smoke/probe_matrix_report_live_smoke.json)
- [live_smoke/report](/Users/test/crm-analytics/output/builder_brain/live_smoke/report)
- created `00OTb000008dUb7MAE`
- verified clean
- deleted clean

Important runtime note:
- the first sandboxed live smoke attempt on March 28, 2026 failed because the Salesforce CLI could not open `/Users/test/.sf/sf-2026-03-28.log`
- rerunning the exact same live smoke commands outside the sandbox succeeded
- current evidence says this is an execution-environment issue, not a builder-brain regression

## Important Recent Upgrade

Top-level probes now support:
- `--package` on `probe`
- `probe-matrix` manifest execution
- `--executor-timeout-seconds` on `probe` and `probe-matrix`
- manifest-relative `package` and `dashboard_filter_automation_script` path resolution inside `probe-matrix`
- checked-in live smoke manifests under `config/builder_brain_live_smoke/`
- recurring `make` targets for report-only and mixed live smoke runs

Why this matters:
- builder probes no longer have to depend on fuzzy query routing when a known-good package already exists
- batch runs are now first-class
- if a nested executor stalls, the builder wrapper can recover the created asset id from partial `01_apply` artifacts and still attempt cleanup
- the live regression suite is now repo-backed instead of one-off output artifacts

## Validation Status

Most recent local checks passed:

```bash
python3 -m py_compile scripts/builder_brain.py tests/test_builder_brain.py
python3 -m ruff check scripts/builder_brain.py tests/test_builder_brain.py
python3 -m pytest tests/test_builder_brain.py -q
```

Most recent live checks passed on March 28, 2026:

```bash
make builder-brain-live-smoke-report
make builder-brain-live-smoke
```

## Cleanup Status

The latest live probe assets from the March 28, 2026 smoke runs were deleted by the batch flow itself.

Most recent deleted probe ids:
- report-only smoke report: `00OTb000008dUb7MAE`
- mixed smoke report: `00OTb000008dUcjMAE`
- mixed smoke dashboard: `01ZTb00000FFdJlMAL`

There was also an earlier stuck dashboard batch run during hardening.
That probe dashboard was cleaned up manually and verified deleted:
- dashboard: `01ZTb00000FFB49MAH`

## Known Gaps

Main remaining gap is not basic authoring. It is builder-system hardening:
- make batch probe cleanup even more defensive if a dashboard verify or browser lane stalls in unusual ways
- decide whether to harden or explicitly document the `sf` CLI log/state requirement for restricted sandbox environments

No current evidence says the native report/dashboard builder lanes are blocked.
The checked-in report-only and mixed live smoke suites both succeeded on March 28, 2026.

## Resume Commands

If work resumes after a Codex update, start here:

Read:
- [BUILDER_BRAIN_HANDOFF_2026-03-28.md](/Users/test/crm-analytics/docs/BUILDER_BRAIN_HANDOFF_2026-03-28.md)
- [builder_brain_cli.md](/Users/test/crm-analytics/docs/builder_brain_cli.md)

Quick local confidence:

```bash
python3 -m pytest tests/test_builder_brain.py -q
```

Recheck latest successful mixed batch proof:

```bash
cat output/builder_brain/live_smoke/mixed/probe_matrix_summary.json
cat output/builder_brain/live_smoke/mixed/02_manager_dashboard_probe/probe_result.json
```

Rerun a report-only live smoke:

```bash
make builder-brain-live-smoke-report
```

Rerun a mixed live smoke:

```bash
make builder-brain-live-smoke
```

Direct manifest entry points:

```bash
python3 scripts/builder_brain.py probe-matrix \
  --manifest config/builder_brain_live_smoke/probe_matrix_report_live_smoke.json \
  --output-dir output/builder_brain/live_smoke/report_rerun \
  --json

python3 scripts/builder_brain.py probe-matrix \
  --manifest config/builder_brain_live_smoke/probe_matrix_mixed_live_smoke.json \
  --output-dir output/builder_brain/live_smoke/mixed_rerun \
  --json
```

## Recommended Next Step

Best next slice:
- harden builder cleanup recovery further for interrupted dashboard filter flows
- decide whether to add a preflight or operator note for the Salesforce CLI `~/.sf` log/state write requirement in restricted environments

The repeatable regression suite now exists. The remaining work is operational hardening around interruption and environment handling.
