# Sales Director Monthly - Phase 2 SF Reports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the Phase 1 audit script against both feeder dashboards (`01ZTb00000FSP7hMAH` for Report 1 with the amended spec at commit `8c81d2d`, `01ZTb00000FSP9JMAX` for Report 2 with the new spec at commit `25cc03d`), commit both delta reports, probe Pipeline Inspection list views for the 5 forecast-accuracy widgets across both reports, and write two source-of-truth contracts that map each spec widget to a canonical SF report ID or PI list view. Phase 4 deck rebuild consumes the source contracts.

**Architecture:** Parameterize the Phase 1 audit script's 3 hardcoded constants (`DASHBOARD_ID`, `SPEC_PATH`, `AUDIT_OUTPUT_FILENAME`) so it accepts them via `argparse` argv. No other code changes - cells 1-10 reused verbatim. Run the script twice (once per report). Then write the two source contracts as separate markdown files using the audit outputs + a small PI list view probe as input.

**Tech Stack:** Python 3.13, `requests`, `subprocess` (for `sf org display`), `argparse`, markdown for all outputs. Same dependencies as Phase 1. No pytest. No mocking. No MCP. No `build_*.py`. Auth via `sf org display --target-org apro@simcorp.com --json`. API v66.0.

**Design doc (input):** `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-design.md` (commit `c8bdcb7`). Read it first if you have not.

**Spec inputs (read-only, already committed):**

- Report 1: `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`, 16 widgets)
- Report 2: `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`, 22 widgets)

---

## File structure

```
crm-analytics/
|-- docs/
|   |-- 2026-04-07-sales-director-monthly-phase2-sf-reports-design.md  (already committed at c8bdcb7)
|   |-- 2026-04-07-sales-director-monthly-phase2-sf-reports-plan.md     (this file)
|   |-- specs/
|   |   |-- sales-director-monthly-dashboard-spec.md                     (already committed at 8c81d2d)
|   |   |-- sales-ops-quarterly-dashboard-spec.md                        (already committed at 25cc03d)
|   |   |-- report-1-source-contract.md                                  (Task 5, NEW, committed)
|   |   `-- report-2-source-contract.md                                  (Task 7, NEW, committed)
|   `-- audits/
|       |-- 2026-04-07-sales-director-monthly-audit.md                   (Task 2, NEW or supersedes Phase 1, committed)
|       `-- 2026-04-07-sales-ops-quarterly-audit.md                      (Task 3, NEW, committed)
`-- scripts/
    `-- audit_sales_director_monthly_dashboard.py                         (Task 1, MODIFIED in place, uncommitted)
```

Responsibilities:

- **`scripts/audit_sales_director_monthly_dashboard.py`** - The Phase 1 audit script, parameterized in Task 1. Stays uncommitted per convention. Runs once per report in Tasks 2 and 3.
- **`docs/audits/2026-04-07-*-audit.md`** - Two delta reports, one per dashboard, each grading the dashboard against its respective spec. Committed by exact path.
- **`docs/specs/report-N-source-contract.md`** - Two source-of-truth contracts, one per report, each mapping spec widgets to canonical SF report IDs or Pipeline Inspection list views. Committed by exact path. Phase 4 deck rebuild reads these.

---

## Task 0: Verify prerequisites

**Files:** none modified. Read-only checks.

- [ ] **Step 1: Verify the Phase 1 audit script exists**

```bash
test -f ~/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py && \
  wc -l ~/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py
```

Expected: file exists, line count ~1369.

- [ ] **Step 2: Verify both spec files exist at the expected commits**

```bash
cd ~/crm-analytics && \
  git log -1 --format=%h -- docs/specs/sales-director-monthly-dashboard-spec.md && \
  git log -1 --format=%h -- docs/specs/sales-ops-quarterly-dashboard-spec.md
```

Expected: first hash is `8c81d2d` (Report 1 spec amendment), second hash is `25cc03d` (Report 2 spec creation). If either doesn't match, STOP and re-run the relevant spec amendment.

- [ ] **Step 3: Verify both feeder dashboards are reachable via the live org**

```bash
python3 - <<'PY'
import json, subprocess, requests
r = subprocess.run(["sf","org","display","--target-org","apro@simcorp.com","--json"], capture_output=True, text=True, check=True)
d = json.loads(r.stdout[r.stdout.find("{"):])["result"]
inst, tok = d["instanceUrl"], d["accessToken"]
hdr = {"Authorization": f"Bearer {tok}"}
for did, label in [("01ZTb00000FSP7hMAH","Report 1 (Sales Director Monthly)"),("01ZTb00000FSP9JMAX","Report 2 (Sales Ops Quarterly KPI)")]:
    r = requests.get(f"{inst}/services/data/v66.0/analytics/dashboards/{did}/describe", headers=hdr, timeout=30)
    print(f"  {did}  {label}: HTTP {r.status_code}  components={len(r.json().get('components',[])) if r.ok else '?'}")
PY
```

Expected: both return HTTP 200 with non-zero component counts (Dashboard 1 = 16, Dashboard 2 = 13). If either returns 404 or auth fails, STOP - the audit will halt at cell 4.

- [ ] **Step 4: Verify docs/audits/ exists from Phase 1**

```bash
test -d ~/crm-analytics/docs/audits && ls ~/crm-analytics/docs/audits/
```

Expected: directory exists, contains at least `2026-04-06-sales-director-monthly-audit.md` (the Phase 1 audit output).

- [ ] **Step 5: No commit**

Read-only checks. Nothing to commit.

---

## Task 1: Parameterize the Phase 1 audit script

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` lines containing `DASHBOARD_ID`, `SPEC_PATH`, and the audit output filename derivation in `_cell10_main`

The Phase 1 script has 3 hardcoded constants: `DASHBOARD_ID = "01ZTb00000FSP7hMAH"`, `SPEC_PATH = Path(...)`, and the audit output filename is derived inside `_cell10_main` as `f"{rundate}-sales-director-monthly-audit.md"`. Phase 2 needs to run the script twice with different inputs. Solution: argparse, with the existing constants as defaults so backward compatibility is preserved.

- [ ] **Step 1: Add argparse imports if not already present**

Check the top imports block of `scripts/audit_sales_director_monthly_dashboard.py`. If `argparse` is not imported, add it to the imports block:

```python
import argparse
```

- [ ] **Step 2: Add an `_parse_args()` helper near the top of the file (after Constants cell, before Cell 1)**

```python
def _parse_args():
    """Parse argv for dashboard ID, spec path, and audit output name overrides.

    Defaults preserve Phase 1 backward compatibility.
    """
    p = argparse.ArgumentParser(description="Audit a Salesforce dashboard against an expected-widgets spec.")
    p.add_argument(
        "--dashboard-id",
        default="01ZTb00000FSP7hMAH",
        help="Salesforce dashboard ID (default: 01ZTb00000FSP7hMAH, the Phase 1 dashboard)",
    )
    p.add_argument(
        "--spec-path",
        default="/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md",
        help="Absolute path to the expected-widgets spec markdown file",
    )
    p.add_argument(
        "--output-name",
        default="sales-director-monthly-audit",
        help="Audit output filename stem (rundate prepended automatically). Result: docs/audits/{rundate}-{stem}.md",
    )
    return p.parse_args()
```

- [ ] **Step 3: Wire the args into the `__main__` block**

Replace the existing `if __name__ == "__main__":` block at the bottom of the file with:

```python
if __name__ == "__main__":
    args = _parse_args()
    # Override the global constants from argv
    DASHBOARD_ID = args.dashboard_id
    SPEC_PATH = Path(args.spec_path)
    _OUTPUT_STEM = args.output_name

    inst, tok = _cell1_main()
    picklist_values = _cell2_main(inst, tok)
    expected_spec = _cell3_main()  # uses SPEC_PATH from override
    dashboard_describe, dashboard_widgets = _cell4_main(inst, tok)  # uses DASHBOARD_ID from override
    report_meta_by_id = _cell5_main(inst, tok, dashboard_widgets)
    report_run_by_id = _cell6_main(inst, tok, dashboard_widgets)
    static_issues_by_widget = _cell7_main(
        dashboard_widgets, report_meta_by_id, expected_spec
    )
    audit_entries, tally = _cell8_main(
        expected_spec,
        dashboard_widgets,
        static_issues_by_widget,
        report_run_by_id,
        report_meta_by_id,
    )
    audit_out_path = _cell10_main(audit_entries, dashboard_describe, tally)
    print(f"Audit complete: {audit_out_path}")
```

Note: `DASHBOARD_ID`, `SPEC_PATH`, and `_OUTPUT_STEM` become local rebindings inside `__main__`. The existing `_cell3_main()` reads `SPEC_PATH` as a global; the rebinding works because Python looks up the name at call time, not at definition time. If the script's existing cells reference `DASHBOARD_ID` or `SPEC_PATH` from inside helper functions (not from `__main__`), the rebinding may not propagate - in that case, also explicitly pass the values as function arguments. **Verify** by reading the script and confirming where these constants are referenced before completing the step.

- [ ] **Step 4: Wire the output stem into `_cell10_main`**

Find `_cell10_main` and locate the line that builds `out_path`:

```python
out_path = AUDIT_OUTPUT_DIR / f"{rundate}-sales-director-monthly-audit.md"
```

Change it to:

```python
out_path = AUDIT_OUTPUT_DIR / f"{rundate}-{_OUTPUT_STEM}.md"
```

If `_OUTPUT_STEM` is not in scope inside `_cell10_main` (because it was set in `__main__`), thread it through as a function parameter:

```python
def _cell10_main(audit_entries, dashboard_describe, tally, output_stem="sales-director-monthly-audit"):
    ...
    out_path = AUDIT_OUTPUT_DIR / f"{rundate}-{output_stem}.md"
    ...
```

And update the `_cell10_main(...)` call site in `__main__` to pass `output_stem=_OUTPUT_STEM`.

- [ ] **Step 5: Verify the file still parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/audit_sales_director_monthly_dashboard.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 6: Smoke-test argparse without running the audit**

```bash
cd ~/crm-analytics && python3 scripts/audit_sales_director_monthly_dashboard.py --help
```

Expected: argparse usage block printed showing `--dashboard-id`, `--spec-path`, and `--output-name` with their defaults. If the script tries to actually run the audit (i.e. argparse isn't reached early), there's a wiring bug.

- [ ] **Step 7: No commit**

Audit script stays uncommitted by convention.

---

## Task 2: Run the audit against Dashboard 1 (Report 1) with the amended spec

**Files:** generates `docs/audits/2026-04-07-sales-director-monthly-audit.md`

- [ ] **Step 1: Run the audit with explicit Report 1 args**

```bash
cd ~/crm-analytics && time python3 scripts/audit_sales_director_monthly_dashboard.py \
  --dashboard-id 01ZTb00000FSP7hMAH \
  --spec-path /Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md \
  --output-name sales-director-monthly-audit \
  2>&1 | tee /tmp/phase2-r1.log
```

Expected: ~1-2 minutes wall time (Phase 1 took ~1 min for the same dashboard). All cell tests pass. Script writes `docs/audits/2026-04-07-sales-director-monthly-audit.md`.

- [ ] **Step 2: Inspect the tally**

```bash
grep -E "^- \*\*Tally" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
```

Note BLOCKING / WRONG-DATA / OK / ORPHAN counts. Expected: roughly the same shape as Phase 1's audit (12 BLOCKING, 10 WRONG-DATA, 1 ORPHAN, 2 OK), PLUS 2 NEW BLOCKING entries for the spec widgets added in commit `8c81d2d`:

- `(MISSING) commercial_approval_approved_ytd` BLOCKING
- `(MISSING) forecast_accuracy_snapshot` BLOCKING

If those two MISSING entries don't appear, the script may have run against a stale spec - re-verify the spec file's commit hash matches `8c81d2d`.

- [ ] **Step 3: Verify the audit output is the only modified path under docs/audits/**

```bash
cd ~/crm-analytics && git status --short docs/audits/
```

Expected: exactly one untracked or modified file - `docs/audits/2026-04-07-sales-director-monthly-audit.md`.

- [ ] **Step 4: Stage and commit by exact path**

```bash
cd ~/crm-analytics && git add docs/audits/2026-04-07-sales-director-monthly-audit.md && \
  git diff --cached --name-only
```

Expected: exactly one line, the audit path.

- [ ] **Step 5: Commit**

```bash
cd ~/crm-analytics && git commit -m "$(cat <<'COMMIT'
docs: phase 2 report 1 audit re-run against amended 16-widget spec

Re-runs the Sales Director Monthly audit against 01ZTb00000FSP7hMAH
using the amended Report 1 spec at commit 8c81d2d (16 widgets, up
from Phase 1's 14). Supersedes the Phase 1 audit at commit b09f423
for the same dashboard.

Expected new findings vs Phase 1:

- (MISSING) commercial_approval_approved_ytd  BLOCKING
- (MISSING) forecast_accuracy_snapshot         BLOCKING

Plus the same Phase 1 findings:

- 3 renewal widgets aggregating Amount instead of APTS_Renewal_ACV__c
  (BLOCKING)
- 13 of 16 widgets using fiscal date filters instead of calendar
  (WRONG-DATA)
- The original 8 MISSING widgets from the Phase 1 spec
- 1 ORPHAN (Win Rate Rolling 90d) and 2 OK (Commercial Approval
  Candidates by Stage, Land Stage 3 Missing Approval by Region)

Tally: <FILL IN FROM THE RUN>

These findings are the input to Phase 1.5 (the 30-min hotfix on
Dashboard 1, now reframed as BLOCKING per the new design at commit
c8bdcb7) and to Task 5 of this plan (drafting the Report 1 source
contract).

Audit script: scripts/audit_sales_director_monthly_dashboard.py
(uncommitted by convention, parameterized in Task 1 of this plan
to take dashboard ID + spec path + output name via argv).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
COMMIT
)"
```

Replace `<FILL IN FROM THE RUN>` with the actual tally line from the audit file before committing.

- [ ] **Step 6: Verify the commit landed**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s"
```

Expected: latest commit subject is the Report 1 audit re-run.

---

## Task 3: Run the audit against Dashboard 2 (Report 2)

**Files:** generates `docs/audits/2026-04-07-sales-ops-quarterly-audit.md`

- [ ] **Step 1: Run the audit with Report 2 args**

```bash
cd ~/crm-analytics && time python3 scripts/audit_sales_director_monthly_dashboard.py \
  --dashboard-id 01ZTb00000FSP9JMAX \
  --spec-path /Users/test/crm-analytics/docs/specs/sales-ops-quarterly-dashboard-spec.md \
  --output-name sales-ops-quarterly-audit \
  2>&1 | tee /tmp/phase2-r2.log
```

Expected: ~1 minute wall time (13 widgets, 13 unique reports to describe + run). All cell tests pass. Script writes `docs/audits/2026-04-07-sales-ops-quarterly-audit.md`.

- [ ] **Step 2: Inspect the tally**

```bash
grep -E "^- \*\*Tally" ~/crm-analytics/docs/audits/2026-04-07-sales-ops-quarterly-audit.md
```

Expected:

- **9 BLOCKING (MISSING)**: 5 process compliance widgets (`pc_*`) + 4 forecast accuracy widgets (`fa_*`) - none exist on Dashboard 2.
- **2 WRONG-DATA**: `ph_no_activity_30_plus` (uses `s!AMOUNT` instead of ARR) + `ph_overdue_opportunities` (uses `FISCAL_QUARTER` instead of calendar).
- **1 WIP / DEFERRED** (or COSMETIC depending on how the static rules tag it): `ph_probability_mismatch_by_stage` - the "Under Construction" widget.
- **10 OK or COSMETIC**: the rest of the existing widgets in Sections 1 (CRM data quality) and 4 (Pipeline hygiene) should mostly pass.

- [ ] **Step 3: Stage by exact path**

```bash
cd ~/crm-analytics && git add docs/audits/2026-04-07-sales-ops-quarterly-audit.md && \
  git diff --cached --name-only
```

Expected: exactly one line, the audit path.

- [ ] **Step 4: Commit**

```bash
cd ~/crm-analytics && git commit -m "$(cat <<'COMMIT'
docs: phase 2 report 2 audit - 01ZTb00000FSP9JMAX vs 22-widget sales ops spec

Net new audit. Grades the Sales Ops Quarterly KPI Dashboard
(01ZTb00000FSP9JMAX, 13 widgets) against the new Report 2 spec at
commit 25cc03d (22 widgets across 4 KPI sections: CRM data quality,
process compliance, forecast accuracy, pipeline hygiene).

Expected findings:

- 9 BLOCKING (MISSING): 5 process compliance widgets (pc_*) and 4
  forecast accuracy widgets (fa_*). None exist on Dashboard 2.
  These 9 widgets are the Phase 2.5 build queue.
- 2 WRONG-DATA defects on existing widgets:
  - ph_no_activity_30_plus: uses s!AMOUNT instead of
    APTS_Opportunity_ARR__c per hard rule 2
  - ph_overdue_opportunities: groups by FISCAL_QUARTER instead
    of calendar quarter per hard rule 1
- 1 WIP / DEFERRED: ph_probability_mismatch_by_stage is marked
  "Under Construction" on the live dashboard
- 10+ OK on the existing widgets in CRM data quality + pipeline
  hygiene sections (ARR aggregation already correct on Missing
  Decision Reason; KYC Not Completed; Aging Pipeline 365 Plus Days;
  High Value Stale Deals; Stale Opportunities; Overdue Close Date;
  etc.)

Tally: <FILL IN FROM THE RUN>

These findings are the input to Phase 2.5 (build the 9 missing
widgets + fix the 2 defects) and to Task 6 of this plan (drafting
the Report 2 source contract).

Audit script: scripts/audit_sales_director_monthly_dashboard.py
(uncommitted, same script as Task 2 with different argv).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
COMMIT
)"
```

Replace `<FILL IN FROM THE RUN>` with the actual tally.

- [ ] **Step 5: Verify the commit landed**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s"
```

---

## Task 4: Probe Pipeline Inspection list views for the 5 forecast-accuracy widgets

**Files:** none committed yet. Output: a small text file at `/tmp/phase2-pi-probe.txt` for use as input by Tasks 5 and 6.

- [ ] **Step 1: List all PipelineInspectionListView records in the org**

```bash
python3 - <<'PY' | tee /tmp/phase2-pi-probe.txt
import json, subprocess, requests
r = subprocess.run(["sf","org","display","--target-org","apro@simcorp.com","--json"], capture_output=True, text=True, check=True)
d = json.loads(r.stdout[r.stdout.find("{"):])["result"]
inst, tok = d["instanceUrl"], d["accessToken"]
hdr = {"Authorization": f"Bearer {tok}"}

# Probe schema first
r = requests.get(f"{inst}/services/data/v66.0/sobjects/PipelineInspectionListView/describe", headers=hdr, timeout=30)
if not r.ok:
    print(f"PipelineInspectionListView describe: HTTP {r.status_code}")
else:
    fields = [f.get("name") for f in r.json().get("fields", [])]
    print(f"PipelineInspectionListView fields: {fields}")
print()

# Query active list views
soql = "SELECT Id, Name FROM PipelineInspectionListView LIMIT 200"
r = requests.get(f"{inst}/services/data/v66.0/query?q={soql.replace(' ','+')}", headers=hdr, timeout=30)
if not r.ok:
    print(f"PipelineInspectionListView query: HTTP {r.status_code}: {r.text[:500]}")
else:
    records = r.json().get("records", [])
    print(f"PipelineInspectionListView records: {len(records)}")
    for rec in records:
        print(f"  {rec.get('Id')}  {rec.get('Name')}")
print()

# Probe PipelineInspMetricConfig + PipelineInspectionSumField for additional context
for obj in ["PipelineInspMetricConfig", "PipelineInspectionSumField"]:
    r = requests.get(f"{inst}/services/data/v66.0/sobjects/{obj}/describe", headers=hdr, timeout=30)
    fields = [f.get("name") for f in r.json().get("fields", [])] if r.ok else []
    print(f"{obj} fields: {fields}")
    soql = f"SELECT Id, Name FROM {obj} LIMIT 100"
    r = requests.get(f"{inst}/services/data/v66.0/query?q={soql.replace(' ','+')}", headers=hdr, timeout=30)
    if r.ok:
        records = r.json().get("records", [])
        print(f"  records: {len(records)}")
        for rec in records[:20]:
            print(f"    {rec.get('Id')}  {rec.get('Name')}")
    else:
        print(f"  query failed: HTTP {r.status_code}")
    print()
PY
```

Expected: prints the schema of `PipelineInspectionListView`, lists active list views in the org, and dumps the schema + records of the related metric config objects. The output is saved to `/tmp/phase2-pi-probe.txt`.

- [ ] **Step 2: Match each forecast accuracy spec widget to a PI list view**

Open `/tmp/phase2-pi-probe.txt` and inspect the list view names. For each of the 5 forecast accuracy widgets, identify the best matching PI list view by name and intent:

- **Report 1 widget 16 `forecast_accuracy_snapshot`**: look for a list view with quarterly time range + roll-up by forecast category (Pipeline / Best Case / Commit / Closed)
- **Report 2 widget 11 `fa_quarterly_realized_vs_commit`**: same as above; possibly the same list view
- **Report 2 widget 12 `fa_quarterly_realized_vs_bestcase`**: same as above with best-case category
- **Report 2 widget 13 `fa_forecast_change_volatility`**: look for a Pipeline Changes view with 6-month time range
- **Report 2 widget 14 `fa_slipped_count_quarterly`**: look for a Pipeline Changes view filtered to "Slipped" change-type

Write the matched IDs to a small notes file at `/tmp/phase2-pi-matches.txt` in this format:

```
report1_widget_16_forecast_accuracy_snapshot:    <PI_LISTVIEW_ID> or <none, needs Phase 2.5 PI config setup>
report2_widget_11_fa_quarterly_realized_vs_commit:    <PI_LISTVIEW_ID> or <none>
report2_widget_12_fa_quarterly_realized_vs_bestcase:  <PI_LISTVIEW_ID> or <none>
report2_widget_13_fa_forecast_change_volatility:      <PI_LISTVIEW_ID> or <none>
report2_widget_14_fa_slipped_count_quarterly:         <PI_LISTVIEW_ID> or <none>
```

If any widget has `<none>`, the spec's open question for that widget remains unresolved and the source contract row marks it `needs_pi_config_change`.

- [ ] **Step 3: No commit**

The probe outputs are temporary inputs to Tasks 5 and 6. Don't commit them.

---

## Task 5: Draft and commit the Report 1 source contract

**Files:**

- Create: `docs/specs/report-1-source-contract.md`

- [ ] **Step 1: Read the Report 1 audit output**

Open `docs/audits/2026-04-07-sales-director-monthly-audit.md` and identify, for each of the 16 spec widgets:

1. Whether the audit found a matching live widget on Dashboard 1
2. The matched widget's `report_id` (if any)
3. The severity (BLOCKING / WRONG-DATA / OK / etc.)
4. The static rule hits (if any)

- [ ] **Step 2: Read the Report 1 spec for widget IDs**

Open `docs/specs/sales-director-monthly-dashboard-spec.md` and list the 16 widget IDs in spec order.

- [ ] **Step 3: Build the Report 1 source contract markdown**

Create `docs/specs/report-1-source-contract.md` with this structure:

```markdown
# Report 1 Source-of-Truth Contract - Pipeline Reporting & Insights (Sales Director Monthly)

> Maps each of the 16 widgets in the Report 1 spec to a canonical SF report ID or Pipeline Inspection list view. Output of Phase 2 Task 5. Phase 4 deck rebuild reads this file to decide which data source feeds each slide.

## Header

- **Report:** Pipeline Reporting & Insights (monthly, Sales Directors)
- **Feeder dashboard:** `01ZTb00000FSP7hMAH`
- **Spec graded against:** `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`, 16 widgets)
- **Audit input:** `docs/audits/2026-04-07-sales-director-monthly-audit.md`
- **PI probe input:** `/tmp/phase2-pi-probe.txt` and `/tmp/phase2-pi-matches.txt`
- **Generated:** 2026-04-07

## Source contract

| Spec widget                | Canonical source | Source ID         | Verification status          | Phase 2.5 / 1.5 fix needed | Notes                    |
| -------------------------- | ---------------- | ----------------- | ---------------------------- | -------------------------- | ------------------------ |
| `pipeline_overview_global` | SF Report        | <fill from audit> | <OK / WRONG-DATA / BLOCKING> | <yes / no>                 | <audit findings summary> |
| ...                        |
```

For each spec widget row:

- **Canonical source**: `SF Report` for widgets 1-13, 14-15 (slipped deals fallback); `Pipeline Inspection` for widget 16 (forecast accuracy) AND for widgets 14-15 if the user wants to upgrade from the SF report fallback to PI native
- **Source ID**: the report ID from the audit's matched widget for SF report rows; the PI list view ID from `/tmp/phase2-pi-matches.txt` for PI rows
- **Verification status**: copy the audit row's severity for matched widgets; `BLOCKING - widget MISSING from dashboard` for unmatched
- **Phase 2.5 / 1.5 fix needed**: `yes` if severity is BLOCKING or WRONG-DATA; `no` if OK
- **Notes**: 1-2 sentence summary of any audit findings or PI probe results for that widget

- [ ] **Step 4: Stage by exact path**

```bash
cd ~/crm-analytics && git add docs/specs/report-1-source-contract.md && \
  git diff --cached --name-only
```

Expected: exactly one line.

- [ ] **Step 5: Commit**

```bash
cd ~/crm-analytics && git commit -m "$(cat <<'COMMIT'
docs: phase 2 report 1 source contract - 16 widgets mapped to canonical sources

Output of Phase 2 Task 5. Maps each of the 16 spec widgets in Report 1
to a canonical Salesforce report ID or Pipeline Inspection list view.
Phase 4 deck rebuild reads this file to decide which data source feeds
each slide.

Inputs:

- Spec: docs/specs/sales-director-monthly-dashboard-spec.md (commit 8c81d2d)
- Audit: docs/audits/2026-04-07-sales-director-monthly-audit.md
- PI probe: /tmp/phase2-pi-probe.txt and /tmp/phase2-pi-matches.txt

Of the 16 widgets:

- N widgets pinned to existing SF reports with severity OK
- N widgets pinned to existing SF reports flagged for Phase 1.5 fix
  (WRONG-DATA - typically fiscal date filters or AMOUNT-vs-ACV)
- N widgets pinned to existing SF reports flagged for Phase 1.5 build
  (BLOCKING (MISSING) - widget needs to be created)
- N widgets pinned to Pipeline Inspection list views (forecast accuracy
  + slipped deals canonical sources per hard rule 11)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
COMMIT
)"
```

Replace the `N` placeholders with actual counts before committing.

- [ ] **Step 6: Verify**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s"
```

---

## Task 6: Draft and commit the Report 2 source contract

**Files:**

- Create: `docs/specs/report-2-source-contract.md`

Same shape as Task 5 but for Report 2's 22 widgets and Dashboard 2.

- [ ] **Step 1: Read the Report 2 audit output**

Open `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` and walk through all 22 spec widgets, capturing match status + report ID + severity + static rule hits.

- [ ] **Step 2: Build the Report 2 source contract markdown**

Create `docs/specs/report-2-source-contract.md` with the same schema as Task 5's Report 1 contract:

```markdown
# Report 2 Source-of-Truth Contract - Sales Ops Quarterly Report

> Maps each of the 22 widgets in the Report 2 spec to a canonical SF report ID or Pipeline Inspection list view. Output of Phase 2 Task 6. Phase 4 deck rebuild reads this file.

## Header

- **Report:** Sales Ops Quarterly Report
- **Feeder dashboard:** `01ZTb00000FSP9JMAX`
- **Spec graded against:** `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`, 22 widgets)
- **Audit input:** `docs/audits/2026-04-07-sales-ops-quarterly-audit.md`
- **PI probe input:** `/tmp/phase2-pi-probe.txt` and `/tmp/phase2-pi-matches.txt`
- **Generated:** 2026-04-07

## Source contract

| Spec widget | Canonical source | Source ID | Verification status | Phase 2.5 fix needed | Notes |
| ----------- | ---------------- | --------- | ------------------- | -------------------- | ----- |

... (22 rows) ...
```

Expected source mapping breakdown:

- **Section 1 (CRM data quality, 5 widgets)**: all 5 pinned to existing SF reports (`00OTb000008el0PMAQ`, `00OTb000008ekynMAA`, `00OTb000008SqblMAC`, `00OTb000008TZqcMAG`, `00OTb000007BvlJMAS`). Most should be OK. Widget 2 (`dq_missing_quote_type`) needs the open question 2 decision (retire vs repurpose).
- **Section 2 (process compliance, 5 widgets)**: all 5 currently `BLOCKING (MISSING)`. Source ID blank, marked `needs_phase_2_5_build` with reference to the spec row's filter shape. Phase 2.5 builds the 5 new SF reports.
- **Section 3 (forecast accuracy, 4 widgets)**: all 4 pinned to PI list view IDs from `/tmp/phase2-pi-matches.txt` if the probe found matches; otherwise marked `needs_pi_config_change` for Phase 2.5 manual setup.
- **Section 4 (pipeline hygiene, 8 widgets)**: all 8 pinned to existing SF reports. 2 flagged for Phase 2.5 fix (`ph_no_activity_30_plus` AMOUNT -> ARR, `ph_overdue_opportunities` fiscal -> calendar). 1 flagged WIP (`ph_probability_mismatch_by_stage`).

- [ ] **Step 3: Stage and commit by exact path**

```bash
cd ~/crm-analytics && git add docs/specs/report-2-source-contract.md && \
  git diff --cached --name-only && \
  git commit -m "$(cat <<'COMMIT'
docs: phase 2 report 2 source contract - 22 widgets mapped to canonical sources

Output of Phase 2 Task 6. Maps each of the 22 spec widgets in Report 2
to a canonical Salesforce report ID or Pipeline Inspection list view.
Phase 4 deck rebuild reads this file.

Inputs:

- Spec: docs/specs/sales-ops-quarterly-dashboard-spec.md (commit 25cc03d)
- Audit: docs/audits/2026-04-07-sales-ops-quarterly-audit.md
- PI probe: /tmp/phase2-pi-probe.txt and /tmp/phase2-pi-matches.txt

Source mapping breakdown:

- Section 1 (CRM data quality, 5 widgets): all pinned to existing SF
  reports. Widget 2 dq_missing_quote_type needs an open-question
  decision (retire vs repurpose for the canonical Type field).
- Section 2 (process compliance, 5 widgets): all BLOCKING (MISSING).
  Source ID blank, needs_phase_2_5_build. Phase 2.5 creates 5 new
  SF reports per the spec row filter shapes (NextStep documented,
  Land commercial approval flow, recent activity logged, won/loss
  reason documented, stage age within threshold).
- Section 3 (forecast accuracy, 4 widgets): pinned to PI list view
  IDs from the probe, OR needs_pi_config_change if no match.
- Section 4 (pipeline hygiene, 8 widgets): all pinned to existing SF
  reports. 2 flagged for Phase 2.5 defect fix
  (ph_no_activity_30_plus uses Amount, ph_overdue_opportunities uses
  fiscal). 1 flagged WIP (ph_probability_mismatch_by_stage).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
COMMIT
)"
```

- [ ] **Step 4: Verify**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s"
```

---

## Task 7: Final acceptance check and handoff

**Files:** none modified. Read-only verification.

- [ ] **Step 1: Walk the design doc's acceptance criteria**

Open `docs/2026-04-07-sales-director-monthly-phase2-sf-reports-design.md` and check each of the 10 acceptance criteria against the actual run:

1. Audit script parameterized + inline tests pass
2. Both audits run end-to-end
3. Report 1 audit committed at exact path
4. Report 2 audit committed at exact path
5. Report 1 source contract committed at exact path with 16 rows
6. Report 2 source contract committed at exact path with 22 rows
7. PI list view IDs pinned for all 5 forecast-accuracy widgets (or flagged needs_pi_config_change)
8. Audit script uncommitted; outputs committed
9. No em-dashes in any committed file
10. Both commit message footers cite spec commit hashes 8c81d2d and 25cc03d

- [ ] **Step 2: Verify the audit script is still uncommitted**

```bash
cd ~/crm-analytics && git status --short scripts/audit_sales_director_monthly_dashboard.py
```

Expected: `?? scripts/audit_sales_director_monthly_dashboard.py` or `M scripts/audit_sales_director_monthly_dashboard.py` if the parameterization edits weren't reverted. Either is fine - the script stays uncommitted.

- [ ] **Step 3: List all Phase 2 commits**

```bash
cd ~/crm-analytics && git log --oneline --grep="phase 2" -- docs/
```

Expected: at least 5 commits matching "phase 2" - the 2 spec commits, the design doc, the plan doc, plus the new audit + source contract commits from Tasks 2-6.

- [ ] **Step 4: Hand back to Andre**

Print a one-paragraph summary of what shipped, what is pending, and what Phase 2.5 / Phase 4 needs to do next. Include the specific defects from each audit and the specific PI list view binding decisions.

- [ ] **Step 5: No commit**

---

## Self-review checklist (run after writing the plan, before handoff)

- [ ] Every section of the design doc has a corresponding task in this plan.
- [ ] No placeholder text in any task body (TBD, TODO, "fill in details").
- [ ] Every code block contains complete, runnable code.
- [ ] Function and variable names match across tasks.
- [ ] All file paths are absolute or `~/crm-analytics`-relative.
- [ ] Every commit step uses `git add <exact path>`, never `.` or `-A` or `-u`.
- [ ] The audit script is committed nowhere; only outputs and contracts are committed.
- [ ] No em-dashes in any task body.

## Notes for the executor

- **The Phase 1 audit script does most of the work.** Tasks 1-3 are essentially "parameterize, run twice, commit both outputs." The script's existing cells already grade SF reports against a markdown spec - no new code beyond Task 1's argparse changes.
- **PI probing (Task 4) is exploratory.** The probe may discover that the org has no active Pipeline Inspection list views configured, in which case all 5 forecast accuracy widgets get marked `needs_pi_config_change` and Phase 2.5 handles manual setup in the SF Lightning UI.
- **Source contracts (Tasks 5-6) are markdown tables, not running code.** They synthesize the audit findings + the PI probe into a deck-builder-readable contract. These tasks are write-only.
- **No live-org modifications anywhere in this plan.** All `requests` calls are GET / POST /query (read-only). No PATCH, no UI clicks. Phase 2.5 (separate plan) handles the actual fixes.
- **If Task 2 or Task 3 surfaces a defect that breaks the audit script** (e.g. an unhandled API error, a parse failure on a malformed spec row), STOP and escalate. The script is hardened for the Phase 1 dashboard but may hit an edge case on Dashboard 2.
- **Wall time estimate for the full plan: ~30 minutes** assuming no surprises. Most of it is the two audit runs (~2 minutes each) plus the source contract markdown drafting (~5-10 minutes each).
