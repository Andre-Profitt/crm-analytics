# Sales Director Monthly Dashboard - Phase 1 Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Audit every widget on the standard Salesforce dashboard `01ZTb00000FSP7hMAH` (Sales Directors Monthly - Pipeline and Insights, 16 widgets) against a newly-written expected-widgets spec distilled from the KPI brief, and produce a committed two-table delta report identifying every BLOCKING, WRONG-DATA, ORPHAN, COSMETIC, and OK widget.

**Architecture:** Two committed deliverables plus one uncommitted tool. (1) A permanent expected-widgets spec at `docs/specs/sales-director-monthly-dashboard-spec.md` that distills the prose KPI brief into a concrete contract. (2) A notebook-style Python audit script at `scripts/audit_sales_director_monthly_dashboard.py` that uses the Salesforce Analytics REST API to grade the live dashboard against the spec. (3) A committed delta report at `docs/audits/<rundate>-sales-director-monthly-audit.md`. The script talks to the org via `sf org display` for auth and `requests` for all API calls (same pattern as the proven Option D POC at `scripts/simcorp_crma_chart_sample.py`). Bidirectional comparison (spec-to-dashboard and dashboard-to-spec). Severity taxonomy: BLOCKING / WRONG-DATA / ORPHAN / COSMETIC / OK.

**Tech Stack:** Python 3.13, `requests` library, `subprocess` to shell out to `sf` CLI, markdown for all outputs. No pytest (notebook-style inline `assert` cells for validation). No mocking framework. No MCP. Target org `apro@simcorp.com`, API v66.0.

---

## File structure

```
crm-analytics/
|-- docs/
|   |-- 2026-04-06-sales-director-monthly-phase1-audit-design.md  (already committed)
|   |-- 2026-04-06-sales-director-monthly-phase1-audit-plan.md     (this file)
|   |-- specs/
|   |   `-- sales-director-monthly-dashboard-spec.md               (Task 1, NEW, committed)
|   `-- audits/
|       `-- 2026-04-07-sales-director-monthly-audit.md             (Task 16, NEW, committed at run time; date is run date)
`-- scripts/
    `-- audit_sales_director_monthly_dashboard.py                   (Tasks 3-13, NEW, uncommitted by default)
```

Responsibilities:

- **`docs/specs/sales-director-monthly-dashboard-spec.md`** - The permanent contract. Every future audit or rebuild of the Sales Director Monthly dashboard grades against this file.
- **`scripts/audit_sales_director_monthly_dashboard.py`** - The audit tool. Notebook-style cells (`# %%` markers) so sections can be re-run independently. Each cell has a single responsibility: auth, picklist assertion, spec load, dashboard describe, report describes, report runs, static rule scan, bidirectional comparison, markdown rendering, composition.
- **`docs/audits/<rundate>-sales-director-monthly-audit.md`** - The delta report the audit produces. Header block, executive summary table, full appendix table, spec-gap notes, phase-2/3/4 implications, reproducibility footer.

Cell decomposition inside the audit script (each cell is one function plus its inline test cell where applicable):

| Cell | Purpose                                                         | Pure / impure  | Inline tests?          |
| ---- | --------------------------------------------------------------- | -------------- | ---------------------- |
| 0    | Imports + constants                                             | pure           | no                     |
| 1    | `get_auth()` - shell out to `sf org display`                    | impure (shell) | no (manual spot-check) |
| 2    | `assert_picklist_fresh()` - `APTS_Primary_Quote_Type__c` values | impure (API)   | no (exits on failure)  |
| 3    | `load_expected_spec(path)` - parse markdown spec                | pure           | YES                    |
| 4    | `get_dashboard_describe(inst, tok, id)` - dashboard metadata    | impure (API)   | no (manual spot-check) |
| 5    | `get_report_describe(inst, tok, id)` - report metadata          | impure (API)   | no (manual spot-check) |
| 6    | `run_report(inst, tok, id)` - report execution                  | impure (API)   | no (manual spot-check) |
| 7    | `apply_static_rules(report_meta, picklist_values)`              | pure           | YES                    |
| 8    | `compare(spec, dashboard_meta_list)` - bidirectional            | pure           | YES                    |
| 9    | `render_markdown(entries, header_block)`                        | pure           | YES                    |
| 10   | Composition cell - calls all of the above end to end            | impure         | no                     |

---

## Task 0: Create directory scaffolding

**Files:**

- Create: `docs/specs/` (empty dir)
- Create: `docs/audits/` (empty dir)

- [ ] **Step 1: Create the spec directory**

```bash
mkdir -p ~/crm-analytics/docs/specs
```

- [ ] **Step 2: Create the audit directory**

```bash
mkdir -p ~/crm-analytics/docs/audits
```

- [ ] **Step 3: Verify directories exist**

```bash
ls -la ~/crm-analytics/docs/specs ~/crm-analytics/docs/audits
```

Expected: both directories listed, both empty.

- [ ] **Step 4: No commit**

Empty directories are not tracked by git. Nothing to commit yet.

---

## Task 1: Draft the expected-widgets spec

**Files:**

- Create: `docs/specs/sales-director-monthly-dashboard-spec.md`

**Source of truth:** The KPI brief verbatim from `docs/2026-04-06-deck-and-dashboard-verification-handoff.md`, under the heading "Report 1: Pipeline Reporting and Insights".

**Target structure:** Four sections, per the design doc: (1) Header, (2) Expected widgets table, (3) Implied widget count, (4) Open questions. Follow the design doc's "The expected-widgets spec format" section exactly.

- [ ] **Step 1: Write the spec header**

Create `~/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md` with this header:

```markdown
# Sales Director Monthly Dashboard - Expected Widgets Spec

> The permanent contract for `01ZTb00000FSP7hMAH` (Sales Directors Monthly - Pipeline and Insights). Distilled from the KPI brief in `docs/2026-04-06-deck-and-dashboard-verification-handoff.md`. Every future audit or rebuild of this dashboard grades against this file.

## Dashboard identity

- **Dashboard ID:** `01ZTb00000FSP7hMAH`
- **Name:** Sales Directors Monthly - Pipeline and Insights
- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/01ZTb00000FSP7hMAH/view
- **Audience:** Sales Directors (level below Managing Directors)
- **Cadence:** Monthly
- **Format in which it is surfaced:** PowerPoint deck (Option D recipe pulls from the CRMA equivalent)

## KPI brief (verbatim from Andre)

Monthly report to the Sales Directors (level below MDs). Forward looking, insight-driven, PowerPoint format.

- A pipeline overview with quarterly focus (one slide per region)
- Commercial Approval overview - which deals have been approved and a list of any Land stage 3 deals with no commercial approval. Global overview (one slide) + list of candidates by region (one slide)
- Renewals tracking - what renewals are coming up this quarter, what is the value and likelihood of renewing
- Churn Risk and trends - difficult for now, but try to build a slide of what we can get from Finance. Andre is to reach out to Alex P. The current snapshot has `finance_feed_status: pending`.
- Slipped deals analysis (root cause commentary) - start with slipped deals; root cause commentary requires reaching out to the opportunity owner. Andre will structure the outreach.

## Hard rules applied to this dashboard

1. **Calendar year only.** No FY labels. Use `April 2026` or `Q2 2026` (calendar).
2. **ACV for renewals.** Land and expand pipeline stays in ARR. This is a SimCorp methodology quirk.
3. **Type field is canonical for renewal / land / expand.** The `APTS_Primary_Quote_Type__c` field is stale; the org migrated to `SBL`, `MBL`, `PPL`. Use `Type='Renewal'`, `Type='Land'`, `Type='Expand'`.
4. **No em-dashes** anywhere in widget labels. Use hyphens, periods, or rephrase.
5. **No gauges or donuts** in widget visuals. Bullet charts and ranked bars only.
```

- [ ] **Step 2: Write the expected widgets table**

Append to the spec file:

```markdown
## Expected widgets table

Each row is a widget the dashboard should contain. The audit grades against this table.

| #   | Widget ID                      | KPI bullet                                          | Type   | Grain      | Required filters                                                                                                                                | Aggregation | Grouping                           | Drilldown report | Notes                                                                                                                                |
| --- | ------------------------------ | --------------------------------------------------- | ------ | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `pipeline_overview_global`     | Pipeline overview with quarterly focus              | metric | global     | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                                       | `sum(ARR)`  | n/a                                | TBD              | Single global number. Calendar quarter, not fiscal.                                                                                  |
| 2   | `pipeline_overview_emea`       | Pipeline overview with quarterly focus              | chart  | per_region | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `SalesRegion IN ('UKI','Central Europe','Northern Europe','Southwestern Europe')` | `sum(ARR)`  | `Stage`                            | TBD              | Stacked bar by stage, one per EMEA sub-region                                                                                        |
| 3   | `pipeline_overview_nam`        | Pipeline overview with quarterly focus              | chart  | per_region | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `SalesRegion='North America'`                                                     | `sum(ARR)`  | `Stage`                            | TBD              | Stacked bar by stage                                                                                                                 |
| 4   | `pipeline_overview_apac`       | Pipeline overview with quarterly focus              | chart  | per_region | `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER` AND `SalesRegion IN ('APAC','Middle East & Africa')`                                  | `sum(ARR)`  | `Stage`                            | TBD              | Stacked bar by stage                                                                                                                 |
| 5   | `commercial_approval_global`   | Commercial Approval overview (global)               | metric | global     | `Commercial_Approval_Status__c IN ('Approved','Pending')`                                                                                       | `count`     | `Commercial_Approval_Status__c`    | TBD              | Count of approved vs pending, global                                                                                                 |
| 6   | `land_stage3_no_approval_emea` | Commercial Approval - Land Stage 3 missing approval | table  | per_region | `Type='Land'` AND `Stage='3'` AND `Commercial_Approval_Status__c=null` AND `SalesRegion IN (EMEA)`                                              | `count`     | n/a                                | TBD              | List of opps; displayed columns: Account, Opp Name, ARR, Stage entered, Owner                                                        |
| 7   | `land_stage3_no_approval_nam`  | Commercial Approval - Land Stage 3 missing approval | table  | per_region | `Type='Land'` AND `Stage='3'` AND `Commercial_Approval_Status__c=null` AND `SalesRegion='North America'`                                        | `count`     | n/a                                | TBD              | Same columns as EMEA version                                                                                                         |
| 8   | `land_stage3_no_approval_apac` | Commercial Approval - Land Stage 3 missing approval | table  | per_region | `Type='Land'` AND `Stage='3'` AND `Commercial_Approval_Status__c=null` AND `SalesRegion IN ('APAC','Middle East & Africa')`                     | `count`     | n/a                                | TBD              | Same columns as EMEA version                                                                                                         |
| 9   | `renewal_acv_this_quarter`     | Renewals tracking                                   | metric | global     | `Type='Renewal'` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                                       | `sum(ACV)`  | n/a                                | TBD              | **ACV, not ARR**                                                                                                                     |
| 10  | `renewal_likelihood`           | Renewals tracking                                   | chart  | global     | `Type='Renewal'` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                                       | `sum(ACV)`  | `Probability` bucket               | TBD              | **Open question: what is "likelihood" - opportunity probability, stage-based proxy, or custom field?**                               |
| 11  | `renewal_upcoming_list`        | Renewals tracking                                   | table  | global     | `Type='Renewal'` AND `IsClosed=false` AND `CloseDate IN THIS_CALENDAR_QUARTER`                                                                  | n/a         | n/a                                | TBD              | Displayed columns: Account, Opp Name, ACV, Close Date, Probability, Owner                                                            |
| 12  | `churn_risk_placeholder`       | Churn Risk and trends                               | metric | global     | n/a                                                                                                                                             | n/a         | n/a                                | TBD              | **Placeholder.** Finance feed is pending from Alex P. Label: "Awaiting Finance feed"                                                 |
| 13  | `slipped_deals_root_cause`     | Slipped deals analysis                              | table  | global     | `CloseDate_Changed__c=true` AND `Previous_CloseDate__c < Current_CloseDate__c`                                                                  | `count`     | `Stage`                            | TBD              | Displayed columns: Account, Opp Name, Previous Close Date, New Close Date, Days slipped, Owner, Root cause (pending - blank for now) |
| 14  | `slipped_deals_trend`          | Slipped deals analysis                              | chart  | global     | `CloseDate_Changed__c=true`                                                                                                                     | `count`     | `Month(CloseDate_Changed_Date__c)` | TBD              | Line chart, trailing 6 calendar months                                                                                               |

**Total expected: 14 widgets.** The live dashboard has 16. The 2 extras, if they exist, are tagged `ORPHAN` by the audit and a decision is needed (keep / drop / fold into spec).
```

- [ ] **Step 3: Write the implied count, open questions, and footer**

Append to the spec file:

```markdown
## Implied widget count

| KPI bullet                             | Expected widgets                                                                 | Count  |
| -------------------------------------- | -------------------------------------------------------------------------------- | ------ |
| Pipeline overview with quarterly focus | 1 global metric plus 3 per-region charts                                         | 4      |
| Commercial Approval overview           | 1 global summary plus 3 per-region Land Stage 3 missing-approval lists           | 4      |
| Renewals tracking                      | 1 ACV-this-quarter metric plus 1 likelihood chart plus 1 upcoming-renewals table | 3      |
| Churn risk and trends                  | 1 placeholder (Finance feed pending)                                             | 1      |
| Slipped deals analysis                 | 1 root cause table plus 1 trend chart                                            | 2      |
| **Total expected**                     |                                                                                  | **14** |

## Open questions

These are gaps in the brief. Andre must resolve them before the audit runs, or the spec records the assumption and the audit notes it.

1. **Renewals "likelihood"** - is that opportunity `Probability` (%), a stage-based proxy (`Stage` maps to rough %), or a custom field? _Current assumption: opportunity `Probability`._
2. **Churn placeholder wording** - what does widget 12 say while the Finance feed is pending? _Current assumption: "Awaiting Finance feed (Alex P)"._
3. **Slipped deals root cause column** - opportunity-owner commentary is pending. Until then, is the column blank, "Pending", or dropped? _Current assumption: column present but blank, label "Root cause (pending)"._
4. **"Quarterly focus" scope** - current calendar quarter only, rolling 4 quarters, or current plus next? _Current assumption: current calendar quarter only._
5. **Commercial approval status field name** - the spec uses `Commercial_Approval_Status__c` as a placeholder. _Current assumption: this field exists; if not, the audit will find the actual field during report describe._

## Changelog

- 2026-04-06: Initial draft distilled from the KPI brief by Claude during the Phase 1 brainstorm session. Open questions listed; awaiting Andre review.
```

- [ ] **Step 4: Verify the spec file is complete**

```bash
wc -l ~/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md
```

Expected: roughly 100-130 lines.

- [ ] **Step 5: Grep for em-dashes (hard rule)**

```bash
grep -n '[—–]' ~/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md
```

Expected: no matches. If any found, replace with ASCII hyphens and re-run.

- [ ] **Step 6: Commit the spec by exact path**

```bash
cd ~/crm-analytics
git add docs/specs/sales-director-monthly-dashboard-spec.md
git status --short docs/specs/sales-director-monthly-dashboard-spec.md
```

Expected: `A  docs/specs/sales-director-monthly-dashboard-spec.md`

```bash
git commit -m "$(cat <<'EOF'
docs: expected-widgets spec for 01ZTb00000FSP7hMAH (sales director monthly)

Distilled from the KPI brief in the 2026-04-06 handoff doc. 14 expected
widgets across 5 KPI bullets: pipeline overview (4), commercial approval
(4), renewals tracking (3), churn risk placeholder (1), slipped deals
(2). Open questions documented at bottom of spec; assumptions noted for
each. This is the permanent contract Phase 1 audit grades against.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7: Verify the commit landed and is only 1 file**

```bash
git log -1 --stat docs/specs/sales-director-monthly-dashboard-spec.md
```

Expected: single file changed, ~100-130 insertions.

---

## Task 2: HUMAN GATE - Andre reviews the spec

**This task blocks all subsequent work.** The audit script grades against this spec. If the spec is wrong, every audit entry will be wrong.

- [ ] **Step 1: Present the spec to Andre**

Print to the session:

> "Expected-widgets spec is committed at `docs/specs/sales-director-monthly-dashboard-spec.md` (commit [hash]). 14 expected widgets. 5 open questions at the bottom of the file with my assumptions. Please review and either approve or tell me what to change. I will not touch the audit script until you sign off on this spec."

- [ ] **Step 2: Wait for Andre's response**

Possible outcomes:

- **Approved as-is** -> proceed to Task 3
- **Resolve one or more open questions** -> edit the spec, re-commit with a fixup commit, re-present
- **Rework one or more rows** -> edit the spec, re-commit with a fixup commit, re-present
- **Reject approach** -> stop, return to brainstorming skill

- [ ] **Step 3: Apply changes if any, commit by exact path, re-present**

Loop until Andre approves.

---

## Task 3: Create the audit script skeleton

**Files:**

- Create: `scripts/audit_sales_director_monthly_dashboard.py`

**Note:** This script stays UNCOMMITTED throughout the plan, per the design doc. It is a tool, not a deliverable.

- [ ] **Step 1: Create the script file with docstring, imports, and constants**

Create `~/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py` with:

```python
#!/usr/bin/env python3
"""Sales Director Monthly Dashboard - Phase 1 Audit.

Grades every widget on the standard Salesforce dashboard
01ZTb00000FSP7hMAH (Sales Directors Monthly - Pipeline and Insights)
against the expected-widgets spec at
docs/specs/sales-director-monthly-dashboard-spec.md.

Notebook style: cells separated by `# %%` markers. Re-run any cell
independently in VSCode interactive or via `python3 -i`.

Output: a two-table delta report at
docs/audits/<today>-sales-director-monthly-audit.md.

Design doc: docs/2026-04-06-sales-director-monthly-phase1-audit-design.md
"""

from __future__ import annotations

import datetime as dt
import html
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

# %% Constants

DASHBOARD_ID = "01ZTb00000FSP7hMAH"
SPEC_PATH = Path("/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md")
AUDIT_OUTPUT_DIR = Path("/Users/test/crm-analytics/docs/audits")
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
REPORT_RUN_TIMEOUT_SECONDS = 30

# Picklist the audit asserts is current
EXPECTED_PICKLIST_FIELD = "APTS_Primary_Quote_Type__c"
EXPECTED_PICKLIST_VALUES_PRESENT = {"SBL", "MBL", "PPL"}
EXPECTED_PICKLIST_VALUES_ABSENT = {"Quote", "Renewal"}
```

- [ ] **Step 2: Verify the file parses**

```bash
python3 -c "import ast; ast.parse(open('/Users/test/crm-analytics/scripts/audit_sales_director_monthly_dashboard.py').read())"
```

Expected: no output (success).

- [ ] **Step 3: Do NOT commit**

The script stays uncommitted for the entire plan. Same convention as the existing `audit_*.py` family in `scripts/` (verified via `ls -la scripts/audit_*.py` in the context exploration).

---

## Task 4: Cell 1 - Auth

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

**Pattern reference:** `scripts/simcorp_crma_chart_sample.py:48-56` (the working Option D POC, just re-run and confirmed alive).

- [ ] **Step 1: Append the auth function and cell marker**

```python

# %% Cell 1: Auth

def get_auth() -> tuple[str, str]:
    """Shell out to `sf org display` and extract instance URL + access token.

    Same pattern as the Option D POC. No .env files. No MCP.
    """
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(r.stdout[r.stdout.find("{"):])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


# Run the auth cell
inst, tok = get_auth()
print(f"Auth OK - instance: {inst}")
assert inst.startswith("https://simcorp.my.salesforce.com"), f"Unexpected instance: {inst}"
print(f"Token length: {len(tok)} chars")
```

- [ ] **Step 2: Run the auth cell in isolation**

```bash
cd ~/crm-analytics
python3 -c "
import sys
sys.path.insert(0, 'scripts')
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 2')[0])
"
```

Expected output:

```
Auth OK - instance: https://simcorp.my.salesforce.com
Token length: 112 chars   (or similar)
```

If instance does not start with `https://simcorp.my.salesforce.com`, stop and verify `sf org display --target-org apro@simcorp.com --json` manually.

---

## Task 5: Cell 2 - Picklist freshness assertion

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

**Why:** If the org has migrated the `APTS_Primary_Quote_Type__c` picklist again since the handoff doc was written, the audit's stale-picklist rule cannot be trusted. The script must exit rather than produce wrong severities.

- [ ] **Step 1: Append the picklist assertion function and cell**

```python

# %% Cell 2: Picklist freshness assertion

def fetch_picklist_values(inst: str, tok: str, sobject: str, field: str) -> set[str]:
    """GET the picklist values for a single field via the sobject describe API."""
    url = f"{inst}/services/data/{API_VERSION}/sobjects/{sobject}/describe"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    r.raise_for_status()
    describe = r.json()
    for f in describe.get("fields", []):
        if f.get("name") == field:
            return {pv["value"] for pv in f.get("picklistValues", []) if pv.get("active")}
    raise KeyError(f"Field {field} not found on {sobject}")


def assert_picklist_fresh(inst: str, tok: str) -> set[str]:
    """Exit the script if the picklist does not match the audit's expectations."""
    values = fetch_picklist_values(inst, tok, "Opportunity", EXPECTED_PICKLIST_FIELD)
    print(f"{EXPECTED_PICKLIST_FIELD} picklist values: {sorted(values)}")

    missing_required = EXPECTED_PICKLIST_VALUES_PRESENT - values
    unexpected_stale = EXPECTED_PICKLIST_VALUES_ABSENT & values

    if missing_required:
        print(f"ERROR: expected picklist values missing: {sorted(missing_required)}")
        print("The audit's stale-picklist rule cannot be trusted. Update the rule and re-run.")
        sys.exit(1)

    if unexpected_stale:
        print(f"ERROR: stale picklist values still present: {sorted(unexpected_stale)}")
        print("The picklist migration the audit assumes is complete did not happen. Update the rule and re-run.")
        sys.exit(1)

    print("Picklist freshness: OK")
    return values


# Run the assertion
picklist_values = assert_picklist_fresh(inst, tok)
```

- [ ] **Step 2: Run cells 1 and 2 in sequence**

```bash
cd ~/crm-analytics
python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 3')[0])
"
```

Expected output:

```
Auth OK - instance: https://simcorp.my.salesforce.com
Token length: 112 chars
APTS_Primary_Quote_Type__c picklist values: ['MBL', 'PPL', 'SBL']   (or similar; at minimum contains SBL, MBL, PPL)
Picklist freshness: OK
```

If exit 1, stop and surface to Andre: "The picklist the audit expects has shifted. Rule update needed before Phase 1 can run."

---

## Task 6: Cell 3 - Load and parse the expected spec (TDD)

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

**Why TDD for this cell:** the spec parser is pure logic with a clear input (markdown file) and output (dict of widget definitions). Bugs here silently corrupt every downstream comparison. Worth 3 asserts.

- [ ] **Step 1: Append the spec loader function**

```python

# %% Cell 3: Load expected spec

def load_expected_spec(path: Path) -> dict[str, dict[str, Any]]:
    """Parse the expected-widgets markdown table into a dict keyed by widget ID.

    Reads the spec file, finds the `## Expected widgets table` section,
    parses the markdown table, and returns a dict where each key is a
    Widget ID (column 2 after `#`) and each value is a dict of the other
    columns.
    """
    text = path.read_text()

    # Find the expected widgets table
    section_marker = "## Expected widgets table"
    if section_marker not in text:
        raise ValueError(f"Spec missing section: {section_marker}")

    section = text.split(section_marker, 1)[1]
    # Stop at the next ## heading
    section = section.split("\n## ", 1)[0]

    # Parse markdown table rows (lines that start with `|`)
    lines = [ln.strip() for ln in section.splitlines() if ln.strip().startswith("|")]
    if len(lines) < 3:
        raise ValueError(f"Expected widgets table has fewer than 3 markdown rows: {len(lines)}")

    # First line is header, second is the separator (`| --- | ...`), rest are data
    header = [c.strip() for c in lines[0].strip("|").split("|")]
    data_lines = [ln for ln in lines[2:] if not set(ln.strip("| ")) <= {"-", " "}]

    spec: dict[str, dict[str, Any]] = {}
    for line in data_lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != len(header):
            continue  # skip malformed lines
        row = dict(zip(header, cells))
        widget_id = row.get("Widget ID", "").strip("`")
        if not widget_id:
            continue
        spec[widget_id] = row

    return spec
```

- [ ] **Step 2: Append the inline test cell**

```python

# %% Cell 3 tests

# Test: the real spec parses into at least 14 widgets
_spec = load_expected_spec(SPEC_PATH)
assert len(_spec) >= 14, f"Expected at least 14 widgets in spec, got {len(_spec)}"
print(f"Loaded {len(_spec)} expected widgets from spec")

# Test: pipeline_overview_global exists and has the right KPI bullet
assert "pipeline_overview_global" in _spec, "Missing pipeline_overview_global in spec"
assert "Pipeline overview" in _spec["pipeline_overview_global"].get("KPI bullet", ""), (
    f"pipeline_overview_global KPI bullet wrong: {_spec['pipeline_overview_global']}"
)

# Test: renewal_acv_this_quarter uses ACV not ARR
agg = _spec["renewal_acv_this_quarter"].get("Aggregation", "")
assert "ACV" in agg, f"renewal_acv_this_quarter must use ACV, got: {agg}"

print("Cell 3 tests: PASS")
expected_spec = _spec
```

- [ ] **Step 3: Run cells 1, 2, 3 in sequence**

```bash
cd ~/crm-analytics
python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 4')[0])
"
```

Expected output includes:

```
Loaded 14 expected widgets from spec
Cell 3 tests: PASS
```

If any assert fails, fix `load_expected_spec` and re-run.

---

## Task 7: Cell 4 - Fetch dashboard describe

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

- [ ] **Step 1: Append the dashboard describe function**

```python

# %% Cell 4: Dashboard describe

def get_dashboard_describe(inst: str, tok: str, dashboard_id: str) -> dict[str, Any]:
    """GET the standard SF dashboard metadata via the Analytics REST API."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/dashboards/{dashboard_id}/describe"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    if r.status_code == 404:
        print(f"ERROR: dashboard {dashboard_id} not found (404)")
        sys.exit(1)
    r.raise_for_status()
    return r.json()


def extract_widgets(describe: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the list of components (widgets) with their report IDs.

    Analytics REST describe returns a nested structure with `components` or
    `componentIds` depending on API version. v66.0 returns `components`.
    """
    components = describe.get("components", [])
    widgets = []
    for c in components:
        widgets.append({
            "component_id": c.get("id"),
            "title": c.get("header", {}).get("title") or c.get("title") or "",
            "type": c.get("type"),  # Chart / Metric / Table / VisualizationProperties
            "report_id": c.get("reportId"),
            "footer": c.get("footer", {}).get("text", ""),
            "raw": c,
        })
    return widgets


# Run cell 4
dashboard_describe = get_dashboard_describe(inst, tok, DASHBOARD_ID)
dashboard_widgets = extract_widgets(dashboard_describe)
dashboard_last_modified = dashboard_describe.get("lastModifiedDate", "unknown")
print(f"Dashboard {DASHBOARD_ID} - {len(dashboard_widgets)} widgets - last modified {dashboard_last_modified}")
for i, w in enumerate(dashboard_widgets, 1):
    print(f"  {i:2d}. {w['type']:<10} {w['title'][:60]:<60} report={w['report_id']}")
```

- [ ] **Step 2: Run cells 1-4 and verify 16 widgets**

```bash
cd ~/crm-analytics
python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 5')[0])
"
```

Expected output includes:

```
Dashboard 01ZTb00000FSP7hMAH - 16 widgets - last modified 2026-04-06...
   1. Chart      ... report=00OTb...
   ...
  16. ...
```

If widget count is not 16, stop and investigate. Possible causes: the dashboard was modified since the handoff doc, or the describe endpoint returns components under a different key for this dashboard variant.

---

## Task 8: Cell 5 - Fetch report describes

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

- [ ] **Step 1: Append the report describe function**

```python

# %% Cell 5: Report describes

def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any] | None:
    """GET the report metadata. Returns None on any failure; caller records BLOCKING."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {tok}"},
            timeout=30,
        )
    except requests.RequestException as e:
        print(f"  report {report_id} describe failed: {e}")
        return None
    if r.status_code == 404:
        print(f"  report {report_id} describe: 404")
        return None
    if not r.ok:
        print(f"  report {report_id} describe: {r.status_code} {r.text[:200]}")
        return None
    return r.json()


def extract_report_meta(describe: dict[str, Any]) -> dict[str, Any]:
    """Flatten a report describe payload into the fields the audit needs."""
    if describe is None:
        return {"_failed": True}
    report_metadata = describe.get("reportMetadata", {})
    return {
        "_failed": False,
        "name": report_metadata.get("name"),
        "report_format": report_metadata.get("reportFormat"),  # TABULAR / SUMMARY / MATRIX
        "filters": report_metadata.get("reportFilters", []),
        "groupings_down": report_metadata.get("groupingsDown", []),
        "groupings_across": report_metadata.get("groupingsAcross", []),
        "aggregates": report_metadata.get("aggregates", []),
        "detail_columns": report_metadata.get("detailColumns", []),
        "raw": describe,
    }


# Run cell 5 - fetch describe for every unique report ID
report_meta_by_id: dict[str, dict[str, Any]] = {}
unique_report_ids = {w["report_id"] for w in dashboard_widgets if w["report_id"]}
print(f"Fetching describe for {len(unique_report_ids)} unique reports")
for rid in sorted(unique_report_ids):
    describe = get_report_describe(inst, tok, rid)
    report_meta_by_id[rid] = extract_report_meta(describe)
    failed = report_meta_by_id[rid]["_failed"]
    name = report_meta_by_id[rid].get("name", "?")
    fmt = report_meta_by_id[rid].get("report_format", "?")
    print(f"  {rid}  {'FAIL' if failed else 'OK  '}  {fmt:<8}  {name}")
```

- [ ] **Step 2: Run cells 1-5 and verify**

```bash
cd ~/crm-analytics
python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 6')[0])
"
```

Expected: every report has `OK` status and a report format (`TABULAR` / `SUMMARY` / `MATRIX`). Any `FAIL` lines are expected to become BLOCKING entries in the final audit.

---

## Task 9: Cell 6 - Run each report

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

**Note:** Running reports is the slowest cell. Budget: up to 16 \* 30s = 8 minutes worst case.

- [ ] **Step 1: Append the report run function**

```python

# %% Cell 6: Run reports

def run_report(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    """POST to the reports/{id}/instances endpoint in synchronous mode.

    Returns a dict with `_failed`, `_timeout`, and the top-line value if the run succeeded.
    """
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}?includeDetails=true"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {tok}"},
            timeout=REPORT_RUN_TIMEOUT_SECONDS,
        )
    except requests.Timeout:
        return {"_failed": True, "_timeout": True}
    except requests.RequestException as e:
        return {"_failed": True, "_timeout": False, "error": str(e)}

    if not r.ok:
        return {"_failed": True, "_timeout": False, "error": f"{r.status_code}: {r.text[:200]}"}

    payload = r.json()
    fact_map = payload.get("factMap", {})
    grand_total = fact_map.get("T!T", {})
    aggregates = grand_total.get("aggregates", [])
    top_value = aggregates[0].get("value") if aggregates else None
    row_count = len(grand_total.get("rows", []))

    return {
        "_failed": False,
        "_timeout": False,
        "top_value": top_value,
        "row_count": row_count,
        "has_detail": payload.get("hasDetailRows", False),
    }


# Run cell 6 - execute every unique report
report_run_by_id: dict[str, dict[str, Any]] = {}
print(f"Running {len(unique_report_ids)} reports (may take up to ~{len(unique_report_ids)*30}s)")
for rid in sorted(unique_report_ids):
    result = run_report(inst, tok, rid)
    report_run_by_id[rid] = result
    if result["_timeout"]:
        print(f"  {rid}  TIMEOUT")
    elif result["_failed"]:
        print(f"  {rid}  FAIL  {result.get('error', '?')[:60]}")
    else:
        print(f"  {rid}  OK    top={result['top_value']!r:<20} rows={result['row_count']}")
```

- [ ] **Step 2: Run cells 1-6 and verify**

```bash
cd ~/crm-analytics
timeout 600 python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 7')[0])
"
```

Expected: all reports show `OK` or a documented `FAIL` / `TIMEOUT`. Each `TIMEOUT` / `FAIL` will become a BLOCKING entry later. Sanity check: the top-value numbers should not all be `None` or `0`.

---

## Task 10: Cell 7 - Static rule scan (TDD)

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

**Why TDD for this cell:** the static rules are the heart of the audit's judgment. Bugs here produce wrong severities silently. Worth a handful of inline asserts with fixture report metadata.

- [ ] **Step 1: Append the static rules function**

```python

# %% Cell 7: Static rule scan

def apply_static_rules(report_meta: dict[str, Any], widget: dict[str, Any]) -> list[dict[str, str]]:
    """Return a list of issues found on this report / widget pair.

    Each issue is {"severity": ..., "rule": ..., "detail": ...}.
    """
    issues: list[dict[str, str]] = []
    if report_meta.get("_failed"):
        issues.append({
            "severity": "BLOCKING",
            "rule": "report_describe_failed",
            "detail": "Report describe API call failed",
        })
        return issues

    fmt = (report_meta.get("report_format") or "").upper()
    filters = report_meta.get("filters") or []
    title = (widget.get("title") or "").lower()

    # Rule 1: stale picklist on APTS_Primary_Quote_Type__c
    for f in filters:
        col = (f.get("column") or "").lower()
        val = str(f.get("value") or "")
        if "apts_primary_quote_type" in col:
            for stale in EXPECTED_PICKLIST_VALUES_ABSENT:
                if stale in val:
                    issues.append({
                        "severity": "BLOCKING",
                        "rule": "stale_picklist",
                        "detail": f"Filter on APTS_Primary_Quote_Type__c uses stale value '{stale}'. Switch to Type field.",
                    })

    # Rule 2: TABULAR format on a Top N widget
    if ("top" in title and any(ch.isdigit() for ch in title)) and fmt == "TABULAR":
        issues.append({
            "severity": "WRONG-DATA",
            "rule": "tabular_top_n",
            "detail": "Widget name contains 'Top N' but underlying report is TABULAR. Should be SUMMARY grouped by the top-N dimension.",
        })

    # Rule 3: Missing X report without showing X in detail columns
    if title.startswith("missing "):
        missing_field_hint = title.replace("missing ", "").strip()
        columns = [c.lower() for c in (report_meta.get("detail_columns") or [])]
        hinted = any(missing_field_hint in col or col in missing_field_hint for col in columns)
        if not hinted:
            issues.append({
                "severity": "WRONG-DATA",
                "rule": "missing_field_not_shown",
                "detail": f"Widget is 'Missing {missing_field_hint}' but the field is not in the detail columns",
            })

    # Rule 4: em-dash in widget title
    if "\u2014" in (widget.get("title") or "") or "\u2013" in (widget.get("title") or ""):
        issues.append({
            "severity": "COSMETIC",
            "rule": "em_dash_in_title",
            "detail": "Widget title contains an em-dash or en-dash. Replace with a hyphen.",
        })

    return issues


# %% Cell 7 tests

_fake_report_stale = {
    "_failed": False,
    "report_format": "SUMMARY",
    "filters": [{"column": "APTS_Primary_Quote_Type__c", "operator": "equals", "value": "Renewal"}],
    "detail_columns": ["Account.Name"],
}
_fake_widget_stale = {"title": "Renewal pipeline by quarter"}
issues = apply_static_rules(_fake_report_stale, _fake_widget_stale)
assert any(i["rule"] == "stale_picklist" for i in issues), f"Expected stale_picklist, got {issues}"

_fake_report_topn = {
    "_failed": False,
    "report_format": "TABULAR",
    "filters": [],
    "detail_columns": ["Account.Name", "ARR"],
}
_fake_widget_topn = {"title": "Top 20 Accounts by ARR"}
issues = apply_static_rules(_fake_report_topn, _fake_widget_topn)
assert any(i["rule"] == "tabular_top_n" for i in issues), f"Expected tabular_top_n, got {issues}"

_fake_report_ok = {
    "_failed": False,
    "report_format": "SUMMARY",
    "filters": [{"column": "Type", "operator": "equals", "value": "Renewal"}],
    "detail_columns": ["Account.Name", "ACV"],
}
_fake_widget_ok = {"title": "Renewal ACV by quarter"}
issues = apply_static_rules(_fake_report_ok, _fake_widget_ok)
assert issues == [], f"Expected no issues, got {issues}"

print("Cell 7 tests: PASS")

# Apply rules to every real widget
static_issues_by_widget: dict[str, list[dict[str, str]]] = {}
for w in dashboard_widgets:
    rid = w["report_id"]
    rmeta = report_meta_by_id.get(rid, {"_failed": True})
    static_issues_by_widget[w["component_id"]] = apply_static_rules(rmeta, w)

total_static = sum(len(v) for v in static_issues_by_widget.values())
print(f"Static rule scan: {total_static} issues across {len(dashboard_widgets)} widgets")
```

- [ ] **Step 2: Run cells 1-7**

```bash
cd ~/crm-analytics
timeout 600 python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 8')[0])
"
```

Expected: `Cell 7 tests: PASS` line and a static issue tally. If any fixture test fails, fix the rule and re-run.

---

## Task 11: Cell 8 - Bidirectional comparison (TDD)

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

**Why TDD for this cell:** bidirectional matching (spec-to-dashboard AND dashboard-to-spec) is easy to get half-wrong in a way that only shows up in production. Small fixtures catch the classic bugs.

- [ ] **Step 1: Append the comparison function**

```python

# %% Cell 8: Bidirectional comparison

def match_widget_to_spec(
    widget: dict[str, Any],
    spec: dict[str, dict[str, Any]],
) -> str | None:
    """Very loose matching: widget title contains any keyword from a spec widget's KPI bullet.

    Returns the matched Widget ID from the spec, or None for orphans.
    Precise matching is not possible because widget titles drift; a human
    reviews the audit delta and confirms orphans.
    """
    title = (widget.get("title") or "").lower()
    if not title:
        return None

    best: tuple[int, str | None] = (0, None)
    for wid, row in spec.items():
        bullet = (row.get("KPI bullet") or "").lower()
        # Score: count of distinct words in the bullet that appear in the title
        bullet_words = {w for w in re.findall(r"\w+", bullet) if len(w) >= 4}
        if not bullet_words:
            continue
        hit = sum(1 for w in bullet_words if w in title)
        if hit > best[0]:
            best = (hit, wid)
    return best[1] if best[0] >= 2 else None


def compare(
    spec: dict[str, dict[str, Any]],
    dashboard_widgets: list[dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    report_run_by_id: dict[str, dict[str, Any]],
    report_meta_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Run both passes and return a flat list of audit entries."""
    entries: list[dict[str, Any]] = []
    matched_spec_ids: set[str] = set()

    # Pass 1: dashboard to spec
    for w in dashboard_widgets:
        matched = match_widget_to_spec(w, spec)
        if matched:
            matched_spec_ids.add(matched)
        static = static_issues_by_widget.get(w["component_id"], [])
        run = report_run_by_id.get(w["report_id"], {})
        meta = report_meta_by_id.get(w["report_id"], {})

        if not matched:
            severity = "ORPHAN"
            issue = "Dashboard widget does not map to any spec entry"
            fix = "Decision needed: keep (add to spec), drop, or fold into an existing spec row"
        elif static:
            severity = max(i["severity"] for i in static)  # crude: lexicographic, BLOCKING wins
            issue = "; ".join(i["detail"] for i in static)
            fix = "See static rule detail"
        elif run.get("_failed") or run.get("_timeout"):
            severity = "BLOCKING"
            issue = f"Report run failed: {'timeout' if run.get('_timeout') else run.get('error', 'unknown')}"
            fix = "Fix the report or its filter"
        elif meta.get("_failed"):
            severity = "BLOCKING"
            issue = "Report describe failed"
            fix = "Verify report ID is valid and accessible"
        else:
            severity = "OK"
            issue = "Matches spec"
            fix = "n/a"

        entries.append({
            "severity": severity,
            "widget_title": w["title"],
            "widget_type": w["type"],
            "component_id": w["component_id"],
            "report_id": w["report_id"],
            "matched_spec_id": matched,
            "kpi_bullet": spec.get(matched, {}).get("KPI bullet", "") if matched else "(orphan)",
            "issue": issue,
            "fix": fix,
            "current_value": run.get("top_value"),
            "row_count": run.get("row_count"),
            "report_format": meta.get("report_format"),
        })

    # Pass 2: spec to dashboard (find missing widgets)
    for wid, row in spec.items():
        if wid not in matched_spec_ids:
            entries.append({
                "severity": "BLOCKING",
                "widget_title": f"(MISSING) {wid}",
                "widget_type": row.get("Type", ""),
                "component_id": None,
                "report_id": None,
                "matched_spec_id": wid,
                "kpi_bullet": row.get("KPI bullet", ""),
                "issue": "Spec requires this widget; dashboard does not have it",
                "fix": f"Add widget {wid} with filters {row.get('Required filters', '')}",
                "current_value": None,
                "row_count": None,
                "report_format": None,
            })

    return entries


# %% Cell 8 tests

_tiny_spec = {
    "widget_a": {"KPI bullet": "Pipeline overview with quarterly focus", "Required filters": "IsClosed=false"},
    "widget_b": {"KPI bullet": "Renewals tracking", "Required filters": "Type=Renewal"},
}
_tiny_dashboard = [
    {"component_id": "c1", "title": "Pipeline overview (EMEA quarterly)", "type": "chart", "report_id": "r1"},
    {"component_id": "c2", "title": "Random unrelated tile", "type": "metric", "report_id": "r2"},
]
_tiny_static = {"c1": [], "c2": []}
_tiny_runs = {"r1": {"_failed": False, "_timeout": False, "top_value": 100}, "r2": {"_failed": False, "_timeout": False, "top_value": 50}}
_tiny_metas = {"r1": {"_failed": False, "report_format": "SUMMARY"}, "r2": {"_failed": False, "report_format": "SUMMARY"}}

_entries = compare(_tiny_spec, _tiny_dashboard, _tiny_static, _tiny_runs, _tiny_metas)

# c1 should match widget_a and be OK
c1 = [e for e in _entries if e["component_id"] == "c1"]
assert len(c1) == 1 and c1[0]["severity"] == "OK", f"c1 should be OK, got {c1}"

# c2 should be ORPHAN (no KPI bullet match)
c2 = [e for e in _entries if e["component_id"] == "c2"]
assert len(c2) == 1 and c2[0]["severity"] == "ORPHAN", f"c2 should be ORPHAN, got {c2}"

# widget_b should appear as a MISSING BLOCKING entry
missing = [e for e in _entries if e["matched_spec_id"] == "widget_b" and e["component_id"] is None]
assert len(missing) == 1 and missing[0]["severity"] == "BLOCKING", f"widget_b should be MISSING BLOCKING, got {missing}"

print("Cell 8 tests: PASS")

# Apply to the real data
audit_entries = compare(
    expected_spec,
    dashboard_widgets,
    static_issues_by_widget,
    report_run_by_id,
    report_meta_by_id,
)
tally: dict[str, int] = {}
for e in audit_entries:
    tally[e["severity"]] = tally.get(e["severity"], 0) + 1
print(f"Audit tally: {tally}")
```

- [ ] **Step 2: Run cells 1-8**

```bash
cd ~/crm-analytics
timeout 600 python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 9')[0])
"
```

Expected: `Cell 8 tests: PASS` and an audit tally summarizing the real run.

---

## Task 12: Cell 9 - Markdown rendering (TDD-lite)

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

- [ ] **Step 1: Append the markdown rendering function**

````python

# %% Cell 9: Markdown rendering

SEVERITY_ORDER = ["BLOCKING", "WRONG-DATA", "ORPHAN", "COSMETIC", "OK"]


def render_markdown(
    entries: list[dict[str, Any]],
    dashboard_id: str,
    dashboard_last_modified: str,
    spec_path: Path,
    tally: dict[str, int],
    rundate: str,
) -> str:
    """Build the full delta report markdown string."""
    lines: list[str] = []
    lines.append(f"# Sales Director Monthly Dashboard Audit - {rundate}")
    lines.append("")
    lines.append("## Header")
    lines.append("")
    lines.append(f"- **Dashboard ID:** `{dashboard_id}`")
    lines.append(f"- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/{dashboard_id}/view")
    lines.append(f"- **Dashboard lastModifiedDate:** {dashboard_last_modified}")
    lines.append(f"- **Audit run date:** {rundate}")
    lines.append(f"- **Spec graded against:** `{spec_path.relative_to(Path('/Users/test/crm-analytics'))}`")
    lines.append(f"- **Audit script:** `scripts/audit_sales_director_monthly_dashboard.py`")
    tally_line = " - ".join(f"{tally.get(s, 0)} {s}" for s in SEVERITY_ORDER)
    lines.append(f"- **Tally:** {len(entries)} entries - {tally_line}")
    lines.append("")

    lines.append("## Table 1: Executive summary")
    lines.append("")
    lines.append("| Severity | Widget | KPI bullet | Issue | Recommended fix |")
    lines.append("|---|---|---|---|---|")
    entries_sorted = sorted(
        entries,
        key=lambda e: (SEVERITY_ORDER.index(e["severity"]) if e["severity"] in SEVERITY_ORDER else 99, e["kpi_bullet"]),
    )
    for e in entries_sorted:
        lines.append(
            f"| {e['severity']} | {e['widget_title']} | {e['kpi_bullet']} | {e['issue']} | {e['fix']} |"
        )
    lines.append("")

    lines.append("## Table 2: Full appendix")
    lines.append("")
    lines.append("| # | Widget | Type | Component | Report ID | Format | Current value | Rows | Matched spec ID | KPI bullet | Severity | Issue |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, e in enumerate(entries_sorted, 1):
        lines.append(
            f"| {i} | {e['widget_title']} | {e['widget_type']} | "
            f"{e['component_id'] or ''} | {e['report_id'] or ''} | "
            f"{e['report_format'] or ''} | {e['current_value']} | {e['row_count']} | "
            f"{e['matched_spec_id'] or ''} | {e['kpi_bullet']} | {e['severity']} | {e['issue']} |"
        )
    lines.append("")

    lines.append("## Spec gaps surfaced during audit")
    lines.append("")
    lines.append("(populated from ORPHAN entries and MISSING-in-spec entries above)")
    lines.append("")
    lines.append("## Phase 2 / 3 / 4 implications")
    lines.append("")
    lines.append("- Phase 2: audit 01ZTb00000FSP9JMAX with the same script (different DASHBOARD_ID and SPEC_PATH constants)")
    lines.append("- Phase 3: for every OK entry in this audit, the current_value should match the corresponding CRMA dashboard step value")
    lines.append("- Phase 4: deck rebuild uses Option D; every slide chart should pull from the CRMA step that matches the BLOCKING-free widgets in this audit")
    lines.append("")
    lines.append("## Reproducibility")
    lines.append("")
    lines.append("```bash")
    lines.append(f"python3 scripts/audit_sales_director_monthly_dashboard.py")
    lines.append("```")
    lines.append("")
    return "\n".join(lines) + "\n"


# %% Cell 9 tests

_tiny_entries = [
    {"severity": "BLOCKING", "widget_title": "W1", "kpi_bullet": "Pipeline", "issue": "stale picklist", "fix": "switch to Type", "widget_type": "chart", "component_id": "c1", "report_id": "r1", "current_value": None, "row_count": 0, "matched_spec_id": "w1", "report_format": "SUMMARY"},
    {"severity": "OK", "widget_title": "W2", "kpi_bullet": "Renewals", "issue": "Matches spec", "fix": "n/a", "widget_type": "metric", "component_id": "c2", "report_id": "r2", "current_value": 1000000, "row_count": 1, "matched_spec_id": "w2", "report_format": "SUMMARY"},
]
_md = render_markdown(
    _tiny_entries, "01ZTbTEST", "2026-04-06", SPEC_PATH,
    {"BLOCKING": 1, "OK": 1}, "2026-04-07",
)
assert "Table 1: Executive summary" in _md
assert "Table 2: Full appendix" in _md
assert "| BLOCKING |" in _md
assert "| OK |" in _md
# BLOCKING should come before OK in the sorted output
assert _md.index("| BLOCKING |") < _md.index("| OK |"), "severity sort order wrong"

print("Cell 9 tests: PASS")
````

- [ ] **Step 2: Run cells 1-9**

```bash
cd ~/crm-analytics
timeout 600 python3 -c "
exec(open('scripts/audit_sales_director_monthly_dashboard.py').read().split('# %% Cell 10')[0])
"
```

Expected: `Cell 9 tests: PASS` among other output.

---

## Task 13: Cell 10 - Composition (write the audit file)

**Files:**

- Modify: `scripts/audit_sales_director_monthly_dashboard.py` (append)

- [ ] **Step 1: Append the composition cell**

```python

# %% Cell 10: Composition

def main() -> None:
    rundate = dt.date.today().isoformat()
    md = render_markdown(
        audit_entries,
        DASHBOARD_ID,
        dashboard_last_modified,
        SPEC_PATH,
        tally,
        rundate,
    )
    out_path = AUDIT_OUTPUT_DIR / f"{rundate}-sales-director-monthly-audit.md"
    out_path.write_text(md)
    print(f"Wrote {out_path}")
    print(f"Tally: {len(audit_entries)} entries - " + " - ".join(f"{tally.get(s, 0)} {s}" for s in SEVERITY_ORDER))


if __name__ == "__main__":
    main()
else:
    # Allow the script to be run cell-by-cell without triggering main()
    pass
```

- [ ] **Step 2: Run the full script end to end**

```bash
cd ~/crm-analytics
timeout 600 python3 scripts/audit_sales_director_monthly_dashboard.py
```

Expected output ends with:

```
Wrote /Users/test/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
Tally: 16 entries - X BLOCKING - Y WRONG-DATA - Z ORPHAN - ... OK
```

(Rundate will be whatever today is when the script runs.)

- [ ] **Step 3: Verify the output file exists**

```bash
ls -la ~/crm-analytics/docs/audits/
```

Expected: one `.md` file dated today.

---

## Task 14: Spot-check validation (per design Section 5)

**Files:**

- Read: `docs/audits/<today>-sales-director-monthly-audit.md`

**Why:** the design's validation strategy requires verifying the audit's judgment against at least one known-correct and one known-broken widget before trusting the full run.

- [ ] **Step 1: Read the audit output file**

```bash
cat ~/crm-analytics/docs/audits/$(date -u +%Y-%m-%d)-sales-director-monthly-audit.md
```

- [ ] **Step 2: Find the row for report `00OTb000008eksLMAQ` (Renewals by Fiscal Quarter)**

This was one of the 10 corrected reports Andre swapped in last session. Expected severity: `OK` (it uses the `Type` field).

If it is flagged BLOCKING or WRONG-DATA, investigate: either the rule is wrong or the corrected report has a real issue. Stop, diagnose, and fix before proceeding.

- [ ] **Step 3: Find the row for any widget whose report is a BOB-clone (not in the corrected-10 list)**

Expected severity: BLOCKING or WRONG-DATA (these are the broken ones we know about).

If everything is `OK`, the static rules are not firing. Stop, diagnose, and fix.

- [ ] **Step 4: Confirm the tally is sensible**

Expected: some mix of severities, not all `OK` and not all `BLOCKING`. If all are one severity, something is wrong.

---

## Task 15: Run the full audit (re-run after spot-check fixes if any)

**Files:**

- Modify: `docs/audits/<today>-sales-director-monthly-audit.md` (overwritten)

- [ ] **Step 1: Re-run the full script**

```bash
cd ~/crm-analytics
timeout 600 python3 scripts/audit_sales_director_monthly_dashboard.py
```

- [ ] **Step 2: Grep the output file for em-dashes (hard rule)**

```bash
grep -n '[—–]' ~/crm-analytics/docs/audits/$(date -u +%Y-%m-%d)-sales-director-monthly-audit.md
```

Expected: no matches. If any, fix the render function and re-run the script.

---

## Task 16: Commit the audit output

**Files:**

- Commit: `docs/audits/<today>-sales-director-monthly-audit.md`

**Do NOT commit:** `scripts/audit_sales_director_monthly_dashboard.py` (stays uncommitted per design).

- [ ] **Step 1: Determine the exact path**

```bash
AUDIT_FILE=docs/audits/$(date -u +%Y-%m-%d)-sales-director-monthly-audit.md
echo "$AUDIT_FILE"
```

- [ ] **Step 2: Check status before staging**

```bash
cd ~/crm-analytics
git status --short "$AUDIT_FILE"
```

Expected: `?? docs/audits/YYYY-MM-DD-sales-director-monthly-audit.md`

- [ ] **Step 3: Get the spec commit hash (for the commit message)**

```bash
cd ~/crm-analytics
SPEC_HASH=$(git log -1 --format=%h -- docs/specs/sales-director-monthly-dashboard-spec.md)
echo "Spec commit: $SPEC_HASH"
```

- [ ] **Step 4: Stage by exact path**

```bash
cd ~/crm-analytics
git add "$AUDIT_FILE"
git status --short "$AUDIT_FILE"
```

Expected: `A  docs/audits/YYYY-MM-DD-sales-director-monthly-audit.md`

- [ ] **Step 5: Commit by exact path**

```bash
cd ~/crm-analytics
git commit -m "$(cat <<EOF
docs: phase 1 audit of 01ZTb00000FSP7hMAH against expected-widgets spec

Graded all 16 widgets on the Sales Director Monthly dashboard against
the expected-widgets spec (commit ${SPEC_HASH}). Output is a two-table
delta report: executive summary plus full appendix. See the tally line
at the top of the file for BLOCKING / WRONG-DATA / ORPHAN / COSMETIC /
OK counts.

Audit tool: scripts/audit_sales_director_monthly_dashboard.py (uncommitted
by convention, matches audit_*.py family).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Verify the commit landed and is only 1 file**

```bash
cd ~/crm-analytics
git log -1 --stat "$AUDIT_FILE"
```

Expected: single file changed, ~50-150 insertions depending on issue count.

- [ ] **Step 7: Do NOT push**

Per Andre's hard rule #6. No push unless explicitly requested.

---

## Task 17: HUMAN GATE - Andre reviews the audit

**This task blocks Phase 1.5 (fixing the issues).** Phase 1 ends when Andre has read the audit and confirmed the next move.

- [ ] **Step 1: Present the audit to Andre**

Print to the session:

> "Phase 1 audit complete. Output at `docs/audits/<rundate>-sales-director-monthly-audit.md`, committed as [hash]. Tally: N entries - X BLOCKING - Y WRONG-DATA - Z ORPHAN - K COSMETIC - J OK. Please read the executive summary (Table 1) and tell me which issues to tackle in Phase 1.5, which to defer, and which to escalate. I will not start Phase 1.5 until you give direction."

- [ ] **Step 2: Wait for Andre's direction**

Possible outcomes:

- **Approved, here is the Phase 1.5 scope** -> start a new brainstorm or skip to writing-plans for Phase 1.5
- **Re-run with rule fixes** -> diagnose, fix rule in the script, re-run Task 15 and Task 16
- **Spec was wrong, re-draft** -> return to Task 1, edit spec, re-run from Task 15 forward
- **Move to Phase 2** -> change `DASHBOARD_ID` and `SPEC_PATH` constants, run the script against `01ZTb00000FSP9JMAX` (after producing the Report 2 spec)

---

## Self-review notes

After writing this plan, I checked it against the committed design doc at `docs/2026-04-06-sales-director-monthly-phase1-audit-design.md`:

**Spec coverage check:** Every section of the design has at least one task.

- Goal and scope: Task 1, Task 13, Task 16
- Expected-widgets spec format: Task 1 (4 sections match)
- Audit methodology cell sequence: Tasks 4-13 (one task per cell, plus cell 7 renumbered to cell 3 for spec load - tracked consistently)
- Failure modes handled inline: Task 5 (picklist), Task 8 (report describe), Task 9 (report run)
- Delta report format: Task 12 (render), Task 13 (composition)
- Validation strategy: Task 14 (spot-check), cells 3/7/8/9 have inline test cells
- Commit discipline: Task 1 Step 6, Task 16 (exact path), plus explicit "do not commit" notes for the script
- Exit criteria: Task 16 (audit committed), Task 17 (Andre sign-off)

**Placeholder scan:** No "TBD", "TODO", "implement later", "add appropriate error handling" remaining. One `TBD` appears in the spec table for drilldown report IDs (widget rows 1-14), which is intentional and documented (drilldown IDs are collected during the audit itself, not pre-filled). No TBDs in code or step instructions.

**Type consistency:** Function names, dict key names, and severity strings match across tasks:

- `expected_spec` (dict) defined in Task 6, used in Task 11
- `dashboard_widgets` (list of dicts) defined in Task 7, used in Tasks 10, 11, 12
- `report_meta_by_id` defined in Task 8, used in Tasks 10, 11
- `report_run_by_id` defined in Task 9, used in Task 11
- `static_issues_by_widget` defined in Task 10, used in Task 11
- `audit_entries` defined in Task 11, used in Tasks 12, 13
- Severity strings: `BLOCKING`, `WRONG-DATA`, `ORPHAN`, `COSMETIC`, `OK` used identically across Tasks 10, 11, 12

**Scope check:** One focused plan implementing one phase of one dashboard. No multi-subsystem split needed.

**Ambiguity check:** "widget title" matching is intentionally loose in `match_widget_to_spec` (score >= 2 word overlap). Task 14 (spot-check) is explicitly there to catch any mismatches this looseness introduces.
