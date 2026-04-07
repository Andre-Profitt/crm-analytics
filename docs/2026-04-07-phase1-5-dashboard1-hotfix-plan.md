# Phase 1.5 - Dashboard 1 Hotfix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Patch Dashboard 1 (`01ZTb00000FSP7hMAH`) to fix the 3 renewal widgets aggregating `Amount` instead of `APTS_Renewal_ACV__c`, swap the ~12-15 fiscal date filters to calendar, and append a new `commercial_approval_approved_ytd` component referencing existing report `00OTb000008aTtJMAU`. Re-run the Phase 1 audit against the post-patch state and commit the new audit output by exact path (overwriting `6cbe8fe`).

**Architecture:** Two new uncommitted Python scripts. `scripts/phase1_5_patch_shape_probe.py` runs first to empirically determine the Analytics REST API PATCH body shape for standard SF reports. `scripts/phase1_5_patch_dashboard1.py` is the main notebook-style script that backs up every affected entity to `/tmp/phase1_5_backup/`, applies the patches in three cells (ARR with inline verification, fiscal batch, dashboard component), and supports `--dry-run` for safety. A final audit re-run validates the post-patch tally and gets committed.

**Tech Stack:** Python 3.13, `requests`, `subprocess`, `argparse`, `json`, `pathlib`. Auth via `sf org display --target-org apro@simcorp.com --json`. API v66.0. Same patterns as the Phase 1 audit script.

**Design doc (input):** `docs/2026-04-07-phase1-5-dashboard1-hotfix-design.md` (commit `8943fe5`). Read it first if you have not.

**Spec inputs (read-only, already committed):**

- `docs/specs/sales-director-monthly-dashboard-spec.md` (commit `8c81d2d`, 16-widget Report 1 spec)
- `docs/audits/2026-04-07-sales-director-monthly-audit.md` (commit `6cbe8fe`, the pre-patch audit)
- `docs/specs/report-1-source-contract.md` (commit `5ac6d62`, for widget 6 source ID confirmation)

---

## File structure

```
crm-analytics/
|-- docs/
|   |-- 2026-04-07-phase1-5-dashboard1-hotfix-design.md  (committed at 8943fe5)
|   |-- 2026-04-07-phase1-5-dashboard1-hotfix-plan.md     (this file)
|   `-- audits/
|       `-- 2026-04-07-sales-director-monthly-audit.md    (Task 11, OVERWRITES commit 6cbe8fe)
|-- scripts/
|   |-- audit_sales_director_monthly_dashboard.py         (parameterized in Phase 2 Task 1, UNTOUCHED)
|   |-- phase1_5_patch_shape_probe.py                     (Task 2, NEW, uncommitted)
|   `-- phase1_5_patch_dashboard1.py                      (Tasks 4-7, NEW, uncommitted)
`-- (/tmp/phase1_5_backup/ and /tmp/phase1_5_probe/ are ephemeral, not in repo)
```

Responsibilities:

- **`scripts/phase1_5_patch_shape_probe.py`** - Pre-flight probe. Empirically confirms the Analytics REST API PATCH body shape for standard SF reports in this org by patching and un-patching a cosmetic field on one low-risk OK widget. Writes the confirmed shape to `/tmp/phase1_5_probe/confirmed_shape.json`.
- **`scripts/phase1_5_patch_dashboard1.py`** - Main patch script. Notebook-style cells for auth, backup, 3 ARR PATCHes with inline verification, N fiscal PATCHes (batch), 1 dashboard component PATCH (batch), summary. Supports `--dry-run`.
- **`docs/audits/2026-04-07-sales-director-monthly-audit.md`** - Post-patch audit output. Same exact path as `6cbe8fe` (committed in Phase 2 Task 2). Phase 1.5 overwrites it with a new tally and commits.

---

## Task 0: Pre-flight verification

**Files:** none modified. Read-only checks + directory scaffolding.

- [ ] **Step 1: Verify the Phase 2 audit exists at expected commit**

```bash
cd ~/crm-analytics && git log -1 --format=%h -- docs/audits/2026-04-07-sales-director-monthly-audit.md
```

Expected: `6cbe8fe` (the Phase 2 Task 2 Report 1 audit).

- [ ] **Step 2: Verify the Report 1 spec is at commit 8c81d2d**

```bash
cd ~/crm-analytics && git log -1 --format=%h -- docs/specs/sales-director-monthly-dashboard-spec.md
```

Expected: `8c81d2d`.

- [ ] **Step 3: Verify the parameterized audit script exists and has --help working**

```bash
cd ~/crm-analytics && python3 scripts/audit_sales_director_monthly_dashboard.py --help 2>&1 | head -5
```

Expected: argparse usage block showing `--dashboard-id`, `--spec-path`, `--output-name`.

- [ ] **Step 4: Verify live org reachability via sf CLI**

```bash
sf org display --target-org apro@simcorp.com --json 2>&1 | python3 -c "import sys, json; d=json.load(sys.stdin)['result']; print(f'Instance: {d[\"instanceUrl\"]}'); print(f'Token present: {bool(d.get(\"accessToken\"))}')"
```

Expected: `Instance: https://simcorp.my.salesforce.com` and `Token present: True`.

- [ ] **Step 5: Create ephemeral working directories**

```bash
mkdir -p /tmp/phase1_5_probe /tmp/phase1_5_backup/reports /tmp/phase1_5_backup/dashboards
ls -la /tmp/phase1_5_probe /tmp/phase1_5_backup/reports /tmp/phase1_5_backup/dashboards
```

Expected: three empty directories listed.

- [ ] **Step 6: No commit.** Read-only + directory scaffolding only.

---

## Task 1: Enumerate affected reports from the Phase 2 audit

**Files:**

- Create: `/tmp/phase1_5_probe/targets.json` (ephemeral, machine-readable list of report IDs Phase 1.5 will touch)

This task reads the `6cbe8fe` audit file and extracts three lists of Report IDs: (1) the 3 reports flagged with the `renewal_uses_amount_not_acv` static rule, (2) the reports flagged with the `fiscal_date_filter` static rule, (3) one candidate OK report for the PATCH shape probe. Output: a single JSON file the subsequent tasks consume.

- [ ] **Step 1: Read the audit file and extract the full table rows**

```bash
cd ~/crm-analytics && grep -E "^\| [0-9]+ \|" docs/audits/2026-04-07-sales-director-monthly-audit.md | head -30
```

Expected: markdown table rows with columns `# | Widget | Type | Component | Report ID | Report name | ... | Severity | Issue`. Note the column order so the extraction script can parse it.

- [ ] **Step 2: Write a small extraction script inline and run it**

```bash
python3 - <<'PY' | tee /tmp/phase1_5_probe/targets.json
import json, re
audit_path = "/Users/test/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md"
with open(audit_path) as f:
    text = f.read()

# Extract all table rows from the Full appendix section (between "## Table 2: Full appendix" and the next "##")
appendix_match = re.search(r"## Table 2: Full appendix(.*?)^## ", text, re.DOTALL | re.MULTILINE)
appendix = appendix_match.group(1) if appendix_match else text

rows = [ln.strip() for ln in appendix.splitlines() if ln.strip().startswith("| ") and re.match(r"^\|\s*\d+\s*\|", ln)]
arr_targets = []   # reports with renewal_uses_amount_not_acv
fiscal_targets = [] # reports with fiscal_date_filter
ok_candidates = [] # reports with OK severity and a non-empty reportId (for probe target)

for row in rows:
    cells = [c.strip() for c in row.strip("|").split("|")]
    # Defensive: skip malformed rows
    if len(cells) < 13:
        continue
    # Typical column order: # | Widget | Type | Component | Report ID | Report name | Format | Date filter | Current value | Matched spec ID | KPI bullet | Severity | Issue
    report_id = cells[4]
    severity = cells[11] if len(cells) > 11 else ""
    issue = cells[12] if len(cells) > 12 else ""
    widget_title = cells[1]

    if not report_id or report_id in ("-", ""):
        continue

    if "renewal_uses_amount_not_acv" in issue.lower():
        arr_targets.append({"report_id": report_id, "widget_title": widget_title, "issue": issue[:120]})
    if "fiscal_date_filter" in issue.lower() or "fiscal" in cells[7].lower() and "THIS_FISCAL" in cells[7]:
        fiscal_targets.append({"report_id": report_id, "widget_title": widget_title, "date_filter": cells[7], "issue": issue[:120]})
    if severity == "OK":
        ok_candidates.append({"report_id": report_id, "widget_title": widget_title})

# Deduplicate by report_id (some reports are referenced by multiple components)
seen = set()
def dedupe(lst):
    out = []
    for item in lst:
        if item["report_id"] not in seen:
            seen.add(item["report_id"])
            out.append(item)
    return out
# Reset seen between lists
seen = set(); arr_targets = dedupe(arr_targets)
seen = set(); fiscal_targets = dedupe(fiscal_targets)
seen = set(); ok_candidates = dedupe(ok_candidates)

result = {
    "arr_targets": arr_targets,
    "fiscal_targets": fiscal_targets,
    "probe_target": ok_candidates[0] if ok_candidates else None,
    "ok_candidates": ok_candidates,
}
print(json.dumps(result, indent=2))
PY
```

Expected: JSON output with `arr_targets` (3 entries), `fiscal_targets` (10-15 entries), `probe_target` (one of the 2 OK widgets).

- [ ] **Step 3: Verify the enumeration counts match expectations**

```bash
python3 -c "
import json
with open('/tmp/phase1_5_probe/targets.json') as f:
    t = json.load(f)
print(f'ARR targets: {len(t[\"arr_targets\"])}')
print(f'Fiscal targets: {len(t[\"fiscal_targets\"])}')
print(f'Probe target: {t[\"probe_target\"]}')
print(f'Total unique reports: {len(t[\"arr_targets\"]) + len(t[\"fiscal_targets\"])}')
"
```

Expected: ARR targets = 3 (matches Phase 1 finding of 3 renewal widgets using Amount). Fiscal targets between 10 and 15 (Phase 1 said "13 of 16 widgets use fiscal date filters"). Probe target is not None.

- [ ] **Step 4: STOP if counts are wrong**

If `arr_targets` is not exactly 3, the audit may have a different issue format than expected. STOP and escalate with the actual count and the first 2 arr_targets entries. Do NOT proceed to Task 2.

If `fiscal_targets` is zero, the audit may have a different issue format. STOP and escalate.

If `probe_target` is None, there are no OK-severity reports to probe against. STOP and escalate.

- [ ] **Step 5: No commit.** The targets file is ephemeral.

---

## Task 2: Write the PATCH shape probe script

**Files:**

- Create: `scripts/phase1_5_patch_shape_probe.py` (uncommitted by convention)

This task writes a standalone script that empirically confirms the Analytics REST API PATCH body shape for standard SF reports. The script picks the probe target from Task 1's targets file, GETs the current report metadata, attempts a PATCH with a cosmetic change to the `name` field, verifies the change landed via another GET, then PATCHes back to restore the original name.

- [ ] **Step 1: Create the script file**

Create `/Users/test/crm-analytics/scripts/phase1_5_patch_shape_probe.py` with the following content:

```python
#!/usr/bin/env python3
"""Phase 1.5 - PATCH shape probe.

Empirically confirms the Analytics REST API PATCH body shape for standard
SF reports in this org. Picks a low-risk OK widget from the Phase 2 audit
(via /tmp/phase1_5_probe/targets.json written by Task 1), GETs its
metadata, attempts a PATCH with a cosmetic name change, verifies, and
restores.

Uncommitted by convention (matches audit_*.py family pattern).

Success: writes the confirmed PATCH body shape to
/tmp/phase1_5_probe/confirmed_shape.json and prints it.
Failure: up to 5 retry attempts with progressively refined bodies.
If all 5 fail, exits non-zero with the last error body.

Run:
    python3 scripts/phase1_5_patch_shape_probe.py
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

TARGETS_PATH = Path("/tmp/phase1_5_probe/targets.json")
CONFIRMED_SHAPE_PATH = Path("/tmp/phase1_5_probe/confirmed_shape.json")
ORIGINAL_PATH = Path("/tmp/phase1_5_probe/original.json")
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"


def get_auth() -> tuple[str, str]:
    """Shell out to `sf org display` and extract (instanceUrl, accessToken)."""
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def load_probe_target() -> dict[str, Any]:
    """Read targets.json from Task 1 and return the probe_target dict."""
    if not TARGETS_PATH.exists():
        print(f"ERROR: {TARGETS_PATH} does not exist. Run Task 1 first.")
        sys.exit(1)
    with open(TARGETS_PATH) as f:
        targets = json.load(f)
    probe = targets.get("probe_target")
    if not probe or not probe.get("report_id"):
        print("ERROR: no probe_target in targets.json")
        sys.exit(1)
    return probe


def get_report_describe(
    inst: str, tok: str, report_id: str
) -> dict[str, Any]:
    """GET /analytics/reports/{id}/describe. Returns the full JSON body."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def try_patch_report(
    inst: str, tok: str, report_id: str, body: dict[str, Any]
) -> tuple[bool, int, str]:
    """PATCH /analytics/reports/{id} with the given body. Return (ok, status, body)."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}"
    try:
        r = requests.patch(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )
    except requests.RequestException as e:
        return False, 0, f"network error: {e}"
    return r.ok, r.status_code, r.text[:1000]


def main() -> None:
    print("Phase 1.5 PATCH shape probe")
    print("=" * 60)

    inst, tok = get_auth()
    print(f"Auth OK: {inst}")

    probe = load_probe_target()
    report_id = probe["report_id"]
    print(f"Probe target: {report_id}  ({probe.get('widget_title', '')})")

    # Step 1: GET original
    describe = get_report_describe(inst, tok, report_id)
    with open(ORIGINAL_PATH, "w") as f:
        json.dump(describe, f, indent=2)
    print(f"Original saved to {ORIGINAL_PATH}")

    original_metadata = describe.get("reportMetadata", {})
    original_name = original_metadata.get("name", "")
    print(f"Original name: {original_name!r}")

    # Build progressively-refined PATCH body attempts
    probe_name = f"{original_name} (probe)"
    attempts: list[tuple[str, dict[str, Any]]] = []

    # Attempt 1: wrapped in reportMetadata envelope, full metadata, name modified
    md1 = copy.deepcopy(original_metadata)
    md1["name"] = probe_name
    attempts.append(("wrapped_full", {"reportMetadata": md1}))

    # Attempt 2: wrapped, but strip known read-only fields (id, createdDate, lastModifiedDate, etc.)
    md2 = copy.deepcopy(original_metadata)
    md2["name"] = probe_name
    for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
        md2.pop(ro_field, None)
    attempts.append(("wrapped_stripped", {"reportMetadata": md2}))

    # Attempt 3: bare body (no envelope)
    md3 = copy.deepcopy(original_metadata)
    md3["name"] = probe_name
    attempts.append(("bare_full", md3))

    # Attempt 4: bare, stripped
    md4 = copy.deepcopy(original_metadata)
    md4["name"] = probe_name
    for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
        md4.pop(ro_field, None)
    attempts.append(("bare_stripped", md4))

    # Attempt 5: only name change, wrapped (sparse update)
    attempts.append(("sparse_name_only", {"reportMetadata": {"name": probe_name}}))

    confirmed_shape = None
    confirmed_label = None
    for label, body in attempts:
        print(f"\nAttempt {label}...")
        ok, status, resp = try_patch_report(inst, tok, report_id, body)
        print(f"  HTTP {status}")
        if ok:
            # Verify the change actually landed
            verify = get_report_describe(inst, tok, report_id)
            verify_name = verify.get("reportMetadata", {}).get("name", "")
            if verify_name == probe_name:
                print(f"  Verified: name changed to {probe_name!r}")
                confirmed_shape = body
                confirmed_label = label
                break
            else:
                print(f"  WARNING: PATCH returned 2xx but name did not change (got {verify_name!r})")
                continue
        else:
            print(f"  Response: {resp[:300]}")

    if confirmed_shape is None:
        print("\nALL 5 ATTEMPTS FAILED. Escalate to controller.")
        sys.exit(1)

    print(f"\nCONFIRMED body shape: {confirmed_label}")

    # Restore the original name using the SAME body shape
    print("\nRestoring original name...")
    restore_body: dict[str, Any]
    if confirmed_label.startswith("wrapped"):
        md_restore = copy.deepcopy(original_metadata)
        if "stripped" in confirmed_label:
            for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
                md_restore.pop(ro_field, None)
        restore_body = {"reportMetadata": md_restore}
    elif confirmed_label.startswith("bare"):
        md_restore = copy.deepcopy(original_metadata)
        if "stripped" in confirmed_label:
            for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
                md_restore.pop(ro_field, None)
        restore_body = md_restore
    elif confirmed_label == "sparse_name_only":
        restore_body = {"reportMetadata": {"name": original_name}}
    else:
        print(f"ERROR: unknown confirmed_label {confirmed_label!r}")
        sys.exit(1)

    ok, status, resp = try_patch_report(inst, tok, report_id, restore_body)
    if not ok:
        print(f"RESTORE FAILED: HTTP {status}: {resp[:300]}")
        print(f"Manual action required: PATCH {report_id} with name={original_name!r}")
        sys.exit(1)

    verify = get_report_describe(inst, tok, report_id)
    verify_name = verify.get("reportMetadata", {}).get("name", "")
    if verify_name != original_name:
        print(f"RESTORE VERIFY FAILED: name is {verify_name!r}, expected {original_name!r}")
        sys.exit(1)
    print(f"Verified: name restored to {original_name!r}")

    # Write the confirmed shape to disk
    shape_info = {
        "label": confirmed_label,
        "probe_report_id": report_id,
        "probe_timestamp": None,  # Not critical
        "description": (
            "Use this body shape for subsequent PATCH operations against "
            "standard SF reports. 'wrapped_*' means wrap in {\"reportMetadata\": ...}. "
            "'bare_*' means the metadata is the root of the body. "
            "'stripped' means remove read-only fields (id, createdDate, lastModifiedDate, lastRunDate) "
            "before sending."
        ),
    }
    with open(CONFIRMED_SHAPE_PATH, "w") as f:
        json.dump(shape_info, f, indent=2)
    print(f"\nConfirmed shape written to {CONFIRMED_SHAPE_PATH}")
    print(f"Label: {confirmed_label}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the file parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase1_5_patch_shape_probe.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 3: Verify the file is NOT staged or committed**

```bash
cd ~/crm-analytics && git status --short scripts/phase1_5_patch_shape_probe.py
```

Expected: `?? scripts/phase1_5_patch_shape_probe.py` (untracked).

- [ ] **Step 4: No commit.** Probe script stays uncommitted.

---

## Task 3: Run the PATCH shape probe

**Files:** produces `/tmp/phase1_5_probe/confirmed_shape.json` and `/tmp/phase1_5_probe/original.json`.

- [ ] **Step 1: Execute the probe**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_shape_probe.py 2>&1 | tee /tmp/phase1_5_probe/probe_run.log
```

Expected output: `Phase 1.5 PATCH shape probe`, auth OK, probe target printed, original saved, one of the 5 attempts marked "Verified: name changed", then "Verified: name restored". Exit 0.

- [ ] **Step 2: Verify the confirmed shape file exists**

```bash
cat /tmp/phase1_5_probe/confirmed_shape.json
```

Expected: JSON with `label` (one of `wrapped_full`, `wrapped_stripped`, `bare_full`, `bare_stripped`, `sparse_name_only`), `probe_report_id`, and a description.

- [ ] **Step 3: Verify the probe target's current name matches the original (full round-trip)**

```bash
python3 - <<'PY'
import json, subprocess, requests
r = subprocess.run(["sf","org","display","--target-org","apro@simcorp.com","--json"], capture_output=True, text=True, check=True)
d = json.loads(r.stdout[r.stdout.find("{"):])["result"]
inst, tok = d["instanceUrl"], d["accessToken"]
hdr = {"Authorization": f"Bearer {tok}"}

with open("/tmp/phase1_5_probe/targets.json") as f:
    targets = json.load(f)
probe_id = targets["probe_target"]["report_id"]

r = requests.get(f"{inst}/services/data/v66.0/analytics/reports/{probe_id}/describe", headers=hdr, timeout=30)
name = r.json().get("reportMetadata", {}).get("name", "")
print(f"Probe target current name: {name!r}")
print(f"Contains '(probe)': {'(probe)' in name}")
PY
```

Expected: name does NOT contain `(probe)` (the restore worked).

- [ ] **Step 4: STOP if the probe failed**

If the probe script exited non-zero, read `/tmp/phase1_5_probe/probe_run.log` and report the error back. Do NOT proceed to Task 4. The controller may need to refine the attempt list or pick a different probe target.

- [ ] **Step 5: No commit.** Probe outputs are ephemeral.

---

## Task 4: Write the main patch script scaffolding (cells 1, 2, 6 + argparse)

**Files:**

- Create: `scripts/phase1_5_patch_dashboard1.py` (uncommitted)

This task creates the skeleton of the main patch script with argparse, auth, backup, and summary cells. Cells 3, 4, 5 are added in subsequent tasks.

- [ ] **Step 1: Create the script file with the scaffolding**

Create `/Users/test/crm-analytics/scripts/phase1_5_patch_dashboard1.py` with:

```python
#!/usr/bin/env python3
"""Phase 1.5 - Dashboard 1 Hotfix Patcher.

Patches Dashboard 1 (01ZTb00000FSP7hMAH) to fix the defects surfaced by
the Phase 2 audit at commit 6cbe8fe:
- 3 renewal widgets aggregating Amount instead of APTS_Renewal_ACV__c
- ~12-15 reports with fiscal date filters (hard rule 1 violation)
- 1 missing spec widget: commercial_approval_approved_ytd

Backs up every affected report + Dashboard 1 to /tmp/phase1_5_backup/
before any PATCH. Supports --dry-run for safety.

Uncommitted by convention.

Design: docs/2026-04-07-phase1-5-dashboard1-hotfix-design.md (commit 8943fe5)
Plan:   docs/2026-04-07-phase1-5-dashboard1-hotfix-plan.md

Run:
    python3 scripts/phase1_5_patch_dashboard1.py --dry-run
    python3 scripts/phase1_5_patch_dashboard1.py
"""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

# %% Constants

DASHBOARD_ID = "01ZTb00000FSP7hMAH"
COMMERCIAL_APPROVAL_APPROVED_YTD_SOURCE_REPORT_ID = "00OTb000008aTtJMAU"
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
TARGETS_PATH = Path("/tmp/phase1_5_probe/targets.json")
CONFIRMED_SHAPE_PATH = Path("/tmp/phase1_5_probe/confirmed_shape.json")
BACKUP_DIR = Path("/tmp/phase1_5_backup")
REPORT_BACKUP_DIR = BACKUP_DIR / "reports"
DASHBOARD_BACKUP_DIR = BACKUP_DIR / "dashboards"

# Fiscal -> Calendar date filter mapping
FISCAL_TO_CALENDAR = {
    "THIS_FISCAL_YEAR": "THIS_CALENDAR_YEAR",
    "THIS_FISCAL_QUARTER": "THIS_CALENDAR_QUARTER",
    "LAST_FISCAL_YEAR": "LAST_CALENDAR_YEAR",
    "LAST_FISCAL_QUARTER": "LAST_CALENDAR_QUARTER",
    "NEXT_FISCAL_YEAR": "NEXT_CALENDAR_YEAR",
    "NEXT_FISCAL_QUARTER": "NEXT_CALENDAR_QUARTER",
}


# %% Argparse

def _parse_args():
    p = argparse.ArgumentParser(description="Phase 1.5 - Dashboard 1 hotfix patcher")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print PATCH bodies instead of sending them. Auth + backup still happen.",
    )
    return p.parse_args()


# %% Cell 1: Auth

def get_auth() -> tuple[str, str]:
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def _cell1_main() -> tuple[str, str]:
    inst, tok = get_auth()
    print(f"Cell 1 (auth): instance={inst}")
    return inst, tok


# %% Cell 2: Backup

def load_targets() -> dict[str, Any]:
    if not TARGETS_PATH.exists():
        print(f"ERROR: {TARGETS_PATH} does not exist. Run Task 1 first.")
        sys.exit(1)
    with open(TARGETS_PATH) as f:
        return json.load(f)


def load_confirmed_shape() -> dict[str, Any]:
    if not CONFIRMED_SHAPE_PATH.exists():
        print(f"ERROR: {CONFIRMED_SHAPE_PATH} does not exist. Run Task 3 first.")
        sys.exit(1)
    with open(CONFIRMED_SHAPE_PATH) as f:
        return json.load(f)


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def get_dashboard_describe(inst: str, tok: str, dashboard_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/dashboards/{dashboard_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def backup_all(inst: str, tok: str) -> tuple[list[str], list[str]]:
    """Backup every affected report + Dashboard 1 to /tmp/phase1_5_backup/.

    Returns (arr_ids, fiscal_ids) - the deduplicated report ID lists for
    downstream cells. Dashboard 1 is always backed up.
    """
    # Inline tests

    # Test 1: BACKUP_DIR is a Path
    assert isinstance(BACKUP_DIR, Path)

    # Create directories
    REPORT_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    targets = load_targets()
    arr_ids = [e["report_id"] for e in targets.get("arr_targets", [])]
    fiscal_ids = [e["report_id"] for e in targets.get("fiscal_targets", [])]

    # Deduplicate union (some reports may be both ARR and fiscal)
    all_report_ids = list(dict.fromkeys(arr_ids + fiscal_ids))
    print(f"Cell 2 (backup): {len(arr_ids)} ARR + {len(fiscal_ids)} fiscal = {len(all_report_ids)} unique reports")

    # Backup each report
    ok_count = 0
    failed: list[str] = []
    for rid in all_report_ids:
        dest = REPORT_BACKUP_DIR / f"{rid}.json"
        try:
            describe = get_report_describe(inst, tok, rid)
            with open(dest, "w") as f:
                json.dump(describe, f, indent=2)
            ok_count += 1
        except Exception as e:
            print(f"  FAILED backup {rid}: {e}")
            failed.append(rid)

    print(f"Cell 2: backed up {ok_count}/{len(all_report_ids)} reports")
    if failed:
        print(f"Cell 2: FAILED backups: {failed}")
        print("STOP: cannot proceed without complete backups.")
        sys.exit(1)

    # Backup Dashboard 1
    dash_dest = DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json"
    try:
        dash_describe = get_dashboard_describe(inst, tok, DASHBOARD_ID)
        with open(dash_dest, "w") as f:
            json.dump(dash_describe, f, indent=2)
        print(f"Cell 2: backed up dashboard {DASHBOARD_ID}")
    except Exception as e:
        print(f"Cell 2 FAILED backup dashboard: {e}")
        sys.exit(1)

    # Write rollback helper shell script
    rollback_path = BACKUP_DIR / "rollback_one.sh"
    with open(rollback_path, "w") as f:
        f.write("""#!/bin/bash
# Usage: rollback_one.sh <report_id>
# Restores a report's reportMetadata from /tmp/phase1_5_backup/reports/<id>.json
set -e
REPORT_ID="$1"
if [ -z "$REPORT_ID" ]; then
  echo "Usage: $0 <report_id>"
  exit 1
fi
BACKUP="/tmp/phase1_5_backup/reports/${REPORT_ID}.json"
if [ ! -f "$BACKUP" ]; then
  echo "No backup at $BACKUP"
  exit 1
fi
INST=$(sf org display --target-org apro@simcorp.com --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["instanceUrl"])')
TOK=$(sf org display --target-org apro@simcorp.com --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["accessToken"])')
BODY=$(python3 -c "import json; d=json.load(open('$BACKUP')); print(json.dumps({'reportMetadata': d['reportMetadata']}))")
curl -s -X PATCH "${INST}/services/data/v66.0/analytics/reports/${REPORT_ID}" \\
  -H "Authorization: Bearer ${TOK}" \\
  -H "Content-Type: application/json" \\
  -d "$BODY"
echo
echo "Restored ${REPORT_ID} from ${BACKUP}"
""")
    rollback_path.chmod(0o755)
    print(f"Cell 2: wrote rollback helper to {rollback_path}")

    return arr_ids, fiscal_ids


def _cell2_main(inst: str, tok: str) -> tuple[list[str], list[str]]:
    return backup_all(inst, tok)


# %% Cell 6: Summary (placeholder - updated as cells 3, 4, 5 are added)

def _cell6_main(
    arr_failures: list[str],
    fiscal_failures: list[str],
    dashboard_patch_ok: bool,
) -> None:
    total_failures = len(arr_failures) + len(fiscal_failures) + (0 if dashboard_patch_ok else 1)
    print()
    print("=" * 60)
    print("Phase 1.5 patch summary")
    print("=" * 60)
    print(f"ARR patches: {len(arr_failures)} failures")
    if arr_failures:
        for rid in arr_failures:
            print(f"  - {rid}")
    print(f"Fiscal patches: {len(fiscal_failures)} failures")
    if fiscal_failures:
        for rid in fiscal_failures:
            print(f"  - {rid}")
    print(f"Dashboard patch ok: {dashboard_patch_ok}")
    print()
    print(f"Backup directory: {BACKUP_DIR}")
    print(f"Rollback helper: {BACKUP_DIR}/rollback_one.sh")
    print()
    if total_failures == 0:
        print("SUCCESS. Next step: re-run the audit script and commit the output.")
    else:
        print(f"FAILED: {total_failures} operations failed. Inspect backups before rollback.")
    print("=" * 60)


# %% Main

def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no PATCH operations will be sent")
        print()

    inst, tok = _cell1_main()
    arr_ids, fiscal_ids = _cell2_main(inst, tok)

    # Cells 3, 4, 5 added in subsequent tasks
    arr_failures: list[str] = []
    fiscal_failures: list[str] = []
    dashboard_patch_ok = True

    _cell6_main(arr_failures, fiscal_failures, dashboard_patch_ok)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the file parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase1_5_patch_dashboard1.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 3: Run --help to verify argparse works**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_dashboard1.py --help 2>&1
```

Expected: argparse usage showing `--dry-run`, exits without running.

- [ ] **Step 4: Run end-to-end in dry-run mode to exercise cells 1, 2, 6**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_dashboard1.py --dry-run 2>&1 | tail -30
```

Expected: `DRY RUN MODE`, auth OK, cell 2 backs up N reports + dashboard, cell 6 summary with 0 failures. Verify `/tmp/phase1_5_backup/reports/` has files and `/tmp/phase1_5_backup/dashboards/01ZTb00000FSP7hMAH.json` exists.

- [ ] **Step 5: Verify the file is still uncommitted**

```bash
cd ~/crm-analytics && git status --short scripts/phase1_5_patch_dashboard1.py
```

Expected: `?? scripts/phase1_5_patch_dashboard1.py`.

- [ ] **Step 6: No commit.**

---

## Task 5: Implement cell 3 (ARR patches with inline verification)

**Files:**

- Modify: `scripts/phase1_5_patch_dashboard1.py` (add cell 3 between cell 2 and cell 6)

- [ ] **Step 1: Add cell 3 to the script**

Insert the following code block BETWEEN the existing `# %% Cell 6: Summary` marker and its `_cell6_main` function (i.e., add cell 3 just before cell 6):

```python
# %% Cell 3: ARR patches (inline verification)

RENEWAL_ACV_AGGREGATE = "s!Opportunity.APTS_Renewal_ACV__c.CONVERT"


def _is_amount_aggregate(agg: str) -> bool:
    """Check if an aggregate reference is the standard Amount field."""
    if not isinstance(agg, str):
        return False
    # Common forms: s!AMOUNT, s!Opportunity.Amount, s!Amount
    normalized = agg.upper()
    return normalized in ("S!AMOUNT", "S!OPPORTUNITY.AMOUNT", "S!OPPORTUNITY.AMOUNT.CONVERT")


def transform_aggregates_to_acv(aggregates: list[str]) -> tuple[list[str], bool]:
    """Replace the first Amount-shaped aggregate with the canonical ACV aggregate.

    Returns (new_aggregates, changed) - changed is True if a swap occurred.
    """
    if not aggregates:
        return aggregates, False
    new_aggs = list(aggregates)
    if _is_amount_aggregate(new_aggs[0]):
        new_aggs[0] = RENEWAL_ACV_AGGREGATE
        return new_aggs, True
    # Already ACV or different field - no-op
    return new_aggs, False


def build_patch_body(
    original_metadata: dict[str, Any], confirmed_shape_label: str
) -> dict[str, Any]:
    """Build a PATCH body for a report using the confirmed shape from Task 3."""
    if confirmed_shape_label in ("wrapped_full", "wrapped_stripped"):
        md = copy.deepcopy(original_metadata)
        if "stripped" in confirmed_shape_label:
            for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
                md.pop(ro_field, None)
        return {"reportMetadata": md}
    elif confirmed_shape_label in ("bare_full", "bare_stripped"):
        md = copy.deepcopy(original_metadata)
        if "stripped" in confirmed_shape_label:
            for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
                md.pop(ro_field, None)
        return md
    elif confirmed_shape_label == "sparse_name_only":
        # Sparse is not usable for field-level updates beyond name
        raise ValueError("sparse_name_only shape cannot carry aggregate changes")
    else:
        raise ValueError(f"unknown shape label: {confirmed_shape_label}")


def patch_report(
    inst: str, tok: str, report_id: str, body: dict[str, Any], dry_run: bool
) -> dict[str, Any]:
    """Send a PATCH for one report. Returns a result dict."""
    if dry_run:
        body_str = json.dumps(body)[:300]
        print(f"  [DRY RUN] would PATCH {report_id}: body[:300]={body_str}")
        return {"ok": True, "error": None, "status": 200}
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}"
    try:
        r = requests.patch(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}", "status": None}
    if not r.ok:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:500]}", "status": r.status_code}
    return {"ok": True, "error": None, "status": r.status_code}


def patch_arr_widgets(
    inst: str, tok: str, arr_ids: list[str], dry_run: bool
) -> list[str]:
    """For each ARR-defective report: read backup, swap aggregate, PATCH, inline verify.

    Returns a list of failed report IDs.
    """
    # Inline tests

    # Test 1: transform_aggregates_to_acv swaps Amount for ACV
    _new, _changed = transform_aggregates_to_acv(["s!AMOUNT", "RowCount"])
    assert _changed is True
    assert _new == [RENEWAL_ACV_AGGREGATE, "RowCount"], f"got {_new}"

    # Test 2: already-ACV is a no-op
    _new, _changed = transform_aggregates_to_acv([RENEWAL_ACV_AGGREGATE])
    assert _changed is False
    assert _new == [RENEWAL_ACV_AGGREGATE]

    # Test 3: Opportunity.Amount variant gets caught
    _new, _changed = transform_aggregates_to_acv(["s!Opportunity.Amount"])
    assert _changed is True
    assert _new == [RENEWAL_ACV_AGGREGATE]

    # Test 4: Different field is no-op
    _new, _changed = transform_aggregates_to_acv(["s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"])
    assert _changed is False

    # Test 5: Empty list is no-op
    _new, _changed = transform_aggregates_to_acv([])
    assert _changed is False

    print("Cell 3 (ARR) tests: PASS")

    shape_info = load_confirmed_shape()
    shape_label = shape_info["label"]
    print(f"Cell 3: using confirmed shape {shape_label}")

    failures: list[str] = []
    for rid in arr_ids:
        backup_path = REPORT_BACKUP_DIR / f"{rid}.json"
        if not backup_path.exists():
            print(f"  {rid}: NO BACKUP. Skipping.")
            failures.append(rid)
            continue
        with open(backup_path) as f:
            describe = json.load(f)
        original_metadata = describe.get("reportMetadata", {})
        current_aggs = original_metadata.get("aggregates", [])
        new_aggs, changed = transform_aggregates_to_acv(current_aggs)
        if not changed:
            print(f"  {rid}: aggregates already correct ({current_aggs}). SKIP.")
            continue
        print(f"  {rid}: {current_aggs} -> {new_aggs}")

        new_metadata = copy.deepcopy(original_metadata)
        new_metadata["aggregates"] = new_aggs
        body = build_patch_body(new_metadata, shape_label)
        result = patch_report(inst, tok, rid, body, dry_run)
        if not result["ok"]:
            print(f"  {rid}: PATCH FAILED: {result['error']}")
            failures.append(rid)
            continue

        if dry_run:
            continue

        # Inline verify: GET and check aggregates[0]
        try:
            verify = get_report_describe(inst, tok, rid)
        except Exception as e:
            print(f"  {rid}: VERIFY GET FAILED: {e}")
            failures.append(rid)
            continue
        actual_aggs = verify.get("reportMetadata", {}).get("aggregates", [])
        if not actual_aggs or actual_aggs[0] != RENEWAL_ACV_AGGREGATE:
            print(f"  {rid}: VERIFY FAILED: aggregates[0]={actual_aggs[0] if actual_aggs else None} (expected {RENEWAL_ACV_AGGREGATE})")
            failures.append(rid)
            continue
        print(f"  {rid}: verified")

    print(f"Cell 3: {len(arr_ids) - len(failures)}/{len(arr_ids)} ARR patches successful")
    return failures


def _cell3_main(
    inst: str, tok: str, arr_ids: list[str], dry_run: bool
) -> list[str]:
    return patch_arr_widgets(inst, tok, arr_ids, dry_run)
```

- [ ] **Step 2: Update the `main()` function to call cell 3**

Replace the existing `main()` body (after the `_cell2_main` call, before the `_cell6_main` call) with:

```python
def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no PATCH operations will be sent")
        print()

    inst, tok = _cell1_main()
    arr_ids, fiscal_ids = _cell2_main(inst, tok)
    arr_failures = _cell3_main(inst, tok, arr_ids, args.dry_run)

    # Cells 4, 5 added in subsequent tasks
    fiscal_failures: list[str] = []
    dashboard_patch_ok = True

    _cell6_main(arr_failures, fiscal_failures, dashboard_patch_ok)
```

- [ ] **Step 3: Verify the file parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase1_5_patch_dashboard1.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 4: Run --dry-run to exercise cells 1, 2, 3, 6**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_dashboard1.py --dry-run 2>&1 | tail -40
```

Expected output includes:

- `Cell 3 (ARR) tests: PASS`
- `Cell 3: using confirmed shape <label>`
- 3 lines like `  <report_id>: [old aggregates] -> [new aggregates]`
- 3 lines like `  [DRY RUN] would PATCH ...`
- `Cell 3: 3/3 ARR patches successful`
- Summary with 0 failures

- [ ] **Step 5: No commit.**

---

## Task 6: Implement cell 4 (fiscal filter swaps)

**Files:**

- Modify: `scripts/phase1_5_patch_dashboard1.py` (add cell 4 between cell 3 and cell 6)

- [ ] **Step 1: Add cell 4 to the script**

Insert the following between the end of cell 3 (`def _cell3_main`) and the start of cell 6 (`# %% Cell 6: Summary`):

```python
# %% Cell 4: Fiscal filter swaps (batch)


def transform_date_filter(duration_value: str) -> tuple[str, bool]:
    """Swap a fiscal date filter for its calendar equivalent.

    Returns (new_value, changed).
    """
    if not duration_value or duration_value not in FISCAL_TO_CALENDAR:
        return duration_value, False
    return FISCAL_TO_CALENDAR[duration_value], True


def patch_fiscal_filters(
    inst: str, tok: str, fiscal_ids: list[str], dry_run: bool
) -> list[str]:
    """For each fiscal-defective report: swap durationValue, PATCH (no inline verify).

    Returns a list of failed report IDs.
    """
    # Inline tests

    # Test 1: all 6 fiscal -> calendar mappings
    for frm, to in FISCAL_TO_CALENDAR.items():
        _new, _changed = transform_date_filter(frm)
        assert _changed is True
        assert _new == to, f"{frm} -> {_new}, expected {to}"

    # Test 2: CUSTOM is a no-op
    _new, _changed = transform_date_filter("CUSTOM")
    assert _changed is False
    assert _new == "CUSTOM"

    # Test 3: None/empty is a no-op
    _new, _changed = transform_date_filter("")
    assert _changed is False

    # Test 4: non-fiscal value is a no-op
    _new, _changed = transform_date_filter("LAST_N_DAYS:90")
    assert _changed is False

    print("Cell 4 (fiscal) tests: PASS")

    shape_info = load_confirmed_shape()
    shape_label = shape_info["label"]

    failures: list[str] = []
    for rid in fiscal_ids:
        backup_path = REPORT_BACKUP_DIR / f"{rid}.json"
        if not backup_path.exists():
            print(f"  {rid}: NO BACKUP. Skipping.")
            failures.append(rid)
            continue
        with open(backup_path) as f:
            describe = json.load(f)
        original_metadata = describe.get("reportMetadata", {})
        std_filter = original_metadata.get("standardDateFilter") or {}
        current_dv = std_filter.get("durationValue", "")
        new_dv, changed = transform_date_filter(current_dv)
        if not changed:
            print(f"  {rid}: date filter already correct or non-fiscal ({current_dv}). SKIP.")
            continue
        print(f"  {rid}: {current_dv} -> {new_dv}")

        new_metadata = copy.deepcopy(original_metadata)
        new_metadata.setdefault("standardDateFilter", {})["durationValue"] = new_dv
        body = build_patch_body(new_metadata, shape_label)
        result = patch_report(inst, tok, rid, body, dry_run)
        if not result["ok"]:
            print(f"  {rid}: PATCH FAILED: {result['error']}")
            failures.append(rid)

    print(f"Cell 4: {len(fiscal_ids) - len(failures)}/{len(fiscal_ids)} fiscal patches successful")
    return failures


def _cell4_main(
    inst: str, tok: str, fiscal_ids: list[str], dry_run: bool
) -> list[str]:
    return patch_fiscal_filters(inst, tok, fiscal_ids, dry_run)
```

- [ ] **Step 2: Update `main()` to call cell 4**

```python
def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no PATCH operations will be sent")
        print()

    inst, tok = _cell1_main()
    arr_ids, fiscal_ids = _cell2_main(inst, tok)
    arr_failures = _cell3_main(inst, tok, arr_ids, args.dry_run)
    fiscal_failures = _cell4_main(inst, tok, fiscal_ids, args.dry_run)

    # Cell 5 added in next task
    dashboard_patch_ok = True

    _cell6_main(arr_failures, fiscal_failures, dashboard_patch_ok)
```

- [ ] **Step 3: Verify the file parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase1_5_patch_dashboard1.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 4: Run --dry-run**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_dashboard1.py --dry-run 2>&1 | tail -60
```

Expected: `Cell 4 (fiscal) tests: PASS`, N lines of fiscal swaps (e.g., `THIS_FISCAL_YEAR -> THIS_CALENDAR_YEAR`), N lines of `[DRY RUN] would PATCH`, and cell 4 success count matching the fiscal target count.

- [ ] **Step 5: No commit.**

---

## Task 7: Implement cell 5 (dashboard component addition)

**Files:**

- Modify: `scripts/phase1_5_patch_dashboard1.py` (add cell 5 between cell 4 and cell 6)

- [ ] **Step 1: Add cell 5 to the script**

Insert the following between the end of cell 4 (`def _cell4_main`) and the start of cell 6:

```python
# %% Cell 5: Dashboard component addition (commercial_approval_approved_ytd)

import uuid


def clone_existing_component_for_new_widget(
    components: list[dict[str, Any]],
    new_report_id: str,
    new_header: str,
    new_title: str,
) -> dict[str, Any]:
    """Clone an existing SUMMARY-report component and modify 4 fields.

    Raises ValueError if no suitable template component exists.
    """
    # Find a SUMMARY report component as template
    template = None
    for c in components:
        props = c.get("properties") or {}
        if c.get("type") == "Report" and props.get("reportFormat") == "SUMMARY":
            template = c
            break
    if template is None:
        raise ValueError("No SUMMARY report component to clone from")

    new_component = copy.deepcopy(template)
    new_component["id"] = f"new_component_{uuid.uuid4().hex[:12]}"
    new_component["reportId"] = new_report_id
    new_component["header"] = new_header
    new_component["title"] = new_title
    return new_component


def patch_dashboard_component(
    inst: str, tok: str, dry_run: bool
) -> bool:
    """Read Dashboard 1 backup, clone a component, append for commercial_approval_approved_ytd, PATCH.

    Returns True on success, False on failure.
    """
    # Inline tests

    # Test 1: clone_existing_component_for_new_widget produces a new entry with changed fields
    _fake_components = [
        {
            "id": "c1",
            "type": "Report",
            "reportId": "orig_report",
            "header": "orig header",
            "title": "orig title",
            "properties": {"reportFormat": "SUMMARY"},
        }
    ]
    _new = clone_existing_component_for_new_widget(
        _fake_components, "00O_NEW", "New Header", "New Title"
    )
    assert _new["reportId"] == "00O_NEW"
    assert _new["header"] == "New Header"
    assert _new["title"] == "New Title"
    assert _new["id"] != "c1"
    assert _new["properties"]["reportFormat"] == "SUMMARY"  # inherited

    # Test 2: no SUMMARY components raises
    _bad_components = [
        {
            "id": "c1",
            "type": "Report",
            "reportId": "orig",
            "properties": {"reportFormat": "TABULAR"},
        }
    ]
    try:
        clone_existing_component_for_new_widget(_bad_components, "x", "y", "z")
        assert False, "should have raised"
    except ValueError:
        pass

    print("Cell 5 (dashboard) tests: PASS")

    backup_path = DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json"
    if not backup_path.exists():
        print(f"  NO BACKUP at {backup_path}")
        return False
    with open(backup_path) as f:
        dashboard = json.load(f)

    components = dashboard.get("components") or []
    # Check if the new widget already exists (idempotent behavior)
    for c in components:
        if c.get("reportId") == COMMERCIAL_APPROVAL_APPROVED_YTD_SOURCE_REPORT_ID:
            print(f"  Dashboard already has a component referencing {COMMERCIAL_APPROVAL_APPROVED_YTD_SOURCE_REPORT_ID}. SKIP.")
            return True

    try:
        new_component = clone_existing_component_for_new_widget(
            components,
            COMMERCIAL_APPROVAL_APPROVED_YTD_SOURCE_REPORT_ID,
            "Commercial Approval Approved YTD (Land)",
            "Deals approved YTD by region",
        )
    except ValueError as e:
        print(f"  Clone failed: {e}")
        return False

    new_components = list(components) + [new_component]
    new_dashboard = copy.deepcopy(dashboard)
    new_dashboard["components"] = new_components

    if dry_run:
        body_str = json.dumps({"components": new_components})[:300]
        print(f"  [DRY RUN] would PATCH dashboard {DASHBOARD_ID}: components len={len(new_components)}")
        print(f"    new component: {json.dumps(new_component)[:200]}")
        return True

    # Strip read-only dashboard fields that Analytics REST may reject
    for ro_field in ("id", "createdDate", "lastModifiedDate", "lastAccessedDate", "url"):
        new_dashboard.pop(ro_field, None)

    url = f"{inst}/services/data/{API_VERSION}/analytics/dashboards/{DASHBOARD_ID}"
    try:
        r = requests.patch(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json=new_dashboard,
            timeout=30,
        )
    except requests.RequestException as e:
        print(f"  Network error: {e}")
        return False

    if not r.ok:
        print(f"  PATCH FAILED: HTTP {r.status_code}: {r.text[:500]}")
        return False

    print(f"  Dashboard {DASHBOARD_ID} patched: new component appended")
    return True


def _cell5_main(inst: str, tok: str, dry_run: bool) -> bool:
    return patch_dashboard_component(inst, tok, dry_run)
```

- [ ] **Step 2: Update `main()` to call cell 5**

```python
def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no PATCH operations will be sent")
        print()

    inst, tok = _cell1_main()
    arr_ids, fiscal_ids = _cell2_main(inst, tok)
    arr_failures = _cell3_main(inst, tok, arr_ids, args.dry_run)
    fiscal_failures = _cell4_main(inst, tok, fiscal_ids, args.dry_run)
    dashboard_patch_ok = _cell5_main(inst, tok, args.dry_run)

    _cell6_main(arr_failures, fiscal_failures, dashboard_patch_ok)
```

- [ ] **Step 3: Verify the file parses and re-run --dry-run end-to-end**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase1_5_patch_dashboard1.py').read()); print('parse OK')" && python3 scripts/phase1_5_patch_dashboard1.py --dry-run 2>&1 | tail -80
```

Expected: all 5 cells run in dry-run mode, all tests pass, summary shows 0 failures, dashboard patch ok=True.

- [ ] **Step 4: No commit.**

---

## Task 8: End-to-end --dry-run review

**Files:** none modified. Diagnostic only.

- [ ] **Step 1: Fresh --dry-run with full output captured**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_dashboard1.py --dry-run 2>&1 | tee /tmp/phase1_5_probe/dry_run.log
```

- [ ] **Step 2: Count the PATCH bodies that would be sent**

```bash
grep -c "\[DRY RUN\] would PATCH" /tmp/phase1_5_probe/dry_run.log
```

Expected: 3 (ARR) + N (fiscal) + 1 (dashboard) = 4+N total. Compare N to Task 1's `fiscal_targets` count.

- [ ] **Step 3: Spot-check the first ARR PATCH body**

```bash
grep -A0 "\[DRY RUN\] would PATCH" /tmp/phase1_5_probe/dry_run.log | head -5
```

Confirm the first PATCH body snippet contains `APTS_Renewal_ACV__c` and does NOT contain `s!AMOUNT`.

- [ ] **Step 4: Spot-check the first fiscal PATCH body**

```bash
grep -B1 "THIS_FISCAL" /tmp/phase1_5_probe/dry_run.log | head -5
```

Confirm the swap lines show `THIS_FISCAL_* -> THIS_CALENDAR_*`.

- [ ] **Step 5: Spot-check the dashboard component PATCH**

```bash
grep "new component:" /tmp/phase1_5_probe/dry_run.log
```

Expected: JSON snippet showing the new component with `reportId: 00OTb000008aTtJMAU` and the header string.

- [ ] **Step 6: STOP and escalate if any spot check looks wrong**

If any of the spot checks reveal malformed PATCH bodies, STOP. Do NOT proceed to Task 9. Report the specific line from `/tmp/phase1_5_probe/dry_run.log` for controller review.

- [ ] **Step 7: No commit.**

---

## Task 9: Live patch script run

**Files:** modifies live org. /tmp/phase1_5_backup/ must already exist from prior tasks.

- [ ] **Step 1: Confirm backups are in place**

```bash
ls /tmp/phase1_5_backup/reports/ | wc -l && ls /tmp/phase1_5_backup/dashboards/
```

Expected: the reports count matches Task 1's unique report count. Dashboard backup exists.

- [ ] **Step 2: Run the patch script live**

```bash
cd ~/crm-analytics && python3 scripts/phase1_5_patch_dashboard1.py 2>&1 | tee /tmp/phase1_5_probe/live_run.log
```

Expected: not in dry-run mode. Cells 1-5 all run. Cell 6 summary shows 0 failures.

- [ ] **Step 3: Check for failures**

```bash
grep -E "FAILED|FAIL" /tmp/phase1_5_probe/live_run.log
```

Expected: zero matches (no failures) OR some matches that are all inside test assertions ("tests: PASS" is safe).

- [ ] **Step 4: Verify Cell 3 inline verification succeeded**

```bash
grep -E "verified" /tmp/phase1_5_probe/live_run.log
```

Expected: 3 lines of `  <report_id>: verified` (one per ARR patch).

- [ ] **Step 5: If any failures, use rollback**

If cell 6 summary shows N > 0 failures:

```bash
# For each failed report ID:
/tmp/phase1_5_backup/rollback_one.sh <FAILED_REPORT_ID>
```

Then STOP and escalate.

- [ ] **Step 6: No commit yet.** Task 10 re-runs the audit and Task 11 commits the result.

---

## Task 10: Re-run the audit script against post-patch Dashboard 1

**Files:** overwrites `docs/audits/2026-04-07-sales-director-monthly-audit.md`.

- [ ] **Step 1: Run the audit**

```bash
cd ~/crm-analytics && python3 scripts/audit_sales_director_monthly_dashboard.py \
  --dashboard-id 01ZTb00000FSP7hMAH \
  --spec-path /Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md \
  --output-name sales-director-monthly-audit \
  2>&1 | tee /tmp/phase1_5_probe/audit_rerun.log
```

Expected: ~1-2 minutes wall time. Script writes `docs/audits/2026-04-07-sales-director-monthly-audit.md`, overwriting the Phase 2 Task 2 version (`6cbe8fe`).

- [ ] **Step 2: Inspect the new tally**

```bash
grep -E "^- \*\*Tally" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
```

Compare to the pre-patch tally from `6cbe8fe`: `26 entries . 13 BLOCKING . 10 WRONG-DATA . 1 ORPHAN . 2 OK`.

Expected post-patch: BLOCKING down by at least 3 (ARR fixes) plus 1 (commercial_approval_approved_ytd now matched). WRONG-DATA down by approximately the fiscal fix count. OK up correspondingly.

- [ ] **Step 3: Verify specific rule hit counts dropped to zero**

```bash
grep -c "renewal_uses_amount_not_acv" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
grep -c "fiscal_date_filter" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
```

Expected: both counts are 0.

- [ ] **Step 4: Verify commercial_approval_approved_ytd is no longer MISSING**

```bash
grep -E "MISSING.*commercial_approval_approved_ytd|commercial_approval_approved_ytd.*MISSING" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
```

Expected: zero matches (the widget now has a matched component).

- [ ] **Step 5: STOP if any check fails**

If the tally shows no improvement, the rule hit counts are non-zero, or the commercial_approval_approved_ytd is still MISSING, STOP. Do NOT commit. Investigate with the live_run.log and consider rollback.

---

## Task 11: Verify acceptance criteria and commit the new audit output

**Files:** stages and commits `docs/audits/2026-04-07-sales-director-monthly-audit.md`.

- [ ] **Step 1: Walk the 14 acceptance criteria from the design doc**

Open `docs/2026-04-07-phase1-5-dashboard1-hotfix-design.md` and find the "## Acceptance criteria" section. Verify each of the 14 items passes:

1. Probe confirmed shape
2. Backups exist
3. 3 ARR patches inline-verified
4. N fiscal patches zero exceptions
5. 1 dashboard component added
6. Cell 6 summary 0 failures
7. Audit re-run completed
8. Post-patch tally strictly better
9. New audit file committed by exact path (next step)
10. audit_sales_director_monthly_dashboard.py stays uncommitted
11. phase1_5_patch_dashboard1.py stays uncommitted
12. phase1_5_patch_shape_probe.py stays uncommitted
13. No em-dashes in any committed file
14. Commit footer cites spec hash 8c81d2d

- [ ] **Step 2: Check no em-dashes in the new audit file**

```bash
grep -cP "\xe2\x80\x94|\xe2\x80\x93" ~/crm-analytics/docs/audits/2026-04-07-sales-director-monthly-audit.md
```

Expected: `0`.

- [ ] **Step 3: Verify all 3 scripts are still uncommitted**

```bash
cd ~/crm-analytics && git status --short scripts/audit_sales_director_monthly_dashboard.py scripts/phase1_5_patch_shape_probe.py scripts/phase1_5_patch_dashboard1.py
```

Expected: all three show `??` or `M` (untracked or modified). None is staged or committed.

- [ ] **Step 4: Stage the audit output by exact path**

```bash
cd ~/crm-analytics && git add docs/audits/2026-04-07-sales-director-monthly-audit.md && git diff --cached --name-only
```

Expected: exactly one line (the audit path).

- [ ] **Step 5: Commit with the prescribed message format**

Replace `<FILL IN FROM THE RUN>` with the actual post-patch tally line before running.

```bash
cd ~/crm-analytics && git commit -m "$(cat <<'COMMIT'
docs: phase 1.5 post-patch report 1 audit - dashboard 1 hotfixes applied

Re-runs the Sales Director Monthly audit against 01ZTb00000FSP7hMAH
after Phase 1.5 patches landed. Supersedes the Phase 2 Task 2 audit
at commit 6cbe8fe (same file path). Phase 1.5 applied:

- 3 ARR patches on renewal reports (s!AMOUNT swapped to
  s!Opportunity.APTS_Renewal_ACV__c.CONVERT), inline-verified.
- N fiscal date filter swaps (THIS_FISCAL_* replaced with
  THIS_CALENDAR_* per hard rule 1), batch-verified via this
  audit re-run.
- 1 dashboard component addition: commercial_approval_approved_ytd
  referencing existing SF report 00OTb000008aTtJMAU (the Sales Ops
  Commercial Approval approved 2026 YTD tracker).

Expected tally improvement:

- Zero rows with renewal_uses_amount_not_acv rule (was 3).
- Zero rows with fiscal_date_filter rule (was roughly 10).
- commercial_approval_approved_ytd no longer (MISSING) BLOCKING.
- Forecast accuracy widgets 00OTb000008TZsDMAW and 00OTb000008TZaTMAW
  now OK (fiscal filter removed per Option D).

Pre-patch tally (6cbe8fe): 26 entries . 13 BLOCKING . 10 WRONG-DATA . 1 ORPHAN . 2 OK
Post-patch tally: <FILL IN FROM THE RUN>

Scripts (all uncommitted by convention):
- scripts/audit_sales_director_monthly_dashboard.py (parameterized in Phase 2 Task 1)
- scripts/phase1_5_patch_shape_probe.py (Phase 1.5 Task 2)
- scripts/phase1_5_patch_dashboard1.py (Phase 1.5 Tasks 4-7)

Backups at /tmp/phase1_5_backup/ with rollback_one.sh helper.

Pre-patch backup of every affected report is captured at
/tmp/phase1_5_backup/reports/<id>.json. Dashboard 1 pre-patch state
is at /tmp/phase1_5_backup/dashboards/01ZTb00000FSP7hMAH.json.

Spec graded against: docs/specs/sales-director-monthly-dashboard-spec.md
(commit 8c81d2d, 16-widget Report 1 spec).

Design: docs/2026-04-07-phase1-5-dashboard1-hotfix-design.md (commit 8943fe5)
Plan: docs/2026-04-07-phase1-5-dashboard1-hotfix-plan.md

Next: Phase 2.5 (Dashboard 2 build queue + defect fixes), Phase 4
(deck rebuild reading both source contracts).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
COMMIT
)"
```

- [ ] **Step 6: Verify the commit landed**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s"
```

Expected: latest commit is `docs: phase 1.5 post-patch report 1 audit - dashboard 1 hotfixes applied`.

- [ ] **Step 7: Print a Phase 1.5 done summary**

```bash
cd ~/crm-analytics && git log --oneline | head -15
```

Should show Phase 1.5's commits in order: the design doc (`8943fe5`), the plan doc (committed after this plan lands), and this audit (the final commit).

---

## Self-review checklist

- [ ] Every section of the design doc has a corresponding task in this plan.
- [ ] No "TBD", "TODO", or placeholder text in any task body.
- [ ] Every code block contains complete, runnable code.
- [ ] Function and variable names match across tasks (e.g. `transform_aggregates_to_acv`, `build_patch_body`, `FISCAL_TO_CALENDAR` are spelled identically everywhere).
- [ ] All file paths are absolute or ~/crm-analytics-relative.
- [ ] Every commit step uses `git add <exact path>`, never `.` or `-A`.
- [ ] The 3 Phase 1.5 scripts all stay uncommitted.
- [ ] No em-dashes in any task body.
- [ ] Rollback path is documented for every failure mode.
- [ ] The --dry-run flag is tested before the live run (Tasks 4-8).

## Notes for the executor

- **Phase 1.5 is the first WRITE workflow.** Unlike Phase 2, this touches production SF report metadata. If anything feels wrong, STOP and escalate. The backups are one `rollback_one.sh` call away.
- **The probe (Task 3) is load-bearing.** If the confirmed body shape is wrong, every subsequent PATCH fails the same way. Trust the probe's output.
- **Inline verification on ARR patches catches silent failures.** A 200 response can still produce unexpected state. The GET-back check is the authoritative signal.
- **Dry-run before live.** Task 8 is not optional - it's the final sanity check before touching production.
- **Sales Directors will see different numbers after the fiscal->calendar swap.** Risk #3 from the design doc. Someone should probably notify them. Phase 1.5 does not include a notification step.
- **No new SF reports are built.** The commercial_approval_approved_ytd component references an EXISTING report. The other 8 missing spec widgets go to Phase 2.5.
- **Wall time estimate: ~10-15 minutes total across all tasks** (including the probe). Most time is in the audit re-run (~2 min) and the backup phase (~30-60 sec for 15-18 GETs).
