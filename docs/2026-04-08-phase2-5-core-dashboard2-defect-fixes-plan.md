# Phase 2.5 B-core - Dashboard 2 Defect Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Patch 2 defective reports on Dashboard 2 (`01ZTb00000FSP9JMAX`) using the Phase 1.5 script pattern. Fix 1 swaps `s!AMOUNT` to the canonical ARR aggregate on `ph_no_activity_30_plus` (with detailColumns updated). Fix 2 swaps the `FISCAL_QUARTER` grouping to a calendar-quarter shape on `ph_overdue_opportunities` (or defers if the probe finds no usable shape). Re-run the Phase 1 audit against Dashboard 2 and commit the new audit output at `docs/audits/2026-04-08-sales-ops-quarterly-audit.md`.

**Architecture:** Two new uncommitted Python scripts. `scripts/phase2_5_shape_probe.py` runs first to confirm (a) the canonical ARR aggregate form for Dashboard 2 and (b) the calendar-quarter grouping shape. `scripts/phase2_5_core_patch.py` applies both fixes using the confirmed shapes, backs up to `/tmp/phase2_5_backup/`, supports `--dry-run`, and inline-verifies each PATCH via GET-back.

**Tech Stack:** Python 3.13, `requests`, `subprocess`, `argparse`, `json`, `pathlib`. Auth via `sf org display --target-org apro@simcorp.com --json`. API v66.0. Same patterns as Phase 1.5.

**Design doc (input):** `docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-design.md` (commit `1c2195f`). Read it first.

**Spec inputs (read-only, already committed):**

- `docs/specs/sales-ops-quarterly-dashboard-spec.md` (commit `25cc03d`)
- `docs/audits/2026-04-07-sales-ops-quarterly-audit.md` (commit `d48f13c`, pre-patch)

---

## File structure

```
crm-analytics/
|-- docs/
|   |-- 2026-04-08-phase2-5-core-dashboard2-defect-fixes-design.md  (committed at 1c2195f)
|   |-- 2026-04-08-phase2-5-core-dashboard2-defect-fixes-plan.md    (this file)
|   `-- audits/
|       `-- 2026-04-08-sales-ops-quarterly-audit.md                 (Task 7, NEW, committed)
`-- scripts/
    |-- audit_sales_director_monthly_dashboard.py                    (parameterized in Phase 2 Task 1, UNTOUCHED)
    |-- phase2_5_shape_probe.py                                     (Task 1, NEW, uncommitted)
    `-- phase2_5_core_patch.py                                      (Tasks 2-4, NEW, uncommitted)
```

---

## Task 0: Pre-flight verification

**Files:** none modified. Read-only.

- [ ] **Step 1: Verify prior state**

```bash
cd ~/crm-analytics && git log -1 --format=%h -- docs/audits/2026-04-07-sales-ops-quarterly-audit.md
```

Expected: `d48f13c`.

- [ ] **Step 2: Verify Report 2 spec commit**

```bash
cd ~/crm-analytics && git log -1 --format=%h -- docs/specs/sales-ops-quarterly-dashboard-spec.md
```

Expected: `25cc03d`.

- [ ] **Step 3: Verify audit script works**

```bash
cd ~/crm-analytics && python3 scripts/audit_sales_director_monthly_dashboard.py --help 2>&1 | head -5
```

Expected: argparse usage block.

- [ ] **Step 4: Create ephemeral working directories**

```bash
mkdir -p /tmp/phase2_5_probe /tmp/phase2_5_backup/reports
ls -la /tmp/phase2_5_probe /tmp/phase2_5_backup/reports
```

Expected: both directories exist and are empty.

- [ ] **Step 5: No commit.**

---

## Task 1: Write and run the pre-flight shape probe

**Files:**

- Create: `scripts/phase2_5_shape_probe.py` (uncommitted)
- Produces: `/tmp/phase2_5_probe/confirmed.json`

The probe must determine three things empirically before the patch script runs:

1. The canonical ARR aggregate form for Dashboard 2's reports (with or without `.CONVERT` suffix).
2. Whether a calendar-quarter grouping shape exists anywhere in the org that Fix 2 can copy.
3. That the `wrapped_full` PATCH body shape still works for Dashboard 2's reports (round-trip test with a cosmetic name change on one low-risk widget).

- [ ] **Step 1: Create the probe script**

Create `/Users/test/crm-analytics/scripts/phase2_5_shape_probe.py` with this content:

```python
#!/usr/bin/env python3
"""Phase 2.5 B-core - Pre-flight shape probe.

Determines empirically:
1. The canonical ARR aggregate form for Dashboard 2's reports by
   inspecting ph_no_activity_30_plus (target of Fix 1) and a reference
   Dashboard 2 widget that already uses ARR correctly
   (dq_missing_decision_reason per the Phase 2 audit).
2. The calendar-quarter grouping shape by scanning up to 100 reports for
   groupingsDown entries that use a CloseDate-derived calendar quarter.
3. That the wrapped_full PATCH body shape still works for Dashboard 2's
   reports by round-tripping a cosmetic name change on one Dashboard 2
   widget (NOT a target of either Fix).

Writes /tmp/phase2_5_probe/confirmed.json with the results. Exits non-zero
if any step fails catastrophically.

Uncommitted by convention.

Run:
    python3 scripts/phase2_5_shape_probe.py
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
PROBE_DIR = Path("/tmp/phase2_5_probe")
CONFIRMED_PATH = PROBE_DIR / "confirmed.json"

# Targets of the two fixes
FIX1_REPORT_ID = "00OTb000008TaEnMAK"  # ph_no_activity_30_plus
FIX2_REPORT_ID = "00OTb000008SrmLMAS"  # ph_overdue_opportunities

# Reference Dashboard 2 widget that uses ARR correctly per the Phase 2 audit
ARR_REFERENCE_REPORT_ID = "00OTb000008el0PMAQ"  # dq_missing_decision_reason

# Probe target for the body-shape round-trip (a third Dashboard 2 widget
# that is NOT a target of either fix). Using dq_missing_decision_reason
# would work but pick another to avoid touching the reference twice.
BODY_SHAPE_PROBE_REPORT_ID = "00OTb000008TZqcMAG"  # dq_missing_amount


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


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def probe_arr_convention(inst: str, tok: str) -> dict[str, Any]:
    """Check the ARR aggregate form on the reference Dashboard 2 widget."""
    print(f"Probing ARR convention on {ARR_REFERENCE_REPORT_ID}...")
    d = get_report_describe(inst, tok, ARR_REFERENCE_REPORT_ID)
    rm = d.get("reportMetadata", {})
    aggs = rm.get("aggregates", [])
    detail = rm.get("detailColumns", [])
    print(f"  aggregates: {aggs}")
    print(f"  detailColumns (ARR-relevant): {[c for c in detail if 'APTS_Opportunity_ARR' in c]}")
    # Find the ARR aggregate form actually used
    arr_agg = None
    for a in aggs:
        if isinstance(a, str) and "APTS_Opportunity_ARR__c" in a:
            arr_agg = a
            break
    return {
        "reference_report": ARR_REFERENCE_REPORT_ID,
        "confirmed_arr_aggregate": arr_agg,  # e.g., "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
        "reference_detail_columns": detail,
    }


def probe_fix1_current_state(inst: str, tok: str) -> dict[str, Any]:
    """Capture the current state of the Fix 1 target."""
    print(f"Probing Fix 1 target {FIX1_REPORT_ID}...")
    d = get_report_describe(inst, tok, FIX1_REPORT_ID)
    rm = d.get("reportMetadata", {})
    return {
        "report_id": FIX1_REPORT_ID,
        "current_aggregates": rm.get("aggregates", []),
        "current_detail_columns": rm.get("detailColumns", []),
    }


def probe_fix2_current_state(inst: str, tok: str) -> dict[str, Any]:
    """Capture the current state of the Fix 2 target."""
    print(f"Probing Fix 2 target {FIX2_REPORT_ID}...")
    d = get_report_describe(inst, tok, FIX2_REPORT_ID)
    rm = d.get("reportMetadata", {})
    return {
        "report_id": FIX2_REPORT_ID,
        "current_groupings_down": rm.get("groupingsDown", []),
        "current_standard_date_filter": rm.get("standardDateFilter"),
    }


def probe_calendar_quarter_grouping(inst: str, tok: str) -> dict[str, Any]:
    """Scan up to 100 reports for a calendar-quarter grouping shape."""
    print("Scanning 100 reports for a calendar-quarter grouping shape...")
    query = "SELECT Id FROM Report LIMIT 100"
    r = requests.get(
        f"{inst}/services/data/{API_VERSION}/query?q={query.replace(' ', '+')}",
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    if not r.ok:
        return {"found": False, "error": f"query HTTP {r.status_code}", "sample_groupings": []}
    recs = r.json().get("records", [])

    sample_groupings: list[dict[str, Any]] = []
    calendar_quarter_shape: dict[str, Any] | None = None

    for rec in recs:
        rid = rec["Id"]
        try:
            d = get_report_describe(inst, tok, rid)
            gd = d.get("reportMetadata", {}).get("groupingsDown", []) or []
            for g in gd:
                if not isinstance(g, dict):
                    continue
                name = g.get("name", "")
                granularity = g.get("dateGranularity")
                # Look for a CloseDate or similar date-field grouping with Quarter granularity
                # that is NOT a fiscal computed field
                if "FISCAL" in name.upper():
                    continue
                if granularity and "QUARTER" in granularity.upper():
                    sample_groupings.append({"report_id": rid, "grouping": g})
                    if calendar_quarter_shape is None:
                        calendar_quarter_shape = g
                # Also look for standalone non-fiscal-Quarter name entries
                elif name.upper() in ("CLOSE_DATE", "CLOSEDATE"):
                    if granularity and "QUARTER" in (granularity or "").upper():
                        sample_groupings.append({"report_id": rid, "grouping": g})
                        if calendar_quarter_shape is None:
                            calendar_quarter_shape = g
        except Exception:
            continue

        if len(sample_groupings) >= 5:
            break

    found = calendar_quarter_shape is not None
    print(f"  found calendar-quarter shape: {found}")
    if found:
        print(f"  shape: {calendar_quarter_shape}")
    return {
        "found": found,
        "calendar_quarter_shape": calendar_quarter_shape,
        "sample_groupings": sample_groupings,
    }


def probe_body_shape_roundtrip(inst: str, tok: str) -> dict[str, Any]:
    """Round-trip a cosmetic name change on BODY_SHAPE_PROBE_REPORT_ID to
    confirm wrapped_full works on Dashboard 2's reports."""
    print(f"Body shape round-trip on {BODY_SHAPE_PROBE_REPORT_ID}...")
    d = get_report_describe(inst, tok, BODY_SHAPE_PROBE_REPORT_ID)
    original_rm = d.get("reportMetadata", {})
    original_name = original_rm.get("name", "")

    probe_name = f"{original_name} (phase2_5_probe)"
    new_rm = copy.deepcopy(original_rm)
    new_rm["name"] = probe_name
    body = {"reportMetadata": new_rm}

    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{BODY_SHAPE_PROBE_REPORT_ID}"
    hdr = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

    try:
        r = requests.patch(url, headers=hdr, json=body, timeout=30)
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}"}
    if not r.ok:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:300]}"}

    # Verify
    v = get_report_describe(inst, tok, BODY_SHAPE_PROBE_REPORT_ID)
    verify_name = v.get("reportMetadata", {}).get("name", "")
    if verify_name != probe_name:
        return {"ok": False, "error": f"verify mismatch: {verify_name!r}"}

    # Restore
    restore_rm = copy.deepcopy(original_rm)
    restore_body = {"reportMetadata": restore_rm}
    r2 = requests.patch(url, headers=hdr, json=restore_body, timeout=30)
    if not r2.ok:
        return {
            "ok": False,
            "error": f"restore FAILED HTTP {r2.status_code}: {r2.text[:300]}",
            "MANUAL_ACTION": f"restore {BODY_SHAPE_PROBE_REPORT_ID} name to {original_name!r}",
        }
    # Re-verify restore
    v2 = get_report_describe(inst, tok, BODY_SHAPE_PROBE_REPORT_ID)
    if v2.get("reportMetadata", {}).get("name") != original_name:
        return {
            "ok": False,
            "error": "restore verify mismatch",
            "MANUAL_ACTION": f"restore {BODY_SHAPE_PROBE_REPORT_ID} name to {original_name!r}",
        }

    print(f"  body shape wrapped_full confirmed working")
    return {"ok": True, "shape": "wrapped_full"}


def main() -> None:
    PROBE_DIR.mkdir(parents=True, exist_ok=True)

    inst, tok = get_auth()
    print(f"Auth OK: {inst}")

    result: dict[str, Any] = {
        "arr_convention": None,
        "fix1_current": None,
        "fix2_current": None,
        "calendar_quarter_probe": None,
        "body_shape_probe": None,
    }

    result["arr_convention"] = probe_arr_convention(inst, tok)
    result["fix1_current"] = probe_fix1_current_state(inst, tok)
    result["fix2_current"] = probe_fix2_current_state(inst, tok)
    result["calendar_quarter_probe"] = probe_calendar_quarter_grouping(inst, tok)
    result["body_shape_probe"] = probe_body_shape_roundtrip(inst, tok)

    with open(CONFIRMED_PATH, "w") as f:
        json.dump(result, f, indent=2)
    print()
    print(f"Probe results written to {CONFIRMED_PATH}")
    print()
    print("SUMMARY")
    print("=" * 60)
    arr = result["arr_convention"]
    print(f"ARR aggregate form: {arr.get('confirmed_arr_aggregate')!r}")
    cq = result["calendar_quarter_probe"]
    print(f"Calendar-quarter shape found: {cq.get('found')}")
    if cq.get("found"):
        print(f"  shape: {cq.get('calendar_quarter_shape')}")
    bs = result["body_shape_probe"]
    print(f"Body shape probe: {'OK' if bs.get('ok') else 'FAIL'}")
    if not bs.get("ok"):
        print(f"  error: {bs.get('error')}")
        if bs.get("MANUAL_ACTION"):
            print(f"  MANUAL ACTION REQUIRED: {bs.get('MANUAL_ACTION')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the file parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase2_5_shape_probe.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 3: Run the probe**

```bash
cd ~/crm-analytics && python3 scripts/phase2_5_shape_probe.py 2>&1 | tee /tmp/phase2_5_probe/probe_run.log
```

Expected: exit 0, SUMMARY block printed, `/tmp/phase2_5_probe/confirmed.json` written.

- [ ] **Step 4: Inspect the probe results**

```bash
cat /tmp/phase2_5_probe/confirmed.json | python3 -m json.tool
```

Key things to note:

- `arr_convention.confirmed_arr_aggregate`: Will be something like `"s!Opportunity.APTS_Opportunity_ARR__c"` or `"s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"`. This is what Fix 1 will use.
- `calendar_quarter_probe.found`: If `true`, Fix 2 can proceed. If `false`, Fix 2 is deferred.
- `body_shape_probe.ok`: Must be `true`, otherwise STOP.

- [ ] **Step 5: STOP if the probe failed**

If `body_shape_probe.ok` is `false`, STOP and report BLOCKED with the error details.

If `arr_convention.confirmed_arr_aggregate` is `null`, STOP - the reference report does not use ARR as expected, so Fix 1 cannot be confidently applied.

If the probe completes with `calendar_quarter_probe.found == false`, DO NOT STOP - note that Fix 2 will be deferred and continue to Task 2.

- [ ] **Step 6: No commit.**

---

## Task 2: Write the main patch script

**Files:**

- Create: `scripts/phase2_5_core_patch.py` (uncommitted)

This script applies both fixes using the shapes confirmed by the probe. Notebook-style with 5 cells and `--dry-run` support. Same pattern as `scripts/phase1_5_patch_dashboard1.py`.

- [ ] **Step 1: Create the script**

Create `/Users/test/crm-analytics/scripts/phase2_5_core_patch.py` with this content:

```python
#!/usr/bin/env python3
"""Phase 2.5 B-core - Dashboard 2 defect patcher.

Applies two mechanical fixes to Dashboard 2 (01ZTb00000FSP9JMAX) reports:

- Fix 1: ph_no_activity_30_plus - swap s!AMOUNT to the canonical ARR
  aggregate (form determined by the probe) and ensure the ARR field
  is in detailColumns.
- Fix 2: ph_overdue_opportunities - swap FISCAL_QUARTER in groupingsDown
  for the calendar-quarter shape from the probe. Deferred if the probe
  did not find a usable shape.

Backs up both reports to /tmp/phase2_5_backup/reports/ before any PATCH.
Supports --dry-run.

Uncommitted by convention.

Design: docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-design.md
Plan: docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-plan.md

Run:
    python3 scripts/phase2_5_core_patch.py --dry-run
    python3 scripts/phase2_5_core_patch.py
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

TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"

FIX1_REPORT_ID = "00OTb000008TaEnMAK"  # ph_no_activity_30_plus
FIX2_REPORT_ID = "00OTb000008SrmLMAS"  # ph_overdue_opportunities

ARR_DETAIL_COLUMN = "Opportunity.APTS_Opportunity_ARR__c"

PROBE_CONFIRMED_PATH = Path("/tmp/phase2_5_probe/confirmed.json")
BACKUP_DIR = Path("/tmp/phase2_5_backup")
REPORT_BACKUP_DIR = BACKUP_DIR / "reports"


def _parse_args():
    p = argparse.ArgumentParser(description="Phase 2.5 B-core - Dashboard 2 defect patcher")
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


def load_confirmed() -> dict[str, Any]:
    if not PROBE_CONFIRMED_PATH.exists():
        print(f"ERROR: {PROBE_CONFIRMED_PATH} does not exist. Run Task 1 probe first.")
        sys.exit(1)
    with open(PROBE_CONFIRMED_PATH) as f:
        return json.load(f)


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def patch_report(inst: str, tok: str, report_id: str, body: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    if dry_run:
        body_str = json.dumps(body)[:400]
        print(f"  [DRY RUN] would PATCH {report_id}: body[:400]={body_str}")
        return {"ok": True, "error": None, "status": 200}
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}"
    try:
        r = requests.patch(
            url,
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}", "status": None}
    if not r.ok:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:500]}", "status": r.status_code}
    return {"ok": True, "error": None, "status": r.status_code}


# %% Cell 2: Backup

def _cell2_main(inst: str, tok: str) -> None:
    REPORT_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    for rid in (FIX1_REPORT_ID, FIX2_REPORT_ID):
        dest = REPORT_BACKUP_DIR / f"{rid}.json"
        try:
            describe = get_report_describe(inst, tok, rid)
            with open(dest, "w") as f:
                json.dump(describe, f, indent=2)
            print(f"Cell 2 (backup): {rid} -> {dest}")
        except Exception as e:
            print(f"Cell 2: BACKUP FAILED for {rid}: {e}")
            sys.exit(1)

    # Write rollback helper
    rollback_path = BACKUP_DIR / "rollback_one.sh"
    with open(rollback_path, "w") as f:
        f.write("""#!/bin/bash
# Usage: rollback_one.sh <report_id>
set -e
REPORT_ID="$1"
if [ -z "$REPORT_ID" ]; then
  echo "Usage: $0 <report_id>"
  exit 1
fi
BACKUP="/tmp/phase2_5_backup/reports/${REPORT_ID}.json"
if [ ! -f "$BACKUP" ]; then
  echo "No backup at $BACKUP"
  exit 1
fi
SF_JSON=$(sf org display --target-org apro@simcorp.com --json 2>/dev/null)
INST=$(python3 -c "import json,sys; t='''$SF_JSON'''; print(json.loads(t[t.find('{'):])['result']['instanceUrl'])")
TOK=$(python3 -c "import json,sys; t='''$SF_JSON'''; print(json.loads(t[t.find('{'):])['result']['accessToken'])")
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


# %% Cell 3: Fix 1 - ARR aggregate swap + detailColumns fix

def transform_no_activity_metadata(
    original_metadata: dict[str, Any], target_arr_aggregate: str
) -> tuple[dict[str, Any], bool]:
    """Build the new reportMetadata for Fix 1.

    Returns (new_metadata, changed). `changed` is False if both the
    aggregate and the detailColumn are already correct (idempotent).
    """
    current_aggs = list(original_metadata.get("aggregates") or [])
    current_detail = list(original_metadata.get("detailColumns") or [])

    changed = False
    new_aggs = list(current_aggs)
    if new_aggs and new_aggs[0] != target_arr_aggregate:
        new_aggs[0] = target_arr_aggregate
        changed = True

    new_detail = list(current_detail)
    if ARR_DETAIL_COLUMN not in new_detail:
        new_detail.append(ARR_DETAIL_COLUMN)
        changed = True

    new_metadata = copy.deepcopy(original_metadata)
    new_metadata["aggregates"] = new_aggs
    new_metadata["detailColumns"] = new_detail
    return new_metadata, changed


def _cell3_main(inst: str, tok: str, target_arr_aggregate: str, dry_run: bool) -> bool:
    """Fix 1: ph_no_activity_30_plus aggregate + detailColumns. Returns True on success."""
    # Inline tests

    # Test 1: swap happens and ARR added to detailColumns
    _fake = {
        "aggregates": ["s!AMOUNT", "RowCount"],
        "detailColumns": ["FULL_NAME"],
    }
    _new, _changed = transform_no_activity_metadata(_fake, "s!Opportunity.APTS_Opportunity_ARR__c")
    assert _changed is True
    assert _new["aggregates"][0] == "s!Opportunity.APTS_Opportunity_ARR__c"
    assert _new["aggregates"][1] == "RowCount"
    assert "Opportunity.APTS_Opportunity_ARR__c" in _new["detailColumns"]
    assert "FULL_NAME" in _new["detailColumns"]

    # Test 2: idempotent when already correct
    _fake2 = {
        "aggregates": ["s!Opportunity.APTS_Opportunity_ARR__c", "RowCount"],
        "detailColumns": ["FULL_NAME", "Opportunity.APTS_Opportunity_ARR__c"],
    }
    _new, _changed = transform_no_activity_metadata(_fake2, "s!Opportunity.APTS_Opportunity_ARR__c")
    assert _changed is False

    # Test 3: detailColumns not duplicated
    _fake3 = {
        "aggregates": ["s!AMOUNT"],
        "detailColumns": ["Opportunity.APTS_Opportunity_ARR__c", "FULL_NAME"],
    }
    _new, _changed = transform_no_activity_metadata(_fake3, "s!Opportunity.APTS_Opportunity_ARR__c")
    assert _changed is True  # aggregate still needed
    assert _new["detailColumns"].count("Opportunity.APTS_Opportunity_ARR__c") == 1

    # Test 4: different target aggregate (with .CONVERT)
    _fake4 = {
        "aggregates": ["s!AMOUNT"],
        "detailColumns": [],
    }
    _new, _changed = transform_no_activity_metadata(_fake4, "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT")
    assert _changed is True
    assert _new["aggregates"][0] == "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    assert "Opportunity.APTS_Opportunity_ARR__c" in _new["detailColumns"]

    print("Cell 3 (Fix 1) tests: PASS")

    backup_path = REPORT_BACKUP_DIR / f"{FIX1_REPORT_ID}.json"
    with open(backup_path) as f:
        describe = json.load(f)
    original_metadata = describe.get("reportMetadata", {})

    new_metadata, changed = transform_no_activity_metadata(original_metadata, target_arr_aggregate)
    if not changed:
        print(f"Cell 3: {FIX1_REPORT_ID} already correct. SKIP.")
        return True

    print(f"Cell 3: {FIX1_REPORT_ID} aggregates {original_metadata.get('aggregates')} -> {new_metadata['aggregates']}")
    print(f"Cell 3: {FIX1_REPORT_ID} detailColumns +{ARR_DETAIL_COLUMN!r}")

    body = {"reportMetadata": new_metadata}
    result = patch_report(inst, tok, FIX1_REPORT_ID, body, dry_run)
    if not result["ok"]:
        print(f"Cell 3: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        return True

    # Inline verify
    try:
        verify = get_report_describe(inst, tok, FIX1_REPORT_ID)
    except Exception as e:
        print(f"Cell 3: VERIFY GET FAILED: {e}")
        return False
    v_aggs = verify.get("reportMetadata", {}).get("aggregates") or []
    if not v_aggs or v_aggs[0] != target_arr_aggregate:
        print(f"Cell 3: VERIFY FAILED: aggregates[0]={v_aggs[0] if v_aggs else None} (expected {target_arr_aggregate})")
        return False
    v_detail = verify.get("reportMetadata", {}).get("detailColumns") or []
    if ARR_DETAIL_COLUMN not in v_detail:
        print(f"Cell 3: VERIFY FAILED: {ARR_DETAIL_COLUMN} not in detailColumns {v_detail}")
        return False
    print(f"Cell 3: {FIX1_REPORT_ID} verified")
    return True


# %% Cell 4: Fix 2 - FISCAL_QUARTER grouping swap (or defer)

def transform_overdue_metadata(
    original_metadata: dict[str, Any], calendar_quarter_shape: dict[str, Any] | None
) -> tuple[dict[str, Any] | None, str]:
    """Build the new reportMetadata for Fix 2.

    Returns (new_metadata, status). Status is one of:
    - "changed" - fix applied, PATCH should proceed
    - "noop" - already correct, no PATCH needed
    - "deferred" - no calendar shape available, skip Fix 2 entirely
    """
    if calendar_quarter_shape is None:
        return None, "deferred"

    current_gd = list(original_metadata.get("groupingsDown") or [])
    # Find the FISCAL_QUARTER entry
    fiscal_idx = None
    for i, g in enumerate(current_gd):
        if isinstance(g, dict) and "FISCAL_QUARTER" in (g.get("name") or "").upper():
            fiscal_idx = i
            break

    if fiscal_idx is None:
        return copy.deepcopy(original_metadata), "noop"

    new_gd = copy.deepcopy(current_gd)
    new_gd[fiscal_idx] = copy.deepcopy(calendar_quarter_shape)

    new_metadata = copy.deepcopy(original_metadata)
    new_metadata["groupingsDown"] = new_gd
    return new_metadata, "changed"


def _cell4_main(
    inst: str, tok: str, calendar_quarter_shape: dict[str, Any] | None, dry_run: bool
) -> bool:
    """Fix 2: ph_overdue_opportunities grouping swap. Returns True on success or deferral."""
    # Inline tests

    # Test 1: FISCAL_QUARTER replaced when shape is provided
    _shape = {"name": "CLOSE_DATE", "dateGranularity": "Quarter", "sortOrder": "Asc"}
    _fake = {
        "groupingsDown": [
            {"name": "Opportunity.Account_Unit_Group__c", "sortOrder": "Asc"},
            {"name": "FISCAL_QUARTER", "sortOrder": "Asc"},
        ]
    }
    _new, _status = transform_overdue_metadata(_fake, _shape)
    assert _status == "changed"
    assert _new is not None
    assert _new["groupingsDown"][0]["name"] == "Opportunity.Account_Unit_Group__c"
    assert _new["groupingsDown"][1]["name"] == "CLOSE_DATE"
    assert _new["groupingsDown"][1]["dateGranularity"] == "Quarter"

    # Test 2: noop if FISCAL_QUARTER not present
    _fake2 = {
        "groupingsDown": [
            {"name": "Opportunity.Account_Unit_Group__c", "sortOrder": "Asc"},
            {"name": "STAGE_NAME", "sortOrder": "Asc"},
        ]
    }
    _new, _status = transform_overdue_metadata(_fake2, _shape)
    assert _status == "noop"

    # Test 3: deferred when no shape provided
    _new, _status = transform_overdue_metadata(_fake, None)
    assert _status == "deferred"
    assert _new is None

    print("Cell 4 (Fix 2) tests: PASS")

    backup_path = REPORT_BACKUP_DIR / f"{FIX2_REPORT_ID}.json"
    with open(backup_path) as f:
        describe = json.load(f)
    original_metadata = describe.get("reportMetadata", {})

    new_metadata, status = transform_overdue_metadata(original_metadata, calendar_quarter_shape)
    if status == "deferred":
        print(f"Cell 4: Fix 2 DEFERRED - no calendar-quarter shape available from probe")
        return True  # Deferral is a successful outcome of B-core
    if status == "noop":
        print(f"Cell 4: {FIX2_REPORT_ID} already correct. SKIP.")
        return True

    print(f"Cell 4: {FIX2_REPORT_ID} groupingsDown FISCAL_QUARTER -> {calendar_quarter_shape}")

    body = {"reportMetadata": new_metadata}
    result = patch_report(inst, tok, FIX2_REPORT_ID, body, dry_run)
    if not result["ok"]:
        print(f"Cell 4: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        return True

    # Inline verify
    try:
        verify = get_report_describe(inst, tok, FIX2_REPORT_ID)
    except Exception as e:
        print(f"Cell 4: VERIFY GET FAILED: {e}")
        return False
    v_gd = verify.get("reportMetadata", {}).get("groupingsDown") or []
    for g in v_gd:
        if isinstance(g, dict) and "FISCAL_QUARTER" in (g.get("name") or "").upper():
            print(f"Cell 4: VERIFY FAILED: FISCAL_QUARTER still present in groupingsDown")
            return False
    print(f"Cell 4: {FIX2_REPORT_ID} verified")
    return True


# %% Cell 5: Summary

def _cell5_main(fix1_ok: bool, fix2_ok: bool) -> None:
    print()
    print("=" * 60)
    print("Phase 2.5 B-core patch summary")
    print("=" * 60)
    print(f"Fix 1 (ph_no_activity_30_plus ARR): {'OK' if fix1_ok else 'FAILED'}")
    print(f"Fix 2 (ph_overdue_opportunities grouping): {'OK or DEFERRED' if fix2_ok else 'FAILED'}")
    print()
    print(f"Backup directory: {BACKUP_DIR}")
    print(f"Rollback helper: {BACKUP_DIR}/rollback_one.sh")
    print()
    if fix1_ok and fix2_ok:
        print("SUCCESS. Next step: re-run the audit script and commit the output.")
    else:
        print("FAILED: one or more fixes failed. Inspect backups before rollback.")
    print("=" * 60)


# %% Main

def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no PATCH operations will be sent")
        print()

    inst, tok = _cell1_main()
    _cell2_main(inst, tok)

    confirmed = load_confirmed()
    target_arr = confirmed.get("arr_convention", {}).get("confirmed_arr_aggregate")
    if not target_arr:
        print("ERROR: confirmed ARR aggregate missing from probe results.")
        sys.exit(1)
    calendar_shape = confirmed.get("calendar_quarter_probe", {}).get("calendar_quarter_shape")

    fix1_ok = _cell3_main(inst, tok, target_arr, args.dry_run)
    fix2_ok = _cell4_main(inst, tok, calendar_shape, args.dry_run)

    _cell5_main(fix1_ok, fix2_ok)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the file parses**

```bash
cd ~/crm-analytics && python3 -c "import ast; ast.parse(open('scripts/phase2_5_core_patch.py').read()); print('parse OK')"
```

Expected: `parse OK`.

- [ ] **Step 3: Run --help**

```bash
cd ~/crm-analytics && python3 scripts/phase2_5_core_patch.py --help
```

Expected: argparse usage showing `--dry-run`.

- [ ] **Step 4: No commit.**

---

## Task 3: End-to-end --dry-run

- [ ] **Step 1: Run dry-run**

```bash
cd ~/crm-analytics && python3 scripts/phase2_5_core_patch.py --dry-run 2>&1 | tee /tmp/phase2_5_probe/dry_run.log
```

Expected output:

- `DRY RUN MODE`
- `Cell 1 (auth): instance=...`
- `Cell 2 (backup): 00OTb000008TaEnMAK -> ...`
- `Cell 2 (backup): 00OTb000008SrmLMAS -> ...`
- `Cell 2: wrote rollback helper to /tmp/phase2_5_backup/rollback_one.sh`
- `Cell 3 (Fix 1) tests: PASS`
- `Cell 3: 00OTb000008TaEnMAK aggregates [...] -> [...]`
- `Cell 3: 00OTb000008TaEnMAK detailColumns +'Opportunity.APTS_Opportunity_ARR__c'`
- `  [DRY RUN] would PATCH 00OTb000008TaEnMAK: body[:400]=...`
- `Cell 4 (Fix 2) tests: PASS`
- Either `Cell 4: Fix 2 DEFERRED - no calendar-quarter shape available` OR `Cell 4: 00OTb000008SrmLMAS groupingsDown FISCAL_QUARTER -> ...` + `[DRY RUN] would PATCH 00OTb000008SrmLMAS`
- Cell 5 summary: `Fix 1: OK`, `Fix 2: OK or DEFERRED`, `SUCCESS`

- [ ] **Step 2: Spot-check the Fix 1 PATCH body includes the ARR field in detailColumns**

```bash
grep "would PATCH 00OTb000008TaEnMAK" /tmp/phase2_5_probe/dry_run.log | grep -c "APTS_Opportunity_ARR"
```

Expected: at least 1.

- [ ] **Step 3: STOP if dry-run fails**

If any cell test fails or the PATCH bodies look malformed, STOP and report BLOCKED.

- [ ] **Step 4: No commit.**

---

## Task 4: Live run

- [ ] **Step 1: Run live**

```bash
cd ~/crm-analytics && python3 scripts/phase2_5_core_patch.py 2>&1 | tee /tmp/phase2_5_probe/live_run.log
```

Expected: ~30-60 seconds wall time. Cell 5 summary shows `Fix 1: OK`, `Fix 2: OK or DEFERRED`, `SUCCESS`.

- [ ] **Step 2: Check for failures**

```bash
grep -iE "FAILED|HTTP 4|HTTP 5" /tmp/phase2_5_probe/live_run.log | grep -v "tests: PASS"
```

Expected: zero hits.

- [ ] **Step 3: If any failure, use rollback**

```bash
/tmp/phase2_5_backup/rollback_one.sh <FAILED_REPORT_ID>
```

Then STOP and report.

- [ ] **Step 4: No commit yet.** Task 5 runs the audit.

---

## Task 5: Re-run the audit against Dashboard 2

- [ ] **Step 1: Run the audit**

```bash
cd ~/crm-analytics && python3 scripts/audit_sales_director_monthly_dashboard.py \
  --dashboard-id 01ZTb00000FSP9JMAX \
  --spec-path /Users/test/crm-analytics/docs/specs/sales-ops-quarterly-dashboard-spec.md \
  --output-name sales-ops-quarterly-audit \
  2>&1 | tee /tmp/phase2_5_probe/audit_rerun.log
```

Expected: ~1-2 minutes. Writes `docs/audits/2026-04-08-sales-ops-quarterly-audit.md`.

- [ ] **Step 2: Inspect the new tally**

```bash
grep -E "^- \*\*Tally" ~/crm-analytics/docs/audits/2026-04-08-sales-ops-quarterly-audit.md
```

Pre-patch (`d48f13c`): `35 entries . 23 BLOCKING . 8 WRONG-DATA . 4 ORPHAN`

Expected post-patch: WRONG-DATA drops by 2 (or 1 if Fix 2 deferred). OK count increases correspondingly.

- [ ] **Step 3: Verify Fix 1 landed**

```bash
grep -c "ph_no_activity_30_plus.*AMOUNT\|No Activity.*s!AMOUNT" ~/crm-analytics/docs/audits/2026-04-08-sales-ops-quarterly-audit.md
```

Expected: 0 (the AMOUNT defect is gone).

- [ ] **Step 4: Verify Fix 2 landed OR was deferred**

```bash
grep -c "ph_overdue_opportunities.*FISCAL_QUARTER\|Overdue Opportunities.*FISCAL_QUARTER" ~/crm-analytics/docs/audits/2026-04-08-sales-ops-quarterly-audit.md
```

Expected: 0 if Fix 2 applied. Non-zero is acceptable if Fix 2 was deferred (check the live_run.log for the DEFERRED marker).

- [ ] **Step 5: No commit yet.** Task 6 commits.

---

## Task 6: Commit the new audit output

- [ ] **Step 1: Verify scripts are uncommitted**

```bash
cd ~/crm-analytics && git status --short scripts/phase2_5_shape_probe.py scripts/phase2_5_core_patch.py scripts/audit_sales_director_monthly_dashboard.py
```

Expected: all show `??` or `M`.

- [ ] **Step 2: Stage the audit by exact path**

```bash
cd ~/crm-analytics && git add docs/audits/2026-04-08-sales-ops-quarterly-audit.md && git diff --cached --name-only
```

Expected: exactly one line.

- [ ] **Step 3: Commit**

Replace `<FIX2_STATUS>` with either "applied" or "deferred" and `<TALLY_LINE>` with the actual tally from the new audit.

```bash
cd ~/crm-analytics && git commit -m "$(cat <<'COMMIT'
docs: phase 2.5 b-core post-patch report 2 audit - dashboard 2 defect fixes

Net new audit (not an overwrite - the pre-patch audit at
2026-04-07-sales-ops-quarterly-audit.md commit d48f13c stays intact).
Both coexist in git history for audit trail, matching the Phase 1.5
pattern.

Phase 2.5 B-core applied to 01ZTb00000FSP9JMAX:

- Fix 1: ph_no_activity_30_plus aggregate swapped from s!AMOUNT to
  the canonical ARR form (confirmed by pre-flight probe against the
  reference Dashboard 2 widget dq_missing_decision_reason).
  Opportunity.APTS_Opportunity_ARR__c added to detailColumns.
- Fix 2: ph_overdue_opportunities FISCAL_QUARTER grouping <FIX2_STATUS>.

Pre-patch tally (d48f13c): 35 entries . 23 BLOCKING . 8 WRONG-DATA . 4 ORPHAN
Post-patch tally: <TALLY_LINE>

Out of scope (deferred to future phases):

- 5 new process compliance SF reports (pc_*) - Phase 2.6
- 9 new Dashboard 1 missing widgets (Report 1 spec gaps) - Phase 2.6+
- 2 new PI list views (fa_forecast_change_volatility,
  fa_slipped_count_quarterly) - needs manual SF Lightning UI work
- WIP completion for ph_probability_mismatch_by_stage - needs threshold
  decision from Sales Ops
- Retire-vs-repurpose for dq_missing_quote_type - needs product decision

Scripts (all uncommitted by convention):
- scripts/audit_sales_director_monthly_dashboard.py (reused)
- scripts/phase2_5_shape_probe.py (Phase 2.5 Task 1)
- scripts/phase2_5_core_patch.py (Phase 2.5 Tasks 2-4)

Backups at /tmp/phase2_5_backup/ with rollback_one.sh helper.

Spec graded against: docs/specs/sales-ops-quarterly-dashboard-spec.md
(commit 25cc03d, 22-widget Report 2 spec).

Design: docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-design.md
(commit 1c2195f).
Plan: docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-plan.md.

Next: Phase 2.6 (larger Phase 2.5 subsets) and Phase 4 (deck rebuild).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
COMMIT
)"
```

- [ ] **Step 4: Verify the commit landed**

```bash
cd ~/crm-analytics && git log -1 --format="%h %s"
```

---

## Self-review checklist

- [ ] Every section of the design doc has a corresponding task.
- [ ] No placeholders.
- [ ] Function/variable names consistent across tasks.
- [ ] All paths absolute or ~/crm-analytics-relative.
- [ ] Every commit uses `git add <exact path>`.
- [ ] All 3 scripts uncommitted.
- [ ] No em-dashes anywhere.
- [ ] Rollback documented for every failure mode.
- [ ] Dry-run before live (Task 3 before Task 4).

## Notes for the executor

- **Phase 2.5 B-core is small.** ~30-45 minutes total across all tasks. Most time is the probe + audit re-run.
- **The probe is load-bearing for Fix 2.** If the probe finds no calendar-quarter grouping shape, Fix 2 is deferred - that is a valid outcome.
- **Phase 1.5 lessons are pre-applied.** Canonical aggregate form (no `.CONVERT` by default but re-verified by the probe), detailColumns must include aggregate fields, `wrapped_full` body shape, inline verification, rollback helper.
- **If Fix 1 or Fix 2 fails**, use `/tmp/phase2_5_backup/rollback_one.sh <report_id>` to restore.
- **Sales Directors will see a calendar-quarter pivot on the Overdue Opportunities widget** if Fix 2 lands - small visible change, much smaller impact than Phase 1.5's changes.
