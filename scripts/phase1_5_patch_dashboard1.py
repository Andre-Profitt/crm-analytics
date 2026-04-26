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
Plan:   docs/2026-04-07-phase1-5-dashboard1-hotfix-plan.md (commit 5cef727)

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

# Fiscal -> Calendar date filter mapping.
# NOTE: Salesforce's bare-calendar enum tokens do NOT use a "CALENDAR" prefix.
# Verified empirically from the live org - THIS_YEAR is the correct form,
# not THIS_CALENDAR_YEAR. A prior version of this script used the "CALENDAR"
# form and got HTTP 400 from all 13 fiscal PATCHes.
FISCAL_TO_CALENDAR = {
    "THIS_FISCAL_YEAR": "THIS_YEAR",
    "THIS_FISCAL_QUARTER": "THIS_QUARTER",
    "LAST_FISCAL_YEAR": "LAST_YEAR",
    "LAST_FISCAL_QUARTER": "LAST_QUARTER",
    "NEXT_FISCAL_YEAR": "NEXT_YEAR",
    "NEXT_FISCAL_QUARTER": "NEXT_QUARTER",
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
    print(
        f"Cell 2 (backup): {len(arr_ids)} ARR + {len(fiscal_ids)} fiscal = {len(all_report_ids)} unique reports"
    )

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

    return arr_ids, fiscal_ids


def _cell2_main(inst: str, tok: str) -> tuple[list[str], list[str]]:
    return backup_all(inst, tok)


# %% Cell 3: ARR patches (inline verification)

RENEWAL_ACV_AGGREGATE = "s!Opportunity.APTS_Renewal_ACV__c"


def _is_amount_aggregate(agg: str) -> bool:
    """Check if an aggregate reference is the standard Amount field."""
    if not isinstance(agg, str):
        return False
    # Common forms: s!AMOUNT, s!AMOUNT.CONVERT, s!Opportunity.Amount, s!Opportunity.Amount.CONVERT
    normalized = agg.upper()
    return normalized in (
        "S!AMOUNT",
        "S!AMOUNT.CONVERT",
        "S!OPPORTUNITY.AMOUNT",
        "S!OPPORTUNITY.AMOUNT.CONVERT",
    )


def transform_aggregates_to_acv(aggregates: list[str]) -> tuple[list[str], bool]:
    """Replace the first Amount-shaped aggregate with the canonical ACV aggregate.

    If the ACV aggregate already exists elsewhere in the list after the swap
    (creating a duplicate), the duplicate entry is removed so the list stays
    unique.  Salesforce rejects PATCH bodies with duplicate aggregates.

    Returns (new_aggregates, changed) - changed is True if a swap occurred.
    """
    if not aggregates:
        return aggregates, False
    new_aggs = list(aggregates)
    if _is_amount_aggregate(new_aggs[0]):
        new_aggs[0] = RENEWAL_ACV_AGGREGATE
        # Deduplicate: if RENEWAL_ACV_AGGREGATE now appears more than once
        # (because it was already present in a later position), remove the
        # extra copies while preserving list order.
        seen: set[str] = set()
        deduped: list[str] = []
        for agg in new_aggs:
            if agg not in seen:
                seen.add(agg)
                deduped.append(agg)
        return deduped, True
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
        return {
            "ok": False,
            "error": f"HTTP {r.status_code}: {r.text[:500]}",
            "status": r.status_code,
        }
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
    _new, _changed = transform_aggregates_to_acv(
        ["s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"]
    )
    assert _changed is False

    # Test 5: Empty list is no-op
    _new, _changed = transform_aggregates_to_acv([])
    assert _changed is False

    # Test 6: Deduplication - when ACV already exists after s!AMOUNT, swap and dedupe
    _new, _changed = transform_aggregates_to_acv(
        ["s!AMOUNT", RENEWAL_ACV_AGGREGATE, "RowCount"]
    )
    assert _changed is True
    assert _new == [RENEWAL_ACV_AGGREGATE, "RowCount"], f"dedup test got {_new}"

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

        # Ensure the ACV field is in detailColumns - Salesforce Reports API
        # requires that a summary aggregate's field also appear in detailColumns.
        # The canonical form (verified from an existing working report) is the
        # lowercase Opportunity-prefixed string without .CONVERT suffix.
        ACV_DETAIL_COLUMN = "Opportunity.APTS_Renewal_ACV__c"
        detail_columns = list(new_metadata.get("detailColumns") or [])
        if ACV_DETAIL_COLUMN not in detail_columns:
            detail_columns.append(ACV_DETAIL_COLUMN)
            new_metadata["detailColumns"] = detail_columns

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
            print(
                f"  {rid}: VERIFY FAILED: aggregates[0]="
                f"{actual_aggs[0] if actual_aggs else None} "
                f"(expected {RENEWAL_ACV_AGGREGATE})"
            )
            failures.append(rid)
            continue
        print(f"  {rid}: verified")

        # Refresh the backup file with the post-PATCH state so Cell 4 (fiscal
        # filter swap) reads the updated aggregate, not the original Amount.
        # The 3 ARR target reports are also in the 13 fiscal target list.
        with open(backup_path, "w") as f:
            json.dump(verify, f, indent=2)

    print(
        f"Cell 3: {len(arr_ids) - len(failures)}/{len(arr_ids)} ARR patches successful"
    )
    return failures


def _cell3_main(inst: str, tok: str, arr_ids: list[str], dry_run: bool) -> list[str]:
    return patch_arr_widgets(inst, tok, arr_ids, dry_run)


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
            print(
                f"  {rid}: date filter already correct or non-fiscal ({current_dv}). SKIP."
            )
            continue
        print(f"  {rid}: {current_dv} -> {new_dv}")

        new_metadata = copy.deepcopy(original_metadata)
        new_metadata.setdefault("standardDateFilter", {})["durationValue"] = new_dv
        body = build_patch_body(new_metadata, shape_label)
        result = patch_report(inst, tok, rid, body, dry_run)
        if not result["ok"]:
            print(f"  {rid}: PATCH FAILED: {result['error']}")
            failures.append(rid)

    print(
        f"Cell 4: {len(fiscal_ids) - len(failures)}/{len(fiscal_ids)} fiscal patches successful"
    )
    return failures


def _cell4_main(inst: str, tok: str, fiscal_ids: list[str], dry_run: bool) -> list[str]:
    return patch_fiscal_filters(inst, tok, fiscal_ids, dry_run)


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
    if "properties" in new_component:
        props = new_component["properties"]
        # Clear the cloned aggregate so the new report's aggregate is resolved
        # at PATCH time (the empty-aggregate resolution loop fills it in).
        if isinstance(props.get("aggregates"), list):
            props["aggregates"] = []
        # Clear groupings — the template's grouping columns almost certainly
        # don't exist in the new report, which causes a "field not available"
        # validation error.  An empty list lets Salesforce auto-resolve.
        if isinstance(props.get("groupings"), list):
            props["groupings"] = []
        # Clear filterColumns for the same reason.
        if isinstance(props.get("filterColumns"), list):
            props["filterColumns"] = []
    return new_component


def patch_dashboard_component(inst: str, tok: str, dry_run: bool) -> bool:
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
            print(
                f"  Dashboard already has a component referencing {COMMERCIAL_APPROVAL_APPROVED_YTD_SOURCE_REPORT_ID}. SKIP."
            )
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

    # Fix any components whose properties.aggregates is an empty list.
    # An empty aggregates list means "auto-select first report aggregate", but
    # after the ARR patches changed those reports' aggregate[0], Salesforce now
    # requires at least one explicit aggregate entry.  We resolve each such
    # component by fetching the live report's aggregates[0] and injecting it.
    # (In dry-run we skip the GET calls and just warn.)
    if not dry_run:
        fixed_agg_count = 0
        for c in new_dashboard.get("components", []):
            if c.get("properties", {}).get("aggregates") == []:
                rid = c.get("reportId")
                if rid:
                    try:
                        report_desc = get_report_describe(inst, tok, rid)
                        live_agg0 = report_desc.get("reportMetadata", {}).get(
                            "aggregates", []
                        )
                        if live_agg0:
                            c["properties"]["aggregates"] = [{"name": live_agg0[0]}]
                            fixed_agg_count += 1
                    except Exception:
                        pass  # leave as [] and let Salesforce decide
        if fixed_agg_count:
            print(
                f"  Resolved {fixed_agg_count} empty-aggregate component(s) "
                f"from live report metadata"
            )

    if dry_run:
        print(
            f"  [DRY RUN] would PATCH dashboard {DASHBOARD_ID}: components len={len(new_components)}"
        )
        print(f"    new component reportId: {new_component.get('reportId')}")
        print(f"    new component header:   {new_component.get('header')}")
        print(f"    new component title:    {new_component.get('title')}")
        print(f"    new component id:       {new_component.get('id')}")
        return True

    # Strip read-only dashboard fields that Analytics REST may reject.
    # owner/runningUser/folderName are lookup objects that the PATCH endpoint
    # rejects when included in the body.
    for ro_field in (
        "id",
        "createdDate",
        "lastModifiedDate",
        "lastAccessedDate",
        "url",
        "owner",
        "runningUser",
        "folderName",
    ):
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


# %% Cell 6: Summary (placeholder - updated as cells 3, 4, 5 are added)


def _cell6_main(
    arr_failures: list[str],
    fiscal_failures: list[str],
    dashboard_patch_ok: bool,
) -> None:
    total_failures = (
        len(arr_failures) + len(fiscal_failures) + (0 if dashboard_patch_ok else 1)
    )
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
        print(
            f"FAILED: {total_failures} operations failed. Inspect backups before rollback."
        )
    print("=" * 60)


# %% Main


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


if __name__ == "__main__":
    main()
